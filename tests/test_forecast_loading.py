import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import numpy as np
import pandas as pd
import rasterio
import xarray as xr

from src.core.case_context import get_case_context
from src.services.ensemble import EnsembleForecastService, normalize_model_timestamp
from src.utils.io import (
    detect_shoreline_mask_regeneration_need,
    get_ensemble_manifest_path,
    get_forecast_manifest_path,
    get_official_control_footprint_mask_path,
    get_official_mask_p50_datecomposite_path,
    get_phase2_loading_audit_paths,
    get_phase3b_forecast_candidates,
    get_recipe_sensitivity_run_name,
)
from src.helpers.raster import GridBuilder


class ForecastLoadingTests(unittest.TestCase):
    def tearDown(self):
        get_case_context.cache_clear()

    def test_normalize_model_timestamp_strips_timezone(self):
        ts = normalize_model_timestamp("2023-03-03T09:59:00Z")
        self.assertIsNone(ts.tzinfo)
        self.assertEqual(str(ts), "2023-03-03 09:59:00")

    def test_extend_forcing_tail_adds_requested_end_time(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            source_path = base / "currents.nc"
            cache_dir = base / "cache"

            ds = xr.Dataset(
                data_vars={
                    "uo": (("time",), np.array([0.1, 0.2], dtype=np.float32)),
                    "vo": (("time",), np.array([0.0, 0.0], dtype=np.float32)),
                },
                coords={
                    "time": pd.to_datetime(["2023-03-03T00:00:00", "2023-03-04T00:00:00"]),
                },
            )
            ds.to_netcdf(source_path)

            service = EnsembleForecastService(str(source_path), str(source_path))
            service.loading_cache_dir = cache_dir

            extended_path = service._extend_forcing_tail(
                source_path=source_path,
                target_end_time=pd.Timestamp("2023-03-04T12:00:00"),
                time_coordinate="time",
            )

            self.assertNotEqual(extended_path, source_path)
            with xr.open_dataset(extended_path) as extended:
                times = pd.to_datetime(extended["time"].values)
                self.assertEqual(str(times[-1]), "2023-03-04 12:00:00")
                self.assertEqual(len(times), 3)

    def test_official_paths_and_candidates_use_canonical_products(self):
        with mock.patch.dict(os.environ, {"WORKFLOW_MODE": "mindoro_retro_2023"}, clear=False):
            get_case_context.cache_clear()
            forecast_manifest = get_forecast_manifest_path()
            ensemble_manifest = get_ensemble_manifest_path()
            audit_paths = get_phase2_loading_audit_paths()
            candidates = get_phase3b_forecast_candidates("cmems_era5")

            self.assertEqual(forecast_manifest.name, "forecast_manifest.json")
            self.assertEqual(ensemble_manifest.name, "ensemble_manifest.json")
            self.assertEqual(audit_paths["json"].name, "phase2_loading_audit.json")
            self.assertEqual(audit_paths["csv"].name, "phase2_loading_audit.csv")
            self.assertEqual(get_official_control_footprint_mask_path().name, "control_footprint_mask_2023-03-06T09-59-00Z.tif")
            self.assertEqual(get_official_mask_p50_datecomposite_path().name, "mask_p50_2023-03-06_datecomposite.tif")
            self.assertTrue(any(spec["path"].endswith("control_footprint_mask_2023-03-06T09-59-00Z.tif") for spec in candidates))
            self.assertTrue(any(spec["path"].endswith("mask_p50_2023-03-06_datecomposite.tif") for spec in candidates))
            self.assertFalse(any(spec["path"].endswith("mask_p50_2023-03-06T09-59-00Z.tif") for spec in candidates))
            self.assertFalse(any("probability_72h" in spec["path"] for spec in candidates))
            self.assertFalse(any("hits_72" in spec["path"] for spec in candidates))

    def test_official_service_reads_case_driven_config(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            dummy = Path(tmpdir) / "dummy.nc"
            xr.Dataset().to_netcdf(dummy)

            with mock.patch.dict(os.environ, {"WORKFLOW_MODE": "mindoro_retro_2023"}, clear=False):
                get_case_context.cache_clear()
                service = EnsembleForecastService(str(dummy), str(dummy), wave_file=str(dummy))

            self.assertEqual(service.official_ensemble_size, 50)
            self.assertEqual(service.official_element_count, 5000)
            self.assertEqual(service.official_polygon_seed_random_seed, 20230303)
            self.assertTrue(service.require_wave_forcing)
            self.assertTrue(service.enable_stokes_drift)
            self.assertTrue(service.provisional_transport_model)
            self.assertEqual(service.audit_json_path.name, "phase2_loading_audit.json")

    def test_official_polygon_seeding_uses_custom_override_path_when_provided(self):
        class DummyModel:
            def __init__(self):
                self.calls = []

            def seed_elements(self, **kwargs):
                self.calls.append(kwargs)

        with tempfile.TemporaryDirectory() as tmpdir:
            dummy = Path(tmpdir) / "dummy.nc"
            xr.Dataset().to_netcdf(dummy)

            with mock.patch.dict(os.environ, {"WORKFLOW_MODE": "mindoro_retro_2023"}, clear=False):
                get_case_context.cache_clear()
                service = EnsembleForecastService(str(dummy), str(dummy), wave_file=str(dummy))

            service.seed_overrides = {
                "polygon_vector_path": str(Path(tmpdir) / "custom_seed_polygon.gpkg"),
                "source_geometry_label": "accepted_march13_noaa_processed_polygon",
            }
            model = DummyModel()
            audit = {}
            with mock.patch(
                "src.utils.io.resolve_polygon_seeding",
                return_value=([121.5], [13.3], "2023-03-12T16:00:00Z"),
            ) as mocked:
                seed_record = service._seed_official_release(
                    model,
                    "2023-03-12T16:00:00Z",
                    num_elements=1,
                    random_seed=123,
                    audit=audit,
                )

        self.assertEqual(mocked.call_args.kwargs["polygon_path"], str(Path(tmpdir) / "custom_seed_polygon.gpkg"))
        self.assertEqual(seed_record["source_geometry_path"], str(Path(tmpdir) / "custom_seed_polygon.gpkg"))
        self.assertEqual(seed_record["release_geometry"], "accepted_march13_noaa_processed_polygon")
        self.assertTrue(seed_record["custom_polygon_override_used"])
        self.assertEqual(len(model.calls), 1)

    def test_official_polygon_seeding_falls_back_to_default_when_no_override_is_present(self):
        class DummyModel:
            def __init__(self):
                self.calls = []

            def seed_elements(self, **kwargs):
                self.calls.append(kwargs)

        with tempfile.TemporaryDirectory() as tmpdir:
            dummy = Path(tmpdir) / "dummy.nc"
            xr.Dataset().to_netcdf(dummy)

            with mock.patch.dict(os.environ, {"WORKFLOW_MODE": "mindoro_retro_2023"}, clear=False):
                get_case_context.cache_clear()
                service = EnsembleForecastService(str(dummy), str(dummy), wave_file=str(dummy))

            service.seed_overrides = {}
            model = DummyModel()
            with mock.patch(
                "src.utils.io.resolve_polygon_seeding",
                return_value=([121.5], [13.3], "2023-03-03T09:59:00Z"),
            ) as mocked:
                seed_record = service._seed_official_release(
                    model,
                    "2023-03-03T09:59:00Z",
                    num_elements=1,
                    random_seed=123,
                )

        self.assertIsNone(mocked.call_args.kwargs["polygon_path"])
        self.assertEqual(seed_record["release_geometry"], "processed_march3_initialization_polygon")
        self.assertFalse(seed_record["custom_polygon_override_used"])
        self.assertEqual(len(model.calls), 1)

    def test_prototype_2016_ensemble_uses_drifter_point_and_records_seed_metadata(self):
        class DummyModel:
            def __init__(self):
                self.seed_calls = []
                self.config_calls = []

            def seed_elements(self, **kwargs):
                self.seed_calls.append(kwargs)

            def set_config(self, *args):
                self.config_calls.append(args)

            def run(self, *, duration, time_step, outfile):
                xr.Dataset(coords={"time": pd.to_datetime(["2016-09-01T00:00:00"])}).to_netcdf(outfile)

        with tempfile.TemporaryDirectory() as tmpdir:
            dummy = Path(tmpdir) / "dummy.nc"
            xr.Dataset(coords={"time": pd.to_datetime(["2016-09-01T00:00:00"])}).to_netcdf(dummy)

            with mock.patch.dict(
                os.environ,
                {
                    "WORKFLOW_MODE": "prototype_2016",
                    "PHASE_1_START_DATE": "2016-09-01",
                    "RUN_NAME": "CASE_2016-09-01",
                },
                clear=False,
            ):
                get_case_context.cache_clear()
                service = EnsembleForecastService(
                    str(dummy),
                    str(dummy),
                    output_run_name="TEST_PROTOTYPE_2016_DRIFTER_POINT",
                )

            runtime_root = Path(tmpdir) / "runtime"
            service.base_output_dir = runtime_root
            service.output_dir = runtime_root / "ensemble"
            service.forecast_dir = runtime_root / "forecast"
            service.member_mask_dir = service.output_dir / "member_presence"
            service.loading_cache_dir = service.forecast_dir / "forcing_cache"
            service.output_dir.mkdir(parents=True, exist_ok=True)
            service.forecast_dir.mkdir(parents=True, exist_ok=True)
            service.member_mask_dir.mkdir(parents=True, exist_ok=True)
            service.loading_cache_dir.mkdir(parents=True, exist_ok=True)
            service.audit_json_path = service.forecast_dir / "forecast_loading_audit.json"
            service.audit_csv_path = service.forecast_dir / "forecast_loading_audit.csv"
            service.ensemble_size = 1
            service.config["perturbations"] = {
                "time_shift_hours": 0.0,
                "diffusivity_min": 1.0,
                "diffusivity_max": 1.0,
                "wind_uncertainty_min": 0.0,
                "wind_uncertainty_max": 0.0,
            }

            manifest_path = service.forecast_dir / "ensemble_manifest.json"
            model = DummyModel()
            with mock.patch.object(service, "_build_model", return_value=model), \
                mock.patch.object(service, "_generate_prototype_probability_products", return_value=([], [])), \
                mock.patch.object(service, "_seed_point_release", wraps=service._seed_point_release) as point_seed, \
                mock.patch.object(service, "_seed_polygon_release", wraps=service._seed_polygon_release) as polygon_seed, \
                mock.patch("src.services.ensemble.get_ensemble_manifest_path", return_value=manifest_path):
                service.run_ensemble(
                    recipe_name="cmems_era5",
                    start_lat=10.4150,
                    start_lon=117.1800,
                    start_time="2016-09-01T00:00:00Z",
                )

            manifest_payload = json.loads(manifest_path.read_text(encoding="utf-8"))
            audit_payload = json.loads(service.audit_json_path.read_text(encoding="utf-8"))
            audit_csv = pd.read_csv(service.audit_csv_path)

            point_seed.assert_called_once()
            polygon_seed.assert_not_called()
            self.assertTrue(manifest_payload["source_geometry"]["initialization_polygon_metadata_only"])
            member_seed = manifest_payload["member_runs"][0]["seed_initialization"]
            self.assertEqual(member_seed["initialization_mode"], "drifter_of_record_point")
            self.assertEqual(member_seed["release_geometry"], "legacy_drifter_of_record_point")
            self.assertEqual(
                member_seed["source_point_path"],
                "data/arcgis/CASE_2016-09-01/source_point_metadata.geojson",
            )
            self.assertAlmostEqual(member_seed["source_lat"], 10.4150)
            self.assertAlmostEqual(member_seed["source_lon"], 117.1800)
            self.assertEqual(member_seed["release_start_utc"], "2016-09-01T00:00:00Z")
            self.assertEqual(
                audit_payload["runs"][0]["seed_initialization"]["release_geometry"],
                "legacy_drifter_of_record_point",
            )
            self.assertEqual(audit_csv["seed_initialization_mode"].iloc[0], "drifter_of_record_point")
            self.assertEqual(audit_csv["seed_release_geometry"].iloc[0], "legacy_drifter_of_record_point")
            get_case_context.cache_clear()

    def test_prototype_2016_probability_products_generate_and_register_drifter_pngs(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            dummy = tmp_path / "dummy.nc"
            xr.Dataset(coords={"time": pd.to_datetime(["2016-09-01T00:00:00"])}).to_netcdf(dummy)

            with mock.patch.dict(
                os.environ,
                {
                    "WORKFLOW_MODE": "prototype_2016",
                    "PHASE_1_START_DATE": "2016-09-01",
                    "RUN_NAME": "CASE_2016-09-01",
                },
                clear=False,
            ):
                get_case_context.cache_clear()
                service = EnsembleForecastService(
                    str(dummy),
                    str(dummy),
                    output_run_name="TEST_PROTOTYPE_2016_DRIFTER_PNGS",
                )

            runtime_root = tmp_path / "runtime"
            service.base_output_dir = runtime_root
            service.output_dir = runtime_root / "ensemble"
            service.forecast_dir = runtime_root / "forecast"
            service.member_mask_dir = service.output_dir / "member_presence"
            service.loading_cache_dir = service.forecast_dir / "forcing_cache"
            service.output_dir.mkdir(parents=True, exist_ok=True)
            service.forecast_dir.mkdir(parents=True, exist_ok=True)
            service.member_mask_dir.mkdir(parents=True, exist_ok=True)
            service.loading_cache_dir.mkdir(parents=True, exist_ok=True)

            member_file = service.output_dir / "member_01.nc"
            xr.Dataset(
                data_vars={
                    "lon": (("time", "particle"), np.array([[117.180], [117.215], [117.255], [117.290]], dtype=np.float32)),
                    "lat": (("time", "particle"), np.array([[10.415], [10.438], [10.461], [10.487]], dtype=np.float32)),
                },
                coords={
                    "time": pd.to_datetime(
                        [
                            "2016-09-01T00:00:00",
                            "2016-09-02T00:00:00",
                            "2016-09-03T00:00:00",
                            "2016-09-04T00:00:00",
                        ]
                    ),
                    "particle": [0],
                },
            ).to_netcdf(member_file)

            drifter_csv = tmp_path / "drifters_noaa.csv"
            pd.DataFrame(
                {
                    "time": [
                        "2016-09-01T00:00:00Z",
                        "2016-09-02T00:00:00Z",
                        "2016-09-03T00:00:00Z",
                        "2016-09-04T00:00:00Z",
                    ],
                    "lat": [10.415, 10.431, 10.454, 10.481],
                    "lon": [117.180, 117.205, 117.241, 117.276],
                    "ID": ["DRIFTER_A", "DRIFTER_A", "DRIFTER_A", "DRIFTER_A"],
                }
            ).to_csv(drifter_csv, index=False)

            def _temp_probability_path(hour: int, run_name: str | None = None):
                return service.output_dir / f"probability_{int(hour)}h.tif"

            with mock.patch("src.services.ensemble.get_ensemble_probability_score_raster_path", side_effect=_temp_probability_path), \
                mock.patch.object(service, "_prototype_2016_drifter_path", return_value=drifter_csv):
                written_files, product_records = service._generate_prototype_probability_products(
                    [member_file],
                    10.4150,
                    117.1800,
                )

            track_png = service.output_dir / "drifter_track_72h.png"
            overlay_png = service.output_dir / "drifter_track_vs_ensemble_72h.png"
            self.assertTrue(track_png.exists())
            self.assertTrue(overlay_png.exists())
            self.assertGreater(track_png.stat().st_size, 0)
            self.assertGreater(overlay_png.stat().st_size, 0)

            product_types = {record["product_type"] for record in product_records}
            self.assertIn("drifter_track_map", product_types)
            self.assertIn("drifter_track_ensemble_overlay", product_types)

            manifest_path = runtime_root / "ensemble_manifest.json"
            with mock.patch("src.services.ensemble.get_ensemble_manifest_path", return_value=manifest_path):
                service.write_output_manifest(
                    recipe_name="cmems_era5",
                    member_runs=[],
                    written_files=written_files,
                    product_records=product_records,
                    start_time=pd.Timestamp("2016-09-01T00:00:00"),
                    selection=None,
                )

            manifest_payload = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest_product_types = {record["product_type"] for record in manifest_payload["products"]}
            manifest_written_paths = {entry["relative_path"] for entry in manifest_payload["written_files"]}
            self.assertIn("drifter_track_map", manifest_product_types)
            self.assertIn("drifter_track_ensemble_overlay", manifest_product_types)
            self.assertIn("ensemble/drifter_track_72h.png", manifest_written_paths)
            self.assertIn("ensemble/drifter_track_vs_ensemble_72h.png", manifest_written_paths)
            get_case_context.cache_clear()

    def test_only_prototype_2016_generates_drifter_visual_products(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            dummy = tmp_path / "dummy.nc"
            xr.Dataset(coords={"time": pd.to_datetime(["2021-03-05T18:00:00"])}).to_netcdf(dummy)

            with mock.patch.dict(
                os.environ,
                {
                    "WORKFLOW_MODE": "prototype_2021",
                    "PROTOTYPE_CASE_ID": "CASE_20210305T180000Z",
                    "RUN_NAME": "CASE_20210305T180000Z",
                },
                clear=False,
            ):
                get_case_context.cache_clear()
                service = EnsembleForecastService(
                    str(dummy),
                    str(dummy),
                    output_run_name="TEST_PROTOTYPE_2021_NO_LEGACY_DRIFTER_PNGS",
                )

            runtime_root = tmp_path / "runtime"
            service.base_output_dir = runtime_root
            service.output_dir = runtime_root / "ensemble"
            service.forecast_dir = runtime_root / "forecast"
            service.member_mask_dir = service.output_dir / "member_presence"
            service.loading_cache_dir = service.forecast_dir / "forcing_cache"
            service.output_dir.mkdir(parents=True, exist_ok=True)
            service.forecast_dir.mkdir(parents=True, exist_ok=True)
            service.member_mask_dir.mkdir(parents=True, exist_ok=True)
            service.loading_cache_dir.mkdir(parents=True, exist_ok=True)

            member_file = service.output_dir / "member_01.nc"
            xr.Dataset(
                data_vars={
                    "lon": (("time", "particle"), np.array([[120.200], [120.240], [120.285], [120.330]], dtype=np.float32)),
                    "lat": (("time", "particle"), np.array([[13.100], [13.145], [13.185], [13.230]], dtype=np.float32)),
                },
                coords={
                    "time": pd.to_datetime(
                        [
                            "2021-03-05T18:00:00",
                            "2021-03-06T18:00:00",
                            "2021-03-07T18:00:00",
                            "2021-03-08T18:00:00",
                        ]
                    ),
                    "particle": [0],
                },
            ).to_netcdf(member_file)

            def _temp_probability_path(hour: int, run_name: str | None = None):
                return service.output_dir / f"probability_{int(hour)}h.tif"

            with mock.patch("src.services.ensemble.get_ensemble_probability_score_raster_path", side_effect=_temp_probability_path), \
                mock.patch("src.services.ensemble.plot_probability_map", return_value=None), \
                mock.patch.object(service, "_generate_prototype_2016_drifter_visual_products", return_value=([], [])) as legacy_visuals:
                written_files, product_records = service._generate_prototype_probability_products(
                    [member_file],
                    13.1000,
                    120.2000,
                )

            legacy_visuals.assert_not_called()
            self.assertNotIn("drifter_track_map", {record["product_type"] for record in product_records})
            self.assertNotIn("drifter_track_ensemble_overlay", {record["product_type"] for record in product_records})
            self.assertFalse(any(path.name == "drifter_track_72h.png" for path in written_files))
            self.assertFalse(any(path.name == "drifter_track_vs_ensemble_72h.png" for path in written_files))
            get_case_context.cache_clear()

    def test_prototype_2016_reuse_validation_accepts_complete_same_case_science_outputs(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            dummy = tmp_path / "dummy.nc"
            xr.Dataset().to_netcdf(dummy)

            with mock.patch.dict(
                os.environ,
                {
                    "WORKFLOW_MODE": "prototype_2016",
                    "PHASE_1_START_DATE": "2016-09-01",
                    "RUN_NAME": "CASE_2016-09-01",
                },
                clear=False,
            ):
                get_case_context.cache_clear()
                service = EnsembleForecastService(str(dummy), str(dummy))

            service.base_output_dir = tmp_path / "output" / "CASE_2016-09-01"
            service.output_dir = service.base_output_dir / "ensemble"
            service.forecast_dir = service.base_output_dir / "forecast"
            service.member_mask_dir = service.output_dir / "member_presence"
            service.loading_cache_dir = service.forecast_dir / "forcing_cache"
            service.output_dir.mkdir(parents=True, exist_ok=True)
            service.forecast_dir.mkdir(parents=True, exist_ok=True)
            service.member_mask_dir.mkdir(parents=True, exist_ok=True)
            service.loading_cache_dir.mkdir(parents=True, exist_ok=True)

            for member_id in range(1, service.ensemble_size + 1):
                (service.output_dir / f"member_{member_id:02d}.nc").write_bytes(b"member")
            (service.output_dir / "metadata.json").write_text(
                json.dumps(
                    {
                        "probability_semantics": "member_occupancy_probability",
                        "probability_semantics_version": "prototype_2016_member_occupancy_v1",
                    }
                ),
                encoding="utf-8",
            )
            for hour in (24, 48, 72):
                (service.output_dir / f"probability_{hour}h.nc").write_bytes(b"nc")
                (service.output_dir / f"probability_{hour}h.tif").write_bytes(b"tif")
                (service.output_dir / f"particle_density_fraction_{hour}h.nc").write_bytes(b"nc")
                (service.output_dir / f"particle_density_fraction_{hour}h.tif").write_bytes(b"tif")
                (service.output_dir / f"mask_p50_{hour}h.tif").write_bytes(b"mask")
                (service.output_dir / f"mask_p90_{hour}h.tif").write_bytes(b"mask")

            manifest_path = tmp_path / "ensemble_manifest.json"
            manifest_payload = {
                "manifest_type": "prototype_phase2_ensemble",
                "workflow_mode": "prototype_2016",
                "source_geometry": {
                    "initialization_mode": "drifter_of_record_point",
                    "release_geometry": "legacy_drifter_of_record_point",
                },
                "member_runs": [
                    {
                        "member_id": member_id,
                        "relative_path": f"ensemble/member_{member_id:02d}.nc",
                        "start_time_utc": "2016-09-01T00:00:00Z",
                        "end_time_utc": "2016-09-04T00:00:00Z",
                        "element_count": 2000,
                        "perturbation": {},
                        "seed_initialization": {
                            "initialization_mode": "drifter_of_record_point",
                            "release_geometry": "legacy_drifter_of_record_point",
                        },
                    }
                    for member_id in range(1, service.ensemble_size + 1)
                ],
                "products": [],
            }
            manifest_path.write_text(json.dumps(manifest_payload), encoding="utf-8")

            with mock.patch("src.services.ensemble.get_ensemble_manifest_path", return_value=manifest_path):
                validation = service._validate_prototype_2016_reusable_science()

            self.assertTrue(validation["valid"])
            self.assertEqual(len(validation["member_runs"]), service.ensemble_size)
            get_case_context.cache_clear()

    def test_prototype_2016_probability_products_use_member_occupancy_not_pooled_density(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            dummy = tmp_path / "dummy.nc"
            xr.Dataset().to_netcdf(dummy)

            with mock.patch.dict(
                os.environ,
                {
                    "WORKFLOW_MODE": "prototype_2016",
                    "PHASE_1_START_DATE": "2016-09-01",
                    "RUN_NAME": "CASE_2016-09-01",
                },
                clear=False,
            ):
                get_case_context.cache_clear()
                service = EnsembleForecastService(
                    str(dummy),
                    str(dummy),
                    output_run_name="TEST_PROTOTYPE_2016_OCCUPANCY",
                )

            runtime_root = tmp_path / "runtime"
            service.base_output_dir = runtime_root
            service.output_dir = runtime_root / "ensemble"
            service.forecast_dir = runtime_root / "forecast"
            service.member_mask_dir = service.output_dir / "member_presence"
            service.loading_cache_dir = service.forecast_dir / "forcing_cache"
            service.output_dir.mkdir(parents=True, exist_ok=True)
            service.forecast_dir.mkdir(parents=True, exist_ok=True)
            service.member_mask_dir.mkdir(parents=True, exist_ok=True)
            service.loading_cache_dir.mkdir(parents=True, exist_ok=True)
            service.snapshot_hours = [24]

            member_01 = service.output_dir / "member_01.nc"
            member_02 = service.output_dir / "member_02.nc"
            member1_particles = 100
            xr.Dataset(
                data_vars={
                    "lon": (
                        ("time", "particle"),
                        np.vstack(
                            [
                                np.full(member1_particles, 117.180, dtype=np.float32),
                                np.full(member1_particles, 117.200, dtype=np.float32),
                            ]
                        ),
                    ),
                    "lat": (
                        ("time", "particle"),
                        np.vstack(
                            [
                                np.full(member1_particles, 10.415, dtype=np.float32),
                                np.full(member1_particles, 10.430, dtype=np.float32),
                            ]
                        ),
                    ),
                    "status": (("time", "particle"), np.zeros((2, member1_particles), dtype=np.int32)),
                },
                coords={
                    "time": pd.to_datetime(["2016-09-01T00:00:00", "2016-09-02T00:00:00"]),
                    "particle": np.arange(member1_particles),
                },
            ).to_netcdf(member_01)
            xr.Dataset(
                data_vars={
                    "lon": (("time", "particle"), np.array([[117.600], [117.650]], dtype=np.float32)),
                    "lat": (("time", "particle"), np.array([[10.700], [10.750]], dtype=np.float32)),
                    "status": (("time", "particle"), np.zeros((2, 1), dtype=np.int32)),
                },
                coords={
                    "time": pd.to_datetime(["2016-09-01T00:00:00", "2016-09-02T00:00:00"]),
                    "particle": [0],
                },
            ).to_netcdf(member_02)

            with mock.patch("src.services.ensemble.plot_probability_map", return_value=None), mock.patch.object(
                service,
                "_prototype_2016_display_bounds",
                return_value=(117.0, 118.0, 10.2, 10.9),
            ), mock.patch.object(
                service,
                "_generate_prototype_2016_drifter_visual_products",
                return_value=([], []),
            ):
                written_files, product_records = service._generate_prototype_probability_products(
                    [member_01, member_02],
                    10.4150,
                    117.1800,
                    nominal_start_time="2016-09-01T00:00:00Z",
                )

            probability_path = service.output_dir / "probability_24h.tif"
            p50_path = service.output_dir / "mask_p50_24h.tif"
            p90_path = service.output_dir / "mask_p90_24h.tif"
            density_path = service.output_dir / "particle_density_fraction_24h.tif"

            with rasterio.open(probability_path) as src:
                probability = src.read(1)
            with rasterio.open(p50_path) as src:
                p50 = src.read(1)
            with rasterio.open(p90_path) as src:
                p90 = src.read(1)
            with rasterio.open(density_path) as src:
                density = src.read(1)

            self.assertEqual(int(np.count_nonzero(probability > 0)), 2)
            self.assertAlmostEqual(float(np.nanmax(probability)), 0.5, places=6)
            self.assertEqual(int(np.count_nonzero(p50 > 0)), 2)
            self.assertEqual(int(np.count_nonzero(p90 > 0)), 0)
            self.assertEqual(int(np.count_nonzero(density > 0)), 2)
            self.assertGreater(float(np.nanmax(density)), 0.95)

            prob_record = next(record for record in product_records if record["product_type"] == "prob_presence")
            self.assertEqual(prob_record["contributing_member_count"], 2)
            self.assertEqual(prob_record["contributing_member_ids"], [1, 2])
            self.assertEqual(prob_record["probability_semantics"], "member_occupancy_probability")
            density_record = next(record for record in product_records if record["product_type"] == "particle_density_fraction")
            self.assertIn("not a probability-of-presence product", density_record["semantics"])
            self.assertTrue(any(path.name == "particle_density_fraction_24h.tif" for path in written_files))
            get_case_context.cache_clear()

    def test_prototype_2016_reuse_validation_rejects_incomplete_science_outputs(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            dummy = tmp_path / "dummy.nc"
            xr.Dataset().to_netcdf(dummy)

            with mock.patch.dict(
                os.environ,
                {
                    "WORKFLOW_MODE": "prototype_2016",
                    "PHASE_1_START_DATE": "2016-09-01",
                    "RUN_NAME": "CASE_2016-09-01",
                },
                clear=False,
            ):
                get_case_context.cache_clear()
                service = EnsembleForecastService(str(dummy), str(dummy))

            service.base_output_dir = tmp_path / "output" / "CASE_2016-09-01"
            service.output_dir = service.base_output_dir / "ensemble"
            service.output_dir.mkdir(parents=True, exist_ok=True)

            manifest_path = tmp_path / "ensemble_manifest.json"
            manifest_path.write_text(
                json.dumps(
                    {
                        "manifest_type": "prototype_phase2_ensemble",
                        "workflow_mode": "prototype_2016",
                        "source_geometry": {
                            "initialization_mode": "drifter_of_record_point",
                            "release_geometry": "legacy_drifter_of_record_point",
                        },
                        "member_runs": [],
                    }
                ),
                encoding="utf-8",
            )

            with mock.patch("src.services.ensemble.get_ensemble_manifest_path", return_value=manifest_path):
                validation = service._validate_prototype_2016_reusable_science()

            self.assertFalse(validation["valid"])
            self.assertIn("member records", validation["reason"])
            get_case_context.cache_clear()

    def test_prototype_2016_reuse_validation_rejects_stale_probability_semantics(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            dummy = tmp_path / "dummy.nc"
            xr.Dataset().to_netcdf(dummy)

            with mock.patch.dict(
                os.environ,
                {
                    "WORKFLOW_MODE": "prototype_2016",
                    "PHASE_1_START_DATE": "2016-09-01",
                    "RUN_NAME": "CASE_2016-09-01",
                },
                clear=False,
            ):
                get_case_context.cache_clear()
                service = EnsembleForecastService(str(dummy), str(dummy))

            service.base_output_dir = tmp_path / "output" / "CASE_2016-09-01"
            service.output_dir = service.base_output_dir / "ensemble"
            service.forecast_dir = service.base_output_dir / "forecast"
            service.member_mask_dir = service.output_dir / "member_presence"
            service.loading_cache_dir = service.forecast_dir / "forcing_cache"
            service.output_dir.mkdir(parents=True, exist_ok=True)
            service.forecast_dir.mkdir(parents=True, exist_ok=True)
            service.member_mask_dir.mkdir(parents=True, exist_ok=True)
            service.loading_cache_dir.mkdir(parents=True, exist_ok=True)

            for member_id in range(1, service.ensemble_size + 1):
                (service.output_dir / f"member_{member_id:02d}.nc").write_bytes(b"member")
            (service.output_dir / "metadata.json").write_text(
                json.dumps(
                    {
                        "probability_semantics": "pooled_particle_density_threshold",
                        "probability_semantics_version": "legacy_v0",
                    }
                ),
                encoding="utf-8",
            )
            for hour in (24, 48, 72):
                (service.output_dir / f"probability_{hour}h.nc").write_bytes(b"nc")
                (service.output_dir / f"probability_{hour}h.tif").write_bytes(b"tif")
                (service.output_dir / f"particle_density_fraction_{hour}h.nc").write_bytes(b"nc")
                (service.output_dir / f"particle_density_fraction_{hour}h.tif").write_bytes(b"tif")
                (service.output_dir / f"mask_p50_{hour}h.tif").write_bytes(b"mask")
                (service.output_dir / f"mask_p90_{hour}h.tif").write_bytes(b"mask")

            manifest_path = tmp_path / "ensemble_manifest.json"
            manifest_path.write_text(
                json.dumps(
                    {
                        "manifest_type": "prototype_phase2_ensemble",
                        "workflow_mode": "prototype_2016",
                        "source_geometry": {
                            "initialization_mode": "drifter_of_record_point",
                            "release_geometry": "legacy_drifter_of_record_point",
                        },
                        "member_runs": [
                            {
                                "member_id": member_id,
                                "relative_path": f"ensemble/member_{member_id:02d}.nc",
                                "seed_initialization": {
                                    "initialization_mode": "drifter_of_record_point",
                                    "release_geometry": "legacy_drifter_of_record_point",
                                },
                            }
                            for member_id in range(1, service.ensemble_size + 1)
                        ],
                    }
                ),
                encoding="utf-8",
            )

            with mock.patch("src.services.ensemble.get_ensemble_manifest_path", return_value=manifest_path):
                validation = service._validate_prototype_2016_reusable_science()

            self.assertFalse(validation["valid"])
            self.assertIn("member_occupancy_probability", validation["reason"])
            get_case_context.cache_clear()

    def test_prototype_2016_reuse_policy_short_circuits_member_rerun(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            dummy = tmp_path / "dummy.nc"
            xr.Dataset().to_netcdf(dummy)

            with mock.patch.dict(
                os.environ,
                {
                    "WORKFLOW_MODE": "prototype_2016",
                    "PHASE_1_START_DATE": "2016-09-01",
                    "RUN_NAME": "CASE_2016-09-01",
                    "PROTOTYPE_2016_ENSEMBLE_POLICY": "reuse_if_valid",
                },
                clear=False,
            ):
                get_case_context.cache_clear()
                service = EnsembleForecastService(str(dummy), str(dummy))
                expected_manifest = {"manifest": "reused.json", "written_files": []}
                with mock.patch.object(
                    service,
                    "_reuse_prototype_2016_ensemble_science",
                    return_value=expected_manifest,
                ) as mock_reuse, mock.patch.object(
                    service,
                    "_build_model",
                ) as mock_build_model:
                    manifest = service.run_ensemble(
                        recipe_name="cmems_era5",
                        start_lat=10.415,
                        start_lon=117.180,
                        start_time="2016-09-01T00:00:00Z",
                    )

            self.assertEqual(manifest, expected_manifest)
            mock_reuse.assert_called_once()
            mock_build_model.assert_not_called()
            get_case_context.cache_clear()

    def test_official_service_can_target_nested_recipe_sensitivity_output(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            dummy = Path(tmpdir) / "dummy.nc"
            xr.Dataset().to_netcdf(dummy)

            with mock.patch.dict(os.environ, {"WORKFLOW_MODE": "mindoro_retro_2023"}, clear=False):
                get_case_context.cache_clear()
                nested_run_name = get_recipe_sensitivity_run_name("hycom_era5")
                service = EnsembleForecastService(
                    str(dummy),
                    str(dummy),
                    wave_file=str(dummy),
                    output_run_name=nested_run_name,
                )

            self.assertIn("recipe_sensitivity", service.output_run_name)
            self.assertTrue(str(service.output_dir).endswith("recipe_sensitivity/hycom_era5/ensemble"))
            self.assertTrue(str(service.forecast_dir).endswith("recipe_sensitivity/hycom_era5/forecast"))
            self.assertTrue(str(service.audit_json_path).endswith("recipe_sensitivity/hycom_era5/forecast/phase2_loading_audit.json"))

    def test_date_composite_mask_unions_same_day_presence(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            nc_path = Path(tmpdir) / "member.nc"
            ds = xr.Dataset(
                data_vars={
                    "lon": (("time", "particle"), np.array([[0.25, np.nan], [1.25, 1.25]], dtype=np.float32)),
                    "lat": (("time", "particle"), np.array([[0.25, np.nan], [1.25, 1.25]], dtype=np.float32)),
                    "status": (("time", "particle"), np.array([[0, 1], [0, 0]], dtype=np.int16)),
                },
                coords={
                    "time": pd.to_datetime(["2023-03-06T01:00:00", "2023-03-06T12:00:00"]),
                    "particle": [0, 1],
                },
            )
            ds.to_netcdf(nc_path)

            grid = GridBuilder(region=[0.0, 2.0, 0.0, 2.0], resolution=1.0)
            composite = EnsembleForecastService._build_date_composite_mask(
                nc_path=nc_path,
                target_date="2023-03-06",
                grid=grid,
            )

            self.assertEqual(composite.shape, (2, 2))
            self.assertGreaterEqual(int(composite.sum()), 2)

    def test_shoreline_signature_mismatch_triggers_regeneration_guard(self):
        mismatches = detect_shoreline_mask_regeneration_need(
            manifest_payload={"grid": {"shoreline_mask_signature": "old-signature"}},
            manifest_path="output/CASE_MINDORO_RETRO_2023/forecast/forecast_manifest.json",
            current_signature="new-signature",
            label="forecast_manifest_shoreline_refresh_required",
        )
        self.assertEqual(len(mismatches), 1)
        self.assertEqual(mismatches[0]["label"], "forecast_manifest_shoreline_refresh_required")

        no_mismatch = detect_shoreline_mask_regeneration_need(
            manifest_payload={"grid": {"shoreline_mask_signature": "same-signature"}},
            manifest_path="output/CASE_MINDORO_RETRO_2023/forecast/forecast_manifest.json",
            current_signature="same-signature",
            label="forecast_manifest_shoreline_refresh_required",
        )
        self.assertEqual(no_mismatch, [])


if __name__ == "__main__":
    unittest.main()
