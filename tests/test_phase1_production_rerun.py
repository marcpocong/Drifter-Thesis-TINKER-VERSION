import json
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pandas as pd
import xarray as xr
import yaml

from src.core.case_context import get_case_context
from src.services.phase1_production_rerun import Phase1ProductionRerunService, _apply_wind_cf_metadata
from src.services.validation import TransportValidationService


def _write_yaml(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")


def _case_context_stub() -> SimpleNamespace:
    return SimpleNamespace(
        workflow_mode="phase1_regional_2016_2022",
        workflow_flavor="historical/regional validation mode",
        transport_track="historical/regional transport validation using strict drogued-only non-overlapping 72 h drifter segments",
        is_historical_regional=True,
        is_official=False,
        is_prototype=False,
        active_domain_name="phase1_validation_box",
        region=[119.5, 124.5, 11.5, 16.5],
        phase1_validation_box=[119.5, 124.5, 11.5, 16.5],
        mindoro_case_domain=[115.0, 122.0, 6.0, 14.5],
        legacy_prototype_display_domain=[115.0, 122.0, 6.0, 14.5],
        run_name="phase1_production_rerun",
    )


def _build_test_repo(root: Path) -> None:
    _write_yaml(
        root / "config" / "phase1_regional_2016_2022.yaml",
        {
            "workflow_mode": "phase1_regional_2016_2022",
            "workflow_track": "historical_regional_validation",
            "case_id": "phase1_production_rerun",
            "phase1_validation_box": [119.5, 124.5, 11.5, 16.5],
            "drifter_acquisition_halo_degrees": 3.0,
            "forcing_bbox_halo_degrees": 0.5,
            "output_root": "output/phase1_production_rerun",
            "historical_window": {
                "start_utc": "2016-01-01T00:00:00Z",
                "end_utc": "2022-12-31T23:59:59Z",
            },
            "segment_policy": {
                "horizon_hours": 72,
                "timestep_hours": 6,
            },
            "transport_settings": {
                "direct_wind_drift_factor": 0.02,
                "enable_stokes_drift": True,
                "horizontal_diffusivity_m2s": 0.0,
                "weathering_enabled": False,
                "current_uncertainty": 0.0,
                "require_wave_stokes_reader": True,
            },
            "drifter": {
                "server": "https://osmc.noaa.gov/erddap",
                "dataset_id": "drifter_6hour_qc",
                "required_fields": [
                    "time",
                    "latitude",
                    "longitude",
                    "ID",
                    "ve",
                    "vn",
                    "drogue_lost_date",
                    "deploy_date",
                    "DrogueType",
                    "DrogueLength",
                    "DrogueDetectSensor",
                ],
            },
            "phase1_recipe_family": [
                "cmems_era5",
                "cmems_gfs",
                "hycom_era5",
                "hycom_gfs",
            ],
        },
    )
    _write_yaml(
        root / "config" / "recipes.yaml",
        {
            "recipes": {
                "cmems_era5": {"currents_file": "cmems_curr.nc", "wind_file": "era5_wind.nc", "wave_file": "cmems_wave.nc", "duration_hours": 72, "time_step_minutes": 60},
                "cmems_gfs": {"currents_file": "cmems_curr.nc", "wind_file": "gfs_wind.nc", "wave_file": "cmems_wave.nc", "duration_hours": 72, "time_step_minutes": 60},
                "hycom_era5": {"currents_file": "hycom_curr.nc", "wind_file": "era5_wind.nc", "wave_file": "cmems_wave.nc", "duration_hours": 72, "time_step_minutes": 60},
                "hycom_gfs": {"currents_file": "hycom_curr.nc", "wind_file": "gfs_wind.nc", "wave_file": "cmems_wave.nc", "duration_hours": 72, "time_step_minutes": 60},
                "cmems_ncep": {"currents_file": "cmems_curr.nc", "wind_file": "ncep_wind.nc", "wave_file": "cmems_wave.nc", "duration_hours": 72, "time_step_minutes": 60},
                "hycom_ncep": {"currents_file": "hycom_curr.nc", "wind_file": "ncep_wind.nc", "wave_file": "cmems_wave.nc", "duration_hours": 72, "time_step_minutes": 60},
            },
            "phase1_recipe_architecture": {
                "official_recipe_family": ["cmems_era5", "cmems_gfs", "hycom_era5", "hycom_gfs"],
                "legacy_recipe_name_aliases": {
                    "cmems_ncep": {"chapter3_target_recipe": "cmems_gfs", "status": "legacy_name_only"},
                    "hycom_ncep": {"chapter3_target_recipe": "hycom_gfs", "status": "legacy_name_only"},
                },
            },
        },
    )
    _write_yaml(
        root / "config" / "phase1_baseline_selection.yaml",
        {
            "baseline_id": "baseline_v1",
            "selected_recipe": "cmems_era5",
            "source_kind": "frozen_historical_artifact",
            "status_flag": "valid",
            "valid": True,
            "provisional": False,
            "rerun_required": False,
        },
    )


class Phase1ProductionRerunTests(unittest.TestCase):
    def tearDown(self):
        get_case_context.cache_clear()

    def test_case_context_recognizes_historical_regional_lane(self):
        with mock.patch.dict(os.environ, {"WORKFLOW_MODE": "phase1_regional_2016_2022"}, clear=False):
            get_case_context.cache_clear()
            case = get_case_context()

        self.assertTrue(case.is_historical_regional)
        self.assertFalse(case.is_official)
        self.assertEqual(case.workflow_flavor, "historical/regional validation mode")
        self.assertIn("strict drogued-only", case.transport_track)

    def test_segment_registry_enforces_strict_gate_and_non_overlap(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            _build_test_repo(root)

            times = pd.date_range("2017-01-01T00:00:00Z", periods=20, freq="6h", tz="UTC")
            short_times = pd.date_range("2017-02-01T00:00:00Z", periods=10, freq="6h", tz="UTC")
            frames = [
                pd.DataFrame(
                    {
                        "time": times,
                        "lat": 12.0,
                        "lon": 121.0,
                        "ID": "A",
                        "ve": 0.0,
                        "vn": 0.0,
                        "drogue_lost_date": pd.Timestamp("2018-01-01T00:00:00Z"),
                        "deploy_date": pd.Timestamp("2016-12-01T00:00:00Z"),
                        "err_lat": 0.0,
                        "err_lon": 0.0,
                        "DrogueType": "holey_sock",
                        "DrogueLength": "15m",
                        "DrogueDetectSensor": "yes",
                    }
                ),
                pd.DataFrame(
                    {
                        "time": short_times,
                        "lat": 12.0,
                        "lon": 121.0,
                        "ID": "B",
                        "ve": 0.0,
                        "vn": 0.0,
                        "drogue_lost_date": pd.Timestamp("2018-01-01T00:00:00Z"),
                        "deploy_date": pd.Timestamp("2016-12-01T00:00:00Z"),
                        "err_lat": 0.0,
                        "err_lon": 0.0,
                        "DrogueType": "holey_sock",
                        "DrogueLength": "15m",
                        "DrogueDetectSensor": "yes",
                    }
                ),
                pd.DataFrame(
                    {
                        "time": times,
                        "lat": 12.0,
                        "lon": 121.0,
                        "ID": "C",
                        "ve": 0.0,
                        "vn": 0.0,
                        "drogue_lost_date": pd.Timestamp("2017-01-02T00:00:00Z"),
                        "deploy_date": pd.Timestamp("2016-12-01T00:00:00Z"),
                        "err_lat": 0.0,
                        "err_lon": 0.0,
                        "DrogueType": "holey_sock",
                        "DrogueLength": "15m",
                        "DrogueDetectSensor": "yes",
                    }
                ),
                pd.DataFrame(
                    {
                        "time": times,
                        "lat": 18.0,
                        "lon": 126.0,
                        "ID": "D",
                        "ve": 0.0,
                        "vn": 0.0,
                        "drogue_lost_date": pd.Timestamp("2018-01-01T00:00:00Z"),
                        "deploy_date": pd.Timestamp("2016-12-01T00:00:00Z"),
                        "err_lat": 0.0,
                        "err_lon": 0.0,
                        "DrogueType": "holey_sock",
                        "DrogueLength": "15m",
                        "DrogueDetectSensor": "yes",
                    }
                ),
            ]
            drifter_df = pd.concat(frames, ignore_index=True)

            with mock.patch("src.services.phase1_production_rerun.get_case_context", return_value=_case_context_stub()):
                service = Phase1ProductionRerunService(repo_root=root)

            registry = service._build_segment_registry(drifter_df)

            accepted = registry[registry["segment_status"] == "accepted"]
            rejected_reasons = set(registry.loc[registry["segment_status"] == "rejected", "rejection_reason"])

            self.assertGreaterEqual(len(accepted), 1)
            self.assertIn("overlaps_prior_accepted_window", rejected_reasons)
            self.assertIn("insufficient_duration", rejected_reasons)
            self.assertIn("drogue_lost_within_window", rejected_reasons)
            self.assertIn("outside_phase1_validation_box", rejected_reasons)

    def test_run_stages_candidate_without_overwriting_canonical_baseline(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            _build_test_repo(root)

            accepted_df = pd.DataFrame(
                [
                    {
                        "segment_id": "A_20170101T000000Z_20170104T000000Z",
                        "segment_status": "accepted",
                        "rejection_reason": "",
                        "drifter_id": "A",
                        "start_time_utc": "2017-01-01T00:00:00+00:00",
                        "end_time_utc": "2017-01-04T00:00:00+00:00",
                        "month_key": "201701",
                    }
                ]
            )
            rejected_df = pd.DataFrame(
                [
                    {
                        "segment_id": "B_20170101T000000Z_20170104T000000Z",
                        "segment_status": "rejected",
                        "rejection_reason": "outside_phase1_validation_box",
                        "drifter_id": "B",
                        "start_time_utc": "2017-01-01T00:00:00+00:00",
                        "end_time_utc": "2017-01-04T00:00:00+00:00",
                        "month_key": "201701",
                    }
                ]
            )
            registry_df = pd.concat([accepted_df, rejected_df], ignore_index=True)
            loading_audit_df = pd.DataFrame(
                [
                    {
                        "case_name": "A_20170101T000000Z_20170104T000000Z",
                        "recipe": "cmems_era5",
                        "validity_flag": "valid",
                        "status_flag": "valid",
                        "hard_fail": False,
                        "hard_fail_reason": "",
                        "invalidity_reason": "",
                        "ncs_score": 0.1,
                    },
                    {
                        "case_name": "A_20170101T000000Z_20170104T000000Z",
                        "recipe": "cmems_gfs",
                        "validity_flag": "valid",
                        "status_flag": "valid",
                        "hard_fail": False,
                        "hard_fail_reason": "",
                        "invalidity_reason": "",
                        "ncs_score": 0.2,
                    },
                    {
                        "case_name": "A_20170101T000000Z_20170104T000000Z",
                        "recipe": "hycom_era5",
                        "validity_flag": "valid",
                        "status_flag": "valid",
                        "hard_fail": False,
                        "hard_fail_reason": "",
                        "invalidity_reason": "",
                        "ncs_score": 0.3,
                    },
                    {
                        "case_name": "A_20170101T000000Z_20170104T000000Z",
                        "recipe": "hycom_gfs",
                        "validity_flag": "valid",
                        "status_flag": "valid",
                        "hard_fail": False,
                        "hard_fail_reason": "",
                        "invalidity_reason": "",
                        "ncs_score": 0.4,
                    },
                ]
            )
            segment_metrics_df = pd.DataFrame(
                [
                    {"segment_id": "A_20170101T000000Z_20170104T000000Z", "drifter_id": "A", "start_time_utc": "2017-01-01T00:00:00+00:00", "end_time_utc": "2017-01-04T00:00:00+00:00", "month_key": "201701", "recipe": "cmems_era5", "validity_flag": "valid", "status_flag": "valid", "hard_fail": False, "hard_fail_reason": "", "invalidity_reason": "", "ncs_score": 0.1, "actual_current_reader": "", "actual_wind_reader": "", "actual_wave_reader": "", "wave_loading_status": "loaded", "current_fallback_used": False, "wind_fallback_used": False, "wave_fallback_used": False, "recipe_family": "official_phase1_production", "is_gfs_recipe": False},
                    {"segment_id": "A_20170101T000000Z_20170104T000000Z", "drifter_id": "A", "start_time_utc": "2017-01-01T00:00:00+00:00", "end_time_utc": "2017-01-04T00:00:00+00:00", "month_key": "201701", "recipe": "cmems_gfs", "validity_flag": "valid", "status_flag": "valid", "hard_fail": False, "hard_fail_reason": "", "invalidity_reason": "", "ncs_score": 0.2, "actual_current_reader": "", "actual_wind_reader": "", "actual_wave_reader": "", "wave_loading_status": "loaded", "current_fallback_used": False, "wind_fallback_used": False, "wave_fallback_used": False, "recipe_family": "official_phase1_production", "is_gfs_recipe": True},
                    {"segment_id": "A_20170101T000000Z_20170104T000000Z", "drifter_id": "A", "start_time_utc": "2017-01-01T00:00:00+00:00", "end_time_utc": "2017-01-04T00:00:00+00:00", "month_key": "201701", "recipe": "hycom_era5", "validity_flag": "valid", "status_flag": "valid", "hard_fail": False, "hard_fail_reason": "", "invalidity_reason": "", "ncs_score": 0.3, "actual_current_reader": "", "actual_wind_reader": "", "actual_wave_reader": "", "wave_loading_status": "loaded", "current_fallback_used": False, "wind_fallback_used": False, "wave_fallback_used": False, "recipe_family": "official_phase1_production", "is_gfs_recipe": False},
                    {"segment_id": "A_20170101T000000Z_20170104T000000Z", "drifter_id": "A", "start_time_utc": "2017-01-01T00:00:00+00:00", "end_time_utc": "2017-01-04T00:00:00+00:00", "month_key": "201701", "recipe": "hycom_gfs", "validity_flag": "valid", "status_flag": "valid", "hard_fail": False, "hard_fail_reason": "", "invalidity_reason": "", "ncs_score": 0.4, "actual_current_reader": "", "actual_wind_reader": "", "actual_wave_reader": "", "wave_loading_status": "loaded", "current_fallback_used": False, "wind_fallback_used": False, "wave_fallback_used": False, "recipe_family": "official_phase1_production", "is_gfs_recipe": True},
                ]
            )
            recipe_summary_df = pd.DataFrame(
                [
                    {"recipe": "cmems_era5", "recipe_rank_pool": "official_phase1_production", "segment_count": 1, "valid_segment_count": 1, "invalid_segment_count": 0, "mean_ncs_score": 0.1, "median_ncs_score": 0.1, "std_ncs_score": 0.0, "min_ncs_score": 0.1, "max_ncs_score": 0.1, "is_gfs_recipe": False},
                    {"recipe": "cmems_gfs", "recipe_rank_pool": "official_phase1_production", "segment_count": 1, "valid_segment_count": 1, "invalid_segment_count": 0, "mean_ncs_score": 0.2, "median_ncs_score": 0.2, "std_ncs_score": 0.0, "min_ncs_score": 0.2, "max_ncs_score": 0.2, "is_gfs_recipe": True},
                    {"recipe": "hycom_era5", "recipe_rank_pool": "official_phase1_production", "segment_count": 1, "valid_segment_count": 1, "invalid_segment_count": 0, "mean_ncs_score": 0.3, "median_ncs_score": 0.3, "std_ncs_score": 0.0, "min_ncs_score": 0.3, "max_ncs_score": 0.3, "is_gfs_recipe": False},
                    {"recipe": "hycom_gfs", "recipe_rank_pool": "official_phase1_production", "segment_count": 1, "valid_segment_count": 1, "invalid_segment_count": 0, "mean_ncs_score": 0.4, "median_ncs_score": 0.4, "std_ncs_score": 0.0, "min_ncs_score": 0.4, "max_ncs_score": 0.4, "is_gfs_recipe": True},
                ]
            )
            recipe_ranking_df = recipe_summary_df.copy()
            recipe_ranking_df.insert(0, "rank", [1, 2, 3, 4])

            drifter_df = pd.DataFrame(
                [{"time": pd.Timestamp("2017-01-01T00:00:00Z"), "lat": 12.0, "lon": 121.0, "ID": "A"}]
            )
            baseline_before = (root / "config" / "phase1_baseline_selection.yaml").read_text(encoding="utf-8")

            with mock.patch("src.services.phase1_production_rerun.get_case_context", return_value=_case_context_stub()):
                service = Phase1ProductionRerunService(repo_root=root)
            service._fetch_full_drifter_pool = mock.Mock(return_value=(drifter_df, [{"month_key": "201701", "status": "cached", "row_count": 1}]))
            service._build_segment_registry = mock.Mock(return_value=registry_df)
            service._evaluate_accepted_segments = mock.Mock(return_value=(loading_audit_df, segment_metrics_df, [{"month_key": "201701"}]))
            service._build_recipe_tables = mock.Mock(return_value=(recipe_summary_df, recipe_ranking_df, "cmems_era5"))

            results = service.run()

            self.assertEqual(results["winning_recipe"], "cmems_era5")
            self.assertTrue((root / "output" / "phase1_production_rerun" / "phase1_baseline_selection_candidate.yaml").exists())
            self.assertTrue((root / "output" / "phase1_production_rerun" / "phase1_production_manifest.json").exists())
            self.assertEqual(
                (root / "config" / "phase1_baseline_selection.yaml").read_text(encoding="utf-8"),
                baseline_before,
            )

    def test_launcher_matrix_includes_phase1_production_entry(self):
        matrix = json.loads(Path("config/launcher_matrix.json").read_text(encoding="utf-8"))
        entry = next(item for item in matrix["entries"] if item["entry_id"] == "phase1_production_rerun")
        self.assertEqual(entry["workflow_mode"], "phase1_regional_2016_2022")
        self.assertEqual(entry["rerun_cost"], "expensive")
        self.assertFalse(entry["safe_default"])
        self.assertEqual(entry["steps"][0]["phase"], "phase1_production_rerun")

    def test_gfs_catalog_parser_supports_legacy_and_modern_archive_names(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            _build_test_repo(root)

            with mock.patch("src.services.phase1_production_rerun.get_case_context", return_value=_case_context_stub()):
                service = Phase1ProductionRerunService(repo_root=root)

            xml_text = """
            <catalog xmlns="http://www.unidata.ucar.edu/namespaces/thredds/InvCatalog/v1.0">
              <dataset name="root">
                <dataset name="gfsanl_4_20190920_0000_000.grb2" urlPath="model-gfs-g4-anl-files-old/201909/20190920/gfsanl_4_20190920_0000_000.grb2" />
                <dataset name="gfs_4_20210305_1800_000.grb2" urlPath="model-gfs-g4-anl-files/202103/20210305/gfs_4_20210305_1800_000.grb2" />
                <dataset name="gfs_4_20210305_1200_006.grb2" urlPath="model-gfs-g4-anl-files/202103/20210305/gfs_4_20210305_1200_006.grb2" />
                <dataset name="gfs_4_20210305_1800_003.grb2" urlPath="model-gfs-g4-anl-files/202103/20210305/gfs_4_20210305_1800_003.grb2" />
                <dataset name="gfs_4_20210305_1500_006.grb2" urlPath="model-gfs-g4-anl-files/202103/20210305/gfs_4_20210305_1500_006.grb2" />
              </dataset>
            </catalog>
            """
            parsed = service._parse_gfs_catalog(xml_text)

            self.assertIn(pd.Timestamp("2019-09-20T00:00:00Z"), parsed)
            self.assertIn(pd.Timestamp("2021-03-05T18:00:00Z"), parsed)
            self.assertIn(pd.Timestamp("2021-03-05T21:00:00Z"), parsed)
            self.assertTrue(parsed[pd.Timestamp("2021-03-05T18:00:00Z")].endswith("gfs_4_20210305_1800_000.grb2"))
            self.assertTrue(parsed[pd.Timestamp("2021-03-05T21:00:00Z")].endswith("gfs_4_20210305_1800_003.grb2"))

    def test_gfs_archived_download_prefers_http_cfgrib_before_opendap(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            _build_test_repo(root)

            with mock.patch("src.services.phase1_production_rerun.get_case_context", return_value=_case_context_stub()):
                service = Phase1ProductionRerunService(repo_root=root)

            expected = xr.Dataset(
                data_vars={
                    "x_wind": (("time", "lat", "lon"), [[[1.0]]]),
                    "y_wind": (("time", "lat", "lon"), [[[2.0]]]),
                },
                coords={"time": [pd.Timestamp("2023-03-03T12:00:00")], "lat": [12.0], "lon": [121.0]},
            )
            url = (
                "https://www.ncei.noaa.gov/thredds/dodsC/model-gfs-g4-anl-files/"
                "202303/20230303/gfs_4_20230303_1200_000.grb2"
            )
            with (
                mock.patch.object(service.gfs_downloader, "download_gfs_subset_via_http_cfgrib", return_value=expected) as http_loader,
                mock.patch.object(service.gfs_downloader, "download_gfs_subset_via_opendap") as opendap_loader,
            ):
                dataset, mode_name = service.gfs_downloader.download_gfs_subset_with_preferred_transport(
                    url=url,
                    timestamp=pd.Timestamp("2023-03-03T12:00:00Z"),
                    scratch_dir=root,
                )

            self.assertEqual(mode_name, "http_cfgrib_fallback")
            self.assertIs(dataset, expected)
            http_loader.assert_called_once()
            opendap_loader.assert_not_called()

    def test_gfs_archived_download_falls_back_to_opendap_if_http_cfgrib_fails(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            _build_test_repo(root)

            with mock.patch("src.services.phase1_production_rerun.get_case_context", return_value=_case_context_stub()):
                service = Phase1ProductionRerunService(repo_root=root)

            expected = xr.Dataset(
                data_vars={
                    "x_wind": (("time", "lat", "lon"), [[[1.0]]]),
                    "y_wind": (("time", "lat", "lon"), [[[2.0]]]),
                },
                coords={"time": [pd.Timestamp("2023-03-03T12:00:00")], "lat": [12.0], "lon": [121.0]},
            )
            url = (
                "https://www.ncei.noaa.gov/thredds/dodsC/model-gfs-g4-anl-files/"
                "202303/20230303/gfs_4_20230303_1200_000.grb2"
            )
            with (
                mock.patch.object(
                    service.gfs_downloader,
                    "download_gfs_subset_via_http_cfgrib",
                    side_effect=RuntimeError("http unavailable"),
                ) as http_loader,
                mock.patch.object(service.gfs_downloader, "download_gfs_subset_via_opendap", return_value=expected) as opendap_loader,
            ):
                dataset, mode_name = service.gfs_downloader.download_gfs_subset_with_preferred_transport(
                    url=url,
                    timestamp=pd.Timestamp("2023-03-03T12:00:00Z"),
                    scratch_dir=root,
                )

            self.assertEqual(mode_name, "opendap")
            self.assertIs(dataset, expected)
            http_loader.assert_called_once()
            opendap_loader.assert_called_once()

    def test_apply_wind_cf_metadata_sets_reader_friendly_standard_names(self):
        ds = xr.Dataset(
            data_vars={
                "x_wind": (("time", "lat", "lon"), [[[1.0, 2.0], [3.0, 4.0]]]),
                "y_wind": (("time", "lat", "lon"), [[[5.0, 6.0], [7.0, 8.0]]]),
            },
            coords={
                "time": [pd.Timestamp("2021-03-05T00:00:00")],
                "lat": [11.5, 12.0],
                "lon": [120.0, 120.5],
            },
        )

        normalized = _apply_wind_cf_metadata(ds)

        self.assertEqual(normalized["x_wind"].attrs["standard_name"], "eastward_wind")
        self.assertEqual(normalized["y_wind"].attrs["standard_name"], "northward_wind")
        self.assertEqual(normalized["lat"].attrs["standard_name"], "latitude")
        self.assertEqual(normalized["lon"].attrs["standard_name"], "longitude")

    def test_validation_normalizes_timezone_aware_drifter_times_to_utc_naive(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            _build_test_repo(root)

            service = TransportValidationService(str(root / "config" / "recipes.yaml"))
            drifter_df = pd.DataFrame(
                [
                    {
                        "time": pd.Timestamp("2017-01-01T08:00:00+08:00"),
                        "lat": 12.0,
                        "lon": 121.0,
                        "ID": "A",
                    },
                    {
                        "time": pd.Timestamp("2017-01-01T14:00:00+08:00"),
                        "lat": 12.1,
                        "lon": 121.1,
                        "ID": "A",
                    },
                ]
            )

            captured: dict[str, object] = {}

            def _fake_run_single_recipe(*, drifter_df, start_time, **kwargs):
                captured["times"] = drifter_df["time"].tolist()
                captured["start_time"] = start_time
                return {"audit": {"case_name": "segment_a", "recipe": "cmems_era5"}, "result": None}

            with mock.patch.object(service, "_run_single_recipe", side_effect=_fake_run_single_recipe):
                payload = service.run_validation_summary(
                    drifter_df=drifter_df,
                    forcing_dir=root,
                    recipe_names=["cmems_era5"],
                    output_dir=root / "validation_out",
                    keep_scratch=False,
                    verbose=False,
                )

            self.assertTrue(payload["audit_df"].empty or len(payload["audit_df"]) == 1)
            self.assertIsNotNone(captured["start_time"])
            self.assertIsNone(pd.Timestamp(captured["start_time"]).tzinfo)
            normalized_times = [pd.Timestamp(value) for value in captured["times"]]
            self.assertTrue(all(ts.tzinfo is None for ts in normalized_times))
            self.assertEqual(normalized_times[0], pd.Timestamp("2017-01-01T00:00:00"))
            self.assertEqual(normalized_times[1], pd.Timestamp("2017-01-01T06:00:00"))


if __name__ == "__main__":
    unittest.main()
