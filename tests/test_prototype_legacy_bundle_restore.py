import json
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import numpy as np
import pandas as pd
import yaml

from src.models.results import ValidationResult
from src.services.ingestion import DataIngestionService, compute_prototype_forcing_window
from src.services.validation import TransportValidationService
from src.utils.io import (
    get_prepared_input_specs,
    get_prototype_debug_recipe_family,
    get_transport_recipe_family_for_workflow,
)


class PrototypeLegacyBundleRestoreTests(unittest.TestCase):
    def test_launcher_matrix_marks_prototype_bundle_as_debug_only_with_best_effort_gfs(self):
        launcher_matrix = json.loads(Path("config/launcher_matrix.json").read_text(encoding="utf-8"))
        prototype_entry = next(
            entry for entry in launcher_matrix["entries"] if entry["entry_id"] == "prototype_legacy_bundle"
        )

        notes = prototype_entry["notes"]
        self.assertIn("debug/regression", notes)
        self.assertIn("best-effort", notes)
        self.assertIn("GFS", notes)

    def test_prototype_debug_recipe_family_is_explicit_and_workflow_aware(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recipes_path = Path(tmpdir) / "recipes.yaml"
            recipes_path.write_text(
                yaml.safe_dump(
                    {
                        "recipes": {
                            "cmems_ncep": {},
                            "cmems_era5": {},
                            "cmems_gfs": {},
                            "hycom_ncep": {},
                            "hycom_era5": {},
                            "hycom_gfs": {},
                            "extra_recipe": {},
                        },
                        "phase1_recipe_architecture": {
                            "prototype_debug_recipe_family": [
                                "cmems_ncep",
                                "cmems_era5",
                                "cmems_gfs",
                                "hycom_ncep",
                                "hycom_era5",
                                "hycom_gfs",
                            ],
                            "official_recipe_family": [
                                "cmems_era5",
                                "cmems_gfs",
                                "hycom_era5",
                                "hycom_gfs",
                            ],
                        },
                    },
                    sort_keys=False,
                ),
                encoding="utf-8",
            )

            self.assertEqual(
                get_prototype_debug_recipe_family(recipes_path),
                ["cmems_ncep", "cmems_era5", "cmems_gfs", "hycom_ncep", "hycom_era5", "hycom_gfs"],
            )
            self.assertEqual(
                get_transport_recipe_family_for_workflow("prototype_2016", recipes_path),
                ["cmems_ncep", "cmems_era5", "cmems_gfs", "hycom_ncep", "hycom_era5", "hycom_gfs"],
            )
            self.assertEqual(
                get_transport_recipe_family_for_workflow("prototype_2021", recipes_path),
                ["cmems_era5", "cmems_gfs", "hycom_era5", "hycom_gfs"],
            )
            self.assertEqual(
                get_transport_recipe_family_for_workflow("phase1_regional_2016_2022", recipes_path),
                ["cmems_era5", "cmems_gfs", "hycom_era5", "hycom_gfs"],
            )

    def test_prepared_input_specs_mark_prototype_gfs_as_best_effort(self):
        case_stub = SimpleNamespace(
            workflow_mode="prototype_2016",
            run_name="CASE_2016-09-01",
            is_prototype=True,
            is_official=False,
            arcgis_layers=[],
            validation_layer=SimpleNamespace(role="validation_polygon"),
        )

        with mock.patch("src.utils.io.get_case_context", return_value=case_stub):
            specs = get_prepared_input_specs(
                include_all_transport_forcing=True,
                run_name="CASE_2016-09-01",
            )

        gfs_specs = [spec for spec in specs if str(spec["path"]).endswith("gfs_wind.nc")]
        self.assertTrue(gfs_specs)
        self.assertTrue(all(spec.get("required") is False for spec in gfs_specs))

        era5_specs = [spec for spec in specs if str(spec["path"]).endswith("era5_wind.nc")]
        self.assertTrue(era5_specs)
        self.assertTrue(all(spec.get("required", True) is True for spec in era5_specs))

    def test_compute_prototype_forcing_window_preserves_plus_minus_three_hour_halo(self):
        start, end = compute_prototype_forcing_window(
            "2016-09-01T00:00:00Z",
            "2016-09-04T00:00:00Z",
            halo_hours=3.0,
        )
        self.assertEqual(start.isoformat(), "2016-08-31T21:00:00+00:00")
        self.assertEqual(end.isoformat(), "2016-09-04T03:00:00+00:00")

    def test_prototype_ingestion_records_padded_window_and_best_effort_gfs_failure(self):
        case_stub = SimpleNamespace(
            workflow_mode="prototype_2016",
            forcing_start_date="2016-09-01",
            forcing_end_date="2016-09-04",
            forcing_start_utc="2016-09-01T00:00:00Z",
            forcing_end_utc="2016-09-04T00:00:00Z",
            prototype_case_dates=("2016-09-01",),
            is_prototype=True,
            is_official=False,
            region=[115.0, 122.0, 6.0, 14.5],
            case_definition_path=None,
            drifter_required=True,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir) / "data"
            prepared_manifest_path = output_dir / "prepared_manifest.csv"
            prepared_manifest_path.parent.mkdir(parents=True, exist_ok=True)
            prepared_manifest_path.write_text("file_path,source,creation_time,workflow_mode\n", encoding="utf-8")

            with mock.patch("src.services.ingestion.get_case_context", return_value=case_stub), \
                mock.patch("src.services.ingestion.GridBuilder", return_value=object()), \
                mock.patch("src.services.ingestion.RUN_NAME", "CASE_2016-09-01"), \
                mock.patch.object(DataIngestionService, "write_prepared_input_manifest", return_value=prepared_manifest_path), \
                mock.patch.object(DataIngestionService, "download_drifters", return_value="downloaded"), \
                mock.patch.object(DataIngestionService, "download_hycom", return_value="downloaded"), \
                mock.patch.object(DataIngestionService, "download_cmems", return_value="downloaded"), \
                mock.patch.object(DataIngestionService, "download_cmems_wave", return_value="downloaded"), \
                mock.patch.object(DataIngestionService, "download_era5", return_value="downloaded"), \
                mock.patch.object(DataIngestionService, "download_ncep", return_value="downloaded"), \
                mock.patch.object(DataIngestionService, "download_arcgis_layers", return_value="downloaded"), \
                mock.patch.object(
                    DataIngestionService,
                    "download_gfs",
                    return_value={"status": "best_effort_failed", "error": "catalog timeout"},
                ):
                service = DataIngestionService(output_dir=str(output_dir))
                result = service.run()

            manifest_path = Path(result["download_manifest"])
            with open(manifest_path, "r", encoding="utf-8") as handle:
                payload = json.load(handle)
            manifest = payload["CASE_2016-09-01"]

            self.assertEqual(manifest["downloads"]["gfs"]["status"], "best_effort_failed")
            self.assertEqual(
                manifest["config"]["effective_forcing_start_utc"],
                "2016-08-31T21:00:00Z",
            )
            self.assertEqual(
                manifest["config"]["effective_forcing_end_utc"],
                "2016-09-04T03:00:00Z",
            )

    def test_validation_can_continue_when_only_gfs_backed_prototype_recipe_is_invalid(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recipes_path = Path(tmpdir) / "recipes.yaml"
            recipes_path.write_text(
                yaml.safe_dump(
                    {
                        "recipes": {
                            "cmems_gfs": {
                                "currents_file": "cmems_curr.nc",
                                "wind_file": "gfs_wind.nc",
                                "wave_file": "cmems_wave.nc",
                                "duration_hours": 72,
                                "time_step_minutes": 60,
                            },
                            "cmems_era5": {
                                "currents_file": "cmems_curr.nc",
                                "wind_file": "era5_wind.nc",
                                "wave_file": "cmems_wave.nc",
                                "duration_hours": 72,
                                "time_step_minutes": 60,
                            },
                        }
                    },
                    sort_keys=False,
                ),
                encoding="utf-8",
            )
            drifter_df = pd.DataFrame(
                {
                    "time": ["2016-09-01T00:00:00Z", "2016-09-01T06:00:00Z"],
                    "lat": [10.0, 10.1],
                    "lon": [117.0, 117.1],
                    "ID": ["A", "A"],
                }
            )
            service = TransportValidationService(str(recipes_path))

            def fake_run_single_recipe(**kwargs):
                recipe_name = kwargs["recipe_name"]
                if recipe_name == "cmems_gfs":
                    return {
                        "audit": {
                            "case_name": "CASE_2016-09-01",
                            "recipe": recipe_name,
                            "validity_flag": "invalid",
                            "invalidity_reason": "Missing intended wind file: gfs_wind.nc",
                        },
                        "result": None,
                    }
                return {
                    "audit": {
                        "case_name": "CASE_2016-09-01",
                        "recipe": recipe_name,
                        "validity_flag": "valid",
                        "invalidity_reason": "",
                    },
                    "result": ValidationResult(recipe_name=recipe_name, ncs_score=0.25, map_file=None),
                }

            with mock.patch(
                "src.services.validation.get_forcing_files",
                side_effect=lambda recipe_name, *_args, **_kwargs: {
                    "recipe": recipe_name,
                    "currents": Path(tmpdir) / "cmems_curr.nc",
                    "wind": Path(tmpdir) / ("gfs_wind.nc" if recipe_name.endswith("gfs") else "era5_wind.nc"),
                    "wave": Path(tmpdir) / "cmems_wave.nc",
                    "duration_hours": 72,
                    "time_step_minutes": 60,
                    "description": recipe_name,
                },
            ), mock.patch.object(service, "_run_single_recipe", side_effect=fake_run_single_recipe):
                rankings = service.run_validation(
                    drifter_df,
                    output_dir=str(Path(tmpdir) / "validation"),
                    recipe_names=["cmems_gfs", "cmems_era5"],
                )

            self.assertEqual(rankings["recipe"].tolist(), ["cmems_era5"])

    def test_validation_still_fails_if_no_prototype_recipe_remains_valid(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            recipes_path = Path(tmpdir) / "recipes.yaml"
            recipes_path.write_text(
                yaml.safe_dump(
                    {
                        "recipes": {
                            "cmems_gfs": {
                                "currents_file": "cmems_curr.nc",
                                "wind_file": "gfs_wind.nc",
                                "wave_file": "cmems_wave.nc",
                                "duration_hours": 72,
                                "time_step_minutes": 60,
                            }
                        }
                    },
                    sort_keys=False,
                ),
                encoding="utf-8",
            )
            drifter_df = pd.DataFrame(
                {
                    "time": ["2016-09-01T00:00:00Z", "2016-09-01T06:00:00Z"],
                    "lat": [10.0, 10.1],
                    "lon": [117.0, 117.1],
                    "ID": ["A", "A"],
                }
            )
            service = TransportValidationService(str(recipes_path))

            with mock.patch(
                "src.services.validation.get_forcing_files",
                return_value={
                    "recipe": "cmems_gfs",
                    "currents": Path(tmpdir) / "cmems_curr.nc",
                    "wind": Path(tmpdir) / "gfs_wind.nc",
                    "wave": Path(tmpdir) / "cmems_wave.nc",
                    "duration_hours": 72,
                    "time_step_minutes": 60,
                    "description": "cmems_gfs",
                },
            ), mock.patch.object(
                service,
                "_run_single_recipe",
                return_value={
                    "audit": {
                        "case_name": "CASE_2016-09-01",
                        "recipe": "cmems_gfs",
                        "validity_flag": "invalid",
                        "invalidity_reason": "Missing intended wind file: gfs_wind.nc",
                    },
                    "result": None,
                },
            ):
                with self.assertRaises(RuntimeError):
                    service.run_validation(
                        drifter_df,
                        output_dir=str(Path(tmpdir) / "validation"),
                        recipe_names=["cmems_gfs"],
                    )

    def test_prototype_2021_downloads_only_the_exact_configured_segment_rows(self):
        case_stub = SimpleNamespace(
            workflow_mode="prototype_2021",
            forcing_start_date="2021-03-05",
            forcing_end_date="2021-03-08",
            forcing_start_utc="2021-03-05T18:00:00Z",
            forcing_end_utc="2021-03-08T18:00:00Z",
            release_start_utc="2021-03-05T18:00:00Z",
            simulation_end_utc="2021-03-08T18:00:00Z",
            prototype_case_dates=(),
            drifter_mode="fixed_drifter_segment_window",
            configured_drifter_id="300534060352020",
            is_prototype=True,
            is_official=False,
            region=[119.5, 124.5, 11.5, 16.5],
            case_definition_path=None,
            drifter_required=True,
            run_name="CASE_20210305T180000Z",
        )
        times = pd.date_range("2021-03-05T18:00:00Z", "2021-03-08T18:00:00Z", freq="6h", tz="UTC")
        exact_df = pd.DataFrame(
            {
                "time (UTC)": [value.strftime("%Y-%m-%dT%H:%M:%SZ") for value in times],
                "latitude (degrees_north)": np.linspace(14.422, 15.300, len(times)),
                "longitude (degrees_east)": np.linspace(124.486, 123.416, len(times)),
                "ID": ["300534060352020"] * len(times),
                "ve": np.zeros(len(times)),
                "vn": np.zeros(len(times)),
            }
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir) / "data"
            with mock.patch("src.services.ingestion.get_case_context", return_value=case_stub), \
                mock.patch("src.services.ingestion.GridBuilder", return_value=object()), \
                mock.patch("src.services.ingestion.RUN_NAME", "CASE_20210305T180000Z"), \
                mock.patch("src.services.ingestion.ERDDAP") as erddap_cls:
                erddap_cls.return_value.to_pandas.return_value = exact_df
                service = DataIngestionService(output_dir=str(output_dir))
                result = service.download_drifters()

            saved = pd.read_csv(result)
            self.assertEqual(len(saved), len(times))
            self.assertEqual(saved["ID"].astype(str).unique().tolist(), ["300534060352020"])
            self.assertEqual(saved["time"].iloc[0], "2021-03-05T18:00:00Z")
            self.assertEqual(saved["time"].iloc[-1], "2021-03-08T18:00:00Z")

    def test_prototype_2021_rejects_wrong_drifter_identity(self):
        case_stub = SimpleNamespace(
            workflow_mode="prototype_2021",
            forcing_start_date="2021-03-05",
            forcing_end_date="2021-03-08",
            forcing_start_utc="2021-03-05T18:00:00Z",
            forcing_end_utc="2021-03-08T18:00:00Z",
            release_start_utc="2021-03-05T18:00:00Z",
            simulation_end_utc="2021-03-08T18:00:00Z",
            prototype_case_dates=(),
            drifter_mode="fixed_drifter_segment_window",
            configured_drifter_id="300534060352020",
            is_prototype=True,
            is_official=False,
            region=[119.5, 124.5, 11.5, 16.5],
            case_definition_path=None,
            drifter_required=True,
            run_name="CASE_20210305T180000Z",
        )
        wrong_df = pd.DataFrame(
            {
                "time (UTC)": ["2021-03-05T18:00:00Z"],
                "latitude (degrees_north)": [14.422],
                "longitude (degrees_east)": [124.486],
                "ID": ["WRONG_ID"],
                "ve": [0.0],
                "vn": [0.0],
            }
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir) / "data"
            with mock.patch("src.services.ingestion.get_case_context", return_value=case_stub), \
                mock.patch("src.services.ingestion.GridBuilder", return_value=object()), \
                mock.patch("src.services.ingestion.RUN_NAME", "CASE_20210305T180000Z"), \
                mock.patch("src.services.ingestion.ERDDAP") as erddap_cls:
                erddap_cls.return_value.to_pandas.return_value = wrong_df
                service = DataIngestionService(output_dir=str(output_dir))
                with self.assertRaises(RuntimeError):
                    service.download_drifters()


if __name__ == "__main__":
    unittest.main()
