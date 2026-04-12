import json
import os
import io
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import numpy as np
import pandas as pd
import requests
import yaml

import src.__main__ as entrypoint

from src.core.case_context import get_case_context, get_case_log_lines
from src.exceptions.custom import BENCHMARK_SKIP_EXIT_CODE, BenchmarkCaseSkipped
from src.models.results import ValidationResult
from src.services.ingestion import DataIngestionService, compute_prototype_forcing_window
from src.utils.gfs_wind import GFSWindDownloader, GFSAcquisitionError
from src.services.validation import TransportValidationService
from src.utils.io import (
    get_prepared_input_specs,
    get_prototype_debug_recipe_family,
    get_transport_recipe_family_for_workflow,
    resolve_spill_origin,
    select_drifter_of_record,
)


class PrototypeLegacyBundleRestoreTests(unittest.TestCase):
    def test_run_benchmark_exits_with_skip_code_for_out_of_grid_case(self):
        case_stub = SimpleNamespace(drifter_required=True)
        selection = SimpleNamespace(recipe="cmems_era5")
        buffer = io.StringIO()

        with mock.patch("src.core.case_context.get_case_context", return_value=case_stub), \
            mock.patch("src.__main__.print_workflow_context"), \
            mock.patch("src.__main__.ensure_prepared_inputs"), \
            mock.patch("src.__main__.print_recipe_selection"), \
            mock.patch("src.utils.io.resolve_recipe_selection", return_value=selection), \
            mock.patch("src.utils.io.resolve_spill_origin", return_value=(16.2980, 112.6630, "2016-09-17T00:00:00Z")), \
            mock.patch("src.core.constants.RUN_NAME", "CASE_2016-09-17"), \
            mock.patch("src.services.benchmark.BenchmarkPipeline.run", side_effect=BenchmarkCaseSkipped("Out-of-grid benchmark case")):
            with redirect_stdout(buffer):
                with self.assertRaises(SystemExit) as exit_context:
                    entrypoint.run_benchmark()

        output = buffer.getvalue()
        self.assertEqual(exit_context.exception.code, BENCHMARK_SKIP_EXIT_CODE)
        self.assertIn("Benchmark skipped.", output)
        self.assertIn("Out-of-grid benchmark case", output)

    def test_prototype_legacy_phase4_wrapper_propagates_phase4_labels(self):
        case_stub = SimpleNamespace(workflow_mode="prototype_2016")
        selection = SimpleNamespace(
            recipe="cmems_era5",
            source_kind="test",
            status_flag="valid",
            valid=True,
            provisional=False,
            rerun_required=False,
            source_path=None,
            note=None,
        )
        budget_df = pd.DataFrame(
            [
                {
                    "hours_elapsed": 72,
                    "surface_pct": 40.0,
                    "evaporated_pct": 25.0,
                    "dispersed_pct": 20.0,
                    "beached_pct": 15.0,
                }
            ]
        )
        weathering_results = {
            "light": {
                "display_name": "Light oil",
                "budget_df": budget_df,
                "nc_path": "fake.nc",
                "csv_path": "fake.csv",
                "qc": {"passed": True, "max_deviation_pct": 0.1},
            }
        }
        refined_result = {
            "display_name": "Refined Oil",
            "budget_df": budget_df,
            "nc_path": "fake_refined.nc",
            "csv_path": "fake_refined.csv",
            "qc": {"passed": True, "max_deviation_pct": 0.1},
        }

        with mock.patch("src.core.case_context.get_case_context", return_value=case_stub), mock.patch(
            "src.__main__.ensure_prepared_inputs"
        ) as mock_prepare, mock.patch(
            "src.__main__.print_recipe_selection"
        ), mock.patch(
            "src.__main__.print_workflow_context"
        ), mock.patch(
            "src.helpers.plotting.plot_diagnostic_forcing"
        ) as mock_plot, mock.patch(
            "src.utils.io.resolve_recipe_selection",
            return_value=selection,
        ), mock.patch(
            "src.utils.io.resolve_spill_origin",
            return_value=(13.0, 121.0, "2016-09-01T00:00:00Z"),
        ), mock.patch(
            "src.services.diagnostics.run_diagnostics",
            return_value={"ok": True},
        ) as mock_diag, mock.patch(
            "src.services.weathering.run_weathering",
            return_value=weathering_results,
        ) as mock_weathering, mock.patch(
            "src.services.weathering.run_refined_weathering",
            return_value=refined_result,
        ) as mock_refined, mock.patch(
            "builtins.open",
            mock.mock_open(read_data="shoreline:\n  enabled: false\n"),
        ), mock.patch(
            "yaml.safe_load",
            return_value={"shoreline": {"enabled": False}},
        ):
            entrypoint.run_prototype_legacy_phase4_weathering()

        mock_prepare.assert_called_once()
        self.assertEqual(mock_prepare.call_args.kwargs["phase_label"], "Phase 4")
        self.assertEqual(
            mock_diag.call_args.kwargs["diagnostics_label"],
            "Phase 4 – Pre-Flight Diagnostics",
        )
        self.assertEqual(mock_plot.call_args.kwargs["title"], "Phase 4 – Pre-Flight Diagnostics")
        self.assertEqual(mock_weathering.call_args.kwargs["phase_label"], "Phase 4")
        self.assertEqual(mock_refined.call_args.kwargs["phase_label"], "Phase 4")
        self.assertEqual(
            mock_refined.call_args.kwargs["refined_stage_label"],
            "Legacy refined oil appendix",
        )

    def test_launcher_matrix_marks_prototype_bundle_as_debug_only_with_best_effort_gfs(self):
        launcher_matrix = json.loads(Path("config/launcher_matrix.json").read_text(encoding="utf-8"))
        prototype_entry = next(
            entry for entry in launcher_matrix["entries"] if entry["entry_id"] == "prototype_legacy_bundle"
        )

        notes = prototype_entry["notes"]
        self.assertIn("debug/regression", notes)
        self.assertIn("best-effort", notes)
        self.assertIn("GFS", notes)
        self.assertIn("Phase 3A -> Phase 4", notes)
        self.assertIn("no thesis-facing prototype_2016 Phase 3B or Phase 3C", notes)
        phases = [step["phase"] for step in prototype_entry["steps"]]
        self.assertEqual(
            phases,
            [
                "prep",
                "1_2",
                "benchmark",
                "prototype_pygnome_similarity_summary",
                "prototype_legacy_phase4_weathering",
                "prototype_legacy_final_figures",
            ],
        )

    def test_select_drifter_of_record_matches_phase1_rule(self):
        drifter_df = pd.DataFrame(
            {
                "time": [
                    "2016-09-01T03:00:00Z",
                    "2016-09-01T00:00:00Z",
                    "2016-09-01T01:00:00Z",
                    "2016-09-01T02:00:00Z",
                ],
                "lat": [10.3, 10.0, 11.1, 10.2],
                "lon": [117.3, 117.0, 118.1, 117.2],
                "ID": ["A", "A", "B", "A"],
            }
        )

        selection = select_drifter_of_record(drifter_df)

        self.assertEqual(selection["selected_id"], "A")
        self.assertEqual(selection["point_count"], 3)
        self.assertEqual(selection["start_time"], "2016-09-01T00:00:00Z")
        self.assertAlmostEqual(selection["start_lat"], 10.0)
        self.assertAlmostEqual(selection["start_lon"], 117.0)

    def test_resolve_spill_origin_uses_drifter_of_record_for_prototype_2016_even_when_provenance_exists(self):
        case_stub = SimpleNamespace(
            workflow_mode="prototype_2016",
            is_official=False,
            release_start_utc="2016-09-01T00:00:00Z",
        )
        drifter_df = pd.DataFrame(
            {
                "time": ["2016-09-01T03:00:00Z", "2016-09-01T00:00:00Z", "2016-09-01T01:00:00Z"],
                "lat": [10.3, 10.0, 10.1],
                "lon": [117.3, 117.0, 117.1],
                "ID": ["A", "A", "A"],
            }
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            drifter_path = Path(tmpdir) / "drifters_noaa.csv"
            drifter_path.write_text("time,lat,lon,ID\n", encoding="utf-8")

            with mock.patch("src.utils.io.get_case_context", return_value=case_stub), \
                mock.patch("src.utils.io.load_drifter_data", return_value=drifter_df), \
                mock.patch("src.utils.io.resolve_provenance_source_point", return_value=(99.0, 100.0)):
                lat, lon, time_str = resolve_spill_origin(drifter_path)

        self.assertEqual((lat, lon, time_str), (10.0, 117.0, "2016-09-01T00:00:00Z"))

    def test_prototype_2016_case_context_reports_drifter_point_initialization(self):
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
            case = get_case_context()
            lines = get_case_log_lines()

        self.assertEqual(case.initialization_mode, "drifter_of_record_point")
        self.assertEqual(case.source_point_role, "drifter_of_record_release_point")
        self.assertEqual(case.release_reference, "phase1_drifter_of_record")
        self.assertEqual(case.active_domain_name, "prototype_2016_case_local_domain")
        self.assertTrue(any("initialization_mode: drifter_of_record_point" in line for line in lines))
        self.assertTrue(any("source_point_role  : drifter_of_record_release_point" in line for line in lines))
        get_case_context.cache_clear()

    def test_prototype_2016_case_context_derives_case_local_domain_from_drifter_track(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            drifter_path = Path(tmpdir) / "drifters_noaa.csv"
            pd.DataFrame(
                {
                    "time": [
                        "2016-09-17T00:00:00Z",
                        "2016-09-18T00:00:00Z",
                        "2016-09-19T00:00:00Z",
                        "2016-09-20T00:00:00Z",
                    ],
                    "lat": [16.298, 16.420, 16.610, 16.770],
                    "lon": [112.663, 112.920, 113.180, 113.410],
                    "ID": ["DRIFTER_A", "DRIFTER_A", "DRIFTER_A", "DRIFTER_A"],
                }
            ).to_csv(drifter_path, index=False)

            with mock.patch.dict(
                os.environ,
                {
                    "WORKFLOW_MODE": "prototype_2016",
                    "PHASE_1_START_DATE": "2016-09-17",
                    "RUN_NAME": "CASE_2016-09-17",
                },
                clear=False,
            ), mock.patch("src.core.case_context._prototype_2016_drifter_csv_path", return_value=drifter_path):
                get_case_context.cache_clear()
                case = get_case_context()

            self.assertEqual(case.active_domain_name, "prototype_2016_case_local_domain")
            self.assertGreaterEqual(case.region[1] - case.region[0], 8.0)
            self.assertGreaterEqual(case.region[3] - case.region[2], 8.0)
            self.assertLessEqual(case.region[0], 112.663)
            self.assertGreaterEqual(case.region[1], 113.410)
            self.assertLessEqual(case.region[2], 16.298)
            self.assertGreaterEqual(case.region[3], 16.770)
            get_case_context.cache_clear()

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

    def test_gfs_catalog_timeout_error_is_user_friendly(self):
        downloader = GFSWindDownloader(forcing_box=[115.0, 122.0, 6.0, 14.5])
        timeout_error = requests.exceptions.ReadTimeout(
            "HTTPSConnectionPool(host='www.ncei.noaa.gov', port=443): Read timed out. (read timeout=180)"
        )

        with mock.patch("src.utils.gfs_wind.requests.get", side_effect=timeout_error):
            with self.assertLogs("src.utils.gfs_wind", level="WARNING") as captured_logs:
                urls = downloader.discover_gfs_analysis_urls(
                    "2016-08-31T21:00:00Z",
                    "2016-09-04T03:00:00Z",
                )

        self.assertTrue(urls)
        self.assertTrue(
            any("GFS catalog for 2016-08-31 is unavailable" in log for log in captured_logs.output)
        )
        self.assertTrue(any("Retrying" in log for log in captured_logs.output))
        self.assertTrue(any("Trying direct file access" in log for log in captured_logs.output))
        self.assertTrue(any("GFS catalog stayed unavailable" in log for log in captured_logs.output))
        self.assertTrue(any("timeout" in log for log in captured_logs.output))

    def test_gfs_file_download_fallback_error_is_user_friendly(self):
        downloader = GFSWindDownloader(forcing_box=[115.0, 122.0, 6.0, 14.5])
        response = requests.Response()
        response.status_code = 503
        response.url = (
            "https://www.ncei.noaa.gov/thredds/fileServer/model-gfs-g4-anl-files-old/"
            "201608/20160830/gfsanl_4_20160830_1800_000.grb2"
        )
        http_error = requests.exceptions.HTTPError(response=response)

        with tempfile.TemporaryDirectory() as tmpdir:
            with mock.patch("src.utils.gfs_wind.requests.get", side_effect=http_error):
                with self.assertLogs("src.utils.gfs_wind", level="WARNING") as captured_logs:
                    with self.assertRaises(GFSAcquisitionError) as error_context:
                        downloader.download_gfs_subset_via_http_cfgrib(
                            url="https://www.ncei.noaa.gov/thredds/dodsC/model-gfs-g4-anl-files-old/201608/20160830/gfsanl_4_20160830_1800_000.grb2",
                            timestamp="2016-08-30T18:00:00Z",
                            scratch_dir=tmpdir,
                        )

        message = str(error_context.exception)
        self.assertIn("Direct file access failed after 1 attempt(s)", message)
        self.assertIn("HTTP 503", message)
        self.assertTrue(any("GFS direct file access failed for 2016-08-30 18:00 UTC" in log for log in captured_logs.output))
        self.assertTrue(any("No retries left" in log for log in captured_logs.output))

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
            active_domain_name="legacy_prototype_display_domain",
            region=[115.0, 122.0, 6.0, 14.5],
            phase1_validation_box=[119.5, 124.5, 11.5, 16.5],
            mindoro_case_domain=[115.0, 122.0, 6.0, 14.5],
            legacy_prototype_display_domain=[115.0, 122.0, 6.0, 14.5],
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

    def test_optional_missing_prepared_inputs_message_is_clear(self):
        import src.__main__ as entrypoint

        buffer = io.StringIO()
        with mock.patch(
            "src.utils.io.find_missing_prepared_inputs",
            return_value=[
                {
                    "label": "forcing_wind_gfs_wind.nc",
                    "path": "data/forcing/CASE_2016-09-01/gfs_wind.nc",
                    "source": "GFS",
                    "required": False,
                }
            ],
        ):
            with redirect_stdout(buffer):
                entrypoint.ensure_prepared_inputs(
                    "CASE_2016-09-01",
                    recipe_name="cmems_gfs",
                    require_drifter=True,
                    phase_label="pipeline prep stage",
                )

        output = buffer.getvalue()
        self.assertIn("Optional inputs are missing for pipeline prep stage. Continuing without them.", output)
        self.assertIn("  - GFS: data/forcing/CASE_2016-09-01/gfs_wind.nc", output)

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
            active_domain_name="legacy_prototype_display_domain",
            region=[119.5, 124.5, 11.5, 16.5],
            phase1_validation_box=[119.5, 124.5, 11.5, 16.5],
            mindoro_case_domain=[115.0, 122.0, 6.0, 14.5],
            legacy_prototype_display_domain=[119.5, 124.5, 11.5, 16.5],
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
            active_domain_name="legacy_prototype_display_domain",
            region=[119.5, 124.5, 11.5, 16.5],
            phase1_validation_box=[119.5, 124.5, 11.5, 16.5],
            mindoro_case_domain=[115.0, 122.0, 6.0, 14.5],
            legacy_prototype_display_domain=[119.5, 124.5, 11.5, 16.5],
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
