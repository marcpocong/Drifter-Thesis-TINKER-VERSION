import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import numpy as np
import pandas as pd
import xarray as xr

from src.exceptions.custom import ForcingOutagePhaseSkipped
from src.services.dwh_phase3c_scientific_forcing import (
    _acquire_hycom_current,
    attrs_mark_smoke_only,
    coverage_spans_window,
    path_is_smoke_only,
    run_dwh_phase3c_scientific_forcing_ready,
    validate_prepared_forcing_file,
)


class DWHScientificForcingReadyTests(unittest.TestCase):
    def test_coverage_window_requires_full_required_span(self):
        self.assertTrue(
            coverage_spans_window(
                "2010-05-20T00:00:00Z",
                "2010-05-24T00:00:00Z",
                "2010-05-20T00:00:00Z",
                "2010-05-23T23:59:59Z",
            )
        )
        self.assertFalse(
            coverage_spans_window(
                "2010-05-20T01:00:00Z",
                "2010-05-24T00:00:00Z",
                "2010-05-20T00:00:00Z",
                "2010-05-23T23:59:59Z",
            )
        )

    def test_smoke_only_paths_and_attrs_are_detected(self):
        self.assertTrue(path_is_smoke_only("output/CASE_DWH_RETRO_2010_72H/dwh_phase3c_forcing_adapter_and_non_scientific_smoke_forecast/prepared_forcing/current.nc"))
        self.assertTrue(path_is_smoke_only("dwh_smoke_current_non_scientific.nc"))
        self.assertTrue(attrs_mark_smoke_only({"non_scientific_smoke": "true"}))
        self.assertTrue(attrs_mark_smoke_only({"source_is_smoke_only": True}))
        self.assertFalse(path_is_smoke_only("output/CASE_DWH_RETRO_2010_72H/dwh_phase3c_scientific_forcing_ready/prepared_forcing/hycom.nc"))

    def test_validation_rejects_dataset_marked_smoke_only_before_reader_open(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "real_name_current.nc"
            times = pd.date_range("2010-05-20", "2010-05-24", freq="1h")
            ds = xr.Dataset(
                {
                    "x_sea_water_velocity": (("time", "lat", "lon"), np.zeros((len(times), 1, 1), dtype=np.float32)),
                    "y_sea_water_velocity": (("time", "lat", "lon"), np.zeros((len(times), 1, 1), dtype=np.float32)),
                },
                coords={"time": times, "lat": [28.0], "lon": [-88.0]},
                attrs={"non_scientific_smoke": "true"},
            )
            ds.to_netcdf(path)

            row = validate_prepared_forcing_file(
                path,
                "current",
                "2010-05-20T00:00:00Z",
                "2010-05-23T23:59:59Z",
                {
                    "source_role": "current",
                    "provider": "test",
                    "dataset_product_id": "test",
                    "access_method": "file",
                    "scientific_ready": False,
                    "source_is_smoke_only": False,
                    "exact_reason_if_false": "",
                },
            )

        self.assertEqual(row["reader_compatibility_status"], "rejected_smoke_attrs")
        self.assertFalse(row["scientific_ready"])
        self.assertTrue(row["source_is_smoke_only"])

    def test_continue_degraded_raises_nonfatal_skip_for_outage_blocked_stack(self):
        status_rows = [
            {
                "source_role": "current",
                "scientific_ready": True,
                "upstream_outage_detected": False,
                "missing_forcing_factor": "hycom_curr.nc",
                "dataset_product_id": "hycom",
            },
            {
                "source_role": "wind",
                "scientific_ready": False,
                "upstream_outage_detected": True,
                "missing_forcing_factor": "era5_wind.nc",
                "dataset_product_id": "era5",
                "forcing_source_budget_seconds": 300,
                "elapsed_seconds": 300.0,
                "budget_exhausted": True,
                "failure_stage": "budget_timeout",
            },
            {
                "source_role": "wave",
                "scientific_ready": True,
                "upstream_outage_detected": False,
                "missing_forcing_factor": "cmems_wave.nc",
                "dataset_product_id": "cmems_wave",
            },
        ]

        with tempfile.TemporaryDirectory() as tmp_dir:
            output_dir = Path(tmp_dir) / "dwh_phase3c_scientific_forcing_ready"
            prepared_dir = output_dir / "prepared_forcing"
            case_stub = SimpleNamespace(workflow_mode="dwh_retro_2010")
            spec_stub = SimpleNamespace()
            with mock.patch.dict(os.environ, {"FORCING_OUTAGE_POLICY": "continue_degraded"}, clear=False), mock.patch(
                "src.services.dwh_phase3c_scientific_forcing.get_case_context",
                return_value=case_stub,
            ), mock.patch(
                "src.services.dwh_phase3c_scientific_forcing.OUTPUT_DIR",
                output_dir,
            ), mock.patch(
                "src.services.dwh_phase3c_scientific_forcing.PREPARED_FORCING_DIR",
                prepared_dir,
            ), mock.patch(
                "src.services.dwh_phase3c_scientific_forcing._load_yaml",
                return_value={"forcing_bbox_halo_degrees": 0.5},
            ), mock.patch(
                "src.services.dwh_phase3c_scientific_forcing.load_dwh_scoring_grid_spec",
                return_value=spec_stub,
            ), mock.patch(
                "src.services.dwh_phase3c_scientific_forcing.derive_forcing_bbox_from_grid",
                return_value=[-89.0, -87.0, 28.0, 30.0],
            ), mock.patch(
                "src.services.dwh_phase3c_scientific_forcing._prepare_forcing_sources",
                return_value=status_rows,
            ), mock.patch(
                "src.services.dwh_phase3c_scientific_forcing._select_stack",
                return_value={"current": status_rows[0], "wind": None, "wave": status_rows[2]},
            ), mock.patch(
                "src.services.dwh_phase3c_scientific_forcing._build_prepared_manifest_rows",
                return_value=[],
            ), mock.patch(
                "src.services.dwh_phase3c_scientific_forcing._write_csv",
            ), mock.patch(
                "src.services.dwh_phase3c_scientific_forcing._write_json",
            ):
                with self.assertRaises(ForcingOutagePhaseSkipped) as context:
                    run_dwh_phase3c_scientific_forcing_ready()

        self.assertEqual(context.exception.forcing_outage_policy, "continue_degraded")
        self.assertEqual(context.exception.missing_forcing_factors, ["era5_wind.nc"])
        self.assertEqual(context.exception.budget_seconds, 300)
        self.assertEqual(context.exception.elapsed_seconds, 300.0)
        self.assertTrue(context.exception.budget_exhausted)
        self.assertEqual(context.exception.failure_stage, "budget_timeout")

    def test_input_cache_policy_force_refresh_bypasses_prepared_forcing_reuse(self):
        with tempfile.TemporaryDirectory() as tmp_dir:
            prepared_dir = Path(tmp_dir)
            output_path = prepared_dir / "hycom_gofs31_current_dwh_20100520_20100524.nc"
            output_path.write_text("stale", encoding="utf-8")

            ds = xr.Dataset(
                {
                    "water_u": (("time", "depth", "lat", "lon"), np.zeros((1, 1, 1, 1), dtype=np.float32)),
                    "water_v": (("time", "depth", "lat", "lon"), np.zeros((1, 1, 1, 1), dtype=np.float32)),
                },
                coords={
                    "time": pd.date_range("2010-05-20T00:00:00Z", periods=1, freq="1h"),
                    "depth": [0.0],
                    "lat": [28.0],
                    "lon": [-88.0],
                },
            )
            remote = mock.MagicMock()
            remote.__enter__.return_value = ds
            remote.__exit__.return_value = False

            def _write_prepared(dataset, path, metadata):
                path.write_text("fresh", encoding="utf-8")

            with mock.patch.dict(os.environ, {"INPUT_CACHE_POLICY": "force_refresh"}, clear=False), mock.patch(
                "src.services.dwh_phase3c_scientific_forcing.PREPARED_FORCING_DIR",
                prepared_dir,
            ), mock.patch(
                "src.services.dwh_phase3c_scientific_forcing.xr.open_dataset",
                return_value=remote,
            ), mock.patch(
                "src.services.dwh_phase3c_scientific_forcing._subset_by_bbox",
                side_effect=lambda dataset, bbox: dataset,
            ), mock.patch(
                "src.services.dwh_phase3c_scientific_forcing._select_surface_depth",
                side_effect=lambda dataset: dataset,
            ), mock.patch(
                "src.services.dwh_phase3c_scientific_forcing._convert_360_lon_to_180",
                side_effect=lambda dataset: dataset,
            ), mock.patch(
                "src.services.dwh_phase3c_scientific_forcing._standardize_current_dataset",
                side_effect=lambda dataset: dataset,
            ), mock.patch(
                "src.services.dwh_phase3c_scientific_forcing._write_prepared_dataset",
                side_effect=_write_prepared,
            ):
                path, note = _acquire_hycom_current(
                    bbox=[-89.0, -87.0, 28.0, 30.0],
                    required_start="2010-05-20T00:00:00Z",
                    request_end="2010-05-24T00:00:00Z",
                )

            self.assertEqual(path, output_path)
            self.assertEqual(note, "downloaded_from_hycom_thredds_opendap")
            self.assertEqual(output_path.read_text(encoding="utf-8"), "fresh")


if __name__ == "__main__":
    unittest.main()
