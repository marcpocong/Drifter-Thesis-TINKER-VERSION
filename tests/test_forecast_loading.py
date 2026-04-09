import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import numpy as np
import pandas as pd
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
