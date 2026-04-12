import unittest
import os
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import numpy as np

from src.core.case_context import get_case_context
from src.exceptions.custom import BenchmarkCaseSkipped
from src.helpers.metrics import calculate_kl_divergence
from src.services.benchmark import _prepare_density_pair_for_metrics, ensure_point_within_benchmark_grid
from src.utils.io import resolve_spill_origin


class BenchmarkMetricTests(unittest.TestCase):
    def test_kl_divergence_renormalizes_on_valid_mask(self):
        forecast = np.array([[0.6, 0.4], [0.0, 0.0]], dtype=float)
        observed = np.array([[0.3, 0.7], [10.0, 10.0]], dtype=float)
        valid_mask = np.array([[True, True], [False, False]])

        actual = calculate_kl_divergence(forecast, observed, epsilon=1e-12, valid_mask=valid_mask)

        expected_forecast = np.array([0.6, 0.4], dtype=float)
        expected_observed = np.array([0.3, 0.7], dtype=float)
        expected_forecast /= expected_forecast.sum()
        expected_observed /= expected_observed.sum()
        expected = float(np.sum(expected_observed * np.log(expected_observed / expected_forecast)))

        self.assertAlmostEqual(actual, expected, places=10)

    def test_kl_divergence_requires_positive_mass(self):
        forecast = np.zeros((2, 2), dtype=float)
        observed = np.ones((2, 2), dtype=float)
        with self.assertRaises(ValueError):
            calculate_kl_divergence(forecast, observed, valid_mask=np.ones((2, 2), dtype=bool))

    def test_benchmark_preflight_rejects_spill_origin_outside_grid(self):
        grid = SimpleNamespace(min_lon=115.0, max_lon=122.0, min_lat=6.0, max_lat=14.5)

        with self.assertRaises(BenchmarkCaseSkipped) as exc:
            ensure_point_within_benchmark_grid(
                lon=112.6630,
                lat=16.2980,
                grid=grid,
            )

        message = str(exc.exception)
        self.assertIn("112.6630E, 16.2980N", message)
        self.assertIn("outside the benchmark grid", message)
        self.assertIn("defensible Phase 3A rasters", message)

    def test_prototype_2016_case_local_context_contains_real_drifter_start_points(self):
        for date in ("2016-09-01", "2016-09-06", "2016-09-17"):
            with mock.patch.dict(
                os.environ,
                {
                    "WORKFLOW_MODE": "prototype_2016",
                    "PHASE_1_START_DATE": date,
                    "RUN_NAME": f"CASE_{date}",
                },
                clear=False,
            ):
                get_case_context.cache_clear()
                case = get_case_context()
                lat, lon, _ = resolve_spill_origin(Path("data") / "drifters" / f"CASE_{date}" / "drifters_noaa.csv")
                grid = SimpleNamespace(
                    min_lon=case.region[0],
                    max_lon=case.region[1],
                    min_lat=case.region[2],
                    max_lat=case.region[3],
                )
                ensure_point_within_benchmark_grid(lon=lon, lat=lat, grid=grid)
                self.assertEqual(case.active_domain_name, "prototype_2016_case_local_domain")
        get_case_context.cache_clear()

    def test_support_density_pair_rebuilds_blank_density_from_hits(self):
        forecast_density = np.zeros((2, 2), dtype=float)
        observed_density = np.array([[0.4, 0.6], [0.0, 0.0]], dtype=float)
        forecast_hits = np.array([[1.0, 1.0], [0.0, 0.0]], dtype=float)
        observed_hits = (observed_density > 0).astype(float)
        valid_mask = np.ones((2, 2), dtype=bool)

        metric_forecast, metric_observed, metric_mask, metadata = _prepare_density_pair_for_metrics(
            forecast_density=forecast_density,
            observed_density=observed_density,
            forecast_hits=forecast_hits,
            observed_hits=observed_hits,
            valid_mask=valid_mask,
            allow_blank_support_fallback=True,
        )

        self.assertEqual(metadata["raw_forecast_sum"], 0.0)
        self.assertEqual(metadata["raw_observed_sum"], 1.0)
        self.assertEqual(metadata["strategy"], "support_hits_density_rebuild")
        self.assertTrue(np.array_equal(metric_mask, valid_mask))
        np.testing.assert_allclose(metric_forecast, np.array([[0.5, 0.5], [0.0, 0.0]], dtype=float))
        self.assertAlmostEqual(float(metric_observed.sum()), 1.0)
        self.assertGreaterEqual(
            calculate_kl_divergence(metric_forecast, metric_observed, valid_mask=metric_mask),
            0.0,
        )

    def test_support_density_pair_uses_positive_support_fallback_for_empty_threshold_track(self):
        forecast_density = np.zeros((2, 2), dtype=float)
        observed_density = np.array([[1.0, 0.0], [0.0, 0.0]], dtype=float)
        forecast_hits = np.zeros((2, 2), dtype=float)
        observed_hits = (observed_density > 0).astype(float)
        sea_mask = np.ones((2, 2), dtype=bool)

        metric_forecast, metric_observed, metric_mask, metadata = _prepare_density_pair_for_metrics(
            forecast_density=forecast_density,
            observed_density=observed_density,
            forecast_hits=forecast_hits,
            observed_hits=observed_hits,
            valid_mask=sea_mask,
            allow_blank_support_fallback=True,
        )

        self.assertEqual(metadata["raw_forecast_sum"], 0.0)
        self.assertEqual(metadata["raw_observed_sum"], 1.0)
        self.assertEqual(metadata["strategy"], "support_positive_mask_fallback")
        self.assertEqual(int(np.count_nonzero(metric_mask)), 1)
        self.assertAlmostEqual(float(metric_forecast[metric_mask].sum()), 1.0)
        self.assertAlmostEqual(float(metric_observed[metric_mask].sum()), 1.0)
        self.assertAlmostEqual(
            calculate_kl_divergence(metric_forecast, metric_observed, valid_mask=metric_mask),
            0.0,
            places=10,
        )


if __name__ == "__main__":
    unittest.main()
