import unittest

import numpy as np

from src.helpers.metrics import calculate_kl_divergence


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


if __name__ == "__main__":
    unittest.main()
