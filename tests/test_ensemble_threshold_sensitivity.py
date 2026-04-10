import unittest

import pandas as pd

from src.services.ensemble_threshold_sensitivity import (
    THRESHOLDS,
    recommend_threshold_strategy,
    select_threshold_from_calibration,
)


class EnsembleThresholdSensitivityTests(unittest.TestCase):
    def test_threshold_candidates_do_not_rename_p50(self):
        self.assertEqual(THRESHOLDS, [0.10, 0.20, 0.30, 0.40, 0.50])

    def test_selection_uses_march4_and_march5_only(self):
        rows = []
        for threshold, calibration_fss, holdout_fss in [(0.1, 0.2, 0.0), (0.2, 0.5, 0.0), (0.5, 0.3, 1.0)]:
            for date in ["2023-03-04", "2023-03-05"]:
                rows.append(
                    {
                        "threshold": threshold,
                        "threshold_label": f"p{int(threshold * 100):02d}",
                        "pair_role": "per_date_union",
                        "obs_date": date,
                        "centroid_distance_m": 10_000,
                        "area_ratio_forecast_to_obs": 1.0,
                        "fss_1km": calibration_fss,
                        "fss_3km": calibration_fss,
                        "fss_5km": calibration_fss,
                        "fss_10km": calibration_fss,
                    }
                )
            rows.append(
                {
                    "threshold": threshold,
                    "threshold_label": f"p{int(threshold * 100):02d}",
                    "pair_role": "per_date_union",
                    "obs_date": "2023-03-06",
                    "centroid_distance_m": 1,
                    "area_ratio_forecast_to_obs": 1.0,
                    "fss_1km": holdout_fss,
                    "fss_3km": holdout_fss,
                    "fss_5km": holdout_fss,
                    "fss_10km": holdout_fss,
                }
            )
        ranking, selected = select_threshold_from_calibration(pd.DataFrame(rows))
        self.assertEqual(selected["threshold"], 0.2)
        self.assertEqual(ranking.iloc[0]["threshold"], 0.2)

    def test_recommend_threshold_not_main_lever_without_event_improvement(self):
        recommendation = recommend_threshold_strategy(
            selected_threshold=0.2,
            p50_event_mean_fss=0.10,
            selected_event_mean_fss=0.105,
            beats_deterministic=False,
            beats_pygnome=False,
        )
        self.assertEqual(recommendation["recommendation"], "conclude that threshold choice is not the main remaining lever")

    def test_recommend_lower_threshold_when_it_materially_improves(self):
        recommendation = recommend_threshold_strategy(
            selected_threshold=0.2,
            p50_event_mean_fss=0.10,
            selected_event_mean_fss=0.25,
            beats_deterministic=False,
            beats_pygnome=False,
        )
        self.assertIn("adopt calibrated lower-threshold", recommendation["recommendation"])


if __name__ == "__main__":
    unittest.main()
