import unittest

import pandas as pd

from src.services.init_mode_sensitivity_r1 import BRANCHES, recommend_initialization_strategy


class InitModeSensitivityR1Tests(unittest.TestCase):
    def test_branches_include_b_and_a1(self):
        by_id = {branch.branch_id: branch for branch in BRANCHES}
        self.assertEqual(set(by_id), {"A1", "B"})
        self.assertTrue(by_id["B"].reuse_official_rerun_r1)
        self.assertEqual(by_id["A1"].seed_overrides["initialization_mode"], "source_point_initialized_same_start")

    def test_recommend_a1_when_displacement_improves(self):
        summary = pd.DataFrame(
            [
                {
                    "branch_id": "B",
                    "pair_role": "strict_march6",
                    "fss_1km": 0.0,
                    "fss_3km": 0.0,
                    "fss_5km": 0.0,
                    "fss_10km": 0.0,
                    "iou": 0.0,
                    "nearest_distance_to_obs_m": 70000,
                    "forecast_nonzero_cells": 3,
                },
                {
                    "branch_id": "A1",
                    "pair_role": "strict_march6",
                    "fss_1km": 0.0,
                    "fss_3km": 0.0,
                    "fss_5km": 0.0,
                    "fss_10km": 0.0,
                    "iou": 0.0,
                    "nearest_distance_to_obs_m": 30000,
                    "forecast_nonzero_cells": 3,
                },
            ]
        )
        recommendation = recommend_initialization_strategy(summary)
        self.assertEqual(
            recommendation["recommended_initialization_strategy"],
            "promote A1 as the stronger main case-definition candidate",
        )

    def test_recommend_distinct_tracks_for_strict_vs_event_mixed_result(self):
        summary = pd.DataFrame(
            [
                {
                    "branch_id": "B",
                    "pair_role": "strict_march6",
                    "fss_1km": 0.0,
                    "fss_3km": 0.0,
                    "fss_5km": 0.0,
                    "fss_10km": 0.0,
                    "iou": 0.0,
                    "nearest_distance_to_obs_m": 70000,
                    "forecast_nonzero_cells": 3,
                },
                {
                    "branch_id": "A1",
                    "pair_role": "strict_march6",
                    "fss_1km": 0.0,
                    "fss_3km": 0.0,
                    "fss_5km": 0.0,
                    "fss_10km": 0.0,
                    "iou": 0.0,
                    "nearest_distance_to_obs_m": "",
                    "forecast_nonzero_cells": 0,
                },
                {
                    "branch_id": "B",
                    "pair_role": "eventcorridor_march4_6",
                    "fss_1km": 0.0,
                    "fss_3km": 0.0,
                    "fss_5km": 0.0,
                    "fss_10km": 0.0,
                    "iou": 0.0,
                    "nearest_distance_to_obs_m": 60000,
                    "forecast_nonzero_cells": 84,
                },
                {
                    "branch_id": "A1",
                    "pair_role": "eventcorridor_march4_6",
                    "fss_1km": 0.15,
                    "fss_3km": 0.17,
                    "fss_5km": 0.19,
                    "fss_10km": 0.25,
                    "iou": 0.1,
                    "nearest_distance_to_obs_m": 0,
                    "forecast_nonzero_cells": 6,
                },
            ]
        )
        recommendation = recommend_initialization_strategy(summary)
        self.assertEqual(recommendation["recommended_initialization_strategy"], "keep both as distinct tracks")


if __name__ == "__main__":
    unittest.main()
