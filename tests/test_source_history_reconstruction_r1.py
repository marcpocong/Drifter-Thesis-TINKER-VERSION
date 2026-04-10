import unittest

import pandas as pd

from src.services.source_history_reconstruction_r1 import (
    A2_SCENARIOS,
    recommend_source_history_strategy,
)


class SourceHistoryReconstructionR1Tests(unittest.TestCase):
    def test_a2_scenario_release_windows_are_anchored_to_march3(self):
        by_id = {scenario.scenario_id: scenario for scenario in A2_SCENARIOS}
        self.assertEqual(set(by_id), {"A2_PULSE", "A2_24H", "A2_48H", "A2_72H"})
        self.assertEqual(by_id["A2_PULSE"].release_window()["release_start_utc"], "2023-03-03T09:59:00Z")
        self.assertEqual(by_id["A2_24H"].release_window()["release_start_utc"], "2023-03-02T09:59:00Z")
        self.assertEqual(by_id["A2_48H"].release_window()["release_start_utc"], "2023-03-01T09:59:00Z")
        self.assertEqual(by_id["A2_72H"].release_window()["release_start_utc"], "2023-02-28T09:59:00Z")
        for scenario in A2_SCENARIOS:
            self.assertEqual(scenario.release_window()["release_end_utc"], "2023-03-03T09:59:00Z")
            self.assertEqual(scenario.release_window()["simulation_end_utc"], "2023-03-06T09:59:00Z")

    def test_recommend_promotes_a2_when_strict_march6_improves(self):
        summary = pd.DataFrame(
            [
                {
                    "scenario_id": "A2_24H",
                    "pair_role": "strict_march6",
                    "fss_1km": 0.05,
                    "fss_3km": 0.05,
                    "fss_5km": 0.05,
                    "fss_10km": 0.05,
                    "iou": 0.1,
                    "nearest_distance_to_obs_m": 0,
                },
                {
                    "scenario_id": "A2_24H",
                    "pair_role": "eventcorridor_march4_6",
                    "fss_1km": 0.1,
                    "fss_3km": 0.1,
                    "fss_5km": 0.1,
                    "fss_10km": 0.1,
                },
            ]
        )
        recommendation = recommend_source_history_strategy(summary, pd.DataFrame())
        self.assertEqual(
            recommendation["recommendation"],
            "promote A2_24H as the stronger main event-reconstruction candidate",
        )
        self.assertFalse(recommendation["convergence_should_be_next"])

    def test_recommend_keeps_a2_as_sensitivity_when_only_eventcorridor_improves(self):
        summary = pd.DataFrame(
            [
                {
                    "scenario_id": "A2_48H",
                    "pair_role": "strict_march6",
                    "fss_1km": 0.0,
                    "fss_3km": 0.0,
                    "fss_5km": 0.0,
                    "fss_10km": 0.0,
                    "iou": 0.0,
                    "nearest_distance_to_obs_m": 50000,
                },
                {
                    "scenario_id": "A2_48H",
                    "pair_role": "eventcorridor_march4_6",
                    "fss_1km": 0.3,
                    "fss_3km": 0.3,
                    "fss_5km": 0.3,
                    "fss_10km": 0.3,
                },
            ]
        )
        previous = pd.DataFrame(
            [
                {
                    "branch_id": "A1",
                    "pair_role": "eventcorridor_march4_6",
                    "fss_1km": 0.1,
                    "fss_3km": 0.1,
                    "fss_5km": 0.1,
                    "fss_10km": 0.1,
                }
            ]
        )
        recommendation = recommend_source_history_strategy(summary, previous)
        self.assertEqual(
            recommendation["recommendation"],
            "keep B as the main case-definition and A2 as reconstruction sensitivity",
        )
        self.assertFalse(recommendation["convergence_should_be_next"])


if __name__ == "__main__":
    unittest.main()
