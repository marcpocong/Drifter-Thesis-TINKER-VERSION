import unittest

import pandas as pd

from src.services.transport_retention_fix import SCENARIOS, TransportRetentionFixService


class TransportRetentionFixTests(unittest.TestCase):
    def test_required_scenarios_are_present_and_r3_is_diagnostic_only(self):
        by_id = {scenario.scenario_id: scenario for scenario in SCENARIOS}
        self.assertEqual(set(by_id), {"R0", "R1", "R2", "R3"})
        self.assertEqual(by_id["R1"].coastline_action, "previous")
        self.assertEqual(by_id["R3"].coastline_action, "none")
        self.assertTrue(by_id["R3"].diagnostic_only)

    def test_recommendation_never_selects_diagnostic_r3(self):
        service = object.__new__(TransportRetentionFixService)
        summary = pd.DataFrame(
            [
                {
                    "scenario_id": "R0",
                    "diagnostic_only": False,
                    "survives_to_strict_validation": False,
                    "prob_presence_to_strict_validation": False,
                    "p50_signal_on_march6": False,
                    "mean_eventcorridor_fss": 0.11,
                    "strict_march6_fss_10km": 0.0,
                    "eventcorridor_forecast_nonzero_cells": 118,
                    "terminal_active_fraction": 0.0,
                    "terminal_stranding_fraction": 0.0,
                    "last_raw_active_time_utc": "2023-03-05T00:59:00Z",
                    "last_nonzero_prob_presence_utc": "2023-03-05T00:59:00Z",
                },
                {
                    "scenario_id": "R1",
                    "diagnostic_only": False,
                    "survives_to_strict_validation": True,
                    "prob_presence_to_strict_validation": True,
                    "p50_signal_on_march6": True,
                    "mean_eventcorridor_fss": 0.10,
                    "strict_march6_fss_10km": 0.02,
                    "eventcorridor_forecast_nonzero_cells": 20,
                    "terminal_active_fraction": 0.1,
                    "terminal_stranding_fraction": 0.5,
                    "last_raw_active_time_utc": "2023-03-09T15:59:00Z",
                    "last_nonzero_prob_presence_utc": "2023-03-09T15:59:00Z",
                },
                {
                    "scenario_id": "R2",
                    "diagnostic_only": False,
                    "survives_to_strict_validation": False,
                    "prob_presence_to_strict_validation": False,
                    "p50_signal_on_march6": False,
                    "mean_eventcorridor_fss": 0.20,
                    "strict_march6_fss_10km": 0.0,
                    "eventcorridor_forecast_nonzero_cells": 200,
                    "terminal_active_fraction": 0.0,
                    "terminal_stranding_fraction": 0.0,
                    "last_raw_active_time_utc": "2023-03-05T01:29:00Z",
                    "last_nonzero_prob_presence_utc": "2023-03-05T00:59:00Z",
                },
                {
                    "scenario_id": "R3",
                    "diagnostic_only": True,
                    "survives_to_strict_validation": True,
                    "prob_presence_to_strict_validation": True,
                    "p50_signal_on_march6": True,
                    "mean_eventcorridor_fss": 0.50,
                    "strict_march6_fss_10km": 0.40,
                    "eventcorridor_forecast_nonzero_cells": 200,
                    "terminal_active_fraction": 1.0,
                    "terminal_stranding_fraction": 0.0,
                    "last_raw_active_time_utc": "2023-03-09T15:59:00Z",
                    "last_nonzero_prob_presence_utc": "2023-03-09T15:59:00Z",
                },
            ]
        )
        recommendation = TransportRetentionFixService._choose_recommendation(service, summary)
        self.assertEqual(recommendation["best_scenario"], "R1")
        self.assertTrue(recommendation["coastline_interaction_confirmed"])
        self.assertTrue(recommendation["medium_tier_should_remain_blocked"])


if __name__ == "__main__":
    unittest.main()
