import unittest

import pandas as pd

from src.services.pygnome_public_comparison import (
    FORECAST_VALIDATION_DATES,
    recommend_public_comparison,
)


class PyGnomePublicComparisonTests(unittest.TestCase):
    def test_forecast_skill_dates_exclude_march3(self):
        self.assertEqual(FORECAST_VALIDATION_DATES, ["2023-03-04", "2023-03-05", "2023-03-06"])

    def test_recommend_all_weak_when_no_track_has_signal(self):
        summary = pd.DataFrame(
            [
                {
                    "track_id": "C1",
                    "pair_role": "eventcorridor_march4_6",
                    "fss_1km": 0.0,
                    "fss_3km": 0.0,
                    "fss_5km": 0.0,
                    "fss_10km": 0.0,
                    "iou": 0.0,
                    "dice": 0.0,
                    "forecast_nonzero_cells": 0,
                },
                {
                    "track_id": "C3",
                    "pair_role": "strict_march6",
                    "fss_1km": 0.0,
                    "fss_3km": 0.0,
                    "fss_5km": 0.0,
                    "fss_10km": 0.0,
                    "iou": 0.0,
                    "dice": 0.0,
                    "forecast_nonzero_cells": 0,
                },
            ]
        )
        recommendation = recommend_public_comparison(summary)
        self.assertIn("all are weak", recommendation["recommendation"])

    def test_recommend_pygnome_when_eventcorridor_is_best(self):
        summary = pd.DataFrame(
            [
                {
                    "track_id": "C2",
                    "pair_role": "eventcorridor_march4_6",
                    "fss_1km": 0.02,
                    "fss_3km": 0.02,
                    "fss_5km": 0.02,
                    "fss_10km": 0.02,
                    "iou": 0.01,
                    "dice": 0.02,
                    "forecast_nonzero_cells": 10,
                },
                {
                    "track_id": "C3",
                    "pair_role": "eventcorridor_march4_6",
                    "fss_1km": 0.08,
                    "fss_3km": 0.09,
                    "fss_5km": 0.10,
                    "fss_10km": 0.11,
                    "iou": 0.04,
                    "dice": 0.08,
                    "forecast_nonzero_cells": 20,
                },
            ]
        )
        recommendation = recommend_public_comparison(summary)
        self.assertEqual(recommendation["recommendation"], "PyGNOME is the better public-validation performer")
        self.assertEqual(recommendation["best_eventcorridor_track"], "C3")


if __name__ == "__main__":
    unittest.main()
