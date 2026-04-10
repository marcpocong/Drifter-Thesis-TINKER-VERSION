import unittest

import pandas as pd

from src.services.recipe_sensitivity_r1_multibranch import (
    BRANCHES,
    RECIPE_MATRIX,
    VALIDATION_DATES,
    recommend_recipe_branch,
)


class RecipeSensitivityR1MultibranchTests(unittest.TestCase):
    def test_validation_dates_exclude_march3(self):
        self.assertEqual(VALIDATION_DATES, ["2023-03-04", "2023-03-05", "2023-03-06"])

    def test_matrix_keeps_requested_recipe_families(self):
        recipe_ids = [recipe.recipe_id for recipe in RECIPE_MATRIX]
        self.assertEqual(recipe_ids, ["cmems_era5", "cmems_gfs", "hycom_era5", "hycom_gfs"])
        branch_ids = [branch.branch_id for branch in BRANCHES]
        self.assertEqual(branch_ids, ["B", "A1"])

    def test_recommendation_prefers_pygnome_when_it_beats_opendrift(self):
        summary = pd.DataFrame(
            [
                {
                    "track_id": "OD_cmems_era5_B_ens_p50",
                    "model_family": "OpenDrift",
                    "recipe_id": "cmems_era5",
                    "branch_id": "B",
                    "pair_role": "eventcorridor_march4_6",
                    "fss_1km": 0.05,
                    "fss_3km": 0.05,
                    "fss_5km": 0.05,
                    "fss_10km": 0.05,
                    "iou": 0.01,
                    "dice": 0.02,
                    "forecast_nonzero_cells": 10,
                    "nearest_distance_to_obs_m": 1000,
                },
                {
                    "track_id": "PYGNOME_FIXED_DET",
                    "model_family": "PyGNOME",
                    "recipe_id": "pygnome_fixed_comparator",
                    "branch_id": "B_surrogate",
                    "pair_role": "eventcorridor_march4_6",
                    "fss_1km": 0.2,
                    "fss_3km": 0.2,
                    "fss_5km": 0.2,
                    "fss_10km": 0.2,
                    "iou": 0.04,
                    "dice": 0.08,
                    "forecast_nonzero_cells": 20,
                    "nearest_distance_to_obs_m": 0,
                },
            ]
        )
        recommendation = recommend_recipe_branch(summary)
        self.assertFalse(recommendation["any_opendrift_branch_beats_pygnome"])
        self.assertEqual(recommendation["recommendation"], "conclude that recipe choice is not enough to beat PyGNOME")

    def test_recommendation_promotes_opendrift_when_it_beats_pygnome(self):
        summary = pd.DataFrame(
            [
                {
                    "track_id": "OD_hycom_era5_A1_ens_p50",
                    "model_family": "OpenDrift",
                    "recipe_id": "hycom_era5",
                    "branch_id": "A1",
                    "pair_role": "eventcorridor_march4_6",
                    "fss_1km": 0.3,
                    "fss_3km": 0.3,
                    "fss_5km": 0.3,
                    "fss_10km": 0.3,
                    "iou": 0.05,
                    "dice": 0.1,
                    "forecast_nonzero_cells": 20,
                    "nearest_distance_to_obs_m": 0,
                },
                {
                    "track_id": "PYGNOME_FIXED_DET",
                    "model_family": "PyGNOME",
                    "recipe_id": "pygnome_fixed_comparator",
                    "branch_id": "B_surrogate",
                    "pair_role": "eventcorridor_march4_6",
                    "fss_1km": 0.1,
                    "fss_3km": 0.1,
                    "fss_5km": 0.1,
                    "fss_10km": 0.1,
                    "iou": 0.02,
                    "dice": 0.04,
                    "forecast_nonzero_cells": 10,
                    "nearest_distance_to_obs_m": 1000,
                },
            ]
        )
        recommendation = recommend_recipe_branch(summary)
        self.assertTrue(recommendation["any_opendrift_branch_beats_pygnome"])
        self.assertIn("promote one OpenDrift", recommendation["recommendation"])
        self.assertEqual(recommendation["best_eventcorridor_track_id"], "OD_hycom_era5_A1_ens_p50")


if __name__ == "__main__":
    unittest.main()
