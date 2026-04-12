import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest import mock

import pandas as pd

from src.services.recipe_sensitivity_r1_multibranch import (
    BRANCHES,
    RECIPE_MATRIX,
    VALIDATION_DATES,
    recommend_recipe_branch,
    select_promotable_opendrift_recipe,
    RecipeSensitivityR1MultibranchService,
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

    def test_promotable_recipe_ignores_pygnome_and_branch_a1(self):
        summary = pd.DataFrame(
            [
                {
                    "track_id": "OD_hycom_gfs_A1_det",
                    "model_family": "OpenDrift",
                    "recipe_id": "hycom_gfs",
                    "branch_id": "A1",
                    "product_kind": "deterministic",
                    "pair_role": "eventcorridor_march4_6",
                    "fss_1km": 0.6,
                    "fss_3km": 0.6,
                    "fss_5km": 0.6,
                    "fss_10km": 0.6,
                    "iou": 0.4,
                    "dice": 0.5,
                    "forecast_nonzero_cells": 400,
                    "nearest_distance_to_obs_m": 0,
                },
                {
                    "track_id": "PYGNOME_FIXED_DET",
                    "model_family": "PyGNOME",
                    "recipe_id": "pygnome_fixed_comparator",
                    "branch_id": "B_surrogate",
                    "product_kind": "deterministic",
                    "pair_role": "eventcorridor_march4_6",
                    "fss_1km": 0.7,
                    "fss_3km": 0.7,
                    "fss_5km": 0.7,
                    "fss_10km": 0.7,
                    "iou": 0.5,
                    "dice": 0.6,
                    "forecast_nonzero_cells": 450,
                    "nearest_distance_to_obs_m": 0,
                },
                {
                    "track_id": "OD_hycom_gfs_B_det",
                    "model_family": "OpenDrift",
                    "recipe_id": "hycom_gfs",
                    "branch_id": "B",
                    "product_kind": "deterministic",
                    "pair_role": "eventcorridor_march4_6",
                    "fss_1km": 0.2,
                    "fss_3km": 0.2,
                    "fss_5km": 0.2,
                    "fss_10km": 0.2,
                    "iou": 0.1,
                    "dice": 0.15,
                    "forecast_nonzero_cells": 200,
                    "nearest_distance_to_obs_m": 0,
                },
                {
                    "track_id": "OD_cmems_gfs_B_ens_p50",
                    "model_family": "OpenDrift",
                    "recipe_id": "cmems_gfs",
                    "branch_id": "B",
                    "product_kind": "ensemble_p50",
                    "pair_role": "eventcorridor_march4_6",
                    "fss_1km": 0.25,
                    "fss_3km": 0.25,
                    "fss_5km": 0.25,
                    "fss_10km": 0.25,
                    "iou": 0.11,
                    "dice": 0.16,
                    "forecast_nonzero_cells": 210,
                    "nearest_distance_to_obs_m": 0,
                },
            ]
        )
        promotion = select_promotable_opendrift_recipe(summary)
        self.assertEqual(promotion["track_id"], "OD_cmems_gfs_B_ens_p50")
        self.assertEqual(promotion["recipe_id"], "cmems_gfs")
        self.assertEqual(promotion["branch_id"], "B")

    def test_evaluate_recipe_matrix_marks_gfs_recipes_available_when_wind_file_exists(self):
        with TemporaryDirectory() as tmpdir:
            forcing_dir = Path(tmpdir)
            for name in ("cmems_curr.nc", "hycom_curr.nc", "cmems_wave.nc", "era5_wind.nc", "gfs_wind.nc"):
                (forcing_dir / name).write_text("", encoding="utf-8")

            service = object.__new__(RecipeSensitivityR1MultibranchService)
            service.forcing_dir = forcing_dir

            rows = service._evaluate_recipe_matrix()
            by_recipe = {row["recipe_id"]: row for row in rows}

            self.assertTrue(by_recipe["cmems_gfs"]["available"])
            self.assertTrue(by_recipe["hycom_gfs"]["available"])
            self.assertFalse(by_recipe["cmems_gfs"]["missing_inputs"])
            self.assertFalse(by_recipe["hycom_gfs"]["missing_inputs"])

    def test_run_or_reuse_model_reuses_canonical_recipe_sensitivity_outputs_for_experiment_slug(self):
        service = object.__new__(RecipeSensitivityR1MultibranchService)
        service.case = SimpleNamespace(
            run_name="CASE_MINDORO_RETRO_2023",
            simulation_start_utc="2023-03-03T09:59:00Z",
            simulation_end_utc="2023-03-06T09:59:00Z",
        )
        service.output_slug = "mindoro_local_recipe_experiment/event_recipe_comparison"
        service.force_rerun = False
        recipe = {"recipe_id": "cmems_era5"}
        branch = BRANCHES[0]
        canonical_dir = Path(
            "output/CASE_MINDORO_RETRO_2023/recipe_sensitivity_r1_multibranch/cmems_era5/B/model_run"
        )

        def _fake_complete(path: Path) -> bool:
            return Path(path) == canonical_dir

        with mock.patch.object(service, "_model_dir_complete", side_effect=_fake_complete):
            result = RecipeSensitivityR1MultibranchService._run_or_reuse_model(service, recipe, branch)

        self.assertEqual(result["status"], "reused_existing_model")
        self.assertEqual(result["run_name"], "CASE_MINDORO_RETRO_2023/recipe_sensitivity_r1_multibranch/cmems_era5/B/model_run")
        self.assertEqual(result["model_dir"], str(canonical_dir))
        self.assertEqual(result["reused_from_output_slug"], "recipe_sensitivity_r1_multibranch")


if __name__ == "__main__":
    unittest.main()
