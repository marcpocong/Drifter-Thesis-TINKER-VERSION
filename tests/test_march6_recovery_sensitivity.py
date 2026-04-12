import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pandas as pd

from src.core.case_context import get_case_context
from src.helpers.metrics import calculate_fss
from src.services.march6_recovery_sensitivity import (
    BRANCH_PRECEDENCE,
    LOCKED_OUTPUT_FILES,
    SUPPORT_BUFFER_KM,
    THRESHOLD_LADDER,
    March6RecoverySensitivityService,
    build_matrix_index,
    build_support_mask,
    build_threshold_mask,
    rank_recovery_rows,
)
from src.services.scoring import OFFICIAL_PHASE3B_WINDOWS_KM, Phase3BScoringService

try:
    import geopandas as gpd
except ImportError:  # pragma: no cover
    gpd = None

try:
    from shapely.geometry import box
except ImportError:  # pragma: no cover
    box = None


def _threshold_by_label(label: str):
    for spec in THRESHOLD_LADDER:
        if spec.label == label:
            return spec
    raise KeyError(label)


def _score_masks(helper: Phase3BScoringService, forecast_mask: np.ndarray, obs_mask: np.ndarray) -> dict:
    diagnostics = helper._compute_mask_diagnostics(forecast_mask, obs_mask)
    row = {**diagnostics}
    valid_mask = helper.sea_mask > 0.5 if helper.sea_mask is not None else None
    for window_km in OFFICIAL_PHASE3B_WINDOWS_KM:
        row[f"fss_{window_km}km"] = float(
            calculate_fss(
                forecast_mask,
                obs_mask,
                window=helper._window_km_to_cells(int(window_km)),
                valid_mask=valid_mask,
            )
        )
    return row


class March6RecoverySensitivityTests(unittest.TestCase):
    def test_threshold_ladder_matches_plan(self):
        self.assertEqual([spec.label for spec in THRESHOLD_LADDER], ["p50", "p40", "p30", "p20", "p10", "p05", "any_presence"])
        self.assertEqual([spec.threshold for spec in THRESHOLD_LADDER[:-1]], [0.50, 0.40, 0.30, 0.20, 0.10, 0.05])
        self.assertTrue(THRESHOLD_LADDER[-1].any_presence)

    def test_support_buffer_ladder_matches_plan(self):
        self.assertEqual(SUPPORT_BUFFER_KM, [0, 1, 2, 5, 10, 15, 20, 25])

    @unittest.skipIf(gpd is None or box is None, "geopandas/shapely are required for support-mask tests")
    def test_support_masks_are_nondecreasing_with_buffer(self):
        with patch.dict(os.environ, {"WORKFLOW_MODE": "mindoro_retro_2023"}, clear=False):
            get_case_context.cache_clear()
            helper = Phase3BScoringService(output_dir=Path(tempfile.gettempdir()) / "march6_recovery_support_test")
            ocean_cells = np.argwhere(helper.sea_mask > 0.5)
            self.assertGreater(len(ocean_cells), 0)
            row, col = ocean_cells[0]
            cell_center_x = helper.grid.min_x + ((float(col) + 0.5) * helper.grid.resolution)
            cell_center_y = helper.grid.max_y - ((float(row) + 0.5) * helper.grid.resolution)
            half_width_m = helper.grid.resolution * 0.15
            source_gdf = gpd.GeoDataFrame(
                geometry=[box(cell_center_x - half_width_m, cell_center_y - half_width_m, cell_center_x + half_width_m, cell_center_y + half_width_m)],
                crs=helper.grid.crs,
            )
            counts = []
            for buffer_km in [0, 1, 2]:
                _, mask = build_support_mask(source_gdf, grid=helper.grid, sea_mask=helper.sea_mask, buffer_km=buffer_km)
                counts.append(int(np.count_nonzero(mask > 0)))
            get_case_context.cache_clear()
        self.assertGreaterEqual(counts[1], counts[0])
        self.assertGreaterEqual(counts[2], counts[1])

    def test_matrix_index_builds_224_rows_for_four_branches(self):
        matrix = build_matrix_index(list(BRANCH_PRECEDENCE))
        self.assertEqual(len(matrix), 224)

    def test_ranking_prefers_smaller_buffer_higher_threshold_then_branch_precedence(self):
        frame = pd.DataFrame(
            [
                {
                    "pair_id": "r1_p05_small",
                    "branch_id": "R1_previous",
                    "threshold_value": 0.05,
                    "buffer_km": 1,
                    "fss_1km": 0.1,
                    "fss_3km": 0.1,
                    "fss_5km": 0.1,
                    "fss_10km": 0.1,
                },
                {
                    "pair_id": "a2_p05_small",
                    "branch_id": "A2_24H",
                    "threshold_value": 0.05,
                    "buffer_km": 1,
                    "fss_1km": 0.1,
                    "fss_3km": 0.1,
                    "fss_5km": 0.1,
                    "fss_10km": 0.1,
                },
                {
                    "pair_id": "r1_any_small",
                    "branch_id": "R1_previous",
                    "threshold_value": 0.0,
                    "buffer_km": 1,
                    "fss_1km": 0.1,
                    "fss_3km": 0.1,
                    "fss_5km": 0.1,
                    "fss_10km": 0.1,
                },
                {
                    "pair_id": "r1_p05_large",
                    "branch_id": "R1_previous",
                    "threshold_value": 0.05,
                    "buffer_km": 5,
                    "fss_1km": 0.1,
                    "fss_3km": 0.1,
                    "fss_5km": 0.1,
                    "fss_10km": 0.1,
                },
            ]
        )
        ranked = rank_recovery_rows(frame)
        self.assertEqual(ranked.iloc[0]["pair_id"], "r1_p05_small")
        self.assertEqual(ranked.iloc[1]["pair_id"], "a2_p05_small")
        self.assertEqual(ranked.iloc[2]["pair_id"], "r1_any_small")
        self.assertEqual(ranked.iloc[3]["pair_id"], "r1_p05_large")

    @unittest.skipIf(gpd is None, "geopandas is required for regression checks")
    def test_r1_p50_zero_km_matches_saved_strict_row(self):
        with patch.dict(os.environ, {"WORKFLOW_MODE": "mindoro_retro_2023"}, clear=False):
            get_case_context.cache_clear()
            with tempfile.TemporaryDirectory() as tmpdir:
                helper = Phase3BScoringService(output_dir=Path(tmpdir) / "phase3b")
                summary = pd.read_csv("output/CASE_MINDORO_RETRO_2023/official_rerun_r1/official_rerun_r1_summary.csv")
                row = summary[(summary["scenario_id"] == "R1") & (summary["pair_role"] == "strict_march6")].iloc[0]
                probability = helper._read_mask(Path(row["probability_path"]))
                forecast_mask = build_threshold_mask(probability, _threshold_by_label("p50"), helper.sea_mask)
                source_gdf = gpd.read_file("data/arcgis/CASE_MINDORO_RETRO_2023/validation_polygon_mar6_processed.gpkg")
                _, obs_mask = build_support_mask(source_gdf, grid=helper.grid, sea_mask=helper.sea_mask, buffer_km=0)
                scored = _score_masks(helper, forecast_mask, obs_mask)
            get_case_context.cache_clear()
        self.assertEqual(scored["forecast_nonzero_cells"], int(row["forecast_nonzero_cells"]))
        self.assertEqual(scored["obs_nonzero_cells"], int(row["obs_nonzero_cells"]))
        self.assertAlmostEqual(scored["centroid_distance_m"], float(row["centroid_distance_m"]), places=6)
        self.assertAlmostEqual(scored["fss_10km"], float(row["fss_10km"]), places=12)

    @unittest.skipIf(gpd is None, "geopandas is required for regression checks")
    def test_r1_any_presence_zero_km_matches_threshold_free_saved_row(self):
        with patch.dict(os.environ, {"WORKFLOW_MODE": "mindoro_retro_2023"}, clear=False):
            get_case_context.cache_clear()
            with tempfile.TemporaryDirectory() as tmpdir:
                helper = Phase3BScoringService(output_dir=Path(tmpdir) / "phase3b")
                summary = pd.read_csv(
                    "output/CASE_MINDORO_RETRO_2023/pygnome_public_comparison/threshold_free_ensemble_sensitivity/threshold_free_ensemble_sensitivity_summary.csv"
                )
                row = summary[(summary["track_id"] == "C2_any") & (summary["pair_role"] == "strict_march6")].iloc[0]
                probability = helper._read_mask(
                    Path("output/CASE_MINDORO_RETRO_2023/transport_retention_fix/R1_previous/model_run/ensemble/prob_presence_2023-03-06_datecomposite.tif")
                )
                forecast_mask = build_threshold_mask(probability, _threshold_by_label("any_presence"), helper.sea_mask)
                source_gdf = gpd.read_file("data/arcgis/CASE_MINDORO_RETRO_2023/validation_polygon_mar6_processed.gpkg")
                _, obs_mask = build_support_mask(source_gdf, grid=helper.grid, sea_mask=helper.sea_mask, buffer_km=0)
                scored = _score_masks(helper, forecast_mask, obs_mask)
            get_case_context.cache_clear()
        self.assertEqual(scored["forecast_nonzero_cells"], int(row["forecast_nonzero_cells"]))
        self.assertEqual(scored["obs_nonzero_cells"], int(row["obs_nonzero_cells"]))
        self.assertAlmostEqual(scored["centroid_distance_m"], float(row["centroid_distance_m"]), places=6)
        self.assertAlmostEqual(scored["fss_10km"], float(row["fss_10km"]), places=12)

    @unittest.skipIf(gpd is None, "geopandas is required for regression checks")
    def test_a2_48h_p50_zero_km_matches_saved_strict_row(self):
        with patch.dict(os.environ, {"WORKFLOW_MODE": "mindoro_retro_2023"}, clear=False):
            get_case_context.cache_clear()
            with tempfile.TemporaryDirectory() as tmpdir:
                helper = Phase3BScoringService(output_dir=Path(tmpdir) / "phase3b")
                summary = pd.read_csv(
                    "output/CASE_MINDORO_RETRO_2023/source_history_reconstruction_r1/source_history_reconstruction_r1_summary.csv"
                )
                row = summary[(summary["scenario_id"] == "A2_48H") & (summary["pair_role"] == "strict_march6")].iloc[0]
                probability = helper._read_mask(Path(row["probability_path"]))
                forecast_mask = build_threshold_mask(probability, _threshold_by_label("p50"), helper.sea_mask)
                source_gdf = gpd.read_file("data/arcgis/CASE_MINDORO_RETRO_2023/validation_polygon_mar6_processed.gpkg")
                _, obs_mask = build_support_mask(source_gdf, grid=helper.grid, sea_mask=helper.sea_mask, buffer_km=0)
                scored = _score_masks(helper, forecast_mask, obs_mask)
            get_case_context.cache_clear()
        self.assertEqual(scored["forecast_nonzero_cells"], int(row["forecast_nonzero_cells"]))
        self.assertEqual(scored["obs_nonzero_cells"], int(row["obs_nonzero_cells"]))
        self.assertAlmostEqual(scored["centroid_distance_m"], float(row["centroid_distance_m"]), places=6)
        self.assertAlmostEqual(scored["fss_10km"], float(row["fss_10km"]), places=12)

    def test_locked_outputs_guardrail_detects_mutation(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            locked_a = Path(tmpdir) / "locked_a.csv"
            locked_b = Path(tmpdir) / "locked_b.csv"
            locked_a.write_text("alpha\n", encoding="utf-8")
            locked_b.write_text("beta\n", encoding="utf-8")

            service = March6RecoverySensitivityService.__new__(March6RecoverySensitivityService)
            with patch("src.services.march6_recovery_sensitivity.LOCKED_OUTPUT_FILES", [locked_a, locked_b]):
                service.locked_hashes_before = service._snapshot_locked_outputs()
                service._verify_locked_outputs_unchanged()
                locked_a.write_text("changed\n", encoding="utf-8")
                with self.assertRaisesRegex(RuntimeError, "locked official outputs"):
                    service._verify_locked_outputs_unchanged()


if __name__ == "__main__":
    unittest.main()
