import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pandas as pd

from src.core.case_context import get_case_context
from src.helpers.raster import save_raster
from src.services.phase3b_extended_public import EXTENDED_DIR_NAME, MARCH23_NOAA_MSI_SOURCE_DATE, MARCH23_NOAA_MSI_SOURCE_KEY
from src.services.phase3b_extended_public_scored_march23 import (
    BRANCHES,
    Phase3BExtendedPublicScoredMarch23Service,
    resolve_march23_extended_window,
)
from src.services.scoring import Phase3BScoringService


def _service_stub(tmpdir: str) -> Phase3BExtendedPublicScoredMarch23Service:
    service = Phase3BExtendedPublicScoredMarch23Service.__new__(Phase3BExtendedPublicScoredMarch23Service)
    service.output_dir = Path(tmpdir) / "march23"
    service.output_dir.mkdir(parents=True, exist_ok=True)
    service.precheck_dir = service.output_dir / "precheck"
    service.precheck_dir.mkdir(parents=True, exist_ok=True)
    service.source_extended_dir = Path(tmpdir) / EXTENDED_DIR_NAME
    service.source_extended_dir.mkdir(parents=True, exist_ok=True)
    service.locked_hashes_before = {}
    return service


class Phase3BExtendedPublicScoredMarch23Tests(unittest.TestCase):
    def tearDown(self):
        get_case_context.cache_clear()

    def test_march23_window_matches_plan(self):
        with patch.dict(os.environ, {"WORKFLOW_MODE": "mindoro_retro_2023"}, clear=False):
            get_case_context.cache_clear()
            window = resolve_march23_extended_window()

        self.assertEqual(window.validation_dates, [MARCH23_NOAA_MSI_SOURCE_DATE])
        self.assertEqual(window.simulation_start_utc, "2023-03-03T09:59:00Z")
        self.assertEqual(window.simulation_end_utc, "2023-03-23T15:59:00Z")
        self.assertEqual(window.required_forcing_end_utc, "2023-03-23T18:59:00Z")
        self.assertEqual(window.download_start_date, "2023-03-03")
        self.assertEqual(window.download_end_date, "2023-03-24")

    def test_loader_selects_only_accepted_march23_source_and_pairings_are_not_eventcorridor(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            service = _service_stub(tmpdir)
            march23_mask = service.source_extended_dir / "march23_mask.tif"
            march23_mask.write_text("placeholder", encoding="utf-8")
            other_mask = service.source_extended_dir / "march28_mask.tif"
            other_mask.write_text("placeholder", encoding="utf-8")
            registry = pd.DataFrame(
                [
                    {
                        "source_key": MARCH23_NOAA_MSI_SOURCE_KEY,
                        "source_name": "MindoroOilSpill_MSI_20230323",
                        "provider": "NOAA/NESDIS",
                        "obs_date": MARCH23_NOAA_MSI_SOURCE_DATE,
                        "accepted_for_extended_quantitative": True,
                        "mask_exists": True,
                        "extended_obs_mask": str(march23_mask),
                    },
                    {
                        "source_key": "another-source",
                        "source_name": "MindoroOilSpill_MSI_20230328",
                        "provider": "NOAA/NESDIS",
                        "obs_date": "2023-03-28",
                        "accepted_for_extended_quantitative": True,
                        "mask_exists": True,
                        "extended_obs_mask": str(other_mask),
                    },
                ]
            )
            registry.to_csv(service.source_extended_dir / "extended_public_obs_acceptance_registry.csv", index=False)
            obs_row = service._load_march23_accepted_observation()
            pairings = service._build_branch_pairings(
                obs_row,
                [
                    {
                        "branch_id": "R0",
                        "branch_description": "Baseline",
                        "branch_precedence": 1,
                        "model_dir": str(service.output_dir / "R0"),
                        "model_run_name": "run/R0",
                        "probability_path": str(service.output_dir / "r0_prob.tif"),
                        "forecast_path": str(service.output_dir / "r0_mask.tif"),
                        "branch_run_status": "reused_existing_branch_run",
                        "empty_forecast_reason": "",
                    },
                    {
                        "branch_id": "R1_previous",
                        "branch_description": "Retention",
                        "branch_precedence": 2,
                        "model_dir": str(service.output_dir / "R1_previous"),
                        "model_run_name": "run/R1_previous",
                        "probability_path": str(service.output_dir / "r1_prob.tif"),
                        "forecast_path": str(service.output_dir / "r1_mask.tif"),
                        "branch_run_status": "reused_existing_branch_run",
                        "empty_forecast_reason": "",
                    },
                ],
            )

        self.assertEqual(obs_row["source_key"], MARCH23_NOAA_MSI_SOURCE_KEY)
        self.assertEqual(obs_row["obs_date"], MARCH23_NOAA_MSI_SOURCE_DATE)
        self.assertTrue(pairings["pair_role"].eq("extended_public_march23_branch_compare").all())
        self.assertFalse(pairings["pair_role"].astype(str).str.contains("eventcorridor", case=False).any())

    def test_branch_manifest_contains_exactly_r0_and_r1_previous(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            service = _service_stub(tmpdir)
            obs_row = pd.Series(
                {
                    "source_key": MARCH23_NOAA_MSI_SOURCE_KEY,
                    "source_name": "MindoroOilSpill_MSI_20230323",
                    "provider": "NOAA/NESDIS",
                    "extended_obs_mask": str(service.output_dir / "obs_mask.tif"),
                }
            )
            branch_products = []
            for branch in BRANCHES:
                branch_products.append(
                    {
                        "branch_id": branch.branch_id,
                        "branch_description": branch.description,
                        "branch_precedence": branch.branch_precedence,
                        "model_dir": str(service.output_dir / branch.output_slug),
                        "model_run_name": f"run/{branch.output_slug}",
                        "probability_path": str(service.output_dir / f"{branch.output_slug}_prob.tif"),
                        "forecast_path": str(service.output_dir / f"{branch.output_slug}_mask.tif"),
                        "branch_run_status": "reused_existing_branch_run",
                        "empty_forecast_reason": "",
                    }
                )
            pairings = service._build_branch_pairings(obs_row, branch_products)

        self.assertEqual(len(pairings), 2)
        self.assertEqual(set(pairings["branch_id"].tolist()), {"R0", "R1_previous"})

    def test_guardrail_detects_locked_output_mutation(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            service = _service_stub(tmpdir)
            locked_a = Path(tmpdir) / "locked_a.csv"
            locked_b = Path(tmpdir) / "locked_b.csv"
            locked_a.write_text("alpha\n", encoding="utf-8")
            locked_b.write_text("beta\n", encoding="utf-8")
            with patch("src.services.phase3b_extended_public_scored_march23.LOCKED_OUTPUT_FILES", [locked_a, locked_b]):
                service.locked_hashes_before = service._snapshot_locked_outputs()
                service._verify_locked_outputs_unchanged()
                locked_a.write_text("changed\n", encoding="utf-8")
                with self.assertRaisesRegex(RuntimeError, "locked strict/public-main outputs"):
                    service._verify_locked_outputs_unchanged()

    def test_zero_cell_march23_raster_raises_clear_error(self):
        with patch.dict(os.environ, {"WORKFLOW_MODE": "mindoro_retro_2023"}, clear=False):
            get_case_context.cache_clear()
            with tempfile.TemporaryDirectory() as tmpdir:
                service = _service_stub(tmpdir)
                service.helper = Phase3BScoringService(output_dir=Path(tmpdir) / "helper")
                zero_mask_path = service.output_dir / "zero_mask.tif"
                zero_mask = np.zeros((service.helper.grid.height, service.helper.grid.width), dtype=np.float32)
                save_raster(service.helper.grid, zero_mask, zero_mask_path)
                obs_row = pd.Series(
                    {
                        "source_key": MARCH23_NOAA_MSI_SOURCE_KEY,
                        "source_name": "MindoroOilSpill_MSI_20230323",
                        "obs_date": MARCH23_NOAA_MSI_SOURCE_DATE,
                        "extended_obs_mask": str(zero_mask_path),
                    }
                )
                with patch("src.services.phase3b_extended_public_scored_march23.LOCKED_OUTPUT_FILES", []):
                    service.locked_hashes_before = service._snapshot_locked_outputs()
                    with self.assertRaisesRegex(RuntimeError, "source not scoreable after rasterization"):
                        service._ensure_scoreable_observation(obs_row)
                self.assertTrue((service.output_dir / "march23_source_not_scoreable_after_rasterization.md").exists())

    def test_all_zero_late_forecast_note_says_blocked_by_survival(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            service = _service_stub(tmpdir)
            summary_df = pd.DataFrame(
                [
                    {
                        "branch_id": "R0",
                        "branch_precedence": 1,
                        "forecast_nonzero_cells": 0,
                        "empty_forecast_reason": "model_survival_did_not_reach_march23_local_date",
                        "mean_fss": 0.0,
                        "fss_1km": 0.0,
                        "fss_3km": 0.0,
                        "fss_5km": 0.0,
                        "fss_10km": 0.0,
                    },
                    {
                        "branch_id": "R1_previous",
                        "branch_precedence": 2,
                        "forecast_nonzero_cells": 0,
                        "empty_forecast_reason": "model_survival_did_not_reach_march23_local_date",
                        "mean_fss": 0.0,
                        "fss_1km": 0.0,
                        "fss_3km": 0.0,
                        "fss_5km": 0.0,
                        "fss_10km": 0.0,
                    },
                ]
            )
            obs_row = pd.Series({"source_key": MARCH23_NOAA_MSI_SOURCE_KEY, "source_name": "MindoroOilSpill_MSI_20230323"})
            note_path = service._write_decision_note(summary_df, obs_row)
            text = note_path.read_text(encoding="utf-8")

        self.assertIn("blocked by model survival, not by missing public data", text)


if __name__ == "__main__":
    unittest.main()
