import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pandas as pd
import xarray as xr

from src.core.case_context import get_case_context
from src.helpers.raster import save_raster
from src.services.phase3b_extended_public import EXTENDED_DIR_NAME
from src.services.phase3b_extended_public_scored_march13_14_reinit import (
    EXPECTED_ENSEMBLE_MEMBER_COUNT,
    BRANCHES,
    MARCH13_NOAA_SOURCE_DATE,
    MARCH13_NOAA_SOURCE_KEY,
    MARCH14_NOAA_SOURCE_DATE,
    MARCH14_NOAA_SOURCE_KEY,
    _forcing_time_and_vars,
    Phase3BExtendedPublicScoredMarch1314ReinitService,
    resolve_march13_14_reinit_window,
)
from src.services.scoring import Phase3BScoringService


def _service_stub(tmpdir: str) -> Phase3BExtendedPublicScoredMarch1314ReinitService:
    service = Phase3BExtendedPublicScoredMarch1314ReinitService.__new__(Phase3BExtendedPublicScoredMarch1314ReinitService)
    service.output_dir = Path(tmpdir) / "march13_14_reinit"
    service.output_dir.mkdir(parents=True, exist_ok=True)
    service.precheck_dir = service.output_dir / "precheck"
    service.precheck_dir.mkdir(parents=True, exist_ok=True)
    service.source_extended_dir = Path(tmpdir) / EXTENDED_DIR_NAME
    service.source_extended_dir.mkdir(parents=True, exist_ok=True)
    service.locked_hashes_before = {}
    return service


class Phase3BExtendedPublicScoredMarch1314ReinitTests(unittest.TestCase):
    def tearDown(self):
        get_case_context.cache_clear()

    def test_window_matches_plan(self):
        window = resolve_march13_14_reinit_window()
        self.assertEqual(window.forecast_local_dates, [MARCH13_NOAA_SOURCE_DATE, MARCH14_NOAA_SOURCE_DATE])
        self.assertEqual(window.seed_obs_date, MARCH13_NOAA_SOURCE_DATE)
        self.assertEqual(window.scored_target_date, MARCH14_NOAA_SOURCE_DATE)
        self.assertEqual(window.simulation_start_utc, "2023-03-12T16:00:00Z")
        self.assertEqual(window.simulation_end_utc, "2023-03-14T15:59:00Z")
        self.assertEqual(window.required_forcing_start_utc, "2023-03-12T13:00:00Z")
        self.assertEqual(window.required_forcing_end_utc, "2023-03-14T18:59:00Z")
        self.assertEqual(window.download_start_date, "2023-03-12")
        self.assertEqual(window.download_end_date, "2023-03-15")

    def test_loader_selects_exact_sources_without_relying_on_service_url_names(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            service = _service_stub(tmpdir)
            accepted_masks_dir = service.source_extended_dir / "accepted_obs_masks"
            processed_vectors_dir = service.source_extended_dir / "processed_vectors"
            accepted_masks_dir.mkdir(parents=True, exist_ok=True)
            processed_vectors_dir.mkdir(parents=True, exist_ok=True)
            march13_mask = accepted_masks_dir / f"{MARCH13_NOAA_SOURCE_KEY}.tif"
            march14_mask = accepted_masks_dir / f"{MARCH14_NOAA_SOURCE_KEY}.tif"
            march13_vector = processed_vectors_dir / f"{MARCH13_NOAA_SOURCE_KEY}.gpkg"
            march14_vector = processed_vectors_dir / f"{MARCH14_NOAA_SOURCE_KEY}.gpkg"
            for path in (march13_mask, march14_mask, march13_vector, march14_vector):
                path.write_text("placeholder", encoding="utf-8")
            registry = pd.DataFrame(
                [
                    {
                        "source_key": MARCH13_NOAA_SOURCE_KEY,
                        "source_name": "MindoroOilSpill_NOAA_230313",
                        "provider": "NOAA/NESDIS",
                        "obs_date": MARCH13_NOAA_SOURCE_DATE,
                        "accepted_for_extended_quantitative": True,
                        "mask_exists": True,
                        "service_url": "https://example.test/weird-name",
                        "processed_vector": "",
                        "extended_obs_mask": "",
                    },
                    {
                        "source_key": MARCH14_NOAA_SOURCE_KEY,
                        "source_name": "MindoroOilSpill_NOAA_230314",
                        "provider": "NOAA/NESDIS",
                        "obs_date": MARCH14_NOAA_SOURCE_DATE,
                        "accepted_for_extended_quantitative": True,
                        "mask_exists": True,
                        "service_url": "https://example.test/another-weird-name",
                        "processed_vector": "",
                        "extended_obs_mask": "",
                    },
                ]
            )
            registry.to_csv(service.source_extended_dir / "extended_public_obs_acceptance_registry.csv", index=False)
            start_row, target_row = service._load_reinit_observation_pair()

        self.assertEqual(start_row["source_key"], MARCH13_NOAA_SOURCE_KEY)
        self.assertEqual(target_row["source_key"], MARCH14_NOAA_SOURCE_KEY)

    def test_branch_manifest_contains_exactly_r0_and_r1_previous(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            service = _service_stub(tmpdir)
            target_row = pd.Series(
                {
                    "source_key": MARCH14_NOAA_SOURCE_KEY,
                    "source_name": "MindoroOilSpill_NOAA_230314",
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
            pairings = service._build_branch_pairings(target_row, branch_products)

        self.assertEqual(len(pairings), 2)
        self.assertEqual(set(pairings["branch_id"].tolist()), {"R0", "R1_previous"})
        self.assertTrue(pairings["pair_role"].eq("march14_nextday_reinit_branch_compare").all())

    def test_element_count_metadata_comes_from_manifests(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            model_dir = Path(tmpdir) / "model"
            (model_dir / "forecast").mkdir(parents=True, exist_ok=True)
            (model_dir / "ensemble").mkdir(parents=True, exist_ok=True)
            (model_dir / "forecast" / "forecast_manifest.json").write_text(
                '{"ensemble": {"actual_element_count": 98765}}',
                encoding="utf-8",
            )
            (model_dir / "ensemble" / "ensemble_manifest.json").write_text(
                '{"ensemble_configuration": {"element_count": 100000}}',
                encoding="utf-8",
            )
            requested, actual = Phase3BExtendedPublicScoredMarch1314ReinitService._element_count_from_manifests(model_dir)

        self.assertEqual(requested, 100000)
        self.assertEqual(actual, 98765)

    def test_guardrail_detects_locked_output_mutation(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            service = _service_stub(tmpdir)
            locked_a = Path(tmpdir) / "locked_a.csv"
            locked_b = Path(tmpdir) / "locked_b.csv"
            locked_a.write_text("alpha\n", encoding="utf-8")
            locked_b.write_text("beta\n", encoding="utf-8")
            with patch(
                "src.services.phase3b_extended_public_scored_march13_14_reinit.LOCKED_OUTPUT_FILES",
                [locked_a, locked_b],
            ):
                service.locked_hashes_before = service._snapshot_locked_outputs()
                service._verify_locked_outputs_unchanged()
                locked_a.write_text("changed\n", encoding="utf-8")
                with self.assertRaisesRegex(RuntimeError, "locked strict/public-main outputs"):
                    service._verify_locked_outputs_unchanged()

    def test_zero_cell_source_raises_clear_error(self):
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
                        "source_key": MARCH13_NOAA_SOURCE_KEY,
                        "source_name": "MindoroOilSpill_NOAA_230313",
                        "obs_date": MARCH13_NOAA_SOURCE_DATE,
                        "extended_obs_mask": str(zero_mask_path),
                    }
                )
                with patch("src.services.phase3b_extended_public_scored_march13_14_reinit.LOCKED_OUTPUT_FILES", []):
                    service.locked_hashes_before = service._snapshot_locked_outputs()
                    with self.assertRaisesRegex(RuntimeError, "march13_seed source not scoreable after rasterization"):
                        service._ensure_scoreable_observation(obs_row, role_label="march13_seed")
                self.assertTrue((service.output_dir / "march13_seed_source_not_scoreable_after_rasterization.md").exists())

    def test_all_zero_forecast_note_says_blocked_by_survival(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            service = _service_stub(tmpdir)
            summary_df = pd.DataFrame(
                [
                    {
                        "branch_id": "R0",
                        "branch_precedence": 1,
                        "forecast_nonzero_cells": 0,
                        "empty_forecast_reason": "model_survival_did_not_reach_march14_local_date",
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
                        "empty_forecast_reason": "model_survival_did_not_reach_march14_local_date",
                        "mean_fss": 0.0,
                        "fss_1km": 0.0,
                        "fss_3km": 0.0,
                        "fss_5km": 0.0,
                        "fss_10km": 0.0,
                    },
                ]
            )
            start_row = pd.Series({"source_key": MARCH13_NOAA_SOURCE_KEY, "source_name": "MindoroOilSpill_NOAA_230313"})
            target_row = pd.Series({"source_key": MARCH14_NOAA_SOURCE_KEY, "source_name": "MindoroOilSpill_NOAA_230314"})
            seed_release = {"release_start_utc": "2023-03-12T16:00:00Z", "release_geometry_label": "accepted_march13_noaa_processed_polygon"}
            note_path = service._write_decision_note(summary_df, start_row, target_row, seed_release)
            text = note_path.read_text(encoding="utf-8")

        self.assertIn("blocked by model survival, not by missing public data", text)

    def test_forcing_precheck_accepts_hycom_water_u_water_v_aliases(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "hycom_curr.nc"
            times = pd.date_range("2023-03-12 00:00:00", "2023-03-15 21:00:00", freq="3H")
            ds = xr.Dataset(
                {
                    "water_u": (("time", "lat", "lon"), np.ones((len(times), 1, 1), dtype=np.float32)),
                    "water_v": (("time", "lat", "lon"), np.ones((len(times), 1, 1), dtype=np.float32)),
                },
                coords={"time": times, "lat": [13.3], "lon": [121.5]},
            )
            ds.to_netcdf(path)

            row = _forcing_time_and_vars(
                path,
                ["uo", "vo"],
                pd.Timestamp("2023-03-12T13:00:00"),
                pd.Timestamp("2023-03-14T18:59:00"),
            )

        self.assertEqual(row["status"], "ready")
        self.assertEqual(row["missing_required_variables"], "")
        self.assertTrue(row["covers_required_window"])

    def test_branch_outputs_are_reusable_with_complete_member_set_even_without_manifest(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            ensemble_dir = Path(tmpdir) / "ensemble"
            ensemble_dir.mkdir(parents=True, exist_ok=True)
            member_paths = []
            for index in range(1, EXPECTED_ENSEMBLE_MEMBER_COUNT + 1):
                path = ensemble_dir / f"member_{index}.nc"
                path.write_text("placeholder", encoding="utf-8")
                member_paths.append(path)
            forecast_manifest = Path(tmpdir) / "forecast_manifest.json"

            reusable = Phase3BExtendedPublicScoredMarch1314ReinitService._branch_outputs_are_reusable(
                member_paths,
                forecast_manifest,
            )

        self.assertTrue(reusable)


if __name__ == "__main__":
    unittest.main()
