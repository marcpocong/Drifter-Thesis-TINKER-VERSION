import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from src.services.phase3b_extended_public import EXTENDED_DIR_NAME
from src.services.phase3b_extended_public_scored_march13_14_reinit import (
    MARCH13_NOAA_SOURCE_DATE,
    MARCH13_NOAA_SOURCE_KEY,
    MARCH14_NOAA_SOURCE_DATE,
    MARCH14_NOAA_SOURCE_KEY,
    resolve_march13_14_reinit_window,
)
from src.services.phase3b_extended_public_scored_march13_14_reinit_pygnome_comparison import (
    PAIR_ROLE,
    PYGNOME_TRACK_ID,
    Phase3BExtendedPublicScoredMarch1314ReinitPyGnomeComparisonService,
)


def _service_stub(tmpdir: str) -> Phase3BExtendedPublicScoredMarch1314ReinitPyGnomeComparisonService:
    service = Phase3BExtendedPublicScoredMarch1314ReinitPyGnomeComparisonService.__new__(
        Phase3BExtendedPublicScoredMarch1314ReinitPyGnomeComparisonService
    )
    service.output_dir = Path(tmpdir) / "march13_14_reinit_pygnome"
    service.output_dir.mkdir(parents=True, exist_ok=True)
    service.track_dir = service.output_dir / "tracks"
    service.products_dir = service.output_dir / "products"
    service.precheck_dir = service.output_dir / "precheck"
    service.qa_dir = service.output_dir / "qa"
    for path in (service.track_dir, service.products_dir, service.precheck_dir, service.qa_dir):
        path.mkdir(parents=True, exist_ok=True)
    service.source_extended_dir = Path(tmpdir) / EXTENDED_DIR_NAME
    service.source_extended_dir.mkdir(parents=True, exist_ok=True)
    service.reinit_output_dir = Path(tmpdir) / "phase3b_extended_public_scored_march13_14_reinit"
    service.reinit_output_dir.mkdir(parents=True, exist_ok=True)
    service.window = resolve_march13_14_reinit_window()
    service.locked_hashes_before = {}
    service.reused_output_paths = []
    service.reused_hashes_before = {}
    return service


class Phase3BExtendedPublicScoredMarch1314ReinitPyGnomeComparisonTests(unittest.TestCase):
    def test_upstream_completion_guard_requires_reinit_outputs(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            service = _service_stub(tmpdir)
            with self.assertRaisesRegex(FileNotFoundError, "Run phase3b_extended_public_scored_march13_14_reinit first"):
                service._load_completed_reinit_context()

    def test_loader_and_pairings_use_exact_march13_and_march14_noaa_sources(self):
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
                        "processed_vector": "",
                        "extended_obs_mask": "",
                    },
                ]
            )
            registry.to_csv(service.source_extended_dir / "extended_public_obs_acceptance_registry.csv", index=False)
            _, target_row = service._load_reinit_observation_pair()
            pairings = service._build_pairings(
                target_row,
                [
                    {
                        "track_id": "R0_reinit_p50",
                        "track_name": "R0_reinit_p50_vs_march14_noaa",
                        "model_name": "OpenDrift R0 reinit p50",
                        "model_family": "OpenDrift",
                        "transport_model": "oceandrift",
                        "provisional_transport_model": True,
                        "initialization_mode": "accepted_march13_noaa_processed_polygon_reinit",
                        "current_source": "same recipe currents",
                        "wind_source": "same recipe winds",
                        "wave_stokes_status": "same wave stack",
                        "structural_limitations": "reused",
                        "forecast_path": str(service.output_dir / "r0_mask.tif"),
                        "empty_forecast_reason": "",
                    }
                ],
            )

        self.assertEqual(str(target_row["source_key"]), MARCH14_NOAA_SOURCE_KEY)
        self.assertEqual(pairings.iloc[0]["source_key"], MARCH14_NOAA_SOURCE_KEY)
        self.assertEqual(pairings.iloc[0]["observation_path"], str(march14_mask))

    def test_track_set_and_pairings_are_exactly_three_and_march14_only(self):
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
            tracks = [
                {
                    "track_id": "R0_reinit_p50",
                    "track_name": "R0_reinit_p50_vs_march14_noaa",
                    "model_name": "OpenDrift R0 reinit p50",
                    "model_family": "OpenDrift",
                    "transport_model": "oceandrift",
                    "provisional_transport_model": True,
                    "initialization_mode": "reinit",
                    "current_source": "same recipe currents",
                    "wind_source": "same recipe winds",
                    "wave_stokes_status": "same wave stack",
                    "structural_limitations": "reused",
                    "forecast_path": str(service.output_dir / "r0_mask.tif"),
                    "empty_forecast_reason": "",
                },
                {
                    "track_id": "R1_previous_reinit_p50",
                    "track_name": "R1_previous_reinit_p50_vs_march14_noaa",
                    "model_name": "OpenDrift R1 previous reinit p50",
                    "model_family": "OpenDrift",
                    "transport_model": "oceandrift",
                    "provisional_transport_model": True,
                    "initialization_mode": "reinit",
                    "current_source": "same recipe currents",
                    "wind_source": "same recipe winds",
                    "wave_stokes_status": "same wave stack",
                    "structural_limitations": "reused",
                    "forecast_path": str(service.output_dir / "r1_mask.tif"),
                    "empty_forecast_reason": "",
                },
                {
                    "track_id": PYGNOME_TRACK_ID,
                    "track_name": "pygnome_reinit_deterministic_vs_march14_noaa",
                    "model_name": "PyGNOME deterministic March 13 reinit comparator",
                    "model_family": "PyGNOME",
                    "transport_model": "pygnome",
                    "provisional_transport_model": True,
                    "initialization_mode": "surrogate",
                    "current_source": "simplified",
                    "wind_source": "constant wind",
                    "wave_stokes_status": "not identical",
                    "structural_limitations": "benchmark only",
                    "forecast_path": str(service.output_dir / "pygnome_mask.tif"),
                    "empty_forecast_reason": "",
                },
            ]
            pairings = service._build_pairings(target_row, tracks)

        self.assertEqual(len(pairings), 3)
        self.assertEqual(set(pairings["track_id"].tolist()), {"R0_reinit_p50", "R1_previous_reinit_p50", PYGNOME_TRACK_ID})
        self.assertTrue(pairings["pair_role"].eq(PAIR_ROLE).all())
        self.assertTrue(pairings["obs_date"].eq(MARCH14_NOAA_SOURCE_DATE).all())

    def test_ranking_prefers_requested_tie_break_order(self):
        summary = pd.DataFrame(
            [
                {
                    "track_id": "R0_reinit_p50",
                    "model_name": "R0",
                    "mean_fss": 0.1,
                    "fss_1km": 0.0,
                    "iou": 0.0,
                    "nearest_distance_to_obs_m": 1000.0,
                    "track_tie_break_order": 1,
                },
                {
                    "track_id": "R1_previous_reinit_p50",
                    "model_name": "R1",
                    "mean_fss": 0.1,
                    "fss_1km": 0.0,
                    "iou": 0.0,
                    "nearest_distance_to_obs_m": 1000.0,
                    "track_tie_break_order": 0,
                },
                {
                    "track_id": PYGNOME_TRACK_ID,
                    "model_name": "PyGNOME",
                    "mean_fss": 0.1,
                    "fss_1km": 0.0,
                    "iou": 0.0,
                    "nearest_distance_to_obs_m": 1000.0,
                    "track_tie_break_order": 2,
                },
            ]
        )

        ranked = Phase3BExtendedPublicScoredMarch1314ReinitPyGnomeComparisonService._rank_tracks(summary)

        self.assertEqual(
            ranked["track_id"].tolist(),
            ["R1_previous_reinit_p50", "R0_reinit_p50", PYGNOME_TRACK_ID],
        )
        self.assertEqual(ranked.iloc[0]["rank"], 1)

    def test_existing_pygnome_nc_can_be_reused_when_gnome_is_unavailable(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            service = _service_stub(tmpdir)
            track_dir = service.track_dir / PYGNOME_TRACK_ID
            track_dir.mkdir(parents=True, exist_ok=True)
            nc_path = track_dir / "pygnome_reinit_deterministic_control.nc"
            nc_path.write_text("placeholder", encoding="utf-8")
            with patch(
                "src.services.phase3b_extended_public_scored_march13_14_reinit_pygnome_comparison.GNOME_AVAILABLE",
                False,
            ), patch.object(
                service,
                "_build_pygnome_local_date_product",
                return_value={
                    "forecast_path": str(track_dir / "pygnome_footprint_mask_2023-03-14_localdate.tif"),
                    "forecast_nonzero_cells_from_mask": 0,
                    "last_active_particle_time_utc": "",
                    "march14_local_timestamp_count": 0,
                    "march14_local_active_timestamp_count": 0,
                    "march14_local_timestamps": "",
                    "march14_local_active_timestamps": "",
                    "reached_march14_local_date": False,
                    "empty_forecast_reason": "pygnome_survival_did_not_reach_march14_local_date",
                },
            ):
                track = service._prepare_pygnome_track(
                    "cmems_era5",
                    {
                        "processed_vector_path": str(service.output_dir / "seed_polygon.gpkg"),
                        "reference_lat": 13.3,
                        "reference_lon": 121.5,
                    },
                )

        self.assertEqual(track["track_id"], PYGNOME_TRACK_ID)
        self.assertEqual(track["source_nc"], str(nc_path))

    def test_guardrail_detects_reused_reinit_mutation(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            service = _service_stub(tmpdir)
            reused = service.output_dir / "reused_summary.csv"
            reused.write_text("alpha\n", encoding="utf-8")
            service.locked_hashes_before = service._snapshot_locked_outputs()
            service.reused_output_paths = [reused]
            service.reused_hashes_before = service._snapshot_paths(service.reused_output_paths)
            service._verify_locked_outputs_unchanged()
            reused.write_text("changed\n", encoding="utf-8")
            with self.assertRaisesRegex(RuntimeError, "modified reused OpenDrift reinit outputs"):
                service._verify_locked_outputs_unchanged()


if __name__ == "__main__":
    unittest.main()
