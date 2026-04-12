import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import numpy as np

from src.helpers.raster import GridBuilder, rasterize_observation_layer
from src.helpers.scoring import apply_ocean_mask
from src.services.public_obs_appendix import (
    BUFFERED_MARCH6_SUPPORT_BUFFER_M,
    PublicObservationAppendixService,
    build_buffered_march6_support_manifest,
    build_buffered_support_mask,
    buffered_march6_support_notice,
    classify_inventory_acceptance,
    is_within_current_horizon,
    parse_obs_date,
)

try:
    import geopandas as gpd
except ImportError:  # pragma: no cover - runtime guarded
    gpd = None

try:
    from shapely.geometry import box
except ImportError:  # pragma: no cover - runtime guarded
    box = None


class PublicObservationAppendixTests(unittest.TestCase):
    def test_parse_obs_date_handles_short_and_long_title_dates(self):
        self.assertEqual(parse_obs_date("MindoroOilSpill_Philsa_230304"), "2023-03-04")
        self.assertEqual(parse_obs_date("MindoroOilSpill_NOAA_20230331"), "2023-03-31")
        self.assertEqual(parse_obs_date("Acquired: 07/03/2023"), "2023-03-07")

    def test_within_current_horizon_is_inclusive(self):
        self.assertTrue(
            is_within_current_horizon(
                "2023-03-03",
                simulation_start_utc="2023-03-03T09:59:00Z",
                simulation_end_utc="2023-03-06T09:59:00Z",
            )
        )
        self.assertTrue(
            is_within_current_horizon(
                "2023-03-06",
                simulation_start_utc="2023-03-03T09:59:00Z",
                simulation_end_utc="2023-03-06T09:59:00Z",
            )
        )
        self.assertFalse(
            is_within_current_horizon(
                "2023-03-07",
                simulation_start_utc="2023-03-03T09:59:00Z",
                simulation_end_utc="2023-03-06T09:59:00Z",
            )
        )

    def test_classify_accepts_within_horizon_polygon_and_rejects_wrappers(self):
        accept_quant, accept_qual, rejection = classify_inventory_acceptance(
            public=True,
            source_type="feature service",
            observation_derived=True,
            reproducibly_ingestible=True,
            geometry_type="polygon",
            obs_date="2023-03-04",
            within_current_72h_horizon=True,
        )
        self.assertTrue(accept_quant)
        self.assertTrue(accept_qual)
        self.assertEqual(rejection, "")

        accept_quant, accept_qual, rejection = classify_inventory_acceptance(
            public=True,
            source_type="web mapping application",
            observation_derived=False,
            reproducibly_ingestible=False,
            geometry_type="unknown",
            obs_date="",
            within_current_72h_horizon=False,
        )
        self.assertFalse(accept_quant)
        self.assertTrue(accept_qual)
        self.assertIn("wrapper", rejection)

        accept_quant, accept_qual, rejection = classify_inventory_acceptance(
            public=True,
            source_type="feature service",
            observation_derived=True,
            reproducibly_ingestible=True,
            geometry_type="polygon",
            obs_date="2023-03-07",
            within_current_72h_horizon=False,
        )
        self.assertFalse(accept_quant)
        self.assertTrue(accept_qual)
        self.assertIn("extended-horizon", rejection)

    @unittest.skipIf(gpd is None or box is None, "geopandas/shapely are required for buffered support tests")
    def test_buffered_support_mask_expands_nonzero_cells(self):
        grid = GridBuilder(region=[0.0, 5000.0, 0.0, 5000.0], resolution=1000.0)
        source_gdf = gpd.GeoDataFrame(geometry=[box(1100.0, 1100.0, 1400.0, 1400.0)], crs=grid.crs)
        sea_mask = np.ones((grid.height, grid.width), dtype=np.float32)

        unbuffered = rasterize_observation_layer(source_gdf, grid)
        unbuffered = apply_ocean_mask(unbuffered, sea_mask=sea_mask, fill_value=0.0)
        _, buffered = build_buffered_support_mask(
            source_gdf,
            grid=grid,
            sea_mask=sea_mask,
            buffer_m=BUFFERED_MARCH6_SUPPORT_BUFFER_M,
        )

        self.assertGreater(int(np.count_nonzero(buffered > 0)), int(np.count_nonzero(unbuffered > 0)))

    @unittest.skipIf(gpd is None or box is None, "geopandas/shapely are required for buffered support tests")
    def test_buffered_support_mask_raises_when_ocean_mask_removes_all_cells(self):
        grid = GridBuilder(region=[0.0, 5000.0, 0.0, 5000.0], resolution=1000.0)
        source_gdf = gpd.GeoDataFrame(geometry=[box(1100.0, 1100.0, 1400.0, 1400.0)], crs=grid.crs)
        zero_sea_mask = np.zeros((grid.height, grid.width), dtype=np.float32)

        with self.assertRaisesRegex(RuntimeError, "zero ocean cells"):
            build_buffered_support_mask(
                source_gdf,
                grid=grid,
                sea_mask=zero_sea_mask,
                buffer_m=BUFFERED_MARCH6_SUPPORT_BUFFER_M,
            )

    def test_buffered_march6_manifest_block_records_buffer_and_guardrail(self):
        payload = build_buffered_march6_support_manifest(
            buffer_m=BUFFERED_MARCH6_SUPPORT_BUFFER_M,
            source_processed_vector=Path("processed.gpkg"),
            buffered_vector_path=Path("buffered.gpkg"),
            buffered_mask_path=Path("buffered.tif"),
            forecast_path=Path("mask_p50_2023-03-06_datecomposite.tif"),
            pairing_manifest_path=Path("pairing.csv"),
            fss_by_window_path=Path("fss.csv"),
            diagnostics_path=Path("diagnostics.csv"),
            summary_md_path=Path("summary.md"),
            qa_overlay_path=Path("overlay.png"),
            summary_row={
                "pair_id": "appendix_buffered_march6_1000m",
                "pair_role": "buffered_march6_support",
                "obs_date": "2023-03-06",
                "fss_1km": 0.1,
                "fss_3km": 0.2,
                "fss_5km": 0.3,
                "fss_10km": 0.4,
                "iou": 0.05,
                "dice": 0.1,
                "centroid_distance_m": 1234.0,
                "forecast_nonzero_cells": 7,
                "obs_nonzero_cells": 5,
            },
        )

        self.assertEqual(payload["buffer_m"], 1000.0)
        self.assertTrue(payload["official_strict_march6_unchanged"])
        self.assertIn("official main March 6 score unchanged", payload["note"])
        self.assertEqual(payload["artifacts"]["diagnostics"], "diagnostics.csv")

    def test_buffered_march6_notice_states_strict_result_is_unchanged(self):
        notice = buffered_march6_support_notice(BUFFERED_MARCH6_SUPPORT_BUFFER_M)
        self.assertIn("official main March 6 score unchanged", notice)
        self.assertIn("1000.0 m buffer", notice)

    def test_verify_locked_phase3b_files_unchanged_detects_mutation(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            locked_a = Path(tmpdir) / "locked_a.csv"
            locked_b = Path(tmpdir) / "locked_b.csv"
            locked_a.write_text("alpha\n", encoding="utf-8")
            locked_b.write_text("beta\n", encoding="utf-8")

            service = PublicObservationAppendixService.__new__(PublicObservationAppendixService)
            with patch(
                "src.services.public_obs_appendix.OFFICIAL_LOCKED_PHASE3B_FILES",
                [locked_a, locked_b],
            ):
                service.main_phase3b_hashes_before = service._snapshot_locked_phase3b_files()
                service._verify_locked_phase3b_files_unchanged()
                locked_a.write_text("changed\n", encoding="utf-8")
                with self.assertRaisesRegex(RuntimeError, "locked official Phase 3B files"):
                    service._verify_locked_phase3b_files_unchanged()


if __name__ == "__main__":
    unittest.main()
