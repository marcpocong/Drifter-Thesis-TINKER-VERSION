import tempfile
import unittest
from pathlib import Path

import geopandas as gpd
import numpy as np
from shapely.geometry import Polygon

from src.helpers.metrics import calculate_fss
from src.helpers.scoring import ScoringGridSpec, apply_ocean_mask
from src.services.shoreline_mask import build_shoreline_mask_artifacts


class ShorelineMaskTests(unittest.TestCase):
    def test_build_shoreline_mask_artifacts_from_local_land_source(self):
        polygon_wgs84 = Polygon(
            [
                (121.08, 13.04),
                (121.12, 13.04),
                (121.12, 13.08),
                (121.08, 13.08),
            ]
        )
        land_gdf = gpd.GeoDataFrame({"id": [1]}, geometry=[polygon_wgs84], crs="EPSG:4326")
        projected_bounds = land_gdf.to_crs("EPSG:32651").total_bounds
        spec = ScoringGridSpec(
            min_x=float(projected_bounds[0] - 2000.0),
            max_x=float(projected_bounds[2] + 2000.0),
            min_y=float(projected_bounds[1] - 2000.0),
            max_y=float(projected_bounds[3] + 2000.0),
            resolution=1000.0,
            crs="EPSG:32651",
            x_name="x",
            y_name="y",
            units="meters",
            display_bounds_wgs84=[121.05, 121.15, 13.01, 13.11],
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            manifest = build_shoreline_mask_artifacts(
                spec,
                force_refresh=True,
                artifact_dir=Path(tmpdir),
                land_source_gdf=land_gdf,
            )

            self.assertGreater(manifest["land_cell_count"], 0)
            self.assertGreater(manifest["sea_cell_count"], 0)
            self.assertGreater(manifest["segment_count"], 0)
            self.assertTrue(Path(manifest["land_mask_path"]).exists())
            self.assertTrue(Path(manifest["sea_mask_path"]).exists())
            self.assertTrue(Path(manifest["shoreline_segments_path"]).exists())
            self.assertTrue(str(manifest["shoreline_mask_status"]).startswith("gshhg_") or str(manifest["shoreline_mask_status"]).startswith("test_"))
            self.assertTrue(manifest["shoreline_mask_signature"])

    def test_apply_ocean_mask_and_fss_ignore_invalid_land_cells(self):
        forecast = np.array([[1.0, 1.0], [0.0, 0.0]], dtype=np.float32)
        observed = np.array([[1.0, 0.0], [0.0, 0.0]], dtype=np.float32)
        sea_mask = np.array([[1.0, 0.0], [1.0, 0.0]], dtype=np.float32)

        masked_forecast = apply_ocean_mask(forecast, sea_mask=sea_mask, fill_value=0.0)
        masked_observed = apply_ocean_mask(observed, sea_mask=sea_mask, fill_value=0.0)

        self.assertEqual(float(masked_forecast[0, 1]), 0.0)
        self.assertEqual(float(masked_observed[0, 1]), 0.0)
        self.assertLess(calculate_fss(forecast, observed, window=1), 1.0)
        self.assertEqual(
            calculate_fss(masked_forecast, masked_observed, window=1, valid_mask=sea_mask > 0.5),
            1.0,
        )


if __name__ == "__main__":
    unittest.main()
