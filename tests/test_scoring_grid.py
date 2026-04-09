import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import numpy as np
import rasterio
import geopandas as gpd
from rasterio.transform import from_origin
from shapely.geometry import LineString, Point, Polygon

from src.core.case_context import get_case_context


class TestScoringGrid(unittest.TestCase):
    def setUp(self):
        get_case_context.cache_clear()
        self.addCleanup(get_case_context.cache_clear)

    def test_official_grid_is_projected_metric_and_artifacts_exist(self):
        with mock.patch.dict(os.environ, {"WORKFLOW_MODE": "mindoro_retro_2023"}, clear=False):
            get_case_context.cache_clear()

            from src.helpers.scoring import build_official_scoring_grid, get_scoring_grid_artifact_paths

            def fake_shoreline_builder(spec, force_refresh=False, artifact_dir=None, land_source_gdf=None):
                artifacts = get_scoring_grid_artifact_paths()
                land = np.zeros((spec.height, spec.width), dtype=np.uint8)
                land[0:2, 0:2] = 1
                sea = np.where(land > 0, 0, 1).astype(np.uint8)
                transform = from_origin(spec.min_x, spec.max_y, spec.resolution, spec.resolution)
                for path, data in (
                    (artifacts["land_mask_tif"], land),
                    (artifacts["sea_mask_tif"], sea),
                ):
                    with rasterio.open(
                        path,
                        "w",
                        driver="GTiff",
                        height=spec.height,
                        width=spec.width,
                        count=1,
                        dtype=data.dtype,
                        crs=spec.crs,
                        transform=transform,
                        compress="lzw",
                    ) as dst:
                        dst.write(data, 1)
                segment_gdf = gpd.GeoDataFrame(
                    {"segment_id": ["shoreline_00001"], "length_m": [1000.0]},
                    geometry=[LineString([(spec.min_x, spec.max_y), (spec.min_x + 1000, spec.max_y)])],
                    crs=spec.crs,
                )
                segment_gdf.to_file(artifacts["shoreline_segments_gpkg"], driver="GPKG")
                payload = {
                    "shoreline_mask_status": "gshhg_test_resolution_i_level_1_all_touched",
                    "shoreline_mask_signature": "test-signature",
                    "land_cell_count": int(np.count_nonzero(land)),
                    "sea_cell_count": int(np.count_nonzero(sea)),
                    "segment_count": 1,
                }
                artifacts["shoreline_manifest_json"].write_text(json.dumps(payload), encoding="utf-8")
                artifacts["shoreline_manifest_csv"].write_text(
                    "shoreline_mask_status,shoreline_mask_signature,land_cell_count,sea_cell_count,segment_count\n"
                    "gshhg_test_resolution_i_level_1_all_touched,test-signature,4,{},1\n".format(int(np.count_nonzero(sea))),
                    encoding="utf-8",
                )
                return payload

            with tempfile.TemporaryDirectory() as tmp_dir:
                temp_root = Path(tmp_dir) / "grids"
                with mock.patch.multiple(
                    "src.helpers.scoring",
                    SCORING_GRID_DIR=temp_root,
                    SCORING_GRID_METADATA_PATH=temp_root / "scoring_grid.yaml",
                    SCORING_GRID_METADATA_JSON_PATH=temp_root / "scoring_grid.json",
                    SCORING_GRID_TEMPLATE_PATH=temp_root / "scoring_grid_template.tif",
                    SCORING_GRID_EXTENT_PATH=temp_root / "grid_extent.gpkg",
                    LAND_MASK_PATH=temp_root / "land_mask.tif",
                    SEA_MASK_PATH=temp_root / "sea_mask.tif",
                    SHORELINE_SEGMENTS_PATH=temp_root / "shoreline_segments.gpkg",
                    SHORELINE_MASK_MANIFEST_JSON_PATH=temp_root / "shoreline_mask_manifest.json",
                    SHORELINE_MASK_MANIFEST_CSV_PATH=temp_root / "shoreline_mask_manifest.csv",
                ):
                    with mock.patch("src.services.shoreline_mask.build_shoreline_mask_artifacts", side_effect=fake_shoreline_builder):
                        spec = build_official_scoring_grid(force_refresh=True)
                        artifacts = get_scoring_grid_artifact_paths()

                self.assertEqual(spec.crs, "EPSG:32651")
                self.assertEqual(spec.resolution, 1000.0)
                self.assertEqual(spec.units, "meters")
                self.assertGreater(spec.width, 0)
                self.assertGreater(spec.height, 0)
                self.assertIsNotNone(spec.display_bounds_wgs84)

                for key in (
                    "metadata_yaml",
                    "template_tif",
                    "extent_gpkg",
                    "land_mask_tif",
                    "sea_mask_tif",
                    "shoreline_segments_gpkg",
                    "shoreline_manifest_json",
                    "shoreline_manifest_csv",
                ):
                    self.assertTrue(artifacts[key].exists(), f"Missing artifact: {artifacts[key]}")

                with rasterio.open(artifacts["template_tif"]) as src:
                    self.assertEqual(src.crs.to_string(), "EPSG:32651")
                    self.assertEqual(src.res, (1000.0, 1000.0))
                    self.assertEqual(src.width, spec.width)
                    self.assertEqual(src.height, spec.height)

    def test_same_grid_precheck_reports_pass_and_fail_cases(self):
        with mock.patch.dict(os.environ, {"WORKFLOW_MODE": "mindoro_retro_2023"}, clear=False):
            get_case_context.cache_clear()

            from src.helpers.raster import GridBuilder, save_raster
            from src.helpers.scoring import precheck_same_grid

            grid = GridBuilder()
            data = np.zeros((grid.height, grid.width), dtype=np.float32)

            with tempfile.TemporaryDirectory() as tmp_dir:
                tmp_path = Path(tmp_dir)
                forecast_path = tmp_path / "forecast.tif"
                target_path = tmp_path / "target.tif"
                shifted_path = tmp_path / "shifted.tif"

                save_raster(grid, data, forecast_path)
                save_raster(grid, data, target_path)

                passing = precheck_same_grid(forecast_path, target_path, tmp_path / "passing")
                self.assertTrue(passing.passed)
                self.assertTrue(passing.csv_report_path.exists())
                self.assertTrue(passing.json_report_path.exists())

                shifted_transform = from_origin(
                    grid.min_x + grid.resolution,
                    grid.max_y,
                    grid.resolution,
                    grid.resolution,
                )
                with rasterio.open(
                    shifted_path,
                    "w",
                    driver="GTiff",
                    height=grid.height,
                    width=grid.width,
                    count=1,
                    dtype=data.dtype,
                    crs=grid.crs,
                    transform=shifted_transform,
                    compress="lzw",
                ) as dst:
                    dst.write(data, 1)

                failing = precheck_same_grid(forecast_path, shifted_path, tmp_path / "failing")
                self.assertFalse(failing.passed)
                self.assertFalse(failing.checks["transform_match"])

    def test_polygon_reference_point_uses_largest_polygon_without_union(self):
        from src.utils.io import _resolve_polygon_reference_point

        with tempfile.TemporaryDirectory() as tmp_dir:
            path = Path(tmp_dir) / "polygons.gpkg"
            small = Polygon([(120.0, 12.0), (120.01, 12.0), (120.01, 12.01), (120.0, 12.01)])
            large = Polygon([(121.0, 13.0), (121.2, 13.0), (121.2, 13.2), (121.0, 13.2)])
            gdf = gpd.GeoDataFrame({"id": [1, 2]}, geometry=[small, large], crs="EPSG:4326")
            gdf.to_file(path, driver="GPKG")

            lat, lon = _resolve_polygon_reference_point(path, geometry_type="polygon")
            self.assertTrue(large.contains(Point(lon, lat)) or large.touches(Point(lon, lat)))


if __name__ == "__main__":
    unittest.main()
