import json
import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
from matplotlib import pyplot as plt
from rasterio.transform import from_origin
from shapely.geometry import LineString

import src.__main__ as entrypoint
from src.services.prototype_pygnome_similarity_summary import (
    CASE_LOCAL_SUPPORT_CONTEXT_MODE,
    PrototypePygnomeSimilaritySummaryService,
)


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _write_mask(
    path: Path,
    active_row_offset: int,
    active_col_offset: int,
    *,
    mask_height: int = 6,
    mask_width: int = 6,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    data = np.zeros((170, 140), dtype=np.uint8)
    data[
        120 + active_row_offset : 120 + active_row_offset + mask_height,
        126 + active_col_offset : 126 + active_col_offset + mask_width,
    ] = 1
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=data.shape[0],
        width=data.shape[1],
        count=1,
        dtype=data.dtype,
        crs="EPSG:4326",
        transform=from_origin(115.0, 14.5, 0.05, 0.05),
        ) as dataset:
        dataset.write(data, 1)


def _count_palette_pixels(path: Path, color_hexes: tuple[str, ...]) -> int:
    rgb = plt.imread(path)[..., :3]
    if float(np.nanmax(rgb)) > 1.0:
        rgb = rgb / 255.0
    match_mask = np.zeros(rgb.shape[:2], dtype=bool)
    for color_hex in color_hexes:
        token = color_hex.lstrip("#")
        target = np.array([int(token[idx : idx + 2], 16) for idx in (0, 2, 4)], dtype=float) / 255.0
        distance = np.sqrt(np.sum((rgb - target) ** 2, axis=2))
        match_mask |= distance < 0.22
    return int(match_mask.sum())


def _count_nonwhite_region(path: Path, *, left_frac: float, right_frac: float, top_frac: float, bottom_frac: float) -> int:
    rgb = plt.imread(path)[..., :3]
    if float(np.nanmax(rgb)) > 1.0:
        rgb = rgb / 255.0
    height, width, _ = rgb.shape
    x0 = max(0, min(width, int(round(width * left_frac))))
    x1 = max(x0 + 1, min(width, int(round(width * right_frac))))
    y0 = max(0, min(height, int(round(height * top_frac))))
    y1 = max(y0 + 1, min(height, int(round(height * bottom_frac))))
    region = rgb[y0:y1, x0:x1]
    return int(((region < 0.965).any(axis=2)).sum())


def _write_prototype_context(root: Path) -> None:
    grids_dir = root / "data_processed" / "grids"
    grids_dir.mkdir(parents=True, exist_ok=True)
    labels_path = root / "config" / "publication_map_labels_mindoro.csv"
    labels_path.parent.mkdir(parents=True, exist_ok=True)
    labels_path.write_text(
        "label_id,label_text,lon,lat,label_group,enabled_yes_no,notes\n"
        "mindoro_island,Mindoro Island,121.15,12.96,major_landmass,yes,Prototype locator label\n"
        "tablas_strait,Tablas Strait,121.88,12.78,major_waterbody,yes,Prototype locator label\n"
        "south_east_mindoro_coast,Southeast Mindoro coast,121.55,12.69,coast_context,yes,Prototype locator label\n",
        encoding="utf-8",
    )

    land_mask_path = grids_dir / "land_mask.tif"
    land_data = np.zeros((169, 124), dtype=np.uint8)
    land_data[10:165, :58] = 1
    with rasterio.open(
        land_mask_path,
        "w",
        driver="GTiff",
        height=land_data.shape[0],
        width=land_data.shape[1],
        count=1,
        dtype=land_data.dtype,
        crs="EPSG:32651",
        transform=from_origin(274000.0, 1524000.0, 1000.0, 1000.0),
    ) as dataset:
        dataset.write(land_data, 1)

    shoreline_path = grids_dir / "shoreline_segments.gpkg"
    shoreline = gpd.GeoDataFrame(
        {
            "segment_id": ["seg_1", "seg_2"],
            "length_m": [10000.0, 12000.0],
            "geometry": [
                LineString([(334000.0, 1370000.0), (336000.0, 1450000.0), (338000.0, 1510000.0)]),
                LineString([(346000.0, 1375000.0), (347500.0, 1435000.0), (349000.0, 1495000.0)]),
            ],
        },
        crs="EPSG:32651",
    )
    shoreline.to_file(shoreline_path, driver="GPKG")

    scoring_grid_json = grids_dir / "scoring_grid.json"
    scoring_grid_json.write_text(
        json.dumps(
            {
                "display_bounds_wgs84": [120.90964677179262, 122.0621541786303, 12.249384840763462, 13.783655303175253],
                "land_mask_path": "data_processed/grids/land_mask.tif",
                "shoreline_segments_path": "data_processed/grids/shoreline_segments.gpkg",
            },
            indent=2,
        ),
        encoding="utf-8",
    )


def _write_case(
    root: Path,
    case_id: str,
    fss_5km_values: tuple[float, float, float],
    kl_values: tuple[float, float, float],
    include_metadata: bool = True,
    mask_shape: tuple[int, int] = (6, 6),
    legacy_deterministic_only: bool = False,
    grid_bounds: tuple[float, float, float, float] = (120.9, 121.9, 7.8, 9.2),
) -> None:
    benchmark_dir = root / "output" / case_id / "benchmark"
    benchmark_dir.mkdir(parents=True, exist_ok=True)
    _write_json(
        benchmark_dir / "grid" / "grid.json",
        {
            "display_bounds_wgs84": list(grid_bounds),
            "extent": list(grid_bounds),
        },
    )

    hours = (24, 48, 72)
    track_rows = {
        "deterministic": {
            "label": "OpenDrift deterministic",
            "fss_5km": fss_5km_values,
            "kl": kl_values,
            "mask_offset": 0,
        },
        "ensemble_p50": {
            "label": "OpenDrift p50 threshold",
            "fss_5km": tuple(min(0.99, value + 0.08) for value in fss_5km_values),
            "kl": tuple(max(0.01, value - 1.0) for value in kl_values),
            "mask_offset": 1,
        },
        "ensemble_p90": {
            "label": "OpenDrift p90 threshold",
            "fss_5km": tuple(min(0.99, value + 0.03) for value in fss_5km_values),
            "kl": tuple(max(0.01, value - 0.4) for value in kl_values),
            "mask_offset": 2,
        },
    }
    fss_rows = []
    for comparison_track_id, track in track_rows.items():
        window_values = {
            1: (0.05, 0.05, 0.05),
            3: (0.20, 0.20, 0.20),
            5: track["fss_5km"],
            10: (0.80, 0.80, 0.80),
        }
        for window_km, values in window_values.items():
            for hour, value in zip(hours, values):
                fss_rows.append(
                    {
                        "comparison_track_id": comparison_track_id,
                        "comparison_track_label": track["label"],
                        "timestamp_utc": f"2016-09-01T{hour:02d}:00:00Z",
                        "hour": hour,
                        "window_km": window_km,
                        "fss": value,
                    }
                )
    fss_df = pd.DataFrame(fss_rows)
    if legacy_deterministic_only:
        fss_df = fss_df[fss_df["comparison_track_id"] == "deterministic"][
            ["timestamp_utc", "hour", "window_km", "fss"]
        ].reset_index(drop=True)
    fss_df.to_csv(benchmark_dir / "phase3a_fss_by_time_window.csv", index=False)

    kl_rows = []
    for comparison_track_id, track in track_rows.items():
        kl_rows.extend(
            {
                "comparison_track_id": comparison_track_id,
                "comparison_track_label": track["label"],
                "hour": hour,
                "kl_divergence": value,
                "epsilon": 1.0e-12,
                "ocean_cell_count": 42,
            }
            for hour, value in zip(hours, track["kl"])
        )
    kl_df = pd.DataFrame(kl_rows)
    if legacy_deterministic_only:
        kl_df = kl_df[kl_df["comparison_track_id"] == "deterministic"][
            ["hour", "kl_divergence", "epsilon", "ocean_cell_count"]
        ].reset_index(drop=True)
    kl_df.to_csv(benchmark_dir / "phase3a_kl_by_time.csv", index=False)

    summary_rows = []
    for comparison_track_id, track in track_rows.items():
        summary_rows.extend(
            [
                {
                    "comparison_track_id": comparison_track_id,
                    "comparison_track_label": track["label"],
                    "metric": "FSS",
                    "window_km": 5,
                    "pair_count": 3,
                    "mean_value": sum(track["fss_5km"]) / 3.0,
                    "min_value": min(track["fss_5km"]),
                    "max_value": max(track["fss_5km"]),
                    "notes": "Prototype comparator summary.",
                },
                {
                    "comparison_track_id": comparison_track_id,
                    "comparison_track_label": track["label"],
                    "metric": "KL",
                    "window_km": "",
                    "pair_count": 3,
                    "mean_value": sum(track["kl"]) / 3.0,
                    "min_value": min(track["kl"]),
                    "max_value": max(track["kl"]),
                    "notes": "Prototype comparator summary.",
                },
            ]
        )
    summary_df = pd.DataFrame(summary_rows)
    if legacy_deterministic_only:
        summary_df = summary_df[summary_df["comparison_track_id"] == "deterministic"][
            ["metric", "window_km", "pair_count", "mean_value", "min_value", "max_value", "notes"]
        ].reset_index(drop=True)
    summary_df.to_csv(benchmark_dir / "phase3a_summary.csv", index=False)

    pairing_rows = []
    for idx, hour in enumerate(hours):
        timestamp_utc = f"2016-09-{idx + 2:02d}T00:00:00Z"
        timestamp_token = timestamp_utc.replace(":", "-")
        pygnome_path = benchmark_dir / "pygnome" / f"pygnome_footprint_mask_{timestamp_token}.tif"
        pygnome_density = benchmark_dir / "pygnome" / f"pygnome_density_norm_{timestamp_token}.tif"
        _write_mask(pygnome_path, idx, 2, mask_height=mask_shape[0], mask_width=mask_shape[1])
        _write_mask(pygnome_density, idx, 2, mask_height=mask_shape[0], mask_width=mask_shape[1])
        for comparison_track_id, track in track_rows.items():
            track_dir = benchmark_dir / comparison_track_id
            control_path = track_dir / f"{comparison_track_id}_footprint_mask_{timestamp_token}.tif"
            control_density = track_dir / f"{comparison_track_id}_density_norm_{timestamp_token}.tif"
            overlay_path = benchmark_dir / "qa" / f"{comparison_track_id}_overlay_{timestamp_token}.png"
            precheck_foot = benchmark_dir / "precheck" / f"footprint_{comparison_track_id}_{timestamp_token}.json"
            precheck_density = benchmark_dir / "precheck" / f"density_{comparison_track_id}_{timestamp_token}.json"

            _write_mask(control_path, idx + int(track["mask_offset"]), 0, mask_height=mask_shape[0], mask_width=mask_shape[1])
            _write_mask(control_density, idx + int(track["mask_offset"]), 0, mask_height=mask_shape[0], mask_width=mask_shape[1])
            _write_json(precheck_foot, {"timestamp_utc": timestamp_utc})
            _write_json(precheck_density, {"timestamp_utc": timestamp_utc})
            overlay_path.parent.mkdir(parents=True, exist_ok=True)
            plt.imsave(overlay_path, np.ones((20, 30, 3), dtype=float))

            if legacy_deterministic_only and comparison_track_id != "deterministic":
                continue
            pairing_rows.append(
                {
                    "comparison_track_id": comparison_track_id,
                    "comparison_track_label": track["label"],
                    "timestamp_utc": timestamp_utc,
                    "hour": hour,
                    "opendrift_footprint_path": str(control_path.relative_to(root)).replace("\\", "/"),
                    "pygnome_footprint_path": str(pygnome_path.relative_to(root)).replace("\\", "/"),
                    "opendrift_density_path": str(control_density.relative_to(root)).replace("\\", "/"),
                    "pygnome_density_path": str(pygnome_density.relative_to(root)).replace("\\", "/"),
                    "control_footprint_path": str(control_path.relative_to(root)).replace("\\", "/"),
                    "control_density_path": str(control_density.relative_to(root)).replace("\\", "/"),
                    "footprint_precheck_json": str(precheck_foot.relative_to(root)).replace("\\", "/"),
                    "density_precheck_json": str(precheck_density.relative_to(root)).replace("\\", "/"),
                    "qa_overlay_path": str(overlay_path.relative_to(root)).replace("\\", "/"),
                    "pygnome_mass_strategy": "mass",
                    "opendrift_density_ocean_sum": 1.0,
                    "pygnome_density_ocean_sum": 1.0,
                }
            )
    pairing_df = pd.DataFrame(pairing_rows)
    if legacy_deterministic_only:
        pairing_df = pairing_df[
            [
                "timestamp_utc",
                "hour",
                "control_footprint_path",
                "pygnome_footprint_path",
                "control_density_path",
                "pygnome_density_path",
                "footprint_precheck_json",
                "density_precheck_json",
                "qa_overlay_path",
                "pygnome_mass_strategy",
                "opendrift_density_ocean_sum",
                "pygnome_density_ocean_sum",
            ]
        ].reset_index(drop=True)
    pairing_df.to_csv(benchmark_dir / "phase3a_pairing_manifest.csv", index=False)

    if include_metadata:
        _write_json(
            benchmark_dir / "pygnome" / "pygnome_benchmark_metadata.json",
            {
                "weathering_enabled": False,
                "benchmark_particles": 2500,
            },
        )

    source_point_path = root / "data" / "arcgis" / case_id / "source_point_metadata.geojson"
    _write_json(
        source_point_path,
        {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "properties": {"case_id": case_id},
                    "geometry": {"type": "Point", "coordinates": [121.35, 8.35]},
                }
            ],
        },
    )


class PrototypePygnomeSimilaritySummaryTests(unittest.TestCase):
    def test_service_builds_summary_package_and_ranks_by_fss_then_kl(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "config").mkdir(parents=True, exist_ok=True)
            (root / "config" / "settings.yaml").write_text(
                "phase_1_start_date:\n"
                "  - 2016-09-01\n"
                "  - 2016-09-06\n"
                "  - 2016-09-17\n",
                encoding="utf-8",
            )
            _write_prototype_context(root)

            _write_case(root, "CASE_2016-09-01", (0.60, 0.60, 0.60), (12.0, 12.0, 12.0))
            _write_case(root, "CASE_2016-09-06", (0.60, 0.60, 0.60), (10.0, 10.0, 10.0))
            _write_case(root, "CASE_2016-09-17", (0.40, 0.40, 0.40), (15.0, 15.0, 15.0))

            service = PrototypePygnomeSimilaritySummaryService(repo_root=root)
            results = service.run()

            for key in (
                "case_registry_csv",
                "similarity_by_case_csv",
                "fss_by_case_window_csv",
                "kl_by_case_hour_csv",
                "figure_registry_csv",
                "figure_captions_md",
                "manifest_json",
                "summary_md",
            ):
                self.assertTrue(Path(results[key]).exists(), key)

            for figure_path in results["qa_figures"]:
                self.assertTrue(Path(figure_path).exists(), figure_path)
            for figure_path in results["forecast_figure_paths"]:
                self.assertTrue(Path(figure_path).exists(), figure_path)

            self.assertEqual(results["top_ranked_case_id"], "CASE_2016-09-06")
            self.assertEqual(results["case_count"], 3)
            self.assertEqual(results["single_figure_count"], 36)
            self.assertEqual(results["board_figure_count"], 3)

            similarity_df = pd.read_csv(results["similarity_by_case_csv"])
            deterministic_df = similarity_df[similarity_df["comparison_track_id"] == "deterministic"].reset_index(drop=True)
            self.assertEqual(
                deterministic_df["case_id"].tolist(),
                ["CASE_2016-09-06", "CASE_2016-09-01", "CASE_2016-09-17"],
            )
            self.assertEqual(deterministic_df["relative_similarity_rank"].tolist(), [1, 2, 3])
            self.assertTrue((similarity_df["pygnome_weathering_enabled"] == False).all())

            summary_text = Path(results["summary_md"]).read_text(encoding="utf-8")
            self.assertIn("PyGNOME is a comparator, not truth", summary_text)
            self.assertIn("Rank 1: `CASE_2016-09-06`", summary_text)
            self.assertIn("OpenDrift p50 threshold", summary_text)
            self.assertIn("exact stored raster cells and exact footprint outlines", summary_text)
            captions_text = Path(results["figure_captions_md"]).read_text(encoding="utf-8")
            self.assertIn("CASE_2016-09-01", captions_text)
            self.assertIn("board", captions_text)
            self.assertIn("exact stored raster cells", captions_text)
            self.assertIn("OpenDrift p50 threshold", captions_text)
            self.assertIn("OpenDrift p90 threshold", captions_text)
            self.assertNotIn("canonical Mindoro", captions_text)

            figure_registry_df = pd.read_csv(results["figure_registry_csv"])
            self.assertEqual(len(figure_registry_df), 39)
            self.assertEqual((figure_registry_df["view_type"] == "single").sum(), 36)
            self.assertEqual((figure_registry_df["view_type"] == "board").sum(), 3)
            self.assertEqual(sorted(figure_registry_df["hour"].dropna().astype(int).unique().tolist()), [24, 48, 72])
            self.assertEqual(
                sorted(figure_registry_df["model_name"].unique().tolist()),
                ["opendrift", "opendrift_p50", "opendrift_p90", "opendrift_vs_pygnome", "pygnome"],
            )
            self.assertTrue((figure_registry_df["geometry_render_mode"] == "exact_stored_raster").all())
            self.assertTrue(figure_registry_df["density_render_mode"].astype(str).str.len().gt(0).all())
            self.assertTrue(figure_registry_df["stored_geometry_status"].astype(str).str.len().gt(0).all())
            self.assertTrue((figure_registry_df["comparison_track_id"].notna()).all())
            self.assertIn("status_key", figure_registry_df.columns)
            self.assertIn("status_label", figure_registry_df.columns)
            self.assertTrue((figure_registry_df["status_key"] == "prototype_2016_support").all())
            self.assertFalse(figure_registry_df["notes"].astype(str).str.contains("Mindoro", regex=False).any())

            manifest = json.loads(Path(results["manifest_json"]).read_text(encoding="utf-8"))
            self.assertTrue(manifest["legacy_debug_only"])
            self.assertEqual(manifest["pygnome_role"], "comparator_only")
            self.assertEqual(manifest["support_context_mode"], CASE_LOCAL_SUPPORT_CONTEXT_MODE)
            self.assertEqual(manifest["rendering_profile"], "prototype_2016_case_local_projected_v1")
            self.assertEqual(manifest["map_projection"], "local_azimuthal_equidistant")
            self.assertIn("CASE_2016-09-01", manifest["case_rendering"])
            self.assertEqual(manifest["headline"]["top_ranked_case_id"], "CASE_2016-09-06")
            self.assertEqual(manifest["headline"]["top_ranked_comparison_track_id"], "deterministic")
            self.assertEqual(manifest["figure_counts"]["single_forecast_figures"], 36)
            self.assertEqual(manifest["figure_counts"]["comparison_boards"], 3)
            self.assertIn("Prototype 2016 legacy debug support", summary_text)
            self.assertIn("Provenance:", captions_text)

    def test_sparse_masks_use_tight_crop_and_render_visible_footprints(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "config").mkdir(parents=True, exist_ok=True)
            (root / "config" / "settings.yaml").write_text(
                "phase_1_start_date:\n"
                "  - 2016-09-01\n"
                "  - 2016-09-06\n"
                "  - 2016-09-17\n",
                encoding="utf-8",
            )
            _write_prototype_context(root)

            sparse_shape = (7, 1)
            _write_case(root, "CASE_2016-09-01", (0.60, 0.60, 0.60), (12.0, 12.0, 12.0), mask_shape=sparse_shape)
            _write_case(root, "CASE_2016-09-06", (0.60, 0.60, 0.60), (10.0, 10.0, 10.0), mask_shape=sparse_shape)
            _write_case(root, "CASE_2016-09-17", (0.40, 0.40, 0.40), (15.0, 15.0, 15.0), mask_shape=sparse_shape)

            service = PrototypePygnomeSimilaritySummaryService(repo_root=root)
            context = service._load_prototype_map_context()
            self.assertIsNone(context["land_mask_path"])
            self.assertIsNone(context["shoreline_path"])
            self.assertTrue(context["labels_df"].empty)
            self.assertEqual(len(context["full_bounds_wgs84"]), 4)
            case_artifacts = service._load_case_artifacts("CASE_2016-09-01")
            self.assertEqual(case_artifacts["display_bounds"], (120.9, 121.9, 7.8, 9.2))
            crop_bounds = case_artifacts["crop_bounds"]
            self.assertLess(crop_bounds[1] - crop_bounds[0], case_artifacts["display_bounds"][1] - case_artifacts["display_bounds"][0])
            self.assertLess(crop_bounds[3] - crop_bounds[2], case_artifacts["display_bounds"][3] - case_artifacts["display_bounds"][2])

            results = service.run()
            registry_df = pd.read_csv(results["figure_registry_csv"])
            self.assertTrue((registry_df["extent_mode"] == "dynamic_forecast_extent").all())
            self.assertTrue(registry_df["plot_bounds_wgs84"].astype(str).str.len().gt(0).all())
            row = registry_df[
                (registry_df["case_id"] == "CASE_2016-09-01")
                & (registry_df["hour"] == 24)
                & (registry_df["model_name"] == "opendrift")
            ].iloc[0]
            rendered_path = Path(row["file_path"])
            self.assertGreater(
                _count_palette_pixels(rendered_path, ("#165ba8", "#4f88c5", "#9fc1e6")),
                3000,
            )
            self.assertGreater(
                _count_palette_pixels(rendered_path, ("#1d9b1d",)),
                40,
            )
            board_row = registry_df[
                (registry_df["case_id"] == "CASE_2016-09-01")
                & (registry_df["view_type"] == "board")
            ].iloc[0]
            board_path = Path(board_row["file_path"])
            self.assertGreater(
                _count_nonwhite_region(board_path, left_frac=0.68, right_frac=0.98, top_frac=0.02, bottom_frac=0.76),
                2500,
            )
            self.assertGreater(
                _count_palette_pixels(board_path, ("#1d9b1d",)),
                40,
            )

    def test_service_repairs_legacy_deterministic_only_case_manifests(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "config").mkdir(parents=True, exist_ok=True)
            (root / "config" / "settings.yaml").write_text(
                "phase_1_start_date:\n"
                "  - 2016-09-01\n"
                "  - 2016-09-06\n"
                "  - 2016-09-17\n",
                encoding="utf-8",
            )
            _write_prototype_context(root)

            _write_case(root, "CASE_2016-09-01", (0.60, 0.60, 0.60), (12.0, 12.0, 12.0), legacy_deterministic_only=True)
            _write_case(root, "CASE_2016-09-06", (0.60, 0.60, 0.60), (10.0, 10.0, 10.0))
            _write_case(root, "CASE_2016-09-17", (0.40, 0.40, 0.40), (15.0, 15.0, 15.0))

            service = PrototypePygnomeSimilaritySummaryService(repo_root=root)
            results = service.run()

            repaired_fss = pd.read_csv(root / "output" / "CASE_2016-09-01" / "benchmark" / "phase3a_fss_by_time_window.csv")
            repaired_kl = pd.read_csv(root / "output" / "CASE_2016-09-01" / "benchmark" / "phase3a_kl_by_time.csv")
            repaired_pairings = pd.read_csv(root / "output" / "CASE_2016-09-01" / "benchmark" / "phase3a_pairing_manifest.csv")
            repaired_summary = pd.read_csv(root / "output" / "CASE_2016-09-01" / "benchmark" / "phase3a_summary.csv")

            self.assertEqual(
                sorted(repaired_fss["comparison_track_id"].astype(str).unique().tolist()),
                ["deterministic", "ensemble_p50", "ensemble_p90"],
            )
            self.assertEqual(
                sorted(repaired_kl["comparison_track_id"].astype(str).unique().tolist()),
                ["deterministic", "ensemble_p50", "ensemble_p90"],
            )
            self.assertEqual(
                sorted(repaired_pairings["comparison_track_id"].astype(str).unique().tolist()),
                ["deterministic", "ensemble_p50", "ensemble_p90"],
            )
            self.assertEqual(
                sorted(repaired_summary["comparison_track_id"].astype(str).unique().tolist()),
                ["deterministic", "ensemble_p50", "ensemble_p90"],
            )

            skipped_df = pd.read_csv(results["skipped_cases_csv"])
            self.assertTrue(skipped_df.empty)

    def test_service_skips_incomplete_cases_and_records_reason(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "config").mkdir(parents=True, exist_ok=True)
            (root / "config" / "settings.yaml").write_text(
                "phase_1_start_date:\n"
                "  - 2016-09-01\n"
                "  - 2016-09-06\n"
                "  - 2016-09-17\n",
                encoding="utf-8",
            )
            _write_prototype_context(root)

            _write_case(root, "CASE_2016-09-01", (0.55, 0.55, 0.55), (10.0, 10.0, 10.0))
            _write_case(root, "CASE_2016-09-06", (0.50, 0.50, 0.50), (11.0, 11.0, 11.0), include_metadata=False)
            _write_case(root, "CASE_2016-09-17", (0.45, 0.45, 0.45), (12.0, 12.0, 12.0))

            service = PrototypePygnomeSimilaritySummaryService(repo_root=root)
            results = service.run()

            self.assertEqual(results["case_count"], 2)
            self.assertEqual(results["configured_case_count"], 3)
            self.assertEqual(results["processed_case_ids"], ["CASE_2016-09-01", "CASE_2016-09-17"])
            self.assertEqual(len(results["skipped_cases"]), 1)
            self.assertEqual(results["skipped_cases"][0]["case_id"], "CASE_2016-09-06")
            self.assertIn("metadata_json", results["skipped_cases"][0]["error_message"])

            skipped_df = pd.read_csv(results["skipped_cases_csv"])
            self.assertEqual(skipped_df["case_id"].tolist(), ["CASE_2016-09-06"])
            self.assertEqual(skipped_df["error_type"].tolist(), ["FileNotFoundError"])

            summary_text = Path(results["summary_md"]).read_text(encoding="utf-8")
            captions_text = Path(results["figure_captions_md"]).read_text(encoding="utf-8")
            self.assertIn("Skipped cases:", summary_text)
            self.assertIn("CASE_2016-09-06", summary_text)
            self.assertIn("## Skipped Cases", captions_text)
            self.assertIn("CASE_2016-09-06", captions_text)

            manifest = json.loads(Path(results["manifest_json"]).read_text(encoding="utf-8"))
            self.assertEqual(manifest["processed_case_ids"], ["CASE_2016-09-01", "CASE_2016-09-17"])
            self.assertEqual(len(manifest["skipped_cases"]), 1)
            self.assertEqual(manifest["skipped_cases"][0]["case_id"], "CASE_2016-09-06")

    def test_main_dispatches_new_similarity_phase(self):
        with mock.patch.dict(os.environ, {"PIPELINE_PHASE": "prototype_pygnome_similarity_summary"}, clear=False):
            with mock.patch.object(entrypoint, "run_prototype_pygnome_similarity_summary_phase") as mock_phase:
                entrypoint.main()

        mock_phase.assert_called_once_with()

    def test_main_dispatches_legacy_final_figure_phase(self):
        with mock.patch.dict(os.environ, {"PIPELINE_PHASE": "prototype_legacy_final_figures"}, clear=False):
            with mock.patch.object(entrypoint, "run_prototype_legacy_final_figures_phase") as mock_phase:
                entrypoint.main()

        mock_phase.assert_called_once_with()

    def test_launcher_matrix_includes_similarity_step_after_benchmark(self):
        repo_root = Path(__file__).resolve().parents[1]
        launcher_matrix = json.loads((repo_root / "config" / "launcher_matrix.json").read_text(encoding="utf-8"))
        entry = next(item for item in launcher_matrix["entries"] if item["entry_id"] == "prototype_legacy_bundle")
        phases = [step["phase"] for step in entry["steps"]]

        self.assertIn("prototype_pygnome_similarity_summary", phases)
        self.assertLess(phases.index("benchmark"), phases.index("prototype_pygnome_similarity_summary"))
        self.assertLess(phases.index("prototype_pygnome_similarity_summary"), phases.index("prototype_legacy_phase4_weathering"))
        self.assertLess(phases.index("prototype_legacy_phase4_weathering"), phases.index("prototype_legacy_final_figures"))
        self.assertNotIn("3b", phases)


if __name__ == "__main__":
    unittest.main()
