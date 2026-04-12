import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import numpy as np
import pandas as pd
import rasterio
from matplotlib import pyplot as plt
from rasterio.transform import from_bounds

from src.services.prototype_legacy_final_figures import PrototypeLegacyFinalFiguresService


STYLE_YAML = """
palette:
  background_land: "#e6dfd1"
  background_sea: "#f7fbfd"
  shoreline: "#8b8178"
  observed_mask: "#2f3a46"
  deterministic_opendrift: "#165ba8"
  ensemble_consolidated: "#0f766e"
  ensemble_p50: "#1f7a4d"
  ensemble_p90: "#72b6ff"
  pygnome: "#9b4dca"
  source_point: "#b42318"
  initialization_polygon: "#d97706"
  validation_polygon: "#0f172a"
  centroid_path: "#111827"
  corridor_hull: "#c2410c"
  ensemble_member_path: "#94a3b8"
  oil_lighter: "#f28c28"
  oil_base: "#8c564b"
  oil_heavier: "#4b0082"
legend_labels:
  observed_mask: "Observed spill extent"
  deterministic_opendrift: "OpenDrift deterministic forecast"
  ensemble_consolidated: "OpenDrift consolidated ensemble trajectory"
  ensemble_p50: "OpenDrift ensemble p50 footprint"
  ensemble_p90: "OpenDrift ensemble p90 footprint"
  pygnome: "PyGNOME comparator"
  source_point: "Source point"
  initialization_polygon: "Initialization polygon"
  validation_polygon: "Validation target polygon"
  centroid_path: "Centroid path"
  corridor_hull: "Corridor / hull"
  ensemble_member_path: "Sampled ensemble trajectories"
  oil_lighter: "Light oil scenario"
  oil_base: "Fixed base medium-heavy proxy"
  oil_heavier: "Heavier oil scenario"
typography:
  title_size: 19
  subtitle_size: 10
  panel_title_size: 11
  legend_title_size: 9
  body_size: 9
  note_size: 8
  font_family: "Arial"
layout:
  board_size_inches: [16, 9]
  single_size_inches: [13, 8]
  dpi: 120
  figure_facecolor: "#ffffff"
  axes_facecolor: "#f7fbfd"
  grid_color: "#cbd5e1"
  legend_facecolor: "#ffffff"
  legend_edgecolor: "#94a3b8"
crop_rules:
  zoom_padding_fraction: 0.18
  close_padding_fraction: 0.08
  minimum_padding_m: 4000
  minimum_crop_span_m: 12000
locator_rules:
  mindoro_scale_km: 25
  dwh_scale_km: 100
  locator_padding_fraction: 0.55
"""


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _write_png(path: Path, width: int = 640, height: int = 360) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    image = np.ones((height, width, 3), dtype=float)
    image[:, :, 0] = 0.92
    image[:, :, 1] = 0.95
    image[:, :, 2] = 0.99
    plt.imsave(path, image)


def _write_raster(path: Path, array: np.ndarray, bounds: tuple[float, float, float, float]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    height, width = array.shape
    transform = from_bounds(*bounds, width=width, height=height)
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=height,
        width=width,
        count=1,
        dtype="float32",
        crs="EPSG:4326",
        transform=transform,
        nodata=0.0,
    ) as dataset:
        dataset.write(array.astype(np.float32), 1)


def _placeholder_map_writer(*args, **kwargs) -> None:
    output_file = kwargs.get("output_file") or args[0]
    _write_png(Path(output_file))


class PrototypeLegacyFinalFiguresTests(unittest.TestCase):
    bounds = (115.0, 122.0, 6.0, 14.5)
    case_id = "CASE_2016-09-01"

    def _build_fixture(self, root: Path, *, include_benchmark: bool) -> None:
        _write_text(root / "config" / "publication_figure_style.yaml", STYLE_YAML)
        _write_text(root / "config" / "settings.yaml", "phase_1_start_date:\n  - 2016-09-01\n")

        drifter_df = pd.DataFrame(
            {
                "time": [
                    "2016-09-01T00:00:00Z",
                    "2016-09-02T00:00:00Z",
                    "2016-09-03T00:00:00Z",
                    "2016-09-04T00:00:00Z",
                ],
                "lat": [10.415, 10.445, 10.470, 10.500],
                "lon": [117.180, 117.225, 117.270, 117.315],
                "ID": ["DRIFTER_A", "DRIFTER_A", "DRIFTER_A", "DRIFTER_A"],
            }
        )
        drifter_path = root / "data" / "drifters" / self.case_id / "drifters_noaa.csv"
        drifter_path.parent.mkdir(parents=True, exist_ok=True)
        drifter_df.to_csv(drifter_path, index=False)

        ensemble_dir = root / "output" / self.case_id / "ensemble"
        _write_json(
            ensemble_dir / "metadata.json",
            {
                "grid": {
                    "display_bounds_wgs84": list(self.bounds),
                    "extent": list(self.bounds),
                }
            },
        )
        probability = np.zeros((12, 12), dtype=np.float32)
        probability[3:8, 4:9] = 0.35
        p50 = np.zeros((12, 12), dtype=np.float32)
        p50[4:7, 5:8] = 1.0
        p90 = np.zeros((12, 12), dtype=np.float32)
        p90[5:6, 6:7] = 1.0
        for hour in (24, 48, 72):
            _write_raster(ensemble_dir / f"probability_{hour}h.tif", probability * (hour / 72.0), self.bounds)
            _write_raster(ensemble_dir / f"mask_p50_{hour}h.tif", p50, self.bounds)
            _write_raster(ensemble_dir / f"mask_p90_{hour}h.tif", p90, self.bounds)

        similarity_rows = [
            {
                "case_id": self.case_id,
                "comparison_track_id": track_id,
                "mean_fss_1km": 0.100,
                "mean_fss_3km": 0.200,
                "mean_fss_5km": 0.300,
                "mean_fss_10km": 0.400,
            }
            for track_id in ("deterministic", "ensemble_p50", "ensemble_p90")
        ]
        similarity_dir = root / "output" / "prototype_2016_pygnome_similarity"
        similarity_dir.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(similarity_rows).to_csv(similarity_dir / "prototype_pygnome_similarity_by_case.csv", index=False)

        if not include_benchmark:
            return

        benchmark_dir = root / "output" / self.case_id / "benchmark"
        _write_json(
            benchmark_dir / "grid" / "grid.json",
            {
                "display_bounds_wgs84": list(self.bounds),
                "extent": list(self.bounds),
            },
        )
        control_dir = benchmark_dir / "control"
        p50_dir = benchmark_dir / "ensemble_p50"
        p90_dir = benchmark_dir / "ensemble_p90"
        pygnome_dir = benchmark_dir / "pygnome"
        qa_dir = benchmark_dir / "qa"
        summary_rows = []
        fss_rows = []
        kl_rows = []
        pairing_rows = []

        for comparison_track_id, track_dir in (
            ("deterministic", control_dir),
            ("ensemble_p50", p50_dir),
            ("ensemble_p90", p90_dir),
        ):
            label = {
                "deterministic": "OpenDrift deterministic",
                "ensemble_p50": "OpenDrift p50 threshold",
                "ensemble_p90": "OpenDrift p90 threshold",
            }[comparison_track_id]
            for hour, day in ((24, "2016-09-02"), (48, "2016-09-03"), (72, "2016-09-04")):
                timestamp = f"{day}T00:00:00Z"
                stamp = f"{day}T00-00-00Z"
                od_footprint = track_dir / f"{comparison_track_id}_footprint_mask_{stamp}.tif"
                od_density = track_dir / f"{comparison_track_id}_density_norm_{stamp}.tif"
                py_footprint = pygnome_dir / f"pygnome_footprint_mask_{stamp}.tif"
                py_density = pygnome_dir / f"pygnome_density_norm_{stamp}.tif"
                qa_overlay = qa_dir / f"{comparison_track_id}_overlay_{stamp}.png"

                _write_raster(od_footprint, p50 if comparison_track_id != "ensemble_p90" else p90, self.bounds)
                _write_raster(od_density, probability * (hour / 72.0), self.bounds)
                _write_raster(py_footprint, p50, self.bounds)
                _write_raster(py_density, probability[::-1] * (hour / 72.0), self.bounds)
                _write_png(qa_overlay)

                summary_rows.append(
                    {
                        "comparison_track_id": comparison_track_id,
                        "comparison_track_label": label,
                        "hour": hour,
                    }
                )
                kl_rows.append(
                    {
                        "comparison_track_id": comparison_track_id,
                        "comparison_track_label": label,
                        "hour": hour,
                        "kl_divergence": 0.100 + (hour / 1000.0),
                    }
                )
                for window_km, base_fss in ((1, 0.10), (3, 0.20), (5, 0.30), (10, 0.40)):
                    fss_rows.append(
                        {
                            "comparison_track_id": comparison_track_id,
                            "comparison_track_label": label,
                            "hour": hour,
                            "window_km": window_km,
                            "fss": base_fss + (0.01 if comparison_track_id == "ensemble_p90" else 0.0),
                        }
                    )
                pairing_rows.append(
                    {
                        "comparison_track_id": comparison_track_id,
                        "comparison_track_label": label,
                        "timestamp_utc": timestamp,
                        "hour": hour,
                        "opendrift_footprint_path": str(od_footprint.relative_to(root)).replace("\\", "/"),
                        "pygnome_footprint_path": str(py_footprint.relative_to(root)).replace("\\", "/"),
                        "opendrift_density_path": str(od_density.relative_to(root)).replace("\\", "/"),
                        "pygnome_density_path": str(py_density.relative_to(root)).replace("\\", "/"),
                        "qa_overlay_path": str(qa_overlay.relative_to(root)).replace("\\", "/"),
                    }
                )

        pd.DataFrame(summary_rows).to_csv(benchmark_dir / "phase3a_summary.csv", index=False)
        pd.DataFrame(fss_rows).to_csv(benchmark_dir / "phase3a_fss_by_time_window.csv", index=False)
        pd.DataFrame(kl_rows).to_csv(benchmark_dir / "phase3a_kl_by_time.csv", index=False)
        pd.DataFrame(pairing_rows).to_csv(benchmark_dir / "phase3a_pairing_manifest.csv", index=False)
        _write_json(
            pygnome_dir / "pygnome_benchmark_metadata.json",
            {
                "weathering_enabled": False,
                "benchmark_particles": 1000,
            },
        )

    def _build_service(self, root: Path) -> PrototypeLegacyFinalFiguresService:
        service = PrototypeLegacyFinalFiguresService(repo_root=root)
        service.prototype_helper._draw_context_layers = lambda ax: None
        service.prototype_helper._draw_crop_labels = lambda ax, crop_bounds: None
        service.prototype_helper._draw_locator = lambda ax, crop_bounds, full_bounds=None: ax.axis("off")
        return service

    def test_run_exports_expected_final_legacy_figures_and_manifest(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self._build_fixture(root, include_benchmark=True)
            service = self._build_service(root)

            with mock.patch("src.services.prototype_legacy_final_figures.plot_legacy_drifter_track_map", side_effect=_placeholder_map_writer), mock.patch(
                "src.services.prototype_legacy_final_figures.plot_legacy_drifter_track_ensemble_overlay",
                side_effect=_placeholder_map_writer,
            ):
                results = service.run()

            case_dir = root / "output" / "2016 Legacy Runs FINAL Figures" / self.case_id
            expected_files = [
                "drifter_track_72h.png",
                "ensemble_probability_24h.png",
                "ensemble_probability_48h.png",
                "ensemble_probability_72h.png",
                "ensemble_consolidated_72h.png",
                "drifter_vs_ensemble_72h.png",
                "pygnome_24h.png",
                "pygnome_48h.png",
                "pygnome_72h.png",
                "pygnome_consolidated_72h.png",
                "pygnome_vs_ensemble_24h.png",
                "pygnome_vs_ensemble_48h.png",
                "pygnome_vs_ensemble_72h.png",
                "pygnome_vs_ensemble_consolidated_72h.png",
            ]
            for filename in expected_files:
                self.assertTrue((case_dir / filename).exists(), filename)

            manifest = json.loads((root / results["manifest_json"]).read_text(encoding="utf-8"))
            self.assertEqual(manifest["figure_count"], 14)
            self.assertEqual(manifest["missing_figure_count"], 0)
            self.assertEqual(manifest["rendering_profile"], "prototype_2016_case_local_projected_v1")
            self.assertEqual(manifest["map_projection"], "local_azimuthal_equidistant")
            self.assertIn(self.case_id, manifest["case_rendering"])
            self.assertEqual(results["figure_count"], 14)
            self.assertEqual(results["missing_figure_count"], 0)
            self.assertEqual(manifest["configured_case_ids"], [self.case_id])
            self.assertEqual(service.style["typography"]["font_family"], "Arial")
            self.assertTrue(results["font_family"])
            figure_rows = manifest["figures"]
            ensemble_row = next(row for row in figure_rows if row["figure_id"] == "ensemble_probability_24h")
            self.assertEqual(ensemble_row["extent_mode"], "dynamic_forecast_extent")
            self.assertTrue(str(ensemble_row["plot_bounds_wgs84"]))
            self.assertEqual(ensemble_row["geometry_render_mode"], "exact_stored_raster")
            self.assertEqual(ensemble_row["density_render_mode"], "direct_raster")
            self.assertTrue(str(ensemble_row["stored_geometry_status"]))
            pygnome_row = next(row for row in figure_rows if row["figure_id"] == "pygnome_24h")
            self.assertEqual(pygnome_row["extent_mode"], "dynamic_forecast_extent")
            self.assertTrue(str(pygnome_row["plot_bounds_wgs84"]))
            self.assertEqual(pygnome_row["geometry_render_mode"], "exact_stored_raster")
            self.assertEqual(pygnome_row["density_render_mode"], "direct_raster")
            drifter_row = next(row for row in figure_rows if row["figure_id"] == "drifter_track_72h")
            self.assertEqual(drifter_row["extent_mode"], "dynamic_forecast_extent")
            self.assertEqual(drifter_row["geometry_render_mode"], "observed_track_line")
            self.assertEqual(drifter_row["density_render_mode"], "not_applicable")

            missing_df = pd.read_csv(root / results["missing_figures_csv"])
            self.assertTrue(missing_df.empty)

    def test_run_records_missing_pygnome_figures_but_keeps_phase2_exports(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self._build_fixture(root, include_benchmark=False)
            service = self._build_service(root)

            with mock.patch("src.services.prototype_legacy_final_figures.plot_legacy_drifter_track_map", side_effect=_placeholder_map_writer), mock.patch(
                "src.services.prototype_legacy_final_figures.plot_legacy_drifter_track_ensemble_overlay",
                side_effect=_placeholder_map_writer,
            ):
                results = service.run()

            case_dir = root / "output" / "2016 Legacy Runs FINAL Figures" / self.case_id
            self.assertTrue((case_dir / "drifter_track_72h.png").exists())
            self.assertTrue((case_dir / "ensemble_probability_24h.png").exists())
            self.assertFalse((case_dir / "pygnome_24h.png").exists())
            self.assertGreater(results["missing_figure_count"], 0)

            missing_df = pd.read_csv(root / results["missing_figures_csv"])
            self.assertIn("pygnome_24h", set(missing_df["figure_id"]))
            self.assertIn("pygnome_vs_ensemble_consolidated_72h", set(missing_df["figure_id"]))
            self.assertIn("benchmark_or_pygnome_output_missing", set(missing_df["missing_cause"]))


if __name__ == "__main__":
    unittest.main()
