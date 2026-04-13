import hashlib
import json
import tempfile
import unittest
from pathlib import Path

import numpy as np
import pandas as pd
import rasterio
from matplotlib import pyplot as plt
from rasterio.transform import from_bounds

from src.services.prototype_2016_display_review import (
    DISPLAY_MODE,
    OUTPUT_DIR,
    TECHNICAL_MODE,
    Prototype2016DisplayReviewService,
    run_prototype_2016_display_review,
)


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
legend_labels:
  observed_mask: "Observed spill extent"
  deterministic_opendrift: "OpenDrift deterministic forecast"
  ensemble_consolidated: "OpenDrift consolidated ensemble trajectory"
  ensemble_p50: "OpenDrift ensemble p50 footprint"
  ensemble_p90: "OpenDrift ensemble p90 footprint"
  pygnome: "PyGNOME comparator"
  source_point: "Source point"
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


def _write_png(path: Path, width: int = 320, height: int = 180) -> None:
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


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


class Prototype2016DisplayReviewTests(unittest.TestCase):
    bounds = (116.8, 117.7, 10.0, 10.8)
    case_id = "CASE_2016-09-01"

    def _build_fixture(self, root: Path) -> dict[str, Path]:
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
        probability[3:8, 4:9] = np.array(
            [
                [0.10, 0.18, 0.22, 0.16, 0.08],
                [0.12, 0.22, 0.35, 0.26, 0.10],
                [0.10, 0.24, 0.40, 0.30, 0.12],
                [0.08, 0.18, 0.26, 0.20, 0.10],
                [0.05, 0.09, 0.12, 0.10, 0.04],
            ],
            dtype=np.float32,
        )
        p50 = np.zeros((12, 12), dtype=np.float32)
        p50[4:7, 5:8] = 1.0
        p90 = np.zeros((12, 12), dtype=np.float32)
        p90[5:6, 6:7] = 1.0
        for hour, scale in ((24, 0.8), (48, 1.0), (72, 1.1)):
            _write_raster(ensemble_dir / f"probability_{hour}h.tif", probability * scale, self.bounds)
            _write_raster(ensemble_dir / f"mask_p50_{hour}h.tif", p50, self.bounds)
            _write_raster(ensemble_dir / f"mask_p90_{hour}h.tif", p90, self.bounds)

        similarity_dir = root / "output" / "prototype_2016_pygnome_similarity"
        similarity_dir.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(
            [
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
        ).to_csv(similarity_dir / "prototype_pygnome_similarity_by_case.csv", index=False)
        pd.DataFrame(columns=["case_id", "error_message"]).to_csv(
            similarity_dir / "prototype_pygnome_skipped_cases.csv", index=False
        )

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
        pygnome_footprint_24h = None
        probability_24h = ensemble_dir / "probability_24h.tif"
        for comparison_track_id, track_dir in (
            ("deterministic", control_dir),
            ("ensemble_p50", p50_dir),
            ("ensemble_p90", p90_dir),
        ):
            label = {
                "deterministic": "OpenDrift deterministic",
                "ensemble_p50": "OpenDrift p50 occupancy footprint",
                "ensemble_p90": "OpenDrift p90 occupancy footprint",
            }[comparison_track_id]
            for hour, day in ((24, "2016-09-02"), (48, "2016-09-03"), (72, "2016-09-04")):
                stamp = f"{day}T00-00-00Z"
                timestamp = f"{day}T00:00:00Z"
                od_footprint = track_dir / f"{comparison_track_id}_footprint_mask_{stamp}.tif"
                od_density = track_dir / f"{comparison_track_id}_density_norm_{stamp}.tif"
                py_footprint = pygnome_dir / f"pygnome_footprint_mask_{stamp}.tif"
                py_density = pygnome_dir / f"pygnome_density_norm_{stamp}.tif"
                qa_overlay = qa_dir / f"{comparison_track_id}_overlay_{stamp}.png"
                od_array = p50 if comparison_track_id != "ensemble_p90" else p90
                py_array = np.zeros((12, 12), dtype=np.float32)
                py_array[4:7, 4:7] = 1.0
                py_density_array = probability * (0.7 if hour == 24 else 0.9 if hour == 48 else 1.0)
                _write_raster(od_footprint, od_array, self.bounds)
                _write_raster(od_density, probability * (hour / 72.0), self.bounds)
                _write_raster(py_footprint, py_array, self.bounds)
                _write_raster(py_density, py_density_array, self.bounds)
                _write_png(qa_overlay)
                if comparison_track_id == "deterministic" and hour == 24:
                    pygnome_footprint_24h = py_footprint

                summary_rows.append({"comparison_track_id": comparison_track_id, "comparison_track_label": label, "hour": hour})
                kl_rows.append(
                    {
                        "comparison_track_id": comparison_track_id,
                        "comparison_track_label": label,
                        "hour": hour,
                        "kl_divergence": 0.100 + (hour / 1000.0),
                        "epsilon": 1.0e-10,
                        "ocean_cell_count": 42,
                    }
                )
                for window_km, value in ((1, 0.10), (3, 0.20), (5, 0.30), (10, 0.40)):
                    fss_rows.append(
                        {
                            "comparison_track_id": comparison_track_id,
                            "comparison_track_label": label,
                            "timestamp_utc": timestamp,
                            "hour": hour,
                            "window_km": window_km,
                            "fss": value,
                        }
                    )
                pairing_rows.append(
                    {
                        "comparison_track_id": comparison_track_id,
                        "comparison_track_label": label,
                        "timestamp_utc": timestamp,
                        "hour": hour,
                        "opendrift_footprint_path": str(od_footprint.relative_to(root)),
                        "opendrift_density_path": str(od_density.relative_to(root)),
                        "control_footprint_path": str((control_dir / f"deterministic_footprint_mask_{stamp}.tif").relative_to(root)),
                        "control_density_path": str((control_dir / f"deterministic_density_norm_{stamp}.tif").relative_to(root)),
                        "pygnome_footprint_path": str(py_footprint.relative_to(root)),
                        "pygnome_density_path": str(py_density.relative_to(root)),
                        "qa_overlay_path": str(qa_overlay.relative_to(root)),
                        "pygnome_nc_path": "",
                    }
                )

        pd.DataFrame(summary_rows).to_csv(benchmark_dir / "phase3a_summary.csv", index=False)
        pd.DataFrame(fss_rows).to_csv(benchmark_dir / "phase3a_fss_by_time_window.csv", index=False)
        pd.DataFrame(kl_rows).to_csv(benchmark_dir / "phase3a_kl_by_time.csv", index=False)
        pd.DataFrame(pairing_rows).to_csv(benchmark_dir / "phase3a_pairing_manifest.csv", index=False)
        _write_json(
            benchmark_dir / "pygnome" / "pygnome_benchmark_metadata.json",
            {
                "transport_forcing_mode": "matched_grid_wind_plus_grid_current",
                "current_mover_used": True,
                "degraded_forcing": False,
                "degraded_reason": "",
            },
        )
        return {
            "probability_24h": probability_24h,
            "pygnome_footprint_24h": pygnome_footprint_24h,
        }

    def _prepare_service(self, root: Path) -> Prototype2016DisplayReviewService:
        service = Prototype2016DisplayReviewService(repo_root=root)
        service.prototype_helper._load_prototype_map_context = lambda: {"full_bounds_wgs84": self.bounds}
        service.prototype_helper._draw_context_layers = lambda ax: None
        service.prototype_helper._draw_locator = lambda ax, crop_bounds, display_bounds=None: None
        service.prototype_helper._draw_crop_labels = lambda ax, crop_bounds: None
        service.case_ids = [self.case_id]
        return service

    def test_display_review_writes_review_only_outputs_and_leaves_sources_unchanged(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            paths = self._build_fixture(root)
            probability_hash = _sha256(paths["probability_24h"])
            pygnome_hash = _sha256(paths["pygnome_footprint_24h"])

            service = self._prepare_service(root)
            results = service.run()

            self.assertEqual(results["phase"], "prototype_2016_display_review")
            review_dir = root / OUTPUT_DIR
            self.assertTrue((review_dir / self.case_id / TECHNICAL_MODE / "ensemble_probability_24h.png").exists())
            self.assertTrue((review_dir / self.case_id / DISPLAY_MODE / "pygnome_vs_ensemble_72h.png").exists())
            self.assertTrue((review_dir / self.case_id / DISPLAY_MODE / "pygnome_vs_ensemble_board_24_48_72h.png").exists())
            self.assertEqual(_sha256(paths["probability_24h"]), probability_hash)
            self.assertEqual(_sha256(paths["pygnome_footprint_24h"]), pygnome_hash)

            manifest = json.loads((review_dir / "display_review_manifest.json").read_text(encoding="utf-8"))
            self.assertIn("No OpenDrift ensemble rerun was triggered by this phase.", manifest["notes"])
            self.assertIn("No PyGNOME rerun was triggered by this phase.", manifest["notes"])
            rendering_modes = {row["rendering_mode"] for row in manifest["figures"]}
            self.assertIn(TECHNICAL_MODE, rendering_modes)
            self.assertIn(DISPLAY_MODE, rendering_modes)

            audit_df = pd.read_csv(review_dir / "figure_input_audit.csv")
            self.assertTrue((audit_df["placeholder_geometry_used"] == False).all())  # noqa: E712
            self.assertTrue((audit_df["science_rerun_performed"] == False).all())  # noqa: E712
            self.assertTrue((audit_df["pygnome_rerun_performed"] == False).all())  # noqa: E712
            self.assertIn("display_only_conservative_shape", {row["geometry_render_mode"] for row in manifest["figures"]})

    def test_display_review_fails_loudly_when_required_stored_geometry_is_missing(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            paths = self._build_fixture(root)
            paths["pygnome_footprint_24h"].unlink()

            service = self._prepare_service(root)
            with self.assertRaises(FileNotFoundError):
                service.run()


if __name__ == "__main__":
    unittest.main()
