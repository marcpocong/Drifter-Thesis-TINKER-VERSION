import csv
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

    def _build_fixture(self, root: Path, *, include_benchmark: bool, include_phase4_comparator: bool = False) -> None:
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
        pd.DataFrame(similarity_rows).to_csv(similarity_dir / "prototype_pygnome_fss_by_case_window.csv", index=False)
        pd.DataFrame(similarity_rows).to_csv(similarity_dir / "prototype_pygnome_kl_by_case_hour.csv", index=False)
        pd.DataFrame([{"case_id": self.case_id, "figure_id": "sample"}]).to_csv(
            similarity_dir / "prototype_pygnome_case_registry.csv",
            index=False,
        )
        pd.DataFrame([{"figure_id": "sample", "relative_path": "output/prototype_2016_pygnome_similarity/figures/sample.png"}]).to_csv(
            similarity_dir / "prototype_pygnome_figure_registry.csv",
            index=False,
        )
        _write_text(similarity_dir / "prototype_pygnome_similarity_summary.md", "prototype summary")
        _write_text(similarity_dir / "prototype_pygnome_figure_captions.md", "captions")
        _write_json(similarity_dir / "prototype_pygnome_similarity_manifest.json", {"workflow_mode": "prototype_2016"})
        pd.DataFrame(columns=["case_id", "error_message"]).to_csv(
            similarity_dir / "prototype_pygnome_skipped_cases.csv",
            index=False,
        )
        similarity_figures_dir = similarity_dir / "figures"
        similarity_figures_dir.mkdir(parents=True, exist_ok=True)
        publication_dir = root / "output" / "figure_package_publication"
        publication_dir.mkdir(parents=True, exist_ok=True)
        for source_name in (
            "case_2016_09_01__prototype_2016__24h__opendrift__single.png",
            "case_2016_09_01__prototype_2016__24h__pygnome__single.png",
            "case_2016_09_01__prototype_2016__24_48_72h__opendrift_vs_pygnome__board.png",
        ):
            _write_png(similarity_figures_dir / source_name)
        for publication_name in (
            "case_2016_09_01__prototype_pygnome_similarity_summary__opendrift__single_forecast__2016_09_02__single__paper__case_2016_09_01_prototype_2016_24h_opendrift_single.png",
            "case_2016_09_01__prototype_pygnome_similarity_summary__pygnome__single_forecast__2016_09_02__single__paper__case_2016_09_01_prototype_2016_24h_pygnome_single.png",
            "case_2016_09_01__prototype_pygnome_similarity_summary__opendrift_vs_pygnome__comparison_board__2016_09_02_to_2016_09_04__board__slide__case_2016_09_01_prototype_2016_24_48_72h_opendrift_vs_pygnome_board.png",
        ):
            _write_png(publication_dir / publication_name)

        weathering_dir = root / "output" / self.case_id / "weathering"
        weathering_dir.mkdir(parents=True, exist_ok=True)
        pd.DataFrame({"hour": [0, 24], "surface_pct": [100.0, 75.0]}).to_csv(
            weathering_dir / "budget_light.csv",
            index=False,
        )
        pd.DataFrame({"hour": [0, 24], "surface_pct": [100.0, 60.0]}).to_csv(
            weathering_dir / "budget_heavy.csv",
            index=False,
        )
        shoreline_df = pd.DataFrame(
            {
                "segment_id": ["SEG_A", "SEG_B"],
                "total_beached_kg": [12.5, 3.5],
                "n_particles": [5, 2],
                "first_arrival_h": [18.0, 30.0],
            }
        )
        shoreline_df.to_csv(weathering_dir / "shoreline_light.csv", index=False)
        shoreline_df.to_csv(weathering_dir / "shoreline_heavy.csv", index=False)
        for filename in ("mass_budget_comparison.png", "mass_budget_light.png", "mass_budget_heavy.png"):
            _write_png(weathering_dir / filename)

        if include_phase4_comparator:
            comparator_dir = root / "output" / self.case_id / "phase4_pygnome_comparator"
            comparator_dir.mkdir(parents=True, exist_ok=True)
            for filename in ("budget_comparison_board.png", "budget_time_series_light.png", "budget_time_series_heavy.png"):
                _write_png(comparator_dir / filename)
            pd.DataFrame(
                [
                    {
                        "case_id": self.case_id,
                        "scenario_key": "light",
                        "hours_elapsed": 24,
                        "comparison_scope": "budget_snapshot",
                        "compartment": "surface",
                        "opendrift_pct": 90.0,
                        "pygnome_pct": 88.0,
                        "abs_percentage_point_diff": 2.0,
                        "comparable": True,
                        "comparable_reason": "Matched budget fraction",
                    }
                ]
            ).to_csv(comparator_dir / "phase4_budget_comparison.csv", index=False)
            pd.DataFrame(
                [
                    {
                        "case_id": self.case_id,
                        "scenario_key": "light",
                        "compartment": "surface",
                        "comparable": True,
                        "hours_compared": 73,
                        "start_hour": 0,
                        "end_hour": 72,
                        "mae_pct_points": 1.2,
                        "rmse_pct_points": 1.7,
                        "end_horizon_opendrift_pct": 60.0,
                        "end_horizon_pygnome_pct": 58.0,
                        "end_horizon_abs_diff_pct_points": 2.0,
                        "notes": "Comparator-only descriptive metric.",
                    }
                ]
            ).to_csv(comparator_dir / "phase4_budget_time_series_metrics.csv", index=False)
            pd.DataFrame({"hours_elapsed": [0, 24], "surface_pct": [100.0, 88.0]}).to_csv(
                comparator_dir / "pygnome_budget_light.csv",
                index=False,
            )
            pd.DataFrame({"hours_elapsed": [0, 24], "surface_pct": [100.0, 70.0]}).to_csv(
                comparator_dir / "pygnome_budget_heavy.csv",
                index=False,
            )
            _write_json(
                comparator_dir / "pygnome_phase4_run_manifest.json",
                {
                    "phase": "prototype_legacy_phase4_pygnome_comparator",
                    "case_id": self.case_id,
                    "budget_only_feasible": True,
                    "shoreline_comparison_feasible": False,
                },
            )

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
                "ensemble_p50": "OpenDrift p50 occupancy footprint",
                "ensemble_p90": "OpenDrift p90 occupancy footprint",
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
                "degraded_forcing": False,
                "degraded_reason": "",
                "transport_forcing_mode": "matched_grid_wind_plus_grid_current",
                "current_mover_used": True,
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
            self._build_fixture(root, include_benchmark=True, include_phase4_comparator=True)
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
            curated_manifest = json.loads((root / results["legacy_final_output_manifest_json"]).read_text(encoding="utf-8"))
            self.assertEqual(manifest["figure_count"], 14)
            self.assertEqual(manifest["missing_figure_count"], 0)
            self.assertEqual(manifest["rendering_profile"], "prototype_2016_case_local_projected_v1")
            self.assertEqual(manifest["map_projection"], "local_azimuthal_equidistant")
            self.assertIn(self.case_id, manifest["case_rendering"])
            self.assertEqual(results["figure_count"], 14)
            self.assertEqual(results["missing_figure_count"], 0)
            self.assertGreater(results["package_registry_count"], 0)
            self.assertEqual(manifest["configured_case_ids"], [self.case_id])
            self.assertEqual(service.style["typography"]["font_family"], "Arial")
            self.assertTrue(results["font_family"])
            self.assertTrue((root / "output" / "2016 Legacy Runs FINAL Figures" / "README.md").exists())
            self.assertTrue(
                (
                    root
                    / "output"
                    / "2016 Legacy Runs FINAL Figures"
                    / "publication"
                    / "phase3a"
                    / self.case_id
                    / "case_2016_09_01__prototype_pygnome_similarity_summary__opendrift__single_forecast__2016_09_02__single__paper__case_2016_09_01_prototype_2016_24h_opendrift_single.png"
                ).exists()
            )
            self.assertTrue(
                (
                    root
                    / "output"
                    / "2016 Legacy Runs FINAL Figures"
                    / "publication"
                    / "phase4"
                    / self.case_id
                    / "shoreline_summary_light.png"
                ).exists()
            )
            self.assertTrue(
                (
                    root
                    / "output"
                    / "2016 Legacy Runs FINAL Figures"
                    / "scientific_source_pngs"
                    / "phase4"
                    / self.case_id
                    / "mass_budget_comparison.png"
                ).exists()
            )
            self.assertTrue(
                (
                    root
                    / "output"
                    / "2016 Legacy Runs FINAL Figures"
                    / "summary"
                    / "phase4"
                    / "prototype_2016_phase4_registry.csv"
                ).exists()
            )
            self.assertTrue(
                (
                    root
                    / "output"
                    / "2016 Legacy Runs FINAL Figures"
                    / "publication"
                    / "phase4_comparator"
                    / self.case_id
                    / "budget_comparison_board.png"
                ).exists()
            )
            self.assertTrue(
                (
                    root
                    / "output"
                    / "2016 Legacy Runs FINAL Figures"
                    / "summary"
                    / "phase4_comparator"
                    / "prototype_2016_phase4_pygnome_comparator_registry.csv"
                ).exists()
            )
            self.assertTrue(
                (
                    root
                    / "output"
                    / "2016 Legacy Runs FINAL Figures"
                    / "phase5"
                    / "prototype_2016_packaging_summary.md"
                ).exists()
            )
            self.assertIn(
                "prototype_2016 p50/p90 products are exact valid-time member-occupancy footprints.",
                manifest["notes"],
            )
            self.assertEqual(
                curated_manifest["authoritative_curated_root"],
                "output/2016 Legacy Runs FINAL Figures",
            )
            self.assertFalse(curated_manifest["scientific_rerun_performed"])
            self.assertIn("publication_phase4_comparator_dir", curated_manifest)
            self.assertIn(
                "A limited deterministic Phase 4 PyGNOME budget comparator pilot may be packaged when stored case-local comparator outputs exist; shoreline comparison remains unavailable in that pilot.",
                curated_manifest["notes"],
            )
            registry_path = root / results["legacy_final_output_registry_csv"]
            with open(registry_path, "r", encoding="utf-8", newline="") as handle:
                reader = csv.DictReader(handle)
                registry_rows = list(reader)
            self.assertGreater(len(registry_rows), 0)
            self.assertTrue(all("\n" not in str(row["notes"]) for row in registry_rows))
            self.assertTrue(all((root / Path(row["final_relative_path"])).exists() for row in registry_rows))
            self.assertTrue(
                any(row["phase_group"] == "phase4" and row["copied_vs_regenerated"] == "regenerated_from_stored_csv" for row in registry_rows)
            )
            self.assertTrue(
                any(row["phase_group"] == "phase4_comparator" and row["comparator_only"] == "True" for row in registry_rows)
            )
            self.assertIn(
                "PyGNOME remains comparator-only; matched grid wind/current forcing is used when available and degraded mode is surfaced explicitly otherwise.",
                manifest["notes"],
            )
            figure_rows = manifest["figures"]
            ensemble_row = next(row for row in figure_rows if row["figure_id"] == "ensemble_probability_24h")
            self.assertEqual(ensemble_row["extent_mode"], "dynamic_forecast_extent")
            self.assertTrue(str(ensemble_row["plot_bounds_wgs84"]))
            self.assertEqual(ensemble_row["geometry_render_mode"], "exact_stored_raster")
            self.assertEqual(ensemble_row["density_render_mode"], "direct_raster")
            self.assertTrue(str(ensemble_row["stored_geometry_status"]))
            self.assertIn("member-occupancy", ensemble_row["notes"])
            pygnome_row = next(row for row in figure_rows if row["figure_id"] == "pygnome_24h")
            self.assertEqual(pygnome_row["extent_mode"], "dynamic_forecast_extent")
            self.assertTrue(str(pygnome_row["plot_bounds_wgs84"]))
            self.assertEqual(pygnome_row["geometry_render_mode"], "exact_stored_raster")
            self.assertEqual(pygnome_row["density_render_mode"], "direct_raster")
            self.assertIn("matched prepared grid wind plus grid current forcing", pygnome_row["notes"])
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
            self.assertTrue((root / "output" / "2016 Legacy Runs FINAL Figures" / "README.md").exists())
            self.assertTrue(
                (
                    root
                    / "output"
                    / "2016 Legacy Runs FINAL Figures"
                    / "publication"
                    / "phase4"
                    / self.case_id
                    / "shoreline_summary_heavy.png"
                ).exists()
            )
            self.assertGreater(results["missing_figure_count"], 0)

            missing_df = pd.read_csv(root / results["missing_figures_csv"])
            self.assertIn("pygnome_24h", set(missing_df["figure_id"]))
            self.assertIn("pygnome_vs_ensemble_consolidated_72h", set(missing_df["figure_id"]))
            self.assertIn("benchmark_or_pygnome_output_missing", set(missing_df["missing_cause"]))


if __name__ == "__main__":
    unittest.main()
