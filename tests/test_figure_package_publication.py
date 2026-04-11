import json
import tempfile
import unittest
from pathlib import Path

from src.services.figure_package_publication import (
    FigurePackagePublicationService,
    build_publication_figure_filename,
    load_publication_style_config,
)


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


STYLE_YAML = """
title_format: "{figure_title}"
subtitle_format: "{case_label} | {date_label} | {phase_label}"
palette:
  background_land: "#e6dfd1"
  background_sea: "#f7fbfd"
  shoreline: "#8b8178"
  observed_mask: "#2f3a46"
  deterministic_opendrift: "#165ba8"
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


class FigurePackagePublicationTests(unittest.TestCase):
    def test_build_publication_figure_filename_uses_machine_readable_tokens(self):
        filename = build_publication_figure_filename(
            case_id="CASE_MINDORO_RETRO_2023",
            phase_or_track="phase3b_strict",
            model_name="opendrift_vs_pygnome",
            run_type="comparison_board",
            date_token="2023-03-04_to_2023-03-06",
            scenario_id="all_scenarios",
            view_type="close",
            variant="paper",
            figure_slug="obs_vs_model",
        )
        self.assertEqual(
            filename,
            "case_mindoro_retro_2023__phase3b_strict__opendrift_vs_pygnome__comparison_board__2023_03_04_to_2023_03_06__all_scenarios__close__paper__obs_vs_model.png",
        )

    def test_load_publication_style_config_requires_expected_keys(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config_path = root / "config" / "publication_figure_style.yaml"
            _write_text(config_path, STYLE_YAML)
            payload = load_publication_style_config(config_path)
            self.assertIn("palette", payload)
            self.assertIn("legend_labels", payload)
            broken_path = root / "config" / "broken.yaml"
            _write_text(broken_path, "palette: {}\n")
            with self.assertRaises(ValueError):
                load_publication_style_config(broken_path)

    def test_service_writes_registry_manifest_and_records_missing_optional_inputs(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            _write_text(root / "config" / "publication_figure_style.yaml", STYLE_YAML)
            _write_text(root / "config" / "publication_map_labels_mindoro.csv", "label_text,lon,lat,enabled_yes_no\nMindoro,121.0,13.0,yes\n")
            _write_text(root / "config" / "publication_map_labels_dwh.csv", "label_text,lon,lat,enabled_yes_no\nDWH,-88.0,28.7,yes\n")
            _write_text(
                root / "output" / "final_reproducibility_package" / "final_phase_status_registry.csv",
                "phase_id,track_id,scientifically_reportable,scientifically_frozen\nphase2,phase2_machine_readable_forecast,True,False\nphase4,mindoro_phase4,True,False\n",
            )
            _write_json(
                root / "output" / "CASE_MINDORO_RETRO_2023" / "forecast" / "forecast_manifest.json",
                {
                    "grid": {
                        "crs": "EPSG:32651",
                        "extent": [274000.0, 1355000.0, 398000.0, 1524000.0],
                        "display_bounds_wgs84": [120.9096, 122.0622, 12.2494, 13.7837],
                    },
                    "source_geometry": {},
                },
            )
            _write_json(root / "output" / "phase4" / "CASE_MINDORO_RETRO_2023" / "phase4_run_manifest.json", {})
            _write_json(root / "output" / "CASE_DWH_RETRO_2010_72H" / "phase3c_external_case_run" / "phase3c_run_manifest.json", {})

            results = FigurePackagePublicationService(repo_root=root).run()

            self.assertTrue(Path(results["registry_csv"]).exists())
            self.assertTrue(Path(results["manifest_json"]).exists())
            self.assertTrue(Path(results["captions_md"]).exists())
            self.assertTrue(Path(results["talking_points_md"]).exists())
            self.assertGreater(results["figure_count"], 0)
            self.assertTrue(results["side_by_side_comparison_boards_produced"])
            self.assertTrue(results["single_image_paper_figures_produced"])
            self.assertTrue(results["missing_optional_artifacts"])

            manifest = json.loads(Path(results["manifest_json"]).read_text(encoding="utf-8"))
            self.assertTrue(manifest["publication_package_built_from_existing_outputs_only"])
            self.assertIn("A", manifest["figure_families_generated"])
            self.assertIn("F", manifest["figure_families_generated"])
            self.assertIn("recommended_main_defense_figures", manifest)
            self.assertTrue(manifest["phase4_deferred_comparison_note_figure_produced"])

            registry_text = Path(results["registry_csv"]).read_text(encoding="utf-8")
            self.assertIn("crossmodel_comparison_deferred", registry_text)


if __name__ == "__main__":
    unittest.main()
