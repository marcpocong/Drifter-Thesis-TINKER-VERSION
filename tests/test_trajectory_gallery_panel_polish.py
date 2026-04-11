import json
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from src.services.trajectory_gallery_panel_polish import (
    TrajectoryGalleryPanelPolishService,
    build_panel_figure_filename,
    load_panel_style_config,
)


class TrajectoryGalleryPanelPolishTests(unittest.TestCase):
    def test_build_panel_figure_filename_includes_variant_token(self):
        filename = build_panel_figure_filename(
            case_id="CASE_MINDORO_RETRO_2023",
            phase_or_track="phase4",
            model_name="OpenOil",
            run_type="panel board",
            date_token="2023-03-03_to_2023-03-06",
            scenario_id="lighter_oil",
            variant="slide",
            figure_slug="shoreline_board",
        )

        self.assertEqual(
            filename,
            "case_mindoro_retro_2023__phase4__openoil__panel_board__2023_03_03_to_2023_03_06__lighter_oil__slide__shoreline_board.png",
        )

    def test_service_writes_panel_registry_and_manifest_with_missing_optional_inputs(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "config").mkdir(parents=True, exist_ok=True)

            (root / "config" / "panel_figure_style.yaml").write_text(
                "\n".join(
                    [
                        "palette:",
                        "  observed_mask: '#4b5563'",
                        "  deterministic_opendrift: '#2563eb'",
                        "  ensemble_p50: '#0f766e'",
                        "  ensemble_p90: '#60a5fa'",
                        "  pygnome: '#9333ea'",
                        "  source_point: '#dc2626'",
                        "  initialization_polygon: '#f59e0b'",
                        "  validation_polygon: '#111827'",
                        "  centroid_path: '#0f172a'",
                        "  corridor_hull: '#f97316'",
                        "  ensemble_member_path: '#94a3b8'",
                        "  oil_lighter: '#ff8c00'",
                        "  oil_base: '#8c564b'",
                        "  oil_heavier: '#4b0082'",
                        "legend_labels:",
                        "  observed_mask: 'Observed mask'",
                        "layout:",
                        "  slide_size_inches: [12, 7]",
                        "  dpi: 120",
                        "typography:",
                        "  title_size: 18",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            (root / "config" / "panel_map_labels_mindoro.csv").write_text(
                "label_id,label_text,lon,lat,label_group,enabled_yes_no,notes\nmindoro,Mindoro,121.2,13.0,major_landmass,yes,test\n",
                encoding="utf-8",
            )
            (root / "config" / "panel_map_labels_dwh.csv").write_text(
                "label_id,label_text,lon,lat,label_group,enabled_yes_no,notes\ndwh,Gulf,-89.0,29.0,major_waterbody,yes,test\n",
                encoding="utf-8",
            )

            service = TrajectoryGalleryPanelPolishService(repo_root=root)
            results = service.run()

            registry_path = Path(results["registry_csv"])
            manifest_path = Path(results["manifest_json"])
            captions_path = Path(results["captions_md"])
            talking_points_path = Path(results["talking_points_md"])

            self.assertTrue(registry_path.exists())
            self.assertTrue(manifest_path.exists())
            self.assertTrue(captions_path.exists())
            self.assertTrue(talking_points_path.exists())
            self.assertEqual(results["figure_count"], 10)
            self.assertTrue(results["side_by_side_comparison_boards_produced"])
            self.assertTrue(results["plain_language_captions_produced"])

            registry_df = pd.read_csv(registry_path)
            self.assertEqual(len(registry_df), 10)
            self.assertIn("case_mindoro_retro_2023", "".join(registry_df["figure_id"].tolist()))
            self.assertTrue((registry_df["recommended_for_main_defense"] == True).any())  # noqa: E712

            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            self.assertTrue(manifest["built_from_existing_outputs_only"])
            self.assertFalse(manifest["expensive_scientific_reruns_triggered"])
            self.assertTrue(manifest["side_by_side_comparison_boards_produced"])
            self.assertGreater(len(manifest["missing_optional_artifacts"]), 0)

    def test_load_panel_style_config_requires_core_sections(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "panel.yaml"
            path.write_text("palette: {}\n", encoding="utf-8")
            with self.assertRaises(ValueError):
                load_panel_style_config(path)


if __name__ == "__main__":
    unittest.main()
