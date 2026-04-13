import tempfile
import unittest
from pathlib import Path

from ui.bootstrap import discover_branding_assets
from ui.data_access import build_dashboard_state, curated_package_roots
from ui.pages import visible_page_definitions


REPO_ROOT = Path(__file__).resolve().parents[1]


class UiBrandingSupportTests(unittest.TestCase):
    def test_missing_logo_assets_fall_back_cleanly(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            assets = discover_branding_assets(asset_dir=tmpdir)
        self.assertFalse(assets["has_logo"])
        self.assertFalse(assets["has_icon"])
        self.assertIsNone(assets["logo_path"])
        self.assertIsNone(assets["icon_path"])
        self.assertIsNone(assets["page_icon_path"])

    def test_present_logo_assets_are_detected(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            assets_dir = Path(tmpdir)
            (assets_dir / "logo.svg").write_text("<svg xmlns='http://www.w3.org/2000/svg'></svg>", encoding="utf-8")
            (assets_dir / "logo_icon.png").write_bytes(b"\x89PNG\r\n\x1a\n")
            assets = discover_branding_assets(asset_dir=assets_dir)
        self.assertTrue(assets["has_logo"])
        self.assertTrue(assets["has_icon"])
        self.assertEqual(Path(assets["logo_path"]).name, "logo.svg")
        self.assertEqual(Path(assets["icon_path"]).name, "logo_icon.png")
        self.assertEqual(Path(assets["page_icon_path"]).name, "logo_icon.png")


class UiReadonlySemanticsTests(unittest.TestCase):
    def test_curated_package_roots_are_prioritized(self):
        packages = curated_package_roots(REPO_ROOT)
        relative_paths = [str(package["relative_path"]).replace("\\", "/") for package in packages]
        self.assertGreaterEqual(len(relative_paths), 6)
        self.assertEqual(relative_paths[0], "output/Phase 3B March13-14 Final Output")
        self.assertEqual(relative_paths[1], "output/Phase 3B March13-14 Final Output/publication/comparator_pygnome")
        self.assertEqual(relative_paths[2], "output/Phase 3C DWH Final Output")
        self.assertEqual(relative_paths[3], "output/2016 Legacy Runs FINAL Figures")
        self.assertNotIn("output/CASE_MINDORO_RETRO_2023", relative_paths[:4])
        self.assertNotIn("output/CASE_DWH_RETRO_2010_72H", relative_paths[:4])

    def test_panel_mode_page_map_hides_internal_status_and_advanced_only_pages(self):
        state = build_dashboard_state(REPO_ROOT)
        panel_labels = [page.label for page in visible_page_definitions(state, advanced=False)]
        self.assertIn("Home / Overview", panel_labels)
        self.assertIn("Phase 1 Recipe Selection", panel_labels)
        self.assertIn("Mindoro B1 Primary Validation", panel_labels)
        self.assertIn("Legacy 2016 Support Package", panel_labels)
        self.assertNotIn("Phase 4 Cross-Model Status", panel_labels)
        self.assertNotIn("Trajectory Explorer", panel_labels)

    def test_panel_mode_hides_phase1_page_when_phase1_artifacts_are_missing(self):
        state = build_dashboard_state(REPO_ROOT)
        state["phase1_focused_manifest"] = {}
        state["phase1_focused_recipe_ranking"] = state["phase1_focused_recipe_ranking"].iloc[0:0].copy()
        panel_labels = [page.label for page in visible_page_definitions(state, advanced=False)]
        self.assertNotIn("Phase 1 Recipe Selection", panel_labels)

    def test_panel_facing_ui_source_avoids_internal_status_strings(self):
        forbidden_tokens = (
            "not_comparable_honestly",
            "Inherited-provisional tracks",
            "Reportable tracks",
            "Current honesty status",
        )
        files_to_check = (
            REPO_ROOT / "ui" / "pages" / "home.py",
            REPO_ROOT / "ui" / "pages" / "mindoro_validation.py",
            REPO_ROOT / "ui" / "pages" / "phase4_oiltype_and_shoreline.py",
            REPO_ROOT / "ui" / "pages" / "legacy_2016_support.py",
        )
        for path in files_to_check:
            text = path.read_text(encoding="utf-8")
            with self.subTest(path=path.name):
                for token in forbidden_tokens:
                    self.assertNotIn(token, text)

    def test_phase4_wording_matches_settled_decisions(self):
        phase4_text = (REPO_ROOT / "ui" / "pages" / "phase4_oiltype_and_shoreline.py").read_text(encoding="utf-8")
        legacy_text = (REPO_ROOT / "ui" / "pages" / "legacy_2016_support.py").read_text(encoding="utf-8")

        self.assertIn("No matched PyGNOME Phase 4 comparison is packaged yet.", phase4_text)
        self.assertIn("Current Mindoro Phase 4 results are OpenDrift/OpenOil scenario outputs only.", phase4_text)
        self.assertIn("Budget-only deterministic PyGNOME comparator pilot.", legacy_text)
        self.assertIn("Shoreline comparison is not packaged because matched PyGNOME shoreline outputs are not available.", legacy_text)

    def test_ui_source_contains_no_scientific_rerun_controls(self):
        forbidden_tokens = (
            "docker-compose exec",
            "PIPELINE_PHASE=",
            ".\\start.ps1 -Entry",
            "python -m src",
        )
        ui_dir = REPO_ROOT / "ui"
        for path in ui_dir.rglob("*.py"):
            text = path.read_text(encoding="utf-8")
            with self.subTest(path=path.name):
                for token in forbidden_tokens:
                    self.assertNotIn(token, text)


if __name__ == "__main__":
    unittest.main()
