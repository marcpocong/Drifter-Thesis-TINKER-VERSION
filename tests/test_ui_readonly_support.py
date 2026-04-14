import ast
import inspect
import tempfile
import unittest
from pathlib import Path

from ui.bootstrap import discover_branding_assets
from ui.data_access import build_dashboard_state, curated_package_roots
from ui.pages import visible_page_definitions
from ui.pages.common import render_figure_cards, render_markdown_block, render_table


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

    def test_present_png_logo_assets_are_detected(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            assets_dir = Path(tmpdir)
            (assets_dir / "logo.png").write_bytes(b"\x89PNG\r\n\x1a\n")
            (assets_dir / "logo_icon.png").write_bytes(b"\x89PNG\r\n\x1a\n")
            assets = discover_branding_assets(asset_dir=assets_dir)
        self.assertTrue(assets["has_logo"])
        self.assertTrue(assets["has_icon"])
        self.assertEqual(Path(assets["logo_path"]).name, "logo.png")
        self.assertEqual(Path(assets["icon_path"]).name, "logo_icon.png")
        self.assertEqual(Path(assets["page_icon_path"]).name, "logo_icon.png")


class UiReadonlySemanticsTests(unittest.TestCase):
    def _helper_keyword_calls(self, path: Path, helper_names: set[str]) -> list[tuple[str, set[str]]]:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        matches: list[tuple[str, set[str]]] = []

        class _Visitor(ast.NodeVisitor):
            def visit_Call(self, node: ast.Call) -> None:
                helper_name = ""
                if isinstance(node.func, ast.Name):
                    helper_name = node.func.id
                elif isinstance(node.func, ast.Attribute):
                    helper_name = node.func.attr
                if helper_name in helper_names:
                    matches.append((helper_name, {keyword.arg for keyword in node.keywords if keyword.arg}))
                self.generic_visit(node)

        _Visitor().visit(tree)
        return matches

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

    def test_panel_mode_page_map_matches_presentation_order(self):
        state = build_dashboard_state(REPO_ROOT)
        panel_pages = visible_page_definitions(state, advanced=False)
        panel_labels = [page.label for page in panel_pages]
        self.assertEqual(
            panel_labels,
            [
                "Home / Overview",
                "Phase 1 Recipe Selection",
                "Mindoro B1 Primary Validation",
                "Mindoro Cross-Model Comparator",
                "DWH Phase 3C Transfer Validation",
                "Phase 4 Oil-Type and Shoreline Context",
                "Legacy 2016 Support Package",
                "Artifacts / Logs / Registries",
            ],
        )
        self.assertEqual(panel_pages[-1].navigation_section, "Reference")

    def test_phase1_page_stays_visible_when_focused_artifacts_are_missing(self):
        state = build_dashboard_state(REPO_ROOT)
        state["phase1_focused_manifest"] = {}
        state["phase1_focused_recipe_ranking"] = state["phase1_focused_recipe_ranking"].iloc[0:0].copy()
        panel_labels = [page.label for page in visible_page_definitions(state, advanced=False)]
        self.assertIn("Phase 1 Recipe Selection", panel_labels)

    def test_panel_facing_ui_source_avoids_internal_status_strings(self):
        forbidden_tokens = (
            "not_comparable_honestly",
            "Inherited-provisional tracks",
            "Reportable tracks",
            "Current honesty status",
        )
        files_to_check = (
            REPO_ROOT / "ui" / "pages" / "home.py",
            REPO_ROOT / "ui" / "pages" / "phase1_recipe_selection.py",
            REPO_ROOT / "ui" / "pages" / "mindoro_validation.py",
            REPO_ROOT / "ui" / "pages" / "cross_model_comparison.py",
            REPO_ROOT / "ui" / "pages" / "dwh_transfer_validation.py",
            REPO_ROOT / "ui" / "pages" / "phase4_oiltype_and_shoreline.py",
            REPO_ROOT / "ui" / "pages" / "legacy_2016_support.py",
        )
        for path in files_to_check:
            text = path.read_text(encoding="utf-8")
            with self.subTest(path=path.name):
                for token in forbidden_tokens:
                    self.assertNotIn(token, text)

    def test_app_source_uses_streamlit_navigation_and_not_custom_page_selector(self):
        app_text = (REPO_ROOT / "ui" / "app.py").read_text(encoding="utf-8")
        self.assertIn("st.navigation(", app_text)
        self.assertNotIn('selectbox(\n            "Page"', app_text)
        self.assertNotIn("page_selector", app_text)
        self.assertIn("st.logo(", app_text)
        self.assertIn('_export_mode_from_query_params(st.query_params)', app_text)
        self.assertIn('position="hidden"', app_text)
        self.assertIn('st.page_link(', app_text)
        self.assertIn('st_config.set_option("client.showSidebarNavigation", False)', app_text)
        self.assertIn("_apply_streamlit_branding(branding)", app_text)
        self.assertIn("_render_sidebar_branding(branding)", app_text)
        self.assertNotIn("Panel-mode detail:", app_text)
        self.assertNotIn('class="hero-card"', app_text)

    def test_export_mode_page_sources_are_wired(self):
        sequential_pages = (
            REPO_ROOT / "ui" / "pages" / "phase1_recipe_selection.py",
            REPO_ROOT / "ui" / "pages" / "mindoro_validation.py",
            REPO_ROOT / "ui" / "pages" / "dwh_transfer_validation.py",
            REPO_ROOT / "ui" / "pages" / "legacy_2016_support.py",
            REPO_ROOT / "ui" / "pages" / "phase4_oiltype_and_shoreline.py",
            REPO_ROOT / "ui" / "pages" / "artifacts_logs.py",
        )
        for path in sequential_pages:
            text = path.read_text(encoding="utf-8")
            with self.subTest(path=path.name):
                self.assertIn("render_section_stack(", text)
                self.assertNotIn("st.tabs(", text)

    def test_publication_pages_use_uniform_click_gallery_renderer(self):
        common_text = (REPO_ROOT / "ui" / "pages" / "common.py").read_text(encoding="utf-8")
        publication_pages = (
            REPO_ROOT / "ui" / "pages" / "home.py",
            REPO_ROOT / "ui" / "pages" / "mindoro_validation.py",
            REPO_ROOT / "ui" / "pages" / "cross_model_comparison.py",
            REPO_ROOT / "ui" / "pages" / "dwh_transfer_validation.py",
            REPO_ROOT / "ui" / "pages" / "legacy_2016_support.py",
            REPO_ROOT / "ui" / "pages" / "phase4_oiltype_and_shoreline.py",
        )

        self.assertIn("def render_figure_gallery(", common_text)
        self.assertIn('overlay_label: str = "Click to enlarge"', common_text)
        self.assertIn('@st.dialog("Figure preview")', common_text)
        self.assertNotIn("def _click_lightbox_markup(", common_text)
        for path in publication_pages:
            text = path.read_text(encoding="utf-8")
            with self.subTest(path=path.name):
                self.assertIn("render_figure_gallery(", text)
                self.assertNotIn('compact_selector=not ui_state["advanced"] and not export_mode', text)
                self.assertNotIn("selector_key=", text)
                self.assertNotIn("Featured figure", text)

    def test_helper_keyword_usage_matches_common_signatures(self):
        helper_signatures = {
            "render_figure_cards": set(inspect.signature(render_figure_cards).parameters),
            "render_table": set(inspect.signature(render_table).parameters),
            "render_markdown_block": set(inspect.signature(render_markdown_block).parameters),
        }
        self.assertIn("export_mode", helper_signatures["render_figure_cards"])
        self.assertIn("export_mode", helper_signatures["render_table"])
        self.assertIn("export_mode", helper_signatures["render_markdown_block"])

        for path in sorted((REPO_ROOT / "ui" / "pages").glob("*.py")):
            if path.name in {"__init__.py", "common.py"}:
                continue
            for helper_name, keyword_names in self._helper_keyword_calls(path, set(helper_signatures)):
                with self.subTest(path=path.name, helper=helper_name):
                    self.assertTrue(keyword_names.issubset(helper_signatures[helper_name]))

    def test_ui_stylesheet_uses_ideal_sans_fallback_without_broad_icon_override(self):
        style_text = (REPO_ROOT / "ui" / "assets" / "style.css").read_text(encoding="utf-8")
        self.assertIn('--ui-font-family: "Ideal Sans", Arial, Helvetica, sans-serif;', style_text)
        self.assertIn(".sidebar-brand__eyebrow", style_text)
        self.assertIn(".sidebar-note", style_text)
        self.assertIn(".home-guide-card__title", style_text)
        self.assertIn(".figure-gallery-card__title", style_text)
        self.assertIn(".figure-gallery-card__missing", style_text)
        self.assertNotIn("span,", style_text)
        self.assertNotIn("div,", style_text)

    def test_ui_font_assets_readme_documents_ideal_sans_filenames(self):
        readme_text = (REPO_ROOT / "ui" / "assets" / "fonts" / "README.md").read_text(encoding="utf-8")
        self.assertIn("IdealSans-Regular.woff2", readme_text)
        self.assertIn("IdealSans-Bold.woff2", readme_text)

    def test_phase4_wording_matches_settled_decisions(self):
        phase1_text = (REPO_ROOT / "ui" / "pages" / "phase1_recipe_selection.py").read_text(encoding="utf-8")
        phase4_text = (REPO_ROOT / "ui" / "pages" / "phase4_oiltype_and_shoreline.py").read_text(encoding="utf-8")
        legacy_text = (REPO_ROOT / "ui" / "pages" / "legacy_2016_support.py").read_text(encoding="utf-8")

        self.assertIn("Study boxes used by the thesis", phase1_text)
        self.assertIn("Per-box geography references", phase1_text)
        self.assertIn("Shared box reference", phase1_text)
        self.assertIn("first-code search box", phase1_text)
        self.assertIn("No matched PyGNOME Phase 4 comparison is packaged yet.", phase4_text)
        self.assertIn("Current Mindoro Phase 4 results are OpenDrift/OpenOil scenario outputs only.", phase4_text)
        self.assertIn("Budget-only deterministic PyGNOME comparator pilot.", legacy_text)
        self.assertIn("Shoreline comparison is not packaged because matched PyGNOME shoreline outputs are not available.", legacy_text)
        self.assertIn("First-code search context", legacy_text)
        self.assertIn("Historical origin source boxes", legacy_text)
        self.assertIn("first-code search box", legacy_text)
        self.assertIn("first three 2016 drifter cases", legacy_text)
        self.assertIn("west coast of the Philippines", legacy_text)
        self.assertIn("operative scientific/display extents", legacy_text)

    def test_ui_asset_branding_readme_covers_supported_paths_and_fallback(self):
        branding_text = (REPO_ROOT / "ui" / "assets" / "README.md").read_text(encoding="utf-8")
        self.assertIn("logo.svg", branding_text)
        self.assertIn("logo.png", branding_text)
        self.assertIn("logo_icon.svg", branding_text)
        self.assertIn("logo_icon.png", branding_text)
        self.assertIn("transparent background", branding_text)
        self.assertIn("1200 x 300", branding_text)
        self.assertIn("falls back to clean text-only branding", branding_text)

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
