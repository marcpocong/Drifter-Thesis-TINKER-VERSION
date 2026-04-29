import ast
import importlib
import re
import tempfile
import unittest
from pathlib import Path

from ui.bootstrap import discover_branding_assets


REPO_ROOT = Path(__file__).resolve().parents[1]


def _dashboard_module(name: str):
    try:
        return importlib.import_module(name)
    except ModuleNotFoundError as exc:
        raise unittest.SkipTest(f"dashboard dependency unavailable for {name}: {exc.name}") from exc


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

    def _function_parameters(self, path: Path, function_name: str) -> set[str]:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in tree.body:
            if isinstance(node, ast.FunctionDef) and node.name == function_name:
                return {arg.arg for arg in [*node.args.posonlyargs, *node.args.args, *node.args.kwonlyargs]}
        self.fail(f"{function_name} not found in {path}")

    def test_curated_package_roots_are_prioritized(self):
        data_access = _dashboard_module("ui.data_access")
        packages = data_access.curated_package_roots(REPO_ROOT)
        relative_paths = [str(package["relative_path"]).replace("\\", "/") for package in packages]
        self.assertGreaterEqual(len(relative_paths), 6)
        self.assertEqual(relative_paths[0], "output/Phase 3B March13-14 Final Output")
        self.assertEqual(relative_paths[1], "output/Phase 3B March13-14 Final Output/publication/comparator_pygnome")
        self.assertEqual(relative_paths[2], "output/final_validation_package")
        self.assertEqual(relative_paths[3], "output/Phase 3C DWH Final Output")
        self.assertIn("output/2016 Legacy Runs FINAL Figures", relative_paths)
        self.assertNotIn("output/CASE_MINDORO_RETRO_2023", relative_paths[:5])
        self.assertNotIn("output/CASE_DWH_RETRO_2010_72H", relative_paths[:5])

    def test_panel_mode_page_map_matches_presentation_order(self):
        data_access = _dashboard_module("ui.data_access")
        pages_module = _dashboard_module("ui.pages")
        state = data_access.build_dashboard_state(REPO_ROOT)
        panel_pages = pages_module.visible_page_definitions(state, advanced=False)
        panel_labels = [page.label for page in panel_pages]
        self.assertEqual(
            panel_labels,
            [
                "Defense / Panel Review",
                "Phase 1 Transport Provenance",
                "Mindoro B1 Primary Validation",
                "Mindoro Track A Comparator Support",
                "DWH External Transfer Validation",
                "Mindoro Oil-Type and Shoreline Context",
                "Archive — Mindoro Validation Provenance",
                "Archive — Legacy 2016 Support",
                "Data Sources & Provenance",
                "Artifacts / Logs / Registries",
            ],
        )
        self.assertEqual(
            [page.navigation_section for page in panel_pages],
            [
                "Main Defense Story",
                "Main Defense Story",
                "Main Defense Story",
                "Main Defense Story",
                "Main Defense Story",
                "Main Defense Story",
                "Archive / Support Only",
                "Archive / Support Only",
                "Reference",
                "Reference",
            ],
        )
        self.assertEqual(panel_pages[-1].navigation_section, "Reference")

    def test_page_registry_source_matches_panel_default_map(self):
        registry_path = REPO_ROOT / "ui" / "pages" / "__init__.py"
        tree = ast.parse(registry_path.read_text(encoding="utf-8"), filename=str(registry_path))
        definitions: list[tuple[str, str, str, bool]] = []
        for node in ast.walk(tree):
            if not isinstance(node, ast.Assign):
                continue
            if not any(isinstance(target, ast.Name) and target.id == "PAGE_DEFINITIONS" for target in node.targets):
                continue
            for item in getattr(node.value, "elts", []):
                if not isinstance(item, ast.Call):
                    continue
                page_id = item.args[0].value
                label = item.args[1].value
                section = "Study"
                advanced_only = False
                for keyword in item.keywords:
                    if keyword.arg == "navigation_section":
                        section = keyword.value.value
                    elif keyword.arg == "advanced_only":
                        advanced_only = bool(keyword.value.value)
                definitions.append((page_id, label, section, advanced_only))
        default_definitions = [definition for definition in definitions if not definition[3]]
        self.assertEqual(
            [definition[1] for definition in default_definitions],
            [
                "Defense / Panel Review",
                "Phase 1 Transport Provenance",
                "Mindoro B1 Primary Validation",
                "Mindoro Track A Comparator Support",
                "DWH External Transfer Validation",
                "Mindoro Oil-Type and Shoreline Context",
                "Archive — Mindoro Validation Provenance",
                "Archive — Legacy 2016 Support",
                "Data Sources & Provenance",
                "Artifacts / Logs / Registries",
            ],
        )
        self.assertEqual(
            sorted({definition[2] for definition in default_definitions}),
            ["Archive / Support Only", "Main Defense Story", "Reference"],
        )

    def test_phase1_page_stays_visible_when_focused_artifacts_are_missing(self):
        data_access = _dashboard_module("ui.data_access")
        pages_module = _dashboard_module("ui.pages")
        state = data_access.build_dashboard_state(REPO_ROOT)
        state["phase1_focused_manifest"] = {}
        state["phase1_focused_recipe_ranking"] = state["phase1_focused_recipe_ranking"].iloc[0:0].copy()
        panel_labels = [page.label for page in pages_module.visible_page_definitions(state, advanced=False)]
        self.assertIn("Phase 1 Transport Provenance", panel_labels)

    def test_panel_facing_ui_source_avoids_internal_status_strings(self):
        forbidden_tokens = (
            "reportable_now_" + "inherited_provisional",
            "not_comparable_" + "honestly",
            "current " + "hon" + "esty status",
            "Inherited-provisional " + "tracks",
            "Reportable " + "tracks",
            "Current " + "hon" + "esty status",
            "not" + "_frozen",
            "st.rerun(",
        )
        files_to_check = (
            REPO_ROOT / "ui" / "evidence_contract.py",
            REPO_ROOT / "ui" / "pages" / "common.py",
            REPO_ROOT / "ui" / "pages" / "home.py",
            REPO_ROOT / "ui" / "pages" / "phase1_recipe_selection.py",
            REPO_ROOT / "ui" / "pages" / "b1_drifter_context.py",
            REPO_ROOT / "ui" / "pages" / "mindoro_validation.py",
            REPO_ROOT / "ui" / "pages" / "cross_model_comparison.py",
            REPO_ROOT / "ui" / "pages" / "dwh_transfer_validation.py",
            REPO_ROOT / "ui" / "pages" / "phase4_oiltype_and_shoreline.py",
            REPO_ROOT / "ui" / "pages" / "legacy_2016_support.py",
            REPO_ROOT / "ui" / "pages" / "data_sources.py",
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
            REPO_ROOT / "ui" / "pages" / "b1_drifter_context.py",
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
        common_path = REPO_ROOT / "ui" / "pages" / "common.py"
        helper_signatures = {
            "render_figure_cards": self._function_parameters(common_path, "render_figure_cards"),
            "render_table": self._function_parameters(common_path, "render_table"),
            "render_markdown_block": self._function_parameters(common_path, "render_markdown_block"),
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
        b1_drifter_text = (REPO_ROOT / "ui" / "pages" / "b1_drifter_context.py").read_text(encoding="utf-8")
        mindoro_text = (REPO_ROOT / "ui" / "pages" / "mindoro_validation.py").read_text(encoding="utf-8")
        comparator_text = (REPO_ROOT / "ui" / "pages" / "cross_model_comparison.py").read_text(encoding="utf-8")
        dwh_text = (REPO_ROOT / "ui" / "pages" / "dwh_transfer_validation.py").read_text(encoding="utf-8")
        phase4_text = (REPO_ROOT / "ui" / "pages" / "phase4_oiltype_and_shoreline.py").read_text(encoding="utf-8")
        archive_text = (REPO_ROOT / "ui" / "pages" / "mindoro_validation_archive.py").read_text(encoding="utf-8")
        legacy_text = (REPO_ROOT / "ui" / "pages" / "legacy_2016_support.py").read_text(encoding="utf-8")

        self.assertIn("Phase 1 Transport Provenance", phase1_text)
        self.assertIn("Drifter segments support transport-provenance and recipe selection; they are not direct oil-footprint truth.", phase1_text)
        self.assertIn("cmems_gfs", phase1_text)
        self.assertIn("4.5886", phase1_text)
        self.assertIn("65", phase1_text)
        self.assertIn("19", phase1_text)
        self.assertIn("Study Box 2 - mindoro_case_domain", phase1_text)
        self.assertIn("Study Box 4 - prototype first-code search box", phase1_text)
        self.assertIn("B1 Recipe Provenance — Not Truth Mask", b1_drifter_text)
        self.assertIn("Drifter segments support transport-provenance and recipe selection; they are not direct oil-footprint truth.", b1_drifter_text)
        self.assertIn("No exact 1 km overlap; this supports coastal-neighborhood usefulness, not exact-grid reproduction.", mindoro_text)
        self.assertIn("The March 13–14 pair shares March 12 WorldView-3 imagery provenance", mindoro_text)
        self.assertIn("0.1075", mindoro_text)
        self.assertNotIn("B2", mindoro_text)
        self.assertNotIn("B3", mindoro_text)
        self.assertNotIn("March 3", mindoro_text)
        self.assertIn("Mindoro Track A Comparator Support", comparator_text)
        self.assertIn("PyGNOME is comparator-only and is not observational truth.", comparator_text)
        self.assertIn("0.0061", comparator_text)
        self.assertIn("DWH External Transfer Validation", dwh_text)
        self.assertIn("DWH is a separate external transfer validation story; it does not recalibrate Mindoro", dwh_text)
        self.assertIn("0.5568", dwh_text)
        self.assertIn("0.5389", dwh_text)
        self.assertIn("0.4966", dwh_text)
        self.assertIn("0.3612", dwh_text)
        self.assertIn("Mindoro Oil-Type and Shoreline Context", phase4_text)
        self.assertIn("Support/context only; not a primary validation phase.", phase4_text)
        self.assertIn("No matched Mindoro PyGNOME fate-and-shoreline comparison is packaged yet.", phase4_text)
        self.assertIn("0.02%", phase4_text)
        self.assertIn("0.61%", phase4_text)
        self.assertIn("0.63%", phase4_text)
        self.assertIn("Budget-only deterministic PyGNOME comparator pilot.", legacy_text)
        self.assertIn("Shoreline comparison is not packaged because matched PyGNOME shoreline outputs are not available.", legacy_text)
        self.assertIn("Archive — Mindoro Validation Provenance", archive_text)
        self.assertIn("ARCHIVE / SUPPORT ONLY — not part of the main Mindoro validation claim.", archive_text)
        self.assertIn("ROLE_ARCHIVE", archive_text)
        self.assertIn("Archive — Legacy 2016 Support", legacy_text)
        self.assertIn("ARCHIVE / SUPPORT ONLY — not part of the main Mindoro validation claim.", legacy_text)
        self.assertIn("ROLE_LEGACY", legacy_text)
        self.assertIn("First-code search context", legacy_text)
        self.assertIn("Historical origin source boxes", legacy_text)
        self.assertIn("first-code search box", legacy_text)
        self.assertIn("first three 2016 drifter cases", legacy_text)
        self.assertIn("west coast of the Philippines", legacy_text)
        self.assertIn("operative scientific/display extents", legacy_text)

    def test_non_archive_pages_have_no_archive_content_leak_terms(self):
        allowed_home_terms = {"Archive — Legacy 2016 Support"}
        pages_to_check = (
            REPO_ROOT / "ui" / "pages" / "phase1_recipe_selection.py",
            REPO_ROOT / "ui" / "pages" / "mindoro_validation.py",
            REPO_ROOT / "ui" / "pages" / "cross_model_comparison.py",
            REPO_ROOT / "ui" / "pages" / "dwh_transfer_validation.py",
            REPO_ROOT / "ui" / "pages" / "phase4_oiltype_and_shoreline.py",
            REPO_ROOT / "ui" / "pages" / "artifacts_logs.py",
        )
        blocked_terms = ("R0", "B2", "B3", "March 3 -> March 6", "March 6", "March3", "March6", "prototype_2016", "Legacy 2016")
        for path in pages_to_check:
            text = path.read_text(encoding="utf-8")
            with self.subTest(path=path.name):
                for term in blocked_terms:
                    self.assertNotIn(term, text)
        home_text = (REPO_ROOT / "ui" / "pages" / "home.py").read_text(encoding="utf-8")
        home_text_without_allowed_archive_link = home_text
        for allowed in allowed_home_terms:
            home_text_without_allowed_archive_link = home_text_without_allowed_archive_link.replace(allowed, "")
        for term in blocked_terms:
            self.assertNotIn(term, home_text_without_allowed_archive_link)

    def test_evidence_contract_usage_is_not_test_only(self):
        contract_path = REPO_ROOT / "ui" / "evidence_contract.py"
        self.assertTrue(contract_path.exists())
        contract_text = contract_path.read_text(encoding="utf-8")
        for phrase in (
            "def filter_for_page",
            "def assert_no_archive_leak",
            "def role_badge_for_record",
            "def panel_safe_label",
            "THESIS-FACING",
            "COMPARATOR SUPPORT",
            "SUPPORT / CONTEXT ONLY",
            "ARCHIVE ONLY",
            "LEGACY / ARCHIVE SUPPORT",
            "ADVANCED TECHNICAL REFERENCE",
        ):
            self.assertIn(phrase, contract_text)
        page_texts = "\n".join(path.read_text(encoding="utf-8") for path in (REPO_ROOT / "ui" / "pages").glob("*.py"))
        data_access_text = (REPO_ROOT / "ui" / "data_access.py").read_text(encoding="utf-8")
        self.assertIn("role_badge_for_record", page_texts)
        self.assertIn("panel_safe_label", page_texts)
        self.assertIn("filter_for_page", page_texts + data_access_text)
        self.assertIn("assert_no_archive_leak", page_texts + data_access_text)

    def test_central_evidence_contract_filters_default_panel_leaks(self):
        data_access = _dashboard_module("ui.data_access")
        evidence_contract = _dashboard_module("ui.evidence_contract")
        state = data_access.build_dashboard_state(REPO_ROOT)
        home_figures = evidence_contract.filter_for_page(state["publication_registry"], "home", advanced=False)
        evidence_contract.assert_no_archive_leak(home_figures, "home", advanced=False)
        if "archive_only" in home_figures.columns:
            self.assertFalse(home_figures["archive_only"].fillna(False).astype(bool).any())
        if "legacy_support" in home_figures.columns:
            self.assertFalse(home_figures["legacy_support"].fillna(False).astype(bool).any())

        mindoro_figures = evidence_contract.filter_for_page(state["mindoro_final_registry"], "mindoro_validation", advanced=False)
        evidence_contract.assert_no_archive_leak(mindoro_figures, "mindoro_validation", advanced=False)
        joined = " ".join(mindoro_figures.astype(str).agg(" ".join, axis=1).tolist()).lower()
        self.assertIsNone(re.search(r"\br0\b", joined))
        self.assertIsNone(re.search(r"\bb2\b", joined))
        self.assertIsNone(re.search(r"\bb3\b", joined))

        self.assertEqual(evidence_contract.role_badge_for_record({"surface_key": "thesis_main"}), evidence_contract.ROLE_THESIS)
        self.assertEqual(evidence_contract.role_badge_for_record({"surface_key": "comparator_support"}), evidence_contract.ROLE_COMPARATOR)
        self.assertEqual(evidence_contract.role_badge_for_record({"status_key": "mindoro_phase4_oil_budget"}), evidence_contract.ROLE_CONTEXT)
        self.assertEqual(evidence_contract.role_badge_for_record({"surface_key": "archive_only"}), evidence_contract.ROLE_ARCHIVE)
        self.assertEqual(evidence_contract.role_badge_for_record({"surface_key": "legacy_support"}), evidence_contract.ROLE_LEGACY)

    def test_artifacts_logs_keeps_raw_previews_advanced_only(self):
        artifacts_text = (REPO_ROOT / "ui" / "pages" / "artifacts_logs.py").read_text(encoding="utf-8")
        self.assertIn('if ui_state["advanced"] and not export_mode:', artifacts_text)
        self.assertIn("Output catalog", artifacts_text)
        self.assertIn("Manifest index", artifacts_text)
        self.assertIn("Log index", artifacts_text)
        self.assertIn("Panel-friendly mode keeps raw artifact, manifest, and log previews out of the main story.", artifacts_text)

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
