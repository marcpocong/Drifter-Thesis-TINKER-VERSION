import importlib
import subprocess
import sys
import textwrap
import unittest
from pathlib import Path
from unittest.mock import patch

try:
    from streamlit.testing.v1 import AppTest
except ImportError:  # pragma: no cover - host environments may not have streamlit installed
    AppTest = None

from ui.data_access import build_dashboard_state


REPO_ROOT = Path(__file__).resolve().parents[1]
APP_PATH = REPO_ROOT / "ui" / "app.py"
PAGES_DIR = REPO_ROOT / "ui" / "pages"


def _phase1_wrapper_for_test() -> None:
    from ui.data_access import build_dashboard_state
    from ui.pages import phase1_recipe_selection

    state = build_dashboard_state()
    panel_state = {"advanced": False, "mode_label": "Panel-friendly", "visual_layer": "publication", "export_mode": False}
    phase1_recipe_selection.render(state, panel_state)


def _phase1_wrapper_missing_focused_for_test() -> None:
    from ui.data_access import build_dashboard_state
    from ui.pages import phase1_recipe_selection

    state = build_dashboard_state()
    state["phase1_focused_manifest"] = {}
    state["phase1_focused_recipe_ranking"] = state["phase1_focused_recipe_ranking"].iloc[0:0].copy()
    state["phase1_focused_recipe_summary"] = state["phase1_focused_recipe_summary"].iloc[0:0].copy()
    state["phase1_focused_accepted_segments"] = state["phase1_focused_accepted_segments"].iloc[0:0].copy()
    state["phase1_focused_ranking_subset"] = state["phase1_focused_ranking_subset"].iloc[0:0].copy()
    state["phase1_focused_loading_audit"] = state["phase1_focused_loading_audit"].iloc[0:0].copy()
    panel_state = {"advanced": False, "mode_label": "Panel-friendly", "visual_layer": "publication", "export_mode": False}
    phase1_recipe_selection.render(state, panel_state)


def _phase1_advanced_wrapper_for_test() -> None:
    from ui.data_access import build_dashboard_state
    from ui.pages import phase1_recipe_selection

    state = build_dashboard_state()
    advanced_state = {"advanced": True, "mode_label": "Advanced", "visual_layer": "publication", "export_mode": False}
    phase1_recipe_selection.render(state, advanced_state)


def _phase4_wrapper_for_test() -> None:
    from ui.data_access import build_dashboard_state
    from ui.pages import phase4_oiltype_and_shoreline

    state = build_dashboard_state()
    panel_state = {"advanced": False, "mode_label": "Panel-friendly", "visual_layer": "publication", "export_mode": False}
    phase4_oiltype_and_shoreline.render(state, panel_state)


def _mindoro_validation_wrapper_for_test() -> None:
    from ui.data_access import build_dashboard_state
    from ui.pages import mindoro_validation

    state = build_dashboard_state()
    panel_state = {"advanced": False, "mode_label": "Panel-friendly", "visual_layer": "publication", "export_mode": False}
    mindoro_validation.render(state, panel_state)


def _mindoro_validation_archive_wrapper_for_test() -> None:
    from ui.data_access import build_dashboard_state
    from ui.pages import mindoro_validation_archive

    state = build_dashboard_state()
    panel_state = {"advanced": False, "mode_label": "Panel-friendly", "visual_layer": "publication", "export_mode": False}
    mindoro_validation_archive.render(state, panel_state)


def _cross_model_wrapper_for_test() -> None:
    from ui.data_access import build_dashboard_state
    from ui.pages import cross_model_comparison

    state = build_dashboard_state()
    panel_state = {"advanced": False, "mode_label": "Panel-friendly", "visual_layer": "publication", "export_mode": False}
    cross_model_comparison.render(state, panel_state)


def _dwh_wrapper_for_test() -> None:
    from ui.data_access import build_dashboard_state
    from ui.pages import dwh_transfer_validation

    state = build_dashboard_state()
    panel_state = {"advanced": False, "mode_label": "Panel-friendly", "visual_layer": "publication", "export_mode": False}
    dwh_transfer_validation.render(state, panel_state)


def _legacy_wrapper_for_test() -> None:
    from ui.data_access import build_dashboard_state
    from ui.pages import legacy_2016_support

    state = build_dashboard_state()
    panel_state = {"advanced": False, "mode_label": "Panel-friendly", "visual_layer": "publication", "export_mode": False}
    legacy_2016_support.render(state, panel_state)


def _home_panel_wrapper_for_test() -> None:
    from ui.data_access import build_dashboard_state
    from ui.pages import home

    state = build_dashboard_state()
    panel_state = {"advanced": False, "mode_label": "Panel-friendly", "visual_layer": "publication", "export_mode": False}
    home.render(state, panel_state)


def _home_advanced_wrapper_for_test() -> None:
    from ui.data_access import build_dashboard_state
    from ui.pages import home

    state = build_dashboard_state()
    advanced_state = {"advanced": True, "mode_label": "Advanced", "visual_layer": "publication", "export_mode": False}
    home.render(state, advanced_state)


def _home_export_wrapper_for_test() -> None:
    from ui.data_access import build_dashboard_state
    from ui.pages import home

    state = build_dashboard_state()
    export_state = {"advanced": False, "mode_label": "Export", "visual_layer": "publication", "export_mode": True}
    home.render(state, export_state)


def _app_main_missing_branding_for_test() -> None:
    import ui.app as app
    from ui.data_access import REPO_ROOT as repo_root
    from unittest.mock import patch

    empty_branding = {
        "assets_root": repo_root / "ui" / "assets",
        "logo_path": None,
        "icon_path": None,
        "page_icon_path": None,
        "has_logo": False,
        "has_icon": False,
    }
    with patch.object(app, "discover_branding_assets", return_value=empty_branding):
        app.main()


def _phase1_export_wrapper_for_test() -> None:
    from ui.data_access import build_dashboard_state
    from ui.pages import phase1_recipe_selection

    state = build_dashboard_state()
    export_state = {"advanced": False, "mode_label": "Export", "visual_layer": "publication", "export_mode": True}
    phase1_recipe_selection.render(state, export_state)


def _missing_image_gallery_wrapper_for_test() -> None:
    import pandas as pd

    from ui.data_access import build_dashboard_state
    from ui.pages.common import render_figure_gallery

    state = build_dashboard_state()
    existing = state.get("home_featured_publication_figures", state["curated_recommended_figures"]).head(1).copy()
    if existing.empty:
        existing = pd.DataFrame(
            [
                {
                    "figure_id": "existing_gallery_test",
                    "display_title": "Existing gallery test figure",
                    "relative_path": "",
                    "notes": "Fallback gallery test figure.",
                }
            ]
        )
    missing = existing.iloc[[0]].copy()
    missing["figure_id"] = "missing_gallery_test"
    missing["display_title"] = "Missing gallery test figure"
    missing["relative_path"] = "output/figure_package_publication/definitely_missing_gallery_test.png"
    if "resolved_path" in missing.columns:
        missing["resolved_path"] = ""
    gallery_df = pd.concat([existing, missing], ignore_index=True)
    render_figure_gallery(
        gallery_df,
        title="Missing image gallery smoke",
        caption="This gallery intentionally mixes one available figure with one missing file.",
        columns_per_row=2,
        overlay_label="Click to enlarge",
    )


def _probe_script_style_import(path: Path) -> subprocess.CompletedProcess[str]:
    code = textwrap.dedent(
        f"""
        import importlib.util
        import pathlib
        import sys

        repo_root = pathlib.Path(r"{REPO_ROOT}").resolve()
        target = pathlib.Path(r"{path}").resolve()
        script_dir = target.parent.resolve()
        sys.path = [str(script_dir)] + [
            entry
            for entry in sys.path
            if entry not in ("", str(repo_root), str(script_dir))
        ]
        spec = importlib.util.spec_from_file_location("streamlit_script_probe", str(target))
        module = importlib.util.module_from_spec(spec)
        if spec.loader is None:
            raise RuntimeError("Unable to load module spec")
        spec.loader.exec_module(module)
        print("IMPORT_OK")
        """
    )
    return subprocess.run(
        [sys.executable, "-c", code],
        cwd=str(REPO_ROOT),
        text=True,
        capture_output=True,
        check=False,
    )


class UiImportBootstrapTests(unittest.TestCase):
    def test_package_imports_work(self):
        importlib.import_module("ui.app")
        importlib.import_module("ui.data_access")

    def test_page_modules_import_through_package(self):
        for page_path in sorted(PAGES_DIR.glob("*.py")):
            if page_path.name in {"__init__.py", "common.py"}:
                continue
            with self.subTest(page=page_path.name):
                importlib.import_module(f"ui.pages.{page_path.stem}")

    def test_app_bootstrap_supports_streamlit_script_style_import(self):
        result = _probe_script_style_import(APP_PATH)
        self.assertEqual(result.returncode, 0, msg=result.stderr or result.stdout)
        self.assertIn("IMPORT_OK", result.stdout)

    def test_page_bootstrap_supports_streamlit_script_style_import(self):
        for page_path in sorted(PAGES_DIR.glob("*.py")):
            if page_path.name in {"__init__.py", "common.py"}:
                continue
            with self.subTest(page=page_path.name):
                result = _probe_script_style_import(page_path)
                self.assertEqual(result.returncode, 0, msg=result.stderr or result.stdout)
                self.assertIn("IMPORT_OK", result.stdout)


@unittest.skipIf(AppTest is None, "streamlit.testing is not available")
class UiAppSmokeTests(unittest.TestCase):
    def _gallery_tile_count(self, at: AppTest) -> int:
        return sum("figure-gallery-card__title" in element.value for element in at.markdown)

    def _visible_text(self, at: AppTest, *element_names: str) -> str:
        chunks: list[str] = []
        for element_name in element_names:
            for element in getattr(at, element_name, []):
                value = getattr(element, "value", "")
                if isinstance(value, str) and value:
                    chunks.append(value)
        return " ".join(chunks)

    def test_app_home_renders_without_exceptions(self):
        at = AppTest.from_file(str(APP_PATH), default_timeout=60)
        at.run()
        self.assertFalse(at.exception)
        titles = [element.value for element in at.title]
        self.assertIn("Home / Overview", titles)
        self.assertEqual(len(at.sidebar.radio), 1)
        self.assertEqual(at.sidebar.radio[0].label, "Viewing mode")
        self.assertEqual(len(at.sidebar.selectbox), 0)

    def test_app_renders_without_logo_assets(self):
        at = AppTest.from_function(_app_main_missing_branding_for_test, default_timeout=60)
        at.run()
        self.assertFalse(at.exception)
        titles = [element.value for element in at.title]
        self.assertIn("Home / Overview", titles)

    def test_pages_render_via_wrapper_functions(self):
        for wrapper, expected_title in (
            (_home_panel_wrapper_for_test, "Home / Overview"),
            (_home_advanced_wrapper_for_test, "Home / Overview"),
            (_mindoro_validation_wrapper_for_test, "Mindoro B1 Primary Validation"),
            (_mindoro_validation_archive_wrapper_for_test, "Mindoro Validation Archive"),
            (_cross_model_wrapper_for_test, "Mindoro Cross-Model Comparator"),
            (_dwh_wrapper_for_test, "DWH Phase 3C Transfer Validation"),
            (_legacy_wrapper_for_test, "Legacy 2016 Support Package"),
            (_phase1_wrapper_for_test, "Phase 1 Recipe Selection"),
            (_phase1_advanced_wrapper_for_test, "Phase 1 Recipe Selection"),
            (_phase4_wrapper_for_test, "Phase 4 Oil-Type and Shoreline Context"),
            (_home_export_wrapper_for_test, "Home / Overview"),
            (_phase1_export_wrapper_for_test, "Phase 1 Recipe Selection"),
        ):
            at = AppTest.from_function(wrapper, default_timeout=60)
            at.run()
            self.assertFalse(at.exception, msg=f"Wrapper page raised for {expected_title}")
            titles = [element.value for element in at.title]
            self.assertIn(expected_title, titles)

    def test_home_panel_gallery_surfaces_multiple_tiles_without_dropdown(self):
        at = AppTest.from_function(_home_panel_wrapper_for_test, default_timeout=60)
        at.run()
        self.assertFalse(at.exception)
        selectbox_labels = [element.label for element in at.selectbox]
        self.assertNotIn("Featured figure", selectbox_labels)
        button_labels = [element.label for element in at.button]
        self.assertIn("Click to enlarge", button_labels)
        self.assertNotIn("Enlarge figure", button_labels)
        expected_count = len(build_dashboard_state(REPO_ROOT)["home_featured_publication_figures"])
        self.assertEqual(self._gallery_tile_count(at), expected_count)
        text_blocks = self._visible_text(at, "markdown", "subheader")
        self.assertNotIn("keyboard_double", text_blocks)
        self.assertIn("Primary thesis story", text_blocks)
        self.assertIn("Secondary lanes", text_blocks)
        self.assertIn("Workflow / provenance context", text_blocks)
        self.assertIn("Archive only", text_blocks)
        self.assertIn("Legacy support", text_blocks)
        self.assertNotIn("Legacy 2016 support triptychs first", text_blocks)

    def test_home_panel_gallery_excludes_mindoro_march3_to_march6_tiles(self):
        at = AppTest.from_function(_home_panel_wrapper_for_test, default_timeout=60)
        at.run()
        self.assertFalse(at.exception)
        text_blocks = self._visible_text(at, "markdown", "subheader", "caption")
        self.assertIn("Mindoro archive, legacy March-family, R0, and Phase 4 figures remain available", text_blocks)
        self.assertNotIn("Mindoro March 3 -> March 6", text_blocks)
        self.assertNotIn("Mindoro March 6", text_blocks)

    def test_home_advanced_gallery_matches_panel_gallery_count(self):
        at = AppTest.from_function(_home_advanced_wrapper_for_test, default_timeout=60)
        at.run()
        self.assertFalse(at.exception)
        selectbox_labels = [element.label for element in at.selectbox]
        self.assertNotIn("Featured figure", selectbox_labels)
        button_labels = [element.label for element in at.button]
        self.assertIn("Click to enlarge", button_labels)
        self.assertNotIn("Enlarge figure", button_labels)
        expected_count = len(build_dashboard_state(REPO_ROOT)["home_featured_publication_figures"])
        self.assertEqual(self._gallery_tile_count(at), expected_count)

    def test_publication_package_pages_use_panel_gallery_without_figure_dropdowns(self):
        for wrapper, minimum_tiles in (
            (_mindoro_validation_wrapper_for_test, 4),
            (_mindoro_validation_archive_wrapper_for_test, 3),
            (_cross_model_wrapper_for_test, 2),
            (_dwh_wrapper_for_test, 11),
            (_legacy_wrapper_for_test, 4),
            (_phase4_wrapper_for_test, 2),
        ):
            with self.subTest(wrapper=wrapper.__name__):
                at = AppTest.from_function(wrapper, default_timeout=60)
                at.run()
                self.assertFalse(at.exception)
                selectbox_labels = [element.label for element in at.selectbox]
                self.assertEqual(selectbox_labels, [])
                self.assertGreaterEqual(self._gallery_tile_count(at), minimum_tiles)

    def test_gallery_click_to_enlarge_dialog_opens_without_crashing(self):
        at = AppTest.from_function(_home_panel_wrapper_for_test, default_timeout=60)
        at.run()
        self.assertFalse(at.exception)
        open_buttons = [element for element in at.button if element.label == "Click to enlarge"]
        self.assertTrue(open_buttons)
        open_buttons[0].click().run()
        self.assertFalse(at.exception)
        button_labels = [element.label for element in at.button]
        self.assertIn("Close preview", button_labels)

    def test_export_mode_renders_note_cards_without_sidebar_controls(self):
        at = AppTest.from_function(_home_export_wrapper_for_test, default_timeout=60)
        at.run()
        self.assertFalse(at.exception)
        text_blocks = " ".join(element.value for element in at.markdown)
        self.assertIn("Export mode converts the dashboard into a print-friendly snapshot", text_blocks)
        button_labels = [element.label for element in at.button]
        self.assertNotIn("Enlarge figure", button_labels)
        self.assertEqual(self._gallery_tile_count(at), 2)

    def test_missing_image_gallery_degrades_gracefully(self):
        at = AppTest.from_function(_missing_image_gallery_wrapper_for_test, default_timeout=60)
        at.run()
        self.assertFalse(at.exception)
        self.assertEqual(self._gallery_tile_count(at), 2)
        text_blocks = " ".join(element.value for element in at.markdown)
        self.assertIn("Missing gallery test figure", text_blocks)

    def test_phase1_page_degrades_gracefully_when_focused_artifacts_are_missing(self):
        at = AppTest.from_function(_phase1_wrapper_missing_focused_for_test, default_timeout=60)
        at.run()
        self.assertFalse(at.exception)
        info_text = " ".join(element.value for element in at.warning)
        self.assertIn("falls back to the broader regional reference artifacts", info_text)

    def test_phase1_page_calls_out_cmems_gfs_and_keeps_cmems_era5_as_runner_up(self):
        at = AppTest.from_function(_phase1_wrapper_for_test, default_timeout=60)
        at.run()
        self.assertFalse(at.exception)
        text_blocks = self._visible_text(
            at,
            "markdown",
            "subheader",
            "info",
            "warning",
            "success",
        )
        self.assertIn("Selected Mindoro B1 recipe: `cmems_gfs`", text_blocks)
        self.assertIn("`cmems_era5` as the runner-up", text_blocks)
        self.assertIn("Diagnostic recipe summary, not winner ranking", text_blocks)
        self.assertNotIn("Selected Mindoro B1 recipe: `cmems_era5`", text_blocks)
        self.assertNotIn("Focused recipe summary", text_blocks)

    def test_phase1_page_surfaces_only_main_thesis_boxes_in_default_mode(self):
        at = AppTest.from_function(_phase1_wrapper_for_test, default_timeout=60)
        at.run()
        self.assertFalse(at.exception)
        text_blocks = self._visible_text(
            at,
            "markdown",
            "subheader",
            "info",
            "warning",
        )
        lowered = text_blocks.lower()
        self.assertEqual(lowered.count("study boxes used by the thesis"), 1)
        self.assertIn("study boxes used by the thesis (boxes 2 and 4)", lowered)
        self.assertIn("per-box geography references for the thesis (boxes 2 and 4)", lowered)
        self.assertNotIn("archived per-box geography references (boxes 1 and 3)", lowered)
        self.assertEqual(self._gallery_tile_count(at), 3)
        self.assertIn("study box 2", lowered)
        self.assertIn("study box 4", lowered)
        self.assertIn("mindoro_case_domain", lowered)
        self.assertIn("first-code search-box", lowered)

    def test_phase1_page_surfaces_archive_only_boxes_in_advanced_mode(self):
        at = AppTest.from_function(_phase1_advanced_wrapper_for_test, default_timeout=60)
        at.run()
        self.assertFalse(at.exception)
        text_blocks = self._visible_text(
            at,
            "markdown",
            "subheader",
            "info",
            "warning",
        )
        lowered = text_blocks.lower()
        self.assertIn("archived per-box geography references (boxes 1 and 3)", lowered)
        self.assertIn("study box 1", lowered)
        self.assertIn("study box 3", lowered)
        self.assertIn("focused mindoro phase 1 box geography reference", lowered)
        self.assertIn("mindoro scoring-grid bounds geography reference", lowered)
        self.assertEqual(self._gallery_tile_count(at), 5)

    def test_legacy_page_surfaces_first_code_origin_story(self):
        at = AppTest.from_function(_legacy_wrapper_for_test, default_timeout=60)
        at.run()
        self.assertFalse(at.exception)
        text_blocks = " ".join(
            [element.value for element in at.markdown]
            + [element.value for element in at.info]
            + [element.value for element in at.warning]
        )
        lowered = text_blocks.lower()
        self.assertIn("first-code search box", lowered)
        self.assertIn("west coast of the philippines", lowered)
        self.assertIn("operative scientific/display extents", lowered)


if __name__ == "__main__":
    unittest.main()
