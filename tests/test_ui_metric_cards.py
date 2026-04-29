import importlib
import importlib.util
import unittest
from unittest.mock import patch


STREAMLIT_MISSING = importlib.util.find_spec("streamlit") is None


@unittest.skipIf(STREAMLIT_MISSING, "streamlit is not available")
class UiMetricCardTests(unittest.TestCase):
    def test_render_metric_row_accepts_long_wrapped_values(self):
        from ui.pages.common import render_metric_row

        long_label = "Phase 4 status with enough wording to require wrapping in panel review"
        long_value = "SUPPORT / CONTEXT ONLY; no matched Mindoro Phase 4 PyGNOME fate-and-shoreline package"
        with patch("ui.pages.common.st.markdown") as markdown:
            render_metric_row(
                [
                    {
                        "label": long_label,
                        "value": long_value,
                        "note": "This note should wrap instead of forcing horizontal overflow.",
                        "full_width": True,
                    }
                ],
                compact=True,
            )
        markdown.assert_called_once()
        rendered_html = markdown.call_args.args[0]
        self.assertIn("ui-metric-grid--compact", rendered_html)
        self.assertIn("ui-metric-card--full", rendered_html)
        self.assertIn("SUPPORT / CONTEXT ONLY", rendered_html)

    def test_metric_card_html_escapes_unsafe_text(self):
        from ui.pages.common import build_metric_row_html

        rendered_html = build_metric_row_html(
            [
                {
                    "label": "<script>alert('label')</script>",
                    "value": "<b>unsafe value</b>",
                    "note": 'quote " and ampersand &',
                }
            ]
        )
        self.assertIn("&lt;script&gt;", rendered_html)
        self.assertIn("&lt;b&gt;unsafe value&lt;/b&gt;", rendered_html)
        self.assertIn("ampersand &amp;", rendered_html)
        self.assertNotIn("<script>", rendered_html)
        self.assertNotIn("<b>unsafe value</b>", rendered_html)

    def test_required_dashboard_pages_import(self):
        for module_name in (
            "ui.app",
            "ui.pages.common",
            "ui.pages.home",
            "ui.pages.phase1_recipe_selection",
            "ui.pages.b1_drifter_context",
            "ui.pages.dwh_transfer_validation",
        ):
            with self.subTest(module=module_name):
                importlib.import_module(module_name)


if __name__ == "__main__":
    unittest.main()
