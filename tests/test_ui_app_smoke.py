import unittest
from pathlib import Path

try:
    from streamlit.testing.v1 import AppTest
except ImportError:  # pragma: no cover - host environments may not have streamlit installed
    AppTest = None


APP_PATH = Path(__file__).resolve().parents[1] / "ui" / "app.py"


@unittest.skipIf(AppTest is None, "streamlit.testing is not available")
class UiAppSmokeTests(unittest.TestCase):
    def test_key_pages_render_without_exceptions(self):
        at = AppTest.from_file(str(APP_PATH), default_timeout=60)
        at.run()
        self.assertFalse(at.exception)

        for label, expected_title in (
            ("Mindoro Validation", "Mindoro Validation"),
            ("DWH Transfer Validation", "DWH Transfer Validation"),
            ("Phase 4 Oil-Type & Shoreline", "Phase 4 Oil-Type & Shoreline"),
            ("Phase 4 Cross-Model Status", "Phase 4 Cross-Model Status"),
            ("Trajectory Explorer", "Trajectory Explorer"),
        ):
            at.sidebar.selectbox[1].set_value(label).run()
            self.assertFalse(at.exception, msg=f"App raised an exception while loading {label}")
            titles = [element.value for element in at.title]
            self.assertIn(expected_title, titles)


if __name__ == "__main__":
    unittest.main()
