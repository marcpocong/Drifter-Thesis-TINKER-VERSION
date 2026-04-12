import importlib
import subprocess
import sys
import textwrap
import unittest
from pathlib import Path

try:
    from streamlit.testing.v1 import AppTest
except ImportError:  # pragma: no cover - host environments may not have streamlit installed
    AppTest = None


REPO_ROOT = Path(__file__).resolve().parents[1]
APP_PATH = REPO_ROOT / "ui" / "app.py"
PAGES_DIR = REPO_ROOT / "ui" / "pages"


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
