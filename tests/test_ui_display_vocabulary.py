from __future__ import annotations

import ast
import importlib.util
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
pytestmark = pytest.mark.skipif(
    importlib.util.find_spec("geopandas") is None,
    reason="dashboard dependencies unavailable",
)

UI_SOURCE_FILES = (
    REPO_ROOT / "ui" / "app.py",
    REPO_ROOT / "ui" / "data_access.py",
    REPO_ROOT / "ui" / "evidence_contract.py",
    REPO_ROOT / "ui" / "pages" / "__init__.py",
    REPO_ROOT / "ui" / "pages" / "common.py",
    REPO_ROOT / "ui" / "pages" / "home.py",
    REPO_ROOT / "ui" / "pages" / "mindoro_validation.py",
    REPO_ROOT / "ui" / "pages" / "cross_model_comparison.py",
    REPO_ROOT / "ui" / "pages" / "dwh_transfer_validation.py",
    REPO_ROOT / "ui" / "pages" / "phase4_oiltype_and_shoreline.py",
    REPO_ROOT / "ui" / "pages" / "phase4_crossmodel_status.py",
    REPO_ROOT / "ui" / "pages" / "phase1_recipe_selection.py",
    REPO_ROOT / "ui" / "pages" / "mindoro_validation_archive.py",
    REPO_ROOT / "ui" / "pages" / "legacy_2016_support.py",
)

FORBIDDEN_PANEL_TERMS = (
    "Draft 22",
    "DWH Phase 3C",
    "Phase 3B",
    "Phase 3C",
    "Phase 4 context",
)

EXPECTED_PACKAGE_LABELS = {
    "mindoro_b1_final": "Mindoro B1 primary validation package",
    "mindoro_comparator": "Mindoro Track A comparator support package",
    "dwh_phase3c_final": "DWH external transfer-validation package",
    "phase4_context_status": "Mindoro oil-type and shoreline support/context",
}


def _string_literals(path: Path) -> list[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    values: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            values.append(node.value)
    return values


def _allowed_legacy_path(value: str) -> bool:
    normalized = value.replace("\\", "/")
    return (
        normalized.startswith("output/Phase 3B ")
        or normalized.startswith("output/Phase 3C ")
        or normalized.startswith("Phase 3B ")
        or normalized.startswith("Phase 3C ")
    )


def test_panel_facing_ui_source_strings_avoid_old_phase_story_labels():
    violations: list[str] = []
    for path in UI_SOURCE_FILES:
        for value in _string_literals(path):
            if _allowed_legacy_path(value):
                continue
            for term in FORBIDDEN_PANEL_TERMS:
                if term in value:
                    rel_path = path.relative_to(REPO_ROOT).as_posix()
                    violations.append(f"{rel_path}: {term!r} in {value!r}")

    assert not violations, "\n".join(violations)


def test_curated_package_display_labels_use_final_story_vocabulary():
    from ui.data_access import build_dashboard_state

    state = build_dashboard_state(REPO_ROOT)
    packages = {package["package_id"]: package for package in state["curated_package_roots"]}

    for package_id, expected_label in EXPECTED_PACKAGE_LABELS.items():
        assert packages[package_id]["label"] == expected_label

    for package in packages.values():
        for key in ("label", "description", "secondary_note"):
            value = str(package.get(key, ""))
            for term in FORBIDDEN_PANEL_TERMS:
                assert term not in value, f"{package['package_id']} {key} exposed {term!r}"


def test_stored_export_root_caption_marks_legacy_folder_names():
    from ui.pages.common import _path_caption_for

    assert _path_caption_for("output/final_validation_package") == "Stored export root: output/final_validation_package"
    assert (
        _path_caption_for("output/Phase 3B March13-14 Final Output")
        == "Stored export root (legacy folder name): output/Phase 3B March13-14 Final Output"
    )
    assert (
        _path_caption_for("output/Phase 3C DWH Final Output")
        == "Stored export root (legacy folder name): output/Phase 3C DWH Final Output"
    )
    assert (
        _path_caption_for("output/phase4/CASE_MINDORO_RETRO_2023")
        == "Stored export root (legacy folder name): output/phase4/CASE_MINDORO_RETRO_2023"
    )
