from __future__ import annotations

import json
import re
from collections import defaultdict
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
START_PS1 = REPO_ROOT / "start.ps1"
COMMAND_MATRIX = REPO_ROOT / "docs" / "COMMAND_MATRIX.md"
LAUNCHER_MATRIX = REPO_ROOT / "config" / "launcher_matrix.json"


def _launcher_matrix() -> dict:
    return json.loads(LAUNCHER_MATRIX.read_text(encoding="utf-8"))


def _entries() -> list[dict]:
    return _launcher_matrix()["entries"]


def _entry_map() -> dict[str, dict]:
    return {entry["entry_id"]: entry for entry in _entries()}


def _combined_entry_text(entry: dict) -> str:
    fields = ("entry_id", "label", "description", "notes", "claim_boundary", "draft_section")
    return " ".join(str(entry.get(field, "")) for field in fields).lower()


def _extract_documented_panel_options() -> dict[str, str]:
    text = COMMAND_MATRIX.read_text(encoding="utf-8")
    in_section = False
    options: dict[str, str] = {}
    for line in text.splitlines():
        if line.strip() == "## Panel Menu Options":
            in_section = True
            continue
        if in_section and line.startswith("## "):
            break
        match = re.match(r"^\| `([^`]+)` \| ([^|]+) \|", line)
        if match:
            options[match.group(1)] = match.group(2).strip()
    return options


def _extract_visible_panel_options_from_start() -> dict[str, set[str]]:
    text = START_PS1.read_text(encoding="utf-8")
    match = re.search(r"function Show-PanelMenu \{(?P<body>.*?)(?=^function )", text, re.MULTILINE | re.DOTALL)
    assert match, "Could not find Show-PanelMenu in start.ps1"

    visible: dict[str, set[str]] = defaultdict(set)
    for option, label in re.findall(r'Write-Host "  ([0-9A-Z])\. ([^"]+)"', match.group("body")):
        normalized = re.sub(r"\s+\[[^\]]+\]$", "", label).strip()
        visible[option].add(normalized)
    return dict(visible)


def test_panel_menu_options_match_command_matrix():
    documented = _extract_documented_panel_options()
    visible = _extract_visible_panel_options_from_start()

    assert documented.keys() == visible.keys()
    for option, labels in visible.items():
        if option == "B":
            assert labels == {"Back", "Open launcher home"}
            assert documented[option] == "Back or launcher home"
        elif option == "C":
            assert labels == {"Cancel and return", "Cancel and open launcher home"}
            assert documented[option] == "Cancel and return"
        else:
            assert documented[option] in labels

    assert visible["7"] == {"View B1 drifter provenance/context"}
    assert documented["7"] == "View B1 drifter provenance/context"
    assert visible["8"] == {"View data sources and provenance registry"}
    assert documented["8"] == "View data sources and provenance registry"
    assert "b1_drifter_context_panel" in _entry_map()


def test_visible_entries_resolve_and_hidden_aliases_stay_out_of_visible_menu():
    entries = _entries()
    entry_map = _entry_map()
    role_groups = _launcher_matrix()["role_groups"]
    role_group_roles = {role for group in role_groups for role in group.get("thesis_roles", [])}

    assert len(entry_map) == len(entries), "launcher entry IDs must be unique"

    visible_ids = {entry["entry_id"] for entry in entries if not entry.get("menu_hidden")}
    hidden_ids = {entry["entry_id"] for entry in entries if entry.get("menu_hidden")}
    assert visible_ids.isdisjoint(hidden_ids)

    for entry in entries:
        assert entry["category_id"] in {category["category_id"] for category in _launcher_matrix()["categories"]}
        assert entry["thesis_role"] in role_group_roles
        assert entry["steps"], entry["entry_id"]

    for entry in entries:
        alias_of = entry.get("alias_of")
        if not alias_of:
            continue
        assert alias_of in entry_map
        assert entry.get("menu_hidden") is True, f"{entry['entry_id']} alias must be hidden"
        assert entry["entry_id"] not in visible_ids


def test_alias_resolution_has_existing_canonical_targets_without_cycles():
    entry_map = _entry_map()

    for entry_id, entry in entry_map.items():
        seen: set[str] = set()
        current = entry
        while current.get("alias_of"):
            assert current["entry_id"] not in seen, f"Alias cycle starts at {entry_id}"
            seen.add(current["entry_id"])
            target_id = current["alias_of"]
            assert target_id in entry_map
            current = entry_map[target_id]
        assert current["entry_id"] in entry_map


def test_read_only_governance_entries_stay_safe_default_or_read_only_like():
    for entry in _entries():
        if entry["thesis_role"] != "read_only_governance":
            continue
        assert entry["safe_default"] is True, entry["entry_id"]
        assert entry["run_kind"] in {"read_only", "packaging_only", "audit"}, entry["entry_id"]


def test_comparator_dwh_and_b1_guardrail_wording_is_present():
    for entry in _entries():
        combined = _combined_entry_text(entry)
        if entry["run_kind"] == "comparator_rerun" or entry["thesis_role"] == "comparator_support":
            assert "comparator-only" in combined or "comparator only" in combined, entry["entry_id"]
        if "pygnome" in combined:
            assert "comparator-only" in combined or "comparator only" in combined, entry["entry_id"]
        if entry["workflow_mode"] == "dwh_retro_2010" or entry["entry_id"].startswith("dwh_"):
            assert "external transfer" in combined, entry["entry_id"]
        if entry["entry_id"] == "mindoro_phase3b_primary_public_validation":
            assert "only main philippine public-observation validation claim" in combined
            assert "independent noaa-published day-specific observation products" in combined
        if entry["entry_id"] == "mindoro_march13_14_noaa_reinit_stress_test":
            assert entry.get("alias_of") == "mindoro_phase3b_primary_public_validation"
        if entry["entry_id"] == "b1_drifter_context_panel":
            assert "not the direct" in combined


def test_start_ps1_uses_compose_helper_for_workflow_invocations():
    text = START_PS1.read_text(encoding="utf-8")
    assert "function Invoke-ComposeCommand" in text

    allowed_direct_functions = {"Resolve-ComposeMode", "Invoke-ComposeCommand"}
    current_function = ""
    offenders: list[str] = []
    for line_number, line in enumerate(text.splitlines(), start=1):
        function_match = re.match(r"^function\s+([A-Za-z0-9-]+)\s*\{", line)
        if function_match:
            current_function = function_match.group(1)

        if re.match(r"^\s*&\s+(docker-compose|docker)\b", line):
            if current_function not in allowed_direct_functions:
                offenders.append(f"{line_number}: {line.strip()}")

    assert offenders == []
