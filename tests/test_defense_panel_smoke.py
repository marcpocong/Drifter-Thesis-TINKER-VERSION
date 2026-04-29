from __future__ import annotations

import os
import subprocess

import pytest

from scripts.defense_readiness_common import (
    PANEL_PS_PATH,
    START_PS_PATH,
    compose_command,
    detect_compose_mode,
    diff_snapshots,
    file_tree_snapshot,
    run_command,
    start_ps_command,
)


SMOKE_ENV = {
    "DEFENSE_SMOKE_TEST": "1",
    "INPUT_CACHE_POLICY": "reuse_if_valid",
}
COMPOSE_MODE = detect_compose_mode()
REQUIRE_DOCKER = os.environ.get("DEFENSE_REQUIRE_DOCKER") == "1"


@pytest.mark.parametrize(
    ("args", "required_tokens"),
    [
        (("-Help", "-NoPause"), ("Panel-safe default path", "Guardrails:")),
        (("-List", "-NoPause"), ("CURRENT LAUNCHER CATALOG", "b1_drifter_context_panel")),
        (("-ListRole", "primary_evidence", "-NoPause"), ("Primary evidence", "mindoro_phase3b_primary_public_validation")),
        (("-ListRole", "read_only_governance", "-NoPause"), ("Read-only governance", "figure_package_publication")),
        (("-Explain", "mindoro_phase3b_primary_public_validation", "-NoPause"), ("Claim boundary:", "Run kind:", "Output warning:")),
        (("-Explain", "phase1_mindoro_focus_provenance", "-NoPause"), ("Claim boundary:", "Run kind:", "Output warning:")),
        (("-Explain", "dwh_reportable_bundle", "-NoPause"), ("Claim boundary:", "Run kind:", "Output warning:")),
        (("-Explain", "b1_drifter_context_panel", "-NoPause"), ("Claim boundary:", "Run kind:", "Output warning:")),
    ],
)
def test_launcher_smoke_commands_return_readable_output(args: tuple[str, ...], required_tokens: tuple[str, ...]):
    result = run_command(start_ps_command(*args), timeout=180, env=SMOKE_ENV)
    assert result.returncode == 0, result.combined_output
    for token in required_tokens:
        assert token in result.combined_output


def test_panel_no_pause_does_not_hang_or_prompt():
    result = run_command(start_ps_command("-Panel", "-NoPause"), timeout=120, env=SMOKE_ENV)
    assert result.returncode == 0, result.combined_output
    assert "PANEL REVIEW MODE" in result.combined_output
    assert "Non-interactive preview mode (-NoPause)." in result.combined_output
    assert "View B1 drifter provenance/context" in result.combined_output


def test_panel_wrapper_no_pause_delegates_cleanly():
    result = run_command(
        [
            "powershell",
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(PANEL_PS_PATH),
            "-NoPause",
        ],
        timeout=120,
        env=SMOKE_ENV,
    )
    assert result.returncode == 0, result.combined_output
    assert "PANEL REVIEW MODE" in result.combined_output
    assert "Use .\\panel.ps1 or .\\start.ps1 -Panel for the interactive defense menu." in result.combined_output


def test_interactive_explain_accepts_visible_menu_number():
    process = subprocess.run(
        [
            "powershell",
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(START_PS_PATH),
        ],
        cwd=str(START_PS_PATH.parent),
        input="1\nX\n3\n\nQ\n",
        text=True,
        capture_output=True,
        timeout=120,
        check=False,
    )
    combined_output = (process.stdout or "") + (process.stderr or "")
    assert process.returncode == 0, combined_output
    assert "ENTRY PREVIEW" in combined_output
    assert "Entry ID: dwh_reportable_bundle" in combined_output


def test_interactive_explain_invalid_reference_stays_in_menu():
    process = subprocess.run(
        [
            "powershell",
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(START_PS_PATH),
        ],
        cwd=str(START_PS_PATH.parent),
        input="1\nX\n999\nQ\n",
        text=True,
        capture_output=True,
        timeout=120,
        check=False,
    )
    combined_output = (process.stdout or "") + (process.stderr or "")
    assert process.returncode == 0, combined_output
    assert "[ERROR] Unknown selection '999'." in combined_output
    assert "Goodbye." in combined_output


def test_expensive_entry_stops_at_noninteractive_confirmation_guard():
    result = run_command(start_ps_command("-Entry", "phase1_mindoro_focus_provenance"), timeout=120, env=SMOKE_ENV)
    assert result.returncode != 0
    assert "requires interactive confirmation" in result.combined_output


@pytest.mark.skipif(not REQUIRE_DOCKER or not COMPOSE_MODE, reason="requires Docker-backed smoke run")
def test_read_only_entries_do_not_mutate_scientific_roots():
    scientific_roots = (
        "config",
        "data_processed/grids",
        "output/CASE_MINDORO_RETRO_2023",
        "output/CASE_DWH_RETRO_2010_72H",
        "output/phase1_mindoro_focus_pre_spill_2016_2023",
    )
    before = file_tree_snapshot(scientific_roots)

    for entry_id in (
        "b1_drifter_context_panel",
        "final_validation_package",
        "phase5_sync",
        "figure_package_publication",
        "trajectory_gallery_panel",
    ):
        result = run_command(start_ps_command("-Entry", entry_id, "-NoPause"), timeout=900, env=SMOKE_ENV)
        assert result.returncode == 0, result.combined_output

    after = file_tree_snapshot(scientific_roots)
    assert diff_snapshots(before, after) == []


@pytest.mark.skipif(not REQUIRE_DOCKER or not COMPOSE_MODE, reason="requires Docker-backed smoke run")
def test_panel_review_check_is_regeneratable_read_only():
    command = compose_command(COMPOSE_MODE, "exec", "-T", "pipeline", "python", "src/services/panel_review_check.py")
    result = run_command(command, timeout=240, env=SMOKE_ENV)
    assert result.returncode == 0, result.combined_output
    assert "Panel review verification complete." in result.combined_output
