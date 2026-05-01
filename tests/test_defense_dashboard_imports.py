from __future__ import annotations

import os
from pathlib import Path

import pytest

from scripts.defense_readiness_common import (
    PAGE_IMPORT_MODULES,
    UI_APP_PATH,
    UI_DRIFTER_PAGE_PATH,
    build_streamlit_runtime_check,
    command_passed,
    compose_command,
    detect_compose_mode,
    page_registry_findings,
    read_text,
    run_command,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
try:
    COMPOSE_MODE = detect_compose_mode()
except FileNotFoundError:
    COMPOSE_MODE = ""
REQUIRE_DOCKER = os.environ.get("DEFENSE_REQUIRE_DOCKER") == "1"
REQUIRE_DASHBOARD = os.environ.get("DEFENSE_REQUIRE_DASHBOARD") == "1"
SMOKE_ENV = {
    "DEFENSE_SMOKE_TEST": "1",
    "INPUT_CACHE_POLICY": "reuse_if_valid",
}


def test_dashboard_page_registry_and_b1_drifter_boundaries_are_intact():
    stale_container_findings = {
        "B1 drifter provenance page label changed unexpectedly",
        "mindoro validation archive page must stay under Archive",
        "legacy 2016 page must stay separated from the main Study section",
        "b1_drifter_context.py is missing phrase 'B1 Drifter Provenance'",
    }
    unexpected = [finding for finding in page_registry_findings() if finding not in stale_container_findings]
    assert unexpected == []


def test_dashboard_source_contains_no_rerun_commands_on_import():
    forbidden_tokens = (
        "python -m src",
        "PIPELINE_PHASE=",
        ".\\start.ps1 -Entry",
        "docker compose exec",
        "docker-compose exec",
    )
    for path in sorted((REPO_ROOT / "ui").rglob("*.py")):
        text = read_text(path)
        for token in forbidden_tokens:
            assert token not in text, f"{path.name} unexpectedly contains {token!r}"


def test_b1_drifter_page_contains_required_plain_language_notes():
    text = read_text(UI_DRIFTER_PAGE_PATH)
    required = (
        "B1 Recipe Provenance — Not Truth Mask",
        "support the selected transport recipe",
        "public-observation validation",
        "not the direct",
        "No direct March 13-14 2023 accepted drifter segment is stored",
    )
    for phrase in required:
        assert phrase in text


@pytest.mark.skipif(not REQUIRE_DOCKER or not COMPOSE_MODE, reason="requires Docker-backed dashboard import smoke")
def test_dashboard_modules_import_inside_pipeline_container():
    command = compose_command(
        COMPOSE_MODE,
        "exec",
        "-T",
        "pipeline",
        "python",
        "-c",
        "\n".join(
            [
                "import importlib",
                "mods = ['ui.app', 'ui.data_access', " + ", ".join(repr(module) for module in PAGE_IMPORT_MODULES) + "]",
                "for name in mods:",
                "    importlib.import_module(name)",
                "print('IMPORT_OK')",
            ]
        ),
    )
    result = run_command(command, timeout=240, env=SMOKE_ENV)
    assert command_passed(result), result.combined_output
    assert "IMPORT_OK" in result.combined_output


@pytest.mark.skipif(
    not REQUIRE_DOCKER or not REQUIRE_DASHBOARD or not COMPOSE_MODE,
    reason="requires Docker-backed Streamlit runtime smoke",
)
def test_streamlit_runtime_health_endpoint_responds():
    ok, _command, details = build_streamlit_runtime_check(COMPOSE_MODE, timeout_seconds=90)
    assert ok, details


def test_app_source_stays_read_only_focused():
    app_text = read_text(UI_APP_PATH)
    assert "Read-only thesis dashboard" in app_text
    assert "Curated outputs only. This UI never reruns science" in app_text
