from __future__ import annotations

import json

import pytest

from launcher_ps_helpers import assert_clean_launcher_exit, assert_no_docker_execution, run_launcher, run_panel


B1_ENTRY_ID = "mindoro_phase3b_primary_public_validation"
B1_BOUNDARY = "Only main Philippine public-observation validation claim"


@pytest.mark.parametrize(
    ("args", "expected_text"),
    [
        (
            ["-Help", "-NoPause"],
            [
                "LAUNCHER HELP",
                B1_ENTRY_ID,
                "B1 is the only main Philippine public-observation validation claim",
                "independent March 13 and March 14 NOAA public-observation products",
            ],
        ),
        (
            ["-List", "-NoPause"],
            [
                "CURRENT LAUNCHER CATALOG",
                "Mindoro B1 primary public-validation rerun",
                B1_BOUNDARY,
                "independent NOAA-published day-specific observation products",
            ],
        ),
        (
            ["-ListRole", "primary_evidence", "-NoPause"],
            [
                "Filtered thesis role: Primary evidence",
                "Mindoro B1 primary public-validation rerun",
                B1_BOUNDARY,
                "independent NOAA-published day-specific observation products",
            ],
        ),
        (
            ["-ListRole", "read_only_governance", "-NoPause"],
            [
                "Filtered thesis role: Read-only governance",
                "Publication-grade figure package",
                "No scientific rerun should occur",
            ],
        ),
        (
            ["-Explain", B1_ENTRY_ID, "-NoPause"],
            [
                "ENTRY PREVIEW",
                f"Entry ID: {B1_ENTRY_ID}",
                f"Claim boundary: {B1_BOUNDARY}",
                "independent NOAA-published day-specific observation products",
            ],
        ),
    ],
)
def test_help_list_role_and_explain_smoke_without_docker(tmp_path, args, expected_text):
    result = run_launcher(args, tmp_path=tmp_path)

    assert_clean_launcher_exit(result)
    assert_no_docker_execution(result)
    for text in expected_text:
        assert text in result.output


def test_direct_entry_dry_run_prints_plan_without_workflow_execution(tmp_path):
    result = run_launcher(["-Entry", B1_ENTRY_ID, "-DryRun", "-NoPause"], tmp_path=tmp_path)

    assert_clean_launcher_exit(result)
    assert_no_docker_execution(result)
    assert f"Entry ID: {B1_ENTRY_ID}" in result.output
    assert "Steps that would run:" in result.output
    assert "Exact commands that would run:" in result.output
    assert "Environment variables that will be passed:" in result.output
    assert "Exact prompt-free docker compose command sequence:" in result.output
    assert "Expected output directories:" in result.output
    assert B1_BOUNDARY in result.output
    assert "Dry run only. No Docker commands were executed" in result.output
    assert "No workflow was executed." in result.output


@pytest.mark.parametrize(
    "entry_id",
    [
        "phase1_mindoro_focus_provenance",
        "mindoro_phase3b_primary_public_validation",
        "dwh_reportable_bundle",
        "figure_package_publication",
        "b1_drifter_context_panel",
    ],
)
def test_required_entries_dry_run_without_docker(tmp_path, entry_id):
    result = run_launcher(["-Entry", entry_id, "-DryRun", "-NoPause"], tmp_path=tmp_path)

    assert_clean_launcher_exit(result)
    assert_no_docker_execution(result)
    assert f"Entry ID: {entry_id}" in result.output
    assert "Canonical entry ID:" in result.output
    assert "Exact commands that would run:" in result.output
    assert "No workflow was executed." in result.output


def test_hidden_alias_dry_run_resolves_to_canonical_entry(tmp_path):
    result = run_launcher(["-Entry", "mindoro_march13_14_noaa_reinit_stress_test", "-DryRun", "-NoPause"], tmp_path=tmp_path)

    assert_clean_launcher_exit(result)
    assert_no_docker_execution(result)
    assert "Requested alias: mindoro_march13_14_noaa_reinit_stress_test" in result.output
    assert "Canonical entry ID: mindoro_phase3b_primary_public_validation" in result.output
    assert "phase3b_extended_public_scored_march13_14_reinit_pygnome_comparison" not in result.output


def test_phase1_hidden_alias_dry_run_resolves_to_canonical_entry(tmp_path):
    result = run_launcher(["-Entry", "phase1_mindoro_focus_pre_spill_experiment", "-DryRun", "-NoPause"], tmp_path=tmp_path)

    assert_clean_launcher_exit(result)
    assert_no_docker_execution(result)
    assert "Requested alias: phase1_mindoro_focus_pre_spill_experiment" in result.output
    assert "Alias resolution: requested ID resolves to the canonical entry metadata shown below." in result.output
    assert "Canonical entry ID: phase1_mindoro_focus_provenance" in result.output


def test_noninteractive_no_pause_without_input_prints_summary_and_exits(tmp_path):
    result = run_launcher(["-NoPause"], tmp_path=tmp_path, stdin="", timeout=10)

    assert_clean_launcher_exit(result)
    assert_no_docker_execution(result)
    assert "LAUNCHER NON-INTERACTIVE SUMMARY" in result.output
    assert "will not wait for menu input" in result.output
    assert "No workflow was executed." in result.output


def test_explain_export_plan_writes_plan_without_docker(tmp_path):
    plan_root = tmp_path / "plans"
    result = run_launcher(
        ["-Explain", B1_ENTRY_ID, "-ExportPlan", "-NoPause"],
        tmp_path=tmp_path,
        extra_env={"LAUNCHER_PLAN_OUTPUT_ROOT": str(plan_root)},
    )

    assert_clean_launcher_exit(result)
    assert_no_docker_execution(result)
    assert "Run plan exported without executing science:" in result.output

    json_path = plan_root / "launcher_plans" / f"{B1_ENTRY_ID}.json"
    markdown_path = plan_root / "launcher_plans" / f"{B1_ENTRY_ID}.md"
    assert json_path.exists()
    assert markdown_path.exists()
    plan = json.loads(json_path.read_text(encoding="utf-8-sig"))
    assert plan["canonical_entry_id"] == B1_ENTRY_ID
    assert plan["no_workflow_executed"] is True
    assert "prompt_free_steps" in plan


def test_direct_entry_blank_confirmation_cancels_cleanly_without_docker(tmp_path):
    result = run_launcher(["-Entry", B1_ENTRY_ID], tmp_path=tmp_path, stdin="\n\n")

    assert_clean_launcher_exit(result)
    assert_no_docker_execution(result)
    assert "Cancelled. No workflow was executed." in result.output


def test_interactive_invalid_choice_then_quit_is_clean(tmp_path):
    result = run_launcher(["-NoPause"], tmp_path=tmp_path, stdin="not-a-menu-choice\nQ\n")

    assert_clean_launcher_exit(result)
    assert_no_docker_execution(result)
    assert "Invalid option 'not-a-menu-choice'. Allowed options:" in result.output
    assert "Goodbye." in result.output


def test_panel_wrapper_forwards_arguments_from_any_cwd(tmp_path):
    result = run_panel(["-NoPause"], tmp_path=tmp_path, cwd=tmp_path)

    assert_clean_launcher_exit(result)
    assert_no_docker_execution(result)
    assert "PANEL REVIEW MODE" in result.output
    assert "View data sources and provenance registry" in result.output


def test_dashboard_dry_run_prints_plan_without_docker(tmp_path):
    result = run_launcher(["-Dashboard", "-DryRun", "-NoPause"], tmp_path=tmp_path)

    assert_clean_launcher_exit(result)
    assert_no_docker_execution(result)
    assert "READ-ONLY DASHBOARD DRY RUN" in result.output
    assert "docker compose up -d pipeline" in result.output
    assert "python -m streamlit run ui/app.py --server.address 0.0.0.0 --server.port 8501" in result.output
    assert "http://localhost:8501" in result.output
    assert "No Docker commands were executed" in result.output


def test_panel_dashboard_flag_routes_to_dashboard_without_docker_in_dry_run(tmp_path):
    result = run_launcher(["-Panel", "-Dashboard", "-DryRun", "-NoPause"], tmp_path=tmp_path)

    assert_clean_launcher_exit(result)
    assert_no_docker_execution(result)
    assert "READ-ONLY DASHBOARD DRY RUN" in result.output
    assert "docker compose up -d pipeline" in result.output


def test_dashboard_docker_unavailable_message_is_actionable(tmp_path):
    result = run_launcher(["-Dashboard", "-NoPause"], tmp_path=tmp_path)

    assert_clean_launcher_exit(result)
    assert "[ERROR]" not in result.output
    assert (
        "Docker is not available. Install/start Docker Desktop, then rerun .\\panel.ps1 "
        "or run the manual Streamlit command in docs/UI_GUIDE.md."
    ) in result.output


def test_dashboard_copies_env_example_before_fake_docker_launch(tmp_path):
    env_root = tmp_path / "env-root"
    env_root.mkdir()
    (env_root / ".env.example").write_text("LOCAL_REVIEW=1\n", encoding="utf-8")

    result = run_launcher(
        ["-Dashboard", "-NoPause"],
        tmp_path=tmp_path,
        timeout=20,
        extra_env={
            "FAKE_DOCKER_MODE": "launch",
            "LAUNCHER_ENV_ROOT": str(env_root),
            "LAUNCHER_DISABLE_BROWSER_OPEN": "1",
            "LAUNCHER_DASHBOARD_HEALTH_WAIT_SECONDS": "0",
        },
    )

    assert_clean_launcher_exit(result)
    assert (env_root / ".env").read_text(encoding="utf-8") == "LOCAL_REVIEW=1\n"
    docker_output = result.docker_log.read_text(encoding="utf-8")
    assert "compose up -d pipeline" in docker_output
    assert "compose exec -T -d pipeline python -m streamlit run ui/app.py" in docker_output
    assert "Created .env from .env.example as the safe non-interactive default" in result.output


def test_dashboard_reuses_existing_port_without_duplicate_streamlit_launch(tmp_path):
    env_root = tmp_path / "env-root"
    env_root.mkdir()
    (env_root / ".env").write_text("LOCAL_REVIEW=1\n", encoding="utf-8")

    result = run_launcher(
        ["-Dashboard", "-NoPause"],
        tmp_path=tmp_path,
        timeout=20,
        extra_env={
            "FAKE_DOCKER_MODE": "duplicate",
            "LAUNCHER_ENV_ROOT": str(env_root),
            "LAUNCHER_DISABLE_BROWSER_OPEN": "1",
            "LAUNCHER_DASHBOARD_HEALTH_WAIT_SECONDS": "0",
        },
    )

    assert_clean_launcher_exit(result)
    docker_output = result.docker_log.read_text(encoding="utf-8")
    assert "compose up -d pipeline" in docker_output
    assert "streamlit run ui/app.py" not in docker_output
    assert "Read-only Streamlit UI is already running." in result.output


def test_interactive_role_group_back_returns_to_launcher_home_then_quits(tmp_path):
    result = run_launcher(["-NoPause"], tmp_path=tmp_path, stdin="1\nB\nQ\n")

    assert_clean_launcher_exit(result)
    assert_no_docker_execution(result)
    assert "Main thesis evidence / reportable" in result.output
    assert result.output.count("Choose a role-based path:") >= 2
    assert "Goodbye." in result.output


@pytest.mark.parametrize(
    ("stdin", "expected"),
    [
        ("S\nB\nQ\n", "Search mode for all visible launcher entries"),
        ("S\nC\nQ\n", "Cancelled. No workflow was executed."),
        ("S\nQ\n", "Goodbye."),
        ("5\nS\nfigure\n1\nB\nQ\n", "Search results for 'figure':"),
    ],
)
def test_interactive_search_back_cancel_quit_paths(tmp_path, stdin, expected):
    result = run_launcher(["-NoPause"], tmp_path=tmp_path, stdin=stdin)

    assert_clean_launcher_exit(result)
    assert_no_docker_execution(result)
    assert expected in result.output


def test_interactive_entry_cancel_returns_to_menu_then_quits_without_docker(tmp_path):
    result = run_launcher(
        ["-NoPause"],
        tmp_path=tmp_path,
        stdin=f"1\n{B1_ENTRY_ID}\nC\nQ\n",
    )

    assert_clean_launcher_exit(result)
    assert_no_docker_execution(result)
    assert f"Entry ID: {B1_ENTRY_ID}" in result.output
    assert "Cancelled. No workflow was executed." in result.output
    assert result.output.count("Main thesis evidence / reportable") >= 2
    assert "Goodbye." in result.output
