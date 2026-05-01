from __future__ import annotations

from launcher_ps_helpers import REPO_ROOT, assert_clean_launcher_exit, assert_no_docker_execution, run_launcher


def test_start_validate_matrix_runs_without_docker(tmp_path):
    result = run_launcher(["-ValidateMatrix", "-NoPause"], tmp_path, timeout=60)

    assert_clean_launcher_exit(result)
    assert_no_docker_execution(result)
    assert "Launcher matrix validation" in result.output
    assert "OVERALL: PASS" in result.output
    assert "Audit report written" not in result.output
    assert "src.utils.validate_launcher_matrix --no-write" in (REPO_ROOT / "start.ps1").read_text(encoding="utf-8")
