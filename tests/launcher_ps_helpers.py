from __future__ import annotations

import os
import shutil
import stat
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
START_PS1 = REPO_ROOT / "start.ps1"
PANEL_PS1 = REPO_ROOT / "panel.ps1"


@dataclass(frozen=True)
class LauncherResult:
    args: tuple[str, ...]
    returncode: int
    stdout: str
    stderr: str
    docker_log: Path

    @property
    def output(self) -> str:
        return f"{self.stdout}\n{self.stderr}"


def require_powershell() -> str:
    pwsh = shutil.which("pwsh")
    if pwsh:
        return pwsh

    powershell = shutil.which("powershell") or shutil.which("powershell.exe")
    if powershell:
        return powershell

    pytest.skip("PowerShell launcher tests require pwsh or Windows PowerShell; neither is installed.")


def _powershell_base_command(executable: str) -> list[str]:
    command = [executable, "-NoProfile"]
    executable_name = Path(executable).name.lower()
    if executable_name in {"powershell", "powershell.exe"}:
        command += ["-ExecutionPolicy", "Bypass"]
    return command


def _write_fake_docker_commands(fake_bin: Path, docker_log: Path) -> None:
    fake_bin.mkdir(parents=True, exist_ok=True)

    if os.name == "nt":
        ps_script = (
            "$ErrorActionPreference = 'Stop'\r\n"
            "$argText = $args -join ' '\r\n"
            "Add-Content -LiteralPath $env:DOCKER_INVOCATION_LOG -Value (\"{0} {1}\" -f [System.IO.Path]::GetFileName($MyInvocation.InvocationName), $argText)\r\n"
            "$mode = $env:FAKE_DOCKER_MODE\r\n"
            "if ($mode -ieq 'success' -or $mode -ieq 'duplicate') { exit 0 }\r\n"
            "if ($mode -ieq 'launch') {\r\n"
            "  if ($argText.Contains('exec -T pipeline python -c') -or $argText.Contains('exec -T pipeline sh -lc')) { exit 1 }\r\n"
            "  exit 0\r\n"
            "}\r\n"
            "exit 99\r\n"
        )
        ps_path = fake_bin / "fake_docker.ps1"
        ps_path.write_text(ps_script, encoding="utf-8")
        script = (
            "@echo off\r\n"
            "powershell.exe -NoProfile -ExecutionPolicy Bypass -File \"%~dp0fake_docker.ps1\" %*\r\n"
            "exit /b %ERRORLEVEL%\r\n"
        )
        for name in ("docker.cmd", "docker-compose.cmd"):
            (fake_bin / name).write_text(script, encoding="utf-8")
        return

    script = (
        "#!/usr/bin/env sh\n"
        "printf '%s %s\\n' \"$(basename \"$0\")\" \"$*\" >> \"$DOCKER_INVOCATION_LOG\"\n"
        "mode=${FAKE_DOCKER_MODE:-fail}\n"
        "args=$*\n"
        "case \"$mode\" in\n"
        "  success|duplicate)\n"
        "    exit 0\n"
        "    ;;\n"
        "  launch)\n"
        "    case \"$args\" in\n"
        "      *\"exec -T pipeline python -c\"*|*\"exec -T pipeline sh -lc\"*) exit 1 ;;\n"
        "      *) exit 0 ;;\n"
        "    esac\n"
        "    ;;\n"
        "esac\n"
        "exit 99\n"
    )
    for name in ("docker", "docker-compose"):
        path = fake_bin / name
        path.write_text(script, encoding="utf-8")
        path.chmod(path.stat().st_mode | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH)


def run_launcher(
    args: list[str],
    tmp_path: Path,
    stdin: str = "",
    timeout: int = 30,
    extra_env: dict[str, str] | None = None,
    script_path: Path = START_PS1,
    cwd: Path = REPO_ROOT,
) -> LauncherResult:
    powershell = require_powershell()
    fake_bin = tmp_path / "fake-bin"
    docker_log = tmp_path / "docker_invocations.log"
    _write_fake_docker_commands(fake_bin=fake_bin, docker_log=docker_log)

    env = os.environ.copy()
    env["PATH"] = f"{fake_bin}{os.pathsep}{env.get('PATH', '')}"
    env["DOCKER_INVOCATION_LOG"] = str(docker_log)
    env["PYTHONIOENCODING"] = "utf-8"
    if stdin:
        env["LAUNCHER_SCRIPTED_STDIN"] = "1"
    if extra_env:
        env.update(extra_env)

    command = [*_powershell_base_command(powershell), "-File", str(script_path), *args]
    if stdin:
        process = subprocess.Popen(
            command,
            cwd=cwd,
            text=True,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
        )
        try:
            assert process.stdin is not None
            for chunk in stdin.splitlines(keepends=True):
                process.stdin.write(chunk)
                process.stdin.flush()
                time.sleep(0.02)
            process.stdin.close()
            stdout, stderr = process.communicate(timeout=timeout)
            returncode = process.returncode
        except subprocess.TimeoutExpired:
            process.kill()
            stdout, stderr = process.communicate()
            returncode = -1
            stderr = (stderr or "") + "\n[timeout]"
    else:
        completed = subprocess.run(
            command,
            cwd=cwd,
            input=stdin,
            text=True,
            capture_output=True,
            env=env,
            timeout=timeout,
        )
        returncode = completed.returncode
        stdout = completed.stdout
        stderr = completed.stderr
    return LauncherResult(
        args=tuple(args),
        returncode=returncode,
        stdout=stdout,
        stderr=stderr,
        docker_log=docker_log,
    )


def run_panel(
    args: list[str],
    tmp_path: Path,
    stdin: str = "",
    timeout: int = 30,
    extra_env: dict[str, str] | None = None,
    cwd: Path | None = None,
) -> LauncherResult:
    return run_launcher(
        args=args,
        tmp_path=tmp_path,
        stdin=stdin,
        timeout=timeout,
        extra_env=extra_env,
        script_path=PANEL_PS1,
        cwd=cwd or tmp_path,
    )


def assert_clean_launcher_exit(result: LauncherResult, expected_returncode: int = 0) -> None:
    assert result.returncode == expected_returncode, result.output
    assert "[ERROR]" not in result.output


def assert_no_docker_execution(result: LauncherResult) -> None:
    docker_output = result.docker_log.read_text(encoding="utf-8") if result.docker_log.exists() else ""
    assert docker_output == "", f"Docker command was invoked unexpectedly:\n{docker_output}\n{result.output}"
    assert "Starting Docker containers" not in result.output
    assert "Launching read-only Streamlit UI" not in result.output
