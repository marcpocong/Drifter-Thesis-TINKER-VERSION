from __future__ import annotations

import ast
import csv
import hashlib
import importlib
import json
import os
import platform
import re
import shutil
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


PASS = "PASS"
WARN = "WARN"
FAIL = "FAIL"

REPO_ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = REPO_ROOT / "output" / "defense_readiness"
LAUNCHER_MATRIX_PATH = REPO_ROOT / "config" / "launcher_matrix.json"
PANEL_REVIEW_JSON_PATH = REPO_ROOT / "output" / "panel_review_check" / "panel_results_match_check.json"
PANEL_REVIEW_MANIFEST_PATH = REPO_ROOT / "output" / "panel_review_check" / "panel_review_manifest.json"
PANEL_DRIFTER_MANIFEST_PATH = REPO_ROOT / "output" / "panel_drifter_context" / "b1_drifter_context_manifest.json"
PUBLICATION_REGISTRY_PATH = REPO_ROOT / "output" / "figure_package_publication" / "publication_figure_registry.csv"
PUBLICATION_MANIFEST_PATH = REPO_ROOT / "output" / "figure_package_publication" / "publication_figure_manifest.json"
PHASE1_MANIFEST_PATH = REPO_ROOT / "output" / "phase1_mindoro_focus_pre_spill_2016_2023" / "phase1_production_manifest.json"
PHASE1_RANKING_PATH = REPO_ROOT / "output" / "phase1_mindoro_focus_pre_spill_2016_2023" / "phase1_recipe_ranking.csv"
PHASE1_ACCEPTED_PATH = REPO_ROOT / "output" / "phase1_mindoro_focus_pre_spill_2016_2023" / "phase1_accepted_segment_registry.csv"
PHASE1_SUBSET_PATH = REPO_ROOT / "output" / "phase1_mindoro_focus_pre_spill_2016_2023" / "phase1_ranking_subset_registry.csv"
PHASE1_REPORT_PATH = REPO_ROOT / "output" / "phase1_mindoro_focus_pre_spill_2016_2023" / "phase1_ranking_subset_report.md"
B1_SUMMARY_PATH = REPO_ROOT / "output" / "Phase 3B March13-14 Final Output" / "summary" / "opendrift_primary" / "march13_14_reinit_summary.csv"
B1_BRANCH_SURVIVAL_PATH = REPO_ROOT / "output" / "CASE_MINDORO_RETRO_2023" / "phase3b_extended_public_scored_march13_14_reinit" / "march13_14_reinit_branch_survival_summary.csv"
TRACK_A_SUMMARY_PATH = REPO_ROOT / "output" / "Phase 3B March13-14 Final Output" / "summary" / "comparator_pygnome" / "march13_14_reinit_crossmodel_summary.csv"
DWH_SCORECARD_PATH = REPO_ROOT / "output" / "Phase 3C DWH Final Output" / "summary" / "comparison" / "phase3c_main_scorecard.csv"
PHASE4_BUDGET_PATH = REPO_ROOT / "output" / "phase4" / "CASE_MINDORO_RETRO_2023" / "phase4_oil_budget_summary.csv"
PHASE4_ARRIVAL_PATH = REPO_ROOT / "output" / "phase4" / "CASE_MINDORO_RETRO_2023" / "phase4_shoreline_arrival.csv"
DOC_PATHS = (
    REPO_ROOT / "README.md",
    REPO_ROOT / "PANEL_QUICK_START.md",
    REPO_ROOT / "docs" / "PANEL_REVIEW_GUIDE.md",
    REPO_ROOT / "docs" / "COMMAND_MATRIX.md",
    REPO_ROOT / "docs" / "PAPER_OUTPUT_REGISTRY.md",
)
UI_APP_PATH = REPO_ROOT / "ui" / "app.py"
UI_PAGES_INIT_PATH = REPO_ROOT / "ui" / "pages" / "__init__.py"
UI_DRIFTER_PAGE_PATH = REPO_ROOT / "ui" / "pages" / "b1_drifter_context.py"
START_PS_PATH = REPO_ROOT / "start.ps1"
PANEL_PS_PATH = REPO_ROOT / "panel.ps1"
DOCKER_COMPOSE_PATH = REPO_ROOT / "docker-compose.yml"
ENV_EXAMPLE_PATH = REPO_ROOT / ".env.example"
ENV_PATH = REPO_ROOT / ".env"

ABSOLUTE_LOCAL_PATH_PATTERN = re.compile(r"(?i)(?:[a-z]:\\users\\|/[a-z]:/users/)")
READ_ONLY_FORBIDDEN_PHASES = {
    "phase1_production_rerun",
    "1_2",
    "phase3b_extended_public_scored_march13_14_reinit",
    "phase3c_external_case_run",
    "phase4_oiltype_and_shoreline",
    "benchmark",
}
ALLOWED_STEP_SERVICES = {"pipeline", "gnome"}
ALLOWED_RUN_KINDS = {"read_only", "packaging_only", "scientific_rerun", "comparator_rerun", "archive_rerun"}
ALLOWED_THESIS_ROLES = {
    "primary_evidence",
    "support_context",
    "comparator_support",
    "archive_provenance",
    "legacy_support",
    "read_only_governance",
}
CRITICAL_ARTIFACT_PATTERNS: dict[str, list[str]] = {
    "b1_primary_board_or_scorecard": [
        "output/figure_package_publication/**/*mindoro_primary_validation_board*.png",
        "output/Phase 3B March13-14 Final Output/summary/opendrift_primary/march13_14_reinit_summary.csv",
    ],
    "b1_stored_metrics": [
        "output/CASE_MINDORO_RETRO_2023/phase3b_extended_public_scored_march13_14_reinit/march13_14_reinit_summary.csv",
        "output/Phase 3B March13-14 Final Output/summary/opendrift_primary/march13_14_reinit_summary.csv",
    ],
    "track_a_board_or_scorecard": [
        "output/figure_package_publication/**/*mindoro_crossmodel_board*.png",
        "output/Phase 3B March13-14 Final Output/summary/comparator_pygnome/march13_14_reinit_crossmodel_summary.csv",
    ],
    "dwh_board_or_scorecard": [
        "output/Phase 3C DWH Final Output/summary/comparison/phase3c_main_scorecard.csv",
        "output/figure_package_publication/**/*dwh*board*.png",
    ],
    "phase1_focused_artifact": [
        "output/panel_drifter_context/b1_drifter_context_map.png",
        "output/phase1_mindoro_focus_pre_spill_2016_2023/phase1_recipe_ranking.csv",
        "output/phase1_mindoro_focus_pre_spill_2016_2023/phase1_ranking_subset_report.md",
    ],
    "publication_registry_or_manifest": [
        "output/figure_package_publication/publication_figure_registry.csv",
        "output/figure_package_publication/publication_figure_manifest.json",
    ],
    "panel_review_output": [
        "output/panel_review_check/panel_results_match_check.json",
        "output/panel_review_check/panel_review_manifest.json",
    ],
    "b1_drifter_provenance_manifest_or_loader": [
        "output/panel_drifter_context/b1_drifter_context_manifest.json",
        "ui/pages/b1_drifter_context.py",
    ],
}
ALLOWED_WRITE_ROOTS = {
    "README.md",
    "docs",
    "logs",
    "output/defense_readiness",
    "output/panel_review_check",
    "output/panel_drifter_context",
    "output/final_validation_package",
    "output/final_reproducibility_package",
    "output/figure_package_publication",
    "output/Phase 3B March13-14 Final Output",
    "output/Phase 3C DWH Final Output",
    "output/trajectory_gallery_panel",
}
SCIENTIFIC_GUARD_ROOTS = (
    "config",
    "data_processed/grids",
    "output/CASE_MINDORO_RETRO_2023",
    "output/CASE_DWH_RETRO_2010_72H",
    "output/phase1_mindoro_focus_pre_spill_2016_2023",
)
PAGE_IMPORT_MODULES = (
    "ui.app",
    "ui.data_access",
    "ui.pages.home",
    "ui.pages.phase1_recipe_selection",
    "ui.pages.b1_drifter_context",
    "ui.pages.mindoro_validation",
    "ui.pages.cross_model_comparison",
    "ui.pages.mindoro_validation_archive",
    "ui.pages.dwh_transfer_validation",
    "ui.pages.phase4_oiltype_and_shoreline",
    "ui.pages.legacy_2016_support",
    "ui.pages.artifacts_logs",
)
DEFAULT_PIPELINE_IMPORT_MODULES = (
    "streamlit",
    "pandas",
    "numpy",
    "geopandas",
    "rasterio",
    "shapely",
    "xarray",
    "yaml",
    "matplotlib",
)
GNOME_IMPORT_MODULES = (
    "gnome",
    "py_gnome",
    "numpy",
    "pandas",
)


@dataclass
class CheckResult:
    check_id: str
    topic: str
    status: str
    summary: str
    details: list[str] = field(default_factory=list)
    command: str = ""
    output_snippet: str = ""
    data: dict[str, Any] = field(default_factory=dict)


@dataclass
class CommandResult:
    command: str
    returncode: int
    stdout: str
    stderr: str
    duration_seconds: float
    timed_out: bool = False

    @property
    def combined_output(self) -> str:
        if self.stdout and self.stderr:
            return f"{self.stdout.rstrip()}\n{self.stderr.rstrip()}".strip()
        return self.stdout or self.stderr


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def relpath(path: str | Path) -> str:
    value = Path(path)
    try:
        return value.resolve().relative_to(REPO_ROOT.resolve()).as_posix()
    except Exception:
        return str(value).replace("\\", "/")


def redact_repo_root(text: str, *, replacement: str = ".") -> str:
    if not text:
        return text
    sanitized = text
    repo_root_windows = str(REPO_ROOT)
    repo_root_posix = repo_root_windows.replace("\\", "/")
    for needle in sorted({repo_root_windows, repo_root_posix}, key=len, reverse=True):
        sanitized = sanitized.replace(needle, replacement)
    return sanitized


def repo_relative_command_text(text: str) -> str:
    return redact_repo_root(text, replacement=".")


def output_snippet(text: str, *, limit: int = 2400) -> str:
    cleaned = (text or "").strip()
    if len(cleaned) <= limit:
        return cleaned
    return cleaned[: limit - 3].rstrip() + "..."


def load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        return [dict(row) for row in csv.DictReader(handle)]


def as_float(value: Any) -> float:
    return float(str(value).strip())


def as_int(value: Any) -> int:
    return int(round(float(str(value).strip())))


def approx_equal(actual: Any, expected: float, tolerance: float) -> bool:
    return abs(float(actual) - expected) <= tolerance


def discover_paths(patterns: Iterable[str]) -> list[str]:
    matches: list[str] = []
    for pattern in patterns:
        matches.extend(relpath(path) for path in sorted(REPO_ROOT.glob(pattern)) if path.is_file())
    return sorted(dict.fromkeys(matches))


def find_first_path(patterns: Iterable[str]) -> str:
    matches = discover_paths(patterns)
    return matches[0] if matches else ""


def file_tree_snapshot(paths: Iterable[str]) -> dict[str, dict[str, dict[str, int]]]:
    snapshot: dict[str, dict[str, dict[str, int]]] = {}
    for rel_root in paths:
        absolute_root = REPO_ROOT / rel_root
        file_map: dict[str, dict[str, int]] = {}
        if absolute_root.is_file():
            stat = absolute_root.stat()
            file_map[relpath(absolute_root)] = {"size": stat.st_size, "mtime_ns": int(stat.st_mtime_ns)}
        elif absolute_root.exists():
            for child in sorted(path for path in absolute_root.rglob("*") if path.is_file()):
                stat = child.stat()
                file_map[relpath(child)] = {"size": stat.st_size, "mtime_ns": int(stat.st_mtime_ns)}
        snapshot[rel_root] = file_map
    return snapshot


def diff_snapshots(before: dict[str, dict[str, dict[str, int]]], after: dict[str, dict[str, dict[str, int]]]) -> list[str]:
    changed: list[str] = []
    all_roots = sorted(set(before) | set(after))
    for rel_root in all_roots:
        before_files = before.get(rel_root, {})
        after_files = after.get(rel_root, {})
        file_paths = sorted(set(before_files) | set(after_files))
        for file_path in file_paths:
            if before_files.get(file_path) != after_files.get(file_path):
                changed.append(file_path)
    return changed


def powershell_base_command() -> list[str]:
    return ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass"]


def start_ps_command(*args: str) -> list[str]:
    return [*powershell_base_command(), "-File", str(START_PS_PATH), *args]


def panel_ps_command(*args: str) -> list[str]:
    return [*powershell_base_command(), "-File", str(PANEL_PS_PATH), *args]


def command_text(command: list[str]) -> str:
    return subprocess.list2cmdline(command)


def run_command(
    command: list[str],
    *,
    timeout: int,
    env: dict[str, str] | None = None,
    cwd: Path | None = None,
) -> CommandResult:
    merged_env = os.environ.copy()
    if env:
        merged_env.update(env)
    started = time.monotonic()
    try:
        completed = subprocess.run(
            command,
            cwd=str(cwd or REPO_ROOT),
            env=merged_env,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
        return CommandResult(
            command=command_text(command),
            returncode=completed.returncode,
            stdout=completed.stdout,
            stderr=completed.stderr,
            duration_seconds=time.monotonic() - started,
        )
    except subprocess.TimeoutExpired as exc:
        return CommandResult(
            command=command_text(command),
            returncode=-1,
            stdout=exc.stdout or "",
            stderr=(exc.stderr or "") + "\n[timeout]",
            duration_seconds=time.monotonic() - started,
            timed_out=True,
        )


def command_passed(result: CommandResult) -> bool:
    return result.returncode == 0 and not result.timed_out


def git_commit_hash() -> str:
    result = run_command(["git", "rev-parse", "HEAD"], timeout=20)
    return result.stdout.strip() if command_passed(result) else ""


def git_status_short() -> list[str]:
    result = run_command(["git", "status", "--short"], timeout=20)
    if not command_passed(result):
        return []
    return [line.rstrip() for line in result.stdout.splitlines() if line.strip()]


def detect_compose_mode() -> dict[str, str]:
    docker_compose = run_command(["docker", "compose", "version"], timeout=20)
    if command_passed(docker_compose):
        return {"command": "docker compose", "kind": "docker_compose_v2"}
    docker_compose_legacy = run_command(["docker-compose", "version"], timeout=20)
    if command_passed(docker_compose_legacy):
        return {"command": "docker-compose", "kind": "docker_compose_v1"}
    return {}


def compose_command(mode: dict[str, str], *args: str) -> list[str]:
    if mode.get("kind") == "docker_compose_v1":
        return ["docker-compose", *args]
    return ["docker", "compose", *args]


def docker_service_names() -> list[str]:
    compose_text = read_text(DOCKER_COMPOSE_PATH)
    return sorted(set(re.findall(r"(?m)^  ([A-Za-z0-9_-]+):\s*$", compose_text)))


def free_tcp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def poll_http_health(url: str, *, timeout_seconds: int, interval_seconds: float = 1.0) -> tuple[bool, str]:
    deadline = time.monotonic() + timeout_seconds
    last_error = ""
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=5) as response:
                body = response.read().decode("utf-8", errors="replace")
                if response.status == 200:
                    return True, body.strip()
                last_error = f"HTTP {response.status}: {body[:200]}"
        except urllib.error.URLError as exc:
            last_error = str(exc)
        except OSError as exc:
            last_error = str(exc)
        time.sleep(interval_seconds)
    return False, last_error


def read_panel_review_payload() -> dict[str, Any]:
    if not PANEL_REVIEW_JSON_PATH.exists():
        return {}
    return load_json(PANEL_REVIEW_JSON_PATH)


def row_by_key(rows: Iterable[dict[str, str]], key: str, value: str) -> dict[str, str]:
    for row in rows:
        if str(row.get(key, "")).strip() == value:
            return row
    return {}


def dwh_scorecard_rows() -> list[dict[str, str]]:
    return read_csv_rows(DWH_SCORECARD_PATH) if DWH_SCORECARD_PATH.exists() else []


def page_definitions() -> list[dict[str, str]]:
    tree = ast.parse(read_text(UI_PAGES_INIT_PATH), filename=str(UI_PAGES_INIT_PATH))
    results: list[dict[str, str]] = []

    class Visitor(ast.NodeVisitor):
        def visit_Call(self, node: ast.Call) -> None:
            func = node.func
            if isinstance(func, ast.Name) and func.id == "PageDefinition":
                values: dict[str, str] = {}
                for index, arg in enumerate(node.args[:2]):
                    if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                        values["page_id" if index == 0 else "label"] = arg.value
                for keyword in node.keywords:
                    if keyword.arg and isinstance(keyword.value, ast.Constant) and isinstance(keyword.value.value, str):
                        values[keyword.arg] = keyword.value.value
                if values:
                    results.append(values)
            self.generic_visit(node)

    Visitor().visit(tree)
    return results


def launcher_matrix_payload() -> dict[str, Any]:
    return load_json(LAUNCHER_MATRIX_PATH)


def launcher_entries() -> list[dict[str, Any]]:
    return list(launcher_matrix_payload().get("entries") or [])


def launcher_entry_map() -> dict[str, dict[str, Any]]:
    return {entry["entry_id"]: entry for entry in launcher_entries() if entry.get("entry_id")}


def launcher_categories() -> dict[str, dict[str, Any]]:
    payload = launcher_matrix_payload()
    return {category["category_id"]: category for category in payload.get("categories") or [] if category.get("category_id")}


def validate_launcher_matrix() -> list[str]:
    from src.utils.validate_launcher_matrix import audit_launcher_matrix

    report = audit_launcher_matrix(REPO_ROOT)
    issues = list(report.get("global_issues") or [])
    for entry in report.get("entries") or []:
        entry_id = str(entry.get("entry_id") or "<missing>")
        for issue in entry.get("issues") or []:
            issues.append(f"{entry_id}: {issue}")
    return issues


def documentation_findings() -> list[str]:
    findings: list[str] = []
    texts = {path.name: read_text(path) for path in DOC_PATHS}
    combined = "\n".join(texts.values()).lower()

    for path in DOC_PATHS:
        text = texts[path.name]
        if ABSOLUTE_LOCAL_PATH_PATTERN.search(text):
            findings.append(f"{path.name} contains a local absolute path")

    expected_global_phrases = (
        "panel mode is the defense-safe default",
        "full launcher is the researcher",
        "only main philippine public-observation validation claim",
        "comparator-only",
        "external transfer validation",
        "not mindoro recalibration",
        "support/context only",
        "legacy/archive support only",
    )
    for phrase in expected_global_phrases:
        if phrase not in combined:
            findings.append(f"documentation set is missing phrase '{phrase}'")

    if "drifter provenance" not in combined or "not the direct" not in combined:
        findings.append("documentation set must state that B1 drifter provenance is not direct validation truth")

    registry_text = texts["PAPER_OUTPUT_REGISTRY.md"].lower()
    if "panel review" not in registry_text and "defense inspection" not in registry_text:
        findings.append("PAPER_OUTPUT_REGISTRY.md should stay framed as a panel-review surface")

    command_matrix_text = texts["COMMAND_MATRIX.md"]
    required_commands = (
        r"Defense / panel inspection.+\\panel\.ps1",
        r"Open dashboard only.+streamlit run ui/app\.py",
        r"Inspect drifter provenance behind `B1`.+b1_drifter_context_panel",
        r"Verify manuscript numbers.+panel option `2`",
        r"Rebuild publication figures.+figure_package_publication",
        r"phase1_mindoro_focus_provenance",
        r"mindoro_phase3b_primary_public_validation",
        r"dwh_reportable_bundle",
    )
    for pattern in required_commands:
        if not re.search(pattern, command_matrix_text, flags=re.IGNORECASE | re.DOTALL):
            findings.append(f"COMMAND_MATRIX.md is missing a readable command mapping for pattern '{pattern}'")

    return findings


def artifact_findings() -> tuple[list[str], dict[str, list[str]]]:
    findings: list[str] = []
    matches: dict[str, list[str]] = {}
    for artifact_id, patterns in CRITICAL_ARTIFACT_PATTERNS.items():
        resolved = discover_paths(patterns)
        matches[artifact_id] = resolved
        if not resolved:
            findings.append(f"no artifact found for {artifact_id}")
    return findings, matches


def phase1_value_findings() -> list[str]:
    findings: list[str] = []
    manifest = load_json(PHASE1_MANIFEST_PATH)
    ranking_rows = read_csv_rows(PHASE1_RANKING_PATH)
    accepted_rows = read_csv_rows(PHASE1_ACCEPTED_PATH)
    subset_rows = read_csv_rows(PHASE1_SUBSET_PATH)

    if as_int(manifest.get("accepted_segment_count", 0)) != 65:
        findings.append("phase1 accepted segment count is not 65")
    if as_int(manifest.get("ranking_subset_segment_count", 0)) != 19:
        findings.append("phase1 ranking subset count is not 19")
    if str(manifest.get("official_b1_recipe", "")).strip() != "cmems_gfs":
        findings.append("phase1 official_b1_recipe is not cmems_gfs")
    if len(accepted_rows) != 65:
        findings.append("phase1 accepted segment registry does not contain 65 rows")
    if len(subset_rows) != 19:
        findings.append("phase1 ranking subset registry does not contain 19 rows")

    expected_order = [
        ("1", "cmems_gfs", 4.5886),
        ("2", "cmems_era5", 4.6237),
        ("3", "hycom_gfs", 4.7027),
        ("4", "hycom_era5", 4.7561),
    ]
    for rank, recipe, mean_ncs in expected_order:
        row = row_by_key(ranking_rows, "rank", rank)
        if not row:
            findings.append(f"phase1 ranking is missing rank {rank}")
            continue
        if str(row.get("recipe", "")).strip() != recipe:
            findings.append(f"phase1 rank {rank} recipe is not {recipe}")
        if not approx_equal(row.get("mean_ncs_score", 0.0), mean_ncs, 0.0002):
            findings.append(f"phase1 rank {rank} mean_ncs_score does not match {mean_ncs}")
    return findings


def manuscript_number_findings() -> list[str]:
    findings: list[str] = []
    payload = read_panel_review_payload()
    summary = payload.get("summary") or {}
    if not payload:
        return ["panel review JSON report is missing"]
    if summary.get("fail_count", 0):
        findings.append(f"panel review report contains {summary.get('fail_count')} FAIL rows")
    if summary.get("missing_source_count", 0):
        findings.append(f"panel review report contains {summary.get('missing_source_count')} MISSING_SOURCE rows")
    if summary.get("lookup_error_count", 0):
        findings.append(f"panel review report contains {summary.get('lookup_error_count')} LOOKUP_ERROR rows")
    return findings


def support_value_warnings() -> list[str]:
    warnings: list[str] = []
    if not PHASE4_BUDGET_PATH.exists():
        return warnings
    warnings.append(
        "Phase 4 thesis-facing support values are final beached percentages, first-arrival time, impacted shoreline segments, and QC status."
    )
    warnings.append(
        "Raw total_beached_kg in output/phase4/CASE_MINDORO_RETRO_2023/phase4_oil_budget_summary.csv is not treated as a primary manuscript claim in this readiness check unless it is separately re-audited."
    )
    rows = read_csv_rows(PHASE4_BUDGET_PATH)
    expected_kg = {
        "lighter_oil": 10.0,
        "fixed_base_medium_heavy_proxy": 305.0,
        "heavier_oil": 315.0,
    }
    for scenario_id, expected in expected_kg.items():
        row = row_by_key(rows, "scenario_id", scenario_id)
        if not row:
            warnings.append(f"phase4 support table is missing scenario '{scenario_id}'")
            continue
        actual = float(row.get("total_beached_kg", "0") or 0.0)
        if abs(actual - expected) > 25.0:
            warnings.append(
                f"phase4 support-layer kg value for {scenario_id} is {actual:.1f} kg, not the expected approximately {expected:.1f} kg"
            )
    return warnings


def b1_r0_findings() -> list[str]:
    findings: list[str] = []
    rows = read_csv_rows(B1_BRANCH_SURVIVAL_PATH)
    row = row_by_key(rows, "branch_id", "R0")
    if not row:
        return ["B1 branch-survival summary is missing the R0 row"]
    reached = str(row.get("reached_march14_local_date", "")).strip().lower()
    forecast_cells = as_int(row.get("forecast_nonzero_cells_from_march14_localdate_mask", 0))
    if reached not in {"false", "0"}:
        findings.append("B1 R0 row unexpectedly reached the target date")
    if forecast_cells != 0:
        findings.append("B1 R0 row unexpectedly has nonzero forecast cells")
    return findings


def page_registry_findings() -> list[str]:
    findings: list[str] = []
    pages = page_definitions()
    page_map = {page.get("page_id"): page for page in pages}
    for page_id in (
        "b1_drifter_context",
        "mindoro_validation",
        "phase1_recipe_selection",
        "dwh_transfer_validation",
        "mindoro_validation_archive",
        "legacy_2016_support",
    ):
        if page_id not in page_map:
            findings.append(f"dashboard page '{page_id}' is not registered")
    if page_map.get("b1_drifter_context", {}).get("label") != "B1 Recipe Provenance — Not Truth Mask":
        findings.append("B1 recipe-provenance advanced page label changed unexpectedly")
    if page_map.get("mindoro_validation_archive", {}).get("navigation_section") != "Archive / Support Only":
        findings.append("mindoro validation archive page must stay under Archive / Support Only")
    legacy_section = page_map.get("legacy_2016_support", {}).get("navigation_section")
    if legacy_section != "Archive / Support Only":
        findings.append("legacy 2016 page must stay separated under Archive / Support Only")

    drifter_page_text = read_text(UI_DRIFTER_PAGE_PATH)
    required_phrases = (
        "B1 Recipe Provenance — Not Truth Mask",
        "support the selected transport recipe",
        "public-observation validation",
        "not the direct",
        "No direct March 13-14 2023 accepted drifter segment is stored",
        "historical focused Phase 1 provenance",
    )
    for phrase in required_phrases:
        if phrase not in drifter_page_text:
            findings.append(f"b1_drifter_context.py is missing phrase '{phrase}'")

    forbidden_tokens = (
        "python -m src",
        ".\\start.ps1 -Entry phase1_mindoro_focus_provenance",
        ".\\start.ps1 -Entry mindoro_phase3b_primary_public_validation",
        "download_drifter",
    )
    for token in forbidden_tokens:
        if token in drifter_page_text:
            findings.append(f"b1_drifter_context.py contains forbidden token '{token}'")
    return findings


def host_package_status() -> dict[str, str]:
    results: dict[str, str] = {}
    for module_name in DEFAULT_PIPELINE_IMPORT_MODULES:
        try:
            importlib.import_module(module_name)
            results[module_name] = PASS
        except Exception as exc:  # pragma: no cover - environment dependent
            results[module_name] = f"{FAIL}: {type(exc).__name__}: {exc}"
    return results


def build_streamlit_runtime_check(mode: dict[str, str], *, timeout_seconds: int = 90) -> tuple[bool, str, str]:
    port = 8501
    cleanup_command = compose_command(
        mode,
        "exec",
        "-T",
        "pipeline",
        "sh",
        "-lc",
        "pkill -f 'streamlit run ui/app.py' >/dev/null 2>&1 || true",
    )
    run_command(cleanup_command, timeout=30)
    command = compose_command(
        mode,
        "exec",
        "-T",
        "pipeline",
        "python",
        "-m",
        "streamlit",
        "run",
        "ui/app.py",
        "--server.address",
        "0.0.0.0",
        "--server.port",
        str(port),
        "--server.headless",
        "true",
    )
    process = subprocess.Popen(
        command,
        cwd=str(REPO_ROOT),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    health_url = f"http://127.0.0.1:{port}/_stcore/health"
    try:
        ok, details = poll_http_health(health_url, timeout_seconds=timeout_seconds)
        return ok, command_text(command), details
    finally:
        process.terminate()
        try:
            process.wait(timeout=20)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=10)
        run_command(cleanup_command, timeout=30)


def run_pipeline_import_check(mode: dict[str, str], modules: Iterable[str]) -> tuple[bool, str, list[str]]:
    module_list = list(modules)
    probe = [
        "import importlib",
        "mods = " + repr(module_list),
        "failures = []",
        "for name in mods:",
        "    try:",
        "        importlib.import_module(name)",
        "        print(name + ':OK')",
        "    except Exception as exc:",
        "        failures.append(f'{name}:{type(exc).__name__}:{exc}')",
        "        print(name + ':FAIL:' + type(exc).__name__ + ':' + str(exc))",
        "raise SystemExit(1 if failures else 0)",
    ]
    result = run_command(
        compose_command(mode, "exec", "-T", "pipeline", "python", "-c", "\n".join(probe)),
        timeout=120,
    )
    return command_passed(result), result.command, [line for line in result.combined_output.splitlines() if line.strip()]


def run_gnome_import_check(mode: dict[str, str], modules: Iterable[str]) -> tuple[bool, str, list[str]]:
    module_list = list(modules)
    probe = [
        "import importlib",
        "mods = " + repr(module_list),
        "failures = []",
        "for name in mods:",
        "    try:",
        "        importlib.import_module(name)",
        "        print(name + ':OK')",
        "    except Exception as exc:",
        "        failures.append(f'{name}:{type(exc).__name__}:{exc}')",
        "        print(name + ':FAIL:' + type(exc).__name__ + ':' + str(exc))",
        "raise SystemExit(1 if failures else 0)",
    ]
    result = run_command(
        compose_command(mode, "exec", "-T", "gnome", "python", "-c", "\n".join(probe)),
        timeout=120,
    )
    return command_passed(result), result.command, [line for line in result.combined_output.splitlines() if line.strip()]


def base_report_payload(*, mode_name: str, quick: bool, require_docker: bool, require_dashboard: bool) -> dict[str, Any]:
    compose_mode = detect_compose_mode()
    return {
        "generated_at_utc": utc_now_iso(),
        "repo_root": str(REPO_ROOT),
        "mode": mode_name,
        "quick": quick,
        "require_docker": require_docker,
        "require_dashboard": require_dashboard,
        "checked_git_commit": git_commit_hash(),
        "checked_git_commit_note": (
            "Generated report records the code commit checked at runtime; if the report is later committed, "
            "the repository commit containing this report may be one commit newer."
        ),
        "platform": {
            "system": platform.system(),
            "release": platform.release(),
            "version": platform.version(),
            "machine": platform.machine(),
            "python_version": sys.version.split()[0],
            "python_executable": sys.executable,
        },
        "compose_mode_detected": compose_mode.get("command", ""),
        "host_package_status": host_package_status(),
    }


def summarize_checks(checks: Iterable[CheckResult]) -> dict[str, int]:
    pass_count = sum(1 for check in checks if check.status == PASS)
    warn_count = sum(1 for check in checks if check.status == WARN)
    fail_count = sum(1 for check in checks if check.status == FAIL)
    return {"pass_count": pass_count, "warn_count": warn_count, "fail_count": fail_count}


def report_topic_status(checks: Iterable[CheckResult]) -> dict[str, str]:
    topic_status: dict[str, str] = {}
    ranking = {PASS: 0, WARN: 1, FAIL: 2}
    for check in checks:
        current = topic_status.get(check.topic, PASS)
        if ranking[check.status] >= ranking[current]:
            topic_status[check.topic] = check.status
    return topic_status


def write_report(report_path_json: Path, report_path_md: Path, payload: dict[str, Any]) -> None:
    report_path_json.parent.mkdir(parents=True, exist_ok=True)
    report_path_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    summary = payload.get("summary") or {}
    status_by_topic = payload.get("status_by_topic") or {}
    hard_failures = payload.get("hard_failures") or []
    warnings = payload.get("warnings") or []
    commands_run = payload.get("commands_run") or []
    files_changed = payload.get("files_changed_by_readiness_test") or []

    lines = [
        "# Defense Readiness Report",
        "",
        f"- Generated at: `{payload.get('generated_at_utc', '')}`",
        f"- Checked git commit: `{payload.get('checked_git_commit', '')}`",
        f"- Commit note: {payload.get('checked_git_commit_note', '')}",
        f"- Mode: `{payload.get('mode', '')}`",
        f"- Compose mode detected: `{payload.get('compose_mode_detected', 'not found')}`",
        f"- Python: `{payload.get('platform', {}).get('python_version', '')}`",
        f"- Platform: `{payload.get('platform', {}).get('system', '')} {payload.get('platform', {}).get('release', '')}`",
        "",
        "## Summary",
        "",
        f"- PASS: {summary.get('pass_count', 0)}",
        f"- WARN: {summary.get('warn_count', 0)}",
        f"- FAIL: {summary.get('fail_count', 0)}",
        "",
        "## Topic Status",
        "",
    ]
    for topic, status in sorted(status_by_topic.items()):
        lines.append(f"- {topic}: `{status}`")

    lines.extend(["", "## Hard Failures", ""])
    if hard_failures:
        lines.extend(f"- {item}" for item in hard_failures)
    else:
        lines.append("- None")

    lines.extend(["", "## Warnings", ""])
    if warnings:
        lines.extend(f"- {item}" for item in warnings)
    else:
        lines.append("- None")

    lines.extend(["", "## Commands Run", "", "- Repo-relative commands are shown below; the local repo root has been redacted.", ""])
    if commands_run:
        lines.extend(f"- `{item}`" for item in commands_run)
    else:
        lines.append("- None")

    lines.extend(["", "## Files Changed By Readiness Test", ""])
    if files_changed:
        lines.extend(f"- `{item}`" for item in files_changed)
    else:
        lines.append("- None detected")

    lines.extend(["", "## Check Details", ""])
    for check in payload.get("checks", []):
        lines.append(f"### {check['check_id']} ({check['status']})")
        lines.append("")
        lines.append(f"- Topic: `{check['topic']}`")
        lines.append(f"- Summary: {check['summary']}")
        if check.get("command"):
            lines.append(f"- Command: `{check['command']}`")
        if check.get("details"):
            lines.extend(f"- {detail}" for detail in check["details"])
        if check.get("output_snippet"):
            lines.append("- Output snippet:")
            lines.append("")
            lines.append("```text")
            lines.append(check["output_snippet"])
            lines.append("```")
        lines.append("")

    report_path_md.write_text("\n".join(lines), encoding="utf-8")


def check_result_payload(result: CheckResult) -> dict[str, Any]:
    payload = asdict(result)
    command = payload.get("command", "")
    if command:
        payload["repo_relative_command"] = repo_relative_command_text(command)
        payload["raw_command_redacted"] = redact_repo_root(command, replacement="<REPO_ROOT>")
        payload["command"] = payload["repo_relative_command"]
    return payload
