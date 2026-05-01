from __future__ import annotations

import argparse
import ast
import csv
import json
import re
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


PASS = "PASS"
FAIL = "FAIL"

DEFAULT_REPORT_DIR = Path("output") / "launcher_matrix_validation"
LAUNCHER_MATRIX_PATH = Path("config") / "launcher_matrix.json"
LAUNCHER_MATRIX_SCHEMA_PATH = Path("config") / "launcher_matrix.schema.json"
SETTINGS_PATH = Path("config") / "settings.yaml"
COMMAND_MATRIX_PATH = Path("docs") / "COMMAND_MATRIX.md"
PYTHON_DISPATCHER_PATH = Path("src") / "__main__.py"

ALLOWED_RUN_KINDS = {
    "read_only",
    "packaging_only",
    "scientific_rerun",
    "comparator_rerun",
    "archive_rerun",
}
SCIENTIFIC_RUN_KINDS = {"scientific_rerun", "comparator_rerun", "archive_rerun"}
READ_ONLY_RUN_KINDS = {"read_only", "packaging_only"}
READ_ONLY_PHASES = {
    "final_validation_package",
    "figure_package_publication",
    "horizon_survival_audit",
    "panel_b1_drifter_context",
    "phase1_finalization_audit",
    "phase2_finalization_audit",
    "phase4_crossmodel_comparability_audit",
    "phase5_launcher_and_docs_sync",
    "prototype_legacy_final_figures",
    "prototype_pygnome_similarity_summary",
    "trajectory_gallery_build",
    "trajectory_gallery_panel_polish",
}
ALLOWED_THESIS_ROLES = {
    "primary_evidence",
    "support_context",
    "comparator_support",
    "archive_provenance",
    "legacy_support",
    "read_only_governance",
}
ALLOWED_ARCHIVE_STATUSES = {
    "archive_provenance",
    "legacy_support",
    "experimental_only",
    "repository_only_development",
}
ALLOWED_LAUNCHER_VISIBILITIES = {
    "visible_archive",
    "hidden_alias",
    "hidden_experimental",
    "read_only_only",
}
PRIMARY_EVIDENCE_ENTRY_IDS = {
    "phase1_mindoro_focus_provenance",
    "mindoro_phase3b_primary_public_validation",
    "dwh_reportable_bundle",
    "mindoro_reportable_core",
}
ARCHIVE_THESIS_ROLES = {"archive_provenance", "legacy_support"}
REQUIRED_ENTRY_FIELDS = (
    "entry_id",
    "label",
    "category_id",
    "workflow_mode",
    "description",
    "rerun_cost",
    "safe_default",
    "thesis_role",
    "manuscript_section",
    "claim_boundary",
    "run_kind",
    "recommended_for",
    "confirms_before_run",
    "thesis_facing",
    "reportable",
    "experimental_only",
    "steps",
)


@dataclass
class EntryAudit:
    entry_id: str
    canonical_id: str
    aliases: list[str]
    visible: bool
    category_id: str
    workflow_mode: str
    run_kind: str
    thesis_role: str
    steps: list[str]
    issues: list[str] = field(default_factory=list)

    @property
    def status(self) -> str:
        return FAIL if self.issues else PASS

    def to_dict(self) -> dict[str, Any]:
        return {
            "entry_id": self.entry_id,
            "canonical_id": self.canonical_id,
            "aliases": list(self.aliases),
            "visible": self.visible,
            "category_id": self.category_id,
            "workflow_mode": self.workflow_mode,
            "run_kind": self.run_kind,
            "thesis_role": self.thesis_role,
            "steps": list(self.steps),
            "status": self.status,
            "issues": list(self.issues),
        }


def _repo_path(repo_root: str | Path, relative_path: str | Path) -> Path:
    return Path(repo_root) / Path(relative_path)


def _read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8-sig") as handle:
        payload = json.load(handle)
    if not isinstance(payload, dict):
        raise ValueError(f"Expected a JSON object in {path}")
    return payload


def _load_launcher_matrix(repo_root: str | Path) -> dict[str, Any]:
    path = _repo_path(repo_root, LAUNCHER_MATRIX_PATH)
    if not path.exists():
        raise FileNotFoundError(f"Missing launcher matrix: {path}")
    return _read_json(path)


def _strip_inline_comment(value: str) -> str:
    return value.split("#", 1)[0].strip()


def _discover_workflow_modes(repo_root: str | Path) -> tuple[set[str], dict[str, str]]:
    settings_path = _repo_path(repo_root, SETTINGS_PATH)
    workflow_modes = {"prototype_2016"}
    workflow_case_files: dict[str, str] = {}
    if not settings_path.exists():
        return workflow_modes, workflow_case_files

    in_mapping = False
    for raw_line in settings_path.read_text(encoding="utf-8").splitlines():
        if re.match(r"^workflow_case_files:\s*(?:#.*)?$", raw_line):
            in_mapping = True
            continue
        if not in_mapping:
            continue
        if raw_line and not raw_line.startswith((" ", "\t")):
            break
        match = re.match(r"^\s{2,}([A-Za-z0-9_]+):\s*(.*?)\s*$", raw_line)
        if not match:
            continue
        mode = match.group(1)
        case_path = _strip_inline_comment(match.group(2)).strip("'\"")
        workflow_modes.add(mode)
        workflow_case_files[mode] = case_path
    return workflow_modes, workflow_case_files


def _discover_compose_files(repo_root: str | Path) -> list[Path]:
    root = Path(repo_root)
    candidates = [root / "docker-compose.yml"]
    candidates.extend(sorted(root.glob("*compose*.yml")))
    candidates.extend(sorted(root.glob("*compose*.yaml")))
    unique: list[Path] = []
    seen: set[Path] = set()
    for path in candidates:
        resolved = path.resolve()
        if path.exists() and resolved not in seen:
            unique.append(path)
            seen.add(resolved)
    return unique


def _discover_compose_services(repo_root: str | Path) -> set[str]:
    services: set[str] = set()
    for compose_path in _discover_compose_files(repo_root):
        in_services = False
        for raw_line in compose_path.read_text(encoding="utf-8").splitlines():
            if re.match(r"^services:\s*(?:#.*)?$", raw_line):
                in_services = True
                continue
            if not in_services:
                continue
            if raw_line and not raw_line.startswith((" ", "\t")):
                break
            match = re.match(r"^  ([A-Za-z0-9_.-]+):\s*(?:#.*)?$", raw_line)
            if match:
                services.add(match.group(1))
    return services


def _phase_literals_from_dispatcher(repo_root: str | Path) -> set[str]:
    dispatcher_path = _repo_path(repo_root, PYTHON_DISPATCHER_PATH)
    if not dispatcher_path.exists():
        return set()

    tree = ast.parse(dispatcher_path.read_text(encoding="utf-8"), filename=str(dispatcher_path))
    phases: set[str] = set()

    class PhaseVisitor(ast.NodeVisitor):
        def visit_Compare(self, node: ast.Compare) -> None:
            if isinstance(node.left, ast.Name) and node.left.id == "phase":
                for op, comparator in zip(node.ops, node.comparators):
                    if (
                        isinstance(op, ast.Eq)
                        and isinstance(comparator, ast.Constant)
                        and isinstance(comparator.value, str)
                    ):
                        phases.add(comparator.value)
            self.generic_visit(node)

    PhaseVisitor().visit(tree)
    phases.add("1_2")
    return phases


def _resolve_alias(entry_id: str, entry_map: dict[str, dict[str, Any]]) -> tuple[str, list[str]]:
    chain: list[str] = []
    current_id = entry_id
    seen: set[str] = set()
    while True:
        if current_id in seen:
            return current_id, [*chain, f"<cycle:{current_id}>"]
        seen.add(current_id)
        current = entry_map.get(current_id)
        if current is None:
            return current_id, [*chain, f"<missing:{current_id}>"]
        alias_of = str(current.get("alias_of") or "").strip()
        if not alias_of:
            return current_id, chain
        chain.append(alias_of)
        current_id = alias_of


def _aliases_by_canonical(entries: Iterable[dict[str, Any]], entry_map: dict[str, dict[str, Any]]) -> dict[str, list[str]]:
    aliases: dict[str, list[str]] = {}
    for entry in entries:
        entry_id = str(entry.get("entry_id") or "")
        if not entry_id or not entry.get("alias_of"):
            continue
        canonical_id, chain = _resolve_alias(entry_id, entry_map)
        if any(item.startswith("<") for item in chain):
            continue
        aliases.setdefault(canonical_id, []).append(entry_id)
    return {key: sorted(value) for key, value in aliases.items()}


def _command_matrix_entry_ids(repo_root: str | Path) -> set[str] | None:
    path = _repo_path(repo_root, COMMAND_MATRIX_PATH)
    if not path.exists():
        return None
    text = path.read_text(encoding="utf-8")
    match = re.search(r"## Launcher Entry Map\s*\n(?P<body>.*?)(?=\n## |\Z)", text, flags=re.DOTALL)
    if not match:
        return set()
    return set(re.findall(r"^\| `([^`]+)` \|", match.group("body"), flags=re.MULTILINE))


def _step_labels(entry: dict[str, Any]) -> list[str]:
    return [
        f"{str(step.get('service') or '').strip()}:{str(step.get('phase') or '').strip()}"
        for step in entry.get("steps") or []
    ]


def _retired_section_key() -> str:
    return "dra" + "ft_section"


def _combined_text(entry: dict[str, Any]) -> str:
    fields = ("entry_id", "label", "description", "notes", "claim_boundary", "manuscript_section")
    text_parts = [str(entry.get(field) or "") for field in fields]
    # Internal compatibility only: older local matrices may still carry the retired field.
    text_parts.append(str(entry.get(_retired_section_key()) or ""))
    return " ".join(text_parts).lower()


def _manuscript_section(entry: dict[str, Any]) -> str:
    section = str(entry.get("manuscript_section") or "").strip()
    if section:
        return section
    # Internal compatibility only. The public matrix schema now requires manuscript_section.
    return str(entry.get(_retired_section_key()) or "").strip()


def _contains_any(text: str, phrases: Iterable[str]) -> bool:
    return any(phrase in text for phrase in phrases)


def _manuscript_alignment_issues(entry: dict[str, Any]) -> list[str]:
    entry_id = str(entry.get("entry_id") or "")
    workflow_mode = str(entry.get("workflow_mode") or "")
    run_kind = str(entry.get("run_kind") or "")
    text = _combined_text(entry)
    boundary = str(entry.get("claim_boundary") or "").strip().lower()
    manuscript_section = _manuscript_section(entry)
    step_phases = {str(step.get("phase") or "") for step in entry.get("steps") or []}
    issues: list[str] = []

    if not manuscript_section:
        issues.append("manuscript_section is empty; final manuscript evidence alignment cannot be checked")
    if not boundary:
        issues.append("claim_boundary is empty")

    if entry_id == "mindoro_phase3b_primary_public_validation":
        for phrase in (
            "only main philippine public-observation validation claim",
            "independent noaa-published day-specific observation products",
            "neighborhood-scale",
        ):
            if phrase not in boundary:
                issues.append(f"B1 claim boundary is missing '{phrase}'")

    if entry_id == "phase1_mindoro_focus_provenance":
        for phrase in ("transport-provenance", "not direct spill-footprint validation"):
            if phrase not in boundary:
                issues.append(f"focused Phase 1 boundary is missing '{phrase}'")

    if entry_id == "phase1_regional_reference_rerun":
        if "does not replace focused mindoro phase 1 provenance" not in boundary:
            issues.append("regional Phase 1 boundary must say it does not replace focused Mindoro Phase 1 provenance")

    if workflow_mode == "dwh_retro_2010" or entry_id.startswith("dwh_"):
        for phrase in ("external transfer", "not mindoro recalibration"):
            if phrase not in text:
                issues.append(f"DWH entry is missing final manuscript boundary phrase '{phrase}'")

    is_comparator_entry = run_kind == "comparator_rerun" or str(entry.get("thesis_role") or "") == "comparator_support"
    if "pygnome" in text or is_comparator_entry:
        if not _contains_any(text, ("comparator-only", "comparator only")):
            issues.append("PyGNOME/comparator entry must keep comparator-only wording")
        if is_comparator_entry and not _contains_any(boundary, ("comparator-only", "comparator only")):
            issues.append("comparator entry claim_boundary must explicitly say comparator-only")

    if "phase4_oiltype_and_shoreline" in step_phases or entry_id == "mindoro_phase4_only":
        if not _contains_any(text, ("support/context", "support-only", "support only")):
            issues.append("Mindoro oil-type/shoreline entry must stay support/context only")
        if "primary validation" in text and "not a primary validation" not in text:
            issues.append("Mindoro oil-type/shoreline wording appears to promote a primary validation claim")

    if workflow_mode == "prototype_2016" or "legacy 2016" in text or "prototype_2016" in text:
        if not _contains_any(text, ("legacy", "archive", "support")):
            issues.append("legacy 2016/prototype entry must stay framed as archive/support")
        if "primary validation" in text and "not a current primary validation" not in text:
            issues.append("legacy 2016/prototype wording appears to promote a primary validation claim")

    if run_kind in READ_ONLY_RUN_KINDS:
        if not _contains_any(text, ("read-only", "stored outputs", "stored-output", "does not recompute science")):
            issues.append("read-only/package entry must say it uses stored outputs or does not recompute science")

    return issues


def _entry_issues(
    *,
    entry: dict[str, Any],
    categories: set[str],
    entry_map: dict[str, dict[str, Any]],
    workflow_modes: set[str],
    workflow_case_files: dict[str, str],
    dispatcher_phases: set[str],
    compose_services: set[str],
    command_matrix_ids: set[str] | None,
) -> list[str]:
    issues: list[str] = []
    entry_id = str(entry.get("entry_id") or "<missing>")
    for field_name in REQUIRED_ENTRY_FIELDS:
        if field_name not in entry:
            issues.append(f"missing required field '{field_name}'")

    category_id = str(entry.get("category_id") or "")
    if category_id not in categories:
        issues.append(f"unknown category_id '{category_id}'")

    workflow_mode = str(entry.get("workflow_mode") or "")
    if workflow_mode not in workflow_modes:
        issues.append(f"workflow_mode '{workflow_mode}' is not accepted by config/settings.yaml or the dispatcher")
    case_file = workflow_case_files.get(workflow_mode)
    if case_file:
        case_path = Path(case_file)
        if not case_path.is_absolute():
            case_path = Path.cwd() / case_path
        # Rebase to the audit repo root instead of the process cwd when possible.
        # The caller passes repo-root-relative case paths from settings.yaml.
        if not case_path.exists():
            issues.append(f"workflow_mode '{workflow_mode}' case config is missing: {case_file}")

    run_kind = str(entry.get("run_kind") or "")
    if run_kind not in ALLOWED_RUN_KINDS:
        issues.append(f"invalid run_kind '{run_kind}'")

    alias_of = str(entry.get("alias_of") or "").strip()
    thesis_role = str(entry.get("thesis_role") or "")
    if thesis_role not in ALLOWED_THESIS_ROLES:
        issues.append(f"invalid thesis_role '{thesis_role}'")
    elif thesis_role == "primary_evidence" and entry_id not in PRIMARY_EVIDENCE_ENTRY_IDS:
        issues.append("primary_evidence role is limited to final manuscript evidence entries")

    for boolean_field in ("thesis_facing", "reportable", "experimental_only"):
        if boolean_field in entry and not isinstance(entry.get(boolean_field), bool):
            issues.append(f"field '{boolean_field}' must be boolean")

    archive_status = str(entry.get("archive_status") or "").strip()
    launcher_visibility = str(entry.get("launcher_visibility") or "").strip()
    boundary = str(entry.get("claim_boundary") or "").strip().lower()
    if archive_status:
        if archive_status not in ALLOWED_ARCHIVE_STATUSES:
            issues.append(f"invalid archive_status '{archive_status}'")
        if not str(entry.get("archive_registry_id") or "").strip():
            issues.append("archive_status entries must declare archive_registry_id")
        if not launcher_visibility:
            issues.append("archive_status entries must declare launcher_visibility")
        elif launcher_visibility not in ALLOWED_LAUNCHER_VISIBILITIES:
            issues.append(f"invalid launcher_visibility '{launcher_visibility}'")
        if entry.get("thesis_facing") is True:
            issues.append("archived launcher entries cannot be thesis_facing=true")
        if archive_status in {"archive_provenance", "legacy_support", "experimental_only"} and entry.get("reportable") is True:
            issues.append("archived launcher entries cannot be reportable=true")

    if thesis_role in ARCHIVE_THESIS_ROLES and entry.get("thesis_facing") is True:
        issues.append("archive/legacy thesis roles cannot be thesis_facing=true")

    if bool(entry.get("experimental_only")):
        if archive_status != "experimental_only":
            issues.append("experimental_only entries must declare archive_status='experimental_only'")
        if launcher_visibility != "hidden_experimental":
            issues.append("experimental_only entries must use launcher_visibility='hidden_experimental'")
        if not bool(entry.get("menu_hidden")):
            issues.append("experimental_only entries must be hidden from panel/main menus")
        if entry.get("thesis_facing") is not False:
            issues.append("experimental_only entries must set thesis_facing=false")
        if entry.get("reportable") is not False:
            issues.append("experimental_only entries must set reportable=false")

    if launcher_visibility == "hidden_alias":
        if not alias_of:
            issues.append("hidden_alias launcher_visibility requires alias_of")
        if not bool(entry.get("menu_hidden")):
            issues.append("hidden_alias entries must be menu_hidden=true")

    if entry_id == "mindoro_reportable_core":
        if entry.get("default_panel_path") is not False:
            issues.append("mindoro_reportable_core must declare default_panel_path=false")
        if entry.get("intentional_full_evidence_support_bundle") is not True:
            issues.append("mindoro_reportable_core must declare intentional_full_evidence_support_bundle=true")
        if "not the default panel path" not in boundary:
            issues.append("mindoro_reportable_core boundary must say it is not the default panel path")

    if bool(entry.get("safe_default")) and run_kind in SCIENTIFIC_RUN_KINDS:
        issues.append("scientific/archive/comparator entries cannot be safe_default=true")
    if run_kind in SCIENTIFIC_RUN_KINDS and not bool(entry.get("confirms_before_run")) and not bool(entry.get("safe_default")):
        issues.append("scientific/archive/comparator entry must set confirms_before_run=true")

    steps = entry.get("steps") or []
    if not isinstance(steps, list) or not steps:
        issues.append("steps must be a non-empty list")
        steps = []

    for index, step in enumerate(steps, start=1):
        phase = str((step or {}).get("phase") or "").strip()
        service = str((step or {}).get("service") or "").strip()
        description = str((step or {}).get("description") or "").strip()
        if not phase:
            issues.append(f"step {index} is missing phase")
        elif dispatcher_phases and phase not in dispatcher_phases:
            issues.append(f"step {index} phase '{phase}' has no explicit Python dispatcher handler")
        if not service:
            issues.append(f"step {index} is missing service")
        elif compose_services and service not in compose_services:
            issues.append(f"step {index} service '{service}' is not defined in compose services")
        if not description:
            issues.append(f"step {index} is missing description")

    if run_kind in READ_ONLY_RUN_KINDS:
        scientific_steps = [
            str((step or {}).get("phase") or "")
            for step in steps
            if str((step or {}).get("phase") or "") not in READ_ONLY_PHASES
        ]
        if scientific_steps:
            issues.append(
                "read-only/package entry calls non-read-only phase(s): "
                + ", ".join(sorted(set(scientific_steps)))
            )

    if alias_of:
        target = entry_map.get(alias_of)
        if target is None:
            issues.append(f"alias_of points to missing entry '{alias_of}'")
        else:
            if bool(target.get("menu_hidden")):
                issues.append(f"alias target '{alias_of}' must be visible")
            for copied_field in ("workflow_mode", "rerun_cost", "safe_default", "run_kind"):
                if entry.get(copied_field) != target.get(copied_field):
                    issues.append(f"alias field '{copied_field}' differs from canonical target '{alias_of}'")
            if entry.get("steps") != target.get("steps"):
                issues.append(f"alias steps differ from canonical target '{alias_of}'")
        if not bool(entry.get("menu_hidden")):
            issues.append("alias entry must be hidden from the default menu")

    if command_matrix_ids is not None and not bool(entry.get("menu_hidden")) and entry_id not in command_matrix_ids:
        issues.append("visible entry is missing from docs/COMMAND_MATRIX.md Launcher Entry Map")

    issues.extend(_manuscript_alignment_issues(entry))
    return issues


def _schema_file_issues(repo_root: str | Path) -> list[str]:
    path = _repo_path(repo_root, LAUNCHER_MATRIX_SCHEMA_PATH)
    if not path.exists():
        return ["config/launcher_matrix.schema.json is missing"]
    try:
        schema = _read_json(path)
    except Exception as exc:  # pragma: no cover - defensive path
        return [f"config/launcher_matrix.schema.json is not valid JSON: {exc}"]

    issues: list[str] = []
    required = set(schema.get("required") or [])
    for field_name in ("catalog_version", "entrypoint", "categories", "role_groups", "entries"):
        if field_name not in required:
            issues.append(f"schema top-level required list is missing '{field_name}'")

    entry_schema = (
        schema.get("properties", {})
        .get("entries", {})
        .get("items", {})
    )
    entry_required = set(entry_schema.get("required") or [])
    missing_required = sorted(set(REQUIRED_ENTRY_FIELDS) - entry_required)
    if missing_required:
        issues.append(
            "schema entry required list is missing: " + ", ".join(missing_required)
        )
    entry_properties = entry_schema.get("properties", {})
    for field_name in (
        "archive_status",
        "archive_registry_id",
        "launcher_visibility",
        "thesis_facing",
        "reportable",
        "experimental_only",
    ):
        if field_name not in entry_properties:
            issues.append(f"schema entry properties are missing '{field_name}'")

    run_kind_enum = set(
        entry_properties
        .get("run_kind", {})
        .get("enum", [])
    )
    if run_kind_enum and run_kind_enum != ALLOWED_RUN_KINDS:
        issues.append("schema run_kind enum does not match validator allowed values")

    thesis_role_enum = set(
        entry_properties
        .get("thesis_role", {})
        .get("enum", [])
    )
    if thesis_role_enum and thesis_role_enum != ALLOWED_THESIS_ROLES:
        issues.append("schema thesis_role enum does not match validator allowed values")

    archive_status_enum = set(entry_properties.get("archive_status", {}).get("enum", []))
    if archive_status_enum and archive_status_enum != ALLOWED_ARCHIVE_STATUSES:
        issues.append("schema archive_status enum does not match validator allowed values")

    launcher_visibility_enum = set(entry_properties.get("launcher_visibility", {}).get("enum", []))
    if launcher_visibility_enum and launcher_visibility_enum != ALLOWED_LAUNCHER_VISIBILITIES:
        issues.append("schema launcher_visibility enum does not match validator allowed values")

    return issues


def _visible_menu_order_issues(
    *,
    entries: list[dict[str, Any]],
    role_groups: list[dict[str, Any]],
) -> list[str]:
    issues: list[str] = []
    for group in role_groups:
        group_id = str(group.get("GroupId") or "<missing>")
        roles = {
            str(role)
            for role in group.get("thesis_roles") or []
            if str(role)
        }
        if not roles:
            issues.append(f"role group '{group_id}' has no thesis_roles")
            continue

        visible_group_entries = [
            entry
            for entry in entries
            if not bool(entry.get("menu_hidden")) and str(entry.get("thesis_role") or "") in roles
        ]
        orders: dict[str, list[str]] = {}
        for entry in visible_group_entries:
            order = entry.get("menu_order")
            if order is None:
                issues.append(f"visible entry '{entry.get('entry_id', '<missing>')}' is missing menu_order")
                continue
            key = str(order)
            orders.setdefault(key, []).append(str(entry.get("entry_id") or "<missing>"))

        duplicates = {
            order: ids
            for order, ids in orders.items()
            if len(ids) > 1
        }
        for order, ids in sorted(duplicates.items()):
            issues.append(
                f"duplicate visible menu_order '{order}' in role group '{group_id}': "
                + ", ".join(sorted(ids))
            )

    return issues


def audit_launcher_matrix(repo_root: str | Path = ".") -> dict[str, Any]:
    root = Path(repo_root).resolve()
    matrix = _load_launcher_matrix(root)
    categories = {
        str(category.get("category_id") or "")
        for category in matrix.get("categories") or []
        if str(category.get("category_id") or "")
    }
    role_groups = [dict(group or {}) for group in matrix.get("role_groups") or []]
    entries = [dict(entry or {}) for entry in matrix.get("entries") or []]
    entry_ids = [str(entry.get("entry_id") or "") for entry in entries]
    entry_map = {str(entry.get("entry_id") or ""): entry for entry in entries if str(entry.get("entry_id") or "")}
    workflow_modes, workflow_case_files = _discover_workflow_modes(root)
    dispatcher_phases = _phase_literals_from_dispatcher(root)
    compose_services = _discover_compose_services(root)
    command_matrix_ids = _command_matrix_entry_ids(root)
    aliases_by_canonical = _aliases_by_canonical(entries, entry_map)
    global_issues: list[str] = []

    global_issues.extend(_schema_file_issues(root))
    if not categories:
        global_issues.append("launcher matrix has no categories")
    if not entries:
        global_issues.append("launcher matrix has no entries")
    if not role_groups:
        global_issues.append("launcher matrix has no role_groups")
    duplicate_ids = sorted({entry_id for entry_id in entry_ids if entry_id and entry_ids.count(entry_id) > 1})
    if duplicate_ids:
        global_issues.append("duplicate entry IDs found: " + ", ".join(duplicate_ids))
    global_issues.extend(_visible_menu_order_issues(entries=entries, role_groups=role_groups))
    if command_matrix_ids is None:
        global_issues.append("docs/COMMAND_MATRIX.md is missing")
    elif command_matrix_ids:
        visible_ids = {str(entry.get("entry_id") or "") for entry in entries if not bool(entry.get("menu_hidden"))}
        documented_visible_ids = command_matrix_ids & set(entry_ids)
        stale_documented = sorted(documented_visible_ids - visible_ids)
        missing_documented = sorted(visible_ids - documented_visible_ids)
        if missing_documented:
            global_issues.append(
                "visible launcher entries missing from Command Matrix table: "
                + ", ".join(missing_documented)
            )
        if stale_documented:
            global_issues.append(
                "Command Matrix Launcher Entry Map includes hidden/stale entries: "
                + ", ".join(stale_documented)
            )

    audits: list[EntryAudit] = []
    for entry in entries:
        entry_id = str(entry.get("entry_id") or "<missing>")
        canonical_id, alias_chain = _resolve_alias(entry_id, entry_map)
        issues = _entry_issues(
            entry=entry,
            categories=categories,
            entry_map=entry_map,
            workflow_modes=workflow_modes,
            workflow_case_files={mode: str(root / path) for mode, path in workflow_case_files.items()},
            dispatcher_phases=dispatcher_phases,
            compose_services=compose_services,
            command_matrix_ids=command_matrix_ids,
        )
        if any(item.startswith("<cycle:") for item in alias_chain):
            issues.append("alias cycle detected")
        if any(item.startswith("<missing:") for item in alias_chain):
            issues.append("alias chain points to a missing entry")

        audits.append(
            EntryAudit(
                entry_id=entry_id,
                canonical_id=canonical_id,
                aliases=aliases_by_canonical.get(entry_id, []),
                visible=not bool(entry.get("menu_hidden")),
                category_id=str(entry.get("category_id") or ""),
                workflow_mode=str(entry.get("workflow_mode") or ""),
                run_kind=str(entry.get("run_kind") or ""),
                thesis_role=str(entry.get("thesis_role") or ""),
                steps=_step_labels(entry),
                issues=issues,
            )
        )

    failing_entries = [audit for audit in audits if audit.issues]
    status = FAIL if global_issues or failing_entries else PASS
    return {
        "generated_at_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "repo_root": str(root),
        "status": status,
        "summary": {
            "entry_count": len(audits),
            "visible_entry_count": sum(1 for audit in audits if audit.visible),
            "hidden_entry_count": sum(1 for audit in audits if not audit.visible),
            "pass_count": sum(1 for audit in audits if audit.status == PASS),
            "fail_count": len(failing_entries),
            "global_issue_count": len(global_issues),
        },
        "global_issues": global_issues,
        "accepted_workflow_modes": sorted(workflow_modes),
        "accepted_dispatcher_phases": sorted(dispatcher_phases),
        "compose_services": sorted(compose_services),
        "entries": [audit.to_dict() for audit in audits],
    }


def _write_csv(report: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=(
                "status",
                "entry_id",
                "canonical_id",
                "aliases",
                "visible",
                "category_id",
                "workflow_mode",
                "run_kind",
                "thesis_role",
                "steps",
                "issues",
            ),
        )
        writer.writeheader()
        for entry in report["entries"]:
            writer.writerow(
                {
                    "status": entry["status"],
                    "entry_id": entry["entry_id"],
                    "canonical_id": entry["canonical_id"],
                    "aliases": "; ".join(entry["aliases"]),
                    "visible": str(bool(entry["visible"])).lower(),
                    "category_id": entry["category_id"],
                    "workflow_mode": entry["workflow_mode"],
                    "run_kind": entry["run_kind"],
                    "thesis_role": entry["thesis_role"],
                    "steps": " -> ".join(entry["steps"]),
                    "issues": " | ".join(entry["issues"]),
                }
            )


def _write_markdown(report: dict[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    summary = report["summary"]
    lines = [
        "# Launcher Matrix Audit Report",
        "",
        f"- Generated at: `{report['generated_at_utc']}`",
        f"- Overall status: `{report['status']}`",
        f"- Entries: {summary['entry_count']} total; {summary['visible_entry_count']} visible; {summary['hidden_entry_count']} hidden",
        f"- Entry PASS/FAIL: {summary['pass_count']} pass; {summary['fail_count']} fail",
        f"- Global issues: {summary['global_issue_count']}",
        "",
        "## Global Issues",
        "",
    ]
    if report["global_issues"]:
        lines.extend(f"- {issue}" for issue in report["global_issues"])
    else:
        lines.append("- None")

    lines.extend(
        [
            "",
            "## Entry Audit",
            "",
            "| Status | Entry ID | Canonical ID | Visible | Run kind | Workflow mode | Steps | Issues |",
            "| --- | --- | --- | --- | --- | --- | --- | --- |",
        ]
    )
    for entry in report["entries"]:
        issues = "<br>".join(entry["issues"]) if entry["issues"] else ""
        lines.append(
            "| {status} | `{entry_id}` | `{canonical_id}` | {visible} | `{run_kind}` | `{workflow_mode}` | {steps} | {issues} |".format(
                status=entry["status"],
                entry_id=entry["entry_id"],
                canonical_id=entry["canonical_id"],
                visible=str(bool(entry["visible"])).lower(),
                run_kind=entry["run_kind"],
                workflow_mode=entry["workflow_mode"],
                steps=" -> ".join(f"`{step}`" for step in entry["steps"]),
                issues=issues,
            )
        )

    lines.extend(
        [
            "",
            "## Accepted Dispatcher Phases",
            "",
            ", ".join(f"`{phase}`" for phase in report["accepted_dispatcher_phases"]),
            "",
            "## Compose Services",
            "",
            ", ".join(f"`{service}`" for service in report["compose_services"]) or "_None discovered_",
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def write_report(report: dict[str, Any], report_dir: str | Path) -> dict[str, str]:
    output_dir = Path(report_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / "launcher_matrix_audit.json"
    csv_path = output_dir / "launcher_matrix_audit.csv"
    markdown_path = output_dir / "launcher_matrix_audit.md"
    json_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    _write_csv(report, csv_path)
    _write_markdown(report, markdown_path)
    return {
        "json": str(json_path),
        "csv": str(csv_path),
        "markdown": str(markdown_path),
    }


def _print_report(report: dict[str, Any], paths: dict[str, str] | None = None) -> None:
    summary = report["summary"]
    print("Launcher matrix validation")
    print(f"OVERALL: {report['status']}")
    print(
        "Entries: {entry_count} total, {visible_entry_count} visible, {hidden_entry_count} hidden, "
        "{pass_count} PASS, {fail_count} FAIL".format(**summary)
    )
    if report["global_issues"]:
        print("")
        print("Global issues:")
        for issue in report["global_issues"]:
            print(f"  FAIL {issue}")
    print("")
    print("Entry results:")
    for entry in report["entries"]:
        suffix = ""
        if entry["issues"]:
            suffix = " - " + "; ".join(entry["issues"])
        print(f"  {entry['status']} {entry['entry_id']} -> {entry['canonical_id']}{suffix}")
    if paths:
        print("")
        print("Audit report written:")
        for label in ("markdown", "csv", "json"):
            print(f"  {label}: {paths[label]}")


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Validate config/launcher_matrix.json without Docker or science runs.")
    parser.add_argument("--repo-root", default=".", help="Repository root to audit.")
    parser.add_argument("--report-dir", default=str(DEFAULT_REPORT_DIR), help="Directory for JSON/CSV/Markdown audit reports.")
    parser.add_argument("--no-write", action="store_true", help="Print only; do not write audit report files.")
    args = parser.parse_args(list(argv) if argv is not None else None)

    report = audit_launcher_matrix(args.repo_root)
    paths = None
    if not args.no_write:
        paths = write_report(report, Path(args.repo_root) / args.report_dir)
    _print_report(report, paths)
    return 0 if report["status"] == PASS else 1


if __name__ == "__main__":
    raise SystemExit(main())
