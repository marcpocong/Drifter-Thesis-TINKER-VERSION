from __future__ import annotations

import json
import os
import sys
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Callable, Iterable

from src.exceptions.custom import PREP_FORCE_REFRESH_ENV
from src.utils.forcing_outage_policy import (
    FORCING_SOURCE_BUDGET_SECONDS_DEFAULT,
    FORCING_SOURCE_BUDGET_SECONDS_ENV,
    resolve_forcing_source_budget_seconds,
)


INPUT_CACHE_POLICY_ENV = "INPUT_CACHE_POLICY"
INPUT_CACHE_POLICY_DEFAULT = "default"
INPUT_CACHE_POLICY_REUSE_IF_VALID = "reuse_if_valid"
INPUT_CACHE_POLICY_FORCE_REFRESH = "force_refresh"
PROTOTYPE_2016_ENSEMBLE_POLICY_ENV = "PROTOTYPE_2016_ENSEMBLE_POLICY"
PROTOTYPE_2016_ENSEMBLE_POLICY_FULL_RERUN = "full_rerun"
PROTOTYPE_2016_ENSEMBLE_POLICY_REUSE_IF_VALID = "reuse_if_valid"
RUN_STARTUP_PROMPTS_RESOLVED_ENV = "RUN_STARTUP_PROMPTS_RESOLVED"
RUN_STARTUP_TOKEN_ENV = "RUN_STARTUP_TOKEN"
STARTUP_PROMPT_PROBE_PREFIX = "STARTUP_PROMPT_PROBE="
STARTUP_VALUE_SOURCE_EXPLICIT_ENV = "explicit_env"
STARTUP_VALUE_SOURCE_INTERACTIVE_PROMPT = "interactive_prompt"
STARTUP_VALUE_SOURCE_NON_INTERACTIVE_DEFAULT = "non_interactive_default"
STARTUP_VALUE_SOURCE_DEFAULT_NO_ELIGIBLE_CACHE = "default_no_eligible_cache"
STARTUP_VALUE_SOURCE_DEFAULT_NON_PROMPTABLE = "default_non_promptable"
STARTUP_VALUE_SOURCE_PREP_FORCE_REFRESH_ALIAS = "prep_force_refresh_alias"

READ_ONLY_LAUNCHER_CATEGORY_ID = "read_only_packaging_help_utilities"
READ_ONLY_PHASES = {
    "final_validation_package",
    "figure_package_publication",
    "horizon_survival_audit",
    "phase1_finalization_audit",
    "phase2_finalization_audit",
    "phase4_crossmodel_comparability_audit",
    "phase5_launcher_and_docs_sync",
    "prototype_legacy_final_figures",
    "prototype_pygnome_similarity_summary",
    "trajectory_gallery_build",
    "trajectory_gallery_panel_polish",
}

WAIT_BUDGET_PRESETS: dict[str, int] = {
    "1": 120,
    "2": 300,
    "3": 600,
    "4": 0,
}

_REPO_ROOT = Path(__file__).resolve().parents[2]
_LAUNCHER_MATRIX_PATH = _REPO_ROOT / "config" / "launcher_matrix.json"


def _env_flag_enabled(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in {"1", "true", "yes", "y", "on"}


def normalize_input_cache_policy(value: str | None) -> str:
    normalized = str(value or INPUT_CACHE_POLICY_DEFAULT).strip().lower()
    if normalized in {
        INPUT_CACHE_POLICY_DEFAULT,
        INPUT_CACHE_POLICY_REUSE_IF_VALID,
        INPUT_CACHE_POLICY_FORCE_REFRESH,
    }:
        return normalized
    raise ValueError(
        "INPUT_CACHE_POLICY must be one of: "
        f"{INPUT_CACHE_POLICY_DEFAULT}, "
        f"{INPUT_CACHE_POLICY_REUSE_IF_VALID}, "
        f"{INPUT_CACHE_POLICY_FORCE_REFRESH}."
    )


def normalize_prototype_2016_ensemble_policy(value: str | None) -> str:
    normalized = str(value or PROTOTYPE_2016_ENSEMBLE_POLICY_FULL_RERUN).strip().lower()
    if normalized in {
        PROTOTYPE_2016_ENSEMBLE_POLICY_FULL_RERUN,
        PROTOTYPE_2016_ENSEMBLE_POLICY_REUSE_IF_VALID,
    }:
        return normalized
    raise ValueError(
        "PROTOTYPE_2016_ENSEMBLE_POLICY must be one of: "
        f"{PROTOTYPE_2016_ENSEMBLE_POLICY_FULL_RERUN}, "
        f"{PROTOTYPE_2016_ENSEMBLE_POLICY_REUSE_IF_VALID}."
    )


def resolve_effective_input_cache_policy(
    value: str | None = None,
    *,
    prep_force_refresh_value: str | None = None,
) -> str:
    normalized = normalize_input_cache_policy(
        os.environ.get(INPUT_CACHE_POLICY_ENV) if value is None else value
    )
    if normalized == INPUT_CACHE_POLICY_FORCE_REFRESH:
        return INPUT_CACHE_POLICY_FORCE_REFRESH
    if normalized == INPUT_CACHE_POLICY_REUSE_IF_VALID:
        return INPUT_CACHE_POLICY_REUSE_IF_VALID
    if prep_force_refresh_value is None and _env_flag_enabled(PREP_FORCE_REFRESH_ENV):
        return INPUT_CACHE_POLICY_FORCE_REFRESH
    if prep_force_refresh_value is not None and str(prep_force_refresh_value).strip().lower() in {
        "1",
        "true",
        "yes",
        "y",
        "on",
    }:
        return INPUT_CACHE_POLICY_FORCE_REFRESH
    return INPUT_CACHE_POLICY_REUSE_IF_VALID


def input_cache_policy_force_refresh_enabled(
    value: str | None = None,
    *,
    prep_force_refresh_value: str | None = None,
) -> bool:
    return (
        resolve_effective_input_cache_policy(
            value,
            prep_force_refresh_value=prep_force_refresh_value,
        )
        == INPUT_CACHE_POLICY_FORCE_REFRESH
    )


def runtime_is_interactive() -> bool:
    try:
        return bool(sys.stdin.isatty() and sys.stdout.isatty())
    except Exception:
        return False


def phase_is_read_only(phase: str | None) -> bool:
    return str(phase or "").strip() in READ_ONLY_PHASES


def pipeline_role_can_prompt(role: str | None = None) -> bool:
    return str(role or os.environ.get("PIPELINE_ROLE", "")).strip().lower() == "pipeline"


def phase_uses_startup_prompts(phase: str | None, *, role: str | None = None) -> bool:
    if not pipeline_role_can_prompt(role):
        return False
    return not phase_is_read_only(phase)


def _load_launcher_matrix() -> dict[str, Any]:
    with open(_LAUNCHER_MATRIX_PATH, "r", encoding="utf-8") as handle:
        return json.load(handle) or {}


def get_launcher_entry(entry_id: str) -> dict[str, Any]:
    matrix = _load_launcher_matrix()
    for entry in matrix.get("entries") or []:
        if str(entry.get("entry_id") or "") == str(entry_id):
            return dict(entry)
    raise KeyError(f"Unknown launcher entry: {entry_id}")


def find_matching_launcher_entry_id(
    *,
    workflow_mode: str,
    phase: str,
    role: str | None = None,
) -> str | None:
    resolved_role = str(role or os.environ.get("PIPELINE_ROLE", "pipeline") or "pipeline").strip().lower()
    desired_service = "pipeline" if resolved_role == "pipeline" else resolved_role
    matrix = _load_launcher_matrix()
    for entry in matrix.get("entries") or []:
        if str(entry.get("workflow_mode") or "") != str(workflow_mode):
            continue
        for step in entry.get("steps") or []:
            if str(step.get("phase") or "") != str(phase):
                continue
            step_service = str(step.get("service") or "").strip().lower()
            if desired_service and step_service != desired_service:
                continue
            return str(entry.get("entry_id") or "")
    return None


@contextmanager
def _temporary_env(updates: dict[str, str | None]):
    original = {key: os.environ.get(key) for key in updates}
    try:
        for key, value in updates.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = str(value)
        yield
    finally:
        for key, value in original.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def _probe_generic_data_ingestion_cache() -> list[dict[str, Any]]:
    from src.services.ingestion import DataIngestionService

    service = DataIngestionService()
    hits: list[dict[str, Any]] = []
    for source_id in ("drifters", "arcgis", "hycom", "cmems", "cmems_wave", "era5", "ncep", "gfs"):
        path = service._canonical_cache_path(source_id)
        if not path.exists():
            continue
        validation = service._validate_cached_source(source_id)
        if validation.get("valid"):
            hits.append(
                {
                    "cache_family": "same_run_input_cache",
                    "source_id": str(source_id),
                    "path": str(validation.get("path") or path),
                    "summary": str(validation.get("summary") or ""),
                }
            )
    return hits


def _probe_phase1_monthly_caches() -> list[dict[str, Any]]:
    from src.core.case_context import get_case_context
    from src.services.phase1_production_rerun import Phase1ProductionRerunService

    get_case_context.cache_clear()
    try:
        service = Phase1ProductionRerunService()
    finally:
        get_case_context.cache_clear()
    hits: list[dict[str, Any]] = []
    for path in sorted(service.drifter_cache_root.glob("*.csv")):
        hits.append(
            {
                "cache_family": "phase1_monthly_drifter_chunks",
                "source_id": "phase1_drifter_monthly_chunk",
                "path": str(path),
                "summary": "existing local monthly drifter input store",
            }
        )
    for path in sorted(service.forcing_cache_root.rglob("*.nc")):
        hits.append(
            {
                "cache_family": "phase1_monthly_forcing_cache",
                "source_id": "phase1_monthly_forcing_file",
                "path": str(path),
                "summary": "existing local monthly forcing input store",
            }
        )
    return hits


def _probe_dwh_input_caches() -> list[dict[str, Any]]:
    from src.services.dwh_phase3c_scientific_forcing import CASE_ID, PREPARED_FORCING_DIR, path_is_smoke_only
    from src.services.phase3c_external_case_setup import PHASE3C_DIR_NAME

    hits: list[dict[str, Any]] = []
    for filename, source_id in (
        ("hycom_gofs31_current_dwh_20100520_20100524.nc", "hycom"),
        ("era5_wind_dwh_20100520_20100524.nc", "era5"),
        ("cmems_wave_stokes_dwh_20100520_20100524.nc", "cmems_wave"),
        ("cmems_physics_current_fallback_dwh_20100520_20100524.nc", "cmems"),
    ):
        path = PREPARED_FORCING_DIR / filename
        if path.exists() and not path_is_smoke_only(path):
            hits.append(
                {
                    "cache_family": "dwh_prepared_forcing",
                    "source_id": source_id,
                    "path": str(path),
                    "summary": "existing prepared DWH historical forcing file",
                }
            )

    setup_manifest = Path("output") / CASE_ID / PHASE3C_DIR_NAME / "phase3c_external_case_setup_manifest.json"
    if setup_manifest.exists():
        hits.append(
            {
                "cache_family": "dwh_setup_bundle",
                "source_id": "phase3c_external_case_setup",
                "path": str(setup_manifest),
                "summary": "existing DWH external-case setup manifest",
            }
        )
    return hits


def probe_input_cache_availability(
    *,
    workflow_mode: str,
    phase: str,
    role: str | None = None,
) -> dict[str, Any]:
    if not phase_uses_startup_prompts(phase, role=role):
        return {
            "has_eligible_input_cache": False,
            "eligible_caches": [],
            "cache_probe_scope": "read_only_or_non_pipeline",
        }

    env_updates = {
        "WORKFLOW_MODE": workflow_mode,
        "PIPELINE_PHASE": phase,
        "PIPELINE_ROLE": str(role or os.environ.get("PIPELINE_ROLE", "pipeline")),
    }
    with _temporary_env(env_updates):
        hits: list[dict[str, Any]] = []
        if workflow_mode in {"phase1_regional_2016_2022", "phase1_mindoro_focus_pre_spill_2016_2023"}:
            hits.extend(_probe_phase1_monthly_caches())
        elif workflow_mode == "dwh_retro_2010":
            hits.extend(_probe_dwh_input_caches())
        else:
            hits.extend(_probe_generic_data_ingestion_cache())
    return {
        "has_eligible_input_cache": bool(hits),
        "eligible_caches": hits,
        "cache_probe_scope": str(workflow_mode),
    }


def build_phase_probe(
    *,
    workflow_mode: str,
    phase: str,
    role: str | None = None,
) -> dict[str, Any]:
    resolved_role = str(role or os.environ.get("PIPELINE_ROLE", "pipeline"))
    cache_probe = probe_input_cache_availability(
        workflow_mode=workflow_mode,
        phase=phase,
        role=resolved_role,
    )
    return {
        "workflow_mode": str(workflow_mode),
        "phase": str(phase),
        "pipeline_role": resolved_role,
        "read_only_phase": phase_is_read_only(phase),
        "should_prompt_wait_budget": phase_uses_startup_prompts(phase, role=resolved_role),
        "has_eligible_input_cache": bool(cache_probe["has_eligible_input_cache"]),
        "eligible_caches": list(cache_probe["eligible_caches"]),
        "cache_probe_scope": str(cache_probe["cache_probe_scope"]),
    }


def build_launcher_entry_probe(entry_id: str) -> dict[str, Any]:
    entry = get_launcher_entry(entry_id)
    steps = list(entry.get("steps") or [])
    phases = [str(step.get("phase") or "") for step in steps if str(step.get("service") or "") == "pipeline"]
    first_prompt_phase = next((phase for phase in phases if not phase_is_read_only(phase)), phases[0] if phases else "")
    cache_probe = (
        probe_input_cache_availability(
            workflow_mode=str(entry.get("workflow_mode") or ""),
            phase=first_prompt_phase,
            role="pipeline",
        )
        if first_prompt_phase
        else {"has_eligible_input_cache": False, "eligible_caches": [], "cache_probe_scope": "no_pipeline_steps"}
    )
    should_prompt = (
        str(entry.get("category_id") or "") != READ_ONLY_LAUNCHER_CATEGORY_ID
        and any(not phase_is_read_only(phase) for phase in phases)
    )
    return {
        "entry_id": str(entry.get("entry_id") or entry_id),
        "workflow_mode": str(entry.get("workflow_mode") or ""),
        "category_id": str(entry.get("category_id") or ""),
        "pipeline_phases": phases,
        "should_prompt_wait_budget": bool(should_prompt),
        "has_eligible_input_cache": bool(cache_probe["has_eligible_input_cache"]),
        "eligible_caches": list(cache_probe["eligible_caches"]),
        "cache_probe_scope": str(cache_probe["cache_probe_scope"]),
        "should_prompt_prototype_2016_ensemble_policy": bool(
            str(entry.get("entry_id") or "") == "prototype_legacy_bundle"
            and str(entry.get("workflow_mode") or "") == "prototype_2016"
        ),
    }


def audit_launcher_startup_prompt_coverage() -> list[dict[str, Any]]:
    matrix = _load_launcher_matrix()
    audit_rows: list[dict[str, Any]] = []
    for entry in matrix.get("entries") or []:
        entry_id = str(entry.get("entry_id") or "")
        phases = [
            str(step.get("phase") or "")
            for step in (entry.get("steps") or [])
            if str(step.get("service") or "").strip().lower() == "pipeline"
        ]
        promptable_pipeline_phases = [phase for phase in phases if phase_uses_startup_prompts(phase, role="pipeline")]
        entry_probe = build_launcher_entry_probe(entry_id)
        audit_rows.append(
            {
                "entry_id": entry_id,
                "category_id": str(entry.get("category_id") or ""),
                "workflow_mode": str(entry.get("workflow_mode") or ""),
                "pipeline_phases": phases,
                "promptable_pipeline_phases": promptable_pipeline_phases,
                "has_promptable_pipeline_phase": bool(promptable_pipeline_phases),
                "should_prompt_wait_budget": bool(entry_probe["should_prompt_wait_budget"]),
                "has_eligible_input_cache": bool(entry_probe["has_eligible_input_cache"]),
            }
        )
    return audit_rows


def _prompt_wait_budget(
    *,
    input_func: Callable[[str], str],
    output,
) -> int:
    output.write("\n")
    output.write("Choose one forcing wait budget for this run:\n")
    output.write("  1. 120 seconds\n")
    output.write("  2. 300 seconds (Recommended)\n")
    output.write("  3. 600 seconds\n")
    output.write("  4. 0 seconds (no hard cap)\n")
    while True:
        raw = str(input_func("Select wait budget [2]: ") or "").strip()
        if raw == "":
            return 300
        if raw in WAIT_BUDGET_PRESETS:
            return WAIT_BUDGET_PRESETS[raw]
        if raw in {"120", "300", "600", "0"}:
            return int(raw)
        output.write("Enter 1, 2, 3, or 4.\n")


def _prompt_input_cache_policy(
    *,
    input_func: Callable[[str], str],
    output,
) -> str:
    output.write("\n")
    output.write("Eligible local input data already exists for this run.\n")
    output.write("  1. Reuse validated local inputs when available (Recommended)\n")
    output.write("  2. Force refresh remote inputs\n")
    while True:
        raw = str(input_func("Select input cache policy [1]: ") or "").strip().lower()
        if raw == "":
            return INPUT_CACHE_POLICY_REUSE_IF_VALID
        if raw in {"1", INPUT_CACHE_POLICY_REUSE_IF_VALID, "reuse"}:
            return INPUT_CACHE_POLICY_REUSE_IF_VALID
        if raw in {"2", INPUT_CACHE_POLICY_FORCE_REFRESH, "refresh"}:
            return INPUT_CACHE_POLICY_FORCE_REFRESH
        output.write("Enter 1 or 2.\n")


def resolve_run_startup_env(
    *,
    workflow_mode: str,
    phase: str,
    role: str | None = None,
    interactive: bool | None = None,
    input_func: Callable[[str], str] = input,
    output=None,
    apply: bool = True,
) -> dict[str, str]:
    probe = build_phase_probe(workflow_mode=workflow_mode, phase=phase, role=role)
    interactive_mode = runtime_is_interactive() if interactive is None else bool(interactive)
    output = output or sys.stdout
    state = resolve_run_startup_state(
        workflow_mode=workflow_mode,
        phase=phase,
        role=role,
        interactive=interactive_mode,
        input_func=input_func,
        output=output,
        apply=apply,
        probe=probe,
    )
    return dict(state["env"])


def resolve_run_startup_state(
    *,
    workflow_mode: str,
    phase: str,
    role: str | None = None,
    interactive: bool | None = None,
    input_func: Callable[[str], str] = input,
    output=None,
    apply: bool = True,
    probe: dict[str, Any] | None = None,
) -> dict[str, Any]:
    probe = probe or build_phase_probe(workflow_mode=workflow_mode, phase=phase, role=role)
    interactive_mode = runtime_is_interactive() if interactive is None else bool(interactive)
    output = output or sys.stdout

    explicit_budget = os.environ.get(FORCING_SOURCE_BUDGET_SECONDS_ENV)
    if explicit_budget not in ("", None):
        resolved_budget = str(resolve_forcing_source_budget_seconds(explicit_budget))
        budget_source = STARTUP_VALUE_SOURCE_EXPLICIT_ENV
    elif interactive_mode and probe["should_prompt_wait_budget"]:
        resolved_budget = str(_prompt_wait_budget(input_func=input_func, output=output))
        budget_source = STARTUP_VALUE_SOURCE_INTERACTIVE_PROMPT
    elif probe["should_prompt_wait_budget"]:
        resolved_budget = str(FORCING_SOURCE_BUDGET_SECONDS_DEFAULT)
        budget_source = STARTUP_VALUE_SOURCE_NON_INTERACTIVE_DEFAULT
    else:
        resolved_budget = str(FORCING_SOURCE_BUDGET_SECONDS_DEFAULT)
        budget_source = STARTUP_VALUE_SOURCE_DEFAULT_NON_PROMPTABLE

    explicit_cache_policy = normalize_input_cache_policy(os.environ.get(INPUT_CACHE_POLICY_ENV))
    if explicit_cache_policy != INPUT_CACHE_POLICY_DEFAULT:
        resolved_cache_policy = explicit_cache_policy
        cache_policy_source = STARTUP_VALUE_SOURCE_EXPLICIT_ENV
    elif _env_flag_enabled(PREP_FORCE_REFRESH_ENV):
        resolved_cache_policy = INPUT_CACHE_POLICY_FORCE_REFRESH
        cache_policy_source = STARTUP_VALUE_SOURCE_PREP_FORCE_REFRESH_ALIAS
    elif interactive_mode and probe["has_eligible_input_cache"]:
        resolved_cache_policy = _prompt_input_cache_policy(input_func=input_func, output=output)
        cache_policy_source = STARTUP_VALUE_SOURCE_INTERACTIVE_PROMPT
    elif probe["has_eligible_input_cache"]:
        resolved_cache_policy = INPUT_CACHE_POLICY_REUSE_IF_VALID
        cache_policy_source = STARTUP_VALUE_SOURCE_NON_INTERACTIVE_DEFAULT
    elif probe["should_prompt_wait_budget"]:
        resolved_cache_policy = INPUT_CACHE_POLICY_REUSE_IF_VALID
        cache_policy_source = STARTUP_VALUE_SOURCE_DEFAULT_NO_ELIGIBLE_CACHE
    else:
        resolved_cache_policy = INPUT_CACHE_POLICY_REUSE_IF_VALID
        cache_policy_source = STARTUP_VALUE_SOURCE_DEFAULT_NON_PROMPTABLE

    resolved_env = {
        FORCING_SOURCE_BUDGET_SECONDS_ENV: str(resolved_budget),
        INPUT_CACHE_POLICY_ENV: str(resolved_cache_policy),
        RUN_STARTUP_PROMPTS_RESOLVED_ENV: "1",
        RUN_STARTUP_TOKEN_ENV: str(
            os.environ.get(RUN_STARTUP_TOKEN_ENV)
            or f"startup_{os.getpid()}_{int(time.time())}"
        ),
    }
    if apply:
        for key, value in resolved_env.items():
            os.environ[key] = str(value)

    prompting_skipped_reason = None
    if probe["should_prompt_wait_budget"] and not interactive_mode:
        prompting_skipped_reason = "non_interactive_runtime"

    return {
        "env": resolved_env,
        "probe": dict(probe),
        "workflow_mode": str(workflow_mode),
        "phase": str(phase),
        "interactive_mode": interactive_mode,
        "forcing_source_budget_seconds": str(resolved_budget),
        "input_cache_policy": str(resolved_cache_policy),
        "wait_budget_source": budget_source,
        "input_cache_policy_source": cache_policy_source,
        "prompting_skipped_reason": prompting_skipped_reason,
        "matching_launcher_entry_id": find_matching_launcher_entry_id(
            workflow_mode=workflow_mode,
            phase=phase,
            role=role,
        ),
    }


def _emit_probe_payload(payload: dict[str, Any]) -> int:
    print(f"{STARTUP_PROMPT_PROBE_PREFIX}{json.dumps(payload, sort_keys=True)}")
    return 0


def main(argv: Iterable[str] | None = None) -> int:
    args = list(argv if argv is not None else sys.argv[1:])
    if not args:
        raise SystemExit(
            "Usage: python -m src.utils.startup_prompt_policy "
            "--probe-launcher-entry <entry_id> | --probe-current-phase"
        )
    if args[0] == "--probe-launcher-entry":
        if len(args) != 2:
            raise SystemExit("--probe-launcher-entry requires an entry id.")
        return _emit_probe_payload(build_launcher_entry_probe(args[1]))
    if args[0] == "--probe-current-phase":
        workflow_mode = str(os.environ.get("WORKFLOW_MODE") or "prototype_2021")
        phase = str(os.environ.get("PIPELINE_PHASE") or "1_2")
        role = str(os.environ.get("PIPELINE_ROLE") or "")
        return _emit_probe_payload(build_phase_probe(workflow_mode=workflow_mode, phase=phase, role=role))
    raise SystemExit(f"Unknown startup-prompt helper command: {args[0]}")


if __name__ == "__main__":
    raise SystemExit(main())
