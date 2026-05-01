"""Validate the data-source provenance registry.

This audit is intentionally read-only. It does not fetch external services,
rerun models, or mutate stored scientific outputs.
"""

from __future__ import annotations

import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

try:
    import yaml
except ModuleNotFoundError:  # pragma: no cover - depends on local environment
    yaml = None


REPO_ROOT = Path(__file__).resolve().parents[1]
REGISTRY_PATH = REPO_ROOT / "config" / "data_sources.yaml"

REQUIRED_FIELDS = (
    "source_id",
    "label",
    "provider",
    "product_or_layer",
    "role",
    "evidence_boundary",
    "workflow_lanes",
    "time_period_used",
    "repo_manifests",
    "official_link_if_known",
    "access_caveats",
    "secrets_required",
    "stored_in_repo",
)

ALLOWED_ROLES = {
    "observation validation mask",
    "transport validation",
    "forcing input",
    "shoreline support",
    "oil-property support",
    "model/tool provenance",
    "ui/review tool",
}

ALLOWED_STORAGE = {
    "full",
    "partial",
    "derived_only",
    "configs_manifests_only",
}

REQUIRED_SOURCE_IDS = {
    "mindoro_noaa_nesdis_march13_14_public_arcgis",
    "dwh_public_daily_observation_masks",
    "noaa_osmc_global_drifter_program_drifter_6hour_qc",
    "copernicus_cmems_currents",
    "noaa_ncep_gfs_winds",
    "hycom_gofs_currents",
    "ecmwf_era5_winds",
    "copernicus_cmems_wave_stokes",
    "gshhg_shoreline",
    "noaa_adios_oillibrary",
    "opendrift_openoil",
    "pygnome_gnome",
    "streamlit_review_ui",
}

SECRET_PATTERNS = (
    re.compile(r"(?i)(password|api[_-]?key|secret|token)\s*[:=]\s*['\"]?[A-Za-z0-9_./+=-]{8,}"),
    re.compile(r"(?i)bearer\s+[A-Za-z0-9._-]{10,}"),
    re.compile(r"(?i)copernicusmarine\s+login\s+\S+\s+\S+"),
)

MISSING_MARKERS = (
    "optional_missing:",
    "missing_optional:",
    "placeholder_missing:",
    "not_stored:",
    "not_present:",
    "not stored:",
    "not present:",
)

_TRACKED_PATHS_CACHE: set[str] | None = None


def _flatten(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, dict):
        return " ".join(f"{key} {_flatten(item)}" for key, item in value.items())
    if isinstance(value, (list, tuple, set)):
        return " ".join(_flatten(item) for item in value)
    return str(value)


def _parse_simple_yaml_scalar(raw_value: str) -> Any:
    text = raw_value.strip()
    if text in {"", '""', "''"}:
        return ""
    if (text.startswith('"') and text.endswith('"')) or (text.startswith("'") and text.endswith("'")):
        if text.startswith('"'):
            try:
                return json.loads(text)
            except json.JSONDecodeError:
                return text[1:-1]
        return text[1:-1]
    if text.lower() in {"true", "false"}:
        return text.lower() == "true"
    return text


def _parse_simple_yaml_mapping(text: str) -> dict[str, Any]:
    """Parse the simple registry YAML shape when PyYAML is unavailable."""

    root: dict[str, Any] = {}
    current_entry: dict[str, Any] | None = None
    current_list_key = ""
    for raw_line in text.splitlines():
        if not raw_line.strip() or raw_line.lstrip().startswith("#"):
            continue
        indent = len(raw_line) - len(raw_line.lstrip(" "))
        stripped = raw_line.strip()
        if indent == 0 and stripped.endswith(":"):
            key = stripped[:-1].strip()
            root[key] = {}
            current_entry = root[key]
            current_list_key = ""
            continue
        if indent == 2 and current_entry is not None and ":" in stripped:
            key, raw_value = stripped.split(":", 1)
            key = key.strip()
            raw_value = raw_value.strip()
            if raw_value == "":
                current_entry[key] = []
                current_list_key = key
            else:
                current_entry[key] = _parse_simple_yaml_scalar(raw_value)
                current_list_key = ""
            continue
        if indent >= 4 and stripped.startswith("- ") and current_entry is not None and current_list_key:
            current_entry.setdefault(current_list_key, []).append(_parse_simple_yaml_scalar(stripped[2:]))
    return root


def _load_registry() -> tuple[dict[str, Any], str]:
    if not REGISTRY_PATH.exists():
        raise RuntimeError(f"Missing registry: {REGISTRY_PATH.relative_to(REPO_ROOT)}")

    text = REGISTRY_PATH.read_text(encoding="utf-8")
    if yaml is not None:
        payload = yaml.safe_load(text) or {}
    else:
        payload = _parse_simple_yaml_mapping(text)

    if not isinstance(payload, dict):
        raise RuntimeError("config/data_sources.yaml must contain a top-level mapping.")
    return payload, text


def _source_entries(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    source_map = payload.get("sources") if isinstance(payload.get("sources"), dict) else payload
    entries: dict[str, dict[str, Any]] = {}
    for key, value in source_map.items():
        if isinstance(value, dict):
            entries[str(key)] = value
    return entries


def _as_list(value: Any) -> list[Any]:
    if value in (None, ""):
        return []
    if isinstance(value, list):
        return value
    return [value]


def _is_optional_missing_manifest(value: str) -> bool:
    lowered = value.strip().lower()
    return any(lowered.startswith(marker) for marker in MISSING_MARKERS)


def _manifest_path_from_value(value: str) -> str:
    text = value.strip()
    lowered = text.lower()
    for marker in MISSING_MARKERS:
        if lowered.startswith(marker):
            remainder = text.split(":", 1)[1].strip()
            return remainder.split(" - ", 1)[0].strip()
    return text.split(" - ", 1)[0].strip()


def _tracked_repo_paths() -> set[str]:
    global _TRACKED_PATHS_CACHE
    if _TRACKED_PATHS_CACHE is not None:
        return _TRACKED_PATHS_CACHE

    output = subprocess.check_output(
        ["git", "ls-files", "-z"],
        cwd=REPO_ROOT,
        encoding="utf-8",
        errors="replace",
    )
    _TRACKED_PATHS_CACHE = {item.replace("\\", "/") for item in output.split("\0") if item}
    return _TRACKED_PATHS_CACHE


def _tracked_path_exists(path_text: str) -> bool:
    normalized = path_text.replace("\\", "/").strip().rstrip("/")
    tracked_paths = _tracked_repo_paths()
    if normalized in tracked_paths:
        return True
    prefix = normalized + "/"
    return any(path.startswith(prefix) for path in tracked_paths)


def _validate_manifest_paths(entry_id: str, entry: dict[str, Any], problems: list[str]) -> None:
    manifests = _as_list(entry.get("repo_manifests"))
    if not manifests:
        return
    for raw_value in manifests:
        value = str(raw_value).strip()
        if not value:
            problems.append(f"{entry_id}: repo_manifests contains an empty path.")
            continue

        optional_missing = _is_optional_missing_manifest(value)
        manifest_path = _manifest_path_from_value(value)
        if not manifest_path:
            problems.append(f"{entry_id}: optional/missing repo_manifest marker lacks a path.")
            continue

        if re.match(r"^[a-z]+://", manifest_path, flags=re.IGNORECASE):
            problems.append(f"{entry_id}: repo_manifests should contain local repo paths, not URLs: {manifest_path}")
            continue

        candidate = Path(manifest_path.replace("\\", "/"))
        if candidate.is_absolute() or ".." in candidate.parts:
            problems.append(f"{entry_id}: repo_manifest path escapes the repository: {manifest_path}")
            continue

        if _tracked_path_exists(manifest_path):
            continue
        if optional_missing and any(word in value.lower() for word in ("missing", "optional", "not stored", "not present")):
            continue
        problems.append(
            f"{entry_id}: repo_manifest path does not exist and is not marked optional/missing honestly: {manifest_path}"
        )


def _validate_claim_boundaries(entry_id: str, entry: dict[str, Any], problems: list[str]) -> None:
    role = str(entry.get("role") or "").strip().lower()
    label_provider_product = " ".join(
        str(entry.get(field) or "").lower()
        for field in ("label", "provider", "product_or_layer")
    )
    boundary = str(entry.get("evidence_boundary") or "").lower()
    all_text = _flatten(entry).lower()

    if role == "observation validation mask" and any(term in label_provider_product for term in ("opendrift", "openoil", "pygnome", "gnome")):
        problems.append(f"{entry_id}: model/tool source is assigned observation validation mask role.")

    if role in {"model/tool provenance", "ui/review tool"}:
        positive_reference_phrases = (
            "is an observation reference",
            "as observation reference",
            "public-observation validation reference",
            "observation validation mask",
            "validation target",
        )
        if any(phrase in boundary for phrase in positive_reference_phrases):
            if not any(qualifier in boundary for qualifier in ("not ", "never ", "does not ")):
                problems.append(f"{entry_id}: model/tool boundary implies validation-reference status.")

    direct_bad_patterns = (
        "model output is observation",
        "model output as observation",
        "forecast is observation",
        "pygnome is observation",
        "opendrift is observation",
        "model output observation reference",
    )
    if any(pattern in all_text for pattern in direct_bad_patterns):
        problems.append(f"{entry_id}: text implies a model output is an observation reference.")

    if entry_id == "pygnome_gnome" and "comparator-only" not in all_text:
        problems.append("pygnome_gnome: PyGNOME must be explicitly marked comparator-only.")

    if entry_id == "dwh_public_daily_observation_masks" and "not mindoro recalibration" not in all_text:
        problems.append("dwh_public_daily_observation_masks: DWH must be marked as not Mindoro recalibration.")


def _validate_required_fields(entry_key: str, entry: dict[str, Any], problems: list[str]) -> None:
    entry_id = str(entry.get("source_id") or entry_key).strip()
    if entry_id != entry_key:
        problems.append(f"{entry_key}: source_id `{entry_id}` must match the registry key.")

    for field in REQUIRED_FIELDS:
        if field not in entry:
            problems.append(f"{entry_id}: missing required field `{field}`.")
            continue
        value = entry.get(field)
        if value in (None, "", []):
            problems.append(f"{entry_id}: required field `{field}` is empty.")

    role = str(entry.get("role") or "").strip()
    if role and role.lower() not in ALLOWED_ROLES:
        problems.append(f"{entry_id}: role `{role}` is not allowed.")

    storage = str(entry.get("stored_in_repo") or "").strip()
    if storage and storage not in ALLOWED_STORAGE:
        problems.append(f"{entry_id}: stored_in_repo `{storage}` is not allowed.")

    if not isinstance(entry.get("secrets_required"), bool):
        problems.append(f"{entry_id}: secrets_required must be true or false.")

    for list_field in ("workflow_lanes", "repo_manifests"):
        if list_field in entry and not isinstance(entry.get(list_field), list):
            problems.append(f"{entry_id}: `{list_field}` must be a list.")


def _validate_secret_scan(raw_text: str, problems: list[str]) -> None:
    for pattern in SECRET_PATTERNS:
        match = pattern.search(raw_text)
        if match:
            excerpt = match.group(0).splitlines()[0][:80]
            problems.append(f"possible secret-like value found in registry: {excerpt}")


def validate_registry() -> list[str]:
    problems: list[str] = []
    try:
        payload, raw_text = _load_registry()
    except RuntimeError as exc:
        return [str(exc)]

    _validate_secret_scan(raw_text, problems)

    entries = _source_entries(payload)
    if not entries:
        problems.append("config/data_sources.yaml contains no source entries.")
        return problems

    missing_source_ids = sorted(REQUIRED_SOURCE_IDS - set(entries))
    for source_id in missing_source_ids:
        problems.append(f"missing required source group `{source_id}`.")

    for entry_key, entry in entries.items():
        entry_id = str(entry.get("source_id") or entry_key)
        _validate_required_fields(entry_key, entry, problems)
        _validate_claim_boundaries(entry_id, entry, problems)
        _validate_manifest_paths(entry_id, entry, problems)

    return problems


def main() -> int:
    problems = validate_registry()
    if problems:
        print("Data-source provenance registry validation FAILED.")
        for problem in problems:
            print(f"- {problem}")
        return 1

    payload, _raw_text = _load_registry()
    entries = _source_entries(payload)
    role_counts: dict[str, int] = {}
    storage_counts: dict[str, int] = {}
    for entry in entries.values():
        role = str(entry.get("role") or "")
        storage = str(entry.get("stored_in_repo") or "")
        role_counts[role] = role_counts.get(role, 0) + 1
        storage_counts[storage] = storage_counts.get(storage, 0) + 1

    print("Data-source provenance registry validation passed.")
    print(f"- Registry: {REGISTRY_PATH.relative_to(REPO_ROOT)}")
    print(f"- Sources registered: {len(entries)}")
    print("- Role counts:")
    for role in sorted(role_counts):
        print(f"  - {role}: {role_counts[role]}")
    print("- Storage counts:")
    for storage in sorted(storage_counts):
        print(f"  - {storage}: {storage_counts[storage]}")
    print("- Required fields, claim boundaries, secret scan, and tracked manifest paths are valid.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
