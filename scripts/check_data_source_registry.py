"""Validate the panel-facing data-source provenance registry.

This checker audits metadata only. It does not fetch external data, rerun
models, recompute scorecards, or mutate stored scientific outputs.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Any

try:
    import yaml
except ModuleNotFoundError:  # pragma: no cover - environment-dependent guard
    yaml = None


REPO_ROOT = Path(__file__).resolve().parents[1]
DOC_PATH = REPO_ROOT / "docs" / "DATA_SOURCES.md"
REGISTRY_CANDIDATES = (
    REPO_ROOT / "config" / "data_sources.yaml",
    REPO_ROOT / "docs" / "data_sources_registry.yaml",
)

ALLOWED_CATEGORIES = {
    "observation_truth",
    "transport_validation",
    "ocean_current_forcing",
    "wind_forcing",
    "wave_forcing",
    "shoreline_geography",
    "oil_property",
    "model_tool",
    "support_reference",
}

ALLOWED_STATUSES = {
    "confirmed_in_repo",
    "confirmed_from_manifest",
    "listed_from_manuscript",
    "needs_exact_url_verification",
}

REQUIRED_FIELDS = ("id", "provider", "category", "evidence_role", "used_in_workflows")

SECRET_PATTERNS = (
    re.compile(r"(?i)(password|api[_-]?key|secret|token)\s*[:=]\s*['\"]?[A-Za-z0-9_./+=-]{8,}"),
    re.compile(r"(?i)bearer\s+[A-Za-z0-9._-]{10,}"),
)


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
        return text[1:-1]
    if text.lower() in {"true", "false"}:
        return text.lower() == "true"
    return text


def _parse_simple_yaml_mapping(text: str) -> dict[str, Any]:
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


def _load_registry(path: Path) -> dict[str, Any]:
    text = path.read_text(encoding="utf-8")
    payload = _parse_simple_yaml_mapping(text) if yaml is None else (yaml.safe_load(text) or {})
    if not isinstance(payload, dict):
        raise RuntimeError(f"{path.relative_to(REPO_ROOT)} is not a mapping.")
    return payload


def _source_entries(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    source_map = payload.get("sources") if isinstance(payload.get("sources"), dict) else payload
    entries: dict[str, dict[str, Any]] = {}
    for key, value in source_map.items():
        if isinstance(value, dict):
            entries[str(key)] = value
    return entries


def _has_missing_url_explanation(entry: dict[str, Any]) -> bool:
    status = str(entry.get("status", "")).strip()
    if status != "needs_exact_url_verification":
        return False
    explanation = _flatten([entry.get("access_endpoint_or_item_url"), entry.get("caveats")]).lower()
    return "exact" in explanation and ("not found" in explanation or "verify" in explanation)


def _matches_family(entry: dict[str, Any], family: str) -> bool:
    category = str(entry.get("category", "")).strip()
    text = _flatten(entry).lower()
    if family == "drifters":
        return "drifter" in text
    if family == "currents":
        return category == "ocean_current_forcing"
    if family == "winds":
        return category == "wind_forcing"
    if family == "public observation masks":
        return category == "observation_truth" and ("mask" in text or "spill extent" in text)
    if family == "shoreline":
        return category == "shoreline_geography"
    if family == "oil-type":
        return category == "oil_property"
    if family == "model tools":
        return category == "model_tool"
    return False


def main() -> int:
    problems: list[str] = []

    if not DOC_PATH.exists():
        problems.append("docs/DATA_SOURCES.md is missing.")

    registry_path = next((path for path in REGISTRY_CANDIDATES if path.exists()), None)
    if registry_path is None:
        problems.append("No data-source registry found at config/data_sources.yaml or docs/data_sources_registry.yaml.")
        registry_payload: dict[str, Any] = {}
    else:
        try:
            registry_payload = _load_registry(registry_path)
        except RuntimeError as exc:
            problems.append(str(exc))
            registry_payload = {}

    entries = _source_entries(registry_payload)
    if not entries:
        problems.append("The registry has no source entries.")

    for key, entry in entries.items():
        entry_id = str(entry.get("id") or key)
        for field in REQUIRED_FIELDS:
            value = entry.get(field)
            if value in (None, "", []):
                problems.append(f"{entry_id}: missing required field `{field}`.")

        category = str(entry.get("category", "")).strip()
        if category and category not in ALLOWED_CATEGORIES:
            problems.append(f"{entry_id}: category `{category}` is not in the allowed category list.")

        status = str(entry.get("status", "")).strip()
        if status not in ALLOWED_STATUSES:
            problems.append(f"{entry_id}: status `{status or '<missing>'}` is not allowed.")

        if not str(entry.get("official_url", "")).strip() and not _has_missing_url_explanation(entry):
            problems.append(
                f"{entry_id}: official_url is missing and status/caveats do not explain exact URL verification."
            )

        flattened = _flatten(entry)
        for pattern in SECRET_PATTERNS:
            if pattern.search(flattened):
                problems.append(f"{entry_id}: possible secret or token-like value found.")

    required_families = (
        "drifters",
        "currents",
        "winds",
        "public observation masks",
        "shoreline",
        "oil-type",
        "model tools",
    )
    for family in required_families:
        if not any(_matches_family(entry, family) for entry in entries.values()):
            problems.append(f"Missing at least one registry entry for {family}.")

    if problems:
        print("Data-source provenance registry check FAILED.")
        for problem in problems:
            print(f"- {problem}")
        return 1

    category_counts: dict[str, int] = {}
    for entry in entries.values():
        category = str(entry.get("category", "")).strip()
        category_counts[category] = category_counts.get(category, 0) + 1

    print("Data-source provenance registry check passed.")
    print(f"- Documentation: {DOC_PATH.relative_to(REPO_ROOT)}")
    print(f"- Registry: {registry_path.relative_to(REPO_ROOT) if registry_path else 'missing'}")
    print(f"- Sources registered: {len(entries)}")
    print("- Category counts:")
    for category in sorted(category_counts):
        print(f"  - {category}: {category_counts[category]}")
    print("- No secrets or token-like values were found by the metadata scan.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
