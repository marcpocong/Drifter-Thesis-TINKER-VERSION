"""Validate the final-paper paper-to-output registry.

This is a stored-output/config/doc audit. It does not launch Docker, import the
science package, or run any scientific workflow.
"""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
REGISTRY_PATH = ROOT / "config" / "paper_to_output_registry.yaml"
OUTPUT_DIR = ROOT / "output" / "paper_to_output_registry_validation"
JSON_REPORT = OUTPUT_DIR / "paper_to_output_registry_validation.json"
MD_REPORT = OUTPUT_DIR / "paper_to_output_registry_validation.md"

REQUIRED_FIELDS = {
    "paper_item_id",
    "paper_label",
    "evidence_role",
    "claim_boundary",
    "repo_paths",
    "source_config_paths",
    "output_exists",
    "validation_method",
    "notes",
}

ALLOWED_ROLES = {
    "primary_evidence",
    "comparator_support",
    "external_transfer",
    "support_context",
    "secondary_support",
    "archive_provenance",
    "governance",
}


def _long_path(path: Path) -> str:
    """Return a Windows long-path-safe absolute path string."""
    absolute = str(path.resolve())
    if os.name != "nt":
        return absolute
    if absolute.startswith("\\\\?\\"):
        return absolute
    return "\\\\?\\" + absolute


def _direct_exists(path: Path) -> bool:
    return path.exists() or os.path.exists(_long_path(path))


def _has_glob_magic(pattern: str) -> bool:
    return any(char in pattern for char in "*?[")


def _match_repo_pattern(pattern: str) -> list[str]:
    normalized = pattern.replace("\\", "/")
    if not _has_glob_magic(normalized):
        candidate = ROOT / normalized
        return [normalized] if _direct_exists(candidate) else []

    try:
        matches = list(ROOT.glob(normalized))
    except (OSError, ValueError):
        matches = []

    rel_matches: list[str] = []
    for match in matches:
        try:
            rel_matches.append(match.relative_to(ROOT).as_posix())
        except ValueError:
            rel_matches.append(match.as_posix())
    return sorted(set(rel_matches))


def _load_registry(path: Path) -> dict[str, Any]:
    text = path.read_text(encoding="utf-8")
    try:
        import yaml  # type: ignore

        data = yaml.safe_load(text)
    except ModuleNotFoundError:
        # The committed registry is JSON-compatible YAML, so stdlib JSON is
        # enough in environments without PyYAML.
        data = json.loads(text)
    except Exception:
        # If PyYAML is installed but cannot parse, still try the JSON-compatible
        # path before reporting a parse failure.
        data = json.loads(text)

    if not isinstance(data, dict):
        raise ValueError("registry root must be a mapping")
    return data


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _validate_entry(entry: dict[str, Any]) -> dict[str, Any]:
    item_id = str(entry.get("paper_item_id", "<missing>"))
    role = entry.get("evidence_role")
    errors: list[str] = []
    warnings: list[str] = []

    missing_fields = sorted(REQUIRED_FIELDS - set(entry))
    if missing_fields:
        errors.append(f"missing required field(s): {', '.join(missing_fields)}")

    if role not in ALLOWED_ROLES:
        errors.append(f"invalid evidence_role: {role!r}")

    repo_paths = _as_list(entry.get("repo_paths"))
    required_repo_paths = set(str(path) for path in _as_list(entry.get("required_repo_paths")))
    source_config_paths = _as_list(entry.get("source_config_paths"))

    if not repo_paths:
        errors.append("repo_paths must be a non-empty list")

    path_results: list[dict[str, Any]] = []
    matched_any = False
    missing_repo_paths: list[str] = []
    missing_required_repo_paths: list[str] = []

    for raw_path in repo_paths:
        pattern = str(raw_path)
        matches = _match_repo_pattern(pattern)
        exists = bool(matches)
        matched_any = matched_any or exists
        if not exists:
            missing_repo_paths.append(pattern)
            if pattern in required_repo_paths:
                if role == "primary_evidence":
                    missing_required_repo_paths.append(pattern)
                else:
                    warnings.append(
                        f"required_repo_path missing outside primary_evidence: {pattern}"
                    )
        path_results.append(
            {
                "pattern": pattern,
                "exists": exists,
                "match_count": len(matches),
                "matches": matches[:25],
                "truncated": len(matches) > 25,
            }
        )

    if missing_required_repo_paths:
        errors.append(
            "missing required primary evidence repo path(s): "
            + ", ".join(missing_required_repo_paths)
        )

    for missing in missing_repo_paths:
        if missing not in required_repo_paths:
            warnings.append(f"optional or placeholder repo path missing: {missing}")

    if entry.get("output_exists") is True and not matched_any:
        warnings.append("declared output_exists=true but no repo_paths matched")
    if entry.get("output_exists") is False and matched_any:
        warnings.append("declared output_exists=false but one or more repo_paths matched")

    source_config_results: list[dict[str, Any]] = []
    for raw_path in source_config_paths:
        path_text = str(raw_path)
        exists = bool(_match_repo_pattern(path_text))
        if not exists:
            warnings.append(f"source_config_path missing: {path_text}")
        source_config_results.append({"path": path_text, "exists": exists})

    return {
        "paper_item_id": item_id,
        "paper_label": entry.get("paper_label"),
        "evidence_role": role,
        "declared_output_exists": entry.get("output_exists"),
        "matched_any_repo_path": matched_any,
        "repo_path_results": path_results,
        "source_config_results": source_config_results,
        "missing_repo_paths": missing_repo_paths,
        "missing_required_repo_paths": missing_required_repo_paths,
        "errors": errors,
        "warnings": warnings,
        "status": "FAIL" if errors else "PASS",
    }


def _write_markdown(report: dict[str, Any]) -> None:
    lines = [
        "# Paper-To-Output Registry Validation",
        "",
        f"- Registry: `{REGISTRY_PATH.relative_to(ROOT).as_posix()}`",
        f"- Generated UTC: `{report['generated_utc']}`",
        f"- Overall status: `{report['overall_status']}`",
        f"- Entries checked: `{report['summary']['entries_checked']}`",
        f"- Errors: `{report['summary']['error_count']}`",
        f"- Warnings: `{report['summary']['warning_count']}`",
        "",
        "This validation checks stored paths only. It does not run scientific workflows.",
        "",
        "## Entry Results",
        "",
        "| Item | Role | Status | Missing repo paths | Notes |",
        "| --- | --- | --- | ---: | --- |",
    ]

    for entry in report["entries"]:
        notes = []
        if entry["errors"]:
            notes.append("Errors: " + "; ".join(entry["errors"]))
        if entry["warnings"]:
            notes.append("Warnings: " + "; ".join(entry["warnings"][:4]))
            if len(entry["warnings"]) > 4:
                notes.append(f"{len(entry['warnings']) - 4} more warning(s)")
        note_text = "<br>".join(notes) if notes else "OK"
        lines.append(
            "| `{}` | `{}` | `{}` | `{}` | {} |".format(
                entry["paper_item_id"],
                entry["evidence_role"],
                entry["status"],
                len(entry["missing_repo_paths"]),
                note_text.replace("|", "\\|"),
            )
        )

    if report["errors"]:
        lines.extend(["", "## Errors", ""])
        for error in report["errors"]:
            lines.append(f"- {error}")

    if report["warnings"]:
        lines.extend(["", "## Warnings", ""])
        for warning in report["warnings"]:
            lines.append(f"- {warning}")

    MD_REPORT.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    registry = _load_registry(REGISTRY_PATH)
    entries = registry.get("entries")
    if not isinstance(entries, list):
        raise ValueError("registry must contain an entries list")

    entry_reports = []
    errors: list[str] = []
    warnings: list[str] = []

    seen_ids: set[str] = set()
    for raw_entry in entries:
        if not isinstance(raw_entry, dict):
            errors.append("registry entry is not a mapping")
            continue
        item_id = str(raw_entry.get("paper_item_id", "<missing>"))
        if item_id in seen_ids:
            errors.append(f"duplicate paper_item_id: {item_id}")
        seen_ids.add(item_id)

        entry_report = _validate_entry(raw_entry)
        entry_reports.append(entry_report)
        for error in entry_report["errors"]:
            errors.append(f"{entry_report['paper_item_id']}: {error}")
        for warning in entry_report["warnings"]:
            warnings.append(f"{entry_report['paper_item_id']}: {warning}")

    report = {
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "registry_path": REGISTRY_PATH.relative_to(ROOT).as_posix(),
        "overall_status": "FAIL" if errors else "PASS",
        "summary": {
            "entries_checked": len(entry_reports),
            "entry_failures": sum(1 for entry in entry_reports if entry["status"] == "FAIL"),
            "error_count": len(errors),
            "warning_count": len(warnings),
        },
        "errors": errors,
        "warnings": warnings,
        "entries": entry_reports,
    }

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    JSON_REPORT.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    _write_markdown(report)

    print(f"Paper-to-output registry validation: {report['overall_status']}")
    print(f"Entries checked: {report['summary']['entries_checked']}")
    print(f"Errors: {report['summary']['error_count']}")
    print(f"Warnings: {report['summary']['warning_count']}")
    print(f"Wrote {JSON_REPORT.relative_to(ROOT)}")
    print(f"Wrote {MD_REPORT.relative_to(ROOT)}")

    return 1 if errors else 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"Registry validation failed before report generation: {exc}", file=sys.stderr)
        raise SystemExit(2)
