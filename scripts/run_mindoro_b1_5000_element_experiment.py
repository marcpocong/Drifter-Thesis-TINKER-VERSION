from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import traceback
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
RUN_NAME = "CASE_MINDORO_RETRO_2023"
CANONICAL_OUTPUT_DIR_NAME = "phase3b_extended_public_scored_march13_14_reinit"
EXPERIMENT_OUTPUT_DIR_NAME = "phase3b_extended_public_scored_march13_14_reinit_5000_experiment"
SENSITIVITY_OUTPUT_DIR_NAME = "phase3b_march13_14_element_count_sensitivity"
CANONICAL_REQUESTED_ELEMENT_COUNT = 100000
EXPERIMENT_REQUESTED_ELEMENT_COUNT = 5000
EXPECTED_ENSEMBLE_MEMBER_COUNT = 50

CASE_OUTPUT_DIR = REPO_ROOT / "output" / RUN_NAME
CANONICAL_OUTPUT_DIR = CASE_OUTPUT_DIR / CANONICAL_OUTPUT_DIR_NAME
EXPERIMENT_OUTPUT_DIR = CASE_OUTPUT_DIR / EXPERIMENT_OUTPUT_DIR_NAME
SENSITIVITY_OUTPUT_DIR = CASE_OUTPUT_DIR / SENSITIVITY_OUTPUT_DIR_NAME
CANONICAL_FORCING_DIR = CANONICAL_OUTPUT_DIR / "forcing"
EXPERIMENT_PERSISTENT_FORCING_DIR = (
    REPO_ROOT / "data" / "local_input_store" / RUN_NAME / EXPERIMENT_OUTPUT_DIR_NAME / "forcing"
)

EXPERIMENT_TRACK = "mindoro_phase3b_experiment_reinit_5000"
EXPERIMENT_TRACK_ID = "EXP_B1_5000_ELEMENT_COUNT"
EXPERIMENT_TRACK_LABEL = "Mindoro March 13-14 5,000-element personal experiment"
EXPERIMENT_REPORTING_ROLE = "personal_experiment_element_count_sensitivity_not_thesis_facing"
EXPERIMENT_LAUNCHER_ENTRY_ID = "phase3b_mindoro_march13_14_reinit_5000_experiment"

PROTECTED_OUTPUT_DIRS = [
    CANONICAL_OUTPUT_DIR,
    CASE_OUTPUT_DIR / "phase3b",
    CASE_OUTPUT_DIR / "phase3b_multidate_public",
    CASE_OUTPUT_DIR / "public_obs_appendix",
    REPO_ROOT / "output" / "final_validation_package",
    REPO_ROOT / "output" / "final_reproducibility_package",
    REPO_ROOT / "output" / "figure_package_publication",
]

EXPERIMENT_ENV = {
    "WORKFLOW_MODE": "mindoro_retro_2023",
    "PIPELINE_PHASE": "phase3b_extended_public_scored_march13_14_reinit",
    "FORCING_OUTAGE_POLICY": "fail_hard",
    "INPUT_CACHE_POLICY": "reuse_if_valid",
    "PHASE3B_REINIT_OUTPUT_DIR_NAME": EXPERIMENT_OUTPUT_DIR_NAME,
    "PHASE3B_REINIT_TRACK_OVERRIDE": EXPERIMENT_TRACK,
    "PHASE3B_REINIT_TRACK_ID_OVERRIDE": EXPERIMENT_TRACK_ID,
    "PHASE3B_REINIT_TRACK_LABEL_OVERRIDE": EXPERIMENT_TRACK_LABEL,
    "PHASE3B_REINIT_REPORTING_ROLE_OVERRIDE": EXPERIMENT_REPORTING_ROLE,
    "PHASE3B_REINIT_APPENDIX_ONLY": "true",
    "PHASE3B_REINIT_PRIMARY_PUBLIC_VALIDATION": "false",
    "PHASE3B_REINIT_LAUNCHER_ENTRY_ID_OVERRIDE": EXPERIMENT_LAUNCHER_ENTRY_ID,
    "PHASE3B_REINIT_REQUESTED_ELEMENT_COUNT": str(EXPERIMENT_REQUESTED_ELEMENT_COUNT),
    "OFFICIAL_ELEMENT_COUNT_OVERRIDE": str(EXPERIMENT_REQUESTED_ELEMENT_COUNT),
    "EXTENDED_PUBLIC_FORCE_RERUN": "true",
}

COMMAND_TEXT = f"{Path(sys.executable).name} scripts/run_mindoro_b1_5000_element_experiment.py"


def _json_default(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    return value


def _write_json(path: Path, payload: dict[str, Any] | list[Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, default=_json_default)
        handle.write("\n")


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _load_json(path: Path) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle) or {}


def _relative(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(REPO_ROOT.resolve())).replace("\\", "/")
    except Exception:
        return str(path).replace("\\", "/")


def _preseed_experiment_forcing_from_canonical() -> list[str]:
    copied: list[str] = []
    if not CANONICAL_FORCING_DIR.exists():
        return copied
    EXPERIMENT_PERSISTENT_FORCING_DIR.mkdir(parents=True, exist_ok=True)
    for source_path in sorted(path for path in CANONICAL_FORCING_DIR.iterdir() if path.is_file()):
        target_path = EXPERIMENT_PERSISTENT_FORCING_DIR / source_path.name
        shutil.copy2(source_path, target_path)
        copied.append(_relative(target_path))
    return copied


def _snapshot_paths(paths: list[Path]) -> dict[str, dict[str, int]]:
    snapshot: dict[str, dict[str, int]] = {}
    for root in paths:
        if not root.exists():
            continue
        for path in sorted(item for item in root.rglob("*") if item.is_file()):
            stat = path.stat()
            snapshot[_relative(path)] = {
                "size_bytes": int(stat.st_size),
                "mtime_ns": int(stat.st_mtime_ns),
            }
    return snapshot


def _snapshot_diff(before: dict[str, Any], after: dict[str, Any]) -> dict[str, list[str]]:
    before_keys = set(before)
    after_keys = set(after)
    added = sorted(after_keys - before_keys)
    removed = sorted(before_keys - after_keys)
    changed = sorted(key for key in before_keys & after_keys if before[key] != after[key])
    return {"added": added, "removed": removed, "changed": changed}


@contextmanager
def _temporary_env(overrides: dict[str, str]):
    previous = {key: os.environ.get(key) for key in overrides}
    try:
        for key, value in overrides.items():
            os.environ[key] = str(value)
        yield
    finally:
        for key, old_value in previous.items():
            if old_value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = old_value


def _branch_manifest_paths(base_dir: Path) -> dict[str, dict[str, Path]]:
    paths: dict[str, dict[str, Path]] = {}
    for branch_id in ("R0", "R1_previous"):
        model_dir = base_dir / branch_id / "model_run"
        paths[branch_id] = {
            "forecast_manifest": model_dir / "forecast" / "forecast_manifest.json",
            "ensemble_manifest": model_dir / "ensemble" / "ensemble_manifest.json",
        }
    return paths


def _branch_manifest_details(base_dir: Path) -> dict[str, dict[str, Any]]:
    details: dict[str, dict[str, Any]] = {}
    for branch_id, paths in _branch_manifest_paths(base_dir).items():
        forecast_manifest_path = paths["forecast_manifest"]
        ensemble_manifest_path = paths["ensemble_manifest"]
        if not forecast_manifest_path.exists() and not ensemble_manifest_path.exists():
            continue
        forecast_manifest = _load_json(forecast_manifest_path) if forecast_manifest_path.exists() else {}
        ensemble_manifest = _load_json(ensemble_manifest_path) if ensemble_manifest_path.exists() else {}
        member_runs = list(ensemble_manifest.get("member_runs") or [])
        details[branch_id] = {
            "forecast_manifest_path": _relative(forecast_manifest_path) if forecast_manifest_path.exists() else "",
            "ensemble_manifest_path": _relative(ensemble_manifest_path) if ensemble_manifest_path.exists() else "",
            "requested_element_count": int(
                (ensemble_manifest.get("ensemble_configuration") or {}).get("element_count")
                or (forecast_manifest.get("ensemble") or {}).get("actual_element_count")
                or 0
            ),
            "detected_element_count": int(
                (forecast_manifest.get("ensemble") or {}).get("actual_element_count")
                or (forecast_manifest.get("deterministic_control") or {}).get("actual_element_count")
                or (ensemble_manifest.get("ensemble_configuration") or {}).get("element_count")
                or 0
            ),
            "actual_member_count": int(
                (forecast_manifest.get("ensemble") or {}).get("actual_member_count")
                or len(member_runs)
                or 0
            ),
            "base_seed": int((ensemble_manifest.get("ensemble_configuration") or {}).get("polygon_seed_random_seed") or 0),
        }
    return details


def _member_signature(ensemble_manifest_path: Path) -> list[dict[str, Any]]:
    manifest = _load_json(ensemble_manifest_path)
    signatures: list[dict[str, Any]] = []
    for row in manifest.get("member_runs") or []:
        signatures.append(
            {
                "member_id": int(row.get("member_id") or 0),
                "start_time_utc": str(row.get("start_time_utc") or ""),
                "end_time_utc": str(row.get("end_time_utc") or ""),
                "perturbation": row.get("perturbation") or {},
                "seed_initialization_random_seed": (
                    (row.get("seed_initialization") or {}).get("random_seed")
                ),
            }
        )
    return signatures


def _compare_member_perturbations(
    canonical_details: dict[str, dict[str, Any]],
    experimental_details: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    compared_branches: list[str] = []
    reasons: list[str] = []
    seed_policy_matches: list[bool] = []

    for branch_id in ("R0", "R1_previous"):
        canonical_branch = canonical_details.get(branch_id)
        experimental_branch = experimental_details.get(branch_id)
        if not canonical_branch or not experimental_branch:
            reasons.append(f"{branch_id}: missing canonical or experimental ensemble manifest.")
            continue
        canonical_manifest = REPO_ROOT / canonical_branch["ensemble_manifest_path"]
        experimental_manifest = REPO_ROOT / experimental_branch["ensemble_manifest_path"]
        if not canonical_manifest.exists() or not experimental_manifest.exists():
            reasons.append(f"{branch_id}: ensemble manifest path missing on disk.")
            continue

        canonical_signature = _member_signature(canonical_manifest)
        experimental_signature = _member_signature(experimental_manifest)
        compared_branches.append(branch_id)
        seed_policy_matches.append(
            int(canonical_branch.get("base_seed") or 0) == int(experimental_branch.get("base_seed") or 0)
        )
        if canonical_signature != experimental_signature:
            reasons.append(f"{branch_id}: member perturbation table differs from canonical.")

    if not compared_branches:
        return {
            "exact_same_member_perturbations_as_canonical": "unknown",
            "reason_if_member_identity_not_proven": "No comparable canonical and experimental ensemble manifests were both available.",
            "same_seed_policy_as_canonical": "unknown",
            "branches_compared": compared_branches,
        }

    if reasons:
        return {
            "exact_same_member_perturbations_as_canonical": False,
            "reason_if_member_identity_not_proven": " ".join(reasons),
            "same_seed_policy_as_canonical": bool(seed_policy_matches) and all(seed_policy_matches),
            "branches_compared": compared_branches,
        }

    return {
        "exact_same_member_perturbations_as_canonical": True,
        "reason_if_member_identity_not_proven": "",
        "same_seed_policy_as_canonical": bool(seed_policy_matches) and all(seed_policy_matches),
        "branches_compared": compared_branches,
    }


def _forcing_block_reasons() -> list[str]:
    manifest_path = EXPERIMENT_OUTPUT_DIR / "march13_14_reinit_forcing_window_manifest.json"
    if not manifest_path.exists():
        return []
    manifest = _load_json(manifest_path)
    reasons = []
    for row in manifest.get("rows") or []:
        if str(row.get("status") or "") == "ready":
            continue
        reasons.append(
            f"{row.get('forcing_kind', 'forcing')}: {row.get('stop_reason', '') or 'insufficient forcing coverage'}"
        )
    return reasons


def _augment_experiment_manifest(
    *,
    manifest_path: Path,
    before_snapshot_path: Path,
    after_snapshot_path: Path,
    protected_snapshot_unchanged: bool,
    protected_snapshot_diff: dict[str, list[str]],
    canonical_details: dict[str, dict[str, Any]],
    experimental_details: dict[str, dict[str, Any]],
    perturbation_info: dict[str, Any],
    preseeded_forcing_files: list[str],
) -> dict[str, Any]:
    manifest = _load_json(manifest_path)
    actual_counts = {
        branch_id: int(details.get("detected_element_count") or 0)
        for branch_id, details in experimental_details.items()
        if int(details.get("detected_element_count") or 0) > 0
    }
    member_counts = {
        branch_id: int(details.get("actual_member_count") or 0)
        for branch_id, details in experimental_details.items()
        if int(details.get("actual_member_count") or 0) > 0
    }
    consensus_actual_count = None
    if actual_counts:
        normalized = set(actual_counts.values())
        if len(normalized) == 1:
            consensus_actual_count = next(iter(normalized))
    consensus_member_count = None
    if member_counts:
        normalized = set(member_counts.values())
        if len(normalized) == 1:
            consensus_member_count = next(iter(normalized))

    manifest.update(
        {
            "thesis_facing": False,
            "reportable": False,
            "experimental_only": True,
            "purpose": "personal element-count sensitivity check",
            "not_for_publication_package": True,
            "not_for_final_validation_package": True,
            "not_for_final_reproducibility_package": True,
            "canonical_100k_output_dir": _relative(CANONICAL_OUTPUT_DIR),
            "experimental_5000_output_dir": _relative(EXPERIMENT_OUTPUT_DIR),
            "changed_parameter": "requested_element_count",
            "canonical_requested_element_count": CANONICAL_REQUESTED_ELEMENT_COUNT,
            "experimental_requested_element_count": EXPERIMENT_REQUESTED_ELEMENT_COUNT,
            "requested_element_count": EXPERIMENT_REQUESTED_ELEMENT_COUNT,
            "element_count_detected_from_manifest": consensus_actual_count,
            "element_count_detected_from_manifest_by_branch": actual_counts,
            "expected_ensemble_member_count": EXPECTED_ENSEMBLE_MEMBER_COUNT,
            "actual_member_count_detected": consensus_member_count,
            "actual_member_count_detected_by_branch": member_counts,
            "exact_same_member_perturbations_as_canonical": perturbation_info[
                "exact_same_member_perturbations_as_canonical"
            ],
            "reason_if_member_identity_not_proven": perturbation_info["reason_if_member_identity_not_proven"],
            "same_seed_policy_as_canonical": perturbation_info["same_seed_policy_as_canonical"],
            "branches_compared_for_member_identity": perturbation_info["branches_compared"],
            "seed_obs_date": "2023-03-13",
            "target_obs_date": "2023-03-14",
            "shared_imagery_caveat_preserved": True,
            "interpretation_boundary": (
                "This experiment is not a thesis-facing validation row and does not replace canonical B1."
            ),
            "canonical_outputs_modified": False,
            "primary_public_validation": False,
            "appendix_only": True,
            "artifact_class": "experimental_only",
            "tracking_metadata": {
                "output_dir_name": EXPERIMENT_OUTPUT_DIR_NAME,
                "track": EXPERIMENT_TRACK,
                "track_id": EXPERIMENT_TRACK_ID,
                "track_label": EXPERIMENT_TRACK_LABEL,
                "reporting_role": EXPERIMENT_REPORTING_ROLE,
                "launcher_entry_id_override": EXPERIMENT_LAUNCHER_ENTRY_ID,
            },
            "forcing_reuse_strategy": {
                "preseeded_from_canonical_output": bool(preseeded_forcing_files),
                "canonical_forcing_dir": _relative(CANONICAL_FORCING_DIR),
                "experiment_persistent_forcing_dir": _relative(EXPERIMENT_PERSISTENT_FORCING_DIR),
                "preseeded_files": preseeded_forcing_files,
            },
            "guardrail_snapshots": {
                "protected_outputs_snapshot_before": _relative(before_snapshot_path),
                "protected_outputs_snapshot_after": _relative(after_snapshot_path),
                "protected_outputs_unchanged": protected_snapshot_unchanged,
                "protected_outputs_diff": protected_snapshot_diff,
            },
            "canonical_branch_manifest_details": canonical_details,
            "experimental_branch_manifest_details": experimental_details,
            "execution": {
                "command": COMMAND_TEXT,
                "env_overrides": EXPERIMENT_ENV,
                "official_element_count_override_restored_after_run": os.environ.get("OFFICIAL_ELEMENT_COUNT_OVERRIDE")
                is None,
            },
        }
    )
    _write_json(manifest_path, manifest)
    return manifest


def _write_blocked_note(
    *,
    before_snapshot_path: Path,
    after_snapshot_path: Path,
    protected_snapshot_unchanged: bool,
    protected_snapshot_diff: dict[str, list[str]],
    exception_text: str,
) -> Path:
    blocked_note_path = EXPERIMENT_OUTPUT_DIR / "experiment_blocked_note.md"
    forcing_reasons = _forcing_block_reasons()
    lines = [
        "# EXPERIMENTAL / NOT THESIS-FACING: March 13-14 5,000-Element Experiment Blocked",
        "",
        "- Status: blocked",
        f"- Command: `{COMMAND_TEXT}`",
        f"- Experimental output directory: `{_relative(EXPERIMENT_OUTPUT_DIR)}`",
        f"- Canonical output directory: `{_relative(CANONICAL_OUTPUT_DIR)}`",
        f"- Requested element count: `{EXPERIMENT_REQUESTED_ELEMENT_COUNT}`",
        f"- Protected outputs unchanged: `{str(protected_snapshot_unchanged).lower()}`",
        f"- Protected snapshot before: `{_relative(before_snapshot_path)}`",
        f"- Protected snapshot after: `{_relative(after_snapshot_path)}`",
        "",
        "## Exception",
        "",
        "```text",
        exception_text.strip() or "No exception text captured.",
        "```",
    ]
    if forcing_reasons:
        lines.extend(["", "## Blocking Reasons", ""])
        lines.extend(f"- {reason}" for reason in forcing_reasons)
    if any(protected_snapshot_diff.values()):
        lines.extend(["", "## Protected Output Diff", ""])
        if protected_snapshot_diff["added"]:
            lines.append(f"- Added: {', '.join(protected_snapshot_diff['added'])}")
        if protected_snapshot_diff["removed"]:
            lines.append(f"- Removed: {', '.join(protected_snapshot_diff['removed'])}")
        if protected_snapshot_diff["changed"]:
            lines.append(f"- Changed: {', '.join(protected_snapshot_diff['changed'])}")
    _write_text(blocked_note_path, "\n".join(lines) + "\n")
    return blocked_note_path


def main() -> int:
    if EXPERIMENT_OUTPUT_DIR.resolve() == CANONICAL_OUTPUT_DIR.resolve():
        raise RuntimeError("Experimental output directory must be separate from the canonical B1 directory.")

    EXPERIMENT_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    blocked_note_path = EXPERIMENT_OUTPUT_DIR / "experiment_blocked_note.md"
    before_snapshot_path = EXPERIMENT_OUTPUT_DIR / "protected_outputs_snapshot_before.json"
    after_snapshot_path = EXPERIMENT_OUTPUT_DIR / "protected_outputs_snapshot_after.json"
    preseeded_forcing_files = _preseed_experiment_forcing_from_canonical()

    before_snapshot = _snapshot_paths(PROTECTED_OUTPUT_DIRS)
    _write_json(
        before_snapshot_path,
        {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "method": "size_and_mtime_ns",
            "paths": before_snapshot,
        },
    )

    completed_process: subprocess.CompletedProcess[str] | None = None
    exception_text = ""
    try:
        with _temporary_env(EXPERIMENT_ENV):
            completed_process = subprocess.run(
                [sys.executable, "-m", "src"],
                cwd=REPO_ROOT,
                text=True,
                check=False,
            )
            if completed_process.returncode != 0:
                raise RuntimeError(
                    "Experiment rerun failed with exit code "
                    f"{completed_process.returncode}. See stdout/stderr above for details."
                )
    except Exception:
        exception_text = traceback.format_exc()
    finally:
        after_snapshot = _snapshot_paths(PROTECTED_OUTPUT_DIRS)
        _write_json(
            after_snapshot_path,
            {
                "generated_at_utc": datetime.now(timezone.utc).isoformat(),
                "method": "size_and_mtime_ns",
                "paths": after_snapshot,
            },
        )

    after_payload = _load_json(after_snapshot_path)
    before_payload = _load_json(before_snapshot_path)
    protected_snapshot_diff = _snapshot_diff(
        before_payload.get("paths") or {},
        after_payload.get("paths") or {},
    )
    protected_snapshot_unchanged = not any(protected_snapshot_diff.values())

    if exception_text:
        blocked_note_path = _write_blocked_note(
            before_snapshot_path=before_snapshot_path,
            after_snapshot_path=after_snapshot_path,
            protected_snapshot_unchanged=protected_snapshot_unchanged,
            protected_snapshot_diff=protected_snapshot_diff,
            exception_text=exception_text,
        )
        print(f"Blocked note written to: {blocked_note_path}")
        if not protected_snapshot_unchanged:
            print("Protected outputs changed unexpectedly while the experiment was blocked.")
        return 1

    manifest_path = EXPERIMENT_OUTPUT_DIR / "march13_14_reinit_run_manifest.json"
    if not manifest_path.exists():
        blocked_note_path = _write_blocked_note(
            before_snapshot_path=before_snapshot_path,
            after_snapshot_path=after_snapshot_path,
            protected_snapshot_unchanged=protected_snapshot_unchanged,
            protected_snapshot_diff=protected_snapshot_diff,
            exception_text="The experiment service finished without writing march13_14_reinit_run_manifest.json.",
        )
        print(f"Blocked note written to: {blocked_note_path}")
        return 1

    canonical_details = _branch_manifest_details(CANONICAL_OUTPUT_DIR)
    experimental_details = _branch_manifest_details(EXPERIMENT_OUTPUT_DIR)
    perturbation_info = _compare_member_perturbations(canonical_details, experimental_details)
    manifest = _augment_experiment_manifest(
        manifest_path=manifest_path,
        before_snapshot_path=before_snapshot_path,
        after_snapshot_path=after_snapshot_path,
        protected_snapshot_unchanged=protected_snapshot_unchanged,
        protected_snapshot_diff=protected_snapshot_diff,
        canonical_details=canonical_details,
        experimental_details=experimental_details,
        perturbation_info=perturbation_info,
        preseeded_forcing_files=preseeded_forcing_files,
    )
    if blocked_note_path.exists():
        blocked_note_path.unlink()

    print("5,000-element experiment complete.")
    print(f"Command: {COMMAND_TEXT}")
    print(f"Experimental output directory: {EXPERIMENT_OUTPUT_DIR}")
    print(f"Run manifest: {manifest_path}")
    print(
        "Actual element count detected from manifest: "
        f"{manifest.get('element_count_detected_from_manifest')}"
    )
    print(
        "Exact same member perturbations as canonical: "
        f"{manifest.get('exact_same_member_perturbations_as_canonical')}"
    )
    print(f"Protected outputs unchanged: {protected_snapshot_unchanged}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
