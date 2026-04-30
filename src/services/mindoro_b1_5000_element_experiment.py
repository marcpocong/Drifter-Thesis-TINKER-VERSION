"""Launcher-safe Mindoro March 13 -> March 14 5,000-element experiment."""

from __future__ import annotations

import json
import os
import shutil
import traceback
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from src.core.case_context import get_case_context
from src.services.ensemble import OFFICIAL_ELEMENT_COUNT_OVERRIDE_ENV
from src.services.phase3b_extended_public_scored_march13_14_reinit import (
    MARCH13_14_REINIT_DIR_NAME,
    PHASE3B_REINIT_APPENDIX_ONLY_ENV,
    PHASE3B_REINIT_LAUNCHER_ENTRY_ID_OVERRIDE_ENV,
    PHASE3B_REINIT_OUTPUT_DIR_NAME_ENV,
    PHASE3B_REINIT_PRIMARY_PUBLIC_VALIDATION_ENV,
    PHASE3B_REINIT_REPORTING_ROLE_OVERRIDE_ENV,
    PHASE3B_REINIT_REQUESTED_ELEMENT_COUNT_ENV,
    PHASE3B_REINIT_TRACK_ID_OVERRIDE_ENV,
    PHASE3B_REINIT_TRACK_LABEL_OVERRIDE_ENV,
    PHASE3B_REINIT_TRACK_OVERRIDE_ENV,
    run_phase3b_extended_public_scored_march13_14_reinit,
)


RUN_NAME = "CASE_MINDORO_RETRO_2023"
CANONICAL_OUTPUT_DIR_NAME = MARCH13_14_REINIT_DIR_NAME
EXPERIMENT_OUTPUT_DIR_NAME = "phase3b_extended_public_scored_march13_14_reinit_5000_experiment"
SENSITIVITY_OUTPUT_DIR_NAME = "phase3b_march13_14_element_count_sensitivity"
CANONICAL_REQUESTED_ELEMENT_COUNT = 100000
EXPERIMENT_REQUESTED_ELEMENT_COUNT = 5000
EXPECTED_ENSEMBLE_MEMBER_COUNT = 50
EXPERIMENT_PHASE_ID = "mindoro_b1_5000_element_experiment"
EXPERIMENT_LAUNCHER_ENTRY_ID = "phase3b_mindoro_march13_14_reinit_5000_experiment"

EXPERIMENT_TRACK = "mindoro_phase3b_experiment_reinit_5000"
EXPERIMENT_TRACK_ID = "EXP_B1_5000_ELEMENT_COUNT"
EXPERIMENT_TRACK_LABEL = "Mindoro March 13-14 5,000-element personal experiment"
EXPERIMENT_REPORTING_ROLE = "personal_experiment_element_count_sensitivity_not_thesis_facing"


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


def _relative(repo_root: Path, path: Path) -> str:
    try:
        return str(path.resolve().relative_to(repo_root.resolve())).replace("\\", "/")
    except Exception:
        return str(path).replace("\\", "/")


def _snapshot_paths(repo_root: Path, paths: list[Path]) -> dict[str, dict[str, int]]:
    snapshot: dict[str, dict[str, int]] = {}
    for root in paths:
        if not root.exists():
            continue
        for path in sorted(item for item in root.rglob("*") if item.is_file()):
            stat = path.stat()
            snapshot[_relative(repo_root, path)] = {
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


def _branch_manifest_details(repo_root: Path, base_dir: Path) -> dict[str, dict[str, Any]]:
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
            "forecast_manifest_path": _relative(repo_root, forecast_manifest_path) if forecast_manifest_path.exists() else "",
            "ensemble_manifest_path": _relative(repo_root, ensemble_manifest_path) if ensemble_manifest_path.exists() else "",
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
    repo_root: Path,
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
        canonical_manifest = repo_root / canonical_branch["ensemble_manifest_path"]
        experimental_manifest = repo_root / experimental_branch["ensemble_manifest_path"]
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


class MindoroB15000ElementExperimentService:
    def __init__(self, *, repo_root: str | Path | None = None):
        self.repo_root = Path(repo_root or Path(__file__).resolve().parents[2])
        self.case = get_case_context()
        if self.case.workflow_mode != "mindoro_retro_2023" or not self.case.is_official:
            raise RuntimeError("mindoro_b1_5000_element_experiment requires WORKFLOW_MODE=mindoro_retro_2023.")

        self.case_output_dir = self.repo_root / "output" / RUN_NAME
        self.canonical_output_dir = self.case_output_dir / CANONICAL_OUTPUT_DIR_NAME
        self.experiment_output_dir = self.case_output_dir / EXPERIMENT_OUTPUT_DIR_NAME
        self.sensitivity_output_dir = self.case_output_dir / SENSITIVITY_OUTPUT_DIR_NAME
        self.canonical_forcing_dir = self.canonical_output_dir / "forcing"
        self.experiment_persistent_forcing_dir = (
            self.repo_root / "data" / "local_input_store" / RUN_NAME / EXPERIMENT_OUTPUT_DIR_NAME / "forcing"
        )
        self.protected_output_dirs = [
            self.canonical_output_dir,
            self.case_output_dir / "phase3b",
            self.case_output_dir / "phase3b_multidate_public",
            self.case_output_dir / "public_obs_appendix",
            self.repo_root / "output" / "final_validation_package",
            self.repo_root / "output" / "final_reproducibility_package",
            self.repo_root / "output" / "figure_package_publication",
        ]
        self.experiment_env = {
            "WORKFLOW_MODE": "mindoro_retro_2023",
            "PIPELINE_PHASE": "phase3b_extended_public_scored_march13_14_reinit",
            "FORCING_OUTAGE_POLICY": "fail_hard",
            "INPUT_CACHE_POLICY": "reuse_if_valid",
            PHASE3B_REINIT_OUTPUT_DIR_NAME_ENV: EXPERIMENT_OUTPUT_DIR_NAME,
            PHASE3B_REINIT_TRACK_OVERRIDE_ENV: EXPERIMENT_TRACK,
            PHASE3B_REINIT_TRACK_ID_OVERRIDE_ENV: EXPERIMENT_TRACK_ID,
            PHASE3B_REINIT_TRACK_LABEL_OVERRIDE_ENV: EXPERIMENT_TRACK_LABEL,
            PHASE3B_REINIT_REPORTING_ROLE_OVERRIDE_ENV: EXPERIMENT_REPORTING_ROLE,
            PHASE3B_REINIT_APPENDIX_ONLY_ENV: "true",
            PHASE3B_REINIT_PRIMARY_PUBLIC_VALIDATION_ENV: "false",
            PHASE3B_REINIT_LAUNCHER_ENTRY_ID_OVERRIDE_ENV: EXPERIMENT_LAUNCHER_ENTRY_ID,
            PHASE3B_REINIT_REQUESTED_ELEMENT_COUNT_ENV: str(EXPERIMENT_REQUESTED_ELEMENT_COUNT),
            OFFICIAL_ELEMENT_COUNT_OVERRIDE_ENV: str(EXPERIMENT_REQUESTED_ELEMENT_COUNT),
            "EXTENDED_PUBLIC_FORCE_RERUN": "true",
        }
        self.command_text = "PIPELINE_PHASE=mindoro_b1_5000_element_experiment python -m src"

    def _preseed_experiment_forcing_from_canonical(self) -> list[str]:
        copied: list[str] = []
        if not self.canonical_forcing_dir.exists():
            return copied
        self.experiment_persistent_forcing_dir.mkdir(parents=True, exist_ok=True)
        for source_path in sorted(path for path in self.canonical_forcing_dir.iterdir() if path.is_file()):
            target_path = self.experiment_persistent_forcing_dir / source_path.name
            shutil.copy2(source_path, target_path)
            copied.append(_relative(self.repo_root, target_path))
        return copied

    def _forcing_block_reasons(self) -> list[str]:
        manifest_path = self.experiment_output_dir / "march13_14_reinit_forcing_window_manifest.json"
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

    def _write_blocked_note(
        self,
        *,
        before_snapshot_path: Path,
        after_snapshot_path: Path,
        protected_snapshot_unchanged: bool,
        protected_snapshot_diff: dict[str, list[str]],
        exception_text: str,
    ) -> Path:
        blocked_note_path = self.experiment_output_dir / "experiment_blocked_note.md"
        forcing_reasons = self._forcing_block_reasons()
        lines = [
            "# EXPERIMENTAL / NOT THESIS-FACING: March 13-14 5,000-Element Experiment Blocked",
            "",
            "- Status: blocked",
            f"- Phase: `{EXPERIMENT_PHASE_ID}`",
            f"- Launcher entry: `{EXPERIMENT_LAUNCHER_ENTRY_ID}`",
            f"- Command equivalent: `{self.command_text}`",
            f"- Experimental output directory: `{_relative(self.repo_root, self.experiment_output_dir)}`",
            f"- Canonical output directory: `{_relative(self.repo_root, self.canonical_output_dir)}`",
            f"- Requested element count: `{EXPERIMENT_REQUESTED_ELEMENT_COUNT}`",
            f"- Protected outputs unchanged: `{str(protected_snapshot_unchanged).lower()}`",
            f"- Protected snapshot before: `{_relative(self.repo_root, before_snapshot_path)}`",
            f"- Protected snapshot after: `{_relative(self.repo_root, after_snapshot_path)}`",
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

    def _augment_experiment_manifest(
        self,
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
                "canonical_100k_output_dir": _relative(self.repo_root, self.canonical_output_dir),
                "experimental_5000_output_dir": _relative(self.repo_root, self.experiment_output_dir),
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
                "independent_noaa_observation_products_confirmed": True,
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
                    "canonical_forcing_dir": _relative(self.repo_root, self.canonical_forcing_dir),
                    "experiment_persistent_forcing_dir": _relative(
                        self.repo_root,
                        self.experiment_persistent_forcing_dir,
                    ),
                    "preseeded_files": preseeded_forcing_files,
                },
                "guardrail_snapshots": {
                    "protected_outputs_snapshot_before": _relative(self.repo_root, before_snapshot_path),
                    "protected_outputs_snapshot_after": _relative(self.repo_root, after_snapshot_path),
                    "protected_outputs_unchanged": protected_snapshot_unchanged,
                    "protected_outputs_diff": protected_snapshot_diff,
                },
                "canonical_branch_manifest_details": canonical_details,
                "experimental_branch_manifest_details": experimental_details,
                "execution": {
                    "phase": EXPERIMENT_PHASE_ID,
                    "launcher_entry_id": EXPERIMENT_LAUNCHER_ENTRY_ID,
                    "command_equivalent": self.command_text,
                    "env_overrides": self.experiment_env,
                    "official_element_count_override_restored_after_run": os.environ.get(
                        OFFICIAL_ELEMENT_COUNT_OVERRIDE_ENV
                    )
                    is None,
                },
            }
        )
        _write_json(manifest_path, manifest)
        return manifest

    def run(self) -> dict[str, Any]:
        if self.experiment_output_dir.resolve() == self.canonical_output_dir.resolve():
            raise RuntimeError("Experimental output directory must be separate from the canonical B1 directory.")

        self.experiment_output_dir.mkdir(parents=True, exist_ok=True)
        blocked_note_path = self.experiment_output_dir / "experiment_blocked_note.md"
        before_snapshot_path = self.experiment_output_dir / "protected_outputs_snapshot_before.json"
        after_snapshot_path = self.experiment_output_dir / "protected_outputs_snapshot_after.json"
        preseeded_forcing_files = self._preseed_experiment_forcing_from_canonical()

        before_snapshot = _snapshot_paths(self.repo_root, self.protected_output_dirs)
        _write_json(
            before_snapshot_path,
            {
                "generated_at_utc": datetime.now(timezone.utc).isoformat(),
                "method": "size_and_mtime_ns",
                "paths": before_snapshot,
            },
        )

        run_results: dict[str, Any] | None = None
        exception_text = ""
        try:
            with _temporary_env(self.experiment_env):
                run_results = run_phase3b_extended_public_scored_march13_14_reinit()
        except Exception:
            exception_text = traceback.format_exc()
        finally:
            after_snapshot = _snapshot_paths(self.repo_root, self.protected_output_dirs)
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
            blocked_note_path = self._write_blocked_note(
                before_snapshot_path=before_snapshot_path,
                after_snapshot_path=after_snapshot_path,
                protected_snapshot_unchanged=protected_snapshot_unchanged,
                protected_snapshot_diff=protected_snapshot_diff,
                exception_text=exception_text,
            )
            raise RuntimeError(f"Mindoro B1 5,000-element experiment blocked. See {blocked_note_path}")

        manifest_path = self.experiment_output_dir / "march13_14_reinit_run_manifest.json"
        if not manifest_path.exists():
            blocked_note_path = self._write_blocked_note(
                before_snapshot_path=before_snapshot_path,
                after_snapshot_path=after_snapshot_path,
                protected_snapshot_unchanged=protected_snapshot_unchanged,
                protected_snapshot_diff=protected_snapshot_diff,
                exception_text="The experiment phase finished without writing march13_14_reinit_run_manifest.json.",
            )
            raise RuntimeError(f"Mindoro B1 5,000-element experiment blocked. See {blocked_note_path}")

        canonical_details = _branch_manifest_details(self.repo_root, self.canonical_output_dir)
        experimental_details = _branch_manifest_details(self.repo_root, self.experiment_output_dir)
        perturbation_info = _compare_member_perturbations(
            self.repo_root,
            canonical_details,
            experimental_details,
        )
        manifest = self._augment_experiment_manifest(
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

        summary_csv = str(run_results["summary_csv"]) if run_results else str(self.experiment_output_dir / "march13_14_reinit_summary.csv")
        diagnostics_csv = str(run_results["diagnostics_csv"]) if run_results else str(self.experiment_output_dir / "march13_14_reinit_diagnostics.csv")
        branch_survival_csv = str(run_results["branch_survival_csv"]) if run_results else str(
            self.experiment_output_dir / "march13_14_reinit_branch_survival_summary.csv"
        )
        decision_note_md = str(run_results["decision_note_md"]) if run_results else str(
            self.experiment_output_dir / "march13_14_reinit_decision_note.md"
        )
        return {
            "output_dir": str(self.experiment_output_dir),
            "summary_csv": summary_csv,
            "diagnostics_csv": diagnostics_csv,
            "branch_survival_csv": branch_survival_csv,
            "decision_note_md": decision_note_md,
            "run_manifest_json": str(manifest_path),
            "element_count_detected_from_manifest": manifest.get("element_count_detected_from_manifest"),
            "exact_same_member_perturbations_as_canonical": manifest.get(
                "exact_same_member_perturbations_as_canonical"
            ),
            "protected_outputs_unchanged": protected_snapshot_unchanged,
            "protected_outputs_diff": protected_snapshot_diff,
            "guardrail_snapshot_before_json": str(before_snapshot_path),
            "guardrail_snapshot_after_json": str(after_snapshot_path),
        }


def run_mindoro_b1_5000_element_experiment() -> dict[str, Any]:
    return MindoroB15000ElementExperimentService().run()


def main() -> int:
    results = run_mindoro_b1_5000_element_experiment()
    print("5,000-element experiment complete.")
    print(f"Output directory: {results['output_dir']}")
    print(f"Summary: {results['summary_csv']}")
    print(f"Decision note: {results['decision_note_md']}")
    print(f"Run manifest: {results['run_manifest_json']}")
    print(f"Actual element count detected from manifest: {results['element_count_detected_from_manifest']}")
    print(
        "Exact same member perturbations as canonical: "
        f"{results['exact_same_member_perturbations_as_canonical']}"
    )
    print(f"Protected outputs unchanged: {results['protected_outputs_unchanged']}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
