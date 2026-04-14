"""Read-only Phase 1 finalization audit against the stable Chapter 3 architecture."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

from src.core.domain_semantics import resolve_phase1_validation_box
from src.services.validation import (
    PHASE1_LOADING_AUDIT_POLICY,
    PHASE1_LOADING_AUDIT_SCHEMA_VERSION,
)
from src.utils.io import (
    get_official_phase1_recipe_family,
    get_phase1_legacy_recipe_aliases,
)

PHASE = "phase1_finalization_audit"
OUTPUT_DIR = Path("output") / PHASE

EXPECTED_HISTORICAL_YEARS = set(range(2016, 2023))
EXPECTED_OFFICIAL_RECIPE_FAMILY = {"cmems_era5", "cmems_gfs", "hycom_era5", "hycom_gfs"}
CLASSIFICATIONS = {
    "implemented_and_scientifically_ready",
    "implemented_but_provisional",
    "partially_implemented",
    "missing",
}


@dataclass(frozen=True)
class RequirementStatus:
    requirement_id: str
    requirement_group: str
    chapter3_requirement: str
    classification: str
    scientifically_ready: bool
    full_production_rerun_required: bool
    low_risk_patch_applied: bool
    evidence_summary: str
    blocker: str
    evidence_paths: list[str]


def _read_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with open(path, "r", encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)


def _write_json(path: Path, payload: dict | list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, default=str)
        handle.write("\n")


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.rstrip() + "\n", encoding="utf-8")


class Phase1FinalizationAuditService:
    def __init__(self, repo_root: str | Path = ".", output_dir: str | Path | None = None):
        self.repo_root = Path(repo_root).resolve()
        self.output_dir = Path(output_dir) if output_dir else self.repo_root / OUTPUT_DIR
        self.config_dir = self.repo_root / "config"
        self.output_root = self.repo_root / "output"
        self.data_root = self.repo_root / "data"
        self.settings_path = self.config_dir / "settings.yaml"
        self.recipes_path = self.config_dir / "recipes.yaml"
        self.baseline_path = self.config_dir / "phase1_baseline_selection.yaml"
        self.readme_path = self.repo_root / "README.md"
        self.final_validation_manifest_path = self.output_root / "final_validation_package" / "final_validation_manifest.json"

        self.settings_cfg = _read_yaml(self.settings_path)
        self.recipes_cfg = _read_yaml(self.recipes_path)
        self.baseline_cfg = _read_yaml(self.baseline_path)
        self.phase1_audit_settings = self.settings_cfg.get("phase1_official_audit") or {}
        self.baseline_audit_cfg = self.baseline_cfg.get("chapter3_finalization_audit") or {}
        self.legacy_aliases = get_phase1_legacy_recipe_aliases(self.recipes_path)
        self.official_recipe_family = set(get_official_phase1_recipe_family(self.recipes_path))
        self.runtime_recipe_ids = set((self.recipes_cfg.get("recipes") or {}).keys())

        self.prototype_rankings = self._load_prototype_rankings()
        self.prototype_audits = self._load_prototype_loading_audits()
        self.prototype_drifter_columns = self._load_drifter_columns()
        self.accepted_segment_registry_candidates = self._discover_artifacts(["*accepted*segment*registry*.csv", "*accepted*segment*registry*.json"])
        self.rejected_segment_registry_candidates = self._discover_artifacts(["*rejected*segment*registry*.csv", "*rejected*segment*registry*.json"])
        self.segment_metrics_candidates = self._discover_artifacts(["*segment*metrics*.csv", "*segment*metrics*.json"])
        self.recipe_summary_candidates = self._discover_artifacts(["*phase1*recipe*summary*.csv", "*phase1*recipe*summary*.json"])

    def _load_prototype_rankings(self) -> list[dict[str, Any]]:
        ranking_paths = []
        for item in self.baseline_cfg.get("historical_validation_artifacts") or []:
            candidate = self.repo_root / str(item)
            if candidate.exists():
                ranking_paths.append(candidate)
        if not ranking_paths:
            ranking_paths = sorted(self.output_root.glob("CASE_20??-??-??/validation/validation_ranking.csv"))

        rows = []
        for path in ranking_paths:
            run_name = path.parents[1].name
            run_date = run_name.replace("CASE_", "")
            df = _read_csv(path)
            if df.empty:
                continue
            winner = df.iloc[0]
            rows.append(
                {
                    "run_name": run_name,
                    "run_date": run_date,
                    "year": int(run_date[:4]) if run_date[:4].isdigit() else None,
                    "ranking_path": str(path.relative_to(self.repo_root)),
                    "winning_recipe": str(winner.get("recipe", "")),
                    "winning_score": float(winner.get("ncs_score", float("nan"))),
                }
            )
        return rows

    def _load_prototype_loading_audits(self) -> list[dict[str, Any]]:
        rows = []
        for ranking in self.prototype_rankings:
            ranking_path = self.repo_root / ranking["ranking_path"]
            audit_path = ranking_path.with_name("phase1_loading_audit.csv")
            if not audit_path.exists():
                continue
            df = _read_csv(audit_path)
            rows.append(
                {
                    "run_name": ranking["run_name"],
                    "audit_path": str(audit_path.relative_to(self.repo_root)),
                    "columns": list(df.columns),
                    "row_count": int(len(df)),
                }
            )
        return rows

    def _load_drifter_columns(self) -> list[str]:
        drifter_paths = sorted(self.data_root.glob("drifters/CASE_20??-??-??/drifters_noaa.csv"))
        for path in drifter_paths:
            with open(path, "r", encoding="utf-8") as handle:
                header = handle.readline().strip()
            if header:
                return [column.strip() for column in header.split(",") if column.strip()]
        return []

    def _discover_artifacts(self, patterns: list[str]) -> list[str]:
        matches: list[str] = []
        for pattern in patterns:
            for path in self.output_root.rglob(pattern):
                matches.append(str(path.relative_to(self.repo_root)))
        return sorted(set(matches))

    def _smoke_checks(self) -> list[dict[str, Any]]:
        legacy_columns_present = bool(self.prototype_audits) and all(
            {"loading_audit_schema_version", "status_flag", "hard_fail", "hard_fail_reason"}.issubset(set(item["columns"]))
            for item in self.prototype_audits
        )
        return [
            {
                "check_id": "baseline_artifact_present",
                "passed": self.baseline_path.exists() and bool(self.baseline_cfg.get("selected_recipe")),
                "evidence": str(self.baseline_path.relative_to(self.repo_root)),
            },
            {
                "check_id": "final_validation_package_present",
                "passed": self.final_validation_manifest_path.exists(),
                "evidence": str(self.final_validation_manifest_path.relative_to(self.repo_root)),
            },
            {
                "check_id": "phase1_loading_audit_schema_available",
                "passed": bool(PHASE1_LOADING_AUDIT_SCHEMA_VERSION and PHASE1_LOADING_AUDIT_POLICY),
                "evidence": PHASE1_LOADING_AUDIT_SCHEMA_VERSION,
            },
            {
                "check_id": "prototype_mode_preserved",
                "passed": str(self.settings_cfg.get("workflow_mode", "")) == "prototype_2016",
                "evidence": str(self.settings_path.relative_to(self.repo_root)),
            },
            {
                "check_id": "legacy_phase1_audits_regenerated_with_new_schema",
                "passed": legacy_columns_present,
                "evidence": ", ".join(item["audit_path"] for item in self.prototype_audits) or "no_existing_phase1_loading_audit_csv",
            },
        ]

    def _requirement(self, **kwargs) -> RequirementStatus:
        record = RequirementStatus(**kwargs)
        if record.classification not in CLASSIFICATIONS:
            raise ValueError(f"Unsupported Phase 1 audit classification: {record.classification}")
        return record

    @staticmethod
    def _biggest_blocker() -> str:
        return (
            "The repo still lacks the accepted/rejected drogued 72 h segment registry generated from a true 2016-2022 "
            "regional drifter pool, so the frozen baseline cannot yet be defended as the final Chapter 3 Phase 1 study."
        )

    def _build_requirements(self) -> list[RequirementStatus]:
        found_years = {item["year"] for item in self.prototype_rankings if item.get("year") is not None}
        year_text = ", ".join(str(value) for value in sorted(found_years)) or "none"
        ranking_paths = [item["ranking_path"] for item in self.prototype_rankings]
        audit_paths = [item["audit_path"] for item in self.prototype_audits]
        phase1_box = resolve_phase1_validation_box(
            self.baseline_audit_cfg,
            self.phase1_audit_settings,
            self.settings_cfg,
        )
        has_drogue_columns = any("drog" in column.lower() for column in self.prototype_drifter_columns)
        legacy_runtime_recipe_gap = sorted(EXPECTED_OFFICIAL_RECIPE_FAMILY - self.runtime_recipe_ids)
        gfs_available = any(path.name == "gfs_wind.nc" for path in self.data_root.glob("forcing/*/gfs_wind.nc"))
        legacy_audits_have_new_fields = bool(self.prototype_audits) and all(
            {"loading_audit_schema_version", "status_flag", "hard_fail", "hard_fail_reason"}.issubset(set(item["columns"]))
            for item in self.prototype_audits
        )
        baseline_selected_recipe = str(self.baseline_cfg.get("selected_recipe") or "")
        prototype_dates = [str(item["run_date"]) for item in self.prototype_rankings]

        return [
            self._requirement(
                requirement_id="historical_window_2016_2022",
                requirement_group="core_architecture",
                chapter3_requirement="Historical regional drifter pool covers 2016-2022.",
                classification="partially_implemented" if self.prototype_rankings else "missing",
                scientifically_ready=False,
                full_production_rerun_required=True,
                low_risk_patch_applied=False,
                evidence_summary=(
                    f"Local Phase 1 rankings exist for {len(self.prototype_rankings)} prototype dates covering year(s): {year_text}. "
                    "The target Chapter 3 window is 2016-2022."
                ),
                blocker="Only the legacy three-date prototype evidence is present locally; the multi-year regional pool has not been built.",
                evidence_paths=ranking_paths,
            ),
            self._requirement(
                requirement_id="fixed_phase1_validation_box",
                requirement_group="core_architecture",
                chapter3_requirement="Phase 1 uses a fixed phase1_validation_box.",
                classification="implemented_but_provisional" if phase1_box else "missing",
                scientifically_ready=False,
                full_production_rerun_required=True,
                low_risk_patch_applied=bool(
                    self.baseline_audit_cfg.get("phase1_validation_box")
                    or self.baseline_audit_cfg.get("regional_validation_box")
                ),
                evidence_summary=(
                    f"Fixed Phase 1 validation-box metadata is present with bounds {phase1_box}."
                    if phase1_box
                    else "No fixed phase1_validation_box metadata was found."
                ),
                blocker=(
                    "The box is frozen in metadata but has not yet been exercised through the final 2016-2022 accepted/rejected segment study."
                    if phase1_box
                    else "phase1_validation_box metadata is absent."
                ),
                evidence_paths=[str(self.baseline_path.relative_to(self.repo_root)), str(self.settings_path.relative_to(self.repo_root))],
            ),
            self._requirement(
                requirement_id="drogued_segments_only_core_pool",
                requirement_group="core_architecture",
                chapter3_requirement="Only drogued segments enter the core historical pool.",
                classification="partially_implemented" if has_drogue_columns else "missing",
                scientifically_ready=False,
                full_production_rerun_required=True,
                low_risk_patch_applied=False,
                evidence_summary=(
                    f"Prototype drifter headers expose columns {self.prototype_drifter_columns}."
                    if self.prototype_drifter_columns
                    else "No prototype drifter headers were available for inspection."
                ),
                blocker=(
                    "No explicit drogue-status filtering was found in the local Phase 1 data path."
                    if not has_drogue_columns
                    else "Drogue-related fields exist, but the accepted/rejected segment registry is still missing."
                ),
                evidence_paths=[
                    "data/drifters/CASE_2016-09-01/drifters_noaa.csv",
                    "src/services/validation.py",
                ],
            ),
            self._requirement(
                requirement_id="non_overlapping_72h_segments",
                requirement_group="core_architecture",
                chapter3_requirement="Core regional validation uses non-overlapping 72 h segments.",
                classification="partially_implemented" if self.prototype_rankings else "missing",
                scientifically_ready=False,
                full_production_rerun_required=True,
                low_risk_patch_applied=bool((self.baseline_audit_cfg.get("segment_policy") or {}).get("horizon_hours") == 72),
                evidence_summary=(
                    f"Prototype Phase 1 runs are 72 h windows for dates {prototype_dates}, but no accepted/rejected segment registry exists."
                    if self.prototype_rankings
                    else "No 72 h Phase 1 regional segment evidence was found."
                ),
                blocker="The repo does not yet materialize a general non-overlapping segment registry for the 2016-2022 regional pool.",
                evidence_paths=ranking_paths + [str(self.baseline_path.relative_to(self.repo_root))],
            ),
            self._requirement(
                requirement_id="official_recipe_family",
                requirement_group="core_architecture",
                chapter3_requirement="Official Phase 1 recipe family is HYCOM/CMEMS crossed with GFS/ERA5.",
                classification="partially_implemented" if self.official_recipe_family else "missing",
                scientifically_ready=False,
                full_production_rerun_required=True,
                low_risk_patch_applied=bool(self.official_recipe_family or self.legacy_aliases),
                evidence_summary=(
                    f"Target recipe family metadata = {sorted(self.official_recipe_family)}; runtime Phase 1 recipe IDs = {sorted(self.runtime_recipe_ids)}; "
                    f"legacy alias map = {sorted(self.legacy_aliases)}; gfs_wind.nc present locally = {gfs_available}."
                ),
                blocker=(
                    f"Core Phase 1 runtime still carries legacy recipe IDs missing {legacy_runtime_recipe_gap}, even though local gfs_wind.nc forcing is now present."
                    if legacy_runtime_recipe_gap or not gfs_available
                    else "The core runtime family is present but still needs the final multi-year rerun."
                ),
                evidence_paths=[str(self.recipes_path.relative_to(self.repo_root))],
            ),
            self._requirement(
                requirement_id="loading_audit_hard_fail_behavior",
                requirement_group="core_architecture",
                chapter3_requirement="Phase 1 writes an explicit loading audit and hard-fails when required forcing support is invalid.",
                classification="implemented_and_scientifically_ready" if legacy_audits_have_new_fields else "implemented_but_provisional",
                scientifically_ready=legacy_audits_have_new_fields,
                full_production_rerun_required=not legacy_audits_have_new_fields,
                low_risk_patch_applied=True,
                evidence_summary=(
                    f"Validation service exposes {PHASE1_LOADING_AUDIT_SCHEMA_VERSION} with policy "
                    f"`{PHASE1_LOADING_AUDIT_POLICY}`. Existing on-disk Phase 1 audits already carry the new fields = {legacy_audits_have_new_fields}."
                ),
                blocker=(
                    "Regenerate the historical Phase 1 audits during the real 2016-2022 production run so the new hard-fail/status fields exist on disk."
                    if not legacy_audits_have_new_fields
                    else ""
                ),
                evidence_paths=audit_paths + ["src/services/validation.py"],
            ),
            self._requirement(
                requirement_id="accepted_segment_registry",
                requirement_group="required_artifacts",
                chapter3_requirement="Accepted regional Phase 1 segments are recorded in an explicit registry.",
                classification="implemented_but_provisional" if self.accepted_segment_registry_candidates else "missing",
                scientifically_ready=False,
                full_production_rerun_required=True,
                low_risk_patch_applied=False,
                evidence_summary=(
                    f"Accepted-segment registry candidates: {self.accepted_segment_registry_candidates}."
                    if self.accepted_segment_registry_candidates
                    else "No accepted-segment registry was found under output/."
                ),
                blocker="The final 2016-2022 accepted regional segment registry has not been produced locally.",
                evidence_paths=self.accepted_segment_registry_candidates,
            ),
            self._requirement(
                requirement_id="rejected_segment_registry",
                requirement_group="required_artifacts",
                chapter3_requirement="Rejected regional Phase 1 segments are recorded in an explicit registry.",
                classification="implemented_but_provisional" if self.rejected_segment_registry_candidates else "missing",
                scientifically_ready=False,
                full_production_rerun_required=True,
                low_risk_patch_applied=False,
                evidence_summary=(
                    f"Rejected-segment registry candidates: {self.rejected_segment_registry_candidates}."
                    if self.rejected_segment_registry_candidates
                    else "No rejected-segment registry was found under output/."
                ),
                blocker="The final 2016-2022 rejected regional segment registry has not been produced locally.",
                evidence_paths=self.rejected_segment_registry_candidates,
            ),
            self._requirement(
                requirement_id="segment_metrics",
                requirement_group="required_artifacts",
                chapter3_requirement="Phase 1 exports segment-level metrics for the accepted/rejected regional pool.",
                classification="implemented_but_provisional" if self.segment_metrics_candidates else "missing",
                scientifically_ready=False,
                full_production_rerun_required=True,
                low_risk_patch_applied=False,
                evidence_summary=(
                    f"Segment-metrics candidates: {self.segment_metrics_candidates}."
                    if self.segment_metrics_candidates
                    else "No dedicated Phase 1 segment-metrics artifact was found under output/."
                ),
                blocker="The final regional accepted/rejected segment metrics still require the dedicated 2016-2022 run.",
                evidence_paths=self.segment_metrics_candidates,
            ),
            self._requirement(
                requirement_id="recipe_summary",
                requirement_group="required_artifacts",
                chapter3_requirement="Phase 1 exports a summary table across the regional segment study before final recipe ranking.",
                classification="implemented_but_provisional" if self.recipe_summary_candidates else "missing",
                scientifically_ready=False,
                full_production_rerun_required=True,
                low_risk_patch_applied=False,
                evidence_summary=(
                    f"Phase 1 recipe-summary candidates: {self.recipe_summary_candidates}."
                    if self.recipe_summary_candidates
                    else "No dedicated Phase 1 recipe-summary artifact was found."
                ),
                blocker="The regional recipe-summary layer still needs to be generated from the final segment registry.",
                evidence_paths=self.recipe_summary_candidates,
            ),
            self._requirement(
                requirement_id="recipe_ranking",
                requirement_group="required_artifacts",
                chapter3_requirement="Phase 1 exports recipe rankings from the historical regional study.",
                classification="implemented_but_provisional" if self.prototype_rankings else "missing",
                scientifically_ready=False,
                full_production_rerun_required=True,
                low_risk_patch_applied=False,
                evidence_summary=(
                    f"Validation rankings exist for {len(self.prototype_rankings)} prototype runs; the current frozen recipe is `{baseline_selected_recipe}`."
                    if self.prototype_rankings
                    else "No Phase 1 validation ranking was found."
                ),
                blocker="Current rankings come from the old prototype cases, not from the final 2016-2022 regional segment corpus.",
                evidence_paths=ranking_paths,
            ),
            self._requirement(
                requirement_id="frozen_baseline_artifact",
                requirement_group="required_artifacts",
                chapter3_requirement="Phase 1 emits a frozen baseline artifact for downstream spill-case workflows.",
                classification="implemented_but_provisional" if self.baseline_path.exists() else "missing",
                scientifically_ready=False,
                full_production_rerun_required=True,
                low_risk_patch_applied=bool(self.baseline_audit_cfg),
                evidence_summary=(
                    f"Frozen baseline artifact exists and currently selects `{baseline_selected_recipe}`."
                    if self.baseline_path.exists()
                    else "Frozen Phase 1 baseline artifact is missing."
                ),
                blocker="The artifact is real, but its evidence base is still the legacy three-date prototype rather than the final regional study.",
                evidence_paths=[str(self.baseline_path.relative_to(self.repo_root))] + ranking_paths,
            ),
            self._requirement(
                requirement_id="transport_vs_spill_validation_separation",
                requirement_group="workflow_guardrails",
                chapter3_requirement="Historical/regional transport validation remains separate from spill-case validation.",
                classification="implemented_and_scientifically_ready",
                scientifically_ready=True,
                full_production_rerun_required=False,
                low_risk_patch_applied=False,
                evidence_summary=(
                    "Official spill-case workflows consume a frozen Phase 1 baseline, while the final validation package is read-only and built from separate finished Mindoro/DWH outputs."
                ),
                blocker="",
                evidence_paths=[
                    "src/__main__.py",
                    "src/core/case_context.py",
                    "src/services/final_validation_package.py",
                    str(self.final_validation_manifest_path.relative_to(self.repo_root)) if self.final_validation_manifest_path.exists() else "output/final_validation_package/final_validation_manifest.json",
                ],
            ),
            self._requirement(
                requirement_id="prototype_mode_preserved_not_final_study",
                requirement_group="workflow_guardrails",
                chapter3_requirement="Prototype mode remains available but is not misrepresented as the final Phase 1 study.",
                classification="implemented_and_scientifically_ready",
                scientifically_ready=True,
                full_production_rerun_required=False,
                low_risk_patch_applied=True,
                evidence_summary=(
                    "prototype_2016 remains the preserved debugging workflow, and the runtime context now explicitly labels it as non-final."
                ),
                blocker="",
                evidence_paths=[
                    str(self.settings_path.relative_to(self.repo_root)),
                    "src/core/case_context.py",
                    str(self.readme_path.relative_to(self.repo_root)),
                ],
            ),
        ]

    def _build_verdict(self, requirements: list[RequirementStatus]) -> dict[str, Any]:
        architecture_structurally_supported = all(
            item.classification in {
                "implemented_and_scientifically_ready",
                "implemented_but_provisional",
                "partially_implemented",
            }
            for item in requirements
            if item.requirement_group == "workflow_guardrails"
        )
        scientifically_ready = all(item.scientifically_ready for item in requirements if item.requirement_group != "workflow_guardrails")
        full_production_rerun_required = any(item.full_production_rerun_required for item in requirements)
        return {
            "architecture_structurally_supported": architecture_structurally_supported,
            "scientifically_ready": scientifically_ready,
            "full_production_rerun_required": full_production_rerun_required,
            "biggest_remaining_blocker": self._biggest_blocker(),
            "verdict_label": (
                "architecturally_auditable_but_not_scientifically_frozen"
                if architecture_structurally_supported and not scientifically_ready
                else ("scientifically_ready" if scientifically_ready else "not_ready")
            ),
        }

    def _write_status_csv(self, requirements: list[RequirementStatus]) -> Path:
        path = self.output_dir / "phase1_finalization_status.csv"
        df = pd.DataFrame(
            [
                {
                    **asdict(item),
                    "evidence_paths": "; ".join(item.evidence_paths),
                }
                for item in requirements
            ]
        )
        path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(path, index=False)
        return path

    def _write_status_json(
        self,
        requirements: list[RequirementStatus],
        smoke_checks: list[dict[str, Any]],
        verdict: dict[str, Any],
    ) -> Path:
        path = self.output_dir / "phase1_finalization_status.json"
        payload = {
            "phase": PHASE,
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "repo_root": str(self.repo_root),
            "smoke_checks": smoke_checks,
            "current_local_prototype_rankings": self.prototype_rankings,
            "current_local_phase1_loading_audits": self.prototype_audits,
            "baseline_artifact": {
                "path": str(self.baseline_path.relative_to(self.repo_root)),
                "selected_recipe": self.baseline_cfg.get("selected_recipe"),
                "source_kind": self.baseline_cfg.get("source_kind"),
                "chapter3_finalization_audit": self.baseline_audit_cfg,
            },
            "recipe_architecture": {
                "official_recipe_family": sorted(self.official_recipe_family),
                "runtime_recipe_ids": sorted(self.runtime_recipe_ids),
                "legacy_recipe_name_aliases": self.legacy_aliases,
            },
            "requirements": [asdict(item) for item in requirements],
            "overall_verdict": verdict,
        }
        _write_json(path, payload)
        return path

    def _write_memo(self, requirements: list[RequirementStatus], verdict: dict[str, Any]) -> Path:
        path = self.output_dir / "phase1_finalization_memo.md"
        lines = [
            "# Phase 1 Finalization Memo",
            "",
            "This phase is a read-only architectural audit. It does not rerun the expensive 2016-2022 production study, and it does not overwrite finished Mindoro, DWH, or final-validation scientific outputs.",
            "",
            "## What This Patch Finalizes",
            "",
            "- Adds a dedicated `phase1_finalization_audit` route that writes its own audit package under `output/phase1_finalization_audit/`.",
            "- Freezes explicit metadata for the Chapter 3 target window, Phase 1 validation box, segment policy, and recipe-family intent without pretending the scientific study already exists.",
            "- Makes Phase 1 loading-audit hard-fail/status fields explicit in code for the next real regional rerun.",
            "- Clarifies that the preserved `prototype_2016` workflow is a legacy debugging path, not the final Chapter 3 Phase 1 evidence base.",
            "",
            "## What The Audit Found",
            "",
        ]

        for item in requirements:
            lines.append(
                f"- `{item.requirement_id}`: `{item.classification}`. {item.evidence_summary}"
                + (f" Blocker: {item.blocker}" if item.blocker else "")
            )

        lines.extend(
            [
                "",
                "## Verdict",
                "",
                f"- Architecture structurally supported now: `{'yes' if verdict['architecture_structurally_supported'] else 'no'}`",
                f"- Scientifically ready to freeze as final Phase 1: `{'yes' if verdict['scientifically_ready'] else 'no'}`",
                f"- Full production rerun still needed: `{'yes' if verdict['full_production_rerun_required'] else 'no'}`",
                f"- Biggest remaining blocker: {verdict['biggest_remaining_blocker']}",
                "",
                "## Deferred Expensive Work",
                "",
                "- Build the real 2016-2022 regional drifter pool.",
                "- Filter it to the drogued-only core pool.",
                "- Generate accepted and rejected non-overlapping 72 h segment registries.",
                "- Export segment metrics, recipe summary, final recipe ranking, and then refresh the frozen baseline artifact from that corpus.",
            ]
        )
        _write_text(path, "\n".join(lines))
        return path

    def _write_verdict(self, verdict: dict[str, Any]) -> Path:
        path = self.output_dir / "phase1_final_verdict.md"
        lines = [
            "# Phase 1 Final Verdict",
            "",
            f"- Phase 1 architecture scientifically ready: `{'yes' if verdict['scientifically_ready'] else 'no'}`",
            f"- Full production rerun still needed: `{'yes' if verdict['full_production_rerun_required'] else 'no'}`",
            f"- Biggest remaining blocker: {verdict['biggest_remaining_blocker']}",
            "",
            "The current repo can support the final Chapter 3 Phase 1 architecture structurally, but the scientific freeze still depends on the later dedicated 2016-2022 regional production run.",
        ]
        _write_text(path, "\n".join(lines))
        return path

    def run(self) -> dict[str, Any]:
        requirements = self._build_requirements()
        smoke_checks = self._smoke_checks()
        verdict = self._build_verdict(requirements)

        status_csv = self._write_status_csv(requirements)
        status_json = self._write_status_json(requirements, smoke_checks, verdict)
        memo_md = self._write_memo(requirements, verdict)
        verdict_md = self._write_verdict(verdict)

        return {
            "phase": PHASE,
            "output_dir": str(self.output_dir),
            "status_csv": str(status_csv),
            "status_json": str(status_json),
            "memo_md": str(memo_md),
            "verdict_md": str(verdict_md),
            "smoke_checks": smoke_checks,
            "overall_verdict": verdict,
        }


def run_phase1_finalization_audit() -> dict[str, Any]:
    return Phase1FinalizationAuditService().run()


__all__ = [
    "PHASE",
    "OUTPUT_DIR",
    "Phase1FinalizationAuditService",
    "RequirementStatus",
    "run_phase1_finalization_audit",
]
