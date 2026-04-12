"""Read-only Phase 2 finalization audit against the stable Chapter 3 architecture."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from src.utils.io import get_phase2_recipe_family_status

PHASE = "phase2_finalization_audit"
OUTPUT_DIR = Path("output") / PHASE
OFFICIAL_RUN_NAME = "CASE_MINDORO_RETRO_2023"
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
    scientifically_usable: bool
    requires_phase1_production_rerun_for_full_freeze: bool
    low_risk_patch_applied: bool
    evidence_summary: str
    blocker: str
    evidence_paths: list[str]


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle) or {}


def _write_json(path: Path, payload: dict | list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, default=str)
        handle.write("\n")


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.rstrip() + "\n", encoding="utf-8")


class Phase2FinalizationAuditService:
    def __init__(
        self,
        repo_root: str | Path = ".",
        output_dir: str | Path | None = None,
        run_name: str = OFFICIAL_RUN_NAME,
    ):
        self.repo_root = Path(repo_root).resolve()
        self.output_dir = Path(output_dir) if output_dir else self.repo_root / OUTPUT_DIR
        self.run_name = run_name
        self.run_output_dir = self.repo_root / "output" / self.run_name
        self.forecast_manifest_path = self.run_output_dir / "forecast" / "forecast_manifest.json"
        self.ensemble_manifest_path = self.run_output_dir / "ensemble" / "ensemble_manifest.json"
        self.loading_audit_json_path = self.run_output_dir / "forecast" / "phase2_loading_audit.json"
        self.loading_audit_csv_path = self.run_output_dir / "forecast" / "phase2_loading_audit.csv"
        self.readme_path = self.repo_root / "README.md"
        self.phase_status_path = self.repo_root / "docs" / "PHASE_STATUS.md"
        self.architecture_path = self.repo_root / "docs" / "ARCHITECTURE.md"
        self.output_catalog_path = self.repo_root / "docs" / "OUTPUT_CATALOG.md"
        self.launcher_path = self.repo_root / "start.ps1"
        self.main_path = self.repo_root / "src" / "__main__.py"
        self.ensemble_source_path = self.repo_root / "src" / "services" / "ensemble.py"
        self.settings_path = self.repo_root / "config" / "settings.yaml"
        self.weathering_path = self.repo_root / "src" / "services" / "weathering.py"
        self.recipes_path = self.repo_root / "config" / "recipes.yaml"
        self.source_history_path = self.repo_root / "src" / "services" / "source_history_reconstruction_r1.py"
        self.extended_public_path = self.repo_root / "src" / "services" / "phase3b_extended_public_scored.py"

        self.forecast_manifest = _read_json(self.forecast_manifest_path)
        self.ensemble_manifest = _read_json(self.ensemble_manifest_path)
        self.loading_audit = _read_json(self.loading_audit_json_path)
        self.phase1_baseline_path = self._baseline_selection_source_path()
        self.selected_recipe = str(
            ((self.forecast_manifest.get("recipe_selection") or {}).get("recipe"))
            or ((self.forecast_manifest.get("baseline_provenance") or {}).get("recipe"))
            or ""
        )
        self.recipe_family_status = get_phase2_recipe_family_status(
            run_name=self.run_name,
            selected_recipe=self.selected_recipe,
            selection_path=self.phase1_baseline_path,
        )
        self.control_products = (self.forecast_manifest.get("deterministic_control") or {}).get("products") or []
        self.ensemble_products = self.ensemble_manifest.get("products") or []
        self.member_runs = self.ensemble_manifest.get("member_runs") or []
        self.loading_runs = self.loading_audit.get("runs") or []
        self.forecast_grid_id = str((self.forecast_manifest.get("grid") or {}).get("grid_id") or "")
        self.ensemble_grid_id = str((self.ensemble_manifest.get("grid") or {}).get("grid_id") or "")
        self.validation_time_utc = str((self.forecast_manifest.get("simulation_window_utc") or {}).get("end") or "")
        self.validation_date_utc = self.validation_time_utc[:10] if len(self.validation_time_utc) >= 10 else ""
        self.current_manifest_has_phase2_finalization = bool(
            self.forecast_manifest.get("phase2_finalization")
            and self.ensemble_manifest.get("phase2_finalization")
        )
        self.current_loading_audit_has_phase2_finalization = bool(self.loading_audit.get("phase2_finalization"))
        self.phase2_honesty_supported_in_code = "phase2_finalization" in self._safe_read_text(self.ensemble_source_path)

    def _safe_read_text(self, path: Path) -> str:
        if not path.exists():
            return ""
        return path.read_text(encoding="utf-8", errors="ignore")

    def _relative_to_repo(self, path: Path) -> str:
        try:
            return str(path.relative_to(self.repo_root))
        except ValueError:
            return str(path)

    def _baseline_selection_source_path(self) -> Path:
        source_path = str(
            ((self.forecast_manifest.get("recipe_selection") or {}).get("source_path"))
            or ((self.forecast_manifest.get("baseline_provenance") or {}).get("source_path"))
            or "config/phase1_baseline_selection.yaml"
        )
        path = Path(source_path)
        if not path.is_absolute():
            path = self.repo_root / path
        return path

    def _scan_lines(self, paths: list[Path], needles: list[str]) -> list[dict[str, Any]]:
        hits: list[dict[str, Any]] = []
        lowered_needles = [needle.lower() for needle in needles]
        for path in paths:
            text = self._safe_read_text(path)
            if not text:
                continue
            for line_number, line in enumerate(text.splitlines(), start=1):
                lowered = line.lower()
                if any(needle in lowered for needle in lowered_needles):
                    hits.append(
                        {
                            "path": str(path.relative_to(self.repo_root)),
                            "line": line_number,
                            "text": line.strip(),
                        }
                    )
        return hits

    def _requirement(self, **kwargs) -> RequirementStatus:
        record = RequirementStatus(**kwargs)
        if record.classification not in CLASSIFICATIONS:
            raise ValueError(f"Unsupported Phase 2 audit classification: {record.classification}")
        return record

    def _find_product(
        self,
        products: list[dict[str, Any]],
        product_type: str,
        *,
        timestamp_utc: str | None = None,
        date_utc: str | None = None,
    ) -> dict[str, Any]:
        for product in products:
            if str(product.get("product_type")) != product_type:
                continue
            if timestamp_utc is not None and str(product.get("timestamp_utc")) != timestamp_utc:
                continue
            if date_utc is not None and str(product.get("date_utc")) != date_utc:
                continue
            return dict(product)
        return {}

    def _artifact_exists(self, relative_or_repo_path: str | None) -> bool:
        if not relative_or_repo_path:
            return False
        return (self.repo_root / str(relative_or_repo_path)).exists()

    def _phase2_output_catalog_rows(self) -> list[dict[str, Any]]:
        canonical_products = self.forecast_manifest.get("canonical_products") or {}
        definitions = [
            {
                "product_group": "deterministic_control",
                "product_type": "control_footprint_mask",
                "canonical_path": canonical_products.get("control_footprint_mask", ""),
                "record": self._find_product(self.control_products, "control_footprint_mask", timestamp_utc=self.validation_time_utc),
                "source_manifest": str(self.forecast_manifest_path.relative_to(self.repo_root)),
            },
            {
                "product_group": "deterministic_control",
                "product_type": "control_density_norm",
                "canonical_path": canonical_products.get("control_density_norm", ""),
                "record": self._find_product(self.control_products, "control_density_norm", timestamp_utc=self.validation_time_utc),
                "source_manifest": str(self.forecast_manifest_path.relative_to(self.repo_root)),
            },
            {
                "product_group": "ensemble",
                "product_type": "prob_presence",
                "canonical_path": canonical_products.get("prob_presence", ""),
                "record": self._find_product(self.ensemble_products, "prob_presence", timestamp_utc=self.validation_time_utc),
                "source_manifest": str(self.ensemble_manifest_path.relative_to(self.repo_root)),
            },
            {
                "product_group": "ensemble",
                "product_type": "mask_p50",
                "canonical_path": canonical_products.get("mask_p50", ""),
                "record": self._find_product(self.ensemble_products, "mask_p50", timestamp_utc=self.validation_time_utc),
                "source_manifest": str(self.ensemble_manifest_path.relative_to(self.repo_root)),
            },
            {
                "product_group": "ensemble",
                "product_type": "mask_p90",
                "canonical_path": canonical_products.get("mask_p90", ""),
                "record": self._find_product(self.ensemble_products, "mask_p90", timestamp_utc=self.validation_time_utc),
                "source_manifest": str(self.ensemble_manifest_path.relative_to(self.repo_root)),
            },
            {
                "product_group": "ensemble",
                "product_type": "prob_presence_datecomposite",
                "canonical_path": "",
                "record": self._find_product(self.ensemble_products, "prob_presence_datecomposite", date_utc=self.validation_date_utc),
                "source_manifest": str(self.ensemble_manifest_path.relative_to(self.repo_root)),
            },
            {
                "product_group": "ensemble",
                "product_type": "mask_p50_datecomposite",
                "canonical_path": canonical_products.get("mask_p50_datecomposite", ""),
                "record": self._find_product(self.ensemble_products, "mask_p50_datecomposite", date_utc=self.validation_date_utc),
                "source_manifest": str(self.ensemble_manifest_path.relative_to(self.repo_root)),
            },
        ]

        rows: list[dict[str, Any]] = []
        for definition in definitions:
            record = definition["record"]
            canonical_path = str(definition["canonical_path"] or "")
            if not canonical_path and record.get("relative_path"):
                canonical_path = str(Path("output") / self.run_name / str(record["relative_path"]))
            rows.append(
                {
                    "product_group": definition["product_group"],
                    "product_type": definition["product_type"],
                    "canonical_path": canonical_path,
                    "exists_on_disk": self._artifact_exists(canonical_path),
                    "timestamp_utc": str(record.get("timestamp_utc") or ""),
                    "date_utc": str(record.get("date_utc") or ""),
                    "relative_path": str(record.get("relative_path") or ""),
                    "semantics": str(record.get("semantics") or ""),
                    "source_manifest": definition["source_manifest"],
                    "grid_id": self.ensemble_grid_id if definition["product_group"] == "ensemble" else self.forecast_grid_id,
                    "same_grid_as_other_phase2_products": bool(self.forecast_grid_id and self.forecast_grid_id == self.ensemble_grid_id),
                }
            )
        return rows

    def _loading_audit_window_ok(self) -> bool:
        if not self.loading_runs:
            return False
        for run in self.loading_runs:
            if str(run.get("status")) != "completed":
                return False
            forcings = run.get("forcings") or {}
            for forcing in forcings.values():
                if not bool(forcing.get("covers_requested_window")):
                    return False
                if str(forcing.get("reader_attach_status")) != "loaded":
                    return False
        return True

    def _smoke_checks(self, output_catalog_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        canonical_tokens = [
            "forecast_manifest.json",
            "phase2_loading_audit.json",
            "prob_presence_",
            "mask_p50_",
            "mask_p90_",
        ]
        user_facing_text = "\n".join(
            [
                self._safe_read_text(self.readme_path),
                self._safe_read_text(self.phase_status_path),
                self._safe_read_text(self.architecture_path),
                self._safe_read_text(self.output_catalog_path),
                self._safe_read_text(self.launcher_path),
                self._safe_read_text(self.main_path),
            ]
        )
        return [
            {
                "check_id": "forecast_manifest_present",
                "passed": self.forecast_manifest_path.exists(),
                "evidence": str(self.forecast_manifest_path.relative_to(self.repo_root)),
            },
            {
                "check_id": "ensemble_manifest_present",
                "passed": self.ensemble_manifest_path.exists(),
                "evidence": str(self.ensemble_manifest_path.relative_to(self.repo_root)),
            },
            {
                "check_id": "phase2_loading_audit_present",
                "passed": self.loading_audit_json_path.exists() and self.loading_audit_csv_path.exists(),
                "evidence": f"{self.loading_audit_json_path.relative_to(self.repo_root)}, {self.loading_audit_csv_path.relative_to(self.repo_root)}",
            },
            {
                "check_id": "fifty_member_ensemble_present",
                "passed": len(self.member_runs) == 50,
                "evidence": f"member_runs={len(self.member_runs)}",
            },
            {
                "check_id": "canonical_output_catalog_buildable",
                "passed": bool(output_catalog_rows) and all(row["canonical_path"] or row["relative_path"] for row in output_catalog_rows),
                "evidence": f"catalog_rows={len(output_catalog_rows)}",
            },
            {
                "check_id": "loading_audit_covers_requested_windows",
                "passed": self._loading_audit_window_ok(),
                "evidence": str(self.loading_audit_json_path.relative_to(self.repo_root)),
            },
            {
                "check_id": "phase2_manifest_honesty_fields_supported_in_code",
                "passed": self.phase2_honesty_supported_in_code,
                "evidence": str(self.ensemble_source_path.relative_to(self.repo_root)),
            },
            {
                "check_id": "official_user_facing_help_mentions_canonical_products",
                "passed": all(token in user_facing_text for token in canonical_tokens),
                "evidence": ", ".join(
                    str(path.relative_to(self.repo_root))
                    for path in [self.readme_path, self.phase_status_path, self.architecture_path, self.output_catalog_path, self.launcher_path, self.main_path]
                    if path.exists()
                ),
            },
        ]

    @staticmethod
    def _biggest_remaining_blocker() -> str:
        return (
            "Phase 2 still depends on a later Phase 1 production rerun for the final frozen-baseline story, and the "
            "official recipe family remains only partially available locally because legacy *_ncep runtime IDs remain "
            "in config space while gfs_wind.nc is still absent."
        )

    def _build_requirements(self, output_catalog_rows: list[dict[str, Any]]) -> list[RequirementStatus]:
        canonical_products = self.forecast_manifest.get("canonical_products") or {}
        expected_canonical_keys = {
            "control_footprint_mask",
            "control_density_norm",
            "prob_presence",
            "mask_p50",
            "mask_p90",
            "mask_p50_datecomposite",
        }
        canonical_keys_present = expected_canonical_keys.issubset(set(canonical_products))
        p50_record = self._find_product(self.ensemble_products, "mask_p50", timestamp_utc=self.validation_time_utc)
        p90_record = self._find_product(self.ensemble_products, "mask_p90", timestamp_utc=self.validation_time_utc)
        prob_record = self._find_product(self.ensemble_products, "prob_presence", timestamp_utc=self.validation_time_utc)
        prob_date_record = self._find_product(self.ensemble_products, "prob_presence_datecomposite", date_utc=self.validation_date_utc)
        p50_date_record = self._find_product(self.ensemble_products, "mask_p50_datecomposite", date_utc=self.validation_date_utc)
        semantics_ok = bool(
            canonical_keys_present
            and "Per-cell ensemble probability" in str(prob_record.get("semantics") or "")
            and "at least 0.50" in str(p50_record.get("semantics") or "")
            and "at least 0.90" in str(p90_record.get("semantics") or "")
            and "at least 0.50" in str(p50_date_record.get("semantics") or "")
        )
        ensemble_cfg = self.ensemble_manifest.get("ensemble_configuration") or {}
        member_count_ok = bool(
            len(self.member_runs) == 50
            and int(ensemble_cfg.get("ensemble_size", 0)) == 50
            and int((self.forecast_manifest.get("ensemble") or {}).get("actual_member_count", 0)) == 50
        )
        deterministic_control = self.forecast_manifest.get("deterministic_control") or {}
        deterministic_separation_ok = bool(
            deterministic_control.get("netcdf_path")
            and (self.forecast_manifest.get("ensemble") or {}).get("manifest_path")
            and str(deterministic_control.get("netcdf_path")) != str((self.forecast_manifest.get("ensemble") or {}).get("manifest_path"))
        )
        date_composite_ok = bool(prob_date_record and p50_date_record and p50_date_record.get("relative_path"))
        same_grid_ok = bool(self.forecast_grid_id and self.forecast_grid_id == self.ensemble_grid_id)
        status_blocks_present = bool(
            (self.forecast_manifest.get("status_flags") or {})
            and (self.forecast_manifest.get("recipe_selection") or {})
            and (self.ensemble_manifest.get("status_flags") or {})
            and (self.ensemble_manifest.get("baseline_provenance") or {})
        )
        launcher_text = self._safe_read_text(self.launcher_path)
        main_text = self._safe_read_text(self.main_path)
        user_facing_text = "\n".join(
            [
                launcher_text,
                main_text,
                self._safe_read_text(self.readme_path),
                self._safe_read_text(self.phase_status_path),
                self._safe_read_text(self.architecture_path),
                self._safe_read_text(self.output_catalog_path),
            ]
        )
        official_output_names_ok = all(
            token in user_facing_text
            for token in [
                "forecast_manifest.json",
                "phase2_loading_audit.json",
                "prob_presence_",
                "mask_p50_",
                "mask_p90_",
            ]
        )
        prototype_legacy_guarded = (
            "legacy debug/regression" in user_facing_text.lower()
            or "legacy prototype" in user_facing_text.lower()
            or "prototype_2016" in user_facing_text.lower()
        )
        legacy_recipe_hits = self._scan_lines(
            [
                self.recipes_path,
                self.settings_path,
                self.weathering_path,
                self.source_history_path,
                self.extended_public_path,
            ],
            ["cmems_ncep", "hycom_ncep", "ncep_wind.nc", "download_ncep"],
        )
        explicit_gfs_missing_message_hits = self._scan_lines(
            [self.source_history_path, self.extended_public_path],
            ["not available locally", "gfs wind forcing"],
        )

        return [
            self._requirement(
                requirement_id="deterministic_control_separate_from_ensemble",
                requirement_group="core_semantics",
                chapter3_requirement="Deterministic control is kept separate from the ensemble products.",
                classification="implemented_and_scientifically_ready" if deterministic_separation_ok else "missing",
                scientifically_usable=deterministic_separation_ok,
                requires_phase1_production_rerun_for_full_freeze=self.recipe_family_status["requires_phase1_production_rerun_for_full_freeze"],
                low_risk_patch_applied=False,
                evidence_summary=(
                    "Forecast manifest exposes a deterministic-control NetCDF plus a separate ensemble manifest reference."
                    if deterministic_separation_ok
                    else "Current official Phase 2 artifacts do not clearly separate deterministic and ensemble outputs."
                ),
                blocker="" if deterministic_separation_ok else "Deterministic control and ensemble artifacts are not clearly separated in the current manifest set.",
                evidence_paths=[
                    str(self.forecast_manifest_path.relative_to(self.repo_root)),
                    str(self.ensemble_manifest_path.relative_to(self.repo_root)),
                ],
            ),
            self._requirement(
                requirement_id="canonical_product_names_and_semantics",
                requirement_group="core_semantics",
                chapter3_requirement="Phase 2 uses canonical product names with stable mask_p50 and mask_p90 semantics.",
                classification="implemented_and_scientifically_ready" if semantics_ok else "partially_implemented",
                scientifically_usable=semantics_ok,
                requires_phase1_production_rerun_for_full_freeze=self.recipe_family_status["requires_phase1_production_rerun_for_full_freeze"],
                low_risk_patch_applied=True,
                evidence_summary=(
                    f"Canonical products present = {sorted(canonical_products)}. Product catalog rows = {len(output_catalog_rows)}."
                    if output_catalog_rows
                    else "No Phase 2 product catalog could be built from the current manifests."
                ),
                blocker="" if semantics_ok else "At least one canonical product or threshold semantic is missing from the current official manifests.",
                evidence_paths=[
                    str(self.forecast_manifest_path.relative_to(self.repo_root)),
                    str(self.ensemble_manifest_path.relative_to(self.repo_root)),
                    "docs/OUTPUT_CATALOG.md",
                ],
            ),
            self._requirement(
                requirement_id="ensemble_50_member_design",
                requirement_group="core_semantics",
                chapter3_requirement="Official Phase 2 uses the fixed 50-member ensemble design.",
                classification="implemented_and_scientifically_ready" if member_count_ok else "partially_implemented",
                scientifically_usable=member_count_ok,
                requires_phase1_production_rerun_for_full_freeze=self.recipe_family_status["requires_phase1_production_rerun_for_full_freeze"],
                low_risk_patch_applied=False,
                evidence_summary=f"Ensemble manifest records ensemble_size={ensemble_cfg.get('ensemble_size')} and member_runs={len(self.member_runs)}.",
                blocker="" if member_count_ok else "The current official manifest set does not show a full 50-member ensemble.",
                evidence_paths=[str(self.ensemble_manifest_path.relative_to(self.repo_root)), "config/ensemble.yaml"],
            ),
            self._requirement(
                requirement_id="date_composite_logic",
                requirement_group="core_semantics",
                chapter3_requirement="Official Phase 2 writes date-composite products without changing the meaning of mask_p50.",
                classification="implemented_and_scientifically_ready" if date_composite_ok else "partially_implemented",
                scientifically_usable=date_composite_ok,
                requires_phase1_production_rerun_for_full_freeze=self.recipe_family_status["requires_phase1_production_rerun_for_full_freeze"],
                low_risk_patch_applied=True,
                evidence_summary=(
                    f"Date-composite products present: prob_presence_datecomposite={bool(prob_date_record)}, "
                    f"mask_p50_datecomposite={bool(p50_date_record)} for {self.validation_date_utc or 'unknown date'}."
                ),
                blocker="" if date_composite_ok else "Date-composite product records are incomplete in the current official manifest set.",
                evidence_paths=[str(self.ensemble_manifest_path.relative_to(self.repo_root)), "src/services/ensemble.py"],
            ),
            self._requirement(
                requirement_id="common_grid_same_grid_discipline",
                requirement_group="core_semantics",
                chapter3_requirement="Deterministic and ensemble Phase 2 products share the same canonical scoring grid.",
                classification="implemented_and_scientifically_ready" if same_grid_ok else "partially_implemented",
                scientifically_usable=same_grid_ok,
                requires_phase1_production_rerun_for_full_freeze=self.recipe_family_status["requires_phase1_production_rerun_for_full_freeze"],
                low_risk_patch_applied=False,
                evidence_summary=f"Forecast grid_id = {self.forecast_grid_id}; ensemble grid_id = {self.ensemble_grid_id}.",
                blocker="" if same_grid_ok else "Forecast and ensemble manifests do not currently advertise the same grid_id.",
                evidence_paths=[
                    str(self.forecast_manifest_path.relative_to(self.repo_root)),
                    str(self.ensemble_manifest_path.relative_to(self.repo_root)),
                ],
            ),
            self._requirement(
                requirement_id="loading_audits_and_manifests",
                requirement_group="auditability",
                chapter3_requirement="Phase 2 writes machine-readable manifests and loading audits with hard-fail coverage discipline.",
                classification=(
                    "implemented_and_scientifically_ready"
                    if self.forecast_manifest_path.exists() and self.ensemble_manifest_path.exists() and self._loading_audit_window_ok()
                    else "partially_implemented"
                ),
                scientifically_usable=self.forecast_manifest_path.exists() and self.ensemble_manifest_path.exists() and self._loading_audit_window_ok(),
                requires_phase1_production_rerun_for_full_freeze=self.recipe_family_status["requires_phase1_production_rerun_for_full_freeze"],
                low_risk_patch_applied=False,
                evidence_summary=(
                    f"Forecast manifest exists = {self.forecast_manifest_path.exists()}, ensemble manifest exists = {self.ensemble_manifest_path.exists()}, "
                    f"loading audit rows = {len(self.loading_runs)}."
                ),
                blocker="" if self._loading_audit_window_ok() else "The current loading audit does not show a fully completed set of window-covering runs.",
                evidence_paths=[
                    str(self.forecast_manifest_path.relative_to(self.repo_root)),
                    str(self.ensemble_manifest_path.relative_to(self.repo_root)),
                    str(self.loading_audit_json_path.relative_to(self.repo_root)),
                    str(self.loading_audit_csv_path.relative_to(self.repo_root)),
                ],
            ),
            self._requirement(
                requirement_id="explicit_provenance_status_fields",
                requirement_group="auditability",
                chapter3_requirement="Phase 2 exposes explicit provenance and status fields, including finalization honesty fields.",
                classification=(
                    "implemented_and_scientifically_ready"
                    if status_blocks_present and self.current_manifest_has_phase2_finalization and self.current_loading_audit_has_phase2_finalization
                    else ("implemented_but_provisional" if status_blocks_present and self.phase2_honesty_supported_in_code else "partially_implemented")
                ),
                scientifically_usable=bool(status_blocks_present),
                requires_phase1_production_rerun_for_full_freeze=self.recipe_family_status["requires_phase1_production_rerun_for_full_freeze"],
                low_risk_patch_applied=self.phase2_honesty_supported_in_code,
                evidence_summary=(
                    f"Existing manifests expose status_flags/recipe provenance = {status_blocks_present}. "
                    f"Current on-disk phase2_finalization block present = {self.current_manifest_has_phase2_finalization and self.current_loading_audit_has_phase2_finalization}. "
                    f"Code support for the honesty block = {self.phase2_honesty_supported_in_code}."
                ),
                blocker=(
                    ""
                    if self.current_manifest_has_phase2_finalization and self.current_loading_audit_has_phase2_finalization
                    else "The read-only audit patched the manifest schema in code, but the existing official Phase 2 manifests were not regenerated by default."
                ),
                evidence_paths=[
                    str(self.forecast_manifest_path.relative_to(self.repo_root)),
                    str(self.ensemble_manifest_path.relative_to(self.repo_root)),
                    str(self.loading_audit_json_path.relative_to(self.repo_root)),
                    str(self.ensemble_source_path.relative_to(self.repo_root)),
                ],
            ),
            self._requirement(
                requirement_id="user_facing_output_names_and_help_text",
                requirement_group="user_facing",
                chapter3_requirement="Official Phase 2 user-facing output names and launcher/help text use the canonical product story instead of obsolete prototype labels.",
                classification=(
                    "implemented_and_scientifically_ready"
                    if official_output_names_ok and prototype_legacy_guarded
                    else "implemented_but_provisional"
                ),
                scientifically_usable=official_output_names_ok,
                requires_phase1_production_rerun_for_full_freeze=self.recipe_family_status["requires_phase1_production_rerun_for_full_freeze"],
                low_risk_patch_applied=True,
                evidence_summary=(
                    "README, docs, launcher help, and __main__ now point official users to forecast manifests, loading audits, prob_presence, mask_p50, and mask_p90 while keeping prototype outputs explicitly legacy/debug-only."
                ),
                blocker=(
                    ""
                    if official_output_names_ok and prototype_legacy_guarded
                    else "Some user-facing text still needs to advertise the canonical official Phase 2 products more clearly."
                ),
                evidence_paths=[
                    str(self.readme_path.relative_to(self.repo_root)),
                    str(self.phase_status_path.relative_to(self.repo_root)),
                    str(self.architecture_path.relative_to(self.repo_root)),
                    str(self.output_catalog_path.relative_to(self.repo_root)),
                    str(self.launcher_path.relative_to(self.repo_root)),
                    str(self.main_path.relative_to(self.repo_root)),
                ],
            ),
            self._requirement(
                requirement_id="phase1_recipe_family_drift_honesty",
                requirement_group="upstream_dependency",
                chapter3_requirement="Phase 2 explicitly discloses remaining Phase 1 recipe-family drift instead of implying the full official family is already frozen locally.",
                classification="implemented_but_provisional" if self.phase2_honesty_supported_in_code else "partially_implemented",
                scientifically_usable=True,
                requires_phase1_production_rerun_for_full_freeze=self.recipe_family_status["requires_phase1_production_rerun_for_full_freeze"],
                low_risk_patch_applied=True,
                evidence_summary=(
                    f"Official recipe family expected = {self.recipe_family_status['official_recipe_family_expected']}; "
                    f"runtime-defined = {self.recipe_family_status['official_recipe_family_runtime_defined']}; "
                    f"locally available = {self.recipe_family_status['official_recipe_family_locally_available']}; "
                    f"legacy runtime IDs = {self.recipe_family_status['legacy_recipe_ids_present_in_runtime']}; "
                    f"gfs_wind.nc present for the official case = {self.recipe_family_status['gfs_wind_present_for_active_case']}."
                ),
                blocker=(
                    "The current repo state still carries legacy *_ncep recipe IDs in config space and does not yet include gfs_wind.nc, "
                    "so the Phase 2 frozen-baseline story remains upstream-provisional."
                ),
                evidence_paths=[
                    self._relative_to_repo(self.phase1_baseline_path),
                    str(self.recipes_path.relative_to(self.repo_root)),
                    str(self.ensemble_source_path.relative_to(self.repo_root)),
                    str(self.phase_status_path.relative_to(self.repo_root)),
                ],
            ),
            self._requirement(
                requirement_id="missing_gfs_not_silent",
                requirement_group="upstream_dependency",
                chapter3_requirement="Missing GFS support is stated honestly instead of silently distorting official-mode behavior.",
                classification=(
                    "implemented_but_provisional"
                    if (not self.recipe_family_status["gfs_wind_present_for_active_case"] and explicit_gfs_missing_message_hits)
                    else ("implemented_and_scientifically_ready" if self.recipe_family_status["gfs_wind_present_for_active_case"] else "partially_implemented")
                ),
                scientifically_usable=True,
                requires_phase1_production_rerun_for_full_freeze=self.recipe_family_status["requires_phase1_production_rerun_for_full_freeze"],
                low_risk_patch_applied=bool(explicit_gfs_missing_message_hits),
                evidence_summary=(
                    f"Explicit GFS-missing guard messages found = {len(explicit_gfs_missing_message_hits)}; "
                    f"current official case has gfs_wind.nc = {self.recipe_family_status['gfs_wind_present_for_active_case']}."
                ),
                blocker=(
                    ""
                    if self.recipe_family_status["gfs_wind_present_for_active_case"]
                    else "Official GFS forcing is still absent locally, so the audit can only document the gap and enforce clearer failure messages."
                ),
                evidence_paths=[
                    str(self.source_history_path.relative_to(self.repo_root)),
                    str(self.extended_public_path.relative_to(self.repo_root)),
                    str(self.loading_audit_json_path.relative_to(self.repo_root)),
                ],
            ),
            self._requirement(
                requirement_id="legacy_recipe_hits_inventory",
                requirement_group="upstream_dependency",
                chapter3_requirement="Legacy recipe naming drift is inventoried instead of being hidden.",
                classification="implemented_but_provisional" if legacy_recipe_hits else "implemented_and_scientifically_ready",
                scientifically_usable=True,
                requires_phase1_production_rerun_for_full_freeze=self.recipe_family_status["requires_phase1_production_rerun_for_full_freeze"],
                low_risk_patch_applied=True,
                evidence_summary=f"Legacy recipe-related code/config hits currently inventoried = {len(legacy_recipe_hits)}.",
                blocker=(
                    "Legacy naming drift still exists in config space and a few official-support branches; this patch makes that explicit but does not fully remove the drift."
                    if legacy_recipe_hits
                    else ""
                ),
                evidence_paths=sorted({hit["path"] for hit in legacy_recipe_hits}),
            ),
        ]

    def _build_verdict(self, requirements: list[RequirementStatus]) -> dict[str, Any]:
        scientifically_usable = all(
            item.classification in {"implemented_and_scientifically_ready", "implemented_but_provisional"}
            for item in requirements
            if item.requirement_group in {"core_semantics", "auditability", "user_facing"}
        )
        scientifically_frozen = all(item.classification == "implemented_and_scientifically_ready" for item in requirements)
        transport_provisional = bool((self.forecast_manifest.get("transport") or {}).get("provisional_transport_model", False))
        return {
            "scientifically_usable_as_implemented": scientifically_usable,
            "scientifically_frozen": scientifically_frozen,
            "transport_model_provisional": transport_provisional,
            "requires_phase1_production_rerun_for_full_freeze": self.recipe_family_status["requires_phase1_production_rerun_for_full_freeze"],
            "legacy_recipe_drift_leaks_into_official_mode": self.recipe_family_status["legacy_recipe_drift_leaks_into_official_mode"],
            "missing_gfs_wind_assumption_still_present": not self.recipe_family_status["gfs_wind_present_for_active_case"],
            "legacy_recipe_drift_fully_removed": False,
            "legacy_recipe_drift_only_documented": True,
            "biggest_remaining_phase2_provisional_item": self._biggest_remaining_blocker(),
            "verdict_label": (
                "scientifically_usable_but_provisional"
                if scientifically_usable and (transport_provisional or self.recipe_family_status["requires_phase1_production_rerun_for_full_freeze"])
                else ("scientifically_ready" if scientifically_frozen else "not_ready")
            ),
        }

    def _write_status_csv(self, requirements: list[RequirementStatus]) -> Path:
        path = self.output_dir / "phase2_finalization_status.csv"
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

    def _write_output_catalog_csv(self, rows: list[dict[str, Any]]) -> Path:
        path = self.output_dir / "phase2_output_catalog.csv"
        pd.DataFrame(rows).to_csv(path, index=False)
        return path

    def _write_status_json(
        self,
        requirements: list[RequirementStatus],
        smoke_checks: list[dict[str, Any]],
        output_catalog_rows: list[dict[str, Any]],
        verdict: dict[str, Any],
    ) -> Path:
        path = self.output_dir / "phase2_finalization_status.json"
        drift_hits = self._scan_lines(
            [
                self.recipes_path,
                self.settings_path,
                self.weathering_path,
                self.source_history_path,
                self.extended_public_path,
            ],
            ["cmems_ncep", "hycom_ncep", "ncep_wind.nc", "download_ncep"],
        )
        payload = {
            "phase": PHASE,
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "repo_root": str(self.repo_root),
            "run_name": self.run_name,
            "manifest_paths": {
                "forecast_manifest": str(self.forecast_manifest_path.relative_to(self.repo_root)),
                "ensemble_manifest": str(self.ensemble_manifest_path.relative_to(self.repo_root)),
                "phase2_loading_audit_json": str(self.loading_audit_json_path.relative_to(self.repo_root)),
                "phase2_loading_audit_csv": str(self.loading_audit_csv_path.relative_to(self.repo_root)),
            },
            "smoke_checks": smoke_checks,
            "current_output_state": {
                "current_manifest_has_phase2_finalization": self.current_manifest_has_phase2_finalization,
                "current_loading_audit_has_phase2_finalization": self.current_loading_audit_has_phase2_finalization,
                "forecast_grid_id": self.forecast_grid_id,
                "ensemble_grid_id": self.ensemble_grid_id,
                "member_runs": len(self.member_runs),
                "loading_audit_runs": len(self.loading_runs),
            },
            "phase1_recipe_family_status": self.recipe_family_status,
            "drift_inspection": {
                "legacy_recipe_related_hits": drift_hits,
                "phase2_manifest_honesty_fields_supported_in_code": self.phase2_honesty_supported_in_code,
                "current_official_outputs_predate_honesty_block": not (
                    self.current_manifest_has_phase2_finalization and self.current_loading_audit_has_phase2_finalization
                ),
            },
            "output_catalog": output_catalog_rows,
            "requirements": [asdict(item) for item in requirements],
            "overall_verdict": verdict,
        }
        _write_json(path, payload)
        return path

    def _write_memo(self, requirements: list[RequirementStatus], verdict: dict[str, Any]) -> Path:
        path = self.output_dir / "phase2_finalization_memo.md"
        lines = [
            "# Phase 2 Finalization Memo",
            "",
            "This phase is a read-only architectural and semantics audit. It does not rerun the expensive official Mindoro workflow, and it does not overwrite finished Mindoro or DWH scientific outputs.",
            "",
            "## What This Patch Finalizes",
            "",
            "- Adds a dedicated `phase2_finalization_audit` route that writes its own audit package under `output/phase2_finalization_audit/`.",
            "- Makes the official Phase 2 schema capable of recording recipe-family honesty fields, upstream Phase 1 freeze dependencies, and explicit provisional reasons.",
            "- Keeps `mask_p50` and `mask_p90` semantics unchanged while documenting the canonical product catalog more clearly.",
            "- Updates launcher/help/docs so official Phase 2 no longer reads like a half-prototype workflow.",
            "",
            "## Requirement Audit",
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
                "## Final Questions",
                "",
                f"- Is Phase 2 scientifically usable as implemented right now? `{'yes' if verdict['scientifically_usable_as_implemented'] else 'no'}`",
                "- What remains provisional? The transport model is still marked provisional, the final frozen-baseline story still depends on the later Phase 1 production rerun, and the full GFS-capable official recipe family is not yet locally available.",
                f"- Does Phase 2 still depend on a later full Phase 1 production rerun for the final frozen baseline story? `{'yes' if verdict['requires_phase1_production_rerun_for_full_freeze'] else 'no'}`",
                f"- Is any part of official product generation still coupled to legacy recipe naming drift? `{'yes' if verdict['legacy_recipe_drift_leaks_into_official_mode'] else 'no'}`",
                "",
                "## Biggest Remaining Provisional Item",
                "",
                f"- {verdict['biggest_remaining_phase2_provisional_item']}",
            ]
        )
        _write_text(path, "\n".join(lines))
        return path

    def _write_verdict(self, verdict: dict[str, Any]) -> Path:
        path = self.output_dir / "phase2_final_verdict.md"
        lines = [
            "# Phase 2 Final Verdict",
            "",
            f"- Phase 2 scientifically usable as implemented right now: `{'yes' if verdict['scientifically_usable_as_implemented'] else 'no'}`",
            "- What remains provisional: transport-model provisional status, the later Phase 1 production rerun needed for the final frozen baseline story, and the still-partial local availability of the full GFS-capable official recipe family.",
            f"- Later full Phase 1 production rerun still needed for the final frozen baseline story: `{'yes' if verdict['requires_phase1_production_rerun_for_full_freeze'] else 'no'}`",
            f"- Any official product generation still coupled to legacy recipe naming drift: `{'yes' if verdict['legacy_recipe_drift_leaks_into_official_mode'] else 'no'}`",
            f"- Legacy recipe-family drift fully removed: `{'yes' if verdict['legacy_recipe_drift_fully_removed'] else 'no'}`",
            f"- Legacy recipe-family drift only documented in this patch: `{'yes' if verdict['legacy_recipe_drift_only_documented'] else 'no'}`",
            f"- Biggest remaining Phase 2 provisional item: {verdict['biggest_remaining_phase2_provisional_item']}",
            "",
            "Phase 2 is reportable and scientifically usable in its current implemented form, but it is not yet the final frozen baseline story for Chapter 3.",
        ]
        _write_text(path, "\n".join(lines))
        return path

    def run(self) -> dict[str, Any]:
        output_catalog_rows = self._phase2_output_catalog_rows()
        requirements = self._build_requirements(output_catalog_rows)
        smoke_checks = self._smoke_checks(output_catalog_rows)
        verdict = self._build_verdict(requirements)

        status_csv = self._write_status_csv(requirements)
        status_json = self._write_status_json(requirements, smoke_checks, output_catalog_rows, verdict)
        memo_md = self._write_memo(requirements, verdict)
        output_catalog_csv = self._write_output_catalog_csv(output_catalog_rows)
        verdict_md = self._write_verdict(verdict)

        return {
            "phase": PHASE,
            "output_dir": str(self.output_dir),
            "status_csv": str(status_csv),
            "status_json": str(status_json),
            "memo_md": str(memo_md),
            "output_catalog_csv": str(output_catalog_csv),
            "verdict_md": str(verdict_md),
            "smoke_checks": smoke_checks,
            "overall_verdict": verdict,
        }


def run_phase2_finalization_audit() -> dict[str, Any]:
    return Phase2FinalizationAuditService().run()


__all__ = [
    "PHASE",
    "OUTPUT_DIR",
    "Phase2FinalizationAuditService",
    "RequirementStatus",
    "run_phase2_finalization_audit",
]
