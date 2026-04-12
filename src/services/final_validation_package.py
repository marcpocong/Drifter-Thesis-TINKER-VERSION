"""Read-only final validation package builder for completed Mindoro and DWH outputs."""

from __future__ import annotations

import json
from shutil import copy2
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import yaml

from src.services.mindoro_primary_validation_metadata import (
    MINDORO_BASE_CASE_CONFIG_PATH,
    MINDORO_LEGACY_MARCH6_TRACK_ID,
    MINDORO_LEGACY_MARCH6_TRACK_LABEL,
    MINDORO_LEGACY_SUPPORT_TRACK_ID,
    MINDORO_LEGACY_SUPPORT_TRACK_LABEL,
    MINDORO_PHASE1_CONFIRMATION_CANDIDATE_BASELINE_PATH,
    MINDORO_PHASE1_CONFIRMATION_INTERPRETATION_TEMPLATE,
    MINDORO_PHASE1_CONFIRMATION_WORKFLOW_MODE,
    MINDORO_PRIMARY_VALIDATION_AMENDMENT_PATH,
    MINDORO_PRIMARY_VALIDATION_FINAL_OUTPUT_DIR,
    MINDORO_PRIMARY_VALIDATION_LAUNCHER_ALIAS_ENTRY_ID,
    MINDORO_PRIMARY_VALIDATION_LAUNCHER_ENTRY_ID,
    MINDORO_PRIMARY_VALIDATION_MIGRATION_NOTE_PATH,
    MINDORO_PRIMARY_VALIDATION_THESIS_PHASE_TITLE,
    MINDORO_PRIMARY_VALIDATION_THESIS_SUBTITLE,
    MINDORO_PRIMARY_VALIDATION_TRACK_ID,
    MINDORO_PRIMARY_VALIDATION_TRACK_LABEL,
    MINDORO_SHARED_IMAGERY_CAVEAT,
)


PHASE = "final_validation_package"
OUTPUT_DIR = Path("output") / PHASE

MINDORO_CASE_ID = "CASE_MINDORO_RETRO_2023"
DWH_CASE_ID = "CASE_DWH_RETRO_2010_72H"

MINDORO_DIR = Path("output") / MINDORO_CASE_ID
DWH_DIR = Path("output") / DWH_CASE_ID

MINDORO_REINIT_DIR = MINDORO_DIR / "phase3b_extended_public_scored_march13_14_reinit"
MINDORO_REINIT_CROSSMODEL_DIR = MINDORO_DIR / "phase3b_extended_public_scored_march13_14_reinit_pygnome_comparison"
MINDORO_EXTENDED_PUBLIC_DIR = MINDORO_DIR / "phase3b_extended_public"
MINDORO_B1_FINAL_OUTPUT_DIR = MINDORO_PRIMARY_VALIDATION_FINAL_OUTPUT_DIR
MINDORO_B1_PUBLICATION_EXPORTS = {
    "march13_seed_mask_on_grid.png": Path("output")
    / "figure_package_publication"
    / "case_mindoro_retro_2023__phase3b_reinit_primary__observation__single_seed_observation__2023_03_13__single__paper__march13_seed_mask_on_grid.png",
    "march13_seed_vs_march14_target.png": Path("output")
    / "figure_package_publication"
    / "case_mindoro_retro_2023__phase3b_reinit_primary__observation__single_seed_target_compare__2023_03_13_to_2023_03_14__single__paper__march13_seed_vs_march14_target.png",
    "march14_r0_overlay.png": Path("output")
    / "figure_package_publication"
    / "case_mindoro_retro_2023__phase3b_reinit_primary__opendrift__single_primary_overlay__2023_03_14__single__paper__march14_r0_overlay.png",
    "march14_r1_previous_overlay.png": Path("output")
    / "figure_package_publication"
    / "case_mindoro_retro_2023__phase3b_reinit_primary__opendrift__single_primary_overlay__2023_03_14__single__paper__march14_r1_previous_overlay.png",
    "mindoro_primary_validation_board.png": Path("output")
    / "figure_package_publication"
    / "case_mindoro_retro_2023__phase3b_reinit_primary__opendrift__comparison_board__2023_03_13_to_2023_03_14__board__slide__mindoro_primary_validation_board.png",
}
MINDORO_B1_SCIENTIFIC_SOURCE_EXPORTS = {
    "qa_march13_seed_mask_on_grid.png": MINDORO_REINIT_DIR / "qa_march13_seed_mask_on_grid.png",
    "qa_march13_seed_vs_march14_target.png": MINDORO_REINIT_DIR / "qa_march13_seed_vs_march14_target.png",
    "qa_march14_reinit_R0_overlay.png": MINDORO_REINIT_DIR / "qa_march14_reinit_R0_overlay.png",
    "qa_march14_reinit_R1_previous_overlay.png": MINDORO_REINIT_DIR / "qa_march14_reinit_R1_previous_overlay.png",
}

MINDORO_MARCH13_SOURCE_KEY = "8f8e3944748c4772910efc9829497e20"
MINDORO_MARCH14_SOURCE_KEY = "10b37c42a9754363a5f7b14199b077e6"

TRACK_SEQUENCE = {"A": 1, "B1": 2, "B2": 3, "B3": 4, "C1": 5, "C2": 6, "C3": 7}
FSS_WINDOWS_KM = (1, 3, 5, 10)
DWH_TRACK_LABELS = {
    "opendrift_control": "OpenDrift deterministic control",
    "ensemble_p50": "OpenDrift ensemble p50",
    "ensemble_p90": "OpenDrift ensemble p90",
    "pygnome_deterministic": "PyGNOME deterministic comparator",
}


def _json_default(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return None if np.isnan(value) else float(value)
    return value


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Required artifact is missing: {path}")
    return pd.read_csv(path)


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Required artifact is missing: {path}")
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def _read_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Required artifact is missing: {path}")
    with open(path, "r", encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def _write_json(path: Path, payload: dict | list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, default=_json_default)
        handle.write("\n")


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.rstrip() + "\n", encoding="utf-8")


def _relative_to_repo(repo_root: Path, path: Path) -> str:
    try:
        return str(path.resolve().relative_to(repo_root.resolve()))
    except Exception:
        return str(path)


def mean_fss(record: pd.Series | dict[str, Any]) -> float:
    values: list[float] = []
    for window in FSS_WINDOWS_KM:
        try:
            value = float(record.get(f"fss_{window}km", np.nan))
        except (TypeError, ValueError):
            value = np.nan
        if np.isfinite(value):
            values.append(value)
    return float(np.mean(values)) if values else 0.0


def decide_final_structure() -> str:
    return (
        "Main text should emphasize Mindoro B1 as the March 13 -> March 14 NOAA reinit validation with an explicit "
        "caveat that both NOAA products cite March 12 WorldView-3 imagery, while DWH Phase 3C remains the rich-data "
        "transfer-validation success; comparative discussion should emphasize the March 13 -> March 14 Mindoro "
        "cross-model comparator and the DWH deterministic-vs-ensemble-vs-PyGNOME comparison; legacy/reference and "
        "appendix sections should retain the Mindoro March 6 sparse reference, the March 3-6 broader-support "
        "reference, recipe/init/source-history sensitivities, and any future DWH threshold or harmonization extensions."
    )


class FinalValidationPackageService:
    def __init__(self):
        self.repo_root = Path(".").resolve()
        self.output_dir = OUTPUT_DIR
        self.required_paths = [
            MINDORO_DIR / "phase3b" / "phase3b_summary.csv",
            MINDORO_DIR / "phase3b" / "phase3b_pairing_manifest.csv",
            MINDORO_DIR / "public_obs_appendix" / "public_obs_inventory.csv",
            MINDORO_DIR / "public_obs_appendix" / "appendix_eventcorridor_diagnostics.csv",
            MINDORO_REINIT_DIR / "march13_14_reinit_summary.csv",
            MINDORO_REINIT_DIR / "march13_14_reinit_branch_pairing_manifest.csv",
            MINDORO_REINIT_DIR / "march13_14_reinit_run_manifest.json",
            MINDORO_PHASE1_CONFIRMATION_CANDIDATE_BASELINE_PATH,
            MINDORO_REINIT_CROSSMODEL_DIR / "march13_14_reinit_crossmodel_summary.csv",
            MINDORO_REINIT_CROSSMODEL_DIR / "march13_14_reinit_crossmodel_run_manifest.json",
            MINDORO_EXTENDED_PUBLIC_DIR / "extended_public_obs_acceptance_registry.csv",
            MINDORO_DIR / "recipe_sensitivity_r1_multibranch" / "recipe_sensitivity_r1_multibranch_ranking.csv",
            MINDORO_DIR / "recipe_sensitivity_r1_multibranch" / "recipe_sensitivity_r1_multibranch_run_manifest.json",
            MINDORO_DIR / "init_mode_sensitivity_r1" / "init_mode_sensitivity_r1_run_manifest.json",
            MINDORO_DIR / "source_history_reconstruction_r1" / "source_history_reconstruction_r1_summary.csv",
            MINDORO_DIR / "source_history_reconstruction_r1" / "source_history_reconstruction_r1_run_manifest.json",
            MINDORO_DIR / "phase3b_extended_public" / "phase3b_extended_summary.csv",
            DWH_DIR / "phase3c_external_case_setup" / "external_case_source_taxonomy.csv",
            DWH_DIR / "phase3c_external_case_setup" / "external_case_service_inventory.csv",
            DWH_DIR / "phase3c_external_case_setup" / "phase3c_external_case_setup_manifest.json",
            DWH_DIR / "dwh_phase3c_scientific_forcing_ready" / "dwh_scientific_forcing_status.csv",
            DWH_DIR / "phase3c_external_case_run" / "phase3c_summary.csv",
            DWH_DIR / "phase3c_external_case_run" / "phase3c_eventcorridor_summary.csv",
            DWH_DIR / "phase3c_external_case_ensemble_comparison" / "phase3c_ensemble_summary.csv",
            DWH_DIR / "phase3c_external_case_ensemble_comparison" / "phase3c_ensemble_eventcorridor_summary.csv",
            DWH_DIR / "phase3c_dwh_pygnome_comparator" / "phase3c_dwh_pygnome_summary.csv",
            DWH_DIR / "phase3c_dwh_pygnome_comparator" / "phase3c_dwh_pygnome_eventcorridor_summary.csv",
            DWH_DIR / "phase3c_dwh_pygnome_comparator" / "phase3c_dwh_pygnome_run_manifest.json",
            *MINDORO_B1_PUBLICATION_EXPORTS.values(),
            *MINDORO_B1_SCIENTIFIC_SOURCE_EXPORTS.values(),
        ]

    def _assert_required_artifacts(self) -> None:
        missing = [str(path) for path in self.required_paths if not path.exists()]
        if missing:
            raise FileNotFoundError("Final validation packaging is missing required artifacts: " + "; ".join(missing))

    def _load_inputs(self) -> None:
        self.phase3b_summary = _read_csv(MINDORO_DIR / "phase3b" / "phase3b_summary.csv")
        self.phase3b_pairing = _read_csv(MINDORO_DIR / "phase3b" / "phase3b_pairing_manifest.csv")
        self.public_obs_inventory = _read_csv(MINDORO_DIR / "public_obs_appendix" / "public_obs_inventory.csv")
        self.appendix_eventcorridor_diag = _read_csv(
            MINDORO_DIR / "public_obs_appendix" / "appendix_eventcorridor_diagnostics.csv"
        )
        self.mindoro_reinit_summary = _read_csv(MINDORO_REINIT_DIR / "march13_14_reinit_summary.csv")
        self.mindoro_reinit_pairing = _read_csv(
            MINDORO_REINIT_DIR / "march13_14_reinit_branch_pairing_manifest.csv"
        )
        self.mindoro_reinit_manifest = _read_json(MINDORO_REINIT_DIR / "march13_14_reinit_run_manifest.json")
        self.mindoro_phase1_confirmation_candidate = _read_yaml(MINDORO_PHASE1_CONFIRMATION_CANDIDATE_BASELINE_PATH)
        self.mindoro_reinit_crossmodel_summary = _read_csv(
            MINDORO_REINIT_CROSSMODEL_DIR / "march13_14_reinit_crossmodel_summary.csv"
        )
        self.mindoro_reinit_crossmodel_manifest = _read_json(
            MINDORO_REINIT_CROSSMODEL_DIR / "march13_14_reinit_crossmodel_run_manifest.json"
        )
        self.extended_public_registry = _read_csv(
            MINDORO_EXTENDED_PUBLIC_DIR / "extended_public_obs_acceptance_registry.csv"
        )
        self.recipe_ranking = _read_csv(
            MINDORO_DIR / "recipe_sensitivity_r1_multibranch" / "recipe_sensitivity_r1_multibranch_ranking.csv"
        )
        self.recipe_manifest = _read_json(
            MINDORO_DIR / "recipe_sensitivity_r1_multibranch" / "recipe_sensitivity_r1_multibranch_run_manifest.json"
        )
        self.init_manifest = _read_json(
            MINDORO_DIR / "init_mode_sensitivity_r1" / "init_mode_sensitivity_r1_run_manifest.json"
        )
        self.source_history_summary = _read_csv(
            MINDORO_DIR / "source_history_reconstruction_r1" / "source_history_reconstruction_r1_summary.csv"
        )
        self.source_history_manifest = _read_json(
            MINDORO_DIR / "source_history_reconstruction_r1" / "source_history_reconstruction_r1_run_manifest.json"
        )
        self.extended_public_summary = _read_csv(
            MINDORO_DIR / "phase3b_extended_public" / "phase3b_extended_summary.csv"
        )
        self.dwh_source_taxonomy = _read_csv(
            DWH_DIR / "phase3c_external_case_setup" / "external_case_source_taxonomy.csv"
        )
        self.dwh_service_inventory = _read_csv(
            DWH_DIR / "phase3c_external_case_setup" / "external_case_service_inventory.csv"
        )
        self.dwh_setup_manifest = _read_json(
            DWH_DIR / "phase3c_external_case_setup" / "phase3c_external_case_setup_manifest.json"
        )
        self.dwh_forcing_status = _read_csv(
            DWH_DIR / "dwh_phase3c_scientific_forcing_ready" / "dwh_scientific_forcing_status.csv"
        )
        self.dwh_deterministic_summary = _read_csv(
            DWH_DIR / "phase3c_external_case_run" / "phase3c_summary.csv"
        )
        self.dwh_deterministic_event = _read_csv(
            DWH_DIR / "phase3c_external_case_run" / "phase3c_eventcorridor_summary.csv"
        )
        self.dwh_ensemble_summary = _read_csv(
            DWH_DIR / "phase3c_external_case_ensemble_comparison" / "phase3c_ensemble_summary.csv"
        )
        self.dwh_ensemble_event = _read_csv(
            DWH_DIR / "phase3c_external_case_ensemble_comparison" / "phase3c_ensemble_eventcorridor_summary.csv"
        )
        self.dwh_cross_model_summary = _read_csv(
            DWH_DIR / "phase3c_dwh_pygnome_comparator" / "phase3c_dwh_pygnome_summary.csv"
        )
        self.dwh_cross_model_event = _read_csv(
            DWH_DIR / "phase3c_dwh_pygnome_comparator" / "phase3c_dwh_pygnome_eventcorridor_summary.csv"
        )
        self.dwh_cross_model_manifest = _read_json(
            DWH_DIR / "phase3c_dwh_pygnome_comparator" / "phase3c_dwh_pygnome_run_manifest.json"
        )

    @staticmethod
    def _coerce_value(value: Any) -> Any:
        if pd.isna(value):
            return ""
        if isinstance(value, (np.integer,)):
            return int(value)
        if isinstance(value, (np.floating, float)):
            return float(value)
        return value

    def _format_validation_dates(self, row: pd.Series | dict[str, Any]) -> str:
        explicit = str(row.get("validation_dates", "") or "").strip()
        if explicit:
            return explicit
        validation_dates_used = str(row.get("validation_dates_used", "") or "").strip()
        if validation_dates_used:
            return validation_dates_used
        obs_date = str(row.get("obs_date", "") or "").strip()
        if obs_date:
            return obs_date
        pair_id = str(row.get("pair_id", "") or "")
        if "2010-05-21_2010-05-23" in pair_id:
            return "2010-05-21_to_2010-05-23"
        for date in ("2010-05-21", "2010-05-22", "2010-05-23", "2023-03-06"):
            if pair_id.endswith(date):
                return date
        if "2023-03-04_to_2023-03-06" in pair_id:
            return "2023-03-04_to_2023-03-06"
        if "2023-03-03_to_2023-03-06" in pair_id:
            return "2023-03-03_to_2023-03-06"
        return ""

    def _mindoro_primary_reinit_row(self) -> pd.Series:
        summary = self.mindoro_reinit_summary.copy()
        if "branch_id" in summary.columns:
            summary = summary[summary["branch_id"].astype(str) == "R1_previous"]
        if summary.empty:
            raise ValueError("Mindoro March 13 -> March 14 reinit summary is missing the R1_previous primary row.")
        return summary.iloc[0]

    def _mindoro_primary_reinit_pairing_row(self) -> pd.Series:
        pairing = self.mindoro_reinit_pairing.copy()
        if "branch_id" in pairing.columns:
            pairing = pairing[pairing["branch_id"].astype(str) == "R1_previous"]
        if pairing.empty:
            raise ValueError("Mindoro March 13 -> March 14 reinit pairing manifest is missing the R1_previous row.")
        return pairing.iloc[0]

    def _mindoro_legacy_strict_row(self) -> pd.Series:
        summary = self.phase3b_summary[self.phase3b_summary["pair_id"].astype(str) == "official_primary_march6"]
        if summary.empty:
            raise ValueError("Mindoro Phase 3B summary is missing the official_primary_march6 row.")
        return summary.iloc[0]

    def _mindoro_legacy_strict_pairing_row(self) -> pd.Series:
        pairing = self.phase3b_pairing[self.phase3b_pairing["pair_id"].astype(str) == "official_primary_march6"]
        if pairing.empty:
            raise ValueError("Mindoro Phase 3B pairing manifest is missing the official_primary_march6 row.")
        return pairing.iloc[0]

    def _mindoro_legacy_support_row(self) -> pd.Series:
        if self.appendix_eventcorridor_diag.empty:
            raise ValueError("Mindoro appendix event-corridor diagnostics are missing.")
        return self.appendix_eventcorridor_diag.iloc[0]

    def _mindoro_crossmodel_rows(self) -> pd.DataFrame:
        summary = self.mindoro_reinit_crossmodel_summary.copy()
        if summary.empty:
            raise ValueError("Mindoro March 13 -> March 14 cross-model summary is missing.")
        if "mean_fss" not in summary.columns:
            summary["mean_fss"] = summary.apply(mean_fss, axis=1)
        if "track_tie_break_order" not in summary.columns:
            summary["track_tie_break_order"] = range(len(summary))
        return summary

    def _mindoro_crossmodel_top_row(self) -> pd.Series:
        rows = self._mindoro_crossmodel_rows().copy()
        if "fss_1km" not in rows.columns:
            rows["fss_1km"] = 0.0
        if "iou" not in rows.columns:
            rows["iou"] = 0.0
        if "nearest_distance_to_obs_m" not in rows.columns:
            rows["nearest_distance_to_obs_m"] = np.nan
        rows.sort_values(
            ["mean_fss", "fss_1km", "iou", "nearest_distance_to_obs_m", "track_tie_break_order"],
            ascending=[False, False, False, True, True],
            inplace=True,
        )
        return rows.iloc[0]

    def _mindoro_dual_provenance_confirmation(self) -> dict[str, Any]:
        stored_recipe = str(self.mindoro_reinit_manifest.get("recipe", {}).get("recipe", "") or "")
        stored_recipe_source_path = str(self.mindoro_reinit_manifest.get("recipe", {}).get("source_path", "") or "")
        confirmation_recipe = str(self.mindoro_phase1_confirmation_candidate.get("selected_recipe", "") or "")
        matches = bool(stored_recipe and confirmation_recipe and stored_recipe == confirmation_recipe)
        return {
            "stored_run_recipe_source_path": stored_recipe_source_path,
            "stored_run_selected_recipe": stored_recipe,
            "posthoc_phase1_confirmation_workflow_mode": MINDORO_PHASE1_CONFIRMATION_WORKFLOW_MODE,
            "posthoc_phase1_confirmation_candidate_baseline_path": str(MINDORO_PHASE1_CONFIRMATION_CANDIDATE_BASELINE_PATH),
            "posthoc_phase1_confirmation_selected_recipe": confirmation_recipe,
            "matches_stored_b1_recipe": matches,
            "confirmation_interpretation": MINDORO_PHASE1_CONFIRMATION_INTERPRETATION_TEMPLATE.format(
                workflow_mode=MINDORO_PHASE1_CONFIRMATION_WORKFLOW_MODE,
                recipe=confirmation_recipe or stored_recipe or "unknown_recipe",
            ),
            "thesis_phase_title": MINDORO_PRIMARY_VALIDATION_THESIS_PHASE_TITLE,
            "thesis_phase_subtitle": MINDORO_PRIMARY_VALIDATION_THESIS_SUBTITLE,
        }

    def _mindoro_final_output_readme(self, confirmation: dict[str, Any]) -> str:
        matches_text = "Yes" if confirmation["matches_stored_b1_recipe"] else "No"
        return "\n".join(
            [
                "# Phase 3B March13-14 Final Output",
                "",
                f"Thesis-facing title: {MINDORO_PRIMARY_VALIDATION_THESIS_PHASE_TITLE}",
                "",
                "This is a read-only curated export of the promoted Mindoro B1 family.",
                "It does not replace the canonical scientific directory under `output/CASE_MINDORO_RETRO_2023/phase3b_extended_public_scored_march13_14_reinit/`.",
                "",
                "What is primary here:",
                "- B1 = Mindoro March 13 -> March 14 NOAA reinit primary validation.",
                "- The primary success statement is that the promoted OpenDrift row achieves non-zero FSS against the March 14 observed spill mask.",
                "- PyGNOME remains comparator-only; OpenDrift outperforming PyGNOME is supportive context, not truth replacement.",
                "",
                "What remains secondary:",
                "- March 6 remains a preserved legacy honesty/reference row and is not renamed as primary.",
                "- The separate March 13 -> March 14 cross-model comparator family is not exported here as the main result.",
                "",
                "Recipe-confirmation provenance:",
                f"- Stored B1 run recipe source path: `{confirmation['stored_run_recipe_source_path']}`",
                f"- Stored B1 run selected recipe: `{confirmation['stored_run_selected_recipe']}`",
                f"- Later drifter-confirmation workflow: `{confirmation['posthoc_phase1_confirmation_workflow_mode']}`",
                f"- Later drifter-confirmation candidate baseline: `{confirmation['posthoc_phase1_confirmation_candidate_baseline_path']}`",
                f"- Later drifter-confirmation selected recipe: `{confirmation['posthoc_phase1_confirmation_selected_recipe']}`",
                f"- Same recipe confirmed: `{matches_text}`",
                f"- Interpretation: {confirmation['confirmation_interpretation']}",
                "",
                "Shared-imagery caveat:",
                f"- {MINDORO_SHARED_IMAGERY_CAVEAT}",
            ]
        )

    def _build_mindoro_final_output_export(self, confirmation: dict[str, Any]) -> dict[str, Any]:
        export_dir = self.repo_root / MINDORO_B1_FINAL_OUTPUT_DIR
        publication_dir = export_dir / "publication"
        scientific_dir = export_dir / "scientific_source_pngs"
        summary_dir = export_dir / "summary"
        copied_files: list[dict[str, Any]] = []

        for destination_name, relative_source in MINDORO_B1_PUBLICATION_EXPORTS.items():
            source = self.repo_root / relative_source
            destination = publication_dir / destination_name
            destination.parent.mkdir(parents=True, exist_ok=True)
            copy2(source, destination)
            copied_files.append(
                {
                    "group": "publication",
                    "file_name": destination_name,
                    "relative_path": _relative_to_repo(self.repo_root, destination),
                    "source_path": _relative_to_repo(self.repo_root, source),
                }
            )

        for destination_name, relative_source in MINDORO_B1_SCIENTIFIC_SOURCE_EXPORTS.items():
            source = self.repo_root / relative_source
            destination = scientific_dir / destination_name
            destination.parent.mkdir(parents=True, exist_ok=True)
            copy2(source, destination)
            copied_files.append(
                {
                    "group": "scientific_source_pngs",
                    "file_name": destination_name,
                    "relative_path": _relative_to_repo(self.repo_root, destination),
                    "source_path": _relative_to_repo(self.repo_root, source),
                }
            )

        summary_sources = {
            "march13_14_reinit_summary.csv": MINDORO_REINIT_DIR / "march13_14_reinit_summary.csv",
            "march13_14_reinit_decision_note.md": MINDORO_REINIT_DIR / "march13_14_reinit_decision_note.md",
            "march13_14_reinit_run_manifest.json": MINDORO_REINIT_DIR / "march13_14_reinit_run_manifest.json",
        }
        for destination_name, relative_source in summary_sources.items():
            source = self.repo_root / relative_source
            destination = summary_dir / destination_name
            destination.parent.mkdir(parents=True, exist_ok=True)
            copy2(source, destination)
            copied_files.append(
                {
                    "group": "summary",
                    "file_name": destination_name,
                    "relative_path": _relative_to_repo(self.repo_root, destination),
                    "source_path": _relative_to_repo(self.repo_root, source),
                }
            )

        readme_path = export_dir / "README.md"
        _write_text(readme_path, self._mindoro_final_output_readme(confirmation))

        manifest_path = export_dir / "final_output_manifest.json"
        _write_json(
            manifest_path,
            {
                "title": MINDORO_PRIMARY_VALIDATION_THESIS_PHASE_TITLE,
                "subtitle": MINDORO_PRIMARY_VALIDATION_THESIS_SUBTITLE,
                "output_dir": _relative_to_repo(self.repo_root, export_dir),
                "canonical_scientific_output_dir": _relative_to_repo(self.repo_root, self.repo_root / MINDORO_REINIT_DIR),
                "read_only_export": True,
                "shared_imagery_caveat": MINDORO_SHARED_IMAGERY_CAVEAT,
                "dual_provenance_confirmation": confirmation,
                "exported_files": copied_files,
            },
        )

        return {
            "output_dir": _relative_to_repo(self.repo_root, export_dir),
            "readme_path": _relative_to_repo(self.repo_root, readme_path),
            "manifest_path": _relative_to_repo(self.repo_root, manifest_path),
            "copied_files": copied_files,
        }

    def _extended_obs_row(self, source_key: str) -> pd.Series:
        rows = self.extended_public_registry[self.extended_public_registry["source_key"].astype(str) == source_key]
        if rows.empty:
            raise ValueError(f"Extended public registry is missing source_key={source_key}.")
        return rows.iloc[0]

    def _build_dwh_main_row(self, row: pd.Series, track_id: str) -> dict[str, Any]:
        track_name = str(row.get("track_id", ""))
        model_comparator = DWH_TRACK_LABELS.get(track_name, str(row.get("track_id", "")))
        if track_id == "C1":
            track_label = "DWH deterministic external transfer validation"
            notes = "Scientific deterministic OpenDrift control using the frozen real historical forcing stack."
        elif track_id == "C2":
            track_label = "DWH ensemble extension and deterministic-vs-ensemble comparison"
            if track_name == "ensemble_p50":
                notes = "Scientific ensemble p50 track; strongest DWH overall mean FSS across per-date and event-corridor rows."
            else:
                notes = "Scientific ensemble threshold track used for comparison against deterministic and p50."
        else:
            track_label = "DWH PyGNOME comparator against the same DWH truth masks"
            notes = (
                "Comparator only; DWH observed masks remain truth. PyGNOME wave/Stokes handling is not identical to the "
                "OpenDrift scientific stack and should be interpreted as a cross-model comparison, not as truth."
            )
        transport_model = "pygnome" if track_name == "pygnome_deterministic" else "opendrift_oceandrift"
        return {
            "case_id": DWH_CASE_ID,
            "track_id": track_id,
            "track_label": track_label,
            "model_comparator": model_comparator,
            "validation_dates": self._format_validation_dates(row),
            "result_scope": str(row.get("pair_role", "")),
            "fss_1km": float(row["fss_1km"]),
            "fss_3km": float(row["fss_3km"]),
            "fss_5km": float(row["fss_5km"]),
            "fss_10km": float(row["fss_10km"]),
            "mean_fss": mean_fss(row),
            "iou": float(row["iou"]),
            "dice": float(row["dice"]),
            "centroid_distance_m": self._coerce_value(row["centroid_distance_m"]),
            "forecast_nonzero_cells": int(row["forecast_nonzero_cells"]),
            "obs_nonzero_cells": int(row["obs_nonzero_cells"]),
            "transport_model": transport_model,
            "provisional_transport_model": bool(row.get("provisional_transport_model", True)),
            "shoreline_mask_status": "dwh_epsg32616_scoring_grid_with_sea_mask_applied",
            "case_definition_path": "",
            "case_freeze_amendment_path": "",
            "base_case_definition_preserved": False,
            "row_role": "scientific_result" if track_id != "C3" else "comparator_only",
            "shared_imagery_caveat": "",
            "notes": notes,
            "source_summary_path": str(
                DWH_DIR
                / (
                    "phase3c_dwh_pygnome_comparator/phase3c_dwh_pygnome_summary.csv"
                    if track_id == "C3"
                    else (
                        "phase3c_external_case_ensemble_comparison/phase3c_ensemble_summary.csv"
                        if track_id == "C2"
                        else "phase3c_external_case_run/phase3c_summary.csv"
                    )
                )
            ),
            "source_pairing_path": "",
            "forecast_product_type": str(row.get("run_type", row.get("track_id", ""))),
        }

    def _build_main_table(self) -> pd.DataFrame:
        rows: list[dict[str, Any]] = []
        confirmation = self._mindoro_dual_provenance_confirmation()

        primary_row = self._mindoro_primary_reinit_row()
        primary_pairing = self._mindoro_primary_reinit_pairing_row()
        rows.append(
            {
                "case_id": MINDORO_CASE_ID,
                "track_id": MINDORO_PRIMARY_VALIDATION_TRACK_ID,
                "track_label": MINDORO_PRIMARY_VALIDATION_TRACK_LABEL,
                "model_comparator": "OpenDrift R1 previous reinit p50",
                "validation_dates": "2023-03-14",
                "result_scope": "primary_nextday_reinit_validation",
                "fss_1km": float(primary_row["fss_1km"]),
                "fss_3km": float(primary_row["fss_3km"]),
                "fss_5km": float(primary_row["fss_5km"]),
                "fss_10km": float(primary_row["fss_10km"]),
                "mean_fss": mean_fss(primary_row),
                "iou": float(primary_row["iou"]),
                "dice": float(primary_row["dice"]),
                "centroid_distance_m": self._coerce_value(primary_row["centroid_distance_m"]),
                "forecast_nonzero_cells": int(primary_row["forecast_nonzero_cells"]),
                "obs_nonzero_cells": int(primary_row["obs_nonzero_cells"]),
                "transport_model": "oceandrift",
                "provisional_transport_model": True,
                "shoreline_mask_status": "canonical_mindoro_scoring_grid_ocean_mask_applied",
                "case_definition_path": str(MINDORO_BASE_CASE_CONFIG_PATH),
                "case_freeze_amendment_path": str(MINDORO_PRIMARY_VALIDATION_AMENDMENT_PATH),
                "base_case_definition_preserved": True,
                "row_role": "primary_public_validation",
                "shared_imagery_caveat": MINDORO_SHARED_IMAGERY_CAVEAT,
                "thesis_phase_title": MINDORO_PRIMARY_VALIDATION_THESIS_PHASE_TITLE,
                "thesis_phase_subtitle": MINDORO_PRIMARY_VALIDATION_THESIS_SUBTITLE,
                **confirmation,
                "notes": (
                    "Phase 3B observation-based spatial validation using public Mindoro spill extents now foregrounds "
                    "the March 13 NOAA seed polygon and March 14 NOAA target through the completed OpenDrift R1 "
                    "previous p50 branch. The base March 3 -> March 6 case definition remains frozen in config, this "
                    "promoted row is authorized by a separate Phase 3B amendment, and the later 2016-2023 "
                    "Mindoro-focused drifter rerun confirmed the same cmems_era5 recipe without rewriting the stored "
                    "run provenance."
                ),
                "source_summary_path": str(MINDORO_REINIT_DIR / "march13_14_reinit_summary.csv"),
                "source_pairing_path": str(MINDORO_REINIT_DIR / "march13_14_reinit_branch_pairing_manifest.csv"),
                "forecast_product_type": str(primary_pairing["forecast_product"]),
            }
        )

        strict_row = self._mindoro_legacy_strict_row()
        strict_pairing = self._mindoro_legacy_strict_pairing_row()
        rows.append(
            {
                "case_id": MINDORO_CASE_ID,
                "track_id": MINDORO_LEGACY_MARCH6_TRACK_ID,
                "track_label": MINDORO_LEGACY_MARCH6_TRACK_LABEL,
                "model_comparator": "OpenDrift ensemble p50 official primary",
                "validation_dates": "2023-03-06",
                "result_scope": "legacy_sparse_single_date_reference",
                "fss_1km": float(strict_row["fss_1km"]),
                "fss_3km": float(strict_row["fss_3km"]),
                "fss_5km": float(strict_row["fss_5km"]),
                "fss_10km": float(strict_row["fss_10km"]),
                "mean_fss": mean_fss(strict_row),
                "iou": float(strict_row["iou"]),
                "dice": float(strict_row["dice"]),
                "centroid_distance_m": self._coerce_value(strict_row["centroid_distance_m"]),
                "forecast_nonzero_cells": int(strict_row["forecast_nonzero_cells"]),
                "obs_nonzero_cells": int(strict_row["obs_nonzero_cells"]),
                "transport_model": "oceandrift",
                "provisional_transport_model": True,
                "shoreline_mask_status": "canonical_mindoro_scoring_grid_ocean_mask_applied",
                "case_definition_path": str(MINDORO_BASE_CASE_CONFIG_PATH),
                "case_freeze_amendment_path": str(MINDORO_PRIMARY_VALIDATION_AMENDMENT_PATH),
                "base_case_definition_preserved": True,
                "row_role": "legacy_honesty_only",
                "shared_imagery_caveat": "",
                "notes": (
                    "Legacy honesty-only row preserved for methodology honesty. The accepted WWF March 6 "
                    "validation mask rasterized to only two observed ocean cells, so this remains valuable context "
                    "but no longer serves as the canonical Mindoro validation row."
                ),
                "source_summary_path": str(MINDORO_DIR / "phase3b" / "phase3b_summary.csv"),
                "source_pairing_path": str(MINDORO_DIR / "phase3b" / "phase3b_pairing_manifest.csv"),
                "forecast_product_type": str(strict_pairing["forecast_product_type"]),
            }
        )

        appendix_row = self._mindoro_legacy_support_row()
        rows.append(
            {
                "case_id": MINDORO_CASE_ID,
                "track_id": MINDORO_LEGACY_SUPPORT_TRACK_ID,
                "track_label": MINDORO_LEGACY_SUPPORT_TRACK_LABEL,
                "model_comparator": "OpenDrift ensemble p50 appendix support union",
                "validation_dates": "2023-03-03_to_2023-03-06",
                "result_scope": "legacy_broader_support_reference",
                "fss_1km": float(appendix_row["fss_1km"]),
                "fss_3km": float(appendix_row["fss_3km"]),
                "fss_5km": float(appendix_row["fss_5km"]),
                "fss_10km": float(appendix_row["fss_10km"]),
                "mean_fss": mean_fss(appendix_row),
                "iou": float(appendix_row["iou"]),
                "dice": float(appendix_row["dice"]),
                "centroid_distance_m": self._coerce_value(appendix_row["centroid_distance_m"]),
                "forecast_nonzero_cells": int(appendix_row["forecast_nonzero_cells"]),
                "obs_nonzero_cells": int(appendix_row["obs_nonzero_cells"]),
                "transport_model": "oceandrift",
                "provisional_transport_model": True,
                "shoreline_mask_status": "canonical_mindoro_scoring_grid_ocean_mask_applied",
                "case_definition_path": str(MINDORO_BASE_CASE_CONFIG_PATH),
                "case_freeze_amendment_path": str(MINDORO_PRIMARY_VALIDATION_AMENDMENT_PATH),
                "base_case_definition_preserved": True,
                "row_role": "legacy_support_only",
                "shared_imagery_caveat": "",
                "notes": (
                    "Legacy broader-support reference preserved for narrative context. This March 3-6 public "
                    "observation union remains informative, but it should not be confused with the promoted "
                    "March 13 -> March 14 primary validation track."
                ),
                "source_summary_path": str(MINDORO_DIR / "public_obs_appendix" / "appendix_eventcorridor_diagnostics.csv"),
                "source_pairing_path": str(MINDORO_DIR / "public_obs_appendix" / "appendix_eventcorridor_pairing_manifest.csv"),
                "forecast_product_type": "appendix_eventcorridor_model_union",
            }
        )

        for _, row in self._mindoro_crossmodel_rows().iterrows():
            rows.append(
                {
                    "case_id": MINDORO_CASE_ID,
                    "track_id": "A",
                    "track_label": "Mindoro March 13 -> March 14 cross-model comparator",
                    "model_comparator": str(row["model_name"]),
                    "validation_dates": "2023-03-14",
                    "result_scope": "primary_cross_model_reinit_compare",
                    "fss_1km": float(row["fss_1km"]),
                    "fss_3km": float(row["fss_3km"]),
                    "fss_5km": float(row["fss_5km"]),
                    "fss_10km": float(row["fss_10km"]),
                    "mean_fss": mean_fss(row),
                    "iou": float(row["iou"]),
                    "dice": float(row["dice"]),
                    "centroid_distance_m": self._coerce_value(row["centroid_distance_m"]),
                    "forecast_nonzero_cells": int(row["forecast_nonzero_cells"]),
                    "obs_nonzero_cells": int(row["obs_nonzero_cells"]),
                    "transport_model": str(row["transport_model"]),
                    "provisional_transport_model": bool(row["provisional_transport_model"]),
                    "shoreline_mask_status": "canonical_mindoro_scoring_grid_ocean_mask_applied",
                    "case_definition_path": str(MINDORO_BASE_CASE_CONFIG_PATH),
                    "case_freeze_amendment_path": str(MINDORO_PRIMARY_VALIDATION_AMENDMENT_PATH),
                    "base_case_definition_preserved": True,
                    "row_role": "comparator_only",
                    "shared_imagery_caveat": MINDORO_SHARED_IMAGERY_CAVEAT,
                    "notes": (
                        "Comparator track only; the accepted March 14 NOAA observation mask remains truth. "
                        + str(row.get("structural_limitations", "") or "")
                    ).strip(),
                    "source_summary_path": str(
                        MINDORO_REINIT_CROSSMODEL_DIR / "march13_14_reinit_crossmodel_summary.csv"
                    ),
                    "source_pairing_path": str(
                        MINDORO_REINIT_CROSSMODEL_DIR / "march13_14_reinit_crossmodel_pairing_manifest.csv"
                    ),
                    "forecast_product_type": str(row["forecast_product"]),
                }
            )

        for _, row in self.dwh_deterministic_summary.iterrows():
            rows.append(self._build_dwh_main_row(row, track_id="C1"))
        dwh_ensemble_rows = self.dwh_ensemble_summary[
            self.dwh_ensemble_summary["track_id"].astype(str).isin(["ensemble_p50", "ensemble_p90"])
        ]
        for _, row in dwh_ensemble_rows.iterrows():
            rows.append(self._build_dwh_main_row(row, track_id="C2"))
        dwh_pygnome_rows = self.dwh_cross_model_summary[
            self.dwh_cross_model_summary["track_id"].astype(str) == "pygnome_deterministic"
        ]
        for _, row in dwh_pygnome_rows.iterrows():
            rows.append(self._build_dwh_main_row(row, track_id="C3"))

        table = pd.DataFrame(rows)
        table["track_order"] = table["track_id"].map(TRACK_SEQUENCE).fillna(99)
        table.sort_values(["track_order", "case_id", "model_comparator", "validation_dates"], inplace=True)
        table.drop(columns=["track_order"], inplace=True)
        return table

    def _build_case_registry(self) -> pd.DataFrame:
        confirmation = self._mindoro_dual_provenance_confirmation()
        rows = [
            {
                "case_id": MINDORO_CASE_ID,
                "track_id": "A",
                "track_label": "Mindoro March 13 -> March 14 cross-model comparator",
                "status": "complete",
                "truth_source": "accepted March 14 NOAA/NESDIS observation mask",
                "primary_output_dir": str(MINDORO_REINIT_CROSSMODEL_DIR),
                "case_definition_path": str(MINDORO_BASE_CASE_CONFIG_PATH),
                "case_freeze_amendment_path": str(MINDORO_PRIMARY_VALIDATION_AMENDMENT_PATH),
                "launcher_entry_id": MINDORO_PRIMARY_VALIDATION_LAUNCHER_ALIAS_ENTRY_ID,
                "launcher_alias_entry_id": "",
                "row_role": "comparator_only",
                "reporting_role": "comparative discussion",
                "main_text_priority": "primary",
                "notes": (
                    "Comparator role only. PyGNOME is not truth, and the March 13/14 comparator must be reported "
                    "with the explicit caveat that both NOAA products cite March 12 WorldView-3 imagery."
                ),
            },
            {
                "case_id": MINDORO_CASE_ID,
                "track_id": MINDORO_PRIMARY_VALIDATION_TRACK_ID,
                "track_label": MINDORO_PRIMARY_VALIDATION_TRACK_LABEL,
                "status": "complete",
                "truth_source": "accepted March 14 NOAA/NESDIS observation mask with March 13 NOAA seed polygon",
                "primary_output_dir": str(MINDORO_REINIT_DIR),
                "case_definition_path": str(MINDORO_BASE_CASE_CONFIG_PATH),
                "case_freeze_amendment_path": str(MINDORO_PRIMARY_VALIDATION_AMENDMENT_PATH),
                "launcher_entry_id": MINDORO_PRIMARY_VALIDATION_LAUNCHER_ENTRY_ID,
                "launcher_alias_entry_id": MINDORO_PRIMARY_VALIDATION_LAUNCHER_ALIAS_ENTRY_ID,
                "row_role": "primary_public_validation",
                "reporting_role": "main-text primary validation",
                "main_text_priority": "primary",
                "thesis_phase_title": MINDORO_PRIMARY_VALIDATION_THESIS_PHASE_TITLE,
                "thesis_phase_subtitle": MINDORO_PRIMARY_VALIDATION_THESIS_SUBTITLE,
                **confirmation,
                "notes": (
                    "Promoted primary Mindoro row for Phase 3B observation-based spatial validation using public "
                    "Mindoro spill extents. The stored B1 run remains sourced from the completed R1_previous reinit "
                    "branch under the frozen baseline file, while the later 2016-2023 Mindoro-focused drifter rerun "
                    "confirmed the same cmems_era5 recipe without rewriting B1 provenance."
                ),
            },
            {
                "case_id": MINDORO_CASE_ID,
                "track_id": MINDORO_LEGACY_MARCH6_TRACK_ID,
                "track_label": MINDORO_LEGACY_MARCH6_TRACK_LABEL,
                "status": "complete",
                "truth_source": "accepted WWF March 6 validation mask",
                "primary_output_dir": str(MINDORO_DIR / "phase3b"),
                "case_definition_path": str(MINDORO_BASE_CASE_CONFIG_PATH),
                "case_freeze_amendment_path": str(MINDORO_PRIMARY_VALIDATION_AMENDMENT_PATH),
                "launcher_entry_id": "mindoro_reportable_core",
                "launcher_alias_entry_id": "",
                "row_role": "legacy_honesty_only",
                "reporting_role": "legacy reference",
                "main_text_priority": "secondary",
                "notes": "Legacy honesty-only reference preserved because the processed strict March 6 target is extremely small.",
            },
            {
                "case_id": MINDORO_CASE_ID,
                "track_id": MINDORO_LEGACY_SUPPORT_TRACK_ID,
                "track_label": MINDORO_LEGACY_SUPPORT_TRACK_LABEL,
                "status": "complete",
                "truth_source": "accepted within-horizon public observation union",
                "primary_output_dir": str(MINDORO_DIR / "public_obs_appendix"),
                "case_definition_path": str(MINDORO_BASE_CASE_CONFIG_PATH),
                "case_freeze_amendment_path": str(MINDORO_PRIMARY_VALIDATION_AMENDMENT_PATH),
                "launcher_entry_id": "mindoro_reportable_core",
                "launcher_alias_entry_id": "",
                "row_role": "legacy_support_only",
                "reporting_role": "legacy reference",
                "main_text_priority": "secondary",
                "notes": "Legacy broader-support reference only; keep it visible, but do not present it as the replacement primary row.",
            },
            {
                "case_id": DWH_CASE_ID,
                "track_id": "C1",
                "track_label": "DWH deterministic external transfer validation",
                "status": "complete",
                "truth_source": "DWH daily public observation-derived masks for 2010-05-21 to 2010-05-23",
                "primary_output_dir": str(DWH_DIR / "phase3c_external_case_run"),
                "case_definition_path": "",
                "case_freeze_amendment_path": "",
                "launcher_entry_id": "dwh_reportable_bundle",
                "launcher_alias_entry_id": "",
                "row_role": "scientific_result",
                "reporting_role": "main-text scientific result",
                "main_text_priority": "primary",
                "notes": "Real historical HYCOM + ERA5 + CMEMS wave/Stokes forcing stack.",
            },
            {
                "case_id": DWH_CASE_ID,
                "track_id": "C2",
                "track_label": "DWH ensemble extension and deterministic-vs-ensemble comparison",
                "status": "complete",
                "truth_source": "same DWH daily public masks as C1",
                "primary_output_dir": str(DWH_DIR / "phase3c_external_case_ensemble_comparison"),
                "case_definition_path": "",
                "case_freeze_amendment_path": "",
                "launcher_entry_id": "dwh_reportable_bundle",
                "launcher_alias_entry_id": "",
                "row_role": "scientific_result",
                "reporting_role": "comparative discussion",
                "main_text_priority": "primary",
                "notes": "p50 leads by overall mean FSS; deterministic remains strongest on the May 21-23 event corridor.",
            },
            {
                "case_id": DWH_CASE_ID,
                "track_id": "C3",
                "track_label": "DWH PyGNOME comparator",
                "status": "complete",
                "truth_source": "same DWH daily public masks as C1",
                "primary_output_dir": str(DWH_DIR / "phase3c_dwh_pygnome_comparator"),
                "case_definition_path": "",
                "case_freeze_amendment_path": "",
                "launcher_entry_id": "dwh_reportable_bundle",
                "launcher_alias_entry_id": "",
                "row_role": "comparator_only",
                "reporting_role": "comparative discussion",
                "main_text_priority": "secondary",
                "notes": "Comparator only; DWH observed masks remain truth.",
            },
            {
                "case_id": MINDORO_CASE_ID,
                "track_id": "appendix_sensitivity",
                "track_label": "Mindoro recipe/init/source-history sensitivities",
                "status": "reviewed_not_promoted",
                "truth_source": "same accepted public observation-derived masks used by the sensitivity branches",
                "primary_output_dir": "; ".join(
                    [
                        str(MINDORO_DIR / "recipe_sensitivity_r1_multibranch"),
                        str(MINDORO_DIR / "init_mode_sensitivity_r1"),
                        str(MINDORO_DIR / "source_history_reconstruction_r1"),
                    ]
                ),
                "case_definition_path": str(MINDORO_BASE_CASE_CONFIG_PATH),
                "case_freeze_amendment_path": str(MINDORO_PRIMARY_VALIDATION_AMENDMENT_PATH),
                "launcher_entry_id": "mindoro_appendix_sensitivity_bundle",
                "launcher_alias_entry_id": "",
                "row_role": "appendix_sensitivity",
                "reporting_role": "appendix_only",
                "main_text_priority": "appendix",
                "notes": "Sensitivity branches remain informative but do not replace the main thesis tracks.",
            },
        ]
        return pd.DataFrame(rows)

    def _build_benchmark_table(self) -> pd.DataFrame:
        rows: list[dict[str, Any]] = []
        primary_mean_fss = mean_fss(self._mindoro_primary_reinit_row())
        legacy_sparse_mean_fss = mean_fss(self._mindoro_legacy_strict_row())
        legacy_support_mean_fss = mean_fss(self._mindoro_legacy_support_row())
        mindoro_ranking = self._mindoro_crossmodel_rows().copy()
        mindoro_ranking.sort_values(
            ["mean_fss", "fss_1km", "iou", "nearest_distance_to_obs_m", "track_tie_break_order"],
            ascending=[False, False, False, True, True],
            inplace=True,
        )
        for rank, (_, row) in enumerate(mindoro_ranking.iterrows(), start=1):
            rows.append(
                {
                    "case_id": MINDORO_CASE_ID,
                    "benchmark_context": "Mindoro March 13 -> March 14 cross-model comparator",
                    "track_id": "A",
                    "model_comparator": str(row["model_name"]),
                    "benchmark_mean_fss": float(row["mean_fss"]),
                    "benchmark_iou": float(row["iou"]),
                    "benchmark_dice": float(row["dice"]),
                    "benchmark_rank": int(rank),
                    "primary_validation_mean_fss": float(primary_mean_fss),
                    "legacy_sparse_reference_mean_fss": float(legacy_sparse_mean_fss),
                    "legacy_support_reference_mean_fss": float(legacy_support_mean_fss),
                    "truth_source": "accepted March 14 NOAA/NESDIS observation mask",
                    "notes": str(row.get("structural_limitations", "") or ""),
                }
            )

        dwh_rows = self.dwh_cross_model_summary.copy()
        dwh_rows["mean_fss"] = dwh_rows.apply(mean_fss, axis=1)
        for track_id, group in dwh_rows.groupby("track_id", dropna=False):
            event_group = group[group["pair_role"].astype(str) == "event_corridor"]
            event_mean = float(event_group["mean_fss"].mean()) if not event_group.empty else float(group["mean_fss"].mean())
            overall_mean = float(group["mean_fss"].mean())
            event_row = event_group.iloc[0] if not event_group.empty else group.iloc[0]
            rows.append(
                {
                    "case_id": DWH_CASE_ID,
                    "benchmark_context": "DWH Phase 3C cross-model validation benchmark",
                    "track_id": "C1/C2/C3",
                    "model_comparator": DWH_TRACK_LABELS.get(str(track_id), str(track_id)),
                    "benchmark_mean_fss": event_mean,
                    "benchmark_iou": float(event_row["iou"]),
                    "benchmark_dice": float(event_row["dice"]),
                    "overall_mean_fss": overall_mean,
                    "truth_source": "DWH daily public observation-derived masks",
                    "notes": "Comparator only." if str(track_id) == "pygnome_deterministic" else "Scientific OpenDrift track.",
                }
            )

        table = pd.DataFrame(rows)
        table["rank_by_context"] = 0
        for case_id in (MINDORO_CASE_ID, DWH_CASE_ID):
            ordered_index = table[table["case_id"].astype(str) == case_id].sort_values(
                "benchmark_mean_fss", ascending=False
            ).index
            for rank, row_index in enumerate(ordered_index, start=1):
                table.at[row_index, "rank_by_context"] = rank
        table.sort_values(["case_id", "rank_by_context", "model_comparator"], inplace=True)
        return table

    def _build_observation_table(self) -> pd.DataFrame:
        rows: list[dict[str, Any]] = []

        for source_key, usage, truth_status in (
            (
                MINDORO_MARCH13_SOURCE_KEY,
                "primary_reinit_seed_polygon",
                "primary_seed_reference",
            ),
            (
                MINDORO_MARCH14_SOURCE_KEY,
                "primary_nextday_validation",
                "primary_truth",
            ),
        ):
            row = self._extended_obs_row(source_key)
            rows.append(
                {
                    "case_id": MINDORO_CASE_ID,
                    "track_id": "B1",
                    "observation_date": str(row["obs_date"]),
                    "source_name": str(row["source_name"]),
                    "provider": str(row["provider"]),
                    "source_type": str(row["source_type"]),
                    "truth_status": truth_status,
                    "observation_usage": usage,
                    "machine_readable": bool(row["machine_readable"]),
                    "observation_derived": bool(row["observation_derived"]),
                    "within_current_72h_horizon": bool(row["within_current_72h_horizon"]),
                    "source_url": str(row["source_url"]),
                    "service_url": str(row["service_url"]),
                    "notes": (
                        str(row.get("notes", "") or "")
                        + " | Primary package caveat: "
                        + MINDORO_SHARED_IMAGERY_CAVEAT
                    ).strip(" |"),
                }
            )

        mindoro_inventory = self.public_obs_inventory.copy()
        mindoro_inventory["obs_date"] = mindoro_inventory["obs_date"].astype(str)
        within_horizon = mindoro_inventory[
            mindoro_inventory["obs_date"].isin(["2023-03-03", "2023-03-04", "2023-03-05", "2023-03-06"])
        ]
        for _, row in within_horizon.iterrows():
            quantitative = bool(row.get("accept_for_appendix_quantitative", False))
            obs_date = str(row["obs_date"])
            if not quantitative:
                truth_status = "context_only"
                observation_usage = "qualitative_context"
            elif obs_date == "2023-03-06":
                truth_status = "legacy_sparse_reference"
                observation_usage = "legacy_sparse_reference"
            else:
                truth_status = "legacy_support_reference"
                observation_usage = "legacy_support_reference"
            rows.append(
                {
                    "case_id": MINDORO_CASE_ID,
                    "track_id": "B2" if obs_date == "2023-03-06" else "B3",
                    "observation_date": obs_date,
                    "source_name": str(row["source_name"]),
                    "provider": str(row["provider"]),
                    "source_type": str(row["source_type"]),
                    "truth_status": truth_status,
                    "observation_usage": observation_usage,
                    "machine_readable": bool(row["machine_readable"]),
                    "observation_derived": bool(row["observation_derived"]),
                    "within_current_72h_horizon": bool(row["within_current_72h_horizon"]),
                    "source_url": str(row["source_url"]),
                    "service_url": str(row["service_url"]),
                    "notes": str(row.get("notes", "") or row.get("rejection_reason", "") or ""),
                }
            )

        dwh_selected = self.dwh_source_taxonomy[self.dwh_source_taxonomy["selected_for_phase3c"].astype(bool)].copy()
        dwh_primary_service = self.dwh_service_inventory[
            self.dwh_service_inventory["service_role"].astype(str) == "public_observation_primary"
        ]["service_url"].iloc[0]
        for _, row in dwh_selected.iterrows():
            role = str(row["role"])
            rows.append(
                {
                    "case_id": DWH_CASE_ID,
                    "track_id": "C1/C2/C3",
                    "observation_date": str(row.get("event_date", "") or ""),
                    "source_name": str(row["layer_name"]),
                    "provider": "DWH FeatureServer",
                    "source_type": str(row["geometry_type"]),
                    "truth_status": (
                        "daily_truth"
                        if bool(row["use_as_truth"])
                        else "initialization_reference"
                        if role == "initialization_polygon"
                        else "context_only"
                    ),
                    "observation_usage": (
                        "daily_validation_mask"
                        if bool(row["use_as_truth"])
                        else "initialization_reference"
                        if role == "initialization_polygon"
                        else "provenance_only"
                    ),
                    "machine_readable": True,
                    "observation_derived": str(row["source_taxonomy"]) == "observation_derived_quantitative",
                    "within_current_72h_horizon": str(row.get("event_date", "") or "") in {"2010-05-20", "2010-05-21", "2010-05-22", "2010-05-23"},
                    "source_url": str(dwh_primary_service),
                    "service_url": str(dwh_primary_service),
                    "notes": str(row["truth_handling"]),
                }
            )
        table = pd.DataFrame(rows)
        table.sort_values(["case_id", "observation_date", "source_name"], inplace=True)
        return table

    def _build_limitations_table(self) -> pd.DataFrame:
        best_recipe = self.recipe_ranking[self.recipe_ranking["model_family"].astype(str) == "OpenDrift"].sort_values(
            "eventcorridor_mean_fss", ascending=False
        ).iloc[0]
        best_source_history = self.source_history_summary[
            self.source_history_summary["pair_role"].astype(str) == "march3_reconstruction_checkpoint"
        ].copy()
        best_source_history["mean_fss"] = best_source_history.apply(mean_fss, axis=1)
        best_source_history_row = best_source_history.sort_values("mean_fss", ascending=False).iloc[0]
        extended_row = self.extended_public_summary.iloc[0]

        rows = [
            {
                "limitation_id": "M1",
                "case_id": MINDORO_CASE_ID,
                "track_id": "B1",
                "category": "shared_imagery_caveat",
                "statement": str(self.mindoro_reinit_manifest["limitations"]["noaa_source_limitation_note"]),
                "implication": (
                    "Present March 13 -> March 14 as the canonical Mindoro validation track, but state clearly that "
                    "it is a reinitialization-based public-validation pair with shared March 12 imagery provenance rather than independent day-to-day validation."
                ),
                "source_artifact": str(MINDORO_REINIT_DIR / "march13_14_reinit_run_manifest.json"),
            },
            {
                "limitation_id": "M2",
                "case_id": MINDORO_CASE_ID,
                "track_id": "B2",
                "category": "legacy_sparse_reference",
                "statement": "Mindoro March 6 remains a legacy honesty-only sparse strict reference with only two observed ocean cells after processing.",
                "implication": "Retain March 6 in methods and limitations discussion, but do not present it as the primary Mindoro validation row.",
                "source_artifact": str(MINDORO_DIR / "phase3b" / "phase3b_summary.csv"),
            },
            {
                "limitation_id": "M3",
                "case_id": MINDORO_CASE_ID,
                "track_id": "B3",
                "category": "legacy_support_reference",
                "statement": "The broader March 3-6 public-support union remains informative legacy context, but it is not the same claim as the promoted March 13 -> March 14 reinit validation.",
                "implication": "Keep B3 visible in the package as reference material rather than silently overwriting or deleting it.",
                "source_artifact": str(MINDORO_DIR / "public_obs_appendix" / "appendix_eventcorridor_diagnostics.csv"),
            },
            {
                "limitation_id": "M4",
                "case_id": MINDORO_CASE_ID,
                "track_id": "A",
                "category": "comparator_role",
                "statement": "PyGNOME remains comparator-only in the promoted March 13 -> March 14 cross-model lane and does not reproduce the exact OpenDrift gridded current/wave/Stokes stack.",
                "implication": "Cross-model discussion must remain model-vs-observation honest and should not present PyGNOME as a forcing-identical twin.",
                "source_artifact": str(MINDORO_REINIT_CROSSMODEL_DIR / "march13_14_reinit_crossmodel_summary.csv"),
            },
            {
                "limitation_id": "M5",
                "case_id": MINDORO_CASE_ID,
                "track_id": "appendix_sensitivity",
                "category": "recipe_sensitivity",
                "statement": (
                    f"The best OpenDrift sensitivity branch ({best_recipe['track_id']}) reached eventcorridor mean FSS "
                    f"{float(best_recipe['eventcorridor_mean_fss']):.4f}, still below the fixed PyGNOME comparator at "
                    f"{float(self.recipe_manifest['recommendation']['pygnome_eventcorridor_mean_fss']):.4f}."
                ),
                "implication": "Recipe choice alone was not enough to overturn the Mindoro benchmark result.",
                "source_artifact": str(MINDORO_DIR / "recipe_sensitivity_r1_multibranch" / "recipe_sensitivity_r1_multibranch_ranking.csv"),
            },
            {
                "limitation_id": "M6",
                "case_id": MINDORO_CASE_ID,
                "track_id": "appendix_sensitivity",
                "category": "source_history_reconstruction",
                "statement": (
                    f"Source-history reconstruction improved the March 3 checkpoint most strongly for {best_source_history_row['pair_id']} "
                    f"(mean FSS {float(best_source_history_row['mean_fss']):.4f}) but did not materially improve strict March 6 or event-corridor skill."
                ),
                "implication": "Keep A2 source-history work in appendix/sensitivity framing rather than promoting it to the main case definition.",
                "source_artifact": str(MINDORO_DIR / "source_history_reconstruction_r1" / "source_history_reconstruction_r1_run_manifest.json"),
            },
            {
                "limitation_id": "M7",
                "case_id": MINDORO_CASE_ID,
                "track_id": "appendix_sensitivity",
                "category": "initialization_sensitivity",
                "statement": str(self.init_manifest["recommendation"]["reason"]),
                "implication": "Keep observation-initialized and reconstruction-initialized branches separate in the narrative.",
                "source_artifact": str(MINDORO_DIR / "init_mode_sensitivity_r1" / "init_mode_sensitivity_r1_run_manifest.json"),
            },
            {
                "limitation_id": "M8",
                "case_id": MINDORO_CASE_ID,
                "track_id": "appendix_sensitivity",
                "category": "extended_horizon",
                "statement": (
                    "Beyond-horizon public observations outside the promoted March 13 -> March 14 pair still require "
                    "dedicated rerun-specific support; the generic extended-horizon lane remains blocked under the "
                    f"current forcing window ({extended_row['reason']})."
                ),
                "implication": "Do not generalize the promoted March 13 -> March 14 packaging change to all later public dates.",
                "source_artifact": str(MINDORO_DIR / "phase3b_extended_public" / "phase3b_extended_summary.csv"),
            },
            {
                "limitation_id": "D1",
                "case_id": DWH_CASE_ID,
                "track_id": "C1/C2/C3",
                "category": "date_composite_logic",
                "statement": "DWH public layers support date-composite logic, not defensible exact sub-daily acquisition times.",
                "implication": "Phase 3C claims must remain date-composite honest rather than inventing exact observation times.",
                "source_artifact": str(DWH_DIR / "phase3c_external_case_setup" / "chapter3_phase3c_external_case_memo.md"),
            },
            {
                "limitation_id": "D2",
                "case_id": DWH_CASE_ID,
                "track_id": "C3",
                "category": "cross_model_harmonization",
                "statement": "The DWH PyGNOME comparator does not reproduce OpenDrift wave/Stokes handling with exact parity.",
                "implication": "Use PyGNOME as a comparator only, not as a forcing-identical twin.",
                "source_artifact": str(DWH_DIR / "phase3c_dwh_pygnome_comparator" / "phase3c_dwh_pygnome_run_manifest.json"),
            },
            {
                "limitation_id": "D3",
                "case_id": DWH_CASE_ID,
                "track_id": "C2",
                "category": "case_dependent_ensemble_benefit",
                "statement": "On DWH, ensemble p50 leads by overall mean FSS while deterministic remains strongest on the May 21-23 event corridor.",
                "implication": "Present ensemble benefit as case-dependent rather than universal.",
                "source_artifact": str(DWH_DIR / "phase3c_external_case_ensemble_comparison" / "phase3c_ensemble_summary.csv"),
            },
        ]
        return pd.DataFrame(rows)

    def _build_headlines(self, main_table: pd.DataFrame) -> dict[str, dict[str, Any]]:
        def _row_for(track_id: str, model_contains: str | None = None, validation_dates: str | None = None) -> pd.Series:
            subset = main_table[main_table["track_id"].astype(str) == track_id]
            if model_contains:
                subset = subset[subset["model_comparator"].astype(str).str.contains(model_contains, case=False, regex=False)]
            if validation_dates:
                subset = subset[subset["validation_dates"].astype(str) == validation_dates]
            if subset.empty:
                raise ValueError(f"Headline row not found for track {track_id}, model filter {model_contains}, validation {validation_dates}")
            return subset.iloc[0]

        return {
            "mindoro_primary_reinit": _row_for("B1").to_dict(),
            "mindoro_legacy_march6": _row_for("B2").to_dict(),
            "mindoro_legacy_broader_support": _row_for("B3").to_dict(),
            "dwh_deterministic_event": _row_for("C1", "OpenDrift deterministic", "2010-05-21_to_2010-05-23").to_dict(),
            "dwh_ensemble_p50_event": _row_for("C2", "ensemble p50", "2010-05-21_to_2010-05-23").to_dict(),
            "dwh_ensemble_p90_event": _row_for("C2", "ensemble p90", "2010-05-21_to_2010-05-23").to_dict(),
            "dwh_pygnome_event": _row_for("C3", "PyGNOME", "2010-05-21_to_2010-05-23").to_dict(),
            "mindoro_crossmodel_top": self._mindoro_crossmodel_top_row().to_dict(),
            "dwh_eventcorridor_top": self.dwh_cross_model_event.assign(mean_fss=self.dwh_cross_model_event.apply(mean_fss, axis=1))
            .sort_values("mean_fss", ascending=False)
            .iloc[0]
            .to_dict(),
            "dwh_overall_mean_top": self.dwh_cross_model_summary.assign(mean_fss=self.dwh_cross_model_summary.apply(mean_fss, axis=1))
            .groupby("track_id", dropna=False)["mean_fss"]
            .mean()
            .sort_values(ascending=False)
            .head(1)
            .rename("overall_mean_fss")
            .reset_index()
            .assign(model_comparator=lambda df: df["track_id"].map(DWH_TRACK_LABELS))
            .iloc[0]
            .to_dict(),
        }

    def _write_claims_guardrails(self) -> Path:
        path = self.output_dir / "final_validation_claims_guardrails.md"
        lines = [
            "# Final Validation Claims Guardrails",
            "",
            f"- {MINDORO_PRIMARY_VALIDATION_THESIS_PHASE_TITLE} is represented by Mindoro B1, the March 13 -> March 14 NOAA reinit validation, and it should be described with the explicit March 12 WorldView-3 caveat.",
            "- The later 2016-2023 Mindoro-focused drifter rerun confirmed the same cmems_era5 recipe used by the stored B1 run, but it does not replace the original B1 raw provenance.",
            "- Mindoro B2 and B3 remain legacy/reference rows, with B2 framed as honesty-only, and they should not be silently rewritten as if they never existed.",
            "- PyGNOME is a comparator, not truth, in both the promoted Mindoro cross-model lane and the DWH cross-model comparison.",
            "- DWH observed masks are truth for Phase 3C.",
            "- DWH currently demonstrates workflow transferability and meaningful spatial skill under real historical forcing.",
            "- On DWH, OpenDrift outperforms PyGNOME under the current case definition.",
            "- On DWH, ensemble p50 improves overall mean FSS while deterministic remains strongest on the May 21-23 event corridor.",
            "- DWH Phase 3C is scientifically reportable even if some optional future extensions remain.",
            "- Do not relabel legacy/reference or sensitivity products as if they were the new promoted primary validation row.",
        ]
        _write_text(path, "\n".join(lines))
        return path

    def _write_chapter_sync_memo(self) -> Path:
        path = self.output_dir / "final_validation_chapter_sync_memo.md"
        lines = [
            "# Final Validation Chapter 3 Sync Memo",
            "",
            f"Thesis-facing title: {MINDORO_PRIMARY_VALIDATION_THESIS_PHASE_TITLE}",
            "",
            "Recommended revised structure:",
            "",
            "1. Phase 1 = Transport Validation and Baseline Configuration Selection",
            "2. Phase 2 = Standardized Machine-Readable Forecast Product Generation",
            "3. Phase 3A = Mindoro March 13 -> March 14 Cross-Model Comparator",
            "4. Phase 3B1 = Mindoro March 13 -> March 14 NOAA Reinit Primary Validation",
            "5. Phase 3B2 = Mindoro Legacy March 6 Sparse Strict Reference",
            "6. Phase 3B3 = Mindoro Legacy March 3-6 Broader-Support Reference",
            "7. Phase 3C = External Rich-Data Spill Transfer Validation (Deepwater Horizon 2010)",
            "8. Phase 4 = Oil-Type Fate and Shoreline Impact Analysis",
            "9. Phase 5 = Reproducibility, Packaging, and Deliverables",
            "",
            "Packaging guidance:",
            "",
            "- Keep Mindoro as the main Philippine case.",
            "- Keep DWH as the rich-data external transfer-validation branch.",
            "- Present Phase 3A as comparator-only benchmarking, not as a truth-source replacement.",
            "- Preserve `config/case_mindoro_retro_2023.yaml` as the frozen March 3 -> March 6 case definition and carry the Phase 3B promotion through the amendment file instead.",
            "- Present March 13 -> March 14 as the canonical Mindoro validation with the shared-imagery caveat stated explicitly.",
            "- State that the later 2016-2023 Mindoro-focused drifter rerun confirmed the same cmems_era5 recipe used by the stored B1 run without rewriting the original run provenance.",
            "- Keep March 6 and March 3-6 visible as legacy/reference material rather than deleting or hiding them.",
        ]
        _write_text(path, "\n".join(lines))
        return path

    def _write_interpretation_memo(self) -> Path:
        path = self.output_dir / "final_validation_interpretation_memo.md"
        lines = [
            "# Final Validation Interpretation Memo",
            "",
            "Key scientific takeaway:",
            "",
            f"- {MINDORO_PRIMARY_VALIDATION_THESIS_PHASE_TITLE} is now carried by the Mindoro March 13 -> March 14 NOAA reinit track.",
            "- The later 2016-2023 Mindoro-focused drifter rerun confirmed the same cmems_era5 recipe used by the stored B1 run.",
            "- DWH is the rich-data external transfer-validation success.",
            "- Ensemble benefit is case-dependent, not universal.",
            "",
            "Interpretation notes:",
            "",
            "- The promoted Mindoro row is a March 13 -> March 14 reinitialization test and must carry the caveat that both NOAA products cite March 12 WorldView-3 imagery.",
            "- The legacy March 6 row should still be interpreted as an honesty-only difficult sparse-data edge case rather than erased from the methods story.",
            "- The legacy March 3-6 broader-support row remains helpful context, but it is not the same claim as the promoted B1 reinit validation.",
            "- The promoted Mindoro comparator lane shows OpenDrift R1 previous reinit p50 leading the March 13 -> March 14 cross-model comparison under the current case definition.",
            "- The DWH external case shows that the workflow transfers to a richer observation setting with meaningful spatial skill.",
            "- On DWH, ensemble p50 improves overall mean FSS, while deterministic retains the strongest event-corridor result.",
        ]
        _write_text(path, "\n".join(lines))
        return path

    def _write_summary(self, headlines: dict[str, dict[str, Any]], recommendation: str) -> Path:
        path = self.output_dir / "final_validation_summary.md"
        lines = [
            "# Final Validation Summary",
            "",
            "This package is read-only with respect to completed scientific outputs. No Mindoro or DWH scientific result files were overwritten.",
            "",
            f"Thesis-facing Phase 3B title: {MINDORO_PRIMARY_VALIDATION_THESIS_PHASE_TITLE}",
            "",
            "## Headline Results",
            "",
            (
                f"- Mindoro March 13 -> March 14 primary validation (B1): FSS(1/3/5/10 km) = "
                f"{headlines['mindoro_primary_reinit']['fss_1km']:.4f}, {headlines['mindoro_primary_reinit']['fss_3km']:.4f}, "
                f"{headlines['mindoro_primary_reinit']['fss_5km']:.4f}, {headlines['mindoro_primary_reinit']['fss_10km']:.4f}; "
                f"IoU={headlines['mindoro_primary_reinit']['iou']:.4f}; Dice={headlines['mindoro_primary_reinit']['dice']:.4f}."
            ),
            "- The later 2016-2023 Mindoro-focused drifter rerun independently confirmed the same cmems_era5 recipe used by the stored B1 run, so the promoted B1 story is now both artifact-preserving and drifter-confirmed.",
            (
                f"- Mindoro promoted cross-model top track (A): {headlines['mindoro_crossmodel_top']['model_name']} "
                f"with FSS(1/3/5/10 km) = {headlines['mindoro_crossmodel_top']['fss_1km']:.4f}, "
                f"{headlines['mindoro_crossmodel_top']['fss_3km']:.4f}, {headlines['mindoro_crossmodel_top']['fss_5km']:.4f}, "
                f"{headlines['mindoro_crossmodel_top']['fss_10km']:.4f}."
            ),
            (
                f"- Mindoro legacy March 6 honesty-only sparse reference (B2): FSS(1/3/5/10 km) = "
                f"{headlines['mindoro_legacy_march6']['fss_1km']:.4f}, {headlines['mindoro_legacy_march6']['fss_3km']:.4f}, "
                f"{headlines['mindoro_legacy_march6']['fss_5km']:.4f}, {headlines['mindoro_legacy_march6']['fss_10km']:.4f}; "
                f"IoU={headlines['mindoro_legacy_march6']['iou']:.4f}; Dice={headlines['mindoro_legacy_march6']['dice']:.4f}."
            ),
            (
                f"- Mindoro legacy March 3-6 broader-support reference (B3): FSS(1/3/5/10 km) = "
                f"{headlines['mindoro_legacy_broader_support']['fss_1km']:.4f}, {headlines['mindoro_legacy_broader_support']['fss_3km']:.4f}, "
                f"{headlines['mindoro_legacy_broader_support']['fss_5km']:.4f}, {headlines['mindoro_legacy_broader_support']['fss_10km']:.4f}; "
                f"IoU={headlines['mindoro_legacy_broader_support']['iou']:.4f}; Dice={headlines['mindoro_legacy_broader_support']['dice']:.4f}."
            ),
            (
                f"- DWH deterministic event corridor (C1): FSS(1/3/5/10 km) = "
                f"{headlines['dwh_deterministic_event']['fss_1km']:.4f}, {headlines['dwh_deterministic_event']['fss_3km']:.4f}, "
                f"{headlines['dwh_deterministic_event']['fss_5km']:.4f}, {headlines['dwh_deterministic_event']['fss_10km']:.4f}; "
                f"IoU={headlines['dwh_deterministic_event']['iou']:.4f}; Dice={headlines['dwh_deterministic_event']['dice']:.4f}."
            ),
            (
                f"- DWH ensemble p50 event corridor (C2): FSS(1/3/5/10 km) = "
                f"{headlines['dwh_ensemble_p50_event']['fss_1km']:.4f}, {headlines['dwh_ensemble_p50_event']['fss_3km']:.4f}, "
                f"{headlines['dwh_ensemble_p50_event']['fss_5km']:.4f}, {headlines['dwh_ensemble_p50_event']['fss_10km']:.4f}; "
                f"overall mean leader = {headlines['dwh_overall_mean_top']['model_comparator']} "
                f"({headlines['dwh_overall_mean_top']['overall_mean_fss']:.4f})."
            ),
            (
                f"- DWH PyGNOME comparator (C3) event corridor: FSS(1/3/5/10 km) = "
                f"{headlines['dwh_pygnome_event']['fss_1km']:.4f}, {headlines['dwh_pygnome_event']['fss_3km']:.4f}, "
                f"{headlines['dwh_pygnome_event']['fss_5km']:.4f}, {headlines['dwh_pygnome_event']['fss_10km']:.4f}; "
                f"IoU={headlines['dwh_pygnome_event']['iou']:.4f}; Dice={headlines['dwh_pygnome_event']['dice']:.4f}."
            ),
            "",
            "## Recommended Final Structure",
            "",
            f"- Base-case provenance: keep `{MINDORO_BASE_CASE_CONFIG_PATH}` frozen for March 3 -> March 6 and carry the B1 promotion through `{MINDORO_PRIMARY_VALIDATION_AMENDMENT_PATH}`.",
            f"- Main text: {MINDORO_PRIMARY_VALIDATION_THESIS_PHASE_TITLE} should foreground Mindoro B1 as the March 13 -> March 14 primary validation with the shared-imagery caveat and the later drifter-confirmation note, plus DWH Phase 3C as the rich-data transfer-validation success.",
            "- Comparative discussion: Mindoro A cross-model comparator and DWH deterministic-vs-ensemble-vs-PyGNOME comparison.",
            "- Legacy/reference and sensitivities: Mindoro B2/B3 legacy rows, recipe/init/source-history sensitivities, and optional future DWH extensions.",
            "",
            "## Final Recommendation",
            "",
            f"- {recommendation}",
        ]
        _write_text(path, "\n".join(lines))
        return path

    def run(self) -> dict[str, Any]:
        self._assert_required_artifacts()
        self._load_inputs()
        self.output_dir.mkdir(parents=True, exist_ok=True)

        main_table = self._build_main_table()
        case_registry = self._build_case_registry()
        benchmark_table = self._build_benchmark_table()
        observation_table = self._build_observation_table()
        limitations_table = self._build_limitations_table()
        headlines = self._build_headlines(main_table)
        recommendation = decide_final_structure()
        confirmation = self._mindoro_dual_provenance_confirmation()

        main_table_path = self.output_dir / "final_validation_main_table.csv"
        case_registry_path = self.output_dir / "final_validation_case_registry.csv"
        benchmark_table_path = self.output_dir / "final_validation_benchmark_table.csv"
        observation_table_path = self.output_dir / "final_validation_observation_table.csv"
        limitations_table_path = self.output_dir / "final_validation_limitations.csv"

        main_table.to_csv(main_table_path, index=False)
        case_registry.to_csv(case_registry_path, index=False)
        benchmark_table.to_csv(benchmark_table_path, index=False)
        observation_table.to_csv(observation_table_path, index=False)
        limitations_table.to_csv(limitations_table_path, index=False)

        guardrails_path = self._write_claims_guardrails()
        chapter_sync_path = self._write_chapter_sync_memo()
        interpretation_path = self._write_interpretation_memo()
        summary_path = self._write_summary(headlines, recommendation)
        final_output_export = self._build_mindoro_final_output_export(confirmation)

        manifest = {
            "phase": PHASE,
            "output_dir": str(self.output_dir),
            "inputs_preserved": [str(path) for path in self.required_paths],
            "artifacts": {
                "final_validation_main_table": str(main_table_path),
                "final_validation_case_registry": str(case_registry_path),
                "final_validation_benchmark_table": str(benchmark_table_path),
                "final_validation_observation_table": str(observation_table_path),
                "final_validation_limitations": str(limitations_table_path),
                "final_validation_claims_guardrails": str(guardrails_path),
                "final_validation_chapter_sync_memo": str(chapter_sync_path),
                "final_validation_interpretation_memo": str(interpretation_path),
                "final_validation_summary": str(summary_path),
                "phase3b_march13_14_final_output_readme": final_output_export["readme_path"],
                "phase3b_march13_14_final_output_manifest": final_output_export["manifest_path"],
            },
            "headlines": headlines,
            "final_recommendation": recommendation,
            "mindoro_primary_validation_promotion": {
                "base_case_definition_path": str(MINDORO_BASE_CASE_CONFIG_PATH),
                "case_freeze_amendment_path": str(MINDORO_PRIMARY_VALIDATION_AMENDMENT_PATH),
                "migration_note_path": str(MINDORO_PRIMARY_VALIDATION_MIGRATION_NOTE_PATH),
                "primary_track_id": MINDORO_PRIMARY_VALIDATION_TRACK_ID,
                "primary_track_label": MINDORO_PRIMARY_VALIDATION_TRACK_LABEL,
                "thesis_phase_title": MINDORO_PRIMARY_VALIDATION_THESIS_PHASE_TITLE,
                "thesis_phase_subtitle": MINDORO_PRIMARY_VALIDATION_THESIS_SUBTITLE,
                "legacy_honesty_track_id": MINDORO_LEGACY_MARCH6_TRACK_ID,
                "legacy_support_track_id": MINDORO_LEGACY_SUPPORT_TRACK_ID,
                "shared_imagery_caveat": MINDORO_SHARED_IMAGERY_CAVEAT,
                "dual_provenance_confirmation": confirmation,
                "final_output_export_dir": final_output_export["output_dir"],
            },
            "recommended_final_chapter_structure": [
                "Phase 1 = Transport Validation and Baseline Configuration Selection",
                "Phase 2 = Standardized Machine-Readable Forecast Product Generation",
                "Phase 3A = Mindoro March 13 -> March 14 Cross-Model Comparator",
                "Phase 3B1 = Mindoro March 13 -> March 14 NOAA Reinit Primary Validation",
                "Phase 3B2 = Mindoro Legacy March 6 Sparse Strict Reference",
                "Phase 3B3 = Mindoro Legacy March 3-6 Broader-Support Reference",
                "Phase 3C = External Rich-Data Spill Transfer Validation (Deepwater Horizon 2010)",
                "Phase 4 = Oil-Type Fate and Shoreline Impact Analysis",
                "Phase 5 = Reproducibility, Packaging, and Deliverables",
            ],
            "phase3b_march13_14_final_output": final_output_export,
        }
        _write_json(self.output_dir / "final_validation_manifest.json", manifest)
        return manifest


def run_final_validation_package() -> dict[str, Any]:
    return FinalValidationPackageService().run()


__all__ = [
    "PHASE",
    "OUTPUT_DIR",
    "FinalValidationPackageService",
    "decide_final_structure",
    "mean_fss",
    "run_final_validation_package",
]
