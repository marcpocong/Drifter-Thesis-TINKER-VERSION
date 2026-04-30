"""Shared status registry for figure labels, provenance, and dashboard wording."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping


MINDORO_CASE_ID = "CASE_MINDORO_RETRO_2023"
DWH_CASE_ID = "CASE_DWH_RETRO_2010_72H"


@dataclass(frozen=True)
class ArtifactStatus:
    key: str
    label: str
    panel_label: str
    role: str
    reportability: str
    official_status: str
    frozen_status: str
    provenance_label: str
    panel_text: str
    dashboard_summary: str


@dataclass(frozen=True)
class ArtifactSurfacePolicy:
    key: str
    label: str
    description: str
    home_visible: bool
    panel_visible: bool
    archive_visible: bool
    advanced_visible: bool
    recommended_visible: bool


SURFACE_REGISTRY: dict[str, ArtifactSurfacePolicy] = {
    "thesis_main": ArtifactSurfacePolicy(
        key="thesis_main",
        label="Thesis-facing main surface",
        description="Eligible for thesis-facing home and main presentation surfaces.",
        home_visible=True,
        panel_visible=True,
        archive_visible=False,
        advanced_visible=True,
        recommended_visible=True,
    ),
    "comparator_support": ArtifactSurfacePolicy(
        key="comparator_support",
        label="Support / comparator surface",
        description="Support, comparator, or context material kept separate from the primary thesis claim.",
        home_visible=True,
        panel_visible=True,
        archive_visible=False,
        advanced_visible=True,
        recommended_visible=True,
    ),
    "archive_only": ArtifactSurfacePolicy(
        key="archive_only",
        label="Archive-only surface",
        description="Preserved for archive, provenance, reproducibility, or audit only.",
        home_visible=False,
        panel_visible=False,
        archive_visible=True,
        advanced_visible=True,
        recommended_visible=False,
    ),
    "legacy_support": ArtifactSurfacePolicy(
        key="legacy_support",
        label="Legacy / prototype support surface",
        description="Legacy or prototype support material kept visible as support only.",
        home_visible=True,
        panel_visible=True,
        archive_visible=False,
        advanced_visible=True,
        recommended_visible=True,
    ),
    "advanced_only": ArtifactSurfacePolicy(
        key="advanced_only",
        label="Advanced-only surface",
        description="Visible only through advanced or lower-level inspection surfaces.",
        home_visible=False,
        panel_visible=False,
        archive_visible=False,
        advanced_visible=True,
        recommended_visible=False,
    ),
}


STATUS_SURFACE_KEY_MAP: dict[str, str] = {
    "mindoro_primary_validation": "thesis_main",
    "mindoro_crossmodel_comparator": "comparator_support",
    "mindoro_b1_r0_archive": "archive_only",
    "mindoro_legacy_march6": "archive_only",
    "mindoro_legacy_support": "archive_only",
    "mindoro_trajectory_context": "advanced_only",
    "thesis_study_box_reference": "thesis_main",
    "mindoro_phase4_oil_budget": "comparator_support",
    "mindoro_phase4_shoreline": "comparator_support",
    "mindoro_phase4_deferred": "comparator_support",
    "dwh_deterministic_transfer": "thesis_main",
    "dwh_observation_truth_context": "thesis_main",
    "dwh_ensemble_transfer": "thesis_main",
    "dwh_crossmodel_comparator": "comparator_support",
    "dwh_cross_track_summary": "thesis_main",
    "dwh_trajectory_context": "advanced_only",
    "prototype_2021_support": "legacy_support",
    "prototype_2016_support": "legacy_support",
}


STATUS_REGISTRY: dict[str, ArtifactStatus] = {
    "mindoro_primary_validation": ArtifactStatus(
        key="mindoro_primary_validation",
        label="Mindoro March 13 -> March 14 R1 primary validation row",
        panel_label="Mindoro March 13 -> March 14 R1 primary validation",
        role="primary_validation",
        reportability="reportable_now_inherited_provisional",
        official_status="promoted_primary_validation",
        frozen_status="not_frozen",
        provenance_label=(
            "March 13 public seed observation and March 14 public target observation, active "
            "Mindoro-specific recipe provenance from the separate focused 2016-2023 drifter rerun, "
            "and the preserved raw scientific note that the stored promoted B1 run remains tied to "
            "the existing R1_previous reinit lineage."
        ),
        panel_text=(
            "Main Mindoro B1 observation-based spatial validation view. This page will use the "
            "March 13 -> March 14 R1 primary validation row only. This validation layer does not directly ingest "
            "drifters; it inherits the official cmems_gfs B1 recipe from the separate focused 2016-2023 "
            "Mindoro drifter rerun after that four-recipe rerun found cmems_gfs as the historical winner "
            "and promoted it directly into official B1, while the broader 2016-2022 regional rerun stays "
            "reference-only. The stored promoted B1 run remains tied to the existing R1_previous reinit "
            "lineage. Treat March 13 and March 14 as independent NOAA-published day-specific observation products."
        ),
        dashboard_summary=(
            "March 13 -> March 14 R1 primary validation row only; the focused 2016-2023 Mindoro drifter "
            "rerun found cmems_gfs as the historical winner, official B1 now uses cmems_gfs, and the stored "
            "promoted run remains tied to the existing R1_previous reinit lineage."
        ),
    ),
    "mindoro_crossmodel_comparator": ArtifactStatus(
        key="mindoro_crossmodel_comparator",
        label="Mindoro March 13 -> March 14 Track A comparator support",
        panel_label="Mindoro March 13 -> March 14 Track A comparator support",
        role="comparator_only",
        reportability="reportable_comparator_inherited_provisional",
        official_status="comparator_only_not_truth",
        frozen_status="not_frozen",
        provenance_label=(
            "March 13 is the independent public seed observation and March 14 is the independent public "
            "target observation for the same-case Track A comparator support; PyGNOME remains comparator-only "
            "and separate from truth."
        ),
        panel_text=(
            "Same-case Track A comparator support attached to the March 13 -> March 14 R1 primary validation "
            "row, using March 13 as the independent public seed observation and March 14 as the independent "
            "public target observation. PyGNOME is a comparator, not truth, and Track A is not a co-primary "
            "validation row. Archived March 13 -> March 14 R0 comparator outputs are not part of this "
            "thesis-facing view."
        ),
        dashboard_summary=(
            "Same-case Track A comparator-only support attached to the March 13 -> March 14 R1 primary "
            "validation row with independent March 13 seed and March 14 target observations."
        ),
    ),
    "mindoro_b1_r0_archive": ArtifactStatus(
        key="mindoro_b1_r0_archive",
        label="Mindoro March 13 -> March 14 R0 archived baseline",
        panel_label="Mindoro March 13 -> March 14 R0 archive only",
        role="archive_reference",
        reportability="archive_only_provenance_reference",
        official_status="archive_only_not_thesis_facing",
        frozen_status="repo_preserved_historical_baseline",
        provenance_label=(
            "Preserved historical March 13 -> March 14 R0 baseline and archived R0-including March13-14 "
            "outputs retained in the repository for audit, reproducibility, and provenance only."
        ),
        panel_text=(
            "Archive only. This status marks the preserved March 13 -> March 14 R0 archived baseline and any "
            "older March13-14 outputs that included or foregrounded R0. They remain repo-preserved for "
            "provenance, audit, and reproducibility, but they are not thesis-facing and are not part of the "
            "main Mindoro validation page or main-paper reporting."
        ),
        dashboard_summary=(
            "March 13 -> March 14 R0 archived baseline and archived R0-including March13-14 outputs; "
            "preserved for provenance only and not thesis-facing."
        ),
    ),
    "mindoro_legacy_march6": ArtifactStatus(
        key="mindoro_legacy_march6",
        label="Mindoro March 6 B2 archived sparse strict reference",
        panel_label="Mindoro March 6 B2 archive only",
        role="legacy_reference",
        reportability="archive_only_provenance_reference",
        official_status="archive_only_not_thesis_facing",
        frozen_status="not_frozen",
        provenance_label="Sparse March 6 reference retained as archive-only provenance material.",
        panel_text=(
            "Archive only. B2 is preserved for provenance, audit, reproducibility, and methods traceability, "
            "but it is not thesis-facing and is not part of the main Mindoro validation page or main paper."
        ),
        dashboard_summary="B2 archived March 6 sparse reference; preserved for provenance only and not thesis-facing.",
    ),
    "mindoro_legacy_support": ArtifactStatus(
        key="mindoro_legacy_support",
        label="Mindoro March 3-6 B3 archived broader-support reference",
        panel_label="Mindoro March 3-6 B3 archive only",
        role="legacy_support_reference",
        reportability="archive_only_provenance_reference",
        official_status="archive_only_not_thesis_facing",
        frozen_status="not_frozen",
        provenance_label="Broader-support March-family event context preserved as archive-only provenance material.",
        panel_text=(
            "Archive only. B3 is preserved for provenance, audit, reproducibility, and historical reference, "
            "but it is not thesis-facing and is not part of the main Mindoro validation page or main paper."
        ),
        dashboard_summary="B3 archived March 3-6 broader-support reference; preserved for provenance only.",
    ),
    "mindoro_trajectory_context": ArtifactStatus(
        key="mindoro_trajectory_context",
        label="Mindoro transport trajectory context",
        panel_label="Mindoro transport trajectory context",
        role="trajectory_context",
        reportability="scientifically_usable_not_frozen",
        official_status="official_transport_context_not_frozen",
        frozen_status="not_frozen",
        provenance_label="Phase 2 / Phase 3 transport context built from stored trajectory outputs.",
        panel_text=(
            "Read-only transport context preserved from stored trajectory outputs. This layer is not thesis-facing "
            "main-page evidence and is better read as provenance or audit support."
        ),
        dashboard_summary="Transport-context archive/support layer from a scientifically usable but not frozen branch.",
    ),
    "thesis_study_box_reference": ArtifactStatus(
        key="thesis_study_box_reference",
        label="Thesis study-box reference",
        panel_label="Thesis study-box reference",
        role="study_context_reference",
        reportability="read_only_context_reference",
        official_status="shared_thesis_box_reference",
        frozen_status="mixed_stored_metadata_reference",
        provenance_label=(
            "Shared box-reference figure set built from stored thesis-facing Mindoro configuration, the stored "
            "scoring-grid display bounds, and the curated prototype_2016 first-code search-box provenance metadata."
        ),
        panel_text=(
            "Shared study-area reference only. Use these figures to distinguish Study Box 1, the focused Mindoro "
            "Phase 1 validation box; Study Box 2, the broader Mindoro case domain; Study Box 3, the scoring-grid "
            "display bounds; and Study Box 4, the prototype_2016 first-code search box without implying that they are "
            "one operative scientific domain. Only Study Boxes 2 and 4 are thesis-facing."
        ),
        dashboard_summary=(
            "Panel-ready thesis study-box reference figures built from stored config, manifest, and provenance "
            "metadata only."
        ),
    ),
    "mindoro_phase4_oil_budget": ArtifactStatus(
        key="mindoro_phase4_oil_budget",
        label="Mindoro Phase 4 oil-budget interpretation",
        panel_label="Mindoro Phase 4 oil-budget interpretation",
        role="phase4_openoil_only",
        reportability="reportable_now_inherited_provisional",
        official_status="opendrift_openoil_only_not_cross_model",
        frozen_status="not_frozen",
        provenance_label="OpenDrift/OpenOil-only Phase 4 output; inherited-provisional from upstream transport.",
        panel_text="These are current OpenDrift/OpenOil Phase 4 interpretation outputs, not cross-model figures.",
        dashboard_summary="Phase 4 OpenDrift/OpenOil-only interpretation; inherited-provisional.",
    ),
    "mindoro_phase4_shoreline": ArtifactStatus(
        key="mindoro_phase4_shoreline",
        label="Mindoro Phase 4 shoreline interpretation",
        panel_label="Mindoro Phase 4 shoreline interpretation",
        role="phase4_openoil_only",
        reportability="reportable_now_inherited_provisional",
        official_status="opendrift_openoil_only_not_cross_model",
        frozen_status="not_frozen",
        provenance_label="OpenDrift/OpenOil-only shoreline outputs on the current reportable replay.",
        panel_text="These shoreline figures are part of the current OpenDrift/OpenOil interpretation layer and should not be framed as cross-model results.",
        dashboard_summary="Phase 4 shoreline interpretation; inherited-provisional.",
    ),
    "mindoro_phase4_deferred": ArtifactStatus(
        key="mindoro_phase4_deferred",
        label="Mindoro Phase 4 no-matched-PyGNOME note",
        panel_label="Mindoro Phase 4: no matched PyGNOME package yet",
        role="deferred_honesty_note",
        reportability="no_matched_phase4_pygnome_package_yet",
        official_status="opendrift_openoil_only_no_matched_pygnome_phase4_package",
        frozen_status="not_applicable",
        provenance_label=(
            "No matched Mindoro Phase 4 PyGNOME package is stored yet; the current PyGNOME branch is "
            "transport-only, keeps weathering disabled, and does not export matched shoreline tables."
        ),
        panel_text=(
            "No matched Phase 4 PyGNOME comparison is packaged yet. Current Phase 4 results are "
            "OpenDrift/OpenOil scenario outputs only."
        ),
        dashboard_summary="No matched Mindoro Phase 4 PyGNOME package is packaged yet.",
    ),
    "dwh_deterministic_transfer": ArtifactStatus(
        key="dwh_deterministic_transfer",
        label="DWH deterministic external transfer validation",
        panel_label="DWH deterministic external transfer validation",
        role="transfer_validation",
        reportability="reportable_now_frozen",
        official_status="reportable_external_transfer_validation",
        frozen_status="frozen",
        provenance_label=(
            "DWH public masks remain truth under the frozen readiness-gated HYCOM GOFS 3.1 + ERA5 + "
            "CMEMS wave/Stokes stack."
        ),
        panel_text=(
            "Clean baseline DWH transfer-validation view; it supports the external-case success "
            "story but does not replace the Mindoro main case."
        ),
        dashboard_summary="Frozen DWH deterministic transfer-validation baseline.",
    ),
    "dwh_observation_truth_context": ArtifactStatus(
        key="dwh_observation_truth_context",
        label="DWH observation-derived truth context",
        panel_label="DWH observation-derived truth context",
        role="observation_truth_context",
        reportability="reportable_now_frozen",
        official_status="truth_context_for_transfer_validation",
        frozen_status="frozen",
        provenance_label=(
            "DWH public masks remain truth and are used as date-composite daily or event-corridor context without "
            "claiming exact sub-daily acquisition times."
        ),
        panel_text=(
            "Establishes the DWH observation-derived truth masks before any deterministic, ensemble, or "
            "PyGNOME comparison is shown."
        ),
        dashboard_summary="Frozen observation-derived truth context for the DWH external transfer-validation lane.",
    ),
    "dwh_ensemble_transfer": ArtifactStatus(
        key="dwh_ensemble_transfer",
        label="DWH ensemble extension and deterministic-vs-ensemble comparison",
        panel_label="DWH ensemble extension and deterministic-vs-ensemble comparison",
        role="transfer_validation_comparison",
        reportability="reportable_now_frozen",
        official_status="reportable_ensemble_comparison",
        frozen_status="frozen",
        provenance_label=(
            "Same frozen DWH truth masks and forcing stack as the deterministic track; p50 is the preferred "
            "probabilistic extension and p90 remains support/comparison only."
        ),
        panel_text=(
            "Explains deterministic, p50, and p90 differences without overstating ensemble benefit as "
            "universal; p50 is the preferred probabilistic extension and p90 is support/comparison only."
        ),
        dashboard_summary="Frozen DWH ensemble extension on the same truth masks; p50 preferred, p90 support-only.",
    ),
    "dwh_crossmodel_comparator": ArtifactStatus(
        key="dwh_crossmodel_comparator",
        label="DWH PyGNOME comparator-only",
        panel_label="DWH PyGNOME comparator-only",
        role="comparator_only",
        reportability="reportable_comparator_frozen",
        official_status="comparator_only_not_truth",
        frozen_status="frozen",
        provenance_label="DWH observed masks remain truth; PyGNOME is a frozen comparator-only track and never truth.",
        panel_text="Cross-model comparison only; PyGNOME is not truth and does not replace the OpenDrift DWH story.",
        dashboard_summary="Frozen DWH cross-model comparator; PyGNOME not truth.",
    ),
    "dwh_cross_track_summary": ArtifactStatus(
        key="dwh_cross_track_summary",
        label="DWH cross-track summary and interpretation",
        panel_label="DWH cross-track summary",
        role="transfer_validation_summary",
        reportability="reportable_now_frozen",
        official_status="cross_track_summary_from_frozen_dwh_package",
        frozen_status="frozen",
        provenance_label=(
            "Read-only summary and interpretation artifacts built from the frozen DWH deterministic, ensemble, "
            "and comparator tracks."
        ),
        panel_text=(
            "Cross-track summary tables and notes that synthesize the frozen DWH deterministic, ensemble, and "
            "PyGNOME comparator lanes without rerunning science."
        ),
        dashboard_summary="Frozen DWH cross-track summary and interpretation layer.",
    ),
    "dwh_trajectory_context": ArtifactStatus(
        key="dwh_trajectory_context",
        label="DWH trajectory context",
        panel_label="DWH trajectory context",
        role="trajectory_context",
        reportability="appendix_support_context",
        official_status="trajectory_context_only",
        frozen_status="not_frozen",
        provenance_label="Stored deterministic, ensemble, and comparator tracks shown as path context.",
        panel_text="Path-intuition context before or after the score-based boards.",
        dashboard_summary="Trajectory appendix-support context.",
    ),
    "prototype_2021_support": ArtifactStatus(
        key="prototype_2021_support",
        label="Prototype 2021 accepted-segment debug support",
        panel_label="Prototype 2021 accepted-segment debug support",
        role="support_only_debug",
        reportability="support_only_not_final_evidence",
        official_status="support_only_not_official",
        frozen_status="not_final_phase1_study",
        provenance_label="Preferred accepted-segment debug lane built from stored deterministic transport benchmarks.",
        panel_text="Preferred debug support lane only; comparator-only and not final Phase 1 evidence.",
        dashboard_summary="Preferred debug support lane; not final Phase 1 evidence.",
    ),
    "prototype_2016_support": ArtifactStatus(
        key="prototype_2016_support",
        label="Prototype 2016 legacy debug support",
        panel_label="Prototype 2016 legacy debug support",
        role="support_only_debug",
        reportability="support_only_not_final_evidence",
        official_status="legacy_support_only_not_official",
        frozen_status="not_final_phase1_study",
        provenance_label=(
            "Legacy/debug regression lane preserved as a support-only Phase 1/2/3A/4/5 package for reproducibility "
            "and thesis context only. Its historical-origin story records that the very first prototype code used "
            "the shared first-code search box [108.6465, 121.3655, 6.1865, 20.3515], surfaced the first three 2016 "
            "drifter cases on the west coast of the Philippines, and the team then kept those three as the first "
            "proof-of-pipeline focus."
        ),
        panel_text=(
            "Legacy debug support only; visible support flow is Phase 1/2/3A/4/5, with Phase 3A comparator-only, a "
            "budget-only Phase 4 PyGNOME pilot when packaged, and a dedicated curated final package under "
            "output/2016 Legacy Runs FINAL Figures. Historical-origin note: the very first prototype code used the "
            "shared first-code search box [108.6465, 121.3655, 6.1865, 20.3515], surfaced the first three 2016 "
            "drifter cases, and the stored per-case local prototype extents remain the operative scientific/display "
            "extents. Not final Phase 1 evidence."
        ),
        dashboard_summary=(
            "Legacy debug support lane with a dedicated curated Phase 5 package, a first-code search-box historical "
            "origin note for the first three 2016 drifters, and an optional budget-only Phase 4 PyGNOME pilot; not "
            "the preferred prototype lane or final Phase 1 evidence."
        ),
    ),
}


TRACK_ID_TO_STATUS_KEY = {
    "A": "mindoro_crossmodel_comparator",
    "B1": "mindoro_primary_validation",
    "archive_r0": "mindoro_b1_r0_archive",
    "B1_archive_r0": "mindoro_b1_r0_archive",
    "B2": "mindoro_legacy_march6",
    "B3": "mindoro_legacy_support",
    "C1": "dwh_deterministic_transfer",
    "C2": "dwh_ensemble_transfer",
    "C3": "dwh_crossmodel_comparator",
    "C1/C2/C3": "dwh_cross_track_summary",
    "prototype_2021": "prototype_2021_support",
    "prototype_2016": "prototype_2016_support",
}

PRIMARY_STATUS_PRIORITY = [
    "thesis_study_box_reference",
    "mindoro_phase4_deferred",
    "mindoro_phase4_shoreline",
    "mindoro_phase4_oil_budget",
    "mindoro_b1_r0_archive",
    "mindoro_primary_validation",
    "mindoro_crossmodel_comparator",
    "mindoro_legacy_march6",
    "mindoro_legacy_support",
    "mindoro_trajectory_context",
    "dwh_cross_track_summary",
    "dwh_crossmodel_comparator",
    "dwh_ensemble_transfer",
    "dwh_deterministic_transfer",
    "dwh_observation_truth_context",
    "dwh_trajectory_context",
    "prototype_2021_support",
    "prototype_2016_support",
]


def get_artifact_status(key: str) -> ArtifactStatus:
    return STATUS_REGISTRY[key]


def status_for_track_id(track_id: str) -> ArtifactStatus | None:
    key = TRACK_ID_TO_STATUS_KEY.get(str(track_id))
    return STATUS_REGISTRY.get(key) if key else None


def _coerce_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _combined_text(record: Mapping[str, Any]) -> str:
    keys = (
        "case_id",
        "track_id",
        "branch_id",
        "track_name",
        "phase_or_track",
        "run_type",
        "figure_id",
        "figure_slug",
        "artifact_group",
        "model_name",
        "relative_path",
        "file_path",
        "forecast_path",
        "observation_path",
        "source_paths",
        "notes",
        "short_plain_language_interpretation",
        "plain_language_interpretation",
        "workflow_mode",
    )
    return " ".join(str(record.get(key) or "") for key in keys).lower()


def _identity_text(record: Mapping[str, Any]) -> str:
    keys = (
        "run_type",
        "figure_slug",
        "figure_id",
        "figure_group_id",
        "figure_group_label",
        "board_family_label",
        "figure_family_label",
        "relative_path",
        "file_path",
    )
    return " ".join(str(record.get(key) or "") for key in keys).lower()


def _is_trajectory_artifact(record: Mapping[str, Any]) -> bool:
    identity = _identity_text(record)
    return any(token in identity for token in ("trajectory", "track", "corridor", "hull", "centroid", "path"))


def get_artifact_surface(key: str) -> ArtifactSurfacePolicy:
    return SURFACE_REGISTRY[key]


def _surface_key_override_for_record(record: Mapping[str, Any], status_key: str) -> str:
    combined = _combined_text(record)
    identity = _identity_text(record)
    track_id = str(record.get("track_id") or "").strip()
    branch_id = str(record.get("branch_id") or "").strip()

    if status_key in {
        "mindoro_primary_validation",
        "mindoro_crossmodel_comparator",
        "mindoro_b1_r0_archive",
    }:
        if (
            branch_id == "R0"
            or track_id in {"archive_r0", "B1_archive_r0", "R0_reinit_p50"}
            or any(
                token in combined
                for token in (
                    "march14_r0_overlay",
                    "crossmodel_r0_overlay",
                    "qa_march14_reinit_r0_overlay",
                    "qa_march14_crossmodel_r0",
                    "r0_reinit_p50",
                )
            )
        ):
            return "archive_only"
        if track_id in {"A", "R1_previous_reinit_p50", "pygnome_reinit_deterministic"}:
            return "comparator_support"
        if branch_id == "R1_previous" or track_id == "B1":
            return "thesis_main"

    if status_key == "thesis_study_box_reference":
        if any(
            token in identity or token in combined
            for token in (
                "focused_phase1_box_geography_reference",
                "scoring_grid_bounds_geography_reference",
                "archived_reference_map",
                "full_context_overview",
            )
        ):
            return "advanced_only"
        return "thesis_main"

    return ""


def surface_key_for_status(status_key: str, record: Mapping[str, Any] | None = None) -> str:
    if not status_key:
        return ""
    if record is not None:
        override = _surface_key_override_for_record(record, status_key)
        if override:
            return override
    return STATUS_SURFACE_KEY_MAP.get(status_key, "")


def surface_key_for_record(record: Mapping[str, Any]) -> str:
    return surface_key_for_status(status_key_for_record(record), record)


def artifact_surface_columns_for_key(
    status_key: str,
    record: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    surface_key = surface_key_for_status(status_key, record)
    if not surface_key:
        return {
            "surface_key": "",
            "surface_label": "",
            "surface_description": "",
            "surface_home_visible": False,
            "surface_panel_visible": False,
            "surface_archive_visible": False,
            "surface_advanced_visible": False,
            "surface_recommended_visible": False,
        }
    surface = SURFACE_REGISTRY[surface_key]
    return {
        "surface_key": surface.key,
        "surface_label": surface.label,
        "surface_description": surface.description,
        "surface_home_visible": surface.home_visible,
        "surface_panel_visible": surface.panel_visible,
        "surface_archive_visible": surface.archive_visible,
        "surface_advanced_visible": surface.advanced_visible,
        "surface_recommended_visible": surface.recommended_visible,
    }


def record_matches_artifact_status(record: Mapping[str, Any], status_key: str) -> bool:
    case_id = str(record.get("case_id") or "").strip().upper()
    track_id = str(record.get("track_id") or "").strip()
    branch_id = str(record.get("branch_id") or "").strip()
    phase_or_track = str(record.get("phase_or_track") or "").strip().lower()
    combined = _combined_text(record)
    trajectory_artifact = _is_trajectory_artifact(record)

    if status_key == "mindoro_b1_r0_archive":
        return (
            (
                case_id == MINDORO_CASE_ID
                or branch_id == "R0"
            )
            and (
                "march14_r0_overlay" in combined
                or "crossmodel_r0_overlay" in combined
                or "qa_march14_reinit_r0_overlay" in combined
                or "qa_march14_crossmodel_r0" in combined
                or "r0 reinit p50" in combined
                or "r0_reinit_p50" in combined
                or "/r0/" in combined
            )
        ) or track_id in {"archive_r0", "B1_archive_r0", "R0_reinit_p50"}
    if status_key == "mindoro_primary_validation":
        return (
            (case_id == MINDORO_CASE_ID and phase_or_track == "phase3b_reinit_primary")
            or track_id == "B1"
            or branch_id == "R1_previous"
        )
    if status_key == "mindoro_crossmodel_comparator":
        return (
            (case_id == MINDORO_CASE_ID and phase_or_track == "phase3a_reinit_crossmodel")
            or track_id in {"A", "R1_previous_reinit_p50", "pygnome_reinit_deterministic"}
        )
    if status_key == "mindoro_legacy_march6":
        return (case_id == MINDORO_CASE_ID and phase_or_track == "phase3b_legacy_strict") or track_id == "B2"
    if status_key == "mindoro_legacy_support":
        return (
            case_id == MINDORO_CASE_ID
            and (
                phase_or_track == "phase3b_support"
                or "broader-support" in combined
                or "broader_support" in combined
                or "public_obs_appendix" in combined
            )
        ) or track_id == "B3"
    if status_key == "mindoro_trajectory_context":
        return case_id == MINDORO_CASE_ID and phase_or_track in {"phase2_official", "phase2_phase3b"}
    if status_key == "thesis_study_box_reference":
        return (
            case_id == "THESIS_STUDY_CONTEXT"
            or phase_or_track == "phase1_study_context"
            or "thesis_study_boxes_reference" in combined
            or "study-box reference" in combined
        )
    if status_key == "mindoro_phase4_oil_budget":
        return case_id == MINDORO_CASE_ID and phase_or_track == "phase4" and "shoreline" not in combined
    if status_key == "mindoro_phase4_shoreline":
        return case_id == MINDORO_CASE_ID and phase_or_track == "phase4" and "shoreline" in combined
    if status_key == "mindoro_phase4_deferred":
        return case_id == MINDORO_CASE_ID and (
            phase_or_track == "phase4_crossmodel_comparability_audit" or "deferred" in combined
        )
    if status_key == "dwh_deterministic_transfer":
        return (
            case_id == DWH_CASE_ID
            and phase_or_track == "phase3c_external_case_run"
            and not trajectory_artifact
        ) or track_id == "C1"
    if status_key == "dwh_ensemble_transfer":
        return (
            case_id == DWH_CASE_ID
            and phase_or_track in {"phase3c_ensemble", "phase3c_external_case_ensemble_comparison"}
            and not trajectory_artifact
        ) or track_id == "C2"
    if status_key == "dwh_crossmodel_comparator":
        return (
            case_id == DWH_CASE_ID
            and phase_or_track in {"phase3c_pygnome_comparator", "phase3c_dwh_pygnome_comparator"}
            and not trajectory_artifact
        ) or track_id == "C3"
    if status_key == "dwh_cross_track_summary":
        return (
            case_id == DWH_CASE_ID
            and (
                track_id == "C1/C2/C3"
                or "summary/comparison" in combined
                or "cross-track comparison summary" in combined
            )
        )
    if status_key == "dwh_trajectory_context":
        return case_id == DWH_CASE_ID and trajectory_artifact
    if status_key == "prototype_2021_support":
        return (
            phase_or_track == "prototype_pygnome_similarity_summary"
            and (
                "prototype_2021" in combined
                or "accepted-segment" in combined
                or ("prototype_2016" not in combined and not _coerce_bool(record.get("legacy_debug_only")))
            )
        ) or track_id == "prototype_2021"
    if status_key == "prototype_2016_support":
        return (
            phase_or_track == "prototype_pygnome_similarity_summary"
            and (
                "prototype_2016" in combined
                or "legacy/debug" in combined
                or "legacy prototype" in combined
                or _coerce_bool(record.get("legacy_debug_only"))
            )
        ) or track_id == "prototype_2016"
    return False


def status_key_for_record(record: Mapping[str, Any]) -> str:
    for key in PRIMARY_STATUS_PRIORITY:
        if record_matches_artifact_status(record, key):
            return key
    return ""


def artifact_status_columns(record: Mapping[str, Any]) -> dict[str, Any]:
    status_key = status_key_for_record(record)
    if not status_key:
        empty_status = {
            "status_key": "",
            "status_label": "",
            "status_role": "",
            "status_reportability": "",
            "status_official_status": "",
            "status_frozen_status": "",
            "status_provenance": "",
            "status_panel_text": "",
            "status_dashboard_summary": "",
        }
        empty_status.update(artifact_surface_columns_for_key("", record))
        return empty_status
    status = STATUS_REGISTRY[status_key]
    payload = {
        "status_key": status.key,
        "status_label": status.label,
        "status_role": status.role,
        "status_reportability": status.reportability,
        "status_official_status": status.official_status,
        "status_frozen_status": status.frozen_status,
        "status_provenance": status.provenance_label,
        "status_panel_text": status.panel_text,
        "status_dashboard_summary": status.dashboard_summary,
    }
    payload.update(artifact_surface_columns_for_key(status_key, record))
    return payload


def artifact_status_columns_for_key(
    status_key: str,
    record: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    status = STATUS_REGISTRY[status_key]
    payload = {
        "status_key": status.key,
        "status_label": status.label,
        "status_role": status.role,
        "status_reportability": status.reportability,
        "status_official_status": status.official_status,
        "status_frozen_status": status.frozen_status,
        "status_provenance": status.provenance_label,
        "status_panel_text": status.panel_text,
        "status_dashboard_summary": status.dashboard_summary,
    }
    payload.update(artifact_surface_columns_for_key(status_key, record))
    return payload
