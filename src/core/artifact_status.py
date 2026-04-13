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


STATUS_REGISTRY: dict[str, ArtifactStatus] = {
    "mindoro_primary_validation": ArtifactStatus(
        key="mindoro_primary_validation",
        label="Mindoro March 13 -> March 14 NOAA reinit primary validation",
        panel_label="Mindoro March 13 -> March 14 primary validation",
        role="primary_validation",
        reportability="reportable_now_inherited_provisional",
        official_status="promoted_primary_validation",
        frozen_status="not_frozen",
        provenance_label=(
            "March 13 seed, March 14 target, explicit shared March 12 imagery caveat, and active "
            "Mindoro-specific recipe provenance from the separate focused 2016-2023 drifter rerun "
            "selecting the same cmems_era5 recipe."
        ),
        panel_text=(
            "Use this as the main Mindoro B1 Phase 3B observation-based spatial validation view. Phase 3B itself does "
            "not directly ingest drifters; it inherits the cmems_era5 recipe selected by the separate focused "
            "2016-2023 Mindoro drifter rerun, while the broader 2016-2022 regional rerun stays reference-only. "
            "Keep the shared March 12 imagery caveat explicit."
        ),
        dashboard_summary=(
            "Promoted primary validation; focused 2016-2023 Mindoro drifter rerun selected the same cmems_era5 "
            "recipe; reportable now but not fully frozen."
        ),
    ),
    "mindoro_crossmodel_comparator": ArtifactStatus(
        key="mindoro_crossmodel_comparator",
        label="Mindoro March 13 -> March 14 cross-model comparator",
        panel_label="Mindoro March 13 -> March 14 cross-model comparator",
        role="comparator_only",
        reportability="reportable_comparator_inherited_provisional",
        official_status="comparator_only_not_truth",
        frozen_status="not_frozen",
        provenance_label="Same March 14 target as B1; same-case comparator support track attached to B1 and PyGNOME remains comparator-only.",
        panel_text="Use this only as the same-case comparator support track attached to B1 on the March 14 target; PyGNOME is a comparator, not truth, and A is not a co-primary validation row.",
        dashboard_summary="Same-case comparator-only support track attached to B1 on the promoted March 14 target.",
    ),
    "mindoro_legacy_march6": ArtifactStatus(
        key="mindoro_legacy_march6",
        label="Mindoro legacy March 6 sparse strict reference",
        panel_label="Mindoro legacy March 6 honesty / limitations",
        role="legacy_reference",
        reportability="reportable_legacy_reference_inherited_provisional",
        official_status="legacy_reference_not_primary",
        frozen_status="not_frozen",
        provenance_label="Sparse March 6 reference retained for methods honesty.",
        panel_text="Keep B2 visible for honesty and limitations, but do not label it as the primary, official, or frozen Mindoro result.",
        dashboard_summary="B2 legacy honesty reference; not the promoted primary row.",
    ),
    "mindoro_legacy_support": ArtifactStatus(
        key="mindoro_legacy_support",
        label="Mindoro legacy March 3-6 broader-support reference",
        panel_label="Mindoro legacy March 3-6 broader-support reference",
        role="legacy_support_reference",
        reportability="reportable_legacy_reference_inherited_provisional",
        official_status="legacy_support_not_primary",
        frozen_status="not_frozen",
        provenance_label="Broader-support event context preserved for narrative reference.",
        panel_text="Use B3 as broader-support context only; do not present it as the promoted primary validation row.",
        dashboard_summary="B3 legacy broader-support context; not a primary row.",
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
        panel_text="Use this for transport context before score tables; the transport story is scientifically usable, but not yet frozen.",
        dashboard_summary="Transport-context layer from a scientifically usable but not frozen branch.",
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
            "Use this as the clean baseline DWH transfer-validation view; it supports the external-case success "
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
            "Use this to establish the DWH observation-derived truth masks before any deterministic, ensemble, or "
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
            "Use this to explain deterministic, p50, and p90 differences without overstating ensemble benefit as "
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
        panel_text="Use this for cross-model comparison only; PyGNOME is not truth and does not replace the OpenDrift DWH story.",
        dashboard_summary="Frozen DWH cross-model comparator; PyGNOME not truth.",
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
        panel_text="Use this when the panel wants path intuition before or after the score-based boards.",
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
        provenance_label="Legacy/debug regression lane preserved as a support-only Phase 1/2/3A/4/5 package for reproducibility and thesis context only, including a budget-only deterministic Phase 4 PyGNOME comparator pilot when stored outputs are present.",
        panel_text="Legacy debug support only; visible support flow is Phase 1/2/3A/4/5, with Phase 3A comparator-only, a budget-only Phase 4 PyGNOME pilot when packaged, and a dedicated curated final package under output/2016 Legacy Runs FINAL Figures. Not final Phase 1 evidence.",
        dashboard_summary="Legacy debug support lane with a dedicated curated Phase 5 package and an optional budget-only Phase 4 PyGNOME pilot; not the preferred prototype lane or final Phase 1 evidence.",
    ),
}


TRACK_ID_TO_STATUS_KEY = {
    "A": "mindoro_crossmodel_comparator",
    "B1": "mindoro_primary_validation",
    "B2": "mindoro_legacy_march6",
    "B3": "mindoro_legacy_support",
    "C1": "dwh_deterministic_transfer",
    "C2": "dwh_ensemble_transfer",
    "C3": "dwh_crossmodel_comparator",
    "prototype_2021": "prototype_2021_support",
    "prototype_2016": "prototype_2016_support",
}

PRIMARY_STATUS_PRIORITY = [
    "mindoro_phase4_deferred",
    "mindoro_phase4_shoreline",
    "mindoro_phase4_oil_budget",
    "mindoro_primary_validation",
    "mindoro_crossmodel_comparator",
    "mindoro_legacy_march6",
    "mindoro_legacy_support",
    "mindoro_trajectory_context",
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
        "phase_or_track",
        "run_type",
        "figure_id",
        "figure_slug",
        "relative_path",
        "file_path",
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


def record_matches_artifact_status(record: Mapping[str, Any], status_key: str) -> bool:
    case_id = str(record.get("case_id") or "").strip().upper()
    track_id = str(record.get("track_id") or "").strip()
    phase_or_track = str(record.get("phase_or_track") or "").strip().lower()
    combined = _combined_text(record)
    trajectory_artifact = _is_trajectory_artifact(record)

    if status_key == "mindoro_primary_validation":
        return (case_id == MINDORO_CASE_ID and phase_or_track == "phase3b_reinit_primary") or track_id == "B1"
    if status_key == "mindoro_crossmodel_comparator":
        return (case_id == MINDORO_CASE_ID and phase_or_track == "phase3a_reinit_crossmodel") or track_id == "A"
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


def artifact_status_columns(record: Mapping[str, Any]) -> dict[str, str]:
    status_key = status_key_for_record(record)
    if not status_key:
        return {
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
    status = STATUS_REGISTRY[status_key]
    return {
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


def artifact_status_columns_for_key(status_key: str) -> dict[str, str]:
    status = STATUS_REGISTRY[status_key]
    return {
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
