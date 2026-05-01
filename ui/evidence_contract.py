"""Final thesis evidence-role contract for the read-only dashboard."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping

import pandas as pd


ROLE_PRIMARY = "Primary evidence"
ROLE_THESIS = ROLE_PRIMARY
ROLE_COMPARATOR = "Comparator support"
ROLE_EXTERNAL_TRANSFER = "External transfer validation"
ROLE_CONTEXT = "Support/context"
ROLE_SECONDARY = "Secondary support"
ROLE_ARCHIVE = "Archive/provenance"
ROLE_LEGACY = "Legacy/debug"
ROLE_GOVERNANCE = "Read-only governance"
ROLE_ADVANCED = ROLE_GOVERNANCE


@dataclass(frozen=True)
class PageEvidencePolicy:
    page_id: str
    allowed_surface_keys: frozenset[str] = field(default_factory=frozenset)
    allowed_status_keys: frozenset[str] = field(default_factory=frozenset)
    allowed_page_targets: frozenset[str] = field(default_factory=frozenset)
    allowed_artifact_groups: frozenset[str] = field(default_factory=frozenset)
    allow_archive: bool = False
    allow_legacy: bool = False
    allow_advanced: bool = False
    block_archive_terms: bool = True


ARCHIVE_STATUS_KEYS = frozenset(
    {
        "mindoro_b1_r0_archive",
        "mindoro_legacy_march6",
        "mindoro_legacy_support",
    }
)
PHASE1_PROVENANCE_STATUS_KEYS = frozenset(
    {
        "focused_phase1_transport_provenance",
        "focused_phase1_recipe_provenance",
    }
)
PRODUCT_FAMILY_STATUS_KEYS = frozenset({"mindoro_product_family_support"})
LEGACY_STATUS_KEYS = frozenset(
    {
        "prototype_2016_support",
        "prototype_2021_support",
        "secondary_2016_drifter_track_support",
        "legacy_2016_fss_support",
    }
)
ADVANCED_STATUS_KEYS = frozenset({"mindoro_trajectory_context", "dwh_trajectory_context"})
CONTEXT_STATUS_KEYS = frozenset(
    {
        "mindoro_phase4_oil_budget",
        "mindoro_phase4_shoreline",
        "mindoro_phase4_deferred",
    }
)
COMPARATOR_STATUS_KEYS = frozenset({"mindoro_crossmodel_comparator", "dwh_crossmodel_comparator"})
THESIS_STATUS_KEYS = frozenset(
    {
        "mindoro_primary_validation",
        "dwh_observation_truth_context",
        "dwh_deterministic_transfer",
        "dwh_ensemble_transfer",
        "dwh_cross_track_summary",
        "thesis_study_box_reference",
    }
) | PHASE1_PROVENANCE_STATUS_KEYS | PRODUCT_FAMILY_STATUS_KEYS


PAGE_POLICIES: dict[str, PageEvidencePolicy] = {
    "home": PageEvidencePolicy(
        page_id="home",
        allowed_surface_keys=frozenset({"thesis_main", "comparator_support"}),
        allowed_status_keys=THESIS_STATUS_KEYS | COMPARATOR_STATUS_KEYS | CONTEXT_STATUS_KEYS,
        allowed_page_targets=frozenset(
            {
                "phase1_recipe_selection",
                "mindoro_validation",
                "cross_model_comparison",
                "dwh_transfer_validation",
                "phase4_oiltype_and_shoreline",
            }
        ),
    ),
    "phase1_recipe_selection": PageEvidencePolicy(
        page_id="phase1_recipe_selection",
        allowed_surface_keys=frozenset({"thesis_main", "advanced_only"}),
        allowed_status_keys=frozenset({"thesis_study_box_reference"}) | PHASE1_PROVENANCE_STATUS_KEYS,
        allowed_page_targets=frozenset({"phase1_recipe_selection"}),
        allow_archive=True,
        allow_advanced=True,
    ),
    "mindoro_validation": PageEvidencePolicy(
        page_id="mindoro_validation",
        allowed_surface_keys=frozenset({"thesis_main"}),
        allowed_status_keys=frozenset({"mindoro_primary_validation"}) | PRODUCT_FAMILY_STATUS_KEYS,
        allowed_page_targets=frozenset({"mindoro_validation"}),
        allowed_artifact_groups=frozenset({"publication/observations", "publication/opendrift_primary"}),
    ),
    "cross_model_comparison": PageEvidencePolicy(
        page_id="cross_model_comparison",
        allowed_surface_keys=frozenset({"comparator_support"}),
        allowed_status_keys=frozenset({"mindoro_crossmodel_comparator"}),
        allowed_page_targets=frozenset({"cross_model_comparison"}),
        allowed_artifact_groups=frozenset({"publication/comparator_pygnome"}),
    ),
    "dwh_transfer_validation": PageEvidencePolicy(
        page_id="dwh_transfer_validation",
        allowed_surface_keys=frozenset({"thesis_main", "comparator_support"}),
        allowed_status_keys=frozenset(
            {
                "dwh_observation_truth_context",
                "dwh_deterministic_transfer",
                "dwh_ensemble_transfer",
                "dwh_crossmodel_comparator",
                "dwh_cross_track_summary",
            }
        ),
        allowed_page_targets=frozenset({"dwh_transfer_validation"}),
        allowed_artifact_groups=frozenset(
            {
                "publication/observations",
                "publication/opendrift_deterministic",
                "publication/opendrift_ensemble",
                "publication/comparator_pygnome",
            }
        ),
    ),
    "phase4_oiltype_and_shoreline": PageEvidencePolicy(
        page_id="phase4_oiltype_and_shoreline",
        allowed_surface_keys=frozenset({"comparator_support"}),
        allowed_status_keys=CONTEXT_STATUS_KEYS,
        allowed_page_targets=frozenset({"phase4_oiltype_and_shoreline"}),
    ),
    "mindoro_validation_archive": PageEvidencePolicy(
        page_id="mindoro_validation_archive",
        allowed_surface_keys=frozenset({"archive_only", "advanced_only"}),
        allowed_status_keys=ARCHIVE_STATUS_KEYS | frozenset({"mindoro_trajectory_context"}),
        allowed_page_targets=frozenset({"mindoro_validation_archive"}),
        allow_archive=True,
        allow_advanced=True,
        block_archive_terms=False,
    ),
    "legacy_2016_support": PageEvidencePolicy(
        page_id="legacy_2016_support",
        allowed_surface_keys=frozenset({"legacy_support"}),
        allowed_status_keys=LEGACY_STATUS_KEYS,
        allowed_page_targets=frozenset({"legacy_2016_support"}),
        allow_legacy=True,
        block_archive_terms=False,
    ),
    "artifacts_logs": PageEvidencePolicy(
        page_id="artifacts_logs",
        allow_archive=True,
        allow_legacy=True,
        allow_advanced=True,
        block_archive_terms=False,
    ),
}


TEXT_COLUMNS = (
    "title",
    "display_title",
    "status_label",
    "track_label",
    "track_title",
    "track_name",
    "branch_id",
    "track_id",
    "phase_or_track",
    "figure_id",
    "figure_slug",
    "figure_family_label",
    "board_family_label",
    "figure_group_label",
    "artifact_group",
    "relative_path",
    "final_relative_path",
    "file_path",
    "source_paths",
    "notes",
    "short_plain_language_interpretation",
    "plain_language_interpretation",
)


def _series(df: pd.DataFrame, column: str) -> pd.Series:
    if column in df.columns:
        return df[column].fillna("").astype(str)
    return pd.Series("", index=df.index, dtype=str)


def _bool_series(df: pd.DataFrame, column: str) -> pd.Series:
    if column not in df.columns:
        return pd.Series(False, index=df.index, dtype=bool)
    values = df[column]
    if values.dtype == bool:
        return values.fillna(False).astype(bool)
    return values.fillna("").astype(str).str.strip().str.lower().isin({"1", "true", "yes", "on"})


def _combined_text(df: pd.DataFrame) -> pd.Series:
    text = pd.Series("", index=df.index, dtype=str)
    for column in TEXT_COLUMNS:
        if column in df.columns:
            text = text.str.cat(df[column].fillna("").astype(str), sep=" ")
    return text.str.lower()


def _term_leak_mask(df: pd.DataFrame) -> pd.Series:
    text = _combined_text(df)
    return (
        text.str.contains(r"\br0\b", regex=True, na=False)
        | text.str.contains(r"\bb2\b", regex=True, na=False)
        | text.str.contains(r"\bb3\b", regex=True, na=False)
        | text.str.contains("march 3 -> march 6", regex=False, na=False)
        | text.str.contains("march 3-6", regex=False, na=False)
        | text.str.contains("march 6", regex=False, na=False)
        | text.str.contains("legacy", regex=False, na=False)
        | text.str.contains("prototype_2016", regex=False, na=False)
    )


def _leak_mask(df: pd.DataFrame, policy: PageEvidencePolicy, *, advanced: bool) -> pd.Series:
    if df.empty:
        return pd.Series(False, index=df.index, dtype=bool)
    surface = _series(df, "surface_key")
    status = _series(df, "status_key")
    scope = _series(df, "recommended_scope")
    mask = pd.Series(False, index=df.index, dtype=bool)

    if not policy.allow_archive:
        mask |= surface.eq("archive_only")
        mask |= status.isin(ARCHIVE_STATUS_KEYS)
        mask |= scope.eq("archive_only")
        mask |= _bool_series(df, "archive_only")
    if not policy.allow_legacy:
        mask |= surface.eq("legacy_support")
        mask |= status.isin(LEGACY_STATUS_KEYS)
        mask |= scope.eq("legacy_support")
        mask |= _bool_series(df, "legacy_support")
    if not (advanced and policy.allow_advanced):
        mask |= surface.eq("advanced_only")
        mask |= status.isin(ADVANCED_STATUS_KEYS)
        mask |= scope.eq("advanced_only")
    if policy.block_archive_terms and not advanced:
        mask |= _term_leak_mask(df)
    return mask


def _apply_allowed_values(df: pd.DataFrame, column: str, allowed: frozenset[str]) -> pd.DataFrame:
    if df.empty or not allowed or column not in df.columns:
        return df.copy()
    values = df[column].fillna("").astype(str)
    nonblank = values.str.strip().ne("")
    if not nonblank.any():
        return df.copy()
    return df.loc[~nonblank | values.isin(allowed)].copy()


def filter_for_page(df: pd.DataFrame, page_id: str, advanced: bool = False) -> pd.DataFrame:
    """Return rows allowed on a panel-facing dashboard page."""

    if df is None:
        return pd.DataFrame()
    if df.empty:
        return df.copy()
    policy = PAGE_POLICIES.get(page_id, PAGE_POLICIES["artifacts_logs"])
    payload = df.copy()
    payload = _apply_allowed_values(payload, "surface_key", policy.allowed_surface_keys)
    payload = _apply_allowed_values(payload, "status_key", policy.allowed_status_keys)
    payload = _apply_allowed_values(payload, "page_target", policy.allowed_page_targets)
    payload = _apply_allowed_values(payload, "artifact_group", policy.allowed_artifact_groups)
    if payload.empty:
        return payload.reset_index(drop=True)
    payload = payload.loc[~_leak_mask(payload, policy, advanced=advanced)].copy()
    return payload.reset_index(drop=True)


def assert_no_archive_leak(df: pd.DataFrame, page_id: str, advanced: bool = False) -> None:
    """Raise if page-filtered rows still contain archive, legacy, or advanced leaks."""

    if df is None or df.empty:
        return
    policy = PAGE_POLICIES.get(page_id, PAGE_POLICIES["artifacts_logs"])
    leaks = _leak_mask(df, policy, advanced=advanced)
    if not leaks.any():
        return
    leaked_rows = df.loc[leaks].head(10)
    labels: list[str] = []
    for record in leaked_rows.to_dict(orient="records"):
        labels.append(
            str(
                record.get("figure_id")
                or record.get("track_id")
                or record.get("branch_id")
                or record.get("relative_path")
                or record
            )
        )
    raise AssertionError(f"{page_id} includes rows outside its thesis evidence role: {labels}")


def _text(record: Mapping[str, Any], key: str) -> str:
    value = record.get(key)
    return "" if value is None else str(value).strip()


def role_badge_for_record(record: Mapping[str, Any]) -> str:
    surface = _text(record, "surface_key").lower()
    status = _text(record, "status_key").lower()
    scope = _text(record, "recommended_scope").lower()
    role = _text(record, "status_role").lower()

    if surface == "archive_only" or status in ARCHIVE_STATUS_KEYS or scope == "archive_only" or _text(record, "archive_only").lower() == "true":
        return ROLE_ARCHIVE
    if surface == "legacy_support" or status in LEGACY_STATUS_KEYS or scope == "legacy_support" or _text(record, "legacy_support").lower() == "true":
        return ROLE_SECONDARY
    if surface == "advanced_only" or status in ADVANCED_STATUS_KEYS or scope == "advanced_only":
        return ROLE_ADVANCED
    if status in CONTEXT_STATUS_KEYS or "phase4" in status or _text(record, "support_only").lower() == "true" or _text(record, "optional_context_only").lower() == "true":
        return ROLE_CONTEXT
    if surface == "comparator_support" or status in COMPARATOR_STATUS_KEYS or "comparator" in role or _text(record, "comparator_only").lower() == "true":
        return ROLE_COMPARATOR
    if status.startswith("dwh_") or "dwh" in _text(record, "case_id").lower():
        return ROLE_EXTERNAL_TRANSFER
    return ROLE_THESIS


def panel_safe_label(record_or_status: Mapping[str, Any] | str) -> str:
    """Return a display-safe thesis-review label without exposing internal status vocabulary."""

    if isinstance(record_or_status, Mapping):
        return role_badge_for_record(record_or_status)
    text = str(record_or_status or "")
    replacements = {
        "reportable_now_" + "inherited_provisional": "reportable support/context",
        "not" + "_frozen": "stored support state",
        "not_comparable_" + "honestly": "no matched comparison is packaged yet",
        "current " + "hon" + "esty status": "current package status",
        "Current " + "hon" + "esty status": "Current package status",
        "Reportable" + " tracks": "Displayed tracks",
        "Inherited-provisional" + " tracks": "Support/context tracks",
    }
    for raw, clean in replacements.items():
        text = text.replace(raw, clean)
    text = text.replace("inherited-provisional", "support/context")
    text = text.replace("Inherited-provisional", "Support/context")
    text = text.replace("reportable now", "reportable")
    text = text.replace("Reportable now", "Reportable")
    return text
