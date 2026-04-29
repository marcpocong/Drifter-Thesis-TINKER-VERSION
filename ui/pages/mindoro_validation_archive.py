"""Mindoro validation archive page."""

from __future__ import annotations

from pathlib import Path

try:
    from ui.bootstrap import ensure_repo_root_on_path
except ModuleNotFoundError:
    import sys

    _UI_DIR = Path(__file__).resolve().parents[1]
    _UI_DIR_TEXT = str(_UI_DIR)
    if _UI_DIR_TEXT not in sys.path:
        sys.path.insert(0, _UI_DIR_TEXT)
    from bootstrap import ensure_repo_root_on_path

ensure_repo_root_on_path(__file__)

import pandas as pd
import streamlit as st

from src.core.artifact_status import get_artifact_status
from ui.data_access import figure_subset
from ui.evidence_contract import ROLE_ARCHIVE
from ui.pages.common import (
    render_archive_notice,
    render_badge_strip,
    render_export_note,
    render_feature_grid,
    render_figure_gallery,
    render_key_takeaway,
    render_markdown_block,
    render_modern_hero,
    render_section_header,
    render_section_stack,
    render_status_callout,
    render_table,
)


def _filter_values(df, *, column: str, allowed_values: set[str] | None = None):
    if df is None or df.empty or column not in df.columns:
        return df
    payload = df.copy()
    series = payload[column].fillna("").astype(str)
    if allowed_values:
        payload = payload.loc[series.isin(sorted(allowed_values))].copy()
    return payload.reset_index(drop=True)


def _filter_text(df, *tokens: str):
    if df is None or df.empty:
        return df
    searchable = (
        df.get("figure_id", pd.Series("", index=df.index)).fillna("").astype(str)
        + " "
        + df.get("relative_path", pd.Series("", index=df.index)).fillna("").astype(str)
        + " "
        + df.get("source_paths", pd.Series("", index=df.index)).fillna("").astype(str)
        + " "
        + df.get("notes", pd.Series("", index=df.index)).fillna("").astype(str)
    ).str.lower()
    mask = pd.Series(False, index=df.index)
    for token in tokens:
        mask |= searchable.str.contains(token.lower(), na=False)
    return df.loc[mask].reset_index(drop=True)


def render(state: dict, ui_state: dict) -> None:
    export_mode = bool(ui_state.get("export_mode"))
    case_id = "CASE_MINDORO_RETRO_2023"
    b2_status = get_artifact_status("mindoro_legacy_march6")
    b3_status = get_artifact_status("mindoro_legacy_support")

    archive_registry = state["final_validation_case_registry"].loc[
        state["final_validation_case_registry"].get("surface_key", "").astype(str).eq("archive_only")
    ].reset_index(drop=True)
    archive_limitations = state["final_validation_limitations"].loc[
        state["final_validation_limitations"].get("surface_key", "").astype(str).eq("archive_only")
    ].reset_index(drop=True)
    r0_summary = state["mindoro_b1_summary"].loc[
        state["mindoro_b1_summary"].get("surface_key", "").astype(str).eq("archive_only")
    ].reset_index(drop=True)
    r0_fss = state["mindoro_b1_fss"].loc[
        state["mindoro_b1_fss"].get("surface_key", "").astype(str).eq("archive_only")
    ].reset_index(drop=True)
    r0_comparator_summary = state["mindoro_comparator_summary"].loc[
        state["mindoro_comparator_summary"].get("surface_key", "").astype(str).eq("archive_only")
    ].reset_index(drop=True)
    r0_comparator_ranking = state["mindoro_comparator_ranking"].loc[
        state["mindoro_comparator_ranking"].get("surface_key", "").astype(str).eq("archive_only")
    ].reset_index(drop=True)

    r0_publication = figure_subset("publication", case_id=case_id, surface_keys=["archive_only"])
    archived_final_output = state["mindoro_final_archive_registry"]
    r0_baseline_figures = _filter_text(r0_publication, "march14_r0_overlay")
    r0_including_figures = _filter_text(
        archived_final_output,
        "crossmodel_r0",
        "mindoro_primary_validation_board",
        "mindoro_crossmodel_board",
    )
    b2_figures = figure_subset("publication", case_id=case_id, status_keys=[b2_status.key], surface_keys=["archive_only"])
    b3_figures = figure_subset("publication", case_id=case_id, status_keys=[b3_status.key], surface_keys=["archive_only"])
    transport_context_figures = figure_subset("panel", case_id=case_id, surface_keys=["advanced_only"])

    render_modern_hero(
        "Archive — Mindoro Validation Provenance",
        "This page centralizes archived Mindoro validation material that remains repo-preserved for provenance, audit, and reproducibility. The main paper and thesis-facing Mindoro reporting will use the March 13 -> March 14 R1 primary validation row only.",
        badge=ROLE_ARCHIVE,
        eyebrow="Archive / support only",
        meta=["Provenance layer", "Not primary claim", "Read-only artifacts"],
        tone="archive",
    )

    if export_mode:
        render_export_note(
            [
                "Export mode keeps the archive page concise and clearly separated from thesis-facing evidence.",
                "Do not cite this page as the main Mindoro validation row; the main paper uses the March 13 -> March 14 R1 primary validation row only.",
            ]
        )

    render_key_takeaway(
        "Archive material stays secondary.",
        "The March 13 -> March 14 R1 primary validation row is the only thesis-facing Mindoro B1 validation row used in the main paper.",
        tone="archive",
        badge=ROLE_ARCHIVE,
    )
    render_archive_notice(
        "Archive boundary",
        "B2, B3, R0, and older R0-including outputs remain preserved for audit and reproducibility only. They are not part of the main Mindoro validation claim.",
    )
    render_feature_grid(
        [
            {
                "title": "Main paper rule",
                "body": "Use B1 R1_previous on the main Mindoro page for the thesis-facing public-observation validation claim.",
                "badge": ROLE_ARCHIVE,
                "tone": "archive",
            },
            {
                "title": "Naming note",
                "body": "March 13 -> March 14 R1 refers to the B1 validation branch, not the Phase 1 Recipe Code R1 family.",
                "badge": ROLE_ARCHIVE,
                "tone": "archive",
            },
            {
                "title": "Review posture",
                "body": "This page is for provenance review: preserved figures, archived rows, limitations, and decision notes.",
                "badge": ROLE_ARCHIVE,
                "tone": "archive",
            },
        ],
        columns_per_row=3,
        export_mode=export_mode,
    )

    render_status_callout("ARCHIVE / SUPPORT ONLY — not part of the main Mindoro validation claim.", "ARCHIVE / SUPPORT ONLY — not part of the main Mindoro validation claim.", "warning")
    render_status_callout(
        "Main paper rule",
        "The March 13 -> March 14 R1 primary validation row will be the only thesis-facing Mindoro B1 validation row used in the main paper.",
        "info",
    )
    render_status_callout(
        "Naming note",
        "March 13 -> March 14 R1 on this page refers to the B1 validation branch. It is not the same label as the Phase 1 Recipe Code R1 family.",
        "info",
    )

    def _decision_note() -> None:
        render_markdown_block(
            "Archive Decision Memo",
            state["mindoro_validation_archive_decision"],
            collapsed=not export_mode,
            export_mode=export_mode,
        )

    def _r0_archived_baseline() -> None:
        render_badge_strip([ROLE_ARCHIVE])
        render_status_callout(
            "Not thesis-facing",
            "The March 13 -> March 14 R0 archived baseline is preserved for provenance and reproducibility only. It is excluded from thesis-facing methodology, tables, figures, and headline claims.",
            "warning",
        )
        render_figure_gallery(
            r0_baseline_figures,
            title="March 13 -> March 14 R0 archived baseline",
            caption="Repo-preserved publication rendering of the archived March 13 -> March 14 R0 baseline.",
            limit=2 if export_mode else 3,
            columns_per_row=1 if export_mode else 2,
            export_mode=export_mode,
            overlay_label="Click to enlarge",
        )
        render_table(
            "March 13 -> March 14 R0 archived baseline summary",
            r0_summary,
            download_name="march13_14_r0_archive_summary.csv",
            caption="Stored R0 archived-baseline summary retained for provenance only.",
            height=220,
            export_mode=export_mode,
        )
        render_table(
            "March 13 -> March 14 R0 archived baseline FSS",
            r0_fss,
            download_name="march13_14_r0_archive_fss.csv",
            caption="Stored R0 archived-baseline FSS table retained for provenance only.",
            height=220,
            export_mode=export_mode,
        )

    def _r0_including_outputs() -> None:
        render_badge_strip([ROLE_ARCHIVE])
        render_status_callout(
            "Provenance-only older outputs",
            "Older March13-14 outputs that included or foregrounded R0 remain accessible here only. They are repo-preserved and archive-only.",
            "warning",
        )
        render_figure_gallery(
            r0_including_figures,
            title="Archived R0-including March13-14 outputs",
            caption="Older March13-14 final-output boards and comparator outputs that foregrounded or included R0 are preserved here for audit and reproducibility only.",
            limit=2 if export_mode else 3,
            columns_per_row=1 if export_mode else 2,
            export_mode=export_mode,
            overlay_label="Click to enlarge",
        )
        render_table(
            "Archived March13-14 R0 comparator summary",
            r0_comparator_summary,
            download_name="march13_14_r0_archive_comparator_summary.csv",
            caption="Stored R0-only comparator summary retained for archive review.",
            height=220,
            export_mode=export_mode,
        )
        render_table(
            "Archived March13-14 R0 comparator ranking",
            r0_comparator_ranking,
            download_name="march13_14_r0_archive_comparator_ranking.csv",
            caption="Stored R0-only comparator ranking retained for archive review.",
            height=220,
            export_mode=export_mode,
        )

    def _b2_archive() -> None:
        render_badge_strip([ROLE_ARCHIVE])
        render_figure_gallery(
            b2_figures,
            title="B2 archived March 6 sparse-reference row",
            caption="B2 is preserved as archive-only provenance material and is not part of thesis-facing reporting.",
            limit=2 if export_mode else 3,
            columns_per_row=1 if export_mode else 2,
            export_mode=export_mode,
            overlay_label="Click to enlarge",
        )
        render_table(
            "B2 archive registry row",
            _filter_values(archive_registry, column="track_id", allowed_values={"B2"}),
            download_name="mindoro_b2_archive_registry.csv",
            caption="Archive-only registry view for the preserved March 6 sparse strict reference.",
            height=180,
            export_mode=export_mode,
        )

    def _b3_archive() -> None:
        render_badge_strip([ROLE_ARCHIVE])
        render_figure_gallery(
            b3_figures,
            title="B3 archived March 3-6 broader-support row",
            caption="B3 is preserved as archive-only provenance material and is not part of thesis-facing reporting.",
            limit=2 if export_mode else 3,
            columns_per_row=1 if export_mode else 2,
            export_mode=export_mode,
            overlay_label="Click to enlarge",
        )
        render_table(
            "B3 archive registry row",
            _filter_values(archive_registry, column="track_id", allowed_values={"B3"}),
            download_name="mindoro_b3_archive_registry.csv",
            caption="Archive-only registry view for the preserved March 3-6 broader-support reference.",
            height=180,
            export_mode=export_mode,
        )

    def _transport_context() -> None:
        render_badge_strip([ROLE_ARCHIVE])
        if transport_context_figures is None or transport_context_figures.empty:
            render_status_callout("Unavailable", "Not packaged in current repo state.", "neutral")
            return
        render_figure_gallery(
            transport_context_figures,
            title="Transport-context archive support",
            caption="Optional transport-context figures retained as read-only archive support rather than main-page evidence.",
            limit=2 if export_mode else 2,
            columns_per_row=1 if export_mode else 2,
            export_mode=export_mode,
            overlay_label="Click to enlarge",
        )

    def _archive_tables() -> None:
        render_badge_strip([ROLE_ARCHIVE])
        render_table(
            "Archived Mindoro validation registry",
            archive_registry,
            download_name="mindoro_validation_archive_registry.csv",
            caption="Archive-only registry rows for the preserved March-family materials.",
            height=220,
            export_mode=export_mode,
        )
        render_table(
            "Archived Mindoro limitations",
            archive_limitations,
            download_name="mindoro_validation_archive_limitations.csv",
            caption="Archive-only limitations and provenance notes for the preserved March-family materials.",
            height=220,
            export_mode=export_mode,
        )

    sections = [
        ("Archive Note / Team Decision", _decision_note),
        ("March 13 -> March 14 R0 Archived Baseline", _r0_archived_baseline),
        ("Older March13-14 Outputs That Included R0", _r0_including_outputs),
        ("B2 Archived March 6 Sparse Reference", _b2_archive),
        ("B3 Archived March 3-6 Broader-Support Row", _b3_archive),
        ("Archived Tables / Notes / Manifests", _archive_tables),
    ]
    if ui_state["advanced"]:
        sections.insert(5, ("Transport-Context Archive Support", _transport_context))

    render_section_header(
        "Archive Details",
        "Raw archive figures and tables are grouped below so they remain inspectable without being visually equal to the main thesis pages.",
        badge=ROLE_ARCHIVE,
    )
    render_section_stack(
        sections,
        export_mode=export_mode,
        use_tabs=ui_state["advanced"],
    )
