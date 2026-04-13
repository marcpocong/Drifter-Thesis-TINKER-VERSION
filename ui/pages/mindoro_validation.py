"""Mindoro B1 primary-validation page."""

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

import streamlit as st

from src.core.artifact_status import get_artifact_status
from ui.data_access import figure_subset
from ui.pages.common import (
    render_export_note,
    render_figure_gallery,
    render_markdown_block,
    render_page_intro,
    render_section_stack,
    render_status_callout,
    render_table,
)


def _mindoro_final_subset(df, artifact_groups: set[str]) -> object:
    if df.empty:
        return df
    if "artifact_group" not in df.columns:
        return df
    return df.loc[df.get("artifact_group", "").astype(str).isin(sorted(artifact_groups))].reset_index(drop=True)


def render(state: dict, ui_state: dict) -> None:
    export_mode = bool(ui_state.get("export_mode"))
    primary_status = get_artifact_status("mindoro_primary_validation")
    legacy_status = get_artifact_status("mindoro_legacy_march6")
    support_status = get_artifact_status("mindoro_legacy_support")
    trajectory_status = get_artifact_status("mindoro_trajectory_context")

    render_page_intro(
        "Mindoro B1 Primary Validation",
        "This page leads with the promoted March 13 -> March 14 B1 family, keeps the shared-imagery caveat visible, and treats all other Mindoro rows as comparator or support material rather than co-primary evidence.",
        badge="Mindoro B1 | primary validation row",
    )

    if export_mode:
        render_export_note(
            [
                "Export mode presents the Mindoro validation story as a single sequential brief.",
                "B1 stays first, Track A remains comparator-only, B2 stays a legacy reference row, and B3 remains broader support rather than co-primary evidence.",
            ]
        )

    render_status_callout("Primary validation", primary_status.panel_text, "info")
    render_status_callout(
        "Shared-imagery caveat",
        "The March 13 and March 14 public products share March 12 WorldView-3 imagery provenance, so this row is a reinitialization-based public-validation pair rather than an independent day-to-day validation claim.",
        "warning",
    )
    render_status_callout(
        "Recipe provenance",
        "B1 inherits the cmems_era5 recipe selected by the separate focused 2016-2023 Mindoro Phase 1 rerun. Phase 3B itself does not directly ingest drifters.",
        "info",
    )

    mindoro_final_registry = state["mindoro_final_registry"]
    primary_figures = _mindoro_final_subset(
        mindoro_final_registry,
        {"publication/observations", "publication/opendrift_primary"},
    )
    comparator_figures = _mindoro_final_subset(
        mindoro_final_registry,
        {"publication/comparator_pygnome"},
    )
    legacy_figures = figure_subset(
        ui_state["visual_layer"],
        case_id="CASE_MINDORO_RETRO_2023",
        status_keys=[legacy_status.key],
    )
    support_figures = figure_subset(
        ui_state["visual_layer"],
        case_id="CASE_MINDORO_RETRO_2023",
        status_keys=[support_status.key],
    )
    trajectory_figures = figure_subset(
        ui_state["visual_layer"],
        case_id="CASE_MINDORO_RETRO_2023",
        status_keys=[trajectory_status.key],
    )

    def _primary_package() -> None:
        render_figure_gallery(
            primary_figures,
            title="B1 curated primary-validation figures",
            caption="These figures come from the curated Phase 3B March13-14 final package and should be used first for thesis-facing Mindoro discussion. Click any figure to enlarge it and read the fuller interpretation there.",
            limit=2 if export_mode else (None if ui_state["advanced"] else 5),
            columns_per_row=1 if export_mode else 2,
            export_mode=export_mode,
            overlay_label="Click to enlarge",
        )
        render_table(
            "B1 summary",
            state["mindoro_b1_summary"],
            download_name="march13_14_reinit_summary.csv",
            caption="Curated B1 summary table from the final March13-14 package.",
            height=250,
            export_mode=export_mode,
        )

    def _comparator_support() -> None:
        render_status_callout(
            "Comparator-only rule",
            "Track A is attached to B1 as same-case comparator support. PyGNOME is comparator-only and never truth.",
            "warning",
        )
        render_figure_gallery(
            comparator_figures,
            title="A comparator-only figures",
            caption="These figures come from the curated comparator subgroup under the final March13-14 package and remain separate from the primary B1 claim. Click any figure to enlarge it.",
            limit=2 if export_mode else (None if ui_state["advanced"] else 4),
            columns_per_row=1 if export_mode else 2,
            export_mode=export_mode,
            overlay_label="Click to enlarge",
        )
        render_table(
            "Comparator model ranking",
            state["mindoro_comparator_ranking"],
            download_name="march13_14_reinit_crossmodel_model_ranking.csv",
            caption="Curated cross-model ranking table for the same-case March 14 comparator lane.",
            height=240,
            export_mode=export_mode,
        )

    def _legacy_reference() -> None:
        render_status_callout("Legacy reference rule", "B2 remains visible as a legacy reference and limitations row, but it is not the main Mindoro validation claim.", "warning")
        render_figure_gallery(
            legacy_figures,
            title=legacy_status.panel_label,
            caption="B2 remains visible for legacy reference and limitations. It is not the promoted primary result, but the figures are still shown directly for quick comparison.",
            limit=2 if export_mode else (None if ui_state["advanced"] else 4),
            columns_per_row=1 if export_mode else 2,
            export_mode=export_mode,
            overlay_label="Click to enlarge",
        )

    def _broader_support() -> None:
        render_status_callout("Support-only rule", support_status.panel_text, "info")
        render_figure_gallery(
            support_figures,
            title=support_status.panel_label,
            caption="B3 remains broader support / appendix context only and should not be presented as the main validation row, but the gallery keeps the packaged figures visible without a selector.",
            limit=2 if export_mode else (None if ui_state["advanced"] else 4),
            columns_per_row=1 if export_mode else 2,
            export_mode=export_mode,
            overlay_label="Click to enlarge",
        )

    def _trajectory_context() -> None:
        render_figure_gallery(
            trajectory_figures,
            title=trajectory_status.panel_label,
            caption="These figures give transport context from stored outputs only. Panel mode now keeps them as a direct gallery while the surrounding notes stay concise.",
            limit=2 if export_mode else (None if ui_state["advanced"] else 4),
            columns_per_row=1 if export_mode else 2,
            export_mode=export_mode,
            overlay_label="Click to enlarge",
        )

    def _tables_and_notes() -> None:
        render_table(
            "B1 FSS by window",
            state["mindoro_b1_fss"],
            download_name="march13_14_reinit_fss_by_window.csv",
            caption="Curated FSS table from the primary March13-14 package.",
            height=220,
            export_mode=export_mode,
        )
        render_table(
            "Comparator summary",
            state["mindoro_comparator_summary"],
            download_name="march13_14_reinit_crossmodel_summary.csv",
            caption="Curated summary for the March14 comparator-only subgroup.",
            height=220,
            export_mode=export_mode,
        )
        render_markdown_block("Mindoro B1 final-package note", state["mindoro_final_readme"], collapsed=True, export_mode=export_mode)

    render_section_stack(
        [
            ("B1 primary package", _primary_package),
            ("A comparator support", _comparator_support),
            ("B2 legacy reference", _legacy_reference),
            ("B3 broader support", _broader_support),
            ("Trajectory context", _trajectory_context),
            ("Tables and notes", _tables_and_notes),
        ],
        export_mode=export_mode,
    )
