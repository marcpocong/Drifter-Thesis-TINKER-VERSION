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
from ui.pages.common import render_figure_cards, render_markdown_block, render_page_intro, render_status_callout, render_table


def _mindoro_final_subset(df, artifact_groups: set[str]) -> object:
    if df.empty:
        return df
    if "artifact_group" not in df.columns:
        return df
    return df.loc[df.get("artifact_group", "").astype(str).isin(sorted(artifact_groups))].reset_index(drop=True)


def render(state: dict, ui_state: dict) -> None:
    primary_status = get_artifact_status("mindoro_primary_validation")
    legacy_status = get_artifact_status("mindoro_legacy_march6")
    support_status = get_artifact_status("mindoro_legacy_support")
    trajectory_status = get_artifact_status("mindoro_trajectory_context")

    render_page_intro(
        "Mindoro B1 Primary Validation",
        "This page leads with the promoted March 13 -> March 14 B1 family, keeps the shared-imagery caveat visible, and treats all other Mindoro rows as comparator or support material rather than co-primary evidence.",
        badge="Mindoro B1 | primary validation row",
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

    tabs = st.tabs(
        [
            "B1 primary package",
            "A comparator support",
            "B2 legacy reference",
            "B3 broader support",
            "Trajectory context",
            "Tables and notes",
        ]
    )

    with tabs[0]:
        render_figure_cards(
            primary_figures,
            title="B1 curated primary-validation figures",
            caption="These figures come from the curated Phase 3B March13-14 final package and should be used first for thesis-facing Mindoro discussion.",
            limit=None if ui_state["advanced"] else 5,
            compact_selector=not ui_state["advanced"],
            selector_key="mindoro_primary_figures",
        )
        render_table(
            "B1 summary",
            state["mindoro_b1_summary"],
            download_name="march13_14_reinit_summary.csv",
            caption="Curated B1 summary table from the final March13-14 package.",
            height=250,
        )

    with tabs[1]:
        render_status_callout(
            "Comparator-only rule",
            "Track A is attached to B1 as same-case comparator support. PyGNOME is comparator-only and never truth.",
            "warning",
        )
        render_figure_cards(
            comparator_figures,
            title="A comparator-only figures",
            caption="These figures come from the curated comparator subgroup under the final March13-14 package and remain separate from the primary B1 claim.",
            limit=None if ui_state["advanced"] else 4,
        )
        render_table(
            "Comparator model ranking",
            state["mindoro_comparator_ranking"],
            download_name="march13_14_reinit_crossmodel_model_ranking.csv",
            caption="Curated cross-model ranking table for the same-case March 14 comparator lane.",
            height=240,
        )

    with tabs[2]:
        render_status_callout("Legacy reference rule", "B2 remains visible as a legacy reference and limitations row, but it is not the main Mindoro validation claim.", "warning")
        render_figure_cards(
            legacy_figures,
            title=legacy_status.panel_label,
            caption="B2 remains visible for legacy reference and limitations. It is not the promoted primary result.",
            limit=None if ui_state["advanced"] else 4,
            compact_selector=not ui_state["advanced"],
            selector_key="mindoro_legacy_figures",
        )

    with tabs[3]:
        render_status_callout("Support-only rule", support_status.panel_text, "info")
        render_figure_cards(
            support_figures,
            title=support_status.panel_label,
            caption="B3 remains broader support / appendix context only and should not be presented as the main validation row.",
            limit=None if ui_state["advanced"] else 4,
            compact_selector=not ui_state["advanced"],
            selector_key="mindoro_support_figures",
        )

    with tabs[4]:
        render_figure_cards(
            trajectory_figures,
            title=trajectory_status.panel_label,
            caption="These figures give transport context from stored outputs only. Publication mode stays compact; advanced mode can open lower-level layers.",
            limit=None if ui_state["advanced"] else 4,
            compact_selector=not ui_state["advanced"],
            selector_key="mindoro_trajectory_figures",
        )

    with tabs[5]:
        render_table(
            "B1 FSS by window",
            state["mindoro_b1_fss"],
            download_name="march13_14_reinit_fss_by_window.csv",
            caption="Curated FSS table from the primary March13-14 package.",
            height=220,
        )
        render_table(
            "Comparator summary",
            state["mindoro_comparator_summary"],
            download_name="march13_14_reinit_crossmodel_summary.csv",
            caption="Curated summary for the March14 comparator-only subgroup.",
            height=220,
        )
        render_markdown_block("Mindoro B1 final-package note", state["mindoro_final_readme"], collapsed=True)
