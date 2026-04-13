"""Mindoro validation page."""

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
from ui.pages.common import render_figure_cards, render_page_intro, render_status_callout, render_table


def render(state: dict, ui_state: dict) -> None:
    primary_status = get_artifact_status("mindoro_primary_validation")
    comparator_status = get_artifact_status("mindoro_crossmodel_comparator")
    legacy_status = get_artifact_status("mindoro_legacy_march6")
    support_status = get_artifact_status("mindoro_legacy_support")
    trajectory_status = get_artifact_status("mindoro_trajectory_context")

    render_page_intro(
        "Mindoro Validation",
        "This page leads with the promoted March 13 -> March 14 primary validation, keeps the March 14 same-case cross-model comparator support track explicit, retains March 6 as an honesty reference, and uses trajectory figures as transport context.",
        badge="Mindoro | promoted primary validation first",
    )

    render_status_callout(
        "Interpretation guardrail",
        primary_status.panel_text,
        "info",
    )
    render_status_callout(
        "Legacy reference",
        legacy_status.panel_text,
        "warning",
    )

    primary_figures = figure_subset(
        ui_state["visual_layer"],
        case_id="CASE_MINDORO_RETRO_2023",
        status_keys=[primary_status.key],
    )
    comparator_figures = figure_subset(
        ui_state["visual_layer"],
        case_id="CASE_MINDORO_RETRO_2023",
        status_keys=[comparator_status.key],
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
            "Primary Validation",
            "Comparator Support",
            "Legacy March 6",
            "Broader Support",
            "Trajectories",
            "Tables",
        ]
    )

    with tabs[0]:
        render_figure_cards(
            primary_figures,
            title=primary_status.panel_label,
            caption="These figures keep the March 13 seed, March 14 target, and promoted OpenDrift reinit result together without losing the shared-imagery provenance note.",
            limit=None if ui_state["advanced"] else 4,
        )

    with tabs[1]:
        render_status_callout(
            "Comparator framing",
            comparator_status.panel_text,
            "info",
        )
        render_figure_cards(
            comparator_figures,
            title=comparator_status.panel_label,
            caption="These figures answer the OpenDrift-versus-PyGNOME question on the promoted March 14 target without treating PyGNOME as truth or letting Track A drift into the main validation claim.",
            limit=None if ui_state["advanced"] else 4,
        )

    with tabs[2]:
        render_figure_cards(
            legacy_figures,
            title=legacy_status.panel_label,
            caption="These figures preserve the March 6 sparse-reference record for honesty and limitations rather than presenting it as the main result.",
            limit=None if ui_state["advanced"] else 4,
        )

    with tabs[3]:
        render_status_callout(
            "Support-only framing",
            support_status.panel_text,
            "info",
        )
        render_figure_cards(
            support_figures,
            title=support_status.panel_label,
            caption="These figures keep the March 3-6 broader-support context visible without letting it drift into primary-result language.",
            limit=None if ui_state["advanced"] else 4,
        )

    with tabs[4]:
        render_figure_cards(
            trajectory_figures,
            title=trajectory_status.panel_label,
            caption="These figures provide transport context from stored deterministic, ensemble, and corridor products before the panel gets into score tables.",
            limit=None if ui_state["advanced"] else 4,
        )

    with tabs[5]:
        render_status_callout(
            "Broader-support record",
            support_status.panel_text,
            "info",
        )
        render_table(
            "Mindoro Phase 3B summary table",
            state["mindoro_phase3b_summary"],
            download_name="mindoro_phase3b_summary.csv",
            caption="This table comes from the stored Mindoro Phase 3B summary and keeps the promoted and legacy machine-readable slices visible.",
            height=310,
        )
        render_table(
            "Mindoro comparator ranking table",
            state["mindoro_model_ranking"],
            download_name="mindoro_model_ranking.csv",
            caption="Stored March 14 comparator ranking table for the promoted cross-model lane.",
            height=260,
        )
