"""Phase 4 oil-type and shoreline page."""

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
from ui.plots import phase4_budget_summary_figure


def render(state: dict, ui_state: dict) -> None:
    oil_status = get_artifact_status("mindoro_phase4_oil_budget")
    shoreline_status = get_artifact_status("mindoro_phase4_shoreline")

    render_page_intro(
        "Phase 4 Oil-Type and Shoreline Context",
        "This page presents the current Mindoro Phase 4 interpretation layer as it exists now: OpenDrift/OpenOil scenario context for oil-type and shoreline interpretation. It does not present a matched Mindoro Phase 4 PyGNOME package because no such package is currently stored.",
        badge="Mindoro Phase 4 context | OpenDrift/OpenOil-only",
    )

    render_status_callout(
        "Current scope",
        "No matched PyGNOME Phase 4 comparison is packaged yet. Current Mindoro Phase 4 results are OpenDrift/OpenOil scenario outputs only.",
        "warning",
    )

    render_status_callout(
        "Follow-up note",
        "The fixed base medium-heavy proxy remains flagged for follow-up because of the recorded mass-balance tolerance exceedance.",
        "warning",
    )
    render_status_callout(
        "Legacy 2016 note",
        "The only packaged Phase 4 PyGNOME pilot in the repo today is the budget-only deterministic pilot inside the Legacy 2016 Support page. It is support-only and does not change the Mindoro Phase 4 decision.",
        "info",
    )

    figures = figure_subset(
        ui_state["visual_layer"],
        case_id="CASE_MINDORO_RETRO_2023",
        status_keys=[oil_status.key, shoreline_status.key],
    )

    st.pyplot(phase4_budget_summary_figure(state["phase4_budget_summary"]), width="stretch")

    tabs = st.tabs(["Publication figures", "Budget tables", "Shoreline tables"])

    with tabs[0]:
        render_figure_cards(
            figures,
            title="Mindoro Phase 4 figures",
            caption="Panel-friendly mode leads with the oil-budget and shoreline boards, then exposes scenario-specific singles as needed.",
            limit=None if ui_state["advanced"] else 5,
            compact_selector=not ui_state["advanced"],
            selector_key="mindoro_phase4_figures",
        )

    with tabs[1]:
        render_table(
            "Phase 4 oil-budget summary",
            state["phase4_budget_summary"],
            download_name="phase4_oil_budget_summary.csv",
            caption="Stored per-scenario summary table for the Mindoro Phase 4 run.",
            height=250,
        )
        render_table(
            "Phase 4 oil-type comparison",
            state["phase4_oiltype_comparison"],
            download_name="phase4_oiltype_comparison.csv",
            caption="Delta-versus-anchor scenario comparison derived from the stored Phase 4 bundle.",
            height=220,
        )

    with tabs[2]:
        render_table(
            "Phase 4 shoreline arrival summary",
            state["phase4_shoreline_arrival"],
            download_name="phase4_shoreline_arrival.csv",
            caption="Stored first-arrival summary per scenario.",
            height=220,
        )
        render_table(
            "Phase 4 shoreline segment impacts",
            state["phase4_shoreline_segments"],
            download_name="phase4_shoreline_segments.csv",
            caption="Stored shoreline segment impact table; advanced mode can be used to inspect the full per-segment rows.",
            height=300,
            max_rows=None if ui_state["advanced"] else 25,
        )
