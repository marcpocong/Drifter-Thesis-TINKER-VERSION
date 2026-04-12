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

from ui.data_access import figure_subset
from ui.pages.common import filter_family, render_figure_cards, render_page_intro, render_status_callout, render_table
from ui.plots import phase4_budget_summary_figure


def render(state: dict, ui_state: dict) -> None:
    render_page_intro(
        "Phase 4 Oil-Type & Shoreline",
        "This page presents the current Mindoro Phase 4 interpretation layer as it exists now: scientifically reportable on the current transport framework, but still inherited-provisional from the upstream Phase 1 and Phase 2 freeze story.",
        badge="Mindoro Phase 4 | OpenDrift/OpenOil-only",
    )

    render_status_callout(
        "Current scope",
        "These figures and tables are OpenDrift/OpenOil outputs only. They are suitable for current interpretation, but they are not a cross-model Phase 4 comparison.",
        "info",
    )

    render_status_callout(
        "Follow-up note",
        "The fixed base medium-heavy proxy remains flagged for follow-up because of the recorded mass-balance tolerance exceedance.",
        "warning",
    )

    figures = figure_subset(
        ui_state["visual_layer"],
        case_id="CASE_MINDORO_RETRO_2023",
        family_codes=["E"] if ui_state["visual_layer"] == "publication" else None,
    )

    st.pyplot(phase4_budget_summary_figure(state["phase4_budget_summary"]), width="stretch")

    tabs = st.tabs(["Publication figures", "Budget tables", "Shoreline tables"])

    with tabs[0]:
        render_figure_cards(
            filter_family(figures, "E"),
            title="Mindoro Phase 4 figures",
            caption="Panel-friendly mode leads with the oil-budget board and the shoreline-impact board, then exposes scenario-specific single figures as needed.",
            limit=None if ui_state["advanced"] else 5,
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
