"""DWH transfer-validation page."""

from __future__ import annotations

import streamlit as st

from ui.data_access import figure_subset
from ui.pages.common import filter_family, render_figure_cards, render_page_intro, render_status_callout, render_table


def render(state: dict, ui_state: dict) -> None:
    render_page_intro(
        "DWH Transfer Validation",
        "This page highlights the external rich-data transfer-validation success story, including deterministic overlays, deterministic-versus-ensemble figures, comparator boards, and DWH trajectory views.",
        badge="External case | reportable transfer validation",
    )

    render_status_callout(
        "Interpretation guardrail",
        "The DWH case is a strong external transfer-validation success, but it does not replace the Mindoro main-case story.",
        "info",
    )

    family_codes = ["G", "H", "I", "J"]
    figures = figure_subset(
        ui_state["visual_layer"],
        case_id="CASE_DWH_RETRO_2010_72H",
        family_codes=family_codes if ui_state["visual_layer"] == "publication" else None,
    )

    tabs = st.tabs(
        [
            "Deterministic",
            "Deterministic vs Ensemble",
            "OpenDrift vs PyGNOME",
            "Trajectories",
            "Tables",
        ]
    )

    with tabs[0]:
        render_figure_cards(
            filter_family(figures, "G"),
            title="DWH deterministic figures",
            caption="Per-date overlays plus the event-corridor board are the most useful first-stop visuals for the DWH case.",
            limit=None if ui_state["advanced"] else 4,
        )

    with tabs[1]:
        render_figure_cards(
            filter_family(figures, "H"),
            title="DWH deterministic versus ensemble figures",
            caption="These figures explain how deterministic, p50, and p90 differ without asking the panel to parse raw threshold tables.",
            limit=None if ui_state["advanced"] else 4,
        )

    with tabs[2]:
        render_status_callout(
            "Comparator framing",
            "PyGNOME remains a comparator here, not truth. The value of this page is the explicit side-by-side interpretation of model behavior.",
            "info",
        )
        render_figure_cards(
            filter_family(figures, "I"),
            title="DWH OpenDrift versus PyGNOME figures",
            caption="Panel-friendly mode emphasizes the direct comparison board first.",
            limit=None if ui_state["advanced"] else 4,
        )

    with tabs[3]:
        render_figure_cards(
            filter_family(figures, "J"),
            title="DWH trajectory figures",
            caption="Trajectory views focus on deterministic paths, sampled ensemble spread, and the PyGNOME comparator path where available.",
            limit=None if ui_state["advanced"] else 4,
        )

    with tabs[4]:
        render_table(
            "DWH deterministic summary",
            state["dwh_summary"],
            download_name="dwh_phase3c_summary.csv",
            caption="Stored DWH deterministic transfer-validation summary table.",
            height=320,
        )
        render_table(
            "DWH OpenDrift versus PyGNOME results",
            state["dwh_all_results"],
            download_name="dwh_all_results.csv",
            caption="Stored DWH comparator table used for advanced inspection.",
            height=280,
        )
