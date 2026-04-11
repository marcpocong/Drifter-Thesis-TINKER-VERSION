"""Mindoro validation page."""

from __future__ import annotations

import streamlit as st

from ui.data_access import figure_subset
from ui.pages.common import filter_family, render_figure_cards, render_page_intro, render_status_callout, render_table


def render(state: dict, ui_state: dict) -> None:
    render_page_intro(
        "Mindoro Validation",
        "This page combines the strict March 6 stress test, the broader March 4-6 support corridor, the OpenDrift-versus-PyGNOME comparator boards, and trajectory figures for the main Mindoro case.",
        badge="Main case | publication layer first",
    )

    render_status_callout(
        "Interpretation guardrail",
        "Strict March 6 remains a hard sparse stress test. It is informative and reportable, but it should not be oversold as broad-support validation.",
        "warning",
    )

    family_codes = ["A", "B", "C", "D"]
    figures = figure_subset(
        ui_state["visual_layer"],
        case_id="CASE_MINDORO_RETRO_2023",
        family_codes=family_codes if ui_state["visual_layer"] == "publication" else None,
    )

    tabs = st.tabs(
        [
            "Strict March 6",
            "March 4-6 Support Corridor",
            "OpenDrift vs PyGNOME",
            "Trajectories",
            "Tables",
        ]
    )

    with tabs[0]:
        strict = filter_family(figures, "A")
        render_figure_cards(
            strict,
            title="Strict March 6 figure set",
            caption="The publication package includes board, locator, zoom, and forced close-up views so the tiny observed target remains readable.",
            limit=None if ui_state["advanced"] else 4,
        )

    with tabs[1]:
        support = filter_family(figures, "B")
        render_figure_cards(
            support,
            title="March 4-6 support corridor figure set",
            caption="This support track complements the strict March 6 case and is useful when the panel needs broader event context.",
            limit=None if ui_state["advanced"] else 4,
        )

    with tabs[2]:
        comparison = filter_family(figures, "C")
        render_status_callout(
            "Cross-model framing",
            "PyGNOME is shown here as a comparator, not as truth. These are Phase 3 comparison figures, not Phase 4 fate comparisons.",
            "info",
        )
        render_figure_cards(
            comparison,
            title="Mindoro OpenDrift vs PyGNOME figures",
            caption="Panel-friendly mode prioritizes the side-by-side board and the close-up overlay that make the comparator role explicit.",
            limit=None if ui_state["advanced"] else 4,
        )

    with tabs[3]:
        trajectories = filter_family(figures, "D")
        render_figure_cards(
            trajectories,
            title="Mindoro trajectory figures",
            caption="These figures favor deterministic paths, sampled ensemble members, centroid/corridor views, and PyGNOME trajectories where available.",
            limit=None if ui_state["advanced"] else 4,
        )

    with tabs[4]:
        render_table(
            "Mindoro Phase 3B summary table",
            state["mindoro_phase3b_summary"],
            download_name="mindoro_phase3b_summary.csv",
            caption="This table comes from the stored Mindoro Phase 3B summary and keeps the strict and support metrics machine-readable.",
            height=310,
        )
        render_table(
            "Mindoro comparator ranking table",
            state["mindoro_model_ranking"],
            download_name="mindoro_model_ranking.csv",
            caption="Model-ranking support from the stored PyGNOME public-comparison outputs.",
            height=260,
        )
