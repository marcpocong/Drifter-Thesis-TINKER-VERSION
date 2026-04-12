"""Phase 3 cross-model comparison page."""

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
    mindoro_status = get_artifact_status("mindoro_crossmodel_comparator")
    dwh_status = get_artifact_status("dwh_crossmodel_comparator")

    render_page_intro(
        "Cross-Model Comparison",
        "This page keeps the cross-model story focused on the existing Phase 3 comparator products. It does not pretend that Phase 4 fate and shoreline comparison already exists.",
        badge="Phase 3 comparator views only",
    )

    render_status_callout(
        "Phase 4 honesty note",
        "Phase 4 OpenDrift-versus-PyGNOME comparison is deferred. Use the dedicated Phase 4 Cross-Model Status page for the blocker memo and next steps.",
        "warning",
    )

    mindoro_figures = figure_subset(
        ui_state["visual_layer"],
        case_id="CASE_MINDORO_RETRO_2023",
        status_keys=[mindoro_status.key],
    )
    dwh_figures = figure_subset(
        ui_state["visual_layer"],
        case_id="CASE_DWH_RETRO_2010_72H",
        status_keys=[dwh_status.key],
    )

    tabs = st.tabs(["Mindoro", "DWH", "Comparison tables"])

    with tabs[0]:
        render_figure_cards(
            mindoro_figures,
            title=mindoro_status.panel_label,
            caption="These are the promoted March 14 comparator figures and should stay separate from any Phase 4 fate-comparison claim.",
            limit=None if ui_state["advanced"] else 4,
        )
        render_table(
            "Mindoro model ranking",
            state["mindoro_model_ranking"],
            download_name="mindoro_model_ranking.csv",
            caption="Stored model ranking table from the Mindoro public-comparison outputs.",
            height=260,
        )

    with tabs[1]:
        render_figure_cards(
            dwh_figures,
            title=dwh_status.panel_label,
            caption="These figures help the panel compare model behavior on the richer DWH case without treating PyGNOME as truth.",
            limit=None if ui_state["advanced"] else 4,
        )
        render_table(
            "DWH cross-model results",
            state["dwh_all_results"],
            download_name="dwh_all_results.csv",
            caption="Stored DWH OpenDrift-versus-PyGNOME results table.",
            height=280,
        )

    with tabs[2]:
        render_table(
            "Mindoro model ranking",
            state["mindoro_model_ranking"],
            download_name="mindoro_model_ranking.csv",
            height=240,
        )
        render_table(
            "DWH comparator results",
            state["dwh_all_results"],
            download_name="dwh_all_results.csv",
            height=260,
        )
