"""DWH transfer-validation page."""

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
    deterministic_status = get_artifact_status("dwh_deterministic_transfer")
    ensemble_status = get_artifact_status("dwh_ensemble_transfer")
    comparator_status = get_artifact_status("dwh_crossmodel_comparator")
    trajectory_status = get_artifact_status("dwh_trajectory_context")

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

    deterministic_figures = figure_subset(
        ui_state["visual_layer"],
        case_id="CASE_DWH_RETRO_2010_72H",
        status_keys=[deterministic_status.key],
    )
    ensemble_figures = figure_subset(
        ui_state["visual_layer"],
        case_id="CASE_DWH_RETRO_2010_72H",
        status_keys=[ensemble_status.key],
    )
    comparator_figures = figure_subset(
        ui_state["visual_layer"],
        case_id="CASE_DWH_RETRO_2010_72H",
        status_keys=[comparator_status.key],
    )
    trajectory_figures = figure_subset(
        ui_state["visual_layer"],
        case_id="CASE_DWH_RETRO_2010_72H",
        status_keys=[trajectory_status.key],
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
            deterministic_figures,
            title=deterministic_status.panel_label,
            caption="Per-date overlays, event-corridor views, or stored deterministic path context are the most useful first-stop visuals for the DWH case.",
            limit=None if ui_state["advanced"] else 4,
        )

    with tabs[1]:
        render_figure_cards(
            ensemble_figures,
            title=ensemble_status.panel_label,
            caption="These figures explain how deterministic, p50, and p90 differ without asking the panel to parse raw threshold tables.",
            limit=None if ui_state["advanced"] else 4,
        )

    with tabs[2]:
        render_status_callout(
            "Comparator framing",
            comparator_status.panel_text,
            "info",
        )
        render_figure_cards(
            comparator_figures,
            title=comparator_status.panel_label,
            caption="Panel-friendly mode emphasizes the direct comparison board first.",
            limit=None if ui_state["advanced"] else 4,
        )

    with tabs[3]:
        render_figure_cards(
            trajectory_figures,
            title=trajectory_status.panel_label,
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
