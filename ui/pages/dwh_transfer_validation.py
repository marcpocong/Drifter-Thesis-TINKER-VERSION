"""DWH Phase 3C transfer-validation page."""

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
from ui.pages.common import render_figure_cards, render_markdown_block, render_page_intro, render_status_callout, render_table


def _dwh_subset(df, artifact_groups: set[str]) -> object:
    if df.empty:
        return df
    if "artifact_group" not in df.columns:
        return df
    return df.loc[df.get("artifact_group", "").astype(str).isin(sorted(artifact_groups))].reset_index(drop=True)


def render(state: dict, ui_state: dict) -> None:
    deterministic_status = get_artifact_status("dwh_deterministic_transfer")
    ensemble_status = get_artifact_status("dwh_ensemble_transfer")
    comparator_status = get_artifact_status("dwh_crossmodel_comparator")
    truth_status = get_artifact_status("dwh_observation_truth_context")

    render_page_intro(
        "DWH Phase 3C Transfer Validation",
        "This page treats DWH as a separate frozen external transfer-validation lane. It keeps C1 deterministic, C2 ensemble extension, and C3 comparator-only semantics explicit, with public observation-derived masks as truth and no drifter baseline.",
        badge="DWH Phase 3C | separate external transfer-validation lane",
    )

    render_status_callout(
        "Truth rule",
        "DWH truth comes from public observation-derived date-composite masks only. No drifter baseline is used here.",
        "info",
    )
    render_status_callout(
        "Forcing stack",
        "The frozen DWH stack is HYCOM GOFS 3.1 currents + ERA5 winds + CMEMS wave/Stokes.",
        "info",
    )
    render_status_callout(
        "Comparator rule",
        "PyGNOME is comparator-only for DWH. It is never truth and does not replace the deterministic OpenDrift transfer-validation story.",
        "warning",
    )

    registry = state["dwh_final_registry"]
    truth_figures = _dwh_subset(registry, {"publication/observations"})
    deterministic_figures = _dwh_subset(registry, {"publication/opendrift_deterministic"})
    ensemble_figures = _dwh_subset(registry, {"publication/opendrift_ensemble"})
    comparator_figures = _dwh_subset(registry, {"publication/comparator_pygnome"})

    tabs = st.tabs(
        [
            "C1 deterministic baseline",
            "C2 ensemble extension",
            "C3 comparator-only",
            "Observation truth context",
            "Tables and notes",
        ]
    )

    with tabs[0]:
        render_status_callout("C1 framing", deterministic_status.panel_text, "info")
        render_figure_cards(
            deterministic_figures,
            title=deterministic_status.panel_label,
            caption="These curated figures are the clean baseline transfer-validation visuals for DWH.",
            limit=None if ui_state["advanced"] else 5,
            compact_selector=not ui_state["advanced"],
            selector_key="dwh_c1_figures",
        )
        render_table(
            "C1 deterministic summary",
            state["dwh_deterministic_summary_final"],
            download_name="phase3c_summary.csv",
            caption="Curated DWH deterministic summary from the Phase 3C final package.",
            height=240,
        )

    with tabs[1]:
        render_status_callout("C2 framing", ensemble_status.panel_text, "info")
        render_figure_cards(
            ensemble_figures,
            title=ensemble_status.panel_label,
            caption="These curated figures show the ensemble extension and deterministic-vs-ensemble comparison. P50 is preferred; p90 remains support/comparison only.",
            limit=None if ui_state["advanced"] else 4,
            compact_selector=not ui_state["advanced"],
            selector_key="dwh_c2_figures",
        )
        render_table(
            "C2 ensemble summary",
            state["dwh_ensemble_summary_final"],
            download_name="phase3c_ensemble_summary.csv",
            caption="Curated DWH ensemble summary from the Phase 3C final package.",
            height=240,
        )

    with tabs[2]:
        render_status_callout("C3 framing", comparator_status.panel_text, "warning")
        render_figure_cards(
            comparator_figures,
            title=comparator_status.panel_label,
            caption="These curated figures keep PyGNOME in its comparator-only role on the DWH lane.",
            limit=None if ui_state["advanced"] else 4,
            compact_selector=not ui_state["advanced"],
            selector_key="dwh_c3_figures",
        )
        render_table(
            "C3 comparator summary",
            state["dwh_comparator_summary_final"],
            download_name="phase3c_dwh_pygnome_summary.csv",
            caption="Curated comparator summary from the DWH Phase 3C final package.",
            height=240,
        )

    with tabs[3]:
        render_status_callout("Observation context", truth_status.panel_text, "info")
        render_figure_cards(
            truth_figures,
            title=truth_status.panel_label,
            caption="These observation-context figures establish the public daily masks and event corridor before any model comparison is discussed.",
            limit=None if ui_state["advanced"] else 4,
            compact_selector=not ui_state["advanced"],
            selector_key="dwh_truth_figures",
        )

    with tabs[4]:
        render_table(
            "Comparator results table",
            state["dwh_all_results_final"],
            download_name="phase3c_dwh_all_results_table.csv",
            caption="Curated DWH all-results table for advanced inspection.",
            height=260,
        )
        render_markdown_block("DWH final-package note", state["dwh_final_readme"], collapsed=True)
