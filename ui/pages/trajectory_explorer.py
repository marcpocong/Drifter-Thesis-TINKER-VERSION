"""Trajectory explorer page."""

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

import pandas as pd
import streamlit as st

from ui.data_access import parse_source_paths, raster_summary, track_summary, trajectory_figures, vector_summary
from ui.pages.common import preview_artifact, render_figure_cards, render_page_intro, render_status_callout, render_table


def _summarize_source(path) -> dict:
    suffix = path.suffix.lower()
    if suffix == ".nc":
        return track_summary(str(path), "")
    if suffix in {".tif", ".tiff"}:
        return raster_summary(str(path), "")
    if suffix in {".gpkg", ".shp"}:
        return vector_summary(str(path), "")
    return {}


def render(state: dict, ui_state: dict) -> None:
    render_page_intro(
        "Trajectory Explorer",
        "This page keeps the trajectory view readable by favoring deterministic paths, sampled ensemble members, centroid or corridor views, and PyGNOME paths where available instead of plotting every particle by default. The dedicated legacy 2016 package has its own page and is not mixed into this main-case explorer.",
        badge="Read-only explorer | no particle flood",
    )

    render_status_callout(
        "Explorer rule",
        "Publication figures are the default. Switch to advanced mode only when you need the panel gallery, raw gallery, or lower-level source artifact inspection.",
        "info",
    )

    case_id = st.selectbox(
        "Case",
        options=["CASE_MINDORO_RETRO_2023", "CASE_DWH_RETRO_2010_72H"],
        index=0,
        key="trajectory_case_selector",
    )
    figures = trajectory_figures(ui_state["visual_layer"], case_id=case_id)

    render_figure_cards(
        figures,
        title="Trajectory figure set",
        caption="These are prebuilt figures from the selected layer. Publication mode stays compact; advanced mode can inspect more source detail.",
        limit=None if ui_state["advanced"] else 6,
        columns_per_row=2,
    )

    render_table(
        "Trajectory figure registry subset",
        figures,
        download_name=f"{case_id.lower()}_trajectory_registry.csv",
        caption="Machine-readable subset of trajectory-oriented figures available for the selected case and layer.",
        height=280,
        max_rows=None if ui_state["advanced"] else 20,
    )

    if ui_state["advanced"] and not figures.empty:
        figure_ids = figures["figure_id"].astype(str).tolist()
        selected_id = st.selectbox("Inspect a source-backed trajectory figure", options=figure_ids, key="trajectory_figure_inspector")
        row = figures.loc[figures["figure_id"].astype(str).eq(selected_id)].iloc[0]
        source_paths = parse_source_paths(row.get("source_paths"))
        if source_paths:
            source_labels = [path.name for path in source_paths]
            selected_label = st.selectbox("Source artifact", options=source_labels, key="trajectory_source_selector")
            selected_path = source_paths[source_labels.index(selected_label)]
            summary = _summarize_source(selected_path)
            if summary:
                st.json(summary)
            preview_artifact(str(selected_path))
        else:
            st.info("No source artifact paths were recorded for this selected figure.")
