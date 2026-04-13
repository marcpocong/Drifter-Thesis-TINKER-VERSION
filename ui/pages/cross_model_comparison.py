"""Mindoro cross-model comparator page."""

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
from ui.pages.common import (
    render_export_note,
    render_figure_gallery,
    render_markdown_block,
    render_page_intro,
    render_status_callout,
    render_table,
)


def render(state: dict, ui_state: dict) -> None:
    export_mode = bool(ui_state.get("export_mode"))
    comparator_status = get_artifact_status("mindoro_crossmodel_comparator")
    registry = state["mindoro_final_registry"]
    figures = registry.loc[
        registry.get("artifact_group", "").astype(str).eq("publication/comparator_pygnome")
    ].reset_index(drop=True)

    render_page_intro(
        "Mindoro Cross-Model Comparator",
        "This page is the dedicated home for Track A. It stays comparator-only, uses the same March 14 target as B1, and never lets PyGNOME read like truth or a co-primary validation row.",
        badge="Mindoro A | comparator-only",
    )

    if export_mode:
        render_export_note(
            [
                "Export mode keeps the comparator page concise so the PDF reads as cross-model context rather than a second validation claim.",
                "The figures below remain comparator-only and should be read alongside the main B1 page, not instead of it.",
            ]
        )

    render_status_callout("Comparator-only rule", comparator_status.panel_text, "warning")
    render_status_callout(
        "B1 relationship",
        "Track A is attached to the B1 package as supporting cross-model context. It helps compare model behavior on the same case, but it does not replace the main OpenDrift-versus-observation claim.",
        "info",
    )
    render_status_callout(
        "Phase 4 honesty note",
        "This page stays in Phase 3 spatial-comparator territory only. No matched Mindoro Phase 4 PyGNOME package is shown here; the current Mindoro Phase 4 layer remains OpenDrift/OpenOil scenario context only.",
        "warning",
    )

    render_figure_gallery(
        figures,
        title=comparator_status.panel_label,
        caption="These figures come from the curated comparator subgroup in the March13-14 final package. Click any figure to enlarge it and read the fuller context there.",
        limit=2 if export_mode else (None if ui_state["advanced"] else 4),
        columns_per_row=1 if export_mode else 2,
        export_mode=export_mode,
        overlay_label="Click to enlarge",
    )

    if export_mode:
        render_table(
            "Comparator ranking",
            state["mindoro_comparator_ranking"],
            download_name="march13_14_reinit_crossmodel_model_ranking.csv",
            caption="Curated ranking table for the same-case March 14 comparator lane.",
            height=260,
            export_mode=export_mode,
        )
        render_table(
            "Comparator summary",
            state["mindoro_comparator_summary"],
            download_name="march13_14_reinit_crossmodel_summary.csv",
            caption="Curated summary table for the Mindoro comparator subgroup.",
            height=260,
            export_mode=export_mode,
        )
    else:
        left, right = st.columns(2)
        with left:
            render_table(
                "Comparator ranking",
                state["mindoro_comparator_ranking"],
                download_name="march13_14_reinit_crossmodel_model_ranking.csv",
                caption="Curated ranking table for the same-case March 14 comparator lane.",
                height=260,
            )
        with right:
            render_table(
                "Comparator summary",
                state["mindoro_comparator_summary"],
                download_name="march13_14_reinit_crossmodel_summary.csv",
                caption="Curated summary table for the Mindoro comparator subgroup.",
                height=260,
            )

    if ui_state["advanced"] or export_mode:
        render_markdown_block("Mindoro final-package note", state["mindoro_final_readme"], collapsed=True, export_mode=export_mode)
