"""Phase 4 cross-model status page."""

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
from ui.pages.common import render_figure_cards, render_markdown_block, render_page_intro, render_status_callout, render_table
from ui.plots import comparability_status_figure


def render(state: dict, ui_state: dict) -> None:
    deferred_status = get_artifact_status("mindoro_phase4_deferred")

    render_page_intro(
        "Phase 4 Cross-Model Status",
        "This page makes the current Mindoro Phase 4 comparator decision explicit. It shows why the repo does not yet package a matched PyGNOME Phase 4 comparison and keeps the current Phase 4 results clearly framed as OpenDrift/OpenOil scenario outputs only.",
        badge="Read-only honesty page | no forced Phase 4 comparator",
    )

    verdict_text = state["phase4_crossmodel_verdict"]
    blockers_text = state["phase4_crossmodel_blockers"]
    next_steps_text = state["phase4_crossmodel_next_steps"]
    matrix = state["phase4_crossmodel_matrix"]

    render_status_callout(
        "Current verdict",
        "No matched Phase 4 PyGNOME comparison is packaged yet.",
        "error",
    )
    render_status_callout(
        "Single biggest blocker",
        deferred_status.provenance_label,
        "warning",
    )

    st.pyplot(comparability_status_figure(matrix), width="stretch")

    deferred_note = figure_subset(
        ui_state["visual_layer"],
        case_id="CASE_MINDORO_RETRO_2023",
        status_keys=[deferred_status.key],
    )
    render_figure_cards(
        deferred_note,
        title="Phase 4 no-matched-PyGNOME note figure",
        caption="This figure is the recommended panel-facing way to explain why no matched Phase 4 PyGNOME comparison is packaged yet.",
        limit=1,
        columns_per_row=1,
    )

    render_table(
        "Comparability matrix",
        matrix,
        download_name="phase4_crossmodel_comparability_matrix.csv",
        caption="Each requested quantity currently resolves to no matched Phase 4 PyGNOME package yet.",
        height=320,
    )

    render_markdown_block("Final verdict", verdict_text, collapsed=False)
    render_markdown_block("Blocker memo", blockers_text, collapsed=True)
    render_markdown_block("Minimal next steps", next_steps_text, collapsed=True)
