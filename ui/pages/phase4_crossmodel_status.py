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

from ui.data_access import figure_subset
from ui.pages.common import filter_family, render_figure_cards, render_markdown_block, render_page_intro, render_status_callout, render_table
from ui.plots import comparability_status_figure


def render(state: dict, ui_state: dict) -> None:
    render_page_intro(
        "Phase 4 Cross-Model Status",
        "This page makes the current Phase 4 cross-model status explicit. It shows the audit verdict, the per-quantity comparability matrix, and the minimal next-step plan without pretending the missing PyGNOME fate outputs already exist.",
        badge="Honesty page | no fake comparison figures",
    )

    verdict_text = state["phase4_crossmodel_verdict"]
    blockers_text = state["phase4_crossmodel_blockers"]
    next_steps_text = state["phase4_crossmodel_next_steps"]
    matrix = state["phase4_crossmodel_matrix"]

    render_status_callout(
        "Current verdict",
        "Deferred. No requested Phase 4 quantities are honestly comparable now.",
        "error",
    )
    render_status_callout(
        "Single biggest blocker",
        "The current Mindoro PyGNOME benchmark is transport-only with weathering_enabled=false, so it does not expose matched Phase 4 fate and shoreline semantics.",
        "warning",
    )

    st.pyplot(comparability_status_figure(matrix), width="stretch")

    deferred_note = filter_family(
        figure_subset(
            ui_state["visual_layer"],
            case_id="CASE_MINDORO_RETRO_2023",
            family_codes=["F"] if ui_state["visual_layer"] == "publication" else None,
        ),
        "F",
    )
    render_figure_cards(
        deferred_note,
        title="Phase 4 deferred-comparison note figure",
        caption="This figure is the recommended panel-facing way to explain why Phase 4 cross-model comparison is not shown yet.",
        limit=1,
        columns_per_row=1,
    )

    render_table(
        "Comparability matrix",
        matrix,
        download_name="phase4_crossmodel_comparability_matrix.csv",
        caption="All requested quantities are currently classified as not comparable honestly.",
        height=320,
    )

    render_markdown_block("Final verdict", verdict_text, collapsed=False)
    render_markdown_block("Blocker memo", blockers_text, collapsed=True)
    render_markdown_block("Minimal next steps", next_steps_text, collapsed=True)
