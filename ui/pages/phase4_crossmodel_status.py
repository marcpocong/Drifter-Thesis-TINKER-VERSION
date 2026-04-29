"""Mindoro oil-type/shoreline comparator availability page."""

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
from ui.evidence_contract import ROLE_ADVANCED
from ui.pages.common import (
    render_caveat_ribbon,
    render_figure_cards,
    render_key_takeaway,
    render_markdown_block,
    render_modern_hero,
    render_section_header,
    render_table,
)
from ui.plots import comparability_status_figure


def render(state: dict, ui_state: dict) -> None:
    export_mode = bool(ui_state.get("export_mode"))
    deferred_status = get_artifact_status("mindoro_phase4_deferred")

    render_modern_hero(
        "Oil-Type/Shoreline Comparator Availability",
        "Advanced status page explaining why no matched Mindoro PyGNOME fate-and-shoreline comparison is packaged yet.",
        badge=ROLE_ADVANCED,
        eyebrow="Advanced technical status",
        meta=["Deferred comparator", "Read-only audit", "No forced claim"],
        tone="advanced",
    )

    verdict_text = state["phase4_crossmodel_verdict"]
    blockers_text = state["phase4_crossmodel_blockers"]
    next_steps_text = state["phase4_crossmodel_next_steps"]
    matrix = state["phase4_crossmodel_matrix"]

    render_key_takeaway(
        "Current verdict",
        "No matched Mindoro PyGNOME fate-and-shoreline comparison is packaged yet.",
        tone="advanced",
        badge=ROLE_ADVANCED,
    )
    render_caveat_ribbon(
        "Single biggest blocker",
        deferred_status.provenance_label,
        tone="warning",
    )

    render_section_header(
        "Comparability Status",
        "The figure and matrix are technical audit support for the Mindoro oil-type/shoreline boundary, not a new validation claim.",
        badge=ROLE_ADVANCED,
    )
    st.pyplot(comparability_status_figure(matrix), width="stretch")

    deferred_note = figure_subset(
        ui_state["visual_layer"],
        case_id="CASE_MINDORO_RETRO_2023",
        status_keys=[deferred_status.key],
    )
    render_figure_cards(
        deferred_note,
        title="No-matched-PyGNOME note figure",
        caption="This figure is the recommended panel-facing way to explain why no matched Mindoro PyGNOME fate-and-shoreline comparison is packaged yet.",
        limit=1,
        columns_per_row=1,
    )

    render_table(
        "Comparability matrix",
        matrix,
        download_name="phase4_crossmodel_comparability_matrix.csv",
        caption="Each requested quantity currently resolves to no matched Mindoro PyGNOME fate-and-shoreline package yet.",
        height=320,
        export_mode=export_mode,
    )

    render_markdown_block("Final verdict", verdict_text, collapsed=False, export_mode=export_mode)
    render_markdown_block("Blocker memo", blockers_text, collapsed=True, export_mode=export_mode)
    render_markdown_block("Minimal next steps", next_steps_text, collapsed=True, export_mode=export_mode)
