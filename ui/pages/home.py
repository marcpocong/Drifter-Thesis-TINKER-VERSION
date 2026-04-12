"""Home / Overview page."""

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

from ui.pages.common import (
    render_figure_cards,
    render_markdown_block,
    render_metric_row,
    render_page_intro,
    render_status_callout,
    render_table,
)
from ui.plots import comparability_status_figure, phase_status_overview_figure


def _reportable_counts(phase_status: pd.DataFrame) -> tuple[int, int]:
    if phase_status.empty:
        return 0, 0
    reportable = int(pd.to_numeric(phase_status["reportable_now"], errors="coerce").fillna(False).astype(bool).sum())
    provisional = int(pd.to_numeric(phase_status["inherited_provisional"], errors="coerce").fillna(False).astype(bool).sum())
    return reportable, provisional


def render(state: dict, ui_state: dict) -> None:
    render_page_intro(
        "Home / Overview",
        "This read-only dashboard summarizes the current reportable thesis tracks, surfaces the publication-grade figure package first, and keeps the current Phase 4 cross-model limits explicit.",
        badge="Phase 5 deliverable layer | read-only",
    )

    phase_status = state["phase_status"]
    recommended = state["curated_recommended_figures"]
    publication_manifest = state["publication_manifest"]
    matrix = state["phase4_crossmodel_matrix"]

    reportable_count, provisional_count = _reportable_counts(phase_status)
    recommended_count = int(len(recommended))
    figure_count = int(len(state["publication_registry"]))

    render_metric_row(
        [
            ("Reportable tracks", str(reportable_count)),
            ("Inherited-provisional tracks", str(provisional_count)),
            ("Recommended defense figures", str(recommended_count)),
            ("Publication figures indexed", str(figure_count)),
        ]
    )

    left, right = st.columns([1.4, 1.0])
    with left:
        st.pyplot(phase_status_overview_figure(phase_status), width="stretch")
    with right:
        st.pyplot(comparability_status_figure(matrix), width="stretch")
        render_status_callout(
            "Phase 4 cross-model status",
            str(publication_manifest.get("phase4_crossmodel_comparison_status", "deferred")).replace("_", " "),
            "warning",
        )
        if publication_manifest.get("phase4_deferred_comparison_note_figure_produced"):
            render_status_callout(
                "Deferred note figure",
                "The publication package includes an explicit Phase 4 deferred-comparison note figure.",
                "info",
            )

    st.subheader("Current honesty status")
    if phase_status.empty:
        st.info("Phase-status registry is not available.")
    else:
        show_columns = [
            "phase_id",
            "track_id",
            "readiness_status",
            "reportable_now",
            "inherited_provisional",
            "summary",
            "main_blocker",
        ]
        render_table(
            "Phase-status registry",
            phase_status.loc[:, [column for column in show_columns if column in phase_status.columns]],
            download_name="final_phase_status_registry.csv",
            caption="This summary is pulled directly from the synced Phase 5 reproducibility package.",
            height=320,
        )

    render_figure_cards(
        recommended,
        title="Recommended figures for the defense panel",
        caption="Panel-friendly mode leads with the publication-grade boards and close-up figures that are already marked as main-defense recommendations.",
        limit=7,
        columns_per_row=2,
    )

    if ui_state["advanced"]:
        render_markdown_block("Publication talking points", state["publication_talking_points"], collapsed=True)
        render_markdown_block("Publication captions", state["publication_captions"], collapsed=True)
