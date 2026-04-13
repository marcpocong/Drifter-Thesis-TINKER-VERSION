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

import streamlit as st

from ui.pages.common import (
    render_figure_cards,
    render_markdown_block,
    render_package_cards,
    render_page_intro,
    render_status_callout,
    render_study_structure_cards,
)


def render(state: dict, ui_state: dict) -> None:
    render_page_intro(
        "Home / Overview",
        "This dashboard is a read-only thesis launch surface over the current curated outputs. It leads with Mindoro B1 primary validation, keeps comparator and support lanes explicit, and surfaces packaged evidence before raw case folders.",
        badge="Read-only dashboard | curated final packages first",
    )

    render_status_callout(
        "Primary claim",
        "Mindoro B1 is the only primary Mindoro validation row. Track A is comparator-only, B2 is the legacy reference row, B3 is broader support, DWH is a separate transfer-validation lane, and prototype_2016 is support-only legacy material.",
        "info",
    )
    render_status_callout(
        "Phase 1 provenance note",
        "Mindoro B1 inherits the recipe selected by the separate focused Phase 1 drifter-based provenance rerun. The Phase 3B B1 case itself does not directly ingest drifters.",
        "info",
    )
    render_status_callout(
        "Phase 4 note",
        "Mindoro Phase 4 is currently an OpenDrift/OpenOil scenario layer only. The only packaged Phase 4 PyGNOME comparator pilot is the budget-only support pilot inside the legacy 2016 package.",
        "warning",
    )

    recommended = state["curated_recommended_figures"]
    st.subheader("Study Structure")
    st.caption("These cards explain the study in thesis language first. Each one links to the page that presents the current curated evidence for that part of the workflow.")
    render_study_structure_cards(
        [
            {
                "title": "Phase 1 Recipe Selection",
                "classification": "Study setup",
                "body": "Historical drifter segments were used to compare forcing recipes and select the Mindoro recipe before the main validation row was discussed.",
                "note": "Main provenance layer for the B1 recipe choice.",
                "page_label": "Phase 1 Recipe Selection",
            },
            {
                "title": "Mindoro Primary Validation",
                "classification": "Main thesis result",
                "body": "The March 13 to March 14 B1 row is the main Mindoro observation-based validation claim.",
                "note": "This is the main Mindoro evidence row.",
                "page_label": "Mindoro B1 Primary Validation",
            },
            {
                "title": "Mindoro Comparator",
                "classification": "Support comparison",
                "body": "Track A compares OpenDrift and PyGNOME on the same March 14 target, but it remains comparator-only.",
                "note": "PyGNOME is never shown as truth here.",
                "page_label": "Mindoro Cross-Model Comparator",
            },
            {
                "title": "DWH Transfer Validation",
                "classification": "Main discussion result",
                "body": "DWH is a separate transfer-validation lane with C1 deterministic, C2 ensemble extension, and C3 comparator-only semantics.",
                "note": "No drifter baseline is used for DWH.",
                "page_label": "DWH Phase 3C Transfer Validation",
            },
            {
                "title": "Phase 4 Oil-Type and Shoreline",
                "classification": "Technical context",
                "body": "Mindoro Phase 4 provides the current OpenDrift/OpenOil scenario context for oil-type and shoreline interpretation.",
                "note": "No matched Mindoro Phase 4 PyGNOME package is currently shown.",
                "page_label": "Phase 4 Oil-Type and Shoreline Context",
            },
            {
                "title": "Legacy 2016 Support",
                "classification": "Legacy support",
                "body": "The 2016 package preserves the legacy support flow and now includes a budget-only deterministic PyGNOME Phase 4 pilot.",
                "note": "This lane remains support-only and is not main validation evidence.",
                "page_label": "Legacy 2016 Support Package",
            },
        ],
        columns_per_row=3,
    )

    st.subheader("Quick Links")
    st.caption("These cards open the current curated package roots rather than raw case folders.")
    render_package_cards(state.get("curated_package_roots", []), columns_per_row=2)

    render_figure_cards(
        recommended,
        title="Featured publication figures",
        caption="Panel mode starts with one featured publication figure at a time so the page stays readable during live discussion. Advanced mode can still open larger galleries.",
        limit=8,
        columns_per_row=2,
        compact_selector=not ui_state["advanced"],
        selector_key="home_featured_figure",
    )

    if ui_state["advanced"]:
        render_status_callout(
            "Advanced note",
            "Advanced mode keeps the lower-level reproducibility notes available, but panel mode deliberately avoids internal governance counters and status jargon.",
            "info",
        )
        render_markdown_block("Final reproducibility summary", state["final_reproducibility_summary"], collapsed=True)
        render_markdown_block("Publication talking points", state["publication_talking_points"], collapsed=True)
        render_markdown_block("Publication captions", state["publication_captions"], collapsed=True)
