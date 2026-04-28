"""Home / Overview page."""

from __future__ import annotations

import html
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
    render_badge_strip,
    render_export_note,
    render_figure_gallery,
    render_markdown_block,
    render_package_cards,
    render_page_intro,
    render_study_structure_cards,
)


def render(state: dict, ui_state: dict) -> None:
    export_mode = bool(ui_state.get("export_mode"))
    render_page_intro(
        "Home / Overview",
        "This read-only overview now follows the final paper and defense story instead of the full historical output trail. Start with workflow and provenance context, then move through Mindoro B1 primary validation, comparator support, DWH transfer-validation, and Phase 4 context before using archive or legacy lanes as secondary references.",
        badge="Thesis-facing | final paper / defense story",
    )

    if export_mode:
        render_export_note(
            [
                "Export mode converts the dashboard into a print-friendly snapshot of the current curated packages.",
                "This view hides the sidebar and interactive controls, keeps only the publication-first layer, and limits the page to a small registry-ordered set of thesis-facing featured figures for PDF export.",
            ]
        )

    recommended = state["home_featured_publication_figures"]
    render_badge_strip(["Thesis-facing", "Comparator support", "Archive only", "Legacy support"])
    st.caption("These surface badges separate main thesis evidence from support, archive, and legacy lanes.")

    guide_cards = [
        {
            "title": "Workflow / provenance context",
            "body": "Phase 1 explains how the focused drifter-based recipe check selected the transport recipe that Mindoro B1 inherits.",
        },
        {
            "title": "Mindoro B1 primary validation",
            "body": "Mindoro B1 is the main Mindoro validation claim. The March 13 -> March 14 R1 row is the only thesis-facing primary validation row.",
        },
        {
            "title": "Comparator support",
            "body": "Track A remains a same-case comparator-support lane. It helps explain cross-model behavior without becoming a second primary result.",
        },
        {
            "title": "DWH transfer-validation",
            "body": "DWH stays a separate external transfer-validation lane with its own truth context, deterministic baseline, ensemble extension, and comparator support.",
        },
        {
            "title": "Phase 4 support / context",
            "body": "Phase 4 remains context for oil-type and shoreline interpretation. Archive and legacy outputs stay available, but only as secondary lanes.",
        },
    ]

    st.subheader("How to read this dashboard")
    st.caption("Use these lanes in order for the shortest thesis-facing defense story.")
    guide_columns_per_row = 1 if export_mode else 3
    for start in range(0, len(guide_cards), guide_columns_per_row):
        visible_columns = min(guide_columns_per_row, len(guide_cards) - start)
        columns = st.columns(visible_columns)
        for column, card in zip(columns, guide_cards[start : start + guide_columns_per_row]):
            with column:
                with st.container(border=not export_mode):
                    st.markdown(
                        (
                            "<div class='home-guide-card'>"
                            f"<div class='home-guide-card__title'>{html.escape(card['title'])}</div>"
                            f"<div class='home-guide-card__body'>{html.escape(card['body'])}</div>"
                            "</div>"
                        ),
                        unsafe_allow_html=True,
                    )

    st.subheader("Primary thesis story")
    st.caption("These cards define the current thesis-facing sequence and keep the active evidence lanes in front.")
    render_study_structure_cards(
        [
            {
                "title": "Phase 1 Recipe Selection",
                "classification": "Thesis-facing",
                "body": "Historical drifter segments were used to compare forcing recipes and select the Mindoro recipe before the main validation row was discussed.",
                "note": "Workflow / provenance context for the B1 recipe choice.",
                "page_label": "Phase 1 Recipe Selection",
            },
            {
                "title": "Mindoro Primary Validation",
                "classification": "Thesis-facing",
                "body": "The March 13 -> March 14 R1 primary validation row is the only thesis-facing Mindoro observation-based validation claim.",
                "note": "Main paper uses this Mindoro row only.",
                "page_label": "Mindoro B1 Primary Validation",
            },
            {
                "title": "Mindoro Comparator",
                "classification": "Comparator support",
                "body": "Track A compares OpenDrift and PyGNOME on the same March 14 target, but it remains comparator-only and excludes archived R0-only outputs from the thesis-facing view.",
                "note": "PyGNOME is never shown as truth here.",
                "page_label": "Mindoro Cross-Model Comparator",
            },
            {
                "title": "DWH Transfer Validation",
                "classification": "Thesis-facing",
                "body": "DWH is a separate transfer-validation lane with C1 deterministic, C2 ensemble extension, and C3 comparator-only semantics.",
                "note": "No drifter baseline is used for DWH.",
                "page_label": "DWH Phase 3C Transfer Validation",
            },
            {
                "title": "Phase 4 Oil-Type and Shoreline",
                "classification": "Comparator support",
                "body": "Mindoro Phase 4 provides the current OpenDrift/OpenOil scenario context for oil-type and shoreline interpretation.",
                "note": "No matched Mindoro Phase 4 PyGNOME package is currently shown.",
                "page_label": "Phase 4 Oil-Type and Shoreline Context",
            },
        ],
        columns_per_row=2 if export_mode else 3,
        export_mode=export_mode,
    )

    st.subheader("Secondary lanes")
    st.caption("Archive and legacy remain discoverable here, but they are not part of the primary thesis evidence chain.")
    render_study_structure_cards(
        [
            {
                "title": "Mindoro Validation Archive",
                "classification": "Archive only",
                "body": "Archived March13-14 R0 baseline, older R0-including March13-14 outputs, and preserved March-family legacy rows are centralized here for provenance, audit, and reproducibility.",
                "note": "Not thesis-facing evidence; main paper uses March 13 -> March 14 R1 only.",
                "page_label": "Mindoro Validation Archive",
            },
            {
                "title": "Legacy 2016 Support",
                "classification": "Legacy support",
                "body": "The 2016 package preserves the legacy support flow and now includes a budget-only deterministic PyGNOME Phase 4 pilot.",
                "note": "This lane remains support-only and is not main validation evidence.",
                "page_label": "Legacy 2016 Support Package",
            },
        ],
        columns_per_row=1 if export_mode else 2,
        export_mode=export_mode,
    )

    package_lookup = {
        package.get("package_id"): package
        for package in state.get("curated_package_roots", [])
        if package.get("package_id")
    }
    phase1_quick_link = {
        "package_id": "phase1_recipe_selection",
        "label": "Phase 1 focused provenance package",
        "page_label": "Phase 1 Recipe Selection",
        "relative_path": "output/phase1_mindoro_focus_pre_spill_2016_2023",
        "description": "Focused historical drifter provenance lane used to select the official Mindoro B1 recipe before Phase 3B is discussed.",
        "secondary_note": "Thesis-facing",
        "artifact_count": int(len(state.get("phase1_focused_recipe_summary", []))),
        "button_label": "Open page",
    }

    def _story_package(package_id: str, lane_label: str) -> dict | None:
        package = package_lookup.get(package_id)
        if not package:
            return None
        return {**package, "secondary_note": lane_label, "button_label": "Open page"}

    primary_quick_links = [
        phase1_quick_link,
        _story_package("mindoro_b1_final", "Thesis-facing"),
        _story_package("mindoro_comparator", "Comparator support"),
        _story_package("dwh_phase3c_final", "Thesis-facing"),
        _story_package("phase4_context_status", "Comparator support"),
    ]
    primary_quick_links = [package for package in primary_quick_links if package]
    secondary_quick_links = [
        _story_package("mindoro_validation_archive", "Archive only"),
        _story_package("legacy_2016_final", "Legacy support"),
    ]
    secondary_quick_links = [package for package in secondary_quick_links if package]

    st.subheader("Story shortcuts")
    st.caption("These shortcuts point to the main curated pages in thesis-story order rather than to raw case folders.")
    render_package_cards(
        primary_quick_links,
        columns_per_row=1 if export_mode else 3,
        export_mode=export_mode,
    )
    if secondary_quick_links:
        st.caption("Secondary lanes stay available when you need provenance or historical support beyond the main story.")
        render_package_cards(
            secondary_quick_links,
            columns_per_row=1 if export_mode else 2,
            export_mode=export_mode,
        )
    st.caption("For manifests, logs, and synced registries, use the Artifacts / Logs / Registries page.")

    render_figure_gallery(
        recommended,
        title="Featured thesis-facing figures",
        caption="These figures keep the Home strip on the thesis-first story order from the publication registry governance fields: workflow/provenance context first, then Mindoro B1 March 13 -> March 14 R1 primary validation, comparator support, and DWH transfer-validation. Mindoro archive, legacy March-family, R0, and Phase 4 figures remain available on their dedicated pages and story shortcuts rather than on this featured strip.",
        limit=2 if export_mode else None,
        columns_per_row=1 if export_mode else 2,
        export_mode=export_mode,
        overlay_label="Click to enlarge",
    )

    if ui_state["advanced"] and not export_mode:
        st.subheader("Advanced notes")
        st.caption("Panel mode stops here. Advanced mode keeps the lower-level reproducibility notes available in read-only form.")
        render_markdown_block("Final reproducibility summary", state["final_reproducibility_summary"], collapsed=True, export_mode=export_mode)
        render_markdown_block("Publication talking points", state["publication_talking_points"], collapsed=True, export_mode=export_mode)
        render_markdown_block("Publication captions", state["publication_captions"], collapsed=True, export_mode=export_mode)
