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

from ui.evidence_contract import ROLE_COMPARATOR, assert_no_archive_leak, filter_for_page
from ui.pages.common import (
    render_caveat_ribbon,
    render_comparator_banner,
    render_export_note,
    render_feature_grid,
    render_figure_gallery,
    render_key_takeaway,
    render_markdown_block,
    render_metric_story_grid,
    render_modern_hero,
    render_package_cards,
    render_section_header,
    render_status_callout,
    render_table,
)


def _track_a_table():
    import pandas as pd

    return pd.DataFrame(
        [
            {
                "Model/branch": "OpenDrift R1_previous",
                "Forecast cells": "5",
                "Nearest distance": "1414.21 m",
                "FSS 1 km": "0.0000",
                "FSS 3 km": "0.0441",
                "FSS 5 km": "0.1371",
                "FSS 10 km": "0.2490",
                "Mean FSS": "0.1075",
            },
            {
                "Model/branch": "PyGNOME deterministic",
                "Forecast cells": "6",
                "Nearest distance": "6082.76 m",
                "FSS 1 km": "0.0000",
                "FSS 3 km": "0.0000",
                "FSS 5 km": "0.0000",
                "FSS 10 km": "0.0244",
                "Mean FSS": "0.0061",
            },
        ]
    )


def _filter_table(df, *, surface_key: str) -> object:
    if df is None or df.empty:
        return df
    payload = df.copy()
    if "surface_key" not in payload.columns:
        return payload.reset_index(drop=True)
    return payload.loc[payload["surface_key"].fillna("").astype(str).eq(surface_key)].reset_index(drop=True)


def render(state: dict, ui_state: dict) -> None:
    export_mode = bool(ui_state.get("export_mode"))
    registry = state["mindoro_final_registry"]
    figures = filter_for_page(
        registry.loc[
            registry.get("artifact_group", "").astype(str).eq("publication/comparator_pygnome")
            & registry.get("surface_key", "").astype(str).eq("comparator_support")
        ].reset_index(drop=True),
        "cross_model_comparison",
        advanced=bool(ui_state.get("advanced")),
    )
    comparator_ranking = filter_for_page(
        _filter_table(state["mindoro_comparator_ranking"], surface_key="comparator_support"),
        "cross_model_comparison",
        advanced=bool(ui_state.get("advanced")),
    )
    comparator_summary = filter_for_page(
        _filter_table(state["mindoro_comparator_summary"], surface_key="comparator_support"),
        "cross_model_comparison",
        advanced=bool(ui_state.get("advanced")),
    )
    assert_no_archive_leak(figures, "cross_model_comparison", advanced=bool(ui_state.get("advanced")))
    assert_no_archive_leak(comparator_ranking, "cross_model_comparison", advanced=bool(ui_state.get("advanced")))
    assert_no_archive_leak(comparator_summary, "cross_model_comparison", advanced=bool(ui_state.get("advanced")))
    archive_package = next(
        (package for package in state.get("curated_package_roots", []) if package.get("package_id") == "mindoro_validation_archive"),
        None,
    )

    render_modern_hero(
        "Mindoro Same-Case OpenDrift-PyGNOME Comparator (Track A)",
        "The Mindoro same-case OpenDrift-PyGNOME comparator is support for the March 13-14 Mindoro case; Track A is the internal alias and it is not a second validation row.",
        badge=ROLE_COMPARATOR,
        eyebrow="Same-case comparator lane",
        meta=["Comparator support only", "March 14 target", "PyGNOME is not truth"],
        tone="comparator",
    )
    render_comparator_banner()

    if export_mode:
        render_export_note(
            [
                "Export mode keeps the comparator page concise so the PDF reads as cross-model context rather than a second validation claim.",
                "The figures and tables below remain comparator-only and should be read alongside the Primary Mindoro March 13-14 page, not instead of it.",
            ]
        )

    render_key_takeaway(
        "The same-case comparator contextualizes the Primary Mindoro result; it is not another validation row.",
        "OpenDrift R1_previous remains the B1-alias primary result, while deterministic PyGNOME is shown only as same-case comparator support.",
        tone="comparator",
        badge=ROLE_COMPARATOR,
    )
    render_caveat_ribbon(
        "Same-case comparator only",
        "Same-case comparator only; the March 14 public mask remains the external reference. PyGNOME is comparator-only and is not observational truth. Track A is the internal alias and does not replace the Primary Mindoro March 13-14 validation claim.",
    )

    render_section_header("Main Comparison", "Two stored model rows are shown side by side with their evidence boundary intact.")
    left, right = st.columns(2)
    with left:
        render_feature_grid(
            [
                {
                    "title": "OpenDrift R1_previous",
                    "badge": ROLE_COMPARATOR,
                    "body": "Primary Mindoro OpenDrift row (B1 alias) on the same March 14 target.",
                    "note": "Mean FSS 0.1075; nearest distance 1414.21 m.",
                }
            ],
            columns_per_row=1,
            export_mode=export_mode,
        )
        render_metric_story_grid(
            [
                ("Mean FSS", "0.1075", "Comparator table value"),
                ("Nearest distance", "1414.21 m", "OpenDrift row"),
            ],
            export_mode=export_mode,
        )
    with right:
        render_feature_grid(
            [
                {
                    "title": "PyGNOME deterministic",
                    "badge": ROLE_COMPARATOR,
                    "body": "Deterministic PyGNOME comparator support on the same external reference.",
                    "note": "Mean FSS 0.0061; nearest distance 6082.76 m.",
                }
            ],
            columns_per_row=1,
            export_mode=export_mode,
        )
        render_metric_story_grid(
            [
                ("Mean FSS", "0.0061", "Comparator-only support"),
                ("Nearest distance", "6082.76 m", "PyGNOME is not truth"),
            ],
            export_mode=export_mode,
        )

    render_section_header("Details", "Stored comparator values, curated figures, and filtered summary tables remain read-only.")
    render_table(
        "Table 4.8 - Mindoro same-case OpenDrift-PyGNOME comparator detail",
        _track_a_table(),
        download_name="mindoro_track_a_comparator_values.csv",
        caption="OpenDrift R1_previous is compared with deterministic PyGNOME as support only; Track A is the internal alias.",
        height=180,
        export_mode=export_mode,
    )

    render_figure_gallery(
        figures,
        title="Comparator publication figures",
        caption="These figures come from the curated comparator subgroup in the March13-14 final package. Click any figure to enlarge it and read the fuller context there.",
        limit=2 if export_mode else (None if ui_state["advanced"] else 4),
        columns_per_row=1 if export_mode else 2,
        export_mode=export_mode,
        overlay_label="Click to enlarge",
    )

    if export_mode:
        render_table(
            "Comparator ranking",
            comparator_ranking,
            download_name="march13_14_reinit_crossmodel_model_ranking.csv",
            caption="Curated thesis-facing ranking table for the same-case March 14 comparator lane after archive-only rows are removed.",
            height=260,
            export_mode=export_mode,
        )
        render_table(
            "Comparator summary",
            comparator_summary,
            download_name="march13_14_reinit_crossmodel_summary.csv",
            caption="Curated thesis-facing summary table for the Mindoro comparator subgroup after archive-only rows are removed.",
            height=260,
            export_mode=export_mode,
        )
    else:
        left, right = st.columns(2)
        with left:
            render_table(
                "Comparator ranking",
                comparator_ranking,
                download_name="march13_14_reinit_crossmodel_model_ranking.csv",
                caption="Curated thesis-facing ranking table for the same-case March 14 comparator lane after archive-only rows are removed.",
                height=260,
            )
        with right:
            render_table(
                "Comparator summary",
                comparator_summary,
                download_name="march13_14_reinit_crossmodel_summary.csv",
                caption="Curated thesis-facing summary table for the Mindoro comparator subgroup after archive-only rows are removed.",
                height=260,
            )

    if archive_package:
        render_status_callout(
            "Archive note",
            "Archived comparator outputs are not displayed here. Use Archive/Provenance and Legacy Support for provenance-only review.",
            "warning",
        )
        archive_card = {
            **archive_package,
            "description": "Archived Mindoro validation provenance is kept for audit and reproducibility only, outside the Mindoro same-case comparator page.",
            "secondary_note": "Archive-only; not same-case comparator evidence.",
            "button_label": "Open Mindoro validation archive",
        }
        render_package_cards(
            [archive_card],
            columns_per_row=1,
            export_mode=export_mode,
        )

    if ui_state["advanced"] or export_mode:
        render_markdown_block("Mindoro final-package note", state["mindoro_final_readme"], collapsed=True, export_mode=export_mode)
