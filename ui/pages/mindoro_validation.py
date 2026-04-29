"""Mindoro B1 primary-validation page."""

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

from ui.evidence_contract import ROLE_THESIS, assert_no_archive_leak, filter_for_page
from ui.pages.common import (
    render_caveat_ribbon,
    render_export_note,
    render_feature_grid,
    render_figure_gallery,
    render_key_takeaway,
    render_markdown_block,
    render_metric_story_grid,
    render_modern_hero,
    render_package_cards,
    render_section_header,
    render_section_stack,
    render_status_callout,
    render_table,
)


def _b1_score_table():
    import pandas as pd

    return pd.DataFrame(
        [
            {"Metric": "Forecast cells", "Value": "5"},
            {"Metric": "Observed cells", "Value": "22"},
            {"Metric": "Nearest distance", "Value": "1414.21 m"},
            {"Metric": "Centroid distance", "Value": "7358.16 m"},
            {"Metric": "IoU", "Value": "0.0"},
            {"Metric": "Dice", "Value": "0.0"},
            {"Metric": "Mean FSS", "Value": "0.1075"},
        ]
    )


def _b1_fss_table():
    import pandas as pd

    return pd.DataFrame(
        [
            {"Neighborhood window": "1 km", "FSS": "0.0000", "Interpretation": "No exact-grid overlap"},
            {"Neighborhood window": "3 km", "FSS": "0.0441", "Interpretation": "Neighborhood agreement begins"},
            {"Neighborhood window": "5 km", "FSS": "0.1371", "Interpretation": "Agreement visible at intermediate scale"},
            {"Neighborhood window": "10 km", "FSS": "0.2490", "Interpretation": "Strongest agreement at broadest window"},
            {"Neighborhood window": "Mean", "FSS": "0.1075", "Interpretation": "Overall mean across four windows"},
        ]
    )


def _mindoro_final_subset(
    df,
    artifact_groups: set[str],
    *,
    status_keys: set[str] | None = None,
    surface_keys: set[str] | None = None,
) -> object:
    if df.empty:
        return df
    payload = df.copy()
    if "artifact_group" in payload.columns:
        payload = payload.loc[payload.get("artifact_group", "").astype(str).isin(sorted(artifact_groups))].copy()
    if status_keys and "status_key" in payload.columns:
        payload = payload.loc[payload.get("status_key", "").astype(str).isin(sorted(status_keys))].copy()
    if surface_keys and "surface_key" in payload.columns:
        payload = payload.loc[payload.get("surface_key", "").astype(str).isin(sorted(surface_keys))].copy()
    return payload.reset_index(drop=True)


def _filter_table(df, *, column: str, allowed_values: set[str] | None = None, blocked_values: set[str] | None = None):
    if df is None or df.empty or column not in df.columns:
        payload = df.copy() if df is not None else df
        if payload is None or payload.empty:
            return payload
        if "surface_key" in payload.columns and allowed_values is None and blocked_values is None:
            return payload.reset_index(drop=True)
        return payload
    payload = df.copy()
    series = payload[column].fillna("").astype(str)
    if allowed_values:
        payload = payload.loc[series.isin(sorted(allowed_values))].copy()
        series = payload[column].fillna("").astype(str)
    if blocked_values:
        payload = payload.loc[~series.isin(sorted(blocked_values))].copy()
    return payload.reset_index(drop=True)


def render(state: dict, ui_state: dict) -> None:
    export_mode = bool(ui_state.get("export_mode"))
    phase1_manifest = state.get("phase1_focused_manifest") or {}
    phase1_ranking = state.get("phase1_focused_recipe_ranking")
    selected_recipe = str(phase1_manifest.get("official_b1_recipe") or "").strip()
    if not selected_recipe and phase1_ranking is not None and not phase1_ranking.empty and "recipe" in phase1_ranking.columns:
        selected_recipe = str(phase1_ranking.iloc[0]["recipe"]).strip()
    recipe_family = [str(value).strip() for value in phase1_manifest.get("official_recipe_family") or [] if str(value).strip()]
    recipe_scope = f"{len(recipe_family)}-recipe" if recipe_family else "focused"

    render_modern_hero(
        "Mindoro B1 Primary Validation",
        "Mindoro B1 is the only main Philippine public-observation validation claim: a March 13-14 reinitialization-based check against the March 14 public observation mask.",
        badge=ROLE_THESIS,
        eyebrow="Primary thesis-facing Mindoro validation",
        meta=["March 13 -> March 14", "Fixed 1 km grid", "Public observation mask"],
        tone="thesis",
    )

    if export_mode:
        render_export_note(
            [
                "Export mode presents the Mindoro validation story as a single sequential brief.",
                "The March 13-14 B1 primary validation row stays first, while comparator and archive material stay on their own pages.",
            ]
        )

    render_caveat_ribbon(
        "Shared-imagery / reinitialization caveat",
        "The March 13–14 pair shares March 12 WorldView-3 imagery provenance, so it is treated as a bounded reinitialization-based validation check rather than a fully independent day-to-day validation claim.",
    )

    mindoro_final_registry = state["mindoro_final_registry"]
    primary_figures = filter_for_page(
        _mindoro_final_subset(
            mindoro_final_registry,
            {"publication/observations", "publication/opendrift_primary"},
            surface_keys={"thesis_main"},
        ),
        "mindoro_validation",
        advanced=bool(ui_state.get("advanced")),
    )
    assert_no_archive_leak(primary_figures, "mindoro_validation", advanced=bool(ui_state.get("advanced")))
    b1_summary = filter_for_page(
        state["mindoro_b1_summary"].loc[
            state["mindoro_b1_summary"].get("surface_key", "").astype(str).eq("thesis_main")
        ].reset_index(drop=True),
        "mindoro_validation",
        advanced=bool(ui_state.get("advanced")),
    )
    b1_fss = filter_for_page(
        state["mindoro_b1_fss"].loc[
            state["mindoro_b1_fss"].get("surface_key", "").astype(str).eq("thesis_main")
        ].reset_index(drop=True),
        "mindoro_validation",
        advanced=bool(ui_state.get("advanced")),
    )
    assert_no_archive_leak(b1_summary, "mindoro_validation", advanced=bool(ui_state.get("advanced")))
    assert_no_archive_leak(b1_fss, "mindoro_validation", advanced=bool(ui_state.get("advanced")))
    archive_package = next(
        (package for package in state.get("curated_package_roots", []) if package.get("package_id") == "mindoro_validation_archive"),
        None,
    )

    render_key_takeaway(
        "B1 is the only main Philippine public-observation validation claim.",
        "The stored B1 row supports coastal-neighborhood usefulness at broader FSS windows, while exact 1 km overlap remains absent.",
        tone="thesis",
        badge=ROLE_THESIS,
    )
    render_section_header(
        "Main Result",
        "The primary board and metrics below are the panel-facing B1 result. Values are stored and not recomputed by the dashboard.",
        badge=ROLE_THESIS,
    )
    result_left, result_right = st.columns([1.35, 1])
    with result_left:
        render_figure_gallery(
            primary_figures,
            title="Primary B1 result board",
            caption="Stored March 13-14 R1_previous primary-validation board and observation context. Archive rows stay off this surface.",
            limit=1,
            columns_per_row=1,
            export_mode=export_mode,
            overlay_label="Click to enlarge",
        )
    with result_right:
        render_metric_story_grid(
            [
                ("Mean FSS", "0.1075", "Mean across 1, 3, 5, and 10 km windows"),
                ("FSS 1 km", "0.0000", "No exact-grid overlap"),
                ("FSS 3 km", "0.0441", "Neighborhood agreement begins"),
                ("FSS 5 km", "0.1371", "Intermediate-scale agreement"),
                ("FSS 10 km", "0.2490", "Strongest agreement at broadest window"),
                ("Nearest distance", "1414.21 m", "Nearest forecast-to-observation distance"),
                ("Centroid distance", "7358.16 m", "Centroid separation"),
            ],
            export_mode=export_mode,
        )
        render_feature_grid(
            [
                {
                    "title": "Recipe provenance",
                    "badge": ROLE_THESIS,
                    "body": f"B1 inherits the official {selected_recipe or 'Phase 1 selected'} recipe from the separate focused 2016-2023 Mindoro Phase 1 lane.",
                    "note": f"The {recipe_scope} comparison promoted the focused historical winner into official B1.",
                }
            ],
            columns_per_row=1,
            export_mode=export_mode,
        )
    render_caveat_ribbon(
        "B1 caveat",
        "No exact 1 km overlap; this supports coastal-neighborhood usefulness, not exact-grid reproduction. No exact 1 km overlap is present; B1 supports coastal-neighborhood usefulness only. The March 13–14 pair shares March 12 WorldView-3 imagery provenance, so this is treated as a bounded reinitialization-based validation check rather than a fully independent day-to-day validation claim.",
    )

    def _primary_package() -> None:
        render_table(
            "Mindoro B1 score card",
            _b1_score_table(),
            download_name="mindoro_b1_score_card.csv",
            caption="March 13-14 R1_previous primary validation row only.",
            height=230,
            export_mode=export_mode,
        )
        render_table(
            "Mindoro B1 FSS by neighborhood window",
            _b1_fss_table(),
            download_name="mindoro_b1_fss.csv",
            caption="FSS grows with neighborhood scale; no exact 1 km overlap is present.",
            height=230,
            export_mode=export_mode,
        )
        render_figure_gallery(
            primary_figures,
            title="March 13-14 R1_previous primary-validation figures",
            caption="These stored figures support the B1 primary validation row only.",
            limit=2 if export_mode else (None if ui_state["advanced"] else 5),
            columns_per_row=1 if export_mode else 2,
            export_mode=export_mode,
            overlay_label="Click to enlarge",
        )

    def _tables_and_notes() -> None:
        render_table(
            "March 13-14 R1_previous stored summary",
            b1_summary,
            download_name="march13_14_r1_primary_summary.csv",
            caption="Curated primary row from the final March13-14 package.",
            height=220,
            export_mode=export_mode,
        )
        render_table(
            "March 13-14 R1_previous stored FSS by window",
            b1_fss,
            download_name="march13_14_reinit_fss_by_window.csv",
            caption="Curated R1-only FSS table from the primary March13-14 package.",
            height=220,
            export_mode=export_mode,
        )
        if ui_state["advanced"]:
            render_markdown_block("Mindoro B1 stored export note", state["mindoro_final_readme"], collapsed=True, export_mode=export_mode)

    def _archive_note() -> None:
        render_status_callout(
            "Archive note",
            "Non-primary Mindoro validation provenance is separated from this B1 page. Use Archive — Mindoro Validation Provenance for audit-only rows.",
            "warning",
        )
        if archive_package:
            archive_card = {
                **archive_package,
                "description": "Archived Mindoro validation provenance is kept for audit and reproducibility only, outside the B1 primary validation page.",
                "secondary_note": "Archive-only; not B1 evidence.",
                "button_label": "Open Mindoro validation archive",
            }
            render_package_cards(
                [archive_card],
                columns_per_row=1,
                export_mode=export_mode,
            )

    render_section_stack(
        [
            ("March 13-14 R1 Previous Main Result", _primary_package),
            ("R1 Tables And Notes", _tables_and_notes),
            ("Archive Note", _archive_note),
        ],
        export_mode=export_mode,
        use_tabs=ui_state["advanced"],
    )
