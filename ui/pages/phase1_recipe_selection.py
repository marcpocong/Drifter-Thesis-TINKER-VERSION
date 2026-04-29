"""Phase 1 recipe-selection page."""

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

from src.core.study_box_catalog import ARCHIVE_ONLY_STUDY_BOX_NUMBERS, THESIS_FACING_STUDY_BOX_NUMBERS
from ui.evidence_contract import ROLE_ADVANCED, ROLE_ARCHIVE, ROLE_THESIS, filter_for_page
from ui.pages.common import (
    render_caveat_ribbon,
    render_evidence_path,
    render_export_note,
    render_feature_grid,
    render_figure_gallery,
    render_key_takeaway,
    render_markdown_block,
    render_metric_story_grid,
    render_modern_hero,
    render_section_header,
    render_section_stack,
    render_status_callout,
    render_table,
)


def _year_counts(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "start_time_utc" not in df.columns:
        return pd.DataFrame()
    years = pd.to_datetime(df["start_time_utc"], errors="coerce", utc=True).dt.year.dropna().astype(int)
    if years.empty:
        return pd.DataFrame()
    counts = years.value_counts().sort_index().rename_axis("year").reset_index(name="accepted_segment_count")
    return counts


def _format_recipe_family(values: object) -> str:
    if isinstance(values, list):
        clean = [str(value).strip() for value in values if str(value).strip()]
        return ", ".join(clean)
    return str(values or "").strip()


def _filter_study_box_figures(df: pd.DataFrame, slugs: tuple[str, ...]) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    payload = df.copy()
    if "study_box_numbers" in payload.columns:
        payload["_study_box_sort"] = payload["study_box_numbers"].astype(str).str.split(",").apply(
            lambda values: min(
                [int(str(value).strip()) for value in values if str(value).strip().isdigit()] or [999]
            )
        )
        return payload.sort_values(["_study_box_sort", "figure_id"]).drop(columns="_study_box_sort").reset_index(drop=True)
    if "figure_id" not in payload.columns:
        return pd.DataFrame()
    if payload.empty:
        return payload.reset_index(drop=True)
    return payload.sort_values("figure_id").reset_index(drop=True)


def _recipe_order_from_ranking(ranking: pd.DataFrame) -> list[str]:
    if ranking.empty or "recipe" not in ranking.columns:
        return []
    payload = ranking.copy()
    payload["_recipe_key"] = payload["recipe"].astype(str).str.strip()
    payload = payload.loc[payload["_recipe_key"].ne("")]
    if payload.empty:
        return []
    if "rank" in payload.columns:
        payload["_rank_sort"] = pd.to_numeric(payload["rank"], errors="coerce")
    else:
        payload["_rank_sort"] = range(1, len(payload) + 1)
    payload = payload.sort_values(["_rank_sort", "_recipe_key"], na_position="last")
    return payload["_recipe_key"].tolist()


def _sort_summary_by_rank(summary: pd.DataFrame, ranking: pd.DataFrame) -> pd.DataFrame:
    if summary.empty or "recipe" not in summary.columns:
        return summary.copy()
    recipe_order = _recipe_order_from_ranking(ranking)
    if not recipe_order:
        return summary.reset_index(drop=True)
    rank_map = {recipe: index + 1 for index, recipe in enumerate(recipe_order)}
    payload = summary.copy()
    payload["_recipe_key"] = payload["recipe"].astype(str).str.strip()
    payload["_rank_sort"] = payload["_recipe_key"].map(rank_map)
    if "rank" not in payload.columns:
        payload.insert(0, "rank", payload["_rank_sort"])
    else:
        payload["rank"] = payload["_rank_sort"].combine_first(pd.to_numeric(payload["rank"], errors="coerce"))
    payload = payload.sort_values(["_rank_sort", "_recipe_key"], na_position="last")
    return payload.drop(columns=["_recipe_key", "_rank_sort"]).reset_index(drop=True)


def _focused_recipe_ranking_summary_table() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"Rank": 1, "Recipe": "cmems_gfs", "Mean NCS": "4.5886", "Median NCS": "4.6305", "Status": "Winner / selected for B1"},
            {"Rank": 2, "Recipe": "cmems_era5", "Mean NCS": "4.6237", "Median NCS": "4.5916", "Status": "Runner-up / not selected"},
            {"Rank": 3, "Recipe": "hycom_gfs", "Mean NCS": "4.7027", "Median NCS": "4.9263", "Status": "Not selected"},
            {"Rank": 4, "Recipe": "hycom_era5", "Mean NCS": "4.7561", "Median NCS": "5.0106", "Status": "Not selected"},
        ]
    )


def _transport_settings_table() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"Field": "Workflow mode", "Stored thesis-facing value": "phase1_mindoro_focus_pre_spill_2016_2023"},
            {"Field": "Historical window", "Stored thesis-facing value": "2016-01-01T00:00:00Z to 2023-03-02T23:59:59Z"},
            {"Field": "Focused validation box", "Stored thesis-facing value": "[118.751, 124.305, 10.62, 16.026]"},
            {"Field": "Drifter dataset", "Stored thesis-facing value": "NOAA OSMC ERDDAP drifter_6hour_qc"},
            {"Field": "Segment structure", "Stored thesis-facing value": "72 h segments on a 6 h grid"},
            {"Field": "Acceptance rules", "Stored thesis-facing value": "drogued only; full duration; continuous coverage; non-overlapping windows; all points inside validation box"},
            {"Field": "Ranking settings", "Stored thesis-facing value": "direct wind drift factor 0.02; Stokes drift on; horizontal diffusivity 0.0 m2/s; weathering off"},
        ]
    )


def _study_box_summary_table() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "Box/domain": "Study Box 2 - mindoro_case_domain",
                "Bounds": "[115.0, 122.0, 6.0, 14.5]",
                "Role": "Broad official Mindoro spill-case fallback transport and forcing domain",
                "Default status": ROLE_THESIS,
            },
            {
                "Box/domain": "Study Box 4 - prototype first-code search box",
                "Bounds": "[108.6465, 121.3655, 6.1865, 20.3515]",
                "Role": "Historical-origin search box that surfaced earliest 2016 prototype cases",
                "Default status": ROLE_THESIS,
            },
            {
                "Box/domain": "Study Box 1 - focused Mindoro Phase 1 box",
                "Bounds": "[118.751, 124.305, 10.62, 16.026]",
                "Role": "Separate drifter-validation box for active Mindoro provenance lane",
                "Default status": ROLE_ARCHIVE,
            },
            {
                "Box/domain": "Study Box 3 - scoreable display bounds",
                "Bounds": "[120.909647, 122.062154, 12.249385, 13.783655]",
                "Role": "Narrow operational scoring-grid display extent",
                "Default status": ROLE_ADVANCED,
            },
        ]
    )


def render(state: dict, ui_state: dict) -> None:
    export_mode = bool(ui_state.get("export_mode"))
    manifest = state["phase1_focused_manifest"] or {}
    ranking = state["phase1_focused_recipe_ranking"]
    summary = state["phase1_focused_recipe_summary"]
    accepted = state["phase1_focused_accepted_segments"]
    subset = state["phase1_focused_ranking_subset"]
    loading_audit = state["phase1_focused_loading_audit"]
    reference_manifest = state["phase1_reference_manifest"] or {}
    reference_ranking = state["phase1_reference_recipe_ranking"]
    reference_summary = state["phase1_reference_recipe_summary"]
    publication_registry = state["publication_registry"]
    focused_missing = not manifest and ranking.empty and summary.empty
    study_box_candidates = filter_for_page(
        publication_registry,
        "phase1_recipe_selection",
        advanced=bool(ui_state.get("advanced")),
    )
    study_box_figures = study_box_candidates.loc[
        study_box_candidates.get("status_key", pd.Series(dtype=str)).astype(str).eq("thesis_study_box_reference")
    ].reset_index(drop=True)
    study_box_overview_figures = study_box_figures.loc[
        study_box_figures.get("study_box_id", pd.Series(dtype=str)).astype(str).eq("thesis_study_boxes_reference")
    ].reset_index(drop=True)
    study_box_detail_figures = study_box_figures.loc[
        study_box_figures.get("run_type", pd.Series(dtype=str)).astype(str).eq("single_box_reference_map")
    ].reset_index(drop=True)
    featured_study_box_detail_figures = _filter_study_box_figures(
        study_box_detail_figures.loc[
            study_box_detail_figures.get("thesis_surface", pd.Series(dtype=bool)).fillna(False).astype(bool)
            & study_box_detail_figures.get("study_box_numbers", pd.Series(dtype=str)).astype(str).isin(
                list(THESIS_FACING_STUDY_BOX_NUMBERS)
            )
        ].copy(),
        THESIS_FACING_STUDY_BOX_NUMBERS,
    )
    archived_study_box_detail_figures = _filter_study_box_figures(
        study_box_detail_figures.loc[
            study_box_detail_figures.get("archive_only", pd.Series(dtype=bool)).fillna(False).astype(bool)
            & study_box_detail_figures.get("study_box_numbers", pd.Series(dtype=str)).astype(str).isin(
                list(ARCHIVE_ONLY_STUDY_BOX_NUMBERS)
            )
        ].copy(),
        ARCHIVE_ONLY_STUDY_BOX_NUMBERS,
    )

    time_window = manifest.get("time_window") or {}
    subset_info = manifest.get("ranking_subset") or {}
    ranked_recipes = _recipe_order_from_ranking(ranking)
    ranking_winner = ranked_recipes[0] if ranked_recipes else ""
    ranking_runner_up = ranked_recipes[1] if len(ranked_recipes) > 1 else ""
    focused_summary = _sort_summary_by_rank(summary, ranking)
    selected_recipe = str(manifest.get("official_b1_recipe") or "").strip() or ranking_winner or "Not available"
    historical_winner = ranking_winner or str(manifest.get("historical_four_recipe_winner") or manifest.get("winning_recipe") or "").strip() or selected_recipe
    gfs_historical_winner_not_adopted = bool(manifest.get("gfs_historical_winner_not_adopted", False))
    recipe_family = _format_recipe_family(manifest.get("official_recipe_family") or [])
    reference_recipe = (
        str(reference_manifest.get("winning_recipe") or "").strip()
        or (str(reference_ranking.iloc[0]["recipe"]).strip() if not reference_ranking.empty and "recipe" in reference_ranking.columns else "")
    )

    render_modern_hero(
        "Phase 1 Transport Provenance",
        "Focused historical drifter segments support transport provenance and recipe selection for official B1; they do not directly validate the mapped oil footprint.",
        badge=ROLE_THESIS,
        eyebrow="Transport provenance lane",
        meta=["Read-only stored outputs", "Recipe selection", "Not oil-footprint truth"],
        tone="thesis",
    )

    if export_mode:
        render_export_note(
            [
                "Export mode keeps the Phase 1 story on one page so the PDF can be read without tabs.",
                "The focused Mindoro provenance lane remains the primary recipe-selection story, while the broader regional lane stays visible only as reference context.",
            ]
        )

    render_key_takeaway(
        "Selected transport recipe",
        "Focused Mindoro Phase 1 selects cmems_gfs as the transport recipe inherited by B1.",
        tone="thesis",
        badge=ROLE_THESIS,
    )
    render_caveat_ribbon(
        "Transport provenance, not oil-footprint truth",
        "Drifter segments support transport-provenance and recipe selection; they are not direct oil-footprint truth. They do not make Phase 1 a B1 public-observation validation row.",
    )
    render_evidence_path(
        [
            ("Drifter screening", "Strict historical drifter windows are screened inside the focused Mindoro provenance lane.", ROLE_THESIS),
            ("72 h segments", "Accepted windows are treated as 72 h segments on a 6 h grid.", ROLE_THESIS),
            ("Four recipes", "cmems_era5, cmems_gfs, hycom_era5, and hycom_gfs are compared.", ROLE_THESIS),
            ("NCS ranking", "The focused ranking table remains the winner authority.", ROLE_THESIS),
            ("Selected recipe", "cmems_gfs is promoted into the official B1 setup.", ROLE_THESIS),
        ],
        title="Recipe-Selection Workflow",
        caption="The workflow stops at transport provenance and recipe selection; public-observation scoring happens later on the B1 page.",
        export_mode=export_mode,
    )
    render_metric_story_grid(
        [
            ("Selected recipe", "cmems_gfs", "Focused historical winner adopted by B1"),
            ("Accepted segments", "65", "Full strict accepted segment set"),
            ("Ranking subset", "19", "February-April ranking subset"),
            ("Historical window", "2016-01-01 to 2023-03-02", "Pre-spill focused provenance period"),
            ("Focused box", "[118.751, 124.305, 10.62, 16.026]", "Focused Mindoro Phase 1 validation box"),
        ],
        export_mode=export_mode,
    )
    render_feature_grid(
        [
            {
                "title": "Recipe family",
                "badge": ROLE_THESIS,
                "body": "The focused Mindoro provenance lane evaluates cmems_era5, cmems_gfs, hycom_era5, and hycom_gfs.",
                "note": "Ranking is by stored NCS values.",
            },
            {
                "title": "B1 inheritance",
                "badge": ROLE_THESIS,
                "body": "B1 inherits the selected transport recipe from this separate provenance lane.",
                "note": "The B1 public-observation validation run itself does not directly ingest drifters; it inherits recipe provenance from the separate focused Phase 1 lane.",
            },
        ],
        columns_per_row=2,
        export_mode=export_mode,
    )

    selected_recipe_value = f"Selected Mindoro B1 recipe: `{selected_recipe}`."
    if ranking_runner_up:
        selected_recipe_value += f" The focused ranking table shows `{ranking_runner_up}` as the runner-up."
    if gfs_historical_winner_not_adopted:
        render_status_callout(
            "Historical-vs-official split",
            f"The raw historical four-recipe winner was `{historical_winner}`, but official B1 uses `{selected_recipe}` under the spill-usable non-GFS fallback rule.",
            "warning",
        )
    if focused_missing:
        render_status_callout(
            "Focused-lane fallback",
            "The focused Mindoro provenance artifacts are not available in this repo state. This page stays visible and falls back to the broader regional reference artifacts where possible so Phase 1 does not look absent.",
            "warning",
        )

    render_section_header("Main Result", selected_recipe_value, badge=ROLE_THESIS)
    render_table(
        "Focused recipe ranking summary",
        _focused_recipe_ranking_summary_table(),
        download_name="phase1_focused_recipe_ranking_summary.csv",
        caption="Stored ranking values for the focused Mindoro Phase 1 provenance lane.",
        height=190,
        export_mode=export_mode,
    )
    render_section_header("Details", "Transport settings, geography references, and stored provenance tables remain read-only.")
    render_table(
        "Transport-provenance settings",
        _transport_settings_table(),
        download_name="phase1_transport_settings.csv",
        caption="The active provenance lane is separate from B1 public-observation scoring.",
        height=250,
        export_mode=export_mode,
    )
    render_table(
        "Default geography references",
        _study_box_summary_table().head(2 if not ui_state["advanced"] else 4),
        download_name="study_box_roles.csv",
        caption=(
            "Default panel mode shows Study Boxes 2 and 4 as thesis-facing geography references. Other provenance and scoring-grid references stay outside the default panel view."
            if not ui_state["advanced"]
            else "Advanced mode also shows Study Boxes 1 and 3 as provenance/archive references."
        ),
        height=220,
        export_mode=export_mode,
    )

    if not study_box_overview_figures.empty or not study_box_detail_figures.empty:
        shared_box_note = (
            "Study Box 2 is the broader `mindoro_case_domain` overview extent and Study Box 4 is the prototype-origin first-code search box. "
            "Other provenance and scoring-grid box references remain outside the default thesis-facing geography set."
        )
        overview_caption = (
            "This updated shared publication figure is built from stored config, manifest, provenance metadata, and a local geography context layer only. "
            "Study Box 2 is `mindoro_case_domain` and Study Box 4 is the prototype-origin first-code search box."
        )
        if ui_state["advanced"]:
            shared_box_note = (
                "Study Box 2 is the broader `mindoro_case_domain` overview extent and Study Box 4 is the prototype-origin first-code search box. Study Box 1, the focused Phase 1 validation box, and Study Box 3, the scoring-grid display bounds, remain archive-only references for appendix, advanced, and audit use."
            )
            overview_caption = (
                "This updated shared publication figure is built from stored config, manifest, provenance metadata, and a local geography context layer only. Study Box 2 is `mindoro_case_domain` and Study Box 4 is the prototype-origin first-code search box, while Study Boxes 1 and 3 stay preserved as archive-only references."
            )
        render_status_callout(
            "Shared box reference",
            shared_box_note,
            "info",
        )
        if not study_box_overview_figures.empty:
            render_figure_gallery(
                study_box_overview_figures,
                title="Study boxes used by the thesis (Boxes 2 and 4)",
                caption=overview_caption,
                limit=1,
                columns_per_row=1,
                export_mode=export_mode,
                overlay_label="Click to enlarge",
            )
        if not featured_study_box_detail_figures.empty:
            render_figure_gallery(
                featured_study_box_detail_figures,
                title="Per-box geography references for the thesis (Boxes 2 and 4)",
                caption="These panel-ready geography references keep Study Box 2 (`mindoro_case_domain`) and Study Box 4 (the prototype-origin first-code search box) visible individually on the main page without bringing archive-only Study Boxes 1 and 3 back into the thesis-facing story.",
                limit=2,
                columns_per_row=2,
                export_mode=export_mode,
                overlay_label="Click to enlarge",
            )
        if ui_state["advanced"] and not archived_study_box_detail_figures.empty:
            render_figure_gallery(
                archived_study_box_detail_figures,
                title="Archived per-box geography references (Boxes 1 and 3)",
                caption="These preserved reference figures keep Study Box 1, the focused Phase 1 provenance box, and Study Box 3, the narrower scoring-grid bounds, available for archive, appendix, advanced, and audit use without making them part of the thesis-facing box set.",
                limit=2,
                columns_per_row=2,
                export_mode=export_mode,
                overlay_label="Click to enlarge",
            )

    def _focused_lane() -> None:
        render_status_callout(
            "Focused lane role",
            "This is the active Mindoro-specific provenance lane used to support the B1 recipe choice. The ranking table below is the authority for which recipe ranks first in the focused lane.",
            "info",
        )
        render_table(
            "Focused recipe ranking",
            ranking,
            download_name="phase1_recipe_ranking.csv",
            caption="Stored ranking for the focused Mindoro Phase 1 provenance lane. Use this table as the winner authority for the focused lane.",
            height=220,
            export_mode=export_mode,
        )
        render_table(
            "Accepted segments by year",
            _year_counts(accepted),
            download_name="phase1_accepted_segments_by_year.csv",
            caption="Accepted segments are historical drifter windows used for the provenance lane. Near-2023 accepted windows are not present in the current stored registry.",
            height=220,
            export_mode=export_mode,
        )
        render_table(
            "Ranking subset registry",
            subset,
            download_name="phase1_ranking_subset_registry.csv",
            caption="This is the subset used to rank the focused Mindoro recipe family. The stored lane hard-fails if this subset is empty.",
            height=260,
            max_rows=None if ui_state["advanced"] else 20,
            export_mode=export_mode,
        )

    def _inheritance_story() -> None:
        render_status_callout(
            "Provenance chain",
            f"The focused ranking table places `{historical_winner}` first, and the active Mindoro B1 package inherits `{selected_recipe}` through the repo baseline-selection layer.",
            "info",
        )
        st.markdown(
            "\n".join(
                [
                    "### Plain-language study chain",
                    "1. Historical drifter segments were screened in the focused Mindoro window.",
                    f"2. The focused ranking subset compared `{recipe_family}` on accepted February-April starts.",
                    f"3. The focused ranking table places `{historical_winner}` at rank 1.",
                    f"4. `{selected_recipe}` is the official B1 recipe that the March 13 -> March 14 primary validation row inherits.",
                ]
            )
        )
        render_markdown_block(
            "Focused ranking-subset report",
            state["phase1_focused_ranking_subset_report"],
            collapsed=not ui_state["advanced"],
            export_mode=export_mode,
        )
        render_markdown_block(
            "Focused baseline candidate",
            state["phase1_focused_baseline_candidate"],
            collapsed=not ui_state["advanced"],
            export_mode=export_mode,
        )

    def _regional_reference() -> None:
        reference_note = (
            f"The broader regional reference lane currently resolves to `{reference_recipe}`."
            if reference_recipe
            else "The broader regional reference lane is preserved for context, but its stored winning recipe is not available here."
        )
        render_status_callout(
            "Regional reference lane role",
            f"{reference_note} It is preserved as broader reference/governance context and is not the active Mindoro B1 provenance story.",
            "warning",
        )
        render_table(
            "Regional reference recipe ranking",
            reference_ranking,
            download_name="phase1_production_rerun_recipe_ranking.csv",
            caption="Stored ranking from the broader regional reference lane.",
            height=220,
            export_mode=export_mode,
        )
        render_table(
            "Regional reference recipe summary",
            reference_summary,
            download_name="phase1_production_rerun_recipe_summary.csv",
            caption="Stored recipe family summary from the broader regional reference lane.",
            height=220,
            export_mode=export_mode,
        )
        render_markdown_block(
            "Regional reference baseline candidate",
            state["phase1_reference_baseline_candidate"],
            collapsed=True,
            export_mode=export_mode,
        )

    def _source_tables() -> None:
        render_table(
            "Diagnostic recipe summary, not winner ranking",
            focused_summary,
            download_name="phase1_recipe_summary.csv",
            caption="Diagnostic view of the recipe family tested in the focused Mindoro provenance lane. It is sorted by focused rank when rank data is available, but the focused ranking table remains the authority for winner display.",
            height=220,
            export_mode=export_mode,
        )
        render_table(
            "Focused loading audit",
            loading_audit,
            download_name="phase1_loading_audit.csv",
            caption="Stored read-only loading audit for the focused provenance lane.",
            height=240,
            max_rows=None if ui_state["advanced"] else 20,
            export_mode=export_mode,
        )
        render_markdown_block(
            "Focused production manifest excerpt",
            f"```json\n{pd.Series(manifest).to_json(indent=2)}\n```" if manifest else "",
            collapsed=True,
            export_mode=export_mode,
        )

    sections = [
        ("Focused provenance lane", _focused_lane),
        ("How B1 inherits the recipe", _inheritance_story),
        ("Regional reference lane", _regional_reference),
        ("Source tables", _source_tables),
    ]

    render_section_stack(
        sections,
        export_mode=export_mode,
        use_tabs=ui_state["advanced"],
    )
