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

from ui.pages.common import (
    render_export_note,
    render_figure_gallery,
    render_markdown_block,
    render_metric_row,
    render_page_intro,
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
    study_box_figures = publication_registry.loc[
        publication_registry.get("status_key", pd.Series(dtype=str)).astype(str).eq("thesis_study_box_reference")
    ].reset_index(drop=True)
    study_box_overview_figures = study_box_figures.loc[
        study_box_figures.get("run_type", pd.Series(dtype=str)).astype(str).eq("single_reference_map")
    ].reset_index(drop=True)
    study_box_detail_figures = study_box_figures.loc[
        study_box_figures.get("run_type", pd.Series(dtype=str)).astype(str).eq("single_box_reference_map")
    ].reset_index(drop=True)

    time_window = manifest.get("time_window") or {}
    subset_info = manifest.get("ranking_subset") or {}
    selected_recipe = str(manifest.get("official_b1_recipe") or "").strip() or (
        str(ranking.iloc[0]["recipe"]).strip() if not ranking.empty and "recipe" in ranking.columns else "Not available"
    )
    historical_winner = str(manifest.get("historical_four_recipe_winner") or manifest.get("winning_recipe") or "").strip() or selected_recipe
    gfs_historical_winner_not_adopted = bool(manifest.get("gfs_historical_winner_not_adopted", False))
    recipe_family = _format_recipe_family(manifest.get("official_recipe_family") or [])
    reference_recipe = (
        str(reference_manifest.get("winning_recipe") or "").strip()
        or (str(reference_ranking.iloc[0]["recipe"]).strip() if not reference_ranking.empty and "recipe" in reference_ranking.columns else "")
    )

    render_page_intro(
        "Phase 1 Recipe Selection",
        "This page explains how the Mindoro transport recipe was chosen before the main validation step. It keeps the focused Mindoro provenance lane separate from the broader regional reference lane and shows how B1 inherits the selected recipe without directly ingesting drifters inside Phase 3B.",
        badge="Phase 1 provenance | recipe selection before B1",
    )

    if export_mode:
        render_export_note(
            [
                "Export mode keeps the Phase 1 story on one page so the PDF can be read without tabs.",
                "The focused Mindoro provenance lane remains the primary recipe-selection story, while the broader regional lane stays visible only as reference context.",
            ]
        )

    render_status_callout(
        "What this page establishes",
        "Historical drifter segments were used to compare candidate recipes before the main Mindoro validation case was discussed.",
        "info",
    )
    render_status_callout(
        "Focused Mindoro result",
        f"The focused Mindoro provenance lane now evaluates `{recipe_family}`. Official B1 currently inherits `{selected_recipe}` from that focused lane.",
        "info",
    )
    render_status_callout(
        "How B1 uses it",
        "Mindoro B1 inherits the recipe selected by this separate Phase 1 provenance lane. Phase 3B itself does not directly ingest drifters.",
        "info",
    )
    render_status_callout(
        "Recipe-scope note",
        (
            "The focused Mindoro provenance lane now evaluates the four-recipe family, and the official B1 baseline promotes the focused historical winner directly."
            if not gfs_historical_winner_not_adopted
            else "The focused Mindoro provenance lane now evaluates the four-recipe family. If a GFS-backed recipe wins historically, the official B1 baseline still keeps the highest-ranked non-GFS fallback until a separate event-scale GFS adoption workflow is completed."
        ),
        "info",
    )
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

    render_metric_row(
        [
            ("Selected recipe", selected_recipe or "Not available"),
            ("Focused recipes tested", str(len(summary)) if not summary.empty else "0"),
            ("Accepted segments", str(manifest.get("accepted_segment_count", 0))),
            ("Ranking subset", str(subset_info.get("segment_count", len(subset)) or 0)),
            ("Study window", f"{str(time_window.get('start_utc', ''))[:10]} to {str(time_window.get('end_utc', ''))[:10]}"),
        ],
        export_mode=export_mode,
    )

    if not study_box_overview_figures.empty or not study_box_detail_figures.empty:
        render_status_callout(
            "Shared box reference",
            "The thesis-facing box vocabulary is now bundled as one shared panel-ready reference figure plus separate per-box geography figures so the focused Mindoro Phase 1 box, `mindoro_case_domain`, the scoring-grid display bounds, and the legacy prototype first-code search box stay distinct.",
            "info",
        )
        if not study_box_overview_figures.empty:
            st.markdown("### Study boxes used by the thesis")
            render_figure_gallery(
                study_box_overview_figures,
                title="Study boxes used by the thesis",
                caption="This shared publication figure is built from stored config, manifest, and provenance metadata only. It keeps the active Mindoro provenance box separate from the broader Mindoro overview domain, the narrower scoring-grid display bounds, and the prototype_2016 historical-origin first-code search box.",
                limit=1,
                columns_per_row=1,
                export_mode=export_mode,
                overlay_label="Click to enlarge",
            )
        if not study_box_detail_figures.empty:
            st.markdown("### Per-box geography references")
            render_figure_gallery(
                study_box_detail_figures,
                title="Per-box geography references",
                caption="These additional panel-ready figures isolate each stored thesis box while keeping the west-coast Philippines geography visible for presentation and thesis-panel use.",
                limit=4,
                columns_per_row=2,
                export_mode=export_mode,
                overlay_label="Click to enlarge",
            )

    def _focused_lane() -> None:
        render_status_callout(
            "Focused lane role",
            "This is the active Mindoro-specific provenance lane used to support the B1 recipe choice.",
            "info",
        )
        render_table(
            "Focused recipe ranking",
            ranking,
            download_name="phase1_recipe_ranking.csv",
            caption="Stored ranking for the focused Mindoro Phase 1 provenance lane.",
            height=220,
            export_mode=export_mode,
        )
        render_table(
            "Focused recipe summary",
            summary,
            download_name="phase1_recipe_summary.csv",
            caption="Stored summary of the recipe family actually tested for the focused Mindoro provenance lane.",
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
            f"Focused Phase 1 recorded `{historical_winner}` as the historical winner and `{selected_recipe}` as the official B1 recipe. The active Mindoro B1 package inherits the official recipe through the repo baseline-selection layer.",
            "info",
        )
        st.markdown(
            "\n".join(
                [
                    "### Plain-language study chain",
                    "1. Historical drifter segments were screened in the focused Mindoro window.",
                    f"2. The focused ranking subset compared `{recipe_family}` on accepted February-April starts.",
                    f"3. `{historical_winner}` ranked best in the stored focused lane.",
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

    def _legacy_support_lane() -> None:
        render_status_callout(
            "Legacy lane role",
            "The prototype_2016 lane remains a legacy support package. It still shows a full support flow, but it does not replace the current Mindoro-specific Phase 1 provenance story.",
            "warning",
        )
        st.markdown(
            "\n".join(
                [
                    "### Legacy support flow",
                    "- `prototype_2016` keeps a visible support flow of Phase 1 -> Phase 2 -> Phase 3A -> Phase 4 -> Phase 5.",
                    "- It is useful for legacy pipeline development context and comparator interpretation.",
                    "- It is not the main recipe-selection evidence for the current Mindoro B1 claim.",
                ]
            )
        )

    def _source_tables() -> None:
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

    render_section_stack(
        [
            ("Focused provenance lane", _focused_lane),
            ("How B1 inherits the recipe", _inheritance_story),
            ("Regional reference lane", _regional_reference),
            ("Legacy 2016 support note", _legacy_support_lane),
            ("Source tables", _source_tables),
        ],
        export_mode=export_mode,
        use_tabs=ui_state["advanced"],
    )
