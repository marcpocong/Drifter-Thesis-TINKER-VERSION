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

from ui.pages.common import render_markdown_block, render_metric_row, render_page_intro, render_status_callout, render_table


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
    manifest = state["phase1_focused_manifest"] or {}
    ranking = state["phase1_focused_recipe_ranking"]
    summary = state["phase1_focused_recipe_summary"]
    accepted = state["phase1_focused_accepted_segments"]
    subset = state["phase1_focused_ranking_subset"]
    loading_audit = state["phase1_focused_loading_audit"]
    reference_manifest = state["phase1_reference_manifest"] or {}
    reference_ranking = state["phase1_reference_recipe_ranking"]
    reference_summary = state["phase1_reference_recipe_summary"]

    time_window = manifest.get("time_window") or {}
    subset_info = manifest.get("ranking_subset") or {}
    selected_recipe = str(manifest.get("winning_recipe") or "").strip() or (
        str(ranking.iloc[0]["recipe"]).strip() if not ranking.empty and "recipe" in ranking.columns else "Not available"
    )
    recipe_family = _format_recipe_family(manifest.get("official_recipe_family") or [])
    reference_recipe = (
        str(reference_manifest.get("winning_recipe") or "").strip()
        or (str(reference_ranking.iloc[0]["recipe"]).strip() if not reference_ranking.empty and "recipe" in reference_ranking.columns else "")
    )

    render_page_intro(
        "Phase 1 Recipe Selection",
        "This page explains how the Mindoro forcing recipe was chosen before the main validation step. It keeps the focused drifter-based provenance lane separate from the broader regional reference lane and shows how B1 inherits the selected recipe without directly ingesting drifters in Phase 3B.",
        badge="Phase 1 provenance | recipe selection before B1",
    )

    render_status_callout(
        "What Phase 1 does",
        "Phase 1 uses historical drifter segments to compare forcing recipes and select a transport recipe before the main Mindoro validation case is discussed.",
        "info",
    )
    render_status_callout(
        "Focused Mindoro result",
        f"The focused Mindoro provenance lane selected `{selected_recipe}` from the outage-constrained family `{recipe_family}`.",
        "info",
    )
    render_status_callout(
        "How B1 uses it",
        "Mindoro B1 uses the recipe selected by this separate Phase 1 drifter-based provenance rerun. Phase 3B itself does not directly ingest drifters.",
        "info",
    )
    render_status_callout(
        "GFS honesty note",
        "GFS-backed recipes were not part of the focused Mindoro provenance lane while archived access was unavailable, so the focused comparison stayed limited to the recipes that could be run cleanly.",
        "warning",
    )

    render_metric_row(
        [
            ("Selected recipe", selected_recipe or "Not available"),
            ("Focused recipes tested", str(len(summary)) if not summary.empty else "0"),
            ("Accepted segments", str(manifest.get("accepted_segment_count", 0))),
            ("Ranking subset", str(subset_info.get("segment_count", len(subset)) or 0)),
            ("Study window", f"{str(time_window.get('start_utc', ''))[:10]} to {str(time_window.get('end_utc', ''))[:10]}"),
        ]
    )

    tabs = st.tabs(
        [
            "Focused Mindoro provenance lane",
            "How B1 inherits the recipe",
            "Regional reference lane",
            "Legacy 2016 support lane",
            "Source tables",
        ]
    )

    with tabs[0]:
        render_status_callout(
            "Focused lane role",
            "This is the active Mindoro-specific provenance lane used to justify the B1 recipe choice.",
            "info",
        )
        render_table(
            "Focused recipe ranking",
            ranking,
            download_name="phase1_recipe_ranking.csv",
            caption="Stored ranking for the focused Mindoro Phase 1 provenance lane.",
            height=220,
        )
        render_table(
            "Focused recipe summary",
            summary,
            download_name="phase1_recipe_summary.csv",
            caption="Stored summary of the recipe family actually tested for the focused Mindoro provenance lane.",
            height=220,
        )
        render_table(
            "Accepted segments by year",
            _year_counts(accepted),
            download_name="phase1_accepted_segments_by_year.csv",
            caption="Accepted segments are historical drifter windows used for the provenance lane. Near-2023 accepted windows are not present in the current stored registry.",
            height=220,
        )
        render_table(
            "Ranking subset registry",
            subset,
            download_name="phase1_ranking_subset_registry.csv",
            caption="This is the subset used to rank the focused Mindoro recipe family. The stored lane hard-fails if this subset is empty.",
            height=260,
            max_rows=None if ui_state["advanced"] else 20,
        )

    with tabs[1]:
        render_status_callout(
            "Provenance chain",
            f"Focused Phase 1 selected `{selected_recipe}`. The active Mindoro B1 package then inherits that recipe choice through the repo baseline-selection layer.",
            "info",
        )
        st.markdown(
            "\n".join(
                [
                    "### Plain-language study chain",
                    "1. Historical drifter segments were screened in the focused Mindoro window.",
                    f"2. The focused ranking subset compared `{recipe_family}` on accepted February-April starts.",
                    f"3. `{selected_recipe}` ranked best in the stored focused lane.",
                    "4. B1 inherited that recipe for the March 13 -> March 14 primary validation row.",
                ]
            )
        )
        render_markdown_block(
            "Focused ranking-subset report",
            state["phase1_focused_ranking_subset_report"],
            collapsed=not ui_state["advanced"],
        )
        render_markdown_block(
            "Focused baseline candidate",
            state["phase1_focused_baseline_candidate"],
            collapsed=not ui_state["advanced"],
        )

    with tabs[2]:
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
        )
        render_table(
            "Regional reference recipe summary",
            reference_summary,
            download_name="phase1_production_rerun_recipe_summary.csv",
            caption="Stored recipe family summary from the broader regional reference lane.",
            height=220,
        )
        render_markdown_block(
            "Regional reference baseline candidate",
            state["phase1_reference_baseline_candidate"],
            collapsed=True,
        )

    with tabs[3]:
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

    with tabs[4]:
        render_table(
            "Focused loading audit",
            loading_audit,
            download_name="phase1_loading_audit.csv",
            caption="Stored read-only loading audit for the focused provenance lane.",
            height=240,
            max_rows=None if ui_state["advanced"] else 20,
        )
        render_markdown_block(
            "Focused production manifest excerpt",
            f"```json\n{pd.Series(manifest).to_json(indent=2)}\n```" if manifest else "",
            collapsed=True,
        )
