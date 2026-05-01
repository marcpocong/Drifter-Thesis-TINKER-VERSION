"""B1 drifter provenance / transport-context page."""

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

from ui.data_access import resolve_repo_path
from ui.evidence_contract import ROLE_ADVANCED
from ui.pages.common import (
    render_export_note,
    render_metric_row,
    render_page_intro,
    render_section_stack,
    render_status_callout,
    render_table,
)


def _clean_text(value: object, *, fallback: str = "Not available") -> str:
    text = str(value or "").strip()
    return text if text else fallback


def _connection_table(context: dict) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"Field": "B1 case", "Value": _clean_text(context.get("b1_case_label"), fallback="March 13 -> March 14")},
            {"Field": "Official B1 recipe", "Value": _clean_text(context.get("official_b1_recipe"))},
            {"Field": "Provenance lane", "Value": _clean_text(context.get("provenance_lane"))},
            {
                "Field": "Ranking subset",
                "Value": _clean_text(context.get("ranking_subset_description") or context.get("ranking_subset_label")),
            },
            {"Field": "Accepted segment count", "Value": str(int(context.get("accepted_segment_count") or 0))},
            {"Field": "Winner", "Value": _clean_text(context.get("winning_recipe"))},
            {"Field": "Claim boundary", "Value": _clean_text(context.get("claim_boundary"))},
        ]
    )


def _ranking_summary_table(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    payload = df.copy()
    recipe_order = ["cmems_gfs", "cmems_era5", "hycom_gfs", "hycom_era5"]
    if "recipe" in payload.columns:
        payload = payload.loc[payload["recipe"].astype(str).isin(recipe_order)].copy()
        payload["_recipe_order"] = payload["recipe"].astype(str).map({recipe: index for index, recipe in enumerate(recipe_order)})
        payload = payload.sort_values(["_recipe_order", "rank"], na_position="last").drop(columns="_recipe_order")
    preferred_columns = [
        "rank",
        "recipe",
        "segment_count",
        "valid_segment_count",
        "mean_ncs_score",
        "median_ncs_score",
        "status",
        "is_gfs_recipe",
    ]
    selected_columns = [column for column in preferred_columns if column in payload.columns]
    return payload[selected_columns].reset_index(drop=True)


def _artifact_discovery_table(context: dict) -> pd.DataFrame:
    rows = [
        {
            "artifact": "Focused Phase 1 manifest",
            "relative_path": _clean_text(context.get("phase1_manifest_source_path"), fallback="Not found"),
        },
        {
            "artifact": "Accepted segment registry",
            "relative_path": _clean_text(context.get("accepted_segments_source_path"), fallback="Not found"),
        },
        {
            "artifact": "Ranking subset registry",
            "relative_path": _clean_text(context.get("ranking_subset_source_path"), fallback="Not found"),
        },
        {
            "artifact": "B1 context manifest",
            "relative_path": _clean_text(context.get("panel_context_manifest_source_path"), fallback="Not found"),
        },
        {
            "artifact": "Stored context map",
            "relative_path": _clean_text(context.get("panel_context_map_figure_path"), fallback="Not found"),
        },
    ]
    return pd.DataFrame(rows)


def _render_map_section(context: dict, *, export_mode: bool) -> None:
    st.subheader("Drifter map")
    st.caption("Historical accepted drifter segments used to select the transport recipe inherited by B1.")

    figure_path = resolve_repo_path(context.get("panel_context_map_figure_path"))
    if figure_path and figure_path.exists():
        try:
            st.image(str(figure_path), width="stretch")
            st.caption("Stored-output-only figure loaded from the local repo.")
        except OSError:
            render_status_callout(
                "Stored map unavailable",
                "The packaged B1 drifter-context map could not be opened. No replacement science is generated in the UI.",
                "warning",
            )
        return

    render_status_callout(
        "Stored map unavailable",
        "No packaged B1 drifter-context map is available in this repo state. The dashboard does not generate a replacement map.",
        "warning",
    )


def render(state: dict, ui_state: dict) -> None:
    export_mode = bool(ui_state.get("export_mode"))
    context = state.get("b1_drifter_context") or {}
    accepted_display = context.get("accepted_segments_display", pd.DataFrame())
    subset_display = context.get("ranking_subset_display", pd.DataFrame())
    recipe_ranking = _ranking_summary_table(context.get("recipe_ranking", pd.DataFrame()))

    render_page_intro(
        "B1 Recipe Provenance — Not Truth Mask",
        "Advanced reference for the focused Phase 1 drifter provenance behind the selected Mindoro B1 transport recipe.",
        badge=ROLE_ADVANCED,
    )

    if export_mode:
        render_export_note(
            [
                "Export mode keeps the drifter-provenance explanation on one sequential page.",
                "The B1 claim boundary stays explicit: drifter provenance supports recipe selection, while public-observation masks remain the validation truth for March 13 -> March 14.",
            ]
        )

    render_status_callout("What the drifter data means here", "Drifter segments support transport-provenance and recipe selection; they are not direct oil-footprint truth.", "info")
    render_status_callout(
        "Required note",
        _clean_text(
            context.get("page_note"),
            fallback=(
                "These drifter records support the selected transport recipe used by B1. "
                "They are not the direct March 13-14 public-observation truth mask for the B1 public-observation validation row."
            ),
        ),
        "warning",
    )

    render_metric_row(
        [
            ("B1 case", _clean_text(context.get("b1_case_label"), fallback="March 13 -> March 14")),
            ("Official B1 recipe", _clean_text(context.get("official_b1_recipe"))),
            ("Accepted segments", str(int(context.get("accepted_segment_count") or 0))),
            ("Ranking subset", str(int(context.get("ranking_subset_count") or 0))),
        ],
        export_mode=export_mode,
    )

    def _connection() -> None:
        render_status_callout(
            "B1 connection",
            "Focused Phase 1 drifter provenance -> selected cmems_gfs recipe -> March 13 -> March 14 B1 public-observation validation.",
            "success",
        )
        st.table(_connection_table(context))

    def _map_and_boundary() -> None:
        _render_map_section(context, export_mode=export_mode)
        render_status_callout(
            "Missing-data boundary panel",
            _clean_text(
                context.get("direct_segment_note"),
                fallback=(
                    "No direct March 13-14 2023 accepted drifter segment is stored for B1. "
                    "The displayed drifter data is the historical focused Phase 1 provenance set used for recipe selection."
                ),
            ),
            "warning",
        )
        direct_accepted = context.get("direct_accepted_segments_display", pd.DataFrame())
        if not direct_accepted.empty:
            render_table(
                "Directly dated March 13-14 drifter records found",
                direct_accepted,
                download_name="b1_direct_march13_14_drifter_records.csv",
                caption="These stored drifter rows are supplementary context only. They are not used here as the B1 validation truth mask.",
                height=220,
                export_mode=export_mode,
            )

    def _tables_and_ranking() -> None:
        render_table(
            "Focused Phase 1 ranking subset",
            subset_display,
            download_name="phase1_focused_ranking_subset.csv",
            caption="Accepted February-April ranking subset used to compare recipes in the focused Mindoro provenance lane.",
            height=260,
            max_rows=25,
            export_mode=export_mode,
        )
        render_table(
            "Focused Phase 1 accepted segment registry",
            accepted_display,
            download_name="phase1_focused_accepted_segments.csv",
            caption="Historical accepted drifter segments used as the broader provenance pool for the focused Mindoro transport-recipe selection lane.",
            height=280,
            max_rows=25,
            export_mode=export_mode,
        )
        render_status_callout(
            "Stored ranking result",
            "cmems_gfs is the selected recipe in the stored focused Phase 1 ranking.",
            "info",
        )
        render_table(
            "Four-recipe ranking summary",
            recipe_ranking,
            download_name="phase1_focused_recipe_ranking.csv",
            caption="Stored focused-lane recipe ranking. This table is reused here as provenance context only; it does not create a new validation claim.",
            height=220,
            export_mode=export_mode,
        )
        if not export_mode:
            with st.expander("Local artifact discovery", expanded=False):
                st.dataframe(_artifact_discovery_table(context), width="stretch", height=210)
                for message in context.get("status_messages", []):
                    st.caption(str(message))

    render_section_stack(
        [
            ("B1 Connection", _connection),
            ("Map And Boundary", _map_and_boundary),
            ("Tables And Ranking", _tables_and_ranking),
        ],
        export_mode=export_mode,
        use_tabs=ui_state["advanced"],
    )
