"""Defense / panel review landing page."""

from __future__ import annotations

from pathlib import Path
from typing import Any

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

from ui.evidence_contract import (
    ROLE_ARCHIVE,
    ROLE_COMPARATOR,
    ROLE_CONTEXT,
    ROLE_LEGACY,
    ROLE_THESIS,
    assert_no_archive_leak,
    filter_for_page,
)
from ui.pages.common import (
    render_badge_strip,
    render_export_note,
    render_figure_gallery,
    render_markdown_block,
    render_metric_row,
    render_package_cards,
    render_page_intro,
    render_status_callout,
    render_study_structure_cards,
)


def _records(df: Any) -> list[dict[str, Any]]:
    if df is None or getattr(df, "empty", True):
        return []
    return list(df.to_dict(orient="records"))


def _find_record(df: Any, **matches: Any) -> dict[str, Any] | None:
    for row in _records(df):
        if all(str(row.get(key, "")).strip() == str(value).strip() for key, value in matches.items()):
            return row
    return None


def _float_text(value: Any, digits: int = 4) -> str:
    try:
        return f"{float(value):.{digits}f}"
    except (TypeError, ValueError):
        return "n/a"


def _int_text(value: Any) -> str:
    try:
        return str(int(round(float(value))))
    except (TypeError, ValueError):
        return "n/a"


def render(state: dict, ui_state: dict) -> None:
    export_mode = bool(ui_state.get("export_mode"))
    recommended = filter_for_page(
        state["home_featured_publication_figures"],
        "home",
        advanced=False,
    )
    assert_no_archive_leak(recommended, "home", advanced=False)
    phase1_manifest = state.get("phase1_focused_manifest") or {}
    phase1_ranking = state.get("phase1_focused_recipe_ranking")
    selected_recipe = str(phase1_manifest.get("official_b1_recipe") or "").strip()
    if not selected_recipe and phase1_ranking is not None and not phase1_ranking.empty and "recipe" in phase1_ranking.columns:
        selected_recipe = str(phase1_ranking.iloc[0]["recipe"]).strip()
    selected_recipe = selected_recipe or "cmems_gfs"

    b1_row = _find_record(state.get("mindoro_b1_summary"), branch_id="R1_previous")
    track_a_open = _find_record(state.get("mindoro_comparator_summary"), track_id="R1_previous_reinit_p50")
    track_a_pygnome = _find_record(state.get("mindoro_comparator_summary"), track_id="pygnome_reinit_deterministic")
    dwh_c1 = _find_record(
        state.get("dwh_main_scorecard_final"),
        track_id="C1",
        result_scope="event_corridor",
        model_product_label="OpenDrift deterministic control",
    )
    dwh_c2_p50 = _find_record(
        state.get("dwh_main_scorecard_final"),
        track_id="C2",
        result_scope="event_corridor",
        model_product_label="OpenDrift ensemble p50",
    )
    dwh_c2_p90 = _find_record(
        state.get("dwh_main_scorecard_final"),
        track_id="C2",
        result_scope="event_corridor",
        model_product_label="OpenDrift ensemble p90",
    )
    dwh_c3 = _find_record(
        state.get("dwh_main_scorecard_final"),
        track_id="C3",
        result_scope="event_corridor",
        model_product_label="PyGNOME deterministic comparator",
    )
    oil_light = _find_record(state.get("phase4_budget_summary"), scenario_id="lighter_oil")
    oil_medium = _find_record(state.get("phase4_budget_summary"), scenario_id="fixed_base_medium_heavy_proxy")
    oil_heavy = _find_record(state.get("phase4_budget_summary"), scenario_id="heavier_oil")
    accepted_count = int(phase1_manifest.get("accepted_segment_count") or 65)
    subset_count = int((phase1_manifest.get("ranking_subset") or {}).get("segment_count") or 19)

    render_page_intro(
        "Defense / Panel Review",
        "This dashboard is read-only and displays stored Draft 22 outputs only; it does not rerun science.",
        badge="Draft 22 | read-only thesis review",
    )

    if export_mode:
        render_export_note(
            [
                "Export mode converts this page into a print-friendly defense snapshot of the stored thesis-facing outputs.",
                "The panel-first summary stays ahead of the deeper archive and advanced reproducibility material.",
            ]
        )

    st.info("This dashboard is read-only and displays stored Draft 22 outputs only; it does not rerun science.")
    render_badge_strip([ROLE_THESIS, ROLE_COMPARATOR, ROLE_CONTEXT, ROLE_ARCHIVE, ROLE_LEGACY])
    st.caption("These badges separate the main thesis claim from comparator, support/context, archive, and legacy lanes.")

    render_status_callout(
        "Only Mindoro B1 is the main Philippine validation claim.",
        "Only Mindoro B1 is the main Philippine public-observation validation claim; Track A, DWH, oil-type/shoreline outputs, and legacy/archive outputs have separate support roles.",
        tone="success",
    )
    render_status_callout(
        "Probability-footprint semantics",
        "prob_presence is the cellwise ensemble fraction; mask_p50 is the majority-member likely footprint and preferred probabilistic extension; mask_p90 is the high-confidence core / conservative support product. Probability classes are 0-10%, 10-25%, 25-50%, 50-75%, 75-90%, and >=90%.",
        tone="info",
    )

    st.subheader("Quick panel summary")
    render_metric_row(
        [
            {
                "label": "Phase 1 selected recipe",
                "value": selected_recipe,
                "note": f"{accepted_count} accepted strict segments; {subset_count} February-April ranked segments",
            },
            {
                "label": "Mindoro B1 mean FSS",
                "value": _float_text((b1_row or {}).get("mean_fss"), 4),
                "note": "No exact 1 km overlap",
            },
            {
                "label": "Track A PyGNOME mean FSS",
                "value": _float_text((track_a_pygnome or {}).get("mean_fss"), 4),
                "note": "Comparator-only support",
            },
            {
                "label": "DWH corridor C1 / C2 p50 / C2 p90 / C3",
                "value": " / ".join(
                    [
                        f"C1 {_float_text((dwh_c1 or {}).get('mean_fss'), 4)}",
                        f"C2 p50 {_float_text((dwh_c2_p50 or {}).get('mean_fss'), 4)}",
                        f"C2 p90 {_float_text((dwh_c2_p90 or {}).get('mean_fss'), 4)}",
                        f"C3 {_float_text((dwh_c3 or {}).get('mean_fss'), 4)}",
                    ]
                ),
                "note": "External transfer corridor mean FSS values; C3 remains the PyGNOME comparator.",
            },
            {
                "label": "Phase 4 status",
                "value": "SUPPORT / CONTEXT ONLY",
                "note": "No matched Mindoro Phase 4 PyGNOME fate-and-shoreline package stored.",
                "full_width": True,
            },
        ],
        export_mode=export_mode,
    )

    st.subheader("What each thesis lane means")
    render_study_structure_cards(
        [
            {
                "title": "Phase 1 transport provenance",
                "classification": ROLE_THESIS,
                "body": (
                    f"Focused Mindoro Phase 1 selected `{selected_recipe}` from {accepted_count} strict accepted segments "
                    f"and {subset_count} February-April ranked segments. Drifters support recipe selection, not direct oil-footprint truth."
                ),
                "note": "The official B1 recipe is adopted from the focused historical winner.",
                "page_label": "Phase 1 Transport Provenance",
            },
            {
                "title": "Mindoro B1 summary",
                "classification": ROLE_THESIS,
                "body": (
                    "Main Mindoro validation row: mean FSS {mean_fss}, FSS rises from {fss_1} at 1 km to {fss_10} at 10 km, "
                    "with {forecast_cells} forecast cells against {obs_cells} observed cells."
                ).format(
                    mean_fss=_float_text((b1_row or {}).get("mean_fss"), 4),
                    fss_1=_float_text((b1_row or {}).get("fss_1km"), 4),
                    fss_10=_float_text((b1_row or {}).get("fss_10km"), 4),
                    forecast_cells=_int_text((b1_row or {}).get("forecast_nonzero_cells")),
                    obs_cells=_int_text((b1_row or {}).get("obs_nonzero_cells")),
                ),
                "note": "B1 is the only main Mindoro validation row.",
                "page_label": "Mindoro B1 Primary Validation",
            },
            {
                "title": "Track A comparator-only note",
                "classification": ROLE_COMPARATOR,
                "body": (
                    "OpenDrift Track A reuses the same March 14 target with mean FSS {od_fss}; "
                    "PyGNOME stays comparator-only with mean FSS {py_fss} and nearest distance {py_dist} m."
                ).format(
                    od_fss=_float_text((track_a_open or {}).get("mean_fss"), 4),
                    py_fss=_float_text((track_a_pygnome or {}).get("mean_fss"), 4),
                    py_dist=_float_text((track_a_pygnome or {}).get("nearest_distance_to_obs_m"), 2),
                ),
                "note": "PyGNOME is not observational truth here.",
                "page_label": "Mindoro Track A Comparator Support",
            },
            {
                "title": "DWH external-transfer note",
                "classification": ROLE_THESIS,
                "body": (
                    "DWH is a separate transfer-validation lane: C1 corridor mean FSS {c1}, "
                    "C2 p50 {c2p50}, C2 p90 {c2p90}, and C3 PyGNOME comparator {c3}."
                ).format(
                    c1=_float_text((dwh_c1 or {}).get("mean_fss"), 4),
                    c2p50=_float_text((dwh_c2_p50 or {}).get("mean_fss"), 4),
                    c2p90=_float_text((dwh_c2_p90 or {}).get("mean_fss"), 4),
                    c3=_float_text((dwh_c3 or {}).get("mean_fss"), 4),
                ),
                "note": "DWH is external transfer validation, not Mindoro recalibration.",
                "page_label": "DWH External Transfer Validation",
            },
            {
                "title": "Oil-type support-only note",
                "classification": ROLE_CONTEXT,
                "body": (
                    "Stored support scenarios show final beached percentages of {light}, {medium}, and {heavy}, "
                    "with first shoreline arrival at {arrival} h for all three."
                ).format(
                    light=_float_text((oil_light or {}).get("final_beached_pct"), 2),
                    medium=_float_text((oil_medium or {}).get("final_beached_pct"), 2),
                    heavy=_float_text((oil_heavy or {}).get("final_beached_pct"), 2),
                    arrival=_float_text((oil_light or {}).get("first_shoreline_arrival_h"), 0),
                ),
                "note": "Oil-type and shoreline outputs are support/context only.",
                "page_label": "Mindoro Oil-Type and Shoreline Context",
            },
        ],
        columns_per_row=1 if export_mode else 2,
        export_mode=export_mode,
    )

    st.warning(
        "**Do not overclaim**\n\n"
        "- B1 supports neighborhood-scale usefulness, not exact 1 km reproduction.\n"
        "- PyGNOME is comparator-only.\n"
        "- DWH is external transfer validation, not Mindoro recalibration.\n"
        "- Oil-type and shoreline outputs are support/context only.\n"
        "- Experimental 5,000-element sensitivity runs are not thesis-facing."
    )

    package_lookup = {
        package.get("package_id"): package
        for package in state.get("curated_package_roots", [])
        if package.get("package_id")
    }
    phase1_quick_link = {
        "package_id": "phase1_recipe_selection",
        "label": "Phase 1 focused provenance package",
        "page_label": "Phase 1 Transport Provenance",
        "relative_path": "output/phase1_mindoro_focus_pre_spill_2016_2023",
        "description": "Focused historical drifter provenance lane used to select the official Mindoro B1 recipe before Phase 3B is discussed.",
        "secondary_note": ROLE_THESIS,
        "artifact_count": int(len(state.get("phase1_focused_recipe_summary", []))),
        "button_label": "Open page",
    }
    def _story_package(package_id: str, lane_label: str) -> dict[str, Any] | None:
        package = package_lookup.get(package_id)
        if not package:
            return None
        return {**package, "secondary_note": lane_label, "button_label": "Open page"}

    review_resource_cards: list[dict[str, Any]] = [
        {
            "package_id": "panel_registry",
            "label": "Paper-to-output registry",
            "page_label": "Artifacts / Logs / Registries",
            "relative_path": "docs/PAPER_OUTPUT_REGISTRY.md",
            "description": "Plain-language manuscript-to-output map for tables, figures, and support-only packages.",
            "secondary_note": "Read-only reference",
            "button_label": "Open reference page",
        },
        {
            "package_id": "final_validation_package",
            "label": "Final validation package",
            "page_label": "Artifacts / Logs / Registries",
            "relative_path": "output/final_validation_package",
            "description": "Stored thesis-facing validation summaries and final packaging manifests.",
            "secondary_note": "Packaging only",
            "button_label": "Open reference page",
        },
        {
            "package_id": "figure_package_publication",
            "label": "Publication figure package",
            "page_label": "Artifacts / Logs / Registries",
            "relative_path": "output/figure_package_publication",
            "description": "Current publication and defense figures rebuilt from stored outputs only.",
            "secondary_note": "Packaging only",
            "button_label": "Open reference page",
        },
        {
            "package_id": "final_reproducibility_package",
            "label": "Final reproducibility package",
            "page_label": "Artifacts / Logs / Registries",
            "relative_path": "output/final_reproducibility_package",
            "description": "Synced reproducibility indexes, command references, manifests, and logs.",
            "secondary_note": "Packaging only",
            "button_label": "Open reference page",
        },
    ]
    if len(_records(state.get("panel_review_check_table"))) > 0:
        review_resource_cards.append(
            {
                "package_id": "panel_review_check",
                "label": "Latest panel verification output",
                "page_label": "Artifacts / Logs / Registries",
                "relative_path": "output/panel_review_check",
                "description": "Latest manuscript-to-output verification results written by the panel review checker.",
                "secondary_note": "Read-only output",
                "artifact_count": int(len(_records(state.get("panel_review_check_table")))),
                "button_label": "Open reference page",
            }
        )

    st.subheader("Recommended panel resources")
    st.caption("These are the safest package roots and registry references for defense review.")
    render_package_cards(
        review_resource_cards,
        columns_per_row=1 if export_mode else 2,
        export_mode=export_mode,
    )

    primary_quick_links = [
        phase1_quick_link,
        _story_package("mindoro_b1_final", ROLE_THESIS),
        _story_package("mindoro_comparator", ROLE_COMPARATOR),
        _story_package("dwh_phase3c_final", ROLE_THESIS),
        _story_package("phase4_context_status", ROLE_CONTEXT),
    ]
    primary_quick_links = [package for package in primary_quick_links if package]
    secondary_quick_links = [
        _story_package("mindoro_validation_archive", ROLE_ARCHIVE),
        _story_package("legacy_2016_final", ROLE_LEGACY),
    ]
    secondary_quick_links = [package for package in secondary_quick_links if package]

    st.subheader("Story shortcuts")
    st.caption("These shortcuts keep the defense story in thesis order instead of exposing the full internal workflow first.")
    render_package_cards(
        primary_quick_links,
        columns_per_row=1 if export_mode else 3,
        export_mode=export_mode,
    )

    st.subheader("Archive / Support only")
    st.caption("Archive and legacy outputs stay available for audit, but they are not the first panel-facing evidence chain.")
    render_study_structure_cards(
        [
            {
                "title": "Archive — Mindoro Validation Provenance",
                "classification": ROLE_ARCHIVE,
                "body": "Archived Mindoro rows are centralized here for provenance, audit, and reproducibility.",
                "note": "Not thesis-facing evidence; main paper uses March 13 -> March 14 R1 only.",
                "page_label": "Archive — Mindoro Validation Provenance",
            },
            {
                "title": "Archive — Legacy 2016 Support",
                "classification": ROLE_LEGACY,
                "body": "The 2016 package preserves the legacy support flow and includes support-only comparator context outside the main Mindoro and DWH thesis claims.",
                "note": "Support-only; not main validation evidence.",
                "page_label": "Archive — Legacy 2016 Support",
            },
        ],
        columns_per_row=1 if export_mode else 2,
        export_mode=export_mode,
    )
    if secondary_quick_links:
        render_package_cards(
            secondary_quick_links,
            columns_per_row=1 if export_mode else 2,
            export_mode=export_mode,
        )

    render_figure_gallery(
        recommended,
        title="Featured thesis-facing figures",
        caption="The featured strip stays panel-first: provenance context, B1 primary validation, comparator support, and DWH transfer-validation. Archive and legacy figures remain on their own pages.",
        limit=2 if export_mode else None,
        columns_per_row=1 if export_mode else 2,
        export_mode=export_mode,
        overlay_label="Click to enlarge",
    )

    if ui_state["advanced"] and not export_mode:
        st.subheader("Advanced notes")
        st.caption("Panel mode stops here. Advanced mode keeps the lower-level reproducibility notes available in read-only form.")
        render_markdown_block("Final reproducibility summary", state["final_reproducibility_summary"], collapsed=True, export_mode=export_mode)
        render_markdown_block("Panel review report", state["panel_review_check_markdown"], collapsed=True, export_mode=export_mode)
        render_markdown_block("Publication talking points", state["publication_talking_points"], collapsed=True, export_mode=export_mode)
        render_markdown_block("Publication captions", state["publication_captions"], collapsed=True, export_mode=export_mode)
