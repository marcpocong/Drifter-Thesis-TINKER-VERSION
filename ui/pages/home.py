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
    render_caveat_ribbon,
    render_evidence_path,
    render_export_note,
    render_feature_grid,
    render_figure_gallery,
    render_key_takeaway,
    render_markdown_block,
    render_metric_story_grid,
    render_modern_hero,
    render_package_cards,
    render_section_header,
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

    render_modern_hero(
        "Defense / Panel Review",
        "This dashboard displays stored thesis-facing outputs only and does not rerun science.",
        badge="Read-only thesis review",
        eyebrow="Thesis defense landing page",
        meta=["Stored outputs only", "No science reruns", "Panel-friendly by default"],
        tone="thesis",
    )

    if export_mode:
        render_export_note(
            [
                "Export mode converts this page into a print-friendly defense snapshot of the stored thesis-facing outputs.",
                "The panel-first summary stays ahead of the deeper archive and advanced reproducibility material.",
            ]
        )

    render_evidence_path(
        [
            {
                "title": "Transport provenance",
                "badge": ROLE_THESIS,
                "body": "Focused Phase 1 selects the transport recipe; drifters are not oil-footprint truth.",
            },
            {
                "title": "Mindoro B1 validation",
                "badge": ROLE_THESIS,
                "body": "The only main Philippine public-observation validation claim.",
            },
            {
                "title": "Track A comparator",
                "badge": ROLE_COMPARATOR,
                "body": "Same-case OpenDrift vs PyGNOME support; not a second validation row.",
            },
            {
                "title": "DWH transfer validation",
                "badge": ROLE_THESIS,
                "body": "Separate external transfer validation; not Mindoro recalibration.",
            },
            {
                "title": "Oil-type / shoreline context",
                "badge": ROLE_CONTEXT,
                "body": "Downstream consequence support only.",
            },
            {
                "title": "Archive / legacy support",
                "badge": ROLE_LEGACY,
                "body": "Preserved for audit and historical context only.",
            },
        ],
        title="Defense Path",
        caption="A panel-first pathway from recipe provenance to the main claim, then comparator, transfer, context, and archive lanes.",
        export_mode=export_mode,
    )

    render_section_header(
        "Panel Answers In 90 Seconds",
        "The first questions a defense panel is likely to ask, answered before the detailed galleries and registries.",
    )
    render_feature_grid(
        [
            {
                "title": "What is the main Philippine validation claim?",
                "badge": ROLE_THESIS,
                "body": "Mindoro B1 is the only main Philippine public-observation validation claim.",
                "note": "Everything else has a bounded support role.",
            },
            {
                "title": "What does B1 prove and not prove?",
                "badge": ROLE_THESIS,
                "body": "B1 supports coastal-neighborhood usefulness and does not prove exact 1 km grid-cell reproduction.",
                "note": "The March 13-14 pair shares March 12 WorldView-3 imagery provenance, so it remains a bounded reinitialization-based check.",
            },
            {
                "title": "What is Track A?",
                "badge": ROLE_COMPARATOR,
                "body": "Track A is same-case OpenDrift versus PyGNOME comparator support on the March 14 target.",
                "note": "PyGNOME is never observational truth here.",
            },
            {
                "title": "What does DWH add?",
                "badge": ROLE_THESIS,
                "body": "DWH adds a separate external transfer-validation lane using public daily observation masks.",
                "note": "It does not recalibrate Mindoro.",
            },
        ],
        columns_per_row=4,
        export_mode=export_mode,
    )

    render_section_header(
        "Quick Panel Metrics",
        "Headline values are pulled from the existing stored dashboard state and are not recomputed by the UI.",
    )
    render_metric_story_grid(
        [
            {
                "label": "Selected recipe",
                "value": selected_recipe,
                "note": f"{accepted_count} accepted strict segments; {subset_count} February-April ranked segments",
            },
            {
                "label": "B1 mean FSS",
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
                "label": "Oil-type/shoreline status",
                "value": "SUPPORT / CONTEXT ONLY",
                "note": "No matched Mindoro PyGNOME fate-and-shoreline package stored.",
                "full_width": True,
            },
        ],
        export_mode=export_mode,
    )

    render_key_takeaway(
        "Main claim boundary",
        "\n".join(
            [
                "- Only Mindoro B1 is the main Philippine validation claim.",
                "- B1 supports coastal-neighborhood usefulness, not exact 1 km overlap.",
                "- Track A, DWH, oil-type/shoreline, and legacy/archive outputs have separate support roles.",
            ]
        ),
        tone="thesis",
        badge=ROLE_THESIS,
    )

    render_section_header(
        "Probability Semantics",
        "Three footprint terms appear throughout the dashboard. These labels must stay fixed across panel and export views.",
    )
    render_feature_grid(
        [
            {
                "title": "prob_presence",
                "badge": ROLE_CONTEXT,
                "body": "Cellwise ensemble fraction.",
                "note": "Displayed as probability classes: 0-10%, 10-25%, 25-50%, 50-75%, 75-90%, and >=90%.",
            },
            {
                "title": "mask_p50",
                "badge": ROLE_THESIS,
                "body": "Preferred likely footprint / majority-member surface.",
                "note": "This is the preferred probabilistic extension.",
            },
            {
                "title": "mask_p90",
                "badge": ROLE_COMPARATOR,
                "body": "Conservative high-confidence core / support product.",
                "note": "Never relabel mask_p90 as a broader envelope.",
            },
        ],
        columns_per_row=3,
        export_mode=export_mode,
    )
    render_caveat_ribbon("mask_p90 boundary", "Never relabel mask_p90 as a broader envelope.")

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
        "description": "Focused historical drifter provenance lane used to select the official Mindoro B1 recipe before the B1 validation result is discussed.",
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

    if primary_quick_links:
        render_section_header(
            "Open The Evidence Pages",
            "Panel-friendly shortcuts keep the defense path in order and avoid raw technical artifact browsing.",
        )
        render_package_cards(
            primary_quick_links,
            columns_per_row=1 if export_mode else 3,
            export_mode=export_mode,
        )

    archive_panel_cards = [
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
    ]
    if ui_state["advanced"] and not export_mode and secondary_quick_links:
        render_section_header(
            "Archive And Legacy Support",
            "Archive and legacy pages stay available for audit, but they remain explicitly outside the main validation claim.",
            badge=ROLE_ARCHIVE,
        )
        render_feature_grid(archive_panel_cards, columns_per_row=2, export_mode=export_mode)
        render_package_cards(
            secondary_quick_links,
            columns_per_row=1 if export_mode else 2,
            export_mode=export_mode,
        )

    if not recommended.empty:
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
        render_section_header(
            "Read-only Package References",
            "Advanced mode exposes registry and package-reference shortcuts without changing stored outputs.",
            badge="Advanced technical reference",
        )
        render_package_cards(
            review_resource_cards,
            columns_per_row=2,
            export_mode=export_mode,
        )
        render_section_header(
            "Advanced Notes",
            "Panel mode stops before these lower-level reproducibility notes.",
            badge="Advanced technical reference",
        )
        render_markdown_block("Final reproducibility summary", state["final_reproducibility_summary"], collapsed=True, export_mode=export_mode)
        render_markdown_block("Panel review report", state["panel_review_check_markdown"], collapsed=True, export_mode=export_mode)
        render_markdown_block("Publication talking points", state["publication_talking_points"], collapsed=True, export_mode=export_mode)
        render_markdown_block("Publication captions", state["publication_captions"], collapsed=True, export_mode=export_mode)
