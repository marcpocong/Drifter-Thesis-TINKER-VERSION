"""DWH external transfer-validation page."""

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

from ui.evidence_contract import ROLE_COMPARATOR, ROLE_CONTEXT, ROLE_THESIS, assert_no_archive_leak, filter_for_page
from ui.pages.common import (
    render_caveat_ribbon,
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


def _dwh_mean_fss_table():
    import pandas as pd

    return pd.DataFrame(
        [
            {"Date/scope": "2010-05-21", "C1 deterministic": "0.4538", "C2 p50": "0.4529", "C2 p90": "0.4930", "C3 PyGNOME comparator": "0.2434"},
            {"Date/scope": "2010-05-22", "C1 deterministic": "0.4808", "C2 p50": "0.5408", "C2 p90": "0.4870", "C3 PyGNOME comparator": "0.2539"},
            {"Date/scope": "2010-05-23", "C1 deterministic": "0.4146", "C2 p50": "0.4727", "C2 p90": "0.4442", "C3 PyGNOME comparator": "0.2532"},
            {"Date/scope": "2010-05-21 to 2010-05-23 corridor", "C1 deterministic": "0.5568", "C2 p50": "0.5389", "C2 p90": "0.4966", "C3 PyGNOME comparator": "0.3612"},
        ]
    )


def _dwh_corridor_table():
    import pandas as pd

    return pd.DataFrame(
        [
            {"Track/row": "C1 deterministic", "Forecast cells": "53980", "Observed cells": "44305", "Area ratio": "1.2184", "Centroid distance": "72547.72 m", "IoU": "0.3362", "Dice": "0.5033", "Corridor mean FSS": "0.5568"},
            {"Track/row": "C2 ensemble p50", "Forecast cells": "51922", "Observed cells": "44305", "Area ratio": "1.1719", "Centroid distance": "94270.47 m", "IoU": "0.3331", "Dice": "0.4997", "Corridor mean FSS": "0.5389"},
            {"Track/row": "C2 ensemble p90", "Forecast cells": "27776", "Observed cells": "44305", "Area ratio": "0.6269", "Centroid distance": "68939.89 m", "IoU": "0.2938", "Dice": "0.4542", "Corridor mean FSS": "0.4966"},
            {"Track/row": "C3 PyGNOME comparator", "Forecast cells": "20639", "Observed cells": "44305", "Area ratio": "0.4658", "Centroid distance": "58867.12 m", "IoU": "0.1903", "Dice": "0.3197", "Corridor mean FSS": "0.3612"},
        ]
    )


def _dwh_subset(df, artifact_groups: set[str]) -> object:
    if df.empty:
        return df
    if "artifact_group" not in df.columns:
        return df
    return df.loc[df.get("artifact_group", "").astype(str).isin(sorted(artifact_groups))].reset_index(drop=True)


def _dwh_name_subset(df, include_terms: tuple[str, ...], exclude_terms: tuple[str, ...] = ()) -> object:
    if df.empty:
        return df
    path_series = df.get("final_relative_path", df.get("relative_path", "")).astype(str).str.lower()
    mask = path_series.apply(lambda value: any(term in value for term in include_terms))
    if exclude_terms:
        mask &= ~path_series.apply(lambda value: any(term in value for term in exclude_terms))
    return df.loc[mask].reset_index(drop=True)


def render(state: dict, ui_state: dict) -> None:
    export_mode = bool(ui_state.get("export_mode"))
    render_modern_hero(
        "DWH External Transfer Validation",
        "DWH is a separate external transfer validation story using public daily observation masks on its own fixed 1 km scoring grid.",
        badge=ROLE_THESIS,
        eyebrow="External transfer validation",
        meta=["DWH only", "Public daily observation masks", "Not Mindoro recalibration"],
        tone="thesis",
    )

    if export_mode:
        render_export_note(
            [
                "Export mode turns the DWH page into a sequential summary of the frozen C1, C2, and C3 tracks.",
                "It keeps DWH separate from the Mindoro drifter-based provenance story and preserves PyGNOME as comparator-only.",
            ]
        )

    render_key_takeaway(
        "DWH tests external transferability; it does not recalibrate Mindoro.",
        "The frozen DWH event-corridor results keep deterministic OpenDrift, ensemble p50, p90 support, and PyGNOME comparator roles separated.",
        tone="thesis",
        badge=ROLE_THESIS,
    )
    render_caveat_ribbon(
        "External-transfer boundary",
        "DWH is a separate external transfer validation story; it does not recalibrate Mindoro. DWH tests external transferability; it does not recalibrate Mindoro. Daily date-composite public observation masks are the truth context, and exact sub-daily acquisition times are not claimed.",
    )
    render_section_header("Event-Corridor Results", "Stored DWH corridor mean FSS values by track.")
    render_feature_grid(
        [
            {
                "title": "C1 deterministic",
                "badge": ROLE_THESIS,
                "body": "OpenDrift deterministic control.",
                "note": "Event-corridor mean FSS 0.5568.",
            },
            {
                "title": "C2 p50 ensemble",
                "badge": ROLE_THESIS,
                "body": "Preferred probabilistic likely footprint.",
                "note": "Event-corridor mean FSS 0.5389.",
            },
            {
                "title": "C2 p90 support",
                "badge": ROLE_CONTEXT,
                "body": "Conservative high-confidence core / support product.",
                "note": "Event-corridor mean FSS 0.4966.",
            },
            {
                "title": "C3 PyGNOME comparator",
                "badge": ROLE_COMPARATOR,
                "body": "PyGNOME is comparator-only and never truth.",
                "note": "Event-corridor mean FSS 0.3612.",
            },
        ],
        columns_per_row=4,
        export_mode=export_mode,
    )
    render_metric_story_grid(
        [
            ("C1 deterministic", "0.5568", "OpenDrift deterministic control"),
            ("C2 p50", "0.5389", "Preferred ensemble footprint"),
            ("C2 p90", "0.4966", "Support product, not broader envelope"),
            ("C3 PyGNOME", "0.3612", "Comparator-only"),
        ],
        export_mode=export_mode,
    )
    render_section_header("Details", "Observation context, track figures, scorecards, and notes remain separated below.")

    registry = filter_for_page(
        state["dwh_final_registry"],
        "dwh_transfer_validation",
        advanced=bool(ui_state.get("advanced")),
    )
    assert_no_archive_leak(registry, "dwh_transfer_validation", advanced=bool(ui_state.get("advanced")))
    truth_figures = _dwh_subset(registry, {"publication/observations"})
    deterministic_figures = _dwh_subset(registry, {"publication/opendrift_deterministic"})
    ensemble_figures = _dwh_subset(registry, {"publication/opendrift_ensemble"})
    comparator_figures = _dwh_subset(registry, {"publication/comparator_pygnome"})
    p50_figures = _dwh_name_subset(ensemble_figures, ("mask_p50_overlay",), ("mask_p50_vs_pygnome_board", "mask_p50_mask_p90_board"))
    ensemble_overview_figures = _dwh_name_subset(
        ensemble_figures,
        (
            "mask_p50_footprint_overview_board",
            "mask_p90_footprint_overview_board",
            "mask_p50_mask_p90_dual_threshold_overview_board",
        ),
    )
    ensemble_comparison_figures = _dwh_name_subset(ensemble_figures, ("mask_p90_overlay", "mask_p50_mask_p90_board"))
    pygnome_truth_figures = _dwh_name_subset(
        comparator_figures,
        ("pygnome_footprint_overlay", "observed_deterministic_mask_p50_pygnome_board"),
        ("_vs_pygnome_board",),
    )
    pygnome_daily_overview_figures = _dwh_name_subset(
        comparator_figures,
        (
            "mask_p50_vs_pygnome_overview_board",
            "mask_p90_vs_pygnome_overview_board",
            "mask_p50_mask_p90_dual_threshold_vs_pygnome_overview_board",
            "mask_p50_mask_p90_vs_pygnome_three_row_overview_board",
        ),
    )
    pygnome_support_figures = _dwh_name_subset(comparator_figures, ("_vs_pygnome_board",))

    def _truth_context() -> None:
        render_status_callout("Observation context", "These figures show the public observation-derived daily masks and the event-corridor union used as truth before any model comparison is discussed.", "info")
        render_table(
            "DWH daily and event-corridor mean FSS",
            _dwh_mean_fss_table(),
            download_name="dwh_mean_fss.csv",
            caption="p50 is the preferred probabilistic footprint; p90 is support/comparison only; PyGNOME is comparator-only.",
            height=190,
            export_mode=export_mode,
        )
        render_table(
            "DWH event-corridor geometry diagnostics",
            _dwh_corridor_table(),
            download_name="dwh_corridor_geometry.csv",
            caption="Corridor summary values for C1, C2 p50, C2 p90, and C3.",
            height=210,
            export_mode=export_mode,
        )
        render_figure_gallery(
            truth_figures,
            title="Observation truth-context figures",
            caption="These figures establish the date-composite truth context first: 24 h = 2010-05-21, 48 h = 2010-05-22, 72 h = 2010-05-23, and the event corridor = 2010-05-21_to_2010-05-23.",
            limit=2 if export_mode else (None if ui_state["advanced"] else 4),
            columns_per_row=1 if export_mode else 2,
            export_mode=export_mode,
            overlay_label="Click to enlarge",
        )

    def _c1_deterministic() -> None:
        render_status_callout("C1 framing", "C1 is the deterministic DWH baseline and the cleanest transfer-validation result to show immediately after the truth-context masks.", "info")
        render_figure_gallery(
            deterministic_figures,
            title="C1 deterministic figures",
            caption="These curated figures keep the deterministic footprint overlays and the daily overview board together as the baseline DWH transfer-validation lane.",
            limit=2 if export_mode else (None if ui_state["advanced"] else 5),
            columns_per_row=1 if export_mode else 2,
            export_mode=export_mode,
            overlay_label="Click to enlarge",
        )
        render_table(
            "C1 deterministic summary",
            state["dwh_deterministic_summary_final"],
            download_name="dwh_transfer_validation_summary.csv",
            caption="Curated DWH deterministic summary from the stored external transfer-validation package.",
            height=240,
            export_mode=export_mode,
        )

    def _c2_p50() -> None:
        render_status_callout("C2 p50", "C2 extends the same frozen DWH truth masks with the preferred probabilistic extension: thresholded mask_p50.", "info")
        render_figure_gallery(
            p50_figures,
            title="C2 mask_p50 figures",
            caption="These figures isolate the preferred mask_p50 overlays before the fuller deterministic-versus-mask_p50-versus-mask_p90 boards, while the official DWH observation-derived masks remain the scoring reference for all displayed scores.",
            limit=2 if export_mode else (None if ui_state["advanced"] else 4),
            columns_per_row=1 if export_mode else 2,
            export_mode=export_mode,
            overlay_label="Click to enlarge",
        )

    def _c2_overview_boards() -> None:
        render_status_callout(
            "C2 overview boards",
            "These daily 24 h / 48 h / 72 h overview boards keep mask_p50, mask_p90, and the exact dual-threshold view visible without changing the official DWH observation-derived scoring reference.",
            "info",
        )
        render_figure_gallery(
            ensemble_overview_figures,
            title="C2 daily ensemble overview boards",
            caption="These overview boards keep the official public observation-derived DWH masks as the scoring reference while surfacing daily mask_p50, mask_p90, and exact dual-threshold views across 24 h, 48 h, and 72 h.",
            limit=2 if export_mode else (None if ui_state["advanced"] else 3),
            columns_per_row=1 if export_mode else 2,
            export_mode=export_mode,
            overlay_label="Click to enlarge",
        )

    def _c2_comparison() -> None:
        render_status_callout("C2 comparison", "These boards keep deterministic as the clean baseline, use mask_p50 as the preferred extension, and retain mask_p90 as support/comparison only.", "info")
        render_figure_gallery(
            ensemble_comparison_figures,
            title="C2 deterministic / mask_p50 / mask_p90 boards",
            caption="These boards and support singles are the thesis-facing place to discuss when the probabilistic extension helps, without forcing a universal ensemble-wins story; the official DWH observation-derived masks remain the scoring reference for every displayed FSS line.",
            limit=2 if export_mode else (None if ui_state["advanced"] else 4),
            columns_per_row=1 if export_mode else 2,
            export_mode=export_mode,
            overlay_label="Click to enlarge",
        )
        render_table(
            "C2 ensemble summary",
            state["dwh_ensemble_summary_final"],
            download_name="dwh_transfer_validation_ensemble_summary.csv",
            caption="Curated DWH ensemble summary from the stored external transfer-validation package.",
            height=240,
            export_mode=export_mode,
        )

    def _c3_pygnome() -> None:
        render_status_callout("C3 framing", "C3 keeps PyGNOME visible as comparator-only against the same observed DWH masks. PyGNOME is never truth.", "warning")
        render_figure_gallery(
            pygnome_truth_figures,
            title="C3 PyGNOME-vs-observed figures",
            caption="These figures keep the PyGNOME footprint overlays and the truth-plus-OpenDrift-plus-PyGNOME boards together under the comparator-only rule, with the official DWH observation-derived masks still serving as the scoring reference.",
            limit=2 if export_mode else (None if ui_state["advanced"] else 4),
            columns_per_row=1 if export_mode else 2,
            export_mode=export_mode,
            overlay_label="Click to enlarge",
        )
        render_table(
            "C3 comparator summary",
            state["dwh_comparator_summary_final"],
            download_name="dwh_pygnome_comparator_summary.csv",
            caption="Curated comparator summary from the stored DWH external transfer-validation package.",
            height=240,
            export_mode=export_mode,
        )

    def _daily_pygnome_overview() -> None:
        render_status_callout(
            "Daily overview comparison",
            "These daily overview boards now include both 2 x 3 and 3 x 3 layouts: the 2 x 3 boards put the requested OpenDrift ensemble view on the top row and PyGNOME on the bottom row, while the 3 x 3 board stacks mask_p50 on top, mask_p90 in the middle, and PyGNOME on the bottom. All displayed scores still use the official DWH observation-derived masks as the scoring reference, and PyGNOME remains comparator-only.",
            "info",
        )
        render_figure_gallery(
            pygnome_daily_overview_figures,
            title="OpenDrift-vs-PyGNOME daily overview boards",
            caption="These daily 24 h / 48 h / 72 h overview boards compare mask_p50, mask_p90, the exact dual-threshold view, and the new three-row mask_p50 / mask_p90 / PyGNOME board without reclassifying PyGNOME as truth; all displayed scores still refer to the official DWH observation-derived masks.",
            limit=2 if export_mode else (None if ui_state["advanced"] else 4),
            columns_per_row=1 if export_mode else 2,
            export_mode=export_mode,
            overlay_label="Click to enlarge",
        )

    def _support_boards() -> None:
        render_status_callout("Support boards", "These boards keep the OpenDrift-versus-PyGNOME comparison visible as support material after the truth-first validation story has already been established.", "info")
        render_figure_gallery(
            pygnome_support_figures,
            title="OpenDrift-vs-PyGNOME support boards",
            caption="These event-corridor boards isolate deterministic-versus-PyGNOME and mask_p50-versus-PyGNOME without reclassifying PyGNOME as truth; the official DWH observation-derived masks remain the scoring reference for the displayed FSS values.",
            limit=2 if export_mode else (None if ui_state["advanced"] else 3),
            columns_per_row=1 if export_mode else 2,
            export_mode=export_mode,
            overlay_label="Click to enlarge",
        )

    def _tables_and_notes() -> None:
        render_table(
            "DWH transfer-validation main scorecard",
            state["dwh_main_scorecard_final"],
            download_name="dwh_transfer_validation_main_scorecard.csv",
            caption="This scorecard reuses the stored DWH final-validation rows across C1, C2, and C3 without recomputing the science.",
            height=260,
            export_mode=export_mode,
        )
        render_markdown_block("Interpretation note", state["dwh_interpretation_note_final"], collapsed=not ui_state["advanced"], export_mode=export_mode)
        render_markdown_block("Output-matrix decision note", state["dwh_output_matrix_decision_note_final"], collapsed=True, export_mode=export_mode)
        render_table(
            "Comparator results table",
            state["dwh_all_results_final"],
            download_name="dwh_pygnome_comparator_all_results_table.csv",
            caption="Curated DWH all-results table for advanced inspection.",
            height=260,
            export_mode=export_mode,
        )
        if ui_state["advanced"]:
            render_markdown_block("DWH stored export note", state["dwh_final_readme"], collapsed=True, export_mode=export_mode)

    render_section_stack(
        [
            ("Observation truth context", _truth_context),
            ("C1 deterministic baseline", _c1_deterministic),
            ("C2 p50 extension", _c2_p50),
            ("C2 overview boards", _c2_overview_boards),
            ("C2 deterministic vs p50 vs p90", _c2_comparison),
            ("C3 PyGNOME vs observed", _c3_pygnome),
            ("OpenDrift vs PyGNOME daily overview", _daily_pygnome_overview),
            ("OpenDrift vs PyGNOME support", _support_boards),
            ("Tables and notes", _tables_and_notes),
        ],
        export_mode=export_mode,
        use_tabs=ui_state["advanced"],
    )
