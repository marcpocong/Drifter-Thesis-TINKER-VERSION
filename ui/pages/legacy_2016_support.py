"""Legacy 2016 support-package page."""

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

from src.core.artifact_status import get_artifact_status
from ui.pages.common import (
    render_figure_cards,
    render_markdown_block,
    render_metric_row,
    render_page_intro,
    render_status_callout,
    render_table,
)


def _filter_case(df, case_id: str) -> object:
    if df.empty or case_id == "ALL":
        return df
    if "case_id" not in df.columns:
        return df
    return df.loc[df.get("case_id", "").astype(str).eq(case_id)].reset_index(drop=True)


def render(state: dict, ui_state: dict) -> None:
    support_status = get_artifact_status("prototype_2016_support")
    registry = state["legacy_2016_final_registry"]
    comparator_registry = state["legacy_2016_phase4_comparator_registry"]
    case_options = ["ALL"] + sorted(registry.get("case_id", []).astype(str).unique().tolist()) if not registry.empty else ["ALL"]
    selected_case = st.selectbox(
        "Legacy case",
        options=case_options,
        format_func=lambda value: "All cases" if value == "ALL" else value,
        index=0,
        key="legacy_2016_case_selector",
    )

    render_page_intro(
        "Legacy 2016 Support Package",
        "This page surfaces the authoritative curated prototype_2016 package. It is support-only legacy material and should be read as a thesis-facing packaging layer for historical pipeline development, not as the main Mindoro or DWH validation evidence.",
        badge="prototype_2016 | support-only legacy package",
    )

    render_status_callout("Lane status", support_status.panel_text, "warning")
    render_status_callout(
        "Visible support flow",
        "The thesis-facing legacy flow is Phase 1 -> Phase 2 -> Phase 3A -> Phase 4 -> Phase 5. Phase 3A is comparator-only OpenDrift vs deterministic PyGNOME support, Phase 4 is legacy weathering/fate, and Phase 5 is this read-only packaging layer.",
        "info",
    )
    render_status_callout(
        "Guardrail",
        "There is no thesis-facing Phase 3B or Phase 3C in prototype_2016, and this lane does not replace the final regional Phase 1 study.",
        "warning",
    )

    filtered_registry = _filter_case(registry, selected_case)
    phase3a_figures = filtered_registry.loc[
        filtered_registry.get("phase_group", "").astype(str).eq("phase3a")
        & filtered_registry.get("final_relative_path", "").astype(str).str.contains(r"publication/phase3a/", case=False, na=False)
    ].reset_index(drop=True)
    phase4_figures = filtered_registry.loc[
        filtered_registry.get("phase_group", "").astype(str).eq("phase4")
        & filtered_registry.get("final_relative_path", "").astype(str).str.contains(r"publication/phase4/", case=False, na=False)
    ].reset_index(drop=True)
    phase4_comparator_figures = filtered_registry.loc[
        filtered_registry.get("phase_group", "").astype(str).eq("phase4_comparator")
        & filtered_registry.get("final_relative_path", "").astype(str).str.contains(r"publication/phase4_comparator/", case=False, na=False)
    ].reset_index(drop=True)
    scenario_order = {"light": 0, "base": 1, "heavy": 2}
    scenario_keys = sorted(
        {
            str(value).strip()
            for value in comparator_registry.get("scenario_key", []).astype(str).tolist()
            if str(value).strip()
        },
        key=lambda value: (scenario_order.get(value, 99), value),
    ) if not comparator_registry.empty and "scenario_key" in comparator_registry.columns else []
    scenario_text = ", ".join(scenario_keys) if scenario_keys else "no comparator scenarios"

    tabs = st.tabs(
        [
            "Package overview",
            "Phase 3A publication",
            "Phase 4 publication",
            "Phase 4 comparator",
            "Summaries and manifests",
        ]
    )

    with tabs[0]:
        metrics = [
            ("Indexed artifacts", str(len(filtered_registry))),
            ("Phase 3A figures", str(len(phase3a_figures))),
            ("Phase 4 figures", str(len(phase4_figures))),
            ("Phase 4 comparator figures", str(len(phase4_comparator_figures))),
            ("Cases", str(len(sorted(registry.get("case_id", []).astype(str).unique().tolist())) if not registry.empty else 0)),
        ]
        render_metric_row(metrics)
        render_markdown_block("Legacy package README", state["legacy_2016_final_readme"], collapsed=False)

    with tabs[1]:
        render_figure_cards(
            phase3a_figures,
            title="Phase 3A publication figures",
            caption="These figures come from the curated legacy package and keep the Phase 3A comparator-only OpenDrift vs deterministic PyGNOME framing explicit.",
            limit=None if ui_state["advanced"] else 6,
            compact_selector=not ui_state["advanced"],
            selector_key="legacy_phase3a_figures",
        )
        render_table(
            "Phase 3A similarity by case",
            _filter_case(state["legacy_2016_phase3a_similarity"], selected_case),
            download_name="prototype_pygnome_similarity_by_case.csv",
            caption="Curated similarity summary from the legacy Phase 3A support package.",
            height=240,
        )

    with tabs[2]:
        render_figure_cards(
            phase4_figures,
            title="Phase 4 publication figures",
            caption="These figures reuse the stored weathering/fate outputs and shoreline summaries derived from stored CSVs only.",
            limit=None if ui_state["advanced"] else 6,
            compact_selector=not ui_state["advanced"],
            selector_key="legacy_phase4_figures",
        )
        render_table(
            "Phase 4 registry",
            _filter_case(state["legacy_2016_phase4_registry"], selected_case),
            download_name="prototype_2016_phase4_registry.csv",
            caption="Phase 4 registry copied into the curated legacy package.",
            height=260,
        )

    with tabs[3]:
        render_status_callout(
            "Comparator scope",
            f"Budget-only deterministic PyGNOME comparator pilot. Currently packaged scenarios: {scenario_text}. Shoreline comparison is not packaged because matched PyGNOME shoreline outputs are not available.",
            "info",
        )
        render_status_callout(
            "Interpretation note",
            "Some cross-model budget differences are large, so this pilot remains support-only and should be discussed as legacy comparator context rather than validation evidence.",
            "warning",
        )
        render_figure_cards(
            phase4_comparator_figures,
            title="Phase 4 comparator figures",
            caption="These figures stay support-only and comparator-only. They describe cross-model budget differences from the stored prototype_2016 Phase 4 PyGNOME pilot; they are not observational skill products.",
            limit=None if ui_state["advanced"] else 6,
            compact_selector=not ui_state["advanced"],
            selector_key="legacy_phase4_comparator_figures",
        )
        render_table(
            "Phase 4 comparator registry",
            _filter_case(state["legacy_2016_phase4_comparator_registry"], selected_case),
            download_name="prototype_2016_phase4_pygnome_comparator_registry.csv",
            caption="Registry for the deterministic prototype_2016 Phase 4 PyGNOME comparator pilot artifacts copied into the curated legacy package.",
            height=260,
        )
        render_markdown_block(
            "Phase 4 comparator decision note",
            state["legacy_2016_phase4_comparator_decision_note"],
            collapsed=False,
        )

    with tabs[4]:
        render_table(
            "Legacy final-output registry",
            filtered_registry,
            download_name="prototype_2016_final_output_registry.csv",
            caption="Machine-readable registry for the authoritative curated legacy package.",
            height=320,
            max_rows=None if ui_state["advanced"] else 30,
        )
        render_table(
            "Phase 3A FSS by case/window",
            _filter_case(state["legacy_2016_phase3a_fss"], selected_case),
            download_name="prototype_pygnome_fss_by_case_window.csv",
            caption="Legacy Phase 3A FSS summary copied into the curated package.",
            height=220,
        )
        render_markdown_block("Phase 5 packaging summary", state["legacy_2016_packaging_summary"], collapsed=True)
