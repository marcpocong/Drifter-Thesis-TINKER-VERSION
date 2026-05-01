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

from ui.evidence_contract import ROLE_LEGACY, ROLE_SECONDARY
from ui.pages.common import (
    render_badge_strip,
    render_comparator_banner,
    render_export_note,
    render_feature_grid,
    render_figure_gallery,
    render_key_takeaway,
    render_markdown_block,
    render_metric_row,
    render_modern_hero,
    render_section_header,
    render_section_stack,
    render_status_callout,
    render_table,
)


def _legacy_cases_table():
    import pandas as pd

    return pd.DataFrame(
        [
            {"Support case": "CASE_2016-09-01", "Drifter reference date": "2016-09-01", "Comparator window": "2016-09-02 to 2016-09-04", "Thesis role": "Legacy/prototype support only"},
            {"Support case": "CASE_2016-09-06", "Drifter reference date": "2016-09-06", "Comparator window": "2016-09-07 to 2016-09-09", "Thesis role": "Legacy/prototype support only"},
            {"Support case": "CASE_2016-09-17", "Drifter reference date": "2016-09-17", "Comparator window": "2016-09-18 to 2016-09-20", "Thesis role": "Legacy/prototype support only"},
        ]
    )


def _legacy_fss_table():
    import pandas as pd

    return pd.DataFrame(
        [
            {"Case": "2016-09-01", "Surface": "deterministic", "1 km": "0.389", "3 km": "0.498", "5 km": "0.546", "10 km": "0.585", "Overall mean": "0.504"},
            {"Case": "2016-09-01", "Surface": "p50 footprint", "1 km": "0.334", "3 km": "0.433", "5 km": "0.433", "10 km": "0.430", "Overall mean": "0.407"},
            {"Case": "2016-09-01", "Surface": "p90 footprint", "1 km": "0.371", "3 km": "0.482", "5 km": "0.482", "10 km": "0.481", "Overall mean": "0.454"},
            {"Case": "2016-09-06", "Surface": "deterministic", "1 km": "0.433", "3 km": "0.492", "5 km": "0.501", "10 km": "0.508", "Overall mean": "0.483"},
            {"Case": "2016-09-06", "Surface": "p50 footprint", "1 km": "0.425", "3 km": "0.577", "5 km": "0.542", "10 km": "0.520", "Overall mean": "0.516"},
            {"Case": "2016-09-06", "Surface": "p90 footprint", "1 km": "0.470", "3 km": "0.618", "5 km": "0.589", "10 km": "0.571", "Overall mean": "0.562"},
            {"Case": "2016-09-17", "Surface": "deterministic", "1 km": "0.667", "3 km": "0.729", "5 km": "0.729", "10 km": "0.729", "Overall mean": "0.714"},
            {"Case": "2016-09-17", "Surface": "p50 footprint", "1 km": "0.278", "3 km": "0.392", "5 km": "0.360", "10 km": "0.336", "Overall mean": "0.342"},
            {"Case": "2016-09-17", "Surface": "p90 footprint", "1 km": "0.309", "3 km": "0.430", "5 km": "0.396", "10 km": "0.373", "Overall mean": "0.377"},
        ]
    )


def _filter_case(df, case_id: str) -> object:
    if df.empty or case_id == "ALL":
        return df
    if "case_id" not in df.columns:
        return df
    return df.loc[df.get("case_id", "").astype(str).eq(case_id)].reset_index(drop=True)


def render(state: dict, ui_state: dict) -> None:
    export_mode = bool(ui_state.get("export_mode"))
    registry = state["legacy_2016_final_registry"]
    comparator_registry = state["legacy_2016_phase4_comparator_registry"]
    provenance = state.get("legacy_2016_provenance_metadata", {})
    case_options = ["ALL"] + sorted(registry.get("case_id", []).astype(str).unique().tolist()) if not registry.empty else ["ALL"]
    if export_mode or not ui_state["advanced"]:
        selected_case = "ALL"
    else:
        selected_case = st.selectbox(
            "Legacy case",
            options=case_options,
            format_func=lambda value: "All cases" if value == "ALL" else value,
            index=0,
            key="legacy_2016_case_selector",
        )

    render_modern_hero(
        "Secondary 2016 Support",
        "This page surfaces the authoritative curated prototype_2016 package as secondary drifter-track and legacy FSS support. It is not public-spill validation and does not replace the Primary Mindoro March 13-14 validation case or DWH.",
        badge=ROLE_SECONDARY,
        eyebrow="Secondary support lane",
        meta=["2016 drifter-track support", "Legacy FSS support", "Read-only artifacts"],
        tone="legacy",
    )

    if export_mode:
        render_export_note(
            [
                "Export mode shows the legacy package as a single support-only brief across all three 2016 cases.",
                "The Phase 4 comparator section remains budget-only and deterministic, and shoreline comparison is still not packaged.",
            ]
        )

    render_key_takeaway(
        "Secondary 2016 drifter-track and legacy FSS material is support only.",
        "These prototype cases preserve direct drifter-track benchmark support and legacy comparator context, but they do not replace the Primary Mindoro March 13-14 case or the DWH external-transfer validation.",
        tone="legacy",
        badge=ROLE_SECONDARY,
    )
    render_status_callout(
        "Secondary support boundary",
        "Secondary 2016 outputs support drifter-track and legacy OpenDrift-PyGNOME FSS context only. They are not public-spill validation and do not replace the Primary Mindoro March 13-14 validation case or DWH.",
        "legacy",
    )
    render_comparator_banner()
    render_feature_grid(
        [
            {
                "title": "Direct drifter-track benchmark",
                "body": "2016-09-01, 2016-09-06, and 2016-09-17 remain grouped as secondary observed-drifter-track support.",
                "badge": ROLE_SECONDARY,
                "tone": "legacy",
            },
            {
                "title": "Legacy FSS support",
                "body": "OpenDrift versus deterministic PyGNOME FSS material is legacy comparator support only, not observation truth.",
                "badge": ROLE_SECONDARY,
                "tone": "legacy",
            },
            {
                "title": "Legacy/debug posture",
                "body": "Panel-friendly mode shows a broad support gallery; Advanced mode exposes case filtering and deeper registries.",
                "badge": ROLE_LEGACY,
                "tone": "legacy",
            },
        ],
        columns_per_row=3,
        export_mode=export_mode,
    )

    render_status_callout("SECONDARY SUPPORT ONLY - not public-spill validation.", "SECONDARY SUPPORT ONLY - not public-spill validation.", "warning")
    render_status_callout(
        "Visible support flow",
        "The thesis-facing legacy flow is Phase 1 -> Phase 2 -> Phase 3A -> Phase 4 -> Phase 5. Phase 3A is comparator-only OpenDrift vs deterministic PyGNOME support, Phase 4 is legacy weathering/fate, and Phase 5 is this read-only packaging layer.",
        "info",
    )
    render_status_callout(
        "Guardrail",
        "There is no thesis-facing Primary Mindoro or DWH validation claim in prototype_2016, and this lane does not replace the final regional Phase 1 study.",
        "warning",
    )
    if not export_mode and not ui_state["advanced"]:
        render_status_callout(
            "Panel browsing",
            "Panel-friendly mode keeps all currently packaged legacy cases visible together so the page reads as one support gallery. Advanced mode can filter by case when you need a narrower inspection view.",
            "info",
        )

    def _format_box(bounds: object) -> str:
        if not isinstance(bounds, list) or len(bounds) != 4:
            return ""
        formatted: list[str] = []
        for value in bounds:
            try:
                formatted.append(format(float(value), ".4f").rstrip("0").rstrip("."))
            except (TypeError, ValueError):
                formatted.append(str(value))
        return ", ".join(formatted)

    initial_capture_box = provenance.get("prototype_2016_initial_capture_box", [])
    source_boxes = provenance.get("prototype_2016_initial_capture_source_boxes", [])
    initial_capture_box_text = _format_box(initial_capture_box)
    historical_origin_summary = str(provenance.get("historical_origin_summary") or "").strip()
    operative_extent_note = str(provenance.get("operative_extent_note") or "").strip()
    render_status_callout(
        "First-code search context",
        (
            " ".join(part for part in (historical_origin_summary, operative_extent_note) if part)
            if historical_origin_summary
            else (
                f"The very first prototype code used the shared first-code search box [{initial_capture_box_text}] on the "
                "west coast of the Philippines (Palawan-side western Philippine context). Because the ingestion-and-validation "
                "pipeline was still in its early stage, that first code surfaced the first three 2016 drifter cases, and the "
                "team then intentionally kept those three as the first study focus to build the workflow and prove the "
                "pipeline was working. The stored case-local prototype extents remain the operative scientific/display extents."
            )
        )
        if initial_capture_box_text
        else (
            "The historical first-code search metadata is not available in the current package copy. "
            "Stored case-local prototype extents still remain the operative scientific/display extents."
        ),
        "info",
    )
    if source_boxes:
        source_box_lines = "\n".join(
            f"- Source box {index}: [{_format_box(box)}]"
            for index, box in enumerate(source_boxes, start=1)
            if _format_box(box)
        )
        source_box_note = (
            "These source boxes support the historical first-code narrative and did not replace the stored case-local "
            "prototype extents."
        )
        if export_mode:
            render_markdown_block(
                "Historical origin source boxes",
                source_box_lines
                + f"\n\n{source_box_note}",
                collapsed=False,
                export_mode=export_mode,
            )
        else:
            with st.expander("Historical origin source boxes", expanded=False):
                st.markdown(source_box_lines)
                st.caption(source_box_note)

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

    def _package_overview() -> None:
        render_badge_strip([ROLE_SECONDARY])
        metrics = [
            ("Indexed artifacts", str(len(filtered_registry))),
            ("Phase 3A figures", str(len(phase3a_figures))),
            ("Phase 4 figures", str(len(phase4_figures))),
            ("Phase 4 comparator figures", str(len(phase4_comparator_figures))),
            ("Cases", str(len(sorted(registry.get("case_id", []).astype(str).unique().tolist())) if not registry.empty else 0)),
        ]
        render_metric_row(metrics, export_mode=export_mode)
        render_markdown_block(
            "Legacy package README",
            state["legacy_2016_final_readme"],
            collapsed=not ui_state["advanced"] and not export_mode,
            export_mode=export_mode,
        )
        render_table(
            "Table 4.11 - Secondary 2016 direct drifter-track benchmark summary",
            _legacy_cases_table(),
            download_name="legacy_2016_support_cases.csv",
            caption="Legacy/prototype support only; not direct public spill validation.",
            height=160,
            export_mode=export_mode,
        )
        render_table(
            "Table 4.12 - Legacy 2016 OpenDrift-versus-PyGNOME mean FSS by case, support surface, and neighborhood window",
            _legacy_fss_table(),
            download_name="legacy_2016_mean_fss.csv",
            caption="Legacy similarity values are displayed only on this archive/support page.",
            height=260,
            export_mode=export_mode,
        )

    def _phase3a_publication() -> None:
        render_badge_strip([ROLE_SECONDARY])
        render_figure_gallery(
            phase3a_figures,
            title="Phase 3A support-comparison figures",
            caption="These figures come from the curated legacy package and keep the Phase 3A comparator-only OpenDrift vs deterministic PyGNOME framing explicit. Click any figure to enlarge it.",
            limit=3 if export_mode else (None if ui_state["advanced"] else 6),
            columns_per_row=1 if export_mode else 2,
            export_mode=export_mode,
            overlay_label="Click to enlarge",
        )
        render_table(
            "Phase 3A similarity by case",
            _filter_case(state["legacy_2016_phase3a_similarity"], selected_case),
            download_name="prototype_pygnome_similarity_by_case.csv",
            caption="Curated similarity summary from the legacy Phase 3A support package.",
            height=240,
            export_mode=export_mode,
        )

    def _phase4_publication() -> None:
        render_badge_strip([ROLE_SECONDARY])
        render_figure_gallery(
            phase4_figures,
            title="Phase 4 legacy-context figures",
            caption="These figures reuse the stored weathering/fate outputs and shoreline summaries derived from stored CSVs only.",
            limit=3 if export_mode else (None if ui_state["advanced"] else 6),
            columns_per_row=1 if export_mode else 2,
            export_mode=export_mode,
            overlay_label="Click to enlarge",
        )
        render_table(
            "Phase 4 registry",
            _filter_case(state["legacy_2016_phase4_registry"], selected_case),
            download_name="prototype_2016_phase4_registry.csv",
            caption="Phase 4 registry copied into the curated legacy package.",
            height=260,
            export_mode=export_mode,
        )

    def _phase4_comparator() -> None:
        render_badge_strip([ROLE_SECONDARY])
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
        render_figure_gallery(
            phase4_comparator_figures,
            title="Phase 4 comparator-pilot figures",
            caption="These figures stay support-only and comparator-only. They describe cross-model budget differences from the stored prototype_2016 Phase 4 PyGNOME pilot; they are not observational skill products.",
            limit=4 if export_mode else (None if ui_state["advanced"] else 6),
            columns_per_row=1 if export_mode else 2,
            export_mode=export_mode,
            overlay_label="Click to enlarge",
        )
        render_table(
            "Phase 4 comparator registry",
            _filter_case(state["legacy_2016_phase4_comparator_registry"], selected_case),
            download_name="prototype_2016_phase4_pygnome_comparator_registry.csv",
            caption="Registry for the deterministic prototype_2016 Phase 4 PyGNOME comparator pilot artifacts copied into the curated legacy package.",
            height=260,
            export_mode=export_mode,
        )
        render_markdown_block(
            "Phase 4 comparator decision note",
            state["legacy_2016_phase4_comparator_decision_note"],
            collapsed=False,
            export_mode=export_mode,
        )

    def _summaries_and_manifests() -> None:
        render_badge_strip([ROLE_SECONDARY])
        render_table(
            "Legacy final-output registry",
            filtered_registry,
            download_name="prototype_2016_final_output_registry.csv",
            caption="Machine-readable registry for the authoritative curated legacy package.",
            height=320,
            max_rows=None if ui_state["advanced"] else 30,
            export_mode=export_mode,
        )
        render_table(
            "Phase 3A FSS by case/window",
            _filter_case(state["legacy_2016_phase3a_fss"], selected_case),
            download_name="prototype_pygnome_fss_by_case_window.csv",
            caption="Legacy Phase 3A FSS summary copied into the curated package.",
            height=220,
            export_mode=export_mode,
        )
        render_markdown_block("Phase 5 packaging summary", state["legacy_2016_packaging_summary"], collapsed=True, export_mode=export_mode)

    render_section_header(
        "Secondary 2016 Details",
        "Direct drifter-track benchmark support and legacy FSS comparator surfaces remain separated, with raw registries and case-level rows kept behind the story summary.",
        badge=ROLE_SECONDARY,
    )
    render_section_stack(
        [
            ("Package overview", _package_overview),
            ("Direct drifter-track benchmark support", _phase3a_publication),
            ("Phase 4 legacy context", _phase4_publication),
            ("Phase 4 comparator pilot", _phase4_comparator),
            ("Tables and notes", _summaries_and_manifests),
        ],
        export_mode=export_mode,
        use_tabs=ui_state["advanced"],
    )
