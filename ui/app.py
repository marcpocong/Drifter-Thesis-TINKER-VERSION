"""Read-only local dashboard for the thesis workflow outputs."""

from __future__ import annotations

from pathlib import Path

import streamlit as st
from PIL import Image

from ui.data_access import build_dashboard_state
from ui.pages import PAGE_BY_ID, PAGE_DEFINITIONS


LAYER_LABELS = {
    "publication": "Publication package",
    "panel": "Panel gallery",
    "raw": "Raw technical gallery",
}


@st.cache_data(show_spinner=False)
def _load_dashboard_state() -> dict:
    return build_dashboard_state()


def _load_css() -> None:
    css_path = Path(__file__).resolve().parent / "assets" / "style.css"
    if css_path.exists():
        st.markdown(f"<style>{css_path.read_text(encoding='utf-8')}</style>", unsafe_allow_html=True)


def _render_sidebar(state: dict) -> dict:
    with st.sidebar:
        st.header("Explorer")
        st.caption("Read-only dashboard over stored outputs, figures, manifests, and logs.")

        mode_label = st.radio(
            "Viewing mode",
            options=["Panel-friendly", "Advanced"],
            index=0,
            key="view_mode_selector",
        )
        advanced = mode_label == "Advanced"
        layer_options = ["publication"] if not advanced else ["publication", "panel", "raw"]
        visual_layer = st.selectbox(
            "Visual layer",
            options=layer_options,
            format_func=lambda value: LAYER_LABELS[value],
            index=0,
            key="visual_layer_selector",
        )
        page_label = st.selectbox(
            "Page",
            options=[page.label for page in PAGE_DEFINITIONS],
            index=0,
            key="page_selector",
        )

        st.markdown("---")
        st.caption("Current guardrails")
        st.markdown(
            "\n".join(
                [
                    "- Read-only only in this first UI version",
                    "- Publication figures are the default layer",
                    "- Phase 4 cross-model comparison is surfaced as deferred",
                    "- No scientific rerun controls are exposed here",
                ]
            )
        )

        phase_status = state["phase_status"]
        reportable_now = int(phase_status["reportable_now"].fillna(False).astype(bool).sum()) if not phase_status.empty else 0
        st.metric("Reportable tracks visible", reportable_now)
        st.metric("Publication figures indexed", len(state["publication_registry"]))

        with st.expander("Read paths", expanded=False):
            st.code(
                "\n".join(
                    [
                        "output/final_validation_package/",
                        "output/final_reproducibility_package/",
                        "output/trajectory_gallery/",
                        "output/trajectory_gallery_panel/",
                        "output/figure_package_publication/",
                        "output/phase4/",
                        "output/phase4_crossmodel_comparability_audit/",
                    ]
                ),
                language="text",
            )

    selected_page = next(page for page in PAGE_DEFINITIONS if page.label == page_label)
    return {
        "advanced": advanced,
        "mode_label": mode_label,
        "visual_layer": visual_layer,
        "page_id": selected_page.page_id,
    }


def main() -> None:
    Image.MAX_IMAGE_PIXELS = None
    st.set_page_config(
        page_title="Oil Spill Validation Dashboard",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    _load_css()
    state = _load_dashboard_state()
    ui_state = _render_sidebar(state)

    st.markdown(
        """
        <div class="hero-card">
          <div class="hero-kicker">Phase 5 read-only dashboard</div>
          <div class="hero-title">Explore the current reportable workflow without touching the scientific runs</div>
          <div class="hero-text">Panel-friendly mode leads with publication-grade figures and plain-language interpretation. Advanced mode opens the lower-level manifests, logs, and archive figure layers.</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    page = PAGE_BY_ID[ui_state["page_id"]]
    page.renderer(state, ui_state)


if __name__ == "__main__":
    main()
