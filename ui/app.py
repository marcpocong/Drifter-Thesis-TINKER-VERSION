"""Read-only local dashboard for the thesis workflow outputs."""

from __future__ import annotations

from pathlib import Path

try:
    from ui.bootstrap import discover_branding_assets, ensure_repo_root_on_path
except ModuleNotFoundError:
    import sys

    _UI_DIR = Path(__file__).resolve().parent
    _UI_DIR_TEXT = str(_UI_DIR)
    if _UI_DIR_TEXT not in sys.path:
        sys.path.insert(0, _UI_DIR_TEXT)
    from bootstrap import discover_branding_assets, ensure_repo_root_on_path

ensure_repo_root_on_path(__file__)

import streamlit as st
from PIL import Image

from ui.data_access import build_dashboard_state
from ui.pages import PAGE_BY_ID, visible_page_definitions


APP_TITLE = "Drifter-Validated Oil Spill Forecasting Dashboard"
APP_SUBTITLE = "Read-only thesis dashboard over the curated final packages, publication figures, and synced registries."


LAYER_LABELS = {
    "publication": "Publication package",
    "panel": "Panel gallery",
    "raw": "Raw technical gallery",
}


@st.cache_data(show_spinner=False)
def _load_dashboard_state() -> dict:
    return build_dashboard_state()


def _branding_payload() -> tuple[dict, Image.Image | None]:
    branding = discover_branding_assets(__file__)
    page_icon = None
    page_icon_path = branding.get("page_icon_path")
    if page_icon_path:
        try:
            page_icon = Image.open(page_icon_path)
        except OSError:
            page_icon = None
    return branding, page_icon


def _load_css() -> None:
    css_path = Path(__file__).resolve().parent / "assets" / "style.css"
    if css_path.exists():
        st.markdown(f"<style>{css_path.read_text(encoding='utf-8')}</style>", unsafe_allow_html=True)


def _render_sidebar(state: dict) -> dict:
    with st.sidebar:
        st.markdown("## Drifter-Validated Dashboard")
        st.caption(APP_SUBTITLE)

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
        visible_pages = visible_page_definitions(state, advanced=advanced)
        page_labels = [page.label for page in visible_pages]
        current_page = st.session_state.get("page_selector")
        if current_page not in page_labels:
            st.session_state["page_selector"] = page_labels[0]
        page_label = st.selectbox(
            "Page",
            options=page_labels,
            index=page_labels.index(st.session_state["page_selector"]),
            key="page_selector",
        )

        st.markdown("---")
        st.caption("Read-only scope")
        st.markdown(
            "\n".join(
                [
                    "- Read-only only; no scientific rerun controls are exposed here",
                    "- Curated final packages are the primary browse surfaces",
                    "- Publication figures stay the default layer",
                    "- Raw CASE_* folders remain advanced-only fallback context",
                    "- Support and comparator lanes stay labeled as support and comparator lanes",
                ]
            )
        )

        curated_packages = state.get("curated_package_roots", [])
        st.metric("Curated package roots", len(curated_packages))
        st.metric("Publication figures indexed", len(state["publication_registry"]))
        st.metric("Focused Phase 1 recipes tested", len(state["phase1_focused_recipe_summary"]))

        with st.expander("Read paths", expanded=False):
            read_paths = [
                "output/phase1_mindoro_focus_pre_spill_2016_2023/",
                "output/Phase 3B March13-14 Final Output/",
                "output/Phase 3C DWH Final Output/",
                "output/2016 Legacy Runs FINAL Figures/",
                "output/final_validation_package/",
                "output/final_reproducibility_package/",
                "output/figure_package_publication/",
                "output/phase4/CASE_MINDORO_RETRO_2023/",
                "output/phase4_crossmodel_comparability_audit/",
            ]
            if advanced:
                read_paths.extend(
                    [
                        "output/trajectory_gallery_panel/",
                        "output/trajectory_gallery/",
                        "output/CASE_MINDORO_RETRO_2023/",
                        "output/CASE_DWH_RETRO_2010_72H/",
                    ]
                )
            st.code("\n".join(read_paths), language="text")

    selected_page = next(page for page in visible_pages if page.label == page_label)
    return {
        "advanced": advanced,
        "mode_label": mode_label,
        "visual_layer": visual_layer,
        "page_id": selected_page.page_id,
    }


def main() -> None:
    Image.MAX_IMAGE_PIXELS = None
    branding, page_icon = _branding_payload()
    st.set_page_config(
        page_title=APP_TITLE,
        page_icon=page_icon,
        layout="wide",
        initial_sidebar_state="expanded",
    )
    _load_css()
    if branding.get("has_logo") and hasattr(st, "logo"):
        st.logo(
            str(branding["logo_path"]),
            size="large",
            link="/",
            icon_image=str(branding["icon_path"]) if branding.get("icon_path") else None,
        )
    state = _load_dashboard_state()
    ui_state = _render_sidebar(state)

    st.markdown(
        """
        <div class="hero-card">
          <div class="hero-kicker">Read-only thesis dashboard</div>
          <div class="hero-title">Study structure first, curated packages first, science reruns never</div>
          <div class="hero-text">The dashboard leads with Phase 1 recipe selection, Mindoro B1 primary validation, the Mindoro comparator package, the frozen DWH Phase 3C package, the Mindoro Phase 4 context layer, and the curated legacy 2016 support package. Advanced mode opens registries, manifests, and lower-level figure layers without changing stored outputs.</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    page = PAGE_BY_ID[ui_state["page_id"]]
    try:
        page.renderer(state, ui_state)
    except Exception as exc:
        if ui_state["advanced"]:
            raise
        st.warning(
            "This page could not load one of its optional packaged artifacts. The dashboard is staying in read-only mode and the other pages remain available."
        )
        st.caption(f"Panel-mode detail: {exc}")


if __name__ == "__main__":
    main()
