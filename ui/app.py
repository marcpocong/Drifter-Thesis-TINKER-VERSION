"""Read-only local dashboard for the thesis workflow outputs."""

from __future__ import annotations

import base64
import os
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
from streamlit import config as st_config
from PIL import Image

from ui.data_access import build_dashboard_state, dashboard_state_signature
from ui.pages import PageDefinition, visible_page_definitions
from ui.pages.common import render_dashboard_read_only_banner, render_status_callout


APP_TITLE = "Drifter-Validated Oil Spill Forecasting"
APP_SUBTITLE = "Read-only final-paper dashboard over curated packages, publication figures, and synced registries."
SIDEBAR_SUBTITLE = "Read-only thesis dashboard"


LAYER_LABELS = {
    "publication": "Publication package",
    "panel": "Panel gallery",
    "raw": "Raw technical gallery",
}


def _strict_errors_enabled() -> bool:
    return str(os.environ.get("UI_STRICT_ERRORS", "")).strip().lower() in {"1", "true", "yes", "on"}


@st.cache_data(show_spinner=False)
def _load_dashboard_state(_cache_token: str) -> dict:
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
        font_css = _inline_font_face_css()
        st.markdown(f"<style>{font_css}\n{css_path.read_text(encoding='utf-8')}</style>", unsafe_allow_html=True)


def _apply_streamlit_branding(branding: dict) -> None:
    logo_path = branding.get("logo_path")
    if not logo_path or not hasattr(st, "logo"):
        return
    logo_args = {"link": "/"}
    icon_path = branding.get("icon_path")
    if icon_path:
        logo_args["icon_image"] = str(icon_path)
    try:
        st.logo(str(logo_path), **logo_args)
    except Exception:
        # Branding should never be able to break the read-only dashboard shell.
        return


def _asset_mime_type(path: Path) -> str:
    suffix = path.suffix.lower()
    return {
        ".png": "image/png",
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".svg": "image/svg+xml",
        ".woff2": "font/woff2",
        ".woff": "font/woff",
        ".ttf": "font/ttf",
        ".otf": "font/otf",
    }.get(suffix, "application/octet-stream")


def _asset_data_uri(path: str | Path) -> str:
    asset_path = Path(path).resolve()
    encoded = base64.b64encode(asset_path.read_bytes()).decode("ascii")
    return f"data:{_asset_mime_type(asset_path)};base64,{encoded}"


def _inline_font_face_css() -> str:
    fonts_dir = Path(__file__).resolve().parent / "assets" / "fonts"
    font_specs = [
        ("Ideal Sans", "IdealSans-Regular", 400, "normal"),
        ("Ideal Sans", "IdealSans-Bold", 700, "normal"),
    ]
    extension_map = {
        ".woff2": "woff2",
        ".woff": "woff",
        ".ttf": "truetype",
        ".otf": "opentype",
    }
    blocks: list[str] = []
    for family_name, filename_stem, weight, style in font_specs:
        for extension, format_name in extension_map.items():
            candidate = fonts_dir / f"{filename_stem}{extension}"
            if not candidate.exists():
                continue
            blocks.append(
                (
                    "@font-face {"
                    f"font-family: '{family_name}';"
                    f"src: url('{_asset_data_uri(candidate)}') format('{format_name}');"
                    f"font-weight: {weight};"
                    f"font-style: {style};"
                    "font-display: swap;"
                    "}"
                )
            )
            break
    return "\n".join(blocks)


def _render_sidebar_branding(branding: dict) -> None:
    title = APP_TITLE if branding.get("has_logo") else "Drifter-Validated Oil Spill Forecasting"
    subtitle = (
        "Panel mode follows the final paper story: provenance, Primary Mindoro March 13-14, same-case comparator, DWH, then context and secondary lanes."
        if branding.get("has_logo")
        else SIDEBAR_SUBTITLE
    )
    st.markdown(
        (
            "<div class='sidebar-brand'>"
            "<div class='sidebar-brand__eyebrow'>Read-only thesis dashboard</div>"
            f"<div class='sidebar-brand__title'>{title}</div>"
            f"<div class='sidebar-brand__subtitle'>{subtitle}</div>"
            "</div>"
        ),
        unsafe_allow_html=True,
    )


def _sidebar_section_modifier(section: str) -> str:
    key = section.strip().lower()
    if "main" in key or "final" in key or "evidence" in key:
        return "main"
    if "archive" in key or "provenance" in key:
        return "archive"
    if "advanced" in key:
        return "advanced"
    if "reference" in key or "governance" in key:
        return "reference"
    return "neutral"


def _truthy_query_param(value: object) -> bool:
    if isinstance(value, list):
        return any(_truthy_query_param(item) for item in value)
    return str(value or "").strip().lower() in {"1", "true", "yes", "on", "print", "pdf"}


def _export_mode_from_query_params(query_params: object) -> bool:
    try:
        value = query_params.get("export")
    except Exception:
        return False
    return _truthy_query_param(value)


def _load_export_css() -> None:
    st.markdown(
        """
        <style>
        [data-testid="stSidebar"],
        [data-testid="stSidebarNav"],
        [data-testid="stToolbar"],
        [data-testid="stStatusWidget"],
        button[kind],
        [data-testid="stDownloadButton"],
        [data-testid="stExpanderToggleIcon"] {
          display: none !important;
        }
        .main .block-container {
          max-width: 1180px !important;
          padding-top: 1rem !important;
          padding-left: 1.25rem !important;
          padding-right: 1.25rem !important;
        }
        .hero-card,
        .page-hero,
        .export-note {
          break-inside: avoid;
          page-break-inside: avoid;
        }
        .stTabs [data-baseweb="tab-list"] {
          display: none !important;
        }
        .stCode pre {
          white-space: pre-wrap !important;
          word-break: break-word !important;
        }
        @media print {
          [data-testid="stSidebar"],
          [data-testid="stSidebarNav"],
          [data-testid="stToolbar"],
          [data-testid="stStatusWidget"],
          button[kind],
          [data-testid="stDownloadButton"] {
            display: none !important;
          }
          .main .block-container {
            max-width: 100% !important;
            padding: 0.5rem 0.75rem !important;
          }
          h1, h2, h3, h4, img, .hero-card, .page-hero, .export-note {
            break-inside: avoid;
            page-break-inside: avoid;
          }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _render_sidebar_controls(state: dict, branding: dict) -> dict:
    with st.sidebar:
        _render_sidebar_branding(branding)
        st.markdown(
            "<div class='sidebar-note'>Curated outputs only. This UI never reruns science. Dashboard pages organize stored outputs only and do not create new scientific results.</div>",
            unsafe_allow_html=True,
        )
        st.markdown("<div class='sidebar-section-label sidebar-section-label--control'>Viewing Mode</div>", unsafe_allow_html=True)

        mode_label = st.radio(
            "Viewing mode",
            options=["Panel-friendly", "Advanced"],
            index=0,
            key="view_mode_selector",
        )
        advanced = mode_label == "Advanced"
        if advanced:
            visual_layer = st.selectbox(
                "Visual layer",
                options=["publication", "panel", "raw"],
                format_func=lambda value: LAYER_LABELS[value],
                index=0,
                key="visual_layer_selector",
            )
            st.markdown(
                (
                    "<div class='sidebar-inspection-note'>"
                    "<strong>Secondary inspection layer.</strong> Publication figures stay default; panel/raw galleries, archive pages, and registries remain read-only audit views."
                    "</div>"
                ),
                unsafe_allow_html=True,
            )

            curated_packages = state.get("curated_package_roots", [])
            st.markdown("<div class='sidebar-section-label sidebar-section-label--reference'>Package Index</div>", unsafe_allow_html=True)
            metric_columns = st.columns(1)
            with metric_columns[0]:
                st.metric("Curated package roots", len(curated_packages))
                st.metric("Publication figures indexed", len(state.get("publication_registry", [])))
                st.metric("Focused Phase 1 recipes tested", len(state.get("phase1_focused_recipe_summary", [])))

            with st.expander("Advanced stored read paths", expanded=False):
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
                    "output/trajectory_gallery_panel/",
                    "output/trajectory_gallery/",
                    "output/CASE_MINDORO_RETRO_2023/",
                    "output/CASE_DWH_RETRO_2010_72H/",
                ]
                st.caption("Exact folder names are preserved for reproducibility; some legacy export roots still contain older phase labels.")
                st.code("\n".join(read_paths), language="text")
        else:
            visual_layer = "publication"
            st.markdown(
                (
                    "<div class='sidebar-inspection-note sidebar-inspection-note--panel'>"
                    "Publication layer is active. Final-paper evidence appears first; archive/provenance and governance pages remain secondary read-only support."
                    "</div>"
                ),
                unsafe_allow_html=True,
            )

    return {
        "advanced": advanced,
        "mode_label": mode_label,
        "visual_layer": visual_layer,
        "export_mode": False,
    }


def _render_artifact_status_expander(state: dict) -> None:
    messages = list(state.get("artifact_status_messages") or [])
    if not messages:
        return
    missing = [message for message in messages if str(message.get("level", "")).lower() == "missing"]
    warnings = [message for message in messages if str(message.get("level", "")).lower() != "missing"]
    with st.sidebar:
        with st.expander("Optional package status", expanded=False):
            st.caption(
                "The dashboard is read-only. Missing optional artifacts are listed here for transparency; pages stay available."
            )
            st.metric("Missing optional artifacts", len(missing))
            if warnings:
                st.metric("Read warnings", len(warnings))
            for message in messages[:10]:
                label = str(message.get("path") or "optional artifact")
                st.caption(f"{label}: {message.get('message', 'not packaged in current repo state')}")
            if len(messages) > 10:
                st.caption(f"{len(messages) - 10} more optional status note(s) are available in the dashboard state.")


def _render_sidebar_navigation(ui_state: dict) -> None:
    if ui_state["export_mode"] or not hasattr(st, "page_link"):
        return
    page_sections = st.session_state.get("_ui_nav_pages_by_section", {})
    if not page_sections:
        return
    with st.sidebar:
        st.markdown("<div class='sidebar-nav-divider'></div>", unsafe_allow_html=True)
        st.markdown(
            f"<div class='sidebar-section-label sidebar-section-label--map'>{'Presentation Map' if not ui_state['advanced'] else 'Page Map'}</div>",
            unsafe_allow_html=True,
        )
        for section, page_entries in page_sections.items():
            modifier = _sidebar_section_modifier(section)
            st.markdown(
                f"<div class='sidebar-nav-heading sidebar-nav-heading--{modifier}'>{section}</div>",
                unsafe_allow_html=True,
            )
            if "Archive" in section:
                st.markdown(
                    "<div class='sidebar-nav-note'>Preserved support and audit material, not primary thesis claims.</div>",
                    unsafe_allow_html=True,
                )
            if ui_state["advanced"] and section.startswith("Advanced"):
                st.markdown(
                    "<div class='sidebar-nav-note'>Technical pages for inspection only; stored artifacts remain read-only.</div>",
                    unsafe_allow_html=True,
                )
            for entry in page_entries:
                st.page_link(
                    entry["page"],
                    label=entry["label"],
                    help=entry.get("page_id"),
                    use_container_width=True,
                )


def _render_page_wrapper(page_definition: PageDefinition, state: dict, ui_state: dict):
    def _run() -> None:
        try:
            render_dashboard_read_only_banner()
            page_definition.renderer(state, ui_state)
        except Exception as exc:
            if _strict_errors_enabled():
                raise
            render_status_callout(
                "Optional artifact could not load",
                f"{page_definition.label} could not load one of its optional packaged artifacts. The dashboard stays read-only and the other pages remain available.",
                "warning",
            )
            st.caption(
                "Open the Reproducibility / Governance / Audit page for the synced file indexes, or switch to Advanced mode if you need lower-level inspection."
            )
            st.caption(f"Status detail: {exc.__class__.__name__}. Set UI_STRICT_ERRORS=1 to re-raise during development.")

    return _run


def _streamlit_navigation_available() -> bool:
    return hasattr(st, "Page") and hasattr(st, "navigation")


def _build_navigation(state: dict, ui_state: dict):
    page_definitions = visible_page_definitions(state, advanced=ui_state["advanced"])
    sections: dict[str, list] = {}
    page_objects_by_label: dict[str, object] = {}
    page_objects_by_section: dict[str, list[dict[str, object]]] = {}
    if not _streamlit_navigation_available():
        st.session_state["_ui_nav_pages_by_label"] = {}
        st.session_state["_ui_nav_pages_by_section"] = {}
        return None
    for index, definition in enumerate(page_definitions):
        page_object = st.Page(
            _render_page_wrapper(definition, state, ui_state),
            title=definition.label,
            url_path=definition.url_path or definition.page_id,
            default=index == 0,
        )
        sections.setdefault(definition.navigation_section, []).append(page_object)
        page_objects_by_label[definition.label] = page_object
        page_objects_by_section.setdefault(definition.navigation_section, []).append(
            {
                "label": definition.label,
                "page": page_object,
                "page_id": definition.page_id,
            }
        )
    st.session_state["_ui_nav_pages_by_label"] = page_objects_by_label
    st.session_state["_ui_nav_pages_by_section"] = page_objects_by_section
    return st.navigation(sections, position="hidden", expanded=True)


def _run_fallback_navigation(state: dict, ui_state: dict) -> None:
    page_definitions = visible_page_definitions(state, advanced=ui_state["advanced"])
    if not page_definitions:
        render_status_callout("No pages available", "No dashboard pages are available in this mode.", "warning")
        return
    if ui_state["export_mode"]:
        selected_definition = page_definitions[0]
    else:
        with st.sidebar:
            st.markdown("<div class='sidebar-nav-divider'></div>", unsafe_allow_html=True)
            selected_label = st.selectbox(
                "Dashboard page",
                options=[definition.label for definition in page_definitions],
                index=0,
                key="fallback_dashboard_page",
            )
        selected_definition = next(
            definition for definition in page_definitions if definition.label == selected_label
        )
    _render_page_wrapper(selected_definition, state, ui_state)()


def main() -> None:
    Image.MAX_IMAGE_PIXELS = None
    try:
        st_config.set_option("client.showSidebarNavigation", False)
    except Exception:
        pass
    branding, page_icon = _branding_payload()
    st.set_page_config(
        page_title=APP_TITLE,
        page_icon=page_icon,
        layout="wide",
        initial_sidebar_state="expanded",
    )
    _load_css()
    _apply_streamlit_branding(branding)
    if hasattr(st, "query_params"):
        export_mode = _export_mode_from_query_params(st.query_params)
    else:
        try:
            export_mode = _export_mode_from_query_params(st.experimental_get_query_params())
        except Exception:
            export_mode = False
    if export_mode:
        _load_export_css()
    state = _load_dashboard_state(dashboard_state_signature())
    ui_state = _render_sidebar_controls(state, branding) if not export_mode else {
        "advanced": False,
        "mode_label": "Export",
        "visual_layer": "publication",
        "export_mode": True,
    }
    if not export_mode:
        _render_artifact_status_expander(state)
    navigation = _build_navigation(state, ui_state)
    _render_sidebar_navigation(ui_state)

    if export_mode:
        render_status_callout(
            "Print / export mode",
            "Print / export mode is active. Navigation chrome, sidebar controls, and interactive-only elements are hidden so this page can be saved cleanly as a PDF.",
            "info",
        )

    if navigation is not None:
        navigation.run()
    else:
        _run_fallback_navigation(state, ui_state)


if __name__ == "__main__":
    main()
