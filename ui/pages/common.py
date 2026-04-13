"""Shared rendering helpers for the read-only Streamlit dashboard."""

from __future__ import annotations

import base64
import html
import hashlib
import io
import json
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

import pandas as pd
import streamlit as st
from PIL import Image as PILImage

from ui.data_access import parse_source_paths, read_json, read_text, resolve_repo_path


def _humanize(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    text = text.replace("_", " ").replace("/", " / ").replace("  ", " ")
    return text


def render_page_intro(title: str, body: str, *, badge: str = "") -> None:
    st.title(title)
    badge_html = f"<div class='page-hero__badge'>{html.escape(badge)}</div>" if badge else ""
    st.markdown(
        (
            "<div class='page-hero'>"
            f"{badge_html}"
            f"<div class='page-hero__body'>{html.escape(body)}</div>"
            "</div>"
        ),
        unsafe_allow_html=True,
    )


def render_status_callout(label: str, value: str, tone: str = "info") -> None:
    message = f"**{label}**\n\n{value}"
    if tone == "success":
        st.success(message)
    elif tone == "warning":
        st.warning(message)
    elif tone == "error":
        st.error(message)
    else:
        st.info(message)


def render_metric_row(metrics: list[tuple[str, str]], *, export_mode: bool = False) -> None:
    if not metrics:
        return
    columns_per_row = min(2 if export_mode else len(metrics), len(metrics))
    for start in range(0, len(metrics), columns_per_row):
        columns = st.columns(columns_per_row)
        for column, (label, value) in zip(columns, metrics[start : start + columns_per_row]):
            with column:
                st.metric(label, value)


def render_section_stack(
    sections: list[tuple[str, Any]],
    *,
    export_mode: bool,
    divider: bool = True,
) -> None:
    if not export_mode:
        tabs = st.tabs([title for title, _ in sections])
        for tab, (_, renderer) in zip(tabs, sections):
            with tab:
                renderer()
        return
    for index, (title, renderer) in enumerate(sections):
        st.markdown(f"## {title}")
        renderer()
        if divider and index < len(sections) - 1:
            st.divider()


def render_table(
    title: str,
    df: pd.DataFrame,
    *,
    download_name: str,
    caption: str = "",
    height: int = 260,
    max_rows: int | None = None,
    export_mode: bool = False,
) -> None:
    st.subheader(title)
    if caption:
        st.caption(caption)
    if df.empty:
        st.info("No rows are available for this view in the current repo state.")
        return
    display_df = df.head(max_rows).copy() if max_rows else df.copy()
    if export_mode and len(display_df) <= 20:
        st.table(display_df)
    else:
        st.dataframe(display_df, width="stretch", height=height)
    if not export_mode:
        st.download_button(
            "Download CSV",
            df.to_csv(index=False).encode("utf-8"),
            file_name=download_name,
            mime="text/csv",
            key=f"download::{download_name}",
        )


def render_markdown_block(title: str, content: str, *, collapsed: bool = True, export_mode: bool = False) -> None:
    st.subheader(title)
    if not content.strip():
        st.info("This markdown artifact is not available in the current repo state.")
        return
    if collapsed and not export_mode:
        with st.expander(f"Open {title}", expanded=False):
            st.markdown(content)
    else:
        st.markdown(content)


def render_badge_strip(labels: list[str]) -> None:
    clean = [label.strip() for label in labels if str(label).strip()]
    if not clean:
        return
    spans = "".join(f"<span class='ui-badge'>{html.escape(label)}</span>" for label in clean)
    st.markdown(f"<div class='ui-badge-strip'>{spans}</div>", unsafe_allow_html=True)


def render_package_cards(packages: list[dict[str, Any]], *, columns_per_row: int = 2, export_mode: bool = False) -> None:
    records = [package for package in packages if package]
    if not records:
        st.info("No curated package cards are available in the current repo state.")
        return
    if export_mode:
        columns_per_row = min(columns_per_row, 2)
    for start in range(0, len(records), columns_per_row):
        columns = st.columns(columns_per_row)
        for column, package in zip(columns, records[start : start + columns_per_row]):
            with column:
                with st.container(border=not export_mode):
                    st.markdown(f"### {package.get('label', 'Package')}")
                    badges = []
                    if package.get("secondary_note"):
                        badges.append(str(package["secondary_note"]))
                    if package.get("artifact_count") is not None:
                        badges.append(f"{package['artifact_count']} indexed artifacts")
                    render_badge_strip(badges)
                    if package.get("description"):
                        st.write(str(package["description"]))
                    if package.get("relative_path"):
                        st.code(str(package["relative_path"]), language="text")
                    if package.get("page_label") and not export_mode:
                        button_label = package.get("button_label") or f"Open {package['page_label']}"
                        if st.button(button_label, key=f"nav::{package['package_id']}"):
                            page_objects = st.session_state.get("_ui_nav_pages_by_label", {})
                            target_page = page_objects.get(package["page_label"])
                            if target_page is not None:
                                st.switch_page(target_page)
                            else:
                                st.info("This page is not available in the current viewing mode.")


def render_export_note(lines: list[str]) -> None:
    clean = [line.strip() for line in lines if str(line).strip()]
    if not clean:
        return
    st.markdown(
        "<div class='export-note'>"
        + "".join(f"<p>{html.escape(line)}</p>" for line in clean)
        + "</div>",
        unsafe_allow_html=True,
    )


def render_study_structure_cards(cards: list[dict[str, Any]], *, columns_per_row: int = 3, export_mode: bool = False) -> None:
    records = [card for card in cards if card]
    if not records:
        st.info("No study-structure cards are available in the current repo state.")
        return
    if export_mode:
        columns_per_row = min(columns_per_row, 2)
    for start in range(0, len(records), columns_per_row):
        columns = st.columns(columns_per_row)
        for column, card in zip(columns, records[start : start + columns_per_row]):
            with column:
                with st.container(border=not export_mode):
                    st.markdown(f"### {card.get('title', 'Study section')}")
                    render_badge_strip([str(card.get("classification", "")).strip()])
                    if card.get("body"):
                        st.write(str(card["body"]))
                    if card.get("note"):
                        st.caption(str(card["note"]))
                    if card.get("page_label") and not export_mode:
                        button_label = card.get("button_label") or f"Open {card['page_label']}"
                        if st.button(button_label, key=f"study::{card['page_label']}"):
                            page_objects = st.session_state.get("_ui_nav_pages_by_label", {})
                            target_page = page_objects.get(card["page_label"])
                            if target_page is not None:
                                st.switch_page(target_page)
                            else:
                                st.info("This page is not available in the current viewing mode.")


def render_figure_strip(
    df: pd.DataFrame,
    *,
    title: str,
    caption: str = "",
    limit: int = 3,
    columns_per_row: int = 3,
) -> None:
    st.subheader(title)
    if caption:
        st.caption(caption)
    if df.empty:
        st.info("No additional figures are available for this view.")
        return
    records = df.head(limit).to_dict(orient="records")
    columns_per_row = max(1, min(columns_per_row, len(records)))
    for start in range(0, len(records), columns_per_row):
        columns = st.columns(columns_per_row)
        for column, record in zip(columns, records[start : start + columns_per_row]):
            row = pd.Series(record)
            figure_path = resolve_repo_path(row.get("resolved_path") or row.get("file_path") or row.get("relative_path"))
            title_text, subtitle = _figure_header(row)
            with column:
                with st.container(border=True):
                    st.markdown(f"##### {title_text}")
                    if subtitle:
                        st.caption(subtitle)
                    if figure_path and figure_path.exists():
                        try:
                            st.image(str(figure_path), width="stretch")
                        except OSError:
                            st.info("This packaged figure could not be opened in the strip view.")
                    else:
                        st.info("Figure file is missing on disk.")


def filter_family(df: pd.DataFrame, code: str) -> pd.DataFrame:
    for column in ("figure_family_code", "board_family_code", "figure_group_code"):
        if column in df.columns:
            return df.loc[df[column].astype(str).eq(code)].copy()
    return df.iloc[0:0].copy()


def _figure_header(row: pd.Series) -> tuple[str, str]:
    family = str(
        row.get("display_title")
        or row.get("status_label")
        or row.get("track_label")
        or row.get("figure_family_label")
        or row.get("board_family_label")
        or row.get("figure_group_label")
        or row.get("artifact_group")
        or Path(str(row.get("relative_path") or row.get("final_relative_path") or row.get("figure_id") or "figure")).stem
    )
    subtitle_bits = [
        _humanize(row.get("case_id", "")).replace("CASE ", ""),
        _humanize(row.get("phase_or_track", "") or row.get("phase_group", "")),
        _humanize(row.get("artifact_group", "")),
        _humanize(row.get("model_names", "") or row.get("model_name", "")),
        _humanize(row.get("date_token", "")),
    ]
    subtitle = " | ".join(bit for bit in subtitle_bits if bit and bit.lower() != "nan")
    return _humanize(family), subtitle


def _status_summary_text(row: pd.Series) -> tuple[str, str]:
    def _panel_safe(text: str) -> str:
        return (
            text.replace("legacy honesty", "legacy reference")
            .replace("Legacy honesty", "Legacy reference")
            .replace("inherited-provisional", "support result")
            .replace("reportable now", "main discussion result")
            .replace("not_comparable_honestly", "no matched comparison is packaged yet")
        )

    summary = str(
        row.get("short_plain_language_interpretation")
        or row.get("plain_language_interpretation")
        or row.get("status_panel_text")
        or row.get("status_dashboard_summary")
        or ""
    ).strip()
    provenance = str(row.get("status_provenance") or row.get("provenance_note") or "").strip()
    return _panel_safe(summary), _panel_safe(provenance)


def _figure_badges(row: pd.Series) -> list[str]:
    badges: list[str] = []
    scientific_flag = str(row.get("scientific_vs_display_only") or "").strip()
    if scientific_flag:
        badges.append(_humanize(scientific_flag))
    primary_flag = str(row.get("primary_vs_secondary") or "").strip()
    if primary_flag:
        badges.append(_humanize(primary_flag))
    if str(row.get("comparator_only") or "").strip().lower() == "true":
        badges.append("Comparator-only")
    if str(row.get("support_only") or "").strip().lower() == "true":
        badges.append("Support-only")
    if str(row.get("optional_context_only") or "").strip().lower() == "true":
        badges.append("Context-only")
    role = str(row.get("status_role") or "").strip()
    if role:
        badges.append(_humanize(role))
    return badges[:4]


def _figure_download_key(row: pd.Series, figure_path: Path | None, *, namespace: str = "card") -> str:
    base_key = str(
        row.get("relative_path")
        or row.get("final_relative_path")
        or row.get("figure_id")
        or figure_path
        or "figure"
    )
    return f"download::{namespace}::{base_key}"


def _truncate_text(value: Any, *, limit: int = 140) -> str:
    text = " ".join(str(value or "").split())
    if len(text) <= limit:
        return text
    truncated = text[: max(1, limit - 1)].rstrip()
    if " " in truncated:
        truncated = truncated.rsplit(" ", 1)[0]
    return f"{truncated}..."


def _gallery_tile_subtitle(row: pd.Series, subtitle: str) -> str:
    if subtitle.strip():
        return _truncate_text(subtitle, limit=110)
    status_summary, _ = _status_summary_text(row)
    if status_summary:
        return _truncate_text(status_summary, limit=110)
    interpretation = str(
        row.get("short_plain_language_interpretation")
        or row.get("plain_language_interpretation")
        or row.get("notes")
        or ""
    ).strip()
    return _truncate_text(interpretation, limit=110)


def _modal_copy_blocks(row: pd.Series, subtitle: str) -> list[tuple[str, str]]:
    status_summary, provenance = _status_summary_text(row)
    interpretation = str(
        row.get("short_plain_language_interpretation")
        or row.get("plain_language_interpretation")
        or row.get("notes")
        or ""
    ).strip()
    notes = str(row.get("notes") or "").strip()

    blocks: list[tuple[str, str]] = []
    caption = status_summary or interpretation or subtitle
    if caption:
        blocks.append(("Caption", caption))
    if interpretation and interpretation not in {caption}:
        blocks.append(("Interpretation", interpretation))
    if notes and notes not in {caption, interpretation}:
        blocks.append(("Notes", notes))
    if provenance and provenance not in {caption, interpretation, notes}:
        blocks.append(("Provenance", provenance))
    return blocks


@st.cache_data(show_spinner=False)
def _image_data_uri(path_text: str, max_width: int = 0) -> str:
    path = Path(path_text)
    with PILImage.open(path) as image:
        if image.mode not in {"RGB", "RGBA"}:
            image = image.convert("RGBA")
        if max_width and image.width > max_width:
            height = int(round(image.height * (max_width / image.width)))
            image = image.resize((max_width, max(1, height)), PILImage.Resampling.LANCZOS)
        buffer = io.BytesIO()
        image.save(buffer, format="PNG", optimize=True)
    encoded = base64.b64encode(buffer.getvalue()).decode("ascii")
    return f"data:image/png;base64,{encoded}"


def _hover_lightbox_markup(
    row: pd.Series,
    figure_path: Path,
    *,
    overlay_label: str,
) -> str:
    return _click_lightbox_markup(row, figure_path, overlay_label=overlay_label)


def _click_lightbox_markup(
    row: pd.Series,
    figure_path: Path,
    *,
    overlay_label: str,
) -> str:
    figure_key = str(row.get("figure_id") or figure_path)
    token = hashlib.sha1(figure_key.encode("utf-8")).hexdigest()[:12]
    anchor_id = f"figure-preview-{token}"
    modal_id = f"figure-lightbox-{token}"
    title_text, _ = _figure_header(row)
    subtitle = _gallery_tile_subtitle(row, _figure_header(row)[1])
    badges = _figure_badges(row)[:3]
    copy_blocks = _modal_copy_blocks(row, subtitle)
    alt_text = html.escape(title_text, quote=True)
    overlay_text = html.escape(overlay_label, quote=False)
    title_html = html.escape(title_text, quote=False)
    subtitle_html = html.escape(subtitle, quote=False)
    thumbnail_uri = _image_data_uri(str(figure_path), max_width=900)
    full_uri = _image_data_uri(str(figure_path), max_width=1800)
    badge_html = (
        "<div class='figure-gallery-card__modal-badges'>"
        + "".join(
            f"<span class='figure-gallery-card__modal-badge'>{html.escape(label, quote=False)}</span>"
            for label in badges
        )
        + "</div>"
        if badges
        else ""
    )
    copy_html = "".join(
        (
            "<div class='figure-gallery-card__modal-copy-block'>"
            f"<div class='figure-gallery-card__modal-copy-label'>{html.escape(label, quote=False)}</div>"
            f"<div class='figure-gallery-card__modal-copy-text'>{html.escape(text, quote=False)}</div>"
            "</div>"
        )
        for label, text in copy_blocks
        if text
    )
    modal_subtitle = (
        f"<div class='figure-gallery-card__modal-subtitle'>{subtitle_html}</div>"
        if subtitle
        else ""
    )
    modal_actions = (
        "<div class='figure-gallery-card__modal-actions'>"
        f"<a class='figure-gallery-card__modal-download' href='{full_uri}' download='{html.escape(figure_path.name, quote=True)}'>Download preview PNG</a>"
        "</div>"
    )
    return (
        f"<div class='figure-gallery-card__lightbox' id='{anchor_id}'>"
        f"<a class='figure-gallery-card__trigger' href='#{modal_id}' aria-label='Open larger preview: {alt_text}'>"
        f"<img class='figure-gallery-card__thumb' src='{thumbnail_uri}' alt='{alt_text}' loading='lazy' />"
        f"<span class='figure-gallery-card__overlay'>{overlay_text}</span>"
        "</a>"
        f"<div class='figure-gallery-card__modal' id='{modal_id}'>"
        f"<a class='figure-gallery-card__backdrop' href='#{anchor_id}' aria-label='Close preview'></a>"
        "<div class='figure-gallery-card__panel' role='dialog' aria-modal='true'>"
        f"<a class='figure-gallery-card__close' href='#{anchor_id}' aria-label='Close preview'>Close</a>"
        f"<div class='figure-gallery-card__modal-title'>{title_html}</div>"
        f"{modal_subtitle}"
        f"{badge_html}"
        f"<img class='figure-gallery-card__full' src='{full_uri}' alt='{alt_text}' loading='lazy' />"
        f"<div class='figure-gallery-card__modal-copy'>{copy_html}</div>"
        f"{modal_actions}"
        "</div>"
        "</div>"
        "</div>"
    )


def render_figure_gallery(
    df: pd.DataFrame,
    *,
    title: str,
    caption: str = "",
    limit: int | None = None,
    columns_per_row: int = 2,
    export_mode: bool = False,
    overlay_label: str = "Click to enlarge",
    show_download: bool = True,
) -> None:
    st.subheader(title)
    if caption:
        st.caption(caption)
    if df.empty:
        st.info("No figures are available for this selection.")
        return

    records = df.head(limit).to_dict(orient="records") if limit else df.to_dict(orient="records")
    columns_per_row = 1 if export_mode else max(1, columns_per_row)

    for start in range(0, len(records), columns_per_row):
        columns = st.columns(columns_per_row)
        for column, record in zip(columns, records[start : start + columns_per_row]):
            row = pd.Series(record)
            figure_path = resolve_repo_path(row.get("resolved_path") or row.get("file_path") or row.get("relative_path"))
            title_text, subtitle = _figure_header(row)
            tile_subtitle = _gallery_tile_subtitle(row, subtitle)
            with column:
                with st.container(border=not export_mode):
                    if figure_path and figure_path.exists():
                        try:
                            if export_mode:
                                st.image(str(figure_path), width="stretch")
                            else:
                                st.html(
                                    _click_lightbox_markup(
                                        row,
                                        figure_path,
                                        overlay_label=overlay_label,
                                    ),
                                    width="stretch",
                                )
                        except OSError:
                            st.info("The packaged figure exists, but the image could not be opened in this gallery view.")
                    else:
                        st.warning("Figure file is missing on disk.")

                    st.markdown(
                        f"<div class='figure-gallery-card__title'>{html.escape(title_text)}</div>",
                        unsafe_allow_html=True,
                    )
                    render_badge_strip(_figure_badges(row)[:3])
                    if tile_subtitle:
                        st.markdown(
                            f"<div class='figure-gallery-card__subtitle'>{html.escape(tile_subtitle)}</div>",
                            unsafe_allow_html=True,
                        )
                    if not export_mode and show_download and figure_path and figure_path.exists():
                        st.download_button(
                            "Download PNG",
                            figure_path.read_bytes(),
                            file_name=figure_path.name,
                            mime="image/png",
                            key=_figure_download_key(row, figure_path, namespace="gallery"),
                        )


def _render_figure_details(
    row: pd.Series,
    figure_path: Path | None,
    *,
    export_mode: bool = False,
    heading_level: str = "####",
    image_interaction: str = "none",
    image_overlay_label: str = "View larger",
    action_namespace: str = "card",
) -> None:
    title_text, subtitle = _figure_header(row)
    status_summary, provenance = _status_summary_text(row)

    st.markdown(f"{heading_level} {title_text}")
    render_badge_strip(_figure_badges(row))
    if subtitle:
        st.caption(subtitle)
    if status_summary:
        st.caption(status_summary)
    if figure_path and figure_path.exists():
        try:
            if not export_mode and image_interaction == "hover_lightbox":
                st.html(
                    _hover_lightbox_markup(
                        row,
                        figure_path,
                        overlay_label=image_overlay_label,
                    ),
                    width="stretch",
                )
            else:
                st.image(str(figure_path), width="stretch")
            if not export_mode:
                st.download_button(
                    "Download PNG",
                    figure_path.read_bytes(),
                    file_name=figure_path.name,
                    mime="image/png",
                    key=_figure_download_key(row, figure_path, namespace=action_namespace),
                )
        except OSError:
            st.info("The packaged figure exists, but the image could not be opened in this view.")
    else:
        st.warning("Figure file is missing on disk.")
    interpretation = str(
        row.get("short_plain_language_interpretation")
        or row.get("plain_language_interpretation")
        or row.get("notes")
        or ""
    ).strip()
    if interpretation:
        st.markdown(f"> {interpretation}")
    notes = str(row.get("notes", "")).strip()
    if notes and notes != interpretation:
        st.caption(notes)
    if provenance:
        st.caption(f"Provenance: {provenance}")


def render_figure_cards(
    df: pd.DataFrame,
    *,
    title: str,
    caption: str = "",
    limit: int | None = None,
    columns_per_row: int = 2,
    compact_selector: bool = False,
    selector_key: str = "",
    export_mode: bool = False,
    image_interaction: str = "none",
    image_overlay_label: str = "View larger",
) -> None:
    st.subheader(title)
    if caption:
        st.caption(caption)
    if df.empty:
        st.info("No figures are available for this selection.")
        return
    records = df.head(limit).to_dict(orient="records") if limit else df.to_dict(orient="records")
    if export_mode:
        columns_per_row = 1
        compact_selector = False
        image_interaction = "none"
    if compact_selector and len(records) > 1:
        labels = []
        for record in records:
            row = pd.Series(record)
            title_text, subtitle = _figure_header(row)
            labels.append(f"{title_text} - {subtitle}" if subtitle else title_text)
        chosen_label = st.selectbox(
            "Featured figure",
            options=labels,
            index=0,
            key=selector_key or f"featured::{title}",
        )
        selected_index = labels.index(chosen_label)
        records = [records[selected_index]]
        columns_per_row = 1
    for start in range(0, len(records), columns_per_row):
        columns = st.columns(columns_per_row)
        for column, record in zip(columns, records[start : start + columns_per_row]):
            row = pd.Series(record)
            figure_path = resolve_repo_path(row.get("resolved_path") or row.get("file_path") or row.get("relative_path"))
            with column:
                with st.container(border=not export_mode):
                    _render_figure_details(
                        row,
                        figure_path,
                        export_mode=export_mode,
                        image_interaction=image_interaction,
                        image_overlay_label=image_overlay_label,
                        action_namespace="card",
                    )


def render_source_artifact_summary(row: pd.Series) -> None:
    source_paths = parse_source_paths(row.get("source_paths"))
    if not source_paths:
        return
    with st.expander("Source artifacts", expanded=False):
        for path in source_paths:
            st.code(str(path), language="text")


def preview_artifact(path_value: str | Path | None, *, repo_root: str | Path | None = None) -> None:
    path = resolve_repo_path(path_value, repo_root)
    if path is None or not path.exists():
        st.info("Selected artifact is not available on disk.")
        return
    suffix = path.suffix.lower()
    if suffix == ".json":
        st.json(read_json(path, repo_root))
    elif suffix in {".md", ".txt", ".log", ".yaml", ".yml"}:
        st.code(read_text(path, repo_root)[:15000], language="text")
    elif suffix == ".csv":
        df = pd.read_csv(path)
        st.dataframe(df.head(200), width="stretch", height=280)
    elif suffix in {".png", ".jpg", ".jpeg"}:
        st.image(str(path), width="stretch")
    else:
        st.code(str(path), language="text")
    st.download_button(
        "Download selected artifact",
        path.read_bytes(),
        file_name=path.name,
        mime="application/octet-stream",
        key=f"artifact::{path}",
    )


def json_excerpt(payload: dict[str, Any]) -> str:
    if not payload:
        return "{}"
    return json.dumps(payload, indent=2)[:4000]
