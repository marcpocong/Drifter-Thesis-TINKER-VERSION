"""Shared rendering helpers for the read-only Streamlit dashboard."""

from __future__ import annotations

import html
import hashlib
import io
import json
from pathlib import Path
from typing import Any, Sequence

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
from ui.evidence_contract import panel_safe_label, role_badge_for_record


def _clean_text_value(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except TypeError:
        pass
    text = str(value).strip()
    return "" if text.lower() in {"", "nan", "none", "<na>"} else text


def _first_present(*values: Any) -> str:
    for value in values:
        text = _clean_text_value(value)
        if text:
            return text
    return ""


def _humanize(value: Any) -> str:
    text = _clean_text_value(value)
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


def _normalize_metric_card(
    metric: dict[str, Any] | Sequence[Any],
    *,
    compact: bool,
    full_width: bool,
) -> dict[str, Any]:
    if isinstance(metric, dict):
        label = metric.get("label", "")
        value = metric.get("value", "")
        note = metric.get("note", "")
        item_compact = bool(metric.get("compact", compact))
        item_full_width = bool(metric.get("full_width", full_width))
    else:
        if len(metric) < 2:
            raise ValueError("Metric cards require at least a label and value.")
        label = metric[0]
        value = metric[1]
        note = metric[2] if len(metric) > 2 else ""
        item_compact = compact
        item_full_width = full_width
    return {
        "label": _clean_text_value(label),
        "value": _clean_text_value(value),
        "note": _clean_text_value(note),
        "compact": item_compact,
        "full_width": item_full_width,
    }


def build_metric_row_html(
    metrics: Sequence[dict[str, Any] | Sequence[Any]],
    *,
    export_mode: bool = False,
    compact: bool = False,
    full_width: bool = False,
) -> str:
    """Build escaped responsive metric-card markup for Streamlit markdown."""
    normalized = [
        _normalize_metric_card(metric, compact=compact or export_mode, full_width=full_width)
        for metric in metrics
    ]
    grid_classes = ["ui-metric-grid"]
    if compact or export_mode:
        grid_classes.append("ui-metric-grid--compact")
    cards: list[str] = []
    for metric in normalized:
        label = metric["label"] or "Metric"
        value = metric["value"] or "n/a"
        note = metric["note"]
        card_classes = ["ui-metric-card"]
        if metric["compact"]:
            card_classes.append("ui-metric-card--compact")
        if metric["full_width"]:
            card_classes.append("ui-metric-card--full")
        note_html = f"<div class='ui-metric-card__note'>{html.escape(note)}</div>" if note else ""
        cards.append(
            (
                f"<article class='{' '.join(card_classes)}' role='listitem'>"
                f"<div class='ui-metric-card__label'>{html.escape(label)}</div>"
                f"<div class='ui-metric-card__value'>{html.escape(value)}</div>"
                f"{note_html}"
                "</article>"
            )
        )
    return f"<div class='{' '.join(grid_classes)}' role='list'>{''.join(cards)}</div>"


def render_metric_row(
    metrics: Sequence[dict[str, Any] | Sequence[Any]],
    *,
    export_mode: bool = False,
    compact: bool = False,
    full_width: bool = False,
) -> None:
    if not metrics:
        return
    st.markdown(
        build_metric_row_html(
            metrics,
            export_mode=export_mode,
            compact=compact,
            full_width=full_width,
        ),
        unsafe_allow_html=True,
    )


def render_section_stack(
    sections: list[tuple[str, Any]],
    *,
    export_mode: bool,
    divider: bool = True,
    use_tabs: bool = True,
) -> None:
    if use_tabs and not export_mode:
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
        st.info("Not packaged in current repo state.")
        return
    display_df = df.head(max_rows).copy() if max_rows else df.copy()
    internal_columns = [
        column
        for column in display_df.columns
        if column in {"status_reportability", "status_official_status", "status_frozen_status"}
        or "launcher" in str(column).lower()
    ]
    if internal_columns:
        display_df = display_df.drop(columns=internal_columns)
    for column in display_df.select_dtypes(include=["object"]).columns:
        display_df[column] = display_df[column].map(panel_safe_label)
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
        st.info("Not packaged in current repo state.")
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
        st.info("Not packaged in current repo state.")
        return
    if export_mode:
        columns_per_row = min(columns_per_row, 2)
    for start in range(0, len(records), columns_per_row):
        visible_columns = min(columns_per_row, len(records) - start)
        columns = st.columns(visible_columns)
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
                        st.caption(f"Package root: {package['relative_path']}")
                    if package.get("page_label") and not export_mode:
                        button_label = package.get("button_label") or f"Open {package['page_label']}"
                        if st.button(button_label, key=f"nav::{package['package_id']}", use_container_width=True):
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
        st.info("Not packaged in current repo state.")
        return
    if export_mode:
        columns_per_row = min(columns_per_row, 2)
    for start in range(0, len(records), columns_per_row):
        visible_columns = min(columns_per_row, len(records) - start)
        columns = st.columns(visible_columns)
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
                        if st.button(button_label, key=f"study::{card['page_label']}", use_container_width=True):
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
        st.info("Not packaged in current repo state.")
        return
    records = df.head(limit).to_dict(orient="records")
    columns_per_row = max(1, min(columns_per_row, len(records)))
    for start in range(0, len(records), columns_per_row):
        visible_columns = min(columns_per_row, len(records) - start)
        columns = st.columns(visible_columns)
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
                            st.info("Not packaged in current repo state.")
                    else:
                        st.info("Not packaged in current repo state.")


def filter_family(df: pd.DataFrame, code: str) -> pd.DataFrame:
    for column in ("figure_family_code", "board_family_code", "figure_group_code"):
        if column in df.columns:
            return df.loc[df[column].astype(str).eq(code)].copy()
    return df.iloc[0:0].copy()


def _figure_header(row: pd.Series) -> tuple[str, str]:
    family = _first_present(
        row.get("display_title"),
        row.get("status_label"),
        row.get("track_label"),
        row.get("figure_family_label"),
        row.get("board_family_label"),
        row.get("figure_group_label"),
        row.get("artifact_group"),
        Path(str(_first_present(row.get("relative_path"), row.get("final_relative_path"), row.get("figure_id"), "figure"))).stem,
    )
    subtitle_bits = [
        _humanize(row.get("case_id", "")).replace("CASE ", ""),
        _humanize(_first_present(row.get("phase_or_track", ""), row.get("phase_group", ""))),
        _humanize(row.get("artifact_group", "")),
        _humanize(_first_present(row.get("model_names", ""), row.get("model_name", ""))),
        _humanize(row.get("date_token", "")),
    ]
    subtitle = " | ".join(bit for bit in subtitle_bits if bit and bit.lower() != "nan")
    return _humanize(family), subtitle


def _status_summary_text(row: pd.Series) -> tuple[str, str]:
    summary = _first_present(
        row.get("short_plain_language_interpretation"),
        row.get("plain_language_interpretation"),
        row.get("status_panel_text"),
        row.get("status_dashboard_summary"),
    )
    provenance = _first_present(row.get("status_provenance"), row.get("provenance_note"))
    return panel_safe_label(summary), panel_safe_label(provenance)


def _figure_badges(row: pd.Series) -> list[str]:
    return [role_badge_for_record(row.to_dict())]


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


def _one_sentence_text(value: Any, *, limit: int = 125) -> str:
    text = " ".join(str(value or "").split())
    for marker in (". ", "? ", "! "):
        marker_index = text.find(marker)
        if marker_index >= 0:
            text = text[: marker_index + 1]
            break
    return _truncate_text(text, limit=limit)


def _gallery_tile_subtitle(row: pd.Series, subtitle: str) -> str:
    status_summary, _ = _status_summary_text(row)
    if status_summary:
        return _one_sentence_text(status_summary, limit=125)
    interpretation = str(
        _first_present(
            row.get("short_plain_language_interpretation"),
            row.get("plain_language_interpretation"),
            row.get("notes"),
        )
    ).strip()
    if interpretation:
        return _one_sentence_text(interpretation, limit=125)
    return _truncate_text(subtitle, limit=110)


def _dialog_copy_blocks(row: pd.Series, subtitle: str) -> list[tuple[str, str]]:
    status_summary, provenance = _status_summary_text(row)
    interpretation = str(
        _first_present(
            row.get("short_plain_language_interpretation"),
            row.get("plain_language_interpretation"),
            row.get("notes"),
        )
    ).strip()
    notes = _clean_text_value(row.get("notes"))

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
def _gallery_image_bytes(path_text: str, max_width: int = 0) -> bytes:
    path = Path(path_text)
    with PILImage.open(path) as image:
        if image.mode not in {"RGB", "RGBA"}:
            image = image.convert("RGBA")
        if max_width and image.width > max_width:
            height = int(round(image.height * (max_width / image.width)))
            image = image.resize((max_width, max(1, height)), PILImage.Resampling.LANCZOS)
        buffer = io.BytesIO()
        image.save(buffer, format="PNG", optimize=True)
    return buffer.getvalue()


@st.cache_data(show_spinner=False)
def _binary_download_payload(path_text: str) -> bytes:
    return Path(path_text).read_bytes()


def _figure_action_token(row: pd.Series, *, namespace: str) -> str:
    figure_key = str(
        row.get("relative_path")
        or row.get("final_relative_path")
        or row.get("figure_id")
        or row.get("display_title")
        or "figure"
    )
    return hashlib.sha1(f"{namespace}::{figure_key}".encode("utf-8")).hexdigest()[:12]


def _render_missing_figure_tile(title_text: str) -> None:
    st.markdown(
        (
            "<div class='figure-gallery-card__missing'>"
            "<div class='figure-gallery-card__missing-title'>Not packaged in current repo state.</div>"
            f"<div class='figure-gallery-card__missing-copy'>{html.escape(_truncate_text(title_text, limit=90))}</div>"
            "</div>"
        ),
        unsafe_allow_html=True,
    )


def _render_gallery_preview_media(figure_path: Path | None, title_text: str) -> bool:
    if figure_path and figure_path.exists():
        try:
            st.image(_gallery_image_bytes(str(figure_path), max_width=1200), width="stretch")
            return True
        except OSError:
            _render_missing_figure_tile(title_text)
            return False
    _render_missing_figure_tile(title_text)
    return False


@st.dialog("Figure preview")
def _render_figure_gallery_dialog(row_payload: dict[str, Any], *, show_download: bool = True) -> None:
    row = pd.Series(row_payload)
    figure_path = resolve_repo_path(row.get("resolved_path") or row.get("file_path") or row.get("relative_path"))
    title_text, subtitle = _figure_header(row)
    dialog_blocks = _dialog_copy_blocks(row, subtitle)
    caption_text = _truncate_text(dialog_blocks[0][1], limit=260) if dialog_blocks else _truncate_text(subtitle, limit=260)
    detail_blocks = dialog_blocks[1:] if dialog_blocks and dialog_blocks[0][0] == "Caption" else dialog_blocks
    token = _figure_action_token(row, namespace="dialog-close")

    st.markdown(f"### {title_text}")
    if subtitle:
        st.caption(subtitle)
    render_badge_strip(_figure_badges(row)[:3])

    if figure_path and figure_path.exists():
        try:
            st.image(str(figure_path), width="stretch")
        except OSError:
            _render_missing_figure_tile(title_text)
    else:
        _render_missing_figure_tile(title_text)

    if caption_text:
        st.caption(caption_text)
    for label, text in detail_blocks[:3]:
        detail = _truncate_text(text, limit=420)
        if detail:
            st.markdown(f"**{label}**")
            st.write(detail)

    if show_download and figure_path and figure_path.exists():
        action_left, action_right = st.columns(2)
        with action_left:
            st.download_button(
                "Download PNG",
                _binary_download_payload(str(figure_path)),
                file_name=figure_path.name,
                mime="image/png",
                key=_figure_download_key(row, figure_path, namespace="dialog"),
                use_container_width=True,
            )
        with action_right:
            if st.button("Close preview", key=f"close::{token}", use_container_width=True):
                st.session_state["_figure_preview_close_ack"] = token
    else:
        if st.button("Close preview", key=f"close::{token}", use_container_width=True):
            st.session_state["_figure_preview_close_ack"] = token


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
        st.info("Not packaged in current repo state.")
        return

    records = df.head(limit).to_dict(orient="records") if limit else df.to_dict(orient="records")
    columns_per_row = 1 if export_mode else max(1, columns_per_row)

    for start in range(0, len(records), columns_per_row):
        visible_columns = min(columns_per_row, len(records) - start)
        columns = st.columns(visible_columns)
        for column, record in zip(columns, records[start : start + columns_per_row]):
            row = pd.Series(record)
            figure_path = resolve_repo_path(row.get("resolved_path") or row.get("file_path") or row.get("relative_path"))
            title_text, subtitle = _figure_header(row)
            tile_subtitle = _gallery_tile_subtitle(row, subtitle)
            dialog_key = _figure_action_token(row, namespace=title)
            with column:
                with st.container(border=not export_mode):
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
                    if export_mode:
                        if figure_path and figure_path.exists():
                            try:
                                st.image(str(figure_path), width="stretch")
                            except OSError:
                                _render_missing_figure_tile(title_text)
                        else:
                            _render_missing_figure_tile(title_text)
                    else:
                        _render_gallery_preview_media(figure_path, title_text)

                    if not export_mode:
                        action_left, action_right = st.columns(2)
                        with action_left:
                            if st.button(overlay_label, key=f"gallery-open::{dialog_key}", use_container_width=True):
                                _render_figure_gallery_dialog(record, show_download=show_download)
                        with action_right:
                            if show_download and figure_path and figure_path.exists():
                                st.download_button(
                                    "Download PNG",
                                    _binary_download_payload(str(figure_path)),
                                    file_name=figure_path.name,
                                    mime="image/png",
                                    key=_figure_download_key(row, figure_path, namespace="gallery"),
                                    use_container_width=True,
                                )


def _render_figure_details(
    row: pd.Series,
    figure_path: Path | None,
    *,
    export_mode: bool = False,
    heading_level: str = "####",
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
            st.image(str(figure_path), width="stretch")
            if not export_mode:
                st.download_button(
                    "Download PNG",
                    _binary_download_payload(str(figure_path)),
                    file_name=figure_path.name,
                    mime="image/png",
                    key=_figure_download_key(row, figure_path, namespace=action_namespace),
                )
        except OSError:
            _render_missing_figure_tile(title_text)
    else:
        _render_missing_figure_tile(title_text)
    interpretation = str(
        _first_present(
            row.get("short_plain_language_interpretation"),
            row.get("plain_language_interpretation"),
            row.get("notes"),
        )
    ).strip()
    if interpretation:
        st.markdown(f"> {interpretation}")
    notes = _clean_text_value(row.get("notes", ""))
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
) -> None:
    st.subheader(title)
    if caption:
        st.caption(caption)
    if df.empty:
        st.info("Not packaged in current repo state.")
        return
    records = df.head(limit).to_dict(orient="records") if limit else df.to_dict(orient="records")
    if export_mode:
        columns_per_row = 1
        compact_selector = False
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
        visible_columns = min(columns_per_row, len(records) - start)
        columns = st.columns(visible_columns)
        for column, record in zip(columns, records[start : start + columns_per_row]):
            row = pd.Series(record)
            figure_path = resolve_repo_path(row.get("resolved_path") or row.get("file_path") or row.get("relative_path"))
            with column:
                with st.container(border=not export_mode):
                    _render_figure_details(
                        row,
                        figure_path,
                        export_mode=export_mode,
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
        st.info("Not packaged in current repo state.")
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
