"""Shared rendering helpers for the read-only Streamlit dashboard."""

from __future__ import annotations

import html
import hashlib
import io
import json
from pathlib import Path
from typing import Any, Mapping, Sequence

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


def _tone_class(tone: str | None, *, default: str = "info") -> str:
    key = str(tone or default).strip().lower().replace("_", "-")
    aliases = {
        "danger": "error",
        "critical": "error",
        "caution": "warning",
        "caveat": "warning",
        "primary": "thesis",
        "thesis-facing": "thesis",
        "comparator-support": "comparator",
        "support-context": "context",
        "support": "context",
        "archive-only": "archive",
        "legacy-support": "legacy",
    }
    allowed = {
        "info",
        "success",
        "warning",
        "error",
        "neutral",
        "thesis",
        "comparator",
        "context",
        "archive",
        "legacy",
        "advanced",
    }
    key = aliases.get(key, key)
    return key if key in allowed else default


def _role_modifier(label: Any) -> str:
    text = _clean_text_value(label).upper()
    if not text:
        return "neutral"
    if "THESIS-FACING" in text:
        return "thesis"
    if "COMPARATOR" in text:
        return "comparator"
    if "LEGACY" in text:
        return "legacy"
    if "ARCHIVE" in text:
        return "archive"
    if "SUPPORT / CONTEXT" in text or "CONTEXT ONLY" in text:
        return "context"
    if "ADVANCED" in text:
        return "advanced"
    if "REFERENCE" in text or "TECHNICAL" in text or "AUDIT" in text:
        return "advanced"
    if "READ-ONLY" in text or "READ ONLY" in text:
        return "readonly"
    return "neutral"


def _badge_html(label: Any, *, class_name: str = "ui-role-badge") -> str:
    text = _clean_text_value(label)
    if not text:
        return ""
    modifier = _role_modifier(text)
    return f"<span class='{class_name} {class_name}--{modifier}'>{html.escape(text)}</span>"


def _body_html(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        blocks = [_clean_text_value(item) for item in value]
        blocks = [block for block in blocks if block]
        return "".join(f"<p>{html.escape(block)}</p>" for block in blocks)
    text = _clean_text_value(value).replace("\r\n", "\n")
    if not text:
        return ""
    paragraphs = [block.strip() for block in text.split("\n\n") if block.strip()]
    rendered: list[str] = []
    for paragraph in paragraphs:
        lines = [line.strip() for line in paragraph.splitlines() if line.strip()]
        if not lines:
            continue
        if all(line.startswith("- ") for line in lines):
            items = "".join(f"<li>{html.escape(line[2:].strip())}</li>" for line in lines)
            rendered.append(f"<ul>{items}</ul>")
            continue
        rendered.append("<p>" + "<br>".join(html.escape(line) for line in lines) + "</p>")
    return "".join(rendered)


def _card_from_sequence(card: Mapping[str, Any] | Sequence[Any]) -> dict[str, Any]:
    if isinstance(card, Mapping):
        return dict(card)
    if len(card) < 2:
        raise ValueError("Feature cards require at least a title and body.")
    return {
        "title": card[0],
        "body": card[1],
        "badge": card[2] if len(card) > 2 else "",
        "note": card[3] if len(card) > 3 else "",
    }


def build_callout_html(label: str, value: str, tone: str = "info", *, compact: bool = False) -> str:
    """Build escaped callout-card markup for Streamlit markdown."""
    tone_class = _tone_class(tone)
    compact_class = " ui-callout--compact" if compact else ""
    return (
        f"<aside class='ui-callout ui-callout--{tone_class}{compact_class}'>"
        "<div class='ui-callout__bar' aria-hidden='true'></div>"
        "<div class='ui-callout__content'>"
        f"<div class='ui-callout__label'>{html.escape(_clean_text_value(label) or 'Note')}</div>"
        f"<div class='ui-callout__body'>{_body_html(value)}</div>"
        "</div>"
        "</aside>"
    )


def render_modern_hero(
    title: str,
    body: str,
    *,
    badge: str = "",
    eyebrow: str = "",
    meta: Sequence[str] | None = None,
    tone: str = "thesis",
) -> None:
    """Render the page hero as a safe, print-friendly research card."""
    badge_html = _badge_html(badge)
    eyebrow_html = (
        f"<div class='ui-hero__eyebrow'>{html.escape(_clean_text_value(eyebrow))}</div>"
        if _clean_text_value(eyebrow)
        else ""
    )
    meta_items = [item for item in (meta or []) if _clean_text_value(item)]
    meta_html = ""
    if meta_items:
        meta_html = (
            "<div class='ui-hero__meta'>"
            + "".join(_badge_html(item, class_name="ui-meta-pill") for item in meta_items)
            + "</div>"
        )
    st.markdown(
        (
            f"<section class='ui-hero ui-hero--{_tone_class(tone, default='thesis')}'>"
            "<div class='ui-hero__content'>"
            f"{eyebrow_html}"
            f"{badge_html}"
            f"<h1 class='ui-hero__title'>{html.escape(_clean_text_value(title) or 'Dashboard')}</h1>"
            f"<div class='ui-hero__body'>{_body_html(body)}</div>"
            f"{meta_html}"
            "</div>"
            "</section>"
        ),
        unsafe_allow_html=True,
    )


def render_page_intro(title: str, body: str, *, badge: str = "") -> None:
    render_modern_hero(
        title,
        body,
        badge=badge,
        eyebrow="Read-only thesis dashboard",
        tone=_role_modifier(badge) if badge else "thesis",
    )


def render_status_callout(label: str, value: str, tone: str = "info") -> None:
    st.markdown(build_callout_html(label, value, tone), unsafe_allow_html=True)


def render_key_takeaway(label: str, value: str, *, tone: str = "thesis", badge: str = "") -> None:
    """Render a prominent thesis takeaway without Streamlit alert chrome."""
    badge_html = _badge_html(badge) if badge else ""
    st.markdown(
        (
            f"<section class='ui-key-takeaway ui-key-takeaway--{_tone_class(tone, default='thesis')}'>"
            "<div class='ui-key-takeaway__content'>"
            f"{badge_html}"
            f"<div class='ui-key-takeaway__label'>{html.escape(_clean_text_value(label) or 'Key takeaway')}</div>"
            f"<div class='ui-key-takeaway__body'>{_body_html(value)}</div>"
            "</div>"
            "</section>"
        ),
        unsafe_allow_html=True,
    )


def render_caveat_ribbon(label: str, value: str, *, tone: str = "warning") -> None:
    """Render a bounded caveat ribbon that stays visible in print/export."""
    st.markdown(
        (
            f"<aside class='ui-caveat-ribbon ui-caveat-ribbon--{_tone_class(tone, default='warning')}'>"
            f"<div class='ui-caveat-ribbon__label'>{html.escape(_clean_text_value(label) or 'Caveat')}</div>"
            f"<div class='ui-caveat-ribbon__body'>{_body_html(value)}</div>"
            "</aside>"
        ),
        unsafe_allow_html=True,
    )


def render_support_notice(label: str, value: str) -> None:
    render_caveat_ribbon(label, value, tone="context")


def render_archive_notice(label: str, value: str) -> None:
    render_caveat_ribbon(label, value, tone="archive")


def render_section_header(title: str, body: str = "", *, eyebrow: str = "", badge: str = "") -> None:
    """Render a section heading with optional eyebrow, role badge, and summary copy."""
    eyebrow_html = (
        f"<div class='ui-section-heading__eyebrow'>{html.escape(_clean_text_value(eyebrow))}</div>"
        if _clean_text_value(eyebrow)
        else ""
    )
    badge_html = _badge_html(badge) if badge else ""
    body_html = f"<div class='ui-section-heading__body'>{_body_html(body)}</div>" if _clean_text_value(body) else ""
    st.markdown(
        (
            "<section class='ui-section-heading'>"
            f"{eyebrow_html}"
            "<div class='ui-section-heading__row'>"
            f"<h2>{html.escape(_clean_text_value(title) or 'Section')}</h2>"
            f"{badge_html}"
            "</div>"
            f"{body_html}"
            "</section>"
        ),
        unsafe_allow_html=True,
    )


def build_feature_card_html(
    title: str,
    body: str,
    *,
    badge: str = "",
    note: str = "",
    footer: str = "",
    tone: str = "neutral",
) -> str:
    badge_html = _badge_html(badge) if badge else ""
    note_html = f"<div class='ui-feature-card__note'>{html.escape(_clean_text_value(note))}</div>" if _clean_text_value(note) else ""
    footer_html = f"<div class='ui-feature-card__footer'>{html.escape(_clean_text_value(footer))}</div>" if _clean_text_value(footer) else ""
    return (
        f"<article class='ui-feature-card ui-feature-card--{_tone_class(tone, default=_role_modifier(badge) if badge else 'neutral')}'>"
        f"{badge_html}"
        f"<h3>{html.escape(_clean_text_value(title) or 'Feature')}</h3>"
        f"<div class='ui-feature-card__body'>{_body_html(body)}</div>"
        f"{note_html}"
        f"{footer_html}"
        "</article>"
    )


def render_feature_card(
    title: str,
    body: str,
    *,
    badge: str = "",
    note: str = "",
    footer: str = "",
    tone: str = "neutral",
) -> None:
    st.markdown(
        build_feature_card_html(title, body, badge=badge, note=note, footer=footer, tone=tone),
        unsafe_allow_html=True,
    )


def render_feature_grid(
    cards: Sequence[Mapping[str, Any] | Sequence[Any]],
    *,
    columns_per_row: int = 3,
    export_mode: bool = False,
) -> None:
    normalized = [_card_from_sequence(card) for card in cards if card]
    if not normalized:
        render_status_callout("Unavailable", "Not packaged in current repo state.", "neutral")
        return
    columns = max(1, min(4, 1 if export_mode else columns_per_row))
    card_html = []
    for card in normalized:
        badge = _clean_text_value(card.get("badge", "") or card.get("classification", ""))
        card_html.append(
            build_feature_card_html(
                str(card.get("title", "Feature")),
                str(card.get("body", "")),
                badge=badge,
                note=str(card.get("note", "")),
                footer=str(card.get("footer", "")),
                tone=str(card.get("tone", "") or _role_modifier(badge)),
            )
        )
    st.markdown(
        f"<div class='ui-feature-grid ui-feature-grid--cols-{columns}'>{''.join(card_html)}</div>",
        unsafe_allow_html=True,
    )


def render_evidence_path(
    steps: Sequence[Mapping[str, Any] | Sequence[Any]],
    *,
    title: str = "",
    caption: str = "",
    export_mode: bool = False,
) -> None:
    normalized = [_card_from_sequence(step) for step in steps if step]
    if not normalized:
        return
    if title or caption:
        render_section_header(title or "Evidence path", caption, eyebrow="Evidence hierarchy")
    step_html: list[str] = []
    for index, step in enumerate(normalized, start=1):
        badge = _clean_text_value(step.get("badge", "") or step.get("classification", ""))
        badge_html = _badge_html(badge) if badge else ""
        note = _clean_text_value(step.get("note", ""))
        note_html = f"<div class='ui-evidence-step__note'>{html.escape(note)}</div>" if note else ""
        step_html.append(
            (
                f"<article class='ui-evidence-step ui-evidence-step--{_role_modifier(badge)}'>"
                f"<div class='ui-evidence-step__index'>{index}</div>"
                "<div class='ui-evidence-step__content'>"
                f"{badge_html}"
                f"<h3>{html.escape(_clean_text_value(step.get('title')) or f'Step {index}')}</h3>"
                f"<div class='ui-evidence-step__body'>{_body_html(step.get('body', ''))}</div>"
                f"{note_html}"
                "</div>"
                "</article>"
            )
        )
    export_class = " ui-evidence-path--export" if export_mode else ""
    st.markdown(
        f"<div class='ui-evidence-path{export_class}'>{''.join(step_html)}</div>",
        unsafe_allow_html=True,
    )


def render_metric_story_grid(
    metrics: Sequence[dict[str, Any] | Sequence[Any]],
    *,
    export_mode: bool = False,
    compact: bool = False,
    full_width: bool = False,
) -> None:
    if not metrics:
        return
    st.markdown(
        "<section class='ui-metric-story-grid'>"
        + build_metric_row_html(
            metrics,
            export_mode=export_mode,
            compact=compact,
            full_width=full_width,
        )
        + "</section>",
        unsafe_allow_html=True,
    )


def render_figure_feature(
    title: str,
    figure_path: str | Path | None,
    *,
    caption: str = "",
    badge: str = "",
    body: str = "",
) -> None:
    """Render one figure as a featured panel while keeping image loading read-only."""
    badge_html = _badge_html(badge) if badge else ""
    st.markdown(
        (
            "<section class='ui-figure-feature'>"
            "<div class='ui-figure-feature__header'>"
            f"{badge_html}"
            f"<h2>{html.escape(_clean_text_value(title) or 'Figure')}</h2>"
            f"<div class='ui-figure-feature__body'>{_body_html(body)}</div>"
            "</div>"
            "</section>"
        ),
        unsafe_allow_html=True,
    )
    path = resolve_repo_path(figure_path)
    if path and path.exists():
        try:
            st.image(str(path), width="stretch")
        except OSError:
            _render_missing_figure_tile(title)
    else:
        _render_missing_figure_tile(title)
    if caption:
        st.caption(caption)


def render_page_footer_note(lines: Sequence[str] | str) -> None:
    body = _body_html(lines)
    if not body:
        return
    st.markdown(f"<footer class='ui-page-footer-note'>{body}</footer>", unsafe_allow_html=True)


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
        render_section_header(title)
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
        render_status_callout("Unavailable", "Not packaged in current repo state.", "neutral")
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
        render_status_callout("Unavailable", "Not packaged in current repo state.", "neutral")
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
    spans = "".join(_badge_html(label, class_name="ui-badge") for label in clean)
    st.markdown(f"<div class='ui-badge-strip'>{spans}</div>", unsafe_allow_html=True)


def _path_caption_for(relative_path: str) -> str:
    path_text = str(relative_path or "").strip()
    if not path_text:
        return ""
    lowered = path_text.lower()
    prefix = "Stored export root"
    if "phase 3b" in lowered or "phase 3c" in lowered or "phase4" in lowered:
        prefix += " (legacy folder name)"
    return f"{prefix}: {path_text}"


def render_package_cards(packages: list[dict[str, Any]], *, columns_per_row: int = 2, export_mode: bool = False) -> None:
    records = [package for package in packages if package]
    if not records:
        render_status_callout("Unavailable", "Not packaged in current repo state.", "neutral")
        return
    if export_mode:
        columns_per_row = min(columns_per_row, 2)
    for start in range(0, len(records), columns_per_row):
        visible_columns = min(columns_per_row, len(records) - start)
        columns = st.columns(visible_columns)
        for column, package in zip(columns, records[start : start + columns_per_row]):
            with column:
                with st.container(border=not export_mode):
                    st.markdown(
                        f"<div class='ui-card-title'>{html.escape(_clean_text_value(package.get('label')) or 'Package')}</div>",
                        unsafe_allow_html=True,
                    )
                    badges = []
                    if package.get("secondary_note"):
                        badges.append(str(package["secondary_note"]))
                    if package.get("artifact_count") is not None:
                        badges.append(f"{package['artifact_count']} indexed artifacts")
                    render_badge_strip(badges)
                    if package.get("description"):
                        st.markdown(
                            f"<div class='ui-card-body'>{_body_html(package['description'])}</div>",
                            unsafe_allow_html=True,
                        )
                    if package.get("relative_path"):
                        st.caption(_path_caption_for(str(package["relative_path"])))
                    if package.get("page_label") and not export_mode:
                        button_label = package.get("button_label") or f"Open {package['page_label']}"
                        if st.button(button_label, key=f"nav::{package['package_id']}", use_container_width=True):
                            page_objects = st.session_state.get("_ui_nav_pages_by_label", {})
                            target_page = page_objects.get(package["page_label"])
                            if target_page is not None:
                                st.switch_page(target_page)
                            else:
                                render_status_callout(
                                    "Page unavailable",
                                    "This page is not available in the current viewing mode.",
                                    "neutral",
                                )


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
        render_status_callout("Unavailable", "Not packaged in current repo state.", "neutral")
        return
    if export_mode:
        columns_per_row = min(columns_per_row, 2)
    for start in range(0, len(records), columns_per_row):
        visible_columns = min(columns_per_row, len(records) - start)
        columns = st.columns(visible_columns)
        for column, card in zip(columns, records[start : start + columns_per_row]):
            with column:
                with st.container(border=not export_mode):
                    st.markdown(
                        f"<div class='ui-card-title'>{html.escape(_clean_text_value(card.get('title')) or 'Study section')}</div>",
                        unsafe_allow_html=True,
                    )
                    render_badge_strip([str(card.get("classification", "")).strip()])
                    if card.get("body"):
                        st.markdown(
                            f"<div class='ui-card-body'>{_body_html(card['body'])}</div>",
                            unsafe_allow_html=True,
                        )
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
                                render_status_callout(
                                    "Page unavailable",
                                    "This page is not available in the current viewing mode.",
                                    "neutral",
                                )


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
        render_status_callout("Unavailable", "Not packaged in current repo state.", "neutral")
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
                            _render_missing_figure_tile(title_text)
                    else:
                        _render_missing_figure_tile(title_text)


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
    key_namespace: str = "",
) -> None:
    st.subheader(title)
    if caption:
        st.caption(caption)
    if df.empty:
        render_status_callout("Unavailable", "Not packaged in current repo state.", "neutral")
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
            download_namespace = f"gallery::{key_namespace or title}"
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
                                    key=_figure_download_key(row, figure_path, namespace=download_namespace),
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
        render_status_callout("Unavailable", "Not packaged in current repo state.", "neutral")
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
        render_status_callout("Unavailable", "Not packaged in current repo state.", "neutral")
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
