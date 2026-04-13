"""Shared rendering helpers for the read-only Streamlit dashboard."""

from __future__ import annotations

import html
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


def render_metric_row(metrics: list[tuple[str, str]]) -> None:
    columns = st.columns(len(metrics))
    for column, (label, value) in zip(columns, metrics):
        with column:
            st.metric(label, value)


def render_table(
    title: str,
    df: pd.DataFrame,
    *,
    download_name: str,
    caption: str = "",
    height: int = 260,
    max_rows: int | None = None,
) -> None:
    st.subheader(title)
    if caption:
        st.caption(caption)
    if df.empty:
        st.info("No rows are available for this view in the current repo state.")
        return
    display_df = df.head(max_rows).copy() if max_rows else df.copy()
    st.dataframe(display_df, width="stretch", height=height)
    st.download_button(
        "Download CSV",
        df.to_csv(index=False).encode("utf-8"),
        file_name=download_name,
        mime="text/csv",
        key=f"download::{download_name}",
    )


def render_markdown_block(title: str, content: str, *, collapsed: bool = True) -> None:
    st.subheader(title)
    if not content.strip():
        st.info("This markdown artifact is not available in the current repo state.")
        return
    if collapsed:
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


def render_package_cards(packages: list[dict[str, Any]], *, columns_per_row: int = 2) -> None:
    records = [package for package in packages if package]
    if not records:
        st.info("No curated package cards are available in the current repo state.")
        return
    for start in range(0, len(records), columns_per_row):
        columns = st.columns(columns_per_row)
        for column, package in zip(columns, records[start : start + columns_per_row]):
            with column:
                with st.container(border=True):
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
                    if package.get("page_label"):
                        button_label = package.get("button_label") or f"Open {package['page_label']}"
                        if st.button(button_label, key=f"nav::{package['package_id']}"):
                            st.session_state["page_selector"] = package["page_label"]
                            st.rerun()


def render_study_structure_cards(cards: list[dict[str, Any]], *, columns_per_row: int = 3) -> None:
    records = [card for card in cards if card]
    if not records:
        st.info("No study-structure cards are available in the current repo state.")
        return
    for start in range(0, len(records), columns_per_row):
        columns = st.columns(columns_per_row)
        for column, card in zip(columns, records[start : start + columns_per_row]):
            with column:
                with st.container(border=True):
                    st.markdown(f"### {card.get('title', 'Study section')}")
                    render_badge_strip([str(card.get("classification", "")).strip()])
                    if card.get("body"):
                        st.write(str(card["body"]))
                    if card.get("note"):
                        st.caption(str(card["note"]))
                    if card.get("page_label"):
                        button_label = card.get("button_label") or f"Open {card['page_label']}"
                        if st.button(button_label, key=f"study::{card['page_label']}"):
                            st.session_state["page_selector"] = card["page_label"]
                            st.rerun()


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


def render_figure_cards(
    df: pd.DataFrame,
    *,
    title: str,
    caption: str = "",
    limit: int | None = None,
    columns_per_row: int = 2,
    compact_selector: bool = False,
    selector_key: str = "",
) -> None:
    st.subheader(title)
    if caption:
        st.caption(caption)
    if df.empty:
        st.info("No figures are available for this selection.")
        return
    records = df.head(limit).to_dict(orient="records") if limit else df.to_dict(orient="records")
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
    for start in range(0, len(records), columns_per_row):
        columns = st.columns(columns_per_row)
        for column, record in zip(columns, records[start : start + columns_per_row]):
            row = pd.Series(record)
            figure_path = resolve_repo_path(row.get("resolved_path") or row.get("file_path") or row.get("relative_path"))
            title_text, subtitle = _figure_header(row)
            status_summary, provenance = _status_summary_text(row)
            with column:
                with st.container(border=True):
                    st.markdown(f"#### {title_text}")
                    render_badge_strip(_figure_badges(row))
                    if subtitle:
                        st.caption(subtitle)
                    if status_summary:
                        st.caption(status_summary)
                    if figure_path and figure_path.exists():
                        try:
                            st.image(str(figure_path), width="stretch")
                            download_key = str(
                                row.get("relative_path")
                                or row.get("final_relative_path")
                                or row.get("figure_id")
                                or figure_path
                            )
                            st.download_button(
                                "Download PNG",
                                figure_path.read_bytes(),
                                file_name=figure_path.name,
                                mime="image/png",
                                key=f"download::{download_key}",
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
