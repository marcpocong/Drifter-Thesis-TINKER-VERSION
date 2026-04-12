"""Shared rendering helpers for the read-only Streamlit dashboard."""

from __future__ import annotations

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


def render_page_intro(title: str, body: str, *, badge: str = "") -> None:
    st.title(title)
    if badge:
        st.caption(badge)
    st.write(body)


def render_status_callout(label: str, value: str, tone: str = "info") -> None:
    icon = {
        "success": "✅",
        "warning": "⚠️",
        "error": "⛔",
        "info": "ℹ️",
    }.get(tone, "ℹ️")
    st.markdown(f"**{icon} {label}:** {value}")


def render_metric_row(metrics: list[tuple[str, str]]) -> None:
    columns = st.columns(len(metrics))
    for column, (label, value) in zip(columns, metrics):
        column.metric(label, value)


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


def filter_family(df: pd.DataFrame, code: str) -> pd.DataFrame:
    for column in ("figure_family_code", "board_family_code", "figure_group_code"):
        if column in df.columns:
            return df.loc[df[column].astype(str).eq(code)].copy()
    return df.iloc[0:0].copy()


def _figure_header(row: pd.Series) -> tuple[str, str]:
    family = str(
        row.get("status_label")
        or row.get("figure_family_label")
        or row.get("board_family_label")
        or row.get("figure_group_label")
        or row.get("figure_id")
    )
    subtitle_bits = [
        str(row.get("case_id", "")).replace("CASE_", ""),
        str(row.get("phase_or_track", "")),
        str(row.get("model_names", "") or row.get("model_name", "")),
        str(row.get("date_token", "")),
        str(row.get("scenario_id", "")),
    ]
    subtitle = " | ".join(bit for bit in subtitle_bits if bit and bit != "nan")
    return family, subtitle


def _status_summary_text(row: pd.Series) -> tuple[str, str]:
    summary = str(row.get("status_dashboard_summary") or "").strip()
    provenance = str(row.get("status_provenance") or "").strip()
    return summary, provenance


def render_figure_cards(
    df: pd.DataFrame,
    *,
    title: str,
    caption: str = "",
    limit: int | None = None,
    columns_per_row: int = 2,
) -> None:
    st.subheader(title)
    if caption:
        st.caption(caption)
    if df.empty:
        st.info("No figures are available for this selection.")
        return
    records = df.head(limit).to_dict(orient="records") if limit else df.to_dict(orient="records")
    for start in range(0, len(records), columns_per_row):
        columns = st.columns(columns_per_row)
        for column, record in zip(columns, records[start : start + columns_per_row]):
            row = pd.Series(record)
            figure_path = resolve_repo_path(row.get("resolved_path") or row.get("file_path") or row.get("relative_path"))
            title_text, subtitle = _figure_header(row)
            status_summary, provenance = _status_summary_text(row)
            with column:
                st.markdown(f"#### {title_text}")
                if subtitle:
                    st.caption(subtitle)
                if status_summary:
                    st.caption(status_summary)
                if figure_path and figure_path.exists():
                    st.image(str(figure_path), width="stretch")
                    st.download_button(
                        "Download PNG",
                        figure_path.read_bytes(),
                        file_name=figure_path.name,
                        mime="image/png",
                        key=f"download::{row.get('figure_id', figure_path.name)}",
                    )
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
