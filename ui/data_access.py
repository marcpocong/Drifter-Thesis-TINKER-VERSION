"""Cached read-only access helpers for the local dashboard."""

from __future__ import annotations

import copy
import json
import hashlib
from functools import lru_cache
from pathlib import Path
from typing import Any

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
import xarray as xr

try:
    import yaml
except ModuleNotFoundError:  # pragma: no cover - exercised only in minimal UI environments
    yaml = None

from src.core.artifact_status import (
    DWH_CASE_ID,
    MINDORO_CASE_ID,
    artifact_status_columns,
    artifact_status_columns_for_key,
    record_matches_artifact_status,
)
from src.core.publication_figure_governance import publication_figure_governance_columns

REPO_ROOT = Path(__file__).resolve().parents[1]
FINAL_REPRO_DIR = Path("output") / "final_reproducibility_package"
FINAL_VALIDATION_DIR = Path("output") / "final_validation_package"
PANEL_REVIEW_DIR = Path("output") / "panel_review_check"
MINDORO_FINAL_DIR = Path("output") / "Phase 3B March13-14 Final Output"
MINDORO_FINAL_ARCHIVE_DIR = Path("output") / "Phase 3B March13-14 Final Output Archive R0 Legacy"
DWH_FINAL_DIR = Path("output") / "Phase 3C DWH Final Output"
LEGACY_2016_FINAL_DIR = Path("output") / "2016 Legacy Runs FINAL Figures"
MINDORO_ARCHIVE_DECISION_PATH = FINAL_VALIDATION_DIR / "mindoro_validation_archive_decision.md"
PAPER_OUTPUT_REGISTRY_PATH = Path("docs") / "PAPER_OUTPUT_REGISTRY.md"
DATA_SOURCE_REGISTRY_PATH = Path("config") / "data_sources.yaml"
RAW_GALLERY_DIR = Path("output") / "trajectory_gallery"
PANEL_GALLERY_DIR = Path("output") / "trajectory_gallery_panel"
PUBLICATION_DIR = Path("output") / "figure_package_publication"
PHASE4_DIR = Path("output") / "phase4" / "CASE_MINDORO_RETRO_2023"
PHASE4_AUDIT_DIR = Path("output") / "phase4_crossmodel_comparability_audit"
PHASE1_FOCUSED_DIR = Path("output") / "phase1_mindoro_focus_pre_spill_2016_2023"
PHASE1_REFERENCE_DIR = Path("output") / "phase1_production_rerun"
MINDORO_DIR = Path("output") / "CASE_MINDORO_RETRO_2023"
DWH_DIR = Path("output") / "CASE_DWH_RETRO_2010_72H"
PANEL_DRIFTER_CONTEXT_DIR = Path("output") / "panel_drifter_context"
PHASE1_FOCUSED_CONFIG_PATH = Path("config") / "phase1_mindoro_focus_pre_spill_2016_2023.yaml"
MINDORO_CASE_CONFIG_PATH = Path("config") / "case_mindoro_retro_2023.yaml"
SETTINGS_PATH = Path("config") / "settings.yaml"

MINDORO_ARCHIVE_SURFACE_KEYS: tuple[str, ...] = ("archive_only",)

DASHBOARD_STATE_PATHS: tuple[Path, ...] = (
    FINAL_REPRO_DIR / "final_phase_status_registry.csv",
    FINAL_REPRO_DIR / "final_case_registry.csv",
    FINAL_REPRO_DIR / "final_output_catalog.csv",
    FINAL_REPRO_DIR / "final_manifest_index.csv",
    FINAL_REPRO_DIR / "final_log_index.csv",
    FINAL_REPRO_DIR / "final_reproducibility_summary.md",
    PAPER_OUTPUT_REGISTRY_PATH,
    DATA_SOURCE_REGISTRY_PATH,
    FINAL_VALIDATION_DIR / "final_validation_manifest.json",
    FINAL_VALIDATION_DIR / "final_validation_case_registry.csv",
    FINAL_VALIDATION_DIR / "final_validation_limitations.csv",
    PANEL_REVIEW_DIR / "panel_results_match_check.csv",
    PANEL_REVIEW_DIR / "panel_results_match_check.md",
    PANEL_REVIEW_DIR / "panel_review_manifest.json",
    PUBLICATION_DIR / "publication_figure_registry.csv",
    PUBLICATION_DIR / "publication_figure_manifest.json",
    PUBLICATION_DIR / "publication_figure_captions.md",
    PUBLICATION_DIR / "publication_figure_inventory.md",
    PUBLICATION_DIR / "publication_talking_points.md",
    MINDORO_ARCHIVE_DECISION_PATH,
    PANEL_GALLERY_DIR / "panel_figure_registry.csv",
    RAW_GALLERY_DIR / "trajectory_gallery_index.csv",
    MINDORO_FINAL_DIR / "manifests" / "final_output_manifest.json",
    MINDORO_FINAL_DIR / "README.md",
    MINDORO_FINAL_DIR / "manifests" / "phase3b_final_output_registry.csv",
    MINDORO_FINAL_DIR / "summary" / "opendrift_primary" / "march13_14_reinit_run_manifest.json",
    MINDORO_FINAL_ARCHIVE_DIR / "README.md",
    MINDORO_FINAL_ARCHIVE_DIR / "manifests" / "phase3b_final_output_registry.csv",
    MINDORO_FINAL_DIR / "summary" / "opendrift_primary" / "march13_14_reinit_summary.csv",
    MINDORO_FINAL_DIR / "summary" / "opendrift_primary" / "march13_14_reinit_fss_by_window.csv",
    MINDORO_FINAL_DIR / "summary" / "comparator_pygnome" / "march13_14_reinit_crossmodel_summary.csv",
    MINDORO_FINAL_DIR / "summary" / "comparator_pygnome" / "march13_14_reinit_crossmodel_model_ranking.csv",
    MINDORO_DIR / "phase3b" / "phase3b_summary.csv",
    MINDORO_DIR / "pygnome_public_comparison" / "pygnome_public_comparison_model_ranking.csv",
    PHASE1_FOCUSED_DIR / "phase1_production_manifest.json",
    PHASE1_FOCUSED_DIR / "phase1_recipe_ranking.csv",
    PHASE1_FOCUSED_DIR / "phase1_recipe_summary.csv",
    PHASE1_FOCUSED_DIR / "phase1_accepted_segment_registry.csv",
    PHASE1_FOCUSED_DIR / "phase1_ranking_subset_registry.csv",
    PHASE1_FOCUSED_DIR / "phase1_segment_metrics.csv",
    PHASE1_FOCUSED_DIR / "phase1_drifter_registry.csv",
    PHASE1_FOCUSED_DIR / "phase1_loading_audit.csv",
    PHASE1_FOCUSED_DIR / "phase1_baseline_selection_candidate.yaml",
    PHASE1_FOCUSED_DIR / "phase1_ranking_subset_report.md",
    PHASE1_REFERENCE_DIR / "phase1_production_manifest.json",
    PHASE1_REFERENCE_DIR / "phase1_recipe_ranking.csv",
    PHASE1_REFERENCE_DIR / "phase1_recipe_summary.csv",
    PHASE1_REFERENCE_DIR / "phase1_baseline_selection_candidate.yaml",
    DWH_FINAL_DIR / "manifests" / "phase3c_final_output_manifest.json",
    DWH_FINAL_DIR / "README.md",
    DWH_FINAL_DIR / "manifests" / "phase3c_final_output_registry.csv",
    DWH_FINAL_DIR / "summary" / "deterministic" / "phase3c_summary.csv",
    DWH_FINAL_DIR / "summary" / "ensemble" / "phase3c_ensemble_summary.csv",
    DWH_FINAL_DIR / "summary" / "comparator_pygnome" / "phase3c_dwh_pygnome_summary.csv",
    DWH_FINAL_DIR / "summary" / "comparator_pygnome" / "phase3c_dwh_all_results_table.csv",
    DWH_FINAL_DIR / "summary" / "comparison" / "phase3c_main_scorecard.csv",
    DWH_FINAL_DIR / "summary" / "comparison" / "phase3c_interpretation_note.md",
    DWH_FINAL_DIR / "summary" / "comparison" / "phase3c_output_matrix_decision_note.md",
    DWH_DIR / "phase3c_external_case_run" / "phase3c_summary.csv",
    DWH_DIR / "phase3c_dwh_pygnome_comparator" / "phase3c_dwh_all_results_table.csv",
    LEGACY_2016_FINAL_DIR / "manifests" / "legacy_final_output_manifest.json",
    LEGACY_2016_FINAL_DIR / "README.md",
    LEGACY_2016_FINAL_DIR / "manifests" / "prototype_2016_provenance_metadata.json",
    LEGACY_2016_FINAL_DIR / "manifests" / "prototype_2016_final_output_registry.csv",
    LEGACY_2016_FINAL_DIR / "summary" / "phase4" / "prototype_2016_phase4_registry.csv",
    LEGACY_2016_FINAL_DIR / "summary" / "phase4_comparator" / "prototype_2016_phase4_pygnome_comparator_registry.csv",
    LEGACY_2016_FINAL_DIR / "summary" / "phase4_comparator" / "prototype_2016_phase4_pygnome_decision_note.md",
    LEGACY_2016_FINAL_DIR / "summary" / "phase3a" / "prototype_pygnome_similarity_by_case.csv",
    LEGACY_2016_FINAL_DIR / "summary" / "phase3a" / "prototype_pygnome_fss_by_case_window.csv",
    LEGACY_2016_FINAL_DIR / "phase5" / "prototype_2016_packaging_summary.md",
    PHASE4_DIR / "phase4_oil_budget_summary.csv",
    PHASE4_DIR / "phase4_oiltype_comparison.csv",
    PHASE4_DIR / "phase4_shoreline_arrival.csv",
    PHASE4_DIR / "phase4_shoreline_segments.csv",
    PHASE4_AUDIT_DIR / "phase4_crossmodel_comparability_matrix.csv",
    PHASE4_AUDIT_DIR / "phase4_crossmodel_final_verdict.md",
    PHASE4_AUDIT_DIR / "phase4_crossmodel_blockers.md",
    PHASE4_AUDIT_DIR / "phase4_crossmodel_minimal_next_steps.md",
    PANEL_DRIFTER_CONTEXT_DIR / "b1_drifter_context_manifest.json",
    PANEL_DRIFTER_CONTEXT_DIR / "b1_drifter_context_map.png",
    PANEL_DRIFTER_CONTEXT_DIR / "b1_drifter_context_map.json",
    PHASE1_FOCUSED_CONFIG_PATH,
    MINDORO_CASE_CONFIG_PATH,
    SETTINGS_PATH,
)


def _root(repo_root: str | Path | None = None) -> Path:
    return Path(repo_root).resolve() if repo_root else REPO_ROOT


def _path_cache_signature(path: str | Path, repo_root: str | Path | None = None) -> tuple[str, str, int, int]:
    root_text = str(_root(repo_root))
    resolved = resolve_repo_path(path, repo_root)
    if resolved is None:
        return str(path), root_text, -1, -1
    try:
        stat = resolved.stat()
    except OSError:
        return str(resolved), root_text, -1, -1
    return str(resolved), root_text, int(stat.st_mtime_ns), int(stat.st_size)


def resolve_repo_path(value: str | Path | None, repo_root: str | Path | None = None) -> Path | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    root = _root(repo_root)
    if text.startswith("/app/"):
        candidate = (root / text.removeprefix("/app/")).resolve()
        if candidate.exists():
            return candidate
    path = Path(text)
    if path.is_absolute() and path.exists():
        return path.resolve()
    if not path.is_absolute():
        candidate = (root / path).resolve()
        if candidate.exists():
            return candidate
    lowered = text.replace("\\", "/")
    for marker in ("output/", "config/", "docs/", "data/", "data_processed/", "logs/", "ui/"):
        idx = lowered.lower().find(marker)
        if idx >= 0:
            candidate = (root / lowered[idx:]).resolve()
            if candidate.exists():
                return candidate
    return ((root / path).resolve() if not path.is_absolute() else path.resolve())


def _normalize_series(series: pd.Series) -> pd.Series:
    if not pd.api.types.is_object_dtype(series):
        return series
    cleaned = series.astype(str).str.strip()
    nonempty = cleaned[cleaned != ""]
    if nonempty.empty:
        return cleaned.replace({"nan": ""})
    lower = nonempty.str.lower()
    if lower.isin(["true", "false"]).all():
        return cleaned.str.lower().map({"true": True, "false": False}).where(cleaned != "", pd.NA)
    numeric = pd.to_numeric(cleaned, errors="coerce")
    if numeric.notna().sum() >= max(1, int(len(nonempty) * 0.8)):
        return numeric
    return cleaned.replace({"nan": ""})


def _drop_repeated_header_rows(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    comparison = pd.DataFrame({column: df[column].astype(str).str.strip() for column in df.columns})
    repeated_mask = pd.Series(True, index=df.index)
    for column in df.columns:
        repeated_mask &= comparison[column].eq(str(column))
    return df.loc[~repeated_mask].reset_index(drop=True)


@lru_cache(maxsize=128)
def _cached_csv(path_text: str, repo_root_text: str, _mtime_ns: int, _size: int) -> pd.DataFrame:
    path = Path(path_text)
    if not path.is_absolute():
        path = resolve_repo_path(path_text, repo_root_text)
    if path is None or not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path)
    df = _drop_repeated_header_rows(df)
    for column in df.columns:
        df[column] = _normalize_series(df[column])
    return df


def read_csv(path: str | Path, repo_root: str | Path | None = None) -> pd.DataFrame:
    return _cached_csv(*_path_cache_signature(path, repo_root)).copy()


@lru_cache(maxsize=128)
def _cached_json(path_text: str, repo_root_text: str, _mtime_ns: int, _size: int) -> dict[str, Any]:
    path = Path(path_text)
    if not path.is_absolute():
        path = resolve_repo_path(path_text, repo_root_text)
    if path is None or not path.exists():
        return {}
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle) or {}


def read_json(path: str | Path, repo_root: str | Path | None = None) -> dict[str, Any]:
    return copy.deepcopy(_cached_json(*_path_cache_signature(path, repo_root)))


@lru_cache(maxsize=128)
def _cached_text(path_text: str, repo_root_text: str, _mtime_ns: int, _size: int) -> str:
    path = Path(path_text)
    if not path.is_absolute():
        path = resolve_repo_path(path_text, repo_root_text)
    if path is None or not path.exists():
        return ""
    return path.read_text(encoding="utf-8")


def read_text(path: str | Path, repo_root: str | Path | None = None) -> str:
    return _cached_text(*_path_cache_signature(path, repo_root))


def _parse_simple_yaml_scalar(raw_value: str) -> Any:
    text = raw_value.strip()
    if text in {"", '""', "''"}:
        return ""
    if (text.startswith('"') and text.endswith('"')) or (text.startswith("'") and text.endswith("'")):
        try:
            return json.loads(text) if text.startswith('"') else text[1:-1]
        except json.JSONDecodeError:
            return text[1:-1]
    if text.lower() in {"true", "false"}:
        return text.lower() == "true"
    return text


def _parse_simple_yaml_mapping(text: str) -> dict[str, Any]:
    """Parse the simple registry YAML shape when PyYAML is unavailable."""
    root: dict[str, Any] = {}
    current_entry: dict[str, Any] | None = None
    current_list_key = ""
    for raw_line in text.splitlines():
        if not raw_line.strip() or raw_line.lstrip().startswith("#"):
            continue
        indent = len(raw_line) - len(raw_line.lstrip(" "))
        stripped = raw_line.strip()
        if indent == 0 and stripped.endswith(":"):
            key = stripped[:-1].strip()
            root[key] = {}
            current_entry = root[key]
            current_list_key = ""
            continue
        if indent == 2 and current_entry is not None and ":" in stripped:
            key, raw_value = stripped.split(":", 1)
            key = key.strip()
            raw_value = raw_value.strip()
            if raw_value == "":
                current_entry[key] = []
                current_list_key = key
            else:
                current_entry[key] = _parse_simple_yaml_scalar(raw_value)
                current_list_key = ""
            continue
        if indent >= 4 and stripped.startswith("- ") and current_entry is not None and current_list_key:
            current_entry.setdefault(current_list_key, []).append(_parse_simple_yaml_scalar(stripped[2:]))
    return root


@lru_cache(maxsize=64)
def _cached_yaml(path_text: str, repo_root_text: str, _mtime_ns: int, _size: int) -> dict[str, Any]:
    path = Path(path_text)
    if not path.is_absolute():
        path = resolve_repo_path(path_text, repo_root_text)
    if path is None or not path.exists():
        return {}
    text = path.read_text(encoding="utf-8")
    if yaml is None:
        return _parse_simple_yaml_mapping(text)
    payload = yaml.safe_load(text) or {}
    return payload if isinstance(payload, dict) else {}


def read_yaml(path: str | Path, repo_root: str | Path | None = None) -> dict[str, Any]:
    return copy.deepcopy(_cached_yaml(*_path_cache_signature(path, repo_root)))


def _discover_first_matching_path(
    candidate_groups: list[tuple[str, list[str]]],
    *,
    repo_root: str | Path | None = None,
) -> dict[str, Any]:
    root = _root(repo_root)
    for priority_label, patterns in candidate_groups:
        for pattern in patterns:
            try:
                matches = sorted(path for path in root.glob(pattern) if path.is_file())
            except OSError:
                matches = []
            if matches:
                match = matches[0]
                return {
                    "path": match,
                    "relative_path": str(match.relative_to(root)),
                    "priority_label": priority_label,
                    "status_message": f"Loaded local artifact from {priority_label}: {match.relative_to(root)}",
                    "searched_patterns": [item for _, group_patterns in candidate_groups for item in group_patterns],
                }
    return {
        "path": None,
        "relative_path": "",
        "priority_label": "missing",
        "status_message": (
            "No local artifact was found after checking curated output packages, focused Phase 1 outputs, "
            "persistent historical inputs, and fallback drifter CSV locations."
        ),
        "searched_patterns": [item for _, group_patterns in candidate_groups for item in group_patterns],
    }


def dashboard_state_signature(repo_root: str | Path | None = None) -> str:
    root = _root(repo_root)
    payload = "\n".join(
        f"{path_text}|{mtime_ns}|{size}"
        for path_text, _repo_root_text, mtime_ns, size in (
            _path_cache_signature(path, root) for path in DASHBOARD_STATE_PATHS
        )
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _attach_resolved_paths(df: pd.DataFrame, repo_root: str | Path | None = None) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    resolved_paths: list[str] = []
    exists: list[bool] = []
    for row in df.to_dict(orient="records"):
        candidate = resolve_repo_path(row.get("relative_path") or row.get("file_path") or row.get("filename"), repo_root)
        resolved_paths.append(str(candidate) if candidate else "")
        exists.append(bool(candidate and candidate.exists()))
    payload = df.copy()
    payload["resolved_path"] = resolved_paths
    payload["resolved_exists"] = exists
    return payload


def _attach_status_fields(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    payload = df.copy()
    status_rows = []
    for row in payload.to_dict(orient="records"):
        raw_status_key = row.get("status_key")
        status_key = "" if pd.isna(raw_status_key) else str(raw_status_key or "").strip()
        if status_key:
            status_rows.append(artifact_status_columns_for_key(status_key, row))
        else:
            status_rows.append(artifact_status_columns(row))
    status_df = pd.DataFrame(status_rows, index=payload.index)
    for column in status_df.columns:
        payload[column] = status_df[column]
    return payload


def _attach_publication_governance_fields(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    payload = df.copy()
    governance_rows = [publication_figure_governance_columns(row) for row in payload.to_dict(orient="records")]
    governance_df = pd.DataFrame(governance_rows, index=payload.index)
    for column in governance_df.columns:
        payload[column] = governance_df[column]
    return payload


def _apply_status_key(df: pd.DataFrame, status_key: str) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    payload = df.copy()
    status_columns = artifact_status_columns_for_key(status_key)
    for column, value in status_columns.items():
        payload[column] = value
    return payload


def _filter_surface_rows(
    df: pd.DataFrame,
    *,
    surface_keys: list[str] | tuple[str, ...] | None = None,
    require_home_visible: bool = False,
    require_panel_visible: bool = False,
    require_archive_visible: bool = False,
    require_advanced_visible: bool = False,
    require_recommended_visible: bool = False,
) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    payload = df.copy()
    if surface_keys and "surface_key" in payload.columns:
        payload = payload.loc[payload["surface_key"].fillna("").astype(str).isin([str(value) for value in surface_keys])].copy()
    for column, required in (
        ("surface_home_visible", require_home_visible),
        ("surface_panel_visible", require_panel_visible),
        ("surface_archive_visible", require_archive_visible),
        ("surface_advanced_visible", require_advanced_visible),
        ("surface_recommended_visible", require_recommended_visible),
    ):
        if required and column in payload.columns:
            payload = payload.loc[payload[column].fillna(False).astype(bool)].copy()
    return payload.reset_index(drop=True)


def _prepare_curated_registry(
    df: pd.DataFrame,
    *,
    repo_root: str | Path | None = None,
    default_case_id: str = "",
) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    payload = df.copy()
    if "relative_path" not in payload.columns and "final_relative_path" in payload.columns:
        payload["relative_path"] = payload["final_relative_path"]
    if "file_path" not in payload.columns and "relative_path" in payload.columns:
        payload["file_path"] = payload["relative_path"]
    if "source_paths" not in payload.columns and "source_relative_path" in payload.columns:
        payload["source_paths"] = payload["source_relative_path"]
    if "notes" not in payload.columns:
        if "provenance_note" in payload.columns:
            payload["notes"] = payload["provenance_note"]
        else:
            payload["notes"] = ""
    if "short_plain_language_interpretation" not in payload.columns:
        payload["short_plain_language_interpretation"] = payload.get("provenance_note", payload.get("notes", ""))
    if "figure_id" not in payload.columns:
        payload["figure_id"] = payload["relative_path"].astype(str).apply(lambda value: Path(value).stem)
    if default_case_id:
        if "case_id" not in payload.columns:
            payload["case_id"] = default_case_id
        else:
            payload["case_id"] = payload["case_id"].fillna("").astype(str).replace("", default_case_id)
    if "figure_family_label" not in payload.columns and "track_label" in payload.columns:
        payload["figure_family_label"] = payload["track_label"]
    if "status_label" not in payload.columns and "track_label" in payload.columns:
        payload["status_label"] = payload["track_label"]
    if "model_names" not in payload.columns and "artifact_group" in payload.columns:
        payload["model_names"] = payload["artifact_group"]
    return _attach_resolved_paths(payload, repo_root)


def _ensure_mindoro_archive_case_row(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "track_id" not in df.columns:
        return df.copy()
    payload = df.copy()
    track_ids = set(payload["track_id"].fillna("").astype(str))
    if "archive_r0" in track_ids or "B1" not in track_ids:
        return payload
    archive_row = {
        "case_id": MINDORO_CASE_ID,
        "track_id": "archive_r0",
        "track_label": "Mindoro March 13 -> March 14 R0 archived baseline",
        "status": "complete",
        "truth_source": "accepted March 14 NOAA/NESDIS observation mask with March 13 NOAA seed polygon",
        "primary_output_dir": str(MINDORO_DIR / "phase3b_extended_public_scored_march13_14_reinit" / "R0"),
        "case_definition_path": "config/case_mindoro_retro_2023.yaml",
        "case_freeze_amendment_path": "config/case_mindoro_retro_2023_phase3b_primary_validation_amendment.yaml",
        "launcher_entry_id": "mindoro_phase3b_primary_public_validation",
        "launcher_alias_entry_id": "mindoro_march13_14_noaa_reinit_stress_test",
        "row_role": "archive_only",
        "reporting_role": "archive / provenance only",
        "main_text_priority": "archive",
        "notes": "Backfilled archive-only R0 row for older validation registries that predate the thesis-surface governance sync.",
    }
    archive_row.update(artifact_status_columns(archive_row))
    return pd.concat([payload, pd.DataFrame([archive_row])], ignore_index=True)


def publication_registry(repo_root: str | Path | None = None) -> pd.DataFrame:
    root = _root(repo_root)
    payload = _attach_status_fields(
        _attach_resolved_paths(read_csv(PUBLICATION_DIR / "publication_figure_registry.csv", root), root)
    )
    payload = _attach_publication_governance_fields(payload)
    if "display_order" in payload.columns:
        payload = payload.sort_values(["display_order", "figure_id"], na_position="last").reset_index(drop=True)
    return payload


def publication_manifest(repo_root: str | Path | None = None) -> dict[str, Any]:
    return read_json(PUBLICATION_DIR / "publication_figure_manifest.json", repo_root)


def panel_registry(repo_root: str | Path | None = None) -> pd.DataFrame:
    root = _root(repo_root)
    return _attach_status_fields(_attach_resolved_paths(read_csv(PANEL_GALLERY_DIR / "panel_figure_registry.csv", root), root))


def raw_gallery_index(repo_root: str | Path | None = None) -> pd.DataFrame:
    root = _root(repo_root)
    return _attach_status_fields(_attach_resolved_paths(read_csv(RAW_GALLERY_DIR / "trajectory_gallery_index.csv", root), root))


def final_phase_status(repo_root: str | Path | None = None) -> pd.DataFrame:
    return read_csv(FINAL_REPRO_DIR / "final_phase_status_registry.csv", repo_root)


def final_output_catalog(repo_root: str | Path | None = None) -> pd.DataFrame:
    return read_csv(FINAL_REPRO_DIR / "final_output_catalog.csv", repo_root)


def final_manifest_index(repo_root: str | Path | None = None) -> pd.DataFrame:
    return read_csv(FINAL_REPRO_DIR / "final_manifest_index.csv", repo_root)


def final_log_index(repo_root: str | Path | None = None) -> pd.DataFrame:
    return read_csv(FINAL_REPRO_DIR / "final_log_index.csv", repo_root)


def final_validation_manifest(repo_root: str | Path | None = None) -> dict[str, Any]:
    return read_json(FINAL_VALIDATION_DIR / "final_validation_manifest.json", repo_root)


def final_case_registry(repo_root: str | Path | None = None) -> pd.DataFrame:
    return read_csv(FINAL_REPRO_DIR / "final_case_registry.csv", repo_root)


def final_reproducibility_summary(repo_root: str | Path | None = None) -> str:
    return read_text(FINAL_REPRO_DIR / "final_reproducibility_summary.md", repo_root)


def paper_output_registry_markdown(repo_root: str | Path | None = None) -> str:
    return read_text(PAPER_OUTPUT_REGISTRY_PATH, repo_root)


def data_source_registry(repo_root: str | Path | None = None) -> dict[str, Any]:
    return read_yaml(DATA_SOURCE_REGISTRY_PATH, repo_root)


def panel_review_check_table(repo_root: str | Path | None = None) -> pd.DataFrame:
    return read_csv(PANEL_REVIEW_DIR / "panel_results_match_check.csv", repo_root)


def panel_review_check_markdown(repo_root: str | Path | None = None) -> str:
    return read_text(PANEL_REVIEW_DIR / "panel_results_match_check.md", repo_root)


def panel_review_check_manifest(repo_root: str | Path | None = None) -> dict[str, Any]:
    return read_json(PANEL_REVIEW_DIR / "panel_review_manifest.json", repo_root)


def final_validation_case_registry(repo_root: str | Path | None = None) -> pd.DataFrame:
    return _attach_status_fields(
        _ensure_mindoro_archive_case_row(read_csv(FINAL_VALIDATION_DIR / "final_validation_case_registry.csv", repo_root))
    )


def final_validation_limitations(repo_root: str | Path | None = None) -> pd.DataFrame:
    return _attach_status_fields(read_csv(FINAL_VALIDATION_DIR / "final_validation_limitations.csv", repo_root))


def mindoro_validation_archive_decision(repo_root: str | Path | None = None) -> str:
    return read_text(MINDORO_ARCHIVE_DECISION_PATH, repo_root)


def mindoro_final_manifest(repo_root: str | Path | None = None) -> dict[str, Any]:
    return read_json(MINDORO_FINAL_DIR / "manifests" / "final_output_manifest.json", repo_root)


def mindoro_final_readme(repo_root: str | Path | None = None) -> str:
    return read_text(MINDORO_FINAL_DIR / "README.md", repo_root)


def mindoro_final_registry(repo_root: str | Path | None = None) -> pd.DataFrame:
    root = _root(repo_root)
    payload = _prepare_curated_registry(
        read_csv(MINDORO_FINAL_DIR / "manifests" / "phase3b_final_output_registry.csv", root),
        repo_root=root,
        default_case_id=MINDORO_CASE_ID,
    )
    if payload.empty:
        return payload
    payload["track_id"] = np.where(
        payload.get("artifact_group", pd.Series(dtype=str)).astype(str).eq("publication/comparator_pygnome"),
        "A",
        "B1",
    )
    return _attach_status_fields(payload)


def mindoro_final_archive_registry(repo_root: str | Path | None = None) -> pd.DataFrame:
    root = _root(repo_root)
    payload = _prepare_curated_registry(
        read_csv(MINDORO_FINAL_ARCHIVE_DIR / "manifests" / "phase3b_final_output_registry.csv", root),
        repo_root=root,
        default_case_id=MINDORO_CASE_ID,
    )
    if payload.empty:
        return payload
    current_prefix = f"{MINDORO_FINAL_DIR.as_posix()}/"
    archive_prefix = f"{MINDORO_FINAL_ARCHIVE_DIR.as_posix()}/"
    for column in ("final_relative_path", "relative_path", "file_path"):
        if column in payload.columns:
            payload[column] = (
                payload[column]
                .fillna("")
                .astype(str)
                .str.replace(current_prefix, archive_prefix, regex=False)
            )
    if "source_paths" in payload.columns:
        payload["source_paths"] = (
            payload["source_paths"]
            .fillna("")
            .astype(str)
            .str.replace(current_prefix, archive_prefix, regex=False)
        )
    payload = _attach_resolved_paths(payload, root)
    payload["track_id"] = np.where(
        payload.get("artifact_group", pd.Series(dtype=str)).astype(str).eq("publication/comparator_pygnome"),
        "A",
        "B1",
    )
    return _attach_status_fields(payload)


def mindoro_b1_summary(repo_root: str | Path | None = None) -> pd.DataFrame:
    return _attach_status_fields(
        read_csv(MINDORO_FINAL_DIR / "summary" / "opendrift_primary" / "march13_14_reinit_summary.csv", repo_root)
    )


def mindoro_b1_fss(repo_root: str | Path | None = None) -> pd.DataFrame:
    return _attach_status_fields(
        read_csv(MINDORO_FINAL_DIR / "summary" / "opendrift_primary" / "march13_14_reinit_fss_by_window.csv", repo_root)
    )


def mindoro_comparator_summary(repo_root: str | Path | None = None) -> pd.DataFrame:
    return _attach_status_fields(
        read_csv(
            MINDORO_FINAL_DIR / "summary" / "comparator_pygnome" / "march13_14_reinit_crossmodel_summary.csv",
            repo_root,
        )
    )


def mindoro_comparator_ranking(repo_root: str | Path | None = None) -> pd.DataFrame:
    return _attach_status_fields(
        read_csv(
            MINDORO_FINAL_DIR / "summary" / "comparator_pygnome" / "march13_14_reinit_crossmodel_model_ranking.csv",
            repo_root,
        )
    )


def dwh_final_manifest(repo_root: str | Path | None = None) -> dict[str, Any]:
    return read_json(DWH_FINAL_DIR / "manifests" / "phase3c_final_output_manifest.json", repo_root)


def dwh_final_readme(repo_root: str | Path | None = None) -> str:
    return read_text(DWH_FINAL_DIR / "README.md", repo_root)


def dwh_final_registry(repo_root: str | Path | None = None) -> pd.DataFrame:
    root = _root(repo_root)
    payload = _prepare_curated_registry(
        read_csv(DWH_FINAL_DIR / "manifests" / "phase3c_final_output_registry.csv", root),
        repo_root=root,
        default_case_id=DWH_CASE_ID,
    )
    if payload.empty:
        return payload
    status_key_map = {
        "publication/observations": "dwh_observation_truth_context",
        "publication/opendrift_deterministic": "dwh_deterministic_transfer",
        "publication/opendrift_ensemble": "dwh_ensemble_transfer",
        "publication/comparator_pygnome": "dwh_crossmodel_comparator",
        "publication/context_optional": "dwh_trajectory_context",
    }
    status_rows = []
    for artifact_group in payload.get("artifact_group", pd.Series(dtype=str)).astype(str):
        status_rows.append(artifact_status_columns_for_key(status_key_map.get(artifact_group, "dwh_trajectory_context")))
    status_df = pd.DataFrame(status_rows, index=payload.index)
    for column in status_df.columns:
        payload[column] = status_df[column]
    return payload


def dwh_deterministic_summary_final(repo_root: str | Path | None = None) -> pd.DataFrame:
    return read_csv(DWH_FINAL_DIR / "summary" / "deterministic" / "phase3c_summary.csv", repo_root)


def dwh_ensemble_summary_final(repo_root: str | Path | None = None) -> pd.DataFrame:
    return read_csv(DWH_FINAL_DIR / "summary" / "ensemble" / "phase3c_ensemble_summary.csv", repo_root)


def dwh_comparator_summary_final(repo_root: str | Path | None = None) -> pd.DataFrame:
    return read_csv(DWH_FINAL_DIR / "summary" / "comparator_pygnome" / "phase3c_dwh_pygnome_summary.csv", repo_root)


def dwh_all_results_final(repo_root: str | Path | None = None) -> pd.DataFrame:
    return read_csv(
        DWH_FINAL_DIR / "summary" / "comparator_pygnome" / "phase3c_dwh_all_results_table.csv",
        repo_root,
    )


def dwh_main_scorecard_final(repo_root: str | Path | None = None) -> pd.DataFrame:
    return read_csv(DWH_FINAL_DIR / "summary" / "comparison" / "phase3c_main_scorecard.csv", repo_root)


def dwh_interpretation_note_final(repo_root: str | Path | None = None) -> str:
    return read_text(DWH_FINAL_DIR / "summary" / "comparison" / "phase3c_interpretation_note.md", repo_root)


def dwh_output_matrix_decision_note_final(repo_root: str | Path | None = None) -> str:
    return read_text(DWH_FINAL_DIR / "summary" / "comparison" / "phase3c_output_matrix_decision_note.md", repo_root)


def legacy_2016_final_manifest(repo_root: str | Path | None = None) -> dict[str, Any]:
    return read_json(LEGACY_2016_FINAL_DIR / "manifests" / "legacy_final_output_manifest.json", repo_root)


def legacy_2016_final_readme(repo_root: str | Path | None = None) -> str:
    return read_text(LEGACY_2016_FINAL_DIR / "README.md", repo_root)


def legacy_2016_provenance_metadata(repo_root: str | Path | None = None) -> dict[str, Any]:
    return read_json(LEGACY_2016_FINAL_DIR / "manifests" / "prototype_2016_provenance_metadata.json", repo_root)


def legacy_2016_final_registry(repo_root: str | Path | None = None) -> pd.DataFrame:
    root = _root(repo_root)
    payload = _prepare_curated_registry(
        read_csv(LEGACY_2016_FINAL_DIR / "manifests" / "prototype_2016_final_output_registry.csv", root),
        repo_root=root,
    )
    if payload.empty:
        return payload
    return _apply_status_key(payload, "prototype_2016_support")


def legacy_2016_phase4_registry(repo_root: str | Path | None = None) -> pd.DataFrame:
    return read_csv(LEGACY_2016_FINAL_DIR / "summary" / "phase4" / "prototype_2016_phase4_registry.csv", repo_root)


def legacy_2016_phase4_comparator_registry(repo_root: str | Path | None = None) -> pd.DataFrame:
    return read_csv(
        LEGACY_2016_FINAL_DIR / "summary" / "phase4_comparator" / "prototype_2016_phase4_pygnome_comparator_registry.csv",
        repo_root,
    )


def legacy_2016_phase4_comparator_decision_note(repo_root: str | Path | None = None) -> str:
    return read_text(
        LEGACY_2016_FINAL_DIR / "summary" / "phase4_comparator" / "prototype_2016_phase4_pygnome_decision_note.md",
        repo_root,
    )


def legacy_2016_phase3a_similarity(repo_root: str | Path | None = None) -> pd.DataFrame:
    return read_csv(LEGACY_2016_FINAL_DIR / "summary" / "phase3a" / "prototype_pygnome_similarity_by_case.csv", repo_root)


def legacy_2016_phase3a_fss(repo_root: str | Path | None = None) -> pd.DataFrame:
    return read_csv(LEGACY_2016_FINAL_DIR / "summary" / "phase3a" / "prototype_pygnome_fss_by_case_window.csv", repo_root)


def legacy_2016_packaging_summary(repo_root: str | Path | None = None) -> str:
    return read_text(LEGACY_2016_FINAL_DIR / "phase5" / "prototype_2016_packaging_summary.md", repo_root)


def curated_package_roots(repo_root: str | Path | None = None) -> list[dict[str, Any]]:
    root = _root(repo_root)
    mindoro_registry = mindoro_final_registry(root)
    dwh_registry = dwh_final_registry(root)
    legacy_registry = legacy_2016_final_registry(root)
    publication_registry_df = publication_registry(root)
    output_catalog = final_output_catalog(root)
    mindoro_primary_artifacts = mindoro_registry.loc[
        mindoro_registry.get("status_key", pd.Series(dtype=str)).astype(str).eq("mindoro_primary_validation")
    ]
    mindoro_comparator_artifacts = mindoro_registry.loc[
        mindoro_registry.get("status_key", pd.Series(dtype=str)).astype(str).eq("mindoro_crossmodel_comparator")
    ]
    mindoro_archive_artifacts = publication_registry_df.loc[
        publication_registry_df.get("case_id", pd.Series(dtype=str)).astype(str).eq(MINDORO_CASE_ID)
        & publication_registry_df.get("surface_key", pd.Series(dtype=str)).astype(str).isin(MINDORO_ARCHIVE_SURFACE_KEYS)
    ]
    return [
        {
            "package_id": "mindoro_b1_final",
            "label": "Mindoro B1 primary validation package",
            "page_label": "Mindoro B1 Primary Validation",
            "relative_path": str(MINDORO_FINAL_DIR),
            "description": "Curated thesis-facing package for the Mindoro March 13 -> March 14 R1 primary validation row built from stored outputs only.",
            "secondary_note": "Main paper uses March 13 -> March 14 R1 only.",
            "artifact_count": int(len(mindoro_primary_artifacts)),
        },
        {
            "package_id": "mindoro_comparator",
            "label": "Mindoro Track A comparator support package",
            "page_label": "Mindoro Track A Comparator Support",
            "relative_path": str(MINDORO_FINAL_DIR / "publication" / "comparator_pygnome"),
            "description": "Curated comparator-only subgroup for the thesis-facing March 14 Track A support view after archived R0-only materials were moved out of the main story.",
            "secondary_note": "Track A is comparator-only; archived R0 outputs live on the archive page.",
            "artifact_count": int(len(mindoro_comparator_artifacts)),
        },
        {
            "package_id": "mindoro_validation_archive",
            "label": "Mindoro validation archive",
            "page_label": "Archive — Mindoro Validation Provenance",
            "relative_path": str(FINAL_VALIDATION_DIR),
            "description": "Archived March13-14 R0 baseline, older R0-including March13-14 outputs, and preserved March-family legacy rows retained for provenance only.",
            "secondary_note": "Archive-only; not thesis-facing evidence.",
            "artifact_count": int(len(mindoro_archive_artifacts)),
        },
        {
            "package_id": "dwh_phase3c_final",
            "label": "DWH external transfer-validation package",
            "page_label": "DWH External Transfer Validation",
            "relative_path": str(DWH_FINAL_DIR),
            "description": "Curated frozen DWH transfer-validation package with C1/C2/C3 kept separate and explicit.",
            "secondary_note": "No drifter baseline is used for DWH.",
            "artifact_count": int(len(dwh_registry)),
        },
        {
            "package_id": "legacy_2016_final",
            "label": "Legacy 2016 final package",
            "page_label": "Archive — Legacy 2016 Support",
            "relative_path": str(LEGACY_2016_FINAL_DIR),
            "description": "Authoritative curated support-only package for the thesis-facing prototype_2016 legacy flow.",
            "secondary_note": "Support-only; visible flow is Phase 1 / 2 / 3A / 4 / 5.",
            "artifact_count": int(len(legacy_registry)),
        },
        {
            "package_id": "phase4_context_status",
            "label": "Mindoro oil-type and shoreline support/context",
            "page_label": "Mindoro Oil-Type and Shoreline Context",
            "relative_path": str(PHASE4_DIR),
            "description": "Mindoro oil-type and shoreline support/context outputs with the no-matched-comparator decision already folded into the plain-language page.",
            "secondary_note": "No matched PyGNOME fate-and-shoreline comparison is packaged yet.",
            "artifact_count": int(
                len(
                    publication_registry_df.loc[
                        publication_registry_df.get("status_key", pd.Series(dtype=str)).astype(str).isin(
                            ["mindoro_phase4_oil_budget", "mindoro_phase4_shoreline", "mindoro_phase4_deferred"]
                        )
                    ]
                )
            ),
        },
        {
            "package_id": "artifacts_and_logs",
            "label": "Artifacts, manifests, and logs",
            "page_label": "Artifacts / Logs / Registries",
            "relative_path": str(FINAL_REPRO_DIR),
            "description": "Synced reproducibility indexes, final validation package pointers, manifests, and log catalogs.",
            "secondary_note": "Curated registries are the safest entry point for thesis indexing and audit work.",
            "artifact_count": int(len(output_catalog)),
        },
    ]


def mindoro_phase3b_summary(repo_root: str | Path | None = None) -> pd.DataFrame:
    return read_csv(MINDORO_DIR / "phase3b" / "phase3b_summary.csv", repo_root)


def mindoro_model_ranking(repo_root: str | Path | None = None) -> pd.DataFrame:
    return read_csv(MINDORO_DIR / "pygnome_public_comparison" / "pygnome_public_comparison_model_ranking.csv", repo_root)


def phase1_focused_manifest(repo_root: str | Path | None = None) -> dict[str, Any]:
    return read_json(PHASE1_FOCUSED_DIR / "phase1_production_manifest.json", repo_root)


def phase1_focused_recipe_ranking(repo_root: str | Path | None = None) -> pd.DataFrame:
    return read_csv(PHASE1_FOCUSED_DIR / "phase1_recipe_ranking.csv", repo_root)


def phase1_focused_recipe_summary(repo_root: str | Path | None = None) -> pd.DataFrame:
    return read_csv(PHASE1_FOCUSED_DIR / "phase1_recipe_summary.csv", repo_root)


def phase1_focused_accepted_segments(repo_root: str | Path | None = None) -> pd.DataFrame:
    return read_csv(PHASE1_FOCUSED_DIR / "phase1_accepted_segment_registry.csv", repo_root)


def phase1_focused_ranking_subset(repo_root: str | Path | None = None) -> pd.DataFrame:
    return read_csv(PHASE1_FOCUSED_DIR / "phase1_ranking_subset_registry.csv", repo_root)


def phase1_focused_loading_audit(repo_root: str | Path | None = None) -> pd.DataFrame:
    return read_csv(PHASE1_FOCUSED_DIR / "phase1_loading_audit.csv", repo_root)


def phase1_focused_baseline_candidate(repo_root: str | Path | None = None) -> str:
    return read_text(PHASE1_FOCUSED_DIR / "phase1_baseline_selection_candidate.yaml", repo_root)


def phase1_focused_ranking_subset_report(repo_root: str | Path | None = None) -> str:
    return read_text(PHASE1_FOCUSED_DIR / "phase1_ranking_subset_report.md", repo_root)


def phase1_focused_segment_metrics(repo_root: str | Path | None = None) -> pd.DataFrame:
    return read_csv(PHASE1_FOCUSED_DIR / "phase1_segment_metrics.csv", repo_root)


def phase1_focused_drifter_registry(repo_root: str | Path | None = None) -> pd.DataFrame:
    return read_csv(PHASE1_FOCUSED_DIR / "phase1_drifter_registry.csv", repo_root)


def mindoro_b1_run_manifest(repo_root: str | Path | None = None) -> dict[str, Any]:
    return read_json(MINDORO_FINAL_DIR / "summary" / "opendrift_primary" / "march13_14_reinit_run_manifest.json", repo_root)


def _load_dataframe_discovery_payload(
    candidate_groups: list[tuple[str, list[str]]],
    *,
    repo_root: str | Path | None = None,
) -> dict[str, Any]:
    discovery = _discover_first_matching_path(candidate_groups, repo_root=repo_root)
    path = discovery["path"]
    data = read_csv(path, repo_root) if path else pd.DataFrame()
    return {**discovery, "data": data}


def _load_json_discovery_payload(
    candidate_groups: list[tuple[str, list[str]]],
    *,
    repo_root: str | Path | None = None,
) -> dict[str, Any]:
    discovery = _discover_first_matching_path(candidate_groups, repo_root=repo_root)
    path = discovery["path"]
    data = read_json(path, repo_root) if path else {}
    return {**discovery, "data": data}


def load_phase1_focused_manifest(repo_root: str | Path | None = None) -> dict[str, Any]:
    return _load_json_discovery_payload(
        [
            (
                "curated output package artifacts",
                [
                    "output/panel_drifter_context/**/*phase1*manifest*.json",
                ],
            ),
            (
                "focused Phase 1 output artifacts",
                [
                    "output/phase1_mindoro_focus_pre_spill_2016_2023/phase1_production_manifest.json",
                    "output/phase1_mindoro_focus_pre_spill_2016_2023/**/*phase1*manifest*.json",
                ],
            ),
            (
                "persistent historical input inventories",
                [
                    "data/historical_validation_inputs/phase1_mindoro_focus_pre_spill_2016_2023/**/*manifest*.json",
                ],
            ),
        ],
        repo_root=repo_root,
    )


def load_phase1_focused_accepted_segments(repo_root: str | Path | None = None) -> dict[str, Any]:
    return _load_dataframe_discovery_payload(
        [
            (
                "curated output package artifacts",
                [
                    "output/panel_drifter_context/**/*accepted*segment*.csv",
                    "output/panel_drifter_context/**/*accepted*.csv",
                ],
            ),
            (
                "focused Phase 1 output artifacts",
                [
                    "output/phase1_mindoro_focus_pre_spill_2016_2023/phase1_accepted_segment_registry.csv",
                    "output/phase1_mindoro_focus_pre_spill_2016_2023/**/*accepted*segment*.csv",
                    "output/phase1_mindoro_focus_pre_spill_2016_2023/**/*accepted*.csv",
                ],
            ),
            (
                "persistent historical input inventories",
                [
                    "data/historical_validation_inputs/phase1_mindoro_focus_pre_spill_2016_2023/**/*accepted*.csv",
                ],
            ),
        ],
        repo_root=repo_root,
    )


def load_phase1_focused_ranking_subset(repo_root: str | Path | None = None) -> dict[str, Any]:
    return _load_dataframe_discovery_payload(
        [
            (
                "curated output package artifacts",
                [
                    "output/panel_drifter_context/**/*ranking*subset*.csv",
                ],
            ),
            (
                "focused Phase 1 output artifacts",
                [
                    "output/phase1_mindoro_focus_pre_spill_2016_2023/phase1_ranking_subset_registry.csv",
                    "output/phase1_mindoro_focus_pre_spill_2016_2023/**/*ranking*subset*.csv",
                ],
            ),
            (
                "persistent historical input inventories",
                [
                    "data/historical_validation_inputs/phase1_mindoro_focus_pre_spill_2016_2023/**/*ranking*subset*.csv",
                ],
            ),
        ],
        repo_root=repo_root,
    )


def load_phase1_focused_drifter_registry(repo_root: str | Path | None = None) -> dict[str, Any]:
    return _load_dataframe_discovery_payload(
        [
            (
                "curated output package artifacts",
                [
                    "output/panel_drifter_context/**/*drifter*registry*.csv",
                    "output/panel_drifter_context/**/*segment*.csv",
                ],
            ),
            (
                "focused Phase 1 output artifacts",
                [
                    "output/phase1_mindoro_focus_pre_spill_2016_2023/phase1_drifter_registry.csv",
                    "output/phase1_mindoro_focus_pre_spill_2016_2023/**/*drifter*registry*.csv",
                ],
            ),
            (
                "persistent historical input inventories",
                [
                    "data/historical_validation_inputs/phase1_mindoro_focus_pre_spill_2016_2023/**/*.csv",
                ],
            ),
            (
                "fallback drifter CSVs",
                [
                    "data/drifters/**/*.csv",
                ],
            ),
        ],
        repo_root=repo_root,
    )


def _direct_march13_14_mask(df: pd.DataFrame) -> pd.Series:
    if df.empty:
        return pd.Series(dtype=bool)
    if "start_time_utc" not in df.columns and "end_time_utc" not in df.columns:
        return pd.Series(False, index=df.index)
    start_series = df.get("start_time_utc", pd.Series("", index=df.index)).fillna("").astype(str)
    end_series = df.get("end_time_utc", pd.Series("", index=df.index)).fillna("").astype(str)
    pattern = r"2023-03-13|2023-03-14"
    return start_series.str.contains(pattern, na=False) | end_series.str.contains(pattern, na=False)


def _segment_table_for_display(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    payload = df.copy()
    if "start_time_utc" in payload.columns and "end_time_utc" in payload.columns:
        start_times = pd.to_datetime(payload["start_time_utc"], errors="coerce", utc=True)
        end_times = pd.to_datetime(payload["end_time_utc"], errors="coerce", utc=True)
        payload["duration_h"] = ((end_times - start_times).dt.total_seconds() / 3600.0).round(2)
        payload["month"] = start_times.dt.month.astype("Int64")
    if "drogue_status" not in payload.columns:
        if "drogue_attached_through_window" in payload.columns:
            payload["drogue_status"] = payload["drogue_attached_through_window"].astype(str)
        elif "drogue_status_complete" in payload.columns:
            payload["drogue_status"] = payload["drogue_status_complete"].astype(str)
    preferred_columns = [
        "segment_id",
        "drifter_id",
        "platform_id",
        "start_time_utc",
        "end_time_utc",
        "start_lat",
        "start_lon",
        "end_lat",
        "end_lon",
        "duration_h",
        "drogue_status",
        "drogue_attached_through_window",
        "month",
        "month_key",
        "ranking_subset_label",
        "ranking_subset_included",
    ]
    selected_columns = [column for column in preferred_columns if column in payload.columns]
    remaining_columns = [column for column in payload.columns if column not in selected_columns]
    return payload[selected_columns + remaining_columns].reset_index(drop=True)


def _segment_metrics_wide(segment_metrics: pd.DataFrame, segment_ids: list[str]) -> pd.DataFrame:
    if segment_metrics.empty or "segment_id" not in segment_metrics.columns or "recipe" not in segment_metrics.columns:
        return pd.DataFrame()
    payload = segment_metrics.loc[segment_metrics["segment_id"].astype(str).isin(segment_ids)].copy()
    if payload.empty or "ncs_score" not in payload.columns:
        return pd.DataFrame()
    payload["recipe_metric_column"] = (
        payload["recipe"].astype(str).str.strip().str.replace(r"[^0-9a-zA-Z]+", "_", regex=True).str.lower()
    )
    wide = payload.pivot_table(
        index="segment_id",
        columns="recipe_metric_column",
        values="ncs_score",
        aggfunc="first",
    )
    if wide.empty:
        return pd.DataFrame()
    wide = wide.reset_index()
    wide.columns = [
        "segment_id" if str(column) == "segment_id" else f"ncs_score_{column}"
        for column in wide.columns
    ]
    return wide


def _box_bounds_from_payload(*payloads: dict[str, Any], key: str) -> list[float]:
    for payload in payloads:
        values = payload.get(key)
        if isinstance(values, list) and len(values) == 4:
            try:
                return [float(value) for value in values]
            except (TypeError, ValueError):
                continue
    return []


def load_b1_drifter_context(repo_root: str | Path | None = None) -> dict[str, Any]:
    root = _root(repo_root)
    manifest_payload = load_phase1_focused_manifest(root)
    accepted_payload = load_phase1_focused_accepted_segments(root)
    subset_payload = load_phase1_focused_ranking_subset(root)
    drifter_registry_payload = load_phase1_focused_drifter_registry(root)
    segment_metrics = phase1_focused_segment_metrics(root)
    recipe_ranking = phase1_focused_recipe_ranking(root)
    recipe_summary = phase1_focused_recipe_summary(root)
    b1_manifest = mindoro_b1_run_manifest(root)
    phase1_config = read_yaml(PHASE1_FOCUSED_CONFIG_PATH, root)
    case_config = read_yaml(MINDORO_CASE_CONFIG_PATH, root)
    settings_config = read_yaml(SETTINGS_PATH, root)

    accepted = accepted_payload["data"]
    subset = subset_payload["data"]
    manifest = manifest_payload["data"]
    subset_ids = subset.get("segment_id", pd.Series(dtype=str)).astype(str).tolist() if not subset.empty else []
    subset_metrics = _segment_metrics_wide(segment_metrics, subset_ids)
    subset_display = _segment_table_for_display(subset)
    if not subset_display.empty and not subset_metrics.empty:
        subset_display = subset_display.merge(subset_metrics, on="segment_id", how="left")
    accepted_display = _segment_table_for_display(accepted)

    direct_accepted = accepted.loc[_direct_march13_14_mask(accepted)].reset_index(drop=True) if not accepted.empty else pd.DataFrame()
    direct_subset = subset.loc[_direct_march13_14_mask(subset)].reset_index(drop=True) if not subset.empty else pd.DataFrame()

    panel_context_manifest = _load_json_discovery_payload(
        [
            (
                "curated output package artifacts",
                [
                    "output/panel_drifter_context/b1_drifter_context_manifest.json",
                    "output/panel_drifter_context/**/*b1*drifter*context*manifest*.json",
                ],
            ),
        ],
        repo_root=root,
    )
    panel_context_map = _discover_first_matching_path(
        [
            (
                "curated output package artifacts",
                [
                    "output/panel_drifter_context/**/*b1*drifter*context*.png",
                    "output/panel_drifter_context/**/*drifter*context*.png",
                    "output/figure_package_publication/**/*accepted*segment*.png",
                    "output/figure_package_publication/**/*phase1*drifter*.png",
                    "output/trajectory_gallery_panel/**/*phase1*drifter*.png",
                    "output/chapter5_generated/**/*focused*phase1*segment*map*.png",
                ],
            ),
        ],
        repo_root=root,
    )
    panel_context_map_metadata = _load_json_discovery_payload(
        [
            (
                "curated output package artifacts",
                [
                    "output/panel_drifter_context/b1_drifter_context_map.json",
                    "output/panel_drifter_context/**/*b1*drifter*context*map*.json",
                    "output/chapter5_generated/**/*focused*phase1*segment*map*.json",
                ],
            ),
        ],
        repo_root=root,
    )

    official_recipe = (
        str(((b1_manifest.get("recipe") or {}).get("recipe")) or "").strip()
        or str(manifest.get("official_b1_recipe") or "").strip()
        or (str(recipe_ranking.iloc[0]["recipe"]).strip() if not recipe_ranking.empty and "recipe" in recipe_ranking.columns else "")
    )
    winner = (
        str(manifest.get("historical_four_recipe_winner") or manifest.get("winning_recipe") or "").strip()
        or (str(recipe_ranking.iloc[0]["recipe"]).strip() if not recipe_ranking.empty and "recipe" in recipe_ranking.columns else "")
    )
    ranking_subset_label = str((manifest.get("ranking_subset") or {}).get("label") or "").strip()
    ranking_subset_description = str((((manifest.get("ranking_subset") or {}).get("config") or {}).get("description")) or "").strip()
    accepted_count = int(manifest.get("accepted_segment_count") or len(accepted))
    ranking_subset_count = int(((manifest.get("ranking_subset") or {}).get("segment_count")) or len(subset))
    direct_segment_note = (
        "No direct March 13-14 2023 accepted drifter segment is stored for B1. "
        "The displayed drifter data is the historical focused Phase 1 provenance set used for recipe selection."
    )
    if not direct_accepted.empty or not direct_subset.empty:
        direct_segment_note = (
            "Directly dated March 13-14 drifter records were found in stored files, but they remain supplementary "
            "context only and are not treated as the B1 public-observation truth mask here."
        )

    return {
        "title": "B1 Recipe Provenance — Not Truth Mask",
        "page_note": (
            "These drifter records support the selected transport recipe used by B1. "
            "They are not the direct truth mask for the March 13-14 public-observation validation row."
        ),
        "evidence_boundary_note": (
            "The March 13 -> March 14 B1 row is validated against public observation masks, not against drifter tracks. "
            "The drifter data shown here belongs to the separate focused Phase 1 provenance lane that selected the "
            "transport recipe inherited by B1."
        ),
        "b1_case_label": "March 13 -> March 14",
        "provenance_lane": "phase1_mindoro_focus_pre_spill_2016_2023",
        "claim_boundary": (
            "Drifter records support B1 recipe provenance only; they are not the direct March 13-14 "
            "public-observation truth mask."
        ),
        "status_messages": [
            manifest_payload["status_message"],
            accepted_payload["status_message"],
            subset_payload["status_message"],
            drifter_registry_payload["status_message"],
            panel_context_manifest["status_message"],
            panel_context_map["status_message"],
            panel_context_map_metadata["status_message"],
        ],
        "phase1_manifest": manifest,
        "phase1_manifest_source_path": manifest_payload["relative_path"],
        "accepted_segments": accepted,
        "accepted_segments_display": accepted_display,
        "accepted_segments_source_path": accepted_payload["relative_path"],
        "ranking_subset": subset,
        "ranking_subset_display": subset_display,
        "ranking_subset_source_path": subset_payload["relative_path"],
        "drifter_registry": drifter_registry_payload["data"],
        "drifter_registry_source_path": drifter_registry_payload["relative_path"],
        "segment_metrics": segment_metrics,
        "recipe_ranking": recipe_ranking,
        "recipe_summary": recipe_summary,
        "b1_run_manifest": b1_manifest,
        "panel_context_manifest": panel_context_manifest["data"],
        "panel_context_manifest_source_path": panel_context_manifest["relative_path"],
        "panel_context_map_figure_path": panel_context_map["relative_path"],
        "panel_context_map_figure_status": panel_context_map["status_message"],
        "panel_context_map_metadata": panel_context_map_metadata["data"],
        "panel_context_map_metadata_source_path": panel_context_map_metadata["relative_path"],
        "official_b1_recipe": official_recipe,
        "winning_recipe": winner,
        "accepted_segment_count": accepted_count,
        "ranking_subset_count": ranking_subset_count,
        "ranking_subset_label": ranking_subset_label,
        "ranking_subset_description": ranking_subset_description,
        "direct_accepted_segments": direct_accepted,
        "direct_accepted_segments_display": _segment_table_for_display(direct_accepted),
        "direct_subset_segments": direct_subset,
        "direct_subset_segments_display": _segment_table_for_display(direct_subset),
        "direct_dated_segments_found": bool(not direct_accepted.empty or not direct_subset.empty),
        "direct_segment_note": direct_segment_note,
        "phase1_validation_box": _box_bounds_from_payload(
            manifest,
            phase1_config,
            settings_config,
            key="validation_box",
        )
        or _box_bounds_from_payload(phase1_config, settings_config, key="phase1_validation_box"),
        "mindoro_case_domain": _box_bounds_from_payload(case_config, settings_config, key="mindoro_case_domain"),
        "source_point": (
            ((manifest.get("distance_audit") or {}).get("source_point"))
            or {}
        ),
    }


def phase1_reference_manifest(repo_root: str | Path | None = None) -> dict[str, Any]:
    return read_json(PHASE1_REFERENCE_DIR / "phase1_production_manifest.json", repo_root)


def phase1_reference_recipe_ranking(repo_root: str | Path | None = None) -> pd.DataFrame:
    return read_csv(PHASE1_REFERENCE_DIR / "phase1_recipe_ranking.csv", repo_root)


def phase1_reference_recipe_summary(repo_root: str | Path | None = None) -> pd.DataFrame:
    return read_csv(PHASE1_REFERENCE_DIR / "phase1_recipe_summary.csv", repo_root)


def phase1_reference_baseline_candidate(repo_root: str | Path | None = None) -> str:
    return read_text(PHASE1_REFERENCE_DIR / "phase1_baseline_selection_candidate.yaml", repo_root)


def dwh_summary(repo_root: str | Path | None = None) -> pd.DataFrame:
    return read_csv(DWH_DIR / "phase3c_external_case_run" / "phase3c_summary.csv", repo_root)


def dwh_all_results(repo_root: str | Path | None = None) -> pd.DataFrame:
    return read_csv(DWH_DIR / "phase3c_dwh_pygnome_comparator" / "phase3c_dwh_all_results_table.csv", repo_root)


def phase4_budget_summary(repo_root: str | Path | None = None) -> pd.DataFrame:
    return read_csv(PHASE4_DIR / "phase4_oil_budget_summary.csv", repo_root)


def phase4_oiltype_comparison(repo_root: str | Path | None = None) -> pd.DataFrame:
    return read_csv(PHASE4_DIR / "phase4_oiltype_comparison.csv", repo_root)


def phase4_shoreline_arrival(repo_root: str | Path | None = None) -> pd.DataFrame:
    return read_csv(PHASE4_DIR / "phase4_shoreline_arrival.csv", repo_root)


def phase4_shoreline_segments(repo_root: str | Path | None = None) -> pd.DataFrame:
    return read_csv(PHASE4_DIR / "phase4_shoreline_segments.csv", repo_root)


def phase4_crossmodel_matrix(repo_root: str | Path | None = None) -> pd.DataFrame:
    return read_csv(PHASE4_AUDIT_DIR / "phase4_crossmodel_comparability_matrix.csv", repo_root)


def phase4_crossmodel_verdict(repo_root: str | Path | None = None) -> str:
    return read_text(PHASE4_AUDIT_DIR / "phase4_crossmodel_final_verdict.md", repo_root)


def phase4_crossmodel_blockers(repo_root: str | Path | None = None) -> str:
    return read_text(PHASE4_AUDIT_DIR / "phase4_crossmodel_blockers.md", repo_root)


def phase4_crossmodel_next_steps(repo_root: str | Path | None = None) -> str:
    return read_text(PHASE4_AUDIT_DIR / "phase4_crossmodel_minimal_next_steps.md", repo_root)


def publication_captions(repo_root: str | Path | None = None) -> str:
    return read_text(PUBLICATION_DIR / "publication_figure_captions.md", repo_root)


def publication_talking_points(repo_root: str | Path | None = None) -> str:
    return read_text(PUBLICATION_DIR / "publication_figure_talking_points.md", repo_root)


HOME_STORY_PAGE_ORDER: dict[str, int] = {
    "phase1_recipe_selection": 10,
    "mindoro_validation": 20,
    "cross_model_comparison": 30,
    "dwh_transfer_validation": 40,
    "phase4_oiltype_and_shoreline": 50,
    "legacy_2016_support": 60,
}

HOME_FEATURED_EXCLUDED_MINDORO_STATUS_KEYS: frozenset[str] = frozenset(
    {
        "mindoro_b1_r0_archive",
        "mindoro_legacy_march6",
        "mindoro_legacy_support",
        "mindoro_phase4_oil_budget",
        "mindoro_phase4_shoreline",
        "mindoro_phase4_deferred",
    }
)

HOME_FEATURED_EXCLUDED_MINDORO_PAGE_TARGETS: frozenset[str] = frozenset(
    {
        "mindoro_validation_archive",
        "phase4_oiltype_and_shoreline",
        "phase4_crossmodel_status",
    }
)

HOME_FEATURED_EXCLUDED_MINDORO_PHASES: frozenset[str] = frozenset(
    {
        "phase3b_legacy_strict",
        "phase3b_support",
        "phase4",
        "phase4_crossmodel_comparability_audit",
    }
)


def _sort_home_story_rows(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    payload = df.copy()
    page_targets = payload.get("page_target", pd.Series("", index=payload.index)).fillna("").astype(str)
    payload["_story_page_order"] = page_targets.map(HOME_STORY_PAGE_ORDER).fillna(999).astype(int)
    if "display_order" in payload.columns:
        payload["_display_order"] = pd.to_numeric(payload["display_order"], errors="coerce").fillna(9999).astype(int)
        sort_columns = ["_story_page_order", "_display_order", "figure_id"]
    else:
        sort_columns = ["_story_page_order", "figure_id"]
    payload = payload.sort_values(sort_columns, na_position="last").drop(
        columns=[column for column in ("_story_page_order", "_display_order") if column in payload.columns]
    )
    return payload.reset_index(drop=True)


def _preferred_defense_patterns() -> list[tuple[str, str]]:
    return [
        ("CASE_MINDORO_RETRO_2023", "mindoro_primary_validation_board"),
        ("CASE_MINDORO_RETRO_2023", "mindoro_crossmodel_board"),
        ("CASE_MINDORO_RETRO_2023", "march14_r1_previous_overlay"),
        ("CASE_MINDORO_RETRO_2023", "march14_crossmodel_r1_overlay"),
        ("CASE_MINDORO_RETRO_2023", "oil_budget_board"),
        ("CASE_MINDORO_RETRO_2023", "shoreline_impact_board"),
        ("CASE_DWH_RETRO_2010_72H", "daily_deterministic_board"),
        ("CASE_DWH_RETRO_2010_72H", "deterministic_vs_ensemble_board"),
        ("CASE_DWH_RETRO_2010_72H", "opendrift_vs_pygnome_board"),
    ]


def _home_overview_featured_patterns() -> list[str]:
    return [
        "legacy_2016_drifter_track_triptych_board",
        "legacy_2016_drifter_vs_mask_p50_mask_p90_triptych_board",
        "legacy_2016_mask_p50_mask_p90_vs_pygnome_triptych_board",
        "mindoro_observed_masks_ensemble_pygnome_board",
        "mindoro_observed_masks_ensemble_pygnome_overlay",
        "24h_48h_72h_mask_p50_footprint_overview_board",
        "24h_48h_72h_mask_p90_footprint_overview_board",
        "24h_48h_72h_mask_p50_mask_p90_dual_threshold_overview_board",
        "24h_48h_72h_mask_p50_vs_pygnome_overview_board",
        "24h_48h_72h_mask_p90_vs_pygnome_overview_board",
        "24h_48h_72h_mask_p50_mask_p90_dual_threshold_vs_pygnome_overview_board",
    ]


def _filter_home_featured_candidates(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df.copy()
    payload = df.copy()
    blocked_mask = pd.Series(False, index=payload.index)

    if "archive_only" in payload.columns:
        blocked_mask |= payload["archive_only"].fillna(False).astype(bool)
    if "legacy_support" in payload.columns:
        blocked_mask |= payload["legacy_support"].fillna(False).astype(bool)

    case_ids = payload.get("case_id", pd.Series("", index=payload.index)).fillna("").astype(str)
    status_keys = payload.get("status_key", pd.Series("", index=payload.index)).fillna("").astype(str)
    page_targets = payload.get("page_target", pd.Series("", index=payload.index)).fillna("").astype(str)
    phases = payload.get("phase_or_track", pd.Series("", index=payload.index)).fillna("").astype(str)
    mindoro_mask = case_ids.eq(MINDORO_CASE_ID)

    blocked_mask |= mindoro_mask & status_keys.isin(HOME_FEATURED_EXCLUDED_MINDORO_STATUS_KEYS)
    blocked_mask |= mindoro_mask & page_targets.isin(HOME_FEATURED_EXCLUDED_MINDORO_PAGE_TARGETS)
    blocked_mask |= mindoro_mask & phases.isin(HOME_FEATURED_EXCLUDED_MINDORO_PHASES)

    return payload.loc[~blocked_mask].reset_index(drop=True)


def parse_source_paths(value: Any, repo_root: str | Path | None = None) -> list[Path]:
    if value is None:
        return []
    tokens = [token.strip() for token in str(value).replace(";", "|").split("|")]
    paths: list[Path] = []
    for token in tokens:
        if not token:
            continue
        resolved = resolve_repo_path(token, repo_root)
        if resolved and resolved.exists():
            paths.append(resolved)
    return paths


def curated_recommended_figures(repo_root: str | Path | None = None) -> pd.DataFrame:
    registry = publication_registry(repo_root)
    if registry.empty:
        return registry
    manifest = publication_manifest(repo_root)
    recommended_ids = manifest.get("recommended_main_defense_figures") or []
    recommended = registry.loc[registry["figure_id"].isin(recommended_ids)].copy()
    if recommended.empty:
        recommended = registry.loc[registry.get("recommended_for_main_defense", pd.Series(dtype=bool)).fillna(False)].copy()
    recommended = _filter_surface_rows(recommended, require_recommended_visible=True)
    if "thesis_surface" in recommended.columns:
        recommended = recommended.loc[recommended["thesis_surface"].fillna(False).astype(bool)].copy()
    if "display_order" in recommended.columns:
        recommended = recommended.sort_values(["display_order", "figure_id"], na_position="last").reset_index(drop=True)
    return recommended.reset_index(drop=True)


def home_featured_publication_figures(repo_root: str | Path | None = None) -> pd.DataFrame:
    from ui.evidence_contract import assert_no_archive_leak, filter_for_page

    featured = _filter_surface_rows(curated_recommended_figures(repo_root), require_home_visible=True)
    registry = publication_registry(repo_root)
    phase1_context = registry.loc[
        (
            registry.get("page_target", pd.Series("", index=registry.index))
            .fillna("")
            .astype(str)
            .eq("phase1_recipe_selection")
        )
        & (
            registry.get("recommended_scope", pd.Series("", index=registry.index))
            .fillna("")
            .astype(str)
            .eq("main_text")
        )
    ].copy()
    phase1_context = _filter_surface_rows(phase1_context, require_home_visible=True)
    if "thesis_surface" in phase1_context.columns:
        phase1_context = phase1_context.loc[phase1_context["thesis_surface"].fillna(False).astype(bool)].copy()
    featured = pd.concat([phase1_context, featured], ignore_index=True)
    featured = _filter_home_featured_candidates(featured)
    if featured.empty:
        return featured
    if "figure_id" in featured.columns:
        featured = featured.drop_duplicates(subset=["figure_id"], keep="first")
    featured = filter_for_page(featured, "home", advanced=False)
    assert_no_archive_leak(featured, "home", advanced=False)
    return _sort_home_story_rows(featured)


def figure_subset(
    layer: str,
    *,
    repo_root: str | Path | None = None,
    case_id: str = "",
    family_codes: list[str] | None = None,
    status_keys: list[str] | None = None,
    surface_keys: list[str] | None = None,
    recommended_only: bool = False,
    text_filter: str = "",
) -> pd.DataFrame:
    if layer == "publication":
        df = publication_registry(repo_root)
        if recommended_only:
            manifest = publication_manifest(repo_root)
            recommended_ids = manifest.get("recommended_main_defense_figures") or []
            df = df.loc[df["figure_id"].isin(recommended_ids)].copy()
    elif layer == "panel":
        df = panel_registry(repo_root)
        if recommended_only and "recommended_for_main_defense" in df.columns:
            df = df.loc[df["recommended_for_main_defense"].fillna(False)].copy()
    else:
        df = raw_gallery_index(repo_root)
        if recommended_only and "ready_for_panel_presentation" in df.columns:
            df = df.loc[df["ready_for_panel_presentation"].fillna(False)].copy()
    if case_id:
        df = df.loc[df.get("case_id", pd.Series(dtype=str)).astype(str).eq(case_id)].copy()
    if family_codes:
        code_column = "figure_family_code" if "figure_family_code" in df.columns else "board_family_code" if "board_family_code" in df.columns else "figure_group_code"
        df = df.loc[df[code_column].astype(str).isin(family_codes)].copy()
    if status_keys:
        if "status_key" in df.columns:
            df = df.loc[df["status_key"].astype(str).isin(status_keys)].copy()
        else:
            records = df.to_dict(orient="records")
            mask = [any(record_matches_artifact_status(record, key) for key in status_keys) for record in records]
            df = df.loc[mask].copy()
    if surface_keys:
        df = _filter_surface_rows(df, surface_keys=surface_keys)
    if text_filter:
        lowered = text_filter.lower()
        searchable_columns = [column for column in df.columns if column in {"figure_id", "figure_family_label", "board_family_label", "figure_group_label", "model_names", "model_name", "notes", "short_plain_language_interpretation", "plain_language_interpretation"}]
        if searchable_columns:
            mask = pd.Series(False, index=df.index)
            for column in searchable_columns:
                mask |= df[column].astype(str).str.lower().str.contains(lowered, na=False)
            df = df.loc[mask].copy()
    if "display_order" in df.columns:
        df = df.sort_values(["display_order", "figure_id"], na_position="last").copy()
    return df.reset_index(drop=True)


def trajectory_figures(
    layer: str = "publication",
    *,
    repo_root: str | Path | None = None,
    case_id: str = "",
) -> pd.DataFrame:
    df = figure_subset(layer, repo_root=repo_root, case_id=case_id)
    if df.empty:
        return df
    keywords = ("trajectory", "track", "corridor", "hull", "centroid")
    mask = pd.Series(False, index=df.index)
    for column in ("figure_id", "run_type", "figure_family_label", "board_family_label", "figure_group_label", "figure_slug"):
        if column in df.columns:
            mask |= df[column].astype(str).str.lower().apply(lambda value: any(word in value for word in keywords))
    return df.loc[mask].reset_index(drop=True)


@lru_cache(maxsize=64)
def raster_summary(path_value: str, repo_root_text: str) -> dict[str, Any]:
    path = resolve_repo_path(path_value, repo_root_text)
    if path is None or not path.exists():
        return {}
    with rasterio.open(path) as dataset:
        return {
            "path": str(path),
            "crs": str(dataset.crs),
            "width": int(dataset.width),
            "height": int(dataset.height),
            "bounds": tuple(float(value) for value in dataset.bounds),
            "count": int(dataset.count),
            "dtype": str(dataset.dtypes[0]),
        }


@lru_cache(maxsize=32)
def vector_summary(path_value: str, repo_root_text: str) -> dict[str, Any]:
    path = resolve_repo_path(path_value, repo_root_text)
    if path is None or not path.exists():
        return {}
    gdf = gpd.read_file(path)
    return {
        "path": str(path),
        "feature_count": int(len(gdf)),
        "crs": str(gdf.crs) if gdf.crs else "",
        "bounds": tuple(float(value) for value in gdf.total_bounds) if not gdf.empty else (),
        "columns": list(gdf.columns),
    }


@lru_cache(maxsize=32)
def track_summary(path_value: str, repo_root_text: str) -> dict[str, Any]:
    path = resolve_repo_path(path_value, repo_root_text)
    if path is None or not path.exists():
        return {}
    with xr.open_dataset(path) as ds:
        variables = list(ds.variables.keys())
        dims = {name: int(size) for name, size in ds.sizes.items()}
        lon_name = "lon" if "lon" in ds.variables else "longitude" if "longitude" in ds.variables else ""
        lat_name = "lat" if "lat" in ds.variables else "latitude" if "latitude" in ds.variables else ""
        lon_span = ()
        lat_span = ()
        if lon_name:
            lon_values = np.asarray(ds[lon_name].values, dtype=float)
            lon_span = (float(np.nanmin(lon_values)), float(np.nanmax(lon_values)))
        if lat_name:
            lat_values = np.asarray(ds[lat_name].values, dtype=float)
            lat_span = (float(np.nanmin(lat_values)), float(np.nanmax(lat_values)))
    return {
        "path": str(path),
        "variables": variables,
        "dims": dims,
        "lon_span": lon_span,
        "lat_span": lat_span,
    }


def build_dashboard_state(repo_root: str | Path | None = None) -> dict[str, Any]:
    root = _root(repo_root)
    return {
        "repo_root": str(root),
        "phase_status": final_phase_status(root),
        "final_case_registry": final_case_registry(root),
        "final_output_catalog": final_output_catalog(root),
        "final_manifest_index": final_manifest_index(root),
        "final_log_index": final_log_index(root),
        "final_reproducibility_summary": final_reproducibility_summary(root),
        "paper_output_registry_markdown": paper_output_registry_markdown(root),
        "data_source_registry": data_source_registry(root),
        "panel_review_check_table": panel_review_check_table(root),
        "panel_review_check_markdown": panel_review_check_markdown(root),
        "panel_review_check_manifest": panel_review_check_manifest(root),
        "final_validation_manifest": final_validation_manifest(root),
        "final_validation_case_registry": final_validation_case_registry(root),
        "final_validation_limitations": final_validation_limitations(root),
        "publication_registry": publication_registry(root),
        "publication_manifest": publication_manifest(root),
        "publication_captions": publication_captions(root),
        "publication_talking_points": publication_talking_points(root),
        "panel_registry": panel_registry(root),
        "raw_gallery_index": raw_gallery_index(root),
        "mindoro_validation_archive_decision": mindoro_validation_archive_decision(root),
        "mindoro_final_manifest": mindoro_final_manifest(root),
        "mindoro_final_readme": mindoro_final_readme(root),
        "mindoro_final_registry": mindoro_final_registry(root),
        "mindoro_final_archive_registry": mindoro_final_archive_registry(root),
        "mindoro_b1_summary": mindoro_b1_summary(root),
        "mindoro_b1_fss": mindoro_b1_fss(root),
        "mindoro_comparator_summary": mindoro_comparator_summary(root),
        "mindoro_comparator_ranking": mindoro_comparator_ranking(root),
        "mindoro_phase3b_summary": mindoro_phase3b_summary(root),
        "mindoro_model_ranking": mindoro_model_ranking(root),
        "phase1_focused_manifest": phase1_focused_manifest(root),
        "phase1_focused_recipe_ranking": phase1_focused_recipe_ranking(root),
        "phase1_focused_recipe_summary": phase1_focused_recipe_summary(root),
        "phase1_focused_accepted_segments": phase1_focused_accepted_segments(root),
        "phase1_focused_ranking_subset": phase1_focused_ranking_subset(root),
        "phase1_focused_loading_audit": phase1_focused_loading_audit(root),
        "phase1_focused_baseline_candidate": phase1_focused_baseline_candidate(root),
        "phase1_focused_ranking_subset_report": phase1_focused_ranking_subset_report(root),
        "b1_drifter_context": load_b1_drifter_context(root),
        "phase1_reference_manifest": phase1_reference_manifest(root),
        "phase1_reference_recipe_ranking": phase1_reference_recipe_ranking(root),
        "phase1_reference_recipe_summary": phase1_reference_recipe_summary(root),
        "phase1_reference_baseline_candidate": phase1_reference_baseline_candidate(root),
        "dwh_final_manifest": dwh_final_manifest(root),
        "dwh_final_readme": dwh_final_readme(root),
        "dwh_final_registry": dwh_final_registry(root),
        "dwh_deterministic_summary_final": dwh_deterministic_summary_final(root),
        "dwh_ensemble_summary_final": dwh_ensemble_summary_final(root),
        "dwh_comparator_summary_final": dwh_comparator_summary_final(root),
        "dwh_all_results_final": dwh_all_results_final(root),
        "dwh_main_scorecard_final": dwh_main_scorecard_final(root),
        "dwh_interpretation_note_final": dwh_interpretation_note_final(root),
        "dwh_output_matrix_decision_note_final": dwh_output_matrix_decision_note_final(root),
        "dwh_summary": dwh_summary(root),
        "dwh_all_results": dwh_all_results(root),
        "legacy_2016_final_manifest": legacy_2016_final_manifest(root),
        "legacy_2016_final_readme": legacy_2016_final_readme(root),
        "legacy_2016_provenance_metadata": legacy_2016_provenance_metadata(root),
        "legacy_2016_final_registry": legacy_2016_final_registry(root),
        "legacy_2016_phase4_registry": legacy_2016_phase4_registry(root),
        "legacy_2016_phase4_comparator_registry": legacy_2016_phase4_comparator_registry(root),
        "legacy_2016_phase4_comparator_decision_note": legacy_2016_phase4_comparator_decision_note(root),
        "legacy_2016_phase3a_similarity": legacy_2016_phase3a_similarity(root),
        "legacy_2016_phase3a_fss": legacy_2016_phase3a_fss(root),
        "legacy_2016_packaging_summary": legacy_2016_packaging_summary(root),
        "phase4_budget_summary": phase4_budget_summary(root),
        "phase4_oiltype_comparison": phase4_oiltype_comparison(root),
        "phase4_shoreline_arrival": phase4_shoreline_arrival(root),
        "phase4_shoreline_segments": phase4_shoreline_segments(root),
        "phase4_crossmodel_matrix": phase4_crossmodel_matrix(root),
        "phase4_crossmodel_verdict": phase4_crossmodel_verdict(root),
        "phase4_crossmodel_blockers": phase4_crossmodel_blockers(root),
        "phase4_crossmodel_next_steps": phase4_crossmodel_next_steps(root),
        "curated_recommended_figures": curated_recommended_figures(root),
        "home_featured_publication_figures": home_featured_publication_figures(root),
        "curated_package_roots": curated_package_roots(root),
    }
