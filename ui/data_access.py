"""Cached read-only access helpers for the local dashboard."""

from __future__ import annotations

import copy
import json
from functools import lru_cache
from pathlib import Path
from typing import Any

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
import xarray as xr

REPO_ROOT = Path(__file__).resolve().parents[1]
FINAL_REPRO_DIR = Path("output") / "final_reproducibility_package"
FINAL_VALIDATION_DIR = Path("output") / "final_validation_package"
RAW_GALLERY_DIR = Path("output") / "trajectory_gallery"
PANEL_GALLERY_DIR = Path("output") / "trajectory_gallery_panel"
PUBLICATION_DIR = Path("output") / "figure_package_publication"
PHASE4_DIR = Path("output") / "phase4" / "CASE_MINDORO_RETRO_2023"
PHASE4_AUDIT_DIR = Path("output") / "phase4_crossmodel_comparability_audit"
MINDORO_DIR = Path("output") / "CASE_MINDORO_RETRO_2023"
DWH_DIR = Path("output") / "CASE_DWH_RETRO_2010_72H"


def _root(repo_root: str | Path | None = None) -> Path:
    return Path(repo_root).resolve() if repo_root else REPO_ROOT


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
def _cached_csv(path_text: str, repo_root_text: str) -> pd.DataFrame:
    path = resolve_repo_path(path_text, repo_root_text)
    if path is None or not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path)
    df = _drop_repeated_header_rows(df)
    for column in df.columns:
        df[column] = _normalize_series(df[column])
    return df


def read_csv(path: str | Path, repo_root: str | Path | None = None) -> pd.DataFrame:
    return _cached_csv(str(path), str(_root(repo_root))).copy()


@lru_cache(maxsize=128)
def _cached_json(path_text: str, repo_root_text: str) -> dict[str, Any]:
    path = resolve_repo_path(path_text, repo_root_text)
    if path is None or not path.exists():
        return {}
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle) or {}


def read_json(path: str | Path, repo_root: str | Path | None = None) -> dict[str, Any]:
    return copy.deepcopy(_cached_json(str(path), str(_root(repo_root))))


@lru_cache(maxsize=128)
def _cached_text(path_text: str, repo_root_text: str) -> str:
    path = resolve_repo_path(path_text, repo_root_text)
    if path is None or not path.exists():
        return ""
    return path.read_text(encoding="utf-8")


def read_text(path: str | Path, repo_root: str | Path | None = None) -> str:
    return _cached_text(str(path), str(_root(repo_root)))


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


def publication_registry(repo_root: str | Path | None = None) -> pd.DataFrame:
    root = _root(repo_root)
    return _attach_resolved_paths(read_csv(PUBLICATION_DIR / "publication_figure_registry.csv", root), root)


def publication_manifest(repo_root: str | Path | None = None) -> dict[str, Any]:
    return read_json(PUBLICATION_DIR / "publication_figure_manifest.json", repo_root)


def panel_registry(repo_root: str | Path | None = None) -> pd.DataFrame:
    root = _root(repo_root)
    return _attach_resolved_paths(read_csv(PANEL_GALLERY_DIR / "panel_figure_registry.csv", root), root)


def raw_gallery_index(repo_root: str | Path | None = None) -> pd.DataFrame:
    root = _root(repo_root)
    return _attach_resolved_paths(read_csv(RAW_GALLERY_DIR / "trajectory_gallery_index.csv", root), root)


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


def mindoro_phase3b_summary(repo_root: str | Path | None = None) -> pd.DataFrame:
    return read_csv(MINDORO_DIR / "phase3b" / "phase3b_summary.csv", repo_root)


def mindoro_model_ranking(repo_root: str | Path | None = None) -> pd.DataFrame:
    return read_csv(MINDORO_DIR / "pygnome_public_comparison" / "pygnome_public_comparison_model_ranking.csv", repo_root)


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


def _preferred_defense_patterns() -> list[tuple[str, str]]:
    return [
        ("CASE_MINDORO_RETRO_2023", "strict_board"),
        ("CASE_MINDORO_RETRO_2023", "opendrift_vs_pygnome_board"),
        ("CASE_MINDORO_RETRO_2023", "oil_budget_board"),
        ("CASE_MINDORO_RETRO_2023", "shoreline_impact_board"),
        ("CASE_DWH_RETRO_2010_72H", "daily_deterministic_board"),
        ("CASE_DWH_RETRO_2010_72H", "deterministic_vs_ensemble_board"),
        ("CASE_DWH_RETRO_2010_72H", "opendrift_vs_pygnome_board"),
    ]


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
    ordered_frames: list[pd.DataFrame] = []
    used_ids: set[str] = set()
    for case_id, token in _preferred_defense_patterns():
        match = recommended.loc[
            recommended["case_id"].astype(str).eq(case_id)
            & recommended["figure_id"].astype(str).str.contains(token, case=False, na=False)
        ]
        if not match.empty:
            ordered_frames.append(match.iloc[[0]].copy())
            used_ids.add(str(match.iloc[0]["figure_id"]))
    remainder = recommended.loc[~recommended["figure_id"].astype(str).isin(used_ids)].copy()
    frames = ordered_frames + ([remainder] if not remainder.empty else [])
    return pd.concat(frames, ignore_index=True) if frames else recommended


def figure_subset(
    layer: str,
    *,
    repo_root: str | Path | None = None,
    case_id: str = "",
    family_codes: list[str] | None = None,
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
    if text_filter:
        lowered = text_filter.lower()
        searchable_columns = [column for column in df.columns if column in {"figure_id", "figure_family_label", "board_family_label", "figure_group_label", "model_names", "model_name", "notes", "short_plain_language_interpretation", "plain_language_interpretation"}]
        if searchable_columns:
            mask = pd.Series(False, index=df.index)
            for column in searchable_columns:
                mask |= df[column].astype(str).str.lower().str.contains(lowered, na=False)
            df = df.loc[mask].copy()
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
        "final_output_catalog": final_output_catalog(root),
        "final_manifest_index": final_manifest_index(root),
        "final_log_index": final_log_index(root),
        "final_validation_manifest": final_validation_manifest(root),
        "publication_registry": publication_registry(root),
        "publication_manifest": publication_manifest(root),
        "publication_captions": publication_captions(root),
        "publication_talking_points": publication_talking_points(root),
        "panel_registry": panel_registry(root),
        "raw_gallery_index": raw_gallery_index(root),
        "mindoro_phase3b_summary": mindoro_phase3b_summary(root),
        "mindoro_model_ranking": mindoro_model_ranking(root),
        "dwh_summary": dwh_summary(root),
        "dwh_all_results": dwh_all_results(root),
        "phase4_budget_summary": phase4_budget_summary(root),
        "phase4_oiltype_comparison": phase4_oiltype_comparison(root),
        "phase4_shoreline_arrival": phase4_shoreline_arrival(root),
        "phase4_shoreline_segments": phase4_shoreline_segments(root),
        "phase4_crossmodel_matrix": phase4_crossmodel_matrix(root),
        "phase4_crossmodel_verdict": phase4_crossmodel_verdict(root),
        "phase4_crossmodel_blockers": phase4_crossmodel_blockers(root),
        "phase4_crossmodel_next_steps": phase4_crossmodel_next_steps(root),
        "curated_recommended_figures": curated_recommended_figures(root),
    }
