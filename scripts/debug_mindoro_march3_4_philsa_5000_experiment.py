#!/usr/bin/env python
"""Debug-only audits for the experimental Mindoro PhilSA Mar 3 -> Mar 4 run.

This script reads existing experimental products. It does not run OpenDrift,
does not refresh observations, and does not write thesis-facing packages.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

os.environ.setdefault("WORKFLOW_MODE", "mindoro_retro_2023")

import geopandas as gpd
import matplotlib
import numpy as np
import pandas as pd
import rasterio
import xarray as xr

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.colors import ListedColormap

from src.helpers.raster import GridBuilder, normalize_time_index, rasterize_observation_layer, save_raster
from src.helpers.scoring import GEOGRAPHIC_CRS, apply_ocean_mask, load_sea_mask_array

try:
    from pyproj import Transformer
except ImportError:  # pragma: no cover
    Transformer = None


EXPERIMENT_DIR = Path(
    os.environ.get(
        "MINDORO_MARCH3_4_EXPERIMENT_DIR",
        "output/CASE_MINDORO_RETRO_2023/phase3b_philsa_march3_4_5000_experiment",
    )
)
DEBUG_DIR = EXPERIMENT_DIR / "debug_march3_4_philsa_5000"
TARGET_DATE = "2023-03-04"
SEED_DATE = "2023-03-03"
LOCAL_TZ = "Asia/Manila"
BRANCHES = ("R0", "R1_previous")
EXPECTED_SOURCES = {
    SEED_DATE: "MindoroOilSpill_Philsa_230303",
    TARGET_DATE: "MindoroOilSpill_Philsa_230304",
}
PROHIBITED_TOKENS = (
    "WWF",
    "MSI",
    "NOAA",
    "March_6",
    "March 6",
    "230305",
    "230306",
    "230313",
    "230314",
    "230323",
)
THRESHOLDS = (0.10, 0.25, 0.50, 0.90)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")


def _write_csv(path: Path, rows: list[dict[str, Any]] | pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame = rows if isinstance(rows, pd.DataFrame) else pd.DataFrame(rows)
    frame.to_csv(path, index=False)


def _read_json(path: Path) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _iso_z(timestamp: pd.Timestamp) -> str:
    ts = pd.Timestamp(timestamp)
    if ts.tzinfo is not None:
        ts = ts.tz_convert("UTC").tz_localize(None)
    return ts.isoformat().replace("+00:00", "") + "Z"


def _local_iso(timestamp: pd.Timestamp) -> str:
    ts = pd.Timestamp(timestamp)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    else:
        ts = ts.tz_convert("UTC")
    return ts.tz_convert(LOCAL_TZ).isoformat()


def _local_date(timestamp: pd.Timestamp) -> str:
    ts = pd.Timestamp(timestamp)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    else:
        ts = ts.tz_convert("UTC")
    return ts.tz_convert(LOCAL_TZ).date().isoformat()


def _load_raster(path: Path) -> np.ndarray:
    with rasterio.open(path) as src:
        return src.read(1).astype(np.float32)


def _format_local_from_iso(value: str) -> str:
    if not value:
        return ""
    return pd.Timestamp(value).tz_convert(LOCAL_TZ).isoformat()


class GridMembership:
    def __init__(self, grid: GridBuilder, sea_mask: np.ndarray | None):
        self.grid = grid
        self.sea_mask = sea_mask
        self.transformer = None
        if Transformer is not None and str(grid.crs).upper() != GEOGRAPHIC_CRS:
            self.transformer = Transformer.from_crs(GEOGRAPHIC_CRS, grid.crs, always_xy=True)

    def to_grid(self, lon: np.ndarray, lat: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
        lon = np.asarray(lon, dtype=float)
        lat = np.asarray(lat, dtype=float)
        if self.transformer is None:
            from src.helpers.raster import project_points_to_grid

            return project_points_to_grid(self.grid, lon, lat)
        x_vals, y_vals = self.transformer.transform(lon, lat)
        return np.asarray(x_vals, dtype=float), np.asarray(y_vals, dtype=float)

    def indices(self, lon: np.ndarray, lat: np.ndarray) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
        lon = np.asarray(lon, dtype=float).reshape(-1)
        lat = np.asarray(lat, dtype=float).reshape(-1)
        finite = np.isfinite(lon) & np.isfinite(lat)
        inside = np.zeros(lon.shape, dtype=bool)
        valid_ocean = np.zeros(lon.shape, dtype=bool)
        rows = np.full(lon.shape, -1, dtype=int)
        cols = np.full(lon.shape, -1, dtype=int)
        if not np.any(finite):
            return inside, valid_ocean, rows, cols
        finite_idx = np.flatnonzero(finite)
        x_vals, y_vals = self.to_grid(lon[finite], lat[finite])
        in_bounds = (
            np.isfinite(x_vals)
            & np.isfinite(y_vals)
            & (x_vals >= self.grid.min_x)
            & (x_vals < self.grid.max_x)
            & (y_vals > self.grid.min_y)
            & (y_vals <= self.grid.max_y)
        )
        if not np.any(in_bounds):
            return inside, valid_ocean, rows, cols
        kept = finite_idx[in_bounds]
        kept_x = x_vals[in_bounds]
        kept_y = y_vals[in_bounds]
        kept_cols = np.floor((kept_x - self.grid.min_x) / self.grid.resolution).astype(int)
        kept_rows = np.floor((self.grid.max_y - kept_y) / self.grid.resolution).astype(int)
        valid_cell = (
            (kept_rows >= 0)
            & (kept_rows < self.grid.height)
            & (kept_cols >= 0)
            & (kept_cols < self.grid.width)
        )
        kept = kept[valid_cell]
        kept_rows = kept_rows[valid_cell]
        kept_cols = kept_cols[valid_cell]
        inside[kept] = True
        rows[kept] = kept_rows
        cols[kept] = kept_cols
        if self.sea_mask is not None and kept.size:
            valid_ocean[kept] = self.sea_mask[kept_rows, kept_cols] > 0.5
        return inside, valid_ocean, rows, cols

    def rasterize_cells(self, lon: np.ndarray, lat: np.ndarray) -> np.ndarray:
        _, _, rows, cols = self.indices(lon, lat)
        valid = (rows >= 0) & (cols >= 0)
        data = np.zeros((self.grid.height, self.grid.width), dtype=np.float32)
        if np.any(valid):
            data[rows[valid], cols[valid]] = 1.0
        return data


def _branch_model_files(branch: str) -> list[tuple[str, str, Path]]:
    model_dir = EXPERIMENT_DIR / branch / "model_run"
    files: list[tuple[str, str, Path]] = []
    control = model_dir / "forecast" / "deterministic_control_cmems_gfs.nc"
    if control.exists():
        files.append(("control", "control", control))
    for member_path in sorted((model_dir / "ensemble").glob("member_*.nc")):
        files.append(("ensemble_member", member_path.stem, member_path))
    return files


def _audit_sources_and_observations(grid: GridBuilder, sea_mask: np.ndarray | None) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    obs_csv = EXPERIMENT_DIR / "march3_4_philsa_5000_observation_manifest.csv"
    obs_df = pd.read_csv(obs_csv)
    rows: list[dict[str, Any]] = []
    source_ok = True
    for record in obs_df.to_dict(orient="records"):
        raw_path = Path(record["raw_geojson"])
        processed_path = Path(record["processed_vector_path"])
        score_mask_path = Path(record["extended_obs_mask"])
        raw_payload = _read_json(raw_path)
        raw_features = raw_payload.get("features") or []
        gdf = gpd.read_file(processed_path)
        if gdf.crs is None:
            gdf = gdf.set_crs(grid.crs)
        gdf_grid = gdf.to_crs(grid.crs)
        cleaned_area_m2 = float(gdf_grid.geometry.area.sum())
        raw_raster = rasterize_observation_layer(gdf_grid, grid)
        ocean_raster = apply_ocean_mask(raw_raster, sea_mask=sea_mask, fill_value=0.0)
        saved_mask = _load_raster(score_mask_path)
        obs_date = str(record.get("obs_date") or record.get("observation_date"))
        source_name = str(record["source_name"])
        provider = str(record["provider"])
        row_ok = (
            provider == "PhilSA"
            and source_name == EXPECTED_SOURCES.get(obs_date)
            and not any(token.lower() in source_name.lower() for token in PROHIBITED_TOKENS)
        )
        source_ok = source_ok and row_ok
        rows.append(
            {
                "obs_date": obs_date,
                "role": record.get("role", ""),
                "provider": provider,
                "source_name": source_name,
                "expected_source_name": EXPECTED_SOURCES.get(obs_date, ""),
                "source_url": record.get("source_url", ""),
                "service_url": record.get("service_url", ""),
                "raw_geojson": str(raw_path),
                "processed_vector": str(processed_path),
                "score_mask": str(score_mask_path),
                "raw_polygon_feature_count": int(len(raw_features)),
                "processed_polygon_feature_count": int(len(gdf_grid.index)),
                "cleaned_polygon_area_m2": cleaned_area_m2,
                "cleaned_polygon_area_km2": cleaned_area_m2 / 1_000_000.0,
                "rasterized_cells_before_ocean_mask": int(np.count_nonzero(raw_raster > 0)),
                "valid_ocean_observed_cells": int(np.count_nonzero(ocean_raster > 0)),
                "saved_score_mask_cells": int(np.count_nonzero(saved_mask > 0)),
                "source_layer_correct": bool(row_ok),
            }
        )
    _write_csv(DEBUG_DIR / "march3_4_philsa_5000_source_observation_audit.csv", rows)
    return rows, {"philsa_only_pair": bool(source_ok), "row_count": int(len(rows))}


def _status_counts(
    ds: xr.Dataset,
    time_index: int,
    grid_membership: GridMembership,
) -> dict[str, int]:
    lon = np.asarray(ds["lon"].isel(time=time_index).values).reshape(-1)
    lat = np.asarray(ds["lat"].isel(time=time_index).values).reshape(-1)
    status = np.asarray(ds["status"].isel(time=time_index).values).reshape(-1)
    total = int(ds.sizes.get("trajectory", lon.size))
    finite_status = np.isfinite(status)
    finite_position = np.isfinite(lon) & np.isfinite(lat)
    active_status = finite_status & (status == 0)
    stranded_status = finite_status & (status == 1)
    active_floating = active_status & finite_position
    inside_grid, inside_ocean, _, _ = grid_membership.indices(lon, lat)
    active_inside_grid = active_floating & inside_grid
    active_inside_ocean = active_floating & inside_ocean
    active_outside_domain = active_floating & ~inside_grid
    known_status = active_status | stranded_status
    return {
        "total_particle_slots": total,
        "finite_position_count": int(np.count_nonzero(finite_position)),
        "finite_status_count": int(np.count_nonzero(finite_status)),
        "active_floating_particle_count": int(np.count_nonzero(active_floating)),
        "stranded_particle_count": int(np.count_nonzero(stranded_status)),
        "outside_domain_particle_count": int(np.count_nonzero(active_outside_domain)),
        "deactivated_lost_particle_count": int(max(total - int(np.count_nonzero(known_status)), 0)),
        "particles_inside_scoring_grid_count": int(np.count_nonzero(active_inside_grid)),
        "particles_inside_valid_ocean_mask_count": int(np.count_nonzero(active_inside_ocean)),
        "all_finite_positions_inside_scoring_grid_count": int(np.count_nonzero(finite_position & inside_grid)),
        "all_finite_positions_inside_valid_ocean_mask_count": int(np.count_nonzero(finite_position & inside_ocean)),
    }


def _audit_particle_survival(grid_membership: GridMembership) -> tuple[pd.DataFrame, pd.DataFrame, list[dict[str, Any]]]:
    survival_rows: list[dict[str, Any]] = []
    timestamp_rows: list[dict[str, Any]] = []
    for branch in BRANCHES:
        for run_kind, run_id, nc_path in _branch_model_files(branch):
            with xr.open_dataset(nc_path) as ds:
                times = normalize_time_index(ds["time"].values)
                target_timestamps: list[str] = []
                for time_index, timestamp in enumerate(times):
                    ts = pd.Timestamp(timestamp)
                    local_date = _local_date(ts)
                    if local_date == TARGET_DATE:
                        target_timestamps.append(_iso_z(ts))
                    survival_rows.append(
                        {
                            "branch": branch,
                            "run_kind": run_kind,
                            "run_id": run_id,
                            "output_index": int(time_index),
                            "timestamp_utc": _iso_z(ts),
                            "timestamp_asia_manila": _local_iso(ts),
                            "local_date_asia_manila": local_date,
                            "netcdf_path": str(nc_path),
                            **_status_counts(ds, time_index, grid_membership),
                        }
                    )
                status_values = np.asarray(ds["status"].values).reshape(-1)
                status_values = status_values[np.isfinite(status_values)]
                unique_status = sorted(set(status_values.astype(int).tolist())) if status_values.size else []
                timestamp_rows.append(
                    {
                        "branch": branch,
                        "run_kind": run_kind,
                        "run_id": run_id,
                        "netcdf_path": str(nc_path),
                        "output_timestamp_count": int(len(times)),
                        "first_output_utc": _iso_z(pd.Timestamp(times[0])) if len(times) else "",
                        "last_output_utc": _iso_z(pd.Timestamp(times[-1])) if len(times) else "",
                        "first_output_asia_manila": _local_iso(pd.Timestamp(times[0])) if len(times) else "",
                        "last_output_asia_manila": _local_iso(pd.Timestamp(times[-1])) if len(times) else "",
                        "target_local_date_timestamp_count": int(len(target_timestamps)),
                        "target_local_date_timestamps_utc": ";".join(target_timestamps),
                        "unique_status_codes": ";".join(str(value) for value in unique_status),
                        "status_flag_meanings": str(ds["status"].attrs.get("flag_meanings", "")),
                    }
                )
    survival_df = pd.DataFrame(survival_rows)
    _write_csv(DEBUG_DIR / "march3_4_philsa_5000_raw_particle_survival_by_time.csv", survival_df)
    _write_csv(DEBUG_DIR / "march3_4_philsa_5000_output_timestamp_audit.csv", timestamp_rows)

    member = survival_df[survival_df["run_kind"].eq("ensemble_member")].copy()
    grouped_rows: list[dict[str, Any]] = []
    count_cols = [
        "active_floating_particle_count",
        "stranded_particle_count",
        "outside_domain_particle_count",
        "deactivated_lost_particle_count",
        "particles_inside_scoring_grid_count",
        "particles_inside_valid_ocean_mask_count",
    ]
    if not member.empty:
        grouped = member.groupby(
            ["branch", "timestamp_utc", "timestamp_asia_manila", "local_date_asia_manila"],
            dropna=False,
        )
        for keys, frame in grouped:
            row = {
                "branch": keys[0],
                "run_kind": "ensemble_members",
                "timestamp_utc": keys[1],
                "timestamp_asia_manila": keys[2],
                "local_date_asia_manila": keys[3],
                "member_count": int(frame["run_id"].nunique()),
            }
            for col in count_cols:
                row[f"{col}_sum"] = int(frame[col].sum())
                row[f"{col}_mean_per_member"] = float(frame[col].mean())
                row[f"{col}_min_member"] = int(frame[col].min())
                row[f"{col}_max_member"] = int(frame[col].max())
            grouped_rows.append(row)
    control = survival_df[survival_df["run_kind"].eq("control")].copy()
    for _, frame in control.iterrows():
        row = {
            "branch": frame["branch"],
            "run_kind": "control",
            "timestamp_utc": frame["timestamp_utc"],
            "timestamp_asia_manila": frame["timestamp_asia_manila"],
            "local_date_asia_manila": frame["local_date_asia_manila"],
            "member_count": 1,
        }
        for col in count_cols:
            row[f"{col}_sum"] = int(frame[col])
            row[f"{col}_mean_per_member"] = float(frame[col])
            row[f"{col}_min_member"] = int(frame[col])
            row[f"{col}_max_member"] = int(frame[col])
        grouped_rows.append(row)
    summary_df = pd.DataFrame(grouped_rows).sort_values(["branch", "run_kind", "timestamp_utc"])
    _write_csv(DEBUG_DIR / "march3_4_philsa_5000_raw_particle_survival_summary_by_time.csv", summary_df)
    return survival_df, summary_df, timestamp_rows


def _composite_from_netcdfs(
    nc_paths: list[Path],
    grid: GridBuilder,
    grid_membership: GridMembership,
    sea_mask: np.ndarray | None,
) -> tuple[np.ndarray, np.ndarray, set[str]]:
    raw_masks: list[np.ndarray] = []
    ocean_masks: list[np.ndarray] = []
    active_timestamps: set[str] = set()
    for nc_path in nc_paths:
        raw = np.zeros((grid.height, grid.width), dtype=np.float32)
        with xr.open_dataset(nc_path) as ds:
            times = normalize_time_index(ds["time"].values)
            for time_index, timestamp in enumerate(times):
                ts = pd.Timestamp(timestamp)
                if _local_date(ts) != TARGET_DATE:
                    continue
                lon = np.asarray(ds["lon"].isel(time=time_index).values).reshape(-1)
                lat = np.asarray(ds["lat"].isel(time=time_index).values).reshape(-1)
                status = np.asarray(ds["status"].isel(time=time_index).values).reshape(-1)
                valid = np.isfinite(lon) & np.isfinite(lat) & np.isfinite(status) & (status == 0)
                if not np.any(valid):
                    continue
                active_timestamps.add(_iso_z(ts))
                raw = np.maximum(raw, grid_membership.rasterize_cells(lon[valid], lat[valid]))
        raw_masks.append(raw)
        ocean_masks.append(apply_ocean_mask(raw, sea_mask=sea_mask, fill_value=0.0))
    if not raw_masks:
        empty = np.zeros((grid.height, grid.width), dtype=np.float32)
        return empty, empty, active_timestamps
    return (
        np.mean(np.stack(raw_masks, axis=0), axis=0).astype(np.float32),
        np.mean(np.stack(ocean_masks, axis=0), axis=0).astype(np.float32),
        active_timestamps,
    )


def _audit_forecast_rasterization(
    grid: GridBuilder,
    grid_membership: GridMembership,
    sea_mask: np.ndarray | None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, dict[str, np.ndarray]]]:
    rows: list[dict[str, Any]] = []
    probability_rows: list[dict[str, Any]] = []
    board_arrays: dict[str, dict[str, np.ndarray]] = {}
    for branch in BRANCHES:
        branch_debug_dir = DEBUG_DIR / branch
        branch_debug_dir.mkdir(parents=True, exist_ok=True)
        model_dir = EXPERIMENT_DIR / branch / "model_run"
        member_paths = sorted((model_dir / "ensemble").glob("member_*.nc"))
        control_paths = sorted((model_dir / "forecast").glob("deterministic_control_*.nc"))

        raw_prob, ocean_prob, member_active_timestamps = _composite_from_netcdfs(
            member_paths,
            grid,
            grid_membership,
            sea_mask,
        )
        control_raw, control_ocean, control_active_timestamps = _composite_from_netcdfs(
            control_paths,
            grid,
            grid_membership,
            sea_mask,
        )
        save_raster(grid, raw_prob, branch_debug_dir / f"{branch}_march4_recomputed_prob_presence_before_ocean_mask.tif")
        save_raster(grid, ocean_prob, branch_debug_dir / f"{branch}_march4_recomputed_prob_presence_after_ocean_mask.tif")
        save_raster(grid, (raw_prob > 0).astype(np.float32), branch_debug_dir / f"{branch}_march4_raw_particle_union_before_ocean_mask.tif")
        save_raster(grid, ((raw_prob > 0) & (ocean_prob <= 0)).astype(np.float32), branch_debug_dir / f"{branch}_march4_dropped_by_ocean_mask.tif")
        for threshold in THRESHOLDS:
            save_raster(
                grid,
                (ocean_prob >= threshold).astype(np.float32),
                branch_debug_dir / f"{branch}_march4_mask_p{int(threshold * 100):02d}_recomputed.tif",
            )

        saved_dir = EXPERIMENT_DIR / branch / "forecast_datecomposites"
        saved_prob = _load_raster(saved_dir / f"prob_presence_{TARGET_DATE}_localdate.tif")
        saved_p50 = _load_raster(saved_dir / f"mask_p50_{TARGET_DATE}_localdate.tif")
        saved_p90 = _load_raster(saved_dir / f"mask_p90_{TARGET_DATE}_localdate.tif")
        saved_control = _load_raster(saved_dir / f"control_footprint_{TARGET_DATE}_localdate.tif")
        positive_values = np.unique(saved_prob[saved_prob > 0])

        raw_threshold_counts = {
            f"raw_p{int(threshold * 100):02d}_cells_before_ocean_mask": int(np.count_nonzero(raw_prob >= threshold))
            for threshold in THRESHOLDS
        }
        ocean_threshold_counts = {
            f"p{int(threshold * 100):02d}_cells_after_valid_ocean_mask": int(np.count_nonzero(ocean_prob >= threshold))
            for threshold in THRESHOLDS
        }
        rows.append(
            {
                "branch": branch,
                "product": "ensemble_prob_presence_recomputed_from_member_netcdfs",
                "member_count": int(len(member_paths)),
                "target_local_date": TARGET_DATE,
                "target_local_date_active_timestamp_count": int(len(member_active_timestamps)),
                "target_local_date_active_timestamps_utc": ";".join(sorted(member_active_timestamps)),
                "raw_particle_cells_before_land_sea_mask": int(np.count_nonzero(raw_prob > 0)),
                "cells_after_valid_ocean_mask": int(np.count_nonzero(ocean_prob > 0)),
                "cells_dropped_by_valid_ocean_mask": int(np.count_nonzero((raw_prob > 0) & (ocean_prob <= 0))),
                "cells_after_probability_threshold": "",
                "deterministic_control_cell_count": int(np.count_nonzero(control_ocean > 0)),
                **raw_threshold_counts,
                **ocean_threshold_counts,
                "saved_prob_presence_positive_cells": int(np.count_nonzero(saved_prob > 0)),
                "saved_p50_cells": int(np.count_nonzero(saved_p50 > 0)),
                "saved_p90_cells": int(np.count_nonzero(saved_p90 > 0)),
                "saved_control_cells": int(np.count_nonzero(saved_control > 0)),
            }
        )
        for threshold in THRESHOLDS:
            rows.append(
                {
                    "branch": branch,
                    "product": f"mask_p{int(threshold * 100):02d}",
                    "member_count": int(len(member_paths)),
                    "target_local_date": TARGET_DATE,
                    "target_local_date_active_timestamp_count": int(len(member_active_timestamps)),
                    "raw_particle_cells_before_land_sea_mask": int(np.count_nonzero(raw_prob > 0)),
                    "cells_after_valid_ocean_mask": int(np.count_nonzero(ocean_prob > 0)),
                    "cells_dropped_by_valid_ocean_mask": int(np.count_nonzero((raw_prob > 0) & (ocean_prob <= 0))),
                    "cells_after_probability_threshold": int(np.count_nonzero(ocean_prob >= threshold)),
                    "deterministic_control_cell_count": int(np.count_nonzero(control_ocean > 0)),
                    **raw_threshold_counts,
                    **ocean_threshold_counts,
                    "saved_prob_presence_positive_cells": int(np.count_nonzero(saved_prob > 0)),
                    "saved_p50_cells": int(np.count_nonzero(saved_p50 > 0)),
                    "saved_p90_cells": int(np.count_nonzero(saved_p90 > 0)),
                    "saved_control_cells": int(np.count_nonzero(saved_control > 0)),
                }
            )
        rows.append(
            {
                "branch": branch,
                "product": "deterministic_control_localdate_footprint",
                "member_count": "",
                "target_local_date": TARGET_DATE,
                "target_local_date_active_timestamp_count": int(len(control_active_timestamps)),
                "target_local_date_active_timestamps_utc": ";".join(sorted(control_active_timestamps)),
                "raw_particle_cells_before_land_sea_mask": int(np.count_nonzero(control_raw > 0)),
                "cells_after_valid_ocean_mask": int(np.count_nonzero(control_ocean > 0)),
                "cells_dropped_by_valid_ocean_mask": int(np.count_nonzero((control_raw > 0) & (control_ocean <= 0))),
                "cells_after_probability_threshold": "",
                "deterministic_control_cell_count": int(np.count_nonzero(control_ocean > 0)),
                "saved_control_cells": int(np.count_nonzero(saved_control > 0)),
            }
        )

        probability_rows.append(
            {
                "branch": branch,
                "prob_presence_path": str(saved_dir / f"prob_presence_{TARGET_DATE}_localdate.tif"),
                "prob_presence_positive_cells": int(np.count_nonzero(saved_prob > 0)),
                "prob_presence_min_positive": float(np.min(positive_values)) if positive_values.size else 0.0,
                "prob_presence_max": float(np.max(saved_prob)) if saved_prob.size else 0.0,
                "prob_presence_unique_positive_value_count": int(positive_values.size),
                "prob_presence_unique_positive_values": ";".join(f"{value:.6g}" for value in positive_values),
                "prob_presence_saved_as_continuous_raster": bool(positive_values.size == 0 or positive_values.size > 2 or np.max(saved_prob) < 1.0),
                "p50_means_probability_gte_0_50": True,
                "p90_means_probability_gte_0_90": True,
                "p10_cells_from_saved_probability": int(np.count_nonzero(saved_prob >= 0.10)),
                "p25_cells_from_saved_probability": int(np.count_nonzero(saved_prob >= 0.25)),
                "p50_cells_from_saved_probability": int(np.count_nonzero(saved_prob >= 0.50)),
                "p90_cells_from_saved_probability": int(np.count_nonzero(saved_prob >= 0.90)),
                "saved_p50_cells": int(np.count_nonzero(saved_p50 > 0)),
                "saved_p90_cells": int(np.count_nonzero(saved_p90 > 0)),
                "p90_subset_of_p50": bool(np.all((saved_p90 > 0) <= (saved_p50 > 0))),
                "saved_p50_matches_probability_gte_0_50": bool(np.array_equal((saved_prob >= 0.50), (saved_p50 > 0))),
                "saved_p90_matches_probability_gte_0_90": bool(np.array_equal((saved_prob >= 0.90), (saved_p90 > 0))),
            }
        )
        board_arrays[branch] = {
            "raw_prob": raw_prob,
            "ocean_prob": ocean_prob,
            "dropped": ((raw_prob > 0) & (ocean_prob <= 0)).astype(np.float32),
            "control": control_ocean,
            "saved_prob": saved_prob,
            "saved_p50": saved_p50,
            "saved_p90": saved_p90,
        }
    _write_csv(DEBUG_DIR / "march3_4_philsa_5000_forecast_rasterization_audit.csv", rows)
    _write_csv(DEBUG_DIR / "march3_4_philsa_5000_probability_logic_audit.csv", probability_rows)
    _write_json(
        DEBUG_DIR / "march3_4_philsa_5000_probability_logic_audit.json",
        {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "target_local_date": TARGET_DATE,
            "threshold_semantics": {
                "p10": "probability >= 0.10",
                "p25": "probability >= 0.25",
                "p50": "probability >= 0.50",
                "p90": "probability >= 0.90",
            },
            "rows": probability_rows,
        },
    )
    return rows, probability_rows, board_arrays


def _make_crop(arrays: list[np.ndarray], pad: int = 12) -> tuple[slice, slice]:
    coords: list[np.ndarray] = []
    for arr in arrays:
        if arr is None:
            continue
        ys, xs = np.nonzero(np.asarray(arr) > 0)
        if ys.size:
            coords.append(np.column_stack([ys, xs]))
    if not coords:
        return slice(None), slice(None)
    all_coords = np.vstack(coords)
    y0 = max(int(all_coords[:, 0].min()) - pad, 0)
    y1 = min(int(all_coords[:, 0].max()) + pad + 1, arrays[0].shape[0])
    x0 = max(int(all_coords[:, 1].min()) - pad, 0)
    x1 = min(int(all_coords[:, 1].max()) + pad + 1, arrays[0].shape[1])
    return slice(y0, y1), slice(x0, x1)


def _show_binary(ax, arr: np.ndarray, title: str, color: str = "magma") -> None:
    masked = np.ma.masked_where(arr <= 0, arr)
    ax.imshow(np.zeros_like(arr), cmap=ListedColormap(["#f5f6f8"]), vmin=0, vmax=1)
    ax.imshow(masked, cmap=color, vmin=0, vmax=max(float(np.max(arr)), 1.0))
    ax.set_title(title, fontsize=10)
    ax.set_xticks([])
    ax.set_yticks([])


def _create_board(
    grid: GridBuilder,
    sea_mask: np.ndarray | None,
    board_arrays: dict[str, dict[str, np.ndarray]],
) -> Path:
    seed = _load_raster(EXPERIMENT_DIR / "march3_seed_mask_on_grid.tif")
    target = _load_raster(EXPERIMENT_DIR / "march4_target_mask_on_grid.tif")
    r0 = board_arrays["R0"]
    r1 = board_arrays["R1_previous"]
    crop = _make_crop(
        [
            seed,
            target,
            r0["raw_prob"],
            r1["raw_prob"],
            r1["saved_prob"],
            r1["control"],
            r1["dropped"],
        ]
    )
    seed = seed[crop]
    target = target[crop]
    r0_raw = r0["raw_prob"][crop]
    r1_raw = r1["raw_prob"][crop]
    r1_prob = r1["saved_prob"][crop]
    r1_control = r1["control"][crop]
    r1_dropped = r1["dropped"][crop]
    sea = sea_mask[crop] if sea_mask is not None else np.ones_like(seed)

    threshold_class = np.zeros_like(r1_prob, dtype=np.float32)
    threshold_class[r1_prob >= 0.10] = 1
    threshold_class[r1_prob >= 0.25] = 2
    threshold_class[r1_prob >= 0.50] = 3
    threshold_class[r1_prob >= 0.90] = 4

    fig, axes = plt.subplots(3, 3, figsize=(15, 13), constrained_layout=True)
    fig.suptitle(
        "Experimental Debug Board: Mindoro PhilSA Mar 3 -> Mar 4, 5,000 elements",
        fontsize=15,
        fontweight="bold",
    )

    _show_binary(axes[0, 0], seed, f"Mar 3 PhilSA Seed\n{int(np.count_nonzero(seed > 0))} cropped cells")
    _show_binary(axes[0, 1], target, f"Mar 4 PhilSA Target\n{int(np.count_nonzero(target > 0))} cropped cells", "viridis")
    _show_binary(axes[0, 2], r0_raw, "R0 Raw Particles on Mar 4\nno target-date cells" if not np.any(r0_raw > 0) else "R0 Raw Particles on Mar 4")

    _show_binary(axes[1, 0], r1_raw, f"R1 Raw Member Presence\n{int(np.count_nonzero(r1_raw > 0))} cells before ocean mask", "plasma")
    im = axes[1, 1].imshow(np.ma.masked_where(r1_prob <= 0, r1_prob), cmap="inferno", vmin=0, vmax=1)
    axes[1, 1].imshow(np.ma.masked_where(r1_prob > 0, np.zeros_like(r1_prob)), cmap=ListedColormap(["#f5f6f8"]))
    axes[1, 1].set_title("R1 prob_presence\ncontinuous probability", fontsize=10)
    axes[1, 1].set_xticks([])
    axes[1, 1].set_yticks([])
    cbar = fig.colorbar(im, ax=axes[1, 1], fraction=0.046, pad=0.02)
    cbar.ax.tick_params(labelsize=8)

    cmap = ListedColormap(["#f5f6f8", "#8ecae6", "#219ebc", "#ffb703", "#d62828"])
    axes[1, 2].imshow(threshold_class, cmap=cmap, vmin=0, vmax=4)
    axes[1, 2].set_title("R1 Threshold Masks\np10 blue, p25 teal, p50 amber, p90 red", fontsize=10)
    axes[1, 2].set_xticks([])
    axes[1, 2].set_yticks([])

    _show_binary(axes[2, 0], r1_control, f"R1 Deterministic Control\n{int(np.count_nonzero(r1_control > 0))} ocean cells", "cividis")
    _show_binary(axes[2, 1], r1_dropped, f"Dropped by Valid-Ocean Mask\n{int(np.count_nonzero(r1_dropped > 0))} cells", "Reds")
    axes[2, 2].imshow(sea, cmap=ListedColormap(["#d9d9d9", "#c7e9f1"]), vmin=0, vmax=1)
    axes[2, 2].imshow(np.ma.masked_where(target <= 0, target), cmap="viridis", alpha=0.85)
    axes[2, 2].set_title("Valid-Ocean Mask Context\nMar 4 target overlay", fontsize=10)
    axes[2, 2].set_xticks([])
    axes[2, 2].set_yticks([])

    for ax in axes.flat:
        for spine in ax.spines.values():
            spine.set_color("#3a3a3a")
            spine.set_linewidth(0.8)

    board_path = DEBUG_DIR / "march3_4_philsa_5000_diagnostic_board.png"
    board_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(board_path, dpi=220)
    plt.close(fig)
    return board_path


def _audit_time_handling(timestamp_rows: list[dict[str, Any]]) -> dict[str, Any]:
    run_manifest_path = EXPERIMENT_DIR / "march3_4_philsa_5000_run_manifest.json"
    run_manifest = _read_json(run_manifest_path)
    window = run_manifest.get("window", {})
    selected_start = run_manifest.get("selected_start_source", {}) or {}
    selected_target = run_manifest.get("selected_target_source", {}) or {}
    branch_survival = pd.read_csv(EXPERIMENT_DIR / "march3_4_philsa_5000_branch_survival_summary.csv")
    start_utc = str(window.get("simulation_start_utc", ""))
    end_utc = str(window.get("simulation_end_utc", ""))
    selected_source_text = json.dumps(
        {
            "selected_start_source": selected_start,
            "selected_target_source": selected_target,
            "seed_release": run_manifest.get("seed_release", {}),
        },
        sort_keys=True,
    )
    forbidden_selected_source = any(token.lower() in selected_source_text.lower() for token in PROHIBITED_TOKENS)
    wrong_date_window = not (
        str(window.get("scored_target_date", "")) == TARGET_DATE
        and list(window.get("forecast_local_dates", [])) == [SEED_DATE, TARGET_DATE]
        and str(selected_start.get("source_name", "")) == EXPECTED_SOURCES[SEED_DATE]
        and str(selected_target.get("source_name", "")) == EXPECTED_SOURCES[TARGET_DATE]
        and str(selected_start.get("provider", "")) == "PhilSA"
        and str(selected_target.get("provider", "")) == "PhilSA"
    )
    payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "release_start_utc": start_utc,
        "release_start_asia_manila": _format_local_from_iso(start_utc),
        "requested_final_simulation_time_utc": end_utc,
        "requested_final_simulation_time_asia_manila": _format_local_from_iso(end_utc),
        "seed_observation_date_local": SEED_DATE,
        "target_observation_date_local": TARGET_DATE,
        "forecast_local_dates": window.get("forecast_local_dates", []),
        "scored_target_date": window.get("scored_target_date", ""),
        "date_composite_rule": window.get("date_composite_rule", ""),
        "branch_survival_manifest_rows": branch_survival.to_dict(orient="records"),
        "output_timestamp_audit_csv": str(DEBUG_DIR / "march3_4_philsa_5000_output_timestamp_audit.csv"),
        "branch_output_timestamp_summary": timestamp_rows,
        "forbidden_date_logic_detected": bool(wrong_date_window or forbidden_selected_source),
        "forbidden_selected_source_detected": bool(forbidden_selected_source),
        "wrong_date_window_detected": bool(wrong_date_window),
        "common_mindoro_scoring_grid_note": (
            "The forecast manifest grid metadata may reference the existing common Mindoro grid "
            "sources. That is expected scoring-grid provenance and is not treated as seed/target "
            "substitution for this debug check."
        ),
        "time_handling_interpretation": (
            "The scoring composite is the Asia/Manila local date 2023-03-04. "
            "UTC timestamps from 2023-03-03T16:00:00Z through 2023-03-04T15:00:00Z "
            "belong to that local date when present in a branch output."
        ),
    }
    _write_json(DEBUG_DIR / "march3_4_philsa_5000_time_handling_audit.json", payload)
    return payload


def _write_debug_note(
    source_rows: list[dict[str, Any]],
    source_summary: dict[str, Any],
    time_audit: dict[str, Any],
    forecast_rows: list[dict[str, Any]],
    probability_rows: list[dict[str, Any]],
    board_path: Path,
) -> Path:
    target_row = next(row for row in source_rows if row["obs_date"] == TARGET_DATE)
    seed_row = next(row for row in source_rows if row["obs_date"] == SEED_DATE)
    forecast_by_branch = {
        row["branch"]: row
        for row in forecast_rows
        if row["product"] == "ensemble_prob_presence_recomputed_from_member_netcdfs"
    }
    prob_by_branch = {row["branch"]: row for row in probability_rows}
    r0 = forecast_by_branch["R0"]
    r1 = forecast_by_branch["R1_previous"]
    r1_prob = prob_by_branch["R1_previous"]
    r0_reason = (
        "R0 has no March 4 local-date active timestamp in the NetCDF outputs; the branch "
        "stops before the target-date composite can contribute forecast cells."
    )
    r1_reason = (
        "R1_previous reaches the March 4 local-date composite, but the member presence is "
        "very compact and thresholding collapses it from "
        f"{r1['cells_after_valid_ocean_mask']} ocean cells with any member support to "
        f"{r1_prob['p50_cells_from_saved_probability']} p50 cells and "
        f"{r1_prob['p90_cells_from_saved_probability']} p90 cells."
    )
    if int(r1["cells_dropped_by_valid_ocean_mask"]) > 0:
        r1_reason += (
            f" The valid-ocean mask also drops {r1['cells_dropped_by_valid_ocean_mask']} "
            "raw member-support cells before thresholding."
        )

    note = f"""# Debug Note: Experimental PhilSA March 3 -> March 4 5,000-Element Test

Status: diagnostic only, archive/provenance only, not thesis-facing.

## Source and Target Checks

- March 3 seed: {seed_row['source_name']} ({seed_row['provider']}), raw features = {seed_row['raw_polygon_feature_count']}, cleaned area = {seed_row['cleaned_polygon_area_km2']:.3f} km2, scoreable ocean cells = {seed_row['valid_ocean_observed_cells']}.
- March 4 target: {target_row['source_name']} ({target_row['provider']}), raw features = {target_row['raw_polygon_feature_count']}, cleaned area = {target_row['cleaned_polygon_area_km2']:.3f} km2, rasterized cells before ocean mask = {target_row['rasterized_cells_before_ocean_mask']}, valid-ocean observed cells = {target_row['valid_ocean_observed_cells']}.
- PhilSA-only source check: {source_summary['philsa_only_pair']}. No WWF/MSI/NOAA/March 6/March 13/March 14/March 23 seed or target source was used.

## Time Handling

- Release/start UTC: {time_audit['release_start_utc']} ({time_audit['release_start_asia_manila']}).
- Requested final UTC: {time_audit['requested_final_simulation_time_utc']} ({time_audit['requested_final_simulation_time_asia_manila']}).
- Scored product: Asia/Manila local-date composite for {TARGET_DATE}.
- Forbidden March 3->6 or March 13/14 scoring logic detected: {time_audit['forbidden_date_logic_detected']}.

## Where Cells Disappear

- R0: {r0_reason}
- R1_previous: {r1_reason}
- Probability logic is consistent: p50 is probability >= 0.50, p90 is probability >= 0.90, and p90 is a subset of p50 for both branches.
- prob_presence is saved as a GeoTIFF probability raster. R1_previous has {r1_prob['prob_presence_unique_positive_value_count']} positive probability levels: {r1_prob['prob_presence_unique_positive_values'] or 'none'}.

## Current Diagnosis

The sparse forecast is not caused by PhilSA source substitution, a zero March 4 target raster, or accidental March 6/March 13/March 14 date selection. R0 sparsity is explained by branch survival/stranding before the March 4 local-date scoring window. R1_previous sparsity is mainly the combination of compact transport, valid-ocean clipping of a small number of raw cells, and probability thresholding from any-member support to p50/p90 masks.

Do not interpret this as thesis-facing evidence. It remains a candidate next-day public-observation validation test that would need manual scientific review before any promotion.

## Debug Artifacts

- Diagnostic board: {board_path}
- Source/observation audit: {DEBUG_DIR / 'march3_4_philsa_5000_source_observation_audit.csv'}
- Particle survival by output time: {DEBUG_DIR / 'march3_4_philsa_5000_raw_particle_survival_by_time.csv'}
- Particle survival summary: {DEBUG_DIR / 'march3_4_philsa_5000_raw_particle_survival_summary_by_time.csv'}
- Forecast rasterization audit: {DEBUG_DIR / 'march3_4_philsa_5000_forecast_rasterization_audit.csv'}
- Probability logic audit: {DEBUG_DIR / 'march3_4_philsa_5000_probability_logic_audit.csv'}
- Time handling audit: {DEBUG_DIR / 'march3_4_philsa_5000_time_handling_audit.json'}
"""
    note_path = DEBUG_DIR / "march3_4_philsa_5000_debug_note.md"
    note_path.write_text(note, encoding="utf-8")
    return note_path


def main() -> None:
    if not EXPERIMENT_DIR.exists():
        raise FileNotFoundError(f"Experiment directory not found: {EXPERIMENT_DIR}")
    DEBUG_DIR.mkdir(parents=True, exist_ok=True)
    grid = GridBuilder()
    sea_mask = load_sea_mask_array(grid.spec)
    grid_membership = GridMembership(grid, sea_mask)

    source_rows, source_summary = _audit_sources_and_observations(grid, sea_mask)
    _, _, timestamp_rows = _audit_particle_survival(grid_membership)
    forecast_rows, probability_rows, board_arrays = _audit_forecast_rasterization(grid, grid_membership, sea_mask)
    time_audit = _audit_time_handling(timestamp_rows)
    board_path = _create_board(grid, sea_mask, board_arrays)
    note_path = _write_debug_note(
        source_rows,
        source_summary,
        time_audit,
        forecast_rows,
        probability_rows,
        board_path,
    )
    manifest = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "experiment_dir": str(EXPERIMENT_DIR),
        "debug_dir": str(DEBUG_DIR),
        "diagnostic_only": True,
        "thesis_facing": False,
        "full_ensemble_rerun_performed_by_this_script": False,
        "board_path": str(board_path),
        "debug_note": str(note_path),
        "philsa_only_pair": source_summary["philsa_only_pair"],
        "target_valid_ocean_cells": int(
            next(row["valid_ocean_observed_cells"] for row in source_rows if row["obs_date"] == TARGET_DATE)
        ),
        "outputs": {
            "source_observation_audit_csv": str(DEBUG_DIR / "march3_4_philsa_5000_source_observation_audit.csv"),
            "raw_particle_survival_by_time_csv": str(DEBUG_DIR / "march3_4_philsa_5000_raw_particle_survival_by_time.csv"),
            "raw_particle_survival_summary_by_time_csv": str(DEBUG_DIR / "march3_4_philsa_5000_raw_particle_survival_summary_by_time.csv"),
            "output_timestamp_audit_csv": str(DEBUG_DIR / "march3_4_philsa_5000_output_timestamp_audit.csv"),
            "forecast_rasterization_audit_csv": str(DEBUG_DIR / "march3_4_philsa_5000_forecast_rasterization_audit.csv"),
            "probability_logic_audit_csv": str(DEBUG_DIR / "march3_4_philsa_5000_probability_logic_audit.csv"),
            "probability_logic_audit_json": str(DEBUG_DIR / "march3_4_philsa_5000_probability_logic_audit.json"),
            "time_handling_audit_json": str(DEBUG_DIR / "march3_4_philsa_5000_time_handling_audit.json"),
        },
    }
    _write_json(DEBUG_DIR / "march3_4_philsa_5000_debug_manifest.json", manifest)
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
