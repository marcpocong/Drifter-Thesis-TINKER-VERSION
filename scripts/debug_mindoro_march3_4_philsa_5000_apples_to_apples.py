#!/usr/bin/env python
"""Apples-to-apples diagnostics for the experimental Mar 3 -> Mar 4 PhilSA run.

This is a read-only diagnostic over existing OpenDrift/PyGNOME run products.
It writes only into the experimental archive/debug directory and does not
rerun the full OpenDrift ensemble.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

os.environ.setdefault("WORKFLOW_MODE", "mindoro_retro_2023")

import geopandas as gpd
import matplotlib
import netCDF4
import numpy as np
import pandas as pd
import rasterio
import xarray as xr
from scipy.ndimage import binary_dilation
from scipy.spatial import cKDTree

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.colors import ListedColormap

from src.helpers.metrics import calculate_fss
from src.helpers.raster import GridBuilder, normalize_time_index, save_raster
from src.helpers.scoring import GEOGRAPHIC_CRS, load_binary_mask, load_sea_mask_array
from src.services.scoring import OFFICIAL_PHASE3B_WINDOWS_KM

try:
    from pyproj import Transformer
except ImportError:  # pragma: no cover
    Transformer = None


BASE = Path("output/CASE_MINDORO_RETRO_2023/phase3b_philsa_march3_4_5000_experiment")
GENERAL_DEBUG = BASE / "debug_march3_4_philsa_5000"
OUT_DIR = GENERAL_DEBUG / "apples_to_apples"
TARGET_DATE = "2023-03-04"
SEED_DATE = "2023-03-03"
LOCAL_TZ = "Asia/Manila"
EXPECTED_ELEMENT_COUNT = 5000


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, default=_json_default) + "\n", encoding="utf-8")


def _write_csv(path: Path, rows: list[dict[str, Any]] | pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame = rows if isinstance(rows, pd.DataFrame) else pd.DataFrame(rows)
    frame.to_csv(path, index=False)


def _json_default(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return None if np.isnan(value) else float(value)
    if isinstance(value, np.ndarray):
        return value.tolist()
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.isoformat()
    return str(value)


def _read_json(path: Path) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _read_raster(path: Path) -> np.ndarray:
    with rasterio.open(path) as src:
        return src.read(1).astype(np.float32)


def _iso_z(value: Any) -> str:
    ts = pd.Timestamp(value)
    if ts.tzinfo is not None:
        ts = ts.tz_convert("UTC").tz_localize(None)
    return ts.isoformat().replace("+00:00", "") + "Z"


def _local_iso(value: Any) -> str:
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    else:
        ts = ts.tz_convert("UTC")
    return ts.tz_convert(LOCAL_TZ).isoformat()


def _local_date(value: Any) -> str:
    return _local_iso(value)[:10]


def _window_cells(grid: GridBuilder, window_km: int) -> int:
    if grid.is_projected:
        return max(1, int(round((float(window_km) * 1000.0) / float(grid.resolution))))
    return max(1, int(window_km))


class GridTools:
    def __init__(self, grid: GridBuilder, sea_mask: np.ndarray | None):
        self.grid = grid
        self.sea_mask = sea_mask
        if Transformer is None:
            raise ImportError("pyproj is required for apples-to-apples diagnostics.")
        self.to_grid = Transformer.from_crs(GEOGRAPHIC_CRS, grid.crs, always_xy=True)
        self.to_wgs84 = Transformer.from_crs(grid.crs, GEOGRAPHIC_CRS, always_xy=True)

    def lonlat_to_rows_cols(self, lon: np.ndarray, lat: np.ndarray) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        lon = np.asarray(lon, dtype=float).reshape(-1)
        lat = np.asarray(lat, dtype=float).reshape(-1)
        finite = np.isfinite(lon) & np.isfinite(lat)
        rows = np.full(lon.shape, -1, dtype=int)
        cols = np.full(lon.shape, -1, dtype=int)
        inside = np.zeros(lon.shape, dtype=bool)
        if not np.any(finite):
            return rows, cols, inside
        idx = np.flatnonzero(finite)
        x, y = self.to_grid.transform(lon[finite], lat[finite])
        x = np.asarray(x, dtype=float)
        y = np.asarray(y, dtype=float)
        in_bounds = (
            np.isfinite(x)
            & np.isfinite(y)
            & (x >= self.grid.min_x)
            & (x < self.grid.max_x)
            & (y > self.grid.min_y)
            & (y <= self.grid.max_y)
        )
        if not np.any(in_bounds):
            return rows, cols, inside
        kept = idx[in_bounds]
        kept_cols = np.floor((x[in_bounds] - self.grid.min_x) / self.grid.resolution).astype(int)
        kept_rows = np.floor((self.grid.max_y - y[in_bounds]) / self.grid.resolution).astype(int)
        ok = (
            (kept_rows >= 0)
            & (kept_rows < self.grid.height)
            & (kept_cols >= 0)
            & (kept_cols < self.grid.width)
        )
        kept = kept[ok]
        rows[kept] = kept_rows[ok]
        cols[kept] = kept_cols[ok]
        inside[kept] = True
        return rows, cols, inside

    def occupancy(self, lon: np.ndarray, lat: np.ndarray) -> tuple[np.ndarray, int]:
        rows, cols, inside = self.lonlat_to_rows_cols(lon, lat)
        data = np.zeros((self.grid.height, self.grid.width), dtype=np.float32)
        valid = inside & (rows >= 0) & (cols >= 0)
        if np.any(valid):
            data[rows[valid], cols[valid]] = 1.0
        return data, int(np.count_nonzero(~inside & np.isfinite(lon) & np.isfinite(lat)))

    def cell_center(self, row: int, col: int) -> tuple[float, float]:
        x = self.grid.min_x + ((float(col) + 0.5) * self.grid.resolution)
        y = self.grid.max_y - ((float(row) + 0.5) * self.grid.resolution)
        return x, y

    def cell_center_lonlat(self, row: int, col: int) -> tuple[float, float]:
        x, y = self.cell_center(row, col)
        lon, lat = self.to_wgs84.transform(x, y)
        return float(lon), float(lat)

    def points_from_mask(self, mask: np.ndarray) -> np.ndarray:
        rows, cols = np.nonzero(mask > 0)
        if rows.size == 0:
            return np.empty((0, 2), dtype=float)
        xs = self.grid.min_x + ((cols + 0.5) * self.grid.resolution)
        ys = self.grid.max_y - ((rows + 0.5) * self.grid.resolution)
        return np.column_stack([xs, ys]).astype(float)


def _validate_inputs() -> dict[str, Any]:
    required = [
        BASE / "march3_4_philsa_5000_run_manifest.json",
        BASE / "march3_4_philsa_5000_observation_manifest.csv",
        BASE / "march4_target_mask_on_grid.tif",
        BASE / "march3_seed_mask_on_grid.tif",
        BASE / "R1_previous/model_run/forecast/deterministic_control_cmems_gfs.nc",
        BASE / "R1_previous/model_run/forecast/forecast_manifest.json",
        BASE / "R1_previous/model_run/ensemble/ensemble_manifest.json",
        BASE / "pygnome_comparator/model_run/pygnome_march3_4_philsa_5000_deterministic_control.nc",
        BASE / "pygnome_comparator/model_run/pygnome_march3_4_philsa_5000_metadata.json",
    ]
    missing = [str(path) for path in required if not path.exists()]
    if missing:
        raise FileNotFoundError("Missing required existing experimental artifacts: " + "; ".join(missing))

    run_manifest = _read_json(BASE / "march3_4_philsa_5000_run_manifest.json")
    if int(run_manifest.get("element_count_detected_from_manifest") or 0) != EXPECTED_ELEMENT_COUNT:
        raise RuntimeError("Existing OpenDrift run is not the required 5,000-element run.")
    if bool(run_manifest.get("thesis_facing")):
        raise RuntimeError("This diagnostic must not run against a thesis-facing artifact.")

    obs = pd.read_csv(BASE / "march3_4_philsa_5000_observation_manifest.csv")
    names = set(obs["source_name"].astype(str))
    providers = set(obs["provider"].astype(str))
    if providers != {"PhilSA"} or names != {"MindoroOilSpill_Philsa_230303", "MindoroOilSpill_Philsa_230304"}:
        raise RuntimeError("Observation manifest is not the required PhilSA-only March 3 -> March 4 pair.")

    py_meta = _read_json(BASE / "pygnome_comparator/model_run/pygnome_march3_4_philsa_5000_metadata.json")
    if int(py_meta.get("benchmark_particles") or 0) != EXPECTED_ELEMENT_COUNT:
        raise RuntimeError("Existing PyGNOME comparator is not the required 5,000-particle run.")

    return {
        "run_manifest": run_manifest,
        "observation_manifest": obs,
        "pygnome_metadata": py_meta,
        "opendrift_forecast_manifest": _read_json(BASE / "R1_previous/model_run/forecast/forecast_manifest.json"),
        "opendrift_ensemble_manifest": _read_json(BASE / "R1_previous/model_run/ensemble/ensemble_manifest.json"),
        "pygnome_run_manifest": _read_json(BASE / "pygnome_comparator/march3_4_philsa_5000_pygnome_run_manifest.json")
        if (BASE / "pygnome_comparator/march3_4_philsa_5000_pygnome_run_manifest.json").exists()
        else {},
    }


def _opendrift_times(nc_path: Path) -> list[pd.Timestamp]:
    with xr.open_dataset(nc_path) as ds:
        return [pd.Timestamp(value) for value in normalize_time_index(ds["time"].values)]


def _opendrift_masks_for_paths(
    nc_paths: list[Path],
    grid: GridBuilder,
    tools: GridTools,
    time_filter: Callable[[pd.Timestamp], bool],
) -> dict[str, Any]:
    masks: list[np.ndarray] = []
    active_timestamps: set[str] = set()
    outside_particle_positions = 0
    status_codes: dict[int, int] = {}
    for path in nc_paths:
        run_mask = np.zeros((grid.height, grid.width), dtype=np.float32)
        with xr.open_dataset(path) as ds:
            times = normalize_time_index(ds["time"].values)
            for time_index, timestamp in enumerate(times):
                ts = pd.Timestamp(timestamp)
                if not time_filter(ts):
                    continue
                status = np.asarray(ds["status"].isel(time=time_index).values).reshape(-1)
                finite_status = status[np.isfinite(status)]
                for code, count in zip(*np.unique(finite_status.astype(int), return_counts=True)):
                    status_codes[int(code)] = status_codes.get(int(code), 0) + int(count)
                lon = np.asarray(ds["lon"].isel(time=time_index).values).reshape(-1)
                lat = np.asarray(ds["lat"].isel(time=time_index).values).reshape(-1)
                valid = np.isfinite(lon) & np.isfinite(lat) & np.isfinite(status) & (status == 0)
                if not np.any(valid):
                    continue
                active_timestamps.add(_iso_z(ts))
                occ, outside = tools.occupancy(lon[valid], lat[valid])
                outside_particle_positions += int(outside)
                run_mask = np.maximum(run_mask, occ)
        masks.append(run_mask)
    if not masks:
        probability = np.zeros((grid.height, grid.width), dtype=np.float32)
    else:
        probability = np.mean(np.stack(masks, axis=0), axis=0).astype(np.float32)
    return {
        "probability": probability,
        "any": (probability > 0).astype(np.float32),
        "run_count": int(len(masks)),
        "active_timestamps": sorted(active_timestamps),
        "outside_particle_positions": int(outside_particle_positions),
        "status_code_counts": status_codes,
    }


def _pygnome_time_index(nc_path: Path) -> tuple[list[pd.Timestamp], np.ndarray]:
    with netCDF4.Dataset(nc_path) as nc:
        raw_times = netCDF4.num2date(
            nc.variables["time"][:],
            nc.variables["time"].units,
            only_use_cftime_datetimes=False,
            only_use_python_datetimes=True,
        )
        counts = np.asarray(nc.variables["particle_count"][:], dtype=int)
    times = normalize_time_index(raw_times)
    return [pd.Timestamp(value) for value in times], counts


def _pygnome_mask_for_filter(
    nc_path: Path,
    grid: GridBuilder,
    tools: GridTools,
    time_filter: Callable[[pd.Timestamp], bool],
) -> dict[str, Any]:
    footprint = np.zeros((grid.height, grid.width), dtype=np.float32)
    outside_particle_positions = 0
    status_code_counts: dict[int, int] = {}
    active_timestamps: set[str] = set()
    timestamps_seen: set[str] = set()
    with netCDF4.Dataset(nc_path) as nc:
        raw_times = netCDF4.num2date(
            nc.variables["time"][:],
            nc.variables["time"].units,
            only_use_cftime_datetimes=False,
            only_use_python_datetimes=True,
        )
        times = normalize_time_index(raw_times)
        counts = np.asarray(nc.variables["particle_count"][:], dtype=int)
        offsets = np.concatenate([[0], np.cumsum(counts)])
        for time_index, timestamp in enumerate(times):
            ts = pd.Timestamp(timestamp)
            if not time_filter(ts):
                continue
            timestamps_seen.add(_iso_z(ts))
            start = int(offsets[time_index])
            end = int(offsets[time_index + 1])
            lon = np.asarray(nc.variables["longitude"][start:end], dtype=float)
            lat = np.asarray(nc.variables["latitude"][start:end], dtype=float)
            status = np.asarray(nc.variables["status_codes"][start:end], dtype=int)
            for code, count in zip(*np.unique(status, return_counts=True)):
                status_code_counts[int(code)] = status_code_counts.get(int(code), 0) + int(count)
            valid = (status == 2) & np.isfinite(lon) & np.isfinite(lat)
            if not np.any(valid):
                continue
            active_timestamps.add(_iso_z(ts))
            occ, outside = tools.occupancy(lon[valid], lat[valid])
            outside_particle_positions += int(outside)
            footprint = np.maximum(footprint, occ)
    return {
        "footprint": footprint,
        "active_timestamps": sorted(active_timestamps),
        "timestamps_seen": sorted(timestamps_seen),
        "outside_particle_positions": int(outside_particle_positions),
        "status_code_counts": status_code_counts,
    }


def _mask_by_scenario(raw: np.ndarray, mask: np.ndarray | None) -> np.ndarray:
    if mask is None:
        return (raw > 0).astype(np.float32)
    return ((raw > 0) & (mask > 0)).astype(np.float32)


def _prob_threshold(probability: np.ndarray, threshold: float, mask: np.ndarray | None) -> np.ndarray:
    if mask is None:
        return (probability >= threshold).astype(np.float32)
    return ((probability >= threshold) & (mask > 0)).astype(np.float32)


def _diagnostics(forecast: np.ndarray, obs: np.ndarray, tools: GridTools) -> dict[str, Any]:
    forecast = (np.asarray(forecast) > 0)
    obs = (np.asarray(obs) > 0)
    f_count = int(np.count_nonzero(forecast))
    o_count = int(np.count_nonzero(obs))
    intersection = int(np.count_nonzero(forecast & obs))
    union = int(np.count_nonzero(forecast | obs))
    iou = float(intersection / union) if union else 1.0
    dice = float((2 * intersection) / (f_count + o_count)) if (f_count + o_count) else 1.0
    f_points = tools.points_from_mask(forecast)
    o_points = tools.points_from_mask(obs)
    centroid_distance = np.nan
    nearest = np.nan
    if len(f_points) and len(o_points):
        centroid_distance = float(np.linalg.norm(f_points.mean(axis=0) - o_points.mean(axis=0)))
        if intersection:
            nearest = 0.0
        else:
            distances, _ = cKDTree(o_points).query(f_points, k=1)
            nearest = float(np.min(distances))
    return {
        "forecast_cells": f_count,
        "observed_cells": o_count,
        "intersection_cells": intersection,
        "union_cells": union,
        "iou": iou,
        "dice": dice,
        "centroid_distance_m": centroid_distance,
        "nearest_distance_to_obs_m": nearest,
    }


def _score_rows(
    *,
    product_id: str,
    model_family: str,
    forecast: np.ndarray,
    obs: np.ndarray,
    scenario_id: str,
    scenario_mask: np.ndarray | None,
    tools: GridTools,
    grid: GridBuilder,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    forecast_bin = (forecast > 0).astype(np.float32)
    obs_bin = (obs > 0).astype(np.float32)
    diag = {
        "product_id": product_id,
        "model_family": model_family,
        "mask_scenario": scenario_id,
        **_diagnostics(forecast_bin, obs_bin, tools),
    }
    fss_rows = []
    valid_mask = None if scenario_mask is None else scenario_mask.astype(bool)
    for window_km in OFFICIAL_PHASE3B_WINDOWS_KM:
        fss = calculate_fss(
            forecast_bin,
            obs_bin,
            window=_window_cells(grid, int(window_km)),
            valid_mask=valid_mask,
        )
        fss_rows.append(
            {
                "product_id": product_id,
                "model_family": model_family,
                "mask_scenario": scenario_id,
                "window_km": int(window_km),
                "window_cells": int(_window_cells(grid, int(window_km))),
                "fss": float(fss),
            }
        )
    return fss_rows, diag


def _coastline_distance_helpers(grid: GridBuilder, sea_mask: np.ndarray | None) -> tuple[cKDTree | None, np.ndarray, Any]:
    sea_tree = None
    sea_points = np.empty((0, 2), dtype=float)
    if sea_mask is not None and np.any(sea_mask > 0.5):
        rows, cols = np.nonzero(sea_mask > 0.5)
        xs = grid.min_x + ((cols + 0.5) * grid.resolution)
        ys = grid.max_y - ((rows + 0.5) * grid.resolution)
        sea_points = np.column_stack([xs, ys])
        sea_tree = cKDTree(sea_points)
    shoreline_union = None
    shoreline_path = Path(grid.spec.shoreline_segments_path or "")
    if shoreline_path.exists():
        try:
            shoreline = gpd.read_file(shoreline_path)
            if shoreline.crs is None:
                shoreline = shoreline.set_crs(grid.crs)
            shoreline = shoreline.to_crs(grid.crs)
            shoreline_union = shoreline.geometry.union_all() if hasattr(shoreline.geometry, "union_all") else shoreline.geometry.unary_union
        except Exception:
            shoreline_union = None
    return sea_tree, sea_points, shoreline_union


def _dropped_cell_audit(
    raw_member_any: np.ndarray,
    retained_member_any: np.ndarray,
    sea_mask: np.ndarray | None,
    land_mask: np.ndarray | None,
    raw_probability: np.ndarray,
    tools: GridTools,
    grid: GridBuilder,
) -> tuple[pd.DataFrame, np.ndarray]:
    dropped = (raw_member_any > 0) & ~(retained_member_any > 0)
    sea_tree, _, shoreline_union = _coastline_distance_helpers(grid, sea_mask)
    rows = []
    for row, col in zip(*np.nonzero(dropped)):
        x, y = tools.cell_center(int(row), int(col))
        lon, lat = tools.cell_center_lonlat(int(row), int(col))
        sea_value = float(sea_mask[row, col]) if sea_mask is not None else np.nan
        land_value = float(land_mask[row, col]) if land_mask is not None else np.nan
        if sea_tree is not None:
            dist_to_valid_ocean, _ = sea_tree.query([[x, y]], k=1)
            dist_to_valid_ocean_m = float(dist_to_valid_ocean[0])
        else:
            dist_to_valid_ocean_m = np.nan
        if shoreline_union is not None:
            from shapely.geometry import Point

            distance_to_shoreline_m = float(shoreline_union.distance(Point(x, y)))
        else:
            distance_to_shoreline_m = np.nan

        if sea_value > 0.5:
            classification = "valid_ocean_retained"
        elif land_value > 0.5 and np.isfinite(dist_to_valid_ocean_m) and dist_to_valid_ocean_m <= grid.resolution * 1.5:
            classification = "shoreline_land_cell"
        elif land_value > 0.5:
            classification = "land_cell"
        elif np.isfinite(dist_to_valid_ocean_m) and dist_to_valid_ocean_m <= grid.resolution * 1.5:
            classification = "shoreline_invalid_ocean_mask_cell"
        else:
            classification = "invalid_ocean_mask_cell"

        rows.append(
            {
                "row": int(row),
                "col": int(col),
                "x_center_m": x,
                "y_center_m": y,
                "lon_center": lon,
                "lat_center": lat,
                "sea_mask_value": sea_value,
                "land_mask_value": land_value,
                "classification": classification,
                "distance_to_nearest_valid_ocean_cell_m": dist_to_valid_ocean_m,
                "distance_to_shoreline_m": distance_to_shoreline_m,
                "raw_member_support_probability": float(raw_probability[row, col]),
                "raw_member_support_count_of_50": int(round(float(raw_probability[row, col]) * 50.0)),
            }
        )
    frame = pd.DataFrame(rows).sort_values(["row", "col"])
    _write_csv(OUT_DIR / "opendrift_r1_dropped_cells_audit.csv", frame)
    return frame, dropped.astype(np.float32)


def _product_definition_tables(
    *,
    grid: GridBuilder,
    sea_mask_bool: np.ndarray,
    od_member_raw_prob: np.ndarray,
    od_control_raw: np.ndarray,
    py_raw: np.ndarray,
    target_raw: np.ndarray,
    py_stats: dict[str, Any],
    od_member_stats: dict[str, Any],
    od_control_stats: dict[str, Any],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    od_prob_ocean = od_member_raw_prob * sea_mask_bool
    rows = [
        {
            "product_id": "opendrift_r1_control",
            "model_family": "OpenDrift",
            "product_definition": "deterministic/control local-date footprint from active status==0 particles",
            "march4_localdate_composite": True,
            "raw_cells_before_valid_ocean_mask": int(np.count_nonzero(od_control_raw > 0)),
            "cells_after_official_valid_ocean_mask": int(np.count_nonzero((od_control_raw > 0) & sea_mask_bool)),
            "active_timestamp_count": int(len(od_control_stats["active_timestamps"])),
            "active_timestamps_utc": ";".join(od_control_stats["active_timestamps"]),
            "outside_particle_positions": int(od_control_stats["outside_particle_positions"]),
        },
        {
            "product_id": "opendrift_r1_raw_member_support_any",
            "model_family": "OpenDrift",
            "product_definition": "50-member union of member-wise March 4 local-date occupancy before mask",
            "march4_localdate_composite": True,
            "raw_cells_before_valid_ocean_mask": int(np.count_nonzero(od_member_raw_prob > 0)),
            "cells_after_official_valid_ocean_mask": int(np.count_nonzero((od_member_raw_prob > 0) & sea_mask_bool)),
            "active_timestamp_count": int(len(od_member_stats["active_timestamps"])),
            "active_timestamps_utc": ";".join(od_member_stats["active_timestamps"]),
            "outside_particle_positions": int(od_member_stats["outside_particle_positions"]),
        },
        {
            "product_id": "opendrift_r1_prob_presence",
            "model_family": "OpenDrift",
            "product_definition": "continuous member-support probability after official valid-ocean mask",
            "march4_localdate_composite": True,
            "raw_cells_before_valid_ocean_mask": int(np.count_nonzero(od_member_raw_prob > 0)),
            "cells_after_official_valid_ocean_mask": int(np.count_nonzero(od_prob_ocean > 0)),
            "positive_probability_values_after_mask": ";".join(f"{v:.6g}" for v in np.unique(od_prob_ocean[od_prob_ocean > 0])),
        },
        {
            "product_id": "opendrift_r1_p10",
            "model_family": "OpenDrift",
            "product_definition": "probability >= 0.10 after official valid-ocean mask",
            "march4_localdate_composite": True,
            "raw_cells_before_valid_ocean_mask": int(np.count_nonzero(od_member_raw_prob >= 0.10)),
            "cells_after_official_valid_ocean_mask": int(np.count_nonzero(od_prob_ocean >= 0.10)),
        },
        {
            "product_id": "opendrift_r1_p25",
            "model_family": "OpenDrift",
            "product_definition": "probability >= 0.25 after official valid-ocean mask",
            "march4_localdate_composite": True,
            "raw_cells_before_valid_ocean_mask": int(np.count_nonzero(od_member_raw_prob >= 0.25)),
            "cells_after_official_valid_ocean_mask": int(np.count_nonzero(od_prob_ocean >= 0.25)),
        },
        {
            "product_id": "opendrift_r1_p50",
            "model_family": "OpenDrift",
            "product_definition": "probability >= 0.50 after official valid-ocean mask",
            "march4_localdate_composite": True,
            "raw_cells_before_valid_ocean_mask": int(np.count_nonzero(od_member_raw_prob >= 0.50)),
            "cells_after_official_valid_ocean_mask": int(np.count_nonzero(od_prob_ocean >= 0.50)),
        },
        {
            "product_id": "opendrift_r1_p90",
            "model_family": "OpenDrift",
            "product_definition": "probability >= 0.90 after official valid-ocean mask",
            "march4_localdate_composite": True,
            "raw_cells_before_valid_ocean_mask": int(np.count_nonzero(od_member_raw_prob >= 0.90)),
            "cells_after_official_valid_ocean_mask": int(np.count_nonzero(od_prob_ocean >= 0.90)),
        },
        {
            "product_id": "pygnome_deterministic",
            "model_family": "PyGNOME",
            "product_definition": "deterministic local-date footprint from status_codes==2 in-water particles",
            "march4_localdate_composite": True,
            "raw_cells_before_valid_ocean_mask": int(np.count_nonzero(py_raw > 0)),
            "cells_inside_scoring_grid": int(np.count_nonzero(py_raw > 0)),
            "cells_after_official_valid_ocean_mask": int(np.count_nonzero((py_raw > 0) & sea_mask_bool)),
            "active_timestamp_count": int(len(py_stats["active_timestamps"])),
            "active_timestamps_utc": ";".join(py_stats["active_timestamps"]),
            "outside_particle_positions": int(py_stats["outside_particle_positions"]),
        },
    ]
    product_df = pd.DataFrame(rows)
    _write_csv(OUT_DIR / "product_definition_parity.csv", product_df)

    target_row = {
        "product_id": "march4_philsa_target",
        "model_family": "Observation",
        "product_definition": "March 4 PhilSA target raster; raw and official-valid-ocean counts are identical in this run",
        "raw_cells_before_valid_ocean_mask": int(np.count_nonzero(target_raw > 0)),
        "cells_after_official_valid_ocean_mask": int(np.count_nonzero((target_raw > 0) & sea_mask_bool)),
    }
    _write_csv(OUT_DIR / "target_product_definition.csv", pd.DataFrame([target_row]))
    return product_df, pd.DataFrame([target_row])


def _time_composite_parity(
    *,
    grid: GridBuilder,
    tools: GridTools,
    sea_mask_bool: np.ndarray,
    od_member_paths: list[Path],
    od_control_path: Path,
    py_nc_path: Path,
) -> pd.DataFrame:
    march4_filter = lambda ts: _local_date(ts) == TARGET_DATE
    march3_to_4_filter = lambda ts: _local_date(ts) in {SEED_DATE, TARGET_DATE}
    exact_utc = "2023-03-04T15:00:00Z"
    exact_ts = pd.Timestamp(exact_utc.replace("Z", ""))
    exact_filter = lambda ts: pd.Timestamp(ts) == exact_ts

    rows = []
    specs = [
        (
            "OpenDrift",
            "opendrift_r1_control",
            lambda flt: _opendrift_masks_for_paths([od_control_path], grid, tools, flt)["any"],
            "deterministic/control",
            True,
        ),
        (
            "OpenDrift",
            "opendrift_r1_member_support_any",
            lambda flt: _opendrift_masks_for_paths(od_member_paths, grid, tools, flt)["any"],
            "50-member any-support union",
            False,
        ),
        (
            "PyGNOME",
            "pygnome_deterministic",
            lambda flt: _pygnome_mask_for_filter(py_nc_path, grid, tools, flt)["footprint"],
            "deterministic in-water footprint",
            True,
        ),
    ]
    for model, product_id, builder, definition, existing in specs:
        exact = builder(exact_filter)
        local = builder(march4_filter)
        cumulative = builder(march3_to_4_filter)
        rows.append(
            {
                "model_family": model,
                "product_id": product_id,
                "definition": definition,
                "existing_scored_comparison_uses_march4_localdate_composite": bool(existing),
                "exact_snapshot_utc_checked": exact_utc,
                "exact_snapshot_raw_cells": int(np.count_nonzero(exact > 0)),
                "exact_snapshot_official_mask_cells": int(np.count_nonzero((exact > 0) & sea_mask_bool)),
                "march4_localdate_raw_cells": int(np.count_nonzero(local > 0)),
                "march4_localdate_official_mask_cells": int(np.count_nonzero((local > 0) & sea_mask_bool)),
                "march3_to_march4_cumulative_raw_cells": int(np.count_nonzero(cumulative > 0)),
                "march3_to_march4_cumulative_official_mask_cells": int(np.count_nonzero((cumulative > 0) & sea_mask_bool)),
            }
        )
    frame = pd.DataFrame(rows)
    _write_csv(OUT_DIR / "time_composite_parity.csv", frame)
    return frame


def _physics_config_parity(
    context: dict[str, Any],
    od_control_path: Path,
    py_nc_path: Path,
) -> pd.DataFrame:
    run_manifest = context["run_manifest"]
    od_forecast = context["opendrift_forecast_manifest"]
    od_ensemble = context["opendrift_ensemble_manifest"]
    py_meta = context["pygnome_metadata"]
    branch = next((row for row in run_manifest.get("branches", []) if row.get("branch_id") == "R1_previous"), {})
    od_config = (od_forecast.get("deterministic_control") or {}).get("configuration") or {}
    od_transport = od_forecast.get("transport") or {}
    od_ensemble_config = od_ensemble.get("ensemble_configuration") or {}

    with xr.open_dataset(od_control_path) as ds:
        od_times = normalize_time_index(ds["time"].values)
        if "wind_drift_factor" in ds:
            wind_drift = np.asarray(ds["wind_drift_factor"].values, dtype=float)
            od_windage = f"{float(np.nanmin(wind_drift)):.6g}..{float(np.nanmax(wind_drift)):.6g}"
        else:
            od_windage = str(od_config.get("wind_factor", ""))
        if "horizontal_diffusivity" in ds:
            diffusivity = np.asarray(ds["horizontal_diffusivity"].values, dtype=float)
            od_diff_exported = f"{float(np.nanmin(diffusivity)):.6g}..{float(np.nanmax(diffusivity)):.6g} m2/s exported"
        else:
            od_diff_exported = "not exported"
        od_status_meanings = str(ds["status"].attrs.get("flag_meanings", ""))
    od_diff = (
        f"configured deterministic={od_config.get('horizontal_diffusivity_m2s', '')} m2/s; "
        f"ensemble={od_ensemble_config.get('horizontal_diffusivity_m2s_min', '')}.."
        f"{od_ensemble_config.get('horizontal_diffusivity_m2s_max', '')} m2/s; "
        f"{od_diff_exported}"
    )

    py_times, _ = _pygnome_time_index(py_nc_path)
    py_windage = ""
    with netCDF4.Dataset(py_nc_path) as nc:
        if "windage_range" in nc.variables:
            vals = np.asarray(nc.variables["windage_range"][:], dtype=float)
            if vals.size:
                py_windage = f"{float(np.nanmin(vals)):.6g}..{float(np.nanmax(vals)):.6g}"

    rows = [
        {
            "field": "release/start time UTC",
            "opendrift_r1_previous": (run_manifest.get("window") or {}).get("simulation_start_utc", ""),
            "pygnome_deterministic": py_meta.get("release_start_utc", ""),
        },
        {
            "field": "release/start time Asia/Manila",
            "opendrift_r1_previous": _local_iso((run_manifest.get("window") or {}).get("simulation_start_utc", "")),
            "pygnome_deterministic": _local_iso(py_meta.get("release_start_utc", "")),
        },
        {
            "field": "release geometry",
            "opendrift_r1_previous": (run_manifest.get("seed_release") or {}).get("release_geometry_label", ""),
            "pygnome_deterministic": "same March 3 PhilSA polygon, sampled as clustered point releases",
        },
        {
            "field": "element/particle count",
            "opendrift_r1_previous": run_manifest.get("element_count_detected_from_manifest", ""),
            "pygnome_deterministic": py_meta.get("benchmark_particles", ""),
        },
        {
            "field": "current forcing",
            "opendrift_r1_previous": str((BASE / "forcing/cmems_curr.nc")),
            "pygnome_deterministic": py_meta.get("current_forcing_path", ""),
        },
        {
            "field": "wind forcing",
            "opendrift_r1_previous": str((BASE / "forcing/gfs_wind.nc")),
            "pygnome_deterministic": py_meta.get("wind_forcing_path", ""),
        },
        {
            "field": "windage/direct wind drift factor",
            "opendrift_r1_previous": od_windage,
            "pygnome_deterministic": py_windage or "PyGNOME WindMover default/exported windage not explicit",
        },
        {
            "field": "diffusion/random walk setting",
            "opendrift_r1_previous": od_diff,
            "pygnome_deterministic": f"{py_meta.get('diffusion_coef_cm2s', '')} cm2/s",
        },
        {
            "field": "wave/Stokes handling",
            "opendrift_r1_previous": f"stokes_drift_enabled={od_transport.get('stokes_drift_enabled', '')}; wave_forcing_required={od_transport.get('wave_forcing_required', '')}",
            "pygnome_deterministic": "no matched wave/Stokes mover in the transport benchmark; weathering_enabled=False",
        },
        {
            "field": "beaching/stranding behavior",
            "opendrift_r1_previous": f"coastline_action={branch.get('coastline_action', '')}; NetCDF status={od_status_meanings}",
            "pygnome_deterministic": "status_codes==2 in-water footprint; no matched shoreline-arrival or beaching product",
        },
        {
            "field": "shoreline/map handling",
            "opendrift_r1_previous": "OpenDrift coastline_action=previous plus official scoring sea/land mask",
            "pygnome_deterministic": "no shoreline map ingested by benchmark; official scoring sea/land mask only applied during post-processing",
        },
        {
            "field": "output timestep",
            "opendrift_r1_previous": f"{int((od_times[1] - od_times[0]).total_seconds() / 60)} minutes" if len(od_times) > 1 else "",
            "pygnome_deterministic": f"{int((py_times[1] - py_times[0]).total_seconds() / 60)} minutes" if len(py_times) > 1 else "",
        },
        {
            "field": "footprint rasterization rule",
            "opendrift_r1_previous": "cell is present if any active status==0 particle occupies it during the March 4 local-date composite; ensemble prob is mean of member-wise presence",
            "pygnome_deterministic": "cell is present if any status_codes==2 in-water particle occupies it during the March 4 local-date composite",
        },
        {
            "field": "ensemble/product note",
            "opendrift_r1_previous": f"member_count={od_ensemble_config.get('ensemble_size', 50)}; p50/p90 threshold member-support probability",
            "pygnome_deterministic": "single deterministic comparator, not an ensemble probability product",
        },
    ]
    frame = pd.DataFrame(rows)
    _write_csv(OUT_DIR / "physics_config_parity.csv", frame)
    return frame


def _make_crop(arrays: list[np.ndarray], pad: int = 12) -> tuple[slice, slice]:
    coords = []
    for arr in arrays:
        ys, xs = np.nonzero(np.asarray(arr) > 0)
        if ys.size:
            coords.append(np.column_stack([ys, xs]))
    if not coords:
        return slice(None), slice(None)
    all_coords = np.vstack(coords)
    y0 = max(0, int(all_coords[:, 0].min()) - pad)
    y1 = min(arrays[0].shape[0], int(all_coords[:, 0].max()) + pad + 1)
    x0 = max(0, int(all_coords[:, 1].min()) - pad)
    x1 = min(arrays[0].shape[1], int(all_coords[:, 1].max()) + pad + 1)
    return slice(y0, y1), slice(x0, x1)


def _imshow_mask(ax, arr: np.ndarray, title: str, color: str, alpha: float = 0.9) -> None:
    ax.imshow(np.zeros_like(arr), cmap=ListedColormap(["#f7f8fb"]), vmin=0, vmax=1)
    ax.imshow(np.ma.masked_where(arr <= 0, arr), cmap=ListedColormap([color]), alpha=alpha, interpolation="nearest")
    ax.set_title(title, fontsize=10, fontweight="bold")
    ax.set_xticks([])
    ax.set_yticks([])


def _write_dropped_map(
    seed: np.ndarray,
    target: np.ndarray,
    raw_member_any: np.ndarray,
    retained: np.ndarray,
    dropped: np.ndarray,
    sea_mask: np.ndarray,
) -> Path:
    crop = _make_crop([target, raw_member_any, dropped, retained], pad=10)
    target_c = target[crop]
    raw_c = raw_member_any[crop]
    retained_c = retained[crop]
    dropped_c = dropped[crop]
    sea_c = sea_mask[crop]
    fig, ax = plt.subplots(figsize=(8, 8), dpi=180)
    ax.imshow(sea_c, cmap=ListedColormap(["#d7d7d7", "#d5f1f8"]), vmin=0, vmax=1, interpolation="nearest")
    ax.contour(sea_c, levels=[0.5], colors=["#334155"], linewidths=0.8)
    ax.imshow(np.ma.masked_where(target_c <= 0, target_c), cmap=ListedColormap(["#2563eb"]), alpha=0.65)
    ax.imshow(np.ma.masked_where(raw_c <= 0, raw_c), cmap=ListedColormap(["#f59e0b"]), alpha=0.35)
    ax.imshow(np.ma.masked_where(dropped_c <= 0, dropped_c), cmap=ListedColormap(["#ef4444"]), alpha=0.92)
    ax.imshow(np.ma.masked_where(retained_c <= 0, retained_c), cmap=ListedColormap(["#16a34a"]), alpha=0.95)
    ax.set_title("OpenDrift R1 Dropped-Cell Map: Raw 19 -> Valid-Ocean 5", fontsize=12, fontweight="bold")
    ax.axis("off")
    fig.text(
        0.05,
        0.03,
        "Blue=Mar 4 PhilSA target; orange=raw OpenDrift member-support; red=dropped by official mask; green=retained valid-ocean cells; gray/blue background=land/sea mask.",
        fontsize=8,
        color="#334155",
    )
    path = OUT_DIR / "opendrift_r1_dropped_cell_map.png"
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)
    return path


def _make_board(
    *,
    seed: np.ndarray,
    target: np.ndarray,
    sea_mask: np.ndarray,
    od_raw_any: np.ndarray,
    od_dropped: np.ndarray,
    od_retained: np.ndarray,
    od_prob: np.ndarray,
    od_threshold_class: np.ndarray,
    py_raw: np.ndarray,
    py_masked: np.ndarray,
) -> Path:
    crop = _make_crop([seed, target, od_raw_any, od_dropped, od_retained, py_raw, py_masked], pad=14)
    seed_c = seed[crop]
    target_c = target[crop]
    sea_c = sea_mask[crop]
    raw_c = od_raw_any[crop]
    dropped_c = od_dropped[crop]
    retained_c = od_retained[crop]
    prob_c = od_prob[crop]
    threshold_c = od_threshold_class[crop]
    py_raw_c = py_raw[crop]
    py_masked_c = py_masked[crop]

    fig, axes = plt.subplots(3, 3, figsize=(15, 13), dpi=180, constrained_layout=True)
    fig.suptitle(
        "Diagnostic Only: Apples-to-Apples March 4 PhilSA Composite",
        fontsize=15,
        fontweight="bold",
    )
    _imshow_mask(axes[0, 0], seed_c, f"March 3 PhilSA Seed\n{int(np.count_nonzero(seed > 0))} cells", "#f59e0b")
    _imshow_mask(axes[0, 1], target_c, f"March 4 PhilSA Target\n{int(np.count_nonzero(target > 0))} cells", "#2563eb")
    _imshow_mask(axes[0, 2], raw_c, f"OpenDrift R1 Raw Member Support\n{int(np.count_nonzero(od_raw_any > 0))} cells before mask", "#f97316")

    axes[1, 0].imshow(sea_c, cmap=ListedColormap(["#d7d7d7", "#e0f7fb"]), vmin=0, vmax=1)
    axes[1, 0].contour(sea_c, levels=[0.5], colors=["#334155"], linewidths=0.6)
    axes[1, 0].imshow(np.ma.masked_where(dropped_c <= 0, dropped_c), cmap=ListedColormap(["#ef4444"]), alpha=0.92)
    axes[1, 0].imshow(np.ma.masked_where(retained_c <= 0, retained_c), cmap=ListedColormap(["#16a34a"]), alpha=0.95)
    axes[1, 0].set_title(
        f"OpenDrift Mask Split\n{int(np.count_nonzero(od_dropped > 0))} dropped / {int(np.count_nonzero(od_retained > 0))} retained",
        fontsize=10,
        fontweight="bold",
    )
    axes[1, 0].set_xticks([])
    axes[1, 0].set_yticks([])

    im = axes[1, 1].imshow(np.ma.masked_where(prob_c <= 0, prob_c), cmap="inferno", vmin=0, vmax=1)
    axes[1, 1].imshow(np.ma.masked_where(prob_c > 0, np.zeros_like(prob_c)), cmap=ListedColormap(["#f7f8fb"]))
    axes[1, 1].set_title("OpenDrift prob_presence\ncontinuous after official mask", fontsize=10, fontweight="bold")
    axes[1, 1].set_xticks([])
    axes[1, 1].set_yticks([])
    fig.colorbar(im, ax=axes[1, 1], fraction=0.046, pad=0.02)

    axes[1, 2].imshow(threshold_c, cmap=ListedColormap(["#f7f8fb", "#8ecae6", "#219ebc", "#ffb703", "#d62828"]), vmin=0, vmax=4)
    axes[1, 2].set_title("OpenDrift thresholds\np10 blue, p25 teal, p50 amber, p90 red", fontsize=10, fontweight="bold")
    axes[1, 2].set_xticks([])
    axes[1, 2].set_yticks([])

    _imshow_mask(axes[2, 0], py_raw_c, f"PyGNOME Raw Deterministic\n{int(np.count_nonzero(py_raw > 0))} cells before mask", "#7c3aed")
    _imshow_mask(axes[2, 1], py_masked_c, f"PyGNOME Same Valid-Ocean Mask\n{int(np.count_nonzero(py_masked > 0))} cells retained", "#16a34a")
    axes[2, 2].imshow(sea_c, cmap=ListedColormap(["#d7d7d7", "#d5f1f8"]), vmin=0, vmax=1)
    axes[2, 2].contour(sea_c, levels=[0.5], colors=["#334155"], linewidths=0.7)
    axes[2, 2].imshow(np.ma.masked_where(target_c <= 0, target_c), cmap=ListedColormap(["#2563eb"]), alpha=0.72)
    axes[2, 2].set_title("Official Valid-Ocean Mask\nwith March 4 target overlay", fontsize=10, fontweight="bold")
    axes[2, 2].set_xticks([])
    axes[2, 2].set_yticks([])

    for ax in axes.flat:
        for spine in ax.spines.values():
            spine.set_color("#cbd5e1")
            spine.set_linewidth(0.8)

    path = OUT_DIR / "march3_4_philsa_5000_apples_to_apples_board.png"
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)
    return path


def _write_note(
    *,
    product_df: pd.DataFrame,
    py_masking: pd.DataFrame,
    dropped_df: pd.DataFrame,
    sensitivity: pd.DataFrame,
    time_parity: pd.DataFrame,
    board_path: Path,
    dropped_map_path: Path,
) -> Path:
    od_raw = int(product_df.loc[product_df.product_id.eq("opendrift_r1_raw_member_support_any"), "raw_cells_before_valid_ocean_mask"].iloc[0])
    od_masked = int(product_df.loc[product_df.product_id.eq("opendrift_r1_raw_member_support_any"), "cells_after_official_valid_ocean_mask"].iloc[0])
    py_raw = int(product_df.loc[product_df.product_id.eq("pygnome_deterministic"), "raw_cells_before_valid_ocean_mask"].iloc[0])
    py_masked = int(product_df.loc[product_df.product_id.eq("pygnome_deterministic"), "cells_after_official_valid_ocean_mask"].iloc[0])
    od_p50 = int(product_df.loc[product_df.product_id.eq("opendrift_r1_p50"), "cells_after_official_valid_ocean_mask"].iloc[0])
    official_od_p50 = sensitivity[
        sensitivity.product_id.eq("opendrift_r1_p50") & sensitivity.mask_scenario.eq("official_valid_ocean") & sensitivity.window_km.eq(10)
    ]["fss"].iloc[0]
    no_mask_od_p50 = sensitivity[
        sensitivity.product_id.eq("opendrift_r1_p50") & sensitivity.mask_scenario.eq("scoring_grid_only") & sensitivity.window_km.eq(10)
    ]["fss"].iloc[0]
    coastal_od_p50 = sensitivity[
        sensitivity.product_id.eq("opendrift_r1_p50") & sensitivity.mask_scenario.eq("valid_ocean_plus_one_cell") & sensitivity.window_km.eq(10)
    ]["fss"].iloc[0]
    time_rows = time_parity[["product_id", "march4_localdate_official_mask_cells", "march3_to_march4_cumulative_official_mask_cells"]]

    note = f"""# Apples-to-Apples Diagnostic Note

Status: diagnostic/archive only. This is not thesis-facing and does not modify canonical B1.

## Main Result

OpenDrift R1_previous does not become sparse because of a missing March 4 target or wrong source layer. The specific 19 -> 5 collapse is caused by official valid-ocean mask clipping of coastal/land-adjacent grid cells: {od_raw} raw member-support cells become {od_masked} valid-ocean cells, and {len(dropped_df.index)} cells are dropped. The remaining p50 mask has {od_p50} cells because probability thresholding is applied after that mask.

PyGNOME is wider even under the same grid/mask: its deterministic March 4 local-date footprint goes from {py_raw} raw cells to {py_masked} official valid-ocean cells. That difference is not explained by plotting or scoring-grid mismatch. It is mainly a product/physics/config difference: PyGNOME is a single deterministic in-water cumulative footprint with no matched shoreline map/beaching product, while OpenDrift R1_previous uses the previous-wet coastline-retention behavior and an ensemble probability product.

## Mask Sensitivity

For OpenDrift R1 p50, FSS at 10 km is official={official_od_p50:.6f}, no-mask={no_mask_od_p50:.6f}, one-cell coastal tolerance={coastal_od_p50:.6f}. These are diagnostic-only calculations and do not change official scoring.

## Time/Product Parity

Both existing OpenDrift and PyGNOME comparison products use the March 4 Asia/Manila local-date composite, not a March 6 or March 13/14 window. The parity table also reports exact snapshot and March 3-to-4 cumulative counts:

{time_rows.to_string(index=False)}

## Interpretation

- True transport/stranding: important for R0; less so for R1_previous because R1 remains active through the target local date.
- Ocean-mask clipping: primary reason for the R1_previous 19 -> 5 cell collapse.
- Shoreline/coastal-grid mismatch: important; dropped cells are official-mask land/shoreline-adjacent cells.
- Probability thresholding: further reduces R1_previous from 5 any-support valid-ocean cells to p50=3 and p90=2.
- Time/composite mismatch: not supported by this diagnostic; both scored products are March 4 local-date composites.
- Product-definition mismatch: important when comparing OpenDrift p50/p90 to PyGNOME deterministic.
- Model-physics differences: important for PyGNOME's wider footprint, especially no matched shoreline map/beaching handling and different diffusion/wind/current implementation.

## Artifacts

- Board: {board_path}
- Dropped-cell map: {dropped_map_path}
- Product parity: {OUT_DIR / 'product_definition_parity.csv'}
- PyGNOME masking check: {OUT_DIR / 'pygnome_masking_check.csv'}
- OpenDrift dropped cells: {OUT_DIR / 'opendrift_r1_dropped_cells_audit.csv'}
- Coastal mask sensitivity: {OUT_DIR / 'coastal_mask_sensitivity_fss.csv'}
- Physics/config parity: {OUT_DIR / 'physics_config_parity.csv'}
"""
    path = OUT_DIR / "march3_4_philsa_5000_apples_to_apples_note.md"
    path.write_text(note, encoding="utf-8")
    return path


def run() -> dict[str, Any]:
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    context = _validate_inputs()
    grid = GridBuilder()
    sea_mask = load_sea_mask_array(grid.spec)
    if sea_mask is None:
        raise RuntimeError("Official sea mask is required for apples-to-apples diagnostics.")
    sea_mask_bool = sea_mask > 0.5
    land_mask = None
    if grid.spec.land_mask_path and Path(grid.spec.land_mask_path).exists():
        land_mask = load_binary_mask(grid.spec.land_mask_path)
    tools = GridTools(grid, sea_mask)

    seed = _read_raster(BASE / "march3_seed_mask_on_grid.tif")
    target = _read_raster(BASE / "march4_target_mask_on_grid.tif")
    target_raw = target.copy()

    od_control_path = BASE / "R1_previous/model_run/forecast/deterministic_control_cmems_gfs.nc"
    od_member_paths = sorted((BASE / "R1_previous/model_run/ensemble").glob("member_*.nc"))
    py_nc_path = BASE / "pygnome_comparator/model_run/pygnome_march3_4_philsa_5000_deterministic_control.nc"

    march4_filter = lambda ts: _local_date(ts) == TARGET_DATE
    od_members = _opendrift_masks_for_paths(od_member_paths, grid, tools, march4_filter)
    od_control = _opendrift_masks_for_paths([od_control_path], grid, tools, march4_filter)
    py = _pygnome_mask_for_filter(py_nc_path, grid, tools, march4_filter)

    od_member_raw_prob = od_members["probability"]
    od_member_raw_any = (od_member_raw_prob > 0).astype(np.float32)
    od_member_ocean_prob = od_member_raw_prob * sea_mask_bool.astype(np.float32)
    od_member_ocean_any = ((od_member_raw_prob > 0) & sea_mask_bool).astype(np.float32)
    od_control_raw = od_control["any"]
    od_control_ocean = ((od_control_raw > 0) & sea_mask_bool).astype(np.float32)
    py_raw = py["footprint"]
    py_ocean = ((py_raw > 0) & sea_mask_bool).astype(np.float32)

    save_raster(grid, od_member_raw_any, OUT_DIR / "opendrift_r1_raw_member_support_before_ocean_mask.tif")
    save_raster(grid, od_member_ocean_any, OUT_DIR / "opendrift_r1_valid_ocean_member_support.tif")
    save_raster(grid, od_control_raw, OUT_DIR / "opendrift_r1_control_before_ocean_mask.tif")
    save_raster(grid, od_control_ocean, OUT_DIR / "opendrift_r1_control_after_ocean_mask.tif")
    save_raster(grid, py_raw, OUT_DIR / "pygnome_deterministic_before_ocean_mask.tif")
    save_raster(grid, py_ocean, OUT_DIR / "pygnome_deterministic_after_same_ocean_mask.tif")

    product_df, _ = _product_definition_tables(
        grid=grid,
        sea_mask_bool=sea_mask_bool,
        od_member_raw_prob=od_member_raw_prob,
        od_control_raw=od_control_raw,
        py_raw=py_raw,
        target_raw=target_raw,
        py_stats=py,
        od_member_stats=od_members,
        od_control_stats=od_control,
    )

    dropped_df, od_dropped = _dropped_cell_audit(
        od_member_raw_any,
        od_member_ocean_any,
        sea_mask,
        land_mask,
        od_member_raw_prob,
        tools,
        grid,
    )
    save_raster(grid, od_dropped, OUT_DIR / "opendrift_r1_dropped_by_official_ocean_mask.tif")

    py_masking_rows = []
    py_diag = _diagnostics(py_ocean, target_raw, tools)
    for window_km in OFFICIAL_PHASE3B_WINDOWS_KM:
        py_diag[f"fss_{int(window_km)}km"] = calculate_fss(
            py_ocean,
            (target_raw > 0).astype(np.float32),
            window=_window_cells(grid, int(window_km)),
            valid_mask=sea_mask_bool,
        )
    py_masking_rows.append(
        {
            "product_id": "pygnome_deterministic",
            "raw_footprint_cells_before_any_mask": int(np.count_nonzero(py_raw > 0)),
            "cells_inside_scoring_grid": int(np.count_nonzero(py_raw > 0)),
            "cells_after_same_valid_ocean_mask": int(np.count_nonzero(py_ocean > 0)),
            "cells_removed_by_land_invalid_ocean_mask": int(np.count_nonzero((py_raw > 0) & ~sea_mask_bool)),
            "active_particle_positions_outside_scoring_grid_across_march4": int(py["outside_particle_positions"]),
            "status_code_counts_across_march4": json.dumps(py["status_code_counts"], sort_keys=True),
            **py_diag,
        }
    )
    py_masking_df = pd.DataFrame(py_masking_rows)
    _write_csv(OUT_DIR / "pygnome_masking_check.csv", py_masking_df)

    scenario_masks = {
        "official_valid_ocean": sea_mask_bool,
        "scoring_grid_only": np.ones_like(sea_mask_bool, dtype=bool),
        "valid_ocean_plus_one_cell": binary_dilation(sea_mask_bool, structure=np.ones((3, 3), dtype=bool)),
    }
    product_builders: dict[str, tuple[str, str, Callable[[np.ndarray], np.ndarray]]] = {
        "opendrift_r1_control": (
            "OpenDrift",
            "deterministic/control",
            lambda mask: _mask_by_scenario(od_control_raw, mask),
        ),
        "opendrift_r1_member_support_any": (
            "OpenDrift",
            "any-member support",
            lambda mask: _mask_by_scenario(od_member_raw_any, mask),
        ),
        "opendrift_r1_p10": (
            "OpenDrift",
            "probability >= 0.10",
            lambda mask: _prob_threshold(od_member_raw_prob, 0.10, mask),
        ),
        "opendrift_r1_p25": (
            "OpenDrift",
            "probability >= 0.25",
            lambda mask: _prob_threshold(od_member_raw_prob, 0.25, mask),
        ),
        "opendrift_r1_p50": (
            "OpenDrift",
            "probability >= 0.50",
            lambda mask: _prob_threshold(od_member_raw_prob, 0.50, mask),
        ),
        "opendrift_r1_p90": (
            "OpenDrift",
            "probability >= 0.90",
            lambda mask: _prob_threshold(od_member_raw_prob, 0.90, mask),
        ),
        "pygnome_deterministic": (
            "PyGNOME",
            "deterministic footprint",
            lambda mask: _mask_by_scenario(py_raw, mask),
        ),
    }
    fss_rows: list[dict[str, Any]] = []
    diag_rows: list[dict[str, Any]] = []
    for scenario_id, scenario_mask in scenario_masks.items():
        obs_scenario = _mask_by_scenario(target_raw, scenario_mask)
        eval_mask = None if scenario_id == "scoring_grid_only" else scenario_mask
        for product_id, (model_family, definition, builder) in product_builders.items():
            forecast = builder(scenario_mask)
            rows, diag = _score_rows(
                product_id=product_id,
                model_family=model_family,
                forecast=forecast,
                obs=obs_scenario,
                scenario_id=scenario_id,
                scenario_mask=eval_mask,
                tools=tools,
                grid=grid,
            )
            for row in rows:
                row["product_definition"] = definition
            diag["product_definition"] = definition
            fss_rows.extend(rows)
            diag_rows.append(diag)
    fss_df = pd.DataFrame(fss_rows)
    diag_df = pd.DataFrame(diag_rows)
    _write_csv(OUT_DIR / "coastal_mask_sensitivity_fss.csv", fss_df)
    _write_csv(OUT_DIR / "coastal_mask_sensitivity_diagnostics.csv", diag_df)

    time_df = _time_composite_parity(
        grid=grid,
        tools=tools,
        sea_mask_bool=sea_mask_bool,
        od_member_paths=od_member_paths,
        od_control_path=od_control_path,
        py_nc_path=py_nc_path,
    )
    physics_df = _physics_config_parity(context, od_control_path, py_nc_path)

    threshold_class = np.zeros_like(od_member_ocean_prob, dtype=np.float32)
    threshold_class[od_member_ocean_prob >= 0.10] = 1
    threshold_class[od_member_ocean_prob >= 0.25] = 2
    threshold_class[od_member_ocean_prob >= 0.50] = 3
    threshold_class[od_member_ocean_prob >= 0.90] = 4

    dropped_map_path = _write_dropped_map(seed, target, od_member_raw_any, od_member_ocean_any, od_dropped, sea_mask_bool.astype(np.float32))
    board_path = _make_board(
        seed=seed,
        target=target,
        sea_mask=sea_mask_bool.astype(np.float32),
        od_raw_any=od_member_raw_any,
        od_dropped=od_dropped,
        od_retained=od_member_ocean_any,
        od_prob=od_member_ocean_prob,
        od_threshold_class=threshold_class,
        py_raw=py_raw,
        py_masked=py_ocean,
    )
    note_path = _write_note(
        product_df=product_df,
        py_masking=py_masking_df,
        dropped_df=dropped_df,
        sensitivity=fss_df,
        time_parity=time_df,
        board_path=board_path,
        dropped_map_path=dropped_map_path,
    )

    manifest = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "diagnostic_only": True,
        "thesis_facing": False,
        "full_opendrift_ensemble_rerun_performed": False,
        "experiment_dir": str(BASE),
        "output_dir": str(OUT_DIR),
        "board": str(board_path),
        "dropped_cell_map": str(dropped_map_path),
        "note": str(note_path),
        "key_counts": {
            "opendrift_r1_raw_member_support_before_mask": int(np.count_nonzero(od_member_raw_any > 0)),
            "opendrift_r1_valid_ocean_member_support": int(np.count_nonzero(od_member_ocean_any > 0)),
            "opendrift_r1_dropped_by_official_ocean_mask": int(np.count_nonzero(od_dropped > 0)),
            "opendrift_r1_p10": int(np.count_nonzero(od_member_ocean_prob >= 0.10)),
            "opendrift_r1_p25": int(np.count_nonzero(od_member_ocean_prob >= 0.25)),
            "opendrift_r1_p50": int(np.count_nonzero(od_member_ocean_prob >= 0.50)),
            "opendrift_r1_p90": int(np.count_nonzero(od_member_ocean_prob >= 0.90)),
            "opendrift_r1_control_after_mask": int(np.count_nonzero(od_control_ocean > 0)),
            "pygnome_raw_before_mask": int(np.count_nonzero(py_raw > 0)),
            "pygnome_after_same_ocean_mask": int(np.count_nonzero(py_ocean > 0)),
            "march4_observed_cells": int(np.count_nonzero(target > 0)),
        },
        "artifacts": {
            "product_definition_parity": str(OUT_DIR / "product_definition_parity.csv"),
            "pygnome_masking_check": str(OUT_DIR / "pygnome_masking_check.csv"),
            "opendrift_dropped_cells": str(OUT_DIR / "opendrift_r1_dropped_cells_audit.csv"),
            "coastal_mask_sensitivity_fss": str(OUT_DIR / "coastal_mask_sensitivity_fss.csv"),
            "coastal_mask_sensitivity_diagnostics": str(OUT_DIR / "coastal_mask_sensitivity_diagnostics.csv"),
            "time_composite_parity": str(OUT_DIR / "time_composite_parity.csv"),
            "physics_config_parity": str(OUT_DIR / "physics_config_parity.csv"),
        },
    }
    _write_json(OUT_DIR / "march3_4_philsa_5000_apples_to_apples_manifest.json", manifest)
    print(json.dumps(manifest, indent=2))
    return manifest


if __name__ == "__main__":
    run()
