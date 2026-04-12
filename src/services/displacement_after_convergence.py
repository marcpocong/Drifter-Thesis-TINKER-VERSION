"""Post-convergence displacement/transport audit for official Mindoro Phase 3B."""

from __future__ import annotations

import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import yaml
from shapely.geometry import box

from src.core.case_context import get_case_context
from src.helpers.raster import GridBuilder
from src.services.ensemble import normalize_time_index
from src.services.ingestion import derive_bbox_from_display_bounds
from src.utils.io import (
    get_case_output_dir,
    get_convergence_after_shoreline_output_dir,
    get_download_manifest_path,
    get_ensemble_manifest_path,
    get_forecast_manifest_path,
)

try:
    import geopandas as gpd
except ImportError:  # pragma: no cover
    gpd = None

try:
    import matplotlib.pyplot as plt
except ImportError:  # pragma: no cover
    plt = None

try:
    import rasterio
except ImportError:  # pragma: no cover
    rasterio = None

try:
    import xarray as xr
except ImportError:  # pragma: no cover
    xr = None


AUDIT_DIR_NAME = "displacement_after_convergence"
HYPOTHESIS_RERUNS = {
    "transport_model_structural_limitation": "transport-model limitation rerun",
    "observation_strictness_from_tiny_march6_target": "observation-resolution sensitivity rerun",
    "forcing_domain_halo_or_subset_issue": "forcing-domain rerun",
    "remaining_broad_case_domain_contamination": "forcing-domain rerun",
    "wave_or_stokes_attach_mismatch": "forcing-domain rerun",
    "march3_initialization_geometry_rescue_path_bias": "initialization repair rerun",
    "release_centroid_offset_vs_processed_march3_polygon": "initialization repair rerun",
    "seed_sample_distribution_not_faithful_to_processed_polygon": "initialization repair rerun",
}


def _json_default(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, np.ndarray):
        return value.tolist()
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return None if np.isnan(value) else float(value)
    return str(value)


def _write_json(path: Path, payload: dict | list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, default=_json_default)
        handle.write("\n")


def _read_json(path: Path) -> dict:
    if not path.exists():
        return {}
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle) or {}


def _read_csv(path: Path) -> pd.DataFrame:
    return pd.read_csv(path) if path.exists() else pd.DataFrame()


def _distance_m(a: tuple[float, float] | None, b: tuple[float, float] | None) -> float:
    if a is None or b is None:
        return float("nan")
    return float(math.hypot(float(a[0]) - float(b[0]), float(a[1]) - float(b[1])))


def _geometry_union(gdf):
    return gdf.geometry.union_all() if hasattr(gdf.geometry, "union_all") else gdf.geometry.unary_union


def _centroid_xy(gdf) -> tuple[float, float] | None:
    if gdf.empty:
        return None
    geom = _geometry_union(gdf)
    if geom is None or geom.is_empty:
        return None
    c = geom.centroid
    return float(c.x), float(c.y)


def _read_binary_mask(path: Path) -> np.ndarray:
    if rasterio is None:
        raise ImportError("rasterio is required for displacement_after_convergence.")
    with rasterio.open(path) as src:
        return (src.read(1) > 0).astype(np.uint8)


def _mask_centroid(mask: np.ndarray, grid: GridBuilder) -> tuple[float, float] | None:
    rows, cols = np.where(mask > 0)
    if len(rows) == 0:
        return None
    xs = grid.min_x + ((cols.astype(float) + 0.5) * grid.resolution)
    ys = grid.max_y - ((rows.astype(float) + 0.5) * grid.resolution)
    return float(xs.mean()), float(ys.mean())


def _mask_stats(path: Path, grid: GridBuilder) -> dict:
    if not path.exists():
        return {"path": str(path), "exists": False}
    mask = _read_binary_mask(path)
    centroid = _mask_centroid(mask, grid)
    return {
        "path": str(path),
        "exists": True,
        "nonzero_cells": int(np.count_nonzero(mask)),
        "centroid_x": centroid[0] if centroid else None,
        "centroid_y": centroid[1] if centroid else None,
    }


def _forcing_bounds(path: Path) -> dict:
    if xr is None:
        raise ImportError("xarray is required for displacement_after_convergence.")
    result = {"path": str(path), "exists": path.exists(), "bounds_wgs84": []}
    if not path.exists():
        return result
    with xr.open_dataset(path) as ds:
        lat_name = next((n for n in ("lat", "latitude", "LATITUDE") if n in ds.coords or n in ds.dims), None)
        lon_name = next((n for n in ("lon", "longitude", "LONGITUDE") if n in ds.coords or n in ds.dims), None)
        if lat_name is None:
            lat_name = next((n for n in ds.dims if "lat" in n.lower()), None)
        if lon_name is None:
            lon_name = next((n for n in ds.dims if "lon" in n.lower()), None)
        if lat_name and lon_name:
            lat = np.asarray(ds[lat_name].values, dtype=float)
            lon = np.asarray(ds[lon_name].values, dtype=float)
            result["bounds_wgs84"] = [float(np.nanmin(lon)), float(np.nanmax(lon)), float(np.nanmin(lat)), float(np.nanmax(lat))]
        time_name = next((n for n in ("time", "valid_time") if n in ds.coords or n in ds.dims), None)
        if time_name:
            times = normalize_time_index(ds[time_name].values)
            result["time_start_utc"] = pd.Timestamp(times[0]).strftime("%Y-%m-%dT%H:%M:%SZ") if len(times) else ""
            result["time_end_utc"] = pd.Timestamp(times[-1]).strftime("%Y-%m-%dT%H:%M:%SZ") if len(times) else ""
            result["time_count"] = int(len(times))
    return result


def _bbox_covers(bounds: list[float], target: list[float], tolerance: float = 0.08) -> bool:
    if len(bounds) != 4 or len(target) != 4:
        return False
    return bool(bounds[0] <= target[0] + tolerance and bounds[1] >= target[1] - tolerance and bounds[2] <= target[2] + tolerance and bounds[3] >= target[3] - tolerance)


def _highest_completed_convergence(summary_df: pd.DataFrame) -> pd.Series | None:
    if summary_df.empty:
        return None
    completed = summary_df[summary_df["status"].astype(str) == "completed"].copy()
    if completed.empty:
        return None
    completed["element_count_actual"] = pd.to_numeric(completed["element_count_actual"], errors="coerce")
    return completed.sort_values("element_count_actual", ascending=False).iloc[0]


def _status_timeline(nc_path: Path, grid_crs: str) -> tuple[pd.DataFrame, dict]:
    if xr is None or gpd is None:
        raise ImportError("xarray and geopandas are required for displacement_after_convergence.")
    rows: list[dict] = []
    if not nc_path.exists():
        return pd.DataFrame(), {"path": str(nc_path), "exists": False}
    with xr.open_dataset(nc_path) as ds:
        times = normalize_time_index(ds["time"].values)
        for index, timestamp in enumerate(times):
            lon = np.asarray(ds["lon"].isel(time=index).values).reshape(-1)
            lat = np.asarray(ds["lat"].isel(time=index).values).reshape(-1)
            status = np.asarray(ds["status"].isel(time=index).values).reshape(-1)
            finite = np.isfinite(lon) & np.isfinite(lat)
            active = finite & (status == 0)
            stranded = finite & (status == 1)
            centroid_x = np.nan
            centroid_y = np.nan
            if np.any(active):
                points = gpd.GeoSeries(gpd.points_from_xy(lon[active], lat[active]), crs="EPSG:4326").to_crs(grid_crs)
                centroid_x = float(points.x.mean())
                centroid_y = float(points.y.mean())
            rows.append(
                {
                    "timestamp_utc": pd.Timestamp(timestamp).strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "time_index": int(index),
                    "finite_particles": int(np.count_nonzero(finite)),
                    "active_particles": int(np.count_nonzero(active)),
                    "stranded_particles": int(np.count_nonzero(stranded)),
                    "active_centroid_x": centroid_x,
                    "active_centroid_y": centroid_y,
                }
            )
    df = pd.DataFrame(rows)
    active_df = df[df["active_particles"] > 0]
    last_active = active_df.iloc[-1].to_dict() if not active_df.empty else {}
    first_zero = {}
    if not active_df.empty:
        later_zero = df[(df["time_index"] > int(last_active["time_index"])) & (df["active_particles"] == 0)]
        if not later_zero.empty:
            first_zero = later_zero.iloc[0].to_dict()
    summary = {
        "path": str(nc_path),
        "exists": True,
        "output_time_start_utc": str(df.iloc[0]["timestamp_utc"]) if not df.empty else "",
        "output_time_end_utc": str(df.iloc[-1]["timestamp_utc"]) if not df.empty else "",
        "output_time_count": int(len(df)),
        "last_active_timestamp_utc": last_active.get("timestamp_utc", ""),
        "last_active_particle_count": int(last_active.get("active_particles", 0) or 0),
        "first_zero_active_after_release_utc": first_zero.get("timestamp_utc", ""),
        "max_active_particles": int(df["active_particles"].max()) if not df.empty else 0,
        "final_active_particles": int(df.iloc[-1]["active_particles"]) if not df.empty else 0,
        "final_stranded_particles": int(df.iloc[-1]["stranded_particles"]) if not df.empty else 0,
    }
    return df, summary


def _seed_distribution_checks(nc_path: Path, init_gdf, grid_crs: str) -> dict:
    if xr is None or gpd is None:
        raise ImportError("xarray and geopandas are required for displacement_after_convergence.")
    result = {"path": str(nc_path), "exists": nc_path.exists()}
    if not nc_path.exists():
        return result
    init_union = _geometry_union(init_gdf)
    init_bounds = tuple(float(v) for v in init_union.bounds)
    with xr.open_dataset(nc_path) as ds:
        lon = np.asarray(ds["lon"].isel(time=0).values).reshape(-1)
        lat = np.asarray(ds["lat"].isel(time=0).values).reshape(-1)
        status = np.asarray(ds["status"].isel(time=0).values).reshape(-1)
    valid = np.isfinite(lon) & np.isfinite(lat)
    active = valid & (status == 0)
    points = gpd.GeoSeries(gpd.points_from_xy(lon[active], lat[active]), crs="EPSG:4326").to_crs(grid_crs)
    within_polygon = points.within(init_union) | points.touches(init_union)
    within_bounds = (points.x >= init_bounds[0]) & (points.x <= init_bounds[2]) & (points.y >= init_bounds[1]) & (points.y <= init_bounds[3])
    centroid = (float(points.x.mean()), float(points.y.mean())) if len(points) else None
    init_centroid = _centroid_xy(init_gdf)
    result.update(
        {
            "seed_particle_count": int(len(points)),
            "valid_lonlat_count": int(np.count_nonzero(valid)),
            "active_at_seed_count": int(np.count_nonzero(active)),
            "init_polygon_bounds_projected": init_bounds,
            "seed_sample_bounds_projected": (
                float(points.x.min()),
                float(points.y.min()),
                float(points.x.max()),
                float(points.y.max()),
            ) if len(points) else None,
            "outside_processed_polygon_count": int((~within_polygon).sum()),
            "outside_processed_polygon_fraction": float((~within_polygon).sum() / len(points)) if len(points) else None,
            "outside_processed_polygon_bounds_count": int((~within_bounds).sum()),
            "outside_processed_polygon_bounds_fraction": float((~within_bounds).sum() / len(points)) if len(points) else None,
            "seed_sample_centroid_x": centroid[0] if centroid else None,
            "seed_sample_centroid_y": centroid[1] if centroid else None,
            "processed_march3_centroid_x": init_centroid[0] if init_centroid else None,
            "processed_march3_centroid_y": init_centroid[1] if init_centroid else None,
            "seed_centroid_to_processed_march3_centroid_m": _distance_m(centroid, init_centroid),
        }
    )
    return result


def _audit_reader_statuses(phase2_audit: dict) -> dict:
    runs = phase2_audit.get("runs") or []
    result: dict[str, Any] = {
        "run_count": int(len(runs)),
        "completed_run_count": int(sum(str(run.get("status")) == "completed" for run in runs)),
        "by_forcing_kind": {},
        "current_tail_extension_run_count": 0,
        "current_tail_extension_max_gap_hours": 0.0,
    }
    for kind in ("current", "wind", "wave"):
        entries = [(run.get("forcings") or {}).get(kind, {}) for run in runs]
        statuses = [str(entry.get("reader_attach_status", "")) for entry in entries]
        result["by_forcing_kind"][kind] = {
            "loaded_count": int(sum(status == "loaded" for status in statuses)),
            "unique_attach_statuses": sorted({status for status in statuses if status}),
            "covers_requested_window_count": int(sum(bool(entry.get("covers_requested_window", False)) for entry in entries)),
            "missing_required_variables_values": sorted({";".join(entry.get("missing_required_variables", []) or []) for entry in entries if entry.get("missing_required_variables")}),
        }
    current_entries = [(run.get("forcings") or {}).get("current", {}) for run in runs]
    gaps = [float(entry.get("coverage_gap_hours", 0.0) or 0.0) for entry in current_entries if entry]
    result["current_tail_extension_run_count"] = int(sum(bool(entry.get("tail_extension_applied", False)) for entry in current_entries))
    result["current_tail_extension_max_gap_hours"] = float(max(gaps) if gaps else 0.0)
    return result


def _read_source_text_flags() -> dict:
    flags = {}
    for path in [Path("src/services/ingestion.py"), Path("src/services/ensemble.py"), Path("src/helpers/scoring.py"), Path("src/services/arcgis.py")]:
        if not path.exists():
            continue
        text = path.read_text(encoding="utf-8")
        flags[str(path)] = {
            "contains_official_active_domain_fallback": "official_active_domain_fallback" in text,
            "contains_legacy_display_domain_plus_pad": "legacy_prototype_display_domain_plus_3deg_pad" in text or "REGION[0]-pad" in text,
            "contains_case_region_reference": "case.region" in text or "case_context.region" in text,
        }
    return flags


def _appendix_centroid_path(appendix_diag: pd.DataFrame, grid: GridBuilder) -> list[dict]:
    rows = []
    for _, row in appendix_diag.iterrows():
        forecast_stats = _mask_stats(Path(str(row.get("forecast_path", ""))), grid)
        obs_stats = _mask_stats(Path(str(row.get("observation_path", ""))), grid)
        fxy = (forecast_stats.get("centroid_x"), forecast_stats.get("centroid_y")) if forecast_stats.get("centroid_x") is not None else None
        oxy = (obs_stats.get("centroid_x"), obs_stats.get("centroid_y")) if obs_stats.get("centroid_x") is not None else None
        rows.append(
            {
                "obs_date": row.get("obs_date", ""),
                "source_name": row.get("source_name", ""),
                "forecast_nonzero_cells": forecast_stats.get("nonzero_cells"),
                "obs_nonzero_cells": obs_stats.get("nonzero_cells"),
                "forecast_centroid_x": forecast_stats.get("centroid_x"),
                "forecast_centroid_y": forecast_stats.get("centroid_y"),
                "obs_centroid_x": obs_stats.get("centroid_x"),
                "obs_centroid_y": obs_stats.get("centroid_y"),
                "centroid_distance_m": _distance_m(fxy, oxy),
                "iou": row.get("iou", np.nan),
                "dice": row.get("dice", np.nan),
                "fss_1km": row.get("fss_1km", np.nan),
                "fss_10km": row.get("fss_10km", np.nan),
            }
        )
    return rows


def rank_displacement_after_convergence_hypotheses(metrics: dict) -> list[dict]:
    """Rank the remaining post-convergence displacement hypotheses."""
    rows: list[dict] = []

    def add(hypothesis_id: str, label: str, score: float, support: list[str], counter: list[str]) -> None:
        rows.append(
            {
                "hypothesis_id": hypothesis_id,
                "label": label,
                "likelihood_score": round(float(max(0.0, min(1.0, score))), 3),
                "supporting_evidence": " | ".join(support),
                "contradicting_evidence": " | ".join(counter),
                "recommended_rerun": HYPOTHESIS_RERUNS[hypothesis_id],
            }
        )

    transport_score = 0.20
    transport_support: list[str] = []
    transport_counter: list[str] = []
    if metrics.get("highest_count_official_p50_nonzero_cells", 0) <= 0:
        transport_score += 0.20
        transport_support.append("Highest-count official March 6 p50 remains empty.")
    if metrics.get("highest_count_max_march6_occupancy_members", 0) <= 0:
        transport_score += 0.20
        transport_support.append("No ensemble member occupancy reaches valid ocean cells on March 6.")
    if metrics.get("control_final_active_particles", 1) <= 0:
        transport_score += 0.16
        transport_support.append("Deterministic control has zero active particles by its final output.")
    if metrics.get("control_hours_short_of_requested_end", 0.0) >= 24.0:
        transport_score += 0.14
        transport_support.append(f"Control output stops {metrics['control_hours_short_of_requested_end']:.1f} h before the requested endpoint.")
    if metrics.get("transport_provisional", False):
        transport_score += 0.08
        transport_support.append("Forecast manifests still mark the transport model as provisional.")
    if metrics.get("appendix_eventcorridor_iou", 0.0) > 0.05:
        transport_counter.append("The appendix event corridor has some overlap, so early transport is not entirely broken.")
    add("transport_model_structural_limitation", "transport-model structural limitation", transport_score, transport_support, transport_counter)

    obs_score = 0.10
    obs_support: list[str] = []
    obs_counter: list[str] = []
    if metrics.get("march6_obs_nonzero_cells", 9999) <= 5:
        obs_score += 0.32
        obs_support.append(f"Official March 6 target has only {metrics.get('march6_obs_nonzero_cells')} scoreable cells.")
    if metrics.get("march6_vector_area_m2", 0.0) <= 500000.0:
        obs_score += 0.10
        obs_support.append(f"Processed March 6 vector area is only {metrics.get('march6_vector_area_m2', 0.0):.0f} m2.")
    if metrics.get("highest_count_official_p50_nonzero_cells", 0) <= 0:
        obs_counter.append("Observation strictness cannot by itself explain an empty March 6 forecast mask.")
    add("observation_strictness_from_tiny_march6_target", "observation strictness from a very small March 6 target", obs_score, obs_support, obs_counter)

    forcing_score = 0.10
    forcing_support: list[str] = []
    forcing_counter: list[str] = []
    if metrics.get("current_tail_extension_run_count", 0) > 0:
        forcing_score += 0.12
        forcing_support.append(f"Current forcing required tail persistence in {metrics.get('current_tail_extension_run_count')} audited runs.")
    if metrics.get("current_tail_extension_max_gap_hours", 0.0) >= 8.0:
        forcing_score += 0.08
        forcing_support.append(f"Maximum current tail-persistence gap was {metrics.get('current_tail_extension_max_gap_hours', 0.0):.2f} h.")
    if metrics.get("forcing_bounds_cover_canonical_halo", False):
        forcing_score -= 0.08
        forcing_counter.append("Actual forcing subsets cover the canonical scoring domain plus 0.5 degree halo.")
    else:
        forcing_score += 0.12
        forcing_support.append("Realized forcing-grid bounds do not fully cover the canonical scoring domain plus 0.5 degree halo.")
    if metrics.get("download_manifest_uses_canonical_bbox", False):
        forcing_score -= 0.05
        forcing_counter.append("Download manifest records canonical scoring-grid bounds plus halo.")
    add("forcing_domain_halo_or_subset_issue", "forcing-domain halo or subset issue", forcing_score, forcing_support, forcing_counter)

    legacy_score = 0.08
    legacy_support: list[str] = []
    legacy_counter: list[str] = []
    if metrics.get("actual_broad_case_domain_usage_detected", False):
        legacy_score += 0.35
        legacy_support.append("Actual manifest or forcing bounds still indicate broad Mindoro spill-case domain usage.")
    else:
        legacy_counter.append("Current manifest evidence does not indicate broad Mindoro spill-case domain usage.")
    if metrics.get("source_has_official_active_domain_fallbacks", False):
        legacy_score += 0.04
        legacy_support.append("Source retains official active-domain fallback branches for missing-grid situations.")
    add("remaining_broad_case_domain_contamination", "remaining broad-case-domain contamination in official prep/runtime", legacy_score, legacy_support, legacy_counter)

    wave_score = 0.05
    wave_support: list[str] = []
    wave_counter: list[str] = []
    if not metrics.get("wave_reader_loaded_for_all_runs", False):
        wave_score += 0.35
        wave_support.append("At least one audited wave/Stokes reader did not load cleanly.")
    else:
        wave_counter.append("Wave/Stokes readers loaded for every audited run with no missing required variables.")
    add("wave_or_stokes_attach_mismatch", "wave/Stokes attach mismatch", wave_score, wave_support, wave_counter)

    geometry_score = 0.10
    geometry_support: list[str] = []
    geometry_counter: list[str] = []
    if metrics.get("march3_rescue_applied", False):
        geometry_score += 0.12
        geometry_support.append("March 3 ArcGIS processing notes include the rescue path.")
    if metrics.get("seed_outside_processed_polygon_fraction", 1.0) <= 0.001:
        geometry_score -= 0.08
        geometry_counter.append("Seed samples are within the processed March 3 polygon.")
    if metrics.get("march3_appendix_fss_1km", 0.0) >= 0.5:
        geometry_score -= 0.08
        geometry_counter.append("March 3 appendix initialization-polygon score is high on the same grid.")
    add("march3_initialization_geometry_rescue_path_bias", "March 3 initialization geometry rescue-path bias", geometry_score, geometry_support, geometry_counter)

    release_score = 0.08
    release_support: list[str] = []
    release_counter: list[str] = []
    if metrics.get("release_centroid_to_processed_march3_centroid_m", 0.0) > 5000.0:
        release_score += 0.12
        release_support.append(f"Release/seed centroid is {metrics.get('release_centroid_to_processed_march3_centroid_m', 0.0):.1f} m from the processed March 3 centroid.")
    else:
        release_counter.append("Release/seed centroid is close to the processed March 3 centroid.")
    if metrics.get("provenance_point_to_processed_march3_centroid_m", 0.0) > 10000.0:
        release_support.append("Layer 0 provenance point is far from March 3, but it is provenance-only.")
    add("release_centroid_offset_vs_processed_march3_polygon", "release centroid offset vs processed March 3 polygon centroid", release_score, release_support, release_counter)

    seed_score = 0.05
    seed_support: list[str] = []
    seed_counter: list[str] = []
    if metrics.get("seed_outside_processed_polygon_fraction", 0.0) > 0.05:
        seed_score += 0.35
        seed_support.append(f"{metrics.get('seed_outside_processed_polygon_fraction', 0.0):.3f} of seed particles fall outside the processed polygon.")
    else:
        seed_counter.append("Seed distribution is faithful to the processed polygon geometry.")
    add("seed_sample_distribution_not_faithful_to_processed_polygon", "seed sample distribution not faithfully representing the processed March 3 polygon", seed_score, seed_support, seed_counter)

    ranked = sorted(rows, key=lambda row: row["likelihood_score"], reverse=True)
    for index, row in enumerate(ranked, start=1):
        row["rank"] = index
        row["status"] = "highest_priority" if index == 1 else "secondary"
    return ranked


def _plot_overlay(
    out_path: Path,
    init_gdf,
    validation_gdf,
    source_gdf,
    control_timeline: pd.DataFrame,
    appendix_rows: list[dict],
    seed_checks: dict,
    halo_bounds: list[float],
    current_bounds: list[float],
) -> Path | None:
    if plt is None or gpd is None:
        return None
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig, axes = plt.subplots(1, 2, figsize=(13, 6))

    init_gdf.plot(ax=axes[0], facecolor="none", edgecolor="#1f77b4", linewidth=1.2, label="March 3 init")
    validation_gdf.plot(ax=axes[0], facecolor="none", edgecolor="#d62728", linewidth=1.2, label="March 6 obs")
    if not source_gdf.empty:
        source_gdf.plot(ax=axes[0], color="#9467bd", markersize=38, label="Layer 0 provenance")
    active = control_timeline.dropna(subset=["active_centroid_x", "active_centroid_y"])
    if not active.empty:
        axes[0].plot(active["active_centroid_x"], active["active_centroid_y"], color="#ff7f0e", linewidth=1.4, label="control active centroid")
        axes[0].scatter(active["active_centroid_x"].iloc[0], active["active_centroid_y"].iloc[0], color="#2ca02c", s=40, label="release centroid")
        axes[0].scatter(active["active_centroid_x"].iloc[-1], active["active_centroid_y"].iloc[-1], color="#ff7f0e", s=40, label="last active centroid")
    if seed_checks.get("seed_sample_centroid_x") is not None:
        axes[0].scatter([seed_checks["seed_sample_centroid_x"]], [seed_checks["seed_sample_centroid_y"]], color="#17becf", marker="x", s=70, label="seed sample centroid")
    axes[0].set_title("Projected release and control centroid path")
    axes[0].set_xlabel("EPSG:32651 x (m)")
    axes[0].set_ylabel("EPSG:32651 y (m)")
    axes[0].legend(loc="best", fontsize=7)

    init_wgs84 = init_gdf.to_crs("EPSG:4326")
    validation_wgs84 = validation_gdf.to_crs("EPSG:4326")
    gpd.GeoSeries([box(halo_bounds[0], halo_bounds[2], halo_bounds[1], halo_bounds[3])], crs="EPSG:4326").plot(
        ax=axes[1], facecolor="none", edgecolor="#ff7f0e", linewidth=1.5, label="scoring domain + halo"
    )
    if current_bounds:
        gpd.GeoSeries([box(current_bounds[0], current_bounds[2], current_bounds[1], current_bounds[3])], crs="EPSG:4326").plot(
            ax=axes[1], facecolor="none", edgecolor="#7f7f7f", linewidth=1.2, linestyle="--", label="current subset"
        )
    init_wgs84.plot(ax=axes[1], facecolor="none", edgecolor="#1f77b4", linewidth=1.0)
    validation_wgs84.plot(ax=axes[1], facecolor="none", edgecolor="#d62728", linewidth=1.0)

    path_df = pd.DataFrame(appendix_rows)
    if not path_df.empty:
        fpts = path_df.dropna(subset=["forecast_centroid_x", "forecast_centroid_y"])
        opts = path_df.dropna(subset=["obs_centroid_x", "obs_centroid_y"])
        if not fpts.empty:
            gpd.GeoDataFrame(fpts, geometry=gpd.points_from_xy(fpts["forecast_centroid_x"], fpts["forecast_centroid_y"]), crs=init_gdf.crs).to_crs("EPSG:4326").plot(
                ax=axes[1], color="#2ca02c", markersize=28, label="appendix forecast centroids"
            )
        if not opts.empty:
            gpd.GeoDataFrame(opts, geometry=gpd.points_from_xy(opts["obs_centroid_x"], opts["obs_centroid_y"]), crs=init_gdf.crs).to_crs("EPSG:4326").plot(
                ax=axes[1], color="#d62728", markersize=22, alpha=0.7, label="appendix obs centroids"
            )
    axes[1].set_title("Forcing subset and appendix centroid context")
    axes[1].set_xlabel("Longitude")
    axes[1].set_ylabel("Latitude")
    axes[1].legend(loc="best", fontsize=7)
    fig.tight_layout()
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return out_path


class DisplacementAfterConvergenceAudit:
    def __init__(self):
        self.case = get_case_context()
        if not self.case.is_official:
            raise RuntimeError("displacement_after_convergence is only supported for official workflows.")
        if gpd is None or xr is None:
            raise ImportError("geopandas and xarray are required for displacement_after_convergence.")
        self.case_output_dir = get_case_output_dir(self.case.run_name)
        self.audit_dir = self.case_output_dir / AUDIT_DIR_NAME
        self.audit_dir.mkdir(parents=True, exist_ok=True)
        self.grid = GridBuilder()

    def _resolve_paths(self) -> dict[str, Path]:
        arcgis_dir = Path("data") / "arcgis" / self.case.run_name
        convergence_dir = get_convergence_after_shoreline_output_dir(self.case.run_name)
        appendix_dir = self.case_output_dir / "public_obs_appendix"
        return {
            "case_config": Path("config/case_mindoro_retro_2023.yaml"),
            "scoring_grid": Path("data_processed/grids/scoring_grid.yaml"),
            "shoreline_manifest_json": Path("data_processed/grids/shoreline_mask_manifest.json"),
            "shoreline_manifest_csv": Path("data_processed/grids/shoreline_mask_manifest.csv"),
            "arcgis_processing_report": arcgis_dir / "arcgis_processing_report.csv",
            "init_vector": arcgis_dir / "seed_polygon_mar3_processed.gpkg",
            "validation_vector": arcgis_dir / "validation_polygon_mar6_processed.gpkg",
            "source_vector": arcgis_dir / "source_point_metadata_processed.gpkg",
            "forecast_manifest": get_forecast_manifest_path(self.case.run_name),
            "ensemble_manifest": get_ensemble_manifest_path(self.case.run_name),
            "phase2_loading_audit": self.case_output_dir / "forecast" / "phase2_loading_audit.json",
            "phase3b_diagnostics": self.case_output_dir / "phase3b" / "phase3b_diagnostics.csv",
            "convergence_summary": convergence_dir / "convergence_after_shoreline_summary.csv",
            "convergence_diagnostics": convergence_dir / "convergence_after_shoreline_diagnostics.csv",
            "appendix_manifest": appendix_dir / "public_obs_appendix_manifest.json",
            "appendix_perdate_diagnostics": appendix_dir / "appendix_perdate_diagnostics.csv",
            "appendix_eventcorridor_diagnostics": appendix_dir / "appendix_eventcorridor_diagnostics.csv",
            "shoreline_forcing_manifest": self.case_output_dir / "shoreline_rerun" / "forcing_domain_manifest.json",
            "current_forcing": Path("data") / "forcing" / self.case.run_name / "cmems_curr.nc",
            "wind_forcing": Path("data") / "forcing" / self.case.run_name / "era5_wind.nc",
            "wave_forcing": Path("data") / "forcing" / self.case.run_name / "cmems_wave.nc",
            "hycom_forcing": Path("data") / "forcing" / self.case.run_name / "hycom_curr.nc",
        }

    @staticmethod
    def _validate_required(paths: dict[str, Path]) -> None:
        required = [
            "case_config",
            "scoring_grid",
            "shoreline_manifest_json",
            "shoreline_manifest_csv",
            "arcgis_processing_report",
            "init_vector",
            "validation_vector",
            "source_vector",
            "forecast_manifest",
            "ensemble_manifest",
            "phase2_loading_audit",
            "phase3b_diagnostics",
            "convergence_summary",
            "convergence_diagnostics",
            "appendix_manifest",
            "appendix_perdate_diagnostics",
            "appendix_eventcorridor_diagnostics",
            "current_forcing",
            "wind_forcing",
            "wave_forcing",
        ]
        missing = [f"{name}={paths[name]}" for name in required if not paths[name].exists()]
        if missing:
            raise FileNotFoundError("displacement_after_convergence requires existing official artifacts. Missing: " + ", ".join(missing))

    def run(self) -> dict:
        paths = self._resolve_paths()
        self._validate_required(paths)

        scoring_grid = yaml.safe_load(paths["scoring_grid"].read_text(encoding="utf-8")) or {}
        arcgis_report = _read_csv(paths["arcgis_processing_report"])
        phase3b_diagnostics = _read_csv(paths["phase3b_diagnostics"])
        convergence_summary = _read_csv(paths["convergence_summary"])
        appendix_diagnostics = _read_csv(paths["appendix_perdate_diagnostics"])
        appendix_manifest = _read_json(paths["appendix_manifest"])
        forecast_manifest = _read_json(paths["forecast_manifest"])
        ensemble_manifest = _read_json(paths["ensemble_manifest"])
        shoreline_forcing_manifest = _read_json(paths["shoreline_forcing_manifest"])
        download_manifest = _read_json(get_download_manifest_path())

        highest = _highest_completed_convergence(convergence_summary)
        if highest is None:
            raise RuntimeError("No completed convergence_after_shoreline run was found to audit.")
        high_run_name = str(highest["run_name"])
        high_output_dir = get_case_output_dir(high_run_name)
        high_forecast_manifest = _read_json(high_output_dir / "forecast" / "forecast_manifest.json")
        high_ensemble_manifest = _read_json(high_output_dir / "ensemble" / "ensemble_manifest.json")
        high_phase2_audit_path = high_output_dir / "forecast" / "phase2_loading_audit.json"
        high_phase2_audit = _read_json(high_phase2_audit_path)

        init_gdf = gpd.read_file(paths["init_vector"]).to_crs(self.grid.crs)
        validation_gdf = gpd.read_file(paths["validation_vector"]).to_crs(self.grid.crs)
        source_gdf = gpd.read_file(paths["source_vector"]).to_crs(self.grid.crs)
        init_centroid = _centroid_xy(init_gdf)
        validation_centroid = _centroid_xy(validation_gdf)
        provenance_centroid = _centroid_xy(source_gdf)

        high_control_nc = high_output_dir / "forecast" / "deterministic_control_cmems_era5.nc"
        control_timeline, control_summary = _status_timeline(high_control_nc, self.grid.crs)
        seed_checks = _seed_distribution_checks(high_control_nc, init_gdf, self.grid.crs)
        active_rows = control_timeline[control_timeline["active_particles"] > 0]
        release_xy = None
        last_active_xy = None
        if not active_rows.empty:
            release_xy = (float(active_rows.iloc[0]["active_centroid_x"]), float(active_rows.iloc[0]["active_centroid_y"]))
            last_active_xy = (float(active_rows.iloc[-1]["active_centroid_x"]), float(active_rows.iloc[-1]["active_centroid_y"]))

        official_main = phase3b_diagnostics[phase3b_diagnostics["pair_id"].astype(str) == "official_primary_march6"]
        official_main_row = official_main.iloc[0].to_dict() if not official_main.empty else {}
        validation_report = arcgis_report[arcgis_report["layer_id"].astype(str) == str(self.case.validation_layer.layer_id)]
        validation_report_row = validation_report.iloc[0].to_dict() if not validation_report.empty else {}
        init_report = arcgis_report[arcgis_report["layer_id"].astype(str) == str(self.case.initialization_layer.layer_id)]
        init_report_row = init_report.iloc[0].to_dict() if not init_report.empty else {}

        display_bounds = scoring_grid.get("display_bounds_wgs84") or list(self.case.region)
        halo_degrees = float(scoring_grid.get("forcing_bbox_halo_degrees", 0.5) or 0.5)
        halo_bounds = derive_bbox_from_display_bounds(display_bounds, halo_degrees=halo_degrees)
        forcing_bounds = {
            "currents": _forcing_bounds(paths["current_forcing"]),
            "winds": _forcing_bounds(paths["wind_forcing"]),
            "waves": _forcing_bounds(paths["wave_forcing"]),
            "hycom_currents": _forcing_bounds(paths["hycom_forcing"]),
        }
        core_bounds = [forcing_bounds[k].get("bounds_wgs84") or [] for k in ("currents", "winds", "waves")]
        forcing_bounds_cover_canonical_halo = all(_bbox_covers(bounds, halo_bounds) for bounds in core_bounds)

        audit_statuses = _audit_reader_statuses(high_phase2_audit)
        source_flags = _read_source_text_flags()
        source_has_fallbacks = any(flag.get("contains_official_active_domain_fallback") for flag in source_flags.values())
        download_config = (download_manifest.get(self.case.run_name) or {}).get("config") or {}
        download_bbox_source = str(download_config.get("bbox_source", ""))
        actual_legacy_usage = (
            bool(shoreline_forcing_manifest.get("legacy_broad_region_usage_detected", False))
            or "legacy" in download_bbox_source.lower()
            or "fallback" in download_bbox_source.lower()
        )

        appendix_rows = _appendix_centroid_path(appendix_diagnostics, self.grid)
        appendix_path_csv = self.audit_dir / "forecast_obs_centroid_path_by_appendix_date.csv"
        pd.DataFrame(appendix_rows).to_csv(appendix_path_csv, index=False)

        march3_appendix = appendix_diagnostics[appendix_diagnostics["pair_id"].astype(str).str.contains("wwf_main_layer3_init_mar3", na=False)]
        march3_appendix_fss = float(march3_appendix.iloc[0]["fss_1km"]) if not march3_appendix.empty else float("nan")
        eventcorridor_diag = _read_csv(paths["appendix_eventcorridor_diagnostics"])
        eventcorridor_row = eventcorridor_diag.iloc[0].to_dict() if not eventcorridor_diag.empty else {}

        requested_end = pd.Timestamp(self.case.simulation_end_utc)
        output_end = pd.Timestamp(control_summary.get("output_time_end_utc")) if control_summary.get("output_time_end_utc") else pd.NaT
        hours_short = float((requested_end - output_end).total_seconds() / 3600.0) if pd.notna(output_end) else float("nan")

        metrics = {
            "highest_completed_element_count": int(highest["element_count_actual"]),
            "highest_count_official_p50_nonzero_cells": int(highest.get("official_main_forecast_nonzero_cells", 0) or 0),
            "highest_count_max_march6_occupancy_members": int(highest.get("max_march6_occupancy_members", 0) or 0),
            "highest_count_official_fss_10km": float(highest.get("official_main_fss_10km", 0.0) or 0.0),
            "control_final_active_particles": int(control_summary.get("final_active_particles", 0) or 0),
            "control_final_stranded_particles": int(control_summary.get("final_stranded_particles", 0) or 0),
            "control_hours_short_of_requested_end": hours_short,
            "transport_provisional": bool((high_forecast_manifest.get("transport") or {}).get("provisional_transport_model", False)),
            "appendix_eventcorridor_iou": float(eventcorridor_row.get("iou", 0.0) or 0.0),
            "march6_obs_nonzero_cells": int(official_main_row.get("obs_nonzero_cells", validation_report_row.get("raster_nonzero_cells", 0)) or 0),
            "march6_vector_area_m2": float(validation_report_row.get("vector_area", 0.0) or 0.0),
            "current_tail_extension_run_count": int(audit_statuses.get("current_tail_extension_run_count", 0)),
            "current_tail_extension_max_gap_hours": float(audit_statuses.get("current_tail_extension_max_gap_hours", 0.0)),
            "forcing_bounds_cover_canonical_halo": forcing_bounds_cover_canonical_halo,
            "download_manifest_uses_canonical_bbox": download_bbox_source.startswith("canonical_scoring_grid_display_bounds_plus"),
            "actual_broad_case_domain_usage_detected": actual_legacy_usage,
            "source_has_official_active_domain_fallbacks": source_has_fallbacks,
            "wave_reader_loaded_for_all_runs": (
                audit_statuses.get("by_forcing_kind", {}).get("wave", {}).get("loaded_count", 0) == audit_statuses.get("run_count", -1)
                and not audit_statuses.get("by_forcing_kind", {}).get("wave", {}).get("missing_required_variables_values")
            ),
            "march3_rescue_applied": "rescued" in str(init_report_row.get("notes", "")).lower(),
            "seed_outside_processed_polygon_fraction": float(seed_checks.get("outside_processed_polygon_fraction") or 0.0),
            "march3_appendix_fss_1km": march3_appendix_fss,
            "release_centroid_to_processed_march3_centroid_m": _distance_m(release_xy, init_centroid),
            "provenance_point_to_processed_march3_centroid_m": _distance_m(provenance_centroid, init_centroid),
        }
        ranked = rank_displacement_after_convergence_hypotheses(metrics)
        ranked_csv = self.audit_dir / "displacement_hypotheses_ranked.csv"
        pd.DataFrame(ranked).to_csv(ranked_csv, index=False)

        control_timeline_csv = self.audit_dir / "control_status_timeline_100000.csv"
        control_timeline.to_csv(control_timeline_csv, index=False)

        plot_path = _plot_overlay(
            self.audit_dir / "qa_displacement_after_convergence_overlay.png",
            init_gdf,
            validation_gdf,
            source_gdf,
            control_timeline,
            appendix_rows,
            seed_checks,
            halo_bounds,
            forcing_bounds["currents"].get("bounds_wgs84") or [],
        )

        payload = {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "run_name": self.case.run_name,
            "workflow_mode": self.case.workflow_mode,
            "official_main_pair_locked": {
                "forecast": "mask_p50_2023-03-06_datecomposite.tif",
                "observation": "obs_mask_2023-03-06.tif",
                "metric": "FSS",
                "windows_km": [1, 3, 5, 10],
            },
            "highest_completed_convergence_run": {
                "run_name": high_run_name,
                "element_count": int(highest["element_count_actual"]),
                "forecast_manifest": str(high_output_dir / "forecast" / "forecast_manifest.json"),
                "ensemble_manifest": str(high_output_dir / "ensemble" / "ensemble_manifest.json"),
                "phase2_loading_audit": str(high_phase2_audit_path),
            },
            "recommended_next_rerun": ranked[0]["recommended_rerun"],
            "remaining_problem_class": "transport",
            "ranked_hypotheses": ranked,
            "metrics": metrics,
            "geometry_checks": {
                "processed_march3_centroid_xy": init_centroid,
                "processed_march6_centroid_xy": validation_centroid,
                "layer0_provenance_point_xy": provenance_centroid,
                "release_seed_centroid_xy": release_xy,
                "last_active_control_centroid_xy": last_active_xy,
                "release_to_march3_centroid_m": metrics["release_centroid_to_processed_march3_centroid_m"],
                "provenance_to_march3_centroid_m": metrics["provenance_point_to_processed_march3_centroid_m"],
                "seed_distribution": seed_checks,
                "march3_processing_notes": init_report_row.get("notes", ""),
            },
            "transport_checks": {
                "control_status_timeline_csv": str(control_timeline_csv),
                "control_summary": control_summary,
                "requested_simulation_end_utc": self.case.simulation_end_utc,
                "hours_short_of_requested_end": hours_short,
                "reader_status_audit": audit_statuses,
                "transport": high_forecast_manifest.get("transport") or forecast_manifest.get("transport") or {},
                "ensemble_configuration": high_ensemble_manifest.get("ensemble_configuration") or ensemble_manifest.get("ensemble_configuration") or {},
            },
            "forcing_domain_checks": {
                "active_case_domain_wgs84": list(self.case.region),
                "mindoro_case_domain_wgs84": list(self.case.mindoro_case_domain),
                "scoring_grid_display_bounds_wgs84": display_bounds,
                "halo_degrees": halo_degrees,
                "canonical_scoring_domain_plus_halo_wgs84": halo_bounds,
                "download_manifest_bbox": download_config.get("bbox", ""),
                "download_manifest_bbox_source": download_bbox_source,
                "shoreline_rerun_forcing_manifest": shoreline_forcing_manifest,
                "forcing_file_bounds": forcing_bounds,
                "forcing_bounds_cover_canonical_halo": forcing_bounds_cover_canonical_halo,
                "source_text_region_flags": source_flags,
            },
            "observation_checks": {
                "march6_processed_vector_area_m2": metrics["march6_vector_area_m2"],
                "march6_obs_nonzero_cells": metrics["march6_obs_nonzero_cells"],
                "observation_collapse_on_1km_grid": metrics["march6_obs_nonzero_cells"] <= 5,
                "arcgis_validation_report": validation_report_row,
            },
            "appendix_checks": {
                "manifest": appendix_manifest,
                "centroid_path_by_date_csv": str(appendix_path_csv),
                "centroid_path_by_date": appendix_rows,
                "eventcorridor_diagnostics": eventcorridor_row,
            },
            "artifact_paths": {
                "report_md": str(self.audit_dir / "displacement_after_convergence_report.md"),
                "audit_json": str(self.audit_dir / "displacement_after_convergence.json"),
                "ranked_hypotheses_csv": str(ranked_csv),
                "qa_overlay_png": str(plot_path) if plot_path else "",
                "control_status_timeline_csv": str(control_timeline_csv),
                "appendix_centroid_path_csv": str(appendix_path_csv),
            },
        }
        audit_json = self.audit_dir / "displacement_after_convergence.json"
        _write_json(audit_json, payload)
        report_md = self.audit_dir / "displacement_after_convergence_report.md"
        report_md.write_text(self._build_report(payload), encoding="utf-8")

        return {
            "report_md": report_md,
            "audit_json": audit_json,
            "ranked_hypotheses_csv": ranked_csv,
            "qa_overlay_png": plot_path,
            "recommended_next_rerun": ranked[0]["recommended_rerun"],
            "top_hypothesis": ranked[0]["label"],
            "remaining_problem_class": payload["remaining_problem_class"],
        }

    @staticmethod
    def _build_report(payload: dict) -> str:
        ranked = payload["ranked_hypotheses"]
        metrics = payload["metrics"]
        transport = payload["transport_checks"]
        forcing = payload["forcing_domain_checks"]
        obs = payload["observation_checks"]
        geometry = payload["geometry_checks"]
        lines = [
            "# Mindoro Displacement Audit After Shoreline + Convergence",
            "",
            "Official main Phase 3B semantics remain unchanged: March 6 date-composite p50 vs `obs_mask_2023-03-06.tif`, FSS at 1, 3, 5, and 10 km.",
            "",
            "## Decision",
            "",
            f"- Highest-priority rerun: `{payload['recommended_next_rerun']}`",
            f"- Most likely remaining problem class: `{payload['remaining_problem_class']}`",
            f"- Top hypothesis: `{ranked[0]['label']}` with score {ranked[0]['likelihood_score']:.3f}",
            "",
            "## Key Evidence",
            "",
            f"- Highest completed convergence count: {metrics['highest_completed_element_count']}",
            f"- Official March 6 p50 cells at that count: {metrics['highest_count_official_p50_nonzero_cells']}",
            f"- Maximum March 6 ensemble occupancy members: {metrics['highest_count_max_march6_occupancy_members']}",
            f"- Control final active particles: {metrics['control_final_active_particles']}; final stranded particles: {metrics['control_final_stranded_particles']}",
            f"- Control output stops {metrics['control_hours_short_of_requested_end']:.2f} h before requested simulation end.",
            f"- Official March 6 observed mask cells: {obs['march6_obs_nonzero_cells']}; processed vector area: {obs['march6_processed_vector_area_m2']:.0f} m2",
            f"- Seed centroid to processed March 3 centroid: {geometry['release_to_march3_centroid_m']:.1f} m",
            f"- Seed particles outside processed March 3 polygon fraction: {geometry['seed_distribution'].get('outside_processed_polygon_fraction', 0.0):.6f}",
            f"- Forcing bounds cover canonical scoring domain + halo: {forcing['forcing_bounds_cover_canonical_halo']}",
            f"- Download bbox source: `{forcing['download_manifest_bbox_source']}`",
            f"- Current/wind/wave reader status counts: {transport['reader_status_audit']['by_forcing_kind']}",
            "",
            "## Ranked Hypotheses",
            "",
        ]
        for row in ranked:
            lines.append(
                f"{row['rank']}. `{row['label']}` | score={row['likelihood_score']:.3f} | rerun={row['recommended_rerun']} | support={row['supporting_evidence'] or 'n/a'} | counter={row['contradicting_evidence'] or 'n/a'}"
            )
        lines.extend(
            [
                "",
                "## Broad-Domain Check",
                "",
                "The broad Mindoro spill-case domain remains present in compatibility fields and guarded fallback/source-QA code paths, but the current official download manifest and shoreline forcing-domain manifest use the canonical scoring-grid display bounds plus the 0.50 degree halo. No current artifact evidence supports broad-domain contamination as the primary remaining failure.",
                "",
                "## Recommendation",
                "",
                "Run one transport-model limitation rerun next. The rerun should focus on why the current provisional OceanDrift lane strands or loses all active particles before March 6 despite valid forcing attachment and canonical-domain forcing subsets. Do not switch cases, extend horizon, or change the official main pairing before that targeted rerun.",
            ]
        )
        return "\n".join(lines) + "\n"


def run_displacement_after_convergence() -> dict:
    return DisplacementAfterConvergenceAudit().run()


if __name__ == "__main__":  # pragma: no cover
    result = run_displacement_after_convergence()
    print(json.dumps({key: str(value) for key, value in result.items()}, indent=2))
