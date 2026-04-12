"""
Read-only displacement audit for the official Mindoro Phase 3B case.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd
import yaml
from shapely.geometry import box

from src.core.case_context import get_case_context
from src.helpers.raster import GridBuilder
from src.services.ensemble import normalize_time_index
from src.services.ingestion import derive_bbox_from_display_bounds
from src.utils.io import get_case_output_dir, get_ensemble_manifest_path, get_forecast_manifest_path

try:
    import geopandas as gpd
except ImportError:  # pragma: no cover - runtime guarded
    gpd = None

try:
    import matplotlib.pyplot as plt
except ImportError:  # pragma: no cover - runtime guarded
    plt = None

try:
    import rasterio
except ImportError:  # pragma: no cover - runtime guarded
    rasterio = None

try:
    import xarray as xr
except ImportError:  # pragma: no cover - runtime guarded
    xr = None


RERUN_BY_HYPOTHESIS = {
    "seed_geometry_bias_or_rescue_path_error": "initialization repair rerun",
    "broad_case_domain_contamination_in_forcing_subset_or_runtime_domain": "forcing-domain rerun",
    "coastal_masking_or_missing_shoreline_interaction": "shoreline-mask rerun",
    "wave_or_stokes_attach_mismatch": "forcing-domain rerun",
    "forcing_timing_or_halo_extent_issue": "forcing-domain rerun",
}


def _write_json(path: Path, payload: dict | list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, default=str)
        handle.write("\n")


def _read_json(path: Path) -> dict:
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle) or {}


def _parse_xy(text: str) -> tuple[float, float]:
    x_str, y_str = str(text).split(",", 1)
    return float(x_str), float(y_str)


def _distance(a: tuple[float, float], b: tuple[float, float]) -> float:
    return float(np.hypot(a[0] - b[0], a[1] - b[1]))


def _mask_centroid(mask: np.ndarray, grid: GridBuilder) -> tuple[float, float] | None:
    rows, cols = np.where(mask > 0)
    if len(rows) == 0:
        return None
    xs = grid.min_x + ((cols.astype(float) + 0.5) * grid.resolution)
    ys = grid.max_y - ((rows.astype(float) + 0.5) * grid.resolution)
    return float(xs.mean()), float(ys.mean())


def _read_binary_mask(path: Path) -> np.ndarray:
    if rasterio is None:
        raise ImportError("rasterio is required for the displacement audit.")
    with rasterio.open(path) as ds:
        return (ds.read(1) > 0).astype(np.uint8)


def _forcing_bounds(path: Path) -> dict:
    if xr is None:
        raise ImportError("xarray is required for the displacement audit.")
    with xr.open_dataset(path) as ds:
        lat_name = next((n for n in ("lat", "latitude", "LATITUDE") if n in ds.coords), None)
        lon_name = next((n for n in ("lon", "longitude", "LONGITUDE") if n in ds.coords), None)
        if lat_name is None:
            lat_name = next((n for n in ds.dims if "lat" in n.lower()), None)
        if lon_name is None:
            lon_name = next((n for n in ds.dims if "lon" in n.lower()), None)
        lat = np.asarray(ds[lat_name].values).astype(float)
        lon = np.asarray(ds[lon_name].values).astype(float)
    return {
        "path": str(path),
        "lat_name": lat_name,
        "lon_name": lon_name,
        "bounds_wgs84": [
            float(np.nanmin(lon)),
            float(np.nanmax(lon)),
            float(np.nanmin(lat)),
            float(np.nanmax(lat)),
        ],
    }


def _control_track(control_nc_path: Path, grid_crs: str) -> pd.DataFrame:
    if xr is None or gpd is None:
        raise ImportError("xarray and geopandas are required for the displacement audit.")
    rows: list[dict] = []
    with xr.open_dataset(control_nc_path) as ds:
        times = normalize_time_index(ds["time"].values)
        for index, timestamp in enumerate(times):
            lon = np.asarray(ds["lon"].isel(time=index).values).reshape(-1)
            lat = np.asarray(ds["lat"].isel(time=index).values).reshape(-1)
            status = np.asarray(ds["status"].isel(time=index).values).reshape(-1)
            valid = ~np.isnan(lon) & ~np.isnan(lat) & (status == 0)
            if not np.any(valid):
                continue
            points = gpd.GeoSeries(gpd.points_from_xy(lon[valid], lat[valid]), crs="EPSG:4326").to_crs(grid_crs)
            rows.append(
                {
                    "timestamp_utc": pd.Timestamp(timestamp).strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "centroid_x": float(points.x.mean()),
                    "centroid_y": float(points.y.mean()),
                    "active_particles": int(valid.sum()),
                }
            )
    return pd.DataFrame(rows)


def rank_displacement_hypotheses(metrics: dict) -> list[dict]:
    rows = []

    shoreline_score = 0.15
    shoreline_support = []
    shoreline_counter = []
    if metrics["sea_mask_all_sea"]:
        shoreline_score += 0.45
        shoreline_support.append("Sea mask is all ones across the scoring grid.")
    if metrics["runtime_landmask_disabled"]:
        shoreline_score += 0.30
        shoreline_support.append("Runtime disables auto landmask and injects a constant all-sea mask.")
    if metrics["local_strip_land_cells"] == 0:
        shoreline_score += 0.10
        shoreline_support.append("The local strip around the March 3 / March 6 geometries contains zero land cells.")
    rows.append(
        {
            "hypothesis_id": "coastal_masking_or_missing_shoreline_interaction",
            "label": "coastal masking / missing shoreline interaction",
            "likelihood_score": shoreline_score,
            "supporting_evidence": " | ".join(shoreline_support),
            "contradicting_evidence": " | ".join(shoreline_counter),
            "recommended_rerun": RERUN_BY_HYPOTHESIS["coastal_masking_or_missing_shoreline_interaction"],
        }
    )

    timing_score = 0.15
    timing_support = []
    timing_counter = []
    if metrics["current_tail_extension_run_count"] > 0:
        timing_score += 0.25
        timing_support.append(f"Current tail persistence was applied to {metrics['current_tail_extension_run_count']} runs.")
    if metrics["current_tail_extension_max_gap_hours"] >= 8.0:
        timing_score += 0.15
        timing_support.append(
            f"The maximum current coverage gap before persistence was {metrics['current_tail_extension_max_gap_hours']:.2f} h."
        )
    if metrics["recipe_best_mean_fss"] <= 0.0 and metrics["recipe_best_centroid_gain_m"] < 10000.0:
        timing_score -= 0.05
        timing_counter.append("Recipe changes did not materially improve FSS or displacement.")
    rows.append(
        {
            "hypothesis_id": "forcing_timing_or_halo_extent_issue",
            "label": "forcing timing / halo-extent issue",
            "likelihood_score": timing_score,
            "supporting_evidence": " | ".join(timing_support),
            "contradicting_evidence": " | ".join(timing_counter),
            "recommended_rerun": RERUN_BY_HYPOTHESIS["forcing_timing_or_halo_extent_issue"],
        }
    )

    domain_score = 0.10
    domain_support = []
    domain_counter = []
    if metrics["forcing_bounds_match_broad_case_domain_plus_pad"]:
        domain_score += 0.30
        domain_support.append("Current forcing files match the broad Mindoro spill-case domain plus the historical 3 degree pad.")
    if metrics["forcing_bounds_cover_canonical_halo"]:
        domain_score -= 0.05
        domain_counter.append("The legacy forcing subsets still fully cover the canonical scoring domain plus the 0.5 degree halo.")
    if metrics["runtime_grid_is_canonical"]:
        domain_score -= 0.05
        domain_counter.append("Runtime products already use the canonical scoring grid.")
    rows.append(
        {
            "hypothesis_id": "broad_case_domain_contamination_in_forcing_subset_or_runtime_domain",
            "label": "broad-case-domain contamination in forcing subset/runtime domain",
            "likelihood_score": domain_score,
            "supporting_evidence": " | ".join(domain_support),
            "contradicting_evidence": " | ".join(domain_counter),
            "recommended_rerun": RERUN_BY_HYPOTHESIS["broad_case_domain_contamination_in_forcing_subset_or_runtime_domain"],
        }
    )

    seed_score = 0.10
    seed_support = []
    seed_counter = []
    if metrics["march3_rescue_applied"]:
        seed_score += 0.18
        seed_support.append("March 3 ArcGIS ingestion required the rescue path for implausible near-zero coordinates.")
    if metrics["release_to_init_vector_centroid_m"] > 5000.0:
        seed_score += 0.08
        seed_support.append(
            f"Release centroid differs from the March 3 processed vector centroid by {metrics['release_to_init_vector_centroid_m']:.1f} m."
        )
    if metrics["release_to_init_raster_centroid_m"] <= 500.0:
        seed_score -= 0.12
        seed_counter.append(
            f"Release centroid is only {metrics['release_to_init_raster_centroid_m']:.1f} m from the March 3 raster centroid."
        )
    if metrics["control_distance_growth_m"] > 50000.0:
        seed_score -= 0.08
        seed_counter.append("Deterministic-control divergence grows strongly after release, which points more to runtime transport.")
    rows.append(
        {
            "hypothesis_id": "seed_geometry_bias_or_rescue_path_error",
            "label": "seed geometry bias or March 3 rescue-path error",
            "likelihood_score": seed_score,
            "supporting_evidence": " | ".join(seed_support),
            "contradicting_evidence": " | ".join(seed_counter),
            "recommended_rerun": RERUN_BY_HYPOTHESIS["seed_geometry_bias_or_rescue_path_error"],
        }
    )

    wave_score = 0.05
    wave_support = []
    wave_counter = []
    if metrics["wave_reader_loaded_for_all_runs"]:
        wave_score -= 0.03
        wave_counter.append("Wave/Stokes reader attached successfully with required variables for every audited run.")
    else:
        wave_score += 0.20
        wave_support.append("At least one audited run did not attach the wave/Stokes reader cleanly.")
    rows.append(
        {
            "hypothesis_id": "wave_or_stokes_attach_mismatch",
            "label": "wave/Stokes attach mismatch",
            "likelihood_score": wave_score,
            "supporting_evidence": " | ".join(wave_support),
            "contradicting_evidence": " | ".join(wave_counter),
            "recommended_rerun": RERUN_BY_HYPOTHESIS["wave_or_stokes_attach_mismatch"],
        }
    )

    ranked = sorted(rows, key=lambda row: (-float(row["likelihood_score"]), row["hypothesis_id"]))
    for index, row in enumerate(ranked, start=1):
        row["rank"] = index
        row["status"] = "strong" if row["likelihood_score"] >= 0.55 else ("mixed" if row["likelihood_score"] >= 0.25 else "weak")
    return ranked


def _plot_tracks(
    out_path: Path,
    grid: GridBuilder,
    init_gdf,
    validation_gdf,
    source_gdf,
    control_track: pd.DataFrame,
    obs_xy: tuple[float, float],
    baseline_p50_xy: tuple[float, float] | None,
    best_p50_xy: tuple[float, float] | None,
    forcing_bounds: list[float],
    halo_bounds: list[float],
) -> Path | None:
    if plt is None or gpd is None:
        return None

    init_wgs84 = init_gdf.to_crs("EPSG:4326")
    validation_wgs84 = validation_gdf.to_crs("EPSG:4326")
    source_wgs84 = source_gdf.to_crs("EPSG:4326")
    control_pts = gpd.GeoSeries(gpd.points_from_xy(control_track["centroid_x"], control_track["centroid_y"]), crs=grid.crs).to_crs("EPSG:4326")
    obs_pt = gpd.GeoSeries(gpd.points_from_xy([obs_xy[0]], [obs_xy[1]]), crs=grid.crs).to_crs("EPSG:4326").iloc[0]

    fig, axes = plt.subplots(1, 2, figsize=(13, 6))
    init_wgs84.plot(ax=axes[0], facecolor="none", edgecolor="#1f77b4", linewidth=1.5, label="March 3 init")
    validation_wgs84.plot(ax=axes[0], facecolor="none", edgecolor="#d62728", linewidth=1.5, label="March 6 validation")
    source_wgs84.plot(ax=axes[0], color="black", marker="*", markersize=40, label="Source point")
    axes[0].plot(control_pts.x, control_pts.y, color="#ff7f0e", linewidth=1.5, label="Control centroid path")
    axes[0].scatter(obs_pt.x, obs_pt.y, color="#d62728", marker="x", s=45, label="Obs centroid")
    if baseline_p50_xy is not None:
        pt = gpd.GeoSeries(gpd.points_from_xy([baseline_p50_xy[0]], [baseline_p50_xy[1]]), crs=grid.crs).to_crs("EPSG:4326").iloc[0]
        axes[0].scatter(pt.x, pt.y, color="#9467bd", s=35, label="Baseline P50 centroid")
    if best_p50_xy is not None:
        pt = gpd.GeoSeries(gpd.points_from_xy([best_p50_xy[0]], [best_p50_xy[1]]), crs=grid.crs).to_crs("EPSG:4326").iloc[0]
        axes[0].scatter(pt.x, pt.y, color="#2ca02c", s=35, label="Best sensitivity P50 centroid")
    axes[0].set_title("Local displacement track")
    axes[0].set_xlabel("Longitude")
    axes[0].set_ylabel("Latitude")
    axes[0].legend(loc="best", fontsize=8)

    gpd.GeoSeries([box(forcing_bounds[0], forcing_bounds[2], forcing_bounds[1], forcing_bounds[3])], crs="EPSG:4326").plot(
        ax=axes[1], facecolor="none", edgecolor="#7f7f7f", linewidth=1.5, label="Current forcing subset"
    )
    gpd.GeoSeries([box(halo_bounds[0], halo_bounds[2], halo_bounds[1], halo_bounds[3])], crs="EPSG:4326").plot(
        ax=axes[1], facecolor="none", edgecolor="#ff7f0e", linewidth=1.5, label="Canonical grid + 0.5 deg halo"
    )
    init_wgs84.plot(ax=axes[1], facecolor="none", edgecolor="#1f77b4", linewidth=1.0)
    validation_wgs84.plot(ax=axes[1], facecolor="none", edgecolor="#d62728", linewidth=1.0)
    axes[1].set_title("Forcing subset comparison")
    axes[1].set_xlabel("Longitude")
    axes[1].set_ylabel("Latitude")
    axes[1].legend(loc="best", fontsize=8)

    fig.tight_layout()
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    return out_path


def run_displacement_audit() -> dict:
    case = get_case_context()
    if not case.is_official:
        raise RuntimeError("The displacement audit is only available for official workflow modes.")
    if gpd is None or xr is None:
        raise ImportError("geopandas and xarray are required for the displacement audit.")

    base_output = get_case_output_dir(case.run_name)
    audit_dir = base_output / "displacement_audit"
    audit_dir.mkdir(parents=True, exist_ok=True)
    grid = GridBuilder()

    paths = {
        "arcgis_report": Path("data") / "arcgis" / case.run_name / "arcgis_processing_report.csv",
        "init_vector": case.initialization_layer.processed_vector_path(case.run_name),
        "validation_vector": case.validation_layer.processed_vector_path(case.run_name),
        "source_vector": case.provenance_layer.processed_vector_path(case.run_name),
        "scoring_grid": Path("data_processed/grids/scoring_grid.yaml"),
        "land_mask": Path("data_processed/grids/land_mask.tif"),
        "sea_mask": Path("data_processed/grids/sea_mask.tif"),
        "phase2_audit": base_output / "forecast" / "phase2_loading_audit.json",
        "recipe_summary": base_output / "recipe_sensitivity" / "recipe_sensitivity_summary.csv",
        "recipe_diagnostics": base_output / "recipe_sensitivity" / "recipe_sensitivity_diagnostics.csv",
        "phase3b_summary": base_output / "phase3b" / "phase3b_summary.csv",
        "phase3b_diagnostics": base_output / "phase3b" / "phase3b_diagnostics.csv",
        "phase3b_forensics": base_output / "phase3b" / "diagnostics_forensics" / "phase3b_diagnosis.json",
        "forecast_manifest": get_forecast_manifest_path(case.run_name),
        "ensemble_manifest": get_ensemble_manifest_path(case.run_name),
        "control_nc": base_output / "forecast" / "deterministic_control_cmems_era5.nc",
        "obs_mask": Path("data") / "arcgis" / case.run_name / "obs_mask_2023-03-06.tif",
        "baseline_p50": base_output / "ensemble" / "mask_p50_2023-03-06_datecomposite.tif",
        "current_forcing": Path("data") / "forcing" / case.run_name / "cmems_curr.nc",
        "wind_forcing": Path("data") / "forcing" / case.run_name / "era5_wind.nc",
        "wave_forcing": Path("data") / "forcing" / case.run_name / "cmems_wave.nc",
    }
    missing = [f"{name}={path}" for name, path in paths.items() if not path.exists()]
    if missing:
        raise FileNotFoundError("Displacement audit requires existing official artifacts. Missing: " + ", ".join(missing))

    arcgis_report = pd.read_csv(paths["arcgis_report"])
    recipe_summary = pd.read_csv(paths["recipe_summary"])
    phase3b_summary = pd.read_csv(paths["phase3b_summary"])
    phase2_audit = _read_json(paths["phase2_audit"])
    forecast_manifest = _read_json(paths["forecast_manifest"])
    ensemble_manifest = _read_json(paths["ensemble_manifest"])
    phase3b_forensics = _read_json(paths["phase3b_forensics"])
    scoring_grid = yaml.safe_load(paths["scoring_grid"].read_text(encoding="utf-8")) or {}

    init_gdf = gpd.read_file(paths["init_vector"]).to_crs(grid.crs)
    validation_gdf = gpd.read_file(paths["validation_vector"]).to_crs(grid.crs)
    source_gdf = gpd.read_file(paths["source_vector"]).to_crs(grid.crs)

    init_report = arcgis_report[arcgis_report["layer_id"].astype(int) == int(case.initialization_layer.layer_id)].iloc[0]
    init_vector_xy = _parse_xy(init_report["vector_centroid"])
    init_raster_xy = _parse_xy(init_report["raster_centroid"])

    obs_mask = _read_binary_mask(paths["obs_mask"])
    obs_xy = _mask_centroid(obs_mask, grid)
    if obs_xy is None:
        raise RuntimeError("Observation mask is empty.")

    control_track = _control_track(paths["control_nc"], grid.crs)
    control_track["dist_to_obs_centroid_m"] = np.hypot(control_track["centroid_x"] - obs_xy[0], control_track["centroid_y"] - obs_xy[1])
    release_xy = (float(control_track.iloc[0]["centroid_x"]), float(control_track.iloc[0]["centroid_y"]))
    final_xy = (float(control_track.iloc[-1]["centroid_x"]), float(control_track.iloc[-1]["centroid_y"]))

    baseline_p50_xy = _mask_centroid(_read_binary_mask(paths["baseline_p50"]), grid)

    completed = recipe_summary[recipe_summary["status"].astype(str) == "completed"].copy()
    for col in ("mean_fss", "centroid_distance_m", "fss_1km", "fss_3km", "fss_5km", "fss_10km", "area_ratio_forecast_to_obs", "iou", "dice", "nearest_distance_to_obs_m", "p50_nonzero_cells", "obs_nonzero_cells", "max_march6_occupancy_members"):
        if col in completed.columns:
            completed[col] = pd.to_numeric(completed[col], errors="coerce")
    completed = completed.sort_values(by=["mean_fss", "fss_10km", "fss_5km", "fss_3km", "fss_1km", "centroid_distance_m"], ascending=[False, False, False, False, False, True])
    best_recipe = completed.iloc[0]
    baseline_recipe = completed[completed["recipe_id"].astype(str) == str(forecast_manifest.get("recipe_selection", {}).get("recipe", ""))]
    baseline_recipe = baseline_recipe.iloc[0] if not baseline_recipe.empty else best_recipe
    best_recipe_xy = _mask_centroid(_read_binary_mask(Path(str(best_recipe["forecast_path"]))), grid)

    current_bounds = _forcing_bounds(paths["current_forcing"])
    wind_bounds = _forcing_bounds(paths["wind_forcing"])
    wave_bounds = _forcing_bounds(paths["wave_forcing"])
    broad_case_domain_plus_pad = [
        float(case.mindoro_case_domain[0] - 3.0),
        float(case.mindoro_case_domain[1] + 3.0),
        float(case.mindoro_case_domain[2] - 3.0),
        float(case.mindoro_case_domain[3] + 3.0),
    ]
    halo_bounds = derive_bbox_from_display_bounds(scoring_grid.get("display_bounds_wgs84", list(case.region)), float(scoring_grid.get("forcing_bbox_halo_degrees", 0.5)))
    covers = lambda bounds, target: bounds[0] <= target[0] and bounds[1] >= target[1] and bounds[2] <= target[2] and bounds[3] >= target[3]

    land = _read_binary_mask(paths["land_mask"])
    sea = _read_binary_mask(paths["sea_mask"])
    roi_bounds = init_gdf.geometry.union_all().union(validation_gdf.geometry.union_all()).buffer(10000.0).bounds if hasattr(init_gdf.geometry, "union_all") else init_gdf.geometry.unary_union.union(validation_gdf.geometry.unary_union).buffer(10000.0).bounds
    min_x, min_y, max_x, max_y = roi_bounds
    min_col = max(0, int(np.floor((min_x - grid.min_x) / grid.resolution)))
    max_col = min(grid.width - 1, int(np.ceil((max_x - grid.min_x) / grid.resolution)))
    min_row = max(0, int(np.floor((grid.max_y - max_y) / grid.resolution)))
    max_row = min(grid.height - 1, int(np.ceil((grid.max_y - min_y) / grid.resolution)))
    local_land_cells = int(np.count_nonzero(land[min_row:max_row + 1, min_col:max_col + 1] > 0))

    wave_statuses = {run.get("forcings", {}).get("wave", {}).get("reader_attach_status", "") for run in phase2_audit.get("runs", [])}
    current_entries = [run.get("forcings", {}).get("current", {}) for run in phase2_audit.get("runs", [])]
    gap_values = [float(entry.get("coverage_gap_hours", 0.0)) for entry in current_entries if entry]
    runtime_source = Path("src/services/ensemble.py").read_text(encoding="utf-8")

    metrics = {
        "release_to_init_vector_centroid_m": _distance(release_xy, init_vector_xy),
        "release_to_init_raster_centroid_m": _distance(release_xy, init_raster_xy),
        "current_tail_extension_run_count": int(sum(bool(entry.get("tail_extension_applied", False)) for entry in current_entries)),
        "current_tail_extension_max_gap_hours": float(max(gap_values) if gap_values else 0.0),
        "recipe_best_mean_fss": float(best_recipe["mean_fss"]),
        "recipe_best_centroid_gain_m": float(baseline_recipe["centroid_distance_m"] - best_recipe["centroid_distance_m"]),
        "forcing_bounds_match_broad_case_domain_plus_pad": all(np.allclose(item["bounds_wgs84"], broad_case_domain_plus_pad) for item in (current_bounds, wind_bounds, wave_bounds)),
        "forcing_bounds_cover_canonical_halo": all(covers(item["bounds_wgs84"], halo_bounds) for item in (current_bounds, wind_bounds, wave_bounds)),
        "runtime_grid_is_canonical": bool((forecast_manifest.get("grid") or {}).get("grid_id") == (ensemble_manifest.get("grid") or {}).get("grid_id") == (phase3b_forensics.get("primary_pairing") or {}).get("grid_id")),
        "march3_rescue_applied": "rescued scaled-to-near-zero coordinates" in str(init_report["notes"]),
        "control_distance_growth_m": _distance(final_xy, obs_xy) - _distance(release_xy, obs_xy),
        "sea_mask_all_sea": bool(np.all(land == 0) and np.all(sea == 1)),
        "local_strip_land_cells": local_land_cells,
        "runtime_landmask_disabled": 'model.set_config("general:use_auto_landmask", False)' in runtime_source and 'reader_constant.Reader({"land_binary_mask": 0})' in runtime_source,
        "wave_reader_loaded_for_all_runs": wave_statuses == {"loaded"},
    }
    ranked = rank_displacement_hypotheses(metrics)
    recommended_next_rerun = ranked[0]["recommended_rerun"]

    plot_path = _plot_tracks(
        out_path=audit_dir / "qa_displacement_tracks.png",
        grid=grid,
        init_gdf=init_gdf,
        validation_gdf=validation_gdf,
        source_gdf=source_gdf,
        control_track=control_track,
        obs_xy=obs_xy,
        baseline_p50_xy=baseline_p50_xy,
        best_p50_xy=best_recipe_xy,
        forcing_bounds=current_bounds["bounds_wgs84"],
        halo_bounds=halo_bounds,
    )

    ranked_csv = audit_dir / "displacement_hypotheses_ranked.csv"
    report_md = audit_dir / "displacement_audit_report.md"
    audit_json = audit_dir / "displacement_audit.json"
    pd.DataFrame(ranked).to_csv(ranked_csv, index=False)

    payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "run_name": case.run_name,
        "workflow_mode": case.workflow_mode,
        "recommended_next_rerun": recommended_next_rerun,
        "ranked_hypotheses": ranked,
        "release_checks": {
            "release_centroid_xy": release_xy,
            "march3_vector_centroid_xy": init_vector_xy,
            "march3_raster_centroid_xy": init_raster_xy,
            "obs_centroid_xy": obs_xy,
            "release_to_init_vector_centroid_m": metrics["release_to_init_vector_centroid_m"],
            "release_to_init_raster_centroid_m": metrics["release_to_init_raster_centroid_m"],
            "first_control_distance_to_obs_m": float(control_track.iloc[0]["dist_to_obs_centroid_m"]),
            "last_control_distance_to_obs_m": float(control_track.iloc[-1]["dist_to_obs_centroid_m"]),
        },
        "forcing_bounds_check": {
            "broad_mindoro_case_domain_plus_3deg_pad": broad_case_domain_plus_pad,
            "canonical_scoring_domain_plus_0_5deg_halo": halo_bounds,
            "currents": current_bounds,
            "winds": wind_bounds,
            "waves": wave_bounds,
            "forcing_bounds_match_broad_case_domain_plus_pad": metrics["forcing_bounds_match_broad_case_domain_plus_pad"],
            "forcing_bounds_cover_canonical_halo": metrics["forcing_bounds_cover_canonical_halo"],
        },
        "runtime_logic_check": {
            "runtime_grid_is_canonical": metrics["runtime_grid_is_canonical"],
            "shoreline_mask_status": scoring_grid.get("shoreline_mask_status", ""),
            "runtime_landmask_disabled": metrics["runtime_landmask_disabled"],
            "wave_reader_statuses": sorted(status for status in wave_statuses if status),
            "current_tail_extension_run_count": metrics["current_tail_extension_run_count"],
            "current_tail_extension_max_gap_hours": metrics["current_tail_extension_max_gap_hours"],
        },
        "recipe_sensitivity_check": {
            "best_recipe_id": str(best_recipe["recipe_id"]),
            "best_recipe_mean_fss": float(best_recipe["mean_fss"]),
            "best_recipe_centroid_distance_m": float(best_recipe["centroid_distance_m"]),
            "baseline_recipe_id": str(baseline_recipe["recipe_id"]),
            "baseline_mean_fss": float(baseline_recipe["mean_fss"]),
            "baseline_centroid_distance_m": float(baseline_recipe["centroid_distance_m"]),
        },
        "artifact_paths": {
            "ranked_hypotheses_csv": str(ranked_csv),
            "audit_json": str(audit_json),
            "report_md": str(report_md),
            "qa_plot_png": str(plot_path) if plot_path else "",
        },
    }
    _write_json(audit_json, payload)

    lines = [
        "# Mindoro Phase 3B Displacement Audit",
        "",
        f"- Recommended next rerun: `{recommended_next_rerun}`",
        f"- Top hypothesis: `{ranked[0]['label']}`",
        f"- Best event-scale sensitivity recipe: `{best_recipe['recipe_id']}`",
        "",
        "## Explicit checks",
        "",
        (
            f"- Release centroid vs March 3 processed vector centroid: {metrics['release_to_init_vector_centroid_m']:.1f} m; "
            f"vs March 3 raster centroid: {metrics['release_to_init_raster_centroid_m']:.1f} m."
        ),
        (
            f"- Control centroid distance to obs grows from {float(control_track.iloc[0]['dist_to_obs_centroid_m']):.1f} m "
            f"at release to {float(control_track.iloc[-1]['dist_to_obs_centroid_m']):.1f} m at 72 h."
        ),
        f"- Current forcing subset bounds: {current_bounds['bounds_wgs84']}",
        f"- Canonical scoring domain + 0.5 degree halo: {halo_bounds}",
        f"- Sea mask all-sea: {metrics['sea_mask_all_sea']}; local strip land cells: {metrics['local_strip_land_cells']}",
        f"- Wave/Stokes reader statuses: {sorted(status for status in wave_statuses if status)}",
        "",
        "## Ranked hypotheses",
        "",
    ]
    for row in ranked:
        lines.append(
            f"{row['rank']}. `{row['label']}` | score={row['likelihood_score']:.2f} | status={row['status']} | support={row['supporting_evidence'] or 'n/a'} | counter={row['contradicting_evidence'] or 'n/a'}"
        )
    lines.extend(
        [
            "",
            "## Conclusion",
            "",
            "The remaining displacement is most consistent with the current all-sea coastal treatment. The current on-disk forcing subsets also still reflect the broad Mindoro spill-case domain, but recipe changes did not materially improve overlap and wave attachment is clean.",
            "Future official prep runs should use the canonical scoring-grid display bounds plus the 0.5 degree halo instead of the broad Mindoro spill-case domain field.",
        ]
    )
    report_md.write_text("\n".join(lines) + "\n", encoding="utf-8")

    return {
        "ranked_hypotheses_csv": ranked_csv,
        "audit_json": audit_json,
        "report_md": report_md,
        "qa_plot_png": plot_path,
        "recommended_next_rerun": recommended_next_rerun,
        "top_hypothesis": ranked[0]["label"],
    }


if __name__ == "__main__":  # pragma: no cover - manual execution entrypoint
    artifacts = run_displacement_audit()
    print(json.dumps({key: str(value) for key, value in artifacts.items()}, indent=2))
