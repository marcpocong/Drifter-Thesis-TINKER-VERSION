"""Build a secondary 2016 drifter-track benchmark from stored outputs.

The observed NOAA drifter track is the reference. PyGNOME is used only as a
deterministic comparator against the same observed drifter timestamps.
"""

from __future__ import annotations

import json
import math
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import matplotlib
import numpy as np
import pandas as pd
import xarray as xr
import yaml

matplotlib.use("Agg")
import matplotlib.pyplot as plt

from src.utils.io import load_drifter_data, select_drifter_of_record


OUTPUT_DIR = REPO_ROOT / "output" / "2016_drifter_benchmark"
CASE_BOARDS_DIR = OUTPUT_DIR / "case_boards"
CASE_REGISTRY = REPO_ROOT / "output" / "prototype_2016_pygnome_similarity" / "prototype_pygnome_case_registry.csv"
HORIZONS_HOURS = (24, 48, 72)
EARTH_RADIUS_KM = 6371.0088
EPS = 1.0e-12


@dataclass(frozen=True)
class CaseInputs:
    case_id: str
    observed_csv: Path
    opendrift_nc: Path
    pygnome_nc: Path
    ensemble_member_ncs: tuple[Path, ...]
    probability_ncs: dict[int, Path]
    benchmark_dir: Path
    config_snapshot: Path | None


def _repo_rel(path: Path | None) -> str:
    if path is None:
        return ""
    try:
        return str(path.resolve().relative_to(REPO_ROOT)).replace("\\", "/")
    except ValueError:
        return str(path).replace("\\", "/")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, default=_json_default) + "\n", encoding="utf-8")


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.rstrip() + "\n", encoding="utf-8")


def _json_default(value: Any) -> Any:
    if isinstance(value, Path):
        return _repo_rel(value)
    if isinstance(value, (pd.Timestamp, np.datetime64)):
        return _utc_iso(value)
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return str(value)


def _utc_naive(value: Any) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    if ts.tzinfo is not None:
        ts = ts.tz_convert("UTC").tz_localize(None)
    return ts


def _utc_iso(value: Any) -> str:
    return _utc_naive(value).strftime("%Y-%m-%dT%H:%M:%SZ")


def _normalize_times(values: Any) -> pd.DatetimeIndex:
    index = pd.DatetimeIndex(pd.to_datetime(values, errors="coerce"))
    if index.tz is not None:
        index = index.tz_convert("UTC").tz_localize(None)
    try:
        index = index.as_unit("ns")
    except AttributeError:
        index = pd.DatetimeIndex(index.to_numpy(dtype="datetime64[ns]"))
    return index


def _finite_or_none(value: Any, digits: int | None = None) -> float | None:
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(numeric):
        return None
    return round(numeric, digits) if digits is not None else numeric


def _safe_percent_improvement(reference_km: float | None, candidate_km: float | None) -> float | None:
    if reference_km is None or candidate_km is None:
        return None
    if not math.isfinite(reference_km) or abs(reference_km) <= EPS:
        return None
    return 100.0 * (reference_km - candidate_km) / reference_km


def haversine_km(lon1: Any, lat1: Any, lon2: Any, lat2: Any) -> np.ndarray:
    lon1_arr = np.asarray(lon1, dtype=float)
    lat1_arr = np.asarray(lat1, dtype=float)
    lon2_arr = np.asarray(lon2, dtype=float)
    lat2_arr = np.asarray(lat2, dtype=float)
    lon1_rad = np.deg2rad(lon1_arr)
    lat1_rad = np.deg2rad(lat1_arr)
    lon2_rad = np.deg2rad(lon2_arr)
    lat2_rad = np.deg2rad(lat2_arr)
    dlon = lon2_rad - lon1_rad
    dlat = lat2_rad - lat1_rad
    a = np.sin(dlat / 2.0) ** 2 + np.cos(lat1_rad) * np.cos(lat2_rad) * np.sin(dlon / 2.0) ** 2
    return EARTH_RADIUS_KM * 2.0 * np.arctan2(np.sqrt(a), np.sqrt(np.maximum(0.0, 1.0 - a)))


def _track_length_km(track: pd.DataFrame) -> float:
    clean = track.dropna(subset=["lon", "lat"]).sort_values("time").reset_index(drop=True)
    if len(clean) < 2:
        return 0.0
    return float(
        np.sum(
            haversine_km(
                clean["lon"].to_numpy()[:-1],
                clean["lat"].to_numpy()[:-1],
                clean["lon"].to_numpy()[1:],
                clean["lat"].to_numpy()[1:],
            )
        )
    )


def _discover_cases() -> list[str]:
    if CASE_REGISTRY.exists():
        registry = pd.read_csv(CASE_REGISTRY)
        if "case_id" in registry.columns:
            case_ids = [str(value).strip() for value in registry["case_id"].tolist() if str(value).strip()]
            return sorted(dict.fromkeys(case_ids))
    return sorted(path.name for path in (REPO_ROOT / "output").glob("CASE_2016-09-*") if path.is_dir())


def _resolve_case_inputs(case_id: str) -> CaseInputs:
    output_case_dir = REPO_ROOT / "output" / case_id
    benchmark_dir = output_case_dir / "benchmark"
    observed_csv = REPO_ROOT / "data" / "drifters" / case_id / "drifters_noaa.csv"
    deterministic_candidates = sorted((output_case_dir / "forecast").glob("deterministic_control*.nc"))
    if not deterministic_candidates:
        raise FileNotFoundError(f"No OpenDrift deterministic NetCDF found for {case_id}.")
    opendrift_nc = deterministic_candidates[0]
    pygnome_nc = benchmark_dir / "pygnome" / "pygnome_deterministic_control.nc"
    ensemble_member_ncs = tuple(sorted((output_case_dir / "ensemble").glob("member_*.nc")))
    probability_ncs = {
        hour: output_case_dir / "ensemble" / f"probability_{hour}h.nc"
        for hour in HORIZONS_HOURS
        if (output_case_dir / "ensemble" / f"probability_{hour}h.nc").exists()
    }
    config_snapshot = benchmark_dir / "config_snapshot.yaml"
    if not config_snapshot.exists():
        config_snapshot = None
    for required in (observed_csv, opendrift_nc, pygnome_nc):
        if not required.exists():
            raise FileNotFoundError(f"Required 2016 benchmark input missing: {_repo_rel(required)}")
    return CaseInputs(
        case_id=case_id,
        observed_csv=observed_csv,
        opendrift_nc=opendrift_nc,
        pygnome_nc=pygnome_nc,
        ensemble_member_ncs=ensemble_member_ncs,
        probability_ncs=probability_ncs,
        benchmark_dir=benchmark_dir,
        config_snapshot=config_snapshot,
    )


def _load_observed_track(observed_csv: Path) -> tuple[pd.DataFrame, dict[str, Any]]:
    selection = select_drifter_of_record(load_drifter_data(observed_csv))
    observed = selection["drifter_df"].copy()
    observed["time"] = _normalize_times(observed["time"])
    observed["lat"] = observed["lat"].astype(float)
    observed["lon"] = observed["lon"].astype(float)
    observed = observed.sort_values("time").reset_index(drop=True)
    metadata = {
        "selected_drifter_id": selection.get("selected_id"),
        "selected_point_count": int(selection.get("point_count") or len(observed)),
        "start_time_utc": selection["start_time"],
        "start_lat": float(selection["start_lat"]),
        "start_lon": float(selection["start_lon"]),
    }
    return observed, metadata


def _median_active_track_from_opendrift(nc_path: Path, label: str) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    with xr.open_dataset(nc_path) as ds:
        times = _normalize_times(ds["time"].values)
        lon_da = ds["lon"].transpose("time", ...)
        lat_da = ds["lat"].transpose("time", ...)
        status_da = ds["status"].transpose("time", ...) if "status" in ds else None
        lon_values = np.asarray(lon_da.values, dtype=float).reshape((len(times), -1))
        lat_values = np.asarray(lat_da.values, dtype=float).reshape((len(times), -1))
        status_values = (
            np.asarray(status_da.values, dtype=float).reshape((len(times), -1))
            if status_da is not None
            else np.zeros_like(lon_values)
        )
        for idx, timestamp in enumerate(times):
            valid = np.isfinite(lon_values[idx]) & np.isfinite(lat_values[idx]) & (status_values[idx] == 0)
            rows.append(
                {
                    "time": pd.Timestamp(timestamp),
                    "lon": float(np.nanmedian(lon_values[idx][valid])) if np.any(valid) else np.nan,
                    "lat": float(np.nanmedian(lat_values[idx][valid])) if np.any(valid) else np.nan,
                    "active_particle_count": int(np.count_nonzero(valid)),
                    "track_label": label,
                    "representative_position": "median_active_particles",
                }
            )
    return pd.DataFrame(rows).dropna(subset=["time"]).sort_values("time").reset_index(drop=True)


def _median_active_track_from_pygnome(nc_path: Path) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    with xr.open_dataset(nc_path) as ds:
        times = _normalize_times(ds["time"].values)
        counts = np.asarray(ds["particle_count"].values, dtype=int)
        starts = np.concatenate(([0], np.cumsum(counts[:-1])))
        lon_all = np.asarray(ds["longitude"].values, dtype=float)
        lat_all = np.asarray(ds["latitude"].values, dtype=float)
        status_all = np.asarray(ds["status_codes"].values, dtype=int) if "status_codes" in ds else np.full_like(lon_all, 2)
        for idx, timestamp in enumerate(times):
            start_idx = int(starts[idx])
            end_idx = start_idx + int(counts[idx])
            lon = lon_all[start_idx:end_idx]
            lat = lat_all[start_idx:end_idx]
            status = status_all[start_idx:end_idx]
            valid = np.isfinite(lon) & np.isfinite(lat) & (status == 2)
            if not np.any(valid):
                valid = np.isfinite(lon) & np.isfinite(lat)
            rows.append(
                {
                    "time": pd.Timestamp(timestamp),
                    "lon": float(np.nanmedian(lon[valid])) if np.any(valid) else np.nan,
                    "lat": float(np.nanmedian(lat[valid])) if np.any(valid) else np.nan,
                    "active_particle_count": int(np.count_nonzero(valid)),
                    "track_label": "PyGNOME deterministic",
                    "representative_position": "median_active_particles",
                }
            )
    return pd.DataFrame(rows).dropna(subset=["time"]).sort_values("time").reset_index(drop=True)


def _interpolate_track(track: pd.DataFrame, target_times: pd.Series | pd.DatetimeIndex | list[pd.Timestamp]) -> pd.DataFrame:
    target_index = _normalize_times(target_times)
    result = pd.DataFrame({"time": target_index})
    clean = track.dropna(subset=["time", "lon", "lat"]).sort_values("time").drop_duplicates("time")
    if len(clean) < 2:
        result["lon"] = np.nan
        result["lat"] = np.nan
        return result
    source_ns = _normalize_times(clean["time"]).asi8
    target_ns = _normalize_times(result["time"]).asi8
    in_range = (target_ns >= source_ns.min()) & (target_ns <= source_ns.max())
    for column in ("lon", "lat"):
        values = np.full(len(target_ns), np.nan, dtype=float)
        values[in_range] = np.interp(target_ns[in_range], source_ns, clean[column].to_numpy(dtype=float))
        result[column] = values
    return result


def _separation_series(observed: pd.DataFrame, model_at_observed: pd.DataFrame, prefix: str) -> pd.DataFrame:
    out = observed[["time", "lon", "lat"]].rename(columns={"lon": "observed_lon", "lat": "observed_lat"}).copy()
    out[f"{prefix}_lon"] = model_at_observed["lon"].to_numpy(dtype=float)
    out[f"{prefix}_lat"] = model_at_observed["lat"].to_numpy(dtype=float)
    valid = np.isfinite(out[f"{prefix}_lon"]) & np.isfinite(out[f"{prefix}_lat"])
    distances = np.full(len(out), np.nan, dtype=float)
    distances[valid] = haversine_km(
        out.loc[valid, "observed_lon"].to_numpy(dtype=float),
        out.loc[valid, "observed_lat"].to_numpy(dtype=float),
        out.loc[valid, f"{prefix}_lon"].to_numpy(dtype=float),
        out.loc[valid, f"{prefix}_lat"].to_numpy(dtype=float),
    )
    out[f"{prefix}_separation_km"] = distances
    return out


def _separation_summary(
    separation_df: pd.DataFrame,
    separation_column: str,
    observed_path_length_km: float,
    start_time: pd.Timestamp,
) -> dict[str, Any]:
    valid = separation_df.dropna(subset=[separation_column]).copy()
    summary: dict[str, Any] = {
        "time_averaged_separation_km": _finite_or_none(valid[separation_column].mean() if not valid.empty else np.nan),
        "normalized_cumulative_separation": None,
        "valid_aligned_count": int(len(valid)),
    }
    if observed_path_length_km > EPS and not valid.empty:
        summary["normalized_cumulative_separation"] = _finite_or_none(
            float(valid[separation_column].sum()) / observed_path_length_km
        )
    for hour in HORIZONS_HOURS:
        target = start_time + pd.Timedelta(hours=hour)
        match = separation_df.loc[separation_df["time"] == target, separation_column]
        summary[f"endpoint_{hour}h_km"] = _finite_or_none(match.iloc[0] if not match.empty else np.nan)
    return summary


def _nearest_member_distances(
    observed: pd.DataFrame,
    member_tracks: list[tuple[str, pd.DataFrame]],
) -> tuple[pd.DataFrame, dict[str, Any]]:
    nearest_rows: list[dict[str, Any]] = []
    target_times = observed["time"]
    obs_lon = observed["lon"].to_numpy(dtype=float)
    obs_lat = observed["lat"].to_numpy(dtype=float)
    all_member_distances: list[np.ndarray] = []
    member_ids: list[str] = []
    for member_id, track in member_tracks:
        interp = _interpolate_track(track, target_times)
        distances = np.full(len(observed), np.nan, dtype=float)
        valid = np.isfinite(interp["lon"].to_numpy(dtype=float)) & np.isfinite(interp["lat"].to_numpy(dtype=float))
        distances[valid] = haversine_km(
            obs_lon[valid],
            obs_lat[valid],
            interp.loc[valid, "lon"].to_numpy(dtype=float),
            interp.loc[valid, "lat"].to_numpy(dtype=float),
        )
        all_member_distances.append(distances)
        member_ids.append(member_id)
    if all_member_distances:
        matrix = np.vstack(all_member_distances)
        all_nan = np.all(np.isnan(matrix), axis=0)
        filled = np.where(np.isnan(matrix), np.inf, matrix)
        min_indices = np.argmin(filled, axis=0)
        min_values = np.min(filled, axis=0)
        min_values[all_nan] = np.nan
    else:
        min_indices = np.zeros(len(observed), dtype=int)
        min_values = np.full(len(observed), np.nan, dtype=float)

    for idx, row in observed.reset_index(drop=True).iterrows():
        nearest_rows.append(
            {
                "time": row["time"],
                "observed_lon": float(row["lon"]),
                "observed_lat": float(row["lat"]),
                "nearest_member_id": member_ids[int(min_indices[idx])] if member_ids and np.isfinite(min_values[idx]) else "",
                "nearest_member_distance_km": _finite_or_none(min_values[idx]),
            }
        )
    nearest_df = pd.DataFrame(nearest_rows)
    return nearest_df, {
        "member_count": len(member_tracks),
        "valid_aligned_count": int(nearest_df["nearest_member_distance_km"].notna().sum()),
    }


def _load_probability_diagnostic(path: Path, lon: float, lat: float) -> dict[str, Any]:
    if not path.exists() or not np.isfinite(lon) or not np.isfinite(lat):
        return {
            "probability_at_observed_cell": None,
            "ensemble_contains_any": None,
            "ensemble_contains_p50": None,
            "ensemble_contains_p90": None,
            "distance_to_any_footprint_km": None,
            "distance_to_p50_footprint_km": None,
            "distance_to_p90_footprint_km": None,
        }
    with xr.open_dataset(path) as ds:
        prob = np.asarray(ds["probability"].isel(time=0).values, dtype=float)
        lons = np.asarray(ds["lon"].values, dtype=float)
        lats = np.asarray(ds["lat"].values, dtype=float)
    lon_idx = int(np.abs(lons - lon).argmin())
    lat_idx = int(np.abs(lats - lat).argmin())
    lon_step = float(np.nanmedian(np.abs(np.diff(lons)))) if len(lons) > 1 else 0.0
    lat_step = float(np.nanmedian(np.abs(np.diff(lats)))) if len(lats) > 1 else 0.0
    in_grid = (
        (float(np.nanmin(lons)) - lon_step / 2.0) <= lon <= (float(np.nanmax(lons)) + lon_step / 2.0)
        and (float(np.nanmin(lats)) - lat_step / 2.0) <= lat <= (float(np.nanmax(lats)) + lat_step / 2.0)
    )
    value = float(prob[lat_idx, lon_idx]) if in_grid and np.isfinite(prob[lat_idx, lon_idx]) else 0.0

    lon_grid, lat_grid = np.meshgrid(lons, lats)

    def _distance_to_mask(mask: np.ndarray) -> float | None:
        if not np.any(mask):
            return None
        distances = haversine_km(lon, lat, lon_grid[mask], lat_grid[mask])
        return _finite_or_none(np.nanmin(distances))

    any_mask = prob > 0.0
    p50_mask = prob >= 0.50
    p90_mask = prob >= 0.90
    return {
        "probability_at_observed_cell": _finite_or_none(value),
        "ensemble_contains_any": bool(value > 0.0),
        "ensemble_contains_p50": bool(value >= 0.50),
        "ensemble_contains_p90": bool(value >= 0.90),
        "distance_to_any_footprint_km": 0.0 if value > 0.0 else _distance_to_mask(any_mask),
        "distance_to_p50_footprint_km": 0.0 if value >= 0.50 else _distance_to_mask(p50_mask),
        "distance_to_p90_footprint_km": 0.0 if value >= 0.90 else _distance_to_mask(p90_mask),
    }


def _model_difference_summary(opendrift: dict[str, Any], pygnome: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    od_avg = opendrift.get("time_averaged_separation_km")
    py_avg = pygnome.get("time_averaged_separation_km")
    out["opendrift_minus_pygnome_time_averaged_km"] = _finite_or_none(
        od_avg - py_avg if od_avg is not None and py_avg is not None else np.nan
    )
    out["opendrift_percent_improvement_vs_pygnome_time_averaged"] = _finite_or_none(
        _safe_percent_improvement(py_avg, od_avg)
    )
    for hour in HORIZONS_HOURS:
        od_value = opendrift.get(f"endpoint_{hour}h_km")
        py_value = pygnome.get(f"endpoint_{hour}h_km")
        out[f"opendrift_minus_pygnome_endpoint_{hour}h_km"] = _finite_or_none(
            od_value - py_value if od_value is not None and py_value is not None else np.nan
        )
        out[f"opendrift_percent_improvement_vs_pygnome_endpoint_{hour}h"] = _finite_or_none(
            _safe_percent_improvement(py_value, od_value)
        )
    return out


def _comparison_sentence(diff_km: float | None, metric_label: str) -> str:
    if diff_km is None:
        return f"OpenDrift and PyGNOME could not both be scored under {metric_label}."
    if abs(diff_km) < 0.005:
        return f"OpenDrift and PyGNOME were effectively tied under {metric_label}."
    direction = "nearer" if diff_km < 0 else "farther"
    return f"OpenDrift was {abs(diff_km):.2f} km {direction} than PyGNOME under {metric_label}."


def _score_winner(opendrift_value: float | None, pygnome_value: float | None, ensemble_value: float | None) -> str:
    candidates = {
        "OpenDrift deterministic": opendrift_value,
        "PyGNOME deterministic comparator": pygnome_value,
        "OpenDrift nearest ensemble member": ensemble_value,
    }
    valid = {key: float(value) for key, value in candidates.items() if value is not None and math.isfinite(float(value))}
    if not valid:
        return "No valid aligned distance."
    return min(valid, key=valid.get)


def _read_case_recipe(config_snapshot: Path | None) -> str:
    if config_snapshot is None or not config_snapshot.exists():
        return ""
    try:
        payload = yaml.safe_load(config_snapshot.read_text(encoding="utf-8")) or {}
    except Exception:
        return ""
    return str(payload.get("recipe") or "")


def _build_case_board(
    *,
    case_id: str,
    observed_eval: pd.DataFrame,
    opendrift_track: pd.DataFrame,
    pygnome_track: pd.DataFrame,
    member_tracks: list[tuple[str, pd.DataFrame]],
    row: dict[str, Any],
    probability_rows: list[dict[str, Any]],
    output_path: Path,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig = plt.figure(figsize=(14.4, 8.2), dpi=170, facecolor="white")
    grid = fig.add_gridspec(1, 2, width_ratios=[2.8, 1.25], left=0.055, right=0.985, top=0.90, bottom=0.085, wspace=0.18)
    ax = fig.add_subplot(grid[0, 0])
    text_ax = fig.add_subplot(grid[0, 1])
    ax.set_facecolor("#e9f6fb")

    all_lon: list[float] = []
    all_lat: list[float] = []

    def _plot_track(track: pd.DataFrame, color: str, label: str, lw: float, alpha: float, zorder: int, marker: str | None = None) -> None:
        clean = track.dropna(subset=["lon", "lat"]).sort_values("time")
        if clean.empty:
            return
        ax.plot(clean["lon"], clean["lat"], color=color, lw=lw, alpha=alpha, label=label, zorder=zorder)
        if marker:
            ax.scatter(clean["lon"].iloc[-1], clean["lat"].iloc[-1], color=color, s=52, marker=marker, zorder=zorder + 1)
        all_lon.extend(clean["lon"].astype(float).tolist())
        all_lat.extend(clean["lat"].astype(float).tolist())

    for idx, (_, member_track) in enumerate(member_tracks):
        label = "OpenDrift ensemble member median tracks" if idx == 0 else None
        _plot_track(member_track, "#0f766e", label or "", 0.7, 0.13, 2)

    start = observed_eval["time"].min()
    end = start + pd.Timedelta(hours=72)
    _plot_track(
        opendrift_track[(opendrift_track["time"] >= start) & (opendrift_track["time"] <= end)],
        "#155da8",
        "OpenDrift deterministic median",
        2.3,
        0.95,
        5,
        "s",
    )
    _plot_track(
        pygnome_track[(pygnome_track["time"] >= start) & (pygnome_track["time"] <= end)],
        "#7e22ce",
        "PyGNOME deterministic median",
        2.3,
        0.95,
        5,
        "^",
    )
    _plot_track(observed_eval, "#111827", "Observed drifter track", 2.8, 1.0, 7, "o")
    if not observed_eval.empty:
        ax.scatter(observed_eval["lon"].iloc[0], observed_eval["lat"].iloc[0], color="#f59e0b", s=95, marker="*", zorder=9, label="Observed release/start")

    if all_lon and all_lat:
        x_min, x_max = min(all_lon), max(all_lon)
        y_min, y_max = min(all_lat), max(all_lat)
        pad_x = max((x_max - x_min) * 0.18, 0.04)
        pad_y = max((y_max - y_min) * 0.18, 0.04)
        ax.set_xlim(x_min - pad_x, x_max + pad_x)
        ax.set_ylim(y_min - pad_y, y_max + pad_y)
        mean_lat = float(np.nanmean(all_lat))
        ax.set_aspect(1.0 / max(math.cos(math.radians(mean_lat)), 0.25), adjustable="box")
    ax.grid(color="white", linewidth=1.1)
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    ax.set_title(f"{case_id}: observed drifter reference vs stored model tracks", loc="left", fontsize=13, weight="bold")
    handles, labels = ax.get_legend_handles_labels()
    clean_handles: list[Any] = []
    clean_labels: list[str] = []
    seen: set[str] = set()
    for handle, label in zip(handles, labels):
        if label and label not in seen:
            clean_handles.append(handle)
            clean_labels.append(label)
            seen.add(label)
    ax.legend(clean_handles, clean_labels, loc="best", frameon=True, framealpha=0.92, fontsize=8.2)

    text_ax.axis("off")
    endpoint_lines = []
    for hour in HORIZONS_HOURS:
        endpoint_lines.append(
            f"{hour:>2} h  OD {_format_nullable(row[f'endpoint_{hour}h_opendrift_km'])} km | "
            f"PyG {_format_nullable(row[f'endpoint_{hour}h_pygnome_km'])} km | "
            f"Ens {_format_nullable(row[f'endpoint_{hour}h_nearest_member_km'])} km"
        )
    probability_lines = []
    for item in probability_rows:
        probability_lines.append(
            f"{int(item['hour']):>2} h  any={item['ensemble_contains_any']} "
            f"p50={item['ensemble_contains_p50']} "
            f"d_any={_format_nullable(item['distance_to_any_footprint_km'])} km"
        )
    winner = row["time_averaged_nearest_model"]
    score_lines = [
        "Scorecard",
        "",
        f"Reference: observed drifter {row['observed_drifter_id']}",
        f"Aligned observations: {row['observed_points_0_72h']} from 0-72 h",
        "",
        "Time-averaged separation",
        f"OD deterministic: {_format_nullable(row['time_averaged_opendrift_km'])} km",
        f"PyGNOME comparator: {_format_nullable(row['time_averaged_pygnome_km'])} km",
        f"Nearest OD ensemble member: {_format_nullable(row['time_averaged_nearest_member_km'])} km",
        "",
        _comparison_sentence(row["opendrift_minus_pygnome_time_averaged_km"], "time-averaged separation"),
        f"Nearest under this score: {winner}.",
        "",
        "Endpoints",
        *endpoint_lines,
        "",
        "Ensemble footprint diagnostic",
        *probability_lines,
        "",
        "Secondary support only; observed drifter is the reference.",
    ]
    text_ax.text(
        0.0,
        1.0,
        "\n".join(score_lines),
        ha="left",
        va="top",
        fontsize=9.2,
        color="#111827",
        linespacing=1.26,
        family="DejaVu Sans Mono",
    )
    fig.suptitle("Secondary 2016 Drifter-Track Benchmark", x=0.055, ha="left", fontsize=16, weight="bold")
    fig.text(
        0.055,
        0.035,
        "Model tracks are median active-particle positions interpolated to observed drifter timestamps. PyGNOME is a deterministic comparator.",
        ha="left",
        va="bottom",
        fontsize=8.4,
        color="#475569",
    )
    fig.savefig(output_path, bbox_inches="tight")
    plt.close(fig)


def _format_nullable(value: Any) -> str:
    numeric = _finite_or_none(value)
    return "n/a" if numeric is None else f"{numeric:.2f}"


def _benchmark_case(inputs: CaseInputs) -> tuple[dict[str, Any], dict[str, Any]]:
    observed, observed_meta = _load_observed_track(inputs.observed_csv)
    start_time = _utc_naive(observed["time"].iloc[0])
    end_time = start_time + pd.Timedelta(hours=72)
    observed_eval = observed[(observed["time"] >= start_time) & (observed["time"] <= end_time)].copy().reset_index(drop=True)
    observed_path_length_km = _track_length_km(observed_eval)

    opendrift_track = _median_active_track_from_opendrift(inputs.opendrift_nc, "OpenDrift deterministic")
    pygnome_track = _median_active_track_from_pygnome(inputs.pygnome_nc)
    member_tracks = [
        (member_path.stem.replace("member_", ""), _median_active_track_from_opendrift(member_path, member_path.stem))
        for member_path in inputs.ensemble_member_ncs
    ]

    od_at_obs = _interpolate_track(opendrift_track, observed_eval["time"])
    py_at_obs = _interpolate_track(pygnome_track, observed_eval["time"])
    od_sep = _separation_series(observed_eval, od_at_obs, "opendrift")
    py_sep = _separation_series(observed_eval, py_at_obs, "pygnome")
    opendrift_summary = _separation_summary(
        od_sep,
        "opendrift_separation_km",
        observed_path_length_km,
        start_time,
    )
    pygnome_summary = _separation_summary(
        py_sep,
        "pygnome_separation_km",
        observed_path_length_km,
        start_time,
    )

    nearest_df, nearest_meta = _nearest_member_distances(observed_eval, member_tracks)
    nearest_summary = _separation_summary(
        nearest_df.rename(columns={"nearest_member_distance_km": "ensemble_nearest_separation_km"}),
        "ensemble_nearest_separation_km",
        observed_path_length_km,
        start_time,
    )

    probability_rows: list[dict[str, Any]] = []
    for hour in HORIZONS_HOURS:
        target = start_time + pd.Timedelta(hours=hour)
        obs_at_target = observed_eval.loc[observed_eval["time"] == target]
        if obs_at_target.empty:
            obs_interp = _interpolate_track(observed_eval, [target])
            obs_lon = float(obs_interp["lon"].iloc[0])
            obs_lat = float(obs_interp["lat"].iloc[0])
        else:
            obs_lon = float(obs_at_target["lon"].iloc[0])
            obs_lat = float(obs_at_target["lat"].iloc[0])
        diag = _load_probability_diagnostic(inputs.probability_ncs.get(hour, Path()), obs_lon, obs_lat)
        probability_rows.append(
            {
                "case_id": inputs.case_id,
                "hour": hour,
                "timestamp_utc": _utc_iso(target),
                "observed_lon": obs_lon,
                "observed_lat": obs_lat,
                "probability_nc": _repo_rel(inputs.probability_ncs.get(hour)),
                **diag,
            }
        )

    difference = _model_difference_summary(opendrift_summary, pygnome_summary)
    row: dict[str, Any] = {
        "case_id": inputs.case_id,
        "benchmark_role": "secondary_support_only",
        "observed_reference": "NOAA observed drifter track",
        "pygnome_role": "deterministic_comparator_only",
        "observed_drifter_id": observed_meta["selected_drifter_id"],
        "start_time_utc": observed_meta["start_time_utc"],
        "start_lat": observed_meta["start_lat"],
        "start_lon": observed_meta["start_lon"],
        "observed_points_0_72h": int(len(observed_eval)),
        "observed_path_length_0_72h_km": observed_path_length_km,
        "opendrift_recipe": _read_case_recipe(inputs.config_snapshot),
        "ensemble_member_count": len(member_tracks),
        "time_averaged_opendrift_km": opendrift_summary["time_averaged_separation_km"],
        "time_averaged_pygnome_km": pygnome_summary["time_averaged_separation_km"],
        "time_averaged_nearest_member_km": nearest_summary["time_averaged_separation_km"],
        "normalized_cumulative_separation_opendrift": opendrift_summary["normalized_cumulative_separation"],
        "normalized_cumulative_separation_pygnome": pygnome_summary["normalized_cumulative_separation"],
        "normalized_cumulative_separation_nearest_member": nearest_summary["normalized_cumulative_separation"],
        "opendrift_valid_aligned_count": opendrift_summary["valid_aligned_count"],
        "pygnome_valid_aligned_count": pygnome_summary["valid_aligned_count"],
        "nearest_member_valid_aligned_count": nearest_meta["valid_aligned_count"],
        **difference,
        "time_averaged_nearest_model": _score_winner(
            opendrift_summary["time_averaged_separation_km"],
            pygnome_summary["time_averaged_separation_km"],
            nearest_summary["time_averaged_separation_km"],
        ),
        "opendrift_nc": _repo_rel(inputs.opendrift_nc),
        "pygnome_nc": _repo_rel(inputs.pygnome_nc),
        "observed_csv": _repo_rel(inputs.observed_csv),
    }
    for hour in HORIZONS_HOURS:
        row[f"endpoint_{hour}h_opendrift_km"] = opendrift_summary[f"endpoint_{hour}h_km"]
        row[f"endpoint_{hour}h_pygnome_km"] = pygnome_summary[f"endpoint_{hour}h_km"]
        row[f"endpoint_{hour}h_nearest_member_km"] = nearest_summary[f"endpoint_{hour}h_km"]
        row[f"endpoint_{hour}h_nearest_model"] = _score_winner(
            row[f"endpoint_{hour}h_opendrift_km"],
            row[f"endpoint_{hour}h_pygnome_km"],
            row[f"endpoint_{hour}h_nearest_member_km"],
        )
        prob = next(item for item in probability_rows if int(item["hour"]) == hour)
        row[f"endpoint_{hour}h_ensemble_probability_at_observed_cell"] = prob["probability_at_observed_cell"]
        row[f"endpoint_{hour}h_ensemble_contains_any"] = prob["ensemble_contains_any"]
        row[f"endpoint_{hour}h_ensemble_contains_p50"] = prob["ensemble_contains_p50"]
        row[f"endpoint_{hour}h_ensemble_contains_p90"] = prob["ensemble_contains_p90"]
        row[f"endpoint_{hour}h_distance_to_any_ensemble_footprint_km"] = prob["distance_to_any_footprint_km"]

    board_path = CASE_BOARDS_DIR / f"{inputs.case_id}_drifter_track_benchmark.png"
    _build_case_board(
        case_id=inputs.case_id,
        observed_eval=observed_eval,
        opendrift_track=opendrift_track,
        pygnome_track=pygnome_track,
        member_tracks=member_tracks,
        row=row,
        probability_rows=probability_rows,
        output_path=board_path,
    )
    row["case_board_png"] = _repo_rel(board_path)

    detail = {
        "case_id": inputs.case_id,
        "inputs": {
            "observed_csv": _repo_rel(inputs.observed_csv),
            "opendrift_nc": _repo_rel(inputs.opendrift_nc),
            "pygnome_nc": _repo_rel(inputs.pygnome_nc),
            "ensemble_members": [_repo_rel(path) for path in inputs.ensemble_member_ncs],
            "probability_ncs": {str(hour): _repo_rel(path) for hour, path in inputs.probability_ncs.items()},
            "benchmark_dir": _repo_rel(inputs.benchmark_dir),
            "config_snapshot": _repo_rel(inputs.config_snapshot),
        },
        "observed": observed_meta,
        "metrics": {
            "opendrift": opendrift_summary,
            "pygnome": pygnome_summary,
            "nearest_member": nearest_summary,
            "opendrift_minus_pygnome": difference,
            "probability_footprint": probability_rows,
        },
        "aligned_separations": {
            "opendrift": od_sep[["time", "observed_lon", "observed_lat", "opendrift_lon", "opendrift_lat", "opendrift_separation_km"]].to_dict(orient="records"),
            "pygnome": py_sep[["time", "observed_lon", "observed_lat", "pygnome_lon", "pygnome_lat", "pygnome_separation_km"]].to_dict(orient="records"),
            "nearest_member": nearest_df.to_dict(orient="records"),
        },
        "case_board_png": _repo_rel(board_path),
    }
    return row, detail


def _aggregate_interpretation(rows: list[dict[str, Any]]) -> dict[str, Any]:
    od_values = [row["time_averaged_opendrift_km"] for row in rows if row.get("time_averaged_opendrift_km") is not None]
    py_values = [row["time_averaged_pygnome_km"] for row in rows if row.get("time_averaged_pygnome_km") is not None]
    if not od_values or not py_values:
        return {
            "metric": "case-mean time-averaged separation",
            "opendrift_mean_km": None,
            "pygnome_mean_km": None,
            "difference_km": None,
            "sentence": "Across the 2016 drifter-track benchmark, the model comparison could not be summarized under case-mean time-averaged separation. This is a secondary drifter-track benchmark and does not replace the primary Mindoro public-observation validation.",
        }
    od_mean = float(np.mean(od_values))
    py_mean = float(np.mean(py_values))
    diff = od_mean - py_mean
    direction = "nearer" if diff < 0 else "farther"
    sentence = (
        "Across the 2016 drifter-track benchmark, the OpenDrift promoted configuration was "
        f"{abs(diff):.2f} km {direction} than the PyGNOME deterministic comparator under "
        "case-mean time-averaged separation. This is a secondary drifter-track benchmark and "
        "does not replace the primary Mindoro public-observation validation."
    )
    return {
        "metric": "case-mean time-averaged separation",
        "opendrift_mean_km": od_mean,
        "pygnome_mean_km": py_mean,
        "difference_km": diff,
        "direction": direction,
        "sentence": sentence,
    }


def _scorecard_columns() -> list[str]:
    columns = [
        "case_id",
        "benchmark_role",
        "observed_reference",
        "pygnome_role",
        "observed_drifter_id",
        "start_time_utc",
        "start_lat",
        "start_lon",
        "observed_points_0_72h",
        "observed_path_length_0_72h_km",
        "opendrift_recipe",
        "ensemble_member_count",
        "time_averaged_opendrift_km",
        "time_averaged_pygnome_km",
        "time_averaged_nearest_member_km",
        "opendrift_minus_pygnome_time_averaged_km",
        "opendrift_percent_improvement_vs_pygnome_time_averaged",
        "normalized_cumulative_separation_opendrift",
        "normalized_cumulative_separation_pygnome",
        "normalized_cumulative_separation_nearest_member",
        "time_averaged_nearest_model",
    ]
    for hour in HORIZONS_HOURS:
        columns.extend(
            [
                f"endpoint_{hour}h_opendrift_km",
                f"endpoint_{hour}h_pygnome_km",
                f"endpoint_{hour}h_nearest_member_km",
                f"opendrift_minus_pygnome_endpoint_{hour}h_km",
                f"opendrift_percent_improvement_vs_pygnome_endpoint_{hour}h",
                f"endpoint_{hour}h_nearest_model",
                f"endpoint_{hour}h_ensemble_probability_at_observed_cell",
                f"endpoint_{hour}h_ensemble_contains_any",
                f"endpoint_{hour}h_ensemble_contains_p50",
                f"endpoint_{hour}h_ensemble_contains_p90",
                f"endpoint_{hour}h_distance_to_any_ensemble_footprint_km",
            ]
        )
    columns.extend(
        [
            "opendrift_valid_aligned_count",
            "pygnome_valid_aligned_count",
            "nearest_member_valid_aligned_count",
            "observed_csv",
            "opendrift_nc",
            "pygnome_nc",
            "case_board_png",
        ]
    )
    return columns


def _build_readme(rows: list[dict[str, Any]], interpretation: dict[str, Any]) -> str:
    lines = [
        "# Secondary 2016 Drifter-Track Benchmark",
        "",
        interpretation["sentence"],
        "",
        "## Scope",
        "",
        "This package compares stored 2016 observed drifter tracks with matched stored OpenDrift and PyGNOME outputs. The observed drifter track is the reference. PyGNOME is a deterministic comparator, and the OpenDrift ensemble diagnostics are support diagnostics.",
        "",
        "This package is secondary support only. It does not modify or replace the primary Mindoro March 13-14 public-observation validation outputs, and it does not modify DWH outputs.",
        "",
        "## Methods",
        "",
        "- Existing internal case IDs are preserved.",
        "- Observed tracks use the repository's drifter-of-record selection rule from `data/drifters/<case_id>/drifters_noaa.csv`.",
        "- Deterministic model tracks use the median active-particle position at each model output time.",
        "- Model tracks are linearly interpolated to observed drifter timestamps in the 0-72 h window.",
        "- Time-averaged separation is the mean great-circle distance over aligned observed timestamps.",
        "- Normalized cumulative separation is the sum of aligned separation distances divided by the observed cumulative 0-72 h track length.",
        "- Ensemble nearest-member distance uses each ensemble member's median active-particle track.",
        "- Ensemble footprint diagnostics sample stored member-occupancy probability rasters at 24 h, 48 h, and 72 h.",
        "",
        "## Cases",
        "",
    ]
    for row in rows:
        lines.append(
            f"- `{row['case_id']}`: time-averaged OD {_format_nullable(row['time_averaged_opendrift_km'])} km, "
            f"PyGNOME {_format_nullable(row['time_averaged_pygnome_km'])} km, "
            f"nearest ensemble member {_format_nullable(row['time_averaged_nearest_member_km'])} km. "
            f"Board: `{row['case_board_png']}`."
        )
    lines.extend(
        [
            "",
            "## Outputs",
            "",
            "- `scorecard.csv`",
            "- `scorecard.json`",
            "- `case_boards/*.png`",
            "- `manifest.json`",
            "- `README.md`",
        ]
    )
    return "\n".join(lines)


def build_benchmark() -> dict[str, Any]:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    CASE_BOARDS_DIR.mkdir(parents=True, exist_ok=True)
    case_ids = _discover_cases()
    rows: list[dict[str, Any]] = []
    details: list[dict[str, Any]] = []
    skipped: list[dict[str, str]] = []
    for case_id in case_ids:
        try:
            row, detail = _benchmark_case(_resolve_case_inputs(case_id))
        except Exception as exc:
            skipped.append({"case_id": case_id, "reason": str(exc)})
            continue
        rows.append(row)
        details.append(detail)

    if not rows:
        raise RuntimeError(f"No 2016 drifter benchmark cases were built. Skipped: {skipped}")

    columns = _scorecard_columns()
    scorecard_df = pd.DataFrame(rows)
    for column in columns:
        if column not in scorecard_df.columns:
            scorecard_df[column] = None
    scorecard_df = scorecard_df[columns]
    scorecard_csv = OUTPUT_DIR / "scorecard.csv"
    scorecard_json = OUTPUT_DIR / "scorecard.json"
    scorecard_df.to_csv(scorecard_csv, index=False)
    scorecard_records = scorecard_df.where(pd.notna(scorecard_df), None).to_dict(orient="records")
    _write_json(scorecard_json, {"cases": scorecard_records, "details": details})

    interpretation = _aggregate_interpretation(rows)
    manifest = {
        "manifest_type": "secondary_2016_drifter_track_benchmark",
        "generated_at_utc": pd.Timestamp.now(tz="UTC").strftime("%Y-%m-%dT%H:%M:%SZ"),
        "benchmark_role": "secondary_support_only",
        "reference": "observed NOAA drifter track",
        "pygnome_role": "deterministic_comparator_only",
        "case_ids": [row["case_id"] for row in rows],
        "skipped_cases": skipped,
        "metric_definitions": {
            "endpoint_distance": "Great-circle distance between observed drifter position and model representative position at 24 h, 48 h, and 72 h.",
            "time_averaged_separation": "Mean great-circle distance over observed timestamps from 0 h through 72 h.",
            "normalized_cumulative_separation": "Sum of aligned separation distances divided by observed cumulative 0-72 h track length.",
            "nearest_member_distance": "Minimum distance from the observed drifter to any OpenDrift ensemble member median track at the same observed timestamp.",
            "opendrift_minus_pygnome": "OpenDrift deterministic separation minus PyGNOME deterministic comparator separation; negative values mean OpenDrift is nearer.",
            "percent_improvement_vs_pygnome": "(PyGNOME comparator separation minus OpenDrift separation) divided by PyGNOME comparator separation.",
        },
        "interpretation": interpretation,
        "outputs": {
            "scorecard_csv": _repo_rel(scorecard_csv),
            "scorecard_json": _repo_rel(scorecard_json),
            "case_boards_dir": _repo_rel(CASE_BOARDS_DIR),
            "readme": _repo_rel(OUTPUT_DIR / "README.md"),
        },
        "cases": details,
    }
    manifest_json = OUTPUT_DIR / "manifest.json"
    _write_json(manifest_json, manifest)
    _write_text(OUTPUT_DIR / "README.md", _build_readme(rows, interpretation))
    return manifest


if __name__ == "__main__":
    result = build_benchmark()
    print(json.dumps({"output_dir": _repo_rel(OUTPUT_DIR), "case_count": len(result["case_ids"])}, indent=2))
