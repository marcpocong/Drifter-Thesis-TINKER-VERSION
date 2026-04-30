"""Experimental Mindoro PhilSA March 3 -> March 4 PyGNOME comparator.

This script is intentionally direct-run only.  It must be executed in the
`gnome` Docker service where PyGNOME is installed.  It reuses the completed
experimental March 3 -> March 4 PhilSA/OpenDrift outputs and writes only into
that same experimental archive directory.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import geopandas as gpd
import matplotlib
import netCDF4
import numpy as np
import pandas as pd
import rasterio

matplotlib.use("Agg")
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
from matplotlib.colors import ListedColormap

from src.helpers.metrics import calculate_fss
from src.helpers.raster import GridBuilder, extract_particles_at_time, rasterize_particles, save_raster
from src.helpers.scoring import apply_ocean_mask, load_sea_mask_array, precheck_same_grid
from src.services.gnome_comparison import GNOME_AVAILABLE, GnomeComparisonService
from src.services.scoring import OFFICIAL_PHASE3B_WINDOWS_KM, Phase3BScoringService


BASE = Path("output/CASE_MINDORO_RETRO_2023/phase3b_philsa_march3_4_5000_experiment")
PYGNOME_DIR = BASE / "pygnome_comparator"
PRODUCTS_DIR = PYGNOME_DIR / "products"
QA_DIR = PYGNOME_DIR / "qa"
PRECHECK_DIR = PYGNOME_DIR / "precheck"
MODEL_DIR = PYGNOME_DIR / "model_run"

TARGET_DATE = "2023-03-04"
SIMULATION_START_UTC = "2023-03-02T16:00:00Z"
DURATION_HOURS = 48
TIME_STEP_MINUTES = 60
REQUESTED_ELEMENT_COUNT = 5000
RANDOM_SEED = 20230303

PYGNOME_NC_NAME = "pygnome_march3_4_philsa_5000_deterministic_control.nc"
PYGNOME_FOOTPRINT_NAME = "pygnome_footprint_mask_2023-03-04_localdate.tif"
PYGNOME_DENSITY_NAME = "pygnome_density_norm_2023-03-04_localdate.tif"

PROTECTED_PATHS = [
    Path("output/final_validation_package"),
    Path("output/figure_package_publication"),
    Path("output/final_reproducibility_package"),
    Path("output/Phase 3B March13-14 Final Output"),
    Path("output/CASE_MINDORO_RETRO_2023/phase3b_extended_public_scored_march13_14_reinit"),
    Path("output/CASE_MINDORO_RETRO_2023/phase3b_extended_public_scored_march13_14_reinit_pygnome_comparison"),
    Path("output/CASE_MINDORO_RETRO_2023/phase3b"),
    Path("output/CASE_MINDORO_RETRO_2023/phase3b_multidate_public"),
    Path("output/CASE_MINDORO_RETRO_2023/public_obs_appendix"),
]


def _json_default(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return None if np.isnan(value) else float(value)
    if isinstance(value, (datetime, pd.Timestamp)):
        return value.isoformat()
    if isinstance(value, np.ndarray):
        return value.tolist()
    return str(value)


def _write_json(path: Path, payload: dict | list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, default=_json_default) + "\n", encoding="utf-8")


def _write_csv(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False)


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _snapshot_paths(paths: list[Path]) -> dict[str, str]:
    snapshot: dict[str, str] = {}
    for root in paths:
        if not root.exists():
            snapshot[str(root)] = "<missing>"
            continue
        if root.is_file():
            snapshot[str(root)] = _hash_file(root)
            continue
        for file_path in sorted(path for path in root.rglob("*") if path.is_file()):
            snapshot[str(file_path)] = _hash_file(file_path)
    return snapshot


def _iso_z(value: Any) -> str:
    ts = pd.Timestamp(value)
    if ts.tzinfo is not None:
        ts = ts.tz_convert("UTC").tz_localize(None)
    return ts.strftime("%Y-%m-%dT%H:%M:%SZ")


def _read_raster(path: Path) -> np.ndarray:
    with rasterio.open(path) as src:
        return src.read(1).astype(np.float32)


def _load_required_context() -> dict[str, Any]:
    required = {
        "run_manifest": BASE / "march3_4_philsa_5000_run_manifest.json",
        "observation_manifest": BASE / "march3_4_philsa_5000_observation_manifest.csv",
        "opendrift_fss": BASE / "march3_4_philsa_5000_fss_by_window.csv",
        "opendrift_diagnostics": BASE / "march3_4_philsa_5000_diagnostics.csv",
        "opendrift_summary": BASE / "march3_4_philsa_5000_summary.csv",
        "opendrift_branch_survival": BASE / "march3_4_philsa_5000_branch_survival_summary.csv",
        "target_mask": BASE / "march4_target_mask_on_grid.tif",
        "seed_mask": BASE / "march3_seed_mask_on_grid.tif",
        "opendrift_r1_p50": BASE / "R1_previous/forecast_datecomposites/mask_p50_2023-03-04_localdate.tif",
        "opendrift_r1_p90": BASE / "R1_previous/forecast_datecomposites/mask_p90_2023-03-04_localdate.tif",
        "opendrift_r1_control": BASE / "R1_previous/forecast_datecomposites/control_footprint_2023-03-04_localdate.tif",
    }
    missing = [str(path) for path in required.values() if not path.exists()]
    if missing:
        raise FileNotFoundError("Missing completed experimental OpenDrift artifacts: " + "; ".join(missing))

    run_manifest = json.loads(required["run_manifest"].read_text(encoding="utf-8"))
    if int(run_manifest.get("element_count_detected_from_manifest") or 0) != REQUESTED_ELEMENT_COUNT:
        raise RuntimeError("Upstream experimental OpenDrift run is not the required 5,000-element run.")

    observation_manifest = pd.read_csv(required["observation_manifest"])
    providers = set(observation_manifest["provider"].astype(str).str.lower())
    if providers != {"philsa"}:
        raise RuntimeError(f"Expected only PhilSA observation providers, found: {sorted(providers)}")

    seed_row = observation_manifest[observation_manifest["role"] == "seed_initialization_layer"].iloc[0]
    target_row = observation_manifest[observation_manifest["role"] == "target_validation_layer"].iloc[0]
    if str(seed_row["observation_date"]) != "2023-03-03" or str(target_row["observation_date"]) != TARGET_DATE:
        raise RuntimeError("Unexpected observation dates in experimental source manifest.")
    if str(seed_row["source_name"]) != "MindoroOilSpill_Philsa_230303":
        raise RuntimeError("Seed source is not the required March 3 PhilSA layer.")
    if str(target_row["source_name"]) != "MindoroOilSpill_Philsa_230304":
        raise RuntimeError("Target source is not the required March 4 PhilSA layer.")

    seed_vector = Path(str(seed_row["processed_vector"]))
    if not seed_vector.exists():
        raise FileNotFoundError(f"Seed processed vector not found: {seed_vector}")

    return {
        **required,
        "run_manifest": run_manifest,
        "observation_manifest_df": observation_manifest,
        "seed_row": seed_row.to_dict(),
        "target_row": target_row.to_dict(),
        "seed_vector": seed_vector,
    }


def _reference_point_from_vector(vector_path: Path) -> tuple[float, float]:
    gdf = gpd.read_file(vector_path).dropna(subset=["geometry"])
    if gdf.empty:
        raise RuntimeError(f"No valid seed geometry found in {vector_path}")
    if gdf.crs is not None and str(gdf.crs).upper() != "EPSG:4326":
        gdf = gdf.to_crs("EPSG:4326")
    point = gdf.geometry.unary_union.representative_point()
    return float(point.y), float(point.x)


def _pygnome_times(nc_path: Path) -> list[pd.Timestamp]:
    with netCDF4.Dataset(nc_path) as nc:
        raw_times = netCDF4.num2date(
            nc.variables["time"][:],
            nc.variables["time"].units,
            only_use_cftime_datetimes=False,
            only_use_python_datetimes=True,
        )
    times = pd.DatetimeIndex(pd.to_datetime(raw_times))
    if times.tz is not None:
        times = times.tz_convert("UTC").tz_localize(None)
    return [pd.Timestamp(value) for value in times]


def _build_pygnome_local_date_products(
    *,
    grid: GridBuilder,
    sea_mask: np.ndarray | None,
    nc_path: Path,
    out_dir: Path,
) -> dict[str, Any]:
    footprint_path = out_dir / PYGNOME_FOOTPRINT_NAME
    density_path = out_dir / PYGNOME_DENSITY_NAME
    footprint = np.zeros((grid.height, grid.width), dtype=np.float32)
    density = np.zeros((grid.height, grid.width), dtype=np.float32)
    target_timestamps_seen: set[str] = set()
    target_active_timestamps: set[str] = set()
    last_active_time: pd.Timestamp | None = None

    for timestamp in _pygnome_times(nc_path):
        local_date = pd.Timestamp(timestamp).tz_localize("UTC").tz_convert("Asia/Manila").date().isoformat()
        if local_date == TARGET_DATE:
            target_timestamps_seen.add(_iso_z(timestamp))
        try:
            lon, lat, mass, actual_time, _ = extract_particles_at_time(
                nc_path,
                timestamp,
                "pygnome",
                allow_uniform_mass_fallback=True,
            )
        except Exception:
            continue
        if len(lon) == 0:
            continue
        last_active_time = actual_time if last_active_time is None else max(last_active_time, actual_time)
        if local_date != TARGET_DATE:
            continue
        target_active_timestamps.add(_iso_z(actual_time))
        hits, probs = rasterize_particles(grid, lon, lat, mass)
        footprint = np.maximum(footprint, hits.astype(np.float32))
        density = np.maximum(density, probs.astype(np.float32))

    footprint = apply_ocean_mask(footprint, sea_mask=sea_mask, fill_value=0.0)
    density = apply_ocean_mask(density, sea_mask=sea_mask, fill_value=0.0)
    save_raster(grid, footprint.astype(np.float32), footprint_path)
    save_raster(grid, density.astype(np.float32), density_path)

    nonzero = int(np.count_nonzero(footprint > 0))
    if nonzero > 0:
        empty_reason = ""
    elif not target_timestamps_seen:
        empty_reason = "pygnome_survival_did_not_reach_march4_local_date"
    elif not target_active_timestamps:
        empty_reason = "pygnome_march4_local_timestamps_present_but_no_active_particles"
    else:
        empty_reason = "pygnome_local_activity_present_but_no_scoreable_ocean_presence_after_masking"

    return {
        "forecast_path": footprint_path,
        "density_path": density_path,
        "forecast_nonzero_cells": nonzero,
        "last_active_particle_time_utc": _iso_z(last_active_time) if last_active_time is not None else "",
        "march4_local_timestamp_count": int(len(target_timestamps_seen)),
        "march4_local_active_timestamp_count": int(len(target_active_timestamps)),
        "march4_local_timestamps": ";".join(sorted(target_timestamps_seen)),
        "march4_local_active_timestamps": ";".join(sorted(target_active_timestamps)),
        "reached_march4_local_date": bool(target_timestamps_seen),
        "empty_forecast_reason": empty_reason,
    }


def _score_pygnome(footprint_path: Path, target_mask_path: Path, valid_mask: np.ndarray | None) -> tuple[pd.DataFrame, pd.DataFrame]:
    helper = Phase3BScoringService(output_dir=PYGNOME_DIR / "_scratch_scoring")
    precheck = precheck_same_grid(
        forecast=footprint_path,
        target=target_mask_path,
        report_base_path=PRECHECK_DIR / "march4_philsa_pygnome_deterministic",
    )
    if not precheck.passed:
        raise RuntimeError(f"PyGNOME same-grid precheck failed: {precheck.json_report_path}")

    forecast = helper._load_binary_score_mask(footprint_path)
    obs = helper._load_binary_score_mask(target_mask_path)
    diagnostics = helper._compute_mask_diagnostics(forecast, obs)
    diagnostics_row = {
        "pair_id": "march4_philsa_pygnome_deterministic",
        "track_id": "pygnome_deterministic",
        "model_family": "PyGNOME",
        "obs_date": TARGET_DATE,
        "forecast_path": str(footprint_path),
        "observation_path": str(target_mask_path),
        "precheck_csv": str(precheck.csv_report_path),
        "precheck_json": str(precheck.json_report_path),
        **diagnostics,
    }

    fss_rows = []
    for window_km in OFFICIAL_PHASE3B_WINDOWS_KM:
        fss = float(
            np.clip(
                calculate_fss(
                    forecast,
                    obs,
                    window=helper._window_km_to_cells(window_km),
                    valid_mask=valid_mask,
                ),
                0.0,
                1.0,
            )
        )
        fss_rows.append(
            {
                "pair_id": "march4_philsa_pygnome_deterministic",
                "track_id": "pygnome_deterministic",
                "model_family": "PyGNOME",
                "obs_date": TARGET_DATE,
                "window_km": int(window_km),
                "window_cells": int(helper._window_km_to_cells(window_km)),
                "fss": fss,
                "forecast_path": str(footprint_path),
                "observation_path": str(target_mask_path),
            }
        )

    return pd.DataFrame(fss_rows), pd.DataFrame([diagnostics_row])


def _copy_opendrift_score_rows(context: dict[str, Any]) -> tuple[pd.DataFrame, pd.DataFrame]:
    fss = pd.read_csv(context["opendrift_fss"]).copy()
    diagnostics = pd.read_csv(context["opendrift_diagnostics"]).copy()
    if "track_id" in fss.columns:
        fss["opendrift_registry_track_id"] = fss["track_id"]
    if "branch_id" in fss.columns:
        fss["track_id"] = fss["branch_id"].astype(str)
    if "track_id" in diagnostics.columns:
        diagnostics["opendrift_registry_track_id"] = diagnostics["track_id"]
    if "branch_id" in diagnostics.columns:
        diagnostics["track_id"] = diagnostics["branch_id"].astype(str)
    fss = fss.loc[:, ~fss.columns.duplicated()].copy()
    diagnostics = diagnostics.loc[:, ~diagnostics.columns.duplicated()].copy()
    fss["model_family"] = "OpenDrift"
    diagnostics["model_family"] = "OpenDrift"
    return fss, diagnostics


def _write_qa_overlay(target_path: Path, pygnome_path: Path) -> Path:
    target = _read_raster(target_path)
    pygnome = _read_raster(pygnome_path)
    path = QA_DIR / "qa_march4_philsa_pygnome_overlay.png"
    fig, ax = plt.subplots(figsize=(7, 7), dpi=150)
    ax.set_facecolor("#eaf4ff")
    ax.imshow(np.ma.masked_where(target <= 0, target), cmap=ListedColormap(["#2563eb"]), alpha=0.65)
    ax.imshow(np.ma.masked_where(pygnome <= 0, pygnome), cmap=ListedColormap(["#16a34a"]), alpha=0.85)
    ax.set_title("March 4 PhilSA Target vs PyGNOME Deterministic")
    ax.axis("off")
    ax.legend(
        handles=[
            mpatches.Patch(color="#2563eb", label="March 4 PhilSA target"),
            mpatches.Patch(color="#16a34a", label="PyGNOME deterministic"),
        ],
        loc="lower left",
    )
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)
    return path


def _make_combined_board(
    *,
    context: dict[str, Any],
    pygnome_footprint: Path,
    pygnome_density: Path,
    combined_fss: pd.DataFrame,
    combined_diag: pd.DataFrame,
    py_metadata: dict[str, Any],
    out_path: Path,
) -> None:
    rasters = {
        "seed": _read_raster(context["seed_mask"]),
        "target": _read_raster(context["target_mask"]),
        "opendrift_r1": _read_raster(context["opendrift_r1_p50"]),
        "opendrift_r1_p90": _read_raster(context["opendrift_r1_p90"]),
        "opendrift_control": _read_raster(context["opendrift_r1_control"]),
        "pygnome": _read_raster(pygnome_footprint),
        "pygnome_density": _read_raster(pygnome_density),
    }
    stack = np.zeros_like(rasters["target"], dtype=bool)
    for arr in rasters.values():
        stack |= arr > 0
    rows, cols = np.where(stack)
    if rows.size == 0:
        r0, r1, c0, c1 = 0, stack.shape[0], 0, stack.shape[1]
    else:
        pad = 10
        r0, r1 = max(0, rows.min() - pad), min(stack.shape[0], rows.max() + pad + 1)
        c0, c1 = max(0, cols.min() - pad), min(stack.shape[1], cols.max() + pad + 1)
        height, width = r1 - r0, c1 - c0
        if width < height:
            extra = height - width
            c0 = max(0, c0 - extra // 2)
            c1 = min(stack.shape[1], c1 + extra - extra // 2)
        elif height < width:
            extra = width - height
            r0 = max(0, r0 - extra // 2)
            r1 = min(stack.shape[0], r1 + extra - extra // 2)
    height, width = r1 - r0, c1 - c0

    def crop(arr: np.ndarray) -> np.ndarray:
        return arr[r0:r1, c0:c1]

    def ma(arr: np.ndarray) -> np.ma.MaskedArray:
        cropped = crop(arr)
        return np.ma.masked_where(cropped <= 0, cropped)

    target_c = "#2563eb"
    seed_c = "#f59e0b"
    opendrift_c = "#dc2626"
    pygnome_c = "#16a34a"
    p90_c = "#7c3aed"
    control_c = "#111827"
    density_cmap = plt.cm.YlGn.copy()
    density_cmap.set_bad((1, 1, 1, 0))

    od_r1_fss = combined_fss[combined_fss["track_id"] == "R1_previous"].sort_values("window_km")
    py_fss = combined_fss[combined_fss["track_id"] == "pygnome_deterministic"].sort_values("window_km")
    od_r1_diag = combined_diag[combined_diag["track_id"] == "R1_previous"].iloc[0]
    py_diag = combined_diag[combined_diag["track_id"] == "pygnome_deterministic"].iloc[0]
    py_mean = float(py_fss["fss"].mean()) if not py_fss.empty else 0.0
    od_mean = float(od_r1_fss["fss"].mean()) if not od_r1_fss.empty else 0.0

    fig = plt.figure(figsize=(17, 10.5), dpi=170, facecolor="#f8fafc")
    fig.text(0.04, 0.955, "Mindoro PhilSA Mar 3 -> Mar 4 Experimental Test with PyGNOME",
             fontsize=23, fontweight="bold", color="#0f172a", ha="left", va="top")
    fig.text(0.04, 0.915,
             "OpenDrift 5,000-element / 50-member run plus deterministic PyGNOME comparator | PhilSA seed and target | not thesis-facing",
             fontsize=12, color="#475569", ha="left", va="top")

    box = mpatches.FancyBboxPatch((0.705, 0.888), 0.265, 0.085, transform=fig.transFigure,
                                  boxstyle="round,pad=0.012,rounding_size=0.012",
                                  facecolor="white", edgecolor="#dbe3ee", linewidth=1.0)
    fig.add_artist(box)
    fig.text(0.72, 0.952, "Comparator summary", fontsize=10.5, color="#64748b", ha="left", va="top")
    fig.text(0.72, 0.928, f"OpenDrift R1\nmean FSS {od_mean:.4f}", fontsize=10.8, fontweight="bold", color="#0f172a", ha="left", va="top")
    fig.text(0.845, 0.928, f"PyGNOME\nmean FSS {py_mean:.4f}\nnearest {float(py_diag.nearest_distance_to_obs_m) / 1000:.2f} km",
             fontsize=9.8, color="#334155", ha="left", va="top")

    legend = [
        mpatches.Patch(color=seed_c, label="Mar 3 PhilSA seed"),
        mpatches.Patch(color=target_c, label="Mar 4 PhilSA target"),
        mpatches.Patch(color=opendrift_c, label="OpenDrift R1 p50"),
        mpatches.Patch(color=pygnome_c, label="PyGNOME deterministic"),
        mpatches.Patch(color=p90_c, label="OpenDrift p90"),
        mpatches.Patch(color=control_c, label="OpenDrift control"),
    ]
    fig.legend(handles=legend, loc="upper center", bbox_to_anchor=(0.5, 0.855), ncol=6, frameon=False, fontsize=9.8)

    positions = [
        (0.035, 0.342, 0.225, 0.44),
        (0.275, 0.342, 0.225, 0.44),
        (0.515, 0.342, 0.225, 0.44),
        (0.755, 0.342, 0.225, 0.44),
    ]
    axes = [fig.add_axes(pos) for pos in positions]

    def style(ax: plt.Axes, title: str, subtitle: str) -> None:
        ax.set_title(title, fontsize=13.2, fontweight="bold", color="#0f172a", pad=20)
        ax.text(0.0, 1.025, subtitle, transform=ax.transAxes, fontsize=8.8, color="#64748b", ha="left", va="bottom")
        ax.set_facecolor("#eaf4ff")
        ax.set_xlim(-0.5, width - 0.5)
        ax.set_ylim(height - 0.5, -0.5)
        ax.set_aspect("equal")
        major = 5
        xt = np.arange(0, width, major)
        yt = np.arange(0, height, major)
        ax.set_xticks(xt)
        ax.set_yticks(yt)
        ax.set_xticklabels([f"{int(x)}" for x in xt], fontsize=7.5, color="#64748b")
        ax.set_yticklabels([f"{int(y)}" for y in yt], fontsize=7.5, color="#64748b")
        ax.grid(color="white", linewidth=0.8, alpha=0.85)
        ax.set_xlabel("km east in cropped view", fontsize=7.8, color="#64748b")
        ax.set_ylabel("km south in cropped view", fontsize=7.8, color="#64748b")
        for spine in ax.spines.values():
            spine.set_color("#cbd5e1")

    ax = axes[0]
    style(ax, "Observation Pair", "Separate dated PhilSA polygon layers")
    ax.imshow(ma(rasters["target"]), origin="upper", interpolation="nearest", cmap=ListedColormap([target_c]), alpha=0.88)
    ax.imshow(ma(rasters["seed"]), origin="upper", interpolation="nearest", cmap=ListedColormap([seed_c]), alpha=0.78)
    ax.text(0.03, 0.04, "Seed: Mar 3 PhilSA\nTarget: Mar 4 PhilSA", transform=ax.transAxes,
            fontsize=8.4, color="#0f172a",
            bbox=dict(facecolor="white", edgecolor="#cbd5e1", alpha=0.95, boxstyle="round,pad=0.35"))

    ax = axes[1]
    style(ax, "OpenDrift R1_previous", "p50 forecast vs March 4 target")
    ax.imshow(ma(rasters["target"]), origin="upper", interpolation="nearest", cmap=ListedColormap([target_c]), alpha=0.58)
    ax.imshow(ma(rasters["opendrift_r1"]), origin="upper", interpolation="nearest", cmap=ListedColormap([opendrift_c]), alpha=0.95)
    ax.imshow(ma(rasters["opendrift_r1_p90"]), origin="upper", interpolation="nearest", cmap=ListedColormap([p90_c]), alpha=0.95)
    ax.imshow(ma(rasters["opendrift_control"]), origin="upper", interpolation="nearest", cmap=ListedColormap([control_c]), alpha=0.55)
    ax.text(0.03, 0.04,
            f"p50 cells: {int(od_r1_diag.forecast_nonzero_cells)} | obs: {int(od_r1_diag.obs_nonzero_cells)}\nNearest miss: {float(od_r1_diag.nearest_distance_to_obs_m) / 1000:.2f} km",
            transform=ax.transAxes, fontsize=8.4, color="#0f172a",
            bbox=dict(facecolor="white", edgecolor="#cbd5e1", alpha=0.95, boxstyle="round,pad=0.35"))

    ax = axes[2]
    style(ax, "PyGNOME Deterministic", "Footprint vs March 4 target")
    ax.imshow(ma(rasters["target"]), origin="upper", interpolation="nearest", cmap=ListedColormap([target_c]), alpha=0.58)
    ax.imshow(ma(rasters["pygnome_density"]), origin="upper", interpolation="nearest", cmap=density_cmap, alpha=0.42)
    ax.imshow(ma(rasters["pygnome"]), origin="upper", interpolation="nearest", cmap=ListedColormap([pygnome_c]), alpha=0.90)
    nearest = float(py_diag.nearest_distance_to_obs_m)
    nearest_text = "n/a" if not np.isfinite(nearest) else f"{nearest / 1000:.2f} km"
    ax.text(0.03, 0.04,
            f"footprint cells: {int(py_diag.forecast_nonzero_cells)} | obs: {int(py_diag.obs_nonzero_cells)}\nNearest miss: {nearest_text}",
            transform=ax.transAxes, fontsize=8.4, color="#0f172a",
            bbox=dict(facecolor="white", edgecolor="#cbd5e1", alpha=0.95, boxstyle="round,pad=0.35"))

    ax = axes[3]
    style(ax, "Model Overlay", "OpenDrift R1 p50 and PyGNOME comparator")
    ax.imshow(ma(rasters["target"]), origin="upper", interpolation="nearest", cmap=ListedColormap([target_c]), alpha=0.42)
    ax.imshow(ma(rasters["opendrift_r1"]), origin="upper", interpolation="nearest", cmap=ListedColormap([opendrift_c]), alpha=0.90)
    ax.imshow(ma(rasters["pygnome"]), origin="upper", interpolation="nearest", cmap=ListedColormap([pygnome_c]), alpha=0.80)
    ax.text(0.03, 0.04,
            "PyGNOME is a comparator only.\nPhilSA target remains the observation layer.",
            transform=ax.transAxes, fontsize=8.4, color="#0f172a",
            bbox=dict(facecolor="white", edgecolor="#cbd5e1", alpha=0.95, boxstyle="round,pad=0.35"))

    card_y, card_h = 0.072, 0.205
    card_xs = [0.035, 0.355, 0.675]
    card_w = 0.29
    for x in card_xs:
        card = mpatches.FancyBboxPatch((x, card_y), card_w, card_h, transform=fig.transFigure,
                                       boxstyle="round,pad=0.012,rounding_size=0.012",
                                       facecolor="white", edgecolor="#dbe3ee", linewidth=1.0)
        fig.add_artist(card)

    fig.text(0.06, card_y + card_h - 0.037, "FSS by Window", fontsize=12.8, fontweight="bold", color="#0f172a")
    rows = ["Window      OD_R1     PyGNOME"]
    for window in OFFICIAL_PHASE3B_WINDOWS_KM:
        od_val = float(od_r1_fss[od_r1_fss.window_km == window].iloc[0].fss)
        py_val = float(py_fss[py_fss.window_km == window].iloc[0].fss)
        rows.append(f"{window:>2} km      {od_val:0.3f}       {py_val:0.3f}")
    fig.text(0.06, card_y + card_h - 0.065, "\n".join(rows), fontsize=9.8, family="monospace", color="#1e293b", va="top")

    fig.text(0.38, card_y + card_h - 0.037, "Diagnostics", fontsize=12.8, fontweight="bold", color="#0f172a")
    diag_lines = [
        f"OpenDrift R1 p50 cells: {int(od_r1_diag.forecast_nonzero_cells)}; nearest miss: {float(od_r1_diag.nearest_distance_to_obs_m) / 1000:.2f} km",
        f"PyGNOME cells: {int(py_diag.forecast_nonzero_cells)}; nearest miss: {nearest_text}",
        f"PyGNOME centroid distance: {float(py_diag.centroid_distance_m) / 1000:.2f} km"
        if np.isfinite(float(py_diag.centroid_distance_m))
        else "PyGNOME centroid distance: n/a",
        f"PyGNOME IoU: {float(py_diag.iou):.3f}; Dice: {float(py_diag.dice):.3f}",
        f"PyGNOME forcing mode: {py_metadata.get('transport_forcing_mode', '')}",
    ]
    fig.text(0.38, card_y + card_h - 0.065, "\n".join(diag_lines), fontsize=9.2, color="#1e293b", va="top")

    fig.text(0.70, card_y + card_h - 0.037, "Boundary", fontsize=12.8, fontweight="bold", color="#0f172a")
    boundary_lines = [
        "Both observation dates are PhilSA FeatureServer polygons.",
        "PyGNOME is not truth and not a co-primary validation row.",
        f"PyGNOME particle count: {py_metadata.get('benchmark_particles')}",
        "This remains experimental/archive-only.",
        "Does not replace canonical B1.",
    ]
    fig.text(0.70, card_y + card_h - 0.065, "\n".join(boundary_lines), fontsize=9.6, color="#1e293b", va="top")
    fig.text(0.04, 0.025,
             "View is cropped around nonzero observation/forecast cells; each raster cell is 1 km. Generated only in the experimental March 3 -> March 4 archive.",
             fontsize=8.7, color="#64748b")
    fig.savefig(out_path, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close(fig)


def run() -> dict[str, Any]:
    if not GNOME_AVAILABLE:
        raise RuntimeError("PyGNOME is not available. Run this script in the `gnome` Docker service.")
    if not BASE.exists():
        raise FileNotFoundError(f"Experimental base output directory not found: {BASE}")

    for path in (PYGNOME_DIR, PRODUCTS_DIR, QA_DIR, PRECHECK_DIR, MODEL_DIR):
        path.mkdir(parents=True, exist_ok=True)

    before = _snapshot_paths(PROTECTED_PATHS)
    before_path = PYGNOME_DIR / "march3_4_philsa_5000_pygnome_protected_outputs_snapshot_before.json"
    _write_json(before_path, before)

    context = _load_required_context()
    reference_lat, reference_lon = _reference_point_from_vector(context["seed_vector"])

    nc_path = MODEL_DIR / PYGNOME_NC_NAME
    metadata_path = MODEL_DIR / "pygnome_march3_4_philsa_5000_metadata.json"
    if nc_path.exists() and metadata_path.exists():
        py_metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    else:
        gnome = GnomeComparisonService()
        gnome.output_dir = MODEL_DIR
        py_nc_path, py_metadata = gnome.run_transport_benchmark_scenario(
            start_lat=reference_lat,
            start_lon=reference_lon,
            start_time=SIMULATION_START_UTC,
            output_name=PYGNOME_NC_NAME,
            random_seed=RANDOM_SEED,
            polygon_path=context["seed_vector"],
            seed_time_override=SIMULATION_START_UTC,
            duration_hours=DURATION_HOURS,
            time_step_minutes=TIME_STEP_MINUTES,
            winds_file=BASE / "forcing/gfs_wind.nc",
            currents_file=BASE / "forcing/cmems_curr.nc",
            allow_degraded_forcing=True,
        )
        nc_path = Path(py_nc_path)
        _write_json(metadata_path, py_metadata)

    actual_particles = int(py_metadata.get("benchmark_particles") or 0)
    if actual_particles != REQUESTED_ELEMENT_COUNT:
        blocked_path = PYGNOME_DIR / "pygnome_comparator_blocked_note.md"
        _write_text(
            blocked_path,
            f"# PyGNOME Comparator Blocked\n\nDetected particle count {actual_particles}, expected {REQUESTED_ELEMENT_COUNT}.\n",
        )
        raise RuntimeError(f"PyGNOME actual particle count {actual_particles} != {REQUESTED_ELEMENT_COUNT}")

    grid = GridBuilder()
    sea_mask = load_sea_mask_array(grid.spec)
    valid_mask = sea_mask > 0.5 if sea_mask is not None else None
    product = _build_pygnome_local_date_products(
        grid=grid,
        sea_mask=sea_mask,
        nc_path=nc_path,
        out_dir=PRODUCTS_DIR,
    )

    py_fss, py_diag = _score_pygnome(Path(product["forecast_path"]), Path(context["target_mask"]), valid_mask)
    od_fss, od_diag = _copy_opendrift_score_rows(context)
    combined_fss = pd.concat([od_fss, py_fss], ignore_index=True, sort=False)
    combined_diag = pd.concat([od_diag, py_diag], ignore_index=True, sort=False)

    py_summary = py_diag.copy()
    fss_pivot = (
        py_fss.pivot(index="pair_id", columns="window_km", values="fss")
        .rename(columns={window: f"fss_{window}km" for window in OFFICIAL_PHASE3B_WINDOWS_KM})
        .reset_index()
    )
    py_summary = py_summary.merge(fss_pivot, on="pair_id", how="left")
    py_summary["mean_fss"] = py_summary[[f"fss_{window}km" for window in OFFICIAL_PHASE3B_WINDOWS_KM]].mean(axis=1)
    py_summary["particle_count_actual"] = actual_particles
    py_summary["transport_forcing_mode"] = py_metadata.get("transport_forcing_mode", "")
    py_summary["degraded_forcing"] = bool(py_metadata.get("degraded_forcing"))
    py_summary["degraded_reason"] = py_metadata.get("degraded_reason", "")
    py_summary["pygnome_used_as_truth"] = False
    py_summary["claim_boundary"] = (
        "Experimental PyGNOME deterministic comparator for PhilSA-only March 3 -> March 4; "
        "not thesis-facing and not a replacement for canonical B1."
    )

    py_fss_path = PYGNOME_DIR / "march3_4_philsa_5000_pygnome_fss_by_window.csv"
    py_diag_path = PYGNOME_DIR / "march3_4_philsa_5000_pygnome_diagnostics.csv"
    py_summary_path = PYGNOME_DIR / "march3_4_philsa_5000_pygnome_summary.csv"
    combined_fss_path = PYGNOME_DIR / "march3_4_philsa_5000_combined_opendrift_pygnome_fss_by_window.csv"
    combined_diag_path = PYGNOME_DIR / "march3_4_philsa_5000_combined_opendrift_pygnome_diagnostics.csv"
    _write_csv(py_fss_path, py_fss)
    _write_csv(py_diag_path, py_diag)
    _write_csv(py_summary_path, py_summary)
    _write_csv(combined_fss_path, combined_fss)
    _write_csv(combined_diag_path, combined_diag)

    qa_overlay = _write_qa_overlay(Path(context["target_mask"]), Path(product["forecast_path"]))
    board_path = BASE / "march3_4_philsa_5000_board_with_pygnome_user_friendly.png"
    _make_combined_board(
        context=context,
        pygnome_footprint=Path(product["forecast_path"]),
        pygnome_density=Path(product["density_path"]),
        combined_fss=combined_fss,
        combined_diag=combined_diag,
        py_metadata=py_metadata,
        out_path=board_path,
    )

    after = _snapshot_paths(PROTECTED_PATHS)
    after_path = PYGNOME_DIR / "march3_4_philsa_5000_pygnome_protected_outputs_snapshot_after.json"
    _write_json(after_path, after)
    protected_unchanged = before == after
    if not protected_unchanged:
        blocked_path = PYGNOME_DIR / "pygnome_comparator_blocked_note.md"
        _write_text(
            blocked_path,
            "# PyGNOME Comparator Blocked\n\nProtected outputs changed during the experimental comparator run.\n",
        )
        raise RuntimeError("Protected outputs changed during PyGNOME comparator run.")

    note_path = PYGNOME_DIR / "march3_4_philsa_5000_pygnome_decision_note.md"
    py_mean = float(py_summary.iloc[0]["mean_fss"])
    py_row = py_diag.iloc[0]
    nearest = float(py_row["nearest_distance_to_obs_m"])
    nearest_text = "n/a" if not np.isfinite(nearest) else f"{nearest / 1000:.3f} km"
    _write_text(
        note_path,
        "\n".join(
            [
                "# EXPERIMENTAL / NOT THESIS-FACING: Mindoro PhilSA March 3 -> March 4 PyGNOME Comparator",
                "",
                "This deterministic PyGNOME run is a comparator only. It is not an observation source, not a co-primary validation row, and not a replacement for canonical B1.",
                "",
                "- Seed source: PhilSA MindoroOilSpill_Philsa_230303",
                "- Target source: PhilSA MindoroOilSpill_Philsa_230304",
                f"- PyGNOME particle count detected: {actual_particles}",
                f"- PyGNOME March 4 footprint cells: {int(py_row['forecast_nonzero_cells'])}",
                f"- PyGNOME mean FSS: {py_mean:.6f}",
                f"- PyGNOME nearest forecast-to-observation distance: {nearest_text}",
                f"- PyGNOME forcing mode: {py_metadata.get('transport_forcing_mode', '')}",
                f"- Degraded forcing: {bool(py_metadata.get('degraded_forcing'))}",
                f"- Degraded reason: {py_metadata.get('degraded_reason', '')}",
                "",
                "Boundary: Both observation dates are PhilSA FeatureServer polygon products from the same continuing event provenance. Do not call this fully statistically independent evidence.",
            ]
        )
        + "\n",
    )

    manifest_path = PYGNOME_DIR / "march3_4_philsa_5000_pygnome_run_manifest.json"
    manifest = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "phase": "mindoro_march3_4_philsa_5000_pygnome_comparator",
        "experimental_only": True,
        "thesis_facing": False,
        "reportable": False,
        "pygnome_used_as_truth": False,
        "output_dir": str(PYGNOME_DIR),
        "combined_board": str(board_path),
        "selected_start_source": context["seed_row"],
        "selected_target_source": context["target_row"],
        "simulation": {
            "simulation_start_utc": SIMULATION_START_UTC,
            "duration_hours": DURATION_HOURS,
            "time_step_minutes": TIME_STEP_MINUTES,
            "random_seed": RANDOM_SEED,
            "particle_count_requested": REQUESTED_ELEMENT_COUNT,
            "particle_count_actual": actual_particles,
        },
        "pygnome_metadata": py_metadata,
        "local_date_product": {key: str(value) if isinstance(value, Path) else value for key, value in product.items()},
        "artifacts": {
            "netcdf": str(nc_path),
            "metadata_json": str(metadata_path),
            "footprint_tif": str(product["forecast_path"]),
            "density_tif": str(product["density_path"]),
            "pygnome_fss_csv": str(py_fss_path),
            "pygnome_diagnostics_csv": str(py_diag_path),
            "pygnome_summary_csv": str(py_summary_path),
            "combined_fss_csv": str(combined_fss_path),
            "combined_diagnostics_csv": str(combined_diag_path),
            "qa_overlay_png": str(qa_overlay),
            "combined_board_png": str(board_path),
            "decision_note_md": str(note_path),
            "protected_snapshot_before": str(before_path),
            "protected_snapshot_after": str(after_path),
        },
        "score_summary": py_summary.to_dict(orient="records"),
        "protected_outputs_unchanged": protected_unchanged,
    }
    _write_json(manifest_path, manifest)

    return {
        "output_dir": str(PYGNOME_DIR),
        "combined_board": str(board_path),
        "pygnome_netcdf": str(nc_path),
        "pygnome_footprint": str(product["forecast_path"]),
        "pygnome_particle_count_actual": actual_particles,
        "pygnome_mean_fss": py_mean,
        "protected_outputs_unchanged": protected_unchanged,
        "run_manifest": str(manifest_path),
    }


if __name__ == "__main__":
    print(json.dumps(run(), indent=2, default=_json_default))
