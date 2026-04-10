"""Observation-based OpenDrift/PyGNOME comparison for Mindoro Phase 3B.

This branch keeps public observation-derived masks as truth and compares model
products against them. PyGNOME is a model comparator only; it is never used as
an observation source.
"""

from __future__ import annotations

import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from src.core.case_context import get_case_context
from src.helpers.metrics import calculate_fss
from src.helpers.raster import GridBuilder, extract_particles_at_time, rasterize_particles, save_raster
from src.helpers.scoring import apply_ocean_mask, load_sea_mask_array, precheck_same_grid
from src.services.gnome_comparison import GNOME_AVAILABLE, GnomeComparisonService
from src.services.official_rerun_r1 import OFFICIAL_RERUN_R1_DIR_NAME, _read_json
from src.services.scoring import OFFICIAL_PHASE3B_WINDOWS_KM, Phase3BScoringService
from src.utils.io import get_case_output_dir, resolve_recipe_selection, resolve_spill_origin

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

try:
    import netCDF4
except ImportError:  # pragma: no cover
    netCDF4 = None


PYGNOME_PUBLIC_COMPARISON_DIR_NAME = "pygnome_public_comparison"
STRICT_VALIDATION_DATE = "2023-03-06"
STRICT_VALIDATION_TIME_UTC = "2023-03-06T09:59:00Z"
FORECAST_VALIDATION_DATES = ["2023-03-04", "2023-03-05", "2023-03-06"]
EVENT_CORRIDOR_LABEL = "2023-03-04_to_2023-03-06"


def _json_default(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return None if np.isnan(value) else float(value)
    if isinstance(value, (pd.Timestamp, datetime)):
        return _iso_z(value)
    if isinstance(value, np.ndarray):
        return value.tolist()
    return str(value)


def _write_json(path: Path, payload: dict | list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, default=_json_default)
        handle.write("\n")


def _write_csv(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False)


def _normalize_utc(value: Any) -> pd.Timestamp:
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is not None:
        return timestamp.tz_convert("UTC").tz_localize(None)
    return timestamp


def _iso_z(value: Any) -> str:
    return _normalize_utc(value).strftime("%Y-%m-%dT%H:%M:%SZ")


def _timestamp_label(value: Any) -> str:
    return _normalize_utc(value).strftime("%Y-%m-%dT%H-%M-%SZ")


def _read_raster(path: Path) -> np.ndarray:
    if rasterio is None:
        raise ImportError("rasterio is required for pygnome_public_comparison.")
    with rasterio.open(path) as src:
        return src.read(1).astype(np.float32)


def _mean_fss(row: pd.Series | dict) -> float:
    values = []
    for window in OFFICIAL_PHASE3B_WINDOWS_KM:
        try:
            value = float(row.get(f"fss_{window}km", np.nan))
        except (TypeError, ValueError):
            value = np.nan
        if np.isfinite(value):
            values.append(value)
    return float(np.mean(values)) if values else 0.0


def recommend_public_comparison(summary_df: pd.DataFrame) -> dict:
    """Return the single thesis-facing recommendation requested by the phase."""
    if summary_df.empty:
        return {
            "recommendation": "all are weak and the main text should emphasize the multi-date public track over the strict March 6 single-date result",
            "best_eventcorridor_track": "",
            "best_strict_march6_track": "",
            "reason": "No score rows were available.",
        }

    working = summary_df.copy()
    working["mean_fss"] = working.apply(_mean_fss, axis=1)
    event = working[working["pair_role"] == "eventcorridor_march4_6"].copy()
    strict = working[working["pair_role"] == "strict_march6"].copy()
    best_event = event.sort_values(["mean_fss", "iou", "dice", "forecast_nonzero_cells"], ascending=False).head(1)
    best_strict = strict.sort_values(["mean_fss", "iou", "dice", "forecast_nonzero_cells"], ascending=False).head(1)

    best_event_track = str(best_event.iloc[0]["track_id"]) if not best_event.empty else ""
    best_strict_track = str(best_strict.iloc[0]["track_id"]) if not best_strict.empty else ""
    best_event_score = float(best_event.iloc[0]["mean_fss"]) if not best_event.empty else 0.0
    best_strict_score = float(best_strict.iloc[0]["mean_fss"]) if not best_strict.empty else 0.0

    if max(best_event_score, best_strict_score) < 0.01:
        recommendation = "all are weak and the main text should emphasize the multi-date public track over the strict March 6 single-date result"
    elif best_event_track == "C2":
        recommendation = "OpenDrift ensemble is the better public-validation performer"
    elif best_event_track == "C1":
        recommendation = "OpenDrift deterministic is the better public-validation performer"
    elif best_event_track == "C3":
        recommendation = "PyGNOME is the better public-validation performer"
    else:
        recommendation = "all are weak and the main text should emphasize the multi-date public track over the strict March 6 single-date result"

    return {
        "recommendation": recommendation,
        "best_eventcorridor_track": best_event_track,
        "best_strict_march6_track": best_strict_track,
        "best_eventcorridor_mean_fss": best_event_score,
        "best_strict_march6_mean_fss": best_strict_score,
        "reason": (
            "Model ranking prioritizes the March 4-6 event-corridor public-observation comparison, "
            "then strict March 6 as a sparse stress-test tie-breaker."
        ),
    }


class PyGnomePublicComparisonService:
    def __init__(self):
        self.case = get_case_context()
        if not self.case.is_official:
            raise RuntimeError("pygnome_public_comparison is only supported for official Mindoro workflows.")
        if xr is None:
            raise ImportError("xarray is required for pygnome_public_comparison.")
        if rasterio is None:
            raise ImportError("rasterio is required for pygnome_public_comparison.")

        self.case_output = get_case_output_dir(self.case.run_name)
        self.output_dir = self.case_output / PYGNOME_PUBLIC_COMPARISON_DIR_NAME
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.products_dir = self.output_dir / "products"
        self.obs_dir = self.output_dir / "observations"
        self.precheck_dir = self.output_dir / "precheck"
        self.qa_dir = self.output_dir / "qa"
        for path in (self.products_dir, self.obs_dir, self.precheck_dir, self.qa_dir):
            path.mkdir(parents=True, exist_ok=True)

        self.grid = GridBuilder()
        self.sea_mask = load_sea_mask_array(self.grid.spec)
        self.valid_mask = self.sea_mask > 0.5 if self.sea_mask is not None else None
        self.helper = Phase3BScoringService(output_dir=self.output_dir / "_scratch_helper")

    def run(self) -> dict:
        upstream = self._load_upstream_context()
        observations = self._prepare_observations()
        tracks = self._prepare_tracks(upstream)
        pairing_df = self._build_pairings(tracks, observations)
        scored_pairings, fss_df, diagnostics_df = self._score_pairings(pairing_df)
        summary_df = self._summarize(scored_pairings, fss_df, diagnostics_df)
        ranking_df = self._rank_models(summary_df)
        recommendation = recommend_public_comparison(summary_df)
        qa_paths = self._write_qa(summary_df)
        paths = self._write_outputs(scored_pairings, fss_df, diagnostics_df, summary_df, ranking_df, tracks)
        memo_path = self._write_memo(recommendation, paths)
        manifest_path = self._write_manifest(upstream, observations, tracks, recommendation, paths, qa_paths, memo_path)

        return {
            "output_dir": self.output_dir,
            "tracks_registry": paths["tracks_registry"],
            "pairing_manifest": paths["pairing_manifest"],
            "fss_by_date_window": paths["fss_by_date_window"],
            "diagnostics": paths["diagnostics"],
            "summary": summary_df,
            "summary_csv": paths["summary"],
            "eventcorridor_summary": paths["eventcorridor_summary"],
            "run_manifest": manifest_path,
            "memo": memo_path,
            "ranking": ranking_df,
            "recommendation": recommendation,
        }

    def _load_upstream_context(self) -> dict:
        official_manifest_path = self.case_output / OFFICIAL_RERUN_R1_DIR_NAME / "official_rerun_r1_run_manifest.json"
        official_manifest = _read_json(official_manifest_path)
        model_dir = Path((official_manifest.get("model_result") or {}).get("model_dir", ""))
        if not model_dir.exists():
            raise FileNotFoundError(f"official_rerun_r1 model_dir not found: {model_dir}")

        required_paths = {
            "official_rerun_r1_manifest": official_manifest_path,
            "official_rerun_r1_summary": self.case_output / OFFICIAL_RERUN_R1_DIR_NAME / "official_rerun_r1_summary.csv",
            "init_mode_manifest": self.case_output / "init_mode_sensitivity_r1" / "init_mode_sensitivity_r1_run_manifest.json",
            "source_history_manifest": self.case_output / "source_history_reconstruction_r1" / "source_history_reconstruction_r1_run_manifest.json",
            "strict_phase3b_summary": self.case_output / "phase3b" / "phase3b_summary.csv",
            "public_obs_inventory": self.case_output / "public_obs_appendix" / "public_obs_inventory.csv",
            "multidate_summary": self.case_output / "phase3b_multidate_public" / "phase3b_multidate_summary.csv",
        }
        missing = [str(path) for path in required_paths.values() if not Path(path).exists()]
        if missing:
            raise FileNotFoundError("pygnome_public_comparison requires existing upstream artifacts: " + "; ".join(missing))

        return {
            "official_rerun_r1_manifest": official_manifest,
            "official_rerun_r1_manifest_path": str(official_manifest_path),
            "model_dir": model_dir,
            "forecast_manifest_path": str(model_dir / "forecast" / "forecast_manifest.json"),
            "ensemble_manifest_path": str(model_dir / "ensemble" / "ensemble_manifest.json"),
            "selected_retention": official_manifest.get("selected_retention_config", {}),
            "provenance": official_manifest.get("provenance", {}),
            "required_paths": {key: str(value) for key, value in required_paths.items()},
        }

    def _prepare_observations(self) -> dict[str, Any]:
        strict = Path("data") / "arcgis" / self.case.run_name / "obs_mask_2023-03-06.tif"
        if not strict.exists():
            raise FileNotFoundError(f"Strict March 6 observed mask is missing: {strict}")

        date_union_dir = self.case_output / "phase3b_multidate_public" / "date_union_obs_masks"
        date_unions: dict[str, Path] = {}
        for date in FORECAST_VALIDATION_DATES:
            path = date_union_dir / f"obs_union_{date}.tif"
            if not path.exists():
                raise FileNotFoundError(f"Required accepted date-union observation mask missing: {path}")
            date_unions[date] = path

        event_obs = self._build_eventcorridor_obs_union(date_unions)
        return {
            "strict_march6": strict,
            "date_unions": date_unions,
            "eventcorridor_march4_6": event_obs,
        }

    def _build_eventcorridor_obs_union(self, date_unions: dict[str, Path]) -> Path:
        union = np.zeros((self.grid.height, self.grid.width), dtype=np.float32)
        for date in FORECAST_VALIDATION_DATES:
            union = np.maximum(union, self.helper._load_binary_score_mask(date_unions[date]))
        union = apply_ocean_mask(union, sea_mask=self.sea_mask, fill_value=0.0)
        path = self.obs_dir / "eventcorridor_obs_union_2023-03-04_to_2023-03-06.tif"
        save_raster(self.grid, union.astype(np.float32), path)
        return path

    def _prepare_tracks(self, upstream: dict) -> list[dict]:
        selection = resolve_recipe_selection()
        model_dir = Path(upstream["model_dir"])
        return [
            self._prepare_opendrift_deterministic_track(model_dir, selection.recipe, upstream),
            self._prepare_opendrift_ensemble_track(model_dir, upstream),
            self._prepare_pygnome_track(selection.recipe, upstream),
        ]

    def _prepare_opendrift_deterministic_track(self, model_dir: Path, recipe: str, upstream: dict) -> dict:
        track_dir = self.products_dir / "C1_od_deterministic"
        track_dir.mkdir(parents=True, exist_ok=True)
        nc_path = next(iter(sorted((model_dir / "forecast").glob("deterministic_control_*.nc"))), None)
        if nc_path is None:
            raise FileNotFoundError(f"Missing OpenDrift deterministic NetCDF under {model_dir / 'forecast'}")

        date_products = {
            date: self._build_opendrift_date_composite(nc_path, date, track_dir, "od_control")
            for date in FORECAST_VALIDATION_DATES
        }
        event = self._build_model_eventcorridor_union(
            date_products,
            track_dir / "od_control_eventcorridor_model_union_2023-03-04_to_2023-03-06.tif",
        )
        return {
            "track_id": "C1",
            "track_name": "od_deterministic_vs_public",
            "model_name": "OpenDrift deterministic control",
            "model_family": "OpenDrift",
            "initialization_mode": "B_observation_initialized_polygon",
            "retention_coastline_action": "previous",
            "transport_model": "oceandrift",
            "provisional_transport_model": True,
            "recipe_used": recipe,
            "current_source": "cmems_era5 recipe currents",
            "wind_source": "cmems_era5 recipe winds",
            "wave_stokes_status": "same OpenDrift R1 forcing stack as official_rerun_r1",
            "forcing_manifest_paths": f"{upstream['forecast_manifest_path']};{upstream['ensemble_manifest_path']}",
            "structural_limitations": "OpenDrift deterministic control has no ensemble probability thresholding.",
            "strict_march6_forecast": date_products[STRICT_VALIDATION_DATE],
            "date_forecasts": date_products,
            "eventcorridor_forecast": event,
            "products_dir": str(track_dir),
            "source_nc": str(nc_path),
            "pygnome_benchmark_metadata": {},
        }

    def _prepare_opendrift_ensemble_track(self, model_dir: Path, upstream: dict) -> dict:
        track_dir = self.products_dir / "C2_od_ensemble_p50"
        track_dir.mkdir(parents=True, exist_ok=True)
        date_products = {}
        for date in FORECAST_VALIDATION_DATES:
            source = model_dir / "ensemble" / f"mask_p50_{date}_datecomposite.tif"
            if not source.exists():
                raise FileNotFoundError(f"Missing OpenDrift ensemble p50 date-composite: {source}")
            dest = track_dir / source.name
            shutil.copyfile(source, dest)
            masked = apply_ocean_mask(_read_raster(dest), sea_mask=self.sea_mask, fill_value=0.0)
            save_raster(self.grid, masked.astype(np.float32), dest)
            date_products[date] = dest
        event = self._build_model_eventcorridor_union(
            date_products,
            track_dir / "od_ensemble_p50_eventcorridor_model_union_2023-03-04_to_2023-03-06.tif",
        )
        provenance = upstream.get("provenance") or {}
        return {
            "track_id": "C2",
            "track_name": "od_ensemble_p50_vs_public",
            "model_name": "OpenDrift ensemble p50",
            "model_family": "OpenDrift",
            "initialization_mode": "B_observation_initialized_polygon",
            "retention_coastline_action": "previous",
            "transport_model": str(provenance.get("transport_model", "oceandrift")),
            "provisional_transport_model": bool(provenance.get("provisional_transport_model", True)),
            "recipe_used": str(provenance.get("recipe_used", resolve_recipe_selection().recipe)),
            "current_source": "cmems_era5 recipe currents",
            "wind_source": "cmems_era5 recipe winds",
            "wave_stokes_status": "wave/Stokes required and inherited from official_rerun_r1",
            "forcing_manifest_paths": f"{upstream['forecast_manifest_path']};{upstream['ensemble_manifest_path']}",
            "structural_limitations": "p50 is a thresholded ensemble probability product and may be sparse by construction.",
            "strict_march6_forecast": date_products[STRICT_VALIDATION_DATE],
            "date_forecasts": date_products,
            "eventcorridor_forecast": event,
            "products_dir": str(track_dir),
            "source_nc": "",
            "pygnome_benchmark_metadata": {},
        }

    def _prepare_pygnome_track(self, recipe: str, upstream: dict) -> dict:
        track_dir = self.products_dir / "C3_pygnome_deterministic"
        track_dir.mkdir(parents=True, exist_ok=True)
        nc_path = track_dir / "pygnome_deterministic_control.nc"
        metadata_path = track_dir / "pygnome_benchmark_metadata.json"
        if not nc_path.exists():
            if not GNOME_AVAILABLE:
                raise RuntimeError("PyGNOME comparison requires the gnome container when PyGNOME products are missing.")
            start_lat, start_lon, start_time = resolve_spill_origin()
            gnome_service = GnomeComparisonService()
            gnome_service.output_dir = track_dir
            py_nc_path, py_metadata = gnome_service.run_transport_benchmark_scenario(
                start_lat=start_lat,
                start_lon=start_lon,
                start_time=start_time,
                output_name=nc_path.name,
            )
            if Path(py_nc_path) != nc_path:
                shutil.copyfile(py_nc_path, nc_path)
            _write_json(metadata_path, py_metadata)
        else:
            py_metadata = _read_json(metadata_path) if metadata_path.exists() else {"nc_path": str(nc_path), "status": "reused_existing"}

        date_products = {
            date: self._build_pygnome_date_composite(nc_path, date, track_dir)
            for date in FORECAST_VALIDATION_DATES
        }
        self._build_pygnome_strict_snapshot_products(nc_path, track_dir)
        event = self._build_model_eventcorridor_union(
            date_products,
            track_dir / "pygnome_eventcorridor_model_union_2023-03-04_to_2023-03-06.tif",
        )
        return {
            "track_id": "C3",
            "track_name": "pygnome_deterministic_vs_public",
            "model_name": "PyGNOME deterministic benchmark",
            "model_family": "PyGNOME",
            "initialization_mode": "B_observation_initialized_polygon_surrogate_clustered_point_spills",
            "retention_coastline_action": "PyGNOME default benchmark behavior",
            "transport_model": "pygnome",
            "provisional_transport_model": True,
            "recipe_used": recipe,
            "current_source": "not attached in current PyGNOME benchmark service",
            "wind_source": "nearest compatible constant-wind PyGNOME benchmark",
            "wave_stokes_status": "not reproduced identically; PyGNOME benchmark does not attach official Stokes forcing",
            "forcing_manifest_paths": f"{upstream['forecast_manifest_path']};{upstream['ensemble_manifest_path']}",
            "structural_limitations": (
                "PyGNOME deterministic benchmark approximates the March 3 polygon with clustered point spills and "
                "uses the repo's available PyGNOME transport benchmark; gridded currents/waves are not hidden as equivalent."
            ),
            "strict_march6_forecast": date_products[STRICT_VALIDATION_DATE],
            "date_forecasts": date_products,
            "eventcorridor_forecast": event,
            "products_dir": str(track_dir),
            "source_nc": str(nc_path),
            "pygnome_benchmark_metadata": py_metadata,
        }

    def _build_opendrift_date_composite(self, nc_path: Path, target_date: str, out_dir: Path, prefix: str) -> Path:
        out_path = out_dir / f"{prefix}_footprint_mask_{target_date}_datecomposite.tif"
        composite = np.zeros((self.grid.height, self.grid.width), dtype=np.float32)
        with xr.open_dataset(nc_path) as ds:
            times = pd.DatetimeIndex(pd.to_datetime(ds["time"].values))
            if times.tz is not None:
                times = times.tz_convert("UTC").tz_localize(None)
            for index, timestamp in enumerate(times):
                if pd.Timestamp(timestamp).date().isoformat() != target_date:
                    continue
                lon = np.asarray(ds["lon"].isel(time=index).values).reshape(-1)
                lat = np.asarray(ds["lat"].isel(time=index).values).reshape(-1)
                status = np.asarray(ds["status"].isel(time=index).values).reshape(-1)
                valid = np.isfinite(lon) & np.isfinite(lat) & (status == 0)
                if not np.any(valid):
                    continue
                hits, _ = rasterize_particles(
                    self.grid,
                    lon[valid],
                    lat[valid],
                    np.ones(int(np.count_nonzero(valid)), dtype=np.float32),
                )
                composite = np.maximum(composite, hits.astype(np.float32))
        composite = apply_ocean_mask(composite, sea_mask=self.sea_mask, fill_value=0.0)
        save_raster(self.grid, composite.astype(np.float32), out_path)
        return out_path

    def _build_pygnome_date_composite(self, nc_path: Path, target_date: str, out_dir: Path) -> Path:
        out_path = out_dir / f"pygnome_footprint_mask_{target_date}_datecomposite.tif"
        composite = np.zeros((self.grid.height, self.grid.width), dtype=np.float32)
        for timestamp in self._pygnome_times(nc_path):
            if pd.Timestamp(timestamp).date().isoformat() != target_date:
                continue
            try:
                lon, lat, mass, _, _ = extract_particles_at_time(
                    nc_path,
                    timestamp,
                    "pygnome",
                    allow_uniform_mass_fallback=True,
                )
            except Exception:
                continue
            if len(lon) == 0:
                continue
            hits, _ = rasterize_particles(self.grid, lon, lat, mass)
            composite = np.maximum(composite, hits.astype(np.float32))
        composite = apply_ocean_mask(composite, sea_mask=self.sea_mask, fill_value=0.0)
        save_raster(self.grid, composite.astype(np.float32), out_path)
        return out_path

    def _build_pygnome_strict_snapshot_products(self, nc_path: Path, out_dir: Path) -> None:
        label = _timestamp_label(STRICT_VALIDATION_TIME_UTC)
        footprint_path = out_dir / f"pygnome_footprint_mask_{label}.tif"
        density_path = out_dir / f"pygnome_density_norm_{label}.tif"
        lon, lat, mass, _, _ = extract_particles_at_time(
            nc_path,
            STRICT_VALIDATION_TIME_UTC,
            "pygnome",
            allow_uniform_mass_fallback=True,
        )
        if len(lon) == 0:
            hits = np.zeros((self.grid.height, self.grid.width), dtype=np.float32)
            density = np.zeros((self.grid.height, self.grid.width), dtype=np.float32)
        else:
            hits, density = rasterize_particles(self.grid, lon, lat, mass)
        hits = apply_ocean_mask(hits, sea_mask=self.sea_mask, fill_value=0.0)
        density = apply_ocean_mask(density, sea_mask=self.sea_mask, fill_value=0.0)
        total = float(np.nansum(density))
        if total > 0:
            density = density / total
        save_raster(self.grid, hits.astype(np.float32), footprint_path)
        save_raster(self.grid, density.astype(np.float32), density_path)

    def _pygnome_times(self, nc_path: Path) -> list[pd.Timestamp]:
        if netCDF4 is None:
            raise ImportError("netCDF4 is required to inspect PyGNOME outputs.")
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

    def _build_model_eventcorridor_union(self, date_products: dict[str, Path], out_path: Path) -> Path:
        union = np.zeros((self.grid.height, self.grid.width), dtype=np.float32)
        for date in FORECAST_VALIDATION_DATES:
            union = np.maximum(union, self.helper._load_binary_score_mask(date_products[date]))
        union = apply_ocean_mask(union, sea_mask=self.sea_mask, fill_value=0.0)
        save_raster(self.grid, union.astype(np.float32), out_path)
        return out_path

    def _build_pairings(self, tracks: list[dict], observations: dict[str, Any]) -> pd.DataFrame:
        rows: list[dict] = []
        for track in tracks:
            rows.append(
                self._pair_record(
                    track,
                    pair_role="strict_march6",
                    obs_date=STRICT_VALIDATION_DATE,
                    forecast_path=track["strict_march6_forecast"],
                    observation_path=observations["strict_march6"],
                    source_semantics="strict_single_date_stress_test_march6_public_obs",
                )
            )
            for date in FORECAST_VALIDATION_DATES:
                rows.append(
                    self._pair_record(
                        track,
                        pair_role="multidate_date_union",
                        obs_date=date,
                        forecast_path=track["date_forecasts"][date],
                        observation_path=observations["date_unions"][date],
                        source_semantics=f"per_date_union_{date}_public_observation_vs_model",
                    )
                )
            rows.append(
                self._pair_record(
                    track,
                    pair_role="eventcorridor_march4_6",
                    obs_date=EVENT_CORRIDOR_LABEL,
                    forecast_path=track["eventcorridor_forecast"],
                    observation_path=observations["eventcorridor_march4_6"],
                    source_semantics="eventcorridor_public_observation_union_excluding_march3",
                )
            )
        return pd.DataFrame(rows)

    def _pair_record(
        self,
        track: dict,
        *,
        pair_role: str,
        obs_date: str,
        forecast_path: Path,
        observation_path: Path,
        source_semantics: str,
    ) -> dict:
        return {
            "track_id": track["track_id"],
            "track_name": track["track_name"],
            "model_name": track["model_name"],
            "model_family": track["model_family"],
            "pair_id": f"{track['track_id']}_{pair_role}_{obs_date}".replace(":", "-"),
            "pair_role": pair_role,
            "obs_date": obs_date,
            "forecast_product": Path(forecast_path).name,
            "forecast_path": str(forecast_path),
            "observation_product": Path(observation_path).name,
            "observation_path": str(observation_path),
            "metric": "FSS",
            "windows_km": ",".join(str(window) for window in OFFICIAL_PHASE3B_WINDOWS_KM),
            "source_semantics": source_semantics,
            "truth_source": "accepted_public_observation_derived_mask",
            "pygnome_used_as_truth": False,
            "initialization_mode": track["initialization_mode"],
            "retention_coastline_action": track["retention_coastline_action"],
            "transport_model": track["transport_model"],
            "provisional_transport_model": track["provisional_transport_model"],
            "recipe_used": track["recipe_used"],
            "current_source": track["current_source"],
            "wind_source": track["wind_source"],
            "wave_stokes_status": track["wave_stokes_status"],
            "structural_limitations": track["structural_limitations"],
            "precheck_csv": "",
            "precheck_json": "",
        }

    def _score_pairings(self, pairings: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        scored_rows: list[dict] = []
        fss_rows: list[dict] = []
        diagnostics_rows: list[dict] = []
        for _, row in pairings.iterrows():
            forecast_path = Path(str(row["forecast_path"]))
            observation_path = Path(str(row["observation_path"]))
            precheck = precheck_same_grid(
                forecast=forecast_path,
                target=observation_path,
                report_base_path=self.precheck_dir / str(row["pair_id"]),
            )
            if not precheck.passed:
                raise RuntimeError(f"PyGNOME public comparison same-grid precheck failed for {row['pair_id']}: {precheck.json_report_path}")

            forecast = self.helper._load_binary_score_mask(forecast_path)
            observation = self.helper._load_binary_score_mask(observation_path)
            diagnostics = self.helper._compute_mask_diagnostics(forecast, observation)
            scored = row.to_dict()
            scored["precheck_csv"] = str(precheck.csv_report_path)
            scored["precheck_json"] = str(precheck.json_report_path)
            scored_rows.append(scored)
            diagnostics_rows.append({**scored, **diagnostics})

            for window_km in OFFICIAL_PHASE3B_WINDOWS_KM:
                window_cells = self.helper._window_km_to_cells(window_km)
                fss = float(
                    np.clip(
                        calculate_fss(
                            forecast,
                            observation,
                            window=window_cells,
                            valid_mask=self.valid_mask,
                        ),
                        0.0,
                        1.0,
                    )
                )
                fss_rows.append(
                    {
                        **scored,
                        "window_km": int(window_km),
                        "window_cells": int(window_cells),
                        "fss": fss,
                    }
                )
        return pd.DataFrame(scored_rows), pd.DataFrame(fss_rows), pd.DataFrame(diagnostics_rows)

    @staticmethod
    def _summarize(pairings: pd.DataFrame, fss_df: pd.DataFrame, diagnostics_df: pd.DataFrame) -> pd.DataFrame:
        fss_pivot = (
            fss_df.pivot(index="pair_id", columns="window_km", values="fss")
            .rename(columns={window: f"fss_{window}km" for window in OFFICIAL_PHASE3B_WINDOWS_KM})
            .reset_index()
        )
        diag_cols = [
            "pair_id",
            "forecast_nonzero_cells",
            "obs_nonzero_cells",
            "area_ratio_forecast_to_obs",
            "centroid_distance_m",
            "iou",
            "dice",
            "nearest_distance_to_obs_m",
            "ocean_cell_count",
        ]
        return pairings.merge(diagnostics_df[diag_cols], on="pair_id", how="left").merge(fss_pivot, on="pair_id", how="left")

    @staticmethod
    def _rank_models(summary_df: pd.DataFrame) -> pd.DataFrame:
        rows = []
        for track_id, group in summary_df.groupby("track_id"):
            track = group.iloc[0]
            strict = group[group["pair_role"] == "strict_march6"]
            event = group[group["pair_role"] == "eventcorridor_march4_6"]
            date_rows = group[group["pair_role"] == "multidate_date_union"]
            strict_row = strict.iloc[0].to_dict() if not strict.empty else {}
            event_row = event.iloc[0].to_dict() if not event.empty else {}
            rows.append(
                {
                    "track_id": track_id,
                    "track_name": track["track_name"],
                    "model_name": track["model_name"],
                    "strict_march6_mean_fss": _mean_fss(strict_row),
                    "strict_march6_iou": strict_row.get("iou", np.nan),
                    "strict_march6_dice": strict_row.get("dice", np.nan),
                    "strict_march6_nearest_distance_to_obs_m": strict_row.get("nearest_distance_to_obs_m", np.nan),
                    "eventcorridor_mean_fss": _mean_fss(event_row),
                    "eventcorridor_iou": event_row.get("iou", np.nan),
                    "eventcorridor_dice": event_row.get("dice", np.nan),
                    "eventcorridor_nearest_distance_to_obs_m": event_row.get("nearest_distance_to_obs_m", np.nan),
                    "multidate_mean_fss": float(date_rows.apply(_mean_fss, axis=1).mean()) if not date_rows.empty else np.nan,
                    "provisional_transport_model": track.get("provisional_transport_model", True),
                    "structural_limitations": track.get("structural_limitations", ""),
                }
            )
        ranked = pd.DataFrame(rows)
        if not ranked.empty:
            ranked = ranked.sort_values(
                ["eventcorridor_mean_fss", "strict_march6_mean_fss", "eventcorridor_iou", "strict_march6_iou"],
                ascending=False,
            ).reset_index(drop=True)
            ranked["eventcorridor_rank"] = np.arange(1, len(ranked) + 1)
        return ranked

    def _write_outputs(
        self,
        pairings: pd.DataFrame,
        fss_df: pd.DataFrame,
        diagnostics_df: pd.DataFrame,
        summary_df: pd.DataFrame,
        ranking_df: pd.DataFrame,
        tracks: list[dict],
    ) -> dict[str, Path]:
        track_rows = []
        for track in tracks:
            record = {
                key: value
                for key, value in track.items()
                if key not in {"strict_march6_forecast", "date_forecasts", "eventcorridor_forecast", "pygnome_benchmark_metadata"}
            }
            record.update(
                {
                    "strict_march6_forecast": str(track["strict_march6_forecast"]),
                    "eventcorridor_forecast": str(track["eventcorridor_forecast"]),
                    "date_forecast_paths": json.dumps({date: str(path) for date, path in track["date_forecasts"].items()}),
                }
            )
            track_rows.append(record)

        paths = {
            "tracks_registry": self.output_dir / "pygnome_public_comparison_tracks_registry.csv",
            "pairing_manifest": self.output_dir / "pygnome_public_comparison_pairing_manifest.csv",
            "fss_by_date_window": self.output_dir / "pygnome_public_comparison_fss_by_date_window.csv",
            "diagnostics": self.output_dir / "pygnome_public_comparison_diagnostics.csv",
            "summary": self.output_dir / "pygnome_public_comparison_summary.csv",
            "eventcorridor_summary": self.output_dir / "pygnome_public_comparison_eventcorridor_summary.csv",
            "ranking": self.output_dir / "pygnome_public_comparison_model_ranking.csv",
        }
        _write_csv(paths["tracks_registry"], pd.DataFrame(track_rows))
        _write_csv(paths["pairing_manifest"], pairings)
        _write_csv(paths["fss_by_date_window"], fss_df)
        _write_csv(paths["diagnostics"], diagnostics_df)
        _write_csv(paths["summary"], summary_df)
        _write_csv(paths["eventcorridor_summary"], summary_df[summary_df["pair_role"] == "eventcorridor_march4_6"].copy())
        _write_csv(paths["ranking"], ranking_df)
        return paths

    def _write_manifest(
        self,
        upstream: dict,
        observations: dict[str, Any],
        tracks: list[dict],
        recommendation: dict,
        paths: dict[str, Path],
        qa_paths: dict[str, Path],
        memo_path: Path,
    ) -> Path:
        manifest_path = self.output_dir / "pygnome_public_comparison_run_manifest.json"
        payload = {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "workflow_mode": self.case.workflow_mode,
            "run_name": self.case.run_name,
            "phase": PYGNOME_PUBLIC_COMPARISON_DIR_NAME,
            "purpose": "comparative model benchmark against accepted public observation-derived Mindoro masks",
            "guardrails": {
                "public_observed_masks_are_truth": True,
                "pygnome_used_as_truth": False,
                "strict_march6_files_unchanged": True,
                "observation_acceptance_rules_unchanged": True,
                "no_new_public_sources_added": True,
                "medium_tier_not_run": True,
                "march3_not_counted_as_forecast_skill": True,
            },
            "dates_scored": FORECAST_VALIDATION_DATES,
            "strict_validation_time_utc": STRICT_VALIDATION_TIME_UTC,
            "observations": {
                "strict_march6": str(observations["strict_march6"]),
                "date_unions": {date: str(path) for date, path in observations["date_unions"].items()},
                "eventcorridor_march4_6": str(observations["eventcorridor_march4_6"]),
            },
            "upstream": upstream,
            "tracks": [
                {
                    **{
                        key: value
                        for key, value in track.items()
                        if key not in {"strict_march6_forecast", "date_forecasts", "eventcorridor_forecast"}
                    },
                    "strict_march6_forecast": str(track["strict_march6_forecast"]),
                    "date_forecasts": {date: str(path) for date, path in track["date_forecasts"].items()},
                    "eventcorridor_forecast": str(track["eventcorridor_forecast"]),
                }
                for track in tracks
            ],
            "recommendation": recommendation,
            "artifacts": {
                **{key: str(value) for key, value in paths.items()},
                **{key: str(value) for key, value in qa_paths.items()},
                "thesis_sync_memo": str(memo_path),
            },
        }
        _write_json(manifest_path, payload)
        return manifest_path

    def _write_memo(self, recommendation: dict, paths: dict[str, Path]) -> Path:
        path = self.output_dir / "chapter3_pygnome_public_comparison_memo.md"
        lines = [
            "# Chapter 3 PyGNOME Public-Observation Comparison Memo",
            "",
            "This branch is a comparative observation benchmark. PyGNOME is not used as truth; accepted public observation-derived masks remain the truth source.",
            "",
            "The strict March 6 result remains the hardest single-date stress test. The March 4-6 multi-date and event-corridor comparisons provide the broader event-scale model comparison.",
            "",
            "## Important Limitations",
            "",
            "- OpenDrift R1 products use the selected `general:coastline_action=previous` retention configuration.",
            "- The PyGNOME deterministic product is generated with the repository's current PyGNOME benchmark transport pathway.",
            "- Current PyGNOME products approximate the March 3 polygon with clustered point spills and do not reproduce official gridded Stokes forcing identically.",
            "- These limitations are manifest-recorded and should be discussed rather than hidden.",
            "",
            "## Recommendation",
            "",
            f"- {recommendation['recommendation']}",
            f"- Reason: {recommendation['reason']}",
            "",
            "## Artifacts",
        ]
        for key, value in paths.items():
            lines.append(f"- {key}: {value}")
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return path

    def _write_qa(self, summary_df: pd.DataFrame) -> dict[str, Path]:
        outputs: dict[str, Path] = {}
        if plt is None:
            return outputs
        overlay = self._plot_overlays(summary_df)
        event = self._plot_eventcorridor(summary_df)
        if overlay:
            outputs["qa_pygnome_public_comparison_overlays"] = overlay
        if event:
            outputs["qa_pygnome_public_comparison_eventcorridor_overlay"] = event
        return outputs

    def _plot_overlays(self, summary_df: pd.DataFrame) -> Path | None:
        selected = summary_df[summary_df["pair_role"].isin(["strict_march6", "eventcorridor_march4_6"])].copy()
        if selected.empty:
            return None
        path = self.output_dir / "qa_pygnome_public_comparison_overlays.png"
        rows = ["strict_march6", "eventcorridor_march4_6"]
        cols = sorted(selected["track_id"].unique().tolist())
        fig, axes = plt.subplots(len(rows), len(cols), figsize=(5 * len(cols), 5 * len(rows)))
        axes_array = np.asarray(axes).reshape(len(rows), len(cols))
        for row_index, role in enumerate(rows):
            for col_index, track_id in enumerate(cols):
                ax = axes_array[row_index, col_index]
                match = selected[(selected["pair_role"] == role) & (selected["track_id"] == track_id)]
                if match.empty:
                    ax.axis("off")
                    continue
                item = match.iloc[0]
                forecast = self.helper._load_binary_score_mask(Path(item["forecast_path"]))
                obs = self.helper._load_binary_score_mask(Path(item["observation_path"]))
                self._render_overlay(ax, forecast, obs, f"{track_id} {role}")
        fig.tight_layout()
        fig.savefig(path, dpi=150, bbox_inches="tight")
        plt.close(fig)
        return path

    def _plot_eventcorridor(self, summary_df: pd.DataFrame) -> Path | None:
        selected = summary_df[summary_df["pair_role"] == "eventcorridor_march4_6"].copy()
        if selected.empty:
            return None
        path = self.output_dir / "qa_pygnome_public_comparison_eventcorridor_overlay.png"
        fig, axes = plt.subplots(1, len(selected), figsize=(5 * len(selected), 5))
        if len(selected) == 1:
            axes = [axes]
        for ax, (_, row) in zip(axes, selected.iterrows()):
            forecast = self.helper._load_binary_score_mask(Path(row["forecast_path"]))
            obs = self.helper._load_binary_score_mask(Path(row["observation_path"]))
            self._render_overlay(ax, forecast, obs, f"{row['track_id']} event corridor")
        fig.tight_layout()
        fig.savefig(path, dpi=150, bbox_inches="tight")
        plt.close(fig)
        return path

    @staticmethod
    def _render_overlay(ax, forecast: np.ndarray, obs: np.ndarray, title: str) -> None:
        canvas = np.ones((forecast.shape[0], forecast.shape[1], 3), dtype=np.float32)
        canvas[obs > 0] = [0.1, 0.35, 0.95]
        canvas[forecast > 0] = [0.95, 0.35, 0.1]
        canvas[(forecast > 0) & (obs > 0)] = [0.1, 0.65, 0.25]
        ax.imshow(canvas, origin="upper")
        ax.set_title(title)
        ax.set_axis_off()


def run_pygnome_public_comparison() -> dict:
    return PyGnomePublicComparisonService().run()


if __name__ == "__main__":  # pragma: no cover
    print(json.dumps(run_pygnome_public_comparison(), indent=2, default=_json_default))
