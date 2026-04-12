"""Scientific DWH Phase 3C external rich-data transfer-validation run."""

from __future__ import annotations

import csv
import json
from datetime import timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import xarray as xr
import yaml

from src.core.case_context import get_case_context
from src.helpers.metrics import calculate_fss
from src.helpers.scoring import GEOGRAPHIC_CRS, ScoringGridSpec
from src.services.dwh_phase3c_scientific_forcing import (
    SCIENTIFIC_PHASE,
    attrs_mark_smoke_only,
    path_is_smoke_only,
)
from src.services.dwh_phase3c_smoke import (
    CASE_ID,
    CONFIG_PATH,
    SETUP_DIR,
    _load_raster,
    _same_grid_precheck,
    _timestamp,
    _timestamp_iso,
    _timestamp_label,
    _write_raster,
    load_dwh_scoring_grid_spec,
)

try:
    import geopandas as gpd
except ImportError:  # pragma: no cover
    gpd = None

try:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
except ImportError:  # pragma: no cover
    plt = None

try:
    from rasterio.warp import transform as rio_transform
except ImportError:  # pragma: no cover
    rio_transform = None

try:
    from scipy.spatial import cKDTree
except ImportError:  # pragma: no cover
    cKDTree = None


PHASE3C_RUN_PHASE = "phase3c_external_case_run"
OUTPUT_DIR = Path("output") / CASE_ID / PHASE3C_RUN_PHASE
FORCING_READY_DIR = Path("output") / CASE_ID / SCIENTIFIC_PHASE
TRACK_DIR = OUTPUT_DIR / "tracks"
PRODUCT_DIR = OUTPUT_DIR / "products"
PRECHECK_DIR = OUTPUT_DIR / "prechecks"
QA_DIR = OUTPUT_DIR

VALIDATION_DATES = ("2010-05-21", "2010-05-22", "2010-05-23")
FSS_WINDOWS_KM = (1, 3, 5, 10)
RECOMMENDATION_READY = "ready to report DWH Phase 3C scientific results"
RECOMMENDATION_BLOCKED_PREFIX = "not ready: "


def _json_default(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, np.ndarray):
        return value.tolist()
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return None if np.isnan(value) else float(value)
    if isinstance(value, pd.Timestamp):
        return _timestamp_iso(value)
    return str(value)


def _write_json(path: Path, payload: dict | list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, default=_json_default)
        handle.write("\n")


def _write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames: list[str] = []
    for row in rows:
        for key in row:
            if key not in fieldnames:
                fieldnames.append(key)
    with open(path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _load_yaml(path: Path) -> dict:
    with open(path, "r", encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def _truthy(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() == "true"
    return bool(value)


def window_cells_for_km(window_km: int, spec: ScoringGridSpec) -> int:
    return max(1, int(round((float(window_km) * 1000.0) / float(spec.resolution))))


def build_event_corridor_mask(masks: list[np.ndarray]) -> np.ndarray:
    if not masks:
        raise ValueError("At least one mask is required to build an event corridor.")
    stacked = np.stack([(np.asarray(mask, dtype=np.float32) > 0).astype(np.float32) for mask in masks], axis=0)
    return np.max(stacked, axis=0).astype(np.float32)


def _normalize_density(values: np.ndarray) -> np.ndarray:
    arr = np.nan_to_num(np.asarray(values, dtype=np.float32), nan=0.0, posinf=0.0, neginf=0.0)
    total = float(arr.sum())
    if total <= 0.0:
        return arr
    return (arr / total).astype(np.float32)


def _apply_sea_mask(values: np.ndarray, sea_mask: np.ndarray | None) -> np.ndarray:
    arr = np.nan_to_num(np.asarray(values, dtype=np.float32), nan=0.0, posinf=0.0, neginf=0.0)
    if sea_mask is None:
        return arr
    return np.where(np.asarray(sea_mask) > 0.5, arr, 0.0).astype(np.float32)


def _binary(values: np.ndarray) -> np.ndarray:
    return (np.nan_to_num(np.asarray(values, dtype=np.float32), nan=0.0, posinf=0.0, neginf=0.0) > 0).astype(np.float32)


def _mask_cell_centers(mask: np.ndarray, spec: ScoringGridSpec) -> np.ndarray:
    active = np.argwhere(np.asarray(mask) > 0)
    if active.size == 0:
        return np.empty((0, 2), dtype=np.float64)
    rows = active[:, 0]
    cols = active[:, 1]
    xs = spec.min_x + ((cols + 0.5) * spec.resolution)
    ys = spec.max_y - ((rows + 0.5) * spec.resolution)
    return np.column_stack([xs, ys]).astype(np.float64)


def mask_diagnostics(forecast: np.ndarray, observed: np.ndarray, spec: ScoringGridSpec, sea_mask: np.ndarray | None) -> dict:
    forecast_mask = _binary(_apply_sea_mask(forecast, sea_mask))
    obs_mask = _binary(_apply_sea_mask(observed, sea_mask))
    forecast_nonzero = int(np.count_nonzero(forecast_mask))
    obs_nonzero = int(np.count_nonzero(obs_mask))
    intersection = int(np.count_nonzero((forecast_mask > 0) & (obs_mask > 0)))
    union = int(np.count_nonzero((forecast_mask > 0) | (obs_mask > 0)))
    area_ratio = np.nan if obs_nonzero == 0 else float(forecast_nonzero / obs_nonzero)
    iou = float(intersection / union) if union > 0 else 1.0
    dice_denom = forecast_nonzero + obs_nonzero
    dice = float((2.0 * intersection) / dice_denom) if dice_denom > 0 else 1.0

    forecast_points = _mask_cell_centers(forecast_mask, spec)
    obs_points = _mask_cell_centers(obs_mask, spec)
    centroid_distance_m = np.nan
    if len(forecast_points) and len(obs_points):
        centroid_distance_m = float(np.linalg.norm(forecast_points.mean(axis=0) - obs_points.mean(axis=0)))

    nearest_distance_to_obs_m = np.nan
    if intersection > 0:
        nearest_distance_to_obs_m = 0.0
    elif len(forecast_points) and len(obs_points):
        if cKDTree is not None:
            distances, _ = cKDTree(obs_points).query(forecast_points, k=1)
            nearest_distance_to_obs_m = float(np.min(distances))
        else:  # pragma: no cover
            deltas = forecast_points[:, None, :] - obs_points[None, :, :]
            nearest_distance_to_obs_m = float(np.sqrt(np.sum(deltas * deltas, axis=2)).min())

    return {
        "forecast_nonzero_cells": forecast_nonzero,
        "obs_nonzero_cells": obs_nonzero,
        "area_ratio_forecast_to_obs": area_ratio,
        "centroid_distance_m": centroid_distance_m,
        "iou": iou,
        "dice": dice,
        "nearest_distance_to_obs_m": nearest_distance_to_obs_m,
        "intersection_cells": intersection,
        "union_cells": union,
        "ocean_cell_count": int(np.count_nonzero(sea_mask > 0.5)) if sea_mask is not None else int(forecast_mask.size),
    }


def _load_scientific_status_rows() -> list[dict]:
    status_path = FORCING_READY_DIR / "dwh_scientific_forcing_status.json"
    if not status_path.exists():
        raise FileNotFoundError(f"DWH scientific forcing status is missing: {status_path}")
    with open(status_path, "r", encoding="utf-8") as handle:
        return json.load(handle) or []


def load_frozen_scientific_forcing_stack(status_rows: list[dict] | None = None) -> dict[str, dict]:
    rows = status_rows if status_rows is not None else _load_scientific_status_rows()
    stack: dict[str, dict] = {}
    for role in ("current", "wind", "wave"):
        row = next((item for item in rows if item.get("source_role") == role), None)
        if row is None:
            raise RuntimeError(f"Scientific forcing stack is missing required {role} row.")
        if not _truthy(row.get("scientific_ready")):
            raise RuntimeError(f"Scientific {role} forcing is not ready: {row.get('exact_reason_if_false') or row}")
        path = Path(str(row.get("local_file_path") or ""))
        if not path.exists():
            raise FileNotFoundError(f"Scientific {role} forcing file is missing: {path}")
        if path_is_smoke_only(path):
            raise RuntimeError(f"Rejected smoke/non-scientific {role} forcing path: {path}")
        with xr.open_dataset(path) as ds:
            if attrs_mark_smoke_only(dict(ds.attrs)):
                raise RuntimeError(f"Rejected smoke-only {role} forcing attrs in {path}")
        stack[role] = {**row, "local_file_path": str(path)}
    return stack


def _project_lonlat_to_grid(spec: ScoringGridSpec, lon: np.ndarray, lat: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    lon_arr = np.asarray(lon, dtype=float)
    lat_arr = np.asarray(lat, dtype=float)
    if str(spec.crs).upper() == GEOGRAPHIC_CRS:
        return lon_arr, lat_arr
    if rio_transform is None:
        raise ImportError("rasterio is required to project particles onto the DWH scoring grid")
    x_vals, y_vals = rio_transform(GEOGRAPHIC_CRS, spec.crs, lon_arr.tolist(), lat_arr.tolist())
    return np.asarray(x_vals, dtype=float), np.asarray(y_vals, dtype=float)


def rasterize_particles_to_spec(
    spec: ScoringGridSpec,
    lon: np.ndarray,
    lat: np.ndarray,
    mass: np.ndarray | None = None,
) -> tuple[np.ndarray, np.ndarray]:
    if len(lon) == 0 or len(lat) == 0:
        empty = np.zeros((spec.height, spec.width), dtype=np.float32)
        return empty, empty
    x_vals, y_vals = _project_lonlat_to_grid(spec, lon, lat)
    counts, _, _ = np.histogram2d(y_vals, x_vals, bins=[spec.y_bins, spec.x_bins])
    counts = np.flipud(counts)
    hits = (counts > 0).astype(np.float32)
    weights = np.ones_like(np.asarray(lon, dtype=float), dtype=np.float32) if mass is None else np.asarray(mass, dtype=np.float32)
    density, _, _ = np.histogram2d(y_vals, x_vals, bins=[spec.y_bins, spec.x_bins], weights=weights)
    density = np.flipud(density).astype(np.float32)
    return hits, _normalize_density(density)


def _sample_points_from_init_polygon(path: Path, count: int, random_seed: int, spec: ScoringGridSpec) -> tuple[list[float], list[float]]:
    if gpd is None:
        raise ImportError("geopandas is required to seed the DWH initialization polygon")
    gdf = gpd.read_file(path)
    if gdf.crs is None:
        gdf = gdf.set_crs(GEOGRAPHIC_CRS)
    work = gdf.to_crs(spec.crs).dropna(subset=["geometry"]).copy()
    if work.empty:
        raise ValueError(f"No valid initialization geometry found in {path}")
    weights = work.geometry.apply(lambda geom: geom.area).to_numpy(dtype=float)
    if not np.isfinite(weights).all() or weights.sum() <= 0:
        weights = np.ones(len(work), dtype=float)
    rng = np.random.default_rng(random_seed)
    counts = rng.multinomial(int(count), weights / weights.sum())
    parts = []
    for geometry, item_count in zip(work.geometry, counts):
        if item_count <= 0:
            continue
        samples = (
            gpd.GeoSeries([geometry], crs=work.crs)
            .sample_points(int(item_count), rng=rng)
            .explode(index_parts=False)
            .reset_index(drop=True)
        )
        parts.append(samples)
    if not parts:
        raise ValueError("Polygon seeding did not generate any DWH sample points")
    points = gpd.GeoSeries(pd.concat(parts, ignore_index=True), crs=work.crs).to_crs(GEOGRAPHIC_CRS)
    return points.x.tolist(), points.y.tolist()


def _source_point_lat_lon(path: Path) -> tuple[float, float]:
    if gpd is None:
        raise ImportError("geopandas is required to load the DWH wellhead source point")
    gdf = gpd.read_file(path)
    if gdf.crs is None:
        gdf = gdf.set_crs(GEOGRAPHIC_CRS)
    point = gdf.to_crs(GEOGRAPHIC_CRS).geometry.dropna().iloc[0]
    return float(point.y), float(point.x)


def _extract_opendrift_snapshot(nc_path: Path, target_time: str | pd.Timestamp) -> tuple[np.ndarray, np.ndarray, np.ndarray, pd.Timestamp]:
    with xr.open_dataset(nc_path) as ds:
        times = pd.DatetimeIndex(pd.to_datetime(ds["time"].values))
        if times.tz is not None:
            times = times.tz_convert("UTC").tz_localize(None)
        target = _timestamp(target_time)
        idx = int(np.abs(times - target).argmin())
        actual_time = pd.Timestamp(times[idx])
        lon = np.asarray(ds["lon"].isel(time=idx).values).reshape(-1)
        lat = np.asarray(ds["lat"].isel(time=idx).values).reshape(-1)
        status = np.asarray(ds["status"].isel(time=idx).values).reshape(-1)
        if "mass_oil" in ds:
            mass = np.asarray(ds["mass_oil"].isel(time=idx).values).reshape(-1)
        else:
            mass = np.ones_like(lon, dtype=np.float32)
    valid = np.isfinite(lon) & np.isfinite(lat) & (status == 0)
    return lon[valid], lat[valid], mass[valid], actual_time


def _extract_opendrift_date_composite(nc_path: Path, date_value: str, spec: ScoringGridSpec, sea_mask: np.ndarray | None) -> tuple[np.ndarray, np.ndarray, dict]:
    footprint = np.zeros((spec.height, spec.width), dtype=np.float32)
    density_accum = np.zeros_like(footprint)
    active_snapshots = 0
    active_particles_total = 0
    with xr.open_dataset(nc_path) as ds:
        times = pd.DatetimeIndex(pd.to_datetime(ds["time"].values))
        if times.tz is not None:
            times = times.tz_convert("UTC").tz_localize(None)
        target_date = pd.Timestamp(date_value).date()
        indices = [idx for idx, value in enumerate(times) if value.date() == target_date]
        for idx in indices:
            lon = np.asarray(ds["lon"].isel(time=idx).values).reshape(-1)
            lat = np.asarray(ds["lat"].isel(time=idx).values).reshape(-1)
            status = np.asarray(ds["status"].isel(time=idx).values).reshape(-1)
            if "mass_oil" in ds:
                mass = np.asarray(ds["mass_oil"].isel(time=idx).values).reshape(-1)
            else:
                mass = np.ones_like(lon, dtype=np.float32)
            valid = np.isfinite(lon) & np.isfinite(lat) & (status == 0)
            if not np.any(valid):
                continue
            active_snapshots += 1
            active_particles_total += int(np.count_nonzero(valid))
            hits, density = rasterize_particles_to_spec(spec, lon[valid], lat[valid], mass[valid])
            footprint = np.maximum(footprint, hits)
            density_accum += density
    footprint = _apply_sea_mask(footprint, sea_mask)
    density = _apply_sea_mask(_normalize_density(density_accum), sea_mask)
    return footprint, density, {
        "date_utc": date_value,
        "active_snapshots": active_snapshots,
        "active_particles_total": active_particles_total,
    }


class DWHPhase3CExternalCaseRunService:
    def __init__(
        self,
        *,
        config_path: str | Path = CONFIG_PATH,
        setup_dir: str | Path = SETUP_DIR,
        forcing_ready_dir: str | Path = FORCING_READY_DIR,
        output_dir: str | Path = OUTPUT_DIR,
    ):
        self.case = get_case_context()
        if self.case.workflow_mode != "dwh_retro_2010":
            raise RuntimeError(f"{PHASE3C_RUN_PHASE} requires WORKFLOW_MODE=dwh_retro_2010.")
        self.config_path = Path(config_path)
        self.setup_dir = Path(setup_dir)
        self.forcing_ready_dir = Path(forcing_ready_dir)
        self.output_dir = Path(output_dir)
        self.track_dir = self.output_dir / "tracks"
        self.product_dir = self.output_dir / "products"
        self.precheck_dir = self.output_dir / "prechecks"
        self.cfg = _load_yaml(self.config_path)
        self.spec = load_dwh_scoring_grid_spec(self.setup_dir / "scoring_grid.yaml")
        self.sea_mask = self._load_sea_mask()
        self.validation_dates = tuple(str(date) for date in self.cfg.get("accepted_validation_dates", VALIDATION_DATES))
        self.start_utc = str(self.cfg.get("simulation_start_utc", "2010-05-20T00:00:00Z"))
        self.end_utc = str(self.cfg.get("simulation_end_utc", "2010-05-23T23:59:59Z"))
        forecast_cfg = self.cfg.get("forecast") or {}
        self.element_count = int(forecast_cfg.get("element_count", 5000))
        self.random_seed = int(forecast_cfg.get("polygon_seed_random_seed", 20100520))
        self.provisional_transport_model = bool(forecast_cfg.get("provisional_transport_model", True))
        self.grid_id = f"{CASE_ID}_{self.spec.crs.replace(':', '')}_{int(self.spec.resolution)}m"

    def _load_sea_mask(self) -> np.ndarray | None:
        sea_mask_path = Path(self.spec.sea_mask_path) if self.spec.sea_mask_path else self.setup_dir / "sea_mask.tif"
        if not sea_mask_path.exists():
            return None
        return _load_raster(sea_mask_path)[0]

    def _assert_required_setup_artifacts(self) -> None:
        required = [
            self.setup_dir / "processed" / "layer_05_dwh_t20100520_composite_processed.gpkg",
            self.setup_dir / "processed" / "layer_00_dwh_wellhead_processed.gpkg",
            self.setup_dir / "scoring_grid.yaml",
            self.setup_dir / "sea_mask.tif",
        ]
        required.extend(self.setup_dir / f"obs_mask_{date}.tif" for date in self.validation_dates)
        missing = [str(path) for path in required if not path.exists()]
        if missing:
            raise FileNotFoundError("DWH Phase 3C run is missing required setup artifacts: " + "; ".join(missing))

    def _load_frozen_stack(self) -> dict[str, dict]:
        rows = _load_scientific_status_rows()
        return load_frozen_scientific_forcing_stack(rows)

    def run(self) -> dict:
        self._assert_required_setup_artifacts()
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.track_dir.mkdir(parents=True, exist_ok=True)
        self.product_dir.mkdir(parents=True, exist_ok=True)
        self.precheck_dir.mkdir(parents=True, exist_ok=True)

        stack = self._load_frozen_stack()
        deterministic = self._run_deterministic(stack)
        product_records = []
        if deterministic["success"]:
            product_records = self._write_deterministic_products(Path(deterministic["track_path"]))
        ensemble = self._ensemble_deferred_record()
        pygnome = self._pygnome_deferred_record()

        scoring = self._score_products(product_records) if deterministic["success"] else self._empty_scoring()
        qa_paths = self._write_qa_artifacts(scoring["pairing_rows"]) if deterministic["success"] else []
        memo_path = self._write_chapter3_memo(pygnome)
        recommendation, blocker = self._recommendation(deterministic, scoring)

        tracks_registry_path = self.output_dir / "phase3c_tracks_registry.csv"
        pairing_manifest_path = self.output_dir / "phase3c_pairing_manifest.csv"
        fss_path = self.output_dir / "phase3c_fss_by_date_window.csv"
        diagnostics_path = self.output_dir / "phase3c_diagnostics.csv"
        summary_path = self.output_dir / "phase3c_summary.csv"
        event_summary_path = self.output_dir / "phase3c_eventcorridor_summary.csv"
        run_manifest_path = self.output_dir / "phase3c_run_manifest.json"

        track_rows = [deterministic, ensemble, pygnome]
        _write_csv(tracks_registry_path, track_rows)
        _write_csv(pairing_manifest_path, scoring["pairing_rows"])
        _write_csv(fss_path, scoring["fss_rows"])
        _write_csv(diagnostics_path, scoring["diagnostics_rows"])
        _write_csv(summary_path, scoring["summary_rows"])
        _write_csv(event_summary_path, scoring["event_summary_rows"])

        manifest = {
            "case_id": CASE_ID,
            "phase": PHASE3C_RUN_PHASE,
            "phase_name": "Phase 3C - External Rich-Data Spill Transfer Validation",
            "mindoro_replaced": False,
            "thesis_role": "external_transfer_validation_support_case",
            "main_case_reference": "mindoro_retro_2023",
            "main_case_note": "Mindoro remains the main Philippine thesis case; DWH is a separate external transfer-validation story.",
            "truth_source": "public DWH observation-derived daily masks",
            "truth_role": "observed_dwh_daily_masks_are_truth",
            "pygnome_role": "comparator_only",
            "date_composite_logic_used": True,
            "date_composite_note": "Public DWH daily layers do not support defensible exact sub-daily acquisition times.",
            "forcing_selection_rule": "Freeze the first complete real historical current+wind+wave stack that passes the scientific-readiness gate; do not use Phase 1 drifter-selected baseline logic.",
            "current_forcing_stack": "HYCOM GOFS 3.1 currents + ERA5 winds + CMEMS wave/Stokes",
            "initialization_mode": self.cfg.get("initialization_mode"),
            "initialization_polygon_layer_id": self.cfg.get("initialization_polygon_layer_id"),
            "validation_polygon_layer_ids": self.cfg.get("validation_polygon_layer_ids"),
            "source_point_layer_id": self.cfg.get("source_point_layer_id"),
            "source_point_role": self.cfg.get("source_point_role"),
            "transport_model": "OpenDrift OceanDrift",
            "provisional_transport_model": self.provisional_transport_model,
            "deterministic_success": deterministic["success"],
            "ensemble_success": ensemble["success"],
            "pygnome_comparator_success": pygnome["success"],
            "scoring_grid": {
                "grid_id": self.grid_id,
                "crs": self.spec.crs,
                "resolution_m": self.spec.resolution,
                "metadata_path": self.spec.metadata_path,
            },
            "forcing_stack": stack,
            "outputs": {
                "tracks_registry_csv": str(tracks_registry_path),
                "pairing_manifest_csv": str(pairing_manifest_path),
                "fss_by_date_window_csv": str(fss_path),
                "diagnostics_csv": str(diagnostics_path),
                "summary_csv": str(summary_path),
                "eventcorridor_summary_csv": str(event_summary_path),
                "chapter3_memo": str(memo_path),
                "qa_artifacts": [str(path) for path in qa_paths if Path(path).exists()],
            },
            "recommendation": recommendation,
            "single_biggest_blocker": blocker,
        }
        _write_json(run_manifest_path, manifest)

        return {
            "output_dir": str(self.output_dir),
            "tracks_registry_csv": str(tracks_registry_path),
            "pairing_manifest_csv": str(pairing_manifest_path),
            "fss_by_date_window_csv": str(fss_path),
            "diagnostics_csv": str(diagnostics_path),
            "summary_csv": str(summary_path),
            "eventcorridor_summary_csv": str(event_summary_path),
            "run_manifest_json": str(run_manifest_path),
            "memo": str(memo_path),
            "deterministic_success": deterministic["success"],
            "ensemble_success": ensemble["success"],
            "pygnome_comparator_success": pygnome["success"],
            "summary_rows": scoring["summary_rows"],
            "event_summary_rows": scoring["event_summary_rows"],
            "headline_fss": self._headline_fss(scoring["summary_rows"]),
            "eventcorridor_fss": self._headline_event_fss(scoring["event_summary_rows"]),
            "recommendation": recommendation,
            "single_biggest_blocker": blocker,
        }

    def _run_deterministic(self, stack: dict[str, dict]) -> dict:
        init_path = self.setup_dir / "processed" / "layer_05_dwh_t20100520_composite_processed.gpkg"
        source_path = self.setup_dir / "processed" / "layer_00_dwh_wellhead_processed.gpkg"
        track_path = self.track_dir / "opendrift_control_dwh_phase3c.nc"
        loading_audit_path = self.output_dir / "phase3c_loading_audit.json"
        seed_lons, seed_lats = _sample_points_from_init_polygon(init_path, self.element_count, self.random_seed, self.spec)
        source_lat, source_lon = _source_point_lat_lon(source_path)

        audit = {
            "case_id": CASE_ID,
            "phase": PHASE3C_RUN_PHASE,
            "track_id": "opendrift_control",
            "transport_model": "OpenDrift OceanDrift",
            "scientific_ready_forcing_required": True,
            "non_scientific_smoke": False,
            "source_is_smoke_only": False,
            "currents_attached": False,
            "winds_attached": False,
            "waves_attached": False,
            "element_count": self.element_count,
            "random_seed": self.random_seed,
            "initialization_polygon_path": str(init_path),
            "source_point_path": str(source_path),
            "source_point_role": self.cfg.get("source_point_role"),
            "provenance_source_point_lat": source_lat,
            "provenance_source_point_lon": source_lon,
            "simulation_start_utc": _timestamp_iso(self.start_utc),
            "simulation_end_utc": _timestamp_iso(self.end_utc),
        }

        try:
            from opendrift.models.oceandrift import OceanDrift
            from opendrift.readers import reader_netCDF_CF_generic

            model = OceanDrift(loglevel=30)
            readers = []
            for role in ("current", "wind", "wave"):
                reader = reader_netCDF_CF_generic.Reader(stack[role]["local_file_path"])
                readers.append(reader)
                if role == "current":
                    audit["currents_attached"] = True
                elif role == "wind":
                    audit["winds_attached"] = True
                else:
                    audit["waves_attached"] = True
            model.add_reader(readers)
            model.seed_elements(
                lon=seed_lons,
                lat=seed_lats,
                number=self.element_count,
                time=_timestamp(self.start_utc).to_pydatetime(),
            )
            if track_path.exists():
                track_path.unlink()
            model.run(
                time_step=timedelta(hours=1),
                time_step_output=timedelta(hours=1),
                end_time=_timestamp(self.end_utc),
                outfile=str(track_path),
            )
            audit["success"] = True
            audit["track_path"] = str(track_path)
            audit["stop_reason"] = ""
        except Exception as exc:
            audit["success"] = False
            audit["track_path"] = str(track_path) if track_path.exists() else ""
            audit["stop_reason"] = f"{type(exc).__name__}: {exc}"

        _write_json(loading_audit_path, audit)
        return {
            "track_id": "opendrift_control",
            "model_family": "OpenDrift",
            "transport_model": "OceanDrift",
            "run_role": "deterministic_control",
            "success": audit["success"],
            "track_path": audit["track_path"],
            "element_count": self.element_count,
            "member_count": 1,
            "non_scientific_smoke": False,
            "scientific_ready_forcing": True,
            "currents_attached": audit["currents_attached"],
            "winds_attached": audit["winds_attached"],
            "waves_attached": audit["waves_attached"],
            "provisional_transport_model": self.provisional_transport_model,
            "stop_reason": audit["stop_reason"],
            "loading_audit_json": str(loading_audit_path),
        }

    def _write_deterministic_products(self, track_path: Path) -> list[dict]:
        records: list[dict] = []
        for date in self.validation_dates:
            target_time = f"{date}T23:59:59Z"
            lon, lat, mass, actual_time = _extract_opendrift_snapshot(track_path, target_time)
            footprint, density = rasterize_particles_to_spec(self.spec, lon, lat, mass)
            footprint = _apply_sea_mask(footprint, self.sea_mask)
            density = _apply_sea_mask(density, self.sea_mask)
            label = _timestamp_label(target_time)
            footprint_path = self.product_dir / f"control_footprint_mask_{label}.tif"
            density_path = self.product_dir / f"control_density_norm_{label}.tif"
            _write_raster(self.spec, footprint.astype(np.float32), footprint_path)
            _write_raster(self.spec, density.astype(np.float32), density_path)
            records.extend(
                [
                    {
                        "track_id": "opendrift_control",
                        "product_type": "control_footprint_mask",
                        "timestamp_utc": _timestamp_iso(target_time),
                        "actual_snapshot_time_utc": _timestamp_iso(actual_time),
                        "date_utc": date,
                        "path": str(footprint_path),
                        "score_candidate": False,
                    },
                    {
                        "track_id": "opendrift_control",
                        "product_type": "control_density_norm",
                        "timestamp_utc": _timestamp_iso(target_time),
                        "actual_snapshot_time_utc": _timestamp_iso(actual_time),
                        "date_utc": date,
                        "path": str(density_path),
                        "score_candidate": False,
                    },
                ]
            )

            composite_footprint, composite_density, composite_meta = _extract_opendrift_date_composite(
                track_path,
                date,
                self.spec,
                self.sea_mask,
            )
            composite_footprint_path = self.product_dir / f"control_footprint_mask_{date}_datecomposite.tif"
            composite_density_path = self.product_dir / f"control_density_norm_{date}_datecomposite.tif"
            _write_raster(self.spec, composite_footprint.astype(np.float32), composite_footprint_path)
            _write_raster(self.spec, composite_density.astype(np.float32), composite_density_path)
            records.extend(
                [
                    {
                        "track_id": "opendrift_control",
                        "product_type": "control_footprint_mask_datecomposite",
                        "date_utc": date,
                        "path": str(composite_footprint_path),
                        "score_candidate": True,
                        **composite_meta,
                    },
                    {
                        "track_id": "opendrift_control",
                        "product_type": "control_density_norm_datecomposite",
                        "date_utc": date,
                        "path": str(composite_density_path),
                        "score_candidate": False,
                        **composite_meta,
                    },
                ]
            )
        _write_csv(self.output_dir / "phase3c_product_registry.csv", records)
        _write_json(self.output_dir / "phase3c_product_registry.json", records)
        return records

    def _ensemble_deferred_record(self) -> dict:
        return {
            "track_id": "opendrift_ensemble_p50",
            "model_family": "OpenDrift",
            "transport_model": "OceanDrift",
            "run_role": "ensemble_50_member",
            "success": False,
            "track_path": "",
            "element_count": "",
            "member_count": 50,
            "non_scientific_smoke": False,
            "scientific_ready_forcing": True,
            "currents_attached": False,
            "winds_attached": False,
            "waves_attached": False,
            "provisional_transport_model": self.provisional_transport_model,
            "stop_reason": "deferred: DWH 50-member ensemble perturbation semantics are not yet implemented cleanly for this external case phase",
        }

    def _pygnome_deferred_record(self) -> dict:
        try:
            import gnome  # noqa: F401

            reason = "deferred: PyGNOME DWH case wiring is not implemented for this phase; observed DWH masks remain truth"
        except Exception as exc:
            reason = f"deferred: PyGNOME comparator unavailable in this pipeline environment ({type(exc).__name__}: {exc})"
        return {
            "track_id": "pygnome_deterministic",
            "model_family": "PyGNOME",
            "transport_model": "PyGNOME",
            "run_role": "deterministic_comparator",
            "success": False,
            "track_path": "",
            "element_count": "",
            "member_count": 1,
            "non_scientific_smoke": False,
            "scientific_ready_forcing": "",
            "currents_attached": False,
            "winds_attached": False,
            "waves_attached": False,
            "provisional_transport_model": "",
            "stop_reason": reason,
        }

    def _empty_scoring(self) -> dict:
        return {"pairing_rows": [], "fss_rows": [], "diagnostics_rows": [], "summary_rows": [], "event_summary_rows": []}

    def _score_products(self, product_records: list[dict]) -> dict:
        forecast_by_date = {
            str(row["date_utc"]): Path(row["path"])
            for row in product_records
            if row.get("product_type") == "control_footprint_mask_datecomposite"
        }
        pairing_rows = []
        fss_rows = []
        diagnostics_rows = []
        forecast_event_masks = []
        obs_event_masks = []

        for date in self.validation_dates:
            forecast_path = forecast_by_date[date]
            obs_path = self.setup_dir / f"obs_mask_{date}.tif"
            pair_id = f"dwh_opendrift_control_{date}"
            pair = self._score_pair(
                pair_id=pair_id,
                pair_role="per_date",
                forecast_path=forecast_path,
                obs_path=obs_path,
                pairing_date_utc=date,
                forecast_product_type="control_footprint_mask_datecomposite",
                observation_product_type="public_dwh_observation_mask",
            )
            pairing_rows.append(pair["pairing"])
            diagnostics_rows.append(pair["diagnostics"])
            fss_rows.extend(pair["fss_rows"])
            forecast_event_masks.append(pair["forecast_mask"])
            obs_event_masks.append(pair["obs_mask"])

        forecast_event = build_event_corridor_mask(forecast_event_masks)
        obs_event = build_event_corridor_mask(obs_event_masks)
        forecast_event_path = self.product_dir / "control_footprint_mask_2010-05-21_2010-05-23_eventcorridor.tif"
        obs_event_path = self.product_dir / "obs_mask_2010-05-21_2010-05-23_eventcorridor.tif"
        _write_raster(self.spec, _apply_sea_mask(forecast_event, self.sea_mask), forecast_event_path)
        _write_raster(self.spec, _apply_sea_mask(obs_event, self.sea_mask), obs_event_path)
        event_pair = self._score_pair(
            pair_id="dwh_opendrift_control_eventcorridor_2010-05-21_2010-05-23",
            pair_role="event_corridor",
            forecast_path=forecast_event_path,
            obs_path=obs_event_path,
            pairing_date_utc="2010-05-21/2010-05-23",
            forecast_product_type="control_footprint_mask_eventcorridor",
            observation_product_type="public_dwh_observation_mask_eventcorridor",
        )
        pairing_rows.append(event_pair["pairing"])
        diagnostics_rows.append(event_pair["diagnostics"])
        fss_rows.extend(event_pair["fss_rows"])

        summary_rows = self._build_summary_rows(pairing_rows, diagnostics_rows, fss_rows)
        event_summary_rows = [row for row in summary_rows if row["pair_role"] == "event_corridor"]
        return {
            "pairing_rows": pairing_rows,
            "fss_rows": fss_rows,
            "diagnostics_rows": diagnostics_rows,
            "summary_rows": summary_rows,
            "event_summary_rows": event_summary_rows,
        }

    def _score_pair(
        self,
        *,
        pair_id: str,
        pair_role: str,
        track_id: str = "opendrift_control",
        forecast_path: Path,
        obs_path: Path,
        pairing_date_utc: str,
        forecast_product_type: str,
        observation_product_type: str,
    ) -> dict:
        precheck_base = self.precheck_dir / pair_id
        precheck = _same_grid_precheck(forecast_path, obs_path)
        precheck_json = precheck_base.with_suffix(".json")
        precheck_csv = precheck_base.with_suffix(".csv")
        _write_json(precheck_json, {"pair_id": pair_id, **precheck})
        _write_csv(precheck_csv, [{"pair_id": pair_id, "passed": precheck["passed"], **precheck["checks"]}])
        if not precheck["passed"]:
            raise RuntimeError(f"DWH Phase 3C same-grid precheck failed for {pair_id}: {precheck_json}")

        forecast_raw, _ = _load_raster(forecast_path)
        obs_raw, _ = _load_raster(obs_path)
        forecast_mask = _binary(_apply_sea_mask(forecast_raw, self.sea_mask))
        obs_mask = _binary(_apply_sea_mask(obs_raw, self.sea_mask))
        diagnostics = {
            "pair_id": pair_id,
            "pair_role": pair_role,
            "track_id": track_id,
            "pairing_date_utc": pairing_date_utc,
            "forecast_path": str(forecast_path),
            "observation_path": str(obs_path),
            **mask_diagnostics(forecast_mask, obs_mask, self.spec, self.sea_mask),
        }
        fss_rows = []
        valid_mask = (self.sea_mask > 0.5) if self.sea_mask is not None else None
        for window_km in FSS_WINDOWS_KM:
            window_cells = window_cells_for_km(window_km, self.spec)
            fss_rows.append(
                {
                    "pair_id": pair_id,
                    "pair_role": pair_role,
                    "track_id": track_id,
                    "metric": "fractions_skill_score",
                    "window_km": window_km,
                    "window_cells": window_cells,
                    "fss": float(
                        calculate_fss(
                            forecast_mask,
                            obs_mask,
                            window=window_cells,
                            valid_mask=valid_mask,
                        )
                    ),
                    "pairing_date_utc": pairing_date_utc,
                    "forecast_product_type": forecast_product_type,
                    "observation_product_type": observation_product_type,
                    "forecast_path": str(forecast_path),
                    "observation_path": str(obs_path),
                    "date_composite_logic_used": True,
                    "truth_source": "public_dwh_observation_derived_mask",
                }
            )
        pairing = {
            "pair_id": pair_id,
            "pair_role": pair_role,
            "track_id": track_id,
            "forecast_product_type": forecast_product_type,
            "forecast_path": str(forecast_path),
            "observation_product_type": observation_product_type,
            "observation_path": str(obs_path),
            "pairing_date_utc": pairing_date_utc,
            "source_semantics": "date-composite public DWH observations; no invented sub-daily acquisition time",
            "truth_source": "public_dwh_observation_derived_mask",
            "transport_model": "OpenDrift OceanDrift",
            "provisional_transport_model": self.provisional_transport_model,
            "grid_id": self.grid_id,
            "grid_crs": self.spec.crs,
            "precheck_csv": str(precheck_csv),
            "precheck_json": str(precheck_json),
        }
        return {
            "pairing": pairing,
            "diagnostics": diagnostics,
            "fss_rows": fss_rows,
            "forecast_mask": forecast_mask,
            "obs_mask": obs_mask,
        }

    def _build_summary_rows(self, pairing_rows: list[dict], diagnostics_rows: list[dict], fss_rows: list[dict]) -> list[dict]:
        diagnostics_by_pair = {row["pair_id"]: row for row in diagnostics_rows}
        fss_by_pair: dict[str, dict[str, float]] = {}
        for row in fss_rows:
            fss_by_pair.setdefault(row["pair_id"], {})[f"fss_{int(row['window_km'])}km"] = float(row["fss"])
        summary = []
        for pair in pairing_rows:
            diag = diagnostics_by_pair.get(pair["pair_id"], {})
            summary.append(
                {
                    **pair,
                    "forecast_nonzero_cells": diag.get("forecast_nonzero_cells", np.nan),
                    "obs_nonzero_cells": diag.get("obs_nonzero_cells", np.nan),
                    "area_ratio_forecast_to_obs": diag.get("area_ratio_forecast_to_obs", np.nan),
                    "centroid_distance_m": diag.get("centroid_distance_m", np.nan),
                    "iou": diag.get("iou", np.nan),
                    "dice": diag.get("dice", np.nan),
                    "nearest_distance_to_obs_m": diag.get("nearest_distance_to_obs_m", np.nan),
                    "ocean_cell_count": diag.get("ocean_cell_count", np.nan),
                    **fss_by_pair.get(pair["pair_id"], {}),
                }
            )
        return summary

    def _write_qa_artifacts(self, pairing_rows: list[dict]) -> list[Path]:
        if plt is None:
            return []
        written: list[Path] = []
        per_date = [row for row in pairing_rows if row["pair_role"] == "per_date"]
        if per_date:
            out_path = self.output_dir / "qa_phase3c_overlays.png"
            try:
                fig, axes = plt.subplots(1, len(per_date), figsize=(5 * len(per_date), 5))
                if len(per_date) == 1:
                    axes = [axes]
                for ax, row in zip(axes, per_date):
                    forecast, _ = _load_raster(Path(row["forecast_path"]))
                    obs, _ = _load_raster(Path(row["observation_path"]))
                    ax.imshow(self._overlay_canvas(forecast, obs), origin="upper")
                    ax.set_title(row["pairing_date_utc"])
                    ax.set_axis_off()
                fig.savefig(out_path, dpi=150, bbox_inches="tight")
                plt.close(fig)
                written.append(out_path)
            except Exception:
                pass

        event_rows = [row for row in pairing_rows if row["pair_role"] == "event_corridor"]
        if event_rows:
            out_path = self.output_dir / "qa_phase3c_eventcorridor_overlay.png"
            try:
                row = event_rows[0]
                forecast, _ = _load_raster(Path(row["forecast_path"]))
                obs, _ = _load_raster(Path(row["observation_path"]))
                fig, ax = plt.subplots(figsize=(7, 6))
                ax.imshow(self._overlay_canvas(forecast, obs), origin="upper")
                ax.set_title("DWH Phase 3C event corridor, May 21-23")
                ax.set_axis_off()
                fig.savefig(out_path, dpi=150, bbox_inches="tight")
                plt.close(fig)
                written.append(out_path)
            except Exception:
                pass
        return written

    def _overlay_canvas(self, forecast: np.ndarray, obs: np.ndarray) -> np.ndarray:
        forecast_mask = _binary(_apply_sea_mask(forecast, self.sea_mask))
        obs_mask = _binary(_apply_sea_mask(obs, self.sea_mask))
        overlap = (forecast_mask > 0) & (obs_mask > 0)
        canvas = np.ones((forecast_mask.shape[0], forecast_mask.shape[1], 3), dtype=np.float32)
        canvas[obs_mask > 0] = np.array([0.2, 0.45, 0.95], dtype=np.float32)
        canvas[forecast_mask > 0] = np.array([0.95, 0.35, 0.2], dtype=np.float32)
        canvas[overlap] = np.array([0.55, 0.2, 0.75], dtype=np.float32)
        if self.sea_mask is not None:
            canvas[self.sea_mask <= 0.5] = np.array([0.86, 0.86, 0.86], dtype=np.float32)
        return canvas

    def _write_chapter3_memo(self, pygnome: dict) -> Path:
        path = self.output_dir / "chapter3_phase3c_external_case_run_memo.md"
        lines = [
            "# Chapter 3 Phase 3C External Case Run Memo",
            "",
            "Phase 3C is the external rich-data transfer-validation branch placed after Phase 3B and before Phase 4.",
            "",
            "Mindoro remains the main Philippine thesis case. Deepwater Horizon is a separate external transfer-validation/support case used to test whether the workflow carries into an observation-rich spill without replacing Mindoro as the main case.",
            "",
            "The DWH truth source is the public observation-derived daily mask set for May 21, May 22, and May 23, 2010. The cumulative DWH layer remains context-only and is not used as truth.",
            "",
            "The DWH run uses date-composite logic because the public daily layers do not support defensible exact sub-daily acquisition times.",
            "",
            "The DWH forcing family is chosen by scientific-readiness gating rather than by Phase 1 drifter-selected baseline logic. This branch freezes the first complete real historical current+wind+wave stack that is not smoke-only, spans the required DWH window, exposes the required variables with usable metadata, opens in the OpenDrift reader, and passes the reader-check forecast.",
            "",
            "In the current stored run, that frozen stack is HYCOM GOFS 3.1 currents + ERA5 winds + CMEMS wave/Stokes.",
            "",
            f"PyGNOME comparator run status: {pygnome['success']}. Any PyGNOME output remains comparator-only, not truth.",
        ]
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return path

    def _recommendation(self, deterministic: dict, scoring: dict) -> tuple[str, str]:
        if not deterministic["success"]:
            blocker = f"deterministic OpenDrift scientific run failed - {deterministic.get('stop_reason', 'unknown')}"
            return f"{RECOMMENDATION_BLOCKED_PREFIX}{blocker}", blocker
        if not scoring["summary_rows"]:
            blocker = "DWH scientific scoring products were not produced"
            return f"{RECOMMENDATION_BLOCKED_PREFIX}{blocker}", blocker
        if len([row for row in scoring["summary_rows"] if row["pair_role"] == "per_date"]) < len(self.validation_dates):
            blocker = "not all May 21/22/23 DWH per-date score rows were produced"
            return f"{RECOMMENDATION_BLOCKED_PREFIX}{blocker}", blocker
        if not scoring["event_summary_rows"]:
            blocker = "DWH May 21-23 event-corridor summary was not produced"
            return f"{RECOMMENDATION_BLOCKED_PREFIX}{blocker}", blocker
        return RECOMMENDATION_READY, ""

    def _headline_fss(self, summary_rows: list[dict]) -> dict[str, dict[str, float]]:
        out: dict[str, dict[str, float]] = {}
        for row in summary_rows:
            if row.get("pair_role") != "per_date":
                continue
            date = str(row.get("pairing_date_utc"))
            out[date] = {f"fss_{window}km": float(row.get(f"fss_{window}km", np.nan)) for window in FSS_WINDOWS_KM}
        return out

    def _headline_event_fss(self, event_summary_rows: list[dict]) -> dict[str, float]:
        if not event_summary_rows:
            return {}
        row = event_summary_rows[0]
        return {f"fss_{window}km": float(row.get(f"fss_{window}km", np.nan)) for window in FSS_WINDOWS_KM}


def run_phase3c_external_case_run() -> dict:
    return DWHPhase3CExternalCaseRunService().run()


__all__ = [
    "PHASE3C_RUN_PHASE",
    "build_event_corridor_mask",
    "load_frozen_scientific_forcing_stack",
    "mask_diagnostics",
    "rasterize_particles_to_spec",
    "run_phase3c_external_case_run",
    "window_cells_for_km",
]
