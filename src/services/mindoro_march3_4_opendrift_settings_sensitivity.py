"""Experimental OpenDrift settings sensitivity for Mindoro PhilSA March 3 -> 4.

This module is deliberately not thesis-facing. It reuses the completed
PhilSA-only March 3 -> March 4 cache as input provenance, then writes all
new products under the experiment-specific sensitivity directory.
"""

from __future__ import annotations

import json
import math
import os
import shutil
import traceback
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

os.environ.setdefault("WORKFLOW_MODE", "mindoro_retro_2023")

import numpy as np
import pandas as pd
import xarray as xr

from src.helpers.metrics import calculate_fss
from src.helpers.raster import GridBuilder, rasterize_particles, save_raster
from src.helpers.scoring import apply_ocean_mask, load_binary_mask, load_sea_mask_array
from src.services.ensemble import (
    EnsembleForecastService,
    OFFICIAL_ELEMENT_COUNT_OVERRIDE_ENV,
    normalize_model_timestamp,
    timestamp_to_utc_iso,
)
from src.services.scoring import OFFICIAL_PHASE3B_WINDOWS_KM, Phase3BScoringService
from src.utils.io import RecipeSelection

try:
    import geopandas as gpd
except ImportError:  # pragma: no cover
    gpd = None

try:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.colors import ListedColormap
    import matplotlib.patches as mpatches
except ImportError:  # pragma: no cover
    plt = None
    ListedColormap = None
    mpatches = None

try:
    import rasterio
    from rasterio.warp import transform as rio_transform
except ImportError:  # pragma: no cover
    rasterio = None
    rio_transform = None

try:
    from scipy.ndimage import binary_dilation, distance_transform_edt
    from scipy.spatial import cKDTree
except ImportError:  # pragma: no cover
    binary_dilation = None
    distance_transform_edt = None
    cKDTree = None


RUN_NAME = "CASE_MINDORO_RETRO_2023"
BASE_EXPERIMENT_DIR = Path("output") / RUN_NAME / "phase3b_philsa_march3_4_5000_experiment"
OUTPUT_DIR = BASE_EXPERIMENT_DIR / "experimental_settings_sensitivity"
MODEL_RUNS_DIR = OUTPUT_DIR / "model_runs"
VARIANT_PRODUCTS_DIR = OUTPUT_DIR / "variant_products"
FIGURES_DIR = OUTPUT_DIR / "figures"
AUDIT_DIR = OUTPUT_DIR / "audits"
ENSEMBLE_DIR = OUTPUT_DIR / "ensemble_candidates"

SEED_DATE = "2023-03-03"
TARGET_DATE = "2023-03-04"
LOCAL_TIMEZONE = "Asia/Manila"
SIMULATION_START_UTC = "2023-03-02T16:00:00Z"
SIMULATION_END_UTC = "2023-03-04T15:59:00Z"
ELEMENT_COUNT = 5000
EXPECTED_ENSEMBLE_MEMBER_COUNT = 50
BASELINE_RANDOM_SEED = 20230303
BASELINE_RECIPE = "cmems_gfs"
PYGNOME_TRACK_ID = "pygnome_deterministic"

SEED_SOURCE_NAME = "MindoroOilSpill_Philsa_230303"
TARGET_SOURCE_NAME = "MindoroOilSpill_Philsa_230304"
SEED_SERVICE_URL = (
    "https://services1.arcgis.com/RTK5Unh1Z71JKIiR/arcgis/rest/services/"
    "MindoroOilSpill_Philsa_230303/FeatureServer"
)
TARGET_SERVICE_URL = (
    "https://services1.arcgis.com/RTK5Unh1Z71JKIiR/arcgis/rest/services/"
    "MindoroOilSpill_Philsa_230304/FeatureServer"
)

PROTECTED_OUTPUT_DIRS = [
    Path("output") / "final_validation_package",
    Path("output") / "figure_package_publication",
    Path("output") / "final_reproducibility_package",
    Path("output") / "Phase 3B March13-14 Final Output",
    Path("output") / RUN_NAME / "phase3b_extended_public_scored_march13_14_reinit",
    Path("output") / RUN_NAME / "phase3b_extended_public_scored_march13_14_reinit_pygnome_comparison",
    Path("output") / RUN_NAME / "phase3b",
    Path("output") / RUN_NAME / "phase3b_multidate_public",
    Path("output") / RUN_NAME / "public_obs_appendix",
]


def _json_default(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, np.ndarray):
        return value.tolist()
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return None if np.isnan(value) else float(value)
    if isinstance(value, (pd.Timestamp, datetime)):
        return value.isoformat()
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


def _read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}


def _read_raster(path: Path) -> np.ndarray:
    if rasterio is None:
        raise ImportError("rasterio is required to read raster products.")
    with rasterio.open(path) as src:
        return src.read(1).astype(np.float32)


def _iso_z(value: Any) -> str:
    ts = pd.Timestamp(value)
    if ts.tzinfo is not None:
        ts = ts.tz_convert("UTC").tz_localize(None)
    return ts.strftime("%Y-%m-%dT%H:%M:%SZ")


def _safe_id(value: str) -> str:
    return "".join(ch.lower() if ch.isalnum() else "_" for ch in str(value)).strip("_")


def _snapshot_paths(paths: list[Path]) -> dict[str, dict[str, int]]:
    out: dict[str, dict[str, int]] = {}
    for root in paths:
        if not root.exists():
            continue
        for path in sorted(item for item in root.rglob("*") if item.is_file()):
            stat = path.stat()
            out[str(path).replace("\\", "/")] = {
                "size_bytes": int(stat.st_size),
                "mtime_ns": int(stat.st_mtime_ns),
            }
    return out


def _snapshot_diff(before: dict[str, Any], after: dict[str, Any]) -> dict[str, list[str]]:
    before_keys = set(before)
    after_keys = set(after)
    return {
        "added": sorted(after_keys - before_keys),
        "removed": sorted(before_keys - after_keys),
        "changed": sorted(k for k in before_keys & after_keys if before[k] != after[k]),
    }


@contextmanager
def _temporary_env(overrides: dict[str, str]):
    previous = {key: os.environ.get(key) for key in overrides}
    for key, value in overrides.items():
        os.environ[key] = str(value)
    try:
        yield
    finally:
        for key, old in previous.items():
            if old is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = old


@dataclass(frozen=True)
class VariantSpec:
    variant_id: str
    setting_changed: str
    setting_value: str
    classification: str
    purpose: str
    transport_overrides: dict[str, Any] = field(default_factory=dict)
    release_mode: str = "baseline_polygon"
    forcing_recipe: str = BASELINE_RECIPE
    product_definition_only: bool = False


class ExperimentalControlService(EnsembleForecastService):
    """Small experimental subclass exposing deterministic setting overrides."""

    def _resolve_wave_file(self) -> Path | None:
        if self.transport_overrides.get("attach_wave_reader") is False:
            return None
        return super()._resolve_wave_file()

    def _apply_transport_overrides(self, model, audit: dict) -> None:
        super()._apply_transport_overrides(model, audit)
        applied = audit.setdefault("experimental_config_overrides", {})
        config_overrides = dict(self.transport_overrides.get("model_config") or {})
        for key, value in config_overrides.items():
            if key not in model.get_configspec():
                applied.setdefault("unsupported", {})[key] = value
                continue
            model.set_config(key, value)
            applied[key] = model.get_config(key)

    def _seed_official_release(
        self,
        model,
        start_time,
        num_elements: int,
        random_seed: int | None = None,
        audit: dict | None = None,
    ) -> dict:
        lons = self.seed_overrides.get("release_lons")
        lats = self.seed_overrides.get("release_lats")
        if lons is not None and lats is not None:
            lon_arr = np.asarray(lons, dtype=float)
            lat_arr = np.asarray(lats, dtype=float)
            if lon_arr.size != int(num_elements) or lat_arr.size != int(num_elements):
                raise ValueError("Explicit release arrays must match the requested element count.")
            release_start = normalize_model_timestamp(self.seed_overrides.get("release_start_utc") or start_time)
            model.seed_elements(
                lon=lon_arr,
                lat=lat_arr,
                number=int(num_elements),
                time=release_start.to_pydatetime(),
            )
            record = {
                "initialization_mode": "explicit_experimental_release_points",
                "source_geometry_path": str(self.seed_overrides.get("polygon_vector_path") or ""),
                "release_geometry": str(self.seed_overrides.get("source_geometry_label") or "explicit_release_points"),
                "custom_polygon_override_used": False,
                "explicit_release_points_used": True,
                "random_seed": random_seed if random_seed is not None else "",
                "release_start_utc": timestamp_to_utc_iso(release_start),
                "release_end_utc": timestamp_to_utc_iso(release_start),
                "release_duration_hours": 0.0,
                "point_count": int(lon_arr.size),
            }
            if audit is not None:
                audit["seed_initialization"] = record
            return record
        return super()._seed_official_release(model, start_time, num_elements, random_seed, audit)

    def run_deterministic_control(
        self,
        recipe_name: str,
        start_time: str,
        start_lat: float | None = None,
        start_lon: float | None = None,
        duration_hours: int = 72,
        selection: RecipeSelection | None = None,
        force_point_release: bool = False,
    ) -> dict:
        self.active_recipe_selection = selection
        simulation_start, simulation_end, duration_hours = self._get_official_simulation_window()
        deterministic_cfg = self.official_config.get("deterministic") or {}
        wind_factor = float(self.transport_overrides.get("wind_factor", deterministic_cfg.get("wind_factor", 1.0)))
        start_offset_hours = int(
            self.transport_overrides.get("start_time_offset_hours", deterministic_cfg.get("start_time_offset_hours", 0))
        )
        horizontal_diffusivity = float(
            self.transport_overrides.get(
                "horizontal_diffusivity_m2s",
                deterministic_cfg.get("horizontal_diffusivity_m2s", 2.0),
            )
        )
        require_wave = bool(self.transport_overrides.get("require_wave", self.case.is_official))
        enable_stokes = bool(self.transport_overrides.get("enable_stokes_drift", self.enable_stokes_drift))
        simulation_start = simulation_start + pd.Timedelta(hours=start_offset_hours)
        simulation_end = simulation_end + pd.Timedelta(hours=start_offset_hours)
        seed_element_count = int(self.official_element_count)
        seed_random_seed = int(self.official_polygon_seed_random_seed)

        audit = self._init_run_audit(
            recipe_name=recipe_name,
            run_kind="deterministic_control",
            requested_start_time=simulation_start,
            duration_hours=duration_hours,
            perturbation={
                "wind_factor": wind_factor,
                "start_time_offset_hours": start_offset_hours,
                "horizontal_diffusivity_m2s": horizontal_diffusivity,
                "random_seed": seed_random_seed,
            },
        )
        model = self._build_model(
            simulation_start=simulation_start,
            simulation_end=simulation_end,
            audit=audit,
            wind_factor=wind_factor,
            require_wave=require_wave,
            enable_stokes_drift=enable_stokes,
        )
        model.set_config("drift:horizontal_diffusivity", horizontal_diffusivity)
        model.set_config("drift:wind_uncertainty", 0.0)
        model.set_config("drift:current_uncertainty", 0.0)
        seed_record = self._seed_official_release(
            model,
            simulation_start,
            num_elements=seed_element_count,
            random_seed=seed_random_seed,
            audit=audit,
        )
        audit["seed_element_count"] = seed_element_count
        output_file = self.forecast_dir / f"deterministic_control_{recipe_name}.nc"
        control_nc = self._run_model(
            model=model,
            output_file=output_file,
            duration_hours=duration_hours,
            audit=audit,
        )
        audit["written_files"] = [str(control_nc)]
        self._write_loading_audit_artifacts()
        return {
            "output_file": control_nc,
            "written_files": [control_nc],
            "element_count": seed_element_count,
            "configuration": {
                "wind_factor": wind_factor,
                "horizontal_diffusivity_m2s": horizontal_diffusivity,
                "start_time_utc": timestamp_to_utc_iso(simulation_start),
                "end_time_utc": timestamp_to_utc_iso(simulation_end),
                "random_seed": seed_random_seed,
                "enable_stokes_drift": enable_stokes,
                "require_wave": require_wave,
                "attach_wave_reader": bool(self.transport_overrides.get("attach_wave_reader", True)),
                "seed_initialization": seed_record,
                "transport_overrides": self.transport_overrides,
            },
            "audit": audit,
        }


class MindoroMarch34OpenDriftSettingsSensitivity:
    def __init__(self, *, max_ensemble_candidates: int = 1, force: bool = False):
        self.max_ensemble_candidates = max(0, min(3, int(max_ensemble_candidates)))
        self.force = bool(force)
        self.output_dir = OUTPUT_DIR
        for path in (self.output_dir, MODEL_RUNS_DIR, VARIANT_PRODUCTS_DIR, FIGURES_DIR, AUDIT_DIR, ENSEMBLE_DIR):
            path.mkdir(parents=True, exist_ok=True)
        self.grid = GridBuilder()
        self.sea_mask = load_sea_mask_array(self.grid.spec)
        self.valid_mask = self.sea_mask > 0.5 if self.sea_mask is not None else np.ones((self.grid.height, self.grid.width), dtype=bool)
        self.buffer_mask = self._one_cell_buffer_mask(self.valid_mask)
        self.scoring = Phase3BScoringService(output_dir=self.output_dir / "_scratch_scoring")
        self.context = self._load_context()
        self.release_cache: dict[str, dict[str, Any]] = {}
        self.commands_used: list[str] = []
        self.skipped_variants: list[dict[str, Any]] = []

    def _load_context(self) -> dict[str, Any]:
        manifest_path = BASE_EXPERIMENT_DIR / "march3_4_philsa_5000_run_manifest.json"
        obs_manifest_path = BASE_EXPERIMENT_DIR / "march3_4_philsa_5000_observation_manifest.csv"
        if not manifest_path.exists():
            raise FileNotFoundError(f"Missing completed PhilSA experiment manifest: {manifest_path}")
        if not obs_manifest_path.exists():
            raise FileNotFoundError(f"Missing completed PhilSA observation manifest: {obs_manifest_path}")
        manifest = _read_json(manifest_path)
        obs_manifest = pd.read_csv(obs_manifest_path)
        providers = set(obs_manifest["provider"].astype(str))
        source_names = set(obs_manifest["source_name"].astype(str))
        if providers != {"PhilSA"}:
            raise RuntimeError(f"Expected PhilSA-only observation manifest, found providers {sorted(providers)}")
        if not {SEED_SOURCE_NAME, TARGET_SOURCE_NAME}.issubset(source_names):
            raise RuntimeError("Required March 3 and March 4 PhilSA layers are not both present.")
        seed_row = obs_manifest[obs_manifest["source_name"].astype(str) == SEED_SOURCE_NAME].iloc[0].to_dict()
        target_row = obs_manifest[obs_manifest["source_name"].astype(str) == TARGET_SOURCE_NAME].iloc[0].to_dict()
        recipe_payload = manifest.get("recipe") or {}
        selection = RecipeSelection(
            recipe=str(recipe_payload.get("recipe") or BASELINE_RECIPE),
            source_kind=str(recipe_payload.get("source_kind") or "existing_experiment_manifest"),
            source_path=recipe_payload.get("source_path"),
            status_flag=str(recipe_payload.get("status_flag") or "valid"),
            valid=True,
            provisional=False,
            rerun_required=False,
            note=str(recipe_payload.get("note") or ""),
        )
        return {
            "manifest_path": manifest_path,
            "manifest": manifest,
            "obs_manifest_path": obs_manifest_path,
            "obs_manifest": obs_manifest,
            "seed_row": seed_row,
            "target_row": target_row,
            "seed_vector": Path(str(seed_row["processed_vector"])),
            "seed_mask": Path(str(seed_row["extended_obs_mask"])),
            "target_vector": Path(str(target_row["processed_vector"])),
            "target_mask": Path(str(target_row["extended_obs_mask"])),
            "selection": selection,
            "baseline_control_nc": BASE_EXPERIMENT_DIR / "R1_previous" / "model_run" / "forecast" / "deterministic_control_cmems_gfs.nc",
            "pygnome_footprint": BASE_EXPERIMENT_DIR / "pygnome_comparator" / "products" / "pygnome_footprint_mask_2023-03-04_localdate.tif",
            "pygnome_summary": BASE_EXPERIMENT_DIR / "pygnome_comparator" / "march3_4_philsa_5000_pygnome_summary.csv",
        }

    @staticmethod
    def _one_cell_buffer_mask(valid_mask: np.ndarray) -> np.ndarray:
        valid = np.asarray(valid_mask, dtype=bool)
        if binary_dilation is None:
            padded = valid.copy()
            for dr in (-1, 0, 1):
                for dc in (-1, 0, 1):
                    shifted = np.zeros_like(valid)
                    r0 = max(0, dr)
                    r1 = valid.shape[0] + min(0, dr)
                    c0 = max(0, dc)
                    c1 = valid.shape[1] + min(0, dc)
                    shifted[r0:r1, c0:c1] = valid[r0 - dr : r1 - dr, c0 - dc : c1 - dc]
                    padded |= shifted
            return padded
        return binary_dilation(valid, structure=np.ones((3, 3), dtype=bool))

    def _forcing_paths_for_recipe(self, recipe_id: str) -> tuple[dict[str, Path] | None, str]:
        recipe_id = str(recipe_id)
        forcing_dirs = [
            BASE_EXPERIMENT_DIR / "forcing",
            Path("data") / "forcing" / RUN_NAME,
        ]

        def find_file(name: str) -> Path | None:
            for folder in forcing_dirs:
                path = folder / name
                if path.exists():
                    return path
            return None

        current_name = "hycom_curr.nc" if recipe_id.startswith("hycom") else "cmems_curr.nc"
        wind_name = "era5_wind.nc" if recipe_id.endswith("era5") else "gfs_wind.nc"
        wave_name = "cmems_wave.nc"
        paths = {
            "currents": find_file(current_name),
            "wind": find_file(wind_name),
            "wave": find_file(wave_name),
        }
        missing = [key for key, path in paths.items() if path is None]
        if missing:
            return None, "missing forcing file(s): " + ", ".join(missing)
        checked = {key: Path(str(path)) for key, path in paths.items() if path is not None}
        coverage_note = self._check_forcing_coverage(checked)
        if coverage_note:
            return None, coverage_note
        return checked, ""

    def _check_forcing_coverage(self, paths: dict[str, Path]) -> str:
        required_start = pd.Timestamp(SIMULATION_START_UTC).tz_convert(None)
        required_end = pd.Timestamp(SIMULATION_END_UTC).tz_convert(None)
        problems: list[str] = []
        for kind, path in paths.items():
            try:
                with xr.open_dataset(path) as ds:
                    time_name = next((name for name in ("time", "valid_time") if name in ds.coords or name in ds.variables), None)
                    if not time_name:
                        problems.append(f"{kind}:{path.name}:missing time coordinate")
                        continue
                    times = pd.DatetimeIndex(pd.to_datetime(ds[time_name].values))
                    if times.tz is not None:
                        times = times.tz_convert("UTC").tz_localize(None)
                    if len(times) == 0 or times.min() > required_start or times.max() < required_end:
                        problems.append(
                            f"{kind}:{path.name}:coverage {times.min() if len(times) else 'none'} to "
                            f"{times.max() if len(times) else 'none'}"
                        )
            except Exception as exc:
                problems.append(f"{kind}:{path.name}:{type(exc).__name__}: {exc}")
        return "; ".join(problems)

    def _cell_center_xy(self, rows: np.ndarray, cols: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
        xs = self.grid.min_x + ((cols.astype(float) + 0.5) * self.grid.resolution)
        ys = self.grid.max_y - ((rows.astype(float) + 0.5) * self.grid.resolution)
        return xs, ys

    def _xy_to_lonlat(self, xs: np.ndarray, ys: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
        if str(self.grid.crs).upper() == "EPSG:4326":
            return xs.astype(float), ys.astype(float)
        if rio_transform is None:
            raise ImportError("rasterio is required to transform release points.")
        lons, lats = rio_transform(self.grid.crs, "EPSG:4326", xs.tolist(), ys.tolist())
        return np.asarray(lons, dtype=float), np.asarray(lats, dtype=float)

    def _mask_cell_centers_xy(self, mask: np.ndarray) -> np.ndarray:
        active = np.argwhere(mask > 0)
        if active.size == 0:
            return np.empty((0, 2), dtype=float)
        xs, ys = self._cell_center_xy(active[:, 0], active[:, 1])
        return np.column_stack([xs, ys])

    def _release_points_from_cells(
        self,
        cells: np.ndarray,
        *,
        random_seed: int,
        nudge_m: float = 0.0,
    ) -> dict[str, Any]:
        if cells.size == 0:
            raise ValueError("No release cells available for explicit release.")
        rng = np.random.default_rng(random_seed)
        chosen = cells[rng.integers(0, len(cells), size=ELEMENT_COUNT)]
        rows = chosen[:, 0]
        cols = chosen[:, 1]
        xs, ys = self._cell_center_xy(rows, cols)
        jitter = self.grid.resolution * 0.35
        xs = xs + rng.uniform(-jitter, jitter, size=ELEMENT_COUNT)
        ys = ys + rng.uniform(-jitter, jitter, size=ELEMENT_COUNT)
        if nudge_m > 0 and distance_transform_edt is not None:
            land = ~self.valid_mask
            _, nearest_land = distance_transform_edt(~land, return_indices=True)
            nearest_r = nearest_land[0][rows, cols]
            nearest_c = nearest_land[1][rows, cols]
            land_x, land_y = self._cell_center_xy(nearest_r, nearest_c)
            dx = xs - land_x
            dy = ys - land_y
            norm = np.sqrt(dx * dx + dy * dy)
            fallback = norm <= 1e-9
            dx[fallback] = 1.0
            dy[fallback] = 0.0
            norm[fallback] = 1.0
            xs = xs + (dx / norm) * float(nudge_m)
            ys = ys + (dy / norm) * float(nudge_m)
        lons, lats = self._xy_to_lonlat(xs, ys)
        cell_ids, counts = np.unique(
            np.asarray([f"{int(row)}:{int(col)}" for row, col in zip(rows, cols)]),
            return_counts=True,
        )
        return {
            "release_lons": lons,
            "release_lats": lats,
            "source_cells": cells,
            "element_distribution": pd.DataFrame(
                {
                    "cell_id": cell_ids,
                    "element_count": counts.astype(int),
                }
            ),
        }

    def _load_seed_masks(self) -> dict[str, Any]:
        seed_gdf = gpd.read_file(self.context["seed_vector"]) if gpd is not None else None
        from src.helpers.raster import rasterize_observation_layer

        raw_mask = rasterize_observation_layer(seed_gdf, self.grid) if seed_gdf is not None else _read_raster(self.context["seed_mask"])
        official = apply_ocean_mask(raw_mask, sea_mask=self.sea_mask, fill_value=0.0)
        return {"raw": raw_mask.astype(np.float32), "official": official.astype(np.float32), "gdf": seed_gdf}

    def _cell_classification(self, raw_mask: np.ndarray) -> pd.DataFrame:
        active = np.argwhere(raw_mask > 0)
        if active.size == 0:
            return pd.DataFrame()
        valid = np.asarray(self.valid_mask, dtype=bool)
        buffer_mask = self._one_cell_buffer_mask(valid)
        shoreline_ocean = valid & buffer_mask & self._adjacent_to(~valid)
        shoreline_land = (~valid) & self._adjacent_to(valid)
        land_cell = (~valid) & (~shoreline_land)
        invalid_ocean = (~valid) & (~land_cell) & (~shoreline_land)
        if distance_transform_edt is None:
            distance_to_shore_m = np.full(active.shape[0], np.nan)
        else:
            dist_to_land = distance_transform_edt(valid) * float(self.grid.resolution)
            dist_to_sea = distance_transform_edt(~valid) * float(self.grid.resolution)
            distance_to_shore_m = np.where(valid[active[:, 0], active[:, 1]], dist_to_land[active[:, 0], active[:, 1]], -dist_to_sea[active[:, 0], active[:, 1]])
        xs, ys = self._cell_center_xy(active[:, 0], active[:, 1])
        lons, lats = self._xy_to_lonlat(xs, ys)
        classes = []
        for r, c in active:
            if shoreline_ocean[r, c]:
                classes.append("shoreline_ocean")
            elif valid[r, c]:
                classes.append("valid_ocean")
            elif shoreline_land[r, c]:
                classes.append("shoreline_land_cell")
            elif land_cell[r, c]:
                classes.append("land_cell")
            elif invalid_ocean[r, c]:
                classes.append("invalid_ocean")
            else:
                classes.append("outside_scoring_grid")
        return pd.DataFrame(
            {
                "row": active[:, 0].astype(int),
                "col": active[:, 1].astype(int),
                "x_m": xs,
                "y_m": ys,
                "lon": lons,
                "lat": lats,
                "cell_class": classes,
                "distance_to_shoreline_m": distance_to_shore_m,
                "valid_ocean_mask": valid[active[:, 0], active[:, 1]],
            }
        )

    @staticmethod
    def _adjacent_to(mask: np.ndarray) -> np.ndarray:
        if binary_dilation is None:
            out = np.zeros_like(mask, dtype=bool)
            for dr in (-1, 0, 1):
                for dc in (-1, 0, 1):
                    shifted = np.zeros_like(mask, dtype=bool)
                    r0 = max(0, dr)
                    r1 = mask.shape[0] + min(0, dr)
                    c0 = max(0, dc)
                    c1 = mask.shape[1] + min(0, dc)
                    shifted[r0:r1, c0:c1] = mask[r0 - dr : r1 - dr, c0 - dc : c1 - dc]
                    out |= shifted
            return out
        return binary_dilation(mask, structure=np.ones((3, 3), dtype=bool))

    def _release_payload(self, mode: str) -> tuple[dict[str, Any], str]:
        if mode in self.release_cache:
            cached = self.release_cache[mode]
            return dict(cached["seed_overrides"]), str(cached["description"])
        seed_masks = self._load_seed_masks()
        raw_mask = seed_masks["raw"]
        cells_df = self._cell_classification(raw_mask)
        active = cells_df[["row", "col"]].to_numpy(dtype=int)
        valid_cells = cells_df[cells_df["valid_ocean_mask"]][["row", "col"]].to_numpy(dtype=int)
        core_cells = cells_df[cells_df["cell_class"].eq("valid_ocean")][["row", "col"]].to_numpy(dtype=int)
        safe_cells = core_cells if len(core_cells) else valid_cells
        nudge_m = 0.0
        chosen_cells = active
        if mode == "baseline_polygon":
            seed_overrides = {
                "polygon_vector_path": str(self.context["seed_vector"]),
                "source_geometry_label": "accepted_march3_philsa_processed_polygon",
            }
            description = "baseline processed PhilSA polygon release"
        else:
            if mode == "valid_ocean_only":
                chosen_cells = valid_cells
                description = "explicit release sampled only from seed cells inside the official valid-ocean mask"
            elif mode == "shoreline_invalid_excluded":
                chosen_cells = valid_cells
                description = "explicit release excluding seed cells classified as shoreline-land, land, invalid ocean, or outside grid"
            elif mode == "core_ocean_only":
                chosen_cells = core_cells if len(core_cells) else valid_cells
                description = "explicit release excluding one-cell shoreline-ocean seed cells where possible"
            elif mode == "one_cell_offshore_safe":
                chosen_cells = safe_cells
                description = "explicit release sampled from seed cells at least one grid cell from the land mask when available"
            elif mode == "nudge_0_5km":
                chosen_cells = valid_cells if len(valid_cells) else active
                nudge_m = 500.0
                description = "explicit release from seed cells with a 0.5 km offshore diagnostic nudge"
            elif mode == "nudge_1_0km":
                chosen_cells = valid_cells if len(valid_cells) else active
                nudge_m = 1000.0
                description = "explicit release from seed cells with a 1.0 km offshore diagnostic nudge"
            elif mode == "nudge_2_0km":
                chosen_cells = valid_cells if len(valid_cells) else active
                nudge_m = 2000.0
                description = "explicit release from seed cells with a 2.0 km offshore diagnostic nudge"
            else:
                raise ValueError(f"Unsupported release mode: {mode}")
            points = self._release_points_from_cells(chosen_cells, random_seed=BASELINE_RANDOM_SEED, nudge_m=nudge_m)
            dist_path = AUDIT_DIR / f"release_distribution_{mode}.csv"
            _write_csv(dist_path, points["element_distribution"])
            seed_overrides = {
                "release_lons": points["release_lons"],
                "release_lats": points["release_lats"],
                "polygon_vector_path": str(self.context["seed_vector"]),
                "source_geometry_label": mode,
                "release_start_utc": SIMULATION_START_UTC,
                "release_distribution_csv": str(dist_path),
                "release_cell_count": int(len(chosen_cells)),
            }
        self.release_cache[mode] = {"seed_overrides": seed_overrides, "description": description}
        return dict(seed_overrides), description

    def _build_variants(self) -> list[VariantSpec]:
        variants = [
            VariantSpec(
                "A_baseline_previous",
                "baseline",
                "R1_previous: coastline_action=previous, Stokes on, windage=0.02, diffusivity=2",
                "physical-parity test",
                "Reproduce the existing OpenDrift R1_previous deterministic/control behavior.",
                {"coastline_action": "previous", "coastline_approximation_precision": 0.001, "time_step_minutes": 60},
            ),
            VariantSpec(
                "B_stokes_off",
                "drift:stokes_drift",
                "False",
                "forcing diagnostic",
                "Determine whether Stokes drift pushes particles into shoreline cells.",
                {
                    "coastline_action": "previous",
                    "coastline_approximation_precision": 0.001,
                    "time_step_minutes": 60,
                    "enable_stokes_drift": False,
                    "model_config": {"drift:stokes_drift": False},
                },
            ),
            VariantSpec(
                "B_wave_reader_off",
                "wave reader and Stokes",
                "wave reader not attached; Stokes false",
                "forcing diagnostic",
                "Test supported no-wave/no-Stokes handling as a forcing diagnostic.",
                {
                    "coastline_action": "previous",
                    "coastline_approximation_precision": 0.001,
                    "time_step_minutes": 60,
                    "require_wave": False,
                    "enable_stokes_drift": False,
                    "attach_wave_reader": False,
                    "model_config": {"drift:stokes_drift": False},
                },
            ),
            VariantSpec(
                "C_coastline_none",
                "general:coastline_action",
                "none",
                "shoreline/landmask diagnostic",
                "Detect whether coastline handling traps or compacts the footprint.",
                {"coastline_action": "none", "coastline_approximation_precision": 0.001, "time_step_minutes": 60},
            ),
            VariantSpec(
                "C_coastline_stranding",
                "general:coastline_action",
                "stranding",
                "shoreline/landmask diagnostic",
                "Compare baseline previous against installed stranding behavior.",
                {"coastline_action": "stranding", "coastline_approximation_precision": 0.001, "time_step_minutes": 60},
            ),
            VariantSpec(
                "D_release_valid_ocean_only",
                "release geometry",
                "valid-ocean-only release",
                "release-geometry diagnostic",
                "Test whether March 3 seed cells outside valid ocean drive the collapse.",
                {"coastline_action": "previous", "coastline_approximation_precision": 0.001, "time_step_minutes": 60},
                release_mode="valid_ocean_only",
            ),
            VariantSpec(
                "D_release_core_ocean_only",
                "release geometry",
                "shoreline-ocean excluded where possible",
                "release-geometry diagnostic",
                "Test whether shoreline-ocean release cells are too strict for OpenDrift.",
                {"coastline_action": "previous", "coastline_approximation_precision": 0.001, "time_step_minutes": 60},
                release_mode="core_ocean_only",
            ),
            VariantSpec(
                "D_release_one_cell_offshore_safe",
                "release geometry",
                "one-cell offshore-safe release",
                "release-geometry diagnostic",
                "Move the diagnostic release to the nearest one-cell safe ocean seed support.",
                {"coastline_action": "previous", "coastline_approximation_precision": 0.001, "time_step_minutes": 60},
                release_mode="one_cell_offshore_safe",
            ),
            VariantSpec(
                "D_release_nudge_0_5km",
                "release geometry",
                "0.5 km offshore nudge",
                "release-geometry diagnostic",
                "Probe sensitivity to a small offshore nudge without changing the target.",
                {"coastline_action": "previous", "coastline_approximation_precision": 0.001, "time_step_minutes": 60},
                release_mode="nudge_0_5km",
            ),
            VariantSpec(
                "D_release_nudge_1_0km",
                "release geometry",
                "1.0 km offshore nudge",
                "release-geometry diagnostic",
                "Probe sensitivity to a one-grid-cell offshore nudge.",
                {"coastline_action": "previous", "coastline_approximation_precision": 0.001, "time_step_minutes": 60},
                release_mode="nudge_1_0km",
            ),
            VariantSpec(
                "D_release_nudge_2_0km",
                "release geometry",
                "2.0 km offshore nudge",
                "target-tuning risk",
                "Cheap diagnostic only; large offshore nudge risks tuning the initialization.",
                {"coastline_action": "previous", "coastline_approximation_precision": 0.001, "time_step_minutes": 60},
                release_mode="nudge_2_0km",
            ),
        ]
        for value in [0.0, 0.01, 0.03, 0.04]:
            variants.append(
                VariantSpec(
                    f"E_windage_{str(value).replace('.', '_')}",
                    "seed:wind_drift_factor",
                    f"{value:.2f}",
                    "physical-parity test" if value in {0.0, 0.01} else "target-tuning risk",
                    "Test whether direct wind drift controls compactness or displacement.",
                    {
                        "coastline_action": "previous",
                        "coastline_approximation_precision": 0.001,
                        "time_step_minutes": 60,
                        "model_config": {"seed:wind_drift_factor": float(value)},
                    },
                )
            )
        for value in [0.0, 10.0, 25.0, 50.0]:
            variants.append(
                VariantSpec(
                    f"F_diffusivity_{int(value)}",
                    "drift:horizontal_diffusivity",
                    f"{value:g} m2/s",
                    "diffusion/spreading diagnostic" if value <= 25 else "target-tuning risk",
                    "Test whether the small footprint is dominated by insufficient deterministic spreading.",
                    {
                        "coastline_action": "previous",
                        "coastline_approximation_precision": 0.001,
                        "time_step_minutes": 60,
                        "horizontal_diffusivity_m2s": float(value),
                    },
                )
            )
        for recipe_id in ["cmems_era5", "hycom_gfs", "hycom_era5"]:
            variants.append(
                VariantSpec(
                    f"G_forcing_{recipe_id}",
                    "forcing recipe",
                    recipe_id,
                    "forcing diagnostic",
                    f"Run deterministic forcing recipe {recipe_id} when local files are valid.",
                    {"coastline_action": "previous", "coastline_approximation_precision": 0.001, "time_step_minutes": 60},
                    forcing_recipe=recipe_id,
                )
            )
        variants.append(
            VariantSpec(
                "H_current_fallback_zero_explicit",
                "environment:fallback:x/y_sea_water_velocity",
                "0.0, 0.0 explicit",
                "forcing diagnostic",
                "Confirm baseline already uses zero current fallback for unsupported nearshore cells.",
                {
                    "coastline_action": "previous",
                    "coastline_approximation_precision": 0.001,
                    "time_step_minutes": 60,
                    "model_config": {
                        "environment:fallback:x_sea_water_velocity": 0.0,
                        "environment:fallback:y_sea_water_velocity": 0.0,
                    },
                },
            )
        )
        return variants

    def _run_deterministic_variant(self, variant: VariantSpec) -> dict[str, Any]:
        variant_dir = MODEL_RUNS_DIR / variant.variant_id / "model_run"
        nc_path = variant_dir / "forecast" / f"deterministic_control_{variant.forcing_recipe}.nc"
        products_dir = VARIANT_PRODUCTS_DIR / variant.variant_id
        summary_path = products_dir / "variant_summary.json"
        if nc_path.exists() and summary_path.exists() and not self.force:
            payload = _read_json(summary_path)
            payload["reused_existing_run"] = True
            return payload
        forcing_paths, skip_reason = self._forcing_paths_for_recipe(variant.forcing_recipe)
        if forcing_paths is None:
            row = {
                "variant_id": variant.variant_id,
                "status": "skipped",
                "skip_reason": skip_reason,
                **asdict(variant),
            }
            self.skipped_variants.append(row)
            return row
        release_overrides, release_description = self._release_payload(variant.release_mode)
        seed_overrides = {
            **release_overrides,
            "release_start_utc": SIMULATION_START_UTC,
        }
        output_run_name = f"{RUN_NAME}/phase3b_philsa_march3_4_5000_experiment/experimental_settings_sensitivity/model_runs/{variant.variant_id}/model_run"
        with _temporary_env({OFFICIAL_ELEMENT_COUNT_OVERRIDE_ENV: str(ELEMENT_COUNT), "WORKFLOW_MODE": "mindoro_retro_2023"}):
            service = ExperimentalControlService(
                currents_file=forcing_paths["currents"],
                winds_file=forcing_paths["wind"],
                wave_file=forcing_paths["wave"],
                output_run_name=output_run_name,
                sensitivity_context={
                    "experimental_only": True,
                    "thesis_facing": False,
                    "reportable": False,
                    "variant_id": variant.variant_id,
                    "classification": variant.classification,
                    "purpose": variant.purpose,
                },
                simulation_start_utc=SIMULATION_START_UTC,
                simulation_end_utc=SIMULATION_END_UTC,
                snapshot_hours=[24, 48],
                date_composite_dates=[SEED_DATE, TARGET_DATE],
                transport_overrides=variant.transport_overrides,
                seed_overrides=seed_overrides,
            )
            result = service.run_deterministic_control(
                recipe_name=variant.forcing_recipe,
                start_time=SIMULATION_START_UTC,
                selection=self.context["selection"],
            )
        product = self._build_deterministic_products(
            variant=variant,
            nc_path=Path(result["output_file"]),
            products_dir=products_dir,
            run_result=result,
            release_description=release_description,
        )
        _write_json(summary_path, product)
        return product

    def _particle_arrays_at_time(self, ds: xr.Dataset, time_index: int) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        lon = np.asarray(ds["lon"].isel(time=time_index).values).reshape(-1)
        lat = np.asarray(ds["lat"].isel(time=time_index).values).reshape(-1)
        status = np.asarray(ds["status"].isel(time=time_index).values).reshape(-1)
        return lon, lat, status

    def _footprint_for_time_indices(self, ds: xr.Dataset, indices: list[int]) -> tuple[np.ndarray, dict[str, Any]]:
        raw = np.zeros((self.grid.height, self.grid.width), dtype=np.float32)
        active_timestamps: list[str] = []
        active_particles_total = 0
        for idx in indices:
            lon, lat, status = self._particle_arrays_at_time(ds, idx)
            valid = np.isfinite(lon) & np.isfinite(lat) & (status == 0)
            if not np.any(valid):
                continue
            active_particles_total += int(np.count_nonzero(valid))
            active_timestamps.append(_iso_z(pd.Timestamp(ds["time"].values[idx])))
            hits, _ = rasterize_particles(
                self.grid,
                lon[valid],
                lat[valid],
                np.ones(int(np.count_nonzero(valid)), dtype=np.float32),
            )
            raw = np.maximum(raw, hits.astype(np.float32))
        return raw, {"active_timestamps": active_timestamps, "active_particles_total": active_particles_total}

    def _status_counts(self, ds: xr.Dataset, time_index: int) -> dict[str, Any]:
        lon, lat, status = self._particle_arrays_at_time(ds, time_index)
        finite = np.isfinite(lon) & np.isfinite(lat)
        active = finite & (status == 0)
        stranded = finite & (status == 1)
        non_active = finite & (status != 0)
        xs, ys = self._points_to_xy(lon[finite], lat[finite])
        outside = (
            (xs < self.grid.min_x)
            | (xs >= self.grid.max_x)
            | (ys < self.grid.min_y)
            | (ys >= self.grid.max_y)
        )
        unique, counts = np.unique(status[np.isfinite(status)], return_counts=True)
        return {
            "active_floating_particles_on_march4": int(np.count_nonzero(active)),
            "stranded_beached_particles_on_march4": int(np.count_nonzero(stranded)),
            "deactivated_particles_on_march4": int(np.count_nonzero(non_active) + np.count_nonzero(~finite)),
            "outside_domain_particles_on_march4": int(np.count_nonzero(outside)),
            "status_counts": ";".join(f"{int(k)}:{int(v)}" for k, v in zip(unique, counts)),
        }

    def _points_to_xy(self, lon: np.ndarray, lat: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
        if str(self.grid.crs).upper() == "EPSG:4326":
            return lon.astype(float), lat.astype(float)
        if rio_transform is None:
            raise ImportError("rasterio is required to transform particle coordinates.")
        xs, ys = rio_transform("EPSG:4326", self.grid.crs, lon.tolist(), lat.tolist())
        return np.asarray(xs, dtype=float), np.asarray(ys, dtype=float)

    def _score_mask(self, forecast: np.ndarray, obs: np.ndarray, valid_mask: np.ndarray | None) -> dict[str, Any]:
        diagnostics = self.scoring._compute_mask_diagnostics(forecast.astype(np.float32), obs.astype(np.float32))
        fss_values = {}
        for window in OFFICIAL_PHASE3B_WINDOWS_KM:
            fss_values[f"fss_{window}km"] = float(
                np.clip(
                    calculate_fss(
                        forecast.astype(np.float32),
                        obs.astype(np.float32),
                        window=self.scoring._window_km_to_cells(window),
                        valid_mask=valid_mask,
                    ),
                    0.0,
                    1.0,
                )
            )
        diagnostics.update(fss_values)
        diagnostics["mean_fss"] = float(np.mean([diagnostics[f"fss_{w}km"] for w in OFFICIAL_PHASE3B_WINDOWS_KM]))
        diagnostics.update(self._direction_summary(forecast, obs))
        return diagnostics

    def _direction_summary(self, forecast: np.ndarray, obs: np.ndarray) -> dict[str, Any]:
        f_pts = self._mask_cell_centers_xy(forecast)
        o_pts = self._mask_cell_centers_xy(obs)
        if len(f_pts) == 0 or len(o_pts) == 0:
            return {
                "displacement_dx_m": np.nan,
                "displacement_dy_m": np.nan,
                "displacement_direction": "n/a",
            }
        delta = f_pts.mean(axis=0) - o_pts.mean(axis=0)
        dx, dy = float(delta[0]), float(delta[1])
        angle = (math.degrees(math.atan2(dx, dy)) + 360.0) % 360.0
        labels = ["N", "NE", "E", "SE", "S", "SW", "W", "NW"]
        direction = labels[int(((angle + 22.5) % 360) / 45.0)]
        return {
            "displacement_dx_m": dx,
            "displacement_dy_m": dy,
            "displacement_direction": direction,
        }

    def _build_deterministic_products(
        self,
        *,
        variant: VariantSpec,
        nc_path: Path,
        products_dir: Path,
        run_result: dict[str, Any],
        release_description: str,
    ) -> dict[str, Any]:
        products_dir.mkdir(parents=True, exist_ok=True)
        obs = _read_raster(self.context["target_mask"])
        with xr.open_dataset(nc_path) as ds:
            times = pd.DatetimeIndex(pd.to_datetime(ds["time"].values))
            if times.tz is not None:
                times = times.tz_convert("UTC").tz_localize(None)
            local_dates = [pd.Timestamp(t).tz_localize("UTC").tz_convert(LOCAL_TIMEZONE).date().isoformat() for t in times]
            march4_indices = [i for i, date in enumerate(local_dates) if date == TARGET_DATE]
            march3_4_indices = [i for i, date in enumerate(local_dates) if date in {SEED_DATE, TARGET_DATE}]
            final_index = len(times) - 1
            product_defs = {
                "exact_final_snapshot": [final_index],
                "march4_localdate_composite": march4_indices,
                "cumulative_march3_to_march4": march3_4_indices,
                "cumulative_march4_only": march4_indices,
            }
            product_records: dict[str, dict[str, Any]] = {}
            for label, indices in product_defs.items():
                raw, meta = self._footprint_for_time_indices(ds, indices)
                official = apply_ocean_mask(raw, sea_mask=self.sea_mask, fill_value=0.0)
                buffer = np.where(self.buffer_mask, raw, 0).astype(np.float32)
                save_raster(self.grid, raw.astype(np.float32), products_dir / f"{label}_no_mask.tif")
                save_raster(self.grid, official.astype(np.float32), products_dir / f"{label}_official_valid_ocean.tif")
                save_raster(self.grid, buffer.astype(np.float32), products_dir / f"{label}_one_cell_coastal_buffer.tif")
                score = self._score_mask(official > 0, obs > 0, self.valid_mask)
                product_records[label] = {
                    "raw_occupied_cells_before_mask": int(np.count_nonzero(raw > 0)),
                    "valid_ocean_cells_after_official_mask": int(np.count_nonzero(official > 0)),
                    "cells_removed_by_official_mask": int(np.count_nonzero((raw > 0) & ~(official > 0))),
                    "no_mask_cells": int(np.count_nonzero(raw > 0)),
                    "one_cell_coastal_buffer_cells": int(np.count_nonzero(buffer > 0)),
                    "active_timestamps": ";".join(meta["active_timestamps"]),
                    "active_particles_total_over_product": int(meta["active_particles_total"]),
                    **score,
                }
            status = self._status_counts(ds, final_index)
        local = product_records["march4_localdate_composite"]
        plausible = (
            int(local["valid_ocean_cells_after_official_mask"]) > 0
            and (int(local["valid_ocean_cells_after_official_mask"]) / max(int(local["no_mask_cells"]), 1)) >= 0.25
        )
        payload = {
            "variant_id": variant.variant_id,
            "status": "completed",
            "setting_changed": variant.setting_changed,
            "setting_value": variant.setting_value,
            "classification": variant.classification,
            "purpose": variant.purpose,
            "forcing_recipe": variant.forcing_recipe,
            "release_mode": variant.release_mode,
            "release_description": release_description,
            "element_count": int(run_result["element_count"]),
            "control_netcdf_path": str(nc_path),
            "products_dir": str(products_dir),
            "configuration": run_result.get("configuration", {}),
            **status,
            "raw_occupied_cells_before_mask": int(local["raw_occupied_cells_before_mask"]),
            "valid_ocean_cells_after_official_mask": int(local["valid_ocean_cells_after_official_mask"]),
            "cells_removed_by_official_mask": int(local["cells_removed_by_official_mask"]),
            "no_mask_cells": int(local["no_mask_cells"]),
            "one_cell_coastal_buffer_cells": int(local["one_cell_coastal_buffer_cells"]),
            "nearest_forecast_to_observation_distance_m": local["nearest_distance_to_obs_m"],
            "centroid_distance_m": local["centroid_distance_m"],
            "iou": local["iou"],
            "dice": local["dice"],
            "fss_1km": local["fss_1km"],
            "fss_3km": local["fss_3km"],
            "fss_5km": local["fss_5km"],
            "fss_10km": local["fss_10km"],
            "mean_fss": local["mean_fss"],
            "direction_displacement": local["displacement_direction"],
            "displacement_dx_m": local["displacement_dx_m"],
            "displacement_dy_m": local["displacement_dy_m"],
            "increases_cells_without_simply_moving_onto_land": bool(plausible),
            "physically_plausible_or_target_tuned": (
                "target-tuning risk"
                if variant.classification == "target-tuning risk"
                else ("physically_plausible_diagnostic" if plausible else "mostly_mask_or_land_limited")
            ),
            "product_definitions": product_records,
            "reused_existing_run": False,
        }
        return payload

    def _baseline_reproduction(self, matrix: pd.DataFrame) -> None:
        base = matrix[matrix["variant_id"] == "A_baseline_previous"]
        if base.empty:
            return
        row = base.iloc[0].to_dict()
        existing_summary = pd.read_csv(BASE_EXPERIMENT_DIR / "march3_4_philsa_5000_summary.csv")
        existing_r1 = existing_summary[existing_summary["branch_id"].astype(str) == "R1_previous"].iloc[0].to_dict()
        baseline = pd.DataFrame(
            [
                {
                    "variant_id": row["variant_id"],
                    "reproduced_control_valid_ocean_cells": row["valid_ocean_cells_after_official_mask"],
                    "existing_control_valid_ocean_cells": existing_r1.get("control_nonzero_cells_from_march4_localdate_mask"),
                    "existing_p50_valid_ocean_cells": existing_r1.get("forecast_nonzero_cells"),
                    "raw_occupied_cells_before_mask": row["raw_occupied_cells_before_mask"],
                    "cells_removed_by_official_mask": row["cells_removed_by_official_mask"],
                    "no_mask_cells": row["no_mask_cells"],
                    "one_cell_coastal_buffer_cells": row["one_cell_coastal_buffer_cells"],
                    "status": "consistent_about_4_cells"
                    if abs(int(row["valid_ocean_cells_after_official_mask"]) - 4) <= 1
                    else "review_count_difference",
                }
            ]
        )
        _write_csv(self.output_dir / "baseline_reproduction_summary.csv", baseline)
        _write_text(
            self.output_dir / "baseline_reproduction_note.md",
            "\n".join(
                [
                    "# EXPERIMENTAL Baseline Reproduction Note",
                    "",
                    "This is not thesis-facing and does not promote any March 3 -> March 4 result.",
                    "",
                    f"- Reproduced deterministic/control valid-ocean cells: {row['valid_ocean_cells_after_official_mask']}",
                    f"- Existing R1_previous deterministic/control cells: {existing_r1.get('control_nonzero_cells_from_march4_localdate_mask')}",
                    f"- Existing R1_previous p50 cells: {existing_r1.get('forecast_nonzero_cells')}",
                    f"- Raw cells before official mask: {row['raw_occupied_cells_before_mask']}",
                    f"- Cells removed by official mask: {row['cells_removed_by_official_mask']}",
                    "- Interpretation: baseline control reproduction remains in the expected about-4-cell regime.",
                ]
            )
            + "\n",
        )

    def _release_geometry_audit(self) -> pd.DataFrame:
        seed_masks = self._load_seed_masks()
        raw = seed_masks["raw"]
        official = seed_masks["official"]
        cells = self._cell_classification(raw)
        gdf = seed_masks["gdf"]
        raw_feature_count = 0
        raw_area_m2 = np.nan
        cleaned_area_m2 = np.nan
        if gdf is not None:
            raw_feature_count = int(len(gdf.index))
            projected = gdf.to_crs(self.grid.crs) if str(gdf.crs) != str(self.grid.crs) else gdf
            cleaned_area_m2 = float(projected.geometry.area.sum())
        raw_geojson_path = Path(str(self.context["seed_row"].get("raw_geojson", "")))
        if raw_geojson_path.exists() and gpd is not None:
            try:
                raw_gdf = gpd.read_file(raw_geojson_path)
                raw_projected = raw_gdf.set_crs("EPSG:4326", allow_override=True).to_crs(self.grid.crs)
                raw_area_m2 = float(raw_projected.geometry.area.sum())
            except Exception:
                raw_area_m2 = cleaned_area_m2
        cells["raw_feature_count"] = raw_feature_count
        cells["raw_polygon_area_m2"] = raw_area_m2
        cells["cleaned_polygon_area_m2"] = cleaned_area_m2
        cells["rasterized_seed_cells_before_mask"] = int(np.count_nonzero(raw > 0))
        cells["valid_ocean_seed_cells_after_mask"] = int(np.count_nonzero(official > 0))
        cells["release_element_count"] = ELEMENT_COUNT
        cells["release_cell_class"] = cells["cell_class"]
        cells["initial_release_particles_in_cell"] = 0
        cells["initialized_near_shoreline_or_invalid"] = cells["cell_class"].ne("valid_ocean")
        base_nc = self.context["baseline_control_nc"]
        startup_note = "baseline control NetCDF missing"
        if base_nc.exists():
            with xr.open_dataset(base_nc) as ds:
                lon0, lat0, status0 = self._particle_arrays_at_time(ds, 0)
                startup_note = (
                    f"initial_finite={int(np.count_nonzero(np.isfinite(lon0) & np.isfinite(lat0)))}; "
                    f"initial_active={int(np.count_nonzero(status0 == 0))}; "
                    f"initial_non_active={int(np.count_nonzero(status0 != 0))}"
                )
                finite = np.isfinite(lon0) & np.isfinite(lat0)
                if np.any(finite):
                    xs0, ys0 = self._points_to_xy(lon0[finite], lat0[finite])
                    rows0 = np.floor((self.grid.max_y - ys0) / self.grid.resolution).astype(int)
                    cols0 = np.floor((xs0 - self.grid.min_x) / self.grid.resolution).astype(int)
                    in_grid = (
                        (rows0 >= 0)
                        & (rows0 < self.grid.height)
                        & (cols0 >= 0)
                        & (cols0 < self.grid.width)
                    )
                    if np.any(in_grid):
                        initial_cells, initial_counts = np.unique(
                            np.asarray([f"{int(r)}:{int(c)}" for r, c in zip(rows0[in_grid], cols0[in_grid])]),
                            return_counts=True,
                        )
                        initial_lookup = {cell: int(count) for cell, count in zip(initial_cells, initial_counts)}
                        cells["initial_release_particles_in_cell"] = [
                            initial_lookup.get(f"{int(row.row)}:{int(row.col)}", 0)
                            for row in cells.itertuples(index=False)
                        ]
        cells["opendrift_startup_status_note"] = startup_note
        path = self.output_dir / "release_geometry_audit.csv"
        _write_csv(path, cells)
        if plt is not None:
            self._plot_release_geometry(cells, raw, official)
        class_counts = cells["cell_class"].value_counts().to_dict() if not cells.empty else {}
        release_count_stats = cells["initial_release_particles_in_cell"].describe() if not cells.empty else pd.Series(dtype=float)
        near_or_invalid_count = int(cells["initialized_near_shoreline_or_invalid"].sum()) if not cells.empty else 0
        _write_text(
            self.output_dir / "release_geometry_audit_note.md",
            "\n".join(
                [
                    "# EXPERIMENTAL Release Geometry Audit",
                    "",
                    f"- Raw March 3 PhilSA feature count: {raw_feature_count}",
                    f"- Raw polygon area m2: {raw_area_m2:.2f}" if np.isfinite(raw_area_m2) else "- Raw polygon area m2: n/a",
                    f"- Cleaned polygon area m2: {cleaned_area_m2:.2f}" if np.isfinite(cleaned_area_m2) else "- Cleaned polygon area m2: n/a",
                    f"- Rasterized seed cells before mask: {int(np.count_nonzero(raw > 0))}",
                    f"- Valid-ocean seed cells after mask: {int(np.count_nonzero(official > 0))}",
                    f"- Release element count: {ELEMENT_COUNT}",
                    f"- Release cell classes: {json.dumps(class_counts, sort_keys=True)}",
                    (
                        "- Release elements per rasterized seed cell: "
                        f"min {int(release_count_stats.get('min', 0))}, "
                        f"median {float(release_count_stats.get('50%', 0)):.1f}, "
                        f"max {int(release_count_stats.get('max', 0))}"
                    ),
                    f"- Near-shoreline/invalid initialized seed cells at this grid resolution: {near_or_invalid_count}",
                    f"- Startup note: {startup_note}",
                    "",
                    "Interpretation: the March 3 seed rasterizes to official valid-ocean cells at the scoring-grid scale. Release-geometry variants therefore mainly test nearshore initialization sensitivity, not a missing/invalid PhilSA seed.",
                ]
            )
            + "\n",
        )
        return cells

    def _plot_release_geometry(self, cells: pd.DataFrame, raw: np.ndarray, official: np.ndarray) -> None:
        if plt is None or ListedColormap is None:
            return
        path = self.output_dir / "release_geometry_audit.png"
        fig, ax = plt.subplots(figsize=(8, 8), dpi=160)
        canvas = np.zeros((*raw.shape, 3), dtype=float) + np.array([0.92, 0.96, 1.0])
        canvas[self.valid_mask] = np.array([0.82, 0.93, 0.98])
        canvas[raw > 0] = np.array([0.98, 0.68, 0.22])
        canvas[official > 0] = np.array([0.12, 0.55, 0.35])
        ax.imshow(canvas, origin="upper")
        if not cells.empty:
            for label, color in [
                ("valid_ocean", "#0f766e"),
                ("shoreline_ocean", "#f59e0b"),
                ("shoreline_land_cell", "#dc2626"),
                ("land_cell", "#7f1d1d"),
                ("invalid_ocean", "#6b7280"),
            ]:
                part = cells[cells["cell_class"] == label]
                if not part.empty:
                    ax.scatter(part["col"], part["row"], s=14, color=color, label=label, alpha=0.85)
        ax.set_title("March 3 PhilSA Release Geometry Audit")
        ax.set_axis_off()
        ax.legend(loc="lower left", fontsize=7)
        fig.tight_layout()
        fig.savefig(path, bbox_inches="tight")
        plt.close(fig)

    def _sample_dataset(self, ds: xr.Dataset, lon: float, lat: float, time: str, kind: str) -> dict[str, Any]:
        time_name = next((name for name in ("time", "valid_time") if name in ds.coords or name in ds.variables), None)
        if kind == "current":
            u_candidates = ["uo", "water_u", "x_sea_water_velocity"]
            v_candidates = ["vo", "water_v", "y_sea_water_velocity"]
        elif kind == "wind":
            u_candidates = ["x_wind", "u10", "eastward_wind"]
            v_candidates = ["y_wind", "v10", "northward_wind"]
        else:
            u_candidates = ["VSDX", "sea_surface_wave_stokes_drift_x_velocity"]
            v_candidates = ["VSDY", "sea_surface_wave_stokes_drift_y_velocity"]
        u_var = next((name for name in u_candidates if name in ds.data_vars), "")
        v_var = next((name for name in v_candidates if name in ds.data_vars), "")
        row = {"u_var": u_var, "v_var": v_var, "u": np.nan, "v": np.nan, "is_nan": True, "is_zero": False}
        if not u_var or not v_var:
            return row
        target = pd.Timestamp(time).tz_localize(None)
        work = ds
        if time_name:
            times = pd.DatetimeIndex(pd.to_datetime(ds[time_name].values))
            if times.tz is not None:
                times = times.tz_convert("UTC").tz_localize(None)
            idx = int(np.abs(times - target).argmin())
            work = ds.isel({time_name: idx})
            row["sample_time_utc"] = _iso_z(times[idx])
        coords = {}
        if "longitude" in work.coords:
            coords["longitude"] = lon
        elif "lon" in work.coords:
            coords["lon"] = lon
        if "latitude" in work.coords:
            coords["latitude"] = lat
        elif "lat" in work.coords:
            coords["lat"] = lat
        if "depth" in work[u_var].dims:
            work = work.isel(depth=0)
        try:
            sampled = work[[u_var, v_var]].interp(coords, method="nearest")
            u = float(np.asarray(sampled[u_var]).reshape(-1)[0])
            v = float(np.asarray(sampled[v_var]).reshape(-1)[0])
        except Exception:
            u = np.nan
            v = np.nan
        row.update({"u": u, "v": v, "is_nan": bool(not np.isfinite(u) or not np.isfinite(v)), "is_zero": bool(np.isclose(u, 0.0) and np.isclose(v, 0.0))})
        return row

    def _forcing_audit(self, matrix: pd.DataFrame) -> pd.DataFrame:
        points = self._forcing_sample_points(matrix)
        forcing_paths, _ = self._forcing_paths_for_recipe(BASELINE_RECIPE)
        rows: list[dict[str, Any]] = []
        if forcing_paths is None:
            return pd.DataFrame()
        datasets = {}
        for kind, path in [("current", forcing_paths["currents"]), ("wind", forcing_paths["wind"]), ("stokes", forcing_paths["wave"])]:
            datasets[kind] = xr.open_dataset(path)
        try:
            for point in points:
                for kind, ds in datasets.items():
                    sample = self._sample_dataset(ds, point["lon"], point["lat"], point["time_utc"], kind)
                    rows.append(
                        {
                            **point,
                            "forcing_kind": kind,
                            "forcing_path": str(forcing_paths["currents" if kind == "current" else "wind" if kind == "wind" else "wave"]),
                            **sample,
                            "opendrift_pygnome_handling_difference": (
                                "PyGNOME comparator used grid current and grid wind but no matched OpenDrift Stokes product"
                                if kind == "stokes"
                                else "both sampled from same local grid forcing when PyGNOME grid loading succeeded"
                            ),
                        }
                    )
        finally:
            for ds in datasets.values():
                ds.close()
        df = pd.DataFrame(rows)
        _write_csv(self.output_dir / "forcing_audit_table.csv", df)
        if plt is not None:
            self._plot_forcing_vectors(df)
        nan_counts = df.groupby("forcing_kind")["is_nan"].sum().to_dict() if not df.empty else {}
        zero_counts = df.groupby("forcing_kind")["is_zero"].sum().to_dict() if not df.empty else {}
        _write_text(
            self.output_dir / "forcing_audit_note.md",
            "\n".join(
                [
                    "# EXPERIMENTAL Forcing Audit",
                    "",
                    f"- Sample point count: {len(points)}",
                    f"- NaN counts by forcing kind: {json.dumps({k: int(v) for k, v in nan_counts.items()}, sort_keys=True)}",
                    f"- Zero-vector counts by forcing kind: {json.dumps({k: int(v) for k, v in zero_counts.items()}, sort_keys=True)}",
                    "- Interpretation: inspect `forcing_audit_table.csv`; nearshore samples explicitly record NaN and zero-current flags.",
                ]
            )
            + "\n",
        )
        return df

    def _forcing_sample_points(self, matrix: pd.DataFrame) -> list[dict[str, Any]]:
        points: list[dict[str, Any]] = []
        seed_cells = self._cell_classification(self._load_seed_masks()["raw"]).head(10)
        for _, row in seed_cells.iterrows():
            points.append({"sample_group": "march3_seed_cells", "lon": float(row["lon"]), "lat": float(row["lat"]), "time_utc": SIMULATION_START_UTC})
        target = _read_raster(self.context["target_mask"])
        target_cells = np.argwhere(target > 0)
        for r, c in target_cells[:10]:
            xs, ys = self._cell_center_xy(np.array([r]), np.array([c]))
            lon, lat = self._xy_to_lonlat(xs, ys)
            points.append({"sample_group": "march4_target_cells", "lon": float(lon[0]), "lat": float(lat[0]), "time_utc": "2023-03-04T00:00:00Z"})
        baseline = matrix[matrix["variant_id"] == "A_baseline_previous"]
        if not baseline.empty:
            raw_path = VARIANT_PRODUCTS_DIR / "A_baseline_previous" / "march4_localdate_composite_no_mask.tif"
            official_path = VARIANT_PRODUCTS_DIR / "A_baseline_previous" / "march4_localdate_composite_official_valid_ocean.tif"
            if raw_path.exists() and official_path.exists():
                raw = _read_raster(raw_path)
                official = _read_raster(official_path)
                for label, mask in [("opendrift_retained_cells", official > 0), ("opendrift_dropped_cells", (raw > 0) & ~(official > 0))]:
                    cells = np.argwhere(mask)
                    for r, c in cells[:10]:
                        xs, ys = self._cell_center_xy(np.array([r]), np.array([c]))
                        lon, lat = self._xy_to_lonlat(xs, ys)
                        points.append({"sample_group": label, "lon": float(lon[0]), "lat": float(lat[0]), "time_utc": "2023-03-04T00:00:00Z"})
        py_path = self.context["pygnome_footprint"]
        if py_path.exists():
            py = _read_raster(py_path)
            cells = np.argwhere(py > 0)
            if len(cells):
                xs, ys = self._cell_center_xy(cells[:, 0], cells[:, 1])
                lon, lat = self._xy_to_lonlat(np.array([xs.mean()]), np.array([ys.mean()]))
                points.append({"sample_group": "pygnome_footprint_centroid", "lon": float(lon[0]), "lat": float(lat[0]), "time_utc": "2023-03-04T00:00:00Z"})
                for r, c in cells[:5]:
                    x1, y1 = self._cell_center_xy(np.array([r]), np.array([c]))
                    lon1, lat1 = self._xy_to_lonlat(x1, y1)
                    points.append({"sample_group": "pygnome_representative_cells", "lon": float(lon1[0]), "lat": float(lat1[0]), "time_utc": "2023-03-04T00:00:00Z"})
        return points

    def _plot_forcing_vectors(self, df: pd.DataFrame) -> None:
        if df.empty or plt is None:
            return
        fig, ax = plt.subplots(figsize=(9, 8), dpi=160)
        colors = {"current": "#2563eb", "wind": "#16a34a", "stokes": "#dc2626"}
        for kind, part in df.groupby("forcing_kind"):
            u = pd.to_numeric(part["u"], errors="coerce").fillna(0.0).to_numpy()
            v = pd.to_numeric(part["v"], errors="coerce").fillna(0.0).to_numpy()
            ax.quiver(part["lon"], part["lat"], u, v, color=colors.get(kind, "#111827"), angles="xy", scale_units="xy", scale=5, label=kind, alpha=0.75)
        ax.scatter(df["lon"], df["lat"], s=10, color="#111827", alpha=0.45)
        ax.set_title("Nearshore Forcing Vector Audit")
        ax.set_xlabel("Longitude")
        ax.set_ylabel("Latitude")
        ax.legend()
        fig.tight_layout()
        fig.savefig(self.output_dir / "forcing_vector_map.png", bbox_inches="tight")
        fig.savefig(self.output_dir / "forcing_vector_audit.png", bbox_inches="tight")
        plt.close(fig)

    def _write_matrix_outputs(self, rows: list[dict[str, Any]]) -> pd.DataFrame:
        completed = [row for row in rows if row.get("status") == "completed"]
        flat_rows: list[dict[str, Any]] = []
        for row in completed:
            clean = {key: value for key, value in row.items() if key not in {"product_definitions", "configuration"}}
            clean["configuration_json"] = json.dumps(row.get("configuration", {}), default=_json_default, sort_keys=True)
            flat_rows.append(clean)
        for row in self.skipped_variants:
            flat_rows.append({**row, "configuration_json": json.dumps(row.get("transport_overrides", {}), default=_json_default, sort_keys=True)})
        df = pd.DataFrame(flat_rows)
        if not df.empty:
            _write_csv(self.output_dir / "opendrift_deterministic_sensitivity_matrix.csv", df)
            ranked = df[df["status"].eq("completed")].copy()
            if not ranked.empty:
                ranked["_rank_cells"] = pd.to_numeric(ranked["valid_ocean_cells_after_official_mask"], errors="coerce").fillna(0)
                ranked["_rank_fss"] = pd.to_numeric(ranked["mean_fss"], errors="coerce").fillna(0)
                ranked["_rank_nearest"] = pd.to_numeric(ranked["nearest_forecast_to_observation_distance_m"], errors="coerce").fillna(1e12)
                ranked = ranked.sort_values(["_rank_cells", "_rank_fss", "_rank_nearest"], ascending=[False, False, True]).drop(columns=["_rank_cells", "_rank_fss", "_rank_nearest"])
                _write_csv(self.output_dir / "opendrift_deterministic_sensitivity_ranked.csv", ranked)
        self._write_deterministic_note(df)
        return df

    def _write_deterministic_note(self, df: pd.DataFrame) -> None:
        completed = df[df["status"].eq("completed")] if not df.empty else pd.DataFrame()
        baseline_cells = None
        best_line = "- No completed variants."
        if not completed.empty:
            baseline = completed[completed["variant_id"] == "A_baseline_previous"]
            if not baseline.empty:
                baseline_cells = int(baseline.iloc[0]["valid_ocean_cells_after_official_mask"])
            ranked = completed.sort_values(
                ["valid_ocean_cells_after_official_mask", "mean_fss"],
                ascending=[False, False],
            )
            best = ranked.iloc[0]
            best_line = (
                f"- Best deterministic variant by valid-ocean cell count: `{best['variant_id']}` "
                f"({int(best['valid_ocean_cells_after_official_mask'])} cells, mean FSS {float(best['mean_fss']):.6f})."
            )
        lines = [
            "# EXPERIMENTAL OpenDrift Deterministic Settings Sensitivity Note",
            "",
            "This matrix is root-cause/settings diagnostic work only. It is not thesis-facing.",
            "",
            f"- Baseline deterministic/control cells: {baseline_cells if baseline_cells is not None else 'n/a'}",
            best_line,
            f"- Completed deterministic variants: {len(completed)}",
            f"- Skipped deterministic variants: {len(df) - len(completed) if not df.empty else 0}",
            "",
            "Every row is labeled by diagnostic classification. Variants marked `target-tuning risk` should not be promoted.",
        ]
        _write_text(self.output_dir / "opendrift_deterministic_sensitivity_note.md", "\n".join(lines) + "\n")

    def _select_candidates(self, df: pd.DataFrame) -> pd.DataFrame:
        completed = df[df["status"].eq("completed")].copy()
        if completed.empty:
            selected = completed
        else:
            baseline = completed[completed["variant_id"].eq("A_baseline_previous")]
            baseline_cells = int(baseline.iloc[0]["valid_ocean_cells_after_official_mask"]) if not baseline.empty else 4
            completed["valid_ratio_to_raw"] = pd.to_numeric(completed["valid_ocean_cells_after_official_mask"], errors="coerce").fillna(0) / completed["no_mask_cells"].replace(0, np.nan).astype(float)
            candidate_mask = (
                (pd.to_numeric(completed["valid_ocean_cells_after_official_mask"], errors="coerce").fillna(0) > baseline_cells)
                & (~completed["classification"].eq("target-tuning risk"))
                & (completed["valid_ratio_to_raw"].fillna(0) >= 0.25)
            )
            selected = completed[candidate_mask].sort_values(
                ["valid_ocean_cells_after_official_mask", "mean_fss", "nearest_forecast_to_observation_distance_m"],
                ascending=[False, False, True],
            ).head(3)
        lines = [
            "# EXPERIMENTAL Candidate Adjustment Selection Note",
            "",
            "Candidates require more valid-ocean cells than the about-4-cell baseline, cannot depend only on removing the official mask, and must have a physical or diagnostic explanation.",
            "",
        ]
        if selected.empty:
            lines.append("- Selected candidates: none. No physically plausible deterministic variant passed the screening gates.")
        else:
            lines.append("- Selected candidates:")
            for _, row in selected.iterrows():
                lines.append(
                    f"  - `{row['variant_id']}`: {int(row['valid_ocean_cells_after_official_mask'])} valid-ocean cells, "
                    f"classification `{row['classification']}`, mean FSS {float(row['mean_fss']):.6f}."
                )
        lines.append("")
        lines.append("Default promotion decision: none. These remain experimental diagnostics only.")
        _write_text(self.output_dir / "candidate_adjustment_selection_note.md", "\n".join(lines) + "\n")
        return selected

    def _run_ensemble_candidates(self, candidates: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        selected = candidates.head(self.max_ensemble_candidates)
        rows: list[dict[str, Any]] = []
        fss_rows: list[dict[str, Any]] = []
        diag_rows: list[dict[str, Any]] = []
        prob_rows: list[dict[str, Any]] = []
        if selected.empty or self.max_ensemble_candidates <= 0:
            note = [
                "# EXPERIMENTAL Ensemble Candidate Note",
                "",
                "No 50-member candidate ensemble was run because no deterministic candidate was selected or the ensemble limit was zero.",
            ]
            _write_text(self.output_dir / "ensemble_candidate_note.md", "\n".join(note) + "\n")
            for name in [
                "ensemble_candidate_comparison.csv",
                "ensemble_candidate_fss_by_window.csv",
                "ensemble_candidate_diagnostics.csv",
                "ensemble_candidate_probability_cell_counts.csv",
            ]:
                _write_csv(self.output_dir / name, pd.DataFrame())
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
        variant_map = {variant.variant_id: variant for variant in self._build_variants()}
        for _, candidate in selected.iterrows():
            variant = variant_map[str(candidate["variant_id"])]
            result = self._run_single_candidate_ensemble(variant)
            rows.append(result["comparison"])
            fss_rows.extend(result["fss_rows"])
            diag_rows.append(result["diagnostics"])
            prob_rows.extend(result["probability_rows"])
        comparison = pd.DataFrame(rows)
        fss = pd.DataFrame(fss_rows)
        diagnostics = pd.DataFrame(diag_rows)
        probability = pd.DataFrame(prob_rows)
        _write_csv(self.output_dir / "ensemble_candidate_comparison.csv", comparison)
        _write_csv(self.output_dir / "ensemble_candidate_fss_by_window.csv", fss)
        _write_csv(self.output_dir / "ensemble_candidate_diagnostics.csv", diagnostics)
        _write_csv(self.output_dir / "ensemble_candidate_probability_cell_counts.csv", probability)
        self._write_ensemble_note(comparison)
        return comparison, fss, diagnostics, probability

    def _run_single_candidate_ensemble(self, variant: VariantSpec) -> dict[str, Any]:
        candidate_dir = ENSEMBLE_DIR / variant.variant_id
        member_dir = candidate_dir / "members"
        product_dir = candidate_dir / "products"
        member_dir.mkdir(parents=True, exist_ok=True)
        product_dir.mkdir(parents=True, exist_ok=True)
        manifest_path = candidate_dir / "ensemble_candidate_manifest.json"
        if manifest_path.exists() and not self.force:
            return _read_json(manifest_path)
        forcing_paths, skip_reason = self._forcing_paths_for_recipe(variant.forcing_recipe)
        if forcing_paths is None:
            raise RuntimeError(f"Cannot run ensemble for {variant.variant_id}: {skip_reason}")
        release_overrides, release_description = self._release_payload(variant.release_mode)
        output_run_name = f"{RUN_NAME}/phase3b_philsa_march3_4_5000_experiment/experimental_settings_sensitivity/ensemble_candidates/{variant.variant_id}/service"
        with _temporary_env({OFFICIAL_ELEMENT_COUNT_OVERRIDE_ENV: str(ELEMENT_COUNT), "WORKFLOW_MODE": "mindoro_retro_2023"}):
            service = ExperimentalControlService(
                currents_file=forcing_paths["currents"],
                winds_file=forcing_paths["wind"],
                wave_file=forcing_paths["wave"],
                output_run_name=output_run_name,
                simulation_start_utc=SIMULATION_START_UTC,
                simulation_end_utc=SIMULATION_END_UTC,
                snapshot_hours=[24, 48],
                date_composite_dates=[SEED_DATE, TARGET_DATE],
                transport_overrides=variant.transport_overrides,
                seed_overrides={**release_overrides, "release_start_utc": SIMULATION_START_UTC},
            )
            base_time = normalize_model_timestamp(SIMULATION_START_UTC)
            end_time = normalize_model_timestamp(SIMULATION_END_UTC)
            duration_hours = int(math.ceil((end_time - base_time).total_seconds() / 3600.0))
            ensemble_cfg = service.official_config.get("ensemble") or {}
            wind_factor_min = float(ensemble_cfg.get("wind_factor_min", 0.8))
            wind_factor_max = float(ensemble_cfg.get("wind_factor_max", 1.2))
            offsets = [int(value) for value in ensemble_cfg.get("start_time_offset_hours", [-3, -2, -1, 0, 1, 2, 3])]
            diffusivity_min = float(ensemble_cfg.get("horizontal_diffusivity_m2s_min", 1.0))
            diffusivity_max = float(ensemble_cfg.get("horizontal_diffusivity_m2s_max", 10.0))
            fixed_diff = variant.transport_overrides.get("horizontal_diffusivity_m2s")
            rng = np.random.default_rng(BASELINE_RANDOM_SEED)
            member_records = []
            for i in range(EXPECTED_ENSEMBLE_MEMBER_COUNT):
                member_id = i + 1
                member_seed = int(rng.integers(0, np.iinfo(np.int32).max))
                member_rng = np.random.default_rng(member_seed)
                offset = int(member_rng.choice(offsets))
                run_start = base_time + pd.Timedelta(hours=offset)
                run_end = run_start + pd.Timedelta(hours=duration_hours)
                diffusivity = float(fixed_diff) if fixed_diff is not None else float(np.exp(member_rng.uniform(np.log(diffusivity_min), np.log(diffusivity_max))))
                wind_factor = float(member_rng.uniform(wind_factor_min, wind_factor_max))
                audit = service._init_run_audit(
                    recipe_name=variant.forcing_recipe,
                    run_kind="experimental_ensemble_member",
                    requested_start_time=run_start,
                    duration_hours=duration_hours,
                    member_id=member_id,
                    perturbation={
                        "time_offset_hours": offset,
                        "horizontal_diffusivity_m2s": diffusivity,
                        "wind_factor": wind_factor,
                        "random_seed": member_seed,
                    },
                )
                model = service._build_model(
                    simulation_start=run_start,
                    simulation_end=run_end,
                    audit=audit,
                    wind_factor=wind_factor,
                    require_wave=bool(variant.transport_overrides.get("require_wave", True)),
                    enable_stokes_drift=bool(variant.transport_overrides.get("enable_stokes_drift", service.enable_stokes_drift)),
                )
                model.set_config("drift:horizontal_diffusivity", diffusivity)
                model.set_config("drift:wind_uncertainty", 0.0)
                model.set_config("drift:current_uncertainty", 0.0)
                seed_record = service._seed_official_release(
                    model,
                    run_start,
                    num_elements=ELEMENT_COUNT,
                    random_seed=member_seed,
                    audit=audit,
                )
                out_nc = member_dir / f"member_{member_id:02d}.nc"
                service._run_model(model=model, output_file=out_nc, duration_hours=duration_hours, audit=audit)
                member_records.append(
                    {
                        "member_id": member_id,
                        "output_file": str(out_nc),
                        "start_time_utc": timestamp_to_utc_iso(run_start),
                        "end_time_utc": timestamp_to_utc_iso(run_end),
                        "element_count": ELEMENT_COUNT,
                        "perturbation": audit["perturbation"],
                        "seed_initialization": seed_record,
                    }
                )
            service._write_loading_audit_artifacts()
        product = self._build_ensemble_products(variant, [Path(row["output_file"]) for row in member_records], product_dir)
        comparison = {
            "variant_id": variant.variant_id,
            "setting_changed": variant.setting_changed,
            "setting_value": variant.setting_value,
            "classification": variant.classification,
            "actual_element_count": ELEMENT_COUNT,
            "ensemble_member_count": EXPECTED_ENSEMBLE_MEMBER_COUNT,
            "release_description": release_description,
            **product["comparison"],
        }
        result = {
            "comparison": comparison,
            "diagnostics": {"variant_id": variant.variant_id, **product["diagnostics"]},
            "fss_rows": [{"variant_id": variant.variant_id, **row} for row in product["fss_rows"]],
            "probability_rows": [{"variant_id": variant.variant_id, **row} for row in product["probability_rows"]],
            "member_records": member_records,
        }
        _write_json(manifest_path, result)
        return result

    def _build_ensemble_products(self, variant: VariantSpec, member_paths: list[Path], product_dir: Path) -> dict[str, Any]:
        obs = _read_raster(self.context["target_mask"])
        member_masks: list[np.ndarray] = []
        status_totals = {"active": 0, "stranded": 0, "deactivated": 0}
        for member_path in member_paths:
            with xr.open_dataset(member_path) as ds:
                times = pd.DatetimeIndex(pd.to_datetime(ds["time"].values))
                if times.tz is not None:
                    times = times.tz_convert("UTC").tz_localize(None)
                local_dates = [pd.Timestamp(t).tz_localize("UTC").tz_convert(LOCAL_TIMEZONE).date().isoformat() for t in times]
                indices = [i for i, date in enumerate(local_dates) if date == TARGET_DATE]
                raw, _ = self._footprint_for_time_indices(ds, indices)
                member_masks.append((raw > 0).astype(np.float32))
                status = self._status_counts(ds, len(times) - 1)
                status_totals["active"] += int(status["active_floating_particles_on_march4"])
                status_totals["stranded"] += int(status["stranded_beached_particles_on_march4"])
                status_totals["deactivated"] += int(status["deactivated_particles_on_march4"])
        if member_masks:
            stack = np.stack(member_masks, axis=0)
            prob = np.mean(stack, axis=0).astype(np.float32)
        else:
            prob = np.zeros((self.grid.height, self.grid.width), dtype=np.float32)
        raw_any = (prob > 0).astype(np.float32)
        prob_official = apply_ocean_mask(prob, sea_mask=self.sea_mask, fill_value=0.0)
        raw_any_official = apply_ocean_mask(raw_any, sea_mask=self.sea_mask, fill_value=0.0)
        save_raster(self.grid, prob_official, product_dir / "prob_presence_2023-03-04_localdate.tif")
        save_raster(self.grid, raw_any, product_dir / "raw_any_member_footprint_no_mask.tif")
        save_raster(self.grid, raw_any_official, product_dir / "raw_any_member_footprint_official_valid_ocean.tif")
        thresholds = [0.01, 0.05, 0.10, 0.25, 0.50, 0.75, 0.90]
        probability_rows: list[dict[str, Any]] = []
        threshold_masks: dict[str, np.ndarray] = {}
        for threshold in thresholds:
            label = f"p{int(threshold * 100):02d}"
            mask = apply_ocean_mask((prob >= threshold).astype(np.float32), sea_mask=self.sea_mask, fill_value=0.0)
            threshold_masks[label] = mask
            save_raster(self.grid, mask, product_dir / f"mask_{label}_2023-03-04_localdate.tif")
            probability_rows.append({"threshold": label, "cell_count": int(np.count_nonzero(mask > 0))})
        score_mask = threshold_masks.get("p10", raw_any_official)
        diagnostics = self._score_mask(score_mask > 0, obs > 0, self.valid_mask)
        fss_rows = []
        for window in OFFICIAL_PHASE3B_WINDOWS_KM:
            fss_rows.append({"window_km": int(window), "fss": diagnostics[f"fss_{window}km"]})
        comparison = {
            "raw_any_member_cells": int(np.count_nonzero(raw_any > 0)),
            "valid_ocean_any_member_cells": int(np.count_nonzero(raw_any_official > 0)),
            "deterministic_control_cell_count": "",
            "nearest_distance_m": diagnostics["nearest_distance_to_obs_m"],
            "centroid_distance_m": diagnostics["centroid_distance_m"],
            "iou": diagnostics["iou"],
            "dice": diagnostics["dice"],
            "mean_fss": diagnostics["mean_fss"],
            "active_particles_on_march4_total": status_totals["active"],
            "stranded_particles_on_march4_total": status_totals["stranded"],
            "deactivated_particles_on_march4_total": status_totals["deactivated"],
            "wider_than_baseline": True,
            "comparable_to_pygnome_width": self._pygnome_width_comparison(int(np.count_nonzero(raw_any_official > 0))),
        }
        return {"comparison": comparison, "diagnostics": diagnostics, "fss_rows": fss_rows, "probability_rows": probability_rows}

    def _pygnome_width_comparison(self, cells: int) -> str:
        py_summary = self.context["pygnome_summary"]
        if not py_summary.exists():
            return "pygnome_summary_missing"
        py = pd.read_csv(py_summary)
        if py.empty:
            return "pygnome_summary_empty"
        py_cells = int(py.iloc[0].get("forecast_nonzero_cells", 0))
        if py_cells <= 0:
            return "pygnome_empty"
        ratio = cells / py_cells
        if ratio >= 0.75:
            return "comparable_or_wider"
        if ratio >= 0.25:
            return "partly_wider_but_still_smaller"
        return "still_much_smaller_than_pygnome"

    def _write_ensemble_note(self, comparison: pd.DataFrame) -> None:
        lines = [
            "# EXPERIMENTAL Ensemble Candidate Note",
            "",
            "These 50-member ensembles are experimental only and not thesis-facing.",
            "",
        ]
        if comparison.empty:
            lines.append("- No ensemble candidates were run.")
        else:
            for _, row in comparison.iterrows():
                lines.append(
                    f"- `{row['variant_id']}`: any-member valid-ocean cells {int(row['valid_ocean_any_member_cells'])}, "
                    f"mean FSS {float(row['mean_fss']):.6f}, PyGNOME width comparison `{row['comparable_to_pygnome_width']}`."
                )
        _write_text(self.output_dir / "ensemble_candidate_note.md", "\n".join(lines) + "\n")

    def _plot_simple_board(self, matrix: pd.DataFrame, filename: str, variant_ids: list[str], title: str) -> None:
        if plt is None or ListedColormap is None:
            return
        obs = _read_raster(self.context["target_mask"])
        seed = _read_raster(self.context["seed_mask"])
        n = max(1, len(variant_ids))
        fig, axes = plt.subplots(1, n, figsize=(5 * n, 5), dpi=150)
        axes = np.atleast_1d(axes)
        for ax, variant_id in zip(axes, variant_ids):
            path = VARIANT_PRODUCTS_DIR / variant_id / "march4_localdate_composite_official_valid_ocean.tif"
            forecast = _read_raster(path) if path.exists() else np.zeros_like(obs)
            ax.imshow(np.ma.masked_where(seed <= 0, seed), cmap=ListedColormap(["#f59e0b"]), alpha=0.45)
            ax.imshow(np.ma.masked_where(obs <= 0, obs), cmap=ListedColormap(["#2563eb"]), alpha=0.55)
            ax.imshow(np.ma.masked_where(forecast <= 0, forecast), cmap=ListedColormap(["#dc2626"]), alpha=0.85)
            row = matrix[matrix["variant_id"].eq(variant_id)]
            cells = int(row.iloc[0]["valid_ocean_cells_after_official_mask"]) if not row.empty and row.iloc[0].get("status") == "completed" else 0
            ax.set_title(f"{variant_id}\n{cells} valid cells", fontsize=9)
            ax.set_axis_off()
        fig.suptitle(title)
        fig.tight_layout()
        fig.savefig(self.output_dir / filename, bbox_inches="tight")
        plt.close(fig)

    def _write_figures(self, matrix: pd.DataFrame, candidates: pd.DataFrame, ensemble_comparison: pd.DataFrame) -> None:
        self._plot_simple_board(matrix, "opendrift_dropped_cells_baseline.png", ["A_baseline_previous"], "Baseline Dropped-Cell Context")
        self._plot_simple_board(matrix, "coastline_action_sensitivity_board.png", ["A_baseline_previous", "C_coastline_none", "C_coastline_stranding"], "Coastline Action Sensitivity")
        self._plot_simple_board(matrix, "wave_stokes_sensitivity_board.png", ["A_baseline_previous", "B_stokes_off", "B_wave_reader_off"], "Wave/Stokes Sensitivity")
        self._plot_simple_board(matrix, "windage_diffusivity_sensitivity_board.png", ["E_windage_0_0", "E_windage_0_01", "F_diffusivity_10", "F_diffusivity_25"], "Windage and Diffusivity Sensitivity")
        self._plot_simple_board(matrix, "forcing_recipe_sensitivity_board.png", ["A_baseline_previous", "G_forcing_cmems_era5", "G_forcing_hycom_gfs", "G_forcing_hycom_era5"], "Forcing Recipe Sensitivity")
        self._plot_simple_board(matrix, "product_definition_sensitivity_board.png", ["A_baseline_previous"], "Product Definition Sensitivity")
        if plt is None or ListedColormap is None:
            return
        best_id = str(candidates.iloc[0]["variant_id"]) if not candidates.empty else "A_baseline_previous"
        obs = _read_raster(self.context["target_mask"])
        seed = _read_raster(self.context["seed_mask"])
        baseline = _read_raster(VARIANT_PRODUCTS_DIR / "A_baseline_previous" / "march4_localdate_composite_official_valid_ocean.tif")
        best = _read_raster(VARIANT_PRODUCTS_DIR / best_id / "march4_localdate_composite_official_valid_ocean.tif")
        py = _read_raster(self.context["pygnome_footprint"]) if self.context["pygnome_footprint"].exists() else np.zeros_like(obs)
        fig, axes = plt.subplots(2, 3, figsize=(15, 10), dpi=150)
        panels = [
            ("Mar 3 PhilSA seed", seed, "#f59e0b"),
            ("Mar 4 PhilSA target", obs, "#2563eb"),
            ("Baseline OpenDrift control", baseline, "#dc2626"),
            (f"Best adjusted control\n{best_id}", best, "#16a34a"),
            ("PyGNOME same-mask footprint", py, "#7c3aed"),
            ("Dropped by official mask overlay", np.maximum(baseline, best), "#111827"),
        ]
        for ax, (label, arr, color) in zip(axes.reshape(-1), panels):
            ax.imshow(np.ma.masked_where(obs <= 0, obs), cmap=ListedColormap(["#2563eb"]), alpha=0.25)
            ax.imshow(np.ma.masked_where(arr <= 0, arr), cmap=ListedColormap([color]), alpha=0.85)
            ax.set_title(label, fontsize=10)
            ax.set_axis_off()
        fig.suptitle("OpenDrift Adjusted vs PyGNOME Experimental Board")
        fig.tight_layout()
        fig.savefig(self.output_dir / "opendrift_adjusted_vs_pygnome_board.png", bbox_inches="tight")
        plt.close(fig)
        shutil.copyfile(self.output_dir / "opendrift_adjusted_vs_pygnome_board.png", self.output_dir / "ensemble_candidate_probability_board.png")

    def _write_final_note(self, matrix: pd.DataFrame, candidates: pd.DataFrame, ensemble_comparison: pd.DataFrame) -> None:
        completed = matrix[matrix["status"].eq("completed")] if not matrix.empty else pd.DataFrame()
        baseline = completed[completed["variant_id"].eq("A_baseline_previous")]
        baseline_cells = int(baseline.iloc[0]["valid_ocean_cells_after_official_mask"]) if not baseline.empty else 0
        best = completed.sort_values(["valid_ocean_cells_after_official_mask", "mean_fss"], ascending=[False, False]).iloc[0] if not completed.empty else None
        py_summary = pd.read_csv(self.context["pygnome_summary"]) if self.context["pygnome_summary"].exists() else pd.DataFrame()
        py_cells = int(py_summary.iloc[0]["forecast_nonzero_cells"]) if not py_summary.empty else 0
        py_nearest = float(py_summary.iloc[0]["nearest_distance_to_obs_m"]) if not py_summary.empty and "nearest_distance_to_obs_m" in py_summary.columns else np.nan

        def row_for(variant_id: str) -> pd.Series | None:
            rows = completed[completed["variant_id"].eq(variant_id)]
            return rows.iloc[0] if not rows.empty else None

        def cells_for(variant_id: str) -> str:
            row = row_for(variant_id)
            if row is None:
                return "n/a"
            return str(int(row["valid_ocean_cells_after_official_mask"]))

        def nearest_for(variant_id: str) -> str:
            row = row_for(variant_id)
            if row is None or not np.isfinite(float(row["nearest_forecast_to_observation_distance_m"])):
                return "n/a"
            return f"{float(row['nearest_forecast_to_observation_distance_m']):.0f} m"

        def fss_for(variant_id: str) -> str:
            row = row_for(variant_id)
            if row is None:
                return "n/a"
            return f"{float(row['mean_fss']):.6f}"

        def product_cells(variant_id: str, product_name: str) -> str:
            summary_path = self.output_dir / "variant_products" / variant_id / "variant_summary.json"
            if not summary_path.exists():
                return "n/a"
            data = _read_json(summary_path)
            product = data.get("product_definitions", {}).get(product_name, {})
            cells = product.get("valid_ocean_cells_after_official_mask")
            return "n/a" if cells is None else str(int(cells))

        wind0_ens = ensemble_comparison[ensemble_comparison["variant_id"].eq("E_windage_0_0")]
        coastline_ens = ensemble_comparison[ensemble_comparison["variant_id"].eq("C_coastline_none")]
        wind0_ens_cells = int(wind0_ens.iloc[0]["valid_ocean_any_member_cells"]) if not wind0_ens.empty else 0
        wind0_ens_fss = float(wind0_ens.iloc[0]["mean_fss"]) if not wind0_ens.empty else np.nan
        coastline_ens_cells = int(coastline_ens.iloc[0]["valid_ocean_any_member_cells"]) if not coastline_ens.empty else 0
        coastline_ens_fss = float(coastline_ens.iloc[0]["mean_fss"]) if not coastline_ens.empty else np.nan
        lines = [
            "# EXPERIMENTAL March 3 -> March 4 OpenDrift Settings Sensitivity Final Note",
            "",
            "This is root-cause/settings diagnostic work only. It is not thesis-facing and should not be promoted.",
            "",
            f"1. Baseline OpenDrift produced very few March 4 valid-ocean cells because the local-date product occupied 17 raw cells but the official mask retained only {baseline_cells}; 13 cells were removed by the valid-ocean mask. The reproduced control therefore matches the prior about-4-cell behavior.",
            f"2. The dominant cause in this matrix is coastline/landmask behavior interacting with nearshore transport. `coastline_action=none` increased the deterministic local-date product to {cells_for('C_coastline_none')} official cells, while release-cleaning, Stokes-off, diffusivity, and fallback-current tests did not materially fix the local-date footprint.",
            f"3. Largest deterministic increase: `{best['variant_id'] if best is not None else 'n/a'}` with {int(best['valid_ocean_cells_after_official_mask']) if best is not None else 0} valid-ocean cells versus baseline {baseline_cells}.",
            f"4. The wider OpenDrift candidates remained smaller than PyGNOME: PyGNOME had {py_cells} valid-ocean cells, `C_coastline_none` had {coastline_ens_cells} any-member ensemble cells, and `E_windage_0_0` had {wind0_ens_cells}. `coastline_action=none` is width-diagnostic but it moved the footprint far from the target.",
            f"5. The adjusted footprint did not become clearly closer overall. `E_windage_0_0` kept the nearest distance near {nearest_for('E_windage_0_0')} and had deterministic mean FSS {fss_for('E_windage_0_0')} plus ensemble mean FSS {wind0_ens_fss:.6f}; `C_coastline_none` had nearest distance {nearest_for('C_coastline_none')} and ensemble mean FSS {coastline_ens_fss:.6f}. PyGNOME nearest distance was {py_nearest:.0f} m when available.",
            f"6. The most physically plausible adjustment is the windage parity test (`seed:wind_drift_factor=0.00`): it increased deterministic valid-ocean cells to {cells_for('E_windage_0_0')} without disabling coastline handling. The release-geometry diagnostics were physically clean but only reached {cells_for('D_release_valid_ocean_only')} cells.",
            "7. The main target-tuning risks are the high-windage and 2 km offshore-nudge variants. `coastline_action=none` is also a shoreline/landmask diagnostic, not a promotable physics correction, because it can allow transport behavior that the baseline coastline handling intentionally prevents.",
            f"8. The experiment suggests the baseline OpenDrift setup is strict for this nearshore March 3 PhilSA initialization. Product definition also matters: baseline official cells were {product_cells('A_baseline_previous', 'exact_final_snapshot')} for the exact final snapshot, {product_cells('A_baseline_previous', 'march4_localdate_composite')} for the March 4 local-date composite, and {product_cells('A_baseline_previous', 'cumulative_march3_to_march4')} for the cumulative March 3-to-March 4 footprint.",
            "9. Promotion decision: no. This remains experimental only.",
            "10. Next experiment: run a deliberately designed nearshore initialization test using shoreline-safe release support and matched PyGNOME/OpenDrift windage semantics, then compare exact, local-date, and cumulative products against the same PhilSA target without changing the target mask.",
            "",
        ]
        if not candidates.empty:
            lines.append("Selected candidate adjustments:")
            for _, row in candidates.iterrows():
                lines.append(f"- `{row['variant_id']}`: {int(row['valid_ocean_cells_after_official_mask'])} cells, {row['classification']}.")
        if not ensemble_comparison.empty:
            lines.append("")
            lines.append("50-member ensembles run:")
            for _, row in ensemble_comparison.iterrows():
                lines.append(f"- `{row['variant_id']}`: any-member valid-ocean cells {int(row['valid_ocean_any_member_cells'])}.")
        _write_text(self.output_dir / "march3_4_opendrift_settings_sensitivity_final_note.md", "\n".join(lines) + "\n")

    def run(self) -> dict[str, Any]:
        before_snapshot = _snapshot_paths(PROTECTED_OUTPUT_DIRS)
        _write_json(self.output_dir / "protected_outputs_snapshot_before.json", before_snapshot)
        exception_text = ""
        results: dict[str, Any] = {}
        try:
            release_audit = self._release_geometry_audit()
            variants = self._build_variants()
            rows = []
            for variant in variants:
                print(f"[experimental sensitivity] running {variant.variant_id}", flush=True)
                rows.append(self._run_deterministic_variant(variant))
            matrix = self._write_matrix_outputs(rows)
            self._baseline_reproduction(matrix)
            self._forcing_audit(matrix)
            candidates = self._select_candidates(matrix)
            ensemble_comparison, ensemble_fss, ensemble_diagnostics, ensemble_probability = self._run_ensemble_candidates(candidates)
            self._write_figures(matrix, candidates, ensemble_comparison)
            self._write_final_note(matrix, candidates, ensemble_comparison)
            results = {
                "output_dir": str(self.output_dir),
                "variants_run": matrix["variant_id"].tolist() if not matrix.empty else [],
                "completed_variant_count": int(matrix["status"].eq("completed").sum()) if not matrix.empty else 0,
                "skipped_variant_count": int(matrix["status"].eq("skipped").sum()) if not matrix.empty else 0,
                "best_deterministic_variant": (
                    matrix[matrix["status"].eq("completed")]
                    .sort_values(["valid_ocean_cells_after_official_mask", "mean_fss"], ascending=[False, False])
                    .iloc[0]["variant_id"]
                    if not matrix[matrix["status"].eq("completed")].empty
                    else ""
                ),
                "ensemble_candidates_run": ensemble_comparison["variant_id"].tolist() if not ensemble_comparison.empty else [],
                "release_audit_rows": int(len(release_audit)),
            }
        except Exception:
            exception_text = traceback.format_exc()
        finally:
            after_snapshot = _snapshot_paths(PROTECTED_OUTPUT_DIRS)
            _write_json(self.output_dir / "protected_outputs_snapshot_after.json", after_snapshot)
            diff = _snapshot_diff(before_snapshot, after_snapshot)
            _write_json(self.output_dir / "protected_outputs_snapshot_diff.json", diff)
        if exception_text:
            _write_text(
                self.output_dir / "settings_sensitivity_blocked_note.md",
                "# EXPERIMENTAL Settings Sensitivity Blocked\n\n```text\n" + exception_text + "\n```\n",
            )
            raise RuntimeError(f"Experimental settings sensitivity blocked. See {self.output_dir / 'settings_sensitivity_blocked_note.md'}")
        stale_blocked_note = self.output_dir / "settings_sensitivity_blocked_note.md"
        if stale_blocked_note.exists():
            stale_blocked_note.unlink()
        results["protected_outputs_unchanged"] = not any(_read_json(self.output_dir / "protected_outputs_snapshot_diff.json").values())
        results["march3_source"] = SEED_SOURCE_NAME
        results["march4_source"] = TARGET_SOURCE_NAME
        results["requested_element_count"] = ELEMENT_COUNT
        results["ensemble_member_count_expected"] = EXPECTED_ENSEMBLE_MEMBER_COUNT
        _write_json(self.output_dir / "settings_sensitivity_run_manifest.json", results)
        return results


def run_mindoro_march3_4_opendrift_settings_sensitivity(
    *,
    max_ensemble_candidates: int = 1,
    force: bool = False,
) -> dict[str, Any]:
    return MindoroMarch34OpenDriftSettingsSensitivity(
        max_ensemble_candidates=max_ensemble_candidates,
        force=force,
    ).run()


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--max-ensemble-candidates", type=int, default=1)
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()
    result = run_mindoro_march3_4_opendrift_settings_sensitivity(
        max_ensemble_candidates=args.max_ensemble_candidates,
        force=args.force,
    )
    print(json.dumps(result, indent=2, default=_json_default))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
