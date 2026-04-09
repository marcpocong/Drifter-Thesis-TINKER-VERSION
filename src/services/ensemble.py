"""
Ensemble forecasting module for uncertainty quantification.
Executes Phase 2: Monte Carlo ensemble with perturbations.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import sys
from contextlib import contextmanager
from datetime import timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr
import yaml
from opendrift.models.oceandrift import OceanDrift
from opendrift.readers import reader_global_landmask, reader_netCDF_CF_generic

from src.core.case_context import get_case_context
from src.core.constants import BASE_OUTPUT_DIR
from src.helpers.plotting import plot_probability_map
from src.helpers.raster import (
    GridBuilder,
    project_points_to_grid,
    rasterize_particles,
    save_raster,
)
from src.helpers.scoring import apply_ocean_mask, load_sea_mask_array
from src.utils.io import (
    RecipeSelection,
    find_current_vars,
    find_wave_vars,
    find_wind_vars,
    get_deterministic_control_output_path,
    get_ensemble_manifest_path,
    get_ensemble_probability_score_raster_path,
    get_forcing_files,
    get_forecast_manifest_path,
    get_official_control_density_norm_path,
    get_official_control_footprint_mask_path,
    get_official_mask_p50_datecomposite_path,
    get_official_mask_threshold_path,
    get_official_prob_presence_path,
    get_phase2_loading_audit_paths,
    resolve_spill_origin,
)

logger = logging.getLogger(__name__)

CURRENT_REQUIRED_VARS = ["x_sea_water_velocity", "y_sea_water_velocity"]
WIND_REQUIRED_VARS = ["x_wind", "y_wind"]
WAVE_VARIABLE_HINTS = [
    "sea_surface_wave_significant_height",
    "significant_wave_height",
    "VHM0",
    "VHMO",
    "swh",
    "Hs",
]
WAVE_REQUIRED_VARS = [
    "sea_surface_wave_stokes_drift_x_velocity",
    "sea_surface_wave_stokes_drift_y_velocity",
    "sea_surface_wave_significant_height",
]
OFFICIAL_ELEMENT_COUNT_OVERRIDE_ENV = "OFFICIAL_ELEMENT_COUNT_OVERRIDE"


def normalize_model_timestamp(value) -> pd.Timestamp:
    """Normalize datetimes to UTC-naive timestamps for OpenDrift compatibility."""
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        return timestamp
    return timestamp.tz_convert("UTC").tz_localize(None)


def normalize_time_index(values) -> pd.DatetimeIndex:
    """Normalize arrays of datetimes to UTC-naive pandas indices."""
    index = pd.DatetimeIndex(pd.to_datetime(values))
    if index.tz is not None:
        index = index.tz_convert("UTC").tz_localize(None)
    return index


def timestamp_to_utc_iso(value) -> str:
    """Format a timestamp as canonical UTC Zulu time."""
    return normalize_model_timestamp(value).strftime("%Y-%m-%dT%H:%M:%SZ")


def timestamp_to_label(value) -> str:
    """Format a timestamp for machine-readable filenames."""
    return normalize_model_timestamp(value).strftime("%Y-%m-%dT%H-%M-%SZ")


def detect_time_coordinate(ds: xr.Dataset) -> str | None:
    """Return the canonical time coordinate/variable name when present."""
    for name in ("time", "valid_time"):
        if name in ds.coords or name in ds.variables:
            return name
    return None


def infer_time_step_hours(times: pd.DatetimeIndex) -> float | None:
    """Infer the dominant time-step spacing in hours for a forcing series."""
    if len(times) < 2:
        return None

    diffs = np.diff(times.view("int64")) / 3_600_000_000_000.0
    positive = diffs[diffs > 0]
    if positive.size == 0:
        return None
    return float(np.median(positive))


def grid_id_from_builder(grid: GridBuilder) -> str:
    """Build a compact canonical grid identifier."""
    return (
        f"{grid.crs}:{grid.resolution}:{grid.width}x{grid.height}:"
        f"{int(round(grid.min_x))}:{int(round(grid.min_y))}:"
        f"{int(round(grid.max_x))}:{int(round(grid.max_y))}"
    )


@contextmanager
def intercept_sys_exit():
    """Turn OpenDrift's sys.exit aborts into catchable exceptions."""
    original_exit = sys.exit

    def _raise_runtime_error(message=None):
        raise RuntimeError(message if message is not None else "sys.exit()")

    sys.exit = _raise_runtime_error
    try:
        yield
    finally:
        sys.exit = original_exit


def _relative_output_path(path: Path, base_output_dir: Path = BASE_OUTPUT_DIR) -> str:
    try:
        return str(path.relative_to(base_output_dir))
    except ValueError:
        return str(path)


def _write_text_atomic(path: Path, text: str, encoding: str = "utf-8") -> None:
    """Write text files atomically so interrupted runs do not leave corrupt artifacts."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        with open(temp_path, "w", encoding=encoding, newline="") as f:
            f.write(text)
            f.flush()
            os.fsync(f.fileno())
        os.replace(temp_path, path)
    finally:
        if temp_path.exists():
            temp_path.unlink(missing_ok=True)


def _write_json_atomic(path: Path, payload: dict | list) -> None:
    """Write JSON files atomically for machine-readable audit and manifest artifacts."""
    _write_text_atomic(path, json.dumps(payload, indent=2) + "\n")


def _stringify_exception_chain(exc: BaseException) -> list[str]:
    messages: list[str] = []
    seen: set[int] = set()
    current: BaseException | None = exc
    while current is not None and id(current) not in seen:
        messages.append(f"{type(current).__name__}: {current}")
        seen.add(id(current))
        current = current.__cause__ or current.__context__
    return messages


def _root_cause_message(exc: BaseException) -> str:
    chain = _stringify_exception_chain(exc)
    return chain[-1] if chain else f"{type(exc).__name__}: {exc}"


def _threshold_label(threshold: float) -> str:
    return f"p{int(round(threshold * 100))}"


class EnsembleForecastService:
    def __init__(
        self,
        currents_file,
        winds_file,
        wave_file=None,
        output_run_name: str | None = None,
        sensitivity_context: dict | None = None,
        historical_baseline_provenance: dict | None = None,
    ):
        self.case = get_case_context()
        self.case_config = self._load_case_config()
        self.output_run_name = str(output_run_name or self.case.run_name)
        self.base_output_dir = Path("output") / self.output_run_name
        self.output_dir = self.base_output_dir / "ensemble"
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.forecast_dir = self.base_output_dir / "forecast"
        self.forecast_dir.mkdir(parents=True, exist_ok=True)
        self.member_mask_dir = self.output_dir / "member_presence"
        self.member_mask_dir.mkdir(parents=True, exist_ok=True)
        self.loading_cache_dir = self.forecast_dir / "forcing_cache"
        self.loading_cache_dir.mkdir(parents=True, exist_ok=True)
        self._wind_scaling_template: xr.Dataset | None = None
        self.sensitivity_context = dict(sensitivity_context or {})
        self.historical_baseline_provenance = dict(historical_baseline_provenance or {})

        self.currents_file = Path(currents_file)
        self.winds_file = Path(winds_file)
        self.wave_file = Path(wave_file) if wave_file else None
        self.alias_probability_cone = True
        audit_paths = get_phase2_loading_audit_paths(run_name=self.output_run_name) if self.case.is_official else {
            "json": self.forecast_dir / "forecast_loading_audit.json",
            "csv": self.forecast_dir / "forecast_loading_audit.csv",
        }
        self.audit_json_path = audit_paths["json"]
        self.audit_csv_path = audit_paths["csv"]
        self.audit_records: list[dict] = []

        self.config_path = Path("config/ensemble.yaml")
        if not self.config_path.exists():
            raise FileNotFoundError(f"Ensemble configuration required at {self.config_path}")

        with open(self.config_path, "r") as f:
            self.config = yaml.safe_load(f) or {}

        self.ensemble_size = int((self.config.get("ensemble") or {}).get("ensemble_size", 50))
        self.official_config = self._load_official_forecast_config()
        official_products = (
            (self.official_config.get("products") or {})
            if self.case.is_official
            else (self.config.get("official_products") or {})
        )
        if self.case.is_official:
            official_products = {
                "snapshot_hours": self.official_config.get("snapshot_hours", [24, 48, 72]),
                "probability_thresholds": self.official_config.get("probability_thresholds", [0.5, 0.9]),
            }
        self.snapshot_hours = [int(hour) for hour in official_products.get("snapshot_hours", [24, 48, 72])]
        self.probability_thresholds = [float(value) for value in official_products.get("probability_thresholds", [0.5, 0.9])]
        self.transport_model_name = str(self.official_config.get("transport_model", "OceanDrift"))
        self.provisional_transport_model = bool(self.official_config.get("provisional_transport_model", self.case.is_official))
        self.require_wave_forcing = bool(self.official_config.get("require_wave_forcing", self.case.is_official))
        self.enable_stokes_drift = bool(self.official_config.get("enable_stokes_drift", self.case.is_official))
        self.official_ensemble_size = int((self.official_config.get("ensemble") or {}).get("ensemble_size", self.ensemble_size))
        self.official_element_count = self._resolve_official_element_count() if self.case.is_official else None
        self.official_polygon_seed_random_seed = self._resolve_official_polygon_seed_random_seed() if self.case.is_official else None
        self.canonical_sea_mask = load_sea_mask_array() if self.case.is_official else None

    def _load_case_config(self) -> dict:
        if not self.case.case_definition_path:
            return {}
        case_path = Path(self.case.case_definition_path)
        if not case_path.exists():
            return {}
        with open(case_path, "r") as f:
            return yaml.safe_load(f) or {}

    def _load_official_forecast_config(self) -> dict:
        if not self.case.is_official:
            return {}
        case_forecast = self.case_config.get("forecast") or {}
        official_cfg = self.config.get("official_forecast") or {}
        merged = dict(official_cfg)
        for key, value in case_forecast.items():
            if isinstance(value, dict) and isinstance(merged.get(key), dict):
                merged[key] = {**merged[key], **value}
            else:
                merged[key] = value
        return merged

    def _resolve_official_element_count(self) -> int:
        override = os.environ.get(OFFICIAL_ELEMENT_COUNT_OVERRIDE_ENV, "").strip()
        if override:
            value = int(override)
            if value <= 0:
                raise ValueError(f"{OFFICIAL_ELEMENT_COUNT_OVERRIDE_ENV} must be > 0.")
            return value

        element_count = self.official_config.get("element_count")
        if element_count is None:
            seeding_cfg = self.official_config.get("seeding") or {}
            element_count = seeding_cfg.get("element_count")
        if element_count is None:
            raise RuntimeError(
                "Official forecast requires forecast.element_count in the case config "
                f"or {OFFICIAL_ELEMENT_COUNT_OVERRIDE_ENV}."
            )
        value = int(element_count)
        if value <= 0:
            raise ValueError("Official forecast element_count must be > 0.")
        return value

    def _resolve_official_polygon_seed_random_seed(self) -> int:
        value = self.official_config.get("polygon_seed_random_seed")
        if value is None:
            seeding_cfg = self.official_config.get("seeding") or {}
            value = seeding_cfg.get("random_seed")
        if value is None:
            return 20230303
        return int(value)

    def _get_official_simulation_window(self) -> tuple[pd.Timestamp, pd.Timestamp, int]:
        start = normalize_model_timestamp(self.case.simulation_start_utc)
        end = normalize_model_timestamp(self.case.simulation_end_utc)
        duration_hours = int(round((end - start).total_seconds() / 3600.0))
        return start, end, duration_hours

    def _build_status_flags(self, selection: RecipeSelection | None = None) -> dict:
        selection_valid = bool(selection.valid) if selection is not None else True
        selection_provisional = bool(selection.provisional) if selection is not None else False
        selection_rerun_required = bool(selection.rerun_required) if selection is not None else False
        provisional = bool(selection_provisional or self.provisional_transport_model)
        rerun_required = bool(selection_rerun_required)
        valid = bool(selection_valid and not provisional and not rerun_required)
        return {
            "valid": valid,
            "provisional": provisional,
            "rerun_required": rerun_required,
        }

    def _init_run_audit(
        self,
        recipe_name: str,
        run_kind: str,
        requested_start_time,
        duration_hours: int,
        member_id: int | None = None,
        perturbation: dict | None = None,
    ) -> dict:
        start_time = normalize_model_timestamp(requested_start_time)
        end_time = start_time + timedelta(hours=duration_hours)
        audit = {
            "run_kind": run_kind,
            "member_id": member_id,
            "recipe": recipe_name,
            "status": "initializing",
            "transport_model": self.transport_model_name,
            "provisional_transport_model": self.provisional_transport_model,
            "requested_start_time_utc": timestamp_to_utc_iso(start_time),
            "requested_end_time_utc": timestamp_to_utc_iso(end_time),
            "requested_duration_hours": int(duration_hours),
            "seed_element_count": 0,
            "first_successful_model_timestep": "",
            "last_successful_model_timestep": "",
            "time_step_minutes": 60,
            "output_path": "",
            "root_cause": "",
            "exception_text": "",
            "exception_chain": [],
            "forcings": {},
            "perturbation": perturbation or {},
            "written_files": [],
        }
        self.audit_records.append(audit)
        return audit

    def _build_model(
        self,
        simulation_start: pd.Timestamp,
        simulation_end: pd.Timestamp,
        audit: dict,
        wind_factor: float = 1.0,
        require_wave: bool | None = None,
        enable_stokes_drift: bool | None = None,
        wave_file: Path | None = None,
    ) -> OceanDrift:
        """Create an OceanDrift model with reader audits and prepared forcing paths."""
        model = OceanDrift(loglevel=50)
        require_wave_forcing = self.require_wave_forcing if require_wave is None else bool(require_wave)
        stokes_drift_enabled = self.enable_stokes_drift if enable_stokes_drift is None else bool(enable_stokes_drift)

        current_entry = self._attach_reader_with_audit(
            model=model,
            file_path=self.currents_file,
            configured_path=self.currents_file,
            reader_kind="current",
            simulation_start=simulation_start,
            simulation_end=simulation_end,
        )
        audit["forcings"]["current"] = current_entry

        scaled_wind_path = self._scale_wind_forcing(self.winds_file, wind_factor)
        wind_entry = self._attach_reader_with_audit(
            model=model,
            file_path=scaled_wind_path,
            configured_path=self.winds_file,
            reader_kind="wind",
            simulation_start=simulation_start,
            simulation_end=simulation_end,
            extra_entry_fields={
                "wind_factor": float(wind_factor),
                "wind_scaling_applied": not np.isclose(float(wind_factor), 1.0),
            },
        )
        audit["forcings"]["wind"] = wind_entry

        if wave_file is None:
            wave_file = self._resolve_wave_file()
        audit["forcings"]["wave"] = self._attach_wave_reader(
            model=model,
            file_path=wave_file,
            simulation_start=simulation_start,
            simulation_end=simulation_end,
            required=require_wave_forcing,
        )

        if self.case.is_official:
            try:
                model.set_config("general:use_auto_landmask", False)
                model.add_reader(reader_global_landmask.Reader())
                audit["shoreline_landmask"] = {
                    "reader": "reader_global_landmask.Reader",
                    "source": "GSHHG-backed OpenDrift global landmask",
                    "status": "loaded",
                }
            except Exception as exc:
                audit["shoreline_landmask"] = {
                    "reader": "reader_global_landmask.Reader",
                    "source": "GSHHG-backed OpenDrift global landmask",
                    "status": "failed",
                    "exception_text": str(exc),
                }
                raise
        else:
            from opendrift.readers import reader_constant

            model.set_config("general:use_auto_landmask", False)
            model.add_reader(reader_constant.Reader({"land_binary_mask": 0}))
            audit["shoreline_landmask"] = {
                "reader": "reader_constant.Reader",
                "source": "prototype constant all-sea mask",
                "status": "loaded",
            }
        model.set_config("drift:stokes_drift", stokes_drift_enabled)
        return model

    def _apply_scoreable_ocean_mask(self, data: np.ndarray) -> np.ndarray:
        working = np.asarray(data, dtype=np.float32)
        if self.canonical_sea_mask is None:
            return working.astype(np.float32)
        return apply_ocean_mask(working, sea_mask=self.canonical_sea_mask, fill_value=0.0).astype(np.float32)

    def _resolve_wave_file(self) -> Path | None:
        if self.wave_file is not None:
            return self.wave_file
        return Path("data/forcing") / self.case.run_name / "cmems_wave.nc"

    def _scale_wind_forcing(self, source_path: Path, wind_factor: float) -> Path:
        if np.isclose(float(wind_factor), 1.0):
            return source_path

        cache_name = f"{source_path.stem}__windfactor__{wind_factor:.3f}.nc"
        cache_path = self.loading_cache_dir / cache_name
        if cache_path.exists():
            try:
                reader = reader_netCDF_CF_generic.Reader(str(cache_path))
                available = set(getattr(reader, "variables", []) or [])
                available.update((getattr(reader, "variable_mapping", {}) or {}).keys())
                if {"x_wind", "y_wind"}.issubset(available) or {"eastward_wind", "northward_wind"}.issubset(available):
                    return cache_path
            except Exception:
                pass
            cache_path.unlink()

        if self._wind_scaling_template is None:
            with xr.open_dataset(source_path) as ds:
                self._wind_scaling_template = ds.load()

        working = self._wind_scaling_template.copy(deep=True)

        u_var, v_var = find_wind_vars(working)
        u_attrs = dict(working[u_var].attrs)
        v_attrs = dict(working[v_var].attrs)
        working[u_var] = working[u_var] * float(wind_factor)
        working[v_var] = working[v_var] * float(wind_factor)
        working[u_var].attrs.update(u_attrs)
        working[v_var].attrs.update(v_attrs)
        working.attrs = dict(working.attrs)
        working.attrs["wind_factor_applied"] = float(wind_factor)
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        working.to_netcdf(cache_path)
        return cache_path

    def _prepare_forcing_entry(
        self,
        file_path: Path,
        configured_path: Path | None,
        reader_kind: str,
        simulation_start: pd.Timestamp,
        simulation_end: pd.Timestamp,
        extra_entry_fields: dict | None = None,
    ) -> dict:
        entry = {
            "kind": reader_kind,
            "configured_path": str(configured_path or file_path),
            "used_path": str(file_path),
            "exists": file_path.exists(),
            "reader_attach_status": "not_attempted",
            "required_variables": [],
            "source_variables": [],
            "mapped_variables": {},
            "available_variables": [],
            "missing_required_variables": [],
            "time_coordinate": "",
            "source_time_start_utc": "",
            "source_time_end_utc": "",
            "used_time_start_utc": "",
            "used_time_end_utc": "",
            "requested_start_time_utc": timestamp_to_utc_iso(simulation_start),
            "requested_end_time_utc": timestamp_to_utc_iso(simulation_end),
            "covers_requested_window": False,
            "coverage_gap_hours": 0.0,
            "inferred_time_step_hours": None,
            "tail_extension_applied": False,
            "tail_extension_reason": "",
            "reader_label": "",
            "reader_variables": [],
            "notes": [],
        }
        if extra_entry_fields:
            entry.update(extra_entry_fields)

        if not entry["exists"]:
            entry["reader_attach_status"] = "missing_file"
            return entry

        required_vars: list[str] = []
        mapped_variables: dict[str, str] = {}
        source_variables: list[str] = []

        with xr.open_dataset(file_path) as ds:
            entry["available_variables"] = sorted(list(ds.data_vars))
            time_coordinate = detect_time_coordinate(ds)
            if time_coordinate:
                entry["time_coordinate"] = time_coordinate
                times = normalize_time_index(ds[time_coordinate].values)
                if len(times) > 0:
                    entry["source_time_start_utc"] = timestamp_to_utc_iso(times.min())
                    entry["source_time_end_utc"] = timestamp_to_utc_iso(times.max())
                    entry["used_time_start_utc"] = entry["source_time_start_utc"]
                    entry["used_time_end_utc"] = entry["source_time_end_utc"]
                    entry["inferred_time_step_hours"] = infer_time_step_hours(times)
                    entry["covers_requested_window"] = bool(times.min() <= simulation_start and times.max() >= simulation_end)
                    if times.max() < simulation_end:
                        gap_hours = (simulation_end - times.max()).total_seconds() / 3600.0
                        entry["coverage_gap_hours"] = float(gap_hours)
                        cadence_hours = entry["inferred_time_step_hours"]
                        if cadence_hours is not None and gap_hours <= cadence_hours + 1e-9:
                            used_path = self._extend_forcing_tail(
                                source_path=file_path,
                                target_end_time=simulation_end,
                                time_coordinate=time_coordinate,
                            )
                            entry["used_path"] = str(used_path)
                            entry["used_time_end_utc"] = timestamp_to_utc_iso(simulation_end)
                            entry["covers_requested_window"] = True
                            entry["tail_extension_applied"] = True
                            entry["tail_extension_reason"] = (
                                f"Extended final {reader_kind} slice with persistence because the "
                                f"requested end was {gap_hours:.2f}h beyond the source end and the source cadence "
                                f"was {cadence_hours:.2f}h."
                            )
                        else:
                            entry["notes"].append(
                                f"Requested end exceeded source coverage by {gap_hours:.2f}h."
                            )
                    elif times.min() > simulation_start:
                        gap_hours = (times.min() - simulation_start).total_seconds() / 3600.0
                        entry["coverage_gap_hours"] = float(gap_hours)
                        entry["notes"].append(
                            f"Requested start preceded source coverage by {gap_hours:.2f}h."
                        )

            if reader_kind == "current":
                required_vars = CURRENT_REQUIRED_VARS
                source_u, source_v = find_current_vars(ds)
                source_variables = [source_u, source_v]
            elif reader_kind == "wind":
                required_vars = WIND_REQUIRED_VARS
                source_u, source_v = find_wind_vars(ds)
                source_variables = [source_u, source_v]
            elif reader_kind == "wave":
                required_vars = WAVE_REQUIRED_VARS
                source_variables = list(find_wave_vars(ds))
            else:
                raise ValueError(f"Unsupported reader kind: {reader_kind}")

        for required, source in zip(required_vars, source_variables):
            if required != source:
                mapped_variables[required] = source

        entry["required_variables"] = required_vars
        entry["source_variables"] = source_variables
        entry["mapped_variables"] = mapped_variables
        return entry

    def _extend_forcing_tail(
        self,
        source_path: Path,
        target_end_time: pd.Timestamp,
        time_coordinate: str,
    ) -> Path:
        target_time = normalize_model_timestamp(target_end_time)
        cache_name = f"{source_path.stem}__tailpersist__{timestamp_to_label(target_time)}.nc"
        cache_path = self.loading_cache_dir / cache_name

        if cache_path.exists():
            with xr.open_dataset(cache_path) as cached:
                cached_times = normalize_time_index(cached[time_coordinate].values)
                if len(cached_times) > 0 and cached_times.max() >= target_time:
                    return cache_path

        with xr.open_dataset(source_path) as ds:
            working = ds.load()

        source_times = normalize_time_index(working[time_coordinate].values)
        if len(source_times) == 0 or source_times.max() >= target_time:
            return source_path

        target_np = np.datetime64(target_time.to_datetime64())
        final_slice = working.isel({time_coordinate: -1}).expand_dims({time_coordinate: [target_np]})
        extended = xr.concat([working, final_slice], dim=time_coordinate)
        extended.attrs = dict(working.attrs)
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        extended.to_netcdf(cache_path)
        return cache_path

    def _attach_reader_with_audit(
        self,
        model: OceanDrift,
        file_path: Path,
        configured_path: Path | None,
        reader_kind: str,
        simulation_start: pd.Timestamp,
        simulation_end: pd.Timestamp,
        extra_entry_fields: dict | None = None,
    ) -> dict:
        entry = self._prepare_forcing_entry(
            file_path=file_path,
            configured_path=configured_path,
            reader_kind=reader_kind,
            simulation_start=simulation_start,
            simulation_end=simulation_end,
            extra_entry_fields=extra_entry_fields,
        )
        if not entry["exists"]:
            raise FileNotFoundError(f"Missing intended {reader_kind} file: {file_path}")
        if not entry["covers_requested_window"]:
            raise RuntimeError(
                f"{reader_kind} forcing does not cover the requested simulation window "
                f"({entry['requested_start_time_utc']} to {entry['requested_end_time_utc']})."
            )

        reader = reader_netCDF_CF_generic.Reader(entry["used_path"])
        if entry["mapped_variables"] and hasattr(reader, "variable_mapping"):
            reader.variable_mapping.update(entry["mapped_variables"])

        available_vars = set(getattr(reader, "variables", []) or [])
        available_vars.update((getattr(reader, "variable_mapping", {}) or {}).keys())
        missing_required = [name for name in entry["required_variables"] if name not in available_vars]
        entry["missing_required_variables"] = missing_required
        if missing_required:
            raise ValueError(
                f"{reader_kind} reader does not expose required variables {missing_required} for {file_path.name}"
            )

        model.add_reader(reader)
        entry["reader_attach_status"] = "loaded"
        entry["reader_label"] = f"{reader.__class__.__name__}<{Path(entry['used_path']).name}>"
        entry["reader_variables"] = sorted(list(available_vars))
        return entry

    def _attach_wave_reader(
        self,
        model: OceanDrift,
        file_path: Path | None,
        simulation_start: pd.Timestamp,
        simulation_end: pd.Timestamp,
        required: bool,
    ) -> dict:
        if not file_path:
            entry = {
                "kind": "wave",
                "configured_path": "",
                "used_path": "",
                "exists": False,
                "reader_attach_status": "not_configured",
                "required_variables": list(WAVE_REQUIRED_VARS),
                "source_variables": [],
                "mapped_variables": {},
                "available_variables": [],
                "missing_required_variables": list(WAVE_REQUIRED_VARS),
                "time_coordinate": "",
                "source_time_start_utc": "",
                "source_time_end_utc": "",
                "used_time_start_utc": "",
                "used_time_end_utc": "",
                "requested_start_time_utc": timestamp_to_utc_iso(simulation_start),
                "requested_end_time_utc": timestamp_to_utc_iso(simulation_end),
                "covers_requested_window": False,
                "coverage_gap_hours": 0.0,
                "inferred_time_step_hours": None,
                "tail_extension_applied": False,
                "tail_extension_reason": "",
                "reader_label": "",
                "reader_variables": [],
                "notes": [],
            }
            if required:
                raise FileNotFoundError("Official forecast requires explicit wave/Stokes forcing, but no wave file was configured.")
            return entry

        entry = self._prepare_forcing_entry(
            file_path=file_path,
            configured_path=file_path,
            reader_kind="wave",
            simulation_start=simulation_start,
            simulation_end=simulation_end,
        )
        if not entry["exists"]:
            entry["reader_attach_status"] = "missing_file"
            if required:
                raise FileNotFoundError(f"Official forecast requires explicit wave/Stokes forcing, but {file_path} is missing.")
            return entry

        try:
            reader = reader_netCDF_CF_generic.Reader(entry["used_path"])
            available_vars = set(getattr(reader, "variables", []) or [])
            available_vars.update((getattr(reader, "variable_mapping", {}) or {}).keys())
            entry["reader_variables"] = sorted(list(available_vars))
            entry["missing_required_variables"] = [
                name for name in entry["required_variables"] if name not in available_vars
            ]
            if entry["missing_required_variables"]:
                raise ValueError(
                    f"wave reader does not expose required variables {entry['missing_required_variables']} "
                    f"for {file_path.name}"
                )
            model.add_reader(reader)
            entry["reader_attach_status"] = "loaded"
            entry["reader_label"] = f"{reader.__class__.__name__}<{Path(entry['used_path']).name}>"
            return entry
        except Exception as exc:
            entry["reader_attach_status"] = f"attach_failed: {exc}"
            entry["notes"].append(str(exc))
            if required:
                raise RuntimeError(
                    "Official forecast requires explicit wave/Stokes forcing and the wave reader could not be attached."
                ) from exc
            return entry

    @staticmethod
    def _seed_polygon_release(
        model: OceanDrift,
        start_time,
        num_elements: int,
        random_seed: int | None = None,
    ):
        """Seed particles across the configured initialization polygon."""
        from src.utils.io import resolve_polygon_seeding

        lons, lats, _ = resolve_polygon_seeding(num_elements, random_seed=random_seed)
        model.seed_elements(
            lon=lons,
            lat=lats,
            number=num_elements,
            time=normalize_model_timestamp(start_time).to_pydatetime(),
        )

    def _record_output_timestep_coverage(self, output_file: Path, audit: dict):
        if not output_file.exists():
            return

        with xr.open_dataset(output_file) as ds:
            if "time" not in ds.coords:
                return
            times = normalize_time_index(ds["time"].values)
            if len(times) == 0:
                return
            audit["first_successful_model_timestep"] = timestamp_to_utc_iso(times.min())
            audit["last_successful_model_timestep"] = timestamp_to_utc_iso(times.max())

    def _run_model(
        self,
        model: OceanDrift,
        output_file: Path,
        duration_hours: int,
        audit: dict,
    ) -> Path:
        output_file.parent.mkdir(parents=True, exist_ok=True)
        audit["output_path"] = str(output_file)

        if output_file.exists():
            output_file.unlink()

        try:
            with intercept_sys_exit():
                model.run(
                    duration=timedelta(hours=duration_hours),
                    time_step=timedelta(minutes=int(audit["time_step_minutes"])),
                    outfile=str(output_file),
                )
        except Exception as exc:
            audit["status"] = "failed"
            audit["root_cause"] = _root_cause_message(exc)
            audit["exception_text"] = str(exc)
            audit["exception_chain"] = _stringify_exception_chain(exc)
            self._record_output_timestep_coverage(output_file, audit)
            self._write_loading_audit_artifacts()
            if output_file.exists():
                output_file.unlink()
            raise RuntimeError(
                f"{audit['run_kind']} failed: {audit['root_cause']}. "
                f"See {self.audit_json_path} for forcing audit details."
            ) from exc

        audit["status"] = "completed"
        self._record_output_timestep_coverage(output_file, audit)
        self._write_loading_audit_artifacts()
        return output_file

    @staticmethod
    def _load_opendrift_snapshot(
        nc_path: Path,
        target_time,
    ) -> tuple[np.ndarray, np.ndarray, np.ndarray, pd.Timestamp]:
        with xr.open_dataset(nc_path) as ds:
            if "time" not in ds.coords:
                raise ValueError(f"{nc_path} is missing a time coordinate.")
            times = normalize_time_index(ds["time"].values)
            target = normalize_model_timestamp(target_time)
            time_index = int(np.abs(times - target).argmin())
            actual_time = normalize_model_timestamp(times[time_index])

            lon = np.asarray(ds["lon"].isel(time=time_index).values).reshape(-1)
            lat = np.asarray(ds["lat"].isel(time=time_index).values).reshape(-1)
            status = np.asarray(ds["status"].isel(time=time_index).values).reshape(-1)
            if "mass_oil" in ds:
                mass = np.asarray(ds["mass_oil"].isel(time=time_index).values).reshape(-1)
            else:
                mass = np.ones_like(lon, dtype=np.float32)

        valid = ~np.isnan(lon) & ~np.isnan(lat) & (status == 0)
        return lon[valid], lat[valid], mass[valid], actual_time

    @staticmethod
    def _build_date_composite_mask(
        nc_path: Path,
        target_date: str,
        grid: GridBuilder,
    ) -> np.ndarray:
        date_value = pd.Timestamp(target_date).date()
        composite = np.zeros((grid.height, grid.width), dtype=np.float32)

        with xr.open_dataset(nc_path) as ds:
            if "time" not in ds.coords:
                raise ValueError(f"{nc_path} is missing a time coordinate.")
            times = normalize_time_index(ds["time"].values)
            matching_indices = [index for index, value in enumerate(times) if value.date() == date_value]
            if not matching_indices:
                return composite

            for time_index in matching_indices:
                lon = np.asarray(ds["lon"].isel(time=time_index).values).reshape(-1)
                lat = np.asarray(ds["lat"].isel(time=time_index).values).reshape(-1)
                status = np.asarray(ds["status"].isel(time=time_index).values).reshape(-1)
                valid = ~np.isnan(lon) & ~np.isnan(lat) & (status == 0)
                if not np.any(valid):
                    continue
                hits, _ = rasterize_particles(
                    grid,
                    lon[valid],
                    lat[valid],
                    np.ones(np.count_nonzero(valid), dtype=np.float32),
                )
                composite = np.maximum(composite, hits)

        return composite.astype(np.float32)

    def _write_probability_snapshot_artifacts(
        self,
        probability_data: np.ndarray,
        target_time: pd.Timestamp,
        grid: GridBuilder,
    ) -> tuple[list[Path], list[dict]]:
        label = timestamp_to_label(target_time)
        written_files: list[Path] = []
        records: list[dict] = []

        probability_data = self._apply_scoreable_ocean_mask(probability_data)
        prob_presence_path = get_official_prob_presence_path(target_time, run_name=self.output_run_name)
        save_raster(grid, probability_data.astype(np.float32), prob_presence_path)
        written_files.append(prob_presence_path)
        records.append(
            {
                "product_type": "prob_presence",
                "timestamp_utc": timestamp_to_utc_iso(target_time),
                "relative_path": _relative_output_path(prob_presence_path, self.base_output_dir),
                "semantics": "Per-cell ensemble probability of member presence at the product timestamp.",
            }
        )

        for threshold in self.probability_thresholds:
            threshold_label = _threshold_label(threshold)
            mask_data = self._apply_scoreable_ocean_mask((probability_data >= threshold).astype(np.float32))
            mask_path = get_official_mask_threshold_path(
                threshold_label,
                target_time,
                run_name=self.output_run_name,
            )
            save_raster(grid, mask_data, mask_path)
            written_files.append(mask_path)
            records.append(
                {
                    "product_type": f"mask_{threshold_label}",
                    "timestamp_utc": timestamp_to_utc_iso(target_time),
                    "relative_path": _relative_output_path(mask_path, self.base_output_dir),
                    "semantics": f"Binary ensemble mask where probability of presence is at least {threshold:.2f}.",
                }
            )

        return written_files, records

    def _write_legacy_probability_products(
        self,
        probability_data: np.ndarray,
        target_time: pd.Timestamp,
        nominal_start_time: pd.Timestamp,
        all_lons: list[float],
        all_lats: list[float],
        start_lat: float,
        start_lon: float,
        grid: GridBuilder,
    ) -> list[Path]:
        written_paths: list[Path] = []
        hours_since_start = int(round((normalize_model_timestamp(target_time) - nominal_start_time).total_seconds() / 3600.0))
        nc_out = self.output_dir / f"probability_{hours_since_start}h.nc"
        dims = ["time", grid.y_name, grid.x_name]
        coords = {
            "time": [hours_since_start],
            grid.y_name: grid.y_centers,
            grid.x_name: grid.x_centers,
        }
        attrs = {
            "description": f"Probability field at T+{hours_since_start}h",
            "units": "decimal_fraction",
            "crs": grid.crs,
            "resolution": grid.resolution,
            "grid_id": grid_id_from_builder(grid),
            "timestamp_utc": timestamp_to_utc_iso(target_time),
        }
        xr.Dataset(
            data_vars={"probability": (dims, probability_data[np.newaxis, :, :])},
            coords=coords,
            attrs=attrs,
        ).to_netcdf(nc_out)
        written_paths.append(nc_out)

        tif_out = get_ensemble_probability_score_raster_path(hours_since_start, run_name=self.output_run_name)
        save_raster(grid, probability_data.astype(np.float32), tif_out)
        written_paths.append(tif_out)

        img_out = self.output_dir / f"probability_{hours_since_start}h.png"
        plot_corners = grid.display_bounds_wgs84 or [grid.min_lon, grid.max_lon, grid.min_lat, grid.max_lat]
        plot_probability_map(
            output_file=str(img_out),
            all_lons=np.asarray(all_lons),
            all_lats=np.asarray(all_lats),
            start_lon=start_lon,
            start_lat=start_lat,
            corners=plot_corners,
            title=f"Ensemble Forecast: T+{hours_since_start}h\nProbability Distribution (N={self.ensemble_size})",
        )
        written_paths.append(img_out)

        if self.alias_probability_cone and hours_since_start == 72 and img_out.exists():
            alias_path = self.output_dir / "probability_cone.png"
            shutil.copyfile(img_out, alias_path)
            written_paths.append(alias_path)

        return written_paths

    def _generate_deterministic_control_products(
        self,
        nc_path: Path,
        recipe_name: str,
        nominal_start_time,
    ) -> tuple[list[Path], list[dict]]:
        grid = GridBuilder()
        written_files: list[Path] = []
        product_records: list[dict] = []
        start_time = normalize_model_timestamp(nominal_start_time)

        for hour in self.snapshot_hours:
            target_time = start_time + timedelta(hours=int(hour))
            lon, lat, mass, actual_time = self._load_opendrift_snapshot(nc_path, target_time)
            hits, probs = rasterize_particles(grid, lon, lat, mass)
            hits = self._apply_scoreable_ocean_mask(hits)
            probs = self._apply_scoreable_ocean_mask(probs)

            footprint_path = get_official_control_footprint_mask_path(
                target_time,
                run_name=self.output_run_name,
            )
            density_path = get_official_control_density_norm_path(
                target_time,
                run_name=self.output_run_name,
            )
            save_raster(grid, hits, footprint_path)
            save_raster(grid, probs, density_path)
            written_files.extend([footprint_path, density_path])

            product_records.extend(
                [
                    {
                        "product_type": "control_footprint_mask",
                        "timestamp_utc": timestamp_to_utc_iso(target_time),
                        "actual_snapshot_time_utc": timestamp_to_utc_iso(actual_time),
                        "relative_path": _relative_output_path(footprint_path, self.base_output_dir),
                        "semantics": "Binary deterministic control footprint mask on the canonical scoring grid.",
                    },
                    {
                        "product_type": "control_density_norm",
                        "timestamp_utc": timestamp_to_utc_iso(target_time),
                        "actual_snapshot_time_utc": timestamp_to_utc_iso(actual_time),
                        "relative_path": _relative_output_path(density_path, self.base_output_dir),
                        "semantics": "Normalized deterministic control particle density on the canonical scoring grid.",
                    },
                ]
            )

        return written_files, product_records

    def _generate_official_ensemble_products(
        self,
        member_runs: list[dict],
        nominal_start_time,
        start_lat: float,
        start_lon: float,
    ) -> tuple[list[Path], list[dict]]:
        grid = GridBuilder()
        written_files: list[Path] = []
        product_records: list[dict] = []
        nominal_start = normalize_model_timestamp(nominal_start_time)

        for hour in self.snapshot_hours:
            target_time = nominal_start + timedelta(hours=int(hour))
            member_masks: list[np.ndarray] = []
            member_density_rasters: list[np.ndarray] = []
            all_lons: list[float] = []
            all_lats: list[float] = []

            for member in member_runs:
                lon, lat, mass, actual_time = self._load_opendrift_snapshot(member["output_file"], target_time)
                hits, density = rasterize_particles(
                    grid,
                    lon,
                    lat,
                    mass if len(mass) else np.ones(len(lon), dtype=np.float32),
                )
                hits = self._apply_scoreable_ocean_mask(hits)
                density = self._apply_scoreable_ocean_mask(density)
                member_masks.append(hits)
                member_density_rasters.append(density)
                all_lons.extend(lon.tolist())
                all_lats.extend(lat.tolist())

                member_mask_path = self.member_mask_dir / (
                    f"member_presence_mask_{member['member_id']:02d}_{timestamp_to_label(target_time)}.tif"
                )
                save_raster(grid, hits, member_mask_path)
                written_files.append(member_mask_path)
                product_records.append(
                    {
                        "product_type": "member_presence_mask",
                        "member_id": member["member_id"],
                        "timestamp_utc": timestamp_to_utc_iso(target_time),
                        "actual_snapshot_time_utc": timestamp_to_utc_iso(actual_time),
                        "relative_path": _relative_output_path(member_mask_path, self.base_output_dir),
                        "semantics": "Binary per-member presence mask on the canonical scoring grid.",
                    }
                )

            probability = np.mean(np.stack(member_masks, axis=0), axis=0).astype(np.float32)
            density_mean = np.mean(np.stack(member_density_rasters, axis=0), axis=0).astype(np.float32)
            density_total = float(density_mean.sum())
            if density_total > 0:
                density_mean = (density_mean / density_total).astype(np.float32)
            density_mean = self._apply_scoreable_ocean_mask(density_mean)
            probability_files, probability_records = self._write_probability_snapshot_artifacts(
                probability_data=probability,
                target_time=target_time,
                grid=grid,
            )
            written_files.extend(probability_files)
            product_records.extend(probability_records)

            ensemble_density_path = self.output_dir / f"ensemble_density_norm_{timestamp_to_label(target_time)}.tif"
            save_raster(grid, density_mean, ensemble_density_path)
            written_files.append(ensemble_density_path)
            product_records.append(
                {
                    "product_type": "ensemble_density_norm",
                    "timestamp_utc": timestamp_to_utc_iso(target_time),
                    "relative_path": _relative_output_path(ensemble_density_path, self.base_output_dir),
                    "semantics": "Mean ensemble normalized density across members on the canonical scoring grid.",
                }
            )

        validation_date = ""
        if self.case.validation_layer.event_time_utc:
            validation_date = str(pd.Timestamp(self.case.validation_layer.event_time_utc).date())

        if validation_date:
            composite_masks: list[np.ndarray] = []
            for member in member_runs:
                composite = self._build_date_composite_mask(
                    member["output_file"],
                    target_date=validation_date,
                    grid=grid,
                )
                composite = self._apply_scoreable_ocean_mask(composite)
                composite_masks.append(composite)
                member_composite_path = self.member_mask_dir / (
                    f"member_presence_mask_{member['member_id']:02d}_{validation_date}_datecomposite.tif"
                )
                save_raster(grid, composite, member_composite_path)
                written_files.append(member_composite_path)
                product_records.append(
                    {
                        "product_type": "member_presence_mask_datecomposite",
                        "member_id": member["member_id"],
                        "date_utc": validation_date,
                        "relative_path": _relative_output_path(member_composite_path, self.base_output_dir),
                        "semantics": "Per-member binary presence union across all forecast timesteps on the target UTC date.",
                    }
                )

            composite_probability = np.mean(np.stack(composite_masks, axis=0), axis=0).astype(np.float32)
            composite_probability = self._apply_scoreable_ocean_mask(composite_probability)
            composite_prob_path = self.output_dir / f"prob_presence_{validation_date}_datecomposite.tif"
            composite_p50_path = get_official_mask_p50_datecomposite_path(run_name=self.output_run_name)
            save_raster(grid, composite_probability, composite_prob_path)
            save_raster(
                grid,
                self._apply_scoreable_ocean_mask((composite_probability >= 0.5).astype(np.float32)),
                composite_p50_path,
            )
            written_files.extend([composite_prob_path, composite_p50_path])
            product_records.extend(
                [
                    {
                        "product_type": "prob_presence_datecomposite",
                        "date_utc": validation_date,
                        "relative_path": _relative_output_path(composite_prob_path, self.base_output_dir),
                        "semantics": "Per-cell ensemble probability of any member presence across the target UTC date.",
                    },
                    {
                        "product_type": "mask_p50_datecomposite",
                        "date_utc": validation_date,
                        "relative_path": _relative_output_path(composite_p50_path, self.base_output_dir),
                        "semantics": "Binary date-composite mask where probability of presence is at least 0.50.",
                    },
                ]
            )

        return written_files, product_records

    def _generate_prototype_probability_products(self, file_list, start_lat, start_lon):
        """
        Generate gridded NetCDF probability fields and PNG snapshots for 24h, 48h, and 72h.
        """
        snapshots = self.snapshot_hours
        grid = GridBuilder()
        written_files: list[Path] = []

        metadata = {
            "ensemble_size": self.ensemble_size,
            "grid": grid.spec.to_metadata(),
            "snapshots_hours": snapshots,
            "variables": ["probability"],
        }

        metadata_path = self.output_dir / "metadata.json"
        with open(metadata_path, "w") as f:
            json.dump(metadata, f, indent=4)
        written_files.append(metadata_path)

        for hr in snapshots:
            logger.info("   Processing T+%sh snapshot...", hr)
            all_lons = []
            all_lats = []

            for file_path in file_list:
                try:
                    with xr.open_dataset(file_path) as ds:
                        times = normalize_time_index(ds.time.values)
                        target_time = normalize_model_timestamp(times[0] + timedelta(hours=hr))
                        idx = int(np.abs(times - target_time).argmin())

                        lons = np.asarray(ds["lon"].isel(time=idx).values).reshape(-1)
                        lats = np.asarray(ds["lat"].isel(time=idx).values).reshape(-1)
                        valid = ~np.isnan(lons) & ~np.isnan(lats)
                        all_lons.extend(lons[valid])
                        all_lats.extend(lats[valid])
                except Exception as exc:
                    logger.warning("Could not process %s for %sh: %s", Path(file_path).name, hr, exc)

            if not all_lons:
                raise RuntimeError(
                    f"Ensemble probability snapshot T+{hr}h could not be generated because "
                    "no valid particle positions were found."
                )

            x_vals, y_vals = project_points_to_grid(grid, np.asarray(all_lons), np.asarray(all_lats))
            hist, _, _ = np.histogram2d(y_vals, x_vals, bins=[grid.y_bins, grid.x_bins])
            hist = np.flipud(hist)
            prob_density = (hist / len(all_lons)).astype(np.float32)

            dims = ["time", grid.y_name, grid.x_name]
            coords = {
                "time": [hr],
                grid.y_name: grid.y_centers,
                grid.x_name: grid.x_centers,
            }
            attrs = {
                "description": f"Probability field at T+{hr}h",
                "units": "decimal_fraction",
                "crs": grid.crs,
                "resolution": grid.resolution,
                "grid_id": grid_id_from_builder(grid),
            }

            ds_prob = xr.Dataset(
                data_vars={"probability": (dims, prob_density[np.newaxis, :, :])},
                coords=coords,
                attrs=attrs,
            )

            nc_out = self.output_dir / f"probability_{hr}h.nc"
            ds_prob.to_netcdf(nc_out)
            written_files.append(nc_out)

            tif_out = get_ensemble_probability_score_raster_path(hr, run_name=self.output_run_name)
            save_raster(grid, prob_density, tif_out)
            written_files.append(tif_out)

            img_out = self.output_dir / f"probability_{hr}h.png"
            plot_corners = grid.display_bounds_wgs84 or [grid.min_lon, grid.max_lon, grid.min_lat, grid.max_lat]
            plot_probability_map(
                output_file=str(img_out),
                all_lons=np.array(all_lons),
                all_lats=np.array(all_lats),
                start_lon=start_lon,
                start_lat=start_lat,
                corners=plot_corners,
                title=f"Ensemble Forecast: T+{hr}h\nProbability Distribution (N={self.ensemble_size})",
            )
            written_files.append(img_out)

            if self.alias_probability_cone and hr == 72 and img_out.exists():
                alias_path = self.output_dir / "probability_cone.png"
                shutil.copyfile(img_out, alias_path)
                written_files.append(alias_path)

        logger.info("All Phase 2 probability products saved to %s", self.output_dir)
        return written_files

    def _write_loading_audit_artifacts(self):
        audit_payload = {"runs": self.audit_records}
        _write_json_atomic(self.audit_json_path, audit_payload)

        rows: list[dict] = []
        for audit in self.audit_records:
            base_row = {
                "run_kind": audit["run_kind"],
                "member_id": audit["member_id"] if audit["member_id"] is not None else "",
                "recipe": audit["recipe"],
                "status": audit["status"],
                "transport_model": audit["transport_model"],
                "provisional_transport_model": audit["provisional_transport_model"],
                "requested_start_time_utc": audit["requested_start_time_utc"],
                "requested_end_time_utc": audit["requested_end_time_utc"],
                "requested_duration_hours": audit["requested_duration_hours"],
                "seed_element_count": audit["seed_element_count"],
                "first_successful_model_timestep": audit["first_successful_model_timestep"],
                "last_successful_model_timestep": audit["last_successful_model_timestep"],
                "output_path": audit["output_path"],
                "root_cause": audit["root_cause"],
                "exception_text": audit["exception_text"],
                "exception_chain": " | ".join(audit["exception_chain"]),
            }
            if not audit["forcings"]:
                rows.append(base_row)
                continue

            for forcing_kind, forcing in audit["forcings"].items():
                rows.append(
                    {
                        **base_row,
                        "forcing_kind": forcing_kind,
                        "configured_path": forcing.get("configured_path", ""),
                        "used_path": forcing.get("used_path", ""),
                        "exists": forcing.get("exists", False),
                        "reader_attach_status": forcing.get("reader_attach_status", ""),
                        "required_variables": ";".join(forcing.get("required_variables", [])),
                        "source_variables": ";".join(forcing.get("source_variables", [])),
                        "missing_required_variables": ";".join(forcing.get("missing_required_variables", [])),
                        "mapped_variables": json.dumps(forcing.get("mapped_variables", {}), sort_keys=True),
                        "available_variables": ";".join(forcing.get("available_variables", [])),
                        "reader_variables": ";".join(forcing.get("reader_variables", [])),
                        "time_coordinate": forcing.get("time_coordinate", ""),
                        "source_time_start_utc": forcing.get("source_time_start_utc", ""),
                        "source_time_end_utc": forcing.get("source_time_end_utc", ""),
                        "used_time_start_utc": forcing.get("used_time_start_utc", ""),
                        "used_time_end_utc": forcing.get("used_time_end_utc", ""),
                        "covers_requested_window": forcing.get("covers_requested_window", False),
                        "coverage_gap_hours": forcing.get("coverage_gap_hours", 0.0),
                        "inferred_time_step_hours": forcing.get("inferred_time_step_hours", ""),
                        "tail_extension_applied": forcing.get("tail_extension_applied", False),
                        "tail_extension_reason": forcing.get("tail_extension_reason", ""),
                        "wind_factor": forcing.get("wind_factor", ""),
                        "wind_scaling_applied": forcing.get("wind_scaling_applied", ""),
                        "reader_label": forcing.get("reader_label", ""),
                        "notes": " | ".join(forcing.get("notes", [])),
                    }
                )

        _write_text_atomic(self.audit_csv_path, pd.DataFrame(rows).to_csv(index=False))

    def _build_ensemble_manifest_payload(
        self,
        recipe_name: str,
        start_time,
        member_runs: list[dict],
        product_records: list[dict],
        selection: RecipeSelection | None = None,
    ) -> dict:
        grid = GridBuilder()
        simulation_start, simulation_end, _ = self._get_official_simulation_window()
        status_flags = self._build_status_flags(selection)
        ensemble_cfg = self.official_config.get("ensemble") or {}
        return {
            "manifest_type": "official_phase2_ensemble",
            "workflow_mode": self.case.workflow_mode,
            "case_id": self.case.case_id,
            "run_name": self.output_run_name,
            "simulation_window_utc": {
                "start": timestamp_to_utc_iso(simulation_start),
                "end": timestamp_to_utc_iso(simulation_end),
            },
            "grid": {
                "grid_id": grid_id_from_builder(grid),
                **grid.spec.to_metadata(),
            },
            "transport": {
                "model": self.transport_model_name,
                "provisional_transport_model": self.provisional_transport_model,
                "wave_forcing_required": self.require_wave_forcing,
                "stokes_drift_enabled": self.enable_stokes_drift,
            },
            "recipe_provenance": {
                "recipe": recipe_name,
                "nominal_start_time_utc": timestamp_to_utc_iso(start_time),
                "baseline_recipe": (
                    (self.historical_baseline_provenance or {}).get("recipe")
                    or recipe_name
                ),
            },
            "baseline_provenance": {
                "recipe": selection.recipe if selection is not None else recipe_name,
                "source_kind": selection.source_kind if selection is not None else "direct_recipe_argument",
                "source_path": selection.source_path if selection is not None else "",
                "note": selection.note if selection is not None else "",
                "status_flag": selection.status_flag if selection is not None else "provisional",
                "valid": selection.valid if selection is not None else False,
                "provisional": selection.provisional if selection is not None else True,
                "rerun_required": selection.rerun_required if selection is not None else False,
            },
            "status_flags": status_flags,
            "loading_audit": {
                "json": str(self.audit_json_path),
                "csv": str(self.audit_csv_path),
            },
            "historical_baseline_provenance": self.historical_baseline_provenance,
            "sensitivity_context": self.sensitivity_context,
            "ensemble_configuration": {
                "ensemble_size": len(member_runs),
                "element_count": self.official_element_count,
                "polygon_seed_random_seed": self.official_polygon_seed_random_seed,
                "wind_factor_min": float(ensemble_cfg.get("wind_factor_min", 0.8)),
                "wind_factor_max": float(ensemble_cfg.get("wind_factor_max", 1.2)),
                "start_time_offset_hours": [int(v) for v in ensemble_cfg.get("start_time_offset_hours", [-3, -2, -1, 0, 1, 2, 3])],
                "horizontal_diffusivity_m2s_min": float(ensemble_cfg.get("horizontal_diffusivity_m2s_min", 1.0)),
                "horizontal_diffusivity_m2s_max": float(ensemble_cfg.get("horizontal_diffusivity_m2s_max", 10.0)),
            },
            "source_geometry": {
                "initialization_mode": self.case.initialization_mode,
                "initialization_polygon": str(self.case.initialization_layer.processed_vector_path(self.case.run_name))
                if self.case.is_official
                else str(self.case.initialization_layer.geojson_path(self.case.run_name)),
                "validation_polygon": str(self.case.validation_layer.processed_vector_path(self.case.run_name))
                if self.case.is_official
                else str(self.case.validation_layer.geojson_path(self.case.run_name)),
                "source_point": str(self.case.provenance_layer.processed_vector_path(self.case.run_name))
                if self.case.is_official
                else str(self.case.provenance_layer.geojson_path(self.case.run_name)),
            },
            "member_runs": [
                {
                    "member_id": member["member_id"],
                    "relative_path": _relative_output_path(member["output_file"], self.base_output_dir),
                    "start_time_utc": member["start_time_utc"],
                    "end_time_utc": member["end_time_utc"],
                    "element_count": member["element_count"],
                    "perturbation": member["perturbation"],
                }
                for member in member_runs
            ],
            "products": product_records,
        }

    def write_output_manifest(
        self,
        recipe_name: str,
        member_runs: list[dict],
        written_files: list[Path],
        product_records: list[dict],
        start_time,
        selection: RecipeSelection | None = None,
    ) -> dict:
        """Write the Phase 2 ensemble manifest."""
        manifest_path = get_ensemble_manifest_path(run_name=self.output_run_name)
        if self.case.is_official:
            payload = self._build_ensemble_manifest_payload(
                recipe_name=recipe_name,
                start_time=start_time,
                member_runs=member_runs,
                product_records=product_records,
                selection=selection,
            )
        else:
            payload = {
                "written_files": [
                    {
                        "file_name": path.name,
                        "relative_path": _relative_output_path(path, self.base_output_dir),
                    }
                    for path in written_files
                    if path.exists()
                ]
            }

        _write_json_atomic(manifest_path, payload)

        logger.info("Wrote ensemble manifest to %s", manifest_path)
        return {
            "manifest": str(manifest_path),
            "written_files": [str(path) for path in written_files if path.exists()] + [str(manifest_path)],
        }

    def run_deterministic_control(
        self,
        recipe_name: str,
        start_time: str,
        duration_hours: int = 72,
        selection: RecipeSelection | None = None,
    ) -> dict:
        """Run a single deterministic spill forecast for official Phase 3B scoring."""
        logger.info("Starting deterministic control forecast for recipe %s", recipe_name)
        if self.case.is_official:
            simulation_start, simulation_end, duration_hours = self._get_official_simulation_window()
            deterministic_cfg = self.official_config.get("deterministic") or {}
            wind_factor = float(deterministic_cfg.get("wind_factor", 1.0))
            start_offset_hours = int(deterministic_cfg.get("start_time_offset_hours", 0))
            horizontal_diffusivity = float(deterministic_cfg.get("horizontal_diffusivity_m2s", 2.0))
            simulation_start = simulation_start + timedelta(hours=start_offset_hours)
            simulation_end = simulation_end + timedelta(hours=start_offset_hours)
            seed_element_count = int(self.official_element_count)
            seed_random_seed = int(self.official_polygon_seed_random_seed)
        else:
            simulation_start = normalize_model_timestamp(start_time)
            simulation_end = simulation_start + timedelta(hours=duration_hours)
            wind_factor = 1.0
            horizontal_diffusivity = 0.0
            seed_element_count = 2000
            seed_random_seed = None

        audit = self._init_run_audit(
            recipe_name=recipe_name,
            run_kind="deterministic_control",
            requested_start_time=simulation_start,
            duration_hours=duration_hours,
            perturbation={
                "wind_factor": wind_factor,
                "start_time_offset_hours": 0 if not self.case.is_official else int((simulation_start - normalize_model_timestamp(self.case.simulation_start_utc)).total_seconds() / 3600.0),
                "horizontal_diffusivity_m2s": horizontal_diffusivity,
                "random_seed": seed_random_seed if seed_random_seed is not None else "",
            },
        )

        model = self._build_model(
            simulation_start=simulation_start,
            simulation_end=simulation_end,
            audit=audit,
            wind_factor=wind_factor,
            require_wave=self.case.is_official,
            enable_stokes_drift=self.enable_stokes_drift if self.case.is_official else False,
        )
        model.set_config("drift:horizontal_diffusivity", horizontal_diffusivity)
        model.set_config("drift:wind_uncertainty", 0.0)
        model.set_config("drift:current_uncertainty", 0.0)
        self._seed_polygon_release(
            model,
            simulation_start,
            num_elements=seed_element_count,
            random_seed=seed_random_seed,
        )
        audit["seed_element_count"] = seed_element_count

        output_file = get_deterministic_control_output_path(recipe_name, run_name=self.output_run_name)
        control_nc = self._run_model(
            model=model,
            output_file=output_file,
            duration_hours=duration_hours,
            audit=audit,
        )
        written_files, product_records = self._generate_deterministic_control_products(
            nc_path=control_nc,
            recipe_name=recipe_name,
            nominal_start_time=simulation_start,
        )
        audit["written_files"] = [str(path) for path in written_files]
        self._write_loading_audit_artifacts()
        logger.info("Deterministic control forecast saved to %s", control_nc)
        return {
            "output_file": control_nc,
            "written_files": [control_nc, *written_files],
            "element_count": seed_element_count,
            "status_flags": self._build_status_flags(selection),
            "configuration": {
                "wind_factor": wind_factor,
                "horizontal_diffusivity_m2s": horizontal_diffusivity,
                "start_time_utc": timestamp_to_utc_iso(simulation_start),
                "end_time_utc": timestamp_to_utc_iso(simulation_end),
                "random_seed": seed_random_seed,
                "provisional_transport_model": self.provisional_transport_model,
            },
            "products": product_records,
        }

    def run_ensemble(
        self,
        recipe_name: str,
        start_lat: float,
        start_lon: float,
        start_time: str,
        duration_hours: int = 72,
        selection: RecipeSelection | None = None,
    ):
        """
        Runs the ensemble and writes official or prototype products.
        """
        active_ensemble_size = self.official_ensemble_size if self.case.is_official else self.ensemble_size
        logger.info("Starting Phase 2: Ensemble Forecast (%s members)...", active_ensemble_size)
        logger.info("Spill Location: %s, %s", start_lat, start_lon)
        logger.info("Nominal Start Time: %s", start_time)
        logger.info("Currents: %s", self.currents_file)
        logger.info("Winds: %s", self.winds_file)

        ensemble_files: list[Path] = []
        member_runs: list[dict] = []
        if self.case.is_official:
            base_time, _, duration_hours = self._get_official_simulation_window()
            ensemble_cfg = self.official_config.get("ensemble") or {}
            wind_factor_min = float(ensemble_cfg.get("wind_factor_min", 0.8))
            wind_factor_max = float(ensemble_cfg.get("wind_factor_max", 1.2))
            offset_choices = [int(value) for value in ensemble_cfg.get("start_time_offset_hours", [-3, -2, -1, 0, 1, 2, 3])]
            diffusivity_min = float(ensemble_cfg.get("horizontal_diffusivity_m2s_min", 1.0))
            diffusivity_max = float(ensemble_cfg.get("horizontal_diffusivity_m2s_max", 10.0))
            base_seed = int(self.official_polygon_seed_random_seed)
            rng = np.random.default_rng(base_seed)

            for i in range(active_ensemble_size):
                member_id = i + 1
                member_seed = int(rng.integers(0, np.iinfo(np.int32).max))
                member_rng = np.random.default_rng(member_seed)
                time_offset_hours = int(member_rng.choice(offset_choices))
                run_start_time = base_time + timedelta(hours=time_offset_hours)
                run_end_time = run_start_time + timedelta(hours=duration_hours)
                diffusivity = float(np.exp(member_rng.uniform(np.log(diffusivity_min), np.log(diffusivity_max))))
                wind_factor = float(member_rng.uniform(wind_factor_min, wind_factor_max))

                print(
                    f"   Member {member_id}/{active_ensemble_size}: "
                    f"T{time_offset_hours:+d}h | K={diffusivity:.3f} | W_fac={wind_factor:.3f} | seed={member_seed}"
                )

                audit = self._init_run_audit(
                    recipe_name=recipe_name,
                    run_kind="ensemble_member",
                    requested_start_time=run_start_time,
                    duration_hours=duration_hours,
                    member_id=member_id,
                    perturbation={
                        "time_offset_hours": time_offset_hours,
                        "horizontal_diffusivity_m2s": diffusivity,
                        "wind_factor": wind_factor,
                        "random_seed": member_seed,
                    },
                )

                model = self._build_model(
                    simulation_start=run_start_time,
                    simulation_end=run_end_time,
                    audit=audit,
                    wind_factor=wind_factor,
                    require_wave=True,
                    enable_stokes_drift=self.enable_stokes_drift,
                )
                model.set_config("drift:horizontal_diffusivity", diffusivity)
                model.set_config("drift:wind_uncertainty", 0.0)
                model.set_config("drift:current_uncertainty", 0.0)
                self._seed_polygon_release(
                    model,
                    run_start_time,
                    num_elements=int(self.official_element_count),
                    random_seed=member_seed,
                )
                audit["seed_element_count"] = int(self.official_element_count)

                output_file = self.output_dir / f"member_{member_id:02d}.nc"
                self._run_model(
                    model=model,
                    output_file=output_file,
                    duration_hours=duration_hours,
                    audit=audit,
                )
                ensemble_files.append(output_file)
                member_runs.append(
                    {
                        "member_id": member_id,
                        "output_file": output_file,
                        "start_time_utc": timestamp_to_utc_iso(run_start_time),
                        "end_time_utc": timestamp_to_utc_iso(run_end_time),
                        "element_count": int(self.official_element_count),
                        "perturbation": audit["perturbation"],
                    }
                )
        else:
            base_time = normalize_model_timestamp(start_time)
            rng = np.random.default_rng()
            p_cfg = self.config["perturbations"]

            for i in range(self.ensemble_size):
                member_id = i + 1

                t_shift = float(p_cfg["time_shift_hours"])
                time_offset_hours = float(rng.uniform(-t_shift, t_shift))
                run_start_time = base_time + timedelta(hours=time_offset_hours)
                run_end_time = run_start_time + timedelta(hours=duration_hours)

                diffusivity = float(rng.uniform(p_cfg["diffusivity_min"], p_cfg["diffusivity_max"]))
                wind_uncertainty = float(
                    rng.uniform(
                        p_cfg["wind_uncertainty_min"],
                        p_cfg["wind_uncertainty_max"],
                    )
                )

                print(
                    f"   Member {member_id}/{self.ensemble_size}: "
                    f"T{time_offset_hours:+.1f}h | K={diffusivity:.3f} | W_unc={wind_uncertainty:.1f}"
                )

                audit = self._init_run_audit(
                    recipe_name=recipe_name,
                    run_kind="ensemble_member",
                    requested_start_time=run_start_time,
                    duration_hours=duration_hours,
                    member_id=member_id,
                    perturbation={
                        "time_offset_hours": time_offset_hours,
                        "horizontal_diffusivity": diffusivity,
                        "wind_uncertainty": wind_uncertainty,
                    },
                )

                model = self._build_model(
                    simulation_start=run_start_time,
                    simulation_end=run_end_time,
                    audit=audit,
                    wind_factor=1.0,
                    require_wave=False,
                    enable_stokes_drift=False,
                )
                model.set_config("drift:horizontal_diffusivity", diffusivity)
                model.set_config("drift:wind_uncertainty", wind_uncertainty)
                model.set_config("drift:current_uncertainty", 0.1)
                self._seed_polygon_release(model, run_start_time, num_elements=2000)
                audit["seed_element_count"] = 2000

                output_file = self.output_dir / f"member_{member_id:02d}.nc"
                self._run_model(
                    model=model,
                    output_file=output_file,
                    duration_hours=duration_hours,
                    audit=audit,
                )
                ensemble_files.append(output_file)
                member_runs.append(
                    {
                        "member_id": member_id,
                        "output_file": output_file,
                        "start_time_utc": timestamp_to_utc_iso(run_start_time),
                        "end_time_utc": timestamp_to_utc_iso(run_end_time),
                        "element_count": 2000,
                        "perturbation": audit["perturbation"],
                    }
                )

        logger.info("Ensemble runs complete. Generating probability products...")
        if self.case.is_official:
            product_files, product_records = self._generate_official_ensemble_products(
                member_runs=member_runs,
                nominal_start_time=base_time,
                start_lat=start_lat,
                start_lon=start_lon,
            )
        else:
            product_files = self._generate_prototype_probability_products(ensemble_files, start_lat, start_lon)
            product_records = []

        manifest = self.write_output_manifest(
            recipe_name=recipe_name,
            member_runs=member_runs,
            written_files=[*ensemble_files, *product_files],
            product_records=product_records,
            start_time=base_time,
            selection=selection,
        )
        return manifest

    def write_official_forecast_manifest(
        self,
        selection: RecipeSelection,
        deterministic_control: dict,
        ensemble_manifest: dict,
        start_time: str,
    ) -> Path:
        """Write an official spill-forecast manifest for Phase 3B consumers."""
        grid = GridBuilder()
        manifest_path = get_forecast_manifest_path(run_name=self.output_run_name)
        simulation_start, simulation_end, _ = self._get_official_simulation_window()
        validation_time = pd.Timestamp(self.case.validation_layer.event_time_utc or self.case.simulation_end_utc)
        status_flags = self._build_status_flags(selection)
        payload = {
            "manifest_type": "official_phase2_forecast",
            "workflow_mode": self.case.workflow_mode,
            "case_id": self.case.case_id,
            "run_name": self.output_run_name,
            "simulation_window_utc": {
                "start": timestamp_to_utc_iso(simulation_start),
                "end": timestamp_to_utc_iso(simulation_end),
            },
            "grid": {
                "grid_id": grid_id_from_builder(grid),
                **grid.spec.to_metadata(),
            },
            "transport": {
                "model": self.transport_model_name,
                "provisional_transport_model": self.provisional_transport_model,
                "wave_forcing_required": self.require_wave_forcing,
                "stokes_drift_enabled": self.enable_stokes_drift,
            },
            "recipe_selection": {
                "recipe": selection.recipe,
                "source_kind": selection.source_kind,
                "source_path": selection.source_path,
                "status_flag": selection.status_flag,
                "valid": selection.valid,
                "provisional": selection.provisional,
                "rerun_required": selection.rerun_required,
                "note": selection.note,
            },
            "baseline_provenance": {
                "recipe": selection.recipe,
                "source_kind": selection.source_kind,
                "source_path": selection.source_path,
                "note": selection.note,
            },
            "status_flags": status_flags,
            "loading_audit": {
                "json": str(self.audit_json_path),
                "csv": str(self.audit_csv_path),
            },
            "historical_baseline_provenance": self.historical_baseline_provenance,
            "sensitivity_context": self.sensitivity_context,
            "source_geometry": {
                "initialization_polygon": str(self.case.initialization_layer.processed_vector_path(self.case.run_name)),
                "validation_polygon": str(self.case.validation_layer.processed_vector_path(self.case.run_name)),
                "source_point": str(self.case.provenance_layer.processed_vector_path(self.case.run_name)),
            },
            "deterministic_control": {
                "netcdf_path": str(deterministic_control["output_file"]),
                "actual_element_count": deterministic_control.get("element_count"),
                "configuration": deterministic_control.get("configuration", {}),
                "products": deterministic_control.get("products", []),
            },
            "ensemble": {
                "manifest_path": ensemble_manifest.get("manifest"),
                "actual_member_count": self.official_ensemble_size,
                "actual_element_count": self.official_element_count,
            },
            "canonical_products": {
                "control_footprint_mask": str(
                    get_official_control_footprint_mask_path(validation_time, run_name=self.output_run_name)
                ),
                "control_density_norm": str(
                    get_official_control_density_norm_path(validation_time, run_name=self.output_run_name)
                ),
                "prob_presence": str(get_official_prob_presence_path(validation_time, run_name=self.output_run_name)),
                "mask_p50": str(get_official_mask_threshold_path("p50", validation_time, run_name=self.output_run_name)),
                "mask_p90": str(get_official_mask_threshold_path("p90", validation_time, run_name=self.output_run_name)),
                "mask_p50_datecomposite": str(get_official_mask_p50_datecomposite_path(run_name=self.output_run_name)),
            },
            "written_files": [str(path) for path in deterministic_control["written_files"]] + list(ensemble_manifest.get("written_files", [])),
        }
        _write_json_atomic(manifest_path, payload)
        logger.info("Wrote official forecast manifest to %s", manifest_path)
        return manifest_path


def run_ensemble(best_recipe, start_time=None, start_lat=None, start_lon=None):
    """
    Wrapper to run ensemble with the winning recipe.
    """
    try:
        forcing = get_forcing_files(best_recipe)
        currents_file = str(forcing["currents"])
        winds_file = str(forcing["wind"])
        wave_file = str(forcing["wave"]) if forcing.get("wave") else None
    except Exception as e:
        logger.error("Invalid recipe '%s': %s", best_recipe, e)
        return {"status": "error", "message": str(e)}

    service = EnsembleForecastService(currents_file, winds_file, wave_file=wave_file)

    d_lat, d_lon, d_time = resolve_spill_origin()
    _start_lat = start_lat if start_lat is not None else d_lat
    _start_lon = start_lon if start_lon is not None else d_lon
    _start_time = start_time if start_time else d_time

    manifest = service.run_ensemble(
        recipe_name=best_recipe,
        start_lat=_start_lat,
        start_lon=_start_lon,
        start_time=_start_time,
    )
    return {
        "status": "success",
        "output": str(service.output_dir),
        "manifest": manifest["manifest"],
        "written_files": manifest["written_files"],
    }


def run_official_spill_forecast(
    selection: RecipeSelection,
    start_time: str | None = None,
    start_lat: float | None = None,
    start_lon: float | None = None,
    output_run_name: str | None = None,
    forcing_override: dict | None = None,
    sensitivity_context: dict | None = None,
    historical_baseline_provenance: dict | None = None,
):
    """Run the official deterministic control plus ensemble path for Phase 3B."""
    try:
        forcing = dict(forcing_override or get_forcing_files(selection.recipe))
        currents_file = str(forcing["currents"])
        winds_file = str(forcing["wind"])
        wave_file = str(forcing["wave"]) if forcing.get("wave") else None
    except Exception as e:
        logger.error("Invalid recipe '%s': %s", selection.recipe, e)
        return {"status": "error", "message": str(e)}

    service = EnsembleForecastService(
        currents_file,
        winds_file,
        wave_file=wave_file,
        output_run_name=output_run_name,
        sensitivity_context=sensitivity_context,
        historical_baseline_provenance=historical_baseline_provenance,
    )

    d_lat, d_lon, d_time = resolve_spill_origin()
    _start_lat = start_lat if start_lat is not None else d_lat
    _start_lon = start_lon if start_lon is not None else d_lon
    _start_time = start_time if start_time else d_time

    deterministic_control = service.run_deterministic_control(
        recipe_name=selection.recipe,
        start_time=_start_time,
        selection=selection,
    )
    ensemble_manifest = service.run_ensemble(
        recipe_name=selection.recipe,
        start_lat=_start_lat,
        start_lon=_start_lon,
        start_time=_start_time,
        selection=selection,
    )
    forecast_manifest = service.write_official_forecast_manifest(
        selection=selection,
        deterministic_control=deterministic_control,
        ensemble_manifest=ensemble_manifest,
        start_time=_start_time,
    )
    return {
        "status": "success",
        "output": str(service.output_dir),
        "manifest": ensemble_manifest["manifest"],
        "forecast_manifest": str(forecast_manifest),
        "deterministic_control": str(deterministic_control["output_file"]),
        "written_files": [
            str(path) for path in deterministic_control["written_files"]
        ] + ensemble_manifest["written_files"] + [str(forecast_manifest)],
    }
