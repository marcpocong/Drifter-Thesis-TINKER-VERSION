"""DWH Phase 3C PyGNOME comparator against reused OpenDrift scientific tracks."""

from __future__ import annotations

import csv
import json
import random
from datetime import timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import yaml

from src.core.case_context import get_case_context
from src.helpers.raster import extract_particles_at_time
from src.services.dwh_phase3c_scientific_forcing import SCIENTIFIC_PHASE
from src.services.dwh_phase3c_smoke import (
    CASE_ID,
    CONFIG_PATH,
    SETUP_DIR,
    _load_raster,
    _timestamp,
    _timestamp_iso,
    _timestamp_label,
    _write_raster,
    load_dwh_scoring_grid_spec,
)
from src.services.gnome_comparison import GNOME_AVAILABLE
from src.services.phase3c_external_case_run import (
    DWHPhase3CExternalCaseRunService,
    _apply_sea_mask,
    _normalize_density,
    _sample_points_from_init_polygon,
    build_event_corridor_mask,
    load_frozen_scientific_forcing_stack,
    rasterize_particles_to_spec,
)

try:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
except ImportError:  # pragma: no cover
    plt = None

try:
    import netCDF4
except ImportError:  # pragma: no cover
    netCDF4 = None


PHASE = "phase3c_dwh_pygnome_comparator"
OUTPUT_DIR = Path("output") / CASE_ID / PHASE
TRACK_DIR = OUTPUT_DIR / "tracks"
PRODUCT_DIR = OUTPUT_DIR / "products"
PRECHECK_DIR = OUTPUT_DIR / "prechecks"

FORCING_READY_DIR = Path("output") / CASE_ID / SCIENTIFIC_PHASE
RUN_DIR = Path("output") / CASE_ID / "phase3c_external_case_run"
ENSEMBLE_DIR = Path("output") / CASE_ID / "phase3c_external_case_ensemble_comparison"

VALIDATION_DATES = ("2010-05-21", "2010-05-22", "2010-05-23")
FSS_WINDOWS_KM = (1, 3, 5, 10)
TRACK_ORDER = ("opendrift_control", "ensemble_p50", "ensemble_p90", "pygnome_deterministic")
TRACK_LABELS = {
    "opendrift_control": "deterministic",
    "ensemble_p50": "p50",
    "ensemble_p90": "p90",
    "pygnome_deterministic": "pygnome",
}
RECOMMENDATION_MAP = {
    "opendrift_control": "OpenDrift deterministic is strongest on DWH; next step: final packaging/chapter sync",
    "ensemble_p50": "OpenDrift ensemble p50 is strongest on DWH; next step: final packaging/chapter sync",
    "ensemble_p90": "OpenDrift ensemble p90 is strongest on DWH; next step: DWH threshold appendix",
    "pygnome_deterministic": "PyGNOME deterministic is strongest on DWH; next step: more DWH model harmonization",
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


def _mean_row_fss(row: pd.Series | dict) -> float:
    values = []
    for window in FSS_WINDOWS_KM:
        try:
            value = float(row.get(f"fss_{window}km", np.nan))
        except (TypeError, ValueError):
            value = np.nan
        if np.isfinite(value):
            values.append(value)
    return float(np.mean(values)) if values else 0.0


def determine_recommendation(summary_df: pd.DataFrame) -> str:
    if summary_df.empty:
        return "the result is mixed by metric/date; next step: more DWH model harmonization"

    working = summary_df.copy()
    working["mean_fss"] = working.apply(_mean_row_fss, axis=1)
    by_track = working.groupby("track_id", dropna=False)["mean_fss"].mean().sort_values(ascending=False)
    if by_track.empty:
        return "the result is mixed by metric/date; next step: more DWH model harmonization"

    top_id = str(by_track.index[0])
    top_score = float(by_track.iloc[0])
    second_score = float(by_track.iloc[1]) if len(by_track) > 1 else -np.inf

    event_rows = working[working["pair_role"] == "event_corridor"].copy()
    best_event_id = top_id
    if not event_rows.empty:
        event_scores = event_rows.groupby("track_id", dropna=False)["mean_fss"].mean().sort_values(ascending=False)
        if not event_scores.empty:
            best_event_id = str(event_scores.index[0])

    if top_id != best_event_id or (top_score - second_score) < 0.01:
        if "pygnome_deterministic" in {top_id, best_event_id}:
            return "the result is mixed by metric/date; next step: more DWH model harmonization"
        return "the result is mixed by metric/date; next step: final packaging/chapter sync"
    return RECOMMENDATION_MAP.get(top_id, "the result is mixed by metric/date; next step: final packaging/chapter sync")


class DWHPhase3CPyGnomeComparatorService:
    def __init__(self):
        self.case = get_case_context()
        if self.case.workflow_mode != "dwh_retro_2010":
            raise RuntimeError(f"{PHASE} requires WORKFLOW_MODE=dwh_retro_2010.")

        self.cfg = _load_yaml(CONFIG_PATH)
        self.phase_cfg = (self.cfg.get(PHASE) or {}).copy()
        self.spec = load_dwh_scoring_grid_spec(SETUP_DIR / "scoring_grid.yaml")
        self.sea_mask = self._load_sea_mask()
        self.validation_dates = tuple(str(value) for value in self.cfg.get("accepted_validation_dates", VALIDATION_DATES))
        self.output_dir = OUTPUT_DIR
        self.track_dir = TRACK_DIR
        self.product_dir = PRODUCT_DIR
        self.precheck_dir = PRECHECK_DIR
        self.scorer = DWHPhase3CExternalCaseRunService(output_dir=self.output_dir)

        forecast_cfg = self.cfg.get("forecast") or {}
        scientific_cfg = self.cfg.get("scientific_forcing") or {}
        self.start_utc = str(self.cfg.get("simulation_start_utc", "2010-05-20T00:00:00Z"))
        self.end_utc = str(scientific_cfg.get("request_end_utc", "2010-05-24T00:00:00Z"))
        self.nominal_end_utc = str(self.cfg.get("simulation_end_utc", "2010-05-23T23:59:59Z"))
        self.element_count = int(self.phase_cfg.get("element_count", forecast_cfg.get("element_count", 5000)))
        self.cluster_count = int(self.phase_cfg.get("polygon_surrogate_cluster_count", 100))
        self.random_seed = int(self.phase_cfg.get("polygon_surrogate_random_seed", forecast_cfg.get("polygon_seed_random_seed", 20100520)))
        self.random_mover_diffusivity_m2s = float(self.phase_cfg.get("random_mover_diffusivity_m2s", 1.0))
        self.output_interval_hours = int(self.phase_cfg.get("output_interval_hours", self.cfg.get("raw_output_interval_hours", 1)))
        self.oil_type = str(self.phase_cfg.get("oil_type", "oil_crude"))
        self.grid_id = f"{CASE_ID}_{self.spec.crs.replace(':', '')}_{int(self.spec.resolution)}m"

    def _load_sea_mask(self) -> np.ndarray | None:
        sea_mask_path = SETUP_DIR / "sea_mask.tif"
        return _load_raster(sea_mask_path)[0] if sea_mask_path.exists() else None

    def _assert_artifacts(self) -> None:
        required = [
            CONFIG_PATH,
            FORCING_READY_DIR / "dwh_scientific_forcing_status.json",
            FORCING_READY_DIR / "dwh_scientific_prepared_forcing_manifest.json",
            RUN_DIR / "phase3c_run_manifest.json",
            ENSEMBLE_DIR / "phase3c_ensemble_pairing_manifest.csv",
            ENSEMBLE_DIR / "phase3c_ensemble_fss_by_date_window.csv",
            ENSEMBLE_DIR / "phase3c_ensemble_diagnostics.csv",
            ENSEMBLE_DIR / "phase3c_ensemble_summary.csv",
            ENSEMBLE_DIR / "phase3c_ensemble_eventcorridor_summary.csv",
            SETUP_DIR / "processed" / "layer_05_dwh_t20100520_composite_processed.gpkg",
            SETUP_DIR / "scoring_grid.yaml",
            SETUP_DIR / "sea_mask.tif",
        ]
        required.extend(SETUP_DIR / f"obs_mask_{date}.tif" for date in self.validation_dates)
        missing = [str(path) for path in required if not path.exists()]
        if missing:
            raise FileNotFoundError("DWH PyGNOME comparator is missing required artifacts: " + "; ".join(missing))

    def _load_stack(self) -> dict[str, dict]:
        rows = json.loads((FORCING_READY_DIR / "dwh_scientific_forcing_status.json").read_text(encoding="utf-8"))
        return load_frozen_scientific_forcing_stack(rows)

    def _load_reused_tables(self) -> dict[str, pd.DataFrame]:
        tables = {
            "pairing": pd.read_csv(ENSEMBLE_DIR / "phase3c_ensemble_pairing_manifest.csv"),
            "fss": pd.read_csv(ENSEMBLE_DIR / "phase3c_ensemble_fss_by_date_window.csv"),
            "diagnostics": pd.read_csv(ENSEMBLE_DIR / "phase3c_ensemble_diagnostics.csv"),
            "summary": pd.read_csv(ENSEMBLE_DIR / "phase3c_ensemble_summary.csv"),
            "event_summary": pd.read_csv(ENSEMBLE_DIR / "phase3c_ensemble_eventcorridor_summary.csv"),
        }
        for name, frame in tables.items():
            frame["reused_from_phase3c_external_case_ensemble_comparison"] = True
            tables[name] = frame

        summary = tables["summary"]
        for track_id in TRACK_ORDER[:-1]:
            per_date = summary[(summary["track_id"] == track_id) & (summary["pair_role"] == "per_date")]
            dates = {str(value) for value in per_date["pairing_date_utc"].tolist()}
            if dates != set(self.validation_dates):
                raise RuntimeError(f"Reused ensemble summary is missing per-date rows for {track_id}: {dates}")
            if summary[(summary["track_id"] == track_id) & (summary["pair_role"] == "event_corridor")].empty:
                raise RuntimeError(f"Reused ensemble summary is missing the event-corridor row for {track_id}.")
        return tables

    def _event_obs_mask_path(self) -> Path:
        existing = ENSEMBLE_DIR / "products" / "obs_mask_2010-05-21_2010-05-23_eventcorridor.tif"
        if existing.exists():
            return existing
        out_path = self.product_dir / "obs_mask_2010-05-21_2010-05-23_eventcorridor.tif"
        masks = [_load_raster(SETUP_DIR / f"obs_mask_{date}.tif")[0] for date in self.validation_dates]
        _write_raster(self.spec, _apply_sea_mask(build_event_corridor_mask(masks), self.sea_mask).astype(np.float32), out_path)
        return out_path

    def _run_or_reuse_pygnome(self, stack: dict[str, dict]) -> dict:
        track_path = self.track_dir / "pygnome_dwh_phase3c.nc"
        audit_json = self.output_dir / "phase3c_dwh_pygnome_loading_audit.json"
        audit_csv = self.output_dir / "phase3c_dwh_pygnome_loading_audit.csv"
        forcing_manifest_paths = (
            f"{FORCING_READY_DIR / 'dwh_scientific_forcing_status.json'};"
            f"{FORCING_READY_DIR / 'dwh_scientific_prepared_forcing_manifest.json'}"
        )

        if track_path.exists() and audit_json.exists():
            audit = json.loads(audit_json.read_text(encoding="utf-8"))
            if audit.get("success"):
                return audit

        audit = {
            "case_id": CASE_ID,
            "phase": PHASE,
            "track_id": "pygnome_deterministic",
            "model_name": "PyGNOME deterministic comparator",
            "run_type": "pygnome_deterministic",
            "success": False,
            "track_path": str(track_path),
            "stop_reason": "",
            "non_scientific_smoke": False,
            "source_is_smoke_only": False,
            "scientific_ready_current": bool(stack.get("current", {}).get("scientific_ready")),
            "scientific_ready_wind": bool(stack.get("wind", {}).get("scientific_ready")),
            "scientific_ready_wave": bool(stack.get("wave", {}).get("scientific_ready")),
            "currents_attached": False,
            "winds_attached": False,
            "waves_attached": False,
            "current_source": stack["current"]["dataset_product_id"],
            "wind_source": stack["wind"]["dataset_product_id"],
            "wave_source": stack["wave"]["dataset_product_id"],
            "wave_stokes_handling_status": "not attached identically; the DWH PyGNOME comparator uses the scientific current and wind stack but does not have a clean direct Stokes-wave mover in this repo/container branch",
            "forcing_manifest_paths": forcing_manifest_paths,
            "forcing_file_paths": {
                "current": stack["current"]["local_file_path"],
                "wind": stack["wind"]["local_file_path"],
                "wave": stack["wave"]["local_file_path"],
            },
            "transport_model": "PyGNOME",
            "provisional_transport_model": True,
            "initialization_mode": "observation_initialized_polygon_surrogate_clustered_point_spills",
            "polygon_release_used": False,
            "surrogate_release_used": True,
            "surrogate_release_notes": "PyGNOME polygon seeding is approximated by clustered point spills sampled from the processed May 20 composite polygon because a clean DWH polygon-release path is not already wired in this repo.",
            "element_count": self.element_count,
            "polygon_surrogate_cluster_count": min(self.cluster_count, self.element_count),
            "polygon_surrogate_random_seed": self.random_seed,
            "random_mover_diffusivity_m2s": self.random_mover_diffusivity_m2s,
            "oil_type": self.oil_type,
            "simulation_start_utc": _timestamp_iso(self.start_utc),
            "simulation_end_utc": _timestamp_iso(self.end_utc),
            "nominal_case_end_utc": _timestamp_iso(self.nominal_end_utc),
            "output_interval_hours": self.output_interval_hours,
            "date_composite_logic_used": True,
            "date_composite_note": "Public DWH daily masks remain date-composite truth; no exact sub-daily observation acquisition times are invented for comparator scoring.",
            "structural_mismatch_note": "PyGNOME is comparator only. It reuses the DWH scientific current and wind family but does not reproduce the OpenDrift wave/Stokes attachment identically.",
        }

        if not GNOME_AVAILABLE:
            audit["stop_reason"] = "PyGNOME is unavailable in this environment; run this phase in the gnome container."
            _write_json(audit_json, audit)
            _write_csv(audit_csv, [audit])
            raise RuntimeError(audit["stop_reason"])

        try:
            from gnome.environment import GridCurrent, GridWind
            from gnome.model import Model
            from gnome.movers import CurrentMover, RandomMover, WindMover
            from gnome.outputters import NetCDFOutput
            from gnome.spills import point_line_spill

            try:
                from gnome.spills.gnome_oil import GnomeOil
            except ImportError:
                from gnome.spills import GnomeOil

            init_path = SETUP_DIR / "processed" / "layer_05_dwh_t20100520_composite_processed.gpkg"
            cluster_count = min(self.cluster_count, self.element_count)
            cluster_lons, cluster_lats = _sample_points_from_init_polygon(init_path, cluster_count, self.random_seed, self.spec)
            cluster_weights = np.full(cluster_count, self.element_count // cluster_count, dtype=int)
            cluster_weights[: self.element_count % cluster_count] += 1

            np.random.seed(self.random_seed)
            random.seed(self.random_seed)

            start_time = _timestamp(self.start_utc).to_pydatetime()
            end_time = _timestamp(self.end_utc).to_pydatetime()
            model = Model(
                start_time=start_time,
                duration=end_time - start_time,
                time_step=timedelta(hours=self.output_interval_hours),
            )

            current = GridCurrent.from_netCDF(
                filename=str(stack["current"]["local_file_path"]),
                varnames=["x_sea_water_velocity", "y_sea_water_velocity"],
            )
            wind = GridWind.from_netCDF(
                filename=str(stack["wind"]["local_file_path"]),
                varnames=["x_wind", "y_wind"],
            )
            model.movers += CurrentMover(current=current)
            audit["currents_attached"] = True
            model.movers += WindMover(wind=wind)
            audit["winds_attached"] = True
            model.movers += RandomMover(diffusion_coef=float(self.random_mover_diffusivity_m2s) * 10000.0)

            try:
                oil = GnomeOil(self.oil_type)
            except Exception:
                oil = GnomeOil("oil_crude")
                audit["oil_type"] = "oil_crude"

            for lon, lat, count in zip(cluster_lons, cluster_lats, cluster_weights):
                if int(count) <= 0:
                    continue
                model.spills += point_line_spill(
                    num_elements=int(count),
                    start_position=(float(lon), float(lat), 0.0),
                    release_time=start_time,
                    amount=float(count),
                    units="kg",
                    substance=oil,
                )

            if track_path.exists():
                track_path.unlink()
            model.outputters += NetCDFOutput(
                filename=str(track_path),
                which_data="most",
                output_timestep=timedelta(hours=self.output_interval_hours),
            )
            model.full_run()
            audit["success"] = True
        except Exception as exc:
            audit["stop_reason"] = f"{type(exc).__name__}: {exc}"

        _write_json(audit_json, audit)
        _write_csv(audit_csv, [audit])
        return audit

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

    def _extract_pygnome_date_composite(self, nc_path: Path, date_value: str) -> tuple[np.ndarray, np.ndarray, dict]:
        footprint = np.zeros((self.spec.height, self.spec.width), dtype=np.float32)
        density_accum = np.zeros_like(footprint)
        active_snapshots = 0
        active_particles_total = 0
        for timestamp in self._pygnome_times(nc_path):
            if pd.Timestamp(timestamp).date().isoformat() != date_value:
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
            active_snapshots += 1
            active_particles_total += int(len(lon))
            hits, density = rasterize_particles_to_spec(self.spec, lon, lat, mass)
            footprint = np.maximum(footprint, hits)
            density_accum += density

        footprint = _apply_sea_mask(footprint, self.sea_mask)
        density = _apply_sea_mask(_normalize_density(density_accum), self.sea_mask)
        return footprint, density, {
            "date_utc": date_value,
            "active_snapshots": active_snapshots,
            "active_particles_total": active_particles_total,
        }

    def _write_pygnome_products(self, track_path: Path) -> list[dict]:
        records: list[dict] = []
        event_masks = []
        for date in self.validation_dates:
            target_time = f"{date}T23:59:59Z"
            lon, lat, mass, actual_time, meta = extract_particles_at_time(
                track_path,
                target_time,
                "pygnome",
                allow_uniform_mass_fallback=True,
            )
            footprint, density = rasterize_particles_to_spec(self.spec, lon, lat, mass)
            footprint = _apply_sea_mask(footprint, self.sea_mask)
            density = _apply_sea_mask(density, self.sea_mask)
            label = _timestamp_label(target_time)
            footprint_path = self.product_dir / f"pygnome_footprint_mask_{label}.tif"
            density_path = self.product_dir / f"pygnome_density_norm_{label}.tif"
            _write_raster(self.spec, footprint.astype(np.float32), footprint_path)
            _write_raster(self.spec, density.astype(np.float32), density_path)
            records.extend(
                [
                    {
                        "track_id": "pygnome_deterministic",
                        "product_type": "pygnome_footprint_mask",
                        "timestamp_utc": _timestamp_iso(target_time),
                        "actual_snapshot_time_utc": _timestamp_iso(actual_time),
                        "date_utc": date,
                        "path": str(footprint_path),
                        "score_candidate": False,
                        **meta,
                    },
                    {
                        "track_id": "pygnome_deterministic",
                        "product_type": "pygnome_density_norm",
                        "timestamp_utc": _timestamp_iso(target_time),
                        "actual_snapshot_time_utc": _timestamp_iso(actual_time),
                        "date_utc": date,
                        "path": str(density_path),
                        "score_candidate": False,
                        **meta,
                    },
                ]
            )

            composite_footprint, composite_density, composite_meta = self._extract_pygnome_date_composite(track_path, date)
            composite_footprint_path = self.product_dir / f"pygnome_footprint_mask_{date}_datecomposite.tif"
            composite_density_path = self.product_dir / f"pygnome_density_norm_{date}_datecomposite.tif"
            _write_raster(self.spec, composite_footprint.astype(np.float32), composite_footprint_path)
            _write_raster(self.spec, composite_density.astype(np.float32), composite_density_path)
            records.extend(
                [
                    {
                        "track_id": "pygnome_deterministic",
                        "product_type": "pygnome_footprint_mask_datecomposite",
                        "date_utc": date,
                        "path": str(composite_footprint_path),
                        "score_candidate": True,
                        **composite_meta,
                    },
                    {
                        "track_id": "pygnome_deterministic",
                        "product_type": "pygnome_density_norm_datecomposite",
                        "date_utc": date,
                        "path": str(composite_density_path),
                        "score_candidate": False,
                        **composite_meta,
                    },
                ]
            )
            event_masks.append(composite_footprint)

        event_path = self.product_dir / "pygnome_eventcorridor_union_2010-05-21_to_2010-05-23.tif"
        event_mask = _apply_sea_mask(build_event_corridor_mask(event_masks), self.sea_mask)
        _write_raster(self.spec, event_mask.astype(np.float32), event_path)
        records.append(
            {
                "track_id": "pygnome_deterministic",
                "product_type": "pygnome_eventcorridor_union",
                "date_utc": "2010-05-21/2010-05-23",
                "path": str(event_path),
                "score_candidate": True,
            }
        )

        _write_csv(self.output_dir / "phase3c_dwh_pygnome_product_registry.csv", records)
        _write_json(self.output_dir / "phase3c_dwh_pygnome_product_registry.json", records)
        return records

    def _score_pygnome(self, product_records: list[dict]) -> dict[str, list[dict]]:
        forecast_by_date = {
            str(row["date_utc"]): Path(row["path"])
            for row in product_records
            if row.get("product_type") == "pygnome_footprint_mask_datecomposite"
        }
        pairing_rows = []
        diagnostics_rows = []
        fss_rows = []
        for date in self.validation_dates:
            result = self.scorer._score_pair(
                pair_id=f"pygnome_deterministic_{date}",
                pair_role="per_date",
                track_id="pygnome_deterministic",
                forecast_path=forecast_by_date[date],
                obs_path=SETUP_DIR / f"obs_mask_{date}.tif",
                pairing_date_utc=date,
                forecast_product_type="pygnome_footprint_mask_datecomposite",
                observation_product_type="public_dwh_observation_mask",
            )
            result["pairing"]["run_type"] = "pygnome_deterministic"
            result["pairing"]["reused_deterministic_products"] = False
            result["pairing"]["reused_from_phase3c_external_case_ensemble_comparison"] = False
            result["diagnostics"]["run_type"] = "pygnome_deterministic"
            result["diagnostics"]["reused_from_phase3c_external_case_ensemble_comparison"] = False
            for row in result["fss_rows"]:
                row["run_type"] = "pygnome_deterministic"
                row["reused_from_phase3c_external_case_ensemble_comparison"] = False
            pairing_rows.append(result["pairing"])
            diagnostics_rows.append(result["diagnostics"])
            fss_rows.extend(result["fss_rows"])

        event_result = self.scorer._score_pair(
            pair_id="pygnome_deterministic_eventcorridor_2010-05-21_2010-05-23",
            pair_role="event_corridor",
            track_id="pygnome_deterministic",
            forecast_path=self.product_dir / "pygnome_eventcorridor_union_2010-05-21_to_2010-05-23.tif",
            obs_path=self._event_obs_mask_path(),
            pairing_date_utc="2010-05-21/2010-05-23",
            forecast_product_type="pygnome_eventcorridor_union",
            observation_product_type="public_dwh_observation_mask_eventcorridor",
        )
        event_result["pairing"]["run_type"] = "pygnome_deterministic"
        event_result["pairing"]["reused_deterministic_products"] = False
        event_result["pairing"]["reused_from_phase3c_external_case_ensemble_comparison"] = False
        event_result["diagnostics"]["run_type"] = "pygnome_deterministic"
        event_result["diagnostics"]["reused_from_phase3c_external_case_ensemble_comparison"] = False
        for row in event_result["fss_rows"]:
            row["run_type"] = "pygnome_deterministic"
            row["reused_from_phase3c_external_case_ensemble_comparison"] = False
        pairing_rows.append(event_result["pairing"])
        diagnostics_rows.append(event_result["diagnostics"])
        fss_rows.extend(event_result["fss_rows"])

        summary_rows = self.scorer._build_summary_rows(pairing_rows, diagnostics_rows, fss_rows)
        for row in summary_rows:
            row["run_type"] = "pygnome_deterministic"
            row["reused_from_phase3c_external_case_ensemble_comparison"] = False
        event_summary_rows = [row for row in summary_rows if row["pair_role"] == "event_corridor"]
        return {
            "pairing_rows": pairing_rows,
            "diagnostics_rows": diagnostics_rows,
            "fss_rows": fss_rows,
            "summary_rows": summary_rows,
            "event_summary_rows": event_summary_rows,
        }

    def _build_track_registry_rows(self, stack: dict[str, dict], pygnome_audit: dict) -> list[dict]:
        forcing_manifest_paths = (
            f"{FORCING_READY_DIR / 'dwh_scientific_forcing_status.json'};"
            f"{FORCING_READY_DIR / 'dwh_scientific_prepared_forcing_manifest.json'}"
        )
        base = {
            "current_source": stack["current"]["dataset_product_id"],
            "wind_source": stack["wind"]["dataset_product_id"],
            "wave_source": stack["wave"]["dataset_product_id"],
            "scientific_ready_current": True,
            "scientific_ready_wind": True,
            "scientific_ready_wave": True,
            "scoring_grid_id": self.grid_id,
            "scoring_grid_crs": self.spec.crs,
            "date_composite_logic_used": True,
            "forcing_manifest_paths": forcing_manifest_paths,
            "source_point_role": self.cfg.get("source_point_role"),
        }
        return [
            {
                **base,
                "track_id": "opendrift_control",
                "model_name": "OpenDrift deterministic control",
                "run_type": "deterministic",
                "initialization_mode": self.cfg.get("initialization_mode"),
                "polygon_release_used": True,
                "surrogate_release_used": False,
                "current_source_role": "scientific-ready HYCOM GOFS 3.1 reanalysis",
                "wind_source_role": "scientific-ready ERA5 hourly single levels",
                "wave_stokes_handling_status": "attached from the scientific-ready DWH stack",
                "transport_model": "OpenDrift OceanDrift",
                "provisional_transport_model": True,
                "reused_from_phase3c_external_case_ensemble_comparison": True,
                "track_path": str(RUN_DIR / "tracks" / "opendrift_control_dwh_phase3c.nc"),
                "structural_limitations": "OpenDrift deterministic remains the baseline scientific control.",
            },
            {
                **base,
                "track_id": "ensemble_p50",
                "model_name": "OpenDrift ensemble p50",
                "run_type": "ensemble_p50",
                "initialization_mode": self.cfg.get("initialization_mode"),
                "polygon_release_used": True,
                "surrogate_release_used": False,
                "current_source_role": "scientific-ready HYCOM GOFS 3.1 reanalysis",
                "wind_source_role": "scientific-ready ERA5 hourly single levels",
                "wave_stokes_handling_status": "attached from the scientific-ready DWH stack",
                "transport_model": "OpenDrift OceanDrift",
                "provisional_transport_model": True,
                "reused_from_phase3c_external_case_ensemble_comparison": True,
                "track_path": "",
                "structural_limitations": "Ensemble p50 is a thresholded probability-derived product reused from the finished DWH ensemble branch.",
            },
            {
                **base,
                "track_id": "ensemble_p90",
                "model_name": "OpenDrift ensemble p90",
                "run_type": "ensemble_p90",
                "initialization_mode": self.cfg.get("initialization_mode"),
                "polygon_release_used": True,
                "surrogate_release_used": False,
                "current_source_role": "scientific-ready HYCOM GOFS 3.1 reanalysis",
                "wind_source_role": "scientific-ready ERA5 hourly single levels",
                "wave_stokes_handling_status": "attached from the scientific-ready DWH stack",
                "transport_model": "OpenDrift OceanDrift",
                "provisional_transport_model": True,
                "reused_from_phase3c_external_case_ensemble_comparison": True,
                "track_path": "",
                "structural_limitations": "Ensemble p90 emphasizes core consensus and is reused from the finished DWH ensemble branch.",
            },
            {
                **base,
                "track_id": "pygnome_deterministic",
                "model_name": "PyGNOME deterministic comparator",
                "run_type": "pygnome_deterministic",
                "initialization_mode": pygnome_audit.get("initialization_mode"),
                "polygon_release_used": pygnome_audit.get("polygon_release_used"),
                "surrogate_release_used": pygnome_audit.get("surrogate_release_used"),
                "current_source_role": "scientific-ready HYCOM GOFS 3.1 reanalysis",
                "wind_source_role": "scientific-ready ERA5 hourly single levels",
                "wave_stokes_handling_status": pygnome_audit.get("wave_stokes_handling_status"),
                "transport_model": pygnome_audit.get("transport_model", "PyGNOME"),
                "provisional_transport_model": pygnome_audit.get("provisional_transport_model", True),
                "reused_from_phase3c_external_case_ensemble_comparison": False,
                "track_path": pygnome_audit.get("track_path", ""),
                "structural_limitations": pygnome_audit.get("structural_mismatch_note", ""),
            },
        ]

    def _write_qa(self, summary_df: pd.DataFrame) -> list[Path]:
        if plt is None or summary_df.empty:
            return []
        written: list[Path] = []

        per_date = summary_df[summary_df["pair_role"] == "per_date"].copy()
        if not per_date.empty:
            out_path = self.output_dir / "qa_phase3c_dwh_pygnome_overlays.png"
            try:
                fig, axes = plt.subplots(len(self.validation_dates), len(TRACK_ORDER), figsize=(16, 4 * len(self.validation_dates)))
                if len(self.validation_dates) == 1:
                    axes = np.array([axes], dtype=object)
                for row_idx, date in enumerate(self.validation_dates):
                    for col_idx, track_id in enumerate(TRACK_ORDER):
                        ax = axes[row_idx, col_idx]
                        subset = per_date[(per_date["pairing_date_utc"] == date) & (per_date["track_id"] == track_id)]
                        if subset.empty:
                            ax.set_axis_off()
                            continue
                        row = subset.iloc[0]
                        forecast, _ = _load_raster(Path(str(row["forecast_path"])))
                        obs, _ = _load_raster(Path(str(row["observation_path"])))
                        ax.imshow(self.scorer._overlay_canvas(forecast, obs), origin="upper")
                        ax.set_title(f"{date} - {TRACK_LABELS.get(track_id, track_id)}")
                        ax.set_axis_off()
                fig.savefig(out_path, dpi=150, bbox_inches="tight")
                plt.close(fig)
                written.append(out_path)
            except Exception:
                pass

        event_rows = summary_df[summary_df["pair_role"] == "event_corridor"].copy()
        if not event_rows.empty:
            out_path = self.output_dir / "qa_phase3c_dwh_pygnome_eventcorridor_overlay.png"
            try:
                fig, axes = plt.subplots(1, len(TRACK_ORDER), figsize=(16, 4))
                for col_idx, track_id in enumerate(TRACK_ORDER):
                    ax = axes[col_idx]
                    subset = event_rows[event_rows["track_id"] == track_id]
                    if subset.empty:
                        ax.set_axis_off()
                        continue
                    row = subset.iloc[0]
                    forecast, _ = _load_raster(Path(str(row["forecast_path"])))
                    obs, _ = _load_raster(Path(str(row["observation_path"])))
                    ax.imshow(self.scorer._overlay_canvas(forecast, obs), origin="upper")
                    ax.set_title(TRACK_LABELS.get(track_id, track_id))
                    ax.set_axis_off()
                fig.savefig(out_path, dpi=150, bbox_inches="tight")
                plt.close(fig)
                written.append(out_path)
            except Exception:
                pass

        return written

    def _write_memo(self) -> Path:
        path = self.output_dir / "chapter3_phase3c_dwh_pygnome_comparison_memo.md"
        lines = [
            "# Chapter 3 Phase 3C DWH PyGNOME Comparison Memo",
            "",
            "Phase 3C is the external rich-data transfer-validation branch. Mindoro remains the main Philippine thesis case, and DWH remains a separate external transfer-validation/support case.",
            "",
            "The DWH observed masks remain truth throughout this comparison. The cumulative DWH layer remains context-only.",
            "",
            "The DWH forcing stack remains the readiness-gated historical stack, not a Phase 1 drifter-selected baseline choice. In the current repo state that stack is HYCOM GOFS 3.1 currents + ERA5 winds + CMEMS wave/Stokes.",
            "",
            "OpenDrift deterministic and the OpenDrift ensemble products remain the main science tracks for the DWH external case. PyGNOME is comparator only, not truth.",
            "",
            "This branch completes the DWH cross-model validation story for Phase 3C by placing the new PyGNOME comparator on the same EPSG:32616 1 km scoring grid and against the same May 21-23 public observation-derived masks and event-corridor logic.",
            "",
            "Where PyGNOME cannot reproduce the OpenDrift/OpenOil forcing stack identically, the mismatch is stated explicitly rather than hidden. In the current implementation PyGNOME reuses the scientific DWH current and wind family but does not attach a matching Stokes-wave mover.",
        ]
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return path

    def _headline(self, summary_df: pd.DataFrame, track_id: str) -> dict[str, dict[str, float]]:
        out: dict[str, dict[str, float]] = {}
        subset = summary_df[(summary_df["track_id"] == track_id) & (summary_df["pair_role"] == "per_date")]
        for _, row in subset.iterrows():
            out[str(row["pairing_date_utc"])] = {f"fss_{window}km": float(row[f"fss_{window}km"]) for window in FSS_WINDOWS_KM}
        return out

    def _event_headline(self, summary_df: pd.DataFrame, track_id: str) -> dict[str, float]:
        subset = summary_df[(summary_df["track_id"] == track_id) & (summary_df["pair_role"] == "event_corridor")]
        if subset.empty:
            return {}
        row = subset.iloc[0]
        return {f"fss_{window}km": float(row[f"fss_{window}km"]) for window in FSS_WINDOWS_KM}

    def run(self) -> dict:
        self._assert_artifacts()
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.track_dir.mkdir(parents=True, exist_ok=True)
        self.product_dir.mkdir(parents=True, exist_ok=True)
        self.precheck_dir.mkdir(parents=True, exist_ok=True)

        stack = self._load_stack()
        reused = self._load_reused_tables()
        pygnome_audit = self._run_or_reuse_pygnome(stack)

        pygnome_scoring = {
            "pairing_rows": [],
            "diagnostics_rows": [],
            "fss_rows": [],
            "summary_rows": [],
            "event_summary_rows": [],
        }
        if pygnome_audit.get("success"):
            products = self._write_pygnome_products(Path(str(pygnome_audit["track_path"])))
            pygnome_scoring = self._score_pygnome(products)

        pairing_df = pd.concat(
            [reused["pairing"], pd.DataFrame(pygnome_scoring["pairing_rows"])],
            ignore_index=True,
            sort=False,
        )
        diagnostics_df = pd.concat(
            [reused["diagnostics"], pd.DataFrame(pygnome_scoring["diagnostics_rows"])],
            ignore_index=True,
            sort=False,
        )
        fss_df = pd.concat(
            [reused["fss"], pd.DataFrame(pygnome_scoring["fss_rows"])],
            ignore_index=True,
            sort=False,
        )
        summary_df = pd.concat(
            [reused["summary"], pd.DataFrame(pygnome_scoring["summary_rows"])],
            ignore_index=True,
            sort=False,
        )
        event_summary_df = summary_df[summary_df["pair_role"] == "event_corridor"].copy()

        tracks_registry_rows = self._build_track_registry_rows(stack, pygnome_audit)
        qa_paths = self._write_qa(summary_df)
        memo_path = self._write_memo()
        recommendation = determine_recommendation(summary_df)

        tracks_registry_path = self.output_dir / "phase3c_dwh_pygnome_tracks_registry.csv"
        pairing_path = self.output_dir / "phase3c_dwh_pygnome_pairing_manifest.csv"
        fss_path = self.output_dir / "phase3c_dwh_pygnome_fss_by_date_window.csv"
        diagnostics_path = self.output_dir / "phase3c_dwh_pygnome_diagnostics.csv"
        summary_path = self.output_dir / "phase3c_dwh_pygnome_summary.csv"
        event_summary_path = self.output_dir / "phase3c_dwh_pygnome_eventcorridor_summary.csv"
        manifest_path = self.output_dir / "phase3c_dwh_pygnome_run_manifest.json"

        _write_csv(tracks_registry_path, tracks_registry_rows)
        pairing_df.to_csv(pairing_path, index=False)
        fss_df.to_csv(fss_path, index=False)
        diagnostics_df.to_csv(diagnostics_path, index=False)
        summary_df.to_csv(summary_path, index=False)
        event_summary_df.to_csv(event_summary_path, index=False)

        manifest = {
            "case_id": CASE_ID,
            "phase": PHASE,
            "phase_name": "Phase 3C - DWH PyGNOME comparator",
            "thesis_role": "external_transfer_validation_support_case",
            "main_case_reference": "mindoro_retro_2023",
            "main_case_note": "Mindoro remains the main Philippine thesis case; DWH is a separate external transfer-validation story.",
            "deterministic_success": True,
            "ensemble_comparison_reused": True,
            "pygnome_comparator_success": bool(pygnome_audit.get("success")),
            "truth_source": "public DWH observation-derived daily masks",
            "truth_role": "observed_dwh_daily_masks_are_truth",
            "pygnome_truth_role": "comparator_only",
            "date_composite_logic_used": True,
            "forcing_selection_rule": "Freeze the first complete real historical current+wind+wave stack that passes the scientific-readiness gate; do not use Phase 1 drifter-selected baseline logic.",
            "current_forcing_stack": "HYCOM GOFS 3.1 currents + ERA5 winds + CMEMS wave/Stokes",
            "initialization_mode": self.cfg.get("initialization_mode"),
            "source_point_role": self.cfg.get("source_point_role"),
            "scoring_grid": {
                "grid_id": self.grid_id,
                "crs": self.spec.crs,
                "resolution_m": self.spec.resolution,
            },
            "forcing_stack": stack,
            "pygnome_loading_audit": pygnome_audit,
            "outputs": {
                "tracks_registry_csv": str(tracks_registry_path),
                "pairing_manifest_csv": str(pairing_path),
                "fss_by_date_window_csv": str(fss_path),
                "diagnostics_csv": str(diagnostics_path),
                "summary_csv": str(summary_path),
                "eventcorridor_summary_csv": str(event_summary_path),
                "chapter3_memo": str(memo_path),
                "qa_artifacts": [str(path) for path in qa_paths],
            },
            "recommendation": recommendation,
        }
        _write_json(manifest_path, manifest)

        return {
            "output_dir": str(self.output_dir),
            "tracks_registry_csv": str(tracks_registry_path),
            "pairing_manifest_csv": str(pairing_path),
            "fss_by_date_window_csv": str(fss_path),
            "diagnostics_csv": str(diagnostics_path),
            "summary_csv": str(summary_path),
            "eventcorridor_summary_csv": str(event_summary_path),
            "run_manifest_json": str(manifest_path),
            "memo": str(memo_path),
            "deterministic_success": True,
            "ensemble_comparison_reused": True,
            "pygnome_comparator_success": bool(pygnome_audit.get("success")),
            "headline_fss": {TRACK_LABELS[track_id]: self._headline(summary_df, track_id) for track_id in TRACK_ORDER},
            "eventcorridor_fss": {TRACK_LABELS[track_id]: self._event_headline(summary_df, track_id) for track_id in TRACK_ORDER},
            "recommendation": recommendation,
        }


def run_phase3c_dwh_pygnome_comparator() -> dict:
    return DWHPhase3CPyGnomeComparatorService().run()


__all__ = [
    "PHASE",
    "determine_recommendation",
    "run_phase3c_dwh_pygnome_comparator",
]
