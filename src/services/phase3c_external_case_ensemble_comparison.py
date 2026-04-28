"""DWH Phase 3C deterministic-versus-ensemble comparison on public masks."""

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
from src.services.phase3c_external_case_run import (
    DWHPhase3CExternalCaseRunService,
    build_event_corridor_mask,
    load_frozen_scientific_forcing_stack,
    rasterize_particles_to_spec,
    window_cells_for_km,
)
from src.utils.ensemble_config import get_official_ensemble_block, load_ensemble_config
from src.utils.io import find_wind_vars

try:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
except ImportError:  # pragma: no cover
    plt = None


PHASE = "phase3c_external_case_ensemble_comparison"
OUTPUT_DIR = Path("output") / CASE_ID / PHASE
FORCING_READY_DIR = Path("output") / CASE_ID / SCIENTIFIC_PHASE
DETERMINISTIC_DIR = Path("output") / CASE_ID / "phase3c_external_case_run"
TRACK_DIR = OUTPUT_DIR / "tracks"
PRODUCT_DIR = OUTPUT_DIR / "products"
WIND_CACHE_DIR = OUTPUT_DIR / "forcing_cache"

VALIDATION_DATES = ("2010-05-21", "2010-05-22", "2010-05-23")
FSS_WINDOWS_KM = (1, 3, 5, 10)


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


def determine_recommendation(track_summaries: dict[str, pd.DataFrame]) -> str:
    scores = {}
    for track_id, summary in track_summaries.items():
        if summary.empty:
            continue
        cols = [f"fss_{window}km" for window in FSS_WINDOWS_KM]
        scores[track_id] = float(summary[cols].astype(float).to_numpy().mean())
    if not scores:
        return "the result is mixed by metric/date; next step: DWH PyGNOME comparator"
    ordered = sorted(scores.items(), key=lambda item: item[1], reverse=True)
    top_id, top_score = ordered[0]
    second_score = ordered[1][1] if len(ordered) > 1 else -np.inf
    if top_score - second_score < 0.01:
        return "the result is mixed by metric/date; next step: final packaging/chapter sync"
    mapping = {
        "opendrift_control": "deterministic DWH is stronger; next step: final packaging/chapter sync",
        "ensemble_p50": "ensemble p50 is stronger; next step: final packaging/chapter sync",
        "ensemble_p90": "ensemble p90 is stronger; next step: DWH threshold appendix",
    }
    return mapping.get(top_id, "the result is mixed by metric/date; next step: final packaging/chapter sync")


class DWHPhase3CEnsembleComparisonService:
    def __init__(self):
        self.case = get_case_context()
        if self.case.workflow_mode != "dwh_retro_2010":
            raise RuntimeError(f"{PHASE} requires WORKFLOW_MODE=dwh_retro_2010.")
        self.cfg = _load_yaml(CONFIG_PATH)
        self.spec = load_dwh_scoring_grid_spec(SETUP_DIR / "scoring_grid.yaml")
        self.sea_mask = self._load_sea_mask()
        self.validation_dates = tuple(str(value) for value in self.cfg.get("accepted_validation_dates", VALIDATION_DATES))
        self.nominal_start = _timestamp(self.cfg.get("simulation_start_utc", "2010-05-20T00:00:00Z"))
        self.nominal_end = _timestamp(self.cfg.get("simulation_end_utc", "2010-05-23T23:59:59Z"))
        self.output_dir = OUTPUT_DIR
        self.track_dir = TRACK_DIR
        self.product_dir = PRODUCT_DIR
        self.wind_cache_dir = WIND_CACHE_DIR
        self.scorer = DWHPhase3CExternalCaseRunService(output_dir=self.output_dir)
        forecast_cfg = self.cfg.get("forecast") or {}
        ensemble_cfg = (self.cfg.get("phase3c_external_case_ensemble_comparison") or {}).get("ensemble") or {}
        thesis_ensemble = get_official_ensemble_block(load_ensemble_config(Path("config") / "ensemble.yaml"))
        self.ensemble_size = int(ensemble_cfg.get("ensemble_size", thesis_ensemble.get("ensemble_size", 50)))
        self.wind_factor_min = float(ensemble_cfg.get("wind_factor_min", thesis_ensemble.get("wind_factor_min", 0.8)))
        self.wind_factor_max = float(ensemble_cfg.get("wind_factor_max", thesis_ensemble.get("wind_factor_max", 1.2)))
        self.offset_choices = [int(v) for v in ensemble_cfg.get("start_time_offset_hours", thesis_ensemble.get("start_time_offset_hours", [-3, -2, -1, 0, 1, 2, 3]))]
        self.diff_min = float(ensemble_cfg.get("horizontal_diffusivity_m2s_min", thesis_ensemble.get("horizontal_diffusivity_m2s_min", 1.0)))
        self.diff_max = float(ensemble_cfg.get("horizontal_diffusivity_m2s_max", thesis_ensemble.get("horizontal_diffusivity_m2s_max", 10.0)))
        self.base_seed = int(ensemble_cfg.get("base_seed", forecast_cfg.get("polygon_seed_random_seed", 20100520)))
        self.element_count = int(ensemble_cfg.get("element_count_per_member", forecast_cfg.get("element_count", 5000)))

    def _load_sea_mask(self) -> np.ndarray | None:
        sea_mask_path = SETUP_DIR / "sea_mask.tif"
        return _load_raster(sea_mask_path)[0] if sea_mask_path.exists() else None

    def _assert_artifacts(self) -> None:
        required = [
            FORCING_READY_DIR / "dwh_scientific_forcing_status.json",
            DETERMINISTIC_DIR / "phase3c_summary.csv",
            DETERMINISTIC_DIR / "phase3c_run_manifest.json",
        ]
        required.extend(SETUP_DIR / f"obs_mask_{date}.tif" for date in self.validation_dates)
        required.extend(
            DETERMINISTIC_DIR / "products" / f"control_footprint_mask_{date}_datecomposite.tif"
            for date in self.validation_dates
        )
        missing = [str(path) for path in required if not path.exists()]
        if missing:
            raise FileNotFoundError("DWH ensemble comparison is missing required artifacts: " + "; ".join(missing))

    def _load_stack(self) -> dict[str, dict]:
        rows = json.loads((FORCING_READY_DIR / "dwh_scientific_forcing_status.json").read_text(encoding="utf-8"))
        return load_frozen_scientific_forcing_stack(rows)

    def _deterministic_paths(self) -> dict[str, Path]:
        paths = {
            date: DETERMINISTIC_DIR / "products" / f"control_footprint_mask_{date}_datecomposite.tif"
            for date in self.validation_dates
        }
        paths["event_corridor"] = DETERMINISTIC_DIR / "products" / "control_footprint_mask_2010-05-21_2010-05-23_eventcorridor.tif"
        return paths

    def _scale_wind_forcing(self, source_path: Path, wind_factor: float) -> Path:
        if np.isclose(float(wind_factor), 1.0):
            return source_path
        self.wind_cache_dir.mkdir(parents=True, exist_ok=True)
        cache_name = f"{source_path.stem}__windfactor__{wind_factor:.3f}.nc"
        out_path = self.wind_cache_dir / cache_name
        if out_path.exists():
            return out_path
        with xr.open_dataset(source_path) as ds:
            u_var, v_var = find_wind_vars(ds)
            working = ds.load()
        working[u_var] = working[u_var] * float(wind_factor)
        working[v_var] = working[v_var] * float(wind_factor)
        working.attrs["wind_factor_applied"] = float(wind_factor)
        working.to_netcdf(out_path)
        return out_path

    def _member_specs(self) -> list[dict]:
        rng = np.random.default_rng(self.base_seed)
        specs = []
        for idx in range(self.ensemble_size):
            member_id = idx + 1
            member_seed = int(rng.integers(0, np.iinfo(np.int32).max))
            member_rng = np.random.default_rng(member_seed)
            requested_offset = int(member_rng.choice(self.offset_choices))
            effective_offset = max(0, requested_offset)
            start_clipped = effective_offset != requested_offset
            diffusivity = float(np.exp(member_rng.uniform(np.log(self.diff_min), np.log(self.diff_max))))
            wind_factor = float(member_rng.uniform(self.wind_factor_min, self.wind_factor_max))
            specs.append(
                {
                    "member_id": member_id,
                    "member_seed": member_seed,
                    "requested_start_time_offset_h": requested_offset,
                    "effective_start_time_offset_h": effective_offset,
                    "start_offset_clipped_to_forcing_window": start_clipped,
                    "horizontal_diffusivity_m2s": diffusivity,
                    "wind_factor": wind_factor,
                    "start_time_utc": _timestamp_iso(self.nominal_start + timedelta(hours=effective_offset)),
                    "end_time_utc": _timestamp_iso(self.nominal_end),
                }
            )
        return specs

    def _run_or_reuse_member(self, spec: dict, stack: dict[str, dict]) -> dict:
        track_path = self.track_dir / f"member_{int(spec['member_id']):02d}.nc"
        audit_path = self.track_dir / f"member_{int(spec['member_id']):02d}_audit.json"
        if track_path.exists() and audit_path.exists():
            audit = json.loads(audit_path.read_text(encoding="utf-8"))
            if audit.get("success"):
                return audit

        audit = {
            **spec,
            "track_id": f"ensemble_member_{int(spec['member_id']):02d}",
            "success": False,
            "track_path": str(track_path),
            "stop_reason": "",
            "scientific_ready_forcing": True,
            "non_scientific_smoke": False,
            "transport_model": "OpenDrift OceanDrift",
            "element_count": self.element_count,
            "current_source": stack["current"]["dataset_product_id"],
            "wind_source": stack["wind"]["dataset_product_id"],
            "wave_source": stack["wave"]["dataset_product_id"],
        }
        start_time = _timestamp(spec["start_time_utc"])
        try:
            from opendrift.models.oceandrift import OceanDrift
            from opendrift.readers import reader_netCDF_CF_generic

            wind_path = self._scale_wind_forcing(Path(stack["wind"]["local_file_path"]), float(spec["wind_factor"]))
            audit["scaled_wind_path"] = str(wind_path)
            model = OceanDrift(loglevel=30)
            readers = [
                reader_netCDF_CF_generic.Reader(stack["current"]["local_file_path"]),
                reader_netCDF_CF_generic.Reader(str(wind_path)),
                reader_netCDF_CF_generic.Reader(stack["wave"]["local_file_path"]),
            ]
            model.add_reader(readers)
            model.set_config("drift:horizontal_diffusivity", float(spec["horizontal_diffusivity_m2s"]))
            model.set_config("drift:wind_uncertainty", 0.0)
            model.set_config("drift:current_uncertainty", 0.0)
            init_path = SETUP_DIR / "processed" / "layer_05_dwh_t20100520_composite_processed.gpkg"
            from src.services.phase3c_external_case_run import _sample_points_from_init_polygon

            lons, lats = _sample_points_from_init_polygon(init_path, self.element_count, int(spec["member_seed"]), self.spec)
            if track_path.exists():
                track_path.unlink()
            model.seed_elements(lon=lons, lat=lats, number=self.element_count, time=start_time.to_pydatetime())
            model.run(
                time_step=timedelta(hours=1),
                time_step_output=timedelta(hours=1),
                end_time=self.nominal_end,
                outfile=str(track_path),
            )
            audit["success"] = True
        except Exception as exc:
            audit["stop_reason"] = f"{type(exc).__name__}: {exc}"
        _write_json(audit_path, audit)
        return audit

    @staticmethod
    def _member_time_bounds(track_path: Path) -> tuple[pd.Timestamp, pd.Timestamp]:
        with xr.open_dataset(track_path) as ds:
            times = pd.DatetimeIndex(pd.to_datetime(ds["time"].values))
            if times.tz is not None:
                times = times.tz_convert("UTC").tz_localize(None)
            return pd.Timestamp(times.min()), pd.Timestamp(times.max())

    def _extract_member_hourly_mask(self, track_path: Path, target_time: pd.Timestamp) -> tuple[np.ndarray, np.ndarray] | None:
        start, end = self._member_time_bounds(track_path)
        if target_time < start or target_time > end:
            return None
        from src.services.phase3c_external_case_run import _extract_opendrift_snapshot

        lon, lat, mass, _ = _extract_opendrift_snapshot(track_path, target_time)
        hits, density = rasterize_particles_to_spec(self.spec, lon, lat, mass)
        return _apply_sea_mask(hits, self.sea_mask), _apply_sea_mask(density, self.sea_mask)

    def _extract_member_date_composite(self, track_path: Path, date_value: str) -> tuple[np.ndarray, np.ndarray]:
        from src.services.phase3c_external_case_run import _extract_opendrift_date_composite

        footprint, density, _ = _extract_opendrift_date_composite(track_path, date_value, self.spec, self.sea_mask)
        return footprint, density

    def _aggregate_masks(self, masks: list[np.ndarray], densities: list[np.ndarray]) -> dict:
        if not masks:
            shape = (self.spec.height, self.spec.width)
            zero = np.zeros(shape, dtype=np.float32)
            return {"prob_presence": zero, "mask_p50": zero, "mask_p90": zero, "density_norm": zero}
        probability = np.mean(np.stack([_binary(mask) for mask in masks], axis=0), axis=0).astype(np.float32)
        density = _normalize_density(np.mean(np.stack(densities, axis=0), axis=0).astype(np.float32))
        return {
            "prob_presence": _apply_sea_mask(probability, self.sea_mask),
            "mask_p50": _apply_sea_mask((probability >= 0.5).astype(np.float32), self.sea_mask),
            "mask_p90": _apply_sea_mask((probability >= 0.9).astype(np.float32), self.sea_mask),
            "density_norm": _apply_sea_mask(density, self.sea_mask),
        }

    def _generate_hourly_products(self, successful_members: list[dict]) -> list[dict]:
        records: list[dict] = []
        end_hour = (_timestamp(self.nominal_end) + timedelta(seconds=1)).floor("h")
        timestamps = pd.date_range(self.nominal_start, end_hour, freq="1h")
        for target_time in timestamps:
            masks = []
            densities = []
            available = 0
            for member in successful_members:
                result = self._extract_member_hourly_mask(Path(member["track_path"]), pd.Timestamp(target_time))
                if result is None:
                    continue
                available += 1
                mask, density = result
                masks.append(mask)
                densities.append(density)
            if available == 0:
                continue
            aggregate = self._aggregate_masks(masks, densities)
            label = _timestamp_label(target_time)
            for product_type in ("prob_presence", "mask_p50", "mask_p90", "density_norm"):
                if product_type == "density_norm":
                    filename = f"ensemble_density_norm_{label}.tif"
                    sem = "ensemble_density_norm"
                else:
                    filename = f"{product_type}_{label}.tif"
                    sem = product_type
                out_path = self.product_dir / filename
                _write_raster(self.spec, aggregate[product_type].astype(np.float32), out_path)
                records.append(
                    {
                        "track_id": "ensemble_probability",
                        "product_type": sem,
                        "timestamp_utc": _timestamp_iso(target_time),
                        "path": str(out_path),
                        "available_member_count": available,
                    }
                )
        return records

    def _generate_date_composite_products(self, successful_members: list[dict]) -> list[dict]:
        records: list[dict] = []
        p50_masks = []
        p90_masks = []
        obs_event = []
        for date in self.validation_dates:
            member_masks = []
            member_densities = []
            for member in successful_members:
                mask, density = self._extract_member_date_composite(Path(member["track_path"]), date)
                member_masks.append(mask)
                member_densities.append(density)
            aggregate = self._aggregate_masks(member_masks, member_densities)
            p50_masks.append(aggregate["mask_p50"])
            p90_masks.append(aggregate["mask_p90"])
            for product_type, filename in (
                ("prob_presence", f"prob_presence_{date}_datecomposite.tif"),
                ("mask_p50", f"mask_p50_{date}_datecomposite.tif"),
                ("mask_p90", f"mask_p90_{date}_datecomposite.tif"),
                ("density_norm", f"ensemble_density_norm_{date}_datecomposite.tif"),
            ):
                out_path = self.product_dir / filename
                _write_raster(self.spec, aggregate[product_type].astype(np.float32), out_path)
                records.append(
                    {
                        "track_id": "ensemble_probability",
                        "product_type": "ensemble_density_norm_datecomposite" if product_type == "density_norm" else f"{product_type}_datecomposite",
                        "date_utc": date,
                        "path": str(out_path),
                        "available_member_count": len(successful_members),
                    }
                )
            obs_event.append(_load_raster(SETUP_DIR / f"obs_mask_{date}.tif")[0])

        event_products = {
            "prob_presence": _apply_sea_mask(np.max(np.stack([_load_raster(self.product_dir / f"prob_presence_{date}_datecomposite.tif")[0] for date in self.validation_dates], axis=0), axis=0), self.sea_mask),
            "mask_p50": build_event_corridor_mask(p50_masks),
            "mask_p90": build_event_corridor_mask(p90_masks),
        }
        for product_type, filename in (
            ("prob_presence", "prob_presence_2010-05-21_2010-05-23_eventcorridor.tif"),
            ("mask_p50", "mask_p50_2010-05-21_2010-05-23_eventcorridor.tif"),
            ("mask_p90", "mask_p90_2010-05-21_2010-05-23_eventcorridor.tif"),
        ):
            out_path = self.product_dir / filename
            _write_raster(self.spec, _apply_sea_mask(event_products[product_type], self.sea_mask).astype(np.float32), out_path)
            records.append(
                {
                    "track_id": "ensemble_probability",
                    "product_type": f"{product_type}_eventcorridor",
                    "date_utc": "2010-05-21/2010-05-23",
                    "path": str(out_path),
                    "available_member_count": len(successful_members),
                }
            )
        return records

    def _score_tracks(self) -> dict:
        pairing_rows = []
        diagnostics_rows = []
        fss_rows = []
        track_summaries: dict[str, list[dict]] = {"opendrift_control": [], "ensemble_p50": [], "ensemble_p90": []}
        deterministic_paths = self._deterministic_paths()
        track_defs = {
            "opendrift_control": {
                "run_type": "deterministic",
                "products": {date: deterministic_paths[date] for date in self.validation_dates},
                "event": deterministic_paths["event_corridor"],
                "forecast_type": "control_footprint_mask_datecomposite",
                "event_type": "control_footprint_mask_eventcorridor",
                "reused_deterministic_products": True,
            },
            "ensemble_p50": {
                "run_type": "ensemble_p50",
                "products": {date: self.product_dir / f"mask_p50_{date}_datecomposite.tif" for date in self.validation_dates},
                "event": self.product_dir / "mask_p50_2010-05-21_2010-05-23_eventcorridor.tif",
                "forecast_type": "mask_p50_datecomposite",
                "event_type": "mask_p50_eventcorridor",
                "reused_deterministic_products": False,
            },
            "ensemble_p90": {
                "run_type": "ensemble_p90",
                "products": {date: self.product_dir / f"mask_p90_{date}_datecomposite.tif" for date in self.validation_dates},
                "event": self.product_dir / "mask_p90_2010-05-21_2010-05-23_eventcorridor.tif",
                "forecast_type": "mask_p90_datecomposite",
                "event_type": "mask_p90_eventcorridor",
                "reused_deterministic_products": False,
            },
        }
        for track_id, info in track_defs.items():
            for date in self.validation_dates:
                pair_id = f"{track_id}_{date}"
                result = self.scorer._score_pair(
                    pair_id=pair_id,
                    pair_role="per_date",
                    track_id=track_id,
                    forecast_path=Path(info["products"][date]),
                    obs_path=SETUP_DIR / f"obs_mask_{date}.tif",
                    pairing_date_utc=date,
                    forecast_product_type=info["forecast_type"],
                    observation_product_type="public_dwh_observation_mask",
                )
                result["pairing"]["run_type"] = info["run_type"]
                result["pairing"]["reused_deterministic_products"] = info["reused_deterministic_products"]
                result["diagnostics"]["run_type"] = info["run_type"]
                for row in result["fss_rows"]:
                    row["run_type"] = info["run_type"]
                pairing_rows.append(result["pairing"])
                diagnostics_rows.append(result["diagnostics"])
                fss_rows.extend(result["fss_rows"])
            event_id = f"{track_id}_eventcorridor_2010-05-21_2010-05-23"
            result = self.scorer._score_pair(
                pair_id=event_id,
                pair_role="event_corridor",
                track_id=track_id,
                forecast_path=Path(info["event"]),
                obs_path=self._event_obs_mask_path(),
                pairing_date_utc="2010-05-21/2010-05-23",
                forecast_product_type=info["event_type"],
                observation_product_type="public_dwh_observation_mask_eventcorridor",
            )
            result["pairing"]["run_type"] = info["run_type"]
            result["pairing"]["reused_deterministic_products"] = info["reused_deterministic_products"]
            result["diagnostics"]["run_type"] = info["run_type"]
            for row in result["fss_rows"]:
                row["run_type"] = info["run_type"]
            pairing_rows.append(result["pairing"])
            diagnostics_rows.append(result["diagnostics"])
            fss_rows.extend(result["fss_rows"])

        summary_rows = self.scorer._build_summary_rows(pairing_rows, diagnostics_rows, fss_rows)
        for row in summary_rows:
            track_summaries.setdefault(str(row["track_id"]), []).append(row)
        event_summary_rows = [row for row in summary_rows if row["pair_role"] == "event_corridor"]
        return {
            "pairing_rows": pairing_rows,
            "diagnostics_rows": diagnostics_rows,
            "fss_rows": fss_rows,
            "summary_rows": summary_rows,
            "event_summary_rows": event_summary_rows,
            "track_summaries": {key: pd.DataFrame(rows) for key, rows in track_summaries.items() if rows},
        }

    def _event_obs_mask_path(self) -> Path:
        out_path = self.product_dir / "obs_mask_2010-05-21_2010-05-23_eventcorridor.tif"
        if out_path.exists():
            return out_path
        masks = [_load_raster(SETUP_DIR / f"obs_mask_{date}.tif")[0] for date in self.validation_dates]
        _write_raster(self.spec, _apply_sea_mask(build_event_corridor_mask(masks), self.sea_mask).astype(np.float32), out_path)
        return out_path

    def _write_qa(self, summary_rows: list[dict]) -> list[Path]:
        if plt is None:
            return []
        written: list[Path] = []
        per_date = [row for row in summary_rows if row["pair_role"] == "per_date"]
        if per_date:
            out_path = self.output_dir / "qa_phase3c_ensemble_overlays.png"
            try:
                fig, axes = plt.subplots(len(self.validation_dates), 3, figsize=(13, 4 * len(self.validation_dates)))
                track_order = ["opendrift_control", "ensemble_p50", "ensemble_p90"]
                for row_idx, date in enumerate(self.validation_dates):
                    for col_idx, track_id in enumerate(track_order):
                        ax = axes[row_idx, col_idx] if len(self.validation_dates) > 1 else axes[col_idx]
                        row = next(item for item in per_date if item["pairing_date_utc"] == date and item["track_id"] == track_id)
                        forecast, _ = _load_raster(Path(row["forecast_path"]))
                        obs, _ = _load_raster(Path(row["observation_path"]))
                        ax.imshow(self.scorer._overlay_canvas(forecast, obs), origin="upper")
                        ax.set_title(f"{date} - {row['run_type']}")
                        ax.set_axis_off()
                fig.savefig(out_path, dpi=150, bbox_inches="tight")
                plt.close(fig)
                written.append(out_path)
            except Exception:
                pass
        event_rows = [row for row in summary_rows if row["pair_role"] == "event_corridor"]
        if event_rows:
            out_path = self.output_dir / "qa_phase3c_ensemble_eventcorridor_overlay.png"
            try:
                fig, axes = plt.subplots(1, 3, figsize=(13, 4))
                track_order = ["opendrift_control", "ensemble_p50", "ensemble_p90"]
                for col_idx, track_id in enumerate(track_order):
                    row = next(item for item in event_rows if item["track_id"] == track_id)
                    forecast, _ = _load_raster(Path(row["forecast_path"]))
                    obs, _ = _load_raster(Path(row["observation_path"]))
                    axes[col_idx].imshow(self.scorer._overlay_canvas(forecast, obs), origin="upper")
                    axes[col_idx].set_title(row["run_type"])
                    axes[col_idx].set_axis_off()
                fig.savefig(out_path, dpi=150, bbox_inches="tight")
                plt.close(fig)
                written.append(out_path)
            except Exception:
                pass
        return written

    def _write_memo(self) -> Path:
        path = self.output_dir / "chapter3_phase3c_ensemble_extension_memo.md"
        lines = [
            "# Chapter 3 Phase 3C Ensemble Extension Memo",
            "",
            "Phase 3C1 is deterministic DWH transfer validation. Phase 3C2 extends that branch to ensemble DWH transfer validation on the same public observation-derived masks and the same frozen DWH scoring grid.",
            "",
            "Mindoro remains the main Philippine thesis case. DWH remains a separate external transfer-validation/support case.",
            "",
            "DWH observed masks remain truth. The cumulative DWH layer remains context-only. PyGNOME remains comparator-only in the later cross-model branch.",
            "",
            "The ensemble branch keeps the same readiness-gated forcing rule and date-composite logic, rather than any Phase 1 drifter-selected baseline logic, while documenting DWH-specific differences such as clipping requested negative start offsets when the frozen forcing stack begins at the nominal case start time.",
            "",
            "In the current repo state, that frozen DWH stack is HYCOM GOFS 3.1 currents + ERA5 winds + CMEMS wave/Stokes.",
        ]
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return path

    def run(self) -> dict:
        self._assert_artifacts()
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.track_dir.mkdir(parents=True, exist_ok=True)
        self.product_dir.mkdir(parents=True, exist_ok=True)
        stack = self._load_stack()

        member_specs = self._member_specs()
        member_rows = [self._run_or_reuse_member(spec, stack) for spec in member_specs]
        successful_members = [row for row in member_rows if row.get("success")]
        ensemble_success = len(successful_members) == self.ensemble_size

        hourly_records: list[dict] = []
        composite_records: list[dict] = []
        if successful_members:
            hourly_records = self._generate_hourly_products(successful_members)
            composite_records = self._generate_date_composite_products(successful_members)
        scoring = self._score_tracks() if successful_members else {
            "pairing_rows": [],
            "diagnostics_rows": [],
            "fss_rows": [],
            "summary_rows": [],
            "event_summary_rows": [],
            "track_summaries": {},
        }
        qa_paths = self._write_qa(scoring["summary_rows"]) if scoring["summary_rows"] else []
        memo_path = self._write_memo()

        tracks_registry_path = self.output_dir / "phase3c_ensemble_tracks_registry.csv"
        pairing_path = self.output_dir / "phase3c_ensemble_pairing_manifest.csv"
        fss_path = self.output_dir / "phase3c_ensemble_fss_by_date_window.csv"
        diagnostics_path = self.output_dir / "phase3c_ensemble_diagnostics.csv"
        summary_path = self.output_dir / "phase3c_ensemble_summary.csv"
        event_summary_path = self.output_dir / "phase3c_ensemble_eventcorridor_summary.csv"
        manifest_path = self.output_dir / "phase3c_ensemble_run_manifest.json"

        deterministic_row = {
            "track_id": "opendrift_control",
            "model_name": "OpenDrift",
            "run_type": "deterministic",
            "success": True,
            "reused_from_phase3c_external_case_run": True,
            "track_path": str(DETERMINISTIC_DIR / "tracks" / "opendrift_control_dwh_phase3c.nc"),
            "element_count": self.cfg.get("forecast", {}).get("element_count", 5000),
            "ensemble_size": 1,
            "current_source": stack["current"]["dataset_product_id"],
            "wind_source": stack["wind"]["dataset_product_id"],
            "wave_source": stack["wave"]["dataset_product_id"],
            "scientific_ready": True,
            "transport_model": "OpenDrift OceanDrift",
            "provisional_transport_model": True,
            "initialization_mode": self.cfg.get("initialization_mode"),
            "grid_id": f"{CASE_ID}_{self.spec.crs.replace(':', '')}_{int(self.spec.resolution)}m",
            "grid_crs": self.spec.crs,
            "date_composite_logic_used": True,
            "stop_reason": "",
        }
        aggregate_rows = [
            {
                "track_id": "ensemble_p50",
                "model_name": "OpenDrift",
                "run_type": "ensemble_p50",
                "success": bool(successful_members),
                "reused_from_phase3c_external_case_run": False,
                "track_path": "",
                "element_count": self.element_count,
                "ensemble_size": len(successful_members),
                "current_source": stack["current"]["dataset_product_id"],
                "wind_source": stack["wind"]["dataset_product_id"],
                "wave_source": stack["wave"]["dataset_product_id"],
                "scientific_ready": True,
                "transport_model": "OpenDrift OceanDrift",
                "provisional_transport_model": True,
                "initialization_mode": self.cfg.get("initialization_mode"),
                "grid_id": f"{CASE_ID}_{self.spec.crs.replace(':', '')}_{int(self.spec.resolution)}m",
                "grid_crs": self.spec.crs,
                "date_composite_logic_used": True,
                "stop_reason": "" if successful_members else "no successful ensemble members",
            },
            {
                "track_id": "ensemble_p90",
                "model_name": "OpenDrift",
                "run_type": "ensemble_p90",
                "success": bool(successful_members),
                "reused_from_phase3c_external_case_run": False,
                "track_path": "",
                "element_count": self.element_count,
                "ensemble_size": len(successful_members),
                "current_source": stack["current"]["dataset_product_id"],
                "wind_source": stack["wind"]["dataset_product_id"],
                "wave_source": stack["wave"]["dataset_product_id"],
                "scientific_ready": True,
                "transport_model": "OpenDrift OceanDrift",
                "provisional_transport_model": True,
                "initialization_mode": self.cfg.get("initialization_mode"),
                "grid_id": f"{CASE_ID}_{self.spec.crs.replace(':', '')}_{int(self.spec.resolution)}m",
                "grid_crs": self.spec.crs,
                "date_composite_logic_used": True,
                "stop_reason": "" if successful_members else "no successful ensemble members",
            },
        ]
        registry_rows = [deterministic_row, *aggregate_rows, *member_rows]
        _write_csv(tracks_registry_path, registry_rows)
        _write_csv(pairing_path, scoring["pairing_rows"])
        _write_csv(fss_path, scoring["fss_rows"])
        _write_csv(diagnostics_path, scoring["diagnostics_rows"])
        _write_csv(summary_path, scoring["summary_rows"])
        _write_csv(event_summary_path, scoring["event_summary_rows"])

        recommendation = determine_recommendation(scoring["track_summaries"])
        manifest = {
            "case_id": CASE_ID,
            "phase": PHASE,
            "phase_name": "Phase 3C - External case ensemble comparison",
            "thesis_role": "external_transfer_validation_support_case",
            "main_case_reference": "mindoro_retro_2023",
            "main_case_note": "Mindoro remains the main Philippine thesis case; DWH is a separate external transfer-validation story.",
            "ensemble_success": ensemble_success,
            "successful_member_count": len(successful_members),
            "requested_member_count": self.ensemble_size,
            "deterministic_reused": True,
            "truth_source": "public DWH observation-derived daily masks",
            "truth_role": "observed_dwh_daily_masks_are_truth",
            "pygnome_role": "comparator_only",
            "date_composite_logic_used": True,
            "forcing_selection_rule": "Freeze the first complete real historical current+wind+wave stack that passes the scientific-readiness gate; do not use Phase 1 drifter-selected baseline logic.",
            "current_forcing_stack": "HYCOM GOFS 3.1 currents + ERA5 winds + CMEMS wave/Stokes",
            "forcing_stack": stack,
            "ensemble_config": {
                "ensemble_size": self.ensemble_size,
                "element_count_per_member": self.element_count,
                "wind_factor_min": self.wind_factor_min,
                "wind_factor_max": self.wind_factor_max,
                "requested_start_time_offset_hours": self.offset_choices,
                "horizontal_diffusivity_m2s_min": self.diff_min,
                "horizontal_diffusivity_m2s_max": self.diff_max,
                "negative_offset_handling": "requested negative offsets are recorded but clipped to 0 h because the frozen DWH forcing stack starts at the nominal case start time",
            },
            "artifacts": {
                "tracks_registry_csv": str(tracks_registry_path),
                "pairing_manifest_csv": str(pairing_path),
                "fss_csv": str(fss_path),
                "diagnostics_csv": str(diagnostics_path),
                "summary_csv": str(summary_path),
                "event_summary_csv": str(event_summary_path),
                "memo": str(memo_path),
                "qa": [str(path) for path in qa_paths],
            },
            "hourly_product_count": len(hourly_records),
            "date_composite_product_count": len(composite_records),
            "recommendation": recommendation,
        }
        _write_json(manifest_path, manifest)
        _write_csv(self.output_dir / "phase3c_ensemble_product_registry.csv", [*hourly_records, *composite_records])

        summary_df = pd.DataFrame(scoring["summary_rows"])
        event_df = pd.DataFrame(scoring["event_summary_rows"])
        def headline(track_id: str) -> dict[str, dict[str, float]]:
            out = {}
            subset = summary_df[(summary_df["track_id"] == track_id) & (summary_df["pair_role"] == "per_date")] if not summary_df.empty else pd.DataFrame()
            for _, row in subset.iterrows():
                out[str(row["pairing_date_utc"])] = {f"fss_{window}km": float(row[f"fss_{window}km"]) for window in FSS_WINDOWS_KM}
            return out
        def event_headline(track_id: str) -> dict[str, float]:
            subset = event_df[event_df["track_id"] == track_id] if not event_df.empty else pd.DataFrame()
            if subset.empty:
                return {}
            row = subset.iloc[0]
            return {f"fss_{window}km": float(row[f"fss_{window}km"]) for window in FSS_WINDOWS_KM}

        return {
            "output_dir": str(self.output_dir),
            "tracks_registry_csv": str(tracks_registry_path),
            "pairing_manifest_csv": str(pairing_path),
            "fss_by_date_window_csv": str(fss_path),
            "summary_csv": str(summary_path),
            "eventcorridor_summary_csv": str(event_summary_path),
            "ensemble_success": ensemble_success,
            "successful_member_count": len(successful_members),
            "requested_member_count": self.ensemble_size,
            "deterministic_success": True,
            "headline_fss": {
                "deterministic": headline("opendrift_control"),
                "p50": headline("ensemble_p50"),
                "p90": headline("ensemble_p90"),
            },
            "eventcorridor_fss": {
                "deterministic": event_headline("opendrift_control"),
                "p50": event_headline("ensemble_p50"),
                "p90": event_headline("ensemble_p90"),
            },
            "recommendation": recommendation,
        }


def run_phase3c_external_case_ensemble_comparison() -> dict:
    return DWHPhase3CEnsembleComparisonService().run()


__all__ = [
    "PHASE",
    "determine_recommendation",
    "run_phase3c_external_case_ensemble_comparison",
]
