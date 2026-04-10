"""Transport-retention sensitivity runner for the Mindoro shoreline stranding failure."""

from __future__ import annotations

import json
import math
import os
import shutil
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from src.core.case_context import get_case_context
from src.helpers.metrics import calculate_fss
from src.helpers.raster import GridBuilder, project_points_to_grid, rasterize_particles, save_raster
from src.helpers.scoring import apply_ocean_mask, load_sea_mask_array, precheck_same_grid
from src.services.ensemble import normalize_time_index, run_official_spill_forecast
from src.services.phase3b_extended_public_scored import (
    EVENT_CORRIDOR_DATES,
    EXTENDED_SCORED_DIR_NAME,
    LOCAL_TIMEZONE,
    SHORT_DATES,
)
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
    from scipy.spatial import cKDTree
except ImportError:  # pragma: no cover
    cKDTree = None


TRANSPORT_RETENTION_DIR_NAME = "transport_retention_fix"
STRICT_VALIDATION_TIME_UTC = "2023-03-06T09:59:00Z"
MARCH6_DATE_START_UTC = "2023-03-06T00:00:00Z"


@dataclass(frozen=True)
class RetentionScenario:
    scenario_id: str
    slug: str
    description: str
    coastline_action: str
    coastline_approximation_precision: float
    time_step_minutes: int
    diagnostic_only: bool = False
    reuse_existing_baseline: bool = False

    @property
    def output_slug(self) -> str:
        return f"{self.scenario_id}_{self.slug}"


SCENARIOS = [
    RetentionScenario(
        scenario_id="R0",
        slug="baseline_stranding",
        description="Current baseline behavior: OpenDrift stranding coastline action.",
        coastline_action="stranding",
        coastline_approximation_precision=0.001,
        time_step_minutes=60,
        reuse_existing_baseline=True,
    ),
    RetentionScenario(
        scenario_id="R1",
        slug="previous",
        description="Move particles back to previous wet location on coastline contact.",
        coastline_action="previous",
        coastline_approximation_precision=0.001,
        time_step_minutes=60,
    ),
    RetentionScenario(
        scenario_id="R2",
        slug="stranding_precision_timestep",
        description="Retain stranding but increase coastline precision and use a smaller timestep.",
        coastline_action="stranding",
        coastline_approximation_precision=0.0001,
        time_step_minutes=30,
    ),
    RetentionScenario(
        scenario_id="R3",
        slug="no_land_interaction_diagnostic",
        description="Diagnostic-only no-land-interaction run to test whether coastline interaction is the kill switch.",
        coastline_action="none",
        coastline_approximation_precision=0.001,
        time_step_minutes=60,
        diagnostic_only=True,
    ),
]


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


def _normalize_utc(value: Any) -> pd.Timestamp:
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        return timestamp
    return timestamp.tz_convert("UTC").tz_localize(None)


def _iso_z(value: Any) -> str:
    return _normalize_utc(value).strftime("%Y-%m-%dT%H:%M:%SZ")


def _local_date(value: pd.Timestamp) -> str:
    return value.tz_localize("UTC").tz_convert(LOCAL_TIMEZONE).date().isoformat()


def _last_time(rows: pd.DataFrame, column: str) -> str:
    if rows.empty or column not in rows:
        return ""
    values = pd.to_numeric(rows[column], errors="coerce").fillna(0)
    if not values.gt(0).any():
        return ""
    timestamps = pd.to_datetime(rows.loc[values.gt(0), "timestamp_utc"], errors="coerce", utc=True).dt.tz_convert(None).dropna()
    if timestamps.empty:
        return ""
    return _iso_z(timestamps.max())


def _time_reaches(value: Any, cutoff_utc: str) -> bool:
    if value is None:
        return False
    text = str(value).strip()
    if not text or text.lower() in {"nan", "nat", "none"}:
        return False
    try:
        return _normalize_utc(text) >= _normalize_utc(cutoff_utc)
    except Exception:
        return False


def _timestamp_label(value: Any) -> str:
    return _normalize_utc(value).strftime("%Y-%m-%dT%H-%M-%SZ")


class TransportRetentionFixService:
    def __init__(self):
        self.case = get_case_context()
        if not self.case.is_official:
            raise RuntimeError("transport_retention_fix is only supported for official spill-case workflows.")
        if xr is None:
            raise ImportError("xarray is required for transport-retention sensitivity diagnostics.")
        if rasterio is None:
            raise ImportError("rasterio is required for transport-retention sensitivity diagnostics.")

        self.base_output = get_case_output_dir()
        self.output_dir = self.base_output / TRANSPORT_RETENTION_DIR_NAME
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.short_dir = self.base_output / EXTENDED_SCORED_DIR_NAME
        self.short_model_dir = self.short_dir / "model_run"
        self.grid = GridBuilder()
        self.sea_mask = load_sea_mask_array(self.grid.spec)
        self.valid_ocean = self.sea_mask > 0.5 if self.sea_mask is not None else np.ones((self.grid.height, self.grid.width), dtype=bool)
        self.scoring_helper = Phase3BScoringService(output_dir=self.output_dir / "_scratch_helper")
        self.land_tree = self._build_land_tree()
        self.window = self._load_short_window()
        self.force_rerun = os.environ.get("TRANSPORT_RETENTION_FORCE_RERUN", "").strip().lower() in {"1", "true", "yes"}

    def run(self) -> dict:
        selection = resolve_recipe_selection()
        forcing_paths = self._resolve_forcing_paths(selection.recipe)
        scenarios = self._selected_scenarios()
        scenario_results = []
        all_hourly = []
        all_fss = []
        all_diagnostics = []

        for scenario in scenarios:
            result = self._run_or_reuse_scenario(scenario, selection, forcing_paths)
            model_dir = Path(result["model_dir"])
            composite_dir = self._build_local_date_composites(scenario, model_dir)
            pairings = self._build_pairings(scenario, model_dir, composite_dir)
            fss_df, diagnostics_df = self._score_pairings(scenario, pairings)
            hourly_df = self._build_hourly_diagnostics(scenario, model_dir)
            summary = self._summarize_scenario(scenario, result, hourly_df, fss_df, diagnostics_df)
            scenario_results.append(summary)
            all_hourly.append(hourly_df)
            all_fss.append(fss_df)
            all_diagnostics.append(diagnostics_df)

        summary_df = pd.DataFrame(scenario_results)
        hourly_df = pd.concat(all_hourly, ignore_index=True) if all_hourly else pd.DataFrame()
        fss_all = pd.concat(all_fss, ignore_index=True) if all_fss else pd.DataFrame()
        diagnostics_all = pd.concat(all_diagnostics, ignore_index=True) if all_diagnostics else pd.DataFrame()
        recommendation = self._choose_recommendation(summary_df)

        paths = self._write_outputs(summary_df, hourly_df, diagnostics_all, fss_all, recommendation, forcing_paths, scenarios)
        qa_paths = self._write_qa(summary_df, hourly_df, diagnostics_all)
        report_path = self._write_report(summary_df, recommendation, paths, qa_paths)
        manifest_path = self._write_manifest(summary_df, recommendation, forcing_paths, scenarios, paths, qa_paths, report_path)

        best = recommendation["best_scenario"]
        return {
            "output_dir": self.output_dir,
            "summary_csv": paths["summary"],
            "hourly_timeseries_csv": paths["hourly_timeseries"],
            "diagnostics_csv": paths["diagnostics"],
            "fss_by_window_csv": paths["fss_by_window"],
            "report_md": report_path,
            "run_manifest": manifest_path,
            "best_scenario": best,
            "coastline_interaction_confirmed": recommendation["coastline_interaction_confirmed"],
            "medium_tier_should_remain_blocked": recommendation["medium_tier_should_remain_blocked"],
            "recommended_next_step": recommendation["recommended_next_step"],
            "scenario_table": summary_df,
        }

    def _load_short_window(self) -> dict:
        manifest_path = self.short_dir / "extended_short_run_manifest.json"
        if manifest_path.exists():
            with open(manifest_path, "r", encoding="utf-8") as handle:
                manifest = json.load(handle) or {}
            tier = manifest.get("tier_window", {})
        else:
            tier = {}
        return {
            "simulation_start_utc": tier.get("simulation_start_utc", self.case.simulation_start_utc),
            "simulation_end_utc": tier.get("simulation_end_utc", "2023-03-09T15:59:00Z"),
            "required_forcing_start_utc": tier.get("required_forcing_start_utc", self.case.simulation_start_utc),
            "required_forcing_end_utc": tier.get("required_forcing_end_utc", "2023-03-09T18:59:00Z"),
        }

    def _selected_scenarios(self) -> list[RetentionScenario]:
        requested = os.environ.get("TRANSPORT_RETENTION_SCENARIOS", "").strip()
        if not requested:
            return list(SCENARIOS)
        wanted = {item.strip().upper() for item in requested.split(",") if item.strip()}
        selected = [scenario for scenario in SCENARIOS if scenario.scenario_id.upper() in wanted]
        if not selected:
            raise ValueError(f"No transport-retention scenarios matched TRANSPORT_RETENTION_SCENARIOS={requested!r}")
        return selected

    def _resolve_forcing_paths(self, recipe_name: str) -> dict:
        forcing_dir = self.short_dir / "forcing"
        candidates = {
            "currents": forcing_dir / "cmems_curr.nc",
            "wind": forcing_dir / "era5_wind.nc",
            "wave": forcing_dir / "cmems_wave.nc",
        }
        if recipe_name.startswith("hycom"):
            candidates["currents"] = forcing_dir / "hycom_curr.nc"
        if "gfs" in recipe_name:
            candidates["wind"] = forcing_dir / "gfs_wind.nc"
        for key, path in candidates.items():
            if not path.exists():
                fallback = Path("data/forcing") / self.case.run_name / path.name
                if fallback.exists():
                    candidates[key] = fallback
                else:
                    raise FileNotFoundError(f"Missing required {key} forcing for retention sensitivity: {path}")
        return {key: str(path) for key, path in candidates.items()}

    def _run_or_reuse_scenario(self, scenario: RetentionScenario, selection, forcing_paths: dict) -> dict:
        scenario_dir = self.output_dir / scenario.output_slug
        scenario_dir.mkdir(parents=True, exist_ok=True)
        if scenario.reuse_existing_baseline:
            return {
                "scenario_id": scenario.scenario_id,
                "scenario_slug": scenario.slug,
                "status": "reused_existing_baseline",
                "model_dir": str(self.short_model_dir),
                "run_name": f"{self.case.run_name}/{EXTENDED_SCORED_DIR_NAME}/model_run",
                "forecast_result": {"status": "reused_existing_short_extended_baseline"},
            }

        model_run_name = f"{self.case.run_name}/{TRANSPORT_RETENTION_DIR_NAME}/{scenario.output_slug}/model_run"
        model_dir = self.base_output / TRANSPORT_RETENTION_DIR_NAME / scenario.output_slug / "model_run"
        member_paths = sorted((model_dir / "ensemble").glob("member_*.nc"))
        if member_paths and (model_dir / "forecast" / "forecast_manifest.json").exists() and not self.force_rerun:
            return {
                "scenario_id": scenario.scenario_id,
                "scenario_slug": scenario.slug,
                "status": "reused_existing_scenario_run",
                "model_dir": str(model_dir),
                "run_name": model_run_name,
                "forecast_result": {"status": "reused_existing_scenario_run", "member_count": len(member_paths)},
            }

        start_lat, start_lon, start_time = resolve_spill_origin()
        simulation_start = _normalize_utc(self.window["simulation_start_utc"])
        simulation_end = _normalize_utc(self.window["simulation_end_utc"])
        duration_hours = int(math.ceil((simulation_end - simulation_start).total_seconds() / 3600.0))
        snapshot_hours = sorted(set([24, 48, 72, 96, 120, 144, duration_hours]))
        result = run_official_spill_forecast(
            selection=selection,
            start_time=start_time,
            start_lat=start_lat,
            start_lon=start_lon,
            output_run_name=model_run_name,
            forcing_override=forcing_paths,
            simulation_start_utc=self.window["simulation_start_utc"],
            simulation_end_utc=self.window["simulation_end_utc"],
            snapshot_hours=snapshot_hours,
            date_composite_dates=list(EVENT_CORRIDOR_DATES),
            transport_overrides={
                "coastline_action": scenario.coastline_action,
                "coastline_approximation_precision": scenario.coastline_approximation_precision,
                "time_step_minutes": scenario.time_step_minutes,
            },
            sensitivity_context={
                "track": "transport_retention_fix",
                "scenario_id": scenario.scenario_id,
                "scenario_slug": scenario.slug,
                "diagnostic_only": scenario.diagnostic_only,
                "coastline_action": scenario.coastline_action,
                "coastline_approximation_precision": scenario.coastline_approximation_precision,
                "time_step_minutes": scenario.time_step_minutes,
            },
        )
        return {
            "scenario_id": scenario.scenario_id,
            "scenario_slug": scenario.slug,
            "status": result.get("status", "unknown"),
            "model_dir": str(model_dir),
            "run_name": model_run_name,
            "forecast_result": result,
        }

    def _build_local_date_composites(self, scenario: RetentionScenario, model_dir: Path) -> Path:
        composite_dir = self.output_dir / scenario.output_slug / "forecast_datecomposites"
        composite_dir.mkdir(parents=True, exist_ok=True)
        member_paths = sorted((model_dir / "ensemble").glob("member_*.nc"))
        if not member_paths:
            raise FileNotFoundError(f"No ensemble member NetCDFs found for {scenario.scenario_id}: {model_dir / 'ensemble'}")
        for date in EVENT_CORRIDOR_DATES:
            probability = self._date_composite_probability(member_paths, date)
            probability = apply_ocean_mask(probability, sea_mask=self.sea_mask, fill_value=0.0)
            p50 = apply_ocean_mask((probability >= 0.5).astype(np.float32), sea_mask=self.sea_mask, fill_value=0.0)
            p90 = apply_ocean_mask((probability >= 0.9).astype(np.float32), sea_mask=self.sea_mask, fill_value=0.0)
            save_raster(self.grid, probability.astype(np.float32), composite_dir / f"prob_presence_{date}_datecomposite.tif")
            save_raster(self.grid, p50.astype(np.float32), composite_dir / f"mask_p50_{date}_datecomposite.tif")
            save_raster(self.grid, p90.astype(np.float32), composite_dir / f"mask_p90_{date}_datecomposite.tif")
        return composite_dir

    def _date_composite_probability(self, member_paths: list[Path], local_date: str) -> np.ndarray:
        masks = [self._member_local_date_mask(path, local_date) for path in member_paths]
        return np.mean(np.stack(masks, axis=0), axis=0).astype(np.float32)

    def _member_local_date_mask(self, nc_path: Path, local_date: str) -> np.ndarray:
        target_date = pd.Timestamp(local_date).date()
        composite = np.zeros((self.grid.height, self.grid.width), dtype=np.float32)
        with xr.open_dataset(nc_path) as ds:
            times = normalize_time_index(ds["time"].values)
            for index, timestamp in enumerate(times):
                local_ts = pd.Timestamp(timestamp).tz_localize("UTC").tz_convert(LOCAL_TIMEZONE)
                if local_ts.date() != target_date:
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
                composite = np.maximum(composite, hits)
        return apply_ocean_mask(composite.astype(np.float32), sea_mask=self.sea_mask, fill_value=0.0)

    def _build_pairings(self, scenario: RetentionScenario, model_dir: Path, composite_dir: Path) -> list[dict]:
        obs_mask = Path("data/arcgis") / self.case.run_name / "obs_mask_2023-03-06.tif"
        if not obs_mask.exists():
            raise FileNotFoundError(f"Missing strict March 6 observed mask: {obs_mask}")
        pairings = [
            {
                "scenario_id": scenario.scenario_id,
                "scenario_slug": scenario.slug,
                "pair_id": f"{scenario.scenario_id}_strict_march6",
                "pair_role": "strict_march6",
                "obs_date": "2023-03-06",
                "forecast_path": model_dir / "ensemble" / "mask_p50_2023-03-06_datecomposite.tif",
                "observation_path": obs_mask,
                "source_semantics": "strict_single_date_stress_test",
            }
        ]
        for date in SHORT_DATES:
            pairings.append(
                {
                    "scenario_id": scenario.scenario_id,
                    "scenario_slug": scenario.slug,
                    "pair_id": f"{scenario.scenario_id}_short_date_union_{date}",
                    "pair_role": "short_extended_date_union",
                    "obs_date": date,
                    "forecast_path": composite_dir / f"mask_p50_{date}_datecomposite.tif",
                    "observation_path": self.short_dir / "date_union_obs_masks" / f"extended_obs_union_{date}.tif",
                    "source_semantics": "appendix_only_short_extended_public_validation",
                }
            )

        event_model = self._build_eventcorridor_model_union(scenario, composite_dir)
        event_obs = self.short_dir / "extended_eventcorridor_obs_union_2023-03-04_to_2023-03-09.tif"
        if not event_obs.exists():
            raise FileNotFoundError(f"Missing short-extended event-corridor observed union: {event_obs}")
        pairings.append(
            {
                "scenario_id": scenario.scenario_id,
                "scenario_slug": scenario.slug,
                "pair_id": f"{scenario.scenario_id}_eventcorridor_2023-03-04_to_2023-03-09",
                "pair_role": "short_extended_eventcorridor",
                "obs_date": "2023-03-04_to_2023-03-09",
                "forecast_path": event_model,
                "observation_path": event_obs,
                "source_semantics": "appendix_only_eventcorridor_excluding_march3_initialization",
            }
        )
        for pair in pairings:
            if not Path(pair["forecast_path"]).exists():
                raise FileNotFoundError(f"Missing forecast product for {pair['pair_id']}: {pair['forecast_path']}")
            if not Path(pair["observation_path"]).exists():
                raise FileNotFoundError(f"Missing observation product for {pair['pair_id']}: {pair['observation_path']}")
        return pairings

    def _build_eventcorridor_model_union(self, scenario: RetentionScenario, composite_dir: Path) -> Path:
        union = np.zeros((self.grid.height, self.grid.width), dtype=np.float32)
        for date in EVENT_CORRIDOR_DATES:
            mask = self._read_raster(composite_dir / f"mask_p50_{date}_datecomposite.tif")
            union = np.maximum(union, (mask > 0).astype(np.float32))
        union = apply_ocean_mask(union, sea_mask=self.sea_mask, fill_value=0.0)
        path = self.output_dir / scenario.output_slug / "eventcorridor_model_union_2023-03-04_to_2023-03-09.tif"
        save_raster(self.grid, union.astype(np.float32), path)
        return path

    def _score_pairings(self, scenario: RetentionScenario, pairings: list[dict]) -> tuple[pd.DataFrame, pd.DataFrame]:
        fss_rows: list[dict] = []
        diagnostics_rows: list[dict] = []
        precheck_dir = self.output_dir / "precheck" / scenario.output_slug
        precheck_dir.mkdir(parents=True, exist_ok=True)
        valid_mask = (self.sea_mask > 0.5) if self.sea_mask is not None else None
        for pair in pairings:
            precheck_base = precheck_dir / pair["pair_id"]
            precheck = precheck_same_grid(
                forecast=Path(pair["forecast_path"]),
                target=Path(pair["observation_path"]),
                report_base_path=precheck_base,
            )
            if not precheck.passed:
                raise RuntimeError(f"Same-grid precheck failed for {pair['pair_id']}: {precheck.json_report_path}")

            forecast_mask = self.scoring_helper._load_binary_score_mask(Path(pair["forecast_path"]))
            obs_mask = self.scoring_helper._load_binary_score_mask(Path(pair["observation_path"]))
            diagnostics = self.scoring_helper._compute_mask_diagnostics(forecast_mask, obs_mask)
            diagnostics_rows.append(
                {
                    **{key: str(value) if isinstance(value, Path) else value for key, value in pair.items()},
                    **diagnostics,
                    "precheck_csv": str(precheck.csv_report_path),
                    "precheck_json": str(precheck.json_report_path),
                }
            )
            for window_km in OFFICIAL_PHASE3B_WINDOWS_KM:
                window_cells = self.scoring_helper._window_km_to_cells(window_km)
                fss = float(
                    np.clip(
                        calculate_fss(
                            forecast_mask,
                            obs_mask,
                            window=window_cells,
                            valid_mask=valid_mask,
                        ),
                        0.0,
                        1.0,
                    )
                )
                fss_rows.append(
                    {
                        **{key: str(value) if isinstance(value, Path) else value for key, value in pair.items()},
                        "window_km": int(window_km),
                        "window_cells": int(window_cells),
                        "fss": fss,
                        "precheck_csv": str(precheck.csv_report_path),
                        "precheck_json": str(precheck.json_report_path),
                    }
                )
        return pd.DataFrame(fss_rows), pd.DataFrame(diagnostics_rows)

    def _build_hourly_diagnostics(self, scenario: RetentionScenario, model_dir: Path) -> pd.DataFrame:
        run_specs = self._scenario_run_specs(model_dir)
        expected_times = pd.date_range(
            _normalize_utc(self.window["simulation_start_utc"]),
            _normalize_utc(self.window["simulation_end_utc"]),
            freq=f"{scenario.time_step_minutes}min",
        )
        rows: list[dict] = []
        ensemble_counts = {
            _normalize_utc(timestamp): np.zeros((self.grid.height, self.grid.width), dtype=np.float32)
            for timestamp in expected_times
        }
        expected_set = set(ensemble_counts)
        for spec in run_specs:
            metrics_by_time: dict[pd.Timestamp, dict] = {}
            with xr.open_dataset(spec["path"]) as ds:
                times = normalize_time_index(ds["time"].values)
                for index, timestamp in enumerate(times):
                    timestamp = _normalize_utc(timestamp)
                    metrics = self._snapshot_metrics(ds, index)
                    metrics_by_time[timestamp] = metrics
                    if spec["run_kind"] == "ensemble_member" and timestamp in expected_set:
                        ensemble_counts[timestamp] += metrics["active_ocean_hits"]
            for timestamp in expected_times:
                timestamp = _normalize_utc(timestamp)
                row = self._empty_hourly_row(scenario, spec, timestamp)
                metrics = metrics_by_time.get(timestamp)
                if metrics is not None:
                    row.update(self._metrics_to_hourly_fields(metrics))
                    row["raw_output_exists"] = True
                rows.append(row)
        member_count = max(1, len([spec for spec in run_specs if spec["run_kind"] == "ensemble_member"]))
        for timestamp in expected_times:
            probability = ensemble_counts[_normalize_utc(timestamp)] / float(member_count)
            rows.append(
                {
                    **self._empty_aggregate_row(scenario, _normalize_utc(timestamp)),
                    **self._probability_fields(probability),
                }
            )
        return pd.DataFrame(rows)

    def _scenario_run_specs(self, model_dir: Path) -> list[dict]:
        specs = []
        control = model_dir / "forecast" / "deterministic_control_cmems_era5.nc"
        if control.exists():
            specs.append({"run_id": "deterministic_control", "run_kind": "deterministic_control", "member_id": "", "path": control})
        for path in sorted((model_dir / "ensemble").glob("member_*.nc")):
            label = path.stem.replace("member_", "")
            specs.append(
                {
                    "run_id": f"member_{label}",
                    "run_kind": "ensemble_member",
                    "member_id": int(label) if label.isdigit() else "",
                    "path": path,
                }
            )
        if not specs:
            raise FileNotFoundError(f"No run NetCDFs found under {model_dir}")
        return specs

    def _snapshot_metrics(self, ds, time_index: int) -> dict:
        lon = np.asarray(ds["lon"].isel(time=time_index).values).reshape(-1)
        lat = np.asarray(ds["lat"].isel(time=time_index).values).reshape(-1)
        status = np.asarray(ds["status"].isel(time=time_index).values).reshape(-1)
        moving = np.asarray(ds["moving"].isel(time=time_index).values).reshape(-1) if "moving" in ds else np.full_like(status, np.nan)
        finite = np.isfinite(lon) & np.isfinite(lat) & np.isfinite(status)
        active = finite & (status == 0)
        stranded = finite & (status == 1)
        moving_mask = finite & (moving == 1)
        active_inside, active_ocean, active_x, active_y = self._point_domain_ocean_flags(lon[active], lat[active])
        stranded_inside, stranded_ocean, stranded_x, stranded_y = self._point_domain_ocean_flags(lon[stranded], lat[stranded])

        active_ocean_hits = np.zeros((self.grid.height, self.grid.width), dtype=np.float32)
        active_domain_hits = np.zeros((self.grid.height, self.grid.width), dtype=np.float32)
        if np.any(active):
            hits, _ = rasterize_particles(
                self.grid,
                lon[active],
                lat[active],
                np.ones(int(np.count_nonzero(active)), dtype=np.float32),
            )
            active_domain_hits = hits.astype(np.float32)
            active_ocean_hits = apply_ocean_mask(hits, sea_mask=self.sea_mask, fill_value=0.0).astype(np.float32)

        active_distance = self._distance_to_land(active_x[active_inside], active_y[active_inside])
        stranded_distance = self._distance_to_land(stranded_x[stranded_inside], stranded_y[stranded_inside])
        centroid_x = float(np.nanmean(active_x[active_ocean])) if active_x.size and np.any(active_ocean) else np.nan
        centroid_y = float(np.nanmean(active_y[active_ocean])) if active_y.size and np.any(active_ocean) else np.nan
        return {
            "finite_count": int(np.count_nonzero(finite)),
            "active_count": int(np.count_nonzero(active)),
            "stranded_count": int(np.count_nonzero(stranded)),
            "moving_count": int(np.count_nonzero(moving_mask)),
            "active_inside_domain_count": int(np.count_nonzero(active_inside)),
            "active_outside_domain_count": int(np.count_nonzero(active) - np.count_nonzero(active_inside)),
            "active_ocean_particle_count": int(np.count_nonzero(active_ocean)),
            "active_land_or_invalid_ocean_particle_count": int(np.count_nonzero(active_inside) - np.count_nonzero(active_ocean)),
            "stranded_inside_domain_count": int(np.count_nonzero(stranded_inside)),
            "stranded_ocean_particle_count": int(np.count_nonzero(stranded_ocean)),
            "surface_presence_cells": int(np.count_nonzero(active_ocean_hits > 0)),
            "domain_presence_cells": int(np.count_nonzero(active_domain_hits > 0)),
            "active_centroid_x": centroid_x,
            "active_centroid_y": centroid_y,
            "active_min_distance_to_land_m": active_distance["min"],
            "active_mean_distance_to_land_m": active_distance["mean"],
            "stranded_min_distance_to_land_m": stranded_distance["min"],
            "stranded_mean_distance_to_land_m": stranded_distance["mean"],
            "active_ocean_hits": active_ocean_hits,
        }

    def _point_domain_ocean_flags(self, lon: np.ndarray, lat: np.ndarray) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
        if lon.size == 0:
            empty_bool = np.zeros(0, dtype=bool)
            empty_float = np.zeros(0, dtype=float)
            return empty_bool, empty_bool, empty_float, empty_float
        x_vals, y_vals = project_points_to_grid(self.grid, lon, lat)
        col = np.floor((x_vals - self.grid.min_x) / self.grid.resolution).astype(int)
        row = np.floor((self.grid.max_y - y_vals) / self.grid.resolution).astype(int)
        inside = (col >= 0) & (col < self.grid.width) & (row >= 0) & (row < self.grid.height)
        ocean = np.zeros(lon.shape, dtype=bool)
        if np.any(inside):
            ocean[inside] = self.valid_ocean[row[inside], col[inside]]
        return inside, ocean, x_vals, y_vals

    def _build_land_tree(self):
        if cKDTree is None or self.sea_mask is None:
            return None
        land = np.argwhere(self.sea_mask <= 0.5)
        if land.size == 0:
            return None
        x = self.grid.min_x + (land[:, 1] + 0.5) * self.grid.resolution
        y = self.grid.max_y - (land[:, 0] + 0.5) * self.grid.resolution
        return cKDTree(np.column_stack([x, y]))

    def _distance_to_land(self, x_vals: np.ndarray, y_vals: np.ndarray) -> dict:
        if self.land_tree is None or x_vals.size == 0:
            return {"min": np.nan, "mean": np.nan}
        distances, _ = self.land_tree.query(np.column_stack([x_vals, y_vals]), k=1)
        return {"min": float(np.nanmin(distances)), "mean": float(np.nanmean(distances))}

    def _empty_hourly_row(self, scenario: RetentionScenario, spec: dict, timestamp: pd.Timestamp) -> dict:
        return {
            "scenario_id": scenario.scenario_id,
            "scenario_slug": scenario.slug,
            "run_id": spec["run_id"],
            "run_kind": spec["run_kind"],
            "member_id": spec["member_id"],
            "timestamp_utc": _iso_z(timestamp),
            "timestamp_local_date": _local_date(timestamp),
            "raw_output_exists": False,
            "finite_count": 0,
            "active_count": 0,
            "stranded_count": 0,
            "moving_count": 0,
            "active_inside_domain_count": 0,
            "active_outside_domain_count": 0,
            "active_ocean_particle_count": 0,
            "active_land_or_invalid_ocean_particle_count": 0,
            "stranded_inside_domain_count": 0,
            "stranded_ocean_particle_count": 0,
            "surface_presence_cells": 0,
            "domain_presence_cells": 0,
            "active_centroid_x": np.nan,
            "active_centroid_y": np.nan,
            "active_min_distance_to_land_m": np.nan,
            "active_mean_distance_to_land_m": np.nan,
            "stranded_min_distance_to_land_m": np.nan,
            "stranded_mean_distance_to_land_m": np.nan,
            "max_prob_presence": np.nan,
            "prob_presence_nonzero_cells": np.nan,
            "p50_nonzero_cells": np.nan,
            "p90_nonzero_cells": np.nan,
        }

    def _empty_aggregate_row(self, scenario: RetentionScenario, timestamp: pd.Timestamp) -> dict:
        row = self._empty_hourly_row(
            scenario,
            {"run_id": "ensemble_aggregate", "run_kind": "ensemble_aggregate", "member_id": ""},
            timestamp,
        )
        row["raw_output_exists"] = ""
        return row

    @staticmethod
    def _metrics_to_hourly_fields(metrics: dict) -> dict:
        return {key: value for key, value in metrics.items() if key != "active_ocean_hits"}

    @staticmethod
    def _probability_fields(probability: np.ndarray) -> dict:
        arr = np.asarray(probability, dtype=np.float32)
        return {
            "max_prob_presence": float(np.nanmax(arr)) if arr.size else 0.0,
            "prob_presence_nonzero_cells": int(np.count_nonzero(arr > 0)),
            "cells_ge_0p10": int(np.count_nonzero(arr >= 0.10)),
            "cells_ge_0p20": int(np.count_nonzero(arr >= 0.20)),
            "cells_ge_0p30": int(np.count_nonzero(arr >= 0.30)),
            "cells_ge_0p40": int(np.count_nonzero(arr >= 0.40)),
            "cells_ge_0p50": int(np.count_nonzero(arr >= 0.50)),
            "cells_ge_0p90": int(np.count_nonzero(arr >= 0.90)),
            "p50_nonzero_cells": int(np.count_nonzero(arr >= 0.50)),
            "p90_nonzero_cells": int(np.count_nonzero(arr >= 0.90)),
        }

    @staticmethod
    def _read_raster(path: Path) -> np.ndarray:
        with rasterio.open(path) as src:
            return src.read(1).astype(np.float32)

    def _summarize_scenario(
        self,
        scenario: RetentionScenario,
        run_result: dict,
        hourly_df: pd.DataFrame,
        fss_df: pd.DataFrame,
        diagnostics_df: pd.DataFrame,
    ) -> dict:
        deterministic = hourly_df[hourly_df["run_kind"] == "deterministic_control"].copy()
        aggregate = hourly_df[hourly_df["run_kind"] == "ensemble_aggregate"].copy()
        members = hourly_df[hourly_df["run_kind"] == "ensemble_member"].copy()
        for frame in [deterministic, aggregate, members]:
            for column in [
                "active_count",
                "stranded_count",
                "active_ocean_particle_count",
                "surface_presence_cells",
                "max_prob_presence",
                "prob_presence_nonzero_cells",
                "p50_nonzero_cells",
                "p90_nonzero_cells",
            ]:
                if column in frame:
                    frame[column] = pd.to_numeric(frame[column], errors="coerce")

        strict = fss_df[fss_df["pair_role"] == "strict_march6"]
        event = fss_df[fss_df["pair_role"] == "short_extended_eventcorridor"]
        short = fss_df[fss_df["pair_role"] == "short_extended_date_union"]
        strict_diag = diagnostics_df[diagnostics_df["pair_role"] == "strict_march6"].iloc[0].to_dict()
        event_diag = diagnostics_df[diagnostics_df["pair_role"] == "short_extended_eventcorridor"].iloc[0].to_dict()
        terminal = members.sort_values("timestamp_utc").groupby("run_id").tail(1)
        terminal_stranding_fraction = float((pd.to_numeric(terminal["stranded_count"], errors="coerce").fillna(0) > 0).mean()) if not terminal.empty else np.nan
        terminal_active_fraction = float((pd.to_numeric(terminal["active_count"], errors="coerce").fillna(0) > 0).mean()) if not terminal.empty else np.nan
        last_raw_active = _last_time(members, "active_count")
        last_deterministic_footprint = _last_time(deterministic, "surface_presence_cells")
        last_prob_presence = _last_time(aggregate, "prob_presence_nonzero_cells")
        last_mask_p50 = _last_time(aggregate, "p50_nonzero_cells")
        last_mask_p90 = _last_time(aggregate, "p90_nonzero_cells")

        row = {
            "scenario_id": scenario.scenario_id,
            "scenario_slug": scenario.slug,
            "description": scenario.description,
            "diagnostic_only": scenario.diagnostic_only,
            "run_status": run_result.get("status", ""),
            "model_dir": run_result.get("model_dir", ""),
            "transport_model": "oceandrift",
            "provisional_transport_model": True,
            "coastline_action": scenario.coastline_action,
            "coastline_approximation_precision": scenario.coastline_approximation_precision,
            "time_step_minutes": scenario.time_step_minutes,
            "recipe_used": resolve_recipe_selection().recipe,
            "element_count_used": self.case.forecast_config.get("element_count", 5000) if hasattr(self.case, "forecast_config") else 5000,
            "shoreline_mask_signature": self.grid.spec.shoreline_mask_signature,
            "terminal_stranding_fraction": terminal_stranding_fraction,
            "terminal_active_fraction": terminal_active_fraction,
            "last_raw_active_time_utc": last_raw_active,
            "last_nonzero_deterministic_footprint_utc": last_deterministic_footprint,
            "last_nonzero_prob_presence_utc": last_prob_presence,
            "last_nonzero_mask_p50_utc": last_mask_p50,
            "last_nonzero_mask_p90_utc": last_mask_p90,
            "survives_to_strict_validation": _time_reaches(last_raw_active, STRICT_VALIDATION_TIME_UTC),
            "prob_presence_to_strict_validation": _time_reaches(last_prob_presence, STRICT_VALIDATION_TIME_UTC),
            "p50_signal_on_march6": _time_reaches(last_mask_p50, MARCH6_DATE_START_UTC),
            "max_prob_presence": float(pd.to_numeric(aggregate["max_prob_presence"], errors="coerce").fillna(0).max()) if not aggregate.empty else 0.0,
            "strict_march6_forecast_nonzero_cells": strict_diag.get("forecast_nonzero_cells", np.nan),
            "strict_march6_obs_nonzero_cells": strict_diag.get("obs_nonzero_cells", np.nan),
            "strict_march6_iou": strict_diag.get("iou", np.nan),
            "strict_march6_dice": strict_diag.get("dice", np.nan),
            "strict_march6_centroid_distance_m": strict_diag.get("centroid_distance_m", np.nan),
            "eventcorridor_forecast_nonzero_cells": event_diag.get("forecast_nonzero_cells", np.nan),
            "eventcorridor_obs_nonzero_cells": event_diag.get("obs_nonzero_cells", np.nan),
            "eventcorridor_iou": event_diag.get("iou", np.nan),
            "eventcorridor_dice": event_diag.get("dice", np.nan),
            "eventcorridor_centroid_distance_m": event_diag.get("centroid_distance_m", np.nan),
        }
        for window in OFFICIAL_PHASE3B_WINDOWS_KM:
            row[f"strict_march6_fss_{window}km"] = self._fss_value(strict, window)
            row[f"eventcorridor_fss_{window}km"] = self._fss_value(event, window)
            row[f"short_extended_mean_fss_{window}km"] = float(short.loc[short["window_km"] == window, "fss"].mean()) if not short.empty else np.nan
        row["mean_strict_fss"] = float(np.nanmean([row[f"strict_march6_fss_{w}km"] for w in OFFICIAL_PHASE3B_WINDOWS_KM]))
        row["mean_eventcorridor_fss"] = float(np.nanmean([row[f"eventcorridor_fss_{w}km"] for w in OFFICIAL_PHASE3B_WINDOWS_KM]))
        return row

    @staticmethod
    def _role_prefix(window_role=None) -> str:
        return ""

    @staticmethod
    def _fss_value(df: pd.DataFrame, window: int) -> float:
        if df.empty:
            return np.nan
        values = df.loc[df["window_km"] == window, "fss"]
        if values.empty:
            return np.nan
        return float(values.iloc[0])

    def _choose_recommendation(self, summary_df: pd.DataFrame) -> dict:
        def _as_bool(series: pd.Series) -> pd.Series:
            return series.map(lambda value: str(value).strip().lower() in {"1", "true", "yes"} if pd.notna(value) else False)

        diagnostic = _as_bool(summary_df.get("diagnostic_only", pd.Series(False, index=summary_df.index)))
        candidates = summary_df[~diagnostic].copy()
        if candidates.empty:
            best = "none"
        else:
            for column in ["survives_to_strict_validation", "prob_presence_to_strict_validation", "p50_signal_on_march6"]:
                if column not in candidates:
                    candidates[column] = False
                candidates[column] = _as_bool(candidates[column]).astype(int)
            candidates["terminal_active_fraction"] = pd.to_numeric(candidates["terminal_active_fraction"], errors="coerce").fillna(0)
            candidates["mean_eventcorridor_fss"] = pd.to_numeric(candidates["mean_eventcorridor_fss"], errors="coerce").fillna(0)
            candidates["strict_march6_fss_10km"] = pd.to_numeric(candidates["strict_march6_fss_10km"], errors="coerce").fillna(0)
            candidates["eventcorridor_forecast_nonzero_cells"] = pd.to_numeric(candidates["eventcorridor_forecast_nonzero_cells"], errors="coerce").fillna(0)
            best = str(
                candidates.sort_values(
                    [
                        "survives_to_strict_validation",
                        "prob_presence_to_strict_validation",
                        "p50_signal_on_march6",
                        "terminal_active_fraction",
                        "mean_eventcorridor_fss",
                        "strict_march6_fss_10km",
                        "eventcorridor_forecast_nonzero_cells",
                    ],
                    ascending=False,
                ).iloc[0]["scenario_id"]
            )
        r3 = summary_df[summary_df["scenario_id"] == "R3"]
        baseline = summary_df[summary_df["scenario_id"] == "R0"]
        r3_survives_to_strict = False
        if not r3.empty:
            r3_row = r3.iloc[0]
            r3_survives_to_strict = _time_reaches(r3_row.get("last_raw_active_time_utc", ""), STRICT_VALIDATION_TIME_UTC) and _time_reaches(
                r3_row.get("last_nonzero_prob_presence_utc", ""), STRICT_VALIDATION_TIME_UTC
            )
        baseline_dies_before_strict = False
        if not baseline.empty:
            baseline_row = baseline.iloc[0]
            baseline_dies_before_strict = not (
                _time_reaches(baseline_row.get("last_raw_active_time_utc", ""), STRICT_VALIDATION_TIME_UTC)
                and _time_reaches(baseline_row.get("last_nonzero_prob_presence_utc", ""), STRICT_VALIDATION_TIME_UTC)
            )
        selectable_survives_to_strict = False
        if not candidates.empty:
            selected_rows = candidates[candidates["scenario_id"].astype(str) == best]
            if not selected_rows.empty:
                selected_row = selected_rows.iloc[0]
                selectable_survives_to_strict = bool(selected_row.get("survives_to_strict_validation", 0)) and bool(
                    selected_row.get("prob_presence_to_strict_validation", 0)
                )
        return {
            "best_scenario": best,
            "coastline_interaction_confirmed": bool(baseline_dies_before_strict and (r3_survives_to_strict or selectable_survives_to_strict)),
            "medium_tier_should_remain_blocked": True,
            "recommended_next_step": "official rerun with the selected retention configuration",
            "r3_diagnostic_only": True,
            "reason": (
                "Survival through the strict March 6 validation time is the primary retention gate; "
                "FSS and event-corridor area are secondary tie-breakers. R3 remains diagnostic-only and cannot be selected."
            ),
        }

    def _write_outputs(
        self,
        summary_df: pd.DataFrame,
        hourly_df: pd.DataFrame,
        diagnostics_df: pd.DataFrame,
        fss_df: pd.DataFrame,
        recommendation: dict,
        forcing_paths: dict,
        scenarios: list[RetentionScenario],
    ) -> dict[str, Path]:
        paths = {
            "summary": self.output_dir / "transport_retention_summary.csv",
            "hourly_timeseries": self.output_dir / "transport_retention_hourly_timeseries.csv",
            "diagnostics": self.output_dir / "transport_retention_diagnostics.csv",
            "fss_by_window": self.output_dir / "transport_retention_fss_by_window.csv",
        }
        summary_df.to_csv(paths["summary"], index=False)
        hourly_df.to_csv(paths["hourly_timeseries"], index=False)
        diagnostics_df.to_csv(paths["diagnostics"], index=False)
        fss_df.to_csv(paths["fss_by_window"], index=False)
        return paths

    def _write_manifest(
        self,
        summary_df: pd.DataFrame,
        recommendation: dict,
        forcing_paths: dict,
        scenarios: list[RetentionScenario],
        paths: dict[str, Path],
        qa_paths: dict[str, Path],
        report_path: Path,
    ) -> Path:
        manifest_path = self.output_dir / "transport_retention_run_manifest.json"
        shoreline_manifest = Path(self.grid.spec.shoreline_mask_manifest_json_path or "data_processed/grids/shoreline_mask_manifest.json")
        payload = {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "workflow_mode": self.case.workflow_mode,
            "run_name": self.case.run_name,
            "phase": "transport_retention_fix",
            "purpose": "diagnose and improve early coastal stranding/beaching that kills active particles before March 6",
            "window": self.window,
            "non_negotiable_guardrails": {
                "strict_march6_semantics_unchanged": True,
                "within_horizon_public_outputs_unchanged": True,
                "short_extended_outputs_unchanged": True,
                "medium_tier_not_run": True,
                "scoring_rules_not_loosened": True,
            },
            "forcing_paths": forcing_paths,
            "shoreline_mask_manifest": str(shoreline_manifest),
            "grid": self.grid.spec.to_metadata(),
            "scenarios": [scenario.__dict__ for scenario in scenarios],
            "recommendation": recommendation,
            "summary": summary_df.to_dict(orient="records"),
            "artifacts": {**{key: str(value) for key, value in paths.items()}, **{key: str(value) for key, value in qa_paths.items()}, "report": str(report_path)},
        }
        _write_json(manifest_path, payload)
        return manifest_path

    def _write_report(self, summary_df: pd.DataFrame, recommendation: dict, paths: dict[str, Path], qa_paths: dict[str, Path]) -> Path:
        path = self.output_dir / "transport_retention_report.md"
        lines = [
            "# Transport Retention Sensitivity",
            "",
            "This appendix-only diagnostic run tests whether OpenDrift coastline interaction is the dominant kill switch for the Mindoro forecast signal.",
            "",
            f"- Best non-diagnostic scenario: {recommendation['best_scenario']}",
            f"- Coastline interaction confirmed: {recommendation['coastline_interaction_confirmed']}",
            f"- Medium tier remains blocked: {recommendation['medium_tier_should_remain_blocked']}",
            f"- Recommended next step: {recommendation['recommended_next_step']}",
            "",
            "R3 uses `general:coastline_action=none` and is diagnostic only. It must not be presented as a thesis-final scientific configuration.",
            "",
            "## Scenario Summary",
            "",
        ]
        cols = [
            "scenario_id",
            "coastline_action",
            "time_step_minutes",
            "terminal_stranding_fraction",
            "last_raw_active_time_utc",
            "last_nonzero_prob_presence_utc",
            "last_nonzero_mask_p50_utc",
            "strict_march6_fss_10km",
            "mean_eventcorridor_fss",
        ]
        lines.extend(self._markdown_table(summary_df[cols]))
        lines.extend(
            [
                "",
                "Artifacts:",
                f"- Summary CSV: {paths['summary']}",
                f"- Hourly diagnostics: {paths['hourly_timeseries']}",
                f"- FSS by window: {paths['fss_by_window']}",
                f"- Diagnostics: {paths['diagnostics']}",
            ]
        )
        for label, qa_path in qa_paths.items():
            lines.append(f"- {label}: {qa_path}")
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return path

    @staticmethod
    def _markdown_table(df: pd.DataFrame) -> list[str]:
        if df.empty:
            return ["No scenario rows were written."]
        columns = list(df.columns)
        lines = [
            "| " + " | ".join(columns) + " |",
            "| " + " | ".join(["---"] * len(columns)) + " |",
        ]
        for _, row in df.iterrows():
            lines.append("| " + " | ".join(str(row.get(column, "")) for column in columns) + " |")
        return lines

    def _write_qa(self, summary_df: pd.DataFrame, hourly_df: pd.DataFrame, diagnostics_df: pd.DataFrame) -> dict[str, Path]:
        outputs: dict[str, Path] = {}
        if plt is None:
            return outputs
        timeseries = self._plot_timeseries(hourly_df)
        tracks = self._plot_tracks()
        overlays = self._plot_overlays(diagnostics_df)
        if timeseries:
            outputs["qa_transport_retention_timeseries"] = timeseries
        if tracks:
            outputs["qa_transport_retention_tracks"] = tracks
        if overlays:
            outputs["qa_transport_retention_overlays"] = overlays
        return outputs

    def _plot_timeseries(self, hourly_df: pd.DataFrame) -> Path | None:
        if hourly_df.empty:
            return None
        path = self.output_dir / "qa_transport_retention_timeseries.png"
        aggregate = hourly_df[hourly_df["run_kind"] == "ensemble_aggregate"].copy()
        deterministic = hourly_df[hourly_df["run_kind"] == "deterministic_control"].copy()
        for frame in [aggregate, deterministic]:
            frame["timestamp"] = pd.to_datetime(frame["timestamp_utc"], errors="coerce", utc=True).dt.tz_convert(None)
            for column in ["max_prob_presence", "p50_nonzero_cells", "active_count", "stranded_count", "surface_presence_cells"]:
                if column in frame:
                    frame[column] = pd.to_numeric(frame[column], errors="coerce")
        fig, axes = plt.subplots(3, 1, figsize=(12, 10), sharex=True)
        for scenario_id, group in aggregate.groupby("scenario_id"):
            axes[0].plot(group["timestamp"], group["max_prob_presence"], label=scenario_id)
            axes[1].plot(group["timestamp"], group["p50_nonzero_cells"], label=scenario_id)
        for scenario_id, group in deterministic.groupby("scenario_id"):
            axes[2].plot(group["timestamp"], group["surface_presence_cells"], label=scenario_id)
        axes[0].set_ylabel("max prob_presence")
        axes[1].set_ylabel("p50 cells")
        axes[2].set_ylabel("det footprint cells")
        axes[2].set_xlabel("UTC time")
        for ax in axes:
            ax.legend(loc="upper right")
            ax.grid(alpha=0.2)
        fig.autofmt_xdate()
        fig.tight_layout()
        fig.savefig(path, dpi=150, bbox_inches="tight")
        plt.close(fig)
        return path

    def _plot_tracks(self) -> Path | None:
        path = self.output_dir / "qa_transport_retention_tracks.png"
        fig, ax = plt.subplots(figsize=(8, 8))
        ax.imshow(
            self.valid_ocean,
            cmap="Blues",
            alpha=0.25,
            extent=[self.grid.min_x, self.grid.max_x, self.grid.min_y, self.grid.max_y],
            origin="upper",
        )
        for scenario in SCENARIOS:
            model_dir = self.short_model_dir if scenario.reuse_existing_baseline else self.output_dir / scenario.output_slug / "model_run"
            member = next(iter(sorted((model_dir / "ensemble").glob("member_*.nc"))), None)
            if member is None:
                continue
            with xr.open_dataset(member) as ds:
                lon = np.asarray(ds["lon"].values)
                lat = np.asarray(ds["lat"].values)
                if lon.ndim != 2:
                    continue
                trajectories = np.linspace(0, lon.shape[0] - 1, min(20, lon.shape[0]), dtype=int)
                for idx in trajectories:
                    finite = np.isfinite(lon[idx]) & np.isfinite(lat[idx])
                    if not np.any(finite):
                        continue
                    x_vals, y_vals = project_points_to_grid(self.grid, lon[idx][finite], lat[idx][finite])
                    ax.plot(x_vals, y_vals, linewidth=0.5, alpha=0.2, label=scenario.scenario_id if idx == trajectories[0] else None)
        ax.set_title("Sample member tracks by retention scenario")
        ax.set_xlim(self.grid.min_x, self.grid.max_x)
        ax.set_ylim(self.grid.min_y, self.grid.max_y)
        ax.set_aspect("equal")
        ax.legend(loc="upper right")
        fig.tight_layout()
        fig.savefig(path, dpi=150, bbox_inches="tight")
        plt.close(fig)
        return path

    def _plot_overlays(self, diagnostics_df: pd.DataFrame) -> Path | None:
        event = diagnostics_df[diagnostics_df["pair_role"] == "short_extended_eventcorridor"].copy()
        if event.empty:
            return None
        path = self.output_dir / "qa_transport_retention_overlays.png"
        fig, axes = plt.subplots(1, len(event), figsize=(5 * len(event), 5))
        if len(event) == 1:
            axes = [axes]
        for ax, (_, row) in zip(axes, event.iterrows()):
            forecast = self.scoring_helper._load_binary_score_mask(Path(row["forecast_path"]))
            obs = self.scoring_helper._load_binary_score_mask(Path(row["observation_path"]))
            canvas = np.ones((forecast.shape[0], forecast.shape[1], 3), dtype=np.float32)
            canvas[obs > 0] = [0.1, 0.35, 0.95]
            canvas[forecast > 0] = [0.95, 0.35, 0.1]
            canvas[(obs > 0) & (forecast > 0)] = [0.1, 0.65, 0.25]
            ax.imshow(canvas)
            ax.set_title(f"{row['scenario_id']} event corridor")
            ax.axis("off")
        fig.tight_layout()
        fig.savefig(path, dpi=150, bbox_inches="tight")
        plt.close(fig)
        return path


def run_transport_retention_fix() -> dict:
    return TransportRetentionFixService().run()


if __name__ == "__main__":  # pragma: no cover
    print(json.dumps(run_transport_retention_fix(), indent=2, default=_json_default))
