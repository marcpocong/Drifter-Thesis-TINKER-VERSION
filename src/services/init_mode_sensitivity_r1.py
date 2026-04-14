"""Initialization-mode sensitivity under the selected R1 retention configuration."""

from __future__ import annotations

import json
import math
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from src.core.case_context import get_case_context
from src.helpers.raster import project_points_to_grid, rasterize_particles, save_raster
from src.helpers.scoring import apply_ocean_mask
from src.services.ensemble import normalize_time_index, run_official_spill_forecast
from src.services.phase3b_multidate_public import (
    format_phase3b_multidate_eventcorridor_label,
    load_phase3b_multidate_validation_dates,
)
from src.services.official_rerun_r1 import OFFICIAL_RERUN_R1_DIR_NAME, load_official_retention_config
from src.services.scoring import OFFICIAL_PHASE3B_WINDOWS_KM
from src.services.transport_retention_fix import (
    STRICT_VALIDATION_TIME_UTC,
    TRANSPORT_RETENTION_DIR_NAME,
    RetentionScenario,
    TransportRetentionFixService,
    _json_default,
    _normalize_utc,
    _time_reaches,
    _write_json,
)
from src.utils.io import get_case_output_dir, model_dir_complete_for_recipe, resolve_recipe_selection, resolve_spill_origin

try:
    import matplotlib.pyplot as plt
except ImportError:  # pragma: no cover
    plt = None

try:
    import xarray as xr
except ImportError:  # pragma: no cover
    xr = None


INIT_MODE_SENSITIVITY_DIR_NAME = "init_mode_sensitivity_r1"
VALIDATION_DATES = ["2023-03-04", "2023-03-05", "2023-03-06"]
FORCE_RERUN_ENV = "INIT_MODE_SENSITIVITY_R1_FORCE_RERUN"


@dataclass(frozen=True)
class InitBranch:
    branch_id: str
    initialization_mode: str
    description: str
    source_geometry_label: str
    seed_overrides: dict[str, Any]
    reuse_official_rerun_r1: bool = False

    @property
    def output_slug(self) -> str:
        return f"{self.branch_id}_{self.initialization_mode}"


BRANCHES = [
    InitBranch(
        branch_id="B",
        initialization_mode="observation_initialized_polygon",
        description="Current official branch: particles seeded across the processed March 3 initialization polygon.",
        source_geometry_label="processed_march3_initialization_polygon",
        seed_overrides={},
        reuse_official_rerun_r1=True,
    ),
    InitBranch(
        branch_id="A1",
        initialization_mode="source_point_initialized_same_start",
        description="Sensitivity branch: same March 3 start time, but active release uses the ArcGIS provenance source point.",
        source_geometry_label="processed_source_point_exact",
        seed_overrides={
            "initialization_mode": "source_point_initialized_same_start",
            "point_release_surrogate": "exact_point_release",
        },
    ),
]


def _read_json(path: Path) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"Required JSON artifact not found: {path}")
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle) or {}


def _read_csv(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_csv(path)


def _truthy(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _float_or_nan(value: Any) -> float:
    try:
        if value in ("", None):
            return np.nan
        return float(value)
    except (TypeError, ValueError):
        return np.nan


def _write_csv(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False)


def recommend_initialization_strategy(summary_df: pd.DataFrame) -> dict:
    strict = summary_df[summary_df["pair_role"] == "strict_march6"].copy()
    event = summary_df[summary_df["pair_role"] == "eventcorridor_march4_6"].copy()
    if strict.empty or not {"A1", "B"}.issubset(set(strict["branch_id"].astype(str))):
        return {
            "recommended_initialization_strategy": "keep both as distinct tracks",
            "a2_source_history_reconstruction_worth_attempting": True,
            "reason": "Both A1 and B strict March 6 rows are required for a decisive branch recommendation.",
        }

    rows = {str(row["branch_id"]): row for _, row in strict.iterrows()}

    def score(row: pd.Series) -> tuple[float, float, float, float]:
        fss = float(np.nanmean([_float_or_nan(row.get(f"fss_{window}km")) for window in OFFICIAL_PHASE3B_WINDOWS_KM]))
        iou = _float_or_nan(row.get("iou"))
        distance = _float_or_nan(row.get("nearest_distance_to_obs_m"))
        cells = _float_or_nan(row.get("forecast_nonzero_cells"))
        return fss, iou, -distance if np.isfinite(distance) else -1.0e12, cells

    a1_score = score(rows["A1"])
    b_score = score(rows["B"])
    event_rows = {str(row["branch_id"]): row for _, row in event.iterrows()}
    a1_event_score = score(event_rows["A1"]) if {"A1", "B"}.issubset(set(event.get("branch_id", pd.Series(dtype=str)).astype(str))) else None
    b_event_score = score(event_rows["B"]) if a1_event_score is not None else None

    if a1_score > b_score and (a1_event_score is None or a1_event_score >= b_event_score):
        return {
            "recommended_initialization_strategy": "promote A1 as the stronger main case-definition candidate",
            "a2_source_history_reconstruction_worth_attempting": True,
            "reason": "A1 improves strict March 6 overlap/displacement relative to B while holding R1 transport fixed.",
        }
    if b_score > a1_score and (a1_event_score is None or b_event_score >= a1_event_score):
        return {
            "recommended_initialization_strategy": "keep B as the main case-definition",
            "a2_source_history_reconstruction_worth_attempting": True,
            "reason": "B remains stronger than the same-start source-point sensitivity under the selected R1 transport setting.",
        }
    return {
        "recommended_initialization_strategy": "keep both as distinct tracks",
        "a2_source_history_reconstruction_worth_attempting": True,
        "reason": (
            "The initialization sensitivity is mixed across strict single-date and event-corridor diagnostics; "
            "keep observation-initialized and source-point-initialized reconstruction tracks separate."
        ),
    }


class InitModeSensitivityR1Service:
    def __init__(self):
        self.case = get_case_context()
        if not self.case.is_official:
            raise RuntimeError("init_mode_sensitivity_r1 is only supported for official Mindoro workflows.")
        if xr is None:
            raise ImportError("xarray is required for init_mode_sensitivity_r1.")
        self.case_output = get_case_output_dir(self.case.run_name)
        self.output_dir = self.case_output / INIT_MODE_SENSITIVITY_DIR_NAME
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.force_rerun = _truthy(os.environ.get(FORCE_RERUN_ENV, ""))
        self.validation_dates = load_phase3b_multidate_validation_dates(
            self.case_output,
            fallback_dates=VALIDATION_DATES,
        )
        self.eventcorridor_label = format_phase3b_multidate_eventcorridor_label(self.validation_dates)
        self.retention_config = load_official_retention_config()
        if self.retention_config["selected_mode"] != "R1":
            raise RuntimeError("init_mode_sensitivity_r1 requires official_retention.selected_mode=R1.")
        self.retention = TransportRetentionFixService()
        self.retention.output_dir = self.output_dir
        self.retention_scenario = RetentionScenario(
            scenario_id="R1",
            slug="selected_previous",
            description="Selected R1 retention configuration: coastline_action=previous.",
            coastline_action=self.retention_config["coastline_action"],
            coastline_approximation_precision=self.retention_config["coastline_approximation_precision"],
            time_step_minutes=self.retention_config["time_step_minutes"],
            diagnostic_only=False,
        )

    def run(self) -> dict:
        transport_manifest = _read_json(self.case_output / TRANSPORT_RETENTION_DIR_NAME / "transport_retention_run_manifest.json")
        official_rerun_manifest = _read_json(self.case_output / OFFICIAL_RERUN_R1_DIR_NAME / "official_rerun_r1_run_manifest.json")
        self._validate_inputs(transport_manifest, official_rerun_manifest)
        forcing_paths = (official_rerun_manifest.get("provenance") or {}).get("forcing_paths") or transport_manifest.get("forcing_paths") or {}

        branch_rows: list[dict] = []
        all_pairings: list[pd.DataFrame] = []
        all_fss: list[pd.DataFrame] = []
        all_diagnostics: list[pd.DataFrame] = []
        all_hourly: list[pd.DataFrame] = []

        for branch in BRANCHES:
            run_result = self._resolve_or_run_branch(branch, forcing_paths)
            model_dir = Path(run_result["model_dir"])
            composite_dir = self._build_utc_date_composites(branch, model_dir)
            pairings = self._build_pairings(branch, composite_dir)
            pairing_df = pd.DataFrame(pairings)
            fss_df, diagnostics_df = self.retention._score_pairings(self.retention_scenario, pairings)
            diagnostics_df = self._augment_probability_diagnostics(branch, diagnostics_df, composite_dir)
            hourly_df = self.retention._build_hourly_diagnostics(
                RetentionScenario(
                    scenario_id=branch.branch_id,
                    slug=branch.initialization_mode,
                    description=branch.description,
                    coastline_action=self.retention_scenario.coastline_action,
                    coastline_approximation_precision=self.retention_scenario.coastline_approximation_precision,
                    time_step_minutes=self.retention_scenario.time_step_minutes,
                ),
                model_dir,
            )
            summary_df = self._summarize_branch(branch, run_result, diagnostics_df, fss_df, hourly_df)
            branch_rows.extend(summary_df.to_dict(orient="records"))
            all_pairings.append(pairing_df)
            all_fss.append(fss_df)
            all_diagnostics.append(diagnostics_df)
            all_hourly.append(hourly_df)

        summary_all = pd.DataFrame(branch_rows)
        pairings_all = pd.concat(all_pairings, ignore_index=True) if all_pairings else pd.DataFrame()
        fss_all = pd.concat(all_fss, ignore_index=True) if all_fss else pd.DataFrame()
        diagnostics_all = pd.concat(all_diagnostics, ignore_index=True) if all_diagnostics else pd.DataFrame()
        hourly_all = pd.concat(all_hourly, ignore_index=True) if all_hourly else pd.DataFrame()
        recommendation = recommend_initialization_strategy(summary_all)
        paths = self._write_outputs(summary_all, diagnostics_all, hourly_all, pairings_all, fss_all)
        qa_paths = self._write_qa(diagnostics_all, hourly_all)
        report_path = self._write_report(summary_all, recommendation, paths, qa_paths)
        manifest_path = self._write_manifest(
            transport_manifest=transport_manifest,
            official_rerun_manifest=official_rerun_manifest,
            forcing_paths=forcing_paths,
            summary_df=summary_all,
            recommendation=recommendation,
            paths=paths,
            qa_paths=qa_paths,
            report_path=report_path,
        )
        return {
            "output_dir": self.output_dir,
            "summary_csv": paths["summary"],
            "diagnostics_csv": paths["diagnostics"],
            "hourly_timeseries_csv": paths["hourly"],
            "pairing_manifest_csv": paths["pairing"],
            "run_manifest": manifest_path,
            "report_md": report_path,
            "recommendation": recommendation,
            "summary": summary_all,
        }

    @staticmethod
    def _validate_inputs(transport_manifest: dict, official_rerun_manifest: dict) -> None:
        if str((transport_manifest.get("recommendation") or {}).get("best_scenario")) != "R1":
            raise RuntimeError("transport_retention_fix did not select R1.")
        if not bool((official_rerun_manifest.get("guardrails") or {}).get("diagnostic_r3_not_promoted")):
            raise RuntimeError("official_rerun_r1 manifest must preserve R3 as diagnostic-only.")

    def _resolve_or_run_branch(self, branch: InitBranch, forcing_paths: dict) -> dict:
        expected_recipe = resolve_recipe_selection().recipe
        if branch.reuse_official_rerun_r1:
            manifest = _read_json(self.case_output / OFFICIAL_RERUN_R1_DIR_NAME / "official_rerun_r1_run_manifest.json")
            model_dir = Path(((manifest.get("model_result") or {}).get("model_dir") or ""))
            if not model_dir_complete_for_recipe(model_dir, expected_recipe):
                raise FileNotFoundError(f"B branch could not reuse official_rerun_r1 model directory: {model_dir}")
            return {
                "branch_id": branch.branch_id,
                "status": "retained_from_official_rerun_r1",
                "model_dir": str(model_dir),
                "run_name": (manifest.get("model_result") or {}).get("run_name", ""),
                "retained_from_official_rerun_r1": True,
                "source_geometry_path": str(self.case.initialization_layer.processed_vector_path(self.case.run_name)),
                "point_release_surrogate": "not_applicable",
            }

        model_run_name = f"{self.case.run_name}/{INIT_MODE_SENSITIVITY_DIR_NAME}/{branch.output_slug}/model_run"
        model_dir = get_case_output_dir(model_run_name)
        if model_dir_complete_for_recipe(model_dir, expected_recipe) and not self.force_rerun:
            return {
                "branch_id": branch.branch_id,
                "status": "reused_existing_branch",
                "model_dir": str(model_dir),
                "run_name": model_run_name,
                "retained_from_official_rerun_r1": False,
                "source_geometry_path": str(self.case.provenance_layer.processed_vector_path(self.case.run_name)),
                "point_release_surrogate": branch.seed_overrides.get("point_release_surrogate", ""),
            }

        selection = resolve_recipe_selection()
        start_lat, start_lon, start_time = resolve_spill_origin()
        start = _normalize_utc(self.case.simulation_start_utc)
        end = _normalize_utc(self.case.simulation_end_utc)
        duration_hours = int(math.ceil((end - start).total_seconds() / 3600.0))
        forecast_result = run_official_spill_forecast(
            selection=selection,
            start_time=start_time,
            start_lat=start_lat,
            start_lon=start_lon,
            output_run_name=model_run_name,
            forcing_override=forcing_paths,
            simulation_start_utc=self.case.simulation_start_utc,
            simulation_end_utc=self.case.simulation_end_utc,
            snapshot_hours=[24, 48, duration_hours],
            date_composite_dates=list(self.validation_dates),
            transport_overrides={
                "coastline_action": self.retention_scenario.coastline_action,
                "coastline_approximation_precision": self.retention_scenario.coastline_approximation_precision,
                "time_step_minutes": self.retention_scenario.time_step_minutes,
            },
            seed_overrides=branch.seed_overrides,
            sensitivity_context={
                "track": INIT_MODE_SENSITIVITY_DIR_NAME,
                "branch_id": branch.branch_id,
                "initialization_mode": branch.initialization_mode,
                "selected_transport_retention_mode": "R1",
                "coastline_action": self.retention_scenario.coastline_action,
            },
        )
        return {
            "branch_id": branch.branch_id,
            "status": forecast_result.get("status", "unknown"),
            "model_dir": str(model_dir),
            "run_name": model_run_name,
            "retained_from_official_rerun_r1": False,
            "forecast_result": forecast_result,
            "source_geometry_path": str(self.case.provenance_layer.processed_vector_path(self.case.run_name)),
            "point_release_surrogate": branch.seed_overrides.get("point_release_surrogate", ""),
        }

    def _build_utc_date_composites(self, branch: InitBranch, model_dir: Path) -> Path:
        composite_dir = self.output_dir / branch.output_slug / "forecast_datecomposites"
        composite_dir.mkdir(parents=True, exist_ok=True)
        member_paths = sorted((model_dir / "ensemble").glob("member_*.nc"))
        if not member_paths:
            raise FileNotFoundError(f"No ensemble members found for {branch.branch_id}: {model_dir / 'ensemble'}")
        for date in self.validation_dates:
            probability = self._date_composite_probability(member_paths, date)
            probability = apply_ocean_mask(probability, sea_mask=self.retention.sea_mask, fill_value=0.0)
            p50 = apply_ocean_mask((probability >= 0.5).astype(np.float32), sea_mask=self.retention.sea_mask, fill_value=0.0)
            save_raster(self.retention.grid, probability.astype(np.float32), composite_dir / f"prob_presence_{date}_datecomposite.tif")
            save_raster(self.retention.grid, p50.astype(np.float32), composite_dir / f"mask_p50_{date}_datecomposite.tif")
        return composite_dir

    def _date_composite_probability(self, member_paths: list[Path], target_date: str) -> np.ndarray:
        masks = [self._member_utc_date_mask(path, target_date) for path in member_paths]
        return np.mean(np.stack(masks, axis=0), axis=0).astype(np.float32)

    def _member_utc_date_mask(self, member_path: Path, target_date: str) -> np.ndarray:
        target = pd.Timestamp(target_date).date()
        composite = np.zeros((self.retention.grid.height, self.retention.grid.width), dtype=np.float32)
        with xr.open_dataset(member_path) as ds:
            times = normalize_time_index(ds["time"].values)
            for index, timestamp in enumerate(times):
                if pd.Timestamp(timestamp).date() != target:
                    continue
                lon = np.asarray(ds["lon"].isel(time=index).values).reshape(-1)
                lat = np.asarray(ds["lat"].isel(time=index).values).reshape(-1)
                status = np.asarray(ds["status"].isel(time=index).values).reshape(-1)
                valid = np.isfinite(lon) & np.isfinite(lat) & (status == 0)
                if not np.any(valid):
                    continue
                hits, _ = rasterize_particles(
                    self.retention.grid,
                    lon[valid],
                    lat[valid],
                    np.ones(int(np.count_nonzero(valid)), dtype=np.float32),
                )
                composite = np.maximum(composite, hits)
        return apply_ocean_mask(composite.astype(np.float32), sea_mask=self.retention.sea_mask, fill_value=0.0)

    def _build_pairings(self, branch: InitBranch, composite_dir: Path) -> list[dict]:
        obs_mask = Path("data/arcgis") / self.case.run_name / "obs_mask_2023-03-06.tif"
        pairings = [
            self._pair(
                branch=branch,
                pair_role="strict_march6",
                pair_id=f"{branch.branch_id}_strict_march6",
                obs_date="2023-03-06",
                forecast_path=composite_dir / "mask_p50_2023-03-06_datecomposite.tif",
                observation_path=obs_mask,
                source_semantics="March6_date_composite_vs_March6_obsmask",
            )
        ]

        union_dir = self.case_output / "phase3b_multidate_public" / "date_union_obs_masks"
        missing_dates: list[str] = []
        for date in self.validation_dates:
            obs_path = union_dir / f"obs_union_{date}.tif"
            if not obs_path.exists():
                missing_dates.append(date)
                continue
            pairings.append(
                self._pair(
                    branch=branch,
                    pair_role="multidate_date_union",
                    pair_id=f"{branch.branch_id}_multidate_date_union_{date}",
                    obs_date=date,
                    forecast_path=composite_dir / f"mask_p50_{date}_datecomposite.tif",
                    observation_path=obs_path,
                    source_semantics=f"per_date_union_{date}_public_observation_derived_vs_p50_datecomposite",
                )
            )

        event_obs = (
            self.case_output
            / "phase3b_multidate_public"
            / f"eventcorridor_obs_union_{self.eventcorridor_label}.tif"
        )
        event_model = self._build_eventcorridor_model_union(branch, composite_dir)
        pairings.append(
            self._pair(
                branch=branch,
                pair_role="eventcorridor_march4_6",
                pair_id=f"{branch.branch_id}_eventcorridor_{self.eventcorridor_label}",
                obs_date=self.eventcorridor_label,
                forecast_path=event_model,
                observation_path=event_obs,
                source_semantics="eventcorridor_public_observation_derived_union_excluding_initialization_date",
                extra={"missing_multidate_obs_dates": ",".join(missing_dates)},
            )
        )
        for pair in pairings:
            if not Path(pair["forecast_path"]).exists():
                raise FileNotFoundError(f"Missing forecast product for {pair['pair_id']}: {pair['forecast_path']}")
            if not Path(pair["observation_path"]).exists():
                raise FileNotFoundError(f"Missing observation product for {pair['pair_id']}: {pair['observation_path']}")
        return pairings

    def _pair(
        self,
        branch: InitBranch,
        pair_role: str,
        pair_id: str,
        obs_date: str,
        forecast_path: Path,
        observation_path: Path,
        source_semantics: str,
        extra: dict | None = None,
    ) -> dict:
        row = {
            "scenario_id": branch.branch_id,
            "scenario_slug": branch.initialization_mode,
            "branch_id": branch.branch_id,
            "initialization_mode": branch.initialization_mode,
            "pair_id": pair_id,
            "pair_role": pair_role,
            "obs_date": obs_date,
            "forecast_path": forecast_path,
            "observation_path": observation_path,
            "metric": "FSS",
            "windows_km": "1,3,5,10",
            "source_semantics": source_semantics,
            "selected_transport_retention_mode": "R1",
            "coastline_action": self.retention_scenario.coastline_action,
            "source_geometry_label": branch.source_geometry_label,
        }
        if extra:
            row.update(extra)
        return row

    def _build_eventcorridor_model_union(self, branch: InitBranch, composite_dir: Path) -> Path:
        union = np.zeros((self.retention.grid.height, self.retention.grid.width), dtype=np.float32)
        for date in self.validation_dates:
            mask_path = composite_dir / f"mask_p50_{date}_datecomposite.tif"
            if mask_path.exists():
                mask = self.retention._read_raster(mask_path)
                union = np.maximum(union, (mask > 0).astype(np.float32))
        union = apply_ocean_mask(union, sea_mask=self.retention.sea_mask, fill_value=0.0)
        path = self.output_dir / branch.output_slug / f"eventcorridor_model_union_{self.eventcorridor_label}.tif"
        save_raster(self.retention.grid, union.astype(np.float32), path)
        return path

    def _augment_probability_diagnostics(self, branch: InitBranch, diagnostics_df: pd.DataFrame, composite_dir: Path) -> pd.DataFrame:
        rows = []
        for _, row in diagnostics_df.iterrows():
            record = row.to_dict()
            obs_date = str(record.get("obs_date", ""))
            prob_path = composite_dir / f"prob_presence_{obs_date}_datecomposite.tif"
            if prob_path.exists():
                probability = self.retention._read_raster(prob_path)
                record["probability_path"] = str(prob_path)
                record["max_probability"] = float(np.nanmax(probability))
                record["probability_nonzero_cells"] = int(np.count_nonzero(probability > 0))
            else:
                record["probability_path"] = ""
                record["max_probability"] = np.nan
                record["probability_nonzero_cells"] = np.nan
            record["branch_id"] = branch.branch_id
            record["initialization_mode"] = branch.initialization_mode
            rows.append(record)
        return pd.DataFrame(rows)

    def _summarize_branch(
        self,
        branch: InitBranch,
        run_result: dict,
        diagnostics_df: pd.DataFrame,
        fss_df: pd.DataFrame,
        hourly_df: pd.DataFrame,
    ) -> pd.DataFrame:
        last_times = self._last_times(hourly_df)
        rows = []
        for _, diag in diagnostics_df.iterrows():
            record = diag.to_dict()
            pair_fss = fss_df[fss_df["pair_id"] == record["pair_id"]]
            for window in OFFICIAL_PHASE3B_WINDOWS_KM:
                values = pair_fss.loc[pair_fss["window_km"].astype(int) == int(window), "fss"]
                record[f"fss_{window}km"] = float(values.iloc[0]) if not values.empty else np.nan
            record.update(
                {
                    "branch_id": branch.branch_id,
                    "initialization_mode": branch.initialization_mode,
                    "branch_description": branch.description,
                    "source_geometry_path": run_result.get("source_geometry_path", ""),
                    "point_release_surrogate": run_result.get("point_release_surrogate", "not_applicable"),
                    "retained_from_official_rerun_r1": run_result.get("retained_from_official_rerun_r1", False),
                    "transport_model": "oceandrift",
                    "provisional_transport_model": True,
                    "selected_transport_retention_mode": "R1",
                    "coastline_action": self.retention_scenario.coastline_action,
                    "coastline_approximation_precision": self.retention_scenario.coastline_approximation_precision,
                    "time_step_minutes": self.retention_scenario.time_step_minutes,
                    "recipe_used": resolve_recipe_selection().recipe,
                    "element_count_used": self._element_count_from_manifest(Path(run_result["model_dir"])),
                    "shoreline_mask_signature": self.retention.grid.spec.shoreline_mask_signature,
                    **last_times,
                    "survives_to_strict_validation": _time_reaches(last_times["last_raw_active_time_utc"], STRICT_VALIDATION_TIME_UTC),
                }
            )
            rows.append(record)
        return pd.DataFrame(rows)

    @staticmethod
    def _last_times(hourly_df: pd.DataFrame) -> dict:
        def last_time(frame: pd.DataFrame, column: str) -> str:
            if frame.empty or column not in frame:
                return ""
            values = pd.to_numeric(frame[column], errors="coerce").fillna(0)
            if not values.gt(0).any():
                return ""
            timestamps = pd.to_datetime(frame.loc[values.gt(0), "timestamp_utc"], errors="coerce", utc=True).dt.tz_convert(None).dropna()
            if timestamps.empty:
                return ""
            return timestamps.max().strftime("%Y-%m-%dT%H:%M:%SZ")

        deterministic = hourly_df[hourly_df["run_kind"] == "deterministic_control"].copy()
        aggregate = hourly_df[hourly_df["run_kind"] == "ensemble_aggregate"].copy()
        members = hourly_df[hourly_df["run_kind"] == "ensemble_member"].copy()
        return {
            "last_raw_active_time_utc": last_time(members, "active_count"),
            "last_nonzero_deterministic_footprint_utc": last_time(deterministic, "surface_presence_cells"),
            "last_nonzero_prob_presence_utc": last_time(aggregate, "prob_presence_nonzero_cells"),
            "last_nonzero_mask_p50_utc": last_time(aggregate, "p50_nonzero_cells"),
            "last_nonzero_mask_p90_utc": last_time(aggregate, "p90_nonzero_cells"),
        }

    @staticmethod
    def _element_count_from_manifest(model_dir: Path) -> int | str:
        manifest_path = model_dir / "forecast" / "forecast_manifest.json"
        if not manifest_path.exists():
            return ""
        manifest = _read_json(manifest_path)
        return (manifest.get("ensemble") or {}).get("actual_element_count") or (
            manifest.get("deterministic_control") or {}
        ).get("actual_element_count", "")

    def _write_outputs(
        self,
        summary_df: pd.DataFrame,
        diagnostics_df: pd.DataFrame,
        hourly_df: pd.DataFrame,
        pairing_df: pd.DataFrame,
        fss_df: pd.DataFrame,
    ) -> dict[str, Path]:
        paths = {
            "summary": self.output_dir / "init_mode_sensitivity_r1_summary.csv",
            "diagnostics": self.output_dir / "init_mode_sensitivity_r1_diagnostics.csv",
            "hourly": self.output_dir / "init_mode_sensitivity_r1_hourly_timeseries.csv",
            "pairing": self.output_dir / "init_mode_sensitivity_r1_pairing_manifest.csv",
            "fss": self.output_dir / "init_mode_sensitivity_r1_fss_by_window.csv",
        }
        _write_csv(paths["summary"], summary_df)
        _write_csv(paths["diagnostics"], diagnostics_df)
        _write_csv(paths["hourly"], hourly_df)
        _write_csv(paths["pairing"], pairing_df)
        _write_csv(paths["fss"], fss_df)
        return paths

    def _write_manifest(
        self,
        transport_manifest: dict,
        official_rerun_manifest: dict,
        forcing_paths: dict,
        summary_df: pd.DataFrame,
        recommendation: dict,
        paths: dict[str, Path],
        qa_paths: dict[str, Path],
        report_path: Path,
    ) -> Path:
        path = self.output_dir / "init_mode_sensitivity_r1_run_manifest.json"
        payload = {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "workflow_mode": self.case.workflow_mode,
            "run_name": self.case.run_name,
            "phase": INIT_MODE_SENSITIVITY_DIR_NAME,
            "purpose": "Controlled initialization geometry sensitivity under selected R1 retention.",
            "selected_transport_retention": {
                "scenario": "R1",
                "coastline_action": self.retention_scenario.coastline_action,
                "selected_from": str(self.case_output / TRANSPORT_RETENTION_DIR_NAME / "transport_retention_run_manifest.json"),
                "official_rerun_r1_manifest": str(self.case_output / OFFICIAL_RERUN_R1_DIR_NAME / "official_rerun_r1_run_manifest.json"),
                "r3_diagnostic_only": True,
            },
            "simulation_window_utc": {
                "start": self.case.simulation_start_utc,
                "end": self.case.simulation_end_utc,
            },
            "forcing_paths": forcing_paths,
            "validation_dates_used_for_forecast_skill": self.validation_dates,
            "branches": [branch.__dict__ for branch in BRANCHES],
            "guardrails": {
                "strict_march6_pairing_unchanged": True,
                "within_horizon_public_semantics_unchanged": True,
                "short_extended_semantics_unchanged": True,
                "medium_tier_not_run": True,
                "a2_source_history_not_implemented": True,
                "scoring_rules_unchanged": True,
            },
            "transport_retention_recommendation": transport_manifest.get("recommendation", {}),
            "official_rerun_r1_recommendation": official_rerun_manifest.get("recommended_next_branch", ""),
            "recommendation": recommendation,
            "summary": summary_df.to_dict(orient="records"),
            "artifacts": {
                **{key: str(value) for key, value in paths.items()},
                **{key: str(value) for key, value in qa_paths.items()},
                "report": str(report_path),
            },
        }
        _write_json(path, payload)
        return path

    def _write_report(
        self,
        summary_df: pd.DataFrame,
        recommendation: dict,
        paths: dict[str, Path],
        qa_paths: dict[str, Path],
    ) -> Path:
        path = self.output_dir / "init_mode_sensitivity_r1_report.md"
        strict = summary_df[summary_df["pair_role"] == "strict_march6"].copy()
        event = summary_df[summary_df["pair_role"] == "eventcorridor_march4_6"].copy()
        lines = [
            "# Initialization Mode Sensitivity R1",
            "",
            "This controlled sensitivity compares B (`observation_initialized_polygon`) against A1 (`source_point_initialized_same_start`) while holding R1 transport retention, forcing, grid, shoreline mask, recipe, and scoring fixed.",
            "",
            f"- Recommended initialization strategy: `{recommendation['recommended_initialization_strategy']}`",
            f"- A2 source-history reconstruction worth attempting next: `{recommendation['a2_source_history_reconstruction_worth_attempting']}`",
            f"- Reason: {recommendation['reason']}",
            "",
            "## Strict March 6",
            "",
        ]
        lines.extend(self._markdown_table(strict[self._summary_columns()]))
        lines.extend(["", f"## {self.eventcorridor_label} Event Corridor", ""])
        lines.extend(self._markdown_table(event[self._summary_columns()]))
        lines.extend(["", "Artifacts:"])
        for label, artifact in paths.items():
            lines.append(f"- {label}: {artifact}")
        for label, artifact in qa_paths.items():
            lines.append(f"- {label}: {artifact}")
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return path

    @staticmethod
    def _summary_columns() -> list[str]:
        return [
            "branch_id",
            "initialization_mode",
            "forecast_nonzero_cells",
            "obs_nonzero_cells",
            "centroid_distance_m",
            "nearest_distance_to_obs_m",
            "iou",
            "dice",
            "fss_1km",
            "fss_3km",
            "fss_5km",
            "fss_10km",
            "last_raw_active_time_utc",
        ]

    @staticmethod
    def _markdown_table(df: pd.DataFrame) -> list[str]:
        if df.empty:
            return ["No rows."]
        columns = list(df.columns)
        lines = ["| " + " | ".join(columns) + " |", "| " + " | ".join(["---"] * len(columns)) + " |"]
        for _, row in df.iterrows():
            lines.append("| " + " | ".join(str(row.get(column, "")) for column in columns) + " |")
        return lines

    def _write_qa(self, diagnostics_df: pd.DataFrame, hourly_df: pd.DataFrame) -> dict[str, Path]:
        outputs: dict[str, Path] = {}
        if plt is None:
            return outputs
        overlays = self._plot_overlays(diagnostics_df)
        tracks = self._plot_tracks()
        if overlays:
            outputs["qa_init_mode_sensitivity_r1_overlays"] = overlays
        if tracks:
            outputs["qa_init_mode_sensitivity_r1_tracks"] = tracks
        return outputs

    def _plot_overlays(self, diagnostics_df: pd.DataFrame) -> Path | None:
        selected = diagnostics_df[diagnostics_df["pair_role"].isin(["strict_march6", "eventcorridor_march4_6"])].copy()
        if selected.empty:
            return None
        path = self.output_dir / "qa_init_mode_sensitivity_r1_overlays.png"
        rows = sorted(selected["pair_role"].unique().tolist())
        cols = sorted(selected["branch_id"].unique().tolist())
        fig, axes = plt.subplots(len(rows), len(cols), figsize=(5 * len(cols), 5 * len(rows)))
        axes_array = np.asarray(axes).reshape(len(rows), len(cols))
        for row_index, pair_role in enumerate(rows):
            for col_index, branch_id in enumerate(cols):
                ax = axes_array[row_index, col_index]
                match = selected[(selected["pair_role"] == pair_role) & (selected["branch_id"] == branch_id)]
                if match.empty:
                    ax.axis("off")
                    continue
                item = match.iloc[0]
                forecast = self.retention.scoring_helper._load_binary_score_mask(Path(item["forecast_path"]))
                obs = self.retention.scoring_helper._load_binary_score_mask(Path(item["observation_path"]))
                canvas = np.ones((forecast.shape[0], forecast.shape[1], 3), dtype=np.float32)
                canvas[obs > 0] = [0.1, 0.35, 0.95]
                canvas[forecast > 0] = [0.95, 0.35, 0.1]
                canvas[(obs > 0) & (forecast > 0)] = [0.1, 0.65, 0.25]
                ax.imshow(canvas)
                ax.set_title(f"{branch_id} {pair_role}")
                ax.axis("off")
        fig.tight_layout()
        fig.savefig(path, dpi=150, bbox_inches="tight")
        plt.close(fig)
        return path

    def _plot_tracks(self) -> Path | None:
        path = self.output_dir / "qa_init_mode_sensitivity_r1_tracks.png"
        fig, ax = plt.subplots(figsize=(8, 8))
        ax.imshow(
            self.retention.valid_ocean,
            cmap="Blues",
            alpha=0.25,
            extent=[self.retention.grid.min_x, self.retention.grid.max_x, self.retention.grid.min_y, self.retention.grid.max_y],
            origin="upper",
        )
        for branch in BRANCHES:
            model_dir = self._branch_model_dir_for_plot(branch)
            member = next(iter(sorted((model_dir / "ensemble").glob("member_*.nc"))), None)
            if member is None:
                continue
            with xr.open_dataset(member) as ds:
                lon = np.asarray(ds["lon"].values)
                lat = np.asarray(ds["lat"].values)
                trajectories = np.linspace(0, lon.shape[0] - 1, min(20, lon.shape[0]), dtype=int)
                for idx in trajectories:
                    finite = np.isfinite(lon[idx]) & np.isfinite(lat[idx])
                    if np.any(finite):
                        x_vals, y_vals = project_points_to_grid(self.retention.grid, lon[idx][finite], lat[idx][finite])
                        ax.plot(x_vals, y_vals, linewidth=0.5, alpha=0.25, label=branch.branch_id if idx == trajectories[0] else None)
        ax.set_title("Sample tracks by initialization branch")
        ax.set_xlim(self.retention.grid.min_x, self.retention.grid.max_x)
        ax.set_ylim(self.retention.grid.min_y, self.retention.grid.max_y)
        ax.set_aspect("equal")
        ax.legend(loc="upper right")
        fig.tight_layout()
        fig.savefig(path, dpi=150, bbox_inches="tight")
        plt.close(fig)
        return path

    def _branch_model_dir_for_plot(self, branch: InitBranch) -> Path:
        if branch.reuse_official_rerun_r1:
            manifest_path = self.case_output / OFFICIAL_RERUN_R1_DIR_NAME / "official_rerun_r1_run_manifest.json"
            manifest = _read_json(manifest_path)
            return Path((manifest.get("model_result") or {}).get("model_dir", ""))
        return get_case_output_dir(f"{self.case.run_name}/{INIT_MODE_SENSITIVITY_DIR_NAME}/{branch.output_slug}/model_run")


def run_init_mode_sensitivity_r1() -> dict:
    return InitModeSensitivityR1Service().run()


if __name__ == "__main__":  # pragma: no cover
    print(json.dumps(run_init_mode_sensitivity_r1(), indent=2, default=_json_default))
