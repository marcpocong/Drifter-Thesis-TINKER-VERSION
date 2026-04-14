"""Source-history reconstruction sensitivity under the selected R1 retention configuration."""

from __future__ import annotations

import json
import math
import os
import shutil
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import yaml

from src.core.case_context import get_case_context
from src.exceptions.custom import ForcingOutagePhaseSkipped
from src.helpers.metrics import calculate_fss
from src.helpers.raster import GridBuilder, project_points_to_grid, rasterize_particles, save_raster
from src.helpers.scoring import apply_ocean_mask, load_sea_mask_array, precheck_same_grid
from src.services.ensemble import normalize_time_index, run_official_spill_forecast
from src.services.ingestion import DataIngestionService
from src.services.init_mode_sensitivity_r1 import INIT_MODE_SENSITIVITY_DIR_NAME
from src.services.official_rerun_r1 import OFFICIAL_RERUN_R1_DIR_NAME, load_official_retention_config
from src.services.phase3b_extended_public_scored import _forcing_time_and_vars
from src.services.phase3b_multidate_public import (
    format_phase3b_multidate_eventcorridor_label,
    load_phase3b_multidate_validation_dates,
)
from src.services.scoring import OFFICIAL_PHASE3B_WINDOWS_KM, Phase3BScoringService
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
from src.utils.forcing_outage_policy import (
    FORCING_OUTAGE_POLICY_CONTINUE_DEGRADED,
    resolve_forcing_source_budget_seconds,
    resolve_forcing_outage_policy,
    source_id_for_recipe_component,
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


SOURCE_HISTORY_RECONSTRUCTION_DIR_NAME = "source_history_reconstruction_r1"
FORCE_RERUN_ENV = "SOURCE_HISTORY_RECONSTRUCTION_R1_FORCE_RERUN"
ANCHOR_END_UTC = "2023-03-03T09:59:00Z"
STRICT_END_UTC = "2023-03-06T09:59:00Z"
DATE_COMPOSITE_DATES = ["2023-03-03", "2023-03-04", "2023-03-05", "2023-03-06"]
FORECAST_SKILL_DATES = ["2023-03-04", "2023-03-05", "2023-03-06"]


@dataclass(frozen=True)
class SourceHistoryScenario:
    scenario_id: str
    release_duration_hours: int
    description: str

    @property
    def output_slug(self) -> str:
        return f"{self.scenario_id.lower()}_{self.release_duration_hours}h"

    def release_window(self, anchor_end_utc: str = ANCHOR_END_UTC) -> dict[str, str]:
        release_end = _normalize_utc(anchor_end_utc)
        release_start = release_end - pd.Timedelta(hours=int(self.release_duration_hours))
        return {
            "release_start_utc": release_start.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "release_end_utc": release_end.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "simulation_start_utc": release_start.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "simulation_end_utc": _normalize_utc(STRICT_END_UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        }


A2_SCENARIOS = [
    SourceHistoryScenario(
        scenario_id="A2_PULSE",
        release_duration_hours=0,
        description="Source-point instantaneous pulse ending at the March 3 observed-slick anchor.",
    ),
    SourceHistoryScenario(
        scenario_id="A2_24H",
        release_duration_hours=24,
        description="Source-point release lasting 24 hours before the March 3 observed-slick anchor.",
    ),
    SourceHistoryScenario(
        scenario_id="A2_48H",
        release_duration_hours=48,
        description="Source-point release lasting 48 hours before the March 3 observed-slick anchor.",
    ),
    SourceHistoryScenario(
        scenario_id="A2_72H",
        release_duration_hours=72,
        description="Source-point release lasting 72 hours before the March 3 observed-slick anchor.",
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


def _write_csv(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False)


def _truthy(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _float_or_nan(value: Any) -> float:
    try:
        if value in ("", None):
            return np.nan
        return float(value)
    except (TypeError, ValueError):
        return np.nan


def _mean_fss(row: pd.Series | dict) -> float:
    values = [_float_or_nan(row.get(f"fss_{window}km")) for window in OFFICIAL_PHASE3B_WINDOWS_KM]
    finite = [value for value in values if np.isfinite(value)]
    return float(np.mean(finite)) if finite else 0.0


def _iso_z(value: Any) -> str:
    return _normalize_utc(value).strftime("%Y-%m-%dT%H:%M:%SZ")


def _required_forcing_vars(kind: str, recipe_file_name: str) -> list[str]:
    if kind == "currents":
        return ["uo", "vo"] if str(recipe_file_name).startswith("cmems") else ["water_u", "water_v"]
    if kind == "wind":
        return ["x_wind", "y_wind"]
    if kind == "wave":
        return ["VHM0", "VSDX", "VSDY"]
    return []


def recommend_source_history_strategy(summary_df: pd.DataFrame, init_summary_df: pd.DataFrame | None = None) -> dict:
    """Choose exactly one post-A2 recommendation from scored source-history rows."""
    strict = summary_df[summary_df["pair_role"] == "strict_march6"].copy()
    event = summary_df[summary_df["pair_role"] == "eventcorridor_march4_6"].copy()
    checkpoint = summary_df[summary_df["pair_role"] == "march3_reconstruction_checkpoint"].copy()
    if strict.empty:
        return {
            "best_a2_scenario": "",
            "recommendation": "A2 is still not enough and recommend convergence next",
            "convergence_should_be_next": True,
            "march3_checkpoint_improved": False,
            "strict_march6_materially_improved": False,
            "eventcorridor_materially_improved": False,
            "reason": "No strict March 6 A2 rows were available.",
        }

    score_rows = []
    for scenario_id, group in strict.groupby("scenario_id"):
        strict_row = group.iloc[0]
        event_row = event[event["scenario_id"] == scenario_id]
        checkpoint_row = checkpoint[checkpoint["scenario_id"] == scenario_id]
        score_rows.append(
            {
                "scenario_id": scenario_id,
                "strict_mean_fss": _mean_fss(strict_row),
                "strict_iou": _float_or_nan(strict_row.get("iou")),
                "strict_nearest_m": _float_or_nan(strict_row.get("nearest_distance_to_obs_m")),
                "event_mean_fss": _mean_fss(event_row.iloc[0]) if not event_row.empty else 0.0,
                "checkpoint_mean_fss": _mean_fss(checkpoint_row.iloc[0]) if not checkpoint_row.empty else 0.0,
                "checkpoint_iou": _float_or_nan(checkpoint_row.iloc[0].get("iou")) if not checkpoint_row.empty else 0.0,
            }
        )
    scores = pd.DataFrame(score_rows)
    scores["strict_nearest_rank"] = -pd.to_numeric(scores["strict_nearest_m"], errors="coerce").fillna(1.0e12)
    best = scores.sort_values(
        ["strict_mean_fss", "strict_iou", "event_mean_fss", "checkpoint_mean_fss", "strict_nearest_rank"],
        ascending=False,
    ).iloc[0]

    previous_strict = 0.0
    previous_event = 0.0
    if init_summary_df is not None and not init_summary_df.empty:
        old_strict = init_summary_df[init_summary_df["pair_role"] == "strict_march6"].copy()
        old_event = init_summary_df[init_summary_df["pair_role"] == "eventcorridor_march4_6"].copy()
        if not old_strict.empty:
            previous_strict = max(_mean_fss(row) for _, row in old_strict.iterrows())
        if not old_event.empty:
            previous_event = max(_mean_fss(row) for _, row in old_event.iterrows())

    strict_improved = bool(float(best["strict_mean_fss"]) > max(0.01, previous_strict + 0.01) or float(best["strict_iou"]) > 0.0)
    event_improved = bool(float(best["event_mean_fss"]) > previous_event + 0.01)
    checkpoint_improved = bool(float(best["checkpoint_mean_fss"]) > 0.01 or float(best["checkpoint_iou"]) > 0.0)

    if strict_improved:
        recommendation = f"promote {best['scenario_id']} as the stronger main event-reconstruction candidate"
        convergence_next = False
        reason = "At least one A2 source-history branch materially improved strict March 6 overlap."
    elif event_improved or checkpoint_improved:
        recommendation = "keep B as the main case-definition and A2 as reconstruction sensitivity"
        convergence_next = False
        reason = (
            "A2 improved reconstruction/event-corridor evidence but did not materially improve the strict March 6 "
            "single-date stress test."
        )
    else:
        recommendation = "A2 is still not enough and recommend convergence next"
        convergence_next = True
        reason = "A2 did not materially improve strict March 6, event-corridor, or March 3 checkpoint skill."

    return {
        "best_a2_scenario": str(best["scenario_id"]),
        "recommendation": recommendation,
        "convergence_should_be_next": convergence_next,
        "march3_checkpoint_improved": checkpoint_improved,
        "strict_march6_materially_improved": strict_improved,
        "eventcorridor_materially_improved": event_improved,
        "previous_best_strict_mean_fss": previous_strict,
        "previous_best_eventcorridor_mean_fss": previous_event,
        "best_a2_strict_mean_fss": float(best["strict_mean_fss"]),
        "best_a2_eventcorridor_mean_fss": float(best["event_mean_fss"]),
        "best_a2_checkpoint_mean_fss": float(best["checkpoint_mean_fss"]),
        "reason": reason,
    }


class SourceHistoryReconstructionR1Service:
    def __init__(self):
        self.case = get_case_context()
        if not self.case.is_official:
            raise RuntimeError("source_history_reconstruction_r1 is only supported for official Mindoro workflows.")
        if xr is None:
            raise ImportError("xarray is required for source_history_reconstruction_r1.")
        self.case_output = get_case_output_dir(self.case.run_name)
        self.output_dir = self.case_output / SOURCE_HISTORY_RECONSTRUCTION_DIR_NAME
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.force_rerun = _truthy(os.environ.get(FORCE_RERUN_ENV, ""))
        self.validation_dates = load_phase3b_multidate_validation_dates(
            self.case_output,
            fallback_dates=FORECAST_SKILL_DATES,
        )
        self.date_composite_dates = list(dict.fromkeys(["2023-03-03", *self.validation_dates]))
        self.eventcorridor_label = format_phase3b_multidate_eventcorridor_label(self.validation_dates)
        self.retention_config = load_official_retention_config()
        if self.retention_config["selected_mode"] != "R1":
            raise RuntimeError("source_history_reconstruction_r1 requires official_retention.selected_mode=R1.")
        self.phase_id = SOURCE_HISTORY_RECONSTRUCTION_DIR_NAME
        self.forcing_outage_policy = resolve_forcing_outage_policy(
            workflow_mode=self.case.workflow_mode,
            phase=self.phase_id,
        )
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
        self.grid = GridBuilder()
        self.sea_mask = load_sea_mask_array(self.grid.spec)
        self.valid_mask = self.sea_mask > 0.5 if self.sea_mask is not None else None
        self.helper = Phase3BScoringService(output_dir=self.output_dir / "_scratch_helper")
        self.forcing_dir = self.output_dir / "forcing"
        self.forcing_dir.mkdir(parents=True, exist_ok=True)

    def run(self) -> dict:
        official_manifest = _read_json(self.case_output / OFFICIAL_RERUN_R1_DIR_NAME / "official_rerun_r1_run_manifest.json")
        init_manifest = _read_json(self.case_output / INIT_MODE_SENSITIVITY_DIR_NAME / "init_mode_sensitivity_r1_run_manifest.json")
        self._validate_inputs(official_manifest, init_manifest)
        selection = resolve_recipe_selection()
        windows = self._scenario_windows()
        forcing_paths = self._prepare_source_history_forcing(selection.recipe, windows)
        forcing_manifest = self._write_forcing_manifest(selection.recipe, forcing_paths, windows)
        failed_forcing = [row for row in forcing_manifest["rows"] if row["status"] != "ready"]
        if failed_forcing:
            self._write_failed_loading_audit(failed_forcing)
            raise RuntimeError(
                "Source-history reconstruction forcing coverage is incomplete. "
                f"See {self.output_dir / 'source_history_forcing_window_manifest.json'}."
            )

        march5_union = self._materialize_missing_march5_union_if_available()
        scenario_rows: list[dict] = []
        all_pairings: list[pd.DataFrame] = []
        all_fss: list[pd.DataFrame] = []
        all_diagnostics: list[pd.DataFrame] = []
        all_hourly: list[pd.DataFrame] = []
        scenario_results: list[dict] = []

        for scenario in A2_SCENARIOS:
            run_result = self._resolve_or_run_scenario(scenario, selection, forcing_paths)
            model_dir = Path(run_result["model_dir"])
            composite_dir = self._build_date_composites(scenario, model_dir)
            pairings = self._build_pairings(scenario, composite_dir)
            fss_df, diagnostics_df = self._score_pairings(scenario, pairings)
            diagnostics_df = self._augment_probability_diagnostics(scenario, diagnostics_df, composite_dir)
            hourly_df = self._build_hourly_diagnostics_for_window(scenario, model_dir)
            summary_df = self._summarize_scenario(scenario, run_result, diagnostics_df, fss_df, hourly_df)
            scenario_rows.extend(summary_df.to_dict(orient="records"))
            scenario_results.append(run_result)
            all_pairings.append(pd.DataFrame(pairings))
            all_fss.append(fss_df)
            all_diagnostics.append(diagnostics_df)
            all_hourly.append(hourly_df)

        summary_df = pd.DataFrame(scenario_rows)
        pairings_df = pd.concat(all_pairings, ignore_index=True) if all_pairings else pd.DataFrame()
        fss_df = pd.concat(all_fss, ignore_index=True) if all_fss else pd.DataFrame()
        diagnostics_df = pd.concat(all_diagnostics, ignore_index=True) if all_diagnostics else pd.DataFrame()
        hourly_df = pd.concat(all_hourly, ignore_index=True) if all_hourly else pd.DataFrame()
        init_summary = _read_csv(self.case_output / INIT_MODE_SENSITIVITY_DIR_NAME / "init_mode_sensitivity_r1_summary.csv")
        recommendation = recommend_source_history_strategy(summary_df, init_summary)
        paths = self._write_outputs(summary_df, diagnostics_df, hourly_df, pairings_df, fss_df)
        qa_paths = self._write_qa(diagnostics_df, hourly_df)
        report_path = self._write_report(summary_df, recommendation, paths, qa_paths)
        manifest_path = self._write_manifest(
            official_manifest=official_manifest,
            init_manifest=init_manifest,
            forcing_manifest=forcing_manifest,
            scenario_results=scenario_results,
            summary_df=summary_df,
            recommendation=recommendation,
            paths=paths,
            qa_paths=qa_paths,
            report_path=report_path,
            march5_union=march5_union,
        )
        self._copy_loading_audits()
        return {
            "output_dir": self.output_dir,
            "summary_csv": paths["summary"],
            "diagnostics_csv": paths["diagnostics"],
            "hourly_timeseries_csv": paths["hourly"],
            "pairing_manifest_csv": paths["pairing"],
            "fss_by_window_csv": paths["fss"],
            "run_manifest": manifest_path,
            "report_md": report_path,
            "recommendation": recommendation,
            "summary": summary_df,
        }

    @staticmethod
    def _validate_inputs(official_manifest: dict, init_manifest: dict) -> None:
        if str(official_manifest.get("selected_scenario")) != "R1":
            raise RuntimeError("official_rerun_r1 did not promote R1.")
        if not bool(official_manifest.get("r3_diagnostic_only", False)):
            raise RuntimeError("official_rerun_r1 manifest must preserve R3 as diagnostic-only.")
        init_recommendation = init_manifest.get("recommendation") or {}
        if not bool(init_recommendation.get("a2_source_history_reconstruction_worth_attempting", False)):
            raise RuntimeError("init_mode_sensitivity_r1 did not recommend attempting A2 source-history reconstruction.")

    def _scenario_windows(self) -> dict[str, dict[str, str]]:
        return {scenario.scenario_id: scenario.release_window() for scenario in A2_SCENARIOS}

    @staticmethod
    def _load_recipe(recipe_name: str) -> dict:
        with open("config/recipes.yaml", "r", encoding="utf-8") as handle:
            recipes = yaml.safe_load(handle) or {}
        recipe = (recipes.get("recipes") or {}).get(recipe_name)
        if not recipe:
            raise RuntimeError(f"Recipe {recipe_name} is not defined in config/recipes.yaml.")
        return recipe

    @staticmethod
    def _required_window_bounds(windows: dict[str, dict[str, str]]) -> tuple[pd.Timestamp, pd.Timestamp]:
        starts = [_normalize_utc(window["simulation_start_utc"]) for window in windows.values()]
        ends = [_normalize_utc(window["simulation_end_utc"]) for window in windows.values()]
        return min(starts), max(ends)

    def _candidate_gfs_cache_paths(self, gfs_path: Path) -> list[Path]:
        candidates = [
            gfs_path,
            Path("data/forcing") / self.case.run_name / gfs_path.name,
            self.case_output / "phase3b_extended_public_scored_short" / "forcing" / gfs_path.name,
        ]
        unique: list[Path] = []
        seen: set[str] = set()
        for candidate in candidates:
            key = str(candidate.resolve()) if candidate.exists() else str(candidate)
            if key in seen:
                continue
            seen.add(key)
            unique.append(candidate)
        return unique

    def _gfs_cache_ready_record(
        self,
        gfs_path: Path,
        windows: dict[str, dict[str, str]],
    ) -> dict[str, Any] | None:
        required_start, required_end = self._required_window_bounds(windows)
        for candidate in self._candidate_gfs_cache_paths(gfs_path):
            inspection = _forcing_time_and_vars(
                candidate,
                ["x_wind", "y_wind"],
                required_start,
                required_end,
            )
            if inspection["status"] != "ready":
                continue
            if candidate != gfs_path:
                gfs_path.parent.mkdir(parents=True, exist_ok=True)
                shutil.copyfile(candidate, gfs_path)
            return {
                "status": "reused_local_file",
                "path": str(gfs_path),
                "source_id": "gfs",
                "forcing_factor": gfs_path.name,
                "upstream_outage_detected": False,
                "source_system": "existing_local_cache",
                "source_tier": "staged",
                "staged_from": str(candidate),
                "requested_start_utc": inspection["required_start_utc"],
                "requested_end_utc": inspection["required_end_utc"],
                "cache_time_start_utc": inspection["time_start_utc"],
                "cache_time_end_utc": inspection["time_end_utc"],
            }
        return None

    def _download_required_gfs_wind(
        self,
        service: DataIngestionService,
        *,
        gfs_path: Path,
        windows: dict[str, dict[str, str]],
    ) -> dict[str, Any]:
        cached_record = self._gfs_cache_ready_record(gfs_path, windows)
        if cached_record is not None:
            return cached_record

        required_start, required_end = self._required_window_bounds(windows)
        gfs_path.unlink(missing_ok=True)
        budget_seconds = resolve_forcing_source_budget_seconds()
        primary_failure = ""
        primary_outage = False

        try:
            record = dict(
                service.gfs_downloader.download(
                    start_time=required_start,
                    end_time=required_end,
                    output_path=gfs_path,
                    scratch_dir=self.forcing_dir,
                    budget_seconds=budget_seconds,
                )
                or {}
            )
            record.setdefault("status", "downloaded")
            record["source_system"] = "ncei_thredds_archive"
            record["source_tier"] = "primary"
        except Exception as primary_exc:
            primary_failure = f"{type(primary_exc).__name__}: {primary_exc}"
            primary_outage = service._is_remote_outage_error(primary_exc)
            try:
                record = dict(
                    service.gfs_downloader.download_secondary_historical(
                        start_time=required_start,
                        end_time=required_end,
                        output_path=gfs_path,
                        scratch_dir=self.forcing_dir,
                        budget_seconds=budget_seconds,
                    )
                    or {}
                )
                record.setdefault("status", "downloaded")
                record["source_system"] = "ucar_gdex_d084001"
                record["source_tier"] = "secondary"
                record["primary_failure"] = primary_failure
            except Exception as secondary_exc:
                secondary_outage = service._is_remote_outage_error(secondary_exc)
                gfs_path.unlink(missing_ok=True)
                return {
                    "status": "failed",
                    "path": str(gfs_path),
                    "source_id": "gfs",
                    "forcing_factor": gfs_path.name,
                    "upstream_outage_detected": bool(primary_outage or secondary_outage),
                    "failure_stage": str(getattr(secondary_exc, "failure_stage", "secondary_gfs_acquisition")),
                    "error": (
                        "Primary GFS acquisition failed: "
                        f"{primary_failure}. Secondary GFS acquisition failed: "
                        f"{type(secondary_exc).__name__}: {secondary_exc}"
                    ),
                    "requested_start_utc": _iso_z(required_start),
                    "requested_end_utc": _iso_z(required_end),
                }

        record["path"] = str(gfs_path)
        record["source_id"] = "gfs"
        record["forcing_factor"] = gfs_path.name
        record["upstream_outage_detected"] = False
        record["requested_start_utc"] = _iso_z(required_start)
        record["requested_end_utc"] = _iso_z(required_end)
        record["primary_failure"] = str(record.get("primary_failure") or primary_failure)

        readiness = self._gfs_cache_ready_record(gfs_path, windows)
        if readiness is None:
            return {
                **record,
                "status": "failed",
                "error": (
                    "GFS wind cache download finished but the staged file does not cover the required "
                    f"window {record['requested_start_utc']} -> {record['requested_end_utc']}."
                ),
            }
        record["cache_time_start_utc"] = readiness["cache_time_start_utc"]
        record["cache_time_end_utc"] = readiness["cache_time_end_utc"]
        return record

    def _prepare_source_history_forcing(self, recipe_name: str, windows: dict[str, dict[str, str]]) -> dict:
        recipe = self._load_recipe(recipe_name)
        current_file = str(recipe["currents_file"])
        wind_file = str(recipe["wind_file"])
        wave_file = str(recipe.get("wave_file") or "")
        candidates = {
            "recipe": recipe_name,
            "currents": self.forcing_dir / current_file,
            "wind": self.forcing_dir / wind_file,
            "wave": self.forcing_dir / wave_file if wave_file else None,
            "downloads": {},
        }
        if self._forcing_candidates_ready(candidates, recipe, windows):
            candidates["downloads"] = {"status": "reused_existing_source_history_forcing"}
            return candidates

        required_start, required_end = self._required_window_bounds(windows)
        service = DataIngestionService()
        service.forcing_dir = self.forcing_dir
        service.forcing_dir.mkdir(parents=True, exist_ok=True)
        service.configure_explicit_download_window(
            start_date=required_start.date().isoformat(),
            end_date=(required_end + pd.Timedelta(days=1)).date().isoformat(),
        )

        downloads: dict[str, Any] = {}
        current_source_id = source_id_for_recipe_component(forcing_kind="current", filename=current_file)
        downloads["currents"] = service.download_required_forcing_record(current_source_id)
        if downloads["currents"]["status"] not in {"downloaded", "cached"}:
            if (
                downloads["currents"].get("upstream_outage_detected")
                and self.forcing_outage_policy == FORCING_OUTAGE_POLICY_CONTINUE_DEGRADED
            ):
                manifest_path = self._write_download_failure_manifest(
                    recipe_name,
                    downloads,
                    windows,
                    status="degraded_skipped_forcing_outage",
                    degraded_continue_used=True,
                    upstream_outage_detected=True,
                    missing_forcing_factors=[downloads["currents"]["forcing_factor"]],
                    stop_reason=str(downloads["currents"].get("error", "")),
                )
                raise ForcingOutagePhaseSkipped(
                    phase=self.phase_id,
                    workflow_mode=self.case.workflow_mode,
                    forcing_outage_policy=self.forcing_outage_policy,
                    reason=str(downloads["currents"].get("error", "")),
                    missing_forcing_factors=[downloads["currents"]["forcing_factor"]],
                    skipped_branch_ids=[scenario.scenario_id for scenario in A2_SCENARIOS],
                    manifest_path=str(manifest_path),
                    budget_seconds=downloads["currents"].get("budget_seconds"),
                    elapsed_seconds=downloads["currents"].get("elapsed_seconds"),
                    budget_exhausted=bool(downloads["currents"].get("budget_exhausted", False)),
                    failure_stage=str(downloads["currents"].get("failure_stage") or ""),
                )
            self._write_download_failure_manifest(recipe_name, downloads, windows)
            raise RuntimeError(f"Source-history forcing download failed: {downloads['currents']}")

        if wind_file.startswith("gfs"):
            downloads["wind"] = self._download_required_gfs_wind(
                service,
                gfs_path=self.forcing_dir / wind_file,
                windows=windows,
            )
            if downloads["wind"]["status"] not in {"downloaded", "cached", "reused_local_file"}:
                if (
                    downloads["wind"].get("upstream_outage_detected")
                    and self.forcing_outage_policy == FORCING_OUTAGE_POLICY_CONTINUE_DEGRADED
                ):
                    manifest_path = self._write_download_failure_manifest(
                        recipe_name,
                        downloads,
                        windows,
                        status="degraded_skipped_forcing_outage",
                        degraded_continue_used=True,
                        upstream_outage_detected=True,
                        missing_forcing_factors=[downloads["wind"]["forcing_factor"]],
                        stop_reason=str(downloads["wind"].get("error", "")),
                    )
                    raise ForcingOutagePhaseSkipped(
                        phase=self.phase_id,
                        workflow_mode=self.case.workflow_mode,
                        forcing_outage_policy=self.forcing_outage_policy,
                        reason=str(downloads["wind"].get("error", "")),
                        missing_forcing_factors=[downloads["wind"]["forcing_factor"]],
                        skipped_branch_ids=[scenario.scenario_id for scenario in A2_SCENARIOS],
                        manifest_path=str(manifest_path),
                        budget_seconds=downloads["wind"].get("budget_seconds"),
                        elapsed_seconds=downloads["wind"].get("elapsed_seconds"),
                        budget_exhausted=bool(downloads["wind"].get("budget_exhausted", False)),
                        failure_stage=str(downloads["wind"].get("failure_stage") or ""),
                    )
                self._write_download_failure_manifest(recipe_name, downloads, windows)
                raise RuntimeError(f"Source-history forcing download failed: {downloads['wind']}")
        else:
            wind_source_id = source_id_for_recipe_component(forcing_kind="wind", filename=wind_file)
            downloads["wind"] = service.download_required_forcing_record(wind_source_id)
            if downloads["wind"]["status"] not in {"downloaded", "cached"}:
                if (
                    downloads["wind"].get("upstream_outage_detected")
                    and self.forcing_outage_policy == FORCING_OUTAGE_POLICY_CONTINUE_DEGRADED
                ):
                    manifest_path = self._write_download_failure_manifest(
                        recipe_name,
                        downloads,
                        windows,
                        status="degraded_skipped_forcing_outage",
                        degraded_continue_used=True,
                        upstream_outage_detected=True,
                        missing_forcing_factors=[downloads["wind"]["forcing_factor"]],
                        stop_reason=str(downloads["wind"].get("error", "")),
                    )
                    raise ForcingOutagePhaseSkipped(
                        phase=self.phase_id,
                        workflow_mode=self.case.workflow_mode,
                        forcing_outage_policy=self.forcing_outage_policy,
                        reason=str(downloads["wind"].get("error", "")),
                        missing_forcing_factors=[downloads["wind"]["forcing_factor"]],
                        skipped_branch_ids=[scenario.scenario_id for scenario in A2_SCENARIOS],
                        manifest_path=str(manifest_path),
                        budget_seconds=downloads["wind"].get("budget_seconds"),
                        elapsed_seconds=downloads["wind"].get("elapsed_seconds"),
                        budget_exhausted=bool(downloads["wind"].get("budget_exhausted", False)),
                        failure_stage=str(downloads["wind"].get("failure_stage") or ""),
                    )
                self._write_download_failure_manifest(recipe_name, downloads, windows)
                raise RuntimeError(f"Source-history forcing download failed: {downloads['wind']}")
        if wave_file:
            wave_source_id = source_id_for_recipe_component(forcing_kind="wave", filename=wave_file)
            downloads["wave"] = service.download_required_forcing_record(wave_source_id)
            if downloads["wave"]["status"] not in {"downloaded", "cached"}:
                if (
                    downloads["wave"].get("upstream_outage_detected")
                    and self.forcing_outage_policy == FORCING_OUTAGE_POLICY_CONTINUE_DEGRADED
                ):
                    manifest_path = self._write_download_failure_manifest(
                        recipe_name,
                        downloads,
                        windows,
                        status="degraded_skipped_forcing_outage",
                        degraded_continue_used=True,
                        upstream_outage_detected=True,
                        missing_forcing_factors=[downloads["wave"]["forcing_factor"]],
                        stop_reason=str(downloads["wave"].get("error", "")),
                    )
                    raise ForcingOutagePhaseSkipped(
                        phase=self.phase_id,
                        workflow_mode=self.case.workflow_mode,
                        forcing_outage_policy=self.forcing_outage_policy,
                        reason=str(downloads["wave"].get("error", "")),
                        missing_forcing_factors=[downloads["wave"]["forcing_factor"]],
                        skipped_branch_ids=[scenario.scenario_id for scenario in A2_SCENARIOS],
                        manifest_path=str(manifest_path),
                        budget_seconds=downloads["wave"].get("budget_seconds"),
                        elapsed_seconds=downloads["wave"].get("elapsed_seconds"),
                        budget_exhausted=bool(downloads["wave"].get("budget_exhausted", False)),
                        failure_stage=str(downloads["wave"].get("failure_stage") or ""),
                    )
                self._write_download_failure_manifest(recipe_name, downloads, windows)
                raise RuntimeError(f"Source-history forcing download failed: {downloads['wave']}")
        candidates["downloads"] = downloads
        return candidates

    def _forcing_candidates_ready(self, candidates: dict, recipe: dict, windows: dict[str, dict[str, str]]) -> bool:
        required_start, required_end = self._required_window_bounds(windows)
        rows = self._forcing_rows(candidates, recipe, required_start, required_end)
        return bool(rows and all(row["status"] == "ready" for row in rows))

    def _forcing_rows(self, forcing_paths: dict, recipe: dict, required_start: pd.Timestamp, required_end: pd.Timestamp) -> list[dict]:
        rows = [
            {
                "forcing_kind": "current",
                **_forcing_time_and_vars(
                    Path(forcing_paths["currents"]),
                    _required_forcing_vars("currents", str(recipe["currents_file"])),
                    required_start,
                    required_end,
                ),
            },
            {
                "forcing_kind": "wind",
                **_forcing_time_and_vars(
                    Path(forcing_paths["wind"]),
                    _required_forcing_vars("wind", str(recipe["wind_file"])),
                    required_start,
                    required_end,
                ),
            },
        ]
        if forcing_paths.get("wave"):
            rows.append(
                {
                    "forcing_kind": "wave",
                    **_forcing_time_and_vars(
                        Path(forcing_paths["wave"]),
                        _required_forcing_vars("wave", str(recipe.get("wave_file") or "")),
                        required_start,
                        required_end,
                    ),
                }
            )
        return rows

    def _write_forcing_manifest(self, recipe_name: str, forcing_paths: dict, windows: dict[str, dict[str, str]]) -> dict:
        recipe = self._load_recipe(recipe_name)
        required_start, required_end = self._required_window_bounds(windows)
        rows = self._forcing_rows(forcing_paths, recipe, required_start, required_end)
        payload = {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "recipe": recipe_name,
            "phase": SOURCE_HISTORY_RECONSTRUCTION_DIR_NAME,
            "required_window_utc": {"start": _iso_z(required_start), "end": _iso_z(required_end)},
            "download_window": {
                "start_date": required_start.date().isoformat(),
                "end_date": (required_end + pd.Timedelta(days=1)).date().isoformat(),
            },
            "scenario_windows": windows,
            "date_composite_rule": "UTC calendar-date composites are used for comparability with init_mode_sensitivity_r1.",
            "rows": rows,
            "downloads": forcing_paths.get("downloads", {}),
            "forcing_outage_policy": self.forcing_outage_policy,
            "degraded_continue_used": False,
            "missing_forcing_factors": [],
            "rerun_required": False,
            "status": "ready" if all(row["status"] == "ready" for row in rows) else "insufficient",
        }
        _write_json(self.output_dir / "source_history_forcing_window_manifest.json", payload)
        pd.DataFrame(rows).to_csv(self.output_dir / "source_history_forcing_window_manifest.csv", index=False)
        return payload

    def _write_download_failure_manifest(
        self,
        recipe_name: str,
        downloads: dict,
        windows: dict[str, dict[str, str]],
        *,
        status: str = "failed_download",
        degraded_continue_used: bool = False,
        upstream_outage_detected: bool = False,
        missing_forcing_factors: list[str] | None = None,
        stop_reason: str = "",
    ) -> Path:
        payload = {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "recipe": recipe_name,
            "scenario_windows": windows,
            "downloads": downloads,
            "status": status,
            "forcing_outage_policy": self.forcing_outage_policy,
            "degraded_continue_used": degraded_continue_used,
            "upstream_outage_detected": upstream_outage_detected,
            "missing_forcing_factors": list(missing_forcing_factors or []),
            "rerun_required": bool(degraded_continue_used),
            "stop_reason": stop_reason,
        }
        manifest_json = self.output_dir / "source_history_forcing_window_manifest.json"
        _write_json(manifest_json, payload)
        pd.DataFrame(
            [
                {
                    "recipe": recipe_name,
                    "status": status,
                    "forcing_outage_policy": self.forcing_outage_policy,
                    "degraded_continue_used": degraded_continue_used,
                    "upstream_outage_detected": upstream_outage_detected,
                    "missing_forcing_factors": ";".join(missing_forcing_factors or []),
                    "rerun_required": bool(degraded_continue_used),
                    "stop_reason": stop_reason,
                    "downloads": json.dumps(downloads),
                }
            ]
        ).to_csv(
            self.output_dir / "source_history_forcing_window_manifest.csv",
            index=False,
        )
        return manifest_json

    def _write_failed_loading_audit(self, failures: list[dict]) -> None:
        payload = {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "status": "failed_before_source_history_forecast",
            "failures": failures,
        }
        _write_json(self.output_dir / "source_history_loading_audit.json", payload)
        pd.DataFrame(failures).to_csv(self.output_dir / "source_history_loading_audit.csv", index=False)

    def _resolve_or_run_scenario(self, scenario: SourceHistoryScenario, selection, forcing_paths: dict) -> dict:
        window = scenario.release_window()
        model_run_name = f"{self.case.run_name}/{SOURCE_HISTORY_RECONSTRUCTION_DIR_NAME}/{scenario.output_slug}/model_run"
        model_dir = get_case_output_dir(model_run_name)
        if model_dir_complete_for_recipe(model_dir, selection.recipe) and not self.force_rerun:
            return {
                "scenario_id": scenario.scenario_id,
                "status": "reused_existing_scenario",
                "model_dir": str(model_dir),
                "run_name": model_run_name,
                "forecast_result": {"status": "reused_existing_scenario"},
                **window,
            }

        start_lat, start_lon, start_time = resolve_spill_origin()
        simulation_start = _normalize_utc(window["simulation_start_utc"])
        simulation_end = _normalize_utc(window["simulation_end_utc"])
        duration_hours = int(math.ceil((simulation_end - simulation_start).total_seconds() / 3600.0))
        snapshot_hours = sorted(set([24, 48, 72, 96, 120, 144, duration_hours]))
        seed_overrides = {
            "initialization_mode": "source_history_reconstruction",
            "point_release_surrogate": "exact_point_release",
            "release_start_utc": window["release_start_utc"],
            "release_end_utc": window["release_end_utc"],
            "release_duration_hours": scenario.release_duration_hours,
            "disable_start_time_offsets": True,
            "release_history_assumption": (
                "Release end is anchored to the March 3 observed-slick timestamp; earlier exact event hour is "
                "treated as a reconstruction sensitivity assumption."
            ),
        }
        forecast_result = run_official_spill_forecast(
            selection=selection,
            start_time=start_time,
            start_lat=start_lat,
            start_lon=start_lon,
            output_run_name=model_run_name,
            forcing_override=forcing_paths,
            historical_baseline_provenance={
                "recipe": selection.recipe,
                "source_kind": selection.source_kind,
                "source_path": selection.source_path,
                "note": selection.note,
            },
            simulation_start_utc=window["simulation_start_utc"],
            simulation_end_utc=window["simulation_end_utc"],
            snapshot_hours=snapshot_hours,
            date_composite_dates=list(self.date_composite_dates),
            transport_overrides={
                "coastline_action": self.retention_scenario.coastline_action,
                "coastline_approximation_precision": self.retention_scenario.coastline_approximation_precision,
                "time_step_minutes": self.retention_scenario.time_step_minutes,
            },
            seed_overrides=seed_overrides,
            sensitivity_context={
                "track": SOURCE_HISTORY_RECONSTRUCTION_DIR_NAME,
                "scenario_id": scenario.scenario_id,
                "initialization_mode": "source_history_reconstruction",
                "selected_transport_retention_mode": "R1",
                "coastline_action": self.retention_scenario.coastline_action,
                "march3_checkpoint_separate_from_forecast_skill": True,
                "medium_tier_not_run": True,
            },
        )
        if forecast_result.get("status") != "success":
            raise RuntimeError(f"Source-history scenario {scenario.scenario_id} failed: {forecast_result}")
        return {
            "scenario_id": scenario.scenario_id,
            "status": forecast_result.get("status", "unknown"),
            "model_dir": str(model_dir),
            "run_name": model_run_name,
            "forecast_result": forecast_result,
            **window,
        }

    def _build_date_composites(self, scenario: SourceHistoryScenario, model_dir: Path) -> Path:
        composite_dir = self.output_dir / scenario.output_slug / "forecast_datecomposites"
        composite_dir.mkdir(parents=True, exist_ok=True)
        member_paths = sorted((model_dir / "ensemble").glob("member_*.nc"))
        if not member_paths:
            raise FileNotFoundError(f"No ensemble members found for {scenario.scenario_id}: {model_dir / 'ensemble'}")
        for date in self.date_composite_dates:
            probability = self._date_composite_probability(member_paths, date)
            probability = apply_ocean_mask(probability, sea_mask=self.sea_mask, fill_value=0.0)
            p50 = apply_ocean_mask((probability >= 0.5).astype(np.float32), sea_mask=self.sea_mask, fill_value=0.0)
            save_raster(self.grid, probability.astype(np.float32), composite_dir / f"prob_presence_{date}_datecomposite.tif")
            save_raster(self.grid, p50.astype(np.float32), composite_dir / f"mask_p50_{date}_datecomposite.tif")
        return composite_dir

    def _date_composite_probability(self, member_paths: list[Path], target_date: str) -> np.ndarray:
        masks = [self._member_utc_date_mask(path, target_date) for path in member_paths]
        return np.mean(np.stack(masks, axis=0), axis=0).astype(np.float32)

    def _member_utc_date_mask(self, nc_path: Path, target_date: str) -> np.ndarray:
        target = pd.Timestamp(target_date).date()
        composite = np.zeros((self.grid.height, self.grid.width), dtype=np.float32)
        with xr.open_dataset(nc_path) as ds:
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
                    self.grid,
                    lon[valid],
                    lat[valid],
                    np.ones(int(np.count_nonzero(valid)), dtype=np.float32),
                )
                composite = np.maximum(composite, hits)
        return apply_ocean_mask(composite.astype(np.float32), sea_mask=self.sea_mask, fill_value=0.0)

    def _materialize_missing_march5_union_if_available(self) -> str:
        union_dir = self.case_output / "phase3b_multidate_public" / "date_union_obs_masks"
        union_path = union_dir / "obs_union_2023-03-05.tif"
        if "2023-03-05" not in self.validation_dates:
            if union_path.exists():
                union_path.unlink()
            return ""
        if union_path.exists():
            return str(union_path)
        accepted = self._accepted_public_obs_for_dates(["2023-03-05"])
        if accepted.empty:
            return ""
        union = np.zeros((self.grid.height, self.grid.width), dtype=np.float32)
        for _, row in accepted.iterrows():
            mask_path = Path(str(row["appendix_obs_mask"]))
            if mask_path.exists():
                union = np.maximum(union, self.helper._load_binary_score_mask(mask_path))
        union = apply_ocean_mask(union, sea_mask=self.sea_mask, fill_value=0.0)
        if np.count_nonzero(union > 0) == 0:
            return ""
        union_dir.mkdir(parents=True, exist_ok=True)
        save_raster(self.grid, union.astype(np.float32), union_path)
        return str(union_path)

    def _accepted_public_obs_for_dates(self, dates: list[str]) -> pd.DataFrame:
        inventory_path = self.case_output / "public_obs_appendix" / "public_obs_inventory.csv"
        if not inventory_path.exists():
            return pd.DataFrame()
        inventory = pd.read_csv(inventory_path)
        if "appendix_obs_mask" not in inventory.columns:
            return pd.DataFrame()
        mask_exists = inventory["appendix_obs_mask"].map(
            lambda value: False if pd.isna(value) else Path(str(value).strip()).exists()
        )
        accepted = inventory[
            inventory["obs_date"].astype(str).isin(dates)
            & inventory["accept_for_appendix_quantitative"].map(_truthy)
            & mask_exists
        ].copy()
        return accepted.sort_values(["obs_date", "source_name"])

    def _build_pairings(self, scenario: SourceHistoryScenario, composite_dir: Path) -> list[dict]:
        pairings = [
            self._pair(
                scenario=scenario,
                pair_role="march3_reconstruction_checkpoint",
                pair_id=f"{scenario.scenario_id}_march3_checkpoint",
                obs_date="2023-03-03",
                forecast_path=composite_dir / "mask_p50_2023-03-03_datecomposite.tif",
                observation_path=self._build_march3_checkpoint_obs_union(),
                source_semantics="reconstruction_checkpoint_only_not_forecast_skill",
            ),
            self._pair(
                scenario=scenario,
                pair_role="strict_march6",
                pair_id=f"{scenario.scenario_id}_strict_march6",
                obs_date="2023-03-06",
                forecast_path=composite_dir / "mask_p50_2023-03-06_datecomposite.tif",
                observation_path=Path("data/arcgis") / self.case.run_name / "obs_mask_2023-03-06.tif",
                source_semantics="strict_single_date_stress_test_march6_p50_vs_arcgis_obsmask",
            ),
        ]
        for date in self.validation_dates:
            obs_path = self._date_union_obs_path(date)
            if not obs_path.exists():
                continue
            pairings.append(
                self._pair(
                    scenario=scenario,
                    pair_role="forecast_skill_date_union",
                    pair_id=f"{scenario.scenario_id}_date_union_{date}",
                    obs_date=date,
                    forecast_path=composite_dir / f"mask_p50_{date}_datecomposite.tif",
                    observation_path=obs_path,
                    source_semantics=f"source_history_forecast_skill_{date}_public_date_union_vs_p50",
                )
            )
        pairings.append(
            self._pair(
                scenario=scenario,
                pair_role="eventcorridor_march4_6",
                pair_id=f"{scenario.scenario_id}_eventcorridor_{self.eventcorridor_label}",
                obs_date=self.eventcorridor_label,
                forecast_path=self._build_eventcorridor_model_union(scenario, composite_dir),
                observation_path=self._build_eventcorridor_obs_union(),
                source_semantics="eventcorridor_public_observation_derived_union_excluding_march3_initialization",
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
        *,
        scenario: SourceHistoryScenario,
        pair_role: str,
        pair_id: str,
        obs_date: str,
        forecast_path: Path,
        observation_path: Path,
        source_semantics: str,
    ) -> dict:
        window = scenario.release_window()
        return {
            "scenario_id": scenario.scenario_id,
            "scenario_slug": scenario.output_slug,
            "initialization_mode": "source_history_reconstruction",
            "pair_id": pair_id,
            "pair_role": pair_role,
            "obs_date": obs_date,
            "forecast_path": str(forecast_path),
            "observation_path": str(observation_path),
            "metric": "FSS",
            "windows_km": "1,3,5,10",
            "source_semantics": source_semantics,
            "march3_checkpoint_counts_as_forecast_skill": False if pair_role == "march3_reconstruction_checkpoint" else "",
            "selected_transport_retention_mode": "R1",
            "coastline_action": self.retention_scenario.coastline_action,
            "release_start_utc": window["release_start_utc"],
            "release_end_utc": window["release_end_utc"],
            "release_duration_hours": int(scenario.release_duration_hours),
            "point_release_surrogate": "exact_point_release",
            "date_composite_rule": "UTC calendar-date composite; p50 is probability >= 0.50 across members.",
        }

    def _date_union_obs_path(self, date: str) -> Path:
        return self.case_output / "phase3b_multidate_public" / "date_union_obs_masks" / f"obs_union_{date}.tif"

    def _build_march3_checkpoint_obs_union(self) -> Path:
        path = self.output_dir / "march3_checkpoint_obs_union_2023-03-03.tif"
        accepted = self._accepted_public_obs_for_dates(["2023-03-03"])
        if accepted.empty:
            fallback = self.case.initialization_layer.mask_path(self.case.run_name)
            if fallback.exists():
                return fallback
            raise FileNotFoundError("No March 3 checkpoint observation masks are available.")
        union = np.zeros((self.grid.height, self.grid.width), dtype=np.float32)
        for _, row in accepted.iterrows():
            union = np.maximum(union, self.helper._load_binary_score_mask(Path(str(row["appendix_obs_mask"]))))
        union = apply_ocean_mask(union, sea_mask=self.sea_mask, fill_value=0.0)
        save_raster(self.grid, union.astype(np.float32), path)
        return path

    def _build_eventcorridor_obs_union(self) -> Path:
        path = self.output_dir / f"eventcorridor_obs_union_{self.eventcorridor_label}.tif"
        union = np.zeros((self.grid.height, self.grid.width), dtype=np.float32)
        used = 0
        for date in self.validation_dates:
            obs_path = self._date_union_obs_path(date)
            if obs_path.exists():
                union = np.maximum(union, self.helper._load_binary_score_mask(obs_path))
                used += 1
        if used == 0:
            raise FileNotFoundError(
                f"No accepted date-union observation masks are available for {self.eventcorridor_label}."
            )
        union = apply_ocean_mask(union, sea_mask=self.sea_mask, fill_value=0.0)
        save_raster(self.grid, union.astype(np.float32), path)
        return path

    def _build_eventcorridor_model_union(self, scenario: SourceHistoryScenario, composite_dir: Path) -> Path:
        union = np.zeros((self.grid.height, self.grid.width), dtype=np.float32)
        for date in self.validation_dates:
            mask_path = composite_dir / f"mask_p50_{date}_datecomposite.tif"
            if mask_path.exists():
                union = np.maximum(union, self.helper._load_binary_score_mask(mask_path))
        union = apply_ocean_mask(union, sea_mask=self.sea_mask, fill_value=0.0)
        path = self.output_dir / scenario.output_slug / f"eventcorridor_model_union_{self.eventcorridor_label}.tif"
        save_raster(self.grid, union.astype(np.float32), path)
        return path

    def _score_pairings(self, scenario: SourceHistoryScenario, pairings: list[dict]) -> tuple[pd.DataFrame, pd.DataFrame]:
        fss_rows: list[dict] = []
        diagnostics_rows: list[dict] = []
        precheck_dir = self.output_dir / "precheck" / scenario.output_slug
        precheck_dir.mkdir(parents=True, exist_ok=True)
        for pair in pairings:
            forecast_path = Path(str(pair["forecast_path"]))
            observation_path = Path(str(pair["observation_path"]))
            precheck = precheck_same_grid(
                forecast=forecast_path,
                target=observation_path,
                report_base_path=precheck_dir / pair["pair_id"],
            )
            if not precheck.passed:
                raise RuntimeError(f"Source-history same-grid precheck failed for {pair['pair_id']}: {precheck.json_report_path}")
            forecast_mask = self.helper._load_binary_score_mask(forecast_path)
            obs_mask = self.helper._load_binary_score_mask(observation_path)
            diagnostics = self.helper._compute_mask_diagnostics(forecast_mask, obs_mask)
            scored = {
                **pair,
                **diagnostics,
                "precheck_csv": str(precheck.csv_report_path),
                "precheck_json": str(precheck.json_report_path),
            }
            diagnostics_rows.append(scored)
            for window_km in OFFICIAL_PHASE3B_WINDOWS_KM:
                window_cells = self.helper._window_km_to_cells(window_km)
                fss = float(
                    np.clip(
                        calculate_fss(
                            forecast_mask,
                            obs_mask,
                            window=window_cells,
                            valid_mask=self.valid_mask,
                        ),
                        0.0,
                        1.0,
                    )
                )
                fss_rows.append(
                    {
                        **pair,
                        "window_km": int(window_km),
                        "window_cells": int(window_cells),
                        "fss": fss,
                        "precheck_csv": str(precheck.csv_report_path),
                        "precheck_json": str(precheck.json_report_path),
                    }
                )
        return pd.DataFrame(fss_rows), pd.DataFrame(diagnostics_rows)

    def _augment_probability_diagnostics(
        self,
        scenario: SourceHistoryScenario,
        diagnostics_df: pd.DataFrame,
        composite_dir: Path,
    ) -> pd.DataFrame:
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
            record["scenario_id"] = scenario.scenario_id
            record["scenario_slug"] = scenario.output_slug
            rows.append(record)
        return pd.DataFrame(rows)

    def _build_hourly_diagnostics_for_window(self, scenario: SourceHistoryScenario, model_dir: Path) -> pd.DataFrame:
        previous_window = dict(self.retention.window)
        try:
            self.retention.window = scenario.release_window()
            hourly = self.retention._build_hourly_diagnostics(
                RetentionScenario(
                    scenario_id=scenario.scenario_id,
                    slug=scenario.output_slug,
                    description=scenario.description,
                    coastline_action=self.retention_scenario.coastline_action,
                    coastline_approximation_precision=self.retention_scenario.coastline_approximation_precision,
                    time_step_minutes=self.retention_scenario.time_step_minutes,
                ),
                model_dir,
            )
        finally:
            self.retention.window = previous_window
        hourly["release_duration_hours"] = int(scenario.release_duration_hours)
        return hourly

    def _summarize_scenario(
        self,
        scenario: SourceHistoryScenario,
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
            window = scenario.release_window()
            record.update(
                {
                    "scenario_id": scenario.scenario_id,
                    "scenario_slug": scenario.output_slug,
                    "scenario_description": scenario.description,
                    "initialization_mode": "source_history_reconstruction",
                    "source_geometry_path": str(self.case.provenance_layer.processed_vector_path(self.case.run_name)),
                    "point_release_surrogate": "exact_point_release",
                    "release_start_utc": window["release_start_utc"],
                    "release_end_utc": window["release_end_utc"],
                    "release_duration_hours": int(scenario.release_duration_hours),
                    "simulation_start_utc": window["simulation_start_utc"],
                    "simulation_end_utc": window["simulation_end_utc"],
                    "transport_model": "oceandrift",
                    "provisional_transport_model": True,
                    "selected_transport_retention_mode": "R1",
                    "coastline_action": self.retention_scenario.coastline_action,
                    "coastline_approximation_precision": self.retention_scenario.coastline_approximation_precision,
                    "time_step_minutes": self.retention_scenario.time_step_minutes,
                    "recipe_used": resolve_recipe_selection().recipe,
                    "element_count_used": self._element_count_from_manifest(Path(run_result["model_dir"])),
                    "shoreline_mask_signature": self.grid.spec.shoreline_mask_signature,
                    "retained_from_official_rerun_r1": True,
                    "ensemble_start_time_offsets_disabled": True,
                    "r3_diagnostic_only": True,
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

    def _copy_loading_audits(self) -> None:
        rows = []
        payloads = []
        for scenario in A2_SCENARIOS:
            model_dir = get_case_output_dir(
                f"{self.case.run_name}/{SOURCE_HISTORY_RECONSTRUCTION_DIR_NAME}/{scenario.output_slug}/model_run"
            )
            json_path = model_dir / "forecast" / "phase2_loading_audit.json"
            csv_path = model_dir / "forecast" / "phase2_loading_audit.csv"
            if json_path.exists():
                payload = _read_json(json_path)
                payload["scenario_id"] = scenario.scenario_id
                payloads.append(payload)
            if csv_path.exists():
                frame = pd.read_csv(csv_path)
                frame["scenario_id"] = scenario.scenario_id
                rows.append(frame)
        _write_json(self.output_dir / "source_history_loading_audit.json", {"scenario_audits": payloads})
        combined = pd.concat(rows, ignore_index=True) if rows else pd.DataFrame([{"status": "missing_model_loading_audits"}])
        _write_csv(self.output_dir / "source_history_loading_audit.csv", combined)

    def _write_outputs(
        self,
        summary_df: pd.DataFrame,
        diagnostics_df: pd.DataFrame,
        hourly_df: pd.DataFrame,
        pairings_df: pd.DataFrame,
        fss_df: pd.DataFrame,
    ) -> dict[str, Path]:
        paths = {
            "summary": self.output_dir / "source_history_reconstruction_r1_summary.csv",
            "diagnostics": self.output_dir / "source_history_reconstruction_r1_diagnostics.csv",
            "hourly": self.output_dir / "source_history_reconstruction_r1_hourly_timeseries.csv",
            "pairing": self.output_dir / "source_history_reconstruction_r1_pairing_manifest.csv",
            "fss": self.output_dir / "source_history_reconstruction_r1_fss_by_window.csv",
        }
        _write_csv(paths["summary"], summary_df)
        _write_csv(paths["diagnostics"], diagnostics_df)
        _write_csv(paths["hourly"], hourly_df)
        _write_csv(paths["pairing"], pairings_df)
        _write_csv(paths["fss"], fss_df)
        return paths

    def _write_manifest(
        self,
        *,
        official_manifest: dict,
        init_manifest: dict,
        forcing_manifest: dict,
        scenario_results: list[dict],
        summary_df: pd.DataFrame,
        recommendation: dict,
        paths: dict[str, Path],
        qa_paths: dict[str, Path],
        report_path: Path,
        march5_union: str,
    ) -> Path:
        path = self.output_dir / "source_history_reconstruction_r1_run_manifest.json"
        payload = {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "workflow_mode": self.case.workflow_mode,
            "run_name": self.case.run_name,
            "phase": SOURCE_HISTORY_RECONSTRUCTION_DIR_NAME,
            "purpose": "A2 source-history reconstruction sensitivity under selected R1 retention.",
            "selected_transport_retention": {
                "scenario": "R1",
                "coastline_action": self.retention_scenario.coastline_action,
                "selected_from": str(self.case_output / TRANSPORT_RETENTION_DIR_NAME / "transport_retention_run_manifest.json"),
                "official_rerun_r1_manifest": str(self.case_output / OFFICIAL_RERUN_R1_DIR_NAME / "official_rerun_r1_run_manifest.json"),
                "r3_diagnostic_only": True,
            },
            "validation_dates_used_for_forecast_skill": self.validation_dates,
            "date_composite_dates_materialized": self.date_composite_dates,
            "scenario_matrix": [asdict(scenario) | scenario.release_window() for scenario in A2_SCENARIOS],
            "forcing_manifest": forcing_manifest,
            "scenario_results": scenario_results,
            "march5_date_union_materialized": march5_union,
            "guardrails": {
                "strict_march6_pairing_unchanged": True,
                "b_observation_initialized_track_unchanged": True,
                "within_horizon_public_semantics_unchanged": True,
                "short_extended_semantics_unchanged": True,
                "medium_tier_not_run": True,
                "public_sources_not_added": True,
                "scoring_rules_unchanged": True,
                "march3_checkpoint_not_forecast_skill": True,
            },
            "official_rerun_r1_recommendation": official_manifest.get("recommended_next_branch", ""),
            "init_mode_sensitivity_recommendation": init_manifest.get("recommendation", {}),
            "recommendation": recommendation,
            "summary": summary_df.to_dict(orient="records"),
            "artifacts": {
                **{key: str(value) for key, value in paths.items()},
                **{key: str(value) for key, value in qa_paths.items()},
                "report": str(report_path),
                "forcing_manifest_json": str(self.output_dir / "source_history_forcing_window_manifest.json"),
                "forcing_manifest_csv": str(self.output_dir / "source_history_forcing_window_manifest.csv"),
                "loading_audit_json": str(self.output_dir / "source_history_loading_audit.json"),
                "loading_audit_csv": str(self.output_dir / "source_history_loading_audit.csv"),
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
        path = self.output_dir / "source_history_reconstruction_r1_report.md"
        strict = summary_df[summary_df["pair_role"] == "strict_march6"].copy()
        event = summary_df[summary_df["pair_role"] == "eventcorridor_march4_6"].copy()
        checkpoint = summary_df[summary_df["pair_role"] == "march3_reconstruction_checkpoint"].copy()
        lines = [
            "# Source-History Reconstruction R1",
            "",
            "This controlled A2 sensitivity uses the provenance source point as the active release geometry while holding the selected R1 retention configuration, forcing family, scoring grid, shoreline mask, and scoring rules fixed.",
            "",
            f"- Best A2 scenario: `{recommendation['best_a2_scenario']}`",
            f"- Recommendation: `{recommendation['recommendation']}`",
            f"- March 3 checkpoint improved: `{recommendation['march3_checkpoint_improved']}`",
            f"- Strict March 6 materially improved: `{recommendation['strict_march6_materially_improved']}`",
            f"- {self.eventcorridor_label} event corridor materially improved: `{recommendation['eventcorridor_materially_improved']}`",
            f"- Convergence should be next: `{recommendation['convergence_should_be_next']}`",
            f"- Reason: {recommendation['reason']}",
            "",
            "March 3 is reported only as a reconstruction checkpoint and is not counted as a normal forecast-skill date.",
            "",
            "## March 3 Checkpoint",
            "",
        ]
        lines.extend(self._markdown_table(checkpoint[self._summary_columns()]))
        lines.extend(["", "## Strict March 6", ""])
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
            "scenario_id",
            "release_duration_hours",
            "forecast_nonzero_cells",
            "obs_nonzero_cells",
            "centroid_distance_m",
            "nearest_distance_to_obs_m",
            "iou",
            "dice",
            "max_probability",
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
            outputs["qa_source_history_reconstruction_r1_overlays"] = overlays
        if tracks:
            outputs["qa_source_history_reconstruction_r1_tracks"] = tracks
        return outputs

    def _plot_overlays(self, diagnostics_df: pd.DataFrame) -> Path | None:
        selected = diagnostics_df[diagnostics_df["pair_role"].isin(["march3_reconstruction_checkpoint", "strict_march6", "eventcorridor_march4_6"])].copy()
        if selected.empty:
            return None
        path = self.output_dir / "qa_source_history_reconstruction_r1_overlays.png"
        rows = ["march3_reconstruction_checkpoint", "strict_march6", "eventcorridor_march4_6"]
        cols = [scenario.scenario_id for scenario in A2_SCENARIOS]
        fig, axes = plt.subplots(len(rows), len(cols), figsize=(4.5 * len(cols), 4.5 * len(rows)))
        axes_array = np.asarray(axes).reshape(len(rows), len(cols))
        for row_index, pair_role in enumerate(rows):
            for col_index, scenario_id in enumerate(cols):
                ax = axes_array[row_index, col_index]
                match = selected[(selected["pair_role"] == pair_role) & (selected["scenario_id"] == scenario_id)]
                if match.empty:
                    ax.axis("off")
                    continue
                item = match.iloc[0]
                forecast = self.helper._load_binary_score_mask(Path(item["forecast_path"]))
                obs = self.helper._load_binary_score_mask(Path(item["observation_path"]))
                canvas = np.ones((forecast.shape[0], forecast.shape[1], 3), dtype=np.float32)
                canvas[obs > 0] = [0.1, 0.35, 0.95]
                canvas[forecast > 0] = [0.95, 0.35, 0.1]
                canvas[(obs > 0) & (forecast > 0)] = [0.1, 0.65, 0.25]
                ax.imshow(canvas)
                ax.set_title(f"{scenario_id} {pair_role}")
                ax.axis("off")
        fig.tight_layout()
        fig.savefig(path, dpi=150, bbox_inches="tight")
        plt.close(fig)
        return path

    def _plot_tracks(self) -> Path | None:
        path = self.output_dir / "qa_source_history_reconstruction_r1_tracks.png"
        fig, ax = plt.subplots(figsize=(8, 8))
        ax.imshow(
            self.valid_mask,
            cmap="Blues",
            alpha=0.25,
            extent=[self.grid.min_x, self.grid.max_x, self.grid.min_y, self.grid.max_y],
            origin="upper",
        )
        for scenario in A2_SCENARIOS:
            model_dir = get_case_output_dir(
                f"{self.case.run_name}/{SOURCE_HISTORY_RECONSTRUCTION_DIR_NAME}/{scenario.output_slug}/model_run"
            )
            member = next(iter(sorted((model_dir / "ensemble").glob("member_*.nc"))), None)
            if member is None:
                continue
            with xr.open_dataset(member) as ds:
                lon = np.asarray(ds["lon"].values)
                lat = np.asarray(ds["lat"].values)
                trajectories = np.linspace(0, lon.shape[0] - 1, min(15, lon.shape[0]), dtype=int)
                for idx in trajectories:
                    finite = np.isfinite(lon[idx]) & np.isfinite(lat[idx])
                    if np.any(finite):
                        x_vals, y_vals = project_points_to_grid(self.grid, lon[idx][finite], lat[idx][finite])
                        ax.plot(x_vals, y_vals, linewidth=0.5, alpha=0.2, label=scenario.scenario_id if idx == trajectories[0] else None)
        ax.set_title("Sample source-history tracks")
        ax.set_xlim(self.grid.min_x, self.grid.max_x)
        ax.set_ylim(self.grid.min_y, self.grid.max_y)
        ax.set_aspect("equal")
        ax.legend(loc="upper right")
        fig.tight_layout()
        fig.savefig(path, dpi=150, bbox_inches="tight")
        plt.close(fig)
        return path


def run_source_history_reconstruction_r1() -> dict:
    return SourceHistoryReconstructionR1Service().run()


if __name__ == "__main__":  # pragma: no cover
    print(json.dumps(run_source_history_reconstruction_r1(), indent=2, default=_json_default))
