"""Promote the selected R1 retention configuration into an official rerun pack."""

from __future__ import annotations

import json
import math
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import yaml

from src.core.case_context import get_case_context
from src.helpers.raster import project_points_to_grid
from src.services.ensemble import run_official_spill_forecast
from src.services.phase3b_extended_public_scored import EVENT_CORRIDOR_DATES
from src.services.transport_retention_fix import (
    MARCH6_DATE_START_UTC,
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


OFFICIAL_RERUN_R1_DIR_NAME = "official_rerun_r1"
OFFICIAL_RETENTION_CONFIG_PATH = Path("config/ensemble.yaml")
SELECTED_RETENTION_MODE = "R1"
FORCE_RERUN_ENV = "OFFICIAL_RERUN_R1_FORCE_RERUN"


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


def load_official_retention_config(config_path: Path = OFFICIAL_RETENTION_CONFIG_PATH) -> dict:
    """Load the selected official retention mode from machine-readable config."""
    if not config_path.exists():
        raise FileNotFoundError(f"Official retention config not found: {config_path}")
    with open(config_path, "r", encoding="utf-8") as handle:
        config = yaml.safe_load(handle) or {}
    retention = config.get("official_retention") or {}
    selected_mode = str(os.environ.get("OFFICIAL_RETENTION_MODE", retention.get("selected_mode", SELECTED_RETENTION_MODE))).strip()
    scenarios = retention.get("scenarios") or {}
    selected = scenarios.get(selected_mode)
    if not selected:
        raise RuntimeError(f"official_retention.scenarios.{selected_mode} is not configured in {config_path}.")
    if _truthy(selected.get("diagnostic_only", False)):
        raise RuntimeError(f"Configured official retention mode {selected_mode} is diagnostic-only and cannot be promoted.")
    return {
        "selected_mode": selected_mode,
        "selected_phase": retention.get("selected_phase", OFFICIAL_RERUN_R1_DIR_NAME),
        "selected_from": retention.get("selected_from", ""),
        "selection_reason": retention.get("selection_reason", ""),
        "medium_tier_blocked_until_selected_rerun": bool(retention.get("medium_tier_blocked_until_selected_rerun", True)),
        "coastline_action": str(selected.get("coastline_action", "previous")),
        "coastline_approximation_precision": float(selected.get("coastline_approximation_precision", 0.001)),
        "time_step_minutes": int(selected.get("time_step_minutes", 60)),
        "diagnostic_only": _truthy(selected.get("diagnostic_only", False)),
        "thesis_final_candidate": _truthy(selected.get("thesis_final_candidate", True)),
        "all_scenarios": scenarios,
    }


def recommend_next_branch(strict_row: dict) -> str:
    """Choose exactly one next branch from the rerun outcome."""
    fss_values = [_float_or_nan(strict_row.get(f"fss_{window}km")) for window in (1, 3, 5, 10)]
    mean_fss = float(np.nanmean(fss_values)) if any(np.isfinite(value) for value in fss_values) else 0.0
    p50_cells = _float_or_nan(strict_row.get("forecast_nonzero_cells"))
    survives = _truthy(strict_row.get("survives_to_strict_validation", False))
    if mean_fss > 0.01 or _float_or_nan(strict_row.get("iou")) > 0:
        return "medium extended tier"
    if p50_cells > 0 and survives:
        return "init-mode sensitivity (A1 vs B)"
    return "convergence study"


class OfficialRerunR1Service:
    def __init__(self):
        self.case = get_case_context()
        if not self.case.is_official:
            raise RuntimeError("official_rerun_r1 is only supported for official Mindoro workflows.")
        if xr is None:
            raise ImportError("xarray is required for official_rerun_r1 diagnostics.")
        self.case_output = get_case_output_dir(self.case.run_name)
        self.output_dir = self.case_output / OFFICIAL_RERUN_R1_DIR_NAME
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.retention = TransportRetentionFixService()
        self.retention.output_dir = self.output_dir
        self.retention.force_rerun = _truthy(os.environ.get(FORCE_RERUN_ENV, ""))
        self.retention_config = load_official_retention_config()
        self.scenario = RetentionScenario(
            scenario_id=self.retention_config["selected_mode"],
            slug="selected_previous",
            description="Selected official rerun candidate from transport_retention_fix: coastline_action=previous.",
            coastline_action=self.retention_config["coastline_action"],
            coastline_approximation_precision=self.retention_config["coastline_approximation_precision"],
            time_step_minutes=self.retention_config["time_step_minutes"],
            diagnostic_only=False,
        )
        if self.scenario.scenario_id != SELECTED_RETENTION_MODE:
            raise RuntimeError(f"official_rerun_r1 expected selected mode R1, got {self.scenario.scenario_id}.")

    def run(self) -> dict:
        transport_manifest = _read_json(self.case_output / TRANSPORT_RETENTION_DIR_NAME / "transport_retention_run_manifest.json")
        self._validate_transport_selection(transport_manifest)
        model_result = self._resolve_or_run_model(transport_manifest)
        model_dir = Path(model_result["model_dir"])
        composite_dir = self.retention._build_local_date_composites(self.scenario, model_dir)
        pairings = self.retention._build_pairings(self.scenario, model_dir, composite_dir)
        pairing_df = self._build_pairing_manifest(pairings, model_result)
        fss_df, diagnostics_df = self.retention._score_pairings(self.scenario, pairings)
        diagnostics_df = self._augment_probability_diagnostics(diagnostics_df, model_dir, composite_dir)
        hourly_df = self.retention._build_hourly_diagnostics(self.scenario, model_dir)
        scenario_summary = self.retention._summarize_scenario(self.scenario, model_result, hourly_df, fss_df, diagnostics_df)
        summary_df = self._build_summary(diagnostics_df, fss_df, scenario_summary)
        before_after_df = self._build_before_after(summary_df)

        paths = self._write_outputs(pairing_df, fss_df, diagnostics_df, hourly_df, summary_df, before_after_df)
        qa_paths = self._write_qa(hourly_df, diagnostics_df, model_dir)
        recommendation = recommend_next_branch(summary_df[summary_df["pair_role"] == "strict_march6"].iloc[0].to_dict())
        report_path = self._write_report(summary_df, before_after_df, recommendation, paths, qa_paths, model_result)
        manifest_path = self._write_manifest(
            transport_manifest=transport_manifest,
            model_result=model_result,
            scenario_summary=scenario_summary,
            summary_df=summary_df,
            before_after_df=before_after_df,
            recommendation=recommendation,
            paths=paths,
            qa_paths=qa_paths,
            report_path=report_path,
        )
        strict = summary_df[summary_df["pair_role"] == "strict_march6"].iloc[0].to_dict()
        event = summary_df[summary_df["pair_role"] == "short_extended_eventcorridor"].iloc[0].to_dict()
        return {
            "output_dir": self.output_dir,
            "selected_scenario": self.scenario.scenario_id,
            "strict_march6": strict,
            "eventcorridor": event,
            "summary_csv": paths["summary"],
            "before_after_csv": paths["before_after"],
            "run_manifest": manifest_path,
            "report_md": report_path,
            "recommended_next_branch": recommendation,
            "retained_from_transport_retention_fix": model_result["retained_from_transport_retention_fix"],
        }

    def _validate_transport_selection(self, manifest: dict) -> None:
        recommendation = manifest.get("recommendation") or {}
        if str(recommendation.get("best_scenario")) != SELECTED_RETENTION_MODE:
            raise RuntimeError("transport_retention_fix did not select R1; refusing official_rerun_r1 promotion.")
        scenarios = {item.get("scenario_id"): item for item in manifest.get("scenarios", [])}
        if not scenarios.get("R3", {}).get("diagnostic_only", False):
            raise RuntimeError("transport_retention_fix manifest must keep R3 marked diagnostic_only=true.")

    def _resolve_or_run_model(self, transport_manifest: dict) -> dict:
        selection = resolve_recipe_selection()
        official_model_dir = self.case_output / OFFICIAL_RERUN_R1_DIR_NAME / "model_run"
        if model_dir_complete_for_recipe(official_model_dir, selection.recipe) and not self.retention.force_rerun:
            return {
                "status": "reused_existing_official_rerun_r1",
                "model_dir": str(official_model_dir),
                "run_name": f"{self.case.run_name}/{OFFICIAL_RERUN_R1_DIR_NAME}/model_run",
                "retained_from_transport_retention_fix": False,
                "forecast_result": {"status": "reused_existing_official_rerun_r1"},
            }

        source_model_dir = self._transport_r1_model_dir(transport_manifest)
        if model_dir_complete_for_recipe(source_model_dir, selection.recipe) and not self.retention.force_rerun:
            return {
                "status": "retained_from_transport_retention_fix_r1",
                "model_dir": str(source_model_dir),
                "run_name": f"{self.case.run_name}/{TRANSPORT_RETENTION_DIR_NAME}/R1_previous/model_run",
                "retained_from_transport_retention_fix": True,
                "forecast_result": {"status": "retained_from_transport_retention_fix_r1"},
            }

        forcing_paths = self.retention._resolve_forcing_paths(selection.recipe)
        window = transport_manifest.get("window") or self.retention.window
        start_lat, start_lon, start_time = resolve_spill_origin()
        simulation_start = _normalize_utc(window["simulation_start_utc"])
        simulation_end = _normalize_utc(window["simulation_end_utc"])
        duration_hours = int(math.ceil((simulation_end - simulation_start).total_seconds() / 3600.0))
        snapshot_hours = sorted(set([24, 48, 72, 96, 120, 144, duration_hours]))
        model_run_name = f"{self.case.run_name}/{OFFICIAL_RERUN_R1_DIR_NAME}/model_run"
        forecast_result = run_official_spill_forecast(
            selection=selection,
            start_time=start_time,
            start_lat=start_lat,
            start_lon=start_lon,
            output_run_name=model_run_name,
            forcing_override=forcing_paths,
            simulation_start_utc=window["simulation_start_utc"],
            simulation_end_utc=window["simulation_end_utc"],
            snapshot_hours=snapshot_hours,
            date_composite_dates=list(EVENT_CORRIDOR_DATES),
            transport_overrides={
                "coastline_action": self.scenario.coastline_action,
                "coastline_approximation_precision": self.scenario.coastline_approximation_precision,
                "time_step_minutes": self.scenario.time_step_minutes,
            },
            sensitivity_context={
                "track": OFFICIAL_RERUN_R1_DIR_NAME,
                "selected_from": str(self.case_output / TRANSPORT_RETENTION_DIR_NAME / "transport_retention_run_manifest.json"),
                "scenario_id": self.scenario.scenario_id,
                "diagnostic_only": False,
            },
        )
        return {
            "status": forecast_result.get("status", "unknown"),
            "model_dir": str(get_case_output_dir(model_run_name)),
            "run_name": model_run_name,
            "retained_from_transport_retention_fix": False,
            "forecast_result": forecast_result,
        }

    def _transport_r1_model_dir(self, manifest: dict) -> Path:
        for row in manifest.get("summary", []):
            if str(row.get("scenario_id")) == "R1" and row.get("model_dir"):
                return Path(row["model_dir"])
        return self.case_output / TRANSPORT_RETENTION_DIR_NAME / "R1_previous" / "model_run"

    def _build_pairing_manifest(self, pairings: list[dict], model_result: dict) -> pd.DataFrame:
        rows = []
        for pair in pairings:
            rows.append(
                {
                    **{key: str(value) if isinstance(value, Path) else value for key, value in pair.items()},
                    "metric": "FSS",
                    "windows_km": "1,3,5,10",
                    "selected_retention_mode": self.scenario.scenario_id,
                    "coastline_action": self.scenario.coastline_action,
                    "retained_from_transport_retention_fix": model_result["retained_from_transport_retention_fix"],
                    "strict_main_pairing_unchanged": pair["pair_role"] == "strict_march6",
                    "medium_tier_not_run": True,
                }
            )
        return pd.DataFrame(rows)

    def _augment_probability_diagnostics(self, diagnostics_df: pd.DataFrame, model_dir: Path, composite_dir: Path) -> pd.DataFrame:
        rows = []
        for _, row in diagnostics_df.iterrows():
            record = row.to_dict()
            pair_role = str(record.get("pair_role", ""))
            obs_date = str(record.get("obs_date", ""))
            if pair_role == "strict_march6":
                probability_path = model_dir / "ensemble" / "prob_presence_2023-03-06_datecomposite.tif"
            elif pair_role == "short_extended_date_union":
                probability_path = composite_dir / f"prob_presence_{obs_date}_datecomposite.tif"
            else:
                probability_path = ""
            if probability_path and Path(probability_path).exists():
                arr = self.retention._read_raster(Path(probability_path))
                record["probability_path"] = str(probability_path)
                record["max_probability"] = float(np.nanmax(arr))
                record["probability_nonzero_cells"] = int(np.count_nonzero(arr > 0))
            else:
                record["probability_path"] = ""
                record["max_probability"] = np.nan
                record["probability_nonzero_cells"] = np.nan
            rows.append(record)
        return pd.DataFrame(rows)

    def _build_summary(self, diagnostics_df: pd.DataFrame, fss_df: pd.DataFrame, scenario_summary: dict) -> pd.DataFrame:
        rows = []
        for _, diag in diagnostics_df.iterrows():
            record = diag.to_dict()
            pair_fss = fss_df[fss_df["pair_id"] == record["pair_id"]]
            for window in (1, 3, 5, 10):
                values = pair_fss.loc[pair_fss["window_km"].astype(int) == window, "fss"]
                record[f"fss_{window}km"] = float(values.iloc[0]) if not values.empty else np.nan
            record.update(
                {
                    "selected_retention_mode": self.scenario.scenario_id,
                    "coastline_action": self.scenario.coastline_action,
                    "coastline_approximation_precision": self.scenario.coastline_approximation_precision,
                    "time_step_minutes": self.scenario.time_step_minutes,
                    "transport_model": scenario_summary.get("transport_model", "oceandrift"),
                    "provisional_transport_model": scenario_summary.get("provisional_transport_model", True),
                    "recipe_used": scenario_summary.get("recipe_used", ""),
                    "element_count_used": scenario_summary.get("element_count_used", ""),
                    "shoreline_mask_signature": scenario_summary.get("shoreline_mask_signature", ""),
                    "last_raw_active_time_utc": scenario_summary.get("last_raw_active_time_utc", ""),
                    "last_nonzero_deterministic_footprint_utc": scenario_summary.get("last_nonzero_deterministic_footprint_utc", ""),
                    "last_nonzero_prob_presence_utc": scenario_summary.get("last_nonzero_prob_presence_utc", ""),
                    "last_nonzero_mask_p50_utc": scenario_summary.get("last_nonzero_mask_p50_utc", ""),
                    "survives_to_strict_validation": _time_reaches(scenario_summary.get("last_raw_active_time_utc", ""), STRICT_VALIDATION_TIME_UTC),
                    "prob_presence_to_strict_validation": _time_reaches(
                        scenario_summary.get("last_nonzero_prob_presence_utc", ""), STRICT_VALIDATION_TIME_UTC
                    ),
                    "p50_signal_on_march6": _time_reaches(scenario_summary.get("last_nonzero_mask_p50_utc", ""), MARCH6_DATE_START_UTC),
                }
            )
            rows.append(record)
        return pd.DataFrame(rows)

    def _build_before_after(self, after_summary: pd.DataFrame) -> pd.DataFrame:
        before = self._load_before_rows()
        rows = []
        for _, after in after_summary.iterrows():
            key = self._comparison_key(after)
            before_row = before.get(key, {})
            row = {
                "comparison_key": key,
                "pair_role": after.get("pair_role", ""),
                "obs_date": after.get("obs_date", ""),
                "before_source": before_row.get("source", ""),
                "after_source": OFFICIAL_RERUN_R1_DIR_NAME,
                "before_forecast_nonzero_cells": before_row.get("forecast_nonzero_cells", np.nan),
                "after_forecast_nonzero_cells": after.get("forecast_nonzero_cells", np.nan),
                "before_obs_nonzero_cells": before_row.get("obs_nonzero_cells", np.nan),
                "after_obs_nonzero_cells": after.get("obs_nonzero_cells", np.nan),
                "before_centroid_distance_m": before_row.get("centroid_distance_m", np.nan),
                "after_centroid_distance_m": after.get("centroid_distance_m", np.nan),
                "before_iou": before_row.get("iou", np.nan),
                "after_iou": after.get("iou", np.nan),
                "before_dice": before_row.get("dice", np.nan),
                "after_dice": after.get("dice", np.nan),
            }
            for window in (1, 3, 5, 10):
                before_fss = _float_or_nan(before_row.get(f"fss_{window}km"))
                after_fss = _float_or_nan(after.get(f"fss_{window}km"))
                row[f"before_fss_{window}km"] = before_fss
                row[f"after_fss_{window}km"] = after_fss
                row[f"delta_fss_{window}km"] = after_fss - before_fss if np.isfinite(before_fss) and np.isfinite(after_fss) else np.nan
            rows.append(row)
        return pd.DataFrame(rows)

    def _load_before_rows(self) -> dict[str, dict]:
        before: dict[str, dict] = {}
        strict = _read_csv(self.case_output / "phase3b" / "phase3b_summary.csv")
        if not strict.empty:
            primary = strict[strict["pair_role"].astype(str) == "primary"]
            if not primary.empty:
                row = primary.iloc[0].to_dict()
                row["source"] = "phase3b strict March 6 pre-R1"
                before["strict_march6"] = row
        short = _read_csv(self.case_output / "phase3b_extended_public_scored_short" / "extended_short_summary.csv")
        if not short.empty:
            for _, row in short.iterrows():
                key = ""
                if str(row.get("pair_role")) == "extended_short_per_date_union":
                    key = f"short_extended_date_union:{row.get('obs_date')}"
                elif str(row.get("pair_role")) == "extended_short_eventcorridor":
                    key = "short_extended_eventcorridor"
                if key:
                    record = row.to_dict()
                    record["source"] = "phase3b_extended_public_scored_short pre-R1"
                    before[key] = record
        return before

    @staticmethod
    def _comparison_key(row: pd.Series | dict) -> str:
        pair_role = str(row.get("pair_role", ""))
        if pair_role == "strict_march6":
            return "strict_march6"
        if pair_role == "short_extended_date_union":
            return f"short_extended_date_union:{row.get('obs_date')}"
        if pair_role == "short_extended_eventcorridor":
            return "short_extended_eventcorridor"
        return str(row.get("pair_id", ""))

    def _write_outputs(
        self,
        pairing_df: pd.DataFrame,
        fss_df: pd.DataFrame,
        diagnostics_df: pd.DataFrame,
        hourly_df: pd.DataFrame,
        summary_df: pd.DataFrame,
        before_after_df: pd.DataFrame,
    ) -> dict[str, Path]:
        paths = {
            "summary": self.output_dir / "official_rerun_r1_summary.csv",
            "diagnostics": self.output_dir / "official_rerun_r1_diagnostics.csv",
            "hourly_timeseries": self.output_dir / "official_rerun_r1_hourly_timeseries.csv",
            "pairing_manifest": self.output_dir / "official_rerun_r1_pairing_manifest.csv",
            "fss_by_window": self.output_dir / "official_rerun_r1_fss_by_window.csv",
            "before_after": self.output_dir / "official_rerun_r1_before_after.csv",
        }
        _write_csv(paths["summary"], summary_df)
        _write_csv(paths["diagnostics"], diagnostics_df)
        _write_csv(paths["hourly_timeseries"], hourly_df)
        _write_csv(paths["pairing_manifest"], pairing_df)
        _write_csv(paths["fss_by_window"], fss_df)
        _write_csv(paths["before_after"], before_after_df)
        return paths

    def _write_manifest(
        self,
        transport_manifest: dict,
        model_result: dict,
        scenario_summary: dict,
        summary_df: pd.DataFrame,
        before_after_df: pd.DataFrame,
        recommendation: str,
        paths: dict[str, Path],
        qa_paths: dict[str, Path],
        report_path: Path,
    ) -> Path:
        manifest_path = self.output_dir / "official_rerun_r1_run_manifest.json"
        payload = {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "workflow_mode": self.case.workflow_mode,
            "run_name": self.case.run_name,
            "phase": OFFICIAL_RERUN_R1_DIR_NAME,
            "selected_scenario": self.scenario.scenario_id,
            "selected_retention_config": self.retention_config,
            "retained_from_transport_retention_fix": model_result["retained_from_transport_retention_fix"],
            "r3_diagnostic_only": True,
            "transport_retention_fix_manifest": str(self.case_output / TRANSPORT_RETENTION_DIR_NAME / "transport_retention_run_manifest.json"),
            "selection_evidence": transport_manifest.get("recommendation", {}),
            "model_result": model_result,
            "provenance": {
                "transport_model": scenario_summary.get("transport_model", "oceandrift"),
                "provisional_transport_model": scenario_summary.get("provisional_transport_model", True),
                "coastline_action": self.scenario.coastline_action,
                "recipe_used": scenario_summary.get("recipe_used", ""),
                "element_count_used": scenario_summary.get("element_count_used", ""),
                "shoreline_mask_signature": scenario_summary.get("shoreline_mask_signature", ""),
                "forcing_paths": transport_manifest.get("forcing_paths", {}),
            },
            "guardrails": {
                "strict_march6_pairing_unchanged": True,
                "within_horizon_public_semantics_unchanged": True,
                "short_extended_semantics_unchanged": True,
                "medium_tier_not_run": True,
                "diagnostic_r3_not_promoted": True,
            },
            "recommended_next_branch": recommendation,
            "summary": summary_df.to_dict(orient="records"),
            "before_after": before_after_df.to_dict(orient="records"),
            "artifacts": {**{key: str(value) for key, value in paths.items()}, **{key: str(value) for key, value in qa_paths.items()}, "report": str(report_path)},
        }
        _write_json(manifest_path, payload)
        return manifest_path

    def _write_report(
        self,
        summary_df: pd.DataFrame,
        before_after_df: pd.DataFrame,
        recommendation: str,
        paths: dict[str, Path],
        qa_paths: dict[str, Path],
        model_result: dict,
    ) -> Path:
        path = self.output_dir / "official_rerun_r1_report.md"
        strict = summary_df[summary_df["pair_role"] == "strict_march6"].iloc[0]
        event = summary_df[summary_df["pair_role"] == "short_extended_eventcorridor"].iloc[0]
        lines = [
            "# Official Rerun R1",
            "",
            "This pack promotes the selected non-diagnostic retention candidate from `transport_retention_fix`: `R1`, using `general:coastline_action=previous`.",
            "",
            "The strict March 6 target, within-horizon public semantics, and short-extended March 7-9 appendix semantics are unchanged.",
            "",
            f"- Selected scenario: `{self.scenario.scenario_id}`",
            f"- Coastline action: `{self.scenario.coastline_action}`",
            "- R3 status: diagnostic-only, not promoted",
            f"- Retained from transport_retention_fix: `{model_result['retained_from_transport_retention_fix']}`",
            f"- Recommended next branch: `{recommendation}`",
            "",
            "## Strict March 6",
            "",
            f"- FSS 1/3/5/10 km: {strict['fss_1km']}, {strict['fss_3km']}, {strict['fss_5km']}, {strict['fss_10km']}",
            f"- Forecast nonzero cells: {strict['forecast_nonzero_cells']}",
            f"- Observed nonzero cells: {strict['obs_nonzero_cells']}",
            f"- March 6 max probability: {strict['max_probability']}",
            f"- Last raw active time: {strict['last_raw_active_time_utc']}",
            f"- Last nonzero prob_presence: {strict['last_nonzero_prob_presence_utc']}",
            f"- Last nonzero p50: {strict['last_nonzero_mask_p50_utc']}",
            "",
            "## Short Extended Event Corridor",
            "",
            f"- FSS 1/3/5/10 km: {event['fss_1km']}, {event['fss_3km']}, {event['fss_5km']}, {event['fss_10km']}",
            f"- Forecast nonzero cells: {event['forecast_nonzero_cells']}",
            f"- Observed nonzero cells: {event['obs_nonzero_cells']}",
            "",
            "## Before/After",
            "",
        ]
        cols = ["comparison_key", "before_forecast_nonzero_cells", "after_forecast_nonzero_cells", "before_fss_10km", "after_fss_10km"]
        lines.extend(self._markdown_table(before_after_df[cols]))
        lines.extend(["", "Artifacts:"])
        for label, artifact in paths.items():
            lines.append(f"- {label}: {artifact}")
        for label, artifact in qa_paths.items():
            lines.append(f"- {label}: {artifact}")
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return path

    @staticmethod
    def _markdown_table(df: pd.DataFrame) -> list[str]:
        if df.empty:
            return ["No rows."]
        columns = list(df.columns)
        lines = ["| " + " | ".join(columns) + " |", "| " + " | ".join(["---"] * len(columns)) + " |"]
        for _, row in df.iterrows():
            lines.append("| " + " | ".join(str(row.get(column, "")) for column in columns) + " |")
        return lines

    def _write_qa(self, hourly_df: pd.DataFrame, diagnostics_df: pd.DataFrame, model_dir: Path) -> dict[str, Path]:
        outputs: dict[str, Path] = {}
        if plt is None:
            return outputs
        timeseries = self._plot_timeseries(hourly_df)
        overlays = self._plot_overlays(diagnostics_df)
        tracks = self._plot_tracks(model_dir)
        if timeseries:
            outputs["qa_official_rerun_r1_timeseries"] = timeseries
        if overlays:
            outputs["qa_official_rerun_r1_overlays"] = overlays
        if tracks:
            outputs["qa_official_rerun_r1_tracks"] = tracks
        return outputs

    def _plot_timeseries(self, hourly_df: pd.DataFrame) -> Path | None:
        if hourly_df.empty:
            return None
        path = self.output_dir / "qa_official_rerun_r1_timeseries.png"
        aggregate = hourly_df[hourly_df["run_kind"] == "ensemble_aggregate"].copy()
        deterministic = hourly_df[hourly_df["run_kind"] == "deterministic_control"].copy()
        for frame in (aggregate, deterministic):
            frame["timestamp"] = pd.to_datetime(frame["timestamp_utc"], errors="coerce", utc=True).dt.tz_convert(None)
            for column in ["max_prob_presence", "p50_nonzero_cells", "surface_presence_cells"]:
                if column in frame:
                    frame[column] = pd.to_numeric(frame[column], errors="coerce")
        fig, axes = plt.subplots(3, 1, figsize=(12, 10), sharex=True)
        axes[0].plot(aggregate["timestamp"], aggregate["max_prob_presence"], color="#1f77b4")
        axes[1].plot(aggregate["timestamp"], aggregate["p50_nonzero_cells"], color="#d62728")
        axes[2].plot(deterministic["timestamp"], deterministic["surface_presence_cells"], color="#2ca02c")
        axes[0].set_ylabel("max prob_presence")
        axes[1].set_ylabel("p50 cells")
        axes[2].set_ylabel("det footprint cells")
        axes[2].set_xlabel("UTC time")
        for ax in axes:
            ax.grid(alpha=0.2)
        fig.autofmt_xdate()
        fig.tight_layout()
        fig.savefig(path, dpi=150, bbox_inches="tight")
        plt.close(fig)
        return path

    def _plot_overlays(self, diagnostics_df: pd.DataFrame) -> Path | None:
        selected = diagnostics_df[diagnostics_df["pair_role"].isin(["strict_march6", "short_extended_eventcorridor"])].copy()
        if selected.empty:
            return None
        path = self.output_dir / "qa_official_rerun_r1_overlays.png"
        fig, axes = plt.subplots(1, len(selected), figsize=(5 * len(selected), 5))
        if len(selected) == 1:
            axes = [axes]
        for ax, (_, row) in zip(axes, selected.iterrows()):
            forecast = self.retention.scoring_helper._load_binary_score_mask(Path(row["forecast_path"]))
            obs = self.retention.scoring_helper._load_binary_score_mask(Path(row["observation_path"]))
            canvas = np.ones((forecast.shape[0], forecast.shape[1], 3), dtype=np.float32)
            canvas[obs > 0] = [0.1, 0.35, 0.95]
            canvas[forecast > 0] = [0.95, 0.35, 0.1]
            canvas[(obs > 0) & (forecast > 0)] = [0.1, 0.65, 0.25]
            ax.imshow(canvas)
            ax.set_title(str(row["pair_role"]))
            ax.axis("off")
        fig.tight_layout()
        fig.savefig(path, dpi=150, bbox_inches="tight")
        plt.close(fig)
        return path

    def _plot_tracks(self, model_dir: Path) -> Path | None:
        member = next(iter(sorted((model_dir / "ensemble").glob("member_*.nc"))), None)
        if member is None:
            return None
        path = self.output_dir / "qa_official_rerun_r1_tracks.png"
        fig, ax = plt.subplots(figsize=(8, 8))
        ax.imshow(
            self.retention.valid_ocean,
            cmap="Blues",
            alpha=0.25,
            extent=[self.retention.grid.min_x, self.retention.grid.max_x, self.retention.grid.min_y, self.retention.grid.max_y],
            origin="upper",
        )
        with xr.open_dataset(member) as ds:
            lon = np.asarray(ds["lon"].values)
            lat = np.asarray(ds["lat"].values)
            trajectories = np.linspace(0, lon.shape[0] - 1, min(25, lon.shape[0]), dtype=int)
            for idx in trajectories:
                finite = np.isfinite(lon[idx]) & np.isfinite(lat[idx])
                if np.any(finite):
                    x_vals, y_vals = project_points_to_grid(self.retention.grid, lon[idx][finite], lat[idx][finite])
                    ax.plot(x_vals, y_vals, linewidth=0.6, alpha=0.25)
        ax.set_title("R1 sample ensemble tracks")
        ax.set_xlim(self.retention.grid.min_x, self.retention.grid.max_x)
        ax.set_ylim(self.retention.grid.min_y, self.retention.grid.max_y)
        ax.set_aspect("equal")
        fig.tight_layout()
        fig.savefig(path, dpi=150, bbox_inches="tight")
        plt.close(fig)
        return path


def run_official_rerun_r1() -> dict:
    return OfficialRerunR1Service().run()


if __name__ == "__main__":  # pragma: no cover
    print(json.dumps(run_official_rerun_r1(), indent=2, default=_json_default))
