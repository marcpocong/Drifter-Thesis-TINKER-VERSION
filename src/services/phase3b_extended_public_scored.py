"""Appendix-only scored short extended public-observation validation for Mindoro."""

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
from src.helpers.raster import GridBuilder, rasterize_particles, save_raster
from src.helpers.scoring import apply_ocean_mask, load_sea_mask_array, precheck_same_grid
from src.services.ensemble import normalize_time_index, run_official_spill_forecast
from src.services.ingestion import DataIngestionService
from src.services.phase3b_multidate_public import _hash_file
from src.services.scoring import OFFICIAL_PHASE3B_WINDOWS_KM, Phase3BScoringService
from src.utils.forcing_outage_policy import (
    FORCING_OUTAGE_POLICY_CONTINUE_DEGRADED,
    resolve_forcing_outage_policy,
    source_id_for_recipe_component,
)
from src.utils.io import get_case_output_dir, resolve_recipe_selection, resolve_spill_origin

try:
    import matplotlib.pyplot as plt
except ImportError:  # pragma: no cover
    plt = None

try:
    import xarray as xr
except ImportError:  # pragma: no cover
    xr = None


EXTENDED_SCORED_DIR_NAME = "phase3b_extended_public_scored_short"
SHORT_DATES = ["2023-03-07", "2023-03-08", "2023-03-09"]
EVENT_CORRIDOR_DATES = [
    "2023-03-04",
    "2023-03-05",
    "2023-03-06",
    "2023-03-07",
    "2023-03-08",
    "2023-03-09",
]
MEDIUM_DATES = ["2023-03-11", "2023-03-12", "2023-03-13", "2023-03-14", "2023-03-15", "2023-03-18", "2023-03-19"]
LONG_TAIL_DATES = ["2023-03-28", "2023-03-30", "2023-03-31"]
DEFAULT_SHORT_END_LOCAL = "2023-03-09 23:59 Asia/Manila"
LOCAL_TIMEZONE = "Asia/Manila"
MAX_OFFICIAL_START_OFFSET_HOURS = 3


@dataclass(frozen=True)
class ExtendedTierWindow:
    tier: str
    validation_dates: list[str]
    simulation_start_utc: str
    simulation_end_utc: str
    required_forcing_start_utc: str
    required_forcing_end_utc: str
    download_start_date: str
    download_end_date: str
    end_selection_source: str
    date_composite_rule: str


def _write_json(path: Path, payload: dict | list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, default=_json_default)
        handle.write("\n")


def _json_default(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, np.ndarray):
        return value.tolist()
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return None if np.isnan(value) else float(value)
    return str(value)


def _normalize_utc(value) -> pd.Timestamp:
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        return timestamp
    return timestamp.tz_convert("UTC").tz_localize(None)


def _iso_z(value) -> str:
    return _normalize_utc(value).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_local_end(value: str) -> pd.Timestamp:
    text = str(value or "").strip() or DEFAULT_SHORT_END_LOCAL
    if text.endswith(LOCAL_TIMEZONE):
        base = text[: -len(LOCAL_TIMEZONE)].strip()
        return pd.Timestamp(base, tz=LOCAL_TIMEZONE).tz_convert("UTC").tz_localize(None)
    timestamp = pd.Timestamp(text)
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize(LOCAL_TIMEZONE)
    return timestamp.tz_convert("UTC").tz_localize(None)


def resolve_short_extended_window() -> ExtendedTierWindow:
    tier = os.environ.get("EXTENDED_PUBLIC_TIER", "short").strip().lower() or "short"
    if tier != "short":
        raise RuntimeError("This patch implements only EXTENDED_PUBLIC_TIER=short.")
    case = get_case_context()
    simulation_start = _normalize_utc(case.simulation_start_utc)
    end_env = os.environ.get("EXTENDED_PUBLIC_END_LOCAL", DEFAULT_SHORT_END_LOCAL)
    simulation_end = _parse_local_end(end_env)
    required_end = simulation_end + pd.Timedelta(hours=MAX_OFFICIAL_START_OFFSET_HOURS)
    download_end = (required_end + pd.Timedelta(days=1)).date().isoformat()
    return ExtendedTierWindow(
        tier="short",
        validation_dates=list(SHORT_DATES),
        simulation_start_utc=_iso_z(simulation_start),
        simulation_end_utc=_iso_z(simulation_end),
        required_forcing_start_utc=_iso_z(simulation_start - pd.Timedelta(hours=MAX_OFFICIAL_START_OFFSET_HOURS)),
        required_forcing_end_utc=_iso_z(required_end),
        download_start_date=simulation_start.date().isoformat(),
        download_end_date=download_end,
        end_selection_source="EXTENDED_PUBLIC_END_LOCAL" if "EXTENDED_PUBLIC_END_LOCAL" in os.environ else "default_short_end_local",
        date_composite_rule=(
            "For each published observation date, forecast member presence is unioned across model timesteps whose UTC "
            "timestamp converts to that calendar date in Asia/Manila; p50 is probability >= 0.50 across members."
        ),
    )


def _forcing_time_and_vars(path: Path, required_vars: list[str], required_start: pd.Timestamp, required_end: pd.Timestamp) -> dict:
    row = {
        "path": str(path),
        "exists": path.exists(),
        "time_start_utc": "",
        "time_end_utc": "",
        "required_start_utc": _iso_z(required_start),
        "required_end_utc": _iso_z(required_end),
        "available_variables": "",
        "required_variables": ";".join(required_vars),
        "missing_required_variables": ";".join(required_vars),
        "covers_required_window": False,
        "status": "missing",
        "stop_reason": "",
    }
    if not path.exists():
        row["stop_reason"] = f"missing forcing file: {path}"
        return row
    if xr is None:
        row["status"] = "failed"
        row["stop_reason"] = "xarray is required to inspect forcing coverage"
        return row
    try:
        with xr.open_dataset(path) as ds:
            variables = sorted(str(name) for name in ds.data_vars)
            row["available_variables"] = ";".join(variables)
            missing = [name for name in required_vars if name not in variables]
            row["missing_required_variables"] = ";".join(missing)
            time_name = next((name for name in ("time", "valid_time") if name in ds.coords or name in ds.variables), None)
            if time_name:
                times = normalize_time_index(ds[time_name].values)
                if len(times):
                    row["time_start_utc"] = _iso_z(times.min())
                    row["time_end_utc"] = _iso_z(times.max())
                    row["covers_required_window"] = bool(times.min() <= required_start and times.max() >= required_end)
            row["status"] = "ready" if row["covers_required_window"] and not missing else "insufficient"
            reasons = []
            if missing:
                reasons.append(f"missing variables: {','.join(missing)}")
            if not row["covers_required_window"]:
                reasons.append("time coverage does not cover required extended window")
            row["stop_reason"] = "; ".join(reasons)
    except Exception as exc:
        row["status"] = "failed"
        row["stop_reason"] = str(exc)
    return row


class Phase3BExtendedPublicScoredService:
    def __init__(self):
        self.case = get_case_context()
        if not self.case.is_official:
            raise RuntimeError("phase3b_extended_public_scored is only supported for official Mindoro workflows.")
        self.phase_id = "phase3b_extended_public_scored"
        self.forcing_outage_policy = resolve_forcing_outage_policy(
            workflow_mode=self.case.workflow_mode,
            phase=self.phase_id,
        )
        self.case_output_dir = get_case_output_dir(self.case.run_name)
        self.source_extended_dir = self.case_output_dir / "phase3b_extended_public"
        self.appendix_dir = self.case_output_dir / "public_obs_appendix"
        self.multidate_dir = self.case_output_dir / "phase3b_multidate_public"
        self.output_dir = self.case_output_dir / EXTENDED_SCORED_DIR_NAME
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.forecast_composite_dir = self.output_dir / "forecast_datecomposites"
        self.obs_union_dir = self.output_dir / "date_union_obs_masks"
        self.precheck_dir = self.output_dir / "precheck"
        self.forcing_dir = self.output_dir / "forcing"
        for path in (self.forecast_composite_dir, self.obs_union_dir, self.precheck_dir, self.forcing_dir):
            path.mkdir(parents=True, exist_ok=True)
        self.grid = GridBuilder()
        self.sea_mask = load_sea_mask_array(self.grid.spec)
        self.valid_mask = self.sea_mask > 0.5 if self.sea_mask is not None else None
        self.helper = Phase3BScoringService(output_dir=self.output_dir / "_scratch_helper")
        self.window = resolve_short_extended_window()
        self.model_run_name = f"{self.case.run_name}/{EXTENDED_SCORED_DIR_NAME}/model_run"
        self.model_output_dir = get_case_output_dir(self.model_run_name)

    def run(self) -> dict:
        strict_before = self._strict_hashes()
        multidate_before = self._multidate_hashes()
        accepted = self._load_short_accepted_observations()
        if accepted.empty:
            raise RuntimeError("No accepted short-tier extended quantitative observations were found. Run phase3b_extended_public first.")

        selection, recipe_source = self._resolve_recipe()
        forcing_paths = self._prepare_extended_forcing(selection.recipe)
        forcing_manifest = self._write_forcing_window_manifest(selection.recipe, forcing_paths)
        failed_forcing = [row for row in forcing_manifest["rows"] if row["status"] != "ready"]
        if failed_forcing:
            self._write_failed_loading_audit(failed_forcing)
            raise RuntimeError(
                "Short-tier extended forcing coverage is incomplete. "
                f"See {self.output_dir / 'extended_forcing_window_manifest.json'}."
            )

        forecast_result = self._run_extended_forecast(selection, recipe_source, forcing_paths)
        self._copy_loading_audit()
        forecast_datecomposites = self._build_forecast_datecomposites()
        source_pairs = self._build_source_pairs(accepted, forecast_datecomposites)
        union_pairs = self._build_date_union_pairs(accepted, forecast_datecomposites)
        event_pair = self._build_eventcorridor_pair(accepted, forecast_datecomposites)
        scored_pairings, fss_df, diagnostics_df = self._score_pairings(pd.DataFrame(source_pairs + union_pairs + [event_pair]))
        summary_df = self._summarize(scored_pairings, fss_df, diagnostics_df)
        event_summary = summary_df[summary_df["score_group"] == "extended_eventcorridor"].copy()
        qa_paths = self._write_qa(scored_pairings)
        decision_note = self._write_decision_note(summary_df, event_summary)

        paths = self._write_outputs(scored_pairings, fss_df, diagnostics_df, summary_df, event_summary)

        strict_after = self._strict_hashes()
        multidate_after = self._multidate_hashes()
        if strict_before != strict_after or multidate_before != multidate_after:
            raise RuntimeError("Short extended appendix scoring modified locked Phase 3B outputs.")

        run_manifest_path = self.output_dir / "extended_short_run_manifest.json"
        _write_json(
            run_manifest_path,
            {
                "generated_at_utc": datetime.now(timezone.utc).isoformat(),
                "workflow_mode": self.case.workflow_mode,
                "run_name": self.case.run_name,
                "track": "appendix_only_short_extended_public_validation",
                "tier_window": asdict(self.window),
                "accepted_short_dates_scored": sorted(accepted["obs_date"].astype(str).unique().tolist()),
                "medium_tier_dates_not_scored": list(MEDIUM_DATES),
                "long_tail_residual_dates_not_scored": list(LONG_TAIL_DATES),
                "recipe": {
                    "recipe": selection.recipe,
                    "source": recipe_source,
                    "selection_source_kind": selection.source_kind,
                    "selection_source_path": selection.source_path,
                    "selection_status_flag": selection.status_flag,
                },
                "forecast_result": forecast_result,
                "model_run_name": self.model_run_name,
                "model_output_dir": str(self.model_output_dir),
                "forcing_manifest": forcing_manifest,
                "strict_march6_files_unchanged": True,
                "within_horizon_multidate_files_unchanged": True,
                "strict_hashes_before": strict_before,
                "strict_hashes_after": strict_after,
                "multidate_hashes_before": multidate_before,
                "multidate_hashes_after": multidate_after,
                "artifacts": {
                    **{key: str(value) for key, value in paths.items()},
                    "decision_note": str(decision_note),
                    **{key: str(value) for key, value in qa_paths.items()},
                },
                "score_summary": summary_df.to_dict(orient="records"),
            },
        )
        return {
            "output_dir": self.output_dir,
            "accepted_short_dates_scored": sorted(accepted["obs_date"].astype(str).unique().tolist()),
            "forcing_manifest_json": self.output_dir / "extended_forcing_window_manifest.json",
            "loading_audit_json": self.output_dir / "extended_loading_audit.json",
            "summary_csv": paths["summary"],
            "fss_csv": paths["fss_by_date_window"],
            "diagnostics_csv": paths["diagnostics"],
            "eventcorridor_summary_md": paths["eventcorridor_summary_md"],
            "run_manifest": run_manifest_path,
            "forcing_extension_worked": True,
            "forcing_extension_clean": True,
            "medium_tier_recommended_next": self._medium_tier_recommendation(summary_df),
            "medium_tier_recommendation": self._medium_tier_recommendation(summary_df),
        }

    def _load_short_accepted_observations(self) -> pd.DataFrame:
        registry_path = self.source_extended_dir / "extended_public_obs_acceptance_registry.csv"
        if not registry_path.exists():
            raise FileNotFoundError(f"Extended observation registry not found: {registry_path}")
        registry = pd.read_csv(registry_path)
        accepted = registry[
            (registry["accepted_for_extended_quantitative"].astype(bool))
            & (registry["obs_date"].astype(str).isin(SHORT_DATES))
            & (registry["mask_exists"].astype(bool))
        ].copy()
        accepted = accepted[accepted["extended_obs_mask"].astype(str).map(lambda value: Path(value).exists())].copy()
        return accepted.sort_values(["obs_date", "source_name"])

    @staticmethod
    def _resolve_recipe():
        selection = resolve_recipe_selection()
        return selection, "frozen_resolved_baseline_no_completed_event_recipe_sensitivity_found"

    def _prepare_extended_forcing(self, recipe_name: str) -> dict:
        with open("config/recipes.yaml", "r", encoding="utf-8") as handle:
            recipes = yaml.safe_load(handle) or {}
        recipe = (recipes.get("recipes") or {}).get(recipe_name)
        if not recipe:
            raise RuntimeError(f"Recipe {recipe_name} is not defined in config/recipes.yaml.")

        service = DataIngestionService()
        service.forcing_dir = self.forcing_dir
        service.forcing_dir.mkdir(parents=True, exist_ok=True)
        service.configure_explicit_download_window(
            start_date=self.window.download_start_date,
            end_date=self.window.download_end_date,
        )

        downloads: dict[str, Any] = {}
        current_file = str(recipe["currents_file"])
        wind_file = str(recipe["wind_file"])
        wave_file = str(recipe.get("wave_file") or "")

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
                    skipped_branch_ids=["appendix_only_short_extended_public_validation"],
                    manifest_path=str(manifest_path),
                    budget_seconds=downloads["currents"].get("budget_seconds"),
                    elapsed_seconds=downloads["currents"].get("elapsed_seconds"),
                    budget_exhausted=bool(downloads["currents"].get("budget_exhausted", False)),
                    failure_stage=str(downloads["currents"].get("failure_stage") or ""),
                )
            self._write_download_failure_manifest(recipe_name, downloads)
            raise RuntimeError(f"Extended forcing download failed: {downloads['currents']}")

        if wind_file.startswith("gfs"):
            gfs_path = self.forcing_dir / wind_file
            if not gfs_path.exists():
                raise FileNotFoundError(
                    f"GFS wind forcing was requested for the extended public-observation branch but is not available locally: {gfs_path}. "
                    "The official Phase 1 recipe family is still only partially available in the current repo state."
                )
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
                        skipped_branch_ids=["appendix_only_short_extended_public_validation"],
                        manifest_path=str(manifest_path),
                        budget_seconds=downloads["wind"].get("budget_seconds"),
                        elapsed_seconds=downloads["wind"].get("elapsed_seconds"),
                        budget_exhausted=bool(downloads["wind"].get("budget_exhausted", False)),
                        failure_stage=str(downloads["wind"].get("failure_stage") or ""),
                    )
                self._write_download_failure_manifest(recipe_name, downloads)
                raise RuntimeError(f"Extended forcing download failed: {downloads['wind']}")
        if wind_file.startswith("gfs"):
            downloads["wind"] = {
                "status": "reused_local_file",
                "path": str(self.forcing_dir / wind_file),
                "source_id": "gfs",
                "forcing_factor": wind_file,
                "upstream_outage_detected": False,
            }

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
                        skipped_branch_ids=["appendix_only_short_extended_public_validation"],
                        manifest_path=str(manifest_path),
                        budget_seconds=downloads["wave"].get("budget_seconds"),
                        elapsed_seconds=downloads["wave"].get("elapsed_seconds"),
                        budget_exhausted=bool(downloads["wave"].get("budget_exhausted", False)),
                        failure_stage=str(downloads["wave"].get("failure_stage") or ""),
                    )
                self._write_download_failure_manifest(recipe_name, downloads)
                raise RuntimeError(f"Extended forcing download failed: {downloads['wave']}")

        return {
            "recipe": recipe_name,
            "currents": self.forcing_dir / current_file,
            "wind": self.forcing_dir / wind_file,
            "wave": self.forcing_dir / wave_file if wave_file else None,
            "downloads": downloads,
            "duration_hours": recipe.get("duration_hours", 72),
            "time_step_minutes": recipe.get("time_step_minutes", 60),
        }

    def _write_download_failure_manifest(
        self,
        recipe_name: str,
        downloads: dict,
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
            "tier_window": asdict(self.window),
            "downloads": downloads,
            "status": status,
            "forcing_outage_policy": self.forcing_outage_policy,
            "degraded_continue_used": degraded_continue_used,
            "upstream_outage_detected": upstream_outage_detected,
            "missing_forcing_factors": list(missing_forcing_factors or []),
            "rerun_required": bool(degraded_continue_used),
            "stop_reason": stop_reason,
        }
        manifest_json = self.output_dir / "extended_forcing_window_manifest.json"
        manifest_csv = self.output_dir / "extended_forcing_window_manifest.csv"
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
            manifest_csv,
            index=False,
        )
        return manifest_json

    def _write_forcing_window_manifest(self, recipe_name: str, forcing_paths: dict) -> dict:
        required_start = _normalize_utc(self.window.required_forcing_start_utc)
        required_end = _normalize_utc(self.window.required_forcing_end_utc)
        rows = [
            {
                "forcing_kind": "current",
                **_forcing_time_and_vars(Path(forcing_paths["currents"]), ["uo", "vo"], required_start, required_end),
            },
            {
                "forcing_kind": "wind",
                **_forcing_time_and_vars(Path(forcing_paths["wind"]), ["x_wind", "y_wind"], required_start, required_end),
            },
        ]
        if forcing_paths.get("wave"):
            rows.append(
                {
                    "forcing_kind": "wave",
                    **_forcing_time_and_vars(Path(forcing_paths["wave"]), ["VHM0", "VSDX", "VSDY"], required_start, required_end),
                }
            )
        payload = {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "recipe": recipe_name,
            "tier_window": asdict(self.window),
            "rows": rows,
            "forcing_outage_policy": self.forcing_outage_policy,
            "degraded_continue_used": False,
            "missing_forcing_factors": [],
            "rerun_required": False,
            "status": "ready" if all(row["status"] == "ready" for row in rows) else "insufficient",
        }
        _write_json(self.output_dir / "extended_forcing_window_manifest.json", payload)
        pd.DataFrame(rows).to_csv(self.output_dir / "extended_forcing_window_manifest.csv", index=False)
        return payload

    def _run_extended_forecast(self, selection, recipe_source: str, forcing_paths: dict) -> dict:
        start_lat, start_lon, start_time = resolve_spill_origin()
        simulation_start = _normalize_utc(self.window.simulation_start_utc)
        simulation_end = _normalize_utc(self.window.simulation_end_utc)
        duration_hours = int(math.ceil((simulation_end - simulation_start).total_seconds() / 3600.0))
        snapshot_hours = sorted(set([24, 48, 72, 96, 120, 144, duration_hours]))
        if os.environ.get("EXTENDED_PUBLIC_FORCE_RERUN", "").strip().lower() not in {"1", "true", "yes"}:
            member_paths = sorted((self.model_output_dir / "ensemble").glob("member_*.nc"))
            ensemble_manifest = self.model_output_dir / "ensemble" / "ensemble_manifest.json"
            forecast_manifest = self.model_output_dir / "forecast" / "forecast_manifest.json"
            if member_paths and ensemble_manifest.exists() and forecast_manifest.exists():
                return {
                    "status": "success",
                    "reused_existing_model_run": True,
                    "member_count": len(member_paths),
                    "ensemble_manifest": str(ensemble_manifest),
                    "forecast_manifest": str(forecast_manifest),
                    "simulation_start_utc": self.window.simulation_start_utc,
                    "simulation_end_utc": self.window.simulation_end_utc,
                    "date_composite_dates": list(EVENT_CORRIDOR_DATES),
                    "reuse_note": (
                        "Reused completed short-extended model artifacts because EXTENDED_PUBLIC_FORCE_RERUN "
                        "was not set; scoring products are regenerated from member NetCDFs."
                    ),
                }
        context = {
            "track": "appendix_only_short_extended_public_validation",
            "extended_public_tier": self.window.tier,
            "date_composite_rule": self.window.date_composite_rule,
            "recipe_source": recipe_source,
            "medium_tier_not_scored_in_this_patch": MEDIUM_DATES,
            "long_tail_not_scored_in_this_patch": LONG_TAIL_DATES,
        }
        result = run_official_spill_forecast(
            selection=selection,
            start_time=start_time,
            start_lat=start_lat,
            start_lon=start_lon,
            output_run_name=self.model_run_name,
            forcing_override=forcing_paths,
            sensitivity_context=context,
            historical_baseline_provenance={
                "recipe": selection.recipe,
                "source_kind": selection.source_kind,
                "source_path": selection.source_path,
                "note": selection.note,
            },
            simulation_start_utc=self.window.simulation_start_utc,
            simulation_end_utc=self.window.simulation_end_utc,
            snapshot_hours=snapshot_hours,
            date_composite_dates=list(EVENT_CORRIDOR_DATES),
        )
        if result.get("status") != "success":
            self._write_failed_loading_audit([{"stop_reason": str(result)}])
            raise RuntimeError(f"Extended short forecast failed: {result}")
        return result

    def _copy_loading_audit(self) -> None:
        source_json = self.model_output_dir / "forecast" / "phase2_loading_audit.json"
        source_csv = self.model_output_dir / "forecast" / "phase2_loading_audit.csv"
        dest_json = self.output_dir / "extended_loading_audit.json"
        dest_csv = self.output_dir / "extended_loading_audit.csv"
        if source_json.exists():
            shutil.copyfile(source_json, dest_json)
        else:
            _write_json(dest_json, {"runs": [], "status": "missing_model_loading_audit"})
        if source_csv.exists():
            shutil.copyfile(source_csv, dest_csv)
        else:
            pd.DataFrame([{"status": "missing_model_loading_audit"}]).to_csv(dest_csv, index=False)

    def _write_failed_loading_audit(self, failures: list[dict]) -> None:
        payload = {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "status": "failed_before_or_during_extended_forecast",
            "tier_window": asdict(self.window),
            "failures": failures,
        }
        _write_json(self.output_dir / "extended_loading_audit.json", payload)
        pd.DataFrame(failures).to_csv(self.output_dir / "extended_loading_audit.csv", index=False)

    def _build_forecast_datecomposites(self) -> dict[str, Path]:
        member_dir = self.model_output_dir / "ensemble"
        member_paths = sorted(member_dir.glob("member_*.nc"))
        if not member_paths:
            raise FileNotFoundError(f"No extended ensemble member files found in {member_dir}")
        outputs: dict[str, Path] = {}
        for date in EVENT_CORRIDOR_DATES:
            probability = self._date_composite_probability(member_paths, date)
            probability = apply_ocean_mask(probability, sea_mask=self.sea_mask, fill_value=0.0)
            p50 = apply_ocean_mask((probability >= 0.5).astype(np.float32), sea_mask=self.sea_mask, fill_value=0.0)
            prob_path = self.forecast_composite_dir / f"prob_presence_{date}_datecomposite.tif"
            p50_path = self.forecast_composite_dir / f"mask_p50_{date}_datecomposite.tif"
            save_raster(self.grid, probability.astype(np.float32), prob_path)
            save_raster(self.grid, p50.astype(np.float32), p50_path)
            outputs[date] = p50_path
        return outputs

    def _date_composite_probability(self, member_paths: list[Path], local_date: str) -> np.ndarray:
        member_masks = [self._member_local_date_mask(member_path, local_date) for member_path in member_paths]
        return np.mean(np.stack(member_masks, axis=0), axis=0).astype(np.float32)

    def _member_local_date_mask(self, nc_path: Path, local_date: str) -> np.ndarray:
        if xr is None:
            raise ImportError("xarray is required to build extended date composites.")
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
                valid = ~np.isnan(lon) & ~np.isnan(lat) & (status == 0)
                if not np.any(valid):
                    continue
                hits, _ = rasterize_particles(
                    self.grid,
                    lon[valid],
                    lat[valid],
                    np.ones(np.count_nonzero(valid), dtype=np.float32),
                )
                composite = np.maximum(composite, hits)
        return apply_ocean_mask(composite.astype(np.float32), sea_mask=self.sea_mask, fill_value=0.0)

    def _build_source_pairs(self, accepted: pd.DataFrame, forecast_datecomposites: dict[str, Path]) -> list[dict]:
        pairs = []
        for _, row in accepted.iterrows():
            obs_date = str(row["obs_date"])
            obs_path = Path(str(row["extended_obs_mask"]))
            pairs.append(
                self._pair_record(
                    score_group="per_source",
                    pair_role="extended_short_per_source",
                    pair_id=f"extended_short_source_{row['source_key']}",
                    obs_date=obs_date,
                    source_key=str(row["source_key"]),
                    source_name=str(row["source_name"]),
                    provider=str(row.get("provider", "")),
                    forecast_path=forecast_datecomposites[obs_date],
                    observation_path=obs_path,
                    source_count=1,
                    source_semantics=f"extended_short_per_source_{obs_date}_public_obs_vs_local_date_p50",
                )
            )
        return pairs

    def _build_date_union_pairs(self, accepted: pd.DataFrame, forecast_datecomposites: dict[str, Path]) -> list[dict]:
        pairs = []
        for date, group in accepted.groupby("obs_date"):
            date = str(date)
            union = np.zeros((self.grid.height, self.grid.width), dtype=np.float32)
            for _, row in group.iterrows():
                union = np.maximum(union, self.helper._load_binary_score_mask(Path(str(row["extended_obs_mask"]))))
            union = apply_ocean_mask(union, sea_mask=self.sea_mask, fill_value=0.0)
            union_path = self.obs_union_dir / f"extended_obs_union_{date}.tif"
            save_raster(self.grid, union.astype(np.float32), union_path)
            pairs.append(
                self._pair_record(
                    score_group="per_date_union",
                    pair_role="extended_short_per_date_union",
                    pair_id=f"extended_short_date_union_{date}",
                    obs_date=date,
                    source_key=";".join(group["source_key"].astype(str).tolist()),
                    source_name=f"extended_short_obs_union_{date}",
                    provider=";".join(sorted(set(group["provider"].astype(str).tolist()))),
                    forecast_path=forecast_datecomposites[date],
                    observation_path=union_path,
                    source_count=int(len(group)),
                    source_semantics=f"extended_short_per_date_union_{date}_public_obs_vs_local_date_p50",
                )
            )
        return pairs

    def _build_eventcorridor_pair(self, accepted: pd.DataFrame, forecast_datecomposites: dict[str, Path]) -> dict:
        obs_union = np.zeros((self.grid.height, self.grid.width), dtype=np.float32)
        model_union = np.zeros((self.grid.height, self.grid.width), dtype=np.float32)
        obs_rows = self._load_within_horizon_event_obs()
        all_obs = pd.concat([obs_rows, accepted], ignore_index=True)
        for _, row in all_obs.iterrows():
            mask_path = self._resolve_obs_mask_path(row)
            obs_union = np.maximum(obs_union, self.helper._load_binary_score_mask(Path(str(mask_path))))
        for date in EVENT_CORRIDOR_DATES:
            model_union = np.maximum(model_union, self.helper._load_binary_score_mask(forecast_datecomposites[date]))
        obs_union = apply_ocean_mask(obs_union, sea_mask=self.sea_mask, fill_value=0.0)
        model_union = apply_ocean_mask(model_union, sea_mask=self.sea_mask, fill_value=0.0)
        obs_path = self.output_dir / "extended_eventcorridor_obs_union_2023-03-04_to_2023-03-09.tif"
        model_path = self.output_dir / "extended_eventcorridor_model_union_2023-03-04_to_2023-03-09.tif"
        save_raster(self.grid, obs_union.astype(np.float32), obs_path)
        save_raster(self.grid, model_union.astype(np.float32), model_path)
        dates_used = sorted(set(all_obs["obs_date"].astype(str).tolist()))
        return self._pair_record(
            score_group="extended_eventcorridor",
            pair_role="extended_short_eventcorridor",
            pair_id="extended_eventcorridor_2023-03-04_to_2023-03-09",
            obs_date="2023-03-04_to_2023-03-09",
            source_key=";".join(all_obs["source_key"].astype(str).tolist()),
            source_name="extended_eventcorridor_obs_union_2023-03-04_to_2023-03-09",
            provider="mixed_public_observation_sources",
            forecast_path=model_path,
            observation_path=obs_path,
            source_count=int(len(all_obs)),
            source_semantics="extended_eventcorridor_public_obs_union_excluding_initialization_date",
            validation_dates_used=",".join(dates_used),
        )

    @staticmethod
    def _resolve_obs_mask_path(row: pd.Series) -> Path:
        for column in ("appendix_obs_mask", "extended_obs_mask"):
            value = row.get(column, "")
            if pd.isna(value):
                continue
            text = str(value).strip()
            if not text or text.lower() in {"nan", "none", "null"}:
                continue
            path = Path(text)
            if path.exists():
                return path
        raise FileNotFoundError(f"No existing observation mask path found for row: {row.to_dict()}")

    def _load_within_horizon_event_obs(self) -> pd.DataFrame:
        inventory_path = self.appendix_dir / "public_obs_inventory.csv"
        if not inventory_path.exists():
            return pd.DataFrame()
        inventory = pd.read_csv(inventory_path)
        if "appendix_obs_mask" not in inventory.columns:
            return pd.DataFrame()
        mask_exists = inventory["appendix_obs_mask"].map(
            lambda value: False if pd.isna(value) else Path(str(value).strip()).exists()
        )
        accepted = inventory[
            inventory["accept_for_appendix_quantitative"].astype(bool)
            & inventory["obs_date"].astype(str).isin(["2023-03-04", "2023-03-05", "2023-03-06"])
            & mask_exists
        ].copy()
        return accepted.sort_values(["obs_date", "source_name"])

    def _pair_record(
        self,
        *,
        score_group: str,
        pair_role: str,
        pair_id: str,
        obs_date: str,
        source_key: str,
        source_name: str,
        provider: str,
        forecast_path: Path,
        observation_path: Path,
        source_count: int,
        source_semantics: str,
        validation_dates_used: str = "",
    ) -> dict:
        return {
            "score_group": score_group,
            "pair_id": pair_id,
            "pair_role": pair_role,
            "obs_date": obs_date,
            "validation_dates_used": validation_dates_used or obs_date,
            "source_key": source_key,
            "source_name": source_name,
            "provider": provider,
            "source_count": source_count,
            "forecast_product": forecast_path.name,
            "forecast_path": str(forecast_path),
            "observation_product": observation_path.name,
            "observation_path": str(observation_path),
            "metric": "FSS",
            "windows_km": ",".join(str(value) for value in OFFICIAL_PHASE3B_WINDOWS_KM),
            "track_label": "appendix_only_short_extended_public_validation",
            "source_semantics": source_semantics,
            "date_composite_rule": self.window.date_composite_rule,
            "precheck_csv": "",
            "precheck_json": "",
        }

    def _score_pairings(self, pairings: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        scored_rows = []
        fss_rows = []
        diagnostics_rows = []
        for _, row in pairings.iterrows():
            forecast_path = Path(str(row["forecast_path"]))
            observation_path = Path(str(row["observation_path"]))
            precheck = precheck_same_grid(
                forecast=forecast_path,
                target=observation_path,
                report_base_path=self.precheck_dir / str(row["pair_id"]),
            )
            if not precheck.passed:
                raise RuntimeError(f"Extended short same-grid precheck failed for {row['pair_id']}: {precheck.json_report_path}")
            forecast = self.helper._load_binary_score_mask(forecast_path)
            obs = self.helper._load_binary_score_mask(observation_path)
            diagnostics = self.helper._compute_mask_diagnostics(forecast, obs)
            scored = row.to_dict()
            scored["precheck_csv"] = str(precheck.csv_report_path)
            scored["precheck_json"] = str(precheck.json_report_path)
            scored_rows.append(scored)
            diagnostics_rows.append({**scored, **diagnostics})
            for window_km in OFFICIAL_PHASE3B_WINDOWS_KM:
                fss = float(
                    np.clip(
                        calculate_fss(
                            forecast,
                            obs,
                            window=self.helper._window_km_to_cells(window_km),
                            valid_mask=self.valid_mask,
                        ),
                        0.0,
                        1.0,
                    )
                )
                fss_rows.append(
                    {
                        "score_group": scored["score_group"],
                        "pair_id": scored["pair_id"],
                        "pair_role": scored["pair_role"],
                        "obs_date": scored["obs_date"],
                        "validation_dates_used": scored["validation_dates_used"],
                        "source_name": scored["source_name"],
                        "window_km": int(window_km),
                        "window_cells": int(self.helper._window_km_to_cells(window_km)),
                        "fss": fss,
                        "forecast_path": scored["forecast_path"],
                        "observation_path": scored["observation_path"],
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

    def _write_outputs(
        self,
        scored_pairings: pd.DataFrame,
        fss_df: pd.DataFrame,
        diagnostics_df: pd.DataFrame,
        summary_df: pd.DataFrame,
        event_summary: pd.DataFrame,
    ) -> dict[str, Path]:
        paths = {
            "pairing_manifest": self.output_dir / "extended_short_pairing_manifest.csv",
            "fss_by_date_window": self.output_dir / "extended_short_fss_by_date_window.csv",
            "diagnostics": self.output_dir / "extended_short_diagnostics.csv",
            "summary": self.output_dir / "extended_short_summary.csv",
            "eventcorridor_pairing_manifest": self.output_dir / "extended_eventcorridor_pairing_manifest.csv",
            "eventcorridor_fss_by_window": self.output_dir / "extended_eventcorridor_fss_by_window.csv",
            "eventcorridor_diagnostics": self.output_dir / "extended_eventcorridor_diagnostics.csv",
            "eventcorridor_summary_md": self.output_dir / "extended_eventcorridor_summary.md",
            "forcing_window_manifest_json": self.output_dir / "extended_forcing_window_manifest.json",
            "forcing_window_manifest_csv": self.output_dir / "extended_forcing_window_manifest.csv",
            "extended_loading_audit_json": self.output_dir / "extended_loading_audit.json",
            "extended_loading_audit_csv": self.output_dir / "extended_loading_audit.csv",
        }
        scored_pairings.to_csv(paths["pairing_manifest"], index=False)
        fss_df.to_csv(paths["fss_by_date_window"], index=False)
        diagnostics_df.to_csv(paths["diagnostics"], index=False)
        summary_df.to_csv(paths["summary"], index=False)
        scored_pairings[scored_pairings["score_group"] == "extended_eventcorridor"].to_csv(
            paths["eventcorridor_pairing_manifest"],
            index=False,
        )
        fss_df[fss_df["score_group"] == "extended_eventcorridor"].to_csv(paths["eventcorridor_fss_by_window"], index=False)
        diagnostics_df[diagnostics_df["score_group"] == "extended_eventcorridor"].to_csv(
            paths["eventcorridor_diagnostics"],
            index=False,
        )
        self._write_eventcorridor_summary_md(event_summary)
        return paths

    def _write_eventcorridor_summary_md(self, event_summary: pd.DataFrame) -> Path:
        path = self.output_dir / "extended_eventcorridor_summary.md"
        if event_summary.empty:
            text = "# Extended Event-Corridor Summary\n\nNo event-corridor score was computed.\n"
        else:
            row = event_summary.iloc[0]
            text = "\n".join(
                [
                    "# Extended Event-Corridor Summary",
                    "",
                    "Appendix-only short extended event corridor, excluding March 3 initialization from forecast-skill scoring.",
                    "",
                    f"- Validation dates represented by observations: {row['validation_dates_used']}",
                    f"- FSS 1/3/5/10 km: {row['fss_1km']:.6f} / {row['fss_3km']:.6f} / {row['fss_5km']:.6f} / {row['fss_10km']:.6f}",
                    f"- Forecast nonzero cells: {row['forecast_nonzero_cells']}",
                    f"- Observation nonzero cells: {row['obs_nonzero_cells']}",
                    f"- Centroid distance m: {row['centroid_distance_m']}",
                ]
            )
        path.write_text(text + "\n", encoding="utf-8")
        return path

    def _write_decision_note(self, summary_df: pd.DataFrame, event_summary: pd.DataFrame) -> Path:
        path = self.output_dir / "extended_short_decision_note.md"
        short_has_forecast = self._short_tier_has_nonzero_forecast(summary_df)
        usable = not summary_df.empty and short_has_forecast
        event_mean = np.nan
        if not event_summary.empty:
            row = event_summary.iloc[0]
            event_mean = float(np.nanmean([row[f"fss_{w}km"] for w in OFFICIAL_PHASE3B_WINDOWS_KM]))
        medium_next = bool(usable and np.isfinite(event_mean) and event_mean > 0.0)
        if usable:
            interpretation = "Short-date p50 masks contain scoreable forecast cells."
        else:
            interpretation = (
                "Short-date p50 masks are empty or near-empty, so the short tier is scored but should not be "
                "treated as a stable quantitative extension without a transport/horizon follow-up."
            )
        lines = [
            "# Extended Short-Tier Decision Note",
            "",
            f"- Short extended tier produced usable quantitative results: {usable}",
            f"- Medium tier should be attempted next: {medium_next}",
            f"- Interpretation: {interpretation}",
            "- Long-tail residual dates should remain out of scope for now because they require a much longer event forecast and substantially more forcing coverage.",
            "",
            "This is appendix-only scoring and does not replace the strict March 6 or within-horizon multi-date Phase 3B tracks.",
        ]
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return path

    @staticmethod
    def _short_tier_has_nonzero_forecast(summary_df: pd.DataFrame) -> bool:
        if summary_df.empty or "forecast_nonzero_cells" not in summary_df.columns:
            return False
        short_rows = summary_df[summary_df["score_group"].isin(["per_source", "per_date_union"])]
        if short_rows.empty:
            return False
        return pd.to_numeric(short_rows["forecast_nonzero_cells"], errors="coerce").fillna(0).gt(0).any()

    @staticmethod
    def _medium_tier_recommendation(summary_df: pd.DataFrame) -> bool:
        return Phase3BExtendedPublicScoredService._short_tier_has_nonzero_forecast(summary_df)

    def _write_qa(self, pairings: pd.DataFrame) -> dict[str, Path]:
        outputs: dict[str, Path] = {}
        if plt is None:
            return outputs
        per_date = pairings[pairings["score_group"] == "per_date_union"].sort_values("obs_date")
        if not per_date.empty:
            path = self.output_dir / "qa_extended_short_overlays.png"
            fig, axes = plt.subplots(1, len(per_date), figsize=(5 * len(per_date), 5))
            if len(per_date) == 1:
                axes = [axes]
            for ax, (_, row) in zip(axes, per_date.iterrows()):
                forecast = self.helper._load_binary_score_mask(Path(row["forecast_path"]))
                obs = self.helper._load_binary_score_mask(Path(row["observation_path"]))
                self._render_overlay(ax, forecast, obs, f"{row['obs_date']} short extended")
            fig.tight_layout()
            fig.savefig(path, dpi=150, bbox_inches="tight")
            plt.close(fig)
            outputs["qa_extended_short_overlays"] = path
        event = pairings[pairings["score_group"] == "extended_eventcorridor"]
        if not event.empty:
            path = self.output_dir / "qa_extended_short_eventcorridor_overlay.png"
            row = event.iloc[0]
            forecast = self.helper._load_binary_score_mask(Path(row["forecast_path"]))
            obs = self.helper._load_binary_score_mask(Path(row["observation_path"]))
            fig, ax = plt.subplots(figsize=(7, 7))
            self._render_overlay(ax, forecast, obs, "Extended short event corridor")
            fig.tight_layout()
            fig.savefig(path, dpi=150, bbox_inches="tight")
            plt.close(fig)
            outputs["qa_extended_short_eventcorridor_overlay"] = path
        return outputs

    @staticmethod
    def _render_overlay(ax, forecast: np.ndarray, obs: np.ndarray, title: str) -> None:
        overlap = (forecast > 0) & (obs > 0)
        canvas = np.ones((forecast.shape[0], forecast.shape[1], 3), dtype=np.float32)
        canvas[obs > 0] = np.array([0.2, 0.45, 0.95], dtype=np.float32)
        canvas[forecast > 0] = np.array([0.95, 0.35, 0.2], dtype=np.float32)
        canvas[overlap] = np.array([0.55, 0.2, 0.75], dtype=np.float32)
        ax.imshow(canvas, origin="upper")
        ax.set_title(title)
        ax.set_axis_off()

    def _strict_hashes(self) -> dict[str, str]:
        files = [
            self.case_output_dir / "phase3b" / "phase3b_pairing_manifest.csv",
            self.case_output_dir / "phase3b" / "phase3b_fss_by_date_window.csv",
            self.case_output_dir / "phase3b" / "phase3b_summary.csv",
            self.case_output_dir / "phase3b" / "phase3b_diagnostics.csv",
            self.case_output_dir / "phase3b" / "phase3b_run_manifest.json",
        ]
        return {str(path): _hash_file(path) for path in files if path.exists()}

    def _multidate_hashes(self) -> dict[str, str]:
        files = [
            self.multidate_dir / "phase3b_multidate_pairing_manifest.csv",
            self.multidate_dir / "phase3b_multidate_fss_by_date_window.csv",
            self.multidate_dir / "phase3b_multidate_summary.csv",
            self.multidate_dir / "phase3b_multidate_diagnostics.csv",
            self.multidate_dir / "phase3b_multidate_run_manifest.json",
            self.multidate_dir / "phase3b_eventcorridor_summary.csv",
        ]
        return {str(path): _hash_file(path) for path in files if path.exists()}


def run_phase3b_extended_public_scored() -> dict:
    return Phase3BExtendedPublicScoredService().run()


if __name__ == "__main__":  # pragma: no cover
    result = run_phase3b_extended_public_scored()
    print(json.dumps(result, indent=2, default=_json_default))
