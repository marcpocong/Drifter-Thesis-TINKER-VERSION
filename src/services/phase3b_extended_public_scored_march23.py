"""Appendix-only March 23 extended public-observation branch stress test for Mindoro."""

from __future__ import annotations

import json
import math
import os
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
from src.services.phase3b_extended_public import (
    EXTENDED_DIR_NAME,
    MARCH23_NOAA_MSI_SOURCE_DATE,
    MARCH23_NOAA_MSI_SOURCE_KEY,
    MARCH23_NOAA_MSI_WHITELIST_REASON,
)
from src.services.phase3b_multidate_public import _as_bool, _hash_file
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


MARCH23_SCORED_DIR_NAME = "phase3b_extended_public_scored_march23"
LOCAL_TIMEZONE = "Asia/Manila"
MAX_OFFICIAL_START_OFFSET_HOURS = 3
LOCKED_OUTPUT_FILES = [
    Path("output/CASE_MINDORO_RETRO_2023/phase3b/phase3b_pairing_manifest.csv"),
    Path("output/CASE_MINDORO_RETRO_2023/phase3b/phase3b_fss_by_date_window.csv"),
    Path("output/CASE_MINDORO_RETRO_2023/phase3b/phase3b_summary.csv"),
    Path("output/CASE_MINDORO_RETRO_2023/phase3b/phase3b_diagnostics.csv"),
    Path("output/CASE_MINDORO_RETRO_2023/phase3b_multidate_public/phase3b_multidate_pairing_manifest.csv"),
    Path("output/CASE_MINDORO_RETRO_2023/phase3b_multidate_public/phase3b_multidate_fss_by_date_window.csv"),
    Path("output/CASE_MINDORO_RETRO_2023/phase3b_multidate_public/phase3b_multidate_summary.csv"),
    Path("output/CASE_MINDORO_RETRO_2023/phase3b_multidate_public/phase3b_multidate_diagnostics.csv"),
    Path("output/CASE_MINDORO_RETRO_2023/public_obs_appendix/appendix_eventcorridor_pairing_manifest.csv"),
    Path("output/CASE_MINDORO_RETRO_2023/public_obs_appendix/appendix_eventcorridor_fss_by_window.csv"),
    Path("output/CASE_MINDORO_RETRO_2023/public_obs_appendix/appendix_eventcorridor_diagnostics.csv"),
    Path("output/final_validation_package/final_validation_main_table.csv"),
    Path("output/final_validation_package/final_validation_summary.md"),
    Path("output/final_validation_package/final_validation_case_registry.csv"),
    Path("output/final_reproducibility_package/final_phase_status_registry.csv"),
    Path("output/final_reproducibility_package/final_case_registry.csv"),
    Path("output/final_reproducibility_package/phase5_packaging_sync_memo.md"),
    Path("output/final_reproducibility_package/phase5_final_verdict.md"),
]


@dataclass(frozen=True)
class March23ExtendedWindow:
    validation_dates: list[str]
    simulation_start_utc: str
    simulation_end_utc: str
    required_forcing_start_utc: str
    required_forcing_end_utc: str
    download_start_date: str
    download_end_date: str
    end_selection_source: str
    date_composite_rule: str


@dataclass(frozen=True)
class March23BranchConfig:
    branch_id: str
    output_slug: str
    description: str
    coastline_action: str
    coastline_approximation_precision: float
    time_step_minutes: int
    branch_precedence: int


BRANCHES = [
    March23BranchConfig("R0", "R0", "Frozen baseline stranding branch.", "stranding", 0.001, 60, 1),
    March23BranchConfig(
        "R1_previous",
        "R1_previous",
        "Selected retention branch using previous wet location on coastline contact.",
        "previous",
        0.001,
        60,
        2,
    ),
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
        return _iso_z(value)
    return str(value)


def _write_json(path: Path, payload: dict | list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, default=_json_default)
        handle.write("\n")


def _write_csv(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False)


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _normalize_utc(value: Any) -> pd.Timestamp:
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        return timestamp
    return timestamp.tz_convert("UTC").tz_localize(None)


def _iso_z(value: Any) -> str:
    return _normalize_utc(value).strftime("%Y-%m-%dT%H:%M:%SZ")


def resolve_march23_extended_window() -> March23ExtendedWindow:
    case = get_case_context()
    simulation_start = _normalize_utc(case.simulation_start_utc)
    simulation_end = pd.Timestamp("2023-03-23 23:59", tz=LOCAL_TIMEZONE).tz_convert("UTC").tz_localize(None)
    required_end = simulation_end + pd.Timedelta(hours=MAX_OFFICIAL_START_OFFSET_HOURS)
    return March23ExtendedWindow(
        validation_dates=[MARCH23_NOAA_MSI_SOURCE_DATE],
        simulation_start_utc=_iso_z(simulation_start),
        simulation_end_utc=_iso_z(simulation_end),
        required_forcing_start_utc=_iso_z(simulation_start - pd.Timedelta(hours=MAX_OFFICIAL_START_OFFSET_HOURS)),
        required_forcing_end_utc=_iso_z(required_end),
        download_start_date=simulation_start.date().isoformat(),
        download_end_date="2023-03-24",
        end_selection_source="fixed_exact_date_march23_local_end",
        date_composite_rule=(
            "Forecast member presence is unioned across model timesteps whose UTC timestamp converts to "
            "2023-03-23 in Asia/Manila; p50 is probability >= 0.50 across members."
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
    return row


class Phase3BExtendedPublicScoredMarch23Service:
    def __init__(self):
        self.case = get_case_context()
        if not self.case.is_official:
            raise RuntimeError("phase3b_extended_public_scored_march23 is only supported for official Mindoro workflows.")
        self.phase_id = "phase3b_extended_public_scored_march23"
        self.forcing_outage_policy = resolve_forcing_outage_policy(
            workflow_mode=self.case.workflow_mode,
            phase=self.phase_id,
        )
        self.case_output_dir = get_case_output_dir(self.case.run_name)
        self.source_extended_dir = self.case_output_dir / EXTENDED_DIR_NAME
        self.output_dir = self.case_output_dir / MARCH23_SCORED_DIR_NAME
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.precheck_dir = self.output_dir / "precheck"
        self.forcing_dir = self.output_dir / "forcing"
        self.precheck_dir.mkdir(parents=True, exist_ok=True)
        self.forcing_dir.mkdir(parents=True, exist_ok=True)
        self.grid = GridBuilder()
        self.sea_mask = load_sea_mask_array(self.grid.spec)
        self.valid_mask = self.sea_mask > 0.5 if self.sea_mask is not None else None
        self.helper = Phase3BScoringService(output_dir=self.output_dir / "_scratch_helper")
        self.window = resolve_march23_extended_window()
        self.force_rerun = os.environ.get("EXTENDED_PUBLIC_FORCE_RERUN", "").strip().lower() in {"1", "true", "yes"}
        self.locked_hashes_before = self._snapshot_locked_outputs()

    def run(self) -> dict:
        obs_row = self._load_march23_accepted_observation()
        self._ensure_scoreable_observation(obs_row)
        selection, recipe_source = self._resolve_recipe()
        forcing_paths = self._prepare_extended_forcing(selection.recipe)
        forcing_manifest = self._write_forcing_window_manifest(selection.recipe, forcing_paths)
        failed_forcing = [row for row in forcing_manifest["rows"] if row["status"] != "ready"]
        if failed_forcing:
            note = self._write_forcing_blocked_note(failed_forcing)
            self._verify_locked_outputs_unchanged()
            raise RuntimeError(
                "March 23 extended forcing coverage is incomplete. "
                f"See {self.output_dir / 'march23_forcing_window_manifest.json'} and {note}."
            )
        branch_runs: list[dict] = []
        branch_products: list[dict] = []
        for branch in BRANCHES:
            run_info = self._run_or_reuse_branch(branch, selection, recipe_source, forcing_paths)
            branch_runs.append(run_info)
            branch_products.append(self._build_branch_local_date_products(branch, run_info))

        pairings = self._build_branch_pairings(obs_row, branch_products)
        scored_pairings, fss_df, diagnostics_df = self._score_pairings(pairings)
        branch_survival_df = pd.DataFrame(branch_products)
        summary_df = self._summarize(scored_pairings, fss_df, diagnostics_df, branch_survival_df)
        qa_paths = self._write_qa_overlays(summary_df)
        decision_note = self._write_decision_note(summary_df, obs_row)
        artifacts = self._write_outputs(scored_pairings, fss_df, diagnostics_df, summary_df, branch_survival_df)
        manifest = self._write_run_manifest(
            obs_row=obs_row,
            summary_df=summary_df,
            branch_runs=branch_runs,
            forcing_manifest=forcing_manifest,
            artifacts=artifacts,
            qa_paths=qa_paths,
            decision_note=decision_note,
            selection=selection,
            recipe_source=recipe_source,
        )
        self._verify_locked_outputs_unchanged()
        return {
            "output_dir": self.output_dir,
            "summary_csv": artifacts["summary_csv"],
            "fss_csv": artifacts["fss_csv"],
            "diagnostics_csv": artifacts["diagnostics_csv"],
            "pairing_manifest_csv": artifacts["pairing_manifest_csv"],
            "branch_survival_csv": artifacts["branch_survival_csv"],
            "forcing_manifest_json": artifacts["forcing_manifest_json"],
            "decision_note_md": decision_note,
            "run_manifest_json": manifest,
            "accepted_source_key": obs_row["source_key"],
            "accepted_obs_date": obs_row["obs_date"],
        }

    def _load_march23_accepted_observation(self) -> pd.Series:
        registry_path = self.source_extended_dir / "extended_public_obs_acceptance_registry.csv"
        if not registry_path.exists():
            raise FileNotFoundError(
                f"Extended observation registry not found: {registry_path}. "
                "Run phase3b_extended_public after the March 23 whitelist patch first."
            )
        registry = pd.read_csv(registry_path)
        accepted = registry[
            registry["accepted_for_extended_quantitative"].map(_as_bool)
            & registry["mask_exists"].map(_as_bool)
            & (registry["obs_date"].astype(str).str.strip() == MARCH23_NOAA_MSI_SOURCE_DATE)
            & (registry["source_key"].astype(str).str.strip() == MARCH23_NOAA_MSI_SOURCE_KEY)
        ].copy()
        accepted = accepted[accepted["extended_obs_mask"].astype(str).map(lambda value: Path(str(value).strip()).exists())].copy()
        if accepted.empty:
            raise RuntimeError(
                "No accepted March 23 extended public observation was found in the extended registry. "
                "Run phase3b_extended_public after the March 23 whitelist patch first."
            )
        if len(accepted) != 1:
            raise RuntimeError(
                "Expected exactly one accepted March 23 observation row for the NOAA/MSI whitelist source, "
                f"found {len(accepted)}."
            )
        return accepted.iloc[0]

    def _ensure_scoreable_observation(self, obs_row: pd.Series) -> None:
        obs_path = Path(str(obs_row["extended_obs_mask"]))
        obs_mask = self.helper._load_binary_score_mask(obs_path)
        obs_nonzero = int(np.count_nonzero(obs_mask > 0))
        if obs_nonzero > 0:
            return
        note = self._write_scoreability_blocked_note(obs_row, obs_nonzero)
        self._verify_locked_outputs_unchanged()
        raise RuntimeError(f"March 23 source not scoreable after rasterization. See {note}.")

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
                    skipped_branch_ids=[branch.branch_id for branch in BRANCHES],
                    manifest_path=str(manifest_path),
                    budget_seconds=downloads["currents"].get("budget_seconds"),
                    elapsed_seconds=downloads["currents"].get("elapsed_seconds"),
                    budget_exhausted=bool(downloads["currents"].get("budget_exhausted", False)),
                    failure_stage=str(downloads["currents"].get("failure_stage") or ""),
                )
            self._write_download_failure_manifest(recipe_name, downloads)
            raise RuntimeError(f"March 23 forcing download failed: {downloads['currents']}")

        if wind_file.startswith("gfs"):
            gfs_path = self.forcing_dir / wind_file
            if not gfs_path.exists():
                raise FileNotFoundError(
                    f"GFS wind forcing was requested for the March 23 stress test but is not available locally: {gfs_path}."
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
                        skipped_branch_ids=[branch.branch_id for branch in BRANCHES],
                        manifest_path=str(manifest_path),
                        budget_seconds=downloads["wind"].get("budget_seconds"),
                        elapsed_seconds=downloads["wind"].get("elapsed_seconds"),
                        budget_exhausted=bool(downloads["wind"].get("budget_exhausted", False)),
                        failure_stage=str(downloads["wind"].get("failure_stage") or ""),
                    )
                self._write_download_failure_manifest(recipe_name, downloads)
                raise RuntimeError(f"March 23 forcing download failed: {downloads['wind']}")
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
                        skipped_branch_ids=[branch.branch_id for branch in BRANCHES],
                        manifest_path=str(manifest_path),
                        budget_seconds=downloads["wave"].get("budget_seconds"),
                        elapsed_seconds=downloads["wave"].get("elapsed_seconds"),
                        budget_exhausted=bool(downloads["wave"].get("budget_exhausted", False)),
                        failure_stage=str(downloads["wave"].get("failure_stage") or ""),
                    )
                self._write_download_failure_manifest(recipe_name, downloads)
                raise RuntimeError(f"March 23 forcing download failed: {downloads['wave']}")

        return {
            "recipe": recipe_name,
            "currents": self.forcing_dir / current_file,
            "wind": self.forcing_dir / wind_file,
            "wave": self.forcing_dir / wave_file if wave_file else None,
            "downloads": downloads,
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
            "window": asdict(self.window),
            "downloads": downloads,
            "status": status,
            "forcing_outage_policy": self.forcing_outage_policy,
            "degraded_continue_used": degraded_continue_used,
            "upstream_outage_detected": upstream_outage_detected,
            "missing_forcing_factors": list(missing_forcing_factors or []),
            "rerun_required": bool(degraded_continue_used),
            "stop_reason": stop_reason,
        }
        manifest_json = self.output_dir / "march23_forcing_window_manifest.json"
        _write_json(manifest_json, payload)
        _write_csv(
            self.output_dir / "march23_forcing_window_manifest.csv",
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
            ),
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
            "track": "appendix_only_march23_extended_public_stress_test",
            "recipe": recipe_name,
            "window": asdict(self.window),
            "rows": rows,
            "forcing_outage_policy": self.forcing_outage_policy,
            "degraded_continue_used": False,
            "missing_forcing_factors": [],
            "rerun_required": False,
            "status": "ready" if all(row["status"] == "ready" for row in rows) else "insufficient",
        }
        _write_json(self.output_dir / "march23_forcing_window_manifest.json", payload)
        _write_csv(self.output_dir / "march23_forcing_window_manifest.csv", pd.DataFrame(rows))
        return payload

    def _run_or_reuse_branch(self, branch: March23BranchConfig, selection, recipe_source: str, forcing_paths: dict) -> dict:
        model_run_name = f"{self.case.run_name}/{MARCH23_SCORED_DIR_NAME}/{branch.output_slug}/model_run"
        model_dir = get_case_output_dir(model_run_name)
        member_paths = sorted((model_dir / "ensemble").glob("member_*.nc"))
        forecast_manifest = model_dir / "forecast" / "forecast_manifest.json"
        if member_paths and forecast_manifest.exists() and not self.force_rerun:
            return {
                "branch_id": branch.branch_id,
                "branch_description": branch.description,
                "status": "reused_existing_branch_run",
                "model_dir": str(model_dir),
                "model_run_name": model_run_name,
                "forecast_result": {"status": "reused_existing_branch_run", "member_count": len(member_paths)},
            }

        start_lat, start_lon, start_time = resolve_spill_origin()
        simulation_start = _normalize_utc(self.window.simulation_start_utc)
        simulation_end = _normalize_utc(self.window.simulation_end_utc)
        duration_hours = int(math.ceil((simulation_end - simulation_start).total_seconds() / 3600.0))
        snapshot_hours = sorted(set([24, 48, 72, 96, 120, 144, duration_hours]))
        result = run_official_spill_forecast(
            selection=selection,
            start_time=start_time,
            start_lat=start_lat,
            start_lon=start_lon,
            output_run_name=model_run_name,
            forcing_override=forcing_paths,
            sensitivity_context={
                "track": "appendix_only_march23_extended_public_stress_test",
                "branch_id": branch.branch_id,
                "branch_description": branch.description,
                "recipe_source": recipe_source,
                "single_date_validation": MARCH23_NOAA_MSI_SOURCE_DATE,
                "date_composite_rule": self.window.date_composite_rule,
                "appendix_only": True,
            },
            historical_baseline_provenance={
                "recipe": selection.recipe,
                "source_kind": selection.source_kind,
                "source_path": selection.source_path,
                "note": selection.note,
            },
            simulation_start_utc=self.window.simulation_start_utc,
            simulation_end_utc=self.window.simulation_end_utc,
            snapshot_hours=snapshot_hours,
            date_composite_dates=list(self.window.validation_dates),
            transport_overrides={
                "coastline_action": branch.coastline_action,
                "coastline_approximation_precision": branch.coastline_approximation_precision,
                "time_step_minutes": branch.time_step_minutes,
            },
        )
        if result.get("status") != "success":
            raise RuntimeError(f"March 23 forecast failed for {branch.branch_id}: {result}")
        return {
            "branch_id": branch.branch_id,
            "branch_description": branch.description,
            "status": "completed_new_branch_run",
            "model_dir": str(model_dir),
            "model_run_name": model_run_name,
            "forecast_result": result,
        }

    def _build_branch_local_date_products(self, branch: March23BranchConfig, run_info: dict) -> dict:
        model_dir = Path(str(run_info["model_dir"]))
        composite_dir = self.output_dir / branch.output_slug / "forecast_datecomposites"
        composite_dir.mkdir(parents=True, exist_ok=True)
        prob_path = composite_dir / f"prob_presence_{MARCH23_NOAA_MSI_SOURCE_DATE}_localdate.tif"
        p50_path = composite_dir / f"mask_p50_{MARCH23_NOAA_MSI_SOURCE_DATE}_localdate.tif"

        member_paths = sorted((model_dir / "ensemble").glob("member_*.nc"))
        member_masks: list[np.ndarray] = []
        last_active_time: pd.Timestamp | None = None
        march23_active_timestamps: set[str] = set()
        if member_paths:
            if xr is None:
                raise ImportError("xarray is required to build March 23 local-date composites.")
            for member_path in member_paths:
                composite = np.zeros((self.grid.height, self.grid.width), dtype=np.float32)
                with xr.open_dataset(member_path) as ds:
                    times = normalize_time_index(ds["time"].values)
                    for index, timestamp in enumerate(times):
                        utc_timestamp = _normalize_utc(timestamp)
                        lon = np.asarray(ds["lon"].isel(time=index).values).reshape(-1)
                        lat = np.asarray(ds["lat"].isel(time=index).values).reshape(-1)
                        status = np.asarray(ds["status"].isel(time=index).values).reshape(-1)
                        valid = np.isfinite(lon) & np.isfinite(lat) & (status == 0)
                        if not np.any(valid):
                            continue
                        last_active_time = utc_timestamp if last_active_time is None else max(last_active_time, utc_timestamp)
                        local_date = pd.Timestamp(utc_timestamp).tz_localize("UTC").tz_convert(LOCAL_TIMEZONE).date().isoformat()
                        if local_date != MARCH23_NOAA_MSI_SOURCE_DATE:
                            continue
                        march23_active_timestamps.add(_iso_z(utc_timestamp))
                        hits, _ = rasterize_particles(
                            self.grid,
                            lon[valid],
                            lat[valid],
                            np.ones(int(np.count_nonzero(valid)), dtype=np.float32),
                        )
                        composite = np.maximum(composite, hits)
                member_masks.append(apply_ocean_mask(composite.astype(np.float32), sea_mask=self.sea_mask, fill_value=0.0))

        probability = (
            np.mean(np.stack(member_masks, axis=0), axis=0).astype(np.float32)
            if member_masks
            else np.zeros((self.grid.height, self.grid.width), dtype=np.float32)
        )
        probability = apply_ocean_mask(probability, sea_mask=self.sea_mask, fill_value=0.0)
        p50 = apply_ocean_mask((probability >= 0.5).astype(np.float32), sea_mask=self.sea_mask, fill_value=0.0)
        save_raster(self.grid, probability.astype(np.float32), prob_path)
        save_raster(self.grid, p50.astype(np.float32), p50_path)

        forecast_nonzero_cells = int(np.count_nonzero(p50 > 0))
        forecast_result_status = str((run_info.get("forecast_result") or {}).get("status") or run_info.get("status") or "").strip()
        if forecast_nonzero_cells > 0:
            empty_forecast_reason = ""
        elif not member_paths and forecast_result_status:
            empty_forecast_reason = f"forecast_run_status_{forecast_result_status}"
        elif not member_paths:
            empty_forecast_reason = "no_ensemble_member_outputs_found"
        elif not march23_active_timestamps:
            empty_forecast_reason = "model_survival_did_not_reach_march23_local_date"
        else:
            empty_forecast_reason = "march23_local_activity_present_but_no_scoreable_ocean_presence_after_masking"

        return {
            "branch_id": branch.branch_id,
            "branch_output_slug": branch.output_slug,
            "branch_description": branch.description,
            "branch_precedence": branch.branch_precedence,
            "branch_run_status": run_info["status"],
            "forecast_result_status": forecast_result_status,
            "model_dir": str(model_dir),
            "model_run_name": run_info["model_run_name"],
            "probability_path": str(prob_path),
            "forecast_path": str(p50_path),
            "member_count": int(len(member_paths)),
            "last_active_particle_time_utc": _iso_z(last_active_time) if last_active_time is not None else "",
            "march23_local_active_timestamp_count": int(len(march23_active_timestamps)),
            "march23_local_active_timestamps": ";".join(sorted(march23_active_timestamps)),
            "reached_march23_local_date": bool(march23_active_timestamps),
            "empty_forecast_reason": empty_forecast_reason,
            "forecast_nonzero_cells_from_localdate_mask": forecast_nonzero_cells,
        }

    def _build_branch_pairings(self, obs_row: pd.Series, branch_products: list[dict]) -> pd.DataFrame:
        obs_path = Path(str(obs_row["extended_obs_mask"]))
        rows = []
        for product in branch_products:
            forecast_path = Path(str(product["forecast_path"]))
            probability_path = Path(str(product["probability_path"]))
            rows.append(
                {
                    "pair_id": f"march23_branch_{product['branch_id']}",
                    "pair_role": "extended_public_march23_branch_compare",
                    "score_group": "single_date_branch_compare",
                    "obs_date": MARCH23_NOAA_MSI_SOURCE_DATE,
                    "validation_dates_used": MARCH23_NOAA_MSI_SOURCE_DATE,
                    "source_key": str(obs_row["source_key"]),
                    "source_name": str(obs_row["source_name"]),
                    "provider": str(obs_row.get("provider", "")),
                    "branch_id": product["branch_id"],
                    "branch_description": product["branch_description"],
                    "branch_precedence": int(product["branch_precedence"]),
                    "branch_model_dir": str(product["model_dir"]),
                    "branch_model_run_name": str(product["model_run_name"]),
                    "branch_run_status": str(product["branch_run_status"]),
                    "forecast_product": forecast_path.name,
                    "forecast_path": str(forecast_path),
                    "probability_path": str(probability_path),
                    "observation_product": obs_path.name,
                    "observation_path": str(obs_path),
                    "metric": "FSS",
                    "windows_km": ",".join(str(value) for value in OFFICIAL_PHASE3B_WINDOWS_KM),
                    "track_label": "appendix_only_march23_extended_public_stress_test",
                    "source_semantics": "single_date_public_obs_vs_local_date_branch_p50",
                    "empty_forecast_reason": str(product["empty_forecast_reason"]),
                    "precheck_csv": "",
                    "precheck_json": "",
                }
            )
        return pd.DataFrame(rows)

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
                raise RuntimeError(f"March 23 same-grid precheck failed for {row['pair_id']}: {precheck.json_report_path}")
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
                        "pair_id": scored["pair_id"],
                        "branch_id": scored["branch_id"],
                        "obs_date": scored["obs_date"],
                        "window_km": int(window_km),
                        "window_cells": int(self.helper._window_km_to_cells(window_km)),
                        "fss": fss,
                        "forecast_path": scored["forecast_path"],
                        "observation_path": scored["observation_path"],
                    }
                )
        return pd.DataFrame(scored_rows), pd.DataFrame(fss_rows), pd.DataFrame(diagnostics_rows)

    @staticmethod
    def _summarize(
        scored_pairings: pd.DataFrame,
        fss_df: pd.DataFrame,
        diagnostics_df: pd.DataFrame,
        branch_survival_df: pd.DataFrame,
    ) -> pd.DataFrame:
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
        summary = (
            scored_pairings.merge(diagnostics_df[diag_cols], on="pair_id", how="left")
            .merge(branch_survival_df, on="branch_id", how="left", suffixes=("", "_survival"))
            .merge(fss_pivot, on="pair_id", how="left")
        )
        summary["mean_fss"] = summary[[f"fss_{window}km" for window in OFFICIAL_PHASE3B_WINDOWS_KM]].mean(axis=1)
        return summary.sort_values(["branch_precedence"]).reset_index(drop=True)

    def _write_outputs(
        self,
        scored_pairings: pd.DataFrame,
        fss_df: pd.DataFrame,
        diagnostics_df: pd.DataFrame,
        summary_df: pd.DataFrame,
        branch_survival_df: pd.DataFrame,
    ) -> dict[str, Path]:
        artifacts = {
            "pairing_manifest_csv": self.output_dir / "march23_branch_pairing_manifest.csv",
            "fss_csv": self.output_dir / "march23_fss_by_window.csv",
            "diagnostics_csv": self.output_dir / "march23_diagnostics.csv",
            "summary_csv": self.output_dir / "march23_summary.csv",
            "branch_survival_csv": self.output_dir / "march23_branch_survival_summary.csv",
            "forcing_manifest_json": self.output_dir / "march23_forcing_window_manifest.json",
            "forcing_manifest_csv": self.output_dir / "march23_forcing_window_manifest.csv",
        }
        _write_csv(artifacts["pairing_manifest_csv"], scored_pairings)
        _write_csv(artifacts["fss_csv"], fss_df)
        _write_csv(artifacts["diagnostics_csv"], diagnostics_df)
        _write_csv(artifacts["summary_csv"], summary_df)
        _write_csv(artifacts["branch_survival_csv"], branch_survival_df)
        return artifacts

    def _write_decision_note(self, summary_df: pd.DataFrame, obs_row: pd.Series) -> Path:
        path = self.output_dir / "march23_decision_note.md"
        both_empty = pd.to_numeric(summary_df.get("forecast_nonzero_cells"), errors="coerce").fillna(0).eq(0).all()
        survival_blocked = both_empty and summary_df["empty_forecast_reason"].astype(str).str.contains(
            "model_survival", case=False, na=False
        ).all()
        best_row = summary_df.sort_values(["mean_fss", "fss_1km", "branch_precedence"], ascending=[False, False, True]).iloc[0]
        lines = [
            "# March 23 Extended Public Stress Test Decision Note",
            "",
            "- Appendix-only track: true",
            f"- Public observation source key: {obs_row['source_key']}",
            f"- Public observation source name: {obs_row['source_name']}",
            f"- Observation date: {MARCH23_NOAA_MSI_SOURCE_DATE}",
            f"- Observation source policy exception: {MARCH23_NOAA_MSI_WHITELIST_REASON}",
            f"- Best branch by mean FSS: {best_row['branch_id']}",
            f"- Best branch mean FSS: {float(best_row['mean_fss']):.6f}",
            f"- Best branch FSS 1/3/5/10 km: {float(best_row['fss_1km']):.6f} / {float(best_row['fss_3km']):.6f} / {float(best_row['fss_5km']):.6f} / {float(best_row['fss_10km']):.6f}",
        ]
        if survival_blocked:
            lines.append("- Decision: March 23 comparison is blocked by model survival, not by missing public data.")
        elif both_empty:
            lines.append("- Decision: Both branches produced empty March 23 p50 masks; inspect empty_forecast_reason before interpreting skill.")
        else:
            lines.append("- Decision: At least one branch produced a scoreable March 23 p50 mask, so the branch comparison is usable as appendix support.")
        lines.extend(
            [
                "",
                "This is appendix-only support material.",
                "It does not replace the frozen strict March 6 result, the within-horizon public main track, or the final validation package.",
                "The comparison is intentionally limited to R0 and R1_previous with the frozen recipe selection.",
            ]
        )
        _write_text(path, "\n".join(lines) + "\n")
        return path

    def _write_scoreability_blocked_note(self, obs_row: pd.Series, obs_nonzero_cells: int) -> Path:
        path = self.output_dir / "march23_source_not_scoreable_after_rasterization.md"
        text = "\n".join(
            [
                "# March 23 Source Not Scoreable After Rasterization",
                "",
                f"- Source key: {obs_row['source_key']}",
                f"- Source name: {obs_row['source_name']}",
                f"- Observation date: {obs_row['obs_date']}",
                f"- Raster nonzero cells after ocean masking: {obs_nonzero_cells}",
                "- Status: blocked before any forecast rerun",
                "",
                "The accepted March 23 public polygon rasterized to zero ocean cells on the canonical scoring grid.",
                "This stress-test phase stops here instead of fabricating a forecast comparison against a non-scoreable target.",
            ]
        )
        _write_text(path, text + "\n")
        return path

    def _write_forcing_blocked_note(self, failed_rows: list[dict]) -> Path:
        path = self.output_dir / "march23_forcing_blocked.md"
        lines = [
            "# March 23 Forcing Coverage Blocked",
            "",
            "The March 23 appendix stress test did not rerun the forecast branches because the prepared forcing window was incomplete.",
            "",
            "## Blocking Rows",
            "",
        ]
        lines.extend(f"- {row.get('forcing_kind', 'forcing')}: {row.get('stop_reason', '')}" for row in failed_rows)
        _write_text(path, "\n".join(lines) + "\n")
        return path

    def _write_qa_overlays(self, summary_df: pd.DataFrame) -> dict[str, Path]:
        outputs: dict[str, Path] = {}
        if plt is None:
            return outputs
        for _, row in summary_df.iterrows():
            forecast = self.helper._load_binary_score_mask(Path(str(row["forecast_path"])))
            obs = self.helper._load_binary_score_mask(Path(str(row["observation_path"])))
            fig, ax = plt.subplots(figsize=(7, 7))
            self._render_overlay(ax, forecast, obs, f"March 23 {row['branch_id']}")
            fig.tight_layout()
            path = self.output_dir / f"qa_march23_{row['branch_id']}_overlay.png"
            fig.savefig(path, dpi=150, bbox_inches="tight")
            plt.close(fig)
            outputs[f"qa_{row['branch_id']}_overlay"] = path
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

    def _write_run_manifest(
        self,
        *,
        obs_row: pd.Series,
        summary_df: pd.DataFrame,
        branch_runs: list[dict],
        forcing_manifest: dict,
        artifacts: dict[str, Path],
        qa_paths: dict[str, Path],
        decision_note: Path,
        selection,
        recipe_source: str,
    ) -> Path:
        path = self.output_dir / "march23_run_manifest.json"
        payload = {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "track": "appendix_only_march23_extended_public_stress_test",
            "appendix_only": True,
            "workflow_mode": self.case.workflow_mode,
            "run_name": self.case.run_name,
            "window": asdict(self.window),
            "selected_truth_source": {
                "source_key": str(obs_row["source_key"]),
                "source_name": str(obs_row["source_name"]),
                "provider": str(obs_row.get("provider", "")),
                "obs_date": str(obs_row["obs_date"]),
                "extended_truth_exception_applied": bool(_as_bool(obs_row.get("extended_truth_exception_applied"))),
                "extended_truth_exception_note": str(obs_row.get("extended_truth_exception_note", "")),
                "scoreable_after_rasterization": bool(_as_bool(obs_row.get("scoreable_after_rasterization", True))),
                "scoreability_note": str(obs_row.get("scoreability_note", "")),
            },
            "truth_source_policy": {
                "one_off_whitelist_source_key": MARCH23_NOAA_MSI_SOURCE_KEY,
                "one_off_whitelist_reason": MARCH23_NOAA_MSI_WHITELIST_REASON,
                "broader_bulletin_exclusion_relaxed": False,
            },
            "recipe": {
                "recipe": selection.recipe,
                "source_kind": selection.source_kind,
                "source_path": selection.source_path,
                "status_flag": selection.status_flag,
                "note": selection.note,
                "recipe_source": recipe_source,
            },
            "branches": [asdict(branch) for branch in BRANCHES],
            "branch_runs": branch_runs,
            "forcing_manifest": forcing_manifest,
            "strict_public_main_outputs_unchanged": True,
            "locked_output_hashes_before": self.locked_hashes_before,
            "locked_output_hashes_after": self._snapshot_locked_outputs(),
            "artifacts": {
                **{key: str(value) for key, value in artifacts.items()},
                "decision_note_md": str(decision_note),
                **{key: str(value) for key, value in qa_paths.items()},
            },
            "score_summary": summary_df.to_dict(orient="records"),
        }
        _write_json(path, payload)
        return path

    def _snapshot_locked_outputs(self) -> dict[str, str]:
        return {str(path): _hash_file(path) for path in LOCKED_OUTPUT_FILES if path.exists()}

    def _verify_locked_outputs_unchanged(self) -> None:
        current = self._snapshot_locked_outputs()
        if current != self.locked_hashes_before:
            raise RuntimeError("March 23 extended public stress test modified locked strict/public-main outputs.")


def run_phase3b_extended_public_scored_march23() -> dict:
    return Phase3BExtendedPublicScoredMarch23Service().run()


if __name__ == "__main__":  # pragma: no cover
    result = run_phase3b_extended_public_scored_march23()
    print(json.dumps(result, indent=2, default=_json_default))
