"""Mindoro Phase 3B March 13 -> March 14 primary public-validation source bundle."""

from __future__ import annotations

import json
import math
import os
from contextlib import contextmanager
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
from src.services.ensemble import OFFICIAL_ELEMENT_COUNT_OVERRIDE_ENV, normalize_time_index, run_official_spill_forecast
from src.services.ingestion import DataIngestionService
from src.services.mindoro_primary_validation_metadata import (
    MINDORO_BASE_CASE_CONFIG_PATH,
    MINDORO_PRIMARY_VALIDATION_AMENDMENT_PATH,
    MINDORO_PRIMARY_VALIDATION_LAUNCHER_ALIAS_ENTRY_ID,
    MINDORO_PRIMARY_VALIDATION_LAUNCHER_ENTRY_ID,
    MINDORO_PRIMARY_VALIDATION_PHASE_OR_TRACK,
    MINDORO_PRIMARY_VALIDATION_TRACK_ID,
    MINDORO_PRIMARY_VALIDATION_TRACK_LABEL,
    MINDORO_OBSERVATION_INDEPENDENCE_NOTE,
)
from src.services.phase3b_extended_public import EXTENDED_DIR_NAME
from src.services.phase3b_multidate_public import _as_bool, _hash_file
from src.services.scoring import OFFICIAL_PHASE3B_WINDOWS_KM, Phase3BScoringService
from src.utils.forcing_outage_policy import (
    FORCING_OUTAGE_POLICY_CONTINUE_DEGRADED,
    resolve_forcing_outage_policy,
    resolve_forcing_source_budget_seconds,
    source_id_for_recipe_component,
)
from src.utils.local_input_store import (
    PERSISTENT_LOCAL_INPUT_STORE,
    persistent_local_input_dir,
    stage_store_file,
)
from src.utils.io import _resolve_polygon_reference_point, find_current_vars, get_case_output_dir, resolve_recipe_selection
from src.utils.startup_prompt_policy import input_cache_policy_force_refresh_enabled

try:
    import matplotlib.pyplot as plt
except ImportError:  # pragma: no cover
    plt = None

try:
    import xarray as xr
except ImportError:  # pragma: no cover
    xr = None


MARCH13_14_REINIT_DIR_NAME = "phase3b_extended_public_scored_march13_14_reinit"
MARCH13_NOAA_SOURCE_KEY = "8f8e3944748c4772910efc9829497e20"
MARCH14_NOAA_SOURCE_KEY = "10b37c42a9754363a5f7b14199b077e6"
MARCH13_NOAA_SOURCE_DATE = "2023-03-13"
MARCH14_NOAA_SOURCE_DATE = "2023-03-14"
LOCAL_TIMEZONE = "Asia/Manila"
MAX_OFFICIAL_START_OFFSET_HOURS = 3
REQUESTED_ELEMENT_COUNT = 100000
EXPECTED_ENSEMBLE_MEMBER_COUNT = 50
TRACK_LABEL = "mindoro_phase3b_primary_public_validation_reinit"
PHASE_OR_TRACK = MINDORO_PRIMARY_VALIDATION_PHASE_OR_TRACK
REPORTING_ROLE = "canonical_phase3b_public_validation_source"
START_SOURCE_GEOMETRY_LABEL = "accepted_march13_noaa_processed_polygon"
NOAA_SOURCE_LIMITATION_NOTE = MINDORO_OBSERVATION_INDEPENDENCE_NOTE
PHASE3B_REINIT_OUTPUT_DIR_NAME_ENV = "PHASE3B_REINIT_OUTPUT_DIR_NAME"
PHASE3B_REINIT_TRACK_OVERRIDE_ENV = "PHASE3B_REINIT_TRACK_OVERRIDE"
PHASE3B_REINIT_TRACK_ID_OVERRIDE_ENV = "PHASE3B_REINIT_TRACK_ID_OVERRIDE"
PHASE3B_REINIT_TRACK_LABEL_OVERRIDE_ENV = "PHASE3B_REINIT_TRACK_LABEL_OVERRIDE"
PHASE3B_REINIT_REPORTING_ROLE_OVERRIDE_ENV = "PHASE3B_REINIT_REPORTING_ROLE_OVERRIDE"
PHASE3B_REINIT_APPENDIX_ONLY_ENV = "PHASE3B_REINIT_APPENDIX_ONLY"
PHASE3B_REINIT_PRIMARY_PUBLIC_VALIDATION_ENV = "PHASE3B_REINIT_PRIMARY_PUBLIC_VALIDATION"
PHASE3B_REINIT_LAUNCHER_ENTRY_ID_OVERRIDE_ENV = "PHASE3B_REINIT_LAUNCHER_ENTRY_ID_OVERRIDE"
PHASE3B_REINIT_REQUESTED_ELEMENT_COUNT_ENV = "PHASE3B_REINIT_REQUESTED_ELEMENT_COUNT"
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
class ReinitWindow:
    forecast_local_dates: list[str]
    scored_target_date: str
    seed_obs_date: str
    simulation_start_utc: str
    simulation_end_utc: str
    required_forcing_start_utc: str
    required_forcing_end_utc: str
    download_start_date: str
    download_end_date: str
    end_selection_source: str
    date_composite_rule: str


@dataclass(frozen=True)
class ReinitBranchConfig:
    branch_id: str
    output_slug: str
    description: str
    coastline_action: str
    coastline_approximation_precision: float
    time_step_minutes: int
    branch_precedence: int


BRANCHES = [
    ReinitBranchConfig("R0", "R0", "Frozen baseline stranding branch.", "stranding", 0.001, 60, 1),
    ReinitBranchConfig(
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


def resolve_march13_14_reinit_window() -> ReinitWindow:
    simulation_start = pd.Timestamp("2023-03-13 00:00", tz=LOCAL_TIMEZONE).tz_convert("UTC").tz_localize(None)
    simulation_end = pd.Timestamp("2023-03-14 23:59", tz=LOCAL_TIMEZONE).tz_convert("UTC").tz_localize(None)
    required_end = simulation_end + pd.Timedelta(hours=MAX_OFFICIAL_START_OFFSET_HOURS)
    return ReinitWindow(
        forecast_local_dates=[MARCH13_NOAA_SOURCE_DATE, MARCH14_NOAA_SOURCE_DATE],
        scored_target_date=MARCH14_NOAA_SOURCE_DATE,
        seed_obs_date=MARCH13_NOAA_SOURCE_DATE,
        simulation_start_utc=_iso_z(simulation_start),
        simulation_end_utc=_iso_z(simulation_end),
        required_forcing_start_utc=_iso_z(simulation_start - pd.Timedelta(hours=MAX_OFFICIAL_START_OFFSET_HOURS)),
        required_forcing_end_utc=_iso_z(required_end),
        download_start_date="2023-03-12",
        download_end_date="2023-03-15",
        end_selection_source="fixed_next_day_local_date_window",
        date_composite_rule=(
            "Forecast member presence is unioned across model timesteps whose UTC timestamp converts to the "
            "local dates 2023-03-13 and 2023-03-14 in Asia/Manila; only the 2023-03-14 local-date p50 product is scored."
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
        if missing and tuple(required_vars) == ("uo", "vo"):
            try:
                find_current_vars(ds)
            except KeyError:
                pass
            else:
                missing = []
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


@contextmanager
def _temporary_element_count_override(element_count: int):
    previous = os.environ.get(OFFICIAL_ELEMENT_COUNT_OVERRIDE_ENV)
    os.environ[OFFICIAL_ELEMENT_COUNT_OVERRIDE_ENV] = str(int(element_count))
    try:
        yield
    finally:
        if previous is None:
            os.environ.pop(OFFICIAL_ELEMENT_COUNT_OVERRIDE_ENV, None)
        else:
            os.environ[OFFICIAL_ELEMENT_COUNT_OVERRIDE_ENV] = previous


def _read_json(path: Path) -> dict:
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle) or {}


def _resolve_requested_element_count(value: Any = None) -> int:
    raw_value = value
    if raw_value is None:
        raw_value = os.environ.get(PHASE3B_REINIT_REQUESTED_ELEMENT_COUNT_ENV, str(REQUESTED_ELEMENT_COUNT))
    text = str(raw_value or "").strip()
    if not text:
        return int(REQUESTED_ELEMENT_COUNT)
    resolved = int(text)
    if resolved <= 0:
        raise ValueError(f"{PHASE3B_REINIT_REQUESTED_ELEMENT_COUNT_ENV} must be > 0.")
    return resolved


def _consensus_int(values: dict[str, Any]) -> int | None:
    normalized = {int(value) for value in values.values() if str(value).strip()}
    if len(normalized) != 1:
        return None
    return next(iter(normalized))


class Phase3BExtendedPublicScoredMarch1314ReinitService:
    def __init__(self):
        self.case = get_case_context()
        if not self.case.is_official:
            raise RuntimeError(
                "phase3b_extended_public_scored_march13_14_reinit is only supported for official Mindoro workflows."
            )
        self.phase_id = "phase3b_extended_public_scored_march13_14_reinit"
        self.forcing_outage_policy = resolve_forcing_outage_policy(
            workflow_mode=self.case.workflow_mode,
            phase=self.phase_id,
        )
        self.output_dir_name = str(
            os.environ.get(PHASE3B_REINIT_OUTPUT_DIR_NAME_ENV, MARCH13_14_REINIT_DIR_NAME)
        ).strip() or MARCH13_14_REINIT_DIR_NAME
        self.track = str(os.environ.get(PHASE3B_REINIT_TRACK_OVERRIDE_ENV, TRACK_LABEL)).strip() or TRACK_LABEL
        self.track_id = str(
            os.environ.get(PHASE3B_REINIT_TRACK_ID_OVERRIDE_ENV, MINDORO_PRIMARY_VALIDATION_TRACK_ID)
        ).strip() or MINDORO_PRIMARY_VALIDATION_TRACK_ID
        self.track_label = str(
            os.environ.get(PHASE3B_REINIT_TRACK_LABEL_OVERRIDE_ENV, MINDORO_PRIMARY_VALIDATION_TRACK_LABEL)
        ).strip() or MINDORO_PRIMARY_VALIDATION_TRACK_LABEL
        self.reporting_role = str(
            os.environ.get(PHASE3B_REINIT_REPORTING_ROLE_OVERRIDE_ENV, REPORTING_ROLE)
        ).strip() or REPORTING_ROLE
        self.appendix_only = _as_bool(os.environ.get(PHASE3B_REINIT_APPENDIX_ONLY_ENV, "false"))
        self.primary_public_validation = _as_bool(
            os.environ.get(
                PHASE3B_REINIT_PRIMARY_PUBLIC_VALIDATION_ENV,
                "true" if self.reporting_role == REPORTING_ROLE else "false",
            )
        )
        self.launcher_entry_id_override = str(
            os.environ.get(PHASE3B_REINIT_LAUNCHER_ENTRY_ID_OVERRIDE_ENV, "")
        ).strip()
        self.requested_element_count = _resolve_requested_element_count()
        self.is_canonical_bundle = (
            self.output_dir_name == MARCH13_14_REINIT_DIR_NAME
            and self.track == TRACK_LABEL
            and self.track_id == MINDORO_PRIMARY_VALIDATION_TRACK_ID
            and self.track_label == MINDORO_PRIMARY_VALIDATION_TRACK_LABEL
            and self.reporting_role == REPORTING_ROLE
            and not self.appendix_only
            and self.primary_public_validation
            and self.requested_element_count == REQUESTED_ELEMENT_COUNT
        )
        self.reportable = bool(self.is_canonical_bundle)
        self.experimental_only = not self.reportable
        self.thesis_facing = bool(self.reportable)
        self.case_output_dir = get_case_output_dir(self.case.run_name)
        self.source_extended_dir = self.case_output_dir / EXTENDED_DIR_NAME
        self.output_dir = self.case_output_dir / self.output_dir_name
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.precheck_dir = self.output_dir / "precheck"
        self.forcing_dir = self.output_dir / "forcing"
        self.persistent_forcing_dir = persistent_local_input_dir(self.case.run_name, self.output_dir_name, "forcing")
        self.precheck_dir.mkdir(parents=True, exist_ok=True)
        self.forcing_dir.mkdir(parents=True, exist_ok=True)
        self.persistent_forcing_dir.mkdir(parents=True, exist_ok=True)
        self.grid = GridBuilder()
        self.sea_mask = load_sea_mask_array(self.grid.spec)
        self.valid_mask = self.sea_mask > 0.5 if self.sea_mask is not None else None
        self.helper = Phase3BScoringService(output_dir=self.output_dir / "_scratch_helper")
        self.window = resolve_march13_14_reinit_window()
        self.force_rerun = os.environ.get("EXTENDED_PUBLIC_FORCE_RERUN", "").strip().lower() in {"1", "true", "yes"}
        self.locked_hashes_before = self._snapshot_locked_outputs()

    @staticmethod
    def _force_refresh_enabled() -> bool:
        return input_cache_policy_force_refresh_enabled()

    def _persistent_forcing_store_dir(self) -> Path:
        return getattr(self, "persistent_forcing_dir", self.forcing_dir)

    def _launcher_entry_ids(self) -> dict[str, str]:
        if self.launcher_entry_id_override:
            return {"experiment": self.launcher_entry_id_override}
        return {
            "canonical": MINDORO_PRIMARY_VALIDATION_LAUNCHER_ENTRY_ID,
            "compatibility_alias": MINDORO_PRIMARY_VALIDATION_LAUNCHER_ALIAS_ENTRY_ID,
        }

    def _sensitivity_context(self, branch: ReinitBranchConfig, recipe_source: str) -> dict[str, Any]:
        return {
            "track": self.track,
            "track_id": self.track_id,
            "track_label": self.track_label,
            "phase_or_track": PHASE_OR_TRACK,
            "branch_id": branch.branch_id,
            "branch_description": branch.description,
            "recipe_source": recipe_source,
            "seed_obs_date": MARCH13_NOAA_SOURCE_DATE,
            "single_date_validation": MARCH14_NOAA_SOURCE_DATE,
            "date_composite_rule": self.window.date_composite_rule,
            "appendix_only": self.appendix_only,
            "reporting_role": self.reporting_role,
            "primary_public_validation": self.primary_public_validation,
            "requested_element_count": int(self.requested_element_count),
            "canonical_requested_element_count_default": int(REQUESTED_ELEMENT_COUNT),
            "thesis_facing": self.thesis_facing,
            "reportable": self.reportable,
            "experimental_only": self.experimental_only,
            "promotion_mode": "reinit_nextday_public_validation",
            "case_freeze_amendment_path": str(MINDORO_PRIMARY_VALIDATION_AMENDMENT_PATH),
            "noaa_source_limitation_note": NOAA_SOURCE_LIMITATION_NOTE,
            "legacy_march6_outputs_preserved": True,
            "canonical_bundle": self.is_canonical_bundle,
        }

    def _existing_path_from_row(self, row: pd.Series, *fields: str) -> Path:
        for field in fields:
            value = str(row.get(field, "") or "").strip()
            if value and value.lower() not in {"nan", "none", "null"}:
                return Path(value)
        source_key = str(row.get("source_key", "") or "").strip()
        if source_key:
            requested_fields = {field.strip().lower() for field in fields}
            if requested_fields.intersection({"processed_vector", "processed_vector_path"}):
                fallback = self.source_extended_dir / "processed_vectors" / f"{source_key}.gpkg"
                if fallback.exists():
                    return fallback
            if requested_fields.intersection({"extended_obs_mask", "extended_obs_mask_path"}):
                fallback = self.source_extended_dir / "accepted_obs_masks" / f"{source_key}.tif"
                if fallback.exists():
                    return fallback
        return Path("")

    def run(self) -> dict:
        start_row, target_row = self._load_reinit_observation_pair()
        self._ensure_scoreable_observation(start_row, role_label="march13_seed")
        self._ensure_scoreable_observation(target_row, role_label="march14_target")
        seed_release = self._prepare_seed_release_artifacts(start_row)
        selection, recipe_source = self._resolve_recipe()
        forcing_paths = self._prepare_extended_forcing(selection.recipe)
        forcing_manifest = self._write_forcing_window_manifest(selection.recipe, forcing_paths)
        failed_forcing = [row for row in forcing_manifest["rows"] if row["status"] != "ready"]
        if failed_forcing:
            note = self._write_forcing_blocked_note(failed_forcing)
            self._verify_locked_outputs_unchanged()
            raise RuntimeError(
                "March 13 -> March 14 reinit forcing coverage is incomplete. "
                f"See {self.output_dir / 'march13_14_reinit_forcing_window_manifest.json'} and {note}."
            )
        self._clear_stale_forcing_blocked_note()

        branch_runs: list[dict] = []
        branch_products: list[dict] = []
        for branch in BRANCHES:
            run_info = self._run_or_reuse_branch(branch, selection, recipe_source, forcing_paths, seed_release)
            self._sync_branch_model_run_manifests(branch, run_info, recipe_source)
            branch_runs.append(run_info)
            branch_products.append(self._build_branch_local_date_products(branch, run_info))

        pairings = self._build_branch_pairings(target_row, branch_products)
        scored_pairings, fss_df, diagnostics_df = self._score_pairings(pairings)
        branch_survival_df = pd.DataFrame(branch_products)
        summary_df = self._summarize(scored_pairings, fss_df, diagnostics_df, branch_survival_df)
        qa_paths = self._write_qa_artifacts(summary_df, seed_release, target_row)
        decision_note = self._write_decision_note(summary_df, start_row, target_row, seed_release)
        artifacts = self._write_outputs(scored_pairings, fss_df, diagnostics_df, summary_df, branch_survival_df)
        manifest = self._write_run_manifest(
            start_row=start_row,
            target_row=target_row,
            seed_release=seed_release,
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
            "start_source_key": start_row["source_key"],
            "target_source_key": target_row["source_key"],
        }

    def _load_reinit_observation_pair(self) -> tuple[pd.Series, pd.Series]:
        registry_path = self.source_extended_dir / "extended_public_obs_acceptance_registry.csv"
        if not registry_path.exists():
            raise FileNotFoundError(
                f"Extended observation registry not found: {registry_path}. Run phase3b_extended_public first."
            )
        registry = pd.read_csv(registry_path)
        return (
            self._select_accepted_observation_row(
                registry,
                source_key=MARCH13_NOAA_SOURCE_KEY,
                obs_date=MARCH13_NOAA_SOURCE_DATE,
                label="March 13 NOAA seed source",
            ),
            self._select_accepted_observation_row(
                registry,
                source_key=MARCH14_NOAA_SOURCE_KEY,
                obs_date=MARCH14_NOAA_SOURCE_DATE,
                label="March 14 NOAA target source",
            ),
        )

    def _select_accepted_observation_row(
        self,
        registry: pd.DataFrame,
        *,
        source_key: str,
        obs_date: str,
        label: str,
    ) -> pd.Series:
        accepted = registry[
            registry["accepted_for_extended_quantitative"].map(_as_bool)
            & registry["mask_exists"].map(_as_bool)
            & (registry["obs_date"].astype(str).str.strip() == obs_date)
            & (registry["source_key"].astype(str).str.strip() == source_key)
        ].copy()
        accepted = accepted[
            accepted.apply(
                lambda row: self._existing_path_from_row(row, "extended_obs_mask").exists()
                and self._existing_path_from_row(row, "processed_vector", "processed_vector_path").exists(),
                axis=1,
            )
        ].copy()
        if accepted.empty:
            raise RuntimeError(
                f"{label} was not found as an accepted processed observation in the extended registry. "
                "Run phase3b_extended_public first."
            )
        if len(accepted) != 1:
            raise RuntimeError(f"Expected exactly one accepted row for {label}, found {len(accepted)}.")
        return accepted.iloc[0]

    def _ensure_scoreable_observation(self, obs_row: pd.Series, *, role_label: str) -> None:
        obs_path = self._existing_path_from_row(obs_row, "extended_obs_mask")
        obs_mask = self.helper._load_binary_score_mask(obs_path)
        obs_nonzero = int(np.count_nonzero(obs_mask > 0))
        if obs_nonzero > 0:
            return
        note = self._write_scoreability_blocked_note(obs_row, role_label, obs_nonzero)
        self._verify_locked_outputs_unchanged()
        raise RuntimeError(f"{role_label} source not scoreable after rasterization. See {note}.")

    def _prepare_seed_release_artifacts(self, start_row: pd.Series) -> dict:
        processed_vector = self._existing_path_from_row(start_row, "processed_vector", "processed_vector_path")
        seed_mask_source = self._existing_path_from_row(start_row, "extended_obs_mask")
        seed_mask = self.helper._load_binary_score_mask(seed_mask_source)
        seed_mask_copy = self.output_dir / "march13_seed_mask_on_grid.tif"
        save_raster(self.grid, seed_mask.astype(np.float32), seed_mask_copy)
        ref_lat, ref_lon = _resolve_polygon_reference_point(processed_vector, geometry_type="polygon")
        return {
            "source_key": str(start_row["source_key"]),
            "source_name": str(start_row["source_name"]),
            "provider": str(start_row.get("provider", "")),
            "obs_date": str(start_row["obs_date"]),
            "processed_vector_path": str(processed_vector),
            "seed_mask_source_path": str(seed_mask_source),
            "seed_mask_path": str(seed_mask_copy),
            "release_start_utc": self.window.simulation_start_utc,
            "reference_lat": float(ref_lat),
            "reference_lon": float(ref_lon),
            "release_geometry_label": START_SOURCE_GEOMETRY_LABEL,
        }

    @staticmethod
    def _resolve_recipe():
        selection = resolve_recipe_selection()
        return selection, "frozen_resolved_baseline_no_completed_event_recipe_sensitivity_found"

    def _required_gfs_time_bounds(self) -> tuple[pd.Timestamp, pd.Timestamp]:
        return (
            _normalize_utc(self.window.required_forcing_start_utc),
            _normalize_utc(self.window.required_forcing_end_utc),
        )

    def _candidate_gfs_cache_paths(self, gfs_path: Path) -> list[Path]:
        candidates = [
            gfs_path,
            Path("data/forcing") / self.case.run_name / gfs_path.name,
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

    def _gfs_cache_ready_record(self, gfs_path: Path) -> dict[str, Any] | None:
        if self._force_refresh_enabled():
            return None
        required_start, required_end = self._required_gfs_time_bounds()
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
                stage_store_file(candidate, gfs_path)
            return {
                "status": "reused_local_file",
                "path": str(gfs_path),
                "source_id": "gfs",
                "forcing_factor": gfs_path.name,
                "upstream_outage_detected": False,
                "source_system": "existing_local_cache",
                "source_tier": "staged",
                "provider": "NOAA GFS archive",
                "source_url": str(candidate),
                "storage_tier": PERSISTENT_LOCAL_INPUT_STORE,
                "local_storage_path": str(gfs_path),
                "reuse_action": "reused_valid_local_store",
                "validation_status": "validated",
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
    ) -> dict[str, Any]:
        cached_record = self._gfs_cache_ready_record(gfs_path)
        if cached_record is not None:
            return cached_record

        required_start, required_end = self._required_gfs_time_bounds()
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
                    scratch_dir=self._persistent_forcing_store_dir(),
                    budget_seconds=budget_seconds,
                )
                or {}
            )
            record.setdefault("status", "downloaded")
            record["source_system"] = "ncei_thredds_archive"
            record["source_tier"] = "primary"
            record["provider"] = "NOAA GFS archive"
            record["source_url"] = "https://www.ncei.noaa.gov/products/weather-climate-models/global-forecast"
        except Exception as primary_exc:
            primary_failure = f"{type(primary_exc).__name__}: {primary_exc}"
            primary_outage = service._is_remote_outage_error(primary_exc)
            try:
                record = dict(
                    service.gfs_downloader.download_secondary_historical(
                        start_time=required_start,
                        end_time=required_end,
                        output_path=gfs_path,
                        scratch_dir=self._persistent_forcing_store_dir(),
                        budget_seconds=budget_seconds,
                    )
                    or {}
                )
                record.setdefault("status", "downloaded")
                record["source_system"] = "ucar_gdex_d084001"
                record["source_tier"] = "secondary"
                record["primary_failure"] = primary_failure
                record["provider"] = "UCAR GDEx"
                record["source_url"] = "https://gdex.ucar.edu/datasets/d084001/"
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
        record["storage_tier"] = PERSISTENT_LOCAL_INPUT_STORE
        record["local_storage_path"] = str(gfs_path)
        record["reuse_action"] = "downloaded_new_file"
        record["validation_status"] = "validated"
        record["requested_start_utc"] = _iso_z(required_start)
        record["requested_end_utc"] = _iso_z(required_end)
        record["primary_failure"] = str(record.get("primary_failure") or primary_failure)

        readiness = self._gfs_cache_ready_record(gfs_path)
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

    def _prepare_extended_forcing(self, recipe_name: str) -> dict:
        with open("config/recipes.yaml", "r", encoding="utf-8") as handle:
            recipes = yaml.safe_load(handle) or {}
        recipe = (recipes.get("recipes") or {}).get(recipe_name)
        if not recipe:
            raise RuntimeError(f"Recipe {recipe_name} is not defined in config/recipes.yaml.")

        service = DataIngestionService()
        service.forcing_dir = self._persistent_forcing_store_dir()
        service.forcing_dir.mkdir(parents=True, exist_ok=True)
        service.configure_explicit_download_window(
            start_date=self.window.download_start_date,
            end_date=self.window.download_end_date,
        )

        downloads: dict[str, Any] = {}
        current_file = str(recipe["currents_file"])
        wind_file = str(recipe["wind_file"])
        wave_file = str(recipe.get("wave_file") or "")
        store_currents_path = service.forcing_dir / current_file
        store_wind_path = service.forcing_dir / wind_file
        store_wave_path = service.forcing_dir / wave_file if wave_file else None
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
            raise RuntimeError(f"March 13 -> March 14 reinit forcing download failed: {downloads['currents']}")

        if wind_file.startswith("gfs"):
            downloads["wind"] = self._download_required_gfs_wind(
                service,
                gfs_path=store_wind_path,
            )
            if downloads["wind"]["status"] not in {"downloaded", "cached", "reused_local_file"}:
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
                raise RuntimeError(f"March 13 -> March 14 reinit forcing download failed: {downloads['wind']}")
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
                raise RuntimeError(f"March 13 -> March 14 reinit forcing download failed: {downloads['wind']}")
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
                raise RuntimeError(f"March 13 -> March 14 reinit forcing download failed: {downloads['wave']}")

        return {
            "recipe": recipe_name,
            "currents": self._stage_prepared_forcing_output(store_currents_path, self.forcing_dir / current_file),
            "wind": self._stage_prepared_forcing_output(store_wind_path, self.forcing_dir / wind_file),
            "wave": self._stage_prepared_forcing_output(store_wave_path, self.forcing_dir / wave_file) if wave_file else None,
            "downloads": downloads,
        }

    def _stage_prepared_forcing_output(self, store_path: Path | None, stage_path: Path) -> Path | None:
        if store_path is None:
            return None
        if store_path.exists():
            stage_store_file(store_path, stage_path)
        return stage_path

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
        manifest_json = self.output_dir / "march13_14_reinit_forcing_window_manifest.json"
        _write_json(manifest_json, payload)
        _write_csv(
            self.output_dir / "march13_14_reinit_forcing_window_manifest.csv",
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
        download_rows = forcing_paths.get("downloads") or {}
        rows = [
            {
                "forcing_kind": "current",
                **_forcing_time_and_vars(Path(forcing_paths["currents"]), ["uo", "vo"], required_start, required_end),
                "provider": str((download_rows.get("currents") or {}).get("provider") or ""),
                "source_url": str((download_rows.get("currents") or {}).get("source_url") or ""),
                "local_storage_path": str((download_rows.get("currents") or {}).get("local_storage_path") or Path(forcing_paths["currents"])),
                "staged_output_path": str(forcing_paths["currents"]),
                "storage_tier": str((download_rows.get("currents") or {}).get("storage_tier") or PERSISTENT_LOCAL_INPUT_STORE),
                "reuse_action": str((download_rows.get("currents") or {}).get("reuse_action") or ""),
                "validation_status": str((download_rows.get("currents") or {}).get("validation_status") or ""),
            },
            {
                "forcing_kind": "wind",
                **_forcing_time_and_vars(Path(forcing_paths["wind"]), ["x_wind", "y_wind"], required_start, required_end),
                "provider": str((download_rows.get("wind") or {}).get("provider") or ""),
                "source_url": str((download_rows.get("wind") or {}).get("source_url") or ""),
                "local_storage_path": str((download_rows.get("wind") or {}).get("local_storage_path") or Path(forcing_paths["wind"])),
                "staged_output_path": str(forcing_paths["wind"]),
                "storage_tier": str((download_rows.get("wind") or {}).get("storage_tier") or PERSISTENT_LOCAL_INPUT_STORE),
                "reuse_action": str((download_rows.get("wind") or {}).get("reuse_action") or ""),
                "validation_status": str((download_rows.get("wind") or {}).get("validation_status") or ""),
            },
        ]
        if forcing_paths.get("wave"):
            rows.append(
                {
                    "forcing_kind": "wave",
                    **_forcing_time_and_vars(Path(forcing_paths["wave"]), ["VHM0", "VSDX", "VSDY"], required_start, required_end),
                    "provider": str((download_rows.get("wave") or {}).get("provider") or ""),
                    "source_url": str((download_rows.get("wave") or {}).get("source_url") or ""),
                    "local_storage_path": str((download_rows.get("wave") or {}).get("local_storage_path") or Path(forcing_paths["wave"])),
                    "staged_output_path": str(forcing_paths["wave"]),
                    "storage_tier": str((download_rows.get("wave") or {}).get("storage_tier") or PERSISTENT_LOCAL_INPUT_STORE),
                    "reuse_action": str((download_rows.get("wave") or {}).get("reuse_action") or ""),
                    "validation_status": str((download_rows.get("wave") or {}).get("validation_status") or ""),
                }
            )
        payload = {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "track": self.track,
            "track_id": self.track_id,
            "track_label": self.track_label,
            "phase_or_track": PHASE_OR_TRACK,
            "recipe": recipe_name,
            "window": asdict(self.window),
            "rows": rows,
            "forcing_outage_policy": self.forcing_outage_policy,
            "degraded_continue_used": False,
            "missing_forcing_factors": [],
            "rerun_required": False,
            "status": "ready" if all(row["status"] == "ready" for row in rows) else "insufficient",
        }
        _write_json(self.output_dir / "march13_14_reinit_forcing_window_manifest.json", payload)
        _write_csv(self.output_dir / "march13_14_reinit_forcing_window_manifest.csv", pd.DataFrame(rows))
        return payload

    def _run_or_reuse_branch(
        self,
        branch: ReinitBranchConfig,
        selection,
        recipe_source: str,
        forcing_paths: dict,
        seed_release: dict,
    ) -> dict:
        model_run_name = f"{self.case.run_name}/{self.output_dir_name}/{branch.output_slug}/model_run"
        model_dir = get_case_output_dir(model_run_name)
        member_paths = sorted((model_dir / "ensemble").glob("member_*.nc"))
        forecast_manifest = model_dir / "forecast" / "forecast_manifest.json"
        can_reuse_members = self._branch_outputs_are_reusable(
            member_paths,
            forecast_manifest,
            expected_recipe=selection.recipe,
            expected_element_count=self.requested_element_count,
        )
        if can_reuse_members and not self.force_rerun:
            manifest_details = self._branch_manifest_details(model_dir)
            return {
                "branch_id": branch.branch_id,
                "branch_description": branch.description,
                "status": "reused_existing_branch_run" if forecast_manifest.exists() else "reused_existing_member_outputs",
                "model_dir": str(model_dir),
                "model_run_name": model_run_name,
                "forecast_result": {
                    "status": "reused_existing_branch_run" if forecast_manifest.exists() else "reused_existing_member_outputs",
                    "member_count": len(member_paths),
                },
                "element_count_requested": manifest_details["element_count_requested"],
                "element_count_actual": manifest_details["element_count_actual"],
                "actual_member_count": manifest_details["actual_member_count"],
                "ensemble_member_count_expected": int(EXPECTED_ENSEMBLE_MEMBER_COUNT),
                "manifest_path": manifest_details["ensemble_manifest_path"],
                "forecast_manifest_path": manifest_details["forecast_manifest_path"],
                "reused_existing_run": True,
            }

        simulation_start = _normalize_utc(self.window.simulation_start_utc)
        simulation_end = _normalize_utc(self.window.simulation_end_utc)
        duration_hours = int(math.ceil((simulation_end - simulation_start).total_seconds() / 3600.0))
        snapshot_hours = sorted(set([24, 48, duration_hours]))
        with _temporary_element_count_override(self.requested_element_count):
            result = run_official_spill_forecast(
                selection=selection,
                start_time=seed_release["release_start_utc"],
                start_lat=float(seed_release["reference_lat"]),
                start_lon=float(seed_release["reference_lon"]),
                output_run_name=model_run_name,
                forcing_override=forcing_paths,
                sensitivity_context=self._sensitivity_context(branch, recipe_source),
                historical_baseline_provenance={
                    "recipe": selection.recipe,
                    "source_kind": selection.source_kind,
                    "source_path": selection.source_path,
                    "note": selection.note,
                },
                simulation_start_utc=self.window.simulation_start_utc,
                simulation_end_utc=self.window.simulation_end_utc,
                snapshot_hours=snapshot_hours,
                date_composite_dates=list(self.window.forecast_local_dates),
                transport_overrides={
                    "coastline_action": branch.coastline_action,
                    "coastline_approximation_precision": branch.coastline_approximation_precision,
                    "time_step_minutes": branch.time_step_minutes,
                },
                seed_overrides={
                    "polygon_vector_path": seed_release["processed_vector_path"],
                    "source_geometry_label": START_SOURCE_GEOMETRY_LABEL,
                },
            )
        if result.get("status") != "success":
            raise RuntimeError(f"March 13 -> March 14 reinit forecast failed for {branch.branch_id}: {result}")
        manifest_details = self._branch_manifest_details(model_dir)
        return {
            "branch_id": branch.branch_id,
            "branch_description": branch.description,
            "status": "completed_new_branch_run",
            "model_dir": str(model_dir),
            "model_run_name": model_run_name,
            "forecast_result": result,
            "element_count_requested": manifest_details["element_count_requested"],
            "element_count_actual": manifest_details["element_count_actual"],
            "actual_member_count": manifest_details["actual_member_count"],
            "ensemble_member_count_expected": int(EXPECTED_ENSEMBLE_MEMBER_COUNT),
            "manifest_path": manifest_details["ensemble_manifest_path"],
            "forecast_manifest_path": manifest_details["forecast_manifest_path"],
            "reused_existing_run": False,
        }

    def _branch_outputs_are_reusable(
        self,
        member_paths: list[Path],
        forecast_manifest: Path,
        *,
        expected_recipe: str | None = None,
        expected_element_count: int | None = None,
    ) -> bool:
        if forecast_manifest.exists():
            payload = _read_json(forecast_manifest)
            if expected_recipe:
                recorded_recipe = str(
                    payload.get("recipe")
                    or (payload.get("selection") or {}).get("recipe")
                    or (payload.get("historical_baseline_provenance") or {}).get("recipe")
                    or ""
                ).strip()
                if recorded_recipe != str(expected_recipe).strip():
                    return False
            if expected_element_count is not None:
                manifest_details = self._branch_manifest_details(forecast_manifest.parent.parent)
                if int(manifest_details["element_count_actual"]) != int(expected_element_count):
                    return False
            return bool(member_paths)
        if expected_recipe:
            return False
        if not member_paths:
            return False
        member_names = {path.stem for path in member_paths}
        return f"member_{EXPECTED_ENSEMBLE_MEMBER_COUNT}" in member_names and len(member_paths) >= EXPECTED_ENSEMBLE_MEMBER_COUNT

    def _branch_manifest_details(self, model_dir: Path) -> dict[str, Any]:
        forecast_manifest_path = model_dir / "forecast" / "forecast_manifest.json"
        ensemble_manifest_path = model_dir / "ensemble" / "ensemble_manifest.json"
        forecast_manifest = _read_json(forecast_manifest_path) if forecast_manifest_path.exists() else {}
        ensemble_manifest = _read_json(ensemble_manifest_path) if ensemble_manifest_path.exists() else {}
        member_runs = list(ensemble_manifest.get("member_runs") or [])
        member_paths = sorted((model_dir / "ensemble").glob("member_*.nc"))
        requested = int(
            (ensemble_manifest.get("ensemble_configuration") or {}).get("element_count")
            or (forecast_manifest.get("ensemble") or {}).get("actual_element_count")
            or self.requested_element_count
        )
        actual = int(
            (forecast_manifest.get("ensemble") or {}).get("actual_element_count")
            or (forecast_manifest.get("deterministic_control") or {}).get("actual_element_count")
            or requested
        )
        actual_member_count = int(
            (forecast_manifest.get("ensemble") or {}).get("actual_member_count")
            or len(member_runs)
            or len(member_paths)
        )
        return {
            "forecast_manifest_path": str(forecast_manifest_path) if forecast_manifest_path.exists() else "",
            "ensemble_manifest_path": str(ensemble_manifest_path) if ensemble_manifest_path.exists() else "",
            "element_count_requested": requested,
            "element_count_actual": actual,
            "actual_member_count": actual_member_count,
        }

    def _sync_branch_model_run_manifests(self, branch: ReinitBranchConfig, run_info: dict, recipe_source: str) -> None:
        model_dir = Path(str(run_info["model_dir"]))
        sensitivity_context = self._sensitivity_context(branch, recipe_source)
        for manifest_path in (
            model_dir / "forecast" / "forecast_manifest.json",
            model_dir / "ensemble" / "ensemble_manifest.json",
        ):
            if not manifest_path.exists():
                continue
            payload = _read_json(manifest_path)
            if not isinstance(payload, dict):
                continue
            payload["sensitivity_context"] = sensitivity_context
            _write_json(manifest_path, payload)

    def _clear_stale_forcing_blocked_note(self) -> None:
        blocked_note = self.output_dir / "march13_14_reinit_forcing_blocked.md"
        if blocked_note.exists():
            blocked_note.unlink()

    def _build_branch_local_date_products(self, branch: ReinitBranchConfig, run_info: dict) -> dict:
        model_dir = Path(str(run_info["model_dir"]))
        composite_dir = self.output_dir / branch.output_slug / "forecast_datecomposites"
        composite_dir.mkdir(parents=True, exist_ok=True)

        per_date_prob_paths = {
            date: composite_dir / f"prob_presence_{date}_localdate.tif" for date in self.window.forecast_local_dates
        }
        per_date_mask_paths = {
            date: composite_dir / f"mask_p50_{date}_localdate.tif" for date in self.window.forecast_local_dates
        }

        member_paths = sorted((model_dir / "ensemble").glob("member_*.nc"))
        per_date_member_masks = {date: [] for date in self.window.forecast_local_dates}
        per_date_active_timestamps = {date: set() for date in self.window.forecast_local_dates}
        last_active_time: pd.Timestamp | None = None
        if member_paths:
            if xr is None:
                raise ImportError("xarray is required to build March 13 -> March 14 local-date composites.")
            for member_path in member_paths:
                composites = {
                    date: np.zeros((self.grid.height, self.grid.width), dtype=np.float32)
                    for date in self.window.forecast_local_dates
                }
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
                        if local_date not in composites:
                            continue
                        per_date_active_timestamps[local_date].add(_iso_z(utc_timestamp))
                        hits, _ = rasterize_particles(
                            self.grid,
                            lon[valid],
                            lat[valid],
                            np.ones(int(np.count_nonzero(valid)), dtype=np.float32),
                        )
                        composites[local_date] = np.maximum(composites[local_date], hits)
                for date in self.window.forecast_local_dates:
                    per_date_member_masks[date].append(
                        apply_ocean_mask(composites[date].astype(np.float32), sea_mask=self.sea_mask, fill_value=0.0)
                    )

        forecast_nonzero_by_date: dict[str, int] = {}
        for date in self.window.forecast_local_dates:
            probability = (
                np.mean(np.stack(per_date_member_masks[date], axis=0), axis=0).astype(np.float32)
                if per_date_member_masks[date]
                else np.zeros((self.grid.height, self.grid.width), dtype=np.float32)
            )
            probability = apply_ocean_mask(probability, sea_mask=self.sea_mask, fill_value=0.0)
            p50 = apply_ocean_mask((probability >= 0.5).astype(np.float32), sea_mask=self.sea_mask, fill_value=0.0)
            save_raster(self.grid, probability.astype(np.float32), per_date_prob_paths[date])
            save_raster(self.grid, p50.astype(np.float32), per_date_mask_paths[date])
            forecast_nonzero_by_date[date] = int(np.count_nonzero(p50 > 0))

        target_date = self.window.scored_target_date
        forecast_result_status = str((run_info.get("forecast_result") or {}).get("status") or run_info.get("status") or "").strip()
        if forecast_nonzero_by_date[target_date] > 0:
            empty_forecast_reason = ""
        elif not member_paths and forecast_result_status:
            empty_forecast_reason = f"forecast_run_status_{forecast_result_status}"
        elif not member_paths:
            empty_forecast_reason = "no_ensemble_member_outputs_found"
        elif not per_date_active_timestamps[target_date]:
            empty_forecast_reason = "model_survival_did_not_reach_march14_local_date"
        else:
            empty_forecast_reason = "march14_local_activity_present_but_no_scoreable_ocean_presence_after_masking"

        return {
            "branch_id": branch.branch_id,
            "branch_output_slug": branch.output_slug,
            "branch_description": branch.description,
            "branch_precedence": branch.branch_precedence,
            "branch_run_status": run_info["status"],
            "forecast_result_status": forecast_result_status,
            "model_dir": str(model_dir),
            "model_run_name": run_info["model_run_name"],
            "probability_path": str(per_date_prob_paths[target_date]),
            "forecast_path": str(per_date_mask_paths[target_date]),
            "march13_probability_path": str(per_date_prob_paths[MARCH13_NOAA_SOURCE_DATE]),
            "march13_forecast_path": str(per_date_mask_paths[MARCH13_NOAA_SOURCE_DATE]),
            "march14_probability_path": str(per_date_prob_paths[MARCH14_NOAA_SOURCE_DATE]),
            "march14_forecast_path": str(per_date_mask_paths[MARCH14_NOAA_SOURCE_DATE]),
            "member_count": int(len(member_paths)),
            "last_active_particle_time_utc": _iso_z(last_active_time) if last_active_time is not None else "",
            "march13_local_active_timestamp_count": int(len(per_date_active_timestamps[MARCH13_NOAA_SOURCE_DATE])),
            "march14_local_active_timestamp_count": int(len(per_date_active_timestamps[MARCH14_NOAA_SOURCE_DATE])),
            "march13_local_active_timestamps": ";".join(sorted(per_date_active_timestamps[MARCH13_NOAA_SOURCE_DATE])),
            "march14_local_active_timestamps": ";".join(sorted(per_date_active_timestamps[MARCH14_NOAA_SOURCE_DATE])),
            "reached_march13_local_date": bool(per_date_active_timestamps[MARCH13_NOAA_SOURCE_DATE]),
            "reached_march14_local_date": bool(per_date_active_timestamps[MARCH14_NOAA_SOURCE_DATE]),
            "empty_forecast_reason": empty_forecast_reason,
            "forecast_nonzero_cells_from_march14_localdate_mask": forecast_nonzero_by_date[target_date],
            "element_count_requested": int(run_info.get("element_count_requested") or self.requested_element_count),
            "element_count_actual": int(run_info.get("element_count_actual") or self.requested_element_count),
        }

    def _build_branch_pairings(self, target_row: pd.Series, branch_products: list[dict]) -> pd.DataFrame:
        obs_path = self._existing_path_from_row(target_row, "extended_obs_mask")
        rows = []
        for product in branch_products:
            forecast_path = Path(str(product["forecast_path"]))
            probability_path = Path(str(product["probability_path"]))
            rows.append(
                {
                    "pair_id": f"march14_reinit_branch_{product['branch_id']}",
                    "pair_role": "march14_nextday_reinit_branch_compare",
                    "score_group": "single_date_branch_compare",
                    "obs_date": MARCH14_NOAA_SOURCE_DATE,
                    "validation_dates_used": MARCH14_NOAA_SOURCE_DATE,
                    "seed_obs_date": MARCH13_NOAA_SOURCE_DATE,
                    "source_key": str(target_row["source_key"]),
                    "source_name": str(target_row["source_name"]),
                    "provider": str(target_row.get("provider", "")),
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
                    "track_label": self.track,
                    "track_id": self.track_id,
                    "track_title": self.track_label,
                    "phase_or_track": PHASE_OR_TRACK,
                    "reporting_role": self.reporting_role,
                    "case_freeze_amendment_path": str(MINDORO_PRIMARY_VALIDATION_AMENDMENT_PATH),
                    "source_semantics": "march13_polygon_reinit_vs_march14_local_date_branch_p50",
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
                raise RuntimeError(
                    f"March 13 -> March 14 reinit same-grid precheck failed for {row['pair_id']}: {precheck.json_report_path}"
                )
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
            "pairing_manifest_csv": self.output_dir / "march13_14_reinit_branch_pairing_manifest.csv",
            "fss_csv": self.output_dir / "march13_14_reinit_fss_by_window.csv",
            "diagnostics_csv": self.output_dir / "march13_14_reinit_diagnostics.csv",
            "summary_csv": self.output_dir / "march13_14_reinit_summary.csv",
            "branch_survival_csv": self.output_dir / "march13_14_reinit_branch_survival_summary.csv",
            "forcing_manifest_json": self.output_dir / "march13_14_reinit_forcing_window_manifest.json",
            "forcing_manifest_csv": self.output_dir / "march13_14_reinit_forcing_window_manifest.csv",
        }
        _write_csv(artifacts["pairing_manifest_csv"], scored_pairings)
        _write_csv(artifacts["fss_csv"], fss_df)
        _write_csv(artifacts["diagnostics_csv"], diagnostics_df)
        _write_csv(artifacts["summary_csv"], summary_df)
        _write_csv(artifacts["branch_survival_csv"], branch_survival_df)
        return artifacts

    def _write_decision_note(
        self,
        summary_df: pd.DataFrame,
        start_row: pd.Series,
        target_row: pd.Series,
        seed_release: dict,
    ) -> Path:
        path = self.output_dir / "march13_14_reinit_decision_note.md"
        both_empty = pd.to_numeric(summary_df.get("forecast_nonzero_cells"), errors="coerce").fillna(0).eq(0).all()
        survival_blocked = both_empty and summary_df["empty_forecast_reason"].astype(str).str.contains(
            "model_survival", case=False, na=False
        ).all()
        best_row = summary_df.sort_values(["mean_fss", "fss_1km", "branch_precedence"], ascending=[False, False, True]).iloc[0]
        lines = [
            "# March 13 -> March 14 NOAA Reinit Primary Validation Decision Note",
            "",
            f"- Canonical Phase 3B primary validation source: {'true' if self.is_canonical_bundle else 'false'}",
            f"- Appendix-only track: {'true' if self.appendix_only else 'false'}",
            f"- Reporting role: {self.reporting_role}",
            f"- Seed source key: {start_row['source_key']}",
            f"- Seed source name: {start_row['source_name']}",
            f"- Target source key: {target_row['source_key']}",
            f"- Target source name: {target_row['source_name']}",
            f"- Seed observation date: {MARCH13_NOAA_SOURCE_DATE}",
            f"- Scored target date: {MARCH14_NOAA_SOURCE_DATE}",
            f"- Release start UTC: {seed_release['release_start_utc']}",
            f"- Seed release geometry: {seed_release['release_geometry_label']}",
            f"- Requested element count: {self.requested_element_count}",
            f"- Best branch by mean FSS: {best_row['branch_id']}",
            f"- Best branch mean FSS: {float(best_row['mean_fss']):.6f}",
            (
                f"- Best branch FSS 1/3/5/10 km: {float(best_row['fss_1km']):.6f} / "
                f"{float(best_row['fss_3km']):.6f} / {float(best_row['fss_5km']):.6f} / {float(best_row['fss_10km']):.6f}"
            ),
            f"- Limitation note: {NOAA_SOURCE_LIMITATION_NOTE}",
        ]
        if survival_blocked:
            lines.append("- Decision: March 14 comparison is blocked by model survival, not by missing public data.")
        elif both_empty:
            lines.append("- Decision: Both branches produced empty March 14 p50 masks; inspect empty_forecast_reason before interpreting skill.")
        elif self.is_canonical_bundle:
            lines.append(
                "- Decision: At least one branch produced a scoreable March 14 p50 mask, so this bundle is usable as the canonical "
                "Phase 3B public-validation source row with independent NOAA-published day-specific observation products."
            )
        else:
            lines.append(
                "- Decision: At least one branch produced a scoreable March 14 p50 mask, so this experimental bundle is usable as a "
                "trial rerun of the March 13 -> March 14 row without replacing the canonical stored B1 outputs."
            )
        lines.extend(
            [
                "",
                (
                    "This bundle is the canonical Phase 3B public-validation source for packaging and figure builders."
                    if self.is_canonical_bundle
                    else "This bundle is experimental and non-canonical. It does not overwrite the stored March 13 -> March 14 primary-validation bundle."
                ),
                "It does not rewrite the frozen March 3 -> March 6 official case definition, and it does not delete the March 6 legacy honesty outputs.",
                "The comparison is intentionally limited to R0 and R1_previous, with March 13 polygon reinitialization and March 14 scoring.",
            ]
        )
        _write_text(path, "\n".join(lines) + "\n")
        return path

    def _write_scoreability_blocked_note(self, obs_row: pd.Series, role_label: str, obs_nonzero_cells: int) -> Path:
        path = self.output_dir / f"{role_label}_source_not_scoreable_after_rasterization.md"
        text = "\n".join(
            [
                f"# {role_label} Source Not Scoreable After Rasterization",
                "",
                f"- Role label: {role_label}",
                f"- Source key: {obs_row['source_key']}",
                f"- Source name: {obs_row['source_name']}",
                f"- Observation date: {obs_row['obs_date']}",
                f"- Raster nonzero cells after ocean masking: {obs_nonzero_cells}",
                "- Status: blocked before any forecast rerun",
                "",
                "The accepted public polygon rasterized to zero ocean cells on the canonical scoring grid.",
                (
                    "This primary public-validation source bundle stops here instead of fabricating a forecast comparison against a non-scoreable target."
                    if self.is_canonical_bundle
                    else "This experimental reinit bundle stops here instead of fabricating a forecast comparison against a non-scoreable target."
                ),
            ]
        )
        _write_text(path, text + "\n")
        return path

    def _write_forcing_blocked_note(self, failed_rows: list[dict]) -> Path:
        path = self.output_dir / "march13_14_reinit_forcing_blocked.md"
        lines = [
            "# March 13 -> March 14 Reinit Forcing Coverage Blocked",
            "",
            (
                "The canonical Phase 3B public-validation rerun did not rerun the forecast branches because the prepared forcing window was incomplete."
                if self.is_canonical_bundle
                else "The experimental March 13 -> March 14 reinit rerun did not rerun the forecast branches because the prepared forcing window was incomplete."
            ),
            "",
            "## Blocking Rows",
            "",
        ]
        lines.extend(f"- {row.get('forcing_kind', 'forcing')}: {row.get('stop_reason', '')}" for row in failed_rows)
        _write_text(path, "\n".join(lines) + "\n")
        return path

    def _write_qa_artifacts(self, summary_df: pd.DataFrame, seed_release: dict, target_row: pd.Series) -> dict[str, Path]:
        outputs: dict[str, Path] = {}
        if plt is None:
            return outputs

        seed_mask = self.helper._load_binary_score_mask(Path(str(seed_release["seed_mask_path"])))
        target_mask = self.helper._load_binary_score_mask(self._existing_path_from_row(target_row, "extended_obs_mask"))

        fig, ax = plt.subplots(figsize=(7, 7))
        canvas = np.ones((seed_mask.shape[0], seed_mask.shape[1], 3), dtype=np.float32)
        canvas[seed_mask > 0] = np.array([0.15, 0.45, 0.95], dtype=np.float32)
        ax.imshow(canvas, origin="upper")
        ax.set_title("March 13 NOAA Seed Polygon on Scoring Grid")
        ax.set_axis_off()
        seed_png = self.output_dir / "qa_march13_seed_mask_on_grid.png"
        fig.tight_layout()
        fig.savefig(seed_png, dpi=150, bbox_inches="tight")
        plt.close(fig)
        outputs["qa_march13_seed_mask_on_grid"] = seed_png

        fig, ax = plt.subplots(figsize=(7, 7))
        self._render_overlay(ax, seed_mask, target_mask, "March 13 Seed vs March 14 Target")
        start_target_png = self.output_dir / "qa_march13_seed_vs_march14_target.png"
        fig.tight_layout()
        fig.savefig(start_target_png, dpi=150, bbox_inches="tight")
        plt.close(fig)
        outputs["qa_march13_seed_vs_march14_target"] = start_target_png

        for _, row in summary_df.iterrows():
            forecast = self.helper._load_binary_score_mask(Path(str(row["forecast_path"])))
            obs = self.helper._load_binary_score_mask(Path(str(row["observation_path"])))
            fig, ax = plt.subplots(figsize=(7, 7))
            self._render_overlay(ax, forecast, obs, f"March 14 Reinit {row['branch_id']}")
            fig.tight_layout()
            path = self.output_dir / f"qa_march14_reinit_{row['branch_id']}_overlay.png"
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
        start_row: pd.Series,
        target_row: pd.Series,
        seed_release: dict,
        summary_df: pd.DataFrame,
        branch_runs: list[dict],
        forcing_manifest: dict,
        artifacts: dict[str, Path],
        qa_paths: dict[str, Path],
        decision_note: Path,
        selection,
        recipe_source: str,
    ) -> Path:
        path = self.output_dir / "march13_14_reinit_run_manifest.json"
        branch_requested_counts = {
            str(run.get("branch_id") or ""): int(run.get("element_count_requested") or self.requested_element_count)
            for run in branch_runs
            if str(run.get("branch_id") or "").strip()
        }
        branch_actual_counts = {
            str(run.get("branch_id") or ""): int(run.get("element_count_actual") or self.requested_element_count)
            for run in branch_runs
            if str(run.get("branch_id") or "").strip()
        }
        branch_member_counts = {
            str(run.get("branch_id") or ""): int(run.get("actual_member_count") or 0)
            for run in branch_runs
            if str(run.get("branch_id") or "").strip()
        }
        payload = {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "track": self.track,
            "track_id": self.track_id,
            "track_label": self.track_label,
            "phase_or_track": PHASE_OR_TRACK,
            "appendix_only": self.appendix_only,
            "reporting_role": self.reporting_role,
            "primary_public_validation": self.primary_public_validation,
            "thesis_facing": self.thesis_facing,
            "reportable": self.reportable,
            "experimental_only": self.experimental_only,
            "artifact_class": "experimental_only" if self.experimental_only else "canonical_public_validation_source",
            "requested_element_count": int(self.requested_element_count),
            "canonical_requested_element_count_default": int(REQUESTED_ELEMENT_COUNT),
            "expected_ensemble_member_count": int(EXPECTED_ENSEMBLE_MEMBER_COUNT),
            "actual_member_count_detected": _consensus_int(branch_member_counts),
            "actual_member_count_detected_by_branch": branch_member_counts,
            "element_count_detected_from_manifest": _consensus_int(branch_actual_counts),
            "element_count_detected_from_manifest_by_branch": branch_actual_counts,
            "requested_element_count_by_branch": branch_requested_counts,
            "independent_noaa_observation_products_confirmed": True,
            "workflow_mode": self.case.workflow_mode,
            "run_name": self.case.run_name,
            "launcher_entry_ids": self._launcher_entry_ids(),
            "case_definition": {
                "base_case_definition_path": str(MINDORO_BASE_CASE_CONFIG_PATH),
                "case_freeze_amendment_path": str(MINDORO_PRIMARY_VALIDATION_AMENDMENT_PATH),
                "base_case_definition_preserved": True,
                "base_case_definition_window": "2023-03-03_to_2023-03-06",
                "promoted_primary_validation_window": "2023-03-13_to_2023-03-14",
            },
            "window": asdict(self.window),
            "selected_start_source": {
                "source_key": str(start_row["source_key"]),
                "source_name": str(start_row["source_name"]),
                "provider": str(start_row.get("provider", "")),
                "obs_date": str(start_row["obs_date"]),
                "processed_vector": str(self._existing_path_from_row(start_row, "processed_vector", "processed_vector_path")),
                "extended_obs_mask": str(self._existing_path_from_row(start_row, "extended_obs_mask")),
            },
            "selected_target_source": {
                "source_key": str(target_row["source_key"]),
                "source_name": str(target_row["source_name"]),
                "provider": str(target_row.get("provider", "")),
                "obs_date": str(target_row["obs_date"]),
                "processed_vector": str(self._existing_path_from_row(target_row, "processed_vector", "processed_vector_path")),
                "extended_obs_mask": str(self._existing_path_from_row(target_row, "extended_obs_mask")),
            },
            "seed_release": seed_release,
            "limitations": {
                "appendix_only": self.appendix_only,
                "noaa_source_limitation_note": NOAA_SOURCE_LIMITATION_NOTE,
                "independent_day_specific_public_observations": True,
                "legacy_march6_outputs_preserved": True,
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
            "canonical_bundle": self.is_canonical_bundle,
            "locked_output_hashes_before": self.locked_hashes_before,
            "locked_output_hashes_after": self._snapshot_locked_outputs(),
            "artifacts": {
                **{key: str(value) for key, value in artifacts.items()},
                "decision_note_md": str(decision_note),
                "seed_mask_tif": str(seed_release["seed_mask_path"]),
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
            raise RuntimeError("March 13 -> March 14 reinit bundle modified locked strict/public-main outputs.")


def run_phase3b_extended_public_scored_march13_14_reinit() -> dict:
    return Phase3BExtendedPublicScoredMarch1314ReinitService().run()


if __name__ == "__main__":  # pragma: no cover
    result = run_phase3b_extended_public_scored_march13_14_reinit()
    print(json.dumps(result, indent=2, default=_json_default))

