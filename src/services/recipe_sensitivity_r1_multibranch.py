"""R1 forcing-recipe sensitivity across Mindoro initialization branches."""

from __future__ import annotations

import json
import math
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Any

import numpy as np
import pandas as pd
import yaml

from src.core.case_context import get_case_context
from src.helpers.metrics import calculate_fss
from src.helpers.raster import GridBuilder, rasterize_particles, save_raster
from src.helpers.scoring import apply_ocean_mask, load_sea_mask_array, precheck_same_grid
from src.services.ensemble import normalize_time_index, run_official_spill_forecast
from src.services.official_rerun_r1 import load_official_retention_config
from src.services.phase3b_multidate_public import (
    format_phase3b_multidate_eventcorridor_label,
    load_phase3b_multidate_validation_dates,
)
from src.services.pygnome_public_comparison import PYGNOME_PUBLIC_COMPARISON_DIR_NAME
from src.services.scoring import OFFICIAL_PHASE3B_WINDOWS_KM, Phase3BScoringService
from src.utils.gfs_wind import GFSWindDownloader
from src.utils.io import RecipeSelection, get_case_output_dir, resolve_recipe_selection, resolve_spill_origin

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


RECIPE_SENSITIVITY_R1_MULTIBRANCH_DIR_NAME = "recipe_sensitivity_r1_multibranch"
FORCE_RERUN_ENV = "RECIPE_SENSITIVITY_R1_MULTIBRANCH_FORCE_RERUN"
VALIDATION_DATES = ["2023-03-04", "2023-03-05", "2023-03-06"]
STRICT_VALIDATION_DATE = "2023-03-06"
EVENT_CORRIDOR_LABEL = "2023-03-04_to_2023-03-06"


@dataclass(frozen=True)
class RecipeSpec:
    recipe_id: str
    current_source: str
    wind_source: str
    wave_source: str
    currents_file: str
    wind_file: str
    wave_file: str
    optional_if_missing: bool = False


@dataclass(frozen=True)
class BranchSpec:
    branch_id: str
    initialization_mode: str
    seed_overrides: dict[str, Any]
    source_geometry_label: str


RECIPE_MATRIX = [
    RecipeSpec("cmems_era5", "CMEMS", "ERA5", "CMEMS wave/Stokes", "cmems_curr.nc", "era5_wind.nc", "cmems_wave.nc"),
    RecipeSpec("cmems_gfs", "CMEMS", "GFS", "CMEMS wave/Stokes", "cmems_curr.nc", "gfs_wind.nc", "cmems_wave.nc"),
    RecipeSpec("hycom_era5", "HYCOM", "ERA5", "CMEMS wave/Stokes", "hycom_curr.nc", "era5_wind.nc", "cmems_wave.nc"),
    RecipeSpec("hycom_gfs", "HYCOM", "GFS", "CMEMS wave/Stokes", "hycom_curr.nc", "gfs_wind.nc", "cmems_wave.nc", True),
]


BRANCHES = [
    BranchSpec(
        "B",
        "observation_initialized_polygon",
        {},
        "processed_march3_initialization_polygon",
    ),
    BranchSpec(
        "A1",
        "source_point_initialized_same_start",
        {
            "initialization_mode": "source_point_initialized_same_start",
            "point_release_surrogate": "exact_point_release",
        },
        "processed_source_point_exact",
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
        return value.isoformat()
    if isinstance(value, np.ndarray):
        return value.tolist()
    return str(value)


def _write_json(path: Path, payload: dict | list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, default=_json_default)
        handle.write("\n")


def _write_csv(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False)


def _read_json(path: Path) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"Required JSON artifact not found: {path}")
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle) or {}


def _read_raster(path: Path) -> np.ndarray:
    if rasterio is None:
        raise ImportError("rasterio is required for recipe_sensitivity_r1_multibranch.")
    with rasterio.open(path) as src:
        return src.read(1).astype(np.float32)


def _normalize_utc(value: Any) -> pd.Timestamp:
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is not None:
        return timestamp.tz_convert("UTC").tz_localize(None)
    return timestamp


def _mean_fss(row: pd.Series | dict) -> float:
    values = []
    for window in OFFICIAL_PHASE3B_WINDOWS_KM:
        try:
            value = float(row.get(f"fss_{window}km", np.nan))
        except (TypeError, ValueError):
            value = np.nan
        if np.isfinite(value):
            values.append(value)
    return float(np.mean(values)) if values else 0.0


def _truthy(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _csv_block(frame: pd.DataFrame) -> str:
    if frame.empty:
        return "_No rows._"
    return "```csv\n" + frame.to_csv(index=False).strip() + "\n```"


def _rank_rows(rows: pd.DataFrame) -> pd.DataFrame:
    if rows.empty:
        return rows
    ranked = rows.copy()
    if "mean_fss" not in ranked.columns:
        ranked["mean_fss"] = ranked.apply(_mean_fss, axis=1)
    for column in ("iou", "dice", "forecast_nonzero_cells", "nearest_distance_to_obs_m"):
        ranked[column] = pd.to_numeric(ranked.get(column, np.nan), errors="coerce")
    return ranked.sort_values(
        ["mean_fss", "iou", "dice", "forecast_nonzero_cells", "nearest_distance_to_obs_m"],
        ascending=[False, False, False, False, True],
        na_position="last",
    ).reset_index(drop=True)


def select_promotable_opendrift_recipe(summary_df: pd.DataFrame) -> dict:
    """Choose the spill-case forcing recipe candidate from OpenDrift branch B only."""
    if summary_df.empty:
        return {
            "track_id": "",
            "recipe_id": "",
            "branch_id": "",
            "product_kind": "",
            "mean_fss": np.nan,
            "iou": np.nan,
            "dice": np.nan,
            "forecast_nonzero_cells": np.nan,
            "nearest_distance_to_obs_m": np.nan,
            "selection_basis": (
                "No event-corridor rows were available, so no OpenDrift branch-B promotion candidate could be selected."
            ),
        }

    candidates = summary_df[
        (summary_df["pair_role"] == "eventcorridor_march4_6")
        & (summary_df["model_family"] == "OpenDrift")
        & (summary_df["branch_id"] == "B")
    ].copy()
    if candidates.empty:
        return {
            "track_id": "",
            "recipe_id": "",
            "branch_id": "",
            "product_kind": "",
            "mean_fss": np.nan,
            "iou": np.nan,
            "dice": np.nan,
            "forecast_nonzero_cells": np.nan,
            "nearest_distance_to_obs_m": np.nan,
            "selection_basis": (
                "PyGNOME and non-branch-B OpenDrift rows were excluded, leaving no promotable OpenDrift branch-B candidate."
            ),
        }

    winner = _rank_rows(candidates).iloc[0]
    return {
        "track_id": str(winner.get("track_id", "")),
        "recipe_id": str(winner.get("recipe_id", "")),
        "branch_id": str(winner.get("branch_id", "")),
        "product_kind": str(winner.get("product_kind", "")),
        "mean_fss": _mean_fss(winner),
        "iou": winner.get("iou", np.nan),
        "dice": winner.get("dice", np.nan),
        "forecast_nonzero_cells": winner.get("forecast_nonzero_cells", np.nan),
        "nearest_distance_to_obs_m": winner.get("nearest_distance_to_obs_m", np.nan),
        "selection_basis": (
            "Selected from OpenDrift branch-B March 4-6 event-corridor rows only; "
            "PyGNOME and branch A1 were excluded from spill-case forcing promotion."
        ),
    }


def recommend_recipe_branch(summary_df: pd.DataFrame) -> dict:
    """Choose exactly one recommendation for the phase."""
    promotion_candidate = select_promotable_opendrift_recipe(summary_df)
    if summary_df.empty:
        return {
            "recommendation": "conclude that recipe choice is not enough to beat PyGNOME",
            "recommended_next_branch": "final Phase 3B reframing/package",
            "best_strict_track_id": "",
            "best_eventcorridor_track_id": "",
            "any_opendrift_branch_beats_pygnome": False,
            "promotable_track_id": promotion_candidate["track_id"],
            "promotable_recipe_id": promotion_candidate["recipe_id"],
            "promotable_branch_id": promotion_candidate["branch_id"],
            "promotable_product_kind": promotion_candidate["product_kind"],
            "promotable_mean_fss": promotion_candidate["mean_fss"],
            "promotion_selection_basis": promotion_candidate["selection_basis"],
            "reason": "No scored rows were available.",
        }

    working = summary_df.copy()
    working["mean_fss"] = working.apply(_mean_fss, axis=1)
    strict = _rank_rows(working[working["pair_role"] == "strict_march6"])
    event = _rank_rows(working[working["pair_role"] == "eventcorridor_march4_6"])
    best_strict = strict.iloc[0].to_dict() if not strict.empty else {}
    best_event = event.iloc[0].to_dict() if not event.empty else {}
    py_event = event[event["model_family"] == "PyGNOME"]
    od_event = event[event["model_family"] == "OpenDrift"]
    py_best = float(py_event["mean_fss"].max()) if not py_event.empty else np.nan
    od_best = float(od_event["mean_fss"].max()) if not od_event.empty else np.nan
    od_beats = bool(np.isfinite(od_best) and (not np.isfinite(py_best) or od_best > py_best))

    if od_beats and best_event.get("model_family") == "OpenDrift":
        recommendation = "promote one OpenDrift recipe/branch as the best public-validation candidate"
        reason = "An OpenDrift R1 recipe/branch beats the fixed PyGNOME comparator on the March 4-6 event corridor."
    else:
        recommendation = "conclude that recipe choice is not enough to beat PyGNOME"
        reason = "The fixed PyGNOME comparator remains ahead of the tested OpenDrift R1 recipe/branch matrix."

    return {
        "recommendation": recommendation,
        "recommended_next_branch": "final Phase 3B reframing/package",
        "best_strict_track_id": str(best_strict.get("track_id", "")),
        "best_strict_recipe_id": str(best_strict.get("recipe_id", "")),
        "best_strict_branch_id": str(best_strict.get("branch_id", "")),
        "best_eventcorridor_track_id": str(best_event.get("track_id", "")),
        "best_eventcorridor_recipe_id": str(best_event.get("recipe_id", "")),
        "best_eventcorridor_branch_id": str(best_event.get("branch_id", "")),
        "pygnome_eventcorridor_mean_fss": None if not np.isfinite(py_best) else py_best,
        "best_opendrift_eventcorridor_mean_fss": None if not np.isfinite(od_best) else od_best,
        "any_opendrift_branch_beats_pygnome": od_beats,
        "promotable_track_id": promotion_candidate["track_id"],
        "promotable_recipe_id": promotion_candidate["recipe_id"],
        "promotable_branch_id": promotion_candidate["branch_id"],
        "promotable_product_kind": promotion_candidate["product_kind"],
        "promotable_mean_fss": promotion_candidate["mean_fss"],
        "promotion_selection_basis": promotion_candidate["selection_basis"],
        "reason": reason,
    }


class RecipeSensitivityR1MultibranchService:
    def __init__(
        self,
        *,
        output_slug: str = RECIPE_SENSITIVITY_R1_MULTIBRANCH_DIR_NAME,
        forcing_dir: str | Path | None = None,
        prepare_missing_gfs: bool = False,
        gfs_prepare_strict: bool = False,
        force_rerun: bool | None = None,
    ):
        self.case = get_case_context()
        if not self.case.is_official:
            raise RuntimeError("recipe_sensitivity_r1_multibranch is only supported for official Mindoro workflows.")
        if xr is None:
            raise ImportError("xarray is required for recipe_sensitivity_r1_multibranch.")
        if rasterio is None:
            raise ImportError("rasterio is required for recipe_sensitivity_r1_multibranch.")

        self.case_output = get_case_output_dir(self.case.run_name)
        self.output_slug = str(PurePosixPath(output_slug))
        self.output_dir = self.case_output / Path(self.output_slug)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.products_dir = self.output_dir / "products"
        self.obs_dir = self.output_dir / "observations"
        self.precheck_dir = self.output_dir / "precheck"
        self.qa_dir = self.output_dir / "qa"
        for path in (self.products_dir, self.obs_dir, self.precheck_dir, self.qa_dir):
            path.mkdir(parents=True, exist_ok=True)

        self.force_rerun = _truthy(os.environ.get(FORCE_RERUN_ENV, "")) if force_rerun is None else bool(force_rerun)
        self.validation_dates = load_phase3b_multidate_validation_dates(
            self.case_output,
            fallback_dates=VALIDATION_DATES,
        )
        self.eventcorridor_label = format_phase3b_multidate_eventcorridor_label(self.validation_dates)
        self.grid = GridBuilder()
        self.sea_mask = load_sea_mask_array(self.grid.spec)
        self.valid_mask = self.sea_mask > 0.5 if self.sea_mask is not None else None
        self.helper = Phase3BScoringService(output_dir=self.output_dir / "_scratch_helper")
        self.retention_config = load_official_retention_config()
        if self.retention_config["selected_mode"] != "R1":
            raise RuntimeError("recipe_sensitivity_r1_multibranch requires official_retention.selected_mode=R1.")
        if self.retention_config["coastline_action"] != "previous":
            raise RuntimeError("recipe_sensitivity_r1_multibranch requires R1 coastline_action=previous.")

        self.frozen_selection = resolve_recipe_selection()
        self.threshold_context = self._load_selected_threshold_context()
        self.forcing_dir = Path(forcing_dir) if forcing_dir is not None else Path("data") / "forcing" / self.case.run_name
        self.prepare_missing_gfs = bool(prepare_missing_gfs)
        self.gfs_prepare_strict = bool(gfs_prepare_strict)
        self.forcing_preparation = {
            "gfs_requested": self.prepare_missing_gfs,
            "gfs_status": "not_requested",
            "gfs_path": str(self.forcing_dir / "gfs_wind.nc"),
            "gfs_error": "",
        }

    def run(self) -> dict:
        observations = self._prepare_observations()
        self.forcing_preparation = self._prepare_missing_gfs_wind()
        recipes = self._evaluate_recipe_matrix()
        tracks: list[dict] = []
        run_records: list[dict] = []

        for recipe in recipes:
            if not recipe["available"]:
                run_records.append({**recipe, "status": "skipped_missing_inputs"})
                continue
            for branch in BRANCHES:
                run_record = self._run_or_reuse_model(recipe, branch)
                run_records.append(run_record)
                if run_record.get("status") not in {"success", "reused_existing_model"}:
                    continue
                tracks.extend(self._prepare_opendrift_tracks(recipe, branch, Path(run_record["model_dir"]), run_record))

        pygnome_track = self._prepare_pygnome_comparator_track()
        if pygnome_track:
            tracks.append(pygnome_track)
        if not tracks:
            raise RuntimeError("No scoreable recipe_sensitivity_r1_multibranch tracks were available.")

        pairing_df = self._build_pairings(tracks, observations)
        scored_pairings, fss_df, diagnostics_df = self._score_pairings(pairing_df)
        summary_df = self._summarize(scored_pairings, fss_df, diagnostics_df)
        ranking_df = self._build_ranking(summary_df)
        recommendation = recommend_recipe_branch(summary_df)
        qa_paths = self._write_qa(summary_df)
        paths = self._write_outputs(scored_pairings, fss_df, diagnostics_df, summary_df, ranking_df)
        report_path = self._write_report(
            summary_df,
            ranking_df,
            recommendation,
            recipes,
            run_records,
            paths,
            qa_paths,
            self.forcing_preparation,
        )
        manifest_path = self._write_manifest(
            recipes=recipes,
            run_records=run_records,
            tracks=tracks,
            observations=observations,
            recommendation=recommendation,
            paths=paths,
            qa_paths=qa_paths,
            report_path=report_path,
            forcing_preparation=self.forcing_preparation,
        )
        return {
            "output_dir": self.output_dir,
            "summary": summary_df,
            "ranking": ranking_df,
            "summary_csv": paths["summary"],
            "ranking_csv": paths["ranking"],
            "diagnostics_csv": paths["diagnostics"],
            "pairing_manifest_csv": paths["pairing"],
            "fss_by_window_csv": paths["fss_by_window"],
            "run_manifest": manifest_path,
            "report_md": report_path,
            "recommendation": recommendation,
            "recipes": recipes,
            "run_records": run_records,
            "forcing_preparation": self.forcing_preparation,
        }

    def _load_selected_threshold_context(self) -> dict:
        manifest_path = self.case_output / "ensemble_threshold_sensitivity" / "ensemble_threshold_calibration_manifest.json"
        if not manifest_path.exists():
            return {
                "manifest_path": "",
                "selected_threshold": 0.5,
                "selected_threshold_label": "p50",
                "include_lower_selected_threshold": False,
            }
        manifest = _read_json(manifest_path)
        selected = manifest.get("selected_threshold") or {}
        threshold = float(selected.get("threshold", 0.5))
        label = str(selected.get("threshold_label", f"p{int(round(threshold * 100)):02d}"))
        return {
            "manifest_path": str(manifest_path),
            "selected_threshold": threshold,
            "selected_threshold_label": label,
            "include_lower_selected_threshold": bool(threshold < 0.5),
        }

    def _evaluate_recipe_matrix(self) -> list[dict]:
        rows = []
        for spec in RECIPE_MATRIX:
            paths = {
                "currents": self.forcing_dir / spec.currents_file,
                "wind": self.forcing_dir / spec.wind_file,
                "wave": self.forcing_dir / spec.wave_file,
            }
            missing = [str(path) for path in paths.values() if not path.exists()]
            rows.append(
                {
                    "recipe_id": spec.recipe_id,
                    "current_source": spec.current_source,
                    "wind_source": spec.wind_source,
                    "wave_source": spec.wave_source,
                    "currents_path": str(paths["currents"]),
                    "wind_path": str(paths["wind"]),
                    "wave_path": str(paths["wave"]),
                    "available": not missing,
                    "missing_inputs": missing,
                    "optional_if_missing": spec.optional_if_missing,
                }
            )
        return rows

    def _load_case_config(self) -> dict[str, Any]:
        case_definition_path = getattr(self.case, "case_definition_path", None)
        if not case_definition_path:
            return {}
        path = Path(case_definition_path)
        if not path.exists():
            return {}
        with open(path, "r", encoding="utf-8") as handle:
            return yaml.safe_load(handle) or {}

    def _official_forcing_bbox(self) -> list[float]:
        case_config = self._load_case_config()
        halo = float(case_config.get("forcing_bbox_halo_degrees", 0.5))
        display_bounds = None
        try:
            from src.helpers.scoring import get_scoring_grid_spec

            spec = get_scoring_grid_spec()
            display_bounds = list(spec.display_bounds_wgs84 or [])
        except Exception:
            display_bounds = None

        if not display_bounds:
            return [float(value) for value in self.case.region]

        min_lon, max_lon, min_lat, max_lat = [float(value) for value in display_bounds]
        return [
            min_lon - halo,
            max_lon + halo,
            min_lat - halo,
            max_lat + halo,
        ]

    def _prepare_missing_gfs_wind(self) -> dict[str, Any]:
        gfs_path = self.forcing_dir / "gfs_wind.nc"
        status = {
            "gfs_requested": self.prepare_missing_gfs,
            "gfs_status": "not_requested",
            "gfs_path": str(gfs_path),
            "gfs_error": "",
        }
        if gfs_path.exists():
            status["gfs_status"] = "already_present"
            return status
        if not self.prepare_missing_gfs:
            return status

        self.forcing_dir.mkdir(parents=True, exist_ok=True)
        downloader = GFSWindDownloader(
            forcing_box=self._official_forcing_bbox(),
            expected_delta=pd.Timedelta(hours=6),
        )
        try:
            download_status = downloader.download(
                start_time=self.case.forcing_start_utc,
                end_time=self.case.forcing_end_utc,
                output_path=gfs_path,
                scratch_dir=self.forcing_dir,
            )
        except Exception as exc:
            status["gfs_status"] = "failed"
            status["gfs_error"] = f"{type(exc).__name__}: {exc}"
            if self.gfs_prepare_strict:
                raise RuntimeError(
                    "Failed to prepare required GFS wind forcing for recipe_sensitivity_r1_multibranch: "
                    f"{type(exc).__name__}: {exc}"
                ) from exc
            return status

        status["gfs_status"] = str(download_status.get("status") or "downloaded")
        status["gfs_path"] = str(gfs_path)
        return status

    def _prepare_observations(self) -> dict[str, Any]:
        strict = Path("data") / "arcgis" / self.case.run_name / "obs_mask_2023-03-06.tif"
        if not strict.exists():
            raise FileNotFoundError(f"Strict March 6 observed mask is missing: {strict}")
        date_union_dir = self.case_output / "phase3b_multidate_public" / "date_union_obs_masks"
        date_unions: dict[str, Path] = {}
        for date in self.validation_dates:
            path = date_union_dir / f"obs_union_{date}.tif"
            if not path.exists():
                raise FileNotFoundError(f"Accepted date-union observation mask missing: {path}")
            date_unions[date] = path
        event_obs = self._build_eventcorridor_obs_union(date_unions)
        return {"strict_march6": strict, "date_unions": date_unions, "eventcorridor_march4_6": event_obs}

    def _build_eventcorridor_obs_union(self, date_unions: dict[str, Path]) -> Path:
        union = np.zeros((self.grid.height, self.grid.width), dtype=np.float32)
        for date in self.validation_dates:
            union = np.maximum(union, self.helper._load_binary_score_mask(date_unions[date]))
        union = apply_ocean_mask(union, sea_mask=self.sea_mask, fill_value=0.0)
        path = self.obs_dir / f"eventcorridor_obs_union_{self.eventcorridor_label}.tif"
        save_raster(self.grid, union.astype(np.float32), path)
        return path

    def _run_or_reuse_model(self, recipe: dict, branch: BranchSpec) -> dict:
        model_run_name = (
            f"{self.case.run_name}/{self.output_slug}/"
            f"{recipe['recipe_id']}/{branch.branch_id}/model_run"
        )
        model_dir = get_case_output_dir(model_run_name)
        if self._model_dir_complete(model_dir) and not self.force_rerun:
            return {
                **recipe,
                "branch_id": branch.branch_id,
                "initialization_mode": branch.initialization_mode,
                "status": "reused_existing_model",
                "run_name": model_run_name,
                "model_dir": str(model_dir),
                "reused_from_output_slug": self.output_slug,
            }
        if not self.force_rerun and self.output_slug != RECIPE_SENSITIVITY_R1_MULTIBRANCH_DIR_NAME:
            canonical_run_name = (
                f"{self.case.run_name}/{RECIPE_SENSITIVITY_R1_MULTIBRANCH_DIR_NAME}/"
                f"{recipe['recipe_id']}/{branch.branch_id}/model_run"
            )
            canonical_dir = get_case_output_dir(canonical_run_name)
            if self._model_dir_complete(canonical_dir):
                return {
                    **recipe,
                    "branch_id": branch.branch_id,
                    "initialization_mode": branch.initialization_mode,
                    "status": "reused_existing_model",
                    "run_name": canonical_run_name,
                    "model_dir": str(canonical_dir),
                    "reused_from_output_slug": RECIPE_SENSITIVITY_R1_MULTIBRANCH_DIR_NAME,
                }

        start_lat, start_lon, start_time = resolve_spill_origin()
        start = _normalize_utc(self.case.simulation_start_utc)
        end = _normalize_utc(self.case.simulation_end_utc)
        duration_hours = int(math.ceil((end - start).total_seconds() / 3600.0))
        try:
            result = run_official_spill_forecast(
                selection=self._recipe_selection(recipe),
                start_time=start_time,
                start_lat=start_lat,
                start_lon=start_lon,
                output_run_name=model_run_name,
                forcing_override={"currents": recipe["currents_path"], "wind": recipe["wind_path"], "wave": recipe["wave_path"]},
                simulation_start_utc=self.case.simulation_start_utc,
                simulation_end_utc=self.case.simulation_end_utc,
                snapshot_hours=[24, 48, duration_hours],
                date_composite_dates=list(self.validation_dates),
                transport_overrides={
                    "coastline_action": self.retention_config["coastline_action"],
                    "coastline_approximation_precision": self.retention_config["coastline_approximation_precision"],
                    "time_step_minutes": self.retention_config["time_step_minutes"],
                },
                seed_overrides=branch.seed_overrides,
                sensitivity_context={
                    "track": RECIPE_SENSITIVITY_R1_MULTIBRANCH_DIR_NAME,
                    "recipe_id": recipe["recipe_id"],
                    "branch_id": branch.branch_id,
                    "initialization_mode": branch.initialization_mode,
                    "selected_retention_mode": "R1",
                    "coastline_action": self.retention_config["coastline_action"],
                    "pygnome_is_comparator_only": True,
                },
            )
            if result.get("status") != "success":
                return {**recipe, "branch_id": branch.branch_id, "initialization_mode": branch.initialization_mode, "status": "failed", "run_name": model_run_name, "model_dir": str(model_dir), "failure_reason": result.get("message", result.get("status", "unknown"))}
        except Exception as exc:
            return {**recipe, "branch_id": branch.branch_id, "initialization_mode": branch.initialization_mode, "status": "failed", "run_name": model_run_name, "model_dir": str(model_dir), "failure_reason": f"{type(exc).__name__}: {exc}"}
        return {**recipe, "branch_id": branch.branch_id, "initialization_mode": branch.initialization_mode, "status": "success", "run_name": model_run_name, "model_dir": str(model_dir)}

    @staticmethod
    def _model_dir_complete(model_dir: Path) -> bool:
        return (
            (model_dir / "forecast" / "forecast_manifest.json").exists()
            and (model_dir / "ensemble" / "ensemble_manifest.json").exists()
            and bool(list((model_dir / "ensemble").glob("member_*.nc")))
        )

    def _recipe_selection(self, recipe: dict) -> RecipeSelection:
        return RecipeSelection(
            recipe=str(recipe["recipe_id"]),
            source_kind="r1_multibranch_forcing_recipe_sensitivity",
            source_path=None,
            status_flag="provisional",
            valid=False,
            provisional=True,
            rerun_required=False,
            note="R1 multibranch recipe sensitivity; public observations remain truth.",
        )

    def _prepare_opendrift_tracks(self, recipe: dict, branch: BranchSpec, model_dir: Path, run_record: dict) -> list[dict]:
        recipe_id = str(recipe["recipe_id"])
        branch_id = branch.branch_id
        track_base = self.products_dir / recipe_id / branch_id
        track_base.mkdir(parents=True, exist_ok=True)
        tracks: list[dict] = []

        deterministic_products = self._prepare_deterministic_products(model_dir, track_base)
        tracks.append(
            self._track_record(
                recipe=recipe,
                branch=branch,
                product_kind="deterministic",
                model_name=f"OpenDrift deterministic {recipe_id} {branch_id}",
                track_id=f"OD_{recipe_id}_{branch_id}_det",
                date_products=deterministic_products,
                event_path=self._build_model_eventcorridor_union(
                    deterministic_products,
                    track_base / f"deterministic_eventcorridor_model_union_{self.eventcorridor_label}.tif",
                ),
                model_dir=model_dir,
                run_record=run_record,
                structural_limitations="Deterministic OpenDrift control; no ensemble probability thresholding.",
            )
        )

        p50_products = self._prepare_ensemble_threshold_products(model_dir, track_base, threshold=0.5, label="p50")
        tracks.append(
            self._track_record(
                recipe=recipe,
                branch=branch,
                product_kind="ensemble_p50",
                model_name=f"OpenDrift ensemble p50 {recipe_id} {branch_id}",
                track_id=f"OD_{recipe_id}_{branch_id}_ens_p50",
                date_products=p50_products,
                event_path=self._build_model_eventcorridor_union(
                    p50_products,
                    track_base / f"ensemble_p50_eventcorridor_model_union_{self.eventcorridor_label}.tif",
                ),
                model_dir=model_dir,
                run_record=run_record,
                structural_limitations="p50 remains p50. Lower thresholds are included only if calibration selected one.",
            )
        )

        if self.threshold_context["include_lower_selected_threshold"]:
            threshold = float(self.threshold_context["selected_threshold"])
            label = str(self.threshold_context["selected_threshold_label"])
            lower_products = self._prepare_ensemble_threshold_products(model_dir, track_base, threshold=threshold, label=label)
            tracks.append(
                self._track_record(
                    recipe=recipe,
                    branch=branch,
                    product_kind=f"ensemble_{label}",
                    model_name=f"OpenDrift ensemble {label} {recipe_id} {branch_id}",
                    track_id=f"OD_{recipe_id}_{branch_id}_ens_{label}",
                    date_products=lower_products,
                    event_path=self._build_model_eventcorridor_union(
                        lower_products,
                        track_base / f"ensemble_{label}_eventcorridor_model_union_{self.eventcorridor_label}.tif",
                    ),
                    model_dir=model_dir,
                    run_record=run_record,
                    structural_limitations=f"{label} is an explicit threshold sensitivity product, not relabeled p50.",
                )
            )
        return tracks

    def _prepare_deterministic_products(self, model_dir: Path, out_dir: Path) -> dict[str, Path]:
        nc_path = next(iter(sorted((model_dir / "forecast").glob("deterministic_control_*.nc"))), None)
        if nc_path is None:
            raise FileNotFoundError(f"Missing deterministic OpenDrift NetCDF under {model_dir / 'forecast'}")
        products = {}
        for date in self.validation_dates:
            out_path = out_dir / f"deterministic_footprint_mask_{date}_datecomposite.tif"
            composite = np.zeros((self.grid.height, self.grid.width), dtype=np.float32)
            with xr.open_dataset(nc_path) as ds:
                times = normalize_time_index(ds["time"].values)
                for index, timestamp in enumerate(times):
                    if pd.Timestamp(timestamp).date().isoformat() != date:
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
                    composite = np.maximum(composite, hits.astype(np.float32))
            composite = apply_ocean_mask(composite, sea_mask=self.sea_mask, fill_value=0.0)
            save_raster(self.grid, composite.astype(np.float32), out_path)
            products[date] = out_path
        return products

    def _prepare_ensemble_threshold_products(self, model_dir: Path, out_dir: Path, *, threshold: float, label: str) -> dict[str, Path]:
        products = {}
        for date in self.validation_dates:
            out_path = out_dir / f"mask_{label}_{date}_datecomposite.tif"
            if abs(threshold - 0.5) < 1e-9:
                source = model_dir / "ensemble" / f"mask_p50_{date}_datecomposite.tif"
                if not source.exists():
                    raise FileNotFoundError(f"Missing p50 date-composite product: {source}")
                data = _read_raster(source)
            else:
                prob_path = model_dir / "ensemble" / f"prob_presence_{date}_datecomposite.tif"
                if not prob_path.exists():
                    raise FileNotFoundError(f"Missing probability date-composite for threshold {label}: {prob_path}")
                data = (_read_raster(prob_path) >= threshold).astype(np.float32)
            data = apply_ocean_mask(data, sea_mask=self.sea_mask, fill_value=0.0)
            save_raster(self.grid, data.astype(np.float32), out_path)
            products[date] = out_path
        return products

    def _build_model_eventcorridor_union(self, date_products: dict[str, Path], out_path: Path) -> Path:
        union = np.zeros((self.grid.height, self.grid.width), dtype=np.float32)
        for date in self.validation_dates:
            union = np.maximum(union, self.helper._load_binary_score_mask(date_products[date]))
        union = apply_ocean_mask(union, sea_mask=self.sea_mask, fill_value=0.0)
        save_raster(self.grid, union.astype(np.float32), out_path)
        return out_path

    def _track_record(
        self,
        *,
        recipe: dict,
        branch: BranchSpec,
        product_kind: str,
        model_name: str,
        track_id: str,
        date_products: dict[str, Path],
        event_path: Path,
        model_dir: Path,
        run_record: dict,
        structural_limitations: str,
    ) -> dict:
        return {
            "track_id": track_id,
            "track_name": f"{product_kind}_vs_public",
            "model_name": model_name,
            "model_family": "OpenDrift",
            "recipe_id": recipe["recipe_id"],
            "branch_id": branch.branch_id,
            "initialization_mode": branch.initialization_mode,
            "product_kind": product_kind,
            "source_geometry_label": branch.source_geometry_label,
            "retention_coastline_action": "previous",
            "transport_model": "oceandrift",
            "provisional_transport_model": True,
            "current_source": recipe["current_source"],
            "wind_source": recipe["wind_source"],
            "wave_stokes_status": f"{recipe['wave_source']} required; R1 retention fixed.",
            "forcing_manifest_paths": f"{model_dir / 'forecast' / 'forecast_manifest.json'};{model_dir / 'ensemble' / 'ensemble_manifest.json'}",
            "structural_limitations": structural_limitations,
            "strict_march6_forecast": date_products[STRICT_VALIDATION_DATE],
            "date_forecasts": date_products,
            "eventcorridor_forecast": event_path,
            "model_dir": str(model_dir),
            "run_name": run_record.get("run_name", ""),
            "element_count_used": self._element_count_from_manifest(model_dir),
        }

    @staticmethod
    def _element_count_from_manifest(model_dir: Path) -> int | str:
        manifest_path = model_dir / "ensemble" / "ensemble_manifest.json"
        if not manifest_path.exists():
            return ""
        manifest = _read_json(manifest_path)
        return (manifest.get("ensemble_configuration") or {}).get("element_count", "")

    def _prepare_pygnome_comparator_track(self) -> dict | None:
        products_dir = self.case_output / PYGNOME_PUBLIC_COMPARISON_DIR_NAME / "products" / "C3_pygnome_deterministic"
        if not products_dir.exists():
            return None
        date_products = {
            date: products_dir / f"pygnome_footprint_mask_{date}_datecomposite.tif"
            for date in self.validation_dates
        }
        event = products_dir / f"pygnome_eventcorridor_model_union_{self.eventcorridor_label}.tif"
        if not event.exists() or any(not path.exists() for path in date_products.values()):
            return None
        metadata_path = products_dir / "pygnome_benchmark_metadata.json"
        metadata = _read_json(metadata_path) if metadata_path.exists() else {}
        return {
            "track_id": "PYGNOME_FIXED_DET",
            "track_name": "pygnome_deterministic_fixed_comparator",
            "model_name": "PyGNOME deterministic benchmark",
            "model_family": "PyGNOME",
            "recipe_id": "pygnome_fixed_comparator",
            "branch_id": "B_surrogate",
            "initialization_mode": "B_observation_initialized_polygon_surrogate_clustered_point_spills",
            "product_kind": "deterministic",
            "source_geometry_label": "clustered_point_surrogate_from_march3_polygon",
            "retention_coastline_action": "PyGNOME default benchmark behavior",
            "transport_model": "pygnome",
            "provisional_transport_model": True,
            "current_source": "not attached in current PyGNOME benchmark service",
            "wind_source": "nearest compatible constant-wind PyGNOME benchmark",
            "wave_stokes_status": "not reproduced identically; PyGNOME benchmark does not attach official Stokes forcing",
            "forcing_manifest_paths": str(self.case_output / PYGNOME_PUBLIC_COMPARISON_DIR_NAME / "pygnome_public_comparison_run_manifest.json"),
            "structural_limitations": "Fixed comparator only. PyGNOME is not truth and does not reproduce the exact OpenDrift gridded forcing stack.",
            "strict_march6_forecast": date_products[STRICT_VALIDATION_DATE],
            "date_forecasts": date_products,
            "eventcorridor_forecast": event,
            "model_dir": str(products_dir),
            "run_name": "pygnome_public_comparison/C3",
            "element_count_used": metadata.get("benchmark_particles", ""),
        }

    def _build_pairings(self, tracks: list[dict], observations: dict[str, Any]) -> pd.DataFrame:
        rows: list[dict] = []
        for track in tracks:
            rows.append(
                self._pair_record(
                    track,
                    pair_role="strict_march6",
                    obs_date=STRICT_VALIDATION_DATE,
                    forecast_path=track["strict_march6_forecast"],
                    observation_path=observations["strict_march6"],
                    source_semantics="strict_single_date_stress_test_march6_public_obs",
                )
            )
            for date in self.validation_dates:
                rows.append(
                    self._pair_record(
                        track,
                        pair_role="multidate_date_union",
                        obs_date=date,
                        forecast_path=track["date_forecasts"][date],
                        observation_path=observations["date_unions"][date],
                        source_semantics=f"per_date_union_{date}_public_observation_vs_model",
                    )
                )
            rows.append(
                self._pair_record(
                    track,
                    pair_role="eventcorridor_march4_6",
                    obs_date=self.eventcorridor_label,
                    forecast_path=track["eventcorridor_forecast"],
                    observation_path=observations["eventcorridor_march4_6"],
                    source_semantics="eventcorridor_public_observation_union_excluding_march3",
                )
            )
        return pd.DataFrame(rows)

    def _pair_record(
        self,
        track: dict,
        *,
        pair_role: str,
        obs_date: str,
        forecast_path: Path,
        observation_path: Path,
        source_semantics: str,
    ) -> dict:
        return {
            "track_id": track["track_id"],
            "track_name": track["track_name"],
            "model_name": track["model_name"],
            "model_family": track["model_family"],
            "recipe_id": track["recipe_id"],
            "branch_id": track["branch_id"],
            "initialization_mode": track["initialization_mode"],
            "product_kind": track["product_kind"],
            "pair_id": f"{track['track_id']}_{pair_role}_{obs_date}".replace(":", "-"),
            "pair_role": pair_role,
            "obs_date": obs_date,
            "forecast_product": Path(forecast_path).name,
            "forecast_path": str(forecast_path),
            "observation_product": Path(observation_path).name,
            "observation_path": str(observation_path),
            "metric": "FSS",
            "windows_km": ",".join(str(window) for window in OFFICIAL_PHASE3B_WINDOWS_KM),
            "source_semantics": source_semantics,
            "truth_source": "accepted_public_observation_derived_mask",
            "pygnome_used_as_truth": False,
            "retention_coastline_action": track["retention_coastline_action"],
            "transport_model": track["transport_model"],
            "provisional_transport_model": track["provisional_transport_model"],
            "current_source": track["current_source"],
            "wind_source": track["wind_source"],
            "wave_stokes_status": track["wave_stokes_status"],
            "structural_limitations": track["structural_limitations"],
            "element_count_used": track.get("element_count_used", ""),
            "precheck_csv": "",
            "precheck_json": "",
        }

    def _score_pairings(self, pairings: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        scored_rows: list[dict] = []
        fss_rows: list[dict] = []
        diagnostics_rows: list[dict] = []
        for _, row in pairings.iterrows():
            forecast_path = Path(str(row["forecast_path"]))
            observation_path = Path(str(row["observation_path"]))
            precheck = precheck_same_grid(
                forecast=forecast_path,
                target=observation_path,
                report_base_path=self.precheck_dir / str(row["pair_id"]),
            )
            if not precheck.passed:
                raise RuntimeError(f"Same-grid precheck failed for {row['pair_id']}: {precheck.json_report_path}")
            forecast = self.helper._load_binary_score_mask(forecast_path)
            observation = self.helper._load_binary_score_mask(observation_path)
            diagnostics = self.helper._compute_mask_diagnostics(forecast, observation)
            scored = row.to_dict()
            scored["precheck_csv"] = str(precheck.csv_report_path)
            scored["precheck_json"] = str(precheck.json_report_path)
            scored_rows.append(scored)
            diagnostics_rows.append({**scored, **diagnostics})
            for window_km in OFFICIAL_PHASE3B_WINDOWS_KM:
                window_cells = self.helper._window_km_to_cells(window_km)
                fss = float(
                    np.clip(
                        calculate_fss(forecast, observation, window=window_cells, valid_mask=self.valid_mask),
                        0.0,
                        1.0,
                    )
                )
                fss_rows.append({**scored, "window_km": int(window_km), "window_cells": int(window_cells), "fss": fss})
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

    def _build_ranking(self, summary_df: pd.DataFrame) -> pd.DataFrame:
        rows = []
        working = summary_df.copy()
        working["mean_fss"] = working.apply(_mean_fss, axis=1)
        for track_id, group in working.groupby("track_id"):
            track = group.iloc[0]
            strict = group[group["pair_role"] == "strict_march6"]
            event = group[group["pair_role"] == "eventcorridor_march4_6"]
            strict_row = strict.iloc[0].to_dict() if not strict.empty else {}
            event_row = event.iloc[0].to_dict() if not event.empty else {}
            rows.append(
                {
                    "track_id": track_id,
                    "model_name": track["model_name"],
                    "model_family": track["model_family"],
                    "recipe_id": track["recipe_id"],
                    "branch_id": track["branch_id"],
                    "product_kind": track["product_kind"],
                    "strict_march6_mean_fss": _mean_fss(strict_row),
                    "strict_march6_iou": strict_row.get("iou", np.nan),
                    "strict_march6_dice": strict_row.get("dice", np.nan),
                    "strict_march6_nearest_distance_to_obs_m": strict_row.get("nearest_distance_to_obs_m", np.nan),
                    "eventcorridor_mean_fss": _mean_fss(event_row),
                    "eventcorridor_iou": event_row.get("iou", np.nan),
                    "eventcorridor_dice": event_row.get("dice", np.nan),
                    "eventcorridor_nearest_distance_to_obs_m": event_row.get("nearest_distance_to_obs_m", np.nan),
                    "element_count_used": track.get("element_count_used", ""),
                }
            )
        ranking = pd.DataFrame(rows)
        if ranking.empty:
            return ranking
        ranking = ranking.sort_values(
            ["eventcorridor_mean_fss", "eventcorridor_iou", "eventcorridor_dice", "strict_march6_mean_fss"],
            ascending=[False, False, False, False],
            na_position="last",
        ).reset_index(drop=True)
        ranking["eventcorridor_rank"] = np.arange(1, len(ranking) + 1)
        strict_ranked = ranking.sort_values(
            ["strict_march6_mean_fss", "strict_march6_iou", "strict_march6_dice", "eventcorridor_mean_fss"],
            ascending=[False, False, False, False],
            na_position="last",
        ).reset_index(drop=True)
        ranking["strict_march6_rank"] = ranking["track_id"].map(
            {track_id: rank for rank, track_id in enumerate(strict_ranked["track_id"], start=1)}
        )
        return ranking

    def _write_outputs(
        self,
        pairings: pd.DataFrame,
        fss_df: pd.DataFrame,
        diagnostics_df: pd.DataFrame,
        summary_df: pd.DataFrame,
        ranking_df: pd.DataFrame,
    ) -> dict[str, Path]:
        paths = {
            "pairing": self.output_dir / "recipe_sensitivity_r1_multibranch_pairing_manifest.csv",
            "fss_by_window": self.output_dir / "recipe_sensitivity_r1_multibranch_fss_by_window.csv",
            "diagnostics": self.output_dir / "recipe_sensitivity_r1_multibranch_diagnostics.csv",
            "summary": self.output_dir / "recipe_sensitivity_r1_multibranch_summary.csv",
            "ranking": self.output_dir / "recipe_sensitivity_r1_multibranch_ranking.csv",
        }
        _write_csv(paths["pairing"], pairings)
        _write_csv(paths["fss_by_window"], fss_df)
        _write_csv(paths["diagnostics"], diagnostics_df)
        _write_csv(paths["summary"], summary_df)
        _write_csv(paths["ranking"], ranking_df)
        return paths

    def _write_qa(self, summary_df: pd.DataFrame) -> dict[str, Path]:
        path = self.output_dir / "qa_recipe_sensitivity_r1_multibranch.png"
        if plt is None:
            return {"ranking_plot": path}
        event = summary_df[summary_df["pair_role"] == "eventcorridor_march4_6"].copy()
        if event.empty:
            return {"ranking_plot": path}
        event["mean_fss"] = event.apply(_mean_fss, axis=1)
        event = event.sort_values("mean_fss", ascending=True)
        fig, ax = plt.subplots(figsize=(10, max(4, 0.35 * len(event))))
        colors = ["#234f1e" if family == "OpenDrift" else "#8c510a" for family in event["model_family"]]
        ax.barh(event["track_id"].astype(str), event["mean_fss"], color=colors)
        ax.set_xlabel("Mean FSS across 1/3/5/10 km")
        ax.set_title(f"{self.eventcorridor_label} Event-Corridor FSS by Recipe/Branch")
        ax.grid(axis="x", alpha=0.25)
        fig.tight_layout()
        fig.savefig(path, dpi=160)
        plt.close(fig)
        return {"ranking_plot": path}

    def _write_report(
        self,
        summary_df: pd.DataFrame,
        ranking_df: pd.DataFrame,
        recommendation: dict,
        recipes: list[dict],
        run_records: list[dict],
        paths: dict[str, Path],
        qa_paths: dict[str, Path],
        forcing_preparation: dict[str, Any],
    ) -> Path:
        path = self.output_dir / "recipe_sensitivity_r1_multibranch_report.md"
        working = summary_df.copy()
        working["mean_fss"] = working.apply(_mean_fss, axis=1)
        strict = _rank_rows(working[working["pair_role"] == "strict_march6"])
        event = _rank_rows(working[working["pair_role"] == "eventcorridor_march4_6"])
        unavailable = [row for row in recipes if not row["available"]]
        failed = [row for row in run_records if row.get("status") == "failed"]
        lines = [
            "# R1 Multibranch Recipe Sensitivity",
            "",
            "Retention is fixed at `R1` with `general:coastline_action=previous`.",
            "Public observation masks remain truth; PyGNOME is a fixed comparator, not truth.",
            "",
            "## Recommendation",
            "",
            f"- Recommendation: `{recommendation['recommendation']}`",
            f"- Next branch: `{recommendation['recommended_next_branch']}`",
            f"- Any OpenDrift branch beats PyGNOME: `{recommendation['any_opendrift_branch_beats_pygnome']}`",
            f"- Promotable OpenDrift branch-B recipe: `{recommendation['promotable_recipe_id'] or 'none'}`",
            f"- Promotion track: `{recommendation['promotable_track_id'] or 'none'}`",
            f"- Reason: {recommendation['reason']}",
            f"- Promotion basis: {recommendation['promotion_selection_basis']}",
            "",
            "## Best Strict March 6 Rows",
            "",
            _csv_block(
                strict[
                    [
                        "track_id",
                        "model_family",
                        "recipe_id",
                        "branch_id",
                        "product_kind",
                        "mean_fss",
                        "iou",
                        "dice",
                        "nearest_distance_to_obs_m",
                    ]
                ].head(8)
            ),
            "",
            f"## Best {self.eventcorridor_label} Event-Corridor Rows",
            "",
            _csv_block(
                event[
                    [
                        "track_id",
                        "model_family",
                        "recipe_id",
                        "branch_id",
                        "product_kind",
                        "mean_fss",
                        "iou",
                        "dice",
                        "nearest_distance_to_obs_m",
                    ]
                ].head(8)
            ),
            "",
            "## Missing Or Skipped Recipes",
            "",
        ]
        if unavailable:
            lines.extend(
                f"- `{row['recipe_id']}` skipped because inputs are missing: `{'; '.join(row['missing_inputs'])}`"
                for row in unavailable
            )
        else:
            lines.append("- None.")
        gfs_available = [row["recipe_id"] for row in recipes if row["recipe_id"].endswith("_gfs") and row["available"]]
        gfs_skipped = [row["recipe_id"] for row in recipes if row["recipe_id"].endswith("_gfs") and not row["available"]]
        lines.extend(
            [
                "",
                "## Forcing Preparation",
                "",
                f"- GFS preparation requested: `{forcing_preparation.get('gfs_requested', False)}`",
                f"- GFS preparation status: `{forcing_preparation.get('gfs_status', 'unknown')}`",
                f"- GFS wind path: `{forcing_preparation.get('gfs_path', '')}`",
                f"- GFS preparation error: `{forcing_preparation.get('gfs_error', '') or 'none'}`",
                f"- GFS recipes included: `{', '.join(gfs_available) if gfs_available else 'none'}`",
                f"- GFS recipes skipped: `{', '.join(gfs_skipped) if gfs_skipped else 'none'}`",
            ]
        )
        lines.extend(["", "## Failed Runs", ""])
        if failed:
            lines.extend(f"- `{row.get('recipe_id')}` `{row.get('branch_id')}` failed: {row.get('failure_reason')}" for row in failed)
        else:
            lines.append("- None.")
        lines.extend(
            [
                "",
                "## Outputs",
                "",
                f"- Summary: `{paths['summary']}`",
                f"- Ranking: `{paths['ranking']}`",
                f"- Diagnostics: `{paths['diagnostics']}`",
                f"- Pairing manifest: `{paths['pairing']}`",
                f"- FSS by window: `{paths['fss_by_window']}`",
                f"- QA plot: `{qa_paths.get('ranking_plot', '')}`",
                "",
            ]
        )
        path.write_text("\n".join(lines), encoding="utf-8")
        return path

    def _write_manifest(
        self,
        *,
        recipes: list[dict],
        run_records: list[dict],
        tracks: list[dict],
        observations: dict[str, Any],
        recommendation: dict,
        paths: dict[str, Path],
        qa_paths: dict[str, Path],
        report_path: Path,
        forcing_preparation: dict[str, Any],
    ) -> Path:
        path = self.output_dir / "recipe_sensitivity_r1_multibranch_run_manifest.json"
        track_records = []
        for track in tracks:
            record = {key: (str(value) if isinstance(value, Path) else value) for key, value in track.items() if key != "date_forecasts"}
            record["date_forecasts"] = {date: str(product_path) for date, product_path in track["date_forecasts"].items()}
            track_records.append(record)
        payload = {
            "phase": RECIPE_SENSITIVITY_R1_MULTIBRANCH_DIR_NAME,
            "run_name": self.case.run_name,
            "created_utc": datetime.utcnow().isoformat(timespec="seconds") + "Z",
            "output_slug": self.output_slug,
            "controls": {
                "retention": "R1",
                "coastline_action": self.retention_config["coastline_action"],
                "coastline_approximation_precision": self.retention_config["coastline_approximation_precision"],
                "time_step_minutes": self.retention_config["time_step_minutes"],
                "simulation_start_utc": self.case.simulation_start_utc,
                "simulation_end_utc": self.case.simulation_end_utc,
                "validation_dates": self.validation_dates,
                "strict_march6_pairing_unchanged": True,
                "public_observation_masks_unchanged": True,
                "pygnome_used_as_truth": False,
            },
            "threshold_context": self.threshold_context,
            "forcing_preparation": forcing_preparation,
            "gfs_recipes_included": [row["recipe_id"] for row in recipes if row["recipe_id"].endswith("_gfs") and row["available"]],
            "gfs_recipes_skipped": [row["recipe_id"] for row in recipes if row["recipe_id"].endswith("_gfs") and not row["available"]],
            "recipe_matrix": recipes,
            "branches": [branch.__dict__ for branch in BRANCHES],
            "run_records": run_records,
            "tracks": track_records,
            "observations": {
                "strict_march6": str(observations["strict_march6"]),
                "date_unions": {date: str(obs_path) for date, obs_path in observations["date_unions"].items()},
                "eventcorridor_march4_6": str(observations["eventcorridor_march4_6"]),
            },
            "recommendation": recommendation,
            "promotion_candidate": select_promotable_opendrift_recipe(
                pd.read_csv(paths["summary"]) if paths["summary"].exists() else pd.DataFrame()
            ),
            "paths": {key: str(value) for key, value in paths.items()},
            "qa_paths": {key: str(value) for key, value in qa_paths.items()},
            "report": str(report_path),
        }
        _write_json(path, payload)
        return path


def run_recipe_sensitivity_r1_multibranch() -> dict:
    return RecipeSensitivityR1MultibranchService().run()
