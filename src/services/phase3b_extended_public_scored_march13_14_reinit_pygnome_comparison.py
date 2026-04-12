"""Appendix-only March 13 -> March 14 NOAA reinit cross-model comparator."""

from __future__ import annotations

import json
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from src.core.case_context import get_case_context
from src.helpers.metrics import calculate_fss
from src.helpers.raster import extract_particles_at_time, rasterize_particles, save_raster
from src.helpers.scoring import apply_ocean_mask, precheck_same_grid
from src.services.gnome_comparison import GNOME_AVAILABLE, GnomeComparisonService
from src.services.phase3b_extended_public_scored_march13_14_reinit import (
    LOCAL_TIMEZONE,
    MARCH13_14_REINIT_DIR_NAME,
    MARCH13_NOAA_SOURCE_DATE,
    MARCH13_NOAA_SOURCE_KEY,
    MARCH14_NOAA_SOURCE_DATE,
    MARCH14_NOAA_SOURCE_KEY,
    NOAA_SOURCE_LIMITATION_NOTE,
    Phase3BExtendedPublicScoredMarch1314ReinitService,
    REQUESTED_ELEMENT_COUNT,
    _iso_z,
    _json_default,
    _read_json,
    _write_csv,
    _write_json,
    _write_text,
)
from src.services.phase3b_multidate_public import _hash_file
from src.services.scoring import OFFICIAL_PHASE3B_WINDOWS_KM
from src.utils.io import resolve_recipe_selection

try:
    import matplotlib.pyplot as plt
except ImportError:  # pragma: no cover
    plt = None

try:
    import netCDF4
except ImportError:  # pragma: no cover
    netCDF4 = None


PHASE = "phase3b_extended_public_scored_march13_14_reinit_pygnome_comparison"
PAIR_ROLE = "march14_nextday_reinit_crossmodel_compare"
TRACK_LABEL = "appendix_only_march13_14_noaa_reinit_crossmodel_compare"
PYGNOME_TRACK_ID = "pygnome_reinit_deterministic"
PYGNOME_TRACK_NAME = "pygnome_reinit_deterministic_vs_march14_noaa"
PYGNOME_MODEL_NAME = "PyGNOME deterministic March 13 reinit comparator"
PYGNOME_OUTPUT_NAME = "pygnome_reinit_deterministic_control.nc"
TRACK_ID_BY_BRANCH = {"R0": "R0_reinit_p50", "R1_previous": "R1_previous_reinit_p50"}
TRACK_TIE_BREAK_ORDER = {
    "R1_previous_reinit_p50": 0,
    "R0_reinit_p50": 1,
    "pygnome_reinit_deterministic": 2,
}


class Phase3BExtendedPublicScoredMarch1314ReinitPyGnomeComparisonService(
    Phase3BExtendedPublicScoredMarch1314ReinitService
):
    def __init__(self):
        super().__init__()
        self.case = get_case_context()
        if not self.case.is_official:
            raise RuntimeError(
                "phase3b_extended_public_scored_march13_14_reinit_pygnome_comparison is only supported for official Mindoro workflows."
            )
        self.reinit_output_dir = self.case_output_dir / MARCH13_14_REINIT_DIR_NAME
        self.output_dir = self.case_output_dir / PHASE
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.track_dir = self.output_dir / "tracks"
        self.products_dir = self.output_dir / "products"
        self.precheck_dir = self.output_dir / "precheck"
        self.qa_dir = self.output_dir / "qa"
        for path in (self.track_dir, self.products_dir, self.precheck_dir, self.qa_dir):
            path.mkdir(parents=True, exist_ok=True)
        self.reused_output_paths: list[Path] = []
        self.reused_hashes_before: dict[str, str] = {}

    def run(self) -> dict:
        start_row, target_row = self._load_reinit_observation_pair()
        self._ensure_scoreable_observation(start_row, role_label="march13_seed")
        self._ensure_scoreable_observation(target_row, role_label="march14_target")
        seed_release = self._prepare_seed_release_artifacts(start_row)
        upstream = self._load_completed_reinit_context()
        tracks = self._prepare_tracks(upstream, seed_release)
        pairings = self._build_pairings(target_row, tracks)
        scored_pairings, fss_df, diagnostics_df = self._score_pairings(pairings)
        summary_df = self._summarize(scored_pairings, fss_df, diagnostics_df)
        ranking_df = self._rank_tracks(summary_df)
        qa_paths = self._write_qa(summary_df)
        tracks_df = pd.DataFrame([self._track_registry_row(track) for track in tracks])
        artifacts = self._write_outputs(tracks_df, scored_pairings, fss_df, diagnostics_df, summary_df, ranking_df)
        memo_path = self._write_memo(summary_df, ranking_df, artifacts)
        manifest_path = self._write_manifest(
            start_row=start_row,
            target_row=target_row,
            seed_release=seed_release,
            upstream=upstream,
            tracks=tracks,
            summary_df=summary_df,
            ranking_df=ranking_df,
            artifacts=artifacts,
            qa_paths=qa_paths,
            memo_path=memo_path,
        )
        self._verify_locked_outputs_unchanged()
        winner = ranking_df.iloc[0].to_dict() if not ranking_df.empty else {}
        return {
            "output_dir": self.output_dir,
            "tracks_registry_csv": artifacts["tracks_registry_csv"],
            "pairing_manifest_csv": artifacts["pairing_manifest_csv"],
            "fss_csv": artifacts["fss_csv"],
            "diagnostics_csv": artifacts["diagnostics_csv"],
            "summary_csv": artifacts["summary_csv"],
            "ranking_csv": artifacts["ranking_csv"],
            "memo_md": memo_path,
            "run_manifest_json": manifest_path,
            "winner_track_id": str(winner.get("track_id") or ""),
            "winner_model_name": str(winner.get("model_name") or ""),
            "start_source_key": MARCH13_NOAA_SOURCE_KEY,
            "target_source_key": MARCH14_NOAA_SOURCE_KEY,
        }

    def _required_reinit_paths(self) -> dict[str, Path]:
        return {
            "reinit_summary_csv": self.reinit_output_dir / "march13_14_reinit_summary.csv",
            "reinit_pairing_manifest_csv": self.reinit_output_dir / "march13_14_reinit_branch_pairing_manifest.csv",
            "reinit_fss_csv": self.reinit_output_dir / "march13_14_reinit_fss_by_window.csv",
            "reinit_diagnostics_csv": self.reinit_output_dir / "march13_14_reinit_diagnostics.csv",
            "reinit_branch_survival_csv": self.reinit_output_dir / "march13_14_reinit_branch_survival_summary.csv",
            "reinit_run_manifest_json": self.reinit_output_dir / "march13_14_reinit_run_manifest.json",
        }

    def _load_completed_reinit_context(self) -> dict:
        required_paths = self._required_reinit_paths()
        missing = [str(path) for path in required_paths.values() if not path.exists()]
        if missing:
            raise FileNotFoundError(
                "March 13 -> March 14 PyGNOME comparator requires completed upstream reinit artifacts. "
                f"Run {MARCH13_14_REINIT_DIR_NAME} first. Missing: {'; '.join(missing)}"
            )

        summary_df = pd.read_csv(required_paths["reinit_summary_csv"])
        branch_survival_df = pd.read_csv(required_paths["reinit_branch_survival_csv"])
        run_manifest = _read_json(required_paths["reinit_run_manifest_json"])
        expected_branches = {"R0", "R1_previous"}
        available_branches = set(summary_df.get("branch_id", pd.Series(dtype=str)).astype(str).tolist())
        missing_branches = sorted(expected_branches - available_branches)
        if missing_branches:
            raise RuntimeError(
                "Completed March 13 -> March 14 reinit summary is missing required branch rows: "
                + ", ".join(missing_branches)
            )

        branch_rows: dict[str, dict[str, Any]] = {}
        tracked_paths = list(required_paths.values())
        for branch_id in sorted(expected_branches):
            branch_frame = summary_df[summary_df["branch_id"].astype(str) == branch_id].copy()
            if len(branch_frame) != 1:
                raise RuntimeError(
                    f"Expected exactly one scored summary row for upstream branch {branch_id}, found {len(branch_frame)}."
                )
            row = branch_frame.iloc[0].to_dict()
            forecast_path = Path(str(row.get("forecast_path", "") or ""))
            if not forecast_path.exists():
                raise FileNotFoundError(
                    f"Completed March 13 -> March 14 reinit branch {branch_id} is missing its March 14 forecast mask: {forecast_path}"
                )
            tracked_paths.append(forecast_path)
            probability_path = Path(str(row.get("probability_path", "") or ""))
            if probability_path.exists():
                tracked_paths.append(probability_path)
            branch_rows[branch_id] = row

        self.reused_output_paths = tracked_paths
        self.reused_hashes_before = self._snapshot_paths(self.reused_output_paths)
        return {
            "required_paths": {key: str(value) for key, value in required_paths.items()},
            "summary_df": summary_df,
            "branch_survival_df": branch_survival_df,
            "run_manifest": run_manifest,
            "branch_rows": branch_rows,
        }

    @staticmethod
    def _snapshot_paths(paths: list[Path]) -> dict[str, str]:
        return {str(path): _hash_file(path) for path in paths if path.exists()}

    def _verify_locked_outputs_unchanged(self) -> None:
        current_locked = Phase3BExtendedPublicScoredMarch1314ReinitService._snapshot_locked_outputs(self)
        if current_locked != self.locked_hashes_before:
            raise RuntimeError("March 13 -> March 14 PyGNOME comparator modified locked official/main outputs.")
        if self.reused_hashes_before:
            current_reused = self._snapshot_paths(self.reused_output_paths)
            if current_reused != self.reused_hashes_before:
                raise RuntimeError(
                    "March 13 -> March 14 PyGNOME comparator modified reused OpenDrift reinit outputs."
                )

    def _prepare_tracks(self, upstream: dict, seed_release: dict) -> list[dict]:
        selection = resolve_recipe_selection()
        return [
            self._prepare_reused_opendrift_track("R0", upstream),
            self._prepare_reused_opendrift_track("R1_previous", upstream),
            self._prepare_pygnome_track(selection.recipe, seed_release),
        ]

    def _prepare_reused_opendrift_track(self, branch_id: str, upstream: dict) -> dict:
        row = dict(upstream["branch_rows"][branch_id])
        track_id = TRACK_ID_BY_BRANCH[branch_id]
        pretty_branch = "R1 previous" if branch_id == "R1_previous" else "R0"
        recipe_name = str((upstream.get("run_manifest") or {}).get("recipe", {}).get("recipe", "") or "")
        return {
            "track_id": track_id,
            "track_name": f"{track_id}_vs_march14_noaa",
            "model_name": f"OpenDrift {pretty_branch} reinit p50",
            "model_family": "OpenDrift",
            "transport_model": "oceandrift",
            "provisional_transport_model": True,
            "initialization_mode": "accepted_march13_noaa_processed_polygon_reinit",
            "current_source": f"same frozen {recipe_name} OpenDrift recipe currents as upstream reinit".strip(),
            "wind_source": f"same frozen {recipe_name} OpenDrift recipe winds as upstream reinit".strip(),
            "wave_stokes_status": "same OpenDrift wave/Stokes stack reused from the completed March 13 -> March 14 reinit phase",
            "structural_limitations": (
                "This comparator reuses the completed March 13 -> March 14 OpenDrift branch p50 product rather than rerunning OpenDrift."
            ),
            "track_source_kind": "reused_opendrift_reinit_output",
            "reused_from_phase": MARCH13_14_REINIT_DIR_NAME,
            "source_nc": str(row.get("model_dir") or ""),
            "forecast_path": str(row.get("forecast_path") or ""),
            "probability_path": str(row.get("probability_path") or ""),
            "empty_forecast_reason": str(row.get("empty_forecast_reason") or ""),
            "forecast_nonzero_cells_from_mask": int(row.get("forecast_nonzero_cells_from_march14_localdate_mask") or 0),
            "last_active_particle_time_utc": str(row.get("last_active_particle_time_utc") or ""),
            "element_count_requested": int(row.get("element_count_requested") or REQUESTED_ELEMENT_COUNT),
            "element_count_actual": int(row.get("element_count_actual") or REQUESTED_ELEMENT_COUNT),
        }

    def _prepare_pygnome_track(self, recipe: str, seed_release: dict) -> dict:
        track_dir = self.track_dir / PYGNOME_TRACK_ID
        track_dir.mkdir(parents=True, exist_ok=True)
        nc_path = track_dir / PYGNOME_OUTPUT_NAME
        metadata_path = track_dir / "pygnome_reinit_metadata.json"
        if not nc_path.exists():
            if not GNOME_AVAILABLE:
                raise RuntimeError(
                    "March 13 -> March 14 PyGNOME comparator requires the gnome container when PyGNOME outputs are missing."
                )
            gnome_service = GnomeComparisonService()
            gnome_service.output_dir = track_dir
            py_nc_path, py_metadata = gnome_service.run_transport_benchmark_scenario(
                start_lat=float(seed_release["reference_lat"]),
                start_lon=float(seed_release["reference_lon"]),
                start_time=self.window.simulation_start_utc,
                output_name=nc_path.name,
                polygon_path=seed_release["processed_vector_path"],
                seed_time_override=self.window.simulation_start_utc,
                duration_hours=48,
                time_step_minutes=60,
            )
            if Path(py_nc_path) != nc_path:
                shutil.copyfile(py_nc_path, nc_path)
            _write_json(metadata_path, py_metadata)
        else:
            py_metadata = (
                _read_json(metadata_path)
                if metadata_path.exists()
                else {
                    "nc_path": str(nc_path),
                    "status": "reused_existing",
                    "custom_polygon_override_used": True,
                    "polygon_path_override": seed_release["processed_vector_path"],
                }
            )

        local_product = self._build_pygnome_local_date_product(nc_path, MARCH14_NOAA_SOURCE_DATE, track_dir)
        return {
            "track_id": PYGNOME_TRACK_ID,
            "track_name": PYGNOME_TRACK_NAME,
            "model_name": PYGNOME_MODEL_NAME,
            "model_family": "PyGNOME",
            "transport_model": "pygnome",
            "provisional_transport_model": True,
            "initialization_mode": "accepted_march13_noaa_processed_polygon_surrogate_clustered_point_spills",
            "current_source": "not attached identically; current PyGNOME benchmark service remains simplified",
            "wind_source": "nearest compatible constant-wind PyGNOME benchmark",
            "wave_stokes_status": "not reproduced identically; PyGNOME benchmark does not attach the OpenDrift wave/Stokes stack",
            "structural_limitations": (
                "PyGNOME is comparator-only. It is seeded from the March 13 NOAA polygon surrogate, but it does not "
                "reproduce the exact OpenDrift gridded current/wave/Stokes forcing stack."
            ),
            "track_source_kind": "new_pygnome_deterministic_comparator",
            "reused_from_phase": "",
            "source_nc": str(nc_path),
            "forecast_path": str(local_product["forecast_path"]),
            "probability_path": "",
            "empty_forecast_reason": str(local_product["empty_forecast_reason"]),
            "forecast_nonzero_cells_from_mask": int(local_product["forecast_nonzero_cells_from_mask"]),
            "last_active_particle_time_utc": str(local_product["last_active_particle_time_utc"]),
            "element_count_requested": int(py_metadata.get("benchmark_particles") or REQUESTED_ELEMENT_COUNT),
            "element_count_actual": int(py_metadata.get("benchmark_particles") or REQUESTED_ELEMENT_COUNT),
            "pygnome_metadata_path": str(metadata_path),
            **local_product,
        }

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

    def _build_pygnome_local_date_product(self, nc_path: Path, target_date: str, out_dir: Path) -> dict[str, Any]:
        out_path = out_dir / f"pygnome_footprint_mask_{target_date}_localdate.tif"
        composite = np.zeros((self.grid.height, self.grid.width), dtype=np.float32)
        target_timestamps_seen: set[str] = set()
        target_active_timestamps: set[str] = set()
        last_active_time: pd.Timestamp | None = None
        for timestamp in self._pygnome_times(nc_path):
            local_date = pd.Timestamp(timestamp).tz_localize("UTC").tz_convert(LOCAL_TIMEZONE).date().isoformat()
            if local_date == target_date:
                target_timestamps_seen.add(_iso_z(timestamp))
            try:
                lon, lat, mass, actual_time, _ = extract_particles_at_time(
                    nc_path,
                    timestamp,
                    "pygnome",
                    allow_uniform_mass_fallback=True,
                )
            except Exception:
                continue
            if len(lon) == 0:
                continue
            last_active_time = actual_time if last_active_time is None else max(last_active_time, actual_time)
            if local_date != target_date:
                continue
            target_active_timestamps.add(_iso_z(actual_time))
            hits, _ = rasterize_particles(self.grid, lon, lat, mass)
            composite = np.maximum(composite, hits.astype(np.float32))
        composite = apply_ocean_mask(composite, sea_mask=self.sea_mask, fill_value=0.0)
        save_raster(self.grid, composite.astype(np.float32), out_path)
        nonzero = int(np.count_nonzero(composite > 0))
        if nonzero > 0:
            empty_forecast_reason = ""
        elif not target_timestamps_seen:
            empty_forecast_reason = "pygnome_survival_did_not_reach_march14_local_date"
        elif not target_active_timestamps:
            empty_forecast_reason = "pygnome_march14_local_timestamps_present_but_no_active_particles"
        else:
            empty_forecast_reason = "pygnome_local_activity_present_but_no_scoreable_ocean_presence_after_masking"
        return {
            "forecast_path": str(out_path),
            "forecast_nonzero_cells_from_mask": nonzero,
            "last_active_particle_time_utc": _iso_z(last_active_time) if last_active_time is not None else "",
            "march14_local_timestamp_count": int(len(target_timestamps_seen)),
            "march14_local_active_timestamp_count": int(len(target_active_timestamps)),
            "march14_local_timestamps": ";".join(sorted(target_timestamps_seen)),
            "march14_local_active_timestamps": ";".join(sorted(target_active_timestamps)),
            "reached_march14_local_date": bool(target_timestamps_seen),
            "empty_forecast_reason": empty_forecast_reason,
        }

    def _build_pairings(self, target_row: pd.Series, tracks: list[dict]) -> pd.DataFrame:
        obs_path = self._existing_path_from_row(target_row, "extended_obs_mask")
        rows = []
        for track in tracks:
            rows.append(
                {
                    "pair_id": f"{track['track_id']}_{PAIR_ROLE}",
                    "pair_role": PAIR_ROLE,
                    "score_group": "march14_single_date_crossmodel_compare",
                    "obs_date": MARCH14_NOAA_SOURCE_DATE,
                    "validation_dates_used": MARCH14_NOAA_SOURCE_DATE,
                    "seed_obs_date": MARCH13_NOAA_SOURCE_DATE,
                    "source_key": str(target_row["source_key"]),
                    "source_name": str(target_row["source_name"]),
                    "provider": str(target_row.get("provider", "")),
                    "track_id": track["track_id"],
                    "track_name": track["track_name"],
                    "model_name": track["model_name"],
                    "model_family": track["model_family"],
                    "transport_model": track["transport_model"],
                    "provisional_transport_model": bool(track["provisional_transport_model"]),
                    "initialization_mode": track["initialization_mode"],
                    "current_source": track["current_source"],
                    "wind_source": track["wind_source"],
                    "wave_stokes_status": track["wave_stokes_status"],
                    "structural_limitations": track["structural_limitations"],
                    "forecast_product": Path(str(track["forecast_path"])).name,
                    "forecast_path": str(track["forecast_path"]),
                    "observation_product": obs_path.name,
                    "observation_path": str(obs_path),
                    "metric": "FSS",
                    "windows_km": ",".join(str(window) for window in OFFICIAL_PHASE3B_WINDOWS_KM),
                    "track_label": TRACK_LABEL,
                    "source_semantics": "march13_reinit_polygon_vs_march14_local_date_crossmodel_compare",
                    "empty_forecast_reason": str(track.get("empty_forecast_reason") or ""),
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
                    f"March 13 -> March 14 PyGNOME comparator same-grid precheck failed for {row['pair_id']}: {precheck.json_report_path}"
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
                        "track_id": scored["track_id"],
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
    def _summarize(scored_pairings: pd.DataFrame, fss_df: pd.DataFrame, diagnostics_df: pd.DataFrame) -> pd.DataFrame:
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
        summary = scored_pairings.merge(diagnostics_df[diag_cols], on="pair_id", how="left").merge(
            fss_pivot, on="pair_id", how="left"
        )
        summary["mean_fss"] = summary[[f"fss_{window}km" for window in OFFICIAL_PHASE3B_WINDOWS_KM]].mean(axis=1)
        summary["track_tie_break_order"] = summary["track_id"].map(TRACK_TIE_BREAK_ORDER).fillna(999).astype(int)
        return summary.sort_values(["track_tie_break_order"]).reset_index(drop=True)

    @staticmethod
    def _rank_tracks(summary_df: pd.DataFrame) -> pd.DataFrame:
        if summary_df.empty:
            return pd.DataFrame()
        ranked = summary_df.copy()
        ranked["_sort_nearest_distance_to_obs_m"] = pd.to_numeric(
            ranked["nearest_distance_to_obs_m"], errors="coerce"
        ).fillna(np.inf)
        ranked = ranked.sort_values(
            ["mean_fss", "fss_1km", "iou", "_sort_nearest_distance_to_obs_m", "track_tie_break_order"],
            ascending=[False, False, False, True, True],
        ).reset_index(drop=True)
        ranked["rank"] = np.arange(1, len(ranked) + 1)
        ranked["is_top_ranked"] = ranked["rank"].eq(1)
        return ranked.drop(columns=["_sort_nearest_distance_to_obs_m"])

    @staticmethod
    def _track_registry_row(track: dict) -> dict[str, Any]:
        return {
            "track_id": track["track_id"],
            "track_name": track["track_name"],
            "model_name": track["model_name"],
            "model_family": track["model_family"],
            "transport_model": track["transport_model"],
            "provisional_transport_model": track["provisional_transport_model"],
            "initialization_mode": track["initialization_mode"],
            "current_source": track["current_source"],
            "wind_source": track["wind_source"],
            "wave_stokes_status": track["wave_stokes_status"],
            "structural_limitations": track["structural_limitations"],
            "track_source_kind": track["track_source_kind"],
            "reused_from_phase": track["reused_from_phase"],
            "forecast_path": track["forecast_path"],
            "probability_path": track["probability_path"],
            "source_nc": track["source_nc"],
            "empty_forecast_reason": track["empty_forecast_reason"],
            "forecast_nonzero_cells_from_mask": track["forecast_nonzero_cells_from_mask"],
            "last_active_particle_time_utc": track["last_active_particle_time_utc"],
            "element_count_requested": track["element_count_requested"],
            "element_count_actual": track["element_count_actual"],
            "pygnome_metadata_path": track.get("pygnome_metadata_path", ""),
        }

    def _write_outputs(
        self,
        tracks_df: pd.DataFrame,
        scored_pairings: pd.DataFrame,
        fss_df: pd.DataFrame,
        diagnostics_df: pd.DataFrame,
        summary_df: pd.DataFrame,
        ranking_df: pd.DataFrame,
    ) -> dict[str, Path]:
        artifacts = {
            "tracks_registry_csv": self.output_dir / "march13_14_reinit_crossmodel_tracks_registry.csv",
            "pairing_manifest_csv": self.output_dir / "march13_14_reinit_crossmodel_pairing_manifest.csv",
            "fss_csv": self.output_dir / "march13_14_reinit_crossmodel_fss_by_window.csv",
            "diagnostics_csv": self.output_dir / "march13_14_reinit_crossmodel_diagnostics.csv",
            "summary_csv": self.output_dir / "march13_14_reinit_crossmodel_summary.csv",
            "ranking_csv": self.output_dir / "march13_14_reinit_crossmodel_model_ranking.csv",
        }
        _write_csv(artifacts["tracks_registry_csv"], tracks_df)
        _write_csv(artifacts["pairing_manifest_csv"], scored_pairings)
        _write_csv(artifacts["fss_csv"], fss_df)
        _write_csv(artifacts["diagnostics_csv"], diagnostics_df)
        _write_csv(artifacts["summary_csv"], summary_df)
        _write_csv(artifacts["ranking_csv"], ranking_df)
        return artifacts

    def _write_memo(self, summary_df: pd.DataFrame, ranking_df: pd.DataFrame, artifacts: dict[str, Path]) -> Path:
        path = self.output_dir / "march13_14_reinit_crossmodel_decision_note.md"
        winner = ranking_df.iloc[0] if not ranking_df.empty else None
        lines = [
            "# March 13 -> March 14 NOAA Reinit Cross-Model Comparator",
            "",
            "This appendix-only comparator reuses the completed March 13 -> March 14 OpenDrift reinit outputs, then adds one deterministic PyGNOME surrogate run seeded from the same accepted March 13 NOAA polygon.",
            "",
            "## Guardrails",
            "",
            "- PyGNOME is comparator-only and is never used as truth.",
            "- The completed March 13 -> March 14 OpenDrift reinit outputs are reused and hash-checked rather than modified.",
            "- The frozen strict March 6 outputs and final validation package remain unchanged.",
            f"- Limitation note: {NOAA_SOURCE_LIMITATION_NOTE}",
            "- PyGNOME still does not reproduce the exact OpenDrift gridded current/wave/Stokes stack.",
            "",
            "## Recommendation",
            "",
        ]
        if winner is None:
            lines.append("- No ranked rows were available.")
        else:
            lines.extend(
                [
                    f"- Top-ranked track: {winner['track_id']}",
                    f"- Model: {winner['model_name']}",
                    (
                        f"- Mean FSS / FSS 1/3/5/10 km: {float(winner['mean_fss']):.6f} / "
                        f"{float(winner['fss_1km']):.6f} / {float(winner['fss_3km']):.6f} / "
                        f"{float(winner['fss_5km']):.6f} / {float(winner['fss_10km']):.6f}"
                    ),
                    f"- IoU: {float(winner['iou']):.6f}",
                    f"- Nearest distance to obs (m): {float(winner['nearest_distance_to_obs_m']):.1f}",
                ]
            )
        lines.extend(["", "## Artifacts", ""])
        for key, value in artifacts.items():
            lines.append(f"- {key}: {value}")
        _write_text(path, "\n".join(lines))
        return path

    def _write_qa(self, summary_df: pd.DataFrame) -> dict[str, Path]:
        outputs: dict[str, Path] = {}
        if plt is None:
            return outputs
        for _, row in summary_df.iterrows():
            forecast = self.helper._load_binary_score_mask(Path(str(row["forecast_path"])))
            obs = self.helper._load_binary_score_mask(Path(str(row["observation_path"])))
            fig, ax = plt.subplots(figsize=(7, 7))
            self._render_overlay(ax, forecast, obs, f"March 14 Cross-Model {row['track_id']}")
            fig.tight_layout()
            path = self.qa_dir / f"qa_march14_crossmodel_{row['track_id']}_overlay.png"
            fig.savefig(path, dpi=150, bbox_inches="tight")
            plt.close(fig)
            outputs[f"qa_{row['track_id']}_overlay"] = path
        return outputs

    def _write_manifest(
        self,
        *,
        start_row: pd.Series,
        target_row: pd.Series,
        seed_release: dict,
        upstream: dict,
        tracks: list[dict],
        summary_df: pd.DataFrame,
        ranking_df: pd.DataFrame,
        artifacts: dict[str, Path],
        qa_paths: dict[str, Path],
        memo_path: Path,
    ) -> Path:
        path = self.output_dir / "march13_14_reinit_crossmodel_run_manifest.json"
        payload = {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "phase": PHASE,
            "track": TRACK_LABEL,
            "appendix_only": True,
            "workflow_mode": self.case.workflow_mode,
            "run_name": self.case.run_name,
            "window": {
                "simulation_start_utc": self.window.simulation_start_utc,
                "simulation_end_utc": self.window.simulation_end_utc,
                "forecast_local_dates": list(self.window.forecast_local_dates),
                "scored_target_date": self.window.scored_target_date,
            },
            "guardrails": {
                "pygnome_used_as_truth": False,
                "reused_completed_opendrift_reinit_outputs": True,
                "strict_march6_files_unchanged": True,
                "final_validation_package_unchanged": True,
                "pygnome_exact_forcing_equivalence_claimed": False,
            },
            "selected_start_source": {
                "source_key": str(start_row["source_key"]),
                "source_name": str(start_row["source_name"]),
                "provider": str(start_row.get("provider", "")),
                "obs_date": str(start_row["obs_date"]),
            },
            "selected_target_source": {
                "source_key": str(target_row["source_key"]),
                "source_name": str(target_row["source_name"]),
                "provider": str(target_row.get("provider", "")),
                "obs_date": str(target_row["obs_date"]),
            },
            "seed_release": seed_release,
            "limitations": {
                "appendix_only": True,
                "noaa_source_limitation_note": NOAA_SOURCE_LIMITATION_NOTE,
                "pygnome_structural_mismatch": "PyGNOME deterministic comparator does not reproduce the exact OpenDrift gridded current/wave/Stokes stack.",
            },
            "upstream_reinit": {
                **upstream["required_paths"],
                "hashes_before": self.reused_hashes_before,
                "hashes_after": self._snapshot_paths(self.reused_output_paths),
            },
            "locked_output_hashes_before": self.locked_hashes_before,
            "locked_output_hashes_after": Phase3BExtendedPublicScoredMarch1314ReinitService._snapshot_locked_outputs(self),
            "tracks": tracks,
            "artifacts": {
                **{key: str(value) for key, value in artifacts.items()},
                "memo_md": str(memo_path),
                **{key: str(value) for key, value in qa_paths.items()},
            },
            "score_summary": summary_df.to_dict(orient="records"),
            "ranking": ranking_df.to_dict(orient="records"),
        }
        _write_json(path, payload)
        return path


def run_phase3b_extended_public_scored_march13_14_reinit_pygnome_comparison() -> dict:
    return Phase3BExtendedPublicScoredMarch1314ReinitPyGnomeComparisonService().run()


if __name__ == "__main__":  # pragma: no cover
    result = run_phase3b_extended_public_scored_march13_14_reinit_pygnome_comparison()
    print(json.dumps(result, indent=2, default=_json_default))
