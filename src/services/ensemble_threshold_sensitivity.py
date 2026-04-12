"""Threshold sensitivity for official Mindoro ensemble probability products.

This phase derives honest lower-threshold ensemble masks (for example
``mask_p20``) from existing probability products. It does not relabel or
overwrite the canonical ``mask_p50`` official product.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from src.core.case_context import get_case_context
from src.helpers.metrics import calculate_fss
from src.helpers.raster import GridBuilder, rasterize_particles, save_raster
from src.helpers.scoring import apply_ocean_mask, load_sea_mask_array, precheck_same_grid
from src.services.official_rerun_r1 import OFFICIAL_RERUN_R1_DIR_NAME, _read_json
from src.services.phase3b_multidate_public import (
    format_phase3b_multidate_eventcorridor_label,
    load_phase3b_multidate_validation_dates,
)
from src.services.scoring import OFFICIAL_PHASE3B_WINDOWS_KM, Phase3BScoringService
from src.utils.io import get_case_output_dir, resolve_recipe_selection

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


ENSEMBLE_THRESHOLD_DIR_NAME = "ensemble_threshold_sensitivity"
THRESHOLDS = [0.10, 0.20, 0.30, 0.40, 0.50]
CALIBRATION_DATES = ["2023-03-04", "2023-03-05"]
HOLDOUT_DATE = "2023-03-06"
FORECAST_SKILL_DATES = ["2023-03-04", "2023-03-05", "2023-03-06"]
EVENT_CORRIDOR_LABEL = "2023-03-04_to_2023-03-06"


def _threshold_label(value: float) -> str:
    return f"p{int(round(float(value) * 100)):02d}"


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


def _read_raster(path: Path) -> np.ndarray:
    if rasterio is None:
        raise ImportError("rasterio is required for ensemble_threshold_sensitivity.")
    with rasterio.open(path) as src:
        return src.read(1).astype(np.float32)


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


def select_threshold_from_calibration(
    summary_df: pd.DataFrame,
    calibration_dates: list[str] | None = None,
) -> tuple[pd.DataFrame, dict]:
    """Select threshold from the accepted non-holdout per-date unions."""
    effective_dates = list(calibration_dates or CALIBRATION_DATES)
    calibration = summary_df[
        (summary_df["pair_role"] == "per_date_union")
        & (summary_df["obs_date"].astype(str).isin(effective_dates))
    ].copy()
    rows = []
    for threshold, group in calibration.groupby("threshold"):
        fss_values = []
        for _, row in group.iterrows():
            fss_values.extend(float(row[f"fss_{window}km"]) for window in OFFICIAL_PHASE3B_WINDOWS_KM)
        centroid = pd.to_numeric(group["centroid_distance_m"], errors="coerce")
        area_error = pd.to_numeric(group["area_ratio_forecast_to_obs"], errors="coerce").map(lambda value: abs(value - 1.0))
        rows.append(
            {
                "threshold": float(threshold),
                "threshold_label": _threshold_label(float(threshold)),
                "calibration_dates": ",".join(effective_dates),
                "calibration_mean_fss": float(np.nanmean(fss_values)) if fss_values else 0.0,
                "calibration_mean_centroid_distance_m": float(centroid.mean()) if centroid.notna().any() else np.inf,
                "calibration_mean_area_ratio_error": float(area_error.mean()) if area_error.notna().any() else np.inf,
            }
        )
    ranking = pd.DataFrame(rows)
    if ranking.empty:
        raise RuntimeError("No accepted non-holdout calibration rows were available for threshold selection.")
    ranking = ranking.sort_values(
        ["calibration_mean_fss", "calibration_mean_centroid_distance_m", "calibration_mean_area_ratio_error"],
        ascending=[False, True, True],
    ).reset_index(drop=True)
    ranking["calibration_rank"] = np.arange(1, len(ranking) + 1)
    return ranking, ranking.iloc[0].to_dict()


def recommend_threshold_strategy(
    selected_threshold: float,
    p50_event_mean_fss: float,
    selected_event_mean_fss: float,
    beats_deterministic: bool,
    beats_pygnome: bool,
) -> dict:
    """Return exactly one recommendation for the threshold sensitivity."""
    improvement = selected_event_mean_fss - p50_event_mean_fss
    if abs(selected_threshold - 0.50) < 1e-9:
        recommendation = "keep mask_p50 as the main ensemble product"
        next_branch = "final Phase 3B packaging/reframing"
        reason = "The calibration rule selected p50, so lower thresholding is not supported by the calibration dates."
    elif improvement > 0.01 and beats_deterministic and beats_pygnome:
        recommendation = f"adopt calibrated lower-threshold ensemble footprint {_threshold_label(selected_threshold)} as the main event-scale validation product"
        next_branch = "final Phase 3B packaging/reframing"
        reason = "The selected lower threshold improved event-corridor FSS and outperformed both deterministic comparators."
    elif improvement > 0.01:
        recommendation = f"adopt calibrated lower-threshold ensemble footprint {_threshold_label(selected_threshold)} as the main event-scale validation product"
        next_branch = "final Phase 3B packaging/reframing"
        reason = "The selected lower threshold improved the ensemble event-corridor score versus p50, but comparator limitations remain explicit."
    else:
        recommendation = "conclude that threshold choice is not the main remaining lever"
        next_branch = "final Phase 3B packaging/reframing"
        reason = "The selected threshold did not materially improve the event-corridor score over p50."
    return {
        "recommendation": recommendation,
        "recommended_next_branch": next_branch,
        "reason": reason,
        "eventcorridor_mean_fss_delta_vs_p50": float(improvement),
        "selected_beats_opendrift_deterministic_eventcorridor": bool(beats_deterministic),
        "selected_beats_pygnome_eventcorridor": bool(beats_pygnome),
    }


class EnsembleThresholdSensitivityService:
    def __init__(self):
        self.case = get_case_context()
        if not self.case.is_official:
            raise RuntimeError("ensemble_threshold_sensitivity is only supported for official Mindoro workflows.")
        if xr is None:
            raise ImportError("xarray is required for ensemble_threshold_sensitivity.")
        if rasterio is None:
            raise ImportError("rasterio is required for ensemble_threshold_sensitivity.")

        self.case_output = get_case_output_dir(self.case.run_name)
        self.output_dir = self.case_output / ENSEMBLE_THRESHOLD_DIR_NAME
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.products_dir = self.output_dir / "products"
        self.obs_dir = self.output_dir / "observations"
        self.precheck_dir = self.output_dir / "precheck"
        self.qa_dir = self.output_dir / "qa"
        for path in (self.products_dir, self.obs_dir, self.precheck_dir, self.qa_dir):
            path.mkdir(parents=True, exist_ok=True)

        self.grid = GridBuilder()
        self.sea_mask = load_sea_mask_array(self.grid.spec)
        self.valid_mask = self.sea_mask > 0.5 if self.sea_mask is not None else None
        self.helper = Phase3BScoringService(output_dir=self.output_dir / "_scratch_helper")
        self.validation_dates = load_phase3b_multidate_validation_dates(
            self.case_output,
            fallback_dates=FORECAST_SKILL_DATES,
        )
        self.calibration_dates = [date for date in self.validation_dates if date != HOLDOUT_DATE]
        self.eventcorridor_label = format_phase3b_multidate_eventcorridor_label(self.validation_dates)

    def run(self) -> dict:
        upstream = self._load_upstream_context()
        observations = self._prepare_observations()
        probability_products = self._prepare_probability_products(Path(upstream["model_dir"]))
        threshold_products = self._materialize_threshold_products(probability_products)
        pairings = self._build_pairings(threshold_products, observations)
        scored_pairings, fss_df, diagnostics_df = self._score_pairings(pairings)
        summary_df = self._summarize(scored_pairings, fss_df, diagnostics_df)
        calibration_ranking, selected = select_threshold_from_calibration(
            summary_df,
            calibration_dates=self.calibration_dates,
        )
        comparator = self._load_comparator_eventcorridor_scores()
        recommendation = self._build_recommendation(summary_df, selected, comparator)
        qa_paths = self._write_qa(summary_df, selected)
        paths = self._write_outputs(scored_pairings, fss_df, diagnostics_df, summary_df, calibration_ranking)
        report_path = self._write_report(summary_df, calibration_ranking, selected, comparator, recommendation, paths, qa_paths)
        manifest_path = self._write_manifest(
            upstream=upstream,
            probability_products=probability_products,
            threshold_products=threshold_products,
            observations=observations,
            calibration_ranking=calibration_ranking,
            selected=selected,
            comparator=comparator,
            recommendation=recommendation,
            paths=paths,
            qa_paths=qa_paths,
            report_path=report_path,
        )
        return {
            "output_dir": self.output_dir,
            "summary": summary_df,
            "calibration_ranking": calibration_ranking,
            "selected_threshold": float(selected["threshold"]),
            "selected_threshold_label": str(selected["threshold_label"]),
            "comparator": comparator,
            "recommendation": recommendation,
            "paths": paths,
            "report_md": report_path,
            "run_manifest": manifest_path,
        }

    def _load_upstream_context(self) -> dict:
        official_manifest_path = self.case_output / OFFICIAL_RERUN_R1_DIR_NAME / "official_rerun_r1_run_manifest.json"
        official_manifest = _read_json(official_manifest_path)
        model_dir = Path((official_manifest.get("model_result") or {}).get("model_dir", ""))
        if not model_dir.exists():
            raise FileNotFoundError(f"official_rerun_r1 model_dir not found: {model_dir}")
        if str((official_manifest.get("selected_retention_config") or {}).get("selected_mode")) != "R1":
            raise RuntimeError("ensemble_threshold_sensitivity requires official_rerun_r1 selected retention mode R1.")
        return {
            "official_rerun_r1_manifest_path": str(official_manifest_path),
            "official_rerun_r1_manifest": official_manifest,
            "model_dir": str(model_dir),
            "forecast_manifest_path": str(model_dir / "forecast" / "forecast_manifest.json"),
            "ensemble_manifest_path": str(model_dir / "ensemble" / "ensemble_manifest.json"),
            "provenance": official_manifest.get("provenance", {}),
            "selected_retention_config": official_manifest.get("selected_retention_config", {}),
        }

    def _prepare_observations(self) -> dict[str, Any]:
        strict = Path("data") / "arcgis" / self.case.run_name / "obs_mask_2023-03-06.tif"
        if not strict.exists():
            raise FileNotFoundError(f"Strict March 6 observed mask missing: {strict}")
        date_union_dir = self.case_output / "phase3b_multidate_public" / "date_union_obs_masks"
        date_unions = {}
        for date in self.validation_dates:
            path = date_union_dir / f"obs_union_{date}.tif"
            if not path.exists():
                raise FileNotFoundError(f"Accepted date-union observed mask missing: {path}")
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

    def _prepare_probability_products(self, model_dir: Path) -> dict[str, Path]:
        ensemble_dir = model_dir / "ensemble"
        products: dict[str, Path] = {}
        for date in self.validation_dates:
            source = ensemble_dir / f"prob_presence_{date}_datecomposite.tif"
            dest = self.products_dir / f"prob_presence_{date}_datecomposite.tif"
            if source.exists():
                data = apply_ocean_mask(_read_raster(source), sea_mask=self.sea_mask, fill_value=0.0)
                save_raster(self.grid, data.astype(np.float32), dest)
            else:
                self._rebuild_probability_datecomposite(model_dir, date, dest)
            products[date] = dest
        return products

    def _rebuild_probability_datecomposite(self, model_dir: Path, date: str, out_path: Path) -> None:
        member_paths = sorted((model_dir / "ensemble").glob("member_*.nc"))
        if not member_paths:
            raise FileNotFoundError(f"Cannot rebuild probability date-composite for {date}; no member NetCDFs found.")
        member_masks = [self._member_utc_date_mask(path, date) for path in member_paths]
        probability = np.mean(np.stack(member_masks, axis=0), axis=0).astype(np.float32)
        probability = apply_ocean_mask(probability, sea_mask=self.sea_mask, fill_value=0.0)
        save_raster(self.grid, probability.astype(np.float32), out_path)

    def _member_utc_date_mask(self, nc_path: Path, target_date: str) -> np.ndarray:
        target = pd.Timestamp(target_date).date()
        composite = np.zeros((self.grid.height, self.grid.width), dtype=np.float32)
        with xr.open_dataset(nc_path) as ds:
            times = pd.DatetimeIndex(pd.to_datetime(ds["time"].values))
            if times.tz is not None:
                times = times.tz_convert("UTC").tz_localize(None)
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
                composite = np.maximum(composite, hits.astype(np.float32))
        return apply_ocean_mask(composite, sea_mask=self.sea_mask, fill_value=0.0)

    def _materialize_threshold_products(self, probabilities: dict[str, Path]) -> dict[float, dict[str, Any]]:
        products: dict[float, dict[str, Any]] = {}
        for threshold in THRESHOLDS:
            label = _threshold_label(threshold)
            threshold_dir = self.products_dir / label
            threshold_dir.mkdir(parents=True, exist_ok=True)
            date_masks: dict[str, Path] = {}
            for date, probability_path in probabilities.items():
                probability = _read_raster(probability_path)
                mask = apply_ocean_mask((probability >= threshold).astype(np.float32), sea_mask=self.sea_mask, fill_value=0.0)
                out_path = threshold_dir / f"mask_{label}_{date}_datecomposite.tif"
                save_raster(self.grid, mask.astype(np.float32), out_path)
                date_masks[date] = out_path
            event_path = self._build_eventcorridor_model_union(
                date_masks,
                threshold_dir / f"eventcorridor_model_union_{label}_{self.eventcorridor_label}.tif",
            )
            products[threshold] = {"threshold_label": label, "date_masks": date_masks, "eventcorridor": event_path}
        return products

    def _build_eventcorridor_model_union(self, date_masks: dict[str, Path], out_path: Path) -> Path:
        union = np.zeros((self.grid.height, self.grid.width), dtype=np.float32)
        for date in self.validation_dates:
            union = np.maximum(union, self.helper._load_binary_score_mask(date_masks[date]))
        union = apply_ocean_mask(union, sea_mask=self.sea_mask, fill_value=0.0)
        save_raster(self.grid, union.astype(np.float32), out_path)
        return out_path

    def _build_pairings(self, threshold_products: dict[float, dict[str, Any]], observations: dict[str, Any]) -> pd.DataFrame:
        rows: list[dict] = []
        for threshold, products in threshold_products.items():
            label = products["threshold_label"]
            rows.append(
                self._pair_record(
                    threshold=threshold,
                    threshold_label=label,
                    pair_role="strict_march6",
                    obs_date=HOLDOUT_DATE,
                    forecast_path=products["date_masks"][HOLDOUT_DATE],
                    observation_path=observations["strict_march6"],
                    source_semantics=f"strict_march6_mask_{label}_vs_fixed_obsmask",
                )
            )
            for date in self.validation_dates:
                rows.append(
                    self._pair_record(
                        threshold=threshold,
                        threshold_label=label,
                        pair_role="per_date_union",
                        obs_date=date,
                        forecast_path=products["date_masks"][date],
                        observation_path=observations["date_unions"][date],
                        source_semantics=f"per_date_union_{date}_mask_{label}_vs_public_obs",
                    )
                )
            rows.append(
                self._pair_record(
                    threshold=threshold,
                    threshold_label=label,
                    pair_role="eventcorridor_march4_6",
                    obs_date=self.eventcorridor_label,
                    forecast_path=products["eventcorridor"],
                    observation_path=observations["eventcorridor_march4_6"],
                    source_semantics=f"eventcorridor_mask_{label}_vs_public_obs_union_excluding_march3",
                )
            )
        return pd.DataFrame(rows)

    def _pair_record(
        self,
        *,
        threshold: float,
        threshold_label: str,
        pair_role: str,
        obs_date: str,
        forecast_path: Path,
        observation_path: Path,
        source_semantics: str,
    ) -> dict:
        return {
            "model_branch": "B_observation_initialized_polygon_R1",
            "initialization_mode": "observation_initialized_polygon",
            "selected_retention_mode": "R1",
            "coastline_action": "previous",
            "threshold": float(threshold),
            "threshold_label": threshold_label,
            "threshold_product_semantics": f"ensemble probability >= {float(threshold):.2f}",
            "mask_p50_semantics_unchanged": True,
            "calibration_selected": False,
            "pair_id": f"{threshold_label}_{pair_role}_{obs_date}",
            "pair_role": pair_role,
            "obs_date": obs_date,
            "forecast_product": Path(forecast_path).name,
            "forecast_path": str(forecast_path),
            "observation_product": Path(observation_path).name,
            "observation_path": str(observation_path),
            "metric": "FSS",
            "windows_km": ",".join(str(value) for value in OFFICIAL_PHASE3B_WINDOWS_KM),
            "source_semantics": source_semantics,
            "truth_source": "accepted_public_observation_derived_mask",
            "march3_counted_as_forecast_skill": False,
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
                raise RuntimeError(f"Ensemble threshold same-grid precheck failed for {row['pair_id']}: {precheck.json_report_path}")
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
                        **scored,
                        "window_km": int(window_km),
                        "window_cells": int(self.helper._window_km_to_cells(window_km)),
                        "fss": fss,
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

    def _load_comparator_eventcorridor_scores(self) -> dict:
        path = self.case_output / "pygnome_public_comparison" / "pygnome_public_comparison_summary.csv"
        if not path.exists():
            return {
                "source": "",
                "opendrift_deterministic_eventcorridor_mean_fss": np.nan,
                "pygnome_eventcorridor_mean_fss": np.nan,
                "notes": "pygnome_public_comparison summary was not available.",
            }
        summary = pd.read_csv(path)
        event = summary[summary["pair_role"].astype(str) == "eventcorridor_march4_6"].copy()

        def track_mean(track_id: str) -> float:
            rows = event[event["track_id"].astype(str) == track_id]
            if rows.empty:
                return np.nan
            return _mean_fss(rows.iloc[0])

        return {
            "source": str(path),
            "opendrift_deterministic_eventcorridor_mean_fss": track_mean("C1"),
            "opendrift_ensemble_p50_eventcorridor_mean_fss": track_mean("C2"),
            "pygnome_eventcorridor_mean_fss": track_mean("C3"),
            "notes": "Comparator scores are reused from the public-observation PyGNOME/OpenDrift comparison.",
        }

    def _build_recommendation(self, summary_df: pd.DataFrame, selected: dict, comparator: dict) -> dict:
        selected_threshold = float(selected["threshold"])
        event_rows = summary_df[summary_df["pair_role"] == "eventcorridor_march4_6"].copy()
        selected_event = event_rows[event_rows["threshold"].astype(float) == selected_threshold]
        p50_event = event_rows[np.isclose(event_rows["threshold"].astype(float), 0.50)]
        selected_event_mean = _mean_fss(selected_event.iloc[0]) if not selected_event.empty else 0.0
        p50_event_mean = _mean_fss(p50_event.iloc[0]) if not p50_event.empty else 0.0
        deterministic = float(comparator.get("opendrift_deterministic_eventcorridor_mean_fss", np.nan))
        pygnome = float(comparator.get("pygnome_eventcorridor_mean_fss", np.nan))
        beats_deterministic = bool(np.isfinite(deterministic) and selected_event_mean > deterministic)
        beats_pygnome = bool(np.isfinite(pygnome) and selected_event_mean > pygnome)
        recommendation = recommend_threshold_strategy(
            selected_threshold=selected_threshold,
            p50_event_mean_fss=p50_event_mean,
            selected_event_mean_fss=selected_event_mean,
            beats_deterministic=beats_deterministic,
            beats_pygnome=beats_pygnome,
        )
        recommendation.update(
            {
                "selected_threshold": selected_threshold,
                "selected_threshold_label": _threshold_label(selected_threshold),
                "selected_eventcorridor_mean_fss": selected_event_mean,
                "p50_eventcorridor_mean_fss": p50_event_mean,
                "opendrift_deterministic_eventcorridor_mean_fss": deterministic,
                "pygnome_eventcorridor_mean_fss": pygnome,
                "strict_march6_not_used_for_selection": True,
                "calibration_dates": self.calibration_dates,
                "holdout_date": HOLDOUT_DATE,
            }
        )
        return recommendation

    def _write_outputs(
        self,
        pairings: pd.DataFrame,
        fss_df: pd.DataFrame,
        diagnostics_df: pd.DataFrame,
        summary_df: pd.DataFrame,
        calibration_ranking: pd.DataFrame,
    ) -> dict[str, Path]:
        paths = {
            "pairing_manifest": self.output_dir / "ensemble_threshold_sensitivity_pairing_manifest.csv",
            "summary": self.output_dir / "ensemble_threshold_sensitivity_summary.csv",
            "by_date_window": self.output_dir / "ensemble_threshold_sensitivity_by_date_window.csv",
            "diagnostics": self.output_dir / "ensemble_threshold_sensitivity_diagnostics.csv",
            "calibration_ranking": self.output_dir / "ensemble_threshold_calibration_ranking.csv",
        }
        _write_csv(paths["pairing_manifest"], pairings)
        _write_csv(paths["summary"], summary_df)
        _write_csv(paths["by_date_window"], fss_df)
        _write_csv(paths["diagnostics"], diagnostics_df)
        _write_csv(paths["calibration_ranking"], calibration_ranking)
        return paths

    def _write_report(
        self,
        summary_df: pd.DataFrame,
        calibration_ranking: pd.DataFrame,
        selected: dict,
        comparator: dict,
        recommendation: dict,
        paths: dict[str, Path],
        qa_paths: dict[str, Path],
    ) -> Path:
        path = self.output_dir / "ensemble_threshold_calibration_report.md"
        selected_threshold = float(selected["threshold"])
        strict = summary_df[
            (summary_df["pair_role"] == "strict_march6")
            & np.isclose(summary_df["threshold"].astype(float), selected_threshold)
        ]
        event = summary_df[summary_df["pair_role"] == "eventcorridor_march4_6"].copy()
        lines = [
            "# Ensemble Threshold Sensitivity",
            "",
            "This study derives thresholded ensemble footprints from existing R1 probability products. It does not overwrite or relabel canonical `mask_p50` products.",
            "",
            f"- Selected threshold from accepted non-holdout calibration dates `{', '.join(self.calibration_dates)}`: `{selected['threshold_label']}` ({selected_threshold:.2f})",
            f"- Selection rule: maximize mean FSS over `{', '.join(self.calibration_dates)}` across 1/3/5/10 km; tie-break by centroid distance, then area-ratio closeness to 1.",
            "- Strict March 6 was not used for threshold selection.",
            f"- Recommendation: {recommendation['recommendation']}",
            f"- Next branch: {recommendation['recommended_next_branch']}",
            "",
            "## Calibration Ranking",
            "",
        ]
        lines.extend(self._markdown_table(calibration_ranking))
        lines.extend(["", "## Holdout Strict March 6", ""])
        if not strict.empty:
            lines.extend(self._markdown_table(strict[self._summary_columns()]))
        else:
            lines.append("No selected-threshold strict March 6 row was available.")
        lines.extend(["", f"## {self.eventcorridor_label} Event Corridor by Threshold", ""])
        lines.extend(self._markdown_table(event[self._summary_columns()]))
        lines.extend(
            [
                "",
                "## Comparator Event-Corridor Scores",
                "",
                f"- OpenDrift deterministic mean FSS: {comparator.get('opendrift_deterministic_eventcorridor_mean_fss')}",
                f"- OpenDrift ensemble p50 mean FSS: {comparator.get('opendrift_ensemble_p50_eventcorridor_mean_fss')}",
                f"- PyGNOME deterministic mean FSS: {comparator.get('pygnome_eventcorridor_mean_fss')}",
                "",
                "## Artifacts",
            ]
        )
        for key, artifact in paths.items():
            lines.append(f"- {key}: {artifact}")
        for key, artifact in qa_paths.items():
            lines.append(f"- {key}: {artifact}")
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return path

    @staticmethod
    def _summary_columns() -> list[str]:
        return [
            "threshold_label",
            "pair_role",
            "obs_date",
            "forecast_nonzero_cells",
            "obs_nonzero_cells",
            "area_ratio_forecast_to_obs",
            "centroid_distance_m",
            "nearest_distance_to_obs_m",
            "iou",
            "dice",
            "fss_1km",
            "fss_3km",
            "fss_5km",
            "fss_10km",
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

    def _write_manifest(
        self,
        upstream: dict,
        probability_products: dict[str, Path],
        threshold_products: dict[float, dict[str, Any]],
        observations: dict[str, Any],
        calibration_ranking: pd.DataFrame,
        selected: dict,
        comparator: dict,
        recommendation: dict,
        paths: dict[str, Path],
        qa_paths: dict[str, Path],
        report_path: Path,
    ) -> Path:
        manifest_path = self.output_dir / "ensemble_threshold_calibration_manifest.json"
        provenance = upstream.get("provenance") or {}
        payload = {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "workflow_mode": self.case.workflow_mode,
            "run_name": self.case.run_name,
            "phase": ENSEMBLE_THRESHOLD_DIR_NAME,
            "purpose": "threshold calibration/sensitivity for existing R1 ensemble probability products",
            "guardrails": {
                "existing_mask_p50_products_not_overwritten": True,
                "lower_threshold_products_not_called_p50": True,
                "strict_march6_official_pair_unchanged": True,
                "no_new_public_sources_added": True,
                "model_not_rerun": True,
                "march3_not_counted_as_forecast_skill": True,
            },
            "model_branch": "B_observation_initialized_polygon",
            "initialization_mode": "observation_initialized_polygon",
            "selected_retention_mode": "R1",
            "retention_configuration": upstream.get("selected_retention_config", {}),
            "recipe_used": provenance.get("recipe_used", resolve_recipe_selection().recipe),
            "element_count_used": provenance.get("element_count_used", ""),
            "shoreline_mask_signature": provenance.get("shoreline_mask_signature", self.grid.spec.shoreline_mask_signature),
            "forcing_manifest_paths": provenance.get("forcing_paths", {}),
            "thresholds": THRESHOLDS,
            "threshold_labels": [_threshold_label(value) for value in THRESHOLDS],
            "calibration_rule": {
                "calibration_dates": self.calibration_dates,
                "holdout_date": HOLDOUT_DATE,
                "primary_objective": f"maximize mean FSS over {', '.join(self.calibration_dates)} across 1/3/5/10 km",
                "tie_breaker_1": "smaller mean centroid distance",
                "tie_breaker_2": "area-ratio closeness to 1.0",
                "strict_march6_used_for_selection": False,
            },
            "selected_threshold": selected,
            "calibration_ranking": calibration_ranking.to_dict(orient="records"),
            "comparator_eventcorridor_scores": comparator,
            "recommendation": recommendation,
            "probability_products": {date: str(path) for date, path in probability_products.items()},
            "threshold_products": {
                _threshold_label(threshold): {
                    "date_masks": {date: str(path) for date, path in products["date_masks"].items()},
                    "eventcorridor": str(products["eventcorridor"]),
                }
                for threshold, products in threshold_products.items()
            },
            "observations": {
                "strict_march6": str(observations["strict_march6"]),
                "date_unions": {date: str(path) for date, path in observations["date_unions"].items()},
                "eventcorridor_march4_6": str(observations["eventcorridor_march4_6"]),
            },
            "upstream": upstream,
            "artifacts": {
                **{key: str(value) for key, value in paths.items()},
                **{key: str(value) for key, value in qa_paths.items()},
                "report": str(report_path),
            },
        }
        _write_json(manifest_path, payload)
        return manifest_path

    def _write_qa(self, summary_df: pd.DataFrame, selected: dict) -> dict[str, Path]:
        outputs: dict[str, Path] = {}
        if plt is None:
            return outputs
        sweep = self._plot_threshold_sweep(summary_df, selected)
        overlay = self._plot_eventcorridor_overlay(summary_df, selected)
        if sweep:
            outputs["qa_ensemble_threshold_sweep"] = sweep
        if overlay:
            outputs["qa_ensemble_threshold_eventcorridor_overlay"] = overlay
        return outputs

    def _plot_threshold_sweep(self, summary_df: pd.DataFrame, selected: dict) -> Path | None:
        event = summary_df[summary_df["pair_role"] == "eventcorridor_march4_6"].copy()
        calibration, _ = select_threshold_from_calibration(summary_df)
        if event.empty or calibration.empty:
            return None
        path = self.output_dir / "qa_ensemble_threshold_sweep.png"
        event["mean_fss"] = event.apply(_mean_fss, axis=1)
        event = event.sort_values("threshold")
        calibration = calibration.sort_values("threshold")
        fig, axes = plt.subplots(1, 2, figsize=(12, 5))
        axes[0].plot(calibration["threshold_label"], calibration["calibration_mean_fss"], marker="o")
        axes[0].axvline(selected["threshold_label"], color="red", linestyle="--", alpha=0.6)
        axes[0].set_title("Calibration mean FSS (Mar 4-5)")
        axes[0].set_ylabel("mean FSS")
        axes[0].grid(alpha=0.2)
        axes[1].plot(event["threshold_label"], event["mean_fss"], marker="o", color="#2ca02c")
        axes[1].axvline(selected["threshold_label"], color="red", linestyle="--", alpha=0.6)
        axes[1].set_title("Event-corridor mean FSS (Mar 4-6)")
        axes[1].grid(alpha=0.2)
        fig.tight_layout()
        fig.savefig(path, dpi=150, bbox_inches="tight")
        plt.close(fig)
        return path

    def _plot_eventcorridor_overlay(self, summary_df: pd.DataFrame, selected: dict) -> Path | None:
        selected_label = str(selected["threshold_label"])
        selected_row = summary_df[
            (summary_df["pair_role"] == "eventcorridor_march4_6")
            & (summary_df["threshold_label"].astype(str) == selected_label)
        ]
        p50_row = summary_df[
            (summary_df["pair_role"] == "eventcorridor_march4_6")
            & (summary_df["threshold_label"].astype(str) == "p50")
        ]
        if selected_row.empty or p50_row.empty:
            return None
        path = self.output_dir / "qa_ensemble_threshold_eventcorridor_overlay.png"
        rows = [("selected " + selected_label, selected_row.iloc[0]), ("p50 reference", p50_row.iloc[0])]
        fig, axes = plt.subplots(1, 2, figsize=(10, 5))
        for ax, (title, row) in zip(axes, rows):
            forecast = self.helper._load_binary_score_mask(Path(row["forecast_path"]))
            obs = self.helper._load_binary_score_mask(Path(row["observation_path"]))
            self._render_overlay(ax, forecast, obs, title)
        fig.tight_layout()
        fig.savefig(path, dpi=150, bbox_inches="tight")
        plt.close(fig)
        return path

    @staticmethod
    def _render_overlay(ax, forecast: np.ndarray, obs: np.ndarray, title: str) -> None:
        canvas = np.ones((forecast.shape[0], forecast.shape[1], 3), dtype=np.float32)
        canvas[obs > 0] = [0.1, 0.35, 0.95]
        canvas[forecast > 0] = [0.95, 0.35, 0.1]
        canvas[(forecast > 0) & (obs > 0)] = [0.1, 0.65, 0.25]
        ax.imshow(canvas, origin="upper")
        ax.set_title(title)
        ax.set_axis_off()


def run_ensemble_threshold_sensitivity() -> dict:
    return EnsembleThresholdSensitivityService().run()


if __name__ == "__main__":  # pragma: no cover
    print(json.dumps(run_ensemble_threshold_sensitivity(), indent=2, default=_json_default))
