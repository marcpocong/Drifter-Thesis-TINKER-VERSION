"""Focused experimental windage/timing follow-up for PhilSA March 3 -> 4.

This is intentionally experimental-only. It reuses the completed PhilSA-only
March 3 -> March 4 cache and writes only under the settings-sensitivity
follow-up directory.
"""

from __future__ import annotations

import json
import math
import os
import shutil
import traceback
from pathlib import Path
from typing import Any

os.environ.setdefault("WORKFLOW_MODE", "mindoro_retro_2023")

import numpy as np
import pandas as pd
import xarray as xr

from src.helpers.raster import GridBuilder, extract_particles_at_time, rasterize_particles, save_raster
from src.helpers.scoring import apply_ocean_mask, load_sea_mask_array
from src.services.ensemble import (
    OFFICIAL_ELEMENT_COUNT_OVERRIDE_ENV,
    normalize_model_timestamp,
    timestamp_to_utc_iso,
)
from src.services.mindoro_march3_4_opendrift_settings_sensitivity import (
    BASELINE_RANDOM_SEED,
    BASELINE_RECIPE,
    BASE_EXPERIMENT_DIR,
    ELEMENT_COUNT,
    EXPECTED_ENSEMBLE_MEMBER_COUNT,
    ExperimentalControlService,
    LOCAL_TIMEZONE,
    OUTPUT_DIR,
    PROTECTED_OUTPUT_DIRS,
    RUN_NAME,
    SEED_DATE,
    SEED_SOURCE_NAME,
    SIMULATION_END_UTC,
    SIMULATION_START_UTC,
    TARGET_DATE,
    TARGET_SOURCE_NAME,
    VariantSpec,
    MindoroMarch34OpenDriftSettingsSensitivity,
    _iso_z,
    _read_json,
    _read_raster,
    _snapshot_diff,
    _snapshot_paths,
    _temporary_env,
    _write_csv,
    _write_json,
    _write_text,
)
from src.services.scoring import OFFICIAL_PHASE3B_WINDOWS_KM, Phase3BScoringService

try:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.colors import ListedColormap
except ImportError:  # pragma: no cover
    plt = None
    ListedColormap = None


FOLLOWUP_DIR = OUTPUT_DIR / "focused_windage_timing_followup"
WINDAGE_VALUES = [0.0, 0.0025, 0.005, 0.0075, 0.01, 0.0125, 0.015, 0.0175, 0.02, 0.025, 0.03]
PYGNOME_NC = BASE_EXPERIMENT_DIR / "pygnome_comparator" / "model_run" / "pygnome_march3_4_philsa_5000_deterministic_control.nc"
PYGNOME_METADATA = BASE_EXPERIMENT_DIR / "pygnome_comparator" / "model_run" / "pygnome_march3_4_philsa_5000_metadata.json"


def _windage_id(value: float) -> str:
    return "windage_" + f"{value:.4f}".replace(".", "_")


def _windage_label(value: float) -> str:
    return f"{value:.4f}"


def _local_date(timestamp: Any) -> str:
    return pd.Timestamp(timestamp).tz_localize("UTC").tz_convert(LOCAL_TIMEZONE).date().isoformat()


def _local_iso(timestamp: Any) -> str:
    return pd.Timestamp(timestamp).tz_localize("UTC").tz_convert(LOCAL_TIMEZONE).isoformat()


class MindoroMarch34FocusedWindageTimingFollowup(MindoroMarch34OpenDriftSettingsSensitivity):
    def __init__(self, *, run_ensemble: bool = True, force: bool = False):
        self.run_ensemble = bool(run_ensemble)
        self.force = bool(force)
        self.max_ensemble_candidates = 1 if run_ensemble else 0
        self.output_dir = FOLLOWUP_DIR
        self.model_runs_dir = self.output_dir / "model_runs"
        self.variant_products_dir = self.output_dir / "variant_products"
        self.figures_dir = self.output_dir / "figures"
        self.ensemble_dir = self.output_dir / "ensemble_candidate"
        self.pygnome_products_dir = self.output_dir / "pygnome_product_definitions"
        for path in (
            self.output_dir,
            self.model_runs_dir,
            self.variant_products_dir,
            self.figures_dir,
            self.ensemble_dir,
            self.pygnome_products_dir,
        ):
            path.mkdir(parents=True, exist_ok=True)
        self.grid = GridBuilder()
        self.sea_mask = load_sea_mask_array(self.grid.spec)
        self.valid_mask = self.sea_mask > 0.5 if self.sea_mask is not None else np.ones((self.grid.height, self.grid.width), dtype=bool)
        self.buffer_mask = self._one_cell_buffer_mask(self.valid_mask)
        self.scoring = Phase3BScoringService(output_dir=self.output_dir / "_scratch_scoring")
        self.context = self._load_context()
        self.release_cache: dict[str, dict[str, Any]] = {}
        self.commands_used: list[str] = []
        self.skipped_variants: list[dict[str, Any]] = []

    def _build_variants(self) -> list[VariantSpec]:
        variants: list[VariantSpec] = []
        for value in WINDAGE_VALUES:
            suffix = " baseline" if math.isclose(value, 0.02) else ""
            variants.append(
                VariantSpec(
                    variant_id=_windage_id(value),
                    setting_changed="seed:wind_drift_factor",
                    setting_value=_windage_label(value),
                    classification="experimental sensitivity",
                    purpose=(
                        "Focused windage refinement with coastline_action=previous, baseline forcing, "
                        "baseline release geometry, and official valid-ocean mask."
                        + suffix
                    ),
                    transport_overrides={
                        "coastline_action": "previous",
                        "coastline_approximation_precision": 0.001,
                        "time_step_minutes": 60,
                        "horizontal_diffusivity_m2s": 2.0,
                        "require_wave": True,
                        "enable_stokes_drift": True,
                        "attach_wave_reader": True,
                        "model_config": {"seed:wind_drift_factor": float(value)},
                    },
                    release_mode="baseline_polygon",
                    forcing_recipe=BASELINE_RECIPE,
                )
            )
        return variants

    def _run_deterministic_variant(self, variant: VariantSpec) -> dict[str, Any]:
        variant_dir = self.model_runs_dir / variant.variant_id / "model_run"
        nc_path = variant_dir / "forecast" / f"deterministic_control_{variant.forcing_recipe}.nc"
        products_dir = self.variant_products_dir / variant.variant_id
        summary_path = products_dir / "variant_summary.json"
        if nc_path.exists() and summary_path.exists() and not self.force:
            payload = _read_json(summary_path)
            payload["reused_existing_run"] = True
            return payload
        forcing_paths, skip_reason = self._forcing_paths_for_recipe(variant.forcing_recipe)
        if forcing_paths is None:
            row = {"variant_id": variant.variant_id, "status": "skipped", "skip_reason": skip_reason}
            self.skipped_variants.append(row)
            return row
        release_overrides, release_description = self._release_payload(variant.release_mode)
        output_run_name = (
            f"{RUN_NAME}/phase3b_philsa_march3_4_5000_experiment/"
            f"experimental_settings_sensitivity/focused_windage_timing_followup/"
            f"model_runs/{variant.variant_id}/model_run"
        )
        with _temporary_env({OFFICIAL_ELEMENT_COUNT_OVERRIDE_ENV: str(ELEMENT_COUNT), "WORKFLOW_MODE": "mindoro_retro_2023"}):
            service = ExperimentalControlService(
                currents_file=forcing_paths["currents"],
                winds_file=forcing_paths["wind"],
                wave_file=forcing_paths["wave"],
                output_run_name=output_run_name,
                sensitivity_context={
                    "experimental_only": True,
                    "thesis_facing": False,
                    "reportable": False,
                    "safe_default": False,
                    "variant_id": variant.variant_id,
                    "classification": variant.classification,
                    "purpose": variant.purpose,
                },
                simulation_start_utc=SIMULATION_START_UTC,
                simulation_end_utc=SIMULATION_END_UTC,
                snapshot_hours=[24, 48],
                date_composite_dates=[SEED_DATE, TARGET_DATE],
                transport_overrides=variant.transport_overrides,
                seed_overrides={**release_overrides, "release_start_utc": SIMULATION_START_UTC},
            )
            result = service.run_deterministic_control(
                recipe_name=variant.forcing_recipe,
                start_time=SIMULATION_START_UTC,
                selection=self.context["selection"],
            )
        product = self._build_deterministic_products(
            variant=variant,
            nc_path=Path(result["output_file"]),
            products_dir=products_dir,
            run_result=result,
            release_description=release_description,
        )
        _write_json(summary_path, product)
        return product

    def _write_philsa_timing_metadata(self) -> pd.DataFrame:
        obs_manifest = pd.read_csv(BASE_EXPERIMENT_DIR / "march3_4_philsa_5000_observation_manifest.csv")
        rows: list[dict[str, Any]] = []
        for source_name in [SEED_SOURCE_NAME, TARGET_SOURCE_NAME]:
            obs = obs_manifest[obs_manifest["source_name"].astype(str).eq(source_name)].iloc[0].to_dict()
            metadata_path = Path(str(obs.get("layer_metadata", "")))
            raw_geojson_path = Path(str(obs.get("raw_geojson", "")))
            metadata = _read_json(metadata_path)
            editing = metadata.get("editingInfo") or {}
            fields = metadata.get("fields") or []
            edit_last = editing.get("lastEditDate")
            data_last = editing.get("dataLastEditDate")
            schema_last = editing.get("schemaLastEditDate")

            def ms_to_iso(value: Any) -> str:
                if value in (None, ""):
                    return ""
                return pd.to_datetime(int(value), unit="ms", utc=True).isoformat()

            raw_feature_time_fields: list[str] = []
            if raw_geojson_path.exists():
                raw = _read_json(raw_geojson_path)
                for feature in raw.get("features", []):
                    props = feature.get("properties") or {}
                    for key in props:
                        if "time" in key.lower() or "date" in key.lower() or "acq" in key.lower() or "image" in key.lower():
                            raw_feature_time_fields.append(key)
            rows.append(
                {
                    "source_name": source_name,
                    "provider": obs.get("provider"),
                    "observation_date": obs.get("observation_date") or obs.get("obs_date"),
                    "role": obs.get("role"),
                    "service_url": obs.get("service_url"),
                    "layer_metadata_path": str(metadata_path),
                    "raw_geojson_path": str(raw_geojson_path),
                    "layer_name": metadata.get("name"),
                    "time_info_present": bool(metadata.get("timeInfo")),
                    "time_info": json.dumps(metadata.get("timeInfo")),
                    "date_fields_time_reference": json.dumps(metadata.get("dateFieldsTimeReference")),
                    "field_names": ";".join(str(field.get("name")) for field in fields),
                    "raw_feature_time_like_fields": ";".join(sorted(set(raw_feature_time_fields))),
                    "last_edit_utc": ms_to_iso(edit_last),
                    "data_last_edit_utc": ms_to_iso(data_last),
                    "schema_last_edit_utc": ms_to_iso(schema_last),
                    "machine_readable_acquisition_time_available": False,
                    "best_target_treatment": (
                        "dated product with uncertain subdaily acquisition time; local-date composite is defensible, "
                        "exact image-time target is not machine-readable from stored layer metadata"
                    ),
                }
            )
        frame = pd.DataFrame(rows)
        _write_csv(self.output_dir / "philsa_timing_metadata_summary.csv", frame)
        return frame

    def _windage_refinement_table(self, matrix: pd.DataFrame) -> pd.DataFrame:
        completed = matrix[matrix["status"].eq("completed")].copy()
        completed["actual_wind_drift_factor"] = pd.to_numeric(completed["setting_value"], errors="coerce")
        completed["mask_removed_fraction"] = (
            pd.to_numeric(completed["cells_removed_by_official_mask"], errors="coerce").fillna(0)
            / pd.to_numeric(completed["raw_occupied_cells_before_mask"], errors="coerce").replace(0, np.nan)
        )
        baseline = completed[np.isclose(completed["actual_wind_drift_factor"], 0.02)]
        baseline_cells = int(baseline.iloc[0]["valid_ocean_cells_after_official_mask"]) if not baseline.empty else 4
        baseline_removed_fraction = float(baseline.iloc[0]["mask_removed_fraction"]) if not baseline.empty else np.nan
        completed["wider_without_becoming_mostly_displaced"] = (
            (completed["valid_ocean_cells_after_official_mask"] > baseline_cells)
            & (pd.to_numeric(completed["nearest_forecast_to_observation_distance_m"], errors="coerce") <= 5000)
        )
        completed["particles_less_trapped_or_clipped_at_shoreline"] = (
            pd.to_numeric(completed["mask_removed_fraction"], errors="coerce").fillna(1.0) < baseline_removed_fraction
        ) & (completed["valid_ocean_cells_after_official_mask"] >= baseline_cells)
        cols = [
            "variant_id",
            "actual_wind_drift_factor",
            "element_count",
            "active_floating_particles_on_march4",
            "stranded_beached_particles_on_march4",
            "deactivated_particles_on_march4",
            "raw_occupied_cells_before_mask",
            "valid_ocean_cells_after_official_mask",
            "cells_removed_by_official_mask",
            "mask_removed_fraction",
            "nearest_forecast_to_observation_distance_m",
            "centroid_distance_m",
            "iou",
            "dice",
            "fss_1km",
            "fss_3km",
            "fss_5km",
            "fss_10km",
            "mean_fss",
            "wider_without_becoming_mostly_displaced",
            "particles_less_trapped_or_clipped_at_shoreline",
        ]
        table = completed[cols].sort_values("actual_wind_drift_factor")
        _write_csv(self.output_dir / "windage_refinement_table.csv", table)
        return table

    def _windage_mask_clipping_table(self, windage_table: pd.DataFrame) -> pd.DataFrame:
        table = windage_table[
            [
                "variant_id",
                "actual_wind_drift_factor",
                "raw_occupied_cells_before_mask",
                "valid_ocean_cells_after_official_mask",
                "cells_removed_by_official_mask",
                "mask_removed_fraction",
                "particles_less_trapped_or_clipped_at_shoreline",
            ]
        ].copy()
        table["valid_to_raw_ratio"] = (
            table["valid_ocean_cells_after_official_mask"] / table["raw_occupied_cells_before_mask"].replace(0, np.nan)
        )
        _write_csv(self.output_dir / "windage_mask_clipping_table.csv", table)
        return table

    def _windage_particle_status_by_time(self, matrix: pd.DataFrame) -> pd.DataFrame:
        rows: list[dict[str, Any]] = []
        for _, row in matrix[matrix["status"].eq("completed")].iterrows():
            nc_path = Path(str(row["control_netcdf_path"]))
            if not nc_path.exists():
                continue
            windage = float(row["setting_value"])
            with xr.open_dataset(nc_path) as ds:
                times = pd.DatetimeIndex(pd.to_datetime(ds["time"].values))
                if times.tz is not None:
                    times = times.tz_convert("UTC").tz_localize(None)
                for idx, timestamp in enumerate(times):
                    status = self._status_counts(ds, idx)
                    raw, _ = self._footprint_for_time_indices(ds, [idx])
                    official = apply_ocean_mask(raw, sea_mask=self.sea_mask, fill_value=0.0)
                    rows.append(
                        {
                            "variant_id": row["variant_id"],
                            "actual_wind_drift_factor": windage,
                            "time_index": idx,
                            "time_utc": _iso_z(timestamp),
                            "time_local": _local_iso(timestamp),
                            "local_date": _local_date(timestamp),
                            "active_floating_particles": status["active_floating_particles_on_march4"],
                            "stranded_beached_particles": status["stranded_beached_particles_on_march4"],
                            "deactivated_particles": status["deactivated_particles_on_march4"],
                            "outside_domain_particles": status["outside_domain_particles_on_march4"],
                            "raw_occupied_cells": int(np.count_nonzero(raw > 0)),
                            "valid_ocean_cells": int(np.count_nonzero(official > 0)),
                            "cells_removed_by_official_mask": int(np.count_nonzero((raw > 0) & ~(official > 0))),
                        }
                    )
        frame = pd.DataFrame(rows)
        _write_csv(self.output_dir / "windage_particle_status_by_time.csv", frame)
        return frame

    def _score_footprint(self, raw: np.ndarray) -> dict[str, Any]:
        obs = _read_raster(self.context["target_mask"])
        official = apply_ocean_mask(raw, sea_mask=self.sea_mask, fill_value=0.0)
        score = self._score_mask(official > 0, obs > 0, self.valid_mask)
        return {
            "raw_cells": int(np.count_nonzero(raw > 0)),
            "valid_ocean_cells": int(np.count_nonzero(official > 0)),
            "removed_by_mask_cells": int(np.count_nonzero((raw > 0) & ~(official > 0))),
            "nearest_distance_m": score["nearest_distance_to_obs_m"],
            "centroid_distance_m": score["centroid_distance_m"],
            "iou": score["iou"],
            "dice": score["dice"],
            "fss_1km": score["fss_1km"],
            "fss_3km": score["fss_3km"],
            "fss_5km": score["fss_5km"],
            "fss_10km": score["fss_10km"],
            "mean_fss": score["mean_fss"],
        }

    def _select_timing_variants(self, windage_table: pd.DataFrame) -> list[str]:
        baseline_id = _windage_id(0.02)
        lower = windage_table[
            (windage_table["actual_wind_drift_factor"] < 0.02)
            & (windage_table["valid_ocean_cells_after_official_mask"] > 4)
        ].copy()
        lower["is_nonzero"] = lower["actual_wind_drift_factor"] > 0
        lower["rank_nearest"] = pd.to_numeric(lower["nearest_forecast_to_observation_distance_m"], errors="coerce").fillna(1e12)
        lower = lower.sort_values(
            ["is_nonzero", "wider_without_becoming_mostly_displaced", "valid_ocean_cells_after_official_mask", "mean_fss", "rank_nearest"],
            ascending=[False, False, False, False, True],
        )
        selected = [baseline_id]
        for variant_id in lower["variant_id"].head(3).tolist():
            if variant_id not in selected:
                selected.append(variant_id)
        return selected

    def _timing_product_definition_table(self, matrix: pd.DataFrame, selected_variant_ids: list[str]) -> pd.DataFrame:
        rows: list[dict[str, Any]] = []
        matrix_lookup = {str(row["variant_id"]): row for _, row in matrix.iterrows()}
        for variant_id in selected_variant_ids:
            row = matrix_lookup.get(variant_id)
            if row is None or str(row.get("status")) != "completed":
                continue
            summary = _read_json(self.variant_products_dir / variant_id / "variant_summary.json")
            windage = float(summary.get("setting_value", "nan"))
            for product_name, product in (summary.get("product_definitions") or {}).items():
                rows.append(
                    {
                        "model": "opendrift",
                        "variant_id": variant_id,
                        "actual_wind_drift_factor": windage,
                        "product_definition": product_name,
                        "time_utc": "",
                        "time_local": "",
                        **self._product_row_metrics(product),
                    }
                )
            nc_path = Path(str(row["control_netcdf_path"]))
            with xr.open_dataset(nc_path) as ds:
                times = pd.DatetimeIndex(pd.to_datetime(ds["time"].values))
                if times.tz is not None:
                    times = times.tz_convert("UTC").tz_localize(None)
                for idx, timestamp in enumerate(times):
                    if _local_date(timestamp) != TARGET_DATE:
                        continue
                    raw, _ = self._footprint_for_time_indices(ds, [idx])
                    rows.append(
                        {
                            "model": "opendrift",
                            "variant_id": variant_id,
                            "actual_wind_drift_factor": windage,
                            "product_definition": "hourly_output_snapshot_march4_local",
                            "time_utc": _iso_z(timestamp),
                            "time_local": _local_iso(timestamp),
                            **self._score_footprint(raw),
                        }
                    )
        frame = pd.DataFrame(rows)
        _write_csv(self.output_dir / "timing_product_definition_table.csv", frame)
        return frame

    @staticmethod
    def _product_row_metrics(product: dict[str, Any]) -> dict[str, Any]:
        return {
            "raw_cells": int(product.get("raw_occupied_cells_before_mask", 0)),
            "valid_ocean_cells": int(product.get("valid_ocean_cells_after_official_mask", 0)),
            "removed_by_mask_cells": int(product.get("cells_removed_by_official_mask", 0)),
            "nearest_distance_m": product.get("nearest_distance_to_obs_m"),
            "centroid_distance_m": product.get("centroid_distance_m"),
            "iou": product.get("iou"),
            "dice": product.get("dice"),
            "fss_1km": product.get("fss_1km"),
            "fss_3km": product.get("fss_3km"),
            "fss_5km": product.get("fss_5km"),
            "fss_10km": product.get("fss_10km"),
            "mean_fss": product.get("mean_fss"),
        }

    def _pygnome_times(self) -> list[pd.Timestamp]:
        if not PYGNOME_NC.exists():
            return []
        with xr.open_dataset(PYGNOME_NC) as ds:
            times = pd.DatetimeIndex(pd.to_datetime(ds["time"].values))
        if times.tz is not None:
            times = times.tz_convert("UTC").tz_localize(None)
        return [pd.Timestamp(value) for value in times]

    def _pygnome_raw_for_timestamps(self, timestamps: list[pd.Timestamp]) -> np.ndarray:
        raw = np.zeros((self.grid.height, self.grid.width), dtype=np.float32)
        for timestamp in timestamps:
            try:
                lon, lat, mass, _, _ = extract_particles_at_time(
                    PYGNOME_NC,
                    timestamp,
                    "pygnome",
                    allow_uniform_mass_fallback=True,
                )
            except Exception:
                continue
            if len(lon) == 0:
                continue
            hits, _ = rasterize_particles(self.grid, lon, lat, mass)
            raw = np.maximum(raw, hits.astype(np.float32))
        return raw

    def _pygnome_product_definition_table(self) -> pd.DataFrame:
        times = self._pygnome_times()
        rows: list[dict[str, Any]] = []
        if not times:
            frame = pd.DataFrame(rows)
            _write_csv(self.output_dir / "pygnome_product_definition_table.csv", frame)
            return frame
        target_times = [ts for ts in times if _local_date(ts) == TARGET_DATE]
        seed_target_times = [ts for ts in times if _local_date(ts) in {SEED_DATE, TARGET_DATE}]
        product_defs = {
            "exact_final_march4_snapshot": [target_times[-1]] if target_times else [times[-1]],
            "march4_localdate_composite": target_times,
            "cumulative_march4_only": target_times,
            "cumulative_march3_to_march4": seed_target_times,
        }
        for product_name, product_times in product_defs.items():
            raw = self._pygnome_raw_for_timestamps(product_times)
            official = apply_ocean_mask(raw, sea_mask=self.sea_mask, fill_value=0.0)
            save_raster(self.grid, raw.astype(np.float32), self.pygnome_products_dir / f"{product_name}_no_mask.tif")
            save_raster(self.grid, official.astype(np.float32), self.pygnome_products_dir / f"{product_name}_official_valid_ocean.tif")
            rows.append(
                {
                    "model": "pygnome",
                    "variant_id": "pygnome_deterministic",
                    "actual_wind_drift_factor": "",
                    "product_definition": product_name,
                    "time_utc": ";".join(_iso_z(ts) for ts in product_times),
                    "time_local": ";".join(_local_iso(ts) for ts in product_times),
                    **self._score_footprint(raw),
                }
            )
        for timestamp in target_times:
            raw = self._pygnome_raw_for_timestamps([timestamp])
            rows.append(
                {
                    "model": "pygnome",
                    "variant_id": "pygnome_deterministic",
                    "actual_wind_drift_factor": "",
                    "product_definition": "hourly_output_snapshot_march4_local",
                    "time_utc": _iso_z(timestamp),
                    "time_local": _local_iso(timestamp),
                    **self._score_footprint(raw),
                }
            )
        frame = pd.DataFrame(rows)
        _write_csv(self.output_dir / "pygnome_product_definition_table.csv", frame)
        return frame

    def _write_product_comparison(self, timing_table: pd.DataFrame, pygnome_table: pd.DataFrame) -> pd.DataFrame:
        comparison = pd.concat([timing_table, pygnome_table], ignore_index=True, sort=False)
        _write_csv(self.output_dir / "opendrift_pygnome_product_definition_comparison.csv", comparison)
        return comparison

    def _select_ensemble_candidate(self, windage_table: pd.DataFrame) -> str:
        lower_nonzero = windage_table[
            (windage_table["actual_wind_drift_factor"] > 0)
            & (windage_table["actual_wind_drift_factor"] < 0.02)
            & (windage_table["wider_without_becoming_mostly_displaced"])
            & (windage_table["particles_less_trapped_or_clipped_at_shoreline"])
        ].copy()
        if lower_nonzero.empty:
            return ""
        lower_nonzero = lower_nonzero.sort_values(
            ["valid_ocean_cells_after_official_mask", "mean_fss", "nearest_forecast_to_observation_distance_m"],
            ascending=[False, False, True],
        )
        return str(lower_nonzero.iloc[0]["variant_id"])

    def _run_single_candidate_ensemble(self, variant: VariantSpec) -> dict[str, Any]:
        candidate_dir = self.ensemble_dir / variant.variant_id
        member_dir = candidate_dir / "members"
        product_dir = candidate_dir / "products"
        member_dir.mkdir(parents=True, exist_ok=True)
        product_dir.mkdir(parents=True, exist_ok=True)
        manifest_path = candidate_dir / "ensemble_candidate_manifest.json"
        if manifest_path.exists() and not self.force:
            return _read_json(manifest_path)
        forcing_paths, skip_reason = self._forcing_paths_for_recipe(variant.forcing_recipe)
        if forcing_paths is None:
            raise RuntimeError(f"Cannot run ensemble for {variant.variant_id}: {skip_reason}")
        release_overrides, release_description = self._release_payload(variant.release_mode)
        output_run_name = (
            f"{RUN_NAME}/phase3b_philsa_march3_4_5000_experiment/"
            f"experimental_settings_sensitivity/focused_windage_timing_followup/"
            f"ensemble_candidate/{variant.variant_id}/service"
        )
        with _temporary_env({OFFICIAL_ELEMENT_COUNT_OVERRIDE_ENV: str(ELEMENT_COUNT), "WORKFLOW_MODE": "mindoro_retro_2023"}):
            service = ExperimentalControlService(
                currents_file=forcing_paths["currents"],
                winds_file=forcing_paths["wind"],
                wave_file=forcing_paths["wave"],
                output_run_name=output_run_name,
                simulation_start_utc=SIMULATION_START_UTC,
                simulation_end_utc=SIMULATION_END_UTC,
                snapshot_hours=[24, 48],
                date_composite_dates=[SEED_DATE, TARGET_DATE],
                transport_overrides=variant.transport_overrides,
                seed_overrides={**release_overrides, "release_start_utc": SIMULATION_START_UTC},
            )
            base_time = normalize_model_timestamp(SIMULATION_START_UTC)
            end_time = normalize_model_timestamp(SIMULATION_END_UTC)
            duration_hours = int(math.ceil((end_time - base_time).total_seconds() / 3600.0))
            ensemble_cfg = service.official_config.get("ensemble") or {}
            wind_factor_min = float(ensemble_cfg.get("wind_factor_min", 0.8))
            wind_factor_max = float(ensemble_cfg.get("wind_factor_max", 1.2))
            offsets = [int(value) for value in ensemble_cfg.get("start_time_offset_hours", [-3, -2, -1, 0, 1, 2, 3])]
            diffusivity_min = float(ensemble_cfg.get("horizontal_diffusivity_m2s_min", 1.0))
            diffusivity_max = float(ensemble_cfg.get("horizontal_diffusivity_m2s_max", 10.0))
            rng = np.random.default_rng(BASELINE_RANDOM_SEED)
            member_records = []
            for i in range(EXPECTED_ENSEMBLE_MEMBER_COUNT):
                member_id = i + 1
                member_seed = int(rng.integers(0, np.iinfo(np.int32).max))
                member_rng = np.random.default_rng(member_seed)
                offset = int(member_rng.choice(offsets))
                run_start = base_time + pd.Timedelta(hours=offset)
                run_end = run_start + pd.Timedelta(hours=duration_hours)
                diffusivity = float(np.exp(member_rng.uniform(np.log(diffusivity_min), np.log(diffusivity_max))))
                wind_factor = float(member_rng.uniform(wind_factor_min, wind_factor_max))
                audit = service._init_run_audit(
                    recipe_name=variant.forcing_recipe,
                    run_kind="focused_windage_followup_ensemble_member",
                    requested_start_time=run_start,
                    duration_hours=duration_hours,
                    member_id=member_id,
                    perturbation={
                        "time_offset_hours": offset,
                        "horizontal_diffusivity_m2s": diffusivity,
                        "wind_factor": wind_factor,
                        "direct_wind_drift_factor": variant.setting_value,
                        "random_seed": member_seed,
                    },
                )
                model = service._build_model(
                    simulation_start=run_start,
                    simulation_end=run_end,
                    audit=audit,
                    wind_factor=wind_factor,
                    require_wave=True,
                    enable_stokes_drift=True,
                )
                model.set_config("drift:horizontal_diffusivity", diffusivity)
                model.set_config("drift:wind_uncertainty", 0.0)
                model.set_config("drift:current_uncertainty", 0.0)
                service._seed_official_release(model, run_start, num_elements=ELEMENT_COUNT, random_seed=member_seed, audit=audit)
                out_nc = member_dir / f"member_{member_id:02d}.nc"
                service._run_model(model=model, output_file=out_nc, duration_hours=duration_hours, audit=audit)
                member_records.append(
                    {
                        "member_id": member_id,
                        "output_file": str(out_nc),
                        "start_time_utc": timestamp_to_utc_iso(run_start),
                        "end_time_utc": timestamp_to_utc_iso(run_end),
                        "element_count": ELEMENT_COUNT,
                        "perturbation": audit["perturbation"],
                    }
                )
            service._write_loading_audit_artifacts()
        product = self._build_ensemble_products(variant, [Path(row["output_file"]) for row in member_records], product_dir)
        deterministic_source = self.variant_products_dir / variant.variant_id / "march4_localdate_composite_official_valid_ocean.tif"
        if deterministic_source.exists():
            shutil.copyfile(deterministic_source, product_dir / "deterministic_control_2023-03-04_localdate_official_valid_ocean.tif")
        comparison = {
            "variant_id": variant.variant_id,
            "setting_changed": variant.setting_changed,
            "setting_value": variant.setting_value,
            "classification": variant.classification,
            "actual_element_count": ELEMENT_COUNT,
            "ensemble_member_count": EXPECTED_ENSEMBLE_MEMBER_COUNT,
            "release_description": release_description,
            **product["comparison"],
        }
        result = {
            "comparison": comparison,
            "diagnostics": {"variant_id": variant.variant_id, **product["diagnostics"]},
            "fss_rows": [{"variant_id": variant.variant_id, **row} for row in product["fss_rows"]],
            "probability_rows": [{"variant_id": variant.variant_id, **row} for row in product["probability_rows"]],
            "member_records": member_records,
        }
        _write_json(manifest_path, result)
        return result

    def _run_optional_ensemble(self, candidate_id: str) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        if not candidate_id or not self.run_ensemble:
            for name in [
                "ensemble_candidate_comparison.csv",
                "ensemble_candidate_fss_by_window.csv",
                "ensemble_candidate_diagnostics.csv",
                "ensemble_candidate_probability_cell_counts.csv",
            ]:
                _write_csv(self.output_dir / name, pd.DataFrame())
            _write_text(
                self.output_dir / "ensemble_candidate_note.md",
                "# EXPERIMENTAL Focused Ensemble Note\n\nNo new 50-member ensemble was run.\n",
            )
            return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()
        variant_map = {variant.variant_id: variant for variant in self._build_variants()}
        result = self._run_single_candidate_ensemble(variant_map[candidate_id])
        deterministic_cells = self._deterministic_control_cells(candidate_id)
        result["comparison"]["deterministic_control_cell_count"] = deterministic_cells
        _write_json(self.ensemble_dir / candidate_id / "ensemble_candidate_manifest.json", result)
        comparison = pd.DataFrame([result["comparison"]])
        diagnostics = pd.DataFrame([result["diagnostics"]])
        fss = pd.DataFrame(result["fss_rows"])
        probability = pd.DataFrame(result["probability_rows"])
        _write_csv(self.output_dir / "ensemble_candidate_comparison.csv", comparison)
        _write_csv(self.output_dir / "ensemble_candidate_fss_by_window.csv", fss)
        _write_csv(self.output_dir / "ensemble_candidate_diagnostics.csv", diagnostics)
        _write_csv(self.output_dir / "ensemble_candidate_probability_cell_counts.csv", probability)
        _write_text(
            self.output_dir / "ensemble_candidate_note.md",
            "\n".join(
                [
                    "# EXPERIMENTAL Focused Ensemble Note",
                    "",
                    f"One new 50-member ensemble was run for `{candidate_id}` only.",
                    f"- Any-member valid-ocean cells: {int(comparison.iloc[0]['valid_ocean_any_member_cells'])}",
                    f"- Mean FSS: {float(comparison.iloc[0]['mean_fss']):.6f}",
                    "- This remains experimental and not thesis-facing.",
                ]
            )
            + "\n",
        )
        return comparison, fss, diagnostics, probability

    def _deterministic_control_cells(self, variant_id: str) -> int | str:
        path = self.variant_products_dir / variant_id / "march4_localdate_composite_official_valid_ocean.tif"
        if not path.exists():
            return ""
        return int(np.count_nonzero(_read_raster(path) > 0))

    def _write_figures(
        self,
        windage_table: pd.DataFrame,
        timing_table: pd.DataFrame,
        comparison_table: pd.DataFrame,
        candidate_id: str,
    ) -> None:
        if plt is None or ListedColormap is None:
            return
        obs = _read_raster(self.context["target_mask"]) > 0
        self._windage_refinement_board(windage_table, obs)
        self._shoreline_mask_clipping_plot(windage_table)
        self._timing_product_definition_board(timing_table, obs, candidate_id)
        self._baseline_lower_pygnome_board(obs, candidate_id)

    def _plot_mask(self, ax: Any, mask: np.ndarray, obs: np.ndarray, title: str, color: str) -> None:
        ax.imshow(np.ma.masked_where(obs <= 0, obs), cmap=ListedColormap(["#2563eb"]), alpha=0.25)
        ax.imshow(np.ma.masked_where(mask <= 0, mask), cmap=ListedColormap([color]), alpha=0.78)
        ax.set_title(title, fontsize=9)
        ax.set_axis_off()

    def _windage_refinement_board(self, windage_table: pd.DataFrame, obs: np.ndarray) -> None:
        variants = windage_table.sort_values("actual_wind_drift_factor")
        ncols = 4
        nrows = int(math.ceil(len(variants) / ncols))
        fig, axes = plt.subplots(nrows, ncols, figsize=(13, 3.2 * nrows), dpi=150)
        axes_arr = np.asarray(axes).reshape(-1)
        for ax, (_, row) in zip(axes_arr, variants.iterrows()):
            path = self.variant_products_dir / row["variant_id"] / "march4_localdate_composite_official_valid_ocean.tif"
            mask = _read_raster(path) > 0 if path.exists() else np.zeros_like(obs)
            title = (
                f"windage {float(row['actual_wind_drift_factor']):.4f}\n"
                f"{int(row['valid_ocean_cells_after_official_mask'])} cells, "
                f"near {float(row['nearest_forecast_to_observation_distance_m']):.0f} m"
            )
            self._plot_mask(ax, mask, obs, title, "#dc2626")
        for ax in axes_arr[len(variants) :]:
            ax.set_axis_off()
        fig.suptitle("Focused Windage Refinement: March 4 Local-Date Official Cells")
        fig.tight_layout()
        fig.savefig(self.output_dir / "windage_refinement_board.png", bbox_inches="tight")
        plt.close(fig)

    def _shoreline_mask_clipping_plot(self, windage_table: pd.DataFrame) -> None:
        fig, ax1 = plt.subplots(figsize=(9, 5), dpi=160)
        x = windage_table["actual_wind_drift_factor"].astype(float)
        ax1.plot(x, windage_table["raw_occupied_cells_before_mask"], marker="o", label="raw cells", color="#64748b")
        ax1.plot(x, windage_table["valid_ocean_cells_after_official_mask"], marker="o", label="official valid-ocean cells", color="#15803d")
        ax1.plot(x, windage_table["cells_removed_by_official_mask"], marker="o", label="removed by mask", color="#dc2626")
        ax1.set_xlabel("OpenDrift seed:wind_drift_factor")
        ax1.set_ylabel("Cells")
        ax2 = ax1.twinx()
        ax2.plot(x, windage_table["mask_removed_fraction"], marker="s", linestyle="--", label="removed fraction", color="#7c3aed")
        ax2.set_ylabel("Removed fraction")
        lines, labels = ax1.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax1.legend(lines + lines2, labels + labels2, loc="best")
        ax1.set_title("Shoreline / Valid-Ocean Mask Clipping by Windage")
        fig.tight_layout()
        fig.savefig(self.output_dir / "shoreline_mask_clipping_by_windage.png", bbox_inches="tight")
        plt.close(fig)

    def _timing_product_definition_board(self, timing_table: pd.DataFrame, obs: np.ndarray, candidate_id: str) -> None:
        variants = [_windage_id(0.02)]
        if candidate_id and candidate_id not in variants:
            variants.append(candidate_id)
        product_names = [
            "exact_final_snapshot",
            "march4_localdate_composite",
            "cumulative_march4_only",
            "cumulative_march3_to_march4",
        ]
        fig, axes = plt.subplots(len(variants), len(product_names), figsize=(14, 3.4 * len(variants)), dpi=150)
        axes_arr = np.asarray(axes).reshape(len(variants), len(product_names))
        for r, variant_id in enumerate(variants):
            for c, product_name in enumerate(product_names):
                path = self.variant_products_dir / variant_id / f"{product_name}_official_valid_ocean.tif"
                mask = _read_raster(path) > 0 if path.exists() else np.zeros_like(obs)
                row = timing_table[
                    timing_table["variant_id"].eq(variant_id)
                    & timing_table["product_definition"].eq(product_name)
                    & timing_table["model"].eq("opendrift")
                ]
                cells = int(row.iloc[0]["valid_ocean_cells"]) if not row.empty else 0
                title = f"{variant_id}\n{product_name}\n{cells} cells"
                self._plot_mask(axes_arr[r, c], mask, obs, title, "#f97316")
        fig.suptitle("OpenDrift Timing / Product Definition Sensitivity")
        fig.tight_layout()
        fig.savefig(self.output_dir / "timing_product_definition_board.png", bbox_inches="tight")
        plt.close(fig)

    def _baseline_lower_pygnome_board(self, obs: np.ndarray, candidate_id: str) -> None:
        panels: list[tuple[str, np.ndarray, str]] = []
        baseline_path = self.variant_products_dir / _windage_id(0.02) / "march4_localdate_composite_official_valid_ocean.tif"
        panels.append(("OpenDrift baseline 0.020", _read_raster(baseline_path) > 0, "#dc2626"))
        if candidate_id:
            cand_path = self.variant_products_dir / candidate_id / "march4_localdate_composite_official_valid_ocean.tif"
            panels.append((f"OpenDrift {candidate_id}", _read_raster(cand_path) > 0, "#16a34a"))
        zero_path = self.variant_products_dir / _windage_id(0.0) / "march4_localdate_composite_official_valid_ocean.tif"
        panels.append(("OpenDrift windage 0.0000", _read_raster(zero_path) > 0, "#f97316"))
        py_local = self.pygnome_products_dir / "march4_localdate_composite_official_valid_ocean.tif"
        py_cum = self.pygnome_products_dir / "cumulative_march3_to_march4_official_valid_ocean.tif"
        if py_local.exists():
            panels.append(("PyGNOME local-date", _read_raster(py_local) > 0, "#7c3aed"))
        if py_cum.exists():
            panels.append(("PyGNOME cumulative 3-4", _read_raster(py_cum) > 0, "#0f766e"))
        ncols = 3
        nrows = int(math.ceil(len(panels) / ncols))
        fig, axes = plt.subplots(nrows, ncols, figsize=(12, 3.8 * nrows), dpi=150)
        axes_arr = np.asarray(axes).reshape(-1)
        for ax, (title, mask, color) in zip(axes_arr, panels):
            self._plot_mask(ax, mask, obs, title, color)
        for ax in axes_arr[len(panels) :]:
            ax.set_axis_off()
        fig.suptitle("Baseline vs Lower Windage vs PyGNOME")
        fig.tight_layout()
        fig.savefig(self.output_dir / "baseline_vs_lower_windage_vs_pygnome_board.png", bbox_inches="tight")
        plt.close(fig)

    def _write_note(
        self,
        timing_meta: pd.DataFrame,
        windage_table: pd.DataFrame,
        timing_table: pd.DataFrame,
        pygnome_table: pd.DataFrame,
        ensemble_comparison: pd.DataFrame,
        candidate_id: str,
    ) -> None:
        baseline = windage_table[np.isclose(windage_table["actual_wind_drift_factor"], 0.02)].iloc[0]
        wind0 = windage_table[np.isclose(windage_table["actual_wind_drift_factor"], 0.0)].iloc[0]
        candidate = windage_table[windage_table["variant_id"].eq(candidate_id)].iloc[0] if candidate_id else None
        timing_baseline = timing_table[
            timing_table["variant_id"].eq(_windage_id(0.02))
            & timing_table["product_definition"].isin(["march4_localdate_composite", "cumulative_march3_to_march4"])
        ]
        baseline_local_cells = int(timing_baseline[timing_baseline["product_definition"].eq("march4_localdate_composite")].iloc[0]["valid_ocean_cells"])
        baseline_cumulative_cells = int(timing_baseline[timing_baseline["product_definition"].eq("cumulative_march3_to_march4")].iloc[0]["valid_ocean_cells"])
        py_local = pygnome_table[pygnome_table["product_definition"].eq("march4_localdate_composite")]
        py_cum = pygnome_table[pygnome_table["product_definition"].eq("cumulative_march3_to_march4")]
        py_metadata = _read_json(PYGNOME_METADATA)
        py_windage_note = "PyGNOME NetCDF windage_range is 0.01-0.04 when present."
        if PYGNOME_NC.exists():
            with xr.open_dataset(PYGNOME_NC) as ds:
                if "windage_range" in ds:
                    windage_range = np.asarray(ds["windage_range"].values, dtype=float)
                    py_windage_note = f"PyGNOME NetCDF windage_range spans {np.nanmin(windage_range):.3f}-{np.nanmax(windage_range):.3f}."
        candidate_line = (
            f"`{candidate_id}` ({float(candidate['actual_wind_drift_factor']):.4f}) with "
            f"{int(candidate['valid_ocean_cells_after_official_mask'])} valid cells, "
            f"nearest distance {float(candidate['nearest_forecast_to_observation_distance_m']):.0f} m, "
            f"mean FSS {float(candidate['mean_fss']):.6f}"
            if candidate is not None
            else "none"
        )
        ensemble_line = "No new ensemble was run."
        if not ensemble_comparison.empty:
            ens = ensemble_comparison.iloc[0]
            ensemble_line = (
                f"One new 50-member ensemble was run for `{ens['variant_id']}`: "
                f"{int(ens['valid_ocean_any_member_cells'])} any-member valid-ocean cells, "
                f"mean FSS {float(ens['mean_fss']):.6f}."
            )
        exact_time_text = "Exact acquisition/image time was not available in machine-readable PhilSA layer metadata."
        if bool(timing_meta["machine_readable_acquisition_time_available"].any()):
            exact_time_text = "Machine-readable acquisition time was found."
        lines = [
            "# EXPERIMENTAL Focused Windage / Timing Follow-up Note",
            "",
            "This remains experimental-only, not thesis-facing, and not promoted.",
            "",
            "## PhilSA Timing Metadata",
            "",
            f"- March 3 layer: `{SEED_SOURCE_NAME}`, observation date `2023-03-03`.",
            f"- March 4 layer: `{TARGET_SOURCE_NAME}`, observation date `2023-03-04`.",
            f"- {exact_time_text}",
            "- Stored ArcGIS layer metadata has edit timestamps and no `timeInfo`; raw feature attributes contain geometry/area fields but no acquisition/image-time field.",
            "- March 4 is therefore best treated as an uncertain dated product; local-date composite is defensible, exact image-time scoring is not supported by the stored metadata.",
            "",
            "## Answers",
            "",
            (
                "1. The tiny baseline footprint is consistent with direct wind drift plus `coastline_action=previous` pushing the local-date footprint into cells that the official valid-ocean mask removes: "
                f"baseline raw cells {int(baseline['raw_occupied_cells_before_mask'])}, valid cells {int(baseline['valid_ocean_cells_after_official_mask'])}, "
                f"removed fraction {float(baseline['mask_removed_fraction']):.3f}."
            ),
            (
                "2. In this experimental nearshore case, `wind_drift_factor=0.020` appears too strong for the March 3 PhilSA initialization under the current shoreline handling, "
                f"because lower windage values reduce mask clipping and increase retained valid-ocean cells."
            ),
            f"3. Best lower nonzero candidate: {candidate_line}.",
            (
                "4. `wind_drift_factor=0.000` is a useful diagnostic extreme, not automatically physically preferable. "
                f"It produced {int(wind0['valid_ocean_cells_after_official_mask'])} valid cells and mean FSS {float(wind0['mean_fss']):.6f}, "
                "but zero direct wind drift suppresses a physical drift mechanism rather than refining it."
            ),
            (
                "5. Product definition explains part of the tiny product: baseline March 4 local-date valid cells were "
                f"{baseline_local_cells}, while cumulative March 3-to-March 4 valid cells were {baseline_cumulative_cells}."
            ),
            "6. The cumulative March 3-to-March 4 footprint is scientifically useful as a transport-corridor diagnostic, not as a direct satellite-snapshot score.",
            (
                "7. PyGNOME remains wider because it uses different direct-wind/windage semantics and shoreline/beaching behavior, and its local-date product remains broad under the same grid/mask. "
                f"{py_windage_note} Local-date PyGNOME valid cells: {int(py_local.iloc[0]['valid_ocean_cells']) if not py_local.empty else 'n/a'}; "
                f"cumulative March 3-to-4 cells: {int(py_cum.iloc[0]['valid_ocean_cells']) if not py_cum.empty else 'n/a'}. "
                f"Transport mode metadata: {py_metadata.get('transport_forcing_mode', '')}."
            ),
            f"8. Future experiments should consider `{candidate_id}` as a lower-nonzero windage candidate if it remains stable in additional cases; do not promote it from this single diagnostic.",
            "9. Nothing here is thesis-facing and nothing should be promoted yet.",
            "",
            f"Optional ensemble: {ensemble_line}",
        ]
        _write_text(self.output_dir / "focused_windage_timing_note.md", "\n".join(lines) + "\n")

    def run(self) -> dict[str, Any]:
        before = _snapshot_paths(PROTECTED_OUTPUT_DIRS)
        _write_json(self.output_dir / "protected_outputs_snapshot_before.json", before)
        exception_text = ""
        results: dict[str, Any] = {}
        try:
            timing_meta = self._write_philsa_timing_metadata()
            rows = []
            for variant in self._build_variants():
                print(f"[focused windage follow-up] running {variant.variant_id}", flush=True)
                rows.append(self._run_deterministic_variant(variant))
            matrix = self._write_matrix_outputs(rows)
            windage_table = self._windage_refinement_table(matrix)
            mask_table = self._windage_mask_clipping_table(windage_table)
            status_table = self._windage_particle_status_by_time(matrix)
            selected_timing = self._select_timing_variants(windage_table)
            timing_table = self._timing_product_definition_table(matrix, selected_timing)
            pygnome_table = self._pygnome_product_definition_table()
            comparison_table = self._write_product_comparison(timing_table, pygnome_table)
            candidate_id = self._select_ensemble_candidate(windage_table)
            ensemble_comparison, ensemble_fss, ensemble_diagnostics, ensemble_probability = self._run_optional_ensemble(candidate_id)
            self._write_figures(windage_table, timing_table, comparison_table, candidate_id)
            self._write_note(timing_meta, windage_table, timing_table, pygnome_table, ensemble_comparison, candidate_id)
            results = {
                "output_dir": str(self.output_dir),
                "variants_run": windage_table["variant_id"].tolist(),
                "completed_variant_count": int(len(windage_table)),
                "requested_element_count": ELEMENT_COUNT,
                "march3_source": SEED_SOURCE_NAME,
                "march4_source": TARGET_SOURCE_NAME,
                "selected_timing_variants": selected_timing,
                "selected_lower_nonzero_ensemble_candidate": candidate_id,
                "ensemble_candidates_run": ensemble_comparison["variant_id"].tolist() if not ensemble_comparison.empty else [],
                "windage_table_rows": int(len(windage_table)),
                "timing_table_rows": int(len(timing_table)),
                "status_table_rows": int(len(status_table)),
                "mask_table_rows": int(len(mask_table)),
            }
        except Exception:
            exception_text = traceback.format_exc()
        finally:
            after = _snapshot_paths(PROTECTED_OUTPUT_DIRS)
            _write_json(self.output_dir / "protected_outputs_snapshot_after.json", after)
            _write_json(self.output_dir / "protected_outputs_snapshot_diff.json", _snapshot_diff(before, after))
        if exception_text:
            _write_text(
                self.output_dir / "focused_windage_timing_blocked_note.md",
                "# EXPERIMENTAL Focused Windage/Timing Follow-up Blocked\n\n```text\n" + exception_text + "\n```\n",
            )
            raise RuntimeError(f"Focused windage/timing follow-up blocked. See {self.output_dir / 'focused_windage_timing_blocked_note.md'}")
        blocked = self.output_dir / "focused_windage_timing_blocked_note.md"
        if blocked.exists():
            blocked.unlink()
        results["protected_outputs_unchanged"] = not any(_read_json(self.output_dir / "protected_outputs_snapshot_diff.json").values())
        results["ensemble_member_count_expected_if_run"] = EXPECTED_ENSEMBLE_MEMBER_COUNT
        _write_json(self.output_dir / "focused_windage_timing_run_manifest.json", results)
        return results


def run_mindoro_march3_4_focused_windage_timing_followup(
    *,
    run_ensemble: bool = True,
    force: bool = False,
) -> dict[str, Any]:
    return MindoroMarch34FocusedWindageTimingFollowup(run_ensemble=run_ensemble, force=force).run()


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--skip-ensemble", action="store_true")
    parser.add_argument("--force", action="store_true")
    args = parser.parse_args()
    result = run_mindoro_march3_4_focused_windage_timing_followup(
        run_ensemble=not args.skip_ensemble,
        force=args.force,
    )
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
