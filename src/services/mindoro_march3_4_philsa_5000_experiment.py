"""Experimental Mindoro PhilSA March 3 -> March 4 5,000-element validation test."""

from __future__ import annotations

import json
import math
import os
import traceback
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import requests

from src.core.case_context import get_case_context
from src.helpers.raster import GridBuilder, rasterize_observation_layer, rasterize_particles, save_raster
from src.helpers.scoring import apply_ocean_mask, load_sea_mask_array
from src.services.arcgis import (
    _infer_source_crs,
    _repair_degree_scaled_geometries,
    _sanitize_vector_columns_for_gpkg,
    clean_arcgis_geometries,
)
from src.services.ensemble import OFFICIAL_ELEMENT_COUNT_OVERRIDE_ENV, normalize_time_index, run_official_spill_forecast
from src.services.mindoro_primary_validation_metadata import (
    MINDORO_BASE_CASE_CONFIG_PATH,
    MINDORO_PRIMARY_VALIDATION_AMENDMENT_PATH,
)
from src.services.phase3b_extended_public_scored_march13_14_reinit import (
    BRANCHES,
    EXPECTED_ENSEMBLE_MEMBER_COUNT,
    LOCAL_TIMEZONE,
    MAX_OFFICIAL_START_OFFSET_HOURS,
    PHASE3B_REINIT_APPENDIX_ONLY_ENV,
    PHASE3B_REINIT_LAUNCHER_ENTRY_ID_OVERRIDE_ENV,
    PHASE3B_REINIT_OUTPUT_DIR_NAME_ENV,
    PHASE3B_REINIT_PRIMARY_PUBLIC_VALIDATION_ENV,
    PHASE3B_REINIT_REPORTING_ROLE_OVERRIDE_ENV,
    PHASE3B_REINIT_REQUESTED_ELEMENT_COUNT_ENV,
    PHASE3B_REINIT_TRACK_ID_OVERRIDE_ENV,
    PHASE3B_REINIT_TRACK_LABEL_OVERRIDE_ENV,
    PHASE3B_REINIT_TRACK_OVERRIDE_ENV,
    PHASE_OR_TRACK as B1_PHASE_OR_TRACK,
    Phase3BExtendedPublicScoredMarch1314ReinitService,
    ReinitBranchConfig,
    ReinitWindow,
    _consensus_int,
    _forcing_time_and_vars,
    _iso_z,
    _json_default,
    _normalize_utc,
    _read_json,
    _write_csv,
    _write_json,
    _write_text,
)
from src.services.scoring import OFFICIAL_PHASE3B_WINDOWS_KM
from src.utils.io import _resolve_polygon_reference_point, get_case_output_dir, resolve_recipe_selection
from src.utils.local_input_store import PERSISTENT_LOCAL_INPUT_STORE

try:
    import geopandas as gpd
except ImportError:  # pragma: no cover
    gpd = None

try:
    import matplotlib.pyplot as plt
except ImportError:  # pragma: no cover
    plt = None

try:
    import xarray as xr
except ImportError:  # pragma: no cover
    xr = None


RUN_NAME = "CASE_MINDORO_RETRO_2023"
EXPERIMENT_PHASE_ID = "mindoro_march3_4_philsa_5000_experiment"
EXPERIMENT_LAUNCHER_ENTRY_ID = "phase3b_mindoro_march3_4_philsa_5000_experiment"
EXPERIMENT_OUTPUT_DIR_NAME = "phase3b_philsa_march3_4_5000_experiment"
EXPERIMENT_REQUESTED_ELEMENT_COUNT = 5000
EXPERIMENT_TRACK = "mindoro_phase3b_philsa_march3_4_5000_experiment"
EXPERIMENT_TRACK_ID = "EXP_PHILSA_MAR03_MAR04_5000"
EXPERIMENT_TRACK_LABEL = "Experimental Mindoro PhilSA March 3 -> March 4 5,000-element test"
EXPERIMENT_REPORTING_ROLE = "experimental_archive_provenance_not_thesis_facing"
EXPERIMENT_PHASE_OR_TRACK = "phase3b_archive_provenance_philsa_march3_4"
START_SOURCE_GEOMETRY_LABEL = "accepted_march3_philsa_processed_polygon"
REQUEST_TIMEOUT = 60

SEED_OBS_DATE = "2023-03-03"
TARGET_OBS_DATE = "2023-03-04"

CLAIM_BOUNDARY = (
    "Experimental PhilSA-only March 3 -> March 4 next-day test; does not replace canonical B1 "
    "and is not thesis-facing unless manually reviewed later."
)


@dataclass(frozen=True)
class PhilSALayerSpec:
    observation_date: str
    provider: str
    source_name: str
    source_url: str
    service_url: str
    role: str

    @property
    def cache_key(self) -> str:
        text = "__".join([self.observation_date, self.provider, self.source_name])
        return "".join(char.lower() if char.isalnum() else "_" for char in text).strip("_")


PHILSA_LAYER_SPECS = [
    PhilSALayerSpec(
        observation_date=SEED_OBS_DATE,
        provider="PhilSA",
        source_name="MindoroOilSpill_Philsa_230303",
        source_url="https://services1.arcgis.com/RTK5Unh1Z71JKIiR/arcgis/rest/services/MindoroOilSpill_Philsa_230303/FeatureServer",
        service_url="https://services1.arcgis.com/RTK5Unh1Z71JKIiR/arcgis/rest/services/MindoroOilSpill_Philsa_230303/FeatureServer",
        role="seed_initialization_layer",
    ),
    PhilSALayerSpec(
        observation_date=TARGET_OBS_DATE,
        provider="PhilSA",
        source_name="MindoroOilSpill_Philsa_230304",
        source_url="https://services1.arcgis.com/RTK5Unh1Z71JKIiR/arcgis/rest/services/MindoroOilSpill_Philsa_230304/FeatureServer",
        service_url="https://services1.arcgis.com/RTK5Unh1Z71JKIiR/arcgis/rest/services/MindoroOilSpill_Philsa_230304/FeatureServer",
        role="target_validation_layer",
    ),
]

PROHIBITED_SOURCE_NAMES = [
    "Possible_oil_Spills_(March_3,_2023)",
    "Possible_oil_slick_(March_6,_2023)",
    "MindoroOilSpill_MSI_230305",
    "MindoroOilSpill_NOAA_230313",
    "MindoroOilSpill_NOAA_230314",
]


def resolve_march3_4_philsa_window() -> ReinitWindow:
    simulation_start = pd.Timestamp("2023-03-03 00:00", tz=LOCAL_TIMEZONE).tz_convert("UTC").tz_localize(None)
    simulation_end = pd.Timestamp("2023-03-04 23:59", tz=LOCAL_TIMEZONE).tz_convert("UTC").tz_localize(None)
    required_end = simulation_end + pd.Timedelta(hours=MAX_OFFICIAL_START_OFFSET_HOURS)
    return ReinitWindow(
        forecast_local_dates=[SEED_OBS_DATE, TARGET_OBS_DATE],
        scored_target_date=TARGET_OBS_DATE,
        seed_obs_date=SEED_OBS_DATE,
        simulation_start_utc=_iso_z(simulation_start),
        simulation_end_utc=_iso_z(simulation_end),
        required_forcing_start_utc=_iso_z(simulation_start - pd.Timedelta(hours=MAX_OFFICIAL_START_OFFSET_HOURS)),
        required_forcing_end_utc=_iso_z(required_end),
        download_start_date="2023-03-02",
        download_end_date="2023-03-05",
        end_selection_source="fixed_next_day_local_date_window",
        date_composite_rule=(
            "Forecast member presence is unioned across model timesteps whose UTC timestamp converts to the "
            "local dates 2023-03-03 and 2023-03-04 in Asia/Manila; only the 2023-03-04 local-date p50 product is scored."
        ),
    )


def _safe_name(value: str) -> str:
    safe = "".join(char.lower() if char.isalnum() else "_" for char in str(value)).strip("_")
    return safe or "philsa_source"


def _snapshot_paths(repo_root: Path, paths: list[Path]) -> dict[str, dict[str, int]]:
    snapshot: dict[str, dict[str, int]] = {}
    for root in paths:
        if not root.exists():
            continue
        for path in sorted(item for item in root.rglob("*") if item.is_file()):
            stat = path.stat()
            try:
                rel = str(path.resolve().relative_to(repo_root.resolve())).replace("\\", "/")
            except Exception:
                rel = str(path).replace("\\", "/")
            snapshot[rel] = {"size_bytes": int(stat.st_size), "mtime_ns": int(stat.st_mtime_ns)}
    return snapshot


def _snapshot_diff(before: dict[str, Any], after: dict[str, Any]) -> dict[str, list[str]]:
    before_keys = set(before)
    after_keys = set(after)
    return {
        "added": sorted(after_keys - before_keys),
        "removed": sorted(before_keys - after_keys),
        "changed": sorted(key for key in before_keys & after_keys if before[key] != after[key]),
    }


def _temporary_env(overrides: dict[str, str]):
    class _EnvContext:
        def __enter__(self):
            self.previous = {key: os.environ.get(key) for key in overrides}
            for key, value in overrides.items():
                os.environ[key] = str(value)
            return self

        def __exit__(self, exc_type, exc, tb):
            for key, old_value in self.previous.items():
                if old_value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = old_value
            return False

    return _EnvContext()


class MindoroMarch34PhilSA5000ExperimentService(Phase3BExtendedPublicScoredMarch1314ReinitService):
    def __init__(self, *, repo_root: str | Path | None = None, session: requests.Session | None = None):
        self.repo_root = Path(repo_root or Path(__file__).resolve().parents[2])
        init_env = {
            PHASE3B_REINIT_OUTPUT_DIR_NAME_ENV: EXPERIMENT_OUTPUT_DIR_NAME,
            PHASE3B_REINIT_TRACK_OVERRIDE_ENV: EXPERIMENT_TRACK,
            PHASE3B_REINIT_TRACK_ID_OVERRIDE_ENV: EXPERIMENT_TRACK_ID,
            PHASE3B_REINIT_TRACK_LABEL_OVERRIDE_ENV: EXPERIMENT_TRACK_LABEL,
            PHASE3B_REINIT_REPORTING_ROLE_OVERRIDE_ENV: EXPERIMENT_REPORTING_ROLE,
            PHASE3B_REINIT_APPENDIX_ONLY_ENV: "true",
            PHASE3B_REINIT_PRIMARY_PUBLIC_VALIDATION_ENV: "false",
            PHASE3B_REINIT_LAUNCHER_ENTRY_ID_OVERRIDE_ENV: EXPERIMENT_LAUNCHER_ENTRY_ID,
            PHASE3B_REINIT_REQUESTED_ELEMENT_COUNT_ENV: str(EXPERIMENT_REQUESTED_ELEMENT_COUNT),
            OFFICIAL_ELEMENT_COUNT_OVERRIDE_ENV: str(EXPERIMENT_REQUESTED_ELEMENT_COUNT),
        }
        with _temporary_env(init_env):
            super().__init__()

        self.repo_root = self.repo_root.resolve()
        self.phase_id = EXPERIMENT_PHASE_ID
        self.window = resolve_march3_4_philsa_window()
        self.requested_element_count = EXPERIMENT_REQUESTED_ELEMENT_COUNT
        self.output_dir_name = EXPERIMENT_OUTPUT_DIR_NAME
        self.output_dir = self.case_output_dir / self.output_dir_name
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.precheck_dir = self.output_dir / "precheck"
        self.forcing_dir = self.output_dir / "forcing"
        self.observation_dir = self.output_dir / "observations"
        for path in (self.precheck_dir, self.forcing_dir, self.observation_dir):
            path.mkdir(parents=True, exist_ok=True)
        self.force_rerun = True
        self.track = EXPERIMENT_TRACK
        self.track_id = EXPERIMENT_TRACK_ID
        self.track_label = EXPERIMENT_TRACK_LABEL
        self.reporting_role = EXPERIMENT_REPORTING_ROLE
        self.appendix_only = True
        self.primary_public_validation = False
        self.reportable = False
        self.thesis_facing = False
        self.experimental_only = True
        self.is_canonical_bundle = False
        self.session = session or requests.Session()
        self.session.headers.update({"User-Agent": "mindoro-march3-4-philsa-5000-experiment/1.0"})
        self.grid = GridBuilder()
        self.sea_mask = load_sea_mask_array(self.grid.spec)
        self.valid_mask = self.sea_mask > 0.5 if self.sea_mask is not None else None
        self.protected_output_dirs = [
            self.case_output_dir / "phase3b_extended_public_scored_march13_14_reinit",
            self.case_output_dir / "phase3b_extended_public_scored_march13_14_reinit_pygnome_comparison",
            self.case_output_dir / "phase3b",
            self.case_output_dir / "phase3b_multidate_public",
            self.case_output_dir / "public_obs_appendix",
            self.case_output_dir / "phase3b_march13_14_element_count_sensitivity",
            self.repo_root / "output" / "final_validation_package",
            self.repo_root / "output" / "final_reproducibility_package",
            self.repo_root / "output" / "figure_package_publication",
            self.repo_root / "output" / "phase4" / RUN_NAME,
        ]
        self.command_text = "PIPELINE_PHASE=mindoro_march3_4_philsa_5000_experiment python -m src"

    def _launcher_entry_ids(self) -> dict[str, str]:
        return {"experiment": EXPERIMENT_LAUNCHER_ENTRY_ID}

    def _sensitivity_context(self, branch: ReinitBranchConfig, recipe_source: str) -> dict[str, Any]:
        return {
            "track": self.track,
            "track_id": self.track_id,
            "track_label": self.track_label,
            "phase_or_track": EXPERIMENT_PHASE_OR_TRACK,
            "branch_id": branch.branch_id,
            "branch_description": branch.description,
            "recipe_source": recipe_source,
            "seed_obs_date": SEED_OBS_DATE,
            "single_date_validation": TARGET_OBS_DATE,
            "date_composite_rule": self.window.date_composite_rule,
            "appendix_only": True,
            "reporting_role": self.reporting_role,
            "primary_public_validation": False,
            "requested_element_count": int(self.requested_element_count),
            "thesis_facing": False,
            "reportable": False,
            "experimental_only": True,
            "promotion_mode": "none_experimental_archive_only",
            "claim_boundary": CLAIM_BOUNDARY,
            "philsa_only_pair": True,
            "prohibited_sources_not_used": PROHIBITED_SOURCE_NAMES,
            "canonical_b1_preserved": True,
        }

    @staticmethod
    def _resolve_recipe():
        selection = resolve_recipe_selection()
        return selection, "frozen_focused_phase1_baseline_recipe_no_new_phase1_selection"

    def _resolve_layer_endpoint(self, spec: PhilSALayerSpec) -> tuple[str, int, dict[str, Any]]:
        service_url = spec.service_url.rstrip("/")
        tail = service_url.rsplit("/", 1)[-1]
        if tail.isdigit():
            metadata_url = service_url
            response = self.session.get(metadata_url, params={"f": "json"}, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            return service_url.rsplit("/", 1)[0], int(tail), response.json() or {}

        response = self.session.get(service_url, params={"f": "json"}, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        root_metadata = response.json() or {}
        layers = root_metadata.get("layers") or []
        if len(layers) == 1 and layers[0].get("id") is not None:
            layer_id = int(layers[0]["id"])
            layer_url = f"{service_url}/{layer_id}"
            layer_response = self.session.get(layer_url, params={"f": "json"}, timeout=REQUEST_TIMEOUT)
            layer_response.raise_for_status()
            return service_url, layer_id, layer_response.json() or {}
        if root_metadata.get("id") is not None and root_metadata.get("geometryType"):
            return service_url.rsplit("/", 1)[0], int(root_metadata["id"]), root_metadata
        raise RuntimeError(f"Could not resolve one ArcGIS layer for {spec.source_name} at {service_url}")

    def _layer_paths(self, spec: PhilSALayerSpec) -> dict[str, Path]:
        cache_dir = self.observation_dir / spec.cache_key
        source_key = _safe_name(spec.source_name)
        return {
            "cache_dir": cache_dir,
            "raw_geojson": cache_dir / f"{source_key}_raw.geojson",
            "layer_metadata": cache_dir / f"{source_key}_layer_metadata.json",
            "processed_vector": cache_dir / f"{source_key}_processed.gpkg",
            "score_mask": cache_dir / f"{source_key}_score_mask.tif",
        }

    def _materialize_philsa_layer(self, spec: PhilSALayerSpec) -> dict[str, Any]:
        if gpd is None:
            raise ImportError("geopandas is required for the PhilSA March 3 -> March 4 experiment.")
        paths = self._layer_paths(spec)
        paths["cache_dir"].mkdir(parents=True, exist_ok=True)
        source_key = _safe_name(spec.source_name)
        force_refresh = self._force_refresh_enabled()
        reuse_ready = (
            not force_refresh
            and paths["raw_geojson"].exists()
            and paths["layer_metadata"].exists()
            and paths["processed_vector"].exists()
            and paths["score_mask"].exists()
        )

        if reuse_ready:
            raw_geojson = json.loads(paths["raw_geojson"].read_text(encoding="utf-8"))
            metadata = json.loads(paths["layer_metadata"].read_text(encoding="utf-8"))
            processed_gdf = gpd.read_file(paths["processed_vector"])
            mask = self.helper._load_binary_score_mask(paths["score_mask"])
            return {
                "source_key": source_key,
                "source_name": spec.source_name,
                "provider": spec.provider,
                "obs_date": spec.observation_date,
                "observation_date": spec.observation_date,
                "role": spec.role,
                "source_url": spec.source_url,
                "service_url": spec.service_url,
                "resolved_layer_id": int(metadata.get("id", 0)),
                "processed_vector": str(paths["processed_vector"]),
                "processed_vector_path": str(paths["processed_vector"]),
                "extended_obs_mask": str(paths["score_mask"]),
                "raw_geojson": str(paths["raw_geojson"]),
                "layer_metadata": str(paths["layer_metadata"]),
                "raw_feature_count": int(len(raw_geojson.get("features") or [])),
                "processed_feature_count": int(len(processed_gdf.index)),
                "raster_nonzero_cells": int(np.count_nonzero(mask > 0)),
                "mask_exists": True,
                "accepted_for_extended_quantitative": True,
                "scoreable_after_rasterization": bool(np.count_nonzero(mask > 0)),
                "processing_status": "reused_cached_experimental_layer",
                "processing_notes": "reused cached PhilSA experiment layer",
                "reuse_action": "reused_valid_local_experiment_store",
            }

        service_root, layer_id, layer_metadata = self._resolve_layer_endpoint(spec)
        query_url = f"{service_root.rstrip('/')}/{layer_id}/query"
        response = self.session.get(
            query_url,
            params={
                "where": "1=1",
                "outFields": "*",
                "returnGeometry": "true",
                "outSR": 4326,
                "f": "geojson",
            },
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        raw_geojson = response.json() or {}
        raw_features = raw_geojson.get("features") or []
        if not raw_features:
            raise RuntimeError(f"{spec.source_name} returned no GeoJSON features from {query_url}")

        raw_gdf = gpd.GeoDataFrame.from_features(raw_features).set_crs("EPSG:4326", allow_override=True)
        inferred_crs, inferred_notes = _infer_source_crs(raw_gdf, layer_metadata, raw_geojson)
        raw_gdf = raw_gdf.set_crs(inferred_crs, allow_override=True)
        raw_gdf, repair_notes = _repair_degree_scaled_geometries(
            raw_gdf,
            metadata=layer_metadata,
            payload=raw_geojson,
            expected_region=list(self.case.region),
        )
        cleaned_gdf, qa = clean_arcgis_geometries(
            raw_gdf=raw_gdf,
            expected_geometry_type="polygon",
            source_crs=inferred_crs,
            target_crs=self.grid.crs,
        )
        if cleaned_gdf.empty:
            raise RuntimeError(f"{spec.source_name} produced no valid polygon geometry after cleaning.")

        _write_json(paths["layer_metadata"], layer_metadata)
        _write_json(paths["raw_geojson"], raw_geojson)
        if paths["processed_vector"].exists():
            paths["processed_vector"].unlink()
        _sanitize_vector_columns_for_gpkg(cleaned_gdf).to_file(paths["processed_vector"], driver="GPKG")

        mask = rasterize_observation_layer(cleaned_gdf, self.grid)
        mask = apply_ocean_mask(mask, sea_mask=self.sea_mask, fill_value=0.0)
        save_raster(self.grid, mask.astype(np.float32), paths["score_mask"])

        raster_nonzero_cells = int(np.count_nonzero(mask > 0))
        notes = inferred_notes + repair_notes
        notes.append("; ".join(f"{key}={value}" for key, value in qa.items()))
        if raster_nonzero_cells <= 0:
            notes.append("zero scoreable ocean cells after applying canonical ocean mask")
        return {
            "source_key": source_key,
            "source_name": spec.source_name,
            "provider": spec.provider,
            "obs_date": spec.observation_date,
            "observation_date": spec.observation_date,
            "role": spec.role,
            "source_url": spec.source_url,
            "service_url": spec.service_url,
            "resolved_layer_id": int(layer_id),
            "processed_vector": str(paths["processed_vector"]),
            "processed_vector_path": str(paths["processed_vector"]),
            "extended_obs_mask": str(paths["score_mask"]),
            "raw_geojson": str(paths["raw_geojson"]),
            "layer_metadata": str(paths["layer_metadata"]),
            "raw_feature_count": int(len(raw_features)),
            "processed_feature_count": int(len(cleaned_gdf.index)),
            "raster_nonzero_cells": raster_nonzero_cells,
            "mask_exists": bool(paths["score_mask"].exists()),
            "accepted_for_extended_quantitative": True,
            "scoreable_after_rasterization": bool(raster_nonzero_cells > 0),
            "processing_status": "processed",
            "processing_notes": " | ".join(note for note in notes if note),
            "reuse_action": "force_refreshed_file" if force_refresh else "downloaded_new_experimental_file",
        }

    def _load_reinit_observation_pair(self) -> tuple[pd.Series, pd.Series]:
        rows = [self._materialize_philsa_layer(spec) for spec in PHILSA_LAYER_SPECS]
        frame = pd.DataFrame(rows)
        self._write_observation_manifest(frame)
        if not frame["provider"].astype(str).eq("PhilSA").all():
            raise RuntimeError("The March 3 -> March 4 experiment is blocked because a non-PhilSA provider was selected.")
        selected_names = set(frame["source_name"].astype(str))
        prohibited = selected_names.intersection(PROHIBITED_SOURCE_NAMES)
        if prohibited:
            raise RuntimeError(f"The March 3 -> March 4 experiment selected prohibited source(s): {sorted(prohibited)}")
        seed = frame.loc[
            frame["obs_date"].astype(str).eq(SEED_OBS_DATE)
            & frame["provider"].astype(str).eq("PhilSA")
            & frame["source_name"].astype(str).eq("MindoroOilSpill_Philsa_230303")
        ]
        target = frame.loc[
            frame["obs_date"].astype(str).eq(TARGET_OBS_DATE)
            & frame["provider"].astype(str).eq("PhilSA")
            & frame["source_name"].astype(str).eq("MindoroOilSpill_Philsa_230304")
        ]
        if len(seed.index) != 1 or len(target.index) != 1:
            raise RuntimeError("Expected exactly one March 3 PhilSA seed and one March 4 PhilSA target layer.")
        if int(target.iloc[0]["raster_nonzero_cells"]) <= 0:
            raise RuntimeError("March 4 PhilSA target rasterized to zero scoreable ocean cells.")
        return seed.iloc[0], target.iloc[0]

    def _write_observation_manifest(self, frame: pd.DataFrame) -> dict[str, Path]:
        csv_path = self.output_dir / "march3_4_philsa_5000_observation_manifest.csv"
        json_path = self.output_dir / "march3_4_philsa_5000_observation_manifest.json"
        _write_csv(csv_path, frame)
        payload = {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "experiment_id": EXPERIMENT_PHASE_ID,
            "philsa_only_pair": True,
            "seed_observation_date": SEED_OBS_DATE,
            "target_observation_date": TARGET_OBS_DATE,
            "selected_layers": frame.to_dict(orient="records"),
            "prohibited_sources_not_used": PROHIBITED_SOURCE_NAMES,
            "claim_boundary": CLAIM_BOUNDARY,
        }
        _write_json(json_path, payload)
        return {"csv": csv_path, "json": json_path}

    def _prepare_seed_release_artifacts(self, start_row: pd.Series) -> dict:
        processed_vector = self._existing_path_from_row(start_row, "processed_vector", "processed_vector_path")
        seed_mask_source = self._existing_path_from_row(start_row, "extended_obs_mask")
        seed_mask = self.helper._load_binary_score_mask(seed_mask_source)
        seed_mask_copy = self.output_dir / "march3_seed_mask_on_grid.tif"
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
        with _temporary_env({OFFICIAL_ELEMENT_COUNT_OVERRIDE_ENV: str(EXPERIMENT_REQUESTED_ELEMENT_COUNT)}):
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
            raise RuntimeError(f"March 3 -> March 4 PhilSA forecast failed for {branch.branch_id}: {result}")
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

    def _build_control_local_date_products(self, model_dir: Path, composite_dir: Path) -> dict[str, Any]:
        control_paths = sorted((model_dir / "forecast").glob("deterministic_control_*.nc"))
        per_date_paths = {
            date: composite_dir / f"control_footprint_{date}_localdate.tif"
            for date in self.window.forecast_local_dates
        }
        nonzero_by_date: dict[str, int] = {}
        if not control_paths:
            for date, path in per_date_paths.items():
                save_raster(self.grid, np.zeros((self.grid.height, self.grid.width), dtype=np.float32), path)
                nonzero_by_date[date] = 0
            return {
                "control_netcdf_path": "",
                "control_footprint_paths": {date: str(path) for date, path in per_date_paths.items()},
                "control_nonzero_by_date": nonzero_by_date,
            }
        if xr is None:
            raise ImportError("xarray is required to build deterministic local-date products.")

        control_path = control_paths[0]
        composites = {
            date: np.zeros((self.grid.height, self.grid.width), dtype=np.float32)
            for date in self.window.forecast_local_dates
        }
        with xr.open_dataset(control_path) as ds:
            times = normalize_time_index(ds["time"].values)
            for index, timestamp in enumerate(times):
                utc_timestamp = _normalize_utc(timestamp)
                local_date = pd.Timestamp(utc_timestamp).tz_localize("UTC").tz_convert(LOCAL_TIMEZONE).date().isoformat()
                if local_date not in composites:
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
                composites[local_date] = np.maximum(composites[local_date], hits)

        for date, data in composites.items():
            footprint = apply_ocean_mask((data > 0).astype(np.float32), sea_mask=self.sea_mask, fill_value=0.0)
            save_raster(self.grid, footprint.astype(np.float32), per_date_paths[date])
            nonzero_by_date[date] = int(np.count_nonzero(footprint > 0))
        return {
            "control_netcdf_path": str(control_path),
            "control_footprint_paths": {date: str(path) for date, path in per_date_paths.items()},
            "control_nonzero_by_date": nonzero_by_date,
        }

    def _build_branch_local_date_products(self, branch: ReinitBranchConfig, run_info: dict) -> dict:
        model_dir = Path(str(run_info["model_dir"]))
        composite_dir = self.output_dir / branch.output_slug / "forecast_datecomposites"
        composite_dir.mkdir(parents=True, exist_ok=True)

        per_date_prob_paths = {
            date: composite_dir / f"prob_presence_{date}_localdate.tif" for date in self.window.forecast_local_dates
        }
        per_date_p50_paths = {
            date: composite_dir / f"mask_p50_{date}_localdate.tif" for date in self.window.forecast_local_dates
        }
        per_date_p90_paths = {
            date: composite_dir / f"mask_p90_{date}_localdate.tif" for date in self.window.forecast_local_dates
        }

        member_paths = sorted((model_dir / "ensemble").glob("member_*.nc"))
        per_date_member_masks = {date: [] for date in self.window.forecast_local_dates}
        per_date_active_timestamps = {date: set() for date in self.window.forecast_local_dates}
        last_active_time: pd.Timestamp | None = None
        if member_paths:
            if xr is None:
                raise ImportError("xarray is required to build March 3 -> March 4 local-date composites.")
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
        forecast_p90_nonzero_by_date: dict[str, int] = {}
        for date in self.window.forecast_local_dates:
            probability = (
                np.mean(np.stack(per_date_member_masks[date], axis=0), axis=0).astype(np.float32)
                if per_date_member_masks[date]
                else np.zeros((self.grid.height, self.grid.width), dtype=np.float32)
            )
            probability = apply_ocean_mask(probability, sea_mask=self.sea_mask, fill_value=0.0)
            p50 = apply_ocean_mask((probability >= 0.5).astype(np.float32), sea_mask=self.sea_mask, fill_value=0.0)
            p90 = apply_ocean_mask((probability >= 0.9).astype(np.float32), sea_mask=self.sea_mask, fill_value=0.0)
            save_raster(self.grid, probability.astype(np.float32), per_date_prob_paths[date])
            save_raster(self.grid, p50.astype(np.float32), per_date_p50_paths[date])
            save_raster(self.grid, p90.astype(np.float32), per_date_p90_paths[date])
            forecast_nonzero_by_date[date] = int(np.count_nonzero(p50 > 0))
            forecast_p90_nonzero_by_date[date] = int(np.count_nonzero(p90 > 0))

        control_products = self._build_control_local_date_products(model_dir, composite_dir)
        target_date = self.window.scored_target_date
        forecast_result_status = str((run_info.get("forecast_result") or {}).get("status") or run_info.get("status") or "").strip()
        if forecast_nonzero_by_date[target_date] > 0:
            empty_forecast_reason = ""
        elif not member_paths and forecast_result_status:
            empty_forecast_reason = f"forecast_run_status_{forecast_result_status}"
        elif not member_paths:
            empty_forecast_reason = "no_ensemble_member_outputs_found"
        elif not per_date_active_timestamps[target_date]:
            empty_forecast_reason = "model_survival_did_not_reach_march4_local_date"
        else:
            empty_forecast_reason = "march4_local_activity_present_but_no_scoreable_ocean_presence_after_masking"

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
            "forecast_path": str(per_date_p50_paths[target_date]),
            "mask_p90_path": str(per_date_p90_paths[target_date]),
            "control_footprint_path": str((control_products["control_footprint_paths"] or {}).get(target_date, "")),
            "control_netcdf_path": str(control_products.get("control_netcdf_path") or ""),
            "march3_probability_path": str(per_date_prob_paths[SEED_OBS_DATE]),
            "march3_forecast_path": str(per_date_p50_paths[SEED_OBS_DATE]),
            "march3_mask_p90_path": str(per_date_p90_paths[SEED_OBS_DATE]),
            "march3_control_footprint_path": str((control_products["control_footprint_paths"] or {}).get(SEED_OBS_DATE, "")),
            "march4_probability_path": str(per_date_prob_paths[TARGET_OBS_DATE]),
            "march4_forecast_path": str(per_date_p50_paths[TARGET_OBS_DATE]),
            "march4_mask_p90_path": str(per_date_p90_paths[TARGET_OBS_DATE]),
            "march4_control_footprint_path": str((control_products["control_footprint_paths"] or {}).get(TARGET_OBS_DATE, "")),
            "member_count": int(len(member_paths)),
            "last_active_particle_time_utc": _iso_z(last_active_time) if last_active_time is not None else "",
            "march3_local_active_timestamp_count": int(len(per_date_active_timestamps[SEED_OBS_DATE])),
            "march4_local_active_timestamp_count": int(len(per_date_active_timestamps[TARGET_OBS_DATE])),
            "march3_local_active_timestamps": ";".join(sorted(per_date_active_timestamps[SEED_OBS_DATE])),
            "march4_local_active_timestamps": ";".join(sorted(per_date_active_timestamps[TARGET_OBS_DATE])),
            "reached_march3_local_date": bool(per_date_active_timestamps[SEED_OBS_DATE]),
            "reached_march4_local_date": bool(per_date_active_timestamps[TARGET_OBS_DATE]),
            "empty_forecast_reason": empty_forecast_reason,
            "forecast_nonzero_cells_from_march4_localdate_mask": forecast_nonzero_by_date[target_date],
            "mask_p90_nonzero_cells_from_march4_localdate_mask": forecast_p90_nonzero_by_date[target_date],
            "control_nonzero_cells_from_march4_localdate_mask": int(
                (control_products.get("control_nonzero_by_date") or {}).get(target_date, 0)
            ),
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
                    "pair_id": f"march4_philsa_branch_{product['branch_id']}",
                    "pair_role": "march4_nextday_philsa_branch_compare",
                    "score_group": "experimental_single_date_branch_compare",
                    "obs_date": TARGET_OBS_DATE,
                    "validation_dates_used": TARGET_OBS_DATE,
                    "seed_obs_date": SEED_OBS_DATE,
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
                    "mask_p90_path": str(product.get("mask_p90_path") or ""),
                    "control_footprint_path": str(product.get("control_footprint_path") or ""),
                    "observation_product": obs_path.name,
                    "observation_path": str(obs_path),
                    "metric": "FSS",
                    "windows_km": ",".join(str(value) for value in OFFICIAL_PHASE3B_WINDOWS_KM),
                    "track_label": self.track,
                    "track_id": self.track_id,
                    "track_title": self.track_label,
                    "phase_or_track": EXPERIMENT_PHASE_OR_TRACK,
                    "reporting_role": self.reporting_role,
                    "case_freeze_amendment_path": str(MINDORO_PRIMARY_VALIDATION_AMENDMENT_PATH),
                    "source_semantics": "march3_philsa_polygon_reinit_vs_march4_philsa_local_date_branch_p50",
                    "claim_boundary": CLAIM_BOUNDARY,
                    "empty_forecast_reason": str(product["empty_forecast_reason"]),
                    "precheck_csv": "",
                    "precheck_json": "",
                }
            )
        return pd.DataFrame(rows)

    def _write_outputs(
        self,
        scored_pairings: pd.DataFrame,
        fss_df: pd.DataFrame,
        diagnostics_df: pd.DataFrame,
        summary_df: pd.DataFrame,
        branch_survival_df: pd.DataFrame,
    ) -> dict[str, Path]:
        artifacts = {
            "pairing_manifest_csv": self.output_dir / "march3_4_philsa_5000_pairing_manifest.csv",
            "fss_csv": self.output_dir / "march3_4_philsa_5000_fss_by_window.csv",
            "diagnostics_csv": self.output_dir / "march3_4_philsa_5000_diagnostics.csv",
            "summary_csv": self.output_dir / "march3_4_philsa_5000_summary.csv",
            "branch_survival_csv": self.output_dir / "march3_4_philsa_5000_branch_survival_summary.csv",
            "forcing_manifest_json": self.output_dir / "march3_4_philsa_5000_forcing_window_manifest.json",
            "forcing_manifest_csv": self.output_dir / "march3_4_philsa_5000_forcing_window_manifest.csv",
        }
        _write_csv(artifacts["pairing_manifest_csv"], scored_pairings)
        _write_csv(artifacts["fss_csv"], fss_df)
        _write_csv(artifacts["diagnostics_csv"], diagnostics_df)
        _write_csv(artifacts["summary_csv"], summary_df)
        _write_csv(artifacts["branch_survival_csv"], branch_survival_df)
        return artifacts

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
            "phase_or_track": EXPERIMENT_PHASE_OR_TRACK,
            "recipe": recipe_name,
            "window": asdict(self.window),
            "rows": rows,
            "forcing_outage_policy": self.forcing_outage_policy,
            "degraded_continue_used": False,
            "missing_forcing_factors": [],
            "rerun_required": False,
            "status": "ready" if all(row["status"] == "ready" for row in rows) else "insufficient",
        }
        _write_json(self.output_dir / "march3_4_philsa_5000_forcing_window_manifest.json", payload)
        _write_csv(self.output_dir / "march3_4_philsa_5000_forcing_window_manifest.csv", pd.DataFrame(rows))
        return payload

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
        manifest_json = self.output_dir / "march3_4_philsa_5000_forcing_window_manifest.json"
        _write_json(manifest_json, payload)
        _write_csv(
            self.output_dir / "march3_4_philsa_5000_forcing_window_manifest.csv",
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

    def _write_forcing_blocked_note(self, failed_rows: list[dict]) -> Path:
        path = self.output_dir / "march3_4_philsa_5000_forcing_blocked.md"
        lines = [
            "# March 3 -> March 4 PhilSA 5,000-Element Forcing Coverage Blocked",
            "",
            "The experimental PhilSA-only rerun did not rerun the forecast branches because the prepared forcing window was incomplete.",
            "",
            "## Blocking Rows",
            "",
        ]
        lines.extend(f"- {row.get('forcing_kind', 'forcing')}: {row.get('stop_reason', '')}" for row in failed_rows)
        _write_text(path, "\n".join(lines) + "\n")
        return path

    def _clear_stale_forcing_blocked_note(self) -> None:
        blocked_note = self.output_dir / "march3_4_philsa_5000_forcing_blocked.md"
        if blocked_note.exists():
            blocked_note.unlink()

    def _write_decision_note(
        self,
        summary_df: pd.DataFrame,
        start_row: pd.Series,
        target_row: pd.Series,
        seed_release: dict,
    ) -> Path:
        path = self.output_dir / "march3_4_philsa_5000_decision_note.md"
        both_empty = pd.to_numeric(summary_df.get("forecast_nonzero_cells"), errors="coerce").fillna(0).eq(0).all()
        survival_blocked = both_empty and summary_df["empty_forecast_reason"].astype(str).str.contains(
            "model_survival", case=False, na=False
        ).all()
        best_row = summary_df.sort_values(["mean_fss", "fss_1km", "branch_precedence"], ascending=[False, False, True]).iloc[0]
        lines = [
            "# EXPERIMENTAL / NOT THESIS-FACING: Mindoro PhilSA March 3 -> March 4 5,000-Element Test",
            "",
            f"- Status boundary: {CLAIM_BOUNDARY}",
            "- Thesis-facing: false",
            "- Reportable: false",
            "- Experimental only: true",
            f"- Seed source name: {start_row['source_name']}",
            f"- Seed provider: {start_row['provider']}",
            f"- Target source name: {target_row['source_name']}",
            f"- Target provider: {target_row['provider']}",
            f"- Seed observation date: {SEED_OBS_DATE}",
            f"- Scored target date: {TARGET_OBS_DATE}",
            f"- Release start UTC: {seed_release['release_start_utc']}",
            f"- Requested element count: {self.requested_element_count}",
            f"- Best branch by mean FSS: {best_row['branch_id']}",
            f"- Best branch mean FSS: {float(best_row['mean_fss']):.6f}",
            (
                f"- Best branch FSS 1/3/5/10 km: {float(best_row['fss_1km']):.6f} / "
                f"{float(best_row['fss_3km']):.6f} / {float(best_row['fss_5km']):.6f} / {float(best_row['fss_10km']):.6f}"
            ),
            "",
            "March 3 and March 4 PhilSA layers are separate dated PhilSA satellite-derived observation products available as machine-readable FeatureServer polygon layers.",
            "This experiment is a candidate next-day public-observation validation test.",
            "It is not yet promoted to thesis-facing evidence.",
            "Do not call it fully statistically independent evidence.",
            "Do not claim different satellites unless exact sensor/image metadata is verified from the source.",
            "Both layers come from the same broader PhilSA/Disasters Charter mapping provenance and the same continuing oil-spill event.",
            "This run does not replace canonical B1 and does not modify or relabel the existing March 13 -> March 14 B1 result.",
            "Fate/oil-type support was not run for this 5,000-element March 3 -> 4 PhilSA experiment because no matching safe experimental implementation was available.",
            "",
        ]
        if survival_blocked:
            lines.append("Decision: March 4 comparison is blocked by model survival, not by missing PhilSA public data.")
        elif both_empty:
            lines.append("Decision: Both branches produced empty March 4 p50 masks; inspect empty_forecast_reason before interpreting skill.")
        else:
            lines.append("Decision: At least one branch produced a scoreable March 4 p50 mask, so this is a completed experimental archive/provenance run only.")
        _write_text(path, "\n".join(lines) + "\n")
        return path

    def _write_qa_artifacts(self, summary_df: pd.DataFrame, seed_release: dict, target_row: pd.Series) -> dict[str, Path]:
        outputs: dict[str, Path] = {}
        if plt is None:
            return outputs

        seed_mask = self.helper._load_binary_score_mask(Path(str(seed_release["seed_mask_path"])))
        target_mask = self.helper._load_binary_score_mask(self._existing_path_from_row(target_row, "extended_obs_mask"))
        target_copy = self.output_dir / "march4_target_mask_on_grid.tif"
        save_raster(self.grid, target_mask.astype(np.float32), target_copy)
        outputs["march4_target_mask_on_grid"] = target_copy

        fig, axes = plt.subplots(2, 2, figsize=(10, 10))
        axes = axes.reshape(-1)
        self._render_overlay(axes[0], seed_mask, target_mask, "March 3 PhilSA Seed vs March 4 PhilSA Target")
        for ax, (_, row) in zip(axes[1:], summary_df.iterrows()):
            forecast = self.helper._load_binary_score_mask(Path(str(row["forecast_path"])))
            obs = self.helper._load_binary_score_mask(Path(str(row["observation_path"])))
            self._render_overlay(ax, forecast, obs, f"March 4 PhilSA {row['branch_id']} p50")
        for ax in axes[1 + len(summary_df.index) :]:
            ax.set_axis_off()
        board_path = self.output_dir / "march3_4_philsa_5000_board.png"
        fig.suptitle("Experimental PhilSA March 3 -> March 4 5,000-Element Test", fontsize=12)
        fig.tight_layout()
        fig.savefig(board_path, dpi=150, bbox_inches="tight")
        plt.close(fig)
        outputs["board_png"] = board_path

        for _, row in summary_df.iterrows():
            forecast = self.helper._load_binary_score_mask(Path(str(row["forecast_path"])))
            obs = self.helper._load_binary_score_mask(Path(str(row["observation_path"])))
            fig, ax = plt.subplots(figsize=(7, 7))
            self._render_overlay(ax, forecast, obs, f"March 4 PhilSA {row['branch_id']} p50")
            fig.tight_layout()
            path = self.output_dir / f"qa_march4_philsa_{row['branch_id']}_overlay.png"
            fig.savefig(path, dpi=150, bbox_inches="tight")
            plt.close(fig)
            outputs[f"qa_{row['branch_id']}_overlay"] = path
        return outputs

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
        path = self.output_dir / "march3_4_philsa_5000_run_manifest.json"
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
            "experiment_id": EXPERIMENT_PHASE_ID,
            "launcher_entry_id": EXPERIMENT_LAUNCHER_ENTRY_ID,
            "workflow_mode": self.case.workflow_mode,
            "run_name": self.case.run_name,
            "track": self.track,
            "track_id": self.track_id,
            "track_label": self.track_label,
            "phase_or_track": EXPERIMENT_PHASE_OR_TRACK,
            "appendix_only": True,
            "reporting_role": self.reporting_role,
            "primary_public_validation": False,
            "thesis_facing": False,
            "reportable": False,
            "experimental_only": True,
            "artifact_class": "experimental_archive_provenance_only",
            "claim_boundary": CLAIM_BOUNDARY,
            "requested_element_count": int(self.requested_element_count),
            "expected_element_count": int(EXPERIMENT_REQUESTED_ELEMENT_COUNT),
            "element_count_detected_from_manifest": _consensus_int(branch_actual_counts),
            "element_count_detected_from_manifest_by_branch": branch_actual_counts,
            "expected_ensemble_member_count": int(EXPECTED_ENSEMBLE_MEMBER_COUNT),
            "actual_member_count_detected": _consensus_int(branch_member_counts),
            "actual_member_count_detected_by_branch": branch_member_counts,
            "case_definition": {
                "base_case_definition_path": str(MINDORO_BASE_CASE_CONFIG_PATH),
                "case_freeze_amendment_path": str(MINDORO_PRIMARY_VALIDATION_AMENDMENT_PATH),
                "base_case_definition_preserved": True,
                "canonical_b1_output_preserved": True,
                "canonical_b1_window": "2023-03-13_to_2023-03-14",
                "experimental_window": "2023-03-03_to_2023-03-04",
            },
            "window": asdict(self.window),
            "selected_start_source": {
                "source_key": str(start_row["source_key"]),
                "source_name": str(start_row["source_name"]),
                "provider": str(start_row.get("provider", "")),
                "obs_date": str(start_row["obs_date"]),
                "source_url": str(start_row.get("source_url", "")),
                "service_url": str(start_row.get("service_url", "")),
                "processed_vector": str(self._existing_path_from_row(start_row, "processed_vector", "processed_vector_path")),
                "extended_obs_mask": str(self._existing_path_from_row(start_row, "extended_obs_mask")),
            },
            "selected_target_source": {
                "source_key": str(target_row["source_key"]),
                "source_name": str(target_row["source_name"]),
                "provider": str(target_row.get("provider", "")),
                "obs_date": str(target_row["obs_date"]),
                "source_url": str(target_row.get("source_url", "")),
                "service_url": str(target_row.get("service_url", "")),
                "processed_vector": str(self._existing_path_from_row(target_row, "processed_vector", "processed_vector_path")),
                "extended_obs_mask": str(self._existing_path_from_row(target_row, "extended_obs_mask")),
            },
            "source_provenance": {
                "both_dates_provider": "PhilSA",
                "philsa_only_pair": True,
                "prohibited_sources_not_used": PROHIBITED_SOURCE_NAMES,
                "interpretation_note": (
                    "Both layers come from the same broader PhilSA/Disasters Charter mapping provenance and the same continuing oil-spill event."
                ),
            },
            "seed_release": seed_release,
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
            "canonical_bundle": False,
            "artifacts": {
                **{key: str(value) for key, value in artifacts.items()},
                "decision_note_md": str(decision_note),
                "seed_mask_tif": str(seed_release["seed_mask_path"]),
                "observation_manifest_csv": str(self.output_dir / "march3_4_philsa_5000_observation_manifest.csv"),
                "observation_manifest_json": str(self.output_dir / "march3_4_philsa_5000_observation_manifest.json"),
                **{key: str(value) for key, value in qa_paths.items()},
            },
            "score_summary": summary_df.to_dict(orient="records"),
        }
        _write_json(path, payload)
        return path

    def _assert_element_and_member_counts(self, manifest_path: Path) -> None:
        manifest = _read_json(manifest_path)
        actual_count = manifest.get("element_count_detected_from_manifest")
        if int(actual_count or 0) != EXPERIMENT_REQUESTED_ELEMENT_COUNT:
            raise RuntimeError(
                f"Detected actual element count {actual_count}; expected {EXPERIMENT_REQUESTED_ELEMENT_COUNT}."
            )
        member_count = manifest.get("actual_member_count_detected")
        if int(member_count or 0) != EXPECTED_ENSEMBLE_MEMBER_COUNT:
            raise RuntimeError(f"Detected ensemble member count {member_count}; expected {EXPECTED_ENSEMBLE_MEMBER_COUNT}.")

    def _run_core(self) -> dict[str, Any]:
        start_row, target_row = self._load_reinit_observation_pair()
        self._ensure_scoreable_observation(start_row, role_label="march3_seed")
        self._ensure_scoreable_observation(target_row, role_label="march4_target")
        seed_release = self._prepare_seed_release_artifacts(start_row)
        selection, recipe_source = self._resolve_recipe()
        forcing_paths = self._prepare_extended_forcing(selection.recipe)
        forcing_manifest = self._write_forcing_window_manifest(selection.recipe, forcing_paths)
        failed_forcing = [row for row in forcing_manifest["rows"] if row["status"] != "ready"]
        if failed_forcing:
            note = self._write_forcing_blocked_note(failed_forcing)
            self._verify_locked_outputs_unchanged()
            raise RuntimeError(
                "March 3 -> March 4 PhilSA experiment forcing coverage is incomplete. "
                f"See {self.output_dir / 'march3_4_philsa_5000_forcing_window_manifest.json'} and {note}."
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
        self._assert_element_and_member_counts(manifest)
        self._verify_locked_outputs_unchanged()
        return {
            "output_dir": str(self.output_dir),
            "summary_csv": str(artifacts["summary_csv"]),
            "fss_csv": str(artifacts["fss_csv"]),
            "diagnostics_csv": str(artifacts["diagnostics_csv"]),
            "pairing_manifest_csv": str(artifacts["pairing_manifest_csv"]),
            "branch_survival_csv": str(artifacts["branch_survival_csv"]),
            "forcing_manifest_json": str(artifacts["forcing_manifest_json"]),
            "decision_note_md": str(decision_note),
            "run_manifest_json": str(manifest),
            "board_png": str(qa_paths.get("board_png", "")),
            "start_source_name": str(start_row["source_name"]),
            "target_source_name": str(target_row["source_name"]),
        }

    def _write_blocked_note(
        self,
        *,
        before_snapshot_path: Path,
        after_snapshot_path: Path,
        protected_snapshot_unchanged: bool,
        protected_snapshot_diff: dict[str, list[str]],
        exception_text: str,
    ) -> Path:
        path = self.output_dir / "experiment_blocked_note.md"
        lines = [
            "# EXPERIMENTAL / NOT THESIS-FACING: March 3 -> March 4 PhilSA 5,000-Element Experiment Blocked",
            "",
            "- Status: blocked",
            f"- Phase: `{EXPERIMENT_PHASE_ID}`",
            f"- Launcher entry: `{EXPERIMENT_LAUNCHER_ENTRY_ID}`",
            f"- Command equivalent: `{self.command_text}`",
            f"- Experimental output directory: `{self.output_dir}`",
            f"- Requested element count: `{EXPERIMENT_REQUESTED_ELEMENT_COUNT}`",
            f"- Protected outputs unchanged: `{str(protected_snapshot_unchanged).lower()}`",
            f"- Protected snapshot before: `{before_snapshot_path}`",
            f"- Protected snapshot after: `{after_snapshot_path}`",
            "",
            "## Exception",
            "",
            "```text",
            exception_text.strip() or "No exception text captured.",
            "```",
        ]
        if any(protected_snapshot_diff.values()):
            lines.extend(["", "## Protected Output Diff", ""])
            if protected_snapshot_diff["added"]:
                lines.append(f"- Added: {', '.join(protected_snapshot_diff['added'])}")
            if protected_snapshot_diff["removed"]:
                lines.append(f"- Removed: {', '.join(protected_snapshot_diff['removed'])}")
            if protected_snapshot_diff["changed"]:
                lines.append(f"- Changed: {', '.join(protected_snapshot_diff['changed'])}")
        _write_text(path, "\n".join(lines) + "\n")
        return path

    def _augment_manifest_with_guardrails(
        self,
        manifest_path: Path,
        *,
        before_snapshot_path: Path,
        after_snapshot_path: Path,
        protected_snapshot_unchanged: bool,
        protected_snapshot_diff: dict[str, list[str]],
    ) -> None:
        if not manifest_path.exists():
            return
        manifest = _read_json(manifest_path)
        manifest["guardrail_snapshots"] = {
            "protected_outputs_snapshot_before": str(before_snapshot_path),
            "protected_outputs_snapshot_after": str(after_snapshot_path),
            "protected_outputs_unchanged": bool(protected_snapshot_unchanged),
            "protected_outputs_diff": protected_snapshot_diff,
        }
        manifest["canonical_outputs_modified"] = not bool(protected_snapshot_unchanged)
        _write_json(manifest_path, manifest)

    def run(self) -> dict[str, Any]:
        if self.output_dir.resolve() == (self.case_output_dir / "phase3b_extended_public_scored_march13_14_reinit").resolve():
            raise RuntimeError("Experimental PhilSA output directory must be separate from canonical B1.")

        before_snapshot_path = self.output_dir / "protected_outputs_snapshot_before.json"
        after_snapshot_path = self.output_dir / "protected_outputs_snapshot_after.json"
        before_snapshot = _snapshot_paths(self.repo_root, self.protected_output_dirs)
        _write_json(
            before_snapshot_path,
            {
                "generated_at_utc": datetime.now(timezone.utc).isoformat(),
                "method": "size_and_mtime_ns",
                "paths": before_snapshot,
            },
        )

        results: dict[str, Any] | None = None
        exception_text = ""
        try:
            results = self._run_core()
        except Exception:
            exception_text = traceback.format_exc()
        finally:
            after_snapshot = _snapshot_paths(self.repo_root, self.protected_output_dirs)
            _write_json(
                after_snapshot_path,
                {
                    "generated_at_utc": datetime.now(timezone.utc).isoformat(),
                    "method": "size_and_mtime_ns",
                    "paths": after_snapshot,
                },
            )

        before_payload = _read_json(before_snapshot_path)
        after_payload = _read_json(after_snapshot_path)
        protected_snapshot_diff = _snapshot_diff(before_payload.get("paths") or {}, after_payload.get("paths") or {})
        protected_snapshot_unchanged = not any(protected_snapshot_diff.values())

        manifest_path = self.output_dir / "march3_4_philsa_5000_run_manifest.json"
        self._augment_manifest_with_guardrails(
            manifest_path,
            before_snapshot_path=before_snapshot_path,
            after_snapshot_path=after_snapshot_path,
            protected_snapshot_unchanged=protected_snapshot_unchanged,
            protected_snapshot_diff=protected_snapshot_diff,
        )

        if exception_text:
            note = self._write_blocked_note(
                before_snapshot_path=before_snapshot_path,
                after_snapshot_path=after_snapshot_path,
                protected_snapshot_unchanged=protected_snapshot_unchanged,
                protected_snapshot_diff=protected_snapshot_diff,
                exception_text=exception_text,
            )
            raise RuntimeError(f"Mindoro PhilSA March 3 -> March 4 experiment blocked. See {note}")

        if not protected_snapshot_unchanged:
            note = self._write_blocked_note(
                before_snapshot_path=before_snapshot_path,
                after_snapshot_path=after_snapshot_path,
                protected_snapshot_unchanged=protected_snapshot_unchanged,
                protected_snapshot_diff=protected_snapshot_diff,
                exception_text="Protected outputs changed during the experimental run.",
            )
            raise RuntimeError(f"Mindoro PhilSA March 3 -> March 4 experiment blocked. See {note}")

        blocked_note = self.output_dir / "experiment_blocked_note.md"
        if blocked_note.exists():
            blocked_note.unlink()
        assert results is not None
        manifest = _read_json(manifest_path)
        results.update(
            {
                "element_count_detected_from_manifest": manifest.get("element_count_detected_from_manifest"),
                "actual_member_count_detected": manifest.get("actual_member_count_detected"),
                "protected_outputs_unchanged": protected_snapshot_unchanged,
                "protected_outputs_diff": protected_snapshot_diff,
                "guardrail_snapshot_before_json": str(before_snapshot_path),
                "guardrail_snapshot_after_json": str(after_snapshot_path),
            }
        )
        return results


def run_mindoro_march3_4_philsa_5000_experiment() -> dict[str, Any]:
    return MindoroMarch34PhilSA5000ExperimentService().run()


def main() -> int:
    results = run_mindoro_march3_4_philsa_5000_experiment()
    print("Experimental PhilSA March 3 -> March 4 5,000-element run complete.")
    print(f"Output directory: {results['output_dir']}")
    print(f"Summary: {results['summary_csv']}")
    print(f"FSS: {results['fss_csv']}")
    print(f"Diagnostics: {results['diagnostics_csv']}")
    print(f"Decision note: {results['decision_note_md']}")
    print(f"Run manifest: {results['run_manifest_json']}")
    print(f"Actual element count detected from manifest: {results['element_count_detected_from_manifest']}")
    print(f"Actual ensemble member count detected: {results['actual_member_count_detected']}")
    print(f"Protected outputs unchanged: {results['protected_outputs_unchanged']}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
