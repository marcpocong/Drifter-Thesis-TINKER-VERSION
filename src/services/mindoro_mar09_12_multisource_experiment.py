"""Experimental Mindoro March 9-12 multi-source validation lane.

This module is intentionally archive-only. It creates a separate experiment
tree and does not update canonical B1, Track A, DWH, publication, or final
validation outputs.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import shutil
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import requests
import yaml

from src.core.case_context import get_case_context
from src.exceptions.custom import ForcingOutagePhaseSkipped
from src.helpers.metrics import calculate_fss
from src.helpers.raster import extract_particles_at_time, rasterize_observation_layer, rasterize_particles, save_raster
from src.helpers.raster import GridBuilder
from src.helpers.scoring import apply_ocean_mask, load_sea_mask_array, precheck_same_grid
from src.services.arcgis import (
    _infer_source_crs,
    _repair_degree_scaled_geometries,
    _sanitize_vector_columns_for_gpkg,
    clean_arcgis_geometries,
)
from src.services.ensemble import normalize_time_index, run_official_spill_forecast
from src.services.ingestion import DataIngestionService
from src.services.scoring import OFFICIAL_PHASE3B_WINDOWS_KM, Phase3BScoringService
from src.utils.forcing_outage_policy import (
    FORCING_OUTAGE_POLICY_CONTINUE_DEGRADED,
    resolve_forcing_outage_policy,
    resolve_forcing_source_budget_seconds,
    source_id_for_recipe_component,
)
from src.utils.io import _resolve_polygon_reference_point, get_case_output_dir, resolve_recipe_selection
from src.utils.io import (
    BASELINE_RECIPE_OVERRIDE_ENV,
    RecipeSelection,
    find_current_vars,
    find_wave_vars,
    find_wind_vars,
)
from src.utils.local_input_store import PERSISTENT_LOCAL_INPUT_STORE, persistent_local_input_dir, stage_store_file

try:
    import geopandas as gpd
except ImportError:  # pragma: no cover
    gpd = None

try:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.patches as mpatches
    import matplotlib.pyplot as plt
    from matplotlib.colors import ListedColormap
except ImportError:  # pragma: no cover
    mpatches = None
    plt = None
    ListedColormap = None

try:
    import rasterio
except ImportError:  # pragma: no cover
    rasterio = None

try:
    import xarray as xr
except ImportError:  # pragma: no cover
    xr = None


RUN_NAME = "CASE_MINDORO_RETRO_2023"
EXPERIMENT_ID = "mindoro_mar09_12_multisource_experiment"
EXPERIMENT_OUTPUT_DIR = Path("output") / RUN_NAME / "experiments" / "mar09_11_12_multisource"
CONFIG_PATH = Path("config") / "experiments" / "mindoro_mar09_12_multisource.yaml"
DOC_PATH = Path("docs") / "EXPERIMENT_MINDORO_MAR09_12_MULTISOURCE.md"
LOCAL_TIMEZONE = "Asia/Manila"
EXPECTED_ENSEMBLE_MEMBER_COUNT = 50
REQUEST_TIMEOUT = 60
SUCCESSFUL_FORCING_STATUSES = {"downloaded", "cached", "reused_validated_cache", "reused_local_file"}
ELEMENT_CAP = 5000
ALLOW_MINIMAL_CASE_FORCING_FETCH_ENV = "ALLOW_MINIMAL_CASE_FORCING_FETCH"
RUN_EXPERIMENT_ENV = "RUN_MINDORO_MAR09_12_MULTISOURCE_EXPERIMENT"
OPENDRIFT_ELEMENT_OVERRIDE_ENV = "OFFICIAL_ELEMENT_COUNT_OVERRIDE"
FORCING_WINDOW_START_UTC = "2023-03-08T18:00:00Z"
FORCING_WINDOW_END_UTC = "2023-03-13T00:00:00Z"
FORCING_BBOX = {
    "lon_min": 114.5,
    "lon_max": 122.5,
    "lat_min": 5.5,
    "lat_max": 15.0,
}
PRIMARY_BASELINE_SELECTION_PATH = Path("config") / "phase1_baseline_selection.yaml"
PRIMARY_VALIDATION_AMENDMENT_PATH = Path("config") / "case_mindoro_retro_2023_phase3b_primary_validation_amendment.yaml"
B1_REINIT_OUTPUT_DIR = Path("output") / RUN_NAME / "phase3b_extended_public_scored_march13_14_reinit"
BANNED_DRY_RUN_TERMS = [
    "phase1 execution",
    "drifter_6hour_qc",
    "accepted drifter segments",
    "gfs historical preflight",
    "historical gfs ingestion",
    "monthly gfs fetch",
    "recipe ranking",
]

CLAIM_BOUNDARY = (
    "Experimental/archive-only March 9-12 multi-source lane. It does not replace "
    "the current B1 thesis-facing public-observation validation claim."
)

PROTECTED_OUTPUT_ROOTS = [
    Path("output") / "final_validation_package",
    Path("output") / "figure_package_publication",
    Path("output") / "final_reproducibility_package",
    Path("output") / "Phase 3B March13-14 Final Output",
    Path("output") / "Phase 3B March13-14 Final Output Archive R0 Legacy",
    Path("output") / "Phase 3C DWH Final Output",
    Path("output") / RUN_NAME / "phase3b",
    Path("output") / RUN_NAME / "phase3b_extended_public_scored_march13_14_reinit",
    Path("output") / RUN_NAME / "phase3b_extended_public_scored_march13_14_reinit_pygnome_comparison",
    Path("output") / RUN_NAME / "phase3b_multidate_public",
    Path("output") / RUN_NAME / "public_obs_appendix",
]

POSITIVE_OIL_CLASSES = {"possible oil", "possible thicker oil"}


@dataclass(frozen=True)
class ObservationSourceSpec:
    mask_id: str
    date_label: str
    satellite: str
    source_name: str
    source_type: str
    service_url: str
    report_url: str
    role: str
    source_label: str
    nominal_datetime_utc: str
    acquisition_hint_utc: str = ""
    notes: str = ""


@dataclass(frozen=True)
class ForecastPairSpec:
    pair_id: str
    seed_mask_id: str
    target_mask_id: str
    seed_date: str
    target_date: str
    nominal_lead_h: int
    start_utc: str
    target_utc: str
    pygnome_random_seed: int


OBS_SOURCES = [
    ObservationSourceSpec(
        mask_id="OBS_MAR09_TERRAMODIS",
        date_label="2023-03-09",
        satellite="Terra MODIS",
        source_name="MindoroOilSpill_NOAA_230309",
        source_type="ArcGIS FeatureServer polygon",
        service_url="https://services1.arcgis.com/RTK5Unh1Z71JKIiR/arcgis/rest/services/MindoroOilSpill_NOAA_230309/FeatureServer",
        report_url="https://disasterscharter.org/cos-api/api/file/public/article-image/28555013",
        role="seed mask for March 9 -> March 11 and March 9 -> March 12 forecasts",
        source_label="OBS_MAR09_TERRAMODIS",
        nominal_datetime_utc="2023-03-09T00:00:00Z",
        acquisition_hint_utc="2023-03-09T00:00:00Z",
        notes="Source metadata exposes acquisition date only; exact acquisition time is not public in the stored metadata.",
    ),
    ObservationSourceSpec(
        mask_id="OBS_MAR11_ICEYE",
        date_label="2023-03-11",
        satellite="ICEYE",
        source_name="MindoroOilSpill_NOAA_230311",
        source_type="ArcGIS FeatureServer polygon",
        service_url="https://services1.arcgis.com/RTK5Unh1Z71JKIiR/arcgis/rest/services/MindoroOilSpill_NOAA_230311/FeatureServer",
        report_url="https://disasterscharter.org/cos-api/api/file/public/article-image/28555023",
        role="validation mask for March 9 +48 h and seed mask for March 11 -> March 12",
        source_label="OBS_MAR11_ICEYE",
        nominal_datetime_utc="2023-03-10T16:00:00Z",
        acquisition_hint_utc="2023-03-11T00:00:00Z",
        notes="Source metadata exposes acquisition date only; exact acquisition time is not public in the stored metadata.",
    ),
    ObservationSourceSpec(
        mask_id="OBS_MAR12_ICEYE",
        date_label="2023-03-12",
        satellite="ICEYE",
        source_name="MindoroOilSpill_NOAA_230312",
        source_type="ArcGIS FeatureServer polygon",
        service_url="https://services1.arcgis.com/RTK5Unh1Z71JKIiR/arcgis/rest/services/MindoroOilSpill_NOAA_230312/FeatureServer",
        report_url="https://disasterscharter.org/cos-api/api/file/public/article-image/28555033",
        role="individual March 12 source mask for OBS_MAR12_COMBINED",
        source_label="OBS_MAR12_ICEYE",
        nominal_datetime_utc="2023-03-11T16:00:00Z",
        acquisition_hint_utc="2023-03-12T00:00:00Z",
        notes="Source metadata exposes acquisition date only; exact acquisition time is not public in the stored metadata.",
    ),
    ObservationSourceSpec(
        mask_id="OBS_MAR12_WORLDVIEW3_NOAA_230313",
        date_label="2023-03-12",
        satellite="WorldView-3",
        source_name="MindoroOilSpill_NOAA_230313",
        source_type="ArcGIS FeatureServer polygon",
        service_url="https://services1.arcgis.com/RTK5Unh1Z71JKIiR/arcgis/rest/services/MindoroOilSpill_NOAA_230313/FeatureServer",
        report_url="",
        role="individual March 12 source mask for OBS_MAR12_COMBINED",
        source_label="OBS_MAR12_WORLDVIEW3_NOAA_230313",
        nominal_datetime_utc="2023-03-11T16:00:00Z",
        acquisition_hint_utc="2023-03-12T00:00:00Z",
        notes="NOAA 230313 item metadata cites WorldView-3 acquired on 12/03/2023; exact time is not public in the stored metadata.",
    ),
    ObservationSourceSpec(
        mask_id="OBS_MAR12_WORLDVIEW3_NOAA_230314",
        date_label="2023-03-12",
        satellite="WorldView-3",
        source_name="MindoroOilSpill_NOAA_230314",
        source_type="ArcGIS FeatureServer polygon",
        service_url="https://services1.arcgis.com/RTK5Unh1Z71JKIiR/arcgis/rest/services/Possible_Oil_Spills_March_14/FeatureServer",
        report_url="",
        role="individual March 12 source mask for OBS_MAR12_COMBINED",
        source_label="OBS_MAR12_WORLDVIEW3_NOAA_230314",
        nominal_datetime_utc="2023-03-11T16:00:00Z",
        acquisition_hint_utc="2023-03-12T00:00:00Z",
        notes=(
            "The direct MindoroOilSpill_NOAA_230314 service root is not a usable layer endpoint; "
            "the public ArcGIS item resolves to Possible_Oil_Spills_March_14/FeatureServer."
        ),
    ),
]

MARCH12_SOURCE_MASK_IDS = [
    "OBS_MAR12_WORLDVIEW3_NOAA_230314",
    "OBS_MAR12_WORLDVIEW3_NOAA_230313",
    "OBS_MAR12_ICEYE",
]

FORECAST_PAIRS = [
    ForecastPairSpec(
        pair_id="E1_MAR09_TO_MAR11_48H",
        seed_mask_id="OBS_MAR09_TERRAMODIS",
        target_mask_id="OBS_MAR11_ICEYE",
        seed_date="2023-03-09",
        target_date="2023-03-11",
        nominal_lead_h=48,
        start_utc="2023-03-09T00:00:00Z",
        target_utc="2023-03-11T00:00:00Z",
        pygnome_random_seed=20230309,
    ),
    ForecastPairSpec(
        pair_id="E2_MAR09_TO_MAR12_72H",
        seed_mask_id="OBS_MAR09_TERRAMODIS",
        target_mask_id="OBS_MAR12_COMBINED",
        seed_date="2023-03-09",
        target_date="2023-03-12",
        nominal_lead_h=72,
        start_utc="2023-03-09T00:00:00Z",
        target_utc="2023-03-12T00:00:00Z",
        pygnome_random_seed=20230312,
    ),
    ForecastPairSpec(
        pair_id="E3_MAR11_TO_MAR12_24H",
        seed_mask_id="OBS_MAR11_ICEYE",
        target_mask_id="OBS_MAR12_COMBINED",
        seed_date="2023-03-11",
        target_date="2023-03-12",
        nominal_lead_h=24,
        start_utc="2023-03-11T00:00:00Z",
        target_utc="2023-03-12T00:00:00Z",
        pygnome_random_seed=20230311,
    ),
]

SCORECARD_COLUMNS = [
    "experiment_id",
    "pair_id",
    "seed_mask_id",
    "target_mask_id",
    "nominal_lead_h",
    "actual_elapsed_h",
    "obs_time_offset_h",
    "model_surface",
    "forcing_recipe_id",
    "forcing_recipe_source",
    "phase1_rerun",
    "element_cap",
    "fss_1km",
    "fss_3km",
    "fss_5km",
    "fss_10km",
    "mean_fss",
    "notes",
]

DIAGNOSTIC_COLUMNS = [
    "experiment_id",
    "pair_id",
    "model_surface",
    "forecast_cells",
    "observed_cells",
    "forecast_area_km2",
    "observed_area_km2",
    "area_ratio",
    "iou",
    "dice",
    "nearest_distance_m",
    "centroid_distance_m",
    "notes",
]

INVENTORY_COLUMNS = [
    "mask_id",
    "date_label",
    "satellite",
    "source_name",
    "source_type",
    "source_url_or_service",
    "acquisition_datetime_utc",
    "report_datetime_utc",
    "crs_original",
    "crs_scoring",
    "area_km2",
    "positive_cells",
    "ingestion_status",
    "notes",
]


def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json_default(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return None if np.isnan(value) else float(value)
    if isinstance(value, np.ndarray):
        return value.tolist()
    if isinstance(value, (pd.Timestamp, datetime)):
        return _iso_z(value)
    return str(value)


def _write_json(path: Path, payload: dict | list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, default=_json_default) + "\n", encoding="utf-8")


def _write_csv(path: Path, frame: pd.DataFrame, columns: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if columns:
        frame = frame.reindex(columns=columns)
    frame.to_csv(path, index=False)


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _iso_z(value: Any) -> str:
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is not None:
        timestamp = timestamp.tz_convert("UTC").tz_localize(None)
    return timestamp.strftime("%Y-%m-%dT%H:%M:%SZ")


def _normalize_utc(value: Any) -> pd.Timestamp:
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is not None:
        return timestamp.tz_convert("UTC").tz_localize(None)
    return timestamp


def _safe_name(value: str) -> str:
    safe = "".join(char.lower() if char.isalnum() else "_" for char in str(value)).strip("_")
    return safe or "item"


def _read_raster(path: Path) -> np.ndarray:
    if rasterio is None:
        raise ImportError("rasterio is required to read experiment rasters")
    with rasterio.open(path) as src:
        return src.read(1).astype(np.float32)


def _read_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8")) or {}


def _nested_get(payload: dict[str, Any], dotted_path: str) -> Any:
    current: Any = payload
    for part in dotted_path.split("."):
        if not isinstance(current, dict):
            return None
        current = current.get(part)
    return current


def _hash_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def snapshot_protected_outputs(paths: list[Path] | None = None) -> dict[str, dict[str, Any]]:
    """Return a compact fingerprint of protected canonical outputs."""
    snapshot: dict[str, dict[str, Any]] = {}
    for root in paths or PROTECTED_OUTPUT_ROOTS:
        if not root.exists():
            snapshot[str(root)] = {"exists": False}
            continue
        if root.is_file():
            stat = root.stat()
            snapshot[str(root)] = {
                "exists": True,
                "size_bytes": int(stat.st_size),
                "mtime_ns": int(stat.st_mtime_ns),
                "sha256": _hash_file(root),
            }
            continue
        files = [path for path in root.rglob("*") if path.is_file()]
        snapshot[str(root)] = {
            "exists": True,
            "file_count": int(len(files)),
            "total_size_bytes": int(sum(path.stat().st_size for path in files)),
            "latest_mtime_ns": int(max((path.stat().st_mtime_ns for path in files), default=0)),
        }
    return snapshot


def snapshot_diff(before: dict[str, Any], after: dict[str, Any]) -> dict[str, list[str]]:
    before_keys = set(before)
    after_keys = set(after)
    return {
        "added": sorted(after_keys - before_keys),
        "removed": sorted(before_keys - after_keys),
        "changed": sorted(key for key in before_keys & after_keys if before[key] != after[key]),
    }


def combine_binary_masks(masks: list[np.ndarray]) -> np.ndarray:
    if not masks:
        raise ValueError("At least one mask is required for a union.")
    shapes = {np.asarray(mask).shape for mask in masks}
    if len(shapes) != 1:
        raise ValueError(f"Cannot union masks with different shapes: {sorted(shapes)}")
    combined = np.zeros(next(iter(shapes)), dtype=np.float32)
    for mask in masks:
        combined = np.maximum(combined, (np.asarray(mask) > 0).astype(np.float32))
    return combined.astype(np.float32)


def assert_march12_union_area(individual_cells: list[int], combined_cells: int, tolerance_cells: int = 2) -> None:
    if not individual_cells:
        raise ValueError("No individual March 12 source masks were provided.")
    largest = max(int(value) for value in individual_cells)
    total = sum(int(value) for value in individual_cells)
    if int(combined_cells) + tolerance_cells < largest:
        raise AssertionError(
            f"Combined March 12 mask has {combined_cells} cells, smaller than largest individual source {largest}."
        )
    if int(combined_cells) > total + tolerance_cells:
        raise AssertionError(
            f"Combined March 12 mask has {combined_cells} cells, larger than source sum {total}."
        )


def assert_scorecard_contract(scorecard: pd.DataFrame, *, expected_rows: int = 12) -> None:
    missing = [column for column in SCORECARD_COLUMNS if column not in scorecard.columns]
    if missing:
        raise AssertionError(f"Scorecard is missing required columns: {missing}")
    if len(scorecard.index) != expected_rows:
        raise AssertionError(f"Expected {expected_rows} FSS rows, found {len(scorecard.index)}.")
    expected_pairs = {pair.pair_id for pair in FORECAST_PAIRS}
    expected_surfaces = {
        "opendrift_deterministic",
        "opendrift_mask_p50",
        "opendrift_mask_p90",
        "pygnome_deterministic_comparator",
    }
    actual_pairs = set(scorecard["pair_id"].astype(str))
    actual_surfaces = set(scorecard["model_surface"].astype(str))
    if actual_pairs != expected_pairs:
        raise AssertionError(f"Unexpected scorecard pair ids: {sorted(actual_pairs)}")
    if actual_surfaces != expected_surfaces:
        raise AssertionError(f"Unexpected scorecard model surfaces: {sorted(actual_surfaces)}")
    combo_counts = scorecard.groupby(["pair_id", "model_surface"]).size()
    missing_combos = [
        (pair_id, surface_id)
        for pair_id in sorted(expected_pairs)
        for surface_id in sorted(expected_surfaces)
        if int(combo_counts.get((pair_id, surface_id), 0)) != 1
    ]
    if missing_combos:
        raise AssertionError(f"Expected exactly one row for each pair/surface combination: {missing_combos}")


def _copy_file(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    if dst.exists():
        dst.unlink()
    shutil.copy2(src, dst)


def _clean_html_text(value: str) -> str:
    text = re.sub(r"<[^>]+>", " ", str(value or ""))
    text = text.replace("&nbsp;", " ")
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _extract_date_only_datetime(text: str) -> tuple[str, str]:
    cleaned = _clean_html_text(text)
    match = re.search(r"Acquired:\s*(\d{1,2})/(\d{1,2})/(\d{4})", cleaned, flags=re.IGNORECASE)
    if not match:
        return "", ""
    day, month, year = [int(part) for part in match.groups()]
    return f"{year:04d}-{month:02d}-{day:02d}T00:00:00Z", "acquisition date only; exact time unavailable"


def _feature_class_column(gdf) -> str | None:
    for column in gdf.columns:
        if str(column).lower().replace("_", "") == "oilspill":
            return str(column)
    return None


def _filter_positive_oil_polygons(gdf) -> tuple[Any, list[str]]:
    notes: list[str] = []
    if gdf is None or gdf.empty:
        return gdf, ["empty source layer"]
    column = _feature_class_column(gdf)
    if not column:
        notes.append("no OilSpill class field found; retained all polygon features")
        return gdf, notes
    values = gdf[column].astype(str).str.strip().str.lower()
    positive = values.isin(POSITIVE_OIL_CLASSES)
    suspected_source = values.str.contains("source", case=False, na=False)
    filtered = gdf.loc[positive & ~suspected_source].copy()
    notes.append(
        "positive classes retained: possible oil, possible thicker oil; suspected source classes excluded"
    )
    if filtered.empty:
        notes.append("positive-class filter produced zero polygons")
    return filtered, notes


class MindoroMar0912MultisourceExperimentService:
    def __init__(self, *, repo_root: str | Path | None = None, session: requests.Session | None = None):
        self.repo_root = Path(repo_root or Path(__file__).resolve().parents[2]).resolve()
        self.case = get_case_context()
        if self.case.run_name != RUN_NAME:
            raise RuntimeError(f"{EXPERIMENT_ID} requires WORKFLOW_MODE=mindoro_retro_2023.")
        self.output_dir = self.repo_root / EXPERIMENT_OUTPUT_DIR
        self.observation_dir = self.output_dir / "observations"
        self.raw_dir = self.observation_dir / "raw"
        self.vector_dir = self.observation_dir / "vectors"
        self.mask_dir = self.observation_dir / "masks"
        self.forecast_dir = self.output_dir / "opendrift"
        self.pygnome_dir = self.output_dir / "pygnome_comparator"
        self.figure_dir = self.output_dir / "figures"
        self.precheck_dir = self.output_dir / "precheck"
        self.forcing_dir = self.output_dir / "forcing"
        self.persistent_forcing_dir = persistent_local_input_dir(RUN_NAME, "mar09_11_12_multisource", "forcing")
        for path in (
            self.output_dir,
            self.observation_dir,
            self.raw_dir,
            self.vector_dir,
            self.mask_dir,
            self.forecast_dir,
            self.pygnome_dir,
            self.figure_dir,
            self.precheck_dir,
            self.forcing_dir,
            self.persistent_forcing_dir,
        ):
            path.mkdir(parents=True, exist_ok=True)
        self.session = session or requests.Session()
        self.session.headers.update({"User-Agent": f"{EXPERIMENT_ID}/1.0"})
        self.grid = GridBuilder()
        self.sea_mask = load_sea_mask_array(self.grid.spec)
        self.valid_mask = self.sea_mask > 0.5 if self.sea_mask is not None else None
        self.helper = Phase3BScoringService(output_dir=self.output_dir / "_scratch_scoring")
        self.force_refresh = os.environ.get("MINDORO_MAR09_12_FORCE_REFRESH", "").strip().lower() in {"1", "true", "yes"}
        self.force_rerun = os.environ.get("MINDORO_MAR09_12_FORCE_RERUN", "").strip().lower() in {"1", "true", "yes"}
        self.forcing_outage_policy = resolve_forcing_outage_policy(
            workflow_mode=self.case.workflow_mode,
            phase=EXPERIMENT_ID,
        )

    @property
    def cell_area_km2(self) -> float:
        if self.grid.is_projected:
            return float((self.grid.resolution * self.grid.resolution) / 1_000_000.0)
        return 0.0

    def run_pipeline(self) -> dict[str, Any]:
        run_started_utc = _now_utc()
        before = snapshot_protected_outputs()
        _write_json(self.output_dir / "protected_outputs_snapshot_before.json", before)
        dry_run_plan = self.run_dry_run()
        if dry_run_plan["status"] == "blocked_missing_forcing":
            after = snapshot_protected_outputs()
            diff = snapshot_diff(before, after)
            _write_json(self.output_dir / "protected_outputs_snapshot_after_missing_forcing.json", after)
            _write_json(self.output_dir / "protected_outputs_snapshot_diff_missing_forcing.json", diff)
            if diff["changed"] or diff["added"] or diff["removed"]:
                raise RuntimeError(f"Protected canonical outputs changed during dry-run gate: {diff}")
            raise RuntimeError(
                f"{EXPERIMENT_ID} dry-run gate blocked execution because required local forcing is missing. "
                f"See {self.output_dir / 'missing_forcing_manifest.json'}."
            )
        if not self._real_run_enabled():
            after = snapshot_protected_outputs()
            diff = snapshot_diff(before, after)
            _write_json(self.output_dir / "protected_outputs_snapshot_after_ready_gate.json", after)
            _write_json(self.output_dir / "protected_outputs_snapshot_diff_ready_gate.json", diff)
            if diff["changed"] or diff["added"] or diff["removed"]:
                raise RuntimeError(f"Protected canonical outputs changed during ready gate: {diff}")
            return {
                "output_dir": self.output_dir,
                "manifest_json": self.output_dir / "dry_run_plan.json",
                "scorecard_csv": "",
                "diagnostics_csv": "",
                "status": "ready_to_run",
                "model_run_executed": False,
                "run_gate_env": RUN_EXPERIMENT_ENV,
            }
        observation_records = self.prepare_observations()
        selection, recipe_source = self._resolve_recipe()
        forcing_paths = self._prepare_forcing(selection.recipe)
        self._write_run_config_resolved(selection, recipe_source, forcing_paths)
        forecast_records = self.run_opendrift_pairs(selection, recipe_source, forcing_paths)
        scorecard_path, diagnostics_path = self.finalize_scorecards(include_pygnome=False)
        manifest_path = self._write_manifest(
            stage="pipeline",
            observation_records=observation_records,
            forecast_records=forecast_records,
            selection=selection,
            recipe_source=recipe_source,
            forcing_paths=forcing_paths,
            scorecard_path=scorecard_path,
            diagnostics_path=diagnostics_path,
        )
        after = snapshot_protected_outputs()
        diff = snapshot_diff(before, after)
        _write_json(self.output_dir / "protected_outputs_snapshot_after_pipeline.json", after)
        _write_json(self.output_dir / "protected_outputs_snapshot_diff_pipeline.json", diff)
        if diff["changed"] or diff["added"] or diff["removed"]:
            raise RuntimeError(f"Protected canonical outputs changed during {EXPERIMENT_ID}: {diff}")
        self._write_real_run_manifest(
            run_started_utc=run_started_utc,
            run_status="opendrift_complete_pending_pygnome",
            selection=selection,
            recipe_source=recipe_source,
            forcing_paths=forcing_paths,
            opendrift_run_executed=True,
            pygnome_run_executed=False,
            scorecard_rows_written=self._scorecard_row_count(scorecard_path),
            canonical_outputs_overwritten=False,
            notes="OpenDrift archive-only stage completed. PyGNOME comparator/final scoring is a separate gated phase.",
        )
        return {
            "output_dir": self.output_dir,
            "manifest_json": manifest_path,
            "scorecard_csv": scorecard_path,
            "diagnostics_csv": diagnostics_path,
            "status": "pipeline_complete",
        }

    def run_pygnome_and_finalize(self) -> dict[str, Any]:
        if not self._real_run_enabled():
            raise RuntimeError(
                f"{EXPERIMENT_ID} PyGNOME finalization is gated. Set {RUN_EXPERIMENT_ENV}=1 only after "
                "dry-run reports ready_to_run and a real model run is explicitly approved."
            )
        run_started_utc = self._prior_real_run_started_utc()
        before = snapshot_protected_outputs()
        _write_json(self.output_dir / "protected_outputs_snapshot_before_pygnome.json", before)
        if not (self.output_dir / "source_ingestion_manifest.json").exists():
            self.prepare_observations()
        pygnome_manifest = self.run_pygnome_pairs()
        scorecard_path, diagnostics_path = self.finalize_scorecards(include_pygnome=True)
        self.write_figures()
        readme_path = self.write_readme()
        manifest_path = self._write_manifest(
            stage="final_with_pygnome",
            observation_records=self._load_source_manifest().get("observation_records", []),
            forecast_records=self._load_opendrift_manifest_records(),
            selection=None,
            recipe_source="",
            forcing_paths={},
            scorecard_path=scorecard_path,
            diagnostics_path=diagnostics_path,
            pygnome_manifest=pygnome_manifest,
            readme_path=readme_path,
        )
        after = snapshot_protected_outputs()
        diff = snapshot_diff(before, after)
        _write_json(self.output_dir / "protected_outputs_snapshot_after_pygnome.json", after)
        _write_json(self.output_dir / "protected_outputs_snapshot_diff_pygnome.json", diff)
        if diff["changed"] or diff["added"] or diff["removed"]:
            raise RuntimeError(f"Protected canonical outputs changed during PyGNOME finalization: {diff}")
        selection, recipe_source = self._resolve_recipe()
        self._write_real_run_manifest(
            run_started_utc=run_started_utc,
            run_status="completed",
            selection=selection,
            recipe_source=recipe_source,
            forcing_paths=self._resolved_forcing_paths_for_manifest(),
            opendrift_run_executed=bool(self._load_opendrift_manifest_records()),
            pygnome_run_executed=True,
            scorecard_rows_written=self._scorecard_row_count(scorecard_path),
            canonical_outputs_overwritten=False,
            notes=(
                "Archive/support-only real run completed using already staged March 9-12 case forcing. "
                "No downloads, Phase 1, drifter ingestion, recipe ranking, GFS preflight, or broad/monthly GFS ingestion "
                "were executed during the real run."
            ),
        )
        return {
            "output_dir": self.output_dir,
            "manifest_json": manifest_path,
            "scorecard_csv": scorecard_path,
            "diagnostics_csv": diagnostics_path,
            "readme_md": readme_path,
            "status": "final_complete",
        }

    def run_dry_run(self) -> dict[str, Any]:
        selection, recipe_source = self._resolve_recipe()
        forcing_plan = self._resolve_forcing_reuse_plan(selection.recipe)
        dry_run = {
            "experiment_id": EXPERIMENT_ID,
            "status": "blocked_missing_forcing" if forcing_plan["missing_forcing_files"] else "ready_to_run",
            "resolved_forcing_recipe_id": selection.recipe,
            "current_provider": forcing_plan["current_provider"],
            "wind_provider": forcing_plan["wind_provider"],
            "wave_provider": forcing_plan["wave_provider"],
            "recipe_source_file_or_manifest_path": selection.source_path,
            "recipe_source_details": recipe_source,
            "phase1_enabled": False,
            "drifter_ingestion_enabled": False,
            "gfs_historical_preflight_enabled": False,
            "broad_gfs_ingestion_enabled": False,
            "monthly_gfs_fetch_enabled": False,
            "recipe_ranking_enabled": False,
            "max_elements_per_run_or_member": ELEMENT_CAP,
            "planned_downloads": forcing_plan["planned_downloads"],
            "actual_downloads": forcing_plan.get("actual_downloads", []),
            "forcing_paths": {
                row["filename"]: row.get("stage_path", "")
                for row in forcing_plan.get("rows", [])
                if row.get("status") == "ready"
            },
            "forcing_coverage": {
                row["filename"]: {
                    "start_utc": (row.get("inspection") or {}).get("time_start_utc", ""),
                    "end_utc": (row.get("inspection") or {}).get("time_end_utc", ""),
                    "bbox": (row.get("inspection") or {}).get("source_bbox", {}),
                    "reader_compatibility_status": (row.get("inspection") or {}).get("reader_compatibility_status", ""),
                }
                for row in forcing_plan.get("rows", [])
            },
            "forcing_bbox": forcing_plan.get("required_bbox", self._required_forcing_bbox()),
            "planned_output_paths": self._planned_output_paths(),
            "planned_execution_terms": [
                "observation FeatureServer/report provenance ingestion",
                "local forcing reuse precheck",
                "OpenDrift deterministic control capped at 5000 elements",
                "OpenDrift 50-member ensemble capped at 5000 elements per member",
                "PyGNOME comparator-only capped at 5000 particles",
                "FSS scoring on existing Mindoro scoring grid",
            ],
            "missing_forcing_files": forcing_plan["missing_forcing_files"],
            "local_forcing_reused": forcing_plan["local_forcing_reused"],
            "allow_minimal_case_forcing_fetch": self._allow_minimal_case_forcing_fetch(),
            "minimal_case_fetch_used": bool(forcing_plan.get("minimal_case_fetch_used", False)),
            "expected_model_cases": [pair.pair_id for pair in FORECAST_PAIRS],
            "expected_forecast_pairs": len(FORECAST_PAIRS),
            "expected_model_surfaces": 4,
            "expected_scorecard_rows": len(FORECAST_PAIRS) * 4,
            "model_run_executed": False,
            "dry_run_status": "blocked_missing_forcing" if forcing_plan["missing_forcing_files"] else "ready_to_run",
            "run_gate_env": RUN_EXPERIMENT_ENV,
            "run_gate_enabled": self._real_run_enabled(),
        }
        self._assert_dry_run_plan_safe(dry_run)
        _write_json(self.output_dir / "dry_run_plan.json", dry_run)
        if forcing_plan["missing_forcing_files"]:
            self._write_missing_forcing_manifest(selection, recipe_source, forcing_plan)
            self.write_blocked_readme(dry_run)
        else:
            self._write_no_missing_forcing_manifest(selection, recipe_source, forcing_plan)
            scorecard_path = self.output_dir / "scorecard_fss_by_pair_surface.csv"
            diagnostics_path = self.output_dir / "scorecard_geometry_diagnostics.csv"
            if self._scorecard_row_count(scorecard_path) == len(FORECAST_PAIRS) * 4 and diagnostics_path.exists():
                self.write_readme()
            else:
                self.write_ready_dry_run_readme(dry_run)
        self._write_forcing_reuse_manifest(
            selection,
            recipe_source,
            forcing_plan,
            actual_downloads=forcing_plan.get("actual_downloads", []),
        )
        return dry_run

    def run_resolve_forcing_only(self) -> dict[str, Any]:
        selection, recipe_source = self._resolve_recipe()
        forcing_plan = self._resolve_forcing_reuse_plan(selection.recipe, stage_aliases=True)
        status = "local_forcing_resolved" if not forcing_plan["missing_forcing_files"] else "blocked_missing_forcing"
        if forcing_plan["missing_forcing_files"] and self._allow_minimal_case_forcing_fetch():
            forcing_plan = self._fetch_minimal_missing_forcing(selection.recipe, forcing_plan)
            status = (
                "minimal_case_forcing_fetched"
                if not forcing_plan["missing_forcing_files"]
                else "blocked_missing_forcing"
            )
        elif forcing_plan["missing_forcing_files"]:
            self._write_missing_forcing_manifest(selection, recipe_source, forcing_plan)

        self._write_forcing_resolution_outputs(selection, recipe_source, forcing_plan, status=status)
        self._write_forcing_reuse_manifest(
            selection,
            recipe_source,
            forcing_plan,
            actual_downloads=forcing_plan.get("actual_downloads", []),
        )
        if not forcing_plan.get("missing_forcing_files"):
            self._write_no_missing_forcing_manifest(selection, recipe_source, forcing_plan)
        return {
            "experiment_id": EXPERIMENT_ID,
            "status": status,
            "output_dir": self.output_dir,
            "forcing_resolution_manifest": self.output_dir / "forcing_resolution_manifest.json",
            "forcing_coverage_audit_csv": self.output_dir / "forcing_coverage_audit.csv",
            "forcing_alias_manifest": self.output_dir / "forcing_alias_manifest.json",
            "missing_forcing_manifest": self.output_dir / "missing_forcing_manifest.json",
            "local_forcing_reused": forcing_plan.get("local_forcing_reused", []),
            "missing_forcing_files": forcing_plan.get("missing_forcing_files", []),
            "minimal_case_fetch_used": bool(forcing_plan.get("minimal_case_fetch_used", False)),
            "planned_downloads": forcing_plan.get("planned_downloads", []),
            "actual_downloads": forcing_plan.get("actual_downloads", []),
            "model_run_executed": False,
        }

    def run_ingest_masks_only(self) -> dict[str, Any]:
        before = snapshot_protected_outputs()
        _write_json(self.output_dir / "protected_outputs_snapshot_before_mask_ingestion.json", before)
        observation_records = self.prepare_observations()
        self.write_figures(observation_only=True)
        after = snapshot_protected_outputs()
        diff = snapshot_diff(before, after)
        _write_json(self.output_dir / "protected_outputs_snapshot_after_mask_ingestion.json", after)
        _write_json(self.output_dir / "protected_outputs_snapshot_diff_mask_ingestion.json", diff)
        if diff["changed"] or diff["added"] or diff["removed"]:
            raise RuntimeError(f"Protected canonical outputs changed during mask ingestion: {diff}")
        return {
            "experiment_id": EXPERIMENT_ID,
            "status": "masks_ingested",
            "output_dir": self.output_dir,
            "observation_records": observation_records,
            "observation_mask_inventory_csv": self.output_dir / "observation_mask_inventory.csv",
            "source_ingestion_manifest_json": self.output_dir / "source_ingestion_manifest.json",
            "observation_figure": self.figure_dir / "fig_observation_masks_mar09_mar11_mar12_combined.png",
            "march12_union_qa_figure": self.figure_dir / "fig_source_mask_union_QA_mar12.png",
            "model_run_executed": False,
        }

    def _recipe_candidates(self) -> list[dict[str, str]]:
        candidates: list[dict[str, str]] = []
        baseline = _read_yaml(self.repo_root / PRIMARY_BASELINE_SELECTION_PATH)
        recipe = str(baseline.get("selected_recipe") or "").strip()
        if recipe:
            candidates.append(
                {
                    "recipe": recipe,
                    "source": str(PRIMARY_BASELINE_SELECTION_PATH),
                    "field": "selected_recipe",
                    "priority": "1",
                }
            )
        amendment = _read_yaml(self.repo_root / PRIMARY_VALIDATION_AMENDMENT_PATH)
        recipe = str(_nested_get(amendment, "phase1_recipe_provenance.active_selected_recipe") or "").strip()
        if recipe:
            candidates.append(
                {
                    "recipe": recipe,
                    "source": str(PRIMARY_VALIDATION_AMENDMENT_PATH),
                    "field": "phase1_recipe_provenance.active_selected_recipe",
                    "priority": "2",
                }
            )
        manifest_sources = [
            (
                B1_REINIT_OUTPUT_DIR / "march13_14_reinit_run_manifest.json",
                ["recipe.recipe", "recipe"],
                "3",
            ),
            (
                B1_REINIT_OUTPUT_DIR / "R1_previous" / "model_run" / "forecast" / "forecast_manifest.json",
                ["recipe_selection.recipe", "historical_baseline_provenance.recipe", "recipe"],
                "3",
            ),
            (
                B1_REINIT_OUTPUT_DIR / "march13_14_reinit_forcing_window_manifest.json",
                ["recipe"],
                "3",
            ),
        ]
        for rel_path, fields, priority in manifest_sources:
            payload = _read_json(self.repo_root / rel_path)
            if not payload:
                continue
            for field in fields:
                value = _nested_get(payload, field)
                if not isinstance(value, str):
                    value = str(value or "").strip()
                else:
                    value = value.strip()
                if not value:
                    continue
                candidates.append({"recipe": value, "source": str(rel_path), "field": field, "priority": priority})
                break
        return candidates

    def _resolve_recipe(self) -> tuple[RecipeSelection, str]:
        if os.environ.get(BASELINE_RECIPE_OVERRIDE_ENV, "").strip():
            raise RuntimeError(
                f"{EXPERIMENT_ID} does not allow {BASELINE_RECIPE_OVERRIDE_ENV}; "
                "use the adopted B1 recipe source of truth instead."
            )
        candidates = self._recipe_candidates()
        if not candidates:
            raise RuntimeError(
                f"{EXPERIMENT_ID} could not resolve the adopted B1 forcing recipe from configured source-of-truth files."
            )
        recipe_values = {candidate["recipe"] for candidate in candidates}
        if len(recipe_values) != 1:
            _write_json(
                self.output_dir / "recipe_conflict_manifest.json",
                {
                    "generated_at_utc": _now_utc(),
                    "experiment_id": EXPERIMENT_ID,
                    "status": "conflicting_recipe_sources",
                    "candidates": candidates,
                },
            )
            raise RuntimeError(f"Conflicting B1 recipe values found; refusing to run: {candidates}")

        primary = candidates[0]
        baseline = _read_yaml(self.repo_root / PRIMARY_BASELINE_SELECTION_PATH)
        selection = RecipeSelection(
            recipe=primary["recipe"],
            source_kind=str(baseline.get("source_kind") or "adopted_b1_source_of_truth"),
            source_path=primary["source"],
            status_flag=str(baseline.get("status_flag") or "valid"),
            valid=bool(baseline.get("valid", True)),
            provisional=bool(baseline.get("provisional", False)),
            rerun_required=bool(baseline.get("rerun_required", False)),
            note=str(baseline.get("selection_basis") or baseline.get("description") or ""),
        )
        if not selection.valid or selection.provisional or selection.rerun_required:
            raise RuntimeError(
                f"Resolved B1 recipe {selection.recipe} is not a final valid selection: {selection}"
            )
        recipe_source = f"adopted_b1_recipe_reused_no_phase1_rerun; candidates={candidates}"
        return selection, recipe_source

    @staticmethod
    def _provider_from_forcing_filename(filename: str) -> str:
        stem = Path(str(filename or "")).stem.lower()
        if stem.startswith("cmems"):
            return "CMEMS"
        if stem.startswith("gfs"):
            return "GFS"
        if stem.startswith("hycom"):
            return "HYCOM"
        if stem.startswith("era5"):
            return "ERA5"
        if stem.startswith("ncep"):
            return "NCEP"
        return stem or "unknown"

    def _recipe_forcing_files(self, recipe_name: str) -> dict[str, str]:
        recipes = _read_yaml(self.repo_root / "config" / "recipes.yaml")
        recipe = (recipes.get("recipes") or {}).get(recipe_name)
        if not recipe:
            raise RuntimeError(f"Recipe {recipe_name} is not defined in config/recipes.yaml.")
        return {
            "currents": str(recipe["currents_file"]),
            "wind": str(recipe["wind_file"]),
            "wave": str(recipe.get("wave_file") or ""),
        }

    def _forcing_search_roots(self) -> list[Path]:
        configured_roots = []
        try:
            files = self._recipe_forcing_files(self._resolve_recipe()[0].recipe)
            for filename in files.values():
                if filename:
                    configured_roots.append(Path("data") / "forcing" / RUN_NAME / filename)
        except Exception:
            configured_roots = []
        roots = [
            self.forcing_dir,
            self.persistent_forcing_dir,
            Path("data") / "forcing",
            Path("data") / "local_input_store",
            Path("data") / "historical_validation_inputs",
            Path("output") / RUN_NAME,
            Path("output") / RUN_NAME / "experiments",
            *[path.parent for path in configured_roots],
        ]
        unique: list[Path] = []
        seen: set[str] = set()
        for root in roots:
            resolved = (self.repo_root / root).resolve() if not root.is_absolute() else root.resolve()
            key = str(resolved)
            if key in seen:
                continue
            seen.add(key)
            unique.append(resolved)
        return unique

    def _all_local_nc_candidates(self) -> list[Path]:
        cached = getattr(self, "_all_local_forcing_candidates_cache", None)
        if cached is not None:
            return list(cached)
        candidates: list[Path] = []
        seen: set[str] = set()
        output_root = (self.repo_root / "output" / RUN_NAME).resolve()
        experiments_root = (self.repo_root / "output" / RUN_NAME / "experiments").resolve()
        for root in self._forcing_search_roots():
            if not root.exists():
                continue
            if root.is_file():
                paths = [root] if root.suffix.lower() == ".nc" else []
            elif root == output_root:
                paths = []
                for pattern in (
                    "*.nc",
                    "*/forcing/*.nc",
                    "*/*/forcing/*.nc",
                    "experiments/*/forcing/*.nc",
                    "experiments/*/*/forcing/*.nc",
                ):
                    paths.extend(root.glob(pattern))
            elif root == experiments_root:
                paths = []
                for pattern in ("*.nc", "*/forcing/*.nc", "*/*/forcing/*.nc"):
                    paths.extend(root.glob(pattern))
            else:
                paths = root.rglob("*.nc")
            for path in paths:
                try:
                    resolved = path.resolve()
                except OSError:
                    resolved = path
                key = str(resolved)
                if key in seen:
                    continue
                seen.add(key)
                candidates.append(resolved)
        self._all_local_forcing_candidates_cache = candidates
        return list(candidates)

    @staticmethod
    def _forcing_kind_tokens(forcing_kind: str, provider: str) -> list[str]:
        provider_token = str(provider or "").lower()
        if forcing_kind == "currents":
            return [provider_token, "curr", "current", "uo", "vo", "ocean"]
        if forcing_kind == "wind":
            return [provider_token, "wind", "u10", "v10"]
        if forcing_kind == "wave":
            return [provider_token, "wave", "stokes", "vsd", "vhm"]
        return [provider_token]

    @staticmethod
    def _looks_like_relevant_month_path(path: Path) -> bool:
        text = str(path).replace("\\", "/").lower()
        month_matches = re.findall(r"/(20\d{4})/", text)
        if month_matches and "202303" not in month_matches:
            return False
        case_dates = re.findall(r"case_(\d{4})-(\d{2})-(\d{2})", text)
        if case_dates and not any(year == "2023" and month == "03" for year, month, _ in case_dates):
            return False
        return True

    def _candidate_matches_requirement(
        self,
        path: Path,
        *,
        filename: str,
        forcing_kind: str,
        provider: str,
    ) -> bool:
        if not self._looks_like_relevant_month_path(path):
            return False
        text = str(path).replace("\\", "/").lower()
        name = path.name.lower()
        expected = str(filename or "").lower()
        if expected and name == expected:
            return True
        if "/forcing_cache/" in text:
            return False
        tokens = [token for token in self._forcing_kind_tokens(forcing_kind, provider) if token]
        provider_token = str(provider or "").lower()
        if provider_token and provider_token not in text:
            return False
        kind_tokens = [token for token in tokens if token != provider_token]
        return any(token in text for token in kind_tokens)

    def _candidate_priority(self, path: Path, *, filename: str) -> tuple[int, str]:
        text = str(path).replace("\\", "/").lower()
        score = 0
        if path.name.lower() == str(filename or "").lower():
            score += 1000
        if "mar09_11_12_multisource" in text:
            score += 700
        if "202303" in text or "2023-03" in text:
            score += 500
        if RUN_NAME.lower() in text:
            score += 300
        if "phase3b_extended_public_scored_march13_14_reinit" in text:
            score += 120
        if "/data/historical_validation_inputs/" in text:
            score += 80
        if "/forcing_cache/" in text:
            score -= 100
        return score, text

    def _candidate_local_forcing_paths(self, filename: str, forcing_kind: str, provider: str) -> list[Path]:
        if not filename:
            return []
        candidates: list[Path] = []
        seen: set[str] = set()
        for root in self._forcing_search_roots():
            direct = root / filename
            key = str(direct.resolve()) if direct.exists() else str(direct)
            if key not in seen:
                seen.add(key)
                candidates.append(direct)
        for path in self._all_local_nc_candidates():
            if not self._candidate_matches_requirement(
                path,
                filename=filename,
                forcing_kind=forcing_kind,
                provider=provider,
            ):
                continue
            key = str(path.resolve()) if path.exists() else str(path)
            if key in seen:
                continue
            seen.add(key)
            candidates.append(path)
        return sorted(candidates, key=lambda path: self._candidate_priority(path, filename=filename), reverse=True)

    @staticmethod
    def _detect_forcing_variables(ds: Any, forcing_kind: str) -> tuple[list[str], str]:
        try:
            if forcing_kind == "currents":
                variables = list(find_current_vars(ds))
            elif forcing_kind == "wind":
                variables = list(find_wind_vars(ds))
            elif forcing_kind == "wave":
                variables = list(find_wave_vars(ds))
            else:
                variables = []
            return variables, ""
        except Exception as exc:
            return [], f"{type(exc).__name__}: {exc}"

    @staticmethod
    def _coord_name(ds: Any, candidates: tuple[str, ...]) -> str:
        return next((name for name in candidates if name in ds.coords or name in ds.variables or name in ds.dims), "")

    @staticmethod
    def _coord_extent(values: Any) -> tuple[float | None, float | None]:
        arr = np.asarray(values, dtype=float)
        arr = arr[np.isfinite(arr)]
        if arr.size == 0:
            return None, None
        if np.nanmax(arr) > 180.0:
            arr = ((arr + 180.0) % 360.0) - 180.0
        return float(np.nanmin(arr)), float(np.nanmax(arr))

    def _required_forcing_bbox(self) -> dict[str, float]:
        if rasterio is None:
            return dict(FORCING_BBOX)
        mask_dir = getattr(self, "mask_dir", self.repo_root / EXPERIMENT_OUTPUT_DIR / "observations" / "masks")
        bounds: list[tuple[float, float, float, float]] = []
        try:
            from rasterio.warp import transform_bounds
            from rasterio.windows import Window, bounds as window_bounds
        except Exception:
            return dict(FORCING_BBOX)
        for mask_path in sorted(Path(mask_dir).glob("OBS_*.tif")):
            try:
                with rasterio.open(mask_path) as src:
                    arr = src.read(1)
                    rows, cols = np.where(arr > 0)
                    if rows.size == 0 or cols.size == 0:
                        continue
                    window = Window(
                        int(cols.min()),
                        int(rows.min()),
                        int(cols.max() - cols.min() + 1),
                        int(rows.max() - rows.min() + 1),
                    )
                    local_bounds = window_bounds(window, src.transform)
                    wgs84_bounds = transform_bounds(src.crs, "EPSG:4326", *local_bounds, densify_pts=21)
                    bounds.append(tuple(float(value) for value in wgs84_bounds))
            except Exception:
                continue
        if not bounds:
            return dict(FORCING_BBOX)
        halo = 0.5
        lon_min = max(FORCING_BBOX["lon_min"], min(item[0] for item in bounds) - halo)
        lat_min = max(FORCING_BBOX["lat_min"], min(item[1] for item in bounds) - halo)
        lon_max = min(FORCING_BBOX["lon_max"], max(item[2] for item in bounds) + halo)
        lat_max = min(FORCING_BBOX["lat_max"], max(item[3] for item in bounds) + halo)
        return {
            "lon_min": float(lon_min),
            "lon_max": float(lon_max),
            "lat_min": float(lat_min),
            "lat_max": float(lat_max),
        }

    def _minimal_fetch_bbox(self) -> dict[str, float]:
        required = self._required_forcing_bbox()
        margin = 0.5
        return {
            "lon_min": max(FORCING_BBOX["lon_min"], required["lon_min"] - margin),
            "lon_max": min(FORCING_BBOX["lon_max"], required["lon_max"] + margin),
            "lat_min": max(FORCING_BBOX["lat_min"], required["lat_min"] - margin),
            "lat_max": min(FORCING_BBOX["lat_max"], required["lat_max"] + margin),
        }

    def _subset_dataset_for_forcing(
        self,
        ds: Any,
        *,
        required_start: pd.Timestamp,
        required_end: pd.Timestamp,
    ) -> Any:
        required_bbox = self._required_forcing_bbox()
        subset = ds
        time_name = self._coord_name(ds, ("time", "valid_time"))
        if time_name:
            subset = subset.sel({time_name: slice(required_start.to_datetime64(), required_end.to_datetime64())})
        lat_name = self._coord_name(subset, ("latitude", "lat", "Latitude", "LATITUDE", "y"))
        lon_name = self._coord_name(subset, ("longitude", "lon", "Longitude", "LONGITUDE", "x"))
        if lat_name and lon_name and subset[lat_name].ndim == 1 and subset[lon_name].ndim == 1:
            lat_values = np.asarray(subset[lat_name].values, dtype=float)
            lon_values = np.asarray(subset[lon_name].values, dtype=float)
            lon_min = required_bbox["lon_min"]
            lon_max = required_bbox["lon_max"]
            if np.nanmax(lon_values) > 180.0:
                lon_min = lon_min % 360.0
                lon_max = lon_max % 360.0
            lon_slice = slice(lon_min, lon_max) if lon_values[0] <= lon_values[-1] else slice(lon_max, lon_min)
            lat_slice = (
                slice(required_bbox["lat_min"], required_bbox["lat_max"])
                if lat_values[0] <= lat_values[-1]
                else slice(required_bbox["lat_max"], required_bbox["lat_min"])
            )
            subset = subset.sel({lat_name: lat_slice, lon_name: lon_slice})
        return subset

    def _reader_smoke_test(self, path: Path) -> tuple[str, str, list[str]]:
        try:
            from opendrift.readers import reader_netCDF_CF_generic
        except Exception as exc:  # pragma: no cover - depends on optional runtime
            return "skipped_opendrift_unavailable", f"{type(exc).__name__}: {exc}", []
        try:
            reader = reader_netCDF_CF_generic.Reader(str(path))
            reader_vars = sorted(str(value) for value in (getattr(reader, "variables", []) or []))
            return "ready", "", reader_vars
        except Exception as exc:
            return "failed", f"{type(exc).__name__}: {exc}", []

    def _inspect_forcing_candidate(
        self,
        path: Path,
        *,
        forcing_kind: str,
        filename: str,
        provider: str,
        required_start: pd.Timestamp,
        required_end: pd.Timestamp,
    ) -> dict[str, Any]:
        required_bbox = self._required_forcing_bbox()
        row = {
            "path": str(path),
            "exists": path.exists(),
            "required_logical_name": filename,
            "forcing_kind": forcing_kind,
            "provider": provider,
            "time_start_utc": "",
            "time_end_utc": "",
            "required_start_utc": _iso_z(required_start),
            "required_end_utc": _iso_z(required_end),
            "required_bbox": required_bbox,
            "source_bbox": {},
            "variables_found": [],
            "missing_required_variables": "",
            "covers_required_window": False,
            "covers_required_bbox": False,
            "reader_compatibility_status": "",
            "reader_variables": [],
            "quality_status": "",
            "alias_action": "",
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
        reasons: list[str] = []
        try:
            with xr.open_dataset(path) as ds:
                variables, variable_error = self._detect_forcing_variables(ds, forcing_kind)
                row["variables_found"] = variables
                if variable_error:
                    row["missing_required_variables"] = variable_error
                    reasons.append(f"missing accepted {forcing_kind} variables")

                time_name = self._coord_name(ds, ("time", "valid_time"))
                if time_name:
                    times = normalize_time_index(ds[time_name].values)
                    if len(times):
                        row["time_start_utc"] = _iso_z(times.min())
                        row["time_end_utc"] = _iso_z(times.max())
                        row["covers_required_window"] = bool(times.min() <= required_start and times.max() >= required_end)
                if not row["covers_required_window"]:
                    reasons.append("time coverage does not cover required experiment window")

                lat_name = self._coord_name(ds, ("latitude", "lat", "Latitude", "LATITUDE", "y"))
                lon_name = self._coord_name(ds, ("longitude", "lon", "Longitude", "LONGITUDE", "x"))
                if lat_name and lon_name:
                    lon_min, lon_max = self._coord_extent(ds[lon_name].values)
                    lat_min, lat_max = self._coord_extent(ds[lat_name].values)
                    row["source_bbox"] = {
                        "lon_min": lon_min,
                        "lon_max": lon_max,
                        "lat_min": lat_min,
                        "lat_max": lat_max,
                    }
                    row["covers_required_bbox"] = bool(
                        lon_min is not None
                        and lon_max is not None
                        and lat_min is not None
                        and lat_max is not None
                        and lon_min <= required_bbox["lon_min"]
                        and lon_max >= required_bbox["lon_max"]
                        and lat_min <= required_bbox["lat_min"]
                        and lat_max >= required_bbox["lat_max"]
                    )
                if not row["covers_required_bbox"]:
                    reasons.append("spatial extent does not cover required experiment bbox")

                if variables and row["covers_required_window"] and row["covers_required_bbox"]:
                    subset = self._subset_dataset_for_forcing(ds, required_start=required_start, required_end=required_end)
                    quality_reasons = []
                    for variable in variables:
                        data = np.asarray(subset[variable].values)
                        finite = np.isfinite(data)
                        if not finite.any():
                            quality_reasons.append(f"{variable} is all-NaN over required window/domain")
                            continue
                        if float(np.nanmax(np.abs(data))) == 0.0:
                            quality_reasons.append(f"{variable} is all-zero over required window/domain")
                    if quality_reasons:
                        row["quality_status"] = "failed"
                        reasons.extend(quality_reasons)
                    else:
                        row["quality_status"] = "ready"

            reader_status, reader_error, reader_vars = self._reader_smoke_test(path)
            row["reader_compatibility_status"] = reader_status
            row["reader_variables"] = reader_vars
            if reader_status == "failed":
                reasons.append(f"OpenDrift reader smoke test failed: {reader_error}")
            elif reader_status == "skipped_opendrift_unavailable":
                row["reader_compatibility_note"] = reader_error

            ready_reader = row["reader_compatibility_status"] in {"ready", "skipped_opendrift_unavailable"}
            row["status"] = (
                "ready"
                if row["covers_required_window"]
                and row["covers_required_bbox"]
                and bool(row["variables_found"])
                and row["quality_status"] == "ready"
                and ready_reader
                else "insufficient"
            )
            row["stop_reason"] = "; ".join(reasons)
            return row
        except Exception as exc:
            row["status"] = "failed"
            row["stop_reason"] = f"{type(exc).__name__}: {exc}"
            return row

    def _stage_or_subset_forcing_alias(
        self,
        source_path: Path,
        target_path: Path,
        *,
        required_start: pd.Timestamp,
        required_end: pd.Timestamp,
    ) -> tuple[Path, str]:
        target_path.parent.mkdir(parents=True, exist_ok=True)
        if source_path.exists() and source_path.resolve() == target_path.resolve():
            return target_path, "already_at_experiment_alias_path"
        if xr is not None:
            try:
                with xr.open_dataset(source_path) as ds:
                    subset = self._subset_dataset_for_forcing(ds, required_start=required_start, required_end=required_end)
                    loaded = subset.load()
                temp_path = target_path.with_suffix(target_path.suffix + ".tmp")
                loaded.to_netcdf(temp_path)
                if target_path.exists():
                    target_path.unlink()
                temp_path.replace(target_path)
                return target_path, "subset_to_experiment_window_bbox"
            except Exception:
                pass
        stage_store_file(source_path, target_path)
        return target_path, "copied_without_subset"

    def _resolve_one_local_forcing(
        self,
        *,
        forcing_kind: str,
        filename: str,
        required_vars: list[str],
        required_start: pd.Timestamp,
        required_end: pd.Timestamp,
        stage_alias: bool = False,
    ) -> dict[str, Any]:
        inspections = []
        provider = self._provider_from_forcing_filename(filename)
        for path in self._candidate_local_forcing_paths(filename, forcing_kind, provider):
            inspection = self._inspect_forcing_candidate(
                path,
                forcing_kind=forcing_kind,
                filename=filename,
                provider=provider,
                required_start=required_start,
                required_end=required_end,
            )
            inspections.append({"candidate_path": str(path), **inspection})
            if inspection["status"] == "ready":
                stage_path = self.forcing_dir / filename
                alias_action = ""
                if stage_alias:
                    stage_path, alias_action = self._stage_or_subset_forcing_alias(
                        path,
                        stage_path,
                        required_start=required_start,
                        required_end=required_end,
                    )
                    inspection = dict(inspection)
                    inspection["alias_action"] = alias_action
                return {
                    "forcing_kind": forcing_kind,
                    "filename": filename,
                    "status": "ready",
                    "source_path": str(path),
                    "stage_path": str(stage_path),
                    "provider": provider,
                    "variable_group": forcing_kind,
                    "alias_action": alias_action,
                    "inspection": inspection,
                    "candidate_inspections": inspections,
                }
        return {
            "forcing_kind": forcing_kind,
            "filename": filename,
            "status": "missing",
            "source_path": "",
            "stage_path": str(self.forcing_dir / filename),
            "provider": provider,
            "variable_group": forcing_kind,
            "alias_action": "",
            "inspection": {},
            "candidate_inspections": inspections,
            "local_stores_searched": [str(path) for path in self._forcing_search_roots()],
            "required_bbox": self._required_forcing_bbox(),
            "required_start_utc": _iso_z(required_start),
            "required_end_utc": _iso_z(required_end),
            "next_required_action": (
                f"Provide validated local {provider} {forcing_kind} forcing for the bounded March 9-12 window, "
                f"or set {ALLOW_MINIMAL_CASE_FORCING_FETCH_ENV}=1 for an explicitly approved minimal case fetch."
            ),
        }

    def _resolve_forcing_reuse_plan(self, recipe_name: str, *, stage_aliases: bool = False) -> dict[str, Any]:
        files = self._recipe_forcing_files(recipe_name)
        required_start, required_end = self._required_forcing_time_bounds()
        required_bbox = self._required_forcing_bbox()
        rows = [
            self._resolve_one_local_forcing(
                forcing_kind="currents",
                filename=files["currents"],
                required_vars=["uo", "vo"],
                required_start=required_start,
                required_end=required_end,
                stage_alias=stage_aliases,
            ),
            self._resolve_one_local_forcing(
                forcing_kind="wind",
                filename=files["wind"],
                required_vars=["x_wind", "y_wind"],
                required_start=required_start,
                required_end=required_end,
                stage_alias=stage_aliases,
            ),
        ]
        if files["wave"]:
            rows.append(
                self._resolve_one_local_forcing(
                    forcing_kind="wave",
                    filename=files["wave"],
                    required_vars=["VHM0", "VSDX", "VSDY"],
                    required_start=required_start,
                    required_end=required_end,
                    stage_alias=stage_aliases,
                )
            )
        missing = [row for row in rows if row["status"] != "ready"]
        planned_downloads = []
        if missing and self._allow_minimal_case_forcing_fetch():
            planned_downloads = [
                {
                    "forcing_kind": row["forcing_kind"],
                    "filename": row["filename"],
                    "provider": row["provider"],
                    "mode": "minimal_case_forcing_fetch_only_2023-03-08T18_to_2023-03-13T00",
                    "bounded_window_start_utc": FORCING_WINDOW_START_UTC,
                    "bounded_window_end_utc": FORCING_WINDOW_END_UTC,
                    "bounded_bbox": dict(FORCING_BBOX),
                }
                for row in missing
            ]
        return {
            "experiment_id": EXPERIMENT_ID,
            "forcing_recipe_id": recipe_name,
            "required_start_utc": _iso_z(required_start),
            "required_end_utc": _iso_z(required_end),
            "rows": rows,
            "current_provider": rows[0]["provider"],
            "wind_provider": rows[1]["provider"],
            "wave_provider": next((row["provider"] for row in rows if row["forcing_kind"] == "wave"), ""),
            "local_forcing_reused": [row for row in rows if row["status"] == "ready"],
            "missing_forcing_files": missing,
            "planned_downloads": planned_downloads,
            "actual_downloads": [],
            "phase1_rerun": False,
            "drifter_ingestion": False,
            "gfs_historical_preflight": False,
            "minimal_case_fetch_used": False,
            "search_roots": [str(path) for path in self._forcing_search_roots()],
            "required_bbox": required_bbox,
        }

    def _allow_minimal_case_forcing_fetch(self) -> bool:
        return os.environ.get(ALLOW_MINIMAL_CASE_FORCING_FETCH_ENV, "").strip().lower() in {"1", "true", "yes"}

    def _real_run_enabled(self) -> bool:
        return os.environ.get(RUN_EXPERIMENT_ENV, "").strip().lower() in {"1", "true", "yes"}

    def _assert_dry_run_plan_safe(self, plan: dict[str, Any]) -> None:
        if plan.get("phase1_enabled") is not False:
            raise RuntimeError("Dry-run plan is unsafe: phase1_enabled is not false.")
        if plan.get("drifter_ingestion_enabled") is not False:
            raise RuntimeError("Dry-run plan is unsafe: drifter_ingestion_enabled is not false.")
        if plan.get("gfs_historical_preflight_enabled") is not False:
            raise RuntimeError("Dry-run plan is unsafe: gfs_historical_preflight_enabled is not false.")
        if int(plan.get("max_elements_per_run_or_member") or 0) > ELEMENT_CAP:
            raise RuntimeError("Dry-run plan is unsafe: element count exceeds 5000.")
        searchable = " ".join(str(term).lower() for term in plan.get("planned_execution_terms", []))
        forbidden_hits = [term for term in BANNED_DRY_RUN_TERMS if term.lower() in searchable]
        if forbidden_hits:
            raise RuntimeError(f"Dry-run plan contains prohibited execution terms: {forbidden_hits}")

    def _planned_output_paths(self) -> list[str]:
        return [
            str(self.output_dir),
            str(self.output_dir / "observation_mask_inventory.csv"),
            str(self.output_dir / "scorecard_fss_by_pair_surface.csv"),
            str(self.output_dir / "scorecard_geometry_diagnostics.csv"),
            str(self.output_dir / "manifest.json"),
            str(self.output_dir / "run_config_resolved.yaml"),
            str(self.output_dir / "source_ingestion_manifest.json"),
            str(self.output_dir / "forcing_reuse_manifest.json"),
            str(self.output_dir / "element_count_audit.json"),
            str(self.output_dir / "pygnome_comparator_manifest.json"),
            str(self.output_dir / "README.md"),
            str(self.figure_dir),
        ]

    def _existing_extended_registry(self) -> pd.DataFrame:
        path = self.repo_root / "output" / RUN_NAME / "phase3b_extended_public" / "extended_public_obs_acceptance_registry.csv"
        if not path.exists():
            return pd.DataFrame()
        return pd.read_csv(path)

    def _registry_row_for_source(self, spec: ObservationSourceSpec) -> pd.Series | None:
        registry = self._existing_extended_registry()
        if registry.empty:
            return None
        matches = registry[
            registry["source_name"].astype(str).eq(spec.source_name)
            & registry.get("accepted_for_extended_quantitative", pd.Series(False, index=registry.index)).astype(str).str.lower().isin(["true", "1"])
        ].copy()
        if matches.empty:
            return None
        row = matches.iloc[0]
        mask_path = self.repo_root / str(row.get("extended_obs_mask") or "")
        vector_path = self.repo_root / str(row.get("extended_processed_vector") or row.get("processed_vector") or "")
        if mask_path.exists() and vector_path.exists() and not self.force_refresh:
            return row
        return None

    def _copy_existing_source(self, spec: ObservationSourceSpec, row: pd.Series) -> dict[str, Any]:
        mask_path = self.mask_dir / f"{spec.mask_id}.tif"
        vector_path = self.vector_dir / f"{spec.mask_id}.gpkg"
        raw_geojson_path = self.raw_dir / spec.mask_id / f"{spec.mask_id}_raw.geojson"
        layer_metadata_path = self.raw_dir / spec.mask_id / f"{spec.mask_id}_layer_metadata.json"
        item_metadata_path = self.raw_dir / spec.mask_id / f"{spec.mask_id}_item_metadata.json"

        source_mask = self.repo_root / str(row.get("extended_obs_mask") or "")
        source_vector = self.repo_root / str(row.get("extended_processed_vector") or row.get("processed_vector") or "")
        _copy_file(source_mask, mask_path)
        _copy_file(source_vector, vector_path)
        for source_key, target in (
            ("extended_raw_geojson", raw_geojson_path),
            ("extended_layer_metadata", layer_metadata_path),
            ("extended_item_metadata", item_metadata_path),
        ):
            src_text = str(row.get(source_key) or "").strip()
            if src_text:
                src = self.repo_root / src_text
                if src.exists():
                    _copy_file(src, target)

        mask = self.helper._load_binary_score_mask(mask_path)
        cells = int(np.count_nonzero(mask > 0))
        notes = [str(spec.notes or "").strip(), str(row.get("processing_notes") or "").strip(), "reused stored extended-public processed vector and mask"]
        acquired, acquired_note = self._resolve_acquisition_datetime(spec, row=row)
        if acquired_note:
            notes.append(acquired_note)
        report_path = self._download_report_png(spec)
        if report_path:
            notes.append(f"report_png={report_path}")
        return {
            "mask_id": spec.mask_id,
            "date_label": spec.date_label,
            "satellite": spec.satellite,
            "source_name": spec.source_name,
            "source_type": spec.source_type,
            "source_url_or_service": str(row.get("source_url") or spec.service_url),
            "acquisition_datetime_utc": acquired,
            "report_datetime_utc": "",
            "crs_original": str(row.get("raw_crs") or ""),
            "crs_scoring": self.grid.crs,
            "area_km2": cells * self.cell_area_km2,
            "positive_cells": cells,
            "ingestion_status": "reused_existing_extended_public_artifact",
            "notes": " | ".join(note for note in notes if note),
            "mask_path": str(mask_path),
            "processed_vector_path": str(vector_path),
            "raw_geojson": str(raw_geojson_path) if raw_geojson_path.exists() else "",
            "layer_metadata": str(layer_metadata_path) if layer_metadata_path.exists() else "",
            "item_metadata": str(item_metadata_path) if item_metadata_path.exists() else "",
            "report_png": str(report_path) if report_path else "",
        }

    def _resolve_layer_endpoint(self, service_url: str) -> tuple[str, int, dict[str, Any]]:
        service_url = service_url.rstrip("/")
        tail = service_url.rsplit("/", 1)[-1]
        if tail.isdigit():
            response = self.session.get(service_url, params={"f": "json"}, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            return service_url.rsplit("/", 1)[0], int(tail), response.json() or {}
        response = self.session.get(service_url, params={"f": "json"}, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        root_metadata = response.json() or {}
        layers = root_metadata.get("layers") or []
        if layers and layers[0].get("id") is not None:
            layer_id = int(layers[0]["id"])
            layer_url = f"{service_url}/{layer_id}"
            layer_response = self.session.get(layer_url, params={"f": "json"}, timeout=REQUEST_TIMEOUT)
            layer_response.raise_for_status()
            return service_url, layer_id, layer_response.json() or {}
        if root_metadata.get("id") is not None and root_metadata.get("geometryType"):
            return service_url.rsplit("/", 1)[0], int(root_metadata["id"]), root_metadata
        raise RuntimeError(f"Could not resolve a FeatureServer layer for {service_url}")

    def _download_report_png(self, spec: ObservationSourceSpec) -> Path | None:
        if not spec.report_url:
            return None
        out_path = self.raw_dir / spec.mask_id / f"{spec.mask_id}_report.png"
        if out_path.exists() and not self.force_refresh:
            return out_path
        try:
            response = self.session.get(spec.report_url, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_bytes(response.content)
            return out_path
        except Exception as exc:
            warning_path = self.raw_dir / spec.mask_id / f"{spec.mask_id}_report_download_warning.txt"
            _write_text(warning_path, f"Could not download report PNG: {type(exc).__name__}: {exc}\n")
            return None

    def _resolve_acquisition_datetime(self, spec: ObservationSourceSpec, *, row: pd.Series | None = None, item_metadata: dict | None = None) -> tuple[str, str]:
        texts = [spec.notes]
        if row is not None:
            texts.append(str(row.get("notes") or ""))
        if item_metadata:
            texts.extend([str(item_metadata.get("description") or ""), str(item_metadata.get("snippet") or "")])
        for text in texts:
            acquired, note = _extract_date_only_datetime(text)
            if acquired:
                return acquired, note
        return spec.acquisition_hint_utc, "acquisition date hint is date-only; exact time unavailable" if spec.acquisition_hint_utc else ""

    def _materialize_remote_source(self, spec: ObservationSourceSpec) -> dict[str, Any]:
        if gpd is None:
            raise ImportError("geopandas is required for observation-mask ingestion")
        raw_dir = self.raw_dir / spec.mask_id
        raw_dir.mkdir(parents=True, exist_ok=True)
        raw_geojson_path = raw_dir / f"{spec.mask_id}_raw.geojson"
        layer_metadata_path = raw_dir / f"{spec.mask_id}_layer_metadata.json"
        processed_vector_path = self.vector_dir / f"{spec.mask_id}.gpkg"
        mask_path = self.mask_dir / f"{spec.mask_id}.tif"

        service_root, layer_id, layer_metadata = self._resolve_layer_endpoint(spec.service_url)
        query_url = f"{service_root.rstrip('/')}/{layer_id}/query"
        response = self.session.get(
            query_url,
            params={"where": "1=1", "outFields": "*", "returnGeometry": "true", "outSR": 4326, "f": "geojson"},
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        raw_geojson = response.json() or {}
        raw_features = raw_geojson.get("features") or []
        if not raw_features:
            raise RuntimeError(f"{spec.source_name} returned no GeoJSON features from {query_url}")

        _write_json(raw_geojson_path, raw_geojson)
        _write_json(layer_metadata_path, layer_metadata)

        raw_gdf = gpd.GeoDataFrame.from_features(raw_features).set_crs("EPSG:4326", allow_override=True)
        inferred_crs, inferred_notes = _infer_source_crs(raw_gdf, layer_metadata, raw_geojson)
        raw_gdf = raw_gdf.set_crs(inferred_crs, allow_override=True)
        raw_gdf, repair_notes = _repair_degree_scaled_geometries(
            raw_gdf,
            metadata=layer_metadata,
            payload=raw_geojson,
            expected_region=list(self.case.region),
        )
        filtered_gdf, class_notes = _filter_positive_oil_polygons(raw_gdf)
        cleaned_gdf, qa = clean_arcgis_geometries(
            raw_gdf=filtered_gdf,
            expected_geometry_type="polygon",
            source_crs=inferred_crs,
            target_crs=self.grid.crs,
        )
        if cleaned_gdf.empty:
            raise RuntimeError(f"{spec.source_name} produced no valid positive oil polygons after cleaning.")
        if processed_vector_path.exists():
            processed_vector_path.unlink()
        _sanitize_vector_columns_for_gpkg(cleaned_gdf).to_file(processed_vector_path, driver="GPKG")
        mask = rasterize_observation_layer(cleaned_gdf, self.grid)
        mask = apply_ocean_mask(mask, sea_mask=self.sea_mask, fill_value=0.0)
        save_raster(self.grid, mask.astype(np.float32), mask_path)
        cells = int(np.count_nonzero(mask > 0))
        if cells <= 0:
            raise RuntimeError(f"{spec.mask_id} rasterized to zero positive ocean cells.")

        report_path = self._download_report_png(spec)
        notes = inferred_notes + repair_notes + class_notes
        notes.append("; ".join(f"{key}={value}" for key, value in qa.items()))
        notes.append(str(spec.notes or ""))
        if report_path:
            notes.append(f"report_png={report_path}")
        acquired, acquired_note = self._resolve_acquisition_datetime(spec)
        if acquired_note:
            notes.append(acquired_note)
        return {
            "mask_id": spec.mask_id,
            "date_label": spec.date_label,
            "satellite": spec.satellite,
            "source_name": spec.source_name,
            "source_type": spec.source_type,
            "source_url_or_service": spec.service_url,
            "acquisition_datetime_utc": acquired,
            "report_datetime_utc": "",
            "crs_original": inferred_crs,
            "crs_scoring": self.grid.crs,
            "area_km2": cells * self.cell_area_km2,
            "positive_cells": cells,
            "ingestion_status": "processed_remote_feature_service",
            "notes": " | ".join(note for note in notes if note),
            "mask_path": str(mask_path),
            "processed_vector_path": str(processed_vector_path),
            "raw_geojson": str(raw_geojson_path),
            "layer_metadata": str(layer_metadata_path),
            "item_metadata": "",
            "report_png": str(report_path) if report_path else "",
        }

    def materialize_source(self, spec: ObservationSourceSpec) -> dict[str, Any]:
        row = self._registry_row_for_source(spec)
        if row is not None:
            return self._copy_existing_source(spec, row)
        return self._materialize_remote_source(spec)

    def prepare_observations(self) -> list[dict[str, Any]]:
        records = [self.materialize_source(spec) for spec in OBS_SOURCES]
        records.append(self._build_march12_combined_mask(records))
        inventory = pd.DataFrame(records)
        _write_csv(self.output_dir / "observation_mask_inventory.csv", inventory, INVENTORY_COLUMNS)
        payload = {
            "generated_at_utc": _now_utc(),
            "experiment_id": EXPERIMENT_ID,
            "observation_records": records,
            "combine_rule": "union_dissolve_after_reprojection",
            "positive_oil_classes": sorted(POSITIVE_OIL_CLASSES),
            "suspected_source_points_included": False,
            "scoring_grid": self.grid.spec.to_metadata(),
        }
        _write_json(self.output_dir / "source_ingestion_manifest.json", payload)
        self._write_observation_figure(records)
        return records

    def _build_march12_combined_mask(self, records: list[dict[str, Any]]) -> dict[str, Any]:
        if gpd is None:
            raise ImportError("geopandas is required for March 12 source-mask union")
        source_records = [record for record in records if record["mask_id"] in MARCH12_SOURCE_MASK_IDS]
        if len(source_records) != len(MARCH12_SOURCE_MASK_IDS):
            found = sorted(record["mask_id"] for record in source_records)
            raise RuntimeError(f"March 12 union needs all source masks. Found {found}.")
        gdfs = []
        individual_cells = []
        acquisition_times = []
        for record in source_records:
            vector_path = Path(record["processed_vector_path"])
            gdf = gpd.read_file(vector_path)
            if gdf.crs is None:
                gdf = gdf.set_crs(self.grid.crs)
            elif str(gdf.crs) != str(self.grid.crs):
                gdf = gdf.to_crs(self.grid.crs)
            gdfs.append(gdf.dropna(subset=["geometry"]))
            individual_cells.append(int(record["positive_cells"]))
            acquired = str(record.get("acquisition_datetime_utc") or "").strip()
            if acquired:
                acquisition_times.append(acquired)
        combined_gdf = pd.concat(gdfs, ignore_index=True)
        union_geom = combined_gdf.geometry.union_all() if hasattr(combined_gdf.geometry, "union_all") else combined_gdf.geometry.unary_union
        dissolved = gpd.GeoDataFrame(
            [{"mask_id": "OBS_MAR12_COMBINED", "combine_rule": "union_dissolve_after_reprojection"}],
            geometry=[union_geom],
            crs=self.grid.crs,
        )
        vector_path = self.vector_dir / "OBS_MAR12_COMBINED.gpkg"
        if vector_path.exists():
            vector_path.unlink()
        dissolved.to_file(vector_path, driver="GPKG")
        mask = rasterize_observation_layer(dissolved, self.grid)
        mask = apply_ocean_mask(mask, sea_mask=self.sea_mask, fill_value=0.0)
        mask_path = self.mask_dir / "OBS_MAR12_COMBINED.tif"
        save_raster(self.grid, mask.astype(np.float32), mask_path)
        combined_cells = int(np.count_nonzero(mask > 0))
        if combined_cells <= 0:
            raise RuntimeError("OBS_MAR12_COMBINED has zero positive cells.")
        assert_march12_union_area(individual_cells, combined_cells)
        unique_acquisition_times = sorted(set(acquisition_times))
        combined_acquisition = unique_acquisition_times[0] if len(unique_acquisition_times) == 1 else ""
        return {
            "mask_id": "OBS_MAR12_COMBINED",
            "date_label": "2023-03-12",
            "satellite": "combined",
            "source_name": ";".join(record["source_name"] for record in source_records),
            "source_type": "union_dissolve_after_reprojection",
            "source_url_or_service": ";".join(record["source_url_or_service"] for record in source_records),
            "acquisition_datetime_utc": combined_acquisition,
            "report_datetime_utc": "",
            "crs_original": self.grid.crs,
            "crs_scoring": self.grid.crs,
            "area_km2": combined_cells * self.cell_area_km2,
            "positive_cells": combined_cells,
            "ingestion_status": "combined_union_dissolved",
            "notes": (
                f"combined from {','.join(MARCH12_SOURCE_MASK_IDS)}; "
                f"individual_cells={individual_cells}; combined_cells={combined_cells}; "
                f"source_acquisition_datetimes={unique_acquisition_times}"
            ),
            "mask_path": str(mask_path),
            "processed_vector_path": str(vector_path),
            "source_mask_ids": ";".join(MARCH12_SOURCE_MASK_IDS),
        }

    def _observation_record_map(self) -> dict[str, dict[str, Any]]:
        manifest = self._load_source_manifest()
        records = manifest.get("observation_records") or []
        return {str(record["mask_id"]): record for record in records}

    @staticmethod
    def _source_spec_map() -> dict[str, ObservationSourceSpec]:
        return {spec.mask_id: spec for spec in OBS_SOURCES}

    @staticmethod
    def _parse_optional_utc(value: Any) -> pd.Timestamp | None:
        text = str(value or "").strip()
        if not text or ";" in text:
            return None
        try:
            return _normalize_utc(text)
        except Exception:
            return None

    def _observation_timestamp(self, mask_id: str, record: dict[str, Any]) -> pd.Timestamp | None:
        timestamp = self._parse_optional_utc(record.get("acquisition_datetime_utc"))
        if timestamp is not None:
            return timestamp
        spec = self._source_spec_map().get(mask_id)
        if spec is not None:
            return self._parse_optional_utc(spec.nominal_datetime_utc)
        return None

    def _pair_timing_values(self, pair: ForecastPairSpec) -> tuple[str, str]:
        observation_map = self._observation_record_map()
        seed_record = observation_map.get(pair.seed_mask_id, {})
        target_record = observation_map.get(pair.target_mask_id, {})
        seed_time = self._observation_timestamp(pair.seed_mask_id, seed_record)
        target_time = self._observation_timestamp(pair.target_mask_id, target_record)
        nominal_target = self._parse_optional_utc(pair.target_utc)
        actual_elapsed_h = ""
        obs_time_offset_h = ""
        if seed_time is not None and target_time is not None:
            actual_elapsed_h = f"{((target_time - seed_time).total_seconds() / 3600.0):.3f}"
        if target_time is not None and nominal_target is not None:
            obs_time_offset_h = f"{((target_time - nominal_target).total_seconds() / 3600.0):.3f}"
        return actual_elapsed_h, obs_time_offset_h

    def _load_source_manifest(self) -> dict[str, Any]:
        path = self.output_dir / "source_ingestion_manifest.json"
        if not path.exists():
            return {}
        return json.loads(path.read_text(encoding="utf-8")) or {}

    @staticmethod
    def _required_forcing_time_bounds() -> tuple[pd.Timestamp, pd.Timestamp]:
        return _normalize_utc(FORCING_WINDOW_START_UTC), _normalize_utc(FORCING_WINDOW_END_UTC)

    def _candidate_gfs_cache_paths(self, gfs_path: Path) -> list[Path]:
        candidates = [
            gfs_path,
            Path("data") / "forcing" / self.case.run_name / gfs_path.name,
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
        if self.force_refresh:
            return None
        required_start, required_end = self._required_forcing_time_bounds()
        for candidate in self._candidate_gfs_cache_paths(gfs_path):
            inspection = self._forcing_time_and_vars(candidate, ["x_wind", "y_wind"], required_start, required_end)
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

    def _download_required_gfs_wind(self, service: DataIngestionService, *, gfs_path: Path) -> dict[str, Any]:
        cached = self._gfs_cache_ready_record(gfs_path)
        if cached is not None:
            return cached

        required_start, required_end = self._required_forcing_time_bounds()
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
                    scratch_dir=self.persistent_forcing_dir,
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
                        scratch_dir=self.persistent_forcing_dir,
                        budget_seconds=budget_seconds,
                    )
                    or {}
                )
                record.setdefault("status", "downloaded")
                record["source_system"] = "ucar_gdex_d084001"
                record["source_tier"] = "secondary"
                record["provider"] = "UCAR GDEx"
                record["source_url"] = "https://gdex.ucar.edu/datasets/d084001/"
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
        record["storage_tier"] = PERSISTENT_LOCAL_INPUT_STORE
        record["local_storage_path"] = str(gfs_path)
        record["reuse_action"] = str(record.get("reuse_action") or "downloaded_new_file")
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

    def _prepare_forcing(self, recipe_name: str) -> dict[str, Any]:
        selection, recipe_source = self._resolve_recipe()
        forcing_plan = self._resolve_forcing_reuse_plan(recipe_name)
        if forcing_plan["missing_forcing_files"] and not self._allow_minimal_case_forcing_fetch():
            self._write_missing_forcing_manifest(selection, recipe_source, forcing_plan)
            self._write_forcing_reuse_manifest(selection, recipe_source, forcing_plan, actual_downloads=[])
            raise RuntimeError(
                f"{EXPERIMENT_ID} is blocked by missing March 9-12 local forcing. "
                "missing_forcing_manifest.json was written; no forcing downloads were attempted."
            )
        if forcing_plan["missing_forcing_files"] and self._allow_minimal_case_forcing_fetch():
            forcing_plan = self._fetch_minimal_missing_forcing(recipe_name, forcing_plan)

        staged: dict[str, Any] = {"recipe": recipe_name, "downloads": {}, "reuse_plan": forcing_plan}
        for row in forcing_plan["rows"]:
            if row["status"] != "ready":
                continue
            source_path = Path(row["source_path"])
            stage_path = Path(row["stage_path"])
            staged[row["forcing_kind"]] = self._stage_forcing_file(source_path, stage_path)
        self._write_forcing_reuse_manifest(
            selection,
            recipe_source,
            forcing_plan,
            actual_downloads=forcing_plan.get("actual_downloads") or [],
        )
        self._write_forcing_manifest(recipe_name, staged)
        return staged

    def _fetch_minimal_missing_forcing(self, recipe_name: str, forcing_plan: dict[str, Any]) -> dict[str, Any]:
        service = DataIngestionService()
        service.forcing_dir = self.persistent_forcing_dir
        service.forcing_dir.mkdir(parents=True, exist_ok=True)
        fetch_bbox = self._minimal_fetch_bbox()
        service.configure_explicit_download_window(start_date="2023-03-08", end_date="2023-03-13")
        service.nominal_forcing_start_utc = FORCING_WINDOW_START_UTC
        service.nominal_forcing_end_utc = FORCING_WINDOW_END_UTC
        service.effective_forcing_start_utc = FORCING_WINDOW_START_UTC
        service.effective_forcing_end_utc = FORCING_WINDOW_END_UTC
        service.bbox = [
            fetch_bbox["lon_min"],
            fetch_bbox["lon_max"],
            fetch_bbox["lat_min"],
            fetch_bbox["lat_max"],
        ]
        if getattr(service, "gfs_downloader", None) is not None:
            service.gfs_downloader.forcing_box = list(service.bbox)
        fetch_manifest = {
            "generated_at_utc": _now_utc(),
            "experiment_id": EXPERIMENT_ID,
            "forcing_recipe_id": recipe_name,
            "status": "started",
            "explicit_fetch_flag_detected": True,
            "bounded_window_start_utc": FORCING_WINDOW_START_UTC,
            "bounded_window_end_utc": FORCING_WINDOW_END_UTC,
            "fetch_window_start": FORCING_WINDOW_START_UTC,
            "fetch_window_end": FORCING_WINDOW_END_UTC,
            "bounded_bbox": fetch_bbox,
            "bbox": fetch_bbox,
            "phase1_rerun": False,
            "drifter_ingestion": False,
            "gfs_historical_preflight": False,
            "broad_gfs_ingestion": False,
            "monthly_gfs_fetch": False,
            "recipe_ranking": False,
            "requested_components": forcing_plan.get("missing_forcing_files", []),
            "components_requested": [row.get("filename", "") for row in forcing_plan.get("missing_forcing_files", [])],
            "components_downloaded": [],
            "files_written": [],
            "source_ids_or_urls": [],
            "proof_no_monthly_gfs_or_preflight_phase1_or_drifters": {
                "monthly_gfs_fetch": False,
                "gfs_historical_preflight": False,
                "phase1_rerun": False,
                "drifter_ingestion": False,
                "broad_gfs_ingestion": False,
            },
            "actual_downloads": [],
            "notes": (
                "Explicitly gated minimal bounded case-forcing fetch only. GFS is allowed here only "
                "as the already-resolved cmems_gfs wind provider; this is not Phase 1 or broad wind acquisition."
            ),
        }
        _write_json(self.output_dir / "minimal_case_forcing_fetch_manifest.json", fetch_manifest)
        actual_downloads: list[dict[str, Any]] = []
        for row in forcing_plan["missing_forcing_files"]:
            kind = "current" if row["forcing_kind"] == "currents" else row["forcing_kind"]
            source_id = source_id_for_recipe_component(forcing_kind=kind, filename=row["filename"])
            target_path = self.persistent_forcing_dir / row["filename"]
            if source_id == "gfs":
                record = self._fetch_minimal_gfs_wind(service, target_path)
            elif source_id == "cmems_wave":
                record = self._fetch_minimal_cmems_wave(service, target_path, fetch_bbox)
            else:
                record = {
                    "status": "failed",
                    "source_id": source_id,
                    "forcing_factor": row["filename"],
                    "path": str(target_path),
                    "error": "minimal fetch is restricted to missing GFS wind and CMEMS wave components in this pass",
                }
            record["minimal_case_fetch_used"] = True
            record["bounded_window_start_utc"] = FORCING_WINDOW_START_UTC
            record["bounded_window_end_utc"] = FORCING_WINDOW_END_UTC
            record["bounded_bbox"] = fetch_bbox
            record["phase1_rerun"] = False
            record["drifter_ingestion"] = False
            record["gfs_historical_preflight"] = False
            record["broad_gfs_ingestion"] = False
            record["monthly_gfs_fetch"] = False
            record["recipe_ranking"] = False
            actual_downloads.append(record)
            fetch_manifest["actual_downloads"] = actual_downloads
            if record.get("status") in SUCCESSFUL_FORCING_STATUSES:
                fetch_manifest["components_downloaded"].append(row["filename"])
                fetch_manifest["files_written"].append(str(record.get("path") or target_path))
                fetch_manifest["source_ids_or_urls"].append(
                    {
                        "component": row["filename"],
                        "source_id": source_id,
                        "source_url_or_provider": str(record.get("source_url") or record.get("provider") or source_id),
                    }
                )
            _write_json(self.output_dir / "minimal_case_forcing_fetch_manifest.json", fetch_manifest)
            if record.get("status") not in SUCCESSFUL_FORCING_STATUSES:
                forcing_plan["actual_downloads"] = actual_downloads
                selection, recipe_source = self._resolve_recipe()
                self._write_missing_forcing_manifest(selection, recipe_source, forcing_plan)
                self._write_forcing_reuse_manifest(selection, recipe_source, forcing_plan, actual_downloads=actual_downloads)
                fetch_manifest["status"] = "failed"
                _write_json(self.output_dir / "minimal_case_forcing_fetch_manifest.json", fetch_manifest)
                raise RuntimeError(f"Minimal case forcing fetch failed for {row['forcing_kind']}: {record}")
        refreshed = self._resolve_forcing_reuse_plan(recipe_name, stage_aliases=True)
        refreshed["actual_downloads"] = actual_downloads
        refreshed["minimal_case_fetch_used"] = bool(actual_downloads)
        fetch_manifest["status"] = "downloaded_and_validated" if not refreshed["missing_forcing_files"] else "insufficient_after_fetch"
        fetch_manifest["actual_downloads"] = actual_downloads
        fetch_manifest["post_fetch_missing_forcing_files"] = refreshed["missing_forcing_files"]
        fetch_manifest["validation_status"] = fetch_manifest["status"]
        _write_json(self.output_dir / "minimal_case_forcing_fetch_manifest.json", fetch_manifest)
        return refreshed

    def _fetch_minimal_gfs_wind(self, service: DataIngestionService, target_path: Path) -> dict[str, Any]:
        target_path.unlink(missing_ok=True)
        indexed = self._fetch_minimal_gfs_wind_from_s3_index(service, target_path)
        if indexed.get("status") in SUCCESSFUL_FORCING_STATUSES:
            return indexed
        return indexed

    def _fetch_minimal_gfs_wind_from_s3_index(
        self,
        service: DataIngestionService,
        target_path: Path,
    ) -> dict[str, Any]:
        start_utc = _normalize_utc(FORCING_WINDOW_START_UTC)
        end_utc = _normalize_utc(FORCING_WINDOW_END_UTC)
        timestamps = pd.date_range(start=start_utc, end=end_utc, freq="3h")
        target_path.parent.mkdir(parents=True, exist_ok=True)
        frames = []
        source_urls: list[str] = []
        temp_files: list[Path] = []
        try:
            for timestamp in timestamps:
                cycle_hour = int(timestamp.hour // 6) * 6
                cycle = timestamp.replace(hour=cycle_hour, minute=0, second=0, microsecond=0)
                lead_h = int((timestamp - cycle).total_seconds() // 3600)
                date = cycle.strftime("%Y%m%d")
                hour = cycle.strftime("%H")
                base_url = (
                    "https://noaa-gfs-bdp-pds.s3.amazonaws.com/"
                    f"gfs.{date}/{hour}/atmos/gfs.t{hour}z.pgrb2.0p25.f{lead_h:03d}"
                )
                grib_path = target_path.parent / f"gfs_range_{timestamp.strftime('%Y%m%d%H')}.grb2"
                self._download_gfs_10m_grib_messages(base_url, grib_path)
                with xr.open_dataset(grib_path, engine="cfgrib", backend_kwargs={"indexpath": ""}) as ds:
                    frames.append(service.gfs_downloader.normalize_gfs_subset(ds, analysis_time=timestamp).load())
                temp_files.append(grib_path)
                source_urls.append(base_url)
            combined = xr.concat(frames, dim="time").sortby("time")
            target_path.unlink(missing_ok=True)
            combined.to_netcdf(target_path)
            for temp_path in temp_files:
                temp_path.unlink(missing_ok=True)
            return {
                "status": "downloaded",
                "path": str(target_path),
                "source_id": "gfs",
                "forcing_factor": target_path.name,
                "provider": "NOAA GFS",
                "source_url": "https://noaa-gfs-bdp-pds.s3.amazonaws.com/",
                "source_system": "bounded_case_gfs_s3_idx_range_requests",
                "source_tier": "minimal_case",
                "source_url_count": len(source_urls),
                "sample_source_url": source_urls[0] if source_urls else "",
                "source_modes_used": ["s3_idx_range_requests"],
                "analysis_count": len(timestamps),
                "analysis_time_start_utc": _iso_z(timestamps.min()),
                "analysis_time_end_utc": _iso_z(timestamps.max()),
                "storage_tier": PERSISTENT_LOCAL_INPUT_STORE,
                "local_storage_path": str(target_path),
                "reuse_action": "downloaded_new_file",
                "validation_status": "downloaded_pending_reader_validation",
                "requested_start_utc": FORCING_WINDOW_START_UTC,
                "requested_end_utc": FORCING_WINDOW_END_UTC,
            }
        except Exception as exc:
            target_path.unlink(missing_ok=True)
            for temp_path in temp_files:
                temp_path.unlink(missing_ok=True)
            return {
                "status": "failed",
                "path": str(target_path),
                "source_id": "gfs",
                "forcing_factor": target_path.name,
                "provider": "NOAA GFS",
                "source_url": "https://noaa-gfs-bdp-pds.s3.amazonaws.com/",
                "failure_stage": "bounded_gfs_s3_idx_range_requests",
                "error": f"{type(exc).__name__}: {exc}",
                "requested_start_utc": FORCING_WINDOW_START_UTC,
                "requested_end_utc": FORCING_WINDOW_END_UTC,
            }

    @staticmethod
    def _download_gfs_10m_grib_messages(base_url: str, output_path: Path) -> None:
        index_response = requests.get(f"{base_url}.idx", timeout=60)
        index_response.raise_for_status()
        lines = [line.strip() for line in index_response.text.splitlines() if line.strip()]
        chunks: list[bytes] = []
        wanted = (":UGRD:10 m above ground:", ":VGRD:10 m above ground:")
        for index, line in enumerate(lines):
            if not any(token in line for token in wanted):
                continue
            parts = line.split(":")
            if len(parts) < 3:
                continue
            start = int(parts[1])
            end = int(lines[index + 1].split(":")[1]) - 1 if index + 1 < len(lines) else None
            headers = {"Range": f"bytes={start}-{end}" if end is not None else f"bytes={start}-"}
            response = requests.get(base_url, headers=headers, timeout=120)
            response.raise_for_status()
            if not response.content.startswith(b"GRIB"):
                raise RuntimeError(f"GFS range request did not return a GRIB message for {line}")
            chunks.append(response.content)
        if len(chunks) != 2:
            raise RuntimeError(f"Expected 2 GFS 10 m wind GRIB messages from {base_url}, found {len(chunks)}")
        output_path.write_bytes(b"".join(chunks))

    def _fetch_minimal_gfs_wind_legacy_downloader(self, service: DataIngestionService, target_path: Path) -> dict[str, Any]:
        budget_seconds = resolve_forcing_source_budget_seconds()
        try:
            record = dict(
                service.gfs_downloader.download(
                    start_time=FORCING_WINDOW_START_UTC,
                    end_time=FORCING_WINDOW_END_UTC,
                    output_path=target_path,
                    scratch_dir=self.persistent_forcing_dir,
                    budget_seconds=budget_seconds,
                )
                or {}
            )
            record.setdefault("status", "downloaded")
            record.update(
                {
                    "path": str(target_path),
                    "source_id": "gfs",
                    "forcing_factor": target_path.name,
                    "provider": "NOAA GFS analysis",
                    "source_url": str(record.get("sample_source_url") or "NOAA GFS bounded analysis sources"),
                    "source_system": "bounded_case_gfs_analysis_fetch",
                    "source_tier": "minimal_case",
                    "storage_tier": PERSISTENT_LOCAL_INPUT_STORE,
                    "local_storage_path": str(target_path),
                    "reuse_action": "downloaded_new_file",
                    "validation_status": "downloaded_pending_reader_validation",
                    "requested_start_utc": FORCING_WINDOW_START_UTC,
                    "requested_end_utc": FORCING_WINDOW_END_UTC,
                }
            )
            return record
        except Exception as exc:
            target_path.unlink(missing_ok=True)
            return {
                "status": "failed",
                "path": str(target_path),
                "source_id": "gfs",
                "forcing_factor": target_path.name,
                "provider": "NOAA GFS analysis",
                "failure_stage": str(getattr(exc, "failure_stage", "bounded_gfs_case_fetch")),
                "error": f"{type(exc).__name__}: {exc}",
                "requested_start_utc": FORCING_WINDOW_START_UTC,
                "requested_end_utc": FORCING_WINDOW_END_UTC,
            }

    def _fetch_minimal_cmems_wave(
        self,
        service: DataIngestionService,
        target_path: Path,
        fetch_bbox: dict[str, float],
    ) -> dict[str, Any]:
        try:
            from src.services import ingestion as ingestion_module
        except Exception as exc:
            return {
                "status": "failed",
                "path": str(target_path),
                "source_id": "cmems_wave",
                "forcing_factor": target_path.name,
                "provider": "Copernicus Marine",
                "failure_stage": "copernicusmarine_import",
                "error": f"{type(exc).__name__}: {exc}",
            }
        username = (
            os.environ.get("CMEMS_USERNAME")
            or os.environ.get("COPERNICUSMARINE_SERVICE_USERNAME")
            or os.environ.get("COPERNICUSMARINE_USERNAME")
        )
        password = (
            os.environ.get("CMEMS_PASSWORD")
            or os.environ.get("COPERNICUSMARINE_SERVICE_PASSWORD")
            or os.environ.get("COPERNICUSMARINE_PASSWORD")
        )
        if not username or not password:
            return {
                "status": "failed_missing_credentials",
                "path": str(target_path),
                "source_id": "cmems_wave",
                "forcing_factor": target_path.name,
                "provider": "Copernicus Marine",
                "failure_stage": "missing_credentials",
                "error": (
                    "CMEMS credentials are required in existing environment variables "
                    "CMEMS_USERNAME/CMEMS_PASSWORD or Copernicus Marine equivalents."
                ),
            }
        if getattr(ingestion_module, "copernicusmarine", None) is None:
            return {
                "status": "failed_missing_library",
                "path": str(target_path),
                "source_id": "cmems_wave",
                "forcing_factor": target_path.name,
                "provider": "Copernicus Marine",
                "failure_stage": "missing_copernicusmarine_library",
                "error": "copernicusmarine library is not installed in this runtime.",
            }
        target_path.parent.mkdir(parents=True, exist_ok=True)
        temp_filename = f"{target_path.stem}_minimal_download.nc"
        temp_path = target_path.parent / temp_filename
        target_path.unlink(missing_ok=True)
        temp_path.unlink(missing_ok=True)
        dataset_id = "cmems_mod_glo_wav_anfc_0.083deg_PT3H-i"
        try:
            kwargs = {
                "dataset_id": dataset_id,
                "minimum_longitude": fetch_bbox["lon_min"],
                "maximum_longitude": fetch_bbox["lon_max"],
                "minimum_latitude": fetch_bbox["lat_min"],
                "maximum_latitude": fetch_bbox["lat_max"],
                "start_datetime": FORCING_WINDOW_START_UTC.replace("Z", ""),
                "end_datetime": FORCING_WINDOW_END_UTC.replace("Z", ""),
                "variables": ["VHM0", "VSDX", "VSDY"],
                "output_filename": temp_filename,
                "output_directory": str(target_path.parent),
                "username": username,
                "password": password,
            }
            try:
                ingestion_module.copernicusmarine.subset(**kwargs, overwrite=True)
            except TypeError:
                ingestion_module.copernicusmarine.subset(**kwargs, force_download=True)
            if not temp_path.exists():
                raise RuntimeError(f"Copernicus Marine subset completed without producing {temp_path}.")
            temp_path.replace(target_path)
            return {
                "status": "downloaded",
                "path": str(target_path),
                "source_id": "cmems_wave",
                "forcing_factor": target_path.name,
                "provider": "Copernicus Marine",
                "source_url": dataset_id,
                "source_system": "copernicusmarine_subset",
                "source_tier": "minimal_case",
                "storage_tier": PERSISTENT_LOCAL_INPUT_STORE,
                "local_storage_path": str(target_path),
                "reuse_action": "downloaded_new_file",
                "validation_status": "downloaded_pending_reader_validation",
                "requested_start_utc": FORCING_WINDOW_START_UTC,
                "requested_end_utc": FORCING_WINDOW_END_UTC,
            }
        except Exception as exc:
            target_path.unlink(missing_ok=True)
            temp_path.unlink(missing_ok=True)
            return {
                "status": "failed",
                "path": str(target_path),
                "source_id": "cmems_wave",
                "forcing_factor": target_path.name,
                "provider": "Copernicus Marine",
                "source_url": dataset_id,
                "failure_stage": "bounded_cmems_wave_case_fetch",
                "error": f"{type(exc).__name__}: {exc}",
                "requested_start_utc": FORCING_WINDOW_START_UTC,
                "requested_end_utc": FORCING_WINDOW_END_UTC,
            }

    def _write_forcing_resolution_outputs(
        self,
        selection: RecipeSelection,
        recipe_source: str,
        forcing_plan: dict[str, Any],
        *,
        status: str,
    ) -> None:
        payload = {
            "generated_at_utc": _now_utc(),
            "experiment_id": EXPERIMENT_ID,
            "status": status,
            "forcing_recipe_id": selection.recipe,
            "forcing_recipe_source_file": selection.source_path,
            "forcing_recipe_source": recipe_source,
            "cmems_curr_status": next(
                (row.get("status", "") for row in forcing_plan.get("rows", []) if row.get("filename") == "cmems_curr.nc"),
                "",
            ),
            "gfs_wind_status": next(
                (row.get("status", "") for row in forcing_plan.get("rows", []) if row.get("filename") == "gfs_wind.nc"),
                "",
            ),
            "cmems_wave_status": next(
                (row.get("status", "") for row in forcing_plan.get("rows", []) if row.get("filename") == "cmems_wave.nc"),
                "",
            ),
            "local_forcing_reused": forcing_plan.get("local_forcing_reused", []),
            "required_start_utc": forcing_plan.get("required_start_utc", ""),
            "required_end_utc": forcing_plan.get("required_end_utc", ""),
            "required_bbox": forcing_plan.get("required_bbox", dict(FORCING_BBOX)),
            "phase1_rerun": False,
            "drifter_ingestion": False,
            "gfs_historical_preflight": False,
            "broad_gfs_ingestion": False,
            "monthly_gfs_fetch": False,
            "recipe_ranking": False,
            "model_run_executed": False,
            "search_roots": forcing_plan.get("search_roots", []),
            "rows": forcing_plan.get("rows", []),
            "planned_downloads": forcing_plan.get("planned_downloads", []),
            "actual_downloads": forcing_plan.get("actual_downloads", []),
            "minimal_case_fetch_used": bool(forcing_plan.get("minimal_case_fetch_used", False)),
            "notes": (
                "This phase only resolves local or explicitly approved bounded case forcing into the "
                "experiment forcing directory. It does not run OpenDrift, PyGNOME, Phase 1, drifter "
                "ingestion, broad wind acquisition, or recipe-selection workflows."
            ),
        }
        _write_json(self.output_dir / "forcing_resolution_manifest.json", payload)

        audit_rows: list[dict[str, Any]] = []
        alias_rows: list[dict[str, Any]] = []
        for row in forcing_plan.get("rows", []):
            inspections = row.get("candidate_inspections") or []
            if not inspections:
                audit_rows.append(
                    {
                        "required_logical_name": row.get("filename", ""),
                        "forcing_kind": row.get("forcing_kind", ""),
                        "candidate_path": "",
                        "provider": row.get("provider", ""),
                        "status": row.get("status", ""),
                        "time_start_utc": "",
                        "time_end_utc": "",
                        "source_bbox": "",
                        "variables_found": "",
                        "reader_compatibility_status": "",
                        "quality_status": "",
                        "reason_not_accepted": row.get("next_required_action", ""),
                    }
                )
            for inspection in inspections:
                audit_rows.append(
                    {
                        "required_logical_name": row.get("filename", ""),
                        "forcing_kind": row.get("forcing_kind", ""),
                        "candidate_path": inspection.get("candidate_path", inspection.get("path", "")),
                        "provider": row.get("provider", ""),
                        "status": inspection.get("status", ""),
                        "time_start_utc": inspection.get("time_start_utc", ""),
                        "time_end_utc": inspection.get("time_end_utc", ""),
                        "source_bbox": json.dumps(inspection.get("source_bbox", {}), default=_json_default),
                        "variables_found": ";".join(str(value) for value in inspection.get("variables_found", [])),
                        "reader_compatibility_status": inspection.get("reader_compatibility_status", ""),
                        "quality_status": inspection.get("quality_status", ""),
                        "reason_not_accepted": inspection.get("stop_reason", ""),
                    }
                )
            inspection = dict(row.get("inspection") or {})
            alias_rows.append(
                {
                    "required_logical_name": row.get("filename", ""),
                    "source_file_found_or_fetched": row.get("source_path", ""),
                    "source_file_found": row.get("source_path", ""),
                    "source_provider": row.get("provider", ""),
                    "source_time_start": inspection.get("time_start_utc", ""),
                    "source_time_end": inspection.get("time_end_utc", ""),
                    "source_bbox": inspection.get("source_bbox", {}),
                    "variables_found": inspection.get("variables_found", []),
                    "alias_or_subset_path": row.get("stage_path", ""),
                    "validation_status": row.get("status", ""),
                    "notes": row.get("alias_action", "") or inspection.get("stop_reason", ""),
                }
            )
        _write_csv(self.output_dir / "forcing_coverage_audit.csv", pd.DataFrame(audit_rows))
        _write_json(self.output_dir / "forcing_alias_manifest.json", alias_rows)

    def _write_forcing_reuse_manifest(
        self,
        selection: RecipeSelection,
        recipe_source: str,
        forcing_plan: dict[str, Any],
        *,
        actual_downloads: list[dict[str, Any]],
    ) -> Path:
        payload = {
            "generated_at_utc": _now_utc(),
            "experiment_id": EXPERIMENT_ID,
            "forcing_recipe_id": selection.recipe,
            "forcing_recipe_source_file": selection.source_path,
            "forcing_recipe_source": recipe_source,
            "current_provider": forcing_plan.get("current_provider", ""),
            "wind_provider": forcing_plan.get("wind_provider", ""),
            "wave_provider": forcing_plan.get("wave_provider", ""),
            "phase1_rerun": False,
            "drifter_ingestion": False,
            "gfs_historical_preflight": False,
            "local_forcing_reused": forcing_plan.get("local_forcing_reused", []),
            "missing_forcing_files": forcing_plan.get("missing_forcing_files", []),
            "minimal_case_fetch_used": bool(forcing_plan.get("minimal_case_fetch_used", False)),
            "planned_downloads": forcing_plan.get("planned_downloads", []),
            "actual_downloads": actual_downloads,
            "required_start_utc": forcing_plan.get("required_start_utc", ""),
            "required_end_utc": forcing_plan.get("required_end_utc", ""),
            "notes": (
                "Default policy is local/case forcing reuse only. GFS is used only because it is "
                "the inherited wind provider of the resolved B1 recipe; this manifest does not imply "
                "a new historical wind-ingestion workflow or Phase 1 recipe selection."
            ),
        }
        path = self.output_dir / "forcing_reuse_manifest.json"
        _write_json(path, payload)
        _write_csv(
            self.output_dir / "forcing_reuse_manifest.csv",
            pd.DataFrame(
                [
                    {
                        "experiment_id": EXPERIMENT_ID,
                        "forcing_recipe_id": selection.recipe,
                        "forcing_recipe_source_file": selection.source_path,
                        "current_provider": forcing_plan.get("current_provider", ""),
                        "wind_provider": forcing_plan.get("wind_provider", ""),
                        "wave_provider": forcing_plan.get("wave_provider", ""),
                        "phase1_rerun": False,
                        "drifter_ingestion": False,
                        "gfs_historical_preflight": False,
                        "local_forcing_reused": ";".join(
                            str(row.get("source_path") or "") for row in forcing_plan.get("local_forcing_reused", [])
                        ),
                        "missing_forcing_files": ";".join(
                            str(row.get("filename") or "") for row in forcing_plan.get("missing_forcing_files", [])
                        ),
                        "minimal_case_fetch_used": bool(forcing_plan.get("minimal_case_fetch_used", False)),
                        "planned_downloads": json.dumps(forcing_plan.get("planned_downloads", []), default=_json_default),
                        "actual_downloads": json.dumps(actual_downloads, default=_json_default),
                        "notes": payload["notes"],
                    }
                ]
            ),
        )
        return path

    def _write_missing_forcing_manifest(
        self,
        selection: RecipeSelection,
        recipe_source: str,
        forcing_plan: dict[str, Any],
    ) -> Path:
        payload = {
            "generated_at_utc": _now_utc(),
            "experiment_id": EXPERIMENT_ID,
            "status": "blocked_missing_forcing",
            "forcing_recipe_id": selection.recipe,
            "forcing_recipe_source_file": selection.source_path,
            "forcing_recipe_source": recipe_source,
            "phase1_rerun": False,
            "drifter_ingestion": False,
            "gfs_historical_preflight": False,
            "planned_downloads": forcing_plan.get("planned_downloads", []),
            "actual_downloads": forcing_plan.get("actual_downloads", []),
            "missing_forcing_files": forcing_plan.get("missing_forcing_files", []),
            "missing_components": [
                {
                    "missing_logical_file": row.get("filename", ""),
                    "missing_provider": row.get("provider", ""),
                    "missing_variable_group": row.get("variable_group", row.get("forcing_kind", "")),
                    "missing_time_interval": {
                        "start_utc": forcing_plan.get("required_start_utc", ""),
                        "end_utc": forcing_plan.get("required_end_utc", ""),
                    },
                    "missing_spatial_coverage": forcing_plan.get("required_bbox", dict(FORCING_BBOX)),
                    "local_stores_searched": row.get("local_stores_searched", forcing_plan.get("search_roots", [])),
                    "reason_not_accepted": "; ".join(
                        str(item.get("stop_reason", ""))
                        for item in row.get("candidate_inspections", [])
                        if str(item.get("stop_reason", "")).strip()
                    )
                    or "No local file passed provider, variable, time, bbox, quality, and reader compatibility checks.",
                    "next_required_action": row.get("next_required_action", ""),
                }
                for row in forcing_plan.get("missing_forcing_files", [])
            ],
            "required_start_utc": forcing_plan.get("required_start_utc", ""),
            "required_end_utc": forcing_plan.get("required_end_utc", ""),
            "required_bbox": forcing_plan.get("required_bbox", dict(FORCING_BBOX)),
            "local_stores_searched": forcing_plan.get("search_roots", []),
            "stop_reason": (
                f"Missing local March 9-12 forcing for the resolved {selection.recipe} B1 recipe. "
                f"Set {ALLOW_MINIMAL_CASE_FORCING_FETCH_ENV}=1 only if a bounded case-forcing fetch is explicitly approved."
            ),
        }
        path = self.output_dir / "missing_forcing_manifest.json"
        _write_json(path, payload)
        return path

    def _write_no_missing_forcing_manifest(
        self,
        selection: RecipeSelection,
        recipe_source: str,
        forcing_plan: dict[str, Any],
    ) -> Path:
        payload = {
            "generated_at_utc": _now_utc(),
            "experiment_id": EXPERIMENT_ID,
            "status": "resolved_no_missing_forcing",
            "forcing_recipe_id": selection.recipe,
            "forcing_recipe_source_file": selection.source_path,
            "forcing_recipe_source": recipe_source,
            "phase1_rerun": False,
            "drifter_ingestion": False,
            "gfs_historical_preflight": False,
            "broad_gfs_ingestion": False,
            "monthly_gfs_fetch": False,
            "recipe_ranking": False,
            "model_run_executed": False,
            "planned_downloads": [],
            "actual_downloads": forcing_plan.get("actual_downloads", []),
            "missing_forcing_files": [],
            "missing_components": [],
            "required_start_utc": forcing_plan.get("required_start_utc", ""),
            "required_end_utc": forcing_plan.get("required_end_utc", ""),
            "required_bbox": forcing_plan.get("required_bbox", dict(FORCING_BBOX)),
            "local_stores_searched": forcing_plan.get("search_roots", []),
            "local_forcing_reused": forcing_plan.get("local_forcing_reused", []),
            "notes": (
                "Supersedes the earlier blocked manifest: all required March 9-12 logical forcing "
                "files are now staged and reader-validated for the resolved B1 recipe. No model run "
                "was executed by this dry-run."
            ),
        }
        path = self.output_dir / "missing_forcing_manifest.json"
        _write_json(path, payload)
        return path

    def _stage_forcing_file(self, source_path: Path, target_path: Path) -> Path:
        if source_path.exists():
            stage_store_file(source_path, target_path)
        return target_path

    def _write_forcing_failure_manifest(self, recipe_name: str, downloads: dict[str, Any], degraded: bool = False) -> Path:
        payload = {
            "generated_at_utc": _now_utc(),
            "experiment_id": EXPERIMENT_ID,
            "recipe": recipe_name,
            "downloads": downloads,
            "status": "degraded_skipped_forcing_outage" if degraded else "failed_download",
            "forcing_outage_policy": self.forcing_outage_policy,
        }
        path = self.output_dir / "forcing_window_manifest.json"
        _write_json(path, payload)
        return path

    def _write_forcing_manifest(self, recipe_name: str, forcing_paths: dict[str, Any]) -> dict[str, Any]:
        required_start, required_end = self._required_forcing_time_bounds()
        rows = [
            {
                "forcing_kind": "current",
                **self._forcing_time_and_vars(Path(forcing_paths["currents"]), ["uo", "vo"], required_start, required_end),
            },
            {
                "forcing_kind": "wind",
                **self._forcing_time_and_vars(Path(forcing_paths["wind"]), ["x_wind", "y_wind"], required_start, required_end),
            },
        ]
        if forcing_paths.get("wave"):
            rows.append(
                {
                    "forcing_kind": "wave",
                    **self._forcing_time_and_vars(Path(forcing_paths["wave"]), ["VHM0", "VSDX", "VSDY"], required_start, required_end),
                }
            )
        payload = {
            "generated_at_utc": _now_utc(),
            "experiment_id": EXPERIMENT_ID,
            "recipe": recipe_name,
            "required_start_utc": _iso_z(required_start),
            "required_end_utc": _iso_z(required_end),
            "rows": rows,
            "status": "ready" if all(row["status"] == "ready" for row in rows) else "insufficient",
        }
        _write_json(self.output_dir / "forcing_window_manifest.json", payload)
        _write_csv(self.output_dir / "forcing_window_manifest.csv", pd.DataFrame(rows))
        if payload["status"] != "ready":
            raise RuntimeError(f"{EXPERIMENT_ID} forcing coverage is incomplete: {payload}")
        return payload

    def _forcing_time_and_vars(
        self,
        path: Path,
        required_vars: list[str],
        required_start: pd.Timestamp,
        required_end: pd.Timestamp,
    ) -> dict[str, Any]:
        row = {
            "path": str(path),
            "exists": path.exists(),
            "time_start_utc": "",
            "time_end_utc": "",
            "required_start_utc": _iso_z(required_start),
            "required_end_utc": _iso_z(required_end),
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
            required_text = " ".join(required_vars).lower()
            if "wind" in required_text or "u10" in required_text:
                detected, variable_error = self._detect_forcing_variables(ds, "wind")
            elif "vhm" in required_text or "stokes" in required_text or "vsd" in required_text:
                detected, variable_error = self._detect_forcing_variables(ds, "wave")
            else:
                detected, variable_error = self._detect_forcing_variables(ds, "currents")
            missing = [] if detected else [name for name in required_vars if name not in variables]
            if variable_error and not missing:
                missing = [variable_error]
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
                reasons.append("time coverage does not cover required experiment window")
            row["stop_reason"] = "; ".join(reasons)
        return row

    def _selected_retention_overrides(self) -> dict[str, Any]:
        ensemble_path = self.repo_root / "config" / "ensemble.yaml"
        if not ensemble_path.exists():
            return {}
        config = yaml.safe_load(ensemble_path.read_text(encoding="utf-8")) or {}
        retention = config.get("official_retention") or {}
        selected = str(retention.get("selected_mode") or "").strip()
        scenario = (retention.get("scenarios") or {}).get(selected) or {}
        if not scenario:
            return {}
        return {
            "coastline_action": scenario.get("coastline_action"),
            "coastline_approximation_precision": scenario.get("coastline_approximation_precision"),
            "time_step_minutes": scenario.get("time_step_minutes"),
        }

    def run_opendrift_pairs(self, selection, recipe_source: str, forcing_paths: dict[str, Any]) -> list[dict[str, Any]]:
        observation_map = self._observation_record_map()
        records = []
        for pair in FORECAST_PAIRS:
            seed_record = observation_map[pair.seed_mask_id]
            pair_dir = self.forecast_dir / pair.pair_id
            pair_dir.mkdir(parents=True, exist_ok=True)
            model_run_name = f"{RUN_NAME}/experiments/mar09_11_12_multisource/opendrift/{pair.pair_id}/model_run"
            model_dir = get_case_output_dir(model_run_name)
            forecast_manifest = model_dir / "forecast" / "forecast_manifest.json"
            member_paths = sorted((model_dir / "ensemble").glob("member_*.nc"))
            if forecast_manifest.exists() and len(member_paths) >= EXPECTED_ENSEMBLE_MEMBER_COUNT and not self.force_rerun:
                run_result = {"status": "reused_existing_run", "forecast_manifest": str(forecast_manifest)}
                self._assert_model_run_element_cap(model_dir)
            else:
                start_lat, start_lon = _resolve_polygon_reference_point(
                    Path(seed_record["processed_vector_path"]),
                    geometry_type="polygon",
                )
                old_override = os.environ.get(OPENDRIFT_ELEMENT_OVERRIDE_ENV)
                os.environ[OPENDRIFT_ELEMENT_OVERRIDE_ENV] = str(ELEMENT_CAP)
                try:
                    run_result = run_official_spill_forecast(
                        selection=selection,
                        start_time=pair.start_utc,
                        start_lat=float(start_lat),
                        start_lon=float(start_lon),
                        output_run_name=model_run_name,
                        forcing_override=forcing_paths,
                        sensitivity_context={
                            "experiment_id": EXPERIMENT_ID,
                            "pair_id": pair.pair_id,
                            "archive_only": True,
                            "reportable": False,
                            "thesis_facing": False,
                            "element_cap": ELEMENT_CAP,
                            "phase1_rerun": False,
                            "drifter_ingestion": False,
                            "claim_boundary": CLAIM_BOUNDARY,
                        },
                        historical_baseline_provenance={
                            "recipe": selection.recipe,
                            "source_kind": selection.source_kind,
                            "source_path": selection.source_path,
                            "note": selection.note,
                            "phase1_rerun": False,
                        },
                        simulation_start_utc=pair.start_utc,
                        simulation_end_utc=pair.target_utc,
                        snapshot_hours=[int(pair.nominal_lead_h)],
                        date_composite_dates=[pair.target_date],
                        transport_overrides=self._selected_retention_overrides(),
                        seed_overrides={
                            "polygon_vector_path": seed_record["processed_vector_path"],
                            "source_geometry_label": pair.seed_mask_id,
                        },
                    )
                finally:
                    if old_override is None:
                        os.environ.pop(OPENDRIFT_ELEMENT_OVERRIDE_ENV, None)
                    else:
                        os.environ[OPENDRIFT_ELEMENT_OVERRIDE_ENV] = old_override
                if run_result.get("status") != "success":
                    raise RuntimeError(f"OpenDrift run failed for {pair.pair_id}: {run_result}")
                self._assert_model_run_element_cap(model_dir)
            product_record = self._build_opendrift_pair_products(pair, model_dir)
            records.append(
                {
                    "pair_id": pair.pair_id,
                    "model_run_name": model_run_name,
                    "model_dir": str(model_dir),
                    "run_result": run_result,
                    **product_record,
                }
            )
        _write_json(
            self.output_dir / "opendrift_forecast_manifest.json",
            {"generated_at_utc": _now_utc(), "experiment_id": EXPERIMENT_ID, "pairs": records},
        )
        self._write_element_count_audit(records)
        return records

    def _build_opendrift_pair_products(self, pair: ForecastPairSpec, model_dir: Path) -> dict[str, Any]:
        if xr is None:
            raise ImportError("xarray is required to build OpenDrift experiment products")
        product_dir = self.forecast_dir / pair.pair_id / "products"
        product_dir.mkdir(parents=True, exist_ok=True)
        target_time = _normalize_utc(pair.target_utc)
        control_paths = sorted((model_dir / "forecast").glob("deterministic_control_*.nc"))
        if not control_paths:
            raise FileNotFoundError(f"Missing deterministic control NetCDF under {model_dir / 'forecast'}")
        control_path = control_paths[0]
        lon, lat, mass, actual_control_time, mass_meta = extract_particles_at_time(control_path, target_time, "opendrift")
        det_hits, det_density = rasterize_particles(self.grid, lon, lat, mass)
        det_hits = apply_ocean_mask(det_hits, sea_mask=self.sea_mask, fill_value=0.0)
        det_density = apply_ocean_mask(det_density, sea_mask=self.sea_mask, fill_value=0.0)
        det_path = product_dir / "opendrift_deterministic_footprint.tif"
        det_density_path = product_dir / "opendrift_deterministic_density_norm.tif"
        save_raster(self.grid, det_hits.astype(np.float32), det_path)
        save_raster(self.grid, det_density.astype(np.float32), det_density_path)

        member_paths = sorted((model_dir / "ensemble").glob("member_*.nc"))
        member_masks = []
        member_times = []
        for member_path in member_paths:
            lon, lat, mass, actual_time, _ = extract_particles_at_time(member_path, target_time, "opendrift")
            hits, _ = rasterize_particles(self.grid, lon, lat, mass)
            hits = apply_ocean_mask((hits > 0).astype(np.float32), sea_mask=self.sea_mask, fill_value=0.0)
            member_masks.append(hits.astype(np.float32))
            member_times.append(_iso_z(actual_time))
        probability = np.mean(np.stack(member_masks, axis=0), axis=0).astype(np.float32) if member_masks else np.zeros((self.grid.height, self.grid.width), dtype=np.float32)
        probability = apply_ocean_mask(probability, sea_mask=self.sea_mask, fill_value=0.0)
        p50 = apply_ocean_mask((probability >= 0.50).astype(np.float32), sea_mask=self.sea_mask, fill_value=0.0)
        p90 = apply_ocean_mask((probability >= 0.90).astype(np.float32), sea_mask=self.sea_mask, fill_value=0.0)
        prob_path = product_dir / "opendrift_prob_presence.tif"
        p50_path = product_dir / "opendrift_mask_p50.tif"
        p90_path = product_dir / "opendrift_mask_p90.tif"
        save_raster(self.grid, probability.astype(np.float32), prob_path)
        save_raster(self.grid, p50.astype(np.float32), p50_path)
        save_raster(self.grid, p90.astype(np.float32), p90_path)
        return {
            "target_time_utc": pair.target_utc,
            "deterministic_control_netcdf": str(control_path),
            "deterministic_actual_time_utc": _iso_z(actual_control_time),
            "deterministic_mass_strategy": mass_meta.get("mass_strategy", ""),
            "member_count": int(len(member_paths)),
            "member_actual_times_utc": sorted(set(member_times)),
            "opendrift_deterministic": str(det_path),
            "opendrift_deterministic_density": str(det_density_path),
            "opendrift_prob_presence": str(prob_path),
            "opendrift_mask_p50": str(p50_path),
            "opendrift_mask_p90": str(p90_path),
            "opendrift_deterministic_cells": int(np.count_nonzero(det_hits > 0)),
            "opendrift_p50_cells": int(np.count_nonzero(p50 > 0)),
            "opendrift_p90_cells": int(np.count_nonzero(p90 > 0)),
        }

    def _collect_element_counts(self, value: Any, path: str = "") -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        if isinstance(value, dict):
            for key, child in value.items():
                child_path = f"{path}.{key}" if path else str(key)
                if key in {
                    "element_count",
                    "actual_element_count",
                    "seed_element_count",
                    "element_count_actual",
                    "element_count_requested",
                    "benchmark_particles",
                    "num_elements",
                }:
                    try:
                        rows.append({"path": child_path, "value": int(child)})
                    except Exception:
                        pass
                rows.extend(self._collect_element_counts(child, child_path))
        elif isinstance(value, list):
            for index, child in enumerate(value):
                rows.extend(self._collect_element_counts(child, f"{path}[{index}]"))
        return rows

    def _element_count_rows_for_model_dir(self, model_dir: Path) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for path in [
            model_dir / "forecast" / "forecast_manifest.json",
            model_dir / "ensemble" / "ensemble_manifest.json",
            model_dir / "forecast" / "phase2_loading_audit.json",
        ]:
            payload = _read_json(path)
            if not payload:
                continue
            for row in self._collect_element_counts(payload):
                rows.append({"manifest_path": str(path), **row})
        return rows

    def _assert_model_run_element_cap(self, model_dir: Path) -> None:
        rows = self._element_count_rows_for_model_dir(model_dir)
        exceeded = [row for row in rows if int(row["value"]) > ELEMENT_CAP]
        if exceeded:
            raise RuntimeError(
                f"{EXPERIMENT_ID} model run exceeds the {ELEMENT_CAP} element cap in {model_dir}: {exceeded[:5]}"
            )

    def _write_element_count_audit(self, forecast_records: list[dict[str, Any]], pygnome_records: list[dict[str, Any]] | None = None) -> Path:
        rows: list[dict[str, Any]] = []
        for record in forecast_records:
            model_dir = Path(str(record.get("model_dir") or ""))
            for count in self._element_count_rows_for_model_dir(model_dir):
                rows.append(
                    {
                        "experiment_id": EXPERIMENT_ID,
                        "pair_id": record.get("pair_id", ""),
                        "model": "opendrift",
                        "manifest_path": count["manifest_path"],
                        "count_path": count["path"],
                        "element_count": int(count["value"]),
                        "element_cap": ELEMENT_CAP,
                        "within_cap": int(count["value"]) <= ELEMENT_CAP,
                    }
                )
        for record in pygnome_records or []:
            metadata = record.get("metadata") or {}
            value = int(metadata.get("benchmark_particles") or record.get("forecast_cells") or 0)
            rows.append(
                {
                    "experiment_id": EXPERIMENT_ID,
                    "pair_id": record.get("pair_id", ""),
                    "model": "pygnome",
                    "manifest_path": str(record.get("netcdf_path") or ""),
                    "count_path": "metadata.benchmark_particles",
                    "element_count": value,
                    "element_cap": ELEMENT_CAP,
                    "within_cap": value <= ELEMENT_CAP,
                }
            )
        exceeded = [row for row in rows if not row["within_cap"]]
        payload = {
            "generated_at_utc": _now_utc(),
            "experiment_id": EXPERIMENT_ID,
            "element_cap": ELEMENT_CAP,
            "rows": rows,
            "status": "failed" if exceeded else "passed",
            "exceeded": exceeded,
        }
        path = self.output_dir / "element_count_audit.json"
        _write_json(path, payload)
        _write_csv(self.output_dir / "element_count_audit.csv", pd.DataFrame(rows))
        if exceeded:
            raise RuntimeError(f"{EXPERIMENT_ID} element cap exceeded: {exceeded[:5]}")
        return path

    def _load_opendrift_manifest_records(self) -> list[dict[str, Any]]:
        path = self.output_dir / "opendrift_forecast_manifest.json"
        if not path.exists():
            return []
        return (json.loads(path.read_text(encoding="utf-8")) or {}).get("pairs") or []

    def _resolved_forcing_paths_for_pygnome(self) -> dict[str, Path]:
        defaults = {
            "currents": self.forcing_dir / "cmems_curr.nc",
            "wind": self.forcing_dir / "gfs_wind.nc",
        }
        resolved_path = self.output_dir / "run_config_resolved.yaml"
        if not resolved_path.exists():
            return defaults
        payload = yaml.safe_load(resolved_path.read_text(encoding="utf-8")) or {}
        forcing_paths = payload.get("forcing_paths") or {}
        return {
            "currents": Path(str(forcing_paths.get("currents") or defaults["currents"])),
            "wind": Path(str(forcing_paths.get("wind") or defaults["wind"])),
        }

    def _resolved_forcing_paths_for_manifest(self) -> dict[str, Any]:
        defaults = {
            "currents": self.forcing_dir / "cmems_curr.nc",
            "wind": self.forcing_dir / "gfs_wind.nc",
            "wave": self.forcing_dir / "cmems_wave.nc",
        }
        resolved_path = self.output_dir / "run_config_resolved.yaml"
        if not resolved_path.exists():
            return defaults
        payload = yaml.safe_load(resolved_path.read_text(encoding="utf-8")) or {}
        forcing_paths = payload.get("forcing_paths") or {}
        return {
            "currents": Path(str(forcing_paths.get("currents") or defaults["currents"])),
            "wind": Path(str(forcing_paths.get("wind") or defaults["wind"])),
            "wave": Path(str(forcing_paths.get("wave") or defaults["wave"])),
        }

    def run_pygnome_pairs(self) -> dict[str, Any]:
        try:
            from src.services.gnome_comparison import GNOME_AVAILABLE, GnomeComparisonService
        except Exception as exc:  # pragma: no cover
            GNOME_AVAILABLE = False
            GnomeComparisonService = None
            import_error = str(exc)
        else:
            import_error = ""
        if not GNOME_AVAILABLE or GnomeComparisonService is None:
            payload = {
                "generated_at_utc": _now_utc(),
                "experiment_id": EXPERIMENT_ID,
                "status": "pygnome_unavailable",
                "import_error": import_error,
                "comparator_only": True,
            }
            _write_json(self.output_dir / "pygnome_comparator_manifest.json", payload)
            raise RuntimeError("PyGNOME is unavailable; run this phase in the gnome Docker service.")

        observation_map = self._observation_record_map()
        pygnome_forcing = self._resolved_forcing_paths_for_pygnome()
        records = []
        for pair in FORECAST_PAIRS:
            seed_record = observation_map[pair.seed_mask_id]
            product_dir = self.pygnome_dir / pair.pair_id / "products"
            model_dir = self.pygnome_dir / pair.pair_id / "model_run"
            product_dir.mkdir(parents=True, exist_ok=True)
            model_dir.mkdir(parents=True, exist_ok=True)
            nc_path = model_dir / f"{pair.pair_id.lower()}_pygnome_deterministic_control.nc"
            metadata_path = model_dir / f"{pair.pair_id.lower()}_pygnome_metadata.json"
            if nc_path.exists() and metadata_path.exists() and not self.force_rerun:
                metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
            else:
                start_lat, start_lon = _resolve_polygon_reference_point(
                    Path(seed_record["processed_vector_path"]),
                    geometry_type="polygon",
                )
                gnome = GnomeComparisonService()
                gnome.output_dir = model_dir
                py_nc, metadata = gnome.run_transport_benchmark_scenario(
                    start_lat=float(start_lat),
                    start_lon=float(start_lon),
                    start_time=pair.start_utc,
                    output_name=nc_path.name,
                    random_seed=pair.pygnome_random_seed,
                    polygon_path=seed_record["processed_vector_path"],
                    seed_time_override=pair.start_utc,
                    duration_hours=int(pair.nominal_lead_h),
                    time_step_minutes=60,
                    winds_file=pygnome_forcing["wind"],
                    currents_file=pygnome_forcing["currents"],
                    allow_degraded_forcing=True,
                )
                nc_path = Path(py_nc)
                metadata["comparator_only"] = True
                metadata["wave_stokes_limitation"] = (
                    "PyGNOME comparator does not reproduce the exact OpenDrift wave/Stokes stack in this workflow."
                )
                _write_json(metadata_path, metadata)
            product_record = self._build_pygnome_pair_products(pair, nc_path, product_dir)
            records.append({**product_record, "pair_id": pair.pair_id, "metadata": metadata, "netcdf_path": str(nc_path)})
        payload = {
            "generated_at_utc": _now_utc(),
            "experiment_id": EXPERIMENT_ID,
            "status": "completed",
            "comparator_only": True,
            "not_truth": True,
            "records": records,
        }
        _write_json(self.output_dir / "pygnome_comparator_manifest.json", payload)
        self._write_element_count_audit(self._load_opendrift_manifest_records(), pygnome_records=records)
        return payload

    def _build_pygnome_pair_products(self, pair: ForecastPairSpec, nc_path: Path, product_dir: Path) -> dict[str, Any]:
        target_time = _normalize_utc(pair.target_utc)
        lon, lat, mass, actual_time, mass_meta = extract_particles_at_time(
            nc_path,
            target_time,
            "pygnome",
            allow_uniform_mass_fallback=True,
        )
        hits, density = rasterize_particles(self.grid, lon, lat, mass)
        hits = apply_ocean_mask(hits, sea_mask=self.sea_mask, fill_value=0.0)
        density = apply_ocean_mask(density, sea_mask=self.sea_mask, fill_value=0.0)
        footprint_path = product_dir / "pygnome_deterministic_comparator_footprint.tif"
        density_path = product_dir / "pygnome_deterministic_comparator_density_norm.tif"
        save_raster(self.grid, hits.astype(np.float32), footprint_path)
        save_raster(self.grid, density.astype(np.float32), density_path)
        return {
            "pygnome_footprint": str(footprint_path),
            "pygnome_density": str(density_path),
            "actual_time_utc": _iso_z(actual_time),
            "mass_strategy": mass_meta.get("mass_strategy", ""),
            "forecast_cells": int(np.count_nonzero(hits > 0)),
            "empty_forecast_warning": "zero_positive_cells" if int(np.count_nonzero(hits > 0)) <= 0 else "",
        }

    def finalize_scorecards(self, *, include_pygnome: bool) -> tuple[Path, Path]:
        observation_map = self._observation_record_map()
        fss_rows: list[dict[str, Any]] = []
        diagnostic_rows: list[dict[str, Any]] = []
        for pair in FORECAST_PAIRS:
            target_record = observation_map[pair.target_mask_id]
            target_path = Path(target_record["mask_path"])
            surfaces = [
                ("opendrift_deterministic", self.forecast_dir / pair.pair_id / "products" / "opendrift_deterministic_footprint.tif", ""),
                ("opendrift_mask_p50", self.forecast_dir / pair.pair_id / "products" / "opendrift_mask_p50.tif", ""),
                ("opendrift_mask_p90", self.forecast_dir / pair.pair_id / "products" / "opendrift_mask_p90.tif", ""),
            ]
            if include_pygnome:
                surfaces.append(
                    (
                        "pygnome_deterministic_comparator",
                        self.pygnome_dir / pair.pair_id / "products" / "pygnome_deterministic_comparator_footprint.tif",
                        "PyGNOME comparator-only; never validation truth.",
                    )
                )
            for surface_id, forecast_path, note in surfaces:
                if not forecast_path.exists():
                    if include_pygnome:
                        raise FileNotFoundError(f"Missing forecast surface {surface_id}: {forecast_path}")
                    continue
                fss_row, diag_row = self._score_surface(pair, surface_id, forecast_path, target_path, note)
                fss_rows.append(fss_row)
                diagnostic_rows.append(diag_row)

        scorecard = pd.DataFrame(fss_rows)
        diagnostics = pd.DataFrame(diagnostic_rows)
        if include_pygnome:
            assert_scorecard_contract(scorecard, expected_rows=12)
        scorecard_path = self.output_dir / "scorecard_fss_by_pair_surface.csv"
        diagnostics_path = self.output_dir / "scorecard_geometry_diagnostics.csv"
        _write_csv(scorecard_path, scorecard, SCORECARD_COLUMNS)
        _write_csv(diagnostics_path, diagnostics, DIAGNOSTIC_COLUMNS)
        scoring_payload = {
            "generated_at_utc": _now_utc(),
            "experiment_id": EXPERIMENT_ID,
            "include_pygnome": bool(include_pygnome),
            "row_count": int(len(scorecard.index)),
            "expected_final_row_count": 12,
            "windows_km": list(OFFICIAL_PHASE3B_WINDOWS_KM),
            "grid": self.grid.spec.to_metadata(),
            "valid_ocean_mask_path": self.grid.spec.sea_mask_path,
            "scorecard_fss_by_pair_surface": str(scorecard_path),
            "scorecard_geometry_diagnostics": str(diagnostics_path),
        }
        _write_json(self.output_dir / "scoring_manifest.json", scoring_payload)
        return scorecard_path, diagnostics_path

    def _score_surface(
        self,
        pair: ForecastPairSpec,
        surface_id: str,
        forecast_path: Path,
        target_path: Path,
        note: str,
    ) -> tuple[dict[str, Any], dict[str, Any]]:
        precheck = precheck_same_grid(
            forecast=forecast_path,
            target=target_path,
            report_base_path=self.precheck_dir / f"{pair.pair_id}_{surface_id}",
        )
        if not precheck.passed:
            raise RuntimeError(f"Same-grid precheck failed for {pair.pair_id} {surface_id}: {precheck.json_report_path}")
        forecast = self.helper._load_binary_score_mask(forecast_path)
        observed = self.helper._load_binary_score_mask(target_path)
        observed_cells = int(np.count_nonzero(observed > 0))
        forecast_cells = int(np.count_nonzero(forecast > 0))
        notes = [note]
        observation_map = self._observation_record_map()
        target_record = observation_map.get(pair.target_mask_id, {})
        source_mask_ids = str(target_record.get("source_mask_ids") or pair.target_mask_id)
        notes.append(f"seed_mask_id={pair.seed_mask_id}; target_source_mask_ids={source_mask_ids}")
        if observed_cells <= 0:
            raise RuntimeError(f"Target mask for {pair.pair_id} has zero positive cells: {target_path}")
        if forecast_cells <= 0:
            notes.append("warning: zero forecast positive cells; retained as explicit zero-signal row")
        actual_elapsed_h, obs_time_offset_h = self._pair_timing_values(pair)
        selection, _recipe_source = self._resolve_recipe()
        fss_values: dict[int, float] = {}
        for window_km in OFFICIAL_PHASE3B_WINDOWS_KM:
            fss_values[int(window_km)] = float(
                np.clip(
                    calculate_fss(
                        forecast,
                        observed,
                        window=self.helper._window_km_to_cells(int(window_km)),
                        valid_mask=self.valid_mask,
                    ),
                    0.0,
                    1.0,
                )
            )
        diagnostics = self.helper._compute_mask_diagnostics(forecast, observed)
        fss_row = {
            "experiment_id": EXPERIMENT_ID,
            "pair_id": pair.pair_id,
            "seed_mask_id": pair.seed_mask_id,
            "target_mask_id": pair.target_mask_id,
            "nominal_lead_h": int(pair.nominal_lead_h),
            "actual_elapsed_h": actual_elapsed_h,
            "obs_time_offset_h": obs_time_offset_h,
            "model_surface": surface_id,
            "forcing_recipe_id": selection.recipe,
            "forcing_recipe_source": selection.source_path,
            "phase1_rerun": False,
            "element_cap": ELEMENT_CAP,
            "fss_1km": fss_values[1],
            "fss_3km": fss_values[3],
            "fss_5km": fss_values[5],
            "fss_10km": fss_values[10],
            "mean_fss": float(np.mean([fss_values[1], fss_values[3], fss_values[5], fss_values[10]])),
            "notes": " | ".join(text for text in notes if text),
        }
        diag_row = {
            "experiment_id": EXPERIMENT_ID,
            "pair_id": pair.pair_id,
            "model_surface": surface_id,
            "forecast_cells": forecast_cells,
            "observed_cells": observed_cells,
            "forecast_area_km2": forecast_cells * self.cell_area_km2,
            "observed_area_km2": observed_cells * self.cell_area_km2,
            "area_ratio": diagnostics["area_ratio_forecast_to_obs"],
            "iou": diagnostics["iou"],
            "dice": diagnostics["dice"],
            "nearest_distance_m": diagnostics["nearest_distance_to_obs_m"],
            "centroid_distance_m": diagnostics["centroid_distance_m"],
            "notes": " | ".join(text for text in notes if text),
        }
        return fss_row, diag_row

    def _write_run_config_resolved(self, selection, recipe_source: str, forcing_paths: dict[str, Any]) -> Path:
        payload = {
            "experiment_id": EXPERIMENT_ID,
            "output_root": str(EXPERIMENT_OUTPUT_DIR),
            "config_path": str(CONFIG_PATH),
            "workflow_mode": self.case.workflow_mode,
            "run_name": self.case.run_name,
            "safe_default": False,
            "experimental_only": True,
            "reportable": False,
            "thesis_facing": False,
            "phase1_enabled": False,
            "phase1_rerun": False,
            "drifter_ingestion_enabled": False,
            "gfs_historical_preflight_enabled": False,
            "max_elements_per_run_or_member": ELEMENT_CAP,
            "claim_boundary": CLAIM_BOUNDARY,
            "recipe": {
                "recipe": selection.recipe,
                "source_kind": selection.source_kind,
                "source_path": selection.source_path,
                "note": selection.note,
                "recipe_source": recipe_source,
            },
            "transport": {
                "opendrift_primary_model": "oceandrift",
                "ensemble_members": EXPECTED_ENSEMBLE_MEMBER_COUNT,
                "deterministic_element_cap": ELEMENT_CAP,
                "ensemble_member_element_cap": ELEMENT_CAP,
                "pygnome_particle_cap": ELEMENT_CAP,
                "mask_p50": "probability >= 0.50",
                "mask_p90": "probability >= 0.90",
                "retention_overrides": self._selected_retention_overrides(),
            },
            "forcing_paths": {
                "currents": str(forcing_paths.get("currents", "")),
                "wind": str(forcing_paths.get("wind", "")),
                "wave": str(forcing_paths.get("wave", "")),
            },
            "forecast_pairs": [asdict(pair) for pair in FORECAST_PAIRS],
            "observation_sources": [asdict(source) for source in OBS_SOURCES],
        }
        path = self.output_dir / "run_config_resolved.yaml"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
        return path

    @staticmethod
    def _scorecard_row_count(scorecard_path: Path) -> int:
        if not scorecard_path.exists():
            return 0
        try:
            return int(len(pd.read_csv(scorecard_path).index))
        except Exception:
            return 0

    def _prior_real_run_started_utc(self) -> str:
        payload = _read_json(self.output_dir / "real_run_manifest.json")
        return str(payload.get("run_started_utc") or _now_utc())

    def _write_real_run_manifest(
        self,
        *,
        run_started_utc: str,
        run_status: str,
        selection,
        recipe_source: str,
        forcing_paths: dict[str, Any],
        opendrift_run_executed: bool,
        pygnome_run_executed: bool,
        scorecard_rows_written: int,
        canonical_outputs_overwritten: bool,
        notes: str,
    ) -> Path:
        staged_forcing_files = {
            "cmems_curr.nc": str(forcing_paths.get("currents", self.forcing_dir / "cmems_curr.nc")),
            "gfs_wind.nc": str(forcing_paths.get("wind", self.forcing_dir / "gfs_wind.nc")),
            "cmems_wave.nc": str(forcing_paths.get("wave", self.forcing_dir / "cmems_wave.nc")),
        }
        payload = {
            "experiment_id": EXPERIMENT_ID,
            "run_started_utc": run_started_utc,
            "run_completed_utc": _now_utc(),
            "run_status": run_status,
            "forcing_recipe_id": selection.recipe,
            "forcing_recipe_source": recipe_source,
            "staged_forcing_files": staged_forcing_files,
            "local_forcing_reused": True,
            "planned_downloads": [],
            "actual_downloads": [],
            "phase1_rerun": False,
            "drifter_ingestion": False,
            "gfs_historical_preflight": False,
            "broad_gfs_ingestion": False,
            "monthly_gfs_fetch": False,
            "recipe_ranking": False,
            "opendrift_run_executed": bool(opendrift_run_executed),
            "pygnome_run_executed": bool(pygnome_run_executed),
            "scorecard_rows_written": int(scorecard_rows_written),
            "expected_scorecard_rows": 12,
            "element_cap": ELEMENT_CAP,
            "canonical_outputs_overwritten": bool(canonical_outputs_overwritten),
            "archive_only": True,
            "experimental_only": True,
            "reportable": False,
            "thesis_facing": False,
            "does_not_replace_b1": True,
            "notes": notes,
        }
        path = self.output_dir / "real_run_manifest.json"
        _write_json(path, payload)
        return path

    def _write_manifest(
        self,
        *,
        stage: str,
        observation_records: list[dict[str, Any]],
        forecast_records: list[dict[str, Any]],
        selection,
        recipe_source: str,
        forcing_paths: dict[str, Any],
        scorecard_path: Path,
        diagnostics_path: Path,
        pygnome_manifest: dict[str, Any] | None = None,
        readme_path: Path | None = None,
    ) -> Path:
        payload = {
            "generated_at_utc": _now_utc(),
            "experiment_id": EXPERIMENT_ID,
            "stage": stage,
            "output_root": str(EXPERIMENT_OUTPUT_DIR),
            "config_path": str(CONFIG_PATH),
            "docs_path": str(DOC_PATH),
            "workflow_mode": self.case.workflow_mode,
            "run_name": self.case.run_name,
            "archive_only": True,
            "experimental_only": True,
            "reportable": False,
            "thesis_facing": False,
            "safe_default": False,
            "phase1_rerun": False,
            "drifter_ingestion": False,
            "gfs_historical_preflight": False,
            "broad_gfs_ingestion": False,
            "monthly_gfs_fetch": False,
            "recipe_ranking": False,
            "model_run_executed": stage in {"pipeline", "final_with_pygnome"},
            "element_cap": ELEMENT_CAP,
            "claim_boundary": CLAIM_BOUNDARY,
            "does_not_replace_b1": True,
            "pygnome_comparator_only": True,
            "observation_records": observation_records,
            "forecast_records": forecast_records,
            "recipe": (
                {
                    "recipe": selection.recipe,
                    "source_kind": selection.source_kind,
                    "source_path": selection.source_path,
                    "note": selection.note,
                    "recipe_source": recipe_source,
                }
                if selection is not None
                else {}
            ),
            "forcing_paths": {
                "currents": str(forcing_paths.get("currents", "")),
                "wind": str(forcing_paths.get("wind", "")),
                "wave": str(forcing_paths.get("wave", "")),
            },
            "scorecard_fss_by_pair_surface": str(scorecard_path),
            "scorecard_geometry_diagnostics": str(diagnostics_path),
            "forcing_reuse_manifest": str(self.output_dir / "forcing_reuse_manifest.json"),
            "element_count_audit": str(self.output_dir / "element_count_audit.json"),
            "pygnome_manifest": pygnome_manifest or {},
            "readme": str(readme_path) if readme_path else "",
            "expected_final_fss_rows": 12,
            "protected_output_roots": [str(path) for path in PROTECTED_OUTPUT_ROOTS],
        }
        path = self.output_dir / "manifest.json"
        _write_json(path, payload)
        return path

    def _write_observation_figure(self, records: list[dict[str, Any]]) -> None:
        if plt is None or ListedColormap is None:
            return
        wanted = [
            "OBS_MAR09_TERRAMODIS",
            "OBS_MAR11_ICEYE",
            "OBS_MAR12_WORLDVIEW3_NOAA_230314",
            "OBS_MAR12_WORLDVIEW3_NOAA_230313",
            "OBS_MAR12_ICEYE",
            "OBS_MAR12_COMBINED",
        ]
        record_map = {record["mask_id"]: record for record in records}
        arrays = {mask_id: _read_raster(Path(record_map[mask_id]["mask_path"])) for mask_id in wanted if mask_id in record_map}
        crop = self._crop_window(list(arrays.values()))
        fig, axes = plt.subplots(2, 3, figsize=(12, 8), dpi=150)
        colors = ["#d97706", "#2563eb", "#7c3aed", "#16a34a", "#dc2626", "#0f172a"]
        for ax, mask_id, color in zip(axes.reshape(-1), wanted, colors):
            arr = arrays.get(mask_id)
            if arr is None:
                ax.set_axis_off()
                continue
            self._plot_single_mask(ax, arr, crop, color, mask_id)
        fig.suptitle(
            "mindoro_mar09_12_multisource_experiment: Observation Masks (archive/support only; not B1 replacement)",
            fontsize=11,
        )
        fig.tight_layout()
        fig.savefig(self.figure_dir / "fig_observation_masks_mar09_mar11_mar12_combined.png", bbox_inches="tight")
        fig.savefig(self.figure_dir / "fig_source_mask_union_QA_mar12.png", bbox_inches="tight")
        plt.close(fig)

    def write_figures(self, *, observation_only: bool = False) -> None:
        if plt is None or ListedColormap is None:
            return
        self._write_observation_figure(self._load_source_manifest().get("observation_records", []))
        if observation_only:
            return
        for pair in FORECAST_PAIRS:
            self._write_pair_figure(pair)
        self._write_fss_summary_figure()

    def _crop_window(self, arrays: list[np.ndarray]) -> tuple[int, int, int, int]:
        stack = np.zeros((self.grid.height, self.grid.width), dtype=bool)
        for arr in arrays:
            if arr is not None:
                stack |= np.asarray(arr) > 0
        rows, cols = np.where(stack)
        if rows.size == 0:
            return 0, self.grid.height, 0, self.grid.width
        pad = 8
        return (
            max(0, int(rows.min()) - pad),
            min(self.grid.height, int(rows.max()) + pad + 1),
            max(0, int(cols.min()) - pad),
            min(self.grid.width, int(cols.max()) + pad + 1),
        )

    def _plot_single_mask(self, ax, arr: np.ndarray, crop: tuple[int, int, int, int], color: str, title: str) -> None:
        r0, r1, c0, c1 = crop
        cropped = arr[r0:r1, c0:c1]
        ax.imshow(np.zeros_like(cropped), cmap=ListedColormap(["#eef6ff"]), interpolation="nearest")
        ax.imshow(np.ma.masked_where(cropped <= 0, cropped), cmap=ListedColormap([color]), alpha=0.85, interpolation="nearest")
        ax.set_title(title, fontsize=8)
        ax.set_xticks([])
        ax.set_yticks([])

    def _write_pair_figure(self, pair: ForecastPairSpec) -> None:
        observation_map = self._observation_record_map()
        arrays = {
            "Seed": _read_raster(Path(observation_map[pair.seed_mask_id]["mask_path"])),
            "Target": _read_raster(Path(observation_map[pair.target_mask_id]["mask_path"])),
            "OpenDrift deterministic": _read_raster(self.forecast_dir / pair.pair_id / "products" / "opendrift_deterministic_footprint.tif"),
            "OpenDrift p50": _read_raster(self.forecast_dir / pair.pair_id / "products" / "opendrift_mask_p50.tif"),
            "OpenDrift p90": _read_raster(self.forecast_dir / pair.pair_id / "products" / "opendrift_mask_p90.tif"),
            "PyGNOME comparator": _read_raster(self.pygnome_dir / pair.pair_id / "products" / "pygnome_deterministic_comparator_footprint.tif"),
        }
        crop = self._crop_window(list(arrays.values()))
        fig, axes = plt.subplots(2, 3, figsize=(12, 8), dpi=150)
        colors = ["#d97706", "#2563eb", "#dc2626", "#16a34a", "#7c3aed", "#111827"]
        for ax, (title, arr), color in zip(axes.reshape(-1), arrays.items(), colors):
            self._plot_single_mask(ax, arr, crop, color, title)
        fig.suptitle(
            f"mindoro_mar09_12_multisource_experiment {pair.pair_id}: archive/support only, not B1 replacement",
            fontsize=11,
        )
        fig.tight_layout()
        name_map = {
            "E1_MAR09_TO_MAR11_48H": "fig_forecast_pair_E1_mar09_to_mar11_48h.png",
            "E2_MAR09_TO_MAR12_72H": "fig_forecast_pair_E2_mar09_to_mar12_72h.png",
            "E3_MAR11_TO_MAR12_24H": "fig_forecast_pair_E3_mar11_to_mar12_24h.png",
        }
        fig.savefig(self.figure_dir / name_map[pair.pair_id], bbox_inches="tight")
        plt.close(fig)

    def _write_fss_summary_figure(self) -> None:
        scorecard_path = self.output_dir / "scorecard_fss_by_pair_surface.csv"
        if not scorecard_path.exists():
            return
        scorecard = pd.read_csv(scorecard_path)
        fig, ax = plt.subplots(figsize=(11, 5), dpi=150)
        pivot = scorecard.pivot(index="pair_id", columns="model_surface", values="mean_fss")
        pivot.plot(kind="bar", ax=ax, width=0.82)
        ax.set_ylabel("Mean FSS")
        ax.set_xlabel("")
        ax.set_ylim(0, max(1.0, float(np.nanmax(pivot.to_numpy(dtype=float))) if not pivot.empty else 1.0))
        ax.set_title("mindoro_mar09_12_multisource_experiment Mean FSS (archive/support only)")
        ax.legend(fontsize=8, loc="upper right")
        fig.tight_layout()
        fig.savefig(self.figure_dir / "fig_fss_summary_by_pair_surface.png", bbox_inches="tight")
        plt.close(fig)

    def write_readme(self) -> Path:
        scorecard = pd.read_csv(self.output_dir / "scorecard_fss_by_pair_surface.csv")
        diagnostics = pd.read_csv(self.output_dir / "scorecard_geometry_diagnostics.csv")
        lines = [
            "# Mindoro March 9-12 Multi-Source Experiment",
            "",
            "**Status:** archive/support-only experimental real run completed.",
            "",
            "This experiment is not the current thesis-facing B1 validation claim and does not replace B1. It reuses the existing B1 forcing-recipe winner, `cmems_gfs`, with already staged local March 9-12 case forcing.",
            "",
            "No new Phase 1 was run. No drifter ingestion was run. No recipe ranking was run. No GFS historical preflight or broad/monthly GFS ingestion was run. No downloads occurred during the real run.",
            "",
            f"OpenDrift deterministic runs and each ensemble member are capped at {ELEMENT_CAP:,} elements. PyGNOME is capped at {ELEMENT_CAP:,} particles when the comparator implementation exposes that control.",
            "",
            "PyGNOME rows are comparator-only and are never observational truth. March 12 validation uses the combined union/dissolved mask from the three preserved March 12 source masks.",
            "",
            "## FSS",
            "",
            "| pair_id | nominal_lead_h | model_surface | fss_1km | fss_3km | fss_5km | fss_10km | mean_fss |",
            "|---|---:|---|---:|---:|---:|---:|---:|",
        ]
        for _, row in scorecard.sort_values(["pair_id", "model_surface"]).iterrows():
            lines.append(
                f"| {row['pair_id']} | {int(row['nominal_lead_h'])} | {row['model_surface']} | "
                f"{float(row['fss_1km']):.4f} | {float(row['fss_3km']):.4f} | "
                f"{float(row['fss_5km']):.4f} | {float(row['fss_10km']):.4f} | "
                f"{float(row['mean_fss']):.4f} |"
            )
        lines.extend(
            [
                "",
                "## Diagnostics",
                "",
                "| pair_id | model_surface | forecast_area_km2 | observed_area_km2 | area_ratio | IoU | Dice | nearest_distance_m | centroid_distance_m |",
                "|---|---|---:|---:|---:|---:|---:|---:|---:|",
            ]
        )
        for _, row in diagnostics.sort_values(["pair_id", "model_surface"]).iterrows():
            lines.append(
                f"| {row['pair_id']} | {row['model_surface']} | "
                f"{float(row['forecast_area_km2']):.2f} | {float(row['observed_area_km2']):.2f} | "
                f"{float(row['area_ratio']):.4f} | {float(row['iou']):.4f} | {float(row['dice']):.4f} | "
                f"{float(row['nearest_distance_m']):.1f} | {float(row['centroid_distance_m']):.1f} |"
            )
        lines.extend(
            [
                "",
                "## Boundaries",
                "",
                "- This lane writes only under `output/CASE_MINDORO_RETRO_2023/experiments/mar09_11_12_multisource/`.",
                "- Existing canonical B1, Track A, DWH, final validation package, publication figures, and read-only UI outputs are not overwritten.",
                "- Observation masks are vector-first FeatureServer products where available; report PNGs are retained as provenance and visual QA.",
                "- March 12 combined is the union/dissolve of the individual March 12 source masks after reprojection to the scoring CRS.",
            ]
        )
        path = self.output_dir / "README.md"
        _write_text(path, "\n".join(lines) + "\n")
        return path

    def write_blocked_readme(self, dry_run: dict[str, Any]) -> Path:
        missing = dry_run.get("missing_forcing_files") or []
        lines = [
            "# Mindoro March 9-12 Multi-Source Experiment",
            "",
            "**Status:** experimental/archive-only dry-run blocked by missing local forcing.",
            "",
            "This experiment does not replace B1. It reuses the existing March 13-14 B1 forcing-recipe winner, and no new Phase 1 or drifter-based ranking of recipes was run.",
            "",
            "Any GFS use is inherited from the resolved recipe and does not imply a new historical wind-ingestion workflow. The default policy is local/case forcing reuse only.",
            "",
            f"OpenDrift deterministic runs and each ensemble member are capped at {ELEMENT_CAP:,} elements. PyGNOME is comparator-only and capped at {ELEMENT_CAP:,} particles when supported.",
            "",
            "## Dry-Run Gate",
            "",
            f"- Resolved forcing recipe: `{dry_run.get('resolved_forcing_recipe_id', '')}`",
            f"- Recipe source: `{dry_run.get('recipe_source_file_or_manifest_path', '')}`",
            f"- Current provider: `{dry_run.get('current_provider', '')}`",
            f"- Wind provider: `{dry_run.get('wind_provider', '')}`",
            "- Phase 1 enabled: `false`",
            "- Drifter ingestion enabled: `false`",
            "- `gfs_historical_preflight_enabled`: `false`",
            "- Planned downloads: none by default",
            "",
            "## Missing Forcing",
            "",
        ]
        if missing:
            for row in missing:
                lines.append(f"- `{row.get('forcing_kind', '')}` / `{row.get('filename', '')}`")
        else:
            lines.append("- None")
        path = self.output_dir / "README.md"
        _write_text(path, "\n".join(lines) + "\n")
        return path

    def write_ready_dry_run_readme(self, dry_run: dict[str, Any]) -> Path:
        coverage = dry_run.get("forcing_coverage") or {}
        forcing_paths = dry_run.get("forcing_paths") or {}
        lines = [
            "# Mindoro March 9-12 Multi-Source Experiment",
            "",
            "**Status:** experimental/archive-only dry-run ready to run. No model run has been executed.",
            "",
            "This experiment does not replace B1. It reuses the existing March 13-14 B1 forcing-recipe winner, and no new Phase 1 or drifter-based ranking of recipes was run.",
            "",
            "Any GFS use is inherited from the resolved recipe and does not imply a new historical wind-ingestion workflow. The final dry-run uses staged local/case forcing only.",
            "",
            f"OpenDrift deterministic runs and each ensemble member are capped at {ELEMENT_CAP:,} elements. PyGNOME is comparator-only and capped at {ELEMENT_CAP:,} particles when supported.",
            "",
            "## Dry-Run Gate",
            "",
            f"- Dry-run status: `{dry_run.get('dry_run_status', dry_run.get('status', ''))}`",
            f"- Resolved forcing recipe: `{dry_run.get('resolved_forcing_recipe_id', '')}`",
            f"- Recipe source: `{dry_run.get('recipe_source_file_or_manifest_path', '')}`",
            f"- Current provider: `{dry_run.get('current_provider', '')}`",
            f"- Wind provider: `{dry_run.get('wind_provider', '')}`",
            f"- Wave provider: `{dry_run.get('wave_provider', '')}`",
            "- Phase 1 enabled: `false`",
            "- Drifter ingestion enabled: `false`",
            "- `gfs_historical_preflight_enabled`: `false`",
            "- `broad_gfs_ingestion_enabled`: `false`",
            "- `monthly_gfs_fetch_enabled`: `false`",
            "- `recipe_ranking_enabled`: `false`",
            "- Planned downloads: `[]`",
            "- Actual downloads: `[]`",
            f"- Model run executed: `{str(dry_run.get('model_run_executed', False)).lower()}`",
            f"- Real-run gate enabled: `{str(dry_run.get('run_gate_enabled', False)).lower()}`",
            f"- Expected forecast pairs: `{dry_run.get('expected_forecast_pairs', '')}`",
            f"- Expected model surfaces: `{dry_run.get('expected_model_surfaces', '')}`",
            f"- Expected FSS rows: `{dry_run.get('expected_scorecard_rows', '')}`",
            "",
            "## Staged Forcing",
            "",
            "| Logical file | Path | Coverage start | Coverage end | Reader |",
            "|---|---|---:|---:|---|",
        ]
        for logical_name in ["cmems_curr.nc", "gfs_wind.nc", "cmems_wave.nc"]:
            row = coverage.get(logical_name) or {}
            lines.append(
                f"| `{logical_name}` | `{forcing_paths.get(logical_name, '')}` | "
                f"{row.get('start_utc', '')} | {row.get('end_utc', '')} | "
                f"{row.get('reader_compatibility_status', '')} |"
            )
        lines.extend(
            [
                "",
                "## Boundaries",
                "",
                "- This lane writes only under `output/CASE_MINDORO_RETRO_2023/experiments/mar09_11_12_multisource/`.",
                "- Existing canonical B1, Track A, DWH, final validation package, publication figures, and read-only UI outputs are not overwritten.",
                "- Observation masks remain vector-first FeatureServer products where available; report PNGs are retained as provenance and visual QA.",
                "- March 12 combined is the union/dissolve of the individual March 12 source masks after reprojection to the scoring CRS.",
            ]
        )
        path = self.output_dir / "README.md"
        _write_text(path, "\n".join(lines) + "\n")
        return path


def run_mindoro_mar09_12_multisource_experiment() -> dict[str, Any]:
    return MindoroMar0912MultisourceExperimentService().run_pipeline()


def run_mindoro_mar09_12_multisource_experiment_dry_run() -> dict[str, Any]:
    return MindoroMar0912MultisourceExperimentService().run_dry_run()


def run_mindoro_mar09_12_multisource_experiment_resolve_forcing_only() -> dict[str, Any]:
    return MindoroMar0912MultisourceExperimentService().run_resolve_forcing_only()


def run_mindoro_mar09_12_multisource_experiment_ingest_masks_only() -> dict[str, Any]:
    return MindoroMar0912MultisourceExperimentService().run_ingest_masks_only()


def run_mindoro_mar09_12_multisource_experiment_pygnome() -> dict[str, Any]:
    return MindoroMar0912MultisourceExperimentService().run_pygnome_and_finalize()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--stage",
        choices=["dry-run", "resolve-forcing-only", "ingest-masks-only", "pipeline", "pygnome"],
        default="pipeline",
    )
    args = parser.parse_args(argv)
    if args.stage == "dry-run":
        result = run_mindoro_mar09_12_multisource_experiment_dry_run()
    elif args.stage == "resolve-forcing-only":
        result = run_mindoro_mar09_12_multisource_experiment_resolve_forcing_only()
    elif args.stage == "ingest-masks-only":
        result = run_mindoro_mar09_12_multisource_experiment_ingest_masks_only()
    elif args.stage == "pipeline":
        result = run_mindoro_mar09_12_multisource_experiment()
    else:
        result = run_mindoro_mar09_12_multisource_experiment_pygnome()
    print(json.dumps(result, indent=2, default=_json_default))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
