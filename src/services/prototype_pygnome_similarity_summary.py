"""Read-only prototype transport-only PyGNOME similarity summary."""

from __future__ import annotations

import json
import os
import textwrap
from pathlib import Path
from typing import Any

import geopandas as gpd
import matplotlib
import numpy as np
import pandas as pd
import rasterio
import xarray as xr
import yaml
import cartopy.crs as ccrs
from rasterio import features as raster_features
from shapely.geometry import shape as shapely_shape

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.colors import LinearSegmentedColormap, to_rgba
from matplotlib.lines import Line2D
from matplotlib.patches import Patch, Polygon, Rectangle

from src.core.artifact_status import artifact_status_columns, status_for_track_id
from src.core.domain_semantics import resolve_legacy_prototype_display_domain
from src.helpers.plotting import (
    add_prototype_2016_geoaxes,
    bounds_from_point,
    bounds_from_points,
    derive_prototype_2016_figure_bounds,
    figure_relative_inset_rect,
    merge_case_display_bounds,
    normalize_prototype_2016_extent_mode,
    PROTOTYPE_2016_EXTENT_MODE_DYNAMIC_FORECAST,
    PROTOTYPE_2016_EXTENT_MODE_FIXED_REGIONAL,
    prototype_2016_rendering_metadata,
)
from src.helpers.metrics import calculate_fss, calculate_kl_divergence
from src.utils.io import load_drifter_data, select_drifter_of_record

PHASE = "prototype_pygnome_similarity_summary"
DEFAULT_OUTPUT_DIRS = {
    "prototype_2016": Path("output") / "prototype_2016_pygnome_similarity",
    "prototype_2021": Path("output") / "prototype_2021_pygnome_similarity",
}
FIGURES_DIRNAME = "figures"
REQUIRED_WINDOWS_KM = (1, 3, 5, 10)
REQUIRED_HOURS = (24, 48, 72)
REQUIRED_BENCHMARK_FILES = {
    "summary_csv": "phase3a_summary.csv",
    "fss_csv": "phase3a_fss_by_time_window.csv",
    "kl_csv": "phase3a_kl_by_time.csv",
    "pairing_csv": "phase3a_pairing_manifest.csv",
    "metadata_json": "pygnome/pygnome_benchmark_metadata.json",
}
REQUIRED_PAIRING_COLUMNS = (
    "timestamp_utc",
    "hour",
    "pygnome_footprint_path",
    "qa_overlay_path",
)
DOMAIN_BOUNDS = (115.0, 122.0, 6.0, 14.5)
CASE_LOCAL_SUPPORT_CONTEXT_MODE = "case_local_drifter_track"
SINGLE_FIGURE_SIZE = (8.4, 12.2)
BOARD_FIGURE_SIZE = (13.4, 10.8)
FIGURE_DPI = 180
PROTOTYPE_CROP_PAD_CELLS = 2
PROTOTYPE_MIN_CROP_CELLS_X = 6
PROTOTYPE_MIN_CROP_CELLS_Y = 8
PROTOTYPE_CELL_EDGE_COLOR = "#f8fafc"
PROTOTYPE_CELL_EDGE_WIDTH = 0.9
PROTOTYPE_OUTLINE_WIDTH = 1.5
PROTOTYPE_DENSITY_GRID_MIN = 220
PROTOTYPE_DENSITY_GRID_MAX = 360
PROTOTYPE_DENSITY_LEVELS = (0.10, 0.22, 0.38, 0.58, 0.80, 1.01)
PROTOTYPE_LOCATOR_PADDING_FRACTION = 0.22
MAP_WATER_COLOR = "#e8f8fc"
MAP_LAND_COLOR = "#d0d0d0"
MAP_SHORELINE_COLOR = "#111827"
SOURCE_POINT_COLOR = "#1d9b1d"
MODEL_STYLES = {
    "opendrift": {
        "color": "#165ba8",
        "mid_color": "#4f88c5",
        "light_color": "#9fc1e6",
        "label": "OpenDrift deterministic",
        "short_label": "OpenDrift",
    },
    "opendrift_p50": {
        "color": "#0f766e",
        "mid_color": "#1f9d8f",
        "light_color": "#8bd8cd",
        "label": "OpenDrift p50 threshold",
        "short_label": "OpenDrift p50",
    },
    "opendrift_p90": {
        "color": "#9a3412",
        "mid_color": "#c85a2b",
        "light_color": "#efb08f",
        "label": "OpenDrift p90 threshold",
        "short_label": "OpenDrift p90",
    },
    "pygnome": {
        "color": "#6b21a8",
        "mid_color": "#8b5bc7",
        "light_color": "#ccb4e6",
        "label": "PyGNOME deterministic",
        "short_label": "PyGNOME",
    },
}
COMPARISON_TRACK_LABELS = {
    "deterministic": "OpenDrift deterministic",
    "ensemble_p50": "OpenDrift p50 threshold",
    "ensemble_p90": "OpenDrift p90 threshold",
}
COMPARISON_TRACK_SUBDIRS = {
    "deterministic": "control",
    "ensemble_p50": "ensemble_p50",
    "ensemble_p90": "ensemble_p90",
}
MODEL_NAME_TO_TRACK_ID = {
    "opendrift": "deterministic",
    "opendrift_p50": "ensemble_p50",
    "opendrift_p90": "ensemble_p90",
}


def _write_csv(path: Path, rows: list[dict[str, Any]], columns: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(rows)
    if columns is not None:
        for column in columns:
            if column not in df.columns:
                df[column] = ""
        df = df[columns]
    df.to_csv(path, index=False)


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, default=str)
        handle.write("\n")


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.rstrip() + "\n", encoding="utf-8")


def _coerce_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _utc_now_iso() -> str:
    return pd.Timestamp.now(tz="UTC").isoformat()


def _safe_token(value: Any) -> str:
    text = str(value or "").strip().lower()
    chars = [char if char.isalnum() else "_" for char in text]
    normalized = "".join(chars)
    while "__" in normalized:
        normalized = normalized.replace("__", "_")
    return normalized.strip("_")


def _geometry_exterior_rings(geometry: dict[str, Any]) -> list[list[tuple[float, float]]]:
    geometry_type = str(geometry.get("type") or "")
    coordinates = geometry.get("coordinates") or []
    if geometry_type == "Polygon":
        if not coordinates:
            return []
        return [[(float(x), float(y)) for x, y in coordinates[0]]]
    if geometry_type == "MultiPolygon":
        rings: list[list[tuple[float, float]]] = []
        for polygon_coordinates in coordinates:
            if polygon_coordinates:
                rings.append([(float(x), float(y)) for x, y in polygon_coordinates[0]])
        return rings
    return []


class PrototypePygnomeSimilaritySummaryService:
    def __init__(
        self,
        repo_root: str | Path = ".",
        output_dir: str | Path | None = None,
        prototype_case_dates: tuple[str, ...] | list[str] | None = None,
        workflow_mode: str | None = None,
    ):
        self.repo_root = Path(repo_root).resolve()
        settings = self._load_settings()
        self.workflow_mode = str(
            workflow_mode or os.environ.get("WORKFLOW_MODE") or settings.get("workflow_mode") or "prototype_2016"
        ).strip()
        workflow_config = self._resolve_workflow_config(settings, prototype_case_dates)
        default_output_dir = DEFAULT_OUTPUT_DIRS.get(self.workflow_mode, DEFAULT_OUTPUT_DIRS["prototype_2016"])
        self.output_dir = Path(output_dir) if output_dir else self.repo_root / workflow_config.get("output_dir", default_output_dir)
        self.figures_dir = self.output_dir / FIGURES_DIRNAME
        self.prototype_case_dates = tuple(workflow_config.get("prototype_case_dates") or ())
        self.case_ids = [str(case_id) for case_id in workflow_config.get("case_ids") or ()]
        if not self.case_ids:
            raise ValueError("Prototype PyGNOME similarity summary requires at least one configured case.")
        self.domain_bounds = tuple(float(value) for value in workflow_config.get("domain_bounds") or DOMAIN_BOUNDS)
        default_context_mode = CASE_LOCAL_SUPPORT_CONTEXT_MODE if self.workflow_mode == "prototype_2016" else "mindoro_canonical"
        self.support_context_mode = str(workflow_config.get("support_context_mode") or default_context_mode)
        self.case_metadata_by_id = {
            str(case_id): dict(payload or {})
            for case_id, payload in (workflow_config.get("case_metadata_by_id") or {}).items()
        }
        self._raster_cache: dict[Path, dict[str, Any]] = {}
        self._vector_cache: dict[tuple[Path, str], gpd.GeoDataFrame | None] = {}
        self._prototype_map_context: dict[str, Any] | None = None
        self._sea_mask_cache: np.ndarray | None = None
        self.extent_mode = (
            PROTOTYPE_2016_EXTENT_MODE_DYNAMIC_FORECAST
            if self.workflow_mode == "prototype_2016"
            else PROTOTYPE_2016_EXTENT_MODE_FIXED_REGIONAL
        )

    def _load_settings(self) -> dict[str, Any]:
        settings_path = self.repo_root / "config" / "settings.yaml"
        if not settings_path.exists():
            return {}
        with open(settings_path, "r", encoding="utf-8") as handle:
            return yaml.safe_load(handle) or {}

    def _resolve_workflow_config(
        self,
        settings: dict[str, Any],
        prototype_case_dates: tuple[str, ...] | list[str] | None,
    ) -> dict[str, Any]:
        if self.workflow_mode == "prototype_2021":
            case_files = settings.get("workflow_case_files") or {}
            case_path = self.repo_root / str(case_files.get("prototype_2021") or "config/prototype_2021_cases.yaml")
            if not case_path.exists():
                raise FileNotFoundError(f"prototype_2021 case definition file not found: {case_path}")
            with open(case_path, "r", encoding="utf-8") as handle:
                cfg = yaml.safe_load(handle) or {}
            cases = [dict(item or {}) for item in cfg.get("cases") or []]
            case_ids = [str(item.get("case_id") or "").strip() for item in cases if str(item.get("case_id") or "").strip()]
            if not case_ids:
                raise ValueError(f"prototype_2021 requires at least one configured case in {case_path}.")
            return {
                "output_dir": Path(str(cfg.get("similarity_output_root") or DEFAULT_OUTPUT_DIRS["prototype_2021"])),
                "prototype_case_dates": (),
                "case_ids": tuple(case_ids),
                "domain_bounds": tuple(resolve_legacy_prototype_display_domain(cfg, settings)),
                "support_context_mode": str(cfg.get("support_context_mode") or "neutral_case_local"),
                "case_metadata_by_id": {str(item["case_id"]): item for item in cases},
            }

        prototype_dates = tuple(prototype_case_dates or self._load_prototype_case_dates())
        if not prototype_dates:
            raise ValueError("Prototype PyGNOME similarity summary requires at least one configured prototype case date.")
        case_ids = [date if str(date).startswith("CASE_") else f"CASE_{date}" for date in prototype_dates]
        return {
            "output_dir": DEFAULT_OUTPUT_DIRS["prototype_2016"],
            "prototype_case_dates": prototype_dates,
            "case_ids": tuple(case_ids),
            "domain_bounds": tuple(resolve_legacy_prototype_display_domain(settings)),
            "support_context_mode": CASE_LOCAL_SUPPORT_CONTEXT_MODE,
            "case_metadata_by_id": {},
        }

    def _workflow_label(self) -> str:
        return "Prototype 2021" if self.workflow_mode == "prototype_2021" else "Prototype 2016"

    def _workflow_token(self) -> str:
        return _safe_token(self.workflow_mode)

    def _context_phrase(self) -> str:
        if self.support_context_mode == "mindoro_canonical":
            return "canonical Mindoro land/shoreline context"
        if self.support_context_mode == CASE_LOCAL_SUPPORT_CONTEXT_MODE:
            return "case-local drifter-centered geographic context"
        return "neutral case-local geographic context"

    def _support_status_phrase(self) -> str:
        if self.workflow_mode == "prototype_2021":
            return "accepted-segment debug support only"
        return "legacy/debug support only"

    def _context_note_sentence(self) -> str:
        if self.support_context_mode == "mindoro_canonical":
            return "Map context reuses the stored canonical Mindoro scoring-grid assets."
        if self.support_context_mode == CASE_LOCAL_SUPPORT_CONTEXT_MODE:
            return "Map context is case-local and drifter-centered, so the legacy 2016 support lane is no longer framed on the old Mindoro prototype domain."
        return "Map context is neutral/case-local because this preferred 2021 debug lane is not tied to the old Mindoro prototype geography."

    def _comparison_track_ids(self) -> tuple[str, ...]:
        if self.workflow_mode == "prototype_2016":
            return ("deterministic", "ensemble_p50", "ensemble_p90")
        return ("deterministic",)

    def _single_model_sequence(self) -> tuple[str, ...]:
        if self.workflow_mode == "prototype_2016":
            return ("opendrift", "opendrift_p50", "opendrift_p90", "pygnome")
        return ("opendrift", "pygnome")

    def _load_prototype_case_dates(self) -> tuple[str, ...]:
        settings_path = self.repo_root / "config" / "settings.yaml"
        if not settings_path.exists():
            raise FileNotFoundError(f"Prototype settings file not found: {settings_path}")
        with open(settings_path, "r", encoding="utf-8") as handle:
            settings = yaml.safe_load(handle) or {}
        raw = settings.get("phase_1_start_date")
        if isinstance(raw, list):
            return tuple(str(item) for item in raw)
        if raw is None:
            return ()
        return (str(raw),)

    def _case_benchmark_dir(self, case_id: str) -> Path:
        return self.repo_root / "output" / case_id / "benchmark"

    def _case_grid_metadata_path(self, case_id: str) -> Path:
        return self._case_benchmark_dir(case_id) / "grid" / "grid.json"

    def _load_case_display_bounds(self, case_id: str) -> tuple[float, float, float, float]:
        ensemble_metadata_path = self.repo_root / "output" / case_id / "ensemble" / "metadata.json"
        if ensemble_metadata_path.exists():
            payload = json.loads(ensemble_metadata_path.read_text(encoding="utf-8")) or {}
            display_bounds = (
                payload.get("display_bounds_wgs84")
                or (payload.get("figure_rendering") or {}).get("display_bounds_wgs84")
                or []
            )
            if len(display_bounds) == 4:
                return tuple(float(value) for value in display_bounds)
        grid_path = self._case_grid_metadata_path(case_id)
        if grid_path.exists():
            payload = json.loads(grid_path.read_text(encoding="utf-8")) or {}
            display_bounds = payload.get("display_bounds_wgs84") or payload.get("extent") or []
            if len(display_bounds) == 4:
                return tuple(float(value) for value in display_bounds)
        return tuple(float(value) for value in self.domain_bounds)

    def _load_prototype_map_context(self) -> dict[str, Any]:
        if self._prototype_map_context is not None:
            return self._prototype_map_context

        if self.support_context_mode != "mindoro_canonical":
            case_bounds = [self._load_case_display_bounds(case_id) for case_id in self.case_ids]
            full_bounds = merge_case_display_bounds(case_bounds, halo_degrees=2.0, minimum_span_degrees=8.0)
            self._prototype_map_context = {
                "land_mask_path": None,
                "shoreline_path": None,
                "labels_path": None,
                "labels_df": pd.DataFrame(columns=["label_text", "lon", "lat", "enabled_yes_no"]),
                "full_bounds_wgs84": tuple(float(value) for value in full_bounds),
            }
            return self._prototype_map_context

        grid_json = self.repo_root / "data_processed" / "grids" / "scoring_grid.json"
        grid_yaml = self.repo_root / "data_processed" / "grids" / "scoring_grid.yaml"
        grid_payload: dict[str, Any] = {}
        if grid_json.exists():
            grid_payload = json.loads(grid_json.read_text(encoding="utf-8")) or {}
        elif grid_yaml.exists():
            grid_payload = yaml.safe_load(grid_yaml.read_text(encoding="utf-8")) or {}
        if not grid_payload:
            raise FileNotFoundError(
                "Prototype support figures in canonical Mindoro mode require scoring-grid metadata at "
                f"{grid_json} or {grid_yaml}."
            )

        display_bounds = grid_payload.get("display_bounds_wgs84") or []
        if len(display_bounds) != 4:
            raise ValueError("Prototype support figures in canonical Mindoro mode require display_bounds_wgs84 in scoring-grid metadata.")

        land_mask_path = self._resolve_repo_path(grid_payload.get("land_mask_path") or "data_processed/grids/land_mask.tif")
        shoreline_path = self._resolve_repo_path(
            grid_payload.get("shoreline_segments_path") or "data_processed/grids/shoreline_segments.gpkg"
        )
        labels_path = self.repo_root / "config" / "publication_map_labels_mindoro.csv"
        for path in (land_mask_path, shoreline_path, labels_path):
            if not Path(path).exists():
                raise FileNotFoundError(f"Prototype support figures in canonical Mindoro mode require context asset: {path}")

        labels_df = pd.read_csv(labels_path)
        self._prototype_map_context = {
            "land_mask_path": land_mask_path,
            "shoreline_path": shoreline_path,
            "labels_path": labels_path.resolve(),
            "labels_df": labels_df,
            "full_bounds_wgs84": tuple(float(value) for value in display_bounds),
        }
        return self._prototype_map_context

    def _required_paths(self, case_id: str) -> dict[str, Path]:
        base_dir = self._case_benchmark_dir(case_id)
        return {
            artifact_id: base_dir / relative_path
            for artifact_id, relative_path in REQUIRED_BENCHMARK_FILES.items()
        }

    def _resolve_repo_path(self, value: str | Path) -> Path:
        if pd.isna(value):
            raise ValueError("Prototype benchmark manifest contained a missing path value where a concrete artifact path was required.")
        path = Path(value)
        if not path.is_absolute():
            path = self.repo_root / path
        return path.resolve()

    def _has_path_value(self, value: Any) -> bool:
        if value is None or pd.isna(value):
            return False
        return bool(str(value).strip())

    def _clean_optional_path_value(self, value: Any) -> str:
        if not self._has_path_value(value):
            return ""
        return str(value).strip()

    def _load_vector(self, path: Path, target_crs: str = "EPSG:4326") -> gpd.GeoDataFrame | None:
        cache_key = (path.resolve(), str(target_crs))
        if cache_key in self._vector_cache:
            return self._vector_cache[cache_key]
        if not path.exists():
            self._vector_cache[cache_key] = None
            return None
        gdf = gpd.read_file(path)
        if gdf.empty:
            self._vector_cache[cache_key] = gdf
            return gdf
        if str(gdf.crs) != str(target_crs):
            gdf = gdf.to_crs(target_crs)
        self._vector_cache[cache_key] = gdf
        return gdf

    def _load_point_from_vector(self, path: Path, target_crs: str = "EPSG:4326") -> tuple[float, float] | None:
        gdf = self._load_vector(path, target_crs=target_crs)
        if gdf is None or gdf.empty or "geometry" not in gdf:
            return None
        series = gdf.geometry.dropna()
        if series.empty:
            return None
        point = series.iloc[0]
        if getattr(point, "geom_type", "") != "Point":
            point = point.representative_point()
        return (float(point.x), float(point.y))

    def _prototype_source_point_candidates(self, case_id: str) -> list[Path]:
        case_dir = self.repo_root / "data" / "arcgis" / case_id
        return [
            case_dir / "source_point_metadata_processed.gpkg",
            case_dir / "source_point_metadata.gpkg",
            case_dir / "source_point_metadata.geojson",
        ]

    def _prototype_drifter_csv_path(self, case_id: str) -> Path:
        return self.repo_root / "data" / "drifters" / case_id / "drifters_noaa.csv"

    def _resolve_case_source_point(self, case_id: str) -> tuple[float, float] | None:
        if self.workflow_mode == "prototype_2016":
            drifter_csv = self._prototype_drifter_csv_path(case_id)
            if drifter_csv.exists():
                try:
                    selection = select_drifter_of_record(load_drifter_data(drifter_csv))
                    return float(selection["start_lon"]), float(selection["start_lat"])
                except Exception:
                    pass
        for candidate in self._prototype_source_point_candidates(case_id):
            if not candidate.exists():
                continue
            point = self._load_point_from_vector(candidate, target_crs="EPSG:4326")
            if point is not None:
                return point
        metadata = self.case_metadata_by_id.get(case_id) or {}
        if metadata.get("start_lon") is not None and metadata.get("start_lat") is not None:
            return float(metadata["start_lon"]), float(metadata["start_lat"])
        return None

    def _load_land_polygons(self, path: Path, target_crs: str = "EPSG:4326") -> gpd.GeoDataFrame | None:
        cache_key = (path.resolve(), f"land::{target_crs}")
        if cache_key in self._vector_cache:
            return self._vector_cache[cache_key]
        if not path.exists():
            self._vector_cache[cache_key] = None
            return None
        with rasterio.open(path) as dataset:
            array = dataset.read(1)
            mask = np.isfinite(array) & (array > 0)
            if not mask.any():
                gdf = gpd.GeoDataFrame(geometry=[], crs=dataset.crs)
            else:
                geometries = [
                    shapely_shape(geometry)
                    for geometry, value in raster_features.shapes(mask.astype(np.uint8), mask=mask, transform=dataset.transform)
                    if int(value) == 1
                ]
                gdf = gpd.GeoDataFrame(geometry=geometries, crs=dataset.crs)
        if not gdf.empty and str(gdf.crs) != str(target_crs):
            gdf = gdf.to_crs(target_crs)
        self._vector_cache[cache_key] = gdf
        return gdf

    def _load_raster_mask(self, path: Path) -> dict[str, Any]:
        if path in self._raster_cache:
            return self._raster_cache[path]
        with rasterio.open(path) as dataset:
            array = dataset.read(1)
            mask = np.isfinite(array) & (array > 0)
            cell_width = float(abs(dataset.transform.a))
            cell_height = float(abs(dataset.transform.e))
            if mask.any():
                rows, cols = np.where(mask)
                lefts, tops = rasterio.transform.xy(dataset.transform, rows, cols, offset="ul")
                rights, bottoms = rasterio.transform.xy(dataset.transform, rows, cols, offset="lr")
                polygon_geometries = [
                    geometry
                    for geometry, value in raster_features.shapes(
                        mask.astype(np.uint8),
                        mask=mask,
                        transform=dataset.transform,
                    )
                    if int(value) == 1
                ]
                footprint_polygons = []
                for geometry in polygon_geometries:
                    footprint_polygons.extend(_geometry_exterior_rings(geometry))
                positive_cell_boxes = [
                    (
                        float(left),
                        float(bottom),
                        float(right),
                        float(top),
                    )
                    for left, bottom, right, top in zip(lefts, bottoms, rights, tops)
                ]
                positive_cell_bounds = (
                    float(np.min(lefts)),
                    float(np.min(bottoms)),
                    float(np.max(rights)),
                    float(np.max(tops)),
                )
            else:
                footprint_polygons = []
                positive_cell_boxes = []
                positive_cell_bounds = None
            info = {
                "path": path,
                "array": array.astype(float),
                "mask": mask,
                "bounds": tuple(dataset.bounds),
                "shape": tuple(array.shape),
                "footprint_polygons": footprint_polygons,
                "positive_cell_boxes": positive_cell_boxes,
                "positive_cell_bounds": positive_cell_bounds,
                "cell_width": cell_width,
                "cell_height": cell_height,
                "crs": str(dataset.crs),
            }
        self._raster_cache[path] = info
        return info

    def _load_similarity_sea_mask(self, reference_shape: tuple[int, int]) -> np.ndarray:
        if self._sea_mask_cache is not None and self._sea_mask_cache.shape == reference_shape:
            return self._sea_mask_cache

        candidate_paths = [
            self.repo_root / "data_processed" / "grids" / "scoring_grid_sea_mask.tif",
            self.repo_root / "data_processed" / "grids" / "sea_mask.tif",
        ]
        for candidate in candidate_paths:
            if not candidate.exists():
                continue
            try:
                with rasterio.open(candidate) as dataset:
                    mask = dataset.read(1) > 0
                if mask.shape == reference_shape:
                    self._sea_mask_cache = mask
                    return mask
            except Exception:
                continue

        self._sea_mask_cache = np.ones(reference_shape, dtype=bool)
        return self._sea_mask_cache

    def _raster_bounds_for_paths(
        self,
        *paths: Path | str | None,
    ) -> tuple[list[tuple[float, float, float, float]], list[float], list[float]]:
        bounds_sets: list[tuple[float, float, float, float]] = []
        cell_widths: list[float] = []
        cell_heights: list[float] = []
        for path in paths:
            if not path:
                continue
            candidate = Path(path)
            if not candidate.exists():
                continue
            info = self._load_raster_mask(candidate)
            positive_cell_bounds = info.get("positive_cell_bounds")
            if positive_cell_bounds is None:
                continue
            bounds_sets.append(tuple(float(value) for value in positive_cell_bounds))
            cell_widths.append(float(info.get("cell_width") or 0.0))
            cell_heights.append(float(info.get("cell_height") or 0.0))
        return bounds_sets, cell_widths, cell_heights

    def _ensure_overlay_png(self, overlay_path: Path, opendrift_hits: np.ndarray, pygnome_hits: np.ndarray) -> None:
        if overlay_path.exists():
            return
        overlay_path.parent.mkdir(parents=True, exist_ok=True)
        control = opendrift_hits > 0
        pygnome = pygnome_hits > 0
        overlay = np.zeros((*opendrift_hits.shape, 3), dtype=np.float32)
        overlay[..., 0] = (control & ~pygnome).astype(np.float32)
        overlay[..., 1] = (control & pygnome).astype(np.float32)
        overlay[..., 2] = (~control & pygnome).astype(np.float32)
        plt.imsave(overlay_path, overlay)

    def _legacy_track_product_paths(self, case_id: str, comparison_track_id: str, timestamp_utc: str) -> dict[str, Path]:
        label = pd.Timestamp(timestamp_utc).strftime("%Y-%m-%dT%H-%M-%SZ")
        benchmark_dir = self._case_benchmark_dir(case_id)
        track_dir = benchmark_dir / COMPARISON_TRACK_SUBDIRS[comparison_track_id]
        return {
            "timestamp_utc": timestamp_utc,
            "footprint": track_dir / f"{comparison_track_id}_footprint_mask_{label}.tif",
            "density": track_dir / f"{comparison_track_id}_density_norm_{label}.tif",
            "pygnome_footprint": benchmark_dir / "pygnome" / f"pygnome_footprint_mask_{label}.tif",
            "pygnome_density": benchmark_dir / "pygnome" / f"pygnome_density_norm_{label}.tif",
            "qa_overlay": benchmark_dir / "qa" / f"{comparison_track_id}_overlay_{label}.png",
        }

    def _repair_legacy_benchmark_artifacts(
        self,
        case_id: str,
        paths: dict[str, Path],
        fss_df: pd.DataFrame,
        kl_df: pd.DataFrame,
        pairing_df: pd.DataFrame,
    ) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, bool]:
        normalized_fss = fss_df.copy()
        normalized_kl = kl_df.copy()
        normalized_pairing = pairing_df.copy()
        repaired = False

        if "comparison_track_id" not in normalized_fss.columns:
            normalized_fss["comparison_track_id"] = "deterministic"
            repaired = True
        if "comparison_track_label" not in normalized_fss.columns:
            normalized_fss["comparison_track_label"] = normalized_fss["comparison_track_id"].map(COMPARISON_TRACK_LABELS).fillna(
                normalized_fss["comparison_track_id"].astype(str)
            )
            repaired = True
        if "comparison_track_id" not in normalized_kl.columns:
            normalized_kl["comparison_track_id"] = "deterministic"
            repaired = True
        if "comparison_track_label" not in normalized_kl.columns:
            normalized_kl["comparison_track_label"] = normalized_kl["comparison_track_id"].map(COMPARISON_TRACK_LABELS).fillna(
                normalized_kl["comparison_track_id"].astype(str)
            )
            repaired = True
        if "comparison_track_id" not in normalized_pairing.columns:
            normalized_pairing["comparison_track_id"] = "deterministic"
            repaired = True
        if "comparison_track_label" not in normalized_pairing.columns:
            normalized_pairing["comparison_track_label"] = normalized_pairing["comparison_track_id"].map(COMPARISON_TRACK_LABELS).fillna(
                normalized_pairing["comparison_track_id"].astype(str)
            )
            repaired = True

        deterministic_rows = normalized_pairing[
            normalized_pairing["comparison_track_id"].astype(str) == "deterministic"
        ].copy()
        if deterministic_rows.empty:
            if repaired:
                normalized_fss.to_csv(paths["fss_csv"], index=False)
                normalized_kl.to_csv(paths["kl_csv"], index=False)
                normalized_pairing.to_csv(paths["pairing_csv"], index=False)
            return normalized_fss, normalized_kl, normalized_pairing, repaired

        timestamps_by_hour = {
            int(row["hour"]): str(row["timestamp_utc"])
            for _, row in deterministic_rows.iterrows()
            if str(row.get("timestamp_utc") or "").strip()
        }

        for comparison_track_id in self._comparison_track_ids():
            for hour in REQUIRED_HOURS:
                has_fss = not normalized_fss[
                    (normalized_fss["comparison_track_id"].astype(str) == comparison_track_id)
                    & (normalized_fss["hour"].astype(int) == int(hour))
                ].empty
                has_kl = not normalized_kl[
                    (normalized_kl["comparison_track_id"].astype(str) == comparison_track_id)
                    & (normalized_kl["hour"].astype(int) == int(hour))
                ].empty
                has_pairing = not normalized_pairing[
                    (normalized_pairing["comparison_track_id"].astype(str) == comparison_track_id)
                    & (normalized_pairing["hour"].astype(int) == int(hour))
                ].empty
                if has_fss and has_kl and has_pairing:
                    continue

                timestamp_utc = timestamps_by_hour.get(int(hour))
                if not timestamp_utc:
                    continue
                product_paths = self._legacy_track_product_paths(case_id, comparison_track_id, timestamp_utc)
                required_paths = [
                    product_paths["footprint"],
                    product_paths["density"],
                    product_paths["pygnome_footprint"],
                    product_paths["pygnome_density"],
                ]
                if not all(path.exists() for path in required_paths):
                    continue

                opendrift_footprint = self._load_raster_mask(product_paths["footprint"])
                pygnome_footprint = self._load_raster_mask(product_paths["pygnome_footprint"])
                opendrift_density = self._load_raster_mask(product_paths["density"])
                pygnome_density = self._load_raster_mask(product_paths["pygnome_density"])
                opendrift_hits = (np.asarray(opendrift_footprint["array"], dtype=float) > 0).astype(np.float32)
                pygnome_hits = (np.asarray(pygnome_footprint["array"], dtype=float) > 0).astype(np.float32)
                density_array = np.asarray(opendrift_density["array"], dtype=float)
                pygnome_density_array = np.asarray(pygnome_density["array"], dtype=float)
                if float(np.clip(density_array, 0.0, None).sum()) <= 0.0 and float(opendrift_hits.sum()) > 0.0:
                    # Legacy prototype threshold products sometimes persist only the footprint mask
                    # while leaving the paired "density" raster blank. Rebuild a simple normalized
                    # pseudo-density from the footprint support so the historical summary can still
                    # compute KL for the support-only p50/p90 tracks.
                    density_array = opendrift_hits.astype(np.float64)
                    density_array /= float(density_array.sum())
                if float(np.clip(pygnome_density_array, 0.0, None).sum()) <= 0.0 and float(pygnome_hits.sum()) > 0.0:
                    pygnome_density_array = pygnome_hits.astype(np.float64)
                    pygnome_density_array /= float(pygnome_density_array.sum())
                sea_mask = self._load_similarity_sea_mask(density_array.shape)

                self._ensure_overlay_png(product_paths["qa_overlay"], opendrift_hits, pygnome_hits)

                if not has_pairing:
                    deterministic_match = deterministic_rows[deterministic_rows["hour"].astype(int) == int(hour)]
                    control_row = deterministic_match.iloc[0].to_dict() if not deterministic_match.empty else {}
                    normalized_pairing = pd.concat(
                        [
                            normalized_pairing,
                            pd.DataFrame(
                                [
                                    {
                                        "comparison_track_id": comparison_track_id,
                                        "comparison_track_label": COMPARISON_TRACK_LABELS.get(comparison_track_id, comparison_track_id),
                                        "timestamp_utc": timestamp_utc,
                                        "hour": int(hour),
                                        "opendrift_footprint_path": str(product_paths["footprint"].relative_to(self.repo_root)),
                                        "pygnome_footprint_path": str(product_paths["pygnome_footprint"].relative_to(self.repo_root)),
                                        "opendrift_density_path": str(product_paths["density"].relative_to(self.repo_root)),
                                        "opendrift_nc_path": str(control_row.get("opendrift_nc_path") or ""),
                                        "pygnome_density_path": str(product_paths["pygnome_density"].relative_to(self.repo_root)),
                                        "control_footprint_path": str(control_row.get("control_footprint_path") or ""),
                                        "control_density_path": str(control_row.get("control_density_path") or ""),
                                        "footprint_precheck_json": str(control_row.get("footprint_precheck_json") or ""),
                                        "density_precheck_json": str(control_row.get("density_precheck_json") or ""),
                                        "qa_overlay_path": str(product_paths["qa_overlay"].relative_to(self.repo_root)),
                                        "pygnome_nc_path": str(control_row.get("pygnome_nc_path") or ""),
                                        "pygnome_mass_strategy": str(control_row.get("pygnome_mass_strategy") or ""),
                                        "opendrift_density_ocean_sum": float(np.clip(density_array[sea_mask], 0.0, None).sum()),
                                        "pygnome_density_ocean_sum": float(np.clip(pygnome_density_array[sea_mask], 0.0, None).sum()),
                                    }
                                ]
                            ),
                        ],
                        ignore_index=True,
                    )
                    repaired = True

                if not has_fss:
                    new_fss_rows = [
                        {
                            "comparison_track_id": comparison_track_id,
                            "comparison_track_label": COMPARISON_TRACK_LABELS.get(comparison_track_id, comparison_track_id),
                            "timestamp_utc": timestamp_utc,
                            "hour": int(hour),
                            "window_km": int(window_km),
                            "window_cells": int(window_km),
                            "fss": float(calculate_fss(opendrift_hits, pygnome_hits, window=int(window_km))),
                        }
                        for window_km in REQUIRED_WINDOWS_KM
                    ]
                    normalized_fss = pd.concat([normalized_fss, pd.DataFrame(new_fss_rows)], ignore_index=True)
                    repaired = True

                if not has_kl:
                    kl_valid_mask = sea_mask
                    try:
                        kl_value = float(
                            calculate_kl_divergence(
                                density_array,
                                pygnome_density_array,
                                epsilon=1e-10,
                                valid_mask=kl_valid_mask,
                            )
                        )
                    except ValueError:
                        # Some legacy prototype threshold rasters do not align defensibly with the
                        # stored sea-mask geometry. Fall back to the positive-support footprint union
                        # so the legacy support package can still be reconstructed from the on-disk
                        # benchmark rasters instead of hard-failing on stale manifests.
                        kl_valid_mask = (
                            np.clip(density_array, 0.0, None) > 0.0
                        ) | (
                            np.clip(pygnome_density_array, 0.0, None) > 0.0
                        )
                        if not np.any(kl_valid_mask):
                            kl_valid_mask = np.ones(density_array.shape, dtype=bool)
                        safe_forecast = np.asarray(density_array, dtype=float)
                        safe_observed = np.asarray(pygnome_density_array, dtype=float)
                        if float(np.clip(safe_forecast[kl_valid_mask], 0.0, None).sum()) <= 0.0:
                            safe_forecast = np.where(kl_valid_mask, 1.0, 0.0)
                        if float(np.clip(safe_observed[kl_valid_mask], 0.0, None).sum()) <= 0.0:
                            safe_observed = np.where(kl_valid_mask, 1.0, 0.0)
                        kl_value = float(
                            calculate_kl_divergence(
                                safe_forecast,
                                safe_observed,
                                epsilon=1e-10,
                                valid_mask=kl_valid_mask,
                            )
                        )
                    normalized_kl = pd.concat(
                        [
                            normalized_kl,
                            pd.DataFrame(
                                [
                                    {
                                        "comparison_track_id": comparison_track_id,
                                        "comparison_track_label": COMPARISON_TRACK_LABELS.get(comparison_track_id, comparison_track_id),
                                        "timestamp_utc": timestamp_utc,
                                        "hour": int(hour),
                                        "epsilon": 1e-10,
                                        "ocean_cell_count": int(np.count_nonzero(kl_valid_mask)),
                                        "kl_divergence": kl_value,
                                    }
                                ]
                            ),
                        ],
                        ignore_index=True,
                    )
                    repaired = True

        if repaired:
            normalized_pairing = normalized_pairing.sort_values(["comparison_track_id", "hour"]).reset_index(drop=True)
            normalized_fss = normalized_fss.sort_values(["comparison_track_id", "hour", "window_km"]).reset_index(drop=True)
            normalized_kl = normalized_kl.sort_values(["comparison_track_id", "hour"]).reset_index(drop=True)
            normalized_fss.to_csv(paths["fss_csv"], index=False)
            normalized_kl.to_csv(paths["kl_csv"], index=False)
            normalized_pairing.to_csv(paths["pairing_csv"], index=False)
            pd.DataFrame(
                self._build_case_summary_rows(
                    normalized_fss,
                    normalized_kl,
                    normalized_pairing,
                )
            ).to_csv(paths["summary_csv"], index=False)

        return normalized_fss, normalized_kl, normalized_pairing, repaired

    def _build_case_summary_rows(
        self,
        fss_df: pd.DataFrame,
        kl_df: pd.DataFrame,
        pairing_df: pd.DataFrame,
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for comparison_track_id in self._comparison_track_ids():
            track_label = COMPARISON_TRACK_LABELS.get(comparison_track_id, comparison_track_id)
            track_fss = fss_df[fss_df["comparison_track_id"].astype(str) == comparison_track_id].copy()
            track_kl = kl_df[kl_df["comparison_track_id"].astype(str) == comparison_track_id].copy()
            track_pairings = pairing_df[pairing_df["comparison_track_id"].astype(str) == comparison_track_id].copy()
            if track_fss.empty and track_kl.empty and track_pairings.empty:
                continue
            for window_km in REQUIRED_WINDOWS_KM:
                subset = track_fss[track_fss["window_km"].astype(int) == int(window_km)].copy()
                rows.append(
                    {
                        "comparison_track_id": comparison_track_id,
                        "comparison_track_label": track_label,
                        "metric": "FSS",
                        "window_km": int(window_km),
                        "pair_count": int(len(subset)),
                        "mean_value": float(subset["fss"].mean()) if not subset.empty else np.nan,
                        "min_value": float(subset["fss"].min()) if not subset.empty else np.nan,
                        "max_value": float(subset["fss"].max()) if not subset.empty else np.nan,
                        "notes": f"Prototype comparator summary for {track_label}.",
                    }
                )
            rows.append(
                {
                    "comparison_track_id": comparison_track_id,
                    "comparison_track_label": track_label,
                    "metric": "KL",
                    "window_km": "",
                    "pair_count": int(len(track_kl)),
                    "mean_value": float(track_kl["kl_divergence"].mean()) if not track_kl.empty else np.nan,
                    "min_value": float(track_kl["kl_divergence"].min()) if not track_kl.empty else np.nan,
                    "max_value": float(track_kl["kl_divergence"].max()) if not track_kl.empty else np.nan,
                    "notes": f"Prototype comparator summary for {track_label}.",
                }
            )
            rows.append(
                {
                    "comparison_track_id": comparison_track_id,
                    "comparison_track_label": track_label,
                    "metric": "PAIRING",
                    "window_km": "",
                    "pair_count": int(len(track_pairings)),
                    "mean_value": np.nan,
                    "min_value": np.nan,
                    "max_value": np.nan,
                    "notes": f"Prototype comparator summary for {track_label}.",
                }
            )
        return rows

    def _apply_minimum_crop_span(
        self,
        lower: float,
        upper: float,
        *,
        minimum_span: float,
        domain_min: float,
        domain_max: float,
    ) -> tuple[float, float]:
        span = upper - lower
        if span >= minimum_span:
            return max(domain_min, lower), min(domain_max, upper)
        center = (lower + upper) / 2.0
        half_span = minimum_span / 2.0
        adjusted_lower = max(domain_min, center - half_span)
        adjusted_upper = min(domain_max, center + half_span)
        if adjusted_upper - adjusted_lower >= minimum_span:
            return adjusted_lower, adjusted_upper
        if adjusted_lower <= domain_min:
            return domain_min, min(domain_max, domain_min + minimum_span)
        return max(domain_min, domain_max - minimum_span), domain_max

    def _compute_case_crop_bounds(
        self,
        pairings_by_hour: dict[str, dict[int, dict[str, Any]]],
        display_bounds: tuple[float, float, float, float],
        *,
        source_point: tuple[float, float] | None = None,
    ) -> tuple[float, float, float, float]:
        bounds_sets: list[tuple[float, float, float, float]] = []
        cell_widths: list[float] = []
        cell_heights: list[float] = []
        for track_pairings in pairings_by_hour.values():
            for hour in REQUIRED_HOURS:
                row = track_pairings[hour]
                for key in ("opendrift_footprint_path", "pygnome_footprint_path"):
                    resolved_key = "opendrift_footprint_path_resolved" if key == "opendrift_footprint_path" else f"{key}_resolved"
                    info = self._load_raster_mask(row[resolved_key])
                    positive_cell_bounds = info.get("positive_cell_bounds")
                    if positive_cell_bounds is None:
                        continue
                    bounds_sets.append(tuple(float(value) for value in positive_cell_bounds))
                    cell_widths.append(float(info["cell_width"]))
                    cell_heights.append(float(info["cell_height"]))
        point_bounds = bounds_from_point(
            source_point,
            lon_pad=max(cell_widths, default=0.05) * 2.0,
            lat_pad=max(cell_heights, default=0.05) * 2.0,
        )
        if point_bounds is not None:
            bounds_sets.append(point_bounds)
        return derive_prototype_2016_figure_bounds(
            base_bounds=display_bounds,
            bounds_sets=bounds_sets,
            cell_widths=cell_widths,
            cell_heights=cell_heights,
            extent_mode=self.extent_mode,
            minimum_span_degrees=max(
                max(cell_widths, default=0.05) * PROTOTYPE_MIN_CROP_CELLS_X,
                max(cell_heights, default=0.05) * PROTOTYPE_MIN_CROP_CELLS_Y,
                0.65,
            ),
        )

    def _resolve_plot_bounds(
        self,
        *,
        base_bounds: tuple[float, float, float, float],
        raster_paths: list[Path | str | None] | tuple[Path | str | None, ...] = (),
        source_point: tuple[float, float] | None = None,
        trajectory_points: list[tuple[float, float]] | tuple[tuple[float, float], ...] | None = None,
    ) -> tuple[float, float, float, float]:
        bounds_sets, cell_widths, cell_heights = self._raster_bounds_for_paths(*raster_paths)
        trajectory_bounds = bounds_from_points(trajectory_points)
        if trajectory_bounds is not None:
            bounds_sets.append(trajectory_bounds)
        point_bounds = bounds_from_point(
            source_point,
            lon_pad=max(cell_widths, default=0.05) * 2.0 if source_point is not None else 0.0,
            lat_pad=max(cell_heights, default=0.05) * 2.0 if source_point is not None else 0.0,
        )
        if point_bounds is not None:
            bounds_sets.append(point_bounds)
        return derive_prototype_2016_figure_bounds(
            base_bounds=base_bounds,
            bounds_sets=bounds_sets,
            cell_widths=cell_widths,
            cell_heights=cell_heights,
            extent_mode=self.extent_mode,
            minimum_span_degrees=0.65,
        )

    def _build_pairings_by_hour(self, case_id: str, pairing_df: pd.DataFrame) -> dict[str, dict[int, dict[str, Any]]]:
        missing_columns = [column for column in REQUIRED_PAIRING_COLUMNS if column not in pairing_df.columns]
        if missing_columns:
            raise ValueError(
                f"Prototype benchmark pairing manifest for {case_id} is missing columns: {', '.join(missing_columns)}"
            )
        pairings: dict[str, dict[int, dict[str, Any]]] = {}
        normalized_df = pairing_df.copy()
        if "comparison_track_id" not in normalized_df.columns:
            normalized_df["comparison_track_id"] = "deterministic"
        if "comparison_track_label" not in normalized_df.columns:
            normalized_df["comparison_track_label"] = normalized_df["comparison_track_id"].map(COMPARISON_TRACK_LABELS).fillna(
                normalized_df["comparison_track_id"].astype(str)
            )

        for comparison_track_id in self._comparison_track_ids():
            track_subset = normalized_df[
                normalized_df["comparison_track_id"].astype(str) == comparison_track_id
            ].copy()
            if track_subset.empty:
                raise ValueError(
                    f"Prototype benchmark pairing manifest for {case_id} is missing the {comparison_track_id} track rows."
                )
            pairings[comparison_track_id] = {}
            for hour in REQUIRED_HOURS:
                subset = track_subset[track_subset["hour"].astype(int) == hour]
                if subset.empty:
                    raise ValueError(
                        f"Prototype benchmark pairing manifest for {case_id} is missing the {comparison_track_id} {hour} h snapshot row."
                    )
                row = subset.iloc[0].to_dict()
                row["hour"] = int(row["hour"])
                footprint_key = (
                    "opendrift_footprint_path"
                    if self._has_path_value(row.get("opendrift_footprint_path"))
                    else "control_footprint_path"
                )
                density_key = (
                    "opendrift_density_path"
                    if self._has_path_value(row.get("opendrift_density_path"))
                    else "control_density_path"
                )
                row["opendrift_footprint_path"] = self._clean_optional_path_value(
                    row.get("opendrift_footprint_path")
                ) or self._clean_optional_path_value(row.get("control_footprint_path"))
                row["opendrift_density_path"] = self._clean_optional_path_value(
                    row.get("opendrift_density_path")
                ) or self._clean_optional_path_value(row.get("control_density_path"))
                for key in (footprint_key, "pygnome_footprint_path", "qa_overlay_path"):
                    resolved = self._resolve_repo_path(self._clean_optional_path_value(row.get(key)))
                    row[f"{key}_resolved"] = resolved
                    if key == footprint_key:
                        row["opendrift_footprint_path_resolved"] = resolved
                    if not resolved.exists():
                        raise FileNotFoundError(
                            "Prototype PyGNOME similarity summary requires per-hour benchmark artifacts for every configured case. "
                            f"Missing {key} for {case_id} track {comparison_track_id} hour {hour}: {resolved}"
                        )
                for key in (density_key, "pygnome_density_path"):
                    raw_value = self._clean_optional_path_value(row.get(key))
                    resolved_key = f"{key}_resolved"
                    if raw_value:
                        resolved = self._resolve_repo_path(raw_value)
                        row[resolved_key] = resolved if resolved.exists() else None
                        if key == density_key:
                            row["opendrift_density_path_resolved"] = resolved if resolved.exists() else None
                    else:
                        row[resolved_key] = None
                        if key == density_key:
                            row["opendrift_density_path_resolved"] = None
                pairings[comparison_track_id][hour] = row
        return pairings

    def _load_case_artifacts(self, case_id: str) -> dict[str, Any]:
        paths = self._required_paths(case_id)
        missing = [f"{artifact_id}={path}" for artifact_id, path in paths.items() if not path.exists()]
        if missing:
            raise FileNotFoundError(
                "Prototype PyGNOME similarity summary requires benchmark artifacts for every configured 2016 case. "
                f"Missing for {case_id}: " + "; ".join(missing)
            )

        fss_df = pd.read_csv(paths["fss_csv"])
        kl_df = pd.read_csv(paths["kl_csv"])
        pairing_df = pd.read_csv(paths["pairing_csv"])
        summary_df = pd.read_csv(paths["summary_csv"])
        metadata = json.loads(paths["metadata_json"].read_text(encoding="utf-8"))

        fss_df, kl_df, pairing_df, repaired = self._repair_legacy_benchmark_artifacts(
            case_id,
            paths,
            fss_df,
            kl_df,
            pairing_df,
        )
        if repaired:
            summary_df = pd.read_csv(paths["summary_csv"])

        if "comparison_track_id" not in fss_df.columns:
            fss_df["comparison_track_id"] = "deterministic"
        if "comparison_track_label" not in fss_df.columns:
            fss_df["comparison_track_label"] = fss_df["comparison_track_id"].map(COMPARISON_TRACK_LABELS).fillna(
                fss_df["comparison_track_id"].astype(str)
            )
        if "comparison_track_id" not in kl_df.columns:
            kl_df["comparison_track_id"] = "deterministic"
        if "comparison_track_label" not in kl_df.columns:
            kl_df["comparison_track_label"] = kl_df["comparison_track_id"].map(COMPARISON_TRACK_LABELS).fillna(
                kl_df["comparison_track_id"].astype(str)
            )
        if "comparison_track_id" not in summary_df.columns:
            summary_df["comparison_track_id"] = "deterministic"
        if "comparison_track_label" not in summary_df.columns:
            summary_df["comparison_track_label"] = summary_df["comparison_track_id"].map(COMPARISON_TRACK_LABELS).fillna(
                summary_df["comparison_track_id"].astype(str)
            )

        missing_windows = [window for window in REQUIRED_WINDOWS_KM if window not in set(fss_df["window_km"].astype(int).tolist())]
        missing_fss_hours: dict[str, list[int]] = {}
        missing_kl_hours: dict[str, list[int]] = {}
        missing_tracks: list[str] = []
        for comparison_track_id in self._comparison_track_ids():
            track_fss = fss_df[fss_df["comparison_track_id"].astype(str) == comparison_track_id]
            track_kl = kl_df[kl_df["comparison_track_id"].astype(str) == comparison_track_id]
            if track_fss.empty or track_kl.empty:
                missing_tracks.append(comparison_track_id)
                continue
            track_missing_fss = [hour for hour in REQUIRED_HOURS if hour not in set(track_fss["hour"].astype(int).tolist())]
            track_missing_kl = [hour for hour in REQUIRED_HOURS if hour not in set(track_kl["hour"].astype(int).tolist())]
            if track_missing_fss:
                missing_fss_hours[comparison_track_id] = track_missing_fss
            if track_missing_kl:
                missing_kl_hours[comparison_track_id] = track_missing_kl
        if missing_windows or missing_fss_hours or missing_kl_hours or missing_tracks:
            raise ValueError(
                f"Prototype benchmark artifacts for {case_id} are incomplete. "
                f"missing_tracks={missing_tracks}, missing_windows={missing_windows}, "
                f"missing_fss_hours={missing_fss_hours}, missing_kl_hours={missing_kl_hours}"
            )

        pairings_by_hour = self._build_pairings_by_hour(case_id, pairing_df)
        display_bounds = self._load_case_display_bounds(case_id)
        source_point = self._resolve_case_source_point(case_id)

        return {
            "case_id": case_id,
            "benchmark_dir": self._case_benchmark_dir(case_id),
            "paths": paths,
            "summary_df": summary_df,
            "fss_df": fss_df,
            "kl_df": kl_df,
            "pairing_df": pairing_df,
            "pairings_by_hour": pairings_by_hour,
            "metadata": metadata,
            "display_bounds": display_bounds,
            "crop_bounds": self._compute_case_crop_bounds(
                pairings_by_hour,
                display_bounds,
                source_point=source_point,
            ),
            "source_point": source_point,
        }

    def _load_available_case_artifacts(
        self,
    ) -> tuple[list[dict[str, Any]], list[dict[str, str]]]:
        case_data: list[dict[str, Any]] = []
        skipped_cases: list[dict[str, str]] = []
        for case_id in self.case_ids:
            try:
                case_data.append(self._load_case_artifacts(case_id))
            except Exception as exc:
                skipped_cases.append(
                    {
                        "case_id": case_id,
                        "error_type": type(exc).__name__,
                        "error_message": str(exc),
                    }
                )
        return case_data, skipped_cases

    def _build_case_registry_rows(self, case_data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for item in case_data:
            metadata = item["metadata"]
            fss_df = item["fss_df"]
            crop_bounds = item["crop_bounds"]
            rendering = prototype_2016_rendering_metadata(crop_bounds)
            rows.append(
                {
                    "case_id": item["case_id"],
                    "benchmark_dir": str(item["benchmark_dir"].relative_to(self.repo_root)),
                    "summary_csv": str(item["paths"]["summary_csv"].relative_to(self.repo_root)),
                    "fss_csv": str(item["paths"]["fss_csv"].relative_to(self.repo_root)),
                    "kl_csv": str(item["paths"]["kl_csv"].relative_to(self.repo_root)),
                    "pairing_manifest_csv": str(item["paths"]["pairing_csv"].relative_to(self.repo_root)),
                    "pygnome_metadata_json": str(item["paths"]["metadata_json"].relative_to(self.repo_root)),
                    "snapshot_hours": ";".join(str(hour) for hour in sorted(fss_df["hour"].astype(int).unique().tolist())),
                    "fss_windows_km": ";".join(str(window) for window in sorted(fss_df["window_km"].astype(int).unique().tolist())),
                    "comparison_track_ids": ";".join(
                        str(track_id)
                        for track_id in sorted(fss_df["comparison_track_id"].astype(str).unique().tolist())
                    ),
                    "case_crop_bounds": ",".join(f"{value:.4f}" for value in crop_bounds),
                    "display_bounds_wgs84": ",".join(f"{value:.4f}" for value in rendering["display_bounds_wgs84"]),
                    "rendering_profile": rendering["rendering_profile"],
                    "map_projection": rendering["map_projection"],
                    "projection_center_lon": float(rendering["projection_center"]["lon"]),
                    "projection_center_lat": float(rendering["projection_center"]["lat"]),
                    "pygnome_weathering_enabled": _coerce_bool(metadata.get("weathering_enabled")),
                    "pygnome_role": "comparator_only",
                    "benchmark_particles": int(metadata.get("benchmark_particles", 0) or 0),
                    "notes": (
                        "Legacy prototype Phase 3A transport benchmark. "
                        "OpenDrift deterministic plus legacy support-only p50/p90 threshold tracks are compared against deterministic PyGNOME."
                        if self.workflow_mode == "prototype_2016"
                        else "Accepted-segment prototype transport benchmark. Deterministic OpenDrift control is compared to deterministic PyGNOME only."
                    ),
                }
            )
        return rows

    def _build_fss_rows(self, case_data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for item in case_data:
            fss_df = item["fss_df"].copy()
            fss_df["window_km"] = fss_df["window_km"].astype(int)
            fss_df["hour"] = fss_df["hour"].astype(int)
            for comparison_track_id in self._comparison_track_ids():
                track_df = fss_df[fss_df["comparison_track_id"].astype(str) == comparison_track_id].copy()
                for window in REQUIRED_WINDOWS_KM:
                    subset = track_df[track_df["window_km"] == window].sort_values("hour")
                    row = {
                        "case_id": item["case_id"],
                        "comparison_track_id": comparison_track_id,
                        "comparison_track_label": COMPARISON_TRACK_LABELS.get(comparison_track_id, comparison_track_id),
                        "window_km": window,
                        "snapshot_count": int(len(subset)),
                        "mean_fss": float(subset["fss"].mean()),
                        "min_fss": float(subset["fss"].min()),
                        "max_fss": float(subset["fss"].max()),
                    }
                    for hour in REQUIRED_HOURS:
                        value = subset.loc[subset["hour"] == hour, "fss"]
                        row[f"fss_{hour}h"] = float(value.iloc[0])
                    rows.append(row)
        return rows

    def _build_kl_rows(self, case_data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for item in case_data:
            kl_df = item["kl_df"].copy()
            kl_df["hour"] = kl_df["hour"].astype(int)
            for comparison_track_id in self._comparison_track_ids():
                track_df = kl_df[kl_df["comparison_track_id"].astype(str) == comparison_track_id].sort_values("hour")
                for _, record in track_df.iterrows():
                    rows.append(
                        {
                            "case_id": item["case_id"],
                            "comparison_track_id": comparison_track_id,
                            "comparison_track_label": COMPARISON_TRACK_LABELS.get(comparison_track_id, comparison_track_id),
                            "hour": int(record["hour"]),
                            "kl_divergence": float(record["kl_divergence"]),
                            "epsilon": float(record.get("epsilon", 0.0)),
                            "ocean_cell_count": int(record.get("ocean_cell_count", 0) or 0),
                        }
                    )
        return rows

    def _build_similarity_rows(
        self,
        case_data: list[dict[str, Any]],
        fss_rows: list[dict[str, Any]],
        kl_rows: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        fss_df = pd.DataFrame(fss_rows)
        kl_df = pd.DataFrame(kl_rows)
        rows: list[dict[str, Any]] = []
        for item in case_data:
            case_id = item["case_id"]
            for comparison_track_id in self._comparison_track_ids():
                case_fss = fss_df[
                    (fss_df["case_id"] == case_id)
                    & (fss_df["comparison_track_id"] == comparison_track_id)
                ].copy()
                case_kl = kl_df[
                    (kl_df["case_id"] == case_id)
                    & (kl_df["comparison_track_id"] == comparison_track_id)
                ].copy().sort_values("hour")
                fss_by_window = {int(row["window_km"]): row for _, row in case_fss.iterrows()}
                kl_by_hour = {int(row["hour"]): row for _, row in case_kl.iterrows()}
                row = {
                    "case_id": case_id,
                    "comparison_track_id": comparison_track_id,
                    "comparison_track_label": COMPARISON_TRACK_LABELS.get(comparison_track_id, comparison_track_id),
                    "pair_count": int(
                        len(
                            item["pairing_df"][
                                item["pairing_df"]["comparison_track_id"].astype(str) == comparison_track_id
                            ]
                        )
                    ),
                    "pygnome_weathering_enabled": _coerce_bool(item["metadata"].get("weathering_enabled")),
                    "relative_similarity_rank": 0,
                    "mean_kl": float(case_kl["kl_divergence"].mean()),
                    "min_kl": float(case_kl["kl_divergence"].min()),
                    "max_kl": float(case_kl["kl_divergence"].max()),
                    "notes": "Comparator-only prototype transport benchmark. PyGNOME is not treated as truth.",
                }
                for window in REQUIRED_WINDOWS_KM:
                    row[f"mean_fss_{window}km"] = float(fss_by_window[window]["mean_fss"])
                    row[f"min_fss_{window}km"] = float(fss_by_window[window]["min_fss"])
                    row[f"max_fss_{window}km"] = float(fss_by_window[window]["max_fss"])
                for hour in REQUIRED_HOURS:
                    row[f"fss_5km_{hour}h"] = float(fss_by_window[5][f"fss_{hour}h"])
                    row[f"kl_{hour}h"] = float(kl_by_hour[hour]["kl_divergence"])
                rows.append(row)

        similarity_df = pd.DataFrame(rows)
        similarity_df = similarity_df.sort_values(
            by=["comparison_track_id", "mean_fss_5km", "mean_kl", "case_id"],
            ascending=[True, False, True, True],
        ).reset_index(drop=True)
        similarity_df["relative_similarity_rank"] = similarity_df.groupby("comparison_track_id").cumcount() + 1
        return similarity_df.to_dict(orient="records")

    def _fss_values_by_window(self, item: dict[str, Any], comparison_track_id: str, hour: int) -> dict[int, float]:
        subset = item["fss_df"][
            (item["fss_df"]["comparison_track_id"].astype(str) == comparison_track_id)
            & (item["fss_df"]["hour"].astype(int) == int(hour))
        ].copy()
        subset["window_km"] = subset["window_km"].astype(int)
        return {int(row["window_km"]): float(row["fss"]) for _, row in subset.iterrows()}

    def _kl_value(self, item: dict[str, Any], comparison_track_id: str, hour: int) -> float:
        subset = item["kl_df"][
            (item["kl_df"]["comparison_track_id"].astype(str) == comparison_track_id)
            & (item["kl_df"]["hour"].astype(int) == int(hour))
        ]
        return float(subset.iloc[0]["kl_divergence"])

    def _metrics_snippet(self, item: dict[str, Any], comparison_track_id: str, hour: int) -> str:
        fss_values = self._fss_values_by_window(item, comparison_track_id, hour)
        fss_line = "/".join(f"{fss_values[window]:.3f}" for window in REQUIRED_WINDOWS_KM)
        return f"FSS 1/3/5/10 km = {fss_line}; KL = {self._kl_value(item, comparison_track_id, hour):.3f}."

    def _pairing_row_for_model(self, item: dict[str, Any], hour: int, model_name: str) -> dict[str, Any]:
        if model_name == "pygnome":
            return item["pairings_by_hour"]["deterministic"][hour]
        return item["pairings_by_hour"][MODEL_NAME_TO_TRACK_ID[model_name]][hour]

    def _load_representative_trajectory(self, nc_path: str | Path | None) -> list[tuple[float, float]] | None:
        if not self._has_path_value(nc_path):
            return None
        resolved = self._resolve_repo_path(self._clean_optional_path_value(nc_path))
        if not resolved.exists():
            return None
        try:
            with xr.open_dataset(resolved) as ds:
                if "lon" not in ds or "lat" not in ds:
                    return None
                lon = np.asarray(ds["lon"].values)
                lat = np.asarray(ds["lat"].values)
                if lon.ndim == 1:
                    lon = lon[:, np.newaxis]
                    lat = lat[:, np.newaxis]
                status = np.zeros_like(lon, dtype=float)
                if "status" in ds:
                    status = np.asarray(ds["status"].values)
                    if status.ndim == 1:
                        status = status[:, np.newaxis]
                points: list[tuple[float, float]] = []
                for time_idx in range(lon.shape[0]):
                    valid = np.isfinite(lon[time_idx]) & np.isfinite(lat[time_idx])
                    if status.shape == lon.shape:
                        valid &= status[time_idx] == 0
                    if not np.any(valid):
                        continue
                    points.append(
                        (
                            float(np.nanmedian(lon[time_idx][valid])),
                            float(np.nanmedian(lat[time_idx][valid])),
                        )
                    )
                return points if len(points) >= 2 else None
        except Exception:
            return None

    def _trajectory_for_model(self, pair_row: dict[str, Any], model_name: str) -> list[tuple[float, float]] | None:
        if model_name == "pygnome":
            return self._load_representative_trajectory(pair_row.get("pygnome_nc_path"))
        if model_name == "opendrift":
            return self._load_representative_trajectory(pair_row.get("opendrift_nc_path"))
        return None

    def _single_figure_filename(self, case_id: str, hour: int, model_name: str) -> str:
        return f"{_safe_token(case_id)}__{self._workflow_token()}__{int(hour)}h__{_safe_token(model_name)}__single.png"

    def _board_figure_filename(self, case_id: str) -> str:
        return f"{_safe_token(case_id)}__{self._workflow_token()}__24_48_72h__opendrift_vs_pygnome__board.png"

    def _figure_pixel_size(self, fig: plt.Figure) -> tuple[int, int]:
        width, height = fig.get_size_inches()
        dpi = fig.get_dpi()
        return int(round(width * dpi)), int(round(height * dpi))

    def _geographic_aspect(self, latitude: float) -> float:
        cosine = abs(float(np.cos(np.deg2rad(latitude))))
        return 1.0 / max(cosine, 1.0e-6)

    def _visible_labels(self, crop_bounds: tuple[float, float, float, float]) -> pd.DataFrame:
        context = self._load_prototype_map_context()
        labels_df = context["labels_df"].copy()
        if labels_df.empty or not {"enabled_yes_no", "lon", "lat"}.issubset(labels_df.columns):
            return labels_df
        active = labels_df.loc[labels_df["enabled_yes_no"].astype(str).str.lower() == "yes"].copy()
        if active.empty:
            return active
        x_min, x_max, y_min, y_max = crop_bounds
        pad_x = max((x_max - x_min) * 0.12, 0.04)
        pad_y = max((y_max - y_min) * 0.12, 0.04)
        return active.loc[
            active["lon"].astype(float).between(x_min - pad_x, x_max + pad_x)
            & active["lat"].astype(float).between(y_min - pad_y, y_max + pad_y)
        ].copy()

    def _draw_context_layers(self, ax: plt.Axes) -> None:
        context = self._load_prototype_map_context()
        land_mask_path = context.get("land_mask_path")
        shoreline_path = context.get("shoreline_path")
        if not land_mask_path and not shoreline_path:
            return
        land = self._load_land_polygons(Path(land_mask_path), "EPSG:4326") if land_mask_path else None
        if land is not None and not land.empty:
            land.plot(ax=ax, color=MAP_LAND_COLOR, edgecolor="none", alpha=1.0, zorder=0)
        shoreline = self._load_vector(Path(shoreline_path), "EPSG:4326") if shoreline_path else None
        if shoreline is not None and not shoreline.empty:
            shoreline.plot(ax=ax, color=MAP_SHORELINE_COLOR, linewidth=0.9, alpha=0.96, zorder=1)

    def _draw_crop_labels(self, ax: plt.Axes, crop_bounds: tuple[float, float, float, float]) -> None:
        labels_df = self._visible_labels(crop_bounds)
        if labels_df.empty:
            return
        for _, row in labels_df.iterrows():
            text_kwargs = {}
            if self.workflow_mode == "prototype_2016":
                text_kwargs["transform"] = ccrs.PlateCarree()
            ax.text(
                float(row["lon"]),
                float(row["lat"]),
                str(row["label_text"]),
                fontsize=7.1,
                color="#374151",
                ha="left",
                va="bottom",
                zorder=7,
                bbox={"boxstyle": "round,pad=0.16", "facecolor": (1, 1, 1, 0.68), "edgecolor": "none"},
                **text_kwargs,
            )

    def _draw_locator(
        self,
        ax: plt.Axes,
        crop_bounds: tuple[float, float, float, float],
        full_bounds: tuple[float, float, float, float] | None = None,
    ) -> None:
        context = self._load_prototype_map_context()
        shoreline_path = context.get("shoreline_path")
        shoreline = self._load_vector(Path(shoreline_path), "EPSG:4326") if shoreline_path else None
        labels_df = context["labels_df"]
        active_full_bounds = tuple(float(value) for value in (full_bounds or context["full_bounds_wgs84"]))
        ax.set_facecolor(MAP_WATER_COLOR)
        locator_patch_kwargs = {"transform": ccrs.PlateCarree()} if self.workflow_mode == "prototype_2016" else {}
        if shoreline is not None and not shoreline.empty:
            shoreline.plot(ax=ax, color=MAP_SHORELINE_COLOR, linewidth=0.6, alpha=0.92, zorder=1)
        if not labels_df.empty:
            active = labels_df.loc[labels_df["enabled_yes_no"].astype(str).str.lower() == "yes"].copy()
            ax.scatter(active["lon"], active["lat"], s=9, color="#111827", zorder=3)
            for _, row in active.iterrows():
                ax.text(
                    float(row["lon"]),
                    float(row["lat"]),
                    str(row["label_text"]),
                    fontsize=5.4,
                    color="#111827",
                    ha="left",
                    va="bottom",
                    zorder=4,
                )
        x_min, x_max, y_min, y_max = crop_bounds
        ax.add_patch(
            Rectangle(
                (x_min, y_min),
                x_max - x_min,
                y_max - y_min,
                fill=False,
                linewidth=1.4,
                linestyle="-",
                edgecolor="#b42318",
                zorder=5,
                **locator_patch_kwargs,
            )
        )
        lon_pad = max((active_full_bounds[1] - active_full_bounds[0]) * PROTOTYPE_LOCATOR_PADDING_FRACTION, 0.08)
        lat_pad = max((active_full_bounds[3] - active_full_bounds[2]) * PROTOTYPE_LOCATOR_PADDING_FRACTION, 0.08)
        if self.workflow_mode == "prototype_2016":
            ax.set_extent(
                [
                    active_full_bounds[0] - lon_pad,
                    active_full_bounds[1] + lon_pad,
                    active_full_bounds[2] - lat_pad,
                    active_full_bounds[3] + lat_pad,
                ],
                crs=ccrs.PlateCarree(),
            )
        else:
            ax.set_xlim(active_full_bounds[0] - lon_pad, active_full_bounds[1] + lon_pad)
            ax.set_ylim(active_full_bounds[2] - lat_pad, active_full_bounds[3] + lat_pad)
            center_lat = ((active_full_bounds[2] - lat_pad) + (active_full_bounds[3] + lat_pad)) / 2.0
            ax.set_aspect(self._geographic_aspect(center_lat), adjustable="box")
        ax.set_title("Locator", fontsize=9.2, loc="left", pad=4)
        ax.set_xticks([])
        ax.set_yticks([])
        for spine in ax.spines.values():
            spine.set_edgecolor("#94a3b8")
            spine.set_linewidth(0.8)

    def _single_legend_handles(
        self,
        model_name: str,
        *,
        include_source_point: bool,
        show_raster_cells: bool = True,
        show_outline: bool = True,
    ) -> list[Any]:
        handles: list[Any] = []
        if include_source_point:
            handles.append(
                Line2D(
                    [0],
                    [0],
                    marker="*",
                    linestyle="None",
                    markerfacecolor=SOURCE_POINT_COLOR,
                    markeredgecolor="#111827",
                    markersize=13,
                    label="Provenance source point",
                )
            )
        if show_raster_cells:
            handles.append(
                Patch(
                    facecolor=to_rgba(MODEL_STYLES[model_name]["mid_color"], alpha=0.40),
                    edgecolor="none",
                    label="Stored raster cells",
                )
            )
        if show_outline:
            handles.append(
                Line2D(
                    [0],
                    [0],
                    color=MODEL_STYLES[model_name]["color"],
                    lw=1.4,
                    label="Exact footprint outline",
                )
            )
        return handles

    def _board_legend_handles(self, include_source_point: bool) -> list[Any]:
        handles: list[Any] = []
        if include_source_point:
            handles.append(
                Line2D(
                    [0],
                    [0],
                    marker="*",
                    linestyle="None",
                    markerfacecolor=SOURCE_POINT_COLOR,
                    markeredgecolor="#111827",
                    markersize=11,
                    label="Provenance source point",
                )
            )
        handles.extend(
            [
                Patch(facecolor=to_rgba("#94a3b8", alpha=0.45), edgecolor="none", label="Stored raster cells"),
                Line2D([0], [0], color=MODEL_STYLES["opendrift"]["color"], lw=1.5, label="OpenDrift outline"),
                Line2D([0], [0], color=MODEL_STYLES["pygnome"]["color"], lw=1.5, label="PyGNOME outline"),
            ]
        )
        return handles

    def _draw_board_side_panel(self, ax: plt.Axes, item: dict[str, Any], *, include_source_point: bool) -> None:
        ax.axis("off")
        legend = ax.legend(
            handles=self._board_legend_handles(include_source_point),
            loc="upper left",
            frameon=True,
            framealpha=0.98,
            facecolor="#ffffff",
            edgecolor="#cbd5e1",
            fontsize=7.5,
            title="Legend",
            title_fontsize=8.7,
        )
        legend.get_frame().set_linewidth(0.9)
        lines = [
            f"Case: {item['case_id']}",
            self._context_note_sentence(),
            "Panels show exact stored raster cells and exact footprint outlines only. Empty stored layers are omitted.",
            "PyGNOME is comparator-only, not truth.",
        ]
        ax.text(
            0.0,
            0.34,
            "\n".join(textwrap.fill(line, width=30) for line in lines),
            fontsize=8.2,
            ha="left",
            va="top",
            color="#334155",
            transform=ax.transAxes,
            bbox={"boxstyle": "round,pad=0.42", "facecolor": "#ffffff", "edgecolor": "#cbd5e1"},
        )

    def _draw_footer_note(self, ax: plt.Axes, title: str, lines: list[str], *, width: int = 140) -> None:
        ax.axis("off")
        wrapped_lines = [textwrap.fill(str(line), width=width) for line in lines if str(line).strip()]
        ax.text(
            0.0,
            0.94,
            title,
            fontsize=10.2,
            fontweight="bold",
            ha="left",
            va="top",
            color="#0f172a",
            transform=ax.transAxes,
        )
        ax.text(
            0.0,
            0.76,
            "\n".join(wrapped_lines),
            fontsize=8.6,
            ha="left",
            va="top",
            color="#334155",
            transform=ax.transAxes,
        )

    def _render_model_footprint(
        self,
        ax: plt.Axes,
        *,
        raster_path: Path,
        display_raster_path: Path | None = None,
        crop_bounds: tuple[float, float, float, float],
        model_name: str,
        panel_title: str,
        row_label: str = "",
        show_xlabel: bool = True,
        show_ylabel: bool = True,
        show_labels: bool = True,
        source_point: tuple[float, float] | None = None,
        trajectory_points: list[tuple[float, float]] | None = None,
    ) -> dict[str, Any]:
        info = self._load_raster_mask(raster_path)
        display_info = self._load_raster_mask(display_raster_path or raster_path)
        ax.set_facecolor(MAP_WATER_COLOR)
        self._draw_context_layers(ax)
        geographic_transform = ccrs.PlateCarree() if self.workflow_mode == "prototype_2016" else None
        patch_kwargs = {"transform": geographic_transform} if geographic_transform is not None else {}
        line_kwargs = {"transform": geographic_transform} if geographic_transform is not None else {}

        raster_cells_drawn = self._render_exact_raster_cells(
            ax,
            display_info=display_info,
            model_name=model_name,
        )
        polygons = info.get("footprint_polygons") or []
        geometry_drawn = False
        if polygons:
            for coordinates in polygons:
                ax.add_patch(
                    Polygon(
                        coordinates,
                        closed=True,
                        facecolor=to_rgba(MODEL_STYLES[model_name]["light_color"], alpha=0.10),
                        edgecolor=MODEL_STYLES[model_name]["color"],
                        linewidth=1.2,
                        alpha=0.98,
                        joinstyle="round",
                        zorder=6,
                        **patch_kwargs,
                    )
                )
                geometry_drawn = True
        else:
            for bounds in info.get("positive_cell_boxes") or []:
                ax.add_patch(
                    Rectangle(
                        (bounds[0], bounds[1]),
                        bounds[2] - bounds[0],
                        bounds[3] - bounds[1],
                        facecolor=to_rgba(MODEL_STYLES[model_name]["light_color"], alpha=0.10),
                        edgecolor=MODEL_STYLES[model_name]["color"],
                        linewidth=1.1,
                        linestyle="-",
                        alpha=0.90,
                        zorder=6,
                        **patch_kwargs,
                    )
                )
                geometry_drawn = True
        if trajectory_points:
            traj_x = [point[0] for point in trajectory_points]
            traj_y = [point[1] for point in trajectory_points]
            ax.plot(
                traj_x,
                traj_y,
                color=MODEL_STYLES[model_name]["color"],
                linewidth=1.2,
                alpha=0.75,
                linestyle="-",
                zorder=6.5,
                **line_kwargs,
            )
        if source_point is not None:
            ax.scatter(
                [source_point[0]],
                [source_point[1]],
                marker="*",
                s=240,
                c=SOURCE_POINT_COLOR,
                edgecolors="#111827",
                linewidths=1.0,
                zorder=7,
                **line_kwargs,
            )
        if self.workflow_mode == "prototype_2016":
            ax.set_extent(crop_bounds, crs=ccrs.PlateCarree())
        else:
            ax.set_xlim(crop_bounds[0], crop_bounds[1])
            ax.set_ylim(crop_bounds[2], crop_bounds[3])
        if show_labels:
            self._draw_crop_labels(ax, crop_bounds)
        if self.workflow_mode != "prototype_2016":
            ax.grid(alpha=0.32, color="#9ca3af", linewidth=0.6, linestyle="--", zorder=1)
        ax.set_title(panel_title, loc="left", fontsize=11.0, fontweight="bold")
        if show_xlabel:
            ax.set_xlabel("Longitude (°E)", fontsize=9.0)
        else:
            ax.set_xlabel("")
            ax.set_xticklabels([])
        if show_ylabel:
            ax.set_ylabel("Latitude (°N)", fontsize=9.0)
        else:
            ax.set_ylabel("")
            ax.set_yticklabels([])
        if row_label:
            ax.text(
                -0.14,
                0.5,
                row_label,
                transform=ax.transAxes,
                rotation=90,
                ha="center",
                va="center",
                fontsize=10.0,
                fontweight="bold",
                color="#0f172a",
            )
        for spine in ax.spines.values():
            spine.set_edgecolor("#111827")
            spine.set_linewidth(0.8)
        return {
            "geometry_render_mode": "exact_stored_raster",
            "density_render_mode": "direct_raster" if raster_cells_drawn else "omitted",
            "stored_geometry_status": "nonempty" if geometry_drawn else "empty_stored_artifact",
            "display_positive_cell_count": int(np.count_nonzero(display_info["mask"])),
            "geometry_positive_cell_count": int(np.count_nonzero(info["mask"])),
        }

    def _render_exact_raster_cells(
        self,
        ax: plt.Axes,
        *,
        display_info: dict[str, Any],
        model_name: str,
    ) -> bool:
        display_array = np.asarray(display_info["array"], dtype=float)
        display_array[~np.isfinite(display_array)] = 0.0
        display_array = np.clip(display_array, a_min=0.0, a_max=None)
        positive_mask = display_array > 0
        if not positive_mask.any():
            return False

        normalized = np.zeros_like(display_array, dtype=float)
        max_value = float(display_array[positive_mask].max())
        if max_value > 0.0:
            normalized[positive_mask] = display_array[positive_mask] / max_value
        else:
            normalized[positive_mask] = 1.0

        cmap = LinearSegmentedColormap.from_list(
            f"{model_name}_exact_raster_cells",
            [
                (0.0, (1.0, 1.0, 1.0, 0.0)),
                (0.45, to_rgba(MODEL_STYLES[model_name]["light_color"], alpha=0.34)),
                (1.0, to_rgba(MODEL_STYLES[model_name]["mid_color"], alpha=0.58)),
            ],
        )
        masked = np.ma.masked_where(~positive_mask, normalized)
        left, bottom, right, top = [float(value) for value in display_info["bounds"]]
        geographic_transform = ccrs.PlateCarree() if self.workflow_mode == "prototype_2016" else None
        imshow_kwargs = {"transform": geographic_transform} if geographic_transform is not None else {}
        ax.imshow(
            masked,
            extent=(left, right, bottom, top),
            origin="upper",
            cmap=cmap,
            interpolation="nearest",
            vmin=0.0,
            vmax=1.0,
            zorder=2,
            **imshow_kwargs,
        )
        if int(np.count_nonzero(positive_mask)) <= 196:
            for bounds in display_info.get("positive_cell_boxes") or []:
                ax.add_patch(
                    Rectangle(
                        (bounds[0], bounds[1]),
                        bounds[2] - bounds[0],
                        bounds[3] - bounds[1],
                        facecolor="none",
                        edgecolor=to_rgba(MODEL_STYLES[model_name]["mid_color"], alpha=0.78),
                        linewidth=0.42,
                        zorder=3,
                        **({"transform": geographic_transform} if geographic_transform is not None else {}),
                    )
                )
        return True

    def _single_note_lines(self, item: dict[str, Any], hour: int, model_name: str) -> list[str]:
        comparison_track_id = MODEL_NAME_TO_TRACK_ID.get(model_name, "deterministic")
        return [
            self._metrics_snippet(item, comparison_track_id, hour),
            "Stored forecast raster cells and exact footprint outlines are rendered directly. Empty stored layers are omitted.",
            "PyGNOME is comparator-only, not truth.",
        ]

    def _board_note_lines(self, item: dict[str, Any]) -> list[str]:
        if self.workflow_mode == "prototype_2016":
            lines = ["Rows show OpenDrift deterministic, p50, p90, and deterministic PyGNOME side by side for each benchmark hour."]
            for hour in REQUIRED_HOURS:
                lines.append(f"{hour} h deterministic: {self._metrics_snippet(item, 'deterministic', hour)}")
                lines.append(f"{hour} h p50: {self._metrics_snippet(item, 'ensemble_p50', hour)}")
                lines.append(f"{hour} h p90: {self._metrics_snippet(item, 'ensemble_p90', hour)}")
        else:
            lines = ["Each row is a paired deterministic benchmark snapshot on the same grid."]
            for hour in REQUIRED_HOURS:
                lines.append(f"{hour} h: {self._metrics_snippet(item, 'deterministic', hour)}")
        lines.extend(
            [
                "Stored forecast raster cells and exact footprint outlines are rendered directly. Empty stored layers are omitted.",
                self._context_note_sentence(),
                "PyGNOME is a comparator only, not truth.",
                f"{self._support_status_phrase().capitalize()}; not final Chapter 3 evidence.",
            ]
        )
        return lines

    def _render_single_figure(self, item: dict[str, Any], *, hour: int, model_name: str) -> dict[str, Any]:
        pair_row = self._pairing_row_for_model(item, hour, model_name)
        comparison_track_id = MODEL_NAME_TO_TRACK_ID.get(model_name, "deterministic")
        source_key = "opendrift_footprint_path_resolved" if model_name != "pygnome" else "pygnome_footprint_path_resolved"
        density_key = "opendrift_density_path_resolved" if model_name != "pygnome" else "pygnome_density_path_resolved"
        source_path = Path(pair_row[source_key])
        density_path = pair_row.get(density_key)
        source_point = self._resolve_case_source_point(item["case_id"])
        trajectory_points = self._trajectory_for_model(pair_row, model_name)
        plot_bounds = self._resolve_plot_bounds(
            base_bounds=item["display_bounds"],
            raster_paths=[
                source_path,
                Path(density_path) if density_path else None,
            ],
            source_point=source_point,
            trajectory_points=trajectory_points,
        )
        output_path = self.figures_dir / self._single_figure_filename(item["case_id"], hour, model_name)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        fig = plt.figure(figsize=SINGLE_FIGURE_SIZE, dpi=FIGURE_DPI, facecolor="#ffffff")
        grid = fig.add_gridspec(2, 1, height_ratios=[0.92, 0.08], left=0.07, right=0.98, top=0.90, bottom=0.06, hspace=0.04)
        if self.workflow_mode == "prototype_2016":
            map_ax = add_prototype_2016_geoaxes(
                fig,
                grid[0, 0].get_position(fig).bounds,
                plot_bounds,
                show_grid_labels=True,
                add_north_arrow=True,
            )
        else:
            map_ax = fig.add_subplot(grid[0, 0])
        footer_ax = fig.add_subplot(grid[1, 0])

        render_info = self._render_model_footprint(
            map_ax,
            raster_path=source_path,
            display_raster_path=Path(density_path) if density_path else None,
            crop_bounds=plot_bounds,
            model_name=model_name,
            panel_title=f"{MODEL_STYLES[model_name]['short_label']} footprint",
            source_point=source_point,
            trajectory_points=trajectory_points,
        )

        if self.workflow_mode == "prototype_2016":
            locator_ax = add_prototype_2016_geoaxes(
                fig,
                figure_relative_inset_rect(map_ax, [0.74, 0.74, 0.22, 0.22]),
                self._load_prototype_map_context()["full_bounds_wgs84"],
                show_grid_labels=False,
                add_scale_bar=False,
                add_north_arrow=False,
            )
        else:
            locator_ax = map_ax.inset_axes([0.74, 0.74, 0.22, 0.22])
        self._draw_locator(locator_ax, plot_bounds, item.get("display_bounds"))
        map_ax.legend(
            handles=self._single_legend_handles(
                model_name,
                include_source_point=source_point is not None,
                show_raster_cells=(render_info["density_render_mode"] == "direct_raster"),
                show_outline=(render_info["stored_geometry_status"] == "nonempty"),
            ),
            loc="upper center",
            bbox_to_anchor=(0.5, -0.085),
            frameon=True,
            framealpha=0.98,
            facecolor="#ffffff",
            edgecolor="#cbd5e1",
            fontsize=8.6,
            ncol=2,
        )
        map_ax.text(
            0.02,
            0.04,
            "\n".join(self._single_note_lines(item, hour, model_name)),
            transform=map_ax.transAxes,
            ha="left",
            va="bottom",
            fontsize=8.4,
            color="#334155",
            bbox={"boxstyle": "round,pad=0.36", "facecolor": (1, 1, 1, 0.96), "edgecolor": "#cbd5e1"},
            zorder=8,
        )
        footer_ax.axis("off")
        figure_title = f"{item['case_id']} | T+{hour} h"
        fig.suptitle(figure_title, x=0.07, y=0.965, ha="left", fontsize=19, fontweight="bold")
        fig.text(
            0.07,
            0.932,
            f"{MODEL_STYLES[model_name]['label']} | exact stored raster cells and footprint outlines | {self.workflow_mode} comparator-only",
            ha="left",
            va="top",
            fontsize=10,
            color="#475569",
        )
        pixel_width, pixel_height = self._figure_pixel_size(fig)
        fig.savefig(output_path, dpi=FIGURE_DPI)
        plt.close(fig)

        interpretation = (
            f"{self._workflow_label()} support figure for {item['case_id']} at {hour} h showing the "
            f"{MODEL_STYLES[model_name]['label']} exact benchmark footprint and direct stored raster cells over {self._context_phrase()}. "
            "PyGNOME remains comparator-only, not truth, and this is not final Chapter 3 evidence."
        )
        context = self._load_prototype_map_context()
        source_paths = [
            str(source_path.relative_to(self.repo_root)),
            str(Path(pair_row["qa_overlay_path_resolved"]).relative_to(self.repo_root)),
        ]
        for key in ("land_mask_path", "shoreline_path", "labels_path"):
            value = context.get(key)
            if value:
                source_paths.append(str(Path(value).relative_to(self.repo_root)))
        if density_path:
            source_paths.insert(1, str(Path(density_path).relative_to(self.repo_root)))
        if model_name == "pygnome" and self._has_path_value(pair_row.get("pygnome_nc_path")):
            source_paths.append(
                str(
                    self._resolve_repo_path(self._clean_optional_path_value(pair_row["pygnome_nc_path"])).relative_to(
                        self.repo_root
                    )
                )
            )
        if model_name == "opendrift" and self._has_path_value(pair_row.get("opendrift_nc_path")):
            source_paths.append(
                str(
                    self._resolve_repo_path(self._clean_optional_path_value(pair_row["opendrift_nc_path"])).relative_to(
                        self.repo_root
                    )
                )
            )
        source_note = "with a provenance source-point star" if source_point is not None else "without a provenance source-point star because no defensible source point was available"
        source_point_path = next((path for path in self._prototype_source_point_candidates(item["case_id"]) if path.exists()), None)
        if source_point_path is not None:
            source_paths.append(str(source_point_path.relative_to(self.repo_root)))
        return {
            **artifact_status_columns(
                {
                    "case_id": item["case_id"],
                    "phase_or_track": "prototype_pygnome_similarity_summary",
                    "run_type": "single_forecast",
                    "relative_path": str(output_path.relative_to(self.repo_root)),
                    "notes": f"{self._support_status_phrase()} transport comparator with {self._context_phrase()}.",
                    "short_plain_language_interpretation": interpretation,
                    "legacy_debug_only": self.workflow_mode == "prototype_2016",
                    "workflow_mode": self.workflow_mode,
                }
            ),
            "figure_id": output_path.stem,
            "case_id": item["case_id"],
            "phase_or_track": "prototype_pygnome_similarity_summary",
            "date_token": pd.Timestamp(str(pair_row["timestamp_utc"])).strftime("%Y-%m-%d"),
            "timestamp_utc": str(pair_row["timestamp_utc"]),
            "hour": hour,
            "model_name": model_name,
            "model_label": MODEL_STYLES[model_name]["label"],
            "run_type": "single_forecast",
            "view_type": "single",
            "variant": "paper",
            "figure_title": figure_title,
            "relative_path": str(output_path.relative_to(self.repo_root)),
            "file_path": str(output_path),
            "pixel_width": pixel_width,
            "pixel_height": pixel_height,
            "short_plain_language_interpretation": interpretation,
            "source_paths": " | ".join(dict.fromkeys(source_paths)),
            "notes": (
                f"{self._support_status_phrase().capitalize()} transport comparator with {self._context_phrase()}, "
                f"a small locator inset, exact stored-raster geometry rendering, and {source_note}. "
                f"{MODEL_STYLES[model_name]['label']} vs deterministic PyGNOME."
            ),
            "extent_mode": self.extent_mode,
            "plot_bounds_wgs84": ",".join(f"{value:.4f}" for value in plot_bounds),
            "comparison_track_id": comparison_track_id,
            "comparison_track_label": COMPARISON_TRACK_LABELS.get(comparison_track_id, MODEL_STYLES[model_name]["label"]),
            "legacy_debug_only": self.workflow_mode == "prototype_2016",
            "pygnome_role": "comparator_only",
            **render_info,
        }

    def _render_board_figure(self, item: dict[str, Any]) -> dict[str, Any]:
        output_path = self.figures_dir / self._board_figure_filename(item["case_id"])
        output_path.parent.mkdir(parents=True, exist_ok=True)
        source_point = self._resolve_case_source_point(item["case_id"])
        board_models = self._single_model_sequence()
        board_size = (15.2, 12.4) if len(board_models) > 2 else BOARD_FIGURE_SIZE
        fig = plt.figure(figsize=board_size, dpi=FIGURE_DPI, facecolor="#ffffff")
        grid = fig.add_gridspec(
            len(board_models) + 1,
            len(REQUIRED_HOURS),
            height_ratios=[1.0] * len(board_models) + [0.36],
            left=0.05,
            right=0.98,
            top=0.91,
            bottom=0.06,
            wspace=0.16,
            hspace=0.20,
        )

        source_paths: list[str] = []
        for row_idx, model_name in enumerate(board_models):
            comparison_track_id = MODEL_NAME_TO_TRACK_ID.get(model_name, "deterministic")
            for col_idx, hour in enumerate(REQUIRED_HOURS):
                pair_row = self._pairing_row_for_model(item, hour, model_name)
                if self.workflow_mode == "prototype_2016":
                    ax = add_prototype_2016_geoaxes(
                        fig,
                        grid[row_idx, col_idx].get_position(fig).bounds,
                        item["crop_bounds"],
                        show_grid_labels=False,
                        add_scale_bar=False,
                        add_north_arrow=False,
                    )
                else:
                    ax = fig.add_subplot(grid[row_idx, col_idx])
                source_key = "opendrift_footprint_path_resolved" if model_name != "pygnome" else "pygnome_footprint_path_resolved"
                density_key = "opendrift_density_path_resolved" if model_name != "pygnome" else "pygnome_density_path_resolved"
                source_path = Path(pair_row[source_key])
                density_path = pair_row.get(density_key)
                source_paths.append(str(source_path.relative_to(self.repo_root)))
                if density_path:
                    source_paths.append(str(Path(density_path).relative_to(self.repo_root)))
                if col_idx == 0:
                    source_paths.append(str(Path(pair_row["qa_overlay_path_resolved"]).relative_to(self.repo_root)))
                trajectory_points = self._trajectory_for_model(pair_row, model_name)
                if model_name == "pygnome" and self._has_path_value(pair_row.get("pygnome_nc_path")):
                    source_paths.append(
                        str(
                            self._resolve_repo_path(
                                self._clean_optional_path_value(pair_row["pygnome_nc_path"])
                            ).relative_to(self.repo_root)
                        )
                    )
                if model_name == "opendrift" and self._has_path_value(pair_row.get("opendrift_nc_path")):
                    source_paths.append(
                        str(
                            self._resolve_repo_path(
                                self._clean_optional_path_value(pair_row["opendrift_nc_path"])
                            ).relative_to(self.repo_root)
                        )
                    )
                self._render_model_footprint(
                    ax,
                    raster_path=source_path,
                    display_raster_path=Path(density_path) if density_path else None,
                    crop_bounds=item["crop_bounds"],
                    model_name=model_name,
                    panel_title=f"T+{hour} h" if row_idx == 0 else "",
                    row_label=MODEL_STYLES[model_name]["short_label"] if col_idx == 0 else "",
                    show_xlabel=row_idx == len(board_models) - 1,
                    show_ylabel=col_idx == 0,
                    show_labels=False,
                    source_point=source_point,
                    trajectory_points=trajectory_points,
                )
                ax.text(
                    0.02,
                    0.98,
                    self._metrics_snippet(item, comparison_track_id, hour),
                    transform=ax.transAxes,
                    ha="left",
                    va="top",
                    fontsize=7.0,
                    color="#334155",
                    bbox={"boxstyle": "round,pad=0.18", "facecolor": (1, 1, 1, 0.92), "edgecolor": "#cbd5e1"},
                )
                if row_idx == 0 and col_idx == len(REQUIRED_HOURS) - 1:
                    if self.workflow_mode == "prototype_2016":
                        locator_ax = add_prototype_2016_geoaxes(
                            fig,
                            figure_relative_inset_rect(ax, [0.62, 0.60, 0.34, 0.34]),
                            self._load_prototype_map_context()["full_bounds_wgs84"],
                            show_grid_labels=False,
                            add_scale_bar=False,
                            add_north_arrow=False,
                        )
                    else:
                        locator_ax = ax.inset_axes([0.62, 0.60, 0.34, 0.34])
                    self._draw_locator(locator_ax, item["crop_bounds"], item.get("display_bounds"))

        note_ax = fig.add_subplot(grid[len(board_models), :])
        self._draw_footer_note(note_ax, "Board reading guide", self._board_note_lines(item), width=190)
        figure_title = f"{item['case_id']} | 24/48/72 h legacy Phase 3A comparator board"
        fig.suptitle(figure_title, x=0.05, y=0.965, ha="left", fontsize=18, fontweight="bold")
        fig.text(
            0.05,
            0.932,
            f"{self.workflow_mode} | exact stored raster cells and footprint outlines | PyGNOME comparator-only",
            ha="left",
            va="top",
            fontsize=10,
            color="#475569",
        )
        pixel_width, pixel_height = self._figure_pixel_size(fig)
        fig.savefig(output_path, dpi=FIGURE_DPI)
        plt.close(fig)

        timestamps = [str(item["pairings_by_hour"]["deterministic"][hour]["timestamp_utc"]) for hour in REQUIRED_HOURS]
        interpretation = (
            f"{self._workflow_label()} support board for {item['case_id']} showing paired 24 h, 48 h, and 72 h "
            f"OpenDrift deterministic, OpenDrift p50, OpenDrift p90, and deterministic PyGNOME exact footprint views on the same benchmark grid with {self._context_phrase()} and locator context. "
            "PyGNOME remains comparator-only, not truth, and this is not final Chapter 3 evidence."
        )
        context = self._load_prototype_map_context()
        for key in ("land_mask_path", "shoreline_path", "labels_path"):
            value = context.get(key)
            if value:
                source_paths.append(str(Path(value).relative_to(self.repo_root)))
        source_note = "with a provenance source-point star" if source_point is not None else "without a provenance source-point star because no defensible source point was available"
        source_point_path = next((path for path in self._prototype_source_point_candidates(item["case_id"]) if path.exists()), None)
        if source_point_path is not None:
            source_paths.append(str(source_point_path.relative_to(self.repo_root)))
        return {
            **artifact_status_columns(
                {
                    "case_id": item["case_id"],
                    "phase_or_track": "prototype_pygnome_similarity_summary",
                    "run_type": "comparison_board",
                    "relative_path": str(output_path.relative_to(self.repo_root)),
                    "notes": f"{self._support_status_phrase()} transport comparator board with {self._context_phrase()}.",
                    "short_plain_language_interpretation": interpretation,
                    "legacy_debug_only": self.workflow_mode == "prototype_2016",
                    "workflow_mode": self.workflow_mode,
                }
            ),
            "figure_id": output_path.stem,
            "case_id": item["case_id"],
            "phase_or_track": "prototype_pygnome_similarity_summary",
            "date_token": f"{pd.Timestamp(timestamps[0]).strftime('%Y-%m-%d')}_to_{pd.Timestamp(timestamps[-1]).strftime('%Y-%m-%d')}",
            "timestamp_utc": ";".join(timestamps),
            "hour": "",
            "model_name": "opendrift_vs_pygnome",
            "model_label": "OpenDrift deterministic/p50/p90 vs PyGNOME deterministic",
            "comparison_track_id": ";".join(self._comparison_track_ids()),
            "comparison_track_label": ";".join(
                COMPARISON_TRACK_LABELS.get(track_id, track_id) for track_id in self._comparison_track_ids()
            ),
            "run_type": "comparison_board",
            "view_type": "board",
            "variant": "slide",
            "figure_title": figure_title,
            "relative_path": str(output_path.relative_to(self.repo_root)),
            "file_path": str(output_path),
            "pixel_width": pixel_width,
            "pixel_height": pixel_height,
            "short_plain_language_interpretation": interpretation,
            "source_paths": " | ".join(dict.fromkeys(source_paths)),
            "notes": f"{self._support_status_phrase().capitalize()} transport comparator board with {self._context_phrase()}, on-panel FSS/KL annotations, exact stored-raster geometry rendering, and {source_note}. Deterministic OpenDrift plus legacy support-only p50/p90 tracks vs deterministic PyGNOME.",
            "legacy_debug_only": self.workflow_mode == "prototype_2016",
            "pygnome_role": "comparator_only",
            "extent_mode": self.extent_mode,
            "plot_bounds_wgs84": ",".join(f"{value:.4f}" for value in item["crop_bounds"]),
            "geometry_render_mode": "exact_stored_raster",
            "density_render_mode": "direct_raster_or_omitted_per_panel",
            "stored_geometry_status": "mixed_panel_stored_artifacts",
        }

    def _render_forecast_figures(self, case_data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        figure_rows: list[dict[str, Any]] = []
        for item in case_data:
            for hour in REQUIRED_HOURS:
                for model_name in self._single_model_sequence():
                    figure_rows.append(self._render_single_figure(item, hour=hour, model_name=model_name))
            figure_rows.append(self._render_board_figure(item))
        return figure_rows

    def _write_fss_figure(self, fss_rows: list[dict[str, Any]]) -> Path:
        path = self.output_dir / "qa_prototype_pygnome_fss_by_case_window.png"
        df = pd.DataFrame(fss_rows).sort_values(["comparison_track_id", "window_km", "case_id"])
        windows = list(REQUIRED_WINDOWS_KM)
        series_keys = df[["case_id", "comparison_track_id", "comparison_track_label"]].drop_duplicates().to_dict("records")
        fig, ax = plt.subplots(figsize=(10.2, 5.6))
        x = np.arange(len(windows))
        width = max(0.08, 0.72 / max(len(series_keys), 1))
        for idx, series in enumerate(series_keys):
            case_df = df[
                (df["case_id"] == series["case_id"])
                & (df["comparison_track_id"] == series["comparison_track_id"])
            ].sort_values("window_km")
            ax.bar(
                x + (idx - (len(series_keys) - 1) / 2) * width,
                case_df["mean_fss"].tolist(),
                width=width,
                label=f"{series['case_id']} | {series['comparison_track_label']}",
            )
        ax.set_xticks(x)
        ax.set_xticklabels([f"{window} km" for window in windows])
        ax.set_ylabel("Mean FSS")
        ax.set_title(f"{self._workflow_label()} Phase 3A similarity by case, track, and window")
        ax.set_ylim(0.0, 1.0)
        ax.legend(loc="upper left", fontsize=7.8)
        ax.grid(axis="y", alpha=0.25)
        fig.tight_layout()
        fig.savefig(path, dpi=180)
        plt.close(fig)
        return path

    def _write_kl_figure(self, kl_rows: list[dict[str, Any]]) -> Path:
        path = self.output_dir / "qa_prototype_pygnome_kl_by_case_hour.png"
        df = pd.DataFrame(kl_rows).sort_values(["comparison_track_id", "case_id", "hour"])
        fig, ax = plt.subplots(figsize=(9, 5))
        for (case_id, comparison_track_id), case_df in df.groupby(["case_id", "comparison_track_id"]):
            case_df = case_df.sort_values("hour")
            track_label = str(case_df["comparison_track_label"].iloc[0])
            ax.plot(
                case_df["hour"].tolist(),
                case_df["kl_divergence"].tolist(),
                marker="o",
                linewidth=2.0,
                label=f"{case_id} | {track_label}",
            )
        ax.set_xticks(list(REQUIRED_HOURS))
        ax.set_xlabel("Snapshot hour")
        ax.set_ylabel("KL divergence")
        ax.set_title(f"{self._workflow_label()} Phase 3A KL divergence by case and track")
        ax.grid(alpha=0.25)
        ax.legend(loc="best", fontsize=7.8)
        fig.tight_layout()
        fig.savefig(path, dpi=180)
        plt.close(fig)
        return path

    def _write_scorecard_figure(self, similarity_rows: list[dict[str, Any]]) -> Path:
        path = self.output_dir / "qa_prototype_pygnome_scorecard.png"
        df = pd.DataFrame(similarity_rows).sort_values(["comparison_track_id", "relative_similarity_rank"])
        fig, ax = plt.subplots(figsize=(10, 2.2 + 0.55 * len(df)))
        ax.axis("off")
        table_rows = [
            [
                row["comparison_track_label"],
                int(row["relative_similarity_rank"]),
                row["case_id"],
                f"{float(row['mean_fss_5km']):.3f}",
                f"{float(row['mean_kl']):.3f}",
                int(row["pair_count"]),
            ]
            for _, row in df.iterrows()
        ]
        table = ax.table(
            cellText=table_rows,
            colLabels=["Track", "Rank", "Case", "Mean FSS @ 5 km", "Mean KL", "Pairs"],
            cellLoc="center",
            loc="center",
        )
        table.auto_set_font_size(False)
        table.set_fontsize(10)
        table.scale(1, 1.4)
        ax.set_title(
            f"{self._workflow_label()} transport benchmark scorecard\n"
            "Ranking rule: higher mean FSS @ 5 km, then lower mean KL, within each comparison track",
            pad=16,
        )
        fig.tight_layout()
        fig.savefig(path, dpi=180, bbox_inches="tight")
        plt.close(fig)
        return path

    def _build_summary_markdown(
        self,
        similarity_rows: list[dict[str, Any]],
        *,
        skipped_cases: list[dict[str, str]] | None = None,
    ) -> str:
        df = pd.DataFrame(similarity_rows).sort_values(["comparison_track_id", "relative_similarity_rank"])
        workflow_status = status_for_track_id(self.workflow_mode)
        skipped_cases = skipped_cases or []
        cohort_phrase = (
            "the configured accepted-segment 2021 deterministic OpenDrift control vs deterministic PyGNOME transport benchmarks"
            if self.workflow_mode == "prototype_2021"
            else "the three legacy 2016 deterministic, p50, and p90 OpenDrift transport benchmarks against deterministic PyGNOME"
        )
        lines = [
            f"# {self._workflow_label()} PyGNOME Similarity Summary",
            "",
            f"This package consolidates {cohort_phrase}.",
            "",
            "Guardrails:",
            "",
            f"- {self._support_status_phrase()}",
            "- transport benchmark only",
            "- PyGNOME is a comparator, not truth",
            "- not final Chapter 3 evidence",
            "",
            "Relative similarity ranking:",
            "",
        ]
        if workflow_status:
            lines.insert(7, f"- provenance: {workflow_status.provenance_label}")
            lines.insert(7, f"- status label: {workflow_status.label}")
        if skipped_cases:
            lines.extend(["", "Skipped cases:", ""])
            for skipped in skipped_cases:
                lines.append(
                    f"- `{skipped['case_id']}` skipped due to {skipped['error_type']}: {skipped['error_message']}"
                )
        for comparison_track_id in self._comparison_track_ids():
            track_df = df[df["comparison_track_id"] == comparison_track_id]
            if track_df.empty:
                continue
            lines.append(f"- `{COMPARISON_TRACK_LABELS.get(comparison_track_id, comparison_track_id)}`:")
            for _, row in track_df.iterrows():
                lines.append(
                    f"  Rank {int(row['relative_similarity_rank'])}: `{row['case_id']}` | "
                    f"mean FSS @ 5 km = {float(row['mean_fss_5km']):.3f}, "
                    f"mean KL = {float(row['mean_kl']):.3f}, "
                    f"pairs = {int(row['pair_count'])}"
                )
        lines.extend(["", "Per-case snapshot highlights:", ""])
        for _, row in df.iterrows():
            lines.append(
                f"- `{row['case_id']}` / `{row['comparison_track_label']}`: "
                f"FSS @ 5 km (24/48/72 h) = "
                f"{float(row['fss_5km_24h']):.3f} / {float(row['fss_5km_48h']):.3f} / {float(row['fss_5km_72h']):.3f}; "
                f"KL (24/48/72 h) = "
                f"{float(row['kl_24h']):.3f} / {float(row['kl_48h']):.3f} / {float(row['kl_72h']):.3f}"
            )
        lines.extend(
            [
                "",
                "Interpretation:",
                "",
                "- Higher FSS means stronger footprint overlap between the named OpenDrift track and deterministic PyGNOME.",
                "- Lower KL means the normalized density fields are more similar over the ocean cells.",
                f"- The ranking is relative within each comparison track inside the {self.workflow_mode} support set only.",
                f"- The per-forecast figures under `figures/` are support visuals built from the stored benchmark rasters only, now shown with exact stored raster cells and exact footprint outlines over {self._context_phrase()}, with a provenance source-point star when available.",
            ]
        )
        return "\n".join(lines)

    def _build_figure_captions_markdown(
        self,
        figure_rows: list[dict[str, Any]],
        *,
        case_ids: list[str] | None = None,
        skipped_cases: list[dict[str, str]] | None = None,
    ) -> str:
        skipped_cases = skipped_cases or []
        ordered_case_ids = case_ids or self.case_ids
        lines = [
            f"# {self._workflow_label()} Forecast Figure Captions",
            "",
            f"These figures are {self._support_status_phrase()} transport benchmark support visuals built from the stored benchmark rasters, with {self._context_phrase()}, a small locator inset, exact stored raster cells, exact footprint outlines, and a provenance source-point star when that asset is available.",
            "",
            "Guardrails:",
            "",
            "- PyGNOME is comparator-only, not truth",
            "- transport benchmark only",
            "- not final Chapter 3 evidence",
            "",
        ]
        figure_df = pd.DataFrame(figure_rows).sort_values(["case_id", "view_type", "hour", "model_name"])
        for case_id in ordered_case_ids:
            lines.append(f"## {case_id}")
            lines.append("")
            case_df = figure_df[figure_df["case_id"] == case_id]
            for _, row in case_df.iterrows():
                label = "board" if row["view_type"] == "board" else f"{row['hour']} h {row['model_label']}"
                status_label = str(row.get("status_label") or "").strip()
                provenance = str(row.get("status_provenance") or "").strip()
                status_text = f" [{status_label}]" if status_label else ""
                provenance_text = f" Provenance: {provenance}" if provenance else ""
                lines.append(
                    f"- `{row['figure_id']}` ({label}){status_text}: {row['short_plain_language_interpretation']}{provenance_text}"
                )
            lines.append("")
        if skipped_cases:
            lines.extend(["## Skipped Cases", ""])
            for skipped in skipped_cases:
                lines.append(
                    f"- `{skipped['case_id']}` skipped due to {skipped['error_type']}: {skipped['error_message']}"
                )
            lines.append("")
        return "\n".join(lines)

    def run(self) -> dict[str, Any]:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.figures_dir.mkdir(parents=True, exist_ok=True)
        case_data, skipped_cases = self._load_available_case_artifacts()
        if not case_data:
            failure_summary = "; ".join(
                f"{item['case_id']}: {item['error_type']}: {item['error_message']}"
                for item in skipped_cases
            )
            raise ValueError(
                "Prototype PyGNOME similarity summary could not find any complete benchmark cases. "
                f"Configured cases={self.case_ids}. "
                f"Failures={failure_summary}"
            )
        case_registry_rows = self._build_case_registry_rows(case_data)
        fss_rows = self._build_fss_rows(case_data)
        kl_rows = self._build_kl_rows(case_data)
        similarity_rows = self._build_similarity_rows(case_data, fss_rows, kl_rows)
        figure_rows = self._render_forecast_figures(case_data)
        processed_case_ids = [item["case_id"] for item in case_data]

        case_registry_csv = self.output_dir / "prototype_pygnome_case_registry.csv"
        similarity_by_case_csv = self.output_dir / "prototype_pygnome_similarity_by_case.csv"
        fss_by_case_window_csv = self.output_dir / "prototype_pygnome_fss_by_case_window.csv"
        kl_by_case_hour_csv = self.output_dir / "prototype_pygnome_kl_by_case_hour.csv"
        figure_registry_csv = self.output_dir / "prototype_pygnome_figure_registry.csv"
        skipped_cases_csv = self.output_dir / "prototype_pygnome_skipped_cases.csv"
        manifest_json = self.output_dir / "prototype_pygnome_similarity_manifest.json"
        summary_md = self.output_dir / "prototype_pygnome_similarity_summary.md"
        figure_captions_md = self.output_dir / "prototype_pygnome_figure_captions.md"

        _write_csv(case_registry_csv, case_registry_rows)
        _write_csv(similarity_by_case_csv, similarity_rows)
        _write_csv(fss_by_case_window_csv, fss_rows)
        _write_csv(kl_by_case_hour_csv, kl_rows)
        _write_csv(skipped_cases_csv, skipped_cases, columns=["case_id", "error_type", "error_message"])
        _write_csv(
            figure_registry_csv,
            figure_rows,
            columns=[
                "figure_id",
                "case_id",
                "phase_or_track",
                "date_token",
                "timestamp_utc",
                "hour",
                "model_name",
                "model_label",
                "comparison_track_id",
                "comparison_track_label",
                "run_type",
                "view_type",
                "variant",
                "figure_title",
                "relative_path",
                "file_path",
                "pixel_width",
                "pixel_height",
                "short_plain_language_interpretation",
                "source_paths",
                "notes",
                "extent_mode",
                "plot_bounds_wgs84",
                "geometry_render_mode",
                "density_render_mode",
                "stored_geometry_status",
                "legacy_debug_only",
                "pygnome_role",
                "status_key",
                "status_label",
                "status_role",
                "status_reportability",
                "status_official_status",
                "status_frozen_status",
                "status_provenance",
                "status_panel_text",
                "status_dashboard_summary",
            ],
        )

        fss_figure = self._write_fss_figure(fss_rows)
        kl_figure = self._write_kl_figure(kl_rows)
        scorecard_figure = self._write_scorecard_figure(similarity_rows)
        _write_text(summary_md, self._build_summary_markdown(similarity_rows, skipped_cases=skipped_cases))
        _write_text(
            figure_captions_md,
            self._build_figure_captions_markdown(
                figure_rows,
                case_ids=processed_case_ids,
                skipped_cases=skipped_cases,
            ),
        )

        deterministic_rows = [
            row for row in similarity_rows
            if str(row.get("comparison_track_id") or "") == "deterministic"
        ]
        ranking_source_rows = deterministic_rows or similarity_rows
        top_case = min(ranking_source_rows, key=lambda row: int(row["relative_similarity_rank"]))
        per_case_rendering = {
            item["case_id"]: prototype_2016_rendering_metadata(item["crop_bounds"])
            for item in case_data
        }
        manifest = {
            "phase": PHASE,
            "workflow_mode": self.workflow_mode,
            "generated_at_utc": _utc_now_iso(),
            "output_root": str(self.output_dir.relative_to(self.repo_root)),
            "configured_case_ids": self.case_ids,
            "processed_case_ids": processed_case_ids,
            "comparison_scope": (
                "deterministic_p50_p90_opendrift_vs_deterministic_pygnome_transport_benchmark"
                if self.workflow_mode == "prototype_2016"
                else "deterministic_opendrift_control_vs_deterministic_pygnome_transport_benchmark"
            ),
            "pygnome_role": "comparator_only",
            "legacy_debug_only": self.workflow_mode == "prototype_2016",
            "final_chapter3_evidence": False,
            "support_context_mode": self.support_context_mode,
            "extent_modes_supported": [
                PROTOTYPE_2016_EXTENT_MODE_FIXED_REGIONAL,
                PROTOTYPE_2016_EXTENT_MODE_DYNAMIC_FORECAST,
            ],
            "default_extent_mode": self.extent_mode,
            "rendering_profile": "prototype_2016_case_local_projected_v1" if self.workflow_mode == "prototype_2016" else "",
            "map_projection": "local_azimuthal_equidistant" if self.workflow_mode == "prototype_2016" else "",
            "common_locator_bounds_wgs84": list(self._load_prototype_map_context()["full_bounds_wgs84"]),
            "case_rendering": per_case_rendering,
            "ranking_rule": "higher mean FSS @ 5 km, then lower mean KL, within each comparison track",
            "required_artifacts_per_case": list(REQUIRED_BENCHMARK_FILES.values()),
            "headline": {
                "top_ranked_case_id": top_case["case_id"],
                "top_ranked_comparison_track_id": top_case.get("comparison_track_id", "deterministic"),
                "top_ranked_mean_fss_5km": float(top_case["mean_fss_5km"]),
                "top_ranked_mean_kl": float(top_case["mean_kl"]),
            },
            "outputs": {
                "case_registry_csv": str(case_registry_csv.relative_to(self.repo_root)),
                "similarity_by_case_csv": str(similarity_by_case_csv.relative_to(self.repo_root)),
                "fss_by_case_window_csv": str(fss_by_case_window_csv.relative_to(self.repo_root)),
                "kl_by_case_hour_csv": str(kl_by_case_hour_csv.relative_to(self.repo_root)),
                "summary_md": str(summary_md.relative_to(self.repo_root)),
                "figure_registry_csv": str(figure_registry_csv.relative_to(self.repo_root)),
                "figure_captions_md": str(figure_captions_md.relative_to(self.repo_root)),
                "figures_dir": str(self.figures_dir.relative_to(self.repo_root)),
                "skipped_cases_csv": str(skipped_cases_csv.relative_to(self.repo_root)),
            },
            "figure_counts": {
                "single_forecast_figures": int(sum(1 for row in figure_rows if row["view_type"] == "single")),
                "comparison_boards": int(sum(1 for row in figure_rows if row["view_type"] == "board")),
            },
            "qa_figures": [
                str(fss_figure.relative_to(self.repo_root)),
                str(kl_figure.relative_to(self.repo_root)),
                str(scorecard_figure.relative_to(self.repo_root)),
            ],
            "forecast_figures": [row["relative_path"] for row in figure_rows],
            "skipped_cases": skipped_cases,
        }
        _write_json(manifest_json, manifest)

        return {
            "output_dir": str(self.output_dir),
            "case_registry_csv": str(case_registry_csv),
            "similarity_by_case_csv": str(similarity_by_case_csv),
            "fss_by_case_window_csv": str(fss_by_case_window_csv),
            "kl_by_case_hour_csv": str(kl_by_case_hour_csv),
            "figure_registry_csv": str(figure_registry_csv),
            "figure_captions_md": str(figure_captions_md),
            "skipped_cases_csv": str(skipped_cases_csv),
            "manifest_json": str(manifest_json),
            "summary_md": str(summary_md),
            "qa_figures": [str(fss_figure), str(kl_figure), str(scorecard_figure)],
            "forecast_figure_paths": [row["file_path"] for row in figure_rows],
            "single_figure_count": int(sum(1 for row in figure_rows if row["view_type"] == "single")),
            "board_figure_count": int(sum(1 for row in figure_rows if row["view_type"] == "board")),
            "top_ranked_case_id": top_case["case_id"],
            "top_ranked_comparison_track_id": str(top_case.get("comparison_track_id", "deterministic")),
            "top_ranked_mean_fss_5km": float(top_case["mean_fss_5km"]),
            "top_ranked_mean_kl": float(top_case["mean_kl"]),
            "case_count": len(case_data),
            "configured_case_count": len(self.case_ids),
            "processed_case_ids": processed_case_ids,
            "skipped_cases": skipped_cases,
        }


def run_prototype_pygnome_similarity_summary(**kwargs: Any) -> dict[str, Any]:
    return PrototypePygnomeSimilaritySummaryService(**kwargs).run()
