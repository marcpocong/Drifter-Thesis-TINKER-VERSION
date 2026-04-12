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
import yaml
from rasterio import features as raster_features
from shapely.geometry import shape as shapely_shape

matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.colors import to_rgba
from matplotlib.lines import Line2D
from matplotlib.patches import Patch, Polygon, Rectangle

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
    "control_footprint_path",
    "pygnome_footprint_path",
    "qa_overlay_path",
)
DOMAIN_BOUNDS = (115.0, 122.0, 6.0, 14.5)
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
    "pygnome": {
        "color": "#9b4dca",
        "mid_color": "#b47ad7",
        "light_color": "#d8afea",
        "label": "PyGNOME deterministic",
        "short_label": "PyGNOME",
    },
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
        self.support_context_mode = str(workflow_config.get("support_context_mode") or "mindoro_canonical")
        self.case_metadata_by_id = {
            str(case_id): dict(payload or {})
            for case_id, payload in (workflow_config.get("case_metadata_by_id") or {}).items()
        }
        self._raster_cache: dict[Path, dict[str, Any]] = {}
        self._vector_cache: dict[tuple[Path, str], gpd.GeoDataFrame | None] = {}
        self._prototype_map_context: dict[str, Any] | None = None

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
                "domain_bounds": tuple(cfg.get("region") or DOMAIN_BOUNDS),
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
            "domain_bounds": tuple(settings.get("region") or DOMAIN_BOUNDS),
            "support_context_mode": "mindoro_canonical",
            "case_metadata_by_id": {},
        }

    def _workflow_label(self) -> str:
        return "Prototype 2021" if self.workflow_mode == "prototype_2021" else "Prototype 2016"

    def _workflow_token(self) -> str:
        return _safe_token(self.workflow_mode)

    def _context_phrase(self) -> str:
        if self.support_context_mode == "mindoro_canonical":
            return "canonical Mindoro land/shoreline context"
        return "neutral case-local geographic context"

    def _support_status_phrase(self) -> str:
        if self.workflow_mode == "prototype_2021":
            return "accepted-segment debug support only"
        return "legacy/debug support only"

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

    def _load_prototype_map_context(self) -> dict[str, Any]:
        if self._prototype_map_context is not None:
            return self._prototype_map_context

        if self.support_context_mode != "mindoro_canonical":
            self._prototype_map_context = {
                "land_mask_path": None,
                "shoreline_path": None,
                "labels_path": None,
                "labels_df": pd.DataFrame(columns=["label_text", "lon", "lat", "enabled_yes_no"]),
                "full_bounds_wgs84": tuple(float(value) for value in self.domain_bounds),
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
        path = Path(value)
        if not path.is_absolute():
            path = self.repo_root / path
        return path.resolve()

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

    def _resolve_case_source_point(self, case_id: str) -> tuple[float, float] | None:
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

    def _compute_case_crop_bounds(self, pairings_by_hour: dict[int, dict[str, Any]]) -> tuple[float, float, float, float]:
        x_values: list[float] = []
        y_values: list[float] = []
        cell_widths: list[float] = []
        cell_heights: list[float] = []
        for hour in REQUIRED_HOURS:
            row = pairings_by_hour[hour]
            for key in ("control_footprint_path", "pygnome_footprint_path"):
                info = self._load_raster_mask(row[f"{key}_resolved"])
                positive_cell_bounds = info.get("positive_cell_bounds")
                if positive_cell_bounds is None:
                    continue
                x_values.extend([positive_cell_bounds[0], positive_cell_bounds[2]])
                y_values.extend([positive_cell_bounds[1], positive_cell_bounds[3]])
                cell_widths.append(float(info["cell_width"]))
                cell_heights.append(float(info["cell_height"]))
        if not x_values or not y_values:
            return DOMAIN_BOUNDS
        cell_width = max(cell_widths) if cell_widths else 0.05
        cell_height = max(cell_heights) if cell_heights else 0.05
        x_min = min(x_values)
        x_max = max(x_values)
        y_min = min(y_values)
        y_max = max(y_values)
        x_min = max(DOMAIN_BOUNDS[0], x_min - (cell_width * PROTOTYPE_CROP_PAD_CELLS))
        x_max = min(DOMAIN_BOUNDS[1], x_max + (cell_width * PROTOTYPE_CROP_PAD_CELLS))
        y_min = max(DOMAIN_BOUNDS[2], y_min - (cell_height * PROTOTYPE_CROP_PAD_CELLS))
        y_max = min(DOMAIN_BOUNDS[3], y_max + (cell_height * PROTOTYPE_CROP_PAD_CELLS))
        x_min, x_max = self._apply_minimum_crop_span(
            x_min,
            x_max,
            minimum_span=cell_width * PROTOTYPE_MIN_CROP_CELLS_X,
            domain_min=DOMAIN_BOUNDS[0],
            domain_max=DOMAIN_BOUNDS[1],
        )
        y_min, y_max = self._apply_minimum_crop_span(
            y_min,
            y_max,
            minimum_span=cell_height * PROTOTYPE_MIN_CROP_CELLS_Y,
            domain_min=DOMAIN_BOUNDS[2],
            domain_max=DOMAIN_BOUNDS[3],
        )
        return (x_min, x_max, y_min, y_max)

    def _build_pairings_by_hour(self, case_id: str, pairing_df: pd.DataFrame) -> dict[int, dict[str, Any]]:
        missing_columns = [column for column in REQUIRED_PAIRING_COLUMNS if column not in pairing_df.columns]
        if missing_columns:
            raise ValueError(
                f"Prototype benchmark pairing manifest for {case_id} is missing columns: {', '.join(missing_columns)}"
            )
        pairings: dict[int, dict[str, Any]] = {}
        for hour in REQUIRED_HOURS:
            subset = pairing_df[pairing_df["hour"].astype(int) == hour]
            if subset.empty:
                raise ValueError(f"Prototype benchmark pairing manifest for {case_id} is missing the {hour} h snapshot row.")
            row = subset.iloc[0].to_dict()
            row["hour"] = int(row["hour"])
            for key in ("control_footprint_path", "pygnome_footprint_path", "qa_overlay_path"):
                resolved = self._resolve_repo_path(row[key])
                row[f"{key}_resolved"] = resolved
                if not resolved.exists():
                    raise FileNotFoundError(
                        "Prototype PyGNOME similarity summary requires per-hour benchmark artifacts for every configured 2016 case. "
                        f"Missing {key} for {case_id} hour {hour}: {resolved}"
                    )
            for key in ("control_density_path", "pygnome_density_path"):
                raw_value = row.get(key)
                resolved_key = f"{key}_resolved"
                if raw_value:
                    resolved = self._resolve_repo_path(raw_value)
                    row[resolved_key] = resolved if resolved.exists() else None
                else:
                    row[resolved_key] = None
            pairings[hour] = row
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

        missing_windows = [window for window in REQUIRED_WINDOWS_KM if window not in set(fss_df["window_km"].astype(int).tolist())]
        missing_fss_hours = [hour for hour in REQUIRED_HOURS if hour not in set(fss_df["hour"].astype(int).tolist())]
        missing_kl_hours = [hour for hour in REQUIRED_HOURS if hour not in set(kl_df["hour"].astype(int).tolist())]
        if missing_windows or missing_fss_hours or missing_kl_hours:
            raise ValueError(
                f"Prototype benchmark artifacts for {case_id} are incomplete. "
                f"Missing windows={missing_windows}, missing_fss_hours={missing_fss_hours}, missing_kl_hours={missing_kl_hours}"
            )

        pairings_by_hour = self._build_pairings_by_hour(case_id, pairing_df)

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
            "crop_bounds": self._compute_case_crop_bounds(pairings_by_hour),
        }

    def _build_case_registry_rows(self, case_data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for item in case_data:
            metadata = item["metadata"]
            fss_df = item["fss_df"]
            crop_bounds = item["crop_bounds"]
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
                    "case_crop_bounds": ",".join(f"{value:.4f}" for value in crop_bounds),
                    "pygnome_weathering_enabled": _coerce_bool(metadata.get("weathering_enabled")),
                    "pygnome_role": "comparator_only",
                    "benchmark_particles": int(metadata.get("benchmark_particles", 0) or 0),
                    "notes": "Legacy prototype transport benchmark. Deterministic OpenDrift control is compared to deterministic PyGNOME only.",
                }
            )
        return rows

    def _build_fss_rows(self, case_data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for item in case_data:
            fss_df = item["fss_df"].copy()
            fss_df["window_km"] = fss_df["window_km"].astype(int)
            fss_df["hour"] = fss_df["hour"].astype(int)
            for window in REQUIRED_WINDOWS_KM:
                subset = fss_df[fss_df["window_km"] == window].sort_values("hour")
                row = {
                    "case_id": item["case_id"],
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
            kl_df = kl_df.sort_values("hour")
            for _, record in kl_df.iterrows():
                rows.append(
                    {
                        "case_id": item["case_id"],
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
            case_fss = fss_df[fss_df["case_id"] == case_id].copy()
            case_kl = kl_df[kl_df["case_id"] == case_id].copy().sort_values("hour")
            fss_by_window = {int(row["window_km"]): row for _, row in case_fss.iterrows()}
            kl_by_hour = {int(row["hour"]): row for _, row in case_kl.iterrows()}
            row = {
                "case_id": case_id,
                "pair_count": int(len(item["pairing_df"])),
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
            by=["mean_fss_5km", "mean_kl", "case_id"],
            ascending=[False, True, True],
        ).reset_index(drop=True)
        similarity_df["relative_similarity_rank"] = np.arange(1, len(similarity_df) + 1)
        return similarity_df.to_dict(orient="records")

    def _fss_values_by_window(self, item: dict[str, Any], hour: int) -> dict[int, float]:
        subset = item["fss_df"][item["fss_df"]["hour"].astype(int) == int(hour)].copy()
        subset["window_km"] = subset["window_km"].astype(int)
        return {int(row["window_km"]): float(row["fss"]) for _, row in subset.iterrows()}

    def _kl_value(self, item: dict[str, Any], hour: int) -> float:
        subset = item["kl_df"][item["kl_df"]["hour"].astype(int) == int(hour)]
        return float(subset.iloc[0]["kl_divergence"])

    def _metrics_snippet(self, item: dict[str, Any], hour: int) -> str:
        fss_values = self._fss_values_by_window(item, hour)
        fss_line = "/".join(f"{fss_values[window]:.3f}" for window in REQUIRED_WINDOWS_KM)
        return f"FSS 1/3/5/10 km = {fss_line}; KL = {self._kl_value(item, hour):.3f}."

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
            )

    def _draw_locator(self, ax: plt.Axes, crop_bounds: tuple[float, float, float, float]) -> None:
        context = self._load_prototype_map_context()
        shoreline_path = context.get("shoreline_path")
        shoreline = self._load_vector(Path(shoreline_path), "EPSG:4326") if shoreline_path else None
        labels_df = context["labels_df"]
        full_bounds = context["full_bounds_wgs84"]
        ax.set_facecolor(MAP_WATER_COLOR)
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
            )
        )
        lon_pad = max((full_bounds[1] - full_bounds[0]) * PROTOTYPE_LOCATOR_PADDING_FRACTION, 0.08)
        lat_pad = max((full_bounds[3] - full_bounds[2]) * PROTOTYPE_LOCATOR_PADDING_FRACTION, 0.08)
        ax.set_xlim(full_bounds[0] - lon_pad, full_bounds[1] + lon_pad)
        ax.set_ylim(full_bounds[2] - lat_pad, full_bounds[3] + lat_pad)
        center_lat = ((full_bounds[2] - lat_pad) + (full_bounds[3] + lat_pad)) / 2.0
        ax.set_aspect(self._geographic_aspect(center_lat), adjustable="box")
        ax.set_title("Locator", fontsize=9.2, loc="left", pad=4)
        ax.set_xticks([])
        ax.set_yticks([])
        for spine in ax.spines.values():
            spine.set_edgecolor("#94a3b8")
            spine.set_linewidth(0.8)

    def _single_legend_handles(self, model_name: str, *, include_source_point: bool) -> list[Any]:
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
        handles.extend(
            [
                Patch(
                    facecolor=to_rgba(MODEL_STYLES[model_name]["mid_color"], alpha=0.46),
                    edgecolor="none",
                    label="Higher-density core",
                ),
                Patch(
                    facecolor=to_rgba(MODEL_STYLES[model_name]["light_color"], alpha=0.26),
                    edgecolor="none",
                    label="Broader support envelope",
                ),
                Line2D(
                    [0],
                    [0],
                    color=MODEL_STYLES[model_name]["color"],
                    lw=1.4,
                    label="Exact deterministic footprint outline",
                ),
            ]
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
                Patch(facecolor=to_rgba("#94a3b8", alpha=0.48), edgecolor="none", label="Higher-density core"),
                Patch(facecolor=to_rgba("#cbd5e1", alpha=0.60), edgecolor="none", label="Broader support envelope"),
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
            (
                "Map context reuses the stored canonical Mindoro scoring-grid assets."
                if self.support_context_mode == "mindoro_canonical"
                else "Map context is neutral/case-local because this preferred 2021 debug lane is not tied to the old Mindoro prototype geography."
            ),
            "The inner core and outer envelope come from the stored deterministic normalized density raster. The dark outline remains the exact benchmark footprint.",
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
    ) -> None:
        info = self._load_raster_mask(raster_path)
        display_info = self._load_raster_mask(display_raster_path or raster_path)
        ax.set_facecolor(MAP_WATER_COLOR)
        self._draw_context_layers(ax)

        density_surface = self._build_density_surface(display_info, crop_bounds)
        if density_surface is not None:
            level_info = self._density_envelope_levels(density_surface["surface"])
            if level_info is not None:
                x_grid = density_surface["x"]
                y_grid = density_surface["y"]
                surface = density_surface["surface"]
                eps = level_info["epsilon"]
                ax.contourf(
                    x_grid,
                    y_grid,
                    surface,
                    levels=[level_info["outer_level"], level_info["max_level"] + eps],
                    colors=[to_rgba(MODEL_STYLES[model_name]["light_color"], alpha=0.26)],
                    antialiased=True,
                    zorder=2,
                )
                ax.contour(
                    x_grid,
                    y_grid,
                    surface,
                    levels=[level_info["outer_level"]],
                    colors=[MODEL_STYLES[model_name]["mid_color"]],
                    linewidths=1.2,
                    linestyles="--",
                    zorder=3,
                )
                ax.contourf(
                    x_grid,
                    y_grid,
                    surface,
                    levels=[level_info["core_level"], level_info["max_level"] + eps],
                    colors=[to_rgba(MODEL_STYLES[model_name]["mid_color"], alpha=0.48)],
                    antialiased=True,
                    zorder=4,
                )
                ax.contour(
                    x_grid,
                    y_grid,
                    surface,
                    levels=[level_info["core_level"]],
                    colors=[MODEL_STYLES[model_name]["color"]],
                    linewidths=1.7,
                    zorder=5,
                )
        positive_cell_bounds = info.get("positive_cell_bounds")
        polygons = info.get("footprint_polygons") or []
        if polygons:
            for coordinates in polygons:
                ax.add_patch(
                    Polygon(
                        coordinates,
                        closed=True,
                        facecolor="none",
                        edgecolor=MODEL_STYLES[model_name]["color"],
                        linewidth=1.2,
                        alpha=0.98,
                        joinstyle="round",
                        zorder=6,
                    )
                )
        elif positive_cell_bounds is not None:
            ax.add_patch(
                Rectangle(
                    (positive_cell_bounds[0], positive_cell_bounds[1]),
                    positive_cell_bounds[2] - positive_cell_bounds[0],
                    positive_cell_bounds[3] - positive_cell_bounds[1],
                    facecolor="none",
                    edgecolor=MODEL_STYLES[model_name]["color"],
                    linewidth=1.1,
                    linestyle="-",
                    alpha=0.82,
                    zorder=6,
                    )
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
            )
        ax.set_xlim(crop_bounds[0], crop_bounds[1])
        ax.set_ylim(crop_bounds[2], crop_bounds[3])
        if show_labels:
            self._draw_crop_labels(ax, crop_bounds)
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

    def _build_density_surface(
        self,
        display_info: dict[str, Any],
        crop_bounds: tuple[float, float, float, float],
    ) -> dict[str, np.ndarray] | None:
        display_array = np.array(display_info["array"], dtype=float)
        display_array[~np.isfinite(display_array)] = 0.0
        display_array = np.clip(display_array, a_min=0.0, a_max=None)
        positive_mask = display_array > 0
        if not positive_mask.any():
            return None

        left, bottom, right, top = [float(value) for value in display_info["bounds"]]
        rows, cols = display_array.shape
        cell_width = float(display_info["cell_width"])
        cell_height = float(display_info["cell_height"])
        x_centers = left + ((np.arange(cols) + 0.5) * cell_width)
        y_centers = top - ((np.arange(rows) + 0.5) * cell_height)
        pos_rows, pos_cols = np.where(positive_mask)
        sample_x = x_centers[pos_cols]
        sample_y = y_centers[pos_rows]
        sample_values = display_array[positive_mask]

        x_min, x_max, y_min, y_max = crop_bounds
        x_span = max(x_max - x_min, cell_width * PROTOTYPE_MIN_CROP_CELLS_X)
        y_span = max(y_max - y_min, cell_height * PROTOTYPE_MIN_CROP_CELLS_Y)
        base_grid = max(PROTOTYPE_DENSITY_GRID_MIN, int(round(max(x_span / cell_width, y_span / cell_height) * 24)))
        grid_width = min(PROTOTYPE_DENSITY_GRID_MAX, max(PROTOTYPE_DENSITY_GRID_MIN, base_grid))
        aspect_ratio = y_span / max(x_span, 1.0e-9)
        grid_height = min(PROTOTYPE_DENSITY_GRID_MAX, max(PROTOTYPE_DENSITY_GRID_MIN, int(round(grid_width * aspect_ratio))))

        x_grid = np.linspace(x_min, x_max, grid_width)
        y_grid = np.linspace(y_min, y_max, grid_height)
        mesh_x, mesh_y = np.meshgrid(x_grid, y_grid)

        sigma_x = max(cell_width * 1.75, x_span / 26.0)
        sigma_y = max(cell_height * 1.75, y_span / 26.0)
        surface = np.zeros_like(mesh_x, dtype=float)
        for center_x, center_y, value in zip(sample_x, sample_y, sample_values):
            surface += value * np.exp(
                -(
                    ((mesh_x - center_x) ** 2) / (2.0 * (sigma_x**2))
                    + ((mesh_y - center_y) ** 2) / (2.0 * (sigma_y**2))
                )
            )

        max_surface = float(np.nanmax(surface))
        if max_surface <= 0.0:
            return None
        surface = surface / max_surface
        return {"x": x_grid, "y": y_grid, "surface": surface}

    def _density_envelope_levels(self, surface: np.ndarray) -> dict[str, float] | None:
        finite = np.asarray(surface, dtype=float)
        finite = finite[np.isfinite(finite) & (finite > 0)]
        if finite.size == 0:
            return None
        ranked = np.sort(finite)[::-1]
        cumulative = np.cumsum(ranked)
        total = float(cumulative[-1])
        if total <= 0.0:
            return None
        max_level = float(ranked[0])
        eps = max(max_level * 1.0e-6, 1.0e-6)
        outer_idx = min(int(np.searchsorted(cumulative, total * 0.90)), len(ranked) - 1)
        core_idx = min(int(np.searchsorted(cumulative, total * 0.50)), len(ranked) - 1)
        outer_level = min(float(ranked[outer_idx]), max_level - (eps * 2.0))
        core_level = min(float(ranked[core_idx]), max_level - eps)
        if core_level <= outer_level:
            outer_level = max(max_level * 0.34, eps)
            core_level = max(max_level * 0.60, outer_level + eps)
        return {
            "outer_level": outer_level,
            "core_level": core_level,
            "max_level": max_level,
            "epsilon": eps,
        }

    def _single_note_lines(self, item: dict[str, Any], hour: int, model_name: str) -> list[str]:
        return [
            self._metrics_snippet(item, hour),
            "Inner core and outer envelope are presentation-only shapes derived from the stored deterministic normalized density raster.",
            "The dark outline remains the exact benchmark footprint. PyGNOME is comparator-only, not truth.",
        ]

    def _board_note_lines(self, item: dict[str, Any]) -> list[str]:
        lines = ["Each row is a paired deterministic benchmark snapshot on the same grid."]
        for hour in REQUIRED_HOURS:
            lines.append(f"{hour} h: {self._metrics_snippet(item, hour)}")
        lines.extend(
            [
                "The inner core and outer envelope are derived from the stored deterministic normalized density raster; the dark outline remains the exact benchmark footprint.",
                (
                    "The shoreline, land backdrop, and locator reuse the stored canonical Mindoro scoring-grid assets."
                    if self.support_context_mode == "mindoro_canonical"
                    else "The figure uses neutral/case-local context so the 2021 debug lane is not misframed as the older Mindoro prototype geography."
                ),
                "OpenDrift is the control forecast lane shown here; PyGNOME is a comparator only.",
                f"{self._support_status_phrase().capitalize()}; not final Chapter 3 evidence.",
            ]
        )
        return lines

    def _render_single_figure(self, item: dict[str, Any], *, hour: int, model_name: str) -> dict[str, Any]:
        pair_row = item["pairings_by_hour"][hour]
        source_key = "control_footprint_path_resolved" if model_name == "opendrift" else "pygnome_footprint_path_resolved"
        density_key = "control_density_path_resolved" if model_name == "opendrift" else "pygnome_density_path_resolved"
        source_path = Path(pair_row[source_key])
        density_path = pair_row.get(density_key)
        source_point = self._resolve_case_source_point(item["case_id"])
        output_path = self.figures_dir / self._single_figure_filename(item["case_id"], hour, model_name)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        fig = plt.figure(figsize=SINGLE_FIGURE_SIZE, dpi=FIGURE_DPI, facecolor="#ffffff")
        grid = fig.add_gridspec(2, 1, height_ratios=[0.92, 0.08], left=0.07, right=0.98, top=0.90, bottom=0.06, hspace=0.04)
        map_ax = fig.add_subplot(grid[0, 0])
        footer_ax = fig.add_subplot(grid[1, 0])

        self._render_model_footprint(
            map_ax,
            raster_path=source_path,
            display_raster_path=Path(density_path) if density_path else None,
            crop_bounds=item["crop_bounds"],
            model_name=model_name,
            panel_title=f"{MODEL_STYLES[model_name]['short_label']} support envelopes",
            source_point=source_point,
        )

        locator_ax = map_ax.inset_axes([0.74, 0.74, 0.22, 0.22])
        self._draw_locator(locator_ax, item["crop_bounds"])
        map_ax.legend(
            handles=self._single_legend_handles(model_name, include_source_point=source_point is not None),
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
            f"{MODEL_STYLES[model_name]['label']} | deterministic density-derived support envelopes | {self.workflow_mode} comparator-only",
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
            f"{MODEL_STYLES[model_name]['label']} higher-density core and broader support envelope derived from the stored deterministic normalized density raster, "
            f"overlaid with the exact benchmark footprint outline and {self._context_phrase()}. "
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
        source_note = "with a provenance source-point star" if source_point is not None else "without a provenance source-point star because no defensible source point was available"
        source_point_path = next((path for path in self._prototype_source_point_candidates(item["case_id"]) if path.exists()), None)
        if source_point_path is not None:
            source_paths.append(str(source_point_path.relative_to(self.repo_root)))
        return {
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
            "notes": f"{self._support_status_phrase().capitalize()} transport comparator with {self._context_phrase()}, a small locator inset, density-derived inner/outer support envelopes, and {source_note}. Deterministic OpenDrift control vs deterministic PyGNOME only.",
            "legacy_debug_only": self.workflow_mode == "prototype_2016",
            "pygnome_role": "comparator_only",
        }

    def _render_board_figure(self, item: dict[str, Any]) -> dict[str, Any]:
        output_path = self.figures_dir / self._board_figure_filename(item["case_id"])
        output_path.parent.mkdir(parents=True, exist_ok=True)
        source_point = self._resolve_case_source_point(item["case_id"])
        fig = plt.figure(figsize=BOARD_FIGURE_SIZE, dpi=FIGURE_DPI, facecolor="#ffffff")
        grid = fig.add_gridspec(
            4,
            3,
            width_ratios=[1.0, 1.0, 0.58],
            height_ratios=[1.0, 1.0, 1.0, 0.42],
            left=0.05,
            right=0.98,
            top=0.91,
            bottom=0.06,
            wspace=0.16,
            hspace=0.18,
        )

        source_paths: list[str] = []
        for row_idx, hour in enumerate(REQUIRED_HOURS):
            pair_row = item["pairings_by_hour"][hour]
            for col_idx, model_name in enumerate(("opendrift", "pygnome")):
                ax = fig.add_subplot(grid[row_idx, col_idx])
                source_key = "control_footprint_path_resolved" if model_name == "opendrift" else "pygnome_footprint_path_resolved"
                density_key = "control_density_path_resolved" if model_name == "opendrift" else "pygnome_density_path_resolved"
                source_path = Path(pair_row[source_key])
                density_path = pair_row.get(density_key)
                source_paths.append(str(source_path.relative_to(self.repo_root)))
                if density_path:
                    source_paths.append(str(Path(density_path).relative_to(self.repo_root)))
                if col_idx == 1:
                    source_paths.append(str(Path(pair_row["qa_overlay_path_resolved"]).relative_to(self.repo_root)))
                self._render_model_footprint(
                    ax,
                    raster_path=source_path,
                    display_raster_path=Path(density_path) if density_path else None,
                    crop_bounds=item["crop_bounds"],
                    model_name=model_name,
                    panel_title=MODEL_STYLES[model_name]["label"] if row_idx == 0 else "",
                    row_label=f"{hour} h" if col_idx == 0 else "",
                    show_xlabel=row_idx == len(REQUIRED_HOURS) - 1,
                    show_ylabel=col_idx == 0,
                    show_labels=False,
                    source_point=source_point,
                )

        locator_ax = fig.add_subplot(grid[0:2, 2])
        side_ax = fig.add_subplot(grid[2, 2])
        note_ax = fig.add_subplot(grid[3, :])
        self._draw_locator(locator_ax, item["crop_bounds"])
        self._draw_board_side_panel(side_ax, item, include_source_point=source_point is not None)
        self._draw_footer_note(note_ax, "Board reading guide", self._board_note_lines(item), width=160)
        figure_title = f"{item['case_id']} | 24/48/72 h OpenDrift vs PyGNOME support envelopes"
        fig.suptitle(figure_title, x=0.05, y=0.965, ha="left", fontsize=18, fontweight="bold")
        fig.text(0.05, 0.932, f"{self.workflow_mode} | deterministic density-derived support envelopes | PyGNOME comparator-only", ha="left", va="top", fontsize=10, color="#475569")
        pixel_width, pixel_height = self._figure_pixel_size(fig)
        fig.savefig(output_path, dpi=FIGURE_DPI)
        plt.close(fig)

        timestamps = [str(item["pairings_by_hour"][hour]["timestamp_utc"]) for hour in REQUIRED_HOURS]
        interpretation = (
            f"{self._workflow_label()} support board for {item['case_id']} showing paired 24 h, 48 h, and 72 h "
            f"deterministic density-derived support envelopes and exact footprint outlines for OpenDrift and PyGNOME on the same benchmark grid with {self._context_phrase()} and locator context. "
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
            "figure_id": output_path.stem,
            "case_id": item["case_id"],
            "phase_or_track": "prototype_pygnome_similarity_summary",
            "date_token": f"{pd.Timestamp(timestamps[0]).strftime('%Y-%m-%d')}_to_{pd.Timestamp(timestamps[-1]).strftime('%Y-%m-%d')}",
            "timestamp_utc": ";".join(timestamps),
            "hour": "",
            "model_name": "opendrift_vs_pygnome",
            "model_label": "OpenDrift deterministic vs PyGNOME deterministic",
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
            "notes": f"{self._support_status_phrase().capitalize()} transport comparator board with {self._context_phrase()}, a small locator/legend side panel, density-derived inner/outer support envelopes, and {source_note}. Deterministic OpenDrift control vs deterministic PyGNOME only.",
            "legacy_debug_only": self.workflow_mode == "prototype_2016",
            "pygnome_role": "comparator_only",
        }

    def _render_forecast_figures(self, case_data: list[dict[str, Any]]) -> list[dict[str, Any]]:
        figure_rows: list[dict[str, Any]] = []
        for item in case_data:
            for hour in REQUIRED_HOURS:
                for model_name in ("opendrift", "pygnome"):
                    figure_rows.append(self._render_single_figure(item, hour=hour, model_name=model_name))
            figure_rows.append(self._render_board_figure(item))
        return figure_rows

    def _write_fss_figure(self, fss_rows: list[dict[str, Any]]) -> Path:
        path = self.output_dir / "qa_prototype_pygnome_fss_by_case_window.png"
        df = pd.DataFrame(fss_rows).sort_values(["window_km", "case_id"])
        windows = list(REQUIRED_WINDOWS_KM)
        case_ids = df["case_id"].drop_duplicates().tolist()
        fig, ax = plt.subplots(figsize=(9, 5))
        x = np.arange(len(windows))
        width = 0.22
        for idx, case_id in enumerate(case_ids):
            case_df = df[df["case_id"] == case_id].sort_values("window_km")
            ax.bar(
                x + (idx - (len(case_ids) - 1) / 2) * width,
                case_df["mean_fss"].tolist(),
                width=width,
                label=case_id,
            )
        ax.set_xticks(x)
        ax.set_xticklabels([f"{window} km" for window in windows])
        ax.set_ylabel("Mean FSS")
        ax.set_title(f"{self._workflow_label()} OpenDrift vs PyGNOME similarity by case and window")
        ax.set_ylim(0.0, 1.0)
        ax.legend(loc="best")
        ax.grid(axis="y", alpha=0.25)
        fig.tight_layout()
        fig.savefig(path, dpi=180)
        plt.close(fig)
        return path

    def _write_kl_figure(self, kl_rows: list[dict[str, Any]]) -> Path:
        path = self.output_dir / "qa_prototype_pygnome_kl_by_case_hour.png"
        df = pd.DataFrame(kl_rows).sort_values(["case_id", "hour"])
        fig, ax = plt.subplots(figsize=(9, 5))
        for case_id in df["case_id"].drop_duplicates().tolist():
            case_df = df[df["case_id"] == case_id].sort_values("hour")
            ax.plot(
                case_df["hour"].tolist(),
                case_df["kl_divergence"].tolist(),
                marker="o",
                linewidth=2.0,
                label=case_id,
            )
        ax.set_xticks(list(REQUIRED_HOURS))
        ax.set_xlabel("Snapshot hour")
        ax.set_ylabel("KL divergence")
        ax.set_title(f"{self._workflow_label()} OpenDrift vs PyGNOME KL divergence by case and hour")
        ax.grid(alpha=0.25)
        ax.legend(loc="best")
        fig.tight_layout()
        fig.savefig(path, dpi=180)
        plt.close(fig)
        return path

    def _write_scorecard_figure(self, similarity_rows: list[dict[str, Any]]) -> Path:
        path = self.output_dir / "qa_prototype_pygnome_scorecard.png"
        df = pd.DataFrame(similarity_rows).sort_values("relative_similarity_rank")
        fig, ax = plt.subplots(figsize=(10, 2.2 + 0.55 * len(df)))
        ax.axis("off")
        table_rows = [
            [
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
            colLabels=["Rank", "Case", "Mean FSS @ 5 km", "Mean KL", "Pairs"],
            cellLoc="center",
            loc="center",
        )
        table.auto_set_font_size(False)
        table.set_fontsize(10)
        table.scale(1, 1.4)
        ax.set_title(
            f"{self._workflow_label()} transport benchmark scorecard\n"
            "Ranking rule: higher mean FSS @ 5 km, then lower mean KL",
            pad=16,
        )
        fig.tight_layout()
        fig.savefig(path, dpi=180, bbox_inches="tight")
        plt.close(fig)
        return path

    def _build_summary_markdown(self, similarity_rows: list[dict[str, Any]]) -> str:
        df = pd.DataFrame(similarity_rows).sort_values("relative_similarity_rank")
        cohort_phrase = (
            "the configured accepted-segment 2021 deterministic OpenDrift control vs deterministic PyGNOME transport benchmarks"
            if self.workflow_mode == "prototype_2021"
            else "the three legacy 2016 deterministic OpenDrift control vs deterministic PyGNOME transport benchmarks"
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
        for _, row in df.iterrows():
            lines.append(
                f"- Rank {int(row['relative_similarity_rank'])}: `{row['case_id']}` | "
                f"mean FSS @ 5 km = {float(row['mean_fss_5km']):.3f}, "
                f"mean KL = {float(row['mean_kl']):.3f}, "
                f"pairs = {int(row['pair_count'])}"
            )
        lines.extend(["", "Per-case snapshot highlights:", ""])
        for _, row in df.iterrows():
            lines.append(
                f"- `{row['case_id']}`: "
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
                "- Higher FSS means stronger footprint overlap between deterministic OpenDrift and deterministic PyGNOME.",
                "- Lower KL means the normalized density fields are more similar over the ocean cells.",
                f"- The ranking is relative within the {self.workflow_mode} support set only.",
                f"- The per-forecast figures under `figures/` are support visuals built from the stored benchmark rasters only, now shown as higher-density core and broader support envelopes over {self._context_phrase()}, with a provenance source-point star when available.",
            ]
        )
        return "\n".join(lines)

    def _build_figure_captions_markdown(self, figure_rows: list[dict[str, Any]]) -> str:
        lines = [
            f"# {self._workflow_label()} Forecast Figure Captions",
            "",
            f"These figures are {self._support_status_phrase()} transport benchmark support visuals built from the stored deterministic OpenDrift and deterministic PyGNOME benchmark rasters, with {self._context_phrase()}, a small locator inset, density-derived inner and outer support envelopes, the exact benchmark footprint outline, and a provenance source-point star when that asset is available.",
            "",
            "Guardrails:",
            "",
            "- PyGNOME is comparator-only, not truth",
            "- transport benchmark only",
            "- not final Chapter 3 evidence",
            "",
        ]
        figure_df = pd.DataFrame(figure_rows).sort_values(["case_id", "view_type", "hour", "model_name"])
        for case_id in self.case_ids:
            lines.append(f"## {case_id}")
            lines.append("")
            case_df = figure_df[figure_df["case_id"] == case_id]
            for _, row in case_df.iterrows():
                label = "board" if row["view_type"] == "board" else f"{row['hour']} h {row['model_label']}"
                lines.append(f"- `{row['figure_id']}` ({label}): {row['short_plain_language_interpretation']}")
            lines.append("")
        return "\n".join(lines)

    def run(self) -> dict[str, Any]:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.figures_dir.mkdir(parents=True, exist_ok=True)
        case_data = [self._load_case_artifacts(case_id) for case_id in self.case_ids]
        case_registry_rows = self._build_case_registry_rows(case_data)
        fss_rows = self._build_fss_rows(case_data)
        kl_rows = self._build_kl_rows(case_data)
        similarity_rows = self._build_similarity_rows(case_data, fss_rows, kl_rows)
        figure_rows = self._render_forecast_figures(case_data)

        case_registry_csv = self.output_dir / "prototype_pygnome_case_registry.csv"
        similarity_by_case_csv = self.output_dir / "prototype_pygnome_similarity_by_case.csv"
        fss_by_case_window_csv = self.output_dir / "prototype_pygnome_fss_by_case_window.csv"
        kl_by_case_hour_csv = self.output_dir / "prototype_pygnome_kl_by_case_hour.csv"
        figure_registry_csv = self.output_dir / "prototype_pygnome_figure_registry.csv"
        manifest_json = self.output_dir / "prototype_pygnome_similarity_manifest.json"
        summary_md = self.output_dir / "prototype_pygnome_similarity_summary.md"
        figure_captions_md = self.output_dir / "prototype_pygnome_figure_captions.md"

        _write_csv(case_registry_csv, case_registry_rows)
        _write_csv(similarity_by_case_csv, similarity_rows)
        _write_csv(fss_by_case_window_csv, fss_rows)
        _write_csv(kl_by_case_hour_csv, kl_rows)
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
                "legacy_debug_only",
                "pygnome_role",
            ],
        )

        fss_figure = self._write_fss_figure(fss_rows)
        kl_figure = self._write_kl_figure(kl_rows)
        scorecard_figure = self._write_scorecard_figure(similarity_rows)
        _write_text(summary_md, self._build_summary_markdown(similarity_rows))
        _write_text(figure_captions_md, self._build_figure_captions_markdown(figure_rows))

        top_case = min(similarity_rows, key=lambda row: int(row["relative_similarity_rank"]))
        manifest = {
            "phase": PHASE,
            "workflow_mode": self.workflow_mode,
            "generated_at_utc": _utc_now_iso(),
            "output_root": str(self.output_dir.relative_to(self.repo_root)),
            "configured_case_ids": self.case_ids,
            "comparison_scope": "deterministic_opendrift_control_vs_deterministic_pygnome_transport_benchmark",
            "pygnome_role": "comparator_only",
            "legacy_debug_only": self.workflow_mode == "prototype_2016",
            "final_chapter3_evidence": False,
            "support_context_mode": self.support_context_mode,
            "ranking_rule": "higher mean FSS @ 5 km, then lower mean KL",
            "required_artifacts_per_case": list(REQUIRED_BENCHMARK_FILES.values()),
            "headline": {
                "top_ranked_case_id": top_case["case_id"],
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
            "manifest_json": str(manifest_json),
            "summary_md": str(summary_md),
            "qa_figures": [str(fss_figure), str(kl_figure), str(scorecard_figure)],
            "forecast_figure_paths": [row["file_path"] for row in figure_rows],
            "single_figure_count": int(sum(1 for row in figure_rows if row["view_type"] == "single")),
            "board_figure_count": int(sum(1 for row in figure_rows if row["view_type"] == "board")),
            "top_ranked_case_id": top_case["case_id"],
            "top_ranked_mean_fss_5km": float(top_case["mean_fss_5km"]),
            "top_ranked_mean_kl": float(top_case["mean_kl"]),
            "case_count": len(self.case_ids),
        }


def run_prototype_pygnome_similarity_summary(**kwargs: Any) -> dict[str, Any]:
    return PrototypePygnomeSimilaritySummaryService(**kwargs).run()
