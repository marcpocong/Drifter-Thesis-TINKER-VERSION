"""
Canonical scoring-grid helpers.

Prototype mode keeps the legacy geographic degree grid. Official Mindoro mode
uses a real projected 1 km scoring grid in EPSG:32651, derived from the case
geometries and persisted as reusable artifacts.
"""

from __future__ import annotations

import csv
import json
import math
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import yaml

from src.core.case_context import get_case_context, load_settings

try:
    import geopandas as gpd
except ImportError:  # pragma: no cover - guarded at runtime
    gpd = None

try:
    import rasterio
    from rasterio.transform import from_origin
except ImportError:  # pragma: no cover - guarded at runtime
    rasterio = None
    from_origin = None

try:
    from shapely.geometry import box
except ImportError:  # pragma: no cover - guarded at runtime
    box = None


OFFICIAL_GRID_CRS = "EPSG:32651"
OFFICIAL_GRID_RESOLUTION_M = 1000.0
OFFICIAL_GRID_BUFFER_M = 50000.0
GEOGRAPHIC_CRS = "EPSG:4326"

SCORING_GRID_DIR = Path("data_processed") / "grids"
SCORING_GRID_METADATA_PATH = SCORING_GRID_DIR / "scoring_grid.yaml"
SCORING_GRID_METADATA_JSON_PATH = SCORING_GRID_DIR / "scoring_grid.json"
SCORING_GRID_TEMPLATE_PATH = SCORING_GRID_DIR / "scoring_grid_template.tif"
SCORING_GRID_EXTENT_PATH = SCORING_GRID_DIR / "grid_extent.gpkg"
LAND_MASK_PATH = SCORING_GRID_DIR / "land_mask.tif"
SEA_MASK_PATH = SCORING_GRID_DIR / "sea_mask.tif"
PRECHECK_REPORT_DIR = SCORING_GRID_DIR / "precheck_reports"


@dataclass(frozen=True)
class ScoringGridSpec:
    min_x: float
    max_x: float
    min_y: float
    max_y: float
    resolution: float
    crs: str = GEOGRAPHIC_CRS
    x_name: str = "lon"
    y_name: str = "lat"
    units: str = "degrees"
    workflow_mode: str | None = None
    run_name: str | None = None
    display_bounds_wgs84: list[float] | None = None
    buffer_m: float | None = None
    grid_snap_m: float | None = None
    metadata_path: str | None = None
    metadata_json_path: str | None = None
    template_path: str | None = None
    extent_path: str | None = None
    land_mask_path: str | None = None
    sea_mask_path: str | None = None
    shoreline_mask_status: str | None = None
    source_paths: dict[str, str] | None = None

    @property
    def width(self) -> int:
        return int(round((self.max_x - self.min_x) / self.resolution))

    @property
    def height(self) -> int:
        return int(round((self.max_y - self.min_y) / self.resolution))

    @property
    def region(self) -> list[float]:
        return [self.min_x, self.max_x, self.min_y, self.max_y]

    @property
    def extent(self) -> list[float]:
        return self.region

    @property
    def x_bins(self) -> np.ndarray:
        return np.linspace(self.min_x, self.max_x, self.width + 1)

    @property
    def y_bins(self) -> np.ndarray:
        return np.linspace(self.min_y, self.max_y, self.height + 1)

    @property
    def x_centers(self) -> np.ndarray:
        return self.x_bins[:-1] + (self.resolution / 2.0)

    @property
    def y_centers(self) -> np.ndarray:
        return self.y_bins[:-1] + (self.resolution / 2.0)

    @property
    def lon_bins(self) -> np.ndarray:
        return self.x_bins

    @property
    def lat_bins(self) -> np.ndarray:
        return self.y_bins

    @property
    def min_lon(self) -> float:
        return self.min_x

    @property
    def max_lon(self) -> float:
        return self.max_x

    @property
    def min_lat(self) -> float:
        return self.min_y

    @property
    def max_lat(self) -> float:
        return self.max_y

    @property
    def is_projected(self) -> bool:
        return str(self.crs).upper() != GEOGRAPHIC_CRS

    def to_metadata(self) -> dict[str, Any]:
        return {
            **asdict(self),
            "width": self.width,
            "height": self.height,
            "extent": self.extent,
            "resolution_x": self.resolution,
            "resolution_y": self.resolution,
            "is_projected": self.is_projected,
        }

    def save_metadata(self, out_path: str | Path) -> dict[str, Any]:
        out_path = Path(out_path)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        meta = self.to_metadata()
        with open(out_path, "w", encoding="utf-8") as f:
            if out_path.suffix.lower() == ".json":
                json.dump(meta, f, indent=2)
            else:
                yaml.safe_dump(meta, f, sort_keys=False)
        return meta


@dataclass(frozen=True)
class SameGridPrecheckResult:
    passed: bool
    csv_report_path: Path
    json_report_path: Path
    checks: dict[str, bool]
    forecast_path: Path
    target_path: Path


def get_scoring_grid_artifact_paths() -> dict[str, Path]:
    return {
        "metadata_yaml": SCORING_GRID_METADATA_PATH,
        "metadata_json": SCORING_GRID_METADATA_JSON_PATH,
        "template_tif": SCORING_GRID_TEMPLATE_PATH,
        "extent_gpkg": SCORING_GRID_EXTENT_PATH,
        "land_mask_tif": LAND_MASK_PATH,
        "sea_mask_tif": SEA_MASK_PATH,
    }


def _sanitize_report_stem(value: str) -> str:
    safe_chars = []
    for char in value:
        safe_chars.append(char if char.isalnum() or char in {"-", "_", "."} else "_")
    return "".join(safe_chars).strip("._") or "same_grid_precheck"


def _as_path_str(path: Path | None) -> str | None:
    return str(path) if path is not None else None


def _load_gdf(path: Path, label: str):
    if gpd is None:
        raise ImportError("geopandas is required to build the official scoring grid")
    if not path.exists():
        raise FileNotFoundError(
            f"Official scoring grid requires the processed {label}. Missing: {path}"
        )
    gdf = gpd.read_file(path)
    if gdf.crs is None:
        gdf = gdf.set_crs(GEOGRAPHIC_CRS)
    valid = gdf.dropna(subset=["geometry"]).copy()
    if valid.empty:
        raise ValueError(f"Official scoring grid input has no valid geometry: {path}")
    return valid


def _snap_min(value: float, snap: float) -> float:
    return math.floor(value / snap) * snap


def _snap_max(value: float, snap: float) -> float:
    return math.ceil(value / snap) * snap


def _union_geometries(geometries):
    if hasattr(geometries, "union_all"):
        return geometries.union_all()
    return geometries.unary_union


def _write_template_raster(spec: ScoringGridSpec, out_path: Path, fill_value: int = 0):
    if rasterio is None or from_origin is None:
        raise ImportError("rasterio is required to write scoring-grid artifacts")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    transform = from_origin(spec.min_x, spec.max_y, spec.resolution, spec.resolution)
    data = np.full((spec.height, spec.width), fill_value, dtype=np.uint8)
    temp_path = out_path.with_name(f"{out_path.stem}__tmp{out_path.suffix}")
    if temp_path.exists():
        temp_path.unlink()
    with rasterio.open(
        temp_path,
        "w",
        driver="GTiff",
        height=spec.height,
        width=spec.width,
        count=1,
        dtype=data.dtype,
        crs=spec.crs,
        transform=transform,
        compress="lzw",
    ) as dst:
        dst.write(data, 1)
    if out_path.exists():
        out_path.unlink()
    temp_path.replace(out_path)


def _remove_existing_vector(path: Path):
    if path.exists():
        path.unlink()


def _write_grid_extent(spec: ScoringGridSpec, out_path: Path):
    if gpd is None or box is None:
        raise ImportError("geopandas and shapely are required to write the grid extent")
    extent_geom = box(spec.min_x, spec.min_y, spec.max_x, spec.max_y)
    gdf = gpd.GeoDataFrame(
        [
            {
                "workflow_mode": spec.workflow_mode or "",
                "run_name": spec.run_name or "",
                "grid_crs": spec.crs,
                "resolution": spec.resolution,
                "buffer_m": spec.buffer_m or 0.0,
            }
        ],
        geometry=[extent_geom],
        crs=spec.crs,
    )
    out_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = out_path.with_name(f"{out_path.stem}__tmp{out_path.suffix}")
    _remove_existing_vector(temp_path)
    gdf.to_file(temp_path, driver="GPKG")
    _remove_existing_vector(out_path)
    temp_path.replace(out_path)


def _load_spec_from_metadata(path: Path) -> ScoringGridSpec:
    with open(path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}

    if "min_x" not in data and "min_lon" in data:
        data["min_x"] = data["min_lon"]
        data["max_x"] = data["max_lon"]
        data["min_y"] = data["min_lat"]
        data["max_y"] = data["max_lat"]

    allowed_keys = {field.name for field in ScoringGridSpec.__dataclass_fields__.values()}
    filtered = {key: value for key, value in data.items() if key in allowed_keys}
    return ScoringGridSpec(**filtered)


def build_official_scoring_grid(force_refresh: bool = False) -> ScoringGridSpec:
    """
    Build and persist the thesis-aligned official 1 km scoring grid.

    Extent rule:
    - union of March 3 initialization polygon, March 6 validation polygon,
      and source point
    - buffer by 50 km
    - snap outward to the 1 km grid
    """
    case = get_case_context()
    if case.is_prototype:
        raise RuntimeError("Official scoring-grid builder is only valid for official workflows.")

    artifact_paths = get_scoring_grid_artifact_paths()
    required_paths = list(artifact_paths.values())
    if not force_refresh and all(path.exists() for path in required_paths):
        cached = _load_spec_from_metadata(artifact_paths["metadata_yaml"])
        if cached.run_name == case.run_name and cached.workflow_mode == case.workflow_mode:
            return cached

    init_path = case.initialization_layer.geojson_path(case.run_name)
    validation_path = case.validation_layer.geojson_path(case.run_name)
    source_path = case.provenance_layer.geojson_path(case.run_name)

    init_gdf = _load_gdf(init_path, "March 3 initialization polygon").to_crs(OFFICIAL_GRID_CRS)
    validation_gdf = _load_gdf(validation_path, "March 6 validation polygon").to_crs(OFFICIAL_GRID_CRS)
    source_gdf = _load_gdf(source_path, "source point").to_crs(OFFICIAL_GRID_CRS)

    combined = gpd.GeoSeries(
        [
            _union_geometries(init_gdf.geometry.buffer(0)),
            _union_geometries(validation_gdf.geometry.buffer(0)),
            _union_geometries(source_gdf.geometry),
        ],
        crs=OFFICIAL_GRID_CRS,
    )
    combined = _union_geometries(combined)
    buffered = combined.buffer(OFFICIAL_GRID_BUFFER_M)
    min_x, min_y, max_x, max_y = buffered.bounds

    snapped_min_x = float(_snap_min(min_x, OFFICIAL_GRID_RESOLUTION_M))
    snapped_min_y = float(_snap_min(min_y, OFFICIAL_GRID_RESOLUTION_M))
    snapped_max_x = float(_snap_max(max_x, OFFICIAL_GRID_RESOLUTION_M))
    snapped_max_y = float(_snap_max(max_y, OFFICIAL_GRID_RESOLUTION_M))

    extent_polygon = gpd.GeoDataFrame(
        [{"name": "official_scoring_grid_extent"}],
        geometry=[box(snapped_min_x, snapped_min_y, snapped_max_x, snapped_max_y)],
        crs=OFFICIAL_GRID_CRS,
    ).to_crs(GEOGRAPHIC_CRS)
    display_min_lon, display_min_lat, display_max_lon, display_max_lat = extent_polygon.total_bounds
    display_bounds = [
        float(display_min_lon),
        float(display_max_lon),
        float(display_min_lat),
        float(display_max_lat),
    ]

    spec = ScoringGridSpec(
        min_x=snapped_min_x,
        max_x=snapped_max_x,
        min_y=snapped_min_y,
        max_y=snapped_max_y,
        resolution=OFFICIAL_GRID_RESOLUTION_M,
        crs=OFFICIAL_GRID_CRS,
        x_name="x",
        y_name="y",
        units="meters",
        workflow_mode=case.workflow_mode,
        run_name=case.run_name,
        display_bounds_wgs84=display_bounds,
        buffer_m=OFFICIAL_GRID_BUFFER_M,
        grid_snap_m=OFFICIAL_GRID_RESOLUTION_M,
        metadata_path=_as_path_str(artifact_paths["metadata_yaml"]),
        metadata_json_path=_as_path_str(artifact_paths["metadata_json"]),
        template_path=_as_path_str(artifact_paths["template_tif"]),
        extent_path=_as_path_str(artifact_paths["extent_gpkg"]),
        land_mask_path=_as_path_str(artifact_paths["land_mask_tif"]),
        sea_mask_path=_as_path_str(artifact_paths["sea_mask_tif"]),
        shoreline_mask_status="scaffold_all_sea_pending_shoreline_integration",
        source_paths={
            "initialization_polygon": str(init_path),
            "validation_polygon": str(validation_path),
            "source_point": str(source_path),
        },
    )

    SCORING_GRID_DIR.mkdir(parents=True, exist_ok=True)
    spec.save_metadata(artifact_paths["metadata_yaml"])
    spec.save_metadata(artifact_paths["metadata_json"])
    _write_template_raster(spec, artifact_paths["template_tif"], fill_value=0)
    _write_grid_extent(spec, artifact_paths["extent_gpkg"])

    # Scaffold masks for the later shoreline-aware patch.
    _write_template_raster(spec, artifact_paths["land_mask_tif"], fill_value=0)
    _write_template_raster(spec, artifact_paths["sea_mask_tif"], fill_value=1)
    return spec


def get_scoring_grid_spec(force_refresh: bool = False) -> ScoringGridSpec:
    """Return the single canonical scoring grid used by benchmark/validation products."""
    case = get_case_context()
    if case.is_prototype:
        settings = load_settings()
        min_lon, max_lon, min_lat, max_lat = case.region
        return ScoringGridSpec(
            min_x=float(min_lon),
            max_x=float(max_lon),
            min_y=float(min_lat),
            max_y=float(max_lat),
            resolution=float(settings["grid_resolution"]),
            crs=GEOGRAPHIC_CRS,
            x_name="lon",
            y_name="lat",
            units="degrees",
            workflow_mode=case.workflow_mode,
            run_name=case.run_name,
            display_bounds_wgs84=[float(min_lon), float(max_lon), float(min_lat), float(max_lat)],
        )

    metadata_path = get_scoring_grid_artifact_paths()["metadata_yaml"]
    if force_refresh or not metadata_path.exists():
        return build_official_scoring_grid(force_refresh=True)

    try:
        spec = _load_spec_from_metadata(metadata_path)
    except Exception:
        return build_official_scoring_grid(force_refresh=True)

    if spec.run_name != case.run_name or spec.workflow_mode != case.workflow_mode:
        return build_official_scoring_grid(force_refresh=True)
    return spec


def _describe_raster(path: str | Path) -> tuple[dict[str, Any], np.ndarray]:
    if rasterio is None:
        raise ImportError("rasterio is required to run same-grid prechecks")

    path = Path(path)
    with rasterio.open(path) as src:
        mask = src.read_masks(1)
        desc = {
            "path": str(path),
            "crs": src.crs.to_string() if src.crs else None,
            "transform": [
                float(src.transform.a),
                float(src.transform.b),
                float(src.transform.c),
                float(src.transform.d),
                float(src.transform.e),
                float(src.transform.f),
            ],
            "width": int(src.width),
            "height": int(src.height),
            "resolution": [float(abs(src.res[0])), float(abs(src.res[1]))],
            "nodata": None if src.nodata is None else float(src.nodata),
            "dtype": src.dtypes[0],
            "mask_all_valid": bool(np.all(mask == 255)),
        }
    return desc, mask


def _values_match(left: float | None, right: float | None, tol: float = 1e-9) -> bool:
    if left is None and right is None:
        return True
    if left is None or right is None:
        return False
    return math.isclose(float(left), float(right), abs_tol=tol)


def precheck_same_grid(
    forecast: str | Path,
    target: str | Path,
    report_base_path: str | Path | None = None,
) -> SameGridPrecheckResult:
    """
    Verify that forecast and target rasters share the exact same scoreable grid.

    The report is always written before the result is returned.
    """
    forecast_path = Path(forecast)
    target_path = Path(target)

    if report_base_path is None:
        report_name = _sanitize_report_stem(f"{forecast_path.stem}__vs__{target_path.stem}")
        base_path = PRECHECK_REPORT_DIR / report_name
    else:
        base_path = Path(report_base_path)

    base_path.parent.mkdir(parents=True, exist_ok=True)
    csv_report_path = base_path.with_suffix(".csv")
    json_report_path = base_path.with_suffix(".json")

    forecast_desc, forecast_mask = _describe_raster(forecast_path)
    target_desc, target_mask = _describe_raster(target_path)

    checks = {
        "crs_match": forecast_desc["crs"] == target_desc["crs"],
        "transform_match": all(
            _values_match(left, right, tol=1e-6)
            for left, right in zip(forecast_desc["transform"], target_desc["transform"])
        ),
        "width_match": forecast_desc["width"] == target_desc["width"],
        "height_match": forecast_desc["height"] == target_desc["height"],
        "resolution_match": all(
            _values_match(left, right, tol=1e-6)
            for left, right in zip(forecast_desc["resolution"], target_desc["resolution"])
        ),
        "nodata_match": _values_match(forecast_desc["nodata"], target_desc["nodata"]),
        "mask_compatible": forecast_mask.shape == target_mask.shape and np.array_equal(forecast_mask, target_mask),
    }
    passed = all(checks.values())

    payload = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "passed": passed,
        "checks": checks,
        "forecast": forecast_desc,
        "target": target_desc,
    }

    with open(json_report_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    row = {
        "generated_at_utc": payload["generated_at_utc"],
        "passed": passed,
        "forecast_path": str(forecast_path),
        "target_path": str(target_path),
        **checks,
        "forecast_crs": forecast_desc["crs"] or "",
        "target_crs": target_desc["crs"] or "",
        "forecast_width": forecast_desc["width"],
        "target_width": target_desc["width"],
        "forecast_height": forecast_desc["height"],
        "target_height": target_desc["height"],
        "forecast_resolution_x": forecast_desc["resolution"][0],
        "forecast_resolution_y": forecast_desc["resolution"][1],
        "target_resolution_x": target_desc["resolution"][0],
        "target_resolution_y": target_desc["resolution"][1],
    }
    with open(csv_report_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(row.keys()))
        writer.writeheader()
        writer.writerow(row)

    return SameGridPrecheckResult(
        passed=passed,
        csv_report_path=csv_report_path,
        json_report_path=json_report_path,
        checks=checks,
        forecast_path=forecast_path,
        target_path=target_path,
    )
