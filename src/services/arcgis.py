"""
ArcGIS FeatureServer ingestion helpers.

Official mode now archives raw payloads, writes cleaned processed vectors,
and records QA metrics before rasterization onto the canonical scoring grid.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Iterable

import numpy as np
import requests

try:
    import geopandas as gpd
except ImportError:  # pragma: no cover - guarded at runtime
    gpd = None

try:
    from shapely import make_valid as shapely_make_valid
except ImportError:  # pragma: no cover - shapely<2 fallback
    shapely_make_valid = None

try:
    from shapely.geometry import GeometryCollection, MultiPoint, MultiPolygon, Point, Polygon
except ImportError:  # pragma: no cover - guarded at runtime
    GeometryCollection = MultiPoint = MultiPolygon = Point = Polygon = None

try:
    from shapely.validation import make_valid as validation_make_valid
except ImportError:  # pragma: no cover - optional fallback
    validation_make_valid = None

from src.helpers.raster import GridBuilder, rasterize_observation_layer, save_mask_raster
from src.helpers.scoring import apply_ocean_mask, load_sea_mask_array

logger = logging.getLogger(__name__)

WEB_MERCATOR_WKIDS = {3857, 102100, 102113, 900913}


@dataclass(frozen=True)
class ArcGISLayerRef:
    service_url: str
    layer_id: int
    name: str
    role: str
    geometry_type: str
    event_time_utc: str | None
    run_name: str
    official_mode: bool
    expected_region: list[float] | None = None

    @property
    def query_url(self) -> str:
        return f"{self.service_url.rstrip('/')}/{self.layer_id}/query"

    @property
    def metadata_url(self) -> str:
        return f"{self.service_url.rstrip('/')}/{self.layer_id}"

    @property
    def event_date(self) -> str:
        return (self.event_time_utc or "")[:10]

    @property
    def raw_geojson_path(self) -> Path:
        if self.official_mode:
            return Path("data") / "arcgis" / self.run_name / f"{self.name}_raw.geojson"
        return Path("data") / "arcgis" / self.run_name / f"{self.name}.geojson"

    @property
    def processed_vector_path(self) -> Path:
        if self.official_mode:
            return Path("data") / "arcgis" / self.run_name / f"{self.name}_processed.gpkg"
        return Path("data") / "arcgis" / self.run_name / f"{self.name}.geojson"

    @property
    def service_metadata_path(self) -> Path:
        return Path("data") / "arcgis" / self.run_name / f"{self.name}_service_metadata.json"

    @property
    def raster_path(self) -> Path | None:
        if self.geometry_type != "polygon":
            return None
        if self.official_mode and self.role == "validation_polygon" and self.event_date:
            return Path("data") / "arcgis" / self.run_name / f"obs_mask_{self.event_date}.tif"
        return Path("data") / "arcgis" / self.run_name / f"{self.name}.tif"


@dataclass(frozen=True)
class ArcGISPreparedLayer:
    name: str
    layer_id: int
    role: str
    geometry_type: str
    event_time_utc: str | None
    raw_feature_count: int
    processed_feature_count: int
    raw_crs: str
    processed_crs: str
    raw_geojson_path: Path
    processed_vector_path: Path
    service_metadata_path: Path
    raster_path: Path | None
    vector_centroid: str
    raster_centroid: str
    vector_area: float
    raster_nonzero_cells: int
    notes: str

    def to_registry_row(self) -> dict[str, str | int | float]:
        return {
            "name": self.name,
            "layer_id": self.layer_id,
            "role": self.role,
            "event_time_utc": self.event_time_utc or "",
            "feature_count": self.processed_feature_count,
            "raw_geojson": str(self.raw_geojson_path),
            "processed_vector": str(self.processed_vector_path),
            "service_metadata": str(self.service_metadata_path),
            "mask": str(self.raster_path) if self.raster_path else "",
        }

    def to_processing_report_row(self) -> dict[str, str | int | float]:
        return {
            "layer_id": self.layer_id,
            "raw_feature_count": self.raw_feature_count,
            "processed_feature_count": self.processed_feature_count,
            "raw_crs": self.raw_crs,
            "processed_crs": self.processed_crs,
            "vector_centroid": self.vector_centroid,
            "raster_centroid": self.raster_centroid,
            "vector_area": self.vector_area,
            "raster_nonzero_cells": self.raster_nonzero_cells,
            "notes": self.notes,
        }


def get_arcgis_registry_path(run_name: str) -> Path:
    return Path("data") / "arcgis" / run_name / "arcgis_registry.csv"


def get_arcgis_processing_report_path(run_name: str) -> Path:
    return Path("data") / "arcgis" / run_name / "arcgis_processing_report.csv"


def get_configured_arcgis_layers() -> list[ArcGISLayerRef]:
    """Return the ArcGIS layer refs for the active workflow case."""
    from src.core.case_context import get_case_context

    case = get_case_context()
    return [
        ArcGISLayerRef(
            service_url=layer.service_url,
            layer_id=layer.layer_id,
            name=layer.local_name,
            role=layer.role,
            geometry_type=layer.geometry_type,
            event_time_utc=layer.event_time_utc,
            run_name=case.run_name,
            official_mode=case.is_official,
            expected_region=list(case.region),
        )
        for layer in case.arcgis_layers
    ]


def _save_json(payload: dict, out_path: Path):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


def _raw_feature_count(payload: dict) -> int:
    return len(payload.get("features") or [])


def _get_payload_crs_name(payload: dict) -> str | None:
    crs = payload.get("crs") or {}
    props = crs.get("properties") or {}
    return props.get("name")


def _extract_wkid(metadata: dict) -> int | None:
    candidates = [
        metadata.get("sourceSpatialReference"),
        metadata.get("extent", {}).get("spatialReference"),
        metadata.get("spatialReference"),
        metadata.get("defaultSpatialReference"),
    ]
    for candidate in candidates:
        if not candidate:
            continue
        for key in ("latestWkid", "wkid"):
            value = candidate.get(key)
            if value is not None:
                try:
                    return int(value)
                except Exception:
                    continue
    return None


def _bounds_are_geographic(bounds: tuple[float, float, float, float] | None) -> bool:
    if not bounds:
        return True
    minx, miny, maxx, maxy = bounds
    return all(
        [
            abs(minx) <= 180.5,
            abs(maxx) <= 180.5,
            abs(miny) <= 90.5,
            abs(maxy) <= 90.5,
        ]
    )


def _get_bounds(gdf) -> tuple[float, float, float, float] | None:
    if gdf is None or gdf.empty or not gdf.geometry.notnull().any():
        return None
    minx, miny, maxx, maxy = gdf.dropna(subset=["geometry"]).total_bounds
    return float(minx), float(miny), float(maxx), float(maxy)


def _metadata_is_web_mercator(metadata: dict, payload_crs_name: str | None) -> bool:
    wkid = _extract_wkid(metadata)
    if wkid in WEB_MERCATOR_WKIDS:
        return True
    if payload_crs_name:
        lowered = payload_crs_name.lower()
        return any(token in lowered for token in ("3857", "102100", "102113", "900913", "mercator"))
    return False


def _bounds_fit_expected_region(bounds: tuple[float, float, float, float] | None, expected_region: list[float] | None) -> bool:
    if not bounds or not expected_region:
        return True

    minx, miny, maxx, maxy = bounds
    region_min_lon, region_max_lon, region_min_lat, region_max_lat = expected_region
    buffer_deg = 1.0
    center_lon = (minx + maxx) / 2.0
    center_lat = (miny + maxy) / 2.0
    in_buffered_extent = (
        (region_min_lon - buffer_deg) <= center_lon <= (region_max_lon + buffer_deg)
        and (region_min_lat - buffer_deg) <= center_lat <= (region_max_lat + buffer_deg)
    )
    overlaps = not (
        maxx < (region_min_lon - buffer_deg)
        or minx > (region_max_lon + buffer_deg)
        or maxy < (region_min_lat - buffer_deg)
        or miny > (region_max_lat + buffer_deg)
    )
    return in_buffered_extent and overlaps


def _repair_degree_scaled_geometries(
    gdf,
    metadata: dict,
    payload: dict,
    expected_region: list[float] | None,
) -> tuple["gpd.GeoDataFrame", list[str]]:
    notes: list[str] = []
    if gdf is None or gdf.empty or str(gdf.crs).upper() != "EPSG:4326":
        return gdf, notes

    payload_crs_name = _get_payload_crs_name(payload)
    if not _metadata_is_web_mercator(metadata, payload_crs_name):
        return gdf, notes

    raw_bounds = _get_bounds(gdf)
    if _bounds_fit_expected_region(raw_bounds, expected_region):
        return gdf, notes
    if not _bounds_are_geographic(raw_bounds):
        return gdf, notes
    if raw_bounds is None or max(abs(value) for value in raw_bounds) > 5.0:
        return gdf, notes

    candidate = gdf.to_crs("EPSG:3857").set_crs("EPSG:4326", allow_override=True)
    candidate_bounds = _get_bounds(candidate)
    if _bounds_fit_expected_region(candidate_bounds, expected_region):
        notes.append(
            "rescued scaled-to-near-zero coordinates using Web Mercator scale normalization because raw bounds were implausible for the configured case region"
        )
        return candidate, notes

    return gdf, notes


def _infer_source_crs(gdf, metadata: dict, payload: dict) -> tuple[str, list[str]]:
    notes: list[str] = []
    payload_crs_name = _get_payload_crs_name(payload)
    bounds = _get_bounds(gdf)
    coords_are_geographic = _bounds_are_geographic(bounds)
    wkid = _extract_wkid(metadata)
    metadata_is_web_mercator = _metadata_is_web_mercator(metadata, payload_crs_name)

    if coords_are_geographic:
        if metadata_is_web_mercator:
            notes.append("raw metadata suggested Web Mercator, but coordinates were degree-like; treated as EPSG:4326")
        elif payload_crs_name:
            notes.append(f"raw payload CRS={payload_crs_name}; treated as EPSG:4326")
        return "EPSG:4326", notes

    if wkid in WEB_MERCATOR_WKIDS or metadata_is_web_mercator:
        notes.append("raw coordinates were projected-like and metadata indicated Web Mercator; treated as EPSG:3857")
        return "EPSG:3857", notes

    notes.append("raw coordinates were projected-like without a trusted CRS tag; defaulted to EPSG:3857 for repair")
    return "EPSG:3857", notes


def _make_valid_safe(geometry):
    if geometry is None or geometry.is_empty:
        return geometry
    make_valid_func = shapely_make_valid or validation_make_valid
    if make_valid_func is not None:
        try:
            return make_valid_func(geometry)
        except Exception:
            pass
    try:
        return geometry.buffer(0)
    except Exception:
        return geometry


def _extract_geometry_parts(geometry, expected_geometry_type: str) -> list:
    if geometry is None or geometry.is_empty:
        return []

    expected = expected_geometry_type.lower()
    if expected == "polygon":
        if isinstance(geometry, Polygon):
            return [geometry]
        if isinstance(geometry, MultiPolygon):
            return [geom for geom in geometry.geoms if geom is not None and not geom.is_empty]
    elif expected == "point":
        if isinstance(geometry, Point):
            return [geometry]
        if isinstance(geometry, MultiPoint):
            return [geom for geom in geometry.geoms if geom is not None and not geom.is_empty]

    if isinstance(geometry, GeometryCollection):
        parts = []
        for part in geometry.geoms:
            parts.extend(_extract_geometry_parts(part, expected_geometry_type))
        return parts

    return []


def clean_arcgis_geometries(
    raw_gdf,
    expected_geometry_type: str,
    source_crs: str,
    target_crs: str,
) -> tuple["gpd.GeoDataFrame", dict[str, int]]:
    if gpd is None:
        raise ImportError("geopandas is required for ArcGIS geometry cleaning")

    qa = {
        "null_geometries_dropped": 0,
        "invalid_geometries_repaired": 0,
        "multipart_parts_exploded": 0,
        "non_matching_parts_dropped": 0,
        "empty_geometries_dropped": 0,
    }

    if raw_gdf is None or raw_gdf.empty:
        empty = gpd.GeoDataFrame(columns=getattr(raw_gdf, "columns", []), geometry=[], crs=target_crs)
        return empty, qa

    work = raw_gdf.copy()
    if work.crs is None:
        work = work.set_crs(source_crs, allow_override=True)
    elif str(work.crs) != str(source_crs):
        work = work.set_crs(source_crs, allow_override=True)

    qa["null_geometries_dropped"] = int(work.geometry.isna().sum())
    work = work.dropna(subset=["geometry"]).copy()

    repaired_rows = []
    for _, row in work.iterrows():
        geometry = row.geometry
        if geometry is None or geometry.is_empty:
            qa["empty_geometries_dropped"] += 1
            continue

        was_invalid = not geometry.is_valid
        fixed = _make_valid_safe(geometry)
        if was_invalid and fixed is not None and not fixed.is_empty:
            qa["invalid_geometries_repaired"] += 1

        parts = _extract_geometry_parts(fixed, expected_geometry_type)
        if not parts:
            if fixed is None or fixed.is_empty:
                qa["empty_geometries_dropped"] += 1
            else:
                qa["non_matching_parts_dropped"] += 1
            continue

        if len(parts) > 1:
            qa["multipart_parts_exploded"] += len(parts) - 1

        for part in parts:
            if part is None or part.is_empty:
                qa["empty_geometries_dropped"] += 1
                continue
            new_row = row.copy()
            new_row.geometry = part
            repaired_rows.append(new_row)

    cleaned = gpd.GeoDataFrame(repaired_rows, columns=work.columns, geometry="geometry", crs=source_crs)
    if cleaned.empty:
        return gpd.GeoDataFrame(columns=work.columns, geometry=[], crs=target_crs), qa
    return cleaned.to_crs(target_crs), qa


def _format_centroid(point) -> str:
    if point is None or point.is_empty:
        return ""
    return f"{float(point.x):.3f},{float(point.y):.3f}"


def get_preferred_reference_geometry(gdf, geometry_type: str):
    if gdf is None or gdf.empty:
        return None
    valid = gdf.dropna(subset=["geometry"])
    if valid.empty:
        return None

    if geometry_type == "polygon":
        areas = valid.geometry.apply(lambda geom: geom.area).to_numpy(dtype=float)
        pos = int(np.argmax(areas))
        return valid.geometry.iloc[pos]

    return valid.geometry.iloc[0]


def compute_vector_centroid(gdf, geometry_type: str) -> str:
    reference = get_preferred_reference_geometry(gdf, geometry_type)
    if reference is None:
        return ""
    if geometry_type == "polygon":
        return _format_centroid(reference.representative_point())
    return _format_centroid(reference)


def compute_vector_area(gdf, geometry_type: str) -> float:
    if gdf is None or gdf.empty or geometry_type != "polygon":
        return 0.0
    return float(gdf.geometry.apply(lambda geom: geom.area).sum())


def compute_raster_centroid(mask_data, grid: GridBuilder) -> tuple[str, int]:
    nonzero = np.argwhere(mask_data > 0)
    if nonzero.size == 0:
        return "", 0

    rows = nonzero[:, 0]
    cols = nonzero[:, 1]
    x_vals = grid.min_x + ((cols.astype(float) + 0.5) * grid.resolution)
    y_vals = grid.max_y - ((rows.astype(float) + 0.5) * grid.resolution)
    centroid = f"{float(x_vals.mean()):.3f},{float(y_vals.mean()):.3f}"
    return centroid, int(nonzero.shape[0])


def _sanitize_vector_columns_for_gpkg(gdf):
    if gdf is None or gdf.empty:
        return gdf

    sanitized = gdf.copy()
    rename_map = {}
    for column in sanitized.columns:
        if column == "geometry":
            continue
        lowered = str(column).strip().lower()
        if lowered in {"fid", "ogc_fid"}:
            candidate = f"source_{lowered}"
            suffix = 1
            while candidate in sanitized.columns or candidate in rename_map.values():
                suffix += 1
                candidate = f"source_{lowered}_{suffix}"
            rename_map[column] = candidate
    if rename_map:
        sanitized = sanitized.rename(columns=rename_map)
    return sanitized


def rasterize_prepared_layer(prepared_layer: ArcGISPreparedLayer, grid: GridBuilder) -> ArcGISPreparedLayer:
    if prepared_layer.raster_path is None:
        return prepared_layer
    if gpd is None:
        raise ImportError("geopandas is required to rasterize processed ArcGIS vectors")

    gdf = gpd.read_file(prepared_layer.processed_vector_path)
    mask_data = rasterize_observation_layer(gdf, grid, prepared_layer.raster_path)
    sea_mask = load_sea_mask_array(grid.spec)
    if sea_mask is not None:
        mask_data = apply_ocean_mask(mask_data, sea_mask=sea_mask, fill_value=0.0)
        save_mask_raster(grid, mask_data.astype(np.float32), prepared_layer.raster_path)
    raster_centroid, raster_nonzero_cells = compute_raster_centroid(mask_data, grid)
    return replace(
        prepared_layer,
        raster_centroid=raster_centroid,
        raster_nonzero_cells=raster_nonzero_cells,
    )


class ArcGISFeatureServerClient:
    def __init__(self, timeout: int = 60):
        if gpd is None:
            raise ImportError("geopandas is required for ArcGIS ingestion")
        self.timeout = timeout

    def fetch_layer_metadata(self, layer: ArcGISLayerRef) -> dict:
        resp = requests.get(layer.metadata_url, params={"f": "json"}, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()

    def fetch_raw_geojson(self, layer: ArcGISLayerRef, where: str = "1=1", out_sr: int = 4326) -> dict:
        params = {
            "where": where,
            "outFields": "*",
            "returnGeometry": "true",
            "outSR": out_sr,
            "f": "geojson",
        }
        resp = requests.get(layer.query_url, params=params, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()

    def payload_to_gdf(self, payload: dict, source_crs: str):
        features = payload.get("features") or []
        if not features:
            return gpd.GeoDataFrame(geometry=[], crs=source_crs)
        gdf = gpd.GeoDataFrame.from_features(features)
        return gdf.set_crs(source_crs, allow_override=True)

    def prepare_layer(
        self,
        layer: ArcGISLayerRef,
        target_crs: str,
        grid: GridBuilder | None = None,
    ) -> ArcGISPreparedLayer:
        metadata = self.fetch_layer_metadata(layer)
        raw_payload = self.fetch_raw_geojson(layer, out_sr=4326)

        _save_json(metadata, layer.service_metadata_path)
        _save_json(raw_payload, layer.raw_geojson_path)

        raw_feature_count = _raw_feature_count(raw_payload)
        raw_gdf = self.payload_to_gdf(raw_payload, source_crs="EPSG:4326")
        inferred_raw_crs, notes = _infer_source_crs(raw_gdf, metadata, raw_payload)
        raw_gdf = raw_gdf.set_crs(inferred_raw_crs, allow_override=True)
        raw_gdf, repair_notes = _repair_degree_scaled_geometries(
            raw_gdf,
            metadata=metadata,
            payload=raw_payload,
            expected_region=layer.expected_region,
        )
        notes.extend(repair_notes)

        processed_gdf, qa = clean_arcgis_geometries(
            raw_gdf=raw_gdf,
            expected_geometry_type=layer.geometry_type,
            source_crs=inferred_raw_crs,
            target_crs=target_crs,
        )
        if processed_gdf.empty:
            raise ValueError(
                f"ArcGIS layer {layer.name} produced no valid {layer.geometry_type} geometries after cleaning."
            )

        layer.processed_vector_path.parent.mkdir(parents=True, exist_ok=True)
        if layer.processed_vector_path.exists():
            layer.processed_vector_path.unlink()
        processed_driver = "GPKG" if layer.official_mode else "GeoJSON"
        processed_to_write = _sanitize_vector_columns_for_gpkg(processed_gdf)
        processed_to_write.to_file(layer.processed_vector_path, driver=processed_driver)

        raster_path = layer.raster_path
        raster_centroid = ""
        raster_nonzero_cells = 0
        if raster_path is not None and grid is not None:
            mask_data = rasterize_observation_layer(processed_gdf, grid, raster_path)
            raster_centroid, raster_nonzero_cells = compute_raster_centroid(mask_data, grid)

        qa_note = (
            f"null_dropped={qa['null_geometries_dropped']}; "
            f"invalid_repaired={qa['invalid_geometries_repaired']}; "
            f"multipart_parts_exploded={qa['multipart_parts_exploded']}; "
            f"non_matching_parts_dropped={qa['non_matching_parts_dropped']}; "
            f"empty_dropped={qa['empty_geometries_dropped']}"
        )
        notes.append(qa_note)

        return ArcGISPreparedLayer(
            name=layer.name,
            layer_id=layer.layer_id,
            role=layer.role,
            geometry_type=layer.geometry_type,
            event_time_utc=layer.event_time_utc,
            raw_feature_count=raw_feature_count,
            processed_feature_count=int(len(processed_gdf)),
            raw_crs=inferred_raw_crs,
            processed_crs=target_crs,
            raw_geojson_path=layer.raw_geojson_path,
            processed_vector_path=layer.processed_vector_path,
            service_metadata_path=layer.service_metadata_path,
            raster_path=raster_path,
            vector_centroid=compute_vector_centroid(processed_gdf, layer.geometry_type),
            raster_centroid=raster_centroid,
            vector_area=compute_vector_area(processed_gdf, layer.geometry_type),
            raster_nonzero_cells=raster_nonzero_cells,
            notes=" | ".join(notes),
        )
