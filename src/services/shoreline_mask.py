"""
Canonical shoreline-mask generation for the official projected scoring grid.

This workflow archives a reproducible GSHHG shoreline source, clips it to the
canonical Mindoro scoring domain, rasterizes land/sea masks on the EPSG:32651
1 km grid, and writes machine-readable manifests describing the artifacts.
"""

from __future__ import annotations

import csv
import hashlib
import json
import os
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

import geopandas as gpd
import numpy as np
import pandas as pd
import requests
import rasterio
from rasterio.features import rasterize as rio_rasterize
from rasterio.transform import from_origin
from shapely.geometry import GeometryCollection, LineString, MultiLineString, box
from shapely.ops import substring

if TYPE_CHECKING:  # pragma: no cover - type-checking only
    from src.helpers.scoring import ScoringGridSpec


GSHHG_SOURCE_NAME = "GSHHG"
GSHHG_SOURCE_VERSION = "2.3.7"
GSHHG_SOURCE_URL = "https://ftp.soest.hawaii.edu/gshhg/gshhg-shp-2.3.7.zip"
GSHHG_RESOLUTION_CODE = "i"
GSHHG_LEVEL = 1
SHORELINE_SEGMENT_LENGTH_M = 1000.0
REQUEST_TIMEOUT_SECONDS = 120


def get_shoreline_artifact_paths(base_dir: str | Path | None = None) -> dict[str, Path]:
    base = Path(base_dir) if base_dir is not None else Path("data_processed") / "grids"
    raw_dir = base / "raw"
    extracted_dir = raw_dir / f"gshhg_shp_{GSHHG_RESOLUTION_CODE}_l{GSHHG_LEVEL}"
    archive_name = f"gshhg-shp-{GSHHG_SOURCE_VERSION}.zip"
    layer_name = f"GSHHS_{GSHHG_RESOLUTION_CODE}_L{GSHHG_LEVEL}"
    return {
        "base_dir": base,
        "raw_dir": raw_dir,
        "raw_archive": raw_dir / archive_name,
        "extracted_dir": extracted_dir,
        "source_shapefile": extracted_dir / f"{layer_name}.shp",
        "land_mask": base / "land_mask.tif",
        "sea_mask": base / "sea_mask.tif",
        "shoreline_segments": base / "shoreline_segments.gpkg",
        "manifest_json": base / "shoreline_mask_manifest.json",
        "manifest_csv": base / "shoreline_mask_manifest.csv",
    }


def load_shoreline_mask_manifest(
    manifest_path: str | Path | None = None,
    base_dir: str | Path | None = None,
) -> dict:
    path = Path(manifest_path) if manifest_path is not None else get_shoreline_artifact_paths(base_dir)["manifest_json"]
    if not path.exists():
        return {}
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f) or {}


def shoreline_mask_signature(base_dir: str | Path | None = None) -> str:
    manifest = load_shoreline_mask_manifest(base_dir=base_dir)
    return str(manifest.get("shoreline_mask_signature") or "")


def shoreline_mask_is_real(base_dir: str | Path | None = None) -> bool:
    manifest = load_shoreline_mask_manifest(base_dir=base_dir)
    if not manifest:
        return False
    return (
        int(manifest.get("land_cell_count", 0)) > 0
        and int(manifest.get("sea_cell_count", 0)) > 0
        and str(manifest.get("shoreline_mask_status") or "").startswith("gshhg_")
    )


def _write_json_atomic(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with open(temp_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
        f.write("\n")
    os.replace(temp_path, path)


def _write_csv_atomic(path: Path, row: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with open(temp_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(row.keys()))
        writer.writeheader()
        writer.writerow(row)
    os.replace(temp_path, path)


def _write_mask_raster(path: Path, spec: "ScoringGridSpec", data: np.ndarray) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f"{path.stem}__tmp{path.suffix}")
    transform = from_origin(spec.min_x, spec.max_y, spec.resolution, spec.resolution)
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
    if path.exists():
        path.unlink()
    temp_path.replace(path)


def _sha256_path(path: Path) -> str:
    hasher = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            hasher.update(chunk)
    return hasher.hexdigest()


def _download_archive(raw_archive_path: Path, force_refresh: bool = False) -> Path:
    if raw_archive_path.exists() and not force_refresh:
        return raw_archive_path

    raw_archive_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = raw_archive_path.with_name(f".{raw_archive_path.name}.{os.getpid()}.tmp")
    with requests.get(GSHHG_SOURCE_URL, stream=True, timeout=REQUEST_TIMEOUT_SECONDS) as response:
        response.raise_for_status()
        with open(temp_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
    os.replace(temp_path, raw_archive_path)
    return raw_archive_path


def _extract_source_layer(raw_archive_path: Path, extracted_dir: Path, force_refresh: bool = False) -> Path:
    source_shp = get_shoreline_artifact_paths(extracted_dir.parent.parent)["source_shapefile"]
    layer_name = source_shp.stem
    required_members = [f"GSHHS_shp/{GSHHG_RESOLUTION_CODE}/{layer_name}{suffix}" for suffix in (".dbf", ".prj", ".shp", ".shx")]

    if force_refresh and extracted_dir.exists():
        for existing in extracted_dir.glob("*"):
            existing.unlink()

    existing_source = extracted_dir / f"{layer_name}.shp"
    if existing_source.exists():
        return existing_source

    extracted_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(raw_archive_path) as archive:
        for member in required_members:
            archive.extract(member, extracted_dir.parent)

    nested_dir = extracted_dir.parent / "GSHHS_shp" / GSHHG_RESOLUTION_CODE
    for member in nested_dir.glob(f"{layer_name}.*"):
        target = extracted_dir / member.name
        if target.exists():
            target.unlink()
        member.replace(target)
    if (extracted_dir.parent / "GSHHS_shp").exists():
        import shutil

        shutil.rmtree(extracted_dir.parent / "GSHHS_shp", ignore_errors=True)

    return existing_source


def _iter_line_parts(geometry):
    if geometry is None or geometry.is_empty:
        return
    if isinstance(geometry, LineString):
        yield geometry
        return
    if isinstance(geometry, MultiLineString):
        for part in geometry.geoms:
            yield from _iter_line_parts(part)
        return
    if isinstance(geometry, GeometryCollection):
        for part in geometry.geoms:
            yield from _iter_line_parts(part)


def _segment_line(line: LineString, segment_length_m: float) -> list[LineString]:
    if line.is_empty or line.length <= 0:
        return []
    if line.length <= segment_length_m:
        return [line]

    parts: list[LineString] = []
    distance = 0.0
    while distance < line.length:
        end_distance = min(distance + segment_length_m, line.length)
        segment = substring(line, distance, end_distance)
        if isinstance(segment, LineString) and not segment.is_empty and segment.length > 0:
            parts.append(segment)
        distance = end_distance
    return parts


def _prepare_land_polygons(spec: "ScoringGridSpec", source_gdf: gpd.GeoDataFrame) -> tuple[gpd.GeoDataFrame, gpd.GeoDataFrame]:
    work = source_gdf.copy()
    if work.crs is None:
        work = work.set_crs("EPSG:4326")
    elif str(work.crs).upper() != "EPSG:4326":
        work = work.to_crs("EPSG:4326")

    clip_bounds = spec.display_bounds_wgs84 or [spec.min_lon, spec.max_lon, spec.min_lat, spec.max_lat]
    min_lon, max_lon, min_lat, max_lat = [float(value) for value in clip_bounds]
    clip_box = box(min_lon, min_lat, max_lon, max_lat)

    if not work.empty:
        work = work.cx[min_lon:max_lon, min_lat:max_lat]
    work = work.dropna(subset=["geometry"]).copy()
    if work.empty:
        raise RuntimeError("Shoreline source clip produced no geometry inside the scoring-grid display bounds.")

    work["geometry"] = work.geometry.intersection(clip_box)
    work = work[~work.geometry.is_empty].copy()
    if work.empty:
        raise RuntimeError("Shoreline source clip produced no valid geometry after intersection.")

    projected = work.to_crs(spec.crs)
    extent_geom = box(spec.min_x, spec.min_y, spec.max_x, spec.max_y)
    projected["geometry"] = projected.geometry.intersection(extent_geom)
    projected = projected[~projected.geometry.is_empty].copy()
    if projected.empty:
        raise RuntimeError("Projected shoreline clip produced no geometry inside the canonical scoring domain.")

    return work, projected


def _build_segment_gdf(projected_land: gpd.GeoDataFrame, spec: "ScoringGridSpec") -> gpd.GeoDataFrame:
    extent_geom = box(spec.min_x, spec.min_y, spec.max_x, spec.max_y)
    segment_records = []
    segment_index = 1
    for geometry in projected_land.geometry:
        shoreline_line = geometry.boundary.intersection(extent_geom)
        for line in _iter_line_parts(shoreline_line):
            for segment in _segment_line(line, SHORELINE_SEGMENT_LENGTH_M):
                segment_records.append(
                    {
                        "segment_id": f"shoreline_{segment_index:05d}",
                        "length_m": float(segment.length),
                        "geometry": segment,
                    }
                )
                segment_index += 1

    return gpd.GeoDataFrame(segment_records, geometry="geometry", crs=spec.crs)


def _rasterize_land_mask(projected_land: gpd.GeoDataFrame, spec: "ScoringGridSpec") -> tuple[np.ndarray, np.ndarray]:
    transform = from_origin(spec.min_x, spec.max_y, spec.resolution, spec.resolution)
    land_mask = rio_rasterize(
        [(geom, 1) for geom in projected_land.geometry if geom is not None and not geom.is_empty],
        out_shape=(spec.height, spec.width),
        transform=transform,
        fill=0,
        all_touched=True,
        dtype=np.uint8,
    )
    sea_mask = np.where(land_mask > 0, 0, 1).astype(np.uint8)
    return land_mask.astype(np.uint8), sea_mask


def build_shoreline_mask_artifacts(
    spec: "ScoringGridSpec",
    *,
    force_refresh: bool = False,
    artifact_dir: str | Path | None = None,
    land_source_gdf: gpd.GeoDataFrame | None = None,
) -> dict:
    paths = get_shoreline_artifact_paths(artifact_dir)
    if not force_refresh and shoreline_mask_is_real(paths["base_dir"]):
        return load_shoreline_mask_manifest(base_dir=paths["base_dir"])

    if land_source_gdf is None:
        raw_archive_path = _download_archive(paths["raw_archive"], force_refresh=force_refresh)
        source_shp = _extract_source_layer(raw_archive_path, paths["extracted_dir"], force_refresh=force_refresh)
        source_gdf = gpd.read_file(source_shp)
        source_name = GSHHG_SOURCE_NAME
        source_version = GSHHG_SOURCE_VERSION
        source_url = GSHHG_SOURCE_URL
        raw_archive = raw_archive_path
    else:
        source_gdf = land_source_gdf
        source_name = "test_land_source"
        source_version = "test"
        source_url = ""
        raw_archive = None

    clipped_wgs84, projected_land = _prepare_land_polygons(spec, source_gdf)
    land_mask, sea_mask = _rasterize_land_mask(projected_land, spec)
    shoreline_segments = _build_segment_gdf(projected_land, spec)

    if paths["shoreline_segments"].exists():
        paths["shoreline_segments"].unlink()
    shoreline_segments.to_file(paths["shoreline_segments"], driver="GPKG")
    _write_mask_raster(paths["land_mask"], spec, land_mask)
    _write_mask_raster(paths["sea_mask"], spec, sea_mask)

    land_cell_count = int(np.count_nonzero(land_mask))
    sea_cell_count = int(np.count_nonzero(sea_mask))
    segment_count = int(len(shoreline_segments))
    rasterization_rule = "land polygons rasterized with all_touched=true; land=1, sea=inverse(land)"
    known_limitations = [
        "Coastal cells are excluded conservatively when any land polygon touches the 1 km cell.",
        "The shoreline mask is clipped to the canonical scoring domain and does not extend beyond it.",
    ]
    clip_bounds = spec.display_bounds_wgs84 or [spec.min_lon, spec.max_lon, spec.min_lat, spec.max_lat]
    generated_at_utc = datetime.now(timezone.utc).isoformat()
    signature_payload = {
        "source_name": source_name,
        "source_version": source_version,
        "resolution_code": GSHHG_RESOLUTION_CODE,
        "level": GSHHG_LEVEL,
        "reprojection_crs": spec.crs,
        "clip_bounds_wgs84": clip_bounds,
        "grid_extent_projected": [spec.min_x, spec.max_x, spec.min_y, spec.max_y],
        "rasterization_rule": rasterization_rule,
        "land_cell_count": land_cell_count,
        "sea_cell_count": sea_cell_count,
        "segment_count": segment_count,
        "land_mask_sha256": hashlib.sha256(land_mask.tobytes()).hexdigest(),
        "sea_mask_sha256": hashlib.sha256(sea_mask.tobytes()).hexdigest(),
    }
    signature = hashlib.sha256(json.dumps(signature_payload, sort_keys=True).encode("utf-8")).hexdigest()
    status = f"gshhg_{source_version}_resolution_{GSHHG_RESOLUTION_CODE}_level_{GSHHG_LEVEL}_all_touched"

    manifest = {
        "generated_at_utc": generated_at_utc,
        "shoreline_source": source_name,
        "source_version": source_version,
        "source_url": source_url,
        "raw_archive_path": str(raw_archive) if raw_archive else "",
        "raw_archive_sha256": _sha256_path(raw_archive) if raw_archive and raw_archive.exists() else "",
        "source_shapefile_path": str(paths["source_shapefile"]) if land_source_gdf is None else "",
        "clipping_domain_used_wgs84": [float(v) for v in clip_bounds],
        "clipping_domain_used_projected": [float(spec.min_x), float(spec.max_x), float(spec.min_y), float(spec.max_y)],
        "reprojection_crs": spec.crs,
        "rasterization_rule": rasterization_rule,
        "land_cell_count": land_cell_count,
        "sea_cell_count": sea_cell_count,
        "segment_count": segment_count,
        "segment_length_m": float(SHORELINE_SEGMENT_LENGTH_M),
        "shoreline_segments_path": str(paths["shoreline_segments"]),
        "land_mask_path": str(paths["land_mask"]),
        "sea_mask_path": str(paths["sea_mask"]),
        "shoreline_mask_status": status,
        "shoreline_mask_signature": signature,
        "known_limitations": known_limitations,
        "wgs84_feature_count": int(len(clipped_wgs84)),
        "projected_feature_count": int(len(projected_land)),
    }
    _write_json_atomic(paths["manifest_json"], manifest)
    _write_csv_atomic(
        paths["manifest_csv"],
        {
            "generated_at_utc": generated_at_utc,
            "shoreline_source": source_name,
            "source_version": source_version,
            "source_url": source_url,
            "raw_archive_path": str(raw_archive) if raw_archive else "",
            "clipping_domain_used_wgs84": json.dumps([float(v) for v in clip_bounds]),
            "clipping_domain_used_projected": json.dumps([float(spec.min_x), float(spec.max_x), float(spec.min_y), float(spec.max_y)]),
            "reprojection_crs": spec.crs,
            "rasterization_rule": rasterization_rule,
            "land_cell_count": land_cell_count,
            "sea_cell_count": sea_cell_count,
            "segment_count": segment_count,
            "known_limitations": " | ".join(known_limitations),
            "shoreline_mask_status": status,
            "shoreline_mask_signature": signature,
        },
    )
    return manifest
