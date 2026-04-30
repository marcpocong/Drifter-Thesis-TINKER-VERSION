#!/usr/bin/env python3
"""Build a public March 12-derived Mindoro NOAA/NESDIS product board."""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional, Union

import geopandas as gpd
import matplotlib
import requests
from shapely.geometry import box

from src.services.arcgis import (
    _infer_source_crs,
    _repair_degree_scaled_geometries,
    _sanitize_vector_columns_for_gpkg,
    clean_arcgis_geometries,
)

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402


REPO_ROOT = Path(__file__).resolve().parents[1]
LAND_CONTEXT_PATH = REPO_ROOT / "data_processed" / "reference" / "study_box_land_context.geojson"
DEFAULT_OUTPUT_DIR = REPO_ROOT / "output" / "experiments" / "mindoro_march12_derived_product_board"
DEFAULT_EXPECTED_REGION = [115.0, 122.0, 6.0, 14.5]
REQUEST_TIMEOUT = 60

FIGURE_FILENAME = "mindoro_march12_derived_product_board.png"
MANIFEST_FILENAME = "mindoro_march12_derived_product_manifest.json"
SOURCE_PRODUCT_FILENAME = "mindoro_march12_source_product.png"
SOURCE_MANIFEST_FILENAME = "mindoro_march12_source_product_manifest.json"
README_FILENAME = "README.md"

SOURCE_LAYER_SPEC = {
    "cache_key": "march12_noaa_source_product",
    "label": "March 12 NOAA source product",
    "panel_title": "March 12 NOAA source product",
    "panel_subtitle": "Public feature service dated 2023-03-12",
    "source_name": "MindoroOilSpill_NOAA_230312",
    "provider": "NOAA/NESDIS",
    "service_url": "https://services1.arcgis.com/RTK5Unh1Z71JKIiR/arcgis/rest/services/MindoroOilSpill_NOAA_230312/FeatureServer",
    "item_id": "8ed68b6d973746ae86f40721a761f114",
    "observation_date": "2023-03-12",
    "facecolor": "#7C3AED",
    "edgecolor": "#5B21B6",
}

LAYER_SPECS = [
    {
        "cache_key": "march13_noaa_seed_product",
        "label": "March 13 NOAA seed product",
        "panel_title": "March 13 NOAA seed",
        "panel_subtitle": "Feature service product citing WorldView-3 acquired 2023-03-12",
        "source_name": "MindoroOilSpill_NOAA_230313",
        "provider": "NOAA/NESDIS",
        "service_url": "https://services1.arcgis.com/RTK5Unh1Z71JKIiR/arcgis/rest/services/MindoroOilSpill_NOAA_230313/FeatureServer",
        "item_id": "8f8e3944748c4772910efc9829497e20",
        "observation_date": "2023-03-13",
        "facecolor": "#4F86F7",
        "edgecolor": "#1D4ED8",
    },
    {
        "cache_key": "march14_noaa_target_product",
        "label": "March 14 NOAA target product",
        "panel_title": "March 14 NOAA target",
        "panel_subtitle": "Feature service product citing WorldView-3 acquired 2023-03-12",
        "source_name": "MindoroOilSpill_NOAA_230314",
        "provider": "NOAA/NESDIS",
        "service_url": "https://services1.arcgis.com/RTK5Unh1Z71JKIiR/arcgis/rest/services/Possible_Oil_Spills_March_14/FeatureServer",
        "item_id": "10b37c42a9754363a5f7b14199b077e6",
        "observation_date": "2023-03-14",
        "facecolor": "#E11D48",
        "edgecolor": "#9F1239",
    },
]


@dataclass(frozen=True)
class ProductLayer:
    cache_key: str
    label: str
    panel_title: str
    panel_subtitle: str
    source_name: str
    provider: str
    service_url: str
    item_id: str
    observation_date: str
    facecolor: str
    edgecolor: str


@dataclass(frozen=True)
class CachedLayerPaths:
    cache_dir: Path
    raw_geojson: Path
    layer_metadata: Path
    item_metadata: Path
    processed_vector: Path


@dataclass(frozen=True)
class MaterializedLayer:
    layer: ProductLayer
    geometry: gpd.GeoDataFrame
    layer_id: int
    source_crs: str
    raw_feature_count: int
    processed_feature_count: int
    notes: list[str]
    layer_metadata: dict[str, Any]
    item_metadata: dict[str, Any]
    paths: CachedLayerPaths


def _json_default(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, tuple):
        return list(value)
    return value


def _write_json(path: Path, payload: Union[dict[str, Any], list[dict[str, Any]]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, default=_json_default) + "\n", encoding="utf-8")


def _default_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": "mindoro-march12-derived-product-board/1.0"})
    return session


def _request_json(session: requests.Session, url: str, *, params: dict[str, Any]) -> dict[str, Any]:
    response = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    return response.json() or {}


def _request_item_json(session: requests.Session, item_id: str) -> dict[str, Any]:
    return _request_json(
        session,
        f"https://www.arcgis.com/sharing/rest/content/items/{item_id}",
        params={"f": "json"},
    )


def _resolve_service_layer_id(layer: ProductLayer, session: requests.Session) -> int:
    metadata = _request_json(session, layer.service_url, params={"f": "json"})
    service_layers = metadata.get("layers") or []
    if len(service_layers) == 1 and service_layers[0].get("id") is not None:
        return int(service_layers[0]["id"])
    if metadata.get("id") is not None and metadata.get("geometryType"):
        return int(metadata["id"])
    raise RuntimeError(f"Could not resolve a single layer id for {layer.source_name}")


def _cached_layer_paths(output_dir: Path, layer: ProductLayer) -> CachedLayerPaths:
    cache_dir = output_dir / "cache" / layer.cache_key
    return CachedLayerPaths(
        cache_dir=cache_dir,
        raw_geojson=cache_dir / f"{layer.cache_key}_raw.geojson",
        layer_metadata=cache_dir / f"{layer.cache_key}_layer_metadata.json",
        item_metadata=cache_dir / f"{layer.cache_key}_item_metadata.json",
        processed_vector=cache_dir / f"{layer.cache_key}_processed.gpkg",
    )


def _materialize_layer(
    layer: ProductLayer,
    *,
    output_dir: Path,
    force_refresh: bool,
    session: requests.Session,
    expected_region: list[float],
) -> MaterializedLayer:
    paths = _cached_layer_paths(output_dir, layer)
    if not force_refresh and all(
        path.exists()
        for path in (paths.raw_geojson, paths.layer_metadata, paths.item_metadata, paths.processed_vector)
    ):
        geometry = gpd.read_file(paths.processed_vector).to_crs("EPSG:4326")
        raw_geojson = json.loads(paths.raw_geojson.read_text(encoding="utf-8"))
        layer_metadata = json.loads(paths.layer_metadata.read_text(encoding="utf-8"))
        item_metadata = json.loads(paths.item_metadata.read_text(encoding="utf-8"))
        return MaterializedLayer(
            layer=layer,
            geometry=geometry,
            layer_id=int(layer_metadata.get("id", 0)),
            source_crs="EPSG:4326",
            raw_feature_count=int(len(raw_geojson.get("features") or [])),
            processed_feature_count=int(len(geometry.index)),
            notes=["reused cached derived product layer"],
            layer_metadata=layer_metadata,
            item_metadata=item_metadata,
            paths=paths,
        )

    paths.cache_dir.mkdir(parents=True, exist_ok=True)
    layer_id = _resolve_service_layer_id(layer, session)
    layer_url = f"{layer.service_url.rstrip('/')}/{layer_id}"
    layer_metadata = _request_json(session, layer_url, params={"f": "json"})
    item_metadata = _request_item_json(session, layer.item_id)
    raw_geojson = _request_json(
        session,
        f"{layer_url}/query",
        params={
            "where": "1=1",
            "outFields": "*",
            "returnGeometry": "true",
            "outSR": 4326,
            "f": "geojson",
        },
    )
    raw_features = raw_geojson.get("features") or []
    if not raw_features:
        raise RuntimeError(f"{layer.source_name} returned no features from {layer_url}")

    raw_gdf = gpd.GeoDataFrame.from_features(raw_features)
    raw_gdf = raw_gdf.set_crs("EPSG:4326", allow_override=True)
    source_crs, notes = _infer_source_crs(raw_gdf, layer_metadata, raw_geojson)
    raw_gdf = raw_gdf.set_crs(source_crs, allow_override=True)
    raw_gdf, repair_notes = _repair_degree_scaled_geometries(
        raw_gdf,
        metadata=layer_metadata,
        payload=raw_geojson,
        expected_region=expected_region,
    )
    notes.extend(repair_notes)
    cleaned_gdf, qa = clean_arcgis_geometries(
        raw_gdf=raw_gdf,
        expected_geometry_type="polygon",
        source_crs=source_crs,
        target_crs="EPSG:4326",
    )
    if cleaned_gdf.empty:
        raise RuntimeError(f"{layer.source_name} produced no valid polygon geometry after cleaning.")

    notes.append(
        "qa "
        f"null_dropped={qa['null_geometries_dropped']} "
        f"invalid_repaired={qa['invalid_geometries_repaired']} "
        f"multipart_parts_exploded={qa['multipart_parts_exploded']} "
        f"non_matching_parts_dropped={qa['non_matching_parts_dropped']} "
        f"empty_dropped={qa['empty_geometries_dropped']}"
    )

    _write_json(paths.raw_geojson, raw_geojson)
    _write_json(paths.layer_metadata, layer_metadata)
    _write_json(paths.item_metadata, item_metadata)
    cleaned_to_write = _sanitize_vector_columns_for_gpkg(cleaned_gdf)
    if paths.processed_vector.exists():
        paths.processed_vector.unlink()
    cleaned_to_write.to_file(paths.processed_vector, driver="GPKG")

    return MaterializedLayer(
        layer=layer,
        geometry=cleaned_gdf,
        layer_id=layer_id,
        source_crs=source_crs,
        raw_feature_count=int(len(raw_features)),
        processed_feature_count=int(len(cleaned_gdf.index)),
        notes=notes,
        layer_metadata=layer_metadata,
        item_metadata=item_metadata,
        paths=paths,
    )


def _load_land_context(bounds: tuple[float, float, float, float]) -> Optional[gpd.GeoDataFrame]:
    if not LAND_CONTEXT_PATH.exists():
        return None
    try:
        land = gpd.read_file(LAND_CONTEXT_PATH, bbox=bounds)
    except Exception:
        land = gpd.read_file(LAND_CONTEXT_PATH)
    if land.empty:
        return land
    if land.crs is not None and str(land.crs).upper() != "EPSG:4326":
        land = land.to_crs("EPSG:4326")
    return land.clip(box(*bounds))


def _combined_bounds(materialized_layers: list[MaterializedLayer]) -> tuple[float, float, float, float]:
    min_x = min(layer.geometry.total_bounds[0] for layer in materialized_layers)
    min_y = min(layer.geometry.total_bounds[1] for layer in materialized_layers)
    max_x = max(layer.geometry.total_bounds[2] for layer in materialized_layers)
    max_y = max(layer.geometry.total_bounds[3] for layer in materialized_layers)
    pad_x = max((max_x - min_x) * 0.12, 0.03)
    pad_y = max((max_y - min_y) * 0.12, 0.03)
    return min_x - pad_x, min_y - pad_y, max_x + pad_x, max_y + pad_y


def _render_panel(
    ax: plt.Axes,
    *,
    materialized: MaterializedLayer,
    bounds: tuple[float, float, float, float],
    land: Optional[gpd.GeoDataFrame],
) -> None:
    ax.set_facecolor("#EAF5FB")
    if land is not None and not land.empty:
        land.plot(ax=ax, color="#F5EBD8", edgecolor="#CCBDA0", linewidth=0.4, zorder=1)

    materialized.geometry.boundary.plot(
        ax=ax,
        color="#FFFFFF",
        linewidth=3.4,
        alpha=0.96,
        zorder=3,
    )
    materialized.geometry.plot(
        ax=ax,
        facecolor=materialized.layer.facecolor,
        edgecolor=materialized.layer.edgecolor,
        linewidth=1.7,
        alpha=0.34,
        zorder=4,
    )

    ax.set_xlim(bounds[0], bounds[2])
    ax.set_ylim(bounds[1], bounds[3])
    ax.set_aspect("equal", adjustable="box")
    ax.grid(color="#9DB4C0", linestyle="--", linewidth=0.45, alpha=0.35)
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    ax.set_title(
        f"{materialized.layer.panel_title}\n{materialized.layer.panel_subtitle}",
        loc="left",
        fontsize=10.5,
        color="#0F172A",
        pad=9,
    )


def render_board(
    materialized_layers: list[MaterializedLayer],
    *,
    output_path: Path,
) -> tuple[float, float, float, float]:
    if len(materialized_layers) != 2:
        raise ValueError("Expected exactly two derived product layers.")

    bounds = _combined_bounds(materialized_layers)
    land = _load_land_context(bounds)
    fig, axes = plt.subplots(1, 2, figsize=(13.0, 8.8), dpi=220, facecolor="#FFFFFF")

    for ax, layer in zip(axes, materialized_layers):
        _render_panel(ax, materialized=layer, bounds=bounds, land=land)

    fig.suptitle(
        "Mindoro March 12 WorldView-3 Public Product Board",
        x=0.055,
        y=0.985,
        ha="left",
        fontsize=18,
        fontweight="bold",
        color="#0F172A",
    )
    fig.text(
        0.055,
        0.94,
        "Public NOAA/NESDIS delineation products whose preserved metadata cite WorldView-3 acquired on 2023-03-12",
        ha="left",
        va="top",
        fontsize=10,
        color="#334155",
        bbox={"boxstyle": "round,pad=0.30", "facecolor": "#FFFFFF", "edgecolor": "#CBD5E1", "alpha": 0.95},
    )
    fig.text(
        0.945,
        0.94,
        "Derived product board\nNot raw satellite raster imagery",
        ha="right",
        va="top",
        fontsize=9.5,
        color="#334155",
        bbox={"boxstyle": "round,pad=0.30", "facecolor": "#FFFFFF", "edgecolor": "#CBD5E1", "alpha": 0.95},
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.tight_layout(rect=(0.0, 0.0, 1.0, 0.91), pad=0.8)
    fig.savefig(output_path, bbox_inches="tight", pad_inches=0.08)
    plt.close(fig)
    return bounds


def render_source_product(
    materialized: MaterializedLayer,
    *,
    output_path: Path,
) -> tuple[float, float, float, float]:
    bounds = _combined_bounds([materialized])
    land = _load_land_context(bounds)
    fig, ax = plt.subplots(figsize=(7.5, 8.8), dpi=220, facecolor="#FFFFFF")

    _render_panel(ax, materialized=materialized, bounds=bounds, land=land)

    fig.suptitle(
        "Mindoro March 12 Public Source Product",
        x=0.08,
        y=0.985,
        ha="left",
        fontsize=18,
        fontweight="bold",
        color="#0F172A",
    )
    fig.text(
        0.08,
        0.94,
        "Public NOAA/NESDIS feature-service product dated 2023-03-12",
        ha="left",
        va="top",
        fontsize=10,
        color="#334155",
        bbox={"boxstyle": "round,pad=0.30", "facecolor": "#FFFFFF", "edgecolor": "#CBD5E1", "alpha": 0.95},
    )
    fig.text(
        0.92,
        0.94,
        "Closest public source artifact\nNot raw WorldView-3 raster imagery",
        ha="right",
        va="top",
        fontsize=9.5,
        color="#334155",
        bbox={"boxstyle": "round,pad=0.30", "facecolor": "#FFFFFF", "edgecolor": "#CBD5E1", "alpha": 0.95},
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.tight_layout(rect=(0.0, 0.0, 1.0, 0.91), pad=0.8)
    fig.savefig(output_path, bbox_inches="tight", pad_inches=0.08)
    plt.close(fig)
    return bounds


def _top_level_item_metadata_path(output_dir: Path, basename: str) -> Path:
    return output_dir / basename


def _write_readme(
    output_dir: Path,
    *,
    board_manifest_path: Path,
    source_manifest_path: Path,
) -> None:
    readme = "\n".join(
        [
            "# Mindoro March 12 Public Source Package",
            "",
            "- `mindoro_march12_source_product.png`: public March 12 NOAA/NESDIS source product rendered from the `MindoroOilSpill_NOAA_230312` feature service.",
            "- `mindoro_march12_derived_product_board.png`: the March 13 seed and March 14 target public products that both preserve metadata citing `WorldView-3` acquired on `2023-03-12`.",
            "- `march12_source_product_item_metadata.json`: public ArcGIS item metadata for the March 12 source product.",
            "- `march13_seed_item_metadata.json` and `march14_target_item_metadata.json`: preserved item metadata that contain the `WorldView-3` and `Acquired: 12/03/2023` citation text.",
            "",
            "Important note:",
            "The raw March 12 WorldView-3 raster image is not stored in this repo and is not openly exposed by the public FeatureServer workflow used here. These files are the closest honest public source artifacts available from the same workflow.",
            "",
            f"- Board manifest: `{board_manifest_path.name}`",
            f"- Source-product manifest: `{source_manifest_path.name}`",
        ]
    )
    (output_dir / README_FILENAME).write_text(readme + "\n", encoding="utf-8")


def generate_board(
    *,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    force_refresh: bool = False,
    session: Optional[requests.Session] = None,
    expected_region: Optional[list[float]] = None,
) -> dict[str, Any]:
    output_dir = output_dir.resolve()
    expected_region = list(expected_region or DEFAULT_EXPECTED_REGION)
    active_session = session or _default_session()
    selected_layers = [ProductLayer(**spec) for spec in LAYER_SPECS]
    materialized_layers = [
        _materialize_layer(
            layer,
            output_dir=output_dir,
            force_refresh=force_refresh,
            session=active_session,
            expected_region=expected_region,
        )
        for layer in selected_layers
    ]
    source_layer = _materialize_layer(
        ProductLayer(**SOURCE_LAYER_SPEC),
        output_dir=output_dir,
        force_refresh=force_refresh,
        session=active_session,
        expected_region=expected_region,
    )

    image_path = output_dir / FIGURE_FILENAME
    plot_bounds = render_board(materialized_layers, output_path=image_path)
    source_image_path = output_dir / SOURCE_PRODUCT_FILENAME
    source_plot_bounds = render_source_product(source_layer, output_path=source_image_path)
    manifest_path = output_dir / MANIFEST_FILENAME
    manifest = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "experiment_id": "mindoro_march12_derived_product_board",
        "output_image": str(image_path),
        "source_product_image": str(source_image_path),
        "plot_bounds_wgs84": list(plot_bounds),
        "source_product_bounds_wgs84": list(source_plot_bounds),
        "force_refresh": bool(force_refresh),
        "note": (
            "This board packages public NOAA/NESDIS delineation products whose preserved metadata cite "
            "WorldView-3 acquired on 2023-03-12. It is not the raw March 12 satellite raster."
        ),
        "selected_layers": [
            {
                "label": item.layer.label,
                "panel_title": item.layer.panel_title,
                "source_name": item.layer.source_name,
                "provider": item.layer.provider,
                "observation_date": item.layer.observation_date,
                "service_url": item.layer.service_url,
                "item_id": item.layer.item_id,
                "resolved_layer_id": int(item.layer_id),
                "source_crs": item.source_crs,
                "raw_feature_count": int(item.raw_feature_count),
                "processed_feature_count": int(item.processed_feature_count),
                "item_title": str(item.item_metadata.get("title") or ""),
                "item_snippet": str(item.item_metadata.get("snippet") or ""),
                "item_description_html": str(item.item_metadata.get("description") or ""),
                "cache": {
                    "cache_dir": str(item.paths.cache_dir),
                    "raw_geojson": str(item.paths.raw_geojson),
                    "layer_metadata": str(item.paths.layer_metadata),
                    "item_metadata": str(item.paths.item_metadata),
                    "processed_vector": str(item.paths.processed_vector),
                },
                "notes": item.notes,
            }
            for item in materialized_layers
        ],
    }
    _write_json(manifest_path, manifest)
    manifest["manifest_path"] = str(manifest_path)

    source_manifest_path = output_dir / SOURCE_MANIFEST_FILENAME
    source_manifest = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "experiment_id": "mindoro_march12_source_product",
        "output_image": str(source_image_path),
        "plot_bounds_wgs84": list(source_plot_bounds),
        "note": (
            "This is the public March 12 NOAA/NESDIS source product rendered from the public FeatureServer layer. "
            "It is the closest public source artifact available in this workflow, but it is still not the raw "
            "WorldView-3 raster image itself."
        ),
        "source_layer": {
            "label": source_layer.layer.label,
            "source_name": source_layer.layer.source_name,
            "provider": source_layer.layer.provider,
            "observation_date": source_layer.layer.observation_date,
            "service_url": source_layer.layer.service_url,
            "item_id": source_layer.layer.item_id,
            "resolved_layer_id": int(source_layer.layer_id),
            "source_crs": source_layer.source_crs,
            "raw_feature_count": int(source_layer.raw_feature_count),
            "processed_feature_count": int(source_layer.processed_feature_count),
            "item_title": str(source_layer.item_metadata.get("title") or ""),
            "item_snippet": str(source_layer.item_metadata.get("snippet") or ""),
            "item_description_html": str(source_layer.item_metadata.get("description") or ""),
            "cache": {
                "cache_dir": str(source_layer.paths.cache_dir),
                "raw_geojson": str(source_layer.paths.raw_geojson),
                "layer_metadata": str(source_layer.paths.layer_metadata),
                "item_metadata": str(source_layer.paths.item_metadata),
                "processed_vector": str(source_layer.paths.processed_vector),
            },
            "notes": source_layer.notes,
        },
        "shared_imagery_evidence_from_related_products": [
            {
                "source_name": item.layer.source_name,
                "observation_date": item.layer.observation_date,
                "item_id": item.layer.item_id,
                "item_title": str(item.item_metadata.get("title") or ""),
                "item_description_html": str(item.item_metadata.get("description") or ""),
            }
            for item in materialized_layers
        ],
    }
    _write_json(source_manifest_path, source_manifest)

    _write_json(
        _top_level_item_metadata_path(output_dir, "march12_source_product_item_metadata.json"),
        source_layer.item_metadata,
    )
    _write_json(
        _top_level_item_metadata_path(output_dir, "march13_seed_item_metadata.json"),
        materialized_layers[0].item_metadata,
    )
    _write_json(
        _top_level_item_metadata_path(output_dir, "march14_target_item_metadata.json"),
        materialized_layers[1].item_metadata,
    )
    _write_readme(
        output_dir,
        board_manifest_path=manifest_path,
        source_manifest_path=source_manifest_path,
    )
    return manifest


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--force-refresh", action="store_true")
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    parser = build_argument_parser()
    args = parser.parse_args(argv)
    result = generate_board(output_dir=args.output_dir, force_refresh=args.force_refresh)
    print(json.dumps(result, indent=2, default=_json_default))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
