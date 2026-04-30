#!/usr/bin/env python3
"""Build an experimental single-map Mindoro March 3-6 observation overlay."""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional, Union

import geopandas as gpd
import matplotlib
import pandas as pd
import requests
from matplotlib.lines import Line2D
from matplotlib.patches import Patch
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
INVENTORY_PATH = REPO_ROOT / "output" / "final_validation_package" / "final_validation_observation_table.csv"
LAND_CONTEXT_PATH = REPO_ROOT / "data_processed" / "reference" / "study_box_land_context.geojson"
DEFAULT_OUTPUT_DIR = REPO_ROOT / "output" / "experiments" / "mindoro_march3_6_observation_overlay"
DEFAULT_EXPECTED_REGION = [115.0, 122.0, 6.0, 14.5]
REQUEST_TIMEOUT = 60

FIGURE_FILENAME = "mindoro_march3_6_observation_overlay.png"
MANIFEST_FILENAME = "mindoro_march3_6_observation_overlay_manifest.json"

SELECTED_LAYER_SPECS = [
    {
        "observation_date": "2023-03-03",
        "provider": "PhilSA",
        "source_name": "MindoroOilSpill_Philsa_230303",
        "label": "2023-03-03 PhilSA",
        "mode": "filled",
        "facecolor": "#4F86F7",
        "edgecolor": "#1D4ED8",
        "alpha": 0.26,
        "linewidth": 2.2,
        "linestyle": "solid",
    },
    {
        "observation_date": "2023-03-03",
        "provider": "WWF Philippines",
        "source_name": "Possible_oil_Spills_(March_3,_2023)",
        "label": "2023-03-03 WWF",
        "mode": "outline",
        "facecolor": "none",
        "edgecolor": "#60A5FA",
        "alpha": 1.0,
        "linewidth": 2.7,
        "linestyle": "--",
    },
    {
        "observation_date": "2023-03-04",
        "provider": "PhilSA",
        "source_name": "MindoroOilSpill_Philsa_230304",
        "label": "2023-03-04 PhilSA",
        "mode": "filled",
        "facecolor": "#22C55E",
        "edgecolor": "#15803D",
        "alpha": 0.24,
        "linewidth": 2.2,
        "linestyle": "solid",
    },
    {
        "observation_date": "2023-03-05",
        "provider": "UP MSI",
        "source_name": "MindoroOilSpill_MSI_230305",
        "label": "2023-03-05 UP MSI",
        "mode": "filled",
        "facecolor": "#F59E0B",
        "edgecolor": "#B45309",
        "alpha": 0.08,
        "linewidth": 1.6,
        "linestyle": "solid",
    },
    {
        "observation_date": "2023-03-06",
        "provider": "WWF Philippines",
        "source_name": "Possible_oil_slick_(March_6,_2023)",
        "label": "2023-03-06 WWF",
        "mode": "filled",
        "facecolor": "#E11D48",
        "edgecolor": "#9F1239",
        "alpha": 0.22,
        "linewidth": 2.3,
        "linestyle": "solid",
    },
]

SELECTED_LAYER_LOOKUP = {
    (spec["observation_date"], spec["provider"], spec["source_name"]): spec for spec in SELECTED_LAYER_SPECS
}


@dataclass(frozen=True)
class ObservationLayer:
    observation_date: str
    provider: str
    source_name: str
    source_type: str
    truth_status: str
    observation_usage: str
    source_url: str
    service_url: str

    @property
    def cache_key(self) -> str:
        joined = "__".join(
            [
                self.observation_date,
                self.provider,
                self.source_name,
            ]
        )
        return "".join(char.lower() if char.isalnum() else "_" for char in joined).strip("_")

    @property
    def style(self) -> dict[str, Any]:
        return dict(SELECTED_LAYER_LOOKUP[(self.observation_date, self.provider, self.source_name)])


@dataclass(frozen=True)
class CachedLayerPaths:
    cache_dir: Path
    raw_geojson: Path
    layer_metadata: Path
    processed_vector: Path


@dataclass(frozen=True)
class MaterializedLayer:
    layer: ObservationLayer
    geometry: gpd.GeoDataFrame
    layer_id: int
    source_crs: str
    raw_feature_count: int
    processed_feature_count: int
    notes: list[str]
    paths: CachedLayerPaths


def _json_default(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, tuple):
        return list(value)
    return value


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    if isinstance(value, (int, float)):
        return bool(value)
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _read_inventory(inventory_path: Path) -> pd.DataFrame:
    if not inventory_path.exists():
        raise FileNotFoundError(f"Observation inventory not found: {inventory_path}")
    return pd.read_csv(inventory_path)


def load_selected_observation_layers(inventory_path: Path = INVENTORY_PATH) -> list[ObservationLayer]:
    frame = _read_inventory(inventory_path)
    selected: list[ObservationLayer] = []

    for spec in SELECTED_LAYER_SPECS:
        matches = frame.loc[
            frame["case_id"].astype(str).eq("CASE_MINDORO_RETRO_2023")
            & frame["observation_date"].astype(str).eq(spec["observation_date"])
            & frame["provider"].astype(str).eq(spec["provider"])
            & frame["source_name"].astype(str).eq(spec["source_name"])
        ].copy()

        if len(matches.index) != 1:
            raise ValueError(
                "Expected exactly one inventory row for "
                f"{spec['observation_date']} / {spec['provider']} / {spec['source_name']}; found {len(matches.index)}."
            )

        row = matches.iloc[0]
        if not _as_bool(row.get("machine_readable")):
            raise ValueError(f"{spec['source_name']} is not machine-readable in the observation inventory.")
        if not _as_bool(row.get("observation_derived")):
            raise ValueError(f"{spec['source_name']} is not observation-derived in the observation inventory.")
        if str(row.get("source_type", "")).strip().lower() not in {"feature service", "feature layer"}:
            raise ValueError(f"{spec['source_name']} is not a polygon-like feature service/layer row.")
        if str(row.get("truth_status", "")).strip().lower() == "context_only":
            raise ValueError(f"{spec['source_name']} is marked context-only and must be excluded.")
        if str(row.get("observation_usage", "")).strip().lower() in {"provenance_only", "qualitative_context"}:
            raise ValueError(f"{spec['source_name']} is not a direct observation polygon row.")

        selected.append(
            ObservationLayer(
                observation_date=str(row["observation_date"]),
                provider=str(row["provider"]),
                source_name=str(row["source_name"]),
                source_type=str(row["source_type"]),
                truth_status=str(row["truth_status"]),
                observation_usage=str(row["observation_usage"]),
                source_url=str(row["source_url"]),
                service_url=str(row["service_url"]),
            )
        )

    return selected


def _request_json(session: requests.Session, url: str, *, params: dict[str, Any]) -> dict[str, Any]:
    response = session.get(url, params=params, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    return response.json() or {}


def _resolve_layer_endpoint(layer: ObservationLayer, session: requests.Session) -> tuple[str, int]:
    source_url = layer.source_url.rstrip("/")
    service_url = (layer.service_url or layer.source_url).rstrip("/")
    tail = source_url.rsplit("/", 1)[-1]
    if tail.isdigit():
        return service_url, int(tail)

    root_metadata = _request_json(session, service_url, params={"f": "json"})
    layers = root_metadata.get("layers") or []
    if len(layers) == 1 and layers[0].get("id") is not None:
        return service_url, int(layers[0]["id"])
    if root_metadata.get("id") is not None and root_metadata.get("geometryType"):
        return service_url.rsplit("/", 1)[0], int(root_metadata["id"])
    raise RuntimeError(f"Could not resolve a single ArcGIS layer for {layer.source_name} at {service_url}")


def _cached_layer_paths(output_dir: Path, layer: ObservationLayer) -> CachedLayerPaths:
    cache_dir = output_dir / "cache" / layer.cache_key
    return CachedLayerPaths(
        cache_dir=cache_dir,
        raw_geojson=cache_dir / f"{layer.cache_key}_raw.geojson",
        layer_metadata=cache_dir / f"{layer.cache_key}_layer_metadata.json",
        processed_vector=cache_dir / f"{layer.cache_key}_processed.gpkg",
    )


def _write_json(path: Path, payload: Union[dict[str, Any], list[dict[str, Any]]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, default=_json_default) + "\n", encoding="utf-8")


def _materialize_layer(
    layer: ObservationLayer,
    *,
    output_dir: Path,
    force_refresh: bool,
    session: requests.Session,
    expected_region: list[float],
) -> MaterializedLayer:
    paths = _cached_layer_paths(output_dir, layer)
    if not force_refresh and paths.processed_vector.exists() and paths.layer_metadata.exists() and paths.raw_geojson.exists():
        geometry = gpd.read_file(paths.processed_vector).to_crs("EPSG:4326")
        cached_metadata = json.loads(paths.layer_metadata.read_text(encoding="utf-8"))
        raw_geojson = json.loads(paths.raw_geojson.read_text(encoding="utf-8"))
        return MaterializedLayer(
            layer=layer,
            geometry=geometry,
            layer_id=int(cached_metadata.get("id", 0)),
            source_crs="EPSG:4326",
            raw_feature_count=int(len(raw_geojson.get("features") or [])),
            processed_feature_count=int(len(geometry.index)),
            notes=["reused cached experiment layer"],
            paths=paths,
        )

    paths.cache_dir.mkdir(parents=True, exist_ok=True)
    service_root, layer_id = _resolve_layer_endpoint(layer, session)
    layer_url = f"{service_root.rstrip('/')}/{layer_id}"
    layer_metadata = _request_json(session, layer_url, params={"f": "json"})
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
        raise RuntimeError(f"{layer.source_name} returned no GeoJSON features from {layer_url}")

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

    _write_json(paths.layer_metadata, layer_metadata)
    _write_json(paths.raw_geojson, raw_geojson)
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
        paths=paths,
    )


def _combined_bounds(materialized_layers: list[MaterializedLayer]) -> tuple[float, float, float, float]:
    min_x = min(layer.geometry.total_bounds[0] for layer in materialized_layers)
    min_y = min(layer.geometry.total_bounds[1] for layer in materialized_layers)
    max_x = max(layer.geometry.total_bounds[2] for layer in materialized_layers)
    max_y = max(layer.geometry.total_bounds[3] for layer in materialized_layers)
    pad_x = max((max_x - min_x) * 0.10, 0.03)
    pad_y = max((max_y - min_y) * 0.10, 0.03)
    return min_x - pad_x, min_y - pad_y, max_x + pad_x, max_y + pad_y


def _load_land_context(
    land_context_path: Optional[Path],
    bbox: tuple[float, float, float, float],
) -> Optional[gpd.GeoDataFrame]:
    if land_context_path is None or not land_context_path.exists():
        return None

    try:
        land = gpd.read_file(land_context_path, bbox=bbox)
    except Exception:
        land = gpd.read_file(land_context_path)

    if land.empty:
        return land
    if land.crs is not None and str(land.crs).upper() != "EPSG:4326":
        land = land.to_crs("EPSG:4326")
    return land.clip(box(*bbox))


def _layer_display_area_m2(materialized: MaterializedLayer) -> float:
    projected = materialized.geometry.to_crs("EPSG:3857")
    return float(projected.geometry.area.sum())


def _ordered_layers_for_plotting(materialized_layers: list[MaterializedLayer]) -> list[MaterializedLayer]:
    return sorted(
        materialized_layers,
        key=lambda item: (
            1 if item.layer.style["mode"] == "outline" else 0,
            -_layer_display_area_m2(item),
            item.layer.observation_date,
            item.layer.provider,
        ),
    )


def _plot_layer(ax: plt.Axes, materialized: MaterializedLayer, *, zorder: float) -> None:
    style = materialized.layer.style
    halo_width = style["linewidth"] + (1.6 if style["mode"] == "outline" else 1.0)
    materialized.geometry.boundary.plot(
        ax=ax,
        color="#FFFFFF",
        linewidth=halo_width,
        alpha=0.95,
        zorder=zorder,
    )

    if style["mode"] == "outline":
        materialized.geometry.plot(
            ax=ax,
            facecolor="none",
            edgecolor=style["edgecolor"],
            linewidth=style["linewidth"],
            linestyle=style["linestyle"],
            alpha=style["alpha"],
            zorder=zorder + 0.1,
        )
        return

    materialized.geometry.plot(
        ax=ax,
        facecolor=style["facecolor"],
        edgecolor=style["edgecolor"],
        linewidth=style["linewidth"],
        linestyle=style["linestyle"],
        alpha=style["alpha"],
        zorder=zorder + 0.1,
    )


def _legend_handles(layers: list[ObservationLayer]) -> list[Any]:
    handles: list[Any] = []
    for layer in layers:
        style = layer.style
        if style["mode"] == "outline":
            handles.append(
                Line2D(
                    [0],
                    [0],
                    color=style["edgecolor"],
                    linestyle=style["linestyle"],
                    linewidth=style["linewidth"],
                    label=style["label"],
                )
            )
        else:
            handles.append(
                Patch(
                    facecolor=style["facecolor"],
                    edgecolor=style["edgecolor"],
                    linewidth=style["linewidth"],
                    alpha=style["alpha"],
                    label=style["label"],
                )
            )
    return handles


def render_single_map_overlay(
    materialized_layers: list[MaterializedLayer],
    *,
    output_path: Path,
    land_context_path: Optional[Path] = LAND_CONTEXT_PATH,
) -> tuple[float, float, float, float]:
    if not materialized_layers:
        raise ValueError("At least one materialized layer is required to render the overlay.")

    bounds = _combined_bounds(materialized_layers)
    land = _load_land_context(land_context_path, bounds)

    fig, ax = plt.subplots(figsize=(6.5, 10.0), dpi=220, facecolor="#FFFFFF")
    ax.set_facecolor("#EAF5FB")

    if land is not None and not land.empty:
        land.plot(ax=ax, color="#F5EBD8", edgecolor="#CCBDA0", linewidth=0.45, zorder=1)

    for index, layer in enumerate(_ordered_layers_for_plotting(materialized_layers), start=1):
        _plot_layer(ax, layer, zorder=4.0 + index)

    ax.set_xlim(bounds[0], bounds[2])
    ax.set_ylim(bounds[1], bounds[3])
    ax.set_aspect("equal", adjustable="box")
    ax.grid(color="#9DB4C0", linestyle="--", linewidth=0.45, alpha=0.4)
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    ax.set_title(
        "Mindoro March 3-6\nObserved Spill Overlay",
        loc="left",
        fontsize=15,
        fontweight="bold",
        color="#0F172A",
        pad=14,
    )
    ax.text(
        0.015,
        0.985,
        "Machine-readable observation layers only",
        transform=ax.transAxes,
        ha="left",
        va="top",
        fontsize=9,
        color="#0F172A",
        bbox={"boxstyle": "round,pad=0.28", "facecolor": "#FFFFFF", "edgecolor": "#CBD5E1", "alpha": 0.94},
    )
    ax.text(
        0.985,
        0.94,
        "Experimental overlay\nNo model or forecast layers",
        transform=ax.transAxes,
        ha="right",
        va="top",
        fontsize=8.5,
        color="#334155",
        bbox={"boxstyle": "round,pad=0.28", "facecolor": "#FFFFFF", "edgecolor": "#CBD5E1", "alpha": 0.94},
    )

    legend = ax.legend(
        handles=_legend_handles([layer.layer for layer in materialized_layers]),
        title="Date / provider",
        loc="lower left",
        frameon=True,
        framealpha=0.96,
        borderpad=0.65,
        labelspacing=0.5,
        fontsize=8.5,
        title_fontsize=9,
    )
    legend.get_frame().set_facecolor("#FFFFFF")
    legend.get_frame().set_edgecolor("#CBD5E1")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.tight_layout(pad=0.6)
    fig.savefig(output_path, bbox_inches="tight", pad_inches=0.08)
    plt.close(fig)
    return bounds


def _default_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": "mindoro-march3-6-observation-overlay/1.0"})
    return session


def generate_experiment(
    *,
    repo_root: Path = REPO_ROOT,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    force_refresh: bool = False,
    session: Optional[requests.Session] = None,
    land_context_path: Optional[Path] = LAND_CONTEXT_PATH,
    expected_region: Optional[list[float]] = None,
) -> dict[str, Any]:
    repo_root = repo_root.resolve()
    output_dir = output_dir.resolve()
    expected_region = list(expected_region or DEFAULT_EXPECTED_REGION)
    inventory_path = repo_root / "output" / "final_validation_package" / "final_validation_observation_table.csv"
    selected_layers = load_selected_observation_layers(inventory_path)
    active_session = session or _default_session()

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

    image_path = output_dir / FIGURE_FILENAME
    plot_bounds = render_single_map_overlay(
        materialized_layers,
        output_path=image_path,
        land_context_path=land_context_path,
    )

    manifest_path = output_dir / MANIFEST_FILENAME
    manifest = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "experiment_id": "mindoro_march3_6_observation_overlay",
        "case_id": "CASE_MINDORO_RETRO_2023",
        "inventory_path": str(inventory_path),
        "output_image": str(image_path),
        "selection_rule": "Exact March 3-6 machine-readable observed spill layer set locked for the experiment.",
        "plot_bounds_wgs84": list(plot_bounds),
        "force_refresh": bool(force_refresh),
        "selected_layers": [
            {
                "observation_date": item.layer.observation_date,
                "provider": item.layer.provider,
                "source_name": item.layer.source_name,
                "source_type": item.layer.source_type,
                "truth_status": item.layer.truth_status,
                "observation_usage": item.layer.observation_usage,
                "source_url": item.layer.source_url,
                "service_url": item.layer.service_url,
                "resolved_layer_id": int(item.layer_id),
                "source_crs": item.source_crs,
                "raw_feature_count": int(item.raw_feature_count),
                "processed_feature_count": int(item.processed_feature_count),
                "style": {
                    key: value
                    for key, value in item.layer.style.items()
                    if key not in {"observation_date", "provider", "source_name"}
                },
                "cache": {
                    "cache_dir": str(item.paths.cache_dir),
                    "raw_geojson": str(item.paths.raw_geojson),
                    "layer_metadata": str(item.paths.layer_metadata),
                    "processed_vector": str(item.paths.processed_vector),
                },
                "notes": item.notes,
            }
            for item in materialized_layers
        ],
    }
    _write_json(manifest_path, manifest)
    manifest["manifest_path"] = str(manifest_path)
    return manifest


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Isolated experiment output directory.",
    )
    parser.add_argument(
        "--force-refresh",
        action="store_true",
        help="Ignore cached experiment fetches and re-download the ArcGIS layers.",
    )
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    parser = build_argument_parser()
    args = parser.parse_args(argv)
    result = generate_experiment(
        output_dir=args.output_dir,
        force_refresh=args.force_refresh,
    )
    print(json.dumps(result, indent=2, default=_json_default))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
