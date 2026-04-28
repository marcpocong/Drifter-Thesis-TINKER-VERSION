"""Generate a Chapter 5-ready Mindoro Phase 2 product-family board."""

from __future__ import annotations

import argparse
import json
import textwrap
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import geopandas as gpd
import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import rasterio
from matplotlib.colors import LinearSegmentedColormap, ListedColormap
from pyproj import Transformer
from shapely.geometry import box as shapely_box

from src.services.figure_package_publication import FigurePackagePublicationService

PHASE2_OUTPUT_CATALOG_PATH = Path("output") / "phase2_finalization_audit" / "phase2_output_catalog.csv"
FORECAST_MANIFEST_PATH = Path("output") / "CASE_MINDORO_RETRO_2023" / "forecast" / "forecast_manifest.json"
ENSEMBLE_MANIFEST_PATH = Path("output") / "CASE_MINDORO_RETRO_2023" / "ensemble" / "ensemble_manifest.json"
PHASE3B_R1_FORECAST_MANIFEST_PATH = (
    Path("output")
    / "CASE_MINDORO_RETRO_2023"
    / "phase3b_extended_public_scored_march13_14_reinit"
    / "R1_previous"
    / "model_run"
    / "forecast"
    / "forecast_manifest.json"
)
PHASE3B_R1_ENSEMBLE_MANIFEST_PATH = (
    Path("output")
    / "CASE_MINDORO_RETRO_2023"
    / "phase3b_extended_public_scored_march13_14_reinit"
    / "R1_previous"
    / "model_run"
    / "ensemble"
    / "ensemble_manifest.json"
)
LAND_CONTEXT_PATH = Path("data_processed") / "reference" / "study_box_land_context.geojson"
LABELS_PATH = Path("config") / "publication_map_labels_mindoro.csv"

DEFAULT_OUTPUT_DIR = Path("output") / "chapter5_generated"
DEFAULT_OUTPUT_STEM = "mindoro_phase2_product_family_board"
DEFAULT_SOURCE_PROFILE = "phase2_official"

REQUIRED_PRODUCT_TYPES = [
    "control_footprint_mask",
    "control_density_norm",
    "prob_presence",
    "mask_p50",
    "mask_p90",
]


@dataclass(frozen=True)
class PanelSpec:
    product_type: str
    title: str
    subtitle: str
    palette_key: str
    colorbar_label: str = ""
    continuous: bool = False


@dataclass(frozen=True)
class SourceProfile:
    key: str
    forecast_manifest_path: Path
    ensemble_manifest_path: Path
    default_output_stem: str
    catalog_path: Path | None = None


PANEL_SPECS = [
    PanelSpec(
        product_type="control_footprint_mask",
        title="Deterministic footprint",
        subtitle="Binary control mask",
        palette_key="deterministic_opendrift",
    ),
    PanelSpec(
        product_type="control_density_norm",
        title="Deterministic density",
        subtitle="Normalized control particle density",
        palette_key="deterministic_opendrift",
        colorbar_label="Normalized density",
        continuous=True,
    ),
    PanelSpec(
        product_type="prob_presence",
        title="prob_presence",
        subtitle="Fraction of members with oil presence",
        palette_key="ensemble_consolidated",
        colorbar_label="Probability",
        continuous=True,
    ),
    PanelSpec(
        product_type="mask_p50",
        title="mask_p50",
        subtitle="Binary mask for probability >= 0.50",
        palette_key="ensemble_p50",
    ),
    PanelSpec(
        product_type="mask_p90",
        title="mask_p90",
        subtitle="Binary mask for probability >= 0.90",
        palette_key="ensemble_p90",
    ),
]


SOURCE_PROFILES = {
    DEFAULT_SOURCE_PROFILE: SourceProfile(
        key=DEFAULT_SOURCE_PROFILE,
        forecast_manifest_path=FORECAST_MANIFEST_PATH,
        ensemble_manifest_path=ENSEMBLE_MANIFEST_PATH,
        default_output_stem=DEFAULT_OUTPUT_STEM,
        catalog_path=PHASE2_OUTPUT_CATALOG_PATH,
    ),
    "march13_14_r1_previous": SourceProfile(
        key="march13_14_r1_previous",
        forecast_manifest_path=PHASE3B_R1_FORECAST_MANIFEST_PATH,
        ensemble_manifest_path=PHASE3B_R1_ENSEMBLE_MANIFEST_PATH,
        default_output_stem="mindoro_product_family_board_march13_14_r1_previous",
        catalog_path=None,
    ),
}


def _read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle) or {}


def _load_catalog(repo_root: Path, catalog_path: Path | None) -> pd.DataFrame:
    if catalog_path is None:
        return pd.DataFrame(columns=["product_type", "timestamp_utc", "semantics"])
    catalog = pd.read_csv(repo_root / catalog_path)
    if catalog.empty:
        raise ValueError(f"Phase 2 output catalog is empty: {catalog_path.as_posix()}")
    catalog = catalog.loc[catalog["product_type"].isin(REQUIRED_PRODUCT_TYPES)].copy()
    catalog = catalog.loc[catalog["timestamp_utc"].fillna("").astype(str).str.strip() != ""].copy()
    catalog["exists_on_disk_flag"] = (
        catalog["exists_on_disk"].astype(str).str.strip().str.lower().isin({"true", "1", "yes", "y"})
    )
    catalog = catalog.loc[catalog["exists_on_disk_flag"]].copy()
    if catalog.empty:
        raise ValueError("No on-disk timestamped Phase 2 rasters were found for the required product families.")
    return catalog


def _manifest_product_records(
    repo_root: Path,
    *,
    manifest_path: Path,
    products: list[dict[str, Any]],
    catalog: pd.DataFrame,
) -> pd.DataFrame:
    base_dir = (repo_root / manifest_path).resolve().parent.parent
    records: list[dict[str, Any]] = []
    for product in products:
        product_type = str(product.get("product_type") or "").strip()
        timestamp_utc = str(product.get("timestamp_utc") or "").strip()
        relative_path = str(product.get("relative_path") or "").strip()
        if product_type not in REQUIRED_PRODUCT_TYPES or not timestamp_utc or not relative_path:
            continue
        path = (base_dir / relative_path).resolve()
        if not path.exists():
            continue
        semantics = str(product.get("semantics") or "").strip()
        if not semantics:
            if not catalog.empty:
                catalog_match = catalog.loc[
                    (catalog["product_type"].astype(str) == product_type)
                    & (catalog["timestamp_utc"].astype(str) == timestamp_utc)
                ]
                if not catalog_match.empty:
                    semantics = str(catalog_match.iloc[0]["semantics"])
        records.append(
            {
                "product_type": product_type,
                "timestamp_utc": timestamp_utc,
                "path": path,
                "relative_path": str(path.relative_to(repo_root)),
                "semantics": semantics,
            }
        )
    return pd.DataFrame(records)


def _load_timestamp_records(
    repo_root: Path,
    *,
    catalog: pd.DataFrame,
    forecast_manifest: dict[str, Any],
    ensemble_manifest: dict[str, Any],
    forecast_manifest_path: Path,
    ensemble_manifest_path: Path,
) -> pd.DataFrame:
    deterministic_products = list((forecast_manifest.get("deterministic_control") or {}).get("products") or [])
    ensemble_products = list(ensemble_manifest.get("products") or [])
    forecast_records = _manifest_product_records(
        repo_root,
        manifest_path=forecast_manifest_path,
        products=deterministic_products,
        catalog=catalog,
    )
    ensemble_records = _manifest_product_records(
        repo_root,
        manifest_path=ensemble_manifest_path,
        products=ensemble_products,
        catalog=catalog,
    )
    records = pd.concat([forecast_records, ensemble_records], ignore_index=True)
    if records.empty:
        raise ValueError("No timestamped Phase 2 records were discovered in the forecast and ensemble manifests.")
    return records.drop_duplicates(subset=["product_type", "timestamp_utc", "relative_path"]).reset_index(drop=True)


def _load_land_context(repo_root: Path) -> gpd.GeoDataFrame | None:
    path = repo_root / LAND_CONTEXT_PATH
    if not path.exists():
        return None
    gdf = gpd.read_file(path)
    if gdf.empty:
        return None
    if gdf.crs and str(gdf.crs) != "EPSG:4326":
        gdf = gdf.to_crs("EPSG:4326")
    return gdf


def _subset_land_context(
    land_context: gpd.GeoDataFrame | None,
    *,
    xlim: tuple[float, float],
    ylim: tuple[float, float],
) -> gpd.GeoDataFrame | None:
    if land_context is None or land_context.empty:
        return None
    window = shapely_box(xlim[0] - 0.15, ylim[0] - 0.15, xlim[1] + 0.15, ylim[1] + 0.15)
    subset = land_context.loc[land_context.geometry.intersects(window)].copy()
    if subset.empty:
        return None
    subset["geometry"] = subset.geometry.intersection(window)
    subset = subset.loc[subset.geometry.notna() & ~subset.geometry.is_empty].copy()
    return subset if not subset.empty else None


def _load_labels(repo_root: Path) -> pd.DataFrame:
    path = repo_root / LABELS_PATH
    if not path.exists():
        return pd.DataFrame()
    labels = pd.read_csv(path)
    if labels.empty:
        return labels
    enabled = labels["enabled_yes_no"].astype(str).str.strip().str.lower().isin({"yes", "true", "1", "y"})
    return labels.loc[enabled].copy()


def _transformer(crs: str) -> Transformer:
    return Transformer.from_crs(crs, "EPSG:4326", always_xy=True)


def _raster_stats(path: Path) -> dict[str, Any]:
    with rasterio.open(path) as dataset:
        array = dataset.read(1)
        finite = array[np.isfinite(array)]
        valid_mask = np.isfinite(array) & (array > 0)
        projected_bounds = tuple(float(value) for value in dataset.bounds)
        raw_bounds_wgs84 = rasterio.warp.transform_bounds(dataset.crs, "EPSG:4326", *dataset.bounds, densify_pts=21)
        bounds_wgs84 = (
            float(raw_bounds_wgs84[0]),
            float(raw_bounds_wgs84[2]),
            float(raw_bounds_wgs84[1]),
            float(raw_bounds_wgs84[3]),
        )
        nonzero_bounds_wgs84: tuple[float, float, float, float] | None = None
        if valid_mask.any():
            rows, cols = np.where(valid_mask)
            xs, ys = rasterio.transform.xy(dataset.transform, rows, cols, offset="center")
            transformer = _transformer(str(dataset.crs))
            lons, lats = transformer.transform(np.asarray(xs), np.asarray(ys))
            nonzero_bounds_wgs84 = (
                float(np.min(lons)),
                float(np.max(lons)),
                float(np.min(lats)),
                float(np.max(lats)),
            )
        return {
            "shape": [int(array.shape[0]), int(array.shape[1])],
            "dtype": str(array.dtype),
            "projected_crs": str(dataset.crs),
            "projected_bounds": [float(value) for value in projected_bounds],
            "bounds_wgs84": [float(value) for value in bounds_wgs84],
            "nonzero_bounds_wgs84": list(nonzero_bounds_wgs84) if nonzero_bounds_wgs84 is not None else None,
            "nonzero_cells": int(np.count_nonzero(valid_mask)),
            "min": float(finite.min()) if finite.size else 0.0,
            "max": float(finite.max()) if finite.size else 0.0,
        }


def _bundle_for_timestamp(records: pd.DataFrame, repo_root: Path, timestamp_utc: str) -> dict[str, dict[str, Any]]:
    bundle: dict[str, dict[str, Any]] = {}
    selected = records.loc[records["timestamp_utc"].astype(str) == str(timestamp_utc)].copy()
    for product_type in REQUIRED_PRODUCT_TYPES:
        matches = selected.loc[selected["product_type"].astype(str) == product_type]
        if matches.empty:
            raise ValueError(f"Missing {product_type} for timestamp {timestamp_utc}.")
        row = matches.iloc[0]
        path = Path(row["path"]).resolve()
        stats = _raster_stats(path)
        bundle[product_type] = {
            "product_type": product_type,
            "path": path,
            "relative_path": str(row["relative_path"]),
            "timestamp_utc": str(row["timestamp_utc"]),
            "semantics": str(row["semantics"]),
            "stats": stats,
        }
    return bundle


def _available_common_timestamps(records: pd.DataFrame) -> list[str]:
    timestamp_sets: list[set[str]] = []
    for product_type in REQUIRED_PRODUCT_TYPES:
        values = set(
            records.loc[records["product_type"].astype(str) == product_type, "timestamp_utc"]
            .astype(str)
            .tolist()
        )
        timestamp_sets.append(values)
    common = set.intersection(*timestamp_sets) if timestamp_sets else set()
    return sorted(common)


def _select_timestamp(
    records: pd.DataFrame,
    repo_root: Path,
    *,
    requested_timestamp: str | None = None,
) -> tuple[str, dict[str, dict[str, Any]], dict[str, dict[str, dict[str, Any]]]]:
    common_timestamps = _available_common_timestamps(records)
    if not common_timestamps:
        raise ValueError("No common timestamp exists across the five required Phase 2 product families.")

    bundles: dict[str, dict[str, dict[str, Any]]] = {}
    for timestamp in common_timestamps:
        bundles[timestamp] = _bundle_for_timestamp(records, repo_root, timestamp)

    if requested_timestamp:
        normalized = str(requested_timestamp).strip()
        if normalized not in bundles:
            available = ", ".join(common_timestamps)
            raise ValueError(f"Timestamp {normalized} is unavailable. Available common timestamps: {available}")
        return normalized, bundles[normalized], bundles

    nonzero_common = [
        timestamp
        for timestamp in common_timestamps
        if all(item["stats"]["nonzero_cells"] > 0 for item in bundles[timestamp].values())
    ]
    if nonzero_common:
        selected = nonzero_common[-1]
    else:
        selected = common_timestamps[-1]
    return selected, bundles[selected], bundles


def _combined_nonzero_bounds(bundle: dict[str, dict[str, Any]]) -> tuple[float, float, float, float] | None:
    bounds = [
        item["stats"]["nonzero_bounds_wgs84"]
        for item in bundle.values()
        if item["stats"].get("nonzero_bounds_wgs84") is not None
    ]
    if not bounds:
        return None
    return (
        min(bound[0] for bound in bounds),
        max(bound[1] for bound in bounds),
        min(bound[2] for bound in bounds),
        max(bound[3] for bound in bounds),
    )


def _crop_from_bounds(
    occupied_bounds: tuple[float, float, float, float] | None,
    display_bounds: tuple[float, float, float, float],
) -> tuple[tuple[float, float], tuple[float, float]]:
    if occupied_bounds is None:
        return (display_bounds[0], display_bounds[1]), (display_bounds[2], display_bounds[3])
    min_lon, max_lon, min_lat, max_lat = occupied_bounds
    span_lon = max(max_lon - min_lon, 0.01)
    span_lat = max(max_lat - min_lat, 0.01)
    pad_lon = max(0.070, span_lon * 1.85)
    pad_lat = max(0.055, span_lat * 0.60)
    xlim = (max(display_bounds[0], min_lon - pad_lon), min(display_bounds[1], max_lon + pad_lon))
    ylim = (max(display_bounds[2], min_lat - pad_lat), min(display_bounds[3], max_lat + pad_lat))
    return xlim, ylim


def _masked_colormap(colors: list[str]) -> Any:
    if len(colors) == 1:
        cmap = ListedColormap(colors)
    else:
        cmap = LinearSegmentedColormap.from_list("phase2_product_family", colors)
    cmap = cmap.copy()
    cmap.set_bad((0.0, 0.0, 0.0, 0.0))
    return cmap


def _panel_colormap(spec: PanelSpec, service: FigurePackagePublicationService) -> Any:
    palette = service._palette()
    accent = str(palette.get(spec.palette_key) or "#2563eb")
    if not spec.continuous:
        return _masked_colormap([accent])
    if spec.product_type == "control_density_norm":
        return _masked_colormap(["#eff6ff", "#93c5fd", accent])
    return _masked_colormap(["#ecfdf5", "#34d399", accent])


def _draw_context(
    ax: plt.Axes,
    *,
    land_context: gpd.GeoDataFrame | None,
    labels: pd.DataFrame,
    service: FigurePackagePublicationService,
    xlim: tuple[float, float],
    ylim: tuple[float, float],
) -> None:
    palette = service._palette()
    layout = service.style.get("layout") or {}
    ax.set_facecolor(str(layout.get("axes_facecolor") or "#f7fbfd"))
    ax.axvspan(xlim[0], xlim[1], color="#eef7fb", alpha=0.60, zorder=0)
    if land_context is not None and not land_context.empty:
        land_context.plot(
            ax=ax,
            color=str(palette.get("background_land") or "#e6dfd1"),
            edgecolor=str(palette.get("shoreline") or "#8b8178"),
            linewidth=0.65,
            zorder=1,
        )
    for row in labels.itertuples(index=False):
        lon = float(row.lon)
        lat = float(row.lat)
        if xlim[0] <= lon <= xlim[1] and ylim[0] <= lat <= ylim[1]:
            ax.text(
                lon,
                lat,
                str(row.label_text),
                fontsize=7.0,
                color="#475569",
                ha="center",
                va="center",
                zorder=6,
                bbox={
                    "boxstyle": "round,pad=0.14",
                    "facecolor": (1.0, 1.0, 1.0, 0.72),
                    "edgecolor": "#cbd5e1",
                    "linewidth": 0.4,
                },
            )


def _add_value_bar(ax: plt.Axes, image: Any, *, label: str, ticks: list[float], ticklabels: list[str]) -> None:
    color_ax = ax.inset_axes([0.055, 0.045, 0.40, 0.030])
    colorbar = ax.figure.colorbar(image, cax=color_ax, orientation="horizontal", ticks=ticks)
    colorbar.ax.tick_params(labelsize=6, pad=1, length=2)
    colorbar.set_label(label, fontsize=6.5, labelpad=2)
    colorbar.ax.set_xticklabels(ticklabels)
    colorbar.outline.set_linewidth(0.4)


def _plot_panel(
    ax: plt.Axes,
    *,
    spec: PanelSpec,
    product: dict[str, Any],
    service: FigurePackagePublicationService,
    land_context: gpd.GeoDataFrame | None,
    labels: pd.DataFrame,
    xlim: tuple[float, float],
    ylim: tuple[float, float],
    show_xlabel: bool,
    show_ylabel: bool,
) -> None:
    _draw_context(ax, land_context=land_context, labels=labels, service=service, xlim=xlim, ylim=ylim)
    path = Path(product["path"])
    with rasterio.open(path) as dataset:
        array = dataset.read(1)
        masked = np.ma.masked_less_equal(array, 0.0)
        raw_extent = rasterio.warp.transform_bounds(dataset.crs, "EPSG:4326", *dataset.bounds, densify_pts=21)
        extent = (raw_extent[0], raw_extent[2], raw_extent[1], raw_extent[3])
        if spec.continuous:
            vmax = 1.0 if spec.product_type == "prob_presence" else max(float(np.nanmax(array)), 1e-6)
            image = ax.imshow(
                masked,
                extent=extent,
                origin="upper",
                cmap=_panel_colormap(spec, service),
                vmin=0.0,
                vmax=vmax,
                interpolation="nearest",
                zorder=4,
            )
            if spec.product_type == "prob_presence":
                _add_value_bar(ax, image, label=spec.colorbar_label, ticks=[0.0, 0.5, 1.0], ticklabels=["0", "0.5", "1"])
            else:
                _add_value_bar(
                    ax,
                    image,
                    label=spec.colorbar_label,
                    ticks=[0.0, float(vmax)],
                    ticklabels=["0", f"{float(vmax):.3f}"],
                )
        else:
            ax.imshow(
                masked,
                extent=extent,
                origin="upper",
                cmap=_panel_colormap(spec, service),
                interpolation="nearest",
                zorder=4,
            )

    ax.set_xlim(*xlim)
    ax.set_ylim(*ylim)
    ax.set_title(
        spec.title,
        loc="left",
        fontsize=float((service.style.get("typography") or {}).get("panel_title_size") or 11),
        fontweight="bold",
        pad=12,
    )
    ax.text(
        0.0,
        1.01,
        spec.subtitle,
        transform=ax.transAxes,
        ha="left",
        va="bottom",
        fontsize=7.7,
        color="#475569",
    )
    stats = product["stats"]
    badge_lines = [f"cells {int(stats['nonzero_cells'])}"]
    if spec.continuous:
        badge_lines.append(f"max {float(stats['max']):.3f}")
    ax.text(
        0.98,
        0.03,
        " | ".join(badge_lines),
        transform=ax.transAxes,
        ha="right",
        va="bottom",
        fontsize=6.8,
        color="#334155",
        bbox={
            "boxstyle": "round,pad=0.20",
            "facecolor": (1.0, 1.0, 1.0, 0.84),
            "edgecolor": "#cbd5e1",
            "linewidth": 0.4,
        },
        zorder=7,
    )
    ax.grid(
        True,
        linestyle="--",
        linewidth=0.30,
        alpha=0.33,
        color=(service.style.get("layout") or {}).get("grid_color") or "#cbd5e1",
    )
    if show_xlabel:
        ax.set_xlabel("Longitude (degrees east)")
    else:
        ax.set_xticklabels([])
    if show_ylabel:
        ax.set_ylabel("Latitude (degrees north)")
    else:
        ax.set_yticklabels([])
    ax.set_aspect(service._geographic_aspect((ylim[0] + ylim[1]) / 2.0), adjustable="box")
    for spine in ax.spines.values():
        spine.set_color("#94a3b8")
        spine.set_linewidth(0.8)


def generate_phase2_product_family_board(
    *,
    repo_root: str | Path = ".",
    output_dir: str | Path = DEFAULT_OUTPUT_DIR,
    output_stem: str | None = None,
    timestamp_utc: str | None = None,
    source_profile: str = DEFAULT_SOURCE_PROFILE,
) -> dict[str, Any]:
    repo_root_path = Path(repo_root).resolve()
    profile = SOURCE_PROFILES.get(source_profile)
    if profile is None:
        available = ", ".join(sorted(SOURCE_PROFILES))
        raise ValueError(f"Unknown source profile {source_profile!r}. Available profiles: {available}")
    output_dir_path = Path(output_dir)
    if not output_dir_path.is_absolute():
        output_dir_path = repo_root_path / output_dir_path
    output_dir_path.mkdir(parents=True, exist_ok=True)

    catalog = _load_catalog(repo_root_path, profile.catalog_path)
    forecast_manifest = _read_json(repo_root_path / profile.forecast_manifest_path)
    ensemble_manifest = _read_json(repo_root_path / profile.ensemble_manifest_path)
    records = _load_timestamp_records(
        repo_root_path,
        catalog=catalog,
        forecast_manifest=forecast_manifest,
        ensemble_manifest=ensemble_manifest,
        forecast_manifest_path=profile.forecast_manifest_path,
        ensemble_manifest_path=profile.ensemble_manifest_path,
    )
    service = FigurePackagePublicationService(repo_root=repo_root_path)

    selected_timestamp, selected_bundle, bundles = _select_timestamp(
        records,
        repo_root_path,
        requested_timestamp=timestamp_utc,
    )
    available_common_timestamps = sorted(bundles)
    canonical_timestamp = available_common_timestamps[-1]
    canonical_bundle = bundles[canonical_timestamp]

    display_bounds = tuple(
        float(value)
        for value in ((forecast_manifest.get("grid") or {}).get("display_bounds_wgs84") or [120.9096, 122.0622, 12.2494, 13.7837])
    )
    occupied_bounds = _combined_nonzero_bounds(selected_bundle)
    xlim, ylim = _crop_from_bounds(occupied_bounds, display_bounds)

    land_context = _subset_land_context(_load_land_context(repo_root_path), xlim=xlim, ylim=ylim)
    labels = _load_labels(repo_root_path)

    effective_output_stem = output_stem or profile.default_output_stem
    output_path = output_dir_path / f"{effective_output_stem}.png"
    metadata_path = output_dir_path / f"{effective_output_stem}.json"

    fig = plt.figure(
        figsize=service._board_size(),
        dpi=service._dpi(),
        facecolor=(service.style.get("layout") or {}).get("figure_facecolor") or "#ffffff",
    )
    grid = fig.add_gridspec(
        2,
        3,
        left=0.045,
        right=0.985,
        top=0.855,
        bottom=0.072,
        wspace=0.13,
        hspace=0.23,
    )

    axes = [
        fig.add_subplot(grid[0, 0]),
        fig.add_subplot(grid[0, 1]),
        fig.add_subplot(grid[0, 2]),
        fig.add_subplot(grid[1, 0]),
        fig.add_subplot(grid[1, 1]),
    ]
    note_ax = fig.add_subplot(grid[1, 2])

    for index, (ax, spec) in enumerate(zip(axes, PANEL_SPECS, strict=True)):
        _plot_panel(
            ax,
            spec=spec,
            product=selected_bundle[spec.product_type],
            service=service,
            land_context=land_context,
            labels=labels,
            xlim=xlim,
            ylim=ylim,
            show_xlabel=index >= 3,
            show_ylabel=index in {0, 3},
        )

    selection_mode = (
        "explicit_timestamp"
        if timestamp_utc
        else "latest_nonzero_common_timestamp"
        if selected_timestamp != canonical_timestamp
        else "latest_common_timestamp"
    )
    sensitivity_context = forecast_manifest.get("sensitivity_context") or {}
    simulation_window = forecast_manifest.get("simulation_window_utc") or {}
    note_lines = [
        f"Displayed timestamp: {selected_timestamp}.",
        f"Selection mode: {selection_mode.replace('_', ' ')}.",
        f"Canonical manifest timestamp: {canonical_timestamp}.",
        "Panels are zoomed to the occupied cells for readability; the stored rasters all share the common Phase 2 scoring grid.",
        f"Shared grid id: {str((forecast_manifest.get('grid') or {}).get('grid_id') or 'unknown')}.",
        f"Grid resolution: {float((forecast_manifest.get('grid') or {}).get('resolution') or 1000):.0f} m.",
        "prob_presence is the fraction of ensemble members with oil presence.",
        "mask_p50 is the binary mask where probability of presence is at least 0.50.",
        "mask_p90 is the binary mask where probability of presence is at least 0.90.",
    ]
    if profile.key == "march13_14_r1_previous":
        note_lines[3] = (
            "Panels are zoomed to the occupied cells for readability; the stored rasters all share the common scoring grid "
            "used by the promoted March 13 -> March 14 validation branch."
        )
        note_lines.insert(
            3,
            f"Run window: {str(simulation_window.get('start') or 'unknown')} to {str(simulation_window.get('end') or 'unknown')}.",
        )
        note_lines.insert(
            4,
            "This board uses the common stored five-surface timestamp family and does not mix in local-date composites.",
        )
        branch_id = str(sensitivity_context.get("branch_id") or "").strip()
        branch_description = str(sensitivity_context.get("branch_description") or "").strip()
        if branch_id:
            note_lines.insert(
                5,
                f"Promoted branch: {branch_id}{f' ({branch_description})' if branch_description else ''}.",
            )
        track_label = str(sensitivity_context.get("track_label") or "").strip()
        if track_label:
            note_lines.insert(6, f"Track label: {track_label}.")
    if selected_timestamp != canonical_timestamp:
        note_lines.append(
            "The later March 5 and March 6 timestamped Phase 2 rasters are all-zero across this five-product family."
        )
    service._add_note_box(
        note_ax,
        "How to read this board",
        note_lines,
        wrap_width=42,
        bullet_lines=False,
        title_y=0.98,
        body_y=0.90,
        box_pad=0.40,
        minimum_title_gap_px=24.0,
    )

    if profile.key == "march13_14_r1_previous":
        branch_id = str(sensitivity_context.get("branch_id") or "R1_previous").strip()
        subtitle_text = (
            "Mindoro | March 13 -> March 14 promoted public-validation deterministic, probability, and threshold "
            f"surfaces | branch {branch_id} | displayed timestamp {selected_timestamp}"
        )
    else:
        subtitle_text = (
            "Mindoro | representative Phase 2 deterministic, probability, and threshold surfaces "
            f"from the shared scoring grid | displayed timestamp {selected_timestamp}"
        )
    subtitle = textwrap.fill(subtitle_text, width=112)
    fig.suptitle(
        "Mindoro product-family board",
        x=0.045,
        y=0.985,
        ha="left",
        fontsize=float((service.style.get("typography") or {}).get("title_size") or 19),
        fontweight="bold",
    )
    fig.text(
        0.045,
        0.948,
        subtitle,
        ha="left",
        va="top",
        fontsize=float((service.style.get("typography") or {}).get("subtitle_size") or 10),
        color="#475569",
        linespacing=1.16,
    )

    fig.savefig(output_path, dpi=service._dpi())
    pixel_width, pixel_height = service._figure_pixel_size(fig)
    plt.close(fig)

    metadata = {
        "title": "Mindoro product-family board",
        "output_path": str(output_path.relative_to(repo_root_path)),
        "pixel_width": pixel_width,
        "pixel_height": pixel_height,
        "source_profile": profile.key,
        "selection_mode": selection_mode,
        "displayed_timestamp_utc": selected_timestamp,
        "canonical_manifest_timestamp_utc": canonical_timestamp,
        "available_common_timestamps_utc": available_common_timestamps,
        "all_zero_common_timestamps_utc": [
            timestamp
            for timestamp, bundle in bundles.items()
            if all(int(item["stats"]["nonzero_cells"]) == 0 for item in bundle.values())
        ],
        "zoom_bounds_wgs84": [float(xlim[0]), float(xlim[1]), float(ylim[0]), float(ylim[1])],
        "display_bounds_wgs84": [float(value) for value in display_bounds],
        "phase2_output_catalog_path": str(profile.catalog_path) if profile.catalog_path is not None else None,
        "forecast_manifest_path": str(profile.forecast_manifest_path),
        "ensemble_manifest_path": str(profile.ensemble_manifest_path),
        "sensitivity_context": sensitivity_context,
        "selected_products": {
            product_type: {
                "path": item["relative_path"],
                "relative_path": item["relative_path"],
                "semantics": item["semantics"],
                "stats": item["stats"],
            }
            for product_type, item in selected_bundle.items()
        },
        "canonical_products": {
            product_type: {
                "path": item["relative_path"],
                "relative_path": item["relative_path"],
                "stats": item["stats"],
            }
            for product_type, item in canonical_bundle.items()
        },
    }
    metadata_path.write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    return metadata


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", default=".", help="Repository root path.")
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_OUTPUT_DIR),
        help="Output directory for the rendered board and metadata sidecar.",
    )
    parser.add_argument(
        "--output-stem",
        default="",
        help="Base filename without extension. Defaults to the selected source profile stem.",
    )
    parser.add_argument(
        "--timestamp-utc",
        default="",
        help="Optional explicit timestamp to render instead of auto-selecting the latest nonzero common timestamp.",
    )
    parser.add_argument(
        "--source-profile",
        default=DEFAULT_SOURCE_PROFILE,
        choices=sorted(SOURCE_PROFILES),
        help="Manifest bundle to render.",
    )
    return parser


def main() -> None:
    args = _build_arg_parser().parse_args()
    metadata = generate_phase2_product_family_board(
        repo_root=args.repo_root,
        output_dir=args.output_dir,
        output_stem=args.output_stem or None,
        timestamp_utc=args.timestamp_utc or None,
        source_profile=args.source_profile,
    )
    print(json.dumps(metadata, indent=2))


if __name__ == "__main__":
    main()
