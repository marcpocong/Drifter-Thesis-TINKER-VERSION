"""
Visualization helpers for plotting trajectories and maps.
"""
import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import numpy as np
import pandas as pd
from datetime import datetime
from pathlib import Path
from matplotlib.colors import ListedColormap
from matplotlib.lines import Line2D
from matplotlib.patches import Patch, Rectangle

try:
    import rasterio
except ImportError:  # pragma: no cover - guarded at runtime
    rasterio = None

try:
    from pyproj import Geod
except ImportError:  # pragma: no cover - guarded at runtime
    Geod = None


PROTOTYPE_2016_RENDERING_PROFILE = "prototype_2016_case_local_projected_v1"
PROTOTYPE_2016_EXTENT_MODE_FIXED_REGIONAL = "fixed_regional_extent"
PROTOTYPE_2016_EXTENT_MODE_DYNAMIC_FORECAST = "dynamic_forecast_extent"
PROTOTYPE_2016_FONT_FAMILY = "Arial"
PROTOTYPE_2016_OCEAN_COLOR = "#e8f8fc"
PROTOTYPE_2016_LAND_COLOR = "#d0d0d0"
PROTOTYPE_2016_GRID_COLOR = "#94a3b8"
_WGS84_GEOD = Geod(ellps="WGS84") if Geod is not None else None


def _normalize_bounds(bounds: list[float] | tuple[float, float, float, float]) -> tuple[float, float, float, float]:
    if len(bounds) != 4:
        raise ValueError("Expected geographic bounds as [min_lon, max_lon, min_lat, max_lat].")
    return tuple(float(value) for value in bounds)


def prototype_2016_projection_center(
    bounds: list[float] | tuple[float, float, float, float],
) -> tuple[float, float]:
    min_lon, max_lon, min_lat, max_lat = _normalize_bounds(bounds)
    return ((min_lon + max_lon) / 2.0, (min_lat + max_lat) / 2.0)


def prototype_2016_rendering_metadata(
    bounds: list[float] | tuple[float, float, float, float],
) -> dict[str, object]:
    normalized_bounds = list(_normalize_bounds(bounds))
    center_lon, center_lat = prototype_2016_projection_center(normalized_bounds)
    return {
        "rendering_profile": PROTOTYPE_2016_RENDERING_PROFILE,
        "display_bounds_wgs84": normalized_bounds,
        "map_projection": "local_azimuthal_equidistant",
        "projection_center": {
            "lon": float(center_lon),
            "lat": float(center_lat),
        },
    }


def normalize_prototype_2016_extent_mode(value: str | None) -> str:
    token = str(value or "").strip().lower()
    if token in {
        PROTOTYPE_2016_EXTENT_MODE_DYNAMIC_FORECAST,
        "dynamic",
        "forecast",
        "dynamic_forecast",
    }:
        return PROTOTYPE_2016_EXTENT_MODE_DYNAMIC_FORECAST
    return PROTOTYPE_2016_EXTENT_MODE_FIXED_REGIONAL


def bounds_from_track_dataframe(
    track_df: pd.DataFrame | None,
) -> tuple[float, float, float, float] | None:
    if track_df is None or track_df.empty:
        return None
    lon_values = pd.to_numeric(track_df["lon"], errors="coerce")
    lat_values = pd.to_numeric(track_df["lat"], errors="coerce")
    valid = lon_values.notna() & lat_values.notna()
    if not valid.any():
        return None
    return (
        float(lon_values.loc[valid].min()),
        float(lon_values.loc[valid].max()),
        float(lat_values.loc[valid].min()),
        float(lat_values.loc[valid].max()),
    )


def bounds_from_points(
    points: list[tuple[float, float]] | tuple[tuple[float, float], ...] | np.ndarray | None,
) -> tuple[float, float, float, float] | None:
    if points is None:
        return None
    if isinstance(points, np.ndarray):
        if points.size == 0:
            return None
        arr = np.asarray(points, dtype=float)
        if arr.ndim != 2 or arr.shape[1] != 2:
            return None
        lon_values = arr[:, 0]
        lat_values = arr[:, 1]
    else:
        normalized_points = [
            (float(point[0]), float(point[1]))
            for point in points
            if point is not None and len(point) == 2
        ]
        if not normalized_points:
            return None
        lon_values = np.asarray([point[0] for point in normalized_points], dtype=float)
        lat_values = np.asarray([point[1] for point in normalized_points], dtype=float)
    finite = np.isfinite(lon_values) & np.isfinite(lat_values)
    if not np.any(finite):
        return None
    return (
        float(np.min(lon_values[finite])),
        float(np.max(lon_values[finite])),
        float(np.min(lat_values[finite])),
        float(np.max(lat_values[finite])),
    )


def bounds_from_point(
    point: tuple[float, float] | None,
    *,
    lon_pad: float = 0.0,
    lat_pad: float = 0.0,
) -> tuple[float, float, float, float] | None:
    if point is None or len(point) != 2:
        return None
    lon = float(point[0])
    lat = float(point[1])
    if not np.isfinite(lon) or not np.isfinite(lat):
        return None
    return (
        float(lon - lon_pad),
        float(lon + lon_pad),
        float(lat - lat_pad),
        float(lat + lat_pad),
    )


def merge_case_display_bounds(
    bounds_sets: list[list[float] | tuple[float, float, float, float]],
    *,
    halo_degrees: float = 1.0,
    minimum_span_degrees: float = 8.0,
) -> tuple[float, float, float, float]:
    normalized_sets = [_normalize_bounds(bounds) for bounds in bounds_sets if bounds and len(bounds) == 4]
    if not normalized_sets:
        raise ValueError("merge_case_display_bounds requires at least one bounds set.")
    min_lon = min(bounds[0] for bounds in normalized_sets) - float(halo_degrees)
    max_lon = max(bounds[1] for bounds in normalized_sets) + float(halo_degrees)
    min_lat = min(bounds[2] for bounds in normalized_sets) - float(halo_degrees)
    max_lat = max(bounds[3] for bounds in normalized_sets) + float(halo_degrees)

    lon_center = (min_lon + max_lon) / 2.0
    lat_center = (min_lat + max_lat) / 2.0
    lon_half_span = max((max_lon - min_lon) / 2.0, float(minimum_span_degrees) / 2.0)
    lat_half_span = max((max_lat - min_lat) / 2.0, float(minimum_span_degrees) / 2.0)
    return (
        float(lon_center - lon_half_span),
        float(lon_center + lon_half_span),
        float(lat_center - lat_half_span),
        float(lat_center + lat_half_span),
    )


def positive_raster_bounds(mask_path: str | Path) -> tuple[float, float, float, float] | None:
    if rasterio is None:
        raise ImportError("rasterio is required to inspect positive raster bounds.")

    with rasterio.open(mask_path) as dataset:
        values = dataset.read(1)
        positive_rows, positive_cols = np.where(np.isfinite(values) & (values > 0))
        if len(positive_rows) == 0 or len(positive_cols) == 0:
            return None
        bounds = dataset.bounds
        cell_width = (float(bounds.right) - float(bounds.left)) / float(dataset.width)
        cell_height = (float(bounds.top) - float(bounds.bottom)) / float(dataset.height)
        row_min = int(positive_rows.min())
        row_max = int(positive_rows.max())
        col_min = int(positive_cols.min())
        col_max = int(positive_cols.max())
        return (
            float(bounds.left + (col_min * cell_width)),
            float(bounds.left + ((col_max + 1) * cell_width)),
            float(bounds.top - ((row_max + 1) * cell_height)),
            float(bounds.top - (row_min * cell_height)),
        )


def derive_prototype_2016_display_bounds(
    *,
    drifter_track_df: pd.DataFrame | None = None,
    mask_paths: list[str | Path] | tuple[str | Path, ...] = (),
    fallback_bounds: list[float] | tuple[float, float, float, float] | None = None,
    halo_degrees: float = 1.0,
    minimum_span_degrees: float = 8.0,
) -> tuple[float, float, float, float]:
    bounds_sets: list[tuple[float, float, float, float]] = []
    if drifter_track_df is not None and not drifter_track_df.empty:
        bounds_sets.append(
            (
                float(pd.to_numeric(drifter_track_df["lon"], errors="coerce").min()),
                float(pd.to_numeric(drifter_track_df["lon"], errors="coerce").max()),
                float(pd.to_numeric(drifter_track_df["lat"], errors="coerce").min()),
                float(pd.to_numeric(drifter_track_df["lat"], errors="coerce").max()),
            )
        )
    for path in mask_paths:
        if path and Path(path).exists():
            positive_bounds = positive_raster_bounds(path)
            if positive_bounds is not None:
                bounds_sets.append(positive_bounds)
    if fallback_bounds is not None:
        bounds_sets.append(_normalize_bounds(fallback_bounds))
    if not bounds_sets:
        raise ValueError("Cannot derive prototype_2016 display bounds without any valid drifter or raster bounds.")
    return merge_case_display_bounds(
        bounds_sets,
        halo_degrees=halo_degrees,
        minimum_span_degrees=minimum_span_degrees,
    )


def derive_prototype_2016_figure_bounds(
    *,
    base_bounds: list[float] | tuple[float, float, float, float],
    bounds_sets: list[list[float] | tuple[float, float, float, float]] | tuple[list[float] | tuple[float, float, float, float], ...] = (),
    cell_widths: list[float] | tuple[float, ...] = (),
    cell_heights: list[float] | tuple[float, ...] = (),
    extent_mode: str = PROTOTYPE_2016_EXTENT_MODE_DYNAMIC_FORECAST,
    padding_fraction: float = 0.18,
    minimum_padding_degrees: float = 0.08,
    maximum_padding_degrees: float = 0.75,
    minimum_span_degrees: float = 0.65,
) -> tuple[float, float, float, float]:
    base = _normalize_bounds(base_bounds)
    if normalize_prototype_2016_extent_mode(extent_mode) != PROTOTYPE_2016_EXTENT_MODE_DYNAMIC_FORECAST:
        return base

    normalized_sets = [_normalize_bounds(bounds) for bounds in bounds_sets if bounds and len(bounds) == 4]
    if not normalized_sets:
        return base

    min_lon = min(bounds[0] for bounds in normalized_sets)
    max_lon = max(bounds[1] for bounds in normalized_sets)
    min_lat = min(bounds[2] for bounds in normalized_sets)
    max_lat = max(bounds[3] for bounds in normalized_sets)

    span_lon = max(max_lon - min_lon, 0.0)
    span_lat = max(max_lat - min_lat, 0.0)
    reference_cell_width = max([float(value) for value in cell_widths if float(value) > 0.0], default=0.0)
    reference_cell_height = max([float(value) for value in cell_heights if float(value) > 0.0], default=0.0)

    pad_lon = max(minimum_padding_degrees, reference_cell_width * 3.0, span_lon * padding_fraction)
    pad_lat = max(minimum_padding_degrees, reference_cell_height * 3.0, span_lat * padding_fraction)
    pad_lon = min(pad_lon, maximum_padding_degrees)
    pad_lat = min(pad_lat, maximum_padding_degrees)

    min_lon -= pad_lon
    max_lon += pad_lon
    min_lat -= pad_lat
    max_lat += pad_lat

    center_lon = (min_lon + max_lon) / 2.0
    center_lat = (min_lat + max_lat) / 2.0
    half_span_lon = max((max_lon - min_lon) / 2.0, minimum_span_degrees / 2.0, reference_cell_width * 6.0)
    half_span_lat = max((max_lat - min_lat) / 2.0, minimum_span_degrees / 2.0, reference_cell_height * 6.0)
    proposed = (
        float(center_lon - half_span_lon),
        float(center_lon + half_span_lon),
        float(center_lat - half_span_lat),
        float(center_lat + half_span_lat),
    )

    base_span_lon = max(base[1] - base[0], minimum_span_degrees)
    base_span_lat = max(base[3] - base[2], minimum_span_degrees)
    final_span_lon = min(proposed[1] - proposed[0], base_span_lon)
    final_span_lat = min(proposed[3] - proposed[2], base_span_lat)

    min_lon = max(base[0], min(proposed[0], base[1] - final_span_lon))
    min_lat = max(base[2], min(proposed[2], base[3] - final_span_lat))
    max_lon = min(base[1], max(proposed[1], base[0] + final_span_lon))
    max_lat = min(base[3], max(proposed[3], base[2] + final_span_lat))

    if (max_lon - min_lon) < final_span_lon:
        if min_lon <= base[0]:
            max_lon = min(base[1], base[0] + final_span_lon)
        else:
            min_lon = max(base[0], base[1] - final_span_lon)
    if (max_lat - min_lat) < final_span_lat:
        if min_lat <= base[2]:
            max_lat = min(base[3], base[2] + final_span_lat)
        else:
            min_lat = max(base[2], base[3] - final_span_lat)

    return (
        float(min_lon),
        float(max_lon),
        float(min_lat),
        float(max_lat),
    )


def _prototype_2016_projection(bounds: list[float] | tuple[float, float, float, float]):
    center_lon, center_lat = prototype_2016_projection_center(bounds)
    return ccrs.AzimuthalEquidistant(
        central_longitude=float(center_lon),
        central_latitude=float(center_lat),
    )


def _pick_scale_bar_length_km(bounds: tuple[float, float, float, float]) -> int:
    min_lon, max_lon, min_lat, max_lat = bounds
    mid_lat = (min_lat + max_lat) / 2.0
    if _WGS84_GEOD is not None:
        _, _, distance_m = _WGS84_GEOD.inv(min_lon, mid_lat, max_lon, mid_lat)
        span_km = abs(distance_m) / 1000.0
    else:
        span_km = abs(max_lon - min_lon) * 111.0 * max(np.cos(np.deg2rad(mid_lat)), 0.2)
    if span_km >= 900:
        return 200
    if span_km >= 450:
        return 100
    return 50


def _draw_scale_bar(ax, bounds: tuple[float, float, float, float]) -> None:
    if _WGS84_GEOD is None:
        return
    min_lon, max_lon, min_lat, max_lat = bounds
    bar_length_km = _pick_scale_bar_length_km(bounds)
    start_lon = min_lon + ((max_lon - min_lon) * 0.07)
    start_lat = min_lat + ((max_lat - min_lat) * 0.07)
    end_lon, end_lat, _ = _WGS84_GEOD.fwd(start_lon, start_lat, 90.0, bar_length_km * 1000.0)
    ax.plot(
        [start_lon, end_lon],
        [start_lat, end_lat],
        color="#111827",
        linewidth=2.6,
        solid_capstyle="butt",
        transform=ccrs.PlateCarree(),
        zorder=20,
    )
    ax.plot(
        [start_lon, start_lon],
        [start_lat - 0.05, start_lat + 0.05],
        color="#111827",
        linewidth=2.0,
        transform=ccrs.PlateCarree(),
        zorder=20,
    )
    ax.plot(
        [end_lon, end_lon],
        [end_lat - 0.05, end_lat + 0.05],
        color="#111827",
        linewidth=2.0,
        transform=ccrs.PlateCarree(),
        zorder=20,
    )
    ax.text(
        (start_lon + end_lon) / 2.0,
        start_lat + ((max_lat - min_lat) * 0.035),
        f"{bar_length_km} km",
        ha="center",
        va="bottom",
        fontsize=8.6,
        color="#111827",
        transform=ccrs.PlateCarree(),
        bbox={"boxstyle": "round,pad=0.16", "facecolor": (1, 1, 1, 0.88), "edgecolor": "none"},
        zorder=21,
    )


def _draw_north_arrow(ax, bounds: tuple[float, float, float, float]) -> None:
    min_lon, max_lon, min_lat, max_lat = bounds
    x = min_lon + ((max_lon - min_lon) * 0.93)
    y0 = min_lat + ((max_lat - min_lat) * 0.10)
    y1 = min_lat + ((max_lat - min_lat) * 0.19)
    ax.annotate(
        "",
        xy=(x, y1),
        xytext=(x, y0),
        xycoords=ccrs.PlateCarree()._as_mpl_transform(ax),
        textcoords=ccrs.PlateCarree()._as_mpl_transform(ax),
        arrowprops={
            "arrowstyle": "-|>",
            "linewidth": 1.6,
            "color": "#111827",
            "shrinkA": 0.0,
            "shrinkB": 0.0,
        },
        zorder=20,
    )
    ax.text(
        x,
        y1 + ((max_lat - min_lat) * 0.012),
        "N",
        ha="center",
        va="bottom",
        fontsize=8.8,
        fontweight="bold",
        color="#111827",
        transform=ccrs.PlateCarree(),
        bbox={"boxstyle": "round,pad=0.12", "facecolor": (1, 1, 1, 0.88), "edgecolor": "none"},
        zorder=21,
    )


def add_prototype_2016_geoaxes(
    fig,
    rect,
    display_bounds: list[float] | tuple[float, float, float, float],
    *,
    title: str | None = None,
    title_size: float = 14.0,
    show_grid_labels: bool = True,
    add_scale_bar: bool = True,
    add_north_arrow: bool = False,
    ocean_color: str = PROTOTYPE_2016_OCEAN_COLOR,
    land_color: str = PROTOTYPE_2016_LAND_COLOR,
):
    bounds = _normalize_bounds(display_bounds)
    ax = fig.add_axes(rect, projection=_prototype_2016_projection(bounds))
    ax.set_extent(bounds, crs=ccrs.PlateCarree())
    ax.set_facecolor(ocean_color)
    ax.add_feature(cfeature.OCEAN, facecolor=ocean_color, zorder=0)
    ax.add_feature(cfeature.LAND, facecolor=land_color, edgecolor="none", zorder=0.5)
    ax.coastlines(resolution="10m", linewidth=0.8, color="#111827", zorder=1.2)
    ax.add_feature(cfeature.BORDERS, linestyle=":", linewidth=0.5, edgecolor="#475569", zorder=1.1)
    gridlines = ax.gridlines(
        crs=ccrs.PlateCarree(),
        draw_labels=show_grid_labels,
        linewidth=0.5,
        color=PROTOTYPE_2016_GRID_COLOR,
        alpha=0.6,
        linestyle="--",
    )
    if show_grid_labels:
        gridlines.top_labels = False
        gridlines.right_labels = False
        gridlines.xlabel_style = {"size": 8, "color": "#334155"}
        gridlines.ylabel_style = {"size": 8, "color": "#334155"}
    if title:
        ax.set_title(title, fontsize=title_size, pad=18, fontfamily=PROTOTYPE_2016_FONT_FAMILY)
    if add_scale_bar:
        _draw_scale_bar(ax, bounds)
    if add_north_arrow:
        _draw_north_arrow(ax, bounds)
    for spine in ax.spines.values():
        spine.set_edgecolor("#111827")
        spine.set_linewidth(0.8)
    return ax


def add_prototype_2016_locator_inset(
    fig,
    parent_ax,
    *,
    crop_bounds: list[float] | tuple[float, float, float, float],
    locator_bounds: list[float] | tuple[float, float, float, float],
    rect: list[float] | tuple[float, float, float, float] = (0.74, 0.74, 0.22, 0.22),
):
    crop = _normalize_bounds(crop_bounds)
    locator = _normalize_bounds(locator_bounds)
    ax = add_prototype_2016_geoaxes(
        fig,
        figure_relative_inset_rect(parent_ax, rect),
        locator,
        show_grid_labels=False,
        add_scale_bar=False,
        add_north_arrow=False,
    )
    ax.add_patch(
        Rectangle(
            (crop[0], crop[2]),
            crop[1] - crop[0],
            crop[3] - crop[2],
            fill=False,
            linewidth=1.25,
            linestyle="-",
            edgecolor="#b42318",
            transform=ccrs.PlateCarree(),
            zorder=5,
        )
    )
    ax.set_title("Locator", fontsize=9.2, loc="left", pad=4)
    ax.set_xticks([])
    ax.set_yticks([])
    for spine in ax.spines.values():
        spine.set_edgecolor("#94a3b8")
        spine.set_linewidth(0.8)
    return ax


def _overlay_sparse_cell_edges(
    ax,
    *,
    dataset,
    positive_mask: np.ndarray,
    edgecolor: str,
    linewidth: float,
    alpha: float,
) -> None:
    positive_rows, positive_cols = np.where(positive_mask)
    if len(positive_rows) == 0 or len(positive_cols) == 0:
        return
    if len(positive_rows) > 196:
        return
    lefts, tops = rasterio.transform.xy(dataset.transform, positive_rows, positive_cols, offset="ul")
    rights, bottoms = rasterio.transform.xy(dataset.transform, positive_rows, positive_cols, offset="lr")
    for left, bottom, right, top in zip(lefts, bottoms, rights, tops):
        ax.add_patch(
            Rectangle(
                (float(left), float(bottom)),
                float(right) - float(left),
                float(top) - float(bottom),
                fill=False,
                edgecolor=edgecolor,
                linewidth=linewidth,
                alpha=alpha,
                zorder=3.2,
                transform=ccrs.PlateCarree(),
            )
        )


def figure_relative_inset_rect(parent_ax, rel_rect: list[float] | tuple[float, float, float, float]) -> list[float]:
    x0, y0, width, height = parent_ax.get_position().bounds
    rel_x, rel_y, rel_width, rel_height = [float(value) for value in rel_rect]
    return [
        x0 + (width * rel_x),
        y0 + (height * rel_y),
        width * rel_width,
        height * rel_height,
    ]

def plot_drifter_track(
    output_path: str,
    domain_bounds: list,
    unique_ids: list,
    df_found: pd.DataFrame,
    get_trajectory_func,
    target_dt: datetime,
    start_date: str,
    end_date: str = None
):
    """
    Plots drifter tracks for the given date range.
    """
    plt.figure(figsize=(12, 10))
    ax = plt.axes(projection=ccrs.PlateCarree())
    
    # Add coastlines and features
    ax.coastlines(resolution='10m')
    ax.add_feature(cfeature.LAND, facecolor='lightgray')

    ax.add_feature(cfeature.OCEAN, facecolor='azure')
    ax.add_feature(cfeature.BORDERS, linestyle=':')
    
    gl = ax.gridlines(draw_labels=True, linewidth=0.5, color='gray', alpha=0.5, linestyle='--')
    gl.top_labels = False
    gl.right_labels = False

    # Set extent with some padding around the active display domain.
    pad = 2
    ax.set_extent([domain_bounds[0]-pad, domain_bounds[1]+pad, domain_bounds[2]-pad, domain_bounds[3]+pad])

    # For each drifter, get full trajectory and plot
    for drifter_id in unique_ids:
        traj_df = get_trajectory_func(drifter_id, target_dt, days=14)
        
        if not traj_df.empty:
            # Plot full trajectory found
            ax.plot(traj_df['lon'], traj_df['lat'], '-', linewidth=2, label=f"Drifter {drifter_id} (Track)", transform=ccrs.PlateCarree())
            
            # Plot Start/End of trajectory
            ax.scatter(traj_df.iloc[0]['lon'], traj_df.iloc[0]['lat'], c='green', s=50, marker='o', transform=ccrs.PlateCarree(), zorder=5)
            ax.scatter(traj_df.iloc[-1]['lon'], traj_df.iloc[-1]['lat'], c='black', s=50, marker='x', transform=ccrs.PlateCarree(), zorder=5)

        # Highlight points found in the specific search window
        subset = df_found[df_found['ID'] == drifter_id]
        ax.scatter(subset['lon'], subset['lat'], c='red', s=80, marker='*', label=f"Observed in window", zorder=10, transform=ccrs.PlateCarree())

    plt.legend()
    period_str = f"{start_date} to {end_date}" if end_date else start_date
    plt.title(f"Drifter Tracking: {period_str}")
    
    plt.savefig(output_path, dpi=300, bbox_inches='tight')

def plot_trajectory_map(output_file: str, 
                       sim_lon: np.ndarray, 
                       sim_lat: np.ndarray, 
                       obs_lon: np.ndarray = None, 
                       obs_lat: np.ndarray = None, 
                       obs_ids: np.ndarray = None,
                       corners: list = None,
                       title: str = "Trajectory validation"):
    """
    Generates an HD map comparing simulated and observed trajectories
    using High-Resolution GSHHG coastlines.
    
    Args:
        output_file: Path to save the PNG
        sim_lon, sim_lat: Arrays of simulated coordinates
        obs_lon, obs_lat: Arrays of observed coordinates (optional)
        obs_ids: Array of drifter IDs corresponding to observations (optional)
        corners: [lon_min, lon_max, lat_min, lat_max] for map extent
        title: Plot title
    """
    # Create HD figure
    # Increased height slightly to accommodate subtitle/legend if needed
    plt.figure(figsize=(12, 11), dpi=300)
    
    # Setup map projection (PlateCarree for standard lat/lon)
    ax = plt.axes(projection=ccrs.PlateCarree())
    
    # Add High-Resolution Coastlines (GSHHG)
    # scale='h' is high resolution. 'f' is full but can be very slow/large. 'i' is intermediate.
    ax.coastlines(resolution='10m')
    ax.add_feature(cfeature.LAND, facecolor='lightgray')

    # Add Ocean/Borders features
    ax.add_feature(cfeature.BORDERS, linestyle=':', alpha=0.5)
    ax.add_feature(cfeature.OCEAN, facecolor='azure')

    # Gridlines
    gl = ax.gridlines(draw_labels=True, linewidth=0.5, color='gray', alpha=0.5, linestyle='--')
    gl.top_labels = False
    gl.right_labels = False

    # Plot Observed Drifter (if provided)
    if obs_lon is not None and obs_lat is not None:
        if obs_ids is not None:
            # Group by ID to prevent "spiderweb" lines connecting distinct drifters
            unique_ids = np.unique(obs_ids)
            for i, drifter_id in enumerate(unique_ids):
                # Mask for current drifter
                mask = (obs_ids == drifter_id)
                curr_lon = obs_lon[mask]
                curr_lat = obs_lat[mask]
                
                # Only label the first one to avoid legend spam
                lbl = 'Actual Drifter' if i == 0 else None
                
                ax.plot(curr_lon, curr_lat, 'k-', transform=ccrs.PlateCarree(), 
                        linewidth=2.5, label=lbl, zorder=10)
                
                # Start/End markers for each segment
                ax.scatter(curr_lon[0], curr_lat[0], c='green', s=60, marker='o', 
                           edgecolors='black', zorder=11, transform=ccrs.PlateCarree(),
                           label='Start' if i == 0 else None)
                ax.scatter(curr_lon[-1], curr_lat[-1], c='black', s=60, marker='x', 
                           zorder=11, transform=ccrs.PlateCarree(),
                           label=None) # Only label start
        else:
            # Fallback for single track
            ax.plot(obs_lon, obs_lat, 'k-', transform=ccrs.PlateCarree(), 
                    linewidth=2.5, label='Actual Drifter', zorder=10)
            # Start/End markers
            ax.scatter(obs_lon[0], obs_lat[0], c='green', s=100, marker='o', 
                       edgecolors='black', label='Start', zorder=11, transform=ccrs.PlateCarree())
            ax.scatter(obs_lon[-1], obs_lat[-1], c='black', s=100, marker='x', 
                       zorder=11, transform=ccrs.PlateCarree())

    # Plot Simulated Trajectory
    ax.plot(sim_lon, sim_lat, 'r--', transform=ccrs.PlateCarree(), 
            linewidth=2, label='Model Prediction', zorder=9)
    ax.scatter(sim_lon[-1], sim_lat[-1], c='red', s=80, marker='x', 
               zorder=11, transform=ccrs.PlateCarree())

    # Calculate Data Bounds with Padding
    data_extent = None
    all_lons = []
    all_lats = []
    if sim_lon is not None: all_lons.append(sim_lon); all_lats.append(sim_lat)
    if obs_lon is not None: all_lons.append(obs_lon); all_lats.append(obs_lat)
    
    if all_lons:
        cat_lons = np.concatenate(all_lons)
        cat_lats = np.concatenate(all_lats)
        min_lon, max_lon = np.min(cat_lons), np.max(cat_lons)
        min_lat, max_lat = np.min(cat_lats), np.max(cat_lats)
        
        # Add padding
        lon_span = max_lon - min_lon
        lat_span = max_lat - min_lat
        pad_lon = max(0.2, lon_span * 0.2)
        pad_lat = max(0.2, lat_span * 0.2)
        data_extent = [min_lon - pad_lon, max_lon + pad_lon, min_lat - pad_lat, max_lat + pad_lat]

    # Set Main Map Extent
    if corners:
        ax.set_extent(corners, crs=ccrs.PlateCarree())
        
        # --- Inset Map logic: if data is small relative to the display domain, zoom in ---
        if data_extent:
            map_span_lon = corners[1] - corners[0]
            data_span_lon = data_extent[1] - data_extent[0]
            
            # If trajectory covers less than 40% of the map, create an inset
            if data_span_lon < (map_span_lon * 0.4):
                # Create inset axis (Bottom Right usually empty in ocean maps)
                axins = ax.inset_axes([0.6, 0.6, 0.35, 0.35], projection=ccrs.PlateCarree())
                
                # Add basic features to inset
                axins.coastlines()
                axins.add_feature(cfeature.LAND, facecolor='lightgray')
                axins.add_feature(cfeature.OCEAN, facecolor='azure')
                
                # Plot lines on inset
                axins.plot(sim_lon, sim_lat, 'r--', linewidth=2, transform=ccrs.PlateCarree())
                if obs_lon is not None:
                     axins.plot(obs_lon, obs_lat, 'k-', linewidth=2, transform=ccrs.PlateCarree())
                
                # Set inset extent
                axins.set_extent(data_extent, crs=ccrs.PlateCarree())
                
                # Add framing
                ax.indicate_inset_zoom(axins, edgecolor="black")
                
    elif data_extent:
        # Dynamic Auto-Zoom Fallback checks
        ax.set_extent(data_extent, crs=ccrs.PlateCarree())

    # Labels and Legend
    plt.title(title, fontsize=14, pad=20)
    
    # Move legend outside to prevent blocking the map
    plt.legend(loc='upper center', bbox_to_anchor=(0.5, -0.05),
              fancybox=True, shadow=True, ncol=3)
    
    # Save
    plt.savefig(output_file, bbox_inches='tight', dpi=300)
    plt.close()


def _setup_legacy_geoaxes(corners: list[float], title: str):
    fig = plt.figure(figsize=(12, 11), dpi=300)
    ax = add_prototype_2016_geoaxes(
        fig,
        [0.06, 0.10, 0.88, 0.80],
        corners,
        title=title,
        title_size=14,
        add_north_arrow=True,
    )
    return fig, ax


def _plot_legacy_drifter_track(ax, drifter_track_df: pd.DataFrame, label: str = "Observed drifter track") -> None:
    if drifter_track_df.empty:
        raise ValueError("Cannot plot a drifter track because the input DataFrame is empty.")

    lons = np.asarray(drifter_track_df["lon"], dtype=float)
    lats = np.asarray(drifter_track_df["lat"], dtype=float)
    ax.plot(
        lons,
        lats,
        color="#111827",
        linewidth=2.5,
        label=label,
        zorder=9,
        transform=ccrs.PlateCarree(),
    )
    ax.scatter(
        lons[0],
        lats[0],
        c="#16A34A",
        s=90,
        marker="o",
        edgecolors="black",
        label="Track start",
        zorder=10,
        transform=ccrs.PlateCarree(),
    )
    ax.scatter(
        lons[-1],
        lats[-1],
        c="#111827",
        s=90,
        marker="X",
        label="Track end",
        zorder=10,
        transform=ccrs.PlateCarree(),
    )


def _draw_legacy_mask_overlay(ax, mask_path: str, color: str, alpha: float) -> bool:
    if rasterio is None:
        raise ImportError("rasterio is required to render legacy mask overlays.")

    with rasterio.open(mask_path) as dataset:
        values = dataset.read(1)
        positive_mask = np.isfinite(values) & (values > 0)
        masked = np.ma.masked_where(~positive_mask, values)
        if masked.mask.all():
            return False
        bounds = dataset.bounds
        ax.imshow(
            masked,
            extent=(bounds.left, bounds.right, bounds.bottom, bounds.top),
            origin="upper",
            cmap=ListedColormap([color]),
            interpolation="nearest",
            alpha=alpha,
            zorder=2,
            transform=ccrs.PlateCarree(),
        )
        _overlay_sparse_cell_edges(
            ax,
            dataset=dataset,
            positive_mask=positive_mask,
            edgecolor=color,
            linewidth=0.45,
            alpha=min(0.85, alpha + 0.25),
        )
    return True


def plot_legacy_drifter_track_map(
    output_file: str,
    drifter_track_df: pd.DataFrame,
    corners: list[float],
    title: str,
    extent_mode: str = PROTOTYPE_2016_EXTENT_MODE_FIXED_REGIONAL,
    locator_bounds: list[float] | tuple[float, float, float, float] | None = None,
) -> None:
    plot_bounds = derive_prototype_2016_figure_bounds(
        base_bounds=corners,
        bounds_sets=[bounds_from_track_dataframe(drifter_track_df)] if bounds_from_track_dataframe(drifter_track_df) else [],
        extent_mode=extent_mode,
        minimum_span_degrees=0.55,
    )
    with matplotlib.rc_context({"font.family": PROTOTYPE_2016_FONT_FAMILY}):
        fig, ax = _setup_legacy_geoaxes(list(plot_bounds), title)
        _plot_legacy_drifter_track(ax, drifter_track_df)
        add_prototype_2016_locator_inset(
            fig,
            ax,
            crop_bounds=plot_bounds,
            locator_bounds=locator_bounds or corners,
        )
        ax.legend(loc="upper center", bbox_to_anchor=(0.5, -0.05), fancybox=True, shadow=True, ncol=3, fontsize=9)
        fig.savefig(output_file, bbox_inches="tight", dpi=300)
        plt.close(fig)


def plot_legacy_drifter_track_ensemble_overlay(
    output_file: str,
    drifter_track_df: pd.DataFrame,
    p50_mask_path: str,
    p90_mask_path: str,
    corners: list[float],
    title: str,
    extent_mode: str = PROTOTYPE_2016_EXTENT_MODE_FIXED_REGIONAL,
    locator_bounds: list[float] | tuple[float, float, float, float] | None = None,
) -> None:
    drifter_bounds = bounds_from_track_dataframe(drifter_track_df)
    plot_bounds = derive_prototype_2016_figure_bounds(
        base_bounds=corners,
        bounds_sets=[
            bounds
            for bounds in (
                drifter_bounds,
                positive_raster_bounds(p50_mask_path) if p50_mask_path and Path(p50_mask_path).exists() else None,
                positive_raster_bounds(p90_mask_path) if p90_mask_path and Path(p90_mask_path).exists() else None,
            )
            if bounds is not None
        ],
        extent_mode=extent_mode,
        minimum_span_degrees=0.60,
    )
    with matplotlib.rc_context({"font.family": PROTOTYPE_2016_FONT_FAMILY}):
        fig, ax = _setup_legacy_geoaxes(list(plot_bounds), title)
        _draw_legacy_mask_overlay(ax, p50_mask_path, color="#0f766e", alpha=0.24)
        _draw_legacy_mask_overlay(ax, p90_mask_path, color="#9a3412", alpha=0.34)
        _plot_legacy_drifter_track(ax, drifter_track_df, label="Observed drifter-of-record")
        add_prototype_2016_locator_inset(
            fig,
            ax,
            crop_bounds=plot_bounds,
            locator_bounds=locator_bounds or corners,
        )
        legend_handles = [
            Patch(facecolor="#0f766e", edgecolor="#0f766e", alpha=0.24, label="Ensemble p50 footprint"),
            Patch(facecolor="#9a3412", edgecolor="#9a3412", alpha=0.34, label="Ensemble p90 footprint"),
            Line2D([0], [0], color="#111827", linewidth=2.5, label="Observed drifter track"),
            Line2D([0], [0], marker="o", linestyle="None", markerfacecolor="#16A34A", markeredgecolor="black", markersize=9, label="Track start"),
            Line2D([0], [0], marker="X", linestyle="None", markerfacecolor="#111827", markeredgecolor="#111827", markersize=9, label="Track end"),
        ]
        ax.legend(
            handles=legend_handles,
            loc="upper center",
            bbox_to_anchor=(0.5, -0.05),
            fancybox=True,
            shadow=True,
            ncol=2,
            fontsize=9,
        )
        fig.savefig(output_file, bbox_inches="tight", dpi=300)
        plt.close(fig)


def render_projected_probability_map(
    *,
    output_file: str,
    probability_raster_path: str,
    start_lon: float,
    start_lat: float,
    display_bounds: list[float] | tuple[float, float, float, float],
    title: str,
    p50_mask_path: str | None = None,
    p90_mask_path: str | None = None,
    extent_mode: str = PROTOTYPE_2016_EXTENT_MODE_FIXED_REGIONAL,
    locator_bounds: list[float] | tuple[float, float, float, float] | None = None,
) -> None:
    if rasterio is None:
        raise ImportError("rasterio is required to render probability rasters.")

    probability_bounds = positive_raster_bounds(probability_raster_path) if Path(probability_raster_path).exists() else None
    point_pad = 0.06
    plot_bounds = derive_prototype_2016_figure_bounds(
        base_bounds=display_bounds,
        bounds_sets=[
            bounds
            for bounds in (
                probability_bounds,
                positive_raster_bounds(p50_mask_path) if p50_mask_path and Path(p50_mask_path).exists() else None,
                positive_raster_bounds(p90_mask_path) if p90_mask_path and Path(p90_mask_path).exists() else None,
                bounds_from_point((float(start_lon), float(start_lat)), lon_pad=point_pad, lat_pad=point_pad),
            )
            if bounds is not None
        ],
        extent_mode=extent_mode,
        minimum_span_degrees=0.60,
    )
    with matplotlib.rc_context({"font.family": PROTOTYPE_2016_FONT_FAMILY}):
        fig = plt.figure(figsize=(12, 11), dpi=300)
        ax = add_prototype_2016_geoaxes(
            fig,
            [0.06, 0.10, 0.88, 0.80],
            plot_bounds,
            title=title,
            title_size=14,
            add_north_arrow=True,
        )
        with rasterio.open(probability_raster_path) as dataset:
            probability = dataset.read(1)
            positive_mask = np.isfinite(probability) & (probability > 0)
            masked = np.ma.masked_where(~positive_mask, probability)
            if not masked.mask.all():
                bounds = dataset.bounds
                ax.imshow(
                    masked,
                    extent=(bounds.left, bounds.right, bounds.bottom, bounds.top),
                    origin="upper",
                    cmap="YlOrRd",
                    interpolation="nearest",
                    alpha=0.42,
                    zorder=2,
                    transform=ccrs.PlateCarree(),
                )
                _overlay_sparse_cell_edges(
                    ax,
                    dataset=dataset,
                    positive_mask=positive_mask,
                    edgecolor="#b45309",
                    linewidth=0.42,
                    alpha=0.62,
                )
        if p50_mask_path:
            _draw_legacy_mask_overlay(ax, p50_mask_path, color="#0f766e", alpha=0.22)
        if p90_mask_path:
            _draw_legacy_mask_overlay(ax, p90_mask_path, color="#9a3412", alpha=0.32)
        ax.plot(
            start_lon,
            start_lat,
            marker="*",
            color="#1d9b1d",
            markersize=15,
            markeredgecolor="#111827",
            label="Drifter-of-record release point",
            zorder=10,
            transform=ccrs.PlateCarree(),
        )
        add_prototype_2016_locator_inset(
            fig,
            ax,
            crop_bounds=plot_bounds,
            locator_bounds=locator_bounds or display_bounds,
        )
        legend_handles = [
            Line2D([0], [0], marker="*", color="w", markerfacecolor="#1d9b1d", markeredgecolor="#111827", markersize=15, label="Drifter-of-record release point"),
            Patch(facecolor="#f59e0b", alpha=0.42, label="Probability support field"),
        ]
        if p50_mask_path:
            legend_handles.append(Line2D([0], [0], color="#0f766e", linewidth=2.0, label="Ensemble p50 footprint"))
        if p90_mask_path:
            legend_handles.append(Line2D([0], [0], color="#9a3412", linewidth=2.0, label="Ensemble p90 footprint"))
        ax.legend(
            handles=legend_handles,
            loc="upper center",
            bbox_to_anchor=(0.5, -0.05),
            fancybox=True,
            shadow=True,
            ncol=2,
            fontsize=9,
        )
        fig.savefig(output_file, bbox_inches="tight", dpi=300)
        plt.close(fig)


# ===========================================================================
# Phase 3 – Mass Budget Charts
# ===========================================================================

def plot_mass_budget_chart(
    budget_df,
    output_file: str,
    title: str = "Oil Mass Budget (72 h)",
    color: str = "#FF8C00",
):
    """
    Stacked area / line chart showing the 72-hour mass budget for one oil type.

    Expects budget_df columns:
        hours_elapsed, surface_pct, evaporated_pct, dispersed_pct, beached_pct
    """
    hours = budget_df["hours_elapsed"].values
    surface    = budget_df["surface_pct"].values
    evaporated = budget_df["evaporated_pct"].values
    dispersed  = budget_df["dispersed_pct"].values
    beached    = budget_df["beached_pct"].values

    fig, (ax_stacked, ax_lines) = plt.subplots(1, 2, figsize=(16, 6), dpi=200)
    fig.suptitle(title, fontsize=14, fontweight="bold", y=1.02)

    # ── Left: Stacked Area ──────────────────────────────────────────────
    ax_stacked.stackplot(
        hours,
        surface, evaporated, dispersed, beached,
        labels=["Surface", "Evaporated", "Dispersed", "Beached"],
        colors=["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728"],
        alpha=0.8,
    )
    ax_stacked.set_xlim(0, hours[-1])
    ax_stacked.set_ylim(0, 100)
    ax_stacked.set_xlabel("Time Elapsed (hours)", fontsize=11)
    ax_stacked.set_ylabel("Mass Fraction (%)", fontsize=11)
    ax_stacked.set_title("Stacked Mass Budget", fontsize=12)
    ax_stacked.legend(loc="upper right", fontsize=9)
    ax_stacked.grid(axis="y", linestyle="--", alpha=0.5)

    # ── Right: Individual Lines ────────────────────────────────────────
    line_styles = [
        ("Surface",    surface,    "#1f77b4", "-",  2.5),
        ("Evaporated", evaporated, "#ff7f0e", "--", 2.0),
        ("Dispersed",  dispersed,  "#2ca02c", "-.", 2.0),
        ("Beached",    beached,    "#d62728", ":",  2.0),
    ]
    for label, data, lc, ls, lw in line_styles:
        ax_lines.plot(hours, data, color=lc, linestyle=ls, linewidth=lw, label=label)

    ax_lines.set_xlim(0, hours[-1])
    ax_lines.set_ylim(0, max(surface.max() * 1.1, 5))
    ax_lines.set_xlabel("Time Elapsed (hours)", fontsize=11)
    ax_lines.set_ylabel("Mass Fraction (%)", fontsize=11)
    ax_lines.set_title("Component Trends", fontsize=12)
    ax_lines.legend(loc="upper right", fontsize=9)
    ax_lines.grid(linestyle="--", alpha=0.4)

    # ── 72 h boundary marker ──────────────────────────────────────────
    for ax in (ax_stacked, ax_lines):
        ax.axvline(x=72, color="gray", linestyle=":", linewidth=1)
        ax.annotate(
            "72 h",
            xy=(72, ax.get_ylim()[1] * 0.95),
            fontsize=8,
            color="gray",
            ha="right",
        )

    plt.tight_layout()
    plt.savefig(output_file, bbox_inches="tight", dpi=200)
    plt.close()


def plot_mass_budget_comparison(
    results: dict,
    output_file: str,
    title: str = "Phase 3 – Oil Type Comparison: Mass Budget Over 72 Hours",
):
    """
    Side-by-side comparison of mass budgets for all oil types.

    Parameters
    ----------
    results : dict
        Keys are oil type labels ('light', 'heavy'), values contain
        'display_name' and 'budget_df'.
    output_file : str
    """
    oil_keys = list(results.keys())
    n = len(oil_keys)

    fig, axes = plt.subplots(
        2, n, figsize=(9 * n, 10), dpi=200,
        gridspec_kw={"hspace": 0.45, "wspace": 0.3},
    )
    if n == 1:
        axes = np.array([[axes[0]], [axes[1]]])

    component_map = {
        "Surface":    ("surface_pct",    "#1f77b4", "-",  2.5),
        "Evaporated": ("evaporated_pct", "#ff7f0e", "--", 2.0),
        "Dispersed":  ("dispersed_pct",  "#2ca02c", "-.", 2.0),
        "Beached":    ("beached_pct",    "#d62728", ":",  2.0),
    }

    for col, oil_key in enumerate(oil_keys):
        df   = results[oil_key]["budget_df"]
        name = results[oil_key]["display_name"]
        hours = df["hours_elapsed"].values

        # Top row: stacked area
        ax_top = axes[0, col]
        arrays      = [df[v].values for _, (v, *_) in component_map.items()]
        labels      = list(component_map.keys())
        colors_fill = [c for _, (_, c, *_) in component_map.items()]
        ax_top.stackplot(hours, *arrays, labels=labels, colors=colors_fill, alpha=0.8)
        ax_top.set_title(f"{name}\n(Stacked Budget)", fontsize=11, fontweight="bold")
        ax_top.set_xlim(0, hours[-1])
        ax_top.set_ylim(0, 100)
        ax_top.set_xlabel("Hours Elapsed")
        ax_top.set_ylabel("Mass %")
        ax_top.legend(loc="upper right", fontsize=8)
        ax_top.grid(axis="y", linestyle="--", alpha=0.4)

        # Bottom row: line trends
        ax_bot = axes[1, col]
        for label, (col_name, lc, ls, lw) in component_map.items():
            ax_bot.plot(hours, df[col_name].values,
                        color=lc, linestyle=ls, linewidth=lw, label=label)
        ax_bot.set_title(f"{name}\n(Component Trends)", fontsize=11, fontweight="bold")
        ax_bot.set_xlim(0, hours[-1])
        ax_bot.set_xlabel("Hours Elapsed")
        ax_bot.set_ylabel("Mass %")
        ax_bot.legend(loc="upper right", fontsize=8)
        ax_bot.grid(linestyle="--", alpha=0.4)

        for ax in (ax_top, ax_bot):
            ax.axvline(x=72, color="gray", linestyle=":", linewidth=1)

    fig.suptitle(
        "Phase 3 – Oil Type Comparison: Mass Budget Over 72 Hours",
        fontsize=15, fontweight="bold", y=1.01,
    )
    fig.suptitle(title, fontsize=15, fontweight="bold", y=1.01)
    plt.savefig(output_file, bbox_inches="tight", dpi=200)
    plt.close()


def plot_gnome_vs_openoil(
    openoil_df,
    gnome_df,
    output_file: str,
    title: str = "Cross-Model Comparison: OpenOil vs PyGNOME",
):
    """
    Overlay comparison of OpenOil and PyGNOME mass budgets for the same scenario.
    """
    components = [
        ("surface_pct",    "Surface"),
        ("evaporated_pct", "Evaporated"),
        ("dispersed_pct",  "Dispersed"),
        ("beached_pct",    "Beached"),
    ]
    colors = ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728"]

    fig, axes = plt.subplots(2, 2, figsize=(14, 9), dpi=200)
    axes = axes.flatten()

    def _safe_col(df, col):
        return df[col].values if col in df.columns else np.zeros(len(df))

    for ax, (col, label), clr in zip(axes, components, colors):
        min_len = min(len(openoil_df), len(gnome_df))
        oo_h = (openoil_df["hours_elapsed"].values if "hours_elapsed" in openoil_df.columns
                else np.arange(min_len))[:min_len]
        gn_h = (gnome_df["hours_elapsed"].values   if "hours_elapsed" in gnome_df.columns
                else np.arange(min_len))[:min_len]

        ax.plot(oo_h, _safe_col(openoil_df, col)[:min_len],
                color=clr, linewidth=2.5, linestyle="-",  label="OpenOil")
        ax.plot(gn_h, _safe_col(gnome_df,   col)[:min_len],
                color=clr, linewidth=2.0, linestyle="--", label="PyGNOME", alpha=0.8)

        ax.set_title(label, fontsize=12, fontweight="bold")
        ax.set_xlabel("Hours Elapsed", fontsize=10)
        ax.set_ylabel("Mass %", fontsize=10)
        ax.legend(fontsize=9)
        ax.grid(linestyle="--", alpha=0.4)
        ax.set_xlim(left=0)
        ax.set_ylim(bottom=0)

    fig.suptitle(title, fontsize=14, fontweight="bold", y=1.01)
    plt.tight_layout()
    plt.savefig(output_file, bbox_inches="tight", dpi=200)
    plt.close()


def plot_probability_map(output_file: str, 
                        all_lons: np.ndarray = None, 
                        all_lats: np.ndarray = None, 
                        prob_grid: np.ndarray = None,
                        lon_bins: np.ndarray = None,
                        lat_bins: np.ndarray = None,
                        start_lon: float = 0, 
                        start_lat: float = 0,
                        corners: list = None,
                        title: str = "Ensemble Probability Forecast",
                        use_projected_case_local: bool = False,
                        probability_raster_path: str | None = None,
                        p50_mask_path: str | None = None,
                        p90_mask_path: str | None = None,
                        extent_mode: str = PROTOTYPE_2016_EXTENT_MODE_FIXED_REGIONAL):
    """
    Generates a 2D histograms probability map for ensemble forecasts.
    Can accept either raw coordinates (all_lons/all_lats) or pre-binned prob_grid.
    """
    if use_projected_case_local:
        if not probability_raster_path:
            raise ValueError("Projected case-local probability rendering requires probability_raster_path.")
        render_projected_probability_map(
            output_file=output_file,
            probability_raster_path=probability_raster_path,
            start_lon=float(start_lon),
            start_lat=float(start_lat),
            display_bounds=corners,
            title=title,
            p50_mask_path=p50_mask_path,
            p90_mask_path=p90_mask_path,
            extent_mode=extent_mode,
            locator_bounds=corners,
        )
        return

    if prob_grid is None and (all_lons is None or len(all_lons) == 0):
        print("Warning: No data to plot.")
        return

    # --- 1. PREPARE GRID DATA ---
    if prob_grid is not None:
        # Use provided pre-binned grid
        H = prob_grid.T # Histogram expects [x, y]
        X, Y = np.meshgrid(lon_bins, lat_bins)
    else:
        # Bin raw data (legacy or fallback mode)
        x = np.array(all_lons)
        y = np.array(all_lats)
        
        # Use 100 bins or derive from corners
        if corners:
            # Match GRID_RESOLUTION concept if possible
            bins_x = np.arange(corners[0], corners[1], 0.05)
            bins_y = np.arange(corners[2], corners[3], 0.05)
            H, xedges, yedges = np.histogram2d(x, y, bins=[bins_x, bins_y])
        else:
            H, xedges, yedges = np.histogram2d(x, y, bins=100)
            
        X, Y = np.meshgrid((xedges[:-1]+xedges[1:])/2, (yedges[:-1]+yedges[1:])/2)

    # Calculate cumulative distribution to find percentile thresholds
    H_flat = np.sort(H.flatten())[::-1] 
    H_sum = np.sum(H_flat)
    if H_sum == 0: 
        print("Warning: Probability mass is zero.")
        return
        
    H_cumsum = np.cumsum(H_flat)
    
    # Find thresholds for 50% and 90% mass
    thresh_50 = H_flat[np.searchsorted(H_cumsum, 0.50 * H_sum)]
    thresh_90 = H_flat[np.searchsorted(H_cumsum, 0.90 * H_sum)]
    
    # --- 2. MAP SETUP (Matches plot_trajectory_map) ---
    plt.figure(figsize=(12, 11), dpi=300)
    ax = plt.axes(projection=ccrs.PlateCarree())

    ax.coastlines(resolution='10m')
    ax.add_feature(cfeature.LAND, facecolor='lightgray')

    ax.add_feature(cfeature.BORDERS, linestyle=':', alpha=0.5)
    ax.add_feature(cfeature.OCEAN, facecolor='azure')
    
    gl = ax.gridlines(draw_labels=True, linewidth=0.5, color='gray', alpha=0.5, linestyle='--')
    gl.top_labels = False
    gl.right_labels = False

    # Calculate Data Bounds for Zoom
    if prob_grid is not None:
        valid_indices = np.where(prob_grid > 0)
        if len(valid_indices[0]) > 0:
            min_lon, max_lon = lon_bins[valid_indices[1]].min(), lon_bins[valid_indices[1]].max()
            min_lat, max_lat = lat_bins[valid_indices[0]].min(), lat_bins[valid_indices[0]].max()
        else:
            min_lon, max_lon, min_lat, max_lat = corners if corners else (0, 1, 0, 1)
    else:
        min_lon, max_lon = x.min(), x.max()
        min_lat, max_lat = y.min(), y.max()
    
    # Add significant padding (1.0 degree)
    pad_lon = max(1.0, (max_lon - min_lon) * 0.5)
    pad_lat = max(1.0, (max_lat - min_lat) * 0.5)
    pad_lat = max(1.0, (max_lat - min_lat) * 0.5)
    
    zoom_extent = [
        min_lon - pad_lon, 
        max_lon + pad_lon, 
        min_lat - pad_lat, 
        max_lat + pad_lat
    ]
    
    # Ensure we don't zoom out BEYOND the fixed corners if they exist
    if corners:
        final_extent = [
            max(zoom_extent[0], corners[0]),
            min(zoom_extent[1], corners[1]),
            max(zoom_extent[2], corners[2]),
            min(zoom_extent[3], corners[3])
        ]
        ax.set_extent(final_extent, crs=ccrs.PlateCarree())
    else:
        ax.set_extent(zoom_extent, crs=ccrs.PlateCarree())

    # --- 3. PLOTTING ---
    # Transposing H with H.T is necessary because np.histogram2d returns H[x, y]
    # while contourf expects grid defined as meshgrid(X, Y), where X varies with columns, Y with rows.
    
    h_max = H.max()
    
    if h_max > thresh_90:
        # 90% Contour (Possible) - Yellow
        ax.contourf(X, Y, H.T, levels=[thresh_90, h_max + 1e-9], colors=['#FFD700'], alpha=0.3, transform=ccrs.PlateCarree())
        ax.contour(X, Y, H.T, levels=[thresh_90], colors=['#DAA520'], linewidths=1, linestyles='--', transform=ccrs.PlateCarree())
    
    if h_max > thresh_50:
        # 50% Contour (Likely) - Red
        ax.contourf(X, Y, H.T, levels=[thresh_50, h_max + 1e-9], colors=['#FF4500'], alpha=0.4, transform=ccrs.PlateCarree())
        ax.contour(X, Y, H.T, levels=[thresh_50], colors=['#8B0000'], linewidths=2, transform=ccrs.PlateCarree())
    
    # Plot Start
    ax.plot(start_lon, start_lat, marker='*', color='green', markersize=15, markeredgecolor='black', 
            label='Spill Origin', zorder=10, transform=ccrs.PlateCarree())

    # --- 4. INSET LOGIC ---
    # Disabled for now as the main map is now auto-zoomed to a comfortable level
    """
    # Data bounds
    min_lon, max_lon = x.min(), x.max()
    min_lat, max_lat = y.min(), y.max()
    data_extent = [min_lon - 0.2, max_lon + 0.2, min_lat - 0.2, max_lat + 0.2]
    
    if corners:
        map_span_lon = corners[1] - corners[0]
        data_span_lon = data_extent[1] - data_extent[0]
        
        # If trajectory covers less than 40% of the map, create an inset
        if data_span_lon < (map_span_lon * 0.4):
            axins = ax.inset_axes([0.6, 0.6, 0.35, 0.35], projection=ccrs.PlateCarree())
            axins.coastlines()
            axins.add_feature(cfeature.LAND, facecolor='lightgray')
            axins.add_feature(cfeature.OCEAN, facecolor='azure')
            
            # Re-plot key features on inset
            axins.contourf(X, Y, H.T, levels=[thresh_90, H.max()], colors=['#FFD700'], alpha=0.3, transform=ccrs.PlateCarree())
            axins.contourf(X, Y, H.T, levels=[thresh_50, H.max()], colors=['#FF4500'], alpha=0.4, transform=ccrs.PlateCarree())
            axins.plot(start_lon, start_lat, marker='*', color='green', markersize=12, markeredgecolor='black', transform=ccrs.PlateCarree())
            
            axins.set_extent(data_extent, crs=ccrs.PlateCarree())
            ax.indicate_inset_zoom(axins, edgecolor="black")
    """

    # --- 5. LEGEND & SAVE ---
    from matplotlib.lines import Line2D
    from matplotlib.patches import Patch
    legend_elements = [
        Line2D([0], [0], marker='*', color='w', markerfacecolor='g', markersize=15, label='Spill Origin'),
        Patch(facecolor='#FF4500', alpha=0.4, label='50% Probability (Likely)'),
        Patch(facecolor='#FFD700', alpha=0.3, label='90% Probability (Possible)')
    ]
    
    plt.title(title, fontsize=14, pad=20)
    plt.legend(handles=legend_elements, loc='upper center', bbox_to_anchor=(0.5, -0.05),
                fancybox=True, shadow=True, ncol=3)

    plt.savefig(output_file, bbox_inches='tight', dpi=300)
    plt.close()


# ===========================================================================
# Phase 3 Enhancement – Diagnostic Charts
# ===========================================================================

def plot_diagnostic_forcing(
    diag_report: dict,
    output_file: str = None,
    title: str = "Phase 3 Enhancement – Pre-Flight Diagnostics",
):
    """
    Visual summary of environmental forcing diagnostics.

    Creates a 3-panel figure:
      1. Wind speed gauge (max vs thresholds)
      2. Current speed gauge
      3. Config audit pass/fail summary
    """
    if output_file is None:
        from src.core.constants import BASE_OUTPUT_DIR
        output_file = str(BASE_OUTPUT_DIR / "diagnostics" / "forcing_summary.png")
    
    from pathlib import Path
    Path(output_file).parent.mkdir(parents=True, exist_ok=True)

    fig, axes = plt.subplots(1, 3, figsize=(16, 5), dpi=200)

    # --- Panel 1: Wind Speed ---
    ax = axes[0]
    wind = diag_report.get("environmental_forcing", {}).get("wind", {})
    max_ws = wind.get("max_wind_speed_ms", 0)
    mean_ws = wind.get("mean_wind_speed_ms", 0)

    thresholds = [0, 5, 12, 25, 40]
    colors_bar = ["#2ca02c", "#ff7f0e", "#d62728", "#7f0000"]

    # Background threshold zones
    for i in range(len(thresholds) - 1):
        ax.barh(0, thresholds[i+1] - thresholds[i], left=thresholds[i],
                height=0.6, color=colors_bar[i], alpha=0.3, edgecolor="none")
    # Marker for actual max
    ax.barh(0, max_ws, height=0.3, color="black", alpha=0.9)
    ax.axvline(x=12, color="orange", linestyle="--", linewidth=1.5, label="Moderate threshold")
    ax.axvline(x=25, color="red", linestyle="--", linewidth=1.5, label="Extreme threshold")
    ax.scatter([max_ws], [0], color="red", s=100, zorder=5, marker="|", linewidths=3)
    ax.set_xlim(0, max(40, max_ws * 1.2))
    ax.set_yticks([])
    ax.set_xlabel("Wind Speed (m/s)")
    ax.set_title(f"Max Wind: {max_ws:.1f} m/s\nMean: {mean_ws:.1f} m/s", fontsize=11, fontweight="bold")
    ax.legend(fontsize=8, loc="upper right")

    # --- Panel 2: Current Speed ---
    ax = axes[1]
    curr = diag_report.get("environmental_forcing", {}).get("currents", {})
    max_cs = curr.get("max_current_speed_ms", 0)
    mean_cs = curr.get("mean_current_speed_ms", 0)

    ax.barh(0, max_cs, height=0.3, color="#1f77b4", alpha=0.9)
    ax.axvline(x=0.5, color="orange", linestyle="--", linewidth=1.5, label="Strong current")
    ax.axvline(x=1.5, color="red", linestyle="--", linewidth=1.5, label="Extreme current")
    ax.set_xlim(0, max(2.0, max_cs * 1.2))
    ax.set_yticks([])
    ax.set_xlabel("Current Speed (m/s)")
    ax.set_title(f"Max Current: {max_cs:.3f} m/s\nMean: {mean_cs:.3f} m/s", fontsize=11, fontweight="bold")
    ax.legend(fontsize=8, loc="upper right")

    # --- Panel 3: Config Audit ---
    ax = axes[2]
    config = diag_report.get("openoil_config", {})
    mismatches = config.get("mismatches", [])
    total_checked = len(config.get("current_values", {}))
    passed = total_checked - len(mismatches)

    if total_checked > 0:
        wedges = [passed, len(mismatches)]
        colors_pie = ["#2ca02c", "#d62728"]
        labels_pie = [f"Pass ({passed})", f"Mismatch ({len(mismatches)})"]
        ax.pie(wedges, labels=labels_pie, colors=colors_pie, autopct="%1.0f%%",
               startangle=90, textprops={"fontsize": 10})
        ax.set_title(f"Config Audit: {passed}/{total_checked}", fontsize=11, fontweight="bold")
    else:
        ax.text(0.5, 0.5, "No config data", ha="center", va="center", fontsize=12)
        ax.set_title("Config Audit", fontsize=11, fontweight="bold")

    fig.suptitle(
        "Phase 3 Enhancement – Pre-Flight Diagnostics",
        fontsize=14, fontweight="bold", y=1.02,
    )
    fig.suptitle(title, fontsize=14, fontweight="bold", y=1.02)
    plt.tight_layout()
    plt.savefig(output_file, bbox_inches="tight", dpi=200)
    plt.close()
