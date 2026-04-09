"""
Rasterization helpers for PyGNOME and OpenDrift outputs.

Prototype mode keeps the legacy geographic grid. Official mode uses the
canonical projected scoring grid and performs real coordinate reprojection
before histogramming or rasterization.
"""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr

from src.helpers.scoring import GEOGRAPHIC_CRS, ScoringGridSpec, get_scoring_grid_spec

try:
    import rasterio
    from rasterio.features import rasterize as rio_rasterize
    from rasterio.transform import from_origin
    from rasterio.warp import transform as rio_transform
except ImportError:  # pragma: no cover - guarded at runtime
    rasterio = None
    rio_rasterize = None
    from_origin = None
    rio_transform = None

try:
    import geopandas as gpd
except ImportError:  # pragma: no cover - guarded at runtime
    gpd = None


class GridBuilder:
    """
    Create the canonical common raster grid used by benchmark/validation outputs.
    """

    def __init__(self, region: list[float] | None = None, resolution: float | None = None):
        spec = get_scoring_grid_spec()

        if region is not None or resolution is not None:
            _region = spec.region if region is None else region
            _resolution = spec.resolution if resolution is None else resolution
            spec = type(spec)(
                min_x=float(_region[0]),
                max_x=float(_region[1]),
                min_y=float(_region[2]),
                max_y=float(_region[3]),
                resolution=float(_resolution),
                crs=spec.crs,
                x_name=spec.x_name,
                y_name=spec.y_name,
                units=spec.units,
                workflow_mode=spec.workflow_mode,
                run_name=spec.run_name,
                display_bounds_wgs84=spec.display_bounds_wgs84,
                buffer_m=spec.buffer_m,
                grid_snap_m=spec.grid_snap_m,
                metadata_path=spec.metadata_path,
                metadata_json_path=spec.metadata_json_path,
                template_path=spec.template_path,
                extent_path=spec.extent_path,
                land_mask_path=spec.land_mask_path,
                sea_mask_path=spec.sea_mask_path,
                shoreline_mask_status=spec.shoreline_mask_status,
                source_paths=spec.source_paths,
            )

        self.spec = spec
        self.min_x = spec.min_x
        self.max_x = spec.max_x
        self.min_y = spec.min_y
        self.max_y = spec.max_y
        self.min_lon = spec.min_lon
        self.max_lon = spec.max_lon
        self.min_lat = spec.min_lat
        self.max_lat = spec.max_lat
        self.resolution = spec.resolution
        self.width = spec.width
        self.height = spec.height
        self.crs = spec.crs
        self.is_projected = spec.is_projected
        self.x_name = spec.x_name
        self.y_name = spec.y_name
        self.units = spec.units
        self.display_bounds_wgs84 = spec.display_bounds_wgs84
        self.region = spec.region
        self.x_bins = spec.x_bins
        self.y_bins = spec.y_bins
        self.x_centers = spec.x_centers
        self.y_centers = spec.y_centers
        self.lon_bins = spec.lon_bins
        self.lat_bins = spec.lat_bins

        if from_origin is None:
            raise ImportError("rasterio is required to build raster transforms")
        self.transform = from_origin(self.min_x, self.max_y, self.resolution, self.resolution)

    def save_metadata(self, out_path: Path):
        meta = self.spec.to_metadata()
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(meta, f, indent=2)
        return meta


def _reproject_gdf_to_grid(gdf, grid: GridBuilder):
    if gdf is None:
        return gdf
    if gpd is None:
        raise ImportError("geopandas is required to reproject geometry layers")
    if gdf.crs is None:
        gdf = gdf.set_crs(GEOGRAPHIC_CRS)
    if str(gdf.crs) != str(grid.crs):
        gdf = gdf.to_crs(grid.crs)
    return gdf


def rasterize_polygon_gdf(gdf, grid: GridBuilder, burn_value: float = 1.0) -> np.ndarray:
    if rasterio is None or rio_rasterize is None:
        raise ImportError("rasterio is required to rasterize polygons")
    if gdf is None or gdf.empty:
        return np.zeros((grid.height, grid.width), dtype=np.float32)

    aligned = _reproject_gdf_to_grid(gdf, grid)
    valid = aligned.dropna(subset=["geometry"])
    if valid.empty:
        return np.zeros((grid.height, grid.width), dtype=np.float32)

    shapes = ((geom, burn_value) for geom in valid.geometry if geom is not None and not geom.is_empty)
    arr = rio_rasterize(
        shapes,
        out_shape=(grid.height, grid.width),
        transform=grid.transform,
        fill=0,
        dtype=np.float32,
        all_touched=True,
    )
    return arr.astype(np.float32)


def save_mask_raster(grid: GridBuilder, data: np.ndarray, out_path: Path):
    if rasterio is None:
        raise ImportError("rasterio is required to save TIFFs")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with rasterio.open(
        out_path,
        "w",
        driver="GTiff",
        height=grid.height,
        width=grid.width,
        count=1,
        dtype=data.dtype,
        crs=grid.crs,
        transform=grid.transform,
        compress="lzw",
    ) as dst:
        dst.write(data, 1)


def rasterize_observation_layer(gdf, grid: GridBuilder, out_path: Path | None = None):
    mask = rasterize_polygon_gdf(gdf, grid)
    if out_path is not None:
        save_mask_raster(grid, mask, out_path)
    return mask


def normalize_time_index(values) -> pd.DatetimeIndex:
    """Normalize datetime-like arrays to UTC-naive pandas timestamps."""
    index = pd.DatetimeIndex(pd.to_datetime(values))
    if index.tz is not None:
        index = index.tz_convert("UTC").tz_localize(None)
    return index


def extract_particles_at_time(
    nc_path: Path,
    target_time,
    model_type: str,
    allow_uniform_mass_fallback: bool = False,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, pd.Timestamp, dict]:
    """
    Extract particle lon/lat coordinates and mass nearest a target timestamp.
    """
    target_ts = pd.Timestamp(target_time)
    if target_ts.tzinfo is not None:
        target_ts = target_ts.tz_convert("UTC").tz_localize(None)

    if model_type == "opendrift":
        with xr.open_dataset(nc_path) as ds:
            times = normalize_time_index(ds.time.values)
            t_idx = int(np.abs(times - target_ts).argmin())
            actual_time = pd.Timestamp(times[t_idx])

            lon = np.asarray(ds["lon"].isel(time=t_idx).values).reshape(-1)
            lat = np.asarray(ds["lat"].isel(time=t_idx).values).reshape(-1)
            status = np.asarray(ds["status"].isel(time=t_idx).values).reshape(-1)
            if "mass_oil" in ds:
                mass = np.asarray(ds["mass_oil"].isel(time=t_idx).values).reshape(-1)
                mass_strategy = "mass_oil"
            else:
                mass = np.ones_like(lon, dtype=np.float32)
                mass_strategy = "uniform_unit_mass"

            valid = (status == 0) & ~np.isnan(lon) & ~np.isnan(lat)
            return lon[valid], lat[valid], mass[valid], actual_time, {"mass_strategy": mass_strategy}

    if model_type == "pygnome":
        import netCDF4

        with netCDF4.Dataset(nc_path) as nc:
            raw_times = netCDF4.num2date(
                nc.variables["time"][:],
                nc.variables["time"].units,
                only_use_cftime_datetimes=False,
                only_use_python_datetimes=True,
            )
            times = normalize_time_index(raw_times)
            t_idx = int(np.abs(times - target_ts).argmin())
            actual_time = pd.Timestamp(times[t_idx])

            particle_counts = np.asarray(nc.variables["particle_count"][:], dtype=int)
            start_idx = int(np.sum(particle_counts[:t_idx]))
            end_idx = start_idx + int(particle_counts[t_idx])

            lon = np.asarray(nc.variables["longitude"][start_idx:end_idx], dtype=float)
            lat = np.asarray(nc.variables["latitude"][start_idx:end_idx], dtype=float)
            status = np.asarray(nc.variables["status_codes"][start_idx:end_idx], dtype=int)
            valid = (status == 2) & ~np.isnan(lon) & ~np.isnan(lat)

            if "mass" in nc.variables:
                mass = np.asarray(nc.variables["mass"][start_idx:end_idx], dtype=float)
                mass_strategy = "mass"
                valid_mass = np.asarray(mass[valid], dtype=float)
                if valid_mass.size > 0 and np.isfinite(valid_mass).all() and float(valid_mass.sum()) > 0.0:
                    return lon[valid], lat[valid], valid_mass, actual_time, {"mass_strategy": mass_strategy}
                if not allow_uniform_mass_fallback:
                    raise ValueError(
                        f"PyGNOME mass export is blank or non-positive at {actual_time.isoformat()} for {nc_path}."
                    )
                mass_strategy = "uniform_unit_mass_fallback"
            else:
                mass_strategy = "uniform_unit_mass_missing_mass_var"
                if not allow_uniform_mass_fallback:
                    raise ValueError(f"PyGNOME mass variable missing in {nc_path}.")

            return (
                lon[valid],
                lat[valid],
                np.ones(np.count_nonzero(valid), dtype=np.float32),
                actual_time,
                {"mass_strategy": mass_strategy},
            )

    raise ValueError(f"Unknown model_type: {model_type}")


def extract_particles_at_hour(
    nc_path: Path,
    target_hours: int,
    model_type: str,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    Extract active particle lon/lat coordinates and mass at a specific target hour.
    """
    if model_type == "opendrift":
        with xr.open_dataset(nc_path) as ds:
            times = normalize_time_index(ds.time.values)
            target_time = times[0] + pd.Timedelta(hours=target_hours)
        lon, lat, mass, _, _ = extract_particles_at_time(nc_path, target_time, model_type)
        return lon, lat, mass

    if model_type == "pygnome":
        import netCDF4

        with netCDF4.Dataset(nc_path) as nc:
            raw_times = netCDF4.num2date(
                nc.variables["time"][:],
                nc.variables["time"].units,
                only_use_cftime_datetimes=False,
                only_use_python_datetimes=True,
            )
            times = normalize_time_index(raw_times)
            target_time = times[0] + pd.Timedelta(hours=target_hours)
        lon, lat, mass, _, _ = extract_particles_at_time(
            nc_path,
            target_time,
            model_type,
            allow_uniform_mass_fallback=False,
        )
        return lon, lat, mass

    raise ValueError(f"Unknown model_type: {model_type}")


def project_points_to_grid(
    grid: GridBuilder,
    lon: np.ndarray,
    lat: np.ndarray,
    source_crs: str = GEOGRAPHIC_CRS,
) -> tuple[np.ndarray, np.ndarray]:
    lon_arr = np.asarray(lon, dtype=float)
    lat_arr = np.asarray(lat, dtype=float)
    if str(grid.crs).upper() == str(source_crs).upper():
        return lon_arr, lat_arr
    if rio_transform is None:
        raise ImportError("rasterio is required to transform coordinates into the scoring grid")
    x_vals, y_vals = rio_transform(source_crs, grid.crs, lon_arr.tolist(), lat_arr.tolist())
    return np.asarray(x_vals, dtype=float), np.asarray(y_vals, dtype=float)


def rasterize_particles(
    grid: GridBuilder,
    lon: np.ndarray,
    lat: np.ndarray,
    mass: np.ndarray,
    source_crs: str = GEOGRAPHIC_CRS,
) -> tuple[np.ndarray, np.ndarray]:
    """
    Convert scattered particles into binary hits and density probability matrices.
    """
    x_vals, y_vals = project_points_to_grid(grid, lon, lat, source_crs=source_crs)
    counts, _, _ = np.histogram2d(y_vals, x_vals, bins=[grid.y_bins, grid.x_bins])
    counts = np.flipud(counts)

    hits = (counts > 0).astype(np.float32)

    mass_grid, _, _ = np.histogram2d(y_vals, x_vals, bins=[grid.y_bins, grid.x_bins], weights=mass)
    mass_grid = np.flipud(mass_grid)

    total_mass = np.sum(mass_grid)
    if total_mass > 0:
        probs = (mass_grid / total_mass).astype(np.float32)
    else:
        probs = mass_grid.astype(np.float32)

    return hits, probs


def save_raster(grid: GridBuilder, data: np.ndarray, out_path: Path):
    """
    Save a 2D numpy array to a GeoTIFF using rasterio.
    """
    if rasterio is None:
        raise ImportError("rasterio is required to save TIFFs")

    out_path.parent.mkdir(parents=True, exist_ok=True)
    with rasterio.open(
        out_path,
        "w",
        driver="GTiff",
        height=grid.height,
        width=grid.width,
        count=1,
        dtype=data.dtype,
        crs=grid.crs,
        transform=grid.transform,
        compress="lzw",
    ) as dst:
        dst.write(data, 1)


def rasterize_model_output(
    grid: GridBuilder,
    nc_path: Path,
    model_type: str,
    out_dir: Path,
    hours: list[int] = [24, 48, 72],
):
    """
    Extract particles for given hours, rasterize them to hits and probabilities,
    save as GeoTIFFs, and return the written artifacts.
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    results = {}

    for hour in hours:
        lon, lat, mass = extract_particles_at_hour(nc_path, hour, model_type)
        hits, probs = rasterize_particles(grid, lon, lat, mass)

        hits_path = out_dir / f"hits_{hour}.tif"
        probs_path = out_dir / f"p_{hour}.tif"

        save_raster(grid, hits, hits_path)
        save_raster(grid, probs, probs_path)

        results[hour] = {
            "hits": hits_path,
            "probs": probs_path,
            "hits_data": hits,
            "probs_data": probs,
        }

    return results
