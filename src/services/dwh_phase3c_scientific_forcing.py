"""DWH Phase 3C scientific forcing-readiness workflow.

This phase prepares real historical forcing candidates for the DWH external
case and validates whether the current repo readers can use them. It never
promotes the earlier analytic smoke fields to scientific forcing.
"""

from __future__ import annotations

import csv
import json
import os
from datetime import timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import xarray as xr
import yaml

from src.core.case_context import get_case_context
from src.helpers.scoring import ScoringGridSpec
from src.services.dwh_phase3c_smoke import (
    CASE_ID,
    CONFIG_PATH,
    SETUP_DIR,
    SMOKE_OUTPUT_DIR,
    _rasterize_gdf,
    _timestamp,
    _timestamp_iso,
    _timestamp_label,
    _write_raster,
    derive_forcing_bbox_from_grid,
    load_dwh_scoring_grid_spec,
)
from src.utils.io import find_current_vars, find_wave_vars, find_wind_vars

try:
    import geopandas as gpd
except ImportError:  # pragma: no cover
    gpd = None


SCIENTIFIC_PHASE = "dwh_phase3c_scientific_forcing_ready"
OUTPUT_DIR = Path("output") / CASE_ID / SCIENTIFIC_PHASE
PREPARED_FORCING_DIR = OUTPUT_DIR / "prepared_forcing"
READER_CHECK_DIR = OUTPUT_DIR / "reader_check"

HYCOM_GLBV31_OPENDAP_URL = "https://tds.hycom.org/thredds/dodsC/GLBv0.08/expt_53.X"
HYCOM_PRODUCT_ID = "GOFS 3.1 reanalysis GLBv0.08 expt_53.X"
ERA5_PRODUCT_ID = "reanalysis-era5-single-levels"
CMEMS_WAVE_PRODUCT_ID = "cmems_mod_glo_wav_my_0.2deg_PT3H-i"
CMEMS_PHY_PRODUCT_ID = "cmems_mod_glo_phy_my_0.083deg_P1D-m"
DOWNLOAD_ENV = "DWH_SCIENTIFIC_FORCING_ATTEMPT_DOWNLOADS"

RECOMMENDATION_READY = "ready for first full scientific DWH Phase 3C run"
RECOMMENDATION_BLOCKED_PREFIX = "not ready: "


def _json_default(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, np.ndarray):
        return value.tolist()
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return None if np.isnan(value) else float(value)
    if isinstance(value, pd.Timestamp):
        return _timestamp_iso(value)
    return str(value)


def _write_json(path: Path, payload: dict | list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, default=_json_default)
        handle.write("\n")


def _write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames: list[str] = []
    for row in rows:
        for key in row:
            if key not in fieldnames:
                fieldnames.append(key)
    with open(path, "w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _load_yaml(path: Path) -> dict:
    with open(path, "r", encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def downloads_enabled_by_default() -> bool:
    value = os.environ.get(DOWNLOAD_ENV, "1").strip().lower()
    return value not in {"0", "false", "no", "off"}


def path_is_smoke_only(path: str | Path | None) -> bool:
    if not path:
        return False
    text = str(path).replace("\\", "/").lower()
    return (
        "non_scientific" in text
        or "smoke" in Path(text).name
        or str(SMOKE_OUTPUT_DIR).replace("\\", "/").lower() in text
    )


def attrs_mark_smoke_only(attrs: dict[str, Any]) -> bool:
    for key in ("non_scientific_smoke", "source_is_smoke_only"):
        value = attrs.get(key)
        if isinstance(value, str) and value.strip().lower() == "true":
            return True
        if value is True:
            return True
    return False


def coverage_spans_window(
    coverage_start: str | pd.Timestamp | None,
    coverage_end: str | pd.Timestamp | None,
    required_start: str | pd.Timestamp,
    required_end: str | pd.Timestamp,
) -> bool:
    if not coverage_start or not coverage_end:
        return False
    return _timestamp(coverage_start) <= _timestamp(required_start) and _timestamp(coverage_end) >= _timestamp(required_end)


def _time_freq_seconds(times: pd.DatetimeIndex) -> float | None:
    if len(times) < 2:
        return None
    diffs = np.diff(times.view("int64")) / 1_000_000_000
    diffs = diffs[diffs > 0]
    if diffs.size == 0:
        return None
    return float(np.median(diffs))


def _coord_name(ds: xr.Dataset, candidates: tuple[str, ...]) -> str | None:
    for name in candidates:
        if name in ds.coords or name in ds.dims:
            return name
    return None


def _normalise_xy_time(ds: xr.Dataset) -> xr.Dataset:
    rename: dict[str, str] = {}
    if "longitude" in ds.coords or "longitude" in ds.dims:
        rename["longitude"] = "lon"
    if "latitude" in ds.coords or "latitude" in ds.dims:
        rename["latitude"] = "lat"
    if "valid_time" in ds.coords or "valid_time" in ds.dims:
        rename["valid_time"] = "time"
    if rename:
        ds = ds.rename(rename)
    if "lat" in ds.coords:
        ds["lat"].attrs.setdefault("units", "degrees_north")
    if "lon" in ds.coords:
        ds["lon"].attrs.setdefault("units", "degrees_east")
    return ds


def _subset_by_bbox(ds: xr.Dataset, bbox: list[float]) -> xr.Dataset:
    min_lon, max_lon, min_lat, max_lat = [float(value) for value in bbox]
    lat_name = _coord_name(ds, ("lat", "latitude"))
    lon_name = _coord_name(ds, ("lon", "longitude"))
    if lat_name is not None:
        lat_values = ds[lat_name]
        if float(lat_values[0]) <= float(lat_values[-1]):
            ds = ds.sel({lat_name: slice(min_lat, max_lat)})
        else:
            ds = ds.sel({lat_name: slice(max_lat, min_lat)})
    if lon_name is not None:
        lon_values = ds[lon_name]
        lon_max = float(lon_values.max())
        if lon_max > 180 and min_lon < 0:
            min_sel = min_lon % 360
            max_sel = max_lon % 360
        else:
            min_sel = min_lon
            max_sel = max_lon
        if min_sel <= max_sel:
            ds = ds.sel({lon_name: slice(min_sel, max_sel)})
    return ds


def _convert_360_lon_to_180(ds: xr.Dataset) -> xr.Dataset:
    lon_name = _coord_name(ds, ("lon", "longitude"))
    if lon_name is None:
        return ds
    lon_values = ds[lon_name]
    if float(lon_values.max()) <= 180:
        return ds
    ds = ds.assign_coords({lon_name: ((lon_values + 180) % 360) - 180})
    return ds.sortby(lon_name)


def _select_surface_depth(ds: xr.Dataset) -> xr.Dataset:
    depth_name = _coord_name(ds, ("depth", "Depth", "depthu", "depthv"))
    if depth_name is None:
        return ds
    return ds.sel({depth_name: 0}, method="nearest")


def _standardize_current_dataset(ds: xr.Dataset) -> xr.Dataset:
    ds = _normalise_xy_time(ds)
    u_var, v_var = find_current_vars(ds)
    ds = ds[[u_var, v_var]].rename({u_var: "x_sea_water_velocity", v_var: "y_sea_water_velocity"})
    ds["x_sea_water_velocity"].attrs.update(
        {
            "units": "m s-1",
            "standard_name": "eastward_sea_water_velocity",
            "long_name": "eastward sea water velocity",
        }
    )
    ds["y_sea_water_velocity"].attrs.update(
        {
            "units": "m s-1",
            "standard_name": "northward_sea_water_velocity",
            "long_name": "northward sea water velocity",
        }
    )
    return ds


def _standardize_wind_dataset(ds: xr.Dataset) -> xr.Dataset:
    ds = _normalise_xy_time(ds)
    u_var, v_var = find_wind_vars(ds)
    ds = ds[[u_var, v_var]].rename({u_var: "x_wind", v_var: "y_wind"})
    ds["x_wind"].attrs.update({"units": "m s-1", "standard_name": "eastward_wind"})
    ds["y_wind"].attrs.update({"units": "m s-1", "standard_name": "northward_wind"})
    return ds


def _standardize_wave_dataset(ds: xr.Dataset) -> xr.Dataset:
    ds = _normalise_xy_time(ds)
    x_var, y_var, h_var = find_wave_vars(ds)
    ds = ds[[x_var, y_var, h_var]].rename(
        {
            x_var: "sea_surface_wave_stokes_drift_x_velocity",
            y_var: "sea_surface_wave_stokes_drift_y_velocity",
            h_var: "sea_surface_wave_significant_height",
        }
    )
    ds["sea_surface_wave_stokes_drift_x_velocity"].attrs.update(
        {"units": "m s-1", "standard_name": "sea_surface_wave_stokes_drift_x_velocity"}
    )
    ds["sea_surface_wave_stokes_drift_y_velocity"].attrs.update(
        {"units": "m s-1", "standard_name": "sea_surface_wave_stokes_drift_y_velocity"}
    )
    ds["sea_surface_wave_significant_height"].attrs.update(
        {"units": "m", "standard_name": "sea_surface_wave_significant_height"}
    )
    return ds


def _write_prepared_dataset(ds: xr.Dataset, path: Path, attrs: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    ds = _normalise_xy_time(ds)
    if "lat" in ds.coords:
        ds = ds.sortby("lat")
    if "lon" in ds.coords:
        ds = ds.sortby("lon")
    ds.attrs.update(
        {
            "non_scientific_smoke": "false",
            "source_is_smoke_only": "false",
            "scientific_ready_candidate": "true",
            **{key: str(value) for key, value in attrs.items()},
        }
    )
    ds.to_netcdf(path)
    return path


def _base_status_row(
    source_role: str,
    provider: str,
    dataset_product_id: str,
    access_method: str,
    local_file_path: str | Path | None,
) -> dict:
    return {
        "case_id": CASE_ID,
        "phase": SCIENTIFIC_PHASE,
        "source_role": source_role,
        "provider": provider,
        "dataset_product_id": dataset_product_id,
        "access_method": access_method,
        "local_file_path": str(local_file_path or ""),
        "variable_names_found": "",
        "actual_start_time_coverage_utc": "",
        "actual_end_time_coverage_utc": "",
        "coverage_spans_required_window": False,
        "reader_compatibility_status": "not_checked",
        "units_time_metadata_usable": False,
        "scientific_ready": False,
        "non_scientific_smoke": False,
        "source_is_smoke_only": False,
        "exact_reason_if_false": "",
    }


def _blocked_status_row(
    source_role: str,
    provider: str,
    dataset_product_id: str,
    access_method: str,
    reason: str,
    local_file_path: str | Path | None = None,
) -> dict:
    row = _base_status_row(source_role, provider, dataset_product_id, access_method, local_file_path)
    row.update(
        {
            "reader_compatibility_status": "blocked_before_file_validation",
            "scientific_ready": False,
            "exact_reason_if_false": reason,
        }
    )
    return row


def validate_prepared_forcing_file(
    path: str | Path,
    forcing_kind: str,
    required_start_utc: str,
    required_end_utc: str,
    base_row: dict,
) -> dict:
    path = Path(path)
    row = dict(base_row)
    row["local_file_path"] = str(path)
    if path_is_smoke_only(path):
        row.update(
            {
                "reader_compatibility_status": "rejected_smoke_path",
                "scientific_ready": False,
                "source_is_smoke_only": True,
                "exact_reason_if_false": "prepared file path points to smoke/non-scientific forcing",
            }
        )
        return row
    if not path.exists():
        row.update(
            {
                "reader_compatibility_status": "missing_file",
                "scientific_ready": False,
                "exact_reason_if_false": f"file does not exist: {path}",
            }
        )
        return row

    try:
        with xr.open_dataset(path) as ds:
            if attrs_mark_smoke_only(dict(ds.attrs)):
                row.update(
                    {
                        "reader_compatibility_status": "rejected_smoke_attrs",
                        "scientific_ready": False,
                        "source_is_smoke_only": True,
                        "exact_reason_if_false": "dataset attributes mark it as smoke-only forcing",
                    }
                )
                return row

            row["variable_names_found"] = ";".join(sorted(ds.data_vars))
            if forcing_kind == "current":
                required_vars = find_current_vars(ds)
            elif forcing_kind == "wind":
                required_vars = find_wind_vars(ds)
            elif forcing_kind == "wave":
                required_vars = find_wave_vars(ds)
            else:
                raise ValueError(f"unsupported forcing kind: {forcing_kind}")
            row["required_variables_found"] = ";".join(required_vars)

            if "time" not in ds.coords and "time" not in ds.dims:
                raise ValueError("no usable time coordinate found")
            times = pd.DatetimeIndex(pd.to_datetime(ds["time"].values))
            if len(times) == 0:
                raise ValueError("time coordinate is empty")
            row["actual_start_time_coverage_utc"] = _timestamp_iso(times.min())
            row["actual_end_time_coverage_utc"] = _timestamp_iso(times.max())
            row["native_time_step_seconds"] = _time_freq_seconds(times)
            row["coverage_spans_required_window"] = coverage_spans_window(
                row["actual_start_time_coverage_utc"],
                row["actual_end_time_coverage_utc"],
                required_start_utc,
                required_end_utc,
            )

            lat_name = _coord_name(ds, ("lat", "latitude"))
            lon_name = _coord_name(ds, ("lon", "longitude"))
            row["units_time_metadata_usable"] = bool(
                lat_name
                and lon_name
                and str(ds[lat_name].attrs.get("units", "")).strip()
                and str(ds[lon_name].attrs.get("units", "")).strip()
            )

        try:
            from opendrift.readers import reader_netCDF_CF_generic

            reader = reader_netCDF_CF_generic.Reader(str(path))
            reader_vars = set(getattr(reader, "variables", []) or [])
            missing = [name for name in row["required_variables_found"].split(";") if name and name not in reader_vars]
            if missing:
                row["reader_compatibility_status"] = "reader_opened_but_required_variables_not_exposed"
                row["exact_reason_if_false"] = f"OpenDrift reader did not expose required variables: {missing}"
            else:
                row["reader_compatibility_status"] = "reader_opened_required_variables_exposed"
        except Exception as exc:
            row["reader_compatibility_status"] = "reader_open_failed"
            row["exact_reason_if_false"] = f"{type(exc).__name__}: {exc}"

        row["scientific_ready"] = bool(
            row["coverage_spans_required_window"]
            and row["units_time_metadata_usable"]
            and row["reader_compatibility_status"] == "reader_opened_required_variables_exposed"
            and not row["source_is_smoke_only"]
        )
        if not row["scientific_ready"] and not row["exact_reason_if_false"]:
            if not row["coverage_spans_required_window"]:
                row["exact_reason_if_false"] = "actual time coverage does not span the required DWH May 20-23 window"
            elif not row["units_time_metadata_usable"]:
                row["exact_reason_if_false"] = "lat/lon/time metadata are not usable by current readers"
            else:
                row["exact_reason_if_false"] = "reader compatibility validation failed"
    except Exception as exc:
        row.update(
            {
                "reader_compatibility_status": "validation_failed",
                "scientific_ready": False,
                "exact_reason_if_false": f"{type(exc).__name__}: {exc}",
            }
        )
    return row


def _acquire_hycom_current(bbox: list[float], required_start: str, request_end: str) -> tuple[Path | None, str]:
    output_path = PREPARED_FORCING_DIR / "hycom_gofs31_current_dwh_20100520_20100524.nc"
    if output_path.exists() and not path_is_smoke_only(output_path):
        return output_path, "reused_existing_real_historical_file"
    try:
        ds = xr.open_dataset(HYCOM_GLBV31_OPENDAP_URL, drop_variables=["tau"], decode_times=True)
        ds = _subset_by_bbox(ds, bbox)
        ds = _select_surface_depth(ds)
        ds = ds.sel(time=slice(_timestamp(required_start), _timestamp(request_end)))
        ds = _convert_360_lon_to_180(ds)
        ds = _standardize_current_dataset(ds)
        ds = ds.load()
        _write_prepared_dataset(
            ds,
            output_path,
            {
                "provider": "HYCOM",
                "dataset_product_id": HYCOM_PRODUCT_ID,
                "source_url": HYCOM_GLBV31_OPENDAP_URL,
            },
        )
        return output_path, "downloaded_from_hycom_thredds_opendap"
    except Exception as exc:
        return None, f"{type(exc).__name__}: {exc}"


def _acquire_era5_wind(bbox: list[float], required_start: str, request_end: str) -> tuple[Path | None, str]:
    output_path = PREPARED_FORCING_DIR / "era5_wind_dwh_20100520_20100524.nc"
    if output_path.exists() and not path_is_smoke_only(output_path):
        return output_path, "reused_existing_real_historical_file"
    try:
        import cdsapi

        min_lon, max_lon, min_lat, max_lat = [float(value) for value in bbox]
        start = _timestamp(required_start)
        end = _timestamp(request_end)
        days = pd.date_range(start.normalize(), end.normalize(), freq="D")
        request = {
            "product_type": "reanalysis",
            "variable": ["10m_u_component_of_wind", "10m_v_component_of_wind"],
            "date": [day.strftime("%Y-%m-%d") for day in days],
            "time": [f"{hour:02d}:00" for hour in range(24)],
            "area": [max_lat, min_lon, min_lat, max_lon],
            "format": "netcdf",
        }
        raw_path = PREPARED_FORCING_DIR / "era5_wind_dwh_raw_20100520_20100524.nc"
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        client_kwargs = {}
        if os.environ.get("CDS_URL") and os.environ.get("CDS_KEY"):
            client_kwargs = {"url": os.environ["CDS_URL"], "key": os.environ["CDS_KEY"]}
        client = cdsapi.Client(**client_kwargs)
        try:
            client.retrieve(ERA5_PRODUCT_ID, request, str(raw_path))
        except Exception:
            request_v2 = dict(request)
            request_v2.pop("format", None)
            request_v2.update({"data_format": "netcdf", "download_format": "unarchived"})
            client.retrieve(ERA5_PRODUCT_ID, request_v2, str(raw_path))
        with xr.open_dataset(raw_path) as raw_ds:
            ds = _standardize_wind_dataset(raw_ds)
            if "time" in ds:
                ds = ds.sel(time=slice(_timestamp(required_start), _timestamp(request_end)))
            ds = ds.load()
        _write_prepared_dataset(
            ds,
            output_path,
            {
                "provider": "ECMWF/Copernicus Climate Data Store",
                "dataset_product_id": ERA5_PRODUCT_ID,
                "source_url": "https://cds.climate.copernicus.eu/datasets/reanalysis-era5-single-levels",
            },
        )
        return output_path, "downloaded_from_cds_api"
    except Exception as exc:
        return None, f"{type(exc).__name__}: {exc}"


def _call_copernicus_subset(**kwargs: Any) -> Any:
    import copernicusmarine

    username = (
        os.environ.get("CMEMS_USERNAME")
        or os.environ.get("COPERNICUSMARINE_SERVICE_USERNAME")
        or os.environ.get("COPERNICUSMARINE_USERNAME")
    )
    password = (
        os.environ.get("CMEMS_PASSWORD")
        or os.environ.get("COPERNICUSMARINE_SERVICE_PASSWORD")
        or os.environ.get("COPERNICUSMARINE_PASSWORD")
    )
    if not username or not password:
        raise RuntimeError(
            "Copernicus Marine credentials are not available in CMEMS_USERNAME/CMEMS_PASSWORD "
            "or COPERNICUSMARINE_SERVICE_USERNAME/COPERNICUSMARINE_SERVICE_PASSWORD"
        )
    kwargs = {**kwargs, "username": username, "password": password}
    try:
        return copernicusmarine.subset(**kwargs, overwrite=True)
    except TypeError:
        return copernicusmarine.subset(**kwargs, force_download=True)


def _acquire_cmems_wave(bbox: list[float], required_start: str, request_end: str) -> tuple[Path | None, str]:
    output_path = PREPARED_FORCING_DIR / "cmems_wave_stokes_dwh_20100520_20100524.nc"
    if output_path.exists() and not path_is_smoke_only(output_path):
        return output_path, "reused_existing_real_historical_file"
    try:
        min_lon, max_lon, min_lat, max_lat = [float(value) for value in bbox]
        raw_filename = "cmems_wave_stokes_dwh_raw_20100520_20100524.nc"
        _call_copernicus_subset(
            dataset_id=CMEMS_WAVE_PRODUCT_ID,
            variables=["VHM0", "VSDX", "VSDY"],
            minimum_longitude=min_lon,
            maximum_longitude=max_lon,
            minimum_latitude=min_lat,
            maximum_latitude=max_lat,
            start_datetime=_timestamp(required_start).strftime("%Y-%m-%dT%H:%M:%S"),
            end_datetime=_timestamp(request_end).strftime("%Y-%m-%dT%H:%M:%S"),
            output_directory=str(PREPARED_FORCING_DIR),
            output_filename=raw_filename,
        )
        raw_path = PREPARED_FORCING_DIR / raw_filename
        with xr.open_dataset(raw_path) as raw_ds:
            ds = _standardize_wave_dataset(raw_ds)
            if "time" in ds:
                ds = ds.sel(time=slice(_timestamp(required_start), _timestamp(request_end)))
            ds = ds.load()
        _write_prepared_dataset(
            ds,
            output_path,
            {
                "provider": "Copernicus Marine",
                "dataset_product_id": CMEMS_WAVE_PRODUCT_ID,
                "source_url": "https://data.marine.copernicus.eu/product/GLOBAL_MULTIYEAR_WAV_001_032/description",
            },
        )
        return output_path, "downloaded_from_copernicusmarine_api"
    except Exception as exc:
        return None, f"{type(exc).__name__}: {exc}"


def _acquire_cmems_current_fallback(bbox: list[float], required_start: str, request_end: str) -> tuple[Path | None, str]:
    output_path = PREPARED_FORCING_DIR / "cmems_physics_current_fallback_dwh_20100520_20100524.nc"
    if output_path.exists() and not path_is_smoke_only(output_path):
        return output_path, "reused_existing_real_historical_file"
    try:
        min_lon, max_lon, min_lat, max_lat = [float(value) for value in bbox]
        raw_filename = "cmems_physics_current_fallback_dwh_raw_20100520_20100524.nc"
        _call_copernicus_subset(
            dataset_id=CMEMS_PHY_PRODUCT_ID,
            variables=["uo", "vo"],
            minimum_longitude=min_lon,
            maximum_longitude=max_lon,
            minimum_latitude=min_lat,
            maximum_latitude=max_lat,
            minimum_depth=0,
            maximum_depth=1,
            start_datetime=_timestamp(required_start).strftime("%Y-%m-%dT%H:%M:%S"),
            end_datetime=_timestamp(request_end).strftime("%Y-%m-%dT%H:%M:%S"),
            output_directory=str(PREPARED_FORCING_DIR),
            output_filename=raw_filename,
        )
        raw_path = PREPARED_FORCING_DIR / raw_filename
        with xr.open_dataset(raw_path) as raw_ds:
            ds = _select_surface_depth(raw_ds)
            ds = _standardize_current_dataset(ds)
            if "time" in ds:
                ds = ds.sel(time=slice(_timestamp(required_start), _timestamp(request_end)))
            ds = ds.load()
        _write_prepared_dataset(
            ds,
            output_path,
            {
                "provider": "Copernicus Marine",
                "dataset_product_id": CMEMS_PHY_PRODUCT_ID,
                "source_url": "https://data.marine.copernicus.eu/product/GLOBAL_MULTIYEAR_PHY_001_030/description",
            },
        )
        return output_path, "downloaded_from_copernicusmarine_api"
    except Exception as exc:
        return None, f"{type(exc).__name__}: {exc}"


def _prepare_forcing_sources(
    bbox: list[float],
    required_start_utc: str,
    required_end_utc: str,
    request_end_utc: str,
) -> list[dict]:
    rows: list[dict] = []
    downloads_enabled = downloads_enabled_by_default()

    attempts = [
        ("current", "HYCOM", HYCOM_PRODUCT_ID, "THREDDS/OPeNDAP", "current", _acquire_hycom_current),
        (
            "wind",
            "ECMWF/Copernicus Climate Data Store",
            ERA5_PRODUCT_ID,
            "CDS API / file download",
            "wind",
            _acquire_era5_wind,
        ),
        (
            "wave",
            "Copernicus Marine",
            CMEMS_WAVE_PRODUCT_ID,
            "Copernicus Marine API / file download",
            "wave",
            _acquire_cmems_wave,
        ),
    ]

    for source_role, provider, product_id, access_method, forcing_kind, acquire in attempts:
        base = _base_status_row(source_role, provider, product_id, access_method, "")
        if not downloads_enabled:
            rows.append(
                _blocked_status_row(
                    source_role,
                    provider,
                    product_id,
                    access_method,
                    f"download attempts disabled by {DOWNLOAD_ENV}=0",
                )
            )
            continue

        path, note = acquire(bbox, required_start_utc, request_end_utc)
        if path is None:
            rows.append(_blocked_status_row(source_role, provider, product_id, access_method, note))
            continue
        row = validate_prepared_forcing_file(path, forcing_kind, required_start_utc, required_end_utc, base)
        row["acquisition_status"] = note
        rows.append(row)

    current_ready = any(row["source_role"] == "current" and row["scientific_ready"] for row in rows)
    if not current_ready and downloads_enabled:
        provider = "Copernicus Marine"
        access_method = "Copernicus Marine API / file download"
        base = _base_status_row("current_fallback", provider, CMEMS_PHY_PRODUCT_ID, access_method, "")
        path, note = _acquire_cmems_current_fallback(bbox, required_start_utc, request_end_utc)
        if path is None:
            rows.append(_blocked_status_row("current_fallback", provider, CMEMS_PHY_PRODUCT_ID, access_method, note))
        else:
            row = validate_prepared_forcing_file(path, "current", required_start_utc, required_end_utc, base)
            row["acquisition_status"] = note
            rows.append(row)
    elif current_ready:
        rows.append(
            _blocked_status_row(
                "current_fallback",
                "Copernicus Marine",
                CMEMS_PHY_PRODUCT_ID,
                "Copernicus Marine API / file download",
                "not attempted because primary HYCOM current source is scientific-ready",
            )
        )
    return rows


def _select_stack(rows: list[dict]) -> dict[str, dict | None]:
    current = next((row for row in rows if row["source_role"] == "current" and row["scientific_ready"]), None)
    if current is None:
        current = next((row for row in rows if row["source_role"] == "current_fallback" and row["scientific_ready"]), None)
    wind = next((row for row in rows if row["source_role"] == "wind" and row["scientific_ready"]), None)
    wave = next((row for row in rows if row["source_role"] == "wave" and row["scientific_ready"]), None)
    return {"current": current, "wind": wind, "wave": wave}


def _load_seed_point() -> tuple[float, float]:
    init_path = SETUP_DIR / "processed" / "layer_05_dwh_t20100520_composite_processed.gpkg"
    if gpd is None:
        raise ImportError("geopandas is required for the DWH reader-check seed point")
    gdf = gpd.read_file(init_path)
    if gdf.empty:
        raise ValueError(f"initialization polygon is empty: {init_path}")
    if gdf.crs is not None and str(gdf.crs).upper() != "EPSG:4326":
        gdf = gdf.to_crs("EPSG:4326")
    point = gdf.unary_union.representative_point()
    return float(point.y), float(point.x)


def _write_reader_check_raster(
    spec: ScoringGridSpec,
    latitudes: np.ndarray,
    longitudes: np.ndarray,
    timestamp: str,
) -> Path:
    if gpd is None:
        raise ImportError("geopandas is required for reader-check raster writing")
    if len(latitudes) == 0 or len(longitudes) == 0:
        raise ValueError("reader-check produced no particle positions")
    gdf = gpd.GeoDataFrame(
        {"value": np.ones(len(latitudes), dtype=np.uint8)},
        geometry=gpd.points_from_xy(longitudes, latitudes),
        crs="EPSG:4326",
    ).to_crs(spec.crs)
    buffered = gdf.buffer(float(spec.resolution) * 0.5)
    mask = _rasterize_gdf(gpd.GeoDataFrame(geometry=buffered, crs=spec.crs), spec)
    path = READER_CHECK_DIR / f"reader_check_control_footprint_mask_{_timestamp_label(timestamp)}.tif"
    _write_raster(spec, mask.astype(np.uint8), path, dtype="uint8")
    return path


def run_scientific_reader_check(
    stack: dict[str, dict],
    spec: ScoringGridSpec,
    required_start_utc: str,
    required_end_utc: str,
) -> dict:
    manifest = {
        "case_id": CASE_ID,
        "phase": SCIENTIFIC_PHASE,
        "scientific_forcing_compatibility_check": True,
        "final_phase3c_scientific_result_table": False,
        "non_scientific_smoke": False,
        "currents_attached": False,
        "winds_attached": False,
        "waves_attached": False,
        "reader_check_run_succeeded": False,
        "reader_check_stop_reason": "",
        "canonical_rasters": [],
    }
    loading_rows: list[dict] = []

    try:
        from opendrift.models.oceandrift import OceanDrift
        from opendrift.readers import reader_netCDF_CF_generic

        model = OceanDrift(loglevel=30)
        readers = []
        for role in ("current", "wind", "wave"):
            row = stack[role]
            reader = reader_netCDF_CF_generic.Reader(row["local_file_path"])
            readers.append(reader)
            if role == "current":
                manifest["currents_attached"] = True
            elif role == "wind":
                manifest["winds_attached"] = True
            else:
                manifest["waves_attached"] = True
            loading_rows.append(
                {
                    "source_role": role,
                    "file_path": row["local_file_path"],
                    "reader_attached": True,
                    "scientific_ready": True,
                    "non_scientific_smoke": False,
                    "source_is_smoke_only": False,
                }
            )

        model.add_reader(readers)
        lat, lon = _load_seed_point()
        start = _timestamp(required_start_utc)
        end = _timestamp(required_end_utc)
        element_count = 10
        model.seed_elements(lon=lon, lat=lat, number=element_count, time=start)
        outfile = READER_CHECK_DIR / "dwh_scientific_reader_check_trajectory.nc"
        READER_CHECK_DIR.mkdir(parents=True, exist_ok=True)
        model.run(
            time_step=timedelta(hours=1),
            time_step_output=timedelta(hours=1),
            end_time=end,
            outfile=str(outfile),
        )

        with xr.open_dataset(outfile) as ds:
            lat_var = "lat" if "lat" in ds else "latitude"
            lon_var = "lon" if "lon" in ds else "longitude"
            final_lats = np.asarray(ds[lat_var].isel(time=-1).values).ravel()
            final_lons = np.asarray(ds[lon_var].isel(time=-1).values).ravel()
        finite = np.isfinite(final_lats) & np.isfinite(final_lons)
        raster_path = _write_reader_check_raster(spec, final_lats[finite], final_lons[finite], required_end_utc)
        manifest.update(
            {
                "reader_check_run_succeeded": True,
                "trajectory_file": str(outfile),
                "seed_point_lat": lat,
                "seed_point_lon": lon,
                "element_count": element_count,
                "simulation_start_utc": _timestamp_iso(start),
                "simulation_end_utc": _timestamp_iso(end),
                "canonical_rasters": [str(raster_path)],
            }
        )
    except Exception as exc:
        manifest["reader_check_stop_reason"] = f"{type(exc).__name__}: {exc}"
        if not loading_rows:
            loading_rows.append(
                {
                    "source_role": "stack",
                    "reader_attached": False,
                    "scientific_ready": False,
                    "non_scientific_smoke": False,
                    "source_is_smoke_only": False,
                    "stop_reason": manifest["reader_check_stop_reason"],
                }
            )

    return {"manifest": manifest, "loading_rows": loading_rows}


def _build_prepared_manifest_rows(status_rows: list[dict], bbox: list[float], required_start: str, required_end: str) -> list[dict]:
    rows = []
    for row in status_rows:
        if not row.get("local_file_path"):
            continue
        rows.append(
            {
                **row,
                "forcing_bbox_min_lon": bbox[0],
                "forcing_bbox_max_lon": bbox[1],
                "forcing_bbox_min_lat": bbox[2],
                "forcing_bbox_max_lat": bbox[3],
                "required_start_utc": required_start,
                "required_end_utc": required_end,
            }
        )
    return rows


def _biggest_blocker(status_rows: list[dict], stack: dict[str, dict | None]) -> str:
    if all(stack.values()):
        return ""
    priority = ["current", "wind", "wave", "current_fallback"]
    for role in priority:
        row = next((item for item in status_rows if item["source_role"] == role and not item["scientific_ready"]), None)
        if row:
            return f"{role} forcing not scientific-ready - {row.get('exact_reason_if_false') or row.get('reader_compatibility_status')}"
    return "scientific forcing stack is incomplete"


def _phase_recommendation(status_rows: list[dict], stack: dict[str, dict | None], reader_manifest: dict) -> tuple[str, str]:
    forcing_blocker = _biggest_blocker(status_rows, stack)
    if forcing_blocker:
        return f"{RECOMMENDATION_BLOCKED_PREFIX}{forcing_blocker}", forcing_blocker
    if not reader_manifest.get("reader_check_run_succeeded"):
        blocker = (
            "scientific reader-check forecast did not complete - "
            f"{reader_manifest.get('reader_check_stop_reason') or 'unknown stop reason'}"
        )
        return f"{RECOMMENDATION_BLOCKED_PREFIX}{blocker}", blocker
    return RECOMMENDATION_READY, ""


def run_dwh_phase3c_scientific_forcing_ready() -> dict:
    case = get_case_context()
    if case.workflow_mode != "dwh_retro_2010":
        raise RuntimeError("DWH scientific forcing readiness requires WORKFLOW_MODE=dwh_retro_2010")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    PREPARED_FORCING_DIR.mkdir(parents=True, exist_ok=True)
    config = _load_yaml(CONFIG_PATH)
    spec = load_dwh_scoring_grid_spec()
    halo = float(config.get("forcing_bbox_halo_degrees", 0.5))
    bbox = derive_forcing_bbox_from_grid(spec, halo)

    required_start = str(config.get("forcing_start_utc", "2010-05-20T00:00:00Z"))
    required_end = str(config.get("forcing_end_utc", "2010-05-23T23:59:59Z"))
    request_end = (_timestamp(required_end) + timedelta(seconds=1)).strftime("%Y-%m-%dT%H:%M:%SZ")

    status_rows = _prepare_forcing_sources(bbox, required_start, required_end, request_end)
    stack = _select_stack(status_rows)

    reader_check = {
        "manifest": {
            "case_id": CASE_ID,
            "phase": SCIENTIFIC_PHASE,
            "scientific_forcing_compatibility_check": True,
            "final_phase3c_scientific_result_table": False,
            "non_scientific_smoke": False,
            "reader_check_run_succeeded": False,
            "reader_check_stop_reason": "scientific forcing stack incomplete; reader-check forecast not run",
            "currents_attached": False,
            "winds_attached": False,
            "waves_attached": False,
        },
        "loading_rows": [
            {
                "source_role": "stack",
                "reader_attached": False,
                "scientific_ready": False,
                "non_scientific_smoke": False,
                "source_is_smoke_only": False,
                "stop_reason": "scientific forcing stack incomplete; reader-check forecast not run",
            }
        ],
    }
    if all(stack.values()):
        reader_check = run_scientific_reader_check(stack, spec, required_start, required_end)

    status_csv = OUTPUT_DIR / "dwh_scientific_forcing_status.csv"
    status_json = OUTPUT_DIR / "dwh_scientific_forcing_status.json"
    manifest_csv = OUTPUT_DIR / "dwh_scientific_prepared_forcing_manifest.csv"
    manifest_json = OUTPUT_DIR / "dwh_scientific_prepared_forcing_manifest.json"
    reader_manifest_json = OUTPUT_DIR / "dwh_scientific_reader_check_manifest.json"
    loading_csv = OUTPUT_DIR / "dwh_scientific_loading_audit.csv"
    loading_json = OUTPUT_DIR / "dwh_scientific_loading_audit.json"
    reader_report_md = OUTPUT_DIR / "dwh_scientific_reader_check_report.md"

    prepared_rows = _build_prepared_manifest_rows(status_rows, bbox, required_start, required_end)
    _write_csv(status_csv, status_rows)
    _write_json(status_json, status_rows)
    _write_csv(manifest_csv, prepared_rows)
    _write_json(manifest_json, prepared_rows)
    _write_json(reader_manifest_json, reader_check["manifest"])
    _write_csv(loading_csv, reader_check["loading_rows"])
    _write_json(loading_json, reader_check["loading_rows"])

    recommendation, blocker = _phase_recommendation(status_rows, stack, reader_check["manifest"])

    report_lines = [
        "# DWH Phase 3C Scientific Forcing Reader Check",
        "",
        f"- case_id: {CASE_ID}",
        f"- phase: {SCIENTIFIC_PHASE}",
        "- thesis_role: external_transfer_validation_support_case",
        "- main_case_note: Mindoro remains the main Philippine thesis case; DWH is a separate external transfer-validation story.",
        "- non_scientific_smoke: false",
        f"- required_window_utc: {required_start} to {required_end}",
        f"- forcing_bbox_with_halo: {bbox}",
        f"- selected_current_source: {stack['current']['dataset_product_id'] if stack['current'] else 'none scientific-ready'}",
        f"- selected_wind_source: {stack['wind']['dataset_product_id'] if stack['wind'] else 'none scientific-ready'}",
        f"- selected_wave_source: {stack['wave']['dataset_product_id'] if stack['wave'] else 'none scientific-ready'}",
        f"- reader_check_run_succeeded: {reader_check['manifest'].get('reader_check_run_succeeded', False)}",
        f"- recommendation: {recommendation}",
        "",
        "Selection policy: DWH Phase 3C freezes the first complete real historical current+wind+wave stack that passes the scientific-readiness gate; it does not use Phase 1 drifter-selected baseline logic.",
        "",
        "Current frozen DWH stack in this repo state: HYCOM GOFS 3.1 currents + ERA5 winds + CMEMS wave/Stokes.",
        "",
        "Scientific-ready means each selected source is not smoke-only, spans the required May 20-23, 2010 window, exposes the required variables with usable metadata, opens cleanly in the OpenDrift reader, and the assembled stack completes the small reader-check forecast.",
        "",
        "Observed DWH daily masks remain truth later in Phase 3C, and PyGNOME remains comparator-only in the cross-model branch.",
        "",
        "This is a forcing-reader compatibility check only. It is not the final Phase 3C scientific result table.",
    ]
    reader_report_md.write_text("\n".join(report_lines) + "\n", encoding="utf-8")

    selected = {
        "current": stack["current"] or next((row for row in status_rows if row["source_role"] == "current"), {}),
        "wind": stack["wind"] or next((row for row in status_rows if row["source_role"] == "wind"), {}),
        "wave": stack["wave"] or next((row for row in status_rows if row["source_role"] == "wave"), {}),
    }

    return {
        "output_dir": str(OUTPUT_DIR),
        "status_csv": str(status_csv),
        "status_json": str(status_json),
        "prepared_forcing_manifest_csv": str(manifest_csv),
        "prepared_forcing_manifest_json": str(manifest_json),
        "reader_check_report": str(reader_report_md),
        "reader_check_manifest": str(reader_manifest_json),
        "loading_audit_csv": str(loading_csv),
        "loading_audit_json": str(loading_json),
        "forcing_bbox": bbox,
        "required_start_utc": required_start,
        "required_end_utc": required_end,
        "selected_sources": selected,
        "status_rows": status_rows,
        "reader_check_run_succeeded": bool(reader_check["manifest"].get("reader_check_run_succeeded", False)),
        "waves_attached": bool(reader_check["manifest"].get("waves_attached", False)),
        "currents_attached": bool(reader_check["manifest"].get("currents_attached", False)),
        "winds_attached": bool(reader_check["manifest"].get("winds_attached", False)),
        "recommendation": recommendation,
        "biggest_blocker": blocker or "",
    }


__all__ = [
    "SCIENTIFIC_PHASE",
    "attrs_mark_smoke_only",
    "coverage_spans_window",
    "path_is_smoke_only",
    "run_dwh_phase3c_scientific_forcing_ready",
    "validate_prepared_forcing_file",
]
