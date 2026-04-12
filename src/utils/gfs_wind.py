from __future__ import annotations

import importlib.util
import logging
import multiprocessing
import queue as queue_module
import re
import tempfile
import time
import traceback
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import requests
import xarray as xr

logger = logging.getLogger(__name__)

GFS_OLD_BASE = "https://www.ncei.noaa.gov/thredds/dodsC/model-gfs-g4-anl-files-old"
GFS_CURRENT_BASE = "https://www.ncei.noaa.gov/thredds/dodsC/model-gfs-g4-anl-files"
GFS_FILE_PATTERNS = (
    re.compile(r"gfsanl_4_(\d{8})_(\d{4})_(\d{3})\.grb2$"),
    re.compile(r"gfs_4_(\d{8})_(\d{4})_(\d{3})\.grb2$"),
)
GFS_CATALOG_TIMEOUT_SECONDS = 45
GFS_CATALOG_MAX_ATTEMPTS = 2
GFS_HTTP_FALLBACK_TIMEOUT_SECONDS = 75
GFS_HTTP_FALLBACK_MAX_ATTEMPTS = 1
GFS_OPENDAP_ATTEMPT_TIMEOUT_SECONDS = 45
GFS_MIN_ATTEMPT_SECONDS = 5


class GFSAcquisitionError(RuntimeError):
    def __init__(self, message: str, *, failure_stage: str) -> None:
        self.failure_stage = str(failure_stage)
        super().__init__(message)


def _relative(repo_root: Path, path: Path) -> str:
    try:
        return str(path.resolve().relative_to(repo_root.resolve())).replace("\\", "/")
    except Exception:
        return str(path).replace("\\", "/")


def _normalize_utc_timestamp(value: pd.Timestamp | str) -> pd.Timestamp:
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        return timestamp.tz_localize("UTC")
    return timestamp.tz_convert("UTC")


def _describe_remote_error(exc: Exception) -> str:
    if isinstance(exc, requests.exceptions.ReadTimeout):
        return "timeout"
    if isinstance(exc, requests.exceptions.ConnectTimeout):
        return "connect timeout"
    if isinstance(exc, requests.exceptions.Timeout):
        return "timeout"
    if isinstance(exc, requests.exceptions.ConnectionError):
        return "connection failed"
    if isinstance(exc, requests.exceptions.HTTPError):
        response = getattr(exc, "response", None)
        if response is not None:
            return f"HTTP {response.status_code}"
        return "HTTP error"
    return str(exc)


def _transport_mode_label(mode_name: str) -> str:
    return {
        "http_cfgrib_fallback": "direct file access",
        "opendap": "OPeNDAP",
    }.get(str(mode_name), str(mode_name))


def _catalog_day_label(value: pd.Timestamp | str) -> str:
    return _normalize_utc_timestamp(value).strftime("%Y-%m-%d")


def _analysis_label(value: pd.Timestamp | str) -> str:
    return _normalize_utc_timestamp(value).strftime("%Y-%m-%d %H:%M UTC")


def _remaining_budget_seconds(deadline_monotonic: float | None) -> float | None:
    if deadline_monotonic is None:
        return None
    return max(0.0, float(deadline_monotonic) - time.monotonic())


def _require_remaining_budget(
    deadline_monotonic: float | None,
    *,
    minimum_seconds: float,
    failure_stage: str,
    stage_label: str,
) -> float | None:
    remaining = _remaining_budget_seconds(deadline_monotonic)
    if remaining is None:
        return None
    if remaining < float(minimum_seconds):
        raise GFSAcquisitionError(
            (
                f"GFS fail-fast budget exhausted before {stage_label}; "
                f"{remaining:.1f}s remained and at least {minimum_seconds:.1f}s was required."
            ),
            failure_stage=failure_stage,
        )
    return remaining


def _capped_timeout(
    deadline_monotonic: float | None,
    *,
    preferred_seconds: int,
    minimum_seconds: int = GFS_MIN_ATTEMPT_SECONDS,
) -> int:
    remaining = _remaining_budget_seconds(deadline_monotonic)
    if remaining is None:
        return int(preferred_seconds)
    return max(int(minimum_seconds), min(int(preferred_seconds), int(max(minimum_seconds, remaining))))


def _opendap_subset_child(
    result_queue,
    *,
    url: str,
    output_path: str,
) -> None:
    try:
        with xr.open_dataset(url) as remote:
            subset = remote[
                [
                    "u-component_of_wind_height_above_ground",
                    "v-component_of_wind_height_above_ground",
                ]
            ].load()
        subset.to_netcdf(output_path)
        result_queue.put({"ok": True})
    except BaseException as exc:  # pragma: no cover - subprocess path
        result_queue.put(
            {
                "ok": False,
                "error": str(exc),
                "traceback": traceback.format_exc(),
            }
        )


def apply_wind_cf_metadata(ds: xr.Dataset) -> xr.Dataset:
    normalized = ds.copy()
    for coord_name, standard_name, units in (
        ("lat", "latitude", "degrees_north"),
        ("latitude", "latitude", "degrees_north"),
        ("lon", "longitude", "degrees_east"),
        ("longitude", "longitude", "degrees_east"),
    ):
        if coord_name in normalized.coords:
            attrs = dict(normalized.coords[coord_name].attrs)
            attrs["standard_name"] = standard_name
            attrs["units"] = units
            normalized.coords[coord_name].attrs = attrs

    if "x_wind" in normalized.data_vars:
        attrs = dict(normalized["x_wind"].attrs)
        attrs["standard_name"] = "eastward_wind"
        attrs["long_name"] = attrs.get("long_name") or "10 metre eastward wind"
        attrs["units"] = "m/s"
        normalized["x_wind"].attrs = attrs
    if "y_wind" in normalized.data_vars:
        attrs = dict(normalized["y_wind"].attrs)
        attrs["standard_name"] = "northward_wind"
        attrs["long_name"] = attrs.get("long_name") or "10 metre northward wind"
        attrs["units"] = "m/s"
        normalized["y_wind"].attrs = attrs

    normalized.attrs["Conventions"] = str(normalized.attrs.get("Conventions") or "CF-1.8")
    return normalized


def wind_cache_has_reader_metadata(ds: xr.Dataset) -> bool:
    return bool(
        "x_wind" in ds.data_vars
        and "y_wind" in ds.data_vars
        and ds["x_wind"].attrs.get("standard_name") == "eastward_wind"
        and ds["y_wind"].attrs.get("standard_name") == "northward_wind"
    )


class GFSWindDownloader:
    def __init__(
        self,
        *,
        forcing_box: list[float],
        repo_root: str | Path | None = None,
        expected_delta: pd.Timedelta | None = None,
    ):
        self.forcing_box = [float(value) for value in forcing_box]
        self.repo_root = Path(repo_root or Path.cwd())
        self.expected_delta = expected_delta or pd.Timedelta(hours=6)

    def ensure_reader_metadata(self, output_path: str | Path) -> None:
        path = Path(output_path)
        if not path.exists():
            return
        with xr.open_dataset(path) as raw:
            loaded = raw.load()
        if wind_cache_has_reader_metadata(loaded):
            return
        repaired = apply_wind_cf_metadata(loaded)
        temp_path = path.with_suffix(f"{path.suffix}.tmp")
        repaired.to_netcdf(temp_path)
        temp_path.replace(path)

    def gfs_dataset_base(self, timestamp: pd.Timestamp | str) -> str:
        month_value = int(_normalize_utc_timestamp(timestamp).strftime("%Y%m"))
        return GFS_OLD_BASE if month_value < 202006 else GFS_CURRENT_BASE

    def gfs_catalog_url_for_day(self, timestamp: pd.Timestamp | str) -> str:
        day = _normalize_utc_timestamp(timestamp)
        base = self.gfs_dataset_base(day).replace("/dodsC/", "/catalog/")
        return f"{base}/{day.strftime('%Y%m')}/{day.strftime('%Y%m%d')}/catalog.xml"

    def parse_gfs_analysis_timestamp(self, dataset_name: str) -> pd.Timestamp | None:
        for pattern in GFS_FILE_PATTERNS:
            match = pattern.match(str(dataset_name))
            if match:
                init_time = pd.Timestamp(f"{match.group(1)}T{match.group(2)}00Z")
                lead_hours = int(match.group(3))
                return init_time + pd.Timedelta(hours=lead_hours)
        return None

    def parse_gfs_catalog(self, xml_text: str) -> dict[pd.Timestamp, str]:
        namespace = {"t": "http://www.unidata.ucar.edu/namespaces/thredds/InvCatalog/v1.0"}
        root = ET.fromstring(xml_text)
        analysis_urls: dict[pd.Timestamp, tuple[int, str]] = {}
        for dataset in root.findall(".//t:dataset", namespace):
            name = str(dataset.attrib.get("name") or "")
            url_path = str(dataset.attrib.get("urlPath") or "")
            timestamp = self.parse_gfs_analysis_timestamp(name)
            if timestamp is None or not url_path:
                continue
            lead_match = next((pattern.match(name) for pattern in GFS_FILE_PATTERNS if pattern.match(name)), None)
            lead_hours = int(lead_match.group(3)) if lead_match else 999
            candidate_url = f"https://www.ncei.noaa.gov/thredds/dodsC/{url_path}"
            current = analysis_urls.get(timestamp)
            if current is None or lead_hours < current[0]:
                analysis_urls[timestamp] = (lead_hours, candidate_url)
        return {timestamp: url for timestamp, (_, url) in analysis_urls.items()}

    def fallback_analysis_urls(
        self,
        start_time: pd.Timestamp | str,
        end_time: pd.Timestamp | str,
    ) -> list[tuple[pd.Timestamp, str]]:
        start_utc = _normalize_utc_timestamp(start_time)
        end_utc = _normalize_utc_timestamp(end_time)
        freq_hours = max(int(round(self.expected_delta.total_seconds() / 3600.0)), 1)
        freq_label = f"{freq_hours}h"
        lower_bound = (start_utc - self.expected_delta).floor(freq_label)
        upper_bound = (end_utc + self.expected_delta).ceil(freq_label)
        timestamps = pd.date_range(start=lower_bound, end=upper_bound, freq=freq_label, tz="UTC")

        fallback_urls: list[tuple[pd.Timestamp, str]] = []
        for timestamp in timestamps:
            base = self.gfs_dataset_base(timestamp)
            if base == GFS_OLD_BASE:
                filename = f"gfsanl_4_{timestamp.strftime('%Y%m%d')}_{timestamp.strftime('%H')}00_000.grb2"
            else:
                filename = f"gfs_4_{timestamp.strftime('%Y%m%d')}_{timestamp.strftime('%H')}00_000.grb2"
            url = f"{base}/{timestamp.strftime('%Y%m')}/{timestamp.strftime('%Y%m%d')}/{filename}"
            fallback_urls.append((timestamp, url))
        return fallback_urls

    def discover_gfs_analysis_urls(
        self,
        start_time: pd.Timestamp | str,
        end_time: pd.Timestamp | str,
        *,
        deadline_monotonic: float | None = None,
    ) -> list[tuple[pd.Timestamp, str]]:
        start_utc = _normalize_utc_timestamp(start_time)
        end_utc = _normalize_utc_timestamp(end_time)
        analysis_urls: dict[pd.Timestamp, str] = {}
        day_range = pd.date_range(
            start=(start_utc - self.expected_delta).floor("D"),
            end=(end_utc + self.expected_delta).ceil("D"),
            freq="D",
            tz="UTC",
        )

        for day in day_range:
            catalog_url = self.gfs_catalog_url_for_day(day)
            response = None
            last_error: Exception | None = None
            for attempt in range(1, GFS_CATALOG_MAX_ATTEMPTS + 1):
                try:
                    _require_remaining_budget(
                        deadline_monotonic,
                        minimum_seconds=GFS_MIN_ATTEMPT_SECONDS,
                        failure_stage="catalog_request",
                        stage_label="the next GFS catalog request",
                    )
                    response = requests.get(
                        catalog_url,
                        timeout=_capped_timeout(
                            deadline_monotonic,
                            preferred_seconds=GFS_CATALOG_TIMEOUT_SECONDS,
                        ),
                    )
                    if response.status_code == 404:
                        response = None
                        break
                    response.raise_for_status()
                    break
                except Exception as exc:  # pragma: no cover - remote variability
                    last_error = exc
                    if attempt < GFS_CATALOG_MAX_ATTEMPTS:
                        logger.warning(
                            "GFS catalog for %s is unavailable (%s/%s). Retrying. Reason: %s",
                            _catalog_day_label(day),
                            attempt,
                            GFS_CATALOG_MAX_ATTEMPTS,
                            _describe_remote_error(exc),
                        )
                    else:
                        logger.warning(
                            "GFS catalog for %s is still unavailable (%s/%s). Trying direct file access. Reason: %s",
                            _catalog_day_label(day),
                            attempt,
                            GFS_CATALOG_MAX_ATTEMPTS,
                            _describe_remote_error(exc),
                        )
            if response is None:
                if last_error is not None:
                    logger.warning(
                        "GFS catalog stayed unavailable for %s. Trying direct file access instead.",
                        _catalog_day_label(day),
                    )
                    return self.fallback_analysis_urls(start_utc, end_utc)
                continue
            try:
                analysis_urls.update(self.parse_gfs_catalog(response.text))
            except ET.ParseError as exc:
                logger.warning(
                    "GFS catalog for %s returned unreadable data. Trying direct file access instead. Reason: %s",
                    _catalog_day_label(day),
                    "invalid catalog response",
                )
                return self.fallback_analysis_urls(start_utc, end_utc)

        discovered = sorted(analysis_urls.items(), key=lambda item: item[0])
        lower_bound = start_utc - self.expected_delta
        upper_bound = end_utc + self.expected_delta
        discovered = [item for item in discovered if lower_bound <= item[0] <= upper_bound]
        if not discovered:
            raise RuntimeError("No GFS analysis files were discoverable for the requested forcing window.")

        timestamps = [item[0] for item in discovered]
        if min(timestamps) > start_utc or max(timestamps) < end_utc:
            raise RuntimeError(
                "GFS analysis catalog does not cover the requested forcing window "
                f"({start_utc.isoformat()} to {end_utc.isoformat()})."
            )

        return discovered

    @staticmethod
    def _coord_slice(coord: xr.DataArray, lower: float, upper: float) -> slice:
        values = np.asarray(coord.values, dtype=float)
        if values.ndim != 1 or values.size == 0:
            raise RuntimeError(f"Unsupported coordinate layout for GFS subset: {coord.name}")
        return slice(lower, upper) if values[0] <= values[-1] else slice(upper, lower)

    def normalize_gfs_subset(
        self,
        ds: xr.Dataset,
        *,
        analysis_time: pd.Timestamp | str,
    ) -> xr.Dataset:
        if {
            "u-component_of_wind_height_above_ground",
            "v-component_of_wind_height_above_ground",
        }.issubset(ds.data_vars):
            u_name = "u-component_of_wind_height_above_ground"
            v_name = "v-component_of_wind_height_above_ground"
        elif {"u10", "v10"}.issubset(ds.data_vars):
            u_name = "u10"
            v_name = "v10"
        else:
            raise RuntimeError(
                "GFS subset does not expose 10 m wind components in a recognized naming scheme."
            )

        u_field = ds[u_name]
        v_field = ds[v_name]
        spatial_dims = {"lat", "lon", "latitude", "longitude"}

        for dim_name in list(u_field.dims):
            if dim_name in spatial_dims or dim_name == "time":
                continue
            coord = u_field.coords.get(dim_name)
            if coord is None:
                continue
            numeric_values = np.asarray(coord.values, dtype=float)
            if numeric_values.size == 1:
                u_field = u_field.isel({dim_name: 0}, drop=True)
                v_field = v_field.isel({dim_name: 0}, drop=True)
                continue
            if np.isclose(numeric_values, 10.0).any():
                u_field = u_field.sel({dim_name: 10.0}, method="nearest")
                v_field = v_field.sel({dim_name: 10.0}, method="nearest")
                continue
            raise RuntimeError(
                f"GFS wind field does not provide a usable 10 m level on dimension {dim_name}."
            )

        lat_name = "lat" if "lat" in u_field.coords else "latitude"
        lon_name = "lon" if "lon" in u_field.coords else "longitude"
        u_field = u_field.sel(
            {
                lat_name: self._coord_slice(u_field.coords[lat_name], self.forcing_box[2], self.forcing_box[3]),
                lon_name: self._coord_slice(u_field.coords[lon_name], self.forcing_box[0], self.forcing_box[1]),
            }
        )
        v_field = v_field.sel(
            {
                lat_name: self._coord_slice(v_field.coords[lat_name], self.forcing_box[2], self.forcing_box[3]),
                lon_name: self._coord_slice(v_field.coords[lon_name], self.forcing_box[0], self.forcing_box[1]),
            }
        )

        u_values = np.asarray(u_field.squeeze(drop=True).values)
        v_values = np.asarray(v_field.squeeze(drop=True).values)
        if u_values.ndim == 3:
            u_values = u_values[0]
        if v_values.ndim == 3:
            v_values = v_values[0]
        if u_values.ndim != 2 or v_values.ndim != 2:
            raise RuntimeError("GFS wind subset could not be reduced to a 2-D lat/lon slice.")

        time_value = _normalize_utc_timestamp(analysis_time)
        normalized = xr.Dataset(
            data_vars={
                "x_wind": (("time", "lat", "lon"), u_values[np.newaxis, :, :]),
                "y_wind": (("time", "lat", "lon"), v_values[np.newaxis, :, :]),
            },
            coords={
                "time": [time_value.tz_localize(None).to_datetime64()],
                "lat": np.asarray(u_field.coords[lat_name].values, dtype=float),
                "lon": np.asarray(u_field.coords[lon_name].values, dtype=float),
            },
        )
        return apply_wind_cf_metadata(normalized)

    def download_gfs_subset_via_opendap(
        self,
        *,
        url: str,
        timestamp: pd.Timestamp | str,
        deadline_monotonic: float | None = None,
    ) -> xr.Dataset:
        _require_remaining_budget(
            deadline_monotonic,
            minimum_seconds=GFS_OPENDAP_ATTEMPT_TIMEOUT_SECONDS,
            failure_stage="opendap",
            stage_label="the GFS OPeNDAP subset attempt",
        )
        timeout_seconds = _capped_timeout(
            deadline_monotonic,
            preferred_seconds=GFS_OPENDAP_ATTEMPT_TIMEOUT_SECONDS,
            minimum_seconds=10,
        )
        with tempfile.NamedTemporaryFile(suffix=".nc", prefix="gfs_opendap_subset_", delete=False) as handle:
            temp_path = Path(handle.name)
        context_name = "fork" if "fork" in multiprocessing.get_all_start_methods() else "spawn"
        context = multiprocessing.get_context(context_name)
        result_queue = context.Queue()
        process = context.Process(
            target=_opendap_subset_child,
            kwargs={
                "result_queue": result_queue,
                "url": url,
                "output_path": str(temp_path),
            },
        )
        process.start()
        process.join(timeout=timeout_seconds)
        try:
            if process.is_alive():
                process.terminate()
                process.join(timeout=5)
                raise GFSAcquisitionError(
                    f"OPeNDAP timed out after {timeout_seconds}s",
                    failure_stage="opendap",
                )
            try:
                payload = result_queue.get_nowait()
            except queue_module.Empty:
                payload = None
            if not payload or not payload.get("ok"):
                raise GFSAcquisitionError(
                    "OPeNDAP subset failed",
                    failure_stage="opendap",
                )
            with xr.open_dataset(temp_path) as subset_ds:
                subset = subset_ds.load()
            return self.normalize_gfs_subset(subset, analysis_time=timestamp)
        finally:
            temp_path.unlink(missing_ok=True)

    def download_gfs_subset_via_http_cfgrib(
        self,
        *,
        url: str,
        timestamp: pd.Timestamp | str,
        scratch_dir: str | Path,
        deadline_monotonic: float | None = None,
    ) -> xr.Dataset:
        if importlib.util.find_spec("cfgrib") is None:
            raise RuntimeError("cfgrib is required for the archived GFS HTTP fallback path.")

        file_url = url.replace("/dodsC/", "/fileServer/")
        last_error: Exception | None = None
        temp_parent = Path(scratch_dir)
        temp_parent.mkdir(parents=True, exist_ok=True)

        for attempt in range(1, GFS_HTTP_FALLBACK_MAX_ATTEMPTS + 1):
            temp_path: Path | None = None
            try:
                _require_remaining_budget(
                    deadline_monotonic,
                    minimum_seconds=GFS_MIN_ATTEMPT_SECONDS,
                    failure_stage="http_cfgrib_fallback",
                    stage_label="the GFS HTTP fallback attempt",
                )
                with tempfile.NamedTemporaryFile(
                    suffix=".grb2",
                    prefix=f"gfs_{_normalize_utc_timestamp(timestamp).strftime('%Y%m%d%H')}_{attempt}_",
                    dir=temp_parent,
                    delete=False,
                ) as handle:
                    temp_path = Path(handle.name)
                    response = requests.get(
                        file_url,
                        stream=True,
                        timeout=_capped_timeout(
                            deadline_monotonic,
                            preferred_seconds=GFS_HTTP_FALLBACK_TIMEOUT_SECONDS,
                        ),
                    )
                    response.raise_for_status()
                    for chunk in response.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            handle.write(chunk)

                dataset = xr.open_dataset(
                    temp_path,
                    engine="cfgrib",
                    filter_by_keys={"typeOfLevel": "heightAboveGround", "level": 10},
                    backend_kwargs={"indexpath": ""},
                )
                try:
                    loaded = dataset.load()
                finally:
                    dataset.close()
                return self.normalize_gfs_subset(loaded, analysis_time=timestamp)
            except Exception as exc:  # pragma: no cover - remote dataset variability
                last_error = exc
                if attempt < GFS_HTTP_FALLBACK_MAX_ATTEMPTS:
                    logger.warning(
                        "GFS direct file access failed for %s (%s/%s). Retrying. Reason: %s",
                        _analysis_label(timestamp),
                        attempt,
                        GFS_HTTP_FALLBACK_MAX_ATTEMPTS,
                        _describe_remote_error(exc),
                    )
                else:
                    logger.warning(
                        "GFS direct file access failed for %s (%s/%s). No retries left. Reason: %s",
                        _analysis_label(timestamp),
                        attempt,
                        GFS_HTTP_FALLBACK_MAX_ATTEMPTS,
                        _describe_remote_error(exc),
                    )
            finally:
                if temp_path is not None:
                    temp_path.unlink(missing_ok=True)

        raise GFSAcquisitionError(
            "Direct file access failed after "
            f"{GFS_HTTP_FALLBACK_MAX_ATTEMPTS} attempt(s). "
            f"Reason: {_describe_remote_error(last_error) if last_error is not None else 'unknown error'}.",
            failure_stage="http_cfgrib_fallback",
        )

    @staticmethod
    def prefers_http_cfgrib(url: str) -> bool:
        return "/model-gfs-g4-anl-files" in str(url)

    def download_gfs_subset_with_preferred_transport(
        self,
        *,
        url: str,
        timestamp: pd.Timestamp | str,
        scratch_dir: str | Path,
        deadline_monotonic: float | None = None,
    ) -> tuple[xr.Dataset, str]:
        prefer_http_first = self.prefers_http_cfgrib(url)
        transport_attempts = (
            (
                ("http_cfgrib_fallback", self.download_gfs_subset_via_http_cfgrib),
                ("opendap", self.download_gfs_subset_via_opendap),
            )
            if prefer_http_first
            else (
                ("opendap", self.download_gfs_subset_via_opendap),
                ("http_cfgrib_fallback", self.download_gfs_subset_via_http_cfgrib),
            )
        )

        errors: list[tuple[str, Exception]] = []
        for attempt_index, (mode_name, loader) in enumerate(transport_attempts, start=1):
            try:
                if mode_name == "http_cfgrib_fallback":
                    dataset = loader(
                        url=url,
                        timestamp=timestamp,
                        scratch_dir=scratch_dir,
                        deadline_monotonic=deadline_monotonic,
                    )
                else:
                    dataset = loader(
                        url=url,
                        timestamp=timestamp,
                        deadline_monotonic=deadline_monotonic,
                    )
                return dataset, mode_name
            except Exception as exc:  # pragma: no cover - remote dataset variability
                errors.append((mode_name, exc))
                mode_label = _transport_mode_label(mode_name)
                if attempt_index < len(transport_attempts):
                    next_mode_label = _transport_mode_label(transport_attempts[attempt_index][0])
                    logger.warning(
                        "GFS %s failed for %s. Trying %s. Reason: %s",
                        mode_label,
                        _analysis_label(timestamp),
                        next_mode_label,
                        exc,
                    )
                else:
                    logger.warning(
                        "GFS %s failed for %s. No access paths remain. Reason: %s",
                        mode_label,
                        _analysis_label(timestamp),
                        exc,
                    )

        tried_modes = ", ".join(_transport_mode_label(mode) for mode, _ in errors)
        raise GFSAcquisitionError(
            f"GFS data for {_analysis_label(timestamp)} could not be opened. Tried: {tried_modes}.",
            failure_stage="transport_modes",
        )

    def download(
        self,
        *,
        start_time: pd.Timestamp | str,
        end_time: pd.Timestamp | str,
        output_path: str | Path,
        scratch_dir: str | Path | None = None,
        budget_seconds: int | float | None = None,
    ) -> dict[str, Any]:
        start_utc = _normalize_utc_timestamp(start_time)
        end_utc = _normalize_utc_timestamp(end_time)
        deadline_monotonic = (
            None
            if budget_seconds in (None, 0)
            else time.monotonic() + float(budget_seconds)
        )
        target_path = Path(output_path)
        target_path.parent.mkdir(parents=True, exist_ok=True)
        if target_path.exists():
            self.ensure_reader_metadata(target_path)
            return {"status": "cached", "path": _relative(self.repo_root, target_path)}

        frames = []
        source_urls: list[str] = []
        source_modes: list[str] = []
        discovered = self.discover_gfs_analysis_urls(
            start_utc,
            end_utc,
            deadline_monotonic=deadline_monotonic,
        )
        working_dir = Path(scratch_dir or target_path.parent)

        for timestamp, url in discovered:
            _require_remaining_budget(
                deadline_monotonic,
                minimum_seconds=GFS_MIN_ATTEMPT_SECONDS,
                failure_stage="analysis_selection",
                stage_label="the next required GFS analysis",
            )
            try:
                subset, mode_name = self.download_gfs_subset_with_preferred_transport(
                    url=url,
                    timestamp=timestamp,
                    scratch_dir=working_dir,
                    deadline_monotonic=deadline_monotonic,
                )
                source_modes.append(mode_name)
            except Exception as exc:  # pragma: no cover - remote dataset variability
                raise GFSAcquisitionError(
                    f"Required GFS data for {_analysis_label(timestamp)} could not be opened. {exc}",
                    failure_stage=getattr(exc, "failure_stage", "transport_modes"),
                ) from exc
            frames.append(subset)
            source_urls.append(url)

        if not frames:
            raise GFSAcquisitionError(
                "All discovered GFS analysis files failed to open for the requested forcing window.",
                failure_stage="transport_modes",
            )

        combined = xr.concat(frames, dim="time").sortby("time")
        combined.to_netcdf(target_path)
        return {
            "status": "downloaded",
            "analysis_count": len(discovered),
            "opened_analysis_count": len(source_urls),
            "skipped_analysis_count": 0,
            "analysis_time_start_utc": discovered[0][0].isoformat(),
            "analysis_time_end_utc": discovered[-1][0].isoformat(),
            "source_url_count": len(source_urls),
            "sample_source_url": source_urls[0] if source_urls else "",
            "source_modes_used": sorted(set(source_modes)),
            "skipped_urls": [],
            "path": _relative(self.repo_root, target_path),
        }
