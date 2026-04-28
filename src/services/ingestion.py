"""
Data Ingestion Service.
Automates downloading of forcing data (Currents, Winds) and Drifter observations.
"""

import os
import json
import logging
import tempfile
import warnings
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List

import xarray as xr
import pandas as pd
import shutil
import yaml
from erddapy import ERDDAP

# Custom Helpers
from src.helpers.metadata import fix_metadata

# Third-party APIs (Authentication required via env vars)
try:
    import cdsapi
    # Suppress pkg_resources deprecation warning via cdsapi
    warnings.filterwarnings("ignore", category=UserWarning, module='cdsapi') 
except ImportError:
    cdsapi = None

try:
    import copernicusmarine
except ImportError:
    copernicusmarine = None

try:
    import geopandas as gpd
except ImportError:
    gpd = None

from src.core.case_context import get_case_context
from src.core.constants import CMEMS_SURFACE_CURRENT_MAX_DEPTH_M, RUN_NAME
from src.core.base import BaseService
from src.exceptions.custom import (
    DataLoadingError,
    ForcingProviderAcquisitionError,
    PREP_FORCE_REFRESH_ENV,
    PrepOutageDecisionRequired,
    PREP_REUSE_APPROVED_ONCE_ENV,
    PREP_REUSE_APPROVED_SOURCE_ENV,
)
from src.models.ingestion import IngestionManifest
from src.helpers.raster import GridBuilder
from src.utils.gfs_wind import GFSWindDownloader
from src.utils.forcing_outage_policy import (
    FORCING_OUTAGE_POLICY_CONTINUE_DEGRADED,
    forcing_factor_id_for_source,
    is_remote_outage_error,
    run_budgeted_forcing_provider_call,
    resolve_forcing_outage_policy,
    resolve_forcing_source_budget_seconds,
)
from src.utils.startup_prompt_policy import input_cache_policy_force_refresh_enabled
from src.utils.io import (
    find_current_vars,
    find_wave_vars,
    find_wind_vars,
    get_prepared_input_manifest_path,
    get_prepared_input_manifest_json_path,
    get_prepared_input_specs,
)
from src.utils.ensemble_config import get_legacy_perturbations_block, load_ensemble_config
from src.utils.local_input_store import (
    PERSISTENT_LOCAL_INPUT_STORE,
    classify_reuse_action,
    validation_status_from_record,
    write_json,
)

# Setup logging
logger = logging.getLogger(__name__)
OFFICIAL_FORCING_HALO_DEGREES_DEFAULT = 0.5
PROTOTYPE_FORCING_HALO_HOURS_DEFAULT = 3.0
PROTOTYPE_GFS_ANALYSIS_DELTA_HOURS = 6
NOAA_DRIFTER_ERDDAP_URL = "https://osmc.noaa.gov/erddap/tabledap/drifter_6hour_qc"
HYCOM_THREDDS_CATALOG_URL = "https://tds.hycom.org/thredds/catalog.html"
NCEP_REANALYSIS_SURFACE_URL = "https://psl.noaa.gov/data/gridded/data.ncep.reanalysis.surface.html"
ERA5_DATASET_URL = "https://cds.climate.copernicus.eu/datasets/reanalysis-era5-single-levels"
CMEMS_CURRENT_MULTIYEAR_URL = "https://data.marine.copernicus.eu/product/GLOBAL_MULTIYEAR_PHY_001_030/description"
CMEMS_CURRENT_FORECAST_URL = "https://data.marine.copernicus.eu/product/GLOBAL_ANALYSISFORECAST_PHY_001_024/description"
CMEMS_WAVE_MULTIYEAR_URL = "https://data.marine.copernicus.eu/product/GLOBAL_MULTIYEAR_WAV_001_032/description"
CMEMS_WAVE_FORECAST_URL = "https://data.marine.copernicus.eu/product/GLOBAL_ANALYSISFORECAST_WAV_001_027/description"
GFS_PRIMARY_URL = "https://www.ncei.noaa.gov/products/weather-climate-models/global-forecast"
GFS_SECONDARY_URL = "https://gdex.ucar.edu/datasets/d084001/"

def derive_bbox_from_display_bounds(
    display_bounds_wgs84: list[float],
    halo_degrees: float = OFFICIAL_FORCING_HALO_DEGREES_DEFAULT,
) -> list[float]:
    """Expand canonical scoring-grid display bounds by a fixed geographic halo."""
    if len(display_bounds_wgs84) != 4:
        raise ValueError("display_bounds_wgs84 must contain [min_lon, max_lon, min_lat, max_lat].")
    min_lon, max_lon, min_lat, max_lat = [float(value) for value in display_bounds_wgs84]
    halo = float(halo_degrees)
    if halo < 0:
        raise ValueError("halo_degrees must be >= 0.")
    return [
        min_lon - halo,
        max_lon + halo,
        min_lat - halo,
        max_lat + halo,
    ]


def _normalize_utc_timestamp(value: str | pd.Timestamp) -> pd.Timestamp:
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        return timestamp.tz_localize("UTC")
    return timestamp.tz_convert("UTC")


def compute_prototype_forcing_window(
    start_utc: str | pd.Timestamp,
    end_utc: str | pd.Timestamp,
    halo_hours: float,
) -> tuple[pd.Timestamp, pd.Timestamp]:
    start_time = _normalize_utc_timestamp(start_utc)
    end_time = _normalize_utc_timestamp(end_utc)
    halo = pd.Timedelta(hours=float(halo_hours))
    return start_time - halo, end_time + halo

class ArcGISLayerIngestionError(RuntimeError):
    pass


def _budgeted_ingestion_forcing_download(
    *,
    source_id: str,
    output_dir: str,
    forcing_dir: str,
    start_date: str,
    end_date: str,
    strict: bool = True,
) -> dict[str, Any]:
    service = DataIngestionService(output_dir=output_dir)
    service.forcing_dir = Path(forcing_dir)
    service.forcing_dir.mkdir(parents=True, exist_ok=True)
    service.configure_explicit_download_window(start_date=start_date, end_date=end_date)

    try:
        if source_id == "hycom":
            result = service.download_hycom(raise_on_failure=True)
        elif source_id == "cmems":
            result = service.download_cmems(raise_on_failure=True)
        elif source_id == "cmems_wave":
            result = service.download_cmems_wave(raise_on_failure=True)
        elif source_id == "era5":
            result = service.download_era5(raise_on_failure=True)
        elif source_id == "ncep":
            result = service.download_ncep(raise_on_failure=True)
        elif source_id == "gfs":
            result = service.download_gfs(
                strict=bool(strict),
                budget_seconds=resolve_forcing_source_budget_seconds(),
            )
        else:
            raise KeyError(f"Unsupported forcing source id: {source_id}")
    except Exception as exc:
        outage = service._is_remote_outage_error(exc)
        return {
            "status": "failed_remote_outage" if outage else "failed",
            "source_id": str(source_id),
            "forcing_factor": forcing_factor_id_for_source(source_id),
            "path": str(service.forcing_dir / forcing_factor_id_for_source(source_id)),
            "upstream_outage_detected": outage,
            "failure_stage": str(getattr(exc, "failure_stage", "provider_call") or "provider_call"),
            "error": str(exc),
        }

    return service._normalize_download_record(source_id, result)

class DataIngestionService(BaseService):
    """
    Service for downloading Ocean/Met forcing and Drifter data.
    """

    def __init__(self, output_dir: str = 'data'):
        self.case_context = get_case_context()
        self._assert_pipeline_role()
        self.case_config = self._load_case_config()
        self.current_phase = os.environ.get("PIPELINE_PHASE", "1_2").strip() or "1_2"
        self.forcing_outage_policy = resolve_forcing_outage_policy(
            workflow_mode=self.case_context.workflow_mode,
            phase=self.current_phase,
        )
        self.output_dir = Path(output_dir)
        self.forcing_dir = self.output_dir / 'forcing' / RUN_NAME
        self.drifter_dir = self.output_dir / 'drifters' / RUN_NAME
        self.arcgis_dir = self.output_dir / 'arcgis' / RUN_NAME
        self.prepared_dir = self.output_dir / 'prepared' / RUN_NAME

        # Ensure directories exist
        self.forcing_dir.mkdir(parents=True, exist_ok=True)
        self.drifter_dir.mkdir(parents=True, exist_ok=True)
        self.arcgis_dir.mkdir(parents=True, exist_ok=True)
        self.prepared_dir.mkdir(parents=True, exist_ok=True)

        drifter_mode = getattr(self.case_context, "drifter_mode", "prototype_scan" if self.case_context.is_prototype else "fixed_case_window")
        self.drifter_search_dates = (
            list(self.case_context.prototype_case_dates)
            if drifter_mode == "prototype_scan"
            else [self.case_context.forcing_start_date]
        )
        self.prototype_forcing_halo_hours = self._resolve_prototype_forcing_halo_hours()

        self.official_forcing_halo_degrees = float(
            self.case_config.get("forcing_bbox_halo_degrees", OFFICIAL_FORCING_HALO_DEGREES_DEFAULT)
        )
        if self.case_context.is_official:
            self.bbox = list(self.case_context.region)
            self.bbox_source = "official_active_domain_fallback_before_scoring_grid"
            try:
                from src.helpers.scoring import get_scoring_grid_artifact_paths, get_scoring_grid_spec

                metadata_path = get_scoring_grid_artifact_paths()["metadata_yaml"]
                if metadata_path.exists():
                    spec = get_scoring_grid_spec()
                    display_bounds = spec.display_bounds_wgs84 or list(self.case_context.region)
                    self.bbox = derive_bbox_from_display_bounds(
                        display_bounds_wgs84=display_bounds,
                        halo_degrees=self.official_forcing_halo_degrees,
                    )
                    self.bbox_source = (
                        f"canonical_scoring_grid_display_bounds_plus_{self.official_forcing_halo_degrees:.2f}deg_halo"
                    )
            except Exception:
                self.bbox = list(self.case_context.region)
                self.bbox_source = "official_active_domain_fallback_before_scoring_grid"
        elif self.case_context.workflow_mode == "prototype_2021":
            self.bbox = derive_bbox_from_display_bounds(
                list(self.case_context.legacy_prototype_display_domain),
                halo_degrees=self.official_forcing_halo_degrees,
            )
            self.bbox_source = (
                f"prototype_2021_display_domain_plus_{self.official_forcing_halo_degrees:.2f}deg_halo"
            )
        else:
            # Pad bounding box heavily to prevent edge-clipping during interpolation for low-res models like NCEP
            pad = 3.0
            prototype_display_domain = list(self.case_context.region)
            self.bbox = [
                prototype_display_domain[0] - pad,
                prototype_display_domain[1] + pad,
                prototype_display_domain[2] - pad,
                prototype_display_domain[3] + pad,
            ]
            self.bbox_source = (
                "prototype_2016_case_local_domain_plus_3deg_pad"
                if self.case_context.workflow_mode == "prototype_2016"
                else "legacy_prototype_display_domain_plus_3deg_pad"
            )
        self.grid = GridBuilder() if self.case_context.is_prototype else None
        self.gfs_downloader = GFSWindDownloader(
            forcing_box=self.bbox,
            expected_delta=pd.Timedelta(hours=PROTOTYPE_GFS_ANALYSIS_DELTA_HOURS),
        )
        self.degraded_continue_used = False
        self.upstream_outage_detected = False
        self.missing_forcing_factors: set[str] = set()
        self.skipped_forcing_sources: set[str] = set()
        self._apply_forcing_window(
            self.case_context.forcing_start_utc,
            self.case_context.forcing_end_utc,
        )

    @staticmethod
    def _assert_pipeline_role():
        role = os.environ.get("PIPELINE_ROLE", "").strip().lower()
        if role and role != "pipeline":
            raise RuntimeError(
                "Data preparation is only supported in the pipeline container. "
                "Run the prep stage from the pipeline service instead."
            )

    def _load_case_config(self) -> dict:
        if not self.case_context.case_definition_path:
            return {}
        case_path = Path(self.case_context.case_definition_path)
        if not case_path.exists():
            return {}
        with open(case_path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f) or {}

    def _refresh_official_forcing_bbox_from_scoring_grid(self) -> None:
        if not self.case_context.is_official:
            return

        from src.helpers.scoring import get_scoring_grid_spec

        spec = get_scoring_grid_spec()
        display_bounds = spec.display_bounds_wgs84 or list(self.case_context.region)
        self.bbox = derive_bbox_from_display_bounds(
            display_bounds_wgs84=display_bounds,
            halo_degrees=self.official_forcing_halo_degrees,
        )
        self.bbox_source = (
            f"canonical_scoring_grid_display_bounds_plus_{self.official_forcing_halo_degrees:.2f}deg_halo"
        )
        logger.info(
            "Official forcing subset bbox refreshed from scoring grid: %s (%s)",
            self.bbox,
            self.bbox_source,
        )

    def _resolve_prototype_forcing_halo_hours(self) -> float:
        if not self.case_context.is_prototype:
            return 0.0

        settings = {}
        settings_path = Path("config/settings.yaml")
        if settings_path.exists():
            with open(settings_path, "r", encoding="utf-8") as handle:
                settings = yaml.safe_load(handle) or {}
        if settings.get("prototype_forcing_halo_hours") is not None:
            return float(settings["prototype_forcing_halo_hours"])

        ensemble_path = Path("config/ensemble.yaml")
        if ensemble_path.exists():
            ensemble_cfg = load_ensemble_config(ensemble_path)
            legacy_perturbations = get_legacy_perturbations_block(ensemble_cfg)
            if bool(legacy_perturbations.get("active", False)) and legacy_perturbations.get("time_shift_hours") is not None:
                return float(legacy_perturbations["time_shift_hours"])

        return PROTOTYPE_FORCING_HALO_HOURS_DEFAULT

    def _apply_forcing_window(self, nominal_start_utc: str, nominal_end_utc: str) -> None:
        nominal_start = _normalize_utc_timestamp(nominal_start_utc)
        nominal_end = _normalize_utc_timestamp(nominal_end_utc)
        if self.case_context.is_prototype:
            effective_start, effective_end = compute_prototype_forcing_window(
                nominal_start,
                nominal_end,
                halo_hours=self.prototype_forcing_halo_hours,
            )
        else:
            effective_start, effective_end = nominal_start, nominal_end

        self.nominal_forcing_start_utc = nominal_start.strftime("%Y-%m-%dT%H:%M:%SZ")
        self.nominal_forcing_end_utc = nominal_end.strftime("%Y-%m-%dT%H:%M:%SZ")
        self.effective_forcing_start_utc = effective_start.strftime("%Y-%m-%dT%H:%M:%SZ")
        self.effective_forcing_end_utc = effective_end.strftime("%Y-%m-%dT%H:%M:%SZ")
        self.start_date = effective_start.strftime("%Y-%m-%d")
        self.end_date = effective_end.strftime("%Y-%m-%d")

    def _manifest_config(self) -> dict[str, str]:
        return {
            "bbox": str(self.bbox),
            "bbox_source": self.bbox_source,
            "nominal_forcing_start_utc": self.nominal_forcing_start_utc,
            "nominal_forcing_end_utc": self.nominal_forcing_end_utc,
            "effective_forcing_start_utc": self.effective_forcing_start_utc,
            "effective_forcing_end_utc": self.effective_forcing_end_utc,
            "prototype_forcing_halo_hours": str(self.prototype_forcing_halo_hours),
            "prep_cache_policy": "force_refresh" if self._force_refresh_enabled() else "cache_first",
            "forcing_outage_policy": self.forcing_outage_policy,
            "forcing_source_budget_seconds": str(resolve_forcing_source_budget_seconds()),
            "pipeline_phase": self.current_phase,
            "degraded_continue_used": str(self.degraded_continue_used).lower(),
            "upstream_outage_detected": str(self.upstream_outage_detected).lower(),
            "missing_forcing_factors": ";".join(sorted(self.missing_forcing_factors)),
            "skipped_forcing_sources": ";".join(sorted(self.skipped_forcing_sources)),
        }

    def configure_explicit_download_window(self, *, start_date: str, end_date: str) -> None:
        """Override the active acquisition window for direct forcing-download callers."""
        self.start_date = str(start_date)
        self.end_date = str(end_date)
        self.nominal_forcing_start_utc = f"{self.start_date}T00:00:00Z"
        self.nominal_forcing_end_utc = f"{self.end_date}T23:59:59Z"
        self.effective_forcing_start_utc = self.nominal_forcing_start_utc
        self.effective_forcing_end_utc = self.nominal_forcing_end_utc

    @staticmethod
    def _env_flag_enabled(name: str) -> bool:
        return os.environ.get(name, "").strip().lower() in {"1", "true", "yes", "y", "on"}

    def _force_refresh_enabled(self) -> bool:
        return input_cache_policy_force_refresh_enabled(
            prep_force_refresh_value=os.environ.get(PREP_FORCE_REFRESH_ENV),
        )

    def _validated_cache_hit(
        self,
        source_id: str,
        *,
        reuse_mode: str = "cache_first",
    ) -> dict[str, Any] | None:
        if self._force_refresh_enabled():
            return None

        validation = self._validate_cached_source(source_id)
        if not validation.get("valid"):
            return None

        path = str(validation.get("path") or self._canonical_cache_path(source_id))
        logger.info("Reusing validated local %s cache: %s", source_id, path)
        return {
            "status": "reused_validated_cache",
            "path": path,
            "validation": validation,
            "reuse_mode": reuse_mode,
            "source_id": source_id,
        }

    def _download_manifest_path(self) -> Path:
        return self.output_dir / "download_manifest.json"

    def _persist_download_manifest(self, manifest: IngestionManifest) -> Path:
        manifest_path = self._download_manifest_path()
        all_manifests = {}
        if manifest_path.exists():
            try:
                with open(manifest_path, "r", encoding="utf-8") as handle:
                    all_manifests = json.load(handle)
            except Exception:
                all_manifests = {}

        all_manifests[RUN_NAME] = {
            "timestamp": manifest.timestamp,
            "config": dict(manifest.config),
            "downloads": dict(manifest.downloads),
        }
        with open(manifest_path, "w", encoding="utf-8") as handle:
            json.dump(all_manifests, handle, indent=2, default=str)
        return manifest_path

    def _normalize_download_record(self, source_id: str, result: Any) -> dict[str, Any]:
        if isinstance(result, dict):
            normalized = dict(result)
            normalized.setdefault("status", "downloaded")
            if normalized.get("path") is not None:
                normalized["path"] = str(normalized["path"])
            normalized.setdefault("source_id", source_id)
            return self._enrich_download_record(source_id, normalized)

        if isinstance(result, Path):
            return self._enrich_download_record(
                source_id,
                {
                "status": "downloaded",
                "path": str(result),
                "source_id": source_id,
                },
            )

        text = str(result)
        uppercase = text.upper()
        if uppercase.startswith("SKIPPED_"):
            return self._enrich_download_record(source_id, {"status": uppercase.lower(), "source_id": source_id})
        if uppercase == "FAILED":
            return self._enrich_download_record(source_id, {"status": "failed", "source_id": source_id})

        candidate_path = Path(text)
        if candidate_path.exists() or candidate_path.suffix:
            return self._enrich_download_record(
                source_id,
                {
                "status": "downloaded",
                "path": str(candidate_path),
                "source_id": source_id,
                },
            )

        return self._enrich_download_record(source_id, {"status": text.lower(), "source_id": source_id})

    def _record_download(self, manifest: IngestionManifest, source_id: str, result: Any) -> dict[str, Any]:
        record = self._normalize_download_record(source_id, result)
        manifest.downloads[source_id] = record
        self._persist_download_manifest(manifest)
        return record

    def _canonical_cache_path(self, source_id: str) -> Path:
        mapping = {
            "drifters": self.drifter_dir / "drifters_noaa.csv",
            "hycom": self.forcing_dir / "hycom_curr.nc",
            "cmems": self.forcing_dir / "cmems_curr.nc",
            "cmems_wave": self.forcing_dir / "cmems_wave.nc",
            "era5": self.forcing_dir / "era5_wind.nc",
            "gfs": self.forcing_dir / "gfs_wind.nc",
            "ncep": self.forcing_dir / "ncep_wind.nc",
            "arcgis": self.arcgis_dir / "arcgis_registry.csv",
        }
        if source_id not in mapping:
            raise KeyError(f"Unsupported cache source id: {source_id}")
        return mapping[source_id]

    def _forcing_source_urls(self) -> dict[str, str]:
        request_year = datetime.strptime(self.start_date, "%Y-%m-%d").year
        return {
            "drifters": NOAA_DRIFTER_ERDDAP_URL,
            "hycom": HYCOM_THREDDS_CATALOG_URL,
            "cmems": CMEMS_CURRENT_MULTIYEAR_URL if request_year < 2022 else CMEMS_CURRENT_FORECAST_URL,
            "cmems_wave": CMEMS_WAVE_MULTIYEAR_URL if request_year < 2022 else CMEMS_WAVE_FORECAST_URL,
            "era5": ERA5_DATASET_URL,
            "gfs": GFS_PRIMARY_URL,
            "ncep": NCEP_REANALYSIS_SURFACE_URL,
            "arcgis": ";".join(sorted({str(layer.service_url) for layer in self.case_context.arcgis_layers if str(layer.service_url).strip()})),
        }

    def _source_metadata_for(self, source_id: str) -> dict[str, str]:
        source_urls = self._forcing_source_urls()
        mapping = {
            "drifters": {"provider": "NOAA GDP ERDDAP", "source_url": source_urls["drifters"]},
            "hycom": {"provider": "HYCOM", "source_url": source_urls["hycom"]},
            "cmems": {"provider": "Copernicus Marine", "source_url": source_urls["cmems"]},
            "cmems_wave": {"provider": "Copernicus Marine", "source_url": source_urls["cmems_wave"]},
            "era5": {"provider": "ECMWF/Copernicus Climate Data Store", "source_url": source_urls["era5"]},
            "gfs": {"provider": "NOAA GFS archive", "source_url": source_urls["gfs"]},
            "ncep": {"provider": "NOAA PSL", "source_url": source_urls["ncep"]},
            "arcgis": {"provider": "ArcGIS FeatureServer", "source_url": source_urls["arcgis"]},
        }
        return dict(mapping.get(str(source_id), {}))

    def _validation_summary_text(self, validation: dict[str, Any] | None) -> str:
        validation = dict(validation or {})
        if validation.get("summary"):
            return str(validation["summary"])
        if validation.get("reason"):
            return str(validation["reason"])
        coverage_start = str(validation.get("coverage_start_utc") or "").strip()
        coverage_end = str(validation.get("coverage_end_utc") or "").strip()
        if coverage_start or coverage_end:
            return " -> ".join(part for part in (coverage_start, coverage_end) if part)
        return ""

    def _enrich_download_record(self, source_id: str, record: dict[str, Any]) -> dict[str, Any]:
        enriched = dict(record or {})
        metadata = self._source_metadata_for(source_id)
        for key, value in metadata.items():
            enriched.setdefault(key, value)
        enriched.setdefault("source_id", source_id)
        enriched.setdefault("storage_tier", PERSISTENT_LOCAL_INPUT_STORE)
        enriched.setdefault("local_storage_path", str(enriched.get("path") or self._canonical_cache_path(source_id)))
        enriched.setdefault("reuse_action", classify_reuse_action(enriched.get("status")))
        validation = dict(enriched.get("validation") or {})
        if not validation and str(enriched.get("status") or "").strip().lower() not in {
            "failed",
            "skipped_no_creds",
            "skipped_no_lib",
            "skipped_no_data_found",
            "best_effort_failed",
            "cancelled_no_cache",
            "failed_after_reuse_prompt",
            "awaiting_cache_reuse_decision",
        }:
            candidate_validation = self._validate_cached_source(source_id)
            if candidate_validation.get("path"):
                validation = candidate_validation
        if validation:
            enriched["validation"] = validation
            enriched["validation_summary"] = self._validation_summary_text(validation)
            enriched["validation_status"] = "validated" if validation.get("valid") else f"invalid:{validation.get('reason', '')}".rstrip(":")
        else:
            enriched.setdefault("validation_summary", "")
            enriched.setdefault("validation_status", validation_status_from_record(enriched))
        return enriched

    def _source_id_for_prepared_spec(self, spec: dict[str, Any]) -> str:
        label = str(spec.get("label") or "")
        path = Path(str(spec.get("path") or ""))
        if "drifter" in label:
            return "drifters"
        if "arcgis" in label or "shoreline" in label or "scoring_grid" in label or "land_mask" in label or "sea_mask" in label:
            return "arcgis"
        filename = path.name.lower()
        if filename == "hycom_curr.nc":
            return "hycom"
        if filename == "cmems_curr.nc":
            return "cmems"
        if filename == "cmems_wave.nc":
            return "cmems_wave"
        if filename == "era5_wind.nc":
            return "era5"
        if filename == "gfs_wind.nc":
            return "gfs"
        if filename == "ncep_wind.nc":
            return "ncep"
        if label in {"prepared_input_manifest", "download_manifest"}:
            return ""
        return ""

    def _dataset_time_bounds(self, ds: xr.Dataset) -> tuple[pd.Timestamp, pd.Timestamp]:
        time_name = next((name for name in ("time", "valid_time") if name in ds.coords or name in ds.variables), None)
        if not time_name:
            raise RuntimeError("Dataset is missing a time coordinate.")

        times = pd.to_datetime(ds[time_name].values, utc=True, errors="coerce")
        if len(times) == 0 or pd.isna(times).all():
            raise RuntimeError("Dataset time coordinate is empty.")

        valid_times = pd.DatetimeIndex(times).dropna()
        if valid_times.empty:
            raise RuntimeError("Dataset time coordinate could not be parsed.")
        return valid_times.min(), valid_times.max()

    def _validate_dataset_cache(
        self,
        path: Path,
        *,
        source_id: str,
        finder,
    ) -> dict[str, Any]:
        if not path.exists():
            return {
                "valid": False,
                "path": str(path),
                "reason": "canonical same-case cache file does not exist",
            }
        if path.stat().st_size <= 0:
            return {
                "valid": False,
                "path": str(path),
                "reason": "canonical same-case cache file is empty",
            }

        with xr.open_dataset(path) as ds:
            loaded = ds.load()
        variables = list(finder(loaded))
        coverage_start, coverage_end = self._dataset_time_bounds(loaded)
        required_start = _normalize_utc_timestamp(self.effective_forcing_start_utc)
        required_end = _normalize_utc_timestamp(self.effective_forcing_end_utc)
        if coverage_start > required_start or coverage_end < required_end:
            return {
                "valid": False,
                "path": str(path),
                "reason": (
                    f"cache time coverage {coverage_start.strftime('%Y-%m-%dT%H:%M:%SZ')} -> "
                    f"{coverage_end.strftime('%Y-%m-%dT%H:%M:%SZ')} does not span the effective forcing window "
                    f"{required_start.strftime('%Y-%m-%dT%H:%M:%SZ')} -> {required_end.strftime('%Y-%m-%dT%H:%M:%SZ')}"
                ),
                "coverage_start_utc": coverage_start.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "coverage_end_utc": coverage_end.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "variables": variables,
            }

        return {
            "valid": True,
            "path": str(path),
            "summary": "validated canonical same-case cache",
            "coverage_start_utc": coverage_start.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "coverage_end_utc": coverage_end.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "variables": variables,
            "source_id": source_id,
        }

    def _validate_drifter_cache(self, path: Path) -> dict[str, Any]:
        if not path.exists():
            return {"valid": False, "path": str(path), "reason": "canonical same-case drifter cache does not exist"}
        if path.stat().st_size <= 0:
            return {"valid": False, "path": str(path), "reason": "canonical same-case drifter cache is empty"}

        df = pd.read_csv(path)
        required_columns = {"time", "lat", "lon", "ID", "ve", "vn"}
        missing = sorted(required_columns.difference(df.columns))
        if missing:
            return {
                "valid": False,
                "path": str(path),
                "reason": f"drifter cache is missing required columns: {', '.join(missing)}",
            }
        if df.empty:
            return {"valid": False, "path": str(path), "reason": "drifter cache contains no rows"}

        times = pd.to_datetime(df["time"], utc=True, errors="coerce")
        if pd.isna(times).any():
            return {"valid": False, "path": str(path), "reason": "drifter cache contains unparsable timestamps"}

        coverage_start = pd.DatetimeIndex(times).min()
        coverage_end = pd.DatetimeIndex(times).max()
        summary = {
            "valid": True,
            "path": str(path),
            "summary": "validated canonical same-case drifter cache",
            "coverage_start_utc": coverage_start.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "coverage_end_utc": coverage_end.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "row_count": int(len(df)),
            "drifter_ids": sorted({str(value).strip() for value in df["ID"].astype(str)}),
        }

        if self.case_context.drifter_mode == "fixed_drifter_segment_window":
            expected_id = str(self.case_context.configured_drifter_id or "").strip()
            actual_ids = sorted({str(value).strip() for value in df["ID"].astype(str)})
            if actual_ids != [expected_id]:
                return {
                    "valid": False,
                    "path": str(path),
                    "reason": (
                        f"drifter cache IDs {actual_ids} do not match the configured exact segment ID {expected_id}"
                    ),
                }
            expected_times = pd.date_range(
                _normalize_utc_timestamp(self.case_context.release_start_utc),
                _normalize_utc_timestamp(self.case_context.simulation_end_utc),
                freq="6H",
                tz="UTC",
            )
            actual_times = pd.DatetimeIndex(times)
            if len(actual_times) != len(expected_times) or not actual_times.equals(expected_times):
                return {
                    "valid": False,
                    "path": str(path),
                    "reason": "drifter cache does not match the configured exact 6-hour segment coverage",
                }
            summary["summary"] = "validated canonical same-case exact NOAA drifter segment cache"

        return summary

    def _arcgis_required_cache_paths(self) -> list[Path]:
        specs = get_prepared_input_specs(
            require_drifter=False,
            include_all_transport_forcing=False,
            run_name=RUN_NAME,
        )
        required_paths: list[Path] = []
        arcgis_prefix = str(self.arcgis_dir.resolve())
        for spec in specs:
            path = Path(spec["path"])
            try:
                resolved = str(path.resolve())
            except Exception:
                resolved = str(path)
            if resolved.startswith(arcgis_prefix):
                required_paths.append(path)
                continue
            if self.case_context.is_official and spec["label"] in {
                "scoring_grid_metadata",
                "scoring_grid_template",
                "scoring_grid_extent",
                "land_mask",
                "sea_mask",
                "shoreline_segments",
                "shoreline_mask_manifest_json",
                "shoreline_mask_manifest_csv",
            }:
                required_paths.append(path)
        return required_paths

    def _validate_arcgis_cache(self, path: Path) -> dict[str, Any]:
        if not path.exists():
            return {"valid": False, "path": str(path), "reason": "canonical same-case ArcGIS registry does not exist"}
        if path.stat().st_size <= 0:
            return {"valid": False, "path": str(path), "reason": "canonical same-case ArcGIS registry is empty"}

        registry_df = pd.read_csv(path)
        if registry_df.empty:
            return {"valid": False, "path": str(path), "reason": "ArcGIS registry exists but contains no layers"}

        missing_paths = [str(candidate) for candidate in self._arcgis_required_cache_paths() if not Path(candidate).exists()]
        if missing_paths:
            return {
                "valid": False,
                "path": str(path),
                "reason": "ArcGIS cache is missing required same-case artifacts",
                "missing_paths": missing_paths,
            }

        return {
            "valid": True,
            "path": str(path),
            "summary": "validated canonical same-case ArcGIS cache bundle",
            "row_count": int(len(registry_df)),
            "layer_names": sorted(str(value) for value in registry_df.get("name", pd.Series(dtype=str)).astype(str).tolist()),
        }

    def _validate_cached_source(self, source_id: str) -> dict[str, Any]:
        cache_path = self._canonical_cache_path(source_id)
        try:
            if source_id == "drifters":
                return self._validate_drifter_cache(cache_path)
            if source_id == "arcgis":
                return self._validate_arcgis_cache(cache_path)
            if source_id == "hycom":
                return self._validate_dataset_cache(cache_path, source_id=source_id, finder=find_current_vars)
            if source_id == "cmems":
                return self._validate_dataset_cache(cache_path, source_id=source_id, finder=find_current_vars)
            if source_id == "cmems_wave":
                return self._validate_dataset_cache(cache_path, source_id=source_id, finder=find_wave_vars)
            if source_id == "era5":
                return self._validate_dataset_cache(cache_path, source_id=source_id, finder=find_wind_vars)
            if source_id == "ncep":
                return self._validate_dataset_cache(cache_path, source_id=source_id, finder=find_wind_vars)
            if source_id == "gfs":
                return self._validate_dataset_cache(cache_path, source_id=source_id, finder=find_wind_vars)
        except Exception as exc:
            return {
                "valid": False,
                "path": str(cache_path),
                "reason": str(exc),
            }
        raise KeyError(f"Unsupported cache source id: {source_id}")

    def _is_remote_outage_error(self, exc: Exception) -> bool:
        return is_remote_outage_error(exc)

    def download_budgeted_forcing_record(self, source_id: str, *, strict: bool = True) -> dict[str, Any]:
        source_id = str(source_id).strip()
        forcing_factor = forcing_factor_id_for_source(source_id)
        return run_budgeted_forcing_provider_call(
            callable_path="src.services.ingestion._budgeted_ingestion_forcing_download",
            kwargs={
                "source_id": source_id,
                "output_dir": str(self.output_dir),
                "forcing_dir": str(self.forcing_dir),
                "start_date": self.start_date,
                "end_date": self.end_date,
                "strict": bool(strict),
            },
            source_id=source_id,
            forcing_factor=forcing_factor,
            failure_path=str(self.forcing_dir / forcing_factor),
        )

    def download_required_forcing_record(self, source_id: str) -> dict[str, Any]:
        return self.download_budgeted_forcing_record(source_id, strict=True)

    def _download_required_forcing_or_raise(self, source_id: str) -> dict[str, Any]:
        record = self.download_required_forcing_record(source_id)
        if record.get("status") in {"failed", "failed_remote_outage"}:
            raise ForcingProviderAcquisitionError(record)
        return record

    def _reuse_approved_for_source(self, source_id: str) -> bool:
        return os.environ.get(PREP_REUSE_APPROVED_SOURCE_ENV, "").strip() == str(source_id)

    def _reuse_prompt_already_consumed(self) -> bool:
        return os.environ.get(PREP_REUSE_APPROVED_ONCE_ENV, "").strip() == "1"

    def _handle_required_download_step(
        self,
        manifest: IngestionManifest,
        *,
        source_id: str,
        download_callable,
        allow_outage_skip: bool = False,
    ) -> dict[str, Any]:
        cached_record = self._validated_cache_hit(source_id)
        if cached_record is not None:
            return self._record_download(manifest, source_id, cached_record)

        try:
            result = download_callable()
        except Exception as exc:
            forcing_record = getattr(exc, "record", None)
            cache_validation = self._validate_cached_source(source_id)
            outage_detected = (
                bool(forcing_record.get("upstream_outage_detected", False))
                if isinstance(forcing_record, dict)
                else self._is_remote_outage_error(exc)
            )
            remote_error = (
                str(forcing_record.get("error") or str(exc))
                if isinstance(forcing_record, dict)
                else str(exc)
            )
            failure_stage = (
                str(forcing_record.get("failure_stage") or "")
                if isinstance(forcing_record, dict)
                else ""
            )
            elapsed_seconds = (
                forcing_record.get("elapsed_seconds")
                if isinstance(forcing_record, dict)
                else None
            )
            budget_seconds = (
                forcing_record.get("budget_seconds")
                if isinstance(forcing_record, dict)
                else None
            )
            budget_exhausted = (
                bool(forcing_record.get("budget_exhausted", False))
                if isinstance(forcing_record, dict)
                else False
            )
            if outage_detected:
                if allow_outage_skip:
                    factor_id = forcing_factor_id_for_source(source_id)
                    self.degraded_continue_used = True
                    self.upstream_outage_detected = True
                    self.missing_forcing_factors.add(factor_id)
                    self.skipped_forcing_sources.add(str(source_id))
                    return self._record_download(
                        manifest,
                        source_id,
                        {
                            "status": "skipped_outage_continue_degraded",
                            "source_id": source_id,
                            "path": str(self._canonical_cache_path(source_id)),
                            "forcing_factor": factor_id,
                            "upstream_outage_detected": True,
                            "elapsed_seconds": elapsed_seconds,
                            "budget_seconds": budget_seconds,
                            "budget_exhausted": budget_exhausted,
                            "failure_stage": failure_stage,
                            "validation": cache_validation if cache_validation.get("path") else None,
                            "remote_error": remote_error,
                        },
                    )
                if cache_validation.get("valid") and self._reuse_approved_for_source(source_id):
                    return self._record_download(
                        manifest,
                        source_id,
                        {
                            "status": "reused_validated_cache",
                            "path": cache_validation.get("path"),
                            "validation": cache_validation,
                            "remote_error": remote_error,
                            "elapsed_seconds": elapsed_seconds,
                            "budget_seconds": budget_seconds,
                            "budget_exhausted": budget_exhausted,
                            "failure_stage": failure_stage,
                            "reuse_mode": "outage_prompt_approved",
                        },
                    )
                if cache_validation.get("valid") and not self._reuse_prompt_already_consumed():
                    self._record_download(
                        manifest,
                        source_id,
                        {
                            "status": "awaiting_cache_reuse_decision",
                            "path": cache_validation.get("path"),
                            "validation": cache_validation,
                            "remote_error": remote_error,
                            "elapsed_seconds": elapsed_seconds,
                            "budget_seconds": budget_seconds,
                            "budget_exhausted": budget_exhausted,
                            "failure_stage": failure_stage,
                        },
                    )
                    raise PrepOutageDecisionRequired(
                        run_name=RUN_NAME,
                        source_id=source_id,
                        cache_path=str(cache_validation.get("path") or self._canonical_cache_path(source_id)),
                        validation=cache_validation,
                        error=remote_error,
                    ) from exc

                record = {
                    "status": "cancelled_no_cache",
                    "path": cache_validation.get("path"),
                    "validation": cache_validation,
                    "remote_error": remote_error,
                    "elapsed_seconds": elapsed_seconds,
                    "budget_seconds": budget_seconds,
                    "budget_exhausted": budget_exhausted,
                    "failure_stage": failure_stage,
                }
                if cache_validation.get("valid") and self._reuse_prompt_already_consumed():
                    record["status"] = "failed_after_reuse_prompt"
                    record["error"] = (
                        "Another required source outage occurred after the single allowed reuse decision for this prep run."
                    )
                self._record_download(manifest, source_id, record)
                raise DataLoadingError(
                    f"Required prep input '{source_id}' hit a remote-service outage and no reusable validated "
                    f"same-case cache is available for this run."
                ) from exc

            self._record_download(
                manifest,
                source_id,
                {
                    "status": forcing_record.get("status", "failed") if isinstance(forcing_record, dict) else "failed",
                    "path": str(self._canonical_cache_path(source_id)),
                    "error": remote_error,
                    "elapsed_seconds": elapsed_seconds,
                    "budget_seconds": budget_seconds,
                    "budget_exhausted": budget_exhausted,
                    "failure_stage": failure_stage,
                    "upstream_outage_detected": outage_detected,
                    "validation": cache_validation if cache_validation.get("path") else None,
                },
            )
            raise

        record = self._normalize_download_record(source_id, result)
        failure_statuses = {
            "failed",
            "skipped_no_creds",
            "skipped_no_lib",
            "skipped_no_data_found",
        }
        if record.get("status") in failure_statuses:
            self._record_download(manifest, source_id, record)
            raise DataLoadingError(
                f"Required prep input '{source_id}' did not complete successfully: {record.get('status')}"
            )
        return self._record_download(manifest, source_id, record)
        
    def run(self):
        """Execute the ingestion logic."""
        manifest = IngestionManifest(
            config=self._manifest_config()
        )
        self._persist_download_manifest(manifest)

        try:
            # 1. Download Drifters
            if self.case_context.drifter_required:
                self._handle_required_download_step(
                    manifest,
                    source_id="drifters",
                    download_callable=self.download_drifters,
                )
            else:
                logger.info(
                    "Skipping drifter download for %s because this workflow uses a frozen Phase 1 baseline.",
                    self.case_context.workflow_mode,
                )
                self._record_download(manifest, "drifters", "SKIPPED_FROZEN_PHASE1_BASELINE")
            manifest.config.update(self._manifest_config())
            self._persist_download_manifest(manifest)

            if self.case_context.is_official:
                self._handle_required_download_step(
                    manifest,
                    source_id="arcgis",
                    download_callable=self.download_arcgis_layers,
                )
                self._refresh_official_forcing_bbox_from_scoring_grid()
                self.gfs_downloader = GFSWindDownloader(
                    forcing_box=self.bbox,
                    expected_delta=pd.Timedelta(hours=PROTOTYPE_GFS_ANALYSIS_DELTA_HOURS),
                )
            manifest.config.update(self._manifest_config())
            self._persist_download_manifest(manifest)

            # 2. Download HYCOM
            self._handle_required_download_step(
                manifest,
                source_id="hycom",
                download_callable=lambda: self._download_required_forcing_or_raise("hycom"),
                allow_outage_skip=self.forcing_outage_policy == FORCING_OUTAGE_POLICY_CONTINUE_DEGRADED,
            )
            
            # 3. Download CMEMS
            self._handle_required_download_step(
                manifest,
                source_id="cmems",
                download_callable=lambda: self._download_required_forcing_or_raise("cmems"),
                allow_outage_skip=self.forcing_outage_policy == FORCING_OUTAGE_POLICY_CONTINUE_DEGRADED,
            )
            self._handle_required_download_step(
                manifest,
                source_id="cmems_wave",
                download_callable=lambda: self._download_required_forcing_or_raise("cmems_wave"),
                allow_outage_skip=self.forcing_outage_policy == FORCING_OUTAGE_POLICY_CONTINUE_DEGRADED,
            )
            
            # 4. Download ERA5
            self._handle_required_download_step(
                manifest,
                source_id="era5",
                download_callable=lambda: self._download_required_forcing_or_raise("era5"),
                allow_outage_skip=self.forcing_outage_policy == FORCING_OUTAGE_POLICY_CONTINUE_DEGRADED,
            )

            if self.case_context.is_prototype:
                self._record_download(manifest, "gfs", self.download_budgeted_forcing_record("gfs", strict=False))

            # 5. Download NCEP
            self._handle_required_download_step(
                manifest,
                source_id="ncep",
                download_callable=lambda: self._download_required_forcing_or_raise("ncep"),
                allow_outage_skip=self.forcing_outage_policy == FORCING_OUTAGE_POLICY_CONTINUE_DEGRADED,
            )

            if not self.case_context.is_official:
                self._handle_required_download_step(
                    manifest,
                    source_id="arcgis",
                    download_callable=self.download_arcgis_layers,
                )

            manifest.config.update(self._manifest_config())
            manifest_path = self._persist_download_manifest(manifest)

            prepared_manifest_path = self.write_prepared_input_manifest()
            logger.info("Prepared-input manifest saved to %s", prepared_manifest_path)
            logger.info("Ingestion complete. Download manifest saved to %s", manifest_path)
            return {
                "download_manifest": str(manifest_path),
                "prepared_input_manifest": str(prepared_manifest_path),
            }
                
            logger.info(f"✅ Ingestion complete. Manifest saved to {manifest_path}")
            
        except Exception as e:
            logger.error(f"❌ Ingestion pipeline failed: {e}")
            raise

    def write_prepared_input_manifest(self) -> Path:
        """Write case-local CSV and JSON manifests of the prepared inputs currently on disk."""
        manifest_path = get_prepared_input_manifest_path(RUN_NAME)
        manifest_json_path = get_prepared_input_manifest_json_path(RUN_NAME)
        download_payload: dict[str, Any] = {}
        if self._download_manifest_path().exists():
            try:
                with open(self._download_manifest_path(), "r", encoding="utf-8") as handle:
                    download_payload = json.load(handle) or {}
            except Exception:
                download_payload = {}
        download_records = dict((download_payload.get(RUN_NAME) or {}).get("downloads") or {})
        records = []
        for spec in get_prepared_input_specs(
            require_drifter=self.case_context.drifter_required,
            include_all_transport_forcing=True,
            run_name=RUN_NAME,
        ):
            path = Path(spec["path"])
            if not path.exists():
                continue

            created_at = datetime.fromtimestamp(path.stat().st_mtime).isoformat()
            source_id = self._source_id_for_prepared_spec(spec)
            download_record = dict(download_records.get(source_id) or {})
            record = {
                "label": str(spec["label"]),
                "source_id": source_id,
                "file_path": str(path),
                "workflow_mode": self.case_context.workflow_mode,
                "source_description": str(spec["source"]),
                "provider": str(download_record.get("provider") or ""),
                "source_url": str(download_record.get("source_url") or ""),
                "storage_tier": str(download_record.get("storage_tier") or PERSISTENT_LOCAL_INPUT_STORE),
                "local_storage_path": str(download_record.get("local_storage_path") or path),
                "download_status": str(download_record.get("status") or ("generated" if source_id == "" else "")),
                "reuse_action": str(download_record.get("reuse_action") or ("generated_manifest" if source_id == "" else "")),
                "validation_status": str(download_record.get("validation_status") or ("generated_from_validated_inputs" if source_id == "" else "")),
                "validation_summary": str(download_record.get("validation_summary") or ""),
                "creation_time": created_at,
            }
            if source_id == "arcgis" and not record["provider"]:
                record.update(self._source_metadata_for(source_id))
            if source_id == "drifters" and not record["provider"]:
                record.update(self._source_metadata_for(source_id))
            records.append(
                record
            )

        generated_at = datetime.now().isoformat()
        records.extend(
            [
                {
                    "label": "prepared_input_manifest_csv",
                    "source_id": "",
                    "file_path": str(manifest_path),
                    "workflow_mode": self.case_context.workflow_mode,
                    "source_description": "Generated prepared-input manifest (CSV)",
                    "provider": "",
                    "source_url": "",
                    "storage_tier": PERSISTENT_LOCAL_INPUT_STORE,
                    "local_storage_path": str(manifest_path),
                    "download_status": "generated",
                    "reuse_action": "generated_manifest",
                    "validation_status": "generated_from_validated_inputs",
                    "validation_summary": "",
                    "creation_time": generated_at,
                },
                {
                    "label": "prepared_input_manifest_json",
                    "source_id": "",
                    "file_path": str(manifest_json_path),
                    "workflow_mode": self.case_context.workflow_mode,
                    "source_description": "Generated prepared-input manifest (JSON)",
                    "provider": "",
                    "source_url": "",
                    "storage_tier": PERSISTENT_LOCAL_INPUT_STORE,
                    "local_storage_path": str(manifest_json_path),
                    "download_status": "generated",
                    "reuse_action": "generated_manifest",
                    "validation_status": "generated_from_validated_inputs",
                    "validation_summary": "",
                    "creation_time": generated_at,
                },
            ]
        )
        pd.DataFrame(records).to_csv(manifest_path, index=False)
        write_json(manifest_json_path, records)
        return manifest_path

    def download_drifters(self) -> str:
        """
        Download drifter observations for the active workflow case.
        Prototype mode preserves the weekly scan behavior.
        Official spill-case mode skips drifter download and consumes a frozen baseline.
        """
        if not self.case_context.drifter_required:
            logger.info(
                "Drifter download not required for %s; using frozen Phase 1 baseline.",
                self.case_context.workflow_mode,
            )
            return "SKIPPED_FROZEN_PHASE1_BASELINE"

        cached = self._validated_cache_hit("drifters")
        if cached is not None:
            return str(cached["path"])

        if self.case_context.drifter_mode == "fixed_drifter_segment_window":
            return self._download_fixed_segment_drifter()

        logger.info("Scanning for NOAA Drifter data...")

        for date_str in self.drifter_search_dates:
            base_date = datetime.strptime(date_str, "%Y-%m-%d")

            scan_offsets = [0] if self.case_context.is_official else range(53)
            for week in scan_offsets:
                current_start = base_date + pd.Timedelta(weeks=week)
                if self.case_context.is_official:
                    current_end = pd.to_datetime(self.case_context.forcing_end_date)
                else:
                    current_end = current_start + pd.Timedelta(hours=72)

                start_str = current_start.strftime("%Y-%m-%d")
                end_str = pd.to_datetime(current_end).strftime("%Y-%m-%d")

                logger.info(f"Scanning Window: {start_str} to {end_str}")

                try:
                    e = ERDDAP(
                        server="https://osmc.noaa.gov/erddap",
                        protocol="tabledap",
                    )
                    e.dataset_id = "drifter_6hour_qc"
                    
                    e.constraints = {
                        "time>=": f"{start_str}T00:00:00Z",
                        "time<=": f"{end_str}T23:59:59Z",
                        "latitude>=": self.bbox[2],
                        "latitude<=": self.bbox[3],
                        "longitude>=": self.bbox[0],
                        "longitude<=": self.bbox[1],
                    }
                    e.variables = ["time", "latitude", "longitude", "ID", "ve", "vn"]
                    
                    df = e.to_pandas()
                    
                    if df.empty:
                        continue
                    
                    logger.info(f"Found {len(df)} drifter points in window {start_str}")
                    self._apply_forcing_window(
                        f"{start_str}T00:00:00Z",
                        f"{end_str}T00:00:00Z",
                    )
                    
                    # Normalize column names
                    df = df.rename(columns={
                        "latitude (degrees_north)": "lat",
                        "longitude (degrees_east)": "lon",
                        "time (UTC)": "time"
                    })
                    
                    output_path = self.drifter_dir / "drifters_noaa.csv"
                    df.to_csv(output_path, index=False)
                    return str(output_path)

                except Exception as e:
                    err_str = str(e)
                    if "503" in err_str or "502" in err_str or "504" in err_str:
                        raise RuntimeError(f"ERDDAP server unavailable. NOAA servers are experiencing an outage: {err_str}")
                    elif "10060" in err_str or "Timeout" in err_str:
                        raise RuntimeError(f"ERDDAP server timed out. NOAA servers are experiencing an outage: {err_str}")
                    logger.warning(f"No data found for window {start_str} to {end_str}.")
                    pass

        logger.warning("No drifters found.")
        return "SKIPPED_NO_DATA_FOUND"

    def _normalize_drifter_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        return df.rename(
            columns={
                "latitude (degrees_north)": "lat",
                "longitude (degrees_east)": "lon",
                "time (UTC)": "time",
            }
        )

    def _download_fixed_segment_drifter(self) -> str:
        if not self.case_context.configured_drifter_id:
            raise RuntimeError(
                f"{self.case_context.workflow_mode} requires configured_drifter_id for exact-segment acquisition."
            )

        start_ts = _normalize_utc_timestamp(self.case_context.release_start_utc)
        end_ts = _normalize_utc_timestamp(self.case_context.simulation_end_utc)
        logger.info(
            "Fetching exact NOAA drifter segment for %s: ID=%s, %s -> %s",
            self.case_context.run_name,
            self.case_context.configured_drifter_id,
            start_ts.isoformat(),
            end_ts.isoformat(),
        )

        try:
            erddap = ERDDAP(
                server="https://osmc.noaa.gov/erddap",
                protocol="tabledap",
            )
            erddap.dataset_id = "drifter_6hour_qc"
            erddap.constraints = {
                "time>=": start_ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "time<=": end_ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "latitude>=": self.bbox[2],
                "latitude<=": self.bbox[3],
                "longitude>=": self.bbox[0],
                "longitude<=": self.bbox[1],
            }
            erddap.variables = ["time", "latitude", "longitude", "ID", "ve", "vn"]
            df = erddap.to_pandas()
        except Exception as exc:
            err_str = str(exc)
            if any(token in err_str for token in ("503", "502", "504", "10060", "Timeout")):
                raise RuntimeError(f"ERDDAP server unavailable while fetching fixed drifter segment: {err_str}")
            raise

        if df.empty:
            raise RuntimeError(
                f"No NOAA drifter rows were returned for {self.case_context.run_name} "
                f"({start_ts.strftime('%Y-%m-%dT%H:%M:%SZ')} -> {end_ts.strftime('%Y-%m-%dT%H:%M:%SZ')})."
            )

        df = self._normalize_drifter_columns(df)
        if "time" not in df.columns or "ID" not in df.columns:
            raise RuntimeError(
                f"NOAA drifter response for {self.case_context.run_name} is missing required columns."
            )

        df["ID"] = df["ID"].astype(str).str.strip()
        df["time"] = pd.to_datetime(df["time"], utc=True)
        segment_df = df[df["ID"] == str(self.case_context.configured_drifter_id)].copy()
        if segment_df.empty:
            raise RuntimeError(
                f"Configured drifter_id={self.case_context.configured_drifter_id} was not present in the NOAA response "
                f"for {self.case_context.run_name}."
            )

        segment_df = segment_df.sort_values("time").reset_index(drop=True)
        expected_times = pd.date_range(start_ts, end_ts, freq="6H", tz="UTC")
        actual_times = pd.DatetimeIndex(segment_df["time"])
        if len(segment_df) != len(expected_times) or not actual_times.equals(expected_times):
            raise RuntimeError(
                f"{self.case_context.run_name} did not return the exact configured 6-hour drifter segment. "
                f"Expected {len(expected_times)} rows for ID={self.case_context.configured_drifter_id}, "
                f"got {len(segment_df)}."
            )

        segment_df["time"] = segment_df["time"].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        output_path = self.drifter_dir / "drifters_noaa.csv"
        segment_df.to_csv(output_path, index=False)
        self._apply_forcing_window(self.case_context.forcing_start_utc, self.case_context.forcing_end_utc)
        return str(output_path)

    def download_gfs(
        self,
        *,
        strict: bool = False,
        budget_seconds: int | float | None = None,
    ) -> dict[str, Any] | str:
        output_path = self.forcing_dir / "gfs_wind.nc"
        cached = self._validated_cache_hit("gfs")
        if cached is not None:
            return cached
        output_path.unlink(missing_ok=True)
        try:
            return self.gfs_downloader.download(
                start_time=self.effective_forcing_start_utc,
                end_time=self.effective_forcing_end_utc,
                output_path=output_path,
                scratch_dir=self.forcing_dir,
                budget_seconds=budget_seconds,
            )
        except Exception as exc:
            if strict:
                raise
            logger.warning(
                "Optional GFS winds are unavailable for %s. Continuing without gfs_wind.nc. Reason: %s",
                self.case_context.workflow_mode,
                exc,
            )
            return {
                "status": "best_effort_failed",
                "error": str(exc),
                "path": str(output_path),
            }

    def download_hycom(self, *, raise_on_failure: bool = False) -> str:
        """Download HYCOM currents via OPeNDAP."""
        cached = self._validated_cache_hit("hycom")
        if cached is not None:
            return str(cached["path"])
        logger.info("Fetching HYCOM currents...")
        
        # Determine appropriate experiment based on year
        # HYCOM experiments change over time. This is a simplified mapping.
        # 56.3: Jul 2014 - Sep 2016 (Reanalysis)
        # 57.2: May 2016 - Feb 2017 (Reanalysis)
        # 92.8: 2017 - ...
        # 93.0: 2018 - Present
        
        year = datetime.strptime(self.start_date, "%Y-%m-%d").year
        month = datetime.strptime(self.start_date, "%Y-%m-%d").month
        
        # List of potential experiments to try
        candidates = []
        
        if year < 2014:
            candidates.append("https://tds.hycom.org/thredds/dodsC/GLBu0.08/expt_19.1")
            
        elif year < 2018:
            # 2014-2017 Range
            # Prioritize 56.3 for 2016 early/mid
            candidates.append("https://tds.hycom.org/thredds/dodsC/GLBv0.08/expt_56.3")
            candidates.append("https://tds.hycom.org/thredds/dodsC/GLBv0.08/expt_57.2")
        else:
            # 2018+
            candidates.append("https://tds.hycom.org/thredds/dodsC/GLBy0.08/expt_93.0")

        # Fallback: Try them all if year logic fails
        candidates.append("https://tds.hycom.org/thredds/dodsC/GLBy0.08/expt_93.0")
        
        output_path = self.forcing_dir / "hycom_curr.nc"
        temp_path = self.forcing_dir / "hycom_curr_download.nc"
        
        last_error: Exception | None = None
        for base_url in candidates:
            logger.info(f"Trying HYCOM source: {base_url}")
            try:
                # Time conversion helper (needed because decode_times=False often safer for remote HYCOM)
                # Re-enabling decode times for slicing convenience, but dropping problematic variables like 'tau'
                ds = xr.open_dataset(base_url, drop_variables=['tau']) 
                
                # Check if our time range is in this dataset
                ds_start = pd.to_datetime(ds.time[0].values)
                ds_end = pd.to_datetime(ds.time[-1].values)
                req_start = pd.to_datetime(self.start_date)
                req_end = pd.to_datetime(self.end_date)
                
                if req_end < ds_start or req_start > ds_end:
                    logger.info(f"Skipping {base_url} (Date range {ds_start.date()} to {ds_end.date()} does not cover request)")
                    continue
                
                subset = ds[['water_u', 'water_v']].sel(
                    time=slice(self.start_date, self.end_date),
                    lat=slice(self.bbox[2], self.bbox[3]),
                    lon=slice(self.bbox[0], self.bbox[1]),
                    depth=0 # Surface only
                )
                
                if subset.time.size == 0:
                     logger.warning(f"Slice resulted in empty Time dimension for {base_url}")
                     continue

                if temp_path.exists():
                    temp_path.unlink()
                subset.to_netcdf(temp_path)
                temp_path.replace(output_path)
                logger.info(f"Saved HYCOM data to {output_path}")
                return str(output_path)
                
            except Exception as e:
                last_error = e
                logger.warning(f"Failed download from {base_url}: {e}")
                continue
            finally:
                temp_path.unlink(missing_ok=True)

        logger.error("All HYCOM sources failed.")
        if raise_on_failure:
            if last_error is not None:
                raise last_error
            raise DataLoadingError("All HYCOM sources failed for the requested forcing window.")
        return "FAILED"

    def download_cmems(self, *, raise_on_failure: bool = False) -> str:
        """Download CMEMS currents using copernicusmarine client."""
        cached = self._validated_cache_hit("cmems")
        if cached is not None:
            return str(cached["path"])
        logger.info("Fetching CMEMS currents...")
        
        username = os.getenv("CMEMS_USERNAME")
        password = os.getenv("CMEMS_PASSWORD")
        
        if not username or not password:
            logger.warning("CMEMS credentials not found. Skipping.")
            return "SKIPPED_NO_CREDS"
            
        if not copernicusmarine:
            logger.warning("copernicusmarine library not installed.")
            return "SKIPPED_NO_LIB"

        output_path = self.forcing_dir / "cmems_curr.nc"
        temp_filename = "cmems_curr_download.nc"
        temp_path = self.forcing_dir / temp_filename

        # Determine if we need Multi-Year (Historical) or Analysis/Forecast (Recent)
        request_year = datetime.strptime(self.start_date, "%Y-%m-%d").year
        
        if request_year < 2022:
            # Global Ocean Physics Reanalysis (1993-2023ish)
            dataset_id = "cmems_mod_glo_phy_my_0.083deg_P1D-m"
            logger.info(f"Using Multi-Year dataset for year {request_year}")
        else:
            # Global Ocean Physics Analysis and Forecast (Recent)
            dataset_id = "cmems_mod_glo_phy-cur_anfc_0.083deg_P1D-m"
            logger.info(f"Using NRT dataset for year {request_year}")

        try:
            subset_kwargs = {
                "dataset_id": dataset_id,
                "minimum_longitude": self.bbox[0],
                "maximum_longitude": self.bbox[1],
                "minimum_latitude": self.bbox[2],
                "maximum_latitude": self.bbox[3],
                "start_datetime": f"{self.start_date}T00:00:00",
                "end_datetime": f"{self.end_date}T23:59:59",
                "maximum_depth": CMEMS_SURFACE_CURRENT_MAX_DEPTH_M,
                "variables": ["uo", "vo"],
                "output_filename": temp_filename,
                "output_directory": str(self.forcing_dir),
                "username": username,
                "password": password,
            }
            temp_path.unlink(missing_ok=True)
            # Prefer the current overwrite flag and fall back only for older client versions.
            try:
                copernicusmarine.subset(**subset_kwargs, overwrite=True)
            except TypeError:
                copernicusmarine.subset(**subset_kwargs, force_download=True)
            if not temp_path.exists():
                raise RuntimeError(f"CMEMS subset completed without producing {temp_path}.")
            temp_path.replace(output_path)
            logger.info(f"Saved CMEMS data to {output_path}")
            return str(output_path)
            
        except Exception as e:
            logger.error(f"CMEMS Download failed: {e}")
            temp_path.unlink(missing_ok=True)
            if raise_on_failure:
                raise
            return "FAILED"

    def download_cmems_wave(self, *, raise_on_failure: bool = False) -> str:
        """Download CMEMS wave/Stokes forcing for official and prototype transport runs."""
        cached = self._validated_cache_hit("cmems_wave")
        if cached is not None:
            return str(cached["path"])
        logger.info("Fetching CMEMS wave/Stokes forcing...")

        username = os.getenv("CMEMS_USERNAME")
        password = os.getenv("CMEMS_PASSWORD")

        if not username or not password:
            logger.warning("CMEMS credentials not found. Skipping wave download.")
            return "SKIPPED_NO_CREDS"

        if not copernicusmarine:
            logger.warning("copernicusmarine library not installed; skipping wave download.")
            return "SKIPPED_NO_LIB"

        output_path = self.forcing_dir / "cmems_wave.nc"
        temp_filename = "cmems_wave_download.nc"
        temp_path = self.forcing_dir / temp_filename

        request_year = datetime.strptime(self.start_date, "%Y-%m-%d").year
        if request_year < 2022:
            dataset_id = "cmems_mod_glo_wav_my_0.2deg_PT3H-i"
            logger.info("Using multi-year CMEMS wave dataset for year %s", request_year)
        else:
            dataset_id = "cmems_mod_glo_wav_anfc_0.083deg_PT3H-i"
            logger.info("Using analysis/forecast CMEMS wave dataset for year %s", request_year)

        try:
            temp_path.unlink(missing_ok=True)
            copernicusmarine.subset(
                dataset_id=dataset_id,
                minimum_longitude=self.bbox[0],
                maximum_longitude=self.bbox[1],
                minimum_latitude=self.bbox[2],
                maximum_latitude=self.bbox[3],
                start_datetime=f"{self.start_date}T00:00:00",
                end_datetime=f"{self.end_date}T23:59:59",
                variables=["VHM0", "VSDX", "VSDY"],
                output_filename=temp_filename,
                output_directory=str(self.forcing_dir),
                overwrite=True,
                username=username,
                password=password,
            )
            if not temp_path.exists():
                raise RuntimeError(f"CMEMS wave subset completed without producing {temp_path}.")
            temp_path.replace(output_path)
            logger.info("Saved CMEMS wave/Stokes data to %s", output_path)
            return str(output_path)
        except Exception as e:
            logger.error("CMEMS wave download failed: %s", e)
            temp_path.unlink(missing_ok=True)
            if raise_on_failure:
                raise
            return "FAILED"

    def download_era5(self, *, raise_on_failure: bool = False) -> str:
        """Download ERA5 winds and fix 'valid_time' dimension issue."""
        cached = self._validated_cache_hit("era5")
        if cached is not None:
            return str(cached["path"])
        logger.info("Fetching ERA5 winds...")
        
        url = os.getenv("CDS_URL")
        key = os.getenv("CDS_KEY")
        
        if not url or not key:
            logger.warning("CDS credentials not found. Skipping.")
            return "SKIPPED_NO_CREDS"
        
        if not cdsapi:
            logger.warning("cdsapi library not installed.")
            return "SKIPPED_NO_LIB"

        # USE A TEMP PATH TO AVOID PERMISSION ERRORS
        final_path = self.forcing_dir / "era5_wind.nc"
        temp_path = self.forcing_dir / "era5_temp.nc"
        final_temp_path = self.forcing_dir / "era5_wind_download.nc"
        
        try:
            c = cdsapi.Client(url=url, key=key)
            
            # 1. Download to TEMP file
            c.retrieve(
                'reanalysis-era5-single-levels',
                {
                    'product_type': 'reanalysis',
                    'variable': ['10m_u_component_of_wind', '10m_v_component_of_wind'],
                    'date': f"{self.start_date}/{self.end_date}",
                    'time': [f"{i:02d}:00" for i in range(24)],
                    'area': [self.bbox[3], self.bbox[0], self.bbox[2], self.bbox[1]],
                    'format': 'netcdf',
                },
                str(temp_path)
            )

            # 2. Fix Variable Names (valid_time -> time)
            logger.info("Standardizing ERA5 structure...")
            
            with xr.open_dataset(temp_path) as ds:
                ds.load() # Load to RAM
                
                rename_map = {}
                # Fix dimensions
                if 'valid_time' in ds.dims or 'valid_time' in ds.variables:
                    rename_map['valid_time'] = 'time'
                
                # Fix variables
                if 'u10' in ds.variables: rename_map['u10'] = 'x_wind'
                if 'v10' in ds.variables: rename_map['v10'] = 'y_wind'
                
                if rename_map:
                    ds = ds.rename(rename_map)
                    logger.info(f"✅ Renamed: {rename_map}")
                
                # Save to a temporary final path so a stale-good cache survives a remote outage.
                final_temp_path.unlink(missing_ok=True)
                ds.to_netcdf(final_temp_path)

            # 3. Cleanup Temp
            if temp_path.exists():
                temp_path.unlink()

            # 4. FIX METADATA (Standard Names & Encoding)
            # This ensures OpenDrift detects 'eastward_wind' automatically
            fix_metadata(str(final_temp_path))
            final_temp_path.replace(final_path)

            logger.info(f"Saved fixed ERA5 data to {final_path}")
            return str(final_path)
            
        except Exception as e:
            logger.error(f"ERA5 Download failed: {e}")
            if temp_path.exists():
                temp_path.unlink()
            final_temp_path.unlink(missing_ok=True)
            if raise_on_failure:
                raise
            return "FAILED"

    def download_ncep(self, *, raise_on_failure: bool = False) -> str:
        """
        Download NCEP/NCAR Reanalysis 1 Winds (Historical Baseline).
        Ref: https://psl.noaa.gov/data/gridded/data.ncep.reanalysis.surface.html
        """
        cached = self._validated_cache_hit("ncep")
        if cached is not None:
            return str(cached["path"])
        logger.info("Fetching NCEP/NCAR Reanalysis 1 Winds (NOAA PSL)...")
        
        # Get year from start_date
        year = datetime.strptime(self.start_date, "%Y-%m-%d").year

        # Correct OPeNDAP URLs for NCEP Reanalysis 1 (Surface Daily)
        # These files are extremely stable.
        variables = {
            "uwnd": f"https://psl.noaa.gov/thredds/dodsC/Datasets/ncep.reanalysis/surface/uwnd.sig995.{year}.nc",
            "vwnd": f"https://psl.noaa.gov/thredds/dodsC/Datasets/ncep.reanalysis/surface/vwnd.sig995.{year}.nc"
        }

        output_path = self.forcing_dir / "ncep_wind.nc"
        temp_path = self.forcing_dir / "ncep_wind_download.nc"

        try:
            ds_list = []
            for var_name, url in variables.items():
                logger.info(f"Opening remote {var_name}...")
                
                # Open remote file
                with xr.open_dataset(url) as ds:
                    # Subset Time and Region
                    # NCEP 1 uses 0..360 Lon, so we might need to adjust if bbox is negative.
                    # Philippines (110-130) is positive, so it's fine.
                    
                    subset = ds[var_name].sel(
                        time=slice(self.start_date, self.end_date),
                        lat=slice(self.bbox[3], self.bbox[2]), 
                        lon=slice(self.bbox[0], self.bbox[1])
                    )
                    ds_list.append(subset)

            logger.info("Merging U/V components...")
            merged = xr.merge(ds_list)
            
            # Rename for OpenDrift (uwnd -> x_wind)
            merged = merged.rename({'uwnd': 'x_wind', 'vwnd': 'y_wind'})
            
            temp_path.unlink(missing_ok=True)
            merged.to_netcdf(temp_path)
             
            # FIX METADATA (Standard Names & Encoding)
            # This ensures OpenDrift detects 'eastward_wind' automatically
            fix_metadata(str(temp_path))
            temp_path.replace(output_path)
            
            logger.info(f"Saved NCEP data to {output_path}")
            return str(output_path)

        except Exception as e:
            logger.error(f"NCEP Download failed: {e}")
            temp_path.unlink(missing_ok=True)
            if raise_on_failure:
                raise
            return "FAILED"

    def download_arcgis_layers(self) -> str:
        """ArcGIS ingestion resolved directly from the configured workflow case."""
        cached = self._validated_cache_hit("arcgis")
        if cached is not None:
            return str(cached["path"])
        from src.helpers.scoring import OFFICIAL_GRID_CRS, build_official_scoring_grid
        from src.services.arcgis import (
            ArcGISFeatureServerClient,
            get_arcgis_processing_report_path,
            get_arcgis_registry_path,
            get_configured_arcgis_layers,
            rasterize_prepared_layer,
        )

        workflow_layers = get_configured_arcgis_layers()

        if not workflow_layers:
            logger.info("No ArcGIS layers resolved from the project case set; skipping.")
            return "SKIPPED_NO_LAYERS"

        client = ArcGISFeatureServerClient(timeout=60)
        prepared_layers = []
        for layer in workflow_layers:
            try:
                logger.info("Downloading ArcGIS layer: %s (ID: %s)", layer.name, layer.layer_id)
                target_crs = OFFICIAL_GRID_CRS if self.case_context.is_official else "EPSG:4326"
                prepared_layers.append(
                    client.prepare_layer(
                        layer=layer,
                        target_crs=target_crs,
                        grid=self.grid if self.case_context.is_prototype else None,
                    )
                )
            except Exception as e:
                logger.error(f"ArcGIS ingestion failed for layer {layer.name}: {e}")
                raise ArcGISLayerIngestionError(str(e)) from e

        if self.case_context.is_official:
            build_official_scoring_grid(force_refresh=True)
            self.grid = GridBuilder()
            prepared_layers = [rasterize_prepared_layer(layer_result, self.grid) for layer_result in prepared_layers]

        registry_rows = [layer_result.to_registry_row() for layer_result in prepared_layers]
        report_rows = [layer_result.to_processing_report_row() for layer_result in prepared_layers]
        pd.DataFrame(registry_rows).to_csv(get_arcgis_registry_path(RUN_NAME), index=False)
        pd.DataFrame(report_rows).to_csv(get_arcgis_processing_report_path(RUN_NAME), index=False)

        records = [layer_result.name for layer_result in prepared_layers]
        return {
            "status": "downloaded" if records else "skipped_no_data",
            "path": str(get_arcgis_registry_path(RUN_NAME)),
            "layer_names": records,
            "source_url": ";".join(sorted({str(layer.service_url) for layer in workflow_layers if str(layer.service_url).strip()})),
        }

if __name__ == "__main__":
    # Setup basic console logging for standalone run
    logging.basicConfig(level=logging.INFO)
    service = DataIngestionService()
    service.run()
