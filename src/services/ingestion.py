"""
Data Ingestion Service.
Automates downloading of forcing data (Currents, Winds) and Drifter observations.
"""

import os
import json
import logging
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
from src.core.constants import RUN_NAME
from src.core.base import BaseService
from src.exceptions.custom import DataLoadingError
from src.models.ingestion import IngestionManifest
from src.helpers.raster import GridBuilder
from src.utils.gfs_wind import GFSWindDownloader
from src.utils.io import get_prepared_input_manifest_path, get_prepared_input_specs

# Setup logging
logger = logging.getLogger(__name__)
OFFICIAL_FORCING_HALO_DEGREES_DEFAULT = 0.5
PROTOTYPE_FORCING_HALO_HOURS_DEFAULT = 3.0
PROTOTYPE_GFS_ANALYSIS_DELTA_HOURS = 6


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

class DataIngestionService(BaseService):
    """
    Service for downloading Ocean/Met forcing and Drifter data.
    """

    def __init__(self, output_dir: str = 'data'):
        self.case_context = get_case_context()
        self._assert_pipeline_role()
        self.case_config = self._load_case_config()
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
            prototype_display_domain = list(self.case_context.legacy_prototype_display_domain)
            self.bbox = [
                prototype_display_domain[0] - pad,
                prototype_display_domain[1] + pad,
                prototype_display_domain[2] - pad,
                prototype_display_domain[3] + pad,
            ]
            self.bbox_source = "legacy_prototype_display_domain_plus_3deg_pad"
        self.grid = GridBuilder() if self.case_context.is_prototype else None
        self.gfs_downloader = GFSWindDownloader(
            forcing_box=self.bbox,
            expected_delta=pd.Timedelta(hours=PROTOTYPE_GFS_ANALYSIS_DELTA_HOURS),
        )
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
            with open(ensemble_path, "r", encoding="utf-8") as handle:
                ensemble_cfg = yaml.safe_load(handle) or {}
            perturbations = ensemble_cfg.get("perturbations") or {}
            if perturbations.get("time_shift_hours") is not None:
                return float(perturbations["time_shift_hours"])

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
        }
        
    def run(self):
        """Execute the ingestion logic."""
        manifest = IngestionManifest(
            config=self._manifest_config()
        )

        try:
            # 1. Download Drifters
            if self.case_context.drifter_required:
                manifest.downloads["drifters"] = self.download_drifters()
            else:
                logger.info(
                    "Skipping drifter download for %s because this workflow uses a frozen Phase 1 baseline.",
                    self.case_context.workflow_mode,
                )
                manifest.downloads["drifters"] = "SKIPPED_FROZEN_PHASE1_BASELINE"
            manifest.config.update(self._manifest_config())

            if self.case_context.is_official:
                manifest.downloads["arcgis"] = self.download_arcgis_layers()
                self._refresh_official_forcing_bbox_from_scoring_grid()
                self.gfs_downloader = GFSWindDownloader(
                    forcing_box=self.bbox,
                    expected_delta=pd.Timedelta(hours=PROTOTYPE_GFS_ANALYSIS_DELTA_HOURS),
                )
                manifest.config.update(self._manifest_config())
            
            # 2. Download HYCOM
            manifest.downloads["hycom"] = self.download_hycom()
            
            # 3. Download CMEMS
            manifest.downloads["cmems"] = self.download_cmems()
            manifest.downloads["cmems_wave"] = self.download_cmems_wave()
            
            # 4. Download ERA5
            manifest.downloads["era5"] = self.download_era5()

            if self.case_context.is_prototype:
                manifest.downloads["gfs"] = self.download_gfs(strict=False)

            # 5. Download NCEP
            manifest.downloads["ncep"] = self.download_ncep()

            if not self.case_context.is_official:
                manifest.downloads["arcgis"] = self.download_arcgis_layers()

            # Save Manifest
            manifest_path = self.output_dir / "download_manifest.json"
            
            all_manifests = {}
            if manifest_path.exists():
                try:
                    with open(manifest_path, 'r') as f:
                        all_manifests = json.load(f)
                except Exception:
                    pass
                    
            all_manifests[RUN_NAME] = manifest.__dict__
            
            # Helper to serialize dataclass
            with open(manifest_path, 'w') as f:
                json.dump(all_manifests, f, indent=2, default=str)

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
        """Write a case-local manifest of the prepared inputs currently on disk."""
        manifest_path = get_prepared_input_manifest_path(RUN_NAME)
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
            records.append(
                {
                    "file_path": str(path),
                    "source": spec["source"],
                    "creation_time": created_at,
                    "workflow_mode": self.case_context.workflow_mode,
                }
            )

        records.append(
            {
                "file_path": str(manifest_path),
                "source": "Generated prepared-input manifest",
                "creation_time": datetime.now().isoformat(),
                "workflow_mode": self.case_context.workflow_mode,
            }
        )
        pd.DataFrame(records).to_csv(manifest_path, index=False)
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

    def download_gfs(self, *, strict: bool = False) -> dict[str, Any] | str:
        output_path = self.forcing_dir / "gfs_wind.nc"
        try:
            return self.gfs_downloader.download(
                start_time=self.effective_forcing_start_utc,
                end_time=self.effective_forcing_end_utc,
                output_path=output_path,
                scratch_dir=self.forcing_dir,
            )
        except Exception as exc:
            if strict:
                raise
            logger.warning(
                "Best-effort GFS wind prep failed for prototype workflow %s: %s",
                self.case_context.workflow_mode,
                exc,
            )
            return {
                "status": "best_effort_failed",
                "error": str(exc),
                "path": str(output_path),
            }

    def download_hycom(self) -> str:
        """Download HYCOM currents via OPeNDAP."""
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

                subset.to_netcdf(output_path)
                logger.info(f"Saved HYCOM data to {output_path}")
                return str(output_path)
                
            except Exception as e:
                logger.warning(f"Failed download from {base_url}: {e}")
                continue

        logger.error("All HYCOM sources failed.")
        return "FAILED"

    def download_cmems(self) -> str:
        """Download CMEMS currents using copernicusmarine client."""
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
        
        # Explicitly delete existing file to prevent (1) suffix or read errors
        if output_path.exists():
            output_path.unlink()
            logger.info(f"Deleted existing CMEMS file: {output_path}")

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
            # Let's try the common ID for Global Ocean Physics Analysis and Forecast.
            copernicusmarine.subset(
                dataset_id=dataset_id,
                minimum_longitude=self.bbox[0],
                maximum_longitude=self.bbox[1],
                minimum_latitude=self.bbox[2],
                maximum_latitude=self.bbox[3],
                start_datetime=f"{self.start_date}T00:00:00",
                end_datetime=f"{self.end_date}T23:59:59",
                minimum_depth=0,
                maximum_depth=1,
                variables=["uo", "vo"], 
                output_filename="cmems_curr.nc",
                output_directory=str(self.forcing_dir),
                force_download=True,
                username=username,
                password=password
            )
            logger.info(f"Saved CMEMS data to {output_path}")
            return str(output_path)
            
        except Exception as e:
            logger.error(f"CMEMS Download failed: {e}")
            return "FAILED"

    def download_cmems_wave(self) -> str:
        """Download CMEMS wave/Stokes forcing for official and prototype transport runs."""
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
        if output_path.exists():
            output_path.unlink()
            logger.info("Deleted existing CMEMS wave file: %s", output_path)

        request_year = datetime.strptime(self.start_date, "%Y-%m-%d").year
        if request_year < 2022:
            dataset_id = "cmems_mod_glo_wav_my_0.2deg_PT3H-i"
            logger.info("Using multi-year CMEMS wave dataset for year %s", request_year)
        else:
            dataset_id = "cmems_mod_glo_wav_anfc_0.083deg_PT3H-i"
            logger.info("Using analysis/forecast CMEMS wave dataset for year %s", request_year)

        try:
            copernicusmarine.subset(
                dataset_id=dataset_id,
                minimum_longitude=self.bbox[0],
                maximum_longitude=self.bbox[1],
                minimum_latitude=self.bbox[2],
                maximum_latitude=self.bbox[3],
                start_datetime=f"{self.start_date}T00:00:00",
                end_datetime=f"{self.end_date}T23:59:59",
                variables=["VHM0", "VSDX", "VSDY"],
                output_filename="cmems_wave.nc",
                output_directory=str(self.forcing_dir),
                overwrite=True,
                username=username,
                password=password,
            )
            logger.info("Saved CMEMS wave/Stokes data to %s", output_path)
            return str(output_path)
        except Exception as e:
            logger.error("CMEMS wave download failed: %s", e)
            return "FAILED"

    def download_era5(self) -> str:
        """Download ERA5 winds and fix 'valid_time' dimension issue."""
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
                
                # Save to FINAL path (No locking issue!)
                ds.to_netcdf(final_path)

            # 3. Cleanup Temp
            if temp_path.exists():
                temp_path.unlink()

            # 4. FIX METADATA (Standard Names & Encoding)
            # This ensures OpenDrift detects 'eastward_wind' automatically
            fix_metadata(str(final_path))

            logger.info(f"Saved fixed ERA5 data to {final_path}")
            return str(final_path)
            
        except Exception as e:
            logger.error(f"ERA5 Download failed: {e}")
            if temp_path.exists():
                temp_path.unlink()
            return "FAILED"

    def download_ncep(self) -> str:
        """
        Download NCEP/NCAR Reanalysis 1 Winds (Historical Baseline).
        Ref: https://psl.noaa.gov/data/gridded/data.ncep.reanalysis.surface.html
        """
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
            
            merged.to_netcdf(output_path)
            
            # FIX METADATA (Standard Names & Encoding)
            # This ensures OpenDrift detects 'eastward_wind' automatically
            fix_metadata(str(output_path))
            
            logger.info(f"Saved NCEP data to {output_path}")
            return str(output_path)

        except Exception as e:
            logger.error(f"NCEP Download failed: {e}")
            return "FAILED"

    def download_arcgis_layers(self) -> str:
        """ArcGIS ingestion resolved directly from the configured workflow case."""
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
        return ",".join(records) if records else "SKIPPED_NO_DATA"

if __name__ == "__main__":
    # Setup basic console logging for standalone run
    logging.basicConfig(level=logging.INFO)
    service = DataIngestionService()
    service.run()
