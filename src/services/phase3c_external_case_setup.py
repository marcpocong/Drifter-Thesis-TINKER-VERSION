"""Phase 3C external rich-data spill transfer-validation setup."""

from __future__ import annotations

import csv
import json
import math
import re
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import requests
import yaml

from src.core.case_context import get_case_context
from src.helpers.scoring import GEOGRAPHIC_CRS, ScoringGridSpec
from src.services.arcgis import clean_arcgis_geometries, compute_vector_area, compute_vector_centroid

try:
    import geopandas as gpd
except ImportError:  # pragma: no cover
    gpd = None

try:
    import rasterio
    from rasterio.features import rasterize as rio_rasterize
    from rasterio.transform import from_origin
except ImportError:  # pragma: no cover
    rasterio = None
    rio_rasterize = None
    from_origin = None

try:
    from shapely.geometry import box
except ImportError:  # pragma: no cover
    box = None


PHASE3C_DIR_NAME = "phase3c_external_case_setup"
PHASE3C_NAME = "Phase 3C – External Rich-Data Spill Transfer Validation"
PHASE3C_PLACEMENT = "after Phase 3B and before Phase 4"
RECOMMENDED_NEXT_BRANCH = "dwh_phase3c_forcing_adapter_and_non_scientific_smoke_forecast"

SOURCE_TAXONOMY_OBS = "observation_derived_quantitative"
SOURCE_TAXONOMY_MODELED = "modeled_forecast_exclude_from_truth"
SOURCE_TAXONOMY_QUALITATIVE = "qualitative_context_only"

SELECTED_DWH_LAYER_IDS = (0, 5, 6, 7, 8)
GRID_BUFFER_M = 50000.0
GRID_SNAP_M = 1000.0
GRID_RESOLUTION_M = 1000.0
REQUEST_TIMEOUT_SECONDS = 120

DWH_FEATURE_SERVER_ROOT = (
    "https://services1.arcgis.com/qr14biwnHA6Vis6l/ArcGIS/rest/services/"
    "Deepwaterhorizon_Oilspill_WM/FeatureServer"
)
NCEI_ERMA_ARCHIVE_BACKUP = "https://www.ncei.noaa.gov/archive/accession/ORR-DWH-ERMA-GIS"


@dataclass(frozen=True)
class ExternalCaseLayer:
    layer_key: str
    layer_id: int | None
    name: str
    local_name: str
    geometry_type: str
    role: str
    event_date: str
    source_taxonomy: str
    selected_for_phase3c: bool
    use_in_scoring_grid_extent: bool
    use_as_truth: bool
    reason: str


def _json_default(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, np.ndarray):
        return value.tolist()
    if isinstance(value, (np.integer,)):
        return int(value)
    if isinstance(value, (np.floating,)):
        return None if np.isnan(value) else float(value)
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


def _slugify(value: str) -> str:
    value = str(value).strip().lower()
    value = re.sub(r"[^a-z0-9]+", "_", value)
    return value.strip("_") or "layer"


def _as_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _normalize_geometry_type(value: str) -> str:
    lowered = str(value or "").strip().lower()
    if "point" in lowered:
        return "point"
    if "polygon" in lowered:
        return "polygon"
    return lowered or "unknown"


def classify_dwh_public_layer(layer_id: int | None, name: str, geometry_type: str = "") -> tuple[str, str]:
    """Classify DWH public layers for truth eligibility."""
    lowered = str(name or "").lower()
    if any(token in lowered for token in ("trajectory", "forecast", "model", "hindcast", "prediction")):
        return SOURCE_TAXONOMY_MODELED, "modeled/forecast wording detected; excluded from truth"
    if layer_id == 1 or "cumulative" in lowered:
        return SOURCE_TAXONOMY_QUALITATIVE, "cumulative composite is context-only unless separately justified"
    if layer_id == 0 or "wellhead" in lowered:
        return SOURCE_TAXONOMY_QUALITATIVE, "source-point provenance, not an observed spill-extent truth mask"
    if re.search(r"\bt20\d{6}[_ ]?composite\b", lowered) and "polygon" in _normalize_geometry_type(geometry_type):
        return SOURCE_TAXONOMY_OBS, "dated public daily composite polygon; observation-derived quantitative candidate"
    return SOURCE_TAXONOMY_QUALITATIVE, "not selected as a dated quantitative daily observation layer"


def build_external_case_service_inventory(cfg: dict) -> list[dict]:
    arcgis = cfg.get("arcgis") or {}
    forcing = cfg.get("forcing_services") or {}
    return [
        {
            "service_role": "public_observation_primary",
            "service_name": "DWH FeatureServer daily composites",
            "service_url": arcgis.get("feature_server_url", DWH_FEATURE_SERVER_ROOT),
            "access_method": "ArcGIS REST FeatureServer / GeoJSON query",
            "expected_temporal_coverage": "2010-05-17 through 2010-08-25 public daily/date-composite layers; Phase 3C uses 2010-05-20 through 2010-05-23",
            "already_compatible_with_current_repo_readers": "yes",
            "adapter_work_needed": "Dedicated Phase 3C layer registry and date-composite mask builder; no forecast reader change required for setup",
            "truth_policy": "daily composites are observation-derived quantitative candidates; cumulative composite is context-only",
        },
        {
            "service_role": "public_observation_backup",
            "service_name": "NCEI ERMA DWH GIS archive",
            "service_url": arcgis.get("erma_archive_backup_url", NCEI_ERMA_ARCHIVE_BACKUP),
            "access_method": "archive/file download",
            "expected_temporal_coverage": "Deepwater Horizon ERMA GIS archive products, used as backup/provenance expansion source",
            "already_compatible_with_current_repo_readers": "partial",
            "adapter_work_needed": "Add archive download/extract adapter if FeatureServer access becomes unavailable",
            "truth_policy": "only dated observation-derived vector/raster products may become truth masks",
        },
        {
            "service_role": "currents_primary",
            "service_name": (forcing.get("currents_primary") or {}).get("service_name", "HYCOM GOFS 3.1 reanalysis"),
            "service_url": (forcing.get("currents_primary") or {}).get("service_url", "https://www.hycom.org/dataserver/gofs-3pt1/reanalysis"),
            "access_method": (forcing.get("currents_primary") or {}).get("access_method", "THREDDS/OPeNDAP"),
            "expected_temporal_coverage": "retrospective Gulf of Mexico window covering 2010-05-20 through 2010-05-23",
            "already_compatible_with_current_repo_readers": "partial",
            "adapter_work_needed": "Point current reader at GOFS 3.1 reanalysis THREDDS catalogs and validate DWH-era variable/time naming",
            "truth_policy": "forcing only; never truth",
        },
        {
            "service_role": "winds_primary",
            "service_name": (forcing.get("winds_primary") or {}).get("service_name", "ERA5 hourly single levels"),
            "service_url": (forcing.get("winds_primary") or {}).get("service_url", "https://cds.climate.copernicus.eu/datasets/reanalysis-era5-single-levels"),
            "access_method": (forcing.get("winds_primary") or {}).get("access_method", "CDS API / file download"),
            "expected_temporal_coverage": "hourly 10 m winds covering 2010-05-20 through 2010-05-23",
            "already_compatible_with_current_repo_readers": "yes",
            "adapter_work_needed": "Use existing ERA5 path with DWH bbox/time window and credentials; verify variable names after download",
            "truth_policy": "forcing only; never truth",
        },
        {
            "service_role": "currents_fallback",
            "service_name": (forcing.get("currents_fallback") or {}).get("service_name", "Copernicus Marine Global Ocean Physics Reanalysis"),
            "service_url": (forcing.get("currents_fallback") or {}).get("service_url", "https://data.marine.copernicus.eu/product/GLOBAL_MULTIYEAR_PHY_001_030/description"),
            "access_method": (forcing.get("currents_fallback") or {}).get("access_method", "Copernicus Marine API / file download"),
            "expected_temporal_coverage": "global multi-year ocean physics reanalysis covering the 2010 DWH window",
            "already_compatible_with_current_repo_readers": "partial",
            "adapter_work_needed": "Confirm dataset ID, daily/hourly cadence, uo/vo naming, and DWH bbox subset behavior in current CMEMS reader",
            "truth_policy": "forcing only; never truth",
        },
        {
            "service_role": "waves_primary",
            "service_name": (forcing.get("waves_primary") or {}).get("service_name", "Copernicus Marine Global Ocean Waves Reanalysis"),
            "service_url": (forcing.get("waves_primary") or {}).get("service_url", "https://data.marine.copernicus.eu/product/GLOBAL_MULTIYEAR_WAV_001_032/description"),
            "access_method": (forcing.get("waves_primary") or {}).get("access_method", "Copernicus Marine API / file download"),
            "expected_temporal_coverage": "global multi-year wave/Stokes reanalysis covering the 2010 DWH window",
            "already_compatible_with_current_repo_readers": "partial",
            "adapter_work_needed": "Confirm historical wave dataset ID and Stokes variable names before enabling full forecast",
            "truth_policy": "forcing only; never truth",
        },
    ]


def build_external_case_forcing_manifest(cfg: dict) -> list[dict]:
    inventory = build_external_case_service_inventory(cfg)
    forcing_roles = {"currents_primary", "winds_primary", "currents_fallback", "waves_primary"}
    rows = []
    for row in inventory:
        if row["service_role"] not in forcing_roles:
            continue
        rows.append(
            {
                "forcing_component": row["service_role"],
                "chosen_service": row["service_name"],
                "service_url": row["service_url"],
                "access_method": row["access_method"],
                "expected_temporal_coverage": row["expected_temporal_coverage"],
                "already_compatible_with_current_repo_readers": row["already_compatible_with_current_repo_readers"],
                "adapter_work_needed": row["adapter_work_needed"],
                "phase3c_status": "readiness_manifest_only_no_forecast_run",
            }
        )
    return rows


def parse_external_case_layer_registry(cfg: dict) -> list[ExternalCaseLayer]:
    layers_cfg = ((cfg.get("arcgis") or {}).get("external_case_layers") or {})
    layers: list[ExternalCaseLayer] = []
    for layer_key, layer_cfg in layers_cfg.items():
        layer_id = layer_cfg.get("layer_id")
        layer_id = None if layer_id in ("", None) else int(layer_id)
        name = str(layer_cfg.get("name") or layer_key)
        geometry_type = _normalize_geometry_type(layer_cfg.get("geometry_type", ""))
        taxonomy = str(layer_cfg.get("source_taxonomy") or "")
        if taxonomy:
            _, reason = classify_dwh_public_layer(layer_id, name, geometry_type)
        else:
            taxonomy, reason = classify_dwh_public_layer(layer_id, name, geometry_type)
        layers.append(
            ExternalCaseLayer(
                layer_key=str(layer_key),
                layer_id=layer_id,
                name=name,
                local_name=str(layer_cfg.get("local_name") or _slugify(name)),
                geometry_type=geometry_type,
                role=str(layer_cfg.get("role") or layer_key),
                event_date=str(layer_cfg.get("event_date") or ""),
                source_taxonomy=taxonomy,
                selected_for_phase3c=_as_bool(layer_cfg.get("selected_for_phase3c")),
                use_in_scoring_grid_extent=_as_bool(layer_cfg.get("use_in_scoring_grid_extent")),
                use_as_truth=_as_bool(layer_cfg.get("use_as_truth")),
                reason=reason,
            )
        )

    selected_order = {layer_id: idx for idx, layer_id in enumerate(SELECTED_DWH_LAYER_IDS)}
    return sorted(
        layers,
        key=lambda layer: (
            0 if layer.selected_for_phase3c else 1,
            selected_order.get(layer.layer_id, 999),
            layer.layer_id if layer.layer_id is not None else 9999,
            layer.layer_key,
        ),
    )


def selected_dwh_layers_from_config(config_path: str | Path = "config/case_dwh_retro_2010_72h.yaml") -> list[ExternalCaseLayer]:
    cfg = _load_yaml(Path(config_path))
    return [layer for layer in parse_external_case_layer_registry(cfg) if layer.selected_for_phase3c]


def derive_projected_crs_from_wgs84(gdfs: list["gpd.GeoDataFrame"]) -> str:
    if gpd is None:
        raise ImportError("geopandas is required to derive the external-case CRS")
    bounds_parts = []
    for gdf in gdfs:
        if gdf is None or gdf.empty:
            continue
        work = gdf
        if work.crs is None:
            work = work.set_crs(GEOGRAPHIC_CRS)
        elif str(work.crs).upper() != GEOGRAPHIC_CRS:
            work = work.to_crs(GEOGRAPHIC_CRS)
        valid = work.dropna(subset=["geometry"])
        if not valid.empty:
            bounds_parts.append(valid.total_bounds)
    if not bounds_parts:
        raise ValueError("Cannot derive projected CRS without at least one valid WGS84 geometry.")

    stacked = np.asarray(bounds_parts, dtype=float)
    min_lon = float(np.min(stacked[:, 0]))
    min_lat = float(np.min(stacked[:, 1]))
    max_lon = float(np.max(stacked[:, 2]))
    max_lat = float(np.max(stacked[:, 3]))
    centroid_lon = (min_lon + max_lon) / 2.0
    centroid_lat = (min_lat + max_lat) / 2.0
    zone = int(math.floor((centroid_lon + 180.0) / 6.0) + 1)
    zone = max(1, min(zone, 60))
    epsg = (32600 if centroid_lat >= 0 else 32700) + zone
    return f"EPSG:{epsg}"


def build_scoring_grid_spec_from_projected_gdfs(
    gdfs: list["gpd.GeoDataFrame"],
    *,
    target_crs: str,
    output_dir: str | Path,
    workflow_mode: str = "dwh_retro_2010",
    run_name: str = "CASE_DWH_RETRO_2010_72H",
    source_paths: dict[str, str] | None = None,
) -> ScoringGridSpec:
    if gpd is None or box is None:
        raise ImportError("geopandas and shapely are required to build the DWH scoring grid")

    geometries = []
    for gdf in gdfs:
        if gdf is None or gdf.empty:
            continue
        work = gdf if str(gdf.crs).upper() == str(target_crs).upper() else gdf.to_crs(target_crs)
        geometries.extend([geom for geom in work.geometry.dropna() if geom is not None and not geom.is_empty])
    if not geometries:
        raise ValueError("Cannot build scoring grid without valid projected geometries.")

    union = _union_geometries(gpd.GeoSeries(geometries, crs=target_crs))
    buffered = union.buffer(GRID_BUFFER_M)
    min_x, min_y, max_x, max_y = buffered.bounds
    snapped_min_x = float(math.floor(min_x / GRID_SNAP_M) * GRID_SNAP_M)
    snapped_min_y = float(math.floor(min_y / GRID_SNAP_M) * GRID_SNAP_M)
    snapped_max_x = float(math.ceil(max_x / GRID_SNAP_M) * GRID_SNAP_M)
    snapped_max_y = float(math.ceil(max_y / GRID_SNAP_M) * GRID_SNAP_M)

    extent_wgs84 = gpd.GeoDataFrame(
        [{"name": "dwh_phase3c_scoring_grid_extent"}],
        geometry=[box(snapped_min_x, snapped_min_y, snapped_max_x, snapped_max_y)],
        crs=target_crs,
    ).to_crs(GEOGRAPHIC_CRS)
    display_min_lon, display_min_lat, display_max_lon, display_max_lat = extent_wgs84.total_bounds
    out = Path(output_dir)
    return ScoringGridSpec(
        min_x=snapped_min_x,
        max_x=snapped_max_x,
        min_y=snapped_min_y,
        max_y=snapped_max_y,
        resolution=GRID_RESOLUTION_M,
        crs=target_crs,
        x_name="x",
        y_name="y",
        units="meters",
        workflow_mode=workflow_mode,
        run_name=run_name,
        display_bounds_wgs84=[
            float(display_min_lon),
            float(display_max_lon),
            float(display_min_lat),
            float(display_max_lat),
        ],
        buffer_m=GRID_BUFFER_M,
        grid_snap_m=GRID_SNAP_M,
        metadata_path=str(out / "scoring_grid.yaml"),
        metadata_json_path=str(out / "scoring_grid.json"),
        template_path=str(out / "scoring_grid_template.tif"),
        extent_path=str(out / "grid_extent.gpkg"),
        land_mask_path=str(out / "land_mask.tif"),
        sea_mask_path=str(out / "sea_mask.tif"),
        shoreline_segments_path=str(out / "shoreline_segments.gpkg"),
        shoreline_mask_manifest_json_path=str(out / "shoreline_mask_manifest.json"),
        shoreline_mask_manifest_csv_path=str(out / "shoreline_mask_manifest.csv"),
        shoreline_mask_status="pending_shoreline_mask_generation",
        shoreline_mask_signature="",
        source_paths=source_paths or {},
    )


def _write_template_raster(spec: ScoringGridSpec) -> None:
    if rasterio is None or from_origin is None:
        raise ImportError("rasterio is required to write the DWH scoring grid template")
    path = Path(spec.template_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    data = np.zeros((spec.height, spec.width), dtype=np.uint8)
    transform = from_origin(spec.min_x, spec.max_y, spec.resolution, spec.resolution)
    if path.exists():
        path.unlink()
    with rasterio.open(
        path,
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


def _write_grid_extent(spec: ScoringGridSpec) -> None:
    if gpd is None or box is None:
        raise ImportError("geopandas and shapely are required to write the DWH grid extent")
    path = Path(spec.extent_path)
    if path.exists():
        path.unlink()
    gdf = gpd.GeoDataFrame(
        [
            {
                "case_id": spec.run_name,
                "workflow_mode": spec.workflow_mode,
                "grid_crs": spec.crs,
                "resolution_m": spec.resolution,
                "buffer_m": spec.buffer_m,
            }
        ],
        geometry=[box(spec.min_x, spec.min_y, spec.max_x, spec.max_y)],
        crs=spec.crs,
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    gdf.to_file(path, driver="GPKG")


def _write_scoring_grid_artifacts(spec: ScoringGridSpec) -> None:
    _write_template_raster(spec)
    _write_grid_extent(spec)
    spec.save_metadata(spec.metadata_path)
    spec.save_metadata(spec.metadata_json_path)


def _sanitize_vector_columns_for_gpkg(gdf: "gpd.GeoDataFrame") -> "gpd.GeoDataFrame":
    sanitized = gdf.copy()
    rename_map = {}
    for column in sanitized.columns:
        if column == "geometry":
            continue
        lowered = str(column).strip().lower()
        if lowered in {"fid", "ogc_fid"}:
            rename_map[column] = f"source_{lowered}"
    return sanitized.rename(columns=rename_map) if rename_map else sanitized


def _raw_payload_to_gdf(payload: dict) -> "gpd.GeoDataFrame":
    if gpd is None:
        raise ImportError("geopandas is required to read DWH GeoJSON payloads")
    features = payload.get("features") or []
    if not features:
        return gpd.GeoDataFrame(geometry=[], crs=GEOGRAPHIC_CRS)
    return gpd.GeoDataFrame.from_features(features).set_crs(GEOGRAPHIC_CRS, allow_override=True)


def _save_mask_raster(spec: ScoringGridSpec, data: np.ndarray, out_path: Path) -> None:
    if rasterio is None or from_origin is None:
        raise ImportError("rasterio is required to write DWH observation masks")
    transform = from_origin(spec.min_x, spec.max_y, spec.resolution, spec.resolution)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if out_path.exists():
        out_path.unlink()
    with rasterio.open(
        out_path,
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


def _rasterize_polygon_gdf(gdf: "gpd.GeoDataFrame", spec: ScoringGridSpec) -> np.ndarray:
    if rio_rasterize is None or from_origin is None:
        raise ImportError("rasterio is required to rasterize DWH observation vectors")
    work = gdf if str(gdf.crs).upper() == str(spec.crs).upper() else gdf.to_crs(spec.crs)
    valid = work.dropna(subset=["geometry"])
    shapes = [(geom, 1.0) for geom in valid.geometry if geom is not None and not geom.is_empty]
    if not shapes:
        return np.zeros((spec.height, spec.width), dtype=np.float32)
    transform = from_origin(spec.min_x, spec.max_y, spec.resolution, spec.resolution)
    return rio_rasterize(
        shapes,
        out_shape=(spec.height, spec.width),
        transform=transform,
        fill=0.0,
        dtype=np.float32,
        all_touched=True,
    ).astype(np.float32)


def _load_mask(path: Path) -> np.ndarray:
    if rasterio is None:
        raise ImportError("rasterio is required to read the DWH sea mask")
    with rasterio.open(path) as src:
        return src.read(1).astype(np.float32)


def _cached_gshhg_paths() -> tuple[Path | None, Path | None]:
    source_shp = Path("data_processed") / "grids" / "raw" / "gshhg_shp_i_l1" / "GSHHS_i_L1.shp"
    raw_archive = Path("data_processed") / "grids" / "raw" / "gshhg-shp-2.3.7.zip"
    return (source_shp if source_shp.exists() else None, raw_archive if raw_archive.exists() else None)


def _union_geometries(geometries):
    if hasattr(geometries, "union_all"):
        return geometries.union_all()
    return geometries.unary_union


class Phase3CExternalCaseSetupService:
    def __init__(
        self,
        *,
        config_path: str | Path | None = None,
        output_dir: str | Path | None = None,
        timeout: int = REQUEST_TIMEOUT_SECONDS,
    ):
        self.case = get_case_context()
        if self.case.workflow_mode != "dwh_retro_2010":
            raise RuntimeError("phase3c_external_case_setup requires WORKFLOW_MODE=dwh_retro_2010.")

        self.config_path = Path(config_path or self.case.case_definition_path or "config/case_dwh_retro_2010_72h.yaml")
        self.cfg = _load_yaml(self.config_path)
        self.case_id = str(self.cfg.get("case_id") or self.case.run_name)
        self.output_dir = Path(output_dir or Path("output") / self.case_id / PHASE3C_DIR_NAME)
        self.raw_dir = self.output_dir / "raw"
        self.processed_dir = self.output_dir / "processed"
        self.timeout = timeout
        self.session = requests.Session()

    @property
    def feature_server_url(self) -> str:
        return str((self.cfg.get("arcgis") or {}).get("feature_server_url") or DWH_FEATURE_SERVER_ROOT).rstrip("/")

    def run(self) -> dict:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.processed_dir.mkdir(parents=True, exist_ok=True)

        service_inventory = build_external_case_service_inventory(self.cfg)
        service_inventory_csv = self.output_dir / "external_case_service_inventory.csv"
        service_inventory_json = self.output_dir / "external_case_service_inventory.json"
        _write_csv(service_inventory_csv, service_inventory)
        _write_json(service_inventory_json, service_inventory)

        forcing_manifest = build_external_case_forcing_manifest(self.cfg)
        forcing_csv = self.output_dir / "external_case_forcing_manifest.csv"
        forcing_json = self.output_dir / "external_case_forcing_manifest.json"
        _write_csv(forcing_csv, forcing_manifest)
        _write_json(forcing_json, forcing_manifest)

        configured_layers = parse_external_case_layer_registry(self.cfg)
        selected_layers = [layer for layer in configured_layers if layer.selected_for_phase3c]
        registry_paths = self._write_selected_layer_registry(configured_layers)

        root_metadata, service_layers = self._fetch_service_root_metadata()
        taxonomy = self._build_source_taxonomy(service_layers, configured_layers)
        taxonomy_csv = self.output_dir / "external_case_source_taxonomy.csv"
        taxonomy_json = self.output_dir / "external_case_source_taxonomy.json"
        _write_csv(taxonomy_csv, taxonomy)
        _write_json(taxonomy_json, taxonomy)

        raw_layers = self._fetch_selected_layers(selected_layers)
        target_crs = derive_projected_crs_from_wgs84(
            [item["raw_gdf"] for item in raw_layers.values() if item["layer"].use_in_scoring_grid_extent]
        )
        processed_layers = self._write_processed_layers(raw_layers, target_crs)

        source_paths = {
            row["layer"].layer_key: str(row["processed_vector_path"])
            for row in processed_layers.values()
            if row["layer"].use_in_scoring_grid_extent
        }
        grid_inputs = [
            row["processed_gdf"]
            for row in processed_layers.values()
            if row["layer"].use_in_scoring_grid_extent
        ]
        spec = build_scoring_grid_spec_from_projected_gdfs(
            grid_inputs,
            target_crs=target_crs,
            output_dir=self.output_dir,
            workflow_mode=self.case.workflow_mode,
            run_name=self.case_id,
            source_paths=source_paths,
        )
        _write_scoring_grid_artifacts(spec)
        shoreline_manifest = self._build_shoreline_masks(spec)
        spec = ScoringGridSpec(
            **{
                **asdict(spec),
                "shoreline_mask_status": str(shoreline_manifest.get("shoreline_mask_status") or ""),
                "shoreline_mask_signature": str(shoreline_manifest.get("shoreline_mask_signature") or ""),
            }
        )
        spec.save_metadata(spec.metadata_path)
        spec.save_metadata(spec.metadata_json_path)

        mask_records = self._write_observation_masks(processed_layers, spec)
        report_paths = self._write_processing_report(processed_layers, mask_records, target_crs)
        grid_manifest_paths = self._write_grid_manifest(spec, shoreline_manifest, mask_records)
        memo_path = self._write_methodology_memo()
        guardrails_path = self._write_claims_guardrails()

        run_manifest_path = self.output_dir / "phase3c_external_case_setup_manifest.json"
        run_manifest = {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "workflow_mode": self.case.workflow_mode,
            "case_id": self.case_id,
            "spill_name": self.cfg.get("spill_name"),
            "phase_name": PHASE3C_NAME,
            "phase_placement": PHASE3C_PLACEMENT,
            "thesis_role": "external_transfer_validation_support_case",
            "main_case_reference": "mindoro_retro_2023",
            "main_case_note": "Mindoro remains the main Philippine thesis case; DWH is a separate external transfer-validation story.",
            "forcing_selection_rule": "Freeze the first complete real historical current+wind+wave stack that passes the scientific-readiness gate; do not use Phase 1 drifter-selected baseline logic.",
            "current_forcing_stack": "HYCOM GOFS 3.1 currents + ERA5 winds + CMEMS wave/Stokes",
            "truth_role": "observed_dwh_daily_masks_are_truth",
            "pygnome_role": "comparator_only",
            "selected_dwh_layers": [self._layer_summary(layer) for layer in selected_layers],
            "selected_forcing_services": forcing_manifest,
            "projected_scoring_crs": target_crs,
            "date_composite_time_policy": self.cfg.get("time_placeholder_semantics"),
            "full_external_case_model_run_performed": False,
            "recommended_next_implementation_branch": RECOMMENDED_NEXT_BRANCH,
            "artifacts": {
                "service_inventory_csv": str(service_inventory_csv),
                "service_inventory_json": str(service_inventory_json),
                "forcing_manifest_csv": str(forcing_csv),
                "forcing_manifest_json": str(forcing_json),
                "source_taxonomy_csv": str(taxonomy_csv),
                "source_taxonomy_json": str(taxonomy_json),
                "selected_layer_registry_csv": str(registry_paths["csv"]),
                "selected_layer_registry_json": str(registry_paths["json"]),
                "processing_report_csv": str(report_paths["csv"]),
                "processing_report_json": str(report_paths["json"]),
                "scoring_grid_yaml": str(spec.metadata_path),
                "scoring_grid_json": str(spec.metadata_json_path),
                "scoring_grid_template": str(spec.template_path),
                "grid_extent": str(spec.extent_path),
                "land_mask": str(spec.land_mask_path),
                "sea_mask": str(spec.sea_mask_path),
                "shoreline_segments": str(spec.shoreline_segments_path),
                "shoreline_manifest_json": str(spec.shoreline_mask_manifest_json_path),
                "shoreline_manifest_csv": str(spec.shoreline_mask_manifest_csv_path),
                "external_case_grid_manifest_csv": str(grid_manifest_paths["csv"]),
                "external_case_grid_manifest_json": str(grid_manifest_paths["json"]),
                "methodology_memo": str(memo_path),
                "claims_guardrails": str(guardrails_path),
                "feature_server_root_metadata": str(self.raw_dir / "feature_server_root_metadata.json"),
                **{record["mask_kind"] + "_" + record["event_date"]: record["mask_path"] for record in mask_records},
            },
            "raw_feature_server_layer_count": len(service_layers),
            "raw_service_metadata_name": root_metadata.get("name", ""),
        }
        _write_json(run_manifest_path, run_manifest)

        return {
            "output_dir": self.output_dir,
            "phase_name": PHASE3C_NAME,
            "phase_placement": PHASE3C_PLACEMENT,
            "selected_dwh_layers": [self._layer_summary(layer) for layer in selected_layers],
            "selected_forcing_services": forcing_manifest,
            "projected_scoring_crs": target_crs,
            "recommended_next_implementation_branch": RECOMMENDED_NEXT_BRANCH,
            "run_manifest": run_manifest_path,
            "service_inventory_csv": service_inventory_csv,
            "service_inventory_json": service_inventory_json,
            "forcing_manifest_csv": forcing_csv,
            "forcing_manifest_json": forcing_json,
            "taxonomy_csv": taxonomy_csv,
            "taxonomy_json": taxonomy_json,
            "processing_report_csv": report_paths["csv"],
            "masks": mask_records,
            "grid_manifest_csv": grid_manifest_paths["csv"],
            "grid_manifest_json": grid_manifest_paths["json"],
            "methodology_memo": memo_path,
            "claims_guardrails": guardrails_path,
        }

    def _get_json(self, url: str, params: dict[str, Any]) -> dict:
        response = self.session.get(url, params=params, timeout=self.timeout)
        response.raise_for_status()
        return response.json()

    def _fetch_service_root_metadata(self) -> tuple[dict, list[dict]]:
        metadata = self._get_json(self.feature_server_url, {"f": "json"})
        _write_json(self.raw_dir / "feature_server_root_metadata.json", metadata)
        return metadata, list(metadata.get("layers") or [])

    def _fetch_selected_layers(self, selected_layers: list[ExternalCaseLayer]) -> dict[int, dict]:
        raw_layers: dict[int, dict] = {}
        for layer in selected_layers:
            if layer.layer_id is None:
                continue
            metadata_url = f"{self.feature_server_url}/{layer.layer_id}"
            query_url = f"{metadata_url}/query"
            metadata = self._get_json(metadata_url, {"f": "json"})
            payload = self._get_json(
                query_url,
                {
                    "where": "1=1",
                    "outFields": "*",
                    "returnGeometry": "true",
                    "outSR": 4326,
                    "f": "geojson",
                },
            )
            prefix = f"layer_{layer.layer_id:02d}_{layer.local_name}"
            metadata_path = self.raw_dir / f"{prefix}_metadata.json"
            raw_geojson_path = self.raw_dir / f"{prefix}_raw.geojson"
            _write_json(metadata_path, metadata)
            _write_json(raw_geojson_path, payload)
            raw_gdf = _raw_payload_to_gdf(payload)
            if raw_gdf.empty:
                raise RuntimeError(f"DWH FeatureServer layer {layer.layer_id} returned no features.")
            raw_layers[layer.layer_id] = {
                "layer": layer,
                "metadata": metadata,
                "payload": payload,
                "raw_gdf": raw_gdf,
                "metadata_path": metadata_path,
                "raw_geojson_path": raw_geojson_path,
            }
        return raw_layers

    def _write_processed_layers(self, raw_layers: dict[int, dict], target_crs: str) -> dict[int, dict]:
        processed_layers: dict[int, dict] = {}
        for layer_id, item in raw_layers.items():
            layer = item["layer"]
            processed_gdf, qa = clean_arcgis_geometries(
                raw_gdf=item["raw_gdf"],
                expected_geometry_type=layer.geometry_type,
                source_crs=GEOGRAPHIC_CRS,
                target_crs=target_crs,
            )
            if processed_gdf.empty:
                raise RuntimeError(f"DWH layer {layer_id} produced no valid processed geometries.")

            processed_path = self.processed_dir / f"layer_{layer_id:02d}_{layer.local_name}_processed.gpkg"
            if processed_path.exists():
                processed_path.unlink()
            _sanitize_vector_columns_for_gpkg(processed_gdf).to_file(processed_path, driver="GPKG")
            processed_layers[layer_id] = {
                **item,
                "processed_gdf": processed_gdf,
                "processed_vector_path": processed_path,
                "qa": qa,
            }
        return processed_layers

    def _build_shoreline_masks(self, spec: ScoringGridSpec) -> dict:
        from src.services.shoreline_mask import GSHHG_SOURCE_URL, GSHHG_SOURCE_VERSION, build_shoreline_mask_artifacts

        source_shp, raw_archive = _cached_gshhg_paths()
        if source_shp is not None and gpd is not None:
            source_gdf = gpd.read_file(source_shp)
            return build_shoreline_mask_artifacts(
                spec,
                force_refresh=True,
                artifact_dir=self.output_dir,
                land_source_gdf=source_gdf,
                land_source_name="GSHHG",
                land_source_version=GSHHG_SOURCE_VERSION,
                land_source_url=GSHHG_SOURCE_URL,
                land_source_path=source_shp,
                land_raw_archive_path=raw_archive,
            )
        return build_shoreline_mask_artifacts(spec, force_refresh=True, artifact_dir=self.output_dir)

    def _write_observation_masks(self, processed_layers: dict[int, dict], spec: ScoringGridSpec) -> list[dict]:
        sea_mask = _load_mask(Path(spec.sea_mask_path)) if spec.sea_mask_path and Path(spec.sea_mask_path).exists() else None
        mask_records = []
        for layer_id in (5, 6, 7, 8):
            item = processed_layers[layer_id]
            layer = item["layer"]
            if layer.geometry_type != "polygon":
                continue
            mask = _rasterize_polygon_gdf(item["processed_gdf"], spec)
            if sea_mask is not None:
                mask = np.where(sea_mask > 0.5, mask, 0.0).astype(np.float32)
            if layer_id == 5:
                mask_kind = "obs_init"
                mask_path = self.output_dir / f"obs_init_{layer.event_date}.tif"
            else:
                mask_kind = "obs_mask"
                mask_path = self.output_dir / f"obs_mask_{layer.event_date}.tif"
            _save_mask_raster(spec, mask.astype(np.float32), mask_path)
            mask_records.append(
                {
                    "layer_id": layer_id,
                    "layer_name": layer.name,
                    "event_date": layer.event_date,
                    "mask_kind": mask_kind,
                    "mask_path": str(mask_path),
                    "nonzero_cells": int(np.count_nonzero(mask > 0)),
                    "grid_crs": spec.crs,
                    "grid_width": spec.width,
                    "grid_height": spec.height,
                    "grid_resolution_m": spec.resolution,
                    "sea_mask_applied": bool(sea_mask is not None),
                }
            )
        return mask_records

    def _write_selected_layer_registry(self, configured_layers: list[ExternalCaseLayer]) -> dict[str, Path]:
        rows = [asdict(layer) for layer in configured_layers]
        csv_path = self.output_dir / "external_case_selected_layer_registry.csv"
        json_path = self.output_dir / "external_case_selected_layer_registry.json"
        _write_csv(csv_path, rows)
        _write_json(json_path, rows)
        return {"csv": csv_path, "json": json_path}

    def _build_source_taxonomy(
        self,
        service_layers: list[dict],
        configured_layers: list[ExternalCaseLayer],
    ) -> list[dict]:
        configured_by_id = {layer.layer_id: layer for layer in configured_layers if layer.layer_id is not None}
        rows = []
        for layer in service_layers:
            layer_id = int(layer.get("id"))
            configured = configured_by_id.get(layer_id)
            name = str(layer.get("name") or (configured.name if configured else f"layer_{layer_id}"))
            geometry_type = _normalize_geometry_type(layer.get("geometryType") or (configured.geometry_type if configured else ""))
            taxonomy, reason = classify_dwh_public_layer(layer_id, name, geometry_type)
            if configured and configured.source_taxonomy:
                taxonomy = configured.source_taxonomy
            event_date = configured.event_date if configured else self._date_from_layer_name(name)
            rows.append(
                {
                    "layer_id": layer_id,
                    "layer_name": name,
                    "geometry_type": geometry_type,
                    "event_date": event_date,
                    "source_taxonomy": taxonomy,
                    "taxonomy_reason": reason,
                    "selected_for_phase3c": bool(configured.selected_for_phase3c) if configured else False,
                    "use_as_truth": bool(configured.use_as_truth) if configured else False,
                    "role": configured.role if configured else ("daily_composite_candidate" if taxonomy == SOURCE_TAXONOMY_OBS else ""),
                    "truth_handling": self._truth_handling(taxonomy, configured),
                }
            )
        rows.append(
            {
                "layer_id": "",
                "layer_name": "DWH modeled trajectory / forecast public products",
                "geometry_type": "varies",
                "event_date": "",
                "source_taxonomy": SOURCE_TAXONOMY_MODELED,
                "taxonomy_reason": "modeled trajectories or forecast products are comparators/context and are excluded from truth",
                "selected_for_phase3c": False,
                "use_as_truth": False,
                "role": "modeled_product_class_guardrail",
                "truth_handling": "exclude_from_truth",
            }
        )
        return sorted(rows, key=lambda row: (9999 if row["layer_id"] == "" else int(row["layer_id"])))

    @staticmethod
    def _date_from_layer_name(name: str) -> str:
        match = re.search(r"t(20\d{2})(\d{2})(\d{2})", str(name).lower())
        if not match:
            return ""
        return f"{match.group(1)}-{match.group(2)}-{match.group(3)}"

    @staticmethod
    def _truth_handling(taxonomy: str, configured: ExternalCaseLayer | None) -> str:
        if taxonomy == SOURCE_TAXONOMY_MODELED:
            return "exclude_from_truth"
        if configured and configured.use_as_truth:
            return "accepted_daily_validation_truth_candidate"
        if configured and configured.role == "initialization_polygon":
            return "initialization_reference_not_validation_skill"
        if taxonomy == SOURCE_TAXONOMY_OBS:
            return "observation_derived_candidate_not_selected_in_initial_72h_subcase"
        return "context_only"

    @staticmethod
    def _layer_summary(layer: ExternalCaseLayer) -> dict:
        if layer.layer_id == 0:
            why = "source point provenance for grid extent"
        elif layer.layer_id == 5:
            why = "initialization date-composite polygon"
        else:
            why = "daily validation date-composite polygon"
        return {
            "layer_id": layer.layer_id,
            "layer_name": layer.name,
            "event_date": layer.event_date,
            "role": layer.role,
            "source_taxonomy": layer.source_taxonomy,
            "why_selected": why,
        }

    def _write_processing_report(
        self,
        processed_layers: dict[int, dict],
        mask_records: list[dict],
        target_crs: str,
    ) -> dict[str, Path]:
        masks_by_layer = {record["layer_id"]: record for record in mask_records}
        rows = []
        for layer_id in sorted(processed_layers):
            item = processed_layers[layer_id]
            layer = item["layer"]
            mask = masks_by_layer.get(layer_id, {})
            qa = item["qa"]
            rows.append(
                {
                    "layer_id": layer_id,
                    "layer_name": layer.name,
                    "role": layer.role,
                    "event_date": layer.event_date,
                    "source_taxonomy": layer.source_taxonomy,
                    "raw_feature_count": int(len(item["raw_gdf"])),
                    "processed_feature_count": int(len(item["processed_gdf"])),
                    "raw_crs": GEOGRAPHIC_CRS,
                    "processed_crs": target_crs,
                    "raw_geojson_path": str(item["raw_geojson_path"]),
                    "raw_metadata_path": str(item["metadata_path"]),
                    "processed_vector_path": str(item["processed_vector_path"]),
                    "mask_path": mask.get("mask_path", ""),
                    "mask_nonzero_cells": mask.get("nonzero_cells", 0),
                    "vector_centroid": compute_vector_centroid(item["processed_gdf"], layer.geometry_type),
                    "vector_area_m2": compute_vector_area(item["processed_gdf"], layer.geometry_type),
                    "null_geometries_dropped": qa["null_geometries_dropped"],
                    "invalid_geometries_repaired": qa["invalid_geometries_repaired"],
                    "multipart_parts_exploded": qa["multipart_parts_exploded"],
                    "non_matching_parts_dropped": qa["non_matching_parts_dropped"],
                    "empty_geometries_dropped": qa["empty_geometries_dropped"],
                    "notes": "date-composite layer; no exact sub-daily observation time asserted",
                }
            )
        csv_path = self.output_dir / "external_case_processing_report.csv"
        json_path = self.output_dir / "external_case_processing_report.json"
        _write_csv(csv_path, rows)
        _write_json(json_path, rows)
        return {"csv": csv_path, "json": json_path}

    def _write_grid_manifest(
        self,
        spec: ScoringGridSpec,
        shoreline_manifest: dict,
        mask_records: list[dict],
    ) -> dict[str, Path]:
        payload = {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "case_id": self.case_id,
            "workflow_mode": self.case.workflow_mode,
            "phase_name": PHASE3C_NAME,
            "grid_derivation_rule": "union of layer 5 initialization polygon, layers 6-8 validation polygons, and layer 0 wellhead point; buffered 50 km and snapped outward to a 1 km grid",
            "projected_crs": spec.crs,
            "resolution_m": spec.resolution,
            "buffer_m": spec.buffer_m,
            "grid_snap_m": spec.grid_snap_m,
            "width": spec.width,
            "height": spec.height,
            "display_bounds_wgs84": spec.display_bounds_wgs84,
            "artifacts": {
                "scoring_grid_yaml": spec.metadata_path,
                "scoring_grid_json": spec.metadata_json_path,
                "scoring_grid_template": spec.template_path,
                "grid_extent": spec.extent_path,
                "land_mask": spec.land_mask_path,
                "sea_mask": spec.sea_mask_path,
                "shoreline_segments": spec.shoreline_segments_path,
                "shoreline_mask_manifest_json": spec.shoreline_mask_manifest_json_path,
                "shoreline_mask_manifest_csv": spec.shoreline_mask_manifest_csv_path,
            },
            "shoreline_manifest": shoreline_manifest,
            "observation_masks": mask_records,
        }
        csv_path = self.output_dir / "external_case_shoreline_grid_manifest.csv"
        json_path = self.output_dir / "external_case_shoreline_grid_manifest.json"
        _write_json(json_path, payload)
        _write_csv(
            csv_path,
            [
                {
                    "case_id": self.case_id,
                    "phase_name": PHASE3C_NAME,
                    "projected_crs": spec.crs,
                    "resolution_m": spec.resolution,
                    "buffer_m": spec.buffer_m,
                    "grid_snap_m": spec.grid_snap_m,
                    "width": spec.width,
                    "height": spec.height,
                    "display_bounds_wgs84": json.dumps(spec.display_bounds_wgs84),
                    "land_mask": spec.land_mask_path,
                    "sea_mask": spec.sea_mask_path,
                    "shoreline_segments": spec.shoreline_segments_path,
                    "shoreline_mask_status": shoreline_manifest.get("shoreline_mask_status", ""),
                    "shoreline_mask_signature": shoreline_manifest.get("shoreline_mask_signature", ""),
                }
            ],
        )
        return {"csv": csv_path, "json": json_path}

    def _write_methodology_memo(self) -> Path:
        path = self.output_dir / "chapter3_phase3c_external_case_memo.md"
        lines = [
            "# Chapter 3 Phase 3C External Case Memo",
            "",
            f"New phase name: {PHASE3C_NAME}",
            "",
            f"Placement: {PHASE3C_PLACEMENT}.",
            "",
            "Mindoro remains the main Philippine case. Phase 3C adds a separate external rich-data transfer-validation case and does not replace, reinterpret, or overwrite any Mindoro Phase 3A/3B outputs.",
            "",
            "Deepwater Horizon becomes the first external rich-data transfer case because the public FeatureServer provides machine-readable daily composite spill polygons and a wellhead provenance point.",
            "",
            "The initial 72 h subcase is observation-initialized first: layer 5 (T20100520_Composite) seeds the reference initialization, and layers 6, 7, and 8 provide separate daily validation masks for 2010-05-21, 2010-05-22, and 2010-05-23.",
            "",
            "Source-point reconstruction can remain a later sensitivity. Layer 0 is retained as provenance and as a scoring-grid extent input, but it is not the first active-release implementation.",
            "",
            "The DWH public layer names support date-composite logic, not defensible exact sub-daily acquisition timestamps. The workflow therefore keeps the daily masks separate and does not invent exact observation times.",
            "",
            "The DWH forcing-selection rule is readiness-gated: freeze the first complete real historical current+wind+wave stack that passes the scientific-readiness gate, rather than any Phase 1 drifter-selected baseline logic.",
            "",
            "In the current repo state, that frozen DWH stack is HYCOM GOFS 3.1 currents + ERA5 winds + CMEMS wave/Stokes.",
            "",
            "Observed DWH daily masks remain truth for this external case. PyGNOME, when introduced later in Phase 3C, remains comparator-only.",
        ]
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return path

    def _write_claims_guardrails(self) -> Path:
        path = self.output_dir / "phase3c_claims_guardrails.md"
        lines = [
            "# Phase 3C Claims Guardrails",
            "",
            "- Phase 3C tests transferability; it does not replace Mindoro as the main Philippine thesis case.",
            "- DWH remains a separate external transfer-validation/support story, not the main Philippine thesis case.",
            "- Observed DWH daily masks remain truth for this external case.",
            "- PyGNOME remains comparator-only and never becomes truth.",
            "- DWH forcing selection is readiness-gated and must never be described as a Phase 1 drifter-selected baseline choice.",
            "- The current stored DWH stack is HYCOM GOFS 3.1 currents + ERA5 winds + CMEMS wave/Stokes.",
            "- Modeled public products, including trajectory or forecast products, are excluded from truth.",
            "- The cumulative DWH composite layer is context-only unless a later memo explicitly justifies a different role.",
            "- Daily validation masks must stay separate from cumulative/context layers.",
        ]
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return path


def run_phase3c_external_case_setup() -> dict:
    return Phase3CExternalCaseSetupService().run()


if __name__ == "__main__":  # pragma: no cover
    result = run_phase3c_external_case_setup()
    print(json.dumps({key: str(value) for key, value in result.items()}, indent=2, default=str))
