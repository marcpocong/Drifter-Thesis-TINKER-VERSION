"""
Appendix-only public observation expansion for the official Mindoro case.

This service keeps the official main Phase 3B target unchanged:
  March 6 date-composite P50 vs obs_mask_2023-03-06.tif

It inventories related public ArcGIS items, accepts only source-based
machine-readable dated observation layers for appendix-only quantitative
comparisons, and writes all secondary outputs under
output/CASE_MINDORO_RETRO_2023/public_obs_appendix/.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
import time
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd
import requests

from src.core.case_context import get_case_context
from src.helpers.metrics import calculate_fss
from src.helpers.raster import GridBuilder, rasterize_observation_layer, save_raster
from src.helpers.scoring import apply_ocean_mask, load_sea_mask_array, precheck_same_grid
from src.services.arcgis import (
    _infer_source_crs,
    _repair_degree_scaled_geometries,
    _sanitize_vector_columns_for_gpkg,
    clean_arcgis_geometries,
)
from src.services.ensemble import run_official_spill_forecast
from src.services.scoring import OFFICIAL_PHASE3B_WINDOWS_KM, Phase3BScoringService
from src.utils.io import (
    find_missing_phase3b_forecast_outputs,
    get_case_output_dir,
    get_ensemble_manifest_path,
    get_forecast_manifest_path,
    get_official_mask_p50_datecomposite_path,
    resolve_recipe_selection,
    resolve_spill_origin,
)

try:
    import geopandas as gpd
except ImportError:  # pragma: no cover - runtime guarded
    gpd = None

try:
    import matplotlib.pyplot as plt
except ImportError:  # pragma: no cover - runtime guarded
    plt = None

try:
    import xarray as xr
except ImportError:  # pragma: no cover - runtime guarded
    xr = None


APP_ITEM_ID = "4c33262db22f46dea1fdf876f1260239"
WEBMAP_ITEM_ID = "9235accb91e3495782c8669a0fdcae76"
MAIN_FEATURE_SERVICE_ITEM_ID = "f62479ef6efb4e9e95086ea6af91fe51"
GROUND_POINTS_ITEM_ID = "343db71a0fff4b26a21bdeedfa665db2"
GROUND_POINTS_APP_ITEM_ID = "9b84eb31bfb04490803c94045242e649"
PHILSA_TIMELINE_URL = (
    "https://philsa.gov.ph/news/philsa-shows-journalists-extent-of-mindoro-oil-spill-"
    "as-captured-by-satellite-images/"
)
ARCGIS_RELATED_QUERY = "MindoroOilSpill owner:jrsales@wwf.org.ph_panda"
ARCGIS_SHARING_BASE = "https://www.arcgis.com/sharing/rest/content/items"
ARCGIS_SEARCH_URL = "https://www.arcgis.com/sharing/rest/search"
REQUEST_TIMEOUT = 60

OFFICIAL_LOCKED_PHASE3B_FILES = [
    Path("output/CASE_MINDORO_RETRO_2023/phase3b/phase3b_pairing_manifest.csv"),
    Path("output/CASE_MINDORO_RETRO_2023/phase3b/phase3b_fss_by_date_window.csv"),
    Path("output/CASE_MINDORO_RETRO_2023/phase3b/phase3b_summary.csv"),
]


@dataclass(frozen=True)
class InventoryRow:
    source_key: str
    source_name: str
    provider: str
    item_or_layer_id: str
    source_url: str
    service_url: str
    layer_id: str
    obs_date: str
    obs_time_local: str
    obs_time_utc: str
    source_type: str
    machine_readable: bool
    public: bool
    observation_derived: bool
    reproducibly_ingestible: bool
    geometry_type: str
    within_current_72h_horizon: bool
    accept_for_appendix_quantitative: bool
    accept_for_appendix_qualitative: bool
    rejection_reason: str
    notes: str
    archived_item_metadata: str = ""
    archived_layer_metadata: str = ""
    archived_raw_download: str = ""
    processed_vector: str = ""
    appendix_obs_mask: str = ""

    def to_dict(self) -> dict:
        return {
            "source_key": self.source_key,
            "source_name": self.source_name,
            "provider": self.provider,
            "item_id or layer_id": self.item_or_layer_id,
            "item_or_layer_id": self.item_or_layer_id,
            "source_url": self.source_url,
            "service_url": self.service_url,
            "layer_id": self.layer_id,
            "obs_date": self.obs_date,
            "obs_time_local": self.obs_time_local,
            "obs_time_utc": self.obs_time_utc,
            "source_type": self.source_type,
            "machine_readable": bool(self.machine_readable),
            "public": bool(self.public),
            "observation_derived": bool(self.observation_derived),
            "reproducibly_ingestible": bool(self.reproducibly_ingestible),
            "geometry_type": self.geometry_type,
            "within_current_72h_horizon": bool(self.within_current_72h_horizon),
            "accept_for_appendix_quantitative": bool(self.accept_for_appendix_quantitative),
            "accept_for_appendix_qualitative": bool(self.accept_for_appendix_qualitative),
            "rejection_reason": self.rejection_reason,
            "notes": self.notes,
            "archived_item_metadata": self.archived_item_metadata,
            "archived_layer_metadata": self.archived_layer_metadata,
            "archived_raw_download": self.archived_raw_download,
            "processed_vector": self.processed_vector,
            "appendix_obs_mask": self.appendix_obs_mask,
        }


def _write_json(path: Path, payload: dict | list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with open(temp_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, default=str)
        f.write("\n")
    _replace_file(temp_path, path)


def _replace_file(temp_path: Path, destination: Path) -> None:
    last_error: Exception | None = None
    for attempt in range(5):
        try:
            if destination.exists():
                destination.unlink()
            os.replace(temp_path, destination)
            return
        except PermissionError as exc:
            last_error = exc
            time.sleep(0.5 * (attempt + 1))
    if temp_path.exists():
        temp_path.unlink(missing_ok=True)
    if last_error is not None:
        raise last_error
    raise RuntimeError(f"Failed to replace {destination} from {temp_path}")


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    with open(temp_path, "w", encoding="utf-8") as f:
        f.write(text)
    _replace_file(temp_path, path)


def _write_csv(path: Path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    df.to_csv(temp_path, index=False)
    _replace_file(temp_path, path)


def _load_json(path: Path) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f) or {}


def _strip_html(text: str | None) -> str:
    value = str(text or "")
    return re.sub(r"<[^>]+>", " ", value).replace("&nbsp;", " ").strip()


def _slugify(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9._-]+", "_", str(value).strip()).strip("._") or "item"


def parse_obs_date(value: str | None) -> str:
    text = str(value or "").strip()
    if not text:
        return ""

    for pattern in (
        r"(20\d{2})(\d{2})(\d{2})",
        r"(?<!\d)(\d{2})(\d{2})(\d{2})(?!\d)",
        r"(\d{2})/(\d{2})/(20\d{2})",
    ):
        match = re.search(pattern, text)
        if not match:
            continue
        groups = match.groups()
        if len(groups[0]) == 4:
            year, month, day = groups
        elif len(groups[2]) == 4:
            day, month, year = groups
        else:
            year = f"20{groups[0]}"
            month = groups[1]
            day = groups[2]
        try:
            return pd.Timestamp(f"{year}-{month}-{day}").strftime("%Y-%m-%d")
        except Exception:
            continue

    return ""


def is_within_current_horizon(obs_date: str, simulation_start_utc: str, simulation_end_utc: str) -> bool:
    if not obs_date:
        return False
    day = pd.Timestamp(obs_date).date()
    return pd.Timestamp(simulation_start_utc).date() <= day <= pd.Timestamp(simulation_end_utc).date()


def infer_provider(title: str, owner: str = "", description: str = "", snippet: str = "") -> str:
    merged = " ".join([title or "", owner or "", description or "", snippet or ""]).lower()
    if "philsa" in merged:
        return "PhilSA"
    if "noaa" in merged or "nesdis" in merged:
        return "NOAA/NESDIS"
    if "msi" in merged or "marine science institute" in merged:
        return "UP MSI"
    if "wwf" in merged or "panda" in merged or "aguab" in merged:
        return "WWF Philippines"
    if "gis" in merged:
        return "WWF GIS"
    return "Unknown"


def classify_inventory_acceptance(
    *,
    public: bool,
    source_type: str,
    observation_derived: bool,
    reproducibly_ingestible: bool,
    geometry_type: str,
    obs_date: str,
    within_current_72h_horizon: bool,
) -> tuple[bool, bool, str]:
    lowered_source_type = str(source_type).lower()
    lowered_geometry = str(geometry_type).lower()
    if not public:
        return False, False, "source is not public"
    if "app" in lowered_source_type or "web map" in lowered_source_type or "visualization" in lowered_source_type:
        return False, True, "wrapper/visualization source, not a single scoreable observation layer"
    if "pdf" in lowered_source_type or "timeline" in lowered_source_type:
        return False, True, "dated public inventory is not machine-readable as a scoreable polygon layer"
    if not observation_derived:
        return False, True, "source is not an observation-derived oil extent layer"
    if not reproducibly_ingestible:
        return False, True, "source is not reproducibly ingestible"
    if lowered_geometry != "polygon":
        if lowered_geometry == "point":
            return False, True, "point source is not a polygonal oil-extent target"
        return False, True, "geometry is not a polygonal oil-extent target"
    if not obs_date:
        return False, True, "source is not explicitly dated"
    if not within_current_72h_horizon:
        return False, True, "beyond current official 72 h horizon; extended-horizon rerun required"
    return True, True, ""


class PublicObservationAppendixService:
    def __init__(self):
        self.case = get_case_context()
        if not self.case.is_official:
            raise RuntimeError("public_obs_appendix is only supported for official spill-case workflows.")
        if gpd is None or xr is None:
            raise ImportError("geopandas and xarray are required for the public observation appendix.")

        self.base_output_dir = get_case_output_dir(self.case.run_name)
        self.appendix_dir = self.base_output_dir / "public_obs_appendix"
        self.raw_dir = self.appendix_dir / "raw"
        self.processed_dir = self.appendix_dir / "processed_vectors"
        self.accepted_masks_dir = self.appendix_dir / "accepted_obs_masks"
        self.forecast_date_dir = self.appendix_dir / "forecast_datecomposites"
        self.precheck_dir = self.appendix_dir / "precheck"
        for path in (self.appendix_dir, self.raw_dir, self.processed_dir, self.accepted_masks_dir, self.forecast_date_dir, self.precheck_dir):
            path.mkdir(parents=True, exist_ok=True)

        self.grid = GridBuilder()
        self.sea_mask = load_sea_mask_array(self.grid.spec)
        self.phase3b_helper = Phase3BScoringService(output_dir=self.appendix_dir / "_scratch_phase3b_helper")
        self.validation_time = pd.Timestamp(self.case.validation_layer.event_time_utc or self.case.simulation_end_utc)
        self.validation_date = self.validation_time.strftime("%Y-%m-%d")
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "mindoro-public-obs-appendix/1.0"})

        self.main_phase3b_hashes_before = self._snapshot_locked_phase3b_files()
        self.forecast_generated_during_run = False

    def _snapshot_locked_phase3b_files(self) -> dict[str, str]:
        snapshot: dict[str, str] = {}
        for path in OFFICIAL_LOCKED_PHASE3B_FILES:
            if path.exists():
                snapshot[str(path)] = hashlib.sha256(path.read_bytes()).hexdigest()
            else:
                snapshot[str(path)] = ""
        return snapshot

    def _verify_locked_phase3b_files_unchanged(self) -> None:
        after = self._snapshot_locked_phase3b_files()
        if after != self.main_phase3b_hashes_before:
            raise RuntimeError(
                "Appendix run modified one of the locked official Phase 3B files. "
                "This appendix workflow must not change the main March 6 score tables."
            )

    def _item_url(self, item_id: str) -> str:
        return f"{ARCGIS_SHARING_BASE}/{item_id}"

    def _fetch_arcgis_item(self, item_id: str) -> dict:
        resp = self.session.get(self._item_url(item_id), params={"f": "json"}, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        return resp.json() or {}

    def _fetch_service_root(self, service_url: str) -> dict:
        resp = self.session.get(service_url, params={"f": "json"}, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        return resp.json() or {}

    def _fetch_service_layer(self, service_url: str, layer_id: int) -> dict:
        resp = self.session.get(f"{service_url.rstrip('/')}/{int(layer_id)}", params={"f": "json"}, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        return resp.json() or {}

    def _fetch_geojson(self, service_url: str, layer_id: int) -> dict:
        resp = self.session.get(
            f"{service_url.rstrip('/')}/{int(layer_id)}/query",
            params={
                "where": "1=1",
                "outFields": "*",
                "returnGeometry": "true",
                "outSR": 4326,
                "f": "geojson",
            },
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        return resp.json() or {}

    def _search_related_items(self) -> list[dict]:
        resp = self.session.get(
            ARCGIS_SEARCH_URL,
            params={"q": ARCGIS_RELATED_QUERY, "f": "json", "num": 100},
            timeout=REQUEST_TIMEOUT,
        )
        resp.raise_for_status()
        return (resp.json() or {}).get("results") or []

    def _ensure_official_forecast_outputs(self) -> None:
        forecast_manifest = get_forecast_manifest_path(self.case.run_name)
        ensemble_manifest = get_ensemble_manifest_path(self.case.run_name)
        main_datecomposite = get_official_mask_p50_datecomposite_path(self.case.run_name)
        selection = resolve_recipe_selection()
        missing_specs = find_missing_phase3b_forecast_outputs(selection.recipe, run_name=self.case.run_name)
        if forecast_manifest.exists() and ensemble_manifest.exists() and main_datecomposite.exists() and not missing_specs:
            return

        start_lat, start_lon, start_time = resolve_spill_origin()
        run_official_spill_forecast(
            selection=selection,
            start_time=start_time,
            start_lat=start_lat,
            start_lon=start_lon,
        )
        self.forecast_generated_during_run = True

    def _copy_official_arcgis_artifacts(
        self,
        *,
        source_key: str,
        raw_geojson_path: Path,
        processed_vector_path: Path,
        service_metadata_path: Path,
        mask_path: Path,
    ) -> tuple[Path, Path, Path, Path]:
        raw_target_dir = self.raw_dir / source_key
        raw_target_dir.mkdir(parents=True, exist_ok=True)
        raw_target = raw_target_dir / raw_geojson_path.name
        meta_target = raw_target_dir / service_metadata_path.name
        processed_target = self.processed_dir / f"{source_key}.gpkg"
        mask_target = self.accepted_masks_dir / f"{source_key}.tif"

        shutil.copyfile(raw_geojson_path, raw_target)
        shutil.copyfile(service_metadata_path, meta_target)
        shutil.copyfile(processed_vector_path, processed_target)
        shutil.copyfile(mask_path, mask_target)
        return raw_target, meta_target, processed_target, mask_target

    def _archive_and_ingest_external_polygon(
        self,
        *,
        source_key: str,
        item_id: str,
        service_url: str,
        layer_id: int,
    ) -> tuple[Path, Path, Path, Path, Path]:
        raw_target_dir = self.raw_dir / source_key
        raw_target_dir.mkdir(parents=True, exist_ok=True)

        item_meta = self._fetch_arcgis_item(item_id)
        layer_meta = self._fetch_service_layer(service_url, layer_id)
        raw_geojson = self._fetch_geojson(service_url, layer_id)

        item_meta_path = raw_target_dir / f"{source_key}_item_metadata.json"
        layer_meta_path = raw_target_dir / f"{source_key}_layer_metadata.json"
        raw_geojson_path = raw_target_dir / f"{source_key}_raw.geojson"
        _write_json(item_meta_path, item_meta)
        _write_json(layer_meta_path, layer_meta)
        _write_json(raw_geojson_path, raw_geojson)

        raw_gdf = gpd.GeoDataFrame.from_features(raw_geojson.get("features") or [])
        raw_gdf = raw_gdf.set_crs("EPSG:4326", allow_override=True)
        inferred_crs, _ = _infer_source_crs(raw_gdf, layer_meta, raw_geojson)
        raw_gdf = raw_gdf.set_crs(inferred_crs, allow_override=True)
        raw_gdf, _ = _repair_degree_scaled_geometries(
            raw_gdf,
            metadata=layer_meta,
            payload=raw_geojson,
            expected_region=list(self.case.region),
        )
        cleaned_gdf, _ = clean_arcgis_geometries(
            raw_gdf=raw_gdf,
            expected_geometry_type="polygon",
            source_crs=inferred_crs,
            target_crs=self.grid.crs,
        )
        if cleaned_gdf.empty:
            raise RuntimeError(f"Accepted appendix source {source_key} produced no valid polygon geometry after cleaning.")

        processed_path = self.processed_dir / f"{source_key}.gpkg"
        cleaned_to_write = _sanitize_vector_columns_for_gpkg(cleaned_gdf)
        cleaned_to_write.to_file(processed_path, driver="GPKG")

        mask_path = self.accepted_masks_dir / f"{source_key}.tif"
        mask_data = rasterize_observation_layer(cleaned_gdf, self.grid)
        mask_data = apply_ocean_mask(mask_data, sea_mask=self.sea_mask, fill_value=0.0)
        save_raster(self.grid, mask_data.astype(np.float32), mask_path)
        return item_meta_path, layer_meta_path, raw_geojson_path, processed_path, mask_path

    def _build_main_service_layer_rows(self, main_item: dict) -> list[InventoryRow]:
        service_url = str(main_item.get("url") or "")
        layer_specs = [
            {
                "layer_id": 0,
                "source_key": "wwf_main_layer0_provenance_mar3",
                "notes": "Public provenance point from the main WWF monitoring feature service.",
            },
            {
                "layer_id": 1,
                "source_key": "wwf_main_layer1_validation_mar6",
                "notes": "Public March 6 validation polygon from the main WWF monitoring feature service.",
            },
            {
                "layer_id": 2,
                "source_key": "wwf_main_layer2_mangrove",
                "notes": "Environmental context layer from the main WWF monitoring feature service.",
            },
            {
                "layer_id": 3,
                "source_key": "wwf_main_layer3_init_mar3",
                "notes": "Public March 3 polygon from the main WWF monitoring feature service.",
            },
            {
                "layer_id": 4,
                "source_key": "wwf_main_layer4_benthic",
                "notes": "Environmental context layer from the main WWF monitoring feature service.",
            },
            {
                "layer_id": 5,
                "source_key": "wwf_main_layer5_vip_region",
                "notes": "Study-region context layer from the main WWF monitoring feature service.",
            },
        ]

        rows: list[InventoryRow] = []
        for spec in layer_specs:
            layer_meta = self._fetch_service_layer(service_url, spec["layer_id"])
            layer_name = str(layer_meta.get("name") or f"Layer {spec['layer_id']}")
            description = _strip_html(layer_meta.get("description"))
            geometry_type = str(layer_meta.get("geometryType") or "").replace("esriGeometry", "").lower() or "unknown"
            observation_derived = spec["layer_id"] in {1, 3}

            explicit_date = parse_obs_date(layer_name) or parse_obs_date(description)
            if spec["layer_id"] == 1:
                explicit_date = "2023-03-06"
            elif spec["layer_id"] == 3:
                explicit_date = "2023-03-03"
            elif spec["layer_id"] == 0:
                explicit_date = "2023-03-03"

            within_horizon = is_within_current_horizon(
                explicit_date,
                self.case.simulation_start_utc,
                self.case.simulation_end_utc,
            )
            accept_quant, accept_qual, rejection_reason = classify_inventory_acceptance(
                public=str(main_item.get("access")) == "public",
                source_type="feature layer",
                observation_derived=observation_derived,
                reproducibly_ingestible=True,
                geometry_type=geometry_type,
                obs_date=explicit_date,
                within_current_72h_horizon=within_horizon,
            )

            rows.append(
                InventoryRow(
                    source_key=spec["source_key"],
                    source_name=layer_name,
                    provider="WWF Philippines",
                    item_or_layer_id=f"{MAIN_FEATURE_SERVICE_ITEM_ID}:{spec['layer_id']}",
                    source_url=f"{service_url}/{spec['layer_id']}",
                    service_url=service_url,
                    layer_id=str(spec["layer_id"]),
                    obs_date=explicit_date,
                    obs_time_local="",
                    obs_time_utc="",
                    source_type="feature layer",
                    machine_readable=True,
                    public=str(main_item.get("access")) == "public",
                    observation_derived=observation_derived,
                    reproducibly_ingestible=True,
                    geometry_type=geometry_type,
                    within_current_72h_horizon=within_horizon,
                    accept_for_appendix_quantitative=accept_quant,
                    accept_for_appendix_qualitative=accept_qual,
                    rejection_reason=rejection_reason,
                    notes=spec["notes"],
                )
            )
        return rows

    def _inventory_row_from_item(self, item: dict) -> InventoryRow:
        title = str(item.get("title") or item.get("name") or item.get("id"))
        item_id = str(item.get("id") or "")
        item_type = str(item.get("type") or "")
        description = _strip_html(item.get("description"))
        snippet = _strip_html(item.get("snippet"))
        source_url = str(item.get("url") or self._item_url(item_id))
        provider = infer_provider(title, owner=str(item.get("owner") or ""), description=description, snippet=snippet)
        obs_date = parse_obs_date(title) or parse_obs_date(description) or parse_obs_date(snippet)
        public = str(item.get("access") or "") == "public"
        machine_readable = item_type in {"Feature Service", "Feature Layer", "Web Map", "Web Mapping Application"}
        reproducibly_ingestible = item_type in {"Feature Service", "Feature Layer"}
        observation_derived = "oilspill" in title.lower() or "oil spill" in title.lower() or "oil spill" in description.lower()
        geometry_type = "unknown"
        layer_id = ""
        service_url = str(item.get("url") or "")

        if item_id in {APP_ITEM_ID, WEBMAP_ITEM_ID}:
            observation_derived = False
        if item_id in {GROUND_POINTS_ITEM_ID, GROUND_POINTS_APP_ITEM_ID}:
            observation_derived = True
            geometry_type = "point"
            reproducibly_ingestible = True
        if item_type == "Feature Service" and service_url:
            try:
                root = self._fetch_service_root(service_url)
                layers = root.get("layers") or []
                if len(layers) == 1:
                    layer_id = str(layers[0].get("id", 0))
                    layer_meta = self._fetch_service_layer(service_url, int(layer_id))
                    geometry_type = str(layer_meta.get("geometryType") or "").replace("esriGeometry", "").lower() or "unknown"
                else:
                    geometry_type = "multilayer"
            except Exception:
                geometry_type = "unknown"

        within_horizon = is_within_current_horizon(
            obs_date,
            self.case.simulation_start_utc,
            self.case.simulation_end_utc,
        )
        accept_quant, accept_qual, rejection_reason = classify_inventory_acceptance(
            public=public,
            source_type=item_type.lower(),
            observation_derived=observation_derived,
            reproducibly_ingestible=reproducibly_ingestible,
            geometry_type=geometry_type,
            obs_date=obs_date,
            within_current_72h_horizon=within_horizon,
        )
        if item_id == MAIN_FEATURE_SERVICE_ITEM_ID:
            accept_quant = False
            accept_qual = True
            rejection_reason = "feature service wrapper contains multiple layers; layer-level acceptance is handled separately"
        if item_id in {APP_ITEM_ID, WEBMAP_ITEM_ID}:
            accept_quant = False
            accept_qual = True
            rejection_reason = "wrapper/config item, not a single scoreable observation layer"
        if item_id in {GROUND_POINTS_ITEM_ID, GROUND_POINTS_APP_ITEM_ID}:
            accept_quant = False
            accept_qual = True
            rejection_reason = "crowdsourced ground-validation points are not polygonal oil-extent masks"

        return InventoryRow(
            source_key=_slugify(item_id),
            source_name=title,
            provider=provider,
            item_or_layer_id=item_id,
            source_url=source_url,
            service_url=service_url,
            layer_id=layer_id,
            obs_date=obs_date,
            obs_time_local="",
            obs_time_utc="",
            source_type=item_type.lower(),
            machine_readable=machine_readable,
            public=public,
            observation_derived=observation_derived,
            reproducibly_ingestible=reproducibly_ingestible,
            geometry_type=geometry_type,
            within_current_72h_horizon=within_horizon,
            accept_for_appendix_quantitative=accept_quant,
            accept_for_appendix_qualitative=accept_qual,
            rejection_reason=rejection_reason,
            notes=description or snippet,
        )

    def _timeline_inventory_row(self) -> InventoryRow:
        notes = (
            "Public dated PhilSA inventory page with a collated 03-19 March 2023 timeline. "
            "Useful for appendix context only because it is not a machine-readable polygon layer."
        )
        return InventoryRow(
            source_key="philsa_timeline_inventory",
            source_name="PhilSA collated oil spill maps 03-19 March 2023",
            provider="PhilSA",
            item_or_layer_id=PHILSA_TIMELINE_URL,
            source_url=PHILSA_TIMELINE_URL,
            service_url="",
            layer_id="",
            obs_date="",
            obs_time_local="",
            obs_time_utc="",
            source_type="pdf timeline / dated public inventory",
            machine_readable=False,
            public=True,
            observation_derived=True,
            reproducibly_ingestible=False,
            geometry_type="none",
            within_current_72h_horizon=False,
            accept_for_appendix_quantitative=False,
            accept_for_appendix_qualitative=True,
            rejection_reason="dated public inventory is not machine-readable as a scoreable polygon layer",
            notes=notes,
        )

    def _build_inventory_rows(self) -> list[InventoryRow]:
        inventory_rows: dict[str, InventoryRow] = {}

        explicit_item_ids = [
            APP_ITEM_ID,
            WEBMAP_ITEM_ID,
            MAIN_FEATURE_SERVICE_ITEM_ID,
            GROUND_POINTS_ITEM_ID,
            GROUND_POINTS_APP_ITEM_ID,
            "8ac3f21af7944e56a8b38f71b663de87",
            "221529e3711b43e78e34775efdf490d9",
            "f014614c39644fb6a7569f11126d4862",
            "ab7b88da577144ad963d5caf5d3afb80",
            "878f1d24819e483b88b510bbc2308512",
            "60cf3070885c42178d307c1161d2858d",
        ]
        explicit_items = [self._fetch_arcgis_item(item_id) for item_id in explicit_item_ids]
        search_items = self._search_related_items()
        merged_items = {
            str(item.get("id")): item
            for item in explicit_items + search_items
            if item.get("id")
        }

        for item in merged_items.values():
            row = self._inventory_row_from_item(item)
            inventory_rows[row.source_key] = row

        main_service_item = merged_items.get(MAIN_FEATURE_SERVICE_ITEM_ID) or self._fetch_arcgis_item(MAIN_FEATURE_SERVICE_ITEM_ID)
        for row in self._build_main_service_layer_rows(main_service_item):
            inventory_rows[row.source_key] = row

        inventory_rows["philsa_timeline_inventory"] = self._timeline_inventory_row()
        return sorted(inventory_rows.values(), key=lambda row: (row.obs_date or "9999-99-99", row.source_name.lower()))

    def _accepted_quantitative_rows(self, rows: list[InventoryRow]) -> list[InventoryRow]:
        return [
            row
            for row in rows
            if row.accept_for_appendix_quantitative and row.within_current_72h_horizon
        ]

    def _archive_quantitative_rows(self, rows: list[InventoryRow]) -> list[InventoryRow]:
        updated_rows: list[InventoryRow] = []
        run_name = self.case.run_name
        existing_official_layers = {
            "wwf_main_layer3_init_mar3": {
                "raw": self.case.initialization_layer.raw_geojson_path(run_name),
                "processed": self.case.initialization_layer.processed_vector_path(run_name),
                "metadata": self.case.initialization_layer.service_metadata_path(run_name),
                "mask": self.case.initialization_layer.mask_path(run_name),
            },
            "wwf_main_layer1_validation_mar6": {
                "raw": self.case.validation_layer.raw_geojson_path(run_name),
                "processed": self.case.validation_layer.processed_vector_path(run_name),
                "metadata": self.case.validation_layer.service_metadata_path(run_name),
                "mask": self.case.validation_layer.official_observed_mask_path(run_name),
            },
        }

        for row in rows:
            if row.source_key in existing_official_layers:
                paths = existing_official_layers[row.source_key]
                raw_path, meta_path, processed_path, mask_path = self._copy_official_arcgis_artifacts(
                    source_key=row.source_key,
                    raw_geojson_path=paths["raw"],
                    processed_vector_path=paths["processed"],
                    service_metadata_path=paths["metadata"],
                    mask_path=paths["mask"],
                )
                updated_rows.append(
                    InventoryRow(
                        **{
                            **row.__dict__,
                            "archived_item_metadata": str(meta_path),
                            "archived_layer_metadata": str(meta_path),
                            "archived_raw_download": str(raw_path),
                            "processed_vector": str(processed_path),
                            "appendix_obs_mask": str(mask_path),
                        }
                    )
                )
                continue

            item_id = row.item_or_layer_id.split(":", 1)[0]
            layer_id = int(row.layer_id or 0)
            item_meta_path, layer_meta_path, raw_geojson_path, processed_path, mask_path = self._archive_and_ingest_external_polygon(
                source_key=row.source_key,
                item_id=item_id,
                service_url=row.service_url,
                layer_id=layer_id,
            )
            updated_rows.append(
                InventoryRow(
                    **{
                        **row.__dict__,
                        "archived_item_metadata": str(item_meta_path),
                        "archived_layer_metadata": str(layer_meta_path),
                        "archived_raw_download": str(raw_geojson_path),
                        "processed_vector": str(processed_path),
                        "appendix_obs_mask": str(mask_path),
                    }
                )
            )

        return updated_rows

    def _write_inventory(self, rows: list[InventoryRow]) -> dict[str, Path]:
        inventory_csv = self.appendix_dir / "public_obs_inventory.csv"
        inventory_json = self.appendix_dir / "public_obs_inventory.json"
        df = pd.DataFrame([row.to_dict() for row in rows])
        _write_csv(inventory_csv, df)
        _write_json(inventory_json, df.to_dict(orient="records"))
        return {"csv": inventory_csv, "json": inventory_json}

    def _load_forecast_members(self) -> list[Path]:
        ensemble_dir = get_case_output_dir(self.case.run_name) / "ensemble"
        members = sorted(ensemble_dir.glob("member_*.nc"))
        if not members:
            raise FileNotFoundError(
                "Appendix date composites require official ensemble member NetCDF outputs. "
                f"None found under {ensemble_dir}"
            )
        return members

    def _build_date_composite_from_members(self, target_date: str, out_threshold_path: Path, out_probability_path: Path) -> None:
        members = self._load_forecast_members()
        date_value = pd.Timestamp(target_date).date()
        member_masks: list[np.ndarray] = []

        for member_path in members:
            composite = np.zeros((self.grid.height, self.grid.width), dtype=np.float32)
            with xr.open_dataset(member_path) as ds:
                if "time" not in ds.coords:
                    raise ValueError(f"{member_path} is missing a time coordinate.")
                times = pd.DatetimeIndex(pd.to_datetime(ds["time"].values))
                if times.tz is not None:
                    times = times.tz_convert("UTC").tz_localize(None)
                indices = [idx for idx, value in enumerate(times) if value.date() == date_value]
                for idx in indices:
                    lon = np.asarray(ds["lon"].isel(time=idx).values).reshape(-1)
                    lat = np.asarray(ds["lat"].isel(time=idx).values).reshape(-1)
                    status = np.asarray(ds["status"].isel(time=idx).values).reshape(-1)
                    valid = ~np.isnan(lon) & ~np.isnan(lat) & (status == 0)
                    if not np.any(valid):
                        continue
                    from src.helpers.raster import rasterize_particles

                    hits, _ = rasterize_particles(
                        self.grid,
                        lon[valid],
                        lat[valid],
                        np.ones(np.count_nonzero(valid), dtype=np.float32),
                    )
                    composite = np.maximum(composite, hits.astype(np.float32))
            member_masks.append(composite.astype(np.float32))

        probability = np.mean(np.stack(member_masks, axis=0), axis=0).astype(np.float32)
        probability = apply_ocean_mask(probability, sea_mask=self.sea_mask, fill_value=0.0)
        threshold_mask = apply_ocean_mask((probability >= 0.5).astype(np.float32), sea_mask=self.sea_mask, fill_value=0.0)
        save_raster(self.grid, probability.astype(np.float32), out_probability_path)
        save_raster(self.grid, threshold_mask.astype(np.float32), out_threshold_path)

    def _build_forecast_date_composites(self, accepted_rows: list[InventoryRow]) -> dict[str, dict[str, Path]]:
        distinct_dates = sorted({row.obs_date for row in accepted_rows if row.obs_date})
        outputs: dict[str, dict[str, Path]] = {}
        official_main_datecomposite = get_official_mask_p50_datecomposite_path(self.case.run_name)

        for target_date in distinct_dates:
            threshold_path = self.forecast_date_dir / f"mask_p50_{target_date}_datecomposite.tif"
            probability_path = self.forecast_date_dir / f"prob_presence_{target_date}_datecomposite.tif"
            if target_date == self.validation_date and official_main_datecomposite.exists():
                shutil.copyfile(official_main_datecomposite, threshold_path)
                ensemble_manifest = _load_json(get_ensemble_manifest_path(self.case.run_name))
                copied_probability = False
                for product in ensemble_manifest.get("products") or []:
                    if str(product.get("product_type")) == "prob_presence_datecomposite" and str(product.get("date_utc")) == target_date:
                        prob_src = get_case_output_dir(self.case.run_name) / str(product.get("relative_path") or "")
                        if prob_src.exists():
                            shutil.copyfile(prob_src, probability_path)
                            copied_probability = True
                            break
                if not copied_probability:
                    self._build_date_composite_from_members(target_date, threshold_path, probability_path)
                threshold_mask = apply_ocean_mask(
                    self.phase3b_helper._read_mask(threshold_path),
                    sea_mask=self.sea_mask,
                    fill_value=0.0,
                )
                save_raster(self.grid, threshold_mask.astype(np.float32), threshold_path)
                if probability_path.exists():
                    probability_data = apply_ocean_mask(
                        self.phase3b_helper._read_mask(probability_path),
                        sea_mask=self.sea_mask,
                        fill_value=0.0,
                    )
                    save_raster(self.grid, probability_data.astype(np.float32), probability_path)
            else:
                self._build_date_composite_from_members(target_date, threshold_path, probability_path)

            outputs[target_date] = {"mask_p50": threshold_path, "prob_presence": probability_path}

        return outputs

    def _score_binary_pair(
        self,
        *,
        pair_id: str,
        pair_role: str,
        source_name: str,
        provider: str,
        obs_date: str,
        forecast_path: Path,
        observation_path: Path,
        source_semantics: str,
        score_group: str,
    ) -> tuple[dict, list[dict], dict]:
        precheck_base = self.precheck_dir / pair_id
        precheck = precheck_same_grid(forecast_path, observation_path, report_base_path=precheck_base)
        if not precheck.passed:
            raise RuntimeError(f"Appendix pair {pair_id} failed same-grid precheck: {precheck.json_report_path}")

        forecast_data = self.phase3b_helper._load_binary_score_mask(forecast_path)
        obs_data = self.phase3b_helper._load_binary_score_mask(observation_path)
        diagnostics = self.phase3b_helper._compute_mask_diagnostics(forecast_data, obs_data)

        fss_rows: list[dict] = []
        summary_fss = {}
        for window_km in OFFICIAL_PHASE3B_WINDOWS_KM:
            window_cells = self.phase3b_helper._window_km_to_cells(int(window_km))
            fss = float(
                np.clip(
                    calculate_fss(
                        forecast_data,
                        obs_data,
                        window=window_cells,
                        valid_mask=(self.sea_mask > 0.5) if self.sea_mask is not None else None,
                    ),
                    0.0,
                    1.0,
                )
            )
            summary_fss[f"fss_{window_km}km"] = fss
            fss_rows.append(
                {
                    "score_group": score_group,
                    "pair_id": pair_id,
                    "pair_role": pair_role,
                    "obs_date": obs_date,
                    "source_name": source_name,
                    "provider": provider,
                    "window_km": int(window_km),
                    "fss": fss,
                    "forecast_path": str(forecast_path),
                    "observation_path": str(observation_path),
                }
            )

        summary_row = {
            "score_group": score_group,
            "pair_id": pair_id,
            "pair_role": pair_role,
            "obs_date": obs_date,
            "source_name": source_name,
            "provider": provider,
            "forecast_path": str(forecast_path),
            "observation_path": str(observation_path),
            "precheck_csv": str(precheck.csv_report_path),
            "precheck_json": str(precheck.json_report_path),
            **summary_fss,
            **diagnostics,
        }
        pairing_row = {
            "score_group": score_group,
            "pair_id": pair_id,
            "pair_role": pair_role,
            "obs_date": obs_date,
            "source_name": source_name,
            "provider": provider,
            "forecast_product": forecast_path.name,
            "forecast_path": str(forecast_path),
            "observation_product": observation_path.name,
            "observation_path": str(observation_path),
            "metric": "FSS",
            "windows_km": ",".join(str(value) for value in OFFICIAL_PHASE3B_WINDOWS_KM),
            "source_semantics": source_semantics,
            "precheck_csv": str(precheck.csv_report_path),
            "precheck_json": str(precheck.json_report_path),
        }
        return pairing_row, fss_rows, summary_row

    def _write_perdate_scores(
        self,
        accepted_rows: list[InventoryRow],
        forecast_datecomposites: dict[str, dict[str, Path]],
    ) -> dict[str, Path]:
        pairing_rows: list[dict] = []
        fss_rows: list[dict] = []
        summary_rows: list[dict] = []

        for row in accepted_rows:
            forecast_path = forecast_datecomposites[row.obs_date]["mask_p50"]
            observation_path = Path(row.appendix_obs_mask)
            pair_id = f"appendix_perdate_{row.source_key}"
            pairing_row, pair_fss_rows, summary_row = self._score_binary_pair(
                pair_id=pair_id,
                pair_role="secondary_appendix_perdate",
                source_name=row.source_name,
                provider=row.provider,
                obs_date=row.obs_date,
                forecast_path=forecast_path,
                observation_path=observation_path,
                source_semantics=f"appendix_perdate_{row.obs_date}",
                score_group="appendix_perdate",
            )
            pairing_rows.append(pairing_row)
            fss_rows.extend(pair_fss_rows)
            summary_rows.append(summary_row)

        diagnostics_df = pd.DataFrame(summary_rows)
        pairing_path = self.appendix_dir / "appendix_perdate_pairing_manifest.csv"
        fss_path = self.appendix_dir / "appendix_perdate_fss_by_date_window.csv"
        diagnostics_path = self.appendix_dir / "appendix_perdate_diagnostics.csv"
        summary_path = self.appendix_dir / "appendix_perdate_summary.csv"
        _write_csv(pairing_path, pd.DataFrame(pairing_rows))
        _write_csv(fss_path, pd.DataFrame(fss_rows))
        _write_csv(diagnostics_path, diagnostics_df)
        _write_csv(summary_path, diagnostics_df)
        return {
            "pairing_manifest": pairing_path,
            "fss_by_window": fss_path,
            "diagnostics": diagnostics_path,
            "summary": summary_path,
        }

    def _write_event_corridor_outputs(
        self,
        accepted_rows: list[InventoryRow],
        forecast_datecomposites: dict[str, dict[str, Path]],
    ) -> dict[str, Path]:
        obs_union = np.zeros((self.grid.height, self.grid.width), dtype=np.float32)
        model_union = np.zeros((self.grid.height, self.grid.width), dtype=np.float32)

        for row in accepted_rows:
            obs_union = np.maximum(
                obs_union,
                self.phase3b_helper._load_binary_score_mask(Path(row.appendix_obs_mask)),
            )
            model_union = np.maximum(
                model_union,
                self.phase3b_helper._load_binary_score_mask(forecast_datecomposites[row.obs_date]["mask_p50"]),
            )

        obs_union_path = self.appendix_dir / "appendix_eventcorridor_obs_union_2023-03-03_to_2023-03-06.tif"
        model_union_path = self.appendix_dir / "appendix_eventcorridor_model_union_2023-03-03_to_2023-03-06.tif"
        obs_union = apply_ocean_mask(obs_union, sea_mask=self.sea_mask, fill_value=0.0)
        model_union = apply_ocean_mask(model_union, sea_mask=self.sea_mask, fill_value=0.0)
        save_raster(self.grid, obs_union.astype(np.float32), obs_union_path)
        save_raster(self.grid, model_union.astype(np.float32), model_union_path)

        pairing_row, fss_rows, summary_row = self._score_binary_pair(
            pair_id="appendix_eventcorridor_secondary",
            pair_role="event_corridor_sensitivity",
            source_name="Accepted within-horizon public observation union",
            provider="Appendix secondary",
            obs_date="2023-03-03_to_2023-03-06",
            forecast_path=model_union_path,
            observation_path=obs_union_path,
            source_semantics="secondary_appendix_only_event_corridor_sensitivity",
            score_group="appendix_eventcorridor",
        )

        pairing_path = self.appendix_dir / "appendix_eventcorridor_pairing_manifest.csv"
        fss_path = self.appendix_dir / "appendix_eventcorridor_fss_by_window.csv"
        diagnostics_path = self.appendix_dir / "appendix_eventcorridor_diagnostics.csv"
        summary_md = self.appendix_dir / "appendix_eventcorridor_summary.md"

        _write_csv(pairing_path, pd.DataFrame([pairing_row]))
        _write_csv(fss_path, pd.DataFrame(fss_rows))
        _write_csv(diagnostics_path, pd.DataFrame([summary_row]))

        summary_lines = [
            "# Appendix Event-Corridor Sensitivity",
            "",
            "- Label: `secondary appendix-only event_corridor_sensitivity`",
            "- Official main quantitative Phase 3B remains unchanged.",
            f"- Forecast union: `{model_union_path.name}`",
            f"- Observation union: `{obs_union_path.name}`",
            (
                "- FSS(1/3/5/10 km): "
                f"{summary_row['fss_1km']:.4f}, {summary_row['fss_3km']:.4f}, "
                f"{summary_row['fss_5km']:.4f}, {summary_row['fss_10km']:.4f}"
            ),
        ]
        _write_text(summary_md, "\n".join(summary_lines) + "\n")
        return {
            "obs_union": obs_union_path,
            "model_union": model_union_path,
            "pairing_manifest": pairing_path,
            "fss_by_window": fss_path,
            "diagnostics": diagnostics_path,
            "summary_md": summary_md,
        }

    def _build_beyond_horizon_candidates(self, rows: list[InventoryRow]) -> Path:
        candidate_rows = []
        for row in rows:
            if row.within_current_72h_horizon:
                continue
            if row.source_type not in {"feature service", "feature layer"}:
                continue
            if not row.obs_date:
                continue
            if row.geometry_type != "polygon":
                continue
            candidate_rows.append(
                {
                    "source_name": row.source_name,
                    "provider": row.provider,
                    "item_or_layer_id": row.item_or_layer_id,
                    "obs_date": row.obs_date,
                    "source_url": row.source_url,
                    "source_type": row.source_type,
                    "geometry_type": row.geometry_type,
                    "extended_horizon_candidate": True,
                    "scientifically_worthwhile": True,
                    "recommendation_note": (
                        "Public dated polygon exists beyond the current 72 h horizon; "
                        "extended-horizon rerun could be worthwhile after coastal/domain issues are addressed."
                    ),
                }
            )
        out_path = self.appendix_dir / "appendix_beyond_horizon_candidates.csv"
        _write_csv(out_path, pd.DataFrame(candidate_rows).sort_values(by=["obs_date", "source_name"]))
        return out_path

    @staticmethod
    def _render_overlay(ax, forecast_mask: np.ndarray, obs_mask: np.ndarray, title: str) -> None:
        overlap = np.logical_and(forecast_mask > 0, obs_mask > 0)
        canvas = np.ones((forecast_mask.shape[0], forecast_mask.shape[1], 3), dtype=np.float32)
        canvas[obs_mask > 0] = np.array([0.2, 0.45, 0.95], dtype=np.float32)
        canvas[forecast_mask > 0] = np.array([0.95, 0.35, 0.2], dtype=np.float32)
        canvas[overlap] = np.array([0.55, 0.2, 0.75], dtype=np.float32)
        ax.imshow(canvas, origin="upper")
        ax.set_title(title)
        ax.set_axis_off()

    def _write_public_timeline_overlay(
        self,
        out_path: Path,
        accepted_rows: list[InventoryRow],
        forecast_datecomposites: dict[str, dict[str, Path]],
    ) -> None:
        dates = sorted({row.obs_date for row in accepted_rows})
        ncols = 2
        nrows = int(np.ceil(len(dates) / ncols))
        fig, axes = plt.subplots(nrows=nrows, ncols=ncols, figsize=(6 * ncols, 6 * nrows))
        axes_array = np.atleast_1d(axes).reshape(-1)

        for ax, target_date in zip(axes_array, dates):
            model = self.phase3b_helper._to_binary_mask(
                self.phase3b_helper._apply_score_mask(self.phase3b_helper._read_mask(forecast_datecomposites[target_date]["mask_p50"]))
            )
            obs_union = np.zeros_like(model)
            for row in accepted_rows:
                if row.obs_date == target_date:
                    obs_union = np.maximum(
                        obs_union,
                        self.phase3b_helper._load_binary_score_mask(Path(row.appendix_obs_mask)),
                    )
            self._render_overlay(ax, model, obs_union, f"{target_date} appendix public obs vs model")

        for ax in axes_array[len(dates):]:
            ax.set_axis_off()

        fig.tight_layout()
        fig.savefig(out_path, dpi=150, bbox_inches="tight")
        plt.close(fig)

    def _write_eventcorridor_overlay(self, out_path: Path, model_union_path: Path, obs_union_path: Path) -> None:
        fig, ax = plt.subplots(figsize=(8, 8))
        model = self.phase3b_helper._load_binary_score_mask(model_union_path)
        obs = self.phase3b_helper._load_binary_score_mask(obs_union_path)
        self._render_overlay(ax, model, obs, "Appendix event-corridor sensitivity")
        fig.tight_layout()
        fig.savefig(out_path, dpi=150, bbox_inches="tight")
        plt.close(fig)

    def _write_qualitative_outputs(
        self,
        inventory_rows: list[InventoryRow],
        accepted_rows: list[InventoryRow],
        forecast_datecomposites: dict[str, dict[str, Path]],
        eventcorridor_outputs: dict[str, Path],
    ) -> dict[str, Path]:
        qualitative_rows = [
            row for row in inventory_rows
            if row.accept_for_appendix_qualitative and not row.accept_for_appendix_quantitative
        ]
        notes_rows = []
        for row in inventory_rows:
            category = (
                "accepted_quantitative_within_horizon"
                if row.accept_for_appendix_quantitative and row.within_current_72h_horizon
                else ("qualitative_only" if row.accept_for_appendix_qualitative else "rejected")
            )
            notes_rows.append(
                {
                    "source_name": row.source_name,
                    "provider": row.provider,
                    "obs_date": row.obs_date,
                    "category": category,
                    "source_type": row.source_type,
                    "source_url": row.source_url,
                    "note": row.rejection_reason or row.notes,
                }
            )

        notes_csv = self.appendix_dir / "appendix_public_vs_model_notes.csv"
        _write_csv(notes_csv, pd.DataFrame(notes_rows))

        qualitative_json = self.appendix_dir / "appendix_qualitative_public_timeline.json"
        qualitative_md = self.appendix_dir / "appendix_qualitative_public_timeline.md"
        _write_json(qualitative_json, [row.to_dict() for row in qualitative_rows])
        lines = [
            "# Appendix Qualitative Public Timeline",
            "",
            "- Official main quantitative Phase 3B remains the March 6 date-composite P50 vs March 6 ArcGIS observation mask.",
            "- The monitoring app, web map, public timeline inventory, and point-based public observations are qualitative context only.",
            "",
        ]
        for row in qualitative_rows:
            lines.append(
                f"- `{row.obs_date or 'undated'}` | `{row.source_name}` | "
                f"{row.provider} | {row.source_type} | {row.rejection_reason or row.notes}"
            )
        _write_text(qualitative_md, "\n".join(lines) + "\n")

        timeline_overlay = self.appendix_dir / "qa_appendix_public_timeline_overlay.png"
        eventcorridor_overlay = self.appendix_dir / "qa_appendix_eventcorridor_overlay.png"
        if plt is not None:
            self._write_public_timeline_overlay(timeline_overlay, accepted_rows, forecast_datecomposites)
            self._write_eventcorridor_overlay(
                eventcorridor_overlay,
                Path(eventcorridor_outputs["model_union"]),
                Path(eventcorridor_outputs["obs_union"]),
            )

        return {
            "qualitative_md": qualitative_md,
            "qualitative_json": qualitative_json,
            "notes_csv": notes_csv,
            "timeline_overlay": timeline_overlay,
            "eventcorridor_overlay": eventcorridor_overlay,
        }

    def _write_validation_wording(self, accepted_rows: list[InventoryRow], beyond_horizon_path: Path) -> Path:
        wording_path = self.appendix_dir / "appendix_validation_wording.md"
        dates = sorted({row.obs_date for row in accepted_rows})
        lines = [
            "# Appendix Validation Wording",
            "",
            "- Official main quantitative Phase 3B remains `mask_p50_2023-03-06_datecomposite.tif` vs `obs_mask_2023-03-06.tif`.",
            "- The broader Mindoro monitoring app is not itself one scored truth layer.",
            "- Extra public items are appendix-only unless they pass the source-based acceptance rules: public, machine-readable, explicitly dated, observation-derived, reproducibly ingestible, and rasterizable on the canonical scoring grid.",
            (
                "- The within-horizon event-corridor result is a secondary `appendix-only` "
                "`event_corridor_sensitivity`, not a replacement main score."
            ),
            f"- Accepted quantitative within-horizon appendix dates: {', '.join(dates)}.",
            f"- Beyond-horizon candidates are listed separately in `{beyond_horizon_path.name}` and are not mixed into the 72 h score tables.",
        ]
        _write_text(wording_path, "\n".join(lines) + "\n")
        return wording_path

    def _write_recommendation(
        self,
        inventory_rows: list[InventoryRow],
        accepted_rows: list[InventoryRow],
        beyond_horizon_path: Path,
    ) -> Path:
        recommendation_path = self.appendix_dir / "appendix_public_obs_recommendation.md"
        accepted_dates = sorted({row.obs_date for row in accepted_rows})
        accepted_sources = [f"{row.obs_date} {row.source_name}" for row in accepted_rows]
        qualitative_only = sorted(
            {
                row.source_name
                for row in inventory_rows
                if row.accept_for_appendix_qualitative and not row.accept_for_appendix_quantitative
            }
        )
        beyond_df = pd.read_csv(beyond_horizon_path) if beyond_horizon_path.exists() else pd.DataFrame()
        extended_horizon_next = False
        lines = [
            "# Appendix Public Observation Recommendation",
            "",
            f"- Were extra public dates found? `{'yes' if accepted_dates else 'no'}`",
            "- Which were accepted quantitatively?",
            f"  {', '.join(accepted_sources) if accepted_sources else 'None.'}",
            "- Which were qualitative only?",
            f"  {', '.join(qualitative_only) if qualitative_only else 'None.'}",
            (
                "- Is there enough accepted within-horizon evidence to support a stronger appendix result? "
                f"`{'yes' if len(accepted_dates) >= 3 else 'no'}`"
            ),
            (
                "- Is an extended-horizon rerun worth doing next? "
                f"`{'yes' if extended_horizon_next else 'no'}`"
            ),
            "",
            (
                "Recommendation: keep the official main March 6 score unchanged; use the accepted public "
                "within-horizon dates as an appendix-only expansion and event-corridor sensitivity. "
                "Do not prioritize an extended-horizon rerun next while the coastal/shoreline issue remains unresolved, "
                "even though beyond-horizon dated public polygon candidates exist."
            ),
        ]
        if not beyond_df.empty:
            next_date = beyond_df.sort_values(by=["obs_date"]).iloc[0]["obs_date"]
            lines.append(f"Closest beyond-horizon quantitative public date currently inventoried: `{next_date}`.")
        _write_text(recommendation_path, "\n".join(lines) + "\n")
        return recommendation_path

    def run(self) -> dict:
        self._ensure_official_forecast_outputs()

        inventory_rows = self._build_inventory_rows()
        accepted_rows = self._accepted_quantitative_rows(inventory_rows)
        accepted_rows = self._archive_quantitative_rows(accepted_rows)

        updated_inventory: list[InventoryRow] = []
        accepted_by_key = {row.source_key: row for row in accepted_rows}
        for row in inventory_rows:
            updated_inventory.append(accepted_by_key.get(row.source_key, row))

        inventory_paths = self._write_inventory(updated_inventory)
        forecast_datecomposites = self._build_forecast_date_composites(accepted_rows)
        perdate_outputs = self._write_perdate_scores(accepted_rows, forecast_datecomposites)
        eventcorridor_outputs = self._write_event_corridor_outputs(accepted_rows, forecast_datecomposites)
        beyond_horizon_path = self._build_beyond_horizon_candidates(updated_inventory)
        qualitative_outputs = self._write_qualitative_outputs(
            updated_inventory,
            accepted_rows,
            forecast_datecomposites,
            eventcorridor_outputs,
        )
        wording_path = self._write_validation_wording(accepted_rows, beyond_horizon_path)
        recommendation_path = self._write_recommendation(updated_inventory, accepted_rows, beyond_horizon_path)

        self._verify_locked_phase3b_files_unchanged()

        result = {
            "inventory_csv": inventory_paths["csv"],
            "inventory_json": inventory_paths["json"],
            "accepted_quantitative_dates": sorted({row.obs_date for row in accepted_rows}),
            "accepted_quantitative_sources": [row.source_name for row in accepted_rows],
            "forecast_datecomposites": {key: {k: str(v) for k, v in value.items()} for key, value in forecast_datecomposites.items()},
            "perdate_outputs": {key: str(value) for key, value in perdate_outputs.items()},
            "eventcorridor_outputs": {key: str(value) for key, value in eventcorridor_outputs.items()},
            "qualitative_outputs": {key: str(value) for key, value in qualitative_outputs.items()},
            "beyond_horizon_candidates": str(beyond_horizon_path),
            "validation_wording": str(wording_path),
            "recommendation": str(recommendation_path),
            "forecast_generated_during_run": self.forecast_generated_during_run,
        }
        _write_json(self.appendix_dir / "public_obs_appendix_manifest.json", result)
        return result


def run_public_obs_appendix() -> dict:
    return PublicObservationAppendixService().run()


if __name__ == "__main__":  # pragma: no cover - manual execution entrypoint
    print(json.dumps(run_public_obs_appendix(), indent=2, default=str))
