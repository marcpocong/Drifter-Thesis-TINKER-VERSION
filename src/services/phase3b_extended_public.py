"""Extended-horizon public-observation validation guardrail for Mindoro."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import requests

from src.core.case_context import get_case_context
from src.helpers.raster import GridBuilder, rasterize_observation_layer, save_raster
from src.helpers.scoring import apply_ocean_mask, load_sea_mask_array
from src.services.arcgis import (
    _infer_source_crs,
    _repair_degree_scaled_geometries,
    _sanitize_vector_columns_for_gpkg,
    clean_arcgis_geometries,
)
from src.services.phase3b_multidate_public import (
    SOURCE_TAXONOMY_MODELED,
    SOURCE_TAXONOMY_OBS,
    SOURCE_TAXONOMY_QUALITATIVE,
    _as_bool,
    _hash_file,
    _is_modeled_forecast_row,
)
from src.utils.io import get_case_output_dir, get_forcing_files, resolve_recipe_selection

try:
    import geopandas as gpd
except ImportError:  # pragma: no cover
    gpd = None

try:
    import matplotlib.pyplot as plt
except ImportError:  # pragma: no cover
    plt = None

try:
    import xarray as xr
except ImportError:  # pragma: no cover
    xr = None


EXTENDED_DIR_NAME = "phase3b_extended_public"
CURRENT_HORIZON_END_DATE = "2023-03-06"
INIT_DATE = "2023-03-03"
DEFAULT_EXTENDED_MAX_DATE = "2023-03-31"
REQUEST_TIMEOUT = 60
MARCH23_NOAA_MSI_SOURCE_KEY = "659af48ef2f243e89409ce5e73dd0b66"
MARCH23_NOAA_MSI_SOURCE_DATE = "2023-03-23"
MARCH23_NOAA_MSI_WHITELIST_REASON = (
    "accepted only in the beyond-horizon public-observation lane because its metadata cites an external "
    "NOAA/NESDIS satellite surveillance report rather than an MSI trajectory forecast product"
)


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


def _extended_public_source_key(row: pd.Series | dict) -> str:
    for key in ("source_key", "item_or_layer_id", "item_id or layer_id"):
        value = str(row.get(key, "") or "").split(":")[0].strip()
        if value:
            return value
    return ""


def _is_march23_noaa_msi_whitelist_row(row: pd.Series | dict) -> bool:
    return (
        _extended_public_source_key(row) == MARCH23_NOAA_MSI_SOURCE_KEY
        and str(row.get("obs_date", "") or "").strip() == MARCH23_NOAA_MSI_SOURCE_DATE
    )


def classify_extended_public_source(row: pd.Series | dict, max_date: str = DEFAULT_EXTENDED_MAX_DATE) -> tuple[str, str, bool]:
    """Classify and decide source-based extended quantitative acceptance."""
    obs_date = str(row.get("obs_date", "") or "").strip()
    machine = _as_bool(row.get("machine_readable"))
    public = _as_bool(row.get("public"))
    obs_derived = _as_bool(row.get("observation_derived"))
    reproducible = _as_bool(row.get("reproducibly_ingestible"))
    geometry_type = str(row.get("geometry_type", "") or "").lower()
    service_url = str(row.get("service_url", "") or "")
    layer_id_value = row.get("layer_id", "")
    whitelisted_march23_source = _is_march23_noaa_msi_whitelist_row(row)

    if not obs_date:
        return SOURCE_TAXONOMY_QUALITATIVE, "source is not explicitly dated", False
    if obs_date <= CURRENT_HORIZON_END_DATE:
        role = "initialization date" if obs_date == INIT_DATE else "within-horizon date"
        return SOURCE_TAXONOMY_QUALITATIVE, f"{role}; handled by existing Phase 3B tracks", False
    if obs_date > max_date:
        return SOURCE_TAXONOMY_QUALITATIVE, f"outside configured extended March pilot window ending {max_date}", False
    if _is_modeled_forecast_row(row) and not whitelisted_march23_source:
        return SOURCE_TAXONOMY_MODELED, "modeled forecast / trajectory source excluded from truth", False
    if not (machine and public and obs_derived and reproducible):
        return SOURCE_TAXONOMY_QUALITATIVE, "does not satisfy public/machine-readable/observation-derived/reproducible requirements", False
    if geometry_type not in {"polygon", "multipolygon"}:
        return SOURCE_TAXONOMY_QUALITATIVE, "not a polygonal spill-extent layer", False
    if not service_url or _coerce_layer_id(layer_id_value) is None:
        return SOURCE_TAXONOMY_QUALITATIVE, "missing reproducible service URL or layer id", False
    if whitelisted_march23_source:
        return (
            SOURCE_TAXONOMY_OBS,
            "accepted beyond-horizon dated observation-derived polygon layer via one-off March 23 NOAA/NESDIS whitelist; "
            f"{MARCH23_NOAA_MSI_WHITELIST_REASON}",
            True,
        )
    return SOURCE_TAXONOMY_OBS, "accepted beyond-horizon dated observation-derived polygon layer", True


def _forcing_time_bounds(path: Path) -> dict:
    result = {"path": str(path), "exists": path.exists(), "time_start_utc": "", "time_end_utc": "", "covers_extended_end": False}
    if not path.exists() or xr is None:
        return result
    with xr.open_dataset(path) as ds:
        time_name = next((name for name in ("time", "valid_time") if name in ds.coords or name in ds.dims), None)
        if not time_name:
            return result
        times = pd.DatetimeIndex(pd.to_datetime(ds[time_name].values))
        if times.tz is not None:
            times = times.tz_convert("UTC").tz_localize(None)
        if len(times):
            result["time_start_utc"] = pd.Timestamp(times.min()).strftime("%Y-%m-%dT%H:%M:%SZ")
            result["time_end_utc"] = pd.Timestamp(times.max()).strftime("%Y-%m-%dT%H:%M:%SZ")
    return result


class Phase3BExtendedPublicService:
    def __init__(self):
        self.case = get_case_context()
        if not self.case.is_official:
            raise RuntimeError("phase3b_extended_public is only supported for official Mindoro workflows.")
        if gpd is None:
            raise ImportError("geopandas is required for phase3b_extended_public.")
        self.case_output_dir = get_case_output_dir(self.case.run_name)
        self.appendix_dir = self.case_output_dir / "public_obs_appendix"
        self.multidate_dir = self.case_output_dir / "phase3b_multidate_public"
        self.output_dir = self.case_output_dir / EXTENDED_DIR_NAME
        self.raw_dir = self.output_dir / "raw"
        self.processed_dir = self.output_dir / "processed_vectors"
        self.mask_dir = self.output_dir / "accepted_obs_masks"
        self.precheck_dir = self.output_dir / "precheck"
        for path in (self.output_dir, self.raw_dir, self.processed_dir, self.mask_dir, self.precheck_dir):
            path.mkdir(parents=True, exist_ok=True)
        self.grid = GridBuilder()
        self.sea_mask = load_sea_mask_array(self.grid.spec)
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "mindoro-phase3b-extended-public/1.0"})

    def run(self) -> dict:
        strict_before = self._strict_hashes()
        multidate_before = self._multidate_hashes()
        inventory = self._load_inventory()
        acceptance = self._build_acceptance_registry(inventory)
        accepted = acceptance[acceptance["accepted_for_extended_quantitative"]].copy()

        inventory_path = self.output_dir / "extended_public_obs_inventory.csv"
        registry_path = self.output_dir / "extended_public_obs_acceptance_registry.csv"
        inventory.to_csv(inventory_path, index=False)

        if accepted.empty:
            acceptance.to_csv(registry_path, index=False)
            not_possible = self._write_not_possible(
                accepted_dates=[],
                reasons=["No beyond-horizon public layers satisfied the quantitative observation-derived source rules."],
                forcing_check={},
            )
            manifest = self._write_manifest(
                accepted_dates=[],
                status="not_possible_no_accepted_observations",
                inventory_path=inventory_path,
                registry_path=registry_path,
                not_possible_path=not_possible,
                forcing_check={},
                strict_before=strict_before,
                multidate_before=multidate_before,
            )
            return self._result([], not_possible, manifest)

        processed = self._archive_and_rasterize_accepted(accepted)
        processed.to_csv(registry_path, index=False)
        accepted_dates = sorted(processed.loc[processed["mask_exists"], "obs_date"].astype(str).unique().tolist())
        latest_date = accepted_dates[-1] if accepted_dates else ""
        forcing_check = self._forcing_preflight(latest_date)

        self._write_empty_score_tables(processed, "; ".join(forcing_check["blocking_reasons"]))
        not_possible = self._write_not_possible(
            accepted_dates=accepted_dates,
            reasons=forcing_check["blocking_reasons"],
            forcing_check=forcing_check,
        )
        self._write_methodology_memo(accepted_dates, forcing_check)
        manifest = self._write_manifest(
            accepted_dates=accepted_dates,
            status="not_possible_forcing_coverage" if forcing_check["blocking_reasons"] else "ready_for_extended_rerun",
            inventory_path=inventory_path,
            registry_path=registry_path,
            not_possible_path=not_possible,
            forcing_check=forcing_check,
            strict_before=strict_before,
            multidate_before=multidate_before,
        )
        return self._result(accepted_dates, not_possible, manifest)

    def _load_inventory(self) -> pd.DataFrame:
        inventory_path = self.appendix_dir / "public_obs_inventory.csv"
        if not inventory_path.exists():
            raise FileNotFoundError(f"Public observation inventory not found: {inventory_path}")
        return pd.read_csv(inventory_path)

    def _build_acceptance_registry(self, inventory: pd.DataFrame) -> pd.DataFrame:
        rows: list[dict] = []
        for _, row in inventory.iterrows():
            taxonomy, reason, accepted = classify_extended_public_source(row)
            whitelist_applied = _is_march23_noaa_msi_whitelist_row(row)
            rows.append(
                {
                    **row.to_dict(),
                    "source_taxonomy": taxonomy,
                    "extended_acceptance_reason": reason,
                    "accepted_for_extended_quantitative": bool(accepted),
                    "extended_truth_exception_applied": bool(whitelist_applied),
                    "extended_truth_exception_note": MARCH23_NOAA_MSI_WHITELIST_REASON if whitelist_applied else "",
                    "mask_exists": False,
                    "extended_raw_geojson": "",
                    "extended_item_metadata": "",
                    "extended_layer_metadata": "",
                    "extended_processed_vector": "",
                    "extended_obs_mask": "",
                    "raster_nonzero_cells": "",
                    "scoreable_after_rasterization": False,
                    "scoreability_note": "",
                    "processing_status": "pending" if accepted else "not_accepted",
                    "processing_error": "",
                }
            )
        return pd.DataFrame(rows)

    def _archive_and_rasterize_accepted(self, accepted: pd.DataFrame) -> pd.DataFrame:
        processed_rows: list[dict] = []
        for _, row in accepted.iterrows():
            record = row.to_dict()
            source_key = str(record.get("source_key") or record.get("item_or_layer_id") or "").strip()
            if not source_key:
                source_key = str(record.get("item_id or layer_id") or "extended_public_source").split(":")[0]
            source_key = _safe_name(source_key)
            service_url = str(record.get("service_url") or "").strip()
            layer_id = _coerce_layer_id(record.get("layer_id"))
            item_id = _coerce_item_id(record)

            try:
                if not service_url or layer_id is None:
                    raise ValueError("accepted source is missing service_url or layer_id")

                item_meta = self._fetch_arcgis_item(item_id) if item_id else {}
                layer_meta = self._fetch_service_layer(service_url, layer_id)
                raw_geojson = self._fetch_geojson(service_url, layer_id)

                raw_dir = self.raw_dir / source_key
                raw_dir.mkdir(parents=True, exist_ok=True)
                item_meta_path = raw_dir / f"{source_key}_item_metadata.json"
                layer_meta_path = raw_dir / f"{source_key}_layer_metadata.json"
                raw_geojson_path = raw_dir / f"{source_key}_raw.geojson"
                _write_json(item_meta_path, item_meta)
                _write_json(layer_meta_path, layer_meta)
                _write_json(raw_geojson_path, raw_geojson)

                raw_gdf = _geojson_to_gdf(raw_geojson)
                inferred_crs, inferred_notes = _infer_source_crs(raw_gdf, layer_meta, raw_geojson)
                raw_gdf = raw_gdf.set_crs(inferred_crs, allow_override=True)
                raw_gdf, rescue_notes = _repair_degree_scaled_geometries(
                    raw_gdf,
                    metadata=layer_meta,
                    payload=raw_geojson,
                    expected_region=list(self.case.region),
                )
                cleaned_gdf, qa = clean_arcgis_geometries(
                    raw_gdf=raw_gdf,
                    expected_geometry_type="polygon",
                    source_crs=inferred_crs,
                    target_crs=self.grid.crs,
                )
                if cleaned_gdf.empty:
                    raise ValueError("no valid polygon geometry remained after cleaning")

                processed_path = self.processed_dir / f"{source_key}.gpkg"
                if processed_path.exists():
                    processed_path.unlink()
                _sanitize_vector_columns_for_gpkg(cleaned_gdf).to_file(processed_path, driver="GPKG")

                mask = rasterize_observation_layer(cleaned_gdf, self.grid)
                mask = apply_ocean_mask(mask, sea_mask=self.sea_mask, fill_value=0.0)
                mask_path = self.mask_dir / f"{source_key}.tif"
                save_raster(self.grid, mask.astype(np.float32), mask_path)

                raster_nonzero_cells = int(np.count_nonzero(mask > 0))
                scoreable_after_rasterization = raster_nonzero_cells > 0
                scoreability_note = (
                    ""
                    if scoreable_after_rasterization
                    else "source not scoreable after rasterization: zero ocean cells remained after applying the canonical ocean mask"
                )
                notes = "; ".join(inferred_notes + rescue_notes)
                qa_notes = "; ".join(f"{key}={value}" for key, value in qa.items())
                record.update(
                    {
                        "source_key": source_key,
                        "extended_raw_geojson": str(raw_geojson_path),
                        "extended_item_metadata": str(item_meta_path),
                        "extended_layer_metadata": str(layer_meta_path),
                        "extended_processed_vector": str(processed_path),
                        "extended_obs_mask": str(mask_path),
                        "raw_feature_count": int(len(raw_geojson.get("features") or [])),
                        "processed_feature_count": int(len(cleaned_gdf)),
                        "raw_crs": inferred_crs,
                        "processed_crs": self.grid.crs,
                        "raster_nonzero_cells": raster_nonzero_cells,
                        "scoreable_after_rasterization": bool(scoreable_after_rasterization),
                        "scoreability_note": scoreability_note,
                        "mask_exists": bool(mask_path.exists()),
                        "processing_status": "processed",
                        "processing_error": "",
                        "processing_notes": " | ".join(part for part in (notes, qa_notes, scoreability_note) if part),
                    }
                )
            except Exception as exc:
                record.update(
                    {
                        "source_key": source_key,
                        "mask_exists": False,
                        "raster_nonzero_cells": 0,
                        "scoreable_after_rasterization": False,
                        "scoreability_note": "",
                        "processing_status": "failed",
                        "processing_error": str(exc),
                    }
                )
            processed_rows.append(record)
        return pd.DataFrame(processed_rows)

    def _item_url(self, item_id: str) -> str:
        return f"https://www.arcgis.com/sharing/rest/content/items/{item_id}"

    def _fetch_arcgis_item(self, item_id: str) -> dict:
        response = self.session.get(self._item_url(item_id), params={"f": "json"}, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        return response.json() or {}

    def _fetch_service_layer(self, service_url: str, layer_id: int) -> dict:
        response = self.session.get(
            f"{service_url.rstrip('/')}/{int(layer_id)}",
            params={"f": "json"},
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        return response.json() or {}

    def _fetch_geojson(self, service_url: str, layer_id: int) -> dict:
        response = self.session.get(
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
        response.raise_for_status()
        return response.json() or {}

    def _forcing_preflight(self, latest_date: str) -> dict:
        selection = resolve_recipe_selection()
        forcing = get_forcing_files(selection.recipe)
        target_end = pd.Timestamp(f"{latest_date}T09:59:00Z") if latest_date else pd.Timestamp(self.case.simulation_end_utc)
        if target_end.tzinfo is not None:
            target_end = target_end.tz_convert("UTC").tz_localize(None)

        rows = []
        blocking_reasons: list[str] = []
        for label, path in (
            ("current", forcing.get("currents")),
            ("wind", forcing.get("wind")),
            ("wave", forcing.get("wave")),
        ):
            path = Path(path) if path else None
            info = _forcing_time_bounds(path) if path is not None else {"path": "", "exists": False}
            time_end = pd.Timestamp(info["time_end_utc"]) if info.get("time_end_utc") else None
            if time_end is not None and time_end.tzinfo is not None:
                time_end = time_end.tz_convert("UTC").tz_localize(None)
            covers = bool(time_end is not None and time_end >= target_end)
            info.update(
                {
                    "forcing_kind": label,
                    "required_for_extended_official_track": True,
                    "requested_extended_end_utc": target_end.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "covers_extended_end": covers,
                }
            )
            if not info.get("exists"):
                blocking_reasons.append(f"{label} forcing file is missing: {path}")
            elif not covers:
                blocking_reasons.append(
                    f"{label} forcing ends at {info.get('time_end_utc') or 'unknown'}, before requested extended end "
                    f"{info['requested_extended_end_utc']}"
                )
            rows.append(info)

        csv_path = self.output_dir / "extended_forcing_preflight.csv"
        json_path = self.output_dir / "extended_forcing_preflight.json"
        pd.DataFrame(rows).to_csv(csv_path, index=False)
        payload = {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "selected_recipe": selection.recipe,
            "selection_source": selection.source_kind,
            "latest_accepted_observation_date": latest_date,
            "requested_extended_end_utc": target_end.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "forcing": rows,
            "blocking_reasons": blocking_reasons,
            "can_run_extended_forecast": not blocking_reasons,
            "policy": (
                "Official extended scoring requires actual current, wind, and wave/Stokes coverage. "
                "No persistence/extrapolation beyond local forcing coverage is allowed here."
            ),
        }
        _write_json(json_path, payload)
        payload["csv_path"] = str(csv_path)
        payload["json_path"] = str(json_path)
        return payload

    def _write_empty_score_tables(self, accepted: pd.DataFrame | None = None, block_reason: str = "") -> dict[str, Path]:
        paths = {
            "pairing_manifest": self.output_dir / "phase3b_extended_pairing_manifest.csv",
            "fss_by_date_window": self.output_dir / "phase3b_extended_fss_by_date_window.csv",
            "diagnostics": self.output_dir / "phase3b_extended_diagnostics.csv",
            "summary": self.output_dir / "phase3b_extended_summary.csv",
            "eventcorridor_summary": self.output_dir / "phase3b_extended_eventcorridor_summary.csv",
        }
        pairing_rows = []
        diagnostics_rows = []
        if accepted is not None and not accepted.empty:
            valid = accepted[accepted["mask_exists"].astype(bool)].copy()
            for _, row in valid.iterrows():
                mask_path = Path(str(row.get("extended_obs_mask") or ""))
                pair_id = f"phase3b_extended_blocked_{row.get('source_key', '')}"
                pairing_rows.append(
                    {
                        "pair_id": pair_id,
                        "pair_role": "extended_horizon_per_source_pending_forecast",
                        "obs_date": row.get("obs_date", ""),
                        "source_key": row.get("source_key", ""),
                        "source_name": row.get("source_name", ""),
                        "forecast_product": "",
                        "forecast_path": "",
                        "observation_product": mask_path.name,
                        "observation_path": str(mask_path),
                        "metric": "FSS",
                        "windows_km": "1,3,5,10",
                        "source_taxonomy": SOURCE_TAXONOMY_OBS,
                        "source_semantics": "extended_public_observation_ready_forecast_blocked_by_forcing_coverage",
                        "status": "blocked_no_extended_forecast",
                        "block_reason": block_reason,
                    }
                )
                diagnostics_rows.append(
                    {
                        "pair_id": pair_id,
                        "obs_date": row.get("obs_date", ""),
                        "forecast_nonzero_cells": "",
                        "obs_nonzero_cells": row.get("raster_nonzero_cells", ""),
                        "area_ratio_forecast_to_obs": "",
                        "centroid_distance_m": "",
                        "iou": "",
                        "dice": "",
                        "nearest_distance_to_obs_m": "",
                        "status": "blocked_no_extended_forecast",
                        "block_reason": block_reason,
                    }
                )

        pd.DataFrame(
            pairing_rows,
            columns=[
                "pair_id",
                "pair_role",
                "obs_date",
                "source_key",
                "source_name",
                "forecast_product",
                "forecast_path",
                "observation_product",
                "observation_path",
                "metric",
                "windows_km",
                "source_taxonomy",
                "source_semantics",
                "status",
                "block_reason",
            ],
        ).to_csv(paths["pairing_manifest"], index=False)
        pd.DataFrame(columns=["pair_id", "obs_date", "window_km", "fss", "status", "block_reason"]).to_csv(
            paths["fss_by_date_window"],
            index=False,
        )
        pd.DataFrame(
            diagnostics_rows,
            columns=[
                "pair_id",
                "obs_date",
                "forecast_nonzero_cells",
                "obs_nonzero_cells",
                "area_ratio_forecast_to_obs",
                "centroid_distance_m",
                "iou",
                "dice",
                "nearest_distance_to_obs_m",
                "status",
                "block_reason",
            ]
        ).to_csv(paths["diagnostics"], index=False)
        dates = []
        if accepted is not None and not accepted.empty:
            dates = sorted(set(str(row.get("obs_date", "")) for _, row in accepted.iterrows()))
        pd.DataFrame(
            [
                {
                    "track": "extended_horizon_public_observation_validation",
                    "accepted_extended_dates": ",".join(date for date in dates if date),
                    "status": "blocked_no_extended_forecast",
                    "reason": block_reason,
                }
            ],
            columns=["track", "accepted_extended_dates", "status", "reason"],
        ).to_csv(paths["summary"], index=False)
        pd.DataFrame(
            [
                {
                    "track": "extended_horizon_eventcorridor",
                    "validation_dates_used": ",".join(date for date in dates if date),
                    "status": "blocked_no_extended_forecast",
                    "reason": block_reason,
                }
            ],
            columns=["track", "validation_dates_used", "status", "reason"],
        ).to_csv(
            paths["eventcorridor_summary"],
            index=False,
        )
        self._write_blocked_qa_plot()
        return paths

    def _write_blocked_qa_plot(self) -> Path | None:
        if plt is None:
            return None
        path = self.output_dir / "qa_phase3b_extended_overlays.png"
        fig, ax = plt.subplots(figsize=(8, 4))
        ax.text(
            0.5,
            0.5,
            "Extended-horizon forecast scoring blocked\nby insufficient local forcing coverage.",
            ha="center",
            va="center",
            fontsize=12,
        )
        ax.set_axis_off()
        fig.tight_layout()
        fig.savefig(path, dpi=150, bbox_inches="tight")
        plt.close(fig)
        return path

    def _write_not_possible(self, accepted_dates: list[str], reasons: list[str], forcing_check: dict) -> Path:
        path = self.output_dir / "extended_public_validation_not_possible.md"
        reason_lines = reasons or ["No accepted beyond-horizon quantitative observation masks are available."]
        lines = [
            "# Extended Public Validation Not Possible From Current Local State",
            "",
            "The extended-horizon public-observation track did not compute FSS values in this run.",
            "",
            "## Accepted Beyond-Horizon Quantitative Dates",
            "",
            f"- {', '.join(accepted_dates) if accepted_dates else 'none'}",
            "",
            "## Blocking Reasons",
            "",
        ]
        lines.extend(f"- {reason}" for reason in reason_lines)
        lines.extend(
            [
                "",
                "## Guardrail",
                "",
                "No modeled forecast products were used as observation truth, and no qualitative-only visualization sources were scored.",
                "No extended forecast was fabricated from stale March 6 products or by extrapolating beyond forcing coverage.",
                "Existing strict March 6 and within-horizon multi-date Phase 3B files were treated as locked inputs and were not modified.",
            ]
        )
        if forcing_check:
            lines.extend(
                [
                    "",
                    "## Requested Extended End",
                    "",
                    f"- {forcing_check.get('requested_extended_end_utc', '')}",
                    f"- Forcing preflight JSON: {forcing_check.get('json_path', '')}",
                ]
            )
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return path

    def _write_methodology_memo(self, accepted_dates: list[str], forcing_check: dict) -> Path:
        path = self.output_dir / "chapter3_phase3b_extended_revision_memo.md"
        lines = [
            "# Chapter 3 Phase 3B Extended Public Validation Memo",
            "",
            "This extended-horizon lane is an event-scale sensitivity/extension track, not a replacement for the locked March 6 stress test or the within-horizon multi-date result.",
            "",
            "- Strict March 6 remains the hardest single-date test.",
            "- Multi-date within-horizon validation remains the main short-range quantitative track.",
            "- Extended-horizon public validation should be reported only after a scientifically valid extended forecast rerun exists.",
            "- March 3 remains initialization consistency only and is not counted as a normal forecast-validation date.",
            "",
            f"Accepted beyond-horizon quantitative observation dates found: {', '.join(accepted_dates) if accepted_dates else 'none'}.",
            "",
            "Current status: extended forecast scoring is blocked by forcing coverage in the local prepared inputs.",
            f"Requested extended end: {forcing_check.get('requested_extended_end_utc', '') if forcing_check else ''}.",
        ]
        path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        return path

    def _strict_hashes(self) -> dict[str, str]:
        files = [
            self.case_output_dir / "phase3b" / "phase3b_pairing_manifest.csv",
            self.case_output_dir / "phase3b" / "phase3b_fss_by_date_window.csv",
            self.case_output_dir / "phase3b" / "phase3b_summary.csv",
            self.case_output_dir / "phase3b" / "phase3b_diagnostics.csv",
            self.case_output_dir / "phase3b" / "phase3b_run_manifest.json",
        ]
        return {str(path): _hash_file(path) for path in files if path.exists()}

    def _multidate_hashes(self) -> dict[str, str]:
        files = [
            self.multidate_dir / "phase3b_multidate_pairing_manifest.csv",
            self.multidate_dir / "phase3b_multidate_fss_by_date_window.csv",
            self.multidate_dir / "phase3b_multidate_summary.csv",
            self.multidate_dir / "phase3b_multidate_diagnostics.csv",
            self.multidate_dir / "phase3b_multidate_run_manifest.json",
            self.multidate_dir / "phase3b_eventcorridor_summary.csv",
        ]
        return {str(path): _hash_file(path) for path in files if path.exists()}

    def _write_manifest(
        self,
        *,
        accepted_dates: list[str],
        status: str,
        inventory_path: Path,
        registry_path: Path,
        not_possible_path: Path,
        forcing_check: dict,
        strict_before: dict[str, str],
        multidate_before: dict[str, str],
    ) -> Path:
        strict_after = self._strict_hashes()
        multidate_after = self._multidate_hashes()
        strict_unchanged = strict_before == strict_after
        multidate_unchanged = multidate_before == multidate_after
        if not strict_unchanged or not multidate_unchanged:
            raise RuntimeError("Extended public validation modified locked Phase 3B outputs.")
        path = self.output_dir / "phase3b_extended_run_manifest.json"
        payload = {
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "workflow_mode": self.case.workflow_mode,
            "run_name": self.case.run_name,
            "track": "extended_horizon_public_observation_validation",
            "status": status,
            "accepted_extended_quantitative_dates": accepted_dates,
            "march3_role": "initialization_consistency_only",
            "truth_source_policy": {
                "observation_derived_quantitative": "public, machine-readable, explicitly dated, observation-derived, reproducibly ingestible, rasterizable",
                "modeled_forecast_exclude_from_truth": "modeled forecasts, trajectory bulletins, and model-generated predictions are excluded from truth",
                "qualitative_context_only": "app/webmap/screenshot/visualization-only sources are context only",
                "extended_one_off_whitelist": {
                    "source_key": MARCH23_NOAA_MSI_SOURCE_KEY,
                    "obs_date": MARCH23_NOAA_MSI_SOURCE_DATE,
                    "applies_only_within_extended_lane": True,
                    "reason": MARCH23_NOAA_MSI_WHITELIST_REASON,
                },
            },
            "strict_march6_files_unchanged": strict_unchanged,
            "within_horizon_multidate_files_unchanged": multidate_unchanged,
            "strict_hashes_before": strict_before,
            "strict_hashes_after": strict_after,
            "multidate_hashes_before": multidate_before,
            "multidate_hashes_after": multidate_after,
            "forcing_preflight": forcing_check,
            "artifacts": {
                "extended_public_obs_inventory": str(inventory_path),
                "extended_public_obs_acceptance_registry": str(registry_path),
                "not_possible_report": str(not_possible_path),
                "forcing_preflight_csv": forcing_check.get("csv_path", ""),
                "forcing_preflight_json": forcing_check.get("json_path", ""),
                "pairing_manifest": str(self.output_dir / "phase3b_extended_pairing_manifest.csv"),
                "fss_by_date_window": str(self.output_dir / "phase3b_extended_fss_by_date_window.csv"),
                "diagnostics": str(self.output_dir / "phase3b_extended_diagnostics.csv"),
                "summary": str(self.output_dir / "phase3b_extended_summary.csv"),
                "eventcorridor_summary": str(self.output_dir / "phase3b_extended_eventcorridor_summary.csv"),
                "qa_overlays": str(self.output_dir / "qa_phase3b_extended_overlays.png"),
                "methodology_sync_memo": str(self.output_dir / "chapter3_phase3b_extended_revision_memo.md"),
            },
        }
        _write_json(path, payload)
        return path

    def _result(self, accepted_dates: list[str], not_possible: Path, manifest: Path) -> dict:
        return {
            "output_dir": self.output_dir,
            "accepted_extended_quantitative_dates": accepted_dates,
            "not_possible_report": not_possible,
            "run_manifest": manifest,
            "status": "not_possible_forcing_coverage" if accepted_dates else "not_possible_no_accepted_observations",
            "headline_fss": "not_computed",
            "strong_enough_to_include": False,
        }


def _safe_name(value: str) -> str:
    chars = [char if char.isalnum() or char in {"-", "_"} else "_" for char in str(value)]
    return "".join(chars).strip("_") or "extended_public_source"


def _coerce_layer_id(value: Any) -> int | None:
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return int(float(text))
    except Exception:
        return None


def _coerce_item_id(row: dict) -> str:
    value = row.get("item_or_layer_id") or row.get("item_id or layer_id") or row.get("source_key") or ""
    return str(value).split(":")[0].strip()


def _geojson_to_gdf(payload: dict):
    features = payload.get("features") or []
    if not features:
        return gpd.GeoDataFrame(geometry=[], crs="EPSG:4326")
    return gpd.GeoDataFrame.from_features(features).set_crs("EPSG:4326", allow_override=True)


def run_phase3b_extended_public() -> dict:
    return Phase3BExtendedPublicService().run()


if __name__ == "__main__":  # pragma: no cover
    result = run_phase3b_extended_public()
    print(json.dumps(result, indent=2, default=_json_default))
