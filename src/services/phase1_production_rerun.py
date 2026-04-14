"""
Dedicated historical/regional Phase 1 production rerun.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from shutil import copy2
from typing import Any

import numpy as np
import pandas as pd
import xarray as xr
import yaml
from erddapy import ERDDAP

try:
    import cdsapi
except ImportError:  # pragma: no cover - runtime dependency
    cdsapi = None

try:
    import copernicusmarine
except ImportError:  # pragma: no cover - runtime dependency
    copernicusmarine = None

from src.core.base import BaseService
from src.core.case_context import get_case_context
from src.core.constants import CMEMS_SURFACE_CURRENT_MAX_DEPTH_M
from src.core.domain_semantics import resolve_phase1_validation_box
from src.exceptions.custom import ForcingOutagePhaseSkipped
from src.services.validation import TransportValidationService
from src.utils.forcing_outage_policy import (
    FORCING_OUTAGE_POLICY_CONTINUE_DEGRADED,
    forcing_factor_id_for_source,
    is_remote_outage_error,
    run_budgeted_forcing_provider_call,
    resolve_forcing_outage_policy,
    resolve_forcing_source_budget_seconds,
    source_id_for_recipe_component,
)
from src.utils.gfs_wind import (
    GFSWindDownloader,
    apply_wind_cf_metadata as _apply_wind_cf_metadata,
    wind_cache_has_reader_metadata as _wind_cache_has_reader_metadata,
)
from src.utils.io import get_official_phase1_recipe_family
from src.utils.startup_prompt_policy import input_cache_policy_force_refresh_enabled

logger = logging.getLogger(__name__)


def _json_default(value: Any) -> Any:
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    if isinstance(value, np.integer):
        return int(value)
    if isinstance(value, np.floating):
        return None if np.isnan(value) else float(value)
    if isinstance(value, np.ndarray):
        return value.tolist()
    return value


def _write_json(path: Path, payload: dict | list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, default=_json_default)
        handle.write("\n")


def _write_yaml(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        yaml.safe_dump(payload, handle, sort_keys=False)


def _relative(repo_root: Path, path: Path) -> str:
    try:
        return str(path.resolve().relative_to(repo_root.resolve())).replace("\\", "/")
    except Exception:
        return str(path).replace("\\", "/")


def _sha256(path: Path) -> str:
    if not path.exists():
        return ""
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _utcnow_isoformat() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(frozen=True)
class SegmentWindow:
    segment_id: str
    drifter_id: str
    start_time: pd.Timestamp
    end_time: pd.Timestamp
    month_key: str


def _budgeted_phase1_forcing_download(
    *,
    repo_root: str,
    config_path: str,
    source_id: str,
    start_time_utc: str,
    end_time_utc: str,
    forcing_dir: str,
) -> dict[str, Any]:
    service = Phase1ProductionRerunService(repo_root=repo_root, config_path=config_path)
    start_time = pd.Timestamp(start_time_utc)
    end_time = pd.Timestamp(end_time_utc)
    target_dir = Path(forcing_dir)
    target_dir.mkdir(parents=True, exist_ok=True)

    try:
        if source_id == "hycom":
            result = service._download_hycom_currents(start_time, end_time, target_dir)
        elif source_id == "cmems":
            result = service._download_cmems_currents(start_time, end_time, target_dir)
        elif source_id == "cmems_wave":
            result = service._download_cmems_wave(start_time, end_time, target_dir)
        elif source_id == "era5":
            result = service._download_era5_winds(start_time, end_time, target_dir)
        elif source_id == "gfs":
            result = service._download_gfs_winds(
                start_time,
                end_time,
                target_dir,
                budget_seconds=resolve_forcing_source_budget_seconds(),
            )
        else:
            raise KeyError(f"Unsupported forcing source id: {source_id}")
    except Exception as exc:
        outage = is_remote_outage_error(exc)
        return {
            "status": "failed_remote_outage" if outage else "failed",
            "source_id": str(source_id),
            "forcing_factor": forcing_factor_id_for_source(source_id),
            "path": str(target_dir / forcing_factor_id_for_source(source_id)),
            "upstream_outage_detected": outage,
            "failure_stage": str(getattr(exc, "failure_stage", "provider_call") or "provider_call"),
            "error": str(exc),
        }

    record = dict(result or {})
    record.setdefault("status", "downloaded")
    record.setdefault("path", str(target_dir / forcing_factor_id_for_source(source_id)))
    return record


class Phase1ProductionRerunService(BaseService):
    NOAA_EMPTY_RESULT_MARKER = "Your query produced no matching results"

    def __init__(
        self,
        *,
        repo_root: str | Path | None = None,
        config_path: str | Path | None = None,
        recipes_path: str | Path = "config/recipes.yaml",
        baseline_path: str | Path = "config/phase1_baseline_selection.yaml",
        validation_service_factory=TransportValidationService,
    ):
        self.repo_root = Path(repo_root or Path(__file__).resolve().parents[2])
        self.case = get_case_context()
        self.pipeline_phase = os.environ.get("PIPELINE_PHASE", "phase1_production_rerun").strip() or "phase1_production_rerun"
        resolved_config = config_path or getattr(self.case, "case_definition_path", None) or "config/phase1_regional_2016_2022.yaml"
        self.config_path = self.repo_root / Path(resolved_config)
        self.recipes_path = self.repo_root / Path(recipes_path)
        self.baseline_path = self.repo_root / Path(baseline_path)

        if not self.config_path.exists():
            raise FileNotFoundError(f"Phase 1 production config not found: {self.config_path}")

        with open(self.config_path, "r", encoding="utf-8") as handle:
            self.config = yaml.safe_load(handle) or {}
        with open(self.recipes_path, "r", encoding="utf-8") as handle:
            self.recipes_config = yaml.safe_load(handle) or {}

        if not self.case.is_historical_regional:
            raise RuntimeError(
                "Phase1ProductionRerunService requires a historical/regional validation workflow mode."
            )

        historical_window = self.config.get("historical_window") or {}
        self.window_start = pd.Timestamp(str(historical_window.get("start_utc"))).tz_convert("UTC")
        self.window_end = pd.Timestamp(str(historical_window.get("end_utc"))).tz_convert("UTC")

        segment_policy = self.config.get("segment_policy") or {}
        self.segment_horizon_hours = int(segment_policy.get("horizon_hours", 72))
        self.segment_timestep_hours = int(segment_policy.get("timestep_hours", 6))
        self.segment_horizon = pd.Timedelta(hours=self.segment_horizon_hours)
        self.expected_delta = pd.Timedelta(hours=self.segment_timestep_hours)

        self.validation_box = [
            float(value)
            for value in resolve_phase1_validation_box(
                self.config,
                {"phase1_validation_box": self.case.phase1_validation_box},
            )
        ]
        halo = float(self.config.get("drifter_acquisition_halo_degrees", 3.0))
        self.drifter_query_box = [
            self.validation_box[0] - halo,
            self.validation_box[1] + halo,
            self.validation_box[2] - halo,
            self.validation_box[3] + halo,
        ]
        forcing_halo = float(self.config.get("forcing_bbox_halo_degrees", 0.5))
        self.forcing_box = [
            self.validation_box[0] - forcing_halo,
            self.validation_box[1] + forcing_halo,
            self.validation_box[2] - forcing_halo,
            self.validation_box[3] + forcing_halo,
        ]
        self.gfs_downloader = GFSWindDownloader(
            forcing_box=self.forcing_box,
            repo_root=self.repo_root,
            expected_delta=self.expected_delta,
        )

        self.output_root = self.repo_root / Path(
            str(self.config.get("output_root") or "output/phase1_production_rerun")
        )
        self.output_root.mkdir(parents=True, exist_ok=True)
        self.scratch_root = self.output_root / "_scratch"
        self.local_input_root = self.repo_root / "data" / "historical_validation_inputs" / str(
            self.case.workflow_mode
        )
        self.drifter_cache_root = self.local_input_root / "drifter_chunks"
        self.forcing_cache_root = self.local_input_root / "forcing_months"
        self.segment_scratch_root = self.scratch_root / "segment_runs"
        self.drifter_cache_root.mkdir(parents=True, exist_ok=True)
        self.forcing_cache_root.mkdir(parents=True, exist_ok=True)
        self.segment_scratch_root.mkdir(parents=True, exist_ok=True)

        self.validation_service = validation_service_factory(recipes_config=str(self.recipes_path))
        configured_family = list(self.config.get("phase1_recipe_family") or [])
        official_family = get_official_phase1_recipe_family(self.recipes_path)
        self.official_recipe_family = configured_family or official_family
        self.required_forcing_source_ids = self._required_forcing_source_ids()
        self.forcing_outage_policy = resolve_forcing_outage_policy(
            workflow_mode=self.case.workflow_mode,
            phase=self.pipeline_phase,
        )
        self.degraded_continue_used = False
        self.upstream_outage_detected = False
        self.missing_forcing_factors: set[str] = set()
        self.skipped_recipe_ids: set[str] = set()
        self.skipped_recipe_missing_factors: dict[str, list[str]] = {}
        self.transport_settings = dict(self.config.get("transport_settings") or {})
        self.ranking_subset_config = dict(self.config.get("ranking_subset") or {})
        self.ranking_subset_label = str(
            self.ranking_subset_config.get("label") or "official_phase1_production"
        )
        self.distance_audit_config = dict(self.config.get("distance_audit_source_point") or {})
        self.distance_audit_source = self._load_distance_audit_source_point()
        self.candidate_baseline_config = dict(self.config.get("candidate_baseline") or {})
        self.gfs_preflight_config = dict(self.config.get("gfs_preflight") or {})
        self.official_baseline_update_config = dict(
            self.config.get("official_baseline_update") or {}
        )
        self.adoption_decision_config = dict(self.config.get("adoption_decision") or {})
        self.required_drifter_fields = [
            str(value)
            for value in ((self.config.get("drifter") or {}).get("required_fields") or [])
        ]
        self.drifter_server = str(
            (self.config.get("drifter") or {}).get("server") or "https://osmc.noaa.gov/erddap"
        )
        self.drifter_dataset_id = str(
            (self.config.get("drifter") or {}).get("dataset_id") or "drifter_6hour_qc"
        )

        self.paths = {
            "drifter_registry": self.output_root / "phase1_drifter_registry.csv",
            "accepted_registry": self.output_root / "phase1_accepted_segment_registry.csv",
            "rejected_registry": self.output_root / "phase1_rejected_segment_registry.csv",
            "ranking_subset_registry": self.output_root / "phase1_ranking_subset_registry.csv",
            "ranking_subset_report": self.output_root / "phase1_ranking_subset_report.md",
            "loading_audit": self.output_root / "phase1_loading_audit.csv",
            "segment_metrics": self.output_root / "phase1_segment_metrics.csv",
            "recipe_summary": self.output_root / "phase1_recipe_summary.csv",
            "recipe_ranking": self.output_root / "phase1_recipe_ranking.csv",
            "manifest": self.output_root / "phase1_production_manifest.json",
            "baseline_candidate": self.output_root / "phase1_baseline_selection_candidate.yaml",
            "gfs_preflight": self.output_root / "phase1_gfs_month_preflight.csv",
            "adoption_decision_json": self.output_root / "phase1_official_adoption_decision.json",
            "adoption_decision_md": self.output_root / "phase1_official_adoption_decision.md",
            "local_input_inventory": self.output_root / "phase1_local_input_inventory.csv",
        }

    def _force_refresh_enabled(self) -> bool:
        return input_cache_policy_force_refresh_enabled()

    def _load_distance_audit_source_point(self) -> dict[str, Any] | None:
        path_text = str(self.distance_audit_config.get("path") or "").strip()
        if not path_text:
            return None

        path = self.repo_root / Path(path_text)
        if not path.exists():
            raise FileNotFoundError(f"Phase 1 distance-audit source point not found: {path}")

        with open(path, "r", encoding="utf-8") as handle:
            payload = json.load(handle) or {}
        features = payload.get("features") or []
        if not features:
            raise RuntimeError(f"Phase 1 distance-audit source point has no features: {path}")
        geometry = (features[0] or {}).get("geometry") or {}
        coordinates = geometry.get("coordinates") or []
        if len(coordinates) < 2:
            raise RuntimeError(f"Phase 1 distance-audit source point is missing lon/lat coordinates: {path}")
        return {
            "path": path,
            "label": str(self.distance_audit_config.get("label") or "source_point"),
            "lon": float(coordinates[0]),
            "lat": float(coordinates[1]),
        }

    @staticmethod
    def _haversine_km(lon1: Any, lat1: Any, lon2: float, lat2: float) -> np.ndarray:
        lon1_arr = np.asarray(lon1, dtype=float)
        lat1_arr = np.asarray(lat1, dtype=float)
        lon2_rad = np.radians(float(lon2))
        lat2_rad = np.radians(float(lat2))
        lon1_rad = np.radians(lon1_arr)
        lat1_rad = np.radians(lat1_arr)
        delta_lon = lon2_rad - lon1_rad
        delta_lat = lat2_rad - lat1_rad
        a = (
            np.sin(delta_lat / 2.0) ** 2
            + np.cos(lat1_rad) * np.cos(lat2_rad) * np.sin(delta_lon / 2.0) ** 2
        )
        return 6371.0088 * 2.0 * np.arcsin(np.minimum(1.0, np.sqrt(a)))

    def _augment_registry_with_distance_audit(self, registry_df: pd.DataFrame) -> pd.DataFrame:
        if registry_df.empty or self.distance_audit_source is None:
            return registry_df

        source_lon = float(self.distance_audit_source["lon"])
        source_lat = float(self.distance_audit_source["lat"])
        start_distance = self._haversine_km(
            registry_df["start_lon"],
            registry_df["start_lat"],
            source_lon,
            source_lat,
        )
        end_distance = self._haversine_km(
            registry_df["end_lon"],
            registry_df["end_lat"],
            source_lon,
            source_lat,
        )
        augmented = registry_df.copy()
        augmented["distance_audit_source_label"] = str(self.distance_audit_source["label"])
        augmented["distance_audit_source_lon"] = source_lon
        augmented["distance_audit_source_lat"] = source_lat
        augmented["distance_to_source_start_km"] = start_distance
        augmented["distance_to_source_end_km"] = end_distance
        augmented["nearest_endpoint_distance_to_source_km"] = np.fmin(start_distance, end_distance)
        augmented["nearest_endpoint_label"] = np.where(start_distance <= end_distance, "start", "end")
        return augmented

    def _select_ranking_subset(self, accepted_df: pd.DataFrame) -> pd.DataFrame:
        if accepted_df.empty:
            return accepted_df.copy()
        if not self.ranking_subset_config:
            return accepted_df.copy()

        mode = str(self.ranking_subset_config.get("mode") or "").strip()
        if mode != "seasonal_start_months":
            raise RuntimeError(f"Unsupported Phase 1 ranking_subset mode '{mode}'.")

        months = {
            int(value)
            for value in (self.ranking_subset_config.get("months") or [])
            if str(value).strip()
        }
        if not months:
            raise RuntimeError("Phase 1 ranking_subset.months must contain at least one calendar month.")

        start_times = pd.to_datetime(accepted_df["start_time_utc"], utc=True, errors="coerce")
        mask = start_times.dt.month.isin(sorted(months))
        return accepted_df.loc[mask].copy().reset_index(drop=True)

    def _annotate_ranking_subset_membership(
        self,
        registry_df: pd.DataFrame,
        ranking_subset_df: pd.DataFrame,
    ) -> pd.DataFrame:
        subset_ids = set(ranking_subset_df["segment_id"].astype(str).tolist())
        annotated = registry_df.copy()
        annotated["ranking_subset_label"] = self.ranking_subset_label
        annotated["ranking_subset_included"] = annotated["segment_id"].astype(str).isin(subset_ids)
        return annotated

    def _write_ranking_subset_report(
        self,
        accepted_df: pd.DataFrame,
        ranking_subset_df: pd.DataFrame,
    ) -> Path:
        months = [
            int(value)
            for value in (self.ranking_subset_config.get("months") or [])
            if str(value).strip()
        ]
        if self.ranking_subset_config:
            lines = [
                "# Phase 1 Ranking Subset Report",
                "",
                f"- Workflow mode: `{self.case.workflow_mode}`",
                f"- Ranking subset label: `{self.ranking_subset_label}`",
                f"- Ranking subset mode: `{self.ranking_subset_config.get('mode', '')}`",
                f"- Ranking subset months: `{', '.join(str(value) for value in months) or 'none'}`",
                f"- Accepted segments in full strict registry: `{len(accepted_df)}`",
                f"- Accepted segments included in ranking subset: `{len(ranking_subset_df)}`",
                f"- Empty-subset behavior: `{self.ranking_subset_config.get('empty_subset_behavior', 'allow')}`",
            ]
        else:
            lines = [
                "# Phase 1 Ranking Subset Report",
                "",
                f"- Workflow mode: `{self.case.workflow_mode}`",
                "- Ranking subset mode: `all_accepted_segments`",
                f"- Accepted segments in full strict registry: `{len(accepted_df)}`",
                f"- Accepted segments included in ranking subset: `{len(ranking_subset_df)}`",
            ]
        if self.distance_audit_source is not None:
            lines.extend(
                [
                    f"- Distance audit source: `{self.distance_audit_source['label']}`",
                    f"- Distance audit source lon/lat: `{self.distance_audit_source['lon']:.6f}, {self.distance_audit_source['lat']:.6f}`",
                    "- Distance audit role: `diagnostic_only`",
                ]
            )
        if ranking_subset_df.empty:
            lines.extend(
                [
                    "",
                    "No accepted drifter segments satisfied the configured ranking subset.",
                    "This workflow stops here instead of silently widening the months, box, or historical window.",
                ]
            )
        self.paths["ranking_subset_report"].write_text("\n".join(lines) + "\n", encoding="utf-8")
        return self.paths["ranking_subset_report"]

    def _gfs_recipes_requested(self) -> bool:
        return any(str(recipe).endswith("_gfs") for recipe in self.official_recipe_family)

    def _require_full_accepted_gfs_preflight(self) -> bool:
        return bool(self.gfs_preflight_config.get("require_full_accepted_month_coverage", False))

    def _allow_secondary_gfs_source(self) -> bool:
        return bool(self.gfs_preflight_config.get("allow_secondary_source", False))

    def _secondary_gfs_source_id(self) -> str:
        return str(self.gfs_preflight_config.get("secondary_source_id") or "ucar_gdex_d084001")

    def _official_baseline_update_enabled(self) -> bool:
        return bool(self.official_baseline_update_config.get("enabled", False))

    def _promote_historical_winner_directly_for_official_b1(self) -> bool:
        return bool(self.official_baseline_update_config.get("promote_historical_winner_directly", False))

    def _gfs_provenance_sidecar_path(self, forcing_dir: Path) -> Path:
        return forcing_dir / "gfs_wind.provenance.json"

    def _default_gfs_provenance_payload(
        self,
        *,
        source_system: str,
        source_tier: str,
        source_inferred: bool,
        output_path: Path,
        required_start: pd.Timestamp,
        required_end: pd.Timestamp,
        cache_inspection: dict[str, Any],
        acquisition_record: dict[str, Any] | None = None,
        primary_failure: str = "",
    ) -> dict[str, Any]:
        payload = {
            "source_system": str(source_system),
            "source_tier": str(source_tier),
            "source_inferred": bool(source_inferred),
            "cache_path": _relative(self.repo_root, output_path),
            "required_start_utc": required_start.isoformat(),
            "required_end_utc": required_end.isoformat(),
            "coverage_start_utc": str(cache_inspection.get("coverage_start_utc") or ""),
            "coverage_end_utc": str(cache_inspection.get("coverage_end_utc") or ""),
            "reader_metadata_ok": bool(cache_inspection.get("reader_metadata_ok", False)),
            "coverage_ok": bool(cache_inspection.get("coverage_ok", False)),
            "written_at_utc": _utcnow_isoformat(),
            "primary_failure": str(primary_failure or ""),
        }
        if acquisition_record:
            payload["acquisition_record"] = acquisition_record
        return payload

    def _read_gfs_provenance_payload(
        self,
        forcing_dir: Path,
    ) -> dict[str, Any] | None:
        path = self._gfs_provenance_sidecar_path(forcing_dir)
        if not path.exists():
            return None
        try:
            with open(path, "r", encoding="utf-8") as handle:
                payload = json.load(handle) or {}
            if not isinstance(payload, dict):
                return None
            return payload
        except Exception:
            return None

    def _write_gfs_provenance_payload(
        self,
        forcing_dir: Path,
        payload: dict[str, Any],
    ) -> Path:
        path = self._gfs_provenance_sidecar_path(forcing_dir)
        _write_json(path, payload)
        return path

    def _legacy_output_monthly_drifter_chunk_path(self, month_key: str) -> Path:
        return self.output_root / "_scratch" / "drifter_chunks" / f"{month_key}.csv"

    def _legacy_output_monthly_forcing_dir(self, month_key: str) -> Path:
        return self.output_root / "_scratch" / "forcing_months" / str(month_key)

    def _stage_legacy_drifter_chunk(
        self,
        *,
        month_key: str,
        cache_path: Path,
    ) -> str:
        if self._force_refresh_enabled() or cache_path.exists():
            return ""

        source_path = self._legacy_output_monthly_drifter_chunk_path(month_key)
        if not source_path.exists():
            return ""

        cache_path.parent.mkdir(parents=True, exist_ok=True)
        copy2(source_path, cache_path)
        return _relative(self.repo_root, source_path)

    def _stage_legacy_forcing_source(
        self,
        *,
        month_key: str,
        source_id: str,
        forcing_dir: Path,
    ) -> str:
        if self._force_refresh_enabled():
            return ""

        factor_name = forcing_factor_id_for_source(source_id)
        target_path = forcing_dir / factor_name
        if target_path.exists():
            return ""

        source_dir = self._legacy_output_monthly_forcing_dir(month_key)
        source_path = source_dir / factor_name
        if not source_path.exists():
            return ""

        forcing_dir.mkdir(parents=True, exist_ok=True)
        copy2(source_path, target_path)
        if source_id == "gfs":
            source_sidecar = self._gfs_provenance_sidecar_path(source_dir)
            if source_sidecar.exists():
                copy2(source_sidecar, self._gfs_provenance_sidecar_path(forcing_dir))
        return _relative(self.repo_root, source_path)

    def _write_local_input_inventory(
        self,
        *,
        drifter_chunk_status: list[dict[str, Any]],
        forcing_status: list[dict[str, Any]],
    ) -> Path:
        rows: list[dict[str, Any]] = []

        for chunk in drifter_chunk_status:
            rows.append(
                {
                    "workflow_mode": self.case.workflow_mode,
                    "store_scope": "phase1_monthly_drifter_store",
                    "month_key": str(chunk.get("month_key") or ""),
                    "source_id": "drifters",
                    "status": str(chunk.get("status") or ""),
                    "path": str(chunk.get("cache_path") or ""),
                    "chunk_start_utc": str(chunk.get("chunk_start_utc") or ""),
                    "chunk_end_utc": str(chunk.get("chunk_end_utc") or ""),
                    "row_count": int(chunk.get("row_count") or 0),
                    "staged_from_legacy_path": str(chunk.get("staged_from_legacy_path") or ""),
                    "source_system": "",
                    "source_tier": "",
                }
            )

        for month_status in forcing_status:
            month_key = str(month_status.get("month_key") or "")
            for source_id in month_status.get("required_forcing_sources") or []:
                source_record = dict(month_status.get(str(source_id)) or {})
                rows.append(
                    {
                        "workflow_mode": self.case.workflow_mode,
                        "store_scope": "phase1_monthly_forcing_store",
                        "month_key": month_key,
                        "source_id": str(source_id),
                        "status": str(source_record.get("status") or ""),
                        "path": str(source_record.get("path") or ""),
                        "chunk_start_utc": str(month_status.get("start_time_utc") or ""),
                        "chunk_end_utc": str(month_status.get("end_time_utc") or ""),
                        "row_count": "",
                        "staged_from_legacy_path": str(source_record.get("staged_from_legacy_path") or ""),
                        "source_system": str(source_record.get("source_system") or ""),
                        "source_tier": str(source_record.get("source_tier") or ""),
                    }
                )

        inventory_df = pd.DataFrame(rows)
        inventory_df.to_csv(self.paths["local_input_inventory"], index=False)
        return self.paths["local_input_inventory"]

    def _inspect_gfs_cache(
        self,
        output_path: Path,
        *,
        required_start: pd.Timestamp,
        required_end: pd.Timestamp,
    ) -> dict[str, Any]:
        if not output_path.exists():
            return {
                "present": False,
                "valid": False,
                "reason": "missing",
                "coverage_ok": False,
                "reader_metadata_ok": False,
                "coverage_start_utc": "",
                "coverage_end_utc": "",
            }

        try:
            self._ensure_wind_cache_reader_metadata(output_path)
            with xr.open_dataset(output_path) as raw:
                loaded = raw.load()
        except Exception as exc:
            return {
                "present": True,
                "valid": False,
                "reason": f"open_failed:{type(exc).__name__}",
                "error": str(exc),
                "coverage_ok": False,
                "reader_metadata_ok": False,
                "coverage_start_utc": "",
                "coverage_end_utc": "",
            }

        reader_metadata_ok = bool(_wind_cache_has_reader_metadata(loaded))
        if not {"x_wind", "y_wind"}.issubset(set(loaded.data_vars)):
            return {
                "present": True,
                "valid": False,
                "reason": "missing_reader_variables",
                "coverage_ok": False,
                "reader_metadata_ok": reader_metadata_ok,
                "coverage_start_utc": "",
                "coverage_end_utc": "",
            }
        if "time" not in loaded.coords:
            return {
                "present": True,
                "valid": False,
                "reason": "missing_time_coordinate",
                "coverage_ok": False,
                "reader_metadata_ok": reader_metadata_ok,
                "coverage_start_utc": "",
                "coverage_end_utc": "",
            }

        time_index = pd.to_datetime(loaded["time"].values, utc=True, errors="coerce")
        time_index = pd.DatetimeIndex(time_index).dropna()
        if time_index.empty:
            return {
                "present": True,
                "valid": False,
                "reason": "empty_time_axis",
                "coverage_ok": False,
                "reader_metadata_ok": reader_metadata_ok,
                "coverage_start_utc": "",
                "coverage_end_utc": "",
            }

        coverage_start = pd.Timestamp(time_index.min()).tz_convert("UTC")
        coverage_end = pd.Timestamp(time_index.max()).tz_convert("UTC")
        coverage_ok = bool(coverage_start <= required_start and coverage_end >= required_end)
        return {
            "present": True,
            "valid": bool(reader_metadata_ok and coverage_ok),
            "reason": "" if reader_metadata_ok and coverage_ok else ("insufficient_coverage" if reader_metadata_ok else "missing_reader_metadata"),
            "coverage_ok": coverage_ok,
            "reader_metadata_ok": reader_metadata_ok,
            "coverage_start_utc": coverage_start.isoformat(),
            "coverage_end_utc": coverage_end.isoformat(),
            "time_count": int(len(time_index)),
        }

    def _ensure_gfs_provenance_payload(
        self,
        forcing_dir: Path,
        *,
        required_start: pd.Timestamp,
        required_end: pd.Timestamp,
    ) -> dict[str, Any]:
        output_path = forcing_dir / "gfs_wind.nc"
        cache_inspection = self._inspect_gfs_cache(
            output_path,
            required_start=required_start,
            required_end=required_end,
        )
        payload = self._read_gfs_provenance_payload(forcing_dir)
        if payload:
            return payload
        inferred_payload = self._default_gfs_provenance_payload(
            source_system="ncei_thredds_archive",
            source_tier="primary",
            source_inferred=True,
            output_path=output_path,
            required_start=required_start,
            required_end=required_end,
            cache_inspection=cache_inspection,
        )
        self._write_gfs_provenance_payload(forcing_dir, inferred_payload)
        return inferred_payload

    def _reusable_gfs_cache_dirs(self, month_key: str) -> list[Path]:
        candidates = [
            self._legacy_output_monthly_forcing_dir(month_key),
            self.repo_root / "output" / "phase1_production_rerun" / "_scratch" / "forcing_months" / str(month_key),
        ]
        unique: list[Path] = []
        seen: set[str] = set()
        current_dir = (self.forcing_cache_root / str(month_key)).resolve()
        for candidate in candidates:
            try:
                resolved = candidate.resolve()
            except Exception:
                resolved = candidate
            key = str(resolved)
            if key == str(current_dir) or key in seen:
                continue
            seen.add(key)
            unique.append(candidate)
        return unique

    def _stage_reusable_gfs_cache(
        self,
        *,
        month_key: str,
        forcing_dir: Path,
        required_start: pd.Timestamp,
        required_end: pd.Timestamp,
    ) -> dict[str, Any] | None:
        output_path = forcing_dir / "gfs_wind.nc"
        if output_path.exists():
            return None

        for source_dir in self._reusable_gfs_cache_dirs(month_key):
            source_path = source_dir / "gfs_wind.nc"
            if not source_path.exists():
                continue
            source_inspection = self._inspect_gfs_cache(
                source_path,
                required_start=required_start,
                required_end=required_end,
            )
            if not bool(source_inspection.get("reader_metadata_ok", False)):
                continue

            forcing_dir.mkdir(parents=True, exist_ok=True)
            copy2(source_path, output_path)
            source_provenance = self._read_gfs_provenance_payload(source_dir)
            if source_provenance:
                staged_provenance = dict(source_provenance)
                staged_provenance["cache_path"] = _relative(self.repo_root, output_path)
                staged_provenance["staged_from_cache_path"] = _relative(self.repo_root, source_path)
                staged_provenance["staged_at_utc"] = _utcnow_isoformat()
                self._write_gfs_provenance_payload(forcing_dir, staged_provenance)
            else:
                inferred_payload = self._default_gfs_provenance_payload(
                    source_system="ncei_thredds_archive",
                    source_tier="primary",
                    source_inferred=True,
                    output_path=output_path,
                    required_start=required_start,
                    required_end=required_end,
                    cache_inspection=source_inspection,
                )
                inferred_payload["staged_from_cache_path"] = _relative(self.repo_root, source_path)
                inferred_payload["staged_at_utc"] = _utcnow_isoformat()
                self._write_gfs_provenance_payload(forcing_dir, inferred_payload)
            return {
                "status": (
                    "staged_existing_cache"
                    if bool(source_inspection.get("valid", False))
                    else "staged_partial_cache"
                ),
                "staged_from_cache_path": _relative(self.repo_root, source_path),
            }
        return None

    @staticmethod
    def _collapse_gfs_source_value(values: list[str]) -> str:
        normalized: list[str] = []
        for value in values:
            text = str(value or "").strip()
            if text and text not in normalized:
                normalized.append(text)
        if not normalized:
            return ""
        if len(normalized) == 1:
            return normalized[0]
        return "mixed"

    def _load_gfs_cache_dataset(self, output_path: Path) -> xr.Dataset:
        self._ensure_wind_cache_reader_metadata(output_path)
        with xr.open_dataset(output_path) as raw:
            return raw.load()

    def _align_gfs_dataset_to_reference_grid(
        self,
        dataset: xr.Dataset,
        *,
        reference: xr.Dataset,
    ) -> xr.Dataset:
        if "lat" not in dataset.coords or "lon" not in dataset.coords:
            return dataset
        if "lat" not in reference.coords or "lon" not in reference.coords:
            return dataset

        same_lat = np.array_equal(np.asarray(dataset["lat"].values), np.asarray(reference["lat"].values))
        same_lon = np.array_equal(np.asarray(dataset["lon"].values), np.asarray(reference["lon"].values))
        if same_lat and same_lon:
            return dataset

        aligned = dataset.interp(
            lat=reference["lat"],
            lon=reference["lon"],
            method="linear",
            kwargs={"fill_value": "extrapolate"},
        )
        for name in ("x_wind", "y_wind"):
            if name in aligned:
                aligned[name] = aligned[name].astype(np.float32)
        return aligned

    @staticmethod
    def _normalize_gfs_frame_for_merge(dataset: xr.Dataset) -> xr.Dataset:
        keep_data_vars = [
            name
            for name in ("x_wind", "y_wind")
            if name in dataset.data_vars
        ]
        normalized = dataset[keep_data_vars]
        drop_coord_names = [
            name
            for name in normalized.coords
            if name not in {"time", "lat", "lon"}
        ]
        if drop_coord_names:
            normalized = normalized.drop_vars(drop_coord_names, errors="ignore")
        return normalized

    def _write_gfs_cache_from_frames(
        self,
        output_path: Path,
        frames: list[xr.Dataset],
    ) -> None:
        if not frames:
            raise RuntimeError("Cannot write a combined GFS cache without at least one frame.")

        normalized_frames = [self._normalize_gfs_frame_for_merge(frame) for frame in frames]
        combined = xr.concat(normalized_frames, dim="time").sortby("time")
        time_index = pd.to_datetime(combined["time"].values, utc=True, errors="coerce")
        valid_mask = ~pd.isna(time_index)
        if not bool(np.all(valid_mask)):
            combined = combined.isel(time=np.flatnonzero(valid_mask))
            time_index = pd.DatetimeIndex(time_index[valid_mask])
        else:
            time_index = pd.DatetimeIndex(time_index)

        duplicated = pd.Index(time_index).duplicated(keep="first")
        if bool(np.any(duplicated)):
            combined = combined.isel(time=np.flatnonzero(~duplicated))

        combined = _apply_wind_cf_metadata(combined)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        combined.to_netcdf(output_path)

    def _acquire_gfs_cache_file(
        self,
        *,
        start_time: pd.Timestamp,
        end_time: pd.Timestamp,
        output_path: Path,
        scratch_dir: Path,
        budget_seconds: int | float | None = None,
    ) -> dict[str, Any]:
        primary_error = ""
        try:
            record = self.gfs_downloader.download(
                start_time=start_time,
                end_time=end_time,
                output_path=output_path,
                scratch_dir=scratch_dir,
                budget_seconds=budget_seconds,
            )
            return {
                **dict(record or {}),
                "source_system": "ncei_thredds_archive",
                "source_tier": "primary",
                "source_inferred": False,
                "primary_failure": "",
            }
        except Exception as exc:
            primary_error = f"{type(exc).__name__}: {exc}"
            if not self._allow_secondary_gfs_source():
                raise

        secondary_source_id = self._secondary_gfs_source_id()
        if secondary_source_id != "ucar_gdex_d084001":
            raise RuntimeError(
                f"Unsupported secondary GFS source '{secondary_source_id}' configured for the Phase 1 rerun."
            )

        record = self.gfs_downloader.download_secondary_historical(
            start_time=start_time,
            end_time=end_time,
            output_path=output_path,
            scratch_dir=scratch_dir,
            budget_seconds=budget_seconds,
        )
        return {
            **dict(record or {}),
            "source_system": "ucar_gdex_d084001",
            "source_tier": "secondary",
            "source_inferred": False,
            "primary_failure": primary_error,
        }

    def _augment_partial_gfs_cache(
        self,
        *,
        start_time: pd.Timestamp,
        end_time: pd.Timestamp,
        forcing_dir: Path,
        budget_seconds: int | float | None = None,
    ) -> dict[str, Any] | None:
        output_path = forcing_dir / "gfs_wind.nc"
        existing_cache = self._inspect_gfs_cache(
            output_path,
            required_start=start_time,
            required_end=end_time,
        )
        if not bool(existing_cache.get("present", False)):
            return None
        if not bool(existing_cache.get("reader_metadata_ok", False)):
            return None

        coverage_start_text = str(existing_cache.get("coverage_start_utc") or "")
        coverage_end_text = str(existing_cache.get("coverage_end_utc") or "")
        if not coverage_start_text or not coverage_end_text:
            return None

        coverage_start = pd.Timestamp(coverage_start_text).tz_convert("UTC")
        coverage_end = pd.Timestamp(coverage_end_text).tz_convert("UTC")

        gap_windows: list[tuple[str, pd.Timestamp, pd.Timestamp]] = []
        if coverage_start > start_time:
            prefix_end = coverage_start - self.expected_delta
            if prefix_end >= start_time:
                gap_windows.append(("prefix", start_time, prefix_end))
        if coverage_end < end_time:
            suffix_start = coverage_end + self.expected_delta
            if suffix_start <= end_time:
                gap_windows.append(("suffix", suffix_start, end_time))
        if not gap_windows:
            return None

        existing_payload = self._ensure_gfs_provenance_payload(
            forcing_dir,
            required_start=start_time,
            required_end=end_time,
        )
        existing_component = {
            "role": "staged_or_existing_partial_cache",
            "source_system": str(existing_payload.get("source_system") or "ncei_thredds_archive"),
            "source_tier": str(existing_payload.get("source_tier") or "primary"),
            "source_inferred": bool(existing_payload.get("source_inferred", False)),
            "cache_path": _relative(self.repo_root, output_path),
            "coverage_start_utc": coverage_start.isoformat(),
            "coverage_end_utc": coverage_end.isoformat(),
            "primary_failure": str(existing_payload.get("primary_failure") or ""),
        }

        existing_ds = self._load_gfs_cache_dataset(output_path)
        frames = [existing_ds]
        patch_records: list[dict[str, Any]] = []
        patch_paths: list[Path] = []

        try:
            for gap_label, gap_start, gap_end in gap_windows:
                patch_path = forcing_dir / f"gfs_wind.{gap_label}.patch.nc"
                patch_path.unlink(missing_ok=True)
                patch_record = self._acquire_gfs_cache_file(
                    start_time=gap_start,
                    end_time=gap_end,
                    output_path=patch_path,
                    scratch_dir=forcing_dir,
                    budget_seconds=budget_seconds,
                )
                patch_ds = self._load_gfs_cache_dataset(patch_path)
                patch_ds = self._align_gfs_dataset_to_reference_grid(
                    patch_ds,
                    reference=existing_ds,
                )
                frames.append(patch_ds)
                patch_records.append(
                    {
                        "role": f"{gap_label}_gap_patch",
                        "gap_start_utc": gap_start.isoformat(),
                        "gap_end_utc": gap_end.isoformat(),
                        "cache_path": _relative(self.repo_root, patch_path),
                        "source_system": str(patch_record.get("source_system") or ""),
                        "source_tier": str(patch_record.get("source_tier") or ""),
                        "source_inferred": bool(patch_record.get("source_inferred", False)),
                        "primary_failure": str(patch_record.get("primary_failure") or ""),
                    }
                )
                patch_paths.append(patch_path)

            self._write_gfs_cache_from_frames(output_path, frames)
        finally:
            for patch_path in patch_paths:
                patch_path.unlink(missing_ok=True)

        cache_inspection = self._inspect_gfs_cache(
            output_path,
            required_start=start_time,
            required_end=end_time,
        )
        components = [existing_component, *patch_records]
        source_system = self._collapse_gfs_source_value(
            [str(component.get("source_system") or "") for component in components]
        )
        source_tier = self._collapse_gfs_source_value(
            [str(component.get("source_tier") or "") for component in components]
        )
        primary_failures: list[str] = []
        for component in components:
            failure_text = str(component.get("primary_failure") or "").strip()
            if failure_text and failure_text not in primary_failures:
                primary_failures.append(failure_text)
        provenance_payload = self._default_gfs_provenance_payload(
            source_system=source_system,
            source_tier=source_tier,
            source_inferred=bool(all(bool(component.get("source_inferred", False)) for component in components)),
            output_path=output_path,
            required_start=start_time,
            required_end=end_time,
            cache_inspection=cache_inspection,
            acquisition_record={
                "status": "augmented_partial_cache",
                "base_component": existing_component,
                "patch_components": patch_records,
            },
            primary_failure=" | ".join(primary_failures),
        )
        provenance_payload["source_components"] = components
        self._write_gfs_provenance_payload(forcing_dir, provenance_payload)
        return {
            "status": "augmented_partial_cache",
            "path": _relative(self.repo_root, output_path),
            "source_system": provenance_payload["source_system"],
            "source_tier": provenance_payload["source_tier"],
            "source_inferred": bool(provenance_payload.get("source_inferred", False)),
            "provenance_sidecar": _relative(self.repo_root, self._gfs_provenance_sidecar_path(forcing_dir)),
            "primary_failure": str(provenance_payload.get("primary_failure") or ""),
            "patch_count": len(patch_records),
        }

    def _preflight_gfs_month_coverage(
        self,
        accepted_df: pd.DataFrame,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        rows: list[dict[str, Any]] = []
        failures: list[dict[str, Any]] = []
        for window in self._forcing_month_windows(accepted_df):
            month_key = str(window["month_key"])
            start_time = pd.Timestamp(window["start_time"]).tz_convert("UTC")
            end_time = pd.Timestamp(window["end_time"]).tz_convert("UTC")
            forcing_dir = self.forcing_cache_root / month_key
            forcing_dir.mkdir(parents=True, exist_ok=True)
            output_path = forcing_dir / "gfs_wind.nc"

            staged_cache = self._stage_reusable_gfs_cache(
                month_key=month_key,
                forcing_dir=forcing_dir,
                required_start=start_time,
                required_end=end_time,
            )

            cache_before = self._inspect_gfs_cache(
                output_path,
                required_start=start_time,
                required_end=end_time,
            )
            row = {
                "month_key": month_key,
                "required_start_utc": start_time.isoformat(),
                "required_end_utc": end_time.isoformat(),
                "cache_present_before": bool(cache_before.get("present", False)),
                "coverage_start_utc_before": str(cache_before.get("coverage_start_utc") or ""),
                "coverage_end_utc_before": str(cache_before.get("coverage_end_utc") or ""),
                "reader_metadata_ok_before": bool(cache_before.get("reader_metadata_ok", False)),
                "coverage_ok_before": bool(cache_before.get("coverage_ok", False)),
                "status": (
                    str(staged_cache.get("status"))
                    if staged_cache is not None
                    else ("already_present" if bool(cache_before.get("valid", False)) else "backfill_required")
                ),
                "gfs_source_system": "",
                "gfs_source_tier": "",
                "gfs_source_inferred": False,
                "gfs_primary_failure": "",
                "cache_path": _relative(self.repo_root, output_path),
                "staged_from_cache_path": str(staged_cache.get("staged_from_cache_path") or "") if staged_cache else "",
                "provenance_sidecar": _relative(
                    self.repo_root,
                    self._gfs_provenance_sidecar_path(forcing_dir),
                ),
            }

            if not bool(cache_before.get("valid", False)):
                try:
                    download_record = self._download_gfs_winds(
                        start_time,
                        end_time,
                        forcing_dir,
                    )
                except Exception as exc:
                    row["status"] = "failed"
                    row["error"] = f"{type(exc).__name__}: {exc}"
                    rows.append(row)
                    failures.append(dict(row))
                    continue
                row["status"] = str(download_record.get("status") or "downloaded")
                row["gfs_source_system"] = str(download_record.get("source_system") or "")
                row["gfs_source_tier"] = str(download_record.get("source_tier") or "")
                row["gfs_source_inferred"] = bool(download_record.get("source_inferred", False))
                row["gfs_primary_failure"] = str(download_record.get("primary_failure") or "")

            cache_after = self._inspect_gfs_cache(
                output_path,
                required_start=start_time,
                required_end=end_time,
            )
            provenance_payload = self._ensure_gfs_provenance_payload(
                forcing_dir,
                required_start=start_time,
                required_end=end_time,
            )
            row["cache_present_after"] = bool(cache_after.get("present", False))
            row["coverage_start_utc_after"] = str(cache_after.get("coverage_start_utc") or "")
            row["coverage_end_utc_after"] = str(cache_after.get("coverage_end_utc") or "")
            row["reader_metadata_ok_after"] = bool(cache_after.get("reader_metadata_ok", False))
            row["coverage_ok_after"] = bool(cache_after.get("coverage_ok", False))
            row["valid_after"] = bool(cache_after.get("valid", False))
            row["gfs_source_system"] = str(
                provenance_payload.get("source_system") or row.get("gfs_source_system") or ""
            )
            row["gfs_source_tier"] = str(
                provenance_payload.get("source_tier") or row.get("gfs_source_tier") or ""
            )
            row["gfs_source_inferred"] = bool(
                provenance_payload.get("source_inferred", row.get("gfs_source_inferred", False))
            )
            row["gfs_primary_failure"] = str(
                provenance_payload.get("primary_failure") or row.get("gfs_primary_failure") or ""
            )
            rows.append(row)
            if not bool(cache_after.get("valid", False)):
                failures.append(dict(row))

        pd.DataFrame(rows).to_csv(self.paths["gfs_preflight"], index=False)
        return rows, failures

    @staticmethod
    def _official_selection_rule_for_recipe(recipe_name: str, *, adopted_directly: bool) -> str:
        if not adopted_directly and str(recipe_name).endswith("_gfs"):
            return (
                "highest_ranked_non_gfs_recipe_adopted_for_official_b1_when_the_historical_"
                "four_recipe_winner_requires_gfs"
            )
        return "historical_four_recipe_winner_used_directly_for_official_b1"

    def _build_adoption_decision(
        self,
        recipe_ranking_df: pd.DataFrame,
        *,
        historical_winner: str,
    ) -> dict[str, Any]:
        if recipe_ranking_df.empty:
            raise RuntimeError("Cannot build adoption decision without a non-empty recipe ranking.")

        historical_winner = str(historical_winner)
        promote_historical_winner_directly = self._promote_historical_winner_directly_for_official_b1()
        if promote_historical_winner_directly or not historical_winner.endswith("_gfs"):
            return {
                "historical_four_recipe_winner": historical_winner,
                "official_b1_recipe": historical_winner,
                "official_b1_selection_rule": self._official_selection_rule_for_recipe(
                    historical_winner,
                    adopted_directly=True,
                ),
                "gfs_historical_winner_not_adopted": False,
                "non_gfs_fallback_recipe": "",
                "reason_for_non_adoption": "",
            }

        non_gfs_rows = recipe_ranking_df[
            ~recipe_ranking_df["recipe"].astype(str).str.endswith("_gfs")
        ].copy()
        if non_gfs_rows.empty:
            raise RuntimeError(
                "The four-recipe historical winner requires GFS, but no non-GFS recipe remains available "
                "for the official B1 spill-usable fallback."
            )
        fallback_recipe = str(non_gfs_rows.iloc[0]["recipe"])
        return {
            "historical_four_recipe_winner": historical_winner,
            "official_b1_recipe": fallback_recipe,
            "official_b1_selection_rule": self._official_selection_rule_for_recipe(
                historical_winner,
                adopted_directly=False,
            ),
            "gfs_historical_winner_not_adopted": True,
            "non_gfs_fallback_recipe": fallback_recipe,
            "reason_for_non_adoption": (
                "A GFS-backed recipe won the completed four-recipe historical rerun, but the canonical "
                "Mindoro March 13 -> March 14 event-scale workflow is not being promoted to an official GFS "
                "forcing stack in this change. Official B1 therefore adopts the highest-ranked non-GFS recipe "
                "from the same rerun while recording the historical GFS win transparently."
            ),
        }

    def run(self) -> dict[str, Any]:
        baseline_hash_before = _sha256(self.baseline_path)

        logger.info("Starting Phase 1 production rerun for %s to %s", self.window_start, self.window_end)
        logger.info("Official Phase 1 recipe family: %s", ", ".join(self.official_recipe_family))

        drifter_df, drifter_chunk_status = self._fetch_full_drifter_pool()
        registry_df = self._build_segment_registry(drifter_df)
        registry_df = self._augment_registry_with_distance_audit(registry_df)
        accepted_df = registry_df[registry_df["segment_status"] == "accepted"].copy()
        rejected_df = registry_df[registry_df["segment_status"] == "rejected"].copy()

        if accepted_df.empty:
            raise RuntimeError(
                "Phase 1 production rerun found zero accepted drifter windows after strict gating."
            )

        ranking_subset_df = self._select_ranking_subset(accepted_df)
        registry_df = self._annotate_ranking_subset_membership(registry_df, ranking_subset_df)
        accepted_df = registry_df[registry_df["segment_status"] == "accepted"].copy()
        rejected_df = registry_df[registry_df["segment_status"] == "rejected"].copy()

        registry_df.to_csv(self.paths["drifter_registry"], index=False)
        accepted_df.to_csv(self.paths["accepted_registry"], index=False)
        rejected_df.to_csv(self.paths["rejected_registry"], index=False)
        ranking_subset_df.to_csv(self.paths["ranking_subset_registry"], index=False)
        ranking_subset_report = self._write_ranking_subset_report(accepted_df, ranking_subset_df)

        if ranking_subset_df.empty and (
            str(self.ranking_subset_config.get("empty_subset_behavior") or "").strip().lower() == "hard_fail"
        ):
            baseline_hash_after = _sha256(self.baseline_path)
            _write_json(
                self.paths["manifest"],
                {
                    "phase": "phase1_production_rerun",
                    "workflow_mode": self.case.workflow_mode,
                    "status": "failed_empty_ranking_subset",
                    "time_window": {
                        "start_utc": self.window_start.isoformat(),
                        "end_utc": self.window_end.isoformat(),
                    },
                    "validation_box": self.validation_box,
                    "accepted_segment_count": int(len(accepted_df)),
                    "rejected_segment_count": int(len(rejected_df)),
                    "ranking_subset": {
                        "label": self.ranking_subset_label,
                        "config": self.ranking_subset_config,
                        "segment_count": 0,
                        "report": _relative(self.repo_root, ranking_subset_report),
                    },
                    "canonical_baseline_integrity": {
                        "path": _relative(self.repo_root, self.baseline_path),
                        "sha256_before": baseline_hash_before,
                        "sha256_after": baseline_hash_after,
                        "unchanged": baseline_hash_before == baseline_hash_after,
                    },
                    "artifacts": {
                        key: _relative(self.repo_root, path)
                        for key, path in self.paths.items()
                    },
                },
            )
            raise RuntimeError(
                "Phase 1 production rerun found zero accepted drifter windows in the configured ranking subset. "
                f"See {self.paths['ranking_subset_report']} and {self.paths['manifest']}."
            )

        gfs_preflight_rows: list[dict[str, Any]] = []
        if self._gfs_recipes_requested() and self._require_full_accepted_gfs_preflight():
            gfs_preflight_rows, gfs_preflight_failures = self._preflight_gfs_month_coverage(accepted_df)
            if gfs_preflight_failures:
                baseline_hash_after = _sha256(self.baseline_path)
                _write_json(
                    self.paths["manifest"],
                    {
                        "phase": "phase1_production_rerun",
                        "workflow_mode": self.case.workflow_mode,
                        "status": "failed_gfs_preflight",
                        "time_window": {
                            "start_utc": self.window_start.isoformat(),
                            "end_utc": self.window_end.isoformat(),
                        },
                        "validation_box": self.validation_box,
                        "accepted_segment_count": int(len(accepted_df)),
                        "rejected_segment_count": int(len(rejected_df)),
                        "ranking_subset": {
                            "label": self.ranking_subset_label,
                            "config": self.ranking_subset_config,
                            "segment_count": int(len(ranking_subset_df)),
                            "report": _relative(self.repo_root, ranking_subset_report),
                        },
                        "gfs_preflight": {
                            "required": True,
                            "rows": gfs_preflight_rows,
                            "failure_count": int(len(gfs_preflight_failures)),
                            "failed_month_keys": [
                                str(row.get("month_key") or "")
                                for row in gfs_preflight_failures
                            ],
                            "artifact": _relative(self.repo_root, self.paths["gfs_preflight"]),
                        },
                        "canonical_baseline_integrity": {
                            "path": _relative(self.repo_root, self.baseline_path),
                            "sha256_before": baseline_hash_before,
                            "sha256_after": baseline_hash_after,
                            "unchanged": baseline_hash_before == baseline_hash_after,
                        },
                        "artifacts": {
                            key: _relative(self.repo_root, path)
                            for key, path in self.paths.items()
                        },
                    },
                )
                failed_months = ", ".join(
                    str(row.get("month_key") or "") for row in gfs_preflight_failures
                )
                raise RuntimeError(
                    "Focused Mindoro Phase 1 GFS preflight failed. The rerun will not rank a four-recipe family "
                    f"until every accepted focused month has a valid gfs_wind.nc cache. Failed months: {failed_months}."
                )

        loading_audit_df, segment_metrics_df, forcing_status = self._evaluate_accepted_segments(
            ranking_subset_df,
            drifter_df,
        )
        loading_audit_df.to_csv(self.paths["loading_audit"], index=False)
        segment_metrics_df.to_csv(self.paths["segment_metrics"], index=False)
        self._write_local_input_inventory(
            drifter_chunk_status=drifter_chunk_status,
            forcing_status=forcing_status,
        )

        recipe_summary_df, recipe_ranking_df, winning_recipe = self._build_recipe_tables(
            segment_metrics_df,
            recipe_rank_pool=self.ranking_subset_label,
        )
        recipe_summary_df.to_csv(self.paths["recipe_summary"], index=False)
        recipe_ranking_df.to_csv(self.paths["recipe_ranking"], index=False)

        adoption_decision = self._build_adoption_decision(
            recipe_ranking_df,
            historical_winner=winning_recipe,
        )

        candidate_payload = self._build_candidate_baseline_payload(
            winning_recipe=winning_recipe,
            official_b1_recipe=str(adoption_decision["official_b1_recipe"]),
            adoption_decision=adoption_decision,
            accepted_df=accepted_df,
            rejected_df=rejected_df,
            ranking_subset_df=ranking_subset_df,
        )
        _write_yaml(self.paths["baseline_candidate"], candidate_payload)

        _write_json(self.paths["adoption_decision_json"], adoption_decision)
        self.paths["adoption_decision_md"].write_text(
            "\n".join(
                [
                    "# Phase 1 Official Adoption Decision",
                    "",
                    f"- Historical four-recipe winner: `{adoption_decision['historical_four_recipe_winner']}`",
                    f"- Official B1 recipe: `{adoption_decision['official_b1_recipe']}`",
                    f"- Official B1 selection rule: `{adoption_decision['official_b1_selection_rule']}`",
                    f"- GFS historical winner not adopted: `{adoption_decision['gfs_historical_winner_not_adopted']}`",
                    f"- Non-GFS fallback recipe: `{adoption_decision['non_gfs_fallback_recipe'] or 'not_needed'}`",
                    f"- Reason for non-adoption: `{adoption_decision['reason_for_non_adoption'] or 'not_applicable'}`",
                    "",
                    "This artifact is machine-readable in the adjacent JSON file and exists so the focused historical "
                    "winner and the official B1 adoption outcome are recorded explicitly.",
                ]
            )
            + "\n",
            encoding="utf-8",
        )

        if self._official_baseline_update_enabled():
            official_payload = self._build_official_baseline_payload(
                recipe_ranking_df=recipe_ranking_df,
                adoption_decision=adoption_decision,
                accepted_df=accepted_df,
                rejected_df=rejected_df,
                ranking_subset_df=ranking_subset_df,
            )
            _write_yaml(self.baseline_path, official_payload)
        baseline_hash_after = _sha256(self.baseline_path)
        if not self._official_baseline_update_enabled() and baseline_hash_before != baseline_hash_after:
            raise RuntimeError(
                "config/phase1_baseline_selection.yaml changed during the production rerun. "
                "This workflow must only stage a candidate baseline artifact."
            )

        manifest_payload = self._build_manifest(
            drifter_chunk_status=drifter_chunk_status,
            forcing_status=forcing_status,
            gfs_preflight_rows=gfs_preflight_rows,
            accepted_df=accepted_df,
            rejected_df=rejected_df,
            ranking_subset_df=ranking_subset_df,
            loading_audit_df=loading_audit_df,
            recipe_ranking_df=recipe_ranking_df,
            winning_recipe=winning_recipe,
            adoption_decision=adoption_decision,
            candidate_payload=candidate_payload,
            baseline_hash_before=baseline_hash_before,
            baseline_hash_after=baseline_hash_after,
        )
        _write_json(self.paths["manifest"], manifest_payload)

        gfs_recipe_rows = recipe_summary_df[recipe_summary_df["recipe"].str.endswith("_gfs")]
        gfs_capable_recipes_ran = bool(
            not gfs_recipe_rows.empty and (gfs_recipe_rows["valid_segment_count"] > 0).all()
        )

        return {
            "output_dir": str(self.output_root),
            "accepted_segment_count": int(len(accepted_df)),
            "rejected_segment_count": int(len(rejected_df)),
            "ranking_subset_segment_count": int(len(ranking_subset_df)),
            "winning_recipe": winning_recipe,
            "historical_four_recipe_winner": str(adoption_decision["historical_four_recipe_winner"]),
            "official_b1_recipe": str(adoption_decision["official_b1_recipe"]),
            "gfs_historical_winner_not_adopted": bool(adoption_decision["gfs_historical_winner_not_adopted"]),
            "gfs_capable_recipes_ran": gfs_capable_recipes_ran,
            "forcing_outage_policy": self.forcing_outage_policy,
            "forcing_source_budget_seconds": resolve_forcing_source_budget_seconds(),
            "degraded_continue_used": self.degraded_continue_used,
            "upstream_outage_detected": self.upstream_outage_detected,
            "missing_forcing_factors": sorted(self.missing_forcing_factors),
            "skipped_recipe_ids": sorted(self.skipped_recipe_ids),
            "rerun_required": bool(candidate_payload.get("rerun_required", False)),
            "candidate_baseline_path": str(self.paths["baseline_candidate"]),
            "gfs_preflight_csv": str(self.paths["gfs_preflight"]) if self.paths["gfs_preflight"].exists() else "",
            "adoption_decision_json": str(self.paths["adoption_decision_json"]),
            "adoption_decision_md": str(self.paths["adoption_decision_md"]),
            "drifter_registry_csv": str(self.paths["drifter_registry"]),
            "ranking_subset_registry_csv": str(self.paths["ranking_subset_registry"]),
            "ranking_subset_report_md": str(self.paths["ranking_subset_report"]),
            "loading_audit_csv": str(self.paths["loading_audit"]),
            "segment_metrics_csv": str(self.paths["segment_metrics"]),
            "recipe_summary_csv": str(self.paths["recipe_summary"]),
            "recipe_ranking_csv": str(self.paths["recipe_ranking"]),
            "manifest_json": str(self.paths["manifest"]),
            "local_input_inventory_csv": str(self.paths["local_input_inventory"]),
        }

    def _month_starts(self) -> list[pd.Timestamp]:
        start = self.window_start.floor("D").tz_convert("UTC")
        end = self.window_end.floor("D").tz_convert("UTC")
        return [ts.tz_localize("UTC") if ts.tzinfo is None else ts for ts in pd.date_range(start=start, end=end, freq="MS")]

    def _normalize_drifter_frame(self, frame: pd.DataFrame) -> pd.DataFrame:
        rename_map = {
            "time": "time",
            "time (UTC)": "time",
            "latitude": "lat",
            "latitude (degrees_north)": "lat",
            "longitude": "lon",
            "longitude (degrees_east)": "lon",
            "drogue_lost_date": "drogue_lost_date",
            "drogue_lost_date (UTC)": "drogue_lost_date",
            "deploy_date": "deploy_date",
            "deploy_date (UTC)": "deploy_date",
            "start_date": "start_date",
            "start_date (UTC)": "start_date",
            "end_date": "end_date",
            "end_date (UTC)": "end_date",
        }
        normalized = frame.rename(columns=rename_map).copy()

        required_columns = []
        for field in self.required_drifter_fields:
            required_columns.append(
                {
                    "time": "time",
                    "latitude": "lat",
                    "longitude": "lon",
                }.get(field, field)
            )

        missing_columns = [column for column in required_columns if column not in normalized.columns]
        if missing_columns:
            raise RuntimeError(
                "NOAA GDP drifter query did not return the required strict-gating fields: "
                f"{', '.join(missing_columns)}"
            )

        normalized["time"] = pd.to_datetime(normalized["time"], utc=True, errors="coerce")
        normalized["drogue_lost_date"] = pd.to_datetime(
            normalized.get("drogue_lost_date"),
            utc=True,
            errors="coerce",
        )
        normalized["deploy_date"] = pd.to_datetime(
            normalized.get("deploy_date"),
            utc=True,
            errors="coerce",
        )
        normalized["lat"] = pd.to_numeric(normalized["lat"], errors="coerce")
        normalized["lon"] = pd.to_numeric(normalized["lon"], errors="coerce")
        normalized["ID"] = normalized["ID"].astype(str)

        normalized = normalized.dropna(subset=["time", "lat", "lon", "ID"]).sort_values(["ID", "time"]).reset_index(drop=True)
        return normalized

    def _fetch_monthly_drifter_chunk(self, month_start: pd.Timestamp) -> tuple[pd.DataFrame, dict[str, Any]]:
        month_key = month_start.strftime("%Y%m")
        cache_path = self.drifter_cache_root / f"{month_key}.csv"
        next_month = month_start + pd.offsets.MonthBegin(1)
        month_end = min(self.window_end, next_month - pd.Timedelta(seconds=1))
        staged_from_legacy_path = self._stage_legacy_drifter_chunk(
            month_key=month_key,
            cache_path=cache_path,
        )

        if cache_path.exists() and not self._force_refresh_enabled():
            cached = pd.read_csv(cache_path)
            normalized = self._normalize_drifter_frame(cached)
            return normalized, {
                "month_key": month_key,
                "chunk_start_utc": month_start.isoformat(),
                "chunk_end_utc": month_end.isoformat(),
                "status": "staged_legacy_cache" if staged_from_legacy_path else "cached",
                "row_count": int(len(normalized)),
                "cache_path": _relative(self.repo_root, cache_path),
                "staged_from_legacy_path": staged_from_legacy_path,
            }

        client = ERDDAP(server=self.drifter_server, protocol="tabledap")
        client.dataset_id = self.drifter_dataset_id
        client.constraints = {
            "time>=": month_start.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "time<=": month_end.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "latitude>=": self.drifter_query_box[2],
            "latitude<=": self.drifter_query_box[3],
            "longitude>=": self.drifter_query_box[0],
            "longitude<=": self.drifter_query_box[1],
        }
        client.variables = list(self.required_drifter_fields)

        try:
            frame = client.to_pandas()
        except Exception as exc:  # pragma: no cover - network behavior
            message = str(exc)
            if self.NOAA_EMPTY_RESULT_MARKER in message or "no matching results" in message.lower():
                empty = pd.DataFrame(columns=self.required_drifter_fields)
                empty.to_csv(cache_path, index=False)
                normalized_empty = self._normalize_drifter_frame(empty)
                return normalized_empty, {
                    "month_key": month_key,
                    "chunk_start_utc": month_start.isoformat(),
                    "chunk_end_utc": month_end.isoformat(),
                    "status": "no_data",
                    "row_count": 0,
                    "cache_path": _relative(self.repo_root, cache_path),
                    "staged_from_legacy_path": "",
                }
            raise RuntimeError(f"NOAA GDP monthly chunk fetch failed for {month_key}: {exc}") from exc

        normalized = self._normalize_drifter_frame(frame)
        normalized.to_csv(cache_path, index=False)
        return normalized, {
            "month_key": month_key,
            "chunk_start_utc": month_start.isoformat(),
            "chunk_end_utc": month_end.isoformat(),
            "status": "downloaded",
            "row_count": int(len(normalized)),
            "cache_path": _relative(self.repo_root, cache_path),
            "staged_from_legacy_path": "",
        }

    def _fetch_full_drifter_pool(self) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
        chunk_frames: list[pd.DataFrame] = []
        chunk_statuses: list[dict[str, Any]] = []
        for month_start in self._month_starts():
            chunk_df, chunk_status = self._fetch_monthly_drifter_chunk(month_start)
            chunk_statuses.append(chunk_status)
            if not chunk_df.empty:
                chunk_frames.append(chunk_df)

        if not chunk_frames:
            raise RuntimeError(
                "NOAA GDP monthly chunk fetch completed but returned no drifter observations in the regional acquisition box."
            )

        full_df = pd.concat(chunk_frames, ignore_index=True)
        full_df = self._normalize_drifter_frame(full_df)
        return full_df, chunk_statuses

    def _point_in_validation_box(self, lon: float, lat: float) -> bool:
        return (
            self.validation_box[0] <= float(lon) <= self.validation_box[1]
            and self.validation_box[2] <= float(lat) <= self.validation_box[3]
        )

    def _window_within_validation_box(self, window_df: pd.DataFrame) -> bool:
        return bool(
            window_df.apply(lambda row: self._point_in_validation_box(row["lon"], row["lat"]), axis=1).all()
        )

    def _build_segment_registry(self, drifter_df: pd.DataFrame) -> pd.DataFrame:
        expected_points = int(self.segment_horizon_hours / self.segment_timestep_hours) + 1
        registry_rows: list[dict[str, Any]] = []

        for drifter_id, group in drifter_df.groupby("ID", sort=True):
            track = group.sort_values("time").reset_index(drop=True)
            next_available_start: pd.Timestamp | None = None

            for idx, start_row in track.iterrows():
                start_time = pd.Timestamp(start_row["time"])
                end_time = start_time + self.segment_horizon
                segment_id = (
                    f"{drifter_id}_{start_time.strftime('%Y%m%dT%H%M%SZ')}_"
                    f"{end_time.strftime('%Y%m%dT%H%M%SZ')}"
                )
                window = track[(track["time"] >= start_time) & (track["time"] <= end_time)].copy()
                last_time = pd.Timestamp(window["time"].iloc[-1]) if not window.empty else pd.NaT
                actual_points = int(len(window))
                start_in_box = self._point_in_validation_box(start_row["lon"], start_row["lat"])
                window_in_box = bool(not window.empty and self._window_within_validation_box(window))
                drogue_missing = bool(
                    window.empty
                    or window["drogue_lost_date"].isna().any()
                    or pd.isna(start_row.get("drogue_lost_date"))
                )
                drogue_attached = bool(
                    not drogue_missing
                    and (window["drogue_lost_date"] > end_time).all()
                )
                overlap_conflict = bool(next_available_start is not None and start_time < next_available_start)

                rejection_reason = ""
                segment_status = "accepted"
                if drogue_missing:
                    rejection_reason = "missing_drogue_status"
                elif pd.isna(last_time) or last_time < end_time:
                    rejection_reason = "insufficient_duration"
                elif actual_points != expected_points or not window["time"].diff().dropna().eq(self.expected_delta).all():
                    rejection_reason = "coverage_gap"
                elif not start_in_box or not window_in_box:
                    rejection_reason = "outside_phase1_validation_box"
                elif not drogue_attached:
                    rejection_reason = "drogue_lost_within_window"
                elif overlap_conflict:
                    rejection_reason = "overlaps_prior_accepted_window"

                if rejection_reason:
                    segment_status = "rejected"
                else:
                    next_available_start = end_time + self.expected_delta

                registry_rows.append(
                    {
                        "segment_id": segment_id,
                        "segment_status": segment_status,
                        "rejection_reason": rejection_reason,
                        "drifter_id": str(drifter_id),
                        "start_time_utc": start_time.isoformat(),
                        "end_time_utc": end_time.isoformat(),
                        "month_key": start_time.strftime("%Y%m"),
                        "expected_observation_count": expected_points,
                        "actual_observation_count": actual_points,
                        "coverage_complete": bool(actual_points == expected_points and not rejection_reason == "insufficient_duration"),
                        "coverage_gap_detected": bool(rejection_reason == "coverage_gap"),
                        "start_point_in_validation_box": start_in_box,
                        "window_points_within_validation_box": window_in_box,
                        "phase1_validation_box_status": "inside" if start_in_box and window_in_box else "outside",
                        "non_overlap_status": "accepted" if not overlap_conflict else "rejected_overlap",
                        "drogue_status_complete": not drogue_missing,
                        "drogue_attached_through_window": drogue_attached,
                        "drogue_lost_date_start_row": ""
                        if pd.isna(start_row.get("drogue_lost_date"))
                        else pd.Timestamp(start_row["drogue_lost_date"]).isoformat(),
                        "deploy_date_start_row": ""
                        if pd.isna(start_row.get("deploy_date"))
                        else pd.Timestamp(start_row["deploy_date"]).isoformat(),
                        "start_lat": float(start_row["lat"]),
                        "start_lon": float(start_row["lon"]),
                        "end_lat": float(window.iloc[-1]["lat"]) if not window.empty else np.nan,
                        "end_lon": float(window.iloc[-1]["lon"]) if not window.empty else np.nan,
                        "ve_start": float(start_row["ve"]) if pd.notna(start_row.get("ve")) else np.nan,
                        "vn_start": float(start_row["vn"]) if pd.notna(start_row.get("vn")) else np.nan,
                        "err_lat_start": float(start_row["err_lat"]) if pd.notna(start_row.get("err_lat")) else np.nan,
                        "err_lon_start": float(start_row["err_lon"]) if pd.notna(start_row.get("err_lon")) else np.nan,
                        "DrogueType": str(start_row.get("DrogueType") or ""),
                        "DrogueLength": str(start_row.get("DrogueLength") or ""),
                        "DrogueDetectSensor": str(start_row.get("DrogueDetectSensor") or ""),
                    }
                )

        return pd.DataFrame(registry_rows).sort_values(["drifter_id", "start_time_utc"]).reset_index(drop=True)

    def _forcing_month_windows(self, accepted_df: pd.DataFrame) -> list[dict[str, Any]]:
        windows = []
        for month_key, group in accepted_df.groupby("month_key", sort=True):
            start_time = pd.to_datetime(group["start_time_utc"], utc=True).min()
            end_time = pd.to_datetime(group["end_time_utc"], utc=True).max()
            windows.append(
                {
                    "month_key": str(month_key),
                    "start_time": start_time,
                    "end_time": end_time,
                }
            )
        return windows

    def _cmems_dataset_ids(self, start_time: pd.Timestamp) -> tuple[str, str]:
        year = start_time.year
        currents = (
            "cmems_mod_glo_phy_my_0.083deg_P1D-m"
            if year < 2022
            else "cmems_mod_glo_phy-cur_anfc_0.083deg_P1D-m"
        )
        wave = (
            "cmems_mod_glo_wav_my_0.2deg_PT3H-i"
            if year < 2022
            else "cmems_mod_glo_wav_anfc_0.083deg_PT3H-i"
        )
        return currents, wave

    def _continue_degraded_enabled(self) -> bool:
        return self.forcing_outage_policy == FORCING_OUTAGE_POLICY_CONTINUE_DEGRADED

    def _recipe_required_factor_ids(self, recipe_name: str) -> set[str]:
        recipes = (self.recipes_config.get("recipes") or {})
        recipe = recipes.get(recipe_name) or {}
        required: set[str] = set()
        if recipe.get("currents_file"):
            required.add(str(recipe["currents_file"]))
        if recipe.get("wind_file"):
            required.add(str(recipe["wind_file"]))
        wave_file = str(recipe.get("wave_file") or "").strip()
        if wave_file:
            required.add(wave_file)
        return required

    def _required_forcing_source_ids(self) -> list[str]:
        recipes = (self.recipes_config.get("recipes") or {})
        required_source_ids: set[str] = set()
        for recipe_name in self.official_recipe_family:
            recipe = recipes.get(recipe_name) or {}
            currents_file = str(recipe.get("currents_file") or "").strip()
            wind_file = str(recipe.get("wind_file") or "").strip()
            wave_file = str(recipe.get("wave_file") or "").strip()
            if currents_file:
                required_source_ids.add(
                    source_id_for_recipe_component(forcing_kind="current", filename=currents_file)
                )
            if wind_file:
                required_source_ids.add(
                    source_id_for_recipe_component(forcing_kind="wind", filename=wind_file)
                )
            if wave_file:
                required_source_ids.add(
                    source_id_for_recipe_component(forcing_kind="wave", filename=wave_file)
                )
        ordered_source_ids = [
            source_id
            for source_id in ("hycom", "cmems", "cmems_wave", "era5", "gfs", "ncep")
            if source_id in required_source_ids
        ]
        return ordered_source_ids

    def _download_forcing_source_with_policy(
        self,
        *,
        source_id: str,
        start_time: pd.Timestamp,
        end_time: pd.Timestamp,
        forcing_dir: Path,
    ) -> dict[str, Any]:
        source_id = str(source_id)
        factor_id = forcing_factor_id_for_source(source_id)
        record = run_budgeted_forcing_provider_call(
            callable_path="src.services.phase1_production_rerun._budgeted_phase1_forcing_download",
            kwargs={
                "repo_root": str(self.repo_root),
                "config_path": str(self.config_path),
                "source_id": source_id,
                "start_time_utc": start_time.isoformat(),
                "end_time_utc": end_time.isoformat(),
                "forcing_dir": str(forcing_dir),
            },
            source_id=source_id,
            forcing_factor=factor_id,
            failure_path=str(forcing_dir / factor_id),
        )
        outage = bool(record.get("upstream_outage_detected", False))
        self.upstream_outage_detected = bool(self.upstream_outage_detected or outage)
        if record.get("status") in {"failed", "failed_remote_outage"}:
            if outage and self._continue_degraded_enabled():
                self.degraded_continue_used = True
                self.missing_forcing_factors.add(factor_id)
                degraded = dict(record)
                degraded["status"] = "skipped_outage_continue_degraded"
                return degraded
            raise RuntimeError(str(record.get("error") or f"{source_id} forcing acquisition failed."))

        record.setdefault("status", "downloaded")
        record["source_id"] = source_id
        record["forcing_factor"] = factor_id
        record["upstream_outage_detected"] = bool(record.get("upstream_outage_detected", False))
        return record

    def _download_cmems_currents(
        self,
        start_time: pd.Timestamp,
        end_time: pd.Timestamp,
        forcing_dir: Path,
    ) -> dict[str, Any]:
        if copernicusmarine is None:
            raise RuntimeError("copernicusmarine is required for CMEMS forcing downloads.")
        username = os.environ.get("CMEMS_USERNAME")
        password = os.environ.get("CMEMS_PASSWORD")
        if not username or not password:
            raise RuntimeError("CMEMS_USERNAME and CMEMS_PASSWORD are required for the Phase 1 production rerun.")

        output_path = forcing_dir / "cmems_curr.nc"
        if output_path.exists() and not self._force_refresh_enabled():
            return {"status": "cached", "path": _relative(self.repo_root, output_path)}

        dataset_id, _ = self._cmems_dataset_ids(start_time)
        copernicusmarine.subset(
            dataset_id=dataset_id,
            minimum_longitude=self.forcing_box[0],
            maximum_longitude=self.forcing_box[1],
            minimum_latitude=self.forcing_box[2],
            maximum_latitude=self.forcing_box[3],
            maximum_depth=CMEMS_SURFACE_CURRENT_MAX_DEPTH_M,
            start_datetime=start_time.tz_localize(None).strftime("%Y-%m-%dT%H:%M:%S"),
            end_datetime=end_time.tz_localize(None).strftime("%Y-%m-%dT%H:%M:%S"),
            variables=["uo", "vo"],
            output_filename=output_path.name,
            output_directory=str(forcing_dir),
            overwrite=True,
            username=username,
            password=password,
        )
        if not output_path.exists():
            raise RuntimeError("CMEMS current download did not create cmems_curr.nc.")
        return {
            "status": "downloaded",
            "dataset_id": dataset_id,
            "path": _relative(self.repo_root, output_path),
        }

    def _download_cmems_wave(
        self,
        start_time: pd.Timestamp,
        end_time: pd.Timestamp,
        forcing_dir: Path,
    ) -> dict[str, Any]:
        if copernicusmarine is None:
            raise RuntimeError("copernicusmarine is required for CMEMS wave downloads.")
        username = os.environ.get("CMEMS_USERNAME")
        password = os.environ.get("CMEMS_PASSWORD")
        if not username or not password:
            raise RuntimeError("CMEMS_USERNAME and CMEMS_PASSWORD are required for the Phase 1 production rerun.")

        output_path = forcing_dir / "cmems_wave.nc"
        if output_path.exists() and not self._force_refresh_enabled():
            return {"status": "cached", "path": _relative(self.repo_root, output_path)}

        _, dataset_id = self._cmems_dataset_ids(start_time)
        copernicusmarine.subset(
            dataset_id=dataset_id,
            minimum_longitude=self.forcing_box[0],
            maximum_longitude=self.forcing_box[1],
            minimum_latitude=self.forcing_box[2],
            maximum_latitude=self.forcing_box[3],
            start_datetime=start_time.tz_localize(None).strftime("%Y-%m-%dT%H:%M:%S"),
            end_datetime=end_time.tz_localize(None).strftime("%Y-%m-%dT%H:%M:%S"),
            variables=["VHM0", "VSDX", "VSDY"],
            output_filename=output_path.name,
            output_directory=str(forcing_dir),
            overwrite=True,
            username=username,
            password=password,
        )
        if not output_path.exists():
            raise RuntimeError("CMEMS wave download did not create cmems_wave.nc.")
        return {
            "status": "downloaded",
            "dataset_id": dataset_id,
            "path": _relative(self.repo_root, output_path),
        }

    def _ensure_wind_cache_reader_metadata(self, output_path: Path) -> None:
        self.gfs_downloader.ensure_reader_metadata(output_path)

    def _download_era5_winds(
        self,
        start_time: pd.Timestamp,
        end_time: pd.Timestamp,
        forcing_dir: Path,
    ) -> dict[str, Any]:
        if cdsapi is None:
            raise RuntimeError("cdsapi is required for ERA5 wind downloads.")
        url = os.environ.get("CDS_URL")
        key = os.environ.get("CDS_KEY")
        if not url or not key:
            raise RuntimeError("CDS_URL and CDS_KEY are required for the Phase 1 production rerun.")

        output_path = forcing_dir / "era5_wind.nc"
        if output_path.exists() and not self._force_refresh_enabled():
            self._ensure_wind_cache_reader_metadata(output_path)
            return {"status": "cached", "path": _relative(self.repo_root, output_path)}

        temp_path = forcing_dir / "era5_wind_raw.nc"
        client = cdsapi.Client(url=url, key=key)
        client.retrieve(
            "reanalysis-era5-single-levels",
            {
                "product_type": "reanalysis",
                "variable": ["10m_u_component_of_wind", "10m_v_component_of_wind"],
                "date": f"{start_time.strftime('%Y-%m-%d')}/{end_time.strftime('%Y-%m-%d')}",
                "time": [f"{hour:02d}:00" for hour in range(24)],
                "area": [self.forcing_box[3], self.forcing_box[0], self.forcing_box[2], self.forcing_box[1]],
                "format": "netcdf",
            },
            str(temp_path),
        )
        with xr.open_dataset(temp_path) as raw:
            ds = raw.load()
        rename_map = {}
        if "valid_time" in ds:
            rename_map["valid_time"] = "time"
        if "u10" in ds:
            rename_map["u10"] = "x_wind"
        if "v10" in ds:
            rename_map["v10"] = "y_wind"
        if rename_map:
            ds = ds.rename(rename_map)
        ds = _apply_wind_cf_metadata(ds)
        ds.to_netcdf(output_path)
        temp_path.unlink(missing_ok=True)
        return {"status": "downloaded", "path": _relative(self.repo_root, output_path)}

    def _hycom_candidate_urls(self, start_time: pd.Timestamp) -> list[str]:
        year = int(start_time.year)
        if year < 2014:
            return ["https://tds.hycom.org/thredds/dodsC/GLBu0.08/expt_19.1"]
        return [
            "https://tds.hycom.org/thredds/dodsC/GLBv0.08/expt_56.3",
            "https://tds.hycom.org/thredds/dodsC/GLBv0.08/expt_57.2",
            "https://tds.hycom.org/thredds/dodsC/GLBv0.08/expt_92.8",
            "https://tds.hycom.org/thredds/dodsC/GLBv0.08/expt_57.7",
            "https://tds.hycom.org/thredds/dodsC/GLBv0.08/expt_92.9",
            "https://tds.hycom.org/thredds/dodsC/GLBv0.08/expt_93.0",
            "https://tds.hycom.org/thredds/dodsC/GLBy0.08/expt_93.0",
        ]

    def _download_hycom_currents(
        self,
        start_time: pd.Timestamp,
        end_time: pd.Timestamp,
        forcing_dir: Path,
    ) -> dict[str, Any]:
        output_path = forcing_dir / "hycom_curr.nc"
        if output_path.exists() and not self._force_refresh_enabled():
            return {"status": "cached", "path": _relative(self.repo_root, output_path)}

        for base_url in self._hycom_candidate_urls(start_time):
            try:
                with xr.open_dataset(base_url, drop_variables=["tau"]) as remote:
                    subset = (
                        remote[["water_u", "water_v"]]
                        .sel(
                            time=slice(start_time.tz_localize(None), end_time.tz_localize(None)),
                            lat=slice(self.forcing_box[2], self.forcing_box[3]),
                            lon=slice(self.forcing_box[0], self.forcing_box[1]),
                            depth=0,
                        )
                        .load()
                    )
                if int(subset.sizes.get("time", 0)) == 0:
                    continue
                subset.to_netcdf(output_path)
                return {
                    "status": "downloaded",
                    "source_url": base_url,
                    "path": _relative(self.repo_root, output_path),
                }
            except Exception as exc:  # pragma: no cover - remote variability
                logger.warning("HYCOM source failed for %s: %s", base_url, exc)

        raise RuntimeError("All HYCOM candidate sources failed for the Phase 1 production rerun.")

    def _gfs_dataset_base(self, timestamp: pd.Timestamp) -> str:
        return self.gfs_downloader.gfs_dataset_base(timestamp)

    def _gfs_catalog_url_for_day(self, timestamp: pd.Timestamp) -> str:
        return self.gfs_downloader.gfs_catalog_url_for_day(timestamp)

    def _parse_gfs_analysis_timestamp(self, dataset_name: str) -> pd.Timestamp | None:
        return self.gfs_downloader.parse_gfs_analysis_timestamp(dataset_name)

    def _parse_gfs_catalog(self, xml_text: str) -> dict[pd.Timestamp, str]:
        return self.gfs_downloader.parse_gfs_catalog(xml_text)

    def _discover_gfs_analysis_urls(
        self,
        start_time: pd.Timestamp,
        end_time: pd.Timestamp,
    ) -> list[tuple[pd.Timestamp, str]]:
        return self.gfs_downloader.discover_gfs_analysis_urls(start_time, end_time)

    @staticmethod
    def _coord_slice(coord: xr.DataArray, lower: float, upper: float) -> slice:
        return GFSWindDownloader._coord_slice(coord, lower, upper)

    def _normalize_gfs_subset(
        self,
        ds: xr.Dataset,
        *,
        analysis_time: pd.Timestamp,
    ) -> xr.Dataset:
        return self.gfs_downloader.normalize_gfs_subset(ds, analysis_time=analysis_time)

    def _download_gfs_subset_via_opendap(
        self,
        *,
        url: str,
        timestamp: pd.Timestamp,
    ) -> xr.Dataset:
        return self.gfs_downloader.download_gfs_subset_via_opendap(url=url, timestamp=timestamp)

    def _download_gfs_subset_via_http_cfgrib(
        self,
        *,
        url: str,
        timestamp: pd.Timestamp,
        forcing_dir: Path,
    ) -> xr.Dataset:
        return self.gfs_downloader.download_gfs_subset_via_http_cfgrib(
            url=url,
            timestamp=timestamp,
            scratch_dir=forcing_dir,
        )

    def _download_gfs_winds(
        self,
        start_time: pd.Timestamp,
        end_time: pd.Timestamp,
        forcing_dir: Path,
        *,
        budget_seconds: int | float | None = None,
    ) -> dict[str, Any]:
        output_path = forcing_dir / "gfs_wind.nc"
        sidecar_path = self._gfs_provenance_sidecar_path(forcing_dir)
        if self._force_refresh_enabled():
            output_path.unlink(missing_ok=True)
            sidecar_path.unlink(missing_ok=True)

        existing_cache = self._inspect_gfs_cache(
            output_path,
            required_start=start_time,
            required_end=end_time,
        )
        if bool(existing_cache.get("valid", False)):
            provenance_payload = self._ensure_gfs_provenance_payload(
                forcing_dir,
                required_start=start_time,
                required_end=end_time,
            )
            return {
                "status": "cached",
                "path": _relative(self.repo_root, output_path),
                "source_system": str(provenance_payload.get("source_system") or "ncei_thredds_archive"),
                "source_tier": str(provenance_payload.get("source_tier") or "primary"),
                "source_inferred": bool(provenance_payload.get("source_inferred", False)),
                "provenance_sidecar": _relative(self.repo_root, sidecar_path),
                "coverage_start_utc": str(existing_cache.get("coverage_start_utc") or ""),
                "coverage_end_utc": str(existing_cache.get("coverage_end_utc") or ""),
                "primary_failure": str(provenance_payload.get("primary_failure") or ""),
            }
        if bool(existing_cache.get("present", False)) and bool(existing_cache.get("reader_metadata_ok", False)):
            augmentation_record = self._augment_partial_gfs_cache(
                start_time=start_time,
                end_time=end_time,
                forcing_dir=forcing_dir,
                budget_seconds=budget_seconds,
            )
            cache_inspection = self._inspect_gfs_cache(
                output_path,
                required_start=start_time,
                required_end=end_time,
            )
            if augmentation_record and bool(cache_inspection.get("valid", False)):
                return augmentation_record
            output_path.unlink(missing_ok=True)
            sidecar_path.unlink(missing_ok=True)
        elif output_path.exists():
            output_path.unlink(missing_ok=True)
            sidecar_path.unlink(missing_ok=True)

        record = self._acquire_gfs_cache_file(
            start_time=start_time,
            end_time=end_time,
            output_path=output_path,
            scratch_dir=forcing_dir,
            budget_seconds=budget_seconds,
        )
        cache_inspection = self._inspect_gfs_cache(
            output_path,
            required_start=start_time,
            required_end=end_time,
        )
        provenance_payload = self._default_gfs_provenance_payload(
            source_system=str(record.get("source_system") or "ncei_thredds_archive"),
            source_tier=str(record.get("source_tier") or "primary"),
            source_inferred=bool(record.get("source_inferred", False)),
            output_path=output_path,
            required_start=start_time,
            required_end=end_time,
            cache_inspection=cache_inspection,
            acquisition_record=record,
            primary_failure=str(record.get("primary_failure") or ""),
        )
        self._write_gfs_provenance_payload(forcing_dir, provenance_payload)
        return {
            **dict(record or {}),
            "provenance_sidecar": _relative(self.repo_root, sidecar_path),
        }

    def _prepare_forcing_cache(
        self,
        month_key: str,
        start_time: pd.Timestamp,
        end_time: pd.Timestamp,
    ) -> tuple[Path, dict[str, Any]]:
        forcing_dir = self.forcing_cache_root / month_key
        forcing_dir.mkdir(parents=True, exist_ok=True)

        status = {
            "month_key": month_key,
            "start_time_utc": start_time.isoformat(),
            "end_time_utc": end_time.isoformat(),
            "forcing_outage_policy": self.forcing_outage_policy,
            "required_forcing_sources": list(self.required_forcing_source_ids),
        }
        ready_statuses = {"downloaded", "cached", "staged_legacy_cache", "augmented_partial_cache"}
        active_source_ids = tuple(self.required_forcing_source_ids)
        staged_legacy_paths: dict[str, str] = {}
        for source_id in active_source_ids:
            staged_legacy_paths[source_id] = self._stage_legacy_forcing_source(
                month_key=month_key,
                source_id=source_id,
                forcing_dir=forcing_dir,
            )
            status[source_id] = self._download_forcing_source_with_policy(
                source_id=source_id,
                start_time=start_time,
                end_time=end_time,
                forcing_dir=forcing_dir,
            )
            if staged_legacy_paths[source_id]:
                status[source_id]["staged_from_legacy_path"] = staged_legacy_paths[source_id]
                if status[source_id].get("status") == "cached":
                    status[source_id]["status"] = "staged_legacy_cache"
            if source_id == "gfs" and status[source_id].get("status") in ready_statuses:
                status["gfs_source_system"] = str(status[source_id].get("source_system") or "")
                status["gfs_source_tier"] = str(status[source_id].get("source_tier") or "")
                status["gfs_source_inferred"] = bool(status[source_id].get("source_inferred", False))
                status["gfs_provenance_sidecar"] = str(
                    status[source_id].get("provenance_sidecar") or ""
                )
                status["gfs_primary_failure"] = str(
                    status[source_id].get("primary_failure") or ""
                )
        status["available_forcing_factors"] = sorted(
            item["forcing_factor"]
            for key, item in status.items()
            if key in active_source_ids
            and item.get("status") in ready_statuses
        )
        status["missing_forcing_factors"] = sorted(
            item["forcing_factor"]
            for key, item in status.items()
            if key in active_source_ids
            and item.get("status") == "skipped_outage_continue_degraded"
        )
        status["degraded_continue_used"] = bool(status["missing_forcing_factors"])
        status["upstream_outage_detected"] = any(
            bool(item.get("upstream_outage_detected", False))
            for key, item in status.items()
            if key in active_source_ids
        )
        return forcing_dir, status

    def _segment_observations(
        self,
        drifter_df: pd.DataFrame,
        *,
        drifter_id: str,
        start_time_utc: str,
        end_time_utc: str,
    ) -> pd.DataFrame:
        start_time = pd.Timestamp(start_time_utc)
        end_time = pd.Timestamp(end_time_utc)
        subset = drifter_df[
            (drifter_df["ID"] == str(drifter_id))
            & (drifter_df["time"] >= start_time)
            & (drifter_df["time"] <= end_time)
        ].copy()
        subset = subset.sort_values("time").reset_index(drop=True)
        return subset

    def _evaluate_accepted_segments(
        self,
        accepted_df: pd.DataFrame,
        drifter_df: pd.DataFrame,
    ) -> tuple[pd.DataFrame, pd.DataFrame, list[dict[str, Any]]]:
        forcing_cache_by_month: dict[str, Path] = {}
        forcing_status: list[dict[str, Any]] = []
        available_factors_by_month: dict[str, set[str]] = {}
        for window in self._forcing_month_windows(accepted_df):
            forcing_dir, cache_status = self._prepare_forcing_cache(
                window["month_key"],
                window["start_time"],
                window["end_time"],
            )
            forcing_cache_by_month[window["month_key"]] = forcing_dir
            forcing_status.append(cache_status)
            available_factors_by_month[str(window["month_key"])] = set(cache_status.get("available_forcing_factors") or [])

        eligible_recipes: list[str] = []
        skipped_recipe_ids: set[str] = set()
        skipped_recipe_missing_factors: dict[str, list[str]] = {}
        for recipe_name in self.official_recipe_family:
            required_factors = self._recipe_required_factor_ids(recipe_name)
            missing_for_recipe: set[str] = set()
            for month_key, available_factors in available_factors_by_month.items():
                month_missing = required_factors.difference(available_factors)
                if month_missing:
                    missing_for_recipe.update(month_missing)
            if missing_for_recipe:
                skipped_recipe_ids.add(str(recipe_name))
                skipped_recipe_missing_factors[str(recipe_name)] = sorted(missing_for_recipe)
                continue
            eligible_recipes.append(str(recipe_name))

        self.skipped_recipe_ids.update(skipped_recipe_ids)
        self.skipped_recipe_missing_factors.update(skipped_recipe_missing_factors)
        for missing_list in skipped_recipe_missing_factors.values():
            self.missing_forcing_factors.update(missing_list)

        if skipped_recipe_ids and self.upstream_outage_detected and not self._continue_degraded_enabled():
            raise RuntimeError(
                "Phase 1 production rerun detected an upstream forcing outage that removed part of the "
                "official recipe family. The strict/reportable lane will not rank a reduced subset unless "
                "FORCING_OUTAGE_POLICY=continue_degraded is set explicitly."
            )

        if not eligible_recipes:
            if self._continue_degraded_enabled() and self.upstream_outage_detected:
                raise ForcingOutagePhaseSkipped(
                    phase=self.pipeline_phase,
                    workflow_mode=self.case.workflow_mode,
                    forcing_outage_policy=self.forcing_outage_policy,
                    reason="No recipe retained complete forcing coverage across the evaluated month subset.",
                    missing_forcing_factors=sorted(self.missing_forcing_factors),
                    skipped_recipe_ids=sorted(skipped_recipe_ids),
                )
            raise RuntimeError(
                "Phase 1 production rerun has no recipe with complete forcing coverage across the evaluated month subset."
            )

        loading_audit_rows: list[pd.DataFrame] = []
        segment_metric_rows: list[pd.DataFrame] = []
        for row in accepted_df.itertuples(index=False):
            segment_obs = self._segment_observations(
                drifter_df,
                drifter_id=row.drifter_id,
                start_time_utc=row.start_time_utc,
                end_time_utc=row.end_time_utc,
            )
            if segment_obs.empty:
                raise RuntimeError(f"Accepted segment {row.segment_id} could not be reconstructed from the drifter pool.")

            forcing_dir = forcing_cache_by_month[str(row.month_key)]
            payload = self.validation_service.run_validation_summary(
                drifter_df=segment_obs,
                forcing_dir=forcing_dir,
                recipe_names=eligible_recipes,
                output_dir=None,
                keep_scratch=False,
                transport_settings=self.transport_settings,
                require_wave_forcing=bool(self.transport_settings.get("require_wave_stokes_reader", False)),
                case_name_override=str(row.segment_id),
                verbose=False,
            )
            audit_df = payload["audit_df"].copy()
            if sorted(audit_df["recipe"].astype(str).tolist()) != sorted(eligible_recipes):
                raise RuntimeError(
                    f"Segment {row.segment_id} did not evaluate the expected complete-forcing recipe subset."
                )
            audit_df["segment_id"] = row.segment_id
            audit_df["drifter_id"] = row.drifter_id
            audit_df["start_time_utc"] = row.start_time_utc
            audit_df["end_time_utc"] = row.end_time_utc
            audit_df["month_key"] = row.month_key
            loading_audit_rows.append(audit_df)

            metrics_df = audit_df[
                [
                    "segment_id",
                    "drifter_id",
                    "start_time_utc",
                    "end_time_utc",
                    "month_key",
                    "recipe",
                    "validity_flag",
                    "status_flag",
                    "hard_fail",
                    "hard_fail_reason",
                    "invalidity_reason",
                    "ncs_score",
                    "actual_current_reader",
                    "actual_wind_reader",
                    "actual_wave_reader",
                    "wave_loading_status",
                    "current_fallback_used",
                    "wind_fallback_used",
                    "wave_fallback_used",
                ]
            ].copy()
            metrics_df["recipe_family"] = self.ranking_subset_label
            metrics_df["is_gfs_recipe"] = metrics_df["recipe"].astype(str).str.endswith("_gfs")
            segment_metric_rows.append(metrics_df)

        loading_audit_df = pd.concat(loading_audit_rows, ignore_index=True).sort_values(
            ["segment_id", "recipe"]
        ).reset_index(drop=True)
        segment_metrics_df = pd.concat(segment_metric_rows, ignore_index=True).sort_values(
            ["segment_id", "recipe"]
        ).reset_index(drop=True)
        return loading_audit_df, segment_metrics_df, forcing_status

    def _build_recipe_tables(
        self,
        segment_metrics_df: pd.DataFrame,
        *,
        recipe_rank_pool: str,
    ) -> tuple[pd.DataFrame, pd.DataFrame, str]:
        summary_rows: list[dict[str, Any]] = []
        evaluated_segment_count = int(segment_metrics_df["segment_id"].nunique()) if not segment_metrics_df.empty else 0
        for recipe, group in segment_metrics_df.groupby("recipe", sort=True):
            valid_group = group[group["validity_flag"] == "valid"].copy()
            if valid_group.empty:
                raise RuntimeError(f"Recipe {recipe} has zero valid segment scores in the production rerun.")
            summary_rows.append(
                {
                    "recipe": recipe,
                    "recipe_rank_pool": recipe_rank_pool,
                    "segment_count": int(len(group)),
                    "valid_segment_count": int(len(valid_group)),
                    "invalid_segment_count": int(len(group) - len(valid_group)),
                    "mean_ncs_score": float(valid_group["ncs_score"].mean()),
                    "median_ncs_score": float(valid_group["ncs_score"].median()),
                    "std_ncs_score": float(valid_group["ncs_score"].std(ddof=0)),
                    "min_ncs_score": float(valid_group["ncs_score"].min()),
                    "max_ncs_score": float(valid_group["ncs_score"].max()),
                    "is_gfs_recipe": bool(str(recipe).endswith("_gfs")),
                    "status": "completed",
                    "excluded_from_ranking": False,
                    "missing_forcing_factors": "",
                }
            )

        for recipe in sorted(self.skipped_recipe_ids):
            summary_rows.append(
                {
                    "recipe": recipe,
                    "recipe_rank_pool": recipe_rank_pool,
                    "segment_count": evaluated_segment_count,
                    "valid_segment_count": 0,
                    "invalid_segment_count": 0,
                    "mean_ncs_score": np.nan,
                    "median_ncs_score": np.nan,
                    "std_ncs_score": np.nan,
                    "min_ncs_score": np.nan,
                    "max_ncs_score": np.nan,
                    "is_gfs_recipe": bool(str(recipe).endswith("_gfs")),
                    "status": "skipped_missing_forcing",
                    "excluded_from_ranking": True,
                    "missing_forcing_factors": ";".join(self.skipped_recipe_missing_factors.get(str(recipe), [])),
                }
            )

        recipe_summary_df = pd.DataFrame(summary_rows).sort_values("recipe").reset_index(drop=True)
        recipe_ranking_df = recipe_summary_df[recipe_summary_df["status"] == "completed"].sort_values(
            ["mean_ncs_score", "median_ncs_score", "invalid_segment_count", "recipe"],
            ascending=[True, True, True, True],
        ).reset_index(drop=True)
        if recipe_ranking_df.empty:
            raise RuntimeError("No recipes remain rankable after forcing-outage filtering.")
        recipe_ranking_df.insert(0, "rank", np.arange(1, len(recipe_ranking_df) + 1))
        winning_recipe = str(recipe_ranking_df.iloc[0]["recipe"])
        return recipe_summary_df, recipe_ranking_df, winning_recipe

    def _build_candidate_baseline_payload(
        self,
        *,
        winning_recipe: str,
        official_b1_recipe: str,
        adoption_decision: dict[str, Any],
        accepted_df: pd.DataFrame,
        rejected_df: pd.DataFrame,
        ranking_subset_df: pd.DataFrame,
    ) -> dict[str, Any]:
        default_workflow_scope = [
            "phase1_regional_2016_2022",
            "mindoro_retro_2023",
            "dwh_retro_2010",
        ]
        candidate_cfg = self.candidate_baseline_config
        default_notes = [
            "This artifact is staged only and does not overwrite config/phase1_baseline_selection.yaml.",
            "Downstream trial runs may set BASELINE_SELECTION_PATH to this candidate artifact explicitly.",
        ]
        notes = [str(value) for value in candidate_cfg.get("notes") or [] if str(value).strip()] if candidate_cfg else []
        if not notes:
            notes = default_notes
        if self.degraded_continue_used:
            notes = [
                *notes,
                "This candidate was produced under degraded continuation because at least one upstream forcing factor was temporarily unavailable.",
                "Only recipes with complete forcing coverage across the evaluated subset were ranked; skipped recipes remain excluded from ranking and a clean rerun is still required.",
            ]
        status_flag = str(candidate_cfg.get("status_flag") or "valid")
        valid = bool(candidate_cfg.get("valid", True))
        provisional = bool(candidate_cfg.get("provisional", False))
        rerun_required = bool(candidate_cfg.get("rerun_required", False))
        if self.degraded_continue_used:
            status_flag = "provisional"
            valid = False
            provisional = True
            rerun_required = True
        return {
            "baseline_id": str(
                candidate_cfg.get("baseline_id")
                or "phase1_historical_transport_baseline_candidate_2016_2022_v1"
            ),
            "description": str(
                candidate_cfg.get("description")
                or "Staged candidate Phase 1 baseline from the completed 2016-2022 regional transport-validation rerun"
            ),
            "selected_recipe": winning_recipe,
            "historical_four_recipe_winner": winning_recipe,
            "official_b1_recipe": official_b1_recipe,
            "official_b1_selection_rule": str(adoption_decision["official_b1_selection_rule"]),
            "gfs_historical_winner_not_adopted": bool(
                adoption_decision["gfs_historical_winner_not_adopted"]
            ),
            "non_gfs_fallback_recipe": str(adoption_decision["non_gfs_fallback_recipe"] or ""),
            "reason_for_non_adoption": str(adoption_decision["reason_for_non_adoption"] or ""),
            "source_kind": str(candidate_cfg.get("source_kind") or "staged_production_candidate"),
            "status_flag": status_flag,
            "valid": valid,
            "provisional": provisional,
            "rerun_required": rerun_required,
            "promotion_required": bool(candidate_cfg.get("promotion_required", True)),
            "selection_basis": str(
                candidate_cfg.get("selection_basis")
                or "Completed 2016-2022 regional drogued-only non-overlapping 72 h transport-validation rerun"
            ),
            "forcing_outage_policy": self.forcing_outage_policy,
            "forcing_source_budget_seconds": resolve_forcing_source_budget_seconds(),
            "degraded_continue_used": self.degraded_continue_used,
            "upstream_outage_detected": self.upstream_outage_detected,
            "missing_forcing_factors": sorted(self.missing_forcing_factors),
            "skipped_recipe_ids": sorted(self.skipped_recipe_ids),
            "workflow_scope": [
                str(value)
                for value in (candidate_cfg.get("workflow_scope") or default_workflow_scope)
                if str(value).strip()
            ],
            "historical_validation_artifacts": [
                _relative(self.repo_root, self.paths["drifter_registry"]),
                _relative(self.repo_root, self.paths["accepted_registry"]),
                _relative(self.repo_root, self.paths["rejected_registry"]),
                _relative(self.repo_root, self.paths["ranking_subset_registry"]),
                _relative(self.repo_root, self.paths["ranking_subset_report"]),
                _relative(self.repo_root, self.paths["local_input_inventory"]),
                _relative(self.repo_root, self.paths["loading_audit"]),
                _relative(self.repo_root, self.paths["segment_metrics"]),
                _relative(self.repo_root, self.paths["recipe_summary"]),
                _relative(self.repo_root, self.paths["recipe_ranking"]),
            ],
            "notes": notes,
            "chapter3_finalization_audit": {
                "target_historical_window": {
                    "start_date": self.window_start.strftime("%Y-%m-%d"),
                    "end_date": self.window_end.strftime("%Y-%m-%d"),
                },
                "phase1_validation_box": self.validation_box,
                "core_pool_policy": "drogued_segments_only",
                "segment_policy": {
                    "horizon_hours": self.segment_horizon_hours,
                    "overlap_policy": "non_overlapping",
                },
                "official_recipe_family": list(self.official_recipe_family),
                "ranking_subset": {
                    "label": self.ranking_subset_label,
                    "config": self.ranking_subset_config,
                    "accepted_segment_count": int(len(ranking_subset_df)),
                },
                "current_local_evidence_scope": str(
                    candidate_cfg.get("current_local_evidence_scope")
                    or "completed_2016_2022_regional_transport_validation_rerun"
                ),
                "accepted_segment_count": int(len(accepted_df)),
                "rejected_segment_count": int(len(rejected_df)),
                "expected_phase1_artifacts": {
                    "drifter_registry": _relative(self.repo_root, self.paths["drifter_registry"]),
                    "accepted_segment_registry": _relative(self.repo_root, self.paths["accepted_registry"]),
                    "rejected_segment_registry": _relative(self.repo_root, self.paths["rejected_registry"]),
                    "ranking_subset_registry": _relative(self.repo_root, self.paths["ranking_subset_registry"]),
                    "ranking_subset_report": _relative(self.repo_root, self.paths["ranking_subset_report"]),
                    "local_input_inventory": _relative(self.repo_root, self.paths["local_input_inventory"]),
                    "gfs_preflight": _relative(self.repo_root, self.paths["gfs_preflight"])
                    if self.paths["gfs_preflight"].exists()
                    else "",
                    "loading_audit": _relative(self.repo_root, self.paths["loading_audit"]),
                    "segment_metrics": _relative(self.repo_root, self.paths["segment_metrics"]),
                    "recipe_summary": _relative(self.repo_root, self.paths["recipe_summary"]),
                    "recipe_ranking": _relative(self.repo_root, self.paths["recipe_ranking"]),
                    "frozen_baseline_candidate": _relative(self.repo_root, self.paths["baseline_candidate"]),
                    "adoption_decision_json": _relative(
                        self.repo_root, self.paths["adoption_decision_json"]
                    ),
                    "adoption_decision_md": _relative(
                        self.repo_root, self.paths["adoption_decision_md"]
                    ),
                },
                "distance_audit": {
                    "enabled": bool(self.distance_audit_source is not None),
                    "source_point_path": _relative(self.repo_root, Path(self.distance_audit_source["path"]))
                    if self.distance_audit_source is not None
                    else "",
                    "diagnostic_only": bool(self.distance_audit_config.get("diagnostic_only", True)),
                },
                "loading_audit_policy": {
                    "hard_fail_on_missing_required_forcing": not self.degraded_continue_used,
                    "hard_fail_on_empty_valid_recipe_set": True,
                    "require_wave_stokes_reader": bool(self.transport_settings.get("require_wave_stokes_reader", False)),
                    "forcing_source_budget_seconds": resolve_forcing_source_budget_seconds(),
                },
                "audit_status": {
                    "classification": "provisional_due_to_forcing_outage" if self.degraded_continue_used else "implemented_and_scientifically_ready",
                    "full_production_rerun_required": bool(self.degraded_continue_used),
                    "blocker": (
                        "Upstream forcing outage reduced the evaluated recipe family; rerun required after provider recovery."
                        if self.degraded_continue_used
                        else ""
                    ),
                },
            },
        }

    def _build_official_baseline_payload(
        self,
        *,
        recipe_ranking_df: pd.DataFrame,
        adoption_decision: dict[str, Any],
        accepted_df: pd.DataFrame,
        rejected_df: pd.DataFrame,
        ranking_subset_df: pd.DataFrame,
    ) -> dict[str, Any]:
        baseline_cfg = self.official_baseline_update_config
        official_recipe = str(adoption_decision["official_b1_recipe"])
        historical_winner = str(adoption_decision["historical_four_recipe_winner"])
        notes = [
            str(value)
            for value in (baseline_cfg.get("notes") or [])
            if str(value).strip()
        ]
        if adoption_decision["gfs_historical_winner_not_adopted"]:
            notes.append(
                f"The completed four-recipe historical winner was `{historical_winner}`, but official B1 uses "
                f"`{official_recipe}` under the spill-usable non-GFS fallback rule."
            )
        return {
            "baseline_id": str(
                baseline_cfg.get("baseline_id")
                or "mindoro_phase1_focused_recipe_provenance_v2"
            ),
            "description": str(
                baseline_cfg.get("description")
                or "Current default Mindoro spill-case Phase 1 recipe provenance finalized from the separate focused pre-spill 2016-2023 Mindoro drifter rerun"
            ),
            "selected_recipe": official_recipe,
            "historical_four_recipe_winner": historical_winner,
            "official_b1_selection_rule": str(adoption_decision["official_b1_selection_rule"]),
            "gfs_historical_winner_not_adopted": bool(
                adoption_decision["gfs_historical_winner_not_adopted"]
            ),
            "non_gfs_fallback_recipe": str(adoption_decision["non_gfs_fallback_recipe"] or ""),
            "reason_for_non_adoption": str(adoption_decision["reason_for_non_adoption"] or ""),
            "source_kind": str(
                baseline_cfg.get("source_kind")
                or "focused_mindoro_phase1_provenance_artifact"
            ),
            "status_flag": "valid",
            "valid": True,
            "provisional": False,
            "rerun_required": False,
            "selection_basis": str(
                baseline_cfg.get("selection_basis")
                or "Focused Mindoro pre-spill 2016-2023 drogued-only non-overlapping 72 h transport-validation rerun with the full four-recipe family ranked on the February-April seasonal subset"
            ),
            "workflow_scope": [
                str(value)
                for value in (baseline_cfg.get("workflow_scope") or ["mindoro_retro_2023"])
                if str(value).strip()
            ],
            "historical_validation_artifacts": [
                _relative(self.repo_root, self.paths["baseline_candidate"]),
                _relative(self.repo_root, self.paths["recipe_ranking"]),
                _relative(self.repo_root, self.paths["accepted_registry"]),
                _relative(self.repo_root, self.paths["local_input_inventory"]),
                _relative(self.repo_root, self.paths["manifest"]),
                _relative(self.repo_root, self.paths["adoption_decision_json"]),
                _relative(self.repo_root, self.paths["adoption_decision_md"]),
            ],
            "notes": notes,
            "chapter3_finalization_audit": {
                "target_historical_window": {
                    "start_date": self.window_start.strftime("%Y-%m-%d"),
                    "end_date": self.window_end.strftime("%Y-%m-%d"),
                },
                "phase1_validation_box": self.validation_box,
                "core_pool_policy": "drogued_segments_only",
                "segment_policy": {
                    "horizon_hours": self.segment_horizon_hours,
                    "overlap_policy": "non_overlapping",
                },
                "official_recipe_family": list(self.official_recipe_family),
                "ranking_subset": {
                    "label": self.ranking_subset_label,
                    "config": self.ranking_subset_config,
                    "accepted_segment_count": int(len(ranking_subset_df)),
                },
                "current_local_evidence_scope": str(
                    baseline_cfg.get("current_local_evidence_scope")
                    or "mindoro_focused_pre_spill_2016_2023_recipe_provenance"
                ),
                "current_local_evidence_dates": [
                    str(value)
                    for value in (
                        baseline_cfg.get("current_local_evidence_dates")
                        or ["2016-2019 and 2021 accepted drifter segments within the focused Mindoro box"]
                    )
                    if str(value).strip()
                ],
                "staged_candidate_baseline_artifact": _relative(
                    self.repo_root, self.paths["baseline_candidate"]
                ),
                "regional_reference_baseline_artifact": "output/phase1_production_rerun/phase1_baseline_selection_candidate.yaml",
                "expected_phase1_artifacts": {
                    "accepted_segment_registry": _relative(self.repo_root, self.paths["accepted_registry"]),
                    "rejected_segment_registry": _relative(self.repo_root, self.paths["rejected_registry"]),
                    "local_input_inventory": _relative(self.repo_root, self.paths["local_input_inventory"]),
                    "segment_metrics": _relative(self.repo_root, self.paths["segment_metrics"]),
                    "recipe_summary": _relative(self.repo_root, self.paths["recipe_summary"]),
                    "recipe_ranking": _relative(self.repo_root, self.paths["recipe_ranking"]),
                    "frozen_baseline_artifact": _relative(self.repo_root, self.baseline_path),
                    "frozen_baseline_candidate": _relative(self.repo_root, self.paths["baseline_candidate"]),
                    "adoption_decision_json": _relative(
                        self.repo_root, self.paths["adoption_decision_json"]
                    ),
                    "adoption_decision_md": _relative(
                        self.repo_root, self.paths["adoption_decision_md"]
                    ),
                },
                "loading_audit_policy": {
                    "hard_fail_on_missing_required_forcing": True,
                    "hard_fail_on_empty_valid_recipe_set": True,
                },
                "audit_status": {
                    "classification": "implemented_and_finalized_for_mindoro_recipe_provenance",
                    "full_production_rerun_required": False,
                    "blocker": "",
                },
            },
        }

    def _build_manifest(
        self,
        *,
        drifter_chunk_status: list[dict[str, Any]],
        forcing_status: list[dict[str, Any]],
        gfs_preflight_rows: list[dict[str, Any]],
        accepted_df: pd.DataFrame,
        rejected_df: pd.DataFrame,
        ranking_subset_df: pd.DataFrame,
        loading_audit_df: pd.DataFrame,
        recipe_ranking_df: pd.DataFrame,
        winning_recipe: str,
        adoption_decision: dict[str, Any],
        candidate_payload: dict[str, Any],
        baseline_hash_before: str,
        baseline_hash_after: str,
    ) -> dict[str, Any]:
        gfs_rows = recipe_ranking_df[recipe_ranking_df["recipe"].astype(str).str.endswith("_gfs")]
        return {
            "phase": "phase1_production_rerun",
            "workflow_mode": self.case.workflow_mode,
            "workflow_flavor": self.case.workflow_flavor,
            "transport_track": self.case.transport_track,
            "forcing_outage_policy": self.forcing_outage_policy,
            "forcing_source_budget_seconds": resolve_forcing_source_budget_seconds(),
            "degraded_continue_used": self.degraded_continue_used,
            "upstream_outage_detected": self.upstream_outage_detected,
            "missing_forcing_factors": sorted(self.missing_forcing_factors),
            "skipped_recipe_ids": sorted(self.skipped_recipe_ids),
            "rerun_required": bool(candidate_payload.get("rerun_required", False)),
            "time_window": {
                "start_utc": self.window_start.isoformat(),
                "end_utc": self.window_end.isoformat(),
            },
            "validation_box": self.validation_box,
            "drifter_query_box": self.drifter_query_box,
            "segment_policy": {
                "horizon_hours": self.segment_horizon_hours,
                "timestep_hours": self.segment_timestep_hours,
                "overlap_policy": "non_overlapping",
                "drogue_policy": "drogued_only_strict",
            },
            "ranking_subset": {
                "label": self.ranking_subset_label,
                "config": self.ranking_subset_config,
                "segment_count": int(len(ranking_subset_df)),
            },
            "distance_audit": {
                "enabled": bool(self.distance_audit_source is not None),
                "source_point": {
                    "label": str(self.distance_audit_source["label"]),
                    "lon": float(self.distance_audit_source["lon"]),
                    "lat": float(self.distance_audit_source["lat"]),
                    "path": _relative(self.repo_root, Path(self.distance_audit_source["path"])),
                }
                if self.distance_audit_source is not None
                else {},
                "diagnostic_only": bool(self.distance_audit_config.get("diagnostic_only", True)),
            },
            "official_recipe_family": list(self.official_recipe_family),
            "accepted_segment_count": int(len(accepted_df)),
            "rejected_segment_count": int(len(rejected_df)),
            "ranking_subset_segment_count": int(len(ranking_subset_df)),
            "winning_recipe": winning_recipe,
            "historical_four_recipe_winner": str(
                adoption_decision["historical_four_recipe_winner"]
            ),
            "official_b1_recipe": str(adoption_decision["official_b1_recipe"]),
            "official_b1_selection_rule": str(
                adoption_decision["official_b1_selection_rule"]
            ),
            "gfs_historical_winner_not_adopted": bool(
                adoption_decision["gfs_historical_winner_not_adopted"]
            ),
            "gfs_capable_recipes_ran": bool(
                not gfs_rows.empty and (gfs_rows["valid_segment_count"] > 0).all()
            ),
            "loading_audit_invalid_count": int((loading_audit_df["validity_flag"] != "valid").sum()),
            "drifter_chunk_status": drifter_chunk_status,
            "gfs_preflight": {
                "required": bool(gfs_preflight_rows),
                "rows": gfs_preflight_rows,
                "artifact": _relative(self.repo_root, self.paths["gfs_preflight"])
                if self.paths["gfs_preflight"].exists()
                else "",
            },
            "local_input_store": {
                "root": _relative(self.repo_root, self.local_input_root),
                "drifter_chunk_root": _relative(self.repo_root, self.drifter_cache_root),
                "forcing_month_root": _relative(self.repo_root, self.forcing_cache_root),
                "inventory_csv": _relative(self.repo_root, self.paths["local_input_inventory"]),
                "legacy_output_scratch_root": _relative(self.repo_root, self.scratch_root),
                "policy": "persist_workflow_scoped_monthly_inputs_under_data_and_reuse_them_before_remote_refetch",
            },
            "forcing_cache_status": forcing_status,
            "baseline_candidate": {
                "path": _relative(self.repo_root, self.paths["baseline_candidate"]),
                "selected_recipe": candidate_payload["selected_recipe"],
                "promotion_required": bool(candidate_payload.get("promotion_required", True)),
                "status_flag": candidate_payload.get("status_flag"),
                "valid": bool(candidate_payload.get("valid", False)),
                "provisional": bool(candidate_payload.get("provisional", False)),
                "rerun_required": bool(candidate_payload.get("rerun_required", False)),
            },
            "canonical_baseline_integrity": {
                "path": _relative(self.repo_root, self.baseline_path),
                "sha256_before": baseline_hash_before,
                "sha256_after": baseline_hash_after,
                "unchanged": baseline_hash_before == baseline_hash_after,
                "official_baseline_update_enabled": self._official_baseline_update_enabled(),
            },
            "artifacts": {
                key: _relative(self.repo_root, path)
                for key, path in self.paths.items()
            },
        }


def run_phase1_production_rerun() -> dict[str, Any]:
    service = Phase1ProductionRerunService()
    return service.run()
