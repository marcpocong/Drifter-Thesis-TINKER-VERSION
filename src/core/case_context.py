"""
Centralized workflow and case loading.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

import pandas as pd
import yaml

from src.core.domain_semantics import (
    coerce_bounds,
    resolve_legacy_prototype_display_domain,
    resolve_mindoro_case_domain,
    resolve_phase1_validation_box,
)

SETTINGS_PATH = Path(__file__).resolve().parent.parent.parent / "config" / "settings.yaml"
DEFAULT_MINDORO_FEATURE_SERVER = (
    "https://services1.arcgis.com/RTK5Unh1Z71JKIiR/ArcGIS/rest/services/"
    "Mindoro_Oil_Spills_Monitoring_Map_WFL1/FeatureServer"
)
PROTOTYPE_CASE_ID_ENV = "PROTOTYPE_CASE_ID"


def load_settings(settings_path: str | Path = SETTINGS_PATH) -> dict:
    with open(settings_path, "r") as f:
        return yaml.safe_load(f) or {}


def _load_yaml(path: str | Path) -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f) or {}


def _parse_env_or_default(name: str, default):
    raw = os.environ.get(name)
    if raw is None:
        return default

    try:
        parsed = yaml.safe_load(raw)
    except Exception:
        parsed = raw
    return parsed


@dataclass(frozen=True)
class CaseLayerConfig:
    key: str
    role: str
    layer_id: int
    name: str
    local_name: str
    service_url: str
    geometry_type: str
    event_time_utc: str | None = None

    def geojson_path(self, run_name: str) -> Path:
        return Path("data") / "arcgis" / run_name / f"{self.local_name}.geojson"

    def mask_path(self, run_name: str) -> Path:
        return Path("data") / "arcgis" / run_name / f"{self.local_name}.tif"

    def raw_geojson_path(self, run_name: str) -> Path:
        return Path("data") / "arcgis" / run_name / f"{self.local_name}_raw.geojson"

    def processed_vector_path(self, run_name: str) -> Path:
        return Path("data") / "arcgis" / run_name / f"{self.local_name}_processed.gpkg"

    def service_metadata_path(self, run_name: str) -> Path:
        return Path("data") / "arcgis" / run_name / f"{self.local_name}_service_metadata.json"

    def processing_notes_path(self, run_name: str) -> Path:
        return Path("data") / "arcgis" / run_name / f"{self.local_name}_processing_notes.json"

    def official_observed_mask_path(self, run_name: str) -> Path:
        if not self.event_time_utc:
            raise ValueError(f"Layer {self.local_name} is missing event_time_utc for observed-mask naming.")
        event_date = str(pd.to_datetime(self.event_time_utc).date())
        return Path("data") / "arcgis" / run_name / f"obs_mask_{event_date}.tif"


@dataclass(frozen=True)
class CaseContext:
    workflow_mode: str
    workflow_lane: str
    active_domain_name: str
    mode_label: str
    case_id: str
    run_name: str
    description: str
    region: list[float]
    phase1_validation_box: list[float]
    mindoro_case_domain: list[float]
    legacy_prototype_display_domain: list[float]
    is_prototype: bool
    initialization_mode: str
    source_point_role: str
    release_mode: str
    release_reference: str
    validation_target: str
    release_start_utc: str
    release_end_utc: str
    simulation_start_utc: str
    simulation_end_utc: str
    forcing_start_utc: str
    forcing_end_utc: str
    drifter_required: bool
    drifter_mode: str
    configured_drifter_id: str | None
    prototype_case_dates: tuple[str, ...]
    current_case_date: str | None
    orchestration_tokens: tuple[str, ...]
    current_orchestration_token: str | None
    case_definition_path: str | None
    initialization_layer: CaseLayerConfig
    validation_layer: CaseLayerConfig
    provenance_layer: CaseLayerConfig

    @property
    def is_official(self) -> bool:
        return self.workflow_lane == "official_spill_case"

    @property
    def is_historical_regional(self) -> bool:
        return self.workflow_lane == "historical_regional_validation"

    @property
    def forcing_start_date(self) -> str:
        return str(pd.to_datetime(self.forcing_start_utc).date())

    @property
    def forcing_end_date(self) -> str:
        return str(pd.to_datetime(self.forcing_end_utc).date())

    @property
    def phase_1_start_date_value(self):
        if self.is_prototype:
            if self.current_case_date:
                return self.current_case_date
            if len(self.prototype_case_dates) == 1:
                return self.prototype_case_dates[0]
            if self.prototype_case_dates:
                return list(self.prototype_case_dates)
            return self.release_start_utc
        return self.forcing_start_date

    @property
    def orchestration_dates(self) -> list[str]:
        if self.current_orchestration_token is None and len(self.orchestration_tokens) > 1:
            return list(self.orchestration_tokens)
        return []

    @property
    def active_case_date(self) -> str:
        if self.current_case_date:
            return self.current_case_date
        if self.prototype_case_dates:
            return self.prototype_case_dates[0]
        if self.current_orchestration_token:
            return self.current_orchestration_token
        if self.orchestration_tokens:
            return self.orchestration_tokens[0]
        return self.forcing_start_date

    @property
    def workflow_flavor(self) -> str:
        if self.is_prototype:
            return f"{self.workflow_mode} prototype/debug mode"
        if self.is_historical_regional:
            return "historical/regional validation mode"
        return "official spill-case mode"

    @property
    def transport_track(self) -> str:
        if self.workflow_mode == "prototype_2021":
            return "preferred accepted-segment debug lane using fixed 2021 drifter windows and the official Phase 1 recipe family"
        if self.is_prototype:
            return "legacy prototype historical transport calibration (not the final Chapter 3 regional study)"
        if self.is_historical_regional:
            return "historical/regional transport validation using strict drogued-only non-overlapping 72 h drifter segments"
        return "official spill case kept separate from historical regional transport validation"

    @property
    def recipe_resolution_mode(self) -> str:
        if self.workflow_mode == "prototype_2021":
            return "preferred debug lane using the official Phase 1 recipe family on fixed accepted 2021 drifter segments"
        if self.is_prototype:
            return "prototype case-local Phase 1 ranking (not the final frozen regional baseline)"
        if self.is_historical_regional:
            return "full scientific Phase 1 regional production rerun using the Chapter 3 official recipe family"
        return "frozen Phase 1 baseline selection for spill-case workflows"

    @property
    def arcgis_layers(self) -> list[CaseLayerConfig]:
        return [self.initialization_layer, self.validation_layer, self.provenance_layer]


def _default_prototype_layer_specs(
    start_time_utc: str,
    end_time_utc: str,
) -> tuple[CaseLayerConfig, CaseLayerConfig, CaseLayerConfig]:
    init_layer = CaseLayerConfig(
        key="initialization_polygon",
        role="initialization_polygon",
        layer_id=3,
        name="seed_polygon_mar3",
        local_name="seed_polygon_mar3",
        service_url=DEFAULT_MINDORO_FEATURE_SERVER,
        geometry_type="polygon",
        event_time_utc=start_time_utc,
    )
    validation_layer = CaseLayerConfig(
        key="validation_polygon",
        role="validation_polygon",
        layer_id=1,
        name="validation_polygon_mar6",
        local_name="validation_polygon_mar6",
        service_url=DEFAULT_MINDORO_FEATURE_SERVER,
        geometry_type="polygon",
        event_time_utc=end_time_utc,
    )
    provenance_layer = CaseLayerConfig(
        key="provenance_source_point",
        role="active_release_fallback",
        layer_id=0,
        name="source_point_metadata",
        local_name="source_point_metadata",
        service_url=DEFAULT_MINDORO_FEATURE_SERVER,
        geometry_type="point",
        event_time_utc=start_time_utc,
    )
    return init_layer, validation_layer, provenance_layer


def _default_historical_regional_layer_specs(
    start_time_utc: str,
    end_time_utc: str,
) -> tuple[CaseLayerConfig, CaseLayerConfig, CaseLayerConfig]:
    init_layer = CaseLayerConfig(
        key="regional_segment_start",
        role="regional_segment_start",
        layer_id=-1,
        name="regional_segment_start",
        local_name="regional_segment_start",
        service_url="",
        geometry_type="point",
        event_time_utc=start_time_utc,
    )
    validation_layer = CaseLayerConfig(
        key="regional_segment_validation",
        role="regional_segment_validation",
        layer_id=-1,
        name="regional_segment_validation",
        local_name="regional_segment_validation",
        service_url="",
        geometry_type="point",
        event_time_utc=end_time_utc,
    )
    provenance_layer = CaseLayerConfig(
        key="regional_segment_provenance",
        role="regional_segment_provenance",
        layer_id=-1,
        name="regional_segment_provenance",
        local_name="regional_segment_provenance",
        service_url="",
        geometry_type="point",
        event_time_utc=start_time_utc,
    )
    return init_layer, validation_layer, provenance_layer


def _resolve_repo_domains(
    settings: dict,
    *sources: dict | None,
) -> tuple[list[float], list[float], list[float]]:
    phase1_audit = settings.get("phase1_official_audit") or {}
    phase1_validation_box = resolve_phase1_validation_box(*sources, phase1_audit, settings)
    mindoro_case_domain = resolve_mindoro_case_domain(*sources, settings)
    legacy_prototype_display_domain = resolve_legacy_prototype_display_domain(*sources, settings)
    return phase1_validation_box, mindoro_case_domain, legacy_prototype_display_domain


def _load_official_context(settings: dict, workflow_mode: str) -> CaseContext:
    case_files = settings.get("workflow_case_files") or {}
    case_path = Path(os.environ.get("CASE_CONFIG_PATH") or case_files.get(workflow_mode, ""))
    if not case_path.exists():
        raise FileNotFoundError(
            f"Workflow mode '{workflow_mode}' requires a case config file. Missing: {case_path}"
        )

    cfg = _load_yaml(case_path)
    phase1_validation_box, mindoro_case_domain, legacy_prototype_display_domain = _resolve_repo_domains(
        settings,
        cfg,
    )
    arcgis_cfg = cfg.get("arcgis") or {}
    layers_cfg = arcgis_cfg.get("layers") or {}
    service_url = arcgis_cfg.get("feature_server_url", DEFAULT_MINDORO_FEATURE_SERVER)

    def build_layer(layer_key: str) -> CaseLayerConfig:
        layer_cfg = layers_cfg[layer_key]
        return CaseLayerConfig(
            key=layer_key,
            role=layer_cfg.get("role", layer_key),
            layer_id=int(layer_cfg["layer_id"]),
            name=layer_cfg.get("name", layer_key),
            local_name=layer_cfg.get("local_name", layer_cfg.get("name", layer_key)),
            service_url=layer_cfg.get("service_url", service_url),
            geometry_type=layer_cfg.get("geometry_type", "polygon"),
            event_time_utc=layer_cfg.get("event_time_utc"),
        )

    run_name = os.environ.get("RUN_NAME", cfg["case_id"])
    if cfg.get("mindoro_case_domain") is not None or workflow_mode == "mindoro_retro_2023":
        active_domain_name = "mindoro_case_domain"
        active_region = list(mindoro_case_domain)
    else:
        active_domain_name = "configured_case_domain"
        active_region = coerce_bounds(cfg.get("region") or mindoro_case_domain, "region")
    return CaseContext(
        workflow_mode=workflow_mode,
        workflow_lane="official_spill_case",
        active_domain_name=active_domain_name,
        mode_label=cfg.get("mode_label", "Official workflow"),
        case_id=cfg["case_id"],
        run_name=run_name,
        description=cfg.get("description", cfg["case_id"]),
        region=active_region,
        phase1_validation_box=phase1_validation_box,
        mindoro_case_domain=mindoro_case_domain,
        legacy_prototype_display_domain=legacy_prototype_display_domain,
        is_prototype=False,
        initialization_mode=cfg.get("initialization_mode", "initialization_polygon"),
        source_point_role=cfg.get("source_point_role", "provenance_only"),
        release_mode=cfg.get("release_mode", "instantaneous_polygon"),
        release_reference=cfg.get("release_reference", "initialization_polygon"),
        validation_target=cfg.get("validation_target", "validation_polygon"),
        release_start_utc=cfg["release_start_utc"],
        release_end_utc=cfg["release_end_utc"],
        simulation_start_utc=cfg["simulation_start_utc"],
        simulation_end_utc=cfg["simulation_end_utc"],
        forcing_start_utc=cfg.get("forcing_start_utc", cfg["simulation_start_utc"]),
        forcing_end_utc=cfg.get("forcing_end_utc", cfg["simulation_end_utc"]),
        drifter_required=bool((cfg.get("drifter") or {}).get("required", True)),
        drifter_mode=(cfg.get("drifter") or {}).get("mode", "fixed_case_window"),
        configured_drifter_id=None,
        prototype_case_dates=(),
        current_case_date=None,
        orchestration_tokens=(),
        current_orchestration_token=None,
        case_definition_path=str(case_path),
        initialization_layer=build_layer("initialization_polygon"),
        validation_layer=build_layer("validation_polygon"),
        provenance_layer=build_layer("provenance_source_point"),
    )


def _load_prototype_context(settings: dict) -> CaseContext:
    prototype_dates_raw = _parse_env_or_default("PHASE_1_START_DATE", settings["phase_1_start_date"])
    if isinstance(prototype_dates_raw, list):
        prototype_dates = tuple(str(item) for item in prototype_dates_raw)
        current_case_date = None
    else:
        current_case_date = str(prototype_dates_raw)
        prototype_dates = (current_case_date,)

    if not prototype_dates:
        raise ValueError("Prototype workflow requires at least one phase_1_start_date.")

    active_date = current_case_date or prototype_dates[0]
    start_ts = pd.to_datetime(active_date)
    end_ts = start_ts + pd.Timedelta(hours=72)
    start_time_utc = start_ts.strftime("%Y-%m-%dT00:00:00Z")
    end_time_utc = end_ts.strftime("%Y-%m-%dT00:00:00Z")
    init_layer, validation_layer, provenance_layer = _default_prototype_layer_specs(
        start_time_utc,
        end_time_utc,
    )
    run_name = os.environ.get("RUN_NAME", f"CASE_{active_date}")
    phase1_validation_box, mindoro_case_domain, legacy_prototype_display_domain = _resolve_repo_domains(settings)

    return CaseContext(
        workflow_mode="prototype_2016",
        workflow_lane="prototype",
        active_domain_name="legacy_prototype_display_domain",
        mode_label="Prototype 2016 debugging workflow",
        case_id=run_name,
        run_name=run_name,
        description="Prototype debugging workflow preserving the original 2016 multi-date behavior",
        region=list(legacy_prototype_display_domain),
        phase1_validation_box=phase1_validation_box,
        mindoro_case_domain=mindoro_case_domain,
        legacy_prototype_display_domain=legacy_prototype_display_domain,
        is_prototype=True,
        initialization_mode="initialization_polygon",
        source_point_role="active_release_fallback",
        release_mode="prototype_debug",
        release_reference="provenance_source_point_or_drifter",
        validation_target="validation_polygon",
        release_start_utc=start_time_utc,
        release_end_utc=start_time_utc,
        simulation_start_utc=start_time_utc,
        simulation_end_utc=end_time_utc,
        forcing_start_utc=start_time_utc,
        forcing_end_utc=end_time_utc,
        drifter_required=True,
        drifter_mode="prototype_scan",
        configured_drifter_id=None,
        prototype_case_dates=prototype_dates,
        current_case_date=current_case_date,
        orchestration_tokens=prototype_dates,
        current_orchestration_token=current_case_date,
        case_definition_path=None,
        initialization_layer=init_layer,
        validation_layer=validation_layer,
        provenance_layer=provenance_layer,
    )


def _load_configured_prototype_context(settings: dict, workflow_mode: str) -> CaseContext:
    case_files = settings.get("workflow_case_files") or {}
    case_path = Path(os.environ.get("CASE_CONFIG_PATH") or case_files.get(workflow_mode, ""))
    if not case_path.exists():
        raise FileNotFoundError(
            f"Workflow mode '{workflow_mode}' requires a case config file. Missing: {case_path}"
        )

    cfg = _load_yaml(case_path)
    configured_cases = [dict(item or {}) for item in cfg.get("cases") or []]
    if not configured_cases:
        raise ValueError(f"Workflow mode '{workflow_mode}' requires at least one configured case in {case_path}.")

    requested_case_id = os.environ.get(PROTOTYPE_CASE_ID_ENV)
    selected_case = None
    if requested_case_id:
        for item in configured_cases:
            if str(item.get("case_id") or "").strip() == requested_case_id:
                selected_case = item
                break
        if selected_case is None:
            raise ValueError(
                f"Workflow mode '{workflow_mode}' received {PROTOTYPE_CASE_ID_ENV}={requested_case_id!r}, "
                f"but that case_id is not defined in {case_path}."
            )
    else:
        selected_case = configured_cases[0]

    release_start_utc = str(selected_case["release_start_utc"])
    release_end_utc = str(selected_case.get("release_end_utc") or release_start_utc)
    simulation_start_utc = str(selected_case.get("simulation_start_utc") or release_start_utc)
    simulation_end_utc = str(selected_case["simulation_end_utc"])
    forcing_start_utc = str(selected_case.get("forcing_start_utc") or simulation_start_utc)
    forcing_end_utc = str(selected_case.get("forcing_end_utc") or simulation_end_utc)
    init_layer, validation_layer, provenance_layer = _default_historical_regional_layer_specs(
        release_start_utc,
        simulation_end_utc,
    )
    case_ids = tuple(str(item.get("case_id") or "").strip() for item in configured_cases if str(item.get("case_id") or "").strip())
    run_name = os.environ.get("RUN_NAME", str(selected_case.get("run_name") or selected_case["case_id"]))
    phase1_validation_box, mindoro_case_domain, legacy_prototype_display_domain = _resolve_repo_domains(
        settings,
        cfg,
        selected_case,
    )

    return CaseContext(
        workflow_mode=workflow_mode,
        workflow_lane="prototype",
        active_domain_name="legacy_prototype_display_domain",
        mode_label=str(cfg.get("mode_label") or "Preferred accepted-segment debug workflow"),
        case_id=str(selected_case["case_id"]),
        run_name=run_name,
        description=str(selected_case.get("description") or cfg.get("description") or selected_case["case_id"]),
        region=list(legacy_prototype_display_domain),
        phase1_validation_box=phase1_validation_box,
        mindoro_case_domain=mindoro_case_domain,
        legacy_prototype_display_domain=legacy_prototype_display_domain,
        is_prototype=True,
        initialization_mode="drifter_segment_start",
        source_point_role="active_release_fallback",
        release_mode="historical_drifter_segment_replay",
        release_reference="configured_accepted_segment",
        validation_target="drifter_trajectory",
        release_start_utc=release_start_utc,
        release_end_utc=release_end_utc,
        simulation_start_utc=simulation_start_utc,
        simulation_end_utc=simulation_end_utc,
        forcing_start_utc=forcing_start_utc,
        forcing_end_utc=forcing_end_utc,
        drifter_required=bool((cfg.get("drifter") or {}).get("required", True)),
        drifter_mode=str((cfg.get("drifter") or {}).get("mode") or "fixed_drifter_segment_window"),
        configured_drifter_id=str(selected_case.get("drifter_id") or "").strip() or None,
        prototype_case_dates=(),
        current_case_date=None,
        orchestration_tokens=case_ids,
        current_orchestration_token=requested_case_id,
        case_definition_path=str(case_path),
        initialization_layer=init_layer,
        validation_layer=validation_layer,
        provenance_layer=provenance_layer,
    )


def _load_historical_regional_context(settings: dict, workflow_mode: str) -> CaseContext:
    case_files = settings.get("workflow_case_files") or {}
    case_path = Path(os.environ.get("CASE_CONFIG_PATH") or case_files.get(workflow_mode, ""))
    if not case_path.exists():
        raise FileNotFoundError(
            f"Workflow mode '{workflow_mode}' requires a case config file. Missing: {case_path}"
        )

    cfg = _load_yaml(case_path)
    phase1_validation_box, mindoro_case_domain, legacy_prototype_display_domain = _resolve_repo_domains(
        settings,
        cfg,
    )
    historical_window = cfg.get("historical_window") or {}
    start_utc = str(historical_window.get("start_utc") or "2016-01-01T00:00:00Z")
    end_utc = str(historical_window.get("end_utc") or "2022-12-31T23:59:59Z")
    init_layer, validation_layer, provenance_layer = _default_historical_regional_layer_specs(
        start_utc,
        end_utc,
    )
    run_name = os.environ.get("RUN_NAME", cfg.get("case_id", "phase1_production_rerun"))

    return CaseContext(
        workflow_mode=workflow_mode,
        workflow_lane="historical_regional_validation",
        active_domain_name="phase1_validation_box",
        mode_label=cfg.get("mode_label", "Historical/regional validation workflow"),
        case_id=str(cfg.get("case_id") or "phase1_production_rerun"),
        run_name=run_name,
        description=cfg.get("description", run_name),
        region=list(phase1_validation_box),
        phase1_validation_box=phase1_validation_box,
        mindoro_case_domain=mindoro_case_domain,
        legacy_prototype_display_domain=legacy_prototype_display_domain,
        is_prototype=False,
        initialization_mode=cfg.get("initialization_mode", "regional_drifter_segment_window"),
        source_point_role=cfg.get("source_point_role", "drifter_segment_start"),
        release_mode=cfg.get("release_mode", "historical_drifter_segment_replay"),
        release_reference=cfg.get("release_reference", "phase1_drifter_registry"),
        validation_target=cfg.get("validation_target", "drifter_trajectory"),
        release_start_utc=start_utc,
        release_end_utc=start_utc,
        simulation_start_utc=start_utc,
        simulation_end_utc=end_utc,
        forcing_start_utc=start_utc,
        forcing_end_utc=end_utc,
        drifter_required=bool((cfg.get("drifter") or {}).get("required", True)),
        drifter_mode=(cfg.get("drifter") or {}).get("mode", "phase1_regional_monthly_chunks"),
        configured_drifter_id=None,
        prototype_case_dates=(),
        current_case_date=None,
        orchestration_tokens=(),
        current_orchestration_token=None,
        case_definition_path=str(case_path),
        initialization_layer=init_layer,
        validation_layer=validation_layer,
        provenance_layer=provenance_layer,
    )


@lru_cache(maxsize=1)
def get_case_context() -> CaseContext:
    settings = load_settings()
    workflow_mode = os.environ.get("WORKFLOW_MODE", settings.get("workflow_mode", "prototype_2021"))
    if workflow_mode == "prototype_2021":
        return _load_configured_prototype_context(settings, workflow_mode)
    if workflow_mode == "prototype_2016":
        return _load_prototype_context(settings)
    if workflow_mode == "phase1_regional_2016_2022":
        return _load_historical_regional_context(settings, workflow_mode)
    case_files = settings.get("workflow_case_files") or {}
    if workflow_mode in case_files:
        return _load_official_context(settings, workflow_mode)
    raise ValueError(f"Unsupported workflow_mode '{workflow_mode}'.")


def get_case_log_lines() -> list[str]:
    case = get_case_context()
    return [
        f"workflow_mode      : {case.workflow_mode}",
        f"case_id            : {case.case_id}",
        f"active_domain_name : {case.active_domain_name}",
        f"transport_track    : {case.transport_track}",
        f"recipe_resolution  : {case.recipe_resolution_mode}",
        f"initialization_mode: {case.initialization_mode}",
        f"source_point_role  : {case.source_point_role}",
        f"simulation_start   : {case.simulation_start_utc}",
        f"simulation_end     : {case.simulation_end_utc}",
        f"workflow_flavor    : {case.workflow_flavor}",
    ]
