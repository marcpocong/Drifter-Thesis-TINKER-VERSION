"""
Phase 3B observation-based validation.

Official Mindoro mode uses explicit manifest-driven March 6 pairings on the
canonical scoring grid. Prototype mode keeps the broader legacy candidate
logic for backward compatibility.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from src.core.case_context import get_case_context
from src.helpers.metrics import calculate_fss
from src.helpers.raster import GridBuilder
from src.helpers.scoring import apply_ocean_mask, load_sea_mask_array, precheck_same_grid
from src.utils.io import (
    get_ensemble_manifest_path,
    get_forecast_manifest_path,
    get_phase3b_forecast_candidates,
    resolve_recipe_selection,
)

try:
    from scipy.spatial import cKDTree
except ImportError:  # pragma: no cover - optional at runtime
    cKDTree = None

try:
    import geopandas as gpd
except ImportError:  # pragma: no cover - optional at runtime
    gpd = None

try:
    import matplotlib.pyplot as plt
except ImportError:  # pragma: no cover - optional at runtime
    plt = None


OFFICIAL_PHASE3B_WINDOWS_KM = [1, 3, 5, 10]
OFFICIAL_PRIMARY_SOURCE_SEMANTICS = "March6_date_composite_vs_March6_obsmask"
OFFICIAL_SENSITIVITY_SOURCE_SEMANTICS = "March6_control_footprint_vs_March6_obsmask"


@dataclass(frozen=True)
class Phase3BArtifacts:
    obs_registry: Path
    pairing_manifest: Path
    fss_by_date_window: Path
    summary: Path
    diagnostics: Path
    run_manifest: Path


def _json_bool(value) -> bool:
    return bool(value)


def _write_json(path: Path, payload: dict | list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)


def _resolve_artifact_path(path_str: str, case_output_dir: Path) -> Path:
    candidate = Path(path_str)
    if candidate.is_absolute():
        return candidate
    if candidate.parts and candidate.parts[0] == "output":
        return candidate
    return case_output_dir / candidate


def _load_json(path: Path, label: str) -> dict:
    if not path.exists():
        raise FileNotFoundError(f"{label} not found: {path}")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f) or {}


def _find_manifest_product(
    products: list[dict],
    product_type: str,
    timestamp_utc: str | None = None,
    date_utc: str | None = None,
) -> dict:
    matches = []
    for product in products:
        if str(product.get("product_type")) != product_type:
            continue
        if timestamp_utc is not None and str(product.get("timestamp_utc")) != timestamp_utc:
            continue
        if date_utc is not None and str(product.get("date_utc")) != date_utc:
            continue
        matches.append(product)
    if not matches:
        selector = timestamp_utc if timestamp_utc is not None else date_utc
        raise RuntimeError(
            f"Manifest is missing product_type='{product_type}'"
            f"{'' if selector is None else f' for {selector}'}."
        )
    return matches[0]


def derive_phase3b_status_from_upstream(
    forecast_manifest: dict,
    ensemble_manifest: dict,
) -> dict:
    forecast_flags = forecast_manifest.get("status_flags") or {}
    ensemble_flags = ensemble_manifest.get("status_flags") or {}
    transport = forecast_manifest.get("transport") or ensemble_manifest.get("transport") or {}
    recipe_selection = forecast_manifest.get("recipe_selection") or {}
    baseline_provenance = ensemble_manifest.get("baseline_provenance") or {}

    reasons: list[str] = []
    if _json_bool(transport.get("provisional_transport_model")):
        reasons.append("transport.provisional_transport_model=true")
    if _json_bool(forecast_flags.get("provisional")):
        reasons.append("forecast_manifest.status_flags.provisional=true")
    if _json_bool(ensemble_flags.get("provisional")):
        reasons.append("ensemble_manifest.status_flags.provisional=true")
    if _json_bool(forecast_flags.get("rerun_required")):
        reasons.append("forecast_manifest.status_flags.rerun_required=true")
    if _json_bool(ensemble_flags.get("rerun_required")):
        reasons.append("ensemble_manifest.status_flags.rerun_required=true")
    if _json_bool(recipe_selection.get("provisional")):
        reasons.append("forecast_manifest.recipe_selection.provisional=true")
    if _json_bool(baseline_provenance.get("provisional")):
        reasons.append("ensemble_manifest.baseline_provenance.provisional=true")

    rerun_required = _json_bool(forecast_flags.get("rerun_required")) or _json_bool(ensemble_flags.get("rerun_required"))
    provisional = rerun_required or bool(reasons)
    valid = not provisional
    status_flag = "rerun_required" if rerun_required else ("provisional" if provisional else "valid")

    return {
        "status_flag": status_flag,
        "valid": valid,
        "provisional": provisional,
        "rerun_required": rerun_required,
        "reasons": reasons,
    }


def resolve_official_phase3b_pairs(
    forecast_manifest: dict,
    ensemble_manifest: dict,
    case_output_dir: str | Path,
    observation_path: str | Path,
    validation_time_utc: str,
) -> list[dict]:
    case_output_dir = Path(case_output_dir)
    obs_path = Path(observation_path)
    if not obs_path.exists():
        raise FileNotFoundError(f"Official observation mask not found: {obs_path}")

    if str(forecast_manifest.get("manifest_type")) != "official_phase2_forecast":
        raise RuntimeError("Official Phase 3B requires forecast_manifest.json with manifest_type='official_phase2_forecast'.")
    if str(ensemble_manifest.get("manifest_type")) != "official_phase2_ensemble":
        raise RuntimeError("Official Phase 3B requires ensemble_manifest.json with manifest_type='official_phase2_ensemble'.")

    canonical_products = forecast_manifest.get("canonical_products") or {}
    required_product_keys = ["mask_p50_datecomposite", "control_footprint_mask"]
    missing_keys = [key for key in required_product_keys if not canonical_products.get(key)]
    if missing_keys:
        raise RuntimeError(
            "Official Phase 3B requires canonical products in forecast_manifest.json. "
            f"Missing keys: {', '.join(missing_keys)}"
        )

    validation_date = str(pd.Timestamp(validation_time_utc).date())
    ensemble_products = ensemble_manifest.get("products") or []
    deterministic_products = (forecast_manifest.get("deterministic_control") or {}).get("products") or []

    datecomposite_product = _find_manifest_product(
        ensemble_products,
        product_type="mask_p50_datecomposite",
        date_utc=validation_date,
    )
    control_product = _find_manifest_product(
        deterministic_products,
        product_type="control_footprint_mask",
        timestamp_utc=validation_time_utc,
    )

    primary_forecast_path = _resolve_artifact_path(str(canonical_products["mask_p50_datecomposite"]), case_output_dir)
    sensitivity_forecast_path = _resolve_artifact_path(str(canonical_products["control_footprint_mask"]), case_output_dir)
    resolved_datecomposite_path = _resolve_artifact_path(str(datecomposite_product.get("relative_path", "")), case_output_dir)
    resolved_control_path = _resolve_artifact_path(str(control_product.get("relative_path", "")), case_output_dir)

    if primary_forecast_path != resolved_datecomposite_path:
        raise RuntimeError(
            "Official Phase 3B manifest mismatch for mask_p50_datecomposite. "
            f"canonical_products -> {primary_forecast_path}, ensemble products -> {resolved_datecomposite_path}"
        )
    if sensitivity_forecast_path != resolved_control_path:
        raise RuntimeError(
            "Official Phase 3B manifest mismatch for control_footprint_mask. "
            f"canonical_products -> {sensitivity_forecast_path}, deterministic products -> {resolved_control_path}"
        )

    for label, path in {
        "mask_p50_datecomposite": primary_forecast_path,
        "control_footprint_mask": sensitivity_forecast_path,
    }.items():
        if not path.exists():
            raise FileNotFoundError(f"Official Phase 3B expected {label} product but it is missing: {path}")

    if not primary_forecast_path.name.endswith("_datecomposite.tif"):
        raise RuntimeError(
            "Official Phase 3B expected the primary forecast product to be the March 6 date-composite P50 mask. "
            f"Got: {primary_forecast_path.name}"
        )

    return [
        {
            "pair_id": "official_primary_march6",
            "pair_role": "primary",
            "forecast_product_type": "mask_p50_datecomposite",
            "forecast_product": primary_forecast_path.name,
            "forecast_path": str(primary_forecast_path),
            "forecast_semantics": str(datecomposite_product.get("semantics", "")),
            "observation_product_type": "obs_mask",
            "observation_product": obs_path.name,
            "observation_path": str(obs_path),
            "metric": "FSS",
            "windows_km": ",".join(str(value) for value in OFFICIAL_PHASE3B_WINDOWS_KM),
            "pairing_time_utc": validation_time_utc,
            "pairing_date_utc": validation_date,
            "source_semantics": OFFICIAL_PRIMARY_SOURCE_SEMANTICS,
        },
        {
            "pair_id": "official_sensitivity_control_march6",
            "pair_role": "sensitivity",
            "forecast_product_type": "control_footprint_mask",
            "forecast_product": sensitivity_forecast_path.name,
            "forecast_path": str(sensitivity_forecast_path),
            "forecast_semantics": str(control_product.get("semantics", "")),
            "observation_product_type": "obs_mask",
            "observation_product": obs_path.name,
            "observation_path": str(obs_path),
            "metric": "FSS",
            "windows_km": ",".join(str(value) for value in OFFICIAL_PHASE3B_WINDOWS_KM),
            "pairing_time_utc": validation_time_utc,
            "pairing_date_utc": validation_date,
            "source_semantics": OFFICIAL_SENSITIVITY_SOURCE_SEMANTICS,
        },
    ]


class Phase3BScoringService:
    def __init__(
        self,
        output_dir: str | Path | None = None,
        forecast_run_name: str | None = None,
        observation_run_name: str | None = None,
        run_context: dict | None = None,
    ):
        self.case = get_case_context()
        self.forecast_run_name = str(forecast_run_name or self.case.run_name)
        self.observation_run_name = str(observation_run_name or self.case.run_name)
        self.run_context = dict(run_context or {})
        default_output_dir = Path("output") / self.forecast_run_name / ("phase3b" if self.case.is_official else "validation")
        self.output_dir = Path(output_dir) if output_dir else default_output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.grid = GridBuilder()
        self.sea_mask = load_sea_mask_array(self.grid.spec) if self.case.is_official else None
        self.windows_km = list(OFFICIAL_PHASE3B_WINDOWS_KM)

    def run(self) -> Phase3BArtifacts:
        if self.case.is_official:
            return self._run_official(self.forecast_run_name, self.observation_run_name)
        return self._run_prototype(self.forecast_run_name)

    def _run_official(self, forecast_run_name: str, observation_run_name: str) -> Phase3BArtifacts:
        forecast_context = self._load_official_forecast_context(forecast_run_name)
        obs_registry = self._build_official_obs_registry(observation_run_name)
        pairing_manifest = self._build_official_pairing_manifest(obs_registry, forecast_context)
        pairing_manifest, fss_df, diagnostics_df = self._score_official_pairings(pairing_manifest)
        summary_df = self._summarize_official(pairing_manifest, fss_df, diagnostics_df)
        qa_artifacts = self._write_official_qa_artifacts(obs_registry, pairing_manifest)

        obs_registry_path = self.output_dir / "phase3b_obs_registry.csv"
        pairing_manifest_path = self.output_dir / "phase3b_pairing_manifest.csv"
        fss_path = self.output_dir / "phase3b_fss_by_date_window.csv"
        summary_path = self.output_dir / "phase3b_summary.csv"
        diagnostics_path = self.output_dir / "phase3b_diagnostics.csv"
        run_manifest_path = self.output_dir / "phase3b_run_manifest.json"

        obs_registry.to_csv(obs_registry_path, index=False)
        pairing_manifest.to_csv(pairing_manifest_path, index=False)
        fss_df.to_csv(fss_path, index=False)
        summary_df.to_csv(summary_path, index=False)
        diagnostics_df.to_csv(diagnostics_path, index=False)
        self._write_official_run_manifest(
            path=run_manifest_path,
            run_name=forecast_run_name,
            forecast_context=forecast_context,
            obs_registry_path=obs_registry_path,
            pairing_manifest_path=pairing_manifest_path,
            fss_path=fss_path,
            summary_path=summary_path,
            diagnostics_path=diagnostics_path,
            pairing_manifest=pairing_manifest,
            qa_artifacts=qa_artifacts,
            observation_run_name=observation_run_name,
        )

        return Phase3BArtifacts(
            obs_registry=obs_registry_path,
            pairing_manifest=pairing_manifest_path,
            fss_by_date_window=fss_path,
            summary=summary_path,
            diagnostics=diagnostics_path,
            run_manifest=run_manifest_path,
        )

    def _run_prototype(self, run_name: str) -> Phase3BArtifacts:
        obs_registry = self._build_prototype_obs_registry(run_name)
        pairing_manifest = self._build_prototype_pairing_manifest(obs_registry, run_name)
        fss_df = self._score_prototype_pairings(pairing_manifest)
        summary_df = self._summarize_prototype(fss_df)
        diagnostics_df = pd.DataFrame()

        obs_registry_path = self.output_dir / "phase3b_obs_registry.csv"
        pairing_manifest_path = self.output_dir / "phase3b_pairing_manifest.csv"
        fss_path = self.output_dir / "phase3b_fss_by_date_window.csv"
        summary_path = self.output_dir / "phase3b_summary.csv"
        diagnostics_path = self.output_dir / "phase3b_diagnostics.csv"
        run_manifest_path = self.output_dir / "phase3b_run_manifest.json"

        obs_registry.to_csv(obs_registry_path, index=False)
        pairing_manifest.to_csv(pairing_manifest_path, index=False)
        fss_df.to_csv(fss_path, index=False)
        summary_df.to_csv(summary_path, index=False)
        diagnostics_df.to_csv(diagnostics_path, index=False)
        self._write_prototype_run_manifest(run_manifest_path, run_name, pairing_manifest)

        return Phase3BArtifacts(
            obs_registry=obs_registry_path,
            pairing_manifest=pairing_manifest_path,
            fss_by_date_window=fss_path,
            summary=summary_path,
            diagnostics=diagnostics_path,
            run_manifest=run_manifest_path,
        )

    def _load_official_forecast_context(self, run_name: str) -> dict:
        case_output_dir = Path("output") / run_name
        forecast_manifest_path = get_forecast_manifest_path(run_name)
        ensemble_manifest_path = get_ensemble_manifest_path(run_name)
        forecast_manifest = _load_json(forecast_manifest_path, "Forecast manifest")
        ensemble_manifest = _load_json(ensemble_manifest_path, "Ensemble manifest")
        validation_time_utc = str(self.case.validation_layer.event_time_utc or self.case.simulation_end_utc)
        phase3b_status = derive_phase3b_status_from_upstream(forecast_manifest, ensemble_manifest)

        recipe_selection = forecast_manifest.get("recipe_selection") or {}
        transport = forecast_manifest.get("transport") or ensemble_manifest.get("transport") or {}
        grid = forecast_manifest.get("grid") or ensemble_manifest.get("grid") or {}
        deterministic_control = forecast_manifest.get("deterministic_control") or {}
        ensemble_config = ensemble_manifest.get("ensemble_configuration") or {}
        historical_baseline = (
            forecast_manifest.get("historical_baseline_provenance")
            or ensemble_manifest.get("historical_baseline_provenance")
            or {}
        )
        sensitivity_context = (
            forecast_manifest.get("sensitivity_context")
            or ensemble_manifest.get("sensitivity_context")
            or {}
        )
        active_recipe_id = (
            recipe_selection.get("recipe")
            or (forecast_manifest.get("recipe_provenance") or {}).get("recipe")
            or (ensemble_manifest.get("recipe_provenance") or {}).get("recipe")
            or ""
        )
        baseline_recipe_id = (
            historical_baseline.get("recipe")
            or (forecast_manifest.get("baseline_provenance") or {}).get("recipe")
            or (ensemble_manifest.get("baseline_provenance") or {}).get("recipe")
            or active_recipe_id
        )

        return {
            "case_output_dir": case_output_dir,
            "forecast_manifest_path": forecast_manifest_path,
            "ensemble_manifest_path": ensemble_manifest_path,
            "forecast_manifest": forecast_manifest,
            "ensemble_manifest": ensemble_manifest,
            "validation_time_utc": validation_time_utc,
            "phase3b_status": phase3b_status,
            "recipe_selection": recipe_selection,
            "active_recipe_id": active_recipe_id,
            "baseline_recipe_id": baseline_recipe_id,
            "historical_baseline_provenance": historical_baseline,
            "sensitivity_context": sensitivity_context,
            "transport_model": str(transport.get("model", "")),
            "provisional_transport_model": _json_bool(transport.get("provisional_transport_model")),
            "grid_id": str(grid.get("grid_id", "")),
            "grid_metadata_path": str(grid.get("metadata_path", "")),
            "grid_metadata_json_path": str(grid.get("metadata_json_path", "")),
            "deterministic_element_count": deterministic_control.get("actual_element_count"),
            "ensemble_element_count": ensemble_config.get("element_count"),
        }

    def _build_official_obs_registry(self, run_name: str) -> pd.DataFrame:
        registry_path = Path(f"data/arcgis/{run_name}/arcgis_registry.csv")
        if not registry_path.exists():
            raise FileNotFoundError(f"ArcGIS registry not found: {registry_path}")

        registry_df = pd.read_csv(registry_path)
        required_layers = [
            {
                "layer_id": int(self.case.initialization_layer.layer_id),
                "role": self.case.initialization_layer.role,
                "usage_note": "Initialization only; not scored in official Phase 3B.",
                "score_target": False,
                "mask_required": False,
            },
            {
                "layer_id": int(self.case.validation_layer.layer_id),
                "role": self.case.validation_layer.role,
                "usage_note": "Accepted March 6 validation target for official Phase 3B.",
                "score_target": True,
                "mask_required": True,
            },
            {
                "layer_id": int(self.case.provenance_layer.layer_id),
                "role": self.case.provenance_layer.role,
                "usage_note": "Provenance only; not scored in official Phase 3B.",
                "score_target": False,
                "mask_required": False,
            },
        ]

        records = []
        for requirement in required_layers:
            matches = registry_df[registry_df["layer_id"].astype(int) == requirement["layer_id"]]
            if "role" in registry_df.columns:
                role_matches = registry_df[registry_df["role"].astype(str) == requirement["role"]]
                if not role_matches.empty:
                    matches = role_matches
            if matches.empty:
                raise RuntimeError(
                    "Official Phase 3B requires ArcGIS registry entries for the canonical Mindoro layers. "
                    f"Missing role={requirement['role']} layer_id={requirement['layer_id']} in {registry_path}"
                )
            row = matches.iloc[0]
            mask_path_raw = str(row.get("mask") or "").strip()
            mask_path = Path(mask_path_raw) if mask_path_raw else None
            if requirement["mask_required"]:
                if mask_path is None:
                    raise RuntimeError(
                        f"Official Phase 3B requires a rasterized observation mask for role={requirement['role']}."
                    )
                if not mask_path.exists():
                    raise FileNotFoundError(f"Official Phase 3B observation mask missing: {mask_path}")

            mask_nonzero_cells = ""
            if mask_path is not None and mask_path.exists():
                mask_nonzero_cells = int(np.count_nonzero(self._to_binary_mask(self._read_mask(mask_path))))

            records.append(
                {
                    "layer_id": int(row["layer_id"]),
                    "role": str(row.get("role") or requirement["role"]),
                    "name": str(row["name"]),
                    "event_time_utc": str(row.get("event_time_utc") or ""),
                    "feature_count": int(row.get("feature_count") or 0),
                    "raw_geojson": str(row.get("raw_geojson") or ""),
                    "processed_vector": str(row.get("processed_vector") or ""),
                    "service_metadata": str(row.get("service_metadata") or ""),
                    "mask_path": str(mask_path) if mask_path is not None else "",
                    "mask_exists": bool(mask_path is not None and mask_path.exists()),
                    "mask_nonzero_cells": mask_nonzero_cells,
                    "accepted_official_role": True,
                    "score_target": requirement["score_target"],
                    "observation_product": mask_path.name if mask_path is not None else "",
                    "usage_note": requirement["usage_note"],
                }
            )
        return pd.DataFrame(records)

    def _build_official_pairing_manifest(self, obs_registry: pd.DataFrame, forecast_context: dict) -> pd.DataFrame:
        validation_rows = obs_registry[obs_registry["score_target"] == True]
        if validation_rows.empty:
            raise RuntimeError("Official Phase 3B requires a single March 6 validation observation target.")
        validation_row = validation_rows.iloc[0]

        official_pairs = resolve_official_phase3b_pairs(
            forecast_manifest=forecast_context["forecast_manifest"],
            ensemble_manifest=forecast_context["ensemble_manifest"],
            case_output_dir=forecast_context["case_output_dir"],
            observation_path=validation_row["mask_path"],
            validation_time_utc=forecast_context["validation_time_utc"],
        )

        forecast_flags = (forecast_context["forecast_manifest"].get("status_flags") or {}).copy()
        ensemble_flags = (forecast_context["ensemble_manifest"].get("status_flags") or {}).copy()
        phase3b_status = forecast_context["phase3b_status"]
        records = []
        for pair in official_pairs:
            pair_element_count = (
                forecast_context["ensemble_element_count"]
                if pair["pair_role"] == "primary"
                else forecast_context["deterministic_element_count"]
            )
            precheck_base = (
                self.output_dir / "precheck_phase3b_march6"
                if pair["pair_role"] == "primary"
                else self.output_dir / "precheck_phase3b_march6_control"
            )
            records.append(
                {
                    **pair,
                    "forecast_manifest_path": str(forecast_context["forecast_manifest_path"]),
                    "ensemble_manifest_path": str(forecast_context["ensemble_manifest_path"]),
                    "transport_model": forecast_context["transport_model"],
                    "provisional_transport_model": forecast_context["provisional_transport_model"],
                    "forecast_status_valid": _json_bool(forecast_flags.get("valid")),
                    "forecast_status_provisional": _json_bool(forecast_flags.get("provisional")),
                    "forecast_status_rerun_required": _json_bool(forecast_flags.get("rerun_required")),
                    "ensemble_status_valid": _json_bool(ensemble_flags.get("valid")),
                    "ensemble_status_provisional": _json_bool(ensemble_flags.get("provisional")),
                    "ensemble_status_rerun_required": _json_bool(ensemble_flags.get("rerun_required")),
                    "phase3b_status_flag": phase3b_status["status_flag"],
                    "phase3b_valid": phase3b_status["valid"],
                    "phase3b_provisional": phase3b_status["provisional"],
                    "phase3b_rerun_required": phase3b_status["rerun_required"],
                    "phase3b_status_reason": " | ".join(phase3b_status["reasons"]),
                    "active_recipe_id": forecast_context["active_recipe_id"],
                    "baseline_recipe_id": forecast_context["baseline_recipe_id"],
                    "historical_baseline_recipe_id": forecast_context["baseline_recipe_id"],
                    "grid_id": forecast_context["grid_id"],
                    "grid_metadata_path": forecast_context["grid_metadata_path"],
                    "grid_metadata_json_path": forecast_context["grid_metadata_json_path"],
                    "pair_element_count": pair_element_count,
                    "deterministic_element_count": forecast_context["deterministic_element_count"],
                    "ensemble_element_count": forecast_context["ensemble_element_count"],
                    "observation_layer_id": int(validation_row["layer_id"]),
                    "observation_role": str(validation_row["role"]),
                    "observation_usage_note": str(validation_row["usage_note"]),
                    "precheck_expected_csv": str(precheck_base.with_suffix(".csv")),
                    "precheck_expected_json": str(precheck_base.with_suffix(".json")),
                    "precheck_csv": "",
                    "precheck_json": "",
                }
            )
        return pd.DataFrame(records)

    def _score_official_pairings(
        self,
        pairing_manifest: pd.DataFrame,
    ) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        updated_pairings = []
        diagnostics_rows = []
        fss_rows = []

        for _, row in pairing_manifest.iterrows():
            forecast_path = Path(row["forecast_path"])
            observation_path = Path(row["observation_path"])
            precheck_base = Path(row["precheck_expected_csv"]).with_suffix("")
            precheck = precheck_same_grid(
                forecast=forecast_path,
                target=observation_path,
                report_base_path=precheck_base,
            )
            if not precheck.passed:
                raise RuntimeError(
                    "Official Phase 3B same-grid precheck failed. "
                    f"Forecast: {forecast_path} | Target: {observation_path} | "
                    f"CSV: {precheck.csv_report_path} | JSON: {precheck.json_report_path}"
                )

            updated_row = row.to_dict()
            updated_row["precheck_csv"] = str(precheck.csv_report_path)
            updated_row["precheck_json"] = str(precheck.json_report_path)
            updated_pairings.append(updated_row)

            forecast_mask = self._load_binary_score_mask(forecast_path)
            obs_mask = self._load_binary_score_mask(observation_path)
            diagnostics = self._compute_mask_diagnostics(forecast_mask, obs_mask)
            diagnostics_rows.append(
                {
                    **{key: updated_row[key] for key in updated_row if key not in {"precheck_expected_csv", "precheck_expected_json"}},
                    **diagnostics,
                }
            )

            for window_km in self.windows_km:
                window_cells = self._window_km_to_cells(window_km)
                fss = float(
                    np.clip(
                        calculate_fss(
                            forecast_mask,
                            obs_mask,
                            window=window_cells,
                            valid_mask=(self.sea_mask > 0.5) if self.sea_mask is not None else None,
                        ),
                        0.0,
                        1.0,
                    )
                )
                fss_rows.append(
                    {
                        "pair_id": updated_row["pair_id"],
                        "pair_role": updated_row["pair_role"],
                        "metric": updated_row["metric"],
                        "window_km": int(window_km),
                        "window_cells": int(window_cells),
                        "fss": fss,
                        "forecast_product": updated_row["forecast_product"],
                        "forecast_product_type": updated_row["forecast_product_type"],
                        "forecast_path": updated_row["forecast_path"],
                        "observation_product": updated_row["observation_product"],
                        "observation_product_type": updated_row["observation_product_type"],
                        "observation_path": updated_row["observation_path"],
                        "pairing_time_utc": updated_row["pairing_time_utc"],
                        "pairing_date_utc": updated_row["pairing_date_utc"],
                        "source_semantics": updated_row["source_semantics"],
                        "baseline_recipe_id": updated_row["baseline_recipe_id"],
                        "transport_model": updated_row["transport_model"],
                        "provisional_transport_model": updated_row["provisional_transport_model"],
                        "phase3b_status_flag": updated_row["phase3b_status_flag"],
                        "phase3b_valid": updated_row["phase3b_valid"],
                        "phase3b_provisional": updated_row["phase3b_provisional"],
                        "phase3b_rerun_required": updated_row["phase3b_rerun_required"],
                        "phase3b_status_reason": updated_row["phase3b_status_reason"],
                        "pair_element_count": updated_row["pair_element_count"],
                        "grid_id": updated_row["grid_id"],
                        "precheck_csv": updated_row["precheck_csv"],
                        "precheck_json": updated_row["precheck_json"],
                    }
                )

        return pd.DataFrame(updated_pairings), pd.DataFrame(fss_rows), pd.DataFrame(diagnostics_rows)

    def _summarize_official(
        self,
        pairing_manifest: pd.DataFrame,
        fss_df: pd.DataFrame,
        diagnostics_df: pd.DataFrame,
    ) -> pd.DataFrame:
        fss_pivot = (
            fss_df.pivot(index="pair_id", columns="window_km", values="fss")
            .rename(columns={window: f"fss_{window}km" for window in self.windows_km})
            .reset_index()
        )
        summary = pairing_manifest.merge(
            diagnostics_df[
                [
                    "pair_id",
                    "forecast_nonzero_cells",
                    "obs_nonzero_cells",
                    "area_ratio_forecast_to_obs",
                    "centroid_distance_m",
                    "iou",
                    "dice",
                    "nearest_distance_to_obs_m",
                    "ocean_cell_count",
                ]
            ],
            on="pair_id",
            how="left",
        ).merge(fss_pivot, on="pair_id", how="left")
        return summary

    def _write_official_run_manifest(
        self,
        path: Path,
        run_name: str,
        forecast_context: dict,
        obs_registry_path: Path,
        pairing_manifest_path: Path,
        fss_path: Path,
        summary_path: Path,
        diagnostics_path: Path,
        pairing_manifest: pd.DataFrame,
        qa_artifacts: list[Path],
        observation_run_name: str,
    ) -> None:
        payload = {
            "run_name": run_name,
            "observation_run_name": observation_run_name,
            "workflow_mode": self.case.workflow_mode,
            "output_dir": str(self.output_dir),
            "mode": "official_phase3b_scoring",
            "phase3b_status_flags": forecast_context["phase3b_status"],
            "run_context": self.run_context,
            "upstream_forecast": {
                "forecast_manifest_path": str(forecast_context["forecast_manifest_path"]),
                "ensemble_manifest_path": str(forecast_context["ensemble_manifest_path"]),
                "active_recipe_id": forecast_context["active_recipe_id"],
                "transport_model": forecast_context["transport_model"],
                "provisional_transport_model": forecast_context["provisional_transport_model"],
                "baseline_recipe_id": forecast_context["baseline_recipe_id"],
                "historical_baseline_provenance": forecast_context["historical_baseline_provenance"],
                "sensitivity_context": forecast_context["sensitivity_context"],
                "grid_id": forecast_context["grid_id"],
                "grid_metadata_path": forecast_context["grid_metadata_path"],
                "grid_metadata_json_path": forecast_context["grid_metadata_json_path"],
                "deterministic_element_count": forecast_context["deterministic_element_count"],
                "ensemble_element_count": forecast_context["ensemble_element_count"],
            },
            "artifacts": {
                "obs_registry": str(obs_registry_path),
                "pairing_manifest": str(pairing_manifest_path),
                "fss_by_date_window": str(fss_path),
                "summary": str(summary_path),
                "diagnostics": str(diagnostics_path),
            },
            "pairings": pairing_manifest.to_dict(orient="records"),
            "qa_artifacts": [str(path_obj) for path_obj in qa_artifacts if path_obj.exists()],
        }
        _write_json(path, payload)

    def _write_official_qa_artifacts(self, obs_registry: pd.DataFrame, pairing_manifest: pd.DataFrame) -> list[Path]:
        written: list[Path] = []
        if plt is None:
            return written

        primary_rows = pairing_manifest[pairing_manifest["pair_role"] == "primary"]
        if not primary_rows.empty:
            primary = primary_rows.iloc[0]
            out_path = self.output_dir / "qa_phase3b_obsmask_vs_p50.png"
            try:
                forecast_mask = self._load_binary_score_mask(Path(primary["forecast_path"]))
                obs_mask = self._load_binary_score_mask(Path(primary["observation_path"]))
                self._write_mask_overlay_png(forecast_mask, obs_mask, out_path)
                written.append(out_path)
            except Exception:
                pass

        if gpd is not None and plt is not None:
            out_path = self.output_dir / "qa_phase3b_source_init_validation_overlay.png"
            try:
                init_path = Path(str(obs_registry.loc[obs_registry["role"] == self.case.initialization_layer.role, "processed_vector"].iloc[0]))
                validation_path = Path(str(obs_registry.loc[obs_registry["role"] == self.case.validation_layer.role, "processed_vector"].iloc[0]))
                source_path = Path(str(obs_registry.loc[obs_registry["role"] == self.case.provenance_layer.role, "processed_vector"].iloc[0]))
                init_gdf = gpd.read_file(init_path)
                validation_gdf = gpd.read_file(validation_path)
                source_gdf = gpd.read_file(source_path)
                if init_gdf.crs and validation_gdf.crs and str(init_gdf.crs) != str(validation_gdf.crs):
                    validation_gdf = validation_gdf.to_crs(init_gdf.crs)
                if init_gdf.crs and source_gdf.crs and str(init_gdf.crs) != str(source_gdf.crs):
                    source_gdf = source_gdf.to_crs(init_gdf.crs)

                fig, ax = plt.subplots(figsize=(8, 8))
                init_gdf.plot(ax=ax, facecolor="none", edgecolor="#1f77b4", linewidth=1.5, label="March 3 init")
                validation_gdf.plot(ax=ax, facecolor="none", edgecolor="#d62728", linewidth=1.5, label="March 6 validation")
                source_gdf.plot(ax=ax, color="#111111", markersize=24, label="Source point")
                ax.set_title("Mindoro initialization, validation, and source layers")
                ax.set_axis_off()
                fig.savefig(out_path, dpi=150, bbox_inches="tight")
                plt.close(fig)
                written.append(out_path)
            except Exception:
                pass

        return written

    def _write_mask_overlay_png(self, forecast_mask: np.ndarray, obs_mask: np.ndarray, out_path: Path) -> None:
        overlap = np.logical_and(forecast_mask > 0, obs_mask > 0)
        canvas = np.ones((forecast_mask.shape[0], forecast_mask.shape[1], 3), dtype=np.float32)
        canvas[obs_mask > 0] = np.array([0.2, 0.45, 0.95], dtype=np.float32)
        canvas[forecast_mask > 0] = np.array([0.95, 0.35, 0.2], dtype=np.float32)
        canvas[overlap] = np.array([0.55, 0.2, 0.75], dtype=np.float32)

        fig, ax = plt.subplots(figsize=(8, 8))
        ax.imshow(canvas, origin="upper")
        ax.set_title("Phase 3B March 6 observed mask vs ensemble P50 date composite")
        ax.set_axis_off()
        fig.savefig(out_path, dpi=150, bbox_inches="tight")
        plt.close(fig)

    def _read_mask(self, path: Path) -> np.ndarray:
        import rasterio

        with rasterio.open(path) as src:
            return src.read(1).astype(np.float32)

    def _apply_score_mask(self, data: np.ndarray) -> np.ndarray:
        masked = np.nan_to_num(np.asarray(data, dtype=np.float32), nan=0.0, posinf=0.0, neginf=0.0)
        if self.sea_mask is not None:
            masked = apply_ocean_mask(masked, sea_mask=self.sea_mask, fill_value=0.0)
        return masked.astype(np.float32)

    def _load_binary_score_mask(self, path: Path) -> np.ndarray:
        return self._to_binary_mask(self._apply_score_mask(self._read_mask(path)))

    @staticmethod
    def _to_binary_mask(data: np.ndarray) -> np.ndarray:
        return (np.nan_to_num(np.asarray(data, dtype=np.float32), nan=0.0, posinf=0.0, neginf=0.0) > 0.0).astype(np.float32)

    def _window_km_to_cells(self, window_km: int) -> int:
        if self.grid.is_projected:
            return max(1, int(round((float(window_km) * 1000.0) / float(self.grid.resolution))))
        return max(1, int(window_km))

    def _mask_cell_centers(self, mask: np.ndarray) -> np.ndarray:
        active = np.argwhere(mask > 0)
        if active.size == 0:
            return np.empty((0, 2), dtype=np.float64)
        rows = active[:, 0]
        cols = active[:, 1]
        xs = self.grid.min_x + ((cols + 0.5) * self.grid.resolution)
        ys = self.grid.max_y - ((rows + 0.5) * self.grid.resolution)
        return np.column_stack([xs, ys]).astype(np.float64)

    def _compute_mask_diagnostics(self, forecast_mask: np.ndarray, obs_mask: np.ndarray) -> dict:
        forecast_nonzero = int(np.count_nonzero(forecast_mask))
        obs_nonzero = int(np.count_nonzero(obs_mask))
        intersection = int(np.count_nonzero((forecast_mask > 0) & (obs_mask > 0)))
        union = int(np.count_nonzero((forecast_mask > 0) | (obs_mask > 0)))

        if obs_nonzero == 0:
            area_ratio = np.nan
        else:
            area_ratio = float(forecast_nonzero / obs_nonzero)

        iou = float(intersection / union) if union > 0 else 1.0
        denom = forecast_nonzero + obs_nonzero
        dice = float((2.0 * intersection) / denom) if denom > 0 else 1.0

        forecast_points = self._mask_cell_centers(forecast_mask)
        obs_points = self._mask_cell_centers(obs_mask)

        centroid_distance_m = np.nan
        if len(forecast_points) > 0 and len(obs_points) > 0:
            centroid_distance_m = float(np.linalg.norm(forecast_points.mean(axis=0) - obs_points.mean(axis=0)))

        nearest_distance_to_obs_m = np.nan
        if intersection > 0:
            nearest_distance_to_obs_m = 0.0
        elif len(forecast_points) > 0 and len(obs_points) > 0:
            if cKDTree is not None:
                distances, _ = cKDTree(obs_points).query(forecast_points, k=1)
                nearest_distance_to_obs_m = float(np.min(distances))
            else:  # pragma: no cover - scipy is available in runtime/tests
                deltas = forecast_points[:, None, :] - obs_points[None, :, :]
                nearest_distance_to_obs_m = float(np.sqrt(np.sum(deltas * deltas, axis=2)).min())

        return {
            "forecast_nonzero_cells": forecast_nonzero,
            "obs_nonzero_cells": obs_nonzero,
            "area_ratio_forecast_to_obs": area_ratio,
            "centroid_distance_m": centroid_distance_m,
            "iou": iou,
            "dice": dice,
            "nearest_distance_to_obs_m": nearest_distance_to_obs_m,
            "ocean_cell_count": int(np.count_nonzero(self.sea_mask > 0.5)) if self.sea_mask is not None else int(forecast_mask.size),
        }

    def _build_prototype_obs_registry(self, run_name: str) -> pd.DataFrame:
        registry_path = Path(f"data/arcgis/{run_name}/arcgis_registry.csv")
        if not registry_path.exists():
            raise FileNotFoundError(f"ArcGIS registry not found: {registry_path}")
        df = pd.read_csv(registry_path)
        records = []
        for _, row in df.iterrows():
            row_role = row.get("role")
            row_layer_id = int(row["layer_id"])
            if row_role:
                is_validation_layer = str(row_role) == self.case.validation_layer.role
            else:
                is_validation_layer = row_layer_id == self.case.validation_layer.layer_id
            if not is_validation_layer:
                continue

            mask_path = Path(row["mask"])
            if not mask_path.exists():
                raise FileNotFoundError(f"ArcGIS mask not found: {mask_path}")

            records.append(
                {
                    "layer_id": row_layer_id,
                    "name": row["name"],
                    "feature_count": int(row["feature_count"]),
                    "mask_path": str(mask_path),
                    "mask_sum": float(self._read_mask(mask_path).sum()),
                    "validation_time_utc": row.get("event_time_utc", self.case.validation_layer.event_time_utc or ""),
                }
            )
        return pd.DataFrame(records)

    def _build_prototype_pairing_manifest(self, obs_registry: pd.DataFrame, run_name: str) -> pd.DataFrame:
        case_dir = Path("output") / run_name
        if not case_dir.exists():
            raise FileNotFoundError(f"Expected case directory not found: {case_dir}")

        candidate_outputs = []
        selection = resolve_recipe_selection(case_dir / "validation" / "validation_ranking.csv")
        forecast_manifest_path = get_forecast_manifest_path(run_name)
        forecast_manifest = {}
        if self.case.is_official and forecast_manifest_path.exists():
            with open(forecast_manifest_path, "r", encoding="utf-8") as f:
                forecast_manifest = json.load(f) or {}

        for candidate in get_phase3b_forecast_candidates(selection.recipe, run_name):
            candidate_path = Path(candidate["path"])
            if candidate_path.exists():
                candidate_outputs.append(
                    {
                        "type": candidate["type"],
                        "path": str(candidate_path),
                    }
                )

        if not candidate_outputs:
            raise FileNotFoundError(
                f"No Phase 3B forecast outputs are available under {case_dir}. "
                f"Expected ensemble probability or deterministic spill forecast artifacts."
            )

        records = []
        for _, row in obs_registry.iterrows():
            for output in candidate_outputs:
                records.append(
                    {
                        "layer_id": row["layer_id"],
                        "name": row["name"],
                        "mask_path": row["mask_path"],
                        "output_case": run_name,
                        "output_type": output["type"],
                        "output_path": output["path"],
                        "pairing_time_utc": row.get("validation_time_utc", ""),
                        "recipe": selection.recipe,
                        "selection_source_kind": selection.source_kind,
                        "selection_source_path": selection.source_path or "",
                        "selection_status_flag": selection.status_flag,
                        "valid": selection.valid,
                        "provisional": selection.provisional,
                        "rerun_required": selection.rerun_required,
                        "selection_note": selection.note,
                        "forecast_manifest_path": str(forecast_manifest_path) if self.case.is_official else "",
                        "forecast_manifest_status_flag": str(
                            ((forecast_manifest.get("recipe_selection") or {}).get("status_flag")) if forecast_manifest else ""
                        ),
                    }
                )
        return pd.DataFrame(records)

    def _score_prototype_pairings(self, pairing_manifest: pd.DataFrame) -> pd.DataFrame:
        rows = []
        for _, row in pairing_manifest.iterrows():
            obs_mask = self._read_mask(Path(row["mask_path"]))
            fc_mask = self._load_forecast_mask(Path(row["output_path"]))
            for window_km in self.windows_km:
                fss = calculate_fss(fc_mask, obs_mask, window=max(1, int(window_km)))
                rows.append(
                    {
                        "layer_id": row["layer_id"],
                        "name": row["name"],
                        "output_case": row["output_case"],
                        "output_type": row["output_type"],
                        "pairing_time_utc": row["pairing_time_utc"],
                        "recipe": row["recipe"],
                        "selection_source_kind": row["selection_source_kind"],
                        "selection_status_flag": row["selection_status_flag"],
                        "valid": row["valid"],
                        "provisional": row["provisional"],
                        "rerun_required": row["rerun_required"],
                        "window_km": window_km,
                        "fss": float(np.clip(fss, 0.0, 1.0)),
                    }
                )
        return pd.DataFrame(rows)

    def _summarize_prototype(self, fss_df: pd.DataFrame) -> pd.DataFrame:
        if fss_df.empty:
            return pd.DataFrame()
        summary = (
            fss_df.groupby(["name", "output_type", "window_km"], as_index=False)
            .agg(mean_fss=("fss", "mean"), min_fss=("fss", "min"), max_fss=("fss", "max"))
        )
        meta_cols = [
            "name",
            "output_type",
            "recipe",
            "selection_source_kind",
            "selection_status_flag",
            "valid",
            "provisional",
            "rerun_required",
        ]
        meta = fss_df[meta_cols].drop_duplicates(subset=["name", "output_type"])
        return summary.merge(meta, on=["name", "output_type"], how="left")

    def _write_prototype_run_manifest(self, path: Path, run_name: str, pairing_manifest: pd.DataFrame) -> None:
        records = []
        for _, row in pairing_manifest.drop_duplicates(subset=["output_type", "output_path"]).iterrows():
            records.append(
                {
                    "output_type": row["output_type"],
                    "output_path": row["output_path"],
                }
            )

        selection = {}
        if not pairing_manifest.empty:
            first = pairing_manifest.iloc[0]
            selection = {
                "recipe": first["recipe"],
                "source_kind": first["selection_source_kind"],
                "source_path": first.get("selection_source_path", ""),
                "status_flag": first["selection_status_flag"],
                "valid": bool(first["valid"]),
                "provisional": bool(first["provisional"]),
                "rerun_required": bool(first["rerun_required"]),
                "note": first.get("selection_note", ""),
            }

        payload = {
            "run_name": run_name,
            "workflow_mode": self.case.workflow_mode,
            "recipe_selection": selection,
            "candidate_outputs": records,
            "status_flags": {
                "valid": bool(selection.get("valid", False)),
                "provisional": bool(selection.get("provisional", False)),
                "rerun_required": bool(selection.get("rerun_required", False)),
            },
        }
        _write_json(path, payload)

    def _load_forecast_mask(self, path: Path) -> np.ndarray:
        import rasterio
        import xarray as xr

        if path.suffix.lower() == ".tif":
            with rasterio.open(path) as src:
                return src.read(1).astype(np.float32)
        if path.suffix.lower() == ".nc":
            with xr.open_dataset(path) as ds:
                if "probability" in ds.data_vars or "probability_density" in ds.data_vars:
                    var_name = "probability" if "probability" in ds.data_vars else "probability_density"
                    prob_data = ds[var_name].values
                    if prob_data.ndim == 3:
                        prob_data = prob_data[0]
                    return (prob_data > 0).astype(np.float32)

            from src.helpers.raster import extract_particles_at_hour, rasterize_particles

            lon, lat, mass = extract_particles_at_hour(path, 72, "opendrift")
            hits, _ = rasterize_particles(self.grid, lon, lat, mass)
            return hits.astype(np.float32)
        raise ValueError(f"Unsupported forecast output: {path}")


def run_phase3b_scoring(
    output_dir: str | Path | None = None,
    forecast_run_name: str | None = None,
    observation_run_name: str | None = None,
    run_context: dict | None = None,
):
    service = Phase3BScoringService(
        output_dir=output_dir,
        forecast_run_name=forecast_run_name,
        observation_run_name=observation_run_name,
        run_context=run_context,
    )
    return service.run()
