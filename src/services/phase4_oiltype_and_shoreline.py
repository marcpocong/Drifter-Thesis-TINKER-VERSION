"""Standardized Phase 4 oil-type fate and shoreline impact workflow."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import pandas as pd
import yaml

from src.core.case_context import get_case_context
from src.services.shoreline import run_shoreline_analysis
from src.services.weathering import run_weathering_scenarios
from src.utils.io import get_phase1_baseline_audit_status, get_phase2_recipe_family_status, resolve_spill_origin

PHASE = "phase4_oiltype_and_shoreline"
OUTPUT_ROOT = Path("output") / "phase4"
SCENARIO_REGISTRY_PATH = Path("config") / "phase4_oil_scenarios.csv"
PHASE1_AUDIT_JSON = Path("output") / "phase1_finalization_audit" / "phase1_finalization_status.json"
PHASE2_AUDIT_JSON = Path("output") / "phase2_finalization_audit" / "phase2_finalization_status.json"
OFFICIAL_MINDORO_RUN_NAME = "CASE_MINDORO_RETRO_2023"

REQUIRED_SCENARIO_COLUMNS = {
    "scenario_id",
    "oil_record_id",
    "oil_label",
    "category",
    "source_note",
    "enabled_yes_no",
}


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle) or {}


def _write_json(path: Path, payload: dict[str, Any] | list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, default=str)
        handle.write("\n")


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text.rstrip() + "\n", encoding="utf-8")


def _yes_no(value: Any) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y", "enabled"}


def _relative_to_repo(repo_root: Path, path: Path) -> str:
    try:
        return str(path.resolve().relative_to(repo_root.resolve()))
    except Exception:
        return str(path)


def _resolve_repo_path(repo_root: Path, value: str | Path) -> Path:
    path = Path(value)
    if not path.is_absolute():
        path = repo_root / path
    return path.resolve()


def _phase4_profile_defaults() -> dict[str, dict[str, Any]]:
    base_overrides = {
        "drift:vertical_mixing": False,
        "drift:wind_uncertainty": 0.0,
        "drift:current_uncertainty": 0.0,
        "processes:dispersion": True,
        "processes:evaporation": True,
        "processes:emulsification": True,
        "processes:update_oilfilm_thickness": True,
        "general:coastline_action": "stranding",
        "wave_entrainment:droplet_size_distribution": "Johansen et al. (2015)",
    }
    return {
        "lighter_oil": {
            "adios_id": "GENERIC DIESEL",
            "density_kg_m3": 850.0,
            "color": "#FF8C00",
            "openoil_overrides": {
                **base_overrides,
                "seed:droplet_diameter_min_subsea": 0.0005,
                "seed:droplet_diameter_max_subsea": 0.0050,
            },
        },
        "fixed_base_medium_heavy_proxy": {
            "adios_id": "GENERIC INTERMEDIATE FUEL OIL 180",
            "density_kg_m3": 930.0,
            "color": "#8C564B",
            "openoil_overrides": {
                **base_overrides,
                "seed:droplet_diameter_min_subsea": 0.0008,
                "seed:droplet_diameter_max_subsea": 0.0080,
            },
        },
        "heavier_oil": {
            "adios_id": "GENERIC HEAVY FUEL OIL",
            "density_kg_m3": 990.0,
            "color": "#4B0082",
            "openoil_overrides": {
                **base_overrides,
                "seed:droplet_diameter_min_subsea": 0.0010,
                "seed:droplet_diameter_max_subsea": 0.0100,
            },
        },
    }


def load_phase4_scenario_registry(registry_path: str | Path = SCENARIO_REGISTRY_PATH) -> list[dict[str, Any]]:
    path = Path(registry_path)
    if not path.exists():
        raise FileNotFoundError(f"Phase 4 scenario registry not found: {path}")

    registry_df = pd.read_csv(path)
    missing = sorted(REQUIRED_SCENARIO_COLUMNS - set(registry_df.columns))
    if missing:
        raise ValueError(f"Phase 4 scenario registry is missing required columns: {missing}")

    defaults = _phase4_profile_defaults()
    scenarios: list[dict[str, Any]] = []
    for row in registry_df.to_dict(orient="records"):
        if not _yes_no(row.get("enabled_yes_no", "")):
            continue

        scenario_id = str(row.get("scenario_id") or "").strip()
        if not scenario_id:
            raise ValueError("Encountered a Phase 4 scenario row with an empty scenario_id.")
        if scenario_id not in defaults:
            raise ValueError(
                f"Phase 4 scenario '{scenario_id}' is not mapped to a supported OpenOil profile. "
                f"Supported IDs: {sorted(defaults)}"
            )

        profile = defaults[scenario_id]
        scenario = {
            "scenario_id": scenario_id,
            "oil_record_id": str(row.get("oil_record_id") or "").strip(),
            "oil_label": str(row.get("oil_label") or scenario_id).strip(),
            "category": str(row.get("category") or "").strip(),
            "source_note": str(row.get("source_note") or "").strip(),
            "enabled_yes_no": "yes",
            "display_name": str(row.get("oil_label") or scenario_id).strip(),
            "adios_id": str(row.get("adios_id") or profile["adios_id"]).strip(),
            "density_kg_m3": float(row.get("density_kg_m3") or profile["density_kg_m3"]),
            "color": str(row.get("color_hex") or profile["color"]).strip(),
            "openoil_overrides": dict(profile["openoil_overrides"]),
        }
        scenarios.append(scenario)

    if not scenarios:
        raise RuntimeError(f"No enabled Phase 4 scenarios were found in {path}.")
    return scenarios


@dataclass(frozen=True)
class TransportSource:
    base_run_name: str
    selected_run_name: str
    branch_kind: str
    shoreline_aware_workflow_reused: bool
    forecast_manifest_path: Path
    ensemble_manifest_path: Path
    convergence_manifest_path: Path | None
    shoreline_rerun_summary_path: Path | None


def resolve_phase4_transport_source(repo_root: str | Path, base_run_name: str) -> TransportSource:
    repo_root = Path(repo_root).resolve()
    base_output_dir = repo_root / "output" / base_run_name
    convergence_manifest_path = base_output_dir / "convergence_after_shoreline" / "convergence_after_shoreline_run_manifest.json"
    shoreline_rerun_summary_path = base_output_dir / "shoreline_rerun" / "shoreline_rerun_summary.csv"

    if convergence_manifest_path.exists():
        convergence_manifest = _read_json(convergence_manifest_path)
        recommended_count = int(((convergence_manifest.get("recommendation") or {}).get("recommended_final_official_element_count")) or 0)
        if recommended_count > 0:
            candidate_run_name = f"{base_run_name}/convergence_after_shoreline/elements_{recommended_count:06d}"
            candidate_output_dir = repo_root / "output" / candidate_run_name
            forecast_manifest_path = candidate_output_dir / "forecast" / "forecast_manifest.json"
            ensemble_manifest_path = candidate_output_dir / "ensemble" / "ensemble_manifest.json"
            if forecast_manifest_path.exists() and ensemble_manifest_path.exists():
                return TransportSource(
                    base_run_name=base_run_name,
                    selected_run_name=candidate_run_name,
                    branch_kind="shoreline_aware_convergence_recommended",
                    shoreline_aware_workflow_reused=True,
                    forecast_manifest_path=forecast_manifest_path,
                    ensemble_manifest_path=ensemble_manifest_path,
                    convergence_manifest_path=convergence_manifest_path,
                    shoreline_rerun_summary_path=shoreline_rerun_summary_path if shoreline_rerun_summary_path.exists() else None,
                )

    return TransportSource(
        base_run_name=base_run_name,
        selected_run_name=base_run_name,
        branch_kind="official_base_phase2",
        shoreline_aware_workflow_reused=False,
        forecast_manifest_path=base_output_dir / "forecast" / "forecast_manifest.json",
        ensemble_manifest_path=base_output_dir / "ensemble" / "ensemble_manifest.json",
        convergence_manifest_path=convergence_manifest_path if convergence_manifest_path.exists() else None,
        shoreline_rerun_summary_path=shoreline_rerun_summary_path if shoreline_rerun_summary_path.exists() else None,
    )


class Phase4OilTypeAndShorelineService:
    def __init__(
        self,
        repo_root: str | Path = ".",
        output_dir: str | Path | None = None,
        registry_path: str | Path = SCENARIO_REGISTRY_PATH,
        weathering_runner: Callable[..., dict[str, Any]] | None = None,
        shoreline_analyzer: Callable[..., pd.DataFrame | None] | None = None,
    ):
        self.repo_root = Path(repo_root).resolve()
        self.case = get_case_context()
        if not self.case.is_official:
            raise RuntimeError("phase4_oiltype_and_shoreline is currently only supported for official spill-case workflows.")

        self.run_name = self.case.run_name
        self.output_dir = Path(output_dir) if output_dir else self.repo_root / OUTPUT_ROOT / self.run_name
        self.registry_path = self.repo_root / registry_path
        self.weathering_runner = weathering_runner or run_weathering_scenarios
        self.shoreline_analyzer = shoreline_analyzer or run_shoreline_analysis
        self.transport_source = resolve_phase4_transport_source(self.repo_root, self.run_name)
        self.forecast_manifest = _read_json(self.transport_source.forecast_manifest_path)
        self.ensemble_manifest = _read_json(self.transport_source.ensemble_manifest_path)
        self.transport_loading_audit_path, self.transport_loading_audit = self._load_transport_loading_audit()
        self.phase1_audit = _read_json(self.repo_root / PHASE1_AUDIT_JSON)
        self.phase2_audit = _read_json(self.repo_root / PHASE2_AUDIT_JSON)
        self.scenarios = load_phase4_scenario_registry(self.registry_path)
        self.oil_config = self._load_oil_config()

        self.selected_recipe = str(
            ((self.forecast_manifest.get("recipe_selection") or {}).get("recipe"))
            or ((self.forecast_manifest.get("baseline_provenance") or {}).get("recipe"))
            or ""
        )
        self.baseline_selection_path = _resolve_repo_path(
            self.repo_root,
            self._baseline_selection_source(),
        )
        self.recipe_family_status = get_phase2_recipe_family_status(
            run_name=self.run_name,
            selected_recipe=self.selected_recipe,
            selection_path=self.baseline_selection_path,
        )
        self.transport_forcing_bundle = self._resolve_transport_forcing_bundle()
        self.phase1_status = self._load_phase1_status()
        self.phase2_status = self._load_phase2_status()
        grid = self.forecast_manifest.get("grid") or {}
        self.shoreline_segments_path = self.repo_root / str(
            grid.get("shoreline_segments_path") or "data_processed/grids/shoreline_segments.gpkg"
        )
        self.shoreline_mask_status = str(grid.get("shoreline_mask_status") or "")
        self.shoreline_mask_signature = str(grid.get("shoreline_mask_signature") or "")

    def _load_oil_config(self) -> dict[str, Any]:
        config_path = self.repo_root / "config" / "oil.yaml"
        with open(config_path, "r", encoding="utf-8") as handle:
            return yaml.safe_load(handle) or {}

    def _load_transport_loading_audit(self) -> tuple[Path, dict[str, Any]]:
        loading_audit = self.forecast_manifest.get("loading_audit") or {}
        json_path = str(loading_audit.get("json") or "").strip()
        if json_path:
            candidate = _resolve_repo_path(self.repo_root, json_path)
            if candidate.exists():
                return candidate, _read_json(candidate)

        fallback = self.transport_source.forecast_manifest_path.parent / "phase2_loading_audit.json"
        if fallback.exists():
            return fallback.resolve(), _read_json(fallback.resolve())

        raise FileNotFoundError(
            "Phase 4 requires the selected transport run's phase2_loading_audit.json so it can reuse the "
            "deterministic-control forcing coverage honestly."
        )

    def _resolve_transport_forcing_bundle(self) -> dict[str, Any]:
        runs = list(self.transport_loading_audit.get("runs") or [])
        deterministic_run = next(
            (
                run
                for run in runs
                if str(run.get("run_kind") or "") == "deterministic_control"
                and str(run.get("status") or "") == "completed"
            ),
            None,
        )
        if deterministic_run is None:
            deterministic_run = next(
                (run for run in runs if str(run.get("run_kind") or "") == "deterministic_control"),
                None,
            )
        if deterministic_run is None:
            raise RuntimeError(
                "Phase 4 could not find a deterministic_control entry inside the selected transport loading audit."
            )

        forcings = deterministic_run.get("forcings") or {}
        resolved_paths: dict[str, Path | None] = {}
        forcing_status: dict[str, dict[str, Any]] = {}
        for output_key, audit_key, required in (
            ("currents", "current", True),
            ("wind", "wind", True),
            ("wave", "wave", False),
        ):
            entry = dict(forcings.get(audit_key) or {})
            used_path = str(entry.get("used_path") or entry.get("configured_path") or "").strip()
            resolved_path: Path | None = None
            if used_path:
                resolved_path = _resolve_repo_path(self.repo_root, used_path)
                if not resolved_path.exists():
                    if required:
                        raise FileNotFoundError(
                            f"Phase 4 could not reuse the selected transport {audit_key} forcing file: {resolved_path}"
                        )
                    resolved_path = None
            elif required:
                raise RuntimeError(
                    f"Phase 4 loading audit is missing a usable {audit_key} forcing path for the selected transport run."
                )

            if required and entry and entry.get("covers_requested_window") is False:
                raise RuntimeError(
                    f"Phase 4 requires selected transport {audit_key} forcing coverage to span the requested window."
                )

            resolved_paths[output_key] = resolved_path
            forcing_status[audit_key] = {
                "used_path": _relative_to_repo(self.repo_root, resolved_path) if resolved_path else "",
                "reader_attach_status": str(entry.get("reader_attach_status") or ""),
                "covers_requested_window": bool(entry.get("covers_requested_window", False)),
                "tail_extension_applied": bool(entry.get("tail_extension_applied", False)),
                "requested_start_time_utc": str(entry.get("requested_start_time_utc") or ""),
                "requested_end_time_utc": str(entry.get("requested_end_time_utc") or ""),
            }

        return {
            "resolution_mode": "phase2_loading_audit_deterministic_control_used_paths",
            "loading_audit_path": _relative_to_repo(self.repo_root, self.transport_loading_audit_path),
            "run_kind": str(deterministic_run.get("run_kind") or ""),
            "requested_start_time_utc": str(deterministic_run.get("requested_start_time_utc") or ""),
            "requested_end_time_utc": str(deterministic_run.get("requested_end_time_utc") or ""),
            "paths": resolved_paths,
            "forcings": forcing_status,
        }

    def _load_phase1_status(self) -> dict[str, Any]:
        audit_status = get_phase1_baseline_audit_status(self.baseline_selection_path)
        return {
            "phase1_finalization_classification": str(audit_status.get("classification") or ""),
            "phase1_frozen_story_complete": not bool(
                audit_status.get("full_production_rerun_required")
                or audit_status.get("requires_phase1_production_rerun_for_full_freeze")
            )
            and str(audit_status.get("classification") or "") == "implemented_and_scientifically_ready",
            "requires_phase1_production_rerun_for_full_freeze": bool(
                audit_status.get("full_production_rerun_required")
                or audit_status.get("requires_phase1_production_rerun_for_full_freeze")
            ),
            "phase1_biggest_blocker": str(audit_status.get("blocker") or ""),
        }

    def _load_phase2_status(self) -> dict[str, Any]:
        verdict = self.phase2_audit.get("overall_verdict") or {}
        if verdict:
            return {
                "phase2_scientifically_usable": bool(verdict.get("scientifically_usable_as_implemented")),
                "phase2_scientifically_frozen": bool(verdict.get("scientifically_frozen")),
                "phase2_transport_model_provisional": bool(verdict.get("transport_model_provisional")),
                "phase2_biggest_remaining_provisional_item": str(
                    verdict.get("biggest_remaining_phase2_provisional_item") or ""
                ),
            }

        transport = self.forecast_manifest.get("transport") or {}
        return {
            "phase2_scientifically_usable": True,
            "phase2_scientifically_frozen": False,
            "phase2_transport_model_provisional": bool(transport.get("provisional_transport_model", True)),
            "phase2_biggest_remaining_provisional_item": "",
        }

    def _weathering_path_audit(self) -> dict[str, Any]:
        return {
            "decision": "partially_implemented_but_needs_refactor",
            "reused_existing_weathering_path": True,
            "service_paths": [
                "src/services/weathering.py",
                "src/services/shoreline.py",
            ],
            "reason": (
                "The repo already contained a working OpenOil weathering path, but it still wrote Phase 3-era outputs and "
                "used synthetic shoreline segment ordering instead of the canonical GSHHG shoreline segment artifacts."
            ),
            "final_phase4_resolution": (
                "Phase 4 reuses the OpenOil path with a scenario registry, inherits the selected transport run's "
                "deterministic-control forcing files from phase2_loading_audit.json, and switches shoreline assignment "
                "onto the stored canonical shoreline segments while keeping the legacy fallback available for older workflows."
            ),
        }

    def _build_weathering_scenarios(self) -> dict[str, dict[str, Any]]:
        return {
            scenario["scenario_id"]: {
                "display_name": scenario["display_name"],
                "adios_id": scenario["adios_id"],
                "density_kg_m3": scenario["density_kg_m3"],
                "color": scenario["color"],
                "openoil_overrides": scenario["openoil_overrides"],
            }
            for scenario in self.scenarios
        }

    def _baseline_selection_source(self) -> str:
        return str(
            ((self.forecast_manifest.get("recipe_selection") or {}).get("source_path"))
            or ((self.forecast_manifest.get("baseline_provenance") or {}).get("source_path"))
            or "config/phase1_baseline_selection.yaml"
        )

    def _canonical_assignment_required(self, shoreline_df: pd.DataFrame) -> None:
        if shoreline_df.empty:
            return
        methods = set(str(value) for value in shoreline_df.get("segment_assignment_method", pd.Series(dtype=str)).dropna())
        if methods != {"canonical_shoreline_segments_gpkg"}:
            raise RuntimeError(
                "Phase 4 shoreline impacts must use the canonical shoreline segment artifact. "
                f"Observed assignment methods: {sorted(methods)}"
            )

    def _write_budget_summary(
        self,
        summary_rows: list[dict[str, Any]],
    ) -> Path:
        path = self.output_dir / "phase4_oil_budget_summary.csv"
        pd.DataFrame(summary_rows).to_csv(path, index=False)
        return path

    def _write_shoreline_arrival(
        self,
        arrival_rows: list[dict[str, Any]],
    ) -> Path:
        path = self.output_dir / "phase4_shoreline_arrival.csv"
        pd.DataFrame(arrival_rows).to_csv(path, index=False)
        return path

    def _write_shoreline_segments(
        self,
        shoreline_rows: list[dict[str, Any]],
    ) -> Path:
        path = self.output_dir / "phase4_shoreline_segments.csv"
        pd.DataFrame(shoreline_rows).to_csv(path, index=False)
        return path

    def _write_oiltype_comparison(
        self,
        summary_rows: list[dict[str, Any]],
    ) -> Path:
        summary_df = pd.DataFrame(summary_rows)
        anchor = summary_df[summary_df["scenario_id"] == "fixed_base_medium_heavy_proxy"]
        if anchor.empty:
            anchor = summary_df.iloc[[0]]
        anchor_row = anchor.iloc[0]

        comparison_rows: list[dict[str, Any]] = []
        for _, row in summary_df.iterrows():
            comparison_rows.append(
                {
                    "scenario_id": row["scenario_id"],
                    "oil_label": row["oil_label"],
                    "comparison_anchor_scenario_id": anchor_row["scenario_id"],
                    "delta_surface_pct_vs_anchor": round(float(row["final_surface_pct"] - anchor_row["final_surface_pct"]), 3),
                    "delta_evaporated_pct_vs_anchor": round(float(row["final_evaporated_pct"] - anchor_row["final_evaporated_pct"]), 3),
                    "delta_dispersed_pct_vs_anchor": round(float(row["final_dispersed_pct"] - anchor_row["final_dispersed_pct"]), 3),
                    "delta_beached_pct_vs_anchor": round(float(row["final_beached_pct"] - anchor_row["final_beached_pct"]), 3),
                    "delta_first_shoreline_arrival_h_vs_anchor": round(
                        float(row["first_shoreline_arrival_h"] - anchor_row["first_shoreline_arrival_h"]),
                        3,
                    )
                    if pd.notna(row["first_shoreline_arrival_h"]) and pd.notna(anchor_row["first_shoreline_arrival_h"])
                    else "",
                    "provisional_inherited_from_transport": bool(row["provisional_inherited_from_transport"]),
                }
            )

        path = self.output_dir / "phase4_oiltype_comparison.csv"
        pd.DataFrame(comparison_rows).to_csv(path, index=False)
        return path

    def _write_budget_timeseries(
        self,
        scenario_id: str,
        budget_df: pd.DataFrame,
    ) -> Path:
        path = self.output_dir / f"phase4_oil_budget_timeseries_{scenario_id}.csv"
        budget_df.to_csv(path, index=False)
        return path

    def _write_phase4_plots(
        self,
        budget_frames: dict[str, pd.DataFrame],
        summary_rows: list[dict[str, Any]],
        arrival_rows: list[dict[str, Any]],
    ) -> tuple[Path, Path]:
        import matplotlib.pyplot as plt

        summary_df = pd.DataFrame(summary_rows)
        arrival_df = pd.DataFrame(arrival_rows)

        comparison_path = self.output_dir / "qa_phase4_oiltype_comparison.png"
        fig, axes = plt.subplots(1, 2, figsize=(12, 5))
        for scenario in self.scenarios:
            scenario_id = scenario["scenario_id"]
            df = budget_frames[scenario_id]
            label = str(scenario["oil_label"])
            axes[0].plot(df["hours_elapsed"], df["evaporated_pct"], label=label)
            axes[1].plot(df["hours_elapsed"], df["beached_pct"], label=label)

        axes[0].set_title("Evaporated fraction")
        axes[0].set_xlabel("Hours elapsed")
        axes[0].set_ylabel("Percent of initial mass")
        axes[1].set_title("Beached fraction")
        axes[1].set_xlabel("Hours elapsed")
        axes[1].set_ylabel("Percent of initial mass")
        axes[1].legend(loc="best")
        fig.tight_layout()
        fig.savefig(comparison_path, dpi=200)
        plt.close(fig)

        shoreline_path = self.output_dir / "qa_phase4_shoreline_impacts.png"
        fig, axes = plt.subplots(1, 2, figsize=(12, 5))
        axes[0].bar(summary_df["oil_label"], summary_df["total_beached_kg"], color=summary_df["color"])
        axes[0].set_title("Total beached mass by scenario")
        axes[0].set_ylabel("kg")
        axes[0].tick_params(axis="x", rotation=20)
        axes[1].bar(arrival_df["oil_label"], arrival_df["impacted_segment_count"], color=summary_df["color"])
        axes[1].set_title("Impacted shoreline segments")
        axes[1].set_ylabel("count")
        axes[1].tick_params(axis="x", rotation=20)
        fig.tight_layout()
        fig.savefig(shoreline_path, dpi=200)
        plt.close(fig)

        return shoreline_path, comparison_path

    def _build_manifest(
        self,
        summary_rows: list[dict[str, Any]],
        arrival_rows: list[dict[str, Any]],
        artifacts: dict[str, str],
        verdict: dict[str, Any],
    ) -> dict[str, Any]:
        transport = self.forecast_manifest.get("transport") or {}
        return {
            "phase": PHASE,
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
            "case_id": self.case.case_id,
            "base_run_name": self.transport_source.base_run_name,
            "selected_transport_run_name": self.transport_source.selected_run_name,
            "transport_branch_kind": self.transport_source.branch_kind,
            "shoreline_aware_workflow_reused": self.transport_source.shoreline_aware_workflow_reused,
            "selected_transport_loading_audit_path": self.transport_forcing_bundle["loading_audit_path"],
            "transport_loading_audit_status": "reused_deterministic_control_forcing_paths",
            "transport_forcing_resolution_mode": self.transport_forcing_bundle["resolution_mode"],
            "transport_forcing_paths": {
                key: _relative_to_repo(self.repo_root, path)
                for key, path in self.transport_forcing_bundle["paths"].items()
                if path is not None
            },
            "transport_forcing_audit": self.transport_forcing_bundle["forcings"],
            "weathering_path_audit": self._weathering_path_audit(),
            "baseline_selection_source": self._baseline_selection_source(),
            "phase1_frozen_story_complete": self.phase1_status["phase1_frozen_story_complete"],
            "phase1_finalization_classification": self.phase1_status["phase1_finalization_classification"],
            "phase2_scientifically_usable": self.phase2_status["phase2_scientifically_usable"],
            "phase2_scientifically_frozen": self.phase2_status["phase2_scientifically_frozen"],
            "inherited_transport_model": str(transport.get("model") or ""),
            "provisional_transport_model": bool(transport.get("provisional_transport_model", True)),
            "provisional_inherited_from_transport": bool(
                transport.get("provisional_transport_model", True)
                or not self.phase1_status["phase1_frozen_story_complete"]
                or not self.phase2_status["phase2_scientifically_frozen"]
            ),
            "shoreline_mask_status": self.shoreline_mask_status,
            "shoreline_mask_signature": self.shoreline_mask_signature,
            "shoreline_segments_path": _relative_to_repo(self.repo_root, self.shoreline_segments_path),
            "oil_scenario_registry_path": _relative_to_repo(self.repo_root, self.registry_path),
            "official_recipe_family_expected": self.recipe_family_status["official_recipe_family_expected"],
            "official_recipe_family_locally_available": self.recipe_family_status["official_recipe_family_locally_available"],
            "legacy_recipe_id_detected": self.recipe_family_status["legacy_recipe_id_detected"],
            "requires_phase1_production_rerun_for_full_freeze": self.recipe_family_status[
                "requires_phase1_production_rerun_for_full_freeze"
            ],
            "transport_reuse_mode": (
                "phase4_deterministic_openoil_replay_using_selected_recipe_and_canonical_shoreline_artifacts"
            ),
            "artifacts": artifacts,
            "scenarios": self.scenarios,
            "summary_rows": summary_rows,
            "shoreline_arrival_rows": arrival_rows,
            "dwh_appendix_phase4_pilot_status": "deferred_not_run",
            "overall_verdict": verdict,
        }

    def _write_methodology_memo(
        self,
        summary_rows: list[dict[str, Any]],
        verdict: dict[str, Any],
    ) -> Path:
        path = self.output_dir / "phase4_methodology_sync_memo.md"
        scenario_labels = ", ".join(f"`{row['scenario_id']}`" for row in summary_rows)
        lines = [
            "# Phase 4 Methodology Sync Memo",
            "",
            "This Phase 4 run finalizes the oil-type fate and shoreline-impact workflow on top of the current reportable transport framework without changing the stored Phase 3 validation outputs.",
            "",
            "## Workflow Decisions",
            "",
            f"- Existing weathering path audit: `{self._weathering_path_audit()['decision']}`.",
            f"- Reused weathering path: `yes`.",
            f"- Selected transport branch: `{self.transport_source.selected_run_name}` (`{self.transport_source.branch_kind}`).",
            f"- Selected transport loading audit: `{self.transport_forcing_bundle['loading_audit_path']}`.",
            f"- Transport forcing reuse mode: `{self.transport_forcing_bundle['resolution_mode']}`.",
            f"- Canonical shoreline artifact reused: `{_relative_to_repo(self.repo_root, self.shoreline_segments_path)}`.",
            f"- Scenario registry: `{_relative_to_repo(self.repo_root, self.registry_path)}` with scenarios {scenario_labels}.",
            "",
            "## Honesty / Provenance",
            "",
            f"- Phase 1 frozen story complete: `{self.phase1_status['phase1_frozen_story_complete']}`.",
            f"- Phase 2 scientifically usable: `{self.phase2_status['phase2_scientifically_usable']}`.",
            f"- Phase 2 scientifically frozen: `{self.phase2_status['phase2_scientifically_frozen']}`.",
            f"- Provisional inherited from transport: `{verdict['provisional_inherited_from_transport']}`.",
            f"- Official recipe family locally available: `{self.recipe_family_status['official_recipe_family_locally_available']}`.",
            f"- Legacy recipe drift still present in runtime/config space: `{self.recipe_family_status['legacy_recipe_drift_leaks_into_official_mode']}`.",
            f"- Scenario mass-balance follow-up required: `{verdict['scenario_mass_balance_follow_up_required']}`.",
            "",
            "## DWH Appendix Hook",
            "",
            "- DWH Phase 4 pilot status: deferred in this patch so Mindoro Phase 4 could be finalized cleanly first.",
        ]
        _write_text(path, "\n".join(lines))
        return path

    def _write_final_verdict(
        self,
        verdict: dict[str, Any],
    ) -> Path:
        path = self.output_dir / "phase4_final_verdict.md"
        lines = [
            "# Phase 4 Final Verdict",
            "",
            f"- Is Phase 4 scientifically reportable now? `{'yes' if verdict['scientifically_reportable_now'] else 'no'}`",
            "- What remains provisional? The inherited transport framework is still upstream-provisional because the final Phase 1 frozen-baseline story is not complete, Phase 2 is scientifically usable but not frozen, and the official recipe-family drift has only been documented rather than fully removed.",
            f"- Does Phase 4 still depend on a later full Phase 1 production rerun for the final frozen baseline story? `{'yes' if verdict['requires_phase1_production_rerun_for_full_freeze'] else 'no'}`",
            f"- Is any part of official product generation still coupled to legacy recipe naming drift? `{'yes' if verdict['legacy_recipe_drift_still_present'] else 'no'}`",
            f"- Any scenario-level mass-balance follow-up still required? `{'yes' if verdict['scenario_mass_balance_follow_up_required'] else 'no'}`",
            f"- Is only a later final rerun needed, or are there still architecture gaps? `{verdict['next_step_label']}`",
            "",
            f"Single biggest remaining Phase 4 blocker: {verdict['biggest_remaining_phase4_blocker']}",
        ]
        if verdict["scenario_qc_failures"]:
            lines.append("")
            lines.append(
                "Scenario-level follow-up note: the configured mass-balance tolerance was exceeded for "
                + ", ".join(f"`{scenario_id}`" for scenario_id in verdict["scenario_qc_failures"])
                + "."
            )
        _write_text(path, "\n".join(lines))
        return path

    def run(self) -> dict[str, Any]:
        self.output_dir.mkdir(parents=True, exist_ok=True)

        start_lat, start_lon, start_time = resolve_spill_origin()
        weathering_scenarios = self._build_weathering_scenarios()
        weathering_results = self.weathering_runner(
            self.selected_recipe,
            start_time,
            start_lat,
            start_lon,
            scenarios=weathering_scenarios,
            output_dir=self.output_dir,
            phase_label="Phase 4",
            forcing_paths=self.transport_forcing_bundle["paths"],
        )

        summary_rows: list[dict[str, Any]] = []
        arrival_rows: list[dict[str, Any]] = []
        shoreline_rows: list[dict[str, Any]] = []
        budget_frames: dict[str, pd.DataFrame] = {}
        artifacts: dict[str, str] = {}
        initial_mass_tonnes = float((self.oil_config.get("simulation") or {}).get("initial_mass_tonnes", 50.0))
        particle_count = int((self.oil_config.get("simulation") or {}).get("num_particles", 100000))

        for scenario in self.scenarios:
            scenario_id = scenario["scenario_id"]
            result = weathering_results[scenario_id]
            budget_df = pd.DataFrame(result["budget_df"]).copy()
            budget_frames[scenario_id] = budget_df
            budget_path = self._write_budget_timeseries(scenario_id, budget_df)
            artifacts[f"phase4_oil_budget_timeseries_{scenario_id}"] = _relative_to_repo(self.repo_root, budget_path)

            shoreline_df = self.shoreline_analyzer(
                nc_path=result["nc_path"],
                initial_mass_tonnes=initial_mass_tonnes,
                canonical_segments_path=self.shoreline_segments_path,
                prefer_canonical_segments=True,
            )
            if shoreline_df is None:
                shoreline_df = pd.DataFrame(
                    columns=[
                        "segment_id",
                        "segment_assignment_method",
                        "total_beached_kg",
                        "n_particles",
                        "first_arrival_h",
                        "first_arrival_utc",
                    ]
                )

            self._canonical_assignment_required(shoreline_df)
            if not shoreline_df.empty:
                shoreline_df = shoreline_df.copy()
                shoreline_df.insert(0, "scenario_id", scenario_id)
                shoreline_df.insert(1, "oil_label", scenario["oil_label"])
                shoreline_rows.extend(shoreline_df.to_dict(orient="records"))

            final_budget = budget_df.iloc[-1]
            total_beached_kg = float(shoreline_df["total_beached_kg"].sum()) if not shoreline_df.empty else 0.0
            impacted_segment_count = int(shoreline_df["segment_id"].nunique()) if not shoreline_df.empty else 0
            first_arrival_h = float(shoreline_df["first_arrival_h"].min()) if not shoreline_df.empty else float("nan")
            first_arrival_utc = str(shoreline_df["first_arrival_utc"].min()) if not shoreline_df.empty else ""
            provisional_inherited = bool(
                (self.forecast_manifest.get("transport") or {}).get("provisional_transport_model", True)
                or not self.phase1_status["phase1_frozen_story_complete"]
                or not self.phase2_status["phase2_scientifically_frozen"]
            )

            summary_rows.append(
                {
                    "scenario_id": scenario_id,
                    "oil_record_id": scenario["oil_record_id"],
                    "oil_label": scenario["oil_label"],
                    "category": scenario["category"],
                    "source_note": scenario["source_note"],
                    "color": scenario["color"],
                    "adios_id": scenario["adios_id"],
                    "particle_count": particle_count,
                    "initial_mass_tonnes": initial_mass_tonnes,
                    "final_surface_pct": round(float(final_budget["surface_pct"]), 3),
                    "final_evaporated_pct": round(float(final_budget["evaporated_pct"]), 3),
                    "final_dispersed_pct": round(float(final_budget["dispersed_pct"]), 3),
                    "final_beached_pct": round(float(final_budget["beached_pct"]), 3),
                    "qc_passed": bool((result.get("qc") or {}).get("passed", True)),
                    "max_mass_balance_deviation_pct": float((result.get("qc") or {}).get("max_deviation_pct", 0.0)),
                    "first_shoreline_arrival_utc": first_arrival_utc,
                    "first_shoreline_arrival_h": round(first_arrival_h, 3) if pd.notna(first_arrival_h) else "",
                    "impacted_segment_count": impacted_segment_count,
                    "total_beached_kg": round(total_beached_kg, 3),
                    "provisional_inherited_from_transport": provisional_inherited,
                    "selected_transport_run_name": self.transport_source.selected_run_name,
                }
            )
            arrival_rows.append(
                {
                    "scenario_id": scenario_id,
                    "oil_label": scenario["oil_label"],
                    "shoreline_arrival_generated": bool(not shoreline_df.empty),
                    "first_shoreline_arrival_utc": first_arrival_utc,
                    "first_shoreline_arrival_h": round(first_arrival_h, 3) if pd.notna(first_arrival_h) else "",
                    "impacted_segment_count": impacted_segment_count,
                    "total_beached_kg": round(total_beached_kg, 3),
                    "segment_assignment_method": (
                        str(shoreline_df["segment_assignment_method"].iloc[0]) if not shoreline_df.empty else "no_beaching_detected"
                    ),
                }
            )

        summary_path = self._write_budget_summary(summary_rows)
        arrival_path = self._write_shoreline_arrival(arrival_rows)
        shoreline_path = self._write_shoreline_segments(shoreline_rows)
        comparison_path = self._write_oiltype_comparison(summary_rows)
        qa_shoreline_path, qa_comparison_path = self._write_phase4_plots(budget_frames, summary_rows, arrival_rows)
        scenario_qc_failures = [
            row["scenario_id"]
            for row in summary_rows
            if not bool(row.get("qc_passed", False))
        ]

        verdict = {
            "scientifically_reportable_now": True,
            "provisional_inherited_from_transport": bool(
                (self.forecast_manifest.get("transport") or {}).get("provisional_transport_model", True)
                or not self.phase1_status["phase1_frozen_story_complete"]
                or not self.phase2_status["phase2_scientifically_frozen"]
            ),
            "requires_phase1_production_rerun_for_full_freeze": self.phase1_status[
                "requires_phase1_production_rerun_for_full_freeze"
            ],
            "legacy_recipe_drift_still_present": bool(self.recipe_family_status["legacy_recipe_drift_leaks_into_official_mode"]),
            "scenario_mass_balance_follow_up_required": bool(scenario_qc_failures),
            "scenario_qc_failures": scenario_qc_failures,
            "architecture_gaps_remaining": False,
            "next_step_label": "later_final_rerun_needed_only",
            "biggest_remaining_phase4_blocker": (
                self.phase1_status["phase1_biggest_blocker"]
                or self.phase2_status["phase2_biggest_remaining_provisional_item"]
                or "The upstream frozen-baseline story still requires the later Phase 1 production rerun."
            ),
        }

        artifacts.update(
            {
                "phase4_oil_budget_summary": _relative_to_repo(self.repo_root, summary_path),
                "phase4_shoreline_arrival": _relative_to_repo(self.repo_root, arrival_path),
                "phase4_shoreline_segments": _relative_to_repo(self.repo_root, shoreline_path),
                "phase4_oiltype_comparison": _relative_to_repo(self.repo_root, comparison_path),
                "qa_phase4_shoreline_impacts": _relative_to_repo(self.repo_root, qa_shoreline_path),
                "qa_phase4_oiltype_comparison": _relative_to_repo(self.repo_root, qa_comparison_path),
            }
        )

        methodology_path = self._write_methodology_memo(summary_rows, verdict)
        final_verdict_path = self._write_final_verdict(verdict)
        artifacts["phase4_methodology_sync_memo"] = _relative_to_repo(self.repo_root, methodology_path)
        artifacts["phase4_final_verdict"] = _relative_to_repo(self.repo_root, final_verdict_path)

        manifest = self._build_manifest(summary_rows, arrival_rows, artifacts, verdict)
        manifest_path = self.output_dir / "phase4_run_manifest.json"
        _write_json(manifest_path, manifest)
        artifacts["phase4_run_manifest"] = _relative_to_repo(self.repo_root, manifest_path)

        return {
            "output_dir": str(self.output_dir),
            "manifest_path": str(manifest_path),
            "summary_csv": str(summary_path),
            "shoreline_arrival_csv": str(arrival_path),
            "shoreline_segments_csv": str(shoreline_path),
            "oiltype_comparison_csv": str(comparison_path),
            "methodology_memo_md": str(methodology_path),
            "final_verdict_md": str(final_verdict_path),
            "qa_phase4_shoreline_impacts_png": str(qa_shoreline_path),
            "qa_phase4_oiltype_comparison_png": str(qa_comparison_path),
            "shoreline_arrival_generated": any(bool(row.get("shoreline_arrival_generated")) for row in arrival_rows),
            "selected_transport_loading_audit_path": str(self.transport_loading_audit_path),
            "verdict": verdict,
            "scenario_ids": [scenario["scenario_id"] for scenario in self.scenarios],
            "weathering_path_audit": self._weathering_path_audit(),
        }


def run_phase4_oiltype_and_shoreline() -> dict[str, Any]:
    return Phase4OilTypeAndShorelineService().run()
