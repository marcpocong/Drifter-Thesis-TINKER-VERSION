"""
Main entry point for the Drifter-Validated Oil Spill Forecasting System.

Container routing via the PIPELINE_PHASE environment variable:
  PIPELINE_PHASE=prep            -> Pipeline-only input preparation and manifests
  PIPELINE_PHASE=1_2  (default) -> Prototype: Phase 1 validation + Phase 2 ensemble
                                    Official: frozen-baseline recipe + deterministic control + Phase 2 ensemble
  PIPELINE_PHASE=official_phase3b -> Official minimal path: deterministic control + ensemble + Phase 3B public-validation lane
  PIPELINE_PHASE=recipe_sensitivity -> Official event-scale Phase 3B validation sensitivities
  PIPELINE_PHASE=convergence_after_shoreline -> Official shoreline-aware particle-count convergence
  PIPELINE_PHASE=displacement_after_convergence -> Official post-convergence displacement/transport audit
  PIPELINE_PHASE=phase3b_multidate_public -> Official multi-date public-observation validation
  PIPELINE_PHASE=phase3b_extended_public -> Official extended-horizon public-observation validation guardrail
  PIPELINE_PHASE=phase3b_extended_public_scored -> Appendix-only short extended public-observation scoring
  PIPELINE_PHASE=phase3b_extended_public_scored_march23 -> Appendix-only March 23 extended public branch stress test
  PIPELINE_PHASE=phase3b_extended_public_scored_march13_14_reinit -> Mindoro Phase 3B primary public-validation row (March 13 -> March 14 NOAA reinit with explicit shared-imagery caveat)
  PIPELINE_PHASE=phase3b_extended_public_scored_march13_14_reinit_pygnome_comparison -> Mindoro Phase 3A same-case comparator-only March 13 -> March 14 support lane
  PIPELINE_PHASE=horizon_survival_audit -> Read-only short-extended horizon survival diagnosis
  PIPELINE_PHASE=transport_retention_fix -> Official transport-retention sensitivity diagnostics
  PIPELINE_PHASE=official_rerun_r1 -> Promote selected R1 retention mode and rescore strict/short tracks
  PIPELINE_PHASE=init_mode_sensitivity_r1 -> Compare B polygon vs A1 source-point initialization under R1
  PIPELINE_PHASE=source_history_reconstruction_r1 -> Test A2 source-history release duration under R1
  PIPELINE_PHASE=pygnome_public_comparison -> Compare OpenDrift/PyGNOME against public observation masks
  PIPELINE_PHASE=ensemble_threshold_sensitivity -> Calibrate ensemble footprint thresholds without rerunning
  PIPELINE_PHASE=recipe_sensitivity_r1_multibranch -> Test R1 OpenDrift recipe/branch matrix vs PyGNOME
  PIPELINE_PHASE=public_obs_appendix -> Official appendix-only public observation expansion
  PIPELINE_PHASE=march6_recovery_sensitivity -> Appendix-only March 6 threshold/buffer/branch recovery matrix
  PIPELINE_PHASE=phase3c_external_case_setup -> External rich-data spill setup and observation ingestion
  PIPELINE_PHASE=dwh_phase3c_forcing_adapter_and_non_scientific_smoke_forecast -> DWH forcing adapter status + non-scientific smoke forecast
  PIPELINE_PHASE=dwh_phase3c_scientific_forcing_ready -> DWH scientific historical forcing readiness check
  PIPELINE_PHASE=phase3c_external_case_run -> DWH Phase 3C external transfer-validation run
  PIPELINE_PHASE=phase3c_external_case_ensemble_comparison -> DWH Phase 3C transfer-validation deterministic-vs-ensemble comparison
  PIPELINE_PHASE=phase3c_dwh_pygnome_comparator -> DWH Phase 3C transfer-validation cross-model PyGNOME comparator
  PIPELINE_PHASE=final_validation_package -> Read-only thesis package built from completed Mindoro + DWH outputs
  PIPELINE_PHASE=phase1_finalization_audit -> Read-only Chapter 3 Phase 1 architecture audit and verdict
  PIPELINE_PHASE=phase2_finalization_audit -> Read-only Chapter 3 Phase 2 semantics/manifests audit and verdict
  PIPELINE_PHASE=phase1_production_rerun -> Dedicated full 2016-2022 historical/regional Phase 1 production rerun
  PIPELINE_PHASE=mindoro_march13_14_phase1_focus_trial -> Experimental March 13 -> March 14 replay using the staged Mindoro-focused pre-spill Phase 1 candidate
  PIPELINE_PHASE=mindoro_local_recipe_experiment -> Stage a Mindoro-local recipe candidate and replay the Mindoro event comparison with GFS
  PIPELINE_PHASE=phase4_oiltype_and_shoreline -> Mindoro support-layer oil-type fate and shoreline-impact workflow
  PIPELINE_PHASE=phase4_crossmodel_comparability_audit -> Read-only Phase 4 OpenDrift-vs-PyGNOME comparability audit
  PIPELINE_PHASE=phase5_launcher_and_docs_sync -> Read-only launcher/docs/reproducibility support sync
  PIPELINE_PHASE=trajectory_gallery_build -> Read-only static trajectory/overlay/shoreline figure gallery
  PIPELINE_PHASE=prototype_pygnome_similarity_summary -> Read-only cross-case prototype OpenDrift-vs-PyGNOME transport summary
  PIPELINE_PHASE=prototype_legacy_final_figures -> Read-only curated prototype_2016 legacy Phase 5 final paper-figure export
  PIPELINE_PHASE=prototype_2016_display_review -> Read-only prototype_2016 review-only display refinement export from stored outputs
  PIPELINE_PHASE=prototype_legacy_phase4_weathering -> Prototype 2016 legacy Phase 4 oil weathering and fate analysis
  PIPELINE_PHASE=3               -> Legacy compatibility alias (prototype_2016 Phase 4; otherwise legacy Phase 3 weathering path)
  PIPELINE_PHASE=benchmark       -> Phase 3A cross-model benchmark
  PIPELINE_PHASE=3b              -> Legacy compatibility alias (prototype_2016 appendix-only hidden path)

docker-compose exec pipeline runs Phase 1 + 2.
docker-compose exec gnome   runs Phase 3.
"""

import json
import os
import sys
from pathlib import Path


def print_workflow_context():
    from src.core.case_context import get_case_log_lines

    print("Workflow Context:")
    for line in get_case_log_lines():
        print(f"  {line}")


def emit_forcing_outage_skip_and_exit(exc) -> None:
    from src.utils.forcing_outage_policy import (
        FORCING_OUTAGE_SKIP_EXIT_CODE,
        FORCING_OUTAGE_SKIP_PAYLOAD_PREFIX,
    )

    print("")
    print("Phase skipped after a temporary forcing-provider outage.")
    print(f"Reason: {exc.reason}")
    if exc.missing_forcing_factors:
        print(f"Missing forcing factors: {', '.join(exc.missing_forcing_factors)}")
    if exc.skipped_recipe_ids:
        print(f"Skipped recipes: {', '.join(exc.skipped_recipe_ids)}")
    if exc.skipped_branch_ids:
        print(f"Skipped branches: {', '.join(exc.skipped_branch_ids)}")
    if exc.manifest_path:
        print(f"Manifest: {exc.manifest_path}")
    print(f"{FORCING_OUTAGE_SKIP_PAYLOAD_PREFIX}{json.dumps(exc.to_payload(), sort_keys=True)}")
    sys.exit(FORCING_OUTAGE_SKIP_EXIT_CODE)


def emit_benchmark_skip_and_exit(exc) -> None:
    from src.exceptions.custom import BENCHMARK_SKIP_EXIT_CODE

    print("")
    print("Benchmark skipped.")
    print(f"Reason: {exc}")
    sys.exit(BENCHMARK_SKIP_EXIT_CODE)


def _prep_command_hint() -> str:
    workflow_mode = os.environ.get("WORKFLOW_MODE", "prototype_2021")
    return (
        "docker-compose -f docker-compose.yml exec -T "
        f"-e WORKFLOW_MODE={workflow_mode} -e PIPELINE_PHASE=prep pipeline python -m src"
    )


def _current_phase() -> str:
    return str(os.environ.get("PIPELINE_PHASE", "1_2") or "1_2")


def _current_workflow_mode() -> str:
    return str(os.environ.get("WORKFLOW_MODE", "prototype_2021") or "prototype_2021")


def _current_phase_allows_input_refresh() -> bool:
    from src.utils.startup_prompt_policy import phase_uses_startup_prompts

    phase = _current_phase()
    if phase == "prep":
        return False
    return phase_uses_startup_prompts(phase, role=os.environ.get("PIPELINE_ROLE", ""))


def _force_refresh_inputs_requested() -> bool:
    from src.utils.startup_prompt_policy import input_cache_policy_force_refresh_enabled

    return _current_phase_allows_input_refresh() and input_cache_policy_force_refresh_enabled()


def _startup_refresh_marker_path(run_name: str) -> Path:
    from src.utils.startup_prompt_policy import RUN_STARTUP_TOKEN_ENV

    token = str(os.environ.get(RUN_STARTUP_TOKEN_ENV, "") or "").strip()
    safe_run_name = str(run_name or "run").replace("\\", "_").replace("/", "_").replace(":", "_")
    if not token:
        return Path("logs") / f".startup_refresh_{safe_run_name}_no_token.json"
    return Path("logs") / f".startup_refresh_{safe_run_name}_{token}.json"


def _startup_refresh_already_completed(run_name: str) -> bool:
    return _startup_refresh_marker_path(run_name).exists()


def _mark_startup_refresh_completed(run_name: str) -> None:
    marker_path = _startup_refresh_marker_path(run_name)
    marker_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "run_name": str(run_name),
        "workflow_mode": _current_workflow_mode(),
        "phase": _current_phase(),
    }
    marker_path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")


def _resolve_startup_prompt_env_once(phase: str) -> dict | None:
    from src.utils.startup_prompt_policy import (
        RUN_STARTUP_PROMPTS_RESOLVED_ENV,
        resolve_run_startup_state,
    )

    if os.environ.get(RUN_STARTUP_PROMPTS_RESOLVED_ENV, "").strip() == "1":
        return None

    return resolve_run_startup_state(
        workflow_mode=_current_workflow_mode(),
        phase=phase,
        role=os.environ.get("PIPELINE_ROLE", ""),
        apply=True,
    )


def _format_startup_policy_source_label(source_id: str) -> str:
    source_map = {
        "explicit_env": "explicit env",
        "interactive_prompt": "interactive startup prompt",
        "non_interactive_default": "non-interactive default",
        "default_no_eligible_cache": "default; no eligible local input cache detected",
        "default_non_promptable": "default; phase does not use startup prompts",
        "prep_force_refresh_alias": "PREP_FORCE_REFRESH compatibility alias",
    }
    return source_map.get(str(source_id or ""), str(source_id or "default"))


def _emit_direct_run_startup_banner(startup_state: dict | None) -> None:
    if not startup_state:
        return

    probe = startup_state.get("probe") or {}
    if not probe.get("should_prompt_wait_budget"):
        return

    print("")
    print("Startup policy:")
    print(
        "  INPUT_CACHE_POLICY="
        f"{startup_state['input_cache_policy']} "
        f"({_format_startup_policy_source_label(startup_state['input_cache_policy_source'])})"
    )
    print(
        "  FORCING_SOURCE_BUDGET_SECONDS="
        f"{startup_state['forcing_source_budget_seconds']} "
        f"({_format_startup_policy_source_label(startup_state['wait_budget_source'])})"
    )

    if startup_state.get("prompting_skipped_reason") == "non_interactive_runtime":
        print("  Startup prompts were skipped because this direct run is non-interactive.")
        matching_entry_id = str(startup_state.get("matching_launcher_entry_id") or "").strip()
        if matching_entry_id:
            print(f"  Use .\\start.ps1 -Entry {matching_entry_id} to be asked once at startup.")
        print("  Or run docker-compose exec without -T if you want the direct container command to prompt.")


def ensure_prepared_inputs(
    run_name: str,
    recipe_name: str | None = None,
    require_drifter: bool = False,
    include_all_transport_forcing: bool = False,
    phase_label: str = "requested phase",
):
    if _force_refresh_inputs_requested() and not _startup_refresh_already_completed(run_name):
        from src.services.ingestion import DataIngestionService

        print(f"Force-refresh requested for {phase_label}. Re-running input preparation first...")
        DataIngestionService().run()
        _mark_startup_refresh_completed(run_name)

    from src.utils.io import find_missing_prepared_inputs

    missing_specs = find_missing_prepared_inputs(
        recipe_name=recipe_name,
        require_drifter=require_drifter,
        include_all_transport_forcing=include_all_transport_forcing,
        run_name=run_name,
    )
    if not missing_specs:
        return

    required_missing = [spec for spec in missing_specs if spec.get("required", True)]
    optional_missing = [spec for spec in missing_specs if not spec.get("required", True)]

    if not required_missing:
        print(f"Optional inputs are missing for {phase_label}. Continuing without them.")
        for spec in optional_missing:
            print(f"  - {spec['source']}: {spec['path']}")
        return

    print(f"Missing prepared inputs for {phase_label}.")
    print("This phase is read-only and will not download or preprocess data.")
    print("Missing files:")
    for spec in required_missing:
        print(f"  - {spec['label']}: {spec['path']}")
        print(f"    source: {spec['source']}")
    if optional_missing:
        print("Optional inputs also missing:")
        for spec in optional_missing:
            print(f"  - {spec['source']}: {spec['path']}")
    print("Run the pipeline-only prep stage first:")
    print(f"  {_prep_command_hint()}")
    sys.exit(1)


def ensure_phase3b_forecast_outputs(
    run_name: str,
    recipe_name: str,
    phase_label: str = "Phase 3B",
):
    from src.core.case_context import get_case_context
    from src.utils.io import find_missing_phase3b_forecast_outputs

    case = get_case_context()
    missing_specs = find_missing_phase3b_forecast_outputs(recipe_name=recipe_name, run_name=run_name)
    if not missing_specs:
        return

    print(f"Missing spill forecast outputs for {phase_label}.")
    print("Phase 3B requires deterministic/ensemble spill outputs but does not depend on benchmark or weathering.")
    print("Missing files:")
    for spec in missing_specs:
        print(f"  - {spec['label']}: {spec['path']}")
        print(f"    source: {spec['source']}")
    if case.is_official:
        print("Run the official minimal forecast path after prep:")
        print(
            "  docker-compose -f docker-compose.yml exec -T "
            f"-e WORKFLOW_MODE={os.environ.get('WORKFLOW_MODE', 'mindoro_retro_2023')} "
            "-e PIPELINE_PHASE=official_phase3b pipeline python -m src"
        )
    else:
        print("Run Phase 1 + 2 first to generate deterministic/ensemble forecast outputs.")
    sys.exit(1)


def ensure_data_exists(run_name: str, require_drifter: bool = True):
    from src.core.case_context import get_case_context
    from src.services.ingestion import DataIngestionService
    from src.utils.io import resolve_initialization_polygon_path, resolve_validation_polygon_path

    drifter_path = Path(f"data/drifters/{run_name}/drifters_noaa.csv")
    case = get_case_context()
    init_polygon_path = resolve_initialization_polygon_path(run_name)
    validation_polygon_path = resolve_validation_polygon_path(run_name)

    missing = (
        not init_polygon_path.exists()
        or not validation_polygon_path.exists()
        or (require_drifter and not drifter_path.exists())
    )
    if missing or (_force_refresh_inputs_requested() and not _startup_refresh_already_completed(run_name)):
        if missing:
            print("Required data not found. Initiating ingestion service...")
        else:
            print("Force-refresh requested. Initiating ingestion service...")
        DataIngestionService().run()
        _mark_startup_refresh_completed(run_name)

        still_missing = (
            not init_polygon_path.exists()
            or not validation_polygon_path.exists()
            or (require_drifter and not drifter_path.exists())
        )
        if still_missing:
            print("Ingestion failed to produce required data. Exiting.")
            sys.exit(1)


def run_prep():
    from src.core.case_context import get_case_context
    from src.core.constants import RUN_NAME
    from src.exceptions.custom import (
        PREP_OUTAGE_DECISION_EXIT_CODE,
        PREP_OUTAGE_PAYLOAD_PREFIX,
        PREP_OUTAGE_PROMPT_SUPPORTED_ENV,
        PrepOutageDecisionRequired,
    )
    from src.services.ingestion import DataIngestionService

    pipeline_role = os.environ.get("PIPELINE_ROLE", "").strip().lower()
    if pipeline_role and pipeline_role != "pipeline":
        print("The prep stage is only available in the pipeline container.")
        print(f"Run it with: {_prep_command_hint()}")
        sys.exit(1)

    case = get_case_context()

    print("Starting pipeline-only input preparation...")
    print_workflow_context()

    try:
        result = DataIngestionService().run() or {}
    except PrepOutageDecisionRequired as exc:
        print("")
        print("Required prep input hit a temporary remote-service outage.")
        print(f"Source: {exc.source_id}")
        print(f"Validated same-case cache: {exc.cache_path}")
        print(f"Remote error: {exc.error}")
        if os.environ.get(PREP_OUTAGE_PROMPT_SUPPORTED_ENV, "").strip() == "1":
            print(f"{PREP_OUTAGE_PAYLOAD_PREFIX}{json.dumps(exc.to_payload(), sort_keys=True)}")
            sys.exit(PREP_OUTAGE_DECISION_EXIT_CODE)
        print("This direct prep command is non-interactive and will not prompt for cache reuse.")
        print("Rerun prep through .\\start.ps1 so you can choose 'Reuse validated cache' or 'Cancel'.")
        sys.exit(1)

    ensure_prepared_inputs(
        RUN_NAME,
        require_drifter=case.drifter_required,
        include_all_transport_forcing=True,
        phase_label="pipeline prep stage",
    )

    print("\nPreparation complete.")
    if result.get("download_manifest"):
        print(f"Download manifest: {result['download_manifest']}")
    if result.get("prepared_input_manifest"):
        print(f"Prepared-input manifest: {result['prepared_input_manifest']}")


def print_phase2_outputs(results):
    from src.core.case_context import get_case_context
    from src.core.constants import BASE_OUTPUT_DIR

    case = get_case_context()
    print("\nEnsemble generation complete.")
    print(f"Outputs saved to: {results['output']}")
    if case.is_official:
        print("Official Phase 2 forecast products:")
        print(f"  - {BASE_OUTPUT_DIR}/forecast/forecast_manifest.json")
        print(f"  - {BASE_OUTPUT_DIR}/forecast/control_footprint_mask_2023-03-06T09-59-00Z.tif")
        print(f"  - {BASE_OUTPUT_DIR}/forecast/control_density_norm_2023-03-06T09-59-00Z.tif")
        print(f"  - {BASE_OUTPUT_DIR}/forecast/phase2_loading_audit.json")
        print(f"  - {BASE_OUTPUT_DIR}/forecast/phase2_loading_audit.csv")
        print(f"  - {BASE_OUTPUT_DIR}/ensemble/ensemble_manifest.json")
        print(f"  - {BASE_OUTPUT_DIR}/ensemble/prob_presence_2023-03-06T09-59-00Z.tif")
        print(f"  - {BASE_OUTPUT_DIR}/ensemble/mask_p50_2023-03-06T09-59-00Z.tif")
        print(f"  - {BASE_OUTPUT_DIR}/ensemble/mask_p90_2023-03-06T09-59-00Z.tif")
        print(f"  - {BASE_OUTPUT_DIR}/ensemble/prob_presence_2023-03-06_datecomposite.tif")
        print(f"  - {BASE_OUTPUT_DIR}/ensemble/mask_p50_2023-03-06_datecomposite.tif")
    else:
        print("Phase 2 probability outputs:")
        print(f"  - {BASE_OUTPUT_DIR}/ensemble/probability_24h.png")
        print(f"  - {BASE_OUTPUT_DIR}/ensemble/probability_48h.png")
        print(f"  - {BASE_OUTPUT_DIR}/ensemble/probability_72h.png")
        print(f"  - {BASE_OUTPUT_DIR}/ensemble/probability_24h.nc")
        print(f"  - {BASE_OUTPUT_DIR}/ensemble/probability_48h.nc")
        print(f"  - {BASE_OUTPUT_DIR}/ensemble/probability_72h.nc")
    if results.get("manifest"):
        print(f"  - {results['manifest']} (ensemble manifest)")
    if results.get("written_files") and not case.is_official:
        alias_path = next((p for p in results["written_files"] if p.endswith("probability_cone.png")), None)
        if alias_path:
            print(f"  - {alias_path} (legacy alias)")


def print_recipe_selection(selection, label: str = "Recipe selection"):
    from src.core.case_context import get_case_context
    from src.utils.io import get_phase2_recipe_family_status

    print(f"{label}: {selection.recipe}")
    print(f"Selection source: {selection.source_kind}")
    print(
        f"Selection status: {selection.status_flag} "
        f"(valid={selection.valid}, provisional={selection.provisional}, rerun_required={selection.rerun_required})"
    )
    if selection.source_path:
        print(f"Selection artifact: {selection.source_path}")
    if selection.note:
        print(f"Selection note: {selection.note}")

    case = get_case_context()
    if case.is_official:
        phase2_status = get_phase2_recipe_family_status(
            run_name=case.run_name,
            selected_recipe=selection.recipe,
        )
        if phase2_status["requires_phase1_production_rerun_for_full_freeze"]:
            print(
                "Phase 1 freeze note: dedicated 2016-2022 rerun outputs now exist, "
                "but the candidate baseline is not yet adopted as the default frozen spill-case baseline."
            )
        if phase2_status["legacy_recipe_drift_leaks_into_official_mode"]:
            print("Recipe-family note: official recipe-family support is still partial locally; legacy *_ncep IDs remain and gfs_wind.nc is not present.")


def run_phase1_and_2():
    from src.core.case_context import get_case_context
    from src.core.constants import RUN_NAME
    from src.services.ensemble import run_ensemble, run_official_spill_forecast
    from src.utils.io import (
        get_transport_recipe_family_for_workflow,
        resolve_best_recipe,
        resolve_recipe_selection,
        resolve_spill_origin,
    )

    case = get_case_context()

    if case.is_official:
        print("Starting official spill-case workflow...")
        print_workflow_context()

        print("Historical Phase 1 drifter validation is disabled in official mode.")
        selection = resolve_recipe_selection()
        best_recipe = selection.recipe
        ensure_prepared_inputs(
            RUN_NAME,
            recipe_name=best_recipe,
            require_drifter=case.drifter_required,
            phase_label="official spill-case forecast",
        )
        print_recipe_selection(selection, label="Baseline recipe")

        start_lat, start_lon, start_time_str = resolve_spill_origin()
        print(f"Origin: {start_lat:.4f}, {start_lon:.4f} at {start_time_str}")

        results = run_official_spill_forecast(
            selection=selection,
            start_time=start_time_str,
            start_lat=start_lat,
            start_lon=start_lon,
        )
        print_phase2_outputs(results)
        if results.get("deterministic_control"):
            print(f"Deterministic control: {results['deterministic_control']}")
        if results.get("forecast_manifest"):
            print(f"Forecast manifest: {results['forecast_manifest']}")
        print("\nOfficial spill-case pipeline completed successfully.")
        return

    from src.services.validation import TransportValidationService
    from src.utils.io import load_drifter_data

    print("Starting Phase 1: Transport Validation Pipeline...")
    print_workflow_context()
    if case.workflow_mode == "prototype_2021":
        print("prototype_2021 is the preferred accepted-segment debug lane. It is stronger than the legacy 2016 prototype, but it is still not the final Chapter 3 Phase 1 study.")
    else:
        print("Prototype mode is preserved for debugging and regression checks only; it is not the final Chapter 3 Phase 1 study.")

    ensure_data_exists(RUN_NAME, require_drifter=True)
    drifter_path = Path(f"data/drifters/{RUN_NAME}/drifters_noaa.csv")

    print(f"Loading drifter data from {drifter_path}...")
    drifter_df = load_drifter_data(drifter_path)

    prototype_recipe_family = get_transport_recipe_family_for_workflow(case.workflow_mode)
    service = TransportValidationService()
    print("Running validation service across the workflow-selected transport recipe family...")
    rankings = service.run_validation(drifter_df, recipe_names=prototype_recipe_family)
    print("\nFinal rankings (lower NCS is better):")
    print(rankings)

    print("\nInitiating Phase 2: Ensemble Uncertainty Quantification...")

    best_recipe = rankings.iloc[0]["recipe"]
    print(f"Winning recipe: {best_recipe} (NCS: {rankings.iloc[0]['ncs_score']:.4f})")

    start_lat, start_lon, start_time_str = resolve_spill_origin()
    print(f"Origin: {start_lat:.4f}, {start_lon:.4f} at {start_time_str}")

    results = run_ensemble(
        best_recipe=best_recipe,
        start_time=start_time_str,
        start_lat=start_lat,
        start_lon=start_lon,
    )
    print_phase2_outputs(results)
    print("\nPhase 1 + 2 pipeline completed successfully.")


def run_official_phase3b_minimal():
    from src.core.case_context import get_case_context

    case = get_case_context()
    if not case.is_official:
        print("official_phase3b is only supported for official spill-case workflows.")
        sys.exit(1)

    print("Starting minimal official spill-case path: deterministic control + ensemble + Phase 3B public-validation...")
    run_phase1_and_2()
    run_phase3b()


def run_phase3(
    *,
    phase_label: str = "Phase 3",
    refined_stage_label: str = "Stage 3b",
    include_gnome_comparison: bool = True,
):
    import yaml

    from src.core.constants import BASE_OUTPUT_DIR, RUN_NAME
    from src.helpers.plotting import plot_diagnostic_forcing
    from src.services.diagnostics import run_diagnostics
    from src.services.gnome_comparison import run_gnome_comparison
    from src.services.shoreline import run_shoreline_analysis
    from src.services.weathering import run_refined_weathering, run_weathering
    from src.utils.io import resolve_recipe_selection, resolve_spill_origin

    print(f"Starting {phase_label}: Oil Weathering and Fate Analysis...")
    print_workflow_context()

    selection = resolve_recipe_selection()
    best_recipe = selection.recipe
    ensure_prepared_inputs(
        RUN_NAME,
        recipe_name=best_recipe,
        phase_label=phase_label,
    )
    print_recipe_selection(selection, label="Transport recipe")
    start_lat, start_lon, start_time_str = resolve_spill_origin()
    print(f"Best recipe  : {best_recipe}")
    print(f"Spill origin : {start_lat:.4f}, {start_lon:.4f} at {start_time_str}")

    print("\nRunning pre-flight diagnostics...")
    diag_report = run_diagnostics(
        best_recipe=best_recipe,
        start_time=start_time_str,
        start_lat=start_lat,
        start_lon=start_lon,
        diagnostics_label=(
            "Phase 3 Enhancement – Pre-Flight Diagnostics"
            if phase_label == "Phase 3"
            else f"{phase_label} – Pre-Flight Diagnostics"
        ),
    )
    try:
        plot_diagnostic_forcing(
            diag_report,
            str(BASE_OUTPUT_DIR / "diagnostics/forcing_summary.png"),
            title=(
                "Phase 3 Enhancement – Pre-Flight Diagnostics"
                if phase_label == "Phase 3"
                else f"{phase_label} – Pre-Flight Diagnostics"
            ),
        )
        print(f"Diagnostic chart -> {BASE_OUTPUT_DIR}/diagnostics/forcing_summary.png")
    except Exception as e:
        print(f"Diagnostic chart generation failed: {e}")

    weathering_results = run_weathering(
        best_recipe=best_recipe,
        start_time=start_time_str,
        start_lat=start_lat,
        start_lon=start_lon,
        phase_label=phase_label,
    )

    print(f"\n{phase_label} - Mass Budget Summary (at 72 h):")
    for oil_key, res in weathering_results.items():
        df = res["budget_df"]
        last = df.iloc[-1]
        print(f"  [{res['display_name']}]")
        print(f"    Surface:    {last['surface_pct']:.1f}%")
        print(f"    Evaporated: {last['evaporated_pct']:.1f}%")
        print(f"    Dispersed:  {last['dispersed_pct']:.1f}%")
        print(f"    Beached:    {last['beached_pct']:.1f}%")

    print("\nMass-Balance QC Check (tolerance <= 2 %):")
    for oil_key, res in weathering_results.items():
        qc = res.get("qc", {})
        status = "PASS" if qc.get("passed", True) else "FAIL"
        print(
            f"  [{res['display_name']}] {status} "
            f"(max deviation {qc.get('max_deviation_pct', 0):.2f} % "
            f"at hour {qc.get('worst_hour', '?')})"
        )
        if qc.get("failing_hours"):
            for hour, dev in qc["failing_hours"][:5]:
                print(f"     hour {hour}: deviation {dev:.2f} %")

    with open("config/oil.yaml", "r") as oil_file:
        oil_cfg = yaml.safe_load(oil_file)
    shore_cfg = oil_cfg.get("shoreline", {})
    if shore_cfg.get("enabled", False):
        print("\nShoreline Impact Analysis:")
        sim_cfg = oil_cfg.get("simulation", {})
        for oil_key, res in weathering_results.items():
            seg_df = run_shoreline_analysis(
                nc_path=res["nc_path"],
                initial_mass_tonnes=sim_cfg.get("initial_mass_tonnes", 50.0),
                segment_length_km=shore_cfg.get("segment_length_km", 1.0),
                segment_prefix=shore_cfg.get("segment_prefix", "PWN"),
            )
            if seg_df is not None and not seg_df.empty:
                csv_path = BASE_OUTPUT_DIR / "weathering" / f"shoreline_{oil_key}.csv"
                seg_df.to_csv(csv_path, index=False)
                print(f"  [{res['display_name']}] {len(seg_df)} segment(s) impacted -> {csv_path.name}")
                print(seg_df.to_string(index=False, max_rows=10))
            else:
                print(f"  [{res['display_name']}] No particles beached.")

    refined_result = run_refined_weathering(
        best_recipe=best_recipe,
        start_time=start_time_str,
        start_lat=start_lat,
        start_lon=start_lon,
        phase_label=phase_label,
        refined_stage_label=refined_stage_label,
    )
    if refined_result:
        r_df = refined_result["budget_df"]
        r_last = r_df.iloc[-1]
        r_qc = refined_result.get("qc", {})
        r_status = "PASS" if r_qc.get("passed", True) else "FAIL"
        print(f"\n{refined_stage_label} - Refined Oil Budget (at 72 h):")
        print(f"  [{refined_result['display_name']}]")
        print(f"    Surface:    {r_last['surface_pct']:.1f}%")
        print(f"    Evaporated: {r_last['evaporated_pct']:.1f}%")
        print(f"    Dispersed:  {r_last['dispersed_pct']:.1f}%")
        print(f"    Beached:    {r_last['beached_pct']:.1f}%")
        print(f"  QC: {r_status}  (max deviation {r_qc.get('max_deviation_pct', 0):.2f} %)")

    if include_gnome_comparison:
        print("\nRunning supplementary PyGNOME cross-comparison...")
        gnome_results = run_gnome_comparison(
            start_lat=start_lat,
            start_lon=start_lon,
            start_time=start_time_str,
            openoil_results=weathering_results,
        )
        if gnome_results:
            print(f"PyGNOME comparison complete. Charts saved to {BASE_OUTPUT_DIR}/gnome_comparison/")
        else:
            print("PyGNOME not available - skipping cross-comparison.")

    print(f"\n{phase_label} complete. Check {BASE_OUTPUT_DIR}/weathering/ for mass budget charts.")


def run_prototype_legacy_phase4_weathering(*, deprecated_alias: str | None = None):
    from src.core.case_context import get_case_context

    case = get_case_context()
    if case.workflow_mode != "prototype_2016":
        print("prototype_legacy_phase4_weathering is only supported for the prototype_2016 workflow.")
        sys.exit(1)

    if deprecated_alias:
        print(
            f"WARNING: PIPELINE_PHASE={deprecated_alias} is deprecated for prototype_2016. "
            "Use PIPELINE_PHASE=prototype_legacy_phase4_weathering instead."
        )
    print(
        "Legacy prototype 2016 thesis-facing flow is Phase 1 -> Phase 2 -> Phase 3A -> Phase 4 -> Phase 5. "
        "No thesis-facing Phase 3B or Phase 3C exists for this lane."
    )
    run_phase3(
        phase_label="Phase 4",
        refined_stage_label="Legacy refined oil appendix",
        include_gnome_comparison=False,
    )


def run_benchmark():
    from src.core.case_context import get_case_context
    from src.core.constants import RUN_NAME
    from src.exceptions.custom import BenchmarkCaseSkipped
    from src.services.benchmark import BenchmarkPipeline
    from src.utils.io import resolve_recipe_selection, resolve_spill_origin

    case = get_case_context()

    print("Starting Benchmark Pipeline...")
    print_workflow_context()

    selection = resolve_recipe_selection()
    best_recipe = selection.recipe
    ensure_prepared_inputs(
        RUN_NAME,
        recipe_name=best_recipe,
        require_drifter=case.drifter_required,
        phase_label="benchmark phase",
    )
    print_recipe_selection(selection, label="Transport recipe")
    start_lat, start_lon, start_time_str = resolve_spill_origin()
    print(f"Best recipe  : {best_recipe}")
    print(f"Spill origin : {start_lat:.4f}, {start_lon:.4f} at {start_time_str}")

    try:
        BenchmarkPipeline().run(
            best_recipe=best_recipe,
            start_lat=start_lat,
            start_lon=start_lon,
            start_time=start_time_str,
        )
    except BenchmarkCaseSkipped as exc:
        emit_benchmark_skip_and_exit(exc)


def run_prototype_pygnome_similarity_summary_phase():
    from src.services.prototype_pygnome_similarity_summary import run_prototype_pygnome_similarity_summary
    from src.core.case_context import get_case_context

    case = get_case_context()
    print(f"Starting {case.workflow_mode} PyGNOME similarity summary...")
    if case.workflow_mode == "prototype_2021":
        print(
            "This phase is support-only and builds a consolidated transport benchmark summary "
            "from the configured accepted-segment 2021 deterministic OpenDrift-vs-deterministic PyGNOME outputs."
        )
    else:
        print(
            "This phase is legacy/debug Phase 3A only and builds a consolidated transport benchmark summary "
            "from the existing three 2016 OpenDrift deterministic, p50, and p90 tracks against deterministic PyGNOME."
        )

    results = run_prototype_pygnome_similarity_summary()

    print("\nPrototype PyGNOME similarity summary complete.")
    print(f"Outputs saved to: {results['output_dir']}")
    print(f"Case registry: {results['case_registry_csv']}")
    print(f"Similarity summary CSV: {results['similarity_by_case_csv']}")
    print(f"FSS by case/window CSV: {results['fss_by_case_window_csv']}")
    print(f"KL by case/hour CSV: {results['kl_by_case_hour_csv']}")
    print(f"Figure registry: {results['figure_registry_csv']}")
    print(f"Figure captions: {results['figure_captions_md']}")
    print(f"Manifest: {results['manifest_json']}")
    print(f"Summary: {results['summary_md']}")
    print(f"Single forecast figures: {results['single_figure_count']}")
    print(f"Comparison boards: {results['board_figure_count']}")
    print(f"Top-ranked case: {results['top_ranked_case_id']}")
    if results.get("top_ranked_comparison_track_id"):
        print(f"Top-ranked comparison track: {results['top_ranked_comparison_track_id']}")
    print(f"Top-ranked mean FSS @ 5 km: {results['top_ranked_mean_fss_5km']:.4f}")
    print(f"Top-ranked mean KL: {results['top_ranked_mean_kl']:.4f}")
    print(f"Cases summarized: {results['case_count']}")


def run_prototype_legacy_final_figures_phase():
    from src.services.prototype_legacy_final_figures import run_prototype_legacy_final_figures
    from src.core.case_context import get_case_context

    case = get_case_context()
    print(f"Starting {case.workflow_mode} final paper-figure export...")
    print(
        "This phase is read-only and redraws a curated legacy 2016 paper-figure set from existing "
        "Phase 2, benchmark, and prototype similarity outputs only."
    )

    results = run_prototype_legacy_final_figures()

    print("\nPrototype legacy final paper-figure export complete.")
    print(f"Outputs saved to: {results['output_dir']}")
    print(f"Manifest: {results['manifest_json']}")
    print(f"Missing figures CSV: {results['missing_figures_csv']}")
    print(f"Figure count: {results['figure_count']}")
    print(f"Missing figure count: {results['missing_figure_count']}")
    print(f"Typography: {results['font_family']}")


def run_prototype_2016_display_review_phase():
    from src.services.prototype_2016_display_review import run_prototype_2016_display_review
    from src.core.case_context import get_case_context

    case = get_case_context()
    print(f"Starting {case.workflow_mode} display-review export...")
    print(
        "This phase is read-only and regenerates improved review-only 2016 figures from existing "
        "Phase 2, benchmark, and prototype similarity outputs without rerunning ensemble or PyGNOME science."
    )

    results = run_prototype_2016_display_review()

    print("\nPrototype 2016 display-review export complete.")
    print(f"Outputs saved to: {results['output_dir']}")
    print(f"Manifest: {results['manifest_json']}")
    print(f"Audit CSV: {results['audit_csv']}")
    print(f"Captions: {results['captions_md']}")
    print(f"Figure count: {results['figure_count']}")
    print(f"Missing figure count: {results['missing_figure_count']}")


def run_phase3b():
    from src.core.constants import BASE_OUTPUT_DIR, RUN_NAME
    from src.core.case_context import get_case_context
    from src.services.scoring import run_phase3b_scoring
    from src.utils.io import resolve_recipe_selection

    case = get_case_context()
    if case.workflow_mode == "prototype_2016":
        print(
            "WARNING: prototype_2016 no longer treats PIPELINE_PHASE=3b as a thesis-facing phase. "
            "This hidden compatibility path is appendix-only and outside the 2016 Phase 1 / 2 / 3A / 4 / 5 story."
        )
        print("Starting legacy appendix-only 3b scoring...")
    else:
        print("Starting Phase 3B public-observation validation...")
    print_workflow_context()

    selection = resolve_recipe_selection()
    best_recipe = selection.recipe
    ensure_prepared_inputs(
        RUN_NAME,
        recipe_name=best_recipe,
        phase_label="Phase 3B",
    )
    ensure_phase3b_forecast_outputs(
        RUN_NAME,
        recipe_name=best_recipe,
        phase_label="Phase 3B",
    )
    print_recipe_selection(selection, label="Transport recipe")

    output_dir = BASE_OUTPUT_DIR / ("phase3b" if case.is_official else "validation")
    output_dir.mkdir(parents=True, exist_ok=True)

    results = run_phase3b_scoring(output_dir=output_dir)
    print("\nPhase 3B complete.")
    print(f"FSS metrics saved to: {results.fss_by_date_window}")
    print(f"Summary saved to: {results.summary}")
    if getattr(results, "diagnostics", None):
        print(f"Diagnostics saved to: {results.diagnostics}")
    if getattr(results, "run_manifest", None):
        print(f"Run manifest saved to: {results.run_manifest}")


def run_recipe_sensitivity_phase():
    from src.core.case_context import get_case_context
    from src.core.constants import RUN_NAME
    from src.services.recipe_sensitivity import run_recipe_sensitivity
    from src.utils.io import get_recipe_sensitivity_output_dir, resolve_recipe_selection

    case = get_case_context()
    if not case.is_official:
        print("recipe_sensitivity is only supported for official spill-case workflows.")
        sys.exit(1)

    print("Starting official Mindoro event-scale Phase 3B recipe sensitivities...")
    print_workflow_context()

    selection = resolve_recipe_selection()
    ensure_prepared_inputs(
        RUN_NAME,
        recipe_name=selection.recipe,
        require_drifter=case.drifter_required,
        phase_label="recipe sensitivity phase",
    )
    print_recipe_selection(selection, label="Frozen historical baseline")

    results = run_recipe_sensitivity()
    output_dir = get_recipe_sensitivity_output_dir()
    print("\nRecipe sensitivity runs complete.")
    print(f"Outputs saved to: {output_dir}")
    print(f"Summary saved to: {results['artifacts']['summary_csv']}")
    print(f"By-window metrics saved to: {results['artifacts']['by_window_csv']}")
    print(f"Diagnostics saved to: {results['artifacts']['diagnostics_csv']}")
    print(f"Report saved to: {results['artifacts']['report_md']}")
    if results["artifacts"].get("overlay_png"):
        print(f"QA overlay saved to: {results['artifacts']['overlay_png']}")


def run_public_obs_appendix_phase():
    from src.core.case_context import get_case_context
    from src.core.constants import RUN_NAME
    from src.services.public_obs_appendix import run_public_obs_appendix
    from src.utils.io import resolve_recipe_selection

    case = get_case_context()
    if not case.is_official:
        print("public_obs_appendix is only supported for official spill-case workflows.")
        sys.exit(1)

    print("Starting appendix-only public observation expansion...")
    print_workflow_context()

    selection = resolve_recipe_selection()
    ensure_prepared_inputs(
        RUN_NAME,
        recipe_name=selection.recipe,
        require_drifter=case.drifter_required,
        phase_label="public observation appendix",
    )
    print_recipe_selection(selection, label="Frozen historical baseline")

    results = run_public_obs_appendix()
    print("\nPublic observation appendix complete.")
    print(f"Inventory CSV: {results['inventory_csv']}")
    print(f"Inventory JSON: {results['inventory_json']}")
    print(f"Accepted quantitative appendix dates: {', '.join(results['accepted_quantitative_dates'])}")
    print(f"Recommendation: {results['recommendation']}")


def run_march6_recovery_sensitivity_phase():
    from src.core.case_context import PROTOTYPE_CASE_ID_ENV, get_case_context
    from src.core.constants import RUN_NAME
    from src.services.march6_recovery_sensitivity import run_march6_recovery_sensitivity
    from src.utils.io import resolve_recipe_selection

    case = get_case_context()
    if not case.is_official:
        print("march6_recovery_sensitivity is only supported for official spill-case workflows.")
        sys.exit(1)

    print("Starting appendix-only March 6 recovery sensitivity matrix...")
    print_workflow_context()

    selection = resolve_recipe_selection()
    ensure_prepared_inputs(
        RUN_NAME,
        recipe_name=selection.recipe,
        require_drifter=case.drifter_required,
        phase_label="March 6 recovery sensitivity",
    )
    print_recipe_selection(selection, label="Frozen historical baseline")

    results = run_march6_recovery_sensitivity()
    print("\nMarch 6 recovery sensitivity complete.")
    print(f"Outputs saved to: {results['output_dir']}")
    print(f"Matrix CSV: {results['matrix_csv']}")
    print(f"Summary CSV: {results['summary_csv']}")
    print(f"Report: {results['report_md']}")
    print(f"Run manifest: {results['run_manifest_json']}")
    best_r1 = results.get("best_r1_candidate") or {}
    best_overall = results.get("best_overall_candidate") or {}
    if best_r1:
        print(
            "Best R1 candidate: "
            f"{best_r1.get('threshold_label')} @ {best_r1.get('buffer_km')} km "
            f"(mean FSS={best_r1.get('mean_fss')})"
        )
    if best_overall:
        print(
            "Best overall candidate: "
            f"{best_overall.get('branch_id')} + {best_overall.get('threshold_label')} "
            f"@ {best_overall.get('buffer_km')} km (mean FSS={best_overall.get('mean_fss')})"
        )


def run_convergence_after_shoreline_phase():
    from src.core.case_context import get_case_context
    from src.core.constants import RUN_NAME
    from src.services.convergence_after_shoreline import run_convergence_after_shoreline

    case = get_case_context()
    if not case.is_official:
        print("convergence_after_shoreline is only supported for official spill-case workflows.")
        sys.exit(1)

    print("Starting shoreline-aware particle-count convergence...")
    print_workflow_context()

    ensure_prepared_inputs(
        RUN_NAME,
        require_drifter=case.drifter_required,
        include_all_transport_forcing=True,
        phase_label="shoreline-aware convergence",
    )

    results = run_convergence_after_shoreline()
    print("\nShoreline-aware convergence complete.")
    print(f"Summary saved to: {results['summary_csv']}")
    print(f"By-window metrics saved to: {results['by_window_csv']}")
    print(f"Diagnostics saved to: {results['diagnostics_csv']}")
    print(f"Report saved to: {results['report_md']}")
    print(f"Run manifest saved to: {results['run_manifest_json']}")
    if results.get("qa_fss_png"):
        print(f"QA FSS plot: {results['qa_fss_png']}")
    if results.get("qa_nonzero_png"):
        print(f"QA nonzero plot: {results['qa_nonzero_png']}")
    if results.get("qa_overlays_png"):
        print(f"QA overlays: {results['qa_overlays_png']}")


def run_displacement_after_convergence_phase():
    from src.core.case_context import get_case_context
    from src.services.displacement_after_convergence import run_displacement_after_convergence

    case = get_case_context()
    if not case.is_official:
        print("displacement_after_convergence is only supported for official spill-case workflows.")
        sys.exit(1)

    print("Starting post-convergence displacement/transport audit...")
    print_workflow_context()

    results = run_displacement_after_convergence()
    print("\nPost-convergence displacement audit complete.")
    print(f"Report saved to: {results['report_md']}")
    print(f"Audit JSON saved to: {results['audit_json']}")
    print(f"Ranked hypotheses saved to: {results['ranked_hypotheses_csv']}")
    print(f"Top hypothesis: {results['top_hypothesis']}")
    print(f"Recommended next rerun: {results['recommended_next_rerun']}")
    if results.get("qa_overlay_png"):
        print(f"QA overlay: {results['qa_overlay_png']}")


def run_phase3b_multidate_public_phase():
    from src.core.case_context import get_case_context
    from src.services.phase3b_multidate_public import run_phase3b_multidate_public

    case = get_case_context()
    if not case.is_official:
        print("phase3b_multidate_public is only supported for official spill-case workflows.")
        sys.exit(1)

    print("Starting formal multi-date public-observation Phase 3B validation...")
    print_workflow_context()

    results = run_phase3b_multidate_public()
    print("\nMulti-date public Phase 3B complete.")
    print(f"Outputs saved to: {results['output_dir']}")
    print(f"Accepted validation dates: {', '.join(results['accepted_validation_dates']) or 'none'}")
    print(f"March 3 excluded from forecast skill summary: {results['march3_excluded']}")
    print(f"Strict March 6 files unchanged: {results['strict_files_unchanged']}")
    print(f"Summary saved to: {results['summary']}")
    print(f"Event-corridor summary saved to: {results['eventcorridor_summary']}")


def run_phase3b_extended_public_phase():
    from src.core.case_context import get_case_context
    from src.services.phase3b_extended_public import run_phase3b_extended_public

    case = get_case_context()
    if not case.is_official:
        print("phase3b_extended_public is only supported for official spill-case workflows.")
        sys.exit(1)

    print("Starting extended-horizon public-observation Phase 3B guardrail...")
    print_workflow_context()

    results = run_phase3b_extended_public()
    print("\nExtended public Phase 3B guardrail complete.")
    print(f"Outputs saved to: {results['output_dir']}")
    print(
        "Accepted extended quantitative dates: "
        f"{', '.join(results['accepted_extended_quantitative_dates']) or 'none'}"
    )
    print(f"Status: {results['status']}")
    print(f"Headline FSS: {results['headline_fss']}")
    print(f"Not-possible report: {results['not_possible_report']}")
    print(f"Run manifest: {results['run_manifest']}")


def run_phase3b_extended_public_scored_phase():
    from src.core.case_context import get_case_context
    from src.exceptions.custom import ForcingOutagePhaseSkipped
    from src.services.phase3b_extended_public_scored import run_phase3b_extended_public_scored

    case = get_case_context()
    if not case.is_official:
        print("phase3b_extended_public_scored is only supported for official spill-case workflows.")
        sys.exit(1)

    print("Starting appendix-only scored short extended public-observation validation...")
    print_workflow_context()

    try:
        results = run_phase3b_extended_public_scored()
    except ForcingOutagePhaseSkipped as exc:
        emit_forcing_outage_skip_and_exit(exc)
    print("\nShort extended public-observation scoring complete.")
    print(f"Outputs saved to: {results['output_dir']}")
    print(f"Accepted short-tier dates scored: {', '.join(results['accepted_short_dates_scored']) or 'none'}")
    print(f"Forcing manifest: {results['forcing_manifest_json']}")
    print(f"Loading audit: {results['loading_audit_json']}")
    print(f"Summary: {results['summary_csv']}")
    print(f"Per-window FSS: {results['fss_csv']}")
    print(f"Diagnostics: {results['diagnostics_csv']}")
    print(f"Event-corridor summary: {results['eventcorridor_summary_md']}")
    print(f"Forcing extension worked cleanly: {results['forcing_extension_clean']}")
    print(f"Medium tier recommendation: {results['medium_tier_recommendation']}")


def run_phase3b_extended_public_scored_march23_phase():
    from src.core.case_context import get_case_context
    from src.exceptions.custom import ForcingOutagePhaseSkipped
    from src.services.phase3b_extended_public_scored_march23 import run_phase3b_extended_public_scored_march23

    case = get_case_context()
    if not case.is_official:
        print("phase3b_extended_public_scored_march23 is only supported for official spill-case workflows.")
        sys.exit(1)

    print("Starting appendix-only March 23 extended public branch stress test...")
    print_workflow_context()

    try:
        results = run_phase3b_extended_public_scored_march23()
    except ForcingOutagePhaseSkipped as exc:
        emit_forcing_outage_skip_and_exit(exc)
    print("\nMarch 23 extended public branch stress test complete.")
    print(f"Outputs saved to: {results['output_dir']}")
    print(f"Accepted March 23 source key: {results['accepted_source_key']}")
    print(f"Summary: {results['summary_csv']}")
    print(f"Per-window FSS: {results['fss_csv']}")
    print(f"Diagnostics: {results['diagnostics_csv']}")
    print(f"Branch survival summary: {results['branch_survival_csv']}")
    print(f"Decision note: {results['decision_note_md']}")
    print(f"Run manifest: {results['run_manifest_json']}")


def run_phase3b_extended_public_scored_march13_14_reinit_phase():
    from src.core.case_context import get_case_context
    from src.exceptions.custom import ForcingOutagePhaseSkipped
    from src.services.phase3b_extended_public_scored_march13_14_reinit import (
        run_phase3b_extended_public_scored_march13_14_reinit,
    )

    case = get_case_context()
    if not case.is_official:
        print("phase3b_extended_public_scored_march13_14_reinit is only supported for official spill-case workflows.")
        sys.exit(1)

    print("Starting Mindoro Phase 3B March 13 -> March 14 primary public-validation rerun...")
    print_workflow_context()

    try:
        results = run_phase3b_extended_public_scored_march13_14_reinit()
    except ForcingOutagePhaseSkipped as exc:
        emit_forcing_outage_skip_and_exit(exc)
    print("\nMindoro Phase 3B March 13 -> March 14 primary public-validation rerun complete.")
    print(f"Outputs saved to: {results['output_dir']}")
    print(f"Start source key: {results['start_source_key']}")
    print(f"Target source key: {results['target_source_key']}")
    print(f"Summary: {results['summary_csv']}")
    print(f"Per-window FSS: {results['fss_csv']}")
    print(f"Diagnostics: {results['diagnostics_csv']}")
    print(f"Branch survival summary: {results['branch_survival_csv']}")
    print(f"Decision note: {results['decision_note_md']}")
    print(f"Run manifest: {results['run_manifest_json']}")


def run_phase3b_extended_public_scored_march13_14_reinit_pygnome_comparison_phase():
    from src.core.case_context import get_case_context
    from src.services.phase3b_extended_public_scored_march13_14_reinit_pygnome_comparison import (
        run_phase3b_extended_public_scored_march13_14_reinit_pygnome_comparison,
    )

    case = get_case_context()
    if not case.is_official:
        print(
            "phase3b_extended_public_scored_march13_14_reinit_pygnome_comparison is only supported for official spill-case workflows."
        )
        sys.exit(1)

    print("Starting Mindoro Phase 3A March 13 -> March 14 comparator-only cross-model bundle...")
    print_workflow_context()

    results = run_phase3b_extended_public_scored_march13_14_reinit_pygnome_comparison()
    print("\nMindoro Phase 3A March 13 -> March 14 comparator-only cross-model bundle complete.")
    print(f"Outputs saved to: {results['output_dir']}")
    print(f"Start source key: {results['start_source_key']}")
    print(f"Target source key: {results['target_source_key']}")
    print(f"Summary: {results['summary_csv']}")
    print(f"Per-window FSS: {results['fss_csv']}")
    print(f"Diagnostics: {results['diagnostics_csv']}")
    print(f"Tracks registry: {results['tracks_registry_csv']}")
    print(f"Ranking: {results['ranking_csv']}")
    print(f"Top-ranked track: {results['winner_track_id']} ({results['winner_model_name']})")
    print(f"Decision note: {results['memo_md']}")
    print(f"Run manifest: {results['run_manifest_json']}")


def run_horizon_survival_audit_phase():
    from src.core.case_context import get_case_context
    from src.services.horizon_survival_audit import run_horizon_survival_audit

    case = get_case_context()
    if not case.is_official:
        print("horizon_survival_audit is only supported for official spill-case workflows.")
        sys.exit(1)

    print("Starting read-only short-extended horizon survival audit...")
    print_workflow_context()

    results = run_horizon_survival_audit()
    print("\nHorizon survival audit complete.")
    print(f"Outputs saved to: {results['output_dir']}")
    print(f"Dominant diagnosis class: {results['dominant_diagnosis_class']}")
    print(f"Last nonzero deterministic footprint: {results['last_nonzero_deterministic_footprint'] or 'none'}")
    print(f"Last nonzero prob_presence: {results['last_nonzero_prob_presence'] or 'none'}")
    print(f"Last nonzero mask_p50: {results['last_nonzero_mask_p50'] or 'none'}")
    print(f"March 7-9 empty reason: {results['march7_9_empty_reason']}")
    print(f"Recommended next rerun: {results['recommended_next_rerun']}")
    print(f"Report: {results['report_md']}")


def run_transport_retention_fix_phase():
    from src.core.case_context import get_case_context
    from src.services.transport_retention_fix import run_transport_retention_fix

    case = get_case_context()
    if not case.is_official:
        print("transport_retention_fix is only supported for official spill-case workflows.")
        sys.exit(1)

    print("Starting transport-retention sensitivity diagnostics...")
    print_workflow_context()

    results = run_transport_retention_fix()
    table = results["scenario_table"]
    print("\nTransport-retention sensitivity complete.")
    print(f"Outputs saved to: {results['output_dir']}")
    print(f"Best scenario: {results['best_scenario']}")
    print(f"Coastline interaction confirmed: {results['coastline_interaction_confirmed']}")
    print(f"Medium tier should remain blocked: {results['medium_tier_should_remain_blocked']}")
    print(f"Recommended next step: {results['recommended_next_step']}")
    print("\nScenario comparison:")
    columns = [
        "scenario_id",
        "coastline_action",
        "last_raw_active_time_utc",
        "last_nonzero_prob_presence_utc",
        "last_nonzero_mask_p50_utc",
        "strict_march6_fss_1km",
        "strict_march6_fss_3km",
        "strict_march6_fss_5km",
        "strict_march6_fss_10km",
        "eventcorridor_fss_1km",
        "eventcorridor_fss_3km",
        "eventcorridor_fss_5km",
        "eventcorridor_fss_10km",
    ]
    print(table[columns].to_string(index=False))
    print(f"Report: {results['report_md']}")


def run_official_rerun_r1_phase():
    from src.core.case_context import get_case_context
    from src.services.official_rerun_r1 import run_official_rerun_r1

    case = get_case_context()
    if not case.is_official:
        print("official_rerun_r1 is only supported for official spill-case workflows.")
        sys.exit(1)

    print("Starting selected R1 official rerun/rescore pack...")
    print_workflow_context()

    results = run_official_rerun_r1()
    strict = results["strict_march6"]
    event = results["eventcorridor"]
    print("\nOfficial R1 rerun/rescore complete.")
    print(f"Outputs saved to: {results['output_dir']}")
    print(f"Selected scenario: {results['selected_scenario']}")
    print(f"Retained from transport_retention_fix: {results['retained_from_transport_retention_fix']}")
    print(
        "Strict March 6 FSS 1/3/5/10 km: "
        f"{strict['fss_1km']}, {strict['fss_3km']}, {strict['fss_5km']}, {strict['fss_10km']}"
    )
    print(f"March 6 p50 nonzero cells: {strict['forecast_nonzero_cells']}")
    print(f"March 6 max probability: {strict['max_probability']}")
    print(f"Last active time: {strict['last_raw_active_time_utc']}")
    print(
        "Short extended event-corridor FSS 1/3/5/10 km: "
        f"{event['fss_1km']}, {event['fss_3km']}, {event['fss_5km']}, {event['fss_10km']}"
    )
    print(f"Recommended next branch: {results['recommended_next_branch']}")
    print(f"Before/after table: {results['before_after_csv']}")
    print(f"Report: {results['report_md']}")


def run_init_mode_sensitivity_r1_phase():
    from src.core.case_context import get_case_context
    from src.services.init_mode_sensitivity_r1 import run_init_mode_sensitivity_r1

    case = get_case_context()
    if not case.is_official:
        print("init_mode_sensitivity_r1 is only supported for official spill-case workflows.")
        sys.exit(1)

    print("Starting R1 initialization-mode sensitivity...")
    print_workflow_context()

    results = run_init_mode_sensitivity_r1()
    summary = results["summary"]
    strict = summary[summary["pair_role"] == "strict_march6"]
    event = summary[summary["pair_role"] == "eventcorridor_march4_6"]
    print("\nInitialization-mode sensitivity complete.")
    print(f"Outputs saved to: {results['output_dir']}")
    print("Selected transport-retention scenario: R1")
    print("\nStrict March 6 FSS:")
    print(strict[["branch_id", "initialization_mode", "forecast_nonzero_cells", "fss_1km", "fss_3km", "fss_5km", "fss_10km", "last_raw_active_time_utc"]].to_string(index=False))
    print("\nMarch 4-6 event-corridor FSS:")
    print(event[["branch_id", "initialization_mode", "forecast_nonzero_cells", "fss_1km", "fss_3km", "fss_5km", "fss_10km", "last_raw_active_time_utc"]].to_string(index=False))
    print(f"\nRecommended initialization strategy: {results['recommendation']['recommended_initialization_strategy']}")
    print(f"A2 worth attempting next: {results['recommendation']['a2_source_history_reconstruction_worth_attempting']}")
    print(f"Report: {results['report_md']}")


def run_source_history_reconstruction_r1_phase():
    from src.core.case_context import get_case_context
    from src.exceptions.custom import ForcingOutagePhaseSkipped
    from src.services.source_history_reconstruction_r1 import run_source_history_reconstruction_r1

    case = get_case_context()
    if not case.is_official:
        print("source_history_reconstruction_r1 is only supported for official spill-case workflows.")
        sys.exit(1)

    print("Starting R1 source-history reconstruction sensitivity...")
    print_workflow_context()

    try:
        results = run_source_history_reconstruction_r1()
    except ForcingOutagePhaseSkipped as exc:
        emit_forcing_outage_skip_and_exit(exc)
    summary = results["summary"]
    strict = summary[summary["pair_role"] == "strict_march6"]
    event = summary[summary["pair_role"] == "eventcorridor_march4_6"]
    checkpoint = summary[summary["pair_role"] == "march3_reconstruction_checkpoint"]
    print("\nSource-history reconstruction sensitivity complete.")
    print(f"Outputs saved to: {results['output_dir']}")
    print(f"Best A2 scenario: {results['recommendation']['best_a2_scenario']}")
    print("\nStrict March 6 FSS:")
    print(strict[["scenario_id", "release_duration_hours", "forecast_nonzero_cells", "fss_1km", "fss_3km", "fss_5km", "fss_10km", "last_raw_active_time_utc"]].to_string(index=False))
    print("\nMarch 4-6 event-corridor FSS:")
    print(event[["scenario_id", "release_duration_hours", "forecast_nonzero_cells", "fss_1km", "fss_3km", "fss_5km", "fss_10km", "last_raw_active_time_utc"]].to_string(index=False))
    print("\nMarch 3 checkpoint FSS:")
    print(checkpoint[["scenario_id", "release_duration_hours", "forecast_nonzero_cells", "fss_1km", "fss_3km", "fss_5km", "fss_10km"]].to_string(index=False))
    print(f"\nRecommendation: {results['recommendation']['recommendation']}")
    print(f"Convergence should be next: {results['recommendation']['convergence_should_be_next']}")
    print(f"Report: {results['report_md']}")


def run_pygnome_public_comparison_phase():
    from src.core.case_context import get_case_context
    from src.services.pygnome_public_comparison import run_pygnome_public_comparison

    case = get_case_context()
    if not case.is_official:
        print("pygnome_public_comparison is only supported for official spill-case workflows.")
        sys.exit(1)

    print("Starting PyGNOME/OpenDrift public-observation comparison...")
    print_workflow_context()

    results = run_pygnome_public_comparison()
    ranking = results["ranking"]
    summary = results["summary"]
    strict = summary[summary["pair_role"] == "strict_march6"].copy()
    event = summary[summary["pair_role"] == "eventcorridor_march4_6"].copy()

    print("\nPyGNOME/OpenDrift public-observation comparison complete.")
    print(f"Outputs saved to: {results['output_dir']}")
    print("\nStrict March 6 ranking inputs:")
    print(
        strict[
            [
                "track_id",
                "model_name",
                "forecast_nonzero_cells",
                "obs_nonzero_cells",
                "nearest_distance_to_obs_m",
                "iou",
                "dice",
                "fss_1km",
                "fss_3km",
                "fss_5km",
                "fss_10km",
            ]
        ].to_string(index=False)
    )
    print("\nMarch 4-6 event-corridor ranking inputs:")
    print(
        event[
            [
                "track_id",
                "model_name",
                "forecast_nonzero_cells",
                "obs_nonzero_cells",
                "nearest_distance_to_obs_m",
                "iou",
                "dice",
                "fss_1km",
                "fss_3km",
                "fss_5km",
                "fss_10km",
            ]
        ].to_string(index=False)
    )
    print("\nModel ranking table:")
    print(ranking.to_string(index=False))
    print(f"\nRecommendation: {results['recommendation']['recommendation']}")
    print(f"Report memo: {results['memo']}")


def run_ensemble_threshold_sensitivity_phase():
    from src.core.case_context import get_case_context
    from src.services.ensemble_threshold_sensitivity import run_ensemble_threshold_sensitivity

    case = get_case_context()
    if not case.is_official:
        print("ensemble_threshold_sensitivity is only supported for official spill-case workflows.")
        sys.exit(1)

    print("Starting ensemble threshold sensitivity...")
    print_workflow_context()

    results = run_ensemble_threshold_sensitivity()
    ranking = results["calibration_ranking"]
    summary = results["summary"]
    selected_label = results["selected_threshold_label"]
    holdout = summary[
        (summary["pair_role"] == "strict_march6")
        & (summary["threshold_label"].astype(str) == selected_label)
    ]
    event = summary[summary["pair_role"] == "eventcorridor_march4_6"].copy()
    event["mean_fss"] = event.apply(
        lambda row: sum(float(row[f"fss_{window}km"]) for window in (1, 3, 5, 10)) / 4.0,
        axis=1,
    )

    print("\nEnsemble threshold sensitivity complete.")
    print(f"Outputs saved to: {results['output_dir']}")
    print("\nThreshold ranking by March 4-5 calibration mean FSS:")
    print(ranking.to_string(index=False))
    print(f"\nSelected threshold: {selected_label} ({results['selected_threshold']:.2f})")
    print("\nHoldout strict March 6 for selected threshold:")
    print(
        holdout[
            [
                "threshold_label",
                "forecast_nonzero_cells",
                "obs_nonzero_cells",
                "nearest_distance_to_obs_m",
                "iou",
                "dice",
                "fss_1km",
                "fss_3km",
                "fss_5km",
                "fss_10km",
            ]
        ].to_string(index=False)
    )
    print("\nMarch 4-6 event-corridor FSS by threshold:")
    print(
        event[
            [
                "threshold_label",
                "forecast_nonzero_cells",
                "obs_nonzero_cells",
                "iou",
                "dice",
                "fss_1km",
                "fss_3km",
                "fss_5km",
                "fss_10km",
                "mean_fss",
            ]
        ].sort_values("threshold_label").to_string(index=False)
    )
    print(f"\nBeats current p50: {results['recommendation']['eventcorridor_mean_fss_delta_vs_p50'] > 0}")
    print(f"Beats OpenDrift deterministic event corridor: {results['recommendation']['selected_beats_opendrift_deterministic_eventcorridor']}")
    print(f"Beats PyGNOME event corridor: {results['recommendation']['selected_beats_pygnome_eventcorridor']}")
    print(f"Recommendation: {results['recommendation']['recommendation']}")
    print(f"Next branch: {results['recommendation']['recommended_next_branch']}")
    print(f"Report: {results['report_md']}")


def run_recipe_sensitivity_r1_multibranch_phase():
    from src.core.case_context import get_case_context
    from src.services.recipe_sensitivity_r1_multibranch import run_recipe_sensitivity_r1_multibranch

    case = get_case_context()
    if not case.is_official:
        print("recipe_sensitivity_r1_multibranch is only supported for official spill-case workflows.")
        sys.exit(1)

    print("Starting R1 multibranch forcing-recipe sensitivity...")
    print_workflow_context()

    results = run_recipe_sensitivity_r1_multibranch()
    ranking = results["ranking"]
    summary = results["summary"].copy()
    summary["mean_fss"] = summary.apply(
        lambda row: sum(float(row[f"fss_{window}km"]) for window in (1, 3, 5, 10)) / 4.0,
        axis=1,
    )
    strict = summary[summary["pair_role"] == "strict_march6"].sort_values("mean_fss", ascending=False)
    event = summary[summary["pair_role"] == "eventcorridor_march4_6"].sort_values("mean_fss", ascending=False)

    print("\nR1 multibranch forcing-recipe sensitivity complete.")
    print(f"Outputs saved to: {results['output_dir']}")
    print("\nBest strict March 6 rows:")
    print(
        strict[
            [
                "track_id",
                "model_family",
                "recipe_id",
                "branch_id",
                "product_kind",
                "forecast_nonzero_cells",
                "obs_nonzero_cells",
                "nearest_distance_to_obs_m",
                "iou",
                "dice",
                "fss_1km",
                "fss_3km",
                "fss_5km",
                "fss_10km",
                "mean_fss",
            ]
        ].head(10).to_string(index=False)
    )
    print("\nBest March 4-6 event-corridor rows:")
    print(
        event[
            [
                "track_id",
                "model_family",
                "recipe_id",
                "branch_id",
                "product_kind",
                "forecast_nonzero_cells",
                "obs_nonzero_cells",
                "nearest_distance_to_obs_m",
                "iou",
                "dice",
                "fss_1km",
                "fss_3km",
                "fss_5km",
                "fss_10km",
                "mean_fss",
            ]
        ].head(10).to_string(index=False)
    )
    print("\nRanking table:")
    print(ranking.to_string(index=False))
    print(f"\nAny OpenDrift branch beats PyGNOME: {results['recommendation']['any_opendrift_branch_beats_pygnome']}")
    print(f"Recommendation: {results['recommendation']['recommendation']}")
    print(f"Next branch: {results['recommendation']['recommended_next_branch']}")
    print(f"Report: {results['report_md']}")


def run_phase3c_external_case_setup_phase():
    from src.core.case_context import get_case_context
    from src.services.phase3c_external_case_setup import run_phase3c_external_case_setup

    case = get_case_context()
    if case.workflow_mode != "dwh_retro_2010":
        print("phase3c_external_case_setup requires WORKFLOW_MODE=dwh_retro_2010.")
        sys.exit(1)

    print("Starting Phase 3C external rich-data spill setup...")
    print_workflow_context()

    results = run_phase3c_external_case_setup()

    print("\nPhase 3C external setup complete.")
    print(f"Outputs saved to: {results['output_dir']}")
    print(f"Proposed phase name: {results['phase_name']}")
    print(f"Proposed placement: {results['phase_placement']}")
    print(f"Projected scoring CRS: {results['projected_scoring_crs']}")
    print("\nSelected DWH daily layers:")
    for layer in [item for item in results["selected_dwh_layers"] if item["layer_id"] in (5, 6, 7, 8)]:
        print(
            f"  - layer {layer['layer_id']}: {layer['layer_name']} "
            f"({layer['event_date'] or 'no date'}, {layer['role']}) - {layer['why_selected']}"
        )
    source_layers = [item for item in results["selected_dwh_layers"] if item["layer_id"] == 0]
    for layer in source_layers:
        print(f"  - source provenance layer {layer['layer_id']}: {layer['layer_name']} - {layer['why_selected']}")
    print("\nSelected forcing services:")
    for service in results["selected_forcing_services"]:
        print(
            f"  - {service['forcing_component']}: {service['chosen_service']} "
            f"[{service['access_method']}; compatible={service['already_compatible_with_current_repo_readers']}]"
        )
    print(f"\nRecommended next implementation branch: {results['recommended_next_implementation_branch']}")
    print(f"Service inventory: {results['service_inventory_csv']}")
    print(f"Forcing manifest: {results['forcing_manifest_csv']}")
    print(f"Source taxonomy: {results['taxonomy_csv']}")
    print(f"Processing report: {results['processing_report_csv']}")
    print(f"Grid manifest: {results['grid_manifest_csv']}")
    print(f"Methodology memo: {results['methodology_memo']}")


def run_dwh_phase3c_smoke_phase():
    from src.core.case_context import get_case_context
    from src.services.dwh_phase3c_smoke import run_dwh_phase3c_smoke

    case = get_case_context()
    if case.workflow_mode != "dwh_retro_2010":
        print(f"dwh_phase3c_forcing_adapter_and_non_scientific_smoke_forecast requires WORKFLOW_MODE=dwh_retro_2010.")
        sys.exit(1)

    print("Starting DWH Phase 3C forcing-adapter status and non-scientific smoke forecast...")
    print_workflow_context()

    results = run_dwh_phase3c_smoke()
    sources = results["selected_sources"]

    print("\nDWH non-scientific smoke phase complete.")
    print(f"Outputs saved to: {results['output_dir']}")
    print("Selected current/wind/wave sources:")
    print(f"  - current: {sources['current_source']} [{sources['current_status']}]")
    print(f"  - wind: {sources['wind_source']} [{sources['wind_status']}]")
    print(f"  - wave/Stokes: {sources['wave_source']} [{sources['wave_status']}]")
    print(
        "Actual smoke forcing coverage: "
        f"{results['actual_forcing_coverage_start_utc']} to {results['actual_forcing_coverage_end_utc']}"
    )
    print(f"Waves attached: {results['waves_attached']}")
    print(f"Smoke forecast ran: {results['smoke_forecast_ran']}")
    print(f"Smoke score vs May 21 produced: {results['smoke_score_produced']}")
    print(f"Next step recommendation: {results['recommendation']}")
    print(f"Forcing adapter status: {results['adapter_status_csv']}")
    print(f"Prepared forcing manifest: {results['prepared_forcing_manifest_csv']}")
    print(f"Smoke loading audit: {results['loading_audit_csv']}")
    print(f"Smoke summary: {results['summary_csv']}")
    print(f"Smoke forecast manifest: {results['forecast_manifest']}")


def run_dwh_phase3c_scientific_forcing_ready_phase():
    from src.core.case_context import get_case_context
    from src.exceptions.custom import ForcingOutagePhaseSkipped
    from src.services.dwh_phase3c_scientific_forcing import run_dwh_phase3c_scientific_forcing_ready

    case = get_case_context()
    if case.workflow_mode != "dwh_retro_2010":
        print("dwh_phase3c_scientific_forcing_ready requires WORKFLOW_MODE=dwh_retro_2010.")
        sys.exit(1)

    print("Starting DWH Phase 3C scientific forcing readiness...")
    print_workflow_context()

    try:
        results = run_dwh_phase3c_scientific_forcing_ready()
    except ForcingOutagePhaseSkipped as exc:
        emit_forcing_outage_skip_and_exit(exc)
    sources = results["selected_sources"]

    print("\nDWH scientific forcing readiness complete.")
    print(f"Outputs saved to: {results['output_dir']}")
    print("Selected current/wind/wave sources:")
    for role in ("current", "wind", "wave"):
        source = sources.get(role) or {}
        print(
            f"  - {role}: {source.get('dataset_product_id', 'none')} "
            f"[scientific_ready={source.get('scientific_ready', False)}; "
            f"coverage={source.get('actual_start_time_coverage_utc', '')} to "
            f"{source.get('actual_end_time_coverage_utc', '')}]"
        )
    print(f"Waves attached: {results['waves_attached']}")
    print(f"Reader-check run succeeded: {results['reader_check_run_succeeded']}")
    print(f"Next step recommendation: {results['recommendation']}")
    print(f"Scientific forcing status: {results['status_csv']}")
    print(f"Scientific prepared forcing manifest: {results['prepared_forcing_manifest_csv']}")
    print(f"Scientific loading audit: {results['loading_audit_csv']}")
    print(f"Scientific reader-check report: {results['reader_check_report']}")


def run_phase3c_external_case_run_phase():
    from src.core.case_context import get_case_context
    from src.services.phase3c_external_case_run import run_phase3c_external_case_run

    case = get_case_context()
    if case.workflow_mode != "dwh_retro_2010":
        print("phase3c_external_case_run requires WORKFLOW_MODE=dwh_retro_2010.")
        sys.exit(1)

    print("Starting Phase 3C external rich-data spill transfer-validation run...")
    print_workflow_context()

    results = run_phase3c_external_case_run()

    print("\nDWH Phase 3C external case run complete.")
    print(f"Outputs saved to: {results['output_dir']}")
    print(f"Deterministic OpenDrift success: {results['deterministic_success']}")
    print(f"Ensemble success: {results['ensemble_success']}")
    print(f"PyGNOME comparator success: {results['pygnome_comparator_success']}")
    print("Headline per-date FSS:")
    for date, values in results["headline_fss"].items():
        print(
            f"  - {date}: "
            f"1km={values.get('fss_1km', float('nan')):.4f}, "
            f"3km={values.get('fss_3km', float('nan')):.4f}, "
            f"5km={values.get('fss_5km', float('nan')):.4f}, "
            f"10km={values.get('fss_10km', float('nan')):.4f}"
        )
    event = results["eventcorridor_fss"]
    print(
        "Headline May 21-23 event-corridor FSS: "
        f"1km={event.get('fss_1km', float('nan')):.4f}, "
        f"3km={event.get('fss_3km', float('nan')):.4f}, "
        f"5km={event.get('fss_5km', float('nan')):.4f}, "
        f"10km={event.get('fss_10km', float('nan')):.4f}"
    )
    print(f"Pairing manifest: {results['pairing_manifest_csv']}")
    print(f"FSS table: {results['fss_by_date_window_csv']}")
    print(f"Summary: {results['summary_csv']}")
    print(f"Event-corridor summary: {results['eventcorridor_summary_csv']}")
    print(f"Final recommendation: {results['recommendation']}")


def run_phase3c_external_case_ensemble_comparison_phase():
    from src.core.case_context import get_case_context
    from src.services.phase3c_external_case_ensemble_comparison import (
        run_phase3c_external_case_ensemble_comparison,
    )

    case = get_case_context()
    if case.workflow_mode != "dwh_retro_2010":
        print("phase3c_external_case_ensemble_comparison requires WORKFLOW_MODE=dwh_retro_2010.")
        sys.exit(1)

    print("Starting Phase 3C external deterministic-vs-ensemble comparison...")
    print_workflow_context()

    results = run_phase3c_external_case_ensemble_comparison()
    print("\nDWH Phase 3C ensemble comparison complete.")
    print(f"Outputs saved to: {results['output_dir']}")
    print(f"Deterministic success: {results['deterministic_success']}")
    print(f"Ensemble success: {results['ensemble_success']}")
    print("Headline per-date FSS:")
    for label, values in results["headline_fss"].items():
        print(f"  {label}:")
        for date, metrics in values.items():
            print(
                f"    - {date}: "
                f"1km={metrics.get('fss_1km', float('nan')):.4f}, "
                f"3km={metrics.get('fss_3km', float('nan')):.4f}, "
                f"5km={metrics.get('fss_5km', float('nan')):.4f}, "
                f"10km={metrics.get('fss_10km', float('nan')):.4f}"
            )
    print("Headline event-corridor FSS:")
    for label, metrics in results["eventcorridor_fss"].items():
        print(
            f"  - {label}: "
            f"1km={metrics.get('fss_1km', float('nan')):.4f}, "
            f"3km={metrics.get('fss_3km', float('nan')):.4f}, "
            f"5km={metrics.get('fss_5km', float('nan')):.4f}, "
            f"10km={metrics.get('fss_10km', float('nan')):.4f}"
        )
    print(f"Pairing manifest: {results['pairing_manifest_csv']}")
    print(f"FSS table: {results['fss_by_date_window_csv']}")
    print(f"Summary: {results['summary_csv']}")
    print(f"Event-corridor summary: {results['eventcorridor_summary_csv']}")
    print(f"Final recommendation: {results['recommendation']}")


def run_phase3c_dwh_pygnome_comparator_phase():
    from src.core.case_context import get_case_context
    from src.services.phase3c_dwh_pygnome_comparator import run_phase3c_dwh_pygnome_comparator

    case = get_case_context()
    if case.workflow_mode != "dwh_retro_2010":
        print("phase3c_dwh_pygnome_comparator requires WORKFLOW_MODE=dwh_retro_2010.")
        sys.exit(1)

    print("Starting Phase 3C DWH PyGNOME comparator...")
    print_workflow_context()

    results = run_phase3c_dwh_pygnome_comparator()
    print("\nDWH Phase 3C PyGNOME comparator complete.")
    print(f"Outputs saved to: {results['output_dir']}")
    print(f"Deterministic success: {results['deterministic_success']}")
    print(f"Ensemble comparison reused: {results['ensemble_comparison_reused']}")
    print(f"PyGNOME comparator success: {results['pygnome_comparator_success']}")
    print("Headline per-date FSS:")
    for label, values in results["headline_fss"].items():
        print(f"  {label}:")
        for date, metrics in values.items():
            print(
                f"    - {date}: "
                f"1km={metrics.get('fss_1km', float('nan')):.4f}, "
                f"3km={metrics.get('fss_3km', float('nan')):.4f}, "
                f"5km={metrics.get('fss_5km', float('nan')):.4f}, "
                f"10km={metrics.get('fss_10km', float('nan')):.4f}"
            )
    print("Headline May 21-23 event-corridor FSS:")
    for label, metrics in results["eventcorridor_fss"].items():
        print(
            f"  - {label}: "
            f"1km={metrics.get('fss_1km', float('nan')):.4f}, "
            f"3km={metrics.get('fss_3km', float('nan')):.4f}, "
            f"5km={metrics.get('fss_5km', float('nan')):.4f}, "
            f"10km={metrics.get('fss_10km', float('nan')):.4f}"
        )
    print(f"Pairing manifest: {results['pairing_manifest_csv']}")
    print(f"FSS table: {results['fss_by_date_window_csv']}")
    print(f"Summary: {results['summary_csv']}")
    print(f"Event-corridor summary: {results['eventcorridor_summary_csv']}")
    print(f"Final recommendation: {results['recommendation']}")


def run_final_validation_package_phase():
    from src.services.final_validation_package import run_final_validation_package

    print("Starting final validation packaging...")
    print("This phase is read-only and will not rerun the completed scientific workflows.")

    results = run_final_validation_package()
    headlines = results["headlines"]

    print("\nFinal validation package complete.")
    print(f"Outputs saved to: {results['output_dir']}")
    print("Recommended final chapter structure:")
    for item in results["recommended_final_chapter_structure"]:
        print(f"  - {item}")
    print("Headline Mindoro promoted primary result:")
    strict = headlines["mindoro_primary_reinit"]
    promotion = results["mindoro_primary_validation_promotion"]
    confirmation = promotion.get("dual_provenance_confirmation") or {}
    print(
        f"  - FSS(1/3/5/10 km) = {strict['fss_1km']:.4f}, {strict['fss_3km']:.4f}, "
        f"{strict['fss_5km']:.4f}, {strict['fss_10km']:.4f}"
    )
    print(f"  - Thesis-facing title: {promotion.get('thesis_phase_title', '')}")
    if confirmation:
        print(
            "  - Recipe confirmation: "
            f"stored={confirmation.get('stored_run_selected_recipe', '')}, "
            f"later_drifter_rerun={confirmation.get('posthoc_phase1_confirmation_selected_recipe', '')}, "
            f"matches={confirmation.get('matches_stored_b1_recipe', False)}"
        )
    support = headlines["mindoro_legacy_broader_support"]
    print("Headline Mindoro legacy broader-support result:")
    print(
        f"  - FSS(1/3/5/10 km) = {support['fss_1km']:.4f}, {support['fss_3km']:.4f}, "
        f"{support['fss_5km']:.4f}, {support['fss_10km']:.4f}"
    )
    legacy_sparse = headlines["mindoro_legacy_march6"]
    print("Headline Mindoro legacy sparse March 6 result:")
    print(
        f"  - FSS(1/3/5/10 km) = {legacy_sparse['fss_1km']:.4f}, {legacy_sparse['fss_3km']:.4f}, "
        f"{legacy_sparse['fss_5km']:.4f}, {legacy_sparse['fss_10km']:.4f}"
    )
    dwh_det = headlines["dwh_deterministic_event"]
    print("Headline DWH deterministic result:")
    print(
        f"  - FSS(1/3/5/10 km) = {dwh_det['fss_1km']:.4f}, {dwh_det['fss_3km']:.4f}, "
        f"{dwh_det['fss_5km']:.4f}, {dwh_det['fss_10km']:.4f}"
    )
    dwh_p50 = headlines["dwh_ensemble_p50_event"]
    dwh_p90 = headlines["dwh_ensemble_p90_event"]
    print("Headline DWH ensemble result:")
    print(
        f"  - p50 event-corridor FSS(1/3/5/10 km) = {dwh_p50['fss_1km']:.4f}, {dwh_p50['fss_3km']:.4f}, "
        f"{dwh_p50['fss_5km']:.4f}, {dwh_p50['fss_10km']:.4f}"
    )
    print(
        f"  - p90 event-corridor FSS(1/3/5/10 km) = {dwh_p90['fss_1km']:.4f}, {dwh_p90['fss_3km']:.4f}, "
        f"{dwh_p90['fss_5km']:.4f}, {dwh_p90['fss_10km']:.4f}"
    )
    dwh_py = headlines["dwh_pygnome_event"]
    print("Headline DWH PyGNOME-comparison result:")
    print(
        f"  - FSS(1/3/5/10 km) = {dwh_py['fss_1km']:.4f}, {dwh_py['fss_3km']:.4f}, "
        f"{dwh_py['fss_5km']:.4f}, {dwh_py['fss_10km']:.4f}"
    )
    print(f"Final recommendation: {results['final_recommendation']}")
    print(f"Main table: {results['artifacts']['final_validation_main_table']}")
    print(f"Summary memo: {results['artifacts']['final_validation_summary']}")
    print(f"Curated B1 final-output export: {results['phase3b_march13_14_final_output']['output_dir']}")
    print(f"Curated B1 final-output manifest: {results['artifacts']['phase3b_march13_14_final_output_manifest']}")


def run_phase1_finalization_audit_phase():
    from src.services.phase1_finalization_audit import run_phase1_finalization_audit

    print("Starting Phase 1 finalization audit...")
    print("This phase is read-only and will not rerun the expensive 2016-2022 regional transport study.")
    print("Existing Mindoro, DWH, and final validation outputs will not be overwritten.")

    results = run_phase1_finalization_audit()
    verdict = results["overall_verdict"]

    print("\nPhase 1 finalization audit complete.")
    print(f"Outputs saved to: {results['output_dir']}")
    print(f"Status CSV: {results['status_csv']}")
    print(f"Status JSON: {results['status_json']}")
    print(f"Memo: {results['memo_md']}")
    print(f"Verdict: {results['verdict_md']}")
    print(f"Phase 1 architecture scientifically ready: {verdict['scientifically_ready']}")
    print(f"Full production rerun still needed: {verdict['full_production_rerun_required']}")
    print(f"Biggest remaining blocker: {verdict['biggest_remaining_blocker']}")


def run_phase2_finalization_audit_phase():
    from src.services.phase2_finalization_audit import run_phase2_finalization_audit

    print("Starting Phase 2 finalization audit...")
    print("This phase is read-only and will not rerun the expensive official Mindoro forecast path.")
    print("Existing Mindoro, DWH, and final validation outputs will not be overwritten.")

    results = run_phase2_finalization_audit()
    verdict = results["overall_verdict"]

    print("\nPhase 2 finalization audit complete.")
    print(f"Outputs saved to: {results['output_dir']}")
    print(f"Status CSV: {results['status_csv']}")
    print(f"Status JSON: {results['status_json']}")
    print(f"Memo: {results['memo_md']}")
    print(f"Output catalog: {results['output_catalog_csv']}")
    print(f"Verdict: {results['verdict_md']}")
    print(f"Phase 2 scientifically usable as implemented: {verdict['scientifically_usable_as_implemented']}")
    print(f"Phase 2 scientifically frozen: {verdict['scientifically_frozen']}")
    print(
        "Phase 1 freeze remains incomplete "
        f"(candidate baseline not yet adopted as default): {verdict['requires_phase1_production_rerun_for_full_freeze']}"
    )
    print(f"Legacy recipe drift still leaks into official mode: {verdict['legacy_recipe_drift_leaks_into_official_mode']}")
    print(f"Biggest remaining provisional item: {verdict['biggest_remaining_phase2_provisional_item']}")


def run_phase1_production_rerun_phase():
    from src.core.case_context import get_case_context
    from src.exceptions.custom import ForcingOutagePhaseSkipped
    from src.services.phase1_production_rerun import run_phase1_production_rerun

    case = get_case_context()
    print("Starting dedicated Phase 1 regional production rerun...")
    print(
        f"This is an expensive scientific rerun for the {case.description.lower()}. "
        "It stages a candidate baseline artifact only and will not overwrite config/phase1_baseline_selection.yaml."
    )

    try:
        results = run_phase1_production_rerun()
    except ForcingOutagePhaseSkipped as exc:
        emit_forcing_outage_skip_and_exit(exc)

    print("\nPhase 1 production rerun complete.")
    print(f"Outputs saved to: {results['output_dir']}")
    print(f"Accepted segments: {results['accepted_segment_count']}")
    print(f"Rejected segments: {results['rejected_segment_count']}")
    print(f"Ranking-subset segments: {results['ranking_subset_segment_count']}")
    print(f"Winning recipe: {results['winning_recipe']}")
    print(f"GFS-capable recipes really ran: {results['gfs_capable_recipes_ran']}")
    print(f"Forcing outage policy: {results['forcing_outage_policy']}")
    print(f"Degraded continuation used: {results['degraded_continue_used']}")
    print(f"Rerun required: {results['rerun_required']}")
    if results.get("missing_forcing_factors"):
        print(f"Missing forcing factors: {', '.join(results['missing_forcing_factors'])}")
    if results.get("skipped_recipe_ids"):
        print(f"Skipped recipes: {', '.join(results['skipped_recipe_ids'])}")
    print(f"Drifter registry: {results['drifter_registry_csv']}")
    print(f"Ranking-subset registry: {results['ranking_subset_registry_csv']}")
    print(f"Ranking-subset report: {results['ranking_subset_report_md']}")
    print(f"Loading audit: {results['loading_audit_csv']}")
    print(f"Segment metrics: {results['segment_metrics_csv']}")
    print(f"Recipe summary: {results['recipe_summary_csv']}")
    print(f"Recipe ranking: {results['recipe_ranking_csv']}")
    print(f"Candidate baseline artifact: {results['candidate_baseline_path']}")
    print("Next manual follow-ups only:")
    print("  - Trial downstream runs with BASELINE_SELECTION_PATH pointed at the candidate artifact")
    print("  - Run phase1_audit manually if you want the read-only audit bundle refreshed")
    print("  - Run phase5_sync manually if you want launcher/docs/package outputs refreshed")


def run_mindoro_march13_14_phase1_focus_trial_phase():
    from src.core.case_context import get_case_context
    from src.services.mindoro_march13_14_phase1_focus_trial import (
        run_mindoro_march13_14_phase1_focus_trial,
    )

    case = get_case_context()
    if case.workflow_mode != "mindoro_retro_2023" or not case.is_official:
        print("mindoro_march13_14_phase1_focus_trial requires WORKFLOW_MODE=mindoro_retro_2023.")
        sys.exit(1)

    print("Starting experimental Mindoro March 13 -> March 14 Phase 1 focus trial...")
    print(
        "This phase uses a staged Mindoro-focused pre-spill Phase 1 candidate baseline, reruns only the "
        "March 13 -> March 14 OpenDrift lane into a separate non-canonical directory, and compares it "
        "against the stored canonical B1 result."
    )
    print("The canonical config/phase1_baseline_selection.yaml and canonical B1 outputs remain unchanged.")

    results = run_mindoro_march13_14_phase1_focus_trial()

    print("\nExperimental Mindoro March 13 -> March 14 Phase 1 focus trial complete.")
    print(f"Outputs saved to: {results['output_dir']}")
    print(f"Trial summary: {results['trial_summary_csv']}")
    print(f"Comparison CSV: {results['comparison_csv']}")
    print(f"Comparison report: {results['report_md']}")
    print(f"Manifest: {results['manifest_json']}")
    print(f"Canonical recipe: {results['canonical_recipe']}")
    print(f"Experimental recipe: {results['experimental_recipe']}")
    print(f"Comparison verdict: {results['comparison_verdict']}")


def run_mindoro_local_recipe_experiment_phase():
    from src.services.mindoro_local_recipe_experiment import run_mindoro_local_recipe_experiment

    print("Starting Mindoro-local recipe experiment...")
    print(
        "This workflow stages a Mindoro-local candidate baseline from existing Phase 1 outputs, "
        "prepares missing GFS wind forcing for the official Mindoro case if needed, and reruns the "
        "Mindoro event-scale recipe comparison in a separate experiment directory."
    )
    print("The canonical config/phase1_baseline_selection.yaml will remain unchanged.")

    results = run_mindoro_local_recipe_experiment()

    print("\nMindoro-local recipe experiment complete.")
    print(f"Outputs saved to: {results['output_dir']}")
    print(f"Mindoro-local subset registry: {results['subset_registry_csv']}")
    print(f"Local recipe summary: {results['recipe_summary_csv']}")
    print(f"Local recipe ranking: {results['recipe_ranking_csv']}")
    print(f"Candidate baseline artifact: {results['candidate_baseline_path']}")
    print(f"Decision report: {results['report_md']}")
    print(f"Manifest: {results['manifest_json']}")
    print(f"Frozen baseline recipe: {results['frozen_baseline_recipe']}")
    print(f"Mindoro-local candidate recipe: {results['local_candidate_recipe']}")
    print(f"Best event-scale recipe: {results['event_best_recipe'] or 'none'}")
    print(f"Candidate beats frozen baseline on event corridor: {results['candidate_beats_frozen']}")
    print(
        "GFS recipes included in the replay: "
        f"{', '.join(results['gfs_recipes_included']) if results['gfs_recipes_included'] else 'none'}"
    )
    if results["gfs_recipes_skipped"]:
        print(f"GFS recipes still skipped: {', '.join(results['gfs_recipes_skipped'])}")
    print(f"Event comparison outputs: {results['comparison_output_dir']}")


def run_phase4_oiltype_and_shoreline_phase():
    from src.services.phase4_oiltype_and_shoreline import run_phase4_oiltype_and_shoreline

    print("Starting support-layer oil-type fate and shoreline-impact workflow...")
    print("This workflow writes a separate Phase 4 support bundle and does not overwrite the stored Mindoro or DWH Phase 3 validation outputs.")

    results = run_phase4_oiltype_and_shoreline()
    verdict = results["verdict"]

    print("\nPhase 4 complete.")
    print(f"Outputs saved to: {results['output_dir']}")
    print(f"Manifest: {results['manifest_path']}")
    print(f"Summary CSV: {results['summary_csv']}")
    print(f"Shoreline arrival CSV: {results['shoreline_arrival_csv']}")
    print(f"Shoreline segments CSV: {results['shoreline_segments_csv']}")
    print(f"Oil-type comparison CSV: {results['oiltype_comparison_csv']}")
    print(f"Methodology memo: {results['methodology_memo_md']}")
    print(f"Verdict: {results['final_verdict_md']}")
    print(f"Scenarios run: {', '.join(results['scenario_ids'])}")
    print(f"Shoreline arrival outputs generated: {results['shoreline_arrival_generated']}")
    print(f"Weathering path audit decision: {results['weathering_path_audit']['decision']}")
    print(
        "Existing weathering path reused: "
        f"{results['weathering_path_audit']['reused_existing_weathering_path']}"
    )
    print(f"Selected transport loading audit: {results['selected_transport_loading_audit_path']}")
    print(f"Support-layer bundle usable now: {verdict['scientifically_reportable_now']}")
    print(f"Support-layer bundle remains inherited-provisional from transport: {verdict['provisional_inherited_from_transport']}")
    print(f"Biggest remaining support-layer blocker: {verdict['biggest_remaining_phase4_blocker']}")


def run_phase4_crossmodel_comparability_audit_phase():
    from src.services.phase4_crossmodel_comparability_audit import run_phase4_crossmodel_comparability_audit

    print("Starting Phase 4 OpenDrift-vs-PyGNOME comparability audit...")
    print("This phase is read-only and will only inspect existing Phase 4, Mindoro PyGNOME, and DWH PyGNOME artifacts.")

    results = run_phase4_crossmodel_comparability_audit()
    verdict = results["overall_verdict"]

    print("\nPhase 4 cross-model comparability audit complete.")
    print(f"Outputs saved to: {results['output_dir']}")
    print(f"Comparability matrix CSV: {results['matrix_csv']}")
    print(f"Comparability matrix JSON: {results['matrix_json']}")
    print(f"Report: {results['report_md']}")
    print(f"Verdict: {results['verdict_md']}")
    print(
        "Quantities directly comparable now: "
        f"{', '.join(verdict['quantities_directly_comparable_now']) if verdict['quantities_directly_comparable_now'] else 'none'}"
    )
    print(
        "Quantities comparable with small adapter: "
        f"{', '.join(verdict['quantities_comparable_with_small_adapter']) if verdict['quantities_comparable_with_small_adapter'] else 'none'}"
    )
    print(f"Pilot comparison figures produced: {verdict['pilot_comparison_produced']}")
    print(f"Biggest blocker: {verdict['biggest_blocker']}")


def run_phase5_launcher_and_docs_sync_phase():
    from src.services.phase5_launcher_and_docs_sync import run_phase5_launcher_and_docs_sync

    print("Starting read-only launcher/docs/package support sync...")
    print("This phase is read-only with respect to scientific outputs and will not rerun expensive science by default.")

    results = run_phase5_launcher_and_docs_sync()
    verdict = results["overall_verdict"]

    print("\nSupport sync complete.")
    print(f"Outputs saved to: {results['output_dir']}")
    print(f"Launcher entrypoint: {results['launcher_entrypoint']}")
    print(f"Menu categories: {', '.join(results['menu_categories'])}")
    print(f"Safe read-only launcher IDs: {', '.join(results['safe_read_only_entry_ids'])}")
    print(f"Docs updated: {', '.join(results['docs_updated'])}")
    print(f"Phase status registry: {results['final_phase_status_registry_csv']}")
    print(f"Manifest index: {results['final_manifest_index_csv']}")
    print(f"Output catalog: {results['final_output_catalog_csv']}")
    print(f"Summary: {results['final_reproducibility_summary_md']}")
    print(f"Verdict: {results['phase5_final_verdict_md']}")
    print(f"Support sync current and usable: {verdict['phase5_reportable_now']}")
    print(f"Launcher/menu honest and current: {verdict['launcher_menu_honest_and_current']}")
    print(
        "Phase 1 freeze remains incomplete (candidate baseline not yet adopted as default): "
        f"{verdict['requires_phase1_production_rerun_for_full_freeze']}"
    )
    print(
        "Legacy recipe drift still leaks into official mode: "
        f"{verdict['legacy_recipe_drift_leaks_into_official_mode']}"
    )
    print(
        "Biggest remaining project-science blocker: "
        f"{verdict['biggest_remaining_project_scientific_blocker']}"
    )


def run_trajectory_gallery_build_phase():
    from src.services.trajectory_gallery_build import run_trajectory_gallery_build

    print("Starting trajectory gallery build...")
    print("This phase is read-only and builds panel-facing figures from existing outputs, manifests, rasters, and NetCDFs only.")

    results = run_trajectory_gallery_build()

    print("\nTrajectory gallery build complete.")
    print(f"Outputs saved to: {results['output_dir']}")
    print(f"Manifest: {results['manifest_path']}")
    print(f"Figure index CSV: {results['index_csv']}")
    print(f"Figures index: {results['figures_index_md']}")
    print(f"Figure count: {results['figure_count']}")
    print(f"Cases with visuals: {', '.join(results['cases_with_visuals'])}")
    print(f"Models with visuals: {', '.join(results['models_with_visuals'])}")
    if results["scenario_ids_with_visuals"]:
        print(f"Scenarios with visuals: {', '.join(results['scenario_ids_with_visuals'])}")
    print("Figure groups generated:")
    for group_code, count in results["figure_group_counts"].items():
        print(f"  - {group_code}: {count}")
    if results["missing_optional_artifacts"]:
        print("Missing optional artifacts recorded:")
        for item in results["missing_optional_artifacts"]:
            print(f"  - {item['relative_path']}: {item['notes']}")


def run_trajectory_gallery_panel_polish_phase():
    from src.services.trajectory_gallery_panel_polish import run_trajectory_gallery_panel_polish

    print("Starting trajectory gallery panel polish...")
    print("This phase is read-only and builds polished panel-facing boards from existing figures, manifests, rasters, and NetCDFs only.")

    results = run_trajectory_gallery_panel_polish()

    print("\nTrajectory gallery panel polish complete.")
    print(f"Outputs saved to: {results['output_dir']}")
    print(f"Registry: {results['registry_csv']}")
    print(f"Manifest: {results['manifest_json']}")
    print(f"Captions: {results['captions_md']}")
    print(f"Talking points: {results['talking_points_md']}")
    print(f"Figure count: {results['figure_count']}")
    print(f"Board families generated: {', '.join(results['board_families_generated'])}")
    print(f"Side-by-side comparison boards produced: {results['side_by_side_comparison_boards_produced']}")
    print(f"Plain-language captions produced: {results['plain_language_captions_produced']}")
    if results["recommended_main_defense_figures"]:
        print("Recommended main-defense figures:")
        for figure_id in results["recommended_main_defense_figures"]:
            print(f"  - {figure_id}")
    if results["missing_optional_artifacts"]:
        print("Missing optional artifacts recorded:")
        for item in results["missing_optional_artifacts"]:
            print(f"  - {item['relative_path']}: {item['notes']}")


def run_figure_package_publication_phase():
    from src.services.figure_package_publication import run_figure_package_publication

    print("Starting publication-grade figure package build...")
    print("This phase is read-only and redraws publication-grade figures from existing outputs, manifests, rasters, and NetCDFs only.")

    results = run_figure_package_publication()

    print("\nPublication-grade figure package complete.")
    print(f"Outputs saved to: {results['output_dir']}")
    print(f"Registry: {results['registry_csv']}")
    print(f"Manifest: {results['manifest_json']}")
    print(f"Captions: {results['captions_md']}")
    print(f"Talking points: {results['talking_points_md']}")
    print(f"Figure count: {results['figure_count']}")
    print(f"Figure families generated: {', '.join(results['figure_families_generated'])}")
    print(f"Side-by-side comparison boards produced: {results['side_by_side_comparison_boards_produced']}")
    print(f"Single-image paper figures produced: {results['single_image_paper_figures_produced']}")
    if results["recommended_main_defense_figures"]:
        print("Recommended main-defense figures:")
        for figure_id in results["recommended_main_defense_figures"]:
            print(f"  - {figure_id}")
    if results["missing_optional_artifacts"]:
        print("Missing optional artifacts recorded:")
        for item in results["missing_optional_artifacts"]:
            print(f"  - {item['relative_path']}: {item['notes']}")


def main():
    import subprocess

    from src.core.case_context import get_case_context
    from src.exceptions.custom import BENCHMARK_SKIP_EXIT_CODE, PREP_OUTAGE_DECISION_EXIT_CODE
    from src.utils.forcing_outage_policy import FORCING_OUTAGE_SKIP_EXIT_CODE

    is_spawned = os.environ.get("RUN_SPAWNED")
    phase = os.environ.get("PIPELINE_PHASE", "1_2")
    startup_state = _resolve_startup_prompt_env_once(phase)
    _emit_direct_run_startup_banner(startup_state)
    if phase == "final_validation_package":
        run_final_validation_package_phase()
        return
    if phase == "phase1_finalization_audit":
        run_phase1_finalization_audit_phase()
        return
    if phase == "phase2_finalization_audit":
        run_phase2_finalization_audit_phase()
        return
    if phase == "phase1_production_rerun":
        run_phase1_production_rerun_phase()
        return
    if phase == "mindoro_march13_14_phase1_focus_trial":
        run_mindoro_march13_14_phase1_focus_trial_phase()
        return
    if phase == "mindoro_local_recipe_experiment":
        run_mindoro_local_recipe_experiment_phase()
        return
    if phase == "phase4_oiltype_and_shoreline":
        run_phase4_oiltype_and_shoreline_phase()
        return
    if phase == "phase4_crossmodel_comparability_audit":
        run_phase4_crossmodel_comparability_audit_phase()
        return
    if phase == "phase5_launcher_and_docs_sync":
        run_phase5_launcher_and_docs_sync_phase()
        return
    if phase == "trajectory_gallery_build":
        run_trajectory_gallery_build_phase()
        return
    if phase == "prototype_pygnome_similarity_summary":
        run_prototype_pygnome_similarity_summary_phase()
        return
    if phase == "prototype_legacy_final_figures":
        run_prototype_legacy_final_figures_phase()
        return
    if phase == "prototype_2016_display_review":
        run_prototype_2016_display_review_phase()
        return
    if phase == "prototype_legacy_phase4_weathering":
        run_prototype_legacy_phase4_weathering()
        return
    if phase == "trajectory_gallery_panel_polish":
        run_trajectory_gallery_panel_polish_phase()
        return
    if phase == "figure_package_publication":
        run_figure_package_publication_phase()
        return

    case = get_case_context()

    if case.orchestration_dates and not is_spawned:
        print(
            f"Prototype workflow detected. Executing orchestrator for "
            f"{len(case.orchestration_dates)} cases..."
        )
        for token in case.orchestration_dates:
            print(f"\n{'=' * 60}")
            print(f"Spawning pipeline for CASE {token}")
            print(f"{'=' * 60}")
            env = os.environ.copy()
            if case.workflow_mode == "prototype_2016":
                env["PHASE_1_START_DATE"] = token
            else:
                env[PROTOTYPE_CASE_ID_ENV] = token
            env["RUN_SPAWNED"] = "1"
            result = subprocess.run([sys.executable, "-m", "src"], env=env)
            if result.returncode != 0:
                if result.returncode == PREP_OUTAGE_DECISION_EXIT_CODE:
                    sys.exit(result.returncode)
                if result.returncode == FORCING_OUTAGE_SKIP_EXIT_CODE:
                    print(f"Case {token} skipped after forcing outage under degraded continuation. Continuing to next case...")
                    continue
                if result.returncode == BENCHMARK_SKIP_EXIT_CODE:
                    print(f"Case {token} skipped in benchmark phase because it lies outside the current benchmark grid. Continuing to next case...")
                    continue
                print(f"Pipeline failed for case {token}. Continuing to next case...")
        return

    if phase == "prep":
        run_prep()
    elif phase == "official_phase3b":
        run_official_phase3b_minimal()
    elif phase == "benchmark":
        run_benchmark()
    elif phase == "recipe_sensitivity":
        run_recipe_sensitivity_phase()
    elif phase == "convergence_after_shoreline":
        run_convergence_after_shoreline_phase()
    elif phase == "displacement_after_convergence":
        run_displacement_after_convergence_phase()
    elif phase == "phase3b_multidate_public":
        run_phase3b_multidate_public_phase()
    elif phase == "phase3b_extended_public":
        run_phase3b_extended_public_phase()
    elif phase == "phase3b_extended_public_scored":
        run_phase3b_extended_public_scored_phase()
    elif phase == "phase3b_extended_public_scored_march23":
        run_phase3b_extended_public_scored_march23_phase()
    elif phase == "phase3b_extended_public_scored_march13_14_reinit":
        run_phase3b_extended_public_scored_march13_14_reinit_phase()
    elif phase == "phase3b_extended_public_scored_march13_14_reinit_pygnome_comparison":
        run_phase3b_extended_public_scored_march13_14_reinit_pygnome_comparison_phase()
    elif phase == "horizon_survival_audit":
        run_horizon_survival_audit_phase()
    elif phase == "transport_retention_fix":
        run_transport_retention_fix_phase()
    elif phase == "official_rerun_r1":
        run_official_rerun_r1_phase()
    elif phase == "init_mode_sensitivity_r1":
        run_init_mode_sensitivity_r1_phase()
    elif phase == "source_history_reconstruction_r1":
        run_source_history_reconstruction_r1_phase()
    elif phase == "pygnome_public_comparison":
        run_pygnome_public_comparison_phase()
    elif phase == "ensemble_threshold_sensitivity":
        run_ensemble_threshold_sensitivity_phase()
    elif phase == "recipe_sensitivity_r1_multibranch":
        run_recipe_sensitivity_r1_multibranch_phase()
    elif phase == "public_obs_appendix":
        run_public_obs_appendix_phase()
    elif phase == "march6_recovery_sensitivity":
        run_march6_recovery_sensitivity_phase()
    elif phase == "phase3c_external_case_setup":
        run_phase3c_external_case_setup_phase()
    elif phase == "dwh_phase3c_forcing_adapter_and_non_scientific_smoke_forecast":
        run_dwh_phase3c_smoke_phase()
    elif phase == "dwh_phase3c_scientific_forcing_ready":
        run_dwh_phase3c_scientific_forcing_ready_phase()
    elif phase == "phase3c_external_case_run":
        run_phase3c_external_case_run_phase()
    elif phase == "phase3c_external_case_ensemble_comparison":
        run_phase3c_external_case_ensemble_comparison_phase()
    elif phase == "phase3c_dwh_pygnome_comparator":
        run_phase3c_dwh_pygnome_comparator_phase()
    elif phase == "final_validation_package":
        run_final_validation_package_phase()
    elif phase == "phase1_finalization_audit":
        run_phase1_finalization_audit_phase()
    elif phase == "phase2_finalization_audit":
        run_phase2_finalization_audit_phase()
    elif phase == "phase1_production_rerun":
        run_phase1_production_rerun_phase()
    elif phase == "mindoro_march13_14_phase1_focus_trial":
        run_mindoro_march13_14_phase1_focus_trial_phase()
    elif phase == "mindoro_local_recipe_experiment":
        run_mindoro_local_recipe_experiment_phase()
    elif phase == "phase4_oiltype_and_shoreline":
        run_phase4_oiltype_and_shoreline_phase()
    elif phase == "phase4_crossmodel_comparability_audit":
        run_phase4_crossmodel_comparability_audit_phase()
    elif phase == "phase5_launcher_and_docs_sync":
        run_phase5_launcher_and_docs_sync_phase()
    elif phase == "trajectory_gallery_build":
        run_trajectory_gallery_build_phase()
    elif phase == "prototype_legacy_final_figures":
        run_prototype_legacy_final_figures_phase()
    elif phase == "prototype_2016_display_review":
        run_prototype_2016_display_review_phase()
    elif phase == "trajectory_gallery_panel_polish":
        run_trajectory_gallery_panel_polish_phase()
    elif phase == "figure_package_publication":
        run_figure_package_publication_phase()
    elif phase == "3":
        if case.workflow_mode == "prototype_2016":
            run_prototype_legacy_phase4_weathering(deprecated_alias="3")
        else:
            run_phase3()
    elif phase == "3b":
        run_phase3b()
    else:
        run_phase1_and_2()


if __name__ == "__main__":
    main()
