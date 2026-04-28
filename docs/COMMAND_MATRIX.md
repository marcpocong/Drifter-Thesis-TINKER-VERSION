# Command Matrix

## Canonical Paths

Start the panel-facing launcher:

```powershell
.\panel.ps1
.\start.ps1 -Panel
```

List the current launcher entries:

```powershell
.\start.ps1 -List -NoPause
.\start.ps1 -Help -NoPause
```

Run one workflow interactively through the launcher:

```powershell
.\start.ps1 -Entry <entry_id>
```

Run one phase prompt-free inside the container:

```bash
docker-compose exec -T -e WORKFLOW_MODE=<workflow_mode> -e PIPELINE_PHASE=<phase> <pipeline|gnome> python -m src
```

Repeat the prompt-free container command once per phase, in the order shown below for multi-step launcher entries.

Launch the read-only UI directly:

```bash
docker-compose exec pipeline python -m streamlit run ui/app.py --server.address 0.0.0.0 --server.port 8501
```

Run the read-only paper-result verification directly:

```bash
docker-compose exec -T pipeline python src/services/panel_review_check.py
```

## Launcher Entry Map

| Entry ID | Use | Workflow Mode | Interactive Launcher Command | Prompt-Free Phase Mapping |
| --- | --- | --- | --- | --- |
| `phase1_audit` | read-only | `mindoro_retro_2023` | `.\start.ps1 -Entry phase1_audit` | `pipeline: phase1_finalization_audit` |
| `phase2_audit` | read-only | `mindoro_retro_2023` | `.\start.ps1 -Entry phase2_audit` | `pipeline: phase2_finalization_audit` |
| `final_validation_package` | read-only | `mindoro_retro_2023` | `.\start.ps1 -Entry final_validation_package` | `pipeline: final_validation_package` |
| `phase5_sync` | read-only | `mindoro_retro_2023` | `.\start.ps1 -Entry phase5_sync` | `pipeline: phase5_launcher_and_docs_sync` |
| `trajectory_gallery` | read-only | `mindoro_retro_2023` | `.\start.ps1 -Entry trajectory_gallery` | `pipeline: trajectory_gallery_build` |
| `trajectory_gallery_panel` | read-only | `mindoro_retro_2023` | `.\start.ps1 -Entry trajectory_gallery_panel` | `pipeline: trajectory_gallery_panel_polish` |
| `figure_package_publication` | read-only | `mindoro_retro_2023` | `.\start.ps1 -Entry figure_package_publication` | `pipeline: figure_package_publication` |
| `prototype_legacy_final_figures` | read-only | `prototype_2016` | `.\start.ps1 -Entry prototype_legacy_final_figures` | `pipeline: prototype_legacy_final_figures` |
| `phase1_production_rerun` | reportable | `phase1_regional_2016_2022` | `.\start.ps1 -Entry phase1_production_rerun` | `pipeline: phase1_production_rerun` |
| `mindoro_phase3b_primary_public_validation` | reportable | `mindoro_retro_2023` | `.\start.ps1 -Entry mindoro_phase3b_primary_public_validation` | `pipeline: phase3b_extended_public -> phase3b_extended_public_scored_march13_14_reinit` |
| `mindoro_reportable_core` | reportable | `mindoro_retro_2023` | `.\start.ps1 -Entry mindoro_reportable_core` | `pipeline: prep -> 1_2 -> phase3b_extended_public -> phase3b_extended_public_scored_march13_14_reinit -> 3b -> phase3b_multidate_public -> phase4_oiltype_and_shoreline` |
| `dwh_reportable_bundle` | reportable | `dwh_retro_2010` | `.\start.ps1 -Entry dwh_reportable_bundle` | `pipeline: phase3c_external_case_setup -> dwh_phase3c_scientific_forcing_ready -> phase3c_external_case_run -> phase3c_external_case_ensemble_comparison; gnome: phase3c_dwh_pygnome_comparator` |
| `mindoro_phase4_only` | support | `mindoro_retro_2023` | `.\start.ps1 -Entry mindoro_phase4_only` | `pipeline: phase4_oiltype_and_shoreline` |
| `mindoro_appendix_sensitivity_bundle` | support | `mindoro_retro_2023` | `.\start.ps1 -Entry mindoro_appendix_sensitivity_bundle` | `pipeline: public_obs_appendix -> phase3b_extended_public -> phase3b_extended_public_scored -> phase3b_extended_public_scored_march23 -> phase3b_extended_public_scored_march13_14_reinit -> horizon_survival_audit -> transport_retention_fix -> official_rerun_r1 -> init_mode_sensitivity_r1 -> source_history_reconstruction_r1 -> ensemble_threshold_sensitivity -> recipe_sensitivity_r1_multibranch; gnome: phase3b_extended_public_scored_march13_14_reinit_pygnome_comparison -> pygnome_public_comparison` |
| `phase1_mindoro_focus_pre_spill_experiment` | support | `phase1_mindoro_focus_pre_spill_2016_2023` | `.\start.ps1 -Entry phase1_mindoro_focus_pre_spill_experiment` | `pipeline: phase1_production_rerun` |
| `mindoro_march13_14_phase1_focus_trial` | archive trial | `mindoro_retro_2023` | `.\start.ps1 -Entry mindoro_march13_14_phase1_focus_trial` | `pipeline: phase3b_extended_public -> mindoro_march13_14_phase1_focus_trial` |
| `mindoro_march6_recovery_sensitivity` | archive support | `mindoro_retro_2023` | `.\start.ps1 -Entry mindoro_march6_recovery_sensitivity` | `pipeline: march6_recovery_sensitivity` |
| `mindoro_march23_extended_public_stress_test` | archive support | `mindoro_retro_2023` | `.\start.ps1 -Entry mindoro_march23_extended_public_stress_test` | `pipeline: phase3b_extended_public -> phase3b_extended_public_scored_march23` |
| `mindoro_march13_14_noaa_reinit_stress_test` | compatibility alias | `mindoro_retro_2023` | `.\start.ps1 -Entry mindoro_march13_14_noaa_reinit_stress_test` | `pipeline: phase3b_extended_public -> phase3b_extended_public_scored_march13_14_reinit; gnome: phase3b_extended_public_scored_march13_14_reinit_pygnome_comparison` |
| `prototype_2021_bundle` | legacy support | `prototype_2021` | `.\start.ps1 -Entry prototype_2021_bundle` | `pipeline: prep -> 1_2 -> prototype_pygnome_similarity_summary; gnome: benchmark` |
| `prototype_legacy_bundle` | legacy support | `prototype_2016` | `.\start.ps1 -Entry prototype_legacy_bundle` | `pipeline: prep -> 1_2 -> prototype_pygnome_similarity_summary -> prototype_legacy_final_figures; gnome: benchmark -> prototype_legacy_phase4_weathering` |

The phase-mapping column shows the ordered services and phases behind each launcher entry. When an entry carries extra environment overrides in `config/launcher_matrix.json`, use the launcher or that file as the exact source of truth for reproducing the full prompt-free run.

## Exact Prompt-Free Read-Only Commands

```bash
docker-compose exec -T -e WORKFLOW_MODE=mindoro_retro_2023 -e PIPELINE_PHASE=phase1_finalization_audit pipeline python -m src
docker-compose exec -T -e WORKFLOW_MODE=mindoro_retro_2023 -e PIPELINE_PHASE=phase2_finalization_audit pipeline python -m src
docker-compose exec -T -e WORKFLOW_MODE=mindoro_retro_2023 -e PIPELINE_PHASE=final_validation_package pipeline python -m src
docker-compose exec -T -e WORKFLOW_MODE=mindoro_retro_2023 -e PIPELINE_PHASE=phase5_launcher_and_docs_sync pipeline python -m src
docker-compose exec -T -e WORKFLOW_MODE=mindoro_retro_2023 -e PIPELINE_PHASE=trajectory_gallery_build pipeline python -m src
docker-compose exec -T -e WORKFLOW_MODE=mindoro_retro_2023 -e PIPELINE_PHASE=trajectory_gallery_panel_polish pipeline python -m src
docker-compose exec -T -e WORKFLOW_MODE=mindoro_retro_2023 -e PIPELINE_PHASE=figure_package_publication pipeline python -m src
docker-compose exec -T -e WORKFLOW_MODE=prototype_2016 -e PIPELINE_PHASE=prototype_legacy_final_figures pipeline python -m src
```

## Exact Prompt-Free Reportable Sequences

Phase 1 regional reference rerun:

```bash
docker-compose exec -T -e WORKFLOW_MODE=phase1_regional_2016_2022 -e PIPELINE_PHASE=phase1_production_rerun pipeline python -m src
```

Mindoro Phase 3B primary public validation:

```bash
docker-compose exec -T -e WORKFLOW_MODE=mindoro_retro_2023 -e PIPELINE_PHASE=phase3b_extended_public pipeline python -m src
docker-compose exec -T -e WORKFLOW_MODE=mindoro_retro_2023 -e PIPELINE_PHASE=phase3b_extended_public_scored_march13_14_reinit pipeline python -m src
```

Mindoro validation core plus support bundle:

```bash
docker-compose exec -T -e WORKFLOW_MODE=mindoro_retro_2023 -e PIPELINE_PHASE=prep pipeline python -m src
docker-compose exec -T -e WORKFLOW_MODE=mindoro_retro_2023 -e PIPELINE_PHASE=1_2 pipeline python -m src
docker-compose exec -T -e WORKFLOW_MODE=mindoro_retro_2023 -e PIPELINE_PHASE=phase3b_extended_public pipeline python -m src
docker-compose exec -T -e WORKFLOW_MODE=mindoro_retro_2023 -e PIPELINE_PHASE=phase3b_extended_public_scored_march13_14_reinit pipeline python -m src
docker-compose exec -T -e WORKFLOW_MODE=mindoro_retro_2023 -e PIPELINE_PHASE=3b pipeline python -m src
docker-compose exec -T -e WORKFLOW_MODE=mindoro_retro_2023 -e PIPELINE_PHASE=phase3b_multidate_public pipeline python -m src
docker-compose exec -T -e WORKFLOW_MODE=mindoro_retro_2023 -e PIPELINE_PHASE=phase4_oiltype_and_shoreline pipeline python -m src
```

DWH Phase 3C reportable bundle:

```bash
docker-compose exec -T -e WORKFLOW_MODE=dwh_retro_2010 -e PIPELINE_PHASE=phase3c_external_case_setup pipeline python -m src
docker-compose exec -T -e WORKFLOW_MODE=dwh_retro_2010 -e PIPELINE_PHASE=dwh_phase3c_scientific_forcing_ready pipeline python -m src
docker-compose exec -T -e WORKFLOW_MODE=dwh_retro_2010 -e PIPELINE_PHASE=phase3c_external_case_run pipeline python -m src
docker-compose exec -T -e WORKFLOW_MODE=dwh_retro_2010 -e PIPELINE_PHASE=phase3c_external_case_ensemble_comparison pipeline python -m src
docker-compose exec -T -e WORKFLOW_MODE=dwh_retro_2010 -e PIPELINE_PHASE=phase3c_dwh_pygnome_comparator gnome python -m src
```

## Read-Only UI

The Streamlit UI is intentionally outside the launcher matrix. Launch it directly:

```bash
docker-compose exec pipeline python -m streamlit run ui/app.py --server.address 0.0.0.0 --server.port 8501
```

## Compatibility Note

- `mindoro_march13_14_noaa_reinit_stress_test` remains supported only for older scripts and notes. Use `mindoro_phase3b_primary_public_validation` as the primary Mindoro B1 command.

## Guardrails

- Use the launcher entry IDs as the primary workflow names in docs and handoff notes.
- Use `docker-compose exec -T ...` only when you intentionally want the prompt-free container path.
- Do not treat raw phase names such as `official_phase3b` as the primary user-facing startup commands.
- Treat `Phase 3B` and `Phase 3C` as validation-only lanes.
- Keep `prototype_2016` on the legacy `Phase 1 / 2 / 3A / 4 / 5` support story only.
- The dedicated Phase 1 rerun stages `output/phase1_production_rerun/phase1_baseline_selection_candidate.yaml` only; promotion of `config/phase1_baseline_selection.yaml` remains explicit and manual.
- Keep `config/case_mindoro_retro_2023.yaml` frozen as the March 3 -> March 6 base case; the promoted March 13 -> March 14 R1 primary validation row is carried by amendment rather than silent rewrite.
