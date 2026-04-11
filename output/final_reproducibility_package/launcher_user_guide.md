# Launcher User Guide

Use the PowerShell launcher from the repository root. The launcher is now organized around honest current tracks instead of the old single Mindoro full-chain menu.

## Safe First Commands

- `./start.ps1 -List -NoPause` shows the current menu catalog without starting Docker work.
- `./start.ps1 -Help -NoPause` prints guidance and safe entry IDs.
- `./start.ps1 -Entry phase1_audit -NoPause` runs the safe read-only entry `Phase 1 finalization audit`.
- `./start.ps1 -Entry phase2_audit -NoPause` runs the safe read-only entry `Phase 2 finalization audit`.
- `./start.ps1 -Entry final_validation_package -NoPause` runs the safe read-only entry `Final validation package refresh`.
- `./start.ps1 -Entry phase5_sync -NoPause` runs the safe read-only entry `Phase 5 launcher/docs/package sync`.
- `./start.ps1 -Entry trajectory_gallery -NoPause` runs the safe read-only entry `Trajectory gallery build`.
- `./start.ps1 -Entry trajectory_gallery_panel -NoPause` runs the safe read-only entry `Trajectory gallery panel polish`.
- `./start.ps1 -Entry figure_package_publication -NoPause` runs the safe read-only entry `Publication-grade figure package`.

## Entry Catalog

### Scientific / reportable tracks

Intentional scientific reruns or reportable output builders.

- `mindoro_reportable_core`: Mindoro reportable core bundle. Workflow mode = `mindoro_retro_2023`. Cost = `expensive`. Safe read-only default = `false`. Phases = prep, 1_2, 3b, phase3b_multidate_public, phase4_oiltype_and_shoreline.
  Note: Use only when an intentional scientific rerun of the main Mindoro reportable path is desired.
  Run with: `./start.ps1 -Entry mindoro_reportable_core -NoPause`
- `mindoro_phase4_only`: Mindoro Phase 4 only. Workflow mode = `mindoro_retro_2023`. Cost = `moderate`. Safe read-only default = `false`. Phases = phase4_oiltype_and_shoreline.
  Note: Does not overwrite stored Mindoro or DWH Phase 3 validation outputs.
  Run with: `./start.ps1 -Entry mindoro_phase4_only -NoPause`
- `dwh_reportable_bundle`: DWH Phase 3C reportable bundle. Workflow mode = `dwh_retro_2010`. Cost = `expensive`. Safe read-only default = `false`. Phases = phase3c_external_case_setup, dwh_phase3c_scientific_forcing_ready, phase3c_external_case_run, phase3c_external_case_ensemble_comparison, phase3c_dwh_pygnome_comparator.
  Note: This is the full DWH scientific rerun path and should only be used intentionally.
  Run with: `./start.ps1 -Entry dwh_reportable_bundle -NoPause`

### Sensitivity / appendix tracks

Supporting branches that are informative but not the main reportable path.

- `mindoro_appendix_sensitivity_bundle`: Mindoro appendix / sensitivity bundle. Workflow mode = `mindoro_retro_2023`. Cost = `expensive`. Safe read-only default = `false`. Phases = public_obs_appendix, phase3b_extended_public, phase3b_extended_public_scored, horizon_survival_audit, transport_retention_fix, official_rerun_r1, init_mode_sensitivity_r1, source_history_reconstruction_r1, pygnome_public_comparison, ensemble_threshold_sensitivity, recipe_sensitivity_r1_multibranch.
  Note: These tracks are informative and reportable as support material, but they are not the main-text scientific core.
  Run with: `./start.ps1 -Entry mindoro_appendix_sensitivity_bundle -NoPause`

### Read-only packaging / help utilities

Safe utilities that summarize or audit the current repo state without rerunning expensive science.

- `phase1_audit`: Phase 1 finalization audit. Workflow mode = `mindoro_retro_2023`. Cost = `cheap_read_only`. Safe read-only default = `true`. Phases = phase1_finalization_audit.
  Note: Does not rerun the expensive 2016-2022 production study.
  Run with: `./start.ps1 -Entry phase1_audit -NoPause`
- `phase2_audit`: Phase 2 finalization audit. Workflow mode = `mindoro_retro_2023`. Cost = `cheap_read_only`. Safe read-only default = `true`. Phases = phase2_finalization_audit.
  Note: Does not rerun the expensive official forecast path by default.
  Run with: `./start.ps1 -Entry phase2_audit -NoPause`
- `final_validation_package`: Final validation package refresh. Workflow mode = `mindoro_retro_2023`. Cost = `cheap_read_only`. Safe read-only default = `true`. Phases = final_validation_package.
  Note: Reuses existing scientific outputs without recomputing scores.
  Run with: `./start.ps1 -Entry final_validation_package -NoPause`
- `phase5_sync`: Phase 5 launcher/docs/package sync. Workflow mode = `mindoro_retro_2023`. Cost = `cheap_read_only`. Safe read-only default = `true`. Phases = phase5_launcher_and_docs_sync.
  Note: Builds final_reproducibility_package without overwriting scientific outputs.
  Run with: `./start.ps1 -Entry phase5_sync -NoPause`
- `trajectory_gallery`: Trajectory gallery build. Workflow mode = `mindoro_retro_2023`. Cost = `cheap_read_only`. Safe read-only default = `true`. Phases = trajectory_gallery_build.
  Note: Builds output/trajectory_gallery from existing outputs only and does not rerun expensive scientific branches.
  Run with: `./start.ps1 -Entry trajectory_gallery -NoPause`
- `trajectory_gallery_panel`: Trajectory gallery panel polish. Workflow mode = `mindoro_retro_2023`. Cost = `cheap_read_only`. Safe read-only default = `true`. Phases = trajectory_gallery_panel_polish.
  Note: Builds output/trajectory_gallery_panel without rerunning expensive scientific branches.
  Run with: `./start.ps1 -Entry trajectory_gallery_panel -NoPause`
- `figure_package_publication`: Publication-grade figure package. Workflow mode = `mindoro_retro_2023`. Cost = `cheap_read_only`. Safe read-only default = `true`. Phases = figure_package_publication.
  Note: Builds output/figure_package_publication without rerunning expensive scientific branches.
  Run with: `./start.ps1 -Entry figure_package_publication -NoPause`

### Legacy prototype tracks

Backward-compatible prototype workflows preserved for debugging and regression.

- `prototype_legacy_bundle`: Prototype 2016 legacy bundle. Workflow mode = `prototype_2016`. Cost = `moderate`. Safe read-only default = `false`. Phases = prep, 1_2, benchmark, 3, 3b.
  Note: Backward-compatible debug/regression path only. Not the final Chapter 3 Phase 1 study.
  Run with: `./start.ps1 -Entry prototype_legacy_bundle -NoPause`

## Guardrails

- `prototype_2016` remains available for debugging and regression only; it is not the final Phase 1 study.
- `mindoro_reportable_core` and `dwh_reportable_bundle` are intentional scientific reruns and are not safe defaults.
- The read-only utilities do not recompute scientific scores and are the safest launcher options for routine status refreshes.

## Optional Future Work

- `ui_run_controls`: Interactive UI run controls [deferred]
- `ui_deeper_search_filters`: Deeper artifact search and filtering inside the UI [deferred]
- `dwh_phase4_appendix_pilot`: DWH Phase 4 appendix pilot [deferred]

## Matrix Source

- Catalog file: `config/launcher_matrix.json`
- Entrypoint script: `start.ps1`
- Catalog version: `phase5_launcher_matrix_v2`
