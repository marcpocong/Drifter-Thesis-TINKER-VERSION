# Launcher User Guide

Use the PowerShell launcher from the repository root. The launcher is now organized around honest current tracks instead of the old single Mindoro full-chain menu.

## Safe First Commands

- `./start.ps1 -List -NoPause` shows the current menu catalog without starting Docker work.
- `./start.ps1 -Help -NoPause` prints guidance and safe entry IDs.
- `./start.ps1 -Entry mindoro_march6_recovery_sensitivity -NoPause` runs the safe read-only entry `Mindoro March 6 recovery sensitivity`.
- `./start.ps1 -Entry phase1_audit -NoPause` runs the safe read-only entry `Phase 1 finalization audit`.
- `./start.ps1 -Entry phase2_audit -NoPause` runs the safe read-only entry `Phase 2 finalization audit`.
- `./start.ps1 -Entry final_validation_package -NoPause` runs the safe read-only entry `Final validation package refresh`.
- `./start.ps1 -Entry phase5_sync -NoPause` runs the safe read-only entry `Phase 5 launcher/docs/package sync`.
- `./start.ps1 -Entry trajectory_gallery -NoPause` runs the safe read-only entry `Trajectory gallery build`.
- `./start.ps1 -Entry trajectory_gallery_panel -NoPause` runs the safe read-only entry `Trajectory gallery panel polish`.
- `./start.ps1 -Entry figure_package_publication -NoPause` runs the safe read-only entry `Publication-grade figure package`.
- `./start.ps1 -Entry prototype_legacy_final_figures -NoPause` runs the safe read-only entry `Prototype 2016 final paper figures`.

## Entry Catalog

### Scientific / reportable tracks

Intentional scientific reruns or reportable output builders.

- `mindoro_reportable_core`: Mindoro validation core + support bundle. Workflow mode = `mindoro_retro_2023`. Cost = `expensive`. Safe read-only default = `false`. Phases = prep, 1_2, phase3b_extended_public, phase3b_extended_public_scored_march13_14_reinit, 3b, phase3b_multidate_public, phase4_oiltype_and_shoreline.
  Note: Use only when an intentional rerun of the main Mindoro spill-case validation chain is desired. The separate phase1_mindoro_focus_pre_spill_2016_2023 Mindoro-specific provenance lane stays outside this entry.
  Run with: `./start.ps1 -Entry mindoro_reportable_core -NoPause`
- `phase1_production_rerun`: Phase 1 regional reference rerun. Workflow mode = `phase1_regional_2016_2022`. Cost = `expensive`. Safe read-only default = `false`. Phases = phase1_production_rerun.
  Note: Scientific rerun only. This broader regional lane is preserved for reference/governance context and does not overwrite config/phase1_baseline_selection.yaml, does not auto-run phase1_audit or phase5_sync, and fails hard by default if a forcing-provider outage removes part of the official recipe family.
  Run with: `./start.ps1 -Entry phase1_production_rerun -NoPause`
- `mindoro_phase3b_primary_public_validation`: Mindoro Phase 3B primary public validation. Workflow mode = `mindoro_retro_2023`. Cost = `expensive`. Safe read-only default = `false`. Phases = phase3b_extended_public, phase3b_extended_public_scored_march13_14_reinit.
  Note: Canonical B1 builder. This does not delete or relabel the March 6 B2 legacy honesty row, and the same-case A comparator-support lane stays separate and comparator-only.
  Run with: `./start.ps1 -Entry mindoro_phase3b_primary_public_validation -NoPause`
- `mindoro_phase4_only`: Mindoro support Phase 4 only. Workflow mode = `mindoro_retro_2023`. Cost = `moderate`. Safe read-only default = `false`. Phases = phase4_oiltype_and_shoreline.
  Note: Does not overwrite stored Mindoro or DWH Phase 3 validation outputs and does not change the main Mindoro validation claim.
  Run with: `./start.ps1 -Entry mindoro_phase4_only -NoPause`
- `dwh_reportable_bundle`: DWH Phase 3C reportable bundle. Workflow mode = `dwh_retro_2010`. Cost = `expensive`. Safe read-only default = `false`. Phases = phase3c_external_case_setup, dwh_phase3c_scientific_forcing_ready, phase3c_external_case_run, phase3c_external_case_ensemble_comparison, phase3c_dwh_pygnome_comparator.
  Note: Separate external transfer-validation story only. Mindoro remains the main Philippine thesis case; DWH observed masks remain truth; PyGNOME remains comparator-only; current frozen stack is HYCOM GOFS 3.1 + ERA5 + CMEMS wave/Stokes; forcing-readiness stays strict by default unless FORCING_OUTAGE_POLICY=continue_degraded is set explicitly.
  Run with: `./start.ps1 -Entry dwh_reportable_bundle -NoPause`

### Sensitivity / appendix tracks

Supporting branches and backward-compatible aliases that are informative but not the main reportable path.

- `mindoro_appendix_sensitivity_bundle`: Mindoro appendix / sensitivity bundle. Workflow mode = `mindoro_retro_2023`. Cost = `expensive`. Safe read-only default = `false`. Phases = public_obs_appendix, phase3b_extended_public, phase3b_extended_public_scored, phase3b_extended_public_scored_march23, phase3b_extended_public_scored_march13_14_reinit, phase3b_extended_public_scored_march13_14_reinit_pygnome_comparison, horizon_survival_audit, transport_retention_fix, official_rerun_r1, init_mode_sensitivity_r1, source_history_reconstruction_r1, pygnome_public_comparison, ensemble_threshold_sensitivity, recipe_sensitivity_r1_multibranch.
  Note: These tracks are informative and reportable as support material, but the promoted B1 row now has its own scientific launcher entry. Forcing-only outages may skip the affected support branch with explicit degraded-mode honesty fields.
  Run with: `./start.ps1 -Entry mindoro_appendix_sensitivity_bundle -NoPause`
- `phase1_mindoro_focus_pre_spill_experiment`: Mindoro-focused Phase 1 provenance rerun. Workflow mode = `phase1_mindoro_focus_pre_spill_2016_2023`. Cost = `expensive`. Safe read-only default = `false`. Phases = phase1_production_rerun.
  Note: Mindoro-specific provenance lane only. This does not rewrite the stored March 13 -> March 14 B1 raw-generation history, does not modify legacy 2016 prototype outputs, and currently evaluates the outage-constrained ERA5-backed family while archived NOAA/NCEI GFS access remains unavailable.
  Run with: `./start.ps1 -Entry phase1_mindoro_focus_pre_spill_experiment -NoPause`
- `mindoro_march13_14_phase1_focus_trial`: Experimental Mindoro March 13-14 Phase 1 focus trial. Workflow mode = `mindoro_retro_2023`. Cost = `expensive`. Safe read-only default = `false`. Phases = phase3b_extended_public, mindoro_march13_14_phase1_focus_trial.
  Note: Archive-labeled comparison trial only. Runs only the OpenDrift March 13 -> March 14 lane in a separate directory; does not rerun PyGNOME or overwrite canonical B1 outputs.
  Run with: `./start.ps1 -Entry mindoro_march13_14_phase1_focus_trial -NoPause`
- `mindoro_march6_recovery_sensitivity`: Mindoro March 6 recovery sensitivity. Workflow mode = `mindoro_retro_2023`. Cost = `moderate`. Safe read-only default = `true`. Phases = march6_recovery_sensitivity.
  Note: Appendix-only. This does not replace or relabel the frozen strict March 6 official result.
  Run with: `./start.ps1 -Entry mindoro_march6_recovery_sensitivity -NoPause`
- `mindoro_march23_extended_public_stress_test`: Mindoro March 23 extended public stress test. Workflow mode = `mindoro_retro_2023`. Cost = `expensive`. Safe read-only default = `false`. Phases = phase3b_extended_public, phase3b_extended_public_scored_march23.
  Note: Appendix-only. This does not replace the frozen strict March 6 official result or the final validation package.
  Run with: `./start.ps1 -Entry mindoro_march23_extended_public_stress_test -NoPause`
- `mindoro_march13_14_noaa_reinit_stress_test`: Legacy alias: Mindoro March 13-14 primary validation bundle. Workflow mode = `mindoro_retro_2023`. Cost = `expensive`. Safe read-only default = `false`. Phases = phase3b_extended_public, phase3b_extended_public_scored_march13_14_reinit, phase3b_extended_public_scored_march13_14_reinit_pygnome_comparison.
  Note: Compatibility alias retained for older scripts. B1 is the only main-text primary row, B2 is legacy honesty-only, B3 is broader-support legacy context, PyGNOME remains comparator-only, and the support-side B1 rebuild uses degraded forcing continuation if a temporary provider outage blocks only forcing acquisition.
  Run with: `./start.ps1 -Entry mindoro_march13_14_noaa_reinit_stress_test -NoPause`

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
- `prototype_legacy_final_figures`: Prototype 2016 final paper figures. Workflow mode = `prototype_2016`. Cost = `cheap_read_only`. Safe read-only default = `true`. Phases = prototype_legacy_final_figures.
  Note: Builds output/2016 Legacy Runs FINAL Figures as the authoritative curated prototype_2016 legacy support package without rerunning the scientific workflow. The structured package now includes publication/phase3a, publication/phase4, publication/phase4_comparator when stored comparator outputs exist, scientific_source_pngs, summary, manifests, and phase5 notes; output/figure_package_publication remains the generic repo-wide publication package and is not the authoritative 2016 Phase 4 package.
  Run with: `./start.ps1 -Entry prototype_legacy_final_figures -NoPause`

### Legacy prototype tracks

Backward-compatible prototype workflows preserved for debugging and regression.

- `prototype_2021_bundle`: Prototype 2021 preferred debug bundle. Workflow mode = `prototype_2021`. Cost = `moderate`. Safe read-only default = `false`. Phases = prep, 1_2, benchmark, prototype_pygnome_similarity_summary.
  Note: Preferred debug/demo lane only. Built from the two accepted 2021 strict-gate drifter segments, uses the official four-recipe Phase 1 family, and stops at the transport-core bundle. Phase 3B and Phase 4 are separate and are not part of this proof path.
  Run with: `./start.ps1 -Entry prototype_2021_bundle -NoPause`
- `prototype_legacy_bundle`: Prototype 2016 legacy bundle. Workflow mode = `prototype_2016`. Cost = `moderate`. Safe read-only default = `false`. Phases = prep, 1_2, benchmark, prototype_pygnome_similarity_summary, prototype_legacy_phase4_weathering, prototype_legacy_final_figures.
  Note: Backward-compatible legacy debug/regression path only. Not the preferred debug lane and not the final Chapter 3 Phase 1 study. Prototype prep now attempts GFS too, but missing GFS remains best-effort and does not collapse the legacy bundle. The visible thesis-facing legacy flow is Phase 1 -> Phase 2 -> Phase 3A -> Phase 4 -> Phase 5; the scientific rerun chain stops at Phase 4 and the bundle finishes with the read-only prototype_legacy_final_figures export. Repo-wide phase5_sync remains a separate cross-repo packaging layer. The PyGNOME similarity step is transport-only, comparator-only, and now surfaces deterministic plus p50/p90 legacy support tracks. There is no thesis-facing prototype_2016 Phase 3B or Phase 3C.
  Run with: `./start.ps1 -Entry prototype_legacy_bundle -NoPause`

## Guardrails

- `prototype_2016` remains available for debugging and regression only; it is not the final Phase 1 study.
- `prototype_2016` is thesis-facing only as Phase 1 / 2 / 3A / 4 / 5, with its dedicated curated package rooted at `output/2016 Legacy Runs FINAL Figures` and no thesis-facing 3B/3C lane. Repo-wide `phase5_sync` remains a separate cross-repo reproducibility layer.
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
