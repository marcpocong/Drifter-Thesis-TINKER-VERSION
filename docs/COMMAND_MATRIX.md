# Command Matrix

## First Principles

- Panel mode is the defense-safe default path.
- The full launcher is the researcher / audit path.
- Use launcher entry IDs from [config/launcher_matrix.json](../config/launcher_matrix.json) as the user-facing vocabulary.
- Panel mode and read-only entries do not rerun science.

## Current Manuscript Evidence Order

1. Focused Mindoro Phase 1 provenance
2. Phase 2 standardized forecast products
3. Mindoro `B1` primary public-observation validation
4. Mindoro `Track A` comparator-only support
5. DWH external transfer validation
6. Mindoro oil-type and shoreline support/context
7. `prototype_2016` legacy/archive support
8. Reproducibility / governance / read-only package layer

## Panel Default

Start here:

```powershell
.\panel.ps1
.\start.ps1 -Panel
```

Safe inspection helpers:

```powershell
.\start.ps1 -List -NoPause
.\start.ps1 -ListRole primary_evidence -NoPause
.\start.ps1 -ValidateMatrix -NoPause
.\start.ps1 -Help -NoPause
.\start.ps1 -Explain mindoro_phase3b_primary_public_validation -NoPause
.\start.ps1 -Explain mindoro_phase3b_primary_public_validation -ExportPlan -NoPause
.\start.ps1 -Entry mindoro_phase3b_primary_public_validation -DryRun -NoPause
```

Shared menu controls:

- `B`, `BACK`, `0` = go back when a previous launcher menu exists
- `C`, `CANCEL` = cancel the current selection cleanly
- `Q`, `QUIT`, `EXIT` = exit cleanly
- `H`, `HELP` = show help
- `L`, `LIST` = show the launcher catalog
- `S`, `SEARCH` = search entry IDs, thesis roles, run kinds, categories, and notes
- `P`, `PANEL` = open the defense-safe panel path
- `U`, `UI` = open the read-only dashboard
- `R`, `RESTART` = restart the read-only dashboard when that shortcut is available
- `X`, `INSPECT` = inspect entries inline inside a launcher section without running them
- `E`, `EXPORT` = after inspect/search preview, export `output/launcher_plans/<entry_id>.md` and `.json` without running science

## Which Command Should I Run?

| Goal | Command |
| --- | --- |
| Defense / panel inspection | `.\panel.ps1` |
| Audit launcher entries without Docker or science | `.\start.ps1 -ValidateMatrix -NoPause` or `python -m src.utils.validate_launcher_matrix` |
| Open dashboard only | panel option `1` or `U` / `UI`; the dashboard launch is a shortcut, not a separate launcher entry ID. Direct container form: `docker compose exec pipeline python -m streamlit run ui/app.py --server.address 0.0.0.0 --server.port 8501` |
| Inspect drifter provenance behind `B1` | panel option `7`, or `.\start.ps1 -Entry b1_drifter_context_panel` |
| Inspect data sources and provenance | panel option `8`, open [DATA_SOURCES.md](DATA_SOURCES.md), inspect [config/data_sources.yaml](../config/data_sources.yaml), or use the dashboard `Data Sources & Provenance` reference page |
| Verify manuscript numbers | panel option `2` |
| Rebuild publication figures from stored outputs only | panel option `3`, or `.\start.ps1 -Entry figure_package_publication` |
| Preview a launcher entry without Docker execution | `.\start.ps1 -Entry <entry_id> -DryRun -NoPause` |
| Export a run plan without Docker execution | `.\start.ps1 -Explain <entry_id> -ExportPlan -NoPause` |
| Run focused Phase 1 provenance rerun intentionally | `.\start.ps1 -Entry phase1_mindoro_focus_provenance` |
| Run `B1` rerun intentionally | `.\start.ps1 -Entry mindoro_phase3b_primary_public_validation` |
| Run DWH rerun intentionally | `.\start.ps1 -Entry dwh_reportable_bundle` |
| Run Mindoro oil-type / shoreline support intentionally | `.\start.ps1 -Entry mindoro_phase4_only` |
| Open legacy/archive support intentionally | `.\start.ps1 -Entry prototype_legacy_final_figures` or `.\start.ps1 -Entry prototype_legacy_bundle` |

## Panel Menu Options

These labels should match the live `Show-PanelMenu` output exactly.

| Option | Live label | Notes |
| --- | --- | --- |
| `1` | Open read-only dashboard | Dashboard launch shortcut only; no separate launcher entry ID |
| `2` | Verify paper numbers against stored scorecards | Read-only manuscript verification |
| `3` | Rebuild publication figures from stored outputs | Packaging-only refresh |
| `4` | Refresh final validation package from stored outputs | Packaging-only refresh |
| `5` | Refresh final reproducibility package / command documentation | Packaging-only governance/doc sync via `phase5_sync` |
| `6` | Show paper-to-output registry | Read-only manuscript/output map |
| `7` | View B1 drifter provenance/context | Read-only `B1` provenance/context panel |
| `8` | View data sources and provenance registry | Opens `docs/DATA_SOURCES.md`; no downloads, reruns, or science rewrites |
| `A` | Open full research launcher | Leaves panel mode for the advanced launcher |
| `U` | Open read-only dashboard shortcut | Same dashboard shortcut as option `1` |
| `R` | Restart the read-only dashboard | Restarts the dashboard helper |
| `L` | List launcher catalog | Read-only catalog view |
| `H` | Help / interpretation guide | Read-only panel guide |
| `B` | Back or launcher home | Returns to caller when panel mode was opened from the launcher |
| `C` | Cancel and return | Clean cancel without an error banner |
| `Q` | Exit | Clean launcher exit |

## Role Groups

| Launcher group | Preferred entries | Notes |
| --- | --- | --- |
| Main thesis evidence / reportable | `phase1_mindoro_focus_provenance`, `mindoro_phase3b_primary_public_validation`, `dwh_reportable_bundle`, `mindoro_reportable_core` | Intentional scientific reruns only. |
| Support/context and appendix | `mindoro_phase4_only`, `mindoro_appendix_sensitivity_bundle` | Support/context only; not main-text validation. |
| Archive/provenance | `phase1_regional_reference_rerun`, `mindoro_march13_14_phase1_focus_trial`, `mindoro_march6_recovery_sensitivity`, `mindoro_march23_extended_public_stress_test` | Archive, provenance, or governance lanes only. |
| Legacy/debug | `prototype_legacy_final_figures`, `prototype_2021_bundle`, `prototype_legacy_bundle` | Legacy support/debug only. |
| Read-only dashboard / packaging / audits / docs | `b1_drifter_context_panel`, `phase1_audit`, `phase2_audit`, `final_validation_package`, `phase5_sync`, `trajectory_gallery`, `trajectory_gallery_panel`, `figure_package_publication` | Stored-output-only or packaging-only actions. Dashboard launch itself is a shortcut, not a catalog entry ID. |

Data-source provenance is a read-only documentation/UI layer, not a launcher rerun entry. Use `docs/DATA_SOURCES.md`, `config/data_sources.yaml`, or the dashboard reference page.

## Launcher Entry Map

| Entry ID | Thesis role | Run kind | Recommended for | Interactive command | Prompt-free phase mapping |
| --- | --- | --- | --- | --- | --- |
| `phase1_mindoro_focus_provenance` | primary evidence | `scientific_rerun` | researcher | `.\start.ps1 -Entry phase1_mindoro_focus_provenance` | `pipeline: phase1_production_rerun` |
| `mindoro_phase3b_primary_public_validation` | primary evidence | `scientific_rerun` | researcher | `.\start.ps1 -Entry mindoro_phase3b_primary_public_validation` | `pipeline: phase3b_extended_public -> phase3b_extended_public_scored_march13_14_reinit` |
| `dwh_reportable_bundle` | primary evidence | `scientific_rerun` | researcher | `.\start.ps1 -Entry dwh_reportable_bundle` | `pipeline: phase3c_external_case_setup -> dwh_phase3c_scientific_forcing_ready -> phase3c_external_case_run -> phase3c_external_case_ensemble_comparison; gnome: phase3c_dwh_pygnome_comparator` |
| `mindoro_reportable_core` | primary evidence | `scientific_rerun` | auditor | `.\start.ps1 -Entry mindoro_reportable_core` | `pipeline: prep -> 1_2 -> phase3b_extended_public -> phase3b_extended_public_scored_march13_14_reinit -> 3b -> phase3b_multidate_public -> phase4_oiltype_and_shoreline` |
| `mindoro_phase4_only` | support/context | `scientific_rerun` | researcher | `.\start.ps1 -Entry mindoro_phase4_only` | `pipeline: phase4_oiltype_and_shoreline` |
| `mindoro_appendix_sensitivity_bundle` | support/context | `archive_rerun` | researcher | `.\start.ps1 -Entry mindoro_appendix_sensitivity_bundle` | `pipeline: public_obs_appendix -> phase3b_extended_public -> phase3b_extended_public_scored -> phase3b_extended_public_scored_march23 -> phase3b_extended_public_scored_march13_14_reinit -> horizon_survival_audit -> transport_retention_fix -> official_rerun_r1 -> init_mode_sensitivity_r1 -> source_history_reconstruction_r1 -> ensemble_threshold_sensitivity -> recipe_sensitivity_r1_multibranch; gnome: phase3b_extended_public_scored_march13_14_reinit_pygnome_comparison -> pygnome_public_comparison` |
| `phase1_regional_reference_rerun` | archive/provenance | `archive_rerun` | auditor | `.\start.ps1 -Entry phase1_regional_reference_rerun` | `pipeline: phase1_production_rerun` |
| `mindoro_march13_14_phase1_focus_trial` | archive/provenance | `archive_rerun` | auditor | `.\start.ps1 -Entry mindoro_march13_14_phase1_focus_trial` | `pipeline: phase3b_extended_public -> mindoro_march13_14_phase1_focus_trial` |
| `mindoro_march6_recovery_sensitivity` | archive/provenance | `archive_rerun` | auditor | `.\start.ps1 -Entry mindoro_march6_recovery_sensitivity` | `pipeline: march6_recovery_sensitivity` |
| `mindoro_march23_extended_public_stress_test` | archive/provenance | `archive_rerun` | researcher | `.\start.ps1 -Entry mindoro_march23_extended_public_stress_test` | `pipeline: phase3b_extended_public -> phase3b_extended_public_scored_march23` |
| `phase1_audit` | read-only governance | `read_only` | auditor | `.\start.ps1 -Entry phase1_audit` | `pipeline: phase1_finalization_audit` |
| `phase2_audit` | read-only governance | `read_only` | auditor | `.\start.ps1 -Entry phase2_audit` | `pipeline: phase2_finalization_audit` |
| `b1_drifter_context_panel` | read-only governance | `read_only` | panel | `.\start.ps1 -Entry b1_drifter_context_panel` | `pipeline: panel_b1_drifter_context` |
| `final_validation_package` | read-only governance | `packaging_only` | auditor | `.\start.ps1 -Entry final_validation_package` | `pipeline: final_validation_package` |
| `phase5_sync` | read-only governance | `packaging_only` | auditor | `.\start.ps1 -Entry phase5_sync` | `pipeline: phase5_launcher_and_docs_sync` |
| `trajectory_gallery` | read-only governance | `packaging_only` | auditor | `.\start.ps1 -Entry trajectory_gallery` | `pipeline: trajectory_gallery_build` |
| `trajectory_gallery_panel` | read-only governance | `packaging_only` | auditor | `.\start.ps1 -Entry trajectory_gallery_panel` | `pipeline: trajectory_gallery_panel_polish` |
| `figure_package_publication` | read-only governance | `packaging_only` | auditor | `.\start.ps1 -Entry figure_package_publication` | `pipeline: figure_package_publication` |
| `prototype_legacy_final_figures` | legacy support | `packaging_only` | auditor | `.\start.ps1 -Entry prototype_legacy_final_figures` | `pipeline: prototype_legacy_final_figures` |
| `prototype_2021_bundle` | legacy support | `scientific_rerun` | developer | `.\start.ps1 -Entry prototype_2021_bundle` | `pipeline: prep -> 1_2 -> prototype_pygnome_similarity_summary; gnome: benchmark` |
| `prototype_legacy_bundle` | legacy support | `scientific_rerun` | developer | `.\start.ps1 -Entry prototype_legacy_bundle` | `pipeline: prep -> 1_2 -> prototype_pygnome_similarity_summary -> prototype_legacy_final_figures; gnome: benchmark -> prototype_legacy_phase4_weathering` |

## Compatibility Aliases / Hidden Legacy IDs

| Hidden ID | Prefer instead | Notes |
| --- | --- | --- |
| `phase1_mindoro_focus_pre_spill_experiment` | `phase1_mindoro_focus_provenance` | Same focused Mindoro provenance workflow. |
| `phase1_production_rerun` | `phase1_regional_reference_rerun` | Same broader regional reference/governance workflow. |
| `mindoro_march13_14_noaa_reinit_stress_test` | `mindoro_phase3b_primary_public_validation` | Hidden legacy ID for older scripts only; it resolves to the canonical `B1` entry and does not run the Track A/PyGNOME comparator lane. |

## Exact Prompt-Free Read-Only Commands

```bash
docker compose exec -T -e WORKFLOW_MODE=mindoro_retro_2023 -e PIPELINE_PHASE=phase1_finalization_audit pipeline python -m src
docker compose exec -T -e WORKFLOW_MODE=mindoro_retro_2023 -e PIPELINE_PHASE=phase2_finalization_audit pipeline python -m src
docker compose exec -T -e WORKFLOW_MODE=mindoro_retro_2023 -e PIPELINE_PHASE=panel_b1_drifter_context pipeline python -m src
docker compose exec -T -e WORKFLOW_MODE=mindoro_retro_2023 -e PIPELINE_PHASE=final_validation_package pipeline python -m src
docker compose exec -T -e WORKFLOW_MODE=mindoro_retro_2023 -e PIPELINE_PHASE=phase5_launcher_and_docs_sync pipeline python -m src
docker compose exec -T -e WORKFLOW_MODE=mindoro_retro_2023 -e PIPELINE_PHASE=trajectory_gallery_build pipeline python -m src
docker compose exec -T -e WORKFLOW_MODE=mindoro_retro_2023 -e PIPELINE_PHASE=trajectory_gallery_panel_polish pipeline python -m src
docker compose exec -T -e WORKFLOW_MODE=mindoro_retro_2023 -e PIPELINE_PHASE=figure_package_publication pipeline python -m src
docker compose exec -T -e WORKFLOW_MODE=prototype_2016 -e PIPELINE_PHASE=prototype_legacy_final_figures pipeline python -m src
```

## Exact Prompt-Free Main Evidence Sequences

Focused Mindoro Phase 1 provenance:

```bash
docker compose exec -T -e WORKFLOW_MODE=phase1_mindoro_focus_pre_spill_2016_2023 -e PIPELINE_PHASE=phase1_production_rerun pipeline python -m src
```

Mindoro `B1` primary public validation:

```bash
docker compose exec -T -e WORKFLOW_MODE=mindoro_retro_2023 -e PIPELINE_PHASE=phase3b_extended_public pipeline python -m src
docker compose exec -T -e WORKFLOW_MODE=mindoro_retro_2023 -e PIPELINE_PHASE=phase3b_extended_public_scored_march13_14_reinit pipeline python -m src
```

DWH external transfer validation:

```bash
docker compose exec -T -e WORKFLOW_MODE=dwh_retro_2010 -e PIPELINE_PHASE=phase3c_external_case_setup pipeline python -m src
docker compose exec -T -e WORKFLOW_MODE=dwh_retro_2010 -e PIPELINE_PHASE=dwh_phase3c_scientific_forcing_ready pipeline python -m src
docker compose exec -T -e WORKFLOW_MODE=dwh_retro_2010 -e PIPELINE_PHASE=phase3c_external_case_run pipeline python -m src
docker compose exec -T -e WORKFLOW_MODE=dwh_retro_2010 -e PIPELINE_PHASE=phase3c_external_case_ensemble_comparison pipeline python -m src
docker compose exec -T -e WORKFLOW_MODE=dwh_retro_2010 -e PIPELINE_PHASE=phase3c_dwh_pygnome_comparator gnome python -m src
```

## Guardrails

- `B1` is the only main Philippine public-observation validation claim.
- `B1` supports coastal-neighborhood usefulness, not exact 1 km overlap or universal operational accuracy.
- The March 13-14 `B1` pair keeps the shared-imagery caveat explicit.
- `Track A` and every PyGNOME branch remain comparator-only support.
- PyGNOME is never observational truth.
- DWH is external transfer validation, not Mindoro recalibration.
- Mindoro oil-type and shoreline outputs remain support/context only.
- Read-only dashboard, packaging, audit, and docs entries do not recompute science.
- `prototype_2016` is legacy/archive support only; some internal package names may still contain Phase 4/Phase 5 labels, but those are not primary defended evidence.
- `output/phase1_production_rerun/phase1_baseline_selection_candidate.yaml` remains a staged-only candidate for the broader regional/reference lane; any promotion into `config/phase1_baseline_selection.yaml` stays explicit and manual, and this does not affect the finalized focused Mindoro `B1` `cmems_gfs` provenance.
