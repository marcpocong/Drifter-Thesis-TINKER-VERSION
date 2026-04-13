# Command Matrix

## PowerShell Launcher Commands

| Entry ID | Category | Workflow Mode | Typical Use | Cost | Command |
| --- | --- | --- | --- | --- | --- |
| `phase1_audit` | read-only packaging/help | `mindoro_retro_2023` | refresh Phase 1 architecture audit | cheap | `.\start.ps1 -Entry phase1_audit -NoPause` |
| `phase2_audit` | read-only packaging/help | `mindoro_retro_2023` | refresh Phase 2 semantics audit | cheap | `.\start.ps1 -Entry phase2_audit -NoPause` |
| `final_validation_package` | read-only packaging/help | `mindoro_retro_2023` | rebuild frozen validation summaries from existing outputs | cheap | `.\start.ps1 -Entry final_validation_package -NoPause` |
| `phase5_sync` | read-only packaging/help | `mindoro_retro_2023` | rebuild reproducibility/package support indexes | cheap | `.\start.ps1 -Entry phase5_sync -NoPause` |
| `trajectory_gallery` | read-only packaging/help | `mindoro_retro_2023` | build the static technical figure gallery from existing outputs | cheap | `.\start.ps1 -Entry trajectory_gallery -NoPause` |
| `trajectory_gallery_panel` | read-only packaging/help | `mindoro_retro_2023` | build the polished panel-ready figure board pack from existing outputs | cheap | `.\start.ps1 -Entry trajectory_gallery_panel -NoPause` |
| `figure_package_publication` | read-only packaging/help | `mindoro_retro_2023` | build the canonical publication-grade single-figure and board package from existing outputs | cheap | `.\start.ps1 -Entry figure_package_publication -NoPause` |
| `prototype_legacy_final_figures` | read-only packaging/help | `prototype_2016` | build the curated legacy-2016 Phase 5 final paper-figure folder from existing Phase 2, benchmark, and similarity outputs | cheap | `.\start.ps1 -Entry prototype_legacy_final_figures -NoPause` |
| `phase1_production_rerun` | scientific/reportable | `phase1_regional_2016_2022` | run the full 2016-2022 historical/regional Phase 1 reference rerun and stage the broader regional candidate baseline artifact only | expensive | `.\start.ps1 -Entry phase1_production_rerun -NoPause` |
| `mindoro_phase3b_primary_public_validation` | scientific/reportable | `mindoro_retro_2023` | rerun the canonical Mindoro March 13 -> March 14 Phase 3B public-validation row without rewriting the frozen March 3 -> March 6 case definition | expensive | `.\start.ps1 -Entry mindoro_phase3b_primary_public_validation -NoPause` |
| `mindoro_phase4_only` | scientific/reportable | `mindoro_retro_2023` | rerun only the Mindoro support-only Phase 4 oil-type/shoreline bundle | moderate | `.\start.ps1 -Entry mindoro_phase4_only -NoPause` |
| `mindoro_reportable_core` | scientific/reportable | `mindoro_retro_2023` | intentional rerun of the main Mindoro Phase 2 -> B1 primary-validation chain plus visible B2/B3/support-only Phase 4 layers | expensive | `.\start.ps1 -Entry mindoro_reportable_core -NoPause` |
| `dwh_reportable_bundle` | scientific/reportable | `dwh_retro_2010` | intentional rerun of the DWH Phase 3C transfer-validation bundle | expensive | `.\start.ps1 -Entry dwh_reportable_bundle -NoPause` |
| `mindoro_appendix_sensitivity_bundle` | sensitivity/appendix | `mindoro_retro_2023` | rerun appendix and sensitivity branches | expensive | `.\start.ps1 -Entry mindoro_appendix_sensitivity_bundle -NoPause` |
| `phase1_mindoro_focus_pre_spill_experiment` | sensitivity/appendix | `phase1_mindoro_focus_pre_spill_2016_2023` | separate Mindoro-focused Phase 1 provenance rerun that now supplies the active B1 recipe-provenance story | expensive | `.\start.ps1 -Entry phase1_mindoro_focus_pre_spill_experiment` |
| `mindoro_march13_14_noaa_reinit_stress_test` | sensitivity/appendix | `mindoro_retro_2023` | backward-compatible alias that still resolves to the promoted March 13 -> March 14 bundle and the same-case comparator-support A lane | expensive | `.\start.ps1 -Entry mindoro_march13_14_noaa_reinit_stress_test -NoPause` |
| `prototype_2021_bundle` | legacy prototype | `prototype_2021` | preferred accepted-segment debug/demo path; exact 2021 drifter windows, official Phase 1 recipe family only, and transport-core bundle only | moderate | `.\start.ps1 -Entry prototype_2021_bundle -NoPause` |
| `prototype_legacy_bundle` | legacy prototype | `prototype_2016` | legacy debug/regression only; visible thesis-facing support flow is `Phase 1 -> Phase 2 -> Phase 3A -> Phase 4 -> Phase 5`, with best-effort GFS prep, deterministic plus `p50`/`p90` comparator outputs, and no thesis-facing 3B/3C lane | moderate | `.\start.ps1 -Entry prototype_legacy_bundle -NoPause` |

## Direct Docker Commands

Interactive direct Docker runs:

```bash
docker-compose exec -e WORKFLOW_MODE=phase1_mindoro_focus_pre_spill_2016_2023 -e PIPELINE_PHASE=phase1_production_rerun pipeline python -m src
```

Omit `-T` when you want the direct container command to ask once about the forcing wait budget and input-cache policy. Keep `-T` when you want a prompt-free run that resolves those values silently and prints the chosen startup policy at process start.

Read-only utilities:

```bash
docker-compose exec -T -e PIPELINE_PHASE=phase1_finalization_audit pipeline python -m src
docker-compose exec -T -e PIPELINE_PHASE=phase2_finalization_audit pipeline python -m src
docker-compose exec -T -e PIPELINE_PHASE=final_validation_package pipeline python -m src
docker-compose exec -T -e PIPELINE_PHASE=phase5_launcher_and_docs_sync pipeline python -m src
docker-compose exec -T -e PIPELINE_PHASE=trajectory_gallery_build pipeline python -m src
docker-compose exec -T -e PIPELINE_PHASE=trajectory_gallery_panel_polish pipeline python -m src
docker-compose exec -T -e PIPELINE_PHASE=figure_package_publication pipeline python -m src
docker-compose exec -T -e WORKFLOW_MODE=prototype_2016 -e PIPELINE_PHASE=prototype_legacy_final_figures pipeline python -m src
docker-compose exec pipeline python -m streamlit run ui/app.py --server.address 0.0.0.0 --server.port 8501
```

The Streamlit command launches the read-only local dashboard. It is intentionally documented as a direct command rather than a scientific launcher entry.

Scientific lanes:

```bash
docker-compose exec -T -e WORKFLOW_MODE=phase1_regional_2016_2022 -e PIPELINE_PHASE=phase1_production_rerun pipeline python -m src
docker-compose exec -T -e WORKFLOW_MODE=phase1_regional_2016_2022 -e FORCING_OUTAGE_POLICY=continue_degraded -e PIPELINE_PHASE=phase1_production_rerun pipeline python -m src
docker-compose exec -T -e WORKFLOW_MODE=mindoro_retro_2023 -e PIPELINE_PHASE=prep pipeline python -m src
docker-compose exec -T -e WORKFLOW_MODE=mindoro_retro_2023 pipeline python -m src
docker-compose exec -T -e WORKFLOW_MODE=mindoro_retro_2023 -e PIPELINE_PHASE=official_phase3b pipeline python -m src
docker-compose exec -T -e WORKFLOW_MODE=mindoro_retro_2023 -e PIPELINE_PHASE=phase3b_extended_public_scored_march13_14_reinit pipeline python -m src
docker-compose exec -T -e WORKFLOW_MODE=mindoro_retro_2023 -e PIPELINE_PHASE=phase3b_multidate_public pipeline python -m src
docker-compose exec -T -e WORKFLOW_MODE=mindoro_retro_2023 -e PIPELINE_PHASE=phase4_oiltype_and_shoreline pipeline python -m src
docker-compose exec -T -e WORKFLOW_MODE=mindoro_retro_2023 -e PIPELINE_PHASE=phase3b_extended_public_scored_march13_14_reinit_pygnome_comparison gnome python -m src
```

Experimental/support lanes:

```bash
docker-compose exec -T -e WORKFLOW_MODE=phase1_mindoro_focus_pre_spill_2016_2023 -e PIPELINE_PHASE=phase1_production_rerun -e INPUT_CACHE_POLICY=reuse_if_valid -e FORCING_SOURCE_BUDGET_SECONDS=120 -e FORCING_OUTAGE_POLICY=continue_degraded pipeline python -m src
```

Candidate-baseline trial for the official Mindoro spill-case lane:

```bash
docker-compose exec -T -e WORKFLOW_MODE=mindoro_retro_2023 -e BASELINE_SELECTION_PATH=output/phase1_production_rerun/phase1_baseline_selection_candidate.yaml pipeline python -m src
docker-compose exec -T -e WORKFLOW_MODE=mindoro_retro_2023 -e BASELINE_SELECTION_PATH=output/phase1_production_rerun/phase1_baseline_selection_candidate.yaml -e PIPELINE_PHASE=phase4_oiltype_and_shoreline pipeline python -m src
```

DWH lane:

```bash
docker-compose exec -T -e WORKFLOW_MODE=dwh_retro_2010 -e PIPELINE_PHASE=phase3c_external_case_setup pipeline python -m src
docker-compose exec -T -e WORKFLOW_MODE=dwh_retro_2010 -e PIPELINE_PHASE=dwh_phase3c_scientific_forcing_ready pipeline python -m src
docker-compose exec -T -e WORKFLOW_MODE=dwh_retro_2010 -e FORCING_OUTAGE_POLICY=continue_degraded -e PIPELINE_PHASE=dwh_phase3c_scientific_forcing_ready pipeline python -m src
docker-compose exec -T -e WORKFLOW_MODE=dwh_retro_2010 -e PIPELINE_PHASE=phase3c_external_case_run pipeline python -m src
docker-compose exec -T -e WORKFLOW_MODE=dwh_retro_2010 -e PIPELINE_PHASE=phase3c_external_case_ensemble_comparison pipeline python -m src
docker-compose exec -T -e WORKFLOW_MODE=dwh_retro_2010 -e PIPELINE_PHASE=phase3c_dwh_pygnome_comparator gnome python -m src
```

Preferred debug lane:

```bash
docker-compose exec -T -e WORKFLOW_MODE=prototype_2021 -e PIPELINE_PHASE=prep pipeline python -m src
docker-compose exec -T -e WORKFLOW_MODE=prototype_2021 pipeline python -m src
docker-compose exec -T -e WORKFLOW_MODE=prototype_2021 -e PIPELINE_PHASE=benchmark gnome python -m src
docker-compose exec -T -e WORKFLOW_MODE=prototype_2021 -e PIPELINE_PHASE=prototype_pygnome_similarity_summary pipeline python -m src
```

Legacy prototype Phase 1 / 2 / 3A / 4 / 5 lane:

```bash
docker-compose exec -T -e WORKFLOW_MODE=prototype_2016 -e PIPELINE_PHASE=prep pipeline python -m src
docker-compose exec -T -e WORKFLOW_MODE=prototype_2016 pipeline python -m src
docker-compose exec -T -e WORKFLOW_MODE=prototype_2016 -e PIPELINE_PHASE=benchmark gnome python -m src
docker-compose exec -T -e WORKFLOW_MODE=prototype_2016 -e PIPELINE_PHASE=prototype_pygnome_similarity_summary pipeline python -m src
docker-compose exec -T -e WORKFLOW_MODE=prototype_2016 -e PIPELINE_PHASE=prototype_legacy_phase4_weathering gnome python -m src
docker-compose exec -T -e WORKFLOW_MODE=prototype_2016 -e PIPELINE_PHASE=prototype_legacy_final_figures pipeline python -m src
```

`prototype_2021` fetches only the fixed accepted 2021 drifter rows, uses the official four-recipe Phase 1 family, and writes its support-only similarity package to `output/prototype_2021_pygnome_similarity/`. `prototype_2016` remains preserved as the historical legacy lane because it captures the earliest prototype stage of the study, when the ingestion-and-validation pipeline was first exercised on 2016 drifter records in the Palawan-side western Philippine context before the study widened toward the broader west-coast Palawan/Mindoro context. Thesis-facing, it remains legacy support only as `Phase 1 -> Phase 2 -> Phase 3A -> Phase 4 -> Phase 5`, with no thesis-facing 3B/3C lane. Its Phase 3A OpenDrift-versus-deterministic-PyGNOME comparator stays support-only: a non-zero fraction skill score means the ensemble footprint was not completely disjoint from the deterministic PyGNOME forecast, not that PyGNOME is truth or that the lane is final proof. Its curated paper-facing export now writes to `output/2016 Legacy Runs FINAL Figures/`, while `phase5_sync` remains the repo-wide read-only support refresh utility and `output/figure_package_publication/` remains the canonical generic publication package.
For `prototype_2016`, the release origin is the selected drifter-of-record start from `data/drifters/CASE_2016-*/drifters_noaa.csv`. Legacy `source_point_metadata.geojson` references can still show up in some compatibility-oriented manifests, but they are not the actual spill origin for those runs.

## Guardrails

- Use the read-only utilities for status refreshes and packaging work.
- Use the scientific rerun entries only when a deliberate rerun is actually desired.
- Do not interpret either prototype lane as the final Phase 1 study.
- Prototype GFS is best-effort in the legacy lane; the dedicated Phase 1 regional rerun remains the strict GFS-required workflow.
- `FORCING_OUTAGE_POLICY=default|continue_degraded|fail_hard` controls forcing-only outage behavior. By default, scientific/reportable lanes fail hard; appendix/legacy/experimental lanes continue only in degraded mode and mark outputs as provisional with `rerun_required=true`.
- `FORCING_SOURCE_BUDGET_SECONDS=300` is the shared default fail-fast budget for each forcing-provider call. Set `0` only when you intentionally want legacy unlimited waiting for debugging.
- `INPUT_CACHE_POLICY=default|reuse_if_valid|force_refresh` controls whether eligible local input caches are reused or refreshed. Interactive launcher/direct runs can resolve this once at startup; prompt-free runs default to `reuse_if_valid`.
- Direct `docker-compose exec -T ...` runs do not prompt. They now print the resolved startup policy and point you back to the matching launcher entry or non-`-T` direct form when you want the one-time startup questions instead.
- The launcher treats the degraded forcing-skip exit as non-fatal for allowed appendix/legacy/experimental entries, prints the skip reason, and continues to the next step.
- Drifter truth and ArcGIS/observation truth inputs remain hard requirements even when degraded forcing continuation is enabled.
- Treat `Phase 3B` and `Phase 3C` as validation-only lanes.
- Keep `prototype_2016` on the legacy `Phase 1 / 2 / 3A / 4 / 5` support story; `3` and `3b` survive only as deprecated compatibility aliases and are not visible thesis-facing steps.
- Treat the selected drifter-of-record start as the authoritative `prototype_2016` release point, even if a legacy audit field still names `source_point_metadata.geojson`.
- The dedicated Phase 1 rerun stages `output/phase1_production_rerun/phase1_baseline_selection_candidate.yaml` only; do not treat it as an automatic overwrite of `config/phase1_baseline_selection.yaml`.
- Use `BASELINE_SELECTION_PATH` for downstream trials against the staged candidate; keep promotion of `config/phase1_baseline_selection.yaml` explicit and manual.
- Keep `config/case_mindoro_retro_2023.yaml` frozen as the March 3 -> March 6 base case; the promoted March 13 -> March 14 B1 row is carried by amendment, not by silent rewrite.
- Keep the Mindoro-focused Phase 1 rerun as the active Mindoro-specific recipe-provenance path for B1; it does not rewrite stored B1 raw-generation history or claim direct drifter ingestion inside Phase 3B.
- `mindoro_march13_14_noaa_reinit_stress_test` is now an alias, not the authoritative scientific label.
- Thesis-facing Mindoro sequencing is separate focused Phase 1 confirmation -> Phase 2 -> Phase 3B primary validation.
- Mindoro track semantics are locked as B1 only-primary, A same-case comparator-support, B2 March 6 legacy honesty, and B3 March 3-6 broader-support legacy context.
- Do not claim independent day-to-day validation for March 13 -> March 14 while the shared-imagery caveat still applies, and do not treat PyGNOME as anything other than same-case comparator evidence.
