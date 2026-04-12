# Command Matrix

## PowerShell Launcher Commands

| Entry ID | Category | Workflow Mode | Typical Use | Cost | Command |
| --- | --- | --- | --- | --- | --- |
| `phase1_audit` | read-only packaging/help | `mindoro_retro_2023` | refresh Phase 1 architecture audit | cheap | `.\start.ps1 -Entry phase1_audit -NoPause` |
| `phase2_audit` | read-only packaging/help | `mindoro_retro_2023` | refresh Phase 2 semantics audit | cheap | `.\start.ps1 -Entry phase2_audit -NoPause` |
| `final_validation_package` | read-only packaging/help | `mindoro_retro_2023` | rebuild frozen validation summaries from existing outputs | cheap | `.\start.ps1 -Entry final_validation_package -NoPause` |
| `phase5_sync` | read-only packaging/help | `mindoro_retro_2023` | rebuild reproducibility/package sync indexes | cheap | `.\start.ps1 -Entry phase5_sync -NoPause` |
| `trajectory_gallery` | read-only packaging/help | `mindoro_retro_2023` | build the static technical figure gallery from existing outputs | cheap | `.\start.ps1 -Entry trajectory_gallery -NoPause` |
| `trajectory_gallery_panel` | read-only packaging/help | `mindoro_retro_2023` | build the polished panel-ready figure board pack from existing outputs | cheap | `.\start.ps1 -Entry trajectory_gallery_panel -NoPause` |
| `figure_package_publication` | read-only packaging/help | `mindoro_retro_2023` | build the canonical publication-grade single-figure and board package from existing outputs | cheap | `.\start.ps1 -Entry figure_package_publication -NoPause` |
| `phase1_production_rerun` | scientific/reportable | `phase1_regional_2016_2022` | run the full 2016-2022 historical/regional Phase 1 rerun and stage a candidate baseline artifact only | expensive | `.\start.ps1 -Entry phase1_production_rerun -NoPause` |
| `mindoro_phase3b_primary_public_validation` | scientific/reportable | `mindoro_retro_2023` | rerun the canonical Mindoro March 13 -> March 14 Phase 3B public-validation row without rewriting the frozen March 3 -> March 6 case definition | expensive | `.\start.ps1 -Entry mindoro_phase3b_primary_public_validation -NoPause` |
| `mindoro_phase4_only` | scientific/reportable | `mindoro_retro_2023` | rerun only Mindoro Phase 4 | moderate | `.\start.ps1 -Entry mindoro_phase4_only -NoPause` |
| `mindoro_reportable_core` | scientific/reportable | `mindoro_retro_2023` | intentional rerun of the main Mindoro reportable chain | expensive | `.\start.ps1 -Entry mindoro_reportable_core -NoPause` |
| `dwh_reportable_bundle` | scientific/reportable | `dwh_retro_2010` | intentional rerun of the DWH Phase 3C bundle | expensive | `.\start.ps1 -Entry dwh_reportable_bundle -NoPause` |
| `mindoro_appendix_sensitivity_bundle` | sensitivity/appendix | `mindoro_retro_2023` | rerun appendix and sensitivity branches | expensive | `.\start.ps1 -Entry mindoro_appendix_sensitivity_bundle -NoPause` |
| `mindoro_march13_14_noaa_reinit_stress_test` | sensitivity/appendix | `mindoro_retro_2023` | backward-compatible alias that still resolves to the promoted March 13 -> March 14 bundle and comparator lane | expensive | `.\start.ps1 -Entry mindoro_march13_14_noaa_reinit_stress_test -NoPause` |
| `prototype_2021_bundle` | legacy prototype | `prototype_2021` | preferred accepted-segment debug/demo path; exact 2021 drifter windows, official Phase 1 recipe family only, and transport-core bundle only | moderate | `.\start.ps1 -Entry prototype_2021_bundle -NoPause` |
| `prototype_legacy_bundle` | legacy prototype | `prototype_2016` | legacy debug/regression only; prep attempts best-effort GFS, pads forcing coverage for the preserved ensemble jitter, writes the legacy similarity package after benchmark, and keeps `3`/`3b` only as appendix/smoke follow-ons | moderate | `.\start.ps1 -Entry prototype_legacy_bundle -NoPause` |

## Direct Docker Commands

Read-only utilities:

```bash
docker-compose exec -T -e PIPELINE_PHASE=phase1_finalization_audit pipeline python -m src
docker-compose exec -T -e PIPELINE_PHASE=phase2_finalization_audit pipeline python -m src
docker-compose exec -T -e PIPELINE_PHASE=final_validation_package pipeline python -m src
docker-compose exec -T -e PIPELINE_PHASE=phase5_launcher_and_docs_sync pipeline python -m src
docker-compose exec -T -e PIPELINE_PHASE=trajectory_gallery_build pipeline python -m src
docker-compose exec -T -e PIPELINE_PHASE=trajectory_gallery_panel_polish pipeline python -m src
docker-compose exec -T -e PIPELINE_PHASE=figure_package_publication pipeline python -m src
docker-compose exec pipeline python -m streamlit run ui/app.py --server.address 0.0.0.0 --server.port 8501
```

The Streamlit command launches the read-only local dashboard. It is intentionally documented as a direct command rather than a scientific launcher entry.

Scientific lanes:

```bash
docker-compose exec -T -e WORKFLOW_MODE=phase1_regional_2016_2022 -e PIPELINE_PHASE=phase1_production_rerun pipeline python -m src
docker-compose exec -T -e WORKFLOW_MODE=mindoro_retro_2023 -e PIPELINE_PHASE=prep pipeline python -m src
docker-compose exec -T -e WORKFLOW_MODE=mindoro_retro_2023 pipeline python -m src
docker-compose exec -T -e WORKFLOW_MODE=mindoro_retro_2023 -e PIPELINE_PHASE=official_phase3b pipeline python -m src
docker-compose exec -T -e WORKFLOW_MODE=mindoro_retro_2023 -e PIPELINE_PHASE=phase3b_extended_public_scored_march13_14_reinit pipeline python -m src
docker-compose exec -T -e WORKFLOW_MODE=mindoro_retro_2023 -e PIPELINE_PHASE=phase3b_multidate_public pipeline python -m src
docker-compose exec -T -e WORKFLOW_MODE=mindoro_retro_2023 -e PIPELINE_PHASE=phase4_oiltype_and_shoreline pipeline python -m src
docker-compose exec -T -e WORKFLOW_MODE=mindoro_retro_2023 -e PIPELINE_PHASE=phase3b_extended_public_scored_march13_14_reinit_pygnome_comparison gnome python -m src
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

Legacy prototype appendix/smoke lane:

```bash
docker-compose exec -T -e WORKFLOW_MODE=prototype_2016 -e PIPELINE_PHASE=prep pipeline python -m src
docker-compose exec -T -e WORKFLOW_MODE=prototype_2016 pipeline python -m src
docker-compose exec -T -e WORKFLOW_MODE=prototype_2016 -e PIPELINE_PHASE=benchmark gnome python -m src
docker-compose exec -T -e WORKFLOW_MODE=prototype_2016 -e PIPELINE_PHASE=prototype_pygnome_similarity_summary pipeline python -m src
docker-compose exec -T -e WORKFLOW_MODE=prototype_2016 -e PIPELINE_PHASE=3 gnome python -m src
```

`prototype_2021` fetches only the fixed accepted 2021 drifter rows, uses the official four-recipe Phase 1 family, and writes its support-only similarity package to `output/prototype_2021_pygnome_similarity/`. `prototype_2016` remains preserved as the historical legacy lane. `Phase 4 = Oil-Type Fate and Shoreline Impact Analysis` and is separate from both drifter-debug workflows.

## Guardrails

- Use the read-only utilities for status refreshes and packaging work.
- Use the scientific rerun entries only when a deliberate rerun is actually desired.
- Do not interpret either prototype lane as the final Phase 1 study.
- Prototype GFS is best-effort in the legacy lane; the dedicated Phase 1 regional rerun remains the strict GFS-required workflow.
- Keep `3` and `3b` as legacy appendix/smoke paths, not part of the preferred `prototype_2021` proof story.
- The dedicated Phase 1 rerun stages `output/phase1_production_rerun/phase1_baseline_selection_candidate.yaml` only; do not treat it as an automatic overwrite of `config/phase1_baseline_selection.yaml`.
- Use `BASELINE_SELECTION_PATH` for downstream trials against the staged candidate; keep promotion of `config/phase1_baseline_selection.yaml` explicit and manual.
- Keep `config/case_mindoro_retro_2023.yaml` frozen as the March 3 -> March 6 base case; the promoted March 13 -> March 14 B1 row is carried by amendment, not by silent rewrite.
- `mindoro_march13_14_noaa_reinit_stress_test` is now an alias, not the authoritative scientific label.
- Do not claim independent day-to-day validation for March 13 -> March 14 while the shared-imagery caveat still applies, and do not treat PyGNOME as anything other than a comparator.
