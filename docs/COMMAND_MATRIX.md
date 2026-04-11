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
| `mindoro_phase4_only` | scientific/reportable | `mindoro_retro_2023` | rerun only Mindoro Phase 4 | moderate | `.\start.ps1 -Entry mindoro_phase4_only -NoPause` |
| `mindoro_reportable_core` | scientific/reportable | `mindoro_retro_2023` | intentional rerun of the main Mindoro reportable chain | expensive | `.\start.ps1 -Entry mindoro_reportable_core -NoPause` |
| `dwh_reportable_bundle` | scientific/reportable | `dwh_retro_2010` | intentional rerun of the DWH Phase 3C bundle | expensive | `.\start.ps1 -Entry dwh_reportable_bundle -NoPause` |
| `mindoro_appendix_sensitivity_bundle` | sensitivity/appendix | `mindoro_retro_2023` | rerun appendix and sensitivity branches | expensive | `.\start.ps1 -Entry mindoro_appendix_sensitivity_bundle -NoPause` |
| `prototype_legacy_bundle` | legacy prototype | `prototype_2016` | debug/regression only | moderate | `.\start.ps1 -Entry prototype_legacy_bundle -NoPause` |

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
docker-compose exec pipeline streamlit run ui/app.py --server.address 0.0.0.0 --server.port 8501
```

The Streamlit command launches the read-only local dashboard. It is intentionally documented as a direct command rather than a scientific launcher entry.

Mindoro main lane:

```bash
docker-compose exec -T -e WORKFLOW_MODE=mindoro_retro_2023 -e PIPELINE_PHASE=prep pipeline python -m src
docker-compose exec -T -e WORKFLOW_MODE=mindoro_retro_2023 pipeline python -m src
docker-compose exec -T -e WORKFLOW_MODE=mindoro_retro_2023 -e PIPELINE_PHASE=official_phase3b pipeline python -m src
docker-compose exec -T -e WORKFLOW_MODE=mindoro_retro_2023 -e PIPELINE_PHASE=phase3b_multidate_public pipeline python -m src
docker-compose exec -T -e WORKFLOW_MODE=mindoro_retro_2023 -e PIPELINE_PHASE=phase4_oiltype_and_shoreline pipeline python -m src
```

DWH lane:

```bash
docker-compose exec -T -e WORKFLOW_MODE=dwh_retro_2010 -e PIPELINE_PHASE=phase3c_external_case_setup pipeline python -m src
docker-compose exec -T -e WORKFLOW_MODE=dwh_retro_2010 -e PIPELINE_PHASE=dwh_phase3c_scientific_forcing_ready pipeline python -m src
docker-compose exec -T -e WORKFLOW_MODE=dwh_retro_2010 -e PIPELINE_PHASE=phase3c_external_case_run pipeline python -m src
docker-compose exec -T -e WORKFLOW_MODE=dwh_retro_2010 -e PIPELINE_PHASE=phase3c_external_case_ensemble_comparison pipeline python -m src
docker-compose exec -T -e WORKFLOW_MODE=dwh_retro_2010 -e PIPELINE_PHASE=phase3c_dwh_pygnome_comparator gnome python -m src
```

Prototype lane:

```bash
docker-compose exec -T -e WORKFLOW_MODE=prototype_2016 -e PIPELINE_PHASE=prep pipeline python -m src
docker-compose exec -T -e WORKFLOW_MODE=prototype_2016 pipeline python -m src
docker-compose exec -T -e WORKFLOW_MODE=prototype_2016 -e PIPELINE_PHASE=benchmark gnome python -m src
docker-compose exec -T -e WORKFLOW_MODE=prototype_2016 -e PIPELINE_PHASE=3 gnome python -m src
```

## Guardrails

- Use the read-only utilities for status refreshes and packaging work.
- Use the scientific rerun entries only when a deliberate rerun is actually desired.
- Do not interpret the prototype lane as the final Phase 1 study.
