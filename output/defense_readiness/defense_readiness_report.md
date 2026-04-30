# Defense Readiness Report

- Generated at: `2026-04-29T00:21:31Z`
- Checked git commit: `a28984be37240c63c7226f53b788f5eb25cf9630`
- Commit note: Generated report records the code commit checked at runtime; if the report is later committed, the repository commit containing this report may be one commit newer.
- Mode: `local`
- Compose mode detected: `docker compose`
- Python: `3.14.4`
- Platform: `Windows 11`

## Summary

- PASS: 23
- WARN: 5
- FAIL: 0

## Topic Status

- artifacts: `PASS`
- claim_boundaries: `PASS`
- dashboard: `WARN`
- docker: `PASS`
- environment: `PASS`
- launcher_commands: `PASS`
- launcher_entries: `WARN`
- launcher_matrix: `PASS`
- launcher_safety: `PASS`
- manuscript_numbers: `PASS`
- package_imports: `WARN`
- panel_commands: `PASS`
- stored_values: `WARN`

## Hard Failures

- None

## Warnings

- support/context Phase 4 machine-readable values reviewed
- host Python package availability checked
- gnome container import check completed
- Streamlit runtime smoke was not required for this run
- quick mode skipped the longer read-only entry batch

## Commands Run

- Repo-relative commands are shown below; the local repo root has been redacted.

- `powershell -NoProfile -ExecutionPolicy Bypass -File .\start.ps1 -Help -NoPause`
- `powershell -NoProfile -ExecutionPolicy Bypass -File .\start.ps1 -List -NoPause`
- `powershell -NoProfile -ExecutionPolicy Bypass -File .\start.ps1 -ListRole primary_evidence -NoPause`
- `powershell -NoProfile -ExecutionPolicy Bypass -File .\start.ps1 -ListRole read_only_governance -NoPause`
- `powershell -NoProfile -ExecutionPolicy Bypass -File .\start.ps1 -Explain mindoro_phase3b_primary_public_validation -NoPause`
- `powershell -NoProfile -ExecutionPolicy Bypass -File .\start.ps1 -Explain phase1_mindoro_focus_provenance -NoPause`
- `powershell -NoProfile -ExecutionPolicy Bypass -File .\start.ps1 -Explain dwh_reportable_bundle -NoPause`
- `powershell -NoProfile -ExecutionPolicy Bypass -File .\start.ps1 -Explain b1_drifter_context_panel -NoPause`
- `powershell -NoProfile -ExecutionPolicy Bypass -File .\start.ps1 -Panel -NoPause`
- `powershell -NoProfile -ExecutionPolicy Bypass -File .\panel.ps1 -NoPause`
- `powershell -NoProfile -ExecutionPolicy Bypass -File .\start.ps1 -Entry phase1_mindoro_focus_provenance`
- `docker compose config`
- `docker compose exec -T pipeline python -c "import importlib
mods = ['streamlit', 'pandas', 'numpy', 'geopandas', 'rasterio', 'shapely', 'xarray', 'yaml', 'matplotlib']
failures = []
for name in mods:
    try:
        importlib.import_module(name)
        print(name + ':OK')
    except Exception as exc:
        failures.append(f'{name}:{type(exc).__name__}:{exc}')
        print(name + ':FAIL:' + type(exc).__name__ + ':' + str(exc))
raise SystemExit(1 if failures else 0)"`
- `docker compose exec -T gnome python -c "import importlib
mods = ['gnome', 'py_gnome', 'numpy', 'pandas']
failures = []
for name in mods:
    try:
        importlib.import_module(name)
        print(name + ':OK')
    except Exception as exc:
        failures.append(f'{name}:{type(exc).__name__}:{exc}')
        print(name + ':FAIL:' + type(exc).__name__ + ':' + str(exc))
raise SystemExit(1 if failures else 0)"`
- `docker compose exec -T pipeline python -c "import importlib
mods = ['ui.app', 'ui.data_access', 'ui.app', 'ui.data_access', 'ui.pages.home', 'ui.pages.phase1_recipe_selection', 'ui.pages.b1_drifter_context', 'ui.pages.mindoro_validation', 'ui.pages.cross_model_comparison', 'ui.pages.mindoro_validation_archive', 'ui.pages.dwh_transfer_validation', 'ui.pages.phase4_oiltype_and_shoreline', 'ui.pages.legacy_2016_support', 'ui.pages.artifacts_logs']
for name in mods:
    importlib.import_module(name)
print('IMPORT_OK')"`

## Files Changed By Readiness Test

- None detected

## Check Details

### launcher_matrix_schema (PASS)

- Topic: `launcher_matrix`
- Summary: launcher matrix schema and thesis-boundary rules checked

### claim_boundary_docs (PASS)

- Topic: `claim_boundaries`
- Summary: defense-facing docs scanned for claim-boundary wording and readability

### artifact_registry (PASS)

- Topic: `artifacts`
- Summary: panel-facing stored artifacts and manifests discovered
- b1_primary_board_or_scorecard: output/Phase 3B March13-14 Final Output/summary/opendrift_primary/march13_14_reinit_summary.csv
- b1_stored_metrics: output/CASE_MINDORO_RETRO_2023/phase3b_extended_public_scored_march13_14_reinit/march13_14_reinit_summary.csv
- track_a_board_or_scorecard: output/Phase 3B March13-14 Final Output/summary/comparator_pygnome/march13_14_reinit_crossmodel_summary.csv
- dwh_board_or_scorecard: output/Phase 3C DWH Final Output/summary/comparison/phase3c_main_scorecard.csv
- phase1_focused_artifact: output/panel_drifter_context/b1_drifter_context_map.png
- publication_registry_or_manifest: output/figure_package_publication/publication_figure_manifest.json
- panel_review_output: output/panel_review_check/panel_results_match_check.json
- b1_drifter_provenance_manifest_or_loader: output/panel_drifter_context/b1_drifter_context_manifest.json

### phase1_and_b1_values (PASS)

- Topic: `stored_values`
- Summary: Phase 1 selection values and B1 R0/B1 guardrail values verified from stored files

### phase4_support_values (WARN)

- Topic: `stored_values`
- Summary: support/context Phase 4 machine-readable values reviewed
- Phase 4 thesis-facing support values are final beached percentages, first-arrival time, impacted shoreline segments, and QC status.
- Raw total_beached_kg in output/phase4/CASE_MINDORO_RETRO_2023/phase4_oil_budget_summary.csv is not treated as a primary manuscript claim in this readiness check unless it is separately re-audited.
- phase4 support-layer kg value for lighter_oil is 5261.2 kg, not the expected approximately 10.0 kg
- phase4 support-layer kg value for fixed_base_medium_heavy_proxy is 34864.9 kg, not the expected approximately 305.0 kg
- phase4 support-layer kg value for heavier_oil is 37393.6 kg, not the expected approximately 315.0 kg

### dashboard_page_registry (PASS)

- Topic: `dashboard`
- Summary: dashboard page registry and B1 drifter provenance page boundaries checked from source

### host_python_packages (WARN)

- Topic: `package_imports`
- Summary: host Python package availability checked
- streamlit: FAIL: ModuleNotFoundError: No module named 'streamlit'
- pandas: FAIL: ModuleNotFoundError: No module named 'pandas'
- numpy: FAIL: ModuleNotFoundError: No module named 'numpy'
- geopandas: FAIL: ModuleNotFoundError: No module named 'geopandas'
- rasterio: FAIL: ModuleNotFoundError: No module named 'rasterio'
- shapely: FAIL: ModuleNotFoundError: No module named 'shapely'
- xarray: FAIL: ModuleNotFoundError: No module named 'xarray'
- yaml: FAIL: ModuleNotFoundError: No module named 'yaml'
- matplotlib: FAIL: ModuleNotFoundError: No module named 'matplotlib'

### launcher_help (PASS)

- Topic: `launcher_commands`
- Summary: start.ps1 -Help -NoPause returned readable launcher help
- Command: `powershell -NoProfile -ExecutionPolicy Bypass -File .\start.ps1 -Help -NoPause`
- Output snippet:

```text

============================================================
   LAUNCHER HELP
============================================================

Panel-safe default path:
  .\panel.ps1
  .\start.ps1 -Panel -NoPause

Full launcher / researcher-audit path:
  .\start.ps1
  .\start.ps1 -List -NoPause
  .\start.ps1 -ListRole primary_evidence -NoPause
  .\start.ps1 -Explain mindoro_phase3b_primary_public_validation -NoPause
  .\start.ps1 -Entry <entry_id>

Preferred user-facing entry IDs:
  phase1_mindoro_focus_provenance
  b1_drifter_context_panel
  mindoro_phase3b_primary_public_validation
  dwh_reportable_bundle
  phase1_regional_reference_rerun
  mindoro_phase4_only

Compatibility aliases still work, but they are no longer the preferred wording:
  phase1_mindoro_focus_pre_spill_experiment
  phase1_production_rerun
  mindoro_march13_14_noaa_reinit_stress_test

Read-only / packaging-safe examples:
  .\start.ps1 -Entry b1_drifter_context_panel
  .\start.ps1 -Entry phase1_audit
  .\start.ps1 -Entry final_validation_package
  .\start.ps1 -Entry phase5_sync
  .\start.ps1 -Entry figure_package_publication

Intentional scientific rerun examples:
  .\start.ps1 -Entry phase1_mindoro_focus_provenance
  .\start.ps1 -Entry mindoro_phase3b_primary_public_validation
  .\start.ps1 -Entry dwh_reportable_bundle
  .\start.ps1 -Entry mindoro_reportable_core

Direct container commands:
  docker compose exec -T -e WORKFLOW_MODE=<workflow_mode> -e PIPELINE_PHASE=<phase> <pipeline|gnome> python -m src
  docker compose exec -T pipeline python src/services/panel_review_check.py
  docker compose exec pipeline python -m streamlit run ui/app.py --server.address 0.0.0.0 --server.port 8501
  docker compose up -d ; docker compose restart pipeline gnome ; docker compose exec pipeline python -m streamlit run ui/app.py --server.address 0.0.0.0 --server.port 8501

Guardrails:
  - Panel mode is the defense-safe default. The full launcher is for researcher/audit use.
  - Use launcher entry IDs and role groups as the user-facing startup vocabulary. Raw phase names are secondary implementation details.
  - B1 is the only main-text primary Mindoro validation row, and the March 13 -> March 14 pair keeps the observation-independence note explicit.
  - Track A and every PyGNOME branch remain comparator-only support, never observational truth.
  - DWH is a separate external transfer-validation story, not Mindoro recalibration.
  - Mindoro Phase 4 oil-type and shoreline outputs remain support/context only.
  - prototype_2016 remains legacy support only.
  - Non-interactive launcher runs default silently to INPUT_CACHE_POLICY=reuse_if_valid and FORCING_SOURCE_BUDGET_SECONDS=300.
  - Interactive launcher runs still ask once for the forcing wait budget and cache policy when the target workflow is eligible.
  - Direct interactive docker compose exec runs do the same once per run; the -T form stays prompt-free and prints the resolved startup policy instead.
  - Do not auto-promote output/phase1_production_rerun/phase1_baseline_selection_candidate.yaml over config/phase1_baseline_selection.yaml.

Not implemented yet:
  - Interactive UI run controls [deferred]
  - Deeper artifact search and filtering inside the UI [deferred]
  - DWH Phase 4 appendix pilot [deferred]

```

### launcher_list (PASS)

- Topic: `launcher_commands`
- Summary: start.ps1 -List -NoPause returned the launcher catalog
- Command: `powershell -NoProfile -ExecutionPolicy Bypass -File .\start.ps1 -List -NoPause`
- Output snippet:

```text

============================================================
   CURRENT LAUNCHER CATALOG
============================================================

Defense default: .\panel.ps1 or .\start.ps1 -Panel
Full launcher: .\start.ps1  # researcher/audit path
Catalog: phase5_launcher_matrix_v3
List by role: .\start.ps1 -ListRole <thesis_role> -NoPause
Explain one entry: .\start.ps1 -Explain <entry_id> -NoPause
Prompt-free container run: docker compose exec -T -e WORKFLOW_MODE=<workflow_mode> -e PIPELINE_PHASE=<phase> <pipeline|gnome> python -m src
Read-only UI: docker compose exec pipeline python -m streamlit run ui/app.py --server.address 0.0.0.0 --server.port 8501

Use user-facing entry IDs and thesis-role groupings here. Raw phase names are not the primary startup commands.

1. Main thesis evidence reruns
   Intentional reruns for the main thesis evidence lanes.
  - Focused Mindoro Phase 1 provenance rerun
     id=phase1_mindoro_focus_provenance
     thesis role=Primary evidence | draft=Evidence 1 / Focused Mindoro Phase 1 provenance
     tags=Scientific rerun | cost=expensive | explicit-confirm | for=researcher
     boundary=Primary transport-provenance basis for B1; not direct spill-footprint validation.

  - Mindoro B1 primary public-validation rerun
     id=mindoro_phase3b_primary_public_validation
     thesis role=Primary evidence | draft=Evidence 3 / Mindoro B1 / Chapter 4.2
     tags=Scientific rerun | cost=expensive | explicit-confirm | for=researcher
     boundary=Only main-text primary Mindoro validation row; uses independent NOAA-published day-specific observations; interpretation remains neighborhood-scale rather than exact 1 km overlap.

  - DWH external transfer-validation bundle
     id=dwh_reportable_bundle
     thesis role=Primary evidence | draft=Evidence 5 / DWH Phase 3C external transfer validation
     tags=Scientific rerun | cost=expensive | explicit-confirm | for=researcher
     boundary=External transfer validation against DWH observation masks; not Mindoro recalibration, and PyGNOME remains comparator-only.

  - Mindoro full evidence/support bundle rerun
     id=mindoro_reportable_core
     thesis role=Primary evidence | draft=Evidence 1-6 / Mindoro integrated evidence bundle
     tags=Scientific rerun | cost=expensive | explicit-confirm | for=auditor
     boundary=Full Mindoro bundle including B1, archive rows, and support/context; use only for intentional researcher/audit reruns, not as the default defense path.

2. Support/context and appendix reruns
   Support, comparator, and appendix reruns outside the main-text claim.
  - Mindoro oil-type and shoreline support rerun
     id=mindoro_phase4_only
     thesis role=Support/context | draft=Evidence 6 / Mindoro oil-type and shoreline support
     tags=Scientific rerun | cost=moderate | explicit-confirm | for=researcher
     boundary=Mindoro oil-type and shoreline support/context only; not a second primary validation lane.

  - Mindoro support/context and appendix bundle
     id=mindoro_appendix_sensitivity_bundle
     thesis role=Support/context | draft=Evidence 4 and 6 / Mindoro support-context appendix
     tags=Archive/support rerun | cost=expensive | explicit-confirm | for=researcher
     boundary=Support, comparator, and appendix branches only; not the main-text Mindoro validation claim, and PyGNOME remains comparator-only where included.

3. Archive/provenance reruns
   Archive, provenance, and governance reruns kept outside the default defense path.
  - Phase 1 regional reference rerun
     id=phase1_regional_reference_rerun
     thesis role=Archive/provenance | draft=Archive / governance appendix / Phase 1 regional reference
     tags=Archive/support rerun | cost=expensive | explicit-confirm | for=auditor
     boundary=Broader reference/governance lane only; does not replace focused Mindoro provenance.

  - Archive Mindoro Phase 1 focus comparison trial
     id=mindoro_march13_14_phase1_focus_trial
     thesis role=Archive/provenance | draft=Archive / Mindoro provenance trial
     tags=Archive/support rerun | cost=expensive | explicit-confirm | for=auditor
     boundary=Archive-labeled comparison trial using staged Mindoro Phase 1 provenance; it does not replace canonical B1, and any referenced PyGNOME material remains comparator-only support.

  - Mindoro March 6 archive recovery sensitivity
     id=mindoro_march6_recovery_sensitivity
     thesis role=Archive/provenance | draft=Archive / Mindoro March 6 honesty support
     tags=Archive/support rerun | cost=moderate | safe-default | for=auditor
     boundary=Archive/support recovery matrix for March 6 only; it does not replace the preserved March 6 honesty row.

  - Mindoro March 23 archive stress test
     id=mindoro_march23_extended_public_stress_test
     thesis role=Archive/provenance | draft=Archive / Mindoro March 23 stress test
     tags=Archive/support rerun | cost=expensive | explicit-confirm | for=researcher
     boundary=Archive support stress test only; not a promoted validation row.

4. Legacy prototype/debug reruns
   Legacy prototype support and debug paths.
  - Prototype 2016 final paper figures
     id=prototype_legacy_final_figures
     thesis role=Legacy support | draft=Evidence 7 / Legacy 2016 archive support
     tags=Packaging-only | cost=cheap_read_only | safe-default | for=auditor
     boundary=Legacy 2016 support package only; it is packaging-only and not a Mindoro or DWH validation rerun, and any PyGNOME material inside remains comparator-only support.

  - Prototype 2021 preferred debug bundle
     id=prototype_2021_bundle
     thesis role=Legacy support | draft=Legacy/debug appendix / Prototype 2021 support
     tags=Scientific rerun | cost=moderate | explicit-confirm | for=developer
     boundary=Debug/demo support lane only; not a thesis-facing validation row, and PyGNOME remains comparator-only.

  - Prototype 2016 legacy bundle
     id=prototype_legacy_bundle
     thesis role=Legacy support | draft=Evidence 7 / Legacy 2016 archive support
     tags=Scientific rerun | cost=moderate | explicit-confirm | for=developer
     boundary=Legacy 2016 support/debug lane only; not a current primary validation path, and PyGNOME remains comparator-only.

5. Read-only packaging, audits, dashboard, and docs
   Safe packaging and audit surfaces built from stored outputs only.
  - Phase 1 finalization audit
     id=phase1_audit
     thesis role=Read-only governance | draft=Evidence 8 / Reproducibility and governance
     tags=Read-only | cost=cheap_read_only | safe-default | for=auditor
     boundary=Read-only audit of the stored Phase 1 state; it does not rerun the Phase 1 science.

  - Phase 2 finalization audit
     id=phase2_audit
     thesis role=Read-only governance | draft=Evidence 8 / Reproducibility and governance
     tags=Read-only | cost=cheap_read_only | safe-default | for=auditor
     boundary=Read-only audit of the stored Phase 2 state; it does not rerun the official forecast path.

  - B1 drifter provenance panel
     id=b1_drifter_context_panel
     thesis role=Read-only governance | draft=Evidence 1 -> Evidence 3 bridge / Phase 1 provenance to Mindoro B1
     tags=Read-only | cost=cheap_read_only | safe-default | for=panel
     boundary=Drifter records support B1 recipe provenance only; they are not the direct March 13-14 public-observation truth mask.

  - Final validation package refresh
     id=final_validation_package
     thesis role=Read-only governance | draft=Evidence 8 / Reproducibility and governance
     tags=Packaging-only | cost=cheap_read_only | safe-default | for=auditor
     boundary=Packaging-only refresh from stored validation outputs; it does not recompute scores.

  - Phase 5 launcher/docs/package sync
     id=phase5_sync
     thesis role=Read-only governance | draft=Evidence 8 / Reproducibility and governance
     tags=Packaging-only | cost=cheap_read_only | safe-default | for=auditor
     boundary=Read-only reproducibility/governance sync layer built from stored outputs and docs only.

  - Trajectory gallery build
     id=trajectory_gallery
     thesis role=Read-only governance | draft=Evidence 8 / Reproducibility and governance
     tags=Packaging-only | cost=cheap_read_only | safe-default | for=auditor
     boundary=Read-only gallery build from stored outputs only; it does not rerun scientific branches.

  - Trajectory gallery panel polish
     id=trajectory_gallery_panel
     thesis role=Read-only governance | draft=Evidence 8 / Reproducibility and governance
     tags=Packaging-only | cost=cheap_read_only | safe-default | for=auditor
     boundary=Read-only panel-figure packaging from stored outputs only; it does not rerun scientific branches.

  - Publication-grade figure package
     id=figure_package_publication
     thesis role=Read-only governance | draft=Evidence 8 / Reproducibility and governance
     tags=Packaging-only | cost=cheap_read_only | safe-default | for=auditor
     boundary=Packaging-only rebuild from stored outputs only; it does not rerun scientific branches or change scores.

Hidden compatibility / experimental IDs
   These remain valid for older scripts or deliberate experiments, but they stay out of the default launcher menu.
  - Legacy ID alias: Focused Mindoro Phase 1 provenance rerun
     id=phase1_mindoro_focus_pre_spill_experiment
     thesis role=Primary evidence | draft=Evidence 1 / Focused Mindoro Phase 1 provenance
     tags=Scientific rerun | cost=expensive | explicit-confirm | for=researcher | hidden-from-default-menu | alias-of=phase1_mindoro_focus_provenance
     boundary=Primary transport-provenance basis for B1; not direct spill-footprint validation.

  - Legacy ID alias: Phase 1 regional reference rerun
     id=phase1_production_rerun
     thesis role=Archive/provenance | draft=Archive / governance appendix / Phase 1 regional reference
     tags=Archive/support rerun | cost=expensive | explicit-confirm | for=auditor | hidden-from-default-menu | alias-of=phase1_regional_reference_rerun
     boundary=Broader reference/governance lane only; does not replace focused Mindoro provenance.

  - Experimental Mindoro B1 5,000-element rerun
     id=phase3b_mindoro_march13_14_reinit_5000_experiment
     thesis role=Archive/provenance | draft=Experimental only / personal sensitivity
     tags=Archive/support rerun | cost=expensive | explicit-confirm | for=developer | hidden-from-default-menu
     boundary=Personal experiment only; not thesis-facing and does not replace canonical B1.

  - Legacy alias: Mindoro March 13-14 validation + comparator bundle
     id=mindoro_march13_14_noaa_reinit_stress_test
     thesis role=Comparator support | draft=Legacy alias / Mindoro B1 plus Track A support
     tags=Comparator rerun | cost=expensive | explicit-confirm | for=developer | hidden-from-default-menu
     boundary=Legacy compatibility bundle for older scripts; B1 remains the primary row, Track A remains comparator support only, and PyGNOME is never observational truth.

Optional future work not implemented in the launcher:
  - Interactive UI run controls [deferred]
  - Deeper artifact search and filtering inside the UI [deferred]
  - DWH Phase 4 appendix pilot [deferred]

```

### launcher_list_primary (PASS)

- Topic: `launcher_commands`
- Summary: primary_evidence role listing returned readable output
- Command: `powershell -NoProfile -ExecutionPolicy Bypass -File .\start.ps1 -ListRole primary_evidence -NoPause`
- Output snippet:

```text

============================================================
   CURRENT LAUNCHER CATALOG
============================================================

Defense default: .\panel.ps1 or .\start.ps1 -Panel
Full launcher: .\start.ps1  # researcher/audit path
Catalog: phase5_launcher_matrix_v3
List by role: .\start.ps1 -ListRole <thesis_role> -NoPause
Explain one entry: .\start.ps1 -Explain <entry_id> -NoPause
Prompt-free container run: docker compose exec -T -e WORKFLOW_MODE=<workflow_mode> -e PIPELINE_PHASE=<phase> <pipeline|gnome> python -m src
Read-only UI: docker compose exec pipeline python -m streamlit run ui/app.py --server.address 0.0.0.0 --server.port 8501

Use user-facing entry IDs and thesis-role groupings here. Raw phase names are not the primary startup commands.

Filtered thesis role: Primary evidence

  - Focused Mindoro Phase 1 provenance rerun
     id=phase1_mindoro_focus_provenance
     thesis role=Primary evidence | draft=Evidence 1 / Focused Mindoro Phase 1 provenance
     tags=Scientific rerun | cost=expensive | explicit-confirm | for=researcher
     boundary=Primary transport-provenance basis for B1; not direct spill-footprint validation.

  - Legacy ID alias: Focused Mindoro Phase 1 provenance rerun
     id=phase1_mindoro_focus_pre_spill_experiment
     thesis role=Primary evidence | draft=Evidence 1 / Focused Mindoro Phase 1 provenance
     tags=Scientific rerun | cost=expensive | explicit-confirm | for=researcher | hidden-from-default-menu | alias-of=phase1_mindoro_focus_provenance
     boundary=Primary transport-provenance basis for B1; not direct spill-footprint validation.

  - Mindoro B1 primary public-validation rerun
     id=mindoro_phase3b_primary_public_validation
     thesis role=Primary evidence | draft=Evidence 3 / Mindoro B1 / Chapter 4.2
     tags=Scientific rerun | cost=expensive | explicit-confirm | for=researcher
     boundary=Only main-text primary Mindoro validation row; uses independent NOAA-published day-specific observations; interpretation remains neighborhood-scale rather than exact 1 km overlap.

  - DWH external transfer-validation bundle
     id=dwh_reportable_bundle
     thesis role=Primary evidence | draft=Evidence 5 / DWH Phase 3C external transfer validation
     tags=Scientific rerun | cost=expensive | explicit-confirm | for=researcher
     boundary=External transfer validation against DWH observation masks; not Mindoro recalibration, and PyGNOME remains comparator-only.

  - Mindoro full evidence/support bundle rerun
     id=mindoro_reportable_core
     thesis role=Primary evidence | draft=Evidence 1-6 / Mindoro integrated evidence bundle
     tags=Scientific rerun | cost=expensive | explicit-confirm | for=auditor
     boundary=Full Mindoro bundle including B1, archive rows, and support/context; use only for intentional researcher/audit reruns, not as the default defense path.

Optional future work not implemented in the launcher:
  - Interactive UI run controls [deferred]
  - Deeper artifact search and filtering inside the UI [deferred]
  - DWH Phase 4 appendix pilot [deferred]

```

### launcher_list_read_only_governance (PASS)

- Topic: `launcher_commands`
- Summary: read_only_governance role listing returned readable output
- Command: `powershell -NoProfile -ExecutionPolicy Bypass -File .\start.ps1 -ListRole read_only_governance -NoPause`
- Output snippet:

```text

============================================================
   CURRENT LAUNCHER CATALOG
============================================================

Defense default: .\panel.ps1 or .\start.ps1 -Panel
Full launcher: .\start.ps1  # researcher/audit path
Catalog: phase5_launcher_matrix_v3
List by role: .\start.ps1 -ListRole <thesis_role> -NoPause
Explain one entry: .\start.ps1 -Explain <entry_id> -NoPause
Prompt-free container run: docker compose exec -T -e WORKFLOW_MODE=<workflow_mode> -e PIPELINE_PHASE=<phase> <pipeline|gnome> python -m src
Read-only UI: docker compose exec pipeline python -m streamlit run ui/app.py --server.address 0.0.0.0 --server.port 8501

Use user-facing entry IDs and thesis-role groupings here. Raw phase names are not the primary startup commands.

Filtered thesis role: Read-only governance

  - Phase 1 finalization audit
     id=phase1_audit
     thesis role=Read-only governance | draft=Evidence 8 / Reproducibility and governance
     tags=Read-only | cost=cheap_read_only | safe-default | for=auditor
     boundary=Read-only audit of the stored Phase 1 state; it does not rerun the Phase 1 science.

  - Phase 2 finalization audit
     id=phase2_audit
     thesis role=Read-only governance | draft=Evidence 8 / Reproducibility and governance
     tags=Read-only | cost=cheap_read_only | safe-default | for=auditor
     boundary=Read-only audit of the stored Phase 2 state; it does not rerun the official forecast path.

  - B1 drifter provenance panel
     id=b1_drifter_context_panel
     thesis role=Read-only governance | draft=Evidence 1 -> Evidence 3 bridge / Phase 1 provenance to Mindoro B1
     tags=Read-only | cost=cheap_read_only | safe-default | for=panel
     boundary=Drifter records support B1 recipe provenance only; they are not the direct March 13-14 public-observation truth mask.

  - Final validation package refresh
     id=final_validation_package
     thesis role=Read-only governance | draft=Evidence 8 / Reproducibility and governance
     tags=Packaging-only | cost=cheap_read_only | safe-default | for=auditor
     boundary=Packaging-only refresh from stored validation outputs; it does not recompute scores.

  - Phase 5 launcher/docs/package sync
     id=phase5_sync
     thesis role=Read-only governance | draft=Evidence 8 / Reproducibility and governance
     tags=Packaging-only | cost=cheap_read_only | safe-default | for=auditor
     boundary=Read-only reproducibility/governance sync layer built from stored outputs and docs only.

  - Trajectory gallery build
     id=trajectory_gallery
     thesis role=Read-only governance | draft=Evidence 8 / Reproducibility and governance
     tags=Packaging-only | cost=cheap_read_only | safe-default | for=auditor
     boundary=Read-only gallery build from stored outputs only; it does not rerun scientific branches.

  - Trajectory gallery panel polish
     id=trajectory_gallery_panel
     thesis role=Read-only governance | draft=Evidence 8 / Reproducibility and governance
     tags=Packaging-only | cost=cheap_read_only | safe-default | for=auditor
     boundary=Read-only panel-figure packaging from stored outputs only; it does not rerun scientific branches.

  - Publication-grade figure package
     id=figure_package_publication
     thesis role=Read-only governance | draft=Evidence 8 / Reproducibility and governance
     tags=Packaging-only | cost=cheap_read_only | safe-default | for=auditor
     boundary=Packaging-only rebuild from stored outputs only; it does not rerun scientific branches or change scores.

Optional future work not implemented in the launcher:
  - Interactive UI run controls [deferred]
  - Deeper artifact search and filtering inside the UI [deferred]
  - DWH Phase 4 appendix pilot [deferred]

```

### explain_mindoro_phase3b_primary_public_validation (PASS)

- Topic: `launcher_commands`
- Summary: -Explain mindoro_phase3b_primary_public_validation returned the thesis-boundary preview
- Command: `powershell -NoProfile -ExecutionPolicy Bypass -File .\start.ps1 -Explain mindoro_phase3b_primary_public_validation -NoPause`
- Output snippet:

```text

============================================================
   ENTRY PREVIEW
============================================================

Entry ID: mindoro_phase3b_primary_public_validation
Label: Mindoro B1 primary public-validation rerun
Thesis role: Primary evidence
Draft section: Evidence 3 / Mindoro B1 / Chapter 4.2
Claim boundary: Only main-text primary Mindoro validation row; uses independent NOAA-published day-specific observations; interpretation remains neighborhood-scale rather than exact 1 km overlap.
Run kind: Scientific rerun
Rerun cost: expensive
Safety: explicit confirmation required
Recommended for: researcher
Steps/phases: pipeline:phase3b_extended_public -> pipeline:phase3b_extended_public_scored_march13_14_reinit
Output warning: May rerun scientific phases and write new workflow artifacts under output/. Continue only when you intentionally want that rerun.
Notes: Canonical B1 builder. This does not delete or relabel the repo-preserved archive-only March-family rows, and the same-case A comparator-support lane stays separate and comparator-only. Public-observation and forcing reruns reuse validated persistent local store files before any remote refetch unless INPUT_CACHE_POLICY=force_refresh is selected.

```

### explain_phase1_mindoro_focus_provenance (PASS)

- Topic: `launcher_commands`
- Summary: -Explain phase1_mindoro_focus_provenance returned the thesis-boundary preview
- Command: `powershell -NoProfile -ExecutionPolicy Bypass -File .\start.ps1 -Explain phase1_mindoro_focus_provenance -NoPause`
- Output snippet:

```text

============================================================
   ENTRY PREVIEW
============================================================

Entry ID: phase1_mindoro_focus_provenance
Label: Focused Mindoro Phase 1 provenance rerun
Thesis role: Primary evidence
Draft section: Evidence 1 / Focused Mindoro Phase 1 provenance
Claim boundary: Primary transport-provenance basis for B1; not direct spill-footprint validation.
Run kind: Scientific rerun
Rerun cost: expensive
Safety: explicit confirmation required
Recommended for: researcher
Steps/phases: pipeline:phase1_production_rerun
Output warning: May rerun scientific phases and write new workflow artifacts under output/. Continue only when you intentionally want that rerun.
Notes: Mindoro-specific provenance lane only. This does not rewrite the stored March 13 -> March 14 R1 raw-generation history, does not modify legacy 2016 prototype outputs, and now evaluates the full four-recipe family with a GFS completeness preflight while promoting the focused historical winner directly into official B1.

```

### explain_dwh_reportable_bundle (PASS)

- Topic: `launcher_commands`
- Summary: -Explain dwh_reportable_bundle returned the thesis-boundary preview
- Command: `powershell -NoProfile -ExecutionPolicy Bypass -File .\start.ps1 -Explain dwh_reportable_bundle -NoPause`
- Output snippet:

```text

============================================================
   ENTRY PREVIEW
============================================================

Entry ID: dwh_reportable_bundle
Label: DWH external transfer-validation bundle
Thesis role: Primary evidence
Draft section: Evidence 5 / DWH Phase 3C external transfer validation
Claim boundary: External transfer validation against DWH observation masks; not Mindoro recalibration, and PyGNOME remains comparator-only.
Run kind: Scientific rerun
Rerun cost: expensive
Safety: explicit confirmation required
Recommended for: researcher
Steps/phases: pipeline:phase3c_external_case_setup -> pipeline:dwh_phase3c_scientific_forcing_ready -> pipeline:phase3c_external_case_run -> pipeline:phase3c_external_case_ensemble_comparison -> gnome:phase3c_dwh_pygnome_comparator
Output warning: May rerun scientific phases and write new workflow artifacts under output/. Continue only when you intentionally want that rerun.
Notes: Separate external transfer-validation story only. Mindoro remains the main Philippine thesis case; DWH observed masks remain truth; PyGNOME remains comparator-only; current frozen stack is HYCOM GOFS 3.1 + ERA5 + CMEMS wave/Stokes; forcing-readiness stays strict by default unless FORCING_OUTAGE_POLICY=continue_degraded is set explicitly. DWH case inputs and scientific forcing now persist under data/local_input_store/... and reruns reuse validated local files before any remote refetch unless INPUT_CACHE_POLICY=force_refresh is selected.

```

### explain_b1_drifter_context_panel (PASS)

- Topic: `launcher_commands`
- Summary: -Explain b1_drifter_context_panel returned the thesis-boundary preview
- Command: `powershell -NoProfile -ExecutionPolicy Bypass -File .\start.ps1 -Explain b1_drifter_context_panel -NoPause`
- Output snippet:

```text

============================================================
   ENTRY PREVIEW
============================================================

Entry ID: b1_drifter_context_panel
Label: B1 drifter provenance panel
Thesis role: Read-only governance
Draft section: Evidence 1 -> Evidence 3 bridge / Phase 1 provenance to Mindoro B1
Claim boundary: Drifter records support B1 recipe provenance only; they are not the direct March 13-14 public-observation truth mask.
Run kind: Read-only
Rerun cost: cheap_read_only
Safety: safe default
Recommended for: panel
Steps/phases: pipeline:panel_b1_drifter_context
Output warning: Uses stored manifests or artifacts only. No scientific rerun should occur.
Notes: Uses stored local Phase 1 registries and manifests only. It does not rerun Phase 1 or B1 science, does not download new drifter data by default, and explicitly says so when no direct March 13-14 2023 accepted drifter segment is stored.

```

### panel_no_pause (PASS)

- Topic: `panel_commands`
- Summary: start.ps1 -Panel -NoPause returned a noninteractive panel summary
- Command: `powershell -NoProfile -ExecutionPolicy Bypass -File .\start.ps1 -Panel -NoPause`
- Output snippet:

```text

============================================================
   DRIFTER-VALIDATED OIL SPILL FORECASTING
============================================================
   PANEL REVIEW MODE

Non-interactive preview mode (-NoPause).
Use .\panel.ps1 or .\start.ps1 -Panel for the interactive defense menu.

Panel-safe actions:
  1. Open read-only dashboard
  2. Verify paper numbers against stored scorecards
  3. Rebuild publication figures from stored outputs
  4. Refresh final validation package from stored outputs
  5. Refresh final reproducibility package / command documentation
  6. Show paper-to-output registry
  7. View B1 drifter provenance/context

Smoke-test-safe examples:
  .\start.ps1 -Explain b1_drifter_context_panel -NoPause
  .\start.ps1 -Entry b1_drifter_context_panel -NoPause
  .\start.ps1 -Entry figure_package_publication -NoPause


```

### panel_wrapper_no_pause (PASS)

- Topic: `panel_commands`
- Summary: panel.ps1 -NoPause returned the read-only panel wrapper preview
- Command: `powershell -NoProfile -ExecutionPolicy Bypass -File .\panel.ps1 -NoPause`
- Output snippet:

```text

============================================================
   DRIFTER-VALIDATED OIL SPILL FORECASTING
============================================================
   PANEL REVIEW MODE

Non-interactive preview mode (-NoPause).
Use .\panel.ps1 or .\start.ps1 -Panel for the interactive defense menu.

Panel-safe actions:
  1. Open read-only dashboard
  2. Verify paper numbers against stored scorecards
  3. Rebuild publication figures from stored outputs
  4. Refresh final validation package from stored outputs
  5. Refresh final reproducibility package / command documentation
  6. Show paper-to-output registry
  7. View B1 drifter provenance/context

Smoke-test-safe examples:
  .\start.ps1 -Explain b1_drifter_context_panel -NoPause
  .\start.ps1 -Entry b1_drifter_context_panel -NoPause
  .\start.ps1 -Entry figure_package_publication -NoPause


```

### expensive_entry_confirmation_guard (PASS)

- Topic: `launcher_safety`
- Summary: noninteractive execution of an expensive launcher entry stopped at the confirmation guard
- Command: `powershell -NoProfile -ExecutionPolicy Bypass -File .\start.ps1 -Entry phase1_mindoro_focus_provenance`
- Output snippet:

```text

============================================================
   Focused Mindoro Phase 1 provenance rerun
============================================================

[ERROR] Launcher entry 'phase1_mindoro_focus_provenance' requires interactive confirmation. Run .\start.ps1 -Explain phase1_mindoro_focus_provenance first, then rerun it from an interactive PowerShell session.


```

### docker_compose_detection (PASS)

- Topic: `docker`
- Summary: Docker Compose detected via docker compose

### docker_compose_config (PASS)

- Topic: `docker`
- Summary: docker compose config completed
- Command: `docker compose config`
- Output snippet:

```text
name: drifter-validated-oilspill-forecasting-rc-v10
services:
  gnome:
    build:
      context: C:\Users\marcp\Downloads\drifter-validated-oilspill-forecasting-rc-v1.0\drifter-validated-oilspill-forecasting-rc-v1.0
      dockerfile: docker/gnome/Dockerfile
    command:
      - tail
      - -f
      - /dev/null
    container_name: phase3
    deploy:
      resources:
        limits:
          memory: "8589934592"
        reservations:
          memory: "4294967296"
    environment:
      CDS_KEY: 57b99494-c603-4c65-86fb-4e2b73a5517b
      CDS_URL: https://cds.climate.copernicus.eu/api
      CMEMS_PASSWORD: GG9mKyujCFPVk5T!
      CMEMS_USERNAME: arjayninosaguisa@gmail.com
      PIPELINE_PHASE: "3"
      PIPELINE_ROLE: gnome
      PYTHONDONTWRITEBYTECODE: "1"
      PYTHONUNBUFFERED: "1"
    image: oil-spill-phase3:latest
    logging:
      driver: json-file
      options:
        max-file: "3"
        max-size: 10m
    networks:
      default: null
    volumes:
      - type: bind
        source: C:\Users\marcp\Downloads\drifter-validated-oilspill-forecasting-rc-v1.0\drifter-validated-oilspill-forecasting-rc-v1.0\data
        target: /app/data
        bind: {}
      - type: bind
        source: C:\Users\marcp\Downloads\drifter-validated-oilspill-forecasting-rc-v1.0\drifter-validated-oilspill-forecasting-rc-v1.0\data_processed
        target: /app/data_processed
        bind: {}
      - type: bind
        source: C:\Users\marcp\Downloads\drifter-validated-oilspill-forecasting-rc-v1.0\drifter-validated-oilspill-forecasting-rc-v1.0\output
        target: /app/output
        bind: {}
      - type: bind
        source: C:\Users\marcp\Downloads\drifter-validated-oilspill-forecasting-rc-v1.0\drifter-validated-oilspill-forecasting-rc-v1.0\config
        target: /app/config
        bind: {}
      - type: bind
        source: C:\Users\marcp\Downloads\drifter-validated-oilspill-forecasting-rc-v1.0\drifter-validated-oilspill-forecasting-rc-v1.0\src
        target: /app/src
        bind: {}
      - type: bind
        source: C:\Users\marcp\Downloads\drifter-validated-oilspill-forecasting-rc-v1.0\drifter-validated-oilspill-forecasting-rc-v1.0\tests
        target: /app/tests
        bind: {}
  pipeline:
    build:
      context: C:\Users\marcp\Downloads\drifter-validated-oilspill-forecasting-rc-v1.0\drifter-validated-oilspill-forecasting-rc-v1.0
      dockerfile: docker/pipeline/Dockerfile
    command:
      - tail
      - -f
      - /dev/null
    container_name: phase1
    deploy:
      resources:
        limits:
          memory: "8589934592"
        reservations:
          memory: "4294967296"
    environment:
      CDS_KEY: 57b99494-c603-4c65-86fb-4e2b73a5517b
      CDS_URL: https://cds.climate.copernicus.eu/api
      CMEMS_PASSWORD: GG9mKyujCFPVk5T!
      CMEMS_USERNAME: arjayninosaguisa@gmail.com
      PATH: /app/.venv/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
      PIPELINE_PHASE: "1_2"
      PIPELINE_ROLE: pipeline
      PYTHONDONTWRITEBYTECODE: "1"
      PYTHONUNBUFFERED: "1"
    image: oil-spill-phase1:latest
    logging:
      driver: json-file
      options:
        max-file: "3"
        max-size: 10m
    networks:
      default: null
    ports:
      - mode: ingress
        target: 8501
        published: "8501"
        protocol: tcp
    volumes:
      - type: bind
        source: C:\Users\marcp\Downloads\drifter-validated-oilspill-forecasting-rc-v1.0\drifter-validated-oilspill-forecasting-rc-v1.0\data
        target: /app/data
        bind: {}
      - type: bind
        source: C:\Users\marcp\Downloads\drifter-validated-oilspill-forecasting-rc-v1.0\drifter-validated-oilspill-forecasting-rc-v1.0\data_processed
        target: /app/data_processed
        bind: {}
      - type: bind
        source: C:\Users\marcp\Downloads\drifter-validated-oilspill-forecasting-rc-v1.0\drifter-validated-oilspill-forecasting-rc-v1.0\output
        target: /app/output
        bind: {}
      - type: bind
        source: C:\Users\marcp\Downloads\drifter-validated-oilspill-forecasting-rc-v1.0\drifter-validated-oilspill-forecasting-rc-v1.0\config
        target: /app/config
        bind: {}
      - type: bind
        source: C:\Users\marcp\Downloads\drifter-validated-oilspill-forecasting-rc-v1.0\drifter-validated-oilspill-forecasting-rc-v1.0\src
        target: /app/src
        bind: {}
      - type: bind
        source: C:\Users\marcp\Downloads\drifter-validated-oilspill-forecasting-rc-v1.0\drifter-validated-oilspill-forecasting-rc-v1.0\ui
        target: /app/ui
        bind: {}
      - type: bind
        source: C:\Users\marcp\Downloads\drifter-validated-oilspill-forecasting-rc-v1.0\drifter-validated-oilspill-forecasting-rc-v1.0\tests
        target: /app/tests
        bind: {}
networks:
  default:
    name: drifter-validated-oilspill-forecasting-rc-v10_default

```

### docker_compose_services (PASS)

- Topic: `docker`
- Summary: docker-compose.yml service list checked
- gnome
- pipeline

### pipeline_package_imports (PASS)

- Topic: `package_imports`
- Summary: pipeline container package imports checked
- Command: `docker compose exec -T pipeline python -c "import importlib
mods = ['streamlit', 'pandas', 'numpy', 'geopandas', 'rasterio', 'shapely', 'xarray', 'yaml', 'matplotlib']
failures = []
for name in mods:
    try:
        importlib.import_module(name)
        print(name + ':OK')
    except Exception as exc:
        failures.append(f'{name}:{type(exc).__name__}:{exc}')
        print(name + ':FAIL:' + type(exc).__name__ + ':' + str(exc))
raise SystemExit(1 if failures else 0)"`
- streamlit:OK
- pandas:OK
- numpy:OK
- geopandas:OK
- rasterio:OK
- shapely:OK
- xarray:OK
- yaml:OK
- matplotlib:OK

### gnome_package_imports (WARN)

- Topic: `package_imports`
- Summary: gnome container import check completed
- Command: `docker compose exec -T gnome python -c "import importlib
mods = ['gnome', 'py_gnome', 'numpy', 'pandas']
failures = []
for name in mods:
    try:
        importlib.import_module(name)
        print(name + ':OK')
    except Exception as exc:
        failures.append(f'{name}:{type(exc).__name__}:{exc}')
        print(name + ':FAIL:' + type(exc).__name__ + ':' + str(exc))
raise SystemExit(1 if failures else 0)"`
- gnome:OK
- py_gnome:FAIL:ModuleNotFoundError:No module named 'py_gnome'
- numpy:OK
- pandas:OK

### dashboard_container_imports (PASS)

- Topic: `dashboard`
- Summary: pipeline container imported ui/app.py and the panel-facing page modules
- Command: `docker compose exec -T pipeline python -c "import importlib
mods = ['ui.app', 'ui.data_access', 'ui.app', 'ui.data_access', 'ui.pages.home', 'ui.pages.phase1_recipe_selection', 'ui.pages.b1_drifter_context', 'ui.pages.mindoro_validation', 'ui.pages.cross_model_comparison', 'ui.pages.mindoro_validation_archive', 'ui.pages.dwh_transfer_validation', 'ui.pages.phase4_oiltype_and_shoreline', 'ui.pages.legacy_2016_support', 'ui.pages.artifacts_logs']
for name in mods:
    importlib.import_module(name)
print('IMPORT_OK')"`
- Output snippet:

```text
IMPORT_OK
2026-04-29 00:21:30.396 WARNING streamlit.runtime.caching.cache_data_api: No runtime found, using MemoryCacheStorageManager
2026-04-29 00:21:30.404 WARNING streamlit.runtime.caching.cache_data_api: No runtime found, using MemoryCacheStorageManager
2026-04-29 00:21:31.013 WARNING streamlit.runtime.caching.cache_data_api: No runtime found, using MemoryCacheStorageManager
```

### streamlit_runtime_smoke (WARN)

- Topic: `dashboard`
- Summary: Streamlit runtime smoke was not required for this run
- rerun with --require-dashboard to make the headless UI health check mandatory

### safe_entry_batch (WARN)

- Topic: `launcher_entries`
- Summary: quick mode skipped the longer read-only entry batch
- rerun without --quick to exercise final_validation_package, phase5_sync, figure_package_publication, and trajectory_gallery_panel

### manuscript_numbers (PASS)

- Topic: `manuscript_numbers`
- Summary: panel review JSON report checked for stored manuscript-number discrepancies

### env_example (PASS)

- Topic: `environment`
- Summary: environment template check completed
- .env.example exists
- .env already exists and was not overwritten
