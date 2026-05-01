# Final Repo Submission Checklist

## 1. Final submission status

- Status: Ready with local environment limitations
- Current git commit: `081e78f325d61ccb42b6d8acb70a5a92de1d00f7`
- Branch: `main`
- Timestamp: `2026-05-01T15:27:10Z` (2026-05-01 Asia/Manila local checkpoint)
- Working tree status: started dirty because `scripts/figures/make_descriptive_label_manuscript_figures.py` was already untracked before this checkpoint; relevant checklist/report changes are isolated for this commit.
- GitHub Actions should be checked after push and must be green before treating the repository as final panel-ready.

## 2. Reviewer first-open files

- `README.md`
- `PANEL_QUICK_START.md`
- `docs/FINAL_PAPER_ALIGNMENT.md`
- `docs/PANEL_REVIEW_GUIDE.md`
- `docs/DATA_SOURCES.md`
- `docs/PAPER_TO_REPO_CROSSWALK.md`
- `docs/ARCHIVE_GOVERNANCE.md`
- `docs/REPRODUCIBILITY_BUNDLE_GUIDE.md`

## 3. Evidence hierarchy

1. Focused Mindoro Phase 1 transport provenance.
2. Standardized deterministic and 50-member forecast products.
3. Mindoro B1 March 13-14 primary public-observation validation.
4. Mindoro same-case OpenDrift-PyGNOME comparator support.
5. DWH external transfer validation.
6. Mindoro oil-type and shoreline support/context.
7. Secondary 2016 drifter-track and legacy FSS support.
8. Reproducibility/governance/read-only package layer.

## 4. Primary evidence launcher entries

- `phase1_mindoro_focus_provenance`
- `mindoro_phase3b_primary_public_validation`
- `dwh_reportable_bundle`
- `mindoro_reportable_core` is retained as a full intentional rerun path for researcher/audit use and is not the default panel path.

## 5. Support/context entries

- `mindoro_phase4_only`
- `mindoro_appendix_sensitivity_bundle`
- `prototype_legacy_final_figures` for secondary 2016 support figures from stored outputs.
- `prototype_legacy_bundle` for legacy secondary 2016 archive/support work.

## 6. Archive/provenance and legacy summary

Archived work is preserved for audit and rerun transparency. It is not final-paper primary evidence. Hidden experimental entries remain non-reportable unless future study changes explicitly promote them.

- `phase1_regional_reference_rerun`: archive_provenance / visible_archive / provenance only
- `phase1_regional_reference_rerun_alias`: archive_provenance / hidden_alias / provenance only
- `phase1_mindoro_focus_pre_spill_experiment_alias`: archive_provenance / hidden_alias / provenance only
- `mindoro_march13_14_phase1_focus_trial`: archive_provenance / visible_archive / provenance only
- `mindoro_march6_recovery_sensitivity`: archive_provenance / visible_archive / not reflected; archived for audit
- `mindoro_march23_extended_public_stress_test`: archive_provenance / visible_archive / not reflected; archived for audit
- `phase3b_mindoro_march3_4_philsa_5000_experiment`: experimental_only / hidden_experimental / not reflected; archived for audit
- `mindoro_mar09_12_multisource_experiment`: experimental_only / hidden_experimental / not reflected; archived for audit
- `phase3b_mindoro_march13_14_reinit_5000_experiment`: experimental_only / hidden_experimental / not reflected; archived for audit
- `mindoro_march13_14_noaa_reinit_stress_test_alias`: archive_provenance / hidden_alias / provenance only
- `mindoro_march3_6_base_case_archive`: archive_provenance / read_only_only / not reflected; archived for audit
- `prototype_legacy_final_figures`: legacy_support / visible_archive / Appendix/support
- `prototype_2021_bundle`: legacy_support / visible_archive / not reflected; archived for audit
- `prototype_legacy_bundle`: legacy_support / visible_archive / Appendix/support
- `prototype_2016_support_surfaces`: legacy_support / read_only_only / Appendix/support
- `root_debug_artifacts_prompt1`: repository_only_development / read_only_only / not reflected; archived for audit

## 7. Read-only governance/package entries

- Final validation package: `output/final_validation_package/` and `final_validation_package` launcher entry.
- Final reproducibility package: `output/final_reproducibility_package/`.
- Figure/publication package: `output/figure_package_publication/` and `figure_package_publication` launcher entry.
- Panel/read-only UI: `panel.ps1`, `start.ps1 -Panel`, dashboard code under `ui/`, and `b1_drifter_context_panel`.
- Registry validators: archive, paper-to-output, data-source, and final-paper guardrail validators.
- Archive governance and data-source provenance: `config/archive_registry.yaml`, `docs/ARCHIVE_GOVERNANCE.md`, `config/data_sources.yaml`, and `docs/DATA_SOURCES.md`.

Panel mode is read-only / packaging-safe. Expensive reruns require explicit launcher entry selection. Archive/provenance and hidden experimental entries are not final-paper primary evidence. Aliases resolve to canonical entries where implemented.

## 8. Key paper metrics checklist

### Focused Phase 1

- workflow mode `phase1_mindoro_focus_pre_spill_2016_2023`
- historical window 2016-01-01 to 2023-03-02
- focused box [118.751, 124.305, 10.620, 16.026]
- full strict accepted segments 65
- February-April ranked subset 19
- selected recipe `cmems_gfs`
- ranking:
  - `cmems_gfs` 4.5886 / 4.6305
  - `cmems_era5` 4.6237 / 4.5916
  - `hycom_gfs` 4.7027 / 4.9263
  - `hycom_era5` 4.7561 / 5.0106

### Mindoro B1

- FSS 1 km 0.0000
- FSS 3 km 0.0441
- FSS 5 km 0.1371
- FSS 10 km 0.2490
- mean FSS 0.1075
- forecast cells 5
- observed cells 22
- nearest distance 1414.21 m
- centroid distance 7358.16 m
- IoU 0.0
- Dice 0.0
- interpretation: coastal-neighborhood usefulness, not exact 1 km overlap

### Mindoro same-case comparator

- OpenDrift p50: 5 cells, nearest distance 1414.21 m, mean FSS 0.1075
- PyGNOME deterministic: 6 cells, nearest distance 6082.76 m, mean FSS 0.0061
- comparator-only, never truth

### DWH

- `CASE_DWH_RETRO_2010_72H`
- HYCOM GOFS 3.1 currents + ERA5 winds + CMEMS wave/Stokes
- deterministic event-corridor mean FSS 0.5568
- ensemble p50 event-corridor mean FSS 0.5389
- ensemble p90 event-corridor mean FSS 0.4966
- PyGNOME comparator event-corridor mean FSS 0.3612
- external transfer validation only

### Oil-type support

- light oil: 0.02% beached, first arrival 4 h, 11 impacted segments, QC pass
- fixed-base medium-heavy proxy: 0.61% beached, first arrival 4 h, 10 impacted segments, QC flagged
- heavier oil: 0.63% beached, first arrival 4 h, 11 impacted segments, QC pass
- support/context only

### Secondary 2016

- direct drifter-track and legacy FSS support only
- not public-spill validation
- not a replacement for Mindoro B1 or DWH

## 9. Probability semantics

- `prob_presence` is cellwise ensemble probability of presence.
- `mask_p50` is P >= 0.50 and is the preferred probabilistic footprint.
- `mask_p90` is P >= 0.90 and is conservative support/comparison only.
- p90 is not a broad envelope.

## 10. Not claimed

- no exact 1 km Mindoro success
- no universal operational accuracy
- no PyGNOME truth
- no DWH Mindoro recalibration
- no oil-type primary validation
- no secondary 2016 public-spill validation
- no p90 broad-envelope interpretation

## 11. Validation command results

1. `python -m pytest tests/test_final_paper_consistency_guardrails.py -q` - pass. 9 passed in 0.81 s.
2. `python -m pytest tests -q` - local_environment_limitation. Collection stopped with 56 import errors and 2 skipped because the local Python 3.14 environment lacks numpy, pandas, xarray, geopandas, and yaml/PyYAML.
3. `python scripts/validate_archive_registry.py` - pass. Archive registry validation passed against launcher matrix.
4. `python scripts/validate_paper_to_output_registry.py` - pass. Paper-to-output registry validation passed: 51 entries, 0 errors, 0 warnings.
5. `python scripts/validate_data_sources_registry.py` - pass. Data-source provenance registry validation passed: 13 sources; roles and secret policy valid.
6. `python scripts/validate_final_paper_guardrails.py` - pass. Final-paper guardrail validation passed for labels, launcher routing, claim boundaries, values, registries, and p90 semantics.
7. `pwsh ./start.ps1 -ValidateMatrix -NoPause` - local_environment_limitation. pwsh executable is unavailable locally; Windows PowerShell fallback and Python launcher schema fallback passed.
8. `powershell -ExecutionPolicy Bypass -File .\start.ps1 -ValidateMatrix -NoPause` - pass. Launcher matrix validation passed: 27 entries, 27 pass, 0 fail.
9. `python launcher-matrix fallback schema check` - pass. Fallback validation passed for 27 entries.
10. `pwsh ./start.ps1 -List -NoPause` - local_environment_limitation. pwsh unavailable; Windows PowerShell fallback list completed.
11. `powershell -ExecutionPolicy Bypass -File .\start.ps1 -List -NoPause` - pass. Catalog grouped by thesis role; archive/legacy/support/read-only routes are separated from main evidence.
12. `pwsh ./start.ps1 -ListRole primary_evidence -NoPause` - local_environment_limitation. pwsh unavailable; Windows PowerShell fallback list completed.
13. `powershell -ExecutionPolicy Bypass -File .\start.ps1 -ListRole primary_evidence -NoPause` - pass. Primary evidence entries listed: phase1_mindoro_focus_provenance, mindoro_phase3b_primary_public_validation, dwh_reportable_bundle, mindoro_reportable_core.
14. `pwsh ./start.ps1 -ListRole archive_provenance -NoPause` - local_environment_limitation. pwsh unavailable; Windows PowerShell fallback list completed.
15. `powershell -ExecutionPolicy Bypass -File .\start.ps1 -ListRole archive_provenance -NoPause` - pass. Archive/provenance aliases, archive entries, and hidden experimental entries listed separately from primary evidence.
16. `pwsh ./start.ps1 -Explain mindoro_phase3b_primary_public_validation -NoPause` - local_environment_limitation. pwsh unavailable; Windows PowerShell fallback explain completed.
17. `powershell -ExecutionPolicy Bypass -File .\start.ps1 -Explain mindoro_phase3b_primary_public_validation -NoPause` - pass. B1 preview confirms primary public-observation validation boundary, expensive rerun status, and curated Phase 3B output path.
18. `pwsh ./start.ps1 -Explain dwh_reportable_bundle -NoPause` - local_environment_limitation. pwsh unavailable; Windows PowerShell fallback explain completed.
19. `powershell -ExecutionPolicy Bypass -File .\start.ps1 -Explain dwh_reportable_bundle -NoPause` - pass. DWH preview confirms external transfer validation boundary, comparator-only PyGNOME, and curated Phase 3C output path.
20. `pwsh ./start.ps1 -Entry mindoro_phase3b_primary_public_validation -DryRun -NoPause` - local_environment_limitation. pwsh unavailable; Windows PowerShell fallback dry run completed.
21. `powershell -ExecutionPolicy Bypass -File .\start.ps1 -Entry mindoro_phase3b_primary_public_validation -DryRun -NoPause` - pass. Dry run printed the two Docker commands that would run and confirmed no workflow executed and no outputs modified.
22. `runtime-constructed forbidden-label scan` - pass. Forbidden manuscript label absent from tracked text files.
23. `uploaded manuscript filename / stale wording scan` - pass. No uploaded manuscript-like filenames or stale current-surface wording found. Safe negated overclaim hits: 66. Historical generated-output hits: 59, documented as archived/generated and not current reviewer-first docs/config.

## 12. Known limitations

- Full local pytest collection requires missing local science/test dependencies: numpy, pandas, xarray, geopandas, and yaml/PyYAML.
- pwsh is not installed in this local shell; Windows PowerShell and Python launcher fallbacks passed.
- The working tree started dirty because scripts/figures/make_descriptive_label_manuscript_figures.py was already untracked before this checkpoint and was not included in this commit.
- Expensive scientific workflows were not rerun; this checkpoint used stored-output checks, validators, scans, launcher listing, and dry runs only.
- Raw canonical generation roots are provenance/rerun paths; curated Phase 3B, Phase 3C, phase4, 2016 benchmark, final validation, and reproducibility package paths are the panel-facing paths.
- Historical generated audit snapshots under output/defense_readiness and output/repo_cleanup_audit contain old audit text, already documented as stale provenance; current reviewer-first docs/config and launcher output use final-paper wording.

## 13. Final reviewer instruction

Start with `.\panel.ps1` or `.\start.ps1 -Panel`. Use read-only review paths first. Use explicit launcher entries only for intentional reruns. Archive/provenance routes are inspectable but not primary evidence.
