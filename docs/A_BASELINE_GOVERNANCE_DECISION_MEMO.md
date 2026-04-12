# A_BASELINE_GOVERNANCE

## Current Default

- Default baseline artifact: `config/phase1_baseline_selection.yaml`
- Current default selected recipe: `cmems_era5`
- Current default evidence base: `output/CASE_2016-09-01/validation/validation_ranking.csv`, `output/CASE_2016-09-06/validation/validation_ranking.csv`, and `output/CASE_2016-09-17/validation/validation_ranking.csv`
- Repo-level default path wiring still points official spill-case work at that artifact through `config/settings.yaml` via `phase1_baseline_selection_path`
- `src/utils/io.py` still treats that canonical artifact as the default baseline-selection path unless an override is supplied
- Downstream audit consumers that needed cleanup were `src/services/phase2_finalization_audit.py` and `src/services/phase4_oiltype_and_shoreline.py`
- Status/docs that describe the current default story are `docs/PHASE_STATUS.md`, `docs/ARCHITECTURE.md`, and `docs/COMMAND_MATRIX.md`

## Candidate Path

- Staged candidate artifact: `output/phase1_production_rerun/phase1_baseline_selection_candidate.yaml`
- Current staged candidate selected recipe: `cmems_gfs`
- Candidate evidence base: the completed `output/phase1_production_rerun/` registry, loading-audit, metrics, summary, ranking, and manifest bundle
- Manual promotion semantics remain unchanged: this candidate is trial-only unless you explicitly promote it over `config/phase1_baseline_selection.yaml`

## Risks

- The canonical default baseline is still scientifically upstream-provisional because it remains backed by the preserved three-date 2016 prototype, not by the completed 2016-2022 regional study.
- Existing stored official manifests that were generated before a candidate-baseline rerun still cite `config/phase1_baseline_selection.yaml`; rerun is required for those manifests to record the staged candidate source path.
- `config/settings.yaml` still keeps the broader project ROI at `[115.0, 122.0, 6.0, 14.5]` on purpose; the official Phase 1 audit box is now carried separately so we do not silently change spill-case or prototype spatial meaning.

## Files Changed

- `config/phase1_baseline_selection.yaml`
- `config/settings.yaml`
- `src/utils/io.py`
- `src/services/phase1_finalization_audit.py`
- `src/services/phase2_finalization_audit.py`
- `src/services/phase4_oiltype_and_shoreline.py`
- `docs/PHASE_STATUS.md`
- `docs/ARCHITECTURE.md`
- `docs/COMMAND_MATRIX.md`
- `docs/A_BASELINE_GOVERNANCE_DECISION_MEMO.md`
