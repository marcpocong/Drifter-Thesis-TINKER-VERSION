# A_BASELINE_GOVERNANCE

## Current Default

- Default baseline artifact: `config/phase1_baseline_selection.yaml`
- Current default selected recipe: `cmems_gfs`
- Focused historical four-recipe winner: `cmems_gfs`
- Current default evidence base: `output/phase1_mindoro_focus_pre_spill_2016_2023/phase1_recipe_ranking.csv`, `output/phase1_mindoro_focus_pre_spill_2016_2023/phase1_recipe_summary.csv`, `output/phase1_mindoro_focus_pre_spill_2016_2023/phase1_segment_metrics.csv`, and `output/phase1_mindoro_focus_pre_spill_2016_2023/phase1_official_adoption_decision.json`
- Official selection rule: promote the focused historical four-recipe winner directly into official B1
- Repo-level default path wiring still points official spill-case work at that artifact through `config/settings.yaml` via `phase1_baseline_selection_path`
- `src/utils/io.py` still treats that canonical artifact as the default baseline-selection path unless an override is supplied
- Downstream audit consumers that needed cleanup were `src/services/phase2_finalization_audit.py` and `src/services/phase4_oiltype_and_shoreline.py`
- Status/docs that describe the current default story are `docs/PHASE_STATUS.md`, `docs/ARCHITECTURE.md`, and `docs/COMMAND_MATRIX.md`

## Candidate Paths

- Staged focused historical-winner artifact: `output/phase1_mindoro_focus_pre_spill_2016_2023/phase1_baseline_selection_candidate.yaml`
- Current staged focused candidate selected recipe: `cmems_gfs`
- Focused adoption-decision artifacts: `output/phase1_mindoro_focus_pre_spill_2016_2023/phase1_official_adoption_decision.json`, `output/phase1_mindoro_focus_pre_spill_2016_2023/phase1_official_adoption_decision.md`
- Broader regional reference candidate artifact: `output/phase1_production_rerun/phase1_baseline_selection_candidate.yaml`
- Manual promotion semantics remain unchanged: staged candidates are trial-only unless you explicitly promote them over `config/phase1_baseline_selection.yaml`

## Risks

- The focused lane searched through early 2023, but its accepted registry does not include near-2023 accepted segments.
- The official B1 baseline now promotes the focused historical winner directly, so main-validation reruns and downstream packages must be refreshed whenever the focused winner changes.
- Existing stored official manifests that were generated before the package refreshes still cite earlier provenance wording; rerun is required for those manifests to mirror the updated adoption-decision story.
- `config/settings.yaml` still keeps the broader project ROI at `[115.0, 122.0, 6.0, 14.5]` on purpose; the official Phase 1 audit box is now carried separately so we do not silently change spill-case or prototype spatial meaning.

## Files Changed

- `config/phase1_baseline_selection.yaml`
- `config/phase1_mindoro_focus_pre_spill_2016_2023.yaml`
- `config/settings.yaml`
- `src/utils/io.py`
- `src/services/phase1_production_rerun.py`
- `src/services/phase1_finalization_audit.py`
- `src/services/phase2_finalization_audit.py`
- `src/services/phase4_oiltype_and_shoreline.py`
- `docs/PHASE_STATUS.md`
- `docs/ARCHITECTURE.md`
- `docs/COMMAND_MATRIX.md`
- `docs/A_BASELINE_GOVERNANCE_DECISION_MEMO.md`
