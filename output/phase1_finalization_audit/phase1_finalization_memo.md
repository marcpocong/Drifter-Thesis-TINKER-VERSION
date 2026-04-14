# Phase 1 Finalization Memo

This phase is a read-only architectural audit. It does not rerun the expensive 2016-2022 production study, and it does not overwrite finished Mindoro, DWH, or final-validation scientific outputs.

## What This Patch Finalizes

- Adds a dedicated `phase1_finalization_audit` route that writes its own audit package under `output/phase1_finalization_audit/`.
- Freezes explicit metadata for the Chapter 3 target window, Phase 1 validation box, segment policy, and recipe-family intent without pretending the scientific study already exists.
- Makes Phase 1 loading-audit hard-fail/status fields explicit in code for the next real regional rerun.
- Clarifies that the preserved `prototype_2016` workflow is a legacy debugging path, not the final Chapter 3 Phase 1 evidence base.

## What The Audit Found

- `historical_window_2016_2022`: `partially_implemented`. Local Phase 1 rankings exist for 7 prototype dates covering year(s): none. The target Chapter 3 window is 2016-2022. Blocker: Only the legacy three-date prototype evidence is present locally; the multi-year regional pool has not been built.
- `fixed_phase1_validation_box`: `implemented_but_provisional`. Fixed Phase 1 validation-box metadata is present with bounds [118.751, 124.305, 10.62, 16.026]. Blocker: The box is frozen in metadata but has not yet been exercised through the final 2016-2022 accepted/rejected segment study.
- `drogued_segments_only_core_pool`: `missing`. Prototype drifter headers expose columns ['time', 'lat', 'lon', 'ID', 've', 'vn']. Blocker: No explicit drogue-status filtering was found in the local Phase 1 data path.
- `non_overlapping_72h_segments`: `partially_implemented`. Prototype Phase 1 runs are 72 h windows for dates ['output', 'output', 'output', 'output', 'output', 'output', 'output'], but no accepted/rejected segment registry exists. Blocker: The repo does not yet materialize a general non-overlapping segment registry for the 2016-2022 regional pool.
- `official_recipe_family`: `partially_implemented`. Target recipe family metadata = ['cmems_era5', 'cmems_gfs', 'hycom_era5', 'hycom_gfs']; runtime Phase 1 recipe IDs = ['cmems_era5', 'cmems_gfs', 'cmems_ncep', 'hycom_era5', 'hycom_gfs', 'hycom_ncep']; legacy alias map = ['cmems_ncep', 'hycom_ncep']; gfs_wind.nc present locally = True. Blocker: The core runtime family is present but still needs the final multi-year rerun.
- `loading_audit_hard_fail_behavior`: `implemented_and_scientifically_ready`. Validation service exposes phase1_loading_audit_v2 with policy `invalidate_recipe_on_required_forcing_or_simulation_failure_and_raise_if_no_valid_recipes_remain`. Existing on-disk Phase 1 audits already carry the new fields = True.
- `accepted_segment_registry`: `implemented_but_provisional`. Accepted-segment registry candidates: ['output/phase1_mindoro_focus_pre_spill_2016_2023/phase1_accepted_segment_registry.csv', 'output/phase1_production_rerun/phase1_accepted_segment_registry.csv']. Blocker: The final 2016-2022 accepted regional segment registry has not been produced locally.
- `rejected_segment_registry`: `implemented_but_provisional`. Rejected-segment registry candidates: ['output/phase1_mindoro_focus_pre_spill_2016_2023/phase1_rejected_segment_registry.csv', 'output/phase1_production_rerun/phase1_rejected_segment_registry.csv']. Blocker: The final 2016-2022 rejected regional segment registry has not been produced locally.
- `segment_metrics`: `implemented_but_provisional`. Segment-metrics candidates: ['output/phase1_mindoro_focus_pre_spill_2016_2023/phase1_segment_metrics.csv', 'output/phase1_production_rerun/phase1_segment_metrics.csv']. Blocker: The final regional accepted/rejected segment metrics still require the dedicated 2016-2022 run.
- `recipe_summary`: `implemented_but_provisional`. Phase 1 recipe-summary candidates: ['output/phase1_mindoro_focus_pre_spill_2016_2023/phase1_recipe_summary.csv', 'output/phase1_production_rerun/phase1_recipe_summary.csv']. Blocker: The regional recipe-summary layer still needs to be generated from the final segment registry.
- `recipe_ranking`: `implemented_but_provisional`. Validation rankings exist for 7 prototype runs; the current frozen recipe is `cmems_gfs`. Blocker: Current rankings come from the old prototype cases, not from the final 2016-2022 regional segment corpus.
- `frozen_baseline_artifact`: `implemented_but_provisional`. Frozen baseline artifact exists and currently selects `cmems_gfs`. Blocker: The artifact is real, but its evidence base is still the legacy three-date prototype rather than the final regional study.
- `transport_vs_spill_validation_separation`: `implemented_and_scientifically_ready`. Official spill-case workflows consume a frozen Phase 1 baseline, while the final validation package is read-only and built from separate finished Mindoro/DWH outputs.
- `prototype_mode_preserved_not_final_study`: `implemented_and_scientifically_ready`. prototype_2016 remains the preserved debugging workflow, and the runtime context now explicitly labels it as non-final.

## Verdict

- Architecture structurally supported now: `yes`
- Scientifically ready to freeze as final Phase 1: `no`
- Full production rerun still needed: `yes`
- Biggest remaining blocker: The repo still lacks the accepted/rejected drogued 72 h segment registry generated from a true 2016-2022 regional drifter pool, so the frozen baseline cannot yet be defended as the final Chapter 3 Phase 1 study.

## Deferred Expensive Work

- Build the real 2016-2022 regional drifter pool.
- Filter it to the drogued-only core pool.
- Generate accepted and rejected non-overlapping 72 h segment registries.
- Export segment metrics, recipe summary, final recipe ranking, and then refresh the frozen baseline artifact from that corpus.
