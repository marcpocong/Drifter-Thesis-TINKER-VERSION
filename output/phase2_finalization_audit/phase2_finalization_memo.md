# Phase 2 Finalization Memo

This phase is a read-only architectural and semantics audit. It does not rerun the expensive official Mindoro workflow, and it does not overwrite finished Mindoro or DWH scientific outputs.

## What This Patch Finalizes

- Adds a dedicated `phase2_finalization_audit` route that writes its own audit package under `output/phase2_finalization_audit/`.
- Makes the official Phase 2 schema capable of recording recipe-family honesty fields, upstream Phase 1 freeze dependencies, and explicit provisional reasons.
- Keeps `mask_p50` and `mask_p90` semantics unchanged while documenting the canonical product catalog more clearly.
- Updates launcher/help/docs so official Phase 2 no longer reads like a half-prototype workflow.

## Requirement Audit

- `deterministic_control_separate_from_ensemble`: `implemented_and_scientifically_ready`. Forecast manifest exposes a deterministic-control NetCDF plus a separate ensemble manifest reference.
- `canonical_product_names_and_semantics`: `implemented_and_scientifically_ready`. Canonical products present = ['control_density_norm', 'control_footprint_mask', 'mask_p50', 'mask_p50_datecomposite', 'mask_p90', 'prob_presence']. Product catalog rows = 7.
- `ensemble_50_member_design`: `implemented_and_scientifically_ready`. Ensemble manifest records ensemble_size=50 and member_runs=50.
- `date_composite_logic`: `implemented_and_scientifically_ready`. Date-composite products present: prob_presence_datecomposite=True, mask_p50_datecomposite=True for 2023-03-06.
- `common_grid_same_grid_discipline`: `implemented_and_scientifically_ready`. Forecast grid_id = EPSG:32651:1000.0:124x169:274000:1355000:398000:1524000; ensemble grid_id = EPSG:32651:1000.0:124x169:274000:1355000:398000:1524000.
- `loading_audits_and_manifests`: `implemented_and_scientifically_ready`. Forecast manifest exists = True, ensemble manifest exists = True, loading audit rows = 51.
- `explicit_provenance_status_fields`: `implemented_and_scientifically_ready`. Existing manifests expose status_flags/recipe provenance = True. Current on-disk phase2_finalization block present = True. Code support for the honesty block = True.
- `user_facing_output_names_and_help_text`: `implemented_and_scientifically_ready`. README, docs, launcher help, and __main__ now point official users to forecast manifests, loading audits, prob_presence, mask_p50, and mask_p90 while keeping prototype outputs explicitly legacy/debug-only.
- `phase1_recipe_family_drift_honesty`: `implemented_but_provisional`. Official recipe family expected = ['cmems_era5', 'cmems_gfs', 'hycom_era5', 'hycom_gfs']; runtime-defined = ['cmems_era5', 'cmems_gfs', 'hycom_era5', 'hycom_gfs']; locally available = ['cmems_era5', 'cmems_gfs', 'hycom_era5', 'hycom_gfs']; legacy runtime IDs = ['cmems_ncep', 'hycom_ncep']; gfs_wind.nc present for the official case = True. Blocker: The current repo state still carries legacy *_ncep recipe IDs in config space and does not yet include gfs_wind.nc, so the Phase 2 frozen-baseline story remains upstream-provisional.
- `missing_gfs_not_silent`: `implemented_and_scientifically_ready`. Explicit GFS-missing guard messages found = 2; current official case has gfs_wind.nc = True.
- `legacy_recipe_hits_inventory`: `implemented_but_provisional`. Legacy recipe-related code/config hits currently inventoried = 10. Blocker: Legacy naming drift still exists in config space and a few official-support branches; this patch makes that explicit but does not fully remove the drift.

## Final Questions

- Is Phase 2 scientifically usable as implemented right now? `yes`
- What remains provisional? The transport model is still marked provisional, the final frozen-baseline story still depends on the later Phase 1 production rerun, and the full GFS-capable official recipe family is not yet locally available.
- Does Phase 2 still depend on a later full Phase 1 production rerun for the final frozen baseline story? `no`
- Is any part of official product generation still coupled to legacy recipe naming drift? `yes`

## Biggest Remaining Provisional Item

- Phase 2 is scientifically usable on the promoted cmems_gfs baseline, but the final frozen story still depends on retiring legacy *_ncep runtime IDs from official-mode config space and closing the remaining transport-model provisionality.
