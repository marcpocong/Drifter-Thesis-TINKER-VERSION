## Mindoro Phase 1 Provenance Final

Mindoro B1 now inherits its recipe provenance from the separate focused `phase1_mindoro_focus_pre_spill_2016_2023` drifter-based Phase 1 rerun.

Authoritative current provenance:
- Active workflow mode: `phase1_mindoro_focus_pre_spill_2016_2023`
- Active selected recipe: `cmems_gfs`
- Historical four-recipe winner in that focused lane: `cmems_gfs`
- Recipe family actually tested in that focused lane: `cmems_era5`, `cmems_gfs`, `hycom_era5`, `hycom_gfs`
- Active baseline file: `config/phase1_baseline_selection.yaml`
- Adoption-decision artifacts: `output/phase1_mindoro_focus_pre_spill_2016_2023/phase1_official_adoption_decision.json`, `output/phase1_mindoro_focus_pre_spill_2016_2023/phase1_official_adoption_decision.md`

Important honesty notes:
- This focused rerun searched through early 2023, but its accepted registry does not include near-2023 accepted segments.
- The focused rerun now includes GFS-backed recipes after a full accepted-month GFS preflight and cache backfill.
- Official B1 now promotes the focused historical winner directly, so the focused four-recipe winner `cmems_gfs` is also the official main-validation recipe.
- Phase 3B does not directly ingest drifters. It inherits a recipe selected by the separate focused drifter-based Phase 1 rerun.
- The stored March 13 -> March 14 B1 science bundle keeps its original raw-generation history.

Governance result:
- Mindoro B1 provenance: focused Mindoro Phase 1 lane
- Broader `phase1_regional_2016_2022` lane: preserved regional reference/governance lane
- March 6 B2: preserved legacy honesty row
- PyGNOME on March 13 -> March 14: comparator-only
