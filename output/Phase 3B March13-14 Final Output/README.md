# Phase 3B March13-14 Final Output

Thesis-facing title: Phase 3B Observation-Based Spatial Validation Using Public Mindoro Spill Extents

This is a read-only curated export of the promoted Mindoro B1 family.
It does not replace the canonical scientific directory under `output/CASE_MINDORO_RETRO_2023/phase3b_extended_public_scored_march13_14_reinit/`.

What is primary here:
- B1 = Mindoro March 13 -> March 14 NOAA reinit primary validation.
- OpenDrift-versus-observation is the main claim in this folder.
- The primary success statement is that the promoted OpenDrift row achieves non-zero FSS against the March 14 observed spill mask.
- PyGNOME remains comparator-only; OpenDrift-versus-PyGNOME figures here are supporting context only and never truth replacement.

What remains secondary:
- March 6 remains a preserved legacy honesty/reference row and is not renamed as primary.
- The separate March 13 -> March 14 cross-model comparator family is exported only in a comparator-only subgroup and is not the main result.
- This folder is curated packaging over canonical scientific outputs; it does not change any scoreable products.

Mindoro Phase 1 provenance:
- Stored B1 run recipe source path: `config/phase1_baseline_selection.yaml`
- Stored B1 run selected recipe: `cmems_gfs`
- Active focused drifter-based provenance workflow: `phase1_mindoro_focus_pre_spill_2016_2023`
- Focused provenance artifact: `config/phase1_baseline_selection.yaml`
- Focused provenance selected recipe: `cmems_gfs`
- Focused historical four-recipe winner: `cmems_gfs`
- Same recipe confirmed: `Yes`
- Interpretation: The separate phase1_mindoro_focus_pre_spill_2016_2023 Mindoro-focused drifter rerun selected cmems_gfs as the active official B1 recipe after the focused four-recipe comparison. It now serves as the active Mindoro-specific recipe-provenance lane for B1 without rewriting the original March 13 -> March 14 raw-generation history.
- The focused rerun searched through early 2023, but its accepted registry does not include near-2023 accepted segments.
- Historical GFS winner not adopted for official B1: `False`
- Non-GFS fallback recipe: `not_needed`
- The broader `phase1_regional_2016_2022` lane remains preserved as a broader reference/governance lane and is not the active provenance for B1.
- Phase 3B itself does not directly ingest drifters; it inherits a recipe selected by the separate focused Phase 1 rerun.

Shared-imagery caveat:
- Both NOAA/NESDIS public products cite WorldView-3 imagery acquired on 2023-03-12, so the promoted March 13 -> March 14 row is a reinitialization-based public-validation pair with shared-imagery provenance rather than a fully independent day-to-day validation.
