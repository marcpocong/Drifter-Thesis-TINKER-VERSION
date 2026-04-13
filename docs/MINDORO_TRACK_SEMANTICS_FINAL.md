# Mindoro Track Semantics Final

This note locks the current thesis-facing Mindoro semantics to the current repo state without rerunning science.

## Track Roles

- `A` means the same-case March 13 -> March 14 PyGNOME/OpenDrift cross-model comparator support track attached to the promoted B1 case.
- `B1` means the March 13 -> March 14 NOAA reinit primary validation row.
- `B2` means the March 6 legacy honesty row.
- `B3` means the March 3 -> March 6 broader-support legacy row.

## Primary Claim Rule

- `B1` is the only main-text primary Mindoro validation claim.
- `A` stays visible for comparative discussion, but it is comparator-only support and never truth.
- `A` is not a co-primary validation row and should not be selected by any "primary Mindoro" packaging or UI view.
- `B2` and `B3` remain visible for provenance honesty, but neither replaces `B1`.

## Phase 1 Provenance Rule

- The focused `phase1_mindoro_focus_pre_spill_2016_2023` rerun is the active Mindoro provenance lane for the B1 recipe story.
- That focused rerun provides provenance only; `Phase 3B` itself does not directly ingest drifters.
- The broader `phase1_regional_2016_2022` rerun remains preserved as a reference/governance lane, not the active Mindoro provenance lane.

## Spatial Extents

- Focused Phase 1 validation box:
  - `[118.751, 124.305, 10.620, 16.026]`
  - Used by the separate Mindoro-focused drifter-validation/provenance rerun.
- `mindoro_case_domain`:
  - `[115.0, 122.0, 6.0, 14.5]`
  - Broad official fallback transport/forcing domain and overview extent for the Mindoro spill-case lane.
  - Not the focused Phase 1 validation box.
  - Not the canonical scoring-grid display bounds.
- Scoring domain / scoring-grid display bounds:
  - `[120.90964677179262, 122.0621541786303, 12.249384840763462, 13.783655303175253]`
  - Comes from the stored scoring-grid artifact in `data_processed/grids/scoring_grid.yaml` and the official forecast manifest.
  - This is the narrower scoreable display extent used when the scoring-grid artifact is available.

## Practical Reporting Rule

- When writing the manuscript or labeling figures, say `B1` for the main validation result.
- When discussing cross-model behavior on the same March 14 target, say `A` and keep the comparator-only caveat explicit.
- When showing March 6 or March 3 -> March 6 context, label them as `B2` and `B3` legacy rows rather than as replacements for `B1`.
