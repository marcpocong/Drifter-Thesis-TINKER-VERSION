# Mindoro Track Semantics Final

This note locks the current Mindoro track semantics to the stored-output review package.

## Track Roles

- `A` = same-case March 13-14 OpenDrift versus PyGNOME comparator-only support
- `B1` = March 13-14 `R1_previous` primary public-observation validation row
- `archive_r0` = March 13-14 `R0` archived baseline preserved for provenance only
- `B2` = March 6 archive-only sparse-reference row
- `B3` = March 3-6 archive-only broader-support row

## Primary Claim Rule

- `B1` is the only main-text primary Mindoro validation claim.
- `A` stays available for cross-model discussion, but it is comparator-only support and never truth.
- `archive_r0`, `B2`, and `B3` are preserved archive/provenance rows and must not be presented as co-primary evidence.

## `B1` Diagnostic Rule

- FSS `1 / 3 / 5 / 10 km`: `0.0000 / 0.0441 / 0.1371 / 0.2490`
- Mean FSS: `0.1075`
- `R1_previous` forecast cells `5`; observed cells `22`; nearest distance `1414.21 m`; centroid distance `7358.16 m`
- `IoU = 0.0`; `Dice = 0.0`
- Promote `R1_previous` because it survives and is scoreable, not because it is an exact-grid match

## Shared-Imagery Rule

- March 13-14 is a reinitialization-based public-observation validation check.
- Both public products cite the same March 12 WorldView-3 imagery provenance.
- Do not describe March 13-14 as independent day-to-day validation.

## Phase 1 Provenance Rule

- The focused `phase1_mindoro_focus_pre_spill_2016_2023` rerun is the active provenance lane for the `B1` recipe story.
- That lane provides recipe provenance only; Phase 3B itself does not directly ingest drifters.
- The broader `phase1_regional_2016_2022` rerun remains preserved as reference/governance context, not the active `B1` provenance lane.

## Spatial Extents

- Focused Phase 1 validation box: `[118.751, 124.305, 10.620, 16.026]`
- `mindoro_case_domain`: `[115.0, 122.0, 6.0, 14.5]`
- Scoring-grid display bounds: `[120.90964677179262, 122.0621541786303, 12.249384840763462, 13.783655303175253]`

## Practical Reporting Rule

- Say `March 13-14 R1_previous primary validation row` for the main Mindoro result.
- Say `Track A comparator-only support` for the same-case OpenDrift-versus-PyGNOME comparison.
- Say `March 13-14 R0 archived baseline`, `B2 archive-only sparse reference`, and `B3 archive-only broader-support reference` for preserved March-family materials.
