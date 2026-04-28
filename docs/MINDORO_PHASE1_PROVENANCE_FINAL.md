# Mindoro Phase 1 Provenance Final

Mindoro `B1` inherits its recipe provenance from the separate focused `phase1_mindoro_focus_pre_spill_2016_2023` drifter-based Phase 1 rerun.

## Authoritative Current Provenance

- Active workflow mode: `phase1_mindoro_focus_pre_spill_2016_2023`
- Active baseline file: `config/phase1_baseline_selection.yaml`
- Historical window: `2016-01-01` to `2023-03-02`
- Focused validation box: `[118.751, 124.305, 10.620, 16.026]`
- Full strict accepted segments: `65`
- Ranked February-April subset: `19`
- Active selected recipe: `cmems_gfs`
- Historical four-recipe winner: `cmems_gfs`
- Tested family: `cmems_era5`, `cmems_gfs`, `hycom_era5`, `hycom_gfs`

## Ranking Snapshot

| Recipe | Mean NCS | Median NCS | Status |
| --- | ---: | ---: | --- |
| `cmems_gfs` | `4.5886` | `4.6305` | winner |
| `cmems_era5` | `4.6237` | `4.5916` | not selected |
| `hycom_gfs` | `4.7027` | `4.9263` | not selected |
| `hycom_era5` | `4.7561` | `5.0106` | not selected |

## Important Honesty Notes

- The focused rerun searched through early 2023, but its accepted registry does not include near-2023 accepted segments.
- Official `B1` promotes the focused historical winner directly, so the focused four-recipe winner `cmems_gfs` is also the official main-validation recipe.
- Phase 3B does not directly ingest drifters. It inherits a recipe selected by the separate focused drifter-based Phase 1 rerun.
- The stored March 13-14 `B1` science bundle keeps its original raw-generation history.

## Governance Result

- Mindoro `B1` provenance: focused Mindoro Phase 1 lane
- Broader `phase1_regional_2016_2022` lane: preserved regional reference/governance lane
- March-family archive rows: preserved provenance only
- PyGNOME on March 13-14: comparator-only support
