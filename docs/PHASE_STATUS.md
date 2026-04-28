# Phase Status

## Current Project Verdict

- Focused Mindoro Phase 1 provenance is finalized for the defended Mindoro `B1` recipe story.
- Phase 2 standardized deterministic and 50-member forecast generation is scientifically usable and stored, but not described as universally frozen.
- Mindoro `B1` is the only main-text primary Philippine / Mindoro validation claim.
- `Track A` and PyGNOME branches are comparator-only support.
- DWH is a separate external transfer-validation lane.
- Mindoro oil-type and shoreline outputs are support/context only.
- `prototype_2016` is legacy/archive support only.
- Publication packages, figure packages, launcher audits, and the UI are read-only presentation/governance surfaces built from stored outputs.

## Current Manuscript Evidence Order

1. Focused Mindoro Phase 1 provenance
2. Phase 2 standardized forecast products
3. Mindoro `B1` primary public-observation validation
4. Mindoro `Track A` comparator-only support
5. DWH external transfer validation
6. Mindoro oil-type and shoreline support/context
7. `prototype_2016` legacy/archive support
8. Reproducibility / governance / read-only package layer

## Focused Mindoro Phase 1 Provenance

- Workflow mode: `phase1_mindoro_focus_pre_spill_2016_2023`
- Historical window: `2016-01-01` to `2023-03-02`
- Validation box: `[118.751, 124.305, 10.620, 16.026]`
- Full strict accepted segments: `65`
- Ranked February-April subset: `19`
- Selected recipe: `cmems_gfs`
- Ranking summary:

| Recipe | Mean NCS | Median NCS | Status |
| --- | ---: | ---: | --- |
| `cmems_gfs` | `4.5886` | `4.6305` | winner |
| `cmems_era5` | `4.6237` | `4.5916` | not selected |
| `hycom_gfs` | `4.7027` | `4.9263` | not selected |
| `hycom_era5` | `4.7561` | `5.0106` | not selected |

- Governance note: the broader `phase1_regional_2016_2022` lane remains separate reference/governance context and is not the main Mindoro validation claim.

## Mindoro `B1`

- Promoted row: March 13-14 `R1_previous`
- Main interpretation: primary public-observation validation row
- FSS `1 / 3 / 5 / 10 km`: `0.0000 / 0.0441 / 0.1371 / 0.2490`
- Mean FSS: `0.1075`
- `R0` did not reach the target date; forecast cells `0`; observed cells `22`
- `R1_previous` forecast cells `5`; observed cells `22`; nearest distance `1414.21 m`; centroid distance `7358.16 m`
- `IoU = 0.0`; `Dice = 0.0`
- Guardrail: `R1_previous` is promoted because it survives and is scoreable, not because it is an exact-grid match
- Caveat: March 13-14 is a reinitialization-based public-observation validation pair; both products cite the same March 12 WorldView-3 imagery provenance

## Mindoro `Track A`

- Role: same-case OpenDrift-versus-PyGNOME comparator-only support
- OpenDrift `R1_previous`: forecast cells `5`; nearest distance `1414.21 m`; mean FSS `0.1075`
- OpenDrift `R0`: forecast cells `0`; mean FSS `0.0000`
- PyGNOME deterministic comparator-only support: forecast cells `6`; nearest distance `6082.76 m`; mean FSS `0.0061`
- Guardrail: do not treat PyGNOME as the observational scoring reference or as an independent validation lane

## DWH

- Case ID: `CASE_DWH_RETRO_2010_72H`
- Role: external transfer validation only
- Scientific forcing stack: `HYCOM GOFS 3.1 currents + ERA5 winds + CMEMS wave/Stokes`
- `C1` deterministic event-corridor mean FSS: `0.5568`
- `C2 p50` event-corridor mean FSS: `0.5389`
- `C2 p90` event-corridor mean FSS: `0.4966`
- `C3` PyGNOME comparator event-corridor mean FSS: `0.3612`
- Guardrail: DWH is not Mindoro recalibration and not a second local Phase 1

## Mindoro Oil-Type And Shoreline Support

| Scenario | Final beached fraction | First arrival | Impacted segments | QC |
| --- | ---: | ---: | ---: | --- |
| light oil | `0.02%` | `4 h` | `11` | pass |
| fixed-base medium-heavy proxy | `0.61%` | `4 h` | `10` | flagged |
| heavier oil | `0.63%` | `4 h` | `11` | pass |

Guardrail: these are support/context values only, not a second primary validation lane.

## Launcher And Surface Status

- Panel mode remains the defense-safe default path.
- The full launcher remains the researcher/audit path.
- `b1_drifter_context_panel`, `phase1_audit`, `phase2_audit`, `final_validation_package`, `phase5_sync`, `trajectory_gallery`, `trajectory_gallery_panel`, and `figure_package_publication` are read-only / packaging-only surfaces.
- The UI, publication package, and figure package organize stored outputs only and do not create new scientific results.
- `prototype_2016` is legacy/archive support only; some internal package names may still contain Phase 4/Phase 5 labels, but those are not primary defended evidence.

## Final Guardrails

- Keep `B1` as the only main-text primary Mindoro validation row.
- Keep the March 13-14 shared-imagery caveat explicit.
- Keep `Track A` and PyGNOME comparator-only.
- Keep DWH external only.
- Keep Mindoro oil-type/shoreline support-only.
- Keep archive/support rows out of the main evidence claim.
- Keep the UI and publication surfaces read-only.

## Status Stamp

- Last updated: `2026-04-28`
- Current sync state: aligned to Draft 22 / final manuscript evidence structure and stored-output review package
- Biggest remaining scientific follow-up: broader `2016-2022` regional/reference Phase 1 lane remains separate from finalized Mindoro-specific B1 provenance and is not the main Mindoro validation claim
