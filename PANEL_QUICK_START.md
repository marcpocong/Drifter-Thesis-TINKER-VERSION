# Panel / Defense Quick Start

Start here:

```powershell
.\panel.ps1
```

or:

```powershell
.\start.ps1 -Panel
```

This opens the panel-safe review menu instead of the full research launcher. Panel mode is review-only / stored-output-only unless you intentionally switch to researcher or audit reruns.

## What Panel Mode Is For

1. Opening the read-only dashboard
2. Verifying stored manuscript numbers against stored scorecards
3. Rebuilding publication figures from stored outputs only
4. Inspecting the focused Phase 1 drifter provenance behind the selected `B1` recipe
5. Refreshing the final validation package from stored outputs only
6. Refreshing the docs/reproducibility package from stored outputs only
7. Opening the paper-to-output registry

## What Panel Mode Does Not Do By Default

- It does not rerun expensive scientific workflows.
- It does not promote archive, appendix, or personal-experiment outputs into defended evidence.
- It does not treat the full launcher as the default defense path.

## Draft 22 / Final Manuscript Evidence Boundaries

1. Focused Mindoro Phase 1 provenance = historical drifter-based transport validation and recipe selection.
2. Phase 2 = standardized deterministic and 50-member machine-readable forecast products.
3. Mindoro `B1` = March 13-14 `R1_previous` primary public-observation validation row and the only main-text primary Philippine / Mindoro validation claim.
4. Mindoro `Track A` = same-case OpenDrift versus PyGNOME comparator-only support; never the observational scoring reference.
5. `DWH` = separate external transfer validation lane; not Mindoro recalibration.
6. Mindoro oil-type / shoreline outputs = support/context only.
7. `prototype_2016` = legacy/archive support only.
8. Publication package, figure package, and UI = read-only presentation/governance surfaces built from stored outputs only.

## Compact Result Values Checklist

- Focused Phase 1 provenance: `cmems_gfs` winner; mean NCS `4.5886`; median NCS `4.6305`; full strict accepted segments `65`; ranked February-April subset `19`
- Mindoro `B1`: mean FSS `0.1075`; FSS `0.0000 / 0.0441 / 0.1371 / 0.2490`; `R1_previous` forecast cells `5`; observed cells `22`; nearest distance `1414.21 m`; centroid distance `7358.16 m`; `IoU = 0.0`; `Dice = 0.0`
- Mindoro `Track A` OpenDrift: forecast cells `5`; nearest distance `1414.21 m`; mean FSS `0.1075`
- Mindoro `Track A` PyGNOME comparator-only support: forecast cells `6`; nearest distance `6082.76 m`; mean FSS `0.0061`
- DWH event corridor: `C1 = 0.5568`; `C2 p50 = 0.5389`; `C2 p90 = 0.4966`; `C3 PyGNOME comparator = 0.3612`
- Mindoro oil-type / shoreline support: light oil `0.02%`, `4 h`, `11`, QC pass; fixed-base medium-heavy proxy `0.61%`, `4 h`, `10`, QC flagged; heavier oil `0.63%`, `4 h`, `11`, QC pass

## Important Caveats

- `B1` is promoted because `R1_previous` survives to the target date and is scoreable, not because it is an exact-grid match.
- March 13-14 is a reinitialization-based public-observation validation check.
- Both public products cite the same March 12 WorldView-3 imagery provenance, so do not call the pair independent day-to-day validation.
- `Track A` and every PyGNOME branch remain comparator-only support.

## Inspect Drifter Provenance Behind `B1`

Use either:

```powershell
.\start.ps1 -Entry b1_drifter_context_panel
```

or panel option `7`.

That page stays stored-output-only. It explains the historical focused Phase 1 provenance behind the selected recipe without turning drifters into the March 13-14 public-observation truth mask.

## If You Intentionally Need The Full Launcher

Choose `Advanced` from the panel menu, or run:

```powershell
.\start.ps1
```

Use the full launcher only when you intentionally want researcher or audit reruns.
