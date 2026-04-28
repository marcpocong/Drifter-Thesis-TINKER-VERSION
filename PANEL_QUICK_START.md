# Panel / Defense Quick Start

This panel mode verifies the stored thesis-facing outputs against the manuscript. It does not rerun expensive scientific simulations by default. Full scientific reruns remain available through the advanced launcher for audit purposes.

## Start here

For thesis defense or panel inspection, run:

```powershell
.\panel.ps1
```

or:

```powershell
.\start.ps1 -Panel
```

## Recommended panel checks

1. Open the read-only dashboard.
2. Verify paper numbers against stored scorecards.
3. Rebuild publication figures from stored outputs only.
4. Refresh the final validation package from stored outputs only.
5. Refresh the final reproducibility package and command documentation from stored outputs only.
6. Show the paper-to-output registry.

## What is safe in panel mode

- Opening the dashboard is read-only.
- Paper-result verification writes only to `output/panel_review_check/`.
- Figure, validation, and reproducibility refresh options are packaging-only commands built from stored outputs.

## What panel mode does not do by default

- It does not rerun expensive scientific phases.
- It does not promote experimental or sensitivity-only outputs into thesis-facing results.
- It does not include the 5,000-element personal experiment in the default panel menu.

## Key interpretation guardrails

- `B1` is the only main Mindoro validation row.
- `Track A` is comparator support only.
- `PyGNOME` is never observational truth in the thesis-facing panel story.
- `DWH` is external transfer validation, not Mindoro recalibration.
- `Oil-type` and `shoreline` outputs are support/context only.

## Advanced launcher

If a panelist or auditor wants the full research launcher, use the `Advanced` option from panel mode or run:

```powershell
.\start.ps1
```
