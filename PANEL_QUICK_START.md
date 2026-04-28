# Panel / Defense Quick Start

If you are reviewing the software for the defense, start here:

```powershell
.\panel.ps1
```

or:

```powershell
.\start.ps1 -Panel
```

This opens the panel menu instead of the full research launcher.

## What panel mode is for

Panel mode is meant to help a reviewer do the practical checks first:

1. open the read-only dashboard
2. check that the stored numbers match the manuscript
3. rebuild figures from stored outputs only
4. refresh the final validation package from stored outputs only
5. refresh the reproducibility package and command docs from stored outputs only
6. open the paper-to-output registry

## What panel mode does not do by default

- It does not rerun the expensive science.
- It does not pull experimental or sensitivity-only outputs into the thesis-facing path.
- It does not include the 5,000-element personal experiment in the default review menu.

## Keep these boundaries in mind

- `B1` is the main Mindoro validation row.
- `Track A` is comparator support, not a second main result.
- `PyGNOME` is comparator-only.
- `DWH` is an external transfer-validation case, not a Mindoro recalibration.
- Oil-type and shoreline results are support/context only.

## If you want the full launcher

Choose `Advanced` from the panel menu, or run:

```powershell
.\start.ps1
```
