# Panel Review Guide

This guide is for panel members who want to open the software, inspect the stored thesis outputs, and check that the numbers shown by the software agree with the manuscript.

The important point is simple: panel mode is for review, not for rerunning the full science. By default it stays on the safe side and works from outputs that are already stored in the repository.

## 1. What the software does

The repository contains the final stored outputs for several parts of the study:

- Mindoro `B1`, which is the main public-observation validation row
- Mindoro `Track A`, which is a same-case OpenDrift vs PyGNOME comparison
- DWH, which is the external transfer-validation case
- Mindoro oil-type and shoreline outputs, which are kept as support/context
- final validation, figure, and reproducibility packages

For defense purposes, the fastest path is to inspect those stored results first.

## 2. What the panel can safely run

Start with:

```powershell
.\panel.ps1
```

or:

```powershell
.\start.ps1 -Panel
```

From there, the recommended panel actions are:

1. Open the read-only dashboard.
2. Check the stored paper numbers against the stored scorecards.
3. Rebuild the publication figures from stored outputs only.
4. Refresh the final validation package from stored outputs only.
5. Refresh the final reproducibility package and command documentation from stored outputs only.
6. Open the paper-to-output registry.

None of those are meant to launch a fresh scientific rerun.

## 3. What results should match the paper

The verification step checks stored values for:

- Mindoro `B1`: FSS, mean FSS, forecast and observed cells, distance diagnostics, IoU, and Dice
- Mindoro `Track A`: the OpenDrift and PyGNOME comparator summaries
- DWH: event-corridor FSS and IoU for `C1`, `C2 p50`, `C2 p90`, and `C3`
- Mindoro oil-type support values, where machine-readable outputs exist

The verification files are written only to:

- `output/panel_review_check/panel_results_match_check.csv`
- `output/panel_review_check/panel_results_match_check.json`
- `output/panel_review_check/panel_results_match_check.md`
- `output/panel_review_check/panel_review_manifest.json`

## 4. Which commands are read-only

These are the main read-only entry points:

```powershell
.\panel.ps1
.\start.ps1 -Panel
.\start.ps1 -List -NoPause
.\start.ps1 -Help -NoPause
```

The dashboard itself is also read-only:

```powershell
docker-compose exec pipeline python -m streamlit run ui/app.py --server.address 0.0.0.0 --server.port 8501
```

The package refresh options exposed in panel mode are packaging steps built from stored outputs:

```powershell
.\start.ps1 -Entry final_validation_package
.\start.ps1 -Entry phase5_sync
.\start.ps1 -Entry figure_package_publication
.\start.ps1 -Entry trajectory_gallery
.\start.ps1 -Entry trajectory_gallery_panel
```

## 5. Which commands are expensive scientific reruns

These belong to the advanced launcher and are not part of the default defense path:

```powershell
.\start.ps1 -Entry phase1_production_rerun
.\start.ps1 -Entry mindoro_phase3b_primary_public_validation
.\start.ps1 -Entry mindoro_reportable_core
.\start.ps1 -Entry dwh_reportable_bundle
.\start.ps1 -Entry mindoro_appendix_sensitivity_bundle
```

Those commands can rerun major workflow phases and are better treated as audit or researcher commands.

## 6. Why PyGNOME is comparator-only

PyGNOME is included because it helps answer a reasonable panel question: how does the OpenDrift result compare with another model on the same case?

It is not treated as truth. In both Mindoro `Track A` and DWH `C3`, PyGNOME is there for comparison only. The observational reference stays the same, and the main OpenDrift lanes remain the thesis-facing results.

## 7. Why B1 is the only main Mindoro validation row

`B1` is the promoted March 13 -> March 14 `R1_previous` row, and it is the one carried into the main thesis-facing argument.

The reason for that boundary is straightforward:

- `B1` is the row used for the main Mindoro validation claim
- the March 13 -> March 14 `R0` branch is preserved for archive and provenance
- the other March-family rows remain useful background, but they are not replacements for `B1`

So if a panelist asks, “Which Mindoro row should I compare with the paper first?”, the answer is `B1`.

## 8. Why the March 13-14 B1 pair has a shared-imagery caveat

The March 13 and March 14 NOAA/NESDIS products both cite March 12 WorldView-3 imagery.

That does not make the row useless, but it does place a limit on how it should be described. It is fair to discuss it as a reinitialization-based validation pair. It is not fair to present it as a fully independent day-to-day validation pair. The panel mode keeps that caveat visible because it matters to an honest defense.

## 9. Why the 5,000-element personal experiment is excluded

The 5,000-element run is not part of the thesis-facing result set, so it is left out of the default panel menu on purpose.

The goal of panel mode is to keep the review path clean:

- main thesis outputs first
- support and comparator outputs second
- experimental work outside the default path

That way a panelist is not forced to sort through personal trials before reaching the stored defense materials.

## 10. How to open the full advanced launcher

From panel mode, choose:

- `A. Open full research launcher`

Or run:

```powershell
.\start.ps1
```

## Short version

If you only want the practical defense path:

1. Run `.\panel.ps1`.
2. Open the dashboard.
3. Run the paper-number check.
4. Use the registry if you want to trace a table or figure back to its stored source.
5. Open the full launcher only if you intentionally want the research-side commands.
