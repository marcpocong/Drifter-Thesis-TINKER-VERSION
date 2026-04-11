# Figure Gallery

## Purpose

The repo now has three read-only figure layers:

- `output/trajectory_gallery/`: the raw technical gallery with standardized filenames and machine-readable metadata
- `output/trajectory_gallery_panel/`: the intermediate polished board pack for non-technical review
- `output/figure_package_publication/`: the canonical publication-grade presentation layer with paper-ready singles and defense boards

It is built from existing outputs only:

- stored trajectory NetCDFs
- existing QA overlays
- existing comparison rasters
- existing Phase 4 shoreline and oil-budget outputs
- existing `final_reproducibility_package` and `final_validation_package` metadata

It does not rerun expensive scientific branches.

## Safe Commands

```powershell
.\start.ps1 -Entry trajectory_gallery -NoPause
.\start.ps1 -Entry trajectory_gallery_panel -NoPause
.\start.ps1 -Entry figure_package_publication -NoPause
```

Equivalent direct command:

```bash
docker-compose exec -T -e PIPELINE_PHASE=trajectory_gallery_build pipeline python -m src
docker-compose exec -T -e PIPELINE_PHASE=trajectory_gallery_panel_polish pipeline python -m src
docker-compose exec -T -e PIPELINE_PHASE=figure_package_publication pipeline python -m src
```

## Raw Technical Gallery Outputs

- `output/trajectory_gallery/trajectory_gallery_manifest.json`
- `output/trajectory_gallery/trajectory_gallery_index.csv`
- `output/trajectory_gallery/figures_index.md`
- standardized `.png` figures with case/phase/model/run/date/scenario tokens in the filename

## Polished Panel Gallery Outputs

- `output/trajectory_gallery_panel/panel_figure_manifest.json`
- `output/trajectory_gallery_panel/panel_figure_registry.csv`
- `output/trajectory_gallery_panel/panel_figure_captions.md`
- `output/trajectory_gallery_panel/panel_figure_talking_points.md`
- polished `.png` boards with case/phase/model/run/date/scenario/variant tokens in the filename

## Publication Figure Package Outputs

- `output/figure_package_publication/publication_figure_manifest.json`
- `output/figure_package_publication/publication_figure_registry.csv`
- `output/figure_package_publication/publication_figure_captions.md`
- `output/figure_package_publication/publication_figure_talking_points.md`
- publication-grade `.png` single figures and side-by-side boards with case/phase/model/run/date/scenario/view/variant tokens in the filename

## Raw Gallery Figure Groups

- A. Mindoro deterministic track/path visuals
- B. Mindoro ensemble sampled-member trajectories
- C. Mindoro centroid/corridor/hull views
- D. Mindoro forecast-vs-observation overlays
- E. Mindoro OpenDrift vs PyGNOME comparison maps
- F. DWH deterministic track/path visuals
- G. DWH ensemble p50/p90 overlays
- H. DWH OpenDrift vs PyGNOME comparison maps
- I. Mindoro Phase 4 oil-budget figures
- J. Mindoro Phase 4 shoreline-arrival / shoreline-segment impact figures

## Recommended First-Look Defense Figures

- A. Mindoro strict March 6 forecast-vs-observation board
- C. Mindoro OpenDrift vs PyGNOME comparison board
- D. Mindoro trajectory board
- E. Mindoro Phase 4 oil-budget board
- F. Mindoro Phase 4 shoreline-arrival / shoreline-impact board
- G. DWH deterministic forecast-vs-observation board
- H. DWH deterministic vs ensemble board
- I. DWH OpenDrift vs PyGNOME comparison board
- Supporting honesty figure: Mindoro Phase 4 deferred-comparison note figure

These are the clearest figures for a main defense presentation. Use the publication package first, the panel gallery second, and the raw gallery only when the panel needs the technical archive behind a polished board.

## Panel-Ready Board Families

- A. Mindoro strict March 6 forecast-vs-observation board
- B. Mindoro March 4-6 event-corridor board
- C. Mindoro OpenDrift vs PyGNOME comparison board
- D. Mindoro trajectory board
- E. Mindoro Phase 4 oil-budget board
- F. Mindoro Phase 4 shoreline-arrival / shoreline-impact board
- G. DWH deterministic forecast-vs-observation board
- H. DWH deterministic vs ensemble board
- I. DWH OpenDrift vs PyGNOME comparison board
- J. DWH trajectory board

The polished board layer adds:

- figure titles and subtitles inside each figure
- a documented visual grammar and legend box
- locator insets, north arrows, and scale context where practical
- plain-language captions and talking points
- explicit main-defense recommendations in the panel registry

## Publication Package Families

- A. Mindoro strict March 6 singles plus board, including locator, zoom, and forced close-up variants
- B. Mindoro March 4-6 event-corridor singles plus board, including locator and close-up variants
- C. Mindoro OpenDrift vs PyGNOME publication singles plus comparison board
- D. Mindoro trajectory singles plus trajectory board
- E. Mindoro Phase 4 OpenDrift-only oil-budget and shoreline-impact singles plus boards
- F. Mindoro Phase 4 deferred-comparison note figure built from the cross-model audit bundle
- G. DWH daily deterministic singles plus deterministic board
- H. DWH deterministic vs ensemble singles plus board
- I. DWH OpenDrift vs PyGNOME singles plus comparison board
- J. DWH trajectory singles plus trajectory board

The publication package adds:

- separate paper-ready single-image figures
- explicit side-by-side comparison boards
- plain-language captions and defense talking points
- forced close-up crops where the observed target would otherwise be unreadable
- an explicit publication-grade note figure explaining why Phase 4 OpenDrift-versus-PyGNOME comparison is still deferred
- the canonical presentation layer for defense and manuscript use

## Honesty Guardrails

- Sampled ensemble trajectory views use summary member-centroid paths or sampled particle tracks instead of plotting every particle.
- Existing QA figures are copied into the gallery with standardized names instead of being redrawn or relabeled.
- The polished board pack reorganizes existing evidence into clearer presentation boards, but it does not fabricate trajectories or relabel score products.
- The publication package redraws from the stored rasters, tracks, and Phase 4 tables, but it still does not fabricate trajectories or relabel score products.
- The publication package includes Phase 3 OpenDrift-versus-PyGNOME comparison boards, but it does not generate fake Phase 4 cross-model figures; instead it writes a deferred-comparison note figure grounded in `output/phase4_crossmodel_comparability_audit/`.
- Mindoro transport and Phase 4 figures remain inherited-provisional from the unfinished Phase 1/2 frozen-baseline story.
- DWH figures remain reportable transfer-validation/support visuals, not a replacement for the Mindoro thesis case.

## Still Optional

- deeper interactive filtering/search across figure metadata inside the UI
- scientific run controls are still intentionally absent from the read-only UI
- alternate print-layout variants for every board
- DWH Phase 4 appendix-only figure set
