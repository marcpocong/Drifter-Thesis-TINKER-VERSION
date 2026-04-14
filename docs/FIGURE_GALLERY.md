# Figure Gallery

## Purpose

The repo now has three read-only figure/support layers:

- `output/trajectory_gallery/`: the raw technical gallery with standardized filenames and machine-readable metadata
- `output/trajectory_gallery_panel/`: the intermediate polished board pack for non-technical review
- `output/figure_package_publication/`: the canonical publication-grade presentation layer with paper-ready singles and defense boards
- `output/2016 Legacy Runs FINAL Figures/`: the curated prototype_2016 paper-facing export with per-case final PNGs plus explicit missing-figure diagnostics

It is built from existing outputs only:

- stored trajectory NetCDFs
- existing QA overlays
- existing comparison rasters
- existing Phase 4 shoreline and oil-budget support outputs
- existing `final_reproducibility_package` and `final_validation_package` metadata

It does not rerun expensive scientific branches.

## Safe Commands

```powershell
.\start.ps1 -Entry trajectory_gallery -NoPause
.\start.ps1 -Entry trajectory_gallery_panel -NoPause
.\start.ps1 -Entry figure_package_publication -NoPause
.\start.ps1 -Entry prototype_legacy_final_figures -NoPause
```

Equivalent direct command:

```bash
docker-compose exec -T -e PIPELINE_PHASE=trajectory_gallery_build pipeline python -m src
docker-compose exec -T -e PIPELINE_PHASE=trajectory_gallery_panel_polish pipeline python -m src
docker-compose exec -T -e PIPELINE_PHASE=figure_package_publication pipeline python -m src
docker-compose exec -T -e WORKFLOW_MODE=prototype_2016 -e PIPELINE_PHASE=prototype_pygnome_similarity_summary pipeline python -m src
docker-compose exec -T -e WORKFLOW_MODE=prototype_2016 -e PIPELINE_PHASE=prototype_legacy_final_figures pipeline python -m src
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

## Prototype 2016 Final Figure Outputs

- `output/2016 Legacy Runs FINAL Figures/final_figure_manifest.json`
- `output/2016 Legacy Runs FINAL Figures/missing_figures.csv`
- per-case folders such as `output/2016 Legacy Runs FINAL Figures/CASE_2016-09-01/`
- curated `.png` exports for drifter tracks, ensemble 24/48/72 plus consolidated 72 h, PyGNOME 24/48/72 plus consolidated 72 h, and PyGNOME-vs-ensemble 24/48/72 plus consolidated 72 h

## Raw Gallery Figure Groups

- A. Mindoro deterministic track/path visuals
- B. Mindoro ensemble sampled-member trajectories
- C. Mindoro centroid/corridor/hull views
- D. Mindoro March 13 -> March 14 primary-validation overlays
- E. Mindoro March 13 -> March 14 cross-model comparison maps
- F. DWH deterministic track/path visuals
- G. DWH ensemble p50/p90 overlays
- H. DWH OpenDrift vs PyGNOME comparison maps
- I. Mindoro Phase 4 oil-budget figures
- J. Mindoro Phase 4 shoreline-arrival / shoreline-segment impact figures

## Recommended First-Look Defense Figures

- A. Mindoro March 13 -> March 14 primary validation board
- B. Mindoro March 13 -> March 14 cross-model comparator board
- C. Mindoro legacy March 6 honesty / limitations board
- D. Mindoro trajectory board
- G. DWH deterministic forecast-vs-observation board
- H. DWH deterministic vs ensemble board
- I. DWH OpenDrift vs PyGNOME comparison board

Optional support/context figures:

- E. Mindoro Phase 4 oil-budget board
- F. Mindoro Phase 4 shoreline-arrival / shoreline-impact board
- Supporting honesty figure: Mindoro Phase 4 deferred-comparison note figure

These are the clearest figures for a main defense presentation. Use the publication package first, the panel gallery second, and the raw gallery only when the panel needs the technical archive behind a polished board. Keep the Phase 4 boards as support/context figures rather than as main-phase claims outside `prototype_2016`.

## Panel-Ready Board Families

- A. Mindoro March 13 -> March 14 primary validation board
- B. Mindoro March 13 -> March 14 cross-model comparator board
- C. Mindoro legacy March 6 honesty / limitations board
- D. Mindoro trajectory board
- G. DWH deterministic forecast-vs-observation board
- H. DWH deterministic vs ensemble board
- I. DWH OpenDrift vs PyGNOME comparison board
- J. DWH trajectory board

Available support/context board families:

- E. Mindoro Phase 4 oil-budget board
- F. Mindoro Phase 4 shoreline-arrival / shoreline-impact board

The polished board layer adds:

- figure titles and subtitles inside each figure
- a documented visual grammar and legend box
- locator insets, north arrows, and scale context where practical
- plain-language captions and talking points
- explicit main-defense recommendations in the panel registry

## Publication Package Families

- A. Mindoro March 13 -> March 14 promoted primary-validation singles plus board
- B. Mindoro March 13 -> March 14 cross-model comparison singles plus board
- C. Mindoro legacy March 6 honesty / limitations singles plus board
- D. Mindoro trajectory singles plus trajectory board
- E. Mindoro Phase 4 OpenDrift-only support/context oil-budget and shoreline-impact singles plus boards
- F. Mindoro Phase 4 deferred-comparison note figure built from the cross-model audit bundle
- G. DWH daily deterministic singles plus deterministic board
- H. DWH deterministic vs ensemble singles plus board
- I. DWH OpenDrift vs PyGNOME singles plus comparison board
- J. DWH trajectory singles plus trajectory board
- K. Prototype 2021 accepted-segment support-only OpenDrift vs PyGNOME singles plus per-case side-by-side boards
- L. Shared thesis study-box reference figure set built from stored box metadata only

The publication package adds:

- separate paper-ready single-image figures
- explicit side-by-side comparison boards
- plain-language captions and defense talking points
- a promoted March 13 -> March 14 Mindoro validation presentation lane for `Phase 3B Observation-Based Spatial Validation Using Public Mindoro Spill Extents`, with the shared March 12 imagery caveat kept explicit and the separate focused Mindoro Phase 1 drifter-based provenance note carried as provenance rather than rewritten run history
- an explicit publication-grade note figure explaining why Phase 4 OpenDrift-versus-PyGNOME comparison is still deferred
- support-only prototype family `K`, which now republishes the preferred accepted-segment 2021 deterministic OpenDrift-vs-PyGNOME forecast figures without elevating them into the default main-defense list
- a shared thesis study-box reference figure plus separate per-box geography panels that keep the focused Mindoro Phase 1 box, `mindoro_case_domain`, the scoring-grid display bounds, and the prototype_2016 first-code search box explicit in panel-ready WGS84 reference images with geography shown
- the canonical presentation layer for defense and manuscript use

## Curated B1 Final Output Export

- `output/Phase 3B March13-14 Final Output/README.md`
- `output/Phase 3B March13-14 Final Output/final_output_manifest.json`
- `output/Phase 3B March13-14 Final Output/publication/observations/*.png`
- `output/Phase 3B March13-14 Final Output/publication/opendrift_primary/*.png`
- `output/Phase 3B March13-14 Final Output/publication/comparator_pygnome/*.png`
- `output/Phase 3B March13-14 Final Output/scientific_source_pngs/opendrift_primary/*.png`
- `output/Phase 3B March13-14 Final Output/scientific_source_pngs/comparator_pygnome/*.png`
- `output/Phase 3B March13-14 Final Output/summary/opendrift_primary/*`
- `output/Phase 3B March13-14 Final Output/summary/comparator_pygnome/*`
- `output/Phase 3B March13-14 Final Output/manifests/*`

This export is the thesis-facing alias for the promoted B1 family. It reuses the publication package and the stored March 13 -> March 14 scientific source PNGs without renaming the canonical scientific directory, while keeping the primary OpenDrift claim separate from the comparator-only PyGNOME subgroup.

## Prototype Forecast Support Figures

The preferred prototype similarity phase now writes actual forecast figures under `output/prototype_2021_pygnome_similarity/figures/`:

- `2` fixed accepted-segment cases
- `24/48/72 h` deterministic OpenDrift singles
- `24/48/72 h` deterministic PyGNOME singles
- `1` side-by-side board per case with `24/48/72 h` rows and `OpenDrift | PyGNOME` columns
- a clean map-first layout with higher-density core and broader support envelopes derived from the stored deterministic normalized density rasters
- neutral/case-local context and a small locator inset on the support figures
- a provenance source-point star when the stored prototype case assets expose one defensibly

These figures remain:

- accepted-segment debug support only
- deterministic OpenDrift control versus deterministic PyGNOME only
- comparator-only, not truth
- support material rather than main thesis evidence

## Honesty Guardrails

- Sampled ensemble trajectory views use summary member-centroid paths or sampled particle tracks instead of plotting every particle.
- Existing QA figures are copied into the gallery with standardized names instead of being redrawn or relabeled.
- The polished board pack reorganizes existing evidence into clearer presentation boards, but it does not fabricate trajectories or relabel score products.
- The publication package redraws from the stored rasters, tracks, and Phase 4 tables, but it still does not fabricate trajectories or relabel score products.
- The publication package includes Phase 3 OpenDrift-versus-PyGNOME comparison boards, but it does not generate fake Phase 4 cross-model figures; instead it writes a deferred-comparison note figure grounded in `output/phase4_crossmodel_comparability_audit/`.
- Mindoro validation figures now report the promoted March 13 -> March 14 B1 row while explicitly inheriting recipe provenance from the separate focused 2016-2023 Mindoro drifter rerun; the Phase 4 figures remain support/context material outside `prototype_2016`.
- DWH figures remain reportable transfer-validation/support visuals, not a replacement for the Mindoro thesis case.

## Still Optional

- deeper interactive filtering/search across figure metadata inside the UI
- scientific run controls are still intentionally absent from the read-only UI
- alternate print-layout variants for every board
- DWH Phase 4 appendix-only figure set
