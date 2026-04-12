# Architecture

## Workflow Separation

This repo intentionally keeps four stories separate:

- historical/regional transport validation
- spill-case forecast generation and validation
- oil-type and shoreline interpretation
- launcher/docs/reproducibility packaging

That separation is now explicit in both the code and the launcher. The project no longer relies on a single stale "Mindoro full chain" entrypoint.

## Lane Model

- `prototype_2016`: backward-compatible legacy/debug lane only, now with an explicit six-recipe prototype debug family, a padded forcing-prep window that preserves the legacy `+/- 3 h` ensemble jitter, and a consolidated transport-only OpenDrift-vs-PyGNOME similarity summary built from the three deterministic prototype benchmark cases
- `prototype_2021`: preferred accepted-segment debug/demo lane built from the two fixed 2021 strict-gate drifter segments, restricted to the official Phase 1 recipe family, and intentionally scoped to the transport-core bundle only
- `mindoro_retro_2023`: main Philippine spill-case lane
- `dwh_retro_2010`: external rich-data transfer-validation lane
- `phase1_regional_2016_2022`: dedicated historical/regional Phase 1 scientific rerun lane

Neither prototype lane is the final Phase 1 scientific evidence base. `prototype_2021` is the preferred debug/demo path, while `prototype_2016` is retained so older transport-validation logic can still be debugged and regression-checked.

## Phase 1 Boundary

Target architecture:

- historical window 2016-2022
- fixed regional validation box
- drogued-only core pool
- non-overlapping 72 h segments
- HYCOM/CMEMS crossed with GFS/ERA5 recipe family
- explicit loading-audit hard-fail behavior
- frozen baseline artifact for downstream spill cases

Current local state:

- the dedicated `phase1_production_rerun` lane now materializes the missing 2016-2022 accepted/rejected corpus, loading audit, metrics, summary, ranking, and staged candidate baseline under `output/phase1_production_rerun/`
- the regional rerun is scientific-only by default and does not auto-run `phase1_audit` or `phase5_sync`
- `config/phase1_baseline_selection.yaml` is intentionally not overwritten; downstream spill-case use of the new baseline remains a manual override/promotion decision

## Phase 2 Boundary

Target architecture:

- deterministic control separate from the ensemble
- fixed canonical product names and unchanged `mask_p50` / `mask_p90` semantics
- 50-member ensemble design
- date-composite logic
- same-grid discipline
- loading audits, manifests, provenance, and honesty fields

Current local state:

- the implemented Phase 2 workflow is scientifically usable
- it is not yet scientifically frozen
- legacy `*_ncep` drift is still present in config/runtime space for backward compatibility
- the dedicated Phase 1 production lane now evaluates only `cmems_era5`, `cmems_gfs`, `hycom_era5`, and `hycom_gfs`
- the spill-case lane still needs deliberate candidate-baseline adoption before the new Phase 1 evidence becomes its default upstream baseline

## Phase 3 Boundary

Mindoro and DWH serve different roles:

- Mindoro Phase 3A/3B establishes thesis-case comparator, strict stress-test, and broader-support interpretation
- DWH Phase 3C provides external rich-data transfer validation

These should not be collapsed into a single validation claim. Mindoro remains the main Philippine case; DWH remains the external rich-data transfer benchmark.

Unlike Mindoro, DWH Phase 3C does not use a Phase 1 drifter-selected forcing recipe. It uses a frozen historical forcing stack chosen by a scientific-readiness gate: the first complete real current+wind+wave stack for the DWH May 20-23, 2010 window that is not smoke-only, spans the required window, exposes the required variables with usable metadata, opens cleanly in the OpenDrift reader, and passes a small end-to-end reader-check forecast. In the current repo state, that frozen DWH stack is HYCOM GOFS 3.1 currents plus ERA5 winds plus CMEMS wave/Stokes.

## Phase 4 Boundary

Phase 4 is now a real workflow, not a placeholder shell:

- dedicated scenario registry in `config/phase4_oil_scenarios.csv`
- reuse of the current reportable transport branch rather than rerunning scoring logic
- shoreline assignment tied to the existing canonical shoreline segments
- separate output bundle under `output/phase4/CASE_MINDORO_RETRO_2023/`

Phase 4 is reportable now for Mindoro, but it still inherits upstream provisional status from the unfinished Phase 1/2 freeze story.
The repo's existing PyGNOME branches remain Phase 3-style transport comparators, so a separate read-only `phase4_crossmodel_comparability_audit` is now the guardrail that decides whether any OpenDrift-versus-PyGNOME Phase 4 comparison is scientifically defensible. In the current repo state, that audit is deferred rather than promoted to a result because matched PyGNOME fate-and-shoreline outputs do not yet exist.
This is also why the prototype/debug drifter lanes should be framed as Phase 1/2/3A support only rather than as proof that `Phase 4 = Oil-Type Fate and Shoreline Impact Analysis` has been validated there.

## Phase 5 Boundary

Phase 5 is the project synchronization layer:

- launcher/menu rationalization
- docs synchronization
- reproducibility/package indexes
- honest cross-phase status registry
- read-only local dashboard over the packaged outputs and figure layers

It is intentionally non-scientific in scope. It reuses existing scientific artifacts rather than recomputing them.

## Launcher Architecture

The launcher source of truth is `config/launcher_matrix.json`. It defines:

- category separation
- workflow mode
- service mix (`pipeline` or `gnome`)
- rerun cost
- safe read-only defaults
- step sequence
- optional future work that is still not implemented

`start.ps1` now reads that matrix directly. This means:

- current menu options match the actual repo state
- prototype mode remains available without being misrepresented
- read-only utilities are visible and easy to run
- non-existent UI features are not advertised as if they already exist, while the implemented raw gallery, panel gallery, and publication-grade figure package are exposed honestly as separate safe utilities
- the expensive `phase1_production_rerun` scientific entry exists as a separate historical/regional workflow instead of being hidden inside the Mindoro spill-case bundle

## Packaging Architecture

The repo now has two different packaging layers:

- `output/final_validation_package/`: frozen thesis validation package built from completed scientific outputs
- `output/final_reproducibility_package/`: Phase 5 synchronization layer that indexes software versions, cases, configs, manifests, outputs, logs, and honest phase status
- `output/prototype_2021_pygnome_similarity/`: read-only preferred accepted-segment debug support package built from the fixed 2021 deterministic OpenDrift-vs-PyGNOME cases
- `output/prototype_2016_pygnome_similarity/`: read-only legacy/debug transport benchmark summary package that consolidates the three deterministic prototype OpenDrift-vs-PyGNOME cases
- `output/trajectory_gallery/`: read-only technical gallery built from existing trajectories, rasters, overlays, and Phase 4 artifacts
- `output/trajectory_gallery_panel/`: read-only polished panel-ready board pack built from the raw gallery and the same stored source artifacts
- `output/figure_package_publication/`: read-only canonical publication-grade package built from stored rasters, tracks, manifests, and Phase 4 tables for defense and paper use
- `ui/`: read-only local dashboard that consumes those packages and archives without becoming a scientific rerun surface

Phase 5 reuses the final validation package rather than replacing it. The validation package remains the thesis summary bundle; the reproducibility package is the reproducibility/indexing layer around the current repo state; the prototype PyGNOME similarity package is a legacy/debug comparator summary only; the raw trajectory gallery is the technical figure archive; the polished panel gallery is the intermediate non-technical review layer; the publication figure package is the canonical presentation layer for defense slides and paper-ready single figures; and the UI is the read-only local explorer over that same packaged state.
