# Phase Status

## Current Project Verdict

- Phase 1: Mindoro-specific recipe provenance is now finalized through the separate focused `2016-2023` drifter rerun selecting `cmems_era5`; the broader `2016-2022` regional rerun remains preserved as a reference/governance lane and currently selects `cmems_gfs`.
- Phase 2: scientifically usable, not frozen.
- Mindoro Phase 3: validation-focused and reportable; `Phase 3B Observation-Based Spatial Validation Using Public Mindoro Spill Extents` is now carried by the March 13 -> March 14 B1 row, March 6 remains the preserved sparse-reference honesty case, the promotion is tracked by amendment rather than by rewriting the frozen March 3 -> March 6 case file, and B1 now inherits its recipe provenance from the separate focused `2016-2023` Mindoro drifter rerun without claiming direct drifter ingestion inside Phase 3B itself.
- DWH Phase 3C: frozen validation-only external rich-data transfer case kept separate from drifter-calibration governance.
- Mindoro `phase4_oiltype_and_shoreline`: implemented as a support/context bundle outside the main thesis phase count; thesis-facing `Phase 4` labeling is reserved for `prototype_2016`.
- Phase 4 cross-model comparison: deferred; current PyGNOME branches remain comparator-only for transport/spatial work rather than matched Phase 4 fate-and-shoreline outputs.
- Repo sync, galleries, publication packaging, and dashboard layers: read-only support/explorer surfaces outside the main thesis phase count; thesis-facing `Phase 5` labeling is reserved for `prototype_2016`.
- Trajectory gallery: read-only technical figure set built from existing outputs only.
- Trajectory gallery panel pack: read-only polished board layer for non-technical review, built from existing outputs only.
- Publication figure package: canonical publication-grade defense/paper figure layer built from existing outputs only, with honest Phase 3 cross-model boards and a Phase 4 deferred-comparison note figure.
- Read-only local dashboard: implemented support/explorer layer over the publication package, panel/raw figure archives, reproducibility indexes, and the Phase 4 cross-model audit.

## Phase 1

- Plain-language status: the focused `2016-2023` Mindoro rerun now provides the active spill-case recipe provenance in `config/phase1_baseline_selection.yaml`, while the dedicated `2016-2022` historical/regional production rerun is preserved as a broader reference/governance lane and still stages its own regional candidate artifact.
- Scientifically reportable: `true`
- Scientifically frozen: `candidate_staged_but_not_promoted`
- Inherited provisional: `false`
- Official Phase 1 audit box/window: `119.5-124.5E / 11.5-16.5N`, `2016-2022`
- Forcing-outage policy: strict/reportable by default. If a provider outage removes part of the official recipe family, the dedicated scientific lane now fails hard unless you explicitly set `FORCING_OUTAGE_POLICY=continue_degraded`.
- Forcing-provider acquisition is also fail-fast now: each forcing source gets a shared `FORCING_SOURCE_BUDGET_SECONDS` wall-clock budget with a default of `300` seconds, while drifter truth and ArcGIS/observation truth remain strict inputs outside that timeout policy.
- Biggest remaining follow-up: archived NOAA/NCEI GFS access is still blocking a full four-recipe rerun of the focused Mindoro provenance lane, so the active Mindoro story remains explicitly outage-constrained to the ERA5-backed family for now.
- Separate Mindoro-focused note: the `phase1_mindoro_focus_pre_spill_2016_2023` rerun is now the active Mindoro-specific recipe-provenance lane for B1. It searched through early 2023, but its accepted registry does not include near-2023 accepted segments.

What is already in place:

- dedicated `WORKFLOW_MODE=phase1_regional_2016_2022` and `PIPELINE_PHASE=phase1_production_rerun`
- consolidated `output/phase1_production_rerun/phase1_drifter_registry.csv` with accepted/rejected status, reject reasons, drogue fields, non-overlap status, and regional-box status
- accepted/rejected segment registries, loading audit, segment metrics, recipe summary, recipe ranking, run manifest, and staged candidate baseline artifact under `output/phase1_production_rerun/`
- strict gate implementation for drogued-only, full-duration, continuous, non-overlapping, in-box 72 h segments
- preferred `prototype_2021` debug lane frozen from the two accepted 2021 strict-gate segments
- preserved `prototype_2016` workflow for debugging/regression
- degraded forcing continuation is available only for forcing-provider outages in appendix/legacy/experimental lanes; drifter truth and ArcGIS/observation truth remain hard requirements
- explicit read-only audit outputs under `output/phase1_finalization_audit/`
- structural separation between historical/regional validation and spill-case validation

What still remains:

- deliberate downstream trial runs using `BASELINE_SELECTION_PATH=output/phase1_production_rerun/phase1_baseline_selection_candidate.yaml`
- explicit promotion only if you decide to replace `config/phase1_baseline_selection.yaml`
- optional read-only follow-up refreshes through `phase1_audit` and `phase5_sync`

## Phase 2

- Plain-language status: scientifically usable right now, but not scientifically frozen.
- Scientifically reportable: `true`
- Scientifically frozen: `false`
- Inherited provisional: `true`
- Biggest blocker: the staged Phase 1 candidate baseline has not yet been promoted into the default spill-case baseline file (`config/phase1_baseline_selection.yaml`, still sourced from `2016-09-01`, `2016-09-06`, and `2016-09-17`), and local official recipe-family support for the spill-case lane is still partial.

What is already in place:

- deterministic control kept separate from the ensemble
- canonical `prob_presence`, `mask_p50`, and `mask_p90` semantics
- fixed 50-member ensemble design
- date-composite logic without silently relabeling products
- common-grid discipline and machine-readable loading audits
- read-only audit outputs under `output/phase2_finalization_audit/`

What remains provisional:

- the default spill-case baseline file still remains the older canonical artifact until you manually promote or override it, and that canonical file still cites the preserved `2016-09-01`, `2016-09-06`, and `2016-09-17` prototype rankings as its evidence base
- legacy `*_ncep` recipe-family drift still exists in config/runtime space, even though the dedicated Phase 1 production lane now evaluates only the Chapter 3 official family
- no local `gfs_wind.nc` is present for the current spill-case lane, so the full official GFS-capable family is not yet locally available there by default
- existing stored Phase 2 manifests predate the new candidate-baseline story and were not regenerated by default
- `prototype_2021` remains a debug/demo lane rather than the final Chapter 3 regional evidence base

## Legacy Prototype 2016 Support Lane

- Plain-language status: preserved legacy support lane only, reframed thesis-facing as `Phase 1 -> Phase 2 -> Phase 3A -> Phase 4 -> Phase 5`.
- Reportable: `false` as final evidence, `true` as legacy support/comparator context.
- Methodological role: this preserved three-date 2016 lane captures the earliest prototype stage of the study, when the ingestion-and-validation pipeline was first exercised on 2016 drifter records in the Palawan-side western Philippine context before the study widened toward the broader west-coast Palawan/Mindoro context.
- Guardrail: there is no thesis-facing `Phase 3B` or `Phase 3C` for `prototype_2016`.
- Phase 3A package scope: deterministic plus support-only `p50`/`p90` OpenDrift tracks against deterministic PyGNOME.
- Comparator interpretation: the Phase 3A OpenDrift-versus-deterministic-PyGNOME check is support-only. A non-zero fraction skill score means the ensemble footprint was not completely disjoint from the deterministic PyGNOME forecast; it does not make PyGNOME truth or elevate this lane into final Chapter 3 evidence.
- Phase 4 legacy weathering scope: drifter-of-record point seeding, preserved as honesty-support/debug context rather than as official Phase 4 validation.
- Release-origin note: the authoritative `prototype_2016` release lat/lon/time comes from the selected drifter-of-record start in `data/drifters/CASE_2016-*/drifters_noaa.csv`. Some stored compatibility fields may still mention `data/arcgis/CASE_2016-*/source_point_metadata.geojson`, but that file is not the actual spill origin for the 2016 runs.

## Mindoro Phase 3

Promotion and provenance control:

- Frozen base case file: `config/case_mindoro_retro_2023.yaml`
- Promotion amendment file: `config/case_mindoro_retro_2023_phase3b_primary_validation_amendment.yaml`
- Canonical primary launcher entry: `mindoro_phase3b_primary_public_validation`
- Backward-compatible launcher alias: `mindoro_march13_14_noaa_reinit_stress_test`
- Shared-imagery guardrail: both NOAA/NESDIS public products cite March 12 WorldView-3 imagery, so the promoted B1 row is reportable as a reinitialization-based public-validation pair, not as an independent day-to-day validation.
- Track semantics: `B1` is the only main-text primary Mindoro validation row; `A` is the same-case comparator-support track attached to `B1`; `B2` is the March 6 legacy honesty row; `B3` is the March 3-6 broader-support legacy row.
- Spatial semantics: focused Phase 1 validation box `[118.751, 124.305, 10.620, 16.026]`; broad `mindoro_case_domain` fallback transport/overview extent `[115.0, 122.0, 6.0, 14.5]`; current scoring-grid display bounds `[120.90964677179262, 122.0621541786303, 12.249384840763462, 13.783655303175253]`.

### Phase 3A Benchmark Comparator Support

- Plain-language status: validation-support comparator track built around the promoted March 13 -> March 14 reinit comparison and attached to B1 rather than standing as a co-primary row.
- Reportable: `true`
- Frozen: `false`
- Guardrail: PyGNOME is comparator-only and not truth.

### Phase 3B1 March 13 -> March 14 Primary Validation

- Plain-language status: validation-focused and reportable promoted primary row carried under the thesis-facing title `Phase 3B Observation-Based Spatial Validation Using Public Mindoro Spill Extents`, with an explicit caveat that both NOAA products cite March 12 WorldView-3 imagery.
- Reportable: `true`
- Frozen: `false`
- Provenance: the original March 3 -> March 6 base case YAML stays frozen; this promotion is authorized through the separate amendment file above.
- Recipe-provenance note: the separate `phase1_mindoro_focus_pre_spill_2016_2023` drifter rerun selected the same `cmems_era5` recipe used by the stored B1 run; that rerun is now the active Mindoro-specific provenance lane, not a replacement raw-generation history for the stored B1 bundle.
- Thesis framing: separate focused drifter-based Phase 1 provenance -> Phase 2 -> Phase 3B primary validation.
- PyGNOME role on the same case: comparator-only support evidence, not truth and not the main validation claim.
- Guardrail: present it as the canonical Mindoro validation row, but keep the shared-imagery caveat explicit and do not describe it as independent day-to-day validation.

### Phase 3B2 Legacy March 6 Sparse Reference

- Plain-language status: preserved legacy sparse-reference honesty track.
- Reportable: `true`
- Frozen: `false`
- Guardrail: keep it visible in methods/limitations discussion, but do not present it as the canonical Mindoro result or call it primary anywhere.

### Phase 3B3 Legacy Broader Public Support

- Plain-language status: reportable legacy broader public-validation support track.
- Reportable: `true`
- Frozen: `false`
- Guardrail: keep it separate from both the promoted March 13 -> March 14 primary row and from DWH transfer validation.

## DWH Phase 3C

- Plain-language status: frozen validation-only external rich-data transfer case.
- Thesis framing: separate external transfer-validation/support case; Mindoro remains the main Philippine thesis case.
- C-track semantics: `C1` deterministic external transfer validation, `C2` ensemble extension and deterministic-vs-ensemble comparison, `C3` PyGNOME comparator-only.
- Deterministic OpenDrift: reportable transfer-validation result and the clean baseline DWH result.
- Ensemble comparison: reportable transfer-validation support/comparative result; p50 is the preferred probabilistic extension and p90 remains support/comparison only.
- PyGNOME comparator: reportable comparator-only result and never truth.
- Frozen: `true`
- Inherited provisional: `false`
- Guardrail: DWH observed masks remain truth, and PyGNOME remains comparator-only.
- No new drifter ingestion: thesis-facing DWH packaging does not ingest new drifter data and does not reopen Phase 1-style recipe competition.
- Forcing-stack selection rule: readiness-gated historical stack, not Phase 1 drifter-selected baseline logic.
- Current frozen DWH stack: HYCOM GOFS 3.1 currents + ERA5 winds + CMEMS wave/Stokes.
- Date-composite truth rule: use the 2010-05-20 initialization composite plus the 2010-05-21, 2010-05-22, and 2010-05-23 validation composites honestly as date labels only; do not invent exact sub-daily observation acquisition times.
- Forcing-outage policy: strict/reportable by default. Use `FORCING_OUTAGE_POLICY=continue_degraded` only if you intentionally want a provisional readiness result after a temporary forcing-provider outage.
- Scientific-ready means: not smoke-only, full May 20-23, 2010 coverage, required variables and usable metadata present, OpenDrift reader exposes the required variables, and the assembled stack completes a small reader-check forecast.
- Drop-in methods note: `docs/DWH_METHODS_NOTE.md`
- Final governance note: `docs/DWH_PHASE3C_FINAL.md`
- Freeze sync note: `docs/DWH_PHASE3C_FREEZE_SYNC_NOTE.md`

## Phase 4 Support Layer

- Plain-language status: implemented Mindoro oil-type and shoreline support bundle outside the main thesis phase count; thesis-facing `Phase 4` language is reserved for `prototype_2016`.
- Scientifically reportable: `true` as a technical/support bundle, `false` as a main thesis phase outside `prototype_2016`
- Scientifically frozen: `false`
- Inherited provisional: `true`
- Does this support bundle still depend on a later full Phase 1 production rerun for the final frozen baseline story? `no`, but it still depends on a manual downstream trial or promotion step if you want the new candidate baseline to become the default.
- Is any part of official product generation still coupled to legacy recipe naming drift? `yes`
- Biggest remaining blocker: the new Phase 1 candidate baseline is staged, but official spill-case workflows still use the older canonical baseline file unless you promote or override it manually with `BASELINE_SELECTION_PATH` or an explicit promotion step.

What is already in place:

- dedicated `phase4_oiltype_and_shoreline` workflow
- stable scenario registry in `config/phase4_oil_scenarios.csv`
- shoreline arrival timing, shoreline segments, oil budgets, and oil-type comparison outputs under `output/phase4/CASE_MINDORO_RETRO_2023/`
- explicit provenance fields recording baseline source, transport provenance, and inherited provisional status

What remains provisional:

- upstream Phase 1 freeze remains incomplete
- Phase 2 remains scientifically usable but not frozen
- legacy recipe-family drift is only documented and guarded, not fully removed
- `fixed_base_medium_heavy_proxy` remains flagged for mass-balance follow-up
- DWH Phase 4 remains deferred

## Phase 4 Cross-Model Comparability Audit

- Plain-language status: deferred; no scientifically defensible Phase 4 OpenDrift-versus-PyGNOME comparison is available from the current stored outputs.
- Scientifically reportable: `false`
- Reportable now: `true` for the audit verdict itself, `false` for a Phase 4 cross-model result
- Scientifically frozen: `false`
- Inherited provisional: `true`
- Biggest blocker: the repo still lacks a matched Mindoro PyGNOME Phase 4 output family with the same scenario registry, weathering compartments, and shoreline-segment semantics used by OpenDrift Phase 4.

What is already in place:

- `output/phase4/CASE_MINDORO_RETRO_2023/` contains real OpenDrift Phase 4 oil-budget, shoreline-arrival, and shoreline-segment outputs
- Mindoro and DWH PyGNOME Phase 3 comparator tracks already exist as spatial transport comparators
- helper code exists for parsing PyGNOME mass budgets and plotting OpenOil-versus-PyGNOME overlays
- explicit read-only audit outputs now live under `output/phase4_crossmodel_comparability_audit/`

Why comparison is still deferred:

- Mindoro PyGNOME benchmark metadata explicitly records `weathering_enabled=false`
- current stored PyGNOME NetCDF diagnostics collapse to 100% surface and 0% evaporated/dispersed/beached, which is not a defensible Phase 4 fate comparator
- no stored PyGNOME shoreline-arrival or shoreline-segment output family exists
- DWH PyGNOME remains a Phase 3 comparator branch and does not substitute for a matched Mindoro Phase 4 run

## Phase 5 Support Layer

- Plain-language status: read-only launcher/docs/package synchronization and explorer support layer outside the main thesis phase count; thesis-facing `Phase 5` language is reserved for `prototype_2016`.
- Scientifically reportable: `false`
- Reportable now: `true`
- Scientifically frozen: `false`
- Inherited provisional: `false`
- Scope: non-scientific reproducibility, launcher, documentation, and packaging sync built from existing artifacts without recomputing science.

What is already in place:

- matrix-driven launcher via `config/launcher_matrix.json` and `start.ps1`
- `output/final_reproducibility_package/` with software, case, config, manifest, log, output, and phase-status indexes
- synchronized documentation for launcher safety, command matrix, and current phase boundaries
- `output/trajectory_gallery/` with a static technical figure manifest, index, and trajectory/overlay/Phase 4 support figures built from existing artifacts only
- `output/trajectory_gallery_panel/` with polished comparison boards, captions, talking points, and panel recommendations built from the existing gallery and stored outputs only
- `output/figure_package_publication/` with canonical publication-grade single figures, side-by-side boards, captions, and defense talking points built from existing outputs only
- `output/2016 Legacy Runs FINAL Figures/` with a curated prototype_2016 paper-facing figure set, per-case subfolders, `final_figure_manifest.json`, and `missing_figures.csv` built from existing legacy outputs only
- `ui/` read-only dashboard pages that consume the publication package first, then expose panel/raw/archive artifacts only when advanced mode is enabled

Optional future work still deferred:

- interactive UI run controls
- deeper artifact filtering inside the UI
- DWH Phase 4 appendix pilot

## Read-Only Dashboard

- Plain-language status: implemented read-only local support/exploration layer for the current packaged outputs.
- Scientifically reportable: `false`
- Reportable now: `true`
- Scientifically frozen: `false`
- Inherited provisional: `true`
- Scope: non-scientific dashboard over publication figures, panel/raw archives, final reproducibility indexes, and the Phase 4 cross-model audit.

What is already in place:

- panel-friendly mode that defaults to publication-grade figures and recommended defense boards
- advanced mode that exposes panel/raw figures, manifests, logs, and lower-level artifact inspection
- dedicated pages for Mindoro validation, DWH transfer validation, cross-model comparison, Phase 4 support/context interpretation, and Phase 4 cross-model deferred status
- an explicit read-only Phase 4 cross-model status page that states the comparison is deferred and links to blocker/next-step artifacts

What remains deferred:

- scientific rerun controls from the UI
- deeper in-app artifact search/filtering beyond the current page-level controls

## Trajectory Gallery

- Plain-language status: implemented read-only technical figure layer for panel inspection.
- Scientifically reportable: `false`
- Reportable now: `true`
- Scientifically frozen: `false`
- Inherited provisional: `true`
- Scope: static trajectories, overlays, comparison maps, and Mindoro Phase 4 support/context shoreline/oil-type figures built from existing outputs without rerunning science.

Panel-ready now:

- Mindoro deterministic path figure
- Mindoro sampled ensemble member trajectory summary
- Mindoro centroid/corridor/hull view
- Mindoro March 13 -> March 14 primary-validation overlays
- Mindoro March 13 -> March 14 cross-model comparison maps
- DWH deterministic path figure
- DWH ensemble p50/p90 overlays
- DWH OpenDrift vs PyGNOME comparison maps
- Mindoro Phase 4 oil-budget figures
- Mindoro Phase 4 shoreline-arrival and shoreline-segment figures

Still optional future work:

- deeper interactive filtering/search around the gallery inside the UI
- interactive trajectory filtering/search

## Trajectory Gallery Panel Pack

- Plain-language status: implemented read-only polished board layer for panel presentation.
- Scientifically reportable: `false`
- Reportable now: `true`
- Scientifically frozen: `false`
- Inherited provisional: `true`
- Scope: panel-ready trajectory, comparison, and support/context Phase 4 boards with captions and talking points built from the raw gallery and stored outputs only.

Recommended main-presentation boards:

- Mindoro March 13 -> March 14 primary validation board
- Mindoro March 13 -> March 14 cross-model comparator board
- Mindoro trajectory board
- Mindoro legacy March 6 honesty / limitations board
- DWH deterministic forecast-vs-observation board
- DWH deterministic vs ensemble board
- DWH OpenDrift vs PyGNOME comparison board

Optional support/context boards:

- Mindoro Phase 4 oil-budget board
- Mindoro Phase 4 shoreline-arrival / shoreline-impact board

Still optional future work:

- alternate print-layout variants for every board
- deeper interactive board filtering/search inside the UI

## Publication Figure Package

- Plain-language status: implemented canonical publication-grade presentation layer.
- Scientifically reportable: `false`
- Reportable now: `true`
- Scientifically frozen: `false`
- Inherited provisional: `true`
- Scope: publication-grade single figures, side-by-side boards, captions, talking points, and a Phase 4 deferred-comparison note figure built from existing outputs without rerunning science.

Recommended main-presentation figures:

- Mindoro March 13 -> March 14 primary validation board
- Mindoro March 13 -> March 14 cross-model comparator board
- Mindoro legacy March 6 honesty / limitations board
- Mindoro trajectory publication board
- DWH deterministic forecast-vs-observation board
- DWH deterministic versus ensemble publication board
- DWH OpenDrift versus PyGNOME publication board

Optional support/context figures:

- Mindoro Phase 4 oil-budget publication board
- Mindoro Phase 4 shoreline-impact publication board
- Mindoro Phase 4 deferred-comparison note figure when the panel asks why no Phase 4 PyGNOME board is shown

What this package adds beyond the panel gallery:

- paper-ready single-image figures
- forced locator, zoom, and close-up variants where the observed target is tiny
- explicit publication registry, captions, and talking points
- a canonical presentation layer while keeping the raw and panel galleries as archives

What remains provisional:

- Mindoro publication figures still inherit the unfinished Phase 1/2 frozen-baseline story
- legacy recipe-family drift remains upstream and is documented rather than fully removed
- DWH publication figures remain transfer-validation/support visuals, not a replacement for the Mindoro thesis case
