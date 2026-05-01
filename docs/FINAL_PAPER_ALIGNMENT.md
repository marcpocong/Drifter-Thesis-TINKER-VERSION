# Final Paper Alignment

## Title And Purpose

Final manuscript title: Drifter-Validated 24–72 h Oil-Spill Forecasting for Philippine Coasts: Probability Footprints and Oil-Type Fate.

This repository is the panel-ready reproducibility package for the final paper. It exposes stored outputs, launcher entries, registries, figures, and read-only review surfaces so panel reviewers can inspect the evidence without needing any uploaded manuscript file.

Use [DATA_SOURCES.md](DATA_SOURCES.md) for data-source provenance and [ARCHIVE_GOVERNANCE.md](ARCHIVE_GOVERNANCE.md) for archive routing and claim boundaries.

## Final Evidence Order

1. Focused Mindoro Phase 1 transport provenance.
2. Phase 2 standardized deterministic and 50-member forecast products.
3. Mindoro B1 March 13-14 primary public-observation validation.
4. Mindoro same-case OpenDrift-PyGNOME comparator support.
5. DWH external transfer validation.
6. Mindoro oil-type and shoreline support/context.
7. Secondary 2016 drifter-track and legacy FSS support.
8. Reproducibility, governance, and read-only package layer.

## Claim Boundaries

- Mindoro B1 is the only main Philippine public-observation validation claim.
- B1 supports coastal-neighborhood usefulness, not exact 1 km overlap and not universal operational accuracy.
- March 13 public NOAA/NESDIS observation extent is the seed; March 14 public NOAA/NESDIS observation extent is the target.
- March 13 and March 14 are independent day-specific public-observation products.
- PyGNOME is comparator-only and never observation truth.
- DWH is external transfer validation only, not Mindoro recalibration.
- Mindoro oil-type and shoreline outputs are support/context only.
- Secondary 2016 outputs support drifter-track and legacy OpenDrift-PyGNOME FSS context only; they are not public-spill validation and do not replace Mindoro B1 or DWH.
- Archived, experimental, legacy, and comparator-only outputs remain inspectable but are not promoted into the main paper claim.

## Phase 1 Ranking

- Workflow mode: `phase1_mindoro_focus_pre_spill_2016_2023`
- Historical window: `2016-01-01` to `2023-03-02`
- Focused box: `[118.751, 124.305, 10.620, 16.026]`
- Full strict accepted segments: `65`
- February-April ranked subset: `19`
- Selected recipe: `cmems_gfs`

| Recipe | Mean NCS | Median NCS |
| --- | ---: | ---: |
| `cmems_gfs` | `4.5886` | `4.6305` |
| `cmems_era5` | `4.6237` | `4.5916` |
| `hycom_gfs` | `4.7027` | `4.9263` |
| `hycom_era5` | `4.7561` | `5.0106` |

## Mindoro B1 Metrics

Mindoro B1 is the March 13-14 primary public-observation validation row. It is interpreted as coastal-neighborhood usefulness, not exact 1 km overlap.

| Metric | Value |
| --- | ---: |
| FSS 1 km | `0.0000` |
| FSS 3 km | `0.0441` |
| FSS 5 km | `0.1371` |
| FSS 10 km | `0.2490` |
| Mean FSS | `0.1075` |
| Forecast cells | `5` |
| Observed cells | `22` |
| Nearest distance | `1414.21 m` |
| Centroid distance | `7358.16 m` |
| IoU | `0.0` |
| Dice | `0.0` |

The row survives to the target date and is scoreable. The zero IoU and Dice values mean it must not be described as exact-grid success.

## Track A Comparator Metrics

Track A is same-case comparator support against the March 14 public mask. It is comparator-only and is not an independent truth source.

| Branch | Forecast cells | Nearest distance | FSS values | Mean FSS |
| --- | ---: | ---: | --- | ---: |
| OpenDrift promoted p50 branch | `5` | `1414.21 m` | `0.0000 / 0.0441 / 0.1371 / 0.2490` | `0.1075` |
| PyGNOME deterministic comparator | `6` | `6082.76 m` | `0.0000 / 0.0000 / 0.0000 / 0.0244` | `0.0061` |

PyGNOME remains comparator-only in this package. It is never observation truth.

## DWH External Transfer Metrics

- Case ID: `CASE_DWH_RETRO_2010_72H`
- Forcing stack: `HYCOM GOFS 3.1 currents + ERA5 winds + CMEMS wave/Stokes`
- Interpretation: external transfer validation only, not Mindoro recalibration.

| Product | Event-corridor mean FSS |
| --- | ---: |
| Deterministic | `0.5568` |
| Ensemble p50 | `0.5389` |
| Ensemble p90 | `0.4966` |
| PyGNOME comparator | `0.3612` |

## Probability Semantics

- `prob_presence` is the cellwise ensemble probability of presence.
- `mask_p50` means `P >= 0.50`; it is the preferred probabilistic footprint.
- `mask_p90` means `P >= 0.90`; it is a conservative support/comparison product only.
- Do not label `mask_p90` as a broad envelope.

## Oil-Type And Shoreline Support

These values are support/context only, not primary validation.

| Scenario | Beached fraction | First arrival | Impacted segments | QC |
| --- | ---: | ---: | ---: | --- |
| Light oil | `0.02%` | `4 h` | `11` | pass |
| Fixed-base medium-heavy proxy | `0.61%` | `4 h` | `10` | flagged |
| Heavier oil | `0.63%` | `4 h` | `11` | pass |

## Secondary 2016 Support Role

The 2016 material provides direct drifter-track and legacy OpenDrift-PyGNOME FSS support only. It is not public-spill validation and is not a replacement for Mindoro B1 or DWH.

## Launcher Entries By Role

Main evidence:

- `phase1_mindoro_focus_provenance`
- `mindoro_phase3b_primary_public_validation`
- `dwh_reportable_bundle`
- `mindoro_reportable_core`

Support/context:

- `mindoro_phase4_only`
- `mindoro_appendix_sensitivity_bundle`

Archive/provenance:

- `phase1_regional_reference_rerun`
- `mindoro_march13_14_phase1_focus_trial`
- `mindoro_march6_recovery_sensitivity`
- `mindoro_march23_extended_public_stress_test`
- `phase1_mindoro_focus_pre_spill_experiment` hidden compatibility alias
- `phase1_production_rerun` hidden compatibility alias
- `mindoro_march13_14_noaa_reinit_stress_test` hidden compatibility alias
- `phase3b_mindoro_march3_4_philsa_5000_experiment` hidden experimental archive
- `mindoro_mar09_12_multisource_experiment` hidden experimental archive
- `phase3b_mindoro_march13_14_reinit_5000_experiment` hidden experimental archive

Legacy support:

- `prototype_legacy_final_figures`
- `prototype_2021_bundle`
- `prototype_legacy_bundle`

Read-only governance:

- `phase1_audit`
- `phase2_audit`
- `b1_drifter_context_panel`
- `final_validation_package`
- `phase5_sync`
- `trajectory_gallery`
- `trajectory_gallery_panel`
- `figure_package_publication`

Use `.\start.ps1 -List -NoPause`, `.\start.ps1 -ListRole archive_provenance -NoPause`, and `.\start.ps1 -Explain <entry_id> -NoPause` to inspect launcher roles without running expensive science.
