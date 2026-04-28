# Paper-to-Output Registry

This registry maps thesis-facing tables and figures to the stored outputs already present in this repository.

- This registry is read-only.
- It is intended for panel review, defense inspection, and audit.
- It does not promote experimental or sensitivity-only outputs into thesis-facing results.
- The 5,000-element personal experiment is intentionally excluded from the default thesis-facing panel registry.

## How to read this file

- "This table is reproduced from..." means the manuscript value should be traceable to the listed stored CSV or package summary.
- "This figure is rebuilt from..." means the current publication or defense figure is a packaging-layer rebuild from stored outputs only.
- "Comparator support only" means the output helps explain behavior but is not observational truth and is not a co-primary validation claim.
- "Support/context only" means the output is useful for interpretation, not for the main validation claim.

## Registry

| Manuscript item | Plain-language mapping | Stored output path(s) | Notes |
| --- | --- | --- | --- |
| `Table 4.5` | This table is reproduced from the stored B1 scorecard for the promoted March 13 -> March 14 R1 row. | `output/CASE_MINDORO_RETRO_2023/phase3b_extended_public_scored_march13_14_reinit/march13_14_reinit_summary.csv` | Main Mindoro validation row only. |
| `Table 4.6` | This table is reproduced from the stored B1 branch-survival and displacement diagnostics. | `output/CASE_MINDORO_RETRO_2023/phase3b_extended_public_scored_march13_14_reinit/march13_14_reinit_branch_survival_summary.csv`; `output/CASE_MINDORO_RETRO_2023/phase3b_extended_public_scored_march13_14_reinit/march13_14_reinit_branch_pairing_manifest.csv`; `output/CASE_MINDORO_RETRO_2023/phase3b_extended_public_scored_march13_14_reinit/march13_14_reinit_summary.csv` | Shared-imagery caveat remains explicit. |
| `Table 4.7` | This table is reproduced from the stored B1 overlap and neighborhood-skill outputs. | `output/CASE_MINDORO_RETRO_2023/phase3b_extended_public_scored_march13_14_reinit/march13_14_reinit_fss_by_window.csv`; `output/CASE_MINDORO_RETRO_2023/phase3b_extended_public_scored_march13_14_reinit/march13_14_reinit_summary.csv` | B1 supports neighborhood-scale usefulness, not exact 1 km reproduction. |
| `Figure 4.4` | This figure is rebuilt from the stored B1 board package. | `output/figure_package_publication/case_mindoro_retro_2023__phase3b_reinit_primary__opendrift__comparison_board__2023_03_13_to_2023_03_14__board__slide__mindoro_primary_validation_board.png` | Read-only packaging from stored outputs only. |
| `Table 4.9` | This table is reproduced from the stored Track A OpenDrift-vs-PyGNOME comparator scorecards. | `output/CASE_MINDORO_RETRO_2023/phase3b_extended_public_scored_march13_14_reinit_pygnome_comparison/march13_14_reinit_crossmodel_summary.csv`; `output/CASE_MINDORO_RETRO_2023/phase3b_extended_public_scored_march13_14_reinit_pygnome_comparison/march13_14_reinit_crossmodel_model_ranking.csv` | Comparator support only. PyGNOME is not observational truth. |
| `Figure 4.5` | This figure is rebuilt from the stored Track A publication board. | `output/figure_package_publication/Figure_4_5_Mindoro_TrackA_OpenDrift_PyGNOME_spatial_board.png`; `output/figure_package_publication/case_mindoro_retro_2023__phase3a_reinit_crossmodel__opendrift_vs_pygnome__comparison_board__2023_03_14__board__slide__mindoro_crossmodel_board.png` | Comparator support only. |
| `Figure 4.6` | This figure is backed by the stored Track A ranking CSV. | `output/CASE_MINDORO_RETRO_2023/phase3b_extended_public_scored_march13_14_reinit_pygnome_comparison/march13_14_reinit_crossmodel_model_ranking.csv` | No dedicated canonical Track A bar-chart PNG was found in the current stored package; use the ranking CSV as the machine-readable source. |
| `Table 4.10` | This table is reproduced from the stored DWH deterministic, ensemble, and comparator FSS summaries. | `output/CASE_DWH_RETRO_2010_72H/phase3c_external_case_run/phase3c_fss_by_date_window.csv`; `output/CASE_DWH_RETRO_2010_72H/phase3c_external_case_ensemble_comparison/phase3c_ensemble_fss_by_date_window.csv`; `output/CASE_DWH_RETRO_2010_72H/phase3c_dwh_pygnome_comparator/phase3c_dwh_pygnome_fss_by_date_window.csv`; `output/Phase 3C DWH Final Output/summary/comparison/phase3c_main_scorecard.csv` | DWH is external transfer validation, not Mindoro recalibration. |
| `Table 4.11` | This table is reproduced from the stored DWH corridor-overlap and geometry diagnostics. | `output/CASE_DWH_RETRO_2010_72H/phase3c_external_case_run/phase3c_eventcorridor_summary.csv`; `output/CASE_DWH_RETRO_2010_72H/phase3c_external_case_ensemble_comparison/phase3c_ensemble_eventcorridor_summary.csv`; `output/CASE_DWH_RETRO_2010_72H/phase3c_dwh_pygnome_comparator/phase3c_dwh_pygnome_eventcorridor_summary.csv`; `output/CASE_DWH_RETRO_2010_72H/phase3c_external_case_run/phase3c_diagnostics.csv` | PyGNOME remains comparator-only. |
| `Figure 4.7` | This figure is rebuilt from the stored DWH deterministic board. | `output/Phase 3C DWH Final Output/publication/opendrift_deterministic/dwh_24h_48h_72h_deterministic_footprint_overview_board.png`; `output/figure_package_publication/case_dwh_retro_2010_72h__phase3c_external_case_run__opendrift__comparison_board__2010_05_21_to_2010_05_23__board__slide__daily_deterministic_footprint_overview_board.png` | Read-only packaging from stored outputs only. |
| `Figure 4.8` | This figure is rebuilt from the stored DWH deterministic / p50 / p90 comparison board. | `output/Phase 3C DWH Final Output/publication/opendrift_ensemble/dwh_2010-05-21_to_2010-05-23_eventcorridor_observed_deterministic_mask_p50_mask_p90_board.png`; `output/figure_package_publication/case_dwh_retro_2010_72h__phase3c_external_case_ensemble_comparison__opendrift__comparison_board__2010_05_21_to_2010_05_23__board__slide__observed_deterministic_mask_p50_mask_p90_board.png` | `p50` is the preferred probabilistic extension; `p90` is support/comparison only. |
| `Figure 4.9` | This figure is rebuilt from the stored DWH deterministic / p50 / PyGNOME comparator board. | `output/Phase 3C DWH Final Output/publication/comparator_pygnome/dwh_2010-05-21_to_2010-05-23_eventcorridor_observed_deterministic_mask_p50_pygnome_board.png`; `output/figure_package_publication/case_dwh_retro_2010_72h__phase3c_dwh_pygnome_comparator__opendrift_vs_pygnome__comparison_board__2010_05_21_to_2010_05_23__board__slide__observed_deterministic_mask_p50_pygnome_board.png` | Comparator support only. PyGNOME is not observational truth. |
| `Table F2` | This support table is reproduced from the stored Mindoro oil-budget summary. | `output/phase4/CASE_MINDORO_RETRO_2023/phase4_oil_budget_summary.csv`; `output/phase4/CASE_MINDORO_RETRO_2023/phase4_shoreline_arrival.csv` | Support/context only. Not observational truth. |

## Important interpretation boundaries

- `B1` is the only main Mindoro validation row for the thesis-facing panel story.
- `Track A` is comparator support only. It compares OpenDrift and PyGNOME on the same case, but PyGNOME is not truth.
- `DWH` is a separate external transfer-validation lane. It does not recalibrate Mindoro.
- `Oil-type` and `shoreline-arrival` outputs are support/context only.
- Experimental or sensitivity-only outputs, including the 5,000-element personal experiment, are excluded from the default panel-facing registry.
