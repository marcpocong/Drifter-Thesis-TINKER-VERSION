# Paper-to-Output Registry

This registry is the short reviewer-facing companion to [`config/paper_to_output_registry.yaml`](../config/paper_to_output_registry.yaml). It maps final manuscript labels to stored outputs already present in this repository.

- It is read-only.
- It does not promote experimental, archive-only, or sensitivity-only outputs.
- It does not run scientific workflows or download data.
- Raw `output/CASE_MINDORO_RETRO_2023` and `output/CASE_DWH_RETRO_2010_72H` paths are provenance/staging context only; curated package paths lead.

## Registry

| Manuscript item | Plain-language mapping | Stored output path(s) | Notes |
| --- | --- | --- | --- |
| `Table 3.7` | Active Phase 1 provenance lane and adopted selection rules. | `output/phase1_mindoro_focus_pre_spill_2016_2023/phase1_accepted_segment_registry.csv`; `phase1_ranking_subset_registry.csv`; `phase1_official_adoption_decision.md` | Provenance only. |
| `Table 3.8` | Four-recipe family tested in the focused Mindoro provenance lane. | `output/phase1_mindoro_focus_pre_spill_2016_2023/phase1_recipe_ranking.csv` | Recipe-selection support only. |
| `Table 3.9` | Final deterministic and ensemble settings used for Mindoro transport-core products. | `output/final_reproducibility_package/final_manifest_index.csv`; `output/final_validation_package/final_validation_manifest.json` | Product settings, not validation by itself. |
| `Table 3.10` | Standardized product families produced in Phase 2. | `output/final_reproducibility_package/final_output_catalog.csv`; `output/final_validation_package/final_validation_manifest.json` | `mask_p50` preferred; `mask_p90` conservative support only. |
| `Table 3.11` | Final Mindoro March 13–14 primary validation case definition. | `output/Phase 3B March13-14 Final Output/summary/opendrift_primary/march13_14_reinit_summary.csv`; `march13_14_reinit_run_manifest.json` | Only main Philippine public-observation validation claim. |
| `Table 3.12` | Final Mindoro manuscript labels. | `docs/FINAL_SUBMISSION_ALIGNMENT_CONTRACT.md`; `docs/FINAL_PAPER_ALIGNMENT.md` | `B1` and `Track A` are internal aliases only. |
| `Table 4.1` | Result groups, evidence roles, and interpretation boundaries. | `docs/FINAL_SUBMISSION_ALIGNMENT_CONTRACT.md`; `output/final_validation_package/final_validation_claims_guardrails.md` | Boundary table only. |
| `Table 4.2` | Focused Mindoro Phase 1 accepted-pool summary. | `phase1_accepted_segment_registry.csv`; `phase1_ranking_subset_registry.csv` | Provenance corpus counts. |
| `Table 4.3` | Focused Mindoro Phase 1 recipe ranking. | `phase1_recipe_ranking.csv` | NCS recipe provenance. |
| `Table 4.4` | Standardized Forecast Products used in later scoring. | `output/final_reproducibility_package/final_output_catalog.csv`; `output/final_validation_package/final_validation_case_registry.csv` | Products used by scoring. |
| `Table 4.5` | Mindoro primary validation FSS by neighborhood window. | `output/Phase 3B March13-14 Final Output/summary/opendrift_primary/march13_14_reinit_fss_by_window.csv` | Coastal-neighborhood usefulness only. |
| `Table 4.6` | Mindoro primary validation branch survival and displacement diagnostics. | `march13_14_reinit_branch_survival_summary.csv`; `march13_14_reinit_diagnostics.csv` | Scoreable branch, not exact-grid success. |
| `Table 4.7` | Mindoro primary validation overlap and neighborhood FSS diagnostics. | `march13_14_reinit_summary.csv`; `march13_14_reinit_fss_by_window.csv` | IoU and Dice are zero. |
| `Table 4.8` | Mindoro same-case OpenDrift–PyGNOME comparator detail. | `output/Phase 3B March13-14 Final Output/summary/comparator_pygnome/march13_14_reinit_crossmodel_summary.csv`; `march13_14_reinit_crossmodel_model_ranking.csv` | Comparator-only; PyGNOME is not truth. |
| `Table 4.9` | Deepwater Horizon daily and event-corridor mean FSS summary. | `output/Phase 3C DWH Final Output/summary/comparison/phase3c_main_scorecard.csv` | DWH external transfer only. |
| `Table 4.10` | Deepwater Horizon event-corridor geometry diagnostics. | `phase3c_eventcorridor_summary.csv`; `phase3c_ensemble_eventcorridor_summary.csv`; `phase3c_dwh_pygnome_eventcorridor_summary.csv` | PyGNOME comparator-only. |
| `Table 4.11` | Secondary 2016 direct drifter-track benchmark summary. | `output/2016_drifter_benchmark/scorecard.csv`; `manifest.json` | Support only. |
| `Table 4.11A` | Secondary 2016 scorecard summary values. | `output/2016_drifter_benchmark/scorecard.csv`; `scorecard.json` | Support only. |
| `Table 4.11B` | Secondary 2016 endpoint and ensemble-footprint diagnostics from the scorecards. | `output/2016_drifter_benchmark/scorecard.csv`; `scorecard.json` | Support only. |
| `Table 4.12` | Legacy 2016 OpenDrift-versus-PyGNOME mean FSS by case, support surface, and neighborhood window. | `output/prototype_2016_pygnome_similarity/prototype_pygnome_fss_by_case_window.csv`; `output/2016 Legacy Runs FINAL Figures/summary/phase3a/prototype_pygnome_fss_by_case_window.csv` | Legacy comparator support only. |
| `Table 4.13` | Synthesis of principal findings and thesis use. | `docs/FINAL_SUBMISSION_ALIGNMENT_CONTRACT.md`; `output/final_validation_package/final_validation_summary.md` | Synthesis only. |
| `Figure 4.1` | Focused Phase 1 accepted February–April segment map. | `phase1_accepted_segment_registry.csv`; `phase1_ranking_subset_registry.csv` | Stored segment registries are the trace source. |
| `Figure 4.2` | Focused Phase 1 recipe ranking chart. | `phase1_recipe_ranking.csv` | Stored ranking CSV is the trace source. |
| `Figure 4.3` | Mindoro product-family board with deterministic, probability, and threshold surfaces. | `output/final_reproducibility_package/final_output_catalog.csv`; Phase 2 publication products | Product-family support only. |
| `Figure 4.4` | Mindoro primary validation board. | `output/Phase 3B March13-14 Final Output/publication/opendrift_primary/mindoro_primary_validation_board.png` | Primary Mindoro visualization. |
| `Figure 4.4A` | NOAA-published March 13 WorldView-3 analysis map. | `output/Phase 3B March13-14 Final Output/publication/observations/figure_4_4A_noaa_mar13_worldview3.png` | Seed observation support. |
| `Figure 4.4B` | NOAA-published March 14 WorldView-3 analysis map. | `output/Phase 3B March13-14 Final Output/publication/observations/figure_4_4B_noaa_mar14_worldview3.png` | Target observation support. |
| `Figure 4.4C` | ArcGIS overlay of March 13 and March 14 observed oil-spill extents. | `output/Phase 3B March13-14 Final Output/publication/observations/figure_4_4C_arcgis_mar13_mar14_observed_overlay.png` | Observation-pair context. |
| `Figure 4.5` | Mindoro same-case OpenDrift–PyGNOME spatial comparator board. | `output/Phase 3B March13-14 Final Output/publication/comparator_pygnome/Figure_4_5_Mindoro_TrackA_OpenDrift_PyGNOME_spatial_board.png` | Comparator-only. |
| `Figure 4.6` | Mindoro same-case OpenDrift–PyGNOME comparator mean FSS summary. | `march13_14_reinit_crossmodel_model_ranking.csv` | Comparator-only. |
| `Figure 4.7` | DWH observed, deterministic, mask_p50, and PyGNOME event-corridor board. | `output/Phase 3C DWH Final Output/publication/comparator_pygnome/dwh_2010-05-21_to_2010-05-23_eventcorridor_observed_deterministic_mask_p50_pygnome_board.png` | DWH external transfer. |
| `Figure 4.8` | DWH 24 h, 48 h, and 72 h mask_p50, mask_p90, and PyGNOME overview board. | `output/Phase 3C DWH Final Output/publication/comparator_pygnome/dwh_24h_48h_72h_mask_p50_mask_p90_vs_pygnome_three_row_overview_board.png` | `mask_p90` conservative support only. |
| `Figure 4.9` | DWH 48 h observed, deterministic, mask_p50, and PyGNOME board. | `output/Phase 3C DWH Final Output/publication/comparator_pygnome/dwh_2010-05-22_48h_observed_deterministic_mask_p50_pygnome_board.png` | Comparator-only for PyGNOME. |
| `Figure 4.10` | CASE_2016-09-01 secondary drifter-track benchmark map panel. | `output/2016_drifter_benchmark/case_boards/CASE_2016-09-01_drifter_track_benchmark.png` | Support only. |
| `Figure 4.11` | CASE_2016-09-06 secondary drifter-track benchmark map panel. | `output/2016_drifter_benchmark/case_boards/CASE_2016-09-06_drifter_track_benchmark.png` | Support only. |
| `Figure 4.12` | CASE_2016-09-17 secondary drifter-track benchmark map panel. | `output/2016_drifter_benchmark/case_boards/CASE_2016-09-17_drifter_track_benchmark.png` | Support only. |
| `Figure 4.13` | Legacy 2016 OpenDrift-versus-PyGNOME overall mean FSS chart. | `output/prototype_2016_pygnome_similarity/qa_prototype_pygnome_scorecard.png`; `prototype_pygnome_fss_by_case_window.csv` | Legacy comparator support only. |

## Important Interpretation Boundaries

- Primary Mindoro March 13–14 is the only main Philippine public-observation validation claim.
- It supports coastal-neighborhood usefulness only, not exact 1 km spill-footprint reproduction.
- PyGNOME is comparator-only.
- DWH is external transfer only.
- Oil-type and shoreline outputs are support/context only.
- Secondary 2016 outputs are support only.
- `mask_p50` is the preferred probabilistic footprint; `mask_p90` is conservative support only.
