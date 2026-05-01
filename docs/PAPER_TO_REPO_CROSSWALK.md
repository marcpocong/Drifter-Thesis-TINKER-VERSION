# Paper-To-Repo Crosswalk

This crosswalk maps final-paper items to stored outputs, configs, docs, or archive notes in this repository. The machine-readable registry is [`config/paper_to_output_registry.yaml`](../config/paper_to_output_registry.yaml), and the validator is [`scripts/validate_paper_to_output_registry.py`](../scripts/validate_paper_to_output_registry.py).

This is a stored-output/config/doc registry. It does not run science, refetch data, or create new claims. Reviewer-facing labels follow [`FINAL_SUBMISSION_ALIGNMENT_CONTRACT.md`](FINAL_SUBMISSION_ALIGNMENT_CONTRACT.md).

Curated reviewer-facing package roots are listed first. Raw `output/CASE_MINDORO_RETRO_2023` and `output/CASE_DWH_RETRO_2010_72H` paths are optional provenance/staging routes only and must not be treated as required reviewer entry points.

## Chapter 3 Tables

| Paper item | Final paper label | Reviewer-facing trace target | Claim boundary |
| --- | --- | --- | --- |
| `table_3_7` | Table 3.7 - Active Phase 1 provenance lane and adopted selection rules | `output/phase1_mindoro_focus_pre_spill_2016_2023/phase1_accepted_segment_registry.csv`; `phase1_ranking_subset_registry.csv`; `phase1_official_adoption_decision.md` | Transport provenance only. |
| `table_3_8` | Table 3.8 - Four-recipe family tested in the focused Mindoro provenance lane | `phase1_recipe_ranking.csv`; `phase1_recipe_summary.csv`; `phase1_gfs_month_preflight.csv`; `config/recipes.yaml` | Recipe-selection support only. |
| `table_3_9` | Table 3.9 - Final deterministic and ensemble settings used for the Mindoro transport-core products | `output/final_reproducibility_package/final_manifest_index.csv`; `output/final_validation_package/final_validation_manifest.json`; Phase 2 trajectory products in `output/figure_package_publication` | Standardized product setup, not validation by itself. |
| `table_3_10` | Table 3.10 - Standardized product families produced in Phase 2 | `output/final_reproducibility_package/final_output_catalog.csv`; `output/final_validation_package/final_validation_manifest.json` | `mask_p50` is preferred; `mask_p90` is conservative support only. |
| `table_3_11` | Table 3.11 - Final Mindoro March 13–14 primary validation case definition | `output/Phase 3B March13-14 Final Output/summary/opendrift_primary/march13_14_reinit_summary.csv`; `march13_14_reinit_run_manifest.json`; observation figures 4.4A/4.4B | Only main Philippine public-observation validation claim; coastal-neighborhood usefulness only. |
| `table_3_12` | Table 3.12 - Final Mindoro manuscript labels | `docs/FINAL_SUBMISSION_ALIGNMENT_CONTRACT.md`; `docs/FINAL_PAPER_ALIGNMENT.md`; `output/final_validation_package/final_validation_claims_guardrails.md` | `B1` and `Track A` are internal aliases only. |
| `table_3_13` | Table 3.13 - Frozen DWH external-case settings in the final implementation | `output/Phase 3C DWH Final Output/manifests/phase3c_final_output_manifest.json`; DWH setup memos; `config/case_dwh_retro_2010_72h.yaml` | DWH is external transfer only. |
| `table_3_14` | Table 3.14 - Deepwater Horizon external-case output groups and interpretation boundaries | DWH deterministic, ensemble, comparator registries and `phase3c_output_matrix_decision_note.md` | PyGNOME comparator-only; no Mindoro recalibration. |
| `table_3_15` | Table 3.15 - Mindoro oil-type support scenarios | `output/phase4/CASE_MINDORO_RETRO_2023/phase4_oil_budget_summary.csv`; `phase4_oiltype_comparison.csv`; `phase4_shoreline_arrival.csv` | Support/context only. |
| `table_3_16` | Table 3.16 - Artifact classes and their allowed presentation surfaces | `docs/ARCHIVE_GOVERNANCE.md`; `config/archive_registry.yaml`; `config/launcher_matrix.json` | Preservation is not promotion. |
| `table_3_17` | Table 3.17 - Reproducibility-control record groups used in the implemented workflow | `output/final_reproducibility_package`; `config/paper_to_output_registry.yaml`; this crosswalk | Governance only; no scientific reruns. |

Stale mappings removed: Table 3.11 is no longer "Mindoro deterministic product setup," and Table 3.12 is no longer "Mindoro ensemble/probability products." Those product concepts now sit under Tables 3.9 and 3.10.

## Chapter 4 Tables

| Paper item | Final paper label | Reviewer-facing trace target | Claim boundary |
| --- | --- | --- | --- |
| `table_4_1` | Table 4.1 - Result groups, evidence roles, and interpretation boundaries | `docs/FINAL_SUBMISSION_ALIGNMENT_CONTRACT.md`; `docs/FINAL_PAPER_ALIGNMENT.md`; `output/final_validation_package/final_validation_claims_guardrails.md` | Defines boundaries; creates no new claim. |
| `table_4_2` | Table 4.2 - Focused Mindoro Phase 1 accepted-pool summary | `phase1_accepted_segment_registry.csv`; `phase1_ranking_subset_registry.csv` | Provenance corpus counts only. |
| `table_4_3` | Table 4.3 - Focused Mindoro Phase 1 recipe ranking | `phase1_recipe_ranking.csv`; `phase1_official_adoption_decision.md` | Recipe provenance only. |
| `table_4_4` | Table 4.4 - Standardized Forecast Products used in later scoring | `output/final_reproducibility_package/final_output_catalog.csv`; `output/final_validation_package/final_validation_case_registry.csv` | Forecast products used by later scoring. |
| `table_4_5` | Table 4.5 - Mindoro primary validation FSS by neighborhood window | `output/Phase 3B March13-14 Final Output/summary/opendrift_primary/march13_14_reinit_fss_by_window.csv`; `march13_14_reinit_summary.csv` | Primary Mindoro March 13–14 only; coastal-neighborhood usefulness only. |
| `table_4_6` | Table 4.6 - Mindoro primary validation branch survival and displacement diagnostics | `march13_14_reinit_branch_survival_summary.csv`; `march13_14_reinit_diagnostics.csv` | Scoreable branch; not exact-grid success. |
| `table_4_7` | Table 4.7 - Mindoro primary validation overlap and neighborhood FSS diagnostics | `march13_14_reinit_summary.csv`; `march13_14_reinit_fss_by_window.csv` | IoU and Dice are zero; no exact 1 km success claim. |
| `table_4_8` | Table 4.8 - Mindoro same-case OpenDrift–PyGNOME comparator detail | `output/Phase 3B March13-14 Final Output/summary/comparator_pygnome/march13_14_reinit_crossmodel_summary.csv`; `march13_14_reinit_crossmodel_model_ranking.csv` | PyGNOME comparator-only. |
| `table_4_9` | Table 4.9 - Deepwater Horizon daily and event-corridor mean FSS summary | `output/Phase 3C DWH Final Output/summary/comparison/phase3c_main_scorecard.csv`; DWH deterministic/ensemble/PyGNOME summaries | DWH external transfer only. |
| `table_4_10` | Table 4.10 - Deepwater Horizon event-corridor geometry diagnostics | DWH event-corridor deterministic, ensemble, and PyGNOME summaries | PyGNOME comparator-only. |
| `table_4_11` | Table 4.11 - Secondary 2016 direct drifter-track benchmark summary | `output/2016_drifter_benchmark/scorecard.csv`; `manifest.json` | Secondary support only. |
| `table_4_11a` | Table 4.11A - Secondary 2016 scorecard summary values | `output/2016_drifter_benchmark/scorecard.csv`; `scorecard.json` | Support only; no universal superiority claim. |
| `table_4_11b` | Table 4.11B - Secondary 2016 endpoint and ensemble-footprint diagnostics from the scorecards | `output/2016_drifter_benchmark/scorecard.csv`; endpoint and footprint fields | Support only. |
| `table_4_12` | Table 4.12 - Legacy 2016 OpenDrift-versus-PyGNOME mean FSS by case, support surface, and neighborhood window | `output/prototype_2016_pygnome_similarity/prototype_pygnome_fss_by_case_window.csv`; `output/2016 Legacy Runs FINAL Figures/summary/phase3a/prototype_pygnome_fss_by_case_window.csv` | Legacy comparator support only. |
| `table_4_13` | Table 4.13 - Synthesis of principal findings and thesis use | `docs/FINAL_SUBMISSION_ALIGNMENT_CONTRACT.md`; `output/final_validation_package/final_validation_summary.md` | Synthesis only; no boundary expansion. |

## Chapter 4 Figures

| Paper item | Final paper label | Reviewer-facing trace target | Claim boundary |
| --- | --- | --- | --- |
| `figure_4_1` | Figure 4.1 - Focused Phase 1 accepted February–April segment map | `phase1_accepted_segment_registry.csv`; `phase1_ranking_subset_registry.csv`; optional final-layout map path in the registry | Transport provenance only. |
| `figure_4_2` | Figure 4.2 - Focused Phase 1 recipe ranking chart | `phase1_recipe_ranking.csv`; `scripts/figures/make_fig_5_3_focused_phase1_recipe_ranking.py`; optional final-layout chart path in the registry | Recipe provenance only. |
| `figure_4_3` | Figure 4.3 - Mindoro product-family board with deterministic, probability, and threshold surfaces | `output/final_reproducibility_package/final_output_catalog.csv`; `output/final_validation_package/final_validation_manifest.json`; Phase 2 publication products | `mask_p50` preferred; `mask_p90` conservative support only. |
| `figure_4_4` | Figure 4.4 - Mindoro primary validation board | `output/Phase 3B March13-14 Final Output/publication/opendrift_primary/mindoro_primary_validation_board.png` | Primary Mindoro bounded-usefulness visualization. |
| `figure_4_4a` | Figure 4.4A - NOAA-published March 13 WorldView-3 analysis map | `output/Phase 3B March13-14 Final Output/publication/observations/figure_4_4A_noaa_mar13_worldview3.png` | Seed observation support. |
| `figure_4_4b` | Figure 4.4B - NOAA-published March 14 WorldView-3 analysis map | `output/Phase 3B March13-14 Final Output/publication/observations/figure_4_4B_noaa_mar14_worldview3.png` | Target observation support. |
| `figure_4_4c` | Figure 4.4C - ArcGIS overlay of March 13 and March 14 observed oil-spill extents | `output/Phase 3B March13-14 Final Output/publication/observations/figure_4_4C_arcgis_mar13_mar14_observed_overlay.png` | Observation-pair context only. |
| `figure_4_5` | Figure 4.5 - Mindoro same-case OpenDrift–PyGNOME spatial comparator board | `output/Phase 3B March13-14 Final Output/publication/comparator_pygnome/Figure_4_5_Mindoro_TrackA_OpenDrift_PyGNOME_spatial_board.png`; `mindoro_crossmodel_board.png` | PyGNOME comparator-only. |
| `figure_4_6` | Figure 4.6 - Mindoro same-case OpenDrift–PyGNOME comparator mean FSS summary | `march13_14_reinit_crossmodel_model_ranking.csv`; optional final-layout chart path in the registry | Comparator-only. |
| `figure_4_7` | Figure 4.7 - DWH observed, deterministic, mask_p50, and PyGNOME event-corridor board | `output/Phase 3C DWH Final Output/publication/comparator_pygnome/dwh_2010-05-21_to_2010-05-23_eventcorridor_observed_deterministic_mask_p50_pygnome_board.png` | DWH external transfer; PyGNOME comparator-only. |
| `figure_4_8` | Figure 4.8 - DWH 24 h, 48 h, and 72 h mask_p50, mask_p90, and PyGNOME overview board | `output/Phase 3C DWH Final Output/publication/comparator_pygnome/dwh_24h_48h_72h_mask_p50_mask_p90_vs_pygnome_three_row_overview_board.png` | `mask_p90` conservative support only. |
| `figure_4_9` | Figure 4.9 - DWH 48 h observed, deterministic, mask_p50, and PyGNOME board | `output/Phase 3C DWH Final Output/publication/comparator_pygnome/dwh_2010-05-22_48h_observed_deterministic_mask_p50_pygnome_board.png` | DWH external transfer; PyGNOME comparator-only. |
| `figure_4_10` | Figure 4.10 - CASE_2016-09-01 secondary drifter-track benchmark map panel | `output/2016_drifter_benchmark/case_boards/CASE_2016-09-01_drifter_track_benchmark.png` | Secondary support only. |
| `figure_4_11` | Figure 4.11 - CASE_2016-09-06 secondary drifter-track benchmark map panel | `output/2016_drifter_benchmark/case_boards/CASE_2016-09-06_drifter_track_benchmark.png` | Secondary support only. |
| `figure_4_12` | Figure 4.12 - CASE_2016-09-17 secondary drifter-track benchmark map panel | `output/2016_drifter_benchmark/case_boards/CASE_2016-09-17_drifter_track_benchmark.png` | Secondary support only. |
| `figure_4_13` | Figure 4.13 - Legacy 2016 OpenDrift-versus-PyGNOME overall mean FSS chart | `output/prototype_2016_pygnome_similarity/qa_prototype_pygnome_scorecard.png`; `prototype_pygnome_fss_by_case_window.csv` | Legacy comparator support only. |

Stale mappings removed: Figure 4.1 is no longer generic study-box context, and Figure 4.2 is no longer generic geography/domain reference.

## Reviewer-Facing Output Roots

| Root | Role |
| --- | --- |
| `output/phase1_mindoro_focus_pre_spill_2016_2023` | Focused Phase 1 transport provenance. |
| `output/Phase 3B March13-14 Final Output` | Curated Primary Mindoro March 13–14 package. |
| `output/Phase 3C DWH Final Output` | Curated DWH external-transfer package. |
| `output/phase4/CASE_MINDORO_RETRO_2023` | Mindoro oil-type/shoreline support context. |
| `output/2016_drifter_benchmark` | Secondary 2016 direct drifter-track support. |
| `output/2016 Legacy Runs FINAL Figures` | Legacy 2016 comparator/support figures. |
| `output/final_validation_package` | Final validation summaries and guardrails. |
| `output/final_reproducibility_package` | Final reproducibility manifests and catalogs. |
| `output/figure_package_publication` | Publication figure package. |

## Validation

Run:

```powershell
python scripts/validate_paper_to_output_registry.py
```

This validator checks tracked repository paths only. It does not run scientific workflows, download data, or reinterpret stored values.
