# Paper-To-Repo Crosswalk

This crosswalk maps final-paper items to stored outputs, configs, docs, or archive notes in this repository. The machine-readable registry is [`config/paper_to_output_registry.yaml`](../config/paper_to_output_registry.yaml), and the validator is [`scripts/validate_paper_to_output_registry.py`](../scripts/validate_paper_to_output_registry.py).

This is a stored-output/config/doc registry. It does not run science, refetch data, or create new claims.

Missing or placeholder figure files must be inserted or regenerated from stored outputs before the final package. Missing files do not create new claims. Non-promoted archive, experimental, legacy, and comparator-only rows follow [ARCHIVE_GOVERNANCE.md](ARCHIVE_GOVERNANCE.md).

## Primary Evidence

| Paper item | Paper label | Trace target | Claim boundary |
| --- | --- | --- | --- |
| `table_3_7` | Table 3.7 - Focused Phase 1 design/window | `output/phase1_mindoro_focus_pre_spill_2016_2023/phase1_production_manifest.json`; `phase1_accepted_segment_registry.csv` | Transport provenance only. |
| `table_3_8` | Table 3.8 - Recipe family/ranking policy | `phase1_recipe_ranking.csv`; `phase1_gfs_month_preflight.csv`; `config/recipes.yaml` | Recipe selection support only. |
| `table_4_2` | Table 4.2 - Accepted segments/subset | `phase1_accepted_segment_registry.csv`; `phase1_ranking_subset_registry.csv` | Counts describe provenance corpus. |
| `table_4_3` | Table 4.3 - Recipe ranking values | `phase1_recipe_ranking.csv`; `phase1_official_adoption_decision.md` | NCS ranking supports B1 recipe provenance. |
| `figure_4_1` | Figure 4.1 - Study-box context | `output/figure_package_publication/*thesis_study_boxes_reference.png`; `publication_figure_registry.csv` | Geographic context only. |
| `figure_4_2` | Figure 4.2 - Focused/Mindoro geography | `output/figure_package_publication/*focused_phase1_box_geography_reference.png`; `*mindoro_case_domain_geography_reference.png` | Geographic provenance context only. |
| `output_phase1_mindoro_focus_root` | Focused Phase 1 output root | `output/phase1_mindoro_focus_pre_spill_2016_2023` | Required stored provenance root. |
| `table_3_11` | Table 3.11 - Mindoro deterministic product setup | `output/CASE_MINDORO_RETRO_2023/forecast/forecast_manifest.json` | Standardized product, not validation by itself. |
| `table_3_12` | Table 3.12 - Mindoro ensemble/probability products | `output/CASE_MINDORO_RETRO_2023/ensemble/ensemble_manifest.json`; `prob_presence`; `mask_p50`; `mask_p90` | Preserve p50/p90 semantics. |
| `table_4_5` | Table 4.5 - Mindoro B1 scorecard | `march13_14_reinit_summary.csv`; `march13_14_reinit_fss_by_window.csv` | Only main Philippine public-observation validation claim. |
| `table_4_6` | Table 4.6 - B1 branch diagnostics | `march13_14_reinit_branch_survival_summary.csv`; `march13_14_reinit_diagnostics.csv` | Scoreable branch, not exact-grid success. |
| `table_4_7` | Table 4.7 - B1 FSS/overlap interpretation | `march13_14_reinit_fss_by_window.csv`; `march13_14_reinit_summary.csv` | Supports coastal-neighborhood usefulness, not exact 1 km overlap. |
| `figure_4_4` | Figure 4.4 - B1 primary board | `output/Phase 3B March13-14 Final Output/publication/opendrift_primary/mindoro_primary_validation_board.png` | Visualization of bounded B1 claim. |
| `figure_4_4a` | Figure 4.4A - March 13 public observation | `output/Phase 3B March13-14 Final Output/publication/observations/figure_4_4A_noaa_mar13_worldview3.png` | Seed observation support. |
| `figure_4_4b` | Figure 4.4B - March 14 public observation | `output/Phase 3B March13-14 Final Output/publication/observations/figure_4_4B_noaa_mar14_worldview3.png` | Target observation support. |
| `figure_4_4c` | Figure 4.4C - March 13/14 observed overlay | `output/Phase 3B March13-14 Final Output/publication/observations/figure_4_4C_arcgis_mar13_mar14_observed_overlay.png` | Observation-pair context only. |
| `output_mindoro_case_root` | Mindoro canonical case root | `output/CASE_MINDORO_RETRO_2023` | Contains primary, support, and archive branches. |
| `output_phase3b_final_export` | Curated B1 export root | `output/Phase 3B March13-14 Final Output` | Read-only B1 export layer. |

## Comparator Support

| Paper item | Paper label | Trace target | Claim boundary |
| --- | --- | --- | --- |
| `table_4_8` | Table 4.8 - Mindoro Track A comparator | `march13_14_reinit_crossmodel_summary.csv`; `march13_14_reinit_crossmodel_model_ranking.csv` | Comparator-only. |
| `figure_4_5` | Figure 4.5 - Track A spatial board | `Figure_4_5_Mindoro_TrackA_OpenDrift_PyGNOME_spatial_board.png`; `mindoro_crossmodel_board.png` | PyGNOME is never observation truth. |
| `figure_4_6` | Figure 4.6 - Track A overlays/ranking | `march14_crossmodel_r1_overlay.png`; `march14_crossmodel_pygnome_overlay.png`; ranking CSV | Stored overlays plus numeric ranking; no new claim if a dedicated bar-chart file is absent. |
| `output_track_a_comparator_root` | Track A comparator roots | `output/CASE_MINDORO_RETRO_2023/..._pygnome_comparison`; `output/Phase 3B March13-14 Final Output/summary/comparator_pygnome` | Comparator support only. |

## External Transfer

| Paper item | Paper label | Trace target | Claim boundary |
| --- | --- | --- | --- |
| `table_3_13` | Table 3.13 - DWH setup/forcing | `phase3c_external_case_setup_manifest.json`; `dwh_scientific_prepared_forcing_manifest.json` | External transfer validation only. |
| `table_3_14` | Table 3.14 - DWH product definitions | DWH deterministic, ensemble, and comparator memos under `output/Phase 3C DWH Final Output/summary` | Not Mindoro recalibration. |
| `table_4_9` | Table 4.9 - DWH event-corridor mean FSS | `phase3c_main_scorecard.csv`; DWH FSS-by-window CSVs | DWH observed masks are scoring reference. |
| `table_4_10` | Table 4.10 - DWH geometry diagnostics | `phase3c_eventcorridor_summary.csv`; `phase3c_ensemble_eventcorridor_summary.csv`; PyGNOME comparator summary | PyGNOME remains comparator-only. |
| `figure_4_7` | Figure 4.7 - DWH deterministic board | `dwh_24h_48h_72h_deterministic_footprint_overview_board.png` | DWH deterministic transfer-validation visual. |
| `figure_4_8` | Figure 4.8 - DWH deterministic/p50/p90 board | `dwh_2010-05-21_to_2010-05-23_eventcorridor_observed_deterministic_mask_p50_mask_p90_board.png` | p90 is conservative support/comparison only. |
| `figure_4_9` | Figure 4.9 - DWH PyGNOME board | `dwh_2010-05-21_to_2010-05-23_eventcorridor_observed_deterministic_mask_p50_pygnome_board.png` | Comparator-only. |
| `output_dwh_case_root` | DWH canonical case root | `output/CASE_DWH_RETRO_2010_72H` | Canonical scientific root. |
| `output_phase3c_final_export` | Curated DWH export root | `output/Phase 3C DWH Final Output` | Read-only transfer-validation export layer. |

## Support Context

| Paper item | Paper label | Trace target | Claim boundary |
| --- | --- | --- | --- |
| `table_3_15` | Table 3.15 - Oil-type/shoreline setup | `phase4_oil_budget_summary.csv`; `phase4_oiltype_comparison.csv`; `phase4_shoreline_arrival.csv` | Support/context only. |
| `figure_appendix_f_oil_budget` | Appendix F oil-budget figure | `mass_budget_comparison.png`; `qa_phase4_oiltype_comparison.png`; publication oil-budget board | Not observational validation. |
| `figure_appendix_f_shoreline` | Appendix F shoreline figure | `qa_phase4_shoreline_impacts.png`; `phase4_shoreline_segments.csv`; publication shoreline board | Does not change B1. |
| `appendix_f` | Appendix F support package | `phase4_final_verdict.md`; `phase4_methodology_sync_memo.md`; `docs/PHASE4_COMPARATOR_DECISION.md` | Fixed-base medium-heavy proxy is QC flagged. |
| `output_phase4_mindoro_root` | Mindoro Phase 4 root | `output/phase4/CASE_MINDORO_RETRO_2023` | Support/context only. |

## Secondary Support

| Paper item | Paper label | Trace target | Claim boundary |
| --- | --- | --- | --- |
| `table_4_11` | Table 4.11 - 2016 drifter benchmark | `output/2016_drifter_benchmark/scorecard.csv`; `manifest.json` | Not public-spill validation. |
| `table_4_11a` | Table 4.11A - 2016 legacy FSS | `prototype_pygnome_fss_by_case_window.csv`; per-case `phase3a_fss_by_time_window.csv` | Legacy FSS support only. |
| `table_4_11b` | Table 4.11B - 2016 similarity support | `prototype_pygnome_similarity_by_case.csv`; `prototype_pygnome_case_registry.csv` | Comparator support only. |
| `table_4_12` | Table 4.12 - Legacy fate/shoreline support | `prototype_2016_phase4_registry.csv`; `prototype_2016_phase4_pygnome_comparator_registry.csv` | Legacy support only. |
| `figure_4_10` | Figure 4.10 - Drifter-track benchmark boards | `output/2016_drifter_benchmark/case_boards/*.png`; legacy triptych board | Secondary support only. |
| `figure_4_11` | Figure 4.11 - Drifter versus ensemble support | legacy drifter-vs-ensemble triptych and per-case boards | Not a B1/DWH replacement. |
| `figure_4_12` | Figure 4.12 - OpenDrift/PyGNOME legacy boards | legacy OpenDrift-vs-PyGNOME triptych and prototype boards | PyGNOME comparator-only. |
| `figure_4_13` | Figure 4.13 - Legacy fate/shoreline support | legacy Phase 4 mass-budget and shoreline figures | Repository/archive support only. |
| `output_2016_drifter_benchmark_root` | 2016 drifter benchmark root | `output/2016_drifter_benchmark` | Secondary support only. |
| `output_legacy_fss_roots` | 2016 legacy FSS roots | `output/prototype_2016_pygnome_similarity`; `output/2016 Legacy Runs FINAL Figures` | Legacy support only. |

## Governance

| Paper item | Paper label | Trace target | Claim boundary |
| --- | --- | --- | --- |
| `table_3_16` | Table 3.16 - Launcher/reproducibility governance | `config/launcher_matrix.json`; `docs/COMMAND_MATRIX.md`; `docs/LAUNCHER_USER_GUIDE.md` | Governance only. |
| `table_3_17` | Table 3.17 - Archive governance | `config/archive_registry.yaml`; `docs/ARCHIVE_GOVERNANCE.md`; `docs/MINDORO_VALIDATION_ARCHIVE_DECISION.md` | Preservation is not promotion. |
| `appendix_g` | Appendix G - Reproducibility/panel package | `output/final_reproducibility_package`; `docs/PANEL_REVIEW_GUIDE.md`; `docs/FINAL_PAPER_ALIGNMENT.md` | Read-only/package governance only. |
| `appendix_h` | Appendix H - UI and crosswalk layer | `ui`; `docs/UI_GUIDE.md`; this crosswalk; registry YAML | Presentation/governance surface only. |
| `data_sources_registry` | Data sources and provenance | `docs/DATA_SOURCES.md`; `config/data_sources.yaml` | Provenance only. |

## Validation

Run:

```powershell
python scripts/validate_paper_to_output_registry.py
```

The validator writes:

- `output/paper_to_output_registry_validation/paper_to_output_registry_validation.json`
- `output/paper_to_output_registry_validation/paper_to_output_registry_validation.md`

It fails only for missing required primary-evidence paths. Optional appendix, support, and figure placeholder paths are warnings.
