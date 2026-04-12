# Trajectory Gallery Index

This gallery is built from existing outputs, manifests, rasters, and NetCDFs only. No expensive scientific branch was rerun to generate these figures.

- Gallery root: `output/trajectory_gallery`
- Figure count: `20`

## A. Mindoro deterministic track/path visuals

- `case_mindoro_retro_2023__phase2_official__opendrift__deterministic_track_map__2023_03_03_to_2023_03_06__sampled_particle_paths.png`: case=`CASE_MINDORO_RETRO_2023`, model=`opendrift`, run_type=`deterministic_track_map`, date=`2023-03-03_to_2023-03-06`, scenario=`n/a`, panel_ready=`true`.
  Note: Generated from the stored Mindoro deterministic OpenDrift control NetCDF using sampled particle tracks and a centroid path.

## B. Mindoro ensemble sampled-member trajectories

- `case_mindoro_retro_2023__phase2_official__opendrift__ensemble_sampled_member_centroids__2023_03_03_to_2023_03_06__member_centroid_paths.png`: case=`CASE_MINDORO_RETRO_2023`, model=`opendrift`, run_type=`ensemble_sampled_member_centroids`, date=`2023-03-03_to_2023-03-06`, scenario=`n/a`, panel_ready=`true`.
  Note: Generated from stored Mindoro ensemble member NetCDFs using sampled member centroid paths rather than every particle track.

## C. Mindoro centroid/corridor/hull views

- `case_mindoro_retro_2023__phase2_phase3b__opendrift__corridor_hull_view__2023_03_06__p50_p90_hull_overlay.png`: case=`CASE_MINDORO_RETRO_2023`, model=`opendrift`, run_type=`corridor_hull_view`, date=`2023-03-06`, scenario=`n/a`, panel_ready=`true`.
  Note: Generated from the stored Mindoro p50 date-composite mask, p90 72 h mask, and sampled member centroid/final-position geometry.

## D. Mindoro March 13 -> March 14 primary-validation overlays

- `case_mindoro_retro_2023__phase3b_reinit_primary__observation__forecast_vs_observation_overlay__2023_03_13_to_2023_03_14__seed_vs_target.png`: case=`CASE_MINDORO_RETRO_2023`, model=`observation`, run_type=`forecast_vs_observation_overlay`, date=`2023-03-13_to_2023-03-14`, scenario=`n/a`, panel_ready=`true`.
  Note: Promoted March 13 seed-versus-March 14 target figure reused as a panel-ready gallery figure.
- `case_mindoro_retro_2023__phase3b_reinit_primary__opendrift__forecast_vs_observation_overlay__2023_03_14__r1_previous_overlay.png`: case=`CASE_MINDORO_RETRO_2023`, model=`opendrift`, run_type=`forecast_vs_observation_overlay`, date=`2023-03-14`, scenario=`n/a`, panel_ready=`true`.
  Note: Promoted March 14 R1 previous reinit overlay reused from the completed reinit QA bundle.

## E. Mindoro March 13 -> March 14 cross-model comparison maps

- `case_mindoro_retro_2023__phase3a_reinit_crossmodel__opendrift_vs_pygnome__comparison_overlay__2023_03_14__pygnome_crossmodel_overlay.png`: case=`CASE_MINDORO_RETRO_2023`, model=`opendrift_vs_pygnome`, run_type=`comparison_overlay`, date=`2023-03-14`, scenario=`n/a`, panel_ready=`true`.
  Note: Promoted March 14 PyGNOME comparator overlay reused from the stored comparator bundle.
- `case_mindoro_retro_2023__phase3a_reinit_crossmodel__opendrift_vs_pygnome__comparison_overlay__2023_03_14__r1_previous_crossmodel_overlay.png`: case=`CASE_MINDORO_RETRO_2023`, model=`opendrift_vs_pygnome`, run_type=`comparison_overlay`, date=`2023-03-14`, scenario=`n/a`, panel_ready=`true`.
  Note: Promoted March 14 R1 previous reinit cross-model overlay reused from the stored comparator outputs.

## F. DWH deterministic track/path visuals

- `case_dwh_retro_2010_72h__phase3c_external_case_run__opendrift__deterministic_track_map__2010_05_20_to_2010_05_23__sampled_particle_paths.png`: case=`CASE_DWH_RETRO_2010_72H`, model=`opendrift`, run_type=`deterministic_track_map`, date=`2010-05-20_to_2010-05-23`, scenario=`n/a`, panel_ready=`true`.
  Note: Generated from the stored DWH deterministic OpenDrift control NetCDF with the public observation event-corridor mask shown as context.

## G. DWH ensemble p50/p90 overlays

- `case_dwh_retro_2010_72h__phase3c_ensemble__opendrift__ensemble_overlay__2010_05_21_to_2010_05_23__eventcorridor_overlay.png`: case=`CASE_DWH_RETRO_2010_72H`, model=`opendrift`, run_type=`ensemble_overlay`, date=`2010-05-21_to_2010-05-23`, scenario=`n/a`, panel_ready=`true`.
  Note: Existing DWH event-corridor p50/p90 overlay reused from the stored ensemble comparison QA bundle.
- `case_dwh_retro_2010_72h__phase3c_ensemble__opendrift__ensemble_overlay__2010_05_21_to_2010_05_23__p50_p90_overlays.png`: case=`CASE_DWH_RETRO_2010_72H`, model=`opendrift`, run_type=`ensemble_overlay`, date=`2010-05-21_to_2010-05-23`, scenario=`n/a`, panel_ready=`true`.
  Note: Existing DWH ensemble overlay reused from the stored Phase 3C ensemble comparison bundle.

## H. DWH OpenDrift vs PyGNOME comparison maps

- `case_dwh_retro_2010_72h__phase3c_pygnome_comparator__opendrift_vs_pygnome__comparison_overlay__2010_05_21_to_2010_05_23__eventcorridor_overlay.png`: case=`CASE_DWH_RETRO_2010_72H`, model=`opendrift_vs_pygnome`, run_type=`comparison_overlay`, date=`2010-05-21_to_2010-05-23`, scenario=`n/a`, panel_ready=`true`.
  Note: Existing DWH OpenDrift vs PyGNOME event-corridor comparison reused from the comparator bundle.
- `case_dwh_retro_2010_72h__phase3c_pygnome_comparator__opendrift_vs_pygnome__comparison_overlay__2010_05_21_to_2010_05_23__per_date_overlays.png`: case=`CASE_DWH_RETRO_2010_72H`, model=`opendrift_vs_pygnome`, run_type=`comparison_overlay`, date=`2010-05-21_to_2010-05-23`, scenario=`n/a`, panel_ready=`true`.
  Note: Existing DWH OpenDrift vs PyGNOME per-date overlay reused from the comparator bundle.

## I. Mindoro Phase 4 oil-budget figures

- `case_mindoro_retro_2023__phase4__openoil__oil_budget_summary__2023_03_03_to_2023_03_06__all_scenarios__mass_budget_comparison.png`: case=`CASE_MINDORO_RETRO_2023`, model=`openoil`, run_type=`oil_budget_summary`, date=`2023-03-03_to_2023-03-06`, scenario=`all_scenarios`, panel_ready=`true`.
  Note: Existing Mindoro Phase 4 mass-budget comparison figure reused from the stored Phase 4 bundle.
- `case_mindoro_retro_2023__phase4__openoil__oil_budget_timeseries__2023_03_03_to_2023_03_06__fixed_base_medium_heavy_proxy__mass_budget_timeseries.png`: case=`CASE_MINDORO_RETRO_2023`, model=`openoil`, run_type=`oil_budget_timeseries`, date=`2023-03-03_to_2023-03-06`, scenario=`fixed_base_medium_heavy_proxy`, panel_ready=`true`.
  Note: Existing scenario-specific Phase 4 mass-budget figure reused from the stored Phase 4 outputs.
- `case_mindoro_retro_2023__phase4__openoil__oil_budget_timeseries__2023_03_03_to_2023_03_06__heavier_oil__mass_budget_timeseries.png`: case=`CASE_MINDORO_RETRO_2023`, model=`openoil`, run_type=`oil_budget_timeseries`, date=`2023-03-03_to_2023-03-06`, scenario=`heavier_oil`, panel_ready=`true`.
  Note: Existing scenario-specific Phase 4 mass-budget figure reused from the stored Phase 4 outputs.
- `case_mindoro_retro_2023__phase4__openoil__oil_budget_timeseries__2023_03_03_to_2023_03_06__lighter_oil__mass_budget_timeseries.png`: case=`CASE_MINDORO_RETRO_2023`, model=`openoil`, run_type=`oil_budget_timeseries`, date=`2023-03-03_to_2023-03-06`, scenario=`lighter_oil`, panel_ready=`true`.
  Note: Existing scenario-specific Phase 4 mass-budget figure reused from the stored Phase 4 outputs.
- `case_mindoro_retro_2023__phase4__openoil__oil_type_comparison__2023_03_03_to_2023_03_06__all_scenarios__oiltype_comparison.png`: case=`CASE_MINDORO_RETRO_2023`, model=`openoil`, run_type=`oil_type_comparison`, date=`2023-03-03_to_2023-03-06`, scenario=`all_scenarios`, panel_ready=`true`.
  Note: Existing Mindoro Phase 4 oil-type comparison QA figure reused from the stored Phase 4 bundle.

## J. Mindoro Phase 4 shoreline-arrival / shoreline-segment impact figures

- `case_mindoro_retro_2023__phase4__openoil__shoreline_arrival_summary__2023_03_03_to_2023_03_06__all_scenarios__scenario_arrival_bars.png`: case=`CASE_MINDORO_RETRO_2023`, model=`openoil`, run_type=`shoreline_arrival_summary`, date=`2023-03-03_to_2023-03-06`, scenario=`all_scenarios`, panel_ready=`true`.
  Note: Generated from the stored Mindoro Phase 4 shoreline arrival summary without rerunning the weathering workflow.
- `case_mindoro_retro_2023__phase4__openoil__shoreline_impact_summary__2023_03_03_to_2023_03_06__all_scenarios__shoreline_impacts.png`: case=`CASE_MINDORO_RETRO_2023`, model=`openoil`, run_type=`shoreline_impact_summary`, date=`2023-03-03_to_2023-03-06`, scenario=`all_scenarios`, panel_ready=`true`.
  Note: Existing Mindoro Phase 4 shoreline impact QA figure reused from the stored Phase 4 bundle.
- `case_mindoro_retro_2023__phase4__openoil__shoreline_segment_impact_map__2023_03_03_to_2023_03_06__all_scenarios__segment_midpoint_impacts.png`: case=`CASE_MINDORO_RETRO_2023`, model=`openoil`, run_type=`shoreline_segment_impact_map`, date=`2023-03-03_to_2023-03-06`, scenario=`all_scenarios`, panel_ready=`true`.
  Note: Generated from the stored Phase 4 shoreline segment registry and canonical shoreline segment geometry.
