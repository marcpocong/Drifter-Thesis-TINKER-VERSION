# Final Reproducibility Summary

This package synchronizes launcher/menu behavior, documentation, and reproducibility indexes around the current local repo state without rerunning the expensive scientific branches by default.

## Launcher Entrypoint

- PowerShell entrypoint: `./start.ps1 -List -NoPause`
- Source-of-truth launcher matrix: `config/launcher_matrix.json`
- Safe read-only launcher IDs: `mindoro_march6_recovery_sensitivity`, `phase1_audit`, `phase2_audit`, `final_validation_package`, `phase5_sync`, `trajectory_gallery`, `trajectory_gallery_panel`, `figure_package_publication`, `prototype_legacy_final_figures`

## Phase Status Highlights

- `phase1` / `phase1_regional_baseline`: Architecture audited; the final 2016-2022 production rerun is still needed.
- `phase2` / `phase2_machine_readable_forecast`: Scientifically usable as implemented, but not yet frozen.
- `phase3a` / `A`: Same-case comparator-only support track attached to B1 on the promoted March 14 target. fss_1km=0.0000, fss_3km=0.0441, fss_5km=0.1371, fss_10km=0.2490
- `phase3b` / `B1`: Promoted primary validation; focused 2016-2023 Mindoro drifter rerun selected the same cmems_era5 recipe; reportable now but not fully frozen. fss_1km=0.0000, fss_3km=0.0441, fss_5km=0.1371, fss_10km=0.2490
- `phase3b` / `B2`: B2 legacy honesty reference; not the promoted primary row. fss_1km=0.0000, fss_3km=0.0000, fss_5km=0.0000, fss_10km=0.0000
- `phase3b` / `B3`: B3 legacy broader-support context; not a primary row. fss_1km=0.1722, fss_3km=0.2004, fss_5km=0.2166, fss_10km=0.2438
- `phase3c` / `C1`: Main DWH deterministic transfer-validation track. fss_1km=0.5033, fss_3km=0.5523, fss_5km=0.5700, fss_10km=0.6018
- `phase3c` / `C2`: DWH ensemble extension on the same truth masks; p50 preferred, p90 support-only. fss_1km=0.4997, fss_3km=0.5299, fss_5km=0.5467, fss_10km=0.5790
- `phase3c` / `C3`: DWH cross-model comparator; PyGNOME not truth. fss_1km=0.3197, fss_3km=0.3495, fss_5km=0.3689, fss_10km=0.4068
- `phase4` / `mindoro_phase4`: Phase 4 OpenDrift/OpenOil-only interpretation; inherited-provisional.
- `phase5` / `phase5_sync`: Launcher, docs, and reproducibility packaging are synchronized around the current repo state without rerunning expensive science.
- `phase5` / `phase5_read_only_dashboard`: The local dashboard is now available as a read-only Phase 5 exploration layer built on the current packaging outputs and publication-grade figures.

## Packaging Sync Scope

- Existing scientific Mindoro and DWH outputs were reused and not recomputed here.
- The existing `output/final_validation_package/` bundle was reused rather than rebuilt from scratch.
- Mindoro keeps the frozen base case definition in `config/case_mindoro_retro_2023.yaml` and records the promoted March 13 -> March 14 Phase 3B primary row through the separate `config/case_mindoro_retro_2023_phase3b_primary_validation_amendment.yaml` amendment file.
- The thesis-facing B1 title is `Phase 3B Observation-Based Spatial Validation Using Public Mindoro Spill Extents`, and the separate focused 2016-2023 Mindoro drifter rerun selected the same `cmems_era5` recipe without rewriting the stored B1 raw provenance.
- The curated read-only B1 export now lives under `output/Phase 3B March13-14 Final Output` and packages the publication figures, canonical scientific source PNGs, summary CSV, decision note, and local manifest.
- The curated read-only DWH export now lives under `output/Phase 3C DWH Final Output` and packages the Phase 3C observation context figures, deterministic baseline figures, ensemble extension figures, PyGNOME comparator figures, canonical scientific source PNGs, and summary/manifests without rerunning science.
- `prototype_2016` is cataloged here as a legacy Phase 1 / 2 / 3A / 4 support lane, with Phase 5 available only through the separate read-only sync entry.
- Mindoro Phase 4 now participates in the reproducibility/package layer via the current `phase4_run_manifest.json` and verdict bundle.
- The static `output/trajectory_gallery/` bundle now participates in the reproducibility/package layer as a read-only technical figure set.
- The static `output/trajectory_gallery_panel/` bundle now participates in the reproducibility/package layer as the polished panel-ready figure pack.
- The static `output/figure_package_publication/` bundle now participates in the reproducibility/package layer as the canonical publication-grade presentation package.
- The new `ui/` layer now participates as a read-only local dashboard over the existing packaged outputs and figures rather than as a rerun control surface.

## Key Artifacts

- Phase status registry: `output/final_reproducibility_package/final_phase_status_registry.csv`
- Reproducibility manifest: `output/final_reproducibility_package/final_reproducibility_manifest.json`
- Packaging sync memo: `output/final_reproducibility_package/phase5_packaging_sync_memo.md`
- Launcher guide: `output/final_reproducibility_package/launcher_user_guide.md`
- UI guide: `docs/UI_GUIDE.md`
- Trajectory gallery manifest: `output/trajectory_gallery/trajectory_gallery_manifest.json`
- Panel gallery manifest: `output/trajectory_gallery_panel/panel_figure_manifest.json`
- Publication figure manifest: `output/figure_package_publication/publication_figure_manifest.json`
- Curated B1 final-output export: `output/Phase 3B March13-14 Final Output`
- Curated DWH final-output export: `output/Phase 3C DWH Final Output`
