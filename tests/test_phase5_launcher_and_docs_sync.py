import json
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from src.services.phase5_launcher_and_docs_sync import (
    Phase5LauncherAndDocsSyncService,
    load_launcher_matrix,
)


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


class Phase5LauncherAndDocsSyncTests(unittest.TestCase):
    def test_read_json_tolerates_utf8_bom(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = root / "bom.json"
            path.write_text("\ufeff{\n  \"status\": \"ok\"\n}\n", encoding="utf-8")

            from src.services.phase5_launcher_and_docs_sync import _read_json

            payload = _read_json(path)

            self.assertEqual(payload["status"], "ok")

    def test_load_launcher_matrix_reads_safe_defaults(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            matrix_path = root / "config" / "launcher_matrix.json"
            matrix_path.parent.mkdir(parents=True, exist_ok=True)
            _write_json(
                matrix_path,
                {
                    "catalog_version": "test",
                    "entrypoint": "start.ps1",
                    "categories": [
                        {"category_id": "read_only_packaging_help_utilities", "label": "Read-only", "description": "Safe"},
                    ],
                    "entries": [
                        {
                            "entry_id": "phase5_sync",
                            "menu_order": 1,
                            "category_id": "read_only_packaging_help_utilities",
                            "workflow_mode": "mindoro_retro_2023",
                            "label": "Phase 5 sync",
                            "description": "Read-only sync",
                            "rerun_cost": "cheap_read_only",
                            "safe_default": True,
                            "steps": [{"phase": "phase5_launcher_and_docs_sync", "service": "pipeline", "description": "Sync"}],
                        }
                    ],
                },
            )

            matrix = load_launcher_matrix(matrix_path)

            self.assertEqual(matrix["entrypoint"], "start.ps1")
            self.assertEqual(matrix["entries"][0]["entry_id"], "phase5_sync")
            self.assertTrue(matrix["entries"][0]["safe_default"])

    def test_phase5_service_generates_outputs_and_handles_missing_optional_artifacts(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "config").mkdir(parents=True, exist_ok=True)
            (root / "docs").mkdir(parents=True, exist_ok=True)
            (root / "logs").mkdir(parents=True, exist_ok=True)

            _write_json(
                root / "config" / "launcher_matrix.json",
                {
                    "catalog_version": "phase5_test_matrix",
                    "entrypoint": "start.ps1",
                    "categories": [
                        {
                            "category_id": "scientific_reportable_tracks",
                            "label": "Scientific / reportable tracks",
                            "description": "Intentional reruns only.",
                        },
                        {
                            "category_id": "read_only_packaging_help_utilities",
                            "label": "Read-only packaging / help utilities",
                            "description": "Safe summary tools.",
                        },
                        {
                            "category_id": "legacy_prototype_tracks",
                            "label": "Legacy prototype tracks",
                            "description": "Regression only.",
                        },
                    ],
                    "entries": [
                        {
                            "entry_id": "mindoro_reportable_core",
                            "menu_order": 10,
                            "category_id": "scientific_reportable_tracks",
                            "workflow_mode": "mindoro_retro_2023",
                            "label": "Mindoro reportable core bundle",
                            "description": "Intentional scientific rerun.",
                            "rerun_cost": "expensive",
                            "safe_default": False,
                            "notes": "Manual scientific rerun only.",
                            "steps": [
                                {"phase": "prep", "service": "pipeline", "description": "Prep"},
                                {"phase": "1_2", "service": "pipeline", "description": "Forecast"},
                            ],
                        },
                        {
                            "entry_id": "phase5_sync",
                            "menu_order": 20,
                            "category_id": "read_only_packaging_help_utilities",
                            "workflow_mode": "mindoro_retro_2023",
                            "label": "Phase 5 launcher/docs/package sync",
                            "description": "Read-only sync.",
                            "rerun_cost": "cheap_read_only",
                            "safe_default": True,
                            "notes": "Safe first run.",
                            "steps": [
                                {
                                    "phase": "phase5_launcher_and_docs_sync",
                                    "service": "pipeline",
                                    "description": "Phase 5 sync",
                                }
                            ],
                        },
                        {
                            "entry_id": "trajectory_gallery",
                            "menu_order": 25,
                            "category_id": "read_only_packaging_help_utilities",
                            "workflow_mode": "mindoro_retro_2023",
                            "label": "Trajectory gallery build",
                            "description": "Read-only gallery.",
                            "rerun_cost": "cheap_read_only",
                            "safe_default": True,
                            "notes": "Panel-facing static figures only.",
                            "steps": [
                                {
                                    "phase": "trajectory_gallery_build",
                                    "service": "pipeline",
                                    "description": "Gallery",
                                }
                            ],
                        },
                        {
                            "entry_id": "trajectory_gallery_panel",
                            "menu_order": 26,
                            "category_id": "read_only_packaging_help_utilities",
                            "workflow_mode": "mindoro_retro_2023",
                            "label": "Trajectory gallery panel polish",
                            "description": "Read-only polished panel gallery.",
                            "rerun_cost": "cheap_read_only",
                            "safe_default": True,
                            "notes": "Panel-ready figure pack only.",
                            "steps": [
                                {
                                    "phase": "trajectory_gallery_panel_polish",
                                    "service": "pipeline",
                                    "description": "Panel gallery",
                                }
                            ],
                        },
                        {
                            "entry_id": "figure_package_publication",
                            "menu_order": 27,
                            "category_id": "read_only_packaging_help_utilities",
                            "workflow_mode": "mindoro_retro_2023",
                            "label": "Publication-grade figure package",
                            "description": "Read-only publication figure package.",
                            "rerun_cost": "cheap_read_only",
                            "safe_default": True,
                            "notes": "Canonical defense and paper presentation figures only.",
                            "steps": [
                                {
                                    "phase": "figure_package_publication",
                                    "service": "pipeline",
                                    "description": "Publication figures",
                                }
                            ],
                        },
                        {
                            "entry_id": "prototype_legacy_bundle",
                            "menu_order": 30,
                            "category_id": "legacy_prototype_tracks",
                            "workflow_mode": "prototype_2016",
                            "label": "Prototype legacy bundle",
                            "description": "Debug only.",
                            "rerun_cost": "moderate",
                            "safe_default": False,
                            "notes": "Not the final study.",
                            "steps": [
                                {"phase": "prep", "service": "pipeline", "description": "Prep"},
                            ],
                        },
                    ],
                    "optional_future_work": [
                        {"work_id": "ui_run_controls", "label": "Interactive UI run controls", "status": "deferred"},
                        {"work_id": "ui_deeper_search_filters", "label": "Deeper artifact search and filtering inside the UI", "status": "deferred"},
                        {"work_id": "dwh_phase4_appendix_pilot", "label": "DWH Phase 4 appendix pilot", "status": "deferred"},
                    ],
                },
            )

            (root / "config" / "case_mindoro_retro_2023.yaml").write_text(
                "\n".join(
                    [
                        "case_id: CASE_MINDORO_RETRO_2023",
                        "workflow_mode: mindoro_retro_2023",
                        "mode_label: Mindoro retro 2023",
                        "description: Main Philippine spill case",
                        "notes:",
                        "  - Mindoro main-text case",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            (root / "config" / "case_dwh_retro_2010_72h.yaml").write_text(
                "\n".join(
                    [
                        "case_id: CASE_DWH_RETRO_2010_72H",
                        "workflow_mode: dwh_retro_2010",
                        "mode_label: DWH retro 2010",
                        "description: External transfer-validation case",
                        "notes:",
                        "  - DWH appendix and transfer validation",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )
            for rel_path in (
                "config/settings.yaml",
                "config/phase1_baseline_selection.yaml",
                "config/recipes.yaml",
                "README.md",
                "docs/PHASE_STATUS.md",
                "docs/ARCHITECTURE.md",
                "docs/OUTPUT_CATALOG.md",
                "docs/FIGURE_GALLERY.md",
                "docs/QUICKSTART.md",
                "docs/UI_GUIDE.md",
                "docs/COMMAND_MATRIX.md",
                "docs/LAUNCHER_USER_GUIDE.md",
                "start.ps1",
                ".gitignore",
            ):
                path = root / rel_path
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(f"{rel_path}\n", encoding="utf-8")

            final_validation_dir = root / "output" / "final_validation_package"
            final_validation_dir.mkdir(parents=True, exist_ok=True)
            (root / "output" / "CASE_MINDORO_RETRO_2023" / "phase3b").mkdir(parents=True, exist_ok=True)
            (root / "output" / "CASE_DWH_RETRO_2010_72H" / "phase3c_external_case_run").mkdir(parents=True, exist_ok=True)
            (root / "output" / "phase1_finalization_audit").mkdir(parents=True, exist_ok=True)
            (root / "output" / "phase2_finalization_audit").mkdir(parents=True, exist_ok=True)
            (root / "output" / "phase4" / "CASE_MINDORO_RETRO_2023").mkdir(parents=True, exist_ok=True)
            (root / "output" / "trajectory_gallery").mkdir(parents=True, exist_ok=True)
            (root / "output" / "trajectory_gallery_panel").mkdir(parents=True, exist_ok=True)
            (root / "output" / "figure_package_publication").mkdir(parents=True, exist_ok=True)
            (root / "output" / "prototype_2016_pygnome_similarity").mkdir(parents=True, exist_ok=True)
            (root / "output" / "CASE_2016-09-01" / "weathering").mkdir(parents=True, exist_ok=True)
            (root / "output" / "2016 Legacy Runs FINAL Figures" / "publication" / "phase4" / "CASE_2016-09-01").mkdir(parents=True, exist_ok=True)
            (root / "output" / "2016 Legacy Runs FINAL Figures" / "manifests").mkdir(parents=True, exist_ok=True)
            (root / "output" / "2016 Legacy Runs FINAL Figures" / "phase5").mkdir(parents=True, exist_ok=True)

            (root / "output" / "CASE_MINDORO_RETRO_2023" / "phase3b" / "phase3b_summary.csv").write_text(
                "metric,value\nfss_1km,0.0\n",
                encoding="utf-8",
            )
            (root / "output" / "CASE_DWH_RETRO_2010_72H" / "phase3c_external_case_run" / "phase3c_summary.csv").write_text(
                "metric,value\nfss_1km,0.5\n",
                encoding="utf-8",
            )

            for artifact_name in (
                "final_validation_main_table.csv",
                "final_validation_benchmark_table.csv",
                "final_validation_observation_table.csv",
                "final_validation_limitations.csv",
                "final_validation_claims_guardrails.md",
                "final_validation_chapter_sync_memo.md",
                "final_validation_interpretation_memo.md",
                "final_validation_summary.md",
            ):
                (final_validation_dir / artifact_name).write_text(f"{artifact_name}\n", encoding="utf-8")

            final_output_dir = root / "output" / "Phase 3B March13-14 Final Output"
            (final_output_dir / "publication").mkdir(parents=True, exist_ok=True)
            (final_output_dir / "scientific_source_pngs").mkdir(parents=True, exist_ok=True)
            (final_output_dir / "summary").mkdir(parents=True, exist_ok=True)
            (final_output_dir / "README.md").write_text("final output readme\n", encoding="utf-8")
            (final_output_dir / "final_output_manifest.json").write_text("{}\n", encoding="utf-8")
            (final_output_dir / "publication" / "mindoro_primary_validation_board.png").write_text("png\n", encoding="utf-8")
            dwh_final_output_dir = root / "output" / "Phase 3C DWH Final Output"
            (dwh_final_output_dir / "publication" / "observations").mkdir(parents=True, exist_ok=True)
            (dwh_final_output_dir / "manifests").mkdir(parents=True, exist_ok=True)
            (dwh_final_output_dir / "README.md").write_text("dwh final output readme\n", encoding="utf-8")
            (dwh_final_output_dir / "publication" / "observations" / "dwh_2010-05-21_observation_context.png").write_text(
                "png\n", encoding="utf-8"
            )
            (dwh_final_output_dir / "manifests" / "phase3c_final_output_manifest.json").write_text("{}\n", encoding="utf-8")

            _write_json(
                final_validation_dir / "final_validation_manifest.json",
                {
                    "phase": "final_validation_package",
                    "artifacts": {
                        "final_validation_summary": "output/final_validation_package/final_validation_summary.md",
                        "final_validation_main_table": "output/final_validation_package/final_validation_main_table.csv",
                        "phase3b_march13_14_final_output_readme": "output/Phase 3B March13-14 Final Output/README.md",
                        "phase3b_march13_14_final_output_manifest": "output/Phase 3B March13-14 Final Output/final_output_manifest.json",
                        "phase3c_dwh_final_output_readme": "output/Phase 3C DWH Final Output/README.md",
                        "phase3c_dwh_final_output_manifest": "output/Phase 3C DWH Final Output/manifests/phase3c_final_output_manifest.json",
                    },
                    "inputs_preserved": [
                        "output/CASE_MINDORO_RETRO_2023/phase3b/phase3b_summary.csv",
                        "output/CASE_DWH_RETRO_2010_72H/phase3c_external_case_run/phase3c_summary.csv",
                    ],
                    "headlines": {
                        "mindoro_primary_reinit": {"fss_1km": 0.0, "fss_3km": 0.044, "fss_5km": 0.137, "fss_10km": 0.249},
                        "mindoro_legacy_march6": {"fss_1km": 0.0, "fss_3km": 0.0, "fss_5km": 0.0, "fss_10km": 0.0},
                        "mindoro_legacy_broader_support": {"fss_1km": 0.17, "fss_3km": 0.20, "fss_5km": 0.22, "fss_10km": 0.24},
                        "dwh_deterministic_event": {"fss_1km": 0.50, "fss_3km": 0.55, "fss_5km": 0.57, "fss_10km": 0.60},
                        "dwh_ensemble_p50_event": {"fss_1km": 0.49, "fss_3km": 0.53, "fss_5km": 0.55, "fss_10km": 0.58},
                        "dwh_pygnome_event": {"fss_1km": 0.32, "fss_3km": 0.35, "fss_5km": 0.37, "fss_10km": 0.41},
                        "mindoro_crossmodel_top": {"mean_fss": 0.108},
                    },
                    "mindoro_primary_validation_promotion": {
                        "thesis_phase_title": "Phase 3B Observation-Based Spatial Validation Using Public Mindoro Spill Extents",
                        "thesis_phase_subtitle": "Mindoro March 13 -> March 14 public spill-extent reinitialization validation",
                        "shared_imagery_caveat": "Both NOAA products cite 2023-03-12 imagery.",
                        "dual_provenance_confirmation": {
                            "stored_run_selected_recipe": "cmems_era5",
                            "posthoc_phase1_confirmation_selected_recipe": "cmems_era5",
                            "matches_stored_b1_recipe": True,
                        },
                        "final_output_export_dir": "output/Phase 3B March13-14 Final Output",
                    },
                    "phase3b_march13_14_final_output": {
                        "output_dir": "output/Phase 3B March13-14 Final Output",
                    },
                    "dwh_phase3c_freeze": {
                        "base_case_definition_path": "config/case_dwh_retro_2010_72h.yaml",
                        "final_note_path": "docs/DWH_PHASE3C_FINAL.md",
                        "forcing_stack": "HYCOM GOFS 3.1 currents + ERA5 winds + CMEMS wave/Stokes",
                        "final_output_export_dir": "output/Phase 3C DWH Final Output",
                    },
                },
            )
            (final_validation_dir / "final_validation_case_registry.csv").write_text(
                "\n".join(
                    [
                        "case_id,track_id,track_label,status,truth_source,primary_output_dir,reporting_role,main_text_priority,notes",
                        "CASE_MINDORO_RETRO_2023,A,Mindoro March 13 -> March 14 cross-model comparator,complete,March 14 NOAA mask,output/CASE_MINDORO_RETRO_2023/phase3b_extended_public_scored_march13_14_reinit_pygnome_comparison,comparative discussion,secondary,Same-case comparator-support only",
                        "CASE_MINDORO_RETRO_2023,B1,Mindoro March 13 -> March 14 NOAA reinit primary validation,complete,March 14 NOAA mask with March 13 seed polygon,output/CASE_MINDORO_RETRO_2023/phase3b_extended_public_scored_march13_14_reinit,main-text primary validation,primary,Promoted primary row with March 12 imagery caveat",
                        "CASE_MINDORO_RETRO_2023,B2,Mindoro legacy March 6 sparse strict reference,complete,WWF March 6 mask,output/CASE_MINDORO_RETRO_2023/phase3b,legacy reference,secondary,Legacy sparse reference",
                        "CASE_MINDORO_RETRO_2023,B3,Mindoro legacy March 3-6 broader-support reference,complete,public union,output/CASE_MINDORO_RETRO_2023/public_obs_appendix,legacy reference,secondary,Legacy broader-support reference",
                        "CASE_DWH_RETRO_2010_72H,C1,DWH deterministic,complete,DWH public masks,output/CASE_DWH_RETRO_2010_72H/phase3c_external_case_run,main-text scientific result,primary,Transfer validation",
                        "CASE_DWH_RETRO_2010_72H,C2,DWH ensemble,complete,DWH public masks,output/CASE_DWH_RETRO_2010_72H/phase3c_external_case_ensemble_comparison,comparative discussion,primary,Ensemble validation",
                        "CASE_DWH_RETRO_2010_72H,C3,DWH PyGNOME,complete,DWH public masks,output/CASE_DWH_RETRO_2010_72H/phase3c_dwh_pygnome_comparator,comparative discussion,secondary,Comparator only",
                        "CASE_MINDORO_RETRO_2023,appendix_sensitivity,Mindoro appendix sensitivity,reviewed_not_promoted,public masks,output/CASE_MINDORO_RETRO_2023/recipe_sensitivity_r1_multibranch,appendix_only,appendix,Support only",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            _write_json(
                root / "output" / "phase1_finalization_audit" / "phase1_finalization_status.json",
                {
                    "overall_verdict": {
                        "biggest_remaining_blocker": "The accepted/rejected 72 h segment registry is still missing.",
                    }
                },
            )
            _write_json(
                root / "output" / "phase2_finalization_audit" / "phase2_finalization_status.json",
                {
                    "overall_verdict": {
                        "scientifically_frozen": False,
                        "requires_phase1_production_rerun_for_full_freeze": True,
                        "legacy_recipe_drift_leaks_into_official_mode": True,
                        "biggest_remaining_phase2_provisional_item": "Phase 2 is usable but not frozen.",
                    }
                },
            )

            phase4_dir = root / "output" / "phase4" / "CASE_MINDORO_RETRO_2023"
            (phase4_dir / "phase4_oil_budget_summary.csv").write_text("scenario_id,final_surface_pct\nlighter_oil,0.0\n", encoding="utf-8")
            (phase4_dir / "phase4_shoreline_arrival.csv").write_text("scenario_id,shoreline_arrival_generated\nlighter_oil,true\n", encoding="utf-8")
            (phase4_dir / "phase4_final_verdict.md").write_text("Phase 4 verdict\n", encoding="utf-8")
            _write_json(
                phase4_dir / "phase4_run_manifest.json",
                {
                    "phase1_frozen_story_complete": False,
                    "provisional_inherited_from_transport": True,
                    "artifacts": {
                        "phase4_oil_budget_summary": "output/phase4/CASE_MINDORO_RETRO_2023/phase4_oil_budget_summary.csv",
                        "phase4_shoreline_arrival": "output/phase4/CASE_MINDORO_RETRO_2023/phase4_shoreline_arrival.csv",
                        "phase4_final_verdict": "output/phase4/CASE_MINDORO_RETRO_2023/phase4_final_verdict.md",
                    },
                    "overall_verdict": {
                        "scientifically_reportable_now": True,
                        "provisional_inherited_from_transport": True,
                        "biggest_remaining_phase4_blocker": "Phase 1 is still not fully frozen.",
                    },
                },
            )
            (root / "output" / "trajectory_gallery" / "trajectory_gallery_manifest.json").write_text(
                "{}\n",
                encoding="utf-8",
            )
            (root / "output" / "trajectory_gallery" / "trajectory_gallery_index.csv").write_text(
                "figure_id,relative_path\nsample,output/trajectory_gallery/sample.png\n",
                encoding="utf-8",
            )
            (root / "output" / "trajectory_gallery_panel" / "panel_figure_manifest.json").write_text(
                "{}\n",
                encoding="utf-8",
            )
            (root / "output" / "trajectory_gallery_panel" / "panel_figure_registry.csv").write_text(
                "figure_id,relative_path\nsample_panel,output/trajectory_gallery_panel/sample_panel.png\n",
                encoding="utf-8",
            )
            (root / "output" / "figure_package_publication" / "publication_figure_manifest.json").write_text(
                "{}\n",
                encoding="utf-8",
            )
            (root / "output" / "figure_package_publication" / "publication_figure_registry.csv").write_text(
                "figure_id,relative_path\nsample_publication,output/figure_package_publication/sample_publication.png\n",
                encoding="utf-8",
            )
            (root / "output" / "prototype_2016_pygnome_similarity" / "prototype_pygnome_similarity_manifest.json").write_text(
                "{}\n",
                encoding="utf-8",
            )
            (root / "output" / "prototype_2016_pygnome_similarity" / "prototype_pygnome_similarity_summary.md").write_text(
                "prototype summary\n",
                encoding="utf-8",
            )
            (root / "output" / "prototype_2016_pygnome_similarity" / "figures").mkdir(parents=True, exist_ok=True)
            (root / "output" / "prototype_2016_pygnome_similarity" / "figures" / "prototype_single.png").write_text(
                "png\n",
                encoding="utf-8",
            )
            (root / "output" / "CASE_2016-09-01" / "weathering" / "budget_light.csv").write_text(
                "hour,surface_pct\n0,100.0\n",
                encoding="utf-8",
            )
            (root / "output" / "2016 Legacy Runs FINAL Figures" / "README.md").write_text(
                "prototype legacy package\n",
                encoding="utf-8",
            )
            (root / "output" / "2016 Legacy Runs FINAL Figures" / "publication" / "phase4" / "CASE_2016-09-01" / "mass_budget_comparison.png").write_text(
                "png\n",
                encoding="utf-8",
            )
            (root / "output" / "2016 Legacy Runs FINAL Figures" / "manifests" / "legacy_final_output_manifest.json").write_text(
                "{}\n",
                encoding="utf-8",
            )
            (root / "output" / "2016 Legacy Runs FINAL Figures" / "manifests" / "prototype_2016_final_output_registry.csv").write_text(
                "final_relative_path\noutput/2016 Legacy Runs FINAL Figures/README.md\n",
                encoding="utf-8",
            )
            (root / "output" / "2016 Legacy Runs FINAL Figures" / "phase5" / "prototype_2016_packaging_summary.md").write_text(
                "packaging summary\n",
                encoding="utf-8",
            )
            (root / "ui").mkdir(parents=True, exist_ok=True)
            (root / "ui" / "app.py").write_text("print('ui')\n", encoding="utf-8")

            (root / "logs" / "run_phase5_sync.log").write_text("phase5 log\n", encoding="utf-8")

            service = Phase5LauncherAndDocsSyncService(
                repo_root=root,
                launcher_matrix_path=root / "config" / "launcher_matrix.json",
            )
            results = service.run()

            for key in (
                "software_versions_csv",
                "final_case_registry_csv",
                "final_config_snapshot_index_csv",
                "final_manifest_index_csv",
                "final_output_catalog_csv",
                "final_log_index_csv",
                "final_phase_status_registry_csv",
                "final_reproducibility_summary_md",
                "final_reproducibility_manifest_json",
                "phase5_packaging_sync_memo_md",
                "phase5_final_verdict_md",
                "launcher_user_guide_md",
            ):
                self.assertTrue(Path(results[key]).exists(), key)

            self.assertEqual(results["launcher_entrypoint"], "./start.ps1 -List -NoPause")
            self.assertIn("phase5_sync", results["safe_read_only_entry_ids"])
            self.assertIn("trajectory_gallery", results["safe_read_only_entry_ids"])
            self.assertIn("trajectory_gallery_panel", results["safe_read_only_entry_ids"])
            self.assertIn("figure_package_publication", results["safe_read_only_entry_ids"])

            phase_status_df = pd.read_csv(results["final_phase_status_registry_csv"])
            self.assertTrue(((phase_status_df["phase_id"] == "phase5") & (phase_status_df["track_id"] == "phase5_sync")).any())
            self.assertTrue(((phase_status_df["phase_id"] == "phase5") & (phase_status_df["track_id"] == "phase5_read_only_dashboard")).any())
            self.assertTrue(((phase_status_df["phase_id"] == "phase4") & (phase_status_df["track_id"] == "mindoro_phase4")).any())
            self.assertTrue((phase_status_df["track_id"] == "A").any())
            self.assertTrue((phase_status_df["track_id"] == "B1").any())
            self.assertTrue((phase_status_df["track_id"] == "B2").any())
            self.assertTrue((phase_status_df["track_id"] == "B3").any())
            phase_status_by_track = phase_status_df.set_index("track_id")
            self.assertEqual(
                phase_status_by_track.loc["B1", "track_label"],
                "Mindoro March 13 -> March 14 NOAA reinit primary validation",
            )
            self.assertEqual(
                phase_status_by_track.loc["C1", "track_label"],
                "DWH deterministic external transfer validation",
            )
            self.assertTrue(bool(phase_status_by_track.loc["C1", "scientifically_frozen"]))
            self.assertFalse(bool(phase_status_by_track.loc["C1", "inherited_provisional"]))
            self.assertTrue(pd.isna(phase_status_by_track.loc["C1", "main_blocker"]))

            case_registry_df = pd.read_csv(results["final_case_registry_csv"])
            prototype_2021 = case_registry_df[case_registry_df["case_id"] == "prototype_2021"].iloc[0]
            prototype_2016 = case_registry_df[case_registry_df["case_id"] == "prototype_2016"].iloc[0]
            self.assertIn("accepted-segment debug support", prototype_2021["mode_label"])
            self.assertIn("not final Phase 1 evidence", prototype_2021["notes"])
            self.assertIn("legacy debug support", prototype_2016["mode_label"].lower())
            self.assertIn("not final phase 1 evidence", prototype_2016["notes"].lower())
            self.assertEqual(prototype_2016["primary_output_root"], "output/2016 Legacy Runs FINAL Figures")
            self.assertEqual(prototype_2016["launcher_alias_entry_id"], "prototype_legacy_final_figures")
            self.assertIn("prototype_legacy_phase5_packaging", prototype_2016["appendix_or_support_track_ids"])

            manifest_index_df = pd.read_csv(results["final_manifest_index_csv"])
            optional_row = manifest_index_df[manifest_index_df["track_id"] == "dwh_phase4_appendix_pilot"].iloc[0]
            self.assertEqual(str(optional_row["exists"]).lower(), "false")
            self.assertEqual(str(optional_row["optional"]).lower(), "true")

            output_catalog_df = pd.read_csv(results["final_output_catalog_csv"])
            self.assertTrue(
                (
                    (output_catalog_df["track_id"] == "trajectory_gallery")
                    & (output_catalog_df["artifact_type"] == "trajectory_gallery_manifest.json")
                ).any()
            )
            self.assertTrue(
                (
                    (output_catalog_df["track_id"] == "trajectory_gallery_panel")
                    & (output_catalog_df["artifact_type"] == "panel_figure_manifest.json")
                ).any()
            )
            self.assertTrue(
                (
                    (output_catalog_df["track_id"] == "figure_package_publication")
                    & (output_catalog_df["artifact_type"] == "publication_figure_manifest.json")
                ).any()
            )
            self.assertTrue(
                (
                    (output_catalog_df["track_id"] == "B1")
                    & (output_catalog_df["artifact_group"] == "phase3b_march13_14_final_output")
                    & (output_catalog_df["relative_path"] == "output/Phase 3B March13-14 Final Output/final_output_manifest.json")
                ).any()
            )
            self.assertTrue(
                (
                    (output_catalog_df["track_id"] == "B1")
                    & (output_catalog_df["artifact_group"] == "phase3b_march13_14_final_output")
                    & (output_catalog_df["relative_path"] == "output/Phase 3B March13-14 Final Output/publication/mindoro_primary_validation_board.png")
                ).any()
            )
            self.assertTrue(
                (
                    (output_catalog_df["track_id"] == "C1/C2/C3")
                    & (output_catalog_df["artifact_group"] == "phase3c_dwh_final_output")
                    & (output_catalog_df["relative_path"] == "output/Phase 3C DWH Final Output/manifests/phase3c_final_output_manifest.json")
                ).any()
            )
            self.assertTrue(
                (
                    (output_catalog_df["track_id"] == "C1/C2/C3")
                    & (output_catalog_df["artifact_group"] == "phase3c_dwh_final_output")
                    & (output_catalog_df["relative_path"] == "output/Phase 3C DWH Final Output/publication/observations/dwh_2010-05-21_observation_context.png")
                ).any()
            )
            self.assertTrue(
                (
                    (output_catalog_df["track_id"] == "prototype_legacy_phase3a")
                    & (output_catalog_df["artifact_type"] == "prototype_pygnome_similarity_manifest.json")
                ).any()
            )
            self.assertTrue(
                (
                    (output_catalog_df["track_id"] == "prototype_legacy_phase3a")
                    & (output_catalog_df["relative_path"] == "output/prototype_2016_pygnome_similarity/figures/prototype_single.png")
                ).any()
            )
            self.assertTrue(
                (
                    (output_catalog_df["track_id"] == "prototype_legacy_phase4_weathering")
                    & (output_catalog_df["relative_path"] == "output/CASE_2016-09-01/weathering/budget_light.csv")
                ).any()
            )
            self.assertTrue(
                (
                    (output_catalog_df["track_id"] == "prototype_legacy_phase5_packaging")
                    & (output_catalog_df["relative_path"] == "output/2016 Legacy Runs FINAL Figures/manifests/legacy_final_output_manifest.json")
                ).any()
            )
            self.assertTrue(
                (
                    (output_catalog_df["track_id"] == "prototype_legacy_phase5_packaging")
                    & (output_catalog_df["relative_path"] == "output/2016 Legacy Runs FINAL Figures/phase5/prototype_2016_packaging_summary.md")
                ).any()
            )

            self.assertTrue(
                (
                    (manifest_index_df["track_id"] == "prototype_legacy_phase3a")
                    & (manifest_index_df["relative_path"] == "output/prototype_2016_pygnome_similarity/prototype_pygnome_similarity_manifest.json")
                ).any()
            )
            self.assertTrue(
                (
                    (manifest_index_df["track_id"] == "prototype_legacy_phase5_packaging")
                    & (manifest_index_df["relative_path"] == "output/2016 Legacy Runs FINAL Figures/manifests/legacy_final_output_manifest.json")
                ).any()
            )

            final_manifest = json.loads(Path(results["final_reproducibility_manifest_json"]).read_text(encoding="utf-8"))
            self.assertTrue(final_manifest["phase5_verdict"]["phase5_reportable_now"])
            self.assertTrue(final_manifest["phase5_verdict"]["launcher_menu_honest_and_current"])
            self.assertTrue(final_manifest["phase5_verdict"]["legacy_recipe_drift_leaks_into_official_mode"])
            self.assertEqual(
                final_manifest["mindoro_primary_validation_promotion"]["final_output_export_dir"],
                "output/Phase 3B March13-14 Final Output",
            )
            self.assertTrue(
                final_manifest["mindoro_primary_validation_promotion"]["dual_provenance_confirmation"]["matches_stored_b1_recipe"]
            )
            self.assertEqual(
                final_manifest["dwh_phase3c_freeze"]["final_output_export_dir"],
                "output/Phase 3C DWH Final Output",
            )
            self.assertIn(
                "output/phase4/CASE_DWH_RETRO_2010_72H/phase4_run_manifest.json",
                final_manifest["phase5_verdict"]["missing_optional_artifacts"],
            )

            launcher_guide_text = Path(results["launcher_user_guide_md"]).read_text(encoding="utf-8")
            self.assertIn("phase5_sync", launcher_guide_text)
            self.assertIn("trajectory_gallery", launcher_guide_text)
            self.assertIn("trajectory_gallery_panel", launcher_guide_text)
            self.assertIn("figure_package_publication", launcher_guide_text)
            self.assertIn("output/2016 Legacy Runs FINAL Figures", launcher_guide_text)

            config_snapshot_df = pd.read_csv(results["final_config_snapshot_index_csv"])
            self.assertTrue((config_snapshot_df["relative_path"] == "docs/UI_GUIDE.md").any())
            self.assertTrue((config_snapshot_df["relative_path"] == "ui/app.py").any())


if __name__ == "__main__":
    unittest.main()
