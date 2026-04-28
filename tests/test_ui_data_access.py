import json
import tempfile
import unittest
from pathlib import Path

from ui import data_access


REPO_ROOT = Path(__file__).resolve().parents[1]


class UiDataAccessTests(unittest.TestCase):
    def test_publication_registry_loads_without_repeated_header_row(self):
        registry = data_access.publication_registry(REPO_ROOT)

        self.assertFalse(registry.empty)
        self.assertNotEqual(str(registry.iloc[0]["figure_id"]).strip(), "figure_id")
        self.assertIn("resolved_path", registry.columns)

    def test_curated_recommended_figures_contains_both_cases(self):
        recommended = data_access.curated_recommended_figures(REPO_ROOT)

        self.assertFalse(recommended.empty)
        case_ids = set(recommended["case_id"].astype(str))
        self.assertIn("CASE_MINDORO_RETRO_2023", case_ids)
        self.assertIn("CASE_DWH_RETRO_2010_72H", case_ids)

    def test_phase4_crossmodel_matrix_matches_deferred_audit(self):
        matrix = data_access.phase4_crossmodel_matrix(REPO_ROOT)

        self.assertFalse(matrix.empty)
        self.assertEqual(set(matrix["classification"].astype(str)), {"no_matched_phase4_pygnome_package_yet"})

    def test_build_dashboard_state_contains_expected_sections(self):
        state = data_access.build_dashboard_state(REPO_ROOT)

        for key in (
            "phase_status",
            "phase1_focused_manifest",
            "phase1_focused_recipe_ranking",
            "phase1_reference_recipe_ranking",
            "publication_registry",
            "publication_manifest",
            "phase4_crossmodel_matrix",
            "curated_recommended_figures",
            "home_featured_publication_figures",
            "legacy_2016_provenance_metadata",
            "legacy_2016_phase4_comparator_registry",
            "legacy_2016_phase4_comparator_decision_note",
        ):
            self.assertIn(key, state)

    def test_home_featured_publication_figures_follow_requested_overview_sequence(self):
        featured = data_access.home_featured_publication_figures(REPO_ROOT)

        self.assertFalse(featured.empty)
        figure_ids = featured["figure_id"].astype(str).tolist()
        page_targets = featured["page_target"].astype(str).tolist()
        self.assertIn("thesis_study_boxes_reference", figure_ids[0])
        self.assertEqual(page_targets[0], "phase1_recipe_selection")
        self.assertIn("mindoro_primary_validation_board", figure_ids[1])
        self.assertIn("march14_r1_previous_overlay", figure_ids[2])
        self.assertTrue(any("mindoro_crossmodel_board" in figure_id for figure_id in figure_ids))
        self.assertTrue(any("daily_deterministic_footprint_overview_board" in figure_id for figure_id in figure_ids))
        self.assertTrue(any("observed_deterministic_mask_p50_mask_p90_board" in figure_id for figure_id in figure_ids))
        self.assertTrue(any("observed_deterministic_mask_p50_pygnome_board" in figure_id for figure_id in figure_ids))
        self.assertTrue(any(page_target == "mindoro_validation" for page_target in page_targets))
        self.assertTrue(any(page_target == "cross_model_comparison" for page_target in page_targets))
        self.assertTrue(any(page_target == "dwh_transfer_validation" for page_target in page_targets))
        self.assertFalse(any(page_target == "phase4_oiltype_and_shoreline" for page_target in page_targets))
        self.assertFalse(any(page_target == "phase4_crossmodel_status" for page_target in page_targets))
        self.assertFalse((featured["surface_key"].astype(str) == "archive_only").any())
        self.assertFalse((featured["surface_key"].astype(str) == "legacy_support").any())

    def test_home_featured_publication_figures_exclude_mindoro_archive_and_phase4_classes(self):
        featured = data_access.home_featured_publication_figures(REPO_ROOT)

        self.assertFalse(featured.empty)
        mindoro = featured.loc[featured["case_id"].astype(str).eq("CASE_MINDORO_RETRO_2023")].copy()
        forbidden_statuses = {
            "mindoro_b1_r0_archive",
            "mindoro_legacy_march6",
            "mindoro_legacy_support",
            "mindoro_phase4_oil_budget",
            "mindoro_phase4_shoreline",
            "mindoro_phase4_deferred",
        }
        forbidden_pages = {
            "mindoro_validation_archive",
            "phase4_oiltype_and_shoreline",
            "phase4_crossmodel_status",
        }
        forbidden_phases = {
            "phase3b_legacy_strict",
            "phase3b_support",
            "phase4",
            "phase4_crossmodel_comparability_audit",
        }

        self.assertFalse(mindoro["status_key"].astype(str).isin(forbidden_statuses).any())
        self.assertFalse(mindoro["page_target"].astype(str).isin(forbidden_pages).any())
        self.assertFalse(mindoro["phase_or_track"].astype(str).isin(forbidden_phases).any())

    def test_publication_registry_contains_thesis_study_box_reference(self):
        registry = data_access.publication_registry(REPO_ROOT)

        thesis_boxes = registry.loc[
            registry.get("status_key", "").astype(str).eq("thesis_study_box_reference")
        ].copy()
        self.assertFalse(thesis_boxes.empty)
        self.assertTrue(thesis_boxes["figure_id"].astype(str).str.contains("thesis_study_boxes_reference").any())
        self.assertGreaterEqual(len(thesis_boxes), 5)
        self.assertTrue(
            thesis_boxes["figure_id"].astype(str).str.contains("focused_phase1_box_geography_reference").any()
        )
        self.assertTrue(
            thesis_boxes["figure_id"].astype(str).str.contains("prototype_first_code_search_box_geography_reference").any()
        )
        self.assertIn("study_box_numbers", thesis_boxes.columns)
        thesis_surface_numbers = set(
            thesis_boxes.loc[thesis_boxes["thesis_surface"] == True, "study_box_numbers"].astype(str)  # noqa: E712
        )
        archive_numbers = set(
            thesis_boxes.loc[thesis_boxes["archive_only"] == True, "study_box_numbers"].astype(str)  # noqa: E712
        )
        self.assertEqual(thesis_surface_numbers, {"2", "4", "2,4"})
        self.assertEqual(archive_numbers, {"1", "3", "1,2,3,4"})

    def test_legacy_2016_provenance_metadata_records_union_and_source_boxes(self):
        metadata = data_access.legacy_2016_provenance_metadata(REPO_ROOT)

        self.assertEqual(
            metadata.get("prototype_2016_initial_capture_box"),
            [108.6465, 121.3655, 6.1865, 20.3515],
        )
        self.assertEqual(
            metadata.get("prototype_2016_initial_capture_source_boxes"),
            [
                [113.267, 121.267, 6.3685, 14.3685],
                [113.3655, 121.3655, 6.1865, 14.1865],
                [108.6465, 116.6465, 12.3515, 20.3515],
            ],
        )

    def test_phase1_focused_artifacts_load(self):
        manifest = data_access.phase1_focused_manifest(REPO_ROOT)
        ranking = data_access.phase1_focused_recipe_ranking(REPO_ROOT)
        accepted = data_access.phase1_focused_accepted_segments(REPO_ROOT)

        winning_recipe = str(manifest.get("winning_recipe") or "").strip()
        official_recipe = str(manifest.get("official_b1_recipe") or "").strip()
        if not winning_recipe and not ranking.empty and "recipe" in ranking.columns:
            winning_recipe = str(ranking.iloc[0]["recipe"])
        self.assertEqual(official_recipe or winning_recipe, "cmems_gfs")
        self.assertEqual(winning_recipe or official_recipe, "cmems_gfs")
        self.assertFalse(ranking.empty)
        self.assertEqual(str(ranking.iloc[0]["recipe"]).strip(), "cmems_gfs")
        self.assertFalse(accepted.empty)
        self.assertIn("start_time_utc", accepted.columns)

    def test_legacy_2016_phase4_comparator_registry_is_budget_only_light_and_heavy(self):
        registry = data_access.legacy_2016_phase4_comparator_registry(REPO_ROOT)

        self.assertFalse(registry.empty)
        scenario_keys = {value for value in registry["scenario_key"].fillna("").astype(str) if value}
        self.assertEqual(scenario_keys, {"light", "heavy"})

    def test_final_validation_case_registry_exposes_archive_r0_surface(self):
        registry = data_access.final_validation_case_registry(REPO_ROOT)

        archive_r0 = registry.loc[registry["track_id"].astype(str).eq("archive_r0")].iloc[0]
        self.assertEqual(archive_r0["status_key"], "mindoro_b1_r0_archive")
        self.assertEqual(archive_r0["surface_key"], "archive_only")

    def test_mindoro_summary_tables_split_r1_and_r0_by_surface(self):
        summary = data_access.mindoro_b1_summary(REPO_ROOT)
        comparator = data_access.mindoro_comparator_summary(REPO_ROOT)

        r1 = summary.loc[summary["branch_id"].astype(str).eq("R1_previous")].iloc[0]
        r0 = summary.loc[summary["branch_id"].astype(str).eq("R0")].iloc[0]
        promoted_comparator = comparator.loc[comparator["track_id"].astype(str).eq("R1_previous_reinit_p50")].iloc[0]
        archive_comparator = comparator.loc[comparator["track_id"].astype(str).eq("R0_reinit_p50")].iloc[0]

        self.assertEqual(r1["surface_key"], "thesis_main")
        self.assertEqual(r0["surface_key"], "archive_only")
        self.assertEqual(promoted_comparator["surface_key"], "comparator_support")
        self.assertEqual(archive_comparator["surface_key"], "archive_only")

    def test_figure_subset_uses_status_keys_for_ambiguous_dwh_rows(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            publication_dir = root / "output" / "figure_package_publication"
            publication_dir.mkdir(parents=True, exist_ok=True)
            (publication_dir / "publication_figure_manifest.json").write_text(
                json.dumps(
                    {
                        "recommended_main_defense_figures": [
                            "mindoro_primary_validation_board",
                            "mindoro_crossmodel_board",
                            "daily_deterministic_board",
                        ]
                    }
                ),
                encoding="utf-8",
            )
            (publication_dir / "publication_figure_registry.csv").write_text(
                "\n".join(
                    [
                        "figure_id,case_id,phase_or_track,run_type,figure_slug,relative_path,recommended_for_main_defense",
                        "mindoro_crossmodel_board,CASE_MINDORO_RETRO_2023,phase3a_reinit_crossmodel,comparison_board,mindoro_crossmodel_board,output/figure_package_publication/mindoro_crossmodel_board.png,true",
                        "daily_deterministic_board,CASE_DWH_RETRO_2010_72H,phase3c_external_case_run,comparison_board,daily_deterministic_board,output/figure_package_publication/daily_deterministic_board.png,true",
                        "mindoro_primary_validation_board,CASE_MINDORO_RETRO_2023,phase3b_reinit_primary,comparison_board,mindoro_primary_validation_board,output/figure_package_publication/mindoro_primary_validation_board.png,true",
                        "deterministic_vs_ensemble_board,CASE_DWH_RETRO_2010_72H,phase3c_external_case_ensemble_comparison,comparison_board,deterministic_vs_ensemble_board,output/figure_package_publication/deterministic_vs_ensemble_board.png,false",
                        "ensemble_sampled_trajectory,CASE_DWH_RETRO_2010_72H,phase3c_external_case_ensemble_comparison,single_trajectory,ensemble_sampled_trajectory,output/figure_package_publication/ensemble_sampled_trajectory.png,false",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            registry = data_access.publication_registry(root)
            by_id = registry.set_index("figure_id")
            self.assertEqual(by_id.loc["mindoro_primary_validation_board", "status_key"], "mindoro_primary_validation")
            self.assertEqual(by_id.loc["deterministic_vs_ensemble_board", "status_key"], "dwh_ensemble_transfer")
            self.assertEqual(by_id.loc["ensemble_sampled_trajectory", "status_key"], "dwh_trajectory_context")
            self.assertTrue(bool(by_id.loc["mindoro_primary_validation_board", "thesis_surface"]))
            self.assertFalse(bool(by_id.loc["ensemble_sampled_trajectory", "thesis_surface"]))
            self.assertEqual(by_id.loc["mindoro_primary_validation_board", "recommended_scope"], "main_text")

            ensemble = data_access.figure_subset(
                "publication",
                repo_root=root,
                case_id="CASE_DWH_RETRO_2010_72H",
                status_keys=["dwh_ensemble_transfer"],
            )
            self.assertEqual(ensemble["figure_id"].tolist(), ["deterministic_vs_ensemble_board"])

            trajectories = data_access.figure_subset(
                "publication",
                repo_root=root,
                case_id="CASE_DWH_RETRO_2010_72H",
                status_keys=["dwh_trajectory_context"],
            )
            self.assertEqual(trajectories["figure_id"].tolist(), ["ensemble_sampled_trajectory"])

            recommended = data_access.curated_recommended_figures(root)
            self.assertEqual(recommended.iloc[0]["figure_id"], "mindoro_primary_validation_board")
            self.assertEqual(recommended.iloc[1]["figure_id"], "mindoro_crossmodel_board")
            self.assertTrue((recommended["thesis_surface"] == True).all())  # noqa: E712

    def test_curated_recommended_figures_ignore_archive_only_rows_even_with_stale_manifest_flags(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            publication_dir = root / "output" / "figure_package_publication"
            publication_dir.mkdir(parents=True, exist_ok=True)
            (publication_dir / "publication_figure_manifest.json").write_text(
                json.dumps(
                    {
                        "recommended_main_defense_figures": [
                            "mindoro_primary_validation_board",
                            "march14_r0_overlay",
                        ]
                    }
                ),
                encoding="utf-8",
            )
            (publication_dir / "publication_figure_registry.csv").write_text(
                "\n".join(
                    [
                        "figure_id,case_id,phase_or_track,run_type,figure_slug,relative_path,recommended_for_main_defense",
                        "mindoro_primary_validation_board,CASE_MINDORO_RETRO_2023,phase3b_reinit_primary,comparison_board,mindoro_primary_validation_board,output/figure_package_publication/mindoro_primary_validation_board.png,true",
                        "march14_r0_overlay,CASE_MINDORO_RETRO_2023,phase3b_reinit_primary,single_model,march14_r0_overlay,output/figure_package_publication/march14_r0_overlay.png,true",
                    ]
                )
                + "\n",
                encoding="utf-8",
            )

            recommended = data_access.curated_recommended_figures(root)

            self.assertEqual(recommended["figure_id"].tolist(), ["mindoro_primary_validation_board"])
            self.assertTrue((recommended["surface_key"].astype(str) == "thesis_main").all())


if __name__ == "__main__":
    unittest.main()
