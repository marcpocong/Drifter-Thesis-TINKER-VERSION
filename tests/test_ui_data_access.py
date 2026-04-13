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
            "legacy_2016_phase4_comparator_registry",
            "legacy_2016_phase4_comparator_decision_note",
        ):
            self.assertIn(key, state)

    def test_phase1_focused_artifacts_load(self):
        manifest = data_access.phase1_focused_manifest(REPO_ROOT)
        ranking = data_access.phase1_focused_recipe_ranking(REPO_ROOT)
        accepted = data_access.phase1_focused_accepted_segments(REPO_ROOT)

        self.assertEqual(manifest.get("winning_recipe"), "cmems_era5")
        self.assertFalse(ranking.empty)
        self.assertFalse(accepted.empty)
        self.assertIn("start_time_utc", accepted.columns)

    def test_legacy_2016_phase4_comparator_registry_is_budget_only_light_and_heavy(self):
        registry = data_access.legacy_2016_phase4_comparator_registry(REPO_ROOT)

        self.assertFalse(registry.empty)
        scenario_keys = {value for value in registry["scenario_key"].fillna("").astype(str) if value}
        self.assertEqual(scenario_keys, {"light", "heavy"})

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


if __name__ == "__main__":
    unittest.main()
