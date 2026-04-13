import unittest
from pathlib import Path

from src.core.artifact_status import (
    artifact_status_columns,
    artifact_status_columns_for_key,
    record_matches_artifact_status,
)


class ArtifactStatusTests(unittest.TestCase):
    def test_mindoro_primary_record_maps_to_promoted_primary_status(self):
        status = artifact_status_columns(
            {
                "case_id": "CASE_MINDORO_RETRO_2023",
                "phase_or_track": "phase3b_reinit_primary",
                "run_type": "comparison_board",
                "figure_slug": "mindoro_primary_validation_board",
            }
        )

        self.assertEqual(status["status_key"], "mindoro_primary_validation")
        self.assertIn("March 13 -> March 14", status["status_label"])

    def test_dwh_long_form_ensemble_phase_maps_to_ensemble_status(self):
        status = artifact_status_columns(
            {
                "case_id": "CASE_DWH_RETRO_2010_72H",
                "phase_or_track": "phase3c_external_case_ensemble_comparison",
                "run_type": "comparison_board",
                "figure_slug": "deterministic_vs_ensemble_board",
            }
        )

        self.assertEqual(status["status_key"], "dwh_ensemble_transfer")

    def test_mindoro_crossmodel_record_maps_to_comparator_support_status(self):
        status = artifact_status_columns(
            {
                "case_id": "CASE_MINDORO_RETRO_2023",
                "phase_or_track": "phase3a_reinit_crossmodel",
                "run_type": "comparison_board",
                "figure_slug": "mindoro_crossmodel_board",
            }
        )

        self.assertEqual(status["status_key"], "mindoro_crossmodel_comparator")
        self.assertEqual(status["status_role"], "comparator_only")
        self.assertIn("support", status["status_dashboard_summary"].lower())

    def test_dwh_trajectory_artifact_does_not_inherit_deterministic_status(self):
        record = {
            "case_id": "CASE_DWH_RETRO_2010_72H",
            "phase_or_track": "phase3c_external_case_run",
            "run_type": "trajectory_board",
            "figure_slug": "trajectory_board",
            "figure_id": "case_dwh_retro_2010_72h__trajectory_board",
        }

        self.assertFalse(record_matches_artifact_status(record, "dwh_deterministic_transfer"))
        self.assertTrue(record_matches_artifact_status(record, "dwh_trajectory_context"))
        self.assertEqual(artifact_status_columns(record)["status_key"], "dwh_trajectory_context")

    def test_prototype_2016_rows_can_classify_from_legacy_debug_flag(self):
        status = artifact_status_columns(
            {
                "phase_or_track": "prototype_pygnome_similarity_summary",
                "legacy_debug_only": True,
                "relative_path": "output/prototype_2016_pygnome_similarity/figures/example.png",
            }
        )

        self.assertEqual(status["status_key"], "prototype_2016_support")
        self.assertIn("legacy debug support", status["status_label"].lower())

    def test_dwh_observation_truth_context_status_is_available_for_explicit_override(self):
        status = artifact_status_columns_for_key("dwh_observation_truth_context")

        self.assertEqual(status["status_key"], "dwh_observation_truth_context")
        self.assertIn("truth context", status["status_label"].lower())

    def test_dwh_reportable_tracks_are_frozen_not_inherited_provisional(self):
        deterministic = artifact_status_columns_for_key("dwh_deterministic_transfer")
        ensemble = artifact_status_columns_for_key("dwh_ensemble_transfer")
        comparator = artifact_status_columns_for_key("dwh_crossmodel_comparator")

        self.assertEqual(deterministic["status_frozen_status"], "frozen")
        self.assertEqual(ensemble["status_frozen_status"], "frozen")
        self.assertEqual(comparator["status_frozen_status"], "frozen")
        self.assertIn("frozen", deterministic["status_dashboard_summary"].lower())
        self.assertIn("frozen", ensemble["status_dashboard_summary"].lower())
        self.assertIn("frozen", comparator["status_dashboard_summary"].lower())

    def test_dwh_case_config_no_longer_marks_c2_or_c3_deferred(self):
        config_text = Path("config/case_dwh_retro_2010_72h.yaml").read_text(encoding="utf-8")

        self.assertNotIn("deferred_until_clean_dwh_ensemble_semantics_are_implemented", config_text)
        self.assertNotIn("deferred_if_not_cleanly_available", config_text)
        self.assertIn("frozen_reportable_probabilistic_extension_p50_preferred_p90_support_only", config_text)
        self.assertIn("frozen_reportable_comparator_only", config_text)


if __name__ == "__main__":
    unittest.main()
