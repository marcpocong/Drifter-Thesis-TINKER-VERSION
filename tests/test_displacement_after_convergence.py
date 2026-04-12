import unittest

from src.services.displacement_after_convergence import rank_displacement_after_convergence_hypotheses


class DisplacementAfterConvergenceTests(unittest.TestCase):
    def test_rank_prefers_transport_when_p50_empty_and_particles_end_early(self):
        ranked = rank_displacement_after_convergence_hypotheses(
            {
                "highest_count_official_p50_nonzero_cells": 0,
                "highest_count_max_march6_occupancy_members": 0,
                "control_final_active_particles": 0,
                "control_hours_short_of_requested_end": 36.0,
                "transport_provisional": True,
                "appendix_eventcorridor_iou": 0.09,
                "march6_obs_nonzero_cells": 2,
                "march6_vector_area_m2": 200000.0,
                "current_tail_extension_run_count": 51,
                "current_tail_extension_max_gap_hours": 9.98,
                "forcing_bounds_cover_canonical_halo": True,
                "download_manifest_uses_canonical_bbox": True,
                "actual_broad_case_domain_usage_detected": False,
                "source_has_official_active_domain_fallbacks": True,
                "wave_reader_loaded_for_all_runs": True,
                "march3_rescue_applied": True,
                "seed_outside_processed_polygon_fraction": 0.0,
                "march3_appendix_fss_1km": 0.68,
                "release_centroid_to_processed_march3_centroid_m": 120.0,
                "provenance_point_to_processed_march3_centroid_m": 43000.0,
            }
        )
        self.assertEqual(ranked[0]["hypothesis_id"], "transport_model_structural_limitation")
        self.assertEqual(ranked[0]["recommended_rerun"], "transport-model limitation rerun")

    def test_rank_keeps_observation_strictness_secondary_when_forecast_is_empty(self):
        ranked = rank_displacement_after_convergence_hypotheses(
            {
                "highest_count_official_p50_nonzero_cells": 0,
                "highest_count_max_march6_occupancy_members": 0,
                "control_final_active_particles": 0,
                "control_hours_short_of_requested_end": 36.0,
                "transport_provisional": False,
                "appendix_eventcorridor_iou": 0.0,
                "march6_obs_nonzero_cells": 2,
                "march6_vector_area_m2": 200000.0,
                "current_tail_extension_run_count": 0,
                "current_tail_extension_max_gap_hours": 0.0,
                "forcing_bounds_cover_canonical_halo": True,
                "download_manifest_uses_canonical_bbox": True,
                "actual_broad_case_domain_usage_detected": False,
                "source_has_official_active_domain_fallbacks": False,
                "wave_reader_loaded_for_all_runs": True,
                "march3_rescue_applied": False,
                "seed_outside_processed_polygon_fraction": 0.0,
                "march3_appendix_fss_1km": 0.7,
                "release_centroid_to_processed_march3_centroid_m": 120.0,
                "provenance_point_to_processed_march3_centroid_m": 43000.0,
            }
        )
        obs_rank = next(row["rank"] for row in ranked if row["hypothesis_id"] == "observation_strictness_from_tiny_march6_target")
        self.assertGreater(obs_rank, 1)


if __name__ == "__main__":
    unittest.main()
