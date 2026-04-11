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
        self.assertEqual(set(matrix["classification"].astype(str)), {"not_comparable_honestly"})

    def test_build_dashboard_state_contains_expected_sections(self):
        state = data_access.build_dashboard_state(REPO_ROOT)

        for key in (
            "phase_status",
            "publication_registry",
            "publication_manifest",
            "phase4_crossmodel_matrix",
            "curated_recommended_figures",
        ):
            self.assertIn(key, state)


if __name__ == "__main__":
    unittest.main()
