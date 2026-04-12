import json
import tempfile
import unittest
from pathlib import Path

from src.services.phase3b_multidate_public import (
    SOURCE_TAXONOMY_MODELED,
    SOURCE_TAXONOMY_OBS,
    classify_public_source,
    format_phase3b_multidate_eventcorridor_label,
    load_phase3b_multidate_validation_dates,
)


class Phase3BMultidatePublicTests(unittest.TestCase):
    def test_load_validation_dates_prefers_manifest(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            case_output = Path(tmpdir)
            multidate_dir = case_output / "phase3b_multidate_public"
            multidate_dir.mkdir(parents=True, exist_ok=True)
            (multidate_dir / "phase3b_multidate_run_manifest.json").write_text(
                json.dumps(
                    {
                        "accepted_validation_dates_used_for_forecast_skill": [
                            "2023-03-04",
                            "2023-03-06",
                        ]
                    }
                ),
                encoding="utf-8",
            )
            (multidate_dir / "phase3b_multidate_summary.csv").write_text(
                "pair_role,obs_date\nper_date_union,2023-03-04\nper_date_union,2023-03-05\n",
                encoding="utf-8",
            )

            self.assertEqual(
                load_phase3b_multidate_validation_dates(case_output),
                ["2023-03-04", "2023-03-06"],
            )

    def test_load_validation_dates_falls_back_to_summary(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            case_output = Path(tmpdir)
            multidate_dir = case_output / "phase3b_multidate_public"
            multidate_dir.mkdir(parents=True, exist_ok=True)
            (multidate_dir / "phase3b_multidate_summary.csv").write_text(
                (
                    "pair_role,obs_date\n"
                    "per_date_union,2023-03-04\n"
                    "per_date_union,2023-03-06\n"
                    "eventcorridor,2023-03-04_to_2023-03-06\n"
                ),
                encoding="utf-8",
            )

            self.assertEqual(
                load_phase3b_multidate_validation_dates(case_output),
                ["2023-03-04", "2023-03-06"],
            )

    def test_eventcorridor_label_uses_first_and_last_validation_dates(self):
        self.assertEqual(
            format_phase3b_multidate_eventcorridor_label(["2023-03-04", "2023-03-06"]),
            "2023-03-04_to_2023-03-06",
        )

    def test_classifies_machine_readable_observation_layer_as_quantitative(self):
        taxonomy, reason = classify_public_source(
            {
                "source_name": "Possible_oil_slick_(March_6,_2023)",
                "provider": "WWF Philippines",
                "obs_date": "2023-03-06",
                "source_type": "feature layer",
                "machine_readable": True,
                "public": True,
                "observation_derived": True,
                "reproducibly_ingestible": True,
                "geometry_type": "polygon",
                "accept_for_appendix_quantitative": True,
                "notes": "Public March 6 validation polygon.",
            }
        )
        self.assertEqual(taxonomy, SOURCE_TAXONOMY_OBS)
        self.assertIn("observation-derived", reason)

    def test_excludes_trajectory_model_from_truth_even_if_previous_appendix_accepted_it(self):
        taxonomy, reason = classify_public_source(
            {
                "source_name": "MindoroOilSpill_MSI_230305",
                "provider": "UP MSI",
                "obs_date": "2023-03-05",
                "source_type": "feature service",
                "machine_readable": True,
                "public": True,
                "observation_derived": True,
                "reproducibly_ingestible": True,
                "geometry_type": "polygon",
                "accept_for_appendix_quantitative": True,
                "notes": "MT Princess Empress Oil Spill Trajectory Model from UP Marine Science Institute",
            }
        )
        self.assertEqual(taxonomy, SOURCE_TAXONOMY_MODELED)
        self.assertIn("modeled", reason)


if __name__ == "__main__":
    unittest.main()
