import unittest

from src.services.phase3b_extended_public import (
    MARCH23_NOAA_MSI_SOURCE_DATE,
    MARCH23_NOAA_MSI_SOURCE_KEY,
    classify_extended_public_source,
)
from src.services.phase3b_multidate_public import (
    SOURCE_TAXONOMY_MODELED,
    SOURCE_TAXONOMY_OBS,
    SOURCE_TAXONOMY_QUALITATIVE,
)


class Phase3BExtendedPublicClassificationTests(unittest.TestCase):
    def _base_row(self, **overrides):
        row = {
            "source_name": "MindoroOilSpill_Philsa_230307",
            "provider": "PhilSA",
            "obs_date": "2023-03-07",
            "source_type": "feature service",
            "machine_readable": True,
            "public": True,
            "observation_derived": True,
            "reproducibly_ingestible": True,
            "geometry_type": "polygon",
            "service_url": "https://example.test/FeatureServer",
            "layer_id": "0",
            "notes": "RCM acquired 07/03/2023.",
        }
        row.update(overrides)
        return row

    def test_accepts_beyond_horizon_observation_polygon(self):
        taxonomy, reason, accepted = classify_extended_public_source(self._base_row())
        self.assertEqual(taxonomy, SOURCE_TAXONOMY_OBS)
        self.assertTrue(accepted)
        self.assertIn("beyond-horizon", reason)

    def test_accepts_numeric_zero_layer_id_from_csv(self):
        taxonomy, _, accepted = classify_extended_public_source(self._base_row(layer_id=0.0))
        self.assertEqual(taxonomy, SOURCE_TAXONOMY_OBS)
        self.assertTrue(accepted)

    def test_excludes_trajectory_model_from_truth(self):
        taxonomy, reason, accepted = classify_extended_public_source(
            self._base_row(
                source_name="MindoroOilSpill_MSI_230307",
                provider="UP MSI",
                notes="Trajectory Model product.",
            )
        )
        self.assertEqual(taxonomy, SOURCE_TAXONOMY_MODELED)
        self.assertFalse(accepted)
        self.assertIn("modeled", reason)

    def test_excludes_march3_initialization_date(self):
        taxonomy, reason, accepted = classify_extended_public_source(self._base_row(obs_date="2023-03-03"))
        self.assertEqual(taxonomy, SOURCE_TAXONOMY_QUALITATIVE)
        self.assertFalse(accepted)
        self.assertIn("initialization", reason)

    def test_excludes_within_horizon_date(self):
        taxonomy, reason, accepted = classify_extended_public_source(self._base_row(obs_date="2023-03-06"))
        self.assertEqual(taxonomy, SOURCE_TAXONOMY_QUALITATIVE)
        self.assertFalse(accepted)
        self.assertIn("within-horizon", reason)

    def test_accepts_one_off_march23_noaa_wrapped_bulletin_source(self):
        taxonomy, reason, accepted = classify_extended_public_source(
            self._base_row(
                source_key=MARCH23_NOAA_MSI_SOURCE_KEY,
                **{
                    "item_or_layer_id": MARCH23_NOAA_MSI_SOURCE_KEY,
                    "item_id or layer_id": MARCH23_NOAA_MSI_SOURCE_KEY,
                },
                source_name="MindoroOilSpill_MSI_20230323",
                provider="NOAA/NESDIS",
                obs_date=MARCH23_NOAA_MSI_SOURCE_DATE,
                notes="MSI Bulletin #12 citing a NOAA/NESDIS satellite surveillance report for March 23.",
            )
        )
        self.assertEqual(taxonomy, SOURCE_TAXONOMY_OBS)
        self.assertTrue(accepted)
        self.assertIn("whitelist", reason)
        self.assertIn("NOAA/NESDIS", reason)

    def test_other_bulletin_wrapped_sources_remain_excluded(self):
        taxonomy, reason, accepted = classify_extended_public_source(
            self._base_row(
                source_key="different-bulletin-source",
                **{
                    "item_or_layer_id": "different-bulletin-source",
                    "item_id or layer_id": "different-bulletin-source",
                },
                source_name="MindoroOilSpill_MSI_20230324",
                provider="UP MSI",
                obs_date="2023-03-24",
                notes="MSI Bulletin #13 modeled outlook for the spill.",
            )
        )
        self.assertEqual(taxonomy, SOURCE_TAXONOMY_MODELED)
        self.assertFalse(accepted)
        self.assertIn("modeled", reason)


if __name__ == "__main__":
    unittest.main()
