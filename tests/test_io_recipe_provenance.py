import json
import tempfile
import unittest
from pathlib import Path

from src.utils.io import extract_manifest_recipe, model_dir_complete_for_recipe


class IORecipeProvenanceTests(unittest.TestCase):
    def test_extract_manifest_recipe_prefers_forecast_recipe_fields(self):
        payload = {
            "selection": {"recipe": "cmems_gfs"},
            "historical_baseline_provenance": {"recipe": "cmems_era5"},
            "provenance": {"recipe_used": "hycom_gfs"},
        }
        self.assertEqual(extract_manifest_recipe(payload), "cmems_gfs")

    def test_model_dir_complete_for_recipe_rejects_recipe_mismatch(self):
        with tempfile.TemporaryDirectory() as tmp:
            model_dir = Path(tmp) / "model_run"
            forecast_dir = model_dir / "forecast"
            ensemble_dir = model_dir / "ensemble"
            forecast_dir.mkdir(parents=True, exist_ok=True)
            ensemble_dir.mkdir(parents=True, exist_ok=True)
            (forecast_dir / "forecast_manifest.json").write_text(
                json.dumps({"selection": {"recipe": "cmems_era5"}}),
                encoding="utf-8",
            )
            (ensemble_dir / "ensemble_manifest.json").write_text("{}", encoding="utf-8")
            (ensemble_dir / "member_01.nc").write_text("placeholder", encoding="utf-8")

            self.assertFalse(model_dir_complete_for_recipe(model_dir, "cmems_gfs"))

    def test_model_dir_complete_for_recipe_accepts_matching_recipe(self):
        with tempfile.TemporaryDirectory() as tmp:
            model_dir = Path(tmp) / "model_run"
            forecast_dir = model_dir / "forecast"
            ensemble_dir = model_dir / "ensemble"
            forecast_dir.mkdir(parents=True, exist_ok=True)
            ensemble_dir.mkdir(parents=True, exist_ok=True)
            (forecast_dir / "forecast_manifest.json").write_text(
                json.dumps({"recipe": "cmems_gfs"}),
                encoding="utf-8",
            )
            (ensemble_dir / "ensemble_manifest.json").write_text("{}", encoding="utf-8")
            (ensemble_dir / "member_01.nc").write_text("placeholder", encoding="utf-8")

            self.assertTrue(model_dir_complete_for_recipe(model_dir, "cmems_gfs"))


if __name__ == "__main__":
    unittest.main()
