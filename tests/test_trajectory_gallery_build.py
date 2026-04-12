import json
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from src.services.trajectory_gallery_build import (
    TrajectoryGalleryBuildService,
    build_figure_filename,
)


class TrajectoryGalleryBuildTests(unittest.TestCase):
    def test_build_figure_filename_includes_expected_tokens(self):
        filename = build_figure_filename(
            case_id="CASE_MINDORO_RETRO_2023",
            phase_or_track="phase4",
            model_name="OpenOil",
            run_type="shoreline impact summary",
            date_token="2023-03-03_to_2023-03-06",
            scenario_id="lighter_oil",
            figure_slug="shoreline_impacts",
        )

        self.assertEqual(
            filename,
            "case_mindoro_retro_2023__phase4__openoil__shoreline_impact_summary__2023_03_03_to_2023_03_06__lighter_oil__shoreline_impacts.png",
        )

    def test_gallery_service_writes_manifest_and_handles_missing_optional_artifacts(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            source_png = (
                root
                / "output"
                / "CASE_MINDORO_RETRO_2023"
                / "phase3b_extended_public_scored_march13_14_reinit"
                / "qa_march13_seed_vs_march14_target.png"
            )
            source_png.parent.mkdir(parents=True, exist_ok=True)
            source_png.write_bytes(b"not_a_real_png_but_copyable")

            service = TrajectoryGalleryBuildService(repo_root=root)
            results = service.run()

            manifest_path = Path(results["manifest_path"])
            index_csv_path = Path(results["index_csv"])
            figures_index_md = Path(results["figures_index_md"])

            self.assertTrue(manifest_path.exists())
            self.assertTrue(index_csv_path.exists())
            self.assertTrue(figures_index_md.exists())
            self.assertGreaterEqual(results["figure_count"], 1)

            index_df = pd.read_csv(index_csv_path)
            self.assertTrue((index_df["case_id"] == "CASE_MINDORO_RETRO_2023").any())
            self.assertTrue(index_df["filename"].str.contains("seed_vs_target").any())
            self.assertIn("status_key", index_df.columns)
            self.assertIn("status_provenance", index_df.columns)
            self.assertTrue((index_df["status_key"] == "mindoro_primary_validation").any())

            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            self.assertTrue(manifest["gallery_built_from_existing_outputs_only"])
            self.assertFalse(manifest["expensive_scientific_reruns_triggered"])
            self.assertGreater(len(manifest["missing_optional_artifacts"]), 0)


if __name__ == "__main__":
    unittest.main()
