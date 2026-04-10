import os
import tempfile
import unittest
from pathlib import Path

from src.services.official_rerun_r1 import load_official_retention_config, recommend_next_branch


class OfficialRerunR1Tests(unittest.TestCase):
    def test_load_official_retention_config_rejects_diagnostic_mode(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "ensemble.yaml"
            path.write_text(
                """
official_retention:
  selected_mode: R3
  scenarios:
    R3:
      coastline_action: none
      diagnostic_only: true
""",
                encoding="utf-8",
            )
            with self.assertRaises(RuntimeError):
                load_official_retention_config(path)

    def test_load_official_retention_config_reads_selected_r1(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "ensemble.yaml"
            path.write_text(
                """
official_retention:
  selected_mode: R1
  selected_phase: official_rerun_r1
  scenarios:
    R1:
      coastline_action: previous
      coastline_approximation_precision: 0.001
      time_step_minutes: 60
      diagnostic_only: false
      thesis_final_candidate: true
""",
                encoding="utf-8",
            )
            old = os.environ.pop("OFFICIAL_RETENTION_MODE", None)
            try:
                config = load_official_retention_config(path)
            finally:
                if old is not None:
                    os.environ["OFFICIAL_RETENTION_MODE"] = old
            self.assertEqual(config["selected_mode"], "R1")
            self.assertEqual(config["coastline_action"], "previous")
            self.assertFalse(config["diagnostic_only"])

    def test_recommend_next_branch_prefers_init_sensitivity_when_p50_survives_but_fss_is_zero(self):
        recommendation = recommend_next_branch(
            {
                "fss_1km": 0.0,
                "fss_3km": 0.0,
                "fss_5km": 0.0,
                "fss_10km": 0.0,
                "iou": 0.0,
                "forecast_nonzero_cells": 3,
                "survives_to_strict_validation": True,
            }
        )
        self.assertEqual(recommendation, "init-mode sensitivity (A1 vs B)")


if __name__ == "__main__":
    unittest.main()
