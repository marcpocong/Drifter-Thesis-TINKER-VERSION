import json
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from src.services.phase4_crossmodel_comparability_audit import Phase4CrossModelComparabilityAuditService


class Phase4CrossModelComparabilityAuditTests(unittest.TestCase):
    def test_audit_writes_blocker_bundle_when_pygnome_phase4_semantics_are_missing(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            phase4_dir = root / "output" / "phase4" / "CASE_MINDORO_RETRO_2023"
            mindoro_py_dir = root / "output" / "CASE_MINDORO_RETRO_2023" / "pygnome_public_comparison" / "products" / "C3_pygnome_deterministic"
            dwh_py_dir = root / "output" / "CASE_DWH_RETRO_2010_72H" / "phase3c_dwh_pygnome_comparator"
            helper_dir = root / "src" / "helpers"
            services_dir = root / "src" / "services"

            phase4_dir.mkdir(parents=True, exist_ok=True)
            mindoro_py_dir.mkdir(parents=True, exist_ok=True)
            dwh_py_dir.mkdir(parents=True, exist_ok=True)
            helper_dir.mkdir(parents=True, exist_ok=True)
            services_dir.mkdir(parents=True, exist_ok=True)

            (phase4_dir / "phase4_run_manifest.json").write_text(
                json.dumps({"overall_verdict": {"scientifically_reportable_now": True}}, indent=2),
                encoding="utf-8",
            )
            pd.DataFrame(
                [
                    {
                        "scenario_id": "lighter_oil",
                        "final_surface_pct": 0.0,
                        "final_evaporated_pct": 0.8,
                        "final_dispersed_pct": 99.1,
                        "final_beached_pct": 0.1,
                    }
                ]
            ).to_csv(phase4_dir / "phase4_oil_budget_summary.csv", index=False)
            pd.DataFrame(
                [
                    {
                        "scenario_id": "lighter_oil",
                        "first_shoreline_arrival_h": 4.0,
                        "impacted_segment_count": 3,
                    }
                ]
            ).to_csv(phase4_dir / "phase4_shoreline_arrival.csv", index=False)
            pd.DataFrame(
                [
                    {
                        "scenario_id": "lighter_oil",
                        "segment_id": "shoreline_00001",
                        "total_beached_kg": 12.3,
                    }
                ]
            ).to_csv(phase4_dir / "phase4_shoreline_segments.csv", index=False)
            pd.DataFrame([{"scenario_id": "lighter_oil"}]).to_csv(
                phase4_dir / "phase4_oiltype_comparison.csv",
                index=False,
            )

            mindoro_root = root / "output" / "CASE_MINDORO_RETRO_2023" / "pygnome_public_comparison"
            mindoro_root.mkdir(parents=True, exist_ok=True)
            (mindoro_root / "pygnome_public_comparison_run_manifest.json").write_text(
                json.dumps(
                    {
                        "tracks": [
                            {
                                "track_id": "C3",
                                "structural_limitations": "PyGNOME deterministic benchmark approximates the polygon and does not provide a matched Phase 4 shoreline workflow.",
                            }
                        ]
                    },
                    indent=2,
                ),
                encoding="utf-8",
            )
            (mindoro_py_dir / "pygnome_benchmark_metadata.json").write_text(
                json.dumps({"weathering_enabled": False}, indent=2),
                encoding="utf-8",
            )

            (dwh_py_dir / "phase3c_dwh_pygnome_run_manifest.json").write_text(
                json.dumps({"phase": "phase3c_dwh_pygnome_comparator"}, indent=2),
                encoding="utf-8",
            )
            (dwh_py_dir / "phase3c_dwh_pygnome_loading_audit.json").write_text(
                json.dumps(
                    {
                        "waves_attached": False,
                        "structural_mismatch_note": "PyGNOME is comparator only.",
                    },
                    indent=2,
                ),
                encoding="utf-8",
            )

            (helper_dir / "metrics.py").write_text("def extract_gnome_budget_from_nc(path):\n    return None\n", encoding="utf-8")
            (helper_dir / "plotting.py").write_text("def plot_gnome_vs_openoil(*args, **kwargs):\n    return None\n", encoding="utf-8")
            (services_dir / "gnome_comparison.py").write_text("def run_comparison():\n    return {}\n", encoding="utf-8")

            service = Phase4CrossModelComparabilityAuditService(repo_root=root)
            results = service.run()

            matrix_path = Path(results["matrix_csv"])
            verdict_path = Path(results["verdict_md"])
            blockers_path = Path(results["blockers_md"])
            next_steps_path = Path(results["minimal_next_steps_md"])
            self.assertTrue(matrix_path.exists())
            self.assertTrue(verdict_path.exists())
            self.assertTrue(blockers_path.exists())
            self.assertTrue(next_steps_path.exists())

            matrix_df = pd.read_csv(matrix_path)
            self.assertEqual(len(matrix_df.index), 7)
            self.assertTrue((matrix_df["classification"] == "no_matched_phase4_pygnome_package_yet").all())
            self.assertTrue(
                (
                    matrix_df["classification_label"]
                    == "No matched Phase 4 PyGNOME comparison is packaged yet"
                ).all()
            )
            self.assertFalse(results["overall_verdict"]["scientifically_available_now"])
            self.assertFalse(results["overall_verdict"]["pilot_comparison_produced"])
            self.assertIn("weathering disabled", results["overall_verdict"]["biggest_blocker"])


if __name__ == "__main__":
    unittest.main()
