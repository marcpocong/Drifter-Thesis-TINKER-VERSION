import json
import tempfile
import unittest
from pathlib import Path

import pandas as pd

from src.services.phase1_finalization_audit import Phase1FinalizationAuditService
from src.utils.io import get_official_phase1_recipe_family, get_phase1_legacy_recipe_aliases


class Phase1FinalizationAuditTests(unittest.TestCase):
    def test_helpers_read_recipe_architecture_metadata(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config_dir = root / "config"
            config_dir.mkdir(parents=True, exist_ok=True)
            recipes_path = config_dir / "recipes.yaml"
            recipes_path.write_text(
                """
recipes:
  cmems_era5:
    description: test
    currents_file: cmems_curr.nc
    wind_file: era5_wind.nc
    wave_file: cmems_wave.nc
    duration_hours: 72
    time_step_minutes: 60
phase1_recipe_architecture:
  official_recipe_family:
    - cmems_era5
    - cmems_gfs
  legacy_recipe_name_aliases:
    cmems_ncep:
      chapter3_target_recipe: cmems_gfs
      status: legacy_name_only
""",
                encoding="utf-8",
            )

            self.assertEqual(get_official_phase1_recipe_family(recipes_path), ["cmems_era5", "cmems_gfs"])
            aliases = get_phase1_legacy_recipe_aliases(recipes_path)
            self.assertEqual(aliases["cmems_ncep"]["chapter3_target_recipe"], "cmems_gfs")

    def test_audit_writes_required_outputs_and_reports_missing_registries(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "config").mkdir(parents=True, exist_ok=True)
            (root / "output" / "CASE_2016-09-01" / "validation").mkdir(parents=True, exist_ok=True)
            (root / "output" / "CASE_2016-09-06" / "validation").mkdir(parents=True, exist_ok=True)
            (root / "output" / "CASE_2016-09-17" / "validation").mkdir(parents=True, exist_ok=True)
            (root / "output" / "final_validation_package").mkdir(parents=True, exist_ok=True)
            (root / "data" / "drifters" / "CASE_2016-09-01").mkdir(parents=True, exist_ok=True)

            (root / "README.md").write_text("prototype_2016\n", encoding="utf-8")
            (root / "config" / "settings.yaml").write_text(
                """
mindoro_case_domain: [115.0, 122.0, 6.0, 14.5]
legacy_prototype_display_domain: [115.0, 122.0, 6.0, 14.5]
phase1_validation_box: [115.0, 122.0, 6.0, 14.5]
workflow_mode: prototype_2016
""",
                encoding="utf-8",
            )
            (root / "config" / "recipes.yaml").write_text(
                """
recipes:
  hycom_ncep:
    description: "HYCOM currents + NCEP winds"
    currents_file: "hycom_curr.nc"
    wind_file: "ncep_wind.nc"
    wave_file: "cmems_wave.nc"
    duration_hours: 72
    time_step_minutes: 60
  hycom_era5:
    description: "HYCOM currents + ERA5 winds"
    currents_file: "hycom_curr.nc"
    wind_file: "era5_wind.nc"
    wave_file: "cmems_wave.nc"
    duration_hours: 72
    time_step_minutes: 60
  cmems_ncep:
    description: "CMEMS currents + NCEP winds"
    currents_file: "cmems_curr.nc"
    wind_file: "ncep_wind.nc"
    wave_file: "cmems_wave.nc"
    duration_hours: 72
    time_step_minutes: 60
  cmems_era5:
    description: "CMEMS currents + ERA5 winds"
    currents_file: "cmems_curr.nc"
    wind_file: "era5_wind.nc"
    wave_file: "cmems_wave.nc"
    duration_hours: 72
    time_step_minutes: 60
phase1_recipe_architecture:
  official_recipe_family:
    - cmems_era5
    - cmems_gfs
    - hycom_era5
    - hycom_gfs
  legacy_recipe_name_aliases:
    cmems_ncep:
      chapter3_target_recipe: cmems_gfs
      status: legacy_name_only
    hycom_ncep:
      chapter3_target_recipe: hycom_gfs
      status: legacy_name_only
""",
                encoding="utf-8",
            )
            (root / "config" / "phase1_baseline_selection.yaml").write_text(
                """
baseline_id: phase1_historical_transport_baseline_v1
description: Frozen prototype baseline
selected_recipe: cmems_era5
source_kind: frozen_historical_artifact
status_flag: valid
valid: true
provisional: false
rerun_required: false
historical_validation_artifacts:
  - output/CASE_2016-09-01/validation/validation_ranking.csv
  - output/CASE_2016-09-06/validation/validation_ranking.csv
  - output/CASE_2016-09-17/validation/validation_ranking.csv
chapter3_finalization_audit:
  phase1_validation_box: [115.0, 122.0, 6.0, 14.5]
  segment_policy:
    horizon_hours: 72
""",
                encoding="utf-8",
            )
            (root / "output" / "final_validation_package" / "final_validation_manifest.json").write_text(
                json.dumps({"phase": "final_validation_package"}),
                encoding="utf-8",
            )
            (root / "data" / "drifters" / "CASE_2016-09-01" / "drifters_noaa.csv").write_text(
                "time,lat,lon,ID,ve,vn\n2016-09-01T00:00:00Z,10.0,117.0,1,0.0,0.0\n",
                encoding="utf-8",
            )

            for case_date, winner in (
                ("2016-09-01", "cmems_era5"),
                ("2016-09-06", "cmems_ncep"),
                ("2016-09-17", "cmems_era5"),
            ):
                validation_dir = root / "output" / f"CASE_{case_date}" / "validation"
                pd.DataFrame(
                    [{"recipe": winner, "ncs_score": 1.0, "map_file": f"output/CASE_{case_date}/validation/map_{winner}.png"}]
                ).to_csv(validation_dir / "validation_ranking.csv", index=False)
                pd.DataFrame(
                    [
                        {
                            "case_name": f"CASE_{case_date}",
                            "recipe": winner,
                            "validity_flag": "valid",
                            "invalidity_reason": "",
                        }
                    ]
                ).to_csv(validation_dir / "phase1_loading_audit.csv", index=False)

            service = Phase1FinalizationAuditService(repo_root=root)
            results = service.run()

            self.assertFalse(results["overall_verdict"]["scientifically_ready"])
            self.assertTrue(results["overall_verdict"]["full_production_rerun_required"])
            self.assertTrue(Path(results["status_csv"]).exists())
            self.assertTrue(Path(results["status_json"]).exists())
            self.assertTrue(Path(results["memo_md"]).exists())
            self.assertTrue(Path(results["verdict_md"]).exists())

            status_df = pd.read_csv(results["status_csv"])
            accepted_row = status_df[status_df["requirement_id"] == "accepted_segment_registry"].iloc[0]
            loading_row = status_df[status_df["requirement_id"] == "loading_audit_hard_fail_behavior"].iloc[0]
            baseline_row = status_df[status_df["requirement_id"] == "frozen_baseline_artifact"].iloc[0]
            separation_row = status_df[status_df["requirement_id"] == "transport_vs_spill_validation_separation"].iloc[0]

            self.assertEqual(accepted_row["classification"], "missing")
            self.assertEqual(loading_row["classification"], "implemented_but_provisional")
            self.assertEqual(baseline_row["classification"], "implemented_but_provisional")
            self.assertEqual(separation_row["classification"], "implemented_and_scientifically_ready")


if __name__ == "__main__":
    unittest.main()
