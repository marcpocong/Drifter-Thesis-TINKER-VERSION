import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import pandas as pd

from src.services.prototype_legacy_phase4_pygnome_comparator import (
    PrototypeLegacyPhase4PygnomeComparatorService,
)


STYLE_YAML = """
palette:
  background_land: "#e6dfd1"
  background_sea: "#f7fbfd"
  shoreline: "#8b8178"
  observed_mask: "#2f3a46"
  deterministic_opendrift: "#165ba8"
  ensemble_consolidated: "#0f766e"
  ensemble_p50: "#1f7a4d"
  ensemble_p90: "#72b6ff"
  pygnome: "#9b4dca"
  source_point: "#b42318"
  initialization_polygon: "#d97706"
  validation_polygon: "#0f172a"
  centroid_path: "#111827"
  corridor_hull: "#c2410c"
  ensemble_member_path: "#94a3b8"
  oil_lighter: "#f28c28"
  oil_base: "#8c564b"
  oil_heavier: "#4b0082"
legend_labels:
  observed_mask: "Observed spill extent"
  deterministic_opendrift: "OpenDrift deterministic forecast"
  ensemble_consolidated: "OpenDrift consolidated ensemble trajectory"
  ensemble_p50: "OpenDrift ensemble p50 footprint"
  ensemble_p90: "OpenDrift ensemble p90 footprint"
  pygnome: "PyGNOME comparator"
  source_point: "Source point"
  initialization_polygon: "Initialization polygon"
  validation_polygon: "Validation target polygon"
  centroid_path: "Centroid path"
  corridor_hull: "Corridor / hull"
  ensemble_member_path: "Sampled ensemble trajectories"
  oil_lighter: "Light oil scenario"
  oil_base: "Fixed base medium-heavy proxy"
  oil_heavier: "Heavier oil scenario"
typography:
  font_family: "Arial"
layout:
  figure_facecolor: "#ffffff"
  axes_facecolor: "#f8fafc"
  grid_color: "#cbd5e1"
crop_rules:
  zoom_padding_fraction: 0.18
  close_padding_fraction: 0.08
  minimum_padding_m: 4000
  minimum_crop_span_m: 12000
locator_rules:
  mindoro_scale_km: 25
  dwh_scale_km: 100
  locator_padding_fraction: 0.55
"""


OIL_YAML = """
oils:
  light:
    adios_id: "GENERIC DIESEL"
    gnome_oil_type: "oil_diesel"
    display_name: "Light Oil"
  heavy:
    adios_id: "GENERIC HEAVY FUEL OIL"
    gnome_oil_type: "oil_6"
    display_name: "Heavy Oil"
simulation:
  duration_hours: 72
  time_step_minutes: 30
  num_particles: 100000
  initial_mass_tonnes: 50.0
gnome_comparison:
  enabled: true
"""


class PrototypeLegacyPhase4PygnomeComparatorTests(unittest.TestCase):
    case_id = "CASE_2016-09-01"

    def _build_fixture(self, root: Path) -> None:
        (root / "config").mkdir(parents=True, exist_ok=True)
        (root / "config" / "publication_figure_style.yaml").write_text(STYLE_YAML, encoding="utf-8")
        (root / "config" / "oil.yaml").write_text(OIL_YAML, encoding="utf-8")

        validation_dir = root / "output" / self.case_id / "validation"
        validation_dir.mkdir(parents=True, exist_ok=True)
        pd.DataFrame([{"recipe": "cmems_era5"}]).to_csv(validation_dir / "validation_ranking.csv", index=False)

        drifter_dir = root / "data" / "drifters" / self.case_id
        drifter_dir.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(
            {
                "time": ["2016-09-01T00:00:00Z", "2016-09-01T06:00:00Z"],
                "lat": [10.40, 10.45],
                "lon": [117.10, 117.18],
                "ID": ["DRIFTER_A", "DRIFTER_A"],
            }
        ).to_csv(drifter_dir / "drifters_noaa.csv", index=False)

        forcing_dir = root / "data" / "forcing" / self.case_id
        forcing_dir.mkdir(parents=True, exist_ok=True)
        for filename in ("cmems_curr.nc", "era5_wind.nc", "cmems_wave.nc"):
            (forcing_dir / filename).write_bytes(b"nc")

        weathering_dir = root / "output" / self.case_id / "weathering"
        weathering_dir.mkdir(parents=True, exist_ok=True)
        openoil_budget = pd.DataFrame(
            {
                "hours_elapsed": [0, 24, 48, 72],
                "surface_pct": [100.0, 90.0, 75.0, 60.0],
                "evaporated_pct": [0.0, 6.0, 12.0, 18.0],
                "dispersed_pct": [0.0, 4.0, 13.0, 22.0],
                "beached_pct": [0.0, 0.0, 0.0, 0.0],
            }
        )
        openoil_budget.to_csv(weathering_dir / "budget_light.csv", index=False)
        (openoil_budget.assign(surface_pct=[100.0, 82.0, 66.0, 51.0])).to_csv(
            weathering_dir / "budget_heavy.csv",
            index=False,
        )

    def test_run_builds_budget_only_outputs_and_manifest(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self._build_fixture(root)
            service = PrototypeLegacyPhase4PygnomeComparatorService(repo_root=root, case_ids=[self.case_id])

            def _fake_run(**kwargs):
                oil_key = kwargs["oil_key"]
                output_dir = service._comparator_dir(self.case_id)
                output_dir.mkdir(parents=True, exist_ok=True)
                nc_path = output_dir / f"pygnome_{oil_key}.nc"
                nc_path.write_bytes(b"nc")
                if oil_key == "light":
                    budget = pd.DataFrame(
                        {
                            "hours_elapsed": [0, 24, 48, 72],
                            "surface_pct": [100.0, 88.0, 73.0, 58.0],
                            "evaporated_pct": [0.0, 8.0, 14.0, 20.0],
                            "dispersed_pct": [0.0, 4.0, 13.0, 22.0],
                            "beached_pct": [0.0, 0.0, 0.0, 0.0],
                        }
                    )
                else:
                    budget = pd.DataFrame(
                        {
                            "hours_elapsed": [0, 24, 48, 72],
                            "surface_pct": [100.0, 78.0, 62.0, 48.0],
                            "evaporated_pct": [0.0, 7.0, 12.0, 16.0],
                            "dispersed_pct": [0.0, 15.0, 26.0, 36.0],
                            "beached_pct": [0.0, 0.0, 0.0, 0.0],
                        }
                    )
                budget.to_csv(output_dir / f"pygnome_budget_{oil_key}.csv", index=False)
                return budget, nc_path, {
                    "oil_key": oil_key,
                    "transport_forcing_mode": "matched_grid_wind_plus_grid_current",
                    "shoreline_comparison_available": False,
                }

            with mock.patch(
                "src.services.prototype_legacy_phase4_pygnome_comparator.GNOME_AVAILABLE",
                True,
            ), mock.patch.object(
                service.gnome_service,
                "run_matched_phase4_weathering_scenario",
                side_effect=_fake_run,
            ):
                results = service.run()

            self.assertFalse(results["full_phase4_comparator_feasible"])
            self.assertTrue(results["budget_only_feasible"])
            self.assertFalse(results["shoreline_comparison_feasible"])
            case_result = results["case_results"][0]
            output_dir = root / case_result["output_dir"]
            self.assertTrue((output_dir / "budget_time_series_light.png").exists())
            self.assertTrue((output_dir / "budget_time_series_heavy.png").exists())
            self.assertTrue((output_dir / "budget_comparison_board.png").exists())
            self.assertTrue((output_dir / "phase4_budget_comparison.csv").exists())
            self.assertTrue((output_dir / "phase4_budget_time_series_metrics.csv").exists())
            manifest = json.loads((output_dir / "pygnome_phase4_run_manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(manifest["selected_recipe"], "cmems_era5")
            self.assertTrue(manifest["budget_only_feasible"])
            self.assertFalse(manifest["shoreline_comparison_feasible"])
            self.assertIn("absolute percentage-point difference at 24/48/72 h", manifest["budget_metrics_description"])

            metrics = pd.read_csv(output_dir / "phase4_budget_time_series_metrics.csv")
            self.assertIn("mae_pct_points", metrics.columns)
            self.assertTrue((metrics["compartment"].astype(str) == "beached").any())


if __name__ == "__main__":
    unittest.main()
