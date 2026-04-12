import tempfile
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pandas as pd

from src.services.pygnome_public_comparison import (
    FORECAST_VALIDATION_DATES,
    PyGnomePublicComparisonService,
    _resolve_opendrift_forcing_labels,
    recommend_public_comparison,
)


class PyGnomePublicComparisonTests(unittest.TestCase):
    def test_forecast_skill_dates_exclude_march3(self):
        self.assertEqual(FORECAST_VALIDATION_DATES, ["2023-03-04", "2023-03-05", "2023-03-06"])

    def test_public_comparison_lane_hard_sets_march4_5_6_dates(self):
        self.assertEqual(
            PyGnomePublicComparisonService._public_comparison_validation_dates(),
            ["2023-03-04", "2023-03-05", "2023-03-06"],
        )

    def test_recommend_all_weak_when_no_track_has_signal(self):
        summary = pd.DataFrame(
            [
                {
                    "track_id": "C1",
                    "pair_role": "eventcorridor_march4_6",
                    "fss_1km": 0.0,
                    "fss_3km": 0.0,
                    "fss_5km": 0.0,
                    "fss_10km": 0.0,
                    "iou": 0.0,
                    "dice": 0.0,
                    "forecast_nonzero_cells": 0,
                },
                {
                    "track_id": "C3",
                    "pair_role": "strict_march6",
                    "fss_1km": 0.0,
                    "fss_3km": 0.0,
                    "fss_5km": 0.0,
                    "fss_10km": 0.0,
                    "iou": 0.0,
                    "dice": 0.0,
                    "forecast_nonzero_cells": 0,
                },
            ]
        )
        recommendation = recommend_public_comparison(summary)
        self.assertIn("all are weak", recommendation["recommendation"])

    def test_recommend_pygnome_when_eventcorridor_is_best(self):
        summary = pd.DataFrame(
            [
                {
                    "track_id": "C2",
                    "pair_role": "eventcorridor_march4_6",
                    "fss_1km": 0.02,
                    "fss_3km": 0.02,
                    "fss_5km": 0.02,
                    "fss_10km": 0.02,
                    "iou": 0.01,
                    "dice": 0.02,
                    "forecast_nonzero_cells": 10,
                },
                {
                    "track_id": "C3",
                    "pair_role": "eventcorridor_march4_6",
                    "fss_1km": 0.08,
                    "fss_3km": 0.09,
                    "fss_5km": 0.10,
                    "fss_10km": 0.11,
                    "iou": 0.04,
                    "dice": 0.08,
                    "forecast_nonzero_cells": 20,
                },
            ]
        )
        recommendation = recommend_public_comparison(summary)
        self.assertEqual(recommendation["recommendation"], "PyGNOME is the better public-validation performer")
        self.assertEqual(recommendation["best_eventcorridor_track"], "C3")

    def test_prepare_opendrift_ensemble_track_uses_consolidated_member_union_semantics(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            model_dir = root / "model_run"
            track_dir = root / "products"
            manifest_path = model_dir / "ensemble" / "ensemble_manifest.json"
            member_paths = [
                model_dir / "ensemble" / "member_01.nc",
                model_dir / "ensemble" / "member_02.nc",
            ]
            for path in member_paths:
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("", encoding="utf-8")

            service = object.__new__(PyGnomePublicComparisonService)
            service.products_dir = track_dir
            service.validation_dates = ["2023-03-04", "2023-03-05", "2023-03-06"]
            service.eventcorridor_label = "2023-03-04_to_2023-03-06"
            service.case = type("Case", (), {"run_name": "CASE_MINDORO_RETRO_2023"})()

            def _fake_date_product(paths: list[Path], date: str, out_dir: Path, prefix: str) -> Path:
                self.assertEqual(paths, member_paths)
                self.assertEqual(prefix, "od_ensemble_consolidated")
                return out_dir / f"{prefix}_{date}_datecomposite.tif"

            event_path = (
                track_dir
                / "C2_od_ensemble_consolidated"
                / "od_ensemble_consolidated_eventcorridor_model_union_2023-03-04_to_2023-03-06.tif"
            )

            with (
                patch.object(service, "_load_opendrift_ensemble_member_paths", return_value=(manifest_path, member_paths)),
                patch.object(service, "_build_opendrift_ensemble_union_date_composite", side_effect=_fake_date_product),
                patch.object(service, "_build_model_eventcorridor_union", return_value=event_path),
                patch("src.services.pygnome_public_comparison.get_forcing_files", return_value={"current_source": "HYCOM", "wind_source": "GFS", "wave_source": "CMEMS"}),
            ):
                track = service._prepare_opendrift_ensemble_track(
                    model_dir,
                    {
                        "forecast_manifest_path": "forecast_manifest.json",
                        "ensemble_manifest_path": "ensemble_manifest.json",
                        "provenance": {
                            "transport_model": "oceandrift",
                            "provisional_transport_model": True,
                            "recipe_used": "cmems_era5",
                        },
                    },
                )

            self.assertEqual(track["track_id"], "C2")
            self.assertEqual(track["track_name"], "od_ensemble_consolidated_vs_public")
            self.assertEqual(track["model_name"], "OpenDrift consolidated ensemble trajectory")
            self.assertEqual(track["source_nc"], str(manifest_path))
            self.assertEqual(track["strict_march6_forecast"], track["date_forecasts"]["2023-03-06"])
            self.assertTrue(str(track["eventcorridor_forecast"]).endswith("od_ensemble_consolidated_eventcorridor_model_union_2023-03-04_to_2023-03-06.tif"))
            self.assertIn("unioned into one support mask", track["structural_limitations"])
            self.assertFalse(track["supports_strict_march6"])
            self.assertEqual(track["recipe_used"], "cmems_era5")
            self.assertEqual(track["current_source"], "HYCOM recipe currents")
            self.assertEqual(track["wind_source"], "GFS recipe winds")
            self.assertEqual(track["wave_stokes_status"], "CMEMS required and inherited from official_rerun_r1")

    def test_resolve_opendrift_forcing_labels_uses_recipe_config_sources(self):
        with patch(
            "src.services.pygnome_public_comparison.get_forcing_files",
            return_value={"current_source": "HYCOM", "wind_source": "GFS", "wave_source": "CMEMS"},
        ):
            labels = _resolve_opendrift_forcing_labels("hycom_gfs", "CASE_MINDORO_RETRO_2023")
        self.assertEqual(
            labels,
            {
                "current_source": "HYCOM recipe currents",
                "wind_source": "GFS recipe winds",
                "wave_source": "CMEMS",
            },
        )

    def test_build_eventcorridor_obs_union_uses_all_public_comparison_dates(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            service = object.__new__(PyGnomePublicComparisonService)
            service.validation_dates = ["2023-03-04", "2023-03-05", "2023-03-06"]
            service.eventcorridor_label = "2023-03-04_to_2023-03-06"
            service.obs_dir = root / "observations"
            service.obs_dir.mkdir(parents=True, exist_ok=True)
            service.grid = type("Grid", (), {"height": 2, "width": 2})()
            service.sea_mask = None

            path_4 = root / "obs_union_2023-03-04.tif"
            path_5 = root / "obs_union_2023-03-05.tif"
            path_6 = root / "obs_union_2023-03-06.tif"
            arrays = {
                path_4: np.array([[1.0, 0.0], [0.0, 0.0]], dtype=np.float32),
                path_5: np.array([[0.0, 1.0], [0.0, 0.0]], dtype=np.float32),
                path_6: np.array([[0.0, 0.0], [1.0, 0.0]], dtype=np.float32),
            }

            class DummyHelper:
                def __init__(self, values: dict[Path, np.ndarray]):
                    self.values = values
                    self.loaded: list[Path] = []

                def _load_binary_score_mask(self, path: Path) -> np.ndarray:
                    self.loaded.append(path)
                    return self.values[path]

            service.helper = DummyHelper(arrays)
            captured: dict[str, np.ndarray] = {}

            def _capture_save(_grid, array: np.ndarray, _path: Path) -> None:
                captured["array"] = array.copy()

            with patch("src.services.pygnome_public_comparison.save_raster", side_effect=_capture_save):
                output_path = service._build_eventcorridor_obs_union(
                    {
                        "2023-03-04": path_4,
                        "2023-03-05": path_5,
                        "2023-03-06": path_6,
                    }
                )

            self.assertEqual(
                service.helper.loaded,
                [path_4, path_5, path_6],
            )
            self.assertEqual(
                output_path,
                service.obs_dir / "eventcorridor_obs_union_2023-03-04_to_2023-03-06.tif",
            )
            np.testing.assert_array_equal(
                captured["array"],
                np.maximum(np.maximum(arrays[path_4], arrays[path_5]), arrays[path_6]),
            )

    def test_build_pairings_skips_strict_row_for_consolidated_ensemble_track(self):
        service = object.__new__(PyGnomePublicComparisonService)
        service.validation_dates = ["2023-03-04", "2023-03-05", "2023-03-06"]
        service.eventcorridor_label = "2023-03-04_to_2023-03-06"
        tracks = [
            {
                "track_id": "C1",
                "track_name": "od_deterministic_vs_public",
                "model_name": "OpenDrift deterministic control",
                "model_family": "OpenDrift",
                "supports_strict_march6": True,
                "strict_march6_forecast": Path("det_strict.tif"),
                "date_forecasts": {
                    "2023-03-04": Path("det_2023-03-04.tif"),
                    "2023-03-05": Path("det_2023-03-05.tif"),
                    "2023-03-06": Path("det_2023-03-06.tif"),
                },
                "eventcorridor_forecast": Path("det_event.tif"),
                "initialization_mode": "B_observation_initialized_polygon",
                "retention_coastline_action": "previous",
                "transport_model": "oceandrift",
                "provisional_transport_model": True,
                "recipe_used": "cmems_era5",
                "current_source": "cmems_era5 recipe currents",
                "wind_source": "cmems_era5 recipe winds",
                "wave_stokes_status": "same OpenDrift R1 forcing stack as official_rerun_r1",
                "structural_limitations": "",
            },
            {
                "track_id": "C2",
                "track_name": "od_ensemble_consolidated_vs_public",
                "model_name": "OpenDrift consolidated ensemble trajectory",
                "model_family": "OpenDrift",
                "supports_strict_march6": False,
                "strict_march6_forecast": Path("ensemble_strict.tif"),
                "date_forecasts": {
                    "2023-03-04": Path("ensemble_2023-03-04.tif"),
                    "2023-03-05": Path("ensemble_2023-03-05.tif"),
                    "2023-03-06": Path("ensemble_2023-03-06.tif"),
                },
                "eventcorridor_forecast": Path("ensemble_event.tif"),
                "initialization_mode": "B_observation_initialized_polygon",
                "retention_coastline_action": "previous",
                "transport_model": "oceandrift",
                "provisional_transport_model": True,
                "recipe_used": "cmems_era5",
                "current_source": "cmems_era5 recipe currents",
                "wind_source": "cmems_era5 recipe winds",
                "wave_stokes_status": "wave/Stokes required and inherited from official_rerun_r1",
                "structural_limitations": "broad by construction",
            },
        ]
        observations = {
            "strict_march6": Path("obs_mask_2023-03-06.tif"),
            "date_unions": {
                "2023-03-04": Path("obs_union_2023-03-04.tif"),
                "2023-03-05": Path("obs_union_2023-03-05.tif"),
                "2023-03-06": Path("obs_union_2023-03-06.tif"),
            },
            "eventcorridor_march4_6": Path("eventcorridor_obs_union_2023-03-04_to_2023-03-06.tif"),
        }

        pairings = service._build_pairings(tracks, observations)

        self.assertTrue((pairings["pair_id"] == "C1_strict_march6_2023-03-06").any())
        self.assertFalse((pairings["pair_id"] == "C2_strict_march6_2023-03-06").any())
        c2_rows = pairings.loc[pairings["track_id"] == "C2"]
        self.assertTrue((c2_rows["observation_product"] == "eventcorridor_obs_union_2023-03-04_to_2023-03-06.tif").any())
        self.assertTrue((c2_rows["observation_product"] == "obs_union_2023-03-05.tif").any())
        self.assertTrue((c2_rows["observation_product"] == "obs_union_2023-03-06.tif").any())
        event_row = c2_rows.loc[c2_rows["pair_role"] == "eventcorridor_march4_6"].iloc[0]
        self.assertEqual(event_row["validation_dates_used"], "2023-03-04,2023-03-05,2023-03-06")
        self.assertEqual(len(c2_rows), 4)


if __name__ == "__main__":
    unittest.main()
