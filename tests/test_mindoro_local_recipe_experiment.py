import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import pandas as pd
import yaml

from src.services.mindoro_local_recipe_experiment import MindoroLocalRecipeExperimentService


def _write_yaml(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _official_case_context_stub(case_definition_path: str | None = None) -> SimpleNamespace:
    return SimpleNamespace(
        workflow_mode="mindoro_retro_2023",
        workflow_flavor="official spill case",
        transport_track="official spill case",
        is_historical_regional=False,
        is_official=True,
        is_prototype=False,
        run_name="CASE_MINDORO_RETRO_2023",
        region=[115.0, 122.0, 6.0, 14.5],
        case_definition_path=case_definition_path,
        forcing_start_utc="2023-03-03T09:59:00Z",
        forcing_end_utc="2023-03-06T09:59:00Z",
    )


def _build_minimal_repo(root: Path) -> None:
    _write_yaml(
        root / "config" / "phase1_baseline_selection.yaml",
        {
            "baseline_id": "baseline_v1",
            "selected_recipe": "cmems_era5",
            "source_kind": "frozen_historical_artifact",
            "status_flag": "valid",
            "valid": True,
            "provisional": False,
            "rerun_required": False,
        },
    )
    _write_yaml(
        root / "config" / "case_mindoro_retro_2023.yaml",
        {
            "workflow_mode": "mindoro_retro_2023",
            "case_id": "CASE_MINDORO_RETRO_2023",
            "region": [115.0, 122.0, 6.0, 14.5],
            "forcing_bbox_halo_degrees": 0.5,
        },
    )
    _write_json(
        root / "data" / "arcgis" / "CASE_MINDORO_RETRO_2023" / "source_point_metadata.geojson",
        {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [121.528, 13.323]},
                    "properties": {"Id": 0},
                }
            ],
        },
    )


class MindoroLocalRecipeExperimentTests(unittest.TestCase):
    def test_load_source_point_reads_geojson_coordinates(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            path = root / "source.geojson"
            _write_json(
                path,
                {
                    "type": "FeatureCollection",
                    "features": [
                        {
                            "type": "Feature",
                            "geometry": {"type": "Point", "coordinates": [121.528, 13.323]},
                            "properties": {},
                        }
                    ],
                },
            )

            lon, lat = MindoroLocalRecipeExperimentService.load_source_point(path)

            self.assertAlmostEqual(lon, 121.528)
            self.assertAlmostEqual(lat, 13.323)

    def test_build_local_subset_registry_reproduces_current_repo_subset(self):
        repo_root = Path(__file__).resolve().parents[1]
        accepted_path = repo_root / "output" / "phase1_production_rerun" / "phase1_accepted_segment_registry.csv"
        source_point_path = (
            repo_root / "data" / "arcgis" / "CASE_MINDORO_RETRO_2023" / "source_point_metadata.geojson"
        )
        if not accepted_path.exists() or not source_point_path.exists():
            self.skipTest("Stored Phase 1 rerun outputs are not available in this checkout.")

        accepted_df = pd.read_csv(accepted_path)
        source_lon, source_lat = MindoroLocalRecipeExperimentService.load_source_point(source_point_path)

        subset_df = MindoroLocalRecipeExperimentService.build_local_subset_registry(
            accepted_df,
            source_lon=source_lon,
            source_lat=source_lat,
            threshold_km=250.0,
        )

        self.assertEqual(len(subset_df), 10)
        self.assertTrue((subset_df["nearest_endpoint_distance_km"] <= 250.0).all())
        self.assertTrue((subset_df["segment_status"].astype(str) == "accepted").all())
        self.assertIn(
            "300234062412680_20170801T060000Z_20170804T060000Z",
            set(subset_df["segment_id"].astype(str)),
        )

    def test_build_local_recipe_tables_ignores_invalid_rows_and_keeps_current_repo_winner(self):
        repo_root = Path(__file__).resolve().parents[1]
        accepted_path = repo_root / "output" / "phase1_production_rerun" / "phase1_accepted_segment_registry.csv"
        metrics_path = repo_root / "output" / "phase1_production_rerun" / "phase1_segment_metrics.csv"
        source_point_path = (
            repo_root / "data" / "arcgis" / "CASE_MINDORO_RETRO_2023" / "source_point_metadata.geojson"
        )
        if not accepted_path.exists() or not metrics_path.exists() or not source_point_path.exists():
            self.skipTest("Stored Phase 1 rerun outputs are not available in this checkout.")

        accepted_df = pd.read_csv(accepted_path)
        metrics_df = pd.read_csv(metrics_path)
        source_lon, source_lat = MindoroLocalRecipeExperimentService.load_source_point(source_point_path)
        subset_df = MindoroLocalRecipeExperimentService.build_local_subset_registry(
            accepted_df,
            source_lon=source_lon,
            source_lat=source_lat,
            threshold_km=250.0,
        )

        poisoned_rows = pd.DataFrame(
            [
                {
                    "segment_id": subset_df.iloc[0]["segment_id"],
                    "recipe": "hycom_era5",
                    "validity_flag": "invalid",
                    "ncs_score": -999.0,
                },
                {
                    "segment_id": subset_df.iloc[1]["segment_id"],
                    "recipe": "hycom_era5",
                    "validity_flag": "invalid",
                    "ncs_score": -999.0,
                },
            ]
        )
        augmented_metrics = pd.concat([metrics_df, poisoned_rows], ignore_index=True, sort=False)

        _, ranking_df, winner = MindoroLocalRecipeExperimentService.build_local_recipe_tables(
            augmented_metrics,
            subset_df["segment_id"],
        )

        self.assertEqual(winner, "cmems_gfs")
        self.assertEqual(str(ranking_df.iloc[0]["recipe"]), "cmems_gfs")

    def test_run_stages_candidate_and_manifest_without_mutating_canonical_baseline(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            _build_minimal_repo(root)
            (root / "output" / "phase1_production_rerun").mkdir(parents=True, exist_ok=True)

            accepted_df = pd.DataFrame(
                [
                    {
                        "segment_id": "SEG_A",
                        "segment_status": "accepted",
                        "drifter_id": "A",
                        "start_time_utc": "2017-01-01T00:00:00+00:00",
                        "end_time_utc": "2017-01-04T00:00:00+00:00",
                        "start_lat": 13.20,
                        "start_lon": 121.45,
                        "end_lat": 13.28,
                        "end_lon": 121.55,
                    },
                    {
                        "segment_id": "SEG_B",
                        "segment_status": "accepted",
                        "drifter_id": "B",
                        "start_time_utc": "2017-01-08T00:00:00+00:00",
                        "end_time_utc": "2017-01-11T00:00:00+00:00",
                        "start_lat": 13.10,
                        "start_lon": 121.60,
                        "end_lat": 13.22,
                        "end_lon": 121.50,
                    },
                    {
                        "segment_id": "SEG_FAR",
                        "segment_status": "accepted",
                        "drifter_id": "F",
                        "start_time_utc": "2017-02-01T00:00:00+00:00",
                        "end_time_utc": "2017-02-04T00:00:00+00:00",
                        "start_lat": 16.00,
                        "start_lon": 124.00,
                        "end_lat": 16.10,
                        "end_lon": 124.10,
                    },
                ]
            )
            accepted_df.to_csv(
                root / "output" / "phase1_production_rerun" / "phase1_accepted_segment_registry.csv",
                index=False,
            )
            metrics_df = pd.DataFrame(
                [
                    {"segment_id": "SEG_A", "recipe": "cmems_era5", "validity_flag": "valid", "ncs_score": 3.0},
                    {"segment_id": "SEG_B", "recipe": "cmems_era5", "validity_flag": "valid", "ncs_score": 3.2},
                    {"segment_id": "SEG_A", "recipe": "cmems_gfs", "validity_flag": "valid", "ncs_score": 2.0},
                    {"segment_id": "SEG_B", "recipe": "cmems_gfs", "validity_flag": "valid", "ncs_score": 2.2},
                    {"segment_id": "SEG_A", "recipe": "hycom_era5", "validity_flag": "valid", "ncs_score": 1.9},
                    {"segment_id": "SEG_B", "recipe": "hycom_era5", "validity_flag": "valid", "ncs_score": 2.8},
                    {"segment_id": "SEG_A", "recipe": "hycom_gfs", "validity_flag": "valid", "ncs_score": 2.6},
                    {"segment_id": "SEG_B", "recipe": "hycom_gfs", "validity_flag": "valid", "ncs_score": 2.9},
                ]
            )
            metrics_df.to_csv(
                root / "output" / "phase1_production_rerun" / "phase1_segment_metrics.csv",
                index=False,
            )

            captured_kwargs: dict[str, object] = {}

            class FakeComparisonService:
                def __init__(self, **kwargs):
                    captured_kwargs.update(kwargs)

                def run(self):
                    output_dir = (
                        root
                        / "output"
                        / "CASE_MINDORO_RETRO_2023"
                        / "mindoro_local_recipe_experiment"
                        / "event_recipe_comparison"
                    )
                    output_dir.mkdir(parents=True, exist_ok=True)
                    summary_df = pd.DataFrame(
                        [
                            {
                                "track_id": "OD_cmems_era5_B_det",
                                "pair_role": "eventcorridor_march4_6",
                                "model_family": "OpenDrift",
                                "recipe_id": "cmems_era5",
                                "branch_id": "B",
                                "product_kind": "deterministic",
                                "fss_1km": 0.10,
                                "fss_3km": 0.10,
                                "fss_5km": 0.10,
                                "fss_10km": 0.10,
                                "iou": 0.05,
                                "dice": 0.08,
                                "forecast_nonzero_cells": 100,
                                "nearest_distance_to_obs_m": 0.0,
                            },
                            {
                                "track_id": "OD_cmems_gfs_B_det",
                                "pair_role": "eventcorridor_march4_6",
                                "model_family": "OpenDrift",
                                "recipe_id": "cmems_gfs",
                                "branch_id": "B",
                                "product_kind": "deterministic",
                                "fss_1km": 0.20,
                                "fss_3km": 0.20,
                                "fss_5km": 0.20,
                                "fss_10km": 0.20,
                                "iou": 0.10,
                                "dice": 0.15,
                                "forecast_nonzero_cells": 120,
                                "nearest_distance_to_obs_m": 0.0,
                            },
                            {
                                "track_id": "OD_hycom_era5_B_det",
                                "pair_role": "eventcorridor_march4_6",
                                "model_family": "OpenDrift",
                                "recipe_id": "hycom_era5",
                                "branch_id": "B",
                                "product_kind": "deterministic",
                                "fss_1km": 0.30,
                                "fss_3km": 0.30,
                                "fss_5km": 0.30,
                                "fss_10km": 0.30,
                                "iou": 0.12,
                                "dice": 0.18,
                                "forecast_nonzero_cells": 140,
                                "nearest_distance_to_obs_m": 0.0,
                            },
                        ]
                    )
                    ranking_df = pd.DataFrame([{"track_id": "OD_hycom_era5_B_det"}])
                    summary_csv = output_dir / "summary.csv"
                    ranking_csv = output_dir / "ranking.csv"
                    report_md = output_dir / "report.md"
                    run_manifest = output_dir / "run_manifest.json"
                    summary_df.to_csv(summary_csv, index=False)
                    ranking_df.to_csv(ranking_csv, index=False)
                    report_md.write_text("# fake\n", encoding="utf-8")
                    run_manifest.write_text("{}", encoding="utf-8")
                    return {
                        "output_dir": output_dir,
                        "summary": summary_df,
                        "ranking": ranking_df,
                        "summary_csv": summary_csv,
                        "ranking_csv": ranking_csv,
                        "report_md": report_md,
                        "run_manifest": run_manifest,
                        "recipes": [
                            {"recipe_id": "cmems_era5", "available": True},
                            {"recipe_id": "cmems_gfs", "available": True},
                            {"recipe_id": "hycom_era5", "available": True},
                            {"recipe_id": "hycom_gfs", "available": True},
                        ],
                        "forcing_preparation": {
                            "gfs_requested": True,
                            "gfs_status": "downloaded",
                            "gfs_path": str(root / "data" / "forcing" / "CASE_MINDORO_RETRO_2023" / "gfs_wind.nc"),
                            "gfs_error": "",
                        },
                    }

            baseline_before = (root / "config" / "phase1_baseline_selection.yaml").read_text(encoding="utf-8")
            case_stub = _official_case_context_stub(
                str(root / "config" / "case_mindoro_retro_2023.yaml")
            )

            with mock.patch(
                "src.services.mindoro_local_recipe_experiment.get_case_context",
                return_value=case_stub,
            ):
                service = MindoroLocalRecipeExperimentService(
                    repo_root=root,
                    comparison_service_factory=FakeComparisonService,
                )
                results = service.run()

            self.assertEqual(results["local_candidate_recipe"], "cmems_gfs")
            self.assertEqual(results["event_best_recipe"], "hycom_era5")
            self.assertTrue(results["candidate_beats_frozen"])
            self.assertTrue((root / "output" / "CASE_MINDORO_RETRO_2023" / "mindoro_local_recipe_experiment" / "mindoro_local_candidate_baseline.yaml").exists())
            self.assertTrue((root / "output" / "CASE_MINDORO_RETRO_2023" / "mindoro_local_recipe_experiment" / "mindoro_local_recipe_experiment_manifest.json").exists())
            self.assertEqual(
                (root / "config" / "phase1_baseline_selection.yaml").read_text(encoding="utf-8"),
                baseline_before,
            )
            self.assertTrue(captured_kwargs["prepare_missing_gfs"])
            self.assertTrue(captured_kwargs["gfs_prepare_strict"])

            manifest = json.loads(
                (
                    root
                    / "output"
                    / "CASE_MINDORO_RETRO_2023"
                    / "mindoro_local_recipe_experiment"
                    / "mindoro_local_recipe_experiment_manifest.json"
                ).read_text(encoding="utf-8")
            )
            self.assertEqual(manifest["historical_subset"]["accepted_segment_count"], 2)
            self.assertEqual(manifest["local_candidate"]["selected_recipe"], "cmems_gfs")
            self.assertEqual(manifest["event_comparison"]["best_event_scale_recipe"]["recipe_id"], "hycom_era5")
            self.assertEqual(
                sorted(manifest["event_comparison"]["gfs_recipes_included"]),
                ["cmems_gfs", "hycom_gfs"],
            )

    def test_run_hard_fails_when_comparison_gfs_prep_fails(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            _build_minimal_repo(root)
            (root / "output" / "phase1_production_rerun").mkdir(parents=True, exist_ok=True)
            pd.DataFrame(
                [
                    {
                        "segment_id": "SEG_A",
                        "segment_status": "accepted",
                        "drifter_id": "A",
                        "start_time_utc": "2017-01-01T00:00:00+00:00",
                        "end_time_utc": "2017-01-04T00:00:00+00:00",
                        "start_lat": 13.20,
                        "start_lon": 121.45,
                        "end_lat": 13.28,
                        "end_lon": 121.55,
                    }
                ]
            ).to_csv(
                root / "output" / "phase1_production_rerun" / "phase1_accepted_segment_registry.csv",
                index=False,
            )
            pd.DataFrame(
                [
                    {"segment_id": "SEG_A", "recipe": "cmems_era5", "validity_flag": "valid", "ncs_score": 3.0},
                    {"segment_id": "SEG_A", "recipe": "cmems_gfs", "validity_flag": "valid", "ncs_score": 2.0},
                ]
            ).to_csv(
                root / "output" / "phase1_production_rerun" / "phase1_segment_metrics.csv",
                index=False,
            )

            class FailingComparisonService:
                def __init__(self, **kwargs):
                    self.kwargs = kwargs

                def run(self):
                    raise RuntimeError(
                        "Failed to prepare required GFS wind forcing for recipe_sensitivity_r1_multibranch: network unavailable"
                    )

            case_stub = _official_case_context_stub(
                str(root / "config" / "case_mindoro_retro_2023.yaml")
            )
            with mock.patch(
                "src.services.mindoro_local_recipe_experiment.get_case_context",
                return_value=case_stub,
            ):
                service = MindoroLocalRecipeExperimentService(
                    repo_root=root,
                    comparison_service_factory=FailingComparisonService,
                )
                with self.assertRaisesRegex(
                    RuntimeError,
                    "Failed to prepare required GFS wind forcing",
                ):
                    service.run()


if __name__ == "__main__":
    unittest.main()
