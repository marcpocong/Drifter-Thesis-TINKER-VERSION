import unittest
import tempfile
from pathlib import Path
import json

import pandas as pd

from src.services.final_validation_package import (
    DWH_FINAL_OUTPUT_DIR,
    DWH_OBSERVATION_SOURCE_MASKS,
    DWH_PUBLICATION_EXPORTS,
    DWH_SCIENTIFIC_SOURCE_EXPORTS,
    DWH_SUMMARY_EXPORTS,
    FinalValidationPackageService,
    MINDORO_B1_FINAL_OUTPUT_DIR,
    MINDORO_B1_PUBLICATION_EXPORTS,
    MINDORO_B1_SCIENTIFIC_SOURCE_EXPORTS,
    MINDORO_B1_SUMMARY_EXPORTS,
    MINDORO_MARCH14_TARGET_MASK_PATH,
    decide_final_structure,
    mean_fss,
)
from src.services.dwh_phase3c_metadata import DWH_BASE_CASE_CONFIG_PATH, DWH_PHASE3C_FINAL_NOTE_PATH


class FinalValidationPackageTests(unittest.TestCase):
    def test_mean_fss_averages_available_windows(self):
        row = {
            "fss_1km": 0.1,
            "fss_3km": 0.2,
            "fss_5km": 0.3,
            "fss_10km": 0.4,
        }
        self.assertAlmostEqual(mean_fss(row), 0.25)

    def test_decide_final_structure_returns_thesis_packaging_guidance(self):
        recommendation = decide_final_structure()
        self.assertIn("Mindoro B1", recommendation)
        self.assertIn("March 13 -> March 14", recommendation)
        self.assertIn("March 6 sparse", recommendation)
        self.assertIn("DWH Phase 3C", recommendation)
        self.assertIn("appendix", recommendation.lower())

    def test_mindoro_packaging_promotes_reinit_and_preserves_legacy_rows(self):
        service = object.__new__(FinalValidationPackageService)
        service._coerce_value = FinalValidationPackageService._coerce_value
        service._format_validation_dates = FinalValidationPackageService._format_validation_dates.__get__(service, FinalValidationPackageService)
        service._build_dwh_main_row = FinalValidationPackageService._build_dwh_main_row.__get__(service, FinalValidationPackageService)
        service._mindoro_primary_reinit_row = FinalValidationPackageService._mindoro_primary_reinit_row.__get__(service, FinalValidationPackageService)
        service._mindoro_primary_reinit_pairing_row = FinalValidationPackageService._mindoro_primary_reinit_pairing_row.__get__(service, FinalValidationPackageService)
        service._mindoro_legacy_strict_row = FinalValidationPackageService._mindoro_legacy_strict_row.__get__(service, FinalValidationPackageService)
        service._mindoro_legacy_strict_pairing_row = FinalValidationPackageService._mindoro_legacy_strict_pairing_row.__get__(service, FinalValidationPackageService)
        service._mindoro_legacy_support_row = FinalValidationPackageService._mindoro_legacy_support_row.__get__(service, FinalValidationPackageService)
        service._mindoro_crossmodel_rows = FinalValidationPackageService._mindoro_crossmodel_rows.__get__(service, FinalValidationPackageService)
        service._mindoro_crossmodel_top_row = FinalValidationPackageService._mindoro_crossmodel_top_row.__get__(service, FinalValidationPackageService)
        service._mindoro_dual_provenance_confirmation = FinalValidationPackageService._mindoro_dual_provenance_confirmation.__get__(service, FinalValidationPackageService)
        service._build_main_table = FinalValidationPackageService._build_main_table.__get__(service, FinalValidationPackageService)
        service._build_case_registry = FinalValidationPackageService._build_case_registry.__get__(service, FinalValidationPackageService)
        service._assert_mindoro_primary_semantics = FinalValidationPackageService._assert_mindoro_primary_semantics.__get__(service, FinalValidationPackageService)
        service._build_benchmark_table = FinalValidationPackageService._build_benchmark_table.__get__(service, FinalValidationPackageService)
        service._build_headlines = FinalValidationPackageService._build_headlines.__get__(service, FinalValidationPackageService)
        service.repo_root = Path(".").resolve()

        service.mindoro_reinit_summary = pd.DataFrame(
            [
                {
                    "branch_id": "R1_previous",
                    "fss_1km": 0.0,
                    "fss_3km": 0.044,
                    "fss_5km": 0.137,
                    "fss_10km": 0.249,
                    "iou": 0.0,
                    "dice": 0.0,
                    "centroid_distance_m": 2000.0,
                    "forecast_nonzero_cells": 5,
                    "obs_nonzero_cells": 22,
                    "validation_dates_used": "2023-03-14",
                }
            ]
        )
        service.mindoro_reinit_pairing = pd.DataFrame([{"branch_id": "R1_previous", "forecast_product": "mask_p50_2023-03-14_datecomposite.tif"}])
        service.mindoro_reinit_manifest = {
            "recipe": {
                "recipe": "cmems_gfs",
                "source_path": "config/phase1_baseline_selection.yaml",
            }
        }
        service.mindoro_phase1_confirmation_candidate = {
            "selected_recipe": "cmems_gfs",
        }
        service.phase3b_summary = pd.DataFrame(
            [
                {
                    "pair_id": "official_primary_march6",
                    "fss_1km": 0.0,
                    "fss_3km": 0.0,
                    "fss_5km": 0.0,
                    "fss_10km": 0.0,
                    "iou": 0.0,
                    "dice": 0.0,
                    "centroid_distance_m": 67500.0,
                    "forecast_nonzero_cells": 0,
                    "obs_nonzero_cells": 2,
                }
            ]
        )
        service.phase3b_pairing = pd.DataFrame([{"pair_id": "official_primary_march6", "forecast_product_type": "mask_p50"}])
        service.appendix_eventcorridor_diag = pd.DataFrame(
            [
                {
                    "fss_1km": 0.1722,
                    "fss_3km": 0.2004,
                    "fss_5km": 0.2166,
                    "fss_10km": 0.2438,
                    "iou": 0.15,
                    "dice": 0.25,
                    "centroid_distance_m": 1000.0,
                    "forecast_nonzero_cells": 30,
                    "obs_nonzero_cells": 40,
                }
            ]
        )
        service.mindoro_reinit_crossmodel_summary = pd.DataFrame(
            [
                {
                    "track_id": "R1_previous_reinit_p50",
                    "model_name": "OpenDrift R1 previous reinit p50",
                    "fss_1km": 0.0,
                    "fss_3km": 0.044,
                    "fss_5km": 0.137,
                    "fss_10km": 0.249,
                    "mean_fss": 0.1075,
                    "iou": 0.0,
                    "dice": 0.0,
                    "nearest_distance_to_obs_m": 1414.2,
                    "centroid_distance_m": 2000.0,
                    "forecast_nonzero_cells": 5,
                    "obs_nonzero_cells": 22,
                    "forecast_product": "mask_p50",
                    "transport_model": "oceandrift",
                    "provisional_transport_model": True,
                    "track_tie_break_order": 1,
                    "structural_limitations": "Shared-imagery caveat applies.",
                },
                {
                    "track_id": "pygnome_reinit_deterministic",
                    "model_name": "PyGNOME deterministic March 13 reinit comparator",
                    "fss_1km": 0.0,
                    "fss_3km": 0.0,
                    "fss_5km": 0.0,
                    "fss_10km": 0.024,
                    "mean_fss": 0.006,
                    "iou": 0.0,
                    "dice": 0.0,
                    "nearest_distance_to_obs_m": 6082.8,
                    "centroid_distance_m": 7000.0,
                    "forecast_nonzero_cells": 6,
                    "obs_nonzero_cells": 22,
                    "forecast_product": "pygnome_mask",
                    "transport_model": "pygnome",
                    "provisional_transport_model": True,
                    "track_tie_break_order": 3,
                    "structural_limitations": "Comparator only.",
                },
            ]
        )
        dwh_base_row = {
            "pair_role": "event_corridor",
            "pairing_date_utc": "2010-05-21_to_2010-05-23",
            "validation_dates": "2010-05-21_to_2010-05-23",
            "fss_1km": 0.4,
            "fss_3km": 0.5,
            "fss_5km": 0.6,
            "fss_10km": 0.7,
            "iou": 0.2,
            "dice": 0.3,
            "centroid_distance_m": 1000.0,
            "forecast_nonzero_cells": 20,
            "obs_nonzero_cells": 25,
            "provisional_transport_model": True,
        }
        service.dwh_deterministic_summary = pd.DataFrame(
            [{**dwh_base_row, "track_id": "opendrift_control", "run_type": "deterministic"}]
        )
        service.dwh_ensemble_summary = pd.DataFrame(
            [
                {**dwh_base_row, "track_id": "ensemble_p50", "run_type": "ensemble_p50"},
                {**dwh_base_row, "track_id": "ensemble_p90", "run_type": "ensemble_p90"},
            ]
        )
        service.dwh_cross_model_summary = pd.DataFrame(
            [
                {
                    **dwh_base_row,
                    "track_id": "opendrift_control",
                    "run_type": "deterministic",
                },
                {
                    **dwh_base_row,
                    "track_id": "pygnome_deterministic",
                    "run_type": "pygnome",
                },
            ]
        )
        service.dwh_cross_model_event = service.dwh_cross_model_summary.copy()

        main_table = service._build_main_table()
        case_registry = service._build_case_registry()
        service._assert_mindoro_primary_semantics(main_table, case_registry)
        benchmark_table = service._build_benchmark_table()
        headlines = service._build_headlines(main_table)

        a_rows = main_table.loc[main_table["track_id"] == "A"].copy()
        self.assertFalse(a_rows.empty)
        self.assertTrue((a_rows["row_role"].astype(str) == "comparator_only").all())
        self.assertTrue((a_rows["result_scope"].astype(str) == "same_case_cross_model_comparator_support").all())
        a_registry = case_registry.loc[case_registry["track_id"] == "A"].iloc[0]
        self.assertEqual(a_registry["main_text_priority"], "secondary")
        mindoro_primary_rows = case_registry.loc[
            (case_registry["case_id"] == "CASE_MINDORO_RETRO_2023")
            & (case_registry["main_text_priority"] == "primary")
        ]
        self.assertEqual(mindoro_primary_rows["track_id"].tolist(), ["B1"])
        b1 = main_table.loc[main_table["track_id"] == "B1"].iloc[0]
        self.assertEqual(b1["track_label"], "Mindoro March 13 -> March 14 NOAA reinit primary validation")
        self.assertAlmostEqual(float(b1["mean_fss"]), 0.1075, places=4)
        self.assertEqual(
            b1["thesis_phase_title"],
            "Phase 3B Observation-Based Spatial Validation Using Public Mindoro Spill Extents",
        )
        self.assertEqual(b1["stored_run_selected_recipe"], "cmems_gfs")
        self.assertEqual(b1["posthoc_phase1_confirmation_selected_recipe"], "cmems_gfs")
        self.assertTrue(bool(b1["matches_stored_b1_recipe"]))
        self.assertTrue((main_table["track_id"] == "B2").any())
        self.assertTrue((main_table["track_id"] == "B3").any())
        self.assertIn("primary_validation_mean_fss", benchmark_table.columns)
        self.assertIn("legacy_sparse_reference_mean_fss", benchmark_table.columns)
        self.assertIn("legacy_support_reference_mean_fss", benchmark_table.columns)
        self.assertNotIn("strict_march6_mean_fss", benchmark_table.columns)
        self.assertNotIn("multidate_mean_fss", benchmark_table.columns)
        self.assertIn("mindoro_primary_reinit", headlines)
        self.assertIn("mindoro_crossmodel_top", headlines)
        self.assertIn("mindoro_legacy_march6", headlines)
        self.assertIn("mindoro_legacy_broader_support", headlines)
        c1_registry = case_registry.loc[case_registry["track_id"] == "C1"].iloc[0]
        c2_registry = case_registry.loc[case_registry["track_id"] == "C2"].iloc[0]
        c3_registry = case_registry.loc[case_registry["track_id"] == "C3"].iloc[0]
        self.assertEqual(c1_registry["case_definition_path"], str(DWH_BASE_CASE_CONFIG_PATH))
        self.assertEqual(c1_registry["case_freeze_amendment_path"], str(DWH_PHASE3C_FINAL_NOTE_PATH))
        self.assertEqual(c1_registry["main_text_priority"], "primary")
        self.assertEqual(c2_registry["main_text_priority"], "secondary")
        self.assertIn("preferred probabilistic extension", str(c2_registry["notes"]))
        self.assertIn("comparator-only", str(c3_registry["track_label"]).lower())

    def test_build_mindoro_final_output_export_writes_nested_tree_and_registry(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            for exports in MINDORO_B1_PUBLICATION_EXPORTS.values():
                for source in exports.values():
                    if source is None:
                        continue
                    path = root / source
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_text("png\n", encoding="utf-8")
            for exports in MINDORO_B1_SCIENTIFIC_SOURCE_EXPORTS.values():
                for source in exports.values():
                    path = root / source
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_text("png\n", encoding="utf-8")
            for exports in MINDORO_B1_SUMMARY_EXPORTS.values():
                for source in exports.values():
                    path = root / source
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_text("summary\n", encoding="utf-8")
            target_mask = root / MINDORO_MARCH14_TARGET_MASK_PATH
            target_mask.parent.mkdir(parents=True, exist_ok=True)
            target_mask.write_text("tif\n", encoding="utf-8")

            service = object.__new__(FinalValidationPackageService)
            service.repo_root = root
            service._mindoro_final_output_readme = (
                FinalValidationPackageService._mindoro_final_output_readme.__get__(service, FinalValidationPackageService)
            )
            service._build_mindoro_final_output_export = (
                FinalValidationPackageService._build_mindoro_final_output_export.__get__(service, FinalValidationPackageService)
            )
            service._render_mindoro_target_mask_publication_png = lambda destination: destination.write_text(
                "generated png\n", encoding="utf-8"
            )

            export = service._build_mindoro_final_output_export(
                {
                    "matches_stored_b1_recipe": True,
                    "stored_run_recipe_source_path": "config/phase1_baseline_selection.yaml",
                    "stored_run_selected_recipe": "cmems_era5",
                    "posthoc_phase1_confirmation_workflow_mode": "phase1_mindoro_focus_pre_spill_2016_2023",
                    "posthoc_phase1_confirmation_candidate_baseline_path": "output/phase1_mindoro_focus_pre_spill_2016_2023/phase1_baseline_selection_candidate.yaml",
                    "posthoc_phase1_confirmation_selected_recipe": "cmems_era5",
                    "confirmation_interpretation": "Focused rerun selected the same recipe.",
                }
            )

            export_root = root / MINDORO_B1_FINAL_OUTPUT_DIR
            self.assertTrue((export_root / "publication" / "observations" / "march14_target_mask_on_grid.png").exists())
            self.assertTrue((export_root / "publication" / "opendrift_primary" / "mindoro_primary_validation_board.png").exists())
            self.assertTrue((export_root / "publication" / "comparator_pygnome" / "mindoro_crossmodel_board.png").exists())
            self.assertTrue((export_root / "scientific_source_pngs" / "opendrift_primary" / "qa_march14_reinit_R1_previous_overlay.png").exists())
            self.assertTrue((export_root / "scientific_source_pngs" / "comparator_pygnome" / "qa_march14_crossmodel_pygnome_reinit_deterministic_overlay.png").exists())
            self.assertTrue((export_root / "summary" / "opendrift_primary" / "march13_14_reinit_run_manifest.json").exists())
            self.assertTrue((export_root / "summary" / "comparator_pygnome" / "march13_14_reinit_crossmodel_run_manifest.json").exists())
            self.assertTrue((export_root / "manifests" / "final_output_manifest.json").exists())
            self.assertTrue((export_root / "manifests" / "phase3b_final_output_registry.csv").exists())
            self.assertTrue((export_root / "manifests" / "phase3b_final_output_registry.json").exists())
            self.assertTrue((export_root / "final_output_manifest.json").exists())
            manifest = json.loads((export_root / "manifests" / "final_output_manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(manifest["registry_path"], "output/Phase 3B March13-14 Final Output/manifests/phase3b_final_output_registry.csv")
            self.assertEqual(export["manifest_path"], "output/Phase 3B March13-14 Final Output/manifests/final_output_manifest.json")

    def test_build_dwh_final_output_export_writes_curated_tree_and_registry(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            for exports in DWH_PUBLICATION_EXPORTS.values():
                for source in exports.values():
                    path = root / source
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_text("png\n", encoding="utf-8")
            for exports in DWH_SCIENTIFIC_SOURCE_EXPORTS.values():
                for source in exports.values():
                    path = root / source
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_text("png\n", encoding="utf-8")
            for exports in DWH_SUMMARY_EXPORTS.values():
                for source in exports.values():
                    path = root / source
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_text("summary\n", encoding="utf-8")
            for source in DWH_OBSERVATION_SOURCE_MASKS.values():
                path = root / source
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("tif\n", encoding="utf-8")

            service = object.__new__(FinalValidationPackageService)
            service.repo_root = root
            service._dwh_final_output_readme = (
                FinalValidationPackageService._dwh_final_output_readme.__get__(service, FinalValidationPackageService)
            )
            service._build_dwh_final_output_export = (
                FinalValidationPackageService._build_dwh_final_output_export.__get__(service, FinalValidationPackageService)
            )
            service._render_dwh_observation_context_png = (
                lambda source_relative_path, destination, **kwargs: destination.write_text("generated png\n", encoding="utf-8")
            )

            export = service._build_dwh_final_output_export()

            export_root = root / DWH_FINAL_OUTPUT_DIR
            self.assertTrue((export_root / "publication" / "observations" / "dwh_2010-05-21_24h_observation_truth_context.png").exists())
            self.assertTrue((export_root / "publication" / "opendrift_deterministic" / "dwh_24h_48h_72h_deterministic_footprint_overview_board.png").exists())
            self.assertTrue((export_root / "publication" / "opendrift_ensemble" / "dwh_2010-05-21_24h_mask_p50_overlay.png").exists())
            self.assertTrue((export_root / "publication" / "comparator_pygnome" / "dwh_2010-05-21_24h_pygnome_footprint_overlay.png").exists())
            self.assertTrue((export_root / "publication" / "context_optional" / "dwh_ensemble_sampled_trajectory_context.png").exists())
            self.assertTrue((export_root / "scientific_source_pngs" / "deterministic" / "qa_phase3c_eventcorridor_overlay.png").exists())
            self.assertTrue((export_root / "scientific_source_pngs" / "ensemble" / "qa_phase3c_ensemble_eventcorridor_overlay.png").exists())
            self.assertTrue((export_root / "scientific_source_pngs" / "comparator_pygnome" / "qa_phase3c_dwh_pygnome_eventcorridor_overlay.png").exists())
            self.assertTrue((export_root / "summary" / "deterministic" / "phase3c_run_manifest.json").exists())
            self.assertTrue((export_root / "summary" / "ensemble" / "phase3c_ensemble_run_manifest.json").exists())
            self.assertTrue((export_root / "summary" / "comparator_pygnome" / "phase3c_dwh_pygnome_run_manifest.json").exists())
            self.assertTrue((export_root / "manifests" / "phase3c_final_output_manifest.json").exists())
            self.assertTrue((export_root / "manifests" / "phase3c_final_output_registry.csv").exists())
            self.assertTrue((export_root / "manifests" / "phase3c_final_output_registry.json").exists())
            manifest = json.loads((export_root / "manifests" / "phase3c_final_output_manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(manifest["registry_path"], "output/Phase 3C DWH Final Output/manifests/phase3c_final_output_registry.csv")
            self.assertFalse(manifest["scientific_rerun_triggered"])
            self.assertEqual(export["manifest_path"], "output/Phase 3C DWH Final Output/manifests/phase3c_final_output_manifest.json")


if __name__ == "__main__":
    unittest.main()
