import unittest

import pandas as pd

from src.services.final_validation_package import (
    FinalValidationPackageService,
    decide_final_structure,
    mean_fss,
)


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
        service._build_main_table = FinalValidationPackageService._build_main_table.__get__(service, FinalValidationPackageService)
        service._build_benchmark_table = FinalValidationPackageService._build_benchmark_table.__get__(service, FinalValidationPackageService)
        service._build_headlines = FinalValidationPackageService._build_headlines.__get__(service, FinalValidationPackageService)

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
        benchmark_table = service._build_benchmark_table()
        headlines = service._build_headlines(main_table)

        b1 = main_table.loc[main_table["track_id"] == "B1"].iloc[0]
        self.assertEqual(b1["track_label"], "Mindoro March 13 -> March 14 NOAA reinit primary validation")
        self.assertAlmostEqual(float(b1["mean_fss"]), 0.1075, places=4)
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


if __name__ == "__main__":
    unittest.main()
