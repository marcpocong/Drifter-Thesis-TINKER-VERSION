import json
import tempfile
import unittest
from pathlib import Path

import numpy as np
import pandas as pd
import rasterio
from matplotlib import pyplot as plt
from rasterio.transform import from_origin

from src.services.figure_package_publication import (
    FigurePackagePublicationService,
    build_publication_figure_filename,
    load_publication_style_config,
)


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _find_spec(specs: list[dict], spec_id: str) -> dict:
    for spec in specs:
        if spec["spec_id"] == spec_id:
            return spec
    raise AssertionError(f"Missing spec_id: {spec_id}")


def _image_dimensions(path: str | Path) -> tuple[int, int]:
    image = plt.imread(path)
    return int(image.shape[1]), int(image.shape[0])


def _write_png(path: Path, width: int = 320, height: int = 180) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    image = np.ones((height, width, 3), dtype=float)
    image[:, :, 0] = 0.85
    image[:, :, 1] = 0.9
    image[:, :, 2] = 0.98
    plt.imsave(path, image)


def _write_tif(path: Path, value: int = 1, width: int = 8, height: int = 8) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    data = np.zeros((height, width), dtype=np.uint8)
    data[2:6, 2:6] = value
    transform = from_origin(120.9, 13.8, 0.02, 0.02)
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=height,
        width=width,
        count=1,
        dtype=data.dtype,
        crs="EPSG:4326",
        transform=transform,
    ) as dataset:
        dataset.write(data, 1)


STYLE_YAML = """
title_format: "{figure_title}"
subtitle_format: "{case_label} | {date_label} | {phase_label}"
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
  title_size: 19
  subtitle_size: 10
  panel_title_size: 11
  legend_title_size: 9
  body_size: 9
  note_size: 8
layout:
  board_size_inches: [16, 9]
  single_size_inches: [13, 8]
  dpi: 120
  figure_facecolor: "#ffffff"
  axes_facecolor: "#f7fbfd"
  grid_color: "#cbd5e1"
  legend_facecolor: "#ffffff"
  legend_edgecolor: "#94a3b8"
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

MINDORO_PHASE3B_SUMMARY_CSV = """pair_id,obs_nonzero_cells,forecast_nonzero_cells,fss_1km,fss_3km,fss_5km,fss_10km
official_primary_march6,2,3,0.111,0.222,0.333,0.444
"""

MINDORO_REINIT_SUMMARY_CSV = """branch_id,model_name,validation_dates_used,fss_1km,fss_3km,fss_5km,fss_10km,mean_fss,forecast_nonzero_cells,obs_nonzero_cells,forecast_path
R1_previous,OpenDrift R1 previous reinit p50,2023-03-14,0.000,0.044,0.137,0.249,0.1075,5,22,output/CASE_MINDORO_RETRO_2023/phase3b_extended_public_scored_march13_14_reinit/r1_previous_mask.tif
R0,OpenDrift R0 reinit p50,2023-03-14,0.000,0.000,0.000,0.000,0.0000,0,22,output/CASE_MINDORO_RETRO_2023/phase3b_extended_public_scored_march13_14_reinit/r0_mask.tif
"""

MINDORO_REINIT_CROSSMODEL_SUMMARY_CSV = """track_id,model_name,validation_dates_used,fss_1km,fss_3km,fss_5km,fss_10km,mean_fss,iou,dice,nearest_distance_to_obs_m,forecast_path
R1_previous_reinit_p50,OpenDrift R1 previous reinit p50,2023-03-14,0.000,0.044,0.137,0.249,0.1075,0.000,0.000,1414.2,output/CASE_MINDORO_RETRO_2023/phase3b_extended_public_scored_march13_14_reinit_pygnome_comparison/r1_crossmodel_mask.tif
R0_reinit_p50,OpenDrift R0 reinit p50,2023-03-14,0.000,0.000,0.000,0.000,0.0000,0.000,0.000,9999.0,output/CASE_MINDORO_RETRO_2023/phase3b_extended_public_scored_march13_14_reinit_pygnome_comparison/r0_crossmodel_mask.tif
pygnome_reinit_deterministic,PyGNOME deterministic March 13 reinit comparator,2023-03-14,0.000,0.000,0.000,0.024,0.0061,0.000,0.000,6082.8,output/CASE_MINDORO_RETRO_2023/phase3b_extended_public_scored_march13_14_reinit_pygnome_comparison/pygnome_crossmodel_mask.tif
"""

DWH_SUMMARY_CSV = """pair_role,pairing_date_utc,fss_1km,fss_3km,fss_5km,fss_10km
per_date,2010-05-21,0.111,0.222,0.333,0.444
per_date,2010-05-22,0.211,0.322,0.433,0.544
per_date,2010-05-23,0.311,0.422,0.533,0.644
event_corridor,2010-05-21/2010-05-23,0.411,0.522,0.633,0.744
"""

DWH_ALL_RESULTS_CSV = """model_result,pair_role,pairing_date_utc,fss_1km,fss_3km,fss_5km,fss_10km,mean_fss
OpenDrift deterministic,per_date,2010-05-21,0.111,0.222,0.333,0.444,0.278
OpenDrift deterministic,per_date,2010-05-22,0.121,0.232,0.343,0.454,0.288
OpenDrift deterministic,per_date,2010-05-23,0.131,0.242,0.353,0.464,0.298
OpenDrift deterministic,event_corridor,2010-05-21/2010-05-23,0.410,0.520,0.630,0.740,0.575
OpenDrift ensemble p50,per_date,2010-05-21,0.151,0.261,0.371,0.481,0.316
OpenDrift ensemble p50,per_date,2010-05-22,0.211,0.322,0.433,0.544,0.378
OpenDrift ensemble p50,per_date,2010-05-23,0.171,0.282,0.393,0.504,0.338
OpenDrift ensemble p50,event_corridor,2010-05-21/2010-05-23,0.150,0.260,0.370,0.480,0.315
OpenDrift ensemble p90,per_date,2010-05-21,0.170,0.280,0.390,0.500,0.335
OpenDrift ensemble p90,per_date,2010-05-22,0.181,0.291,0.401,0.511,0.346
OpenDrift ensemble p90,per_date,2010-05-23,0.191,0.301,0.411,0.521,0.356
OpenDrift ensemble p90,event_corridor,2010-05-21/2010-05-23,0.170,0.280,0.390,0.500,0.335
PyGNOME deterministic,per_date,2010-05-21,0.161,0.271,0.381,0.491,0.326
PyGNOME deterministic,per_date,2010-05-22,0.171,0.281,0.391,0.501,0.336
PyGNOME deterministic,per_date,2010-05-23,0.190,0.290,0.390,0.490,0.340
PyGNOME deterministic,event_corridor,2010-05-21/2010-05-23,0.190,0.290,0.390,0.490,0.340
"""


class FigurePackagePublicationTests(unittest.TestCase):
    def _build_service_fixture(self, root: Path) -> FigurePackagePublicationService:
        _write_text(root / "config" / "publication_figure_style.yaml", STYLE_YAML)
        _write_text(root / "config" / "publication_map_labels_mindoro.csv", "label_text,lon,lat,enabled_yes_no\nMindoro,121.0,13.0,yes\n")
        _write_text(root / "config" / "publication_map_labels_dwh.csv", "label_text,lon,lat,enabled_yes_no\nDWH,-88.0,28.7,yes\n")
        _write_text(
            root / "output" / "final_reproducibility_package" / "final_phase_status_registry.csv",
            "phase_id,track_id,scientifically_reportable,scientifically_frozen\nphase2,phase2_machine_readable_forecast,True,False\nphase4,mindoro_phase4,True,False\n",
        )
        _write_json(
            root / "output" / "CASE_MINDORO_RETRO_2023" / "forecast" / "forecast_manifest.json",
            {
                "grid": {
                    "crs": "EPSG:32651",
                    "extent": [274000.0, 1355000.0, 398000.0, 1524000.0],
                    "display_bounds_wgs84": [120.9096, 122.0622, 12.2494, 13.7837],
                },
                "source_geometry": {},
            },
        )
        _write_json(root / "output" / "phase4" / "CASE_MINDORO_RETRO_2023" / "phase4_run_manifest.json", {})
        _write_json(root / "output" / "CASE_DWH_RETRO_2010_72H" / "phase3c_external_case_run" / "phase3c_run_manifest.json", {})
        _write_text(root / "output" / "CASE_MINDORO_RETRO_2023" / "phase3b" / "phase3b_summary.csv", MINDORO_PHASE3B_SUMMARY_CSV)
        _write_text(
            root / "output" / "CASE_MINDORO_RETRO_2023" / "phase3b_extended_public_scored_march13_14_reinit" / "march13_14_reinit_summary.csv",
            MINDORO_REINIT_SUMMARY_CSV,
        )
        _write_text(
            root / "output" / "CASE_MINDORO_RETRO_2023" / "phase3b_extended_public_scored_march13_14_reinit_pygnome_comparison" / "march13_14_reinit_crossmodel_summary.csv",
            MINDORO_REINIT_CROSSMODEL_SUMMARY_CSV,
        )
        _write_text(
            root / "output" / "phase1_mindoro_focus_pre_spill_2016_2023" / "phase1_baseline_selection_candidate.yaml",
            "selected_recipe: cmems_gfs\n",
        )
        _write_text(
            root / "config" / "phase1_baseline_selection.yaml",
            "\n".join(
                [
                    "selected_recipe: cmems_gfs",
                    "historical_four_recipe_winner: cmems_gfs",
                    "gfs_historical_winner_not_adopted: false",
                    "chapter3_finalization_audit:",
                    "  phase1_validation_box: [118.751, 124.305, 10.620, 16.026]",
                ]
            )
            + "\n",
        )
        _write_text(
            root / "config" / "case_mindoro_retro_2023.yaml",
            "mindoro_case_domain: [115.0, 122.0, 6.0, 14.5]\n",
        )
        _write_text(
            root / "docs" / "DOMAIN_GLOSSARY.md",
            "# Domain Glossary\n\n- Focused Mindoro Phase 1 validation box\n- mindoro_case_domain\n- scoring_grid.display_bounds_wgs84\n- prototype_2016 first-code search box\n",
        )
        _write_json(
            root / "output" / "2016 Legacy Runs FINAL Figures" / "manifests" / "prototype_2016_provenance_metadata.json",
            {
                "prototype_2016_initial_capture_box": [108.6465, 121.3655, 6.1865, 20.3515],
                "prototype_2016_initial_capture_source_boxes": [
                    [113.267, 121.267, 6.3685, 14.3685],
                    [113.3655, 121.3655, 6.1865, 14.1865],
                    [108.6465, 116.6465, 12.3515, 20.3515],
                ],
                "historical_origin_summary": "The very first prototype code used the shared first-code search box [108.6465, 121.3655, 6.1865, 20.3515] on the west coast of the Philippines and surfaced the first three 2016 drifter cases.",
                "operative_extent_note": "The stored case-local prototype extents remain the operative scientific/display extents.",
            },
        )
        for rel_path, value in (
            ("output/CASE_MINDORO_RETRO_2023/phase3b_extended_public_scored_march13_14_reinit/march13_seed_mask_on_grid.tif", 1),
            ("output/CASE_MINDORO_RETRO_2023/phase3b_extended_public/accepted_obs_masks/10b37c42a9754363a5f7b14199b077e6.tif", 1),
            ("output/CASE_MINDORO_RETRO_2023/phase3b_extended_public_scored_march13_14_reinit/r1_previous_mask.tif", 1),
            ("output/CASE_MINDORO_RETRO_2023/phase3b_extended_public_scored_march13_14_reinit/r0_mask.tif", 1),
            ("output/CASE_MINDORO_RETRO_2023/phase3b_extended_public_scored_march13_14_reinit_pygnome_comparison/r1_crossmodel_mask.tif", 1),
            ("output/CASE_MINDORO_RETRO_2023/phase3b_extended_public_scored_march13_14_reinit_pygnome_comparison/r0_crossmodel_mask.tif", 1),
            ("output/CASE_MINDORO_RETRO_2023/phase3b_extended_public_scored_march13_14_reinit_pygnome_comparison/pygnome_crossmodel_mask.tif", 1),
        ):
            _write_tif(root / rel_path, value=value)
        for rel_path in (
            "output/CASE_MINDORO_RETRO_2023/phase3b/qa_phase3b_obsmask_vs_p50.png",
            "output/CASE_MINDORO_RETRO_2023/phase3b/qa_phase3b_source_init_validation_overlay.png",
            "output/CASE_MINDORO_RETRO_2023/phase3b_extended_public_scored_march13_14_reinit/qa_march13_seed_mask_on_grid.png",
            "output/CASE_MINDORO_RETRO_2023/phase3b_extended_public_scored_march13_14_reinit/qa_march13_seed_vs_march14_target.png",
            "output/CASE_MINDORO_RETRO_2023/phase3b_extended_public_scored_march13_14_reinit/qa_march14_reinit_R1_previous_overlay.png",
            "output/CASE_MINDORO_RETRO_2023/phase3b_extended_public_scored_march13_14_reinit/qa_march14_reinit_R0_overlay.png",
            "output/CASE_MINDORO_RETRO_2023/phase3b_extended_public_scored_march13_14_reinit_pygnome_comparison/qa/qa_march14_crossmodel_R1_previous_reinit_p50_overlay.png",
            "output/CASE_MINDORO_RETRO_2023/phase3b_extended_public_scored_march13_14_reinit_pygnome_comparison/qa/qa_march14_crossmodel_R0_reinit_p50_overlay.png",
            "output/CASE_MINDORO_RETRO_2023/phase3b_extended_public_scored_march13_14_reinit_pygnome_comparison/qa/qa_march14_crossmodel_pygnome_reinit_deterministic_overlay.png",
        ):
            _write_png(root / rel_path, width=480, height=300)
        _write_text(root / "output" / "CASE_DWH_RETRO_2010_72H" / "phase3c_external_case_run" / "phase3c_summary.csv", DWH_SUMMARY_CSV)
        _write_text(
            root / "output" / "CASE_DWH_RETRO_2010_72H" / "phase3c_dwh_pygnome_comparator" / "phase3c_dwh_all_results_table.csv",
            DWH_ALL_RESULTS_CSV,
        )
        return FigurePackagePublicationService(repo_root=root)

    def _write_prototype_support_fixture(self, root: Path) -> None:
        single_path = root / "output" / "prototype_2016_pygnome_similarity" / "figures" / "case_2016_09_01__prototype_2016__24h__opendrift__single.png"
        board_path = root / "output" / "prototype_2016_pygnome_similarity" / "figures" / "case_2016_09_01__prototype_2016__24_48_72h__opendrift_vs_pygnome__board.png"
        _write_png(single_path, width=480, height=300)
        _write_png(board_path, width=720, height=540)
        _write_text(
            root / "output" / "prototype_2016_pygnome_similarity" / "prototype_pygnome_figure_registry.csv",
            "\n".join(
                [
                    "figure_id,case_id,phase_or_track,date_token,timestamp_utc,hour,model_name,model_label,run_type,view_type,variant,figure_title,relative_path,file_path,pixel_width,pixel_height,short_plain_language_interpretation,source_paths,notes,legacy_debug_only,pygnome_role",
                    f"prototype_single,CASE_2016-09-01,prototype_pygnome_similarity_summary,2016-09-02,2016-09-02T00:00:00Z,24,opendrift,OpenDrift deterministic,single_forecast,single,paper,Prototype single,{str(single_path.relative_to(root)).replace(chr(92), '/')},{single_path},480,300,Legacy prototype support single with PyGNOME as comparator only.,output/CASE_2016-09-01/benchmark/control/control_footprint_mask_2016-09-02T00-00-00Z.tif,Legacy/debug transport comparator,true,comparator_only",
                    f"prototype_board,CASE_2016-09-01,prototype_pygnome_similarity_summary,2016-09-02_to_2016-09-04,2016-09-02T00:00:00Z;2016-09-03T00:00:00Z;2016-09-04T00:00:00Z,,opendrift_vs_pygnome,OpenDrift deterministic vs PyGNOME deterministic,comparison_board,board,slide,Prototype board,{str(board_path.relative_to(root)).replace(chr(92), '/')},{board_path},720,540,Legacy prototype support board with PyGNOME as comparator only.,output/CASE_2016-09-01/benchmark/qa/footprint_overlay_2016-09-02T00-00-00Z.png,Legacy/debug transport comparator,true,comparator_only",
                ]
            )
            + "\n",
        )

    def test_build_publication_figure_filename_uses_machine_readable_tokens(self):
        filename = build_publication_figure_filename(
            case_id="CASE_MINDORO_RETRO_2023",
            phase_or_track="phase3b_strict",
            model_name="opendrift_vs_pygnome",
            run_type="comparison_board",
            date_token="2023-03-04_to_2023-03-06",
            scenario_id="all_scenarios",
            view_type="close",
            variant="paper",
            figure_slug="obs_vs_model",
        )
        self.assertEqual(
            filename,
            "case_mindoro_retro_2023__phase3b_strict__opendrift_vs_pygnome__comparison_board__2023_03_04_to_2023_03_06__all_scenarios__close__paper__obs_vs_model.png",
        )

    def test_load_publication_style_config_requires_expected_keys(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            config_path = root / "config" / "publication_figure_style.yaml"
            _write_text(config_path, STYLE_YAML)
            payload = load_publication_style_config(config_path)
            self.assertIn("palette", payload)
            self.assertIn("legend_labels", payload)
            broken_path = root / "config" / "broken.yaml"
            _write_text(broken_path, "palette: {}\n")
            with self.assertRaises(ValueError):
                load_publication_style_config(broken_path)

    def test_format_fss_line_includes_all_windows_and_mean(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            service = self._build_service_fixture(Path(tmpdir))
            line = service._format_fss_line(
                {"fss_1km": 0.1, "fss_3km": 0.2, "fss_5km": 0.3, "fss_10km": 0.4},
                "Deterministic",
            )
            self.assertEqual(
                line,
                "Deterministic FSS(1/3/5/10 km): 0.100/0.200/0.300/0.400; mean: 0.250.",
            )

    def test_mindoro_publication_specs_use_exact_model_specific_fss_rows(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            service = self._build_service_fixture(Path(tmpdir))
            singles, boards = service._mindoro_publication_specs()
            specs = {spec["spec_id"]: spec for spec in [*singles, *boards]}

            self.assertIn(
                "OpenDrift R1 previous reinit p50 FSS(1/3/5/10 km): 0.000/0.044/0.137/0.249; mean: 0.108.",
                specs["mindoro_primary_board"]["note_lines"],
            )
            self.assertIn(
                "The separate focused 2016-2023 Mindoro drifter rerun found cmems_gfs as the historical four-recipe winner, and official B1 now uses cmems_gfs from that focused lane.",
                specs["mindoro_primary_board"]["note_lines"],
            )
            self.assertIn(
                "OpenDrift R1 previous reinit p50 FSS(1/3/5/10 km): 0.000/0.044/0.137/0.249; mean: 0.108.",
                specs["mindoro_crossmodel_board"]["note_lines"],
            )
            self.assertIn(
                "PyGNOME comparator FSS(1/3/5/10 km): 0.000/0.000/0.000/0.024; mean: 0.006.",
                specs["mindoro_crossmodel_board"]["note_lines"],
            )
            self.assertIn(
                "Legacy strict March 6 FSS(1/3/5/10 km): 0.111/0.222/0.333/0.444; mean: 0.278.",
                specs["mindoro_legacy_board"]["note_lines"],
            )
            self.assertIn("March 13 -> March 14", specs["mindoro_primary_board"]["figure_title"])
            self.assertIn(
                "Phase 3B Observation-Based Spatial Validation Using Public Mindoro Spill Extents",
                specs["mindoro_primary_seed_mask"]["subtitle"],
            )
            self.assertIn("sparse-reference", specs["mindoro_legacy_board"]["short_plain_language_interpretation"])

    def test_dwh_publication_specs_use_exact_model_specific_fss_rows(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            service = self._build_service_fixture(Path(tmpdir))
            singles, boards = service._dwh_publication_specs_v2()
            specs = {spec["spec_id"]: spec for spec in [*singles, *boards]}

            self.assertTrue(
                any(
                    "FSS(1/3/5/10 km): 0.211/0.322/0.433/0.544; mean: 0.378." in str(line)
                    for line in specs["dwh_2010_05_22_mask_p50_overlay"]["note_lines"]
                )
            )
            self.assertTrue(
                any(
                    "FSS(1/3/5/10 km): 0.170/0.280/0.390/0.500; mean: 0.335." in str(line)
                    for line in specs["dwh_2010_05_21_mask_p90_overlay"]["note_lines"]
                )
            )
            self.assertTrue(
                any(
                    "FSS(1/3/5/10 km): 0.190/0.290/0.390/0.490; mean: 0.340." in str(line)
                    for line in specs["dwh_2010_05_23_pygnome_footprint_overlay"]["note_lines"]
                )
            )

    def test_dwh_daily_overview_boards_use_stored_per_date_rows_only(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            service = self._build_service_fixture(Path(tmpdir))
            singles, boards = service._dwh_publication_specs_v2()
            specs = {spec["spec_id"]: spec for spec in [*singles, *boards]}

            required_board_ids = {
                "dwh_24h_48h_72h_mask_p50_footprint_overview_board",
                "dwh_24h_48h_72h_mask_p90_footprint_overview_board",
                "dwh_24h_48h_72h_mask_p50_mask_p90_dual_threshold_overview_board",
                "dwh_24h_48h_72h_mask_p50_vs_pygnome_overview_board",
                "dwh_24h_48h_72h_mask_p90_vs_pygnome_overview_board",
                "dwh_24h_48h_72h_mask_p50_mask_p90_dual_threshold_vs_pygnome_overview_board",
                "dwh_24h_48h_72h_mask_p50_mask_p90_vs_pygnome_three_row_overview_board",
            }
            self.assertTrue(required_board_ids.issubset(set(specs)))

            self.assertEqual(len(specs["dwh_24h_48h_72h_mask_p50_footprint_overview_board"]["panels"]), 3)
            self.assertEqual(len(specs["dwh_24h_48h_72h_mask_p50_vs_pygnome_overview_board"]["panels"]), 6)
            self.assertEqual(len(specs["dwh_24h_48h_72h_mask_p50_mask_p90_vs_pygnome_three_row_overview_board"]["panels"]), 9)

            p50_board_titles = [panel["panel_title"] for panel in specs["dwh_24h_48h_72h_mask_p50_footprint_overview_board"]["panels"]]
            self.assertIn("24 h | 2010-05-21 | mask_p50 mean FSS 0.316", p50_board_titles)
            self.assertIn("48 h | 2010-05-22 | mask_p50 mean FSS 0.378", p50_board_titles)
            self.assertIn("72 h | 2010-05-23 | mask_p50 mean FSS 0.338", p50_board_titles)

            dual_board_titles = [
                panel["panel_title"] for panel in specs["dwh_24h_48h_72h_mask_p50_mask_p90_dual_threshold_overview_board"]["panels"]
            ]
            self.assertIn("24 h | 2010-05-21 | p50 mean FSS 0.316 | p90 mean FSS 0.335", dual_board_titles)
            self.assertIn("48 h | 2010-05-22 | p50 mean FSS 0.378 | p90 mean FSS 0.346", dual_board_titles)
            self.assertIn("72 h | 2010-05-23 | p50 mean FSS 0.338 | p90 mean FSS 0.356", dual_board_titles)

            dual_vs_pygnome_titles = [
                panel["panel_title"]
                for panel in specs["dwh_24h_48h_72h_mask_p50_mask_p90_dual_threshold_vs_pygnome_overview_board"]["panels"]
            ]
            self.assertIn("24 h | 2010-05-21 | PyGNOME mean FSS 0.326", dual_vs_pygnome_titles)
            self.assertIn("48 h | 2010-05-22 | PyGNOME mean FSS 0.336", dual_vs_pygnome_titles)
            self.assertIn("72 h | 2010-05-23 | PyGNOME mean FSS 0.340", dual_vs_pygnome_titles)

            three_row_titles = [
                panel["panel_title"]
                for panel in specs["dwh_24h_48h_72h_mask_p50_mask_p90_vs_pygnome_three_row_overview_board"]["panels"]
            ]
            self.assertIn("24 h | 2010-05-21 | mask_p50 mean FSS 0.316", three_row_titles)
            self.assertIn("48 h | 2010-05-22 | mask_p90 mean FSS 0.346", three_row_titles)
            self.assertIn("72 h | 2010-05-23 | PyGNOME mean FSS 0.340", three_row_titles)

            self.assertIn(
                "Official public observation-derived DWH date-composite masks remain the scoring reference for every panel.",
                specs["dwh_24h_48h_72h_mask_p50_footprint_overview_board"]["note_lines"],
            )
            self.assertIn(
                "No combined dual-threshold FSS is reported; each date keeps the stored `mask_p50` and `mask_p90` rows separate.",
                specs["dwh_24h_48h_72h_mask_p50_mask_p90_dual_threshold_overview_board"]["guide_bullets"],
            )
            self.assertIn(
                "`mask_p50` remains the preferred probabilistic extension, `mask_p90` remains support/comparison only, and PyGNOME remains comparator-only.",
                specs["dwh_24h_48h_72h_mask_p50_mask_p90_vs_pygnome_three_row_overview_board"]["guide_bullets"],
            )

    def test_study_box_publication_spec_uses_stored_repo_box_metadata(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            service = self._build_service_fixture(Path(tmpdir))
            singles, boards = service._study_box_publication_specs()
            self.assertEqual(boards, [])
            self.assertEqual(len(singles), 5)
            spec = _find_spec(singles, "thesis_study_boxes_reference")
            focused_spec = _find_spec(singles, "focused_phase1_box_geography_reference")
            prototype_spec = _find_spec(singles, "prototype_first_code_search_box_geography_reference")

            self.assertEqual(spec["figure_family_code"], "L")
            self.assertEqual(spec["status_key_override"], "thesis_study_box_reference")
            self.assertEqual(spec["case_id"], "THESIS_STUDY_CONTEXT")
            self.assertEqual(spec["renderer"], "study_boxes")
            self.assertEqual(len(spec["study_boxes"]), 4)
            self.assertIn(
                "1. Focused Mindoro Phase 1 validation box: [118.751, 124.305, 10.62, 16.026]",
                spec["subtitle_box_lines"],
            )
            self.assertIn(
                "2. `mindoro_case_domain` fallback transport/overview extent: [115, 122, 6, 14.5]",
                spec["subtitle_box_lines"],
            )
            self.assertIn(
                "4. prototype_2016 first-code search box: [108.6465, 121.3655, 6.1865, 20.3515]",
                spec["subtitle_box_lines"],
            )
            self.assertIn(
                "The prototype_2016 first-code search box is historical-origin support metadata only; the stored case-local prototype extents remain the operative scientific/display extents for the 2016 figures.",
                spec["note_lines"],
            )
            self.assertIn("config/phase1_baseline_selection.yaml", spec["source_paths"])
            self.assertIn(
                "output/2016 Legacy Runs FINAL Figures/manifests/prototype_2016_provenance_metadata.json",
                spec["source_paths"],
            )
            self.assertEqual(focused_spec["run_type"], "single_box_reference_map")
            self.assertEqual(focused_spec["model_names"], "study_box_geography")
            self.assertEqual(len(focused_spec["study_boxes"]), 1)
            self.assertEqual(focused_spec["figure_slug"], "focused_phase1_box_geography_reference")
            self.assertIn("Selected box: Focused Mindoro Phase 1 validation box", focused_spec["subtitle_box_lines"])
            self.assertIn(
                "The geographic backdrop is reference context so the Mindoro setting stays visible in a panel-ready figure.",
                focused_spec["note_lines"],
            )
            self.assertEqual(prototype_spec["figure_slug"], "prototype_first_code_search_box_geography_reference")
            self.assertIn(
                "Role: historical-origin west-coast Philippines search box used by the earliest prototype code",
                prototype_spec["subtitle_box_lines"],
            )

    def test_case_context_normalizes_mindoro_manifest_extent_order(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            service = self._build_service_fixture(Path(tmpdir))
            context = service._case_context("CASE_MINDORO_RETRO_2023")
            self.assertEqual(context["full_bounds"], (274000.0, 1355000.0, 398000.0, 1524000.0))

    def test_board_note_lines_include_scored_panels_only(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            service = self._build_service_fixture(Path(tmpdir))
            mindoro_singles, mindoro_boards = service._mindoro_publication_specs()
            dwh_singles, dwh_boards = service._dwh_publication_specs_v2()

            mindoro_primary_board = _find_spec(mindoro_boards, "mindoro_primary_board")
            mindoro_primary_score_lines = [line for line in mindoro_primary_board["note_lines"] if "FSS(" in line]
            self.assertEqual(len(mindoro_primary_score_lines), 2)
            self.assertEqual(
                mindoro_primary_score_lines,
                [
                    "OpenDrift R1 previous reinit p50 FSS(1/3/5/10 km): 0.000/0.044/0.137/0.249; mean: 0.108.",
                    "OpenDrift R0 reinit p50 FSS(1/3/5/10 km): 0.000/0.000/0.000/0.000; mean: 0.000.",
                ],
            )

            mindoro_crossmodel_board = _find_spec(mindoro_boards, "mindoro_crossmodel_board")
            mindoro_crossmodel_score_lines = [line for line in mindoro_crossmodel_board["note_lines"] if "FSS(" in line]
            self.assertEqual(len(mindoro_crossmodel_score_lines), 3)

            dwh_trajectory_board = _find_spec(dwh_boards, "dwh_trajectory_board")
            self.assertFalse(any("FSS(" in line for line in dwh_trajectory_board["note_lines"]))

            mindoro_primary_seed_mask = _find_spec(mindoro_singles, "mindoro_primary_seed_mask")
            self.assertFalse(any("FSS(" in line for line in mindoro_primary_seed_mask["note_lines"]))

            dwh_event_observation = _find_spec(dwh_singles, "dwh_2010_05_21_to_2010_05_23_observation_truth_context")
            self.assertFalse(any("FSS(" in line for line in dwh_event_observation["note_lines"]))

            dwh_mask_p50_board = _find_spec(dwh_boards, "dwh_24h_48h_72h_mask_p50_footprint_overview_board")
            self.assertEqual(len([line for line in dwh_mask_p50_board["note_lines"] if "FSS(" in line]), 3)

            dwh_dual_threshold_board = _find_spec(dwh_boards, "dwh_24h_48h_72h_mask_p50_mask_p90_dual_threshold_overview_board")
            self.assertEqual(len([line for line in dwh_dual_threshold_board["note_lines"] if "FSS(" in line]), 6)
            self.assertFalse(any("combined dual-threshold FSS" in line and "FSS(" in line for line in dwh_dual_threshold_board["note_lines"]))

            dwh_three_row_board = _find_spec(dwh_boards, "dwh_24h_48h_72h_mask_p50_mask_p90_vs_pygnome_three_row_overview_board")
            self.assertEqual(len([line for line in dwh_three_row_board["note_lines"] if "FSS(" in line]), 9)

    def test_apply_map_axes_style_uses_equal_aspect_for_projected_maps(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            service = self._build_service_fixture(Path(tmpdir))
            fig, ax = plt.subplots()
            try:
                service._apply_map_axes_style(ax, "EPSG:32651", (274000.0, 1355000.0, 398000.0, 1524000.0), "CASE_MINDORO_RETRO_2023")
                self.assertEqual(float(ax.get_aspect()), 1.0)
            finally:
                plt.close(fig)

    def test_add_locator_uses_latitude_based_aspect(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            service = self._build_service_fixture(Path(tmpdir))
            fig, ax = plt.subplots()
            try:
                service._add_locator(ax, "CASE_MINDORO_RETRO_2023", None, "EPSG:32651")
                bounds = service._case_wgs84_bounds("CASE_MINDORO_RETRO_2023")
                expected = service._geographic_aspect((bounds[2] + bounds[3]) / 2.0)
                self.assertAlmostEqual(float(ax.get_aspect()), expected, places=6)
            finally:
                plt.close(fig)

    def test_service_writes_registry_manifest_and_records_missing_optional_inputs(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            service = self._build_service_fixture(root)
            results = service.run()

            self.assertTrue(Path(results["registry_csv"]).exists())
            self.assertTrue(Path(results["manifest_json"]).exists())
            self.assertTrue(Path(results["captions_md"]).exists())
            self.assertTrue(Path(results["talking_points_md"]).exists())
            self.assertTrue(Path(results["font_audit_csv"]).exists())
            self.assertTrue(Path(results["board_layout_audit_csv"]).exists())
            self.assertGreater(results["figure_count"], 0)
            self.assertTrue(results["side_by_side_comparison_boards_produced"])
            self.assertTrue(results["single_image_paper_figures_produced"])
            self.assertTrue(results["missing_optional_artifacts"])

            manifest = json.loads(Path(results["manifest_json"]).read_text(encoding="utf-8"))
            self.assertTrue(manifest["publication_package_built_from_existing_outputs_only"])
            self.assertIn("A", manifest["figure_families_generated"])
            self.assertIn("F", manifest["figure_families_generated"])
            self.assertIn("L", manifest["figure_families_generated"])
            self.assertIn("recommended_main_defense_figures", manifest)
            self.assertTrue(manifest["phase4_deferred_comparison_note_figure_produced"])
            self.assertIn("font_audit", manifest)
            self.assertEqual(manifest["font_audit"]["requested_font_family"], "Arial")
            self.assertGreater(manifest["board_layout_audit_row_count"], 0)
            self.assertGreaterEqual(int(manifest["figure_families_generated"]["H"]), 18)
            self.assertGreaterEqual(int(manifest["figure_families_generated"]["I"]), 13)
            registry_df = pd.read_csv(results["registry_csv"])
            self.assertIn("pixel_width", registry_df.columns)
            self.assertIn("pixel_height", registry_df.columns)
            self.assertIn("status_key", registry_df.columns)
            self.assertIn("status_provenance", registry_df.columns)
            self.assertEqual(len(registry_df), len(manifest["figures"]))
            thesis_boxes_row = registry_df[
                (registry_df["case_id"] == "THESIS_STUDY_CONTEXT")
                & registry_df["figure_id"].str.contains("thesis_study_boxes_reference", na=False)
            ].iloc[0]
            thesis_detail_rows = registry_df[
                (registry_df["case_id"] == "THESIS_STUDY_CONTEXT")
                & registry_df["run_type"].astype(str).eq("single_box_reference_map")
            ].copy()
            self.assertEqual(thesis_boxes_row["status_key"], "thesis_study_box_reference")
            self.assertIn("stored Phase 1 baseline/config metadata", thesis_boxes_row["notes"])
            self.assertEqual(thesis_boxes_row["figure_family_code"], "L")
            self.assertEqual(len(thesis_detail_rows), 4)
            self.assertTrue(thesis_detail_rows["figure_id"].astype(str).str.contains("focused_phase1_box_geography_reference", na=False).any())
            self.assertTrue(thesis_detail_rows["figure_id"].astype(str).str.contains("prototype_first_code_search_box_geography_reference", na=False).any())

            font_audit_df = pd.read_csv(results["font_audit_csv"])
            self.assertEqual(font_audit_df.iloc[0]["requested_font_family"], "Arial")
            self.assertTrue(str(font_audit_df.iloc[0]["actual_font_family"]).strip())
            layout_audit_df = pd.read_csv(results["board_layout_audit_csv"])
            self.assertGreaterEqual(len(layout_audit_df), 3)
            self.assertTrue((layout_audit_df["filenames_stayed_same"] == True).all())
            self.assertTrue((layout_audit_df["title_within_bounds"] == True).all())
            self.assertTrue((layout_audit_df["subtitle_within_bounds"] == True).all())
            self.assertTrue((layout_audit_df["guide_within_bounds"] == True).all())
            self.assertTrue(
                layout_audit_df["board_family"].astype(str).str.contains("Mindoro|DWH", regex=True).any()
            )

            for figure_slug, expected_status in (
                ("24h_48h_72h_mask_p50_footprint_overview_board", "dwh_ensemble_transfer"),
                ("24h_48h_72h_mask_p90_footprint_overview_board", "dwh_ensemble_transfer"),
                ("24h_48h_72h_mask_p50_mask_p90_dual_threshold_overview_board", "dwh_ensemble_transfer"),
                ("24h_48h_72h_mask_p50_vs_pygnome_overview_board", "dwh_crossmodel_comparator"),
                ("24h_48h_72h_mask_p90_vs_pygnome_overview_board", "dwh_crossmodel_comparator"),
                ("24h_48h_72h_mask_p50_mask_p90_dual_threshold_vs_pygnome_overview_board", "dwh_crossmodel_comparator"),
                ("24h_48h_72h_mask_p50_mask_p90_vs_pygnome_three_row_overview_board", "dwh_crossmodel_comparator"),
            ):
                board_row = registry_df[
                    (registry_df["case_id"] == "CASE_DWH_RETRO_2010_72H")
                    & registry_df["figure_id"].str.contains(figure_slug, na=False)
                ].iloc[0]
                self.assertEqual(board_row["status_key"], expected_status)

            dwh_trajectory_board = registry_df[
                (registry_df["case_id"] == "CASE_DWH_RETRO_2010_72H")
                & registry_df["figure_id"].str.contains("trajectory_board", na=False)
            ].iloc[0]
            self.assertEqual(dwh_trajectory_board["status_key"], "dwh_trajectory_context")
            dwh_event_obs = registry_df[
                (registry_df["case_id"] == "CASE_DWH_RETRO_2010_72H")
                & registry_df["figure_id"].str.contains("eventcorridor_observation", na=False)
            ].iloc[0]
            self.assertEqual(dwh_event_obs["status_key"], "dwh_observation_truth_context")

            expected_single_dims = (
                int(service._single_size()[0] * service._dpi()),
                int(service._single_size()[1] * service._dpi()),
            )
            expected_board_dims = (
                int(service._board_size()[0] * service._dpi()),
                int(service._board_size()[1] * service._dpi()),
            )
            manifest_by_figure_id = {item["figure_id"]: item for item in manifest["figures"]}
            for row in registry_df.to_dict(orient="records"):
                figure_path = root / str(row["relative_path"])
                self.assertTrue(figure_path.exists(), figure_path)
                actual_dims = _image_dimensions(figure_path)
                if str(row["view_type"]) == "board":
                    self.assertEqual(actual_dims, expected_board_dims)
                else:
                    self.assertGreater(actual_dims[0], 0)
                    self.assertGreater(actual_dims[1], 0)
                self.assertEqual((int(row["pixel_width"]), int(row["pixel_height"])), actual_dims)
                manifest_entry = manifest_by_figure_id[str(row["figure_id"])]
                self.assertEqual((int(manifest_entry["pixel_width"]), int(manifest_entry["pixel_height"])), actual_dims)

            registry_text = Path(results["registry_csv"]).read_text(encoding="utf-8")
            self.assertIn("crossmodel_comparison_deferred", registry_text)
            self.assertNotIn("when the panel", registry_text.lower())
            captions_text = Path(results["captions_md"]).read_text(encoding="utf-8")
            self.assertIn("Provenance:", captions_text)
            talking_points_text = Path(results["talking_points_md"]).read_text(encoding="utf-8")
            self.assertNotIn("when the panel", talking_points_text.lower())

    def test_publication_package_surfaces_prototype_support_family_k_when_registry_exists(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            self._write_prototype_support_fixture(root)
            service = self._build_service_fixture(root)
            results = service.run()

            manifest = json.loads(Path(results["manifest_json"]).read_text(encoding="utf-8"))
            self.assertIn("K", manifest["figure_families_generated"])
            self.assertGreaterEqual(manifest["figure_families_generated"]["K"], 2)

            registry_df = pd.read_csv(results["registry_csv"])
            family_k = registry_df[registry_df["figure_family_code"] == "K"].copy()
            self.assertEqual(len(family_k), 2)
            self.assertTrue((family_k["recommended_for_main_defense"] == False).all())
            singles = family_k[family_k["view_type"] == "single"]
            boards = family_k[family_k["view_type"] == "board"]
            self.assertEqual(len(singles), 1)
            self.assertEqual(len(boards), 1)
            self.assertTrue((singles["recommended_for_paper"] == True).all())
            self.assertTrue((boards["recommended_for_paper"] == False).all())
            self.assertTrue((family_k["status_key"] == "prototype_2016_support").all())
            self.assertTrue((family_k["notes"].str.contains("Support-only publication copy")).all())

            talking_points = Path(results["talking_points_md"]).read_text(encoding="utf-8")
            self.assertIn("Prototype Support Figures", talking_points)
            self.assertIn("Legacy debug support only;", talking_points)
            self.assertIn("Not final Phase 1 evidence.", talking_points)


if __name__ == "__main__":
    unittest.main()
