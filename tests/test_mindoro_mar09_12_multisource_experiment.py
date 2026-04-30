import json
from pathlib import Path
from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

from src.services.mindoro_mar09_12_multisource_experiment import (
    ALLOW_MINIMAL_CASE_FORCING_FETCH_ENV,
    CONFIG_PATH,
    ELEMENT_CAP,
    EXPERIMENT_ID,
    EXPERIMENT_OUTPUT_DIR,
    FORCING_BBOX,
    FORCING_WINDOW_END_UTC,
    FORCING_WINDOW_START_UTC,
    FORECAST_PAIRS,
    MindoroMar0912MultisourceExperimentService,
    MARCH12_SOURCE_MASK_IDS,
    PROTECTED_OUTPUT_ROOTS,
    RUN_EXPERIMENT_ENV,
    SCORECARD_COLUMNS,
    assert_march12_union_area,
    assert_scorecard_contract,
    combine_binary_masks,
    snapshot_diff,
    _filter_positive_oil_polygons,
)


def test_positive_oil_filter_keeps_possible_classes_and_excludes_source_points():
    gpd = pytest.importorskip("geopandas")
    from shapely.geometry import Point, Polygon

    gdf = gpd.GeoDataFrame(
        {
            "OilSpill_": [
                "Possible Oil",
                "Possible Thicker Oil",
                "Suspected Source",
                "Cloud shadow",
            ],
        },
        geometry=[
            Polygon([(0, 0), (1, 0), (1, 1), (0, 1)]),
            Polygon([(2, 0), (3, 0), (3, 1), (2, 1)]),
            Point(0.5, 0.5),
            Polygon([(4, 0), (5, 0), (5, 1), (4, 1)]),
        ],
        crs="EPSG:4326",
    )

    filtered, notes = _filter_positive_oil_polygons(gdf)

    assert list(filtered["OilSpill_"]) == ["Possible Oil", "Possible Thicker Oil"]
    assert all(geom.geom_type == "Polygon" for geom in filtered.geometry)
    assert any("suspected source" in note.lower() for note in notes)


def test_march12_union_mask_bounds_are_checked():
    source_a = np.array([[1, 0, 0], [0, 1, 0]], dtype=np.float32)
    source_b = np.array([[0, 1, 0], [0, 1, 1]], dtype=np.float32)
    source_c = np.array([[0, 0, 1], [0, 0, 1]], dtype=np.float32)

    combined = combine_binary_masks([source_a, source_b, source_c])

    individual_cells = [int(np.count_nonzero(mask > 0)) for mask in [source_a, source_b, source_c]]
    combined_cells = int(np.count_nonzero(combined > 0))
    assert combined_cells >= max(individual_cells)
    assert combined_cells <= sum(individual_cells)
    assert_march12_union_area(individual_cells, combined_cells)
    assert MARCH12_SOURCE_MASK_IDS == [
        "OBS_MAR12_WORLDVIEW3_NOAA_230314",
        "OBS_MAR12_WORLDVIEW3_NOAA_230313",
        "OBS_MAR12_ICEYE",
    ]


def test_common_grid_precheck_accepts_matching_rasters(tmp_path, monkeypatch):
    rasterio = pytest.importorskip("rasterio")
    from rasterio.transform import from_origin
    import src.helpers.scoring as scoring

    monkeypatch.setattr(scoring, "get_scoring_grid_spec", lambda: SimpleNamespace(sea_mask_path=None))
    profile = {
        "driver": "GTiff",
        "height": 3,
        "width": 4,
        "count": 1,
        "dtype": "float32",
        "crs": "EPSG:32651",
        "transform": from_origin(500000, 1400000, 1000, 1000),
    }
    forecast = tmp_path / "forecast.tif"
    target = tmp_path / "target.tif"
    for path in (forecast, target):
        with rasterio.open(path, "w", **profile) as dst:
            dst.write(np.zeros((3, 4), dtype=np.float32), 1)

    result = scoring.precheck_same_grid(forecast, target, report_base_path=tmp_path / "common_grid")

    assert result.passed
    assert result.checks["crs_match"]
    assert result.checks["transform_match"]
    assert result.checks["width_match"]
    assert result.checks["height_match"]


def test_scorecard_contract_requires_three_pairs_by_four_surfaces():
    rows = []
    surfaces = [
        "opendrift_deterministic",
        "opendrift_mask_p50",
        "opendrift_mask_p90",
        "pygnome_deterministic_comparator",
    ]
    for pair in FORECAST_PAIRS:
        for surface in surfaces:
            rows.append(
                {
                    "experiment_id": EXPERIMENT_ID,
                    "pair_id": pair.pair_id,
                    "seed_mask_id": pair.seed_mask_id,
                    "target_mask_id": pair.target_mask_id,
                    "nominal_lead_h": pair.nominal_lead_h,
                    "actual_elapsed_h": "",
                    "obs_time_offset_h": "",
                    "model_surface": surface,
                    "forcing_recipe_id": "cmems_gfs",
                    "forcing_recipe_source": "config/phase1_baseline_selection.yaml",
                    "phase1_rerun": False,
                    "element_cap": ELEMENT_CAP,
                    "fss_1km": 0.0,
                    "fss_3km": 0.0,
                    "fss_5km": 0.0,
                    "fss_10km": 0.0,
                    "mean_fss": 0.0,
                    "notes": "",
                }
            )
    scorecard = pd.DataFrame(rows, columns=SCORECARD_COLUMNS)

    assert_scorecard_contract(scorecard)


def test_launcher_entry_is_hidden_archive_only_and_not_canonical():
    matrix = json.loads(Path("config/launcher_matrix.json").read_text(encoding="utf-8"))
    entry = next(row for row in matrix["entries"] if row["entry_id"] == EXPERIMENT_ID)

    assert entry["workflow_mode"] == "mindoro_retro_2023"
    assert entry["category_id"] == "sensitivity_appendix_tracks"
    assert entry["thesis_role"] == "archive_provenance"
    assert entry["service_profile"] == "mixed_pipeline_and_gnome"
    assert entry["safe_default"] is False
    assert entry["experimental_only"] is True
    assert entry["reportable"] is False
    assert entry["thesis_facing"] is False
    assert entry["confirms_before_run"] is True
    assert entry["menu_hidden"] is True
    phases = [step["phase"] for step in entry["steps"]]
    assert phases == [
        "mindoro_mar09_12_multisource_experiment_resolve_forcing_only",
        "mindoro_mar09_12_multisource_experiment_ingest_masks_only",
        "mindoro_mar09_12_multisource_experiment",
        "mindoro_mar09_12_multisource_experiment_pygnome",
    ]
    assert "does not replace the current B1" in entry["notes"]


def test_service_dry_run_gate_has_no_phase1_drifter_or_broad_gfs_plan(monkeypatch):
    monkeypatch.delenv("ALLOW_MINIMAL_CASE_FORCING_FETCH", raising=False)
    monkeypatch.setenv("WORKFLOW_MODE", "mindoro_retro_2023")
    service = MindoroMar0912MultisourceExperimentService()

    plan = service.run_dry_run()

    assert plan["resolved_forcing_recipe_id"] == "cmems_gfs"
    assert plan["phase1_enabled"] is False
    assert plan["drifter_ingestion_enabled"] is False
    assert plan["gfs_historical_preflight_enabled"] is False
    assert plan["max_elements_per_run_or_member"] == 5000
    assert plan["planned_downloads"] == []
    planned_text = " ".join(plan["planned_execution_terms"]).lower()
    assert "drifter_6hour_qc" not in planned_text
    assert "accepted segment" not in planned_text
    assert "gfs preflight" not in planned_text
    assert "historical gfs ingestion" not in planned_text
    assert "monthly gfs" not in planned_text
    assert "recipe ranking" not in planned_text


def test_minimal_fetch_plan_is_flag_gated_and_bounded(monkeypatch):
    monkeypatch.setenv("WORKFLOW_MODE", "mindoro_retro_2023")

    def fake_missing(self, **kwargs):
        return {
            "forcing_kind": kwargs["forcing_kind"],
            "filename": kwargs["filename"],
            "status": "missing",
            "source_path": "",
            "stage_path": str(self.forcing_dir / kwargs["filename"]),
            "provider": self._provider_from_forcing_filename(kwargs["filename"]),
            "variable_group": kwargs["forcing_kind"],
            "inspection": {},
            "candidate_inspections": [],
            "local_stores_searched": [],
            "next_required_action": "provide local forcing",
        }

    monkeypatch.setattr(MindoroMar0912MultisourceExperimentService, "_resolve_one_local_forcing", fake_missing)
    service = MindoroMar0912MultisourceExperimentService()

    monkeypatch.delenv(ALLOW_MINIMAL_CASE_FORCING_FETCH_ENV, raising=False)
    blocked = service._resolve_forcing_reuse_plan("cmems_gfs")
    assert blocked["planned_downloads"] == []

    monkeypatch.setenv(ALLOW_MINIMAL_CASE_FORCING_FETCH_ENV, "1")
    planned = service._resolve_forcing_reuse_plan("cmems_gfs")
    assert planned["planned_downloads"]
    assert all(row["bounded_window_start_utc"] == FORCING_WINDOW_START_UTC for row in planned["planned_downloads"])
    assert all(row["bounded_window_end_utc"] == FORCING_WINDOW_END_UTC for row in planned["planned_downloads"])
    assert all(row["bounded_bbox"] == FORCING_BBOX for row in planned["planned_downloads"])
    assert all("monthly" not in row["mode"].lower() for row in planned["planned_downloads"])


def test_real_run_gate_stops_pipeline_before_models(monkeypatch):
    import src.services.mindoro_mar09_12_multisource_experiment as module

    monkeypatch.setenv("WORKFLOW_MODE", "mindoro_retro_2023")
    monkeypatch.delenv(RUN_EXPERIMENT_ENV, raising=False)
    monkeypatch.setattr(module, "snapshot_protected_outputs", lambda paths=None: {})
    monkeypatch.setattr(MindoroMar0912MultisourceExperimentService, "run_dry_run", lambda self: {"status": "ready_to_run"})

    def fail_if_called(self):
        raise AssertionError("prepare_observations should not be called while real-run gate is disabled")

    monkeypatch.setattr(MindoroMar0912MultisourceExperimentService, "prepare_observations", fail_if_called)
    result = MindoroMar0912MultisourceExperimentService().run_pipeline()

    assert result["status"] == "ready_to_run"
    assert result["model_run_executed"] is False


def test_mask_ingestion_only_phase_does_not_require_forcing(monkeypatch):
    import src.services.mindoro_mar09_12_multisource_experiment as module

    monkeypatch.setenv("WORKFLOW_MODE", "mindoro_retro_2023")
    monkeypatch.setattr(module, "snapshot_protected_outputs", lambda paths=None: {})
    monkeypatch.setattr(
        MindoroMar0912MultisourceExperimentService,
        "prepare_observations",
        lambda self: [{"mask_id": "OBS_MAR09_TERRAMODIS"}],
    )
    monkeypatch.setattr(MindoroMar0912MultisourceExperimentService, "write_figures", lambda self, **kwargs: None)

    result = MindoroMar0912MultisourceExperimentService().run_ingest_masks_only()

    assert result["status"] == "masks_ingested"
    assert result["model_run_executed"] is False


def test_config_and_docs_restate_experimental_boundary():
    assert (Path(CONFIG_PATH)).exists()
    config_text = Path(CONFIG_PATH).read_text(encoding="utf-8")

    assert "reportable: false" in config_text
    assert "thesis_facing: false" in config_text
    doc_path = Path("docs/EXPERIMENT_MINDORO_MAR09_12_MULTISOURCE.md")
    if not doc_path.exists():
        pytest.skip("docs/ is not mounted in the Docker test container")
    docs = doc_path.read_text(encoding="utf-8")
    assert "experimental/archive-only" in docs
    assert "does not replace the current B1" in docs


def test_protected_output_snapshot_diff_and_scope_do_not_include_experiment_root():
    before = {
        "output/final_validation_package": {"exists": True, "file_count": 3},
        "output/figure_package_publication": {"exists": True, "file_count": 2},
    }
    after = dict(before)

    assert snapshot_diff(before, after) == {"added": [], "removed": [], "changed": []}
    assert EXPERIMENT_OUTPUT_DIR not in PROTECTED_OUTPUT_ROOTS
