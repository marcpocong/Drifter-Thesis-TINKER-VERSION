import json
from pathlib import Path

import pytest

pytest.importorskip("opendrift")

from src.services.mindoro_march3_4_philsa_5000_experiment import (
    EXPERIMENT_LAUNCHER_ENTRY_ID,
    EXPERIMENT_OUTPUT_DIR_NAME,
    EXPERIMENT_REQUESTED_ELEMENT_COUNT,
    PHILSA_LAYER_SPECS,
    PROHIBITED_SOURCE_NAMES,
    SEED_OBS_DATE,
    TARGET_OBS_DATE,
    resolve_march3_4_philsa_window,
)


def test_window_uses_march3_to_march4_local_dates_with_three_hour_guardrail():
    window = resolve_march3_4_philsa_window()

    assert window.forecast_local_dates == [SEED_OBS_DATE, TARGET_OBS_DATE]
    assert window.seed_obs_date == SEED_OBS_DATE
    assert window.scored_target_date == TARGET_OBS_DATE
    assert window.simulation_start_utc == "2023-03-02T16:00:00Z"
    assert window.simulation_end_utc == "2023-03-04T15:59:00Z"
    assert window.required_forcing_start_utc == "2023-03-02T13:00:00Z"
    assert window.required_forcing_end_utc == "2023-03-04T18:59:00Z"


def test_layer_specs_are_philsa_only_and_exclude_disallowed_sources():
    assert [(row.observation_date, row.provider, row.source_name) for row in PHILSA_LAYER_SPECS] == [
        ("2023-03-03", "PhilSA", "MindoroOilSpill_Philsa_230303"),
        ("2023-03-04", "PhilSA", "MindoroOilSpill_Philsa_230304"),
    ]
    selected_names = {row.source_name for row in PHILSA_LAYER_SPECS}
    assert not selected_names.intersection(PROHIBITED_SOURCE_NAMES)


def test_launcher_entry_is_archive_only_and_5000_specific():
    matrix = json.loads(Path("config/launcher_matrix.json").read_text(encoding="utf-8"))
    entry = next(row for row in matrix["entries"] if row["entry_id"] == EXPERIMENT_LAUNCHER_ENTRY_ID)

    assert entry["workflow_mode"] == "mindoro_retro_2023"
    assert entry["thesis_role"] == "archive_provenance"
    assert entry["safe_default"] is False
    assert entry["confirms_before_run"] is True
    assert entry["reportable"] is False
    assert entry["thesis_facing"] is False
    assert entry["experimental_only"] is True
    assert entry["steps"] == [
        {
            "phase": "mindoro_march3_4_philsa_5000_experiment",
            "service": "pipeline",
            "description": "Run the separate PhilSA March 3 -> March 4 5,000-element experimental archive/provenance validation test",
        }
    ]
    assert str(EXPERIMENT_REQUESTED_ELEMENT_COUNT) in entry["notes"]
    assert EXPERIMENT_OUTPUT_DIR_NAME in entry["notes"]
