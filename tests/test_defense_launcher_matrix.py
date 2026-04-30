from __future__ import annotations

from scripts.defense_readiness_common import launcher_categories, launcher_entry_map, launcher_matrix_payload, validate_launcher_matrix


def test_launcher_matrix_has_categories_and_entries():
    payload = launcher_matrix_payload()
    assert payload["categories"]
    assert payload["entries"]
    assert launcher_categories()
    assert launcher_entry_map()


def test_launcher_matrix_matches_defense_schema_and_boundaries():
    issues = validate_launcher_matrix()
    assert issues == []


def test_b1_drifter_context_entry_stays_panel_safe():
    entry = launcher_entry_map()["b1_drifter_context_panel"]
    assert entry["safe_default"] is True
    assert entry["rerun_cost"] == "cheap_read_only"
    assert entry["run_kind"] == "read_only"
    assert entry["thesis_role"] == "read_only_governance"
    assert entry["recommended_for"] == "panel"
    assert entry["category_id"] == "read_only_packaging_help_utilities"
    assert "not the direct" in entry["claim_boundary"].lower()


def test_phase1_aliases_remain_backward_compatible():
    entries = launcher_entry_map()
    assert entries["phase1_mindoro_focus_pre_spill_experiment"]["alias_of"] == "phase1_mindoro_focus_provenance"
    assert entries["phase1_production_rerun"]["alias_of"] == "phase1_regional_reference_rerun"
    assert entries["mindoro_march13_14_noaa_reinit_stress_test"]["alias_of"] == "mindoro_phase3b_primary_public_validation"


def test_primary_claim_boundaries_keep_required_language():
    entries = launcher_entry_map()
    b1_boundary = entries["mindoro_phase3b_primary_public_validation"]["claim_boundary"].lower()
    phase1_boundary = entries["phase1_mindoro_focus_provenance"]["claim_boundary"].lower()
    dwh_boundary = entries["dwh_reportable_bundle"]["claim_boundary"].lower()

    assert "only main philippine public-observation validation claim" in b1_boundary
    assert "independent noaa-published day-specific observation products" in b1_boundary
    assert "neighborhood-scale" in b1_boundary

    assert "transport-provenance" in phase1_boundary
    assert "not direct spill-footprint validation" in phase1_boundary

    assert "external transfer validation" in dwh_boundary
    assert "not mindoro recalibration" in dwh_boundary
    assert "comparator-only" in dwh_boundary


def test_packaging_entries_stay_read_only_or_packaging_only():
    entries = launcher_entry_map()
    for entry_id in (
        "figure_package_publication",
        "final_validation_package",
        "phase5_sync",
        "trajectory_gallery_panel",
        "b1_drifter_context_panel",
    ):
        assert entries[entry_id]["run_kind"] in {"read_only", "packaging_only"}
