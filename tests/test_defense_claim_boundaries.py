from __future__ import annotations

from scripts.defense_readiness_common import documentation_findings, launcher_entry_map, read_text, DOC_PATHS


def test_defense_docs_have_no_claim_boundary_regressions():
    assert documentation_findings() == []


def test_launcher_claim_boundaries_keep_main_thesis_separation():
    entries = launcher_entry_map()
    b1_boundary = entries["mindoro_phase3b_primary_public_validation"]["claim_boundary"].lower()
    drifter_boundary = entries["b1_drifter_context_panel"]["claim_boundary"].lower()
    dwh_boundary = entries["dwh_reportable_bundle"]["claim_boundary"].lower()
    phase1_boundary = entries["phase1_mindoro_focus_provenance"]["claim_boundary"].lower()

    assert "only main philippine public-observation validation claim" in b1_boundary
    assert "independent noaa-published day-specific observation products" in b1_boundary
    assert "neighborhood-scale" in b1_boundary
    assert "not the direct" in drifter_boundary
    assert "external transfer validation" in dwh_boundary
    assert "not mindoro recalibration" in dwh_boundary
    assert "transport-provenance" in phase1_boundary


def test_docs_repeat_panel_safe_default_and_researcher_audit_split():
    combined = "\n".join(read_text(path) for path in DOC_PATHS).lower()
    assert "panel mode is the defense-safe default" in combined
    assert "full launcher is the researcher" in combined
    assert "raw phase names are not the primary startup commands" in combined or "raw phase names are secondary" in combined


def test_docs_keep_pygnome_and_drifter_language_honest():
    combined = "\n".join(read_text(path) for path in DOC_PATHS).lower()
    assert "pygnome is comparator-only" in combined or "pygnome remains comparator-only" in combined
    assert "drifter provenance" in combined
    assert "not the direct" in combined


def test_command_matrix_keeps_panel_b1_context_option_in_sync():
    command_matrix = read_text(next(path for path in DOC_PATHS if path.name == "COMMAND_MATRIX.md"))
    lowered = command_matrix.lower()
    assert "panel option `7`" in lowered
    assert "b1_drifter_context_panel" in command_matrix
