"""Page registry for the read-only local dashboard."""

from __future__ import annotations

from dataclasses import dataclass
from importlib import import_module
from typing import Callable


@dataclass(frozen=True)
class PageDefinition:
    page_id: str
    label: str
    renderer: Callable[[dict, dict], None]
    advanced_only: bool = False
    navigation_section: str = "Study"
    url_path: str = ""


def _lazy_renderer(module_name: str) -> Callable[[dict, dict], None]:
    def _render(state: dict, ui_state: dict) -> None:
        module = import_module(f"ui.pages.{module_name}")
        module.render(state, ui_state)

    return _render


PAGE_DEFINITIONS = [
    PageDefinition("home", "Overview / Final Manuscript Alignment", _lazy_renderer("home"), navigation_section="Final Paper Evidence", url_path="home"),
    PageDefinition("data_sources", "Data Sources & Provenance", _lazy_renderer("data_sources"), navigation_section="Final Paper Evidence", url_path="data-sources-provenance"),
    PageDefinition("phase1_recipe_selection", "Focused Mindoro Phase 1 Provenance", _lazy_renderer("phase1_recipe_selection"), navigation_section="Final Paper Evidence", url_path="focused-mindoro-phase1-provenance"),
    PageDefinition("mindoro_validation", "Primary Mindoro March 13-14 Validation Case (B1)", _lazy_renderer("mindoro_validation"), navigation_section="Final Paper Evidence", url_path="mindoro-b1-public-observation-validation"),
    PageDefinition("cross_model_comparison", "Mindoro Same-Case OpenDrift-PyGNOME Comparator (Track A)", _lazy_renderer("cross_model_comparison"), navigation_section="Final Paper Evidence", url_path="mindoro-track-a-comparator-support"),
    PageDefinition("dwh_transfer_validation", "DWH External Transfer Validation", _lazy_renderer("dwh_transfer_validation"), navigation_section="Final Paper Evidence", url_path="dwh-external-transfer-validation"),
    PageDefinition("phase4_oiltype_and_shoreline", "Mindoro Oil-Type and Shoreline Support/Context", _lazy_renderer("phase4_oiltype_and_shoreline"), navigation_section="Final Paper Evidence", url_path="mindoro-oiltype-shoreline-support-context"),
    PageDefinition("legacy_2016_support", "Secondary 2016 Support", _lazy_renderer("legacy_2016_support"), navigation_section="Final Paper Evidence", url_path="secondary-2016-support"),
    PageDefinition("mindoro_validation_archive", "Archive/Provenance and Legacy Support", _lazy_renderer("mindoro_validation_archive"), navigation_section="Archive / Provenance", url_path="archive-provenance-legacy-support"),
    PageDefinition("artifacts_logs", "Reproducibility / Governance / Audit", _lazy_renderer("artifacts_logs"), navigation_section="Governance", url_path="reproducibility-governance-audit"),
    PageDefinition("b1_drifter_context", "B1 Recipe Provenance - Not Truth Mask", _lazy_renderer("b1_drifter_context"), advanced_only=True, navigation_section="Advanced / Debug", url_path="b1-recipe-provenance-not-truth-mask"),
    PageDefinition("phase4_crossmodel_status", "Oil-Type/Shoreline Comparator Availability", _lazy_renderer("phase4_crossmodel_status"), advanced_only=True, navigation_section="Advanced / Debug", url_path="phase4-crossmodel-status"),
    PageDefinition("trajectory_explorer", "Trajectory Explorer", _lazy_renderer("trajectory_explorer"), advanced_only=True, navigation_section="Advanced / Debug", url_path="trajectory-explorer"),
]

PAGE_BY_ID = {page.page_id: page for page in PAGE_DEFINITIONS}


def visible_page_definitions(state: dict, *, advanced: bool) -> list[PageDefinition]:
    del state
    return [page for page in PAGE_DEFINITIONS if advanced or not page.advanced_only]
