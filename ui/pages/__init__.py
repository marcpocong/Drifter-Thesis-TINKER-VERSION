"""Page registry for the read-only local dashboard."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from ui.pages import (
    artifacts_logs,
    cross_model_comparison,
    dwh_transfer_validation,
    home,
    legacy_2016_support,
    mindoro_validation_archive,
    mindoro_validation,
    phase1_recipe_selection,
    phase4_crossmodel_status,
    phase4_oiltype_and_shoreline,
    trajectory_explorer,
)


@dataclass(frozen=True)
class PageDefinition:
    page_id: str
    label: str
    renderer: Callable[[dict, dict], None]
    advanced_only: bool = False
    navigation_section: str = "Study"
    url_path: str = ""


PAGE_DEFINITIONS = [
    PageDefinition("home", "Defense / Panel Review", home.render, navigation_section="Study", url_path="home"),
    PageDefinition("phase1_recipe_selection", "Phase 1 Recipe Selection", phase1_recipe_selection.render, navigation_section="Study", url_path="phase1-recipe-selection"),
    PageDefinition("mindoro_validation", "Mindoro B1 Primary Validation", mindoro_validation.render, navigation_section="Study", url_path="mindoro-b1-primary-validation"),
    PageDefinition("cross_model_comparison", "Mindoro Cross-Model Comparator", cross_model_comparison.render, navigation_section="Study", url_path="mindoro-cross-model-comparator"),
    PageDefinition("mindoro_validation_archive", "Mindoro Validation Archive", mindoro_validation_archive.render, navigation_section="Archive", url_path="mindoro-validation-archive"),
    PageDefinition("dwh_transfer_validation", "DWH Phase 3C Transfer Validation", dwh_transfer_validation.render, navigation_section="Study", url_path="dwh-phase3c-transfer-validation"),
    PageDefinition("phase4_oiltype_and_shoreline", "Phase 4 Oil-Type and Shoreline Context", phase4_oiltype_and_shoreline.render, navigation_section="Study", url_path="phase4-oiltype-and-shoreline-context"),
    PageDefinition("legacy_2016_support", "Legacy 2016 Support Package", legacy_2016_support.render, navigation_section="Study", url_path="legacy-2016-support-package"),
    PageDefinition("artifacts_logs", "Artifacts / Logs / Registries", artifacts_logs.render, navigation_section="Reference", url_path="artifacts-logs-registries"),
    PageDefinition("phase4_crossmodel_status", "Phase 4 Cross-Model Status", phase4_crossmodel_status.render, advanced_only=True, navigation_section="Advanced", url_path="phase4-crossmodel-status"),
    PageDefinition("trajectory_explorer", "Trajectory Explorer", trajectory_explorer.render, advanced_only=True, navigation_section="Advanced", url_path="trajectory-explorer"),
]


PAGE_BY_ID = {page.page_id: page for page in PAGE_DEFINITIONS}


def visible_page_definitions(state: dict, *, advanced: bool) -> list[PageDefinition]:
    del state
    return [page for page in PAGE_DEFINITIONS if advanced or not page.advanced_only]
