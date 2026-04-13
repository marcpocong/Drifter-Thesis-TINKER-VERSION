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
    panel_visible: bool = True
    visible_when: Callable[[dict], bool] | None = None


def _has_phase1_artifacts(state: dict) -> bool:
    ranking = state.get("phase1_focused_recipe_ranking")
    return bool(state.get("phase1_focused_manifest")) or not bool(getattr(ranking, "empty", True))


def _has_dwh_artifacts(state: dict) -> bool:
    registry = state.get("dwh_final_registry")
    return not bool(getattr(registry, "empty", True))


def _has_legacy_artifacts(state: dict) -> bool:
    registry = state.get("legacy_2016_final_registry")
    return not bool(getattr(registry, "empty", True))


def _has_phase4_artifacts(state: dict) -> bool:
    budget = state.get("phase4_budget_summary")
    shoreline = state.get("phase4_shoreline_segments")
    return not bool(getattr(budget, "empty", True)) or not bool(getattr(shoreline, "empty", True))


PAGE_DEFINITIONS = [
    PageDefinition("home", "Home / Overview", home.render),
    PageDefinition("phase1_recipe_selection", "Phase 1 Recipe Selection", phase1_recipe_selection.render, visible_when=_has_phase1_artifacts),
    PageDefinition("mindoro_validation", "Mindoro B1 Primary Validation", mindoro_validation.render),
    PageDefinition("cross_model_comparison", "Mindoro Cross-Model Comparator", cross_model_comparison.render),
    PageDefinition("dwh_transfer_validation", "DWH Phase 3C Transfer Validation", dwh_transfer_validation.render, visible_when=_has_dwh_artifacts),
    PageDefinition("phase4_oiltype_and_shoreline", "Phase 4 Oil-Type and Shoreline Context", phase4_oiltype_and_shoreline.render, visible_when=_has_phase4_artifacts),
    PageDefinition("legacy_2016_support", "Legacy 2016 Support Package", legacy_2016_support.render, visible_when=_has_legacy_artifacts),
    PageDefinition("phase4_crossmodel_status", "Phase 4 Cross-Model Status", phase4_crossmodel_status.render, panel_visible=False),
    PageDefinition("trajectory_explorer", "Trajectory Explorer", trajectory_explorer.render, panel_visible=False),
    PageDefinition("artifacts_logs", "Artifacts / Logs / Registries", artifacts_logs.render),
]


PAGE_BY_ID = {page.page_id: page for page in PAGE_DEFINITIONS}


def visible_page_definitions(state: dict, *, advanced: bool) -> list[PageDefinition]:
    pages: list[PageDefinition] = []
    for page in PAGE_DEFINITIONS:
        if not advanced and not page.panel_visible:
            continue
        if page.visible_when is not None and not page.visible_when(state):
            continue
        pages.append(page)
    return pages
