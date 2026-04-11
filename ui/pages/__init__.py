"""Page registry for the read-only local dashboard."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from ui.pages import (
    artifacts_logs,
    cross_model_comparison,
    dwh_transfer_validation,
    home,
    mindoro_validation,
    phase4_crossmodel_status,
    phase4_oiltype_and_shoreline,
    trajectory_explorer,
)


@dataclass(frozen=True)
class PageDefinition:
    page_id: str
    label: str
    renderer: Callable[[dict, dict], None]


PAGE_DEFINITIONS = [
    PageDefinition("home", "Home / Overview", home.render),
    PageDefinition("mindoro_validation", "Mindoro Validation", mindoro_validation.render),
    PageDefinition("dwh_transfer_validation", "DWH Transfer Validation", dwh_transfer_validation.render),
    PageDefinition("cross_model_comparison", "Cross-Model Comparison", cross_model_comparison.render),
    PageDefinition("phase4_oiltype_and_shoreline", "Phase 4 Oil-Type & Shoreline", phase4_oiltype_and_shoreline.render),
    PageDefinition("phase4_crossmodel_status", "Phase 4 Cross-Model Status", phase4_crossmodel_status.render),
    PageDefinition("trajectory_explorer", "Trajectory Explorer", trajectory_explorer.render),
    PageDefinition("artifacts_logs", "Artifacts / Logs", artifacts_logs.render),
]


PAGE_BY_ID = {page.page_id: page for page in PAGE_DEFINITIONS}
