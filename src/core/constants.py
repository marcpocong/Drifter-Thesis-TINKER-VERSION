"""
Core constants for the application.
"""

import os
from pathlib import Path

from src.core.case_context import get_case_context, load_settings

_settings = load_settings()
CASE_CONTEXT = get_case_context()

WORKFLOW_MODE = CASE_CONTEXT.workflow_mode
CASE_ID = CASE_CONTEXT.case_id
CASE_IS_PROTOTYPE = CASE_CONTEXT.is_prototype
ACTIVE_DOMAIN_NAME = CASE_CONTEXT.active_domain_name
INITIALIZATION_MODE = CASE_CONTEXT.initialization_mode
SOURCE_POINT_ROLE = CASE_CONTEXT.source_point_role
SIMULATION_START_UTC = CASE_CONTEXT.simulation_start_utc
SIMULATION_END_UTC = CASE_CONTEXT.simulation_end_utc

# Backward-compatible aliases used throughout the repo.
PHASE_1_START_DATE = CASE_CONTEXT.phase_1_start_date_value
RUN_NAME = CASE_CONTEXT.run_name

BASE_OUTPUT_DIR = Path("output") / RUN_NAME
for sub in ["diagnostics", "ensemble", "gnome_comparison", "validation", "weathering"]:
    os.makedirs(BASE_OUTPUT_DIR / sub, exist_ok=True)

# Backward-compatible alias for the current workflow's active domain.
REGION = CASE_CONTEXT.region
PHASE1_VALIDATION_BOX = CASE_CONTEXT.phase1_validation_box
MINDORO_CASE_DOMAIN = CASE_CONTEXT.mindoro_case_domain
LEGACY_PROTOTYPE_DISPLAY_DOMAIN = CASE_CONTEXT.legacy_prototype_display_domain
GRID_RESOLUTION = _settings["grid_resolution"]
RECIPE_COMPONENT_NAMES = _settings["recipe_component_names"]

OIL_ADIOS_LIGHT = _settings["oil_adios_light"]
OIL_ADIOS_HEAVY = _settings["oil_adios_heavy"]

OIL_DISPLAY_NAMES = _settings["oil_display_names"]
OIL_COLORS = _settings["oil_colors"]
BUDGET_COLORS = _settings["budget_colors"]

NOAA_DEFAULTS = _settings["noaa_defaults"]

WIND_SPEED_REALISTIC_MAX_MS = _settings["wind_speed_realistic_max_ms"]
WAVE_HEIGHT_REALISTIC_MAX_M = _settings["wave_height_realistic_max_m"]
WIND_SPEED_MODERATE_MS = _settings["wind_speed_moderate_ms"]
