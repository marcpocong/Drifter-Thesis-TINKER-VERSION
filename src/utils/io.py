"""
I/O utilities for loading drifter data and forcing configuration.
"""

from dataclasses import dataclass
import logging
import os
import json
from pathlib import Path, PurePosixPath
from typing import Any, Dict

import numpy as np
import pandas as pd
import xarray as xr
import yaml

from src.core.case_context import get_case_context

logger = logging.getLogger(__name__)

ALLOW_UNVALIDATED_FALLBACK_ENV = "ALLOW_UNVALIDATED_FALLBACK"
BASELINE_SELECTION_PATH_ENV = "BASELINE_SELECTION_PATH"
BASELINE_RECIPE_OVERRIDE_ENV = "BASELINE_RECIPE_OVERRIDE"
DEFAULT_VALIDATED_FALLBACK_RECIPE = "cmems_gfs"
DEFAULT_BASELINE_SELECTION_PATH = Path("config/phase1_baseline_selection.yaml")


@dataclass(frozen=True)
class RecipeSelection:
    recipe: str
    source_kind: str
    source_path: str | None
    status_flag: str
    valid: bool
    provisional: bool
    rerun_required: bool
    note: str = ""


def _load_settings(settings_path: str | Path = "config/settings.yaml") -> dict:
    """Load application settings from the canonical settings file."""
    with open(settings_path, "r") as f:
        return yaml.safe_load(f) or {}


def _load_recipes_config(config_path: str | Path = "config/recipes.yaml") -> dict:
    """Load the canonical recipe configuration file."""
    with open(config_path, "r") as f:
        return yaml.safe_load(f) or {}


def allow_unvalidated_fallback() -> bool:
    """Return whether downstream phases may continue without validated Phase 1 output."""
    return os.environ.get(ALLOW_UNVALIDATED_FALLBACK_ENV, "").strip() == "1"


def _default_baseline_selection_path() -> Path:
    """Return the configured baseline-selection artifact path."""
    settings = _load_settings()
    configured_path = settings.get("phase1_baseline_selection_path", str(DEFAULT_BASELINE_SELECTION_PATH))
    return Path(os.environ.get(BASELINE_SELECTION_PATH_ENV, configured_path))


def _infer_source_name(filename: str | None) -> str | None:
    """Infer a short source label from a configured forcing filename."""
    if not filename:
        return None

    stem = Path(filename).stem.lower()
    for suffix in ("_curr", "_wind", "_wave"):
        if stem.endswith(suffix):
            stem = stem[: -len(suffix)]
            break

    return stem.upper() if stem else None


def get_recipe_definition(recipe_name: str, config_path: str | Path = "config/recipes.yaml") -> dict:
    """Return the canonical recipe config entry for a recipe name."""
    config = _load_recipes_config(config_path)
    recipes = config.get("recipes") or {}
    if recipe_name not in recipes:
        raise ValueError(f"Recipe '{recipe_name}' not found in {config_path}")
    return dict(recipes[recipe_name])


def get_phase1_recipe_architecture(config_path: str | Path = "config/recipes.yaml") -> dict:
    """Return non-runtime metadata describing the intended Phase 1 recipe architecture."""
    config = _load_recipes_config(config_path)
    architecture = config.get("phase1_recipe_architecture") or {}
    return dict(architecture)


def get_official_phase1_recipe_family(config_path: str | Path = "config/recipes.yaml") -> list[str]:
    """Return the Chapter 3 target recipe family for Phase 1 finalization audits."""
    architecture = get_phase1_recipe_architecture(config_path)
    return [str(value) for value in architecture.get("official_recipe_family") or [] if str(value).strip()]


def get_phase1_legacy_recipe_aliases(config_path: str | Path = "config/recipes.yaml") -> dict[str, dict]:
    """Return legacy recipe-name metadata used to keep prototype naming drift explicit."""
    architecture = get_phase1_recipe_architecture(config_path)
    aliases = architecture.get("legacy_recipe_name_aliases") or {}
    return {str(key): dict(value or {}) for key, value in aliases.items()}


def get_runtime_recipe_ids(config_path: str | Path = "config/recipes.yaml") -> list[str]:
    """Return the recipe identifiers currently defined in the runtime config."""
    config = _load_recipes_config(config_path)
    return [str(key) for key in (config.get("recipes") or {}).keys()]


def get_prototype_debug_recipe_family(config_path: str | Path = "config/recipes.yaml") -> list[str]:
    """Return the explicit prototype debug/regression recipe family."""
    architecture = get_phase1_recipe_architecture(config_path)
    configured_family = [
        str(value).strip()
        for value in architecture.get("prototype_debug_recipe_family") or []
        if str(value).strip()
    ]
    fallback_family = [
        "cmems_ncep",
        "cmems_era5",
        "cmems_gfs",
        "hycom_ncep",
        "hycom_era5",
        "hycom_gfs",
    ]
    runtime_recipe_ids = set(get_runtime_recipe_ids(config_path))
    family = configured_family or fallback_family
    return [recipe_id for recipe_id in family if recipe_id in runtime_recipe_ids]


def get_transport_recipe_family_for_workflow(
    workflow_mode: str | None = None,
    config_path: str | Path = "config/recipes.yaml",
) -> list[str]:
    """Return the intended transport recipe family for the active workflow lane."""
    mode = str(workflow_mode or get_case_context().workflow_mode)
    if mode == "prototype_2021":
        return get_official_phase1_recipe_family(config_path)
    if mode == "prototype_2016":
        return get_prototype_debug_recipe_family(config_path)
    if mode.startswith("phase1_"):
        return get_official_phase1_recipe_family(config_path)
    return get_runtime_recipe_ids(config_path)


def get_phase1_baseline_audit_status(selection_path: str | Path | None = None) -> dict[str, Any]:
    """Return Phase 1 finalization-audit status metadata from the frozen baseline artifact."""
    try:
        _, selection = load_baseline_selection(selection_path)
    except FileNotFoundError:
        return {}

    chapter3_audit = selection.get("chapter3_finalization_audit") or {}
    return dict(chapter3_audit.get("audit_status") or {})


def get_phase2_recipe_family_status(
    *,
    run_name: str | None = None,
    selected_recipe: str | None = None,
    config_path: str | Path = "config/recipes.yaml",
    selection_path: str | Path | None = None,
) -> dict[str, Any]:
    """Summarize how much of the official Phase 1 recipe family is available to official Phase 2."""
    active_run_name = str(run_name or get_case_context().run_name)
    forcing_dir = Path("data") / "forcing" / active_run_name

    expected_family = sorted(set(get_official_phase1_recipe_family(config_path)))
    runtime_recipe_ids = sorted(set(get_runtime_recipe_ids(config_path)))
    runtime_defined_official = [recipe_id for recipe_id in expected_family if recipe_id in runtime_recipe_ids]
    legacy_aliases = get_phase1_legacy_recipe_aliases(config_path)
    legacy_recipe_ids_present = sorted(recipe_id for recipe_id in runtime_recipe_ids if recipe_id in legacy_aliases)
    gfs_wind_present = (forcing_dir / "gfs_wind.nc").exists()

    locally_available: list[str] = []
    unavailable_reasons: dict[str, list[str]] = {}
    for recipe_id in expected_family:
        reasons: list[str] = []
        if recipe_id not in runtime_recipe_ids:
            reasons.append("recipe_id_not_defined_in_runtime_config")
        else:
            recipe = get_recipe_definition(recipe_id, config_path)
            for forcing_kind, forcing_key in (
                ("currents", "currents_file"),
                ("wind", "wind_file"),
                ("wave", "wave_file"),
            ):
                forcing_file = str(recipe.get(forcing_key) or "").strip()
                if forcing_file and not (forcing_dir / forcing_file).exists():
                    reasons.append(f"missing_{forcing_kind}_file:{forcing_file}")
        if "gfs" in recipe_id and not gfs_wind_present:
            reasons.append("missing_gfs_wind_nc")
        if reasons:
            unavailable_reasons[recipe_id] = sorted(set(reasons))
        else:
            locally_available.append(recipe_id)

    selected_recipe = str(selected_recipe or "").strip()
    selected_recipe_alias = dict(legacy_aliases.get(selected_recipe) or {})
    baseline_audit_status = get_phase1_baseline_audit_status(selection_path)
    requires_phase1_rerun = bool(
        baseline_audit_status.get("full_production_rerun_required")
        or baseline_audit_status.get("requires_phase1_production_rerun_for_full_freeze")
    )
    legacy_drift_leaks = bool(
        selected_recipe_alias
        or legacy_recipe_ids_present
        or unavailable_reasons
    )

    return {
        "official_recipe_family_expected": expected_family,
        "official_recipe_family_runtime_defined": runtime_defined_official,
        "official_recipe_family_locally_available": locally_available,
        "official_recipe_family_unavailable": [recipe_id for recipe_id in expected_family if recipe_id not in locally_available],
        "official_recipe_family_unavailable_reasons": unavailable_reasons,
        "legacy_recipe_ids_present_in_runtime": legacy_recipe_ids_present,
        "legacy_recipe_id_detected": bool(legacy_recipe_ids_present or selected_recipe_alias),
        "selected_recipe_id_is_legacy_alias": bool(selected_recipe_alias),
        "selected_recipe_chapter3_target": str(selected_recipe_alias.get("chapter3_target_recipe") or ""),
        "gfs_wind_present_for_active_case": gfs_wind_present,
        "phase1_finalization_classification": str(baseline_audit_status.get("classification") or ""),
        "requires_phase1_production_rerun_for_full_freeze": requires_phase1_rerun,
        "legacy_recipe_drift_leaks_into_official_mode": legacy_drift_leaks,
    }


def extract_manifest_recipe(payload: dict[str, Any] | None) -> str:
    """Return the recorded recipe from a forecast-style manifest payload."""
    manifest = dict(payload or {})
    return str(
        manifest.get("recipe")
        or (manifest.get("selection") or {}).get("recipe")
        or (manifest.get("historical_baseline_provenance") or {}).get("recipe")
        or (manifest.get("recipe_selection") or {}).get("recipe")
        or (manifest.get("provenance") or {}).get("recipe_used")
        or ""
    ).strip()


def forecast_manifest_matches_recipe(path: str | Path, expected_recipe: str | None = None) -> bool:
    """Return whether a forecast manifest exists and records the expected recipe."""
    recipe = str(expected_recipe or "").strip()
    if not recipe:
        return True
    manifest_path = Path(path)
    if not manifest_path.exists():
        return False
    try:
        with open(manifest_path, "r", encoding="utf-8") as handle:
            payload = json.load(handle) or {}
    except Exception:
        return False
    return extract_manifest_recipe(payload) == recipe


def model_dir_complete_for_recipe(model_dir: str | Path, expected_recipe: str | None = None) -> bool:
    """Return whether an official model directory is complete for the expected recipe."""
    directory = Path(model_dir)
    forecast_manifest = directory / "forecast" / "forecast_manifest.json"
    ensemble_manifest = directory / "ensemble" / "ensemble_manifest.json"
    member_paths = list((directory / "ensemble").glob("member_*.nc"))
    if not forecast_manifest.exists() or not ensemble_manifest.exists() or not member_paths:
        return False
    return forecast_manifest_matches_recipe(forecast_manifest, expected_recipe)


def _validate_recipe_name(recipe_name: str, source_label: str) -> str:
    """Validate that a recipe exists in the canonical recipe config."""
    recipe = str(recipe_name).strip()
    if not recipe:
        raise RuntimeError(f"{source_label} did not provide a non-empty recipe name.")

    get_recipe_definition(recipe)
    return recipe


def _normalize_selection_status(
    source_kind: str,
    status_flag: str | None = None,
    valid: bool | None = None,
    provisional: bool | None = None,
    rerun_required: bool | None = None,
) -> tuple[str, bool, bool, bool]:
    """Normalize recipe-selection status flags to a consistent representation."""
    default_status = "valid" if source_kind in {"phase1_ranking", "frozen_historical_artifact"} else "provisional"
    normalized_status = str(status_flag or default_status).strip().lower()
    if normalized_status not in {"valid", "provisional", "rerun_required"}:
        normalized_status = default_status

    normalized_valid = bool(valid) if valid is not None else normalized_status == "valid"
    normalized_provisional = bool(provisional) if provisional is not None else normalized_status in {"provisional", "rerun_required"}
    normalized_rerun_required = bool(rerun_required) if rerun_required is not None else normalized_status == "rerun_required"
    return normalized_status, normalized_valid, normalized_provisional, normalized_rerun_required


def _build_recipe_selection(
    recipe: str,
    source_kind: str,
    source_path: str | None = None,
    note: str = "",
    status_flag: str | None = None,
    valid: bool | None = None,
    provisional: bool | None = None,
    rerun_required: bool | None = None,
) -> RecipeSelection:
    """Build a normalized recipe-selection record."""
    normalized_recipe = _validate_recipe_name(recipe, source_path or source_kind)
    normalized_status, normalized_valid, normalized_provisional, normalized_rerun_required = _normalize_selection_status(
        source_kind=source_kind,
        status_flag=status_flag,
        valid=valid,
        provisional=provisional,
        rerun_required=rerun_required,
    )
    return RecipeSelection(
        recipe=normalized_recipe,
        source_kind=source_kind,
        source_path=source_path,
        status_flag=normalized_status,
        valid=normalized_valid,
        provisional=normalized_provisional,
        rerun_required=normalized_rerun_required,
        note=note,
    )


def load_baseline_selection(selection_path: str | Path | None = None) -> tuple[Path, dict]:
    """Load the frozen Phase 1 baseline-selection artifact."""
    path = Path(selection_path) if selection_path else _default_baseline_selection_path()
    if not path.exists():
        raise FileNotFoundError(
            "Official workflow mode requires a frozen Phase 1 baseline selection file. "
            f"Missing: {path}. Provide {BASELINE_SELECTION_PATH_ENV}=<path> or "
            f"{BASELINE_RECIPE_OVERRIDE_ENV}=<recipe>."
        )

    with open(path, "r") as f:
        data = yaml.safe_load(f) or {}
    return path, data


def resolve_recipe_selection(
    ranking_csv: str | Path = None,
    default: str = DEFAULT_VALIDATED_FALLBACK_RECIPE,
    allow_fallback: bool | None = None,
    selection_path: str | Path | None = None,
) -> RecipeSelection:
    """Resolve the active recipe and its provenance/status metadata."""
    case = get_case_context()
    if allow_fallback is None:
        allow_fallback = allow_unvalidated_fallback()

    if case.is_official:
        override_recipe = os.environ.get(BASELINE_RECIPE_OVERRIDE_ENV, "").strip()
        if override_recipe:
            selection = _build_recipe_selection(
                recipe=override_recipe,
                source_kind="override_recipe",
                source_path=None,
                note=f"Manual override from {BASELINE_RECIPE_OVERRIDE_ENV}.",
                status_flag="provisional",
                valid=False,
                provisional=True,
                rerun_required=True,
            )
            logger.warning("Using baseline recipe override '%s' from %s.", selection.recipe, BASELINE_RECIPE_OVERRIDE_ENV)
            return selection

        try:
            path, selection = load_baseline_selection(selection_path)
        except FileNotFoundError:
            if allow_fallback:
                selection = _build_recipe_selection(
                    recipe=default,
                    source_kind="provisional_fallback",
                    source_path=None,
                    note=(
                        "Baseline artifact missing; using provisional fallback because "
                        f"{ALLOW_UNVALIDATED_FALLBACK_ENV}=1."
                    ),
                    status_flag="rerun_required",
                    valid=False,
                    provisional=True,
                    rerun_required=True,
                )
                logger.warning("%s", selection.note)
                return selection
            raise

        selected_recipe = selection.get("selected_recipe") or selection.get("recipe") or selection.get("baseline_recipe")
        if not selected_recipe:
            if allow_fallback:
                fallback_selection = _build_recipe_selection(
                    recipe=default,
                    source_kind="provisional_fallback",
                    source_path=str(path),
                    note=(
                        f"{path} is missing 'selected_recipe'; using provisional fallback because "
                        f"{ALLOW_UNVALIDATED_FALLBACK_ENV}=1."
                    ),
                    status_flag="rerun_required",
                    valid=False,
                    provisional=True,
                    rerun_required=True,
                )
                logger.warning("%s", fallback_selection.note)
                return fallback_selection
            raise RuntimeError(
                f"{path} is missing 'selected_recipe'. "
                f"Set {BASELINE_RECIPE_OVERRIDE_ENV}=<recipe> or update the baseline artifact."
            )

        resolved_selection = _build_recipe_selection(
            recipe=selected_recipe,
            source_kind=str(selection.get("source_kind") or selection.get("source_type") or "frozen_historical_artifact"),
            source_path=str(path),
            note=str(selection.get("note") or selection.get("selection_basis") or ""),
            status_flag=selection.get("status_flag") or selection.get("selection_status"),
            valid=selection.get("valid"),
            provisional=selection.get("provisional"),
            rerun_required=selection.get("rerun_required"),
        )
        logger.info(
            "Loaded official recipe selection from %s: %s [%s]",
            path,
            resolved_selection.recipe,
            resolved_selection.status_flag,
        )
        return resolved_selection

    from src.core.constants import BASE_OUTPUT_DIR

    if ranking_csv is None:
        ranking_csv = BASE_OUTPUT_DIR / "validation" / "validation_ranking.csv"

    path = Path(ranking_csv)
    fallback_reason = None

    if path.exists():
        df = pd.read_csv(path)
        if df.empty:
            fallback_reason = f"{path} exists but contains no valid recipes."
        elif "recipe" not in df.columns:
            fallback_reason = f"{path} is missing the required 'recipe' column."
        else:
            recipe = str(df.iloc[0]["recipe"]).strip()
            if recipe:
                selection = _build_recipe_selection(
                    recipe=recipe,
                    source_kind="phase1_ranking",
                    source_path=str(path),
                    note="Resolved from Phase 1 validation ranking.",
                    status_flag="valid",
                    valid=True,
                    provisional=False,
                    rerun_required=False,
                )
                logger.info("Loaded best recipe from %s: %s", path, selection.recipe)
                return selection
            fallback_reason = f"{path} contains a blank winning recipe."
    else:
        fallback_reason = f"{path} not found."

    message = (
        f"{fallback_reason} Phase 1 must produce a non-empty validation_ranking.csv "
        f"before downstream phases can continue."
    )

    if allow_fallback:
        selection = _build_recipe_selection(
            recipe=default,
            source_kind="provisional_fallback",
            source_path=str(path) if path.exists() else None,
            note=f"{message} Falling back because {ALLOW_UNVALIDATED_FALLBACK_ENV}=1.",
            status_flag="rerun_required",
            valid=False,
            provisional=True,
            rerun_required=True,
        )
        logger.warning("%s Falling back to unvalidated recipe '%s' because %s=1.", message, selection.recipe, ALLOW_UNVALIDATED_FALLBACK_ENV)
        return selection

    if path.exists():
        raise RuntimeError(message)
    raise FileNotFoundError(message)


def resolve_frozen_baseline_recipe(selection_path: str | Path | None = None) -> str:
    """Resolve the recipe for official workflows from an explicit frozen baseline artifact."""
    return resolve_recipe_selection(selection_path=selection_path).recipe


def load_drifter_data(path: str | Path) -> pd.DataFrame:
    """
    Load drifter data from a CSV or NetCDF file.
    Returns a cleaned DataFrame with columns: [time, lat, lon, ID]
    """
    path = Path(path)
    if path.suffix == ".csv":
        df = pd.read_csv(path, parse_dates=["time"])

        # Ensure ID column exists (ERDDAP usually provides "ID", forcing files might use "drifter_id")
        if "ID" not in df.columns and "id" in df.columns:
            df = df.rename(columns={"id": "ID"})

        # Strip timezone info to avoid offset mismatches with NetCDF readers.
        if hasattr(df["time"].dt, "tz") and df["time"].dt.tz is not None:
            df["time"] = df["time"].dt.tz_localize(None)

    elif path.suffix == ".nc":
        with xr.open_dataset(path) as ds:
            df = ds.to_dataframe().reset_index()
    else:
        raise ValueError(f"Unsupported file format: {path.suffix}")

    required_cols = ["time", "lat", "lon"]
    if not all(col in df.columns for col in required_cols):
        raise ValueError(f"Data missing required columns: {required_cols}. Found: {df.columns}")

    cols_to_keep = ["time", "lat", "lon"]
    if "ID" in df.columns:
        cols_to_keep.append("ID")

    return df[cols_to_keep].sort_values("time").reset_index(drop=True)


def select_drifter_of_record(drifter_df: pd.DataFrame) -> dict[str, Any]:
    """Select the legacy prototype drifter-of-record using the Phase 1 rule."""
    if drifter_df.empty:
        raise ValueError("Cannot select a drifter of record from an empty drifter DataFrame.")

    selected_df = drifter_df.copy()
    selected_df["time"] = pd.to_datetime(selected_df["time"], utc=True, errors="coerce")
    selected_df = selected_df.dropna(subset=["time", "lat", "lon"]).copy()
    if selected_df.empty:
        raise ValueError("Cannot select a drifter of record because the drifter data has no valid time/lat/lon rows.")

    selected_id: str | None = None
    point_count = int(len(selected_df))
    if "ID" in selected_df.columns:
        normalized_ids = selected_df["ID"].astype(str).str.strip()
        valid_ids = normalized_ids[normalized_ids != ""]
        if not valid_ids.empty:
            counts = valid_ids.value_counts()
            selected_id = str(counts.idxmax())
            point_count = int(counts[selected_id])
            selected_df = selected_df.loc[normalized_ids == selected_id].copy()

    selected_df["time"] = selected_df["time"].dt.tz_convert("UTC").dt.tz_localize(None)
    selected_df = selected_df.sort_values("time").reset_index(drop=True)
    first_row = selected_df.iloc[0]
    start_time = pd.Timestamp(first_row["time"]).strftime("%Y-%m-%dT%H:%M:%SZ")
    return {
        "drifter_df": selected_df,
        "selected_id": selected_id,
        "point_count": point_count,
        "start_time": start_time,
        "start_lat": float(first_row["lat"]),
        "start_lon": float(first_row["lon"]),
    }


def get_forcing_files(
    recipe_name: str,
    config_path: str = "config/recipes.yaml",
    *,
    run_name: str | None = None,
    forcing_dir: str | Path | None = None,
) -> Dict[str, Any]:
    """
    Read recipes.yaml and return the canonical forcing files for a given recipe.
    """
    recipe = get_recipe_definition(recipe_name, config_path)
    if forcing_dir is not None:
        data_dir = Path(forcing_dir)
    else:
        from src.core.constants import RUN_NAME

        data_dir = Path("data/forcing") / str(run_name or RUN_NAME)
    wave_file = recipe.get("wave_file")

    return {
        "recipe": recipe_name,
        "currents": data_dir / recipe["currents_file"],
        "wind": data_dir / recipe["wind_file"],
        "wave": data_dir / wave_file if wave_file else None,
        "duration_hours": recipe["duration_hours"],
        "time_step_minutes": recipe["time_step_minutes"],
        "description": recipe.get("description", recipe_name),
        "current_source": recipe.get("current_source") or _infer_source_name(recipe.get("currents_file")),
        "wind_source": recipe.get("wind_source") or _infer_source_name(recipe.get("wind_file")),
        "wave_source": recipe.get("wave_source") or _infer_source_name(wave_file),
    }


def get_prepared_input_manifest_path(run_name: str | None = None) -> Path:
    """Return the case-local prepared-input manifest path."""
    case = get_case_context()
    active_run_name = run_name or case.run_name
    return Path("data") / "prepared" / active_run_name / "prepared_input_manifest.csv"


def get_download_manifest_path() -> Path:
    """Return the shared ingestion download manifest path."""
    return Path("data") / "download_manifest.json"


def get_prepared_input_specs(
    recipe_name: str | None = None,
    require_drifter: bool = False,
    include_all_transport_forcing: bool = False,
    run_name: str | None = None,
) -> list[dict[str, str]]:
    """Return the canonical prepared-input requirements for the active workflow case."""
    case = get_case_context()
    active_run_name = run_name or case.run_name
    specs: list[dict[str, str]] = []

    def add_spec(label: str, path: Path, source: str):
        specs.append(
            {
                "label": label,
                "path": str(path),
                "source": source,
                "required": True,
            }
        )

    add_spec(
        "prepared_input_manifest",
        get_prepared_input_manifest_path(active_run_name),
        "Generated prepared-input manifest",
    )
    add_spec(
        "download_manifest",
        get_download_manifest_path(),
        "Generated ingestion download manifest",
    )

    arcgis_registry_path = Path("data") / "arcgis" / active_run_name / "arcgis_registry.csv"
    add_spec("arcgis_registry", arcgis_registry_path, "Generated ArcGIS layer registry")

    for layer in case.arcgis_layers:
        layer_source = f"ArcGIS FeatureServer layer {layer.layer_id}: {layer.service_url}"
        if case.is_official:
            add_spec(
                f"{layer.role}_raw_geojson",
                layer.raw_geojson_path(active_run_name),
                layer_source,
            )
            add_spec(
                f"{layer.role}_processed_vector",
                layer.processed_vector_path(active_run_name),
                f"Processed vector derived from {layer_source}",
            )
            add_spec(
                f"{layer.role}_service_metadata",
                layer.service_metadata_path(active_run_name),
                f"Archived service metadata from {layer_source}",
            )
            if layer.geometry_type == "polygon":
                mask_path = (
                    layer.official_observed_mask_path(active_run_name)
                    if layer.role == case.validation_layer.role
                    else layer.mask_path(active_run_name)
                )
                add_spec(
                    f"{layer.role}_mask",
                    mask_path,
                    f"Rasterized mask from processed {layer.role} geometry on the canonical scoring grid",
                )
        else:
            add_spec(
                f"{layer.role}_geojson",
                layer.geojson_path(active_run_name),
                layer_source,
            )
            if layer.geometry_type == "polygon":
                add_spec(
                    f"{layer.role}_mask",
                    layer.mask_path(active_run_name),
                    f"Rasterized mask from {layer_source}",
                )

    if case.is_official:
        add_spec(
            "arcgis_processing_report",
            Path("data") / "arcgis" / active_run_name / "arcgis_processing_report.csv",
            "Generated ArcGIS processing QA report",
        )

    if case.is_official:
        from src.helpers.scoring import get_scoring_grid_artifact_paths

        scoring_grid_paths = get_scoring_grid_artifact_paths()
        add_spec(
            "scoring_grid_metadata",
            scoring_grid_paths["metadata_yaml"],
            "Generated official scoring-grid metadata",
        )
        add_spec(
            "scoring_grid_template",
            scoring_grid_paths["template_tif"],
            "Generated official scoring-grid template raster",
        )
        add_spec(
            "scoring_grid_extent",
            scoring_grid_paths["extent_gpkg"],
            "Generated official scoring-grid extent polygon",
        )
        add_spec(
            "land_mask",
            scoring_grid_paths["land_mask_tif"],
            "Generated GSHHG-based land mask aligned to the official scoring grid",
        )
        add_spec(
            "sea_mask",
            scoring_grid_paths["sea_mask_tif"],
            "Generated GSHHG-based sea mask aligned to the official scoring grid",
        )
        add_spec(
            "shoreline_segments",
            scoring_grid_paths["shoreline_segments_gpkg"],
            "Generated shoreline segments derived from the GSHHG coastline on the official scoring grid",
        )
        add_spec(
            "shoreline_mask_manifest_json",
            scoring_grid_paths["shoreline_manifest_json"],
            "Generated machine-readable shoreline-mask manifest (JSON)",
        )
        add_spec(
            "shoreline_mask_manifest_csv",
            scoring_grid_paths["shoreline_manifest_csv"],
            "Generated machine-readable shoreline-mask manifest (CSV)",
        )

    if require_drifter:
        add_spec(
            "drifter_observations",
            Path("data") / "drifters" / active_run_name / "drifters_noaa.csv",
            "NOAA ERDDAP drifter_6hour_qc",
        )

    if include_all_transport_forcing:
        recipe_config = _load_recipes_config()
        recipes = recipe_config.get("recipes") or {}
        recipe_ids_to_include = (
            [str(recipe_name)]
            if recipe_name
            else get_transport_recipe_family_for_workflow(case.workflow_mode, config_path="config/recipes.yaml")
        )
        forcing_entries: dict[tuple[str, str], dict[str, str]] = {}
        forcing_dir = Path("data") / "forcing" / active_run_name
        for recipe_id in recipe_ids_to_include:
            recipe = dict(recipes.get(str(recipe_id)) or {})
            if not recipe:
                continue
            currents_file = recipe.get("currents_file")
            wind_file = recipe.get("wind_file")
            wave_file = recipe.get("wave_file")
            if currents_file:
                forcing_entries.setdefault(
                    ("forcing_currents", currents_file),
                    {
                        "label": f"forcing_currents_{Path(currents_file).name}",
                        "path": str(forcing_dir / currents_file),
                        "source": recipe.get("current_source") or _infer_source_name(currents_file) or "Currents forcing",
                    },
                )
            if wind_file:
                forcing_entries.setdefault(
                    ("forcing_wind", wind_file),
                    {
                        "label": f"forcing_wind_{Path(wind_file).name}",
                        "path": str(forcing_dir / wind_file),
                        "source": recipe.get("wind_source") or _infer_source_name(wind_file) or "Wind forcing",
                        "required": not (
                            case.is_prototype
                            and recipe_name is None
                            and Path(wind_file).name == "gfs_wind.nc"
                        ),
                    },
                )
            if wave_file:
                forcing_entries.setdefault(
                    ("forcing_wave", wave_file),
                    {
                        "label": f"forcing_wave_{Path(wave_file).name}",
                        "path": str(forcing_dir / wave_file),
                        "source": recipe.get("wave_source") or _infer_source_name(wave_file) or "Wave forcing",
                    },
                )
        specs.extend(forcing_entries.values())
    elif recipe_name:
        forcing = get_forcing_files(recipe_name)
        add_spec(
            f"recipe_currents_{recipe_name}",
            Path(forcing["currents"]),
            forcing.get("current_source") or "Currents forcing",
        )
        add_spec(
            f"recipe_wind_{recipe_name}",
            Path(forcing["wind"]),
            forcing.get("wind_source") or "Wind forcing",
        )
        if forcing.get("wave"):
            add_spec(
                f"recipe_wave_{recipe_name}",
                Path(forcing["wave"]),
                forcing.get("wave_source") or "Wave forcing",
            )

    return specs


def find_missing_prepared_inputs(
    recipe_name: str | None = None,
    require_drifter: bool = False,
    include_all_transport_forcing: bool = False,
    run_name: str | None = None,
) -> list[dict[str, str]]:
    """Return the prepared-input specs that are currently missing on disk."""
    missing = []
    for spec in get_prepared_input_specs(
        recipe_name=recipe_name,
        require_drifter=require_drifter,
        include_all_transport_forcing=include_all_transport_forcing,
        run_name=run_name,
    ):
        if not Path(spec["path"]).exists():
            missing.append(spec)
    return missing


def _manifest_shoreline_signature(manifest_payload: dict) -> str:
    grid = manifest_payload.get("grid") or {}
    return str(
        grid.get("shoreline_mask_signature")
        or manifest_payload.get("shoreline_mask_signature")
        or ""
    )


def detect_shoreline_mask_regeneration_need(
    *,
    manifest_payload: dict,
    manifest_path: str | Path,
    current_signature: str,
    label: str,
) -> list[dict[str, str]]:
    if not current_signature:
        return []
    recorded_signature = _manifest_shoreline_signature(manifest_payload)
    if recorded_signature == current_signature:
        return []
    return [
        {
            "label": label,
            "path": str(manifest_path),
            "source": (
                "Current shoreline-mask signature does not match the signature recorded in this manifest. "
                "Rerun the official forecast path to regenerate shoreline-compatible outputs."
            ),
        }
    ]


def get_case_output_dir(run_name: str | None = None) -> Path:
    """Return the case-local output directory."""
    case = get_case_context()
    active_run_name = run_name or case.run_name
    return Path("output") / active_run_name


def get_recipe_sensitivity_output_dir(run_name: str | None = None) -> Path:
    """Return the top-level event-sensitivity output directory for a case."""
    return get_case_output_dir(run_name) / "recipe_sensitivity"


def get_recipe_sensitivity_run_name(
    recipe_name: str,
    run_name: str | None = None,
) -> str:
    """Return the nested run name used for a specific event-sensitivity recipe."""
    case = get_case_context()
    active_run_name = run_name or case.run_name
    return str(PurePosixPath(active_run_name) / "recipe_sensitivity" / str(recipe_name))


def get_convergence_after_shoreline_output_dir(run_name: str | None = None) -> Path:
    """Return the top-level shoreline-aware convergence output directory for a case."""
    return get_case_output_dir(run_name) / "convergence_after_shoreline"


def get_convergence_after_shoreline_run_name(
    element_count: int,
    run_name: str | None = None,
) -> str:
    """Return the nested run name used for a specific shoreline-aware convergence experiment."""
    case = get_case_context()
    active_run_name = run_name or case.run_name
    return str(
        PurePosixPath(active_run_name)
        / "convergence_after_shoreline"
        / f"elements_{int(element_count):06d}"
    )


def get_forecast_output_dir(run_name: str | None = None) -> Path:
    """Return the case-local deterministic forecast directory."""
    return get_case_output_dir(run_name) / "forecast"


def get_forecast_manifest_path(run_name: str | None = None) -> Path:
    """Return the case-local spill-forecast manifest path."""
    return get_forecast_output_dir(run_name) / "forecast_manifest.json"


def get_ensemble_manifest_path(run_name: str | None = None) -> Path:
    """Return the case-local ensemble manifest path."""
    return get_case_output_dir(run_name) / "ensemble" / "ensemble_manifest.json"


def get_phase2_loading_audit_paths(run_name: str | None = None) -> dict[str, Path]:
    """Return the canonical official Phase 2 loading-audit paths."""
    forecast_dir = get_forecast_output_dir(run_name)
    return {
        "json": forecast_dir / "phase2_loading_audit.json",
        "csv": forecast_dir / "phase2_loading_audit.csv",
    }


def _timestamp_to_label(value) -> str:
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is not None:
        timestamp = timestamp.tz_convert("UTC").tz_localize(None)
    return timestamp.strftime("%Y-%m-%dT%H-%M-%SZ")


def _resolve_official_validation_timestamp():
    case = get_case_context()
    if case.validation_layer.event_time_utc:
        return pd.Timestamp(case.validation_layer.event_time_utc)
    return pd.Timestamp(case.simulation_end_utc)


def get_official_control_footprint_mask_path(timestamp=None, run_name: str | None = None) -> Path:
    """Return the canonical official deterministic footprint-mask path."""
    target_time = timestamp if timestamp is not None else _resolve_official_validation_timestamp()
    return get_forecast_output_dir(run_name) / f"control_footprint_mask_{_timestamp_to_label(target_time)}.tif"


def get_official_control_density_norm_path(timestamp=None, run_name: str | None = None) -> Path:
    """Return the canonical official deterministic normalized-density path."""
    target_time = timestamp if timestamp is not None else _resolve_official_validation_timestamp()
    return get_forecast_output_dir(run_name) / f"control_density_norm_{_timestamp_to_label(target_time)}.tif"


def get_official_prob_presence_path(timestamp=None, run_name: str | None = None) -> Path:
    """Return the canonical official ensemble probability raster path."""
    target_time = timestamp if timestamp is not None else _resolve_official_validation_timestamp()
    return get_case_output_dir(run_name) / "ensemble" / f"prob_presence_{_timestamp_to_label(target_time)}.tif"


def get_official_mask_threshold_path(
    threshold_label: str,
    timestamp=None,
    run_name: str | None = None,
) -> Path:
    """Return the canonical official ensemble threshold-mask path."""
    target_time = timestamp if timestamp is not None else _resolve_official_validation_timestamp()
    return get_case_output_dir(run_name) / "ensemble" / f"mask_{threshold_label}_{_timestamp_to_label(target_time)}.tif"


def get_official_mask_p50_datecomposite_path(run_name: str | None = None) -> Path:
    """Return the canonical official date-composite P50 path."""
    validation_date = str(_resolve_official_validation_timestamp().date())
    return get_case_output_dir(run_name) / "ensemble" / f"mask_p50_{validation_date}_datecomposite.tif"


def get_deterministic_control_output_path(recipe_name: str, run_name: str | None = None) -> Path:
    """Return the deterministic control output path for a recipe."""
    return get_forecast_output_dir(run_name) / f"deterministic_control_{recipe_name}.nc"


def get_deterministic_control_score_raster_dir(recipe_name: str, run_name: str | None = None) -> Path:
    """Return the canonical raster output directory for a deterministic control forecast."""
    return get_forecast_output_dir(run_name) / f"deterministic_control_{recipe_name}_rasters"


def get_deterministic_control_score_raster_path(
    recipe_name: str,
    hour: int = 72,
    run_name: str | None = None,
    raster_kind: str = "hits",
) -> Path:
    """Return the canonical deterministic-control score raster path."""
    if raster_kind not in {"hits", "p"}:
        raise ValueError(f"Unsupported raster_kind '{raster_kind}'. Expected 'hits' or 'p'.")
    return get_deterministic_control_score_raster_dir(recipe_name, run_name) / f"{raster_kind}_{hour}.tif"


def get_ensemble_probability_score_raster_path(hour: int = 72, run_name: str | None = None) -> Path:
    """Return the canonical ensemble probability score raster path."""
    return get_case_output_dir(run_name) / "ensemble" / f"probability_{hour}h.tif"


def get_phase3b_forecast_candidates(
    recipe_name: str,
    run_name: str | None = None,
) -> list[dict[str, str]]:
    """Return the forecast outputs that Phase 3B may score."""
    case = get_case_context()
    active_run_name = run_name or case.run_name
    case_dir = get_case_output_dir(active_run_name)

    candidates = []
    if case.is_official:
        candidates.append(
            {
                "label": "ensemble_mask_p50_datecomposite",
                "type": "ensemble_mask_p50_datecomposite",
                "path": str(get_official_mask_p50_datecomposite_path(run_name=active_run_name)),
                "source": "Official ensemble P50 date-composite footprint mask for the validation date",
            }
        )
        candidates.append(
            {
                "label": f"deterministic_control_{recipe_name}",
                "type": f"deterministic_control_{recipe_name}_footprint_validation",
                "path": str(get_official_control_footprint_mask_path(run_name=active_run_name)),
                "source": "Official deterministic control footprint mask at the validation timestamp",
            }
        )
    else:
        candidates.append(
            {
                "label": f"deterministic_best_{recipe_name}",
                "type": f"deterministic_best_{recipe_name}",
                "path": str(case_dir / "validation" / f"{recipe_name}_validation.nc"),
                "source": "Phase 1 deterministic validation artifact",
            }
        )

    if not case.is_official:
        candidates.append(
            {
                "label": "ensemble_prob_72h",
                "type": "ensemble_prob_72h",
                "path": str(case_dir / "ensemble" / "probability_72h.nc"),
                "source": "Phase 2 ensemble probability product at T+72h",
            }
        )
    return candidates


def find_missing_phase3b_forecast_outputs(
    recipe_name: str,
    run_name: str | None = None,
) -> list[dict[str, str]]:
    """Return the expected Phase 3B forecast outputs that are missing."""
    case = get_case_context()
    active_run_name = run_name or case.run_name
    missing = []

    if case.is_official:
        forecast_manifest = get_forecast_manifest_path(active_run_name)
        if not forecast_manifest.exists():
            missing.append(
                {
                    "label": "forecast_manifest",
                    "path": str(forecast_manifest),
                    "source": "Official spill forecast manifest",
                }
            )
        ensemble_manifest = get_ensemble_manifest_path(active_run_name)
        if not ensemble_manifest.exists():
            missing.append(
                {
                    "label": "ensemble_manifest",
                    "path": str(ensemble_manifest),
                    "source": "Official ensemble forecast manifest",
                }
            )
        from src.helpers.scoring import get_current_shoreline_mask_signature

        current_signature = get_current_shoreline_mask_signature()
        if forecast_manifest.exists():
            with open(forecast_manifest, "r", encoding="utf-8") as f:
                forecast_payload = json.load(f) or {}
            missing.extend(
                detect_shoreline_mask_regeneration_need(
                    manifest_payload=forecast_payload,
                    manifest_path=forecast_manifest,
                    current_signature=current_signature,
                    label="forecast_manifest_shoreline_refresh_required",
                )
            )
        if ensemble_manifest.exists():
            with open(ensemble_manifest, "r", encoding="utf-8") as f:
                ensemble_payload = json.load(f) or {}
            missing.extend(
                detect_shoreline_mask_regeneration_need(
                    manifest_payload=ensemble_payload,
                    manifest_path=ensemble_manifest,
                    current_signature=current_signature,
                    label="ensemble_manifest_shoreline_refresh_required",
                )
            )

    for spec in get_phase3b_forecast_candidates(recipe_name, active_run_name):
        if not Path(spec["path"]).exists():
            missing.append(spec)
    return missing


def resolve_best_recipe(
    ranking_csv: str | Path = None,
    default: str = DEFAULT_VALIDATED_FALLBACK_RECIPE,
    allow_fallback: bool | None = None,
) -> str:
    """
    Return the winning recipe name from Phase 1 output.
    Fail fast when Phase 1 did not produce a validated ranking unless
    ALLOW_UNVALIDATED_FALLBACK=1 (or allow_fallback=True) explicitly allows
    a fallback recipe.
    """
    selection = resolve_recipe_selection(
        ranking_csv=ranking_csv,
        default=default,
        allow_fallback=allow_fallback,
    )
    return selection.recipe


def resolve_spill_origin(
    drifter_csv: str | Path = None,
) -> tuple[float, float, str]:
    """
    Resolve the active spill origin reference (lat, lon, time_str).
    Prototype mode preserves the old Layer 0 / drifter fallback behavior.
    Official Mindoro mode uses the initialization polygon as the active release
    geometry and never uses the provenance Layer 0 point as the release origin.
    """
    from src.core.constants import RUN_NAME

    case = get_case_context()
    if case.is_official:
        provenance_point = resolve_provenance_source_point()
        if provenance_point is not None:
            lat, lon = provenance_point
            logger.info(
                "Loaded official provenance point for metadata/logging: (%.4f, %.4f)",
                lat,
                lon,
            )
            return lat, lon, case.release_start_utc

        init_path = resolve_initialization_polygon_path()
        lat, lon = _resolve_polygon_reference_point(init_path, geometry_type="polygon")
        logger.info(
            "Loaded official polygon reference from cleaned initialization vector: (%.4f, %.4f)",
            lat,
            lon,
        )
        return lat, lon, case.release_start_utc

    if case.workflow_mode == "prototype_2016":
        if drifter_csv is None:
            drifter_csv = Path(f"data/drifters/{RUN_NAME}/drifters_noaa.csv")
        path = Path(drifter_csv)
        if not path.exists():
            raise FileNotFoundError(
                f"Cannot resolve the prototype_2016 drifter-of-record origin: '{path}' not found."
            )

        selection = select_drifter_of_record(load_drifter_data(path))
        logger.info(
            "Loaded prototype_2016 drifter-of-record origin from drifter %s: (%.4f, %.4f) at %s",
            selection["selected_id"] or "<unlabeled>",
            selection["start_lat"],
            selection["start_lon"],
            selection["start_time"],
        )
        return selection["start_lat"], selection["start_lon"], selection["start_time"]

    provenance_point = resolve_provenance_source_point()
    if provenance_point is not None:
        lat, lon = provenance_point
        logger.info(f"Loaded prototype spill origin from Layer 0: ({lat:.4f}, {lon:.4f})")
        return lat, lon, case.release_start_utc

    if drifter_csv is None:
        drifter_csv = Path(f"data/drifters/{RUN_NAME}/drifters_noaa.csv")

    path = Path(drifter_csv)
    if not path.exists():
        raise FileNotFoundError(f"Cannot resolve spill origin: '{path}' not found. Drifter data is required.")

    selection = select_drifter_of_record(load_drifter_data(path))
    return selection["start_lat"], selection["start_lon"], selection["start_time"]


def resolve_polygon_seeding(
    num_elements: int,
    random_seed: int | None = None,
    polygon_path: str | Path | None = None,
    seed_time_override: str | None = None,
) -> tuple[list[float], list[float], str]:
    """
    Uniformly scatters points inside the authoritative initialization polygon.
    Returns (lons, lats, time_str) for models to ingest during seeding.

    When polygon_path is provided, it overrides the default official March 3
    initialization polygon for appendix/sensitivity reinitialization runs.
    """
    case = get_case_context()
    import geopandas as gpd

    seed_path = Path(polygon_path) if polygon_path else resolve_initialization_polygon_path()
    if not seed_path.exists():
        raise FileNotFoundError(f"Missing initialization polygon: {seed_path}")

    gdf = gpd.read_file(seed_path)
    valid_gdf = gdf.dropna(subset=["geometry"])
    if valid_gdf.empty:
        raise ValueError(f"No valid geometry found in {seed_path}")

    if case.is_official:
        lons, lats = _sample_points_from_polygons(valid_gdf, num_elements, random_seed=random_seed)
    else:
        united = valid_gdf.geometry.buffer(0).unary_union
        temp_series = gpd.GeoSeries([united], crs=valid_gdf.crs)
        pts = temp_series.sample_points(num_elements).explode(index_parts=False)
        lons = pts.x.tolist()
        lats = pts.y.tolist()

    seed_time = (
        str(seed_time_override)
        if seed_time_override is not None
        else case.release_start_utc
    )
    return lons, lats, seed_time


def resolve_initialization_polygon_path(run_name: str | None = None) -> Path:
    """Return the local GeoJSON path for the configured initialization polygon."""
    case = get_case_context()
    active_run_name = run_name or case.run_name
    return _resolve_layer_vector_path(case.initialization_layer, active_run_name, case.is_official)


def resolve_validation_polygon_path(run_name: str | None = None) -> Path:
    """Return the local GeoJSON path for the configured validation polygon."""
    case = get_case_context()
    active_run_name = run_name or case.run_name
    return _resolve_layer_vector_path(case.validation_layer, active_run_name, case.is_official)


def resolve_validation_mask_path(run_name: str | None = None) -> Path:
    """Return the local raster path for the configured validation mask."""
    case = get_case_context()
    active_run_name = run_name or case.run_name
    if case.is_official:
        preferred = case.validation_layer.official_observed_mask_path(active_run_name)
        if preferred.exists():
            return preferred
        return preferred
    return case.validation_layer.mask_path(active_run_name)


def resolve_provenance_source_point() -> tuple[float, float] | None:
    """Return the provenance Layer 0 point when available."""
    case = get_case_context()
    point_path = _resolve_layer_vector_path(case.provenance_layer, case.run_name, case.is_official)
    if not point_path.exists():
        return None

    try:
        import geopandas as gpd

        gdf = gpd.read_file(point_path)
        if gdf.empty or not gdf.geometry.notnull().any():
            return None
        pt = gdf.geometry.dropna().iloc[0]
        if gdf.crs is not None and str(gdf.crs).upper() != "EPSG:4326":
            pt = gpd.GeoSeries([pt], crs=gdf.crs).to_crs("EPSG:4326").iloc[0]
        return float(pt.y), float(pt.x)
    except Exception as e:
        logger.warning(f"Failed to read provenance source point {point_path}: {e}")
        return None


def _resolve_layer_vector_path(layer, run_name: str, official_mode: bool) -> Path:
    if official_mode:
        for path in (
            layer.processed_vector_path(run_name),
            layer.raw_geojson_path(run_name),
            layer.geojson_path(run_name),
        ):
            if path.exists():
                return path
        return layer.processed_vector_path(run_name)
    return layer.geojson_path(run_name)


def _sample_points_from_polygons(
    gdf,
    num_elements: int,
    random_seed: int | None = None,
) -> tuple[list[float], list[float]]:
    import geopandas as gpd

    work = gdf.dropna(subset=["geometry"]).copy()
    if work.empty:
        raise ValueError("No valid geometry is available for polygon seeding.")

    weights = work.geometry.apply(lambda geom: geom.area).to_numpy(dtype=float)
    if not np.isfinite(weights).all() or weights.sum() <= 0:
        weights = np.ones(len(work), dtype=float)
    probabilities = weights / weights.sum()
    rng = np.random.default_rng(random_seed)
    counts = rng.multinomial(num_elements, probabilities)

    point_series_parts = []
    for geometry, count in zip(work.geometry, counts):
        if count <= 0:
            continue
        samples = (
            gpd.GeoSeries([geometry], crs=work.crs)
            .sample_points(count, rng=rng)
            .explode(index_parts=False)
        )
        point_series_parts.append(samples.reset_index(drop=True))

    if not point_series_parts:
        raise ValueError("Polygon seeding did not generate any sample points.")

    points = gpd.GeoSeries(pd.concat(point_series_parts, ignore_index=True), crs=work.crs)
    if points.crs is not None and str(points.crs).upper() != "EPSG:4326":
        points = points.to_crs("EPSG:4326")
    return points.x.tolist(), points.y.tolist()


def _resolve_polygon_reference_point(path: str | Path, geometry_type: str = "polygon") -> tuple[float, float]:
    """Resolve a stable representative point from a cleaned vector geometry file."""
    import geopandas as gpd
    from src.services.arcgis import get_preferred_reference_geometry

    polygon_path = Path(path)
    if not polygon_path.exists():
        raise FileNotFoundError(f"Initialization polygon not found: {polygon_path}")

    gdf = gpd.read_file(polygon_path)
    valid_gdf = gdf.dropna(subset=["geometry"])
    if valid_gdf.empty:
        raise ValueError(f"No valid geometry found in {polygon_path}")

    representative_geometry = get_preferred_reference_geometry(valid_gdf, geometry_type)
    if representative_geometry is None:
        raise ValueError(f"No valid {geometry_type} geometry found in {polygon_path}")

    representative = (
        representative_geometry.representative_point()
        if geometry_type == "polygon"
        else representative_geometry
    )
    if valid_gdf.crs is not None and str(valid_gdf.crs).upper() != "EPSG:4326":
        representative = gpd.GeoSeries([representative], crs=valid_gdf.crs).to_crs("EPSG:4326").iloc[0]
    return float(representative.y), float(representative.x)


def find_wind_vars(ds: xr.Dataset) -> tuple[str, str]:
    """Detect U/V wind variable names."""
    candidates = [
        ("x_wind", "y_wind"),
        ("u10", "v10"),
        ("uwnd", "vwnd"),
        ("U10M", "V10M"),
    ]
    for u, v in candidates:
        if u in ds and v in ds:
            return u, v
    raise KeyError(f"No wind variables found. Available: {list(ds.data_vars)}")


def find_current_vars(ds: xr.Dataset) -> tuple[str, str]:
    """Detect U/V current variable names."""
    candidates = [
        ("x_sea_water_velocity", "y_sea_water_velocity"),
        ("uo", "vo"),
        ("water_u", "water_v"),
        ("u", "v"),
    ]
    for u, v in candidates:
        if u in ds and v in ds:
            return u, v
    raise KeyError(f"No current variables found. Available: {list(ds.data_vars)}")


def find_wave_vars(ds: xr.Dataset) -> tuple[str, str, str]:
    """Detect Stokes-drift and significant-wave-height variables."""
    direct_candidates = [
        (
            "sea_surface_wave_stokes_drift_x_velocity",
            "sea_surface_wave_stokes_drift_y_velocity",
            "sea_surface_wave_significant_height",
        ),
        ("VSDX", "VSDY", "VHM0"),
    ]
    for vx, vy, hs in direct_candidates:
        if vx in ds and vy in ds and hs in ds:
            return vx, vy, hs

    by_standard_name = {}
    for name, data_var in ds.data_vars.items():
        standard_name = str(data_var.attrs.get("standard_name") or "").strip()
        if standard_name:
            by_standard_name[standard_name] = name

    required = (
        "sea_surface_wave_stokes_drift_x_velocity",
        "sea_surface_wave_stokes_drift_y_velocity",
        "sea_surface_wave_significant_height",
    )
    if all(key in by_standard_name for key in required):
        return (
            by_standard_name[required[0]],
            by_standard_name[required[1]],
            by_standard_name[required[2]],
        )

    raise KeyError(
        "No wave/Stokes variable triplet found. "
        f"Available variables: {list(ds.data_vars)}"
    )


def select_nearest_point(ds: xr.Dataset, lat: float, lon: float) -> xr.Dataset:
    """Select nearest grid point, handling different coord names."""
    lat_names = ["latitude", "lat", "Latitude", "LATITUDE"]
    lon_names = ["longitude", "lon", "Longitude", "LONGITUDE"]

    lat_dim = next((n for n in lat_names if n in ds.coords or n in ds.dims), None)
    lon_dim = next((n for n in lon_names if n in ds.coords or n in ds.dims), None)

    if lat_dim and lon_dim:
        return ds.sel(**{lat_dim: lat, lon_dim: lon}, method="nearest")

    logger.warning("Could not identify lat/lon coordinates - using full spatial extent.")
    return ds
