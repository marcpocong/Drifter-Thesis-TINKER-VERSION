from __future__ import annotations

from pathlib import Path

import yaml


ENSEMBLE_CONFIG_PATH = Path("config/ensemble.yaml")


def load_ensemble_config(path: Path | str = ENSEMBLE_CONFIG_PATH) -> dict:
    config_path = Path(path)
    with open(config_path, "r", encoding="utf-8") as handle:
        return yaml.safe_load(handle) or {}


def get_official_forecast_block(config: dict) -> dict:
    return dict(config.get("official_forecast") or {})


def get_official_ensemble_block(config: dict) -> dict:
    return dict((get_official_forecast_block(config).get("ensemble") or {}))


def get_legacy_perturbations_block(config: dict) -> dict:
    return dict(config.get("legacy_perturbations_inactive") or {})


def get_active_legacy_perturbations(config: dict) -> dict:
    if config.get("perturbations") is not None:
        raise RuntimeError(
            "Top-level `perturbations` is deprecated. Move legacy prototype settings to "
            "`legacy_perturbations_inactive` and activate them explicitly for support-only runs."
        )

    legacy = get_legacy_perturbations_block(config)
    if not legacy or not bool(legacy.get("active", False)):
        raise RuntimeError(
            "Legacy prototype perturbations are inactive in config/ensemble.yaml. "
            "Official/reportable lanes must use `official_forecast`, and support-only "
            "prototype runs must opt in by setting `legacy_perturbations_inactive.active: true`."
        )
    return legacy
