"""
Helpers for thesis-facing domain semantics.

The repo now treats three geographic concepts as distinct:
- phase1_validation_box: Chapter 3 historical/regional transport-validation box
- mindoro_case_domain: broad official Mindoro spill-case fallback transport/forcing domain
  and overview extent, not the canonical scoring-grid display bounds
- legacy_prototype_display_domain: prototype/debug plotting extent

`region` remains only as a backward-compatible alias for stale configs and code.
"""

from __future__ import annotations

from typing import Any, Mapping

DEFAULT_PHASE1_VALIDATION_BOX = [119.5, 124.5, 11.5, 16.5]
DEFAULT_MINDORO_CASE_DOMAIN = [115.0, 122.0, 6.0, 14.5]
DEFAULT_LEGACY_PROTOTYPE_DISPLAY_DOMAIN = [115.0, 122.0, 6.0, 14.5]


def coerce_bounds(value: Any, label: str) -> list[float]:
    if value is None:
        raise ValueError(f"{label} is missing.")
    if not isinstance(value, (list, tuple)) or len(value) != 4:
        raise ValueError(f"{label} must contain [min_lon, max_lon, min_lat, max_lat].")
    return [float(item) for item in value]


def _resolve_named_bounds(
    primary_key: str,
    *sources: Mapping[str, Any] | None,
    legacy_keys: tuple[str, ...] = (),
    default: list[float],
) -> list[float]:
    for source in sources:
        if not source:
            continue
        if source.get(primary_key) is not None:
            return coerce_bounds(source.get(primary_key), primary_key)
        for legacy_key in legacy_keys:
            if source.get(legacy_key) is not None:
                return coerce_bounds(source.get(legacy_key), legacy_key)
    return coerce_bounds(default, primary_key)


def resolve_phase1_validation_box(*sources: Mapping[str, Any] | None) -> list[float]:
    return _resolve_named_bounds(
        "phase1_validation_box",
        *sources,
        legacy_keys=("regional_validation_box",),
        default=DEFAULT_PHASE1_VALIDATION_BOX,
    )


def resolve_mindoro_case_domain(*sources: Mapping[str, Any] | None) -> list[float]:
    return _resolve_named_bounds(
        "mindoro_case_domain",
        *sources,
        legacy_keys=("region",),
        default=DEFAULT_MINDORO_CASE_DOMAIN,
    )


def resolve_legacy_prototype_display_domain(*sources: Mapping[str, Any] | None) -> list[float]:
    return _resolve_named_bounds(
        "legacy_prototype_display_domain",
        *sources,
        legacy_keys=("region",),
        default=DEFAULT_LEGACY_PROTOTYPE_DISPLAY_DOMAIN,
    )
