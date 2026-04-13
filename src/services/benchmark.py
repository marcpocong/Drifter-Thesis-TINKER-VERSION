"""
Phase 3A benchmark pipeline orchestrator.

Pairs OpenDrift reference products against deterministic PyGNOME products on the
canonical scoring grid and computes FSS/KL only on defensible raster pairs.
"""

from __future__ import annotations

import json
import logging
import shutil
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import rasterio
import yaml

from src.core.case_context import get_case_context
from src.exceptions.custom import BenchmarkCaseSkipped
from src.helpers.plotting import prototype_2016_rendering_metadata
from src.helpers.metrics import calculate_fss, calculate_kl_divergence
from src.helpers.raster import GridBuilder, extract_particles_at_time, rasterize_particles, save_raster
from src.helpers.scoring import precheck_same_grid
from src.services.ensemble import EnsembleForecastService
from src.services.gnome_comparison import GNOME_AVAILABLE, GnomeComparisonService
from src.utils.io import get_forcing_files

logger = logging.getLogger(__name__)


COMPARISON_TRACK_REGISTRY = {
    "deterministic": {
        "label": "OpenDrift deterministic",
        "subdir": "control",
    },
    "ensemble_p50": {
        "label": "OpenDrift p50 occupancy footprint",
        "subdir": "ensemble_p50",
    },
    "ensemble_p90": {
        "label": "OpenDrift p90 occupancy footprint",
        "subdir": "ensemble_p90",
    },
}


def _normalize_timestamp(value) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    if ts.tzinfo is not None:
        ts = ts.tz_convert("UTC").tz_localize(None)
    return ts


def _timestamp_to_label(value) -> str:
    return _normalize_timestamp(value).strftime("%Y-%m-%dT%H-%M-%SZ")


def _timestamp_to_utc_iso(value) -> str:
    return _normalize_timestamp(value).strftime("%Y-%m-%dT%H:%M:%SZ")


def _read_raster_data(path: Path) -> np.ndarray:
    with rasterio.open(path) as src:
        return src.read(1)


def _raster_matches_grid(path: Path, grid: GridBuilder, tol: float = 1.0e-9) -> bool:
    if not path.exists():
        return False
    with rasterio.open(path) as src:
        if str(src.crs) != str(grid.crs):
            return False
        if int(src.width) != int(grid.width) or int(src.height) != int(grid.height):
            return False
        checks = (
            abs(float(src.transform.a) - float(grid.transform.a)) <= tol,
            abs(float(src.transform.b) - float(grid.transform.b)) <= tol,
            abs(float(src.transform.c) - float(grid.transform.c)) <= tol,
            abs(float(src.transform.d) - float(grid.transform.d)) <= tol,
            abs(float(src.transform.e) - float(grid.transform.e)) <= tol,
            abs(float(src.transform.f) - float(grid.transform.f)) <= tol,
        )
        return all(checks)


def _prepare_density_pair_for_metrics(
    *,
    forecast_density: np.ndarray,
    observed_density: np.ndarray,
    forecast_hits: np.ndarray,
    observed_hits: np.ndarray,
    valid_mask: np.ndarray,
    allow_blank_support_fallback: bool = False,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, dict[str, float | str]]:
    forecast_arr = np.asarray(forecast_density, dtype=float)
    observed_arr = np.asarray(observed_density, dtype=float)
    forecast_hits_arr = np.asarray(forecast_hits, dtype=float)
    observed_hits_arr = np.asarray(observed_hits, dtype=float)
    metric_mask = np.asarray(valid_mask, dtype=bool)

    if forecast_arr.shape != observed_arr.shape:
        raise ValueError("Benchmark density pairs must share the same shape.")
    if forecast_hits_arr.shape != forecast_arr.shape or observed_hits_arr.shape != observed_arr.shape:
        raise ValueError("Benchmark hit rasters must align with their density rasters.")
    if metric_mask.shape != forecast_arr.shape:
        raise ValueError("Benchmark valid_mask must align with the density rasters.")

    def _positive_sum(array: np.ndarray, mask: np.ndarray) -> float:
        return float(np.clip(np.asarray(array, dtype=float)[mask], 0.0, None).sum())

    raw_forecast_sum = _positive_sum(forecast_arr, metric_mask)
    raw_observed_sum = _positive_sum(observed_arr, metric_mask)
    metadata: dict[str, float | str] = {
        "raw_forecast_sum": raw_forecast_sum,
        "raw_observed_sum": raw_observed_sum,
        "effective_forecast_sum": raw_forecast_sum,
        "effective_observed_sum": raw_observed_sum,
        "strategy": "ocean_mask",
    }
    if not allow_blank_support_fallback:
        return forecast_arr, observed_arr, metric_mask, metadata

    rebuilt_from_hits = False
    if metadata["effective_forecast_sum"] <= 0.0 and _positive_sum(forecast_hits_arr, metric_mask) > 0.0:
        forecast_arr = np.where(metric_mask & (forecast_hits_arr > 0.0), 1.0, 0.0).astype(np.float64)
        forecast_arr /= float(forecast_arr[metric_mask].sum())
        metadata["effective_forecast_sum"] = _positive_sum(forecast_arr, metric_mask)
        rebuilt_from_hits = True
    if metadata["effective_observed_sum"] <= 0.0 and _positive_sum(observed_hits_arr, metric_mask) > 0.0:
        observed_arr = np.where(metric_mask & (observed_hits_arr > 0.0), 1.0, 0.0).astype(np.float64)
        observed_arr /= float(observed_arr[metric_mask].sum())
        metadata["effective_observed_sum"] = _positive_sum(observed_arr, metric_mask)
        rebuilt_from_hits = True

    if metadata["effective_forecast_sum"] > 0.0 and metadata["effective_observed_sum"] > 0.0:
        if rebuilt_from_hits:
            metadata["strategy"] = "support_hits_density_rebuild"
        return forecast_arr, observed_arr, metric_mask, metadata

    support_mask = (
        (np.clip(forecast_arr, 0.0, None) > 0.0)
        | (np.clip(observed_arr, 0.0, None) > 0.0)
        | (forecast_hits_arr > 0.0)
        | (observed_hits_arr > 0.0)
    )
    if not np.any(support_mask):
        support_mask = np.ones(forecast_arr.shape, dtype=bool)

    safe_forecast = np.asarray(forecast_arr, dtype=float)
    safe_observed = np.asarray(observed_arr, dtype=float)
    if _positive_sum(safe_forecast, support_mask) <= 0.0:
        safe_forecast = np.where(support_mask, 1.0, 0.0)
    if _positive_sum(safe_observed, support_mask) <= 0.0:
        safe_observed = np.where(support_mask, 1.0, 0.0)

    metadata["effective_forecast_sum"] = _positive_sum(safe_forecast, support_mask)
    metadata["effective_observed_sum"] = _positive_sum(safe_observed, support_mask)
    metadata["strategy"] = "support_positive_mask_fallback"
    return safe_forecast, safe_observed, support_mask, metadata


def ensure_point_within_benchmark_grid(*, lon: float, lat: float, grid: GridBuilder, label: str = "Benchmark spill origin") -> None:
    if (
        float(grid.min_lon) <= float(lon) <= float(grid.max_lon)
        and float(grid.min_lat) <= float(lat) <= float(grid.max_lat)
    ):
        return
    raise BenchmarkCaseSkipped(
        f"{label} {float(lon):.4f}E, {float(lat):.4f}N lies outside the benchmark grid "
        f"({float(grid.min_lon):.2f}-{float(grid.max_lon):.2f}E, {float(grid.min_lat):.2f}-{float(grid.max_lat):.2f}N). "
        "This case cannot produce defensible Phase 3A rasters on the current scoring grid."
    )


class BenchmarkPipeline:
    def __init__(self, output_base: str = None):
        from src.core.constants import RUN_NAME

        self.case = get_case_context()
        self.run_id = RUN_NAME
        self.base_dir = Path("output") / self.run_id / "benchmark" if output_base is None else Path(output_base) / self.run_id / "benchmark"
        self.setup_directories()

        self.logger = logging.getLogger(f"Benchmark_{self.run_id}")
        self.logger.setLevel(logging.INFO)
        self.logger.handlers.clear()
        handler = logging.FileHandler(self.base_dir / "logs" / "run.log")
        handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
        self.logger.addHandler(handler)
        self.logger.propagate = False

        self.fss_windows_km = [1, 3, 5, 10]
        self.kl_epsilon = 1e-10

        self.pairing_manifest_path = self.base_dir / "phase3a_pairing_manifest.csv"
        self.fss_manifest_path = self.base_dir / "phase3a_fss_by_time_window.csv"
        self.kl_manifest_path = self.base_dir / "phase3a_kl_by_time.csv"
        self.summary_path = self.base_dir / "phase3a_summary.csv"

    def _comparison_track_ids(self) -> list[str]:
        if self.case.workflow_mode == "prototype_2016":
            return ["deterministic", "ensemble_p50", "ensemble_p90"]
        return ["deterministic"]

    def setup_directories(self):
        self.base_dir.mkdir(parents=True, exist_ok=True)
        for sub in ["grid", "control", "ensemble_p50", "ensemble_p90", "pygnome", "precheck", "qa", "logs"]:
            (self.base_dir / sub).mkdir(parents=True, exist_ok=True)

    def generate_config_snapshot(
        self,
        best_recipe: str,
        start_lat: float,
        start_lon: float,
        start_time: str,
        grid: GridBuilder,
        pygnome_metadata: dict,
    ):
        config_snapshot = {
            "case_id": self.run_id,
            "workflow_mode": self.case.workflow_mode,
            "start_time": start_time,
            "start_lat": start_lat,
            "start_lon": start_lon,
            "recipe": best_recipe,
            "comparison_tracks": self._comparison_track_ids(),
            "fss_windows_km": self.fss_windows_km,
            "kl_epsilon": self.kl_epsilon,
            "grid": grid.spec.to_metadata(),
            "figure_rendering": prototype_2016_rendering_metadata(
                grid.display_bounds_wgs84 or [grid.min_lon, grid.max_lon, grid.min_lat, grid.max_lat]
            ),
            "pygnome_benchmark": pygnome_metadata,
        }
        with open(self.base_dir / "config_snapshot.yaml", "w", encoding="utf-8") as f:
            yaml.dump(config_snapshot, f, indent=2)

    def _window_cells(self, window_km: int, grid: GridBuilder) -> int:
        if str(grid.units).lower().startswith("meter"):
            return max(1, int(round((window_km * 1000.0) / float(grid.resolution))))
        return max(1, int(window_km))

    def _snapshot_targets(self, start_time: str, snapshot_hours: list[int]) -> list[tuple[int, pd.Timestamp]]:
        start_ts = _normalize_timestamp(start_time)
        return [(hour, start_ts + pd.Timedelta(hours=hour)) for hour in snapshot_hours]

    def _copy_track_product(
        self,
        *,
        source_footprint: Path,
        source_density: Path,
        track_id: str,
        hour: int,
        target_time: pd.Timestamp,
    ) -> dict:
        label = _timestamp_to_label(target_time)
        target_dir = self.base_dir / COMPARISON_TRACK_REGISTRY[track_id]["subdir"]
        local_footprint = target_dir / f"{track_id}_footprint_mask_{label}.tif"
        local_density = target_dir / f"{track_id}_density_norm_{label}.tif"
        shutil.copyfile(source_footprint, local_footprint)
        shutil.copyfile(source_density, local_density)
        return {
            "comparison_track_id": track_id,
            "comparison_track_label": COMPARISON_TRACK_REGISTRY[track_id]["label"],
            "hour": int(hour),
            "timestamp_utc": _timestamp_to_utc_iso(target_time),
            "opendrift_source_footprint": str(source_footprint),
            "opendrift_source_density": str(source_density),
            "opendrift_footprint_path": str(local_footprint),
            "opendrift_density_path": str(local_density),
            "control_source_footprint": str(source_footprint),
            "control_source_density": str(source_density),
            "control_footprint_path": str(local_footprint),
            "control_density_path": str(local_density),
        }

    def _ensure_deterministic_control_products(
        self,
        service: EnsembleForecastService,
        recipe_name: str,
        start_lat: float,
        start_lon: float,
        start_time: str,
        grid: GridBuilder,
    ) -> list[dict]:
        targets = self._snapshot_targets(start_time, service.snapshot_hours)
        control_nc_path = service.output_dir / f"deterministic_control_{recipe_name}.nc"
        expected_paths = [
            (
                service.forecast_dir / f"control_footprint_mask_{_timestamp_to_label(target_time)}.tif",
                service.forecast_dir / f"control_density_norm_{_timestamp_to_label(target_time)}.tif",
            )
            for _, target_time in targets
        ]
        if not all(
            _raster_matches_grid(foot, grid) and _raster_matches_grid(density, grid)
            for foot, density in expected_paths
        ):
            service.run_deterministic_control(
                recipe_name=recipe_name,
                start_time=start_time,
                start_lat=start_lat,
                start_lon=start_lon,
                force_point_release=self.case.workflow_mode == "prototype_2016",
            )

        track_records: list[dict] = []
        for hour, target_time in targets:
            label = _timestamp_to_label(target_time)
            source_footprint = service.forecast_dir / f"control_footprint_mask_{label}.tif"
            source_density = service.forecast_dir / f"control_density_norm_{label}.tif"
            if not source_footprint.exists() or not source_density.exists():
                raise FileNotFoundError(
                    f"Missing deterministic control products for {label}: {source_footprint} | {source_density}"
                )
            track_records.append(
                {
                    **self._copy_track_product(
                    source_footprint=source_footprint,
                    source_density=source_density,
                    track_id="deterministic",
                    hour=hour,
                    target_time=target_time,
                    ),
                    "opendrift_nc_path": str(control_nc_path) if control_nc_path.exists() else "",
                }
            )
        return track_records

    def _ensure_legacy_threshold_products(
        self,
        service: EnsembleForecastService,
        recipe_name: str,
        start_lat: float,
        start_lon: float,
        start_time: str,
        grid: GridBuilder,
    ) -> list[dict]:
        if self.case.workflow_mode != "prototype_2016":
            return []

        targets = self._snapshot_targets(start_time, service.snapshot_hours)
        expected_sources = [
            service.output_dir / f"mask_p50_{hour}h.tif"
            for hour, _ in targets
        ] + [
            service.output_dir / f"mask_p90_{hour}h.tif"
            for hour, _ in targets
        ]
        reuse_validation = service._validate_prototype_2016_reusable_science()
        if (not all(_raster_matches_grid(path, grid) for path in expected_sources)) or not reuse_validation.get("valid"):
            service.run_ensemble(
                recipe_name=recipe_name,
                start_lat=start_lat,
                start_lon=start_lon,
                start_time=start_time,
            )

        track_records: list[dict] = []
        for hour, target_time in targets:
            for track_id, filename in (
                ("ensemble_p50", f"mask_p50_{hour}h.tif"),
                ("ensemble_p90", f"mask_p90_{hour}h.tif"),
            ):
                source_path = service.output_dir / filename
                if not source_path.exists():
                    raise FileNotFoundError(
                        f"Missing legacy ensemble occupancy-footprint product for {track_id} {hour} h: {source_path}"
                    )
                track_records.append(
                    {
                        **self._copy_track_product(
                        source_footprint=source_path,
                        source_density=source_path,
                        track_id=track_id,
                        hour=hour,
                        target_time=target_time,
                        ),
                        "opendrift_nc_path": "",
                    }
                )
        return track_records

    def _build_opendrift_reference_products(
        self,
        service: EnsembleForecastService,
        recipe_name: str,
        start_lat: float,
        start_lon: float,
        start_time: str,
        grid: GridBuilder,
    ) -> list[dict]:
        deterministic_records = self._ensure_deterministic_control_products(
            service,
            recipe_name,
            start_lat,
            start_lon,
            start_time,
            grid,
        )
        extra_records = self._ensure_legacy_threshold_products(
            service,
            recipe_name,
            start_lat,
            start_lon,
            start_time,
            grid,
        )
        return sorted(
            [*deterministic_records, *extra_records],
            key=lambda item: (self._comparison_track_ids().index(item["comparison_track_id"]), int(item["hour"])),
        )

    def _generate_pygnome_products(
        self,
        start_lat: float,
        start_lon: float,
        start_time: str,
        snapshot_hours: list[int],
        grid: GridBuilder,
        *,
        winds_file: str | Path | None = None,
        currents_file: str | Path | None = None,
    ) -> tuple[list[dict], dict]:
        if not GNOME_AVAILABLE:
            raise RuntimeError("Phase 3A benchmark requires the gnome container.")

        gnome_service = GnomeComparisonService()
        gnome_service.output_dir = self.base_dir / "pygnome"
        gnome_service.output_dir.mkdir(parents=True, exist_ok=True)

        py_nc_path, py_metadata = gnome_service.run_transport_benchmark_scenario(
            start_lat=start_lat,
            start_lon=start_lon,
            start_time=start_time,
            output_name="pygnome_deterministic_control.nc",
            use_start_point_release=self.case.workflow_mode == "prototype_2016",
            winds_file=winds_file,
            currents_file=currents_file,
            allow_degraded_forcing=True,
        )
        if py_metadata.get("degraded_forcing"):
            self.logger.warning(
                "PyGNOME benchmark is running in degraded forcing mode for %s: %s",
                self.run_id,
                py_metadata.get("degraded_reason") or "unknown_reason",
            )

        py_records = []
        targets = self._snapshot_targets(start_time, snapshot_hours)
        for hour, target_time in targets:
            lon, lat, mass, actual_time, extract_meta = extract_particles_at_time(
                py_nc_path,
                target_time,
                "pygnome",
                allow_uniform_mass_fallback=False,
            )
            if len(lon) == 0:
                raise RuntimeError(f"PyGNOME produced no valid surface particles for benchmark snapshot {target_time}.")

            hits, density = rasterize_particles(grid, lon, lat, mass)
            if float(np.sum(density)) <= 0.0:
                lon_arr = np.asarray(lon, dtype=float)
                lat_arr = np.asarray(lat, dtype=float)
                inside_mask = (
                    (lon_arr >= float(grid.min_lon))
                    & (lon_arr <= float(grid.max_lon))
                    & (lat_arr >= float(grid.min_lat))
                    & (lat_arr <= float(grid.max_lat))
                )
                inside_count = int(np.count_nonzero(inside_mask))
                particle_count = int(len(lon_arr))
                particle_bounds = (
                    f"{float(np.min(lon_arr)):.2f}-{float(np.max(lon_arr)):.2f}E, "
                    f"{float(np.min(lat_arr)):.2f}-{float(np.max(lat_arr)):.2f}N"
                )
                grid_bounds = (
                    f"{float(grid.min_lon):.2f}-{float(grid.max_lon):.2f}E, "
                    f"{float(grid.min_lat):.2f}-{float(grid.max_lat):.2f}N"
                )
                raise RuntimeError(
                    f"PyGNOME density raster is blank for benchmark snapshot {target_time}. "
                    f"Only {inside_count}/{particle_count} surface particles fell inside the benchmark grid "
                    f"({grid_bounds}); particle bounds were {particle_bounds}. "
                    "This usually means the PyGNOME particle cloud drifted outside the scoring grid."
                )

            label = _timestamp_to_label(target_time)
            footprint_path = self.base_dir / "pygnome" / f"pygnome_footprint_mask_{label}.tif"
            density_path = self.base_dir / "pygnome" / f"pygnome_density_norm_{label}.tif"
            save_raster(grid, hits, footprint_path)
            save_raster(grid, density, density_path)

            py_records.append(
                {
                    "hour": int(hour),
                    "timestamp_utc": _timestamp_to_utc_iso(target_time),
                    "actual_snapshot_time_utc": _timestamp_to_utc_iso(actual_time),
                    "pygnome_nc_path": str(py_nc_path),
                    "pygnome_footprint_path": str(footprint_path),
                    "pygnome_density_path": str(density_path),
                    "pygnome_mass_strategy": extract_meta["mass_strategy"],
                    "pygnome_nonzero_density_cells": int(np.count_nonzero(density > 0)),
                    "pygnome_degraded_forcing": bool(py_metadata.get("degraded_forcing")),
                    "pygnome_degraded_reason": str(py_metadata.get("degraded_reason") or ""),
                    "pygnome_transport_forcing_mode": str(py_metadata.get("transport_forcing_mode") or ""),
                    "pygnome_current_mover_used": bool(py_metadata.get("current_mover_used")),
                }
            )

        py_metadata["output_dir"] = str(gnome_service.output_dir)
        with open(self.base_dir / "pygnome" / "pygnome_benchmark_metadata.json", "w", encoding="utf-8") as f:
            json.dump(py_metadata, f, indent=2)

        return py_records, py_metadata

    def _load_sea_mask(self, grid: GridBuilder) -> np.ndarray:
        sea_mask_path = Path(grid.spec.sea_mask_path) if grid.spec.sea_mask_path else None
        if sea_mask_path and sea_mask_path.exists():
            sea_mask = _read_raster_data(sea_mask_path) > 0
            if sea_mask.shape == (grid.height, grid.width):
                return sea_mask
        return np.ones((grid.height, grid.width), dtype=bool)

    def _write_overlay(
        self,
        opendrift_hits: np.ndarray,
        pygnome_hits: np.ndarray,
        target_time: pd.Timestamp,
        comparison_track_id: str,
    ) -> Path:
        label = _timestamp_to_label(target_time)
        overlay_path = self.base_dir / "qa" / f"{comparison_track_id}_overlay_{label}.png"
        control = opendrift_hits > 0
        pygnome = pygnome_hits > 0
        overlay = np.zeros((*opendrift_hits.shape, 3), dtype=np.float32)
        overlay[..., 0] = (control & ~pygnome).astype(np.float32)
        overlay[..., 1] = (control & pygnome).astype(np.float32)
        overlay[..., 2] = (~control & pygnome).astype(np.float32)
        plt.imsave(overlay_path, overlay)
        return overlay_path

    def _write_summary(self, fss_df: pd.DataFrame, kl_df: pd.DataFrame, pairing_df: pd.DataFrame):
        rows = []
        for comparison_track_id in self._comparison_track_ids():
            track_label = COMPARISON_TRACK_REGISTRY[comparison_track_id]["label"]
            track_fss = fss_df[fss_df["comparison_track_id"] == comparison_track_id]
            track_kl = kl_df[kl_df["comparison_track_id"] == comparison_track_id]
            track_pairings = pairing_df[pairing_df["comparison_track_id"] == comparison_track_id]
            for window_km in self.fss_windows_km:
                subset = track_fss[track_fss["window_km"] == window_km]
                rows.append(
                    {
                        "comparison_track_id": comparison_track_id,
                        "comparison_track_label": track_label,
                        "metric": "FSS",
                        "window_km": window_km,
                        "pair_count": int(len(subset)),
                        "mean_value": float(subset["fss"].mean()) if not subset.empty else np.nan,
                        "min_value": float(subset["fss"].min()) if not subset.empty else np.nan,
                        "max_value": float(subset["fss"].max()) if not subset.empty else np.nan,
                        "notes": (
                            "Footprint-mask Fractions Skill Score on "
                            f"{track_label} vs deterministic PyGNOME."
                        ),
                    }
                )

            rows.append(
                {
                    "comparison_track_id": comparison_track_id,
                    "comparison_track_label": track_label,
                    "metric": "KL",
                    "window_km": "",
                    "pair_count": int(len(track_kl)),
                    "mean_value": float(track_kl["kl_divergence"].mean()) if not track_kl.empty else np.nan,
                    "min_value": float(track_kl["kl_divergence"].min()) if not track_kl.empty else np.nan,
                    "max_value": float(track_kl["kl_divergence"].max()) if not track_kl.empty else np.nan,
                    "notes": "KL divergence on ocean-only normalized density rasters after epsilon handling and renormalization.",
                }
            )

            rows.append(
                {
                    "comparison_track_id": comparison_track_id,
                    "comparison_track_label": track_label,
                    "metric": "PAIRING",
                    "window_km": "",
                    "pair_count": int(len(track_pairings)),
                    "mean_value": np.nan,
                    "min_value": np.nan,
                    "max_value": np.nan,
                    "notes": "Benchmark raster pairing count.",
                }
            )

        pd.DataFrame(rows).to_csv(self.summary_path, index=False)

    def run(
        self,
        best_recipe: str,
        start_lat: float,
        start_lon: float,
        start_time: str,
        base_config_path: str = "config/oil.yaml",
    ):
        del base_config_path
        self.logger.info("Starting Benchmark RUN_ID: %s", self.run_id)
        print(f"Starting Benchmark Case {self.run_id}")

        grid = GridBuilder()
        ensure_point_within_benchmark_grid(lon=start_lon, lat=start_lat, grid=grid)
        grid_metadata_path = self.base_dir / "grid" / "grid.json"
        grid_metadata = grid.save_metadata(grid_metadata_path)
        if self.case.workflow_mode == "prototype_2016":
            grid_metadata.update(
                prototype_2016_rendering_metadata(
                    grid.display_bounds_wgs84 or [grid.min_lon, grid.max_lon, grid.min_lat, grid.max_lat]
                )
            )
            with open(grid_metadata_path, "w", encoding="utf-8") as handle:
                json.dump(grid_metadata, handle, indent=2)
        sea_mask = self._load_sea_mask(grid)

        forcing = get_forcing_files(best_recipe)
        service = EnsembleForecastService(str(forcing["currents"]), str(forcing["wind"]))
        opendrift_records = self._build_opendrift_reference_products(
            service,
            best_recipe,
            start_lat,
            start_lon,
            start_time,
            grid,
        )

        print("   Running deterministic PyGNOME benchmark transport case...")
        py_records, py_metadata = self._generate_pygnome_products(
            start_lat=start_lat,
            start_lon=start_lon,
            start_time=start_time,
            snapshot_hours=service.snapshot_hours,
            grid=grid,
            winds_file=str(service.winds_file),
            currents_file=str(service.currents_file),
        )
        self.generate_config_snapshot(best_recipe, start_lat, start_lon, start_time, grid, py_metadata)

        py_by_timestamp = {record["timestamp_utc"]: record for record in py_records}
        pairing_rows = []
        fss_rows = []
        kl_rows = []

        for opendrift in opendrift_records:
            timestamp_utc = opendrift["timestamp_utc"]
            if timestamp_utc not in py_by_timestamp:
                raise RuntimeError(f"Missing PyGNOME benchmark product for {timestamp_utc}.")
            py_record = py_by_timestamp[timestamp_utc]

            target_time = _normalize_timestamp(timestamp_utc)
            precheck_prefix = f"{opendrift['comparison_track_id']}_{_timestamp_to_label(target_time)}"
            footprint_precheck = precheck_same_grid(
                opendrift["opendrift_footprint_path"],
                py_record["pygnome_footprint_path"],
                report_base_path=self.base_dir / "precheck" / f"footprint_{precheck_prefix}",
            )
            density_precheck = precheck_same_grid(
                opendrift["opendrift_density_path"],
                py_record["pygnome_density_path"],
                report_base_path=self.base_dir / "precheck" / f"density_{precheck_prefix}",
            )
            if not footprint_precheck.passed or not density_precheck.passed:
                raise RuntimeError(
                    f"Phase 3A same-grid precheck failed for {timestamp_utc} "
                    f"({opendrift['comparison_track_id']}). "
                    f"Footprint report: {footprint_precheck.json_report_path} | "
                    f"Density report: {density_precheck.json_report_path}"
                )

            opendrift_hits = _read_raster_data(Path(opendrift["opendrift_footprint_path"]))
            py_hits = _read_raster_data(Path(py_record["pygnome_footprint_path"]))
            opendrift_density = _read_raster_data(Path(opendrift["opendrift_density_path"]))
            py_density = _read_raster_data(Path(py_record["pygnome_density_path"]))

            allow_blank_support_fallback = (
                self.case.workflow_mode == "prototype_2016"
                and opendrift["comparison_track_id"] in {"ensemble_p50", "ensemble_p90"}
            )
            metric_forecast_density, metric_py_density, kl_valid_mask, density_pair_meta = _prepare_density_pair_for_metrics(
                forecast_density=opendrift_density,
                observed_density=py_density,
                forecast_hits=opendrift_hits,
                observed_hits=py_hits,
                valid_mask=sea_mask,
                allow_blank_support_fallback=allow_blank_support_fallback,
            )
            opendrift_density_ocean_sum = float(density_pair_meta["raw_forecast_sum"])
            py_density_ocean_sum = float(density_pair_meta["raw_observed_sum"])
            if (
                float(density_pair_meta["effective_forecast_sum"]) <= 0.0
                or float(density_pair_meta["effective_observed_sum"]) <= 0.0
            ):
                raise RuntimeError(
                    f"Invalid density pair for {timestamp_utc} ({opendrift['comparison_track_id']}): "
                    f"opendrift_sum={opendrift_density_ocean_sum}, pygnome_sum={py_density_ocean_sum}"
                )
            if str(density_pair_meta["strategy"]) != "ocean_mask":
                self.logger.warning(
                    "Legacy support track %s at %s used %s during KL preparation "
                    "(raw_opendrift_sum=%.6f, raw_pygnome_sum=%.6f).",
                    opendrift["comparison_track_id"],
                    timestamp_utc,
                    density_pair_meta["strategy"],
                    opendrift_density_ocean_sum,
                    py_density_ocean_sum,
                )

            overlay_path = self._write_overlay(
                opendrift_hits,
                py_hits,
                target_time,
                opendrift["comparison_track_id"],
            )

            for window_km in self.fss_windows_km:
                window_cells = self._window_cells(window_km, grid)
                fss_rows.append(
                    {
                        "comparison_track_id": opendrift["comparison_track_id"],
                        "comparison_track_label": opendrift["comparison_track_label"],
                        "timestamp_utc": timestamp_utc,
                        "hour": int(opendrift["hour"]),
                        "window_km": window_km,
                        "window_cells": window_cells,
                        "fss": calculate_fss(opendrift_hits, py_hits, window=window_cells),
                    }
                )

            kl_rows.append(
                {
                    "comparison_track_id": opendrift["comparison_track_id"],
                    "comparison_track_label": opendrift["comparison_track_label"],
                    "timestamp_utc": timestamp_utc,
                    "hour": int(opendrift["hour"]),
                    "epsilon": self.kl_epsilon,
                    "ocean_cell_count": int(np.count_nonzero(kl_valid_mask)),
                    "kl_divergence": calculate_kl_divergence(
                        metric_forecast_density,
                        metric_py_density,
                        epsilon=self.kl_epsilon,
                        valid_mask=kl_valid_mask,
                    ),
                }
                )

            pairing_rows.append(
                {
                    "comparison_track_id": opendrift["comparison_track_id"],
                    "comparison_track_label": opendrift["comparison_track_label"],
                    "timestamp_utc": timestamp_utc,
                    "hour": int(opendrift["hour"]),
                    "opendrift_footprint_path": opendrift["opendrift_footprint_path"],
                    "pygnome_footprint_path": py_record["pygnome_footprint_path"],
                    "opendrift_density_path": opendrift["opendrift_density_path"],
                    "opendrift_nc_path": opendrift.get("opendrift_nc_path", ""),
                    "pygnome_density_path": py_record["pygnome_density_path"],
                    "control_footprint_path": opendrift["control_footprint_path"],
                    "control_density_path": opendrift["control_density_path"],
                    "footprint_precheck_json": str(footprint_precheck.json_report_path),
                    "density_precheck_json": str(density_precheck.json_report_path),
                    "qa_overlay_path": str(overlay_path),
                    "pygnome_nc_path": py_record["pygnome_nc_path"],
                    "pygnome_mass_strategy": py_record["pygnome_mass_strategy"],
                    "pygnome_degraded_forcing": bool(py_record.get("pygnome_degraded_forcing")),
                    "pygnome_degraded_reason": str(py_record.get("pygnome_degraded_reason") or ""),
                    "pygnome_transport_forcing_mode": str(py_record.get("pygnome_transport_forcing_mode") or ""),
                    "pygnome_current_mover_used": bool(py_record.get("pygnome_current_mover_used")),
                    "opendrift_density_ocean_sum": opendrift_density_ocean_sum,
                    "pygnome_density_ocean_sum": py_density_ocean_sum,
                    "density_pair_strategy": density_pair_meta["strategy"],
                }
            )

        pairing_df = pd.DataFrame(pairing_rows).sort_values(["comparison_track_id", "hour"]).reset_index(drop=True)
        fss_df = pd.DataFrame(fss_rows).sort_values(["comparison_track_id", "hour", "window_km"]).reset_index(drop=True)
        kl_df = pd.DataFrame(kl_rows).sort_values(["comparison_track_id", "hour"]).reset_index(drop=True)

        pairing_df.to_csv(self.pairing_manifest_path, index=False)
        fss_df.to_csv(self.fss_manifest_path, index=False)
        kl_df.to_csv(self.kl_manifest_path, index=False)
        self._write_summary(fss_df, kl_df, pairing_df)

        self.logger.info("Benchmark complete.")
        print(f"Benchmark Complete. Outputs saved to: {self.base_dir}")
        return str(self.base_dir)
