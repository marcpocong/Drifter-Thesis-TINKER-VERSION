"""
Phase 3A benchmark pipeline orchestrator.

Pairs deterministic control products against deterministic PyGNOME products on
the canonical scoring grid and computes FSS/KL only on defensible raster pairs.
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

from src.helpers.metrics import calculate_fss, calculate_kl_divergence
from src.helpers.raster import GridBuilder, extract_particles_at_time, rasterize_particles, save_raster
from src.helpers.scoring import precheck_same_grid
from src.services.ensemble import EnsembleForecastService
from src.services.gnome_comparison import GNOME_AVAILABLE, GnomeComparisonService
from src.utils.io import get_forcing_files

logger = logging.getLogger(__name__)


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


class BenchmarkPipeline:
    def __init__(self, output_base: str = None):
        from src.core.constants import RUN_NAME

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

    def setup_directories(self):
        self.base_dir.mkdir(parents=True, exist_ok=True)
        for sub in ["grid", "control", "pygnome", "precheck", "qa", "logs"]:
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
            "start_time": start_time,
            "start_lat": start_lat,
            "start_lon": start_lon,
            "recipe": best_recipe,
            "fss_windows_km": self.fss_windows_km,
            "kl_epsilon": self.kl_epsilon,
            "grid": grid.spec.to_metadata(),
            "pygnome_benchmark": pygnome_metadata,
        }
        with open(self.base_dir / "config_snapshot.yaml", "w") as f:
            yaml.dump(config_snapshot, f, indent=2)

    def _window_cells(self, window_km: int, grid: GridBuilder) -> int:
        if str(grid.units).lower().startswith("meter"):
            return max(1, int(round((window_km * 1000.0) / float(grid.resolution))))
        return max(1, int(window_km))

    def _snapshot_targets(self, start_time: str, snapshot_hours: list[int]) -> list[tuple[int, pd.Timestamp]]:
        start_ts = _normalize_timestamp(start_time)
        return [(hour, start_ts + pd.Timedelta(hours=hour)) for hour in snapshot_hours]

    def _ensure_deterministic_control_products(
        self,
        service: EnsembleForecastService,
        recipe_name: str,
        start_time: str,
    ) -> list[dict]:
        target_records = []
        targets = self._snapshot_targets(start_time, service.snapshot_hours)
        expected_paths = [
            (
                service.forecast_dir / f"control_footprint_mask_{_timestamp_to_label(target_time)}.tif",
                service.forecast_dir / f"control_density_norm_{_timestamp_to_label(target_time)}.tif",
            )
            for _, target_time in targets
        ]
        if not all(foot.exists() and density.exists() for foot, density in expected_paths):
            service.run_deterministic_control(recipe_name=recipe_name, start_time=start_time)

        for hour, target_time in targets:
            label = _timestamp_to_label(target_time)
            source_footprint = service.forecast_dir / f"control_footprint_mask_{label}.tif"
            source_density = service.forecast_dir / f"control_density_norm_{label}.tif"
            if not source_footprint.exists() or not source_density.exists():
                raise FileNotFoundError(
                    f"Missing deterministic control products for {label}: {source_footprint} | {source_density}"
                )

            local_footprint = self.base_dir / "control" / source_footprint.name
            local_density = self.base_dir / "control" / source_density.name
            shutil.copyfile(source_footprint, local_footprint)
            shutil.copyfile(source_density, local_density)

            target_records.append(
                {
                    "hour": hour,
                    "timestamp_utc": _timestamp_to_utc_iso(target_time),
                    "control_source_footprint": str(source_footprint),
                    "control_source_density": str(source_density),
                    "control_footprint_path": str(local_footprint),
                    "control_density_path": str(local_density),
                }
            )

        return target_records

    def _generate_pygnome_products(
        self,
        start_lat: float,
        start_lon: float,
        start_time: str,
        snapshot_hours: list[int],
        grid: GridBuilder,
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
                raise RuntimeError(f"PyGNOME density raster is blank for benchmark snapshot {target_time}.")

            label = _timestamp_to_label(target_time)
            footprint_path = self.base_dir / "pygnome" / f"pygnome_footprint_mask_{label}.tif"
            density_path = self.base_dir / "pygnome" / f"pygnome_density_norm_{label}.tif"
            save_raster(grid, hits, footprint_path)
            save_raster(grid, density, density_path)

            py_records.append(
                {
                    "hour": hour,
                    "timestamp_utc": _timestamp_to_utc_iso(target_time),
                    "actual_snapshot_time_utc": _timestamp_to_utc_iso(actual_time),
                    "pygnome_nc_path": str(py_nc_path),
                    "pygnome_footprint_path": str(footprint_path),
                    "pygnome_density_path": str(density_path),
                    "pygnome_mass_strategy": extract_meta["mass_strategy"],
                    "pygnome_nonzero_density_cells": int(np.count_nonzero(density > 0)),
                }
            )

        py_metadata["output_dir"] = str(gnome_service.output_dir)
        with open(self.base_dir / "pygnome" / "pygnome_benchmark_metadata.json", "w") as f:
            json.dump(py_metadata, f, indent=2)

        return py_records, py_metadata

    def _load_sea_mask(self, grid: GridBuilder) -> np.ndarray:
        sea_mask_path = Path(grid.spec.sea_mask_path) if grid.spec.sea_mask_path else None
        if sea_mask_path and sea_mask_path.exists():
            sea_mask = _read_raster_data(sea_mask_path) > 0
            if sea_mask.shape == (grid.height, grid.width):
                return sea_mask
        return np.ones((grid.height, grid.width), dtype=bool)

    def _write_overlay(self, control_hits: np.ndarray, pygnome_hits: np.ndarray, target_time: pd.Timestamp) -> Path:
        label = _timestamp_to_label(target_time)
        overlay_path = self.base_dir / "qa" / f"footprint_overlay_{label}.png"
        control = control_hits > 0
        pygnome = pygnome_hits > 0
        overlay = np.zeros((*control_hits.shape, 3), dtype=np.float32)
        overlay[..., 0] = (control & ~pygnome).astype(np.float32)
        overlay[..., 1] = (control & pygnome).astype(np.float32)
        overlay[..., 2] = (~control & pygnome).astype(np.float32)
        plt.imsave(overlay_path, overlay)
        return overlay_path

    def _write_summary(self, fss_df: pd.DataFrame, kl_df: pd.DataFrame, pairing_df: pd.DataFrame):
        rows = []
        for window_km in self.fss_windows_km:
            subset = fss_df[fss_df["window_km"] == window_km]
            rows.append(
                {
                    "metric": "FSS",
                    "window_km": window_km,
                    "pair_count": int(len(subset)),
                    "mean_value": float(subset["fss"].mean()) if not subset.empty else np.nan,
                    "min_value": float(subset["fss"].min()) if not subset.empty else np.nan,
                    "max_value": float(subset["fss"].max()) if not subset.empty else np.nan,
                    "notes": "Footprint-mask Fractions Skill Score on deterministic control vs deterministic PyGNOME.",
                }
            )

        rows.append(
            {
                "metric": "KL",
                "window_km": "",
                "pair_count": int(len(kl_df)),
                "mean_value": float(kl_df["kl_divergence"].mean()) if not kl_df.empty else np.nan,
                "min_value": float(kl_df["kl_divergence"].min()) if not kl_df.empty else np.nan,
                "max_value": float(kl_df["kl_divergence"].max()) if not kl_df.empty else np.nan,
                "notes": "KL divergence on ocean-only normalized density rasters after epsilon handling and renormalization.",
            }
        )

        rows.append(
            {
                "metric": "PAIRING",
                "window_km": "",
                "pair_count": int(len(pairing_df)),
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
        self.logger.info("Starting Benchmark RUN_ID: %s", self.run_id)
        print(f"Starting Benchmark Case {self.run_id}")

        grid = GridBuilder()
        grid.save_metadata(self.base_dir / "grid" / "grid.json")
        sea_mask = self._load_sea_mask(grid)

        forcing = get_forcing_files(best_recipe)
        service = EnsembleForecastService(str(forcing["currents"]), str(forcing["wind"]))
        control_records = self._ensure_deterministic_control_products(service, best_recipe, start_time)

        print("   Running deterministic PyGNOME benchmark transport case...")
        py_records, py_metadata = self._generate_pygnome_products(
            start_lat=start_lat,
            start_lon=start_lon,
            start_time=start_time,
            snapshot_hours=service.snapshot_hours,
            grid=grid,
        )
        self.generate_config_snapshot(best_recipe, start_lat, start_lon, start_time, grid, py_metadata)

        py_by_timestamp = {record["timestamp_utc"]: record for record in py_records}
        pairing_rows = []
        fss_rows = []
        kl_rows = []

        for control in control_records:
            timestamp_utc = control["timestamp_utc"]
            if timestamp_utc not in py_by_timestamp:
                raise RuntimeError(f"Missing PyGNOME benchmark product for {timestamp_utc}.")
            py_record = py_by_timestamp[timestamp_utc]

            target_time = _normalize_timestamp(timestamp_utc)
            footprint_precheck = precheck_same_grid(
                control["control_footprint_path"],
                py_record["pygnome_footprint_path"],
                report_base_path=self.base_dir / "precheck" / f"footprint_{_timestamp_to_label(target_time)}",
            )
            density_precheck = precheck_same_grid(
                control["control_density_path"],
                py_record["pygnome_density_path"],
                report_base_path=self.base_dir / "precheck" / f"density_{_timestamp_to_label(target_time)}",
            )
            if not footprint_precheck.passed or not density_precheck.passed:
                raise RuntimeError(
                    f"Phase 3A same-grid precheck failed for {timestamp_utc}. "
                    f"Footprint report: {footprint_precheck.json_report_path} | "
                    f"Density report: {density_precheck.json_report_path}"
                )

            control_hits = _read_raster_data(Path(control["control_footprint_path"]))
            py_hits = _read_raster_data(Path(py_record["pygnome_footprint_path"]))
            control_density = _read_raster_data(Path(control["control_density_path"]))
            py_density = _read_raster_data(Path(py_record["pygnome_density_path"]))

            control_density_ocean_sum = float(np.clip(control_density[sea_mask], 0.0, None).sum())
            py_density_ocean_sum = float(np.clip(py_density[sea_mask], 0.0, None).sum())
            if control_density_ocean_sum <= 0.0 or py_density_ocean_sum <= 0.0:
                raise RuntimeError(
                    f"Invalid density pair for {timestamp_utc}: "
                    f"control_sum={control_density_ocean_sum}, pygnome_sum={py_density_ocean_sum}"
                )

            overlay_path = self._write_overlay(control_hits, py_hits, target_time)

            for window_km in self.fss_windows_km:
                window_cells = self._window_cells(window_km, grid)
                fss_rows.append(
                    {
                        "timestamp_utc": timestamp_utc,
                        "hour": control["hour"],
                        "window_km": window_km,
                        "window_cells": window_cells,
                        "fss": calculate_fss(control_hits, py_hits, window=window_cells),
                    }
                )

            kl_rows.append(
                {
                    "timestamp_utc": timestamp_utc,
                    "hour": control["hour"],
                    "epsilon": self.kl_epsilon,
                    "ocean_cell_count": int(np.count_nonzero(sea_mask)),
                    "kl_divergence": calculate_kl_divergence(
                        control_density,
                        py_density,
                        epsilon=self.kl_epsilon,
                        valid_mask=sea_mask,
                    ),
                }
            )

            pairing_rows.append(
                {
                    "timestamp_utc": timestamp_utc,
                    "hour": control["hour"],
                    "control_footprint_path": control["control_footprint_path"],
                    "pygnome_footprint_path": py_record["pygnome_footprint_path"],
                    "control_density_path": control["control_density_path"],
                    "pygnome_density_path": py_record["pygnome_density_path"],
                    "footprint_precheck_json": str(footprint_precheck.json_report_path),
                    "density_precheck_json": str(density_precheck.json_report_path),
                    "qa_overlay_path": str(overlay_path),
                    "pygnome_mass_strategy": py_record["pygnome_mass_strategy"],
                    "control_density_ocean_sum": control_density_ocean_sum,
                    "pygnome_density_ocean_sum": py_density_ocean_sum,
                }
            )

        pairing_df = pd.DataFrame(pairing_rows).sort_values("hour").reset_index(drop=True)
        fss_df = pd.DataFrame(fss_rows).sort_values(["hour", "window_km"]).reset_index(drop=True)
        kl_df = pd.DataFrame(kl_rows).sort_values("hour").reset_index(drop=True)

        pairing_df.to_csv(self.pairing_manifest_path, index=False)
        fss_df.to_csv(self.fss_manifest_path, index=False)
        kl_df.to_csv(self.kl_manifest_path, index=False)
        self._write_summary(fss_df, kl_df, pairing_df)

        self.logger.info("Benchmark complete.")
        print(f"Benchmark Complete. Outputs saved to: {self.base_dir}")
        return str(self.base_dir)
