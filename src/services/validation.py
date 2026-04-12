"""
Validation module for oil spill forecasts against drifter observations.
"""

import gc
import logging
from datetime import timedelta
from pathlib import Path
import shutil
import tempfile

import numpy as np
import pandas as pd
import xarray as xr
import yaml
from opendrift.models.oceandrift import OceanDrift
from opendrift.readers import reader_constant, reader_netCDF_CF_generic

from src.core.constants import REGION, RUN_NAME
from src.helpers.metrics import calculate_ncs
from src.helpers.plotting import plot_trajectory_map
from src.models.results import ValidationResult
from src.utils.io import find_current_vars, find_wind_vars, get_forcing_files

logger = logging.getLogger(__name__)

PHASE1_LOADING_AUDIT_SCHEMA_VERSION = "phase1_loading_audit_v2"
PHASE1_LOADING_AUDIT_POLICY = (
    "invalidate_recipe_on_required_forcing_or_simulation_failure_and_raise_if_no_valid_recipes_remain"
)


class TransportValidationService:
    def __init__(self, recipes_config: str = "config/recipes.yaml"):
        self.recipes_config = Path(recipes_config)

        if not self.recipes_config.exists():
            raise FileNotFoundError(f"Recipes configuration required at {self.recipes_config}")

        with open(self.recipes_config, "r") as f:
            self.config = yaml.safe_load(f) or {}

        self.recipe_map = self.config.get("recipes") or {}
        if not self.recipe_map:
            raise ValueError(f"No recipes found in {self.recipes_config}")

        self.recipes = list(self.recipe_map.keys())

    def run_validation(
        self,
        drifter_df: pd.DataFrame,
        output_dir: str = None,
        *,
        recipe_names: list[str] | None = None,
    ):
        """
        Run Phase 1 validation across the configured recipes.
        """
        payload = self._run_validation_internal(
            drifter_df=drifter_df,
            output_dir=output_dir,
            recipe_names=recipe_names or self.recipes,
            forcing_dir=None,
            summary_only=False,
            keep_scratch=True,
            transport_settings=None,
            require_wave_forcing=False,
            case_name_override=None,
            write_summary_files=True,
            verbose=True,
        )
        return payload["results_df"]

    def run_validation_summary(
        self,
        drifter_df: pd.DataFrame,
        *,
        forcing_dir: str | Path,
        recipe_names: list[str] | None = None,
        output_dir: str | Path | None = None,
        keep_scratch: bool = False,
        transport_settings: dict | None = None,
        require_wave_forcing: bool = True,
        case_name_override: str | None = None,
        verbose: bool = False,
    ) -> dict[str, pd.DataFrame]:
        """
        Run Phase 1 validation in summary-only mode.

        This mode reuses the transport-validation logic but avoids archiving
        permanent per-segment NetCDF/map bundles.
        """
        temp_dir: Path | None = None
        if output_dir is None:
            temp_dir = Path(tempfile.mkdtemp(prefix="phase1_validation_summary_"))
            output_dir = temp_dir

        try:
            payload = self._run_validation_internal(
                drifter_df=drifter_df,
                output_dir=output_dir,
                recipe_names=recipe_names or self.recipes,
                forcing_dir=forcing_dir,
                summary_only=True,
                keep_scratch=keep_scratch,
                transport_settings=transport_settings,
                require_wave_forcing=require_wave_forcing,
                case_name_override=case_name_override,
                write_summary_files=False,
                verbose=verbose,
            )
        finally:
            if temp_dir is not None and not keep_scratch:
                shutil.rmtree(temp_dir, ignore_errors=True)

        return {
            "results_df": payload["results_df"],
            "audit_df": payload["audit_df"],
        }

    def _run_validation_internal(
        self,
        *,
        drifter_df: pd.DataFrame,
        output_dir: str | Path | None,
        recipe_names: list[str],
        forcing_dir: str | Path | None,
        summary_only: bool,
        keep_scratch: bool,
        transport_settings: dict | None,
        require_wave_forcing: bool,
        case_name_override: str | None,
        write_summary_files: bool,
        verbose: bool,
    ) -> dict[str, pd.DataFrame | Path | None]:
        if output_dir is None:
            from src.core.constants import BASE_OUTPUT_DIR

            output_dir = BASE_OUTPUT_DIR / "validation"

        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        drifter_df = drifter_df.copy()
        if "ID" in drifter_df.columns:
            counts = drifter_df["ID"].value_counts()
            best_id = counts.idxmax()
            if verbose or not summary_only:
                print(f"Selecting Drifter ID {best_id} for validation (Points: {counts[best_id]})")
            drifter_df = drifter_df[drifter_df["ID"] == best_id].copy()
        drifter_df["time"] = self._normalize_drifter_times(drifter_df["time"])
        drifter_df = drifter_df.sort_values("time").reset_index(drop=True)

        results: list[ValidationResult] = []
        audit_rows: list[dict] = []

        start_time = drifter_df.iloc[0]["time"]
        start_lat = drifter_df.iloc[0]["lat"]
        start_lon = drifter_df.iloc[0]["lon"]

        for recipe_name in recipe_names:
            forcing = get_forcing_files(
                recipe_name,
                str(self.recipes_config),
                forcing_dir=forcing_dir,
            )
            output_file = output_dir / f"{recipe_name}_validation.nc"
            plot_file = None if summary_only else output_dir / f"map_{recipe_name}.png"

            self._remove_stale_file(output_file)
            if plot_file is not None:
                self._remove_stale_file(plot_file)

            audit = self._init_audit_row(recipe_name, forcing, None if summary_only else output_file, case_name=case_name_override)
            if verbose or not summary_only:
                print(f"\nRunning recipe: {recipe_name}")

            result = self._run_single_recipe(
                drifter_df=drifter_df,
                recipe_name=recipe_name,
                forcing=forcing,
                audit=audit,
                start_time=start_time,
                start_lat=start_lat,
                start_lon=start_lon,
                output_file=output_file,
                plot_file=plot_file,
                transport_settings=transport_settings,
                require_wave_forcing=require_wave_forcing,
                persist_output=not summary_only or keep_scratch,
                verbose=verbose or not summary_only,
            )
            audit_rows.append(result["audit"])
            if result["result"] is not None:
                results.append(result["result"])

        audit_df = pd.DataFrame(audit_rows)
        if not audit_df.empty:
            audit_df = audit_df.sort_values(["case_name", "recipe"]).reset_index(drop=True)

        results_data = [
            {"recipe": r.recipe_name, "ncs_score": r.ncs_score, "map_file": r.map_file}
            for r in results
        ]
        results_df = pd.DataFrame(results_data)
        if results_df.empty:
            results_df = pd.DataFrame(columns=["recipe", "ncs_score", "map_file"])
        else:
            results_df = results_df.sort_values("ncs_score").reset_index(drop=True)

        audit_path = output_dir / "phase1_loading_audit.csv"
        ranking_path = output_dir / "validation_ranking.csv"
        if write_summary_files:
            audit_df.to_csv(audit_path, index=False)
            results_df.to_csv(ranking_path, index=False)

            if results_df.empty:
                raise RuntimeError(
                    f"Phase 1 failed for {RUN_NAME}: no valid recipes remain. "
                    f"See {audit_path} for forcing load failures."
                )

        return {
            "results_df": results_df,
            "audit_df": audit_df,
            "audit_path": audit_path if write_summary_files else None,
            "ranking_path": ranking_path if write_summary_files else None,
        }

    @staticmethod
    def _normalize_drifter_times(times: pd.Series) -> pd.Series:
        normalized = pd.to_datetime(times, utc=True, errors="coerce")
        if normalized.isna().any():
            invalid_count = int(normalized.isna().sum())
            raise ValueError(f"Drifter validation input contains {invalid_count} unparseable timestamps.")
        return normalized.dt.tz_convert(None)

    def _run_single_recipe(
        self,
        drifter_df: pd.DataFrame,
        recipe_name: str,
        forcing: dict,
        audit: dict,
        start_time,
        start_lat: float,
        start_lon: float,
        output_file: Path,
        plot_file: Path | None,
        transport_settings: dict | None = None,
        require_wave_forcing: bool = False,
        persist_output: bool = True,
        verbose: bool = True,
    ) -> dict:
        o = OceanDrift(loglevel=20)
        transport_settings = dict(transport_settings or {})
        try:
            try:
                self._attach_required_reader(
                    model=o,
                    file_path=forcing["currents"],
                    reader_kind="current",
                    audit=audit,
                )
            except Exception as exc:
                self._invalidate(audit, f"Current forcing failed: {exc}", "current_fallback_used")

            try:
                self._attach_required_reader(
                    model=o,
                    file_path=forcing["wind"],
                    reader_kind="wind",
                    audit=audit,
                )
            except Exception as exc:
                self._invalidate(audit, f"Wind forcing failed: {exc}", "wind_fallback_used")

            self._attach_wave_reader(
                model=o,
                file_path=forcing.get("wave"),
                audit=audit,
                required=require_wave_forcing or bool(transport_settings.get("enable_stokes_drift")),
            )

            if audit["validity_flag"] != "valid":
                logger.error("Skipping invalid recipe %s: %s", recipe_name, audit["invalidity_reason"])
                return {"audit": audit, "result": None}

            o.set_config("general:use_auto_landmask", False)
            o.add_reader(reader_constant.Reader({"land_binary_mask": 0}))
            o.set_config("drift:horizontal_diffusivity", float(transport_settings.get("horizontal_diffusivity_m2s", 0.0)))
            o.set_config("drift:current_uncertainty", float(transport_settings.get("current_uncertainty", 0.0)))
            o.set_config("drift:stokes_drift", bool(transport_settings.get("enable_stokes_drift", False)))
            seed_kwargs = {}
            if "direct_wind_drift_factor" in transport_settings:
                seed_kwargs["wind_drift_factor"] = float(transport_settings["direct_wind_drift_factor"])
            o.seed_elements(lon=start_lon, lat=start_lat, time=start_time, number=1, **seed_kwargs)

            duration = timedelta(hours=int(forcing["duration_hours"]))
            time_step = timedelta(minutes=int(forcing["time_step_minutes"]))

            try:
                o.run(duration=duration, time_step=time_step, outfile=str(output_file))
            except Exception as exc:
                self._remove_stale_file(output_file)
                self._invalidate(audit, f"Simulation failed: {exc}")
                return {"audit": audit, "result": None}

            with xr.open_dataset(output_file) as ds_sim:
                if "trajectory" in ds_sim.dims:
                    lon_sim = ds_sim["lon"].values[0, :]
                    lat_sim = ds_sim["lat"].values[0, :]
                else:
                    lon_sim = ds_sim["lon"].values.flatten()
                    lat_sim = ds_sim["lat"].values.flatten()

            if not persist_output:
                self._remove_stale_file(output_file)

            min_len = min(len(lon_sim), len(drifter_df))
            ncs_score = calculate_ncs(
                lat_sim[:min_len],
                lon_sim[:min_len],
                drifter_df["lat"].values[:min_len],
                drifter_df["lon"].values[:min_len],
            )
            audit["ncs_score"] = float(ncs_score)

            if ncs_score > 1.0:
                if verbose:
                    print(f"   NCS Score: {ncs_score:.4f} (POOR - exceeds 1.0)")
                logger.warning(
                    "Recipe %s failed the NCS quality gate (%.2f > 1.0).",
                    recipe_name,
                    ncs_score,
                )
            else:
                if verbose:
                    print(f"   NCS Score: {ncs_score:.4f} (PASS)")

            if plot_file is not None:
                try:
                    plot_trajectory_map(
                        output_file=plot_file,
                        sim_lon=lon_sim,
                        sim_lat=lat_sim,
                        obs_lon=drifter_df["lon"].values,
                        obs_lat=drifter_df["lat"].values,
                        obs_ids=drifter_df["ID"].values if "ID" in drifter_df.columns else None,
                        corners=REGION,
                        title=self._build_plot_title(forcing["description"], start_time, duration),
                    )
                    audit["map_file"] = str(plot_file)
                except Exception as exc:
                    logger.warning("Plotting warning for %s: %s", recipe_name, exc)
                    audit["map_file"] = ""
            else:
                audit["map_file"] = ""

            return {
                "audit": audit,
                "result": ValidationResult(
                    recipe_name=recipe_name,
                    ncs_score=float(ncs_score),
                    map_file=str(plot_file) if audit["map_file"] else None,
                ),
            }
        finally:
            self._cleanup_model_resources(o)

    def _attach_required_reader(
        self,
        model: OceanDrift,
        file_path: Path,
        reader_kind: str,
        audit: dict,
    ) -> None:
        file_path = Path(file_path)
        if not file_path.exists():
            raise FileNotFoundError(f"Missing intended {reader_kind} file: {file_path}")

        required_vars, mapping, max_abs_value = self._inspect_forcing_file(file_path, reader_kind)
        reader = reader_netCDF_CF_generic.Reader(str(file_path))
        if mapping and hasattr(reader, "variable_mapping"):
            reader.variable_mapping.update(mapping)

        available_vars = set(getattr(reader, "variables", []))
        available_vars.update(getattr(reader, "variable_mapping", {}).keys())
        missing_required = [var for var in required_vars if var not in available_vars]
        if missing_required:
            raise ValueError(
                f"{reader_kind} reader does not expose required variables {missing_required} for {file_path.name}"
            )

        if reader_kind == "wind" and max_abs_value <= 0.0:
            raise ValueError(
                f"{file_path.name} resolves to zero-valued wind forcing; refusing fallback-zero wind validation."
            )

        model.add_reader(reader)

        if reader_kind == "current":
            audit["actual_current_reader"] = self._reader_label(reader, file_path)
            audit["current_fallback_used"] = False
            print(f"   Added current reader: {file_path.name}")
        elif reader_kind == "wind":
            audit["actual_wind_reader"] = self._reader_label(reader, file_path)
            audit["wind_fallback_used"] = False
            print(f"   Added wind reader: {file_path.name}")

    def _attach_wave_reader(
        self,
        model: OceanDrift,
        file_path: Path | None,
        audit: dict,
        *,
        required: bool,
    ) -> None:
        if not file_path:
            audit["wave_loading_status"] = "not_configured_required" if required else "not_configured"
            audit["wave_forcing_present"] = False
            if required:
                self._invalidate(audit, "Wave/Stokes forcing is required but was not configured.", "wave_fallback_used")
            return

        wave_path = Path(file_path)
        if not wave_path.exists():
            audit["wave_loading_status"] = "missing_file"
            audit["wave_forcing_present"] = False
            audit["wave_fallback_used"] = True
            if required:
                self._invalidate(audit, f"Wave/Stokes forcing is required but {wave_path} is missing.", "wave_fallback_used")
            logger.warning("Wave forcing not found for %s: %s", audit["recipe"], wave_path)
            return

        try:
            reader = reader_netCDF_CF_generic.Reader(str(wave_path))
            model.add_reader(reader)
            audit["actual_wave_reader"] = self._reader_label(reader, wave_path)
            audit["wave_forcing_present"] = True
            audit["wave_fallback_used"] = False
            audit["wave_loading_status"] = "loaded"
            print("   Added explicit wave/Stokes forcing")
        except Exception as exc:
            audit["wave_loading_status"] = f"attach_failed: {exc}"
            audit["wave_forcing_present"] = False
            audit["wave_fallback_used"] = True
            if required:
                self._invalidate(audit, f"Wave/Stokes reader attach failed: {exc}", "wave_fallback_used")
            logger.warning("Wave forcing attach failed for %s: %s", audit["recipe"], exc)

    def _inspect_forcing_file(self, file_path: Path, reader_kind: str) -> tuple[list[str], dict[str, str], float]:
        with xr.open_dataset(file_path) as ds:
            if reader_kind == "current":
                source_u, source_v = find_current_vars(ds)
                required = ["x_sea_water_velocity", "y_sea_water_velocity"]
            elif reader_kind == "wind":
                source_u, source_v = find_wind_vars(ds)
                required = ["x_wind", "y_wind"]
            else:
                raise ValueError(f"Unsupported reader kind: {reader_kind}")

            mapping = {}
            if source_u != required[0]:
                mapping[required[0]] = source_u
            if source_v != required[1]:
                mapping[required[1]] = source_v

            max_abs_value = max(
                self._max_abs(ds[source_u]),
                self._max_abs(ds[source_v]),
            )

        return required, mapping, max_abs_value

    def _init_audit_row(self, recipe_name: str, forcing: dict, output_file: Path | None, case_name: str | None = None) -> dict:
        return {
            "loading_audit_schema_version": PHASE1_LOADING_AUDIT_SCHEMA_VERSION,
            "loading_audit_policy": PHASE1_LOADING_AUDIT_POLICY,
            "case_name": case_name or RUN_NAME,
            "recipe": recipe_name,
            "description": forcing.get("description", recipe_name),
            "intended_current_source": forcing.get("current_source") or "",
            "intended_wind_source": forcing.get("wind_source") or "",
            "intended_wave_source": forcing.get("wave_source") or "",
            "actual_current_reader": "",
            "actual_wind_reader": "",
            "actual_wave_reader": "",
            "current_fallback_used": True,
            "wind_fallback_used": True,
            "wave_fallback_used": bool(forcing.get("wave")),
            "wave_forcing_present": False,
            "wave_loading_status": "not_attempted",
            "validity_flag": "valid",
            "status_flag": "valid",
            "hard_fail": False,
            "hard_fail_reason": "",
            "invalidity_reason": "",
            "output_path": str(output_file) if output_file else "",
            "map_file": "",
            "ncs_score": np.nan,
        }

    def _invalidate(self, audit: dict, reason: str, fallback_flag: str | None = None) -> None:
        audit["validity_flag"] = "invalid"
        audit["status_flag"] = "invalid"
        audit["hard_fail"] = True
        if fallback_flag:
            audit[fallback_flag] = True
        if audit["invalidity_reason"]:
            audit["invalidity_reason"] = f"{audit['invalidity_reason']}; {reason}"
        else:
            audit["invalidity_reason"] = reason
        audit["hard_fail_reason"] = audit["invalidity_reason"]

    @staticmethod
    def _max_abs(data_array: xr.DataArray) -> float:
        values = np.asarray(data_array.values)
        if values.size == 0:
            return 0.0
        values = values[np.isfinite(values)]
        if values.size == 0:
            return 0.0
        return float(np.max(np.abs(values)))

    @staticmethod
    def _reader_label(reader, file_path: Path) -> str:
        return f"{reader.__class__.__name__}<{file_path.name}>"

    @staticmethod
    def _cleanup_model_resources(model: OceanDrift) -> None:
        env = getattr(model, "env", None)
        readers = getattr(env, "readers", {}) if env is not None else {}
        for reader in readers.values():
            try:
                dataset = getattr(reader, "Dataset", None)
                if hasattr(dataset, "close"):
                    dataset.close()
            except Exception:
                pass
            for cache_name in ("var_block_before", "var_block_after"):
                block = getattr(reader, cache_name, None)
                if isinstance(block, dict):
                    for value in block.values():
                        if hasattr(value, "close"):
                            try:
                                value.close()
                            except Exception:
                                pass
        try:
            result = getattr(model, "result", None)
            if hasattr(result, "close"):
                result.close()
        except Exception:
            pass
        try:
            outfile = getattr(model, "outfile", None)
            if hasattr(outfile, "close"):
                outfile.close()
        except Exception:
            pass
        gc.collect()

    @staticmethod
    def _remove_stale_file(path: Path) -> None:
        if path.exists():
            path.unlink()

    @staticmethod
    def _build_plot_title(description: str, start_time, duration: timedelta) -> str:
        start_dt = pd.to_datetime(start_time)
        end_dt = start_dt + duration
        return (
            f"Phase 1: {description}\n"
            f"{start_dt.strftime('%Y-%m-%d %H:%M')} to {end_dt.strftime('%Y-%m-%d %H:%M')}"
        )
