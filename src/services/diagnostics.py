"""
Phase 3 Enhancement: Simulation Data Diagnostics Service.

Identifies why OpenOil produces unphysical results (100% dispersion for heavy
bunker fuel by hour 20) compared to PyGNOME's realistic persistence (~75%
surface mass at 72 h).

Diagnostic checks:
  1. Environmental Forcing – extract max wind speed & wave height from ERA5/CMEMS.
  2. Configuration Audit  – compare current OpenOil config vs NOAA operational defaults.
  3. PyGNOME Import Test  – verify gnome package availability & report status.
"""

import logging
import importlib
from datetime import timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr

from src.core.constants import (
    NOAA_DEFAULTS,
    WIND_SPEED_REALISTIC_MAX_MS,
    WAVE_HEIGHT_REALISTIC_MAX_M,
    WIND_SPEED_MODERATE_MS,
)
from src.utils.io import find_wind_vars, find_current_vars, select_nearest_point, get_forcing_files

logger = logging.getLogger(__name__)

class DiagnosticsService:
    """
    Pre-flight diagnostic checks for Phase 3 weathering simulations.

    Designed to be run *before* the main OpenOil weathering pass to surface
    configuration or data issues that would cause unphysical budgets.
    """

    def __init__(
        self,
        currents_file: str,
        winds_file: str,
        spill_lat: float,
        spill_lon: float,
        start_time: str,
        diagnostics_label: str = "Phase 3 Enhancement – Pre-Flight Diagnostics",
    ):
        self.currents_file = Path(currents_file)
        self.winds_file = Path(winds_file)
        self.spill_lat = spill_lat
        self.spill_lon = spill_lon
        self.start_time = pd.to_datetime(start_time)
        self.diagnostics_label = str(diagnostics_label)

        from src.core.constants import BASE_OUTPUT_DIR
        self.output_dir = BASE_OUTPUT_DIR / "diagnostics"
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.report: dict = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run_all(self) -> dict:
        """
        Execute all diagnostic checks and return a structured report dict.
        """
        print("\n" + "=" * 60)
        print("🔍 Phase 3 Enhancement – Pre-Flight Diagnostics")
        print("=" * 60)

        self._check_environmental_forcing()
        self._check_openoil_config()
        self._check_gnome_availability()
        self._print_summary()
        self._save_report()

        return self.report

    # ------------------------------------------------------------------
    # 1. Environmental Forcing Check
    # ------------------------------------------------------------------

    def _check_environmental_forcing(self):
        """
        Extract max wind speed and wave height from the forcing files
        at the spill origin over the 72-hour forecast window.
        """
        print("\n── 1. Environmental Forcing Analysis ──────────────────────")
        forcing_report = {}

        # --- Wind data ---
        if self.winds_file.exists():
            try:
                wind_stats = self._extract_wind_stats()
                forcing_report["wind"] = wind_stats
                max_ws = wind_stats["max_wind_speed_ms"]
                mean_ws = wind_stats["mean_wind_speed_ms"]

                status = "✅ NORMAL"
                if max_ws > WIND_SPEED_REALISTIC_MAX_MS:
                    status = "🚨 EXTREME (possibly corrupt data)"
                elif max_ws > WIND_SPEED_MODERATE_MS:
                    status = "⚠️  HIGH (expect significant dispersion)"

                print(f"   Wind file: {self.winds_file.name}")
                print(f"   Max wind speed:  {max_ws:.2f} m/s  [{status}]")
                print(f"   Mean wind speed: {mean_ws:.2f} m/s")
                print(f"   Max U-wind:      {wind_stats['max_u_wind']:.2f} m/s")
                print(f"   Max V-wind:      {wind_stats['max_v_wind']:.2f} m/s")

                if max_ws > WIND_SPEED_MODERATE_MS:
                    print(f"   💡 Diagnosis: Wind speeds above {WIND_SPEED_MODERATE_MS} m/s")
                    print(f"      will drive excessive wave entrainment in OpenOil,")
                    print(f"      potentially explaining 100% dispersion for heavy oil.")

            except Exception as e:
                forcing_report["wind"] = {"error": str(e)}
                print(f"   ❌ Failed to read wind data: {e}")
        else:
            forcing_report["wind"] = {"error": "File not found"}
            print(f"   ❌ Wind file not found: {self.winds_file}")

        # --- Wave data (if present in wind/current files) ---
        wave_stats = self._extract_wave_stats()
        if wave_stats:
            forcing_report["waves"] = wave_stats
            max_wh = wave_stats.get("max_wave_height_m", 0)
            print(f"\n   Wave data found:")
            print(f"   Max significant wave height: {max_wh:.2f} m")
            if max_wh > WAVE_HEIGHT_REALISTIC_MAX_M:
                print(f"   🚨 Wave height exceeds realistic max ({WAVE_HEIGHT_REALISTIC_MAX_M} m)")
        else:
            forcing_report["waves"] = {"note": "No explicit wave data; OpenOil will compute from wind"}
            print(f"\n   ℹ️  No explicit wave height variable found in forcing.")
            print(f"      OpenOil will internally estimate waves from wind speed.")

        # --- Current data ---
        if self.currents_file.exists():
            try:
                curr_stats = self._extract_current_stats()
                forcing_report["currents"] = curr_stats
                print(f"\n   Current file: {self.currents_file.name}")
                print(f"   Max current speed: {curr_stats['max_current_speed_ms']:.3f} m/s")
                print(f"   Mean current speed: {curr_stats['mean_current_speed_ms']:.3f} m/s")
            except Exception as e:
                forcing_report["currents"] = {"error": str(e)}
                print(f"   ❌ Failed to read current data: {e}")
        else:
            forcing_report["currents"] = {"error": "File not found"}
            print(f"   ❌ Currents file not found: {self.currents_file}")

        self.report["environmental_forcing"] = forcing_report

    def _extract_wind_stats(self) -> dict:
        """Read wind NetCDF and compute statistics near the spill origin."""
        ds = xr.open_dataset(str(self.winds_file))

        # Identify wind variable names (ERA5: u10/v10, NCEP: uwnd/vwnd, or x_wind/y_wind)
        u_var, v_var = find_wind_vars(ds)

        # Spatial subset: nearest grid point to spill
        ds_point = select_nearest_point(ds, self.spill_lat, self.spill_lon)

        # Time subset: 72-hour window from start_time
        t_end = self.start_time + timedelta(hours=72)
        ds_point = ds_point.sel(time=slice(self.start_time, t_end))

        u = ds_point[u_var].values.flatten()
        v = ds_point[v_var].values.flatten()
        u = u[~np.isnan(u)]
        v = v[~np.isnan(v)]

        wind_speed = np.sqrt(u**2 + v**2)
        ds.close()

        return {
            "max_wind_speed_ms": float(np.max(wind_speed)) if len(wind_speed) > 0 else 0.0,
            "mean_wind_speed_ms": float(np.mean(wind_speed)) if len(wind_speed) > 0 else 0.0,
            "max_u_wind": float(np.max(np.abs(u))) if len(u) > 0 else 0.0,
            "max_v_wind": float(np.max(np.abs(v))) if len(v) > 0 else 0.0,
            "num_timesteps": len(wind_speed),
            "source_file": self.winds_file.name,
        }

    def _extract_wave_stats(self) -> dict | None:
        """Check for wave height variables in both forcing files."""
        wave_vars = ["VHM0", "VHMO", "swh", "significant_wave_height",
                     "sea_surface_wave_significant_height", "Hs"]
        for fpath in [self.winds_file, self.currents_file]:
            if not fpath.exists():
                continue
            try:
                ds = xr.open_dataset(str(fpath))
                for wv in wave_vars:
                    if wv in ds:
                        ds_point = select_nearest_point(ds, self.spill_lat, self.spill_lon)
                        vals = ds_point[wv].values.flatten()
                        vals = vals[~np.isnan(vals)]
                        ds.close()
                        if len(vals) > 0:
                            return {
                                "max_wave_height_m": float(np.max(vals)),
                                "mean_wave_height_m": float(np.mean(vals)),
                                "source_file": fpath.name,
                                "variable": wv,
                            }
                ds.close()
            except Exception:
                continue
        return None

    def _extract_current_stats(self) -> dict:
        """Read current NetCDF and compute statistics near spill origin."""
        ds = xr.open_dataset(str(self.currents_file))

        u_var, v_var = find_current_vars(ds)
        ds_point = select_nearest_point(ds, self.spill_lat, self.spill_lon)

        t_end = self.start_time + timedelta(hours=72)
        ds_point = ds_point.sel(time=slice(self.start_time, t_end))

        u = ds_point[u_var].values.flatten()
        v = ds_point[v_var].values.flatten()
        u = u[~np.isnan(u)]
        v = v[~np.isnan(v)]

        speed = np.sqrt(u**2 + v**2)
        ds.close()

        return {
            "max_current_speed_ms": float(np.max(speed)) if len(speed) > 0 else 0.0,
            "mean_current_speed_ms": float(np.mean(speed)) if len(speed) > 0 else 0.0,
            "num_timesteps": len(speed),
            "source_file": self.currents_file.name,
        }

    # ------------------------------------------------------------------
    # 2. OpenOil Configuration Audit
    # ------------------------------------------------------------------

    def _check_openoil_config(self):
        """
        Instantiate a temporary OpenOil model, apply the overrides from
        config/oil.yaml, then compare against NOAA operational defaults.

        Only mismatches that *remain after overrides* are flagged as issues.
        """
        print("\n── 2. OpenOil Configuration Audit ─────────────────────────")
        config_report = {
            "defaults_match": True,
            "mismatches": [],
            "post_override_mismatches": [],
            "current_values": {},
        }

        try:
            import yaml as _yaml
            from opendrift.models.openoil import OpenOil

            # --- Load overrides from oil.yaml ---
            oil_yaml_path = Path("config/oil.yaml")
            all_overrides: dict = {}
            explicitly_overridden_keys: set = set()
            if oil_yaml_path.exists():
                with open(oil_yaml_path, "r") as f:
                    oil_cfg = _yaml.safe_load(f)
                for _oil_key, ocfg in oil_cfg.get("oils", {}).items():
                    for k, v in ocfg.get("openoil_overrides", {}).items():
                        all_overrides[k] = v
                        explicitly_overridden_keys.add(k)

            # --- Create temporary model & apply overrides ---
            o = OpenOil(loglevel=50, weathering_model="noaa")
            for cfg_key, cfg_val in all_overrides.items():
                try:
                    o.set_config(cfg_key, cfg_val)
                except Exception:
                    pass

            # --- Audit post-override values vs NOAA defaults ---
            print(f"   {'Config Key':<52} {'Effective':<22} {'NOAA Default':<22} Status")
            print(f"   {'─'*52} {'─'*22} {'─'*22} {'─'*8}")

            for key, noaa_val in NOAA_DEFAULTS.items():
                try:
                    current_val = o.get_config(key)
                except Exception:
                    current_val = "<not found>"

                config_report["current_values"][key] = str(current_val)

                match = self._values_match(current_val, noaa_val)

                if match:
                    status = "\u2705"
                elif key in explicitly_overridden_keys:
                    # Value differs from generic NOAA default but is
                    # explicitly set in oil.yaml (e.g. heavy-oil droplet
                    # diameters). Treat as intentional, not a problem.
                    status = "\u2705 (oil-specific)"
                else:
                    status = "\u26a0\ufe0f"
                    config_report["defaults_match"] = False
                    entry = {
                        "key": key,
                        "current": str(current_val),
                        "noaa_default": str(noaa_val),
                    }
                    config_report["mismatches"].append(entry)
                    config_report["post_override_mismatches"].append(entry)

                print(f"   {key:<52} {str(current_val):<22} {str(noaa_val):<22} {status}")

            # --- Critical parameters summary ---
            print(f"\n   📋 Critical Parameters for Heavy Oil (IFO 380):")
            critical_keys = [
                "seed:droplet_diameter_min_subsea",
                "seed:droplet_diameter_max_subsea",
                "wave_entrainment:droplet_size_distribution",
                "vertical_mixing:diffusivitymodel",
                "processes:dispersion",
            ]
            for key in critical_keys:
                try:
                    val = o.get_config(key)
                    print(f"      {key}: {val}")
                except Exception:
                    print(f"      {key}: <not available>")

            n_issues = len(config_report["post_override_mismatches"])
            if n_issues > 0:
                print(f"\n   💡 Diagnosis: {n_issues} config value(s) still differ from NOAA defaults")
                print(f"      after applying oil.yaml overrides.")
            else:
                print(f"\n   ✅ All config values match NOAA defaults or are intentional oil-specific overrides.")

        except ImportError as e:
            config_report["error"] = f"OpenOil not importable: {e}"
            print(f"   ❌ Cannot import OpenOil: {e}")
        except Exception as e:
            config_report["error"] = str(e)
            print(f"   ❌ Config audit failed: {e}")

        self.report["openoil_config"] = config_report

    # ------------------------------------------------------------------
    # 3. PyGNOME Availability Check
    # ------------------------------------------------------------------

    def _check_gnome_availability(self):
        """Verify whether PyGNOME can be imported and report status."""
        print("\n── 3. PyGNOME Availability Check ──────────────────────────")
        gnome_report = {}

        try:
            import gnome  # noqa: F811
            gnome_report["available"] = True
            gnome_report["version"] = getattr(gnome, "__version__", "unknown")
            gnome_report["location"] = getattr(gnome, "__file__", "unknown")
            print(f"   ✅ PyGNOME is available")
            print(f"      Version:  {gnome_report['version']}")
            print(f"      Location: {gnome_report['location']}")

            # Check critical sub-imports
            sub_modules = [
                "gnome.model",
                "gnome.movers",
                "gnome.outputters",
                "gnome.spills",
                "gnome.environment",
                "gnome.weatherers",
            ]
            for mod_name in sub_modules:
                try:
                    importlib.import_module(mod_name)
                    print(f"      ✅ {mod_name}")
                except ImportError as e:
                    print(f"      ❌ {mod_name}: {e}")
                    gnome_report.setdefault("failed_imports", []).append(
                        {"module": mod_name, "error": str(e)}
                    )

        except ImportError as e:
            gnome_report["available"] = False
            gnome_report["error"] = str(e)
            print(f"   ❌ PyGNOME NOT available: {e}")
            print(f"   💡 Diagnosis: PyGNOME is only installed in the `gnome` container.")
            print(f"      Make sure to run Phase 3 via: docker compose exec gnome python -m src")
            print(f"      Running in `pipeline` container will silently skip GNOME comparison.")

        # Check container identity
        import os
        pipeline_phase = os.environ.get("PIPELINE_PHASE", "1_2")
        gnome_report["pipeline_phase_env"] = pipeline_phase
        expected_container = "gnome (Phase 3)" if pipeline_phase == "3" else "pipeline (Phase 1+2)"
        print(f"\n   Container context: PIPELINE_PHASE={pipeline_phase} → {expected_container}")

        if pipeline_phase != "3" and not gnome_report.get("available", False):
            print(f"   ⚠️  You are in the pipeline container. PyGNOME requires the gnome container.")

        self.report["gnome_availability"] = gnome_report

    # ------------------------------------------------------------------
    # Summary & Save
    # ------------------------------------------------------------------

    def _print_summary(self):
        """Print a final diagnostic summary with actionable recommendations."""
        print("\n" + "=" * 60)
        print("📋 Diagnostic Summary & Recommendations")
        print("=" * 60)

        issues_found = 0

        # Wind assessment
        wind = self.report.get("environmental_forcing", {}).get("wind", {})
        max_ws = wind.get("max_wind_speed_ms", 0)
        if max_ws > WIND_SPEED_MODERATE_MS:
            issues_found += 1
            print(f"\n   🔸 Issue #{issues_found}: HIGH WIND FORCING ({max_ws:.1f} m/s)")
            print(f"      Wind speeds above {WIND_SPEED_MODERATE_MS} m/s cause aggressive wave")
            print(f"      entrainment in OpenOil. For heavy bunker fuel (IFO 380),")
            print(f"      this drives unrealistic rapid dispersion.")
            print(f"      → FIX: Cap wind drift factor or increase droplet diameter range.")

        # Config mismatches (only flag values still wrong AFTER overrides)
        config = self.report.get("openoil_config", {})
        mismatches = config.get("post_override_mismatches", [])
        if mismatches:
            issues_found += 1
            print(f"\n   \U0001f538 Issue #{issues_found}: OPENOIL CONFIG MISMATCHES ({len(mismatches)} found after overrides)")
            for mm in mismatches:
                print(f"      • {mm['key']}: {mm['current']} (should be {mm['noaa_default']})")
            print(f"      → FIX: Set these in oil.yaml under 'openoil_overrides' per oil type.")

        # GNOME availability
        gnome = self.report.get("gnome_availability", {})
        if not gnome.get("available", False):
            issues_found += 1
            print(f"\n   🔸 Issue #{issues_found}: PyGNOME NOT AVAILABLE")
            print(f"      Cross-comparison is silently skipped without PyGNOME.")
            print(f"      → FIX: Run Phase 3 in gnome container: docker compose exec gnome python -m src")

        if issues_found == 0:
            print(f"\n   ✅ No critical issues found. Configuration looks correct.")
        else:
            print(f"\n   ⚠️  {issues_found} issue(s) found. Apply fixes above before re-running weathering.")

    def _save_report(self):
        """Save diagnostic report to CSV for traceability."""
        rows = []

        # Flatten report into rows
        for category, data in self.report.items():
            if isinstance(data, dict):
                for key, val in data.items():
                    if isinstance(val, dict):
                        for sub_key, sub_val in val.items():
                            rows.append({
                                "category": category,
                                "key": f"{key}.{sub_key}",
                                "value": str(sub_val),
                            })
                    elif isinstance(val, list):
                        for i, item in enumerate(val):
                            rows.append({
                                "category": category,
                                "key": f"{key}[{i}]",
                                "value": str(item),
                            })
                    else:
                        rows.append({
                            "category": category,
                            "key": key,
                            "value": str(val),
                        })

        if rows:
            df = pd.DataFrame(rows)
            report_path = self.output_dir / "diagnostic_report.csv"
            df.to_csv(report_path, index=False)
            print(f"\n   💾 Full report saved → {report_path}")

    # ------------------------------------------------------------------
    # Utility helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _values_match(current, expected) -> bool:
        """Loose comparison between config values."""
        if current == expected:
            return True
        # String comparison (case-insensitive)
        if isinstance(current, str) and isinstance(expected, str):
            return current.lower().strip() == expected.lower().strip()
        # Numeric tolerance
        try:
            return abs(float(current) - float(expected)) < 1e-9
        except (ValueError, TypeError):
            return str(current).lower() == str(expected).lower()


# ---------------------------------------------------------------------------
# Convenience wrapper
# ---------------------------------------------------------------------------

def run_diagnostics(
    best_recipe: str,
    start_time: str,
    start_lat: float,
    start_lon: float,
    diagnostics_label: str = "Phase 3 Enhancement – Pre-Flight Diagnostics",
) -> dict:
    """
    Entry-point wrapper called from __main__.py before weathering runs.

    Returns the diagnostic report dict.
    """
    forcing = get_forcing_files(best_recipe)
    currents_file = str(forcing["currents"])
    winds_file = str(forcing["wind"])

    service = DiagnosticsService(
        currents_file=currents_file,
        winds_file=winds_file,
        spill_lat=start_lat,
        spill_lon=start_lon,
        start_time=start_time,
        diagnostics_label=diagnostics_label,
    )
    if diagnostics_label == "Phase 3 Enhancement – Pre-Flight Diagnostics":
        return service.run_all()

    print("\n" + "=" * 60)
    print(service.diagnostics_label)
    print("=" * 60)
    service._check_environmental_forcing()
    service._check_openoil_config()
    service._check_gnome_availability()
    service._print_summary()
    service._save_report()
    return service.report
