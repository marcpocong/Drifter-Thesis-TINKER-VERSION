"""
Oil weathering and fate service shared by the legacy Phase 3 path and Phase 4.

Runs OpenOil (OpenDrift) simulations for one or more configured oil scenarios
along the selected transport pathway and writes 72-hour time-series mass budgets:
surface, evaporated, dispersed, and beached.
"""

import logging
import yaml
import pandas as pd
from datetime import timedelta
from pathlib import Path
from typing import Any

from opendrift.models.openoil import OpenOil
from opendrift.readers import reader_netCDF_CF_generic

from src.helpers.metrics import check_mass_balance, extract_mass_budget
from src.utils.io import get_forcing_files

logger = logging.getLogger(__name__)


class OilWeatheringService:
    """
    Runs OpenOil weathering simulations for Phase 3 and the standardized Phase 4.

    Model settings (per the paper's Phase 3 fate-assessment specification):

    +-----------------------+----------------------------------------------+
    | Forcing / Process     | Setting                                      |
    +-----------------------+----------------------------------------------+
    | Environmental Data    | Local values at particle position from the   |
    |                       | Phase 1 winning met-ocean products.           |
    | Vertical Mixing       | **OFF** – keeps transport on the surface,     |
    |                       | consistent with the drifter validation.       |
    | Weathering Modules    | **ON** – evaporation, natural dispersion,     |
    |                       | emulsification, and spreading.               |
    +-----------------------+----------------------------------------------+

    For each configured oil scenario, it:
      1. Seeds N Lagrangian particles (N from config; default 10,000) using the validated best-recipe forcing.
      2. Runs for 72 hours using OpenDrift’s OpenOil physical weathering model.
      3. Extracts a time-series mass budget from the NetCDF output:
           - surface_mass  : oil remaining on the sea surface (tonnes)
           - evaporated    : mass lost to atmosphere   (tonnes)
           - dispersed     : mass mixed into water column (tonnes)
           - beached       : mass stranded on shoreline (tonnes)
      4. Saves budget to CSV and triggers mass-budget chart generation.
    """

    def __init__(
        self,
        currents_file: str,
        winds_file: str,
        *,
        wave_file: str | Path | None = None,
        output_dir: str | Path | None = None,
        config_path: str | Path = "config/oil.yaml",
        scenarios: dict[str, dict[str, Any]] | None = None,
        phase_label: str = "Phase 3",
        refined_stage_label: str = "Stage 3b",
    ):
        from src.core.constants import BASE_OUTPUT_DIR

        self.output_dir = Path(output_dir) if output_dir else BASE_OUTPUT_DIR / "weathering"
        self.output_dir.mkdir(parents=True, exist_ok=True)

        self.currents_file = currents_file
        self.winds_file = winds_file
        self.wave_file = Path(wave_file) if wave_file else None
        self.phase_label = str(phase_label)
        self.refined_stage_label = str(refined_stage_label)

        config_path = Path(config_path)
        if not config_path.exists():
            raise FileNotFoundError(f"Oil weathering config required at {config_path}")

        with open(config_path, "r") as f:
            self.config = yaml.safe_load(f)

        self.sim_cfg = self.config["simulation"]
        self.oils_cfg = scenarios or self.config["oils"]

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run_all(self, start_lat: float, start_lon: float, start_time: str) -> dict:
        """
        Run weathering simulations for every oil type in the config.

        Returns
        -------
        dict
            Keyed by oil key ('light', 'heavy'), each containing the mass
            budget DataFrame and output paths.
        """
        results = {}
        for oil_key, oil_cfg in self.oils_cfg.items():
            logger.info(f"\n{'='*60}")
            logger.info(f"{self.phase_label} | Oil Type: {oil_cfg['display_name']}")
            logger.info(f"{'='*60}")
            print(f"\nRunning {self.phase_label} weathering: {oil_cfg['display_name']}")
            budget_df, nc_path = self._run_single(
                oil_key=oil_key,
                oil_cfg=oil_cfg,
                start_lat=start_lat,
                start_lon=start_lon,
                start_time=start_time,
            )
            # Mass-balance QC check
            tolerance = self.sim_cfg.get("mass_balance_tolerance_pct", 2.0)
            qc = check_mass_balance(budget_df, tolerance)

            results[oil_key] = {
                "display_name": oil_cfg["display_name"],
                "budget_df": budget_df,
                "nc_path": nc_path,
                "csv_path": self.output_dir / f"budget_{oil_key}.csv",
                "qc": qc,
            }

        # --- Generate plots ---
        self._generate_plots(results)

        return results

    def run_refined_oil(
        self, start_lat: float, start_lon: float, start_time: str
    ) -> dict | None:
        """
        Stage 3b: Re-run with an ADIOS-confirmed specific oil type.

        Reads the ``refined_oil`` section from config/oil.yaml.  Returns
        ``None`` when disabled or when no ``adios_id`` is specified.
        """
        if self.refined_stage_label != "Stage 3b":
            refined_cfg = self.config.get("refined_oil", {})
            if not refined_cfg.get("enabled", False):
                return None

            adios_id = refined_cfg.get("adios_id", "")
            if not adios_id:
                logger.warning("%s enabled but no adios_id specified - skipping.", self.refined_stage_label)
                return None

            oil_cfg = {
                "adios_id": adios_id,
                "display_name": refined_cfg.get("display_name", "Refined Oil"),
                "color": refined_cfg.get("color", "#228B22"),
                "openoil_overrides": {
                    "drift:vertical_mixing": False,
                    "processes:dispersion": True,
                    "processes:evaporation": True,
                    "processes:emulsification": True,
                    "processes:update_oilfilm_thickness": True,
                    "general:coastline_action": "stranding",
                    "wave_entrainment:droplet_size_distribution": "Johansen2015",
                },
            }

            print(f"\n{self.refined_stage_label}: Running refined oil simulation ({adios_id})")
            budget_df, nc_path = self._run_single(
                oil_key="refined",
                oil_cfg=oil_cfg,
                start_lat=start_lat,
                start_lon=start_lon,
                start_time=start_time,
            )

            tolerance = self.sim_cfg.get("mass_balance_tolerance_pct", 2.0)
            qc = check_mass_balance(budget_df, tolerance)

            return {
                "display_name": oil_cfg["display_name"],
                "budget_df": budget_df,
                "nc_path": nc_path,
                "csv_path": self.output_dir / "budget_refined.csv",
                "qc": qc,
            }

        refined_cfg = self.config.get("refined_oil", {})
        if not refined_cfg.get("enabled", False):
            return None

        adios_id = refined_cfg.get("adios_id", "")
        if not adios_id:
            logger.warning("Stage 3b enabled but no adios_id specified – skipping.")
            return None

        oil_cfg = {
            "adios_id": adios_id,
            "display_name": refined_cfg.get("display_name", "Refined Oil"),
            "color": refined_cfg.get("color", "#228B22"),
            "openoil_overrides": {
                "drift:vertical_mixing": False,
                "processes:dispersion": True,
                "processes:evaporation": True,
                "processes:emulsification": True,
                "processes:update_oilfilm_thickness": True,
                "general:coastline_action": "stranding",
                "wave_entrainment:droplet_size_distribution": "Johansen2015",
            },
        }

        print(f"\n🛢️  Stage 3b: Running refined oil simulation ({adios_id})")
        budget_df, nc_path = self._run_single(
            oil_key="refined",
            oil_cfg=oil_cfg,
            start_lat=start_lat,
            start_lon=start_lon,
            start_time=start_time,
        )

        tolerance = self.sim_cfg.get("mass_balance_tolerance_pct", 2.0)
        qc = check_mass_balance(budget_df, tolerance)

        return {
            "display_name": oil_cfg["display_name"],
            "budget_df": budget_df,
            "nc_path": nc_path,
            "csv_path": self.output_dir / "budget_refined.csv",
            "qc": qc,
        }

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _run_single(
        self,
        oil_key: str,
        oil_cfg: dict,
        start_lat: float,
        start_lon: float,
        start_time: str,
    ):
        """Run one OpenOil simulation and return (budget_df, nc_path)."""

        adios_id = oil_cfg["adios_id"]
        num_particles = self.sim_cfg["num_particles"]
        duration_hours = self.sim_cfg["duration_hours"]
        time_step_minutes = self.sim_cfg["time_step_minutes"]
        initial_mass_tonnes = self.sim_cfg["initial_mass_tonnes"]
        self.sim_cfg["radius_m"]

        # Derive mass per particle (kg, since OpenDrift uses SI)
        total_mass_kg = initial_mass_tonnes * 1000.0
        total_mass_kg / num_particles

        # OpenOil's seed_elements() *always* overwrites mass_oil from:
        #   mass_oil = m3_per_hour × duration_hours / num × density
        # For an instantaneous release duration_hours=1, so we need:
        #   m3_per_hour = total_mass_kg / density_kg_m3
        # to achieve the desired M₀.  Density comes from the config
        # (approximate ADIOS value at 285 K); OpenOil will internally
        # recompute the exact density and the two should be very close.
        approx_density = oil_cfg.get("density_kg_m3", 900)
        m3_per_hour = total_mass_kg / approx_density

        # ── 1. Model Setup ──────────────────────────────────────────────
        o = OpenOil(loglevel=50, weathering_model="noaa")

        # Readers
        reader_curr = reader_netCDF_CF_generic.Reader(self.currents_file)
        o.add_reader(reader_curr)

        reader_wind = reader_netCDF_CF_generic.Reader(self.winds_file)
        o.add_reader(reader_wind)

        try:
            wave_path = self.wave_file
            if wave_path is None:
                from src.core.constants import RUN_NAME

                wave_path = Path(f"data/forcing/{RUN_NAME}/cmems_wave.nc")
            reader_wave = reader_netCDF_CF_generic.Reader(str(wave_path))
            o.add_reader(reader_wave)
        except Exception as e:
            pass

        # Minimal landmask (use auto internally via OpenOil's stranding)
        o.set_config("general:use_auto_landmask", True)

        # ── 1b. Apply OpenOil Config Overrides ──────────────────────
        # Per the paper’s Phase 3 specification:
        #   • Vertical mixing  = OFF  (surface-only, consistent with drifter validation)
        #   • Weathering       = ON   (evaporation, dispersion, emulsification, spreading)
        # Oil-specific overrides from config/oil.yaml also set droplet-size
        # distributions that prevent unrealistic heavy-oil dispersion.
        openoil_overrides = oil_cfg.get("openoil_overrides", {})
        if openoil_overrides:
            logger.info(f"Applying {len(openoil_overrides)} OpenOil config overrides for {oil_key}")
            for cfg_key, cfg_val in openoil_overrides.items():
                try:
                    o.set_config(cfg_key, cfg_val)
                    logger.debug(f"  set_config('{cfg_key}', {cfg_val})")
                except Exception as e:
                    logger.warning(f"  Could not set '{cfg_key}': {e}")
        else:
            # Fallback: apply safe defaults for critical parameters.
            # Vertical mixing is OFF to keep transport consistent with the
            # Phase 1 surface-drifter validation; weathering modules stay ON.
            logger.info("No openoil_overrides in config – applying safe defaults")
            safe_defaults = {
                "drift:vertical_mixing": False,
                "drift:wind_uncertainty": 0.0,
                "drift:current_uncertainty": 0.0,
                "wave_entrainment:droplet_size_distribution": "Johansen et al. (2015)",
                "processes:dispersion": True,
                "processes:evaporation": True,
                "processes:emulsification": True,
                "processes:update_oilfilm_thickness": True,   # spreading
            }
            for cfg_key, cfg_val in safe_defaults.items():
                try:
                    o.set_config(cfg_key, cfg_val)
                except Exception as e:
                    logger.debug(f"  Could not set default '{cfg_key}': {e}")

        # ── 1c. Verify critical overrides took effect ─────────────────
        critical_checks = {
            "drift:vertical_mixing": False,
            "processes:evaporation": True,
            "processes:update_oilfilm_thickness": True,
        }
        for ck_key, ck_expected in critical_checks.items():
            try:
                actual = o.get_config(ck_key)
                if actual != ck_expected:
                    print(f"   ⚠️  Override NOT applied: {ck_key} = {actual} (expected {ck_expected})")
            except Exception:
                pass

        # ── 2. Seed ─────────────────────────────────────────────────────
        # NOTE: OpenOil always computes mass_oil from m3_per_hour;
        # passing mass_oil directly is overwritten.  We therefore
        # supply the volume-rate equivalent of the desired M₀.
        from src.services.ensemble import normalize_model_timestamp
        from src.core.case_context import get_case_context
        from src.utils.io import resolve_polygon_seeding

        release_time = normalize_model_timestamp(start_time)
        case = get_case_context()
        if case.workflow_mode == "prototype_2016":
            logger.info(
                "Seeding legacy prototype weathering from the drifter-of-record point at (%.4f, %.4f).",
                float(start_lat),
                float(start_lon),
            )
            o.seed_elements(
                lon=float(start_lon),
                lat=float(start_lat),
                number=num_particles,
                time=release_time,
                oil_type=adios_id,
                m3_per_hour=m3_per_hour,
            )
        else:
            lons, lats, _ = resolve_polygon_seeding(num_particles)
            logger.info("Seeding elements across Official Layer 3 target.")
            o.seed_elements(
                lon=lons,
                lat=lats,
                number=num_particles,
                time=release_time,
                oil_type=adios_id,
                m3_per_hour=m3_per_hour,
            )

        # Verify the actual seeded mass
        try:
            sched_mass = o.elements_scheduled.mass_oil
            actual_mass_per_p = float(sched_mass[0]) if hasattr(sched_mass, '__len__') else float(sched_mass)
            actual_total_kg = actual_mass_per_p * num_particles
            print(f"   ⚖️  Seeded mass: {actual_total_kg/1000:.2f} t "
                  f"({actual_mass_per_p:.3f} kg/particle × {num_particles})")
        except Exception as e:
            logger.debug(f"Could not verify seeded mass: {e}")

        # ── 3. Run ──────────────────────────────────────────────────────
        nc_path = self.output_dir / f"openoil_{oil_key}.nc"
        temp_nc_path = Path(f"/tmp/openoil_{oil_key}.nc")
        
        if nc_path.exists():
            nc_path.unlink()
        if temp_nc_path.exists():
            temp_nc_path.unlink()

        from src.services.ensemble import intercept_sys_exit

        with intercept_sys_exit():
            o.run(
                duration=timedelta(hours=duration_hours),
                time_step=timedelta(minutes=time_step_minutes),
                time_step_output=timedelta(hours=1),   # hourly budget snapshots
                outfile=str(temp_nc_path),
            )

        import shutil
        shutil.move(str(temp_nc_path), str(nc_path))

        print(f"   ✅ OpenOil run complete → {nc_path.name}")

        # ── 4. Extract Budget ────────────────────────────────────────────
        budget_df = extract_mass_budget(nc_path, initial_mass_tonnes)

        csv_path = self.output_dir / f"budget_{oil_key}.csv"
        budget_df.to_csv(csv_path, index=False)
        print(f"   📊 Mass budget saved → {csv_path.name}")

        return budget_df, nc_path

    # ------------------------------------------------------------------

    def _generate_plots(self, results: dict):
        """Delegate to plotting helper for individual and comparison charts."""
        from src.helpers.plotting import plot_mass_budget_chart, plot_mass_budget_comparison

        for oil_key, res in results.items():
            chart_path = self.output_dir / f"mass_budget_{oil_key}.png"
            plot_mass_budget_chart(
                budget_df=res["budget_df"],
                output_file=str(chart_path),
                title=f"{self.phase_label} - Mass Budget: {res['display_name']}",
                color=self.oils_cfg[oil_key]["color"],
            )
            print(f"   📈 Mass budget chart → {chart_path.name}")

        # Side-by-side comparison plot
        if len(results) >= 2:
            comparison_path = self.output_dir / "mass_budget_comparison.png"
            plot_mass_budget_comparison(
                results=results,
                output_file=str(comparison_path),
                title=f"{self.phase_label} – Oil Type Comparison: Mass Budget Over 72 Hours",
            )
            print(f"   📈 Comparison chart   → {comparison_path.name}")


# ---------------------------------------------------------------------------
# Convenience wrapper (mirrors run_ensemble pattern)
# ---------------------------------------------------------------------------

def _resolve_forcing_paths(
    best_recipe: str,
    forcing_paths: dict[str, str | Path] | None = None,
) -> tuple[str, str, str | None]:
    """Resolve forcing files for a recipe key or explicit caller-supplied paths."""
    if forcing_paths:
        wave_path = forcing_paths.get("wave")
        return (
            str(forcing_paths["currents"]),
            str(forcing_paths["wind"]),
            str(wave_path) if wave_path else None,
        )

    forcing = get_forcing_files(best_recipe)
    return (
        str(forcing["currents"]),
        str(forcing["wind"]),
        str(forcing["wave"]) if forcing.get("wave") else None,
    )


def run_weathering(
    best_recipe: str,
    start_time: str,
    start_lat: float,
    start_lon: float,
    *,
    phase_label: str = "Phase 3",
) -> dict:
    """
    Entry-point wrapper called from __main__.py.

    Parameters
    ----------
    best_recipe : str
        Winning or selected recipe key from Phase 1/official baseline selection (e.g. 'cmems_era5').
    start_time : str
        ISO-format simulation start time.
    start_lat, start_lon : float
        Spill origin coordinates.

    Returns
    -------
    dict  – keyed by oil type, each containing budget_df + paths.
    """
    currents_file, winds_file, wave_file = _resolve_forcing_paths(best_recipe)

    service = OilWeatheringService(
        currents_file,
        winds_file,
        wave_file=wave_file,
        phase_label=phase_label,
    )
    return service.run_all(
        start_lat=start_lat,
        start_lon=start_lon,
        start_time=start_time,
    )


def run_weathering_scenarios(
    best_recipe: str,
    start_time: str,
    start_lat: float,
    start_lon: float,
    *,
    scenarios: dict[str, dict[str, Any]],
    output_dir: str | Path,
    phase_label: str = "Phase 4",
    forcing_paths: dict[str, str | Path] | None = None,
) -> dict:
    """Run a caller-supplied scenario registry without changing the legacy Phase 3 path."""
    currents_file, winds_file, wave_file = _resolve_forcing_paths(best_recipe, forcing_paths=forcing_paths)
    service = OilWeatheringService(
        currents_file,
        winds_file,
        wave_file=wave_file,
        output_dir=output_dir,
        scenarios=scenarios,
        phase_label=phase_label,
    )
    return service.run_all(
        start_lat=start_lat,
        start_lon=start_lon,
        start_time=start_time,
    )


def run_refined_weathering(
    best_recipe: str,
    start_time: str,
    start_lat: float,
    start_lon: float,
    *,
    phase_label: str = "Phase 3",
    refined_stage_label: str = "Stage 3b",
) -> dict | None:
    """
    Stage 3b entry point: re-run with ADIOS-confirmed oil.

    Returns None if Stage 3b is not enabled in config/oil.yaml.
    """
    try:
        currents_file, winds_file, wave_file = _resolve_forcing_paths(best_recipe)
    except Exception:
        return None

    service = OilWeatheringService(
        currents_file,
        winds_file,
        wave_file=wave_file,
        phase_label=phase_label,
        refined_stage_label=refined_stage_label,
    )
    return service.run_refined_oil(
        start_lat=start_lat,
        start_lon=start_lon,
        start_time=start_time,
    )
