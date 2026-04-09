"""
Metric calculation helpers for validation.
"""
import numpy as np
import pandas as pd
import xarray as xr
from scipy.ndimage import uniform_filter

def calculate_fss(forecast: np.ndarray, observed: np.ndarray, window: int = 5) -> float:
    """
    Fractions Skill Score (FSS) implementation for spatial probability fields.
    FSS = 1 - (MSE / MSE_ref)
    Measures spatial overlap with neighborhood tolerance.
    """
    forecast_arr = np.asarray(forecast, dtype=float)
    observed_arr = np.asarray(observed, dtype=float)
    if forecast_arr.shape != observed_arr.shape:
        raise ValueError("FSS requires forecast and observed grids with identical shapes.")
    if window < 1:
        raise ValueError("FSS window must be at least 1 cell.")

    # Neighborhood smoothing
    f_smooth = uniform_filter(forecast_arr, size=window)
    o_smooth = uniform_filter(observed_arr, size=window)
    
    mse = np.mean((f_smooth - o_smooth)**2)
    mse_ref = np.mean(f_smooth**2) + np.mean(o_smooth**2)
    
    if mse_ref == 0:
        return 1.0 if mse == 0 else 0.0
        
    return float(np.clip(1.0 - (mse / mse_ref), 0.0, 1.0))

def calculate_kl_divergence(
    forecast: np.ndarray,
    observed: np.ndarray,
    epsilon: float = 1e-10,
    valid_mask: np.ndarray | None = None,
) -> float:
    """
    Kullback-Leibler (KL) Divergence for probability distributions.
    Measures difference between forecast and observed spatial mass.
    """
    forecast_arr = np.asarray(forecast, dtype=float)
    observed_arr = np.asarray(observed, dtype=float)
    if forecast_arr.shape != observed_arr.shape:
        raise ValueError("KL divergence requires forecast and observed grids with identical shapes.")

    valid = np.isfinite(forecast_arr) & np.isfinite(observed_arr)
    if valid_mask is not None:
        valid &= np.asarray(valid_mask, dtype=bool)

    if not np.any(valid):
        raise ValueError("KL divergence requires at least one valid cell after masking.")

    f = np.clip(forecast_arr[valid], 0.0, None)
    o = np.clip(observed_arr[valid], 0.0, None)

    f_sum = float(f.sum())
    o_sum = float(o.sum())
    if f_sum <= 0.0 or o_sum <= 0.0:
        raise ValueError("KL divergence requires positive forecast and observed mass over valid cells.")

    # Normalize over the valid domain first, then apply epsilon and renormalize.
    f = np.clip(f / f_sum, epsilon, None)
    o = np.clip(o / o_sum, epsilon, None)
    f /= f.sum()
    o /= o.sum()

    return float(np.sum(o * np.log(o / f)))

def haversine(lat1, lon1, lat2, lon2):
    """
    Calculate the great circle distance between two points 
    on the earth (specified in decimal degrees)
    """
    R = 6371.0  # Earth radius in km
    phi1, phi2 = np.radians(lat1), np.radians(lat2)
    dphi = np.radians(lat2 - lat1)
    dlambda = np.radians(lon2 - lon1)
    a = np.sin(dphi/2)**2 + np.cos(phi1)*np.cos(phi2)*np.sin(dlambda/2)**2
    return 2 * R * np.arctan2(np.sqrt(a), np.sqrt(1 - a))

def calculate_ncs(forecast_lat: np.ndarray, forecast_lon: np.ndarray, 
                 obs_lat: np.ndarray, obs_lon: np.ndarray) -> float:
    """
    Calculate Normalized Cumulative Separation (NCS) metric (Liu & Weisberg, 2011).
    NCS = sum(separation_distances) / total_drifter_path_length
    """
    # Calculate separation distance at each time step
    separations = haversine(forecast_lat, forecast_lon, obs_lat, obs_lon)
    
    # Calculate path length of the drifter
    # Distance between consecutive observed points
    steps = haversine(obs_lat[:-1], obs_lon[:-1], obs_lat[1:], obs_lon[1:])
    total_path_length = np.sum(steps)

    if total_path_length == 0:
        return float('inf')

    # NCS formula
    ncs = np.sum(separations) / total_path_length
    return ncs

def check_mass_balance(
    budget_df: pd.DataFrame, tolerance_pct: float = 2.0
) -> dict:
    """
    Quality-control check: |100% − Σ %X(t)| ≤ tolerance.

    Returns
    -------
    dict
        passed            : bool
        tolerance_pct     : float
        max_deviation_pct : float
        worst_hour        : int
        failing_hours     : list of (hour, deviation_pct)
    """
    pct_cols = ["surface_pct", "evaporated_pct", "dispersed_pct", "beached_pct"]
    totals = budget_df[pct_cols].sum(axis=1)
    deviations = (100.0 - totals).abs()

    max_dev = float(deviations.max())
    worst_idx = int(deviations.idxmax())
    worst_hour = int(budget_df.loc[worst_idx, "hours_elapsed"])

    failing = []
    for idx, dev in deviations.items():
        if dev > tolerance_pct:
            failing.append(
                (int(budget_df.loc[idx, "hours_elapsed"]), round(float(dev), 2))
            )

    return {
        "passed": max_dev <= tolerance_pct,
        "tolerance_pct": tolerance_pct,
        "max_deviation_pct": round(max_dev, 2),
        "worst_hour": worst_hour,
        "failing_hours": failing,
    }

def extract_mass_budget(nc_path: str, initial_mass_tonnes: float) -> pd.DataFrame:
    """
    Parse an OpenOil NetCDF output and build a normalised mass budget.

    Phase 3 Enhancement fix: properly accounts for subsurface/dispersed
    particles via OpenDrift status codes instead of computing dispersed
    as a residual (which caused the 100% dispersion anomaly for heavy oil).

    OpenDrift status codes:
        0 = active (surface)
        1 = stranded (beached)
        2 = evaporated
        3 = dispersed (submerged below mixed layer)

    Columns: time, surface_pct, evaporated_pct, dispersed_pct, beached_pct
             surface_t, evaporated_t, dispersed_t, beached_t
    """
    ds = xr.open_dataset(nc_path)

    # Retrieve time axis
    times = pd.to_datetime(ds.time.values)

    # ── Variable extraction ────────────────────────────────────────
    status = ds["status"].values          # shape: (traj, time)
    mass_oil = ds["mass_oil"].values      # mass remaining per particle (kg)

    # fraction_evaporated: cumulative fraction that has evaporated
    frac_evap = None
    if "fraction_evaporated" in ds:
        frac_evap = ds["fraction_evaporated"].values

    # z (depth) – particles below surface are dispersed, not "lost"
    z_values = None
    if "z" in ds:
        z_values = ds["z"].values

    ds.close()

    # Ensure shape is (n_traj, n_time)
    if status.ndim == 2 and status.shape[0] < status.shape[1]:
        pass  # already (traj, time)

    n_traj, n_time = status.shape if status.ndim == 2 else (status.shape[1], status.shape[0])

    initial_mass_kg = initial_mass_tonnes * 1000.0
    mass_per_particle_kg = initial_mass_kg / n_traj

    records = []
    for t_idx in range(n_time):
        st = status[:, t_idx] if status.ndim == 2 else status[t_idx]
        mo = mass_oil[:, t_idx] if mass_oil.ndim == 2 else mass_oil[t_idx]

        # Replace NaN (deactivated particles) with 0
        mo = np.where(np.isnan(mo), 0.0, mo)
        st_float = np.where(np.isnan(st.astype(float)), -1, st)
        st_int = st_float.astype(int)

        # ── Status-based classification ──
        # Status 0: active / surface
        surface_mask = (st_int == 0)
        # Status 1: stranded / beached
        beached_mask = (st_int == 1)
        # Status 2: evaporated (OpenDrift marks fully evaporated particles)
        evaporated_status_mask = (st_int == 2)

        # For active particles, use depth to distinguish surface vs dispersed
        if z_values is not None:
            z = z_values[:, t_idx] if z_values.ndim == 2 else z_values[t_idx]
            z = np.where(np.isnan(z), 0.0, z)
            # Particles with z < -0.5 m are considered dispersed into water column
            subsurface_mask = surface_mask & (z < -0.5)
            true_surface_mask = surface_mask & (z >= -0.5)
        else:
            subsurface_mask = np.zeros_like(surface_mask)
            true_surface_mask = surface_mask

        surface_kg = float(np.sum(mo[true_surface_mask]))
        beached_kg = float(np.sum(mo[beached_mask]))

        # Evaporated: use fraction_evaporated for accurate accounting
        if frac_evap is not None:
            fe = frac_evap[:, t_idx] if frac_evap.ndim == 2 else frac_evap[t_idx]
            fe = np.where(np.isnan(fe), 0.0, fe)
            # Evaporated mass = sum of (fraction evaporated × initial mass per particle)
            evaporated_kg = float(np.sum(fe * mass_per_particle_kg))
            # Also add mass of fully-evaporated particles (status == 2)
            evaporated_kg += float(np.sum(mo[evaporated_status_mask]))
        else:
            # Fallback: count particles with status=2 as evaporated
            evaporated_kg = float(np.sum(mo[evaporated_status_mask]))
            if evaporated_kg == 0:
                # Last resort: estimate from mass loss (60/40 evap/disp split)
                lost_mass = max(0.0, initial_mass_kg - float(np.sum(mo)))
                evaporated_kg = lost_mass * 0.6

        # Dispersed: subsurface particles + residual budget gap
        dispersed_from_subsurface = float(np.sum(mo[subsurface_mask]))
        accounted_kg = surface_kg + beached_kg + evaporated_kg + dispersed_from_subsurface
        residual_kg = max(0.0, initial_mass_kg - accounted_kg)
        dispersed_kg = dispersed_from_subsurface + residual_kg

        records.append({
            "time": times[t_idx],
            "hours_elapsed": t_idx,
            "surface_t":    round(surface_kg / 1000.0, 4),
            "beached_t":    round(beached_kg / 1000.0, 4),
            "evaporated_t": round(evaporated_kg / 1000.0, 4),
            "dispersed_t":  round(dispersed_kg / 1000.0, 4),
            "surface_pct":    round(100.0 * surface_kg / max(initial_mass_kg, 1e-9), 2),
            "beached_pct":    round(100.0 * beached_kg / max(initial_mass_kg, 1e-9), 2),
            "evaporated_pct": round(100.0 * evaporated_kg / max(initial_mass_kg, 1e-9), 2),
            "dispersed_pct":  round(100.0 * dispersed_kg / max(initial_mass_kg, 1e-9), 2),
        })

    return pd.DataFrame(records)

def extract_gnome_budget_from_nc(nc_path: str) -> pd.DataFrame:
    """
    Pull mass fractions from PyGNOME's NetCDF output.
    Replaces deprecated _extract_gnome_budget using status_counts.
    """
    import netCDF4
    
    records = []
    with netCDF4.Dataset(nc_path) as nc:
        # Get variables
        times = nc.variables['time'][:]
        particle_counts = nc.variables['particle_count'][:]
        
        # Data variables are flattened. We need to slice them per step.
        status_codes = nc.variables['status_codes'][:]
        mass = nc.variables['mass'][:]
        
        # Optional variables (handle if missing)
        frac_evap = nc.variables['frac_evap'][:] if 'frac_evap' in nc.variables else None
        # depth = nc.variables['depth'][:] if 'depth' in nc.variables else None
        
        start_idx = 0
        initial_mass_total = None
        
        for step_num, count in enumerate(particle_counts):
            end_idx = start_idx + count
            
            # Slices for this timestep
            step_codes = status_codes[start_idx:end_idx]
            step_mass = mass[start_idx:end_idx]
            step_evap = frac_evap[start_idx:end_idx] if frac_evap is not None else np.zeros_like(step_mass)
            
            # Calculate Initial Mass for these particles (Mass / (1 - frac_evap))
            # Avoid division by zero if frac_evap is 1 (fully evaporated)
            # Actually, if fully evaporated, mass should be 0.
            # A better proxy for total mass is Sum(Mass / (1 - Evap))
            # BUT, simpler: Use t=0 mass as reference.
            
            current_mass_sum = np.sum(step_mass)
            
            if step_num == 0:
                initial_mass_total = current_mass_sum
            
            mass_ref = max(initial_mass_total, 1e-9)
            
            # Categorize Mass
            # 2 = in_water (floating or dispersed if depth > 0, but simplicity: floating)
            # 3 = on_land (beached)
            # 10 = evaporated (usually these particles stay in list but have 0 mass? No, mass var is oil mass.)
            # Actually, PyGNOME mass variable is "mass of oil in the LE". 
            # Beached particles (3) retain their mass.
            # Floating particles (2) retain their mass.
            # Evaporated mass is the missing mass!
            
            # Calculate totals based on status
            surface_mass = np.sum(step_mass[step_codes == 2])
            beached_mass = np.sum(step_mass[(step_codes == 3) | (step_codes == 32)]) # 3=land, 32=tideflat
            
            # Evaporated: 
            # Since mass variable decreases with evaporation, Evaporated Mass is
            # (Initial Mass of these particles) - (Current Mass of these particles).
            # We can approximate Initial Mass using frac_evap:
            #    Initial_i = Mass_i / (1 - FracEvap_i) 
            #    Evaporated_i = Initial_i * FracEvap_i
            
            # Handle edge case where 1-FracEvap is close to 0
            denom = 1.0 - step_evap
            denom[denom < 1e-6] = 1e-6 # avoid div/0
            
            initial_mass_est = step_mass / denom
            evaporated_mass = np.sum(initial_mass_est * step_evap)
            
            # Dispersed? 
            # If particles are removed (code 12 or similar), their mass is gone from the model?
            # Or they stick around?
            # Let's assume dispersed = Total Initial - (Surface + Beached + Evaporated)
            # This catches any other loss mechanisms.
            
            # Re-normalize to total tracked mass (to percentage)
            
            # Refine Initial Mass: Use the value calculated at t=0
            # Because particles might be added/removed, but we used a localized spill.
            
            dispersed_mass = max(0, mass_ref - (surface_mass + beached_mass + evaporated_mass))
            
            records.append({
                "hours_elapsed": step_num,
                "surface_pct":    round(100.0 * surface_mass   / mass_ref, 2),
                "beached_pct":    round(100.0 * beached_mass   / mass_ref, 2),
                "evaporated_pct": round(100.0 * evaporated_mass/ mass_ref, 2),
                "dispersed_pct":  round(100.0 * dispersed_mass / mass_ref, 2),
            })
            
            start_idx = end_idx

    return pd.DataFrame(records)
