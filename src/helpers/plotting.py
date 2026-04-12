"""
Visualization helpers for plotting trajectories and maps.
"""
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import numpy as np
import pandas as pd
from datetime import datetime

def plot_drifter_track(
    output_path: str,
    domain_bounds: list,
    unique_ids: list,
    df_found: pd.DataFrame,
    get_trajectory_func,
    target_dt: datetime,
    start_date: str,
    end_date: str = None
):
    """
    Plots drifter tracks for the given date range.
    """
    plt.figure(figsize=(12, 10))
    ax = plt.axes(projection=ccrs.PlateCarree())
    
    # Add coastlines and features
    ax.coastlines(resolution='10m')
    ax.add_feature(cfeature.LAND, facecolor='lightgray')

    ax.add_feature(cfeature.OCEAN, facecolor='azure')
    ax.add_feature(cfeature.BORDERS, linestyle=':')
    
    gl = ax.gridlines(draw_labels=True, linewidth=0.5, color='gray', alpha=0.5, linestyle='--')
    gl.top_labels = False
    gl.right_labels = False

    # Set extent with some padding around the active display domain.
    pad = 2
    ax.set_extent([domain_bounds[0]-pad, domain_bounds[1]+pad, domain_bounds[2]-pad, domain_bounds[3]+pad])

    # For each drifter, get full trajectory and plot
    for drifter_id in unique_ids:
        traj_df = get_trajectory_func(drifter_id, target_dt, days=14)
        
        if not traj_df.empty:
            # Plot full trajectory found
            ax.plot(traj_df['lon'], traj_df['lat'], '-', linewidth=2, label=f"Drifter {drifter_id} (Track)", transform=ccrs.PlateCarree())
            
            # Plot Start/End of trajectory
            ax.scatter(traj_df.iloc[0]['lon'], traj_df.iloc[0]['lat'], c='green', s=50, marker='o', transform=ccrs.PlateCarree(), zorder=5)
            ax.scatter(traj_df.iloc[-1]['lon'], traj_df.iloc[-1]['lat'], c='black', s=50, marker='x', transform=ccrs.PlateCarree(), zorder=5)

        # Highlight points found in the specific search window
        subset = df_found[df_found['ID'] == drifter_id]
        ax.scatter(subset['lon'], subset['lat'], c='red', s=80, marker='*', label=f"Observed in window", zorder=10, transform=ccrs.PlateCarree())

    plt.legend()
    period_str = f"{start_date} to {end_date}" if end_date else start_date
    plt.title(f"Drifter Tracking: {period_str}")
    
    plt.savefig(output_path, dpi=300, bbox_inches='tight')

def plot_trajectory_map(output_file: str, 
                       sim_lon: np.ndarray, 
                       sim_lat: np.ndarray, 
                       obs_lon: np.ndarray = None, 
                       obs_lat: np.ndarray = None, 
                       obs_ids: np.ndarray = None,
                       corners: list = None,
                       title: str = "Trajectory validation"):
    """
    Generates an HD map comparing simulated and observed trajectories
    using High-Resolution GSHHG coastlines.
    
    Args:
        output_file: Path to save the PNG
        sim_lon, sim_lat: Arrays of simulated coordinates
        obs_lon, obs_lat: Arrays of observed coordinates (optional)
        obs_ids: Array of drifter IDs corresponding to observations (optional)
        corners: [lon_min, lon_max, lat_min, lat_max] for map extent
        title: Plot title
    """
    # Create HD figure
    # Increased height slightly to accommodate subtitle/legend if needed
    plt.figure(figsize=(12, 11), dpi=300)
    
    # Setup map projection (PlateCarree for standard lat/lon)
    ax = plt.axes(projection=ccrs.PlateCarree())
    
    # Add High-Resolution Coastlines (GSHHG)
    # scale='h' is high resolution. 'f' is full but can be very slow/large. 'i' is intermediate.
    ax.coastlines(resolution='10m')
    ax.add_feature(cfeature.LAND, facecolor='lightgray')

    # Add Ocean/Borders features
    ax.add_feature(cfeature.BORDERS, linestyle=':', alpha=0.5)
    ax.add_feature(cfeature.OCEAN, facecolor='azure')

    # Gridlines
    gl = ax.gridlines(draw_labels=True, linewidth=0.5, color='gray', alpha=0.5, linestyle='--')
    gl.top_labels = False
    gl.right_labels = False

    # Plot Observed Drifter (if provided)
    if obs_lon is not None and obs_lat is not None:
        if obs_ids is not None:
            # Group by ID to prevent "spiderweb" lines connecting distinct drifters
            unique_ids = np.unique(obs_ids)
            for i, drifter_id in enumerate(unique_ids):
                # Mask for current drifter
                mask = (obs_ids == drifter_id)
                curr_lon = obs_lon[mask]
                curr_lat = obs_lat[mask]
                
                # Only label the first one to avoid legend spam
                lbl = 'Actual Drifter' if i == 0 else None
                
                ax.plot(curr_lon, curr_lat, 'k-', transform=ccrs.PlateCarree(), 
                        linewidth=2.5, label=lbl, zorder=10)
                
                # Start/End markers for each segment
                ax.scatter(curr_lon[0], curr_lat[0], c='green', s=60, marker='o', 
                           edgecolors='black', zorder=11, transform=ccrs.PlateCarree(),
                           label='Start' if i == 0 else None)
                ax.scatter(curr_lon[-1], curr_lat[-1], c='black', s=60, marker='x', 
                           zorder=11, transform=ccrs.PlateCarree(),
                           label=None) # Only label start
        else:
            # Fallback for single track
            ax.plot(obs_lon, obs_lat, 'k-', transform=ccrs.PlateCarree(), 
                    linewidth=2.5, label='Actual Drifter', zorder=10)
            # Start/End markers
            ax.scatter(obs_lon[0], obs_lat[0], c='green', s=100, marker='o', 
                       edgecolors='black', label='Start', zorder=11, transform=ccrs.PlateCarree())
            ax.scatter(obs_lon[-1], obs_lat[-1], c='black', s=100, marker='x', 
                       zorder=11, transform=ccrs.PlateCarree())

    # Plot Simulated Trajectory
    ax.plot(sim_lon, sim_lat, 'r--', transform=ccrs.PlateCarree(), 
            linewidth=2, label='Model Prediction', zorder=9)
    ax.scatter(sim_lon[-1], sim_lat[-1], c='red', s=80, marker='x', 
               zorder=11, transform=ccrs.PlateCarree())

    # Calculate Data Bounds with Padding
    data_extent = None
    all_lons = []
    all_lats = []
    if sim_lon is not None: all_lons.append(sim_lon); all_lats.append(sim_lat)
    if obs_lon is not None: all_lons.append(obs_lon); all_lats.append(obs_lat)
    
    if all_lons:
        cat_lons = np.concatenate(all_lons)
        cat_lats = np.concatenate(all_lats)
        min_lon, max_lon = np.min(cat_lons), np.max(cat_lons)
        min_lat, max_lat = np.min(cat_lats), np.max(cat_lats)
        
        # Add padding
        lon_span = max_lon - min_lon
        lat_span = max_lat - min_lat
        pad_lon = max(0.2, lon_span * 0.2)
        pad_lat = max(0.2, lat_span * 0.2)
        data_extent = [min_lon - pad_lon, max_lon + pad_lon, min_lat - pad_lat, max_lat + pad_lat]

    # Set Main Map Extent
    if corners:
        ax.set_extent(corners, crs=ccrs.PlateCarree())
        
        # --- Inset Map logic: if data is small relative to the display domain, zoom in ---
        if data_extent:
            map_span_lon = corners[1] - corners[0]
            data_span_lon = data_extent[1] - data_extent[0]
            
            # If trajectory covers less than 40% of the map, create an inset
            if data_span_lon < (map_span_lon * 0.4):
                # Create inset axis (Bottom Right usually empty in ocean maps)
                axins = ax.inset_axes([0.6, 0.6, 0.35, 0.35], projection=ccrs.PlateCarree())
                
                # Add basic features to inset
                axins.coastlines()
                axins.add_feature(cfeature.LAND, facecolor='lightgray')
                axins.add_feature(cfeature.OCEAN, facecolor='azure')
                
                # Plot lines on inset
                axins.plot(sim_lon, sim_lat, 'r--', linewidth=2, transform=ccrs.PlateCarree())
                if obs_lon is not None:
                     axins.plot(obs_lon, obs_lat, 'k-', linewidth=2, transform=ccrs.PlateCarree())
                
                # Set inset extent
                axins.set_extent(data_extent, crs=ccrs.PlateCarree())
                
                # Add framing
                ax.indicate_inset_zoom(axins, edgecolor="black")
                
    elif data_extent:
        # Dynamic Auto-Zoom Fallback checks
        ax.set_extent(data_extent, crs=ccrs.PlateCarree())

    # Labels and Legend
    plt.title(title, fontsize=14, pad=20)
    
    # Move legend outside to prevent blocking the map
    plt.legend(loc='upper center', bbox_to_anchor=(0.5, -0.05),
              fancybox=True, shadow=True, ncol=3)
    
    # Save
    plt.savefig(output_file, bbox_inches='tight', dpi=300)
    plt.close()


# ===========================================================================
# Phase 3 – Mass Budget Charts
# ===========================================================================

def plot_mass_budget_chart(
    budget_df,
    output_file: str,
    title: str = "Oil Mass Budget (72 h)",
    color: str = "#FF8C00",
):
    """
    Stacked area / line chart showing the 72-hour mass budget for one oil type.

    Expects budget_df columns:
        hours_elapsed, surface_pct, evaporated_pct, dispersed_pct, beached_pct
    """
    hours = budget_df["hours_elapsed"].values
    surface    = budget_df["surface_pct"].values
    evaporated = budget_df["evaporated_pct"].values
    dispersed  = budget_df["dispersed_pct"].values
    beached    = budget_df["beached_pct"].values

    fig, (ax_stacked, ax_lines) = plt.subplots(1, 2, figsize=(16, 6), dpi=200)
    fig.suptitle(title, fontsize=14, fontweight="bold", y=1.02)

    # ── Left: Stacked Area ──────────────────────────────────────────────
    ax_stacked.stackplot(
        hours,
        surface, evaporated, dispersed, beached,
        labels=["Surface", "Evaporated", "Dispersed", "Beached"],
        colors=["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728"],
        alpha=0.8,
    )
    ax_stacked.set_xlim(0, hours[-1])
    ax_stacked.set_ylim(0, 100)
    ax_stacked.set_xlabel("Time Elapsed (hours)", fontsize=11)
    ax_stacked.set_ylabel("Mass Fraction (%)", fontsize=11)
    ax_stacked.set_title("Stacked Mass Budget", fontsize=12)
    ax_stacked.legend(loc="upper right", fontsize=9)
    ax_stacked.grid(axis="y", linestyle="--", alpha=0.5)

    # ── Right: Individual Lines ────────────────────────────────────────
    line_styles = [
        ("Surface",    surface,    "#1f77b4", "-",  2.5),
        ("Evaporated", evaporated, "#ff7f0e", "--", 2.0),
        ("Dispersed",  dispersed,  "#2ca02c", "-.", 2.0),
        ("Beached",    beached,    "#d62728", ":",  2.0),
    ]
    for label, data, lc, ls, lw in line_styles:
        ax_lines.plot(hours, data, color=lc, linestyle=ls, linewidth=lw, label=label)

    ax_lines.set_xlim(0, hours[-1])
    ax_lines.set_ylim(0, max(surface.max() * 1.1, 5))
    ax_lines.set_xlabel("Time Elapsed (hours)", fontsize=11)
    ax_lines.set_ylabel("Mass Fraction (%)", fontsize=11)
    ax_lines.set_title("Component Trends", fontsize=12)
    ax_lines.legend(loc="upper right", fontsize=9)
    ax_lines.grid(linestyle="--", alpha=0.4)

    # ── 72 h boundary marker ──────────────────────────────────────────
    for ax in (ax_stacked, ax_lines):
        ax.axvline(x=72, color="gray", linestyle=":", linewidth=1)
        ax.annotate(
            "72 h",
            xy=(72, ax.get_ylim()[1] * 0.95),
            fontsize=8,
            color="gray",
            ha="right",
        )

    plt.tight_layout()
    plt.savefig(output_file, bbox_inches="tight", dpi=200)
    plt.close()


def plot_mass_budget_comparison(results: dict, output_file: str):
    """
    Side-by-side comparison of mass budgets for all oil types.

    Parameters
    ----------
    results : dict
        Keys are oil type labels ('light', 'heavy'), values contain
        'display_name' and 'budget_df'.
    output_file : str
    """
    oil_keys = list(results.keys())
    n = len(oil_keys)

    fig, axes = plt.subplots(
        2, n, figsize=(9 * n, 10), dpi=200,
        gridspec_kw={"hspace": 0.45, "wspace": 0.3},
    )
    if n == 1:
        axes = np.array([[axes[0]], [axes[1]]])

    component_map = {
        "Surface":    ("surface_pct",    "#1f77b4", "-",  2.5),
        "Evaporated": ("evaporated_pct", "#ff7f0e", "--", 2.0),
        "Dispersed":  ("dispersed_pct",  "#2ca02c", "-.", 2.0),
        "Beached":    ("beached_pct",    "#d62728", ":",  2.0),
    }

    for col, oil_key in enumerate(oil_keys):
        df   = results[oil_key]["budget_df"]
        name = results[oil_key]["display_name"]
        hours = df["hours_elapsed"].values

        # Top row: stacked area
        ax_top = axes[0, col]
        arrays      = [df[v].values for _, (v, *_) in component_map.items()]
        labels      = list(component_map.keys())
        colors_fill = [c for _, (_, c, *_) in component_map.items()]
        ax_top.stackplot(hours, *arrays, labels=labels, colors=colors_fill, alpha=0.8)
        ax_top.set_title(f"{name}\n(Stacked Budget)", fontsize=11, fontweight="bold")
        ax_top.set_xlim(0, hours[-1])
        ax_top.set_ylim(0, 100)
        ax_top.set_xlabel("Hours Elapsed")
        ax_top.set_ylabel("Mass %")
        ax_top.legend(loc="upper right", fontsize=8)
        ax_top.grid(axis="y", linestyle="--", alpha=0.4)

        # Bottom row: line trends
        ax_bot = axes[1, col]
        for label, (col_name, lc, ls, lw) in component_map.items():
            ax_bot.plot(hours, df[col_name].values,
                        color=lc, linestyle=ls, linewidth=lw, label=label)
        ax_bot.set_title(f"{name}\n(Component Trends)", fontsize=11, fontweight="bold")
        ax_bot.set_xlim(0, hours[-1])
        ax_bot.set_xlabel("Hours Elapsed")
        ax_bot.set_ylabel("Mass %")
        ax_bot.legend(loc="upper right", fontsize=8)
        ax_bot.grid(linestyle="--", alpha=0.4)

        for ax in (ax_top, ax_bot):
            ax.axvline(x=72, color="gray", linestyle=":", linewidth=1)

    fig.suptitle(
        "Phase 3 – Oil Type Comparison: Mass Budget Over 72 Hours",
        fontsize=15, fontweight="bold", y=1.01,
    )
    plt.savefig(output_file, bbox_inches="tight", dpi=200)
    plt.close()


def plot_gnome_vs_openoil(
    openoil_df,
    gnome_df,
    output_file: str,
    title: str = "Cross-Model Comparison: OpenOil vs PyGNOME",
):
    """
    Overlay comparison of OpenOil and PyGNOME mass budgets for the same scenario.
    """
    components = [
        ("surface_pct",    "Surface"),
        ("evaporated_pct", "Evaporated"),
        ("dispersed_pct",  "Dispersed"),
        ("beached_pct",    "Beached"),
    ]
    colors = ["#1f77b4", "#ff7f0e", "#2ca02c", "#d62728"]

    fig, axes = plt.subplots(2, 2, figsize=(14, 9), dpi=200)
    axes = axes.flatten()

    def _safe_col(df, col):
        return df[col].values if col in df.columns else np.zeros(len(df))

    for ax, (col, label), clr in zip(axes, components, colors):
        min_len = min(len(openoil_df), len(gnome_df))
        oo_h = (openoil_df["hours_elapsed"].values if "hours_elapsed" in openoil_df.columns
                else np.arange(min_len))[:min_len]
        gn_h = (gnome_df["hours_elapsed"].values   if "hours_elapsed" in gnome_df.columns
                else np.arange(min_len))[:min_len]

        ax.plot(oo_h, _safe_col(openoil_df, col)[:min_len],
                color=clr, linewidth=2.5, linestyle="-",  label="OpenOil")
        ax.plot(gn_h, _safe_col(gnome_df,   col)[:min_len],
                color=clr, linewidth=2.0, linestyle="--", label="PyGNOME", alpha=0.8)

        ax.set_title(label, fontsize=12, fontweight="bold")
        ax.set_xlabel("Hours Elapsed", fontsize=10)
        ax.set_ylabel("Mass %", fontsize=10)
        ax.legend(fontsize=9)
        ax.grid(linestyle="--", alpha=0.4)
        ax.set_xlim(left=0)
        ax.set_ylim(bottom=0)

    fig.suptitle(title, fontsize=14, fontweight="bold", y=1.01)
    plt.tight_layout()
    plt.savefig(output_file, bbox_inches="tight", dpi=200)
    plt.close()


def plot_probability_map(output_file: str, 
                        all_lons: np.ndarray = None, 
                        all_lats: np.ndarray = None, 
                        prob_grid: np.ndarray = None,
                        lon_bins: np.ndarray = None,
                        lat_bins: np.ndarray = None,
                        start_lon: float = 0, 
                        start_lat: float = 0,
                        corners: list = None,
                        title: str = "Ensemble Probability Forecast"):
    """
    Generates a 2D histograms probability map for ensemble forecasts.
    Can accept either raw coordinates (all_lons/all_lats) or pre-binned prob_grid.
    """
    if prob_grid is None and (all_lons is None or len(all_lons) == 0):
        print("Warning: No data to plot.")
        return

    # --- 1. PREPARE GRID DATA ---
    if prob_grid is not None:
        # Use provided pre-binned grid
        H = prob_grid.T # Histogram expects [x, y]
        X, Y = np.meshgrid(lon_bins, lat_bins)
    else:
        # Bin raw data (legacy or fallback mode)
        x = np.array(all_lons)
        y = np.array(all_lats)
        
        # Use 100 bins or derive from corners
        if corners:
            # Match GRID_RESOLUTION concept if possible
            bins_x = np.arange(corners[0], corners[1], 0.05)
            bins_y = np.arange(corners[2], corners[3], 0.05)
            H, xedges, yedges = np.histogram2d(x, y, bins=[bins_x, bins_y])
        else:
            H, xedges, yedges = np.histogram2d(x, y, bins=100)
            
        X, Y = np.meshgrid((xedges[:-1]+xedges[1:])/2, (yedges[:-1]+yedges[1:])/2)

    # Calculate cumulative distribution to find percentile thresholds
    H_flat = np.sort(H.flatten())[::-1] 
    H_sum = np.sum(H_flat)
    if H_sum == 0: 
        print("Warning: Probability mass is zero.")
        return
        
    H_cumsum = np.cumsum(H_flat)
    
    # Find thresholds for 50% and 90% mass
    thresh_50 = H_flat[np.searchsorted(H_cumsum, 0.50 * H_sum)]
    thresh_90 = H_flat[np.searchsorted(H_cumsum, 0.90 * H_sum)]
    
    # --- 2. MAP SETUP (Matches plot_trajectory_map) ---
    plt.figure(figsize=(12, 11), dpi=300)
    ax = plt.axes(projection=ccrs.PlateCarree())

    ax.coastlines(resolution='10m')
    ax.add_feature(cfeature.LAND, facecolor='lightgray')

    ax.add_feature(cfeature.BORDERS, linestyle=':', alpha=0.5)
    ax.add_feature(cfeature.OCEAN, facecolor='azure')
    
    gl = ax.gridlines(draw_labels=True, linewidth=0.5, color='gray', alpha=0.5, linestyle='--')
    gl.top_labels = False
    gl.right_labels = False

    # Calculate Data Bounds for Zoom
    if prob_grid is not None:
        valid_indices = np.where(prob_grid > 0)
        if len(valid_indices[0]) > 0:
            min_lon, max_lon = lon_bins[valid_indices[1]].min(), lon_bins[valid_indices[1]].max()
            min_lat, max_lat = lat_bins[valid_indices[0]].min(), lat_bins[valid_indices[0]].max()
        else:
            min_lon, max_lon, min_lat, max_lat = corners if corners else (0, 1, 0, 1)
    else:
        min_lon, max_lon = x.min(), x.max()
        min_lat, max_lat = y.min(), y.max()
    
    # Add significant padding (1.0 degree)
    pad_lon = max(1.0, (max_lon - min_lon) * 0.5)
    pad_lat = max(1.0, (max_lat - min_lat) * 0.5)
    pad_lat = max(1.0, (max_lat - min_lat) * 0.5)
    
    zoom_extent = [
        min_lon - pad_lon, 
        max_lon + pad_lon, 
        min_lat - pad_lat, 
        max_lat + pad_lat
    ]
    
    # Ensure we don't zoom out BEYOND the fixed corners if they exist
    if corners:
        final_extent = [
            max(zoom_extent[0], corners[0]),
            min(zoom_extent[1], corners[1]),
            max(zoom_extent[2], corners[2]),
            min(zoom_extent[3], corners[3])
        ]
        ax.set_extent(final_extent, crs=ccrs.PlateCarree())
    else:
        ax.set_extent(zoom_extent, crs=ccrs.PlateCarree())

    # --- 3. PLOTTING ---
    # Transposing H with H.T is necessary because np.histogram2d returns H[x, y]
    # while contourf expects grid defined as meshgrid(X, Y), where X varies with columns, Y with rows.
    
    h_max = H.max()
    
    if h_max > thresh_90:
        # 90% Contour (Possible) - Yellow
        ax.contourf(X, Y, H.T, levels=[thresh_90, h_max + 1e-9], colors=['#FFD700'], alpha=0.3, transform=ccrs.PlateCarree())
        ax.contour(X, Y, H.T, levels=[thresh_90], colors=['#DAA520'], linewidths=1, linestyles='--', transform=ccrs.PlateCarree())
    
    if h_max > thresh_50:
        # 50% Contour (Likely) - Red
        ax.contourf(X, Y, H.T, levels=[thresh_50, h_max + 1e-9], colors=['#FF4500'], alpha=0.4, transform=ccrs.PlateCarree())
        ax.contour(X, Y, H.T, levels=[thresh_50], colors=['#8B0000'], linewidths=2, transform=ccrs.PlateCarree())
    
    # Plot Start
    ax.plot(start_lon, start_lat, marker='*', color='green', markersize=15, markeredgecolor='black', 
            label='Spill Origin', zorder=10, transform=ccrs.PlateCarree())

    # --- 4. INSET LOGIC ---
    # Disabled for now as the main map is now auto-zoomed to a comfortable level
    """
    # Data bounds
    min_lon, max_lon = x.min(), x.max()
    min_lat, max_lat = y.min(), y.max()
    data_extent = [min_lon - 0.2, max_lon + 0.2, min_lat - 0.2, max_lat + 0.2]
    
    if corners:
        map_span_lon = corners[1] - corners[0]
        data_span_lon = data_extent[1] - data_extent[0]
        
        # If trajectory covers less than 40% of the map, create an inset
        if data_span_lon < (map_span_lon * 0.4):
            axins = ax.inset_axes([0.6, 0.6, 0.35, 0.35], projection=ccrs.PlateCarree())
            axins.coastlines()
            axins.add_feature(cfeature.LAND, facecolor='lightgray')
            axins.add_feature(cfeature.OCEAN, facecolor='azure')
            
            # Re-plot key features on inset
            axins.contourf(X, Y, H.T, levels=[thresh_90, H.max()], colors=['#FFD700'], alpha=0.3, transform=ccrs.PlateCarree())
            axins.contourf(X, Y, H.T, levels=[thresh_50, H.max()], colors=['#FF4500'], alpha=0.4, transform=ccrs.PlateCarree())
            axins.plot(start_lon, start_lat, marker='*', color='green', markersize=12, markeredgecolor='black', transform=ccrs.PlateCarree())
            
            axins.set_extent(data_extent, crs=ccrs.PlateCarree())
            ax.indicate_inset_zoom(axins, edgecolor="black")
    """

    # --- 5. LEGEND & SAVE ---
    from matplotlib.lines import Line2D
    from matplotlib.patches import Patch
    legend_elements = [
        Line2D([0], [0], marker='*', color='w', markerfacecolor='g', markersize=15, label='Spill Origin'),
        Patch(facecolor='#FF4500', alpha=0.4, label='50% Probability (Likely)'),
        Patch(facecolor='#FFD700', alpha=0.3, label='90% Probability (Possible)')
    ]
    
    plt.title(title, fontsize=14, pad=20)
    plt.legend(handles=legend_elements, loc='upper center', bbox_to_anchor=(0.5, -0.05),
                fancybox=True, shadow=True, ncol=3)

    plt.savefig(output_file, bbox_inches='tight', dpi=300)
    plt.close()


# ===========================================================================
# Phase 3 Enhancement – Diagnostic Charts
# ===========================================================================

def plot_diagnostic_forcing(
    diag_report: dict,
    output_file: str = None,
):
    """
    Visual summary of environmental forcing diagnostics.

    Creates a 3-panel figure:
      1. Wind speed gauge (max vs thresholds)
      2. Current speed gauge
      3. Config audit pass/fail summary
    """
    if output_file is None:
        from src.core.constants import BASE_OUTPUT_DIR
        output_file = str(BASE_OUTPUT_DIR / "diagnostics" / "forcing_summary.png")
    
    from pathlib import Path
    Path(output_file).parent.mkdir(parents=True, exist_ok=True)

    fig, axes = plt.subplots(1, 3, figsize=(16, 5), dpi=200)

    # --- Panel 1: Wind Speed ---
    ax = axes[0]
    wind = diag_report.get("environmental_forcing", {}).get("wind", {})
    max_ws = wind.get("max_wind_speed_ms", 0)
    mean_ws = wind.get("mean_wind_speed_ms", 0)

    thresholds = [0, 5, 12, 25, 40]
    colors_bar = ["#2ca02c", "#ff7f0e", "#d62728", "#7f0000"]

    # Background threshold zones
    for i in range(len(thresholds) - 1):
        ax.barh(0, thresholds[i+1] - thresholds[i], left=thresholds[i],
                height=0.6, color=colors_bar[i], alpha=0.3, edgecolor="none")
    # Marker for actual max
    ax.barh(0, max_ws, height=0.3, color="black", alpha=0.9)
    ax.axvline(x=12, color="orange", linestyle="--", linewidth=1.5, label="Moderate threshold")
    ax.axvline(x=25, color="red", linestyle="--", linewidth=1.5, label="Extreme threshold")
    ax.scatter([max_ws], [0], color="red", s=100, zorder=5, marker="|", linewidths=3)
    ax.set_xlim(0, max(40, max_ws * 1.2))
    ax.set_yticks([])
    ax.set_xlabel("Wind Speed (m/s)")
    ax.set_title(f"Max Wind: {max_ws:.1f} m/s\nMean: {mean_ws:.1f} m/s", fontsize=11, fontweight="bold")
    ax.legend(fontsize=8, loc="upper right")

    # --- Panel 2: Current Speed ---
    ax = axes[1]
    curr = diag_report.get("environmental_forcing", {}).get("currents", {})
    max_cs = curr.get("max_current_speed_ms", 0)
    mean_cs = curr.get("mean_current_speed_ms", 0)

    ax.barh(0, max_cs, height=0.3, color="#1f77b4", alpha=0.9)
    ax.axvline(x=0.5, color="orange", linestyle="--", linewidth=1.5, label="Strong current")
    ax.axvline(x=1.5, color="red", linestyle="--", linewidth=1.5, label="Extreme current")
    ax.set_xlim(0, max(2.0, max_cs * 1.2))
    ax.set_yticks([])
    ax.set_xlabel("Current Speed (m/s)")
    ax.set_title(f"Max Current: {max_cs:.3f} m/s\nMean: {mean_cs:.3f} m/s", fontsize=11, fontweight="bold")
    ax.legend(fontsize=8, loc="upper right")

    # --- Panel 3: Config Audit ---
    ax = axes[2]
    config = diag_report.get("openoil_config", {})
    mismatches = config.get("mismatches", [])
    total_checked = len(config.get("current_values", {}))
    passed = total_checked - len(mismatches)

    if total_checked > 0:
        wedges = [passed, len(mismatches)]
        colors_pie = ["#2ca02c", "#d62728"]
        labels_pie = [f"Pass ({passed})", f"Mismatch ({len(mismatches)})"]
        ax.pie(wedges, labels=labels_pie, colors=colors_pie, autopct="%1.0f%%",
               startangle=90, textprops={"fontsize": 10})
        ax.set_title(f"Config Audit: {passed}/{total_checked}", fontsize=11, fontweight="bold")
    else:
        ax.text(0.5, 0.5, "No config data", ha="center", va="center", fontsize=12)
        ax.set_title("Config Audit", fontsize=11, fontweight="bold")

    fig.suptitle(
        "Phase 3 Enhancement – Pre-Flight Diagnostics",
        fontsize=14, fontweight="bold", y=1.02,
    )
    plt.tight_layout()
    plt.savefig(output_file, bbox_inches="tight", dpi=200)
    plt.close()
