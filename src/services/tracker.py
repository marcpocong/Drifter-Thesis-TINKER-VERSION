import logging
import pandas as pd
from datetime import datetime, timedelta
from erddapy import ERDDAP

from src.core.constants import REGION

logger = logging.getLogger(__name__)

class DrifterTracker:
    def __init__(self, domain_bounds=REGION):
        self.domain_bounds = domain_bounds
        self.region = domain_bounds

    def find_drifters(self, start_date: str, end_date: str = None) -> pd.DataFrame:
        """
        Check if there are any drifters in the active domain within the specified date range.
        
        Args:
            start_date (str): Start date in 'YYYY-MM-DD' format.
            end_date (str, optional): End date in 'YYYY-MM-DD' format. If None, defaults to start_date.
            
        Returns:
            pd.DataFrame: DataFrame containing drifter data if found, else empty.
        """
        try:
            start_dt = datetime.strptime(start_date, "%Y-%m-%d")
            end_dt = datetime.strptime(end_date, "%Y-%m-%d") if end_date else start_dt
        except ValueError:
            logger.error("Invalid date format. Use YYYY-MM-DD.")
            return pd.DataFrame()

        start_str = start_dt.strftime("%Y-%m-%dT00:00:00Z")
        end_str = end_dt.strftime("%Y-%m-%dT23:59:59Z")
        
        logger.info(f"Searching for drifters in domain {self.domain_bounds} from {start_date} to {end_date or start_date}...")

        try:
            e = ERDDAP(
                server="https://osmc.noaa.gov/erddap",
                protocol="tabledap",
            )
            e.dataset_id = "drifter_6hour_qc"
            
            e.constraints = {
                "time>=": start_str,
                "time<=": end_str,
                "latitude>=": self.domain_bounds[2],
                "latitude<=": self.domain_bounds[3],
                "longitude>=": self.domain_bounds[0],
                "longitude<=": self.domain_bounds[1],
            }
            e.variables = ["time", "latitude", "longitude", "ID"]
            
            df = e.to_pandas()
            
            if not df.empty:
                # Cleanup column names
                df = df.rename(columns={
                    "latitude (degrees_north)": "lat",
                    "longitude (degrees_east)": "lon",
                    "time (UTC)": "time"
                })
                # Ensure time is datetime
                df['time'] = pd.to_datetime(df['time'])
                return df
            
        except Exception as e:
            logger.error(f"Error querying ERDDAP: {e}")
            
        return pd.DataFrame()

    def get_trajectory(self, drifter_id, center_date: datetime, days=7) -> pd.DataFrame:
        """
        Get the trajectory for a specific drifter around a center date.
        """
        start_date = center_date - timedelta(days=days)
        end_date = center_date + timedelta(days=days)
        
        start_str = start_date.strftime("%Y-%m-%dT00:00:00Z")
        end_str = end_date.strftime("%Y-%m-%dT23:59:59Z")
        
        try:
            e = ERDDAP(
                server="https://osmc.noaa.gov/erddap",
                protocol="tabledap",
            )
            e.dataset_id = "drifter_6hour_qc"
            
            e.constraints = {
                "time>=": start_str,
                "time<=": end_str,
                "ID=": str(drifter_id)
            }
            e.variables = ["time", "latitude", "longitude", "ID"]
            
            df = e.to_pandas()
            if not df.empty:
                # Cleanup column names
                df = df.rename(columns={
                    "latitude (degrees_north)": "lat",
                    "longitude (degrees_east)": "lon",
                    "time (UTC)": "time"
                })
                df['time'] = pd.to_datetime(df['time'])
                return df.sort_values('time')
        except Exception as e:
            logger.error(f"Error fetching trajectory for {drifter_id}: {e}")
            
        return pd.DataFrame()

    def track(self, start_date: str, end_date: str = None, output_path: str = "drifter_track.png", scan_only: bool = False):
        """
        Finds and plots drifters for the given date range.
        If end_date is None, tracks for a single day.
        """
        # 1. Find drifters in the date range within the active display domain.
        df_found = self.find_drifters(start_date, end_date)
        
        if df_found.empty:
            logger.info("No drifters found for this period in the active domain.")
            return
        
        unique_ids = df_found['ID'].unique()
        found_dates = sorted(df_found['time'].dt.strftime('%Y-%m-%d').unique())
        
        print("\n📅 Dates with confirmed drifter observations:")
        for d in found_dates:
            print(f" - {d}")
        print(f"\n🔎 Total Drifters: {len(unique_ids)} IDs: {unique_ids}\n")

        if scan_only:
            logger.info("Scan mode enabled. Skipping trajectory download and plotting.")
            return

        logger.info(f"Found {len(unique_ids)} drifter(s): {unique_ids}")
        
        target_dt = datetime.strptime(start_date, "%Y-%m-%d")

        from src.helpers.plotting import plot_drifter_track
        plot_drifter_track(
            output_path=output_path,
            domain_bounds=self.domain_bounds,
            unique_ids=unique_ids,
            df_found=df_found,
            get_trajectory_func=self.get_trajectory,
            target_dt=target_dt,
            start_date=start_date,
            end_date=end_date
        )
        logger.info(f"Track map saved to {output_path}")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Drifter Tracker")
    parser.add_argument("start_date", help="Start date (YYYY-MM-DD)")
    parser.add_argument("end_date", nargs="?", help="End date (YYYY-MM-DD)")
    parser.add_argument("--scan", action="store_true", help="Only scan for dates, do not plot")
    
    args = parser.parse_args()
    
    tracker = DrifterTracker()
    tracker.track(args.start_date, args.end_date, scan_only=args.scan)
