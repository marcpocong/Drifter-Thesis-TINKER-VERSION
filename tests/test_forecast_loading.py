import tempfile
import unittest
from pathlib import Path

import numpy as np
import pandas as pd
import xarray as xr

from src.services.ensemble import EnsembleForecastService, normalize_model_timestamp


class ForecastLoadingTests(unittest.TestCase):
    def test_normalize_model_timestamp_strips_timezone(self):
        ts = normalize_model_timestamp("2023-03-03T09:59:00Z")
        self.assertIsNone(ts.tzinfo)
        self.assertEqual(str(ts), "2023-03-03 09:59:00")

    def test_extend_forcing_tail_adds_requested_end_time(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            base = Path(tmpdir)
            source_path = base / "currents.nc"
            cache_dir = base / "cache"

            ds = xr.Dataset(
                data_vars={
                    "uo": (("time",), np.array([0.1, 0.2], dtype=np.float32)),
                    "vo": (("time",), np.array([0.0, 0.0], dtype=np.float32)),
                },
                coords={
                    "time": pd.to_datetime(["2023-03-03T00:00:00", "2023-03-04T00:00:00"]),
                },
            )
            ds.to_netcdf(source_path)

            service = EnsembleForecastService(str(source_path), str(source_path))
            service.loading_cache_dir = cache_dir

            extended_path = service._extend_forcing_tail(
                source_path=source_path,
                target_end_time=pd.Timestamp("2023-03-04T12:00:00"),
                time_coordinate="time",
            )

            self.assertNotEqual(extended_path, source_path)
            with xr.open_dataset(extended_path) as extended:
                times = pd.to_datetime(extended["time"].values)
                self.assertEqual(str(times[-1]), "2023-03-04 12:00:00")
                self.assertEqual(len(times), 3)


if __name__ == "__main__":
    unittest.main()
