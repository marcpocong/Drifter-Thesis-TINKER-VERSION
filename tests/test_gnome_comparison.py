import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from src.services.gnome_comparison import GnomeComparisonService


class _DummyAccumulator(list):
    def __iadd__(self, other):
        if isinstance(other, list):
            self.extend(other)
        else:
            self.append(other)
        return self


class _DummyModel:
    instances = []

    def __init__(self, *args, **kwargs):
        self.environment = _DummyAccumulator()
        self.movers = _DummyAccumulator()
        self.spills = _DummyAccumulator()
        self.outputters = _DummyAccumulator()
        self.full_run_called = False
        type(self).instances.append(self)

    def full_run(self):
        self.full_run_called = True


class GnomeComparisonServiceTests(unittest.TestCase):
    def test_transport_benchmark_accepts_polygon_and_time_overrides(self):
        _DummyModel.instances.clear()

        def _dummy_spill(**kwargs):
            return kwargs

        with tempfile.TemporaryDirectory() as tmpdir:
            service = GnomeComparisonService()
            service.output_dir = Path(tmpdir)
            with (
                patch("src.services.gnome_comparison.GNOME_AVAILABLE", True),
                patch("src.services.gnome_comparison.Model", _DummyModel, create=True),
                patch("src.services.gnome_comparison.Wind", side_effect=lambda *args, **kwargs: {"kind": "wind"}, create=True),
                patch(
                    "src.services.gnome_comparison.WindMover",
                    side_effect=lambda wind: {"kind": "wind_mover", "wind": wind},
                    create=True,
                ),
                patch("src.services.gnome_comparison.GnomeOil", side_effect=lambda oil_type: {"oil_type": oil_type}, create=True),
                patch(
                    "src.services.gnome_comparison.NetCDFOutput",
                    side_effect=lambda **kwargs: {"kind": "netcdf_output", **kwargs},
                    create=True,
                ),
                patch("src.services.gnome_comparison.surface_point_line_spill", side_effect=_dummy_spill, create=True),
                patch(
                    "src.utils.io.resolve_polygon_seeding",
                    return_value=([121.5], [13.3], "2023-03-12T16:00:00Z"),
                ) as mocked_seed,
            ):
                nc_path, metadata = service.run_transport_benchmark_scenario(
                    start_lat=13.3,
                    start_lon=121.5,
                    start_time="2023-03-12T16:00:00Z",
                    output_name="custom.nc",
                    polygon_path="custom_seed_polygon.gpkg",
                    seed_time_override="2023-03-12T16:00:00Z",
                    duration_hours=48,
                    time_step_minutes=60,
                )

        self.assertEqual(mocked_seed.call_args.kwargs["polygon_path"], "custom_seed_polygon.gpkg")
        self.assertEqual(mocked_seed.call_args.kwargs["seed_time_override"], "2023-03-12T16:00:00Z")
        self.assertEqual(metadata["polygon_path_override"], "custom_seed_polygon.gpkg")
        self.assertTrue(metadata["custom_polygon_override_used"])
        self.assertEqual(metadata["duration_hours"], 48)
        self.assertEqual(metadata["time_step_minutes"], 60)
        self.assertEqual(metadata["seed_time_override_utc"], "2023-03-12T16:00:00Z")
        self.assertTrue(str(nc_path).endswith("custom.nc"))
        self.assertTrue(_DummyModel.instances[0].full_run_called)
        self.assertEqual(
            _DummyModel.instances[0].spills[0]["release_time"].isoformat().replace("+00:00", "Z"),
            "2023-03-12T16:00:00Z",
        )


if __name__ == "__main__":
    unittest.main()
