import json
import tempfile
import unittest
from pathlib import Path

from scripts.generate_mindoro_march3_6_observation_overlay import (
    FIGURE_FILENAME,
    MANIFEST_FILENAME,
    REPO_ROOT,
    generate_experiment,
    load_selected_observation_layers,
)


def _polygon_feature(min_lon: float, min_lat: float, max_lon: float, max_lat: float) -> dict:
    return {
        "type": "Feature",
        "properties": {"name": "test"},
        "geometry": {
            "type": "Polygon",
            "coordinates": [
                [
                    [min_lon, min_lat],
                    [max_lon, min_lat],
                    [max_lon, max_lat],
                    [min_lon, max_lat],
                    [min_lon, min_lat],
                ]
            ],
        },
    }


class _FakeResponse:
    def __init__(self, payload):
        self.payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self):
        return self.payload


class _FakeSession:
    def __init__(self):
        self.calls = []

    def get(self, url, params=None, timeout=None):
        self.calls.append((url, dict(params or {})))
        normalized = url.rstrip("/")

        if normalized.endswith("MindoroOilSpill_Philsa_230303/FeatureServer"):
            return _FakeResponse({"layers": [{"id": 0}]})
        if normalized.endswith("MindoroOilSpill_Philsa_230304/FeatureServer"):
            return _FakeResponse({"layers": [{"id": 0}]})
        if normalized.endswith("MindoroOilSpill_MSI_230305/FeatureServer"):
            return _FakeResponse({"layers": [{"id": 0}]})

        if normalized.endswith("MindoroOilSpill_Philsa_230303/FeatureServer/0"):
            return _FakeResponse({"id": 0, "name": "MindoroOilSpill_Philsa_230303", "geometryType": "esriGeometryPolygon"})
        if normalized.endswith("MindoroOilSpill_Philsa_230304/FeatureServer/0"):
            return _FakeResponse({"id": 0, "name": "MindoroOilSpill_Philsa_230304", "geometryType": "esriGeometryPolygon"})
        if normalized.endswith("MindoroOilSpill_MSI_230305/FeatureServer/0"):
            return _FakeResponse({"id": 0, "name": "MindoroOilSpill_MSI_230305", "geometryType": "esriGeometryPolygon"})
        if normalized.endswith("Mindoro_Oil_Spills_Monitoring_Map_WFL1/FeatureServer/3"):
            return _FakeResponse({"id": 3, "name": "Possible_oil_Spills_(March_3,_2023)", "geometryType": "esriGeometryPolygon"})
        if normalized.endswith("Mindoro_Oil_Spills_Monitoring_Map_WFL1/FeatureServer/1"):
            return _FakeResponse({"id": 1, "name": "Possible_oil_slick_(March_6,_2023)", "geometryType": "esriGeometryPolygon"})

        if normalized.endswith("MindoroOilSpill_Philsa_230303/FeatureServer/0/query"):
            return _FakeResponse({"type": "FeatureCollection", "features": [_polygon_feature(120.96, 12.30, 121.01, 12.36)]})
        if normalized.endswith("MindoroOilSpill_Philsa_230304/FeatureServer/0/query"):
            return _FakeResponse({"type": "FeatureCollection", "features": [_polygon_feature(120.98, 12.34, 121.05, 12.41)]})
        if normalized.endswith("MindoroOilSpill_MSI_230305/FeatureServer/0/query"):
            return _FakeResponse({"type": "FeatureCollection", "features": [_polygon_feature(121.00, 12.37, 121.08, 12.47)]})
        if normalized.endswith("Mindoro_Oil_Spills_Monitoring_Map_WFL1/FeatureServer/3/query"):
            return _FakeResponse({"type": "FeatureCollection", "features": [_polygon_feature(120.95, 12.28, 121.02, 12.34)]})
        if normalized.endswith("Mindoro_Oil_Spills_Monitoring_Map_WFL1/FeatureServer/1/query"):
            return _FakeResponse({"type": "FeatureCollection", "features": [_polygon_feature(121.02, 12.39, 121.11, 12.52)]})

        raise AssertionError(f"Unexpected URL requested in test fake session: {url} params={params}")


class _FailingSession:
    def get(self, url, params=None, timeout=None):
        raise AssertionError(f"Network access should not be needed when cached files exist: {url}")


class MindoroMarch36ObservationOverlayTests(unittest.TestCase):
    def test_selection_locks_exact_five_inventory_rows(self):
        selected = load_selected_observation_layers()
        self.assertEqual(
            [(row.observation_date, row.provider, row.source_name) for row in selected],
            [
                ("2023-03-03", "PhilSA", "MindoroOilSpill_Philsa_230303"),
                ("2023-03-03", "WWF Philippines", "Possible_oil_Spills_(March_3,_2023)"),
                ("2023-03-04", "PhilSA", "MindoroOilSpill_Philsa_230304"),
                ("2023-03-05", "UP MSI", "MindoroOilSpill_MSI_230305"),
                ("2023-03-06", "WWF Philippines", "Possible_oil_slick_(March_6,_2023)"),
            ],
        )

    def test_cache_reuse_avoids_second_network_fetch(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir) / "overlay"
            first = generate_experiment(
                repo_root=REPO_ROOT,
                output_dir=output_dir,
                session=_FakeSession(),
                land_context_path=None,
            )
            second = generate_experiment(
                repo_root=REPO_ROOT,
                output_dir=output_dir,
                session=_FailingSession(),
                land_context_path=None,
            )

            self.assertTrue(Path(first["output_image"]).exists())
            self.assertTrue(Path(second["output_image"]).exists())

    def test_smoke_generate_writes_png_and_manifest_only_in_experiment_folder(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            output_dir = Path(tmpdir) / "mindoro_overlay"
            result = generate_experiment(
                repo_root=REPO_ROOT,
                output_dir=output_dir,
                session=_FakeSession(),
                land_context_path=None,
            )

            image_path = output_dir / FIGURE_FILENAME
            manifest_path = output_dir / MANIFEST_FILENAME

            self.assertEqual(result["output_image"], str(image_path))
            self.assertTrue(image_path.exists())
            self.assertTrue(manifest_path.exists())

            pngs = sorted(output_dir.glob("*.png"))
            manifests = sorted(output_dir.glob("*.json"))
            self.assertEqual([path.name for path in pngs], [FIGURE_FILENAME])
            self.assertEqual([path.name for path in manifests], [MANIFEST_FILENAME])

            payload = json.loads(manifest_path.read_text(encoding="utf-8"))
            self.assertEqual(payload["case_id"], "CASE_MINDORO_RETRO_2023")
            self.assertEqual(len(payload["selected_layers"]), 5)
            self.assertIn("plot_bounds_wgs84", payload)


if __name__ == "__main__":
    unittest.main()
