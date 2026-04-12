import io
import os
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

import numpy as np
import pandas as pd
import xarray as xr

import src.__main__ as main_module
from src.exceptions.custom import (
    ForcingProviderAcquisitionError,
    PREP_OUTAGE_DECISION_EXIT_CODE,
    PREP_FORCE_REFRESH_ENV,
    PREP_REUSE_APPROVED_ONCE_ENV,
    PREP_REUSE_APPROVED_SOURCE_ENV,
    PrepOutageDecisionRequired,
)
from src.models.ingestion import IngestionManifest
from src.services.ingestion import DataIngestionService
from src.utils.forcing_outage_policy import FORCING_OUTAGE_SKIP_EXIT_CODE
from src.utils.startup_prompt_policy import INPUT_CACHE_POLICY_ENV


def _prototype_case_stub() -> SimpleNamespace:
    return SimpleNamespace(
        workflow_mode="prototype_2016",
        forcing_start_date="2016-09-01",
        forcing_end_date="2016-09-04",
        forcing_start_utc="2016-09-01T00:00:00Z",
        forcing_end_utc="2016-09-04T00:00:00Z",
        prototype_case_dates=("2016-09-01",),
        is_prototype=True,
        is_official=False,
        active_domain_name="legacy_prototype_display_domain",
        region=[115.0, 122.0, 6.0, 14.5],
        phase1_validation_box=[119.5, 124.5, 11.5, 16.5],
        mindoro_case_domain=[115.0, 122.0, 6.0, 14.5],
        legacy_prototype_display_domain=[115.0, 122.0, 6.0, 14.5],
        case_definition_path=None,
        drifter_required=False,
        drifter_mode="prototype_scan",
        arcgis_layers=[],
        validation_layer=SimpleNamespace(role="validation_polygon"),
    )


def _write_valid_wind_cache(path: Path) -> None:
    times = pd.date_range("2016-08-31T21:00:00Z", "2016-09-04T03:00:00Z", freq="6h", tz="UTC")
    ds = xr.Dataset(
        data_vars={
            "x_wind": (("time", "lat", "lon"), np.ones((len(times), 1, 1), dtype=float)),
            "y_wind": (("time", "lat", "lon"), np.ones((len(times), 1, 1), dtype=float)),
        },
        coords={
            "time": times.tz_convert(None).to_pydatetime(),
            "lat": [10.0],
            "lon": [120.0],
        },
    )
    ds.to_netcdf(path)


class PrepOutagePolicyTests(unittest.TestCase):
    def _build_service(self, tmpdir: str) -> DataIngestionService:
        case_stub = _prototype_case_stub()
        output_dir = Path(tmpdir) / "data"
        with mock.patch("src.services.ingestion.get_case_context", return_value=case_stub), mock.patch(
            "src.services.ingestion.GridBuilder", return_value=object()
        ), mock.patch("src.services.ingestion.RUN_NAME", "CASE_2016-09-01"):
            return DataIngestionService(output_dir=str(output_dir))

    def test_required_remote_outage_with_valid_cache_requires_decision_when_force_refresh_enabled(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            service = self._build_service(tmpdir)
            cache_path = Path(tmpdir) / "data" / "forcing" / "CASE_2016-09-01" / "era5_wind.nc"
            _write_valid_wind_cache(cache_path)
            manifest = IngestionManifest(config=service._manifest_config())

            with mock.patch.dict(os.environ, {PREP_FORCE_REFRESH_ENV: "1"}, clear=False):
                with self.assertRaises(PrepOutageDecisionRequired):
                    service._handle_required_download_step(
                        manifest,
                        source_id="era5",
                        download_callable=lambda: (_ for _ in ()).throw(RuntimeError("503 Server Error: Service Unavailable")),
                    )

            self.assertEqual(manifest.downloads["era5"]["status"], "awaiting_cache_reuse_decision")

    def test_required_download_step_reuses_valid_cache_before_remote_call(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            service = self._build_service(tmpdir)
            cache_path = Path(tmpdir) / "data" / "forcing" / "CASE_2016-09-01" / "era5_wind.nc"
            _write_valid_wind_cache(cache_path)
            manifest = IngestionManifest(config=service._manifest_config())
            download_callable = mock.Mock(side_effect=AssertionError("remote download should not run"))

            record = service._handle_required_download_step(
                manifest,
                source_id="era5",
                download_callable=download_callable,
            )

            download_callable.assert_not_called()
            self.assertEqual(record["status"], "reused_validated_cache")
            self.assertEqual(record["reuse_mode"], "cache_first")
            self.assertEqual(manifest.downloads["era5"]["path"], str(cache_path))

    def test_required_remote_outage_with_reuse_approval_uses_validated_cache(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            service = self._build_service(tmpdir)
            cache_path = Path(tmpdir) / "data" / "forcing" / "CASE_2016-09-01" / "era5_wind.nc"
            _write_valid_wind_cache(cache_path)
            manifest = IngestionManifest(config=service._manifest_config())

            with mock.patch.dict(
                os.environ,
                {
                    PREP_FORCE_REFRESH_ENV: "1",
                    PREP_REUSE_APPROVED_SOURCE_ENV: "era5",
                    PREP_REUSE_APPROVED_ONCE_ENV: "1",
                },
                clear=False,
            ):
                record = service._handle_required_download_step(
                    manifest,
                    source_id="era5",
                    download_callable=lambda: (_ for _ in ()).throw(RuntimeError("503 Server Error: Service Unavailable")),
                )

            self.assertEqual(record["status"], "reused_validated_cache")
            self.assertEqual(record["reuse_mode"], "outage_prompt_approved")
            self.assertEqual(manifest.downloads["era5"]["status"], "reused_validated_cache")
            self.assertEqual(manifest.downloads["era5"]["path"], str(cache_path))

    def test_force_refresh_bypasses_valid_cache(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            service = self._build_service(tmpdir)
            cache_path = Path(tmpdir) / "data" / "forcing" / "CASE_2016-09-01" / "era5_wind.nc"
            _write_valid_wind_cache(cache_path)
            manifest = IngestionManifest(config=service._manifest_config())
            fresh_path = Path(tmpdir) / "fresh_era5.nc"
            download_callable = mock.Mock(return_value=str(fresh_path))

            with mock.patch.dict(os.environ, {PREP_FORCE_REFRESH_ENV: "1"}, clear=False):
                record = service._handle_required_download_step(
                    manifest,
                    source_id="era5",
                    download_callable=download_callable,
                )

            download_callable.assert_called_once()
            self.assertEqual(record["status"], "downloaded")
            self.assertEqual(record["path"], str(fresh_path))

    def test_input_cache_policy_force_refresh_bypasses_valid_cache(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            service = self._build_service(tmpdir)
            cache_path = Path(tmpdir) / "data" / "forcing" / "CASE_2016-09-01" / "era5_wind.nc"
            _write_valid_wind_cache(cache_path)
            manifest = IngestionManifest(config=service._manifest_config())
            fresh_path = Path(tmpdir) / "fresh_era5.nc"
            download_callable = mock.Mock(return_value=str(fresh_path))

            with mock.patch.dict(os.environ, {INPUT_CACHE_POLICY_ENV: "force_refresh"}, clear=False):
                record = service._handle_required_download_step(
                    manifest,
                    source_id="era5",
                    download_callable=download_callable,
                )

            download_callable.assert_called_once()
            self.assertEqual(record["status"], "downloaded")
            self.assertEqual(record["path"], str(fresh_path))

    def test_input_cache_policy_reuse_overrides_prep_force_refresh_alias(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            service = self._build_service(tmpdir)
            cache_path = Path(tmpdir) / "data" / "forcing" / "CASE_2016-09-01" / "era5_wind.nc"
            _write_valid_wind_cache(cache_path)
            manifest = IngestionManifest(config=service._manifest_config())
            download_callable = mock.Mock(side_effect=AssertionError("remote download should not run"))

            with mock.patch.dict(
                os.environ,
                {
                    INPUT_CACHE_POLICY_ENV: "reuse_if_valid",
                    PREP_FORCE_REFRESH_ENV: "1",
                },
                clear=False,
            ):
                record = service._handle_required_download_step(
                    manifest,
                    source_id="era5",
                    download_callable=download_callable,
                )

            download_callable.assert_not_called()
            self.assertEqual(record["status"], "reused_validated_cache")

    def test_required_forcing_outage_can_continue_degraded_for_support_lane(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            service = self._build_service(tmpdir)
            manifest = IngestionManifest(config=service._manifest_config())

            record = service._handle_required_download_step(
                manifest,
                source_id="era5",
                download_callable=lambda: (_ for _ in ()).throw(RuntimeError("503 Server Error: Service Unavailable")),
                allow_outage_skip=True,
            )

            self.assertEqual(record["status"], "skipped_outage_continue_degraded")
            self.assertTrue(record["upstream_outage_detected"])
            self.assertEqual(record["forcing_factor"], "era5_wind.nc")
            self.assertTrue(service.degraded_continue_used)
            self.assertIn("era5", service.skipped_forcing_sources)
            self.assertIn("era5_wind.nc", service.missing_forcing_factors)

    def test_required_forcing_outage_skip_record_preserves_budget_metadata(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            service = self._build_service(tmpdir)
            manifest = IngestionManifest(config=service._manifest_config())
            failure_record = {
                "status": "failed_remote_outage",
                "source_id": "era5",
                "forcing_factor": "era5_wind.nc",
                "upstream_outage_detected": True,
                "error": "Forcing provider 'era5' exceeded the 300-second fail-fast budget and was stopped.",
                "budget_seconds": 300,
                "elapsed_seconds": 300.0,
                "budget_exhausted": True,
                "failure_stage": "budget_timeout",
            }

            record = service._handle_required_download_step(
                manifest,
                source_id="era5",
                download_callable=lambda: (_ for _ in ()).throw(ForcingProviderAcquisitionError(failure_record)),
                allow_outage_skip=True,
            )

            self.assertEqual(record["status"], "skipped_outage_continue_degraded")
            self.assertEqual(record["budget_seconds"], 300)
            self.assertEqual(record["elapsed_seconds"], 300.0)
            self.assertTrue(record["budget_exhausted"])
            self.assertEqual(record["failure_stage"], "budget_timeout")

    def test_required_remote_outage_without_valid_cache_cancels_immediately(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            service = self._build_service(tmpdir)
            manifest = IngestionManifest(config=service._manifest_config())

            with self.assertRaises(Exception):
                service._handle_required_download_step(
                    manifest,
                    source_id="era5",
                    download_callable=lambda: (_ for _ in ()).throw(RuntimeError("503 Server Error: Service Unavailable")),
                )

            self.assertEqual(manifest.downloads["era5"]["status"], "cancelled_no_cache")

    def test_hard_failure_with_cache_does_not_prompt(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            service = self._build_service(tmpdir)
            cache_path = Path(tmpdir) / "data" / "forcing" / "CASE_2016-09-01" / "era5_wind.nc"
            _write_valid_wind_cache(cache_path)
            manifest = IngestionManifest(config=service._manifest_config())

            with mock.patch.dict(os.environ, {PREP_FORCE_REFRESH_ENV: "1"}, clear=False):
                with self.assertRaises(ValueError):
                    service._handle_required_download_step(
                        manifest,
                        source_id="era5",
                        download_callable=lambda: (_ for _ in ()).throw(ValueError("schema mismatch in downloaded dataset")),
                    )

            self.assertEqual(manifest.downloads["era5"]["status"], "failed")

    def test_direct_download_method_reuses_valid_cache_before_credential_check(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            service = self._build_service(tmpdir)
            cache_path = Path(tmpdir) / "data" / "forcing" / "CASE_2016-09-01" / "era5_wind.nc"
            _write_valid_wind_cache(cache_path)

            with mock.patch.dict(os.environ, {}, clear=False):
                result = service.download_era5()

            self.assertEqual(result, str(cache_path))

    def test_run_prep_noninteractive_outage_points_user_to_launcher(self):
        payload_exc = PrepOutageDecisionRequired(
            run_name="CASE_2016-09-01",
            source_id="era5",
            cache_path="data/forcing/CASE_2016-09-01/era5_wind.nc",
            validation={"valid": True, "summary": "validated canonical same-case cache"},
            error="503 Server Error: Service Unavailable",
        )
        case_stub = SimpleNamespace(drifter_required=False)
        stdout = io.StringIO()

        with mock.patch("src.__main__.get_case_context", return_value=case_stub, create=True), mock.patch(
            "src.core.case_context.get_case_context", return_value=case_stub
        ), mock.patch("src.services.ingestion.DataIngestionService.run", side_effect=payload_exc), mock.patch(
            "src.__main__.print_workflow_context"
        ), mock.patch.dict(os.environ, {}, clear=False), redirect_stdout(stdout):
            with self.assertRaises(SystemExit) as exit_context:
                main_module.run_prep()

        self.assertEqual(exit_context.exception.code, 1)
        output = stdout.getvalue()
        self.assertIn(".\\start.ps1", output)
        self.assertIn("non-interactive", output)

    def test_main_propagates_special_outage_exit_code_from_orchestrated_case(self):
        case_stub = SimpleNamespace(
            orchestration_dates=["2016-09-01", "2016-09-08"],
            workflow_mode="prototype_2016",
        )
        stdout = io.StringIO()

        with mock.patch("src.core.case_context.get_case_context", return_value=case_stub), mock.patch(
            "subprocess.run", return_value=SimpleNamespace(returncode=PREP_OUTAGE_DECISION_EXIT_CODE)
        ), mock.patch.dict(
            os.environ,
            {"PIPELINE_PHASE": "prep", "WORKFLOW_MODE": "prototype_2016"},
            clear=False,
        ), redirect_stdout(stdout):
            with self.assertRaises(SystemExit) as exit_context:
                main_module.main()

        self.assertEqual(exit_context.exception.code, PREP_OUTAGE_DECISION_EXIT_CODE)
        self.assertNotIn("Continuing to next case", stdout.getvalue())

    def test_main_continues_after_nonfatal_forcing_skip_in_orchestrated_case(self):
        case_stub = SimpleNamespace(
            orchestration_dates=["2016-09-01", "2016-09-08"],
            workflow_mode="prototype_2016",
        )
        stdout = io.StringIO()

        with mock.patch("src.core.case_context.get_case_context", return_value=case_stub), mock.patch(
            "subprocess.run",
            side_effect=[
                SimpleNamespace(returncode=FORCING_OUTAGE_SKIP_EXIT_CODE),
                SimpleNamespace(returncode=0),
            ],
        ), mock.patch.dict(
            os.environ,
            {"PIPELINE_PHASE": "prep", "WORKFLOW_MODE": "prototype_2016"},
            clear=False,
        ), redirect_stdout(stdout):
            main_module.main()

        self.assertIn("skipped after forcing outage under degraded continuation", stdout.getvalue())
        self.assertIn("Spawning pipeline for CASE 2016-09-08", stdout.getvalue())


if __name__ == "__main__":
    unittest.main()
