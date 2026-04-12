import os
import unittest
from unittest import mock

from src.utils.forcing_outage_policy import (
    FORCING_OUTAGE_POLICY_CONTINUE_DEGRADED,
    FORCING_OUTAGE_POLICY_FAIL_HARD,
    resolve_forcing_outage_policy,
    resolve_forcing_source_budget_seconds,
)


class ForcingOutagePolicyTests(unittest.TestCase):
    def test_reportable_workflows_fail_hard_by_default(self):
        with mock.patch.dict(os.environ, {}, clear=False):
            self.assertEqual(
                resolve_forcing_outage_policy(
                    workflow_mode="phase1_regional_2016_2022",
                    phase="phase1_production_rerun",
                ),
                FORCING_OUTAGE_POLICY_FAIL_HARD,
            )
            self.assertEqual(
                resolve_forcing_outage_policy(
                    workflow_mode="dwh_retro_2010",
                    phase="dwh_phase3c_scientific_forcing_ready",
                ),
                FORCING_OUTAGE_POLICY_FAIL_HARD,
            )

    def test_support_workflows_and_phases_continue_degraded_by_default(self):
        with mock.patch.dict(os.environ, {}, clear=False):
            self.assertEqual(
                resolve_forcing_outage_policy(
                    workflow_mode="phase1_mindoro_focus_pre_spill_2016_2023",
                    phase="phase1_production_rerun",
                ),
                FORCING_OUTAGE_POLICY_CONTINUE_DEGRADED,
            )
            self.assertEqual(
                resolve_forcing_outage_policy(
                    workflow_mode="mindoro_retro_2023",
                    phase="phase3b_extended_public_scored_march23",
                ),
                FORCING_OUTAGE_POLICY_CONTINUE_DEGRADED,
            )

    def test_env_override_wins_over_defaults(self):
        with mock.patch.dict(os.environ, {"FORCING_OUTAGE_POLICY": "continue_degraded"}, clear=False):
            self.assertEqual(
                resolve_forcing_outage_policy(
                    workflow_mode="phase1_regional_2016_2022",
                    phase="phase1_production_rerun",
                ),
                FORCING_OUTAGE_POLICY_CONTINUE_DEGRADED,
            )

        with mock.patch.dict(os.environ, {"FORCING_OUTAGE_POLICY": "fail_hard"}, clear=False):
            self.assertEqual(
                resolve_forcing_outage_policy(
                    workflow_mode="phase1_mindoro_focus_pre_spill_2016_2023",
                    phase="phase1_production_rerun",
                ),
                FORCING_OUTAGE_POLICY_FAIL_HARD,
            )

    def test_forcing_source_budget_defaults_to_300_seconds(self):
        with mock.patch.dict(os.environ, {}, clear=False):
            self.assertEqual(resolve_forcing_source_budget_seconds(), 300)

    def test_forcing_source_budget_zero_disables_hard_timeout(self):
        with mock.patch.dict(os.environ, {"FORCING_SOURCE_BUDGET_SECONDS": "0"}, clear=False):
            self.assertEqual(resolve_forcing_source_budget_seconds(), 0)


if __name__ == "__main__":
    unittest.main()
