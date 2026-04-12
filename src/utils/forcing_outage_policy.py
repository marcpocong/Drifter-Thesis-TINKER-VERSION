from __future__ import annotations

import importlib
import multiprocessing
import os
import queue as queue_module
import socket
import ssl
import time
import traceback
from typing import Any

import requests


FORCING_OUTAGE_POLICY_ENV = "FORCING_OUTAGE_POLICY"
FORCING_OUTAGE_POLICY_DEFAULT = "default"
FORCING_OUTAGE_POLICY_CONTINUE_DEGRADED = "continue_degraded"
FORCING_OUTAGE_POLICY_FAIL_HARD = "fail_hard"

FORCING_SOURCE_BUDGET_SECONDS_ENV = "FORCING_SOURCE_BUDGET_SECONDS"
FORCING_SOURCE_BUDGET_SECONDS_DEFAULT = 300
FORCING_FAILURE_STAGE_PROVIDER_CALL = "provider_call"
FORCING_FAILURE_STAGE_BUDGET_TIMEOUT = "budget_timeout"
FORCING_FAILURE_STAGE_PROVIDER_SUBPROCESS = "provider_subprocess"

FORCING_OUTAGE_SKIP_EXIT_CODE = 87
FORCING_OUTAGE_SKIP_PAYLOAD_PREFIX = "FORCING_OUTAGE_SKIP_PAYLOAD="

REMOTE_OUTAGE_MESSAGE_TOKENS = (
    "502",
    "503",
    "504",
    "service unavailable",
    "bad gateway",
    "gateway timeout",
    "temporarily unavailable",
    "timed out",
    "timeout",
    "name or service not known",
    "temporary failure in name resolution",
    "failed to establish a new connection",
    "max retries exceeded",
    "connection aborted",
    "connection reset",
    "remote disconnected",
    "dns",
    "ssl",
    "tls",
    "proxyerror",
    "error reading from remote server",
    "dap server error",
    "fail-fast budget",
    "budget exceeded",
    "budget timeout",
)

DEFAULT_CONTINUE_DEGRADED_PHASES = {
    "phase3b_extended_public_scored",
    "phase3b_extended_public_scored_march23",
    "source_history_reconstruction_r1",
    "mindoro_march13_14_phase1_focus_trial",
}

DEFAULT_CONTINUE_DEGRADED_WORKFLOWS = {
    "prototype_2016",
    "prototype_2021",
    "phase1_mindoro_focus_pre_spill_2016_2023",
}


def normalize_forcing_outage_policy(value: str | None) -> str:
    normalized = str(value or FORCING_OUTAGE_POLICY_DEFAULT).strip().lower()
    if normalized in {
        FORCING_OUTAGE_POLICY_DEFAULT,
        FORCING_OUTAGE_POLICY_CONTINUE_DEGRADED,
        FORCING_OUTAGE_POLICY_FAIL_HARD,
    }:
        return normalized
    raise ValueError(
        "FORCING_OUTAGE_POLICY must be one of: "
        f"{FORCING_OUTAGE_POLICY_DEFAULT}, "
        f"{FORCING_OUTAGE_POLICY_CONTINUE_DEGRADED}, "
        f"{FORCING_OUTAGE_POLICY_FAIL_HARD}."
    )


def resolve_forcing_outage_policy(
    *,
    workflow_mode: str | None = None,
    phase: str | None = None,
    ) -> str:
    env_value = normalize_forcing_outage_policy(os.environ.get(FORCING_OUTAGE_POLICY_ENV))
    if env_value != FORCING_OUTAGE_POLICY_DEFAULT:
        return env_value

    if str(phase or "").strip() in DEFAULT_CONTINUE_DEGRADED_PHASES:
        return FORCING_OUTAGE_POLICY_CONTINUE_DEGRADED

    if str(workflow_mode or "").strip() in DEFAULT_CONTINUE_DEGRADED_WORKFLOWS:
        return FORCING_OUTAGE_POLICY_CONTINUE_DEGRADED

    return FORCING_OUTAGE_POLICY_FAIL_HARD


def resolve_forcing_source_budget_seconds(value: str | int | None = None) -> int:
    raw_value = os.environ.get(FORCING_SOURCE_BUDGET_SECONDS_ENV) if value is None else value
    if raw_value in ("", None):
        return FORCING_SOURCE_BUDGET_SECONDS_DEFAULT
    try:
        normalized = int(raw_value)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"{FORCING_SOURCE_BUDGET_SECONDS_ENV} must be a non-negative integer number of seconds."
        ) from exc
    if normalized < 0:
        raise ValueError(
            f"{FORCING_SOURCE_BUDGET_SECONDS_ENV} must be a non-negative integer number of seconds."
        )
    return normalized


def forcing_outage_policy_allows_continue(
    *,
    workflow_mode: str | None = None,
    phase: str | None = None,
) -> bool:
    return resolve_forcing_outage_policy(workflow_mode=workflow_mode, phase=phase) == FORCING_OUTAGE_POLICY_CONTINUE_DEGRADED


def is_remote_outage_error(exc_or_message: Exception | str | Any) -> bool:
    if isinstance(
        exc_or_message,
        (
            requests.exceptions.Timeout,
            requests.exceptions.ConnectionError,
            requests.exceptions.SSLError,
            socket.timeout,
            socket.gaierror,
            TimeoutError,
            ssl.SSLError,
        ),
    ):
        return True

    if isinstance(exc_or_message, requests.exceptions.HTTPError):
        response = getattr(exc_or_message, "response", None)
        if response is not None and response.status_code >= 500:
            return True

    message = str(exc_or_message).lower()
    return any(token in message for token in REMOTE_OUTAGE_MESSAGE_TOKENS)


def _import_callable(callable_path: str):
    module_name, _, attr_name = str(callable_path).rpartition(".")
    if not module_name or not attr_name:
        raise ValueError(f"Invalid callable path: {callable_path}")
    module = importlib.import_module(module_name)
    target = getattr(module, attr_name)
    if not callable(target):
        raise TypeError(f"Imported object is not callable: {callable_path}")
    return target


def _budgeted_provider_child(
    result_queue,
    *,
    callable_path: str,
    kwargs: dict[str, Any],
) -> None:
    started = time.monotonic()
    try:
        target = _import_callable(callable_path)
        result = target(**kwargs)
        result_queue.put(
            {
                "ok": True,
                "result": result,
                "elapsed_seconds": time.monotonic() - started,
            }
        )
    except BaseException as exc:  # pragma: no cover - exercised through parent process
        result_queue.put(
            {
                "ok": False,
                "error": str(exc),
                "error_type": type(exc).__name__,
                "elapsed_seconds": time.monotonic() - started,
                "upstream_outage_detected": is_remote_outage_error(exc),
                "failure_stage": getattr(exc, "failure_stage", FORCING_FAILURE_STAGE_PROVIDER_CALL),
                "traceback": traceback.format_exc(),
            }
        )


def _standardize_budgeted_provider_success(
    *,
    source_id: str,
    forcing_factor: str,
    budget_seconds: int,
    elapsed_seconds: float,
    result: Any,
) -> dict[str, Any]:
    if isinstance(result, dict):
        record = dict(result)
    else:
        record = {"result": result}

    record.setdefault("status", "downloaded")
    record["source_id"] = str(record.get("source_id") or source_id)
    record["forcing_factor"] = str(record.get("forcing_factor") or forcing_factor)
    record["elapsed_seconds"] = float(round(elapsed_seconds, 3))
    record["budget_seconds"] = int(budget_seconds)
    record["budget_exhausted"] = bool(record.get("budget_exhausted", False))
    record["upstream_outage_detected"] = bool(record.get("upstream_outage_detected", False))
    record["failure_stage"] = str(record.get("failure_stage") or "")
    return record


def _standardize_budgeted_provider_failure(
    *,
    source_id: str,
    forcing_factor: str,
    budget_seconds: int,
    elapsed_seconds: float,
    error: str,
    upstream_outage_detected: bool,
    failure_stage: str,
    budget_exhausted: bool = False,
    path: str = "",
) -> dict[str, Any]:
    return {
        "status": "failed_remote_outage" if upstream_outage_detected else "failed",
        "source_id": str(source_id),
        "forcing_factor": str(forcing_factor),
        "path": str(path or ""),
        "elapsed_seconds": float(round(elapsed_seconds, 3)),
        "budget_seconds": int(budget_seconds),
        "budget_exhausted": bool(budget_exhausted),
        "upstream_outage_detected": bool(upstream_outage_detected),
        "failure_stage": str(failure_stage or FORCING_FAILURE_STAGE_PROVIDER_CALL),
        "error": str(error),
    }


def run_budgeted_forcing_provider_call(
    *,
    callable_path: str,
    kwargs: dict[str, Any],
    source_id: str,
    forcing_factor: str,
    failure_path: str = "",
    budget_seconds: int | None = None,
) -> dict[str, Any]:
    resolved_budget = resolve_forcing_source_budget_seconds(budget_seconds)
    started = time.monotonic()

    if resolved_budget == 0:
        try:
            target = _import_callable(callable_path)
            result = target(**kwargs)
            return _standardize_budgeted_provider_success(
                source_id=source_id,
                forcing_factor=forcing_factor,
                budget_seconds=resolved_budget,
                elapsed_seconds=time.monotonic() - started,
                result=result,
            )
        except Exception as exc:
            return _standardize_budgeted_provider_failure(
                source_id=source_id,
                forcing_factor=forcing_factor,
                budget_seconds=resolved_budget,
                elapsed_seconds=time.monotonic() - started,
                error=str(exc),
                upstream_outage_detected=is_remote_outage_error(exc),
                failure_stage=getattr(exc, "failure_stage", FORCING_FAILURE_STAGE_PROVIDER_CALL),
                path=failure_path,
            )

    context_name = "fork" if "fork" in multiprocessing.get_all_start_methods() else "spawn"
    context = multiprocessing.get_context(context_name)
    result_queue = context.Queue()
    process = context.Process(
        target=_budgeted_provider_child,
        kwargs={
            "result_queue": result_queue,
            "callable_path": callable_path,
            "kwargs": dict(kwargs or {}),
        },
    )
    process.start()
    process.join(timeout=resolved_budget)

    if process.is_alive():
        process.terminate()
        process.join(timeout=5)
        if process.is_alive():  # pragma: no cover - defensive
            process.kill()
            process.join(timeout=5)
        return _standardize_budgeted_provider_failure(
            source_id=source_id,
            forcing_factor=forcing_factor,
            budget_seconds=resolved_budget,
            elapsed_seconds=time.monotonic() - started,
            error=(
                f"Forcing provider '{source_id}' exceeded the {resolved_budget}-second fail-fast budget and was stopped."
            ),
            upstream_outage_detected=True,
            failure_stage=FORCING_FAILURE_STAGE_BUDGET_TIMEOUT,
            budget_exhausted=True,
            path=failure_path,
        )

    try:
        payload = result_queue.get_nowait()
    except queue_module.Empty:
        payload = None

    if payload is None:
        return _standardize_budgeted_provider_failure(
            source_id=source_id,
            forcing_factor=forcing_factor,
            budget_seconds=resolved_budget,
            elapsed_seconds=time.monotonic() - started,
            error=(
                f"Forcing provider subprocess for '{source_id}' exited without returning a result "
                f"(exit code {process.exitcode})."
            ),
            upstream_outage_detected=False,
            failure_stage=FORCING_FAILURE_STAGE_PROVIDER_SUBPROCESS,
            path=failure_path,
        )

    if payload.get("ok"):
        return _standardize_budgeted_provider_success(
            source_id=source_id,
            forcing_factor=forcing_factor,
            budget_seconds=resolved_budget,
            elapsed_seconds=float(payload.get("elapsed_seconds", time.monotonic() - started)),
            result=payload.get("result"),
        )

    return _standardize_budgeted_provider_failure(
        source_id=source_id,
        forcing_factor=forcing_factor,
        budget_seconds=resolved_budget,
        elapsed_seconds=float(payload.get("elapsed_seconds", time.monotonic() - started)),
        error=str(payload.get("error") or "unknown forcing-provider failure"),
        upstream_outage_detected=bool(payload.get("upstream_outage_detected", False)),
        failure_stage=str(payload.get("failure_stage") or FORCING_FAILURE_STAGE_PROVIDER_CALL),
        path=failure_path,
    )


def forcing_factor_id_for_source(source_id: str) -> str:
    mapping = {
        "hycom": "hycom_curr.nc",
        "cmems": "cmems_curr.nc",
        "cmems_wave": "cmems_wave.nc",
        "era5": "era5_wind.nc",
        "ncep": "ncep_wind.nc",
        "gfs": "gfs_wind.nc",
    }
    try:
        return mapping[str(source_id)]
    except KeyError as exc:
        raise KeyError(f"Unsupported forcing source id: {source_id}") from exc


def forcing_kind_for_source(source_id: str) -> str:
    factor_id = forcing_factor_id_for_source(source_id)
    if factor_id.endswith("_curr.nc"):
        return "current"
    if factor_id.endswith("_wave.nc"):
        return "wave"
    return "wind"


def source_id_for_recipe_component(*, forcing_kind: str, filename: str) -> str:
    lower = str(filename or "").strip().lower()
    if forcing_kind == "current":
        if lower.startswith("cmems"):
            return "cmems"
        if lower.startswith("hycom"):
            return "hycom"
    elif forcing_kind == "wave":
        if lower.startswith("cmems"):
            return "cmems_wave"
    elif forcing_kind == "wind":
        if lower.startswith("era5"):
            return "era5"
        if lower.startswith("ncep"):
            return "ncep"
        if lower.startswith("gfs"):
            return "gfs"
    raise ValueError(f"Unsupported {forcing_kind} forcing file: {filename}")
