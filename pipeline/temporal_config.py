from __future__ import annotations

import asyncio
import os
import shutil
import socket
import time
from datetime import timedelta

from .constants import TEMPORAL_DEFAULT_ADDRESS, TEMPORAL_DEFAULT_NAMESPACE, TEMPORAL_DEFAULT_TASK_QUEUE
from .temporal_sdk import Client, ensure_temporal_sdk_available

TEMPORAL_NAMESPACE_READY_TIMEOUT_SECONDS = 60.0
TEMPORAL_NAMESPACE_POLL_INTERVAL_SECONDS = 1.0
TEMPORAL_NAMESPACE_RETENTION_SECONDS = 24 * 60 * 60


def temporal_address() -> str:
    return (
        os.environ.get("TEMPORAL_ADDRESS", "").strip()
        or os.environ.get("TEMPORAL_HOST", "").strip()
        or TEMPORAL_DEFAULT_ADDRESS
    )


def temporal_namespace() -> str:
    return os.environ.get("TEMPORAL_NAMESPACE", "").strip() or TEMPORAL_DEFAULT_NAMESPACE


def temporal_task_queue() -> str:
    return os.environ.get("TEMPORAL_TASK_QUEUE", "").strip() or TEMPORAL_DEFAULT_TASK_QUEUE


def temporal_address_parts() -> tuple[str, int]:
    address = temporal_address()
    host, _, port_text = address.partition(":")
    host = host.strip() or "localhost"
    try:
        port = int(port_text.strip() or "7233")
    except ValueError:
        port = 7233
    return host, port


def temporal_server_is_reachable(timeout_seconds: float = 1.0) -> bool:
    host, port = temporal_address_parts()
    try:
        with socket.create_connection((host, port), timeout=timeout_seconds):
            return True
    except OSError:
        return False


def find_temporal_cli() -> str | None:
    configured = os.environ.get("TEMPORAL_CLI_PATH", "").strip()
    if configured:
        return configured

    for candidate in ("temporal", r"C:\temporal\temporal.exe"):
        resolved = shutil.which(candidate) if candidate == "temporal" else candidate
        if resolved and os.path.exists(resolved):
            return resolved
    return None


def _close_client_result(client: Client) -> object | None:
    if not hasattr(client, "close"):
        return None
    return client.close()


def _is_rpc_status(error: BaseException, *status_names: str) -> bool:
    try:
        from temporalio.service import RPCError, RPCStatusCode
    except Exception:
        return False
    if not isinstance(error, RPCError):
        return False
    statuses = {getattr(RPCStatusCode, status_name) for status_name in status_names}
    return error.status in statuses


async def _register_temporal_namespace(client: Client, namespace: str) -> None:
    from google.protobuf.duration_pb2 import Duration
    from temporalio.api.workflowservice.v1 import RegisterNamespaceRequest

    retention = Duration()
    retention.FromSeconds(TEMPORAL_NAMESPACE_RETENTION_SECONDS)
    try:
        await client.workflow_service.register_namespace(
            RegisterNamespaceRequest(
                namespace=namespace,
                workflow_execution_retention_period=retention,
            ),
            retry=False,
            timeout=timedelta(seconds=5),
        )
    except Exception as error:
        if _is_rpc_status(error, "ALREADY_EXISTS"):
            return
        raise


async def ensure_temporal_namespace(
    *,
    timeout_seconds: float = TEMPORAL_NAMESPACE_READY_TIMEOUT_SECONDS,
) -> None:
    ensure_temporal_sdk_available()
    from temporalio.api.workflowservice.v1 import DescribeNamespaceRequest

    address = temporal_address()
    namespace = temporal_namespace()
    deadline = time.monotonic() + timeout_seconds
    last_error: BaseException | None = None

    while time.monotonic() < deadline:
        client: Client | None = None
        try:
            if not temporal_server_is_reachable(timeout_seconds=0.5):
                await asyncio.sleep(TEMPORAL_NAMESPACE_POLL_INTERVAL_SECONDS)
                continue

            client = await Client.connect(address, namespace=namespace, lazy=True)
            try:
                await client.workflow_service.describe_namespace(
                    DescribeNamespaceRequest(namespace=namespace),
                    retry=False,
                    timeout=timedelta(seconds=5),
                )
                return
            except Exception as error:
                if _is_rpc_status(error, "NOT_FOUND"):
                    await _register_temporal_namespace(client, namespace)
                elif _is_rpc_status(error, "UNAVAILABLE", "DEADLINE_EXCEEDED", "UNKNOWN"):
                    last_error = error
                else:
                    raise
        except Exception as error:
            last_error = error
            if not _is_rpc_status(
                error,
                "UNAVAILABLE",
                "DEADLINE_EXCEEDED",
                "UNKNOWN",
                "NOT_FOUND",
                "ALREADY_EXISTS",
            ):
                raise
        finally:
            if client is not None:
                close_result = _close_client_result(client)
                if hasattr(close_result, "__await__"):
                    await close_result

        await asyncio.sleep(TEMPORAL_NAMESPACE_POLL_INTERVAL_SECONDS)

    detail = f" Last error: {last_error}" if last_error else ""
    raise RuntimeError(
        f"Temporal namespace {namespace!r} was not ready at {address} within {timeout_seconds:.0f} seconds."
        f"{detail}"
    )


async def get_temporal_workflow_status(
    workflow_id: str,
    *,
    client: Client | None = None,
) -> str | None:
    if not workflow_id.strip():
        return None

    created_client = client is None
    resolved_client = client or await connect_temporal_client()
    try:
        handle = resolved_client.get_workflow_handle(workflow_id)
        description = await handle.describe()
        status_value = int(description.raw_description.workflow_execution_info.status)
    except Exception:
        return None
    finally:
        if created_client and hasattr(resolved_client, "close"):
            close_result = resolved_client.close()
            if hasattr(close_result, "__await__"):
                await close_result

    status_map = {
        1: "running",
        2: "completed",
        3: "failed",
        4: "canceled",
        5: "terminated",
        6: "continued_as_new",
        7: "timed_out",
    }
    return status_map.get(status_value, "unknown")


async def terminate_temporal_workflow(
    workflow_id: str,
    *,
    reason: str,
    client: Client | None = None,
) -> bool:
    if not workflow_id.strip():
        return False

    created_client = client is None
    resolved_client = client or await connect_temporal_client()
    try:
        handle = resolved_client.get_workflow_handle(workflow_id)
        await handle.terminate(reason)
        return True
    except Exception:
        return False
    finally:
        if created_client and hasattr(resolved_client, "close"):
            close_result = resolved_client.close()
            if hasattr(close_result, "__await__"):
                await close_result


async def connect_temporal_client() -> Client:
    ensure_temporal_sdk_available()
    await ensure_temporal_namespace()
    return await Client.connect(temporal_address(), namespace=temporal_namespace())
