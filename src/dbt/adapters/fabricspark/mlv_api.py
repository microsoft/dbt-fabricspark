"""REST API helpers for Materialized Lake View (MLV) operations.

Provides functions to manage MLV refresh schedules and on-demand jobs
via the Fabric Job Scheduler API.  All HTTP calls include automatic retries
with exponential back-off and surface detailed error messages on failure.

API reference:
https://learn.microsoft.com/en-us/fabric/data-engineering/materialized-lake-views/materialized-lake-views-public-api
"""

from __future__ import annotations

import datetime as dt
import random
import time
from typing import Any, Dict, List, Optional

import requests
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.fabricspark.credentials import FabricSparkCredentials
from dbt.adapters.fabricspark.livysession import get_headers

logger = AdapterLogger("Microsoft Fabric-Spark")

MLV_JOB_TYPE = "RefreshMaterializedLakeViews"

# Retry configuration
MAX_RETRIES = 3
RETRY_BACKOFF_BASE = 2  # seconds
RETRYABLE_STATUS_CODES = {429, 500, 502, 503, 504}

# Error codes from Fabric that indicate capacity/throttle issues on the job itself.
# These are distinct from HTTP 429s during polling — they come from the job's
# failureReason when Fabric couldn't allocate Spark capacity for the MLV refresh.
_THROTTLE_ERROR_CODES = {
    "MLV_SPARK_JOB_CAPACITY_THROTTLING",
    "RequestBlocked",
}

# Cache: workspace_id -> {lakehouse_name -> lakehouse_id}
_lakehouse_id_cache: Dict[str, Dict[str, str]] = {}


class MLVApiError(DbtRuntimeError):
    """Raised when an MLV REST API call fails after all retries."""

    def __init__(self, operation: str, detail: str) -> None:
        super().__init__(f"MLV API error during {operation}: {detail}")


def resolve_lakehouse_id(
    credentials: FabricSparkCredentials,
    lakehouse_name: str,
) -> str:
    """Resolve a lakehouse name to its ID within the workspace.

    Uses GET /v1/workspaces/{workspaceId}/lakehouses to list all lakehouses,
    then matches by ``displayName`` (case-insensitive).  Results are cached
    per workspace to avoid repeated API calls.

    Raises ``MLVApiError`` if the lakehouse cannot be found or the API call fails.
    """
    workspace_id = credentials.workspaceid

    # Check cache first
    if workspace_id in _lakehouse_id_cache:
        cached = _lakehouse_id_cache[workspace_id]
        name_lower = lakehouse_name.lower()
        if name_lower in cached:
            return cached[name_lower]

    # Fetch all lakehouses in the workspace
    url = f"{credentials.endpoint}/workspaces/{workspace_id}/lakehouses"
    headers = get_headers(credentials)
    logger.debug(f"Resolving lakehouse name '{lakehouse_name}': GET {url}")

    try:
        response = _request_with_retry(
            "GET",
            url,
            headers,
            operation=f"resolve lakehouse '{lakehouse_name}'",
            timeout=credentials.http_timeout,
        )
    except MLVApiError:
        raise

    data = response.json()
    lakehouses = data.get("value", [])

    # Build cache for this workspace
    name_to_id: Dict[str, str] = {}
    for lh in lakehouses:
        display_name = lh.get("displayName", "")
        lh_id = lh.get("id", "")
        if display_name and lh_id:
            name_to_id[display_name.lower()] = lh_id

    _lakehouse_id_cache[workspace_id] = name_to_id

    name_lower = lakehouse_name.lower()
    if name_lower not in name_to_id:
        available = ", ".join(lh.get("displayName", "?") for lh in lakehouses) or "(none)"
        raise MLVApiError(
            f"resolve lakehouse '{lakehouse_name}'",
            f"Lakehouse '{lakehouse_name}' not found in workspace {workspace_id}. "
            f"Available lakehouses: {available}",
        )

    resolved_id = name_to_id[name_lower]
    logger.info(f"Resolved lakehouse '{lakehouse_name}' -> {resolved_id}")
    return resolved_id


def _base_url(credentials: FabricSparkCredentials, lakehouse_id: Optional[str] = None) -> str:
    """Build the base URL for MLV job scheduler API calls."""
    lh_id = lakehouse_id or credentials.lakehouseid
    return (
        f"{credentials.endpoint}/workspaces/{credentials.workspaceid}"
        f"/lakehouses/{lh_id}/jobs/{MLV_JOB_TYPE}"
    )


def _extract_error_detail(response: requests.Response) -> str:
    """Extract a human-readable error message from a failed API response."""
    try:
        body = response.json()
        # Fabric API errors typically use {"error": {"code": ..., "message": ...}}
        if "error" in body:
            err = body["error"]
            code = err.get("code", "Unknown")
            message = err.get("message", response.text)
            return f"[{code}] {message}"
        return response.text
    except Exception:
        return response.text or f"HTTP {response.status_code}"


def _parse_retry_after(response: requests.Response) -> float:
    """Extract wait time (seconds) from a 429 response.

    Checks the ``Retry-After`` header first, then falls back to the
    Fabric-specific "until: <timestamp>" pattern in the response body.
    Returns 0 if no hint is found.
    """
    header = response.headers.get("Retry-After", "")
    if header:
        try:
            return float(header)
        except ValueError:
            pass
    try:
        body = response.json()
        msg = body.get("message", "")
        if "until:" in msg:
            ts_str = msg.split("until:")[1].strip().rstrip(")")
            ts_str = ts_str.replace("(UTC", "").strip()
            target = dt.datetime.strptime(ts_str, "%m/%d/%Y %I:%M:%S %p")
            delta = (target - dt.datetime.utcnow()).total_seconds()
            return max(delta, 0)
    except Exception:
        pass
    return 0


def _request_with_retry(
    method: str,
    url: str,
    headers: Dict[str, str],
    operation: str,
    timeout: int,
    json_body: Optional[Dict[str, Any]] = None,
    max_retries: int = MAX_RETRIES,
) -> requests.Response:
    """Execute an HTTP request with automatic retries and exponential back-off.

    For 429 responses, honours the ``Retry-After`` header (or the Fabric
    "until:" body hint) so the client waits exactly as long as the server
    requests, avoiding unnecessary hammering during throttle windows.

    Raises ``MLVApiError`` when all attempts are exhausted or a non-retryable
    error is encountered.
    """
    last_exception: Optional[Exception] = None

    for attempt in range(1, max_retries + 1):
        try:
            logger.debug(f"MLV API {method} {url} (attempt {attempt}/{max_retries})")
            response = requests.request(
                method, url, headers=headers, json=json_body, timeout=timeout
            )

            if response.status_code < 400:
                return response

            # Retryable server / throttle errors
            if response.status_code in RETRYABLE_STATUS_CODES and attempt < max_retries:
                detail = _extract_error_detail(response)
                if response.status_code == 429:
                    retry_after = _parse_retry_after(response)
                    wait = max(retry_after, RETRY_BACKOFF_BASE**attempt) + random.uniform(0, 2)
                else:
                    wait = RETRY_BACKOFF_BASE**attempt
                logger.warning(
                    f"MLV API {operation} returned {response.status_code} "
                    f"({detail}). Retrying in {wait:.0f}s (attempt {attempt}/{max_retries})..."
                )
                time.sleep(wait)
                continue

            # Non-retryable or final attempt
            detail = _extract_error_detail(response)
            raise MLVApiError(
                operation,
                f"HTTP {response.status_code} from {method} {url} — {detail}",
            )

        except requests.exceptions.ConnectionError as exc:
            last_exception = exc
            if attempt < max_retries:
                wait = RETRY_BACKOFF_BASE**attempt
                logger.warning(
                    f"MLV API {operation} connection error: {exc}. "
                    f"Retrying in {wait}s (attempt {attempt}/{max_retries})..."
                )
                time.sleep(wait)
                continue
        except requests.exceptions.Timeout as exc:
            last_exception = exc
            if attempt < max_retries:
                wait = RETRY_BACKOFF_BASE**attempt
                logger.warning(
                    f"MLV API {operation} timed out: {exc}. "
                    f"Retrying in {wait}s (attempt {attempt}/{max_retries})..."
                )
                time.sleep(wait)
                continue
        except MLVApiError:
            raise
        except requests.exceptions.RequestException as exc:
            raise MLVApiError(operation, str(exc)) from exc

    # All retries exhausted
    raise MLVApiError(
        operation,
        f"All {max_retries} attempts failed. Last error: {last_exception}",
    )


def _job_instance_url(
    credentials: FabricSparkCredentials, lakehouse_id: Optional[str], job_instance_id: str
) -> str:
    """Build the URL for a specific job instance (Get Item Job Instance)."""
    lh_id = lakehouse_id or credentials.lakehouseid
    return (
        f"{credentials.endpoint}/workspaces/{credentials.workspaceid}"
        f"/lakehouses/{lh_id}/jobs/instances/{job_instance_id}"
    )


# Terminal job statuses — polling stops when one of these is reached
_TERMINAL_STATUSES = {"Completed", "Failed", "Cancelled", "Deduped"}


def get_job_instance(
    credentials: FabricSparkCredentials,
    job_instance_id: str,
    lakehouse_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Get the status of a specific MLV job instance.

    GET .../jobs/instances/{jobInstanceId}

    Returns the full job instance dict including ``status`` and ``failureReason``.
    """
    url = _job_instance_url(credentials, lakehouse_id, job_instance_id)
    headers = get_headers(credentials)

    response = _request_with_retry(
        "GET",
        url,
        headers,
        operation=f"get MLV job instance {job_instance_id}",
        timeout=credentials.http_timeout,
    )
    return response.json()


def poll_job_instance_until_complete(
    credentials: FabricSparkCredentials,
    job_instance_id: str,
    lakehouse_id: Optional[str] = None,
    deadline: Optional[float] = None,
) -> Dict[str, Any]:
    """Poll a job instance until it reaches a terminal status.

    Uses a wall-clock deadline to prevent runaway polling. If *deadline* is not
    provided, one is computed from ``credentials.statement_timeout``.

    The poll interval starts at ``credentials.poll_statement_wait`` and grows
    adaptively when the MLV API returns sustained 429s, then shrinks back to
    the base interval when the API recovers.

    Returns the final job instance dict on success (``Completed``).
    Raises ``MLVApiError`` if the job fails, is cancelled, or times out.
    """
    base_interval = max(credentials.poll_statement_wait, 2.0)
    poll_interval = base_interval
    max_poll_interval = 60.0
    if deadline is None:
        deadline = time.time() + credentials.statement_timeout

    logger.info(
        f"Polling MLV job instance {job_instance_id} "
        f"(interval={base_interval}s, deadline in {deadline - time.time():.0f}s)..."
    )

    consecutive_429s = 0

    while True:
        if time.time() >= deadline:
            raise MLVApiError(
                "on-demand MLV refresh",
                f"Job {job_instance_id} timed out (wall-clock deadline reached). "
                f"Increase statement_timeout in your profile to wait longer.",
            )

        try:
            job = get_job_instance(credentials, job_instance_id, lakehouse_id)
            consecutive_429s = 0
            poll_interval = base_interval
        except MLVApiError as exc:
            # If the poll GET itself exhausted retries due to sustained 429s,
            # back off adaptively instead of failing the whole refresh.
            if "429" in str(exc) or "RequestBlocked" in str(exc):
                consecutive_429s += 1
                poll_interval = min(
                    base_interval * (2**consecutive_429s) + random.uniform(0, 3),
                    max_poll_interval,
                )
                logger.warning(
                    f"MLV job {job_instance_id} poll throttled "
                    f"(consecutive={consecutive_429s}), "
                    f"backing off to {poll_interval:.0f}s."
                )
                if time.time() + poll_interval >= deadline:
                    raise MLVApiError(
                        "on-demand MLV refresh",
                        f"Job {job_instance_id} timed out during sustained throttle. "
                        f"Increase statement_timeout or reduce concurrency.",
                    ) from exc
                time.sleep(poll_interval)
                continue
            raise

        status = job.get("status", "Unknown")
        failure_reason = job.get("failureReason")

        logger.debug(
            f"MLV job {job_instance_id}: status={status}, remaining={deadline - time.time():.0f}s"
        )

        if status == "Completed":
            logger.info(f"MLV on-demand refresh completed successfully (job {job_instance_id}).")
            return job

        if status in {"Cancelled", "Deduped"}:
            # Fabric returns ``Cancelled``/``Deduped`` when a concurrent (or
            # previously queued) refresh supersedes this one. The underlying
            # lineage is (or will be) refreshed by the other job, so from the
            # caller's perspective this is a successful no-op rather than a
            # failure. Surfacing it as success avoids brittle retry storms when
            # multiple tests in the same lakehouse trigger refreshes in quick
            # succession.
            logger.info(
                f"MLV on-demand refresh superseded by concurrent job "
                f"(job {job_instance_id}, status={status}); treating as success."
            )
            return job

        if status in _TERMINAL_STATUSES:
            detail = failure_reason or f"Job ended with status: {status}"
            raise MLVApiError(
                "on-demand MLV refresh",
                f"Job {job_instance_id} {status}. {detail}",
            )

        time.sleep(poll_interval)


def _is_throttle_failure(failure_reason: Any) -> bool:
    """Return True if a job's failureReason indicates a capacity/throttle issue.

    Checks the structured ``errorCode`` field returned by Fabric, so the check
    is precise and won't accidentally match unrelated error messages.
    """
    if isinstance(failure_reason, dict):
        return failure_reason.get("errorCode", "") in _THROTTLE_ERROR_CODES
    if isinstance(failure_reason, str):
        # Fallback: the failure reason was stringified. Check for known codes.
        return any(code in failure_reason for code in _THROTTLE_ERROR_CODES)
    return False


def run_on_demand_refresh(
    credentials: FabricSparkCredentials,
    lakehouse_id: Optional[str] = None,
    max_retries: int = 6,
) -> Dict[str, Any]:
    """Trigger an immediate refresh of MLV lineage and poll until completion.

    1. POST .../jobs/RefreshMaterializedLakeViews/instances → 202 Accepted
    2. Extract job instance ID from the ``Location`` header
    3. Poll GET .../jobs/instances/{jobInstanceId} until terminal status
    4. Raise ``MLVApiError`` if the job fails or times out

    Transient conditions that trigger a retry of the full POST+poll cycle:
    - ``Cancelled`` / ``Deduped``: concurrent refresh superseded this one.
    - ``MLV_NOT_FOUND`` / ``MLV_LINEAGE_NOT_FOUND``: concurrent worker dropped
      an MLV between POST and job execution.
    - ``Failed`` with a throttle error code (e.g. ``MLV_SPARK_JOB_CAPACITY_THROTTLING``):
      Fabric couldn't allocate Spark capacity; retry after backoff.

    A wall-clock deadline spans the entire retry sequence so the total operation
    time is bounded by ``statement_timeout``.
    """
    url = f"{_base_url(credentials, lakehouse_id)}/instances"
    headers = get_headers(credentials)
    last_err: Optional[MLVApiError] = None

    # Overall deadline for all retries so we never exceed statement_timeout.
    overall_deadline = time.time() + credentials.statement_timeout

    for attempt in range(1, max_retries + 1):
        if time.time() >= overall_deadline:
            raise last_err or MLVApiError(
                "on-demand MLV refresh",
                f"Overall deadline exceeded after {attempt - 1} attempts.",
            )

        logger.info(
            f"Triggering on-demand MLV refresh: POST {url} (attempt {attempt}/{max_retries})"
        )
        response = _request_with_retry(
            "POST",
            url,
            headers,
            operation="on-demand MLV refresh",
            timeout=credentials.http_timeout,
        )
        location = response.headers.get("Location", "")
        logger.info(f"On-demand MLV refresh triggered. Job instance URL: {location}")

        job_instance_id = location.rstrip("/").rsplit("/", 1)[-1] if location else ""
        if not job_instance_id:
            raise MLVApiError(
                "on-demand MLV refresh",
                f"Could not extract job instance ID from Location header. Location: '{location}'",
            )

        try:
            return poll_job_instance_until_complete(
                credentials, job_instance_id, lakehouse_id, deadline=overall_deadline
            )
        except MLVApiError as err:
            msg = str(err)
            # Determine if this is a transient failure worth retrying.
            transient = (
                "Cancelled" in msg
                or "Deduped" in msg
                or "MLV_NOT_FOUND" in msg
                or "MLV_LINEAGE_NOT_FOUND" in msg
            )
            # Also retry Failed jobs when Fabric reports a throttle error code.
            if not transient and "Failed" in msg:
                # Try to extract the structured failureReason from the error.
                transient = any(code in msg for code in _THROTTLE_ERROR_CODES)

            if not transient or attempt >= max_retries:
                raise
            last_err = err
            # Jittered exponential backoff; capped at 120s to survive sustained throttle.
            wait = min(2 ** (attempt - 1) * 4.0, 120.0) + random.uniform(0, 5.0)
            remaining = overall_deadline - time.time()
            if wait > remaining:
                raise  # Not enough time for another attempt
            logger.warning(
                f"MLV refresh transient failure (attempt {attempt}/{max_retries}): "
                f"{msg}. Retrying in {wait:.1f}s (remaining budget: {remaining:.0f}s)."
            )
            time.sleep(wait)

    # Should be unreachable: loop always returns or raises.
    raise last_err or MLVApiError("on-demand MLV refresh", "exhausted retries")


def list_schedules(
    credentials: FabricSparkCredentials,
    lakehouse_id: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """List all MLV refresh schedules for a lakehouse.

    GET .../jobs/RefreshMaterializedLakeViews/schedules

    Raises ``MLVApiError`` on failure after retries.
    """
    url = f"{_base_url(credentials, lakehouse_id)}/schedules"
    headers = get_headers(credentials)
    logger.debug(f"Listing MLV schedules: GET {url}")

    response = _request_with_retry(
        "GET", url, headers, operation="list MLV schedules", timeout=credentials.http_timeout
    )

    data = response.json()
    return data.get("value", [])


def create_schedule(
    credentials: FabricSparkCredentials,
    schedule_config: Dict[str, Any],
    lakehouse_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Create a new MLV refresh schedule.

    POST .../jobs/RefreshMaterializedLakeViews/schedules

    Parameters
    ----------
    schedule_config : dict
        Schedule configuration matching the Fabric API schema. Example::

            {
                "enabled": True,
                "configuration": {
                    "startDateTime": "2026-04-10T00:00:00",
                    "endDateTime": "2026-12-31T23:59:59",
                    "localTimeZoneId": "Central Standard Time",
                    "type": "Cron",
                    "interval": 10
                }
            }

        Supported schedule types: ``Cron`` (with ``interval`` in minutes),
        ``Daily`` (with ``times``), ``Weekly`` (with ``weekdays`` and ``times``).

    Raises ``MLVApiError`` on failure after retries.
    """
    # Fabric API requires endDateTime — fail fast if not provided.
    cfg = schedule_config.get("configuration", {})
    if "endDateTime" not in cfg:
        raise MLVApiError(
            "create MLV schedule",
            "The 'endDateTime' field is required in the schedule configuration. "
            "Add it to your mlv_schedule config, e.g.: "
            '"endDateTime": "2027-12-31T23:59:59"',
        )

    url = f"{_base_url(credentials, lakehouse_id)}/schedules"
    headers = get_headers(credentials)
    logger.info(f"Creating MLV schedule: POST {url}")

    response = _request_with_retry(
        "POST",
        url,
        headers,
        operation="create MLV schedule",
        timeout=credentials.http_timeout,
        json_body=schedule_config,
    )

    result = response.json()
    logger.info(f"MLV schedule created: {result.get('id')}")
    return result


def update_schedule(
    credentials: FabricSparkCredentials,
    schedule_id: str,
    schedule_config: Dict[str, Any],
    lakehouse_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Update an existing MLV refresh schedule.

    PATCH .../jobs/RefreshMaterializedLakeViews/schedules/{scheduleId}

    Raises ``MLVApiError`` on failure after retries.
    """
    url = f"{_base_url(credentials, lakehouse_id)}/schedules/{schedule_id}"
    headers = get_headers(credentials)
    logger.info(f"Updating MLV schedule {schedule_id}: PATCH {url}")

    response = _request_with_retry(
        "PATCH",
        url,
        headers,
        operation=f"update MLV schedule {schedule_id}",
        timeout=credentials.http_timeout,
        json_body=schedule_config,
    )

    result = response.json()
    logger.info(f"MLV schedule updated: {result.get('id')}")
    return result


def delete_schedule(
    credentials: FabricSparkCredentials,
    schedule_id: str,
    lakehouse_id: Optional[str] = None,
) -> None:
    """Delete an existing MLV refresh schedule.

    DELETE .../jobs/RefreshMaterializedLakeViews/schedules/{scheduleId}

    Raises ``MLVApiError`` on failure after retries.
    """
    url = f"{_base_url(credentials, lakehouse_id)}/schedules/{schedule_id}"
    headers = get_headers(credentials)
    logger.info(f"Deleting MLV schedule {schedule_id}: DELETE {url}")

    _request_with_retry(
        "DELETE",
        url,
        headers,
        operation=f"delete MLV schedule {schedule_id}",
        timeout=credentials.http_timeout,
    )
    logger.info(f"MLV schedule {schedule_id} deleted.")


def create_or_update_schedule(
    credentials: FabricSparkCredentials,
    schedule_config: Dict[str, Any],
    lakehouse_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Create a new schedule or update the existing one.

    MLV supports only one active schedule per lakehouse lineage.
    This function lists existing schedules and updates the first one found,
    or creates a new one if none exist.

    Raises ``MLVApiError`` on failure after retries.
    """
    existing = list_schedules(credentials, lakehouse_id)
    if existing:
        schedule_id = existing[0]["id"]
        logger.info(f"Existing MLV schedule found ({schedule_id}), updating.")
        return update_schedule(credentials, schedule_id, schedule_config, lakehouse_id)
    else:
        logger.info("No existing MLV schedule found, creating new one.")
        return create_schedule(credentials, schedule_config, lakehouse_id)
