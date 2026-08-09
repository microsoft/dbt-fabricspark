"""Shared HTTP helpers for Fabric REST clients."""

from __future__ import annotations

import math
from datetime import datetime, timezone

import requests

# Fabric answers a 429 with hints as long as 7199s. Obeying that literally
# stalls a run for hours, and the quota often frees up well before it expires,
# so the wait is capped and the endpoint re-probed instead. Also keeps a
# non-finite or absurd header from parking every call in the process forever.
MAX_RETRY_AFTER = 120.0


def parse_retry_after(response: requests.Response) -> float:
    """Extract wait time (seconds) from a 429 response.

    Checks the ``Retry-After`` header first, then falls back to the
    Fabric-specific ``until: <timestamp>`` pattern in the response body
    (e.g. ``"...until: 4/17/2026 12:22:35 PM (UTC)"``). Returns 0 if no
    usable hint is found; non-finite, negative and absurdly large hints are
    treated as absent so a malformed header cannot stall or spin the run.
    """
    header = response.headers.get("Retry-After", "")
    if header:
        try:
            return _sanitize(float(header))
        except ValueError:
            pass
    try:
        body = response.json()
        msg = body.get("message", "")
        if "until:" in msg:
            ts_str = msg.split("until:")[1].strip().rstrip(")")
            ts_str = ts_str.replace("(UTC", "").strip()
            target = datetime.strptime(ts_str, "%m/%d/%Y %I:%M:%S %p").replace(tzinfo=timezone.utc)
            delta = (target - datetime.now(timezone.utc)).total_seconds()
            return _sanitize(delta)
    except Exception:
        pass
    return 0


def _sanitize(seconds: float) -> float:
    if not math.isfinite(seconds) or seconds <= 0:
        return 0
    return min(seconds, MAX_RETRY_AFTER)
