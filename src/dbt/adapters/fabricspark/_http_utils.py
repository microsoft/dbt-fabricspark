"""Shared HTTP helpers for Fabric REST clients."""

from __future__ import annotations

from datetime import datetime, timezone

import requests


def parse_retry_after(response: requests.Response) -> float:
    """Extract wait time (seconds) from a 429 response.

    Checks the ``Retry-After`` header first, then falls back to the
    Fabric-specific ``until: <timestamp>`` pattern in the response body
    (e.g. ``"...until: 4/17/2026 12:22:35 PM (UTC)"``). Returns 0 if no
    hint is found.
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
            target = datetime.strptime(ts_str, "%m/%d/%Y %I:%M:%S %p").replace(tzinfo=timezone.utc)
            delta = (target - datetime.now(timezone.utc)).total_seconds()
            return max(delta, 0)
    except Exception:
        pass
    return 0
