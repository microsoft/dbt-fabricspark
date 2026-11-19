import logging
import os
import time
import uuid
from contextlib import contextmanager, nullcontext
from contextvars import ContextVar
from datetime import datetime, timezone
from functools import wraps
from typing import Any, Callable, Iterator, Mapping

_Capture = tuple[Callable[[dict[str, Any]], None], Mapping[str, Any], str]
_capture: ContextVar[_Capture | None] = ContextVar("fabricspark_span_capture", default=None)
_parent_span_id: ContextVar[str | None] = ContextVar("fabricspark_parent_span_id", default=None)
_clock_instance = uuid.uuid4().hex
_logger = logging.getLogger(__name__)
_disabled_span = nullcontext()


@contextmanager
def capture_adapter_spans(
    emit: Callable[[dict[str, Any]], None],
    context: Mapping[str, Any],
) -> Iterator[None]:
    """Bind the runner's span sink and request identity on the executing dbt worker."""
    clock_id = context.get("clock_domain_id") or f"python:{os.getpid()}:{_clock_instance}"
    capture = _capture.set((emit, dict(context), clock_id))
    parent = _parent_span_id.set(context.get("parent_span_id"))
    try:
        yield
    finally:
        _parent_span_id.reset(parent)
        _capture.reset(capture)


def adapter_span(
    operation: str, *, work_class: str = "metadata", spark_job_group_id: str | None = None
):
    capture = _capture.get()
    if capture is None:
        return _disabled_span
    return _adapter_span(capture, operation, work_class, spark_job_group_id)


@contextmanager
def _adapter_span(
    capture: _Capture, operation: str, work_class: str, spark_job_group_id: str | None
) -> Iterator[None]:
    emit, context, clock_domain_id = capture
    span_id = uuid.uuid4().hex
    parent_span_id = _parent_span_id.get()
    token = _parent_span_id.set(span_id)
    utc = datetime.now(timezone.utc).isoformat()
    start_ns = time.perf_counter_ns()
    status = "success"
    try:
        yield
    except KeyboardInterrupt:
        status = "cancelled"
        raise
    except BaseException:
        status = "failed"
        raise
    finally:
        duration_ns = time.perf_counter_ns() - start_ns
        _parent_span_id.reset(token)
        record = {
            **context,
            "component": "adapter",
            "scope": "model" if context.get("dbt_node_id") else "run",
            "operation": operation,
            "span_id": span_id,
            "parent_span_id": parent_span_id,
            "status": status,
            "clock_domain_id": clock_domain_id,
            "start_monotonic_ns": start_ns,
            "duration_ns": duration_ns,
            "start_utc": utc,
            "utc_uncertainty_ns": None,
            "attempt": context.get("attempt", 1),
            "work_class": work_class,
        }
        if spark_job_group_id is not None:
            record["spark_job_group_id"] = spark_job_group_id
        try:
            emit(record)
        except Exception:
            _logger.exception("Could not export adapter span for %s", operation)


def measure_adapter_phase(operation: str, *, work_class: str = "metadata"):
    def decorate(function):
        @wraps(function)
        def measured(*args, **kwargs):
            if _capture.get() is None:
                return function(*args, **kwargs)
            with adapter_span(operation, work_class=work_class):
                return function(*args, **kwargs)

        return measured

    return decorate
