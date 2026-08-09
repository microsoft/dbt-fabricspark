from dbt_common.exceptions import DbtRuntimeError


class AmbiguousSubmissionError(DbtRuntimeError):
    """A statement submit failed without proving whether Fabric accepted it.

    The original POST may already be executing side-effecting DDL/DML, so this
    failure must never be retried by any layer — a resubmit could apply the
    statement twice.
    """
