from types import SimpleNamespace

import pytest

from dbt.adapters.fabricspark.message_retry import MessageRetryPolicy


def _credentials(**overrides):
    values = {
        "enable_job_retry": True,
        "job_retry_on_messages": [],
        "job_retry_max_attempts": 3,
        "job_retry_initial_wait_seconds": 30.0,
        "job_retry_max_wait_seconds": 300.0,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def test_empty_patterns_disable_retry() -> None:
    assert not MessageRetryPolicy.for_job_retry(_credentials()).enabled


def test_disabled_flag_disables_retry() -> None:
    policy = MessageRetryPolicy.for_job_retry(
        _credentials(enable_job_retry=False, job_retry_on_messages=["retry me"])
    )
    assert not policy.enabled


def test_regex_and_substring_matching() -> None:
    policy = MessageRetryPolicy.for_job_retry(
        _credentials(
            job_retry_on_messages=[
                "plain transient",
                "re:ServerBusy.*alterTable",
            ]
        )
    )
    assert policy.matches(RuntimeError("plain transient error")) == "plain transient"
    assert (
        policy.matches(RuntimeError("ServerBusy during alterTable")) == "re:ServerBusy.*alterTable"
    )
    assert policy.matches(RuntimeError("syntax error")) is None


def test_attempts_and_backoff() -> None:
    policy = MessageRetryPolicy.for_job_retry(
        _credentials(
            job_retry_max_attempts=3,
            job_retry_initial_wait_seconds=5,
            job_retry_max_wait_seconds=8,
        )
    )
    assert policy.max_retries == 2
    assert policy.delay_for_attempt(1) == 5
    assert policy.delay_for_attempt(2) == 8
    assert policy.delay_for_attempt(3) == 8


@pytest.mark.parametrize(
    "overrides, message",
    [
        ({"job_retry_max_attempts": 0}, "job_retry_max_attempts"),
        ({"job_retry_initial_wait_seconds": 0}, "job_retry_initial_wait_seconds"),
        ({"job_retry_initial_wait_seconds": float("nan")}, "finite"),
        ({"job_retry_max_wait_seconds": 0}, "job_retry_max_wait_seconds"),
        ({"job_retry_max_wait_seconds": float("inf")}, "finite"),
        (
            {
                "job_retry_initial_wait_seconds": 20,
                "job_retry_max_wait_seconds": 10,
            },
            "must be <=",
        ),
        ({"job_retry_on_messages": ["re:"]}, "empty after"),
    ],
)
def test_invalid_policy_configuration(overrides, message) -> None:
    with pytest.raises(ValueError, match=message):
        MessageRetryPolicy.for_job_retry(_credentials(**overrides))
