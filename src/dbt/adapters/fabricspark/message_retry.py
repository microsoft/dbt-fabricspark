from __future__ import annotations

import math
import re
from dataclasses import dataclass
from typing import Any

_REGEX_PREFIX = "re:"


def _compile_patterns(raw_patterns: list[Any]) -> list[Any]:
    compiled: list[Any] = []
    for entry in raw_patterns:
        if not isinstance(entry, str) or not entry:
            raise ValueError(f"job retry patterns must be non-empty strings; got {entry!r}")
        if entry.startswith(_REGEX_PREFIX):
            pattern_text = entry[len(_REGEX_PREFIX) :]
            if not pattern_text:
                raise ValueError(f"job retry regex {entry!r} is empty after 're:'")
            compiled.append(re.compile(pattern_text))
        else:
            compiled.append(entry)
    return compiled


@dataclass(frozen=True)
class MessageRetryPolicy:
    patterns: tuple[Any, ...] = ()
    max_retries: int = 0
    initial_wait_seconds: float = 30.0
    max_wait_seconds: float = 300.0

    @classmethod
    def disabled(cls) -> MessageRetryPolicy:
        return cls()

    @classmethod
    def for_job_retry(cls, credentials: Any) -> MessageRetryPolicy:
        if not bool(getattr(credentials, "enable_job_retry", True)):
            return cls.disabled()

        patterns = _compile_patterns(getattr(credentials, "job_retry_on_messages", None) or [])
        max_attempts = int(getattr(credentials, "job_retry_max_attempts", 3))
        initial_wait = float(getattr(credentials, "job_retry_initial_wait_seconds", 30.0))
        max_wait = float(getattr(credentials, "job_retry_max_wait_seconds", 300.0))

        if max_attempts < 1:
            raise ValueError(f"job_retry_max_attempts must be >= 1; got {max_attempts}")
        if not math.isfinite(initial_wait) or initial_wait <= 0:
            raise ValueError(
                f"job_retry_initial_wait_seconds must be finite and > 0; got {initial_wait}"
            )
        if not math.isfinite(max_wait) or max_wait <= 0:
            raise ValueError(f"job_retry_max_wait_seconds must be finite and > 0; got {max_wait}")
        if initial_wait > max_wait:
            raise ValueError(
                "job_retry_initial_wait_seconds must be <= "
                f"job_retry_max_wait_seconds; got {initial_wait} > {max_wait}"
            )

        return cls(
            patterns=tuple(patterns),
            max_retries=max_attempts - 1,
            initial_wait_seconds=initial_wait,
            max_wait_seconds=max_wait,
        )

    @property
    def enabled(self) -> bool:
        return bool(self.patterns) and self.max_retries > 0

    def matches(self, exc: BaseException) -> str | None:
        text = str(exc)
        for pattern in self.patterns:
            if isinstance(pattern, re.Pattern):
                if pattern.search(text):
                    return f"re:{pattern.pattern}"
            elif pattern in text:
                return pattern
        return None

    def delay_for_attempt(self, attempt: int) -> float:
        normalized_attempt = max(attempt, 1)
        return min(
            self.initial_wait_seconds * (2 ** (normalized_attempt - 1)),
            self.max_wait_seconds,
        )
