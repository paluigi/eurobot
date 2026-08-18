"""Bounded tenacity retry helpers shared by fetchers and Windmill scripts.

Sensible defaults: 3 attempts (initial + 2 retries), exponential backoff
2→4→8s, only for transient network/5xx errors. Hard failures (4xx auth
errors, config errors) are never retried. The cap guarantees no infinite
loops — after the last attempt the exception is re-raised.
"""

from __future__ import annotations

import logging

import requests
from tenacity import (
    Retrying,
    before_sleep_log,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

logger = logging.getLogger(__name__)

# Default cap for transient retries (initial attempt + 2 retries).
TRANSIENT_MAX_ATTEMPTS = 3

# HTTP status codes worth retrying: server errors + rate limiting + timeouts.
_RETRYABLE_STATUS = {429, 500, 502, 503, 504}


class TransientError(Exception):
    """Wraps a failure that is worth retrying (network, 5xx, 429)."""


def _is_transient(exc: BaseException) -> bool:
    """True for network-level failures and retryable HTTP statuses."""
    if isinstance(exc, TransientError):
        return True
    if isinstance(exc, (requests.ConnectionError, requests.Timeout)):
        return True
    if isinstance(exc, requests.HTTPError):
        status = getattr(exc.response, "status_code", None)
        return status in _RETRYABLE_STATUS
    return False


def transient_retryer(
    max_attempts: int = TRANSIENT_MAX_ATTEMPTS,
    multiplier: float = 1,
    min_wait: float = 2,
    max_wait: float = 30,
):
    """Build a tenacity Retrying instance for transient failures.

    Bounded by ``max_attempts`` — never loops forever.
    """
    return Retrying(
        stop=stop_after_attempt(max_attempts),
        wait=wait_exponential(multiplier=multiplier, min=min_wait, max=max_wait),
        retry=retry_if_exception(_is_transient),
        before_sleep=before_sleep_log(logger, logging.WARNING),
        reraise=True,
    )


def call_with_retries(
    func, *args, max_attempts: int = TRANSIENT_MAX_ATTEMPTS, **kwargs
):
    """Run ``func(*args, **kwargs)`` with bounded transient-error retries.

    Non-transient exceptions propagate immediately on the first attempt.
    """
    return transient_retryer(max_attempts=max_attempts)(func, *args, **kwargs)
