"""Shared HTTP request helpers for Microsoft service clients."""

from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from threading import Lock
import time
from typing import Callable, Optional

import requests


class RequestPacer:
    """Thread-safe minimum interval between requests from one client instance."""

    def __init__(self, requests_per_minute: float):
        if requests_per_minute <= 0:
            raise ValueError('requests_per_minute must be greater than zero.')
        self._interval = 60 / requests_per_minute
        self._lock = Lock()
        self._last_request_at = 0.0

    def wait(self) -> None:
        """Wait until the next request is allowed."""
        with self._lock:
            elapsed = time.monotonic() - self._last_request_at
            if elapsed < self._interval:
                time.sleep(self._interval - elapsed)
            self._last_request_at = time.monotonic()


def request_with_retry(
    method: str,
    url: str,
    *,
    max_retries: int = 3,
    pacer: Optional[RequestPacer] = None,
    request_func: Callable[..., requests.Response] = requests.request,
    sleep: Optional[Callable[[float], None]] = None,
    log_retries: bool = True,
    on_rate_limit: Optional[Callable[[float], None]] = None,
    **kwargs,
) -> requests.Response:
    """Send an HTTP request and retry HTTP 429 responses.

    ``method`` accepts any HTTP verb supported by ``requests``. When a 429
    includes ``Retry-After``, its delta-seconds or HTTP date value is honored.
    Otherwise retries back off exponentially from one second. A shared
    ``RequestPacer`` can be supplied by callers that issue concurrent work.
    """
    normalized_method = method.strip().upper() if isinstance(method, str) else ''
    if not normalized_method:
        raise ValueError('method must be a non-empty HTTP method string.')
    if max_retries < 0:
        raise ValueError('max_retries must be zero or greater.')
    if sleep is None:
        sleep = time.sleep

    for attempt in range(max_retries + 1):
        if pacer is not None:
            pacer.wait()

        response = request_func(normalized_method, url, **kwargs)
        if response.status_code != 429:
            return response

        if attempt < max_retries:
            retry_delay = _get_retry_delay(response, attempt)
            if on_rate_limit is not None:
                on_rate_limit(retry_delay)
            if log_retries:
                print(
                    f'  Rate limited (429). Retrying in {retry_delay:g}s... '
                    f'(attempt {attempt + 1}/{max_retries})'
                )
            sleep(retry_delay)

    return response


def _get_retry_delay(response: requests.Response, attempt: int) -> float:
    """Return a Retry-After delay, falling back to exponential backoff."""
    retry_after = response.headers.get('Retry-After')
    try:
        return max(float(retry_after), 0)
    except (TypeError, ValueError):
        try:
            retry_after_at = parsedate_to_datetime(retry_after)
            if retry_after_at.tzinfo is None:
                retry_after_at = retry_after_at.replace(tzinfo=timezone.utc)
            return max((retry_after_at - datetime.now(timezone.utc)).total_seconds(), 0)
        except (TypeError, ValueError):
            return min(2 ** attempt, 60)
