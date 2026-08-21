"""Tests for the shared HTTP retry helper."""

from unittest.mock import MagicMock

import pytest

from msdev_kit.http import request_with_retry


def _response(status_code: int, retry_after: str | None = None):
    response = MagicMock()
    response.status_code = status_code
    response.headers = {} if retry_after is None else {'Retry-After': retry_after}
    return response


def test_normalizes_http_method_and_forwards_request_arguments():
    request = MagicMock(return_value=_response(200))

    response = request_with_retry(
        'post',
        'https://example.test/items',
        request_func=request,
        headers={'Authorization': 'Bearer token'},
        json={'name': 'item'},
        timeout=30,
    )

    assert response.status_code == 200
    request.assert_called_once_with(
        'POST',
        'https://example.test/items',
        headers={'Authorization': 'Bearer token'},
        json={'name': 'item'},
        timeout=30,
    )


def test_retries_429_using_retry_after_then_returns_response():
    request = MagicMock(side_effect=[_response(429, '2'), _response(204)])
    sleep = MagicMock()
    on_rate_limit = MagicMock()

    response = request_with_retry(
        'DELETE',
        'https://example.test/items/1',
        request_func=request,
        sleep=sleep,
        on_rate_limit=on_rate_limit,
    )

    assert response.status_code == 204
    assert request.call_count == 2
    sleep.assert_called_once_with(2)
    on_rate_limit.assert_called_once_with(2)


def test_uses_exponential_backoff_when_retry_after_is_missing():
    request = MagicMock(side_effect=[_response(429), _response(429), _response(200)])
    sleep = MagicMock()

    response = request_with_retry('GET', 'https://example.test/items', request_func=request, sleep=sleep)

    assert response.status_code == 200
    assert sleep.call_args_list == [((1,),), ((2,),)]


@pytest.mark.parametrize('method', ['', '   ', None])
def test_rejects_empty_http_method(method):
    with pytest.raises(ValueError, match='method'):
        request_with_retry(method, 'https://example.test/items')
