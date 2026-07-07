from __future__ import annotations

from collections.abc import Mapping
from contextlib import contextmanager
import socket

import requests


DEFAULT_TIMEOUT_SECONDS = 60.0
DEFAULT_USER_AGENT = "free-ticker-database-maintenance/1.0"


def session_with_user_agent(user_agent: str = DEFAULT_USER_AGENT) -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": user_agent})
    return session


def get_text(
    url: str,
    *,
    session: requests.Session | None = None,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
    headers: Mapping[str, str] | None = None,
) -> str:
    client = session or session_with_user_agent()
    response = client.get(url, timeout=timeout, headers=dict(headers or {}))
    response.raise_for_status()
    return response.text


@contextmanager
def socket_timeout(seconds: float):
    previous = socket.getdefaulttimeout()
    socket.setdefaulttimeout(seconds)
    try:
        yield
    finally:
        socket.setdefaulttimeout(previous)
