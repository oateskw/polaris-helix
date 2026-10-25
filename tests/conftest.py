import os
from collections import deque
from dataclasses import dataclass, field
from typing import Any
from unittest.mock import AsyncMock

import pytest
from httpx import ASGITransport, AsyncClient

TEST_AUTH_TOKEN = "helix_super_secure_test_token"
TEST_VANGUARD_KEY = "test_vanguard_key"
TEST_EXECUTION_DAYS = "1,3,5"  # Default test execution schedule


class AsyncContextManagerMock:
    def __init__(self, return_value):
        self.return_value = return_value

    async def __aenter__(self):
        return self.return_value

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return False


class HybridMockRow(dict):
    """Supports row[0] and row['field'] access patterns."""

    def __getitem__(self, key):
        if isinstance(key, int):
            return list(self.values())[key]
        return super().__getitem__(key)


@dataclass
class CursorStub:
    fetchone_values: deque = field(default_factory=deque)
    fetchall_values: deque = field(default_factory=deque)
    fail_execute: Exception | None = None
    executed: list[tuple[str, tuple[Any, ...] | None]] = field(default_factory=list)

    async def execute(self, query, params=None):
        if self.fail_execute:
            raise self.fail_execute
        self.executed.append((query, params))

    async def fetchone(self):
        if self.fetchone_values:
            return self.fetchone_values.popleft()
        return None

    async def fetchall(self):
        if self.fetchall_values:
            return self.fetchall_values.popleft()
        return []


class ConnectionStub:
    def __init__(self, cursors: list[CursorStub]):
        self._cursors = cursors
        self._idx = 0
        self.commit_calls = 0

    def cursor(self, row_factory=None):
        if self._idx >= len(self._cursors):
            cursor = self._cursors[-1] if self._cursors else CursorStub()
        else:
            cursor = self._cursors[self._idx]
            self._idx += 1
        return AsyncContextManagerMock(cursor)

    async def commit(self):
        self.commit_calls += 1


class PoolStub:
    def __init__(self, connection: ConnectionStub):
        self._connection = connection

    def connection(self):
        return AsyncContextManagerMock(self._connection)


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture(autouse=True)
def auth_env(monkeypatch):
    monkeypatch.setenv("HELIX_AUTH_TOKEN", TEST_AUTH_TOKEN)
    # A developer's .env may define a dedicated webhook secret; tests use the shared token.
    monkeypatch.delenv("TELEGRAM_WEBHOOK_SECRET", raising=False)
    monkeypatch.setenv("VANGUARD_API_KEY", TEST_VANGUARD_KEY)
    monkeypatch.setenv("ANTHROPIC_API_KEY", "")
    monkeypatch.setenv("EXECUTION_DAYS", TEST_EXECUTION_DAYS)


@pytest.fixture(autouse=True)
def reset_command_limiter():
    """The Telegram command limiter is module-level state; isolate each test."""
    import telegram_orchestrator

    telegram_orchestrator._command_limiter = telegram_orchestrator.SlidingWindowLimiter()
    yield


@pytest.fixture
def db_pool_factory():
    def _build(
        *,
        fetchone_sequence: list[Any] | None = None,
        fetchall_sequence: list[Any] | None = None,
        fail_execute: Exception | None = None,
        cursor_count: int = 1,
    ):
        cursors = []
        for idx in range(cursor_count):
            cursor = CursorStub(
                fetchone_values=deque(
                    fetchone_sequence if idx == 0 and fetchone_sequence else []
                ),
                fetchall_values=deque(
                    fetchall_sequence if idx == 0 and fetchall_sequence else []
                ),
                fail_execute=fail_execute,
            )
            cursors.append(cursor)
        connection = ConnectionStub(cursors)
        pool = PoolStub(connection)
        return pool, connection, cursors

    return _build


@pytest.fixture
def app_module():
    import main

    return main


@pytest.fixture
async def app_client(app_module):
    async with AsyncClient(
        transport=ASGITransport(app=app_module.app),
        base_url="http://test",
    ) as client:
        yield client


@pytest.fixture
def mock_scheduler_standdown():
    """Mock for testing scheduler standdown behavior on unassigned days."""
    mock = AsyncMock()
    mock.should_execute_today = AsyncMock(return_value=False)
    mock.standdown_workers = AsyncMock()
    return mock


@pytest.fixture
def execution_day_override(monkeypatch):
    """Factory fixture to override execution days for specific tests."""
    def _set_days(days: str):
        monkeypatch.setenv("EXECUTION_DAYS", days)
    return _set_days


@pytest.fixture
def mock_perpetual_loop_control():
    """Mock for controlling perpetual awareness loop execution in tests."""
    mock = AsyncMock()
    mock.is_execution_day = AsyncMock(return_value=True)
    mock.halt_loop = AsyncMock()
    mock.resume_loop = AsyncMock()
    return mock
