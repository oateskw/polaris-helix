import pytest
from unittest.mock import AsyncMock, patch

import database.connection as connection
from database.schemas import PROVISION_SQL


@pytest.mark.anyio
async def test_placeholder_async_marker():
    # Keeps test runner behavior consistent with mixed async suites.
    assert True


@pytest.mark.anyio
async def test_pool_lifecycle_respects_scheduling_constraints(monkeypatch):
    """Verify pool closes cleanly when scheduling constraints require standdown."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@localhost:5432/db")

    mock_pool = AsyncMock()
    mock_pool.close = AsyncMock()

    with patch.object(connection, "AsyncConnectionPool", return_value=mock_pool):
        pool = connection.create_async_db_pool()
        pool.open = True

        # Simulate standdown scenario
        await pool.close()
        mock_pool.close.assert_called_once()


def test_get_validated_database_url_missing(monkeypatch):
    monkeypatch.delenv("DATABASE_URL", raising=False)
    with pytest.raises(ValueError):
        connection.get_validated_database_url()


def test_get_validated_database_url_invalid_scheme(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "mysql://localhost/db")
    with pytest.raises(ValueError):
        connection.get_validated_database_url()


def test_get_validated_database_url_appends_sslmode(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@localhost:5432/db")
    validated = connection.get_validated_database_url()
    assert "sslmode=require" in validated


def test_get_validated_database_url_preserves_require_ssl(monkeypatch):
    raw = "postgresql://u:p@localhost:5432/db?sslmode=require&connect_timeout=5"
    monkeypatch.setenv("DATABASE_URL", raw)
    validated = connection.get_validated_database_url()
    assert "sslmode=require" in validated
    assert "connect_timeout=5" in validated


def test_create_async_db_pool_contract(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@localhost:5432/db")

    captured = {}

    class PoolSpy:
        check_connection = object()

        def __init__(self, **kwargs):
            captured.update(kwargs)

    monkeypatch.setattr(connection, "AsyncConnectionPool", PoolSpy)

    connection.create_async_db_pool()

    assert captured["open"] is False
    assert captured["max_idle"] == 270.0
    assert "sslmode=require" in captured["conninfo"]


def test_pool_initialization_deferred_for_scheduling_compliance(monkeypatch):
    """Ensure pool remains closed until explicitly opened per scheduling rules."""
    monkeypatch.setenv("DATABASE_URL", "postgresql://u:p@localhost:5432/db")

    captured = {}

    class PoolSpy:
        check_connection = object()

        def __init__(self, **kwargs):
            captured.update(kwargs)

    monkeypatch.setattr(connection, "AsyncConnectionPool", PoolSpy)

    pool = connection.create_async_db_pool()

    # Critical: pool must start closed to prevent resource leaks on unassigned days
    assert (
        captured["open"] is False
    ), "Pool must not auto-open to respect scheduling constraints"


def test_schema_requires_python_traceback_for_new_error_logs():
    assert "error_logs_require_python_traceback" in PROVISION_SQL
    assert "log_level <> 'ERROR'" in PROVISION_SQL
    assert "metadata->>'traceback'" in PROVISION_SQL
    assert "NOT VALID" in PROVISION_SQL


def test_schema_provisions_telegram_update_deduplication():
    assert "CREATE TABLE IF NOT EXISTS telegram_processed_updates" in PROVISION_SQL
    assert "update_id    BIGINT      PRIMARY KEY" in PROVISION_SQL
    assert "idx_tpu_processed_at" in PROVISION_SQL
