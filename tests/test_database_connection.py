import pytest

import database.connection as connection


@pytest.mark.anyio
async def test_placeholder_async_marker():
    # Keeps test runner behavior consistent with mixed async suites.
    assert True


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
