import pytest

from services.telemetry import is_vendor_circuit_healthy

pytestmark = pytest.mark.anyio


@pytest.mark.parametrize(
    ("record", "expected"),
    [
        ({"is_circuit_broken": False}, True),
        ({"is_circuit_broken": True}, False),
        ({"is_circuit_broken": None}, False),
        ({}, False),
        (None, False),
        ((False,), False),
    ],
)
async def test_is_vendor_circuit_healthy_requires_explicit_unbroken_state(
    db_pool_factory, record, expected
):
    pool, _, cursors = db_pool_factory(fetchone_sequence=[record])

    result = await is_vendor_circuit_healthy(pool, "aNtHrOpIc")

    assert result is expected
    assert cursors[0].executed[0][1] == ("aNtHrOpIc",)
    assert "LOWER(vendor_name) = LOWER(%s)" in cursors[0].executed[0][0]


async def test_is_vendor_circuit_healthy_rejects_blank_vendor_without_query(
    db_pool_factory,
):
    pool, _, cursors = db_pool_factory()

    assert await is_vendor_circuit_healthy(pool, "  ") is False
    assert cursors[0].executed == []


async def test_is_vendor_circuit_healthy_fails_closed_on_database_error(
    db_pool_factory,
):
    pool, _, _ = db_pool_factory(fail_execute=RuntimeError("database unavailable"))

    assert await is_vendor_circuit_healthy(pool, "Anthropic") is False
