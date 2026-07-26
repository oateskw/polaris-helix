from unittest.mock import AsyncMock, MagicMock

import pytest

import helix_optimizer

pytestmark = pytest.mark.anyio


async def test_optimizer_does_not_construct_anthropic_client_when_circuit_blocked(
    monkeypatch,
):
    circuit_check = AsyncMock(return_value=False)
    anthropic_client = MagicMock()
    monkeypatch.setattr(helix_optimizer, "is_vendor_circuit_healthy", circuit_check)
    monkeypatch.setattr(helix_optimizer, "AsyncAnthropic", anthropic_client)
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-key")
    db_pool = object()

    await helix_optimizer.run_daily_prompt_optimization(db_pool, force=True)

    circuit_check.assert_awaited_once_with(db_pool, "Anthropic")
    anthropic_client.assert_not_called()
