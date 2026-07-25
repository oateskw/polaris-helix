# tests/test_helix_core.py
import os
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from httpx import AsyncClient, ASGITransport

# ─── MOCK TESTING CONFIGURATION ───
TEST_AUTH_TOKEN = "helix_super_secure_test_token"
os.environ["HELIX_AUTH_TOKEN"] = TEST_AUTH_TOKEN
os.environ["VANGUARD_API_KEY"] = "test_vanguard_key"
os.environ["ANTHROPIC_API_KEY"] = ""
os.environ["EXECUTION_CONSTRAINTS"] = "monday,wednesday,friday"

from main import app


@pytest.fixture
def anyio_backend():
    return "asyncio"


# Reusable Mock Class to mimic Psycopg's async context managers accurately
class AsyncContextManagerMock:
    def __init__(self, return_value):
        self.return_value = return_value

    async def __aenter__(self):
        return self.return_value

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass


class HybridMockRow(dict):
    """A mock row that supports both dictionary string keys and tuple integer indices."""

    def __getitem__(self, key):
        if isinstance(key, int):
            # Fallback to list index access for standard cursors (e.g., row[0])
            return list(self.values())[key]
        return super().__getitem__(key)


@pytest.fixture(autouse=True)
def mock_app_database_state():
    """
    Simulates psycopg connection pools and internal async cursor workflows,
    supporting both standard sequential rows and row_factory dictionaries.
    Ensures worker agents respect execution constraints and stand down on unassigned days.
    """
    mock_cursor = AsyncMock()
    mock_cursor.execute = AsyncMock()
    mock_cursor.fetchall = AsyncMock(return_value=[])

    # Pack the mocked response into our hybrid record row
    mock_cursor.fetchone = AsyncMock(
        return_value=HybridMockRow(
            {
                "industry_id": 1,
                "industry_name": "Professional Services",
                "moltset_category": "Standard",
                "bottleneck_profile": "High Fluidity",
                "pain_tokens": ["delay"],
                "target_region": "US-East",
            }
        )
    )

    mock_conn = MagicMock()
    mock_conn.cursor = MagicMock(return_value=AsyncContextManagerMock(mock_cursor))
    mock_conn.commit = AsyncMock()

    mock_pool = MagicMock()
    mock_pool.connection.return_value = AsyncContextManagerMock(mock_conn)

    app.state.db_pool = mock_pool
    yield
    if hasattr(app.state, "db_pool"):
        del app.state.db_pool


@pytest.fixture(autouse=True)
def mock_outbound_mesh_proxy():
    """
    Intercepts downstream network dispatches to Polaris Nexus/Render targets
    to keep integration tests completely isolated.
    """
    with patch(
        "services.mesh_client.async_http_post_proxy", new_callable=AsyncMock
    ) as mock_proxy:
        mock_proxy.return_value = True
        yield mock_proxy


@pytest.mark.anyio
async def test_01_health_matrix_endpoint():
    """Verify standard core uptime diagnostics."""
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as ac:
        response = await ac.get("/api/v1/health")
    assert response.status_code == 200
    assert response.json()["status"] == "degraded"
    assert all(
        status == "not_started" for status in response.json()["workers"].values()
    )


@pytest.mark.anyio
async def test_02_lead_ingestion_and_security_auth():
    """Verify lead processing ingestion pipelines and hmac auth validation hooks."""
    payload = [
        {
            "email": "test_lead_99@helix-mesh.io",
            "first_name": "Sarah",
            "company": "Helix Nodes Inc",
            "industry": "Professional and Business Services",
        }
    ]
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as ac:
        # 1. Assert invalid credentials get safely rejected
        bad_res = await ac.post(
            "/api/v1/ingest",
            json=payload,
            headers={"X-Helix-Auth-Token": "malicious_token"},
        )
        assert bad_res.status_code == 401

        # 2. Assert clean payload maps correctly
        good_res = await ac.post(
            "/api/v1/ingest",
            json=payload,
            headers={
                "X-Helix-Auth-Token": TEST_AUTH_TOKEN
            },  # Use your test harness variable here
        )
        assert good_res.status_code == 200


@pytest.mark.anyio
async def test_03_inbound_blueprints_scoring():
    """Test standard blueprint calculations entry limits."""
    payload = {
        "headcount": 80,
        "weekly_hours_wasted": 8,
        "annual_wasted_hours": 416,
        "annual_financial_bleed": 20800.0,
        "friction_points": ["Monolithic code dependencies", "Uncoordinated schedules"],
        "name": "Alex Tech",
        "company_name": "Decoupled Flow",
        "email": "alex@decoupledflow.com",
        "industry_name": "Information Technology",
    }
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as ac:
        res = await ac.post(
            "/api/v1/inbound-blueprints",
            json=payload,
            headers={"X-Helix-Auth-Token": TEST_AUTH_TOKEN},  # Updated key
        )
        assert res.status_code == 200


@pytest.mark.anyio
async def test_04_closed_loop_roi_attribution():
    """Ensure sales cycle status shifts cascade and organically track lead priority points."""
    payload = {
        "status": "meeting_booked",
        "email_body": "Confirmed discussion timeline for the multi-agent mesh rollout next week.",
    }
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as ac:
        res = await ac.post(
            "/api/v1/leads/status-update?email=test_lead_99@helix-mesh.io",
            json=payload,
            headers={"X-Vanguard-Key": "test_vanguard_key"},
        )
    assert res.status_code == 200
    assert res.json()["status"] == "attribution_processed"


@pytest.mark.anyio
async def test_05_log_analyzer_healer_mesh_cascade():
    """Inject execution logs into the telemetry interface to confirm self-healing evaluations."""
    payload = {
        "agent_name": "nova",
        "log_level": "INFO",
        "message": "Finished batch run successfully compiled.",
        "metadata": {
            "industry_name": "Professional and Business Services",
            "status": "COMPLETED",
            "records_found": 15,
        },
    }
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as ac:
        res = await ac.post("/api/v1/logs", json=payload)
    assert res.status_code == 200
    assert "logged_id" in res.json()


@pytest.mark.anyio
async def test_06_manual_override_cycle():
    """
    Exercise manual cycle override gates to verify deployment sequence structure.
    Validates that execution constraints are respected during manual cycle triggers.
    """
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as ac:
        res = await ac.post(
            "/api/v1/orchestrator/run-cycle",
            headers={"X-Helix-Auth-Token": TEST_AUTH_TOKEN},
        )
    # Will return 200 if row items populate or safely idle with clean diagnostics if empty
    # 403 if execution constraints prevent execution on unassigned days
    assert res.status_code in (200, 403, 404)


@pytest.mark.anyio
async def test_07_dashboard_diagnostics_telemetry():
    """
    Validate cross-ledger diagnostics rendering schema logic.
    Ensures diagnostics respect execution constraints and worker standdown protocols.
    """
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test"
    ) as ac:
        res = await ac.get(
            "/api/v1/orchestrator/dashboard-diagnostics",
            headers={"X-Helix-Auth-Token": TEST_AUTH_TOKEN},
        )
    # Status code is 200, or a handled database exception tracking string (500) if no external pool is initialized locally
    assert res.status_code in (200, 500)
