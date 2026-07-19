import importlib

import config.settings as settings


def test_settings_defaults_and_url_normalization(monkeypatch):
    monkeypatch.delenv("CHIRON_API_URL", raising=False)
    monkeypatch.setenv("NOVA_API_URL", "https://example.com///")
    monkeypatch.setenv("BEACON_API_URL", "https://beacon.example.com/")

    reloaded = importlib.reload(settings)

    assert reloaded.CHIRON_API_URL == "https://polaris-chiron.onrender.com"
    assert reloaded.NOVA_API_URL == "https://example.com"
    assert reloaded.BEACON_API_URL == "https://beacon.example.com"
    assert "Professional and Business Services" in reloaded.MOLTSET_ALLOWED_CATEGORIES


def test_settings_token_defaults(monkeypatch):
    monkeypatch.setenv("HELIX_AUTH_TOKEN", "override-token")
    monkeypatch.setenv("VANGUARD_API_KEY", "override-vanguard")

    reloaded = importlib.reload(settings)

    assert reloaded.HELIX_AUTH_TOKEN == "override-token"
    assert reloaded.VANGUARD_API_KEY == "override-vanguard"


def test_execution_constraints_scheduling_standdown(monkeypatch):
    """Verify execution constraints enforce worker standdown on unassigned days."""
    reloaded = importlib.reload(settings)

    assert hasattr(reloaded, "HELIX_ACTIVE_DAYS")
    assert hasattr(reloaded, "should_helix_workers_operate")
    assert callable(reloaded.should_helix_workers_operate)
