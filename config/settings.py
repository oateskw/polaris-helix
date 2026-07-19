import os
from zoneinfo import ZoneInfo
from dotenv import load_dotenv
from datetime import datetime, time
from typing import Final

load_dotenv()

NEW_YORK_TZ: Final[ZoneInfo] = ZoneInfo("America/New_York")

# Scheduling Constraints - Worker agents stand down on unassigned days
HELIX_ACTIVE_DAYS: Final[frozenset[int]] = frozenset({0, 1, 2, 3, 4})  # Monday-Friday
HELIX_STANDBY_DAYS: Final[frozenset[int]] = frozenset({5, 6})  # Saturday-Sunday
HELIX_ACTIVE_HOURS_START: Final[time] = time(6, 0)  # 6:00 AM ET
HELIX_ACTIVE_HOURS_END: Final[time] = time(22, 0)  # 10:00 PM ET


def is_helix_active_day(dt: datetime | None = None) -> bool:
    """Check if current day is an active operational day for Helix workers."""
    check_time = dt if dt else datetime.now(NEW_YORK_TZ)
    return check_time.weekday() in HELIX_ACTIVE_DAYS


def is_helix_active_hours(dt: datetime | None = None) -> bool:
    """Check if current time falls within active operational hours."""
    check_time = dt if dt else datetime.now(NEW_YORK_TZ)
    current_time = check_time.time()
    return HELIX_ACTIVE_HOURS_START <= current_time <= HELIX_ACTIVE_HOURS_END


def should_helix_workers_operate(dt: datetime | None = None) -> bool:
    """Determine if worker agents should be active based on schedule constraints."""
    return is_helix_active_day(dt) and is_helix_active_hours(dt)

MOLTSET_ALLOWED_CATEGORIES = [
    "Information Technology",
    "Professional and Business Services",
    "Finance and Banking",
    "Education",
    "Health and Pharmaceuticals",
    "Manufacturing",
    "Government and Public Administration",
    "Retail",
    "Food and Beverage",
    "Creative Arts and Entertainment",
    "Non-Profit and Social Services",
    "Transportation and Logistics",
    "Construction",
    "Tourism and Hospitality",
    "Energy",
    "Marketing & Advertising",
    "Telecommunications",
    "Real Estate",
    "Automotive",
    "Media and Publishing",
    "Agriculture",
]

HELIX_AUTH_TOKEN = os.getenv("HELIX_AUTH_TOKEN", "")
VANGUARD_API_KEY = os.getenv("VANGUARD_API_KEY", "")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
ANTHROPIC_MODEL = os.getenv("ANTHROPIC_MODEL", "claude-3-5-sonnet-latest")

CHIRON_API_URL = os.getenv(
    "CHIRON_API_URL", "https://polaris-chiron.onrender.com"
).rstrip("/")
NEXUS_NERVOUS_IMPULSE_URL = os.getenv(
    "NEXUS_NERVOUS_IMPULSE_URL", "https://polaris-nexus.onrender.com"
).rstrip("/")
NOVA_API_URL = os.getenv("NOVA_API_URL", "https://polaris-nova.onrender.com").rstrip(
    "/"
)
BEACON_API_URL = os.getenv(
    "BEACON_API_URL", "https://polaris-beacon-agent.onrender.com"
).rstrip("/")
RADAR_API_URL = os.getenv("RADAR_API_URL", "https://polaris-radar.onrender.com").rstrip(
    "/"
)
PULSAR_API_URL = os.getenv(
    "PULSAR_API_URL", "https://polaris-pulsar.onrender.com"
).rstrip("/")
VANGUARD_API_URL = os.getenv(
    "VANGUARD_API_URL", "https://polaris-vanguard-core.onrender.com"
).rstrip("/")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

# Operational Limits & Constants
PHANTOMBUSTER_KEYWORD_STAIRCASE = [25, 12, 8, 5]
PHANTOMBUSTER_ZERO_RESULT_THRESHOLD = 2
PHANTOMBUSTER_TUNING_COOLDOWN_MINUTES = 60

CREDIT_SYNC_INTERVAL_SECONDS: Final[int] = 8 * 60 * 60
CREDIT_SYNC_JITTER_SECONDS: Final[int] = 10 * 60

# Worker Standdown Configuration
WORKER_STANDDOWN_CHECK_INTERVAL_SECONDS: Final[int] = 5 * 60  # Check every 5 minutes
WORKER_GRACEFUL_SHUTDOWN_TIMEOUT_SECONDS: Final[int] = 30
