import os
from zoneinfo import ZoneInfo
from dotenv import load_dotenv

load_dotenv()

NEW_YORK_TZ = ZoneInfo("America/New_York")

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

CREDIT_SYNC_INTERVAL_SECONDS = 8 * 60 * 60
CREDIT_SYNC_JITTER_SECONDS = 10 * 60
