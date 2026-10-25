import hmac
import os
import re
import time
from collections import defaultdict, deque

# Environment variables whose values must never appear in outbound messages or logs.
SECRET_ENV_VARS = (
    "HELIX_AUTH_TOKEN",
    "TELEGRAM_WEBHOOK_SECRET",
    "TELEGRAM_BOT_TOKEN",
    "TELEGRAM_CRITICAL_BOT_TOKEN",
    "ANTHROPIC_API_KEY",
    "OPENAI_API_KEY",
    "MOLTSET_API_KEY",
    "PHANTOMBUSTER_API_KEY",
    "MYEMAILVERIFIER_API_KEY",
    "VANGUARD_API_KEY",
    "DATABASE_URL",
    "VANGUARD_DATABASE_URL",
)
_MIN_SECRET_LENGTH = 6
_TELEGRAM_BOT_URL_TOKEN = re.compile(r"bot\d+:[A-Za-z0-9_-]+")


def tokens_match(provided: str | None, expected: str | None) -> bool:
    """Constant-time secret comparison that fails closed.

    Compares UTF-8 bytes, because hmac.compare_digest raises TypeError on
    non-ASCII str input, which would turn a bad header into a 500.
    """
    if not provided or not expected:
        return False
    return hmac.compare_digest(provided.encode("utf-8"), expected.encode("utf-8"))


def redact_secrets(text: object) -> str:
    """Replace configured secret values and Telegram bot URLs in text with ***."""
    redacted = _TELEGRAM_BOT_URL_TOKEN.sub("bot***", str(text))
    for name in SECRET_ENV_VARS:
        value = os.getenv(name, "")
        if len(value) >= _MIN_SECRET_LENGTH:
            redacted = redacted.replace(value, "***")
    return redacted


class SlidingWindowLimiter:
    """In-process sliding-window limiter. Helix runs as a single instance."""

    def __init__(self) -> None:
        self._hits: dict[str, deque[float]] = defaultdict(deque)

    def allow(
        self, key: str, limit: int, window_seconds: float, now: float | None = None
    ) -> bool:
        current = time.monotonic() if now is None else now
        hits = self._hits[key]
        while hits and hits[0] <= current - window_seconds:
            hits.popleft()
        if len(hits) >= limit:
            return False
        hits.append(current)
        return True
