import argparse
import os
import re
from urllib.parse import urlparse

import httpx
from dotenv import load_dotenv

TELEGRAM_SECRET_PATTERN = re.compile(r"^[A-Za-z0-9_-]{1,256}$")


def _webhook_url(value: str) -> str:
    parsed = urlparse(value)
    if parsed.scheme != "https" or not parsed.netloc:
        raise argparse.ArgumentTypeError("webhook URL must be a public HTTPS URL")
    return value


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Register the Helix Telegram webhook (TELEGRAM_WEBHOOK_SECRET, else HELIX_AUTH_TOKEN)."
    )
    parser.add_argument(
        "webhook_url",
        type=_webhook_url,
        help="Full Helix endpoint, for example https://host/api/v1/telegram/webhook",
    )
    args = parser.parse_args()

    load_dotenv()
    bot_token = os.getenv("TELEGRAM_BOT_TOKEN", "")
    auth_token = os.getenv("TELEGRAM_WEBHOOK_SECRET", "").strip() or os.getenv(
        "HELIX_AUTH_TOKEN", ""
    )
    if not bot_token:
        parser.error("TELEGRAM_BOT_TOKEN is not configured")
    if not auth_token:
        parser.error("TELEGRAM_WEBHOOK_SECRET or HELIX_AUTH_TOKEN is not configured")
    if not TELEGRAM_SECRET_PATTERN.fullmatch(auth_token):
        parser.error(
            "The webhook secret must contain only letters, numbers, underscores, or hyphens "
            "to be accepted by Telegram"
        )

    try:
        response = httpx.post(
            f"https://api.telegram.org/bot{bot_token}/setWebhook",
            json={
                "url": args.webhook_url,
                "secret_token": auth_token,
                "allowed_updates": ["message"],
                "drop_pending_updates": False,
            },
            timeout=15.0,
        )
        response.raise_for_status()
        result = response.json()
    except (httpx.HTTPError, ValueError) as exc:
        print(f"Webhook registration failed: {str(exc).replace(bot_token, '***')}")
        return 1

    if not result.get("ok"):
        print(
            f"Webhook registration rejected: {result.get('description', 'unknown error')}"
        )
        return 1

    print("Helix Telegram webhook registered with secret-token authentication.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
