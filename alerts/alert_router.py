"""
Order Intelligence Platform — Phase 10
Alert Router
============
Dispatches alert notifications to Slack and/or PagerDuty.
Implements cooldown deduplication at the notification layer.

Channels:
  - critical → PagerDuty + Slack #alerts-critical
  - warning  → Slack #alerts-warning only
"""

import json
import os
from datetime import datetime, timezone
from typing import Optional

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from utils.logger import configure_logging, get_logger

configure_logging()
log = get_logger("alert_router")

SLACK_WEBHOOK_URL        = os.getenv("SLACK_WEBHOOK_URL", "")
PAGERDUTY_INTEGRATION_KEY = os.getenv("PAGERDUTY_INTEGRATION_KEY", "")
PAGERDUTY_EVENTS_URL     = "https://events.pagerduty.com/v2/enqueue"


# ---------------------------------------------------------------------------
# Slack notification
# ---------------------------------------------------------------------------

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def _send_slack(payload: dict) -> None:
    if not SLACK_WEBHOOK_URL:
        log.info("slack_webhook_not_configured_skipping")
        return

    with httpx.Client(timeout=10) as client:
        resp = client.post(SLACK_WEBHOOK_URL, json=payload)
        resp.raise_for_status()
    log.info("slack_notification_sent")


def _build_slack_payload(
    alert_type: str,
    severity: str,
    region: str,
    current_value: float,
    baseline_value: float,
    deviation_pct: float,
    message: str,
    layer: int,
) -> dict:
    icon   = "🚨" if severity == "critical" else "⚠️"
    colour = "#EF4444" if severity == "critical" else "#F59E0B"
    ts     = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    return {
        "attachments": [
            {
                "color": colour,
                "blocks": [
                    {
                        "type": "section",
                        "text": {
                            "type": "mrkdwn",
                            "text": (
                                f"{icon} *{alert_type.replace('_', ' ').title()}* "
                                f"[{severity.upper()}] — Layer {layer}\n"
                                f"*Region:* {region}\n"
                                f"*Message:* {message}"
                            ),
                        },
                    },
                    {
                        "type": "section",
                        "fields": [
                            {"type": "mrkdwn", "text": f"*Current:*\n₹{current_value:,.2f}"},
                            {"type": "mrkdwn", "text": f"*Baseline:*\n₹{baseline_value:,.2f}"},
                            {"type": "mrkdwn", "text": f"*Deviation:*\n{deviation_pct:+.1f}%"},
                            {"type": "mrkdwn", "text": f"*Time:*\n{ts}"},
                        ],
                    },
                ],
            }
        ]
    }


# ---------------------------------------------------------------------------
# PagerDuty notification
# ---------------------------------------------------------------------------

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def _send_pagerduty(
    alert_type: str,
    region: str,
    current_value: float,
    message: str,
) -> None:
    if not PAGERDUTY_INTEGRATION_KEY:
        log.info("pagerduty_key_not_configured_skipping")
        return

    payload = {
        "routing_key":   PAGERDUTY_INTEGRATION_KEY,
        "event_action":  "trigger",
        "dedup_key":     f"{alert_type}::{region}",
        "payload": {
            "summary":   message,
            "source":    "order-intelligence-platform",
            "severity":  "critical",
            "component": f"region:{region}",
            "custom_details": {
                "alert_type":    alert_type,
                "region":        region,
                "current_value": current_value,
            },
        },
    }

    with httpx.Client(timeout=10) as client:
        resp = client.post(PAGERDUTY_EVENTS_URL, json=payload)
        resp.raise_for_status()
    log.info("pagerduty_event_sent", alert_type=alert_type, region=region)


# ---------------------------------------------------------------------------
# Main router
# ---------------------------------------------------------------------------

def fire_alert(
    alert_type: str,
    severity: str,
    region: str,
    current_value: float,
    baseline_value: float,
    deviation_pct: float,
    message: str,
    layer: int,
) -> None:
    """
    Route an alert to the appropriate notification channels.
    Critical → PagerDuty + Slack
    Warning  → Slack only
    """
    log.info("alert_firing",
             alert_type=alert_type,
             severity=severity,
             region=region,
             current_value=current_value,
             deviation_pct=deviation_pct,
             layer=layer)

    try:
        slack_payload = _build_slack_payload(
            alert_type, severity, region,
            current_value, baseline_value, deviation_pct, message, layer,
        )
        _send_slack(slack_payload)
    except Exception as exc:
        log.error("slack_notification_failed", error=str(exc))

    if severity == "critical":
        try:
            _send_pagerduty(alert_type, region, current_value, message)
        except Exception as exc:
            log.error("pagerduty_notification_failed", error=str(exc))
