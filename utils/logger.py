"""
Order Intelligence Platform — Structured Logger
================================================
Wraps structlog to produce JSON log lines with consistent fields:
  timestamp, level, service, event, and any extra kwargs.

Usage:
    from utils.logger import get_logger

    log = get_logger("producer")
    log.info("event_published", topic="order-events", offset=10042)
    log.error("publish_failed", topic="order-events", error=str(e))
"""

import logging
import sys

import structlog


def configure_logging(log_level: str = "INFO") -> None:
    """
    Configure structlog for structured JSON output.
    Call once at application startup (e.g. in main.py).
    """
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.stdlib.add_log_level,
            structlog.stdlib.add_logger_name,
            structlog.processors.TimeStamper(fmt="iso", utc=True),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.stdlib.BoundLogger,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=getattr(logging, log_level.upper(), logging.INFO),
    )


def get_logger(service: str) -> structlog.stdlib.BoundLogger:
    """
    Return a structlog logger pre-bound with the service name.

    Args:
        service: Short identifier for the calling service/module.
                 e.g. "producer", "bronze_writer", "dq_engine"

    Returns:
        A bound structlog logger.
    """
    return structlog.get_logger(service)
