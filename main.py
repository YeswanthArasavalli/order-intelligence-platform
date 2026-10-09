"""
Order Intelligence Platform — Main Entry Point
===============================================
Launches any platform service by name.

Usage:
    python main.py producer          # start Kafka publisher (Phase 2)
    python main.py bronze            # start Bronze writer (Phase 4)
    python main.py silver            # start Silver transformer (Phase 6)
    python main.py gold              # start Gold aggregator (Phase 7)
    python main.py alerts --rules alerts/rules.example.json  # M7 one-shot evaluation
    python main.py notifications     # M8 one-shot persisted-event delivery
    python main.py metrics           # start metrics exporter (Phase 11)
    python main.py topics            # create Kafka topics (Phase 3)
    python main.py verify            # run self-contained verification tests (no services required)
"""

import sys

from utils.logger import configure_logging, get_logger

configure_logging()
log = get_logger("main")

SERVICES = {
    "producer": "producer.kafka_publisher:main",
    "bronze":   "streaming.bronze_writer:run",
    "silver":   "streaming.silver_transformer:run",
    "gold":     "streaming.gold_aggregator:run",
    "alerts":   "alerts.evaluator:main",
    "notifications": "notifications.service:main",
    "metrics":  "observability.metrics_exporter:main",
    "topics":   "kafka.topic_config:create_topics",
    "verify":   "scripts.verify:run",
}


def _run(service: str) -> None:
    if service not in SERVICES:
        print(f"Unknown service: '{service}'")
        print(f"Available: {', '.join(SERVICES.keys())}")
        sys.exit(1)

    module_path, fn_name = SERVICES[service].rsplit(":", 1)
    log.info("launching_service", service=service, module=module_path)

    import importlib
    module = importlib.import_module(module_path)
    fn     = getattr(module, fn_name)
    fn()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(0)

    # Keep service-specific CLI arguments available to the selected module's
    # argparse parser, but do not pass the dispatcher selector itself through
    # as an application argument (for example, "producer").
    service = sys.argv[1]
    sys.argv = [sys.argv[0], *sys.argv[2:]]
    _run(service)
