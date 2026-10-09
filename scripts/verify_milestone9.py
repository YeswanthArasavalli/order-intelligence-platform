"""Host-side real Prometheus/Grafana/API checks; credentials never enter evidence."""

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import shutil
import subprocess
import time

from dotenv import dotenv_values
import httpx
from prometheus_client.parser import text_string_to_metric_families

from observability.catalog import SPECS


def wait(predicate, timeout=90):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            result = predicate()
            if result:
                return result
        except (httpx.HTTPError, ValueError, KeyError, AssertionError):
            pass
        time.sleep(1)
    raise AssertionError("Verification condition did not converge within timeout")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--phase", required=True)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    config = dotenv_values(root / ".env")
    prometheus = f"http://127.0.0.1:{config.get('HOST_PROMETHEUS_PORT', '9090')}"
    grafana = f"http://127.0.0.1:{config.get('HOST_GRAFANA_PORT', '3000')}"
    exporter = f"http://127.0.0.1:{config.get('HOST_METRICS_PORT', '8000')}"
    with httpx.Client(timeout=5, trust_env=False) as client:
        def query(expression):
            response = client.get(prometheus + "/api/v1/query", params={"query": expression})
            response.raise_for_status()
            data = response.json()
            assert data["status"] == "success"
            return data["data"]["result"]

        def scalar(expression):
            rows = query(expression)
            assert len(rows) == 1
            return float(rows[0]["value"][1])

        def targets():
            response = client.get(prometheus + "/api/v1/targets")
            response.raise_for_status()
            return [{"job": t["labels"]["job"], "health": t["health"], "scrapeUrl": t["scrapeUrl"],
                     "lastScrape": t["lastScrape"]} for t in response.json()["data"]["activeTargets"]]

        output = {"phase": args.phase, "verified_at": datetime.now(timezone.utc).isoformat()}
        if args.phase == "target-down":
            wait(lambda: scalar('up{job="oip"}') == 0)
            output["targets"] = targets()
            output["target_failure_detected"] = True
        elif args.phase in {"postgres-down", "redis-down"}:
            component = args.phase.split("-")[0]
            wait(lambda: scalar(f'oip_dependency_up{{component="{component}"}}') == 0)
            assert client.get(exporter + "/healthz").status_code == 200
            assert client.get(exporter + "/readyz").status_code == 503
            output.update(dependency_down=component, exporter_alive=True, readiness_degraded=True,
                          probe=query(f'oip_dependency_state{{component="{component}"}}'))
            if component == "redis":
                wait(lambda: scalar('oip_dashboard_loads_total{source="postgres",redis_status="unavailable"}') >= 1)
                output["dashboard_fallback_observed"] = True
        elif args.phase == "activity":
            expected = [
                'oip_producer_events_total{result="delivered"}', 'oip_bronze_records_total{result="written"}',
                'oip_dq_records_total{result="evaluated"}', 'oip_dlq_records_written_total',
                'oip_silver_records_total{route="accepted"}', 'oip_gold_events_total{result="new"}',
                'oip_operation_calls_total{operation="postgres_commit",result="success"}',
                'oip_operation_calls_total{operation="redis_refresh",result="success"}',
                'oip_dashboard_loads_total{source="redis",redis_status="valid"}',
                'oip_alert_evaluations_total{result="FIRED"}', 'oip_alert_evaluations_total{result="SUPPRESSED"}',
                'oip_notification_requests_total{result="created"}',
                'oip_notification_attempt_results_total{result="DELIVERED"}',
                'oip_notification_attempt_results_total{result="RETRYABLE_FAILURE"}',
                'oip_notification_attempt_results_total{result="UNKNOWN"}',
                'oip_provider_acceptances_total{provider="controlled_http",channel="http"}',
                'oip_operation_duration_seconds_count{operation="provider_send"}',
            ]
            wait(lambda: all(scalar(q) > 0 for q in expected))
            output["promql"] = {q: query(q) for q in expected}
        elif args.phase == "restart":
            wait(lambda: scalar('up{job="oip"}') == 1)
            wait(lambda: client.get(exporter + "/readyz").status_code == 200)
            assert scalar('oip_producer_events_total{result="delivered"}') == 0
            output.update(counters_reset=True, snapshot_version=query('oip_snapshot_version{component="postgres"}'),
                          targets=targets(), zero_activity_is_not_failure=True)
        elif args.phase == "application-restart":
            expression = 'oip_alert_evaluations_total{result="NOOP"}'
            before = scalar(expression)
            command = [shutil.which("docker.exe") or shutil.which("docker"), "exec", "-e", "PYTHONPATH=/opt/oip",
                       "-e", "OIP_TELEMETRY_URL=http://metrics-exporter:8000/observe", "oip-modernization-verify",
                       "python3", "-m", "alerts.evaluator", "--rules", "/opt/oip/alerts/rules.example.json"]
            results = []
            for _ in range(2):
                proc = subprocess.run(command, capture_output=True, text=True, timeout=30)
                assert proc.returncode == 0
                counts = json.loads(proc.stdout)["counts"]
                assert counts["NOOP"] == 4 and counts["FIRED"] == 0
                results.append({"returncode": proc.returncode, "counts": counts})
            wait(lambda: scalar(expression) == before + 8)
            output.update(command=command, processes=results, before=before, after=scalar(expression),
                          business_result_unchanged=True, metrics_across_process_restart=True)
        else:
            wait(lambda: client.get(prometheus + "/-/ready").status_code == 200)
            wait(lambda: len(targets()) == 3 and all(t["health"] == "up" for t in targets()))
            wait(lambda: client.get(exporter + "/readyz").status_code == 200)
            wait(lambda: scalar('sum(oip_dependency_up)') == 6)
            assert client.get(exporter + "/healthz").status_code == 200
            exposition = client.get(exporter + "/metrics")
            exposition.raise_for_status()
            allowed = {}
            for name, spec in SPECS.items():
                names = [name]
                if spec.kind == "counter":
                    names.append(name.removesuffix("_total") + "_created")
                elif spec.kind == "histogram":
                    names = [name + suffix for suffix in ("_bucket", "_sum", "_count", "_created")]
                for sample_name in names:
                    allowed[sample_name] = (spec, set(spec.labels) | ({"le"} if sample_name.endswith("_bucket") else set()))
            sample_count = 0
            for family in text_string_to_metric_families(exposition.text):
                for observation in family.samples:
                    spec, labels = allowed[observation.name]
                    assert set(observation.labels) == labels
                    assert all(observation.labels[key] in values for key, values in spec.labels.items())
                    sample_count += 1
            assert sample_count == 972
            auth = (config.get("GRAFANA_ADMIN_USER", "admin"), config["GRAFANA_ADMIN_PASSWORD"])
            health = client.get(grafana + "/api/health")
            health.raise_for_status()
            datasource = client.get(grafana + "/api/datasources/uid/prometheus-oip/health", auth=auth)
            datasource.raise_for_status()
            assert datasource.json()["status"] == "OK"
            proxy = client.get(grafana + "/api/datasources/proxy/uid/prometheus-oip/api/v1/query",
                               params={"query": 'up{job="oip"}'}, auth=auth)
            proxy.raise_for_status()
            assert proxy.json()["status"] == "success"
            dashboards = []
            for uid in ("oip-platform", "oip-pipeline", "oip-serving-delivery"):
                filename = {"oip-platform": "platform.json", "oip-pipeline": "pipeline.json", "oip-serving-delivery": "serving-delivery.json"}[uid]
                expected = json.loads((root / "observability" / "grafana" / "dashboards" / filename).read_text(encoding="utf-8"))
                wait(lambda: client.get(grafana + "/api/dashboards/uid/" + uid, auth=auth).json()["dashboard"]["panels"][0]["title"] == expected["panels"][0]["title"])
                wait(lambda: len(client.get(grafana + "/api/dashboards/uid/" + uid, auth=auth).json()["dashboard"]["panels"]) == len(expected["panels"]))
                response = client.get(grafana + "/api/dashboards/uid/" + uid, auth=auth)
                response.raise_for_status()
                document = response.json()
                assert document["meta"]["provisioned"] is True
                for actual, wanted in zip(document["dashboard"]["panels"], expected["panels"]):
                    assert actual["title"] == wanted["title"]
                    if "fieldConfig" in wanted:
                        assert actual["fieldConfig"]["defaults"]["mappings"] == wanted["fieldConfig"]["defaults"]["mappings"]
                for panel in document["dashboard"]["panels"]:
                    for target in panel["targets"]:
                        query(target["expr"])  # Actual PromQL parsing/execution, including empty/no-activity results.
                dashboards.append({"uid": uid, "title": document["dashboard"]["title"],
                                   "panels": len(document["dashboard"]["panels"]), "provisioned": True})
            output.update(targets=targets(), prometheus_ready=True, grafana_health=health.json(),
                          datasource_health=datasource.json(), grafana_proxy_query=proxy.json(), dashboards=dashboards,
                          metrics_response=True, snapshot_versions=query("oip_snapshot_version"),
                          dependency_states=query("oip_dependency_state"), browser_pixel_validation=False,
                          exposition_samples=sample_count, all_metric_labels_allowlisted=True,
                          exposition_preview=[line for line in exposition.text.splitlines() if line.startswith((
                              "# TYPE oip_", "oip_bronze_records_total", "oip_snapshot_version", "oip_provider_acceptances_total"))])
        print("MILESTONE9_VERIFIED " + json.dumps(output, sort_keys=True))


if __name__ == "__main__":
    main()
