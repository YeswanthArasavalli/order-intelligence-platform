"""One concrete TEST/CONTROLLED HTTP provider, with a provider-neutral interface."""

from dataclasses import dataclass
import json
import re
from typing import Protocol
from urllib.parse import urlsplit

import httpx
from observability import telemetry as m9

from notifications.model import identity


@dataclass(frozen=True)
class DeliveryResult:
    outcome: str
    provider_status: int | None = None
    reference: str | None = None
    error_class: str | None = None

    def __post_init__(self):
        if (self.outcome not in {"DELIVERED", "RETRYABLE_FAILURE", "PERMANENT_FAILURE"}
                or self.error_class not in {None, "timeout", "network", "http_retryable", "http_permanent",
                                           "invalid_receipt", "provider_exception"}
                or self.reference is not None and not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", self.reference)
                or self.outcome == "DELIVERED" and (self.reference is None or self.error_class is not None)
                or self.provider_status is not None and (type(self.provider_status) is not int or not 100 <= self.provider_status <= 599)):
            raise ValueError("Invalid provider-neutral delivery result")


class Provider(Protocol):
    @property
    def route(self) -> dict: ...

    def send(self, payload: dict) -> DeliveryResult: ...


class ControlledHTTPProvider:
    """Durable acceptance at a loopback test receiver, not external messaging."""

    def __init__(self, endpoint: str, destination: str, timeout: float = 2):
        try:
            url = urlsplit(endpoint)
            valid = (url.scheme == "http" and url.hostname == "127.0.0.1" and url.port is not None
                     and 1 <= url.port <= 65535 and url.path == "/notify" and not url.query and not url.fragment
                     and url.username is None and url.password is None)
        except (ValueError, TypeError):
            valid = False
        if not valid or not isinstance(destination, str) or not re.fullmatch(r"[a-z][a-z0-9_-]{0,63}", destination):
            raise ValueError("Controlled provider requires an explicit loopback endpoint and destination alias")
        if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or not 0.05 <= timeout <= 10:
            raise ValueError("Provider timeout must be 0.05–10 seconds")
        self._endpoint, self._destination, self._timeout = endpoint, destination, timeout

    @property
    def route(self):
        return {"provider": "controlled_http", "channel": "http", "destination": self._destination,
                "endpoint_digest": identity(self._endpoint), "timeout": self._timeout}

    @m9.observed("provider_send")
    def send(self, payload):
        key = payload["notification_id"]
        try:
            with httpx.Client(timeout=self._timeout, trust_env=False, follow_redirects=False) as client:
                with client.stream("POST", self._endpoint, headers={"Idempotency-Key": key},
                                   json={"destination": self._destination, "notification": payload}) as response:
                    status = response.status_code
                    if status != 200:
                        retryable = status in {408, 425, 429} or 500 <= status <= 599
                        return DeliveryResult("RETRYABLE_FAILURE" if retryable else "PERMANENT_FAILURE", status,
                                              error_class="http_retryable" if retryable else "http_permanent")
                    body = b""
                    for chunk in response.iter_bytes():
                        body += chunk
                        if len(body) > 4096:
                            return DeliveryResult("RETRYABLE_FAILURE", status, error_class="invalid_receipt")
                    try:
                        receipt = json.loads(body)
                        expected = "controlled-" + key
                        if (receipt["notification_id"] != key or receipt["reference"] != expected
                                or receipt["status"] != "accepted"):
                            raise ValueError("Invalid receipt")
                        return DeliveryResult("DELIVERED", status, reference=expected)
                    except (ValueError, KeyError, TypeError):
                        return DeliveryResult("RETRYABLE_FAILURE", status, error_class="invalid_receipt")
        except httpx.TimeoutException:
            return DeliveryResult("RETRYABLE_FAILURE", error_class="timeout")
        except httpx.HTTPError:
            return DeliveryResult("RETRYABLE_FAILURE", error_class="network")
