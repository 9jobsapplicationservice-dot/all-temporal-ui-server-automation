from __future__ import annotations

import time


class ProviderRateLimiter:
    def __init__(self, requests_per_minute: int) -> None:
        normalized = max(1, requests_per_minute)
        self.requests_per_minute = normalized
        self._minimum_interval = 60.0 / float(normalized)
        self._next_allowed_by_provider: dict[str, float] = {}

    def wait(self, provider: str) -> float:
        now = time.monotonic()
        next_allowed = self._next_allowed_by_provider.get(provider, now)
        sleep_for = max(0.0, next_allowed - now)
        if sleep_for > 0:
            time.sleep(sleep_for)
        updated_now = time.monotonic()
        self._next_allowed_by_provider[provider] = updated_now + self._minimum_interval
        return sleep_for
