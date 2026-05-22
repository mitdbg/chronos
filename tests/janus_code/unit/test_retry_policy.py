"""Unit tests for RetryPolicy (in config.py)."""

import pytest

from janus_code.config import RetryPolicy


class TestRetryPolicy:
    def test_defaults(self):
        rp = RetryPolicy()
        assert rp.strategy == "exponential"
        assert rp.max_retries == 3
        assert rp.base_delay_ms == 100
        assert rp.max_delay_ms == 5000

    def test_delay_for_attempt_exponential(self):
        rp = RetryPolicy(strategy="exponential", base_delay_ms=100, max_delay_ms=5000)
        assert rp.delay_for_attempt(1) == 100
        assert rp.delay_for_attempt(2) == 200
        assert rp.delay_for_attempt(3) == 400
        assert rp.delay_for_attempt(4) == 800

    def test_delay_capped_at_max(self):
        rp = RetryPolicy(base_delay_ms=1000, max_delay_ms=2000)
        # attempt 3: 1000 * 2^2 = 4000, capped at 2000
        assert rp.delay_for_attempt(3) == 2000

    def test_none_strategy(self):
        rp = RetryPolicy(strategy="none")
        assert rp.strategy == "none"

    def test_delay_for_attempt_zero(self):
        rp = RetryPolicy(base_delay_ms=100)
        assert rp.delay_for_attempt(0) == 50  # 100 * 2^(-1) = 50
