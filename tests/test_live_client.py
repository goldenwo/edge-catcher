"""Tests for edge_catcher.live.client — KalshiOrderClient + dataclasses."""
from __future__ import annotations

import pytest

from edge_catcher.live.client import (
	Balance,  # noqa: F401 — used by Tasks 7-9 tests
	CancelResult,  # noqa: F401 — used by Tasks 7-9 tests
	KalshiOrderClient,
	Order,  # noqa: F401 — used by Tasks 7-9 tests
	OrderRequest,
	Position,  # noqa: F401 — used by Tasks 7-9 tests
)
from edge_catcher.live.config import LiveConfig
from edge_catcher.live.audit import AuditLogger


@pytest.fixture
def cfg(tmp_path):
	return LiveConfig(audit_log_path=tmp_path / "audit.jsonl")


@pytest.fixture
def audit(tmp_path):
	return AuditLogger(tmp_path / "audit.jsonl")


def test_order_request_exposure_dollars():
	req = OrderRequest(
		ticker="X", action="buy", side="yes", count=10, limit_price_cents=5,
	)
	assert req.exposure_dollars == 0.50


def test_order_request_default_tif_gtc():
	req = OrderRequest(ticker="X", action="buy", side="yes", count=1, limit_price_cents=1)
	assert req.time_in_force == "gtc"


def test_client_init_and_close(cfg, audit):
	client = KalshiOrderClient(cfg, audit)
	client.close()


def test_client_context_manager(cfg, audit):
	with KalshiOrderClient(cfg, audit) as c:
		assert c is not None
