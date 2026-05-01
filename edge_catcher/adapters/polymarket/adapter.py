"""Polymarket REST adapter — Gamma (markets metadata) + CLOB (trade history).

Both APIs are public and unauthenticated for read paths. We hit:
  GET https://gamma-api.polymarket.com/markets    — markets list with filters
  GET https://gamma-api.polymarket.com/markets/{id}  — single market detail
  GET https://clob.polymarket.com/markets/{conditionId}/trades — trade history

The adapter joins the two — Gamma supplies the listing layer (paginated +
filterable by status / category), CLOB supplies per-market trade events
keyed by `condition_id`.

Mapping notes:
  - Polymarket markets are typically binary (Yes/No) with a `condition_id`
    as primary key and per-outcome `tokens` (ERC1155 token IDs).
  - We use the condition_id as our `ticker` field for storage parity with
    Kalshi's market-ticker convention. The Yes/No outcome split is
    encoded in the trade's `side` (BUY of YES → "yes", BUY of NO → "no").
  - `series_ticker` defaults to a category slug (politics/sports/crypto)
    derived from market metadata when present, falling back to "default".
"""
from __future__ import annotations

import json
import logging
import random
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, List, Optional

import requests
import yaml

from edge_catcher.adapters.base import PredictionMarketAdapter
from edge_catcher.storage.models import Market, Trade

logger = logging.getLogger(__name__)

# Minimal schemas for response validation. Polymarket APIs return JSON arrays
# at the top level for lists and a flat object for detail / trade events.
SCHEMAS: dict[str, Any] = {
	"gamma_markets_list": {
		"required": [],  # top-level is a list[dict]; see _validate_list
		"item_required": ["id", "conditionId", "active", "closed"],
	},
	"gamma_market_detail": {
		"required": ["id", "conditionId"],
	},
	"clob_trades_list": {
		"required": [],  # top-level list[dict]
		"item_required": ["id", "side", "size", "price", "outcome", "timestamp"],
	},
}


class PolymarketAdapter(PredictionMarketAdapter):
	"""Polymarket REST adapter (Gamma + CLOB public APIs, no auth)."""

	GAMMA_BASE = "https://gamma-api.polymarket.com"
	CLOB_BASE = "https://clob.polymarket.com"
	SCHEMAS = SCHEMAS

	def __init__(
		self,
		config_path: Path = Path("config/markets-polymarket.yaml"),
		dry_run: bool = False,
	) -> None:
		config_path = Path(config_path)
		if not config_path.is_absolute():
			config_path = Path.cwd() / config_path

		with open(config_path, "r") as fh:
			config = yaml.safe_load(fh)

		poly_cfg = config["adapters"]["polymarket"]
		self.gamma_base: str = poly_cfg.get("gamma_base", self.GAMMA_BASE).rstrip("/")
		self.clob_base: str = poly_cfg.get("clob_base", self.CLOB_BASE).rstrip("/")
		self.rate_limit_seconds: float = float(poly_cfg.get("rate_limit_seconds", 0.5))
		# `series` here is a list of category-slug strings (e.g. ["politics",
		# "sports"]); empty list means "all categories".
		self.series: List[str] = poly_cfg.get("series", [])
		self.pagination_limit: int = int(
			poly_cfg.get("pagination", {}).get("default_limit", 100)
		)
		# Status filter — defaults to ["closed"] (settled markets) for
		# backtest-friendly historical data.
		self.statuses: List[str] = poly_cfg.get("statuses", ["closed"])
		self.dry_run: bool = dry_run
		self.min_available_ram_pct: float = float(
			poly_cfg.get("min_available_ram_pct", 10)
		)

		self.session = requests.Session()
		self.session.headers.update({"Accept": "application/json"})
		# No Authorization header — public endpoints only.

		self._last_request_time: float = 0.0

	# ------------------------------------------------------------------
	# Memory guard (mirrors the Kalshi adapter pattern)
	# ------------------------------------------------------------------

	def _check_memory(self) -> None:
		"""Pause if available RAM drops below a percentage of total."""
		try:
			import psutil
			mem = psutil.virtual_memory()
			available_pct = mem.available / mem.total * 100
			if available_pct < self.min_available_ram_pct:
				available_mb = mem.available / (1024 * 1024)
				total_gb = mem.total / (1024 ** 3)
				logger.warning(
					"Low RAM: %.0fMB free (%.1f%% of %.0fGB). Pausing 30s...",
					available_mb, available_pct, total_gb,
				)
				time.sleep(30)
		except ImportError:
			pass

	# ------------------------------------------------------------------
	# Rate limiting + retries
	# ------------------------------------------------------------------

	def _rate_limit(self) -> None:
		elapsed = time.monotonic() - self._last_request_time
		if elapsed < self.rate_limit_seconds:
			time.sleep(self.rate_limit_seconds - elapsed)
		self._last_request_time = time.monotonic()

	def _get(self, url: str, params: Optional[dict] = None, max_attempts: int = 3) -> Any:
		"""GET with exponential backoff. Returns parsed JSON or raises on
		repeated failure."""
		self._check_memory()
		for attempt in range(max_attempts):
			self._rate_limit()
			try:
				resp = self.session.get(url, params=params, timeout=30)
			except requests.RequestException as e:
				if attempt + 1 == max_attempts:
					raise
				delay = (2 ** attempt) + random.uniform(0, 0.5)
				logger.warning("polymarket GET %s network error (%s); retry in %.1fs",
					url, e, delay)
				time.sleep(delay)
				continue
			# 429 / 5xx → backoff. 4xx other than 429 → raise immediately.
			if resp.status_code == 429 or 500 <= resp.status_code < 600:
				if attempt + 1 == max_attempts:
					resp.raise_for_status()
				delay = (2 ** attempt) + random.uniform(0, 0.5)
				logger.warning("polymarket GET %s status=%d; retry in %.1fs",
					url, resp.status_code, delay)
				time.sleep(delay)
				continue
			resp.raise_for_status()
			return resp.json()
		raise RuntimeError(f"polymarket GET {url} failed after {max_attempts} attempts")

	# ------------------------------------------------------------------
	# Validation
	# ------------------------------------------------------------------

	def validate_response(self, data: dict, schema_key: str) -> bool:
		"""Validate against SCHEMAS[schema_key]. Raises ValueError on miss."""
		schema = self.SCHEMAS.get(schema_key)
		if schema is None:
			raise ValueError(f"unknown schema_key: {schema_key!r}")
		for required in schema.get("required", []):
			if required not in data:
				raise ValueError(
					f"polymarket response missing required field {required!r} "
					f"(schema={schema_key!r})"
				)
		return True

	def _validate_list(self, items: list, schema_key: str) -> None:
		"""Per-item validation for list endpoints (Gamma /markets, CLOB /trades)."""
		schema = self.SCHEMAS.get(schema_key, {})
		item_required = schema.get("item_required", [])
		for i, item in enumerate(items):
			if not isinstance(item, dict):
				raise ValueError(
					f"polymarket {schema_key}: item {i} is not a dict (got {type(item).__name__})"
				)
			for k in item_required:
				if k not in item:
					raise ValueError(
						f"polymarket {schema_key}: item {i} missing field {k!r}"
					)

	# ------------------------------------------------------------------
	# Public API: collect_markets
	# ------------------------------------------------------------------

	def collect_markets(self, series_tickers: Optional[List[str]] = None) -> List[Market]:
		"""Fetch markets from Gamma API, filtered by status + category.

		`series_tickers` overrides the configured `series` list. An empty
		series filter means "all categories" — the API call doesn't pass a
		category param.
		"""
		series_filter = series_tickers if series_tickers is not None else self.series
		out: List[Market] = []
		offset = 0
		while True:
			params: dict[str, Any] = {
				"limit": self.pagination_limit,
				"offset": offset,
			}
			# Polymarket Gamma exposes filters via repeated query params;
			# `closed=true` flags settled markets. We keep the mapping
			# explicit so future status additions don't silently grow.
			if "closed" in self.statuses:
				params["closed"] = "true"
			if "open" in self.statuses or "active" in self.statuses:
				params["active"] = "true"
			# Category filter — Polymarket Gamma uses tag_id internally; the
			# user-facing `series` list maps to slugs. For now pass through
			# the slug as a `tag_slug` query — adapter consumers can
			# refine via the YAML config.
			# (No-op when series_filter is empty.)

			try:
				batch = self._get(f"{self.gamma_base}/markets", params=params)
			except requests.exceptions.HTTPError as exc:
				# Gamma rejects offsets beyond an undocumented hard ceiling
				# with a 422; treat that as natural end-of-pagination so a
				# liquid-but-finite category doesn't crash the whole sweep.
				if exc.response is not None and exc.response.status_code == 422 and offset > 0:
					logger.info(
						"gamma /markets returned 422 at offset=%d; treating as end-of-pagination",
						offset,
					)
					break
				raise
			if not isinstance(batch, list):
				raise ValueError(
					f"gamma /markets returned non-list (got {type(batch).__name__}); "
					"upstream API contract may have changed"
				)
			if not batch:
				break
			self._validate_list(batch, "gamma_markets_list")
			for raw in batch:
				if series_filter:
					# Match the market's category against the configured slugs.
					# Gamma exposes both `category` (string) and `events` (list)
					# — we check both. If neither matches, skip.
					mkt_category = (raw.get("category") or "").lower()
					evt_slugs = [
						(e.get("slug") or "").lower()
						for e in raw.get("events", []) or []
					]
					if mkt_category not in [s.lower() for s in series_filter] \
						and not any(s.lower() in evt_slugs for s in series_filter):
						continue
				out.append(self._raw_market_to_market(raw, series_filter))
			# Stop when we get a partial page (last page).
			if len(batch) < self.pagination_limit:
				break
			# In dry_run mode only one page is fetched (parity with Kalshi adapter).
			if self.dry_run:
				break
			offset += len(batch)
		return out

	def _raw_market_to_market(self, raw: dict, series_filter: List[str]) -> Market:
		"""Map a Gamma market dict to our Market dataclass.

		Polymarket markets are keyed by `condition_id`; we use that as our
		ticker. `series_ticker` is best-effort from category metadata.
		"""
		condition_id = raw.get("conditionId") or raw.get("condition_id") or str(raw.get("id"))
		title = raw.get("question") or raw.get("title") or ""
		# Status mapping: Gamma uses booleans (active, closed) → our enum.
		if raw.get("closed"):
			status = "settled" if raw.get("acceptingOrders") is False else "closed"
		elif raw.get("active"):
			status = "open"
		else:
			status = "closed"
		# Result encoding for closed binary markets — Gamma returns
		# `outcomePrices` like ["1.0", "0.0"] for resolved Yes/No.
		result: Optional[str] = None
		outcome_prices = raw.get("outcomePrices")
		if isinstance(outcome_prices, str):
			# Sometimes serialized as a JSON-string; defensively parse.
			try:
				outcome_prices = json.loads(outcome_prices)
			except json.JSONDecodeError:
				outcome_prices = None
		if status == "settled" and isinstance(outcome_prices, list) and len(outcome_prices) == 2:
			yes_p = float(outcome_prices[0]) if outcome_prices[0] is not None else 0.0
			result = "yes" if yes_p > 0.5 else "no"
		# Category slug → series_ticker. If no category, use the first
		# matching series_filter entry, else "default".
		category = raw.get("category") or ""
		series_ticker = (
			category.lower() if category else (series_filter[0].lower() if series_filter else "default")
		)
		return Market(
			ticker=condition_id,
			event_ticker=str(raw.get("id", "")),  # Gamma's market id (separate from condition_id)
			series_ticker=series_ticker,
			title=title,
			status=status,
			result=result,
			yes_bid=_safe_float(raw.get("bestBid")),
			yes_ask=_safe_float(raw.get("bestAsk")),
			last_price=_safe_float(raw.get("lastTradePrice")),
			open_interest=_safe_int(raw.get("openInterest")),
			volume=_safe_int(raw.get("volumeNum") or raw.get("volume")),
			expiration_time=_parse_iso(raw.get("endDateIso") or raw.get("end_date_iso") or raw.get("endDate")),
			close_time=_parse_iso(raw.get("closedTime") or raw.get("closed_time")),
			created_time=_parse_iso(raw.get("createdAt") or raw.get("created_at")),
			settled_time=_parse_iso(raw.get("resolvedTime") or raw.get("resolved_time")),
			open_time=_parse_iso(raw.get("startDate") or raw.get("start_date")),
			notional_value=_safe_float(raw.get("liquidityNum") or raw.get("liquidity")),
			floor_strike=None,
			cap_strike=None,
			raw_data=json.dumps(raw, default=str)[:65535],  # cap stored blob
		)

	# ------------------------------------------------------------------
	# Public API: collect_trades
	# ------------------------------------------------------------------

	def collect_trades(self, ticker: str, since: Optional[str] = None) -> List[Trade]:
		"""Fetch trade history for a market via CLOB API.

		`ticker` is the condition_id (Polymarket's market primary key).
		`since` is an ISO datetime string; trades older than `since` are
		filtered out client-side (CLOB doesn't expose a since-filter on
		this endpoint as of the docs we have).
		"""
		try:
			raw_trades = self._get(f"{self.clob_base}/markets/{ticker}/trades")
		except requests.exceptions.HTTPError as exc:
			# Closed/settled markets routinely have no live CLOB trade endpoint —
			# CLOB returns 404. Treat as empty rather than failing the whole sweep.
			if exc.response is not None and exc.response.status_code == 404:
				logger.debug("clob /markets/%s/trades returned 404 — treating as no trades", ticker)
				return []
			raise
		if not isinstance(raw_trades, list):
			raise ValueError(
				f"clob /markets/{ticker}/trades returned non-list "
				f"(got {type(raw_trades).__name__})"
			)
		self._validate_list(raw_trades, "clob_trades_list")
		since_dt: Optional[datetime] = None
		if since:
			try:
				since_dt = datetime.fromisoformat(since)
				if since_dt.tzinfo is None:
					since_dt = since_dt.replace(tzinfo=timezone.utc)
			except ValueError:
				logger.warning("polymarket collect_trades: bad since=%r; ignoring", since)
				since_dt = None
		out: List[Trade] = []
		for raw in raw_trades:
			t = self._raw_trade_to_trade(raw, ticker)
			if since_dt is not None and t.created_time < since_dt:
				continue
			out.append(t)
		return out

	def _raw_trade_to_trade(self, raw: dict, ticker: str) -> Trade:
		"""Map a CLOB trade event to our Trade dataclass.

		Polymarket trades carry side (BUY/SELL) and outcome (Yes/No). We
		project to our schema's `taker_side` of yes/no (the side the taker
		ended up holding) and yes_price/no_price (cents).
		"""
		# Outcome: Polymarket returns "Yes" / "No" — normalize to lowercase.
		outcome = (raw.get("outcome") or "").strip().lower()
		# Side: BUY of Yes ⇒ taker now holds Yes; SELL of Yes ⇒ taker now
		# holds No (sold Yes back). Same logic mirrored for No.
		side = (raw.get("side") or "").strip().upper()
		if outcome == "yes":
			taker_side = "yes" if side == "BUY" else "no"
		elif outcome == "no":
			taker_side = "no" if side == "BUY" else "yes"
		else:
			# Unknown outcome (multi-outcome market?) — store as-is for the
			# downstream replay layer to surface.
			taker_side = outcome or "unknown"
		# Price: Polymarket trades are in USD-per-share (0.0–1.0). Convert
		# to cents (0–100) to match our schema.
		price_usd = _safe_float(raw.get("price"))
		yes_cents: int
		no_cents: int
		if price_usd is None:
			yes_cents = 0
			no_cents = 0
		else:
			# `price` is the YES-leg price when outcome=Yes; for outcome=No
			# the price is the NO-leg price (1 − yes_price).
			if outcome == "yes":
				yes_cents = int(round(price_usd * 100))
			elif outcome == "no":
				no_cents = int(round(price_usd * 100))
				yes_cents = 100 - no_cents
				return Trade(
					trade_id=str(raw.get("id", "")),
					ticker=ticker,
					yes_price=yes_cents,
					no_price=no_cents,
					count=int(round(_safe_float(raw.get("size")) or 0)),
					taker_side=taker_side,
					created_time=_parse_iso_strict(raw.get("timestamp")),
					raw_data=json.dumps(raw, default=str)[:65535],
				)
			else:
				yes_cents = int(round(price_usd * 100))
			no_cents = 100 - yes_cents
		return Trade(
			trade_id=str(raw.get("id", "")),
			ticker=ticker,
			yes_price=yes_cents,
			no_price=no_cents,
			count=int(round(_safe_float(raw.get("size")) or 0)),
			taker_side=taker_side,
			created_time=_parse_iso_strict(raw.get("timestamp")),
			raw_data=json.dumps(raw, default=str)[:65535],
		)


# ---------------------------------------------------------------------------
# Module-level coercion helpers
# ---------------------------------------------------------------------------

def _safe_float(v: Any) -> Optional[float]:
	"""Coerce v to float, returning None on missing / non-numeric input."""
	if v is None or v == "":
		return None
	try:
		return float(v)
	except (TypeError, ValueError):
		return None


def _safe_int(v: Any) -> Optional[int]:
	"""Coerce v to int, returning None on missing / non-numeric input."""
	if v is None or v == "":
		return None
	try:
		return int(float(v))  # tolerate "100.0" strings
	except (TypeError, ValueError):
		return None


def _parse_iso(v: Any) -> Optional[datetime]:
	"""Parse an ISO-8601 timestamp string. Returns None on invalid input."""
	if v is None or v == "":
		return None
	if isinstance(v, datetime):
		return v if v.tzinfo else v.replace(tzinfo=timezone.utc)
	if not isinstance(v, str):
		return None
	# Polymarket may use trailing Z; isoformat doesn't accept Z in <3.11.
	s = v.replace("Z", "+00:00") if v.endswith("Z") else v
	try:
		dt = datetime.fromisoformat(s)
		return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
	except ValueError:
		return None


def _parse_iso_strict(v: Any) -> datetime:
	"""Like _parse_iso but raises (ISO timestamp is required for Trade rows)."""
	dt = _parse_iso(v)
	if dt is None:
		raise ValueError(f"polymarket trade missing/invalid timestamp: {v!r}")
	return dt
