"""Polymarket REST adapter — Gamma (markets metadata) + data-api (trades).

Both APIs are public and unauthenticated for read paths. We hit:
  GET https://gamma-api.polymarket.com/markets    — markets list with filters
  GET https://gamma-api.polymarket.com/markets/{id}  — single market detail
  GET https://data-api.polymarket.com/trades?market={conditionId} — trade
      history (rows NEWEST-first, `limit`/`offset` paging)

The old CLOB trade endpoint (GET clob.polymarket.com/markets/{id}/trades)
is GONE — it 404s for every market as of 2026-07 (verified live
2026-07-27). The adapter joins the two live APIs — Gamma supplies the
listing layer (paginated + filterable by status / category / endDate
window), data-api supplies per-market trade events keyed by `condition_id`.

Mapping notes:
  - Polymarket markets are typically binary (Yes/No) with a `condition_id`
    as primary key and per-outcome `tokens` (ERC1155 token IDs).
  - We use the condition_id as our `ticker` field for storage parity with
    Kalshi's market-ticker convention. Trade rows carry `outcomeIndex`
    (0 = yes-leg token, 1 = no-leg token) + `side` (BUY/SELL); taker_side
    is derived from that pair — the `outcome` label is display-only (team
    names on sports markets, "Yes"/"No" on binaries).
  - Gamma list-ish fields (`outcomes`, `outcomePrices`, `clobTokenIds`)
    arrive as JSON-in-STRING; prices (`bestBid`/`bestAsk`/`lastTradePrice`)
    are 0–1 USD floats and are stored as integer cents for parity with
    Trade and the Kalshi adapter.
  - `series_ticker` defaults to a category slug (politics/sports/crypto)
    derived from market metadata when present, falling back to "default".
"""
from __future__ import annotations

import hashlib
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
	# data-api /trades rows (verified live 2026-07-27): no `id` field exists;
	# `timestamp` is an epoch int (unix seconds); `outcome` is a display
	# label only (team names on sports) so it is NOT required here.
	"clob_trades_list": {
		"required": [],  # top-level list[dict]
		"item_required": [
			"side", "size", "price", "timestamp",
			"conditionId", "outcomeIndex", "transactionHash",
		],
	},
}


class PolymarketAdapter(PredictionMarketAdapter):
	"""Polymarket REST adapter (Gamma + data-api public APIs, no auth)."""

	GAMMA_BASE = "https://gamma-api.polymarket.com"
	# Legacy CLOB base — its /markets/{id}/trades endpoint is dead (404s);
	# retained for config compatibility only.
	CLOB_BASE = "https://clob.polymarket.com"
	DATA_BASE = "https://data-api.polymarket.com"
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
		self.data_base: str = poly_cfg.get("data_base", self.DATA_BASE).rstrip("/")
		self.rate_limit_seconds: float = float(poly_cfg.get("rate_limit_seconds", 0.5))
		# `series` here is a list of category-slug strings (e.g. ["politics",
		# "sports"]); empty list means "all categories".
		self.series: List[str] = poly_cfg.get("series", [])
		pagination_cfg = poly_cfg.get("pagination") or {}
		self.pagination_limit: int = int(pagination_cfg.get("default_limit", 100))
		# Defensive per-market page cap on /trades paging — a runaway market
		# (or an offset param the API stops honoring) must not loop forever.
		self.max_trade_pages: int = int(pagination_cfg.get("max_trade_pages", 200))
		# Status filter — defaults to ["closed"] (settled markets) for
		# backtest-friendly historical data.
		self.statuses: List[str] = poly_cfg.get("statuses", ["closed"])
		# Optional Gamma /markets server-side sort + endDate window params
		# (all verified binding live 2026-07-27). None/absent → omitted.
		self.order: Optional[str] = poly_cfg.get("order")
		self.ascending: Optional[bool] = poly_cfg.get("ascending")
		self.end_date_min: Optional[str] = poly_cfg.get("end_date_min")
		self.end_date_max: Optional[str] = poly_cfg.get("end_date_max")
		self.dry_run: bool = dry_run
		self.min_available_ram_pct: float = float(
			poly_cfg.get("min_available_ram_pct", 10)
		)

		# Trade-path diagnostics — a silent 404→[] here hid a dead trades
		# endpoint for 12 weeks (0 trades stored, no signal). Counted and
		# logged so sweeps surface endpoint drift instead of swallowing it.
		self.trade_404_count: int = 0
		self.trade_empty_count: int = 0
		self.non_binary_skipped_count: int = 0
		# conditionIds flagged non-binary during collect_markets (negRisk or
		# >2 outcomes) — collect_trades skips these outright.
		self._non_binary_tickers: set[str] = set()

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
			# Server-side sort + endDate window (all bind; verified live
			# 2026-07-27). `ascending` must serialize as lowercase.
			if self.order:
				params["order"] = self.order
			if self.ascending is not None:
				params["ascending"] = "true" if self.ascending else "false"
			if self.end_date_min:
				params["end_date_min"] = self.end_date_min
			if self.end_date_max:
				params["end_date_max"] = self.end_date_max
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
				market = self._raw_market_to_market(raw, series_filter)
				# Non-binary markets (negRisk or >2 outcomes) keep their
				# metadata row but are flagged so collect_trades skips them —
				# their fills can't be stored as yes/no taker sides.
				if _is_non_binary_market(raw):
					self._non_binary_tickers.add(market.ticker)
				out.append(market)
			# Stop when we get a partial page (last page).
			if len(batch) < self.pagination_limit:
				break
			# In dry_run mode only one page is fetched (parity with Kalshi adapter).
			if self.dry_run:
				break
			offset += len(batch)
		if self._non_binary_tickers:
			logger.info(
				"polymarket collect_markets: %d non-binary markets flagged — "
				"trades collection will be skipped for them",
				len(self._non_binary_tickers),
			)
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
			# bestBid/bestAsk/lastTradePrice arrive as 0–1 USD floats — store
			# integer cents for unit parity with Trade and the Kalshi adapter
			# (raw_data keeps the original full-precision values).
			yes_bid=_usd_to_cents(raw.get("bestBid")),
			yes_ask=_usd_to_cents(raw.get("bestAsk")),
			last_price=_usd_to_cents(raw.get("lastTradePrice")),
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
		"""Fetch trade history for a market via the public data-api.

		`ticker` is the condition_id (Polymarket's market primary key).
		`since` is an ISO datetime string. Rows come back NEWEST-first, so
		the bound is applied as an EARLY-STOP during pagination — once a
		page crosses it, no further (older) pages are requested.

		Failure visibility: 404s and zero-trade markets are counted on the
		adapter (`trade_404_count` / `trade_empty_count`) and logged at
		WARNING with the market id. The previous silent 404→[] here hid a
		dead trades endpoint for 12 weeks.
		"""
		if ticker in self._non_binary_tickers:
			self.non_binary_skipped_count += 1
			logger.info(
				"polymarket collect_trades: skipping non-binary market %s "
				"(non_binary_skipped=%d)",
				ticker, self.non_binary_skipped_count,
			)
			return []

		since_dt = _parse_since(since)
		out: List[Trade] = []
		offset = 0
		pages = 0
		dropped_rows = 0
		stop = False
		while not stop:
			if pages >= self.max_trade_pages:
				logger.warning(
					"polymarket /trades market=%s: hit defensive page cap "
					"(%d pages × %d rows) — truncating",
					ticker, self.max_trade_pages, self.pagination_limit,
				)
				break
			params: dict[str, Any] = {
				"market": ticker,
				"limit": self.pagination_limit,
				"offset": offset,
			}
			try:
				batch = self._get(f"{self.data_base}/trades", params=params)
			except requests.exceptions.HTTPError as exc:
				if exc.response is not None and exc.response.status_code == 404:
					self.trade_404_count += 1
					logger.warning(
						"polymarket /trades returned 404 for market %s at offset=%d "
						"(404_count=%d) — possible endpoint drift; keeping %d trades "
						"fetched so far",
						ticker, offset, self.trade_404_count, len(out),
					)
					return out
				raise
			pages += 1
			if not isinstance(batch, list):
				raise ValueError(
					f"polymarket /trades for {ticker} returned non-list "
					f"(got {type(batch).__name__})"
				)
			if not batch:
				if offset == 0:
					# Zero trades for a market we expected data on — this is
					# the exact silence that masked the CLOB breakage, so it
					# is counted and logged, not swallowed.
					self.trade_empty_count += 1
					logger.warning(
						"polymarket /trades returned 0 rows for market %s (empty_count=%d)",
						ticker, self.trade_empty_count,
					)
				# offset > 0: total % limit == 0 — natural end of pagination.
				break
			self._validate_list(batch, "clob_trades_list")
			for raw in batch:
				if raw.get("outcomeIndex") not in (0, 1):
					# Non-binary leg (belt-and-braces for markets never seen
					# by collect_markets, e.g. --skip-market-scan restarts).
					# Never store taker_side 'unknown' rows.
					dropped_rows += 1
					continue
				t = self._raw_trade_to_trade(raw, ticker)
				if since_dt is not None and t.created_time < since_dt:
					# Newest-first ordering: everything after this row —
					# including all further pages — is older. Stop.
					stop = True
					break
				out.append(t)
			if len(batch) < self.pagination_limit:
				break
			offset += len(batch)
		if dropped_rows:
			logger.warning(
				"polymarket /trades market=%s: dropped %d rows with outcomeIndex "
				"outside 0/1 — non-binary legs are not storable as yes/no",
				ticker, dropped_rows,
			)
		return out

	def _raw_trade_to_trade(self, raw: dict, ticker: str) -> Trade:
		"""Map a data-api trade row to our Trade dataclass.

		Rows carry `side` (BUY/SELL of the traded outcome token) and
		`outcomeIndex` (0 = yes-leg token, 1 = no-leg token). The `outcome`
		field is a display label ("Yes"/"No" on binaries, team names on
		sports markets) and is NOT used for side mapping. taker_side is the
		side the taker ends up holding: BUY of leg 0 → "yes", SELL of leg 0
		→ "no", mirrored for leg 1. `price` is the traded leg's own 0–1 USD
		price; converted to integer cents. `timestamp` is an epoch int in
		unix seconds. Rows have no server id — trade_id is synthesized
		deterministically (see _synth_trade_id).

		Caller guarantees outcomeIndex ∈ {0, 1} (collect_trades drops the rest).
		"""
		side = (raw.get("side") or "").strip().upper()
		outcome_index = raw.get("outcomeIndex")
		if outcome_index == 0:
			taker_side = "yes" if side == "BUY" else "no"
		else:  # outcome_index == 1
			taker_side = "no" if side == "BUY" else "yes"
		price_usd = _safe_float(raw.get("price"))
		yes_cents: int
		no_cents: int
		if price_usd is None:
			yes_cents = 0
			no_cents = 0
		elif outcome_index == 0:
			yes_cents = int(round(price_usd * 100))
			no_cents = 100 - yes_cents
		else:
			no_cents = int(round(price_usd * 100))
			yes_cents = 100 - no_cents
		return Trade(
			trade_id=_synth_trade_id(raw),
			ticker=ticker,
			yes_price=yes_cents,
			no_price=no_cents,
			count=int(round(_safe_float(raw.get("size")) or 0)),
			taker_side=taker_side,
			created_time=_parse_epoch(raw.get("timestamp")),
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


def _parse_epoch(v: Any) -> datetime:
	"""Parse a unix-seconds epoch (data-api `timestamp` is an epoch int) to a
	tz-aware UTC datetime. Raises ValueError on missing/invalid input —
	Trade rows require a timestamp. NOT for ISO strings (see _parse_iso)."""
	if v is None or isinstance(v, bool) or v == "":
		raise ValueError(f"polymarket trade missing/invalid epoch timestamp: {v!r}")
	try:
		ts = float(v)
	except (TypeError, ValueError):
		raise ValueError(
			f"polymarket trade missing/invalid epoch timestamp: {v!r}"
		) from None
	return datetime.fromtimestamp(ts, tz=timezone.utc)


def _parse_since(since: Optional[str]) -> Optional[datetime]:
	"""Parse a `since` ISO bound to tz-aware UTC; warn + ignore on garbage."""
	if not since:
		return None
	dt = _parse_iso(since)
	if dt is None:
		logger.warning("polymarket collect_trades: bad since=%r; ignoring", since)
	return dt


def _usd_to_cents(v: Any) -> Optional[int]:
	"""Convert a 0–1 USD price to integer cents (0–100) — unit parity with
	Trade prices and the Kalshi adapter's cents convention."""
	f = _safe_float(v)
	if f is None:
		return None
	return int(round(f * 100))


def _synth_trade_id(raw: dict) -> str:
	"""Deterministic trade id for data-api rows, which carry no server id.

	transactionHash alone is NOT unique — one transaction can settle several
	fills — so the id hashes the fill-identifying tuple (tx, asset,
	timestamp, size, price, side). sha256 (not Python's salted hash()) so
	ids are stable across runs, making trades-table upserts idempotent.
	"""
	key = "|".join((
		str(raw.get("transactionHash", "")),
		str(raw.get("asset", "")),
		str(raw.get("timestamp", "")),
		str(raw.get("size", "")),
		str(raw.get("price", "")),
		str(raw.get("side", "")),
	))
	return hashlib.sha256(key.encode("utf-8")).hexdigest()[:32]


def _is_non_binary_market(raw: dict) -> bool:
	"""True when a Gamma market is not a plain two-outcome binary — negRisk
	event legs or >2 outcomes. Gamma serializes `outcomes` as JSON-in-STRING
	(e.g. '["Yes", "No"]'); parsed defensively."""
	if raw.get("negRisk") or raw.get("neg_risk"):
		return True
	outcomes = raw.get("outcomes")
	if isinstance(outcomes, str):
		try:
			outcomes = json.loads(outcomes)
		except json.JSONDecodeError:
			return False
	return isinstance(outcomes, list) and len(outcomes) > 2
