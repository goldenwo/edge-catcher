from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, List, Optional

from edge_catcher.fees import FeeModel
from edge_catcher.storage.models import Market, Trade


class PredictionMarketAdapter(ABC):
	"""Abstract contract for prediction-market adapters (markets + trades shape).

	This is the Kalshi-shaped contract: the adapter downloads market metadata
	plus per-market trade history. Polymarket and other prediction markets
	would inherit from this.

	Exchanges with a different shape (e.g. Coinbase OHLC candles) do NOT
	inherit — they expose their own interface. The AdapterMeta entry still
	registers them; only the implementation class differs.
	"""

	@abstractmethod
	def collect_markets(self, series_tickers: Optional[List[str]] = None) -> List[Market]:
		"""Download/refresh market metadata + settlements for given series."""
		...

	@abstractmethod
	def collect_trades(self, ticker: str, since: Optional[str] = None) -> List[Trade]:
		"""Download trade history for a market. since = ISO datetime string."""
		...

	@abstractmethod
	def validate_response(self, data: dict, schema_key: str) -> bool:
		"""Validate API response against expected schema. Raise ValueError on failure."""
		...

	def stream_realtime(self):
		"""WebSocket/polling live feed. Override for Phase 5 live collectors."""
		raise NotImplementedError("stream_realtime not implemented for this adapter")


@dataclass
class AdapterMeta:
	"""Metadata for a data adapter. See docs/adr/0001-adapter-registry.md.

	Contract:
	- `exchange`, `db_file`, and `fee_model` MUST be specified explicitly.
	- Exchange-specific fields go in `extra`, not as typed attributes.
	- Each exchange lives in edge_catcher/adapters/<exchange>/ with its
	  own registry.py and fees.py (if applicable).
	"""
	# Identity (all required)
	id: str
	exchange: str
	name: str
	description: str
	db_file: str
	fee_model: FeeModel

	# Optional
	requires_api_key: bool = False
	api_key_env_var: Optional[str] = None
	default_start_date: Optional[str] = None
	markets_yaml: Optional[str] = None
	fee_overrides: dict[str, FeeModel] = field(default_factory=dict)
	extra: dict[str, Any] = field(default_factory=dict)
