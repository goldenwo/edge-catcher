from abc import ABC, abstractmethod
from typing import List, Optional
from edge_catcher.storage.models import Market, Trade


class MarketAdapter(ABC):
    """Abstract interface for market data collectors."""

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
