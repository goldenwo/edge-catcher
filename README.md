# edge-catcher

A production-grade hypothesis testing pipeline for detecting pricing inefficiencies in prediction markets, sports betting, and options.

Built for rigorous, anti-p-hacking statistical research: pre-registered hypotheses, out-of-sample validation, clustered standard errors, and multi-comparison correction.

---

## Features

- **Multi-market adapter pattern** — Kalshi supported out of the box; plug in sports or options adapters
- **Incremental data pipeline** — SQLite with WAL mode, resumable downloads, 90-day rolling archive
- **Rigorous statistics** — `proportions_ztest` for binary outcomes, clustered SEs by expiration date, Harvey-Liu-Zhu threshold (t > 3.0), Bonferroni correction
- **5-verdict system** — `INSUFFICIENT_DATA` / `NO_EDGE` / `INCONCLUSIVE` / `EDGE_EXISTS` / `EDGE_NOT_TRADEABLE`
- **Fee-adjusted edge** — Config-driven fee models (Kalshi maker/taker) applied before any verdict
- **AI-powered workflow (optional)** — Describe hypotheses in plain English, get structured configs; interpret results in natural language
- **No auto-trading** — Research and alerting only

---

## Architecture

```
edge-catcher/
├── edge_catcher/
│   ├── adapters/         # Market data collectors (Kalshi, ...)
│   ├── hypotheses/       # Statistical hypothesis modules
│   │   ├── examples/     # Example hypothesis template
│   │   ├── kalshi/       # Kalshi-specific hypotheses
│   │   └── registry.py   # Auto-discovery + multi-comparison correction
│   ├── ai/               # Optional AI integration
│   │   ├── client.py     # LLM client (Anthropic, OpenAI, OpenRouter)
│   │   ├── formalizer.py # English → hypothesis config + stub
│   │   ├── interpreter.py# Analysis JSON → English summary
│   │   └── prompts/      # Editable system prompts
│   ├── runner/           # Backtest orchestration
│   ├── storage/          # SQLite persistence layer
│   └── reports/          # JSON → human-readable formatting
├── config/
│   ├── markets.yaml      # Adapter config (series, rate limits)
│   ├── fees.yaml         # Fee models per market
│   └── hypotheses.yaml   # Hypothesis configs and thresholds
└── tests/                # pytest suite (60 tests)
```

---

## Quickstart

**Requirements:** Python 3.11+, a Kalshi API key (free at [kalshi.com](https://kalshi.com))

```bash
# Clone and install
git clone https://github.com/goldenwo/edge-catcher.git
cd edge-catcher
pip install -e ".[dev]"

# Set your API key
cp .env.example .env
# Edit .env: KALSHI_API_KEY=your_key_here

# Download market data (Kalshi BTC series by default)
python -m edge_catcher download

# Run all registered hypotheses
python -m edge_catcher analyze

# Archive trades older than 90 days
python -m edge_catcher archive
```

---

## Configuration

### Markets (`config/markets.yaml`)

```yaml
adapters:
  kalshi:
    enabled: true
    series:
      - KXBTC    # BTC hourly range contracts
      - KXBTCD   # BTC daily contracts
    statuses:
      - settled
```

Add any Kalshi series ticker to pull that market's data.

### Fees (`config/fees.yaml`)

```yaml
kalshi:
  maker:
    formula: "0.0175 * P * (1 - P)"
  taker:
    formula: "0.07 * P * (1 - P)"
```

Fees are applied to the edge calculation before the verdict is issued.

---

## Writing a Hypothesis

Create a module under `edge_catcher/hypotheses/<market>/your_hypothesis.py`:

```python
from edge_catcher.hypotheses.base import HypothesisResult

HYPOTHESIS_ID = "my_hypothesis"

def run(db_conn, config_path) -> HypothesisResult:
    # 1. Query settled markets from db_conn
    # 2. Compute price signal (VWAP or last_price)
    # 3. Bucket by implied probability
    # 4. Run proportions_ztest per bucket
    # 5. Cluster by expiration date
    # 6. Return HypothesisResult with verdict + per-bucket stats
    ...
```

Register it in `config/hypotheses.yaml`:

```yaml
hypotheses:
  my_hypothesis:
    name: "My Hypothesis"
    module: "edge_catcher.hypotheses.my_market.my_hypothesis"
    market: kalshi
    status: exploratory   # or confirmatory
    thresholds:
      t_stat: 3.0
      min_n_per_bucket: 30
      min_independent_obs: 80
```

The registry discovers it automatically and applies Bonferroni correction when multiple hypotheses run together.

---

## AI-Powered Workflow (Optional)

Edge-catcher includes optional AI features that let you describe hypotheses in plain English and get human-readable research summaries. **AI is completely optional** — the core pipeline (download, analyze, archive) works without any API key.

### Setup

```bash
# Install with AI support
pip install -e ".[ai]"

# Set your LLM API key (pick one)
export ANTHROPIC_API_KEY=sk-ant-...    # Anthropic (recommended)
export OPENAI_API_KEY=sk-...           # OpenAI
export OPENROUTER_API_KEY=sk-or-...    # OpenRouter (any model)
```

Provider is auto-detected from whichever API key is set (priority: Anthropic → OpenAI → OpenRouter). Override with `--provider` or `EDGE_CATCHER_LLM_PROVIDER` env var.

### Full Workflow

```
 FORMALIZE ──→ IMPLEMENT ──→ DOWNLOAD ──→ ANALYZE ──→ INTERPRET
    🤖             👤            ⚙️           ⚙️          🤖
   (AI)        (you code)    (Python)     (Python)      (AI)
  ~$0.01         free          free         free       ~$0.002
```

AI bookends the process. The entire data pipeline and statistical engine runs locally — no tokens burned, no data sent to any LLM during download or analysis.

### Step 1: Formalize a Hypothesis

Describe your market hunch in plain English:

```bash
python -m edge_catcher formalize "I think Kalshi election contracts for \
  longshot candidates are overpriced because people overestimate underdogs"
```

This calls an LLM (Sonnet by default) which generates:
- A new entry in `config/hypotheses.yaml` with proper buckets, thresholds, and fee model
- A stub Python module at `edge_catcher/hypotheses/custom/<hypothesis_id>.py`
- Instructions on what to do next

### Step 2: Implement Your Test

Edit the generated stub module to add your statistical logic. The example template at `edge_catcher/hypotheses/examples/example_hypothesis.py` shows the standard pattern:

1. Query settled markets from the SQLite database
2. Compute a price signal per market (VWAP or last_price)
3. Bucket contracts by implied probability
4. Run `proportions_ztest` per bucket
5. Cluster standard errors by expiration date
6. Return a `HypothesisResult` with verdict and per-bucket statistics

### Step 3: Download & Analyze

```bash
# Download settled market data (pure Python, no AI)
python -m edge_catcher download

# Run your hypothesis (pure Python + scipy/statsmodels)
python -m edge_catcher analyze --hypothesis your_hypothesis_id
```

### Step 4: Interpret Results

Turn the raw JSON analysis into a plain-English research summary:

```bash
python -m edge_catcher interpret
# or specify a specific report:
python -m edge_catcher interpret reports/latest_analysis.json
```

This calls an LLM (Haiku by default) which reads your analysis JSON and outputs a structured summary including:
- The verdict (e.g., "This analysis found EDGE_EXISTS")
- Key findings per price bucket
- Fee-adjusted edge and tradeability assessment
- Caveats, limitations, and suggested next steps

### CLI Options

```bash
# Override provider or model
python -m edge_catcher formalize --provider openai --model gpt-4o "your hypothesis"
python -m edge_catcher interpret --provider anthropic --model claude-haiku-4-20250414

# Works with any OpenRouter model
export OPENROUTER_API_KEY=sk-or-...
python -m edge_catcher formalize --provider openrouter --model meta-llama/llama-4-70b "your hypothesis"
```

### What AI Does vs. Doesn't Do

| | Uses AI | Runs Locally |
|---|:-:|:-:|
| Formalize (English → config) | ✅ | |
| Implement (write test logic) | | ✅ (you) |
| Download (fetch market data) | | ✅ |
| Analyze (statistical tests) | | ✅ |
| Interpret (JSON → English) | ✅ | |

AI never touches your data pipeline or statistical analysis. It only translates at the human boundaries.

---

## Statistical Methodology

### Why t > 3.0 instead of 1.96?

The Harvey-Liu-Zhu (2016) threshold of t > 3.0 corrects for multiple comparison bias in financial research. At t = 1.96, ~5% of random noise passes significance. Requiring t > 3.0 drops the false discovery rate to <0.3%.

### Why clustered standard errors?

Contracts expiring on the same date share a common shock (e.g., BTC price at 3pm). Treating them as independent inflates the effective sample size and produces misleadingly small p-values. We cluster by expiration date and use the within-cluster majority outcome as the unit of observation.

### Why `proportions_ztest` not t-test?

Binary outcomes (YES/NO) follow a Bernoulli distribution. The z-test for proportions is the correct test; t-tests assume normally distributed residuals, which doesn't hold for 0/1 data.

### Verdict logic

```
INSUFFICIENT_DATA    → n < 30 per bucket or < 80 independent observations
NO_EDGE              → no bucket clears t > 3.0
INCONCLUSIVE         → mixed signal (some buckets significant, some not)
EDGE_EXISTS          → signal clears HLZ threshold, edge survives fees
EDGE_NOT_TRADEABLE   → signal is real but fee-adjusted edge ≤ 0
```

---

## Running Tests

```bash
pytest tests/ -v
```

All 60 tests run against mocked API responses — no live API key needed.

---

## Adding a New Market Adapter

Implement the base interface in `edge_catcher/adapters/`:

```python
from edge_catcher.adapters.base import BaseAdapter

class MyAdapter(BaseAdapter):
    def collect_markets(self, series_tickers=None) -> List[Market]:
        ...
    def collect_trades(self, ticker: str) -> List[Trade]:
        ...
```

The `Market` and `Trade` dataclasses are shared across all adapters — hypotheses don't need to know which adapter produced the data.

---

## License

MIT
