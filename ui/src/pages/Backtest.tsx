import { useEffect, useRef, useState } from 'react'
import { api, StrategyInfo, BacktestHistoryItem } from '../api'
import { usePipeline } from '../components/PipelineStatus'
import EquityCurve from '../components/EquityCurve'

interface BacktestResult {
  total_trades?: number
  wins?: number
  losses?: number
  win_rate?: number
  net_pnl_cents?: number
  sharpe?: number
  max_drawdown_cents?: number
  per_strategy?: Record<string, Record<string, unknown>>
  equity_curve?: [string, number][]
  trade_log?: Record<string, unknown>[]
  [key: string]: unknown
}

function fmt(n: unknown, decimals = 2): string {
  if (n == null) return '--'
  const num = Number(n)
  return isNaN(num) ? '--' : num.toFixed(decimals)
}

function fmtDollar(cents: unknown): string {
  if (cents == null) return '--'
  const num = Number(cents)
  return isNaN(num) ? '--' : `$${(num / 100).toFixed(2)}`
}

function fmtPct(n: unknown): string {
  if (n == null) return '--'
  const num = Number(n)
  return isNaN(num) ? '--' : `${(num * 100).toFixed(1)}%`
}

export default function Backtest() {
  const { status: pipeline, loading: pipelineLoading, refresh: refreshPipeline } = usePipeline()

  // Config form state
  const [seriesList, setSeriesList] = useState<string[]>([])
  const [strategiesList, setStrategiesList] = useState<StrategyInfo[]>([])
  const [series, setSeries] = useState<string>('')
  const [selectedStrategies, setSelectedStrategies] = useState<Set<string>>(new Set())
  const [startDate, setStartDate] = useState('')
  const [endDate, setEndDate] = useState('')
  const [showAdvanced, setShowAdvanced] = useState(false)
  const [cash, setCash] = useState(10000)
  const [slippage, setSlippage] = useState(1)
  const [tp, setTp] = useState<string>('')
  const [sl, setSl] = useState<string>('')
  const [minPrice, setMinPrice] = useState<string>('')
  const [maxPrice, setMaxPrice] = useState<string>('')

  // Run state
  const [taskId, setTaskId] = useState<string | null>(null)
  const [running, setRunning] = useState(false)
  const [progress, setProgress] = useState('')
  const [tradesProcessed, setTradesProcessed] = useState<number | null>(null)
  const [tradesEstimated, setTradesEstimated] = useState<number | null>(null)
  const [livePnl, setLivePnl] = useState<number | null>(null)
  const [result, setResult] = useState<BacktestResult | null>(null)
  const [error, setError] = useState<string | null>(null)

  // History
  const [history, setHistory] = useState<BacktestHistoryItem[]>([])
  const [showHistory, setShowHistory] = useState(false)

  const intervalRef = useRef<ReturnType<typeof setInterval>>()

  const hasData = pipeline?.data.has_data ?? false
  const hasStrategies = (pipeline?.strategies.count ?? 0) > 0
  const prerequisitesMet = hasData && hasStrategies

  // Load series, strategies, and history on mount
  useEffect(() => {
    api.series().then(setSeriesList).catch(() => {})
    api.strategies().then(setStrategiesList).catch(() => {})
    api.backtestHistory().then(setHistory).catch(() => {})
  }, [])

  // Cleanup polling on unmount
  useEffect(() => {
    return () => clearInterval(intervalRef.current)
  }, [])

  const toggleStrategy = (name: string) => {
    setSelectedStrategies((prev) => {
      const next = new Set(prev)
      if (next.has(name)) next.delete(name)
      else next.add(name)
      return next
    })
  }

  const pollStatus = async (tid: string) => {
    try {
      const s = await api.backtestStatus(tid)
      setProgress(s.progress)
      setTradesProcessed(s.trades_processed ?? null)
      setTradesEstimated(s.trades_estimated ?? null)
      setLivePnl(s.net_pnl_cents ?? null)
      if (s.error) {
        clearInterval(intervalRef.current)
        setRunning(false)
        setError(s.error)
      } else if (!s.running) {
        clearInterval(intervalRef.current)
        setRunning(false)
        try {
          const r = await api.backtestResult(tid)
          setResult(r as BacktestResult)
        } catch (e) {
          setError(e instanceof Error ? e.message : String(e))
        }
        // Refresh history + pipeline status
        api.backtestHistory().then(setHistory).catch(() => {})
        refreshPipeline()
      }
    } catch (e) {
      clearInterval(intervalRef.current)
      setRunning(false)
      setError(e instanceof Error ? e.message : String(e))
    }
  }

  const handleRun = async () => {
    if (!series || selectedStrategies.size === 0) return
    setError(null)
    setResult(null)
    setProgress('')
    setTradesProcessed(null)
    setTradesEstimated(null)
    setLivePnl(null)
    setRunning(true)

    const params: Record<string, unknown> = {
      series,
      strategies: Array.from(selectedStrategies),
    }
    if (startDate) params.start = startDate
    if (endDate) params.end = endDate
    if (cash !== 10000) params.cash = cash
    if (slippage !== 1) params.slippage = slippage
    if (tp) params.tp = Number(tp)
    if (sl) params.sl = Number(sl)
    if (minPrice) params.min_price = Number(minPrice)
    if (maxPrice) params.max_price = Number(maxPrice)

    try {
      const { task_id } = await api.startBacktest(params as Parameters<typeof api.startBacktest>[0])
      setTaskId(task_id)
      intervalRef.current = setInterval(() => pollStatus(task_id), 2000)
    } catch (e) {
      setRunning(false)
      setError(e instanceof Error ? e.message : String(e))
    }
  }

  const loadHistoryResult = async (item: BacktestHistoryItem) => {
    setError(null)
    setResult(null)
    setTaskId(item.task_id)
    try {
      const r = await api.backtestResult(item.task_id)
      setResult(r as BacktestResult)
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    }
  }

  const perStrategyEntries = result?.per_strategy
    ? Object.entries(result.per_strategy)
    : []

  const tradeLog = result?.trade_log?.slice(-50) ?? []

  return (
    <div className="space-y-6">
      <h1 className="text-2xl font-semibold">Backtest</h1>

      {/* Prerequisite banner */}
      {!pipelineLoading && !prerequisitesMet && (
        <div className="rounded border border-yellow-700 bg-yellow-950 px-4 py-3 text-sm text-yellow-300">
          {!hasData && <p>No market data found. Download data first.</p>}
          {hasData && !hasStrategies && <p>No strategies found. Generate a strategy first.</p>}
        </div>
      )}

      {error && (
        <div className="rounded border border-red-700 bg-red-950 px-4 py-3 text-sm text-red-300">
          {error}
        </div>
      )}

      {/* Config form */}
      <div className="rounded-lg border border-gray-800 bg-gray-900 px-5 py-4 space-y-4">
        <h2 className="text-sm font-medium text-gray-300">Configuration</h2>

        <div className="grid grid-cols-1 sm:grid-cols-2 gap-4">
          {/* Series */}
          <div>
            <label className="block text-xs text-gray-400 mb-1.5" htmlFor="series-select">
              Series
            </label>
            <select
              id="series-select"
              value={series}
              onChange={(e) => setSeries(e.target.value)}
              className="w-full rounded-lg border border-gray-700 bg-gray-900 px-3 py-2 text-sm text-gray-100 focus:outline-none focus:ring-1 focus:ring-indigo-500"
            >
              <option value="">Select series...</option>
              {seriesList.map((s) => (
                <option key={s} value={s}>{s}</option>
              ))}
            </select>
          </div>

          {/* Date range */}
          <div className="flex gap-3">
            <div className="flex-1">
              <label className="block text-xs text-gray-400 mb-1.5" htmlFor="start-date">
                Start
              </label>
              <input
                id="start-date"
                type="date"
                value={startDate}
                onChange={(e) => setStartDate(e.target.value)}
                className="w-full rounded-lg border border-gray-700 bg-gray-900 px-3 py-2 text-sm text-gray-100 focus:outline-none focus:ring-1 focus:ring-indigo-500"
              />
            </div>
            <div className="flex-1">
              <label className="block text-xs text-gray-400 mb-1.5" htmlFor="end-date">
                End
              </label>
              <input
                id="end-date"
                type="date"
                value={endDate}
                onChange={(e) => setEndDate(e.target.value)}
                className="w-full rounded-lg border border-gray-700 bg-gray-900 px-3 py-2 text-sm text-gray-100 focus:outline-none focus:ring-1 focus:ring-indigo-500"
              />
            </div>
          </div>
        </div>

        {/* Strategies multi-select */}
        <div>
          <label className="block text-xs text-gray-400 mb-1.5">
            Strategies
          </label>
          <div className="flex flex-wrap gap-2">
            {strategiesList.map((s) => (
              <label
                key={s.name}
                className={`flex items-center gap-1.5 px-3 py-1.5 rounded-lg border text-sm cursor-pointer transition-colors ${
                  selectedStrategies.has(s.name)
                    ? 'border-indigo-500 bg-indigo-950 text-indigo-300'
                    : 'border-gray-700 bg-gray-800 text-gray-400 hover:border-gray-600'
                }`}
              >
                <input
                  type="checkbox"
                  checked={selectedStrategies.has(s.name)}
                  onChange={() => toggleStrategy(s.name)}
                  className="sr-only"
                />
                {s.name}
              </label>
            ))}
            {strategiesList.length === 0 && (
              <span className="text-xs text-gray-500">No strategies available.</span>
            )}
          </div>
        </div>

        {/* Advanced section */}
        <div>
          <button
            onClick={() => setShowAdvanced(!showAdvanced)}
            className="text-xs text-gray-500 hover:text-gray-300 transition-colors"
          >
            {showAdvanced ? 'Hide' : 'Show'} advanced options
          </button>
          {showAdvanced && (
            <div className="mt-3 grid grid-cols-2 sm:grid-cols-3 gap-3">
              <div>
                <label className="block text-xs text-gray-400 mb-1" htmlFor="cash">Cash ($)</label>
                <input
                  id="cash"
                  type="number"
                  value={cash}
                  onChange={(e) => setCash(Number(e.target.value))}
                  className="w-full rounded border border-gray-700 bg-gray-900 px-2 py-1.5 text-sm text-gray-100 focus:outline-none focus:ring-1 focus:ring-indigo-500"
                />
              </div>
              <div>
                <label className="block text-xs text-gray-400 mb-1" htmlFor="slippage">Slippage</label>
                <input
                  id="slippage"
                  type="number"
                  value={slippage}
                  onChange={(e) => setSlippage(Number(e.target.value))}
                  className="w-full rounded border border-gray-700 bg-gray-900 px-2 py-1.5 text-sm text-gray-100 focus:outline-none focus:ring-1 focus:ring-indigo-500"
                />
              </div>
              <div>
                <label className="block text-xs text-gray-400 mb-1" htmlFor="tp">Take Profit</label>
                <input
                  id="tp"
                  type="number"
                  value={tp}
                  onChange={(e) => setTp(e.target.value)}
                  placeholder="--"
                  className="w-full rounded border border-gray-700 bg-gray-900 px-2 py-1.5 text-sm text-gray-100 placeholder-gray-600 focus:outline-none focus:ring-1 focus:ring-indigo-500"
                />
              </div>
              <div>
                <label className="block text-xs text-gray-400 mb-1" htmlFor="sl">Stop Loss</label>
                <input
                  id="sl"
                  type="number"
                  value={sl}
                  onChange={(e) => setSl(e.target.value)}
                  placeholder="--"
                  className="w-full rounded border border-gray-700 bg-gray-900 px-2 py-1.5 text-sm text-gray-100 placeholder-gray-600 focus:outline-none focus:ring-1 focus:ring-indigo-500"
                />
              </div>
              <div>
                <label className="block text-xs text-gray-400 mb-1" htmlFor="min-price">Min Price</label>
                <input
                  id="min-price"
                  type="number"
                  value={minPrice}
                  onChange={(e) => setMinPrice(e.target.value)}
                  placeholder="--"
                  className="w-full rounded border border-gray-700 bg-gray-900 px-2 py-1.5 text-sm text-gray-100 placeholder-gray-600 focus:outline-none focus:ring-1 focus:ring-indigo-500"
                />
              </div>
              <div>
                <label className="block text-xs text-gray-400 mb-1" htmlFor="max-price">Max Price</label>
                <input
                  id="max-price"
                  type="number"
                  value={maxPrice}
                  onChange={(e) => setMaxPrice(e.target.value)}
                  placeholder="--"
                  className="w-full rounded border border-gray-700 bg-gray-900 px-2 py-1.5 text-sm text-gray-100 placeholder-gray-600 focus:outline-none focus:ring-1 focus:ring-indigo-500"
                />
              </div>
            </div>
          )}
        </div>

        {/* Run button */}
        <button
          onClick={handleRun}
          disabled={running || !series || selectedStrategies.size === 0 || !prerequisitesMet}
          className="px-4 py-2 rounded bg-indigo-600 hover:bg-indigo-500 disabled:opacity-50 disabled:cursor-not-allowed text-sm font-medium transition-colors"
        >
          {running ? 'Running...' : 'Run Backtest'}
        </button>
      </div>

      {/* Progress */}
      {running && (
        <div className="rounded-lg border border-gray-800 bg-gray-900 px-4 py-3 text-sm space-y-2">
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-3">
              <div className="h-2 w-2 rounded-full bg-indigo-500 animate-pulse" />
              <span className="text-gray-300">{progress || 'Starting backtest...'}</span>
            </div>
            {livePnl != null && (
              <span className={livePnl >= 0 ? 'text-emerald-400' : 'text-red-400'}>
                P&L: {livePnl >= 0 ? '+' : ''}{(livePnl / 100).toFixed(2)}$
              </span>
            )}
          </div>
          {tradesEstimated != null && tradesEstimated > 0 && (
            <div className="h-1.5 w-full rounded-full bg-gray-700 overflow-hidden">
              <div
                className="h-full rounded-full bg-indigo-500 transition-all duration-500"
                style={{ width: `${Math.min(100, ((tradesProcessed ?? 0) / tradesEstimated) * 100)}%` }}
              />
            </div>
          )}
        </div>
      )}

      {/* Results */}
      {result && (
        <div className="space-y-6">
          {/* Summary cards */}
          <div className="grid grid-cols-2 sm:grid-cols-3 lg:grid-cols-6 gap-3">
            {([
              ['Total Trades', result.total_trades ?? '--'],
              ['Wins / Losses', `${result.wins ?? '--'} / ${result.losses ?? '--'}`],
              ['Win Rate', fmtPct(result.win_rate)],
              ['Net P&L', fmtDollar(result.net_pnl_cents)],
              ['Sharpe', fmt(result.sharpe)],
              ['Max Drawdown', fmtDollar(result.max_drawdown_cents)],
            ] as [string, string | number][]).map(([label, value]) => (
              <div key={label} className="rounded-lg border border-gray-800 bg-gray-900 px-4 py-3">
                <dt className="text-xs text-gray-500">{label}</dt>
                <dd className="text-lg font-mono mt-1 text-gray-100">{value}</dd>
              </div>
            ))}
          </div>

          {/* Per-strategy table */}
          {perStrategyEntries.length > 0 && (
            <div>
              <h3 className="text-sm font-medium text-gray-300 mb-2">Per-Strategy Breakdown</h3>
              <div className="overflow-x-auto rounded-lg border border-gray-800">
                <table className="w-full text-sm">
                  <thead className="border-b border-gray-800 bg-gray-900">
                    <tr>
                      <th className="px-4 py-2 text-left text-xs text-gray-400 font-medium">Strategy</th>
                      <th className="px-4 py-2 text-left text-xs text-gray-400 font-medium">Trades</th>
                      <th className="px-4 py-2 text-left text-xs text-gray-400 font-medium">Win Rate</th>
                      <th className="px-4 py-2 text-left text-xs text-gray-400 font-medium">Net P&L</th>
                      <th className="px-4 py-2 text-left text-xs text-gray-400 font-medium">Sharpe</th>
                    </tr>
                  </thead>
                  <tbody>
                    {perStrategyEntries.map(([name, data]) => (
                      <tr key={name} className="border-b border-gray-800/50">
                        <td className="px-4 py-3 font-mono">{name}</td>
                        <td className="px-4 py-3 text-gray-400">{data.total_trades as number ?? '--'}</td>
                        <td className="px-4 py-3 text-gray-400">{fmtPct(data.win_rate)}</td>
                        <td className="px-4 py-3 text-gray-400">{fmtDollar(data.net_pnl_cents)}</td>
                        <td className="px-4 py-3 text-gray-400">{fmt(data.sharpe)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}

          {/* Equity curve */}
          {result.equity_curve && result.equity_curve.length > 0 && (
            <div>
              <h3 className="text-sm font-medium text-gray-300 mb-2">Equity Curve</h3>
              <div className="rounded-lg border border-gray-800 bg-gray-900 p-4">
                <EquityCurve data={result.equity_curve} />
              </div>
            </div>
          )}

          {/* Trade log */}
          {tradeLog.length > 0 && (
            <div>
              <h3 className="text-sm font-medium text-gray-300 mb-2">
                Trade Log <span className="text-gray-500 font-normal">(last 50)</span>
              </h3>
              <div className="overflow-x-auto rounded-lg border border-gray-800">
                <table className="w-full text-sm">
                  <thead className="border-b border-gray-800 bg-gray-900">
                    <tr>
                      {Object.keys(tradeLog[0]).map((col) => (
                        <th key={col} className="px-3 py-2 text-left text-xs text-gray-400 font-medium whitespace-nowrap">
                          {col}
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {tradeLog.map((row, i) => (
                      <tr key={i} className="border-b border-gray-800/50">
                        {Object.values(row).map((val, j) => (
                          <td key={j} className="px-3 py-2 text-gray-400 font-mono text-xs whitespace-nowrap">
                            {val == null ? '--' : String(val)}
                          </td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          )}
        </div>
      )}

      {/* History */}
      <div>
        <button
          onClick={() => setShowHistory(!showHistory)}
          className="text-sm text-gray-400 hover:text-gray-200 transition-colors"
        >
          {showHistory ? 'Hide' : 'Show'} backtest history ({history.length})
        </button>
        {showHistory && history.length > 0 && (
          <div className="mt-3 overflow-x-auto rounded-lg border border-gray-800">
            <table className="w-full text-sm">
              <thead className="border-b border-gray-800 bg-gray-900">
                <tr>
                  <th className="px-4 py-2 text-left text-xs text-gray-400 font-medium">Timestamp</th>
                  <th className="px-4 py-2 text-left text-xs text-gray-400 font-medium">Series</th>
                  <th className="px-4 py-2 text-left text-xs text-gray-400 font-medium">Strategies</th>
                  <th className="px-4 py-2 text-left text-xs text-gray-400 font-medium">Trades</th>
                  <th className="px-4 py-2 text-left text-xs text-gray-400 font-medium">Net P&L</th>
                  <th className="px-4 py-2 text-left text-xs text-gray-400 font-medium">Win Rate</th>
                  <th className="px-4 py-2 text-left text-xs text-gray-400 font-medium">Sharpe</th>
                </tr>
              </thead>
              <tbody>
                {history.map((h) => (
                  <tr
                    key={h.task_id}
                    className={`border-b border-gray-800/50 cursor-pointer transition-colors ${
                      taskId === h.task_id ? 'bg-gray-800' : 'hover:bg-gray-900'
                    }`}
                    onClick={() => loadHistoryResult(h)}
                  >
                    <td className="px-4 py-3 text-xs text-gray-400">
                      {new Date(h.timestamp).toLocaleString()}
                    </td>
                    <td className="px-4 py-3 font-mono">{h.series}</td>
                    <td className="px-4 py-3 text-gray-400 text-xs">
                      {h.strategies.join(', ')}
                    </td>
                    <td className="px-4 py-3 text-gray-400">{h.total_trades}</td>
                    <td className="px-4 py-3 text-gray-400">{fmtDollar(h.net_pnl_cents)}</td>
                    <td className="px-4 py-3 text-gray-400">{fmtPct(h.win_rate)}</td>
                    <td className="px-4 py-3 text-gray-400">{fmt(h.sharpe)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
        {showHistory && history.length === 0 && (
          <p className="mt-2 text-xs text-gray-500">No backtest history yet.</p>
        )}
      </div>
    </div>
  )
}
