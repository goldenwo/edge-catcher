import { Fragment, useCallback, useEffect, useRef, useState } from 'react'
import { research, ResearchLoopStatus, ResearchLoopStartRequest, VerdictCounts, ResearchResult, AuditExecution, AuditDecision } from '../api'

// ── types ───────────────────────────────────────────────────────────────────

interface SeriesProfile {
  series_ticker: string
  db_path: string
  description: string
  settlement_frequency: string
  market_count: number
  volume_stats: { median: number; mean: number; p90: number }
  asset_class: string
  external_asset: string | null
}

type Tab = 'overview' | 'review' | 'activity' | 'series'

// ── helpers ─────────────────────────────────────────────────────────────────

const verdictBadge = (v: string) => {
  const styles: Record<string, string> = {
    promote: 'bg-emerald-900/50 text-emerald-400',
    review: 'bg-amber-900/50 text-amber-400',
    explore: 'bg-indigo-900/50 text-indigo-400',
    kill: 'bg-red-900/50 text-red-400',
    accepted: 'bg-cyan-900/50 text-cyan-400',
  }
  return styles[v] || 'bg-gray-800 text-gray-400'
}

const gateBadge = (passed: boolean, borderline?: boolean) => {
  if (!passed) return { icon: '✗', cls: 'text-red-400' }
  if (borderline) return { icon: '⚠', cls: 'text-amber-400' }
  return { icon: '✓', cls: 'text-emerald-400' }
}

// ── main component ──────────────────────────────────────────────────────────

export default function Research() {
  const [tab, setTab] = useState<Tab>('overview')
  const [loopStatus, setLoopStatus] = useState<ResearchLoopStatus>({
    running: false, phase: 'idle', runs_completed: 0, runs_total: 0,
    elapsed_seconds: 0, task_id: null, error: null,
  })
  const [showConfig, setShowConfig] = useState(false)
  const [reviewCount, setReviewCount] = useState(0)

  // ── polling ─────────────────────────────────────────────────────────────

  const pollLoop = useCallback(async () => {
    try {
      const s = await research.loopStatus()
      setLoopStatus(s)
    } catch { /* ignore */ }
  }, [])

  useEffect(() => {
    pollLoop()
    const id = setInterval(pollLoop, 5000)
    return () => clearInterval(id)
  }, [pollLoop])

  // ── loop control ────────────────────────────────────────────────────────

  const handleStart = async (config: ResearchLoopStartRequest) => {
    try {
      await research.startLoop(config)
      setShowConfig(false)
      pollLoop()
    } catch (e) {
      alert(e instanceof Error ? e.message : String(e))
    }
  }

  const handleStop = async () => {
    try {
      await research.stopLoop()
      pollLoop()
    } catch (e) {
      alert(e instanceof Error ? e.message : String(e))
    }
  }

  // ── format helpers ──────────────────────────────────────────────────────

  const elapsed = (s: number) => {
    const m = Math.floor(s / 60)
    const sec = Math.floor(s % 60)
    return m > 0 ? `${m}m ${sec}s` : `${sec}s`
  }

  const phaseColor = (phase: string) => {
    switch (phase) {
      case 'ideate': return 'bg-indigo-900/50 text-indigo-400'
      case 'expand': return 'bg-emerald-900/50 text-emerald-400'
      case 'refine': return 'bg-amber-900/50 text-amber-400'
      case 'completed': return 'bg-emerald-900/50 text-emerald-400'
      case 'error': return 'bg-red-900/50 text-red-400'
      default: return 'bg-gray-800 text-gray-400'
    }
  }

  return (
    <div className="max-w-6xl">
      {/* Status Bar */}
      <div className="flex items-center gap-4 px-4 py-3 mb-4 bg-gray-900 border border-gray-800 rounded-lg">
        <span className={`px-2 py-0.5 rounded text-xs font-medium ${
          loopStatus.running ? 'bg-emerald-900/50 text-emerald-400' : 'bg-gray-800 text-gray-400'
        }`}>
          {loopStatus.running ? '● Running' : '● Idle'}
        </span>
        {loopStatus.running ? (
          <>
            <span className={`px-2 py-0.5 rounded text-xs font-medium ${phaseColor(loopStatus.phase)}`}>
              {loopStatus.phase}
            </span>
            <span className="text-sm text-gray-400">
              {loopStatus.runs_completed}/{loopStatus.runs_total} runs · {elapsed(loopStatus.elapsed_seconds)}
            </span>
            <button
              onClick={handleStop}
              className="ml-auto px-3 py-1 text-xs bg-red-900/50 text-red-400 border border-red-800 rounded hover:bg-red-900"
            >
              Stop
            </button>
          </>
        ) : (
          <>
            {loopStatus.error && (
              <span className="text-xs text-red-400">{loopStatus.error}</span>
            )}
            <span className="text-sm text-gray-500">
              {loopStatus.phase === 'completed' ? 'Loop completed' : 'No loop running'}
            </span>
            <button
              onClick={() => setShowConfig(!showConfig)}
              className="ml-auto px-3 py-1 text-xs bg-indigo-600 text-white rounded hover:bg-indigo-500"
            >
              Start Loop {showConfig ? '▴' : '▾'}
            </button>
          </>
        )}
      </div>

      {/* Config Panel */}
      {showConfig && <LoopConfigPanel onStart={handleStart} onCancel={() => setShowConfig(false)} />}

      {/* Tabs */}
      <div className="flex gap-0 border-b border-gray-800 mb-6">
        {([
          ['overview', 'Overview'],
          ['review', `Review Queue${reviewCount > 0 ? ` (${reviewCount})` : ''}`],
          ['activity', 'Activity'],
          ['series', 'Series'],
        ] as const).map(([key, label]) => (
          <button
            key={key}
            onClick={() => setTab(key as Tab)}
            className={`px-4 py-2 text-sm border-b-2 transition-colors ${
              tab === key
                ? 'border-indigo-500 text-white'
                : 'border-transparent text-gray-500 hover:text-gray-300'
            }`}
          >
            {label}
          </button>
        ))}
      </div>

      {/* Tab Content */}
      {tab === 'overview' && <OverviewTab running={loopStatus.running} />}
      {tab === 'review' && <ReviewQueueTab running={loopStatus.running} onCountChange={setReviewCount} />}
      {tab === 'activity' && <ActivityTab />}
      {tab === 'series' && <SeriesTab />}
    </div>
  )
}

// ── Config Panel ────────────────────────────────────────────────────────────

function LoopConfigPanel({ onStart, onCancel }: {
  onStart: (c: ResearchLoopStartRequest) => void
  onCancel: () => void
}) {
  const [mode, setMode] = useState('full')
  const [maxRuns, setMaxRuns] = useState(100)
  const [maxTime, setMaxTime] = useState(60)
  const [parallel, setParallel] = useState(4)
  const [showAdvanced, setShowAdvanced] = useState(false)
  const [feePct, setFeePct] = useState<number | undefined>()
  const [startDate, setStartDate] = useState('')
  const [endDate, setEndDate] = useState('')
  const [maxLlm, setMaxLlm] = useState<number | undefined>()
  const [force, setForce] = useState(false)

  const modes = ['full', 'grid_only', 'llm_only', 'refine_only']
  const modeLabels: Record<string, string> = {
    full: 'Full', grid_only: 'Grid only', llm_only: 'LLM only', refine_only: 'Refine only',
  }

  return (
    <div className="mb-4 p-4 bg-gray-900 border border-gray-800 border-b-2 border-b-indigo-500 rounded-lg">
      <div className="flex flex-wrap gap-6">
        <div>
          <div className="text-xs text-gray-500 uppercase mb-1">Mode</div>
          <div className="flex gap-1">
            {modes.map(m => (
              <button
                key={m}
                onClick={() => setMode(m)}
                className={`px-3 py-1 text-xs rounded ${
                  mode === m ? 'bg-indigo-600 text-white' : 'bg-gray-800 text-gray-400 hover:text-white'
                }`}
              >
                {modeLabels[m]}
              </button>
            ))}
          </div>
        </div>
        <div>
          <div className="text-xs text-gray-500 uppercase mb-1">Max Runs</div>
          <input type="number" value={maxRuns} onChange={e => setMaxRuns(+e.target.value)}
            className="w-20 bg-gray-950 border border-gray-700 rounded px-2 py-1 text-sm text-white font-mono" />
        </div>
        <div>
          <div className="text-xs text-gray-500 uppercase mb-1">Max Time (min)</div>
          <input type="number" value={maxTime} onChange={e => setMaxTime(+e.target.value)}
            className="w-20 bg-gray-950 border border-gray-700 rounded px-2 py-1 text-sm text-white font-mono" />
        </div>
        <div>
          <div className="text-xs text-gray-500 uppercase mb-1">Parallel</div>
          <input type="number" value={parallel} onChange={e => setParallel(+e.target.value)}
            className="w-16 bg-gray-950 border border-gray-700 rounded px-2 py-1 text-sm text-white font-mono" />
        </div>
      </div>
      <div className="mt-3 pt-3 border-t border-gray-800">
        <button onClick={() => setShowAdvanced(!showAdvanced)} className="text-xs text-gray-500 hover:text-gray-300">
          {showAdvanced ? '▾' : '▸'} Advanced
        </button>
        {showAdvanced && (
          <div className="flex flex-wrap gap-6 mt-3">
            <div>
              <div className="text-xs text-gray-500 uppercase mb-1">Fee %</div>
              <input type="number" step="0.1" value={feePct ?? ''} onChange={e => setFeePct(e.target.value ? +e.target.value : undefined)}
                placeholder="1.0" className="w-20 bg-gray-950 border border-gray-700 rounded px-2 py-1 text-sm text-white font-mono" />
            </div>
            <div>
              <div className="text-xs text-gray-500 uppercase mb-1">Start Date</div>
              <input type="date" value={startDate} onChange={e => setStartDate(e.target.value)}
                className="bg-gray-950 border border-gray-700 rounded px-2 py-1 text-sm text-white" />
            </div>
            <div>
              <div className="text-xs text-gray-500 uppercase mb-1">End Date</div>
              <input type="date" value={endDate} onChange={e => setEndDate(e.target.value)}
                className="bg-gray-950 border border-gray-700 rounded px-2 py-1 text-sm text-white" />
            </div>
            <div>
              <div className="text-xs text-gray-500 uppercase mb-1">Max LLM Calls</div>
              <input type="number" value={maxLlm ?? ''} onChange={e => setMaxLlm(e.target.value ? +e.target.value : undefined)}
                placeholder="10" className="w-20 bg-gray-950 border border-gray-700 rounded px-2 py-1 text-sm text-white font-mono" />
            </div>
            <div className="flex items-end">
              <label className="flex items-center gap-2 cursor-pointer py-1">
                <input type="checkbox" checked={force} onChange={e => setForce(e.target.checked)}
                  className="accent-indigo-500" />
                <span className="text-xs text-gray-400">Force re-run</span>
              </label>
            </div>
          </div>
        )}
      </div>
      <div className="flex gap-3 mt-4">
        <button
          onClick={() => onStart({
            mode, max_runs: maxRuns, max_time: maxTime, parallel,
            ...(feePct != null && { fee_pct: feePct }),
            ...(startDate && { start: startDate }),
            ...(endDate && { end: endDate }),
            ...(maxLlm != null && { max_llm_calls: maxLlm }),
            ...(force && { force: true }),
          })}
          className="px-4 py-1.5 text-sm bg-indigo-600 text-white rounded hover:bg-indigo-500"
        >
          Start
        </button>
        <button onClick={onCancel} className="px-4 py-1.5 text-sm text-gray-500 hover:text-gray-300">
          Cancel
        </button>
      </div>
    </div>
  )
}

// ── Overview Tab ────────────────────────────────────────────────────────────

function OverviewTab({ running }: { running: boolean }) {
  const [counts, setCounts] = useState<VerdictCounts>({ promote: 0, review: 0, explore: 0, kill: 0 })
  const [results, setResults] = useState<ResearchResult[]>([])
  const [feedOpen, setFeedOpen] = useState(false)
  const [feed, setFeed] = useState<AuditExecution[]>([])
  const [sortCol, setSortCol] = useState<string>('completed_at')
  const feedRef = useRef<HTMLDivElement>(null)
  const [autoScroll, setAutoScroll] = useState(true)

  const load = useCallback(async () => {
    try {
      const [c, r] = await Promise.all([
        research.verdictCounts(),
        research.results(50, 0, sortCol),
      ])
      setCounts(c)
      setResults(r.results)
    } catch { /* ignore */ }
  }, [sortCol])

  // Poll when running
  useEffect(() => {
    load()
    if (!running) return
    const id = setInterval(load, 5000)
    return () => clearInterval(id)
  }, [load, running])

  // Live feed polling
  useEffect(() => {
    if (!feedOpen || !running) return
    const loadFeed = async () => {
      try {
        setFeed(await research.auditExecutions(50))
      } catch { /* ignore */ }
    }
    loadFeed()
    const id = setInterval(loadFeed, 3000)
    return () => clearInterval(id)
  }, [feedOpen, running])

  // Auto-scroll feed
  useEffect(() => {
    if (autoScroll && feedRef.current) {
      feedRef.current.scrollTop = feedRef.current.scrollHeight
    }
  }, [feed, autoScroll])

  const verdictCards = [
    { label: 'Promote', count: counts.promote, color: 'text-emerald-400' },
    { label: 'Review', count: counts.review, color: 'text-amber-400' },
    { label: 'Explore', count: counts.explore, color: 'text-indigo-400' },
    { label: 'Kill', count: counts.kill, color: 'text-red-400' },
  ]

  const columns = [
    { key: 'strategy', label: 'Strategy', sortable: false },
    { key: 'series', label: 'Series', sortable: false },
    { key: 'verdict', label: 'Verdict', sortable: false },
    { key: 'sharpe', label: 'Sharpe', right: true, sortable: true },
    { key: 'win_rate', label: 'Win Rate', right: true, sortable: true },
    { key: 'net_pnl_cents', label: 'PnL', right: true, sortable: true },
    { key: 'total_trades', label: 'Trades', right: true, sortable: true },
  ]

  return (
    <div>
      {/* Verdict cards */}
      <div className="grid grid-cols-4 gap-3 mb-6">
        {verdictCards.map(c => (
          <div key={c.label} className="bg-gray-900 border border-gray-800 rounded-lg px-4 py-3">
            <div className="text-xs text-gray-500">{c.label}</div>
            <div className={`text-2xl font-mono ${c.color}`}>{c.count}</div>
          </div>
        ))}
      </div>

      {/* Live feed toggle */}
      <button
        onClick={() => setFeedOpen(!feedOpen)}
        className="text-xs text-gray-500 hover:text-gray-300 mb-3 block"
      >
        {feedOpen ? '▾' : '▸'} Live feed
      </button>
      {feedOpen && (
        <div className="mb-6 bg-gray-900 border border-gray-800 rounded-lg">
          <div className="flex items-center justify-between px-3 py-2 border-b border-gray-800">
            <span className="text-xs text-gray-500">Execution log</span>
            <button
              onClick={() => setAutoScroll(!autoScroll)}
              className={`text-xs ${autoScroll ? 'text-indigo-400' : 'text-gray-600'}`}
            >
              auto-scroll {autoScroll ? 'on' : 'off'}
            </button>
          </div>
          <div ref={feedRef} className="max-h-48 overflow-y-auto p-2 space-y-0.5">
            {feed.length === 0 ? (
              <div className="text-xs text-gray-600 px-2">No executions yet.</div>
            ) : feed.map(e => (
              <div key={e.id} className="flex gap-3 text-xs px-2 py-0.5">
                <span className="text-gray-600 font-mono w-36 shrink-0">
                  {new Date(e.completed_at).toLocaleTimeString()}
                </span>
                <span className="text-gray-300 font-mono">{e.hypothesis_id?.slice(0, 12)}</span>
                <span className="text-gray-400">{e.phase}</span>
                <span className={`px-1.5 rounded ${verdictBadge(e.verdict)}`}>{e.verdict}</span>
              </div>
            ))}
          </div>
        </div>
      )}

      {/* Recent Results */}
      <div className="text-xs text-gray-500 uppercase tracking-wider mb-2">Recent Results</div>
      <div className="bg-gray-900 border border-gray-800 rounded-lg overflow-hidden">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-gray-700">
              {columns.map(col => (
                <th
                  key={col.key}
                  onClick={col.sortable ? () => setSortCol(col.key) : undefined}
                  className={`py-2 px-3 text-xs text-gray-400 font-medium ${
                    col.sortable ? 'cursor-pointer hover:text-white' : ''
                  } ${col.right ? 'text-right' : 'text-left'
                  } ${sortCol === col.key ? 'text-indigo-400' : ''}`}
                >
                  {col.label}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {results.map(r => (
              <tr key={r.id} className="border-b border-gray-800 hover:bg-gray-800/50">
                <td className="py-2 px-3 font-mono text-white">{r.strategy}</td>
                <td className="py-2 px-3 text-gray-400">{r.series}</td>
                <td className="py-2 px-3">
                  <span className={`px-1.5 py-0.5 rounded text-xs ${verdictBadge(r.verdict)}`}>
                    {r.verdict}
                  </span>
                </td>
                <td className="py-2 px-3 text-right font-mono text-gray-300">{r.sharpe?.toFixed(2) ?? '—'}</td>
                <td className="py-2 px-3 text-right text-gray-300">
                  {r.win_rate != null ? `${(r.win_rate * 100).toFixed(1)}%` : '—'}
                </td>
                <td className={`py-2 px-3 text-right font-mono ${
                  (r.net_pnl_cents ?? 0) >= 0 ? 'text-emerald-400' : 'text-red-400'
                }`}>
                  {r.net_pnl_cents != null ? `${r.net_pnl_cents >= 0 ? '+' : ''}${r.net_pnl_cents}¢` : '—'}
                </td>
                <td className="py-2 px-3 text-right text-gray-300">{r.total_trades ?? '—'}</td>
              </tr>
            ))}
            {results.length === 0 && (
              <tr><td colSpan={7} className="py-4 text-center text-gray-600 text-sm">No results yet.</td></tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  )
}

// ── Review Queue Tab ────────────────────────────────────────────────────────

function ReviewQueueTab({ running, onCountChange }: { running: boolean; onCountChange: (n: number) => void }) {
  const [queue, setQueue] = useState<ResearchResult[]>([])
  const [expanded, setExpanded] = useState<string | null>(null)
  const [loading, setLoading] = useState(true)
  const [confirmReject, setConfirmReject] = useState<string | null>(null)

  const load = useCallback(async () => {
    try {
      const data = await research.reviewQueue()
      setQueue(data.strategies)
      onCountChange(data.count)
    } catch { /* ignore */ }
    finally { setLoading(false) }
  }, [onCountChange])

  useEffect(() => {
    load()
    if (!running) return
    const id = setInterval(load, 5000)
    return () => clearInterval(id)
  }, [load, running])

  const handleApprove = async (id: string) => {
    try {
      await research.approve(id)
      setQueue(q => {
        const next = q.filter(r => r.id !== id)
        onCountChange(next.length)
        return next
      })
    } catch (e) {
      alert(e instanceof Error ? e.message : String(e))
    }
  }

  const handleReject = async (id: string) => {
    try {
      await research.reject(id, 'Manually rejected from dashboard')
      setConfirmReject(null)
      setQueue(q => {
        const next = q.filter(r => r.id !== id)
        onCountChange(next.length)
        return next
      })
    } catch (e) {
      alert(e instanceof Error ? e.message : String(e))
    }
  }

  if (loading) return <div className="text-gray-500 text-sm">Loading...</div>
  if (queue.length === 0) return <div className="text-gray-500 text-sm">No strategies pending review.</div>

  return (
    <div className="bg-gray-900 border border-gray-800 rounded-lg overflow-hidden">
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b border-gray-700">
            <th className="py-2 px-3 text-left text-xs text-gray-400">Strategy</th>
            <th className="py-2 px-3 text-left text-xs text-gray-400">Series</th>
            <th className="py-2 px-3 text-left text-xs text-gray-400">Verdict</th>
            <th className="py-2 px-3 text-right text-xs text-gray-400">Sharpe</th>
            <th className="py-2 px-3 text-right text-xs text-gray-400">Win Rate</th>
            <th className="py-2 px-3 text-right text-xs text-gray-400">PnL</th>
            <th className="py-2 px-3 text-right text-xs text-gray-400">Actions</th>
          </tr>
        </thead>
        <tbody>
          {queue.map(r => (
            <Fragment key={r.id}>
              <tr
                className={`border-b cursor-pointer hover:bg-gray-800/50 ${
                  expanded === r.id ? 'border-indigo-500 bg-gray-800/30' : 'border-gray-800'
                }`}
                onClick={() => setExpanded(expanded === r.id ? null : r.id)}
              >
                <td className="py-2 px-3 font-mono text-white">
                  {expanded === r.id ? '▾ ' : '▸ '}{r.strategy}
                </td>
                <td className="py-2 px-3 text-gray-400">{r.series}</td>
                <td className="py-2 px-3">
                  <span className={`px-1.5 py-0.5 rounded text-xs ${verdictBadge(r.verdict)}`}>{r.verdict}</span>
                </td>
                <td className="py-2 px-3 text-right font-mono text-gray-300">{r.sharpe?.toFixed(2)}</td>
                <td className="py-2 px-3 text-right text-gray-300">
                  {r.win_rate != null ? `${(r.win_rate * 100).toFixed(1)}%` : '—'}
                </td>
                <td className="py-2 px-3 text-right font-mono text-gray-300">
                  {r.net_pnl_cents != null ? `${r.net_pnl_cents >= 0 ? '+' : ''}${r.net_pnl_cents}¢` : '—'}
                </td>
                <td className="py-2 px-3 text-right relative" onClick={e => e.stopPropagation()}>
                  <div className="flex gap-2 justify-end">
                    <button onClick={() => handleApprove(r.id)} className="text-emerald-400 hover:text-emerald-300" title="Approve">✓</button>
                    <button onClick={() => setConfirmReject(r.id)} className="text-red-400 hover:text-red-300" title="Reject">✗</button>
                  </div>
                  {confirmReject === r.id && (
                    <div className="absolute right-0 mt-1 p-3 bg-gray-800 border border-gray-700 rounded-lg shadow-xl z-10 text-left">
                      <p className="text-xs text-gray-300 mb-2">Kill this strategy?</p>
                      <div className="flex gap-2">
                        <button onClick={() => handleReject(r.id)} className="text-xs px-2 py-1 bg-red-900/50 text-red-400 rounded">Confirm</button>
                        <button onClick={() => setConfirmReject(null)} className="text-xs px-2 py-1 text-gray-500">Cancel</button>
                      </div>
                    </div>
                  )}
                </td>
              </tr>
              {expanded === r.id && (
                <tr key={`${r.id}-detail`} className="border-b border-indigo-500">
                  <td colSpan={7} className="p-4 bg-gray-950">
                    <GateDetails result={r} />
                  </td>
                </tr>
              )}
            </Fragment>
          ))}
        </tbody>
      </table>
    </div>
  )
}

// ── Gate Details (used by ReviewQueueTab) ───────────────────────────────────

function GateDetails({ result: r }: { result: ResearchResult }) {
  const details = r.validation_details as { verdict?: string; reason?: string; gate_results?: Array<{
    gate: string; passed: boolean; metrics: Record<string, unknown>; reason?: string
  }> } | null

  const stats = [
    { label: 'Trades', value: r.total_trades },
    { label: 'Net PnL', value: `${r.net_pnl_cents >= 0 ? '+' : ''}${r.net_pnl_cents}¢`, color: r.net_pnl_cents >= 0 ? 'text-emerald-400' : 'text-red-400' },
    { label: 'Max DD', value: `${r.max_drawdown_pct?.toFixed(1)}%`, color: 'text-amber-400' },
    { label: 'Fees Paid', value: `${r.fees_paid_cents}¢` },
  ]

  const gateNames: Record<string, string> = {
    deflated_sharpe: 'Deflated Sharpe Ratio',
    monte_carlo: 'Monte Carlo Permutation',
    temporal_consistency: 'Temporal Consistency',
    param_sensitivity: 'Parameter Sensitivity',
  }

  return (
    <div>
      {/* Summary stats */}
      <div className="grid grid-cols-4 gap-3 mb-4">
        {stats.map(s => (
          <div key={s.label} className="bg-gray-900 border border-gray-800 rounded px-3 py-2">
            <div className="text-xs text-gray-600">{s.label}</div>
            <div className={`text-lg font-mono ${s.color || 'text-white'}`}>{s.value}</div>
          </div>
        ))}
      </div>

      {/* Validation gates */}
      {details?.gate_results && details.gate_results.length > 0 && (
        <>
          <div className="text-xs text-gray-500 uppercase tracking-wider mb-2">Validation Gates</div>
          <div className="space-y-2 mb-4">
            {details.gate_results.map((g, i) => {
              const badge = gateBadge(g.passed, g.reason?.toLowerCase().includes('borderline'))
              return (
                <div key={i} className="flex items-center gap-3 bg-gray-900 border border-gray-800 rounded px-3 py-2">
                  <span className={`text-base ${badge.cls}`}>{badge.icon}</span>
                  <div className="flex-1 min-w-0">
                    <div className="text-sm text-white">{gateNames[g.gate] || g.gate}</div>
                    <div className="text-xs text-gray-500 truncate">
                      {Object.entries(g.metrics || {}).map(([k, v]) =>
                        `${k} = ${typeof v === 'number' ? (v as number).toFixed(3) : v}`
                      ).join(' · ')}
                    </div>
                  </div>
                  {g.reason && <span className="text-xs text-gray-500 shrink-0">{g.reason}</span>}
                </div>
              )
            })}
          </div>
        </>
      )}

      {/* Verdict reason */}
      {(details?.reason || r.verdict_reason) && (
        <div className="bg-gray-900 border border-gray-800 rounded px-3 py-2">
          <div className="text-xs text-amber-400 italic">{details?.reason || r.verdict_reason}</div>
        </div>
      )}
    </div>
  )
}

// ── Activity Tab ────────────────────────────────────────────────────────────

function ActivityTab() {
  const [executions, setExecutions] = useState<AuditExecution[]>([])
  const [decisions, setDecisions] = useState<AuditDecision[]>([])
  const [expandedDecision, setExpandedDecision] = useState<number | null>(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    Promise.all([
      research.auditExecutions().then(setExecutions),
      research.auditDecisions().then(setDecisions),
    ]).finally(() => setLoading(false))
  }, [])

  if (loading) return <div className="text-gray-500 text-sm">Loading...</div>

  return (
    <div className="space-y-8">
      {/* Executions */}
      <section>
        <h3 className="text-xs text-gray-500 uppercase tracking-wider mb-2">
          Executions ({executions.length})
        </h3>
        <div className="bg-gray-900 border border-gray-800 rounded-lg overflow-hidden">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-gray-700">
                <th className="py-2 px-3 text-left text-xs text-gray-400">Time</th>
                <th className="py-2 px-3 text-left text-xs text-gray-400">Hypothesis</th>
                <th className="py-2 px-3 text-left text-xs text-gray-400">Phase</th>
                <th className="py-2 px-3 text-left text-xs text-gray-400">Verdict</th>
                <th className="py-2 px-3 text-left text-xs text-gray-400">Status</th>
              </tr>
            </thead>
            <tbody>
              {executions.map(e => (
                <tr key={e.id} className="border-b border-gray-800">
                  <td className="py-1.5 px-3 text-xs text-gray-500 font-mono">
                    {new Date(e.completed_at).toLocaleString()}
                  </td>
                  <td className="py-1.5 px-3 text-xs text-gray-300 font-mono">
                    {e.hypothesis_id?.slice(0, 12)}
                  </td>
                  <td className="py-1.5 px-3 text-xs text-gray-400">{e.phase}</td>
                  <td className="py-1.5 px-3">
                    <span className={`px-1.5 py-0.5 rounded text-xs ${verdictBadge(e.verdict)}`}>
                      {e.verdict}
                    </span>
                  </td>
                  <td className="py-1.5 px-3 text-xs text-gray-400">{e.status}</td>
                </tr>
              ))}
              {executions.length === 0 && (
                <tr><td colSpan={5} className="py-4 text-center text-gray-600 text-sm">No executions.</td></tr>
              )}
            </tbody>
          </table>
        </div>
      </section>

      {/* LLM Decisions */}
      <section>
        <h3 className="text-xs text-gray-500 uppercase tracking-wider mb-2">
          LLM Decisions ({decisions.length})
        </h3>
        <div className="bg-gray-900 border border-gray-800 rounded-lg overflow-hidden">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-gray-700">
                <th className="py-2 px-3 text-left text-xs text-gray-400">Time</th>
                <th className="py-2 px-3 text-left text-xs text-gray-400">Model</th>
                <th className="py-2 px-3 text-right text-xs text-gray-400">Tokens</th>
              </tr>
            </thead>
            <tbody>
              {decisions.map(d => (
                <Fragment key={d.id}>
                  <tr
                    className={`border-b cursor-pointer hover:bg-gray-800/50 ${
                      expandedDecision === d.id ? 'border-indigo-500' : 'border-gray-800'
                    }`}
                    onClick={() => setExpandedDecision(expandedDecision === d.id ? null : d.id)}
                  >
                    <td className="py-1.5 px-3 text-xs text-gray-500 font-mono">
                      {new Date(d.created_at).toLocaleString()}
                    </td>
                    <td className="py-1.5 px-3 text-xs text-gray-300">{d.model}</td>
                    <td className="py-1.5 px-3 text-right text-xs text-gray-400 font-mono">
                      {d.token_count?.toLocaleString()}
                    </td>
                  </tr>
                  {expandedDecision === d.id && (
                    <tr key={`${d.id}-detail`} className="border-b border-indigo-500">
                      <td colSpan={3} className="p-4 bg-gray-950">
                        <div className="mb-3">
                          <div className="text-xs text-gray-500 uppercase mb-1">Prompt</div>
                          <pre className="text-xs text-gray-300 whitespace-pre-wrap max-h-40 overflow-y-auto bg-gray-900 rounded p-2">
                            {d.prompt_text}
                          </pre>
                        </div>
                        <div>
                          <div className="text-xs text-gray-500 uppercase mb-1">Response</div>
                          <pre className="text-xs text-gray-300 whitespace-pre-wrap max-h-40 overflow-y-auto bg-gray-900 rounded p-2">
                            {d.response_text}
                          </pre>
                        </div>
                      </td>
                    </tr>
                  )}
                </Fragment>
              ))}
              {decisions.length === 0 && (
                <tr><td colSpan={3} className="py-4 text-center text-gray-600 text-sm">No LLM decisions.</td></tr>
              )}
            </tbody>
          </table>
        </div>
      </section>
    </div>
  )
}

// ── Series Tab ──────────────────────────────────────────────────────────────

function SeriesTab() {
  const [profiles, setProfiles] = useState<SeriesProfile[]>([])
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    research.profiles()
      .then(d => setProfiles((d.profiles || []) as SeriesProfile[]))
      .finally(() => setLoading(false))
  }, [])

  const grouped = profiles.reduce<Record<string, SeriesProfile[]>>((acc, p) => {
    const key = p.asset_class || 'Unknown'
    ;(acc[key] = acc[key] || []).push(p)
    return acc
  }, {})

  if (loading) return <div className="text-gray-500 text-sm">Loading...</div>
  if (profiles.length === 0) return <div className="text-gray-500 text-sm">No series profiles found.</div>

  return (
    <div>
      {Object.entries(grouped).sort().map(([assetClass, group]) => (
        <div key={assetClass} className="mb-6">
          <h3 className="text-sm font-medium text-gray-400 uppercase tracking-wider mb-2">
            {assetClass}
          </h3>
          <div className="bg-gray-900 border border-gray-800 rounded-lg overflow-hidden">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-gray-700">
                  <th className="py-2 px-3 text-left text-xs text-gray-400">Series</th>
                  <th className="py-2 px-3 text-left text-xs text-gray-400">Description</th>
                  <th className="py-2 px-3 text-left text-xs text-gray-400">Frequency</th>
                  <th className="py-2 px-3 text-right text-xs text-gray-400">Markets</th>
                  <th className="py-2 px-3 text-right text-xs text-gray-400">Vol (med)</th>
                  <th className="py-2 px-3 text-center text-xs text-gray-400">OHLC</th>
                </tr>
              </thead>
              <tbody>
                {group.sort((a, b) => a.series_ticker.localeCompare(b.series_ticker)).map(p => (
                  <tr key={p.series_ticker} className="border-b border-gray-800">
                    <td className="py-2 px-3 font-mono text-white">{p.series_ticker}</td>
                    <td className="py-2 px-3 text-gray-400 text-xs max-w-xs truncate">
                      {p.description?.slice(0, 60)}
                    </td>
                    <td className="py-2 px-3 text-gray-300">{p.settlement_frequency}</td>
                    <td className="py-2 px-3 text-right text-gray-300">{p.market_count?.toLocaleString()}</td>
                    <td className="py-2 px-3 text-right text-gray-300">{p.volume_stats?.median}</td>
                    <td className="py-2 px-3 text-center">
                      {p.external_asset
                        ? <span className="text-emerald-400">✓</span>
                        : <span className="text-gray-600">—</span>}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      ))}
    </div>
  )
}
