import { useEffect, useRef, useState } from 'react'
import {
  reporting,
  type DbInfo,
  type Report,
  type AllTimeStats,
  type TodayStats,
  type TodayByStrategyRow,
  type OpenPositionRow,
  type AllTimeByStrategyRow,
} from '../api'

function todayUtc(): string {
  return new Date().toISOString().slice(0, 10)
}

function pnlClass(cents: number): string {
  if (cents > 0) return 'text-emerald-400'
  if (cents < 0) return 'text-red-400'
  return 'text-gray-300'
}

function fmtPnlUsd(cents: number): string {
  const sign = cents > 0 ? '+' : cents < 0 ? '-' : ''
  return `${sign}$${(Math.abs(cents) / 100).toFixed(2)}`
}

function relativeTime(iso: string): string {
  const ms = Date.now() - new Date(iso).getTime()
  const minutes = Math.floor(ms / 60_000)
  if (minutes < 1) return 'just now'
  if (minutes < 60) return `${minutes}m ago`
  const hours = Math.floor(minutes / 60)
  if (hours < 24) return `${hours}h ago`
  const days = Math.floor(hours / 24)
  return `${days}d ago`
}

export default function Reports() {
  const [dbs, setDbs] = useState<DbInfo[]>([])
  const [dbsLoading, setDbsLoading] = useState(true)
  const [selectedDb, setSelectedDb] = useState<string | null>(null)
  const [date, setDate] = useState<string>(todayUtc())
  const [report, setReport] = useState<Report | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  // Monotonic counter to discard stale responses if multiple fetches overlap.
  const requestIdRef = useRef(0)

  // mount: fetch dbs, pick most-recent, fetch first report.
  // `cancelled` flag handles React Strict Mode's double-mount in dev.
  useEffect(() => {
    let cancelled = false
    reporting.listDbs()
      .then(({ dbs }) => {
        if (cancelled) return
        setDbs(dbs)
        const first = dbs[0]?.name ?? null
        setSelectedDb(first)
        if (first) generate(first, todayUtc())
      })
      .catch(e => {
        if (!cancelled) setError(e instanceof Error ? e.message : String(e))
      })
      .finally(() => { if (!cancelled) setDbsLoading(false) })
    return () => { cancelled = true }
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [])

  const generate = (db: string, d: string) => {
    const myId = ++requestIdRef.current
    setLoading(true)
    setError(null)
    reporting.runReport(db, d)
      .then(r => { if (myId === requestIdRef.current) setReport(r) })
      .catch(e => {
        if (myId !== requestIdRef.current) return  // stale failure; ignore
        const msg = e instanceof TypeError
          ? 'Cannot reach the API — is `uvicorn` running?'
          : e instanceof Error ? e.message : String(e)
        setError(msg)
      })
      .finally(() => {
        if (myId === requestIdRef.current) setLoading(false)
      })
  }

  return (
    <div className="max-w-4xl space-y-8">
      <h1 className="text-2xl font-semibold">Reports</h1>
      <ReportForm
        dbs={dbs}
        db={selectedDb}
        date={date}
        dbsLoading={dbsLoading}
        loading={loading}
        formDisabled={dbsLoading || dbs.length === 0}
        generateDisabled={loading || dbsLoading || dbs.length === 0 || !selectedDb}
        onDbChange={setSelectedDb}
        onDateChange={setDate}
        onGenerate={() => selectedDb && !loading && generate(selectedDb, date)}
      />
      {error && <ErrorBanner message={error} />}
      {!dbsLoading && dbs.length === 0 && <EmptyDbsState />}
      {report && (
        <>
          <TodayHero today={report.today} date={report.date} all_time={report.all_time} />
          <TodayByStrategySection rows={report.today_by_strategy} date={report.date} />
          <OpenPositionsSection rows={report.open_positions} />
          <AllTimeSection stats={report.all_time} />
          <AllTimeByStrategySection rows={report.all_time_by_strategy} />
          <RawJsonExpander data={report} />
        </>
      )}
    </div>
  )
}

// ── Sub-components — bodies filled in subsequent commits ───────────────────

function ReportForm({
  dbs, db, date, dbsLoading, loading,
  formDisabled, generateDisabled,
  onDbChange, onDateChange, onGenerate,
}: {
  dbs: DbInfo[]; db: string | null; date: string;
  dbsLoading: boolean; loading: boolean;
  formDisabled: boolean; generateDisabled: boolean;
  onDbChange: (n: string) => void;
  onDateChange: (d: string) => void;
  onGenerate: () => void;
}): JSX.Element {
  return (
    <section className="flex flex-col sm:flex-row sm:items-end gap-3">
      <label className="flex flex-col gap-1 flex-1">
        <span className="text-[10px] font-medium tracking-wider text-gray-500 uppercase">Database</span>
        <select
          value={db ?? ''}
          disabled={formDisabled}
          onChange={e => onDbChange(e.target.value)}
          className="bg-gray-900 border border-gray-800 rounded px-3 py-2 text-sm font-mono disabled:opacity-50"
        >
          {dbsLoading ? (
            <option value="">Loading databases…</option>
          ) : dbs.length === 0 ? (
            <option value="">No DBs in data/</option>
          ) : dbs.map(d => (
            <option key={d.name} value={d.name}>
              {d.name} — {d.row_count} rows, {relativeTime(d.mtime)}
            </option>
          ))}
        </select>
      </label>
      <label className="flex flex-col gap-1">
        <span className="text-[10px] font-medium tracking-wider text-gray-500 uppercase">Date</span>
        <input
          type="date"
          value={date}
          disabled={formDisabled}
          onChange={e => onDateChange(e.target.value)}
          className="bg-gray-900 border border-gray-800 rounded px-3 py-2 text-sm font-mono disabled:opacity-50"
        />
      </label>
      <button
        type="button"
        disabled={generateDisabled}
        onClick={onGenerate}
        className="bg-indigo-700 hover:bg-indigo-600 disabled:opacity-50 disabled:cursor-not-allowed text-indigo-50 px-4 py-2 rounded text-sm font-medium"
      >
        {loading ? 'Generating…' : 'Generate'}
      </button>
    </section>
  )
}

function TodayHero({ today, date, all_time }: {
  today: TodayStats; date: string; all_time: AllTimeStats;
}): JSX.Element {
  let subtitle: string
  if (today.settled_count === 0 && all_time.total_trades > 0) {
    subtitle = `0 settled on ${date} — trader has all-time activity, just nothing closed today (try a prior date).`
  } else if (today.settled_count === 0) {
    subtitle = `0 settled on ${date} — no trades in this DB yet. Run the paper trader to start populating data.`
  } else {
    const sign = all_time.roi_deployed_pct >= 0 ? '+' : ''
    subtitle = `${today.settled_count} settled · ROI on deployed ${sign}${all_time.roi_deployed_pct}%`
  }
  return (
    <section>
      <div className="rounded-lg border border-gray-800 bg-gray-900 px-6 py-5">
        <div className="text-xs text-gray-500 uppercase tracking-wider mb-1">
          Today's P&amp;L · {date}
        </div>
        <div className={`font-mono text-3xl font-semibold ${pnlClass(today.pnl_cents)}`}>
          {fmtPnlUsd(today.pnl_cents)}
        </div>
        <div className="text-xs text-gray-500 mt-2 font-mono">{subtitle}</div>
      </div>
    </section>
  )
}

function TodayByStrategySection({ rows, date }: {
  rows: TodayByStrategyRow[]; date: string;
}): JSX.Element {
  return (
    <section>
      <h2 className="text-sm font-medium text-gray-400 uppercase tracking-wider mb-3">
        Today by strategy
      </h2>
      <div className="rounded-lg border border-gray-800 bg-gray-900 overflow-hidden font-mono text-xs">
        <div className="grid grid-cols-[2fr_2fr_1fr_1fr_1fr] px-4 py-2 bg-gray-950 text-gray-500 uppercase text-[10px]">
          <span>Strategy</span><span>Series</span>
          <span className="text-right">N</span>
          <span className="text-right">P&amp;L</span>
          <span className="text-right">Status</span>
        </div>
        {rows.length === 0 ? (
          <div className="px-4 py-3 text-gray-500 text-xs">No settled trades on {date}.</div>
        ) : rows.map((r, i) => (
          <div key={i} className="grid grid-cols-[2fr_2fr_1fr_1fr_1fr] px-4 py-2 border-t border-gray-800">
            <span>{r.strategy}</span><span>{r.series_ticker}</span>
            <span className="text-right">{r.count}</span>
            <span className={`text-right ${pnlClass(r.pnl_cents)}`}>{fmtPnlUsd(r.pnl_cents)}</span>
            <span className="text-right">{r.status}</span>
          </div>
        ))}
      </div>
    </section>
  )
}

function OpenPositionsSection({ rows }: {
  rows: OpenPositionRow[];
}): JSX.Element {
  return (
    <section>
      <h2 className="text-sm font-medium text-gray-400 uppercase tracking-wider mb-3">
        Open positions
      </h2>
      <div className="rounded-lg border border-gray-800 bg-gray-900 overflow-hidden font-mono text-xs">
        <div className="grid grid-cols-[2fr_2fr_1fr] px-4 py-2 bg-gray-950 text-gray-500 uppercase text-[10px]">
          <span>Strategy</span><span>Series</span>
          <span className="text-right">N</span>
        </div>
        {rows.length === 0 ? (
          <div className="px-4 py-3 text-gray-500 text-xs">No open positions.</div>
        ) : rows.map((r, i) => (
          <div key={i} className="grid grid-cols-[2fr_2fr_1fr] px-4 py-2 border-t border-gray-800">
            <span>{r.strategy}</span><span>{r.series_ticker}</span>
            <span className="text-right">{r.count}</span>
          </div>
        ))}
      </div>
    </section>
  )
}

function AllTimeSection({ stats }: { stats: AllTimeStats }): JSX.Element {
  const cards = [
    { label: 'Net P&L', value: fmtPnlUsd(stats.net_pnl_cents), color: pnlClass(stats.net_pnl_cents) },
    { label: 'Win rate', value: `${stats.win_rate_pct}%`, color: '' },
    { label: 'Closed', value: String(stats.closed_trades), color: '' },
    { label: 'Open', value: String(stats.open_trades), color: '' },
  ]
  return (
    <section>
      <h2 className="text-sm font-medium text-gray-400 uppercase tracking-wider mb-3">All-time</h2>
      <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
        {cards.map(c => (
          <div key={c.label} className="rounded-lg border border-gray-800 bg-gray-900 px-4 py-3">
            <p className="text-xs text-gray-500 mb-1">{c.label}</p>
            <p className={`text-xl font-mono ${c.color}`}>{c.value}</p>
          </div>
        ))}
      </div>
    </section>
  )
}

function AllTimeByStrategySection({ rows }: {
  rows: AllTimeByStrategyRow[];
}): JSX.Element {
  return (
    <section>
      <h2 className="text-sm font-medium text-gray-400 uppercase tracking-wider mb-3">
        All-time by strategy
      </h2>
      <div className="rounded-lg border border-gray-800 bg-gray-900 overflow-hidden font-mono text-xs">
        <div className="grid grid-cols-[2fr_1fr_1fr_1fr] px-4 py-2 bg-gray-950 text-gray-500 uppercase text-[10px]">
          <span>Strategy</span>
          <span className="text-right">Closed</span>
          <span className="text-right">Win rate</span>
          <span className="text-right">P&amp;L</span>
        </div>
        {rows.length === 0 ? (
          <div className="px-4 py-3 text-gray-500 text-xs">No closed trades yet.</div>
        ) : rows.map((r, i) => (
          <div key={i} className="grid grid-cols-[2fr_1fr_1fr_1fr] px-4 py-2 border-t border-gray-800">
            <span>{r.strategy}</span>
            <span className="text-right">{r.closed_trades}</span>
            <span className="text-right">{r.win_rate_pct}%</span>
            <span className={`text-right ${pnlClass(r.net_pnl_cents)}`}>{fmtPnlUsd(r.net_pnl_cents)}</span>
          </div>
        ))}
      </div>
    </section>
  )
}

function RawJsonExpander({ data }: { data: Report }): JSX.Element {
  return (
    <details className="rounded-lg border border-gray-800 bg-gray-900 px-4 py-3">
      <summary className="text-xs text-gray-500 cursor-pointer">Raw JSON</summary>
      <pre className="text-xs font-mono text-gray-400 mt-3 overflow-x-auto">
        {JSON.stringify(data, null, 2)}
      </pre>
    </details>
  )
}

function ErrorBanner({ message }: { message: string }): JSX.Element {
  return (
    <div className="rounded border border-red-700 bg-red-950 px-4 py-3 text-sm text-red-300">
      {message}
    </div>
  )
}

function EmptyDbsState(): JSX.Element {
  return (
    <div className="rounded-lg border border-gray-800 bg-gray-900 px-4 py-6 text-sm text-gray-400 text-center">
      No <code className="text-gray-300">paper_trades</code> DBs found in <code className="text-gray-300">data/</code>.<br />
      Run the paper trader, then refresh this page.
    </div>
  )
}
