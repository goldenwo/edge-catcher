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

function ReportForm(_props: {
  dbs: DbInfo[]; db: string | null; date: string;
  dbsLoading: boolean; loading: boolean;
  formDisabled: boolean; generateDisabled: boolean;
  onDbChange: (n: string) => void;
  onDateChange: (d: string) => void;
  onGenerate: () => void;
}): JSX.Element | null { return null }

function TodayHero(_props: {
  today: TodayStats; date: string; all_time: AllTimeStats;
}): JSX.Element | null { return null }

function TodayByStrategySection(_props: {
  rows: TodayByStrategyRow[]; date: string;
}): JSX.Element | null { return null }

function OpenPositionsSection(_props: {
  rows: OpenPositionRow[];
}): JSX.Element | null { return null }

function AllTimeSection(_props: { stats: AllTimeStats }): JSX.Element | null { return null }

function AllTimeByStrategySection(_props: {
  rows: AllTimeByStrategyRow[];
}): JSX.Element | null { return null }

function RawJsonExpander(_props: { data: Report }): JSX.Element | null { return null }

function ErrorBanner(_props: { message: string }): JSX.Element | null { return null }

function EmptyDbsState(): JSX.Element | null { return null }
