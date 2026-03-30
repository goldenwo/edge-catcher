import { useCallback, useEffect, useState } from 'react'
import { Link } from 'react-router-dom'
import { api, ResultSummary, Status } from '../api'
import Badge from '../components/Badge'

export default function Dashboard() {
  const [status, setStatus] = useState<Status | null>(null)
  const [recent, setRecent] = useState<ResultSummary[]>([])
  const [error, setError] = useState<string | null>(null)

  const loadStatus = useCallback(async () => {
    try {
      const [s, r] = await Promise.all([api.status(), api.results()])
      setStatus(s)
      setRecent(r.slice(0, 5))
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    }
  }, [])

  useEffect(() => { loadStatus() }, [loadStatus])

  return (
    <div className="max-w-3xl space-y-8">
      <h1 className="text-2xl font-semibold">Dashboard</h1>

      {error && (
        <div className="rounded border border-red-700 bg-red-950 px-4 py-3 text-sm text-red-300">
          {error}
        </div>
      )}

      {/* DB Stats */}
      <section>
        <h2 className="text-sm font-medium text-gray-400 uppercase tracking-wider mb-3">
          Database
        </h2>
        <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
          {[
            { label: 'Markets', value: status?.markets ?? '—' },
            { label: 'Trades', value: status?.trades ?? '—' },
            { label: 'Results', value: status?.results ?? '—' },
            { label: 'DB Size', value: status ? `${status.db_size_mb} MB` : '—' },
          ].map(({ label, value }) => (
            <div
              key={label}
              className="rounded-lg border border-gray-800 bg-gray-900 px-4 py-3"
            >
              <p className="text-xs text-gray-500 mb-1">{label}</p>
              <p className="text-xl font-mono">{value}</p>
            </div>
          ))}
        </div>
        {status?.last_download && (
          <p className="mt-2 text-xs text-gray-500">
            Last download: {new Date(status.last_download).toLocaleString()}
          </p>
        )}
        <Link
          to="/data-sources"
          className="mt-3 inline-block text-xs text-indigo-400 hover:text-indigo-300 transition-colors"
        >
          Manage data sources →
        </Link>
      </section>

      {/* Recent verdicts */}
      <section>
        <h2 className="text-sm font-medium text-gray-400 uppercase tracking-wider mb-3">
          Recent Runs
        </h2>
        {recent.length === 0 ? (
          <p className="text-sm text-gray-500">No results yet.</p>
        ) : (
          <div className="space-y-2">
            {recent.map((r) => (
              <div
                key={r.run_id}
                className="flex items-center justify-between rounded-lg border border-gray-800 bg-gray-900 px-4 py-3"
              >
                <div>
                  <p className="text-sm font-mono">{r.hypothesis_id}</p>
                  <p className="text-xs text-gray-500">
                    {new Date(r.run_timestamp).toLocaleString()}
                  </p>
                </div>
                <Badge verdict={r.verdict} />
              </div>
            ))}
          </div>
        )}
      </section>
    </div>
  )
}
