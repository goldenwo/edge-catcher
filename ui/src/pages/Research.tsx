import { useEffect, useState } from 'react'
import { getResearchProfiles, getLoopStatus, getReviewQueue } from '../api'

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

interface LoopStatus {
  phase: string
  recent_activity: Array<{ decision: string; created_at: string }>
  latest_checkpoint: { checkpoint: string; created_at: string } | null
}

interface ReviewItem {
  strategy: string
  series: string
  sharpe: number
  net_pnl_cents: number
  win_rate: number
  total_trades: number
  verdict: string
  verdict_reason: string
}

export default function Research() {
  const [profiles, setProfiles] = useState<SeriesProfile[]>([])
  const [loopStatus, setLoopStatus] = useState<LoopStatus | null>(null)
  const [reviewQueue, setReviewQueue] = useState<ReviewItem[]>([])
  const [expandedRows, setExpandedRows] = useState<Set<string>>(new Set())
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    Promise.all([
      getResearchProfiles().then(d => setProfiles((d.profiles || []) as SeriesProfile[])),
      getLoopStatus().then(d => setLoopStatus(d as LoopStatus)),
      getReviewQueue().then(d => setReviewQueue((d.strategies || []) as ReviewItem[])),
    ]).finally(() => setLoading(false))
  }, [])

  const toggleRow = (ticker: string) => {
    setExpandedRows(prev => {
      const next = new Set(prev)
      next.has(ticker) ? next.delete(ticker) : next.add(ticker)
      return next
    })
  }

  // Group profiles by asset class
  const grouped = profiles.reduce<Record<string, SeriesProfile[]>>((acc, p) => {
    const key = p.asset_class || 'Unknown'
    ;(acc[key] = acc[key] || []).push(p)
    return acc
  }, {})

  if (loading) return <div className="text-gray-400">Loading...</div>

  return (
    <div className="max-w-6xl">
      <h1 className="text-2xl font-semibold text-white mb-6">Research Dashboard</h1>

      {/* Loop Status */}
      <section className="mb-8 p-4 bg-gray-900 border border-gray-800 rounded-lg">
        <h2 className="text-lg font-medium text-white mb-3">Loop Status</h2>
        <div className="flex gap-6 items-center">
          <div>
            <span className="text-gray-400 text-sm">Phase: </span>
            <span className={`px-2 py-0.5 rounded text-xs font-medium ${
              loopStatus?.phase === 'idle'
                ? 'bg-gray-800 text-gray-400'
                : 'bg-emerald-900/50 text-emerald-400'
            }`}>
              {loopStatus?.phase || 'idle'}
            </span>
          </div>
          {loopStatus?.latest_checkpoint && (
            <div className="text-sm text-gray-400">
              Last checkpoint: {loopStatus.latest_checkpoint.checkpoint} at{' '}
              {new Date(loopStatus.latest_checkpoint.created_at).toLocaleString()}
            </div>
          )}
        </div>
        {loopStatus?.recent_activity && loopStatus.recent_activity.length > 0 && (
          <div className="mt-3">
            <span className="text-sm text-gray-400">Recent activity:</span>
            <ul className="mt-1 space-y-0.5">
              {loopStatus.recent_activity.slice(0, 5).map((a, i) => (
                <li key={i} className="text-xs text-gray-500 pl-4">
                  {a.decision}
                </li>
              ))}
            </ul>
          </div>
        )}
      </section>

      {/* Review Queue */}
      {reviewQueue.length > 0 && (
        <section className="mb-8">
          <h2 className="text-lg font-medium text-white mb-3">
            Review Queue ({reviewQueue.length})
          </h2>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-gray-700 text-left text-gray-400">
                  <th className="py-2 pr-4">Strategy</th>
                  <th className="py-2 pr-4">Series</th>
                  <th className="py-2 pr-4">Verdict</th>
                  <th className="py-2 pr-4 text-right">Sharpe</th>
                  <th className="py-2 pr-4 text-right">PnL</th>
                  <th className="py-2 pr-4 text-right">Win Rate</th>
                  <th className="py-2 text-right">Trades</th>
                </tr>
              </thead>
              <tbody>
                {reviewQueue.map((r, i) => (
                  <tr key={i} className="border-b border-gray-800">
                    <td className="py-2 pr-4 font-medium text-white">{r.strategy}</td>
                    <td className="py-2 pr-4 text-gray-300">{r.series}</td>
                    <td className="py-2 pr-4">
                      <span className={`px-1.5 py-0.5 rounded text-xs font-medium ${
                        r.verdict === 'promote'
                          ? 'bg-emerald-900/50 text-emerald-400'
                          : 'bg-yellow-900/50 text-yellow-400'
                      }`}>
                        {r.verdict}
                      </span>
                    </td>
                    <td className="py-2 pr-4 text-right text-gray-300">
                      {r.sharpe?.toFixed(2) ?? '—'}
                    </td>
                    <td className="py-2 pr-4 text-right text-gray-300">
                      {r.net_pnl_cents?.toFixed(0) ?? '—'}¢
                    </td>
                    <td className="py-2 pr-4 text-right text-gray-300">
                      {r.win_rate != null ? `${(r.win_rate * 100).toFixed(1)}%` : '—'}
                    </td>
                    <td className="py-2 text-right text-gray-300">{r.total_trades ?? '—'}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </section>
      )}

      {/* Series Profiles */}
      <section>
        <h2 className="text-lg font-medium text-white mb-3">
          Series Profiles ({profiles.length})
        </h2>
        {Object.entries(grouped).sort().map(([assetClass, group]) => (
          <div key={assetClass} className="mb-6">
            <h3 className="text-sm font-medium text-gray-400 uppercase tracking-wider mb-2">
              {assetClass}
            </h3>
            <div className="overflow-x-auto">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b border-gray-700 text-left text-gray-400">
                    <th className="py-2 pr-4">Series</th>
                    <th className="py-2 pr-4">Description</th>
                    <th className="py-2 pr-4">Frequency</th>
                    <th className="py-2 pr-4 text-right">Markets</th>
                    <th className="py-2 pr-4 text-right">Vol (med)</th>
                    <th className="py-2 text-center">OHLC</th>
                  </tr>
                </thead>
                <tbody>
                  {group.sort((a, b) => a.series_ticker.localeCompare(b.series_ticker)).map(p => (
                    <tr
                      key={p.series_ticker}
                      className="border-b border-gray-800 cursor-pointer hover:bg-gray-900/50"
                      onClick={() => toggleRow(p.series_ticker)}
                    >
                      <td className="py-2 pr-4 font-medium text-white">{p.series_ticker}</td>
                      <td className="py-2 pr-4 text-gray-400 text-xs max-w-xs truncate">
                        {p.description?.slice(0, 60)}
                      </td>
                      <td className="py-2 pr-4 text-gray-300">{p.settlement_frequency}</td>
                      <td className="py-2 pr-4 text-right text-gray-300">
                        {p.market_count?.toLocaleString()}
                      </td>
                      <td className="py-2 pr-4 text-right text-gray-300">
                        {p.volume_stats?.median}
                      </td>
                      <td className="py-2 text-center">
                        {p.external_asset ? (
                          <span className="text-emerald-400">✓</span>
                        ) : (
                          <span className="text-gray-600">—</span>
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        ))}
      </section>
    </div>
  )
}
