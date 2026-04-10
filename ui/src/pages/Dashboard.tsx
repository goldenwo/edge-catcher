import { useCallback, useEffect, useState } from 'react'
import { Link } from 'react-router-dom'
import { api, BacktestHistoryItem, ResultSummary, Status } from '../api'
import Badge from '../components/Badge'
import { usePipeline } from '../components/PipelineStatus'

interface ActivityItem {
  id: string
  type: 'analysis' | 'backtest'
  label: string
  sub: string
  timestamp: string
  verdict?: string | null
  sharpe?: number | null
}

export default function Dashboard() {
  const { status: pipeline } = usePipeline()
  const [status, setStatus] = useState<Status | null>(null)
  const [activity, setActivity] = useState<ActivityItem[]>([])
  const [error, setError] = useState<string | null>(null)

  const load = useCallback(async () => {
    try {
      const [s, resultsPage, historyPage] = await Promise.all([
        api.status(),
        api.results(10),
        api.backtestHistory(10),
      ])
      setStatus(s)

      const analysisItems: ActivityItem[] = (resultsPage.results as ResultSummary[]).map((r) => ({
        id: `a-${r.id}`,
        type: 'analysis',
        label: r.series,
        sub: r.test_type,
        timestamp: r.created_at,
        verdict: r.verdict,
      }))

      const backtestItems: ActivityItem[] = (historyPage.results as BacktestHistoryItem[]).map((b) => ({
        id: `b-${b.task_id}`,
        type: 'backtest',
        label: b.series,
        sub: b.strategies.join(', ') || 'Backtest',
        timestamp: b.timestamp,
        sharpe: b.sharpe,
      }))

      const merged = [...analysisItems, ...backtestItems].sort(
        (a, b) => new Date(b.timestamp).getTime() - new Date(a.timestamp).getTime()
      )
      setActivity(merged.slice(0, 10))
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    }
  }, [])

  useEffect(() => { load() }, [load])

  const pipelineCards = [
    {
      title: 'Data Sources',
      href: '/data-sources',
      icon: '⬡',
      stats: pipeline
        ? `${pipeline.data.markets} markets · ${pipeline.data.trades} trades`
        : '—',
      active: pipeline?.data.has_data ?? false,
    },
    {
      title: 'Hypothesize',
      href: '/hypotheses',
      icon: '◈',
      stats: pipeline ? `${pipeline.hypotheses.count} hypotheses` : '—',
      active: (pipeline?.hypotheses.count ?? 0) > 0,
    },
    {
      title: 'Analyze',
      href: '/analyze',
      icon: '◉',
      stats: pipeline
        ? `${pipeline.analysis.count} runs · ${pipeline.analysis.latest_verdict ?? 'no verdict'}`
        : '—',
      active: (pipeline?.analysis.count ?? 0) > 0,
    },
    {
      title: 'Strategize',
      href: '/strategize',
      icon: '◧',
      stats: pipeline ? `${pipeline.strategies.count} strategies` : '—',
      active: (pipeline?.strategies.count ?? 0) > 0,
    },
    {
      title: 'Backtest',
      href: '/backtest',
      icon: '◪',
      stats: pipeline
        ? `${pipeline.backtest.count} runs · sharpe ${pipeline.backtest.latest_sharpe != null ? pipeline.backtest.latest_sharpe.toFixed(2) : '—'}`
        : '—',
      active: (pipeline?.backtest.count ?? 0) > 0,
    },
  ]

  return (
    <div className="max-w-4xl space-y-8">
      <h1 className="text-2xl font-semibold">Dashboard</h1>

      {error && (
        <div className="rounded border border-red-700 bg-red-950 px-4 py-3 text-sm text-red-300">
          {error}
        </div>
      )}

      {/* Pipeline Overview */}
      <section>
        <h2 className="text-sm font-medium text-gray-400 uppercase tracking-wider mb-3">
          Pipeline
        </h2>
        <div className="grid grid-cols-1 sm:grid-cols-5 gap-3">
          {pipelineCards.map((card, i) => (
            <Link
              key={card.title}
              to={card.href}
              className="group relative rounded-lg border border-gray-800 bg-gray-900 px-4 py-4 hover:border-indigo-700 transition-colors"
            >
              {/* Step number */}
              <span className="text-xs text-gray-600 font-mono">{i + 1}</span>
              {/* Active dot */}
              <span
                className={`absolute top-3 right-3 w-2 h-2 rounded-full ${
                  card.active ? 'bg-green-500' : 'bg-gray-700'
                }`}
              />
              <p className="font-medium text-sm mt-1 group-hover:text-indigo-300 transition-colors">
                {card.title}
              </p>
              <p className="text-xs text-gray-500 mt-1 leading-tight">{card.stats}</p>
            </Link>
          ))}
        </div>
      </section>

      {/* Quick Stats */}
      <section>
        <h2 className="text-sm font-medium text-gray-400 uppercase tracking-wider mb-3">
          Database
        </h2>
        <div className="grid grid-cols-2 sm:grid-cols-3 gap-3">
          {[
            { label: 'Markets', value: status?.markets ?? '—' },
            { label: 'Trades', value: status?.trades ?? '—' },
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
      </section>

      {/* Activity Feed */}
      <section>
        <h2 className="text-sm font-medium text-gray-400 uppercase tracking-wider mb-3">
          Recent Activity
        </h2>
        {activity.length === 0 ? (
          <p className="text-sm text-gray-500">No activity yet.</p>
        ) : (
          <div className="space-y-2">
            {activity.map((item) => (
              <div
                key={item.id}
                className="flex items-center justify-between rounded-lg border border-gray-800 bg-gray-900 px-4 py-3"
              >
                <div className="flex items-center gap-3 min-w-0">
                  <span
                    className={`shrink-0 text-xs px-1.5 py-0.5 rounded border font-mono ${
                      item.type === 'analysis'
                        ? 'border-indigo-800 text-indigo-400'
                        : 'border-purple-800 text-purple-400'
                    }`}
                  >
                    {item.type === 'analysis' ? 'analysis' : 'backtest'}
                  </span>
                  <div className="min-w-0">
                    <p className="text-sm font-mono truncate">{item.label}</p>
                    <p className="text-xs text-gray-500 truncate">{item.sub}</p>
                  </div>
                </div>
                <div className="shrink-0 flex items-center gap-3 ml-4">
                  {item.type === 'analysis' && item.verdict !== undefined && (
                    <Badge verdict={item.verdict} />
                  )}
                  {item.type === 'backtest' && item.sharpe != null && (
                    <span className="text-xs font-mono text-gray-300">
                      sharpe {item.sharpe.toFixed(2)}
                    </span>
                  )}
                  <span className="text-xs text-gray-600">
                    {new Date(item.timestamp).toLocaleString()}
                  </span>
                </div>
              </div>
            ))}
          </div>
        )}
      </section>
    </div>
  )
}
