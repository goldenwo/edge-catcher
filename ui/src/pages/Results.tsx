import { useEffect, useState } from 'react'
import { api, ResultDetail, ResultSummary } from '../api'
import Badge from '../components/Badge'

type SortKey = keyof Pick<ResultSummary, 'run_timestamp' | 'hypothesis_id' | 'verdict'>

function fmt(n: number | null | undefined, decimals = 4) {
  return n == null ? '—' : n.toFixed(decimals)
}

export default function Results() {
  const [rows, setRows] = useState<ResultSummary[]>([])
  const [detail, setDetail] = useState<ResultDetail | null>(null)
  const [summary, setSummary] = useState<string | null>(null)
  const [interpreting, setInterpreting] = useState(false)
  const [sortKey, setSortKey] = useState<SortKey>('run_timestamp')
  const [sortAsc, setSortAsc] = useState(false)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    api.results().then(setRows).catch((e) => setError(String(e)))
  }, [])

  const sortBy = (key: SortKey) => {
    if (sortKey === key) setSortAsc((a) => !a)
    else { setSortKey(key); setSortAsc(true) }
  }

  const sorted = [...rows].sort((a, b) => {
    const av = a[sortKey] ?? ''
    const bv = b[sortKey] ?? ''
    return sortAsc ? String(av).localeCompare(String(bv)) : String(bv).localeCompare(String(av))
  })

  const loadDetail = async (run_id: string) => {
    setSummary(null)
    try {
      setDetail(await api.result(run_id))
    } catch (e) {
      setError(String(e))
    }
  }

  const interpret = async () => {
    if (!detail) return
    setInterpreting(true)
    try {
      const r = await api.interpret(detail.run_id, null)
      setSummary(r.summary)
    } catch (e) {
      setSummary(`Error: ${e instanceof Error ? e.message : String(e)}`)
    } finally {
      setInterpreting(false)
    }
  }

  const Th = ({ col, label }: { col: SortKey; label: string }) => (
    <th
      className="px-4 py-2 text-left text-xs text-gray-400 font-medium cursor-pointer select-none hover:text-white"
      onClick={() => sortBy(col)}
    >
      {label} {sortKey === col ? (sortAsc ? '↑' : '↓') : ''}
    </th>
  )

  return (
    <div className="space-y-6">
      <h1 className="text-2xl font-semibold">Results</h1>

      {error && (
        <div className="rounded border border-red-700 bg-red-950 px-4 py-3 text-sm text-red-300">
          {error}
        </div>
      )}

      <div className="overflow-x-auto rounded-lg border border-gray-800">
        <table className="w-full text-sm">
          <thead className="border-b border-gray-800 bg-gray-900">
            <tr>
              <Th col="hypothesis_id" label="Hypothesis" />
              <Th col="verdict" label="Verdict" />
              <th className="px-4 py-2 text-left text-xs text-gray-400 font-medium">
                Fee-adj Edge
              </th>
              <Th col="run_timestamp" label="Timestamp" />
            </tr>
          </thead>
          <tbody>
            {sorted.map((r) => (
              <tr
                key={r.run_id}
                className={`border-b border-gray-800/50 cursor-pointer transition-colors ${
                  detail?.run_id === r.run_id ? 'bg-gray-800' : 'hover:bg-gray-900'
                }`}
                onClick={() => loadDetail(r.run_id)}
              >
                <td className="px-4 py-3 font-mono">{r.hypothesis_id}</td>
                <td className="px-4 py-3">
                  <Badge verdict={r.verdict} />
                </td>
                <td className="px-4 py-3 font-mono text-gray-400">—</td>
                <td className="px-4 py-3 text-gray-400 text-xs">
                  {new Date(r.run_timestamp).toLocaleString()}
                </td>
              </tr>
            ))}
            {rows.length === 0 && (
              <tr>
                <td
                  colSpan={4}
                  className="px-4 py-8 text-center text-gray-500 text-sm"
                >
                  No results yet.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>

      {detail && (
        <div className="rounded-lg border border-gray-800 bg-gray-900 p-6 space-y-4">
          <div className="flex items-center justify-between">
            <h2 className="font-medium">{detail.hypothesis_id}</h2>
            <Badge verdict={detail.verdict} />
          </div>

          <dl className="grid grid-cols-2 sm:grid-cols-3 gap-x-6 gap-y-3 text-sm">
            {(
              [
                ['Market', detail.market],
                ['N (naive)', detail.naive_n],
                ['N (clustered)', detail.clustered_n],
                ['Z-stat (naive)', fmt(detail.naive_z_stat, 3)],
                ['Z-stat (clustered)', fmt(detail.clustered_z_stat, 3)],
                ['Naive edge', fmt(detail.naive_edge)],
                ['Clustered edge', fmt(detail.clustered_edge)],
                ['Fee-adj edge', fmt(detail.fee_adjusted_edge)],
                ['CI low', fmt(detail.confidence_interval_low)],
                ['CI high', fmt(detail.confidence_interval_high)],
                ['Markets seen', detail.total_markets_seen],
              ] as [string, unknown][]
            ).map(([label, value]) => (
              <div key={label}>
                <dt className="text-gray-500 text-xs">{label}</dt>
                <dd className="font-mono mt-0.5">{value == null ? '—' : String(value)}</dd>
              </div>
            ))}
          </dl>

          {Array.isArray(detail.raw_bucket_data) &&
            (detail.raw_bucket_data as Record<string, unknown>[]).length > 0 && (
              <div>
                <h3 className="text-xs text-gray-400 uppercase tracking-wider mb-2">
                  Buckets
                </h3>
                <div className="space-y-1">
                  {(detail.raw_bucket_data as Record<string, unknown>[]).map((b, i) => {
                    const edge = Number(b.fee_adjusted_edge ?? 0)
                    const width = Math.min(Math.abs(edge) * 500, 100)
                    return (
                      <div key={i} className="flex items-center gap-3 text-xs">
                        <span className="text-gray-500 w-20 shrink-0 font-mono">
                          {String(b.bucket_lo)}–{String(b.bucket_hi)}
                        </span>
                        <div className="flex-1 bg-gray-800 rounded h-2 overflow-hidden">
                          <div
                            className={`h-full rounded ${
                              edge >= 0 ? 'bg-green-600' : 'bg-red-600'
                            }`}
                            style={{ width: `${width}%` }}
                          />
                        </div>
                        <span className="font-mono w-16 text-right text-gray-400">
                          {fmt(edge)}
                        </span>
                      </div>
                    )
                  })}
                </div>
              </div>
            )}

          <div>
            <button
              onClick={interpret}
              disabled={interpreting}
              className="px-3 py-1.5 rounded bg-indigo-700 hover:bg-indigo-600 disabled:opacity-50 text-sm transition-colors"
            >
              {interpreting ? 'Interpreting…' : 'Interpret with AI'}
            </button>
            {summary && (
              <div className="mt-3 rounded border border-gray-700 bg-gray-950 p-4 text-sm text-gray-300 whitespace-pre-wrap">
                {summary}
              </div>
            )}
          </div>
        </div>
      )}
    </div>
  )
}
