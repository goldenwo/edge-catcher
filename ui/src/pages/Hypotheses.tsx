import { useEffect, useState } from 'react'
import { api, Hypothesis } from '../api'
import Badge from '../components/Badge'

interface RunResult {
  verdict?: string
  error?: string
}

export default function Hypotheses() {
  const [hypotheses, setHypotheses] = useState<Hypothesis[]>([])
  const [running, setRunning] = useState<Record<string, boolean>>({})
  const [results, setResults] = useState<Record<string, RunResult>>({})
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    api.hypotheses().then(setHypotheses).catch((e) => setError(String(e)))
  }, [])

  const runAnalysis = async (id: string) => {
    setRunning((r) => ({ ...r, [id]: true }))
    try {
      const data = await api.analyze(id)
      const result = data[id] as Record<string, unknown> | undefined
      setResults((prev) => ({
        ...prev,
        [id]: { verdict: result?.verdict as string | undefined },
      }))
    } catch (e) {
      setResults((prev) => ({
        ...prev,
        [id]: { error: e instanceof Error ? e.message : String(e) },
      }))
    } finally {
      setRunning((r) => ({ ...r, [id]: false }))
    }
  }

  return (
    <div className="max-w-3xl space-y-6">
      <h1 className="text-2xl font-semibold">Hypotheses</h1>

      {error && (
        <div className="rounded border border-red-700 bg-red-950 px-4 py-3 text-sm text-red-300">
          {error}
        </div>
      )}

      {hypotheses.length === 0 && !error && (
        <p className="text-sm text-gray-500">No hypotheses configured.</p>
      )}

      <div className="space-y-3">
        {hypotheses.map((h) => (
          <div
            key={h.id}
            className="rounded-lg border border-gray-800 bg-gray-900 px-5 py-4"
          >
            <div className="flex items-start justify-between gap-4">
              <div>
                <p className="font-medium">{h.name}</p>
                <p className="text-xs text-gray-500 mt-0.5 font-mono">{h.id}</p>
                <div className="flex gap-2 mt-2">
                  <span className="text-xs text-gray-400 border border-gray-700 rounded px-1.5 py-0.5">
                    {h.market}
                  </span>
                  <span className="text-xs text-gray-400 border border-gray-700 rounded px-1.5 py-0.5">
                    {h.status}
                  </span>
                </div>
              </div>
              <button
                onClick={() => runAnalysis(h.id)}
                disabled={running[h.id]}
                className="shrink-0 px-3 py-1.5 rounded bg-indigo-700 hover:bg-indigo-600 disabled:opacity-50 text-sm transition-colors"
              >
                {running[h.id] ? 'Running…' : 'Run Analysis'}
              </button>
            </div>

            {results[h.id] && (
              <div className="mt-3 pt-3 border-t border-gray-800">
                {results[h.id].error ? (
                  <p className="text-sm text-red-400">{results[h.id].error}</p>
                ) : (
                  <div className="flex items-center gap-2">
                    <span className="text-xs text-gray-500">Verdict:</span>
                    <Badge verdict={results[h.id].verdict ?? null} />
                  </div>
                )}
              </div>
            )}
          </div>
        ))}
      </div>
    </div>
  )
}
