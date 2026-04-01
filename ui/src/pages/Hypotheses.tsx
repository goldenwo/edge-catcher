import { useEffect, useState } from 'react'
import { useSearchParams } from 'react-router-dom'
import { api, FormalizeResponse, Hypothesis } from '../api'
import Badge from '../components/Badge'

interface RunResult {
  verdict?: string
  error?: string
}

const providers = ['anthropic', 'openai', 'openrouter'] as const
type Provider = (typeof providers)[number]

type Tab = 'list' | 'create'

export default function Hypotheses() {
  const [searchParams, setSearchParams] = useSearchParams()
  const tabParam = searchParams.get('tab')
  const activeTab: Tab = tabParam === 'create' ? 'create' : 'list'

  // My Hypotheses state
  const [hypotheses, setHypotheses] = useState<Hypothesis[]>([])
  const [running, setRunning] = useState<Record<string, boolean>>({})
  const [results, setResults] = useState<Record<string, RunResult>>({})
  const [listError, setListError] = useState<string | null>(null)

  // Create New state
  const [description, setDescription] = useState('')
  const [provider, setProvider] = useState<Provider | ''>('')
  const [loading, setLoading] = useState(false)
  const [formalizeResult, setFormalizeResult] = useState<FormalizeResponse | null>(null)

  const loadHypotheses = () => {
    api.hypotheses().then(setHypotheses).catch((e) => setListError(String(e)))
  }

  useEffect(() => { loadHypotheses() }, [])

  const setTab = (tab: Tab) => {
    if (tab === 'list') {
      setSearchParams({})
    } else {
      setSearchParams({ tab: 'create' })
    }
  }

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

  const submitFormalize = async (e: React.FormEvent) => {
    e.preventDefault()
    if (!description.trim()) return
    setLoading(true)
    setFormalizeResult(null)
    try {
      const r = await api.formalize(description.trim(), provider || null)
      setFormalizeResult(r)
      if (!r.error) {
        // Switch to list tab and refresh
        loadHypotheses()
        setDescription('')
        setProvider('')
        setTab('list')
      }
    } catch (e) {
      setFormalizeResult({
        message: '',
        error: e instanceof Error ? e.message : String(e),
      })
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="max-w-3xl space-y-6">
      <h1 className="text-2xl font-semibold">Hypotheses</h1>

      {/* Tab bar */}
      <div className="flex gap-1 border-b border-gray-800">
        {([
          { id: 'list' as Tab, label: 'My Hypotheses' },
          { id: 'create' as Tab, label: 'Create New' },
        ]).map((tab) => (
          <button
            key={tab.id}
            onClick={() => setTab(tab.id)}
            className={`px-4 py-2 text-sm font-medium border-b-2 -mb-px transition-colors ${
              activeTab === tab.id
                ? 'border-indigo-500 text-white'
                : 'border-transparent text-gray-400 hover:text-gray-200'
            }`}
          >
            {tab.label}
          </button>
        ))}
      </div>

      {/* My Hypotheses tab */}
      {activeTab === 'list' && (
        <>
          {listError && (
            <div className="rounded border border-red-700 bg-red-950 px-4 py-3 text-sm text-red-300">
              {listError}
            </div>
          )}

          {hypotheses.length === 0 && !listError && (
            <p className="text-sm text-gray-500">
              No hypotheses configured.{' '}
              <button
                onClick={() => setTab('create')}
                className="text-indigo-400 hover:text-indigo-300 transition-colors"
              >
                Create one →
              </button>
            </p>
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
        </>
      )}

      {/* Create New tab */}
      {activeTab === 'create' && (
        <div className="space-y-6">
          <p className="text-sm text-gray-400">
            Describe a market hypothesis in plain English. The AI will generate a
            YAML config and stub module for it.
          </p>

          <form onSubmit={submitFormalize} className="space-y-4">
            <div>
              <label className="block text-xs text-gray-400 mb-1.5" htmlFor="desc">
                Hypothesis description
              </label>
              <textarea
                id="desc"
                rows={5}
                value={description}
                onChange={(e) => setDescription(e.target.value)}
                placeholder="e.g. Markets where the yes price is below 10¢ tend to expire yes more often than the implied probability suggests…"
                className="w-full rounded-lg border border-gray-700 bg-gray-900 px-4 py-3 text-sm text-gray-100 placeholder-gray-600 focus:outline-none focus:ring-1 focus:ring-indigo-500 resize-none"
              />
            </div>

            <div>
              <label className="block text-xs text-gray-400 mb-1.5" htmlFor="provider">
                AI provider
              </label>
              <select
                id="provider"
                value={provider}
                onChange={(e) => setProvider(e.target.value as Provider | '')}
                className="rounded-lg border border-gray-700 bg-gray-900 px-3 py-2 text-sm text-gray-100 focus:outline-none focus:ring-1 focus:ring-indigo-500"
              >
                <option value="">Auto-detect</option>
                {providers.map((p) => (
                  <option key={p} value={p}>
                    {p}
                  </option>
                ))}
              </select>
            </div>

            <button
              type="submit"
              disabled={loading || !description.trim()}
              className="px-4 py-2 rounded bg-indigo-600 hover:bg-indigo-500 disabled:opacity-50 disabled:cursor-not-allowed text-sm font-medium transition-colors"
            >
              {loading ? 'Formalizing…' : 'Formalize'}
            </button>
          </form>

          {formalizeResult && (
            <div
              className={`rounded-lg border p-4 text-sm whitespace-pre-wrap font-mono ${
                formalizeResult.error
                  ? 'border-red-700 bg-red-950 text-red-300'
                  : 'border-gray-700 bg-gray-900 text-gray-200'
              }`}
            >
              {formalizeResult.error
                ? `Error: ${formalizeResult.error}\n\n${formalizeResult.message}`
                : formalizeResult.message}
            </div>
          )}
        </div>
      )}
    </div>
  )
}
