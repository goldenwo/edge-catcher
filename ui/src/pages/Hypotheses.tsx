import { useEffect, useMemo, useState } from 'react'
import { useSearchParams } from 'react-router-dom'
import { api, FormalizeResponse, Hypothesis } from '../api'
import ConfirmButton from '../components/ConfirmButton'

const providers = ['anthropic', 'openai', 'openrouter'] as const
type Provider = (typeof providers)[number]

type Tab = 'list' | 'create'

export default function Hypotheses() {
  const [searchParams, setSearchParams] = useSearchParams()
  const tabParam = searchParams.get('tab')
  const activeTab: Tab = tabParam === 'create' ? 'create' : 'list'

  // My Hypotheses state
  const [hypotheses, setHypotheses] = useState<Hypothesis[]>([])
  const [listError, setListError] = useState<string | null>(null)

  // Create New state
  const [description, setDescription] = useState('')
  const [provider, setProvider] = useState<Provider | ''>('')
  const [loading, setLoading] = useState(false)
  const [formalizeResult, setFormalizeResult] = useState<FormalizeResponse | null>(null)
  const [newHypothesisId, setNewHypothesisId] = useState<string | null>(null)

  // Filters
  const [search, setSearch] = useState('')
  const [marketFilter, setMarketFilter] = useState('')
  const [statusFilter, setStatusFilter] = useState('')

  const markets = useMemo(() => [...new Set(hypotheses.map((h) => h.market))].sort(), [hypotheses])
  const statuses = useMemo(() => [...new Set(hypotheses.map((h) => h.status))].sort(), [hypotheses])

  const filtered = useMemo(() => {
    const q = search.toLowerCase()
    return hypotheses.filter((h) => {
      if (q && !h.name.toLowerCase().includes(q) && !h.id.toLowerCase().includes(q)) return false
      if (marketFilter && h.market !== marketFilter) return false
      if (statusFilter && h.status !== statusFilter) return false
      return true
    })
  }, [hypotheses, search, marketFilter, statusFilter])

  const deleteHypothesis = async (id: string) => {
    try {
      await api.deleteHypothesis(id)
      setHypotheses((prev) => prev.filter((h) => h.id !== id))
    } catch (e) {
      setListError(e instanceof Error ? e.message : String(e))
    }
  }

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

  const submitFormalize = async (e: React.FormEvent) => {
    e.preventDefault()
    if (!description.trim()) return
    setLoading(true)
    setFormalizeResult(null)
    try {
      const r = await api.formalize(description.trim(), provider || null)
      setFormalizeResult(r)
      if (!r.error) {
        // Switch to list tab, refresh, and highlight the new hypothesis
        setNewHypothesisId(r.hypothesis_id)
        loadHypotheses()
        setDescription('')
        setProvider('')
        setTab('list')
        setTimeout(() => setNewHypothesisId(null), 5000)
      }
    } catch (e) {
      setFormalizeResult({
        message: '',
        error: e instanceof Error ? e.message : String(e),
        hypothesis_id: null,
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

          {/* Filters */}
          {hypotheses.length > 0 && (
            <div className="flex flex-wrap items-center gap-3">
              <input
                type="text"
                value={search}
                onChange={(e) => setSearch(e.target.value)}
                placeholder="Search by name or ID..."
                className="flex-1 min-w-[200px] rounded border border-gray-700 bg-gray-900 px-3 py-1.5 text-sm text-gray-100 placeholder-gray-600 focus:outline-none focus:ring-1 focus:ring-indigo-500"
              />
              {markets.length > 1 && (
                <select
                  value={marketFilter}
                  onChange={(e) => setMarketFilter(e.target.value)}
                  className="rounded border border-gray-700 bg-gray-900 px-2 py-1.5 text-sm text-gray-100 focus:outline-none focus:ring-1 focus:ring-indigo-500"
                >
                  <option value="">All markets</option>
                  {markets.map((m) => <option key={m} value={m}>{m}</option>)}
                </select>
              )}
              <div className="flex gap-1">
                {statuses.map((s) => (
                  <button
                    key={s}
                    onClick={() => setStatusFilter(statusFilter === s ? '' : s)}
                    className={`px-2 py-1 rounded text-xs transition-colors ${
                      statusFilter === s
                        ? 'bg-indigo-700 text-white'
                        : 'bg-gray-800 text-gray-400 hover:text-gray-200'
                    }`}
                  >
                    {s}
                  </button>
                ))}
              </div>
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
            {filtered.map((h) => (
              <div
                key={h.id}
                className={`rounded-lg border px-5 py-4 transition-colors duration-700 ${
                  newHypothesisId === h.id
                    ? 'border-indigo-500 bg-indigo-950/40'
                    : 'border-gray-800 bg-gray-900'
                }`}
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
                  <div className="flex items-center gap-2 shrink-0">
                    {h.source === 'local' && (
                      <ConfirmButton
                        onConfirm={() => deleteHypothesis(h.id)}
                        label="Delete"
                        confirmText="Delete?"
                      />
                    )}
                  </div>
                </div>
              </div>
            ))}
            {hypotheses.length > 0 && filtered.length === 0 && (
              <p className="text-sm text-gray-500">No hypotheses match your filters.</p>
            )}
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
