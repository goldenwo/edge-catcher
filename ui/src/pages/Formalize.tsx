import { useState } from 'react'
import { api, FormalizeResponse } from '../api'

const providers = ['anthropic', 'openai', 'openrouter'] as const
type Provider = (typeof providers)[number]

export default function Formalize() {
  const [description, setDescription] = useState('')
  const [provider, setProvider] = useState<Provider | ''>('')
  const [loading, setLoading] = useState(false)
  const [result, setResult] = useState<FormalizeResponse | null>(null)

  const submit = async (e: React.FormEvent) => {
    e.preventDefault()
    if (!description.trim()) return
    setLoading(true)
    setResult(null)
    try {
      const r = await api.formalize(description.trim(), provider || null)
      setResult(r)
    } catch (e) {
      setResult({
        message: '',
        error: e instanceof Error ? e.message : String(e),
      })
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="max-w-2xl space-y-6">
      <h1 className="text-2xl font-semibold">Formalize Hypothesis</h1>
      <p className="text-sm text-gray-400">
        Describe a market hypothesis in plain English. The AI will generate a
        YAML config and stub module for it.
      </p>

      <form onSubmit={submit} className="space-y-4">
        <div>
          <label
            className="block text-xs text-gray-400 mb-1.5"
            htmlFor="desc"
          >
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
          <label
            className="block text-xs text-gray-400 mb-1.5"
            htmlFor="provider"
          >
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

      {result && (
        <div
          className={`rounded-lg border p-4 text-sm whitespace-pre-wrap font-mono ${
            result.error
              ? 'border-red-700 bg-red-950 text-red-300'
              : 'border-gray-700 bg-gray-900 text-gray-200'
          }`}
        >
          {result.error
            ? `Error: ${result.error}\n\n${result.message}`
            : result.message}
        </div>
      )}
    </div>
  )
}
