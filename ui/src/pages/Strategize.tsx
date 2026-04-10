import { useEffect, useState } from 'react'
import { useSearchParams } from 'react-router-dom'
import { api, Hypothesis, ResultSummary } from '../api'
import { usePipeline } from '../components/PipelineStatus'

const providers = ['anthropic', 'openai', 'openrouter'] as const

export default function Strategize() {
  const { status: pipeline, loading: pipelineLoading, refresh: refreshPipeline } = usePipeline()
  const [searchParams] = useSearchParams()

  const [hypotheses, setHypotheses] = useState<Hypothesis[]>([])
  const [selectedHypId, setSelectedHypId] = useState<string>('')
  const [selectedRunId, setSelectedRunId] = useState<string>('')
  const [latestVerdict, setLatestVerdict] = useState<string | null>(null)
  const [provider, setProvider] = useState<string>('')
  const [generatedCode, setGeneratedCode] = useState<string>('')
  const [strategyName, setStrategyName] = useState<string>('')
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [saveResult, setSaveResult] = useState<{ ok: boolean; message: string } | null>(null)

  // Load hypotheses and read URL params on mount
  useEffect(() => {
    api.hypotheses().then((hyps) => {
      setHypotheses(hyps)
      const paramHyp = searchParams.get('hypothesis_id')
      const paramRun = searchParams.get('run_id')
      if (paramHyp) setSelectedHypId(paramHyp)
      if (paramRun) setSelectedRunId(paramRun)
    }).catch((e) => setError(String(e)))
  }, [searchParams])

  // Fetch latest verdict when hypothesis changes
  useEffect(() => {
    if (!selectedHypId) {
      setLatestVerdict(null)
      return
    }
    api.results(1, 0, { hypothesis_id: selectedHypId }).then((page) => {
      const matching = page.results
      if (matching.length > 0) {
        setLatestVerdict(matching[0].verdict)
        if (!selectedRunId) setSelectedRunId(matching[0].id)
      } else {
        setLatestVerdict(null)
      }
    }).catch(() => setLatestVerdict(null))
  }, [selectedHypId]) // eslint-disable-line react-hooks/exhaustive-deps

  const hasData = pipeline?.data.has_data ?? false
  const hasAnalysis = (pipeline?.analysis.count ?? 0) > 0
  const prerequisitesMet = hasData && hasAnalysis

  const handleGenerate = async () => {
    if (!selectedHypId) return
    setLoading(true)
    setError(null)
    setSaveResult(null)
    try {
      const r = await api.strategize(selectedHypId, selectedRunId || null, provider || null)
      if (r.error) {
        setError(r.error)
      } else {
        setGeneratedCode(r.code)
        setStrategyName(r.strategy_name)
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    } finally {
      setLoading(false)
    }
  }

  const handleSave = async () => {
    if (!generatedCode || !strategyName) return
    setSaveResult(null)
    try {
      const r = await api.saveStrategy(generatedCode, strategyName)
      if (r.ok) {
        setSaveResult({ ok: true, message: `Saved to ${r.path}` })
        refreshPipeline()
      } else {
        setSaveResult({ ok: false, message: r.error ?? 'Unknown error' })
      }
    } catch (e) {
      setSaveResult({ ok: false, message: e instanceof Error ? e.message : String(e) })
    }
  }

  return (
    <div className="space-y-6">
      <h1 className="text-2xl font-semibold">Strategize</h1>

      {/* Prerequisite banner */}
      {!pipelineLoading && !prerequisitesMet && (
        <div className="rounded border border-yellow-700 bg-yellow-950 px-4 py-3 text-sm text-yellow-300">
          {!hasData && <p>No market data found. Download data first.</p>}
          {hasData && !hasAnalysis && <p>No analysis results found. Run an analysis first.</p>}
        </div>
      )}

      {error && (
        <div className="rounded border border-red-700 bg-red-950 px-4 py-3 text-sm text-red-300">
          {error}
        </div>
      )}

      <div className="flex gap-6">
        {/* Left panel */}
        <div className="w-1/3 space-y-4">
          {/* Hypothesis dropdown */}
          <div>
            <label className="block text-xs text-gray-400 mb-1.5" htmlFor="hyp-select">
              Hypothesis
            </label>
            <select
              id="hyp-select"
              value={selectedHypId}
              onChange={(e) => {
                setSelectedHypId(e.target.value)
                setSelectedRunId('')
              }}
              className="w-full rounded-lg border border-gray-700 bg-gray-900 px-3 py-2 text-sm text-gray-100 focus:outline-none focus:ring-1 focus:ring-indigo-500"
            >
              <option value="">Select hypothesis...</option>
              {hypotheses.map((h) => (
                <option key={h.id} value={h.id}>
                  {h.name} ({h.id})
                </option>
              ))}
            </select>
          </div>

          {/* Latest verdict */}
          {selectedHypId && latestVerdict !== null && (
            <div className="rounded-lg border border-gray-800 bg-gray-900 px-4 py-3">
              <span className="text-xs text-gray-500">Latest verdict: </span>
              <span className={`text-sm font-medium ${
                latestVerdict === 'edge_confirmed' ? 'text-green-400' :
                latestVerdict === 'no_edge' ? 'text-red-400' :
                'text-yellow-400'
              }`}>
                {latestVerdict}
              </span>
            </div>
          )}

          {/* AI provider */}
          <div>
            <label className="block text-xs text-gray-400 mb-1.5" htmlFor="provider-select">
              AI Provider
            </label>
            <select
              id="provider-select"
              value={provider}
              onChange={(e) => setProvider(e.target.value)}
              className="w-full rounded-lg border border-gray-700 bg-gray-900 px-3 py-2 text-sm text-gray-100 focus:outline-none focus:ring-1 focus:ring-indigo-500"
            >
              <option value="">Auto-detect</option>
              {providers.map((p) => (
                <option key={p} value={p}>{p}</option>
              ))}
            </select>
          </div>

          {/* Generate button */}
          <button
            onClick={handleGenerate}
            disabled={loading || !selectedHypId || !prerequisitesMet}
            className="w-full px-4 py-2 rounded bg-indigo-600 hover:bg-indigo-500 disabled:opacity-50 disabled:cursor-not-allowed text-sm font-medium transition-colors"
          >
            {loading ? 'Generating...' : 'Generate Strategy'}
          </button>
        </div>

        {/* Right panel */}
        <div className="w-2/3 space-y-3">
          {strategyName && (
            <div className="flex items-center justify-between">
              <h2 className="text-sm font-medium text-gray-300 font-mono">{strategyName}</h2>
              <button
                onClick={handleSave}
                className="px-3 py-1.5 rounded bg-gray-700 hover:bg-gray-600 text-sm transition-colors"
              >
                Save to strategies_local.py
              </button>
            </div>
          )}

          {/* Code viewer */}
          <pre className="bg-gray-900 border border-gray-700 rounded-lg p-4 text-sm font-mono overflow-x-auto whitespace-pre-wrap text-gray-100 min-h-[300px]">
            {loading ? (
              <span className="text-gray-500 animate-pulse">Generating strategy code...</span>
            ) : generatedCode ? (
              generatedCode
            ) : (
              <span className="text-gray-500">Generated strategy code will appear here.</span>
            )}
          </pre>

          {/* Save result feedback */}
          {saveResult && (
            <div className={`rounded border px-4 py-3 text-sm ${
              saveResult.ok
                ? 'border-green-700 bg-green-950 text-green-300'
                : 'border-red-700 bg-red-950 text-red-300'
            }`}>
              {saveResult.message}
            </div>
          )}
        </div>
      </div>
    </div>
  )
}
