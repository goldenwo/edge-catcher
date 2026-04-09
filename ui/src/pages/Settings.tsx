import { useEffect, useState } from 'react'
import { api, AISettings, ModelSettings, StorageReport, VacuumResult, ArchiveResult } from '../api'

interface ProviderConfig {
  id: keyof AISettings
  name: string
  envVar: string
  docsUrl: string
}

const PROVIDERS: ProviderConfig[] = [
  {
    id: 'anthropic',
    name: 'Anthropic',
    envVar: 'ANTHROPIC_API_KEY',
    docsUrl: 'https://console.anthropic.com/settings/keys',
  },
  {
    id: 'openai',
    name: 'OpenAI',
    envVar: 'OPENAI_API_KEY',
    docsUrl: 'https://platform.openai.com/api-keys',
  },
  {
    id: 'openrouter',
    name: 'OpenRouter',
    envVar: 'OPENROUTER_API_KEY',
    docsUrl: 'https://openrouter.ai/settings/keys',
  },
]

function ProviderCard({
  provider,
  isSet,
  onSaved,
}: {
  provider: ProviderConfig
  isSet: boolean
  onSaved: () => void
}) {
  const [keySet, setKeySet] = useState(isSet)
  const [input, setInput] = useState('')
  const [saving, setSaving] = useState(false)
  const [err, setErr] = useState<string | null>(null)
  const [saved, setSaved] = useState(false)

  useEffect(() => {
    setKeySet(isSet)
  }, [isSet])

  async function handleSave() {
    if (!input.trim()) return
    setSaving(true)
    setErr(null)
    setSaved(false)
    try {
      await api.saveAiKey(provider.id, input.trim())
      setKeySet(true)
      setInput('')
      setSaved(true)
      onSaved()
      setTimeout(() => setSaved(false), 3000)
    } catch (e) {
      setErr(e instanceof Error ? e.message : String(e))
    } finally {
      setSaving(false)
    }
  }

  return (
    <div className="rounded-lg border border-gray-800 bg-gray-900 p-5 flex flex-col gap-3">
      <div className="flex items-center justify-between">
        <div>
          <span className="font-semibold text-gray-100">{provider.name}</span>
          <span className="ml-2 text-xs text-gray-500 font-mono">{provider.envVar}</span>
        </div>
        {keySet ? (
          <span className="text-xs px-2 py-0.5 rounded bg-green-900 text-green-300">Key Set</span>
        ) : (
          <span className="text-xs px-2 py-0.5 rounded bg-yellow-900 text-yellow-300">Not Set</span>
        )}
      </div>

      <div className="flex gap-2">
        <input
          type="password"
          placeholder={keySet ? 'Enter new key to replace…' : `Paste ${provider.envVar}…`}
          value={input}
          onChange={e => setInput(e.target.value)}
          onKeyDown={e => e.key === 'Enter' && handleSave()}
          className="flex-1 px-3 py-1.5 rounded bg-gray-800 border border-gray-700 text-sm text-gray-100 placeholder-gray-500 focus:outline-none focus:border-indigo-500"
        />
        <button
          onClick={handleSave}
          disabled={!input.trim() || saving}
          className="px-3 py-1.5 rounded text-sm bg-indigo-700 hover:bg-indigo-600 disabled:opacity-40 disabled:cursor-not-allowed text-white transition-colors"
        >
          {saving ? 'Saving…' : 'Save'}
        </button>
      </div>

      {saved && (
        <p className="text-xs text-green-400">Saved — key will be used for all AI features.</p>
      )}
      {err && (
        <p className="text-xs text-red-400 bg-red-950 rounded px-3 py-1.5">{err}</p>
      )}
    </div>
  )
}

function StorageSection() {
  const [report, setReport] = useState<StorageReport | null>(null)
  const [loading, setLoading] = useState(false)
  const [archiving, setArchiving] = useState(false)
  const [vacuuming, setVacuuming] = useState(false)
  const [days, setDays] = useState(90)
  const [archiveResult, setArchiveResult] = useState<Record<string, ArchiveResult> | null>(null)
  const [vacuumResult, setVacuumResult] = useState<VacuumResult | null>(null)
  const [error, setError] = useState<string | null>(null)

  function loadReport() {
    setLoading(true)
    api.storageReport()
      .then(setReport)
      .catch((e) => setError(e instanceof Error ? e.message : String(e)))
      .finally(() => setLoading(false))
  }

  useEffect(() => { loadReport() }, [])

  async function handleArchive() {
    setArchiving(true)
    setArchiveResult(null)
    setError(null)
    try {
      const r = await api.storageArchive(days)
      setArchiveResult(r)
      loadReport()
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    } finally {
      setArchiving(false)
    }
  }

  async function handleVacuum() {
    setVacuuming(true)
    setVacuumResult(null)
    setError(null)
    try {
      const r = await api.storageVacuum()
      setVacuumResult(r)
      loadReport()
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e))
    } finally {
      setVacuuming(false)
    }
  }

  const fmtMB = (n: number) => n < 1 ? `${(n * 1024).toFixed(0)} KB` : `${n.toFixed(1)} MB`

  return (
    <section>
      <h2 className="text-xs font-medium text-gray-400 uppercase tracking-wider mb-3">
        Storage
      </h2>

      {error && (
        <p className="text-sm text-red-400 mb-4 bg-red-950 rounded px-3 py-1.5">{error}</p>
      )}

      {loading && !report && (
        <p className="text-sm text-gray-500">Loading storage info...</p>
      )}

      {report && (
        <div className="space-y-4">
          {/* DB sizes */}
          <div className="grid grid-cols-2 gap-3">
            {Object.entries(report.databases).map(([name, info]) => (
              <div key={name} className="rounded-lg border border-gray-800 bg-gray-900 px-4 py-3">
                <dt className="text-xs text-gray-500 font-mono">{name}</dt>
                <dd className="text-lg font-mono mt-1">{fmtMB(info.db_size_mb)}</dd>
              </div>
            ))}
          </div>

          {/* Row counts */}
          {Object.keys(report.row_counts).length > 0 && (
            <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
              {Object.entries(report.row_counts).map(([table, count]) => (
                <div key={table} className="rounded-lg border border-gray-800 bg-gray-900 px-4 py-3">
                  <dt className="text-xs text-gray-500">{table.replace(/_/g, ' ')}</dt>
                  <dd className="font-mono mt-1">{count.toLocaleString()}</dd>
                </div>
              ))}
            </div>
          )}

          {/* Archive */}
          <div className="rounded-lg border border-gray-800 bg-gray-900 p-5 space-y-3">
            <div>
              <h3 className="text-sm font-medium text-gray-200">Archive old data</h3>
              <p className="text-xs text-gray-500 mt-1">
                Exports rows older than the threshold to compressed CSV, then deletes them from the database.
              </p>
            </div>
            <div className="flex items-center gap-3">
              <label className="text-xs text-gray-400">Keep last</label>
              <input
                type="number"
                value={days}
                onChange={(e) => setDays(Math.max(1, Number(e.target.value)))}
                min={1}
                className="w-20 px-2 py-1.5 rounded bg-gray-800 border border-gray-700 text-sm text-gray-100 focus:outline-none focus:border-indigo-500"
              />
              <span className="text-xs text-gray-400">days</span>
              <button
                onClick={handleArchive}
                disabled={archiving}
                className="ml-auto px-3 py-1.5 rounded bg-amber-700 hover:bg-amber-600 disabled:opacity-50 text-sm text-white transition-colors"
              >
                {archiving ? 'Archiving...' : 'Archive Now'}
              </button>
            </div>
            {archiveResult && (
              <div className="text-xs text-gray-400 space-y-1 border-t border-gray-800 pt-3">
                {Object.entries(archiveResult).map(([key, r]) => (
                  <div key={key} className="flex justify-between">
                    <span>{key}</span>
                    <span className="font-mono">
                      {r.rows_archived > 0
                        ? `${r.rows_archived} archived, ${r.rows_deleted} deleted`
                        : 'nothing to archive'}
                    </span>
                  </div>
                ))}
              </div>
            )}
          </div>

          {/* Vacuum */}
          <div className="rounded-lg border border-gray-800 bg-gray-900 p-5 space-y-3">
            <div className="flex items-center justify-between">
              <div>
                <h3 className="text-sm font-medium text-gray-200">Vacuum database</h3>
                <p className="text-xs text-gray-500 mt-1">
                  Defragments the database file and reclaims unused space.
                </p>
              </div>
              <button
                onClick={handleVacuum}
                disabled={vacuuming}
                className="px-3 py-1.5 rounded bg-gray-700 hover:bg-gray-600 disabled:opacity-50 text-sm text-white transition-colors"
              >
                {vacuuming ? 'Vacuuming...' : 'Vacuum'}
              </button>
            </div>
            {vacuumResult && (
              <p className="text-xs text-gray-400 border-t border-gray-800 pt-3">
                {fmtMB(vacuumResult.before_mb)} → {fmtMB(vacuumResult.after_mb)}
                {vacuumResult.saved_mb > 0 && (
                  <span className="text-green-400 ml-2">
                    (saved {fmtMB(vacuumResult.saved_mb)})
                  </span>
                )}
              </p>
            )}
          </div>
        </div>
      )}
    </section>
  )
}

export default function Settings() {
  const [settings, setSettings] = useState<AISettings | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [modelSettings, setModelSettings] = useState<ModelSettings | null>(null)
  const [modelSaving, setModelSaving] = useState(false)
  const [modelSaved, setModelSaved] = useState(false)

  function load() {
    api.aiSettings()
      .then(setSettings)
      .catch(e => setError(e.message))
  }

  function loadModels() {
    api.aiModels().then(setModelSettings).catch(() => {})
  }

  useEffect(() => { load() }, [])
  useEffect(() => { loadModels() }, [])

  async function handleModelSave(model: string) {
    setModelSaving(true)
    setModelSaved(false)
    try {
      await api.saveAiModel(model || null)
      loadModels()
      setModelSaved(true)
      setTimeout(() => setModelSaved(false), 3000)
    } catch {
    } finally {
      setModelSaving(false)
    }
  }

  return (
    <div className="max-w-2xl space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-gray-100 mb-1">Settings</h1>
        <p className="text-sm text-gray-400">
          AI provider keys are saved to <span className="font-mono">.env</span> and
          used by Formalize and Interpret. Keys set here take effect immediately without restart.
        </p>
      </div>

      <section>
        <h2 className="text-xs font-medium text-gray-400 uppercase tracking-wider mb-3">
          AI Providers
        </h2>

        {error && (
          <p className="text-sm text-red-400 mb-4">{error}</p>
        )}

        <div className="flex flex-col gap-4">
          {PROVIDERS.map(p => (
            <ProviderCard
              key={p.id}
              provider={p}
              isSet={settings?.[p.id] ?? false}
              onSaved={() => { load(); loadModels() }}
            />
          ))}
        </div>
      </section>

      <section>
        <h2 className="text-xs font-medium text-gray-400 uppercase tracking-wider mb-3">
          Model
        </h2>

        {!modelSettings?.models.length ? (
          <p className="text-sm text-gray-500">Set an API key above to choose a model.</p>
        ) : (
          <div className="rounded-lg border border-gray-800 bg-gray-900 p-5 flex flex-col gap-3">
            <div className="flex items-center gap-3">
              <select
                value={modelSettings.current_model ?? ''}
                onChange={e => handleModelSave(e.target.value)}
                disabled={modelSaving}
                className="flex-1 px-3 py-1.5 rounded bg-gray-800 border border-gray-700 text-sm text-gray-100 focus:outline-none focus:border-indigo-500"
              >
                <option value="">Provider default</option>
                {modelSettings.models.map(m => (
                  <option key={m.id} value={m.id}>{m.label}</option>
                ))}
              </select>
            </div>
            {modelSaved && (
              <p className="text-xs text-green-400">Saved — model will be used for all AI features.</p>
            )}
          </div>
        )}
      </section>

      <StorageSection />
    </div>
  )
}
