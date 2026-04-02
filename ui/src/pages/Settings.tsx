import { useEffect, useState } from 'react'
import { api, AISettings, ModelSettings } from '../api'

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
    </div>
  )
}
