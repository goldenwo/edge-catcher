import { useEffect, useRef, useState } from 'react'

interface AdapterInfo {
  id: string
  name: string
  description: string
  requires_api_key: boolean
  api_key_env_var: string | null
  api_key_set: boolean
  download_status: 'idle' | 'running' | 'complete' | 'error'
  default_start_date: string | null
}

interface AdapterDownloadStatus {
  adapter_id: string
  running: boolean
  progress: string
  rows_fetched: number
  error: string | null
}

const API = import.meta.env.VITE_API_URL ?? ''

async function fetchAdapters(): Promise<AdapterInfo[]> {
  const res = await fetch(`${API}/adapters`)
  if (!res.ok) throw new Error(await res.text())
  return res.json()
}

async function fetchStatus(id: string): Promise<AdapterDownloadStatus> {
  const res = await fetch(`${API}/adapters/${id}/status`)
  if (!res.ok) throw new Error(await res.text())
  return res.json()
}

async function startDownload(id: string, apiKey?: string, startDate?: string): Promise<void> {
  const token = localStorage.getItem('ec_token') ?? ''
  const res = await fetch(`${API}/adapters/${id}/download`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      ...(token ? { Authorization: `Bearer ${token}` } : {}),
    },
    body: JSON.stringify({ adapter_id: id, api_key: apiKey ?? null, start_date: startDate ?? null }),
  })
  if (!res.ok) throw new Error(await res.text())
}

function StatusBadge({ set, required }: { set: boolean; required: boolean }) {
  if (set) return <span className="text-xs px-2 py-0.5 rounded bg-green-900 text-green-300">Key Set</span>
  if (required) return <span className="text-xs px-2 py-0.5 rounded bg-yellow-900 text-yellow-300">Key Required</span>
  return <span className="text-xs px-2 py-0.5 rounded bg-gray-800 text-gray-500">No Key</span>
}

function AdapterCard({ adapter }: { adapter: AdapterInfo }) {
  const [status, setStatus] = useState<AdapterDownloadStatus>({
    adapter_id: adapter.id,
    running: adapter.download_status === 'running',
    progress: adapter.download_status === 'idle' ? 'Idle' : adapter.download_status,
    rows_fetched: 0,
    error: null,
  })
  const [apiKey, setApiKey] = useState('')
  const [apiKeySet, setApiKeySet] = useState(adapter.api_key_set)
  const [editingKey, setEditingKey] = useState(false)
  const [startDate, setStartDate] = useState(adapter.default_start_date ?? '')
  const [triggering, setTriggering] = useState(false)
  const [err, setErr] = useState<string | null>(null)
  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null)

  // Fetch real status immediately on mount so we never show the raw string from /adapters
  useEffect(() => {
    fetchStatus(adapter.id).then(setStatus).catch(() => null)
  }, [adapter.id])

  useEffect(() => {
    if (!status.running) return
    intervalRef.current = setInterval(async () => {
      try {
        const s = await fetchStatus(adapter.id)
        setStatus(s)
        if (!s.running) clearInterval(intervalRef.current!)
      } catch {
        clearInterval(intervalRef.current!)
      }
    }, 2000)
    return () => {
      if (intervalRef.current) clearInterval(intervalRef.current)
    }
  }, [status.running, adapter.id])

  const canDownload = !status.running && !triggering && (!adapter.requires_api_key || apiKeySet || apiKey.trim().length > 0)

  async function handleDownload() {
    setTriggering(true)
    setErr(null)
    try {
      const keyToSend = apiKey.trim() || undefined
      await startDownload(adapter.id, keyToSend, startDate.trim() || undefined)
      if (keyToSend) { setApiKeySet(true); setEditingKey(false) }
      setStatus(s => ({ ...s, running: true, progress: 'Starting...', error: null }))
    } catch (e: unknown) {
      setErr(e instanceof Error ? e.message : String(e))
    } finally {
      setTriggering(false)
    }
  }

  return (
    <div className="rounded-lg border border-gray-800 bg-gray-900 p-5 flex flex-col gap-3">
      <div className="flex items-start justify-between gap-4">
        <div>
          <div className="flex items-center gap-2 mb-1">
            <span className="font-semibold text-gray-100">{adapter.name}</span>
            {adapter.api_key_env_var && <StatusBadge set={apiKeySet} required={adapter.requires_api_key} />}
          </div>
          <p className="text-sm text-gray-400">{adapter.description}</p>
        </div>
        <div className="flex items-center gap-2 shrink-0">
          <div className="flex flex-col items-end gap-0.5">
            <label className="text-xs text-gray-500">Start date</label>
            <input
              type="date"
              value={startDate}
              onChange={e => setStartDate(e.target.value)}
              disabled={status.running}
              className="px-2 py-1 rounded bg-gray-800 border border-gray-700 text-sm text-gray-100 focus:outline-none focus:border-indigo-500 disabled:opacity-40"
            />
          </div>
          <button
            onClick={handleDownload}
            disabled={!canDownload}
            className="px-4 py-2 rounded text-sm font-medium transition-colors bg-indigo-600 hover:bg-indigo-500 disabled:opacity-40 disabled:cursor-not-allowed text-white self-end"
          >
            {status.running ? 'Running…' : 'Download'}
          </button>
        </div>
      </div>

      {adapter.api_key_env_var && (apiKeySet && !editingKey ? (
        <button
          onClick={() => setEditingKey(true)}
          className="text-xs text-gray-500 hover:text-gray-300 transition-colors self-start"
        >
          Change key
        </button>
      ) : (
        <div className="flex gap-2">
          <input
            type="password"
            placeholder={adapter.requires_api_key ? adapter.api_key_env_var! : `${adapter.api_key_env_var} (optional)`}
            value={apiKey}
            onChange={e => setApiKey(e.target.value)}
            className="flex-1 px-3 py-1.5 rounded bg-gray-800 border border-gray-700 text-sm text-gray-100 placeholder-gray-500 focus:outline-none focus:border-indigo-500"
          />
          {editingKey && (
            <button
              onClick={() => { setApiKey(''); setEditingKey(false) }}
              className="px-3 py-1.5 rounded text-sm bg-gray-700 hover:bg-gray-600 text-gray-300 transition-colors"
            >
              Cancel
            </button>
          )}
          <button
            onClick={handleDownload}
            disabled={!apiKey.trim() || status.running}
            className="px-3 py-1.5 rounded text-sm bg-indigo-700 hover:bg-indigo-600 disabled:opacity-40 disabled:cursor-not-allowed text-white transition-colors"
          >
            Save & Use
          </button>
        </div>
      ))}

      {status.running && (
        <div className="space-y-1">
          <div className="text-xs text-gray-400">{status.progress}</div>
          <div className="h-1.5 rounded-full bg-gray-800 overflow-hidden">
            <div className="h-full bg-indigo-500 animate-pulse w-full" />
          </div>
        </div>
      )}

      {!status.running && status.progress && status.progress !== 'Idle' && (
        <div className="text-xs text-gray-400">{status.progress}</div>
      )}

      {status.error && (
        <div className="text-xs text-red-400 bg-red-950 rounded px-3 py-1.5">{status.error}</div>
      )}

      {err && (
        <div className="text-xs text-red-400 bg-red-950 rounded px-3 py-1.5">{err}</div>
      )}

      {status.rows_fetched > 0 && !status.running && (
        <div className="text-xs text-gray-500">
          {status.rows_fetched.toLocaleString()} rows fetched
        </div>
      )}
    </div>
  )
}

export default function DataSources() {
  const [adapters, setAdapters] = useState<AdapterInfo[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    fetchAdapters()
      .then(setAdapters)
      .catch(e => setError(e.message))
      .finally(() => setLoading(false))
  }, [])

  return (
    <div className="max-w-2xl mx-auto">
      <h1 className="text-2xl font-bold text-gray-100 mb-1">Data Sources</h1>
      <p className="text-sm text-gray-400 mb-6">Manage adapters and trigger data downloads.</p>

      {loading && <div className="text-gray-500 text-sm">Loading adapters…</div>}
      {error && <div className="text-red-400 text-sm">{error}</div>}

      <div className="flex flex-col gap-4">
        {adapters.map(a => (
          <AdapterCard key={a.id} adapter={a} />
        ))}
      </div>
    </div>
  )
}
