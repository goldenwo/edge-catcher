const BASE = (import.meta.env.VITE_API_URL as string | undefined) ?? ''

async function req<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${BASE}${path}`, init)
  if (!res.ok) {
    const body = await res.json().catch(() => ({ detail: res.statusText }))
    throw new Error((body as { detail?: string }).detail ?? res.statusText)
  }
  return res.json() as Promise<T>
}

const json = (body: unknown) => ({
  method: 'POST',
  headers: { 'Content-Type': 'application/json' },
  body: JSON.stringify(body),
})

export interface Status {
  markets: number
  trades: number
  results: number
  db_size_mb: number
  last_download: string | null
}

export interface DownloadStatus {
  running: boolean
  progress: string
  markets_fetched: number
  trades_fetched: number
}

export interface Hypothesis {
  id: string
  name: string
  market: string
  status: string
}

export interface ResultSummary {
  run_id: string
  hypothesis_id: string
  verdict: string | null
  run_timestamp: string
}

export interface ResultDetail extends ResultSummary {
  market: string
  status: string
  naive_n: number | null
  naive_z_stat: number | null
  naive_p_value: number | null
  naive_edge: number | null
  clustered_n: number | null
  clustered_z_stat: number | null
  clustered_p_value: number | null
  clustered_edge: number | null
  fee_adjusted_edge: number | null
  confidence_interval_low: number | null
  confidence_interval_high: number | null
  warnings: string[] | null
  total_markets_seen: number | null
  delisted_or_cancelled: number | null
  raw_bucket_data: unknown
}

export interface FormalizeResponse {
  message: string
  error: string | null
}

export interface AISettings {
  anthropic: boolean
  openai: boolean
  openrouter: boolean
}

// ── Pipeline Status ─────────────────────────────────────────────────────────

export interface PipelineStatus {
  data: { has_data: boolean; markets: number; trades: number }
  hypotheses: { count: number }
  analysis: { count: number; latest_verdict: string | null }
  strategies: { count: number; names: string[] }
  backtest: { count: number; latest_sharpe: number | null }
}

export interface StrategyInfo {
  name: string
  class_name: string
}

export interface StrategizeResponse {
  code: string
  strategy_name: string
  error: string | null
}

export interface StrategySaveResponse {
  ok: boolean
  path: string | null
  error: string | null
}

export interface BacktestStatusResp {
  running: boolean
  progress: string
  error: string | null
  trades_processed?: number | null
  trades_estimated?: number | null
  net_pnl_cents?: number | null
}

export interface BacktestHistoryItem {
  task_id: string
  series: string
  strategies: string[]
  timestamp: string
  total_trades: number
  net_pnl_cents: number
  sharpe: number
  win_rate: number
}

export const api = {
  status: () => req<Status>('/api/status'),
  startDownload: () => req<{ task_id: string }>('/api/download', { method: 'POST' }),
  downloadStatus: () => req<DownloadStatus>('/api/download/status'),
  hypotheses: () => req<Hypothesis[]>('/api/hypotheses'),
  analyze: (hypothesis_id: string | null) =>
    req<Record<string, unknown>>('/api/analyze', json({ hypothesis_id })),
  results: () => req<ResultSummary[]>('/api/results'),
  result: (run_id: string) => req<ResultDetail>(`/api/results/${run_id}`),
  formalize: (description: string, provider: string | null) =>
    req<FormalizeResponse>('/api/formalize', json({ description, provider })),
  interpret: (run_id: string, provider: string | null) =>
    req<{ summary: string }>('/api/interpret', json({ run_id, provider })),
  aiSettings: () => req<AISettings>('/api/settings/ai'),
  saveAiKey: (provider: string, api_key: string) =>
    req<{ ok: boolean }>('/api/settings/ai', json({ provider, api_key })),
  pipelineStatus: () => req<PipelineStatus>('/api/pipeline/status'),
  series: () => req<string[]>('/api/series'),
  strategies: () => req<StrategyInfo[]>('/api/strategies'),
  strategize: (hypothesis_id: string, run_id: string | null, provider: string | null) =>
    req<StrategizeResponse>('/api/strategize', json({ hypothesis_id, run_id, provider })),
  saveStrategy: (code: string, strategy_name: string) =>
    req<StrategySaveResponse>('/api/strategies/save', json({ code, strategy_name })),
  startBacktest: (params: {
    series: string; strategies: string[]; start?: string; end?: string;
    cash?: number; slippage?: number; tp?: number; sl?: number;
    min_price?: number; max_price?: number;
  }) => req<{ task_id: string }>('/api/backtest', json(params)),
  backtestStatus: (taskId: string) => req<BacktestStatusResp>(`/api/backtest/${taskId}/status`),
  stopBacktest: (taskId: string) => req<{ ok: boolean }>(`/api/backtest/${taskId}/stop`, { method: 'POST' }),
  backtestResult: (taskId: string) => req<Record<string, unknown>>(`/api/backtest/${taskId}/result`),
  backtestHistory: () => req<BacktestHistoryItem[]>('/api/backtest/history'),
}
