const BASE = (import.meta.env.VITE_API_URL as string | undefined) ?? ''

async function req<T>(path: string, init?: RequestInit): Promise<T> {
  const controller = new AbortController()
  const timeout = setTimeout(() => controller.abort(), 15_000)
  try {
    const res = await fetch(`${BASE}${path}`, { ...init, signal: controller.signal })
    if (!res.ok) {
      const body = await res.json().catch(() => ({ detail: res.statusText }))
      throw new Error((body as { detail?: string }).detail ?? res.statusText)
    }
    return res.json() as Promise<T>
  } finally {
    clearTimeout(timeout)
  }
}

// ── Simple TTL cache for GET endpoints ─────────────────────────────────────

const _cache = new Map<string, { data: unknown; ts: number; inflight?: Promise<unknown> }>()

function cachedReq<T>(path: string, ttlMs: number): Promise<T> {
  const entry = _cache.get(path)
  const now = Date.now()
  if (entry && now - entry.ts < ttlMs) return Promise.resolve(entry.data as T)
  if (entry?.inflight) return entry.inflight as Promise<T>
  const p = req<T>(path).then(data => {
    _cache.set(path, { data, ts: Date.now() })
    return data
  }).catch(err => {
    _cache.delete(path)
    throw err
  })
  _cache.set(path, { data: null, ts: 0, inflight: p })
  return p
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
  source: 'public' | 'local'
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

export interface Paginated<T> {
  results: T[]
  total: number
}

export interface StorageReport {
  databases: Record<string, { db_size_mb: number; archive_size_mb: number; total_mb: number }>
  row_counts: Record<string, number>
}

export interface ArchiveResult {
  rows_archived: number
  rows_deleted: number
  archive_file: string
}

export interface VacuumResult {
  before_mb: number
  after_mb: number
  saved_mb: number
}

export interface FormalizeResponse {
  message: string
  error: string | null
  hypothesis_id: string | null
}

export interface AISettings {
  anthropic: boolean
  openai: boolean
  openrouter: boolean
}

export interface ModelOption {
  id: string
  label: string
}

export interface ModelSettings {
  provider: string | null
  current_model: string | null
  models: ModelOption[]
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
  hypothesis_id: string | null
  timestamp: string
  total_trades: number
  net_pnl_cents: number
  sharpe: number
  win_rate: number
}

export interface FeeInfo {
  id: string
  name: string
  description: string
  formula: string
}

export const api = {
  status: () => cachedReq<Status>('/api/status', 30_000),
  startDownload: () => req<{ task_id: string }>('/api/download', { method: 'POST' }),
  downloadStatus: () => req<DownloadStatus>('/api/download/status'),
  hypotheses: () => req<Hypothesis[]>('/api/hypotheses'),
  deleteHypothesis: (id: string) =>
    req<{ ok: boolean }>(`/api/hypotheses/${encodeURIComponent(id)}`, { method: 'DELETE' }),
  resultHypothesisIds: () => req<string[]>('/api/results/hypothesis-ids'),
  deleteResult: (run_id: string) =>
    req<{ ok: boolean }>(`/api/results/${encodeURIComponent(run_id)}`, { method: 'DELETE' }),
  deleteBacktest: (task_id: string) =>
    req<{ ok: boolean }>(`/api/backtest/history/${encodeURIComponent(task_id)}`, { method: 'DELETE' }),
  analyze: (hypothesis_id: string | null) =>
    req<{ task_id: string }>('/api/analyze', json({ hypothesis_id })),
  analyzeStatus: (taskId: string) =>
    req<{ running: boolean; progress: string; error: string | null }>(`/api/analyze/${taskId}/status`),
  analyzeResult: (taskId: string) =>
    req<Record<string, unknown>>(`/api/analyze/${taskId}/result`),
  results: (limit = 25, offset = 0, filters?: { hypothesis_id?: string; verdict?: string }) => {
    const p = new URLSearchParams({ limit: String(limit), offset: String(offset) })
    if (filters?.hypothesis_id) p.set('hypothesis_id', filters.hypothesis_id)
    if (filters?.verdict) p.set('verdict', filters.verdict)
    return req<Paginated<ResultSummary>>(`/api/results?${p}`)
  },
  result: (run_id: string) => req<ResultDetail>(`/api/results/${run_id}`),
  formalize: (description: string, provider: string | null) =>
    req<FormalizeResponse>('/api/formalize', json({ description, provider })),
  interpret: (run_id: string, provider: string | null) =>
    req<{ summary: string }>('/api/interpret', json({ run_id, provider })),
  aiSettings: () => req<AISettings>('/api/settings/ai'),
  saveAiKey: (provider: string, api_key: string) =>
    req<{ ok: boolean }>('/api/settings/ai', json({ provider, api_key })),
  aiModels: () => req<ModelSettings>('/api/settings/ai/models'),
  saveAiModel: (model: string | null) =>
    req<{ ok: boolean }>('/api/settings/ai/model', json({ model })),
  pipelineStatus: () => cachedReq<PipelineStatus>('/api/pipeline/status', 30_000),
  series: () => req<string[]>('/api/series'),
  strategies: () => cachedReq<StrategyInfo[]>('/api/strategies', 60_000),
  strategize: (hypothesis_id: string, run_id: string | null, provider: string | null) =>
    req<StrategizeResponse>('/api/strategize', json({ hypothesis_id, run_id, provider })),
  saveStrategy: (code: string, strategy_name: string) =>
    req<StrategySaveResponse>('/api/strategies/save', json({ code, strategy_name })),
  startBacktest: (params: {
    series: string; strategies: string[]; hypothesis_id?: string;
    start?: string; end?: string; cash?: number; slippage?: number;
    tp?: number; sl?: number; min_price?: number; max_price?: number;
  }) => req<{ task_id: string }>('/api/backtest', json(params)),
  backtestActive: () => req<{ task_id: string | null }>('/api/backtest/active'),
  backtestStatus: (taskId: string) => req<BacktestStatusResp>(`/api/backtest/${taskId}/status`),
  stopBacktest: (taskId: string) => req<{ ok: boolean }>(`/api/backtest/${taskId}/stop`, { method: 'POST' }),
  backtestResult: (taskId: string) => req<Record<string, unknown>>(`/api/backtest/${taskId}/result`),
  backtestHistory: (limit = 25, offset = 0) =>
    req<Paginated<BacktestHistoryItem>>(`/api/backtest/history?limit=${limit}&offset=${offset}`),
  feeInfo: (series: string) => req<FeeInfo>(`/api/series/${encodeURIComponent(series)}/fee-info`),
  storageReport: () => req<StorageReport>('/api/storage/report'),
  storageArchive: (days = 90) =>
    req<Record<string, ArchiveResult>>(`/api/storage/archive?days=${days}`, { method: 'POST' }),
  storageVacuum: () => req<VacuumResult>('/api/storage/vacuum', { method: 'POST' }),
}

// ── Research Dashboard ──────────────────────────────────────────────────────

export interface ResearchLoopStatus {
  running: boolean
  phase: string
  runs_completed: number
  runs_total: number
  elapsed_seconds: number
  task_id: string | null
  error: string | null
}

export interface ResearchLoopStartRequest {
  mode: string
  max_runs: number
  max_time: number
  parallel: number
  fee_pct?: number
  start?: string
  end?: string
  max_llm_calls?: number
  force?: boolean
}

export interface VerdictCounts {
  promote: number
  review: number
  explore: number
  kill: number
}

export interface ResearchResult {
  id: string
  strategy: string
  series: string
  db_path: string
  verdict: string
  verdict_reason: string
  validation_details: unknown
  total_trades: number
  wins: number
  losses: number
  win_rate: number
  net_pnl_cents: number
  sharpe: number
  max_drawdown_pct: number
  fees_paid_cents: number
  completed_at: string
}

export interface AuditExecution {
  id: number
  hypothesis_id: string
  phase: string
  verdict: string
  status: string
  completed_at: string
}

export interface AuditDecision {
  id: number
  prompt_text: string
  response_text: string
  model: string
  token_count: number
  created_at: string
}

export const research = {
  loopStatus: () => req<ResearchLoopStatus>('/api/research/loop/status'),
  startLoop: (config: ResearchLoopStartRequest) =>
    req<{ task_id: string }>('/api/research/loop/start', json(config)),
  stopLoop: () =>
    req<{ ok: boolean }>('/api/research/loop/stop', { method: 'POST' }),
  verdictCounts: () => req<VerdictCounts>('/api/research/verdict-counts'),
  results: (limit = 50, offset = 0, sort = 'completed_at', verdict?: string) =>
    req<{ results: ResearchResult[]; total: number }>(
      `/api/research/results?limit=${limit}&offset=${offset}&sort=${sort}${verdict ? `&verdict=${verdict}` : ''}`
    ),
  reviewQueue: () =>
    req<{ strategies: ResearchResult[]; count: number }>('/api/research/review-queue'),
  approve: (id: string) =>
    req<{ ok: boolean }>(`/api/research/review/${id}/approve`, { method: 'POST' }),
  reject: (id: string, reason?: string) =>
    req<{ ok: boolean }>(`/api/research/review/${id}/reject`, json({ reason })),
  profiles: () =>
    cachedReq<{ profiles: unknown[]; count: number }>('/api/research/profiles', 120_000),
  auditExecutions: (limit = 100) =>
    req<AuditExecution[]>(`/api/research/audit/executions?limit=${limit}`),
  auditDecisions: (limit = 100) =>
    req<AuditDecision[]>(`/api/research/audit/decisions?limit=${limit}`),
}
