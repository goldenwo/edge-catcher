import { createContext, useContext, useEffect, useState, useCallback } from 'react'
import { api, PipelineStatus } from '../api'

interface PipelineCtx {
  status: PipelineStatus | null
  loading: boolean
  refresh: () => void
}

const PipelineContext = createContext<PipelineCtx>({
  status: null,
  loading: true,
  refresh: () => {},
})

export function PipelineProvider({ children }: { children: React.ReactNode }) {
  const [status, setStatus] = useState<PipelineStatus | null>(null)
  const [loading, setLoading] = useState(true)

  const refresh = useCallback(async () => {
    try {
      setStatus(await api.pipelineStatus())
    } catch {
      // Silently fail — sidebar dots just stay gray
    } finally {
      setLoading(false)
    }
  }, [])

  useEffect(() => { refresh() }, [refresh])

  return (
    <PipelineContext.Provider value={{ status, loading, refresh }}>
      {children}
    </PipelineContext.Provider>
  )
}

export function usePipeline() {
  return useContext(PipelineContext)
}
