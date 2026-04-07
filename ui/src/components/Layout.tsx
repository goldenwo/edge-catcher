import { NavLink } from 'react-router-dom'
import { usePipeline } from './PipelineStatus'

const pipelineSteps = [
  { to: '/data-sources', label: 'Data Sources', step: 1, key: 'data' as const },
  { to: '/hypotheses', label: 'Hypothesize', step: 2, key: 'hypotheses' as const },
  { to: '/analyze', label: 'Analyze', step: 3, key: 'analysis' as const },
  { to: '/strategize', label: 'Strategize', step: 4, key: 'strategies' as const },
  { to: '/backtest', label: 'Backtest', step: 5, key: 'backtest' as const },
]

function StatusDot({ active }: { active: boolean }) {
  return (
    <span
      className={`inline-block w-1.5 h-1.5 rounded-full ${
        active ? 'bg-emerald-400' : 'bg-gray-600'
      }`}
    />
  )
}

export default function Layout({ children }: { children: React.ReactNode }) {
  const { status } = usePipeline()

  const isStepActive = (key: string): boolean => {
    if (!status) return false
    switch (key) {
      case 'data': return status.data.has_data
      case 'hypotheses': return status.hypotheses.count > 0
      case 'analysis': return status.analysis.count > 0
      case 'strategies': return status.strategies.count > 0
      case 'backtest': return status.backtest.count > 0
      default: return false
    }
  }

  return (
    <div className="flex h-screen bg-gray-950 text-gray-100 overflow-hidden">
      <aside className="w-52 shrink-0 flex flex-col border-r border-gray-800 py-6 px-4">
        <NavLink
          to="/"
          className="text-sm font-semibold tracking-widest text-gray-400 uppercase mb-6 hover:text-white transition-colors"
        >
          Edge Catcher
        </NavLink>

        <span className="text-[10px] font-medium tracking-wider text-gray-500 uppercase mb-2 px-3">
          Pipeline
        </span>
        <nav className="flex flex-col gap-0.5 mb-6">
          {pipelineSteps.map(({ to, label, step, key }) => (
            <NavLink
              key={to}
              to={to}
              className={({ isActive }) =>
                `flex items-center gap-2.5 px-3 py-2 rounded text-sm transition-colors ${
                  isActive
                    ? 'bg-gray-800 text-white'
                    : 'text-gray-400 hover:text-white hover:bg-gray-800/50'
                }`
              }
            >
              <span className="text-xs text-gray-500 w-4 text-right">{step}.</span>
              <span className="flex-1">{label}</span>
              <StatusDot active={isStepActive(key)} />
            </NavLink>
          ))}
        </nav>

        <span className="text-[10px] font-medium tracking-wider text-gray-500 uppercase mb-2 px-3 mt-4">
          Research
        </span>
        <nav className="flex flex-col gap-0.5 mb-6">
          <NavLink
            to="/research"
            className={({ isActive }) =>
              `flex items-center px-3 py-2 rounded text-sm transition-colors ${
                isActive
                  ? 'bg-gray-800 text-white'
                  : 'text-gray-400 hover:text-white hover:bg-gray-800/50'
              }`
            }
          >
            Research
          </NavLink>
        </nav>

        <div className="border-t border-gray-800 pt-4">
          <NavLink
            to="/settings"
            className={({ isActive }) =>
              `flex items-center px-3 py-2 rounded text-sm transition-colors ${
                isActive
                  ? 'bg-gray-800 text-white'
                  : 'text-gray-400 hover:text-white hover:bg-gray-800/50'
              }`
            }
          >
            Settings
          </NavLink>
        </div>
      </aside>
      <main className="flex-1 overflow-y-auto p-8">{children}</main>
    </div>
  )
}
