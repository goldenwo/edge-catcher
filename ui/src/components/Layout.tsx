import { NavLink } from 'react-router-dom'

const nav = [
  { to: '/', label: 'Dashboard' },
  { to: '/hypotheses', label: 'Hypotheses' },
  { to: '/results', label: 'Results' },
  { to: '/formalize', label: 'Formalize' },
  { to: '/data-sources', label: 'Data Sources' },
  { to: '/settings', label: 'Settings' },
]

export default function Layout({ children }: { children: React.ReactNode }) {
  return (
    <div className="flex h-screen bg-gray-950 text-gray-100 overflow-hidden">
      <aside className="w-48 shrink-0 flex flex-col border-r border-gray-800 py-6 px-4">
        <span className="text-sm font-semibold tracking-widest text-gray-400 uppercase mb-8">
          Edge Catcher
        </span>
        <nav className="flex flex-col gap-1">
          {nav.map(({ to, label }) => (
            <NavLink
              key={to}
              to={to}
              end={to === '/'}
              className={({ isActive }) =>
                `px-3 py-2 rounded text-sm transition-colors ${
                  isActive
                    ? 'bg-gray-800 text-white'
                    : 'text-gray-400 hover:text-white hover:bg-gray-800/50'
                }`
              }
            >
              {label}
            </NavLink>
          ))}
        </nav>
      </aside>
      <main className="flex-1 overflow-y-auto p-8">{children}</main>
    </div>
  )
}
