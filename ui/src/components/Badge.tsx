type Verdict =
  | 'EDGE_EXISTS'
  | 'INCONCLUSIVE'
  | 'NO_EDGE'
  | 'NOT_TRADEABLE'
  | 'INSUFFICIENT_DATA'
  | string
  | null

const colours: Record<string, string> = {
  EDGE_EXISTS: 'bg-green-900 text-green-300',
  INCONCLUSIVE: 'bg-yellow-900 text-yellow-300',
  NO_EDGE: 'bg-red-900 text-red-300',
  NOT_TRADEABLE: 'bg-red-900 text-red-300',
  INSUFFICIENT_DATA: 'bg-gray-700 text-gray-300',
}

export default function Badge({ verdict }: { verdict: Verdict }) {
  const cls =
    verdict
      ? (colours[verdict] ?? 'bg-gray-700 text-gray-300')
      : 'bg-gray-800 text-gray-500'
  return (
    <span className={`inline-block px-2 py-0.5 rounded text-xs font-mono ${cls}`}>
      {verdict ?? '—'}
    </span>
  )
}
