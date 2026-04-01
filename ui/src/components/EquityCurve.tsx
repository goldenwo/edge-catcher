const WIDTH = 800
const HEIGHT = 200
const PADDING = { top: 20, right: 20, bottom: 30, left: 60 }

interface EquityCurveProps {
  data: [string, number][]
}

export default function EquityCurve({ data }: EquityCurveProps) {
  if (!data || data.length === 0) {
    return (
      <div className="text-sm text-gray-500 text-center py-8">
        No equity curve data available.
      </div>
    )
  }

  if (data.length === 1) {
    const dollarVal = (data[0][1] / 100).toFixed(0)
    return (
      <div className="text-sm text-gray-400 text-center py-8">
        Single data point: ${dollarVal} on {data[0][0]}
      </div>
    )
  }

  const values = data.map((d) => d[1])
  const minY = Math.min(...values)
  const maxY = Math.max(...values)
  const rangeY = maxY - minY || 1 // avoid division by zero if all values equal

  const plotW = WIDTH - PADDING.left - PADDING.right
  const plotH = HEIGHT - PADDING.top - PADDING.bottom

  const points = data.map((d, i) => {
    const x = PADDING.left + (i / (data.length - 1)) * plotW
    const y = PADDING.top + (1 - (d[1] - minY) / rangeY) * plotH
    return `${x},${y}`
  })

  const pointsStr = points.join(' ')

  const midY = (minY + maxY) / 2
  const fmtDollar = (val: number) => `$${(val / 100).toFixed(0)}`

  const firstDate = data[0][0]
  const lastDate = data[data.length - 1][0]

  return (
    <svg
      viewBox={`0 0 ${WIDTH} ${HEIGHT}`}
      className="w-full"
      preserveAspectRatio="xMidYMid meet"
    >
      {/* Polyline */}
      <polyline
        points={pointsStr}
        fill="none"
        stroke="#34d399"
        strokeWidth="1.5"
      />

      {/* Y-axis labels */}
      <text
        x={PADDING.left - 8}
        y={PADDING.top}
        textAnchor="end"
        className="text-xs fill-gray-500"
        dominantBaseline="middle"
        fontSize="11"
      >
        {fmtDollar(maxY)}
      </text>
      <text
        x={PADDING.left - 8}
        y={PADDING.top + plotH / 2}
        textAnchor="end"
        className="text-xs fill-gray-500"
        dominantBaseline="middle"
        fontSize="11"
      >
        {fmtDollar(midY)}
      </text>
      <text
        x={PADDING.left - 8}
        y={PADDING.top + plotH}
        textAnchor="end"
        className="text-xs fill-gray-500"
        dominantBaseline="middle"
        fontSize="11"
      >
        {fmtDollar(minY)}
      </text>

      {/* X-axis labels */}
      <text
        x={PADDING.left}
        y={HEIGHT - 6}
        textAnchor="start"
        className="text-xs fill-gray-500"
        fontSize="11"
      >
        {firstDate}
      </text>
      <text
        x={WIDTH - PADDING.right}
        y={HEIGHT - 6}
        textAnchor="end"
        className="text-xs fill-gray-500"
        fontSize="11"
      >
        {lastDate}
      </text>
    </svg>
  )
}
