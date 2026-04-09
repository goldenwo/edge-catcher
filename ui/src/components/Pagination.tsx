interface Props {
	offset: number
	limit: number
	total: number
	onChange: (offset: number) => void
}

export default function Pagination({ offset, limit, total, onChange }: Props) {
	if (total <= limit) return null

	const page = Math.floor(offset / limit) + 1
	const totalPages = Math.ceil(total / limit)
	const from = offset + 1
	const to = Math.min(offset + limit, total)

	return (
		<div className="flex items-center justify-between text-sm text-gray-400">
			<span>
				Showing {from}–{to} of {total}
			</span>
			<div className="flex items-center gap-2">
				<button
					onClick={() => onChange(Math.max(0, offset - limit))}
					disabled={offset === 0}
					className="px-2.5 py-1 rounded bg-gray-800 hover:bg-gray-700 disabled:opacity-40 disabled:cursor-not-allowed text-xs transition-colors"
				>
					Prev
				</button>
				<span className="text-xs text-gray-500">
					{page} / {totalPages}
				</span>
				<button
					onClick={() => onChange(offset + limit)}
					disabled={offset + limit >= total}
					className="px-2.5 py-1 rounded bg-gray-800 hover:bg-gray-700 disabled:opacity-40 disabled:cursor-not-allowed text-xs transition-colors"
				>
					Next
				</button>
			</div>
		</div>
	)
}
