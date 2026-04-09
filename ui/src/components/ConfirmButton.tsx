import { useEffect, useState } from 'react'

interface Props {
	onConfirm: () => void
	label?: string
	confirmText?: string
	className?: string
}

export default function ConfirmButton({
	onConfirm,
	label = 'Delete',
	confirmText = 'Confirm?',
	className = '',
}: Props) {
	const [confirming, setConfirming] = useState(false)

	useEffect(() => {
		if (!confirming) return
		const t = setTimeout(() => setConfirming(false), 3000)
		return () => clearTimeout(t)
	}, [confirming])

	if (confirming) {
		return (
			<div className={`inline-flex items-center gap-1.5 ${className}`}>
				<button
					onClick={() => { setConfirming(false); onConfirm() }}
					className="px-2 py-1 rounded bg-red-700 hover:bg-red-600 text-xs text-white transition-colors"
				>
					{confirmText}
				</button>
				<button
					onClick={() => setConfirming(false)}
					className="px-2 py-1 rounded bg-gray-700 hover:bg-gray-600 text-xs text-gray-300 transition-colors"
				>
					Cancel
				</button>
			</div>
		)
	}

	return (
		<button
			onClick={() => setConfirming(true)}
			className={`px-2 py-1 rounded text-xs text-gray-500 hover:text-red-400 hover:bg-gray-800 transition-colors ${className}`}
			title={label}
		>
			{label}
		</button>
	)
}
