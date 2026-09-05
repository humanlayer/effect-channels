/** Default cadence for provider post-and-edit streaming fallbacks. */
export const streamEditIntervalMs = 500

const tableRow = /^\s*\|.*\|\s*$/
const tableSeparator = /^\s*\|[\s:]*-+[\s:]*(\|[\s:]*-+[\s:]*)*\|\s*$/

const hasOpenFence = (markdown: string) => {
	let open = false
	for (const line of markdown.split('\n')) {
		const trimmed = line.trimStart()
		if (trimmed.startsWith('```') || trimmed.startsWith('~~~')) open = !open
	}
	return open
}

/**
 * Produces a stable intermediate edit without exposing a half-written table
 * header or an unterminated fenced code block. Final output must always use
 * the original accumulated markdown instead.
 */
export const renderStreamingMarkdown = (markdown: string): string => {
	let rendered = markdown
	const lines = rendered.split('\n')
	if (!rendered.endsWith('\n') && lines.length > 0) {
		const last = lines.at(-1)
		const previous = lines.at(-2)
		if (last !== undefined && tableRow.test(last) && (previous === undefined || !tableSeparator.test(previous))) {
			lines.pop()
			rendered = lines.length === 0 ? '' : `${lines.join('\n')}\n`
		}
	}
	if (hasOpenFence(rendered)) rendered += '\n```'
	return rendered
}
