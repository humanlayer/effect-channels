import { OutputStore } from '@humanlayer/fold-agent'
import { CurrentToolCall, truncateTail } from '@humanlayer/fold-core'
import { Effect } from 'effect'

/**
 * Long tool output as the model sees it: its tail, and when that is not all of it, where the whole output
 * is saved in the workspace. Saving is best-effort; the notice says when it failed. `tool` names the tool
 * in the logs.
 */
export const visibleOutput = (text: string, tool: string) =>
	Effect.gen(function* () {
		const truncation = truncateTail(text)
		if (!truncation.truncated) return text
		const outputStore = yield* OutputStore
		const { toolCallId } = yield* CurrentToolCall
		const saved = yield* outputStore.append(toolCallId, text).pipe(
			Effect.tap((ref) =>
				Effect.logInfo(`${tool}.output_saved`).pipe(
					Effect.annotateLogs({ path: ref.path, lines: truncation.totalLines, bytes: text.length }),
				),
			),
			Effect.map((ref) => `Full output: ${ref.path}`),
			Effect.catch((error) =>
				Effect.logWarning(`${tool}.output_save failed`, error).pipe(
					Effect.as('The full output could not be saved'),
				),
			),
		)
		const start = truncation.totalLines - truncation.outputLines + 1
		return `${truncation.content}\n\n[Showing lines ${start}-${truncation.totalLines} of ${truncation.totalLines}. ${saved}]`
	})
