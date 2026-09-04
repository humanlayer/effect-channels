import { FileUpload, MarkdownContent, PlainTextContent, type Thread } from '@humanlayer/channels'
import { Effect } from 'effect'

const encoder = new TextEncoder()

/** Runs the optional credentialed Slack file smoke checks for an existing thread. */
export const runLiveFileChecks = Effect.fn('example.slack.live_files')(function* (thread: Thread) {
	const textAndFile = yield* thread.post(
		MarkdownContent.make({
			markdown: 'Phase 6 live check: text plus file',
			files: [
				FileUpload.make({
					filename: 'channels-phase-6.txt',
					mimeType: 'text/plain',
					data: encoder.encode('text-plus-file'),
				}),
			],
		}),
	)
	const attachment = textAndFile.message.attachments.at(0)
	if (attachment === undefined) {
		return yield* Effect.die(new Error('Slack did not return the uploaded attachment'))
	}
	const downloaded = yield* attachment.download()
	if (new TextDecoder().decode(downloaded) !== 'text-plus-file') {
		return yield* Effect.die(new Error('Slack attachment download did not match the uploaded bytes'))
	}
	yield* thread.post(
		MarkdownContent.make({
			markdown: 'Phase 6 live check: multiple files',
			files: [
				FileUpload.make({ filename: 'channels-one.txt', data: encoder.encode('one') }),
				FileUpload.make({ filename: 'channels-two.txt', data: encoder.encode('two') }),
			],
		}),
	)
	yield* thread.post(
		PlainTextContent.make({
			text: '',
			files: [FileUpload.make({ filename: 'channels-file-only.txt', data: encoder.encode('file-only') })],
		}),
	)
})
