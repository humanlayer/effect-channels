/**
 * Output scenarios every destination with lasting, editable messages must pass: a Slack thread, a
 * Linear issue, and a GitHub issue or pull request. Each provider test supplies a harness that runs one
 * operation through its real output processor over a fake provider API, and reports what the provider
 * was asked to show in a neutral form:
 *
 * - `post: <markdown>` for a new message or comment
 * - `edit: <markdown>` for a change to one
 * - `delete` for its removal
 *
 * Provider-specific behavior, such as activity or the exact API request, stays in each provider's tests.
 */
import { describe, it } from '@effect/vitest'
import { Effect, Result } from 'effect'

import {
	AddExternalLink,
	CreateMessage,
	DeliveryOutcome,
	ExternalLink,
	MessageId,
	ProviderDeleteMessage,
	ProviderPresentOutcome,
	ProviderUpdateMessage,
	type DeliveryOutputApplied,
	type DeliveryOutputFailed,
	type ProviderOutputOperation,
} from '../src'

/**
 * How the fake provider answers every call of one attempt.
 *
 * @property retryable - a failure another attempt may get past, such as an outage
 * @property final - a failure another attempt cannot fix, such as a refused permission
 * @property gone - the message or comment no longer exists
 */
export type CommentOutputFault = 'retryable' | 'final' | 'gone'

export type CommentOutputRun = {
	readonly result: Result.Result<DeliveryOutputApplied, DeliveryOutputFailed>
	/** What the provider was asked to show, in order. */
	readonly shown: ReadonlyArray<string>
}

export type CommentOutputHarness = {
	/** The provider's name, for test titles. */
	readonly provider: string
	/** How the provider lists a question's options after its Markdown, such as `- ` or `• `. */
	readonly optionBullet: string
	/**
	 * Run one operation through the provider's output processor.
	 *
	 * @param options.fault - how the fake provider answers every call
	 * @param options.futureVersion - save the destination under a presentation version the provider does not know
	 */
	readonly run: (
		operation: ProviderOutputOperation,
		options?: { readonly fault?: CommentOutputFault; readonly futureVersion?: boolean },
	) => Effect.Effect<CommentOutputRun>
}

const completed = DeliveryOutcome.cases.Completed.make({})
const messageId = MessageId.make('progress')

/** The receipt of a successful run, which a later update or deletion receives as its reference. */
const receiptOf = (run: CommentOutputRun) =>
	Result.match(run.result, {
		onSuccess: (applied) => applied.receipt,
		onFailure: () => undefined,
	})

export const commentOutputScenarios = (harness: CommentOutputHarness) =>
	describe(`${harness.provider} output: shared comment scenarios`, () => {
		it.effect('posts a result with Markdown, lists a question’s options, and makes no call without Markdown', ({ expect }) =>
			Effect.gen(function* () {
				const posted = yield* harness.run(
					ProviderPresentOutcome.make({ clearActivity: false, outcome: completed, markdown: 'The fix is ready.' }),
				)
				expect(Result.isSuccess(posted.result)).toBe(true)
				expect(posted.shown).toEqual(['post: The fix is ready.'])

				const asked = yield* harness.run(
					ProviderPresentOutcome.make({
						clearActivity: false,
						outcome: DeliveryOutcome.cases.AwaitingInput.make({ options: ['staging', 'production'] }),
						markdown: 'Where to?',
					}),
				)
				expect(asked.shown).toEqual([
					`post: Where to?\n\n${harness.optionBullet}staging\n${harness.optionBullet}production`,
				])

				for (const outcome of [completed, DeliveryOutcome.cases.Failed.make({})]) {
					const silent = yield* harness.run(ProviderPresentOutcome.make({ clearActivity: false, outcome }))
					expect(Result.isSuccess(silent.result)).toBe(true)
					expect(silent.shown).toEqual([])
				}
			}),
		)

		it.effect('creates a message, then edits and removes it through the receipt of its create', ({ expect }) =>
			Effect.gen(function* () {
				const created = yield* harness.run(CreateMessage.make({ messageId, markdown: 'Running tests…' }))
				expect(created.shown).toEqual(['post: Running tests…'])
				const reference = receiptOf(created)
				if (reference === undefined) return expect.unreachable()

				const edited = yield* harness.run(ProviderUpdateMessage.make({ messageId, markdown: 'Tests passed.', reference }))
				expect(Result.isSuccess(edited.result)).toBe(true)
				expect(edited.shown).toEqual(['edit: Tests passed.'])

				const removed = yield* harness.run(ProviderDeleteMessage.make({ messageId, reference }))
				expect(Result.isSuccess(removed.result)).toBe(true)
				expect(removed.shown).toEqual(['delete'])
			}),
		)

		it.effect('counts a message already gone as removed, but not as edited', ({ expect }) =>
			Effect.gen(function* () {
				const reference = receiptOf(yield* harness.run(CreateMessage.make({ messageId, markdown: 'x' })))
				if (reference === undefined) return expect.unreachable()
				const removed = yield* harness.run(ProviderDeleteMessage.make({ messageId, reference }), { fault: 'gone' })
				expect(Result.isSuccess(removed.result)).toBe(true)
				const edited = yield* harness.run(ProviderUpdateMessage.make({ messageId, markdown: 'late', reference }), {
					fault: 'gone',
				})
				if (!Result.isFailure(edited.result)) return expect.unreachable()
				expect(edited.result.failure.retryable).toBe(false)
			}),
		)

		it.effect('reports a failure another attempt may get past as retryable, and others as final', ({ expect }) =>
			Effect.gen(function* () {
				const create = CreateMessage.make({ messageId, markdown: 'x' })
				const outage = yield* harness.run(create, { fault: 'retryable' })
				const refused = yield* harness.run(create, { fault: 'final' })
				if (!Result.isFailure(outage.result) || !Result.isFailure(refused.result)) return expect.unreachable()
				expect(outage.result.failure).toMatchObject({ _tag: 'DeliveryOutputFailed', retryable: true })
				expect(refused.result.failure).toMatchObject({ _tag: 'DeliveryOutputFailed', retryable: false })
			}),
		)

		it.effect('refuses a reference it cannot read, and a presentation version it does not know, without a call', ({ expect }) =>
			Effect.gen(function* () {
				const unreadable = yield* harness.run(ProviderDeleteMessage.make({ messageId, reference: { something: 'else' } }))
				if (!Result.isFailure(unreadable.result)) return expect.unreachable()
				expect(unreadable.result.failure).toMatchObject({ retryable: false, safeCode: 'message_reference_invalid' })
				const future = yield* harness.run(CreateMessage.make({ messageId, markdown: 'x' }), { futureVersion: true })
				if (!Result.isFailure(future.result)) return expect.unreachable()
				expect(future.result.failure).toMatchObject({ retryable: false, safeCode: 'unsupported_presentation_version' })
				expect([...unreadable.shown, ...future.shown]).toEqual([])
			}),
		)

		it.effect('applies a link without a call', ({ expect }) =>
			Effect.gen(function* () {
				const link = ExternalLink.make({ label: 'Run log', url: 'https://example.com/run/1' })
				const linked = yield* harness.run(AddExternalLink.make({ link }))
				expect(Result.isSuccess(linked.result)).toBe(true)
				expect(linked.shown).toEqual([])
			}),
		)
	})
