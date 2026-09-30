/**
 * Slack's output half: each saved operation becomes the right Slack call, or none, over a recording
 * `SlackApi`, and Slack failures come back as retryable or not.
 */
import { describe, it } from '@effect/vitest'
import {
	AddExternalLink,
	BatchId,
	CreateMessage,
	DeliveryOperationId,
	DeliveryOutcome,
	ExternalLink,
	MessageId,
	PreparedDeliveryInvocation,
	DeliveryActivity,
	ProviderDeleteMessage,
	ProviderPresentOutcome,
	SetActivity,
	ProviderOutputAttempt,
	ProviderUpdateMessage,
	makeDeliveryId,
	type ProviderOutputOperation,
} from '@humanlayer/channels-delivery-next'
import { Effect, Layer, Ref, Result, Schema } from 'effect'

import {
	SlackApi,
	SlackApiError,
	SlackChannelId,
	SlackDeliveryDestination,
	SlackDeliveryDestinationJson,
	SlackMessage,
	SlackMessageRef,
	SlackMessageTs,
	SlackOutputReceipt,
	SlackOutputReceiptJson,
	SlackParticipant,
	SlackPlainTextContent,
	SlackTeamId,
	SlackThreadRef,
	SlackUserId,
	makeSlackOutputProcessor,
	slackPresentationVersion,
	slackThreadSupportedOperations,
	type SlackDeleteMessageRequest,
	type SlackThreadRequest,
	type SlackThreadStatusRequest,
	type SlackPostToThreadRequest,
	type SlackUpdateMessageRequest,
} from '../src'

const teamId = SlackTeamId.make('T_OUTPUT')
const channelId = SlackChannelId.make('C_OUTPUT')
const threadTs = SlackMessageTs.make('1700000000.000001')
const thread = SlackThreadRef.make({ teamId, channelId, threadTs, isDm: false })
const postedRef = SlackMessageRef.make({ teamId, channelId, messageTs: SlackMessageTs.make('1700000009.000009') })

const prepared = (overrides: Partial<PreparedDeliveryInvocation> = {}) =>
	PreparedDeliveryInvocation.make({
		callback: 'onNewMention',
		presentationVersion: slackPresentationVersion,
		destination: Schema.encodeSync(SlackDeliveryDestinationJson)(SlackDeliveryDestination.make({ thread })),
		supportedOperations: slackThreadSupportedOperations,
		...overrides,
	})

const attempt = (operation: ProviderOutputOperation, invocation = prepared()) =>
	ProviderOutputAttempt.make({
		deliveryId: makeDeliveryId({ mailboxKey: 'slack:v1:mailbox', batchId: BatchId.make('batch-1') }),
		operationId: DeliveryOperationId.make('outcome'),
		attempt: 1,
		hadAmbiguousAttempt: false,
		idempotencyKey: '00000000-0000-4000-8000-000000000001',
		prepared: invocation,
		operation,
	})

type SlackCalls = {
	readonly statuses: ReadonlyArray<SlackThreadStatusRequest>
	readonly clears: ReadonlyArray<SlackThreadRequest>
	readonly posts: ReadonlyArray<SlackPostToThreadRequest>
	readonly updates: ReadonlyArray<SlackUpdateMessageRequest>
	readonly deletes: ReadonlyArray<SlackDeleteMessageRequest>
}

/** A `SlackApi` that records every post, edit, and deletion; posts answer with `postedRef`. Each fails with `failWith`. */
const recordingSlackApi = (calls: Ref.Ref<SlackCalls>, failWith?: string) =>
	Layer.mock(SlackApi, {
		setThreadStatus: (request) =>
			Effect.gen(function* () {
				yield* Ref.update(calls, (all) => ({ ...all, statuses: [...all.statuses, request] }))
				if (failWith !== undefined) {
					return yield* SlackApiError.make({ operation: 'set_thread_status', message: failWith })
				}
			}),
		clearThreadStatus: (request) =>
			Effect.gen(function* () {
				yield* Ref.update(calls, (all) => ({ ...all, clears: [...all.clears, request] }))
				if (failWith !== undefined) {
					return yield* SlackApiError.make({ operation: 'clear_thread_status', message: failWith })
				}
			}),
		updateMessage: (request) =>
			Effect.gen(function* () {
				yield* Ref.update(calls, (all) => ({ ...all, updates: [...all.updates, request] }))
				if (failWith !== undefined) {
					return yield* SlackApiError.make({ operation: 'update_message', message: failWith })
				}
			}),
		deleteMessage: (request) =>
			Effect.gen(function* () {
				yield* Ref.update(calls, (all) => ({ ...all, deletes: [...all.deletes, request] }))
				if (failWith !== undefined) {
					return yield* SlackApiError.make({ operation: 'delete_message', message: failWith })
				}
			}),
		postToThread: (request) =>
			Effect.gen(function* () {
				yield* Ref.update(calls, (all) => ({ ...all, posts: [...all.posts, request] }))
				if (failWith !== undefined) return yield* SlackApiError.make({ operation: 'post', message: failWith })
				return {
					ref: postedRef,
					message: SlackMessage.make({
						ref: postedRef,
						thread,
						author: SlackParticipant.make({
							userId: SlackUserId.make('U_BOT'),
							userName: 'bot',
							fullName: 'Bot',
							isBot: true,
							isMe: true,
						}),
						content: SlackPlainTextContent.make({ text: 'posted' }),
						files: [],
						metadata: {},
					}),
				}
			}),
	})

const run = (
	operation: ProviderOutputOperation,
	options: { readonly failWith?: string; readonly invocation?: PreparedDeliveryInvocation } = {},
) =>
	Effect.gen(function* () {
		const calls = yield* Ref.make<SlackCalls>({ statuses: [], clears: [], posts: [], updates: [], deletes: [] })
		const processor = yield* makeSlackOutputProcessor({ namespace: 'slack-output-test' }).pipe(
			Effect.provide(recordingSlackApi(calls, options.failWith)),
		)
		const result = yield* processor.process(attempt(operation, options.invocation)).pipe(Effect.result)
		return { result, ...(yield* Ref.get(calls)) }
	})

const messageId = MessageId.make('progress')
/** The reference a later update or deletion receives: the receipt of the message's create. */
const postedReference = Schema.encodeSync(SlackOutputReceiptJson)(SlackOutputReceipt.make({ message: postedRef }))

const completed = DeliveryOutcome.cases.Completed.make({})

describe('Slack delivery output', () => {
	it.effect(
		'posts a result with Markdown to the saved thread and keeps the posted message as its receipt',
		({ expect }) =>
			Effect.gen(function* () {
				const { result, posts } = yield* run(
					ProviderPresentOutcome.make({
						clearActivity: false,
						outcome: completed,
						markdown: 'The fix is ready.',
					}),
				)
				expect(posts).toEqual([
					{ thread, content: { _tag: 'SlackMarkdownContent', markdown: 'The fix is ready.' } },
				])
				if (!Result.isSuccess(result)) return expect.unreachable()
				const receipt = yield* Schema.decodeUnknownEffect(SlackOutputReceiptJson)(result.success.receipt)
				expect(receipt.message).toEqual(postedRef)
			}),
	)

	it.effect('posts nothing for a result without Markdown', ({ expect }) =>
		Effect.gen(function* () {
			const failedOutcome = DeliveryOutcome.cases.Failed.make({})
			const { result, posts } = yield* run(
				ProviderPresentOutcome.make({ clearActivity: false, outcome: failedOutcome }),
			)
			expect(posts).toEqual([])
			expect(Result.isSuccess(result)).toBe(true)
		}),
	)

	it.effect('lists the options of a question after its Markdown', ({ expect }) =>
		Effect.gen(function* () {
			const outcome = DeliveryOutcome.cases.AwaitingInput.make({ options: ['staging', 'production'] })
			const { posts } = yield* run(
				ProviderPresentOutcome.make({ clearActivity: false, outcome, markdown: 'Where should I deploy?' }),
			)
			expect(posts[0]?.content).toEqual({
				_tag: 'SlackMarkdownContent',
				markdown: 'Where should I deploy?\n\n• staging\n• production',
			})
		}),
	)

	it.effect('applies a link without calling Slack', ({ expect }) =>
		Effect.gen(function* () {
			const link = ExternalLink.make({ label: 'Run', url: 'https://example.com/run/1' })
			const { result, posts } = yield* run(AddExternalLink.make({ link }))
			expect(posts).toEqual([])
			expect(Result.isSuccess(result)).toBe(true)
		}),
	)

	it.effect('reports a failure Slack may get over as retryable, and one it will not as final', ({ expect }) =>
		Effect.gen(function* () {
			const present = ProviderPresentOutcome.make({ clearActivity: false, outcome: completed, markdown: 'done' })
			const unreachable = yield* run(present, { failWith: 'Could not reach Slack' })
			const archived = yield* run(present, { failWith: 'is_archived' })
			if (!Result.isFailure(unreachable.result) || !Result.isFailure(archived.result)) return expect.unreachable()
			expect(unreachable.result.failure).toMatchObject({ retryable: true, safeCode: 'slack_post_failed' })
			expect(archived.result.failure).toMatchObject({ retryable: false, safeCode: 'slack_post_failed' })
		}),
	)

	it.effect('refuses a destination it cannot read, or a presentation version it does not know', ({ expect }) =>
		Effect.gen(function* () {
			const present = ProviderPresentOutcome.make({ clearActivity: false, outcome: completed, markdown: 'done' })
			const unreadable = yield* run(present, { invocation: prepared({ destination: { thread: 'nope' } }) })
			const future = yield* run(present, {
				invocation: prepared({ presentationVersion: slackPresentationVersion + 1 }),
			})
			if (!Result.isFailure(unreadable.result) || !Result.isFailure(future.result)) return expect.unreachable()
			expect(unreadable.result.failure).toMatchObject({ retryable: false, safeCode: 'destination_invalid' })
			expect(future.result.failure).toMatchObject({
				retryable: false,
				safeCode: 'unsupported_presentation_version',
			})
			expect([...unreadable.posts, ...future.posts]).toEqual([])
		}),
	)

	it.effect('posts a message and keeps the posted message as the reference for later changes', ({ expect }) =>
		Effect.gen(function* () {
			const { result, posts } = yield* run(CreateMessage.make({ messageId, markdown: 'Running tests…' }))
			expect(posts).toEqual([{ thread, content: { _tag: 'SlackMarkdownContent', markdown: 'Running tests…' } }])
			if (!Result.isSuccess(result)) return expect.unreachable()
			expect(result.success.receipt).toEqual(postedReference)
		}),
	)

	it.effect('edits and removes the message its reference names', ({ expect }) =>
		Effect.gen(function* () {
			const updated = yield* run(
				ProviderUpdateMessage.make({ messageId, markdown: 'Tests passed.', reference: postedReference }),
			)
			expect(updated.updates).toEqual([
				{ message: postedRef, content: { _tag: 'SlackMarkdownContent', markdown: 'Tests passed.' } },
			])
			expect(Result.isSuccess(updated.result)).toBe(true)
			const deleted = yield* run(ProviderDeleteMessage.make({ messageId, reference: postedReference }))
			expect(deleted.deletes).toEqual([{ message: postedRef }])
			expect(Result.isSuccess(deleted.result)).toBe(true)
			expect([...updated.posts, ...deleted.posts]).toEqual([])
		}),
	)

	it.effect('counts a message already gone as deleted, but not as edited', ({ expect }) =>
		Effect.gen(function* () {
			const deleted = yield* run(ProviderDeleteMessage.make({ messageId, reference: postedReference }), {
				failWith: 'message_not_found',
			})
			expect(Result.isSuccess(deleted.result)).toBe(true)
			const updated = yield* run(
				ProviderUpdateMessage.make({ messageId, markdown: 'late', reference: postedReference }),
				{ failWith: 'message_not_found' },
			)
			if (!Result.isFailure(updated.result)) return expect.unreachable()
			expect(updated.result.failure).toMatchObject({ retryable: false, safeCode: 'slack_update_failed' })
		}),
	)

	it.effect('retries an edit Slack may get over, and refuses a reference it cannot read', ({ expect }) =>
		Effect.gen(function* () {
			const unreachable = yield* run(
				ProviderUpdateMessage.make({ messageId, markdown: 'x', reference: postedReference }),
				{ failWith: 'Could not reach Slack' },
			)
			if (!Result.isFailure(unreachable.result)) return expect.unreachable()
			expect(unreachable.result.failure).toMatchObject({ retryable: true, safeCode: 'slack_update_failed' })
			const unreadable = yield* run(ProviderDeleteMessage.make({ messageId, reference: { ts: 'nope' } }))
			if (!Result.isFailure(unreadable.result)) return expect.unreachable()
			expect(unreadable.result.failure).toMatchObject({ retryable: false, safeCode: 'message_reference_invalid' })
			expect(unreadable.deletes).toEqual([])
		}),
	)

	it.effect('shows Working as the thread status line, and clears it for Idle', ({ expect }) =>
		Effect.gen(function* () {
			const working = yield* run(
				SetActivity.make({ activity: DeliveryActivity.cases.Working.make({ message: 'Running tests' }) }),
			)
			expect(working.statuses).toEqual([{ thread, status: 'Running tests' }])
			expect(Result.isSuccess(working.result)).toBe(true)
			const idle = yield* run(SetActivity.make({ activity: DeliveryActivity.cases.Idle.make({}) }))
			expect(idle.clears).toEqual([{ thread }])
			expect([...idle.statuses, ...working.clears, ...working.posts]).toEqual([])
		}),
	)

	it.effect('a result clears the status line: by posting, or on its own when there is no Markdown', ({ expect }) =>
		Effect.gen(function* () {
			const silent = yield* run(ProviderPresentOutcome.make({ clearActivity: true, outcome: completed }))
			expect(silent.clears).toEqual([{ thread }])
			expect(silent.posts).toEqual([])
			const posted = yield* run(
				ProviderPresentOutcome.make({ clearActivity: true, outcome: completed, markdown: 'done' }),
			)
			expect(posted.posts).toHaveLength(1)
			expect(posted.clears).toEqual([])
			const neverWorking = yield* run(ProviderPresentOutcome.make({ clearActivity: false, outcome: completed }))
			expect([...neverWorking.clears, ...neverWorking.posts]).toEqual([])
		}),
	)

	it.effect('reports a status failure Slack may get over as retryable, and a missing scope as final', ({ expect }) =>
		Effect.gen(function* () {
			const working = SetActivity.make({ activity: DeliveryActivity.cases.Working.make({ message: 'x' }) })
			const unreachable = yield* run(working, { failWith: 'Could not reach Slack' })
			const noScope = yield* run(working, { failWith: 'missing_scope' })
			if (!Result.isFailure(unreachable.result) || !Result.isFailure(noScope.result)) return expect.unreachable()
			expect(unreachable.result.failure).toMatchObject({ retryable: true, safeCode: 'slack_status_failed' })
			expect(noScope.result.failure).toMatchObject({ retryable: false, safeCode: 'slack_status_failed' })
		}),
	)
})
