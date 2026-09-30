/**
 * Slack's output half: each saved operation becomes the right Slack call, or none, over a recording
 * `SlackApi`, and Slack failures come back as retryable or not.
 */
import { describe, it } from '@effect/vitest'
import {
	AddExternalLink,
	BatchId,
	DeliveryOperationId,
	DeliveryOutcome,
	ExternalLink,
	PreparedDeliveryInvocation,
	PresentOutcome,
	ProviderOutputAttempt,
	makeDeliveryId,
	type DeliveryOutputOperation,
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
	SlackOutputReceiptJson,
	SlackParticipant,
	SlackPlainTextContent,
	SlackTeamId,
	SlackThreadRef,
	SlackUserId,
	makeSlackOutputProcessor,
	slackPresentationVersion,
	slackThreadSupportedOperations,
	type SlackPostToThreadRequest,
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

const attempt = (operation: DeliveryOutputOperation, invocation = prepared()) =>
	ProviderOutputAttempt.make({
		deliveryId: makeDeliveryId({ mailboxKey: 'slack:v1:mailbox', batchId: BatchId.make('batch-1') }),
		operationId: DeliveryOperationId.make('outcome'),
		attempt: 1,
		hadAmbiguousAttempt: false,
		prepared: invocation,
		operation,
	})

/** A `SlackApi` that records every post and answers with `postedRef`, or fails with `failWith`. */
const recordingSlackApi = (posts: Ref.Ref<ReadonlyArray<SlackPostToThreadRequest>>, failWith?: string) =>
	Layer.mock(SlackApi, {
		postToThread: (request) =>
			Effect.gen(function* () {
				yield* Ref.update(posts, (all) => [...all, request])
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
	operation: DeliveryOutputOperation,
	options: { readonly failWith?: string; readonly invocation?: PreparedDeliveryInvocation } = {},
) =>
	Effect.gen(function* () {
		const posts = yield* Ref.make<ReadonlyArray<SlackPostToThreadRequest>>([])
		const processor = yield* makeSlackOutputProcessor({ namespace: 'slack-output-test' }).pipe(
			Effect.provide(recordingSlackApi(posts, options.failWith)),
		)
		const result = yield* processor.process(attempt(operation, options.invocation)).pipe(Effect.result)
		return { result, posts: yield* Ref.get(posts) }
	})

const completed = DeliveryOutcome.cases.Completed.make({})

describe('Slack delivery output', () => {
	it.effect(
		'posts a result with Markdown to the saved thread and keeps the posted message as its receipt',
		({ expect }) =>
			Effect.gen(function* () {
				const { result, posts } = yield* run(
					PresentOutcome.make({ outcome: completed, markdown: 'The fix is ready.' }),
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
			const { result, posts } = yield* run(PresentOutcome.make({ outcome: failedOutcome }))
			expect(posts).toEqual([])
			expect(Result.isSuccess(result)).toBe(true)
		}),
	)

	it.effect('lists the options of a question after its Markdown', ({ expect }) =>
		Effect.gen(function* () {
			const outcome = DeliveryOutcome.cases.AwaitingInput.make({ options: ['staging', 'production'] })
			const { posts } = yield* run(PresentOutcome.make({ outcome, markdown: 'Where should I deploy?' }))
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
			const present = PresentOutcome.make({ outcome: completed, markdown: 'done' })
			const unreachable = yield* run(present, { failWith: 'Could not reach Slack' })
			const archived = yield* run(present, { failWith: 'is_archived' })
			if (!Result.isFailure(unreachable.result) || !Result.isFailure(archived.result)) return expect.unreachable()
			expect(unreachable.result.failure).toMatchObject({ retryable: true, safeCode: 'slack_post_failed' })
			expect(archived.result.failure).toMatchObject({ retryable: false, safeCode: 'slack_post_failed' })
		}),
	)

	it.effect('refuses a destination it cannot read, or a presentation version it does not know', ({ expect }) =>
		Effect.gen(function* () {
			const present = PresentOutcome.make({ outcome: completed, markdown: 'done' })
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
})
