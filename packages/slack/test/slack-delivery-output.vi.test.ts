/**
 * Slack's output half: each saved operation becomes the right Slack call, or none, over a recording
 * `SlackApi`, and Slack failures come back as retryable or not.
 */
import { describe, it } from '@effect/vitest'
import {
	BatchId,
	CreateMessage,
	DeliveryOperationId,
	DeliveryOutcome,
	MessageId,
	PreparedDeliveryCallback,
	DeliveryActivity,
	ProviderDeleteMessage,
	ProviderPresentOutcome,
	ProviderReactionTarget,
	ProviderSetMessageReaction,
	SetActivity,
	ProviderOutputAttempt,
	ProviderUpdateMessage,
	makeDeliveryId,
	type ProviderOutputOperation,
} from '@humanlayer/channels-delivery'
import { Effect, Layer, Ref, Result, Schema } from 'effect'

import { commentOutputScenarios } from '../../delivery/test/comment-output-scenarios'
import {
	SlackApi,
	SlackApiError,
	SlackChannelId,
	SlackDeliveryDestination,
	SlackDeliveryDestinationJson,
	SlackMessage,
	SlackMessageRef,
	SlackMarkdownContent,
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
	SlackActivationTarget,
	SlackActivationTargetJson,
	type SlackDeleteMessageRequest,
	type SlackReactionRequest,
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

const prepared = (overrides: Partial<PreparedDeliveryCallback> = {}) =>
	PreparedDeliveryCallback.make({
		name: 'onNewMention',
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
	readonly reactions: ReadonlyArray<{ readonly change: 'add' | 'remove'; readonly request: SlackReactionRequest }>
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
		addReaction: (request) =>
			Effect.gen(function* () {
				yield* Ref.update(calls, (all) => ({
					...all,
					reactions: [...all.reactions, { change: 'add' as const, request }],
				}))
				if (failWith !== undefined)
					return yield* SlackApiError.make({ operation: 'add_reaction', message: failWith })
			}),
		removeReaction: (request) =>
			Effect.gen(function* () {
				yield* Ref.update(calls, (all) => ({
					...all,
					reactions: [...all.reactions, { change: 'remove' as const, request }],
				}))
				if (failWith !== undefined) {
					return yield* SlackApiError.make({ operation: 'remove_reaction', message: failWith })
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
	options: { readonly failWith?: string; readonly invocation?: PreparedDeliveryCallback } = {},
) =>
	Effect.gen(function* () {
		const calls = yield* Ref.make<SlackCalls>({
			statuses: [],
			clears: [],
			posts: [],
			updates: [],
			deletes: [],
			reactions: [],
		})
		const processor = yield* makeSlackOutputProcessor({ namespace: 'slack-output-test' }).pipe(
			Effect.provide(recordingSlackApi(calls, options.failWith)),
		)
		const result = yield* processor.process(attempt(operation, options.invocation)).pipe(Effect.result)
		return { result, ...(yield* Ref.get(calls)) }
	})

/** How Slack answers for each shared fault. */
const slackFaults = {
	retryable: 'Could not reach Slack',
	final: 'is_archived',
	gone: 'message_not_found',
} as const

commentOutputScenarios({
	provider: 'Slack thread',
	optionBullet: '• ',
	run: (operation, options = {}) =>
		run(operation, {
			failWith: options.fault === undefined ? undefined : slackFaults[options.fault],
			invocation:
				options.futureVersion === true
					? prepared({ presentationVersion: slackPresentationVersion + 1 })
					: undefined,
		}).pipe(
			Effect.map(({ result, posts, updates, deletes }) => ({
				result,
				shown: [
					...posts.map(
						({ content }) => `post: ${Schema.is(SlackMarkdownContent)(content) ? content.markdown : '?'}`,
					),
					...updates.map(
						({ content }) => `edit: ${Schema.is(SlackMarkdownContent)(content) ? content.markdown : '?'}`,
					),
					...deletes.map(() => 'delete'),
				],
			})),
		),
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

	describe('portable reactions', () => {
		const triggerRef = SlackMessageRef.make({
			teamId,
			channelId,
			messageTs: SlackMessageTs.make('1700000005.000005'),
		})
		const withTrigger = prepared({
			activationTarget: Schema.encodeSync(SlackActivationTargetJson)(
				SlackActivationTarget.make({ message: triggerRef }),
			),
		})
		const onActivation = ProviderReactionTarget.cases.ActivationTarget.make({})
		const react = (
			target: ProviderReactionTarget,
			reaction: ProviderSetMessageReaction['reaction'],
			active: boolean,
		) => ProviderSetMessageReaction.make({ target, reaction, active })

		it.effect("adds and removes Slack's emoji on the message that started the delivery", ({ expect }) =>
			Effect.gen(function* () {
				const added = yield* run(react(onActivation, 'thumbs_up', true), { invocation: withTrigger })
				const removed = yield* run(react(onActivation, 'hooray', false), { invocation: withTrigger })
				expect(Result.isSuccess(added.result)).toBe(true)
				expect(Result.isSuccess(removed.result)).toBe(true)
				expect([...added.reactions, ...removed.reactions]).toEqual([
					{ change: 'add', request: { message: triggerRef, reaction: 'thumbsup' } },
					{ change: 'remove', request: { message: triggerRef, reaction: 'tada' } },
				])
			}),
		)

		it.effect('reacts on a message the delivery posted, through its receipt', ({ expect }) =>
			Effect.gen(function* () {
				const { result, reactions } = yield* run(
					react(
						ProviderReactionTarget.cases.MessageTarget.make({ messageId, reference: postedReference }),
						'eyes',
						true,
					),
				)
				expect(Result.isSuccess(result)).toBe(true)
				expect(reactions).toEqual([{ change: 'add', request: { message: postedRef, reaction: 'eyes' } }])
			}),
		)

		it.effect('counts a reaction already there, or already gone, as done', ({ expect }) =>
			Effect.gen(function* () {
				const again = yield* run(react(onActivation, 'eyes', true), {
					invocation: withTrigger,
					failWith: 'already_reacted',
				})
				const gone = yield* run(react(onActivation, 'eyes', false), {
					invocation: withTrigger,
					failWith: 'no_reaction',
				})
				const messageGone = yield* run(react(onActivation, 'eyes', false), {
					invocation: withTrigger,
					failWith: 'message_not_found',
				})
				expect([again, gone, messageGone].map(({ result }) => Result.isSuccess(result))).toEqual([
					true,
					true,
					true,
				])
			}),
		)

		it.effect('sorts other failures as retryable or final, and fails without an activation target', ({ expect }) =>
			Effect.gen(function* () {
				const unreachable = yield* run(react(onActivation, 'eyes', true), {
					invocation: withTrigger,
					failWith: 'Could not reach Slack',
				})
				const archived = yield* run(react(onActivation, 'eyes', true), {
					invocation: withTrigger,
					failWith: 'is_archived',
				})
				const noTarget = yield* run(react(onActivation, 'eyes', true))
				if (
					!Result.isFailure(unreachable.result) ||
					!Result.isFailure(archived.result) ||
					!Result.isFailure(noTarget.result)
				) {
					return expect.unreachable()
				}
				expect(unreachable.result.failure).toMatchObject({ retryable: true, safeCode: 'slack_reaction_failed' })
				expect(archived.result.failure).toMatchObject({ retryable: false, safeCode: 'slack_reaction_failed' })
				expect(noTarget.result.failure).toMatchObject({
					retryable: false,
					safeCode: 'activation_target_missing',
				})
				expect(noTarget.reactions).toEqual([])
			}),
		)
	})
})
