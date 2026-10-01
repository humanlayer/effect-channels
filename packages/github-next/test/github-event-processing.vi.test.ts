import { describe, it } from '@effect/vitest'
import {
	DeliveryAdmission,
	deliveryMailboxKey,
	type DeliveryOperationKind,
	DeliveryPreparationConflict,
	DeliveryPreparationUnavailable,
	MailboxSubscriptions,
	MailboxSubscriptionsMemory,
	PreparedDeliveryInvocation,
	ProviderDeliveryExecution,
	ProviderEventExecutionFailed,
	ProviderEventHandled,
	ProviderEventIgnored,
	ProviderEventInvalid,
} from '@humanlayer/channels-delivery-next'
import { Cause, Effect, Exit, Layer, Option, Ref } from 'effect'
import { vi } from 'vite-plus/test'

import { makeTestDeliveryExecution } from '../../delivery-next/test/delivery-execution'
import { GitHubApi } from '../src/GitHubApi'
import type { GitHubMentioned } from '../src/GitHubCallbackEvents'
import { GitHubCallbacks, type GitHubCallbackHandler, type GitHubCallbackHandlers } from '../src/GitHubCallbacks'
import { GitHubBotConfiguration, makeGitHubEventProcessor } from '../src/GitHubEventProcessor'
import { GitHubId } from '../src/GitHubIdentity'
import {
	checkRunPayload,
	issueCommentPayload,
	issuePayload,
	pullRequestIssueCommentPayload,
	pullRequestPayload,
} from './fixtures'

type Handler<K extends keyof GitHubCallbackHandlers<never, never>> = NonNullable<
	GitHubCallbackHandlers<never, never>[K]
>

const namespace = 'github-processing-test'
const bot = GitHubBotConfiguration.make({ mentionNames: ['agent'], botUserId: GitHubId.make(999) })

const admission = (
	event: string,
	payload: ReturnType<typeof issuePayload> | ReturnType<typeof issueCommentPayload>,
	eventId: string,
): DeliveryAdmission =>
	DeliveryAdmission.make({
		namespace,
		provider: 'github',
		installationId: '100',
		resourceId: `github:v1:200:issue:${payload.issue.number}`,
		eventId,
		payload: { event, payload },
	})

const pullRequestAdmission = (
	event: string,
	payload:
		| ReturnType<typeof pullRequestPayload>
		| ReturnType<typeof pullRequestIssueCommentPayload>
		| ReturnType<typeof checkRunPayload>,
	eventId: string,
	number = 42,
): DeliveryAdmission =>
	DeliveryAdmission.make({
		namespace,
		provider: 'github',
		installationId: '100',
		resourceId: `github:v1:200:pull-request:${number}`,
		eventId,
		payload: { event, payload },
	})

const layer = <E, R>(handlers: GitHubCallbackHandlers<E, R>, subscribed: boolean) =>
	Layer.mergeAll(
		GitHubCallbacks.layer(handlers),
		Layer.mock(GitHubApi, {}),
		Layer.mock(MailboxSubscriptions, { isSubscribed: () => Effect.succeed(subscribed) }),
	)

const process = <E, R>(
	handlers: GitHubCallbackHandlers<E, R>,
	admissions: readonly [DeliveryAdmission, ...Array<DeliveryAdmission>],
	subscribed = false,
) =>
	Effect.flatMap(makeTestDeliveryExecution(), ({ execution }) =>
		makeGitHubEventProcessor({ namespace, bot }).process(admissions, execution),
	).pipe(Effect.provide(layer(handlers, subscribed)))

describe('GitHub event batch processing', () => {
	it.effect('routes an opened issue to onIssueCreated', ({ expect }) =>
		Effect.gen(function* () {
			const onIssueCreated = vi.fn<Handler<'onIssueCreated'>>(() => Effect.void)
			expect(yield* process({ onIssueCreated }, [admission('issues', issuePayload('opened'), 'opened')])).toEqual(
				ProviderEventHandled.make({}),
			)
			expect(onIssueCreated).toHaveBeenCalledOnce()
			expect(onIssueCreated.mock.calls[0]?.[0].trigger._tag).toBe('GitHubIssueOpened')
		}),
	)

	it.effect('lets a later mention beat opened and includes opened as a trailing event', ({ expect }) =>
		Effect.gen(function* () {
			const onIssueCreated = vi.fn<Handler<'onIssueCreated'>>(() => Effect.void)
			const onMentioned = vi.fn<Handler<'onMentioned'>>(() => Effect.void)
			const mention = issueCommentPayload()
			expect(
				yield* process({ onIssueCreated, onMentioned }, [
					admission('issues', issuePayload('opened'), 'opened'),
					admission('issue_comment', mention, 'mention'),
				]),
			).toEqual(ProviderEventHandled.make({}))
			expect(onIssueCreated).not.toHaveBeenCalled()
			expect(onMentioned).toHaveBeenCalledOnce()
			expect(onMentioned.mock.calls[0]?.[0].trigger._tag).toBe('GitHubIssueCommentCreated')
			expect(onMentioned.mock.calls[0]?.[0].events.map((event) => event._tag)).toEqual(['GitHubIssueOpened'])
		}),
	)

	it.effect('delivers subscribed pull request events once and in mailbox order', ({ expect }) =>
		Effect.gen(function* () {
			const onSubscribedPrEvents = vi.fn<Handler<'onSubscribedPrEvents'>>(() => Effect.void)
			expect(
				yield* process(
					{ onSubscribedPrEvents },
					[
						pullRequestAdmission('pull_request', pullRequestPayload('edited'), 'edited'),
						pullRequestAdmission('issue_comment', pullRequestIssueCommentPayload(), 'comment'),
						pullRequestAdmission('pull_request', pullRequestPayload('closed'), 'closed'),
					],
					true,
				),
			).toEqual(ProviderEventHandled.make({}))
			expect(onSubscribedPrEvents).toHaveBeenCalledOnce()
			expect(onSubscribedPrEvents.mock.calls[0]?.[0].events.map((event) => event._tag)).toEqual([
				'GitHubPrEdited',
				'GitHubPrCommentCreated',
				'GitHubPrClosed',
			])
		}),
	)

	it.effect('normalizes completed checks into subscribed pull request events', ({ expect }) =>
		Effect.gen(function* () {
			const onSubscribedPrEvents = vi.fn<Handler<'onSubscribedPrEvents'>>(() => Effect.void)
			expect(
				yield* process(
					{ onSubscribedPrEvents },
					[pullRequestAdmission('check_run', checkRunPayload([42]), 'check:pull-request:200:42')],
					true,
				),
			).toEqual(ProviderEventHandled.make({}))
			expect(onSubscribedPrEvents.mock.calls[0]?.[0].events[0]).toEqual(
				expect.objectContaining({
					_tag: 'GitHubPrCheckCompleted',
					checkRunId: 900,
					name: 'build',
					conclusion: 'success',
					headSha: 'abc123',
				}),
			)
		}),
	)

	it.effect('normalizes a fanned-out check for the admission mailbox association', ({ expect }) =>
		Effect.gen(function* () {
			const onSubscribedPrEvents = vi.fn<Handler<'onSubscribedPrEvents'>>(() => Effect.void)
			yield* process(
				{ onSubscribedPrEvents },
				[pullRequestAdmission('check_run', checkRunPayload([42, 57]), 'check:pull-request:200:57', 57)],
				true,
			)
			const event = onSubscribedPrEvents.mock.calls[0]?.[0]
			expect(event?.pullRequest.ref.number).toBe(57)
			expect(event?.events[0]?._tag).toBe('GitHubPrCheckCompleted')
		}),
	)

	it.effect('does not suppress completed checks sent by the configured bot identity', ({ expect }) =>
		Effect.gen(function* () {
			const onSubscribedPrEvents = vi.fn<Handler<'onSubscribedPrEvents'>>(() => Effect.void)
			const payload = checkRunPayload([42])
			payload.sender.id = 999
			expect(
				yield* process(
					{ onSubscribedPrEvents },
					[pullRequestAdmission('check_run', payload, 'bot-check')],
					true,
				),
			).toEqual(ProviderEventHandled.make({}))
			expect(onSubscribedPrEvents).toHaveBeenCalledOnce()
		}),
	)

	it.effect('suppresses bot-authored events before routing', ({ expect }) =>
		Effect.gen(function* () {
			const onMentioned = vi.fn<Handler<'onMentioned'>>(() => Effect.void)
			const payload = issueCommentPayload()
			payload.sender.id = 999
			payload.comment.user.id = 999
			expect(yield* process({ onMentioned }, [admission('issue_comment', payload, 'self')])).toEqual(
				ProviderEventIgnored.make({ reason: 'no_relevant_event' }),
			)
			expect(onMentioned).not.toHaveBeenCalled()
		}),
	)

	it.effect('rejects a payload whose repository identity disagrees with its mailbox', ({ expect }) =>
		Effect.gen(function* () {
			const payload = issuePayload('opened')
			const mismatched = DeliveryAdmission.make({
				...admission('issues', payload, 'opened'),
				resourceId: 'github:v1:201:issue:42',
			})
			const error = yield* Effect.flip(process<never, never>({}, [mismatched]))
			expect(error).toEqual(ProviderEventInvalid.make({ provider: 'github', reason: 'identity_mismatch' }))
		}),
	)

	it.effect('narrows callback retryability after the callback layer observes the failure', ({ expect }) =>
		Effect.gen(function* () {
			const error = yield* Effect.flip(
				process({ onIssueCreated: () => Effect.fail({ retryability: 'non_retryable' as const }) }, [
					admission('issues', issuePayload('opened'), 'opened'),
				]),
			)
			expect(error).toEqual(
				ProviderEventExecutionFailed.make({
					provider: 'github',
					retryable: false,
					safeCode: 'callback_failed',
				}),
			)
		}),
	)

	it.effect('preserves callback interruption', ({ expect }) =>
		Effect.gen(function* () {
			const exit = yield* process({ onIssueCreated: () => Effect.interrupt }, [
				admission('issues', issuePayload('opened'), 'opened'),
			]).pipe(Effect.exit)
			expect(Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause)).toBe(true)
		}),
	)
})

const processor = makeGitHubEventProcessor({ namespace, bot })
const mentionAdmission = () => admission('issue_comment', issueCommentPayload(), 'mention')
const openedAdmission = () => admission('issues', issuePayload('opened'), 'opened')

/** Callbacks and the processor share one in-memory subscription store, so a callback can subscribe the issue. */
const sharedLayer = <E, R>(handlers: GitHubCallbackHandlers<E, R>) =>
	Layer.merge(
		Layer.provideMerge(GitHubCallbacks.layer(handlers), MailboxSubscriptionsMemory),
		Layer.mock(GitHubApi, {}),
	)

/** Another test mutates the fixtures' shared user into the bot, so pull request payloads name their own sender. */
const pullRequestFrom = (action: string) => ({
	...pullRequestPayload(action),
	sender: { id: 402, login: 'carol', type: 'User' },
})

const issueRef = { installationId: 100, repositoryId: 200, owner: 'alice', repository: 'project', number: 42 }
/** What a subscribed batch supports: no activation target, so no `SetActivity`. */
const discussionSupportedOperations: ReadonlyArray<DeliveryOperationKind> = [
	'PresentOutcome',
	'CreateMessage',
	'UpdateMessage',
	'DeleteMessage',
	'SetMessageReaction',
	'RenderPlan',
	'AddExternalLink',
]
/** What a delivery with an activation target supports: `SetActivity` shows as `eyes` on it. */
const activatedSupportedOperations: ReadonlyArray<DeliveryOperationKind> = [
	'PresentOutcome',
	'CreateMessage',
	'UpdateMessage',
	'DeleteMessage',
	'SetMessageReaction',
	'SetActivity',
	'RenderPlan',
	'AddExternalLink',
]

describe('GitHub delivery preparation', () => {
	it.effect('prepares the callback, issue, and mentioning comment once before the callback runs', ({ expect }) => {
		const preparedBeforeCallback: Array<number> = []
		const deliveryIds: Array<string> = []
		return Effect.gen(function* () {
			const test = yield* makeTestDeliveryExecution()
			const onMentioned: Handler<'onMentioned'> = (_event, delivery) =>
				Ref.get(test.preparations).pipe(
					Effect.map((preparations) => {
						preparedBeforeCallback.push(preparations.length)
						deliveryIds.push(delivery.deliveryId)
					}),
				)
			const result = yield* processor
				.process([mentionAdmission()], test.execution)
				.pipe(Effect.provide(sharedLayer({ onMentioned })))

			expect(result).toEqual(ProviderEventHandled.make({}))
			expect(preparedBeforeCallback).toEqual([1])
			expect(deliveryIds).toEqual([test.execution.deliveryId])
			expect(yield* Ref.get(test.preparations)).toEqual([
				PreparedDeliveryInvocation.make({
					callback: 'onMentioned',
					presentationVersion: 1,
					destination: { _tag: 'GitHubIssue', issue: issueRef },
					activationTarget: {
						_tag: 'GitHubIssueComment',
						comment: { discussion: { _tag: 'Issue', ref: issueRef }, id: 500 },
					},
					supportedOperations: activatedSupportedOperations,
					reactionTargets: ['ActivationTarget', 'MessageTarget'],
				}),
			])
		})
	})

	it.effect('targets the opened issue or pull request itself, and nothing for subscribed batches', ({ expect }) =>
		Effect.gen(function* () {
			const issue = yield* makeTestDeliveryExecution()
			yield* processor
				.process([openedAdmission()], issue.execution)
				.pipe(Effect.provide(sharedLayer({ onIssueCreated: () => Effect.void })))
			expect(yield* Ref.get(issue.preparations)).toEqual([
				PreparedDeliveryInvocation.make({
					callback: 'onIssueCreated',
					presentationVersion: 1,
					destination: { _tag: 'GitHubIssue', issue: issueRef },
					activationTarget: { _tag: 'GitHubIssue', issue: issueRef },
					supportedOperations: activatedSupportedOperations,
					reactionTargets: ['ActivationTarget', 'MessageTarget'],
				}),
			])

			const pullRequest = yield* makeTestDeliveryExecution()
			yield* processor
				.process(
					[pullRequestAdmission('pull_request', pullRequestFrom('opened'), 'opened')],
					pullRequest.execution,
				)
				.pipe(Effect.provide(sharedLayer({ onPrCreated: () => Effect.void })))
			expect(yield* Ref.get(pullRequest.preparations)).toEqual([
				PreparedDeliveryInvocation.make({
					callback: 'onPrCreated',
					presentationVersion: 1,
					destination: { _tag: 'GitHubPullRequest', pullRequest: issueRef },
					activationTarget: { _tag: 'GitHubPullRequest', pullRequest: issueRef },
					supportedOperations: activatedSupportedOperations,
					reactionTargets: ['ActivationTarget', 'MessageTarget'],
				}),
			])

			const subscribed = yield* makeTestDeliveryExecution()
			yield* processor
				.process(
					[pullRequestAdmission('pull_request', pullRequestFrom('closed'), 'closed')],
					subscribed.execution,
				)
				.pipe(Effect.provide(layer({ onSubscribedPrEvents: () => Effect.void }, true)))
			expect(yield* Ref.get(subscribed.preparations)).toEqual([
				PreparedDeliveryInvocation.make({
					callback: 'onSubscribedPrEvents',
					presentationVersion: 1,
					destination: { _tag: 'GitHubPullRequest', pullRequest: issueRef },
					supportedOperations: discussionSupportedOperations,
					reactionTargets: ['MessageTarget'],
				}),
			])
		}),
	)

	it.effect('does not prepare a batch it ignores', ({ expect }) =>
		Effect.gen(function* () {
			const test = yield* makeTestDeliveryExecution()
			const result = yield* processor
				.process([openedAdmission()], test.execution)
				.pipe(Effect.provide(sharedLayer({ onMentioned: () => Effect.void })))
			expect(result).toEqual(ProviderEventIgnored.make({ reason: 'callback_not_configured' }))
			expect(yield* Ref.get(test.preparations)).toEqual([])
		}),
	)

	it.effect('retries the saved callback after the first attempt subscribed the issue', ({ expect }) => {
		const calls: Array<string> = []
		const onMentioned: GitHubCallbackHandler<
			GitHubMentioned,
			{ readonly retryable: boolean },
			MailboxSubscriptions
		> = () =>
			Effect.suspend(() => {
				calls.push('onMentioned')
				return calls.length === 1
					? Effect.flatMap(MailboxSubscriptions, (subscriptions) =>
							subscriptions.subscribe({ mailboxKey: deliveryMailboxKey(mentionAdmission()) }),
						).pipe(Effect.orDie, Effect.andThen(Effect.fail({ retryable: true })))
					: Effect.void
			})
		const onSubscribedIssueEvents = () =>
			Effect.sync(() => {
				calls.push('onSubscribedIssueEvents')
			})
		return Effect.gen(function* () {
			const first = yield* makeTestDeliveryExecution()
			expect(yield* processor.process([mentionAdmission()], first.execution).pipe(Effect.flip)).toEqual(
				ProviderEventExecutionFailed.make({ provider: 'github', retryable: true, safeCode: 'callback_failed' }),
			)

			const second = yield* first.retry
			expect(Option.map(second.execution.prepared, ({ callback }) => callback)).toEqual(
				Option.some('onMentioned'),
			)
			expect(yield* processor.process([mentionAdmission()], second.execution)).toEqual(
				ProviderEventHandled.make({}),
			)
			expect(yield* Ref.get(second.preparations)).toEqual([])
			expect(calls).toEqual(['onMentioned', 'onMentioned'])

			const fresh = yield* makeTestDeliveryExecution()
			yield* processor.process([mentionAdmission()], fresh.execution)
			expect(calls).toEqual(['onMentioned', 'onMentioned', 'onSubscribedIssueEvents'])
		}).pipe(Effect.provide(sharedLayer({ onMentioned, onSubscribedIssueEvents })))
	})

	it.effect('records a handoff the callback returns and reports the batch handled', ({ expect }) =>
		Effect.gen(function* () {
			const test = yield* makeTestDeliveryExecution()
			const result = yield* processor
				.process([openedAdmission()], test.execution)
				.pipe(Effect.provide(sharedLayer({ onIssueCreated: (_event, delivery) => delivery.handoff() })))
			expect(result).toEqual(ProviderEventHandled.make({}))
			expect(yield* Ref.get(test.handoffs)).toHaveLength(1)
		}),
	)

	it.effect('maps preparation failures without running the callback', ({ expect }) => {
		const onIssueCreated = vi.fn<Handler<'onIssueCreated'>>(() => Effect.void)
		return Effect.gen(function* () {
			const test = yield* makeTestDeliveryExecution()
			const withPrepare = (prepare: ProviderDeliveryExecution['prepare']) =>
				new ProviderDeliveryExecution({ ...test.execution, prepare })

			const conflict = yield* processor
				.process(
					[openedAdmission()],
					withPrepare(() =>
						Effect.fail(DeliveryPreparationConflict.make({ deliveryId: test.execution.deliveryId })),
					),
				)
				.pipe(Effect.flip)
			expect(conflict).toEqual(
				ProviderEventExecutionFailed.make({
					provider: 'github',
					retryable: false,
					safeCode: 'delivery_prepare_conflict',
				}),
			)

			const unavailable = yield* processor
				.process(
					[openedAdmission()],
					withPrepare(() => Effect.fail(DeliveryPreparationUnavailable.make({ reason: 'store offline' }))),
				)
				.pipe(Effect.flip)
			expect(unavailable).toEqual(
				ProviderEventExecutionFailed.make({
					provider: 'github',
					retryable: true,
					safeCode: 'delivery_prepare_unavailable',
				}),
			)
			expect(onIssueCreated).not.toHaveBeenCalled()
		}).pipe(Effect.provide(sharedLayer({ onIssueCreated })))
	})

	it.effect('fails a saved callback that is unknown, unconfigured, or cannot be rebuilt', ({ expect }) => {
		const onIssueCreated = vi.fn<Handler<'onIssueCreated'>>(() => Effect.void)
		const onPrCreated = vi.fn<Handler<'onPrCreated'>>(() => Effect.void)
		return Effect.gen(function* () {
			const test = yield* makeTestDeliveryExecution()
			const withPrepared = (callback: string) =>
				new ProviderDeliveryExecution({
					...test.execution,
					prepared: Option.some(
						PreparedDeliveryInvocation.make({
							callback,
							presentationVersion: 1,
							destination: null,
							supportedOperations: [],
						}),
					),
				})
			const failed = (safeCode: string) =>
				ProviderEventExecutionFailed.make({ provider: 'github', retryable: false, safeCode })

			expect(
				yield* processor.process([openedAdmission()], withPrepared('onMentioned')).pipe(Effect.flip),
			).toEqual(failed('prepared_callback_missing'))
			expect(yield* processor.process([openedAdmission()], withPrepared('onRemoved')).pipe(Effect.flip)).toEqual(
				failed('prepared_callback_missing'),
			)
			expect(
				yield* processor.process([openedAdmission()], withPrepared('onPrCreated')).pipe(Effect.flip),
			).toEqual(failed('prepared_callback_unbuildable'))
			expect(onIssueCreated).not.toHaveBeenCalled()
			expect(onPrCreated).not.toHaveBeenCalled()
			expect(yield* Ref.get(test.preparations)).toEqual([])
		}).pipe(Effect.provide(sharedLayer({ onIssueCreated, onPrCreated })))
	})
})
