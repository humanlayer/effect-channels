import { describe, it } from '@effect/vitest'
import {
	DeliveryAdmission,
	MailboxSubscriptions,
	ProviderEventExecutionFailed,
	ProviderEventHandled,
	ProviderEventIgnored,
	ProviderEventInvalid,
} from '@humanlayer/channels-delivery-next'
import { Cause, Effect, Exit, Layer } from 'effect'
import { vi } from 'vitest'

import { GitHubApi } from '../src/GitHubApi'
import type { GitHubIssueCreated, GitHubMentioned, GitHubSubscribedPrEvents } from '../src/GitHubCallbackEvents'
import { GitHubCallbacks, type GitHubCallbackHandlers } from '../src/GitHubCallbacks'
import { GitHubBotConfiguration, makeGitHubEventProcessor } from '../src/GitHubEventProcessor'
import { GitHubId } from '../src/GitHubIdentity'
import {
	checkRunPayload,
	issueCommentPayload,
	issuePayload,
	pullRequestIssueCommentPayload,
	pullRequestPayload,
} from './fixtures'

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
		resourceId: `github:v1:200:${'pull_request' in payload.issue ? 'pull-request' : 'issue'}:${payload.issue.number}`,
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
	makeGitHubEventProcessor({ namespace, bot })
		.process(admissions)
		.pipe(Effect.provide(layer(handlers, subscribed)))

describe('GitHub event batch processing', () => {
	it.effect('routes an opened issue to onIssueCreated', ({ expect }) =>
		Effect.gen(function* () {
			const onIssueCreated = vi.fn((_event: GitHubIssueCreated) => Effect.void)
			expect(yield* process({ onIssueCreated }, [admission('issues', issuePayload('opened'), 'opened')])).toEqual(
				ProviderEventHandled.make({}),
			)
			expect(onIssueCreated).toHaveBeenCalledOnce()
			expect(onIssueCreated.mock.calls[0]?.[0].trigger._tag).toBe('GitHubIssueOpened')
		}),
	)

	it.effect('lets a later mention beat opened and includes opened as a trailing event', ({ expect }) =>
		Effect.gen(function* () {
			const onIssueCreated = vi.fn(() => Effect.void)
			const onMentioned = vi.fn((_event: GitHubMentioned) => Effect.void)
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
			const onSubscribedPrEvents = vi.fn((_event: GitHubSubscribedPrEvents) => Effect.void)
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
			const onSubscribedPrEvents = vi.fn((_event: GitHubSubscribedPrEvents) => Effect.void)
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
					name: 'build',
					conclusion: 'success',
					headSha: 'abc123',
				}),
			)
		}),
	)

	it.effect('normalizes a fanned-out check for the admission mailbox association', ({ expect }) =>
		Effect.gen(function* () {
			const onSubscribedPrEvents = vi.fn((_event: GitHubSubscribedPrEvents) => Effect.void)
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
			const onSubscribedPrEvents = vi.fn((_event: GitHubSubscribedPrEvents) => Effect.void)
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
			const onMentioned = vi.fn(() => Effect.void)
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
