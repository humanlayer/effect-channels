/**
 * GitHub handoff end to end: signed webhooks into `Channels.make` over memory storage, callbacks that
 * hand off, a remote worker using the generated delivery client, and a fake GitHub that records the
 * comments it is asked to make and keeps the bot's reactions as state, as GitHub does.
 */
import * as NodeCrypto from '@effect/platform-node/NodeCrypto'
import { describe, it } from '@effect/vitest'
import { expect } from 'vite-plus/test'
import {
	Channels,
	ChannelsMemory,
	DeliveryActivity,
	DeliveryReactionTarget,
	MessageId,
	QueueDeliveryMode,
	makeDeliveryClient,
	type DeliveryClient,
	type DeliveryContext,
} from '@humanlayer/channels-delivery-next'
import { Config, Effect, Layer, Match, Predicate, Queue, Redacted, Ref, Schedule, Schema } from 'effect'
import { HttpClient, HttpClientRequest, HttpClientResponse } from 'effect/unstable/http'

import {
	GitHubApi,
	GitHubApiError,
	GitHubBot,
	GitHubId,
	GitHubIssueComment,
	GitHubReviewCommentRef,
	type GitHubCommentRef,
	type GitHubDiscussionRef,
	type GitHubReactionTarget,
} from '../src'
import { githubWebhookSecret, issueCommentPayload, signedGitHubInput } from './fixtures'

type FakeGitHubOptions = {
	/** Runs while GitHub is posting a comment with this text, before it answers. */
	readonly during?: { readonly body: string; readonly run: Effect.Effect<void> }
	/** GitHub refuses the first post of a comment with this text as an outage. */
	readonly failFirstPostOf?: string
}

const describeComment = (ref: GitHubCommentRef) =>
	Schema.is(GitHubReviewCommentRef)(ref) ? `review-comment ${ref.id}` : `comment ${ref.id}`
const describeDiscussion = (discussion: GitHubDiscussionRef) =>
	`${Predicate.isTagged(discussion, 'Issue') ? 'issue' : 'pull-request'} ${discussion.ref.number}`
const describeReactionTarget = (target: GitHubReactionTarget) =>
	Match.value(target).pipe(
		Match.tagsExhaustive({
			Comment: ({ comment }) => describeComment(comment),
			Discussion: ({ discussion }) => describeDiscussion(discussion),
		}),
	)

/**
 * A fake GitHub. It records every comment it makes, edits, or deletes, and holds the bot's reactions
 * as a set: adding one already there, or removing one already gone, changes nothing, as on GitHub.
 */
const makeFakeGitHub = (options: FakeGitHubOptions = {}) =>
	Effect.gen(function* () {
		const shown = yield* Queue.unbounded<string>()
		const reactions = yield* Ref.make<ReadonlySet<string>>(new Set())
		const commentCount = yield* Ref.make(0)
		const failedPosts = yield* Ref.make(0)
		const post = (discussion: GitHubDiscussionRef, markdown: string) =>
			Effect.gen(function* () {
				if (options.failFirstPostOf === markdown && (yield* Ref.getAndUpdate(failedPosts, (n) => n + 1)) === 0) {
					return yield* GitHubApiError.make({ operation: 'post_issue_comment', reason: 'unavailable', retryable: true })
				}
				if (options.during !== undefined && options.during.body === markdown) yield* options.during.run
				const id = 900 + (yield* Ref.updateAndGet(commentCount, (n) => n + 1))
				yield* Queue.offer(shown, `post ${describeDiscussion(discussion)}: ${markdown}`)
				return GitHubIssueComment.make({
					ref: { discussion, id: GitHubId.make(id) },
					body: markdown,
					url: `https://github.com/alice/project/issues/42#issuecomment-${id}`,
					author: null,
				})
			})
		const react = (reaction: string, target: string, active: boolean) =>
			Ref.modify(reactions, (current) => {
				const key = `${reaction} on ${target}`
				const next = new Set(current)
				if (active) next.add(key)
				else next.delete(key)
				return [current.has(key) !== active, next] as const
			}).pipe(
				Effect.flatMap((changed) =>
					changed ? Queue.offer(shown, `${reaction} ${active ? 'on' : 'off'} ${target}`) : Effect.void,
				),
				Effect.asVoid,
			)
		const api = Layer.mock(GitHubApi, {
			postIssueComment: ({ issue, content }) => post({ _tag: 'Issue', ref: issue }, content.markdown),
			postPullRequestComment: ({ pullRequest, content }) =>
				post({ _tag: 'PullRequest', ref: pullRequest }, content.markdown),
			updateComment: ({ comment, content }) =>
				Schema.is(GitHubReviewCommentRef)(comment)
					? Effect.die(new Error('the delivery never edits review comments'))
					: Queue.offer(shown, `edit ${describeComment(comment)}: ${content.markdown}`).pipe(
							Effect.as(
								GitHubIssueComment.make({
									ref: comment,
									body: content.markdown,
									url: 'https://github.com/alice/project/issues/42',
									author: null,
								}),
							),
						),
			deleteComment: ({ comment }) => Queue.offer(shown, `delete ${describeComment(comment)}`).pipe(Effect.asVoid),
			addReaction: ({ target, reaction }) => react(reaction, describeReactionTarget(target), true),
			removeReaction: ({ target, reaction }) => react(reaction, describeReactionTarget(target), false),
		})
		return { api, shown, reactions }
	})

type Event = {
	readonly _tag: 'Mentioned' | 'SubscribedIssueEvents'
	readonly delivery: DeliveryContext
}

/** A comment mentioning the bot, on issue 42 or on pull request 42. */
const mentionPayload = (input: { readonly id: number; readonly body: string; readonly pullRequest?: boolean }) => {
	const payload = issueCommentPayload({ pullRequest: input.pullRequest ?? false })
	return { ...payload, comment: { ...payload.comment, id: input.id, body: input.body } }
}

/**
 * Start a GitHub bot and a delivery client that reaches it. A mention that says `subscribe` subscribes
 * the issue and returns; any other mention, and any subscribed batch, hands off. `send` posts a signed
 * webhook.
 */
const startBot = (api: Layer.Layer<GitHubApi>) =>
	Effect.gen(function* () {
		const events = yield* Queue.unbounded<Event>()
		const bot = Channels.make({
			namespace: 'github-remote-delivery-test',
			basePath: '/api/channels',
			providers: [
				GitHubBot.make({
					webhookSecret: Config.succeed(Redacted.make(githubWebhookSecret)),
					deliveryMode: QueueDeliveryMode.make({}),
					bot: { mentionNames: ['agent'], botUserId: GitHubId.make(999) },
					gitHubApi: api,
					handlers: {
						onMentioned: (event, delivery) =>
							Effect.gen(function* () {
								yield* Queue.offer(events, { _tag: 'Mentioned', delivery })
								const discussion = Predicate.isTagged(event, 'GitHubIssueMentioned') ? event.issue : event.pullRequest
								const text = Match.value(event.trigger).pipe(
									Match.tag(
										'GitHubIssueCommentCreated',
										'GitHubPrCommentCreated',
										'GitHubPrReviewCommentCreated',
										({ comment }) => comment.body,
									),
									Match.orElse(() => ''),
								)
								if (text.includes('subscribe')) return yield* discussion.subscribe().pipe(Effect.orDie, Effect.asVoid)
								return yield* delivery.handoff()
							}),
						onSubscribedIssueEvents: (_event, delivery) =>
							Queue.offer(events, { _tag: 'SubscribedIssueEvents', delivery }).pipe(
								Effect.andThen(delivery.handoff()),
							),
					},
				}),
			],
			eventProcessing: { concurrency: 1, leaseMs: 30_000 },
			storage: ChannelsMemory.make({ polling: { intervalMs: 10 } }),
		})
		const started = yield* Effect.promise(() => bot.start(NodeCrypto.layer, bot.deliveryApi))
		yield* Effect.addFinalizer(() => Effect.promise(started.stop))
		/** The remote worker's HTTP client: each request goes straight to the bot's handler. */
		const http = HttpClient.make((request) =>
			HttpClientRequest.toWeb(request).pipe(
				Effect.orDie,
				Effect.flatMap((web) => Effect.promise(() => started.handle(web))),
				Effect.map((response) => HttpClientResponse.fromWeb(request, response)),
			),
		)
		const client = yield* makeDeliveryClient({ baseUrl: 'http://localhost', basePath: '/api/channels' }).pipe(
			Effect.provideService(HttpClient.HttpClient, http),
		)
		const send = (payload: Schema.Json, webhookId: string) =>
			Effect.gen(function* () {
				const signed = signedGitHubInput('issue_comment', payload, webhookId)
				const response = yield* Effect.promise(() =>
					started.handle(
						new Request('http://localhost/api/channels/integrations/github/webhook', {
							method: 'POST',
							headers: signed.headers,
							body: new TextDecoder().decode(signed.body),
						}),
					),
				)
				if (response.status !== 200) return yield* Effect.die(new Error(`webhook answered ${response.status}`))
			})
		return { events, client, send }
	})

const targetOf = (delivery: DeliveryContext) => ({ deliveryId: delivery.deliveryId, accessToken: delivery.accessToken })

const awaitRetired = (client: DeliveryClient, delivery: DeliveryContext) =>
	client
		.status(targetOf(delivery))
		.pipe(Effect.repeat({ until: ({ stage }) => stage === 'Retired', schedule: Schedule.spaced('20 millis') }))

const takeShown = (shown: Queue.Queue<string>, count: number) =>
	Effect.forEach(Array.from({ length: count }), () => Queue.take(shown))

const working = (message: string) => DeliveryActivity.cases.Working.make({ message })

/** Mention the bot on issue 42 in comment 500, and wait for the callback to hand off. */
const handOffMention = (api: Layer.Layer<GitHubApi>, input: { readonly pullRequest?: boolean } = {}) =>
	Effect.gen(function* () {
		const bot = yield* startBot(api)
		yield* bot.send(mentionPayload({ id: 500, body: '@agent handoff 30', pullRequest: input.pullRequest }), 'mention-1')
		const mentioned = yield* Queue.take(bot.events)
		expect(mentioned._tag).toBe('Mentioned')
		return { ...bot, delivery: mentioned.delivery, target: targetOf(mentioned.delivery) }
	})

describe('GitHub remote delivery: issue and pull request', () => {
	it.live('a handed-off mention shows eyes while working, keeps a summary comment, and ends with one comment', ({ expect }) =>
		Effect.gen(function* () {
			const github = yield* makeFakeGitHub()
			const { client, delivery, target, events } = yield* handOffMention(github.api)
			const waiting = yield* client.status(target)
			expect(waiting.stage).toBe('ExternalWaiting')
			expect(waiting.supportedOperations).toEqual(
				expect.arrayContaining(['PresentOutcome', 'CreateMessage', 'UpdateMessage', 'DeleteMessage', 'SetActivity']),
			)

			yield* client.activity.set({ ...target, activity: working('Reading logs') })
			expect(yield* Queue.take(github.shown)).toBe('eyes on comment 500')
			/** New text for the same state: GitHub already shows eyes, so nothing changes. */
			yield* client.activity.set({ ...target, activity: working('Running tests') })
			const summary = MessageId.make('summary')
			yield* client.messages.create({ ...target, message: { messageId: summary, markdown: 'Summary: halfway.' } })
			expect(yield* Queue.take(github.shown)).toBe('post issue 42: Summary: halfway.')
			yield* client.messages.update({ ...target, messageId: summary, message: { markdown: 'Summary: all green.' } })
			expect(yield* Queue.take(github.shown)).toBe('edit comment 901: Summary: all green.')
			yield* client.complete({ ...target, payload: { markdown: 'Fixed the flaky test.' } })
			expect(yield* takeShown(github.shown, 2)).toEqual(['eyes off comment 500', 'post issue 42: Fixed the flaky test.'])

			const retired = yield* awaitRetired(client, delivery)
			expect(retired.output.map(({ kind, state }) => `${kind}:${state}`)).toEqual([
				'SetActivity:Delivered',
				'SetActivity:Delivered',
				'CreateMessage:Delivered',
				'UpdateMessage:Delivered',
				'PresentOutcome:Delivered',
			])
			expect(yield* Ref.get(github.reactions)).toEqual(new Set())
			expect(yield* Queue.size(github.shown)).toBe(0)
			expect(yield* Queue.size(events)).toBe(0)
		}),
	)

	it.live('adds and removes portable reactions on the mention and on a posted comment, and repeats change nothing', ({ expect }) =>
		Effect.gen(function* () {
			const github = yield* makeFakeGitHub()
			const { client, delivery, target } = yield* handOffMention(github.api)
			expect((yield* client.status(target)).reactionTargets).toEqual(['ActivationTarget', 'MessageTarget'])
			const onMention = { ...target, target: DeliveryReactionTarget.cases.ActivationTarget.make({}) }
			expect((yield* client.reactions.set({ ...onMention, reaction: 'thumbs_up', active: true })).status).toBe('accepted')
			expect(yield* Queue.take(github.shown)).toBe('+1 on comment 500')
			expect((yield* client.reactions.set({ ...onMention, reaction: 'thumbs_up', active: true })).status).toBe(
				'already_recorded',
			)

			const summary = MessageId.make('summary')
			yield* client.messages.create({ ...target, message: { messageId: summary, markdown: 'Summary.' } })
			yield* client.reactions.set({
				...target,
				target: DeliveryReactionTarget.cases.MessageTarget.make({ messageId: summary }),
				reaction: 'hooray',
				active: true,
			})
			yield* client.reactions.set({ ...onMention, reaction: 'thumbs_up', active: false })
			expect(yield* takeShown(github.shown, 3)).toEqual([
				'post issue 42: Summary.',
				'hooray on comment 901',
				'+1 off comment 500',
			])

			yield* client.complete(target)
			const retired = yield* awaitRetired(client, delivery)
			expect(retired.output.map(({ kind, messageId }) => [kind, messageId])).toEqual([
				['SetMessageReaction', undefined],
				['CreateMessage', 'summary'],
				['SetMessageReaction', 'summary'],
				['SetMessageReaction', undefined],
				['PresentOutcome', undefined],
			])
			/** The delivery ID holds a random batch ID, so only the rest of the status is checked for GitHub IDs. */
			const { deliveryId: _, ...rest } = retired
			expect(JSON.stringify(rest)).not.toMatch(/500|901|alice|project/)
			expect(yield* Ref.get(github.reactions)).toEqual(new Set(['hooray on comment 901']))
			expect(yield* Queue.size(github.shown)).toBe(0)
		}),
	)

	it.live('a pull request mention comments on the pull request, and Idle removes eyes before the result', ({ expect }) =>
		Effect.gen(function* () {
			const github = yield* makeFakeGitHub()
			const { client, delivery, target } = yield* handOffMention(github.api, { pullRequest: true })
			yield* client.activity.set({ ...target, activity: working('Reviewing') })
			expect(yield* Queue.take(github.shown)).toBe('eyes on comment 500')
			yield* client.activity.set({ ...target, activity: DeliveryActivity.cases.Idle.make({}) })
			expect(yield* Queue.take(github.shown)).toBe('eyes off comment 500')
			yield* client.complete({
				...target,
				payload: { markdown: 'Which environment?', awaitingInput: { options: ['staging', 'production'] } },
			})
			expect(yield* Queue.take(github.shown)).toBe('post pull-request 42: Which environment?\n\n- staging\n- production')
			yield* awaitRetired(client, delivery)
			expect(yield* Queue.size(github.shown)).toBe(0)
		}),
	)

	it.live('a final comment GitHub refuses once is posted on retry, without running the callback again', ({ expect }) =>
		Effect.gen(function* () {
			const github = yield* makeFakeGitHub({ failFirstPostOf: 'Done.' })
			const { client, delivery, target, events } = yield* handOffMention(github.api)
			yield* client.complete({ ...target, payload: { markdown: 'Done.' } })
			expect(yield* Queue.take(github.shown)).toBe('post issue 42: Done.')
			const retired = yield* awaitRetired(client, delivery)
			expect(retired.output.at(-1)).toMatchObject({ kind: 'PresentOutcome', state: 'Delivered', attempts: 2 })
			expect(yield* Queue.size(events)).toBe(0)
		}),
	)

	it.live('a subscribed batch has no activation target, so it refuses activity but still comments', ({ expect }) =>
		Effect.gen(function* () {
			const github = yield* makeFakeGitHub()
			const { client, send, events } = yield* startBot(github.api)
			yield* send(mentionPayload({ id: 500, body: '@agent subscribe' }), 'mention-subscribe')
			expect((yield* Queue.take(events))._tag).toBe('Mentioned')
			yield* send(mentionPayload({ id: 501, body: 'a follow-up' }), 'follow-up')
			const subscribed = yield* Queue.take(events)
			expect(subscribed._tag).toBe('SubscribedIssueEvents')
			const target = targetOf(subscribed.delivery)
			expect((yield* client.status(target)).supportedOperations).not.toContain('SetActivity')
			const refused = yield* client.activity.set({ ...target, activity: working('x') }).pipe(Effect.flip)
			expect(refused).toMatchObject({ _tag: 'DeliveryOperationUnsupported', operation: 'SetActivity' })
			yield* client.complete({ ...target, payload: { markdown: 'Noted.' } })
			expect(yield* Queue.take(github.shown)).toBe('post issue 42: Noted.')
			yield* awaitRetired(client, subscribed.delivery)
		}),
	)
})

describe('GitHub remote delivery: output accepted while an earlier GitHub output is being sent', () => {
	it.live('a result and activity sent while the summary comment is being posted run after it, and the next mention then runs', ({ expect }) =>
		Effect.gen(function* () {
			const during = yield* Ref.make<Effect.Effect<void>>(Effect.void)
			const github = yield* makeFakeGitHub({
				during: { body: 'Summary', run: Effect.flatten(Ref.getAndSet(during, Effect.void)) },
			})
			const { client, delivery, target, send, events } = yield* handOffMention(github.api)
			yield* client.activity.set({ ...target, activity: working('Reading logs') })
			expect(yield* Queue.take(github.shown)).toBe('eyes on comment 500')
			yield* send(mentionPayload({ id: 502, body: '@agent handoff 10' }), 'mention-2')
			const accepted = yield* Ref.make<ReadonlyArray<string>>([])
			yield* Ref.set(
				during,
				Effect.gen(function* () {
					const activity = yield* client.activity.set({ ...target, activity: working('Still going') })
					const result = yield* client.complete({ ...target, payload: { markdown: 'Done.' } })
					yield* Ref.set(accepted, [activity.status, result.status])
				}).pipe(Effect.orDie),
			)
			yield* client.messages.create({ ...target, message: { messageId: MessageId.make('summary'), markdown: 'Summary' } })
			expect(yield* takeShown(github.shown, 3)).toEqual([
				'post issue 42: Summary',
				'eyes off comment 500',
				'post issue 42: Done.',
			])
			expect(yield* Ref.get(accepted)).toEqual(['accepted', 'accepted'])
			const retired = yield* awaitRetired(client, delivery)
			expect(retired.output.map(({ kind }) => kind)).toEqual([
				'SetActivity',
				'CreateMessage',
				'SetActivity',
				'PresentOutcome',
			])
			const next = yield* Queue.take(events)
			expect(next._tag).toBe('Mentioned')
			expect(next.delivery.deliveryId === delivery.deliveryId).toBe(false)
		}),
	)
})
