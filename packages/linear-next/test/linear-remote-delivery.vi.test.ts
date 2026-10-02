/**
 * Linear handoff end to end: signed webhooks into `Channels.make` over memory storage, callbacks that
 * hand off, a remote worker using the generated delivery client, and a fake Linear that records what
 * it is asked to show and, like Linear, refuses a second activity with an ID it has seen.
 */
import * as NodeCrypto from '@effect/platform-node/NodeCrypto'
import { describe, it } from '@effect/vitest'
import { expect } from 'vite-plus/test'
import {
	Channels,
	ChannelsMemory,
	DeliveryActivity,
	ExternalLink,
	MessageId,
	makeDeliveryClient,
	type DeliveryClient,
	type DeliveryContext,
} from '@humanlayer/channels-delivery-next'
import { Clock, Config, Data, Effect, Layer, Predicate, Queue, Redacted, Ref, Schedule, type Schema } from 'effect'
import { HttpClient, HttpClientRequest, HttpClientResponse } from 'effect/unstable/http'

import { LinearAuth, LinearBot } from '../src'
import { LinearApi, LinearApiError } from '../src/LinearApi'
import { LinearAgentActivityId, LinearCommentId, type LinearIssueId } from '../src/LinearIdentity'
import {
	LinearAgentActivityReceipt,
	LinearCommentRef,
	LinearIssueRef,
	type LinearCreateAgentActivityRequest,
} from '../src/LinearModels'
import { LinearComment } from '../src/LinearResources'
import {
	agentSessionPayloads,
	issueCreatePayload,
	linearAppUserId,
	linearOauthClientId,
	linearOrganizationId,
	linearWebhookSecret,
	signedLinearInput,
} from './fixtures'

/** What the fake Linear shows, in order. */
type Shown = string

/** How the fake Linear shows an activity: `thought~` for an ephemeral thought, `thought` for a lasting one. */
const describeActivity = (request: LinearCreateAgentActivityRequest): Shown => {
	const { content } = request
	const kind = Predicate.isTagged(content, 'Thought') && request.ephemeral ? 'thought~' : content._tag.toLowerCase()
	const options =
		Predicate.isTagged(content, 'Elicitation') && content.options !== undefined ? ` [${content.options.join('|')}]` : ''
	return `${kind}: ${content.body}${options}`
}

type FakeLinearOptions = {
	/** Runs while Linear is creating an activity whose body is this text, before it answers. */
	readonly during?: { readonly body: string; readonly run: Effect.Effect<void> }
	/** Linear makes the first activity with this body, then the answer is lost: the attempt sees a retryable failure. */
	readonly loseAnswerFor?: string
}

/**
 * A fake Linear. It records every activity it makes and every link and comment, and refuses an
 * activity whose ID it has already used, as Linear does.
 */
const makeFakeLinear = (options: FakeLinearOptions = {}) =>
	Effect.gen(function* () {
		const shown = yield* Queue.unbounded<Shown>()
		const activityIds = yield* Ref.make<ReadonlyArray<string>>([])
		const requests = yield* Ref.make<ReadonlyArray<LinearCreateAgentActivityRequest>>([])
		const answersLost = yield* Ref.make(0)
		const commentCount = yield* Ref.make(0)
		const commentOn = (issueId: LinearIssueId, commentId: string, markdown: string) =>
			LinearComment.make({
				ref: LinearCommentRef.make({
					organizationId: linearOrganizationId,
					teamId: null,
					issueId,
					commentId: LinearCommentId.make(commentId),
				}),
				issue: LinearIssueRef.make({ organizationId: linearOrganizationId, teamId: null, issueId }),
				parentCommentId: null,
				content: { markdown },
				author: null,
				files: [],
			})
		const api = Layer.mock(LinearApi, {
			createAgentActivity: (request) =>
				Effect.gen(function* () {
					yield* Ref.update(requests, (all) => [...all, request])
					const activityId = request.activityId ?? `assigned-${(yield* Ref.get(requests)).length}`
					if ((yield* Ref.get(activityIds)).includes(activityId)) {
						return yield* LinearApiError.make({
							operation: 'create_agent_activity',
							reason: 'already_exists',
							retryable: false,
						})
					}
					if (options.during !== undefined && request.content.body === options.during.body) yield* options.during.run
					yield* Ref.update(activityIds, (ids) => [...ids, activityId])
					yield* Queue.offer(shown, describeActivity(request))
					if (request.content.body === options.loseAnswerFor && (yield* Ref.getAndUpdate(answersLost, (n) => n + 1)) === 0) {
						return yield* LinearApiError.make({ operation: 'create_agent_activity', reason: 'unavailable', retryable: true })
					}
					return LinearAgentActivityReceipt.make({
						activityId: LinearAgentActivityId.make(activityId),
						sessionId: request.sessionId,
					})
				}),
			updateAgentSession: (request) =>
				Effect.gen(function* () {
					if (request.addedExternalUrls !== undefined) {
						yield* Queue.offer(shown, `link: ${request.addedExternalUrls.map(({ label, url }) => `${label} ${url}`).join(', ')}`)
					}
					if (request.plan !== undefined) {
						yield* Queue.offer(shown, `plan: ${request.plan.map(({ content, status }) => `${content} [${status}]`).join(', ')}`)
					}
				}),
			createComment: (request) =>
				Effect.gen(function* () {
					const count = yield* Ref.updateAndGet(commentCount, (n) => n + 1)
					yield* Queue.offer(shown, `comment ${count}: ${request.content.markdown}`)
					return commentOn(request.issue.issueId, `comment-${count}`, request.content.markdown)
				}),
			updateComment: (request) =>
				Queue.offer(shown, `edit ${request.comment.commentId}: ${request.content.markdown}`).pipe(
					Effect.as(commentOn(request.comment.issueId, request.comment.commentId, request.content.markdown)),
				),
			deleteComment: (request) => Queue.offer(shown, `delete ${request.comment.commentId}`).pipe(Effect.asVoid),
		})
		return { api, shown, requests }
	})

type Event = Data.TaggedEnum<{
	SessionCreated: { readonly delivery: DeliveryContext }
	SessionPrompted: { readonly delivery: DeliveryContext; readonly signal: string | null }
	IssueCreated: { readonly delivery: DeliveryContext }
}>
const Event = Data.taggedEnum<Event>()

const promptPayload = (activityId: string, signal: string | null) => ({
	...agentSessionPayloads[1],
	agentActivity: { ...agentSessionPayloads[1].agentActivity, id: activityId, signal },
})

/**
 * Start a Linear bot whose session and issue callbacks hand off (with a run-log link for a session),
 * and a delivery client that reaches it. `send` posts a signed webhook.
 */
const startBot = (api: Layer.Layer<LinearApi>) =>
	Effect.gen(function* () {
		const events = yield* Queue.unbounded<Event>()
		const bot = Channels.make({
			namespace: 'linear-remote-delivery-test',
			basePath: '/api/channels',
			providers: [
				LinearBot.make({
					webhookSecret: Config.succeed(Redacted.make(linearWebhookSecret)),
					bot: { organizationId: linearOrganizationId, appUserId: linearAppUserId },
					auth: LinearAuth.clientCredentials({
						clientId: Config.succeed(linearOauthClientId),
						clientSecret: Config.succeed(Redacted.make('unused-secret')),
					}),
					linearApi: api,
					handlers: {
						onAgentSessionCreated: (_event, delivery) =>
							Queue.offer(events, Event.SessionCreated({ delivery })).pipe(
								Effect.andThen(
									delivery.handoff({
										links: [ExternalLink.make({ label: 'Run log', url: 'https://agent.example.com/runs/1' })],
									}),
								),
							),
						onAgentSessionPrompted: (event, delivery) =>
							Queue.offer(events, Event.SessionPrompted({ delivery, signal: event.prompt.signal })).pipe(
								Effect.asVoid,
							),
						onIssueCreated: (_event, delivery) =>
							Queue.offer(events, Event.IssueCreated({ delivery })).pipe(Effect.andThen(delivery.handoff())),
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
		const send = (payload: Schema.Json, eventType: string, deliveryId: string) =>
			Effect.gen(function* () {
				const now = yield* Clock.currentTimeMillis
				const signed = signedLinearInput(payload, eventType, deliveryId, now)
				const response = yield* Effect.promise(() =>
					started.handle(
						new Request('http://localhost/api/channels/integrations/linear/webhook', {
							method: 'POST',
							headers: signed.headers,
							body: new TextDecoder().decode(signed.body),
						}),
					),
				)
				if (response.status !== 200) return yield* Effect.die(new Error(`webhook answered ${response.status}`))
				return response.status
			})
		return { events, client, send }
	})

const targetOf = (delivery: DeliveryContext) => ({ deliveryId: delivery.deliveryId, accessToken: delivery.accessToken })

const awaitRetired = (client: DeliveryClient, delivery: DeliveryContext) =>
	client
		.status(targetOf(delivery))
		.pipe(Effect.repeat({ until: ({ stage }) => stage === 'Retired', schedule: Schedule.spaced('20 millis') }))

/** Take the next `count` things the fake Linear showed. */
const takeShown = (shown: Queue.Queue<Shown>, count: number) =>
	Effect.forEach(Array.from({ length: count }), () => Queue.take(shown))

/** Send `created` and wait for the callback's handoff and the session's automatic thought and link. */
const handOffSession = (api: Layer.Layer<LinearApi>, shown: Queue.Queue<Shown>) =>
	Effect.gen(function* () {
		const bot = yield* startBot(api)
		yield* bot.send(agentSessionPayloads[0], 'AgentSessionEvent', 'delivery-created')
		const created = yield* Queue.take(bot.events)
		expect(created._tag).toBe('SessionCreated')
		/** The automatic thought before the callback, then the link the callback gave at handoff. */
		expect(yield* takeShown(shown, 2)).toEqual([
			'thought~: Working on this…',
			'link: Run log https://agent.example.com/runs/1',
		])
		return { ...bot, delivery: created.delivery, target: targetOf(created.delivery) }
	})

describe('Linear remote delivery: Agent Session', () => {
	it.live(
		'a handed-off turn shows Working as ephemeral thoughts, a message as a lasting thought, and ends with one response',
		({ expect }) =>
			Effect.gen(function* () {
				const linear = yield* makeFakeLinear()
				const { client, delivery, target } = yield* handOffSession(linear.api, linear.shown)
				const waiting = yield* client.status(target)
				expect(waiting.stage).toBe('ExternalWaiting')
				expect(waiting.supportedOperations).toContain('SetActivity')
				expect(waiting.supportedOperations).not.toContain('UpdateMessage')

				const working = (message: string) =>
					client.activity.set({ ...target, activity: DeliveryActivity.cases.Working.make({ message }) })
				yield* working('Reading logs')
				expect(yield* Queue.take(linear.shown)).toBe('thought~: Reading logs')
				yield* working('Running tests')
				expect(yield* Queue.take(linear.shown)).toBe('thought~: Running tests')
				const summary = MessageId.make('summary')
				yield* client.messages.create({ ...target, message: { messageId: summary, markdown: 'Summary: all green.' } })
				expect(yield* Queue.take(linear.shown)).toBe('thought: Summary: all green.')
				const edit = yield* client.messages
					.update({ ...target, messageId: summary, message: { markdown: 'other' } })
					.pipe(Effect.flip)
				expect(edit).toMatchObject({ _tag: 'DeliveryOperationUnsupported', operation: 'UpdateMessage' })
				yield* client.activity.set({ ...target, activity: DeliveryActivity.cases.Idle.make({}) })
				yield* client.complete({ ...target, payload: { markdown: 'Fixed the flaky test.' } })
				expect(yield* Queue.take(linear.shown)).toBe('response: Fixed the flaky test.')

				const retired = yield* awaitRetired(client, delivery)
				expect(retired.output.map(({ kind, state }) => `${kind}:${state}`)).toEqual([
					'AddExternalLink:Delivered',
					'SetActivity:Delivered',
					'SetActivity:Delivered',
					'CreateMessage:Delivered',
					'SetActivity:Delivered',
					'PresentOutcome:Delivered',
				])
				expect(yield* Queue.size(linear.shown)).toBe(0)
				const ids = (yield* Ref.get(linear.requests)).map(({ activityId }) => activityId)
				expect(ids.every((id) => id !== undefined)).toBe(true)
				expect(new Set(ids).size).toBe(ids.length)
			}),
	)

	it.live('a response Linear made but whose answer was lost is sent again under the same ID and shown once', ({ expect }) =>
		Effect.gen(function* () {
			const linear = yield* makeFakeLinear({ loseAnswerFor: 'Done.' })
			const { client, delivery, target } = yield* handOffSession(linear.api, linear.shown)
			yield* client.complete(target)
			expect(yield* Queue.take(linear.shown)).toBe('response: Done.')
			const retired = yield* awaitRetired(client, delivery)
			expect(retired.output.at(-1)).toMatchObject({ kind: 'PresentOutcome', state: 'Delivered', attempts: 2 })
			expect(yield* Queue.size(linear.shown)).toBe(0)
			const responses = (yield* Ref.get(linear.requests)).filter(({ content }) => Predicate.isTagged(content, 'Response'))
			expect(responses).toHaveLength(2)
			expect(responses[0]?.activityId).toBeDefined()
			expect(responses[1]?.activityId).toBe(responses[0]?.activityId)
		}),
	)

	it.live('ending a turn with a question posts an elicitation with choices; the reply is a new delivery', ({ expect }) =>
		Effect.gen(function* () {
			const linear = yield* makeFakeLinear()
			const { client, delivery, target, send, events } = yield* handOffSession(linear.api, linear.shown)
			yield* client.complete({
				...target,
				payload: { markdown: 'Which environment?', awaitingInput: { options: ['staging', 'production'] } },
			})
			expect(yield* Queue.take(linear.shown)).toBe('elicitation: Which environment? [staging|production]')
			yield* awaitRetired(client, delivery)
			yield* send(promptPayload('73000000-0000-4000-8000-0000000000aa', null), 'AgentSessionEvent', 'delivery-reply')
			const reply = yield* Queue.take(events)
			expect(reply._tag).toBe('SessionPrompted')
			expect(reply.delivery.deliveryId === delivery.deliveryId).toBe(false)
			expect(reply.delivery.conversationId).toBe(delivery.conversationId)
		}),
	)

	it.live('Stop marks the handed-off turn; the worker fails it with one error, then the stop prompt runs', ({ expect }) =>
		Effect.gen(function* () {
			const linear = yield* makeFakeLinear()
			const { client, delivery, target, send, events } = yield* handOffSession(linear.api, linear.shown)
			yield* send(promptPayload('73000000-0000-4000-8000-0000000000bb', 'stop'), 'AgentSessionEvent', 'delivery-stop')
			const stopped = yield* client
				.status(target)
				.pipe(Effect.repeat({ until: ({ interruptRequested }) => interruptRequested, schedule: Schedule.spaced('10 millis') }))
			expect(stopped.stage).toBe('ExternalWaiting')
			expect(yield* Queue.size(events)).toBe(0)
			yield* client.fail({ ...target, payload: { markdown: 'Stopped as requested.' } })
			expect(yield* Queue.take(linear.shown)).toBe('error: Stopped as requested.')
			const stop = yield* Queue.take(events)
			expect(stop).toMatchObject({ _tag: 'SessionPrompted', signal: 'stop' })
			const retired = yield* awaitRetired(client, delivery)
			expect(retired.interruptRequested).toBe(true)
			expect(yield* Queue.size(linear.shown)).toBe(0)
		}),
	)
})

describe('Linear remote delivery: output accepted while an earlier Linear output is being sent', () => {
	it.live('a result sent while the summary thought is being posted is shown after it, and the next prompt then runs', ({ expect }) =>
		Effect.gen(function* () {
			const during = yield* Ref.make<Effect.Effect<void>>(Effect.void)
			const linear = yield* makeFakeLinear({
				during: { body: 'Summary', run: Effect.flatten(Ref.getAndSet(during, Effect.void)) },
			})
			const { client, delivery, target, send, events } = yield* handOffSession(linear.api, linear.shown)
			yield* send(promptPayload('73000000-0000-4000-8000-0000000000cc', null), 'AgentSessionEvent', 'delivery-next')
			const accepted = yield* Ref.make<ReadonlyArray<string>>([])
			yield* Ref.set(
				during,
				client.complete({ ...target, payload: { markdown: 'Done.' } }).pipe(
					Effect.flatMap(({ status }) => Ref.update(accepted, (all) => [...all, status])),
					Effect.orDie,
				),
			)
			yield* client.messages.create({ ...target, message: { messageId: MessageId.make('summary'), markdown: 'Summary' } })
			expect(yield* takeShown(linear.shown, 2)).toEqual(['thought: Summary', 'response: Done.'])
			expect(yield* Ref.get(accepted)).toEqual(['accepted'])
			yield* awaitRetired(client, delivery)
			const next = yield* Queue.take(events)
			expect(next._tag).toBe('SessionPrompted')
		}),
	)

	it.live('activity sent while the summary thought is being posted is shown after it', ({ expect }) =>
		Effect.gen(function* () {
			const during = yield* Ref.make<Effect.Effect<void>>(Effect.void)
			const linear = yield* makeFakeLinear({
				during: { body: 'Summary', run: Effect.flatten(Ref.getAndSet(during, Effect.void)) },
			})
			const { client, delivery, target } = yield* handOffSession(linear.api, linear.shown)
			yield* Ref.set(
				during,
				client
					.activity.set({ ...target, activity: DeliveryActivity.cases.Working.make({ message: 'Still going' }) })
					.pipe(Effect.asVoid, Effect.orDie),
			)
			yield* client.messages.create({ ...target, message: { messageId: MessageId.make('summary'), markdown: 'Summary' } })
			expect(yield* takeShown(linear.shown, 2)).toEqual(['thought: Summary', 'thought~: Still going'])
			yield* client.complete(target)
			expect(yield* Queue.take(linear.shown)).toBe('response: Done.')
			yield* awaitRetired(client, delivery)
		}),
	)
})

describe('Linear remote delivery: issue', () => {
	it.live('a handed-off issue delivery comments its messages and result, and refuses activity', ({ expect }) =>
		Effect.gen(function* () {
			const linear = yield* makeFakeLinear()
			const { client, send, events } = yield* startBot(linear.api)
			yield* send(issueCreatePayload, 'Issue', 'delivery-issue')
			const created = yield* Queue.take(events)
			expect(created._tag).toBe('IssueCreated')
			const target = targetOf(created.delivery)
			const status = yield* client.status(target)
			expect(status.supportedOperations).not.toContain('SetActivity')
			const refused = yield* client.activity
				.set({ ...target, activity: DeliveryActivity.cases.Working.make({ message: 'x' }) })
				.pipe(Effect.flip)
			expect(refused).toMatchObject({ _tag: 'DeliveryOperationUnsupported', operation: 'SetActivity' })

			const progress = MessageId.make('progress')
			yield* client.messages.create({ ...target, message: { messageId: progress, markdown: 'Running' } })
			yield* client.messages.update({ ...target, messageId: progress, message: { markdown: 'Passed' } })
			yield* client.messages.delete({ ...target, messageId: progress })
			yield* client.complete({ ...target, payload: { markdown: 'Done.' } })
			expect(yield* takeShown(linear.shown, 4)).toEqual([
				'comment 1: Running',
				'edit comment-1: Passed',
				'delete comment-1',
				'comment 2: Done.',
			])
			yield* awaitRetired(client, created.delivery)
			expect(yield* Ref.get(linear.requests)).toEqual([])
		}),
	)
})
