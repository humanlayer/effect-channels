/**
 * The real Slack, GitHub, and Linear providers in one `ChannelsCloudflare.make`, over the Durable Object
 * fake, with fake provider APIs. Linear Agent Session and GitHub issue handoffs run through the Worker's
 * routes and the mailbox object, as they do when deployed.
 */
import * as NodeCrypto from '@effect/platform-node/NodeCrypto'
import { it } from '@effect/vitest'
import { DeliveryAdmission, QueueDeliveryMode, type DeliveryContext } from '@humanlayer/channels-delivery-next'
import {
	GitHubApi,
	GitHubBot,
	GitHubId,
	GitHubIssueComment,
	type GitHubReactionTarget,
} from '@humanlayer/channels-github-next'
import {
	LinearAgentActivityId,
	LinearAgentActivityReceipt,
	LinearAgentSession,
	LinearAgentSessionCreated,
	LinearApi,
	LinearAuth,
	LinearBot,
} from '@humanlayer/channels-linear-next'
import { SlackApi, SlackBot } from '@humanlayer/channels-slack-next'
import { Config, Context, Effect, Layer, Match, Option, Predicate, Queue, Redacted, Ref, Schema } from 'effect'
import { HttpServerRequest, HttpServerResponse } from 'effect/unstable/http'

import {
	agentSessionPayloads,
	linearAppUserId,
	linearOauthClientId,
	linearOrganizationId,
	linearWebhookSecret,
	signedLinearInput,
} from '../../linear-next/test/fixtures'
import { githubWebhookSecret, issueCommentPayload, signedGitHubInput } from '../../github-next/test/fixtures'
import { ChannelsCloudflare, DeliveryMailboxes } from '../src'
import { DurableObjectFake, DurableObjectFakeAlarm } from './DurableObjectFake'

/** What a callback saw. */
type Seen = { readonly event: unknown; readonly delivery: DeliveryContext }

/** A reaction target by the ID of its comment, or its issue or pull request number. */
const reactionTargetName = (target: GitHubReactionTarget) =>
	Match.value(target).pipe(
		Match.tagsExhaustive({
			Comment: ({ comment }) => `${comment.id}`,
			Discussion: ({ discussion }) => `#${discussion.ref.number}`,
		}),
	)

/**
 * One bot with all three providers. The Linear session callback and the GitHub mention callback hand
 * off. `duringSummary` runs while the fake Linear posts an activity, or the fake GitHub a comment, whose
 * text is `Summary`, as a remote worker's request can arrive while the mailbox object waits on the
 * provider.
 */
const makeOptions = (input: {
	readonly seen: Queue.Queue<Seen>
	readonly shown: Queue.Queue<string>
	readonly duringSummary: Ref.Ref<Effect.Effect<void>>
}) => {
	const slack = SlackBot.make({
		signingSecret: Config.succeed(Redacted.make('slack-secret')),
		deliveryMode: QueueDeliveryMode.make({}),
		slackApi: Layer.mock(SlackApi, {}),
		handlers: { onNewMention: () => Effect.die('no Slack events in this test') },
	})
	const github = GitHubBot.make({
		webhookSecret: Config.succeed(Redacted.make(githubWebhookSecret)),
		deliveryMode: QueueDeliveryMode.make({}),
		bot: { mentionNames: ['bot'], botUserId: GitHubId.make(1) },
		gitHubApi: Layer.mock(GitHubApi, {
			postIssueComment: ({ issue, content }) =>
				Effect.gen(function* () {
					if (content.markdown === 'Summary') yield* Effect.flatten(Ref.getAndSet(input.duringSummary, Effect.void))
					yield* Queue.offer(input.shown, `comment: ${content.markdown}`)
					return GitHubIssueComment.make({
						ref: { discussion: { _tag: 'Issue', ref: issue }, id: GitHubId.make(900) },
						body: content.markdown,
						url: 'https://github.com/alice/project/issues/42#issuecomment-900',
						author: null,
					})
				}),
			addReaction: ({ target }) => Queue.offer(input.shown, `eyes on ${reactionTargetName(target)}`).pipe(Effect.asVoid),
			removeReaction: ({ target }) =>
				Queue.offer(input.shown, `eyes off ${reactionTargetName(target)}`).pipe(Effect.asVoid),
		}),
		handlers: {
			onMentioned: (event, delivery) =>
				Queue.offer(input.seen, { event, delivery }).pipe(Effect.andThen(delivery.handoff())),
		},
	})
	const linear = LinearBot.make({
		webhookSecret: Config.succeed(Redacted.make(linearWebhookSecret)),
		bot: { organizationId: linearOrganizationId, appUserId: linearAppUserId },
		auth: LinearAuth.clientCredentials({
			clientId: Config.succeed(linearOauthClientId),
			clientSecret: Config.succeed(Redacted.make('unused-secret')),
		}),
		linearApi: Layer.mock(LinearApi, {
			createAgentActivity: (request) =>
				Effect.gen(function* () {
					if (request.content.body === 'Summary') yield* Effect.flatten(Ref.getAndSet(input.duringSummary, Effect.void))
					const kind = request.ephemeral ? 'thought~' : request.content._tag.toLowerCase()
					yield* Queue.offer(input.shown, `${kind}: ${request.content.body}`)
					return LinearAgentActivityReceipt.make({
						activityId: request.activityId ?? LinearAgentActivityId.make('assigned'),
						sessionId: request.sessionId,
					})
				}),
		}),
		handlers: {
			onAgentSessionCreated: (event, delivery) =>
				Queue.offer(input.seen, { event, delivery }).pipe(Effect.andThen(delivery.handoff())),
			onAgentSessionPrompted: (event, delivery) => Queue.offer(input.seen, { event, delivery }).pipe(Effect.asVoid),
		},
	})
	return {
		namespace: 'providers-cloudflare-test',
		basePath: '/api/channels',
		providers: [slack, github, linear],
		eventProcessing: { concurrency: 1, leaseMs: 30_000 },
	} as const
}

const setUp = Effect.gen(function* () {
	const seen = yield* Queue.unbounded<Seen>()
	const shown = yield* Queue.unbounded<string>()
	const duringSummary = yield* Ref.make<Effect.Effect<void>>(Effect.void)
	const options = makeOptions({ seen, shown, duringSummary })
	const bot = ChannelsCloudflare.make(options)
	const durableObject = yield* Layer.build(DurableObjectFake)
	const mailbox = yield* ChannelsCloudflare.makeMailbox(
		options,
		{ rearmAfterMs: 1_000 },
		Layer.succeedContext(durableObject),
	).pipe(Effect.provide(NodeCrypto.layer))
	const alarm = Context.get(durableObject, DurableObjectFakeAlarm)
	const routedTo = yield* Ref.make<ReadonlyArray<string>>([])
	const mailboxes = DeliveryMailboxes.of({
		getByName: (mailboxKey) => ({
			deliver: (admission: DeliveryAdmission) =>
				Ref.update(routedTo, (keys) => [...keys, mailboxKey]).pipe(Effect.andThen(mailbox.deliver(admission))),
			deliveryRequest: mailbox.deliveryRequest,
		}),
	})
	const fetch = yield* ChannelsCloudflare.serve(Layer.merge(bot.routes, bot.deliveryApi)).pipe(
		Effect.provide(Layer.merge(NodeCrypto.layer, Layer.succeed(DeliveryMailboxes, mailboxes))),
	)
	const request = (path: string, init: RequestInit) =>
		fetch.pipe(
			Effect.provideService(
				HttpServerRequest.HttpServerRequest,
				HttpServerRequest.fromWeb(new Request(`http://localhost/api/channels${path}`, init)),
			),
			Effect.map((response) => HttpServerResponse.toWeb(response).status),
			Effect.orDie,
		)
	const sendLinear = (payload: typeof agentSessionPayloads[number], deliveryId: string) => {
		const signed = signedLinearInput(payload, 'AgentSessionEvent', deliveryId)
		return request('/integrations/linear/webhook', {
			method: 'POST',
			headers: signed.headers,
			body: new TextDecoder().decode(signed.body),
		})
	}
	const sendGitHubMention = (commentId: number, webhookId: string) => {
		const payload = issueCommentPayload()
		const signed = signedGitHubInput(
			'issue_comment',
			{ ...payload, comment: { ...payload.comment, id: commentId, body: '@bot handoff 30' } },
			webhookId,
		)
		return request('/integrations/github/webhook', {
			method: 'POST',
			headers: signed.headers,
			body: new TextDecoder().decode(signed.body),
		})
	}
	const callDelivery = (delivery: DeliveryContext, path: string, method: string, body?: Schema.Json) => {
		const headers = {
			authorization: `Bearer ${Redacted.value(delivery.accessToken)}`,
			'content-type': 'application/json',
		}
		const init: RequestInit = { method, headers }
		if (Predicate.isNotUndefined(body)) init.body = JSON.stringify(body)
		return request(`/deliveries/${delivery.deliveryId}${path}`, init)
	}
	return { seen, shown, duringSummary, mailbox, alarm, routedTo, request, sendLinear, sendGitHubMention, callDelivery }
})

it.effect(
	'ChannelsCloudflare with Slack, GitHub, and Linear: each webhook route exists, and a Linear session reaches its callback with Linear values',
	({ expect }) =>
		Effect.gen(function* () {
			const { seen, shown, mailbox, routedTo, request, sendLinear } = yield* setUp
			expect(yield* request('/integrations/slack/webhook', { method: 'POST', body: '{}' })).toEqual(401)
			expect(yield* request('/integrations/github/webhook', { method: 'POST', body: '{}' })).toEqual(401)
			expect(yield* sendLinear(agentSessionPayloads[0], 'delivery-created')).toEqual(200)
			const routed = yield* Ref.get(routedTo)
			expect(routed).toHaveLength(1)
			expect(routed[0]).toContain(`linear:v1:agent-session:${agentSessionPayloads[0].agentSession.id}`)
			yield* mailbox.alarm()
			const created = Option.getOrThrow(yield* Queue.poll(seen))
			expect(Schema.is(LinearAgentSessionCreated)(created.event)).toBe(true)
			expect(created.event).toMatchObject({ session: expect.any(LinearAgentSession) })
			expect(yield* Queue.take(shown)).toEqual('thought~: Working on this…')
		}),
)

it.effect(
	'ChannelsCloudflare Linear session: a result sent while the summary thought is being posted runs in the same alarm, retires the turn, and runs the next prompt',
	({ expect }) =>
		Effect.gen(function* () {
			const { seen, shown, duringSummary, mailbox, alarm, sendLinear, callDelivery } = yield* setUp
			yield* sendLinear(agentSessionPayloads[0], 'delivery-created')
			yield* mailbox.alarm()
			const { delivery } = Option.getOrThrow(yield* Queue.poll(seen))
			expect(yield* Queue.take(shown)).toEqual('thought~: Working on this…')

			const working = { activity: { _tag: 'Working', message: 'Running tests' } }
			expect(yield* callDelivery(delivery, '/activity', 'PUT', working)).toEqual(202)
			expect(yield* callDelivery(delivery, '/messages', 'POST', { messageId: 'summary', markdown: 'Summary' })).toEqual(202)
			yield* sendLinear(agentSessionPayloads[1], 'delivery-prompted')
			const statuses = yield* Ref.make<ReadonlyArray<number>>([])
			yield* Ref.set(
				duringSummary,
				callDelivery(delivery, '/complete', 'POST', { markdown: 'Done.' }).pipe(
					Effect.flatMap((status) => Ref.update(statuses, (all) => [...all, status])),
					Effect.scoped,
				),
			)

			yield* alarm.clearAsCloudflareDoesBeforeTheHandler
			yield* mailbox.alarm()
			expect(yield* Ref.get(statuses)).toEqual([202])
			expect(Array.from(yield* Queue.takeAll(shown))).toEqual(['thought~: Running tests', 'thought: Summary', 'response: Done.'])
			const next = Option.getOrThrow(yield* Queue.poll(seen))
			expect(next.delivery.deliveryId === delivery.deliveryId).toBe(false)
			expect(yield* callDelivery(delivery, '', 'GET')).toEqual(200)
		}),
)

it.effect(
	'ChannelsCloudflare GitHub issue: activity and a result sent while the summary comment is being posted run in the same alarm, retire the delivery, and run the next mention',
	({ expect }) =>
		Effect.gen(function* () {
			const { seen, shown, duringSummary, mailbox, alarm, sendGitHubMention, callDelivery } = yield* setUp
			expect(yield* sendGitHubMention(500, 'github-mention-1')).toEqual(200)
			yield* mailbox.alarm()
			const { delivery } = Option.getOrThrow(yield* Queue.poll(seen))

			const working = { activity: { _tag: 'Working', message: 'Running tests' } }
			expect(yield* callDelivery(delivery, '/activity', 'PUT', working)).toEqual(202)
			expect(yield* callDelivery(delivery, '/messages', 'POST', { messageId: 'summary', markdown: 'Summary' })).toEqual(202)
			expect(yield* sendGitHubMention(501, 'github-mention-2')).toEqual(200)
			const statuses = yield* Ref.make<ReadonlyArray<number>>([])
			const record = (status: number) => Ref.update(statuses, (all) => [...all, status])
			yield* Ref.set(
				duringSummary,
				Effect.gen(function* () {
					yield* record(yield* callDelivery(delivery, '/activity', 'PUT', { activity: { _tag: 'Idle' } }))
					yield* record(yield* callDelivery(delivery, '/complete', 'POST', { markdown: 'Done.' }))
				}).pipe(Effect.scoped),
			)

			yield* alarm.clearAsCloudflareDoesBeforeTheHandler
			yield* mailbox.alarm()
			expect(yield* Ref.get(statuses)).toEqual([202, 202])
			expect(Array.from(yield* Queue.takeAll(shown))).toEqual([
				'eyes on 500',
				'comment: Summary',
				'eyes off 500',
				'comment: Done.',
			])
			const next = Option.getOrThrow(yield* Queue.poll(seen))
			expect(next.delivery.deliveryId === delivery.deliveryId).toBe(false)
			expect(yield* callDelivery(delivery, '', 'GET')).toEqual(200)
		}),
)
