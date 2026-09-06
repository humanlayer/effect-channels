import { assert, describe, it } from '@effect/vitest'
import { MarkdownContent, ThreadId } from '@humanlayer/channels-slack'
import { SlackClient, Slack, SlackState, SlackTeamId, SlackTenantCredentials } from '@humanlayer/channels-slack'
import { connections } from '@humanlayer/channels-slack/postgres'
import { Effect, Layer, Option, Queue, Random, Redacted } from 'effect'
import { HttpClient, HttpClientRequest, HttpClientResponse } from 'effect/unstable/http'

import { database } from './database.ts'

const RepositoryLive = SlackState.layer.pipe(Layer.provideMerge(connections), Layer.provideMerge(database))

const seed = (workspaceId: string, token: string) =>
	Effect.flatMap(SlackState, (state) =>
		state.upsertConnection({
			workspaceId: SlackTeamId.make(workspaceId),
			connection: {
				credentials: {
					botToken: Redacted.make(token),
					botUserId: `U_${workspaceId}`,
					botId: `B_${workspaceId}`,
				},
			},
		}),
	)

describe('Slack multi-tenant Postgres repository', () => {
	it.effect('loads independent workspace token and identity records', () =>
		Effect.gen(function* () {
			const suffix = Math.abs(yield* Random.nextInt).toString()
			const teamA = SlackTeamId.make(`T_A_${suffix}`)
			const teamB = SlackTeamId.make(`T_B_${suffix}`)
			yield* seed(teamA, 'xoxb-token-a')
			yield* seed(teamB, 'xoxb-token-b')
			const services = SlackTenantCredentials.layer
			const program = Effect.gen(function* () {
				const credentials = yield* SlackTenantCredentials
				const a = Option.getOrThrow(yield* credentials.load({ teamId: teamA }))
				const b = Option.getOrThrow(yield* credentials.load({ teamId: teamB }))
				assert.strictEqual(Redacted.value(a.botToken), 'xoxb-token-a')
				assert.strictEqual(a.botUserId, `U_${teamA}`)
				assert.strictEqual(Redacted.value(b.botToken), 'xoxb-token-b')
				assert.ok(Option.isNone(yield* credentials.load({ teamId: SlackTeamId.make(`T_UNKNOWN_${suffix}`) })))
			})
			yield* program.pipe(Effect.provide(services))
		}).pipe(Effect.provide(RepositoryLive)),
	)

	it.effect('uses the canonical ThreadId workspace for outbound post and history credentials', () =>
		Effect.gen(function* () {
			const suffix = Math.abs(yield* Random.nextInt).toString()
			const teamA = SlackTeamId.make(`T_A_${suffix}`)
			const teamB = SlackTeamId.make(`T_B_${suffix}`)
			yield* seed(teamA, 'xoxb-token-a')
			yield* seed(teamB, 'xoxb-token-b')
			const authorizations = yield* Queue.unbounded<string>()
			const http = HttpClient.make((request) =>
				Effect.gen(function* () {
					const web = yield* HttpClientRequest.toWeb(request).pipe(Effect.orDie)
					yield* Queue.offer(authorizations, web.headers.get('authorization') ?? 'missing')
					const body = web.url.includes('users.info')
						? '{"ok":false,"error":"user_not_found"}'
						: web.url.includes('conversations.replies')
							? `{"ok":true,"messages":[{"user":"U_${teamB}","text":"own","ts":"200.2","thread_ts":"200.1"}]}`
							: '{"ok":true,"channel":"C_TEST","ts":"100.1"}'
					return HttpClientResponse.fromWeb(
						request,
						new Response(body, { status: 200, headers: { 'content-type': 'application/json' } }),
					)
				}),
			)
			const connectionServices = SlackTenantCredentials.layer
			const client = SlackClient.layer.pipe(
				Layer.provide(Layer.merge(connectionServices, Layer.succeed(HttpClient.HttpClient, http))),
			)
			const provider = Slack.layer.pipe(Layer.provide(client))
			const program = Effect.gen(function* () {
				const slackProvider = yield* Slack
				yield* slackProvider.post({
					threadId: ThreadId.make(`slack:v1:${teamA}:C_TEST:100.1`),
					content: MarkdownContent.make({ markdown: 'A' }),
				})
				const history = yield* slackProvider.messages({
					threadId: ThreadId.make(`slack:v1:${teamB}:C_TEST:200.1`),
				})
				assert.strictEqual(yield* Queue.take(authorizations), 'Bearer xoxb-token-a')
				assert.strictEqual(yield* Queue.take(authorizations), 'Bearer xoxb-token-b')
				assert.strictEqual(history.messages.at(0)?.author.isMe, true)
			})
			yield* program.pipe(Effect.provide(provider))
		}).pipe(Effect.provide(RepositoryLive)),
	)
})
