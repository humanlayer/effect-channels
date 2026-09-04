import { PgClient } from '@effect/sql-pg'
import { assert, describe, it } from '@effect/vitest'
import { MarkdownContent, Organizations, OrgId, TenantId, ThreadId } from '@humanlayer/channels'
import { makeConnectionServices, slack } from '@humanlayer/channels-app'
import { SlackClient, SlackProvider, SlackTeamId, SlackTenantCredentials } from '@humanlayer/channels-slack'
import { Config, Effect, Layer, Option, Queue, Random, Redacted } from 'effect'
import { HttpClient, HttpClientRequest, HttpClientResponse } from 'effect/unstable/http'

import { loadSlackConnection, seedSlackConnection, SlackConnectionRepositoryLive } from '../src/store.ts'

const postgresTestsEnabled = import.meta.env.DATABASE_URL !== undefined
const DatabaseLive = PgClient.layerConfig({ url: Config.redacted('DATABASE_URL') })
const RepositoryLive = SlackConnectionRepositoryLive.pipe(Layer.provideMerge(DatabaseLive))

const seed = (workspaceId: string, organizationId: string, token: string, enabled: boolean) =>
	seedSlackConnection({
		workspaceId,
		organizationId,
		enabled,
		botToken: Redacted.make(token),
		botUserId: `U_${workspaceId}`,
		botId: `B_${workspaceId}`,
	})

describe.skipIf(!postgresTestsEnabled)('Slack multi-tenant Postgres repository', () => {
	it.effect('loads independent workspace organization, token, enablement, and identity records', () =>
		Effect.gen(function* () {
			const suffix = Math.abs(yield* Random.nextInt).toString()
			const teamA = SlackTeamId.make(`T_A_${suffix}`)
			const teamB = SlackTeamId.make(`T_B_${suffix}`)
			yield* seed(teamA, 'org-a', 'xoxb-token-a', true)
			yield* seed(teamB, 'org-b', 'xoxb-token-b', false)
			const services = makeConnectionServices(slack({ loadConnection: loadSlackConnection })).pipe(
				Layer.provide(DatabaseLive),
			)
			const program = Effect.gen(function* () {
				const organizations = yield* Organizations
				const credentials = yield* SlackTenantCredentials
				const a = Option.getOrThrow(yield* credentials.load({ teamId: teamA }))
				const b = Option.getOrThrow(yield* credentials.load({ teamId: teamB }))
				assert.strictEqual(Redacted.value(a.botToken), 'xoxb-token-a')
				assert.strictEqual(a.botUserId, `U_${teamA}`)
				assert.strictEqual(Redacted.value(b.botToken), 'xoxb-token-b')
				assert.deepStrictEqual(
					yield* organizations.resolve({ source: 'slack', tenant: TenantId.make(teamA) }),
					Option.some(OrgId.make('org-a')),
				)
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
			yield* seed(teamA, 'org-a', 'xoxb-token-a', true)
			yield* seed(teamB, 'org-b', 'xoxb-token-b', true)
			const authorizations = yield* Queue.unbounded<string>()
			const http = HttpClient.make((request) =>
				Effect.gen(function* () {
					const web = yield* HttpClientRequest.toWeb(request).pipe(Effect.orDie)
					yield* Queue.offer(authorizations, web.headers.get('authorization') ?? 'missing')
					const body = web.url.includes('conversations.replies')
						? `{"ok":true,"messages":[{"user":"U_${teamB}","text":"own","ts":"200.2","thread_ts":"200.1"}]}`
						: '{"ok":true,"channel":"C_TEST","ts":"100.1"}'
					return HttpClientResponse.fromWeb(
						request,
						new Response(body, { status: 200, headers: { 'content-type': 'application/json' } }),
					)
				}),
			)
			const connectionServices = makeConnectionServices(slack({ loadConnection: loadSlackConnection })).pipe(
				Layer.provide(DatabaseLive),
			)
			const client = SlackClient.layer.pipe(
				Layer.provide(Layer.merge(connectionServices, Layer.succeed(HttpClient.HttpClient, http))),
			)
			const provider = SlackProvider.layer.pipe(Layer.provide(client))
			const program = Effect.gen(function* () {
				const slackProvider = yield* SlackProvider
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
