import { assert, it } from '@effect/vitest'
import {
	ChannelsGate,
	ChannelsObserver,
	ChannelId,
	ConversationCoordinator,
	IdempotencyKey,
	Ingress,
	IngressDropped,
	Message,
	MessageRef,
	NormalizedMessage,
	Organizations,
	OrgId,
	Subscriptions,
	TenantId,
	Thread,
	ThreadId,
	UserId,
} from '@humanlayer/channels'
import {
	SlackConnection,
	SlackConnectionCredentials,
	SlackTeamId,
	SlackTenantCredentials,
} from '@humanlayer/channels-slack'
import { DateTime, Effect, Layer, Match, Option, Redacted } from 'effect'
import { Persistence } from 'effect/unstable/persistence'

import { makeConnectionServices, slack } from '../src/index.ts'

const provider = slack({
	loadConnection: ({ workspaceId }) =>
		Effect.succeed(
			Match.value(workspaceId).pipe(
				Match.when('T_A', () =>
					SlackConnection.make({
						organizationId: OrgId.make('org-a'),
						enabled: true,
						credentials: SlackConnectionCredentials.make({
							botToken: Redacted.make('token-a'),
							botUserId: 'U_A',
							botId: 'B_A',
						}),
					}),
				),
				Match.when('T_B', () =>
					SlackConnection.make({
						organizationId: OrgId.make('org-b'),
						enabled: false,
						credentials: SlackConnectionCredentials.make({
							botToken: Redacted.make('token-b'),
							botUserId: 'U_B',
							botId: 'B_B',
						}),
					}),
				),
				Match.orElse(() => undefined),
			),
		),
})

it.effect('shares organization, gate, credentials, and bot identity from one callback', () =>
	Effect.gen(function* () {
		const organizations = yield* Organizations
		const gate = yield* ChannelsGate
		const credentials = yield* SlackTenantCredentials
		const a = Option.getOrThrow(yield* credentials.load({ teamId: SlackTeamId.make('T_A') }))
		const b = Option.getOrThrow(yield* credentials.load({ teamId: SlackTeamId.make('T_B') }))
		assert.strictEqual(Redacted.value(a.botToken), 'token-a')
		assert.strictEqual(a.botUserId, 'U_A')
		assert.strictEqual(a.botId, 'B_A')
		assert.strictEqual(Redacted.value(b.botToken), 'token-b')
		assert.deepStrictEqual(
			yield* organizations.resolve({ source: 'slack', tenant: TenantId.make('T_A') }),
			Option.some(OrgId.make('org-a')),
		)
		assert.strictEqual(
			yield* gate.allowed({ orgId: OrgId.make('org-b'), source: 'slack', tenant: TenantId.make('T_B') }),
			false,
		)
	}).pipe(Effect.provide(makeConnectionServices(provider))),
)

it.effect('narrows malformed and unknown callback output safely', () =>
	Effect.gen(function* () {
		const organizations = yield* Organizations
		const error = yield* Effect.flip(organizations.resolve({ source: 'slack', tenant: TenantId.make('T_BAD') }))
		assert.strictEqual(error._tag, 'OrganizationStoreError')
	}).pipe(
		Effect.provide(
			makeConnectionServices(slack({ loadConnection: () => Effect.succeed({ organizationId: 'not-complete' }) })),
		),
	),
)

const messageFor = (tenant: string, seed: string) => {
	const channel = {
		id: ChannelId.make(`slack:v1:${tenant}:C_TEST`),
		provider: 'slack' as const,
		tenant: TenantId.make(tenant),
		isDm: false,
	}
	const ref = { id: ThreadId.make(`${channel.id}:100.1`), channel, isNew: true }
	const message = Message.make({
		ref: MessageRef.make('100.1'),
		threadRef: ref,
		text: 'hello',
		markdown: 'hello',
		author: { userId: UserId.make('U_TEST'), userName: 'test', fullName: 'Test', isBot: false, isMe: false },
		metadata: { sentAt: DateTime.makeUnsafe('2026-09-01T00:00:00Z') },
		attachments: [],
		raw: {},
	})
	return NormalizedMessage.make({
		provider: 'slack',
		tenant: TenantId.make(tenant),
		idempotencyKey: IdempotencyKey.make(`evt_${seed.repeat(32).slice(0, 32)}`),
		thread: Thread.make({ ref, currentMessage: message, recentMessages: [message] }),
		message,
		mentioned: true,
		raw: {},
	})
}

it.effect('drops disabled and unknown workspace connections before admission', () =>
	Effect.gen(function* () {
		const ingress = yield* Ingress
		assert.deepStrictEqual(
			yield* ingress.acceptMessage(messageFor('T_B', 'b')),
			IngressDropped.make({ reason: 'tenant_disabled' }),
		)
		assert.deepStrictEqual(
			yield* ingress.acceptMessage(messageFor('T_UNKNOWN', 'c')),
			IngressDropped.make({ reason: 'unknown_organization' }),
		)
	}).pipe(
		Effect.provide(
			Ingress.layer.pipe(
				Layer.provide(
					Layer.mergeAll(
						makeConnectionServices(provider),
						ConversationCoordinator.layerMemory(),
						ChannelsObserver.layerLogger,
						Subscriptions.layer.pipe(Layer.provide(Persistence.layerMemory)),
					),
				),
			),
		),
	),
)
