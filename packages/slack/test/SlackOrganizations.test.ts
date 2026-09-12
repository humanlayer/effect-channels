import { assert, it } from '@effect/vitest'
import { DeliveryLocatorStore, MailboxReadiness, MailboxStore, MailboxStoreError } from '@humanlayer/channels-delivery'
import { layer as memory } from '@humanlayer/channels-delivery/memory'
import { Context, Deferred, Effect, Fiber, Layer, Logger, Queue, Ref, Schema } from 'effect'

import {
	Message,
	NormalizedMessage,
	NormalizedConversationStopped,
	SlackIngress,
	SlackIngressError,
	SlackOrganizations,
	SlackOrganizationLookupError,
	type SlackOrganizationLookup,
	SlackTeamId,
	SlackChannelId,
	SlackMessageTs,
	Thread,
	slackThreadRef,
} from '../src/index.js'
import { nativeIngressLayer, nativeMailbox, nativeMessage, nativeRunner } from './nativeSupport.js'

const lookupBoundary: Context.Key<
	SlackOrganizations,
	{
		readonly resolve: (
			input: SlackOrganizationLookup,
		) => Effect.Effect<Schema.Json | undefined, SlackOrganizationLookupError>
	}
> = SlackOrganizations

it.effect('custom Slack lookup results are decoded before writes and failures log only safe classifications', () =>
	Effect.gen(function* () {
		const retained = yield* Layer.build(memory({ maxMailboxes: 10 }))
		const store = Context.get(retained, MailboxStore)
		const writes = yield* Ref.make(0)
		const storage = Layer.succeedContext(
			Context.add(
				retained,
				MailboxStore,
				MailboxStore.of({
					...store,
					commitMailbox: (input) =>
						Ref.update(writes, (n) => n + 1).pipe(Effect.andThen(store.commitMailbox(input))),
				}),
			),
		)
		const logs: string[] = []
		const logger = Logger.layer([Logger.make((entry) => logs.push(JSON.stringify(entry.message)))])
		const results: ReadonlyArray<Schema.Json | undefined> = [
			null,
			undefined,
			{},
			{ organizationId: '' },
			{ organizationId: { private: 'private-result-sentinel' } },
			'failed',
		]
		for (const result of results) {
			const lookup = Layer.succeed(lookupBoundary, {
				resolve: () =>
					result === 'failed'
						? Effect.fail(
								Object.assign(SlackOrganizationLookupError.make({}), {
									message: 'private-error-sentinel',
								}),
							)
						: Effect.succeed(result),
			})
			yield* Effect.gen(function* () {
				const ingress = yield* SlackIngress
				const attempt = ingress.acceptMessage(nativeMessage('a'))
				if (result === null) yield* attempt
				else
					assert.deepStrictEqual(
						yield* attempt.pipe(Effect.flip),
						SlackIngressError.make({ operation: 'delivery_admit' }),
					)
				assert.strictEqual(yield* nativeMailbox('reply', nativeMessage('a')), undefined)
			}).pipe(
				Effect.provide(
					Layer.merge(
						nativeIngressLayer(
							{ onNewMention: [{ id: 'reply', handler: () => Effect.die('Unexpected handler') }] },
							storage,
						).pipe(Layer.provide(lookup)),
						logger,
					),
				),
			)
		}
		assert.strictEqual(yield* Ref.get(writes), 0)
		assert.ok(logs.some((log) => log.includes('invalid_result')))
		assert.ok(logs.some((log) => log.includes('lookup_failed')))
		assert.ok(logs.every((log) => !log.includes('private-')))
	}),
)

for (const result of ['unknown', 'failed'] as const) {
	it.effect(`${result} organization lookup prevents Stop from mutating the active native mailbox`, () =>
		Effect.gen(function* () {
			const started = yield* Deferred.make<void>()
			const mode = yield* Ref.make<'known' | 'unknown' | 'failed'>('known')
			const organizations = Layer.succeed(
				SlackOrganizations,
				SlackOrganizations.of({
					resolve: () =>
						Effect.gen(function* () {
							const value = yield* Ref.get(mode)
							if (value === 'failed') return yield* SlackOrganizationLookupError.make({})
							return value === 'unknown' ? null : { organizationId: 'A' }
						}),
				}),
			)
			yield* Effect.gen(function* () {
				const ingress = yield* SlackIngress
				const event = nativeMessage('a')
				yield* ingress.acceptMessage(event)
				const worker = yield* ingress.run(nativeRunner).pipe(Effect.forkChild)
				yield* Deferred.await(started)
				yield* Ref.set(mode, result)
				const stop = NormalizedConversationStopped.make({
					provider: 'slack',
					tenant: event.tenant,
					threadRef: event.thread.ref,
					idempotencyKey: nativeMessage('b').idempotencyKey,
					raw: {},
				})
				if (result === 'failed') yield* ingress.acceptConversationStopped(stop).pipe(Effect.flip)
				else yield* ingress.acceptConversationStopped(stop)
				const saved = yield* nativeMailbox('reply', event)
				assert.strictEqual(saved?.state.active?.cancelled, false)
				assert.strictEqual(saved?.state.active?.envelopes[0].organizationId, 'A')
				assert.deepStrictEqual(saved?.state.outcomes, [])
				yield* Fiber.interrupt(worker)
			}).pipe(
				Effect.provide(
					nativeIngressLayer({
						onNewMention: [
							{
								id: 'reply',
								handler: () => Deferred.succeed(started, undefined).pipe(Effect.andThen(Effect.never)),
							},
						],
					}).pipe(Layer.provide(organizations)),
				),
			)
		}),
	)
}

it.effect('overlapping workspaces share one ingress without leaking organization context', () =>
	Effect.gen(function* () {
		const entered = yield* Queue.unbounded<string>()
		const observed = yield* Queue.unbounded<string>()
		const a = yield* Deferred.make<void>()
		const b = yield* Deferred.make<void>()
		const first = nativeMessage('a')
		const firstWorkspaceId = SlackTeamId.make(first.tenant)
		const original = nativeMessage('b')
		const ref = slackThreadRef(
			{
				teamId: SlackTeamId.make('T_OTHER'),
				channelId: SlackChannelId.make('C_OTHER'),
				threadTs: SlackMessageTs.make('100.1'),
			},
			true,
		)
		const message = Message.make({
			ref: original.message.ref,
			threadRef: ref,
			text: original.message.text,
			markdown: original.message.markdown,
			author: original.message.author,
			metadata: original.message.metadata,
			attachments: original.message.attachments,
			raw: original.message.raw,
		})
		const second = NormalizedMessage.make({
			...original,
			tenant: ref.channel.tenant,
			message,
			thread: Thread.make({ ref, currentMessage: message, recentMessages: [message] }),
		})
		const organizations = SlackOrganizations.layer(({ workspaceId }) =>
			Queue.offer(entered, workspaceId).pipe(
				Effect.andThen(Deferred.await(workspaceId === firstWorkspaceId ? a : b)),
				Effect.as({ organizationId: workspaceId === firstWorkspaceId ? 'A' : 'B' }),
			),
		)
		yield* Effect.gen(function* () {
			const ingress = yield* SlackIngress
			const left = yield* ingress.acceptMessage(first).pipe(Effect.forkChild)
			assert.strictEqual(yield* Queue.take(entered), first.tenant)
			const right = yield* ingress.acceptMessage(second).pipe(Effect.forkChild)
			assert.strictEqual(yield* Queue.take(entered), second.tenant)
			yield* Deferred.succeed(b, undefined)
			yield* Fiber.join(right)
			yield* Deferred.succeed(a, undefined)
			yield* Fiber.join(left)
			assert.strictEqual((yield* nativeMailbox('reply', first))?.state.pending[0]?.organizationId, 'A')
			assert.strictEqual((yield* nativeMailbox('reply', second))?.state.pending[0]?.organizationId, 'B')
			const worker = yield* ingress.run(nativeRunner).pipe(Effect.forkChild)
			assert.deepStrictEqual(
				[yield* Queue.take(observed), yield* Queue.take(observed)].sort(),
				[`${first.tenant}:A`, `${second.tenant}:B`].sort(),
			)
			yield* Fiber.interrupt(worker)
		}).pipe(
			Effect.provide(
				nativeIngressLayer({
					onNewMention: [
						{
							id: 'reply',
							handler: (event, context) =>
								Queue.offer(observed, `${event.tenant}:${context.organizationId}`).pipe(Effect.asVoid),
						},
					],
				}).pipe(Layer.provide(organizations)),
			),
		)
	}),
)

it.effect('default attribution consumes capacity and new admission fails without losing accepted work', () =>
	Effect.gen(function* () {
		const ingress = yield* SlackIngress
		const first = nativeMessage('a')
		yield* ingress.acceptMessage(first)
		assert.deepStrictEqual(
			yield* ingress.acceptMessage(nativeMessage('b')).pipe(Effect.flip),
			SlackIngressError.make({ operation: 'delivery_admit' }),
		)
		yield* ingress.acceptMessage(first)
		const saved = yield* nativeMailbox('reply', first)
		assert.strictEqual(saved?.state.pending.length, 1)
		assert.strictEqual(saved?.state.pending[0]?.organizationId, 'default')
	}).pipe(
		Effect.provide(
			nativeIngressLayer(
				{ onNewMention: [{ id: 'reply', handler: () => Effect.void }] },
				memory({ maxMailboxes: 2 }),
			),
		),
	),
)

it.effect('partial fan-out reconstruction uses the saved attribution, not a reassigned directory', () =>
	Effect.gen(function* () {
		const retained = yield* Layer.build(memory({ maxMailboxes: 100 }))
		const store = Context.get(retained, MailboxStore)
		const first = yield* Ref.make(true)
		const calls = yield* Ref.make(0)
		const organizationId = yield* Ref.make('A')
		const organizations = Layer.succeed(
			SlackOrganizations,
			SlackOrganizations.of({
				resolve: () =>
					Ref.update(calls, (count) => count + 1).pipe(
						Effect.andThen(Ref.get(organizationId)),
						Effect.map((organizationId) => ({ organizationId })),
					),
			}),
		)
		const storage = Layer.mergeAll(
			Layer.succeed(MailboxReadiness, Context.get(retained, MailboxReadiness)),
			Layer.succeed(DeliveryLocatorStore, Context.get(retained, DeliveryLocatorStore)),
			Layer.succeed(
				MailboxStore,
				MailboxStore.of({
					...store,
					commitMailbox: (input) =>
						Effect.gen(function* () {
							if (input.key.includes('6:second') && (yield* Ref.getAndSet(first, false)))
								return yield* MailboxStoreError.make({ operation: 'commit' })
							return yield* store.commitMailbox(input)
						}),
				}),
			),
		)
		const handlers = {
			onNewMention: [
				{ id: 'first', handler: () => Effect.die('admission must not execute') },
				{ id: 'second', handler: () => Effect.die('admission must not execute') },
			],
		}
		const event = nativeMessage('a')
		yield* Effect.gen(function* () {
			const ingress = yield* SlackIngress
			yield* ingress.acceptMessage(event).pipe(Effect.flip)
			assert.strictEqual((yield* nativeMailbox('first', event))?.state.pending[0]?.organizationId, 'A')
			assert.strictEqual(yield* nativeMailbox('second', event), undefined)
		}).pipe(Effect.provide(nativeIngressLayer(handlers, storage).pipe(Layer.provide(organizations))))
		yield* Ref.set(organizationId, 'B')
		yield* Effect.gen(function* () {
			yield* (yield* SlackIngress).acceptMessage(event)
			for (const handler of ['first', 'second']) {
				const saved = yield* nativeMailbox(handler, event)
				assert.strictEqual(saved?.state.pending.length, 1)
				assert.strictEqual(saved?.state.pending[0]?.organizationId, 'A')
			}
		}).pipe(Effect.provide(nativeIngressLayer(handlers, storage).pipe(Layer.provide(organizations))))
		assert.strictEqual(yield* Ref.get(calls), 1)
	}),
)

for (const organizationId of ['default', 'fixed']) {
	it.effect(`${organizationId} organization is saved before the native handler receives it`, () =>
		Effect.gen(function* () {
			const calls = yield* Queue.unbounded<string>()
			const services = nativeIngressLayer({
				onNewMention: [
					{
						id: 'reply',
						handler: (_, context) => Queue.offer(calls, context.organizationId).pipe(Effect.asVoid),
					},
				],
			})
			const configured =
				organizationId === 'default'
					? services
					: services.pipe(Layer.provide(SlackOrganizations.fixed({ organizationId })))
			yield* Effect.gen(function* () {
				const ingress = yield* SlackIngress
				const event = nativeMessage('a')
				yield* ingress.acceptMessage(event)
				assert.strictEqual(
					(yield* nativeMailbox('reply', event))?.state.pending[0]?.organizationId,
					organizationId,
				)
				const worker = yield* ingress.run(nativeRunner).pipe(Effect.forkChild)
				assert.strictEqual(yield* Queue.take(calls), organizationId)
				yield* Fiber.interrupt(worker)
			}).pipe(Effect.provide(configured))
		}),
	)
}
