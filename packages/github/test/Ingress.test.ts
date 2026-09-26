import { assert, it } from '@effect/vitest'
import {
	bind,
	DeliveryQueue,
	enqueueDelivery,
	MailboxStore,
	MailboxStoreError,
	mailboxKey,
} from '@humanlayer/channels-delivery'
import { layer as memory } from '@humanlayer/channels-github/memory'
import { Context, Deferred, Effect, Fiber, Layer, Queue, Redacted, Ref, Schema } from 'effect'

import {
	GitHubCrypto,
	GitHubIngress,
	GitHubActivityEvent,
	GitHubRoutes,
	GitHubSubscriptions,
	activityEventDefinition,
	issueResourceKey,
} from '../src/index'
import { policy } from './fixtures'
import { event, routeCredentials, user } from './fixtures'
import { encodeWebhookBody, host, payloadFor, secret, signedRequest, webhookRequest } from './support'

it.live(
	'rejects unsigned, tampered, malformed, oversized and wrong-installation requests; ignores self/unsupported events',
	() =>
		Effect.gen(function* () {
			const storage = memory({ maxMailboxes: 20 })
			const services = GitHubIngress.layer({
				namespace: 'security',
				policy,
				handlers: [{ id: 'handler', onCreation: () => Effect.void }],
			}).pipe(Layer.provideMerge(storage))
			const environment = yield* Layer.build(services)
			const request = yield* host(
				GitHubRoutes.layer({ signingSecret: Redacted.make(secret), maxBodyBytes: 2_000 }).pipe(
					Layer.provide(Layer.succeedContext(environment)),
					Layer.provide(routeCredentials),
					Layer.provide(GitHubCrypto.layerWebCrypto),
				),
			)
			const payload = payloadFor(event)
			const body = yield* encodeWebhookBody(payload)
			const bad = signedRequest('issues', body)
			bad.headers.set('x-hub-signature-256', `sha256=${'0'.repeat(64)}`)
			assert.equal((yield* request(bad)).status, 401)
			const unsigned = signedRequest('issues', body)
			unsigned.headers.delete('x-hub-signature-256')
			assert.equal((yield* request(unsigned)).status, 401)
			const original = signedRequest('issues', body)
			assert.equal(
				(yield* request(
					new Request(original.url, { method: 'POST', headers: original.headers, body: `${body} ` }),
				)).status,
				401,
			)
			const missing = signedRequest('issues', body)
			missing.headers.delete('x-github-delivery')
			assert.equal((yield* request(missing)).status, 401)
			assert.equal((yield* request(signedRequest('issues', '{'))).status, 400)
			assert.equal((yield* request(signedRequest('issues', new Uint8Array([0xff])))).status, 400)
			assert.equal(
				(yield* request(
					yield* webhookRequest('issues', { ...payload, repository: { ...payload.repository, name: '..' } }),
				)).status,
				400,
			)
			assert.equal((yield* request(signedRequest('issues', 'x'.repeat(2_001)))).status, 413)
			assert.equal(
				(yield* request(yield* webhookRequest('issues', { ...payload, installation: { id: 200 } }))).status,
				403,
			)
			for (const ignored of [
				{ ...payload, sender: { ...user, id: 99 } },
				{ ...payload, action: 'assigned' },
			])
				assert.equal(
					(yield* request(
						yield* webhookRequest('issues', ignored, ignored.sender?.id === 99 ? 'self' : 'assigned'),
					)).status,
					200,
				)
			assert.equal((yield* request(signedRequest('ping', '{}'))).status, 200)
			const binding = bind({
				namespace: 'security',
				handlerId: '["handler","creation"]',
				policy,
				definition: activityEventDefinition,
				handler: () => Effect.void,
			})
			const store = Context.get(environment, MailboxStore)
			assert.equal(yield* store.loadMailbox({ key: yield* binding.keyFor({ event }) }), undefined)
			assert.equal((yield* request(signedRequest('issues', body))).status, 200)
			assert.equal((yield* store.loadMailbox({ key: yield* binding.keyFor({ event }) }))?.state.pending.length, 1)
		}),
)

it.live('partial fanout returns 503; retry fills missing admission, then each handler executes once', () =>
	Effect.gen(function* () {
		const storage = yield* Layer.build(memory({ maxMailboxes: 10 }))
		const underlying = Context.get(storage, MailboxStore)
		const failSecond = yield* Ref.make(true)
		const keyFor = (id: string) =>
			mailboxKey({
				namespace: 'fanout',
				provider: 'github',
				handlerId: `["${id}","creation"]`,
				installation: '100',
				resourceKey: issueResourceKey(event.resource),
			})
		const seen = yield* Queue.unbounded<string>()
		const faultStore = MailboxStore.of({
			loadMailbox: underlying.loadMailbox,
			commitMailbox: (input) =>
				Effect.gen(function* () {
					if (input.key === keyFor('two') && (yield* Ref.getAndSet(failSecond, false)))
						return yield* MailboxStoreError.make({ operation: 'commit' })
					return yield* underlying.commitMailbox(input)
				}),
		})
		const fault = Layer.succeed(MailboxStore, faultStore)
		const faultQueue = DeliveryQueue.of({
			enqueue: (input) => enqueueDelivery(input).pipe(Effect.provideService(MailboxStore, faultStore)),
		})
		const services = GitHubIngress.layer({
			namespace: 'fanout',
			policy,
			handlers: ['one', 'two'].map((id) => ({ id, onCreation: () => Queue.offer(seen, id).pipe(Effect.asVoid) })),
		}).pipe(
			Layer.provide(fault),
			Layer.provide(Layer.succeed(DeliveryQueue, faultQueue)),
			Layer.provide(Layer.succeedContext(Context.add(storage, DeliveryQueue, faultQueue))),
		)
		const environment = yield* Layer.build(services)
		const request = yield* host(
			GitHubRoutes.layer({ signingSecret: Redacted.make(secret), maxBodyBytes: 5_000 }).pipe(
				Layer.provide(Layer.succeedContext(environment)),
				Layer.provide(routeCredentials),
				Layer.provide(GitHubCrypto.layerWebCrypto),
			),
		)
		const send = () => webhookRequest('issues', payloadFor(event), event.deliveryId).pipe(Effect.flatMap(request))
		assert.equal((yield* send()).status, 503)
		assert.equal((yield* underlying.loadMailbox({ key: keyFor('one') }))?.state.pending.length, 1)
		assert.equal(yield* underlying.loadMailbox({ key: keyFor('two') }), undefined)
		assert.equal((yield* send()).status, 200)
		const ingress = Context.get(environment, GitHubIngress)
		yield* ingress.processActivity({ event }).pipe(Effect.provideService(MailboxStore, faultStore))
		assert.deepEqual([yield* Queue.take(seen), yield* Queue.take(seen)], ['one', 'two'])
		assert.equal((yield* send()).status, 200)
		yield* ingress.processActivity({ event }).pipe(Effect.provideService(MailboxStore, faultStore))
		assert.equal(yield* Queue.size(seen), 0)
	}),
)

it.effect(
	'serial ingress holds active work, reconstructs every native comment and isolates installation/repository/handler',
	() =>
		Effect.gen(function* () {
			const retained = yield* Layer.build(memory({ maxMailboxes: 20 }))
			const started = yield* Deferred.make<void>()
			const release = yield* Deferred.make<void>()
			const seen = yield* Queue.unbounded<ReadonlyArray<string>>()
			const handler = (
				value: GitHubActivityEvent,
				context: { readonly skipped: ReadonlyArray<GitHubActivityEvent> },
			) =>
				Effect.gen(function* () {
					yield* Queue.offer(seen, [value.deliveryId, ...context.skipped.map((e) => e.deliveryId)])
					if (value.deliveryId === 'event-a') {
						yield* Deferred.succeed(started, undefined)
						yield* Deferred.await(release)
					}
				})
			const make = () =>
				GitHubIngress.layer({
					namespace: 'queue',
					policy,
					handlers: [{ id: 'one', onSubscribedEvent: handler }],
				}).pipe(Layer.provide(Layer.succeedContext(retained)))
			const ingress = Context.get(yield* Layer.build(make()), GitHubIngress)
			yield* Context.get(retained, GitHubSubscriptions).subscribe({
				namespace: 'queue',
				resource: event.resource,
			})
			const binding = bind({
				namespace: 'queue',
				handlerId: '["one","subscribed"]',
				definition: activityEventDefinition,
				policy,
				handler: () => Effect.void,
			})
			const key = yield* binding.keyFor({ event })
			yield* ingress.acceptActivity({ event, mentioned: false, own: false })
			const fiber = yield* ingress.processActivity({ event }).pipe(Effect.provide(retained), Effect.forkChild)
			yield* Deferred.await(started)
			for (const id of ['b', 'c', 'd']) {
				const comment = GitHubActivityEvent.make({
					...event,
					event: 'issue_comment',
					action: 'created',
					deliveryId: id,
					comment: { id: 60, body: id, html_url: 'https://test/comment', user },
				})
				yield* ingress.acceptActivity({ event: comment, mentioned: false, own: false })
				assert.equal(yield* binding.keyFor({ event: comment }), key)
			}
			yield* Deferred.succeed(release, undefined)
			yield* Fiber.join(fiber)
			const store = Context.get(retained, MailboxStore)
			assert.equal((yield* store.loadMailbox({ key }))?.state.pending.length, 3)
			const reconstructed = Context.get(yield* Layer.build(make()), GitHubIngress)
			for (const _id of ['b', 'c', 'd'])
				yield* reconstructed.processActivity({ event }).pipe(Effect.provide(retained))
			assert.deepEqual(yield* Queue.takeAll(seen), [['event-a'], ['b'], ['c'], ['d']])
			assert.equal((yield* store.loadMailbox({ key }))?.state.outcomes.length, 4)
			assert.equal((yield* store.loadMailbox({ key }))?.state.pending.length, 0)
			assert.notEqual(
				key,
				yield* bind({
					namespace: 'queue',
					handlerId: '["two","subscribed"]',
					definition: activityEventDefinition,
					policy,
					handler: () => Effect.void,
				}).keyFor({ event }),
			)
			const codec = Schema.fromJsonString(GitHubActivityEvent)
			assert.deepEqual(yield* Schema.decodeEffect(codec)(yield* Schema.encodeEffect(codec)(event)), event)
			assert.notEqual(
				key,
				yield* binding.keyFor({
					event: {
						...event,
						resource: {
							...event.resource,
							repository: { ...event.resource.repository, installationId: 101 },
						},
					},
				}),
			)
			assert.notEqual(
				key,
				yield* binding.keyFor({
					event: {
						...event,
						resource: { ...event.resource, repository: { ...event.resource.repository, id: 21 } },
					},
				}),
			)
		}),
)

it.effect('canonical handlers recover existing activity mailbox IDs and saved attribution without rerouting', () =>
	Effect.gen(function* () {
		const retained = yield* Layer.build(memory({ maxMailboxes: 10 }))
		const storage = Layer.succeedContext(retained)
		const seen = yield* Queue.unbounded<string>()
		const keys: string[] = []
		for (const route of ['creation', 'mention', 'subscribed'] as const) {
			const oldBinding = bind({
				namespace: 'retained',
				handlerId: `["receive","${route}"]`,
				definition: activityEventDefinition,
				policy: { ...policy, mode: 'serial' },
				handler: () => Effect.die('The old binding must never execute'),
			})
			const admitted = yield* oldBinding
				.admit({ event, organizationId: 'original' })
				.pipe(Effect.provide(storage))
			keys.push(admitted.key)
		}
		const record = (route: string) => (_event: GitHubActivityEvent, context: { readonly organizationId: string }) =>
			Queue.offer(seen, `${route}:${context.organizationId}`).pipe(Effect.asVoid)
		const environment = yield* Layer.build(
			GitHubIngress.layer({
				namespace: 'retained',
				policy,
				handlers: [
					{
						id: 'receive',
						onCreation: record('creation'),
						onMention: record('mention'),
						onSubscribedEvent: record('subscribed'),
					},
				],
			}).pipe(Layer.provide(storage)),
		)
		yield* Context.get(environment, GitHubIngress).processActivity({ event }).pipe(Effect.provide(retained))
		assert.deepEqual(yield* Queue.takeAll(seen), ['creation:original', 'mention:original', 'subscribed:original'])
		for (const key of keys) {
			const saved = yield* Context.get(retained, MailboxStore).loadMailbox({ key })
			assert.equal(saved?.state.pending.length, 0)
			assert.equal(saved?.state.outcomes[0]?.kind, 'completed')
		}
	}),
)

it.live('without bot login, normalized PR creation and followed lifecycle work but mentions do not', () =>
	Effect.gen(function* () {
		const seen = yield* Queue.unbounded<string>()
		const environment = yield* Layer.build(
			GitHubIngress.layer({
				namespace: 'no-login',
				policy,
				handlers: [
					{
						id: 'receive',
						onCreation: (event) => Queue.offer(seen, `creation:${event.resource.kind}`).pipe(Effect.asVoid),
						onMention: () => Effect.die('Mention detection requires botLogin'),
						onSubscribedEvent: (event) => Queue.offer(seen, event.action).pipe(Effect.asVoid),
					},
				],
			}).pipe(Layer.provideMerge(memory({ maxMailboxes: 10 }))),
		)
		const send = yield* host(
			GitHubRoutes.layer({ signingSecret: Redacted.make(secret), maxBodyBytes: 5000 }).pipe(
				Layer.provide(Layer.succeedContext(environment)),
				Layer.provide(routeCredentials),
				Layer.provide(GitHubCrypto.layerWebCrypto),
			),
		)
		const pr: GitHubActivityEvent = {
			event: 'pull_request',
			action: 'opened',
			deliveryId: 'no-login-pr',
			resource: { ...event.resource, kind: 'github.pull-request' },
			pull_request: { ...event.issue, body: '@channels help', merged: false },
			sender: user,
		}
		const ingress = Context.get(environment, GitHubIngress)
		assert.equal((yield* send(yield* webhookRequest(pr.event, payloadFor(pr), pr.deliveryId))).status, 200)
		yield* ingress.processActivity({ event: pr }).pipe(Effect.provide(environment))
		assert.deepEqual(yield* Queue.takeAll(seen), ['creation:github.pull-request'])
		yield* Context.get(environment, GitHubSubscriptions).subscribe({ namespace: 'no-login', resource: pr.resource })
		assert.equal(
			(yield* send(yield* webhookRequest(pr.event, { ...payloadFor(pr), action: 'closed' }, 'no-login-close')))
				.status,
			200,
		)
		yield* ingress.processActivity({ event: pr }).pipe(Effect.provide(environment))
		assert.deepEqual(yield* Queue.takeAll(seen), ['closed'])
	}),
)
