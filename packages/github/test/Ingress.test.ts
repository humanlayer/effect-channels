import { assert, it } from '@effect/vitest'
import { bind, MailboxStore, MailboxStoreError } from '@humanlayer/channels-delivery'
import { layer as memory } from '@humanlayer/channels-github/memory'
import { Context, Deferred, Effect, Fiber, Layer, Queue, Redacted, Ref, Schema } from 'effect'

import { GitHubCrypto, GitHubIngress, GitHubIssueEvent, GitHubRoutes, issueEventDefinition } from '../src/index.js'
import { policy } from './fixtures.js'
import { event, routeCredentials, user } from './fixtures.js'
import { host, payloadFor, secret, signedRequest } from './support.js'

it.live(
	'rejects unsigned, tampered, malformed, oversized and wrong-installation requests; ignores self/PR/unsupported events',
	() =>
		Effect.gen(function* () {
			const storage = memory({ maxMailboxes: 20 })
			const services = GitHubIngress.layer({
				namespace: 'security',
				policy,
				handlers: [{ id: 'handler', handler: () => Effect.void }],
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
			const body = JSON.stringify(payload)
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
					signedRequest(
						'issues',
						JSON.stringify({ ...payload, repository: { ...payload.repository, name: '..' } }),
					),
				)).status,
				400,
			)
			assert.equal((yield* request(signedRequest('issues', 'x'.repeat(2_001)))).status, 413)
			assert.equal(
				(yield* request(signedRequest('issues', JSON.stringify({ ...payload, installation: { id: 200 } }))))
					.status,
				403,
			)
			for (const ignored of [
				{ ...payload, sender: { ...user, id: 99 } },
				{ ...payload, issue: { ...event.issue, pull_request: { url: 'https://api.github.test/pr' } } },
				{ ...payload, action: 'assigned' },
			])
				assert.equal((yield* request(signedRequest('issues', JSON.stringify(ignored)))).status, 200)
			assert.equal((yield* request(signedRequest('ping', '{}'))).status, 200)
			const binding = bind({
				namespace: 'security',
				handlerId: 'handler',
				policy,
				definition: issueEventDefinition,
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
		const writes = yield* Ref.make(0)
		const seen = yield* Queue.unbounded<string>()
		const fault = Layer.succeed(
			MailboxStore,
			MailboxStore.of({
				loadMailbox: underlying.loadMailbox,
				commitMailbox: (input) =>
					Ref.updateAndGet(writes, (n) => n + 1).pipe(
						Effect.flatMap((n) =>
							n === 2
								? Effect.fail(MailboxStoreError.make({ operation: 'commit' }))
								: underlying.commitMailbox(input),
						),
					),
			}),
		)
		const services = GitHubIngress.layer({
			namespace: 'fanout',
			policy,
			handlers: ['one', 'two'].map((id) => ({ id, handler: () => Queue.offer(seen, id).pipe(Effect.asVoid) })),
		}).pipe(Layer.provide(fault), Layer.provide(Layer.succeedContext(storage)))
		const environment = yield* Layer.build(services)
		const request = yield* host(
			GitHubRoutes.layer({ signingSecret: Redacted.make(secret), maxBodyBytes: 5_000 }).pipe(
				Layer.provide(Layer.succeedContext(environment)),
				Layer.provide(routeCredentials),
				Layer.provide(GitHubCrypto.layerWebCrypto),
			),
		)
		const send = () => request(signedRequest('issues', JSON.stringify(payloadFor(event)), event.deliveryId))
		assert.equal((yield* send()).status, 503)
		assert.equal((yield* send()).status, 200)
		const ingress = Context.get(environment, GitHubIngress)
		yield* ingress.process({ event })
		assert.deepEqual([yield* Queue.take(seen), yield* Queue.take(seen)], ['one', 'two'])
		assert.equal((yield* send()).status, 200)
		yield* ingress.process({ event })
		assert.equal(yield* Queue.size(seen), 0)
	}),
)

it.effect(
	'queue holds active work, reconstructs native comment latest/skipped and isolates installation/repository/handler',
	() =>
		Effect.gen(function* () {
			const started = yield* Deferred.make<void>()
			const release = yield* Deferred.make<void>()
			const seen = yield* Queue.unbounded<ReadonlyArray<string>>()
			const handler = (value: GitHubIssueEvent, context: { readonly skipped: ReadonlyArray<GitHubIssueEvent> }) =>
				Effect.gen(function* () {
					yield* Queue.offer(seen, [value.deliveryId, ...context.skipped.map((e) => e.deliveryId)])
					if (value.deliveryId === 'event-a') {
						yield* Deferred.succeed(started, undefined)
						yield* Deferred.await(release)
					}
				})
			const make = () =>
				bind({ namespace: 'queue', handlerId: 'one', definition: issueEventDefinition, policy, handler })
			const binding = make()
			const { key } = yield* binding.admit({ event })
			const fiber = yield* binding.processMailbox({ key }).pipe(Effect.forkChild)
			yield* Deferred.await(started)
			for (const id of ['b', 'c', 'd']) {
				const comment = GitHubIssueEvent.make({
					...event,
					event: 'issue_comment',
					action: 'created',
					deliveryId: id,
					comment: { id: 60, body: id, html_url: 'https://test/comment', user },
				})
				assert.equal((yield* binding.admit({ event: comment })).key, key)
			}
			yield* Deferred.succeed(release, undefined)
			yield* Fiber.join(fiber)
			yield* make().processMailbox({ key })
			assert.deepEqual([yield* Queue.take(seen), yield* Queue.take(seen)], [['event-a'], ['d', 'b', 'c']])
			const codec = Schema.fromJsonString(GitHubIssueEvent)
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
		}).pipe(Effect.provide(memory({ maxMailboxes: 10 }))),
)
