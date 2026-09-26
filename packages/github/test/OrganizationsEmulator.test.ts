import { assert, it } from '@effect/vitest'
import { MailboxReadiness, MailboxStore } from '@humanlayer/channels-delivery'
import { Context, Deferred, Effect, Exit, Fiber, Layer, Logger, Match, Redacted, Ref, Schema, Scope } from 'effect'
import { FetchHttpClient } from 'effect/unstable/http'

import {
	GitHub,
	GitHubIngress,
	GitHubIssueData,
	GitHubOrganizationLookupError,
	GitHubOrganizations,
	GitHubRoutes,
	type GitHubActivityEvent,
} from '../src/index'
import { layer as memory } from '../src/memory'
import { policy } from './fixtures'
import { adminCall, captureWebhooks, emulator, eventFor, host, secret } from './support'

class SynchronousLookupDefect extends Schema.TaggedError<SynchronousLookupDefect>()('SynchronousLookupDefect', {
	detail: Schema.String,
}) {}

for (const callback of ['onCreation', 'onMention'] as const) {
	it.live(
		`${callback} generated signed GitHub events persist organization and produce native comments`,
		() =>
			Effect.gen(function* () {
				const capture = yield* captureWebhooks
				const em = yield* emulator(capture.url)
				const mapping = yield* Ref.make<'A' | 'B' | 'unknown' | 'failed' | 'defect' | 'cancel'>('A')
				const lookups = yield* Ref.make(0)
				const entered = yield* Deferred.make<void>()
				const finalized = yield* Deferred.make<void>()
				const logs: string[] = []
				const logger = Logger.layer([
					Logger.make((entry) => logs.push(JSON.stringify(Logger.formatStructured.log(entry)))),
				])
				let synchronousThrow = false
				const organizations = GitHubOrganizations.layer(({ installationId }) => {
					if (synchronousThrow) throw SynchronousLookupDefect.make({ detail: 'private-synchronous-sentinel' })
					return Effect.gen(function* () {
						assert.strictEqual(installationId, 100)
						yield* Ref.update(lookups, (count) => count + 1)
						const current = yield* Ref.get(mapping)
						if (current === 'failed') return yield* GitHubOrganizationLookupError.make({})
						if (current === 'defect') return yield* Effect.die('private-defect-sentinel')
						if (current === 'cancel')
							return yield* Deferred.succeed(entered, undefined).pipe(
								Effect.andThen(Effect.never),
								Effect.ensuring(Deferred.succeed(finalized, undefined)),
							)
						return current === 'unknown' ? null : { organizationId: current }
					})
				})
				const respond = (
					event: { readonly resource: GitHubActivityEvent['resource'] },
					context: { readonly organizationId: string },
				) =>
					Effect.flatMap(GitHub, (github) =>
						github.createComment({ issue: event.resource, body: `organization=${context.organizationId}` }),
					).pipe(Effect.asVoid)
				const registration = {
					handlers: [
						callback === 'onCreation'
							? { id: 'reply', onCreation: respond }
							: { id: 'reply', onMention: respond },
					],
				}
				const environment = yield* Layer.build(
					GitHubIngress.layer({ namespace: 'organization-emulator', policy, ...registration }).pipe(
						Layer.provideMerge(memory({ maxMailboxes: 30 })),
						Layer.provideMerge(GitHub.layer.pipe(Layer.provideMerge(em.credentials))),
						Layer.provide(organizations),
						Layer.provide(logger),
					),
				)
				const ingress = Context.get(environment, GitHubIngress)
				const send = yield* host(
					GitHubRoutes.layer({
						signingSecret: Redacted.make(secret),
						maxBodyBytes: 256000,
						botLogin: 'channels[bot]',
					}).pipe(Layer.provide(Layer.succeedContext(environment)), Layer.provide(logger)),
				)
				const issue = yield* adminCall(em.resource.url, '/repos/alice/project/issues', GitHubIssueData, {
					title: 'Organization',
					body: '@channels[bot] please reply',
				})
				const request = yield* capture.take
				const deliveryId = request.headers.get('x-github-delivery')
				assert.ok(deliveryId !== null)
				const duplicate = request.clone()
				const invalidSignature = request.clone()
				invalidSignature.headers.set('x-hub-signature-256', `sha256=${'0'.repeat(64)}`)
				assert.strictEqual((yield* send(invalidSignature)).status, 401)
				assert.strictEqual(yield* Ref.get(lookups), 0)
				assert.strictEqual((yield* send(request)).status, 200)
				const readiness = Context.get(environment, MailboxReadiness)
				const [key] = yield* readiness.scanReady({ prefix: '', now: Number.MAX_SAFE_INTEGER, limit: 10 })
				assert.ok(key !== undefined)
				const store = Context.get(environment, MailboxStore)
				assert.strictEqual((yield* store.loadMailbox({ key }))?.state.pending[0]?.organizationId, 'A')
				yield* Ref.set(mapping, 'B')
				assert.strictEqual((yield* send(duplicate)).status, 200)
				assert.strictEqual(yield* Ref.get(lookups), 1)
				const event = eventFor(em.repository, issue, deliveryId)
				yield* ingress.processActivity({ event }).pipe(Effect.provide(environment))
				assert.strictEqual((yield* store.loadMailbox({ key }))?.state.outcomes.length, 1)
				assert.deepStrictEqual(
					(yield* Context.get(environment, GitHub).listComments({ issue: event.resource })).map(
						(comment) => comment.data.body,
					),
					['organization=A'],
				)
				yield* capture.take
				for (const current of ['unknown', 'failed', 'defect', 'throw'] as const) {
					synchronousThrow = current === 'throw'
					yield* Ref.set(mapping, current === 'throw' ? 'defect' : current)
					const rejected = yield* adminCall(em.resource.url, '/repos/alice/project/issues', GitHubIssueData, {
						title: current,
						body: '@channels[bot] please reply',
					})
					const response = yield* send(yield* capture.take)
					assert.strictEqual(
						response.status,
						Match.value(current).pipe(
							Match.when('unknown', () => 200),
							Match.when('failed', () => 503),
							Match.orElse(() => 500),
						),
					)
					assert.strictEqual(yield* Effect.promise(() => response.text()), '')
					assert.deepStrictEqual(
						yield* readiness.scanReady({ prefix: '', now: Number.MAX_SAFE_INTEGER, limit: 10 }),
						[],
					)
					const rejectedEvent = eventFor(em.repository, rejected)
					yield* ingress.processActivity({ event: rejectedEvent }).pipe(Effect.provide(environment))
					assert.deepStrictEqual(
						yield* Context.get(environment, GitHub).listComments({ issue: rejectedEvent.resource }),
						[],
					)
				}
				assert.ok(logs.some((log) => log.includes('unexpected_defect')))
				assert.ok(logs.every((log) => !log.includes('private-')))
				synchronousThrow = false
				yield* Ref.set(mapping, 'cancel')
				yield* adminCall(em.resource.url, '/repos/alice/project/issues', GitHubIssueData, {
					title: 'Cancel',
					body: '@channels[bot] please reply',
				})
				const cancellation = yield* Scope.make()
				const signal = yield* Effect.abortSignal.pipe(Scope.provide(cancellation))
				const cancelRequest = new Request(yield* capture.take, { signal })
				const logCount = logs.length
				const sending = yield* send(cancelRequest).pipe(Effect.forkChild)
				yield* Deferred.await(entered)
				yield* Scope.close(cancellation, Exit.void)
				assert.strictEqual((yield* Fiber.join(sending)).status, 499)
				yield* Deferred.await(finalized)
				assert.strictEqual(logs.length, logCount)
				assert.deepStrictEqual(
					yield* readiness.scanReady({ prefix: '', now: Number.MAX_SAFE_INTEGER, limit: 10 }),
					[],
				)
			}).pipe(Effect.provide(FetchHttpClient.layer)),
		{ timeout: 15000 },
	)
}
