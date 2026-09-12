import { assert, it } from '@effect/vitest'
import { bind, HandlerFailure } from '@humanlayer/channels-delivery'
import { layer as memory } from '@humanlayer/channels-github/memory'
import { Context, Effect, Layer, Redacted, Schema } from 'effect'
import { FetchHttpClient } from 'effect/unstable/http'

import {
	GitHub,
	GitHubCommentData,
	GitHubCredentials,
	GitHubIngress,
	GitHubIssueData,
	GitHubRoutes,
	activityEventDefinition,
} from '../src/index.js'
import { policy } from './fixtures.js'
import { adminCall, emulator, eventFor, host, payloadFor, secret, signedRequest } from './support.js'

it.live('App-authenticated proactive create/read/update issue and comments without inbound services', () =>
	Effect.gen(function* () {
		const em = yield* emulator()
		yield* Effect.gen(function* () {
			const github = yield* GitHub
			const issue = yield* github.createIssue({
				repository: em.repository,
				title: 'Proactive',
				body: 'Native issue',
			})
			const comment = yield* github.createComment({ issue: issue.ref, body: 'First' })
			yield* github.updateComment({ comment: comment.ref, body: 'Updated' })
			yield* github.updateIssue({ issue: issue.ref, title: 'Updated issue', state: 'closed' })
			assert.equal((yield* github.getIssue({ issue: issue.ref })).data.state, 'closed')
			const visible = yield* adminCall(
				em.resource.url,
				`/repos/alice/project/issues/${issue.ref.number}/comments`,
				Schema.Array(GitHubCommentData),
			)
			assert.deepEqual(
				visible.map((entry) => entry.body),
				['Updated'],
			)
			const metadata = yield* adminCall(
				em.resource.url,
				'/_emulate/installation-tokens',
				Schema.Struct({
					installation_tokens: Schema.Array(
						Schema.Struct({
							installation: Schema.Struct({ id: Schema.Int }),
							repository_ids: Schema.Array(Schema.Int),
							permissions: Schema.Record(Schema.String, Schema.String),
						}),
					),
				}),
			)
			assert.equal(metadata.installation_tokens.length, 1)
			assert.deepEqual(metadata.installation_tokens[0]?.repository_ids, [em.repository.id])
			assert.deepEqual(metadata.installation_tokens[0]?.permissions, { issues: 'write' })
		}).pipe(Effect.provide(GitHub.layer.pipe(Layer.provideMerge(em.credentials))))
	}).pipe(Effect.provide(FetchHttpClient.layer)),
)

it.live(
	'signed ingress ACK commits without executing; stable rename identity, duplicate and native comment output',
	() =>
		Effect.gen(function* () {
			const em = yield* emulator()
			const data = yield* adminCall(em.resource.url, '/repos/alice/project/issues', GitHubIssueData, {
				title: 'Inbound',
				body: 'Hello',
			})
			const event = eventFor(em.repository, data)
			const storage = memory({ maxMailboxes: 10 })
			const handler = (
				event: typeof activityEventDefinition.event.Type,
				context: { readonly skipped: ReadonlyArray<typeof activityEventDefinition.event.Type> },
			) =>
				Effect.flatMap(GitHub, (github) =>
					github.createComment({
						issue: event.resource,
						body: `${event.deliveryId}:${context.skipped.map((e) => e.deliveryId).join(',')}`,
					}),
				).pipe(Effect.asVoid)
			const services = GitHubIngress.layer({
				namespace: 'test',
				policy,
				handlers: [{ id: 'reply', onCreation: handler }],
			}).pipe(Layer.provideMerge(GitHub.layer), Layer.provideMerge(storage), Layer.provideMerge(em.credentials))
			const environment = yield* Layer.build(services)
			const request = yield* host(
				GitHubRoutes.layer({ signingSecret: Redacted.make(secret), maxBodyBytes: 256_000 }).pipe(
					Layer.provide(Layer.succeedContext(environment)),
				),
			)
			assert.equal((yield* request(signedRequest('issues', JSON.stringify(payloadFor(event))))).status, 200)
			assert.deepEqual(
				yield* adminCall(
					em.resource.url,
					`/repos/alice/project/issues/${data.number}/comments`,
					Schema.Array(GitHubCommentData),
				),
				[],
			)
			const ingress = Context.get(environment, GitHubIngress)
			yield* ingress.processActivity({ event })
			assert.equal((yield* request(signedRequest('issues', JSON.stringify(payloadFor(event))))).status, 200)
			yield* ingress.processActivity({ event })
			const binding = bind({
				namespace: 'test',
				handlerId: '["reply","creation"]',
				policy,
				definition: activityEventDefinition,
				handler: () => Effect.fail(HandlerFailure.make({ retryable: false })),
			})
			assert.equal(
				yield* binding.keyFor({ event }),
				yield* binding.keyFor({
					event: {
						...event,
						resource: {
							...event.resource,
							repository: { ...event.resource.repository, name: 'renamed', owner: 'transferred' },
						},
					},
				}),
			)
			assert.equal(
				(yield* adminCall(
					em.resource.url,
					`/repos/alice/project/issues/${data.number}/comments`,
					Schema.Array(GitHubCommentData),
				)).length,
				1,
			)
			const credentials = Context.get(environment, GitHubCredentials)
			assert.equal(credentials.acceptsInstallation({ installationId: 101 }), false)
		}).pipe(Effect.provide(FetchHttpClient.layer)),
)
