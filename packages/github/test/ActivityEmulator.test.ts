import { assert, it } from '@effect/vitest'
import { Context, Effect, Layer, Queue, Redacted, Schema } from 'effect'
import { FetchHttpClient, HttpClient, HttpClientRequest, HttpClientResponse } from 'effect/unstable/http'

import {
	GitHubActivityEvent,
	GitHubIngress,
	GitHubIssueData,
	GitHubPullRequestData,
	GitHubRoutes,
	GitHubSubscriptions,
	GitHubUser,
} from '../src/index.js'
import { layer as memory } from '../src/memory.js'
import { policy } from './fixtures.js'
import { adminCall, captureWebhooks, emulator, eventFor, host, secret } from './support.js'

it.live('native emulator creation selects subscriptions; issue and PR lifecycle state remains provider-visible', () =>
	Effect.gen(function* () {
		const capture = yield* captureWebhooks
		const em = yield* emulator(capture.url)
		const seen = yield* Queue.unbounded<GitHubActivityEvent>()
		const storage = memory({ maxMailboxes: 30 })
		const environment = yield* Layer.build(
			GitHubIngress.layer({
				namespace: 'emulator-activity',
				policy,
				handlers: [
					{
						id: 'observe',
						onCreation: (event) =>
							Effect.gen(function* () {
								const subscriptions = yield* GitHubSubscriptions
								yield* subscriptions.subscribe({
									namespace: 'emulator-activity',
									resource: event.resource,
								})
								yield* Queue.offer(seen, event)
							}),
						onSubscribedEvent: (event) => Queue.offer(seen, event).pipe(Effect.asVoid),
					},
				],
			}).pipe(Layer.provideMerge(storage), Layer.provideMerge(em.credentials)),
		)
		const ingress = Context.get(environment, GitHubIngress)
		const send = yield* host(
			GitHubRoutes.layer({
				signingSecret: Redacted.make(secret),
				maxBodyBytes: 256_000,
				botLogin: 'channels[bot]',
			}).pipe(Layer.provide(Layer.succeedContext(environment))),
		)
		const issue = yield* adminCall(em.resource.url, '/repos/alice/project/issues', GitHubIssueData, {
			title: 'Follow creation without mention',
			body: 'No mention',
		})
		assert.equal((yield* send(yield* capture.take)).status, 200)
		yield* ingress.processActivity({ event: eventFor(em.repository, issue) })
		assert.equal((yield* Queue.take(seen)).action, 'opened')
		const pr = yield* adminCall(em.resource.url, '/repos/alice/project/pulls', GitHubPullRequestData, {
			title: 'Native PR',
			head: 'feature',
			base: 'main',
			body: 'No mention',
		})
		const prEvent: GitHubActivityEvent = {
			event: 'pull_request',
			action: 'opened',
			deliveryId: 'pr-key',
			resource: { kind: 'github.pull-request', repository: em.repository, number: pr.number },
			pull_request: pr,
			sender: yield* Schema.decodeUnknownEffect(GitHubUser)(pr.user),
		}
		assert.equal((yield* send(yield* capture.take)).status, 200)
		yield* ingress.processActivity({ event: prEvent })
		assert.equal((yield* Queue.take(seen)).action, 'opened')
		const client = yield* HttpClient.HttpClient
		for (const target of [
			{ path: `issues/${issue.number}`, event: eventFor(em.repository, issue) },
			{ path: `pulls/${pr.number}`, event: prEvent },
		]) {
			for (const state of ['closed', 'open']) {
				const request = yield* HttpClientRequest.bodyJson(
					HttpClientRequest.patch(`${em.resource.url}/repos/alice/project/${target.path}`).pipe(
						HttpClientRequest.bearerToken('test_token_admin'),
					),
					{ state },
				)
				const response = yield* client.execute(request)
				assert.equal(response.status, 200)
				assert.equal((yield* HttpClientResponse.schemaBodyJson(GitHubIssueData)(response)).state, state)
				assert.equal((yield* send(yield* capture.take)).status, 200)
				yield* ingress.processActivity({ event: target.event })
				assert.equal((yield* Queue.take(seen)).action, state === 'closed' ? 'closed' : 'reopened')
			}
		}
		const review = yield* adminCall(
			em.resource.url,
			`/repos/alice/project/pulls/${pr.number}/reviews`,
			Schema.Struct({ id: Schema.Int, state: Schema.String }),
			{ body: 'Native review', event: 'COMMENT' },
		)
		assert.equal(review.state, 'COMMENTED')
		assert.equal((yield* send(yield* capture.take)).status, 400)
		const reviewers = yield* adminCall(
			em.resource.url,
			`/repos/alice/project/pulls/${pr.number}/requested_reviewers`,
			Schema.Struct({ requested_reviewers: Schema.Array(Schema.Struct({ login: Schema.String })) }),
			{ reviewers: ['alice'] },
		)
		assert.equal(reviewers.requested_reviewers[0]?.login, 'alice')
		assert.equal((yield* send(yield* capture.take)).status, 400)
		const comment = yield* adminCall(
			em.resource.url,
			`/repos/alice/project/pulls/${pr.number}/comments`,
			Schema.Struct({ id: Schema.Int, in_reply_to_id: Schema.NullOr(Schema.Int) }),
			{ body: 'Native inline root', path: 'README.md', line: 1, side: 'RIGHT' },
		)
		assert.equal(comment.in_reply_to_id, null)
		assert.equal((yield* send(yield* capture.take)).status, 400)
		assert.equal(yield* Queue.size(seen), 0)
		const mergeRequest = yield* HttpClientRequest.bodyJson(
			HttpClientRequest.put(`${em.resource.url}/repos/alice/project/pulls/${pr.number}/merge`).pipe(
				HttpClientRequest.bearerToken('test_token_admin'),
			),
			{},
		)
		const merged = yield* client.execute(mergeRequest)
		assert.equal(merged.status, 200)
		assert.equal(
			(yield* HttpClientResponse.schemaBodyJson(Schema.Struct({ merged: Schema.Boolean }))(merged)).merged,
			true,
		)
		assert.equal((yield* send(yield* capture.take)).status, 200)
		yield* ingress.processActivity({ event: prEvent })
		const mergeEvent = yield* Queue.take(seen)
		assert.equal(mergeEvent.event, 'pull_request')
		if (mergeEvent.event === 'pull_request' && mergeEvent.action === 'closed')
			assert.equal(mergeEvent.pull_request.merged, true)
		else assert.fail('Expected native closed/merged event')
	}).pipe(Effect.provide(FetchHttpClient.layer)),
)
