import { assert, it } from '@effect/vitest'
import { bind, MailboxStore } from '@humanlayer/channels-delivery'
import { layer as memory } from '@humanlayer/channels-github/memory'
import { Effect, Layer, Redacted, Schema } from 'effect'
import { HttpClient, HttpClientResponse } from 'effect/unstable/http'

import {
	GitHub,
	GitHubCredentials,
	GitHubActivityEvent,
	GitHubIngress,
	GitHubIngressError,
	activityEventDefinition,
} from '../src/index'
import { event, policy, user, unusedGitHub } from './fixtures'

it.effect('comment updates reject mismatched native identities before issuing a mutation', () =>
	Effect.gen(function* () {
		const origin = 'https://api.github.test'
		const parent = `${origin}/repos/alice/project/issues/1`
		for (const existing of [
			{ id: 60, issue_url: `${origin}/repos/other/project/issues/1` },
			{ id: 60, issue_url: 'https://other.test/repos/alice/project/issues/1' },
			{ id: 61, issue_url: parent },
		]) {
			const requests: Array<string> = []
			const http = Layer.succeed(
				HttpClient.HttpClient,
				HttpClient.make((request) =>
					Effect.sync(() => {
						requests.push(`${request.method} ${request.url}`)
						return HttpClientResponse.fromWeb(
							request,
							Response.json(request.url === parent ? event.issue : existing),
						)
					}),
				),
			)
			const credentials = Layer.mock(GitHubCredentials, {
				apiUrl: origin,
				botUserId: 99,
				acceptsInstallation: () => true,
				token: () => Effect.succeed(Redacted.make('test-token')),
			})
			const error = yield* Effect.gen(function* () {
				const github = yield* GitHub
				return yield* github.updateComment({
					comment: { kind: 'github.issue-comment', issue: event.resource, id: 60 },
					body: 'Must not be written',
				})
			}).pipe(Effect.provide(GitHub.layer.pipe(Layer.provide(Layer.merge(http, credentials)))), Effect.flip)
			assert.equal(error.reason, 'invalid_input')
			assert.deepEqual(requests, [`GET ${parent}`, `GET ${origin}/repos/alice/project/issues/comments/60`])
		}
	}),
)

it.effect('rejects contradictory persisted event identities before mailbox admission', () =>
	Effect.gen(function* () {
		const binding = bind({
			namespace: 'identity',
			handlerId: '["receive","creation"]',
			definition: activityEventDefinition,
			policy,
			handler: () => Effect.die('Invalid events must not execute'),
		})
		const ingress = yield* GitHubIngress
		const mismatches: ReadonlyArray<GitHubActivityEvent> = [
			{ ...event, issue: { ...event.issue, number: event.resource.number + 1 } },
			{ ...event, issue: { ...event.issue, pull_request: { url: 'https://test/pulls/1' } } },
			{
				...event,
				event: 'issue_comment',
				action: 'created',
				resource: { ...event.resource, kind: 'github.pull-request' },
				comment: { id: 60, body: 'Hello', html_url: 'https://test/comments/60', user },
			},
			{
				event: 'pull_request',
				action: 'opened',
				deliveryId: event.deliveryId,
				resource: { ...event.resource, kind: 'github.pull-request' },
				pull_request: { ...event.issue, number: event.resource.number + 1 },
				sender: user,
			},
		]
		for (const value of mismatches) {
			assert.equal(Schema.is(GitHubActivityEvent)(value), false)
			assert.deepEqual(
				yield* ingress.acceptActivity({ event: value, mentioned: false, own: false }).pipe(Effect.flip),
				GitHubIngressError.make({ operation: 'admit' }),
			)
			const store = yield* MailboxStore
			assert.equal(yield* store.loadMailbox({ key: yield* binding.keyFor({ event: value }) }), undefined)
		}
		const codec = Schema.fromJsonString(GitHubActivityEvent)
		assert.deepEqual(yield* Schema.decodeEffect(codec)(yield* Schema.encodeEffect(codec)(event)), event)
	}).pipe(
		Effect.provide(
			GitHubIngress.layer({
				namespace: 'identity',
				policy,
				handlers: [{ id: 'receive', onCreation: () => Effect.die('Invalid events must not execute') }],
			}).pipe(Layer.provide(unusedGitHub), Layer.provideMerge(memory({ maxMailboxes: 10 }))),
		),
	),
)
