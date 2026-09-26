import { assert, it } from '@effect/vitest'
import { layer as memory } from '@humanlayer/channels-github/memory'
import { Context, Effect, Layer, Queue, Redacted } from 'effect'

import { GitHubCrypto, GitHubIngress, GitHubRoutes, type GitHubActivityEvent, issueResourceKey } from '../src/index'
import { event, policy, routeCredentials, user, unusedGitHub } from './fixtures'
import { host, payloadFor, secret, webhookBase, webhookRequest, type WebhookFixture } from './support'

it.live(
	'targeted issue/PR bodies and discussion comments only; edits, boundaries, assignments and own-loop safety',
	() =>
		Effect.gen(function* () {
			const seen = yield* Queue.unbounded<GitHubActivityEvent>()
			const storage = yield* Layer.build(memory({ maxMailboxes: 64 }))
			const environment = yield* Layer.build(
				GitHubIngress.layer({
					namespace: 'mentions',
					policy,
					handlers: [{ id: 'receive', onMention: (value) => Queue.offer(seen, value).pipe(Effect.asVoid) }],
				}).pipe(Layer.provide(unusedGitHub), Layer.provide(Layer.succeedContext(storage))),
			)
			const send = yield* host(
				GitHubRoutes.layer({
					signingSecret: Redacted.make(secret),
					maxBodyBytes: 16_000,
					botLogin: 'channels[bot]',
				}).pipe(
					Layer.provide(Layer.succeedContext(environment)),
					Layer.provide(routeCredentials),
					Layer.provide(GitHubCrypto.layerWebCrypto),
				),
			)
			const ingress = Context.get(environment, GitHubIngress)
			const base = payloadFor(event)
			const common = webhookBase(event)
			const bot = { ...user, id: 99, login: 'channels[bot]', type: 'Bot' }
			const issue = { ...event.issue, body: '@channels please help' }
			const pull_request = {
				...issue,
				number: 2,
				merged: false,
				head: { ref: 'feature', sha: 'after' },
				base: { ref: 'main', sha: 'base' },
			}
			const comment = { id: 60, body: '@channels[bot] help', html_url: 'https://test/comment', user }
			const fixtures: ReadonlyArray<WebhookFixture & { readonly accepted: boolean; readonly status?: number }> = [
				{ event: 'issues', payload: { ...base, issue }, accepted: true },
				{
					event: 'issues',
					payload: { ...base, issue: { ...issue, body: '@CHANNELS[bot], help' } },
					accepted: true,
				},
				{
					event: 'issues',
					payload: {
						...base,
						issue: { ...issue, body: 'other@channels.example @channels-other @other @channels[other]' },
					},
					accepted: false,
				},
				{
					event: 'issues',
					payload: { ...base, issue: { ...issue, body: null }, assignee: bot },
					accepted: false,
				},
				{ event: 'issues', payload: { ...base, action: 'assigned', issue, assignee: bot }, accepted: false },
				{ event: 'issues', payload: { ...base, action: 'assigned', issue, assignee: user }, accepted: false },
				{
					event: 'issues',
					payload: { ...base, action: 'edited', issue, changes: { body: { from: null } } },
					accepted: true,
				},
				{
					event: 'issues',
					payload: { ...base, action: 'edited', issue, changes: { body: { from: '@channels old request' } } },
					accepted: false,
				},
				{ event: 'issues', payload: { ...base, action: 'edited', issue }, accepted: false },
				{
					event: 'issues',
					payload: {
						...base,
						action: 'edited',
						issue: { ...issue, body: 'removed' },
						changes: { body: { from: '@channels' } },
					},
					accepted: false,
				},
				{ event: 'issues', payload: { ...base, issue, sender: bot }, accepted: false },
				{ event: 'issues', payload: { ...base, issue: { ...issue, user: bot } }, accepted: false },
				{ event: 'issue_comment', payload: { ...base, action: 'created', issue, comment }, accepted: true },
				{
					event: 'issue_comment',
					payload: { ...base, action: 'created', issue, comment: { ...comment, body: 'no new mention' } },
					accepted: false,
				},
				{
					event: 'issue_comment',
					payload: { ...base, action: 'created', issue, comment: { ...comment, user: bot } },
					accepted: false,
				},
				{
					event: 'issue_comment',
					payload: { ...base, action: 'created', issue, comment: { ...comment, user: { ...bot, id: 123 } } },
					accepted: false,
				},
				{
					event: 'issue_comment',
					payload: { ...base, action: 'edited', issue, comment, changes: { body: { from: 'before' } } },
					accepted: true,
				},
				{ event: 'issue_comment', payload: { ...base, action: 'deleted', issue, comment }, accepted: false },
				{ event: 'pull_request', payload: { ...common, pull_request }, accepted: true },
				{
					event: 'pull_request',
					payload: { ...common, pull_request: { ...pull_request, body: 'ordinary PR' } },
					accepted: false,
				},
				{
					event: 'pull_request',
					payload: {
						...common,
						action: 'edited',
						pull_request,
						changes: { body: { from: '' } },
					},
					accepted: true,
				},
				{
					event: 'issue_comment',
					payload: {
						...base,
						action: 'created',
						issue: { ...pull_request, pull_request: { url: 'https://test/pulls/2' } },
						comment,
					},
					accepted: true,
				},
				{ event: 'pull_request_review_comment', payload: { ...base, comment }, accepted: false, status: 400 },
				{ event: 'pull_request_review', payload: base, accepted: false, status: 400 },
				{ event: 'workflow_run', payload: base, accepted: false },
				{ event: 'issues', payload: { ...base, action: 'closed', issue }, accepted: false },
				{ event: 'issues', payload: { ...base, action: 'reopened', issue }, accepted: false },
				{
					event: 'issue_comment',
					payload: { ...base, action: 'created', issue: { ...issue, user: bot }, comment },
					accepted: true,
				},
				...['issue', 'pr'].flatMap((kind) => {
					const discussion =
						kind === 'issue' ? issue : { ...pull_request, pull_request: { url: 'https://test/pulls/2' } }
					return [
						{
							event: 'issue_comment',
							payload: {
								...base,
								action: 'edited',
								issue: discussion,
								comment,
								changes: { body: { from: 'before' } },
							},
							accepted: true,
						},
						{
							event: 'issue_comment',
							payload: {
								...base,
								action: 'edited',
								issue: discussion,
								comment,
								changes: { body: { from: '@CHANNELS already' } },
							},
							accepted: false,
						},
						{
							event: 'issue_comment',
							payload: { ...base, action: 'edited', issue: discussion, comment },
							accepted: false,
						},
						{
							event: 'issue_comment',
							payload: {
								...base,
								action: 'edited',
								issue: discussion,
								comment: { ...comment, body: 'removed' },
								changes: { body: { from: '@channels' } },
							},
							accepted: false,
						},
					]
				}),
				...['edited', 'reopened', 'closed', 'assigned', 'synchronize'].map((action) => ({
					event: 'pull_request',
					payload: {
						...common,
						action,
						pull_request,
						changes: { body: { from: '@channels existing' } },
						before: 'before',
						after: 'after',
					},
					accepted: false,
				})),
			]
			let id = 0
			for (const fixture of fixtures) {
				const deliveryId = `target-${id++}`
				const request = yield* webhookRequest(fixture.event, fixture.payload, deliveryId)
				const duplicate = request.clone()
				assert.equal((yield* send(request)).status, fixture.status ?? 200)
				yield* ingress.processActivity({ event }).pipe(Effect.provide(storage))
				const prEvent: GitHubActivityEvent = {
					event: 'pull_request',
					action: 'opened',
					deliveryId,
					resource: { kind: 'github.pull-request', repository: event.resource.repository, number: 2 },
					pull_request,
					sender: user,
				}
				yield* ingress.processActivity({ event: prEvent }).pipe(Effect.provide(storage))
				assert.equal(yield* Queue.size(seen), fixture.accepted ? 1 : 0, fixture.event + ' ' + deliveryId)
				if (fixture.accepted) {
					const received = yield* Queue.take(seen)
					assert.equal(received.event, fixture.event)
					assert.equal(received.deliveryId, deliveryId)
					assert.equal(
						received.resource.number,
						received.event === 'issues' || received.event === 'issue_comment'
							? received.issue.number
							: received.pull_request.number,
					)
					if (received.event === 'issue_comment')
						assert.equal(
							received.resource.kind,
							received.issue.pull_request === undefined ? 'github.issue' : 'github.pull-request',
						)
					if (received.event === 'pull_request') {
						assert.equal(received.pull_request.body, pull_request.body)
						assert.equal(received.resource.kind, 'github.pull-request')
					}
				}
				assert.equal((yield* send(duplicate)).status, fixture.status ?? 200)
				yield* ingress.processActivity({ event }).pipe(Effect.provide(storage))
				yield* ingress.processActivity({ event: prEvent }).pipe(Effect.provide(storage))
				assert.equal(yield* Queue.size(seen), 0)
			}
			assert.notEqual(
				issueResourceKey(event.resource),
				issueResourceKey({ ...event.resource, kind: 'github.pull-request' }),
			)
		}),
)
