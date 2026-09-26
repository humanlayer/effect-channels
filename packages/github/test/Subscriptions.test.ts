import { assert, it } from '@effect/vitest'
import {
	DeliveryQueue,
	enqueueDelivery,
	MailboxReadiness,
	MailboxStore,
	MailboxStoreError,
	mailboxKey,
} from '@humanlayer/channels-delivery'
import { Clock, Context, Effect, Layer, Queue, Redacted, Ref, Schema } from 'effect'
import { TestClock } from 'effect/testing'

import {
	GitHubActivityEvent,
	GitHubError,
	GitHubCrypto,
	GitHubIngress,
	GitHubIssueData,
	GitHubPullRequestData,
	GitHubReviewCommentData,
	GitHubReviewData,
	GitHubWebhookPayload,
	GitHubRoutes,
	GitHubSubscriptions,
	GitHubSubscriptionRoute,
	GitHubSubscriptionStore,
	issueResourceKey,
	reviewCommentRootId,
} from '../src/index'
import { layer as memory } from '../src/memory'
import { event, policy, routeCredentials, user, unusedGitHub } from './fixtures'
import {
	host,
	payloadFor,
	secret,
	signedRequest,
	webhookBase,
	webhookLocation,
	webhookRequest,
	type WebhookFixture,
} from './support'

const base = payloadFor(event)
const common = webhookBase(event)
const pull_request = {
	...event.issue,
	merged: false,
	draft: false,
	head: { ref: 'feature', sha: 'after' },
	base: { ref: 'main', sha: 'base' },
} satisfies GitHubPullRequestData
const review = {
	id: 50,
	node_id: 'PRR_50',
	body: 'Review',
	state: 'approved',
	user,
	commit_id: 'after',
	html_url: 'https://github.test/review/50',
} satisfies GitHubReviewData
const comment = {
	id: 60,
	node_id: 'PRRC_60',
	body: 'Inline',
	user,
	html_url: 'https://github.test/comment/60',
	pull_request_review_id: 50,
	path: 'src/main.ts',
	commit_id: 'after',
	original_commit_id: 'before',
	diff_hunk: '@@ -1 +1 @@',
	pull_request_url: 'https://api.github.test/repos/alice/project/pulls/1',
	line: 1,
	side: 'RIGHT',
} satisfies GitHubReviewCommentData
/** A review-thread body whose thread carries a numeric `id` instead of the required `node_id`. */
const ThreadWithoutNodeIdBody = Schema.fromJsonString(
	Schema.Struct({
		...GitHubWebhookPayload.fields,
		thread: Schema.Struct({ id: Schema.Int, comments: Schema.Array(GitHubReviewCommentData) }),
	}),
)
const subscription = { namespace: 'activity-test', resource: event.resource }

const setup = Effect.gen(function* () {
	const seen = yield* Queue.unbounded<string>()
	const values = yield* Queue.unbounded<GitHubActivityEvent>()
	const storage = yield* Layer.build(memory({ maxMailboxes: 100 }))
	const record = (kind: string) => (value: GitHubActivityEvent) =>
		Queue.offer(seen, kind).pipe(Effect.andThen(Queue.offer(values, value)), Effect.asVoid)
	const services = yield* Layer.build(
		GitHubIngress.layer({
			namespace: subscription.namespace,
			policy,
			handlers: [
				{
					id: 'agent',
					onCreation: record('creation'),
					onMention: record('mention'),
					onSubscribedEvent: record('followed'),
				},
				{ id: 'observer', onSubscribedEvent: record('observer') },
			],
		}).pipe(Layer.provide(unusedGitHub), Layer.provide(Layer.succeedContext(storage))),
	)
	const ingress = Context.get(services, GitHubIngress)
	const subscriptions = Context.get(storage, GitHubSubscriptions)
	const send = yield* host(
		GitHubRoutes.layer({
			signingSecret: Redacted.make(secret),
			maxBodyBytes: 32_000,
			botLogin: 'channels[bot]',
		}).pipe(
			Layer.provide(Layer.succeedContext(services)),
			Layer.provide(routeCredentials),
			Layer.provide(GitHubCrypto.layerWebCrypto),
		),
	)
	const drain = (pr: boolean) =>
		ingress
			.processActivity({
				event: pr
					? {
							event: 'pull_request',
							action: 'opened',
							deliveryId: 'key',
							resource: { ...event.resource, kind: 'github.pull-request' },
							pull_request,
							sender: user,
						}
					: event,
			})
			.pipe(Effect.provide(storage))
	return { seen, values, subscriptions, send, drain }
})

it.effect('signed complete direct trigger matrix; all lifecycle notifications survive serial admission', () =>
	Effect.gen(function* () {
		const test = yield* setup
		yield* test.subscriptions.subscribe(subscription)
		yield* test.subscriptions.subscribe({
			...subscription,
			resource: { ...event.resource, kind: 'github.pull-request' },
		})
		const matrix: ReadonlyArray<WebhookFixture & { readonly pr: boolean }> = [
			...['closed', 'reopened', 'edited', 'assigned', 'unassigned', 'labeled', 'unlabeled'].map((action) => ({
				event: 'issues',
				payload: { ...base, action, assignee: user, label: { id: 70, name: 'bug', color: 'ff0000' } },
				pr: false,
			})),
			...['created', 'edited', 'deleted'].flatMap((action) =>
				[false, true].map((pr) => ({
					event: 'issue_comment',
					payload: {
						...base,
						action,
						issue: pr
							? { ...event.issue, pull_request: { url: 'https://api.github.test/pulls/1' } }
							: event.issue,
						comment: { id: 61, body: 'Discussion', user, html_url: 'https://github.test/comment/61' },
					},
					pr,
				})),
			),
			...[
				'closed',
				'reopened',
				'edited',
				'synchronize',
				'review_requested',
				'review_request_removed',
				'assigned',
				'unassigned',
				'labeled',
				'unlabeled',
				'converted_to_draft',
				'ready_for_review',
			].map((action) => ({
				event: 'pull_request',
				payload: {
					...common,
					pull_request: { ...pull_request, draft: action === 'converted_to_draft' },
					action,
					before: 'before',
					after: 'after',
					assignee: user,
					requested_reviewer: user,
					label: { id: 70, name: 'bug', color: 'ff0000' },
				},
				pr: true,
			})),
			{
				event: 'pull_request',
				payload: {
					...common,
					action: 'closed',
					pull_request: { ...pull_request, merged: true },
				},
				pr: true,
			},
			{
				event: 'pull_request',
				payload: {
					...common,
					action: 'review_requested',
					pull_request,
					requested_team: { id: 71, name: 'Maintainers', slug: 'maintainers' },
				},
				pr: true,
			},
			...(['approved', 'changes_requested', 'commented'] as const).map((state) => ({
				event: 'pull_request_review',
				payload: { ...common, pull_request, action: 'submitted', review: { ...review, state } },
				pr: true,
			})),
			...(
				[
					['edited', 'commented'],
					['dismissed', 'dismissed'],
				] as const
			).map(([action, state]) => ({
				event: 'pull_request_review',
				payload: { ...common, pull_request, action, review: { ...review, state } },
				pr: true,
			})),
			...['created', 'edited', 'deleted'].flatMap((action) =>
				[false, true].map((reply) => ({
					event: 'pull_request_review_comment',
					payload: {
						...common,
						pull_request,
						action,
						comment: reply ? { ...comment, id: 62, in_reply_to_id: 60 } : comment,
					},
					pr: true,
				})),
			),
			...['resolved', 'unresolved'].map((action) => ({
				event: 'pull_request_review_thread',
				payload: {
					...common,
					pull_request,
					action,
					thread: { node_id: 'PRRT_native', comments: [comment, { ...comment, id: 62, in_reply_to_id: 60 }] },
				},
				pr: true,
			})),
		]
		let id = 0
		for (const fixture of matrix)
			assert.equal(
				(yield* test.send(yield* webhookRequest(fixture.event, fixture.payload, `matrix-${id++}`))).status,
				200,
				`${fixture.event}:${fixture.payload.action}`,
			)
		for (let i = 0; i < matrix.length; i++) {
			yield* test.drain(false)
			yield* test.drain(true)
		}
		assert.equal(yield* Queue.size(test.seen), matrix.length * 2)
		const codec = Schema.fromJsonString(GitHubActivityEvent)
		for (let i = 0; i < matrix.length * 2; i++) {
			const value = yield* Queue.take(test.values)
			assert.deepEqual(yield* Schema.decodeEffect(codec)(yield* Schema.encodeEffect(codec)(value)), value)
			if (value.event === 'pull_request_review_comment') assert.equal(reviewCommentRootId(value.comment), 60)
			if (value.event === 'pull_request_review_thread') assert.equal(value.thread.node_id, 'PRRT_native')
		}
	}),
)

it.effect('signed native PR activity accepts null and minimal authors without weakening legacy issue data', () =>
	Effect.gen(function* () {
		const test = yield* setup
		yield* test.subscriptions.subscribe({
			...subscription,
			resource: { ...event.resource, kind: 'github.pull-request' },
		})
		let id = 0
		const codec = Schema.fromJsonString(GitHubActivityEvent)
		for (const author of [null, { id: user.id, login: user.login }]) {
			assert.equal(Schema.is(GitHubIssueData)({ ...event.issue, user: author }), false)
			const parent = { ...pull_request, user: author }
			for (const fixture of [
				{ event: 'pull_request_review', action: 'submitted', review },
				{
					event: 'pull_request_review_thread',
					action: 'resolved',
					thread: { node_id: 'PRRT_native', comments: [comment] },
				},
				{ event: 'pull_request', action: 'synchronize', before: 'before', after: 'after' },
			]) {
				assert.equal(
					(yield* test.send(
						yield* webhookRequest(
							fixture.event,
							{
								...common,
								...fixture,
								pull_request: parent,
							},
							`minimal-author-${id++}`,
						),
					)).status,
					200,
				)
				yield* test.drain(true)
				assert.deepEqual(yield* Queue.takeAll(test.seen), ['followed', 'observer'])
				assert.equal(yield* Queue.size(test.values), 2)
				for (const value of yield* Queue.takeAll(test.values)) {
					const { pull_request } = yield* Schema.decodeUnknownEffect(
						Schema.Struct({ pull_request: GitHubPullRequestData }),
					)(value)
					assert.deepEqual(pull_request.user, author)
					assert.deepEqual(yield* Schema.decodeEffect(codec)(yield* Schema.encodeEffect(codec)(value)), value)
				}
			}
		}
	}),
)

it.effect('signed review requests accept minimal reviewer and team targets through persisted consumers', () =>
	Effect.gen(function* () {
		const test = yield* setup
		yield* test.subscriptions.subscribe({
			...subscription,
			resource: { ...event.resource, kind: 'github.pull-request' },
		})
		let id = 0
		const codec = Schema.fromJsonString(GitHubActivityEvent)
		for (const author of [null, { id: user.id, login: user.login }]) {
			for (const target of [
				{ action: 'review_requested', requested_reviewer: { id: 72, login: 'reviewer' } },
				{ action: 'review_request_removed', requested_reviewer: { id: 72, login: 'reviewer' } },
				{ action: 'review_requested', requested_team: { id: 71, name: 'Maintainers' } },
			]) {
				assert.equal(
					(yield* test.send(
						yield* webhookRequest(
							'pull_request',
							{
								...common,
								pull_request: { ...pull_request, user: author },
								...target,
							},
							`minimal-target-${id++}`,
						),
					)).status,
					200,
				)
				yield* test.drain(true)
				assert.deepEqual(yield* Queue.takeAll(test.seen), ['followed', 'observer'])
				assert.equal(yield* Queue.size(test.values), 2)
				for (const value of yield* Queue.takeAll(test.values)) {
					assert.ok(
						value.event === 'pull_request' &&
							(value.action === 'review_requested' || value.action === 'review_request_removed'),
					)
					if (
						value.event === 'pull_request' &&
						(value.action === 'review_requested' || value.action === 'review_request_removed')
					) {
						assert.deepEqual(value.pull_request.user, author)
						assert.deepEqual(value.requested_reviewer, target.requested_reviewer)
						assert.deepEqual(value.requested_team, target.requested_team)
						assert.equal(value.requested_reviewer?.type, undefined)
						assert.equal(value.requested_team?.slug, undefined)
					}
					assert.deepEqual(yield* Schema.decodeEffect(codec)(yield* Schema.encodeEffect(codec)(value)), value)
				}
			}
		}
	}),
)

it.effect('nullable and minimal PR authors preserve mention and own-content suppression', () =>
	Effect.gen(function* () {
		const test = yield* setup
		let id = 0
		for (const fixture of [
			{ author: null, sender: user, expected: ['creation', 'mention'] },
			{ author: { id: user.id, login: user.login }, sender: user, expected: ['creation', 'mention'] },
			{ author: { id: 99, login: 'renamed-bot' }, sender: user, expected: [] },
			{ author: { id: 101, login: 'channels[bot]' }, sender: user, expected: [] },
			{ author: null, sender: { ...user, id: 99 }, expected: [] },
		]) {
			assert.equal(
				(yield* test.send(
					yield* webhookRequest(
						'pull_request',
						{
							...common,
							action: 'opened',
							sender: fixture.sender,
							pull_request: { ...pull_request, user: fixture.author, body: '@channels help' },
						},
						`author-suppression-${id++}`,
					),
				)).status,
				200,
			)
			yield* test.drain(true)
			assert.equal(yield* Queue.size(test.seen), fixture.expected.length)
			if (fixture.expected.length > 0) assert.deepEqual(yield* Queue.takeAll(test.seen), fixture.expected)
		}
	}),
)

it.effect('creation independent of mention; per-consumer precedence, unsubscribe and frozen ignored redelivery', () =>
	Effect.gen(function* () {
		const test = yield* setup
		const send = (id: string, body: string, action = 'created') =>
			webhookRequest(
				'issue_comment',
				{ ...base, action, comment: { id: 61, body, user, html_url: 'https://github.test/comment/61' } },
				id,
			).pipe(Effect.flatMap(test.send))
		assert.equal((yield* test.send(yield* webhookRequest('issues', base, 'created'))).status, 200)
		yield* test.drain(false)
		assert.equal(yield* Queue.take(test.seen), 'creation')
		assert.equal(yield* test.subscriptions.isSubscribed(subscription), false)
		assert.equal((yield* send('ignored', 'unmentioned')).status, 200)
		yield* test.subscriptions.subscribe(subscription)
		assert.equal((yield* send('ignored', 'unmentioned')).status, 200)
		yield* test.drain(false)
		assert.equal(yield* Queue.size(test.seen), 0)
		assert.equal((yield* send('mention', '@channels help')).status, 200)
		yield* test.drain(false)
		assert.deepEqual([yield* Queue.take(test.seen), yield* Queue.take(test.seen)], ['mention', 'observer'])
		assert.equal((yield* send('followed', 'update')).status, 200)
		yield* test.subscriptions.unsubscribe(subscription)
		assert.equal((yield* send('followed', 'update')).status, 200)
		yield* test.drain(false)
		assert.deepEqual([yield* Queue.take(test.seen), yield* Queue.take(test.seen)], ['followed', 'observer'])
		assert.equal((yield* send('after-unsubscribe', 'update')).status, 200)
		yield* test.drain(false)
		assert.equal(yield* Queue.size(test.seen), 0)
		for (const resource of [
			{ ...event.resource, kind: 'github.pull-request' as const },
			{ ...event.resource, repository: { ...event.resource.repository, id: 21 } },
			{ ...event.resource, repository: { ...event.resource.repository, installationId: 101 } },
		])
			assert.equal(yield* test.subscriptions.isSubscribed({ ...subscription, resource }), false)
		yield* test.subscriptions.subscribe(subscription)
		assert.equal(yield* test.subscriptions.isSubscribed({ ...subscription, namespace: 'other' }), false)
	}),
)

it.effect('partial fanout is frozen before admission and survives unsubscribe and ingress reconstruction', () =>
	Effect.gen(function* () {
		const storage = yield* Layer.build(memory({ maxMailboxes: 20 }))
		const store = Context.get(storage, MailboxStore)
		const subscriptions = Context.get(storage, GitHubSubscriptions)
		const clock = yield* Clock.Clock
		const failSecond = yield* Ref.make(true)
		const keyForHandler = (id: string) =>
			mailboxKey({
				namespace: subscription.namespace,
				handlerId: `["${id}","subscribed"]`,
				provider: 'github',
				installation: String(event.resource.repository.installationId),
				resourceKey: issueResourceKey(event.resource),
			})
		const seen = yield* Queue.unbounded<string>()
		const faultStore = MailboxStore.of({
			loadMailbox: store.loadMailbox,
			commitMailbox: (input) =>
				Effect.gen(function* () {
					if (input.key === keyForHandler('two') && (yield* Ref.getAndSet(failSecond, false)))
						return yield* MailboxStoreError.make({ operation: 'commit' })
					return yield* store.commitMailbox(input)
				}),
		})
		const faultQueue = DeliveryQueue.of({
			enqueue: (input) =>
				enqueueDelivery(input).pipe(
					Effect.provideService(MailboxStore, faultStore),
					Effect.provideService(Clock.Clock, clock),
				),
		})
		const fault = Layer.succeedContext(
			Context.make(MailboxStore, faultStore).pipe(Context.add(DeliveryQueue, faultQueue)),
		)
		const make = () =>
			GitHubIngress.layer({
				namespace: subscription.namespace,
				policy,
				handlers: ['one', 'two'].map((id) => ({
					id,
					onSubscribedEvent: () => Queue.offer(seen, id).pipe(Effect.asVoid),
				})),
			}).pipe(Layer.provide(unusedGitHub), Layer.provide(fault), Layer.provide(Layer.succeedContext(storage)))
		const first = yield* Layer.build(make())
		const send = yield* host(
			GitHubRoutes.layer({
				signingSecret: Redacted.make(secret),
				maxBodyBytes: 32_000,
				botLogin: 'channels[bot]',
			}).pipe(
				Layer.provide(Layer.succeedContext(first)),
				Layer.provide(routeCredentials),
				Layer.provide(GitHubCrypto.layerWebCrypto),
			),
		)
		yield* subscriptions.subscribe(subscription)
		const request = () =>
			webhookRequest('issues', { ...base, action: 'closed' }, 'partial').pipe(Effect.flatMap(send))
		assert.equal((yield* request()).status, 503)
		assert.strictEqual(
			(yield* store.loadMailbox({ key: keyForHandler('one') }))?.state.pending[0]?.eventId,
			'partial',
		)
		assert.strictEqual(yield* store.loadMailbox({ key: keyForHandler('two') }), undefined)
		yield* subscriptions.unsubscribe(subscription)
		assert.equal((yield* request()).status, 200)
		const reconstructed = yield* Layer.build(make())
		yield* Context.get(reconstructed, GitHubIngress)
			.processActivity({ event })
			.pipe(Effect.provideService(MailboxStore, faultStore))
		assert.deepEqual([yield* Queue.take(seen), yield* Queue.take(seen)], ['one', 'two'])
		assert.equal((yield* request()).status, 200)
		yield* Context.get(reconstructed, GitHubIngress)
			.processActivity({ event })
			.pipe(Effect.provideService(MailboxStore, faultStore))
		assert.equal(yield* Queue.size(seen), 0)
		assert.equal(Schema.is(GitHubSubscriptionRoute)({ version: 2, targets: [] }), false)
	}),
)

it.effect('signed mentions in every content family, edits, own actors and native schema rejection', () =>
	Effect.gen(function* () {
		const test = yield* setup
		yield* test.subscriptions.subscribe(subscription)
		yield* test.subscriptions.subscribe({
			...subscription,
			resource: { ...event.resource, kind: 'github.pull-request' },
		})
		const bot = { ...user, id: 99, login: 'channels[bot]', type: 'Bot' }
		const cases: ReadonlyArray<WebhookFixture & { readonly expected: ReadonlyArray<string> }> = [
			{
				event: 'issues',
				payload: { ...base, issue: { ...event.issue, body: '@channels help' } },
				expected: ['creation', 'mention'],
			},
			{
				event: 'pull_request',
				payload: { ...common, pull_request: { ...pull_request, body: '@channels help' } },
				expected: ['creation', 'mention'],
			},
			{
				event: 'issue_comment',
				payload: { ...base, action: 'created', comment: { ...comment, body: '@channels help' } },
				expected: ['mention'],
			},
			{
				event: 'pull_request_review',
				payload: {
					...common,
					pull_request,
					action: 'submitted',
					review: { ...review, body: '@channels help' },
				},
				expected: ['mention'],
			},
			{
				event: 'pull_request_review_comment',
				payload: {
					...common,
					pull_request,
					action: 'created',
					comment: { ...comment, id: 62, in_reply_to_id: 60, body: '@channels help' },
				},
				expected: ['mention'],
			},
			...['pull_request_review', 'pull_request_review_comment'].flatMap((family) =>
				[true, false].map((fresh) => ({
					event: family,
					payload: {
						...common,
						pull_request,
						action: 'edited',
						review: { ...review, body: '@channels help' },
						comment: { ...comment, body: '@channels help' },
						changes: { body: { from: fresh ? 'before' : '@channels before' } },
					},
					expected: [fresh ? 'mention' : 'followed'],
				})),
			),
			{ event: 'issue_comment', payload: { ...base, action: 'created', sender: bot, comment }, expected: [] },
			{
				event: 'issue_comment',
				payload: {
					...base,
					action: 'edited',
					comment: { ...comment, user: bot, body: '@channels' },
					changes: { body: { from: '' } },
				},
				expected: [],
			},
			{
				event: 'issues',
				payload: { ...base, action: 'closed', issue: { ...event.issue, user: bot } },
				expected: ['followed'],
			},
			{
				event: 'issue_comment',
				payload: {
					...base,
					action: 'created',
					sender: { ...user, id: 101, login: 'other[bot]', type: 'Bot' },
					comment,
				},
				expected: ['followed'],
			},
			{
				event: 'pull_request_review_thread',
				payload: {
					...webhookLocation(event),
					pull_request,
					action: 'resolved',
					thread: { node_id: 'PRRT_native', comments: [comment] },
				},
				expected: ['followed'],
			},
		]
		let id = 0
		for (const fixture of cases) {
			assert.equal(
				(yield* test.send(yield* webhookRequest(fixture.event, fixture.payload, `content-${id++}`))).status,
				200,
			)
			yield* test.drain(fixture.event.startsWith('pull_request'))
			const expected = fixture.expected.length === 0 ? [] : [...fixture.expected, 'observer']
			assert.equal(yield* Queue.size(test.seen), expected.length, fixture.event)
			for (const name of expected) assert.equal(yield* Queue.take(test.seen), name)
		}
		const invalid: ReadonlyArray<WebhookFixture> = [
			{
				event: 'issues',
				payload: { ...webhookLocation(event), issue: event.issue },
			},
			{
				event: 'pull_request',
				payload: {
					...common,
					pull_request,
					action: 'synchronize',
					before: 'before',
					after: 'wrong-head',
				},
			},
			{ event: 'pull_request', payload: { ...common, pull_request, number: 2 } },
			{
				event: 'pull_request_review',
				payload: {
					...common,
					pull_request,
					action: 'submitted',
					review: { ...review, state: 'pending' },
				},
			},
			{
				event: 'pull_request_review_comment',
				payload: {
					...common,
					pull_request,
					action: 'created',
					comment: { ...comment, pull_request_url: 'https://api.github.test/repos/other/repo/pulls/1' },
				},
			},
		]
		for (const fixture of invalid)
			assert.equal(
				(yield* test.send(yield* webhookRequest(fixture.event, fixture.payload, `invalid-${id++}`))).status,
				400,
			)
		const threadWithoutNodeId = yield* Schema.encodeEffect(ThreadWithoutNodeIdBody)({
			...common,
			pull_request,
			action: 'resolved',
			thread: { id: 50, comments: [comment] },
		})
		assert.equal(
			(yield* test.send(signedRequest('pull_request_review_thread', threadWithoutNodeId, `invalid-${id++}`)))
				.status,
			400,
		)
		for (const family of ['check_run', 'check_suite', 'workflow_run', 'workflow_job', 'merge_group', 'push'])
			assert.equal((yield* test.send(signedRequest(family, '{}', `excluded-${id++}`))).status, 200)
		yield* test.drain(false)
		yield* test.drain(true)
		assert.equal(yield* Queue.size(test.seen), 0)
	}),
)

it.effect('route storage is bounded, never evicts frozen decisions, and isolates every scope', () =>
	Effect.gen(function* () {
		const subscriptions = yield* GitHubSubscriptions
		const store = yield* GitHubSubscriptionStore
		const route = { ...subscription, deliveryId: 'same', direct: [], followed: ['receive'] }
		assert.deepEqual((yield* store.resolveRoute(route)).targets, [])
		yield* subscriptions.subscribe(subscription)
		assert.deepEqual((yield* store.resolveRoute(route)).targets, [])
		const other = { ...subscription, namespace: 'other' }
		yield* subscriptions.subscribe(other)
		assert.deepEqual((yield* store.resolveRoute({ ...route, ...other })).targets, ['receive'])
		for (const resource of [
			{ ...event.resource, kind: 'github.pull-request' as const },
			{ ...event.resource, repository: { ...event.resource.repository, id: 21 } },
			{ ...event.resource, repository: { ...event.resource.repository, installationId: 101 } },
		]) {
			assert.deepEqual((yield* store.resolveRoute({ ...route, resource })).targets, [])
		}
		assert.equal(
			(yield* store.resolveRoute({ ...route, deliveryId: 'over-capacity' }).pipe(Effect.flip)).reason,
			'capacity',
		)
		assert.deepEqual((yield* store.resolveRoute(route)).targets, [])
		yield* subscriptions.unsubscribe(subscription)
		assert.deepEqual((yield* store.resolveRoute({ ...route, ...other })).targets, ['receive'])
	}).pipe(Effect.provide(memory({ maxSubscriptions: 2, maxRoutes: 5 }))),
)

it.effect('a first admitted mention may subscribe without adding new consumers during partial retry', () =>
	Effect.gen(function* () {
		const storage = yield* Layer.build(memory({ maxMailboxes: 20 }))
		const store = Context.get(storage, MailboxStore)
		const clock = yield* Clock.Clock
		const subscriptions = Context.get(storage, GitHubSubscriptions)
		const failSecond = yield* Ref.make(true)
		const secondKey = mailboxKey({
			namespace: subscription.namespace,
			handlerId: '["two","mention"]',
			provider: 'github',
			installation: String(event.resource.repository.installationId),
			resourceKey: issueResourceKey(event.resource),
		})
		const seen = yield* Queue.unbounded<string>()
		const faultStore = MailboxStore.of({
			loadMailbox: store.loadMailbox,
			commitMailbox: (input) =>
				Effect.gen(function* () {
					if (input.key === secondKey && (yield* Ref.getAndSet(failSecond, false)))
						return yield* MailboxStoreError.make({ operation: 'commit' })
					return yield* store.commitMailbox(input)
				}),
		})
		const faultQueue = DeliveryQueue.of({
			enqueue: (input) =>
				enqueueDelivery(input).pipe(
					Effect.provideService(MailboxStore, faultStore),
					Effect.provideService(Clock.Clock, clock),
				),
		})
		const fault = Layer.succeedContext(
			Context.make(MailboxStore, faultStore).pipe(Context.add(DeliveryQueue, faultQueue)),
		)
		const services = yield* Layer.build(
			GitHubIngress.layer({
				namespace: subscription.namespace,
				policy,
				handlers: [
					...['one', 'two'].map((id) => ({
						id,
						onMention: () =>
							subscriptions
								.subscribe(subscription)
								.pipe(Effect.andThen(Queue.offer(seen, id)), Effect.asVoid),
					})),
					{
						id: 'late-observer',
						onSubscribedEvent: () => Queue.offer(seen, 'unexpected').pipe(Effect.asVoid),
					},
				],
			}).pipe(Layer.provide(unusedGitHub), Layer.provide(fault), Layer.provide(Layer.succeedContext(storage))),
		)
		const ingress = Context.get(services, GitHubIngress)
		const send = yield* host(
			GitHubRoutes.layer({
				signingSecret: Redacted.make(secret),
				maxBodyBytes: 32_000,
				botLogin: 'channels[bot]',
			}).pipe(
				Layer.provide(Layer.succeedContext(services)),
				Layer.provide(routeCredentials),
				Layer.provide(GitHubCrypto.layerWebCrypto),
			),
		)
		const request = () =>
			webhookRequest(
				'issue_comment',
				{ ...base, action: 'created', comment: { ...comment, body: '@channels help' } },
				'partial-mention',
			).pipe(Effect.flatMap(send))
		assert.equal((yield* request()).status, 503)
		yield* ingress.processActivity({ event }).pipe(Effect.provideService(MailboxStore, faultStore))
		assert.equal(yield* Queue.take(seen), 'one')
		assert.equal(yield* subscriptions.isSubscribed(subscription), true)
		assert.equal((yield* request()).status, 200)
		yield* ingress.processActivity({ event }).pipe(Effect.provideService(MailboxStore, faultStore))
		assert.equal(yield* Queue.take(seen), 'two')
		assert.equal(yield* Queue.size(seen), 0)
	}),
)

it.effect('frozen handler retry survives unsubscribe and retains its native event', () =>
	Effect.gen(function* () {
		const attempts = yield* Ref.make(0)
		const seen = yield* Queue.unbounded<string>()
		const storage = yield* Layer.build(memory({ maxMailboxes: 20 }))
		const subscriptions = Context.get(storage, GitHubSubscriptions)
		const services = yield* Layer.build(
			GitHubIngress.layer({
				namespace: subscription.namespace,
				policy,
				handlers: [
					{
						id: 'retry',
						onSubscribedEvent: (event) =>
							Effect.gen(function* () {
								const attempt = yield* Ref.updateAndGet(attempts, (n) => n + 1)
								if (attempt === 1) return yield* GitHubError.make({ reason: 'unavailable' })
								yield* Queue.offer(seen, event.action)
							}),
					},
				],
			}).pipe(Layer.provide(unusedGitHub), Layer.provide(Layer.succeedContext(storage))),
		)
		const ingress = Context.get(services, GitHubIngress)
		const send = yield* host(
			GitHubRoutes.layer({
				signingSecret: Redacted.make(secret),
				maxBodyBytes: 32_000,
				botLogin: 'channels[bot]',
			}).pipe(
				Layer.provide(Layer.succeedContext(services)),
				Layer.provide(routeCredentials),
				Layer.provide(GitHubCrypto.layerWebCrypto),
			),
		)
		yield* subscriptions.subscribe(subscription)
		assert.equal((yield* send(yield* webhookRequest('issues', { ...base, action: 'closed' }, 'retry'))).status, 200)
		assert.equal(
			(yield* Context.get(storage, MailboxReadiness).scanReady({ prefix: '', now: 0, limit: 100 })).length,
			1,
		)
		yield* ingress.processActivity({ event }).pipe(Effect.provide(storage))
		assert.equal(yield* Ref.get(attempts), 1)
		yield* subscriptions.unsubscribe(subscription)
		yield* TestClock.adjust(policy.retryBaseMs)
		yield* ingress.processActivity({ event }).pipe(Effect.provide(storage))
		assert.equal(yield* Ref.get(attempts), 2)
		assert.equal(yield* Queue.take(seen), 'closed')
	}),
)
