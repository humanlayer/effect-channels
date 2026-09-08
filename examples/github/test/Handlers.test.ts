import { assert, it } from '@effect/vitest'
import {
	GitHub,
	GitHubActivityEvent,
	GitHubCreationEvent,
	GitHubSubscriptions,
	type GitHubReaction,
} from '@humanlayer/channels-github'
import { layer as memory } from '@humanlayer/channels-github/memory'
import { Deferred, Effect, Fiber, Layer, Logger } from 'effect'

import { namespace, observeActivity, observeCreation, respond } from '../src/handlers.js'
import { acknowledge, listAcknowledgements, removeAcknowledgement, stopFollowing } from '../src/usage.js'

const repository = { kind: 'github.repository', installationId: 1, id: 2, owner: 'alice', name: 'project' } as const
const resource = { kind: 'github.issue', repository, number: 3 } as const
const user = { id: 4, login: 'alice', type: 'User' }
const issue = {
	id: 5,
	number: 3,
	title: 'private title',
	body: 'private body',
	state: 'open',
	html_url: '',
	user,
} as const
const creation = GitHubCreationEvent.make({
	event: 'issues',
	action: 'opened',
	deliveryId: 'creation',
	resource,
	issue,
	sender: user,
})

it.effect('creation is observation only; mentions opt in and explicit cleanup opts out', () =>
	Effect.gen(function* () {
		const comments: string[] = []
		const subscriptions = yield* GitHubSubscriptions
		const input = { namespace, resource }
		yield* observeCreation(creation)
		assert.isFalse(yield* subscriptions.isSubscribed(input))
		yield* respond(creation).pipe(
			Effect.provide(
				Layer.mock(GitHub, {
					createComment: ({ body }) =>
						Effect.sync(() => {
							comments.push(body)
							return {
								ref: { kind: 'github.issue-comment', issue: resource, id: 6 },
								data: { id: 6, body, html_url: '', user },
							}
						}),
				}),
			),
		)
		assert.isTrue(yield* subscriptions.isSubscribed(input))
		assert.deepEqual(comments, ['Received issues for #3.'])
		assert.isTrue(yield* stopFollowing(resource))
		assert.isFalse(yield* subscriptions.isSubscribed(input))
		assert.isFalse(yield* stopFollowing(resource))
	}).pipe(Effect.provide(memory({ maxMailboxes: 10 }))),
)

it.effect('native event/action discrimination logs metadata without bodies or provider writes', () =>
	Effect.gen(function* () {
		const logs: string[] = []
		const pr = { ...resource, kind: 'github.pull-request' } as const
		const common = { deliveryId: 'activity', resource: pr, sender: user, pull_request: issue }
		const events = [
			creation,
			GitHubActivityEvent.make({
				resource,
				issue,
				sender: user,
				deliveryId: 'comment',
				event: 'issue_comment',
				action: 'deleted',
				comment: { id: 6, body: 'private comment', html_url: '', user },
			}),
			GitHubActivityEvent.make({
				...common,
				event: 'pull_request',
				action: 'closed',
				pull_request: { ...issue, merged: true },
			}),
			GitHubActivityEvent.make({
				...common,
				event: 'pull_request',
				action: 'closed',
				pull_request: { ...issue, merged: false },
			}),
			GitHubActivityEvent.make({
				...common,
				event: 'pull_request_review',
				action: 'submitted',
				review: {
					id: 7,
					node_id: 'review',
					body: 'private review',
					user,
					state: 'changes_requested',
					commit_id: 'sha',
					html_url: '',
				},
			}),
			GitHubActivityEvent.make({
				...common,
				event: 'pull_request_review_thread',
				action: 'resolved',
				thread: { node_id: 'thread', comments: [] },
			}),
			GitHubActivityEvent.make({
				...common,
				event: 'pull_request',
				action: 'synchronize',
				before: 'before-sha',
				after: 'after-sha',
				pull_request: {
					...issue,
					head: { ref: 'private branch', sha: 'after-sha' },
					base: { ref: 'main', sha: 'base-sha' },
				},
			}),
			GitHubActivityEvent.make({
				...common,
				event: 'pull_request_review_comment',
				action: 'edited',
				comment: {
					id: 9,
					node_id: 'inline',
					body: 'private inline comment',
					html_url: '',
					user,
					pull_request_review_id: 7,
					path: 'private path',
					commit_id: 'sha',
					original_commit_id: 'sha',
					diff_hunk: 'private diff',
					pull_request_url: 'https://api.github.com/repos/alice/project/pulls/3',
				},
			}),
		]
		yield* Effect.forEach(events, observeActivity).pipe(
			Effect.provide(Logger.layer([Logger.make((entry) => logs.push(JSON.stringify(entry.message)))])),
		)
		assert.equal(logs.length, events.length)
		assert.include(logs[1] ?? '', 'deleted')
		assert.include(logs[2] ?? '', '"merged":true')
		assert.include(logs[3] ?? '', '"merged":false')
		assert.include(logs[4] ?? '', 'changes_requested')
		assert.include(logs[5] ?? '', 'resolved')
		assert.include(logs[6] ?? '', '"before":"before-sha"')
		assert.include(logs[6] ?? '', '"after":"after-sha"')
		assert.include(logs[7] ?? '', '"commentId":9')
		for (const log of logs) {
			assert.include(log, '"installationId":1')
			assert.include(log, '"repositoryId":2')
			assert.include(log, '"number":3')
			assert.include(log, '"deliveryId":')
			assert.include(log, '"resourceKind":')
		}
		assert.notInclude(logs.join(''), 'private')
		assert.notInclude(logs.join(''), 'alice')
	}),
)

it.effect('cleanup returns its earlier snapshot even if another caller unsubscribes first', () =>
	Effect.gen(function* () {
		const subscriptions = yield* GitHubSubscriptions
		const input = { namespace, resource }
		yield* subscriptions.subscribe(input)
		const read = yield* Deferred.make<void>()
		const resume = yield* Deferred.make<void>()
		const cleanup = yield* stopFollowing(resource).pipe(
			Effect.provideService(GitHubSubscriptions, {
				...subscriptions,
				isSubscribed: (input) =>
					subscriptions.isSubscribed(input).pipe(
						Effect.tap(() => Deferred.succeed(read, undefined)),
						Effect.tap(() => Deferred.await(resume)),
					),
			}),
			Effect.forkChild,
		)
		yield* Deferred.await(read)
		assert.isTrue(yield* stopFollowing(resource))
		yield* Deferred.succeed(resume, undefined)
		assert.isTrue(yield* Fiber.join(cleanup))
		assert.isFalse(yield* subscriptions.isSubscribed(input))
	}).pipe(Effect.provide(memory({ maxMailboxes: 10 }))),
)

it.effect('callable reaction examples forward native targets and retain the returned ref for explicit removal', () =>
	Effect.gen(function* () {
		const calls: unknown[] = []
		const reaction: GitHubReaction = {
			ref: { kind: 'github.reaction', target: resource, id: 8 },
			data: { id: 8, node_id: 'reaction', user, content: 'eyes', created_at: '' },
		}
		yield* Effect.gen(function* () {
			const added = yield* acknowledge(resource)
			assert.deepEqual(calls, [{ target: resource, content: 'eyes' }])
			assert.deepEqual(yield* listAcknowledgements(resource), [reaction])
			yield* removeAcknowledgement(added.ref)
		}).pipe(
			Effect.provide(
				Layer.mock(GitHub, {
					addReaction: (input) =>
						Effect.sync(() => {
							calls.push(input)
							return reaction
						}),
					listReactions: (input) =>
						Effect.sync(() => {
							calls.push(input)
							return [reaction]
						}),
					removeReaction: (input) =>
						Effect.sync(() => {
							calls.push(input)
						}),
				}),
			),
		)
		assert.deepEqual(calls, [
			{ target: resource, content: 'eyes' },
			{ target: resource, content: 'eyes', page: 1, perPage: 20 },
			{ reaction: reaction.ref },
		])
	}),
)
