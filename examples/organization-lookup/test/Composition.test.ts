import { assert, it } from '@effect/vitest'
import { MailboxReadiness, MailboxStore } from '@humanlayer/channels-delivery'
import { GitHub, GitHubComment, GitHubIngress, GitHubOrganizations } from '@humanlayer/channels-github'
import { layer as memory } from '@humanlayer/channels-github/memory'
import {
	MarkdownContent,
	SentMessage,
	Slack,
	SlackOrganizations,
	SlackSubscriptions,
	SlackTeamId,
} from '@humanlayer/channels-slack'
import { Context, Deferred, Effect, Fiber, Layer, Queue, Schema } from 'effect'
import { expectTypeOf } from 'vite-plus/test'

import { event, policy, user } from '../../../packages/github/test/fixtures.js'
import { testMessageEvent } from '../../../packages/slack/test/legacy/support.js'
import { githubFollowup, githubReply, slackReply } from '../src/handlers.js'
import { githubOrganizations, organizations, slackOrganizations } from '../src/organizations.js'

it.effect('followed GitHub status, label, deleted, blank and bot events do not produce replies', () =>
	Effect.gen(function* () {
		const context = { organizationId: 'north', skipped: [] }
		for (const action of ['closed', 'reopened', 'labeled', 'unlabeled', 'assigned', 'unassigned'] as const) {
			yield* githubFollowup({ ...event, action }, context)
		}
		for (const input of [
			{ action: 'deleted' as const, body: 'unsubscribe', sender: user },
			{ action: 'created' as const, body: '   ', sender: user },
			{ action: 'created' as const, body: 'hello', sender: { ...user, type: 'Bot' } },
		]) {
			yield* githubFollowup(
				{
					...event,
					event: 'issue_comment',
					action: input.action,
					sender: input.sender,
					comment: {
						id: 61,
						body: input.body,
						user: input.sender,
						html_url: 'https://github.test/comment/61',
					},
				},
				context,
			)
		}
	}).pipe(Effect.provide(Layer.merge(memory(), Layer.mock(GitHub, {})))),
)

it.effect('supplies both lookup callbacks through library service Layers without extra dependencies', () =>
	Effect.gen(function* () {
		expectTypeOf(slackOrganizations).toEqualTypeOf<Layer.Layer<SlackOrganizations>>()
		expectTypeOf(githubOrganizations).toEqualTypeOf<Layer.Layer<GitHubOrganizations>>()
		const slack = yield* SlackOrganizations
		const github = yield* GitHubOrganizations
		assert.deepStrictEqual(
			yield* Effect.all(
				[
					slack.resolve({ workspaceId: SlackTeamId.make('T_NORTH') }),
					github.resolve({ installationId: 200 }),
					slack.resolve({ workspaceId: SlackTeamId.make('T_SOUTH') }),
					github.resolve({ installationId: 100 }),
				],
				{ concurrency: 'unbounded' },
			),
			[
				{ organizationId: 'north' },
				{ organizationId: 'south' },
				{ organizationId: 'south' },
				{ organizationId: 'north' },
			],
		)
		assert.strictEqual(yield* slack.resolve({ workspaceId: SlackTeamId.make('T_UNKNOWN') }), null)
		assert.strictEqual(yield* github.resolve({ installationId: 999 }), null)
		assert.strictEqual(slack.legacyOrganizationId, undefined)
		assert.strictEqual(github.legacyOrganizationId, undefined)
	}).pipe(Effect.provide(organizations)),
)

it.effect('callback ownership reaches the real GitHub handler through saved ingress; only outbound is recorded', () =>
	Effect.gen(function* () {
		const posts = yield* Queue.unbounded<string>()
		const entered = yield* Queue.unbounded<void>()
		const release = yield* Deferred.make<void>()
		const outbound = Layer.mock(GitHub, {
			createComment: ({ issue, body }) =>
				Queue.offer(entered, undefined).pipe(
					Effect.andThen(Deferred.await(release)),
					Effect.andThen(Queue.offer(posts, body)),
					Effect.as(
						GitHubComment.make({
							ref: { kind: 'github.issue-comment', issue, id: 60 },
							data: { id: 60, body, user, html_url: 'https://github.test/comment/60' },
						}),
					),
				),
		})
		const shared = yield* Layer.build(
			GitHubIngress.layer({
				namespace: 'recipe',
				policy,
				handlers: [{ id: 'reply', onCreation: githubReply }],
			}).pipe(Layer.provideMerge(memory()), Layer.provide(organizations), Layer.provide(outbound)),
		)
		const ingress = Context.get(shared, GitHubIngress)
		const incoming = [100, 200].map((installationId) => ({
			...event,
			deliveryId: `recipe-${installationId}`,
			resource: { ...event.resource, repository: { ...event.resource.repository, installationId } },
		}))
		for (const item of incoming) yield* ingress.acceptActivity({ event: item, mentioned: false, own: false })
		const keys = yield* Context.get(shared, MailboxReadiness).scanReady({ prefix: '', now: 0, limit: 10 })
		assert.strictEqual(keys.length, 2)
		const store = Context.get(shared, MailboxStore)
		const owners = yield* Effect.forEach(keys, (key) =>
			store.loadMailbox({ key }).pipe(Effect.map((saved) => saved?.state.pending[0]?.organizationId)),
		)
		assert.deepStrictEqual(new Set(owners), new Set(['north', 'south']))
		const running = yield* Effect.forEach(incoming, (item) => ingress.processActivity({ event: item }), {
			concurrency: 2,
		}).pipe(Effect.forkChild)
		yield* Queue.take(entered)
		yield* Queue.take(entered)
		yield* Deferred.succeed(release, undefined)
		yield* Fiber.join(running)
		for (const key of keys) {
			assert.strictEqual((yield* store.loadMailbox({ key }))?.state.outcomes[0]?.kind, 'completed')
		}
		assert.deepStrictEqual((yield* Queue.takeAll(posts)).sort(), ['Organization: north', 'Organization: south'])
	}),
)

it.effect('the Slack handler replies using saved organization context, not the native tenant', () =>
	Effect.gen(function* () {
		const posts = yield* Queue.unbounded<MarkdownContent>()
		const outbound = Layer.mock(Slack, {
			capabilities: {
				threadPost: true,
				channelPost: false,
				edit: false,
				delete: false,
				streaming: 'unsupported',
				typing: { thread: false, channel: false },
				history: { thread: false, channelMessages: false, channelThreads: false },
				reactions: { add: false, remove: false, events: false },
				files: { read: false, upload: false },
				actions: false,
				threadInfo: false,
				channelInfo: false,
				createThread: false,
				directMessages: { ingress: false, open: false },
				ephemeral: { native: false, dmFallback: false },
				subject: false,
			},
			post: ({ threadId, content }) => {
				assert.strictEqual(threadId, testMessageEvent.thread.ref.id)
				assert.ok(Schema.is(MarkdownContent)(content))
				return Queue.offer(posts, content).pipe(
					Effect.as(
						SentMessage.make({
							ref: {
								threadId,
								messageRef: testMessageEvent.message.ref,
								provider: testMessageEvent.provider,
								degraded: [],
							},
							message: testMessageEvent.message,
						}),
					),
				)
			},
		})
		const dependencies = Layer.merge(outbound, SlackSubscriptions.layerMemory())
		for (const organizationId of ['north', 'south']) {
			yield* slackReply(testMessageEvent, { organizationId, skipped: [] }).pipe(Effect.provide(dependencies))
		}
		assert.deepStrictEqual(yield* Queue.takeAll(posts), [
			MarkdownContent.make({ markdown: 'Organization: north' }),
			MarkdownContent.make({ markdown: 'Organization: south' }),
		])
	}),
)
