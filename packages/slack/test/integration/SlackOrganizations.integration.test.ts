import { assert, it } from '@effect/vitest'
import { MailboxReadiness, MailboxStore } from '@humanlayer/channels-delivery'
import { layer as memory } from '@humanlayer/channels-delivery/memory'
import {
	MarkdownContent,
	SlackOrganizationLookupError,
	SlackOrganizations,
	SlackSubscriptions,
	type SlackTeamId,
} from '@humanlayer/channels-slack'
import { Clock, ConfigProvider, Context, Deferred, Effect, Fiber, Layer, Logger, Queue, Ref, Scope } from 'effect'
import { TestClock } from 'effect/testing'
import { HttpRouter } from 'effect/unstable/http'
import { expectTypeOf } from 'vite-plus/test'

import {
	HistoryResponse,
	PostedMessageResponse,
	SlackEmulator,
	slackEmulatorAliceToken,
	slackEmulatorBotToken,
	slackEmulatorBotUserId,
	slackEmulatorBotId,
	slackEmulatorSigningSecret,
} from './support/SlackEmulator.js'
import { StorageTypeId, makeSlackTestHost, slack } from './support/SlackTestHost.js'

class Directory extends Context.Service<
	Directory,
	{
		readonly lookup: (input: {
			readonly workspaceId: SlackTeamId
		}) => Effect.Effect<{ readonly organizationId: string } | null, SlackOrganizationLookupError>
	}
>()('test/ApplicationDirectory') {}

it.effect(
	'signed Slack ingress uses an independent directory Layer, freezes attribution and posts through the emulator',
	() =>
		Effect.gen(function* () {
			yield* TestClock.setTime(yield* Clock.currentTimeMillis.pipe(TestClock.withLive))
			const emulator = yield* SlackEmulator
			const lookupCalls = yield* Ref.make(0)
			const mode = yield* Ref.make<'A' | 'B' | 'unknown' | 'failed' | 'defect' | 'cancel'>('A')
			const entered = yield* Deferred.make<void>()
			const finalized = yield* Deferred.make<void>()
			const logs: string[] = []
			const logger = Logger.layer([Logger.make((entry) => logs.push(JSON.stringify(entry.message)))])
			let synchronousThrow = false
			const organizations = SlackOrganizations.layer((input) => {
				if (synchronousThrow) throw new Error('private-synchronous-sentinel')
				expectTypeOf(input.workspaceId).toEqualTypeOf<SlackTeamId>()
				return Effect.flatMap(Directory, (directory) => directory.lookup(input))
			}).pipe(
				Layer.provide(
					Layer.succeed(
						Directory,
						Directory.of({
							lookup: ({ workspaceId }) =>
								Effect.gen(function* () {
									assert.strictEqual(workspaceId, emulator.teamId)
									yield* Ref.update(lookupCalls, (count) => count + 1)
									const value = yield* Ref.get(mode)
									if (value === 'failed') return yield* SlackOrganizationLookupError.make({})
									if (value === 'defect') return yield* Effect.die('private-defect-sentinel')
									if (value === 'cancel')
										return yield* Deferred.succeed(entered, undefined).pipe(
											Effect.andThen(Effect.never),
											Effect.ensuring(Deferred.succeed(finalized, undefined)),
										)
									return value === 'unknown' ? null : { organizationId: value }
								}),
						}),
					),
				),
			)
			const commits = yield* Queue.unbounded<void>()
			const memoryContext = yield* Layer.build(memory({ maxMailboxes: 100 }))
			const underlying = Context.get(memoryContext, MailboxStore)
			const storage = Layer.mergeAll(
				Layer.succeed(MailboxReadiness, Context.get(memoryContext, MailboxReadiness)),
				SlackSubscriptions.layerMemory(),
				Layer.succeed(
					MailboxStore,
					MailboxStore.of({
						...underlying,
						commitMailbox: (input) =>
							underlying
								.commitMailbox(input)
								.pipe(
									Effect.tap((result) =>
										result === 'committed' &&
										input.nextState.outcomes.some((outcome) => outcome.kind === 'completed')
											? Queue.offer(commits, undefined)
											: Effect.void,
									),
								),
					}),
				),
			)
			const app = makeSlackTestHost({
				providers: [slack()],
				storage: { [StorageTypeId]: storage },
				onNewMention: (thread, _, context) =>
					thread
						.post(MarkdownContent.make({ markdown: `organization=${context.organizationId}` }))
						.pipe(Effect.asVoid),
				advanced: {
					slackApiOrigin: new URL(`${emulator.emulator.url}/api`),
					configProvider: ConfigProvider.fromUnknown({
						SLACK_BOT_TOKEN: slackEmulatorBotToken,
						SLACK_BOT_USER_ID: slackEmulatorBotUserId,
						SLACK_BOT_ID: slackEmulatorBotId,
						SLACK_SIGNING_SECRET: slackEmulatorSigningSecret,
					}),
				},
			})
			const memoMap = yield* Layer.makeMemoMap
			const context = yield* Layer.buildWithMemoMap(
				app.services.pipe(Layer.provide(organizations), Layer.provide(logger)),
				memoMap,
				yield* Scope.Scope,
			)
			const web = HttpRouter.toWebHandler(app.routes.pipe(Layer.provide(organizations), Layer.provide(logger)), {
				memoMap,
				disableLogger: true,
			})
			yield* Effect.addFinalizer(() => Effect.promise(web.dispose))
			const clock = Context.merge(Context.make(Clock.Clock, yield* Clock.Clock), yield* Layer.build(logger))
			const root = yield* emulator.call(
				slackEmulatorAliceToken,
				'chat.postMessage',
				{ channel: emulator.publicChannelId, text: 'organization test' },
				PostedMessageResponse,
			)
			const deliver = (id: string, index: number, signal?: AbortSignal) =>
				Effect.promise(() =>
					web.handler(
						new Request(
							emulator.signedWebhook({
								type: 'event_callback',
								team_id: emulator.teamId,
								event_id: `Ev_organization_${id}`,
								event_time: 1,
								event: {
									type: 'app_mention',
									channel: root.channel,
									thread_ts: root.ts,
									ts: `${Math.floor(Number(root.ts)) + index}.000001`,
									text: `<@${slackEmulatorBotUserId}> hello`,
									user: emulator.aliceUserId,
								},
							}),
							{ signal },
						),
						clock,
					),
				)
			assert.strictEqual((yield* deliver('first', 1)).status, 200)
			const readiness = Context.get(context, MailboxReadiness)
			const [key] = yield* readiness.scanReady({ prefix: '', now: yield* Clock.currentTimeMillis, limit: 10 })
			assert.ok(key !== undefined)
			assert.strictEqual((yield* underlying.loadMailbox({ key }))?.state.pending[0]?.organizationId, 'A')
			yield* Ref.set(mode, 'B')
			assert.strictEqual((yield* deliver('first', 1)).status, 200)
			assert.strictEqual(yield* Ref.get(lookupCalls), 1)
			yield* Ref.set(mode, 'unknown')
			assert.strictEqual((yield* deliver('unknown', 2)).status, 200)
			yield* Ref.set(mode, 'failed')
			assert.strictEqual((yield* deliver('failed', 3)).status, 503)
			yield* Ref.set(mode, 'defect')
			const defect = yield* deliver('defect', 4)
			assert.strictEqual(defect.status, 500)
			assert.strictEqual(yield* Effect.promise(() => defect.text()), '')
			synchronousThrow = true
			const thrown = yield* deliver('throw', 5)
			assert.strictEqual(thrown.status, 500)
			assert.strictEqual(yield* Effect.promise(() => thrown.text()), '')
			assert.ok(logs.some((log) => log.includes('unexpected_defect')))
			assert.ok(logs.every((log) => !log.includes('private-')))
			synchronousThrow = false
			yield* Ref.set(mode, 'cancel')
			const abort = new AbortController()
			const logCount = logs.length
			const sending = yield* deliver('cancel', 6, abort.signal).pipe(Effect.forkChild)
			yield* Deferred.await(entered)
			abort.abort()
			assert.strictEqual((yield* Fiber.join(sending)).status, 499)
			yield* Deferred.await(finalized)
			assert.strictEqual(logs.length, logCount)
			assert.strictEqual((yield* underlying.loadMailbox({ key }))?.state.pending.length, 1)
			const worker = yield* app.run.pipe(Effect.provide(context), Effect.forkChild)
			yield* Queue.take(commits)
			yield* Fiber.interrupt(worker)
			const final = yield* underlying.loadMailbox({ key })
			assert.strictEqual(final?.state.active, null)
			assert.strictEqual(final?.state.outcomes.length, 1)
			const history = yield* emulator.call(
				slackEmulatorBotToken,
				'conversations.replies',
				{ channel: root.channel, ts: root.ts },
				HistoryResponse,
			)
			assert.deepStrictEqual(
				history.messages
					.filter((message) => message.user === slackEmulatorBotUserId)
					.map((message) => message.text),
				['organization=A'],
			)
		}).pipe(Effect.provide(SlackEmulator.layer)),
	{ timeout: 15000 },
)
