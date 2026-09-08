import { assert, it } from '@effect/vitest'
import { activeBatches, DeliveryPolicy, MailboxReadiness, MailboxStore } from '@humanlayer/channels-delivery'
import { MarkdownContent } from '@humanlayer/channels-slack'
import { Clock, ConfigProvider, Context, Deferred, Effect, Fiber, Layer, Match, Queue, Scope } from 'effect'
import { TestClock } from 'effect/testing'
import { HttpRouter } from 'effect/unstable/http'

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
import { ChannelsStorage, defaultDeliveryPolicy, makeSlackTestHost, slack } from './support/SlackTestHost.js'

const policies: ReadonlyArray<DeliveryPolicy> = [
	{ ...defaultDeliveryPolicy, mode: 'queue' },
	{ ...defaultDeliveryPolicy, mode: 'concurrent', maxConcurrency: 2 },
	{ ...defaultDeliveryPolicy, mode: 'debounce', quietPeriodMs: 300 },
	{ ...defaultDeliveryPolicy, mode: 'burst', windowMs: 300 },
	{ ...defaultDeliveryPolicy, mode: 'drop' },
]

for (const policy of policies) {
	const initialDeadline = Match.value(policy.mode).pipe(
		Match.when('debounce', () => 400),
		Match.when('burst', () => 300),
		Match.orElse(() => 0),
	)
	const initialReplies = Match.value(policy.mode).pipe(
		Match.when('concurrent', () => ['A; skipped=', 'B; skipped=']),
		Match.when('drop', () => ['A; skipped=']),
		Match.orElse(() => ['B; skipped=A']),
	)
	it.effect(
		`signed Slack ${policy.mode} admission produces native emulator replies and durable outcomes`,
		() =>
			Effect.gen(function* () {
				yield* TestClock.setTime(yield* Clock.currentTimeMillis.pipe(TestClock.withLive))
				const start = yield* Clock.currentTimeMillis
				const emulator = yield* SlackEmulator
				const posted = yield* Queue.unbounded<string>()
				const gate = yield* Deferred.make<void>()
				const app = makeSlackTestHost({
					providers: [slack()],
					storage: ChannelsStorage.memory(),
					policy,
					runner: { scanLimit: 10, concurrency: 4, pollMs: 10 },
					onNewMention: (thread, message, context) =>
						Effect.gen(function* () {
							const text = `${message.text}; skipped=${context.skipped.map((event) => event.message.text).join(',')}`
							yield* thread.post(MarkdownContent.make({ markdown: text }))
							yield* Queue.offer(posted, text)
							yield* Deferred.await(gate)
						}),
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
				const context = yield* Layer.buildWithMemoMap(app.services, memoMap, yield* Scope.Scope)
				const web = HttpRouter.toWebHandler(app.routes, { memoMap, disableLogger: true })
				yield* Effect.addFinalizer(() => Effect.promise(web.dispose))
				const clock = Context.make(Clock.Clock, yield* Clock.Clock)
				const root = yield* emulator.call(
					slackEmulatorAliceToken,
					'chat.postMessage',
					{ channel: emulator.publicChannelId, text: 'mode test root' },
					PostedMessageResponse,
				)
				const deliver = (id: string, index: number) =>
					Effect.promise(() =>
						web.handler(
							emulator.signedWebhook({
								type: 'event_callback',
								team_id: emulator.teamId,
								event_id: `Ev_${id}`,
								event_time: 1,
								event: {
									type: 'app_mention',
									channel: root.channel,
									thread_ts: root.ts,
									ts: `${Math.floor(Number(root.ts)) + index}.000001`,
									text: `<@${slackEmulatorBotUserId}> ${id}`,
									user: emulator.aliceUserId,
								},
							}),
							clock,
						),
					).pipe(Effect.tap((response) => Effect.sync(() => assert.strictEqual(response.status, 200))))
				yield* deliver('A', 1)
				yield* TestClock.adjust(100)
				yield* deliver('B', 2)
				yield* TestClock.adjust(100)
				yield* deliver('B', 2)
				assert.strictEqual(yield* Queue.size(posted), 0)
				const readiness = Context.get(context, MailboxReadiness)
				const [key] = yield* readiness.scanReady({ prefix: '', now: start + 1000, limit: 10 })
				assert.ok(key !== undefined)
				const store = Context.get(context, MailboxStore)
				const before = yield* store.loadMailbox({ key })
				assert.ok(before !== undefined)
				assert.strictEqual(before.state.pending.length, policy.mode === 'drop' ? 1 : 2)
				assert.strictEqual(before.state.readyAt, start + initialDeadline)
				const worker = yield* app.run.pipe(Effect.provide(context), Effect.forkChild)
				if (policy.mode === 'burst' || policy.mode === 'debounce') {
					yield* TestClock.adjust(99)
					assert.strictEqual(yield* Queue.size(posted), 0)
					yield* TestClock.adjust(policy.mode === 'burst' ? 1 : 101)
				}
				const first = yield* Queue.take(posted)
				const expected = [...initialReplies]
				const observed = [first]
				if (policy.mode === 'concurrent') {
					yield* TestClock.adjust(10)
					observed.push(yield* Queue.take(posted))
				}
				assert.deepStrictEqual(observed.sort(), [...expected].sort())
				yield* deliver('C', 3)
				yield* deliver('D', 4)
				yield* deliver('D', 4)
				const busy = yield* store.loadMailbox({ key })
				assert.ok(busy !== undefined)
				assert.strictEqual(activeBatches(busy.state).length, policy.mode === 'concurrent' ? 2 : 1)
				yield* Deferred.succeed(gate, undefined)
				yield* TestClock.adjust(policy.mode === 'debounce' ? 310 : 10)
				if (policy.mode !== 'drop') {
					expected.push(policy.mode === 'concurrent' ? 'C; skipped=' : 'D; skipped=C')
					observed.push(yield* Queue.take(posted))
					if (policy.mode === 'concurrent') {
						yield* TestClock.adjust(10)
						expected.push('D; skipped=')
						observed.push(yield* Queue.take(posted))
					}
				}
				yield* TestClock.adjust(10)
				const final = yield* store.loadMailbox({ key })
				assert.ok(final !== undefined)
				assert.strictEqual(final.state.readyAt, null)
				assert.strictEqual(final.state.outcomes.length, 4)
				assert.strictEqual(
					final.state.outcomes.filter((outcome) => outcome.kind === 'dropped').length,
					policy.mode === 'drop' ? 3 : 0,
				)
				const history = yield* emulator.call(
					slackEmulatorBotToken,
					'conversations.replies',
					{ channel: root.channel, ts: root.ts },
					HistoryResponse,
				)
				assert.deepStrictEqual(
					history.messages
						.filter((message) => message.user === slackEmulatorBotUserId)
						.map((message) => message.text)
						.sort(),
					[...expected].sort(),
				)
				assert.deepStrictEqual(observed.sort(), [...expected].sort())
				yield* Fiber.interrupt(worker)
			}).pipe(Effect.provide(SlackEmulator.layer)),
		{ timeout: 15_000 },
	)
}
