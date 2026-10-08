import { describe, it } from '@effect/vitest'
import {
	BatchId,
	CompleteDelivery,
	DeliveryActivity,
	DeliveryClosed,
	DeliveryControl,
	DeliveryId,
	DeliveryMutationReceipt,
	FailDelivery,
	SetDeliveryActivity,
	deliveryApiRoutes,
	makeDeliveryId,
	type DeliveryMutation,
	type DeliveryMutationError,
} from '@humanlayer/channels-delivery'
import { GitHubId, GitHubIssue, GitHubIssueRef } from '@humanlayer/channels-github'
import {
	AgentFinishedLogEntry,
	AgentId,
	EventId,
	EventLog,
	MessageId,
	UserMessageLogEntry,
	type LogEntry,
} from '@humanlayer/fold-core'
import * as Cloudflare from 'alchemy/Cloudflare'
import { Deferred, Effect, Fiber, Layer, Option, Predicate, Queue, Redacted, Ref, Stream } from 'effect'
import { HttpClient, HttpRouter, HttpServerRequest } from 'effect/http'

import { DeliveryApi } from '../src/DeliveryApi'
import {
	ActiveDelivery,
	ActiveDeliveryRecord,
	AgentConversation,
	AgentSessionMessage,
	DeliveryTurns,
	RESTART_NUDGE,
	type TurnSession,
} from '../src/DeliveryTurn'
import { RunRecovery } from '../src/SessionRecovery'
import { RepoCloneError } from '../src/Workspace'

const rootAgentId = AgentId.create()
const issue = GitHubIssue.make({
	ref: GitHubIssueRef.make({
		installationId: GitHubId.make(100),
		repositoryId: GitHubId.make(200),
		owner: 'humanlayer',
		repository: 'effect-channels',
		number: GitHubId.make(42),
	}),
	mailboxKey: 'github:issue:42',
})

const messageFor = (batch: string) =>
	AgentSessionMessage.make({
		prompt: 'fix the bug',
		githubDiscussion: issue,
		deliveryId: makeDeliveryId({ mailboxKey: issue.mailboxKey, batchId: BatchId.make(batch) }),
		accessToken: Redacted.make('secret-token'),
	})

const userMessage = (seq: number, text: string): LogEntry =>
	UserMessageLogEntry.make({
		seq,
		eventId: EventId.create(),
		ts: 0,
		version: 1,
		agentId: rootAgentId,
		parentAgentId: null,
		toolCallId: null,
		messageId: MessageId.create(),
		message: { role: 'user', content: text },
	})

const agentFinished = (seq: number, outcome: AgentFinishedLogEntry['outcome'], resultText: string | null) =>
	AgentFinishedLogEntry.make({
		seq,
		eventId: EventId.create(),
		ts: 0,
		version: 1,
		agentId: rootAgentId,
		parentAgentId: null,
		toolCallId: null,
		outcome,
		resultText,
		reason: null,
	})

const working = SetDeliveryActivity.make({
	activity: DeliveryActivity.cases.Working.make({ message: 'Working on it' }),
})
const somethingWentWrong = FailDelivery.make({ markdown: 'Something went wrong, and I could not finish.' })

interface Applied {
	readonly deliveryId: string
	readonly accessToken: string
	readonly mutation: DeliveryMutation
}

/**
 * DeliveryTurns over fakes of its dependencies. The fake Fold session appends the user message and the run's
 * `agent-finished` entry to the log, as Fold does, and finishes with `outcome`. Deliveries are reported through
 * the real delivery API: the client sends HTTP requests to the delivery routes, which apply them to a recording
 * `DeliveryControl`.
 */
const makeHarness = (
	options: {
		readonly log?: ReadonlyArray<LogEntry>
		readonly saved?: ActiveDeliveryRecord
		readonly outcome?: AgentFinishedLogEntry['outcome']
		readonly open?: Effect.Effect<void, RepoCloneError>
		readonly gate?: Deferred.Deferred<void>
		readonly finishError?: DeliveryMutationError
	} = {},
) =>
	Effect.gen(function* () {
		const log = yield* Ref.make<ReadonlyArray<LogEntry>>(options.log ?? [])
		const saved = yield* Ref.make(Option.fromNullishOr(options.saved))
		const sent = yield* Ref.make<ReadonlyArray<string>>([])
		const applied = yield* Ref.make<ReadonlyArray<Applied>>([])
		const turns = yield* Queue.unbounded<Fiber.Fiber<void>>()

		const session: TurnSession = {
			rootAgentId,
			entries: Ref.get(log),
			send: (prompt) =>
				Effect.gen(function* () {
					yield* Ref.update(sent, (all) => [...all, prompt])
					if (options.gate !== undefined) yield* Deferred.await(options.gate)
					const seq = (yield* Ref.get(log)).length
					const finished = agentFinished(seq + 1, options.outcome ?? 'completed', 'done')
					yield* Ref.update(log, (entries) => [...entries, userMessage(seq, prompt), finished])
					return finished
				}),
		}

		const deliveryControl = Layer.succeed(
			DeliveryControl,
			DeliveryControl.of({
				status: () => Effect.die('unexpected delivery status read'),
				apply: (input) =>
					Effect.gen(function* () {
						yield* Ref.update(applied, (all) => [
							...all,
							{
								deliveryId: input.deliveryId,
								accessToken: Redacted.value(input.accessToken),
								mutation: input.mutation,
							},
						])
						if (
							options.finishError !== undefined &&
							!Predicate.isTagged(input.mutation, 'SetDeliveryActivity')
						) {
							return yield* options.finishError
						}
						return DeliveryMutationReceipt.make({
							deliveryId: DeliveryId.make(input.deliveryId),
							status: 'accepted',
						})
					}),
			}),
		)
		const deliveryRoutes = yield* HttpRouter.toHttpEffect(
			deliveryApiRoutes({ basePath: undefined }).pipe(Layer.provide(deliveryControl)),
		)
		/** Requests go straight to the routes, through the adapter production uses for its self binding. */
		const routesHttpClient = Cloudflare.toHttpClient({
			fetch: (request) =>
				deliveryRoutes.pipe(Effect.provideService(HttpServerRequest.HttpServerRequest, request), Effect.scoped),
		})

		const dependencies = Layer.mergeAll(
			DeliveryApi.layer.pipe(Layer.provide(Layer.succeed(HttpClient.HttpClient, routesHttpClient))),
			Layer.succeed(
				ActiveDelivery,
				ActiveDelivery.of({
					current: Ref.get(saved),
					save: (record) => Ref.set(saved, Option.some(record)),
					clear: ({ deliveryId }) =>
						Ref.update(
							saved,
							Option.filter((record) => record.message.deliveryId !== deliveryId),
						),
				}),
			),
			Layer.succeed(
				AgentConversation,
				AgentConversation.of({ open: () => (options.open ?? Effect.void).pipe(Effect.as(session)) }),
			),
			Layer.succeed(
				RunRecovery,
				RunRecovery.of({
					fork: (effect) =>
						Effect.forkChild(effect).pipe(
							Effect.flatMap((fiber) => Queue.offer(turns, fiber)),
							Effect.asVoid,
						),
					alarm: Effect.die('unexpected recovery alarm'),
				}),
			),
			Layer.mock(EventLog, { entries: () => Stream.fromIterableEffect(Ref.get(log)) }),
		)

		return {
			layer: DeliveryTurns.layer.pipe(Layer.provide(dependencies)),
			/** Wait for the next turn started in the background to end. */
			nextTurnEnded: Effect.flatMap(Queue.take(turns), Fiber.join),
			startedTurns: Queue.size(turns),
			saved: Ref.get(saved),
			sent: Ref.get(sent),
			applied: Ref.get(applied),
		}
	})

describe('DeliveryTurns.accept', () => {
	it.effect('saves the delivery, then completes it with the result of its Fold turn and forgets it', ({ expect }) =>
		Effect.gen(function* () {
			const gate = yield* Deferred.make<void>()
			const harness = yield* makeHarness({ gate })
			const message = messageFor('one')

			yield* Effect.gen(function* () {
				const turns = yield* DeliveryTurns
				yield* turns.accept(message)
				expect(yield* harness.saved).toEqual(Option.some(ActiveDeliveryRecord.make({ message, fromSeq: 0 })))
				yield* Deferred.succeed(gate, undefined)
				yield* harness.nextTurnEnded
			}).pipe(Effect.provide(harness.layer))

			expect(yield* harness.sent).toEqual(['fix the bug'])
			expect(yield* harness.applied).toEqual([
				{ deliveryId: message.deliveryId, accessToken: 'secret-token', mutation: working },
				{
					deliveryId: message.deliveryId,
					accessToken: 'secret-token',
					mutation: CompleteDelivery.make({ markdown: 'done' }),
				},
			])
			expect(yield* harness.saved).toEqual(Option.none())
		}),
	)

	it.effect('starts no second turn for a retried callback of the same delivery', ({ expect }) =>
		Effect.gen(function* () {
			const gate = yield* Deferred.make<void>()
			const harness = yield* makeHarness({ gate })
			const message = messageFor('one')

			yield* Effect.gen(function* () {
				const turns = yield* DeliveryTurns
				yield* turns.accept(message)
				yield* turns.accept(message)
				yield* Deferred.succeed(gate, undefined)
				yield* harness.nextTurnEnded
			}).pipe(Effect.provide(harness.layer))

			expect(yield* harness.startedTurns).toBe(0)
			expect(yield* harness.sent).toEqual(['fix the bug'])
			expect((yield* harness.applied).map(({ mutation }) => mutation._tag)).toEqual([
				'SetDeliveryActivity',
				'CompleteDelivery',
			])
		}),
	)

	it.effect('records where the Fold log ended, so a later turn reads only its own entries', ({ expect }) =>
		Effect.gen(function* () {
			const harness = yield* makeHarness({
				log: [userMessage(0, 'earlier'), agentFinished(1, 'completed', 'old')],
			})
			const message = messageFor('two')

			yield* Effect.gen(function* () {
				const turns = yield* DeliveryTurns
				yield* turns.accept(message)
				expect(yield* harness.saved).toEqual(Option.some(ActiveDeliveryRecord.make({ message, fromSeq: 2 })))
				yield* harness.nextTurnEnded
			}).pipe(Effect.provide(harness.layer))

			expect(yield* harness.sent).toEqual(['fix the bug'])
			expect((yield* harness.applied).at(-1)?.mutation).toEqual(CompleteDelivery.make({ markdown: 'done' }))
		}),
	)

	it.effect('fails the delivery when the Fold run ends in an error', ({ expect }) =>
		Effect.gen(function* () {
			const harness = yield* makeHarness({ outcome: 'error' })

			yield* Effect.gen(function* () {
				yield* (yield* DeliveryTurns).accept(messageFor('one'))
				yield* harness.nextTurnEnded
			}).pipe(Effect.provide(harness.layer))

			expect((yield* harness.applied).at(-1)?.mutation).toEqual(
				FailDelivery.make({ markdown: 'I ran into an error and could not finish.' }),
			)
			expect(yield* harness.saved).toEqual(Option.none())
		}),
	)

	it.effect('fails the delivery when the repository cannot be cloned', ({ expect }) =>
		Effect.gen(function* () {
			const harness = yield* makeHarness({ open: Effect.fail(new RepoCloneError({ message: 'no access' })) })

			yield* Effect.gen(function* () {
				yield* (yield* DeliveryTurns).accept(messageFor('one'))
				yield* harness.nextTurnEnded
			}).pipe(Effect.provide(harness.layer))

			expect(yield* harness.sent).toEqual([])
			expect((yield* harness.applied).map(({ mutation }) => mutation)).toEqual([working, somethingWentWrong])
			expect(yield* harness.saved).toEqual(Option.none())
		}),
	)

	it.effect('forgets a delivery that already ended', ({ expect }) =>
		Effect.gen(function* () {
			const harness = yield* makeHarness({ finishError: new DeliveryClosed() })

			yield* Effect.gen(function* () {
				yield* (yield* DeliveryTurns).accept(messageFor('one'))
				yield* harness.nextTurnEnded
			}).pipe(Effect.provide(harness.layer))

			expect(yield* harness.saved).toEqual(Option.none())
		}),
	)
})

describe('DeliveryTurns.recover', () => {
	const message = messageFor('one')
	const saved = ActiveDeliveryRecord.make({ message, fromSeq: 0 })

	const recover = (harness: Effect.Success<ReturnType<typeof makeHarness>>) =>
		Effect.gen(function* () {
			yield* (yield* DeliveryTurns).recover
			yield* harness.nextTurnEnded
		}).pipe(Effect.provide(harness.layer))

	it.effect('nudges a turn a restart cut off, then completes its delivery once', ({ expect }) =>
		Effect.gen(function* () {
			const harness = yield* makeHarness({ saved, log: [userMessage(0, 'fix the bug')] })
			yield* recover(harness)

			expect(yield* harness.sent).toEqual([RESTART_NUDGE])
			expect(yield* harness.applied).toEqual([
				{ deliveryId: message.deliveryId, accessToken: 'secret-token', mutation: working },
				{
					deliveryId: message.deliveryId,
					accessToken: 'secret-token',
					mutation: CompleteDelivery.make({ markdown: 'done' }),
				},
			])
			expect(yield* harness.saved).toEqual(Option.none())
		}),
	)

	it.effect('reports a turn that finished before the restart without running it again', ({ expect }) =>
		Effect.gen(function* () {
			const harness = yield* makeHarness({
				saved,
				log: [userMessage(0, 'fix the bug'), agentFinished(1, 'completed', 'finished before the deploy')],
			})
			yield* recover(harness)

			expect(yield* harness.sent).toEqual([])
			expect((yield* harness.applied).at(-1)?.mutation).toEqual(
				CompleteDelivery.make({ markdown: 'finished before the deploy' }),
			)
		}),
	)

	it.effect('sends the prompt when it never reached Fold', ({ expect }) =>
		Effect.gen(function* () {
			const harness = yield* makeHarness({ saved })
			yield* recover(harness)

			expect(yield* harness.sent).toEqual(['fix the bug'])
			expect(yield* harness.saved).toEqual(Option.none())
		}),
	)

	it.effect('fails the delivery after three restarts without finishing', ({ expect }) =>
		Effect.gen(function* () {
			const harness = yield* makeHarness({
				saved,
				log: [
					userMessage(0, 'fix the bug'),
					userMessage(1, RESTART_NUDGE),
					userMessage(2, RESTART_NUDGE),
					userMessage(3, RESTART_NUDGE),
				],
			})
			yield* recover(harness)

			expect(yield* harness.sent).toEqual([])
			expect((yield* harness.applied).at(-1)?.mutation).toEqual(somethingWentWrong)
			expect(yield* harness.saved).toEqual(Option.none())
		}),
	)

	it.effect('does nothing with no saved delivery', ({ expect }) =>
		Effect.gen(function* () {
			const harness = yield* makeHarness()
			yield* Effect.gen(function* () {
				yield* (yield* DeliveryTurns).recover
			}).pipe(Effect.provide(harness.layer))

			expect(yield* harness.startedTurns).toBe(0)
			expect(yield* harness.applied).toEqual([])
		}),
	)
})
