import { Context, Duration, Effect, Layer, Match, Queue, Ref, Schedule } from 'effect'

import type { ConversationCoordinatorUnavailable, ConversationLeaseLost } from './Errors.ts'
import type { InboundEvent } from './Events.ts'
import { unimplemented } from './internal/unimplemented.ts'
import type { CancelConversationInput, ConversationCoordinatorOptions } from './Operations.ts'
import { ConversationCoordinatorOptions as ConversationCoordinatorOptionsSchema } from './Operations.ts'
import type { ThreadId } from './Schema.ts'
import { ThreadId as ThreadIdSchema } from './Schema.ts'

const defaultOptions = ConversationCoordinatorOptionsSchema.make({
	leaseTtlMs: 30_000,
	heartbeatEveryMs: 10_000,
	acquireTimeoutMs: 30_000,
	retryBaseMs: 100,
	retryMaxMs: 30_000,
	alertAfterAttempts: 3,
})

const deliverWithRetry = <E, R>(input: {
	readonly options: ConversationCoordinatorOptions
	readonly threadId: ThreadId
	readonly event: InboundEvent
	readonly handler: (event: InboundEvent) => Effect.Effect<void, E, R>
}): Effect.Effect<void, E, R> =>
	Effect.gen(function* () {
		const attempts = yield* Ref.make(0)
		const backoff = Schedule.exponential(input.options.retryBaseMs).pipe(
			Schedule.modifyDelay((metadata) =>
				Effect.succeed(Duration.min(metadata.duration, Duration.millis(input.options.retryMaxMs))),
			),
		)
		yield* input.handler(input.event).pipe(
			Effect.tapError((error) =>
				Effect.gen(function* () {
					const attempt = yield* Ref.updateAndGet(attempts, (count) => count + 1)
					const capture =
						attempt >= input.options.alertAfterAttempts
							? Effect.logError('channels conversation delivery keeps failing', error)
							: Effect.logWarning('channels conversation delivery failed; retrying', error)
					yield* capture.pipe(
						Effect.annotateLogs({
							thread_id: input.threadId,
							idempotency_key: input.event.idempotencyKey,
							attempt,
						}),
					)
				}),
			),
			Effect.retry(backoff),
		)
	})

const eventThreadId = Match.type<InboundEvent>().pipe(
	Match.tagsExhaustive({
		MessageEvent: (event) => event.thread.ref.id,
		MessageUpdatedEvent: (event) => event.thread.ref.id,
		MessageDeletedEvent: (event) => event.threadRef.id,
		ConversationStoppedEvent: (event) => event.threadRef.id,
		AssignedEvent: (event) => event.thread.ref.id,
		ActionEvent: (event) => event.thread.ref.id,
		ReactionEvent: (event) => event.thread.ref.id,
		CommandEvent: (event) =>
			event.thread === undefined ? ThreadIdSchema.make(`${event.channel.ref.id}:command`) : event.thread.ref.id,
	}),
)

export class ConversationCoordinator extends Context.Service<
	ConversationCoordinator,
	{
		readonly submit: (event: InboundEvent) => Effect.Effect<void, ConversationCoordinatorUnavailable>
		readonly requestCancellation: (
			input: CancelConversationInput,
		) => Effect.Effect<void, ConversationCoordinatorUnavailable>
		readonly run: <E, R>(
			handler: (event: InboundEvent) => Effect.Effect<void, E, R>,
		) => Effect.Effect<never, E | ConversationLeaseLost | ConversationCoordinatorUnavailable, R>
	}
>()('channels/ConversationCoordinator') {
	static layerMemory(options: ConversationCoordinatorOptions = defaultOptions) {
		return Layer.effect(
			ConversationCoordinator,
			Effect.gen(function* () {
				const ready = yield* Queue.unbounded<ThreadId>()
				const mailboxes = new Map<ThreadId, Array<InboundEvent>>()
				const scheduled = new Set<ThreadId>()
				const active = new Set<ThreadId>()
				const accepted = new Set<string>()

				const submit = (event: InboundEvent) =>
					Effect.suspend(() => {
						if (accepted.has(event.idempotencyKey)) {
							return Effect.void
						}
						accepted.add(event.idempotencyKey)
						const threadId = eventThreadId(event)
						const mailbox = mailboxes.get(threadId) ?? []
						mailbox.push(event)
						mailboxes.set(threadId, mailbox)
						if (active.has(threadId) || scheduled.has(threadId)) {
							return Effect.void
						}
						scheduled.add(threadId)
						return Queue.offer(ready, threadId).pipe(Effect.asVoid)
					})

				const run = <E, R>(handler: (event: InboundEvent) => Effect.Effect<void, E, R>) => {
					const drain = (threadId: ThreadId): Effect.Effect<void, E, R> =>
						Effect.suspend(() => {
							const mailbox = mailboxes.get(threadId)
							const event = mailbox?.at(0)
							if (event === undefined || mailbox === undefined) {
								active.delete(threadId)
								mailboxes.delete(threadId)
								return Effect.void
							}
							return deliverWithRetry({ options, threadId, event, handler }).pipe(
								Effect.tap(() => Effect.sync(() => mailbox.shift())),
								Effect.andThen(drain(threadId)),
							)
						})

					const claimAndDrain = Effect.flatMap(Queue.take(ready), (threadId) => {
						scheduled.delete(threadId)
						active.add(threadId)
						return drain(threadId).pipe(
							Effect.withSpan('channels.conversation_coordinator', {
								attributes: { thread_id: threadId },
							}),
							Effect.forkChild,
						)
					})
					return Effect.forever(claimAndDrain)
				}

				return ConversationCoordinator.of({
					submit,
					requestCancellation: () => unimplemented('ConversationCoordinator.requestCancellation'),
					run,
				})
			}),
		)
	}

	static layerPostgres(_options: ConversationCoordinatorOptions = defaultOptions) {
		return Layer.effect(ConversationCoordinator, unimplemented('ConversationCoordinator.layerPostgres'))
	}
}
