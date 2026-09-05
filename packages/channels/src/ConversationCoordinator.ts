import { Context, Deferred, Duration, Effect, Fiber, Layer, Match, Queue, Ref, Schedule, Schema } from 'effect'

import { conversationCoordinatorPostgresLayer } from './ConversationCoordinatorPostgres.ts'
import type { ConversationCoordinatorUnavailable, ConversationLeaseLost } from './Errors.ts'
import { RetryabilityMetadata, isNonRetryableError } from './Errors.ts'
import type { ConversationStoppedEvent, InboundEvent } from './Events.ts'
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
			Effect.catchIf(
				(error) => Schema.is(RetryabilityMetadata)(error) && isNonRetryableError(error),
				() =>
					Effect.logError('channels conversation delivery failed permanently; will not retry').pipe(
						Effect.annotateLogs({
							thread_id: input.threadId,
							idempotency_key: input.event.idempotencyKey,
							retryability: 'non_retryable',
						}),
					),
			),
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
		readonly submit: (event: InboundEvent) => Effect.Effect<boolean, ConversationCoordinatorUnavailable>
		readonly submitCancellation: (
			event: ConversationStoppedEvent,
		) => Effect.Effect<boolean, ConversationCoordinatorUnavailable>
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
				const activeCancellations = new Map<ThreadId, Deferred.Deferred<void>>()
				const pendingCancellations = new Set<ThreadId>()

				const submit = (event: InboundEvent) =>
					Effect.suspend(() => {
						if (accepted.has(event.idempotencyKey)) {
							return Effect.succeed(false)
						}
						accepted.add(event.idempotencyKey)
						const threadId = eventThreadId(event)
						const mailbox = mailboxes.get(threadId) ?? []
						mailbox.push(event)
						mailboxes.set(threadId, mailbox)
						if (active.has(threadId) || scheduled.has(threadId)) {
							return Effect.succeed(true)
						}
						scheduled.add(threadId)
						return Queue.offer(ready, threadId).pipe(Effect.as(true))
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
							return Effect.gen(function* () {
								const cancellation = yield* Deferred.make<void>()
								activeCancellations.set(threadId, cancellation)
								if (pendingCancellations.delete(threadId))
									yield* Deferred.succeed(cancellation, undefined)
								const deliveryFiber = yield* deliverWithRetry({
									options,
									threadId,
									event,
									handler,
								}).pipe(Effect.forkChild)
								const outcome = yield* Effect.raceFirst(
									Fiber.join(deliveryFiber).pipe(Effect.as('delivered' as const)),
									Deferred.await(cancellation).pipe(Effect.as('cancelled' as const)),
								)
								activeCancellations.delete(threadId)
								if (outcome === 'cancelled') {
									yield* Fiber.interrupt(deliveryFiber)
									yield* Effect.logInfo('conversation event cancelled by provider').pipe(
										Effect.annotateLogs({
											thread_id: threadId,
											idempotency_key: event.idempotencyKey,
										}),
									)
								}
								mailbox.shift()
								yield* drain(threadId)
							}).pipe(Effect.ensuring(Effect.sync(() => activeCancellations.delete(threadId))))
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
					submitCancellation: (event) =>
						Effect.suspend(() => {
							if (accepted.has(event.idempotencyKey)) return Effect.succeed(false)
							accepted.add(event.idempotencyKey)
							const threadId = event.threadRef.id
							const mailbox = mailboxes.get(threadId) ?? []
							const hadPendingWork = mailbox.length > 0
							mailbox.push(event)
							mailboxes.set(threadId, mailbox)
							const activeCancellation = activeCancellations.get(threadId)
							if (activeCancellation !== undefined) {
								return Deferred.succeed(activeCancellation, undefined).pipe(Effect.as(true))
							}
							if (hadPendingWork) pendingCancellations.add(threadId)
							if (active.has(threadId) || scheduled.has(threadId)) return Effect.succeed(true)
							scheduled.add(threadId)
							return Queue.offer(ready, threadId).pipe(Effect.as(true))
						}),
					requestCancellation: ({ threadId }) =>
						Effect.suspend(() => {
							const activeCancellation = activeCancellations.get(threadId)
							if (activeCancellation !== undefined) {
								return Deferred.succeed(activeCancellation, undefined).pipe(Effect.asVoid)
							}
							if ((mailboxes.get(threadId)?.length ?? 0) > 0) pendingCancellations.add(threadId)
							return Effect.void
						}),
					run,
				})
			}),
		)
	}

	static layerPostgres(options: ConversationCoordinatorOptions = defaultOptions) {
		return conversationCoordinatorPostgresLayer(ConversationCoordinator, options)
	}
}
