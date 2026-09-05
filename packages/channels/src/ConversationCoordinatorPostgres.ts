import { Cause, Effect, Exit, Fiber, FiberSet, Layer, Match, Option, Queue, Schema } from 'effect'
import { SqlClient } from 'effect/unstable/sql'

import type { ConversationCoordinator } from './ConversationCoordinator.ts'
import { ConversationCoordinatorUnavailable, ConversationLeaseLost, isNonRetryableCause } from './Errors.ts'
import { InboundEvent, type ConversationStoppedEvent } from './Events.ts'
import type { ConversationCoordinatorOptions } from './Operations.ts'
import type { ThreadId } from './Schema.ts'
import { ThreadId as ThreadIdSchema } from './Schema.ts'

const eventJson = Schema.fromJsonString(Schema.toCodecJson(InboundEvent))

const RuntimeIdentityRow = Schema.Struct({ runtime_instance_id: Schema.NonEmptyString })

const ClaimRow = Schema.Struct({
	thread_id: ThreadIdSchema,
	sequence: Schema.NonEmptyString,
	attempts: Schema.Int,
	event_json: Schema.String,
	lease_token: Schema.NonEmptyString,
	cancellation_generation: Schema.Int,
})
type ClaimRow = typeof ClaimRow.Type

const OwnershipRow = Schema.Struct({ thread_id: ThreadIdSchema })

const RenewalRow = Schema.Struct({ cancellation_generation: Schema.Int })

const NextEventRow = Schema.Struct({
	sequence: Schema.NonEmptyString,
	attempts: Schema.Int,
	event_json: Schema.String,
})

const encodeEvent = Schema.encodeUnknownEffect(eventJson)
const decodeEvent = Schema.decodeUnknownEffect(eventJson)
const decodeRuntimeIdentityRow = Schema.decodeUnknownEffect(RuntimeIdentityRow)
const decodeClaimRow = Schema.decodeUnknownEffect(ClaimRow)
const decodeOwnershipRow = Schema.decodeUnknownEffect(OwnershipRow)
const decodeRenewalRow = Schema.decodeUnknownEffect(RenewalRow)
const decodeNextEventRow = Schema.decodeUnknownEffect(NextEventRow)

const coordinatorUnavailable = (operation: string) =>
	ConversationCoordinatorUnavailable.make({ operation, message: 'Postgres conversation coordinator unavailable' })

const captureAndNarrow = <A, E, R>(operation: string, effect: Effect.Effect<A, E, R>) =>
	effect.pipe(
		Effect.tapError((error) =>
			Effect.logError('Postgres conversation coordinator operation failed', error).pipe(
				Effect.annotateLogs({ operation }),
			),
		),
		Effect.mapError(() => coordinatorUnavailable(operation)),
	)

export const ConversationCoordinatorPostgresMigrations = Effect.gen(function* () {
	const sql = (yield* SqlClient.SqlClient).withoutTransforms()
	yield* sql`
		CREATE TABLE IF NOT EXISTS channels_conversations (
			thread_id text PRIMARY KEY,
			ready_at timestamptz,
			lease_owner text,
			lease_token text,
			lease_expires_at timestamptz,
			active_sequence bigint,
			cancellation_generation bigint NOT NULL DEFAULT 0,
			created_at timestamptz NOT NULL DEFAULT now(),
			updated_at timestamptz NOT NULL DEFAULT now()
		)
	`
	yield* sql`
		CREATE TABLE IF NOT EXISTS channels_conversation_mailbox (
			sequence bigserial PRIMARY KEY,
			thread_id text NOT NULL REFERENCES channels_conversations(thread_id) ON DELETE CASCADE,
			idempotency_key text NOT NULL UNIQUE,
			event_json jsonb NOT NULL,
			status text NOT NULL DEFAULT 'pending'
				CHECK (status IN ('pending', 'completed', 'cancelled_by_provider')),
			attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
			available_at timestamptz NOT NULL DEFAULT now(),
			created_at timestamptz NOT NULL DEFAULT now(),
			completed_at timestamptz
		)
	`
	yield* sql`
		CREATE INDEX IF NOT EXISTS channels_conversation_mailbox_pending_idx
		ON channels_conversation_mailbox (thread_id, sequence)
		WHERE status = 'pending'
	`
	yield* sql`
		CREATE INDEX IF NOT EXISTS channels_conversations_ready_idx
		ON channels_conversations (ready_at, lease_expires_at)
	`
}).pipe(Effect.asVoid)

type ClaimedConversation = {
	readonly threadId: ThreadId
	readonly sequence: string
	readonly attempts: number
	readonly event: InboundEvent
	readonly leaseToken: string
	readonly cancellationGeneration: number
}

const decodeClaim = (row: ClaimRow) =>
	decodeEvent(row.event_json).pipe(
		Effect.map((event): ClaimedConversation => ({
			threadId: row.thread_id,
			sequence: row.sequence,
			attempts: row.attempts,
			event,
			leaseToken: row.lease_token,
			cancellationGeneration: row.cancellation_generation,
		})),
	)

const retryDelay = (options: ConversationCoordinatorOptions, attempts: number) =>
	Math.min(options.retryMaxMs, options.retryBaseMs * 2 ** Math.min(attempts, 30))

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

export const conversationCoordinatorPostgresLayer = (
	service: typeof ConversationCoordinator,
	options: ConversationCoordinatorOptions,
) =>
	Layer.effect(
		service,
		Effect.gen(function* () {
			const sql = (yield* SqlClient.SqlClient).withoutTransforms()
			const wake = yield* Queue.sliding<void>(1)
			yield* captureAndNarrow('ConversationCoordinator.migrate', ConversationCoordinatorPostgresMigrations)
			const runtimeRows = yield* captureAndNarrow(
				'ConversationCoordinator.runtimeIdentity',
				sql`SELECT gen_random_uuid()::text AS runtime_instance_id`,
			)
			const runtimeRow = yield* captureAndNarrow(
				'ConversationCoordinator.runtimeIdentity.decode',
				decodeRuntimeIdentityRow(runtimeRows.at(0)),
			)
			const runtimeInstanceId = runtimeRow.runtime_instance_id

			const submit = Effect.fn('channels.conversation_coordinator.submit')(function* (event: InboundEvent) {
				const threadId = eventThreadId(event)
				const encoded = yield* captureAndNarrow('ConversationCoordinator.submit.encode', encodeEvent(event))
				const wasInserted = yield* captureAndNarrow(
					'ConversationCoordinator.submit',
					sql.withTransaction(
						Effect.gen(function* () {
							yield* sql`
								INSERT INTO channels_conversations (thread_id, ready_at)
								VALUES (${threadId}, now())
								ON CONFLICT (thread_id) DO NOTHING
							`
							const inserted = yield* sql<{ readonly idempotency_key: string }>`
								INSERT INTO channels_conversation_mailbox (thread_id, idempotency_key, event_json)
								VALUES (${threadId}, ${event.idempotencyKey}, ${encoded}::jsonb)
								ON CONFLICT (idempotency_key) DO NOTHING
								RETURNING idempotency_key
							`
							if (inserted.length > 0) {
								yield* sql`
									UPDATE channels_conversations
									SET ready_at = COALESCE(ready_at, now()), updated_at = now()
									WHERE thread_id = ${threadId}
								`
							}
							return inserted.length > 0
						}),
					),
				)
				if (wasInserted) yield* Queue.offer(wake, undefined)
				return wasInserted
			})

			const submitCancellation = Effect.fn('channels.conversation_coordinator.submit_cancellation')(function* (
				event: ConversationStoppedEvent,
			) {
				const threadId = event.threadRef.id
				const encoded = yield* captureAndNarrow(
					'ConversationCoordinator.submitCancellation.encode',
					encodeEvent(event),
				)
				const wasInserted = yield* captureAndNarrow(
					'ConversationCoordinator.submitCancellation',
					sql.withTransaction(
						Effect.gen(function* () {
							yield* sql`
								INSERT INTO channels_conversations (thread_id, ready_at)
								VALUES (${threadId}, now())
								ON CONFLICT (thread_id) DO NOTHING
							`
							const inserted = yield* sql<{ readonly idempotency_key: string }>`
								INSERT INTO channels_conversation_mailbox (thread_id, idempotency_key, event_json)
								VALUES (${threadId}, ${event.idempotencyKey}, ${encoded}::jsonb)
								ON CONFLICT (idempotency_key) DO NOTHING
								RETURNING idempotency_key
							`
							if (inserted.length === 0) return false
							yield* sql`
								SELECT thread_id
								FROM channels_conversations
								WHERE thread_id = ${threadId}
								FOR UPDATE
							`
							yield* sql`
								UPDATE channels_conversation_mailbox AS mailbox
								SET status = 'cancelled_by_provider', completed_at = now()
								FROM channels_conversations AS conversation
								WHERE conversation.thread_id = ${threadId}
									AND conversation.active_sequence = mailbox.sequence
									AND mailbox.status = 'pending'
							`
							yield* sql`
								UPDATE channels_conversations
								SET cancellation_generation = CASE
										WHEN active_sequence IS NULL THEN cancellation_generation
										ELSE cancellation_generation + 1
									END,
									ready_at = COALESCE(ready_at, now()),
									updated_at = now()
								WHERE thread_id = ${threadId}
							`
							return true
						}),
					),
				)
				if (wasInserted) yield* Queue.offer(wake, undefined)
				return wasInserted
			})

			const claim = () =>
				captureAndNarrow(
					'ConversationCoordinator.claim',
					sql.withTransaction(
						Effect.gen(function* () {
							const rows = yield* sql<ClaimRow>`
							WITH candidate AS (
								SELECT conversation.thread_id, mailbox.sequence
								FROM channels_conversations AS conversation
								JOIN LATERAL (
									SELECT sequence, available_at
									FROM channels_conversation_mailbox
									WHERE thread_id = conversation.thread_id AND status = 'pending'
									ORDER BY sequence ASC
									LIMIT 1
								) AS mailbox ON true
								WHERE mailbox.available_at <= now()
									AND (conversation.lease_expires_at IS NULL OR conversation.lease_expires_at <= now())
								ORDER BY mailbox.available_at ASC, mailbox.sequence ASC
								FOR UPDATE OF conversation SKIP LOCKED
								LIMIT 1
							), claimed AS (
								UPDATE channels_conversations AS conversation
								SET lease_owner = ${runtimeInstanceId},
									lease_token = gen_random_uuid()::text,
									lease_expires_at = now() + (${options.leaseTtlMs} * interval '1 millisecond'),
									active_sequence = candidate.sequence,
									ready_at = NULL,
									updated_at = now()
								FROM candidate
								WHERE conversation.thread_id = candidate.thread_id
								RETURNING conversation.thread_id, conversation.active_sequence, conversation.lease_token,
									conversation.cancellation_generation
							)
							SELECT claimed.thread_id,
								claimed.active_sequence::text AS sequence,
								mailbox.attempts,
								mailbox.event_json::text AS event_json,
								claimed.lease_token,
								claimed.cancellation_generation::int AS cancellation_generation
							FROM claimed
							JOIN channels_conversation_mailbox AS mailbox
								ON mailbox.sequence = claimed.active_sequence
						`
							const row = rows.at(0)
							if (row === undefined) {
								return Option.none<ClaimedConversation>()
							}
							const decoded = yield* decodeClaimRow(row)
							return Option.some(yield* decodeClaim(decoded))
						}),
					),
				).pipe(
					Effect.timeoutOrElse({
						duration: options.acquireTimeoutMs,
						orElse: () => Effect.fail(coordinatorUnavailable('ConversationCoordinator.claim.timeout')),
					}),
				)

			const assertOwnership = (claimed: ClaimedConversation) =>
				captureAndNarrow(
					'ConversationCoordinator.assertOwnership',
					Effect.gen(function* () {
						const rows = yield* sql`
							SELECT thread_id
							FROM channels_conversations
							WHERE thread_id = ${claimed.threadId}
								AND lease_token = ${claimed.leaseToken}
								AND active_sequence = ${claimed.sequence}::bigint
								AND lease_expires_at > now()
							FOR UPDATE
						`
						const row = rows.at(0)
						if (row === undefined) {
							return Option.none<ThreadId>()
						}
						const decoded = yield* decodeOwnershipRow(row)
						return Option.some(decoded.thread_id)
					}),
				)

			const renewLease = (
				claimed: ClaimedConversation,
			): Effect.Effect<boolean, ConversationLeaseLost | ConversationCoordinatorUnavailable> =>
				captureAndNarrow(
					'ConversationCoordinator.renewLease',
					sql<{ readonly cancellation_generation: number }>`
						UPDATE channels_conversations
						SET lease_expires_at = now() + (${options.leaseTtlMs} * interval '1 millisecond'),
							updated_at = now()
						WHERE thread_id = ${claimed.threadId}
							AND lease_token = ${claimed.leaseToken}
							AND lease_expires_at > now()
						RETURNING cancellation_generation::int AS cancellation_generation
					`,
				).pipe(
					Effect.flatMap(
						(rows): Effect.Effect<boolean, ConversationLeaseLost | ConversationCoordinatorUnavailable> => {
							const row = rows.at(0)
							return row === undefined
								? Effect.fail(ConversationLeaseLost.make({ threadId: claimed.threadId }))
								: decodeRenewalRow(row).pipe(
										Effect.map(
											(decoded) =>
												decoded.cancellation_generation > claimed.cancellationGeneration,
										),
										Effect.mapError(() =>
											coordinatorUnavailable('ConversationCoordinator.renewLease.decode'),
										),
									)
						},
					),
				)

			const releaseLease = (claimed: ClaimedConversation) =>
				sql`
					UPDATE channels_conversations AS conversation
					SET lease_owner = NULL,
						lease_token = NULL,
						lease_expires_at = NULL,
						active_sequence = NULL,
						ready_at = (
							SELECT available_at
							FROM channels_conversation_mailbox
							WHERE thread_id = conversation.thread_id AND status = 'pending'
							ORDER BY sequence ASC
							LIMIT 1
						),
						updated_at = now()
					WHERE thread_id = ${claimed.threadId} AND lease_token = ${claimed.leaseToken}
				`.pipe(
					Effect.tapError((error) =>
						Effect.logError('failed to release Postgres conversation lease', error).pipe(
							Effect.annotateLogs({
								thread_id: claimed.threadId,
								runtime_instance_id: runtimeInstanceId,
							}),
						),
					),
					Effect.ignore,
				)

			const advanceEvent = (claimed: ClaimedConversation, status: 'completed' | 'cancelled_by_provider') => {
				const operation = 'ConversationCoordinator.complete'
				return sql
					.withTransaction(
						Effect.gen(function* () {
							const owned = yield* assertOwnership(claimed)
							if (Option.isNone(owned)) {
								return yield* ConversationLeaseLost.make({ threadId: claimed.threadId })
							}
							yield* sql`
								UPDATE channels_conversation_mailbox
								SET status = ${status}, completed_at = now()
								WHERE sequence = ${claimed.sequence}::bigint AND status = 'pending'
							`
							const rows = yield* sql`
								SELECT sequence::text AS sequence, attempts, event_json::text AS event_json
								FROM channels_conversation_mailbox
								WHERE thread_id = ${claimed.threadId} AND status = 'pending'
								ORDER BY sequence ASC
								LIMIT 1
							`
							const row = rows.at(0)
							if (row === undefined) {
								yield* sql`
									UPDATE channels_conversations
									SET lease_owner = NULL,
										lease_token = NULL,
										lease_expires_at = NULL,
										active_sequence = NULL,
										ready_at = NULL,
										updated_at = now()
									WHERE thread_id = ${claimed.threadId} AND lease_token = ${claimed.leaseToken}
								`
								return Option.none<ClaimedConversation>()
							}
							const decoded = yield* decodeNextEventRow(row)
							const generations = yield* sql<{ readonly cancellation_generation: number }>`
								UPDATE channels_conversations
								SET active_sequence = ${decoded.sequence}::bigint,
									lease_expires_at = now() + (${options.leaseTtlMs} * interval '1 millisecond'),
									updated_at = now()
								WHERE thread_id = ${claimed.threadId} AND lease_token = ${claimed.leaseToken}
								RETURNING cancellation_generation::int AS cancellation_generation
							`
							const generation = yield* decodeRenewalRow(generations.at(0))
							return Option.some(
								yield* decodeClaim({
									thread_id: claimed.threadId,
									sequence: decoded.sequence,
									attempts: decoded.attempts,
									event_json: decoded.event_json,
									lease_token: claimed.leaseToken,
									cancellation_generation: generation.cancellation_generation,
								}),
							)
						}),
					)
					.pipe(
						Effect.tapErrorTag('SqlError', (error) =>
							Effect.logError('Postgres conversation coordinator SQL operation failed', error).pipe(
								Effect.annotateLogs({ operation }),
							),
						),
						Effect.tapErrorTag('SchemaError', (error) =>
							Effect.logError('Postgres conversation coordinator decode failed', error).pipe(
								Effect.annotateLogs({ operation }),
							),
						),
						Effect.catchTags({
							SqlError: () => Effect.fail(coordinatorUnavailable(operation)),
							SchemaError: () => Effect.fail(coordinatorUnavailable(operation)),
						}),
					)
			}
			const nextEvent = (claimed: ClaimedConversation) => advanceEvent(claimed, 'completed')

			const retryEvent = (claimed: ClaimedConversation, cause: Cause.Cause<unknown>) => {
				const attempt = claimed.attempts + 1
				const delay = retryDelay(options, claimed.attempts)
				const capture =
					attempt >= options.alertAfterAttempts
						? Effect.logError('channels conversation delivery keeps failing', cause)
						: Effect.logWarning('channels conversation delivery failed; retrying', cause)
				return capture.pipe(
					Effect.annotateLogs({
						thread_id: claimed.threadId,
						idempotency_key: claimed.event.idempotencyKey,
						runtime_instance_id: runtimeInstanceId,
						attempt,
					}),
					Effect.andThen(
						(() => {
							const operation = 'ConversationCoordinator.retry'
							return sql
								.withTransaction(
									Effect.gen(function* () {
										const owned = yield* assertOwnership(claimed)
										if (Option.isNone(owned)) {
											return yield* ConversationLeaseLost.make({ threadId: claimed.threadId })
										}
										yield* sql`
										UPDATE channels_conversation_mailbox
										SET attempts = attempts + 1,
											available_at = now() + (${delay} * interval '1 millisecond')
										WHERE sequence = ${claimed.sequence}::bigint AND status = 'pending'
									`
										yield* sql`
										UPDATE channels_conversations
										SET lease_owner = NULL,
											lease_token = NULL,
											lease_expires_at = NULL,
											active_sequence = NULL,
											ready_at = now() + (${delay} * interval '1 millisecond'),
											updated_at = now()
										WHERE thread_id = ${claimed.threadId} AND lease_token = ${claimed.leaseToken}
									`
									}),
								)
								.pipe(
									Effect.tapErrorTag('SqlError', (error) =>
										Effect.logError(
											'Postgres conversation coordinator SQL operation failed',
											error,
										).pipe(Effect.annotateLogs({ operation })),
									),
									Effect.catchTags({
										SqlError: () => Effect.fail(coordinatorUnavailable(operation)),
									}),
								)
						})(),
					),
				)
			}

			const completeNonRetryableEvent = (claimed: ClaimedConversation) =>
				Effect.logError('channels conversation delivery failed permanently; will not retry').pipe(
					Effect.annotateLogs({
						thread_id: claimed.threadId,
						idempotency_key: claimed.event.idempotencyKey,
						runtime_instance_id: runtimeInstanceId,
						retryability: 'non_retryable',
					}),
					Effect.andThen(nextEvent(claimed)),
				)

			const heartbeat = (
				claimed: ClaimedConversation,
			): Effect.Effect<void, ConversationLeaseLost | ConversationCoordinatorUnavailable> =>
				Effect.sleep(options.heartbeatEveryMs).pipe(
					Effect.andThen(renewLease(claimed)),
					Effect.tap(() =>
						Effect.logDebug('renewed Postgres conversation lease').pipe(
							Effect.annotateLogs({
								thread_id: claimed.threadId,
								runtime_instance_id: runtimeInstanceId,
							}),
						),
					),
					Effect.flatMap((cancelled) => (cancelled ? Effect.void : heartbeat(claimed))),
				)

			const deliver = <E, R>(
				claimed: ClaimedConversation,
				handler: (event: InboundEvent) => Effect.Effect<void, E, R>,
			): Effect.Effect<
				Option.Option<ClaimedConversation>,
				E | ConversationLeaseLost | ConversationCoordinatorUnavailable,
				R
			> =>
				Effect.gen(function* () {
					const handlerFiber = yield* handler(claimed.event).pipe(Effect.forkChild)
					const outcome = yield* Effect.raceFirst(
						Fiber.await(handlerFiber).pipe(
							Effect.map((exit) => ({ _tag: 'HandlerCompleted' as const, exit })),
						),
						heartbeat(claimed).pipe(Effect.as({ _tag: 'Cancelled' as const })),
					)
					yield* Match.value(outcome).pipe(
						Match.tagsExhaustive({
							Cancelled: () => Fiber.interrupt(handlerFiber),
							HandlerCompleted: () => Effect.void,
						}),
					)
					return yield* Effect.succeed(outcome)
				}).pipe(
					Effect.flatMap(
						Match.type<
							| { readonly _tag: 'HandlerCompleted'; readonly exit: Exit.Exit<void, E> }
							| { readonly _tag: 'Cancelled' }
						>().pipe(
							Match.tagsExhaustive({
								Cancelled: () =>
									Effect.logInfo('conversation event cancelled by provider').pipe(
										Effect.annotateLogs({
											thread_id: claimed.threadId,
											idempotency_key: claimed.event.idempotencyKey,
										}),
										Effect.andThen(advanceEvent(claimed, 'cancelled_by_provider')),
									),
								HandlerCompleted: ({ exit }) =>
									Match.value(exit).pipe(
										Match.tagsExhaustive({
											Success: () => nextEvent(claimed),
											Failure: ({ cause }) =>
												isNonRetryableCause(cause)
													? completeNonRetryableEvent(claimed)
													: Cause.hasInterruptsOnly(cause)
														? Effect.failCause(cause)
														: retryEvent(claimed, cause).pipe(
																Effect.as(Option.none<ClaimedConversation>()),
															),
										}),
									),
							}),
						),
					),
				)

			const drain = <E, R>(
				claimed: ClaimedConversation,
				handler: (event: InboundEvent) => Effect.Effect<void, E, R>,
			): Effect.Effect<void, E | ConversationLeaseLost | ConversationCoordinatorUnavailable, R> =>
				deliver(claimed, handler).pipe(
					Effect.flatMap(
						Option.match({
							onNone: () => Effect.void,
							onSome: (next) => drain(next, handler),
						}),
					),
				)

			const run = <E, R>(handler: (event: InboundEvent) => Effect.Effect<void, E, R>) =>
				Effect.scoped(
					Effect.gen(function* () {
						const workers = yield* FiberSet.make<
							void,
							E | ConversationLeaseLost | ConversationCoordinatorUnavailable
						>()
						const claimLoop = Effect.suspend(claim).pipe(
							Effect.flatMap(
								Option.match({
									onNone: () => Effect.raceFirst(Effect.sleep(50), Queue.take(wake)),
									onSome: (claimed) =>
										FiberSet.run(
											workers,
											drain(claimed, handler).pipe(
												Effect.ensuring(releaseLease(claimed)),
												Effect.withSpan('channels.conversation_coordinator', {
													attributes: {
														thread_id: claimed.threadId,
														runtime_instance_id: runtimeInstanceId,
													},
												}),
											),
										).pipe(Effect.asVoid),
								}),
							),
							Effect.forever,
						)
						return yield* Effect.raceFirst(
							claimLoop,
							FiberSet.join(workers).pipe(Effect.andThen(Effect.never)),
						)
					}),
				)

			const requestCancellation = Effect.fn('channels.conversation_coordinator.cancel')(function* (input: {
				readonly threadId: ThreadId
			}) {
				yield* captureAndNarrow(
					'ConversationCoordinator.requestCancellation',
					sql`
						UPDATE channels_conversations
						SET cancellation_generation = cancellation_generation + 1,
							updated_at = now()
						WHERE thread_id = ${input.threadId} AND active_sequence IS NOT NULL
					`,
				)
				yield* Queue.offer(wake, undefined)
			})

			return service.of({
				submit,
				submitCancellation,
				requestCancellation,
				run,
			})
		}),
	)
