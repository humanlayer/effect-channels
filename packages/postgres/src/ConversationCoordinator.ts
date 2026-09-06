import {
	ThreadId,
	type ConversationCoordinatorUnavailable,
	type ConversationLeaseLost,
	type ConversationStoppedEvent,
	type InboundEvent,
} from '@humanlayer/channels-slack'
import { Context, Effect, Schema } from 'effect'

import { conversationCoordinatorPostgresLayer } from './ConversationCoordinatorPostgres.ts'

export const ConversationCoordinatorOptions = Schema.Struct({
	leaseTtlMs: Schema.Int.check(Schema.isGreaterThan(0)),
	heartbeatEveryMs: Schema.Int.check(Schema.isGreaterThan(0)),
	acquireTimeoutMs: Schema.Int.check(Schema.isGreaterThan(0)),
	retryBaseMs: Schema.Int.check(Schema.isGreaterThan(0)),
	retryMaxMs: Schema.Int.check(Schema.isGreaterThan(0)),
	alertAfterAttempts: Schema.Int.check(Schema.isGreaterThan(0)),
})
export type ConversationCoordinatorOptions = typeof ConversationCoordinatorOptions.Type

export const CancelConversationInput = Schema.Struct({
	threadId: ThreadId,
	reason: Schema.Literals(['provider_stop', 'application']),
})
export type CancelConversationInput = typeof CancelConversationInput.Type

const defaultOptions = ConversationCoordinatorOptions.make({
	leaseTtlMs: 30_000,
	heartbeatEveryMs: 10_000,
	acquireTimeoutMs: 30_000,
	retryBaseMs: 100,
	retryMaxMs: 30_000,
	alertAfterAttempts: 3,
})

/** Transitional access to the legacy SQL mailbox; not the shared Delivery engine. */
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
	static layerPostgres(options: ConversationCoordinatorOptions = defaultOptions) {
		return conversationCoordinatorPostgresLayer(ConversationCoordinator, options)
	}
}
