import { layerOutputStore } from '@humanlayer/fold-agent'
import * as Cloudflare from 'alchemy/Cloudflare'
import type { RuntimeContext } from 'alchemy/RuntimeContext'
import { Context, Effect, Layer, Schema } from 'effect'

import { ActiveDelivery } from './ActiveDelivery'
import { AgentConversation, TOOL_OUTPUT_DIRECTORY } from './AgentConversation'
import { AgentSessionMessage } from './AgentSessionMessage'
import { DeliveryTurns } from './DeliveryTurn'
import * as FoldLog from './DurableObjectSqliteFoldAgentEventLog'
import { SessionExpiry } from './SessionExpiry'
import { RunRecovery } from './SessionRecovery'
import { Workspace } from './Workspace'

const WORKSPACE_GRACE_MILLIS = 24 * 60 * 60 * 1_000

/**
 * The AgentSession namespace; callers address it directly by the GitHub discussion's mailbox key. `send`
 * saves the handed-off delivery and starts its turn in the background, returning once the turn is accepted;
 * the turn reports its result to the delivery.
 */
export class AgentSession extends Cloudflare.DurableObject<
	AgentSession,
	{
		readonly send: (input: typeof AgentSessionMessage.Encoded) => Effect.Effect<void, never, RuntimeContext>
		readonly alarm: () => Effect.Effect<void, never, RuntimeContext>
	}
>()('AgentSession') {}

/** The AgentSession namespace as a service, as `DeliveryMailboxes` is for mailboxes. */
export class AgentSessions extends Context.Service<
	AgentSessions,
	{
		readonly getByName: (mailboxKey: string) => {
			readonly send: (input: typeof AgentSessionMessage.Encoded) => Effect.Effect<void, never, RuntimeContext>
		}
	}
>()('alchemy-cloudflare/AgentSessions', {
	/** The namespace of {@link AgentSession}. */
	make: AgentSession,
}) {
	static readonly layer = Layer.effect(this, this.make)
}

/** This object's services. The Worker entrypoint provides their dependencies. */
const AgentSessionLive = DeliveryTurns.layer.pipe(
	Layer.provideMerge(AgentConversation.layer),
	Layer.provideMerge(Layer.mergeAll(ActiveDelivery.layer, RunRecovery.layer, SessionExpiry.layer)),
	Layer.provideMerge(layerOutputStore({ directory: TOOL_OUTPUT_DIRECTORY })),
	Layer.provideMerge(Workspace.fileSystemLayer),
	Layer.provideMerge(Workspace.layer),
)

/**
 * Build this object's services, then, once the object is live, create the Fold log's table, recover a turn a
 * deploy or crash cut off, and return the RPC methods.
 */
const AgentSessionImplementation = Effect.gen(function* () {
	const state = yield* Cloudflare.DurableObjectState
	const workspace = yield* Workspace
	const recovery = yield* RunRecovery
	const expiry = yield* SessionExpiry
	const turns = yield* DeliveryTurns

	return Effect.gen(function* () {
		const mailboxKey = yield* Schema.decodeUnknownEffect(Schema.NonEmptyString)(state.id.name)
		yield* FoldLog.createTable(state.raw.storage)
		if (!(yield* expiry.expired)) {
			yield* expiry.ensureScheduled
			yield* turns.recover
		}

		return {
			send: Effect.fn('agent_session.send')(function* (input: typeof AgentSessionMessage.Encoded) {
				const message = yield* Schema.decodeEffect(AgentSessionMessage)(input).pipe(Effect.orDie)
				if (message.githubDiscussion.mailboxKey !== mailboxKey) {
					return yield* Effect.die(
						new Error('AgentSession message does not match this Durable Object mailbox key'),
					)
				}
				const deadline = yield* expiry.touch
				yield* workspace.expireAt(deadline + WORKSPACE_GRACE_MILLIS)
				return yield* turns.accept(message)
			}),
			alarm: () =>
				Effect.flatMap(recovery.alarm, (recoveryOwnsAlarm) =>
					recoveryOwnsAlarm ? Effect.void : expiry.alarm(workspace.destroy),
				),
		}
	}).pipe(
		Effect.tapCause((cause) => Effect.logError('AgentSession could not start', cause)),
		Effect.orDie,
	)
}).pipe(Effect.provide(AgentSessionLive))

/** Register the AgentSession implementation in the Worker runtime. */
export const AgentSessionDOLive = AgentSession.make(AgentSessionImplementation)
