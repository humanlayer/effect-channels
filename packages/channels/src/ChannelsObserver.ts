import { Context, Effect, Layer, Match } from 'effect'

import type { ObserverError } from './Errors.ts'
import type { InboundEvent } from './Events.ts'
import type { OutboundReport } from './Operations.ts'

const eventThreadId = Match.type<InboundEvent>().pipe(
	Match.tagsExhaustive({
		MessageEvent: (event) => event.thread.ref.id,
		MessageUpdatedEvent: (event) => event.thread.ref.id,
		MessageDeletedEvent: (event) => event.threadRef.id,
		ConversationStoppedEvent: (event) => event.threadRef.id,
		AssignedEvent: (event) => event.thread.ref.id,
		ActionEvent: (event) => event.thread.ref.id,
		ReactionEvent: (event) => event.thread.ref.id,
		CommandEvent: (event) => event.thread?.ref.id,
	}),
)

export class ChannelsObserver extends Context.Service<
	ChannelsObserver,
	{
		readonly eventReceived: (event: InboundEvent) => Effect.Effect<void, ObserverError>
		readonly outboundSent: (report: OutboundReport) => Effect.Effect<void, ObserverError>
	}
>()('channels/ChannelsObserver') {
	static readonly layerLogger = Layer.succeed(
		ChannelsObserver,
		ChannelsObserver.of({
			eventReceived: (event) => {
				const threadId = eventThreadId(event)
				const base = {
					event_kind: event._tag,
					provider: event.provider,
					tenant: event.tenant,
					idempotency_key: event.idempotencyKey,
				}
				const annotations = threadId === undefined ? base : { ...base, thread_id: threadId }
				return Effect.logInfo('channels inbound event accepted').pipe(Effect.annotateLogs(annotations))
			},
			outboundSent: (report) => {
				const log = report.ok
					? Effect.logInfo('channels outbound operation completed')
					: Effect.logWarning('channels outbound operation failed')
				return log.pipe(
					Effect.annotateLogs({
						operation: report.operation,
						org_id: report.orgId,
						provider: report.provider,
						tenant: report.tenant,
						thread_id: report.threadId,
						ok: report.ok,
						degraded: report.degraded,
					}),
				)
			},
		}),
	)

	static make(handlers: {
		readonly eventReceived: (event: InboundEvent) => Effect.Effect<void, ObserverError>
		readonly outboundSent: (report: OutboundReport) => Effect.Effect<void, ObserverError>
	}) {
		return Layer.succeed(ChannelsObserver, ChannelsObserver.of(handlers))
	}
}
