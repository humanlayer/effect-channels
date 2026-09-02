import { Context, Effect, Layer } from 'effect'

import type { ObserverError } from './Errors.ts'
import type { InboundEvent } from './Events.ts'
import type { OutboundReport } from './Operations.ts'

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
			eventReceived: (event) =>
				Effect.logInfo('channels inbound event accepted').pipe(
					Effect.annotateLogs({
						event_kind: event._tag,
						provider: event.provider,
						tenant: event.tenant,
						idempotency_key: event.idempotencyKey,
					}),
				),
			outboundSent: (report) =>
				Effect.logInfo('channels outbound operation completed').pipe(
					Effect.annotateLogs({
						operation: report.operation,
						provider: report.provider,
						tenant: report.tenant,
						thread_id: report.threadId,
						ok: report.ok,
					}),
				),
		}),
	)

	static make(handlers: {
		readonly eventReceived: (event: InboundEvent) => Effect.Effect<void, ObserverError>
		readonly outboundSent: (report: OutboundReport) => Effect.Effect<void, ObserverError>
	}) {
		return Layer.succeed(ChannelsObserver, ChannelsObserver.of(handlers))
	}
}
