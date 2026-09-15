/**
 * This file is responsible for locating the configured provider for a given event that was put into a mailbox,
 * and handing it to that provider for execution. The flip side of MailboxDelivery.ts, if you will.
 */
import { Schema, Effect, Predicate } from 'effect'

import { DeliveryAdmission } from './MailboxDelivery'

/** Providers can elect to handle an event that's read */
export const ProviderEventHandled = Schema.TaggedStruct('ProviderEventHandled', {})
export type ProviderEventHandled = typeof ProviderEventHandled.Type

/** or the provider can ignore the event which is still successfull processing */
export const ProviderEventIgnored = Schema.TaggedStruct('ProviderEventIgnored', {
	reason: Schema.NonEmptyString,
})
export type ProviderEventIgnored = typeof ProviderEventIgnored.Type

export const ProviderEventResult = Schema.Union([ProviderEventHandled, ProviderEventIgnored])
export type ProviderEventResult = typeof ProviderEventResult.Type

/**
 * It  is possible that there is no provider configured to process an event - e.g. 'linear' event is in mailbox somehow but we only have slack/github
 * in that case we still know the namespace & provider it was intended for since that was written when the event was delivered to the mailbox initially
 */
export class ProviderEventProcessorNotFound extends Schema.TaggedError<ProviderEventProcessorNotFound>()(
	'ProviderEventProcessorNotFound',
	{
		namespace: Schema.NonEmptyString,
		provider: Schema.NonEmptyString,
	},
) {}

/** The provider could tell us that the event is invalid & unable to be executed */
export class ProviderEventInvalid extends Schema.TaggedError<ProviderEventInvalid>()('ProviderEventInvalid', {
	provider: Schema.NonEmptyString,
	reason: Schema.Literals(['invalid_payload', 'identity_mismatch', 'unsupported_version']),
}) {}

/** The provider could tell us that execution of the user-provided callback failed */
export class ProviderEventExecutionFailed extends Schema.TaggedError<ProviderEventExecutionFailed>()(
	'ProviderEventExecutionFailed',
	{
		provider: Schema.NonEmptyString,
		retryable: Schema.Boolean,
		safeCode: Schema.NonEmptyString,
		retryAfterMs: Schema.optionalKey(
			Schema.Int.check(Schema.isGreaterThanOrEqualTo(0), Schema.isLessThanOrEqualTo(Number.MAX_SAFE_INTEGER)),
		),
	},
) {}

export const ProviderEventProcessorError = Schema.Union([ProviderEventInvalid, ProviderEventExecutionFailed])
export type ProviderEventProcessorError = typeof ProviderEventProcessorError.Type

export const ProviderEventProcessingError = Schema.Union([ProviderEventProcessorError, ProviderEventProcessorNotFound])

export type ProviderEventProcessingError = typeof ProviderEventProcessingError.Type

/**
 * the type of a thing that processes events for a provider - it must declare a provider name like 'slack', a namespace (in case e.g. there are multiple slack bots),
 * and a function for processing a delivery to the mailbox for it
 *
 * this is equivalent to {@link WebhookProvider}
 */
export type ProviderEventProcessor<R = never> = {
	readonly namespace: string
	readonly providerName: string
	readonly process: (
		admission: DeliveryAdmission,
	) => Effect.Effect<ProviderEventResult, ProviderEventProcessorError, R>
}

/**
 * Given a list of provider event processors (e.g. for slack, github), and delivery for mailbox, create an event processor
 * that will process Deliveries into the mailbox (loading from the mailbox is a separate concern)
 * @param processors
 * @returns
 */
export const processProviderEvent = <R = never>(processors: ReadonlyArray<ProviderEventProcessor<R>>) =>
	Effect.fn('delivery.process_provider_event')(function* (admission: DeliveryAdmission) {
		const processor = processors.find(
			(candidate) => candidate.namespace === admission.namespace && candidate.providerName === admission.provider,
		)

		if (Predicate.isUndefined(processor)) {
			return yield* ProviderEventProcessorNotFound.make({
				namespace: admission.namespace,
				provider: admission.provider,
			})
		}

		return yield* processor.process(admission)
	})
