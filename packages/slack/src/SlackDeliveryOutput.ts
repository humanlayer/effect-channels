import { DeliveryOutputError, DeliveryOutputReceipt, type FinalMessageOperation } from '@humanlayer/channels-delivery'
import { Effect, Schema } from 'effect'

import { MarkdownContent } from './Content'
import { PostFailed, RetryabilityMetadata, UnknownTenant } from './DomainErrors'
import { ThreadId } from './Model'
import { SentRef } from './SentMessage'
import { Slack } from './Slack'
import { decodeSlackThreadId } from './SlackThreadId'

const destinationCodec = Schema.fromJsonString(Schema.Struct({ threadId: ThreadId }))
const receiptCodec = Schema.fromJsonString(SentRef)
const presentations = new Set([
	'slack.message',
	'slack.message_updated',
	'slack.message_deleted',
	'slack.reaction',
	'slack.conversation_stopped',
])

/** Deliver one persisted final-message obligation through the current Slack connection. */
export const deliverSlackFinalMessage = Effect.fn('slack.delivery_output.final_message')(function* (
	operation: FinalMessageOperation,
) {
	if (
		operation.provider !== 'slack' ||
		operation.presentationVersion !== '1' ||
		!presentations.has(operation.presentation)
	)
		return yield* DeliveryOutputError.make({ retryable: false, safeCode: 'presentation_mismatch' })
	const destination = yield* Schema.decodeEffect(destinationCodec)(operation.destination).pipe(
		Effect.tapError(() =>
			Effect.logError('Slack final-message destination decoding failed', {
				operation_id: operation.operationId,
				provider: operation.provider,
			}),
		),
		Effect.mapError(() => DeliveryOutputError.make({ retryable: false, safeCode: 'invalid_destination' })),
	)
	const thread = yield* decodeSlackThreadId(destination.threadId).pipe(
		Effect.tapError(() =>
			Effect.logError('Slack final-message thread decoding failed', {
				operation_id: operation.operationId,
				provider: operation.provider,
			}),
		),
		Effect.mapError(() => DeliveryOutputError.make({ retryable: false, safeCode: 'invalid_destination' })),
	)
	if (String(thread.teamId) !== operation.installation)
		return yield* DeliveryOutputError.make({ retryable: false, safeCode: 'installation_mismatch' })
	const slack = yield* Slack
	const sent = yield* slack
		.post({ threadId: destination.threadId, content: MarkdownContent.make({ markdown: operation.markdown }) })
		.pipe(
			Effect.tapError((error) =>
				Effect.logError('Slack final-message provider call failed', {
					operation_id: operation.operationId,
					provider: operation.provider,
					retryable:
						!Schema.is(UnknownTenant)(error) &&
						(!Schema.is(RetryabilityMetadata)(error) || error.retryability === 'retryable'),
				}),
			),
			Effect.mapError((error) => {
				const fields = {
					retryable:
						!Schema.is(UnknownTenant)(error) &&
						(!Schema.is(RetryabilityMetadata)(error) || error.retryability === 'retryable'),
					safeCode: Schema.is(UnknownTenant)(error) ? 'unknown_installation' : 'post_failed',
				}
				return Schema.is(PostFailed)(error) && error.retryAfterMs !== undefined
					? DeliveryOutputError.make({ ...fields, retryAfterMs: error.retryAfterMs })
					: DeliveryOutputError.make(fields)
			}),
		)
	const providerReceipt = yield* Schema.encodeEffect(receiptCodec)(sent.ref).pipe(
		Effect.mapError(() => DeliveryOutputError.make({ retryable: false, safeCode: 'invalid_receipt' })),
	)
	return DeliveryOutputReceipt.make({ providerReceipt })
})
