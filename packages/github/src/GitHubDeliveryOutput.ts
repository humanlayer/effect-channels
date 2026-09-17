import { DeliveryOutputError, DeliveryOutputReceipt, type FinalMessageOperation } from '@humanlayer/channels-delivery'
import { Effect, Schema } from 'effect'

import { GitHub } from './GitHub'
import { activityEventDefinition } from './GitHubActivity'
import { GitHubError } from './GitHubErrors'
import { GitHubCommentRef, GitHubDiscussionRef } from './GitHubResource'

const destinationCodec = Schema.fromJsonString(GitHubDiscussionRef)
const receiptCodec = Schema.fromJsonString(GitHubCommentRef)

/** Deliver one persisted final-message obligation through current GitHub App credentials. */
export const deliverGitHubFinalMessage = Effect.fn('github.delivery_output.final_message')(function* (
	operation: FinalMessageOperation,
) {
	if (
		operation.provider !== 'github' ||
		operation.presentation !== activityEventDefinition.name ||
		operation.presentationVersion !== activityEventDefinition.version
	)
		return yield* DeliveryOutputError.make({ retryable: false, safeCode: 'presentation_mismatch' })
	const destination = yield* Schema.decodeEffect(destinationCodec)(operation.destination).pipe(
		Effect.tapError(() =>
			Effect.logError('GitHub final-message destination decoding failed', {
				operation_id: operation.operationId,
				provider: operation.provider,
			}),
		),
		Effect.mapError(() => DeliveryOutputError.make({ retryable: false, safeCode: 'invalid_destination' })),
	)
	if (String(destination.repository.installationId) !== operation.installation)
		return yield* DeliveryOutputError.make({ retryable: false, safeCode: 'installation_mismatch' })
	const github = yield* GitHub
	const comment = yield* github.createComment({ issue: destination, body: operation.markdown }).pipe(
		Effect.tapError((error) =>
			Effect.logError('GitHub final-message provider call failed', {
				operation_id: operation.operationId,
				provider: operation.provider,
				reason: error.reason,
				retryable: error.reason === 'unavailable',
			}),
		),
		Effect.mapError((error: GitHubError) =>
			error.retryAfterMs === undefined
				? DeliveryOutputError.make({ retryable: error.reason === 'unavailable', safeCode: error.reason })
				: DeliveryOutputError.make({
						retryable: error.reason === 'unavailable',
						retryAfterMs: error.retryAfterMs,
						safeCode: error.reason,
					}),
		),
	)
	const providerReceipt = yield* Schema.encodeEffect(receiptCodec)(comment.ref).pipe(
		Effect.mapError(() => DeliveryOutputError.make({ retryable: false, safeCode: 'invalid_receipt' })),
	)
	return DeliveryOutputReceipt.make({ providerReceipt })
})
