import { loadIngressAttribution, saveIngressAttribution } from '@humanlayer/channels-delivery'
import { Effect, Schema } from 'effect'

import { SlackIngressError } from './DomainErrors.js'
import { SlackOrganization, SlackOrganizationLookup, SlackOrganizations } from './SlackOrganizations.js'

const SlackAttributionInput = Schema.Struct({
	namespace: Schema.NonEmptyString,
	eventId: Schema.NonEmptyString,
	...SlackOrganizationLookup.fields,
})
interface SlackAttributionInput extends Schema.Codec.Encoded<typeof SlackAttributionInput> {}

const reject = (input: {
	readonly classification: 'invalid_identity' | 'invalid_result' | 'lookup_failed' | 'storage_failed'
	readonly errorTag?: 'SlackOrganizationLookupError'
}) =>
	Effect.logError('Slack organization attribution failed', input).pipe(
		Effect.andThen(Effect.fail(SlackIngressError.make({ operation: 'organization_lookup' }))),
	)

export const resolveSlackIngressAttribution = Effect.fn('slack.ingress.organization')(function* (
	input: SlackAttributionInput,
) {
	const parsed = yield* Schema.decodeEffect(SlackAttributionInput)(input).pipe(
		Effect.catchTag('SchemaError', () => reject({ classification: 'invalid_identity' })),
	)
	const identity = {
		namespace: parsed.namespace,
		provider: 'slack',
		installation: parsed.workspaceId,
		eventId: parsed.eventId,
	}
	return yield* Effect.gen(function* () {
		const saved = yield* loadIngressAttribution(identity)
		if (saved !== undefined) return saved
		const organizations = yield* SlackOrganizations
		const resolved = yield* organizations
			.resolve({ workspaceId: parsed.workspaceId })
			.pipe(
				Effect.catchTag('SlackOrganizationLookupError', (error) =>
					error.reason === 'unexpected'
						? Effect.fail(
								SlackIngressError.make({ operation: 'organization_lookup', reason: 'unexpected' }),
							)
						: reject({ classification: 'lookup_failed', errorTag: 'SlackOrganizationLookupError' }),
				),
			)
		const organization = yield* Schema.decodeUnknownEffect(Schema.NullOr(SlackOrganization))(resolved).pipe(
			Effect.catchTag('SchemaError', () => reject({ classification: 'invalid_result' })),
		)
		if (organization === null) return null
		return yield* saveIngressAttribution({ ...identity, ...organization })
	}).pipe(Effect.catchTag('MailboxStoreError', () => reject({ classification: 'storage_failed' })))
})
