import { loadIngressAttribution, saveIngressAttribution } from '@humanlayer/channels-delivery'
import { Effect, Schema } from 'effect'

import { GitHubIngressError } from './GitHubErrors.js'
import { GitHubOrganization, GitHubOrganizationLookup, GitHubOrganizations } from './GitHubOrganizations.js'

const GitHubAttributionInput = Schema.Struct({
	namespace: Schema.NonEmptyString,
	eventId: Schema.NonEmptyString,
	...GitHubOrganizationLookup.fields,
})
interface GitHubAttributionInput extends Schema.Schema.Type<typeof GitHubAttributionInput> {}

const reject = (input: {
	readonly classification: 'invalid_identity' | 'invalid_result' | 'lookup_failed' | 'storage_failed'
	readonly errorTag?: 'GitHubOrganizationLookupError'
}) =>
	Effect.logError('GitHub organization attribution failed', input).pipe(
		Effect.andThen(Effect.fail(GitHubIngressError.make({ operation: 'admit' }))),
	)

export const resolveGitHubIngressAttribution = Effect.fn('github.ingress.organization')(function* (
	input: GitHubAttributionInput,
) {
	const parsed = yield* Schema.decodeEffect(GitHubAttributionInput)(input).pipe(
		Effect.catchTag('SchemaError', () => reject({ classification: 'invalid_identity' })),
	)
	const identity = {
		namespace: parsed.namespace,
		provider: 'github',
		installation: String(parsed.installationId),
		eventId: parsed.eventId,
	}
	return yield* Effect.gen(function* () {
		const saved = yield* loadIngressAttribution(identity)
		if (saved !== undefined) return saved
		const organizations = yield* GitHubOrganizations
		const resolved = yield* organizations
			.resolve({ installationId: parsed.installationId })
			.pipe(
				Effect.catchTag('GitHubOrganizationLookupError', (error) =>
					error.reason === 'unexpected'
						? Effect.fail(GitHubIngressError.make({ operation: 'admit', reason: 'unexpected' }))
						: reject({ classification: 'lookup_failed', errorTag: 'GitHubOrganizationLookupError' }),
				),
			)
		const organization = yield* Schema.decodeUnknownEffect(Schema.NullOr(GitHubOrganization))(resolved).pipe(
			Effect.catchTag('SchemaError', () => reject({ classification: 'invalid_result' })),
		)
		if (organization === null) return null
		return yield* saveIngressAttribution({ ...identity, ...organization })
	}).pipe(Effect.catchTag('MailboxStoreError', () => reject({ classification: 'storage_failed' })))
})
