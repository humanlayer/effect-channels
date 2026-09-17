import { Cause, Context, Effect, Layer, Logger, Schema } from 'effect'

import { GitHubId } from './GitHubResource'

export const GitHubOrganizationLookup = Schema.Struct({ installationId: GitHubId })
export interface GitHubOrganizationLookup extends Schema.Schema.Type<typeof GitHubOrganizationLookup> {}

export const GitHubOrganization = Schema.Struct({ organizationId: Schema.NonEmptyString })
export interface GitHubOrganization extends Schema.Schema.Type<typeof GitHubOrganization> {}

export class GitHubOrganizationLookupError extends Schema.TaggedError<GitHubOrganizationLookupError>()(
	'GitHubOrganizationLookupError',
	{ reason: Schema.optionalKey(Schema.Literals(['unavailable', 'unexpected'])) },
) {}

export class GitHubOrganizations extends Context.Service<
	GitHubOrganizations,
	{
		readonly resolve: (
			input: GitHubOrganizationLookup,
		) => Effect.Effect<GitHubOrganization | null, GitHubOrganizationLookupError>
		readonly legacyOrganizationId?: string
	}
>()('github/Organizations') {
	/** Capture callback dependencies once; decode results and sanitize failures at the lookup boundary. */
	static readonly layer = <E, R>(
		lookup: (input: GitHubOrganizationLookup) => Effect.Effect<GitHubOrganization | null, E, R>,
	) =>
		Layer.effect(
			GitHubOrganizations,
			Effect.gen(function* () {
				const context = yield* Effect.context<R>()
				return GitHubOrganizations.of({
					resolve: Effect.fn('github.organizations.resolve')((input) =>
						Effect.flatMap(Logger.CurrentLoggers, (loggers) =>
							Effect.suspend(() => lookup(input)).pipe(
								Effect.provide(context),
								Effect.provideService(Logger.CurrentLoggers, loggers),
								Effect.catchCause((cause) => {
									if (Cause.hasInterrupts(cause))
										return Effect.failCause(
											Cause.fromReasons<never>(cause.reasons.filter(Cause.isInterruptReason)),
										)
									const unexpected = Cause.hasDies(cause)
									return Effect.logError('GitHub organization lookup failed').pipe(
										Effect.annotateLogs({
											classification: unexpected ? 'unexpected_defect' : 'lookup_failed',
										}),
										Effect.andThen(
											Effect.fail(
												GitHubOrganizationLookupError.make(
													unexpected ? { reason: 'unexpected' } : {},
												),
											),
										),
									)
								}),
								Effect.flatMap((result) =>
									Schema.decodeUnknownEffect(Schema.NullOr(GitHubOrganization))(result).pipe(
										Effect.tapError(() =>
											Effect.logError('GitHub organization lookup failed').pipe(
												Effect.annotateLogs({ classification: 'invalid_result' }),
											),
										),
										Effect.mapError(() => GitHubOrganizationLookupError.make({})),
									),
								),
							),
						),
					),
				})
			}),
		)

	static readonly fixed = (input: GitHubOrganization) =>
		Layer.effect(
			GitHubOrganizations,
			GitHubOrganization.makeEffect(input).pipe(
				Effect.map((organization) =>
					GitHubOrganizations.of({
						legacyOrganizationId: organization.organizationId,
						resolve: Effect.fn('github.organizations.fixed')(() => Effect.succeed(organization)),
					}),
				),
			),
		)
}
