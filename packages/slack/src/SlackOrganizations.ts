import { Cause, Context, Effect, Layer, Logger, Schema } from 'effect'

import { SlackTeamId } from './SlackIdentity'

export const SlackOrganizationLookup = Schema.Struct({ workspaceId: SlackTeamId })
export interface SlackOrganizationLookup extends Schema.Schema.Type<typeof SlackOrganizationLookup> {}

export const SlackOrganization = Schema.Struct({ organizationId: Schema.NonEmptyString })
export interface SlackOrganization extends Schema.Schema.Type<typeof SlackOrganization> {}

export class SlackOrganizationLookupError extends Schema.TaggedError<SlackOrganizationLookupError>()(
	'SlackOrganizationLookupError',
	{ reason: Schema.optionalKey(Schema.Literals(['unavailable', 'unexpected'])) },
) {}

export class SlackOrganizations extends Context.Service<
	SlackOrganizations,
	{
		readonly resolve: (
			input: SlackOrganizationLookup,
		) => Effect.Effect<SlackOrganization | null, SlackOrganizationLookupError>
		readonly legacyOrganizationId?: string
	}
>()('slack/Organizations') {
	/** Capture callback dependencies once; decode results and sanitize failures at the lookup boundary. */
	static readonly layer = <E, R>(
		lookup: (input: SlackOrganizationLookup) => Effect.Effect<SlackOrganization | null, E, R>,
	) =>
		Layer.effect(
			SlackOrganizations,
			Effect.gen(function* () {
				const context = yield* Effect.context<R>()
				return SlackOrganizations.of({
					resolve: Effect.fn('slack.organizations.resolve')((input) =>
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
									return Effect.logError('Slack organization lookup failed').pipe(
										Effect.annotateLogs({
											classification: unexpected ? 'unexpected_defect' : 'lookup_failed',
										}),
										Effect.andThen(
											Effect.fail(
												SlackOrganizationLookupError.make(
													unexpected ? { reason: 'unexpected' } : {},
												),
											),
										),
									)
								}),
								Effect.flatMap((result) =>
									Schema.decodeUnknownEffect(Schema.NullOr(SlackOrganization))(result).pipe(
										Effect.tapError(() =>
											Effect.logError('Slack organization lookup failed').pipe(
												Effect.annotateLogs({ classification: 'invalid_result' }),
											),
										),
										Effect.mapError(() => SlackOrganizationLookupError.make({})),
									),
								),
							),
						),
					),
				})
			}),
		)

	static readonly fixed = (input: SlackOrganization) =>
		Layer.effect(
			SlackOrganizations,
			SlackOrganization.makeEffect(input).pipe(
				Effect.map((organization) =>
					SlackOrganizations.of({
						legacyOrganizationId: organization.organizationId,
						resolve: Effect.fn('slack.organizations.fixed')(() => Effect.succeed(organization)),
					}),
				),
			),
		)
}
