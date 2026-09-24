import { Config, Redacted, Schema } from 'effect'

const StringConfig = Schema.declare<Config.Config<string>>((value): value is Config.Config<string> =>
	Config.isConfig(value),
)
const RedactedStringConfig = Schema.declare<Config.Config<Redacted.Redacted<string>>>(
	(value): value is Config.Config<Redacted.Redacted<string>> => Config.isConfig(value),
)

export const LinearClientCredentials = Schema.TaggedStruct('LinearClientCredentials', {
	clientId: StringConfig,
	clientSecret: RedactedStringConfig,
})
export type ClientCredentials = typeof LinearClientCredentials.Type

export const LinearDeveloperToken = Schema.TaggedStruct('LinearDeveloperToken', {
	token: RedactedStringConfig,
})
export type DeveloperToken = typeof LinearDeveloperToken.Type

export const LinearAuthentication = Schema.Union([LinearClientCredentials, LinearDeveloperToken])
export type Authentication = typeof LinearAuthentication.Type

const AuthenticationConfig = Schema.declare<Config.Config<Authentication>>(
	(value): value is Config.Config<Authentication> => Config.isConfig(value),
)

export const LinearAuthenticationInput = Schema.Union([LinearAuthentication, AuthenticationConfig])
export type AuthenticationInput = typeof LinearAuthenticationInput.Type

export const clientCredentials = (input: {
	readonly clientId: Config.Config<string>
	readonly clientSecret: Config.Config<Redacted.Redacted<string>>
}): ClientCredentials => LinearClientCredentials.make(input)

export const developerToken = (input: { readonly token: Config.Config<Redacted.Redacted<string>> }): DeveloperToken =>
	LinearDeveloperToken.make(input)

/** Reads a developer token when present, otherwise falls back to OAuth client credentials. */
export const fromEnvironment: Config.Config<Authentication> = Config.redacted('LINEAR_DEVELOPER_TOKEN').pipe(
	Config.map((token) => developerToken({ token: Config.succeed(token) })),
	Config.orElse(() =>
		Config.succeed(
			clientCredentials({
				clientId: Config.string('LINEAR_CLIENT_ID'),
				clientSecret: Config.redacted('LINEAR_CLIENT_SECRET'),
			}),
		),
	),
)

export const resolve = (input: AuthenticationInput): Config.Config<Authentication> =>
	Schema.is(LinearAuthentication)(input) ? Config.succeed(input) : input
