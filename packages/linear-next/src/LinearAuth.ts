import { Config, Redacted, Schema } from 'effect'

const StringConfig = Schema.declare<Config.Config<string>>(
	(value): value is Config.Config<string> => Config.isConfig(value),
)
const RedactedStringConfig = Schema.declare<Config.Config<Redacted.Redacted<string>>>(
	(value): value is Config.Config<Redacted.Redacted<string>> => Config.isConfig(value),
)

export const LinearClientCredentials = Schema.TaggedStruct('LinearClientCredentials', {
	clientId: StringConfig,
	clientSecret: RedactedStringConfig,
})
export type ClientCredentials = typeof LinearClientCredentials.Type

export const clientCredentials = (input: {
	readonly clientId: Config.Config<string>
	readonly clientSecret: Config.Config<Redacted.Redacted<string>>
}): ClientCredentials => LinearClientCredentials.make(input)
