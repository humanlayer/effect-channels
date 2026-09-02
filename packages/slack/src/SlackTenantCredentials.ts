import { unimplemented } from '@humanlayer/channels'
import { Context, Effect, Layer, Option } from 'effect'
import { Config } from 'effect'

import type { CredentialStoreError } from './Errors.ts'
import type { SlackLoadCredentialsInput, SlackSaveCredentialsInput, SlackTenantCreds } from './Schema.ts'
import { SlackTenantCreds as SlackTenantCredsSchema } from './Schema.ts'

export class SlackTenantCredentials extends Context.Service<
	SlackTenantCredentials,
	{
		readonly load: (
			input: SlackLoadCredentialsInput,
		) => Effect.Effect<Option.Option<SlackTenantCreds>, CredentialStoreError>
		readonly save: (input: SlackSaveCredentialsInput) => Effect.Effect<void, CredentialStoreError>
	}
>()('channels/SlackTenantCredentials') {
	static readonly layerFromConfig = Layer.effect(
		SlackTenantCredentials,
		Effect.map(Config.redacted('SLACK_BOT_TOKEN'), (botToken) => {
			const credentials = SlackTenantCredsSchema.make({ botToken })
			return SlackTenantCredentials.of({
				load: () => Effect.succeed(Option.some(credentials)),
				save: () => unimplemented('SlackTenantCredentials.save'),
			})
		}),
	)

	static make(operations: {
		readonly load: (
			input: SlackLoadCredentialsInput,
		) => Effect.Effect<Option.Option<SlackTenantCreds>, CredentialStoreError>
		readonly save: (input: SlackSaveCredentialsInput) => Effect.Effect<void, CredentialStoreError>
	}) {
		return Layer.succeed(SlackTenantCredentials, SlackTenantCredentials.of(operations))
	}
}
