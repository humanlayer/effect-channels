import { Option } from 'effect'

import { SlackBotIdentity, type SlackTenantCreds } from './Schema.ts'

interface SlackBotIdentityFields {
	botUserId?: string
	botId?: string
}

export interface SlackBotIdentityInput {
	readonly botUserId?: string | undefined
	readonly botId?: string | undefined
}

export const slackBotIdentity = (input: SlackBotIdentityInput): SlackBotIdentity => {
	const fields: SlackBotIdentityFields = {}
	if (input.botUserId !== undefined) {
		fields.botUserId = input.botUserId
	}
	if (input.botId !== undefined) {
		fields.botId = input.botId
	}
	return SlackBotIdentity.make(fields)
}

export const mergeSlackBotIdentity = (
	fallback: SlackBotIdentity,
	creds: Option.Option<SlackTenantCreds>,
): SlackBotIdentity =>
	Option.match(creds, {
		onNone: () => fallback,
		onSome: (tenant) =>
			slackBotIdentity({
				botUserId: tenant.botUserId ?? fallback.botUserId,
				botId: tenant.botId ?? fallback.botId,
			}),
	})
