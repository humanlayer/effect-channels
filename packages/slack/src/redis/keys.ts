import type { SubscriptionInput } from '../Operations'
import type { SlackConnectionLookupInput } from '../Schema'
import type { ResolveSlackDirectMessageRoute } from '../SlackSubscriptions'

const namespace = 'humanlayer:slack:v1:{state}'

const encode = (value: string) => {
	let result = ''
	for (let offset = 0; offset < value.length; offset++) {
		result += value.charCodeAt(offset).toString(16).padStart(4, '0')
	}
	return result
}

export const connectionKey = (input: SlackConnectionLookupInput) =>
	`${namespace}:connection:${encode(input.workspaceId)}`
export const subscriptionKey = (input: SubscriptionInput) => `${namespace}:subscription:${encode(input.threadId)}`
export const routeKey = (input: ResolveSlackDirectMessageRoute) =>
	`${namespace}:route:${[input.rootedThread.channel.tenant, input.rootedThread.channel.id, input.eventId].map(encode).join(':')}`
