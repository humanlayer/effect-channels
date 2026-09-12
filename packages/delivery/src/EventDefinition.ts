import { Schema } from 'effect'

export type EventDefinition<
	Event extends Schema.Constraint,
	Resource extends Schema.Constraint,
	Provider extends string = string,
> = {
	readonly name: string
	readonly version: string
	readonly provider: Provider
	readonly event: Event
	readonly resource: Resource
	readonly resourceKey: (resource: Resource['Type']) => string
	readonly identify: (event: Event['Type']) => {
		readonly installation: string
		readonly eventId: string
		readonly resource: Resource['Type']
	}
}
