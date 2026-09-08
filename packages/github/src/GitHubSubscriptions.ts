import { Context, Effect, Layer, Schema } from 'effect'

import { GitHubDiscussionRef, issueResourceKey } from './GitHubResource.js'

export class GitHubSubscriptionError extends Schema.TaggedError<GitHubSubscriptionError>()('GitHubSubscriptionError', {
	reason: Schema.Literals(['invalid_input', 'capacity', 'storage']),
}) {}

export const GitHubSubscriptionInput = Schema.Struct({
	namespace: Schema.NonEmptyString,
	resource: GitHubDiscussionRef,
})
export interface GitHubSubscriptionInput extends Schema.Schema.Type<typeof GitHubSubscriptionInput> {}

export const GitHubSubscriptionRouteInput = Schema.Struct({
	...GitHubSubscriptionInput.fields,
	deliveryId: Schema.NonEmptyString,
	direct: Schema.Array(Schema.NonEmptyString),
	followed: Schema.Array(Schema.NonEmptyString),
})
export interface GitHubSubscriptionRouteInput extends Schema.Schema.Type<typeof GitHubSubscriptionRouteInput> {}

export const GitHubSubscriptionRoute = Schema.Struct({
	version: Schema.Literal(1),
	targets: Schema.Array(Schema.NonEmptyString),
})
export interface GitHubSubscriptionRoute extends Schema.Schema.Type<typeof GitHubSubscriptionRoute> {}

export const subscriptionKey = (input: GitHubSubscriptionInput) =>
	JSON.stringify([input.namespace, input.resource.repository.installationId, issueResourceKey(input.resource)])

export class GitHubSubscriptionStore extends Context.Service<
	GitHubSubscriptionStore,
	{
		readonly isSubscribed: (input: GitHubSubscriptionInput) => Effect.Effect<boolean, GitHubSubscriptionError>
		readonly subscribe: (input: GitHubSubscriptionInput) => Effect.Effect<void, GitHubSubscriptionError>
		readonly unsubscribe: (input: GitHubSubscriptionInput) => Effect.Effect<void, GitHubSubscriptionError>
		readonly resolveRoute: (
			input: GitHubSubscriptionRouteInput,
		) => Effect.Effect<GitHubSubscriptionRoute, GitHubSubscriptionError>
	}
>()('github/GitHubSubscriptionStore') {}

export class GitHubSubscriptions extends Context.Service<
	GitHubSubscriptions,
	{
		readonly isSubscribed: (input: GitHubSubscriptionInput) => Effect.Effect<boolean, GitHubSubscriptionError>
		readonly subscribe: (input: GitHubSubscriptionInput) => Effect.Effect<void, GitHubSubscriptionError>
		readonly unsubscribe: (input: GitHubSubscriptionInput) => Effect.Effect<void, GitHubSubscriptionError>
	}
>()('github/GitHubSubscriptions') {
	static readonly layer = Layer.effect(
		GitHubSubscriptions,
		Effect.gen(function* () {
			const store = yield* GitHubSubscriptionStore
			const parse = (input: GitHubSubscriptionInput) =>
				GitHubSubscriptionInput.makeEffect(input).pipe(
					Effect.mapError(() => GitHubSubscriptionError.make({ reason: 'invalid_input' })),
				)
			return GitHubSubscriptions.of({
				isSubscribed: Effect.fn('github.subscriptions.is_subscribed')((input) =>
					parse(input).pipe(Effect.flatMap(store.isSubscribed)),
				),
				subscribe: Effect.fn('github.subscriptions.subscribe')((input) =>
					parse(input).pipe(Effect.flatMap(store.subscribe)),
				),
				unsubscribe: Effect.fn('github.subscriptions.unsubscribe')((input) =>
					parse(input).pipe(Effect.flatMap(store.unsubscribe)),
				),
			})
		}),
	)
}
