import { it } from '@effect/vitest'
import { Context, Effect, Layer, Schema } from 'effect'
import { expectTypeOf } from 'vite-plus/test'

import { GitHubBot, GitHubError, GitHubSubscriptionStore } from '../src/index'
import { policy } from './fixtures'

class First extends Context.Service<First, { readonly value: string }>()('test/First') {}
class Second extends Context.Service<Second, { readonly value: number }>()('test/Second') {}
class SecondError extends Schema.TaggedError<SecondError>()('SecondError', {}) {}

it('infers all heterogeneous handler requirements without caller unions or widening to unknown', () => {
	const bot = GitHubBot.make({
		namespace: 'inference',
		policy,
		handlers: [
			{
				id: 'first',
				onMention: () => Effect.andThen(First, Effect.fail(GitHubError.make({ reason: 'unavailable' }))),
			},
			{ id: 'second', onMention: () => Effect.andThen(Second, Effect.fail(SecondError.make({}))) },
		],
	})
	expectTypeOf<Extract<Layer.Services<typeof bot>, First | Second>>().toEqualTypeOf<First | Second>()
})

it('infers heterogeneous optional activity callbacks and exposes explicit storage requirements', () => {
	const bot = GitHubBot.make({
		namespace: 'activity-inference',
		policy,
		handlers: [
			{
				id: 'first',
				onCreation: (event) => {
					expectTypeOf(event.action).toEqualTypeOf<'opened'>()
					return Effect.asVoid(First)
				},
			},
			{
				id: 'second',
				onMention: (event) => {
					expectTypeOf(event.action).toEqualTypeOf<'opened' | 'created' | 'edited' | 'submitted'>()
					return Effect.andThen(Second, Effect.fail(SecondError.make({})))
				},
			},
		],
	})
	expectTypeOf<Extract<Layer.Services<typeof bot>, First | Second | GitHubSubscriptionStore>>().toEqualTypeOf<
		First | Second | GitHubSubscriptionStore
	>()
})
