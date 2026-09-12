import { assert, it } from '@effect/vitest'
import { Cause, Context, Deferred, Effect, Exit, Fiber, Layer, Logger, Ref } from 'effect'

import { SlackOrganizations, SlackOrganizationLookupError, SlackTeamId } from '../src/index.js'

class LookupState extends Context.Service<LookupState, Ref.Ref<string | null>>()('test/slack/LookupState') {}

it.effect('factory captures dependencies once but reads live state and owns the service span', () =>
	Effect.gen(function* () {
		const state = yield* Ref.make<string | null>('first')
		const acquisitions = yield* Ref.make(0)
		const spans: string[] = []
		const layer = SlackOrganizations.layer(() =>
			Effect.gen(function* () {
				spans.push((yield* Effect.currentSpan).name)
				const value = yield* Ref.get(yield* LookupState)
				return value === null ? null : { organizationId: value }
			}),
		).pipe(Layer.provide(Layer.effect(LookupState, Ref.update(acquisitions, (n) => n + 1).pipe(Effect.as(state)))))
		const service = Context.get(yield* Layer.build(layer), SlackOrganizations)
		assert.deepStrictEqual(yield* service.resolve({ workspaceId: SlackTeamId.make('T_TEST') }), {
			organizationId: 'first',
		})
		yield* Ref.set(state, 'second')
		assert.deepStrictEqual(yield* service.resolve({ workspaceId: SlackTeamId.make('T_TEST') }), {
			organizationId: 'second',
		})
		yield* Ref.set(state, null)
		assert.strictEqual(yield* service.resolve({ workspaceId: SlackTeamId.make('T_TEST') }), null)
		assert.strictEqual(yield* Ref.get(acquisitions), 1)
		assert.deepStrictEqual(spans, Array(3).fill('slack.organizations.resolve'))
		assert.strictEqual(service.legacyOrganizationId, undefined)
	}),
)

it.effect('factory catches suspended throws and composite defects but never recovers cancellation', () =>
	Effect.gen(function* () {
		const logs: string[] = []
		const logger = Logger.layer([Logger.make((entry) => logs.push(JSON.stringify(entry.message)))])
		const callbacks = [
			() => Effect.die('private-defect-sentinel'),
			() => {
				throw new Error('private-throw-sentinel')
			},
			() => Effect.failCause(Cause.combine(Cause.fail('private-failure'), Cause.die('private-defect'))),
		]
		for (const callback of callbacks) {
			yield* Effect.gen(function* () {
				const service = yield* SlackOrganizations
				assert.deepStrictEqual(
					yield* service.resolve({ workspaceId: SlackTeamId.make('T_TEST') }).pipe(Effect.flip),
					SlackOrganizationLookupError.make({ reason: 'unexpected' }),
				)
			}).pipe(Effect.provide(Layer.merge(SlackOrganizations.layer(callback), logger)))
		}
		assert.strictEqual(logs.length, 3)
		assert.ok(logs.every((log) => log.includes('unexpected_defect') && !log.includes('private-')))
		const entered = yield* Deferred.make<void>()
		const service = Context.get(
			yield* Layer.build(
				SlackOrganizations.layer(() =>
					Deferred.succeed(entered, undefined).pipe(Effect.andThen(Effect.never)),
				).pipe(Layer.provide(logger)),
			),
			SlackOrganizations,
		)
		const fiber = yield* service
			.resolve({ workspaceId: SlackTeamId.make('T_TEST') })
			.pipe(Effect.provide(logger), Effect.forkChild)
		yield* Deferred.await(entered)
		yield* Fiber.interrupt(fiber)
		const exit = yield* Fiber.await(fiber)
		assert.ok(Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause))
		const mixed = yield* Effect.flatMap(SlackOrganizations, (organizations) =>
			organizations.resolve({ workspaceId: SlackTeamId.make('T_TEST') }),
		).pipe(
			Effect.provide(
				Layer.merge(
					SlackOrganizations.layer(() =>
						Effect.failCause(
							Cause.combine(
								Cause.interrupt(42),
								Cause.combine(Cause.fail('private-failure'), Cause.die('private-defect')),
							),
						),
					),
					logger,
				),
			),
			Effect.exit,
		)
		assert.ok(Exit.isFailure(mixed) && Cause.hasInterruptsOnly(mixed.cause))
		assert.strictEqual(logs.length, 3)
	}),
)

it.effect('factory maps application failures and invalid results without logging private data', () =>
	Effect.gen(function* () {
		const logs: string[] = []
		const logger = Logger.layer([Logger.make((entry) => logs.push(JSON.stringify(entry.message)))])
		const callbacks = [
			() => Effect.fail({ secret: 'private-error-sentinel' }),
			() => Effect.succeed({ organizationId: '', secret: 'private-result-sentinel' }),
		]
		for (const callback of callbacks) {
			yield* Effect.gen(function* () {
				const service = yield* SlackOrganizations
				assert.deepStrictEqual(
					yield* service.resolve({ workspaceId: SlackTeamId.make('T_TEST') }).pipe(Effect.flip),
					SlackOrganizationLookupError.make({}),
				)
			}).pipe(Effect.provide(Layer.merge(SlackOrganizations.layer(callback), logger)))
		}
		assert.ok(logs.some((log) => log.includes('lookup_failed')))
		assert.ok(logs.some((log) => log.includes('invalid_result')))
		assert.ok(logs.every((log) => !log.includes('private-')))
	}),
)
