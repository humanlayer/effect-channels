/**
 * Leaves a durable wake-up behind while an agent turn runs. With compatibility date 2026-10-01, pending
 * model, Computer, and timer I/O already keeps the object resident. The alarm exists only so a deploy or
 * crash wakes a fresh object, which can detect and resume an unfinished turn from the durable Fold log. A
 * healthy object re-arms the alarm because the next crash can happen after the current alarm fires.
 */
import * as Cloudflare from 'alchemy/Cloudflare'
import type { RuntimeContext } from 'alchemy/RuntimeContext'
import { Clock, Context, Effect, Layer, Ref } from 'effect'

const RECOVERY_WAKE_MILLIS = 30_000

export class RunRecovery extends Context.Service<
	RunRecovery,
	{
		/**
		 * Run an agent operation in the background with the object's `waitUntil`, keeping its crash-recovery
		 * alarm until it ends. Returns once the alarm is set, so a crash after that still wakes the object.
		 */
		readonly fork: <R>(effect: Effect.Effect<void, never, R>) => Effect.Effect<void, never, R | RuntimeContext>
		/**
		 * Re-arm when this object instance still has an active operation. Returns whether recovery owns the
		 * alarm; otherwise the caller may use the alarm for session expiry.
		 */
		readonly alarm: Effect.Effect<boolean>
	}
>()('alchemy-cloudflare/RunRecovery') {
	static readonly layer = Layer.effect(
		RunRecovery,
		Effect.gen(function* () {
			const state = yield* Cloudflare.DurableObjectState
			const active = yield* Ref.make(0)
			const scheduleRecoveryWake = Effect.flatMap(Clock.currentTimeMillis, (now) =>
				Effect.promise(() => state.raw.storage.setAlarm(now + RECOVERY_WAKE_MILLIS)),
			)

			return RunRecovery.of({
				fork: (effect) =>
					Ref.updateAndGet(active, (count) => count + 1).pipe(
						Effect.tap((count) => (count === 1 ? scheduleRecoveryWake : Effect.void)),
						Effect.andThen(
							state.waitUntil(effect.pipe(Effect.ensuring(Ref.update(active, (count) => count - 1)))),
						),
					),
				alarm: Effect.flatMap(Ref.get(active), (count) =>
					count > 0 ? Effect.as(scheduleRecoveryWake, true) : Effect.succeed(false),
				),
			})
		}),
	)
}
