/**
 * Leaves a durable wake-up behind while an agent turn runs. With compatibility date 2026-10-01, pending
 * model, Computer, and timer I/O already keeps the object resident; this service does not keep it alive.
 * Its alarm exists only so a deploy or crash wakes a fresh object, which can detect and resume an unfinished
 * turn from the durable Fold log. A healthy object re-arms the alarm because the next crash can happen after
 * the current alarm fires.
 */
import * as Cloudflare from 'alchemy/Cloudflare'
import { Clock, Context, Effect, Layer, Ref, Scope } from 'effect'

const RECOVERY_WAKE_MILLIS = 30_000

export class RunRecovery extends Context.Service<
	RunRecovery,
	{
		/**
		 * Run an agent operation in the background, keeping its crash-recovery alarm until it ends. Returns once
		 * the alarm is set, so a crash after that still wakes the object. The operation lives as long as this
		 * layer, which the AgentSession builds for the object's lifetime; pending I/O keeps the object resident
		 * meanwhile, so it needs no `waitUntil`.
		 */
		readonly fork: (effect: Effect.Effect<void>) => Effect.Effect<void>
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
			const { raw } = yield* Cloudflare.DurableObjectState
			const scope = yield* Scope.Scope
			const active = yield* Ref.make(0)
			const scheduleRecoveryWake = Effect.flatMap(Clock.currentTimeMillis, (now) =>
				Effect.promise(() => raw.storage.setAlarm(now + RECOVERY_WAKE_MILLIS)),
			)

			return RunRecovery.of({
				fork: (effect) =>
					Ref.updateAndGet(active, (count) => count + 1).pipe(
						Effect.tap((count) => (count === 1 ? scheduleRecoveryWake : Effect.void)),
						Effect.andThen(
							effect.pipe(
								Effect.ensuring(Ref.update(active, (count) => count - 1)),
								Effect.forkIn(scope),
							),
						),
						Effect.asVoid,
					),
				alarm: Effect.flatMap(Ref.get(active), (count) =>
					count > 0 ? Effect.as(scheduleRecoveryWake, true) : Effect.succeed(false),
				),
			})
		}),
	)
}
