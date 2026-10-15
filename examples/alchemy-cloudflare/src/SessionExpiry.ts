/**
 * Deletes an idle session. Every message pushes the session's deadline to {@link IDLE_TIME} from now. Once no turn is running, the object's alarm is set for the deadline; when it fires past
 * the deadline, the session's workspace is deleted, then the object's own storage, and the object restarts
 * empty, so its id is new again.
 *
 * The alarm is shared with RunRecovery. While a turn runs, recovery uses it as a durable post-crash wake-up;
 * once no turn is running, expiry owns it.
 */
import * as Cloudflare from 'alchemy/Cloudflare'
import { Clock, Context, Duration, Effect, Layer, Option, Schema } from 'effect'

/** How long a session lasts after its last message. */
export const IDLE_TIME = Duration.days(14)

const DEADLINE_KEY = 'session_deadline'

/** How long after deleting the session the object restarts. */
const RESTART_DELAY_MILLIS = 1_000

export class SessionExpiry extends Context.Service<
	SessionExpiry,
	{
		/** Push the deadline back to the idle time from now. Returns the new deadline, in epoch milliseconds. */
		readonly touch: Effect.Effect<number>
		/** Whether the deadline has passed: the session is waiting for its alarm to delete it. */
		readonly expired: Effect.Effect<boolean>
		/** Give a session with a deadline an alarm if it has none. */
		readonly ensureScheduled: Effect.Effect<void>
		/**
		 * The object's alarm when no turn holds it: past the deadline, run `deleteWorkspace` and delete the
		 * session; before it, set the alarm for the deadline. A session with no deadline was already deleted
		 * (or never written), so there is nothing to do.
		 */
		readonly alarm: (deleteWorkspace: Effect.Effect<void>) => Effect.Effect<void>
	}
>()('alchemy-cloudflare/SessionExpiry', {
	/** Over the Durable Object's storage, with {@link IDLE_TIME}. */
	make: Effect.gen(function* () {
		const { raw } = yield* Cloudflare.DurableObjectState
		const readDeadline = Effect.promise(() => raw.storage.get(DEADLINE_KEY)).pipe(
			Effect.map((value) => Option.getOrUndefined(Schema.decodeUnknownOption(Schema.Finite)(value))),
		)
		const setAlarm = (at: number) => Effect.promise(() => raw.storage.setAlarm(at))
		/** Throw away the object's in-memory state, so the next call starts from its (empty) storage. */
		const restart = Effect.sleep(Duration.millis(RESTART_DELAY_MILLIS)).pipe(
			Effect.andThen(Effect.try(() => raw.abort('session expired'))),
			Effect.ignore,
			Effect.forkDetach(),
			Effect.asVoid,
		)

		return {
			touch: Effect.gen(function* () {
				const deadline = (yield* Clock.currentTimeMillis) + Duration.toMillis(IDLE_TIME)
				yield* Effect.promise(() => raw.storage.put(DEADLINE_KEY, deadline))
				return deadline
			}),
			expired: Effect.gen(function* () {
				const deadline = yield* readDeadline
				return deadline !== undefined && (yield* Clock.currentTimeMillis) >= deadline
			}),
			ensureScheduled: Effect.gen(function* () {
				if ((yield* Effect.promise(() => raw.storage.getAlarm())) !== null) return
				const deadline = yield* readDeadline
				if (deadline !== undefined) yield* setAlarm(deadline)
			}),
			alarm: (deleteWorkspace) =>
				Effect.gen(function* () {
					const deadline = yield* readDeadline
					if (deadline === undefined) return
					if ((yield* Clock.currentTimeMillis) < deadline) return yield* setAlarm(deadline)

					yield* deleteWorkspace
					yield* Effect.promise(async () => {
						await raw.storage.deleteAlarm()
						await raw.storage.deleteAll()
					})
					yield* Effect.logInfo('session.expired')
					yield* restart
				}),
		}
	}),
}) {
	static readonly layer = Layer.effect(this, this.make)
}
