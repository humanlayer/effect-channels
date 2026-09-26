import { Clock, Context, Effect, Layer } from 'effect'

import { DeliveryInterruption, interruptDelivery } from './DeliveryInterruption'
import { DeliveryQueue, enqueueDelivery } from './DeliveryQueue'
import { IngressAttributionStore, loadIngressAttribution, saveIngressAttribution } from './IngressAttribution'
import { DeliveryLocatorStore, MailboxReadiness, MailboxStore } from './MailboxStore'

/** Addressed ingress capabilities and the exact MailboxStore services backing them. */
export const layerMailboxStoreServices = Layer.effectContext(
	Effect.gen(function* () {
		const store = yield* MailboxStore
		const readiness = yield* MailboxReadiness
		const clock = yield* Clock.Clock
		const locator = yield* DeliveryLocatorStore
		const provideStore = <A, E>(effect: Effect.Effect<A, E, MailboxStore>) =>
			effect.pipe(Effect.provideService(MailboxStore, store), Effect.provideService(Clock.Clock, clock))
		return Context.make(MailboxStore, store).pipe(
			Context.add(MailboxReadiness, readiness),
			Context.add(DeliveryLocatorStore, locator),
			Context.add(DeliveryQueue, DeliveryQueue.of({ enqueue: (input) => provideStore(enqueueDelivery(input)) })),
			Context.add(
				DeliveryInterruption,
				DeliveryInterruption.of({ interrupt: (input) => provideStore(interruptDelivery(input)) }),
			),
			Context.add(
				IngressAttributionStore,
				IngressAttributionStore.of({
					load: (input) => provideStore(loadIngressAttribution(input)),
					save: (input) => provideStore(saveIngressAttribution(input)),
				}),
			),
		)
	}),
)
