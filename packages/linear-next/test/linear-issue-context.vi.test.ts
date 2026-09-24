import { describe, it } from '@effect/vitest'
import { MailboxSubscriptionCreatedResult, MailboxSubscriptions } from '@humanlayer/channels-delivery-next'
import { Effect, Layer, Predicate } from 'effect'
import { vi } from 'vitest'

import { LinearApi } from '../src/LinearApi'
import type { LinearMentioned } from '../src/LinearCallbackEvents'
import { LinearCallbacks } from '../src/LinearCallbacks'
import { makeLinearEventProcessor } from '../src/LinearEventProcessor'
import { appUserNotificationPayloads, linearNotificationAdmission } from './fixtures'

describe('Linear issue context', () => {
	it.effect('builds a comment resource and delegates issue subscription methods', ({ expect }) =>
		Effect.gen(function* () {
			let subscribed = false
			const subscribe = vi.fn(() => {
				subscribed = true
				return Effect.succeed(MailboxSubscriptionCreatedResult.make({}))
			})
			const unsubscribe = vi.fn(() => {
				subscribed = false
				return Effect.void
			})
			const onMentioned = vi.fn((event: LinearMentioned) =>
				Effect.gen(function* () {
					expect(Predicate.isTagged('LinearCommentMentioned')(event)).toBe(true)
					if (!Predicate.isTagged('LinearCommentMentioned')(event)) return
					expect(event.trigger.comment.content.markdown).toContain('@test-app')
					expect(event.trigger.comment.ref.issueId).toBe(event.issue.ref.issueId)
					yield* event.issue.subscribe()
					expect(yield* event.issue.isSubscribed()).toBe(true)
					yield* event.issue.unsubscribe()
				}),
			)
			const subscriptions = Layer.mock(MailboxSubscriptions, {
				subscribe,
				isSubscribed: () => Effect.succeed(subscribed),
				unsubscribe,
			})
			const services = Layer.mergeAll(
				LinearCallbacks.layer({ onMentioned }).pipe(Layer.provide(subscriptions)),
				Layer.mock(LinearApi, {}),
				subscriptions,
			)
			yield* makeLinearEventProcessor({ namespace: 'linear-processing-test' })
				.process([linearNotificationAdmission(appUserNotificationPayloads[1])])
				.pipe(Effect.provide(services))
			expect(subscribe).toHaveBeenCalledOnce()
			expect(unsubscribe).toHaveBeenCalledOnce()
			expect(subscribed).toBe(false)
		}),
	)
})
