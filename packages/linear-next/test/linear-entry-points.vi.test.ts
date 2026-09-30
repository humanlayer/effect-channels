import { describe, it } from '@effect/vitest'
import { MailboxSubscriptions, ProviderEventHandled, ProviderEventIgnored } from '@humanlayer/channels-delivery-next'
import { Effect, Layer, Schema } from 'effect'
import { vi } from 'vite-plus/test'

import { LinearApi } from '../src/LinearApi'
import { LinearIssueOpened } from '../src/LinearCallbackEvents'
import { type LinearCallbackHandler, LinearCallbacks, type LinearCallbackHandlers } from '../src/LinearCallbacks'
import { makeLinearEventProcessor } from '../src/LinearEventProcessor'
import {
	appUserNotificationPayloads,
	firstAttempt,
	linearIssueCreateAdmission,
	linearNotificationAdmission,
} from './fixtures'

const namespace = 'linear-processing-test'
const layer = (handlers: LinearCallbackHandlers<never, never>, subscribed = false) =>
	Layer.mergeAll(
		LinearCallbacks.layer(handlers),
		Layer.mock(LinearApi, {}),
		Layer.mock(MailboxSubscriptions, {
			isSubscribed: () => Effect.succeed(subscribed),
			subscribe: () => Effect.die('not used'),
			unsubscribe: () => Effect.die('not used'),
		}),
	)

const process = (
	handlers: LinearCallbackHandlers<never, never>,
	admissions: Parameters<ReturnType<typeof makeLinearEventProcessor>['process']>[0],
	subscribed = false,
) =>
	firstAttempt().pipe(
		Effect.flatMap((execution) => makeLinearEventProcessor({ namespace }).process(admissions, execution)),
		Effect.provide(layer(handlers, subscribed)),
	)

describe('Linear directed entry points', () => {
	it.effect('routes issue mentions, comment mentions, and assignments', ({ expect }) =>
		Effect.gen(function* () {
			const onMentioned = vi.fn<LinearCallbackHandler<'onMentioned'>>(() => Effect.void)
			const onAssigned = vi.fn<LinearCallbackHandler<'onAssigned'>>(() => Effect.void)
			for (const payload of appUserNotificationPayloads.slice(0, 3)) {
				const result = yield* process({ onMentioned, onAssigned }, [linearNotificationAdmission(payload)])
				expect(result).toEqual(ProviderEventHandled.make({}))
			}
			expect(onMentioned).toHaveBeenCalledTimes(2)
			expect(onMentioned.mock.calls[0]?.[0]._tag).toBe('LinearIssueMentioned')
			expect(onMentioned.mock.calls[1]?.[0]._tag).toBe('LinearCommentMentioned')
			expect(onAssigned).toHaveBeenCalledOnce()
		}),
	)

	it.effect('acknowledges the five duplicate notification actions without a callback', ({ expect }) =>
		Effect.gen(function* () {
			const onMentioned = vi.fn<LinearCallbackHandler<'onMentioned'>>(() => Effect.void)
			const onAssigned = vi.fn<LinearCallbackHandler<'onAssigned'>>(() => Effect.void)
			for (const payload of appUserNotificationPayloads.slice(3)) {
				const result = yield* process({ onMentioned, onAssigned }, [linearNotificationAdmission(payload)])
				expect(result).toEqual(ProviderEventIgnored.make({ reason: 'not_subscribed' }))
			}
			expect(onMentioned).not.toHaveBeenCalled()
			expect(onAssigned).not.toHaveBeenCalled()
		}),
	)

	it.effect('lets a directed callback win while subscribed and preserves the rest of the batch', ({ expect }) =>
		Effect.gen(function* () {
			const onMentioned = vi.fn<LinearCallbackHandler<'onMentioned'>>(() => Effect.void)
			const onIssueCreated = vi.fn<LinearCallbackHandler<'onIssueCreated'>>(() => Effect.void)
			const mention = linearNotificationAdmission(appUserNotificationPayloads[0])
			const result = yield* process(
				{ onMentioned, onIssueCreated },
				[linearIssueCreateAdmission(namespace), mention],
				true,
			)
			expect(result).toEqual(ProviderEventHandled.make({}))
			expect(onMentioned).toHaveBeenCalledOnce()
			expect(onMentioned.mock.calls[0]?.[0].events.every(Schema.is(LinearIssueOpened))).toBe(true)
			expect(onIssueCreated).not.toHaveBeenCalled()
		}),
	)

	it.effect('selects the first configured directed trigger in admission order', ({ expect }) =>
		Effect.gen(function* () {
			const onAssigned = vi.fn<LinearCallbackHandler<'onAssigned'>>(() => Effect.void)
			const result = yield* process({ onAssigned }, [
				linearNotificationAdmission(appUserNotificationPayloads[0]),
				linearNotificationAdmission(appUserNotificationPayloads[2], 'assignment-delivery'),
			])
			expect(result).toEqual(ProviderEventHandled.make({}))
			expect(onAssigned).toHaveBeenCalledOnce()
			expect(onAssigned.mock.calls[0]?.[0].events.map((event) => event._tag)).toEqual(['LinearIssueMention'])
		}),
	)
})
