import { describe, it } from '@effect/vitest'
import {
	DeliveryAdmission,
	MailboxSubscriptionCreatedResult,
	MailboxSubscriptions,
	ProviderEventHandled,
	ProviderEventIgnored,
	ProviderEventInvalid,
} from '@humanlayer/channels-delivery-next'
import { Effect, Layer } from 'effect'
import { vi } from 'vite-plus/test'

import { LinearApi } from '../src/LinearApi'
import type { LinearIssueCreated } from '../src/LinearCallbackEvents'
import { LinearCallbacks } from '../src/LinearCallbacks'
import { makeLinearEventProcessor } from '../src/LinearEventProcessor'
import { linearIssueCreateAdmission } from './fixtures'

const namespace = 'linear-processing-test'
const services = <E, R>(handler?: (event: LinearIssueCreated) => Effect.Effect<void, E, R>) =>
	Layer.mergeAll(
		LinearCallbacks.layer(handler === undefined ? {} : { onIssueCreated: handler }),
		Layer.mock(LinearApi, {}),
		Layer.mock(MailboxSubscriptions, {
			subscribe: () => Effect.succeed(MailboxSubscriptionCreatedResult.make({})),
			isSubscribed: () => Effect.succeed(false),
			unsubscribe: () => Effect.void,
		}),
	)

describe('Linear event processing', () => {
	it.effect('decodes stored payloads and dispatches onIssueCreated', ({ expect }) =>
		Effect.gen(function* () {
			const admission = linearIssueCreateAdmission(namespace)
			const callback = vi.fn((_event: LinearIssueCreated) => Effect.void)
			const result = yield* makeLinearEventProcessor({ namespace })
				.process([admission])
				.pipe(Effect.provide(services(callback)))
			expect(result).toEqual(ProviderEventHandled.make({}))
			expect(callback).toHaveBeenCalledOnce()
			const event = callback.mock.calls[0]?.[0]
			expect(event?.issue.ref.issueId).toBe('b33fb278-fbe0-45e4-b4eb-94b0839f51b9')
			expect(event?.issue.mailboxKey).toContain('linear:v1:issue:')
		}),
	)

	it.effect('returns an explicit ignore when the callback is absent', ({ expect }) =>
		Effect.gen(function* () {
			const admission = linearIssueCreateAdmission(namespace)
			const result = yield* makeLinearEventProcessor({ namespace })
				.process([admission])
				.pipe(Effect.provide(services<never, never>()))
			expect(result).toEqual(ProviderEventIgnored.make({ reason: 'callback_not_configured' }))
		}),
	)

	it.effect('rejects mailbox and payload identity disagreement', ({ expect }) =>
		Effect.gen(function* () {
			const admission = linearIssueCreateAdmission(namespace)
			const mismatched = DeliveryAdmission.make({ ...admission, installationId: 'another-workspace' })
			const error = yield* makeLinearEventProcessor({ namespace })
				.process([mismatched])
				.pipe(Effect.provide(services<never, never>()), Effect.flip)
			expect(error).toEqual(ProviderEventInvalid.make({ provider: 'linear', reason: 'identity_mismatch' }))
		}),
	)
})
