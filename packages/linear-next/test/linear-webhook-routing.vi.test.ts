import { NodeCrypto } from '@effect/platform-node'
import { describe, it } from '@effect/vitest'
import { DeliveryReceipt, MailboxDelivery, webhookRoutes } from '@humanlayer/channels-delivery-next'
import { Context, Effect, Redacted } from 'effect'
import { HttpRouter } from 'effect/unstable/http'

import { makeLinearWebhookProvider } from '../src/LinearWebhookProvider'
import {
	issueCreatePayload,
	linearAppUserId,
	linearOauthClientId,
	linearOrganizationId,
	linearWebhookSecret,
	signedLinearInput,
} from './fixtures'

const makeApp = Effect.gen(function* () {
	const provider = makeLinearWebhookProvider({
		namespace: 'linear-routing-test',
		webhookSecret: Redacted.make(linearWebhookSecret),
		organizationId: linearOrganizationId,
		appUserId: linearAppUserId,
		oauthClientId: linearOauthClientId,
		maxBodyBytes: 4096,
	})
	const web = HttpRouter.toWebHandler(webhookRoutes([provider]).pipe(HttpRouter.provideRequest(NodeCrypto.layer)), {
		disableLogger: true,
	})
	yield* Effect.addFinalizer(() => Effect.promise(web.dispose))
	const context = Context.make(
		MailboxDelivery,
		MailboxDelivery.of({
			deliver: (admission) =>
				Effect.succeed(DeliveryReceipt.make({ mailboxKey: admission.resourceId, accepted: true })),
		}),
	)
	return (input: ReturnType<typeof signedLinearInput>) =>
		Effect.promise(() =>
			web.handler(
				new Request('http://localhost/integrations/linear/webhook', {
					method: 'POST',
					headers: input.headers,
					body: new TextDecoder().decode(input.body),
				}),
				context,
			),
		)
})

describe('Linear webhook routing', () => {
	it.effect('maps accepted, unauthenticated, mismatched, and oversized requests', ({ expect }) =>
		Effect.gen(function* () {
			const request = yield* makeApp
			expect((yield* request(signedLinearInput(issueCreatePayload, undefined, Date.now()))).status).toBe(200)

			const signed = signedLinearInput(issueCreatePayload, undefined, Date.now())
			const invalid = {
				...signed,
				headers: { ...signed.headers, 'linear-signature': '0'.repeat(64) },
			}
			expect((yield* request(invalid)).status).toBe(401)

			const mismatch = { ...(issueCreatePayload as Record<string, unknown>), organizationId: 'another-workspace' }
			expect((yield* request(signedLinearInput(mismatch, undefined, Date.now()))).status).toBe(403)

			const oversized = { ...(issueCreatePayload as Record<string, unknown>), padding: 'x'.repeat(8192) }
			expect((yield* request(signedLinearInput(oversized, undefined, Date.now()))).status).toBe(413)
		}),
	)
})
