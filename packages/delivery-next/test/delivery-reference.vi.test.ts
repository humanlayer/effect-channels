import * as NodeCrypto from '@effect/platform-node/NodeCrypto'
import { describe, it } from '@effect/vitest'
import { Effect, Option } from 'effect'

import {
	BatchId,
	ENCODED_MAILBOX_KEY_MAX_LENGTH,
	deliveryAccessTokenMatches,
	deliveryMailboxKey,
	isRoutableMailboxKey,
	makeBatchId,
	makeConversationId,
	makeDeliveryAccessToken,
	makeDeliveryId,
	parseDeliveryId,
} from '../src'

const mailboxKey = deliveryMailboxKey({
	namespace: 'app',
	provider: 'linear',
	installationId: 'org-1',
	resourceId: 'linear:v1:agent-session:6f7c|x',
})
const batchId = BatchId.make('batch_1-a')

describe('delivery reference', () => {
	it('round-trips a delivery ID to its mailbox key and batch ID', ({ expect }) => {
		const deliveryId = makeDeliveryId({ mailboxKey, batchId })
		expect(deliveryId.startsWith('delivery:v1:')).toBe(true)
		expect(parseDeliveryId(deliveryId)).toEqual(Option.some({ deliveryId, mailboxKey, batchId }))
	})

	it('refuses anything that is not a canonical v1 delivery ID', ({ expect }) => {
		const deliveryId = makeDeliveryId({ mailboxKey, batchId })
		for (const candidate of [
			'',
			'delivery:v2:abc',
			'delivery:v1::batch',
			`${deliveryId}:extra`,
			deliveryId.replace('delivery:v1:', 'delivery:v1:%%'),
			`delivery:v1:${'A'.repeat(2_100)}:batch`,
			/** Base64url with padding bits set decodes, but does not re-encode to the same text. */
			'delivery:v1:YR:batch',
		]) {
			expect(parseDeliveryId(candidate)).toEqual(Option.none())
		}
	})

	it('gives every delivery in a mailbox the same conversation ID', ({ expect }) => {
		expect(makeConversationId(mailboxKey)).toBe(makeConversationId(mailboxKey))
		expect(makeConversationId(mailboxKey) === makeConversationId(`${mailboxKey}-other`)).toBe(false)
	})

	it('only routes mailbox keys whose delivery IDs fit in a request path', ({ expect }) => {
		expect(isRoutableMailboxKey(mailboxKey)).toBe(true)
		expect(isRoutableMailboxKey('k'.repeat(ENCODED_MAILBOX_KEY_MAX_LENGTH))).toBe(false)
	})

	it('compares tokens exactly', ({ expect }) => {
		expect(deliveryAccessTokenMatches({ saved: 'abc', presented: 'abc' })).toBe(true)
		expect(deliveryAccessTokenMatches({ saved: 'abc', presented: 'abd' })).toBe(false)
		expect(deliveryAccessTokenMatches({ saved: 'abc', presented: 'ab' })).toBe(false)
	})

	it.effect('makes url-safe batch IDs and tokens that differ every time', ({ expect }) =>
		Effect.gen(function* () {
			const [firstBatch, secondBatch] = [yield* makeBatchId, yield* makeBatchId]
			const [firstToken, secondToken] = [yield* makeDeliveryAccessToken, yield* makeDeliveryAccessToken]
			expect(firstBatch === secondBatch).toBe(false)
			expect(firstToken === secondToken).toBe(false)
			expect(firstToken).toMatch(/^[A-Za-z0-9_-]{43}$/)
		}).pipe(Effect.provide(NodeCrypto.layer)),
	)
})
