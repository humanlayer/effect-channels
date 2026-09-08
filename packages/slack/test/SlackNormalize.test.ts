import { NodeCrypto } from '@effect/platform-node'
import { assert, it } from '@effect/vitest'
import { Effect, Option, Schema } from 'effect'

import { SlackEventCallback } from '../src/Schema.js'
import { normalizeSlackMessage } from '../src/SlackNormalize.js'
import { appMentionCallback } from './support.js'

it.effect('normalizes a root app mention into queue-safe schema classes', () =>
	Effect.gen(function* () {
		const callback = yield* Schema.decodeEffect(SlackEventCallback)(appMentionCallback)
		const result = yield* normalizeSlackMessage({ callback, identity: { botUserId: 'U_BOT' } })
		const normalized = Option.getOrThrow(result)

		assert.strictEqual(normalized.provider, 'slack')
		assert.strictEqual(normalized.tenant, 'T_TEST')
		assert.strictEqual(normalized.thread.ref.id, 'slack:v1:T_TEST:C_TEST:100.1')
		assert.strictEqual(normalized.thread.ref.isNew, true)
		assert.strictEqual(normalized.message.text, 'hello from Slack')
		assert.strictEqual(normalized.message.ref, '100.1')
		assert.strictEqual(normalized.mentioned, true)
		assert.match(normalized.idempotencyKey, /^evt_[a-f0-9]{32}$/)
	}).pipe(Effect.provide(NodeCrypto.layer)),
)

it.effect('encodes transformed Slack file URLs before storing raw queue data', () =>
	Effect.gen(function* () {
		const callback = yield* Schema.decodeEffect(SlackEventCallback)({
			...appMentionCallback,
			event: {
				...appMentionCallback.event,
				files: [
					{
						id: 'F_IMAGE',
						name: 'image.png',
						mimetype: 'image/png',
						url_private: 'https://files.slack.com/files-pri/F_IMAGE/image.png',
					},
				],
			},
		})
		const result = yield* normalizeSlackMessage({ callback, identity: { botUserId: 'U_BOT' } })
		const normalized = Option.getOrThrow(result)

		assert.ok(Schema.is(Schema.Json)(normalized.raw))
		assert.ok(Schema.is(Schema.Json)(normalized.message.raw))
		assert.strictEqual(normalized.message.attachments[0]?.ref.id, 'F_IMAGE')
	}).pipe(Effect.provide(NodeCrypto.layer)),
)
