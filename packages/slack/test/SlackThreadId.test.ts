import { assert, it } from '@effect/vitest'
import { ThreadId } from '@humanlayer/channels'
import { Effect, Schema } from 'effect'

import { InvalidSlackThreadId } from '../src/Errors.ts'
import { SlackChannelId, SlackMessageTs, SlackTeamId, SlackThreadRef } from '../src/Schema.ts'
import { decodeSlackThreadId, encodeSlackThreadId } from '../src/SlackThreadId.ts'

it.effect('round-trips and pins canonical escaped Slack thread ids', () =>
	Effect.gen(function* () {
		const ref = SlackThreadRef.make({
			teamId: SlackTeamId.make('T:ONE'),
			channelId: SlackChannelId.make('C/ONE'),
			threadTs: SlackMessageTs.make('100.1'),
		})
		const encoded = encodeSlackThreadId(ref)

		assert.strictEqual(encoded, 'slack:v1:T%3AONE:C%2FONE:100.1')
		assert.deepStrictEqual(yield* decodeSlackThreadId(encoded), ref)
	}),
)

it.effect('rejects non-canonical and wrong-version ids', () =>
	Effect.gen(function* () {
		const nonCanonical = yield* Effect.flip(decodeSlackThreadId(ThreadId.make('slack:v1:T%3aONE:C%2fONE:100.1')))
		const wrongVersion = yield* Effect.flip(decodeSlackThreadId(ThreadId.make('slack:v2:T:C:100.1')))

		assert.ok(Schema.is(InvalidSlackThreadId)(nonCanonical))
		assert.ok(Schema.is(InvalidSlackThreadId)(wrongVersion))
	}),
)
