import { assert, it } from '@effect/vitest'
import { Effect, Schema } from 'effect'

import { Emoji } from '../src/index.ts'

it.effect('round-trips canonical and custom typed emoji', () =>
	Effect.gen(function* () {
		for (const emoji of [Emoji.ThumbsUp, Emoji.Heart, Emoji.Check, Emoji.custom('party_parrot')]) {
			const encoded = yield* Schema.encodeEffect(Emoji)(emoji)
			const decoded = yield* Schema.decodeEffect(Emoji)(encoded)
			assert.strictEqual(decoded.name, emoji.name)
			assert.strictEqual(decoded.unicode, emoji.unicode)
		}
	}),
)
