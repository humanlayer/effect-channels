import { assert, it } from '@effect/vitest'
import { Effect, Schema } from 'effect'

import { GitHubEmoji, GitHubReactionContent } from '../src/index'

it.effect('round-trips all named GitHub emoji as native strings through both schema names', () =>
	Effect.gen(function* () {
		const cases = [
			[GitHubEmoji.ThumbsUp, '+1'],
			[GitHubEmoji.ThumbsDown, '-1'],
			[GitHubEmoji.Laugh, 'laugh'],
			[GitHubEmoji.Confused, 'confused'],
			[GitHubEmoji.Heart, 'heart'],
			[GitHubEmoji.Hooray, 'hooray'],
			[GitHubEmoji.Rocket, 'rocket'],
			[GitHubEmoji.Eyes, 'eyes'],
		] as const
		assert.strictEqual(GitHubReactionContent, GitHubEmoji)
		assert.deepEqual(
			GitHubEmoji.literals,
			cases.map(([, wire]) => wire),
		)
		for (const schema of [GitHubEmoji, GitHubReactionContent]) {
			for (const [emoji, wire] of cases) {
				assert.strictEqual(emoji, wire)
				assert.strictEqual(yield* Schema.encodeEffect(schema)(emoji), wire)
				assert.strictEqual(yield* Schema.decodeEffect(schema)(wire), emoji)
				assert.strictEqual(schema.make(wire), emoji)
			}
			for (const invalid of ['thumbs_up', 'thumbsup', 'party_parrot', '👍', { name: 'eyes' }]) {
				assert.strictEqual(yield* Schema.decodeUnknownEffect(schema)(invalid).pipe(Effect.isFailure), true)
			}
		}
	}),
)
