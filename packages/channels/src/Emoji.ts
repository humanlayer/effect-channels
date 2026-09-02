import { Schema } from 'effect'

export class Emoji extends Schema.TaggedClass<Emoji>()('Emoji', {
	name: Schema.NonEmptyString,
	unicode: Schema.optionalKey(Schema.String),
}) {
	static readonly ThumbsUp = Emoji.make({ name: 'thumbs_up', unicode: '👍' })
	static readonly Heart = Emoji.make({ name: 'heart', unicode: '❤️' })
	static readonly Check = Emoji.make({ name: 'check', unicode: '✅' })

	static custom(name: string) {
		return Emoji.make({ name })
	}
}
