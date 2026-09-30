/**
 * The example's `handoff [seconds] [flaky]` command, read from the text of a Slack mention.
 *
 * `@bot handoff` waits the default time; `@bot handoff 60` waits 60 seconds. `flaky` makes Slack
 * refuse the final message for a while, to show that output retries on its own. A wait outside the
 * allowed range, or text without the command, is not a handoff command.
 */
import type { SlackContent } from '@humanlayer/channels-slack-next'
import { Effect, Match, Option, Predicate, Schema, SchemaIssue, SchemaTransformation, type Types } from 'effect'

/** How long the remote agent waits before finishing: whole seconds, so a typo cannot park a thread for hours. */
export const HandoffDelaySeconds = Schema.Int.check(Schema.isBetween({ minimum: 5, maximum: 900 })).pipe(
	Schema.brand('HandoffDelaySeconds'),
)
export type HandoffDelaySeconds = typeof HandoffDelaySeconds.Type

/**
 * A handoff command. Encoded, the wait is the text after `handoff`, and defaults to 60 when absent.
 *
 * @property flakyOutput - make Slack refuse the final message for a while
 */
export const HandoffCommand = Schema.Struct({
	delaySeconds: Schema.FiniteFromString.pipe(
		Schema.decodeTo(HandoffDelaySeconds),
		Schema.withDecodingDefaultKey(Effect.succeed('60')),
	),
	flakyOutput: Schema.Boolean.pipe(Schema.withDecodingDefaultKey(Effect.succeed(false))),
})
export type HandoffCommand = typeof HandoffCommand.Type

/** The word `handoff` on its own, then optionally a whole number of seconds, then optionally `flaky`. */
const HANDOFF_COMMAND = /(?:^|\s)handoff(?:\s+(\d+))?(?:\s+(flaky))?(?=\s|$)/i

/** Message text to a handoff command. Fails for text that does not contain the command. */
export const HandoffCommandFromText = Schema.String.pipe(
	Schema.decodeTo(
		HandoffCommand,
		SchemaTransformation.transformOrFail({
			decode: (text) =>
				Option.match(Option.fromNullishOr(HANDOFF_COMMAND.exec(text)), {
					onNone: () =>
						Effect.fail(new SchemaIssue.InvalidValue({ message: 'not a handoff command' }, text)),
					onSome: ([, delaySeconds, flaky]) => {
						const command: Types.Mutable<typeof HandoffCommand.Encoded> = {}
						if (Predicate.isNotUndefined(delaySeconds)) command.delaySeconds = delaySeconds
						if (Predicate.isNotUndefined(flaky)) command.flakyOutput = true
						return Effect.succeed(command)
					},
				}),
			encode: ({ delaySeconds, flakyOutput }) => {
				const words = ['handoff']
				if (Predicate.isNotUndefined(delaySeconds)) words.push(delaySeconds)
				if (flakyOutput === true) words.push('flaky')
				return Effect.succeed(words.join(' '))
			},
		}),
	),
)

const contentText = (content: SlackContent): string =>
	Match.valueTags(content, {
		SlackPlainTextContent: (plain) => plain.text,
		SlackMarkdownContent: (markdown) => markdown.markdown,
	})

/** The handoff command in a Slack message, if it has one. */
export const parseHandoffCommand = (content: SlackContent): Option.Option<HandoffCommand> =>
	Schema.decodeOption(HandoffCommandFromText)(contentText(content))
