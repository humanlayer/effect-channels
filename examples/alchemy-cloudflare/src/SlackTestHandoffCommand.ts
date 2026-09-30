/**
 * The example's `handoff [seconds]` command, read from the text of a Slack mention.
 *
 * `@bot handoff` waits the default time; `@bot handoff 60` waits 60 seconds. A wait outside the
 * allowed range, or text without the command, is not a handoff command.
 */
import type { SlackContent } from '@humanlayer/channels-slack-next'
import { Effect, Match, Option, Predicate, Schema, SchemaIssue, SchemaTransformation } from 'effect'

/** How long the remote agent waits before finishing: whole seconds, so a typo cannot park a thread for hours. */
export const HandoffDelaySeconds = Schema.Int.check(Schema.isBetween({ minimum: 5, maximum: 900 })).pipe(
	Schema.brand('HandoffDelaySeconds'),
)
export type HandoffDelaySeconds = typeof HandoffDelaySeconds.Type

/** A handoff command. Encoded, the wait is the text after `handoff`, and defaults to 60 when absent. */
export const HandoffCommand = Schema.Struct({
	delaySeconds: Schema.FiniteFromString.pipe(
		Schema.decodeTo(HandoffDelaySeconds),
		Schema.withDecodingDefaultKey(Effect.succeed('60')),
	),
})
export type HandoffCommand = typeof HandoffCommand.Type

/** The word `handoff` on its own, then optionally a whole number of seconds. */
const HANDOFF_COMMAND = /(?:^|\s)handoff(?:\s+(\d+))?(?=\s|$)/i

/** Message text to a handoff command. Fails for text that does not contain the command. */
export const HandoffCommandFromText = Schema.String.pipe(
	Schema.decodeTo(
		HandoffCommand,
		SchemaTransformation.transformOrFail({
			decode: (text) =>
				Option.match(Option.fromNullishOr(HANDOFF_COMMAND.exec(text)), {
					onNone: () =>
						Effect.fail(new SchemaIssue.InvalidValue({ message: 'not a handoff command' }, text)),
					onSome: ([, delaySeconds]) =>
						Effect.succeed(Predicate.isUndefined(delaySeconds) ? {} : { delaySeconds }),
				}),
			encode: ({ delaySeconds }) =>
				Effect.succeed(Predicate.isUndefined(delaySeconds) ? 'handoff' : `handoff ${delaySeconds}`),
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
