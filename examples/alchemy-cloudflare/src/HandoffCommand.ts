/**
 * The example's `handoff [seconds] [flaky] [ask]` command, read from a Slack mention, a GitHub mention,
 * or the text of a Linear Agent Session. A new Linear issue uses `issue-handoff [seconds]` instead, so an issue
 * titled `handoff 30` and delegated to the app starts only the session's handoff, not a second one.
 *
 * `handoff` waits the default time; `handoff 60` waits 60 seconds. `flaky` makes Slack refuse the final
 * message for a while, to show that output retries on its own; it is a Slack-only test switch, and the
 * GitHub and Linear callbacks ignore it. `ask` ends the turn with a question
 * instead of an answer. A wait outside the allowed range, or text without the command, is not a
 * handoff command.
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
 * @property askForInput - end the turn with a question and choices instead of an answer
 */
export const HandoffCommand = Schema.Struct({
	delaySeconds: Schema.FiniteFromString.pipe(
		Schema.decodeTo(HandoffDelaySeconds),
		Schema.withDecodingDefaultKey(Effect.succeed('60')),
	),
	flakyOutput: Schema.Boolean.pipe(Schema.withDecodingDefaultKey(Effect.succeed(false))),
	askForInput: Schema.Boolean.pipe(Schema.withDecodingDefaultKey(Effect.succeed(false))),
})
export type HandoffCommand = typeof HandoffCommand.Type

/**
 * The word `keyword` on its own, then optionally a whole number of seconds, then any of `flaky` and
 * `ask`. `>` and `<` count as edges too, so a command inside Linear's XML-like prompt context is found.
 */
const commandPattern = (keyword: string) =>
	new RegExp(`(?:^|[\\s>])${keyword}(?:\\s+(\\d+))?((?:\\s+(?:flaky|ask))*)(?=[\\s<]|$)`, 'i')

/** Text to the command named `keyword`. Fails for text that does not contain the command. */
const commandFromText = (keyword: string) => {
	const pattern = commandPattern(keyword)
	return Schema.String.pipe(
	Schema.decodeTo(
		HandoffCommand,
		SchemaTransformation.transformOrFail({
			decode: (text) =>
				Option.match(Option.fromNullishOr(pattern.exec(text)), {
					onNone: () =>
						Effect.fail(new SchemaIssue.InvalidValue({ message: 'not a handoff command' }, text)),
					onSome: ([, delaySeconds, flags]) => {
						const command: Types.Mutable<typeof HandoffCommand.Encoded> = {}
						const words = (flags ?? '').toLowerCase().split(/\s+/)
						if (Predicate.isNotUndefined(delaySeconds)) command.delaySeconds = delaySeconds
						if (words.includes('flaky')) command.flakyOutput = true
						if (words.includes('ask')) command.askForInput = true
						return Effect.succeed(command)
					},
				}),
			encode: ({ delaySeconds, flakyOutput, askForInput }) => {
				const words = [keyword]
				if (Predicate.isNotUndefined(delaySeconds)) words.push(delaySeconds)
				if (flakyOutput === true) words.push('flaky')
				if (askForInput === true) words.push('ask')
				return Effect.succeed(words.join(' '))
			},
		}),
	),
	)
}

/** Message text to a `handoff` command. */
export const HandoffCommandFromText = commandFromText('handoff')

/** Issue text to an `issue-handoff` command. */
export const IssueHandoffCommandFromText = commandFromText('issue-handoff')

const contentText = (content: SlackContent): string =>
	Match.valueTags(content, {
		SlackPlainTextContent: (plain) => plain.text,
		SlackMarkdownContent: (markdown) => markdown.markdown,
	})

/** The `handoff` command in some text, if it has one. */
export const parseHandoffText = (text: string): Option.Option<HandoffCommand> =>
	Schema.decodeOption(HandoffCommandFromText)(text)

/** The `issue-handoff` command in an issue's text, if it has one. */
export const parseIssueHandoffText = (text: string): Option.Option<HandoffCommand> =>
	Schema.decodeOption(IssueHandoffCommandFromText)(text)

/** The handoff command in a Slack message, if it has one. */
export const parseHandoffCommand = (content: SlackContent): Option.Option<HandoffCommand> =>
	parseHandoffText(contentText(content))
