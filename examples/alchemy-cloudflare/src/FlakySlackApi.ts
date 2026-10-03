/**
 * A test aid: a `SlackApi` that refuses some posts for a while, to show that output retries on its own.
 *
 * The fake remote agent ends a `flaky` job's Markdown with a marker naming a time. Until that time,
 * posting that message fails the way an unreachable Slack does, which output treats as retryable.
 * After it, the message posts with the marker removed. Every other call goes to `SlackApiLive`.
 */
import { SlackApi, SlackApiError, SlackApiLive, SlackMarkdownContent } from '@humanlayer/channels-slack'
import { Clock, Effect, Layer, Option, Predicate } from 'effect'

const MARKER = /\s*\[fail-slack-output-until:(\d+)\]\s*$/

/** Markdown that `FlakySlackApiLive` refuses to post until `until`. */
export const withFlakyOutputMarker = (markdown: string, until: number) =>
	`${markdown} [fail-slack-output-until:${until}]`

export const FlakySlackApiLive = Layer.effect(
	SlackApi,
	Effect.gen(function* () {
		const slackApi = yield* SlackApi
		return SlackApi.of({
			...slackApi,
			postToThread: (request) =>
				Effect.gen(function* () {
					const { content } = request
					if (!Predicate.isTagged(content, 'SlackMarkdownContent'))
						return yield* slackApi.postToThread(request)
					const marked = Option.fromNullishOr(MARKER.exec(content.markdown))
					if (Option.isNone(marked)) return yield* slackApi.postToThread(request)
					const until = Number(marked.value[1])
					if ((yield* Clock.currentTimeMillis) < until) {
						yield* Effect.logWarning('Example Slack API refusing a flaky post on purpose').pipe(
							Effect.annotateLogs({ fails_until_ms: until }),
						)
						return yield* SlackApiError.make({ operation: 'post', message: 'Could not reach Slack' })
					}
					const markdown = content.markdown.replace(MARKER, '')
					return yield* slackApi.postToThread({
						...request,
						content: SlackMarkdownContent.make({ markdown }),
					})
				}),
		})
	}),
).pipe(Layer.provide(SlackApiLive))
