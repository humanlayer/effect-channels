import { ConfigProvider, Effect, Layer, Queue } from 'effect'
import { HttpClient, HttpClientRequest, HttpClientResponse } from 'effect/http'

import { SlackApiLiveBase } from '../src/SlackApiLive'
import { SlackChannelId, SlackMessageTs, SlackTeamId } from '../src/SlackIdentity'
import { SlackChannelRef, SlackThreadRef } from '../src/SlackModels'
import { SlackFileMetadata } from '../src/SlackWebhookEventSchemas'

export const botToken = 'xoxb-never-log'
export const teamId = SlackTeamId.make('T061EG9R6')
export const channelId = SlackChannelId.make('C0123CHANNEL')
export const rootTs = SlackMessageTs.make('1531763342.000100')
export const channelRef = SlackChannelRef.make({ teamId, channelId, isDm: false })
export const threadRef = SlackThreadRef.make({ teamId, channelId, threadTs: rootTs, isDm: false })

export const fileId = 'F0S43PZDF'
export const privateUrl = `https://files.slack.com/files-pri/${teamId}-${fileId}/notes.txt`
export const privateDownloadUrl = `https://files.slack.com/files-pri/${teamId}-${fileId}/download/notes.txt`

/** A file object as Slack sends it in message events, `conversations.replies`, and upload completion. */
export const slackFileObject = {
	id: fileId,
	created: 1_531_763_342,
	timestamp: 1_531_763_342,
	name: 'notes.txt',
	title: 'notes.txt',
	mimetype: 'text/plain',
	filetype: 'text',
	pretty_type: 'Plain Text',
	user: 'U061F7AUR',
	user_team: teamId,
	editable: true,
	size: 24,
	mode: 'hosted',
	is_external: false,
	external_type: '',
	is_public: true,
	public_url_shared: false,
	display_as_bot: false,
	username: '',
	url_private: privateUrl,
	url_private_download: privateDownloadUrl,
	permalink: `https://example.slack.com/files/U061F7AUR/${fileId}/notes.txt`,
	permalink_public: `https://slack-files.com/${teamId}-${fileId}-3e9c6b4cd9`,
	has_rich_preview: false,
	file_access: 'visible',
} as const

export const slackFileMetadata = SlackFileMetadata.make({
	id: slackFileObject.id,
	name: slackFileObject.name,
	mimetype: slackFileObject.mimetype,
	size: slackFileObject.size,
	url_private: slackFileObject.url_private,
	url_private_download: slackFileObject.url_private_download,
})

export type RecordedSlackRequest = {
	readonly url: string
	readonly method: string
	readonly authorization: string | null
	readonly contentType: string | null
	readonly body: Uint8Array
}

export type SlackHttpResponder = (request: RecordedSlackRequest) => Response

/** Records every outgoing request in order and answers it from the test's responder. */
export const makeRecordingSlackHttp = (requests: Queue.Queue<RecordedSlackRequest>, respond: SlackHttpResponder) =>
	HttpClient.make((request) =>
		Effect.gen(function* () {
			const web = yield* HttpClientRequest.toWeb(request).pipe(Effect.orDie)
			const recorded: RecordedSlackRequest = {
				url: web.url,
				method: web.method,
				authorization: web.headers.get('authorization'),
				contentType: web.headers.get('content-type'),
				body: new Uint8Array(yield* Effect.promise(() => web.arrayBuffer())),
			}
			yield* Queue.offer(requests, recorded)
			return HttpClientResponse.fromWeb(request, respond(recorded))
		}),
	)

export const slackApiLayer = (http: HttpClient.HttpClient) =>
	SlackApiLiveBase.pipe(
		Layer.provide(
			Layer.mergeAll(
				Layer.succeed(HttpClient.HttpClient, http),
				ConfigProvider.layer(
					ConfigProvider.fromUnknown({ SLACK_BOT_TOKEN: botToken, SLACK_BOT_USER_ID: 'U_BOT' }),
				),
			),
		),
	)

export const formParams = (request: RecordedSlackRequest) =>
	Object.fromEntries(new URLSearchParams(new TextDecoder().decode(request.body)))

export const slackMethod = (request: RecordedSlackRequest) =>
	request.url.startsWith('https://slack.com/api/') ? request.url.slice('https://slack.com/api/'.length) : null
