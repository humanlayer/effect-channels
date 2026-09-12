import { Config, Effect, Layer, Match, Option, Predicate, Schema } from 'effect'
import type { Redacted } from 'effect'
import { HttpRouter, HttpServerResponse } from 'effect/unstable/http'

import { SlackWebhookError } from './Errors.js'
import type { SlackBotIdentity, SlackChannelId, SlackEventCallback } from './Schema.js'
import { SlackChannelInfoInput, SlackEventsRequest, SlackMessageTs } from './Schema.js'
import { mergeSlackBotIdentity, slackBotIdentity } from './SlackBotIdentity.js'
import { SlackClient } from './SlackClient.js'
import { SlackIngress } from './SlackIngress.js'
import {
	normalizeSlackConversationStopped,
	normalizeSlackMessage,
	normalizeSlackMessageDeleted,
	normalizeSlackMessageUpdated,
	normalizeSlackReaction,
} from './SlackNormalize.js'
import { verifySlackSignature } from './SlackSignature.js'
import { SlackTenantCredentials } from './SlackTenantCredentials.js'

type SlackRoutesConfig = {
	readonly signingSecret: Redacted.Redacted<string>
	readonly identity: SlackBotIdentity
}

const webhookErrorResponse = (error: SlackWebhookError) => {
	const status = Match.value(error.reason).pipe(
		Match.when('decode', () => 400),
		Match.when('crypto', () => 500),
		Match.when('missing_headers', () => 401),
		Match.when('invalid_timestamp', () => 401),
		Match.when('stale', () => 401),
		Match.when('invalid_signature', () => 401),
		Match.exhaustive,
	)
	return Effect.succeed(HttpServerResponse.empty({ status }))
}

const resolveBotIdentity = (config: SlackRoutesConfig, callback: SlackEventCallback) =>
	Effect.gen(function* () {
		const credentials = yield* SlackTenantCredentials
		return yield* credentials
			.load({ teamId: callback.team_id })
			.pipe(Effect.map(Option.map((creds) => mergeSlackBotIdentity(config.identity, Option.some(creds)))))
	})

const resolveDirectMessageKind = Effect.fn('slack.webhook.resolve_direct_message_kind')(function* (input: {
	readonly teamId: SlackEventCallback['team_id']
	readonly channelId: SlackChannelId
	readonly hint?: 'channel' | 'group' | 'im' | 'mpim'
}) {
	if (input.hint === 'im' || input.channelId.startsWith('D')) return 'im' as const
	if (input.hint === 'mpim') return 'mpim' as const
	if (input.hint === 'channel' || input.hint === 'group' || !input.channelId.startsWith('G')) return undefined
	const client = yield* SlackClient
	const info = yield* client.channelInfo(
		SlackChannelInfoInput.make({ teamId: input.teamId, channelId: input.channelId }),
	)
	return info.channel.isDm ? ('mpim' as const) : undefined
})

const acceptMessageEvent = (config: SlackRoutesConfig, callback: SlackEventCallback) =>
	Effect.gen(function* () {
		const ingress = yield* SlackIngress
		const identity = yield* resolveBotIdentity(config, callback)
		if (Option.isNone(identity)) {
			yield* Effect.logWarning('acknowledging Slack event for an unknown installation').pipe(
				Effect.annotateLogs({ provider: 'slack', tenant: callback.team_id, event_id: callback.event_id }),
			)
			return HttpServerResponse.empty({ status: 200 })
		}
		const normalized = yield* normalizeSlackMessage({ callback, identity: identity.value })
		if (Option.isNone(normalized)) {
			yield* Effect.logInfo('acknowledging and dropping ineligible Slack message subtype').pipe(
				Effect.annotateLogs({
					provider: 'slack',
					event_type: callback.event.type,
					event_subtype: 'subtype' in callback.event ? callback.event.subtype : undefined,
					event_id: callback.event_id,
				}),
			)
			return HttpServerResponse.empty({ status: 200 })
		}
		yield* ingress.acceptMessage(normalized.value)
		return HttpServerResponse.empty({ status: 200 })
	})

const acceptLifecycleEvent = (config: SlackRoutesConfig, callback: SlackEventCallback) =>
	Effect.gen(function* () {
		const ingress = yield* SlackIngress
		const identity = yield* resolveBotIdentity(config, callback)
		if (Option.isNone(identity)) return HttpServerResponse.empty({ status: 200 })
		const event = callback.event
		if (event.type === 'reaction_added' || event.type === 'reaction_removed') {
			const directMessageKind = yield* resolveDirectMessageKind({
				teamId: callback.team_id,
				channelId: event.item.channel,
			})
			const client = yield* SlackClient
			const parentThreadTs = yield* client
				.replies({
					teamId: callback.team_id,
					channelId: event.item.channel,
					threadTs: event.item.ts,
					limit: 1,
				})
				.pipe(
					Effect.map((page) => {
						const raw = page.messages[0]?.raw
						return Predicate.hasProperty(raw, 'thread_ts') && Predicate.isString(raw.thread_ts)
							? SlackMessageTs.make(raw.thread_ts)
							: event.item.ts
					}),
					Effect.catchTag(['UnknownTenant', 'SlackTransportError', 'SlackApiError'], (error) =>
						Effect.logWarning(
							'Slack reaction parent lookup failed; using reacted message as thread root',
							error,
						).pipe(Effect.as(event.item.ts)),
					),
				)
			yield* ingress.acceptReaction(
				yield* normalizeSlackReaction({
					callback,
					identity: identity.value,
					parentThreadTs,
					directMessageKind,
				}),
			)
		} else if (event.type === 'message' && event.subtype === 'message_changed') {
			const directMessageKind = yield* resolveDirectMessageKind({
				teamId: callback.team_id,
				channelId: event.channel,
				hint: event.channel_type,
			})
			yield* ingress.acceptMessageUpdated(
				yield* normalizeSlackMessageUpdated({ callback, identity: identity.value, directMessageKind }),
			)
		} else if (event.type === 'message' && event.subtype === 'message_deleted') {
			const directMessageKind = yield* resolveDirectMessageKind({
				teamId: callback.team_id,
				channelId: event.channel,
				hint: event.channel_type,
			})
			yield* ingress.acceptMessageDeleted(
				yield* normalizeSlackMessageDeleted({ callback, identity: identity.value, directMessageKind }),
			)
		} else {
			return yield* SlackWebhookError.make({ reason: 'decode' })
		}
		return HttpServerResponse.empty({ status: 200 })
	})

const acceptConversationStopped = (callback: SlackEventCallback) =>
	Effect.gen(function* () {
		const ingress = yield* SlackIngress
		const tenantCredentials = yield* SlackTenantCredentials
		const credentials = yield* tenantCredentials.load({ teamId: callback.team_id })
		if (Option.isNone(credentials)) return HttpServerResponse.empty({ status: 200 })
		const event = callback.event
		if (event.type !== 'agent_session_stopped') return yield* SlackWebhookError.make({ reason: 'decode' })
		const directMessageKind = yield* resolveDirectMessageKind({
			teamId: callback.team_id,
			channelId: event.channel,
		})
		yield* ingress.acceptConversationStopped(
			yield* normalizeSlackConversationStopped({ callback, directMessageKind }),
		)
		return HttpServerResponse.empty({ status: 200 })
	})

const routes = (config: SlackRoutesConfig) =>
	HttpRouter.add('POST', '/api/v1/integrations/slack/webhook', (request) =>
		Effect.gen(function* () {
			const body = yield* request.text
			const timestamp = request.headers['x-slack-request-timestamp']
			const signature = request.headers['x-slack-signature']
			if (timestamp === undefined || signature === undefined) {
				return yield* SlackWebhookError.make({ reason: 'missing_headers' })
			}
			yield* verifySlackSignature({
				body,
				timestamp,
				signature,
				signingSecret: config.signingSecret,
			})
			const payload = yield* Schema.decodeEffect(Schema.fromJsonString(SlackEventsRequest))(body, {
				onExcessProperty: 'preserve',
			}).pipe(Effect.mapError(() => SlackWebhookError.make({ reason: 'decode' })))
			return yield* Match.value(payload).pipe(
				Match.discriminatorsExhaustive('type')({
					url_verification: (verification) => Effect.succeed(HttpServerResponse.text(verification.challenge)),
					event_callback: (callback) =>
						Match.value(callback.event).pipe(
							Match.discriminatorsExhaustive('type')({
								app_mention: () => acceptMessageEvent(config, callback),
								message: (message) =>
									message.subtype === 'message_changed' || message.subtype === 'message_deleted'
										? acceptLifecycleEvent(config, callback)
										: acceptMessageEvent(config, callback),
								reaction_added: () => acceptLifecycleEvent(config, callback),
								reaction_removed: () => acceptLifecycleEvent(config, callback),
								agent_session_stopped: () => acceptConversationStopped(callback),
							}),
						),
				}),
			)
		}).pipe(
			Effect.catchTags({
				SlackWebhookError: webhookErrorResponse,
				SlackIngressError: (error) =>
					Effect.logError('Slack ingress admission failed', error).pipe(
						Effect.as(HttpServerResponse.empty({ status: error.reason === 'unexpected' ? 500 : 503 })),
					),
				CredentialStoreError: (error) =>
					Effect.logError('Slack credential lookup failed', error).pipe(
						Effect.as(HttpServerResponse.empty({ status: 503 })),
					),
				UnknownTenant: (error) =>
					Effect.logError('Slack installation became unavailable', error).pipe(
						Effect.as(HttpServerResponse.empty({ status: 503 })),
					),
				SlackTransportError: (error) =>
					Effect.logError('Slack metadata request failed', error).pipe(
						Effect.as(HttpServerResponse.empty({ status: 503 })),
					),
				SlackApiError: (error) =>
					Effect.logError('Slack metadata request failed', error).pipe(
						Effect.as(HttpServerResponse.empty({ status: 503 })),
					),
			}),
			Effect.withSpan('slack.webhook', { attributes: { provider: 'slack' } }),
		),
	)

export const SlackRoutes = {
	layer: Layer.unwrap(
		Effect.gen(function* () {
			const signingSecret = yield* Config.redacted('SLACK_SIGNING_SECRET')
			const botUserId = yield* Config.option(Config.string('SLACK_BOT_USER_ID'))
			const botId = yield* Config.option(Config.string('SLACK_BOT_ID'))
			const identity = slackBotIdentity({
				botUserId: Option.getOrUndefined(botUserId),
				botId: Option.getOrUndefined(botId),
			})
			return routes({ signingSecret, identity }).pipe(
				HttpRouter.provideRequest(Layer.effect(SlackTenantCredentials, SlackTenantCredentials)),
				HttpRouter.provideRequest(Layer.effect(SlackClient, SlackClient)),
			)
		}),
	),
}
