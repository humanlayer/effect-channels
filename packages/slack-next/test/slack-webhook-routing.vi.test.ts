import { describe, it } from '@effect/vitest'
import { DeliveryReceipt, type DeliveryAdmission } from '@humanlayer/channels-delivery-next'
import { Effect } from 'effect'

import { makeSlackEmulatorFixture } from './fixtures'

describe('Slack webhook routing', () => {
	it.live('delivers a signed app mention through the HttpRouter into MailboxDelivery', ({ expect }) =>
		Effect.gen(function* () {
			const admissions: Array<DeliveryAdmission> = []
			const slack = yield* makeSlackEmulatorFixture({
				mailboxDelivery: {
					deliver: (admission) =>
						Effect.sync(() => {
							admissions.push(admission)
							return DeliveryReceipt.make({ mailboxKey: admission.resourceId, accepted: true })
						}),
				},
				reactionThreadResolver: {
					resolve: (input) => Effect.succeed(input.messageTs),
				},
			})
			const postMessage = (body: Record<string, unknown>) =>
				Effect.promise(async () => {
					const response = await fetch(`${slack.url}/api/chat.postMessage`, {
						method: 'POST',
						headers: {
							authorization: `Bearer ${slack.aliceToken}`,
							'content-type': 'application/json',
						},
						body: JSON.stringify(body),
					})
					const result = (await response.json()) as { readonly ok: boolean; readonly ts?: string }
					if (!result.ok || result.ts === undefined)
						throw new Error('Slack emulator rejected chat.postMessage')
					return result.ts
				})
			const rootTs = yield* postMessage({ channel: slack.channelId, text: 'Existing channel message' })

			yield* Effect.promise(() =>
				slack.webhooks.dispatch(
					'app_mention',
					undefined,
					{
						type: 'event_callback',
						team_id: slack.teamId,
						event_id: 'Ev-app-mention-root',
						event_time: Math.floor(Date.now() / 1000),
						event: {
							type: 'app_mention',
							user: slack.aliceUserId,
							text: '<@U_SLACK_NEXT> root mention',
							ts: rootTs,
							channel: slack.channelId,
						},
					},
					'slack',
				),
			)
			const replyTs = yield* postMessage({
				channel: slack.channelId,
				text: 'Existing threaded reply',
				thread_ts: rootTs,
			})
			yield* Effect.promise(() =>
				slack.webhooks.dispatch(
					'app_mention',
					undefined,
					{
						type: 'event_callback',
						team_id: slack.teamId,
						event_id: 'Ev-app-mention-thread',
						event_time: Math.floor(Date.now() / 1000),
						event: {
							type: 'app_mention',
							user: slack.aliceUserId,
							text: '<@U_SLACK_NEXT> threaded mention',
							ts: replyTs,
							thread_ts: rootTs,
							channel: slack.channelId,
						},
					},
					'slack',
				),
			)

			expect(admissions).toHaveLength(2)
			expect(admissions[0]).toMatchObject({
				namespace: 'slack-emulator-test',
				provider: 'slack',
				installationId: slack.teamId,
				resourceId: `slack:v1:${slack.teamId}:${slack.channelId}:${rootTs}`,
				eventId: 'Ev-app-mention-root',
			})
			expect(admissions[1]).toMatchObject({
				resourceId: `slack:v1:${slack.teamId}:${slack.channelId}:${rootTs}`,
				eventId: 'Ev-app-mention-thread',
			})
		}),
	)

	it.live('routes every supported Slack event through the emulator', ({ expect }) =>
		Effect.gen(function* () {
			const admissions: Array<DeliveryAdmission> = []
			const slack = yield* makeSlackEmulatorFixture({
				mailboxDelivery: {
					deliver: (admission) =>
						Effect.sync(() => {
							admissions.push(admission)
							return DeliveryReceipt.make({ mailboxKey: admission.resourceId, accepted: true })
						}),
				},
				reactionThreadResolver: {
					resolve: (input) => Effect.succeed(input.messageTs),
				},
			})
			const eventTime = Math.floor(Date.now() / 1000)
			yield* Effect.promise(() =>
				slack.webhooks.dispatch(
					'url_verification',
					undefined,
					{ type: 'url_verification', challenge: 'emulator-challenge' },
					'slack',
				),
			)

			for (const input of [
				{ eventId: 'Ev-message-channel', channel: slack.channelId, channelType: 'channel' },
				{ eventId: 'Ev-message-im', channel: 'D_DIRECT', channelType: 'im' },
				{ eventId: 'Ev-message-mpim', channel: 'G_GROUP', channelType: 'mpim' },
			]) {
				yield* Effect.promise(() =>
					slack.webhooks.dispatch(
						'message',
						undefined,
						{
							type: 'event_callback',
							team_id: slack.teamId,
							event_id: input.eventId,
							event_time: eventTime,
							event: {
								type: 'message',
								user: slack.aliceUserId,
								text: 'hello agent',
								ts: '1700000001.000001',
								channel: input.channel,
								channel_type: input.channelType,
							},
						},
						'slack',
					),
				)
			}

			yield* Effect.promise(() =>
				slack.webhooks.dispatch(
					'message',
					undefined,
					{
						type: 'event_callback',
						team_id: slack.teamId,
						event_id: 'Ev-message-thread',
						event_time: eventTime,
						event: {
							type: 'message',
							user: slack.aliceUserId,
							text: 'thread reply',
							ts: '1700000002.000001',
							thread_ts: '1700000000.000001',
							channel: slack.channelId,
							channel_type: 'channel',
						},
					},
					'slack',
				),
			)

			for (const type of ['reaction_added', 'reaction_removed'] as const) {
				yield* Effect.promise(() =>
					slack.webhooks.dispatch(
						type,
						undefined,
						{
							type: 'event_callback',
							team_id: slack.teamId,
							event_id: `Ev-${type}`,
							event_time: eventTime,
							event: {
								type,
								user: slack.aliceUserId,
								reaction: 'eyes',
								item: { type: 'message', channel: slack.channelId, ts: '1700000000.000001' },
								event_ts: '1700000003.000001',
							},
						},
						'slack',
					),
				)
			}

			yield* Effect.promise(() =>
				slack.webhooks.dispatch(
					'agent_session_stopped',
					undefined,
					{
						type: 'event_callback',
						team_id: slack.teamId,
						event_id: 'Ev-agent-session-stopped',
						event_time: eventTime,
						event: {
							type: 'agent_session_stopped',
							channel: slack.channelId,
							thread_ts: '1700000000.000001',
							user: slack.aliceUserId,
							event_ts: '1700000004.000001',
							streaming_message_ts: ['1700000002.000001'],
						},
					},
					'slack',
				),
			)

			expect(admissions.map((admission) => admission.eventId)).toEqual([
				'Ev-message-channel',
				'Ev-message-im',
				'Ev-message-mpim',
				'Ev-message-thread',
				'Ev-reaction_added',
				'Ev-reaction_removed',
				'Ev-agent-session-stopped',
			])
			expect(slack.webhooks.getDeliveries().every((delivery) => delivery.status_code === 200)).toBe(true)
			expect(admissions[3]?.resourceId).toBe(`slack:v1:${slack.teamId}:${slack.channelId}:1700000000.000001`)
		}),
	)
})
