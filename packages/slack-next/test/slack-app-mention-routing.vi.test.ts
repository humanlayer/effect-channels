import { describe, it } from '@effect/vitest'
import { DeliveryReceipt, type DeliveryAdmission } from '@humanlayer/channels-delivery-next'
import { Effect } from 'effect'

import { makeSlackEmulatorFixture } from './fixtures'

describe('Slack app mention webhook routing', () => {
	it.live('delivers a signed app mention through the HttpRouter into DeliveryQueue', ({ expect }) =>
		Effect.gen(function* () {
			const admissions: Array<DeliveryAdmission> = []
			const slack = yield* makeSlackEmulatorFixture({
				deliveryQueue: {
					enqueue: (admission) =>
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
})
