import { assert, it, layer } from '@effect/vitest'
import {
	ChannelInfo,
	MarkdownContent,
	MessagePage,
	OrgId,
	ThreadInfo,
	ThreadPage,
	type Author,
} from '@humanlayer/channels'
import { SlackConnection, SlackConnectionCredentials } from '@humanlayer/channels-slack'
import { ConfigProvider, Context, Effect, Exit, Layer, Queue, Redacted, Ref, Scope, Stream } from 'effect'

import { ChannelsStorage, createChannelsApp, slack } from '../src/index.ts'
import {
	HistoryResponse,
	PostedMessageResponse,
	SlackEmulator,
	slackEmulatorAdminToken,
	slackEmulatorAliceToken,
	slackEmulatorBotId,
	slackEmulatorBotToken,
	slackEmulatorBotUserId,
	slackEmulatorIntegrationToken,
	slackEmulatorIntegrationUserId,
	slackEmulatorSigningSecret,
} from './support/SlackEmulator.ts'

interface MentionObservation {
	readonly channelId: string
	readonly isNew: boolean
	readonly author: Author
	readonly backward: MessagePage
	readonly forwardTexts: ReadonlyArray<string>
	readonly allTexts: ReadonlyArray<string>
	readonly nonSelfBotTexts: ReadonlyArray<string>
	readonly participants: ReadonlyArray<Author>
	readonly threadInfo: ThreadInfo
	readonly channelInfo: ChannelInfo
	readonly channelPage: MessagePage
	readonly threadPage: ThreadPage
}

interface FollowUpObservation {
	readonly channelId: string
	readonly text: string
	readonly subscribed: boolean
}

interface SlackInnerEventPayload {
	type: 'app_mention' | 'message'
	channel: string
	ts: string
	text: string
	user: string
	thread_ts?: string
	bot_id?: string
}

interface SlackCallbackPayload {
	type: 'event_callback'
	team_id: string
	event_id: string
	event_time: number
	event: SlackInnerEventPayload
}

const eventCallback = (input: {
	readonly teamId: string
	readonly eventId: string
	readonly type: 'app_mention' | 'message'
	readonly channelId: string
	readonly ts: string
	readonly text: string
	readonly userId: string
	readonly threadTs?: string
	readonly botId?: string
}) => {
	const event: SlackInnerEventPayload = {
		type: input.type,
		channel: input.channelId,
		ts: input.ts,
		text: input.text,
		user: input.userId,
	}
	if (input.threadTs !== undefined) {
		event.thread_ts = input.threadTs
	}
	if (input.botId !== undefined) {
		event.bot_id = input.botId
	}
	const callback: SlackCallbackPayload = {
		type: 'event_callback',
		team_id: input.teamId,
		event_id: input.eventId,
		event_time: Math.floor(Date.now() / 1000),
		event,
	}
	return callback
}

layer(SlackEmulator.layer, { timeout: '30 seconds' })('Slack emulator integration', (it) => {
	it.effect(
		'runs signed public/private Slack conversations through production HTTP, routing, history, metadata, hydration, and suppression',
		() =>
			Effect.gen(function* () {
				const emulator = yield* SlackEmulator
				const mentions = yield* Queue.unbounded<MentionObservation>()
				const followUps = yield* Queue.unbounded<FollowUpObservation>()
				const loadedWorkspaces = yield* Ref.make<ReadonlyArray<string>>([])
				const mentionCount = yield* Ref.make(0)
				const followUpCount = yield* Ref.make(0)
				const connection = (enabled: boolean) =>
					SlackConnection.make({
						organizationId: OrgId.make(enabled ? 'org-emulator' : 'org-disabled'),
						enabled,
						credentials: SlackConnectionCredentials.make({
							botToken: Redacted.make(slackEmulatorBotToken),
							botUserId: slackEmulatorBotUserId,
							botId: slackEmulatorBotId,
						}),
					})
				const app = createChannelsApp({
					providers: [
						slack({
							loadConnection: ({ workspaceId }) =>
								Ref.update(loadedWorkspaces, (workspaces) => [...workspaces, workspaceId]).pipe(
									Effect.as(
										workspaceId === emulator.teamId
											? connection(true)
											: workspaceId === 'T_DISABLED'
												? connection(false)
												: undefined,
									),
								),
						}),
					],
					storage: ChannelsStorage.memory(),
					handlers: {
						onNewMention: (thread, message) =>
							Effect.gen(function* () {
								yield* Ref.update(mentionCount, (count) => count + 1)
								yield* thread.subscribe()
								const backward = yield* thread.listMessages({ direction: 'backward', limit: 1 })
								const forward = yield* thread.listMessages({ direction: 'forward', limit: 1 })
								const all = yield* Stream.runCollect(thread.allMessages)
								const participants = yield* thread.getParticipants()
								const threadInfo = yield* thread.fetchMetadata()
								const channelInfo = yield* thread.channel.fetchMetadata()
								const channelPage = yield* thread.channel.listMessages({
									direction: 'backward',
									limit: 1,
								})
								const threadPage = yield* thread.channel.listThreads({ limit: 10 })
								yield* thread.post(MarkdownContent.make({ markdown: `mention echo: ${message.text}` }))
								yield* Queue.offer(mentions, {
									channelId: thread.ref.channel.id,
									isNew: thread.ref.isNew,
									author: message.author,
									backward,
									forwardTexts: forward.messages.map((item) => item.text),
									allTexts: Array.from(all, (item) => item.text),
									nonSelfBotTexts: Array.from(all)
										.filter((item) => item.author.isBot === true && !item.author.isMe)
										.map((item) => item.text),
									participants,
									threadInfo,
									channelInfo,
									channelPage,
									threadPage,
								})
							}),
						onSubscribedMessage: (thread, message) =>
							Effect.gen(function* () {
								yield* Ref.update(followUpCount, (count) => count + 1)
								const subscribed = yield* thread.isSubscribed()
								yield* thread.post(
									MarkdownContent.make({ markdown: `follow-up echo: ${message.text}` }),
								)
								yield* Queue.offer(followUps, {
									channelId: thread.ref.channel.id,
									text: message.text,
									subscribed,
								})
							}),
					},
					advanced: {
						slackApiOrigin: new URL(`${emulator.emulator.url}/api`),
						configProvider: ConfigProvider.fromUnknown({
							SLACK_SIGNING_SECRET: slackEmulatorSigningSecret,
							SLACK_BOT_USER_ID: slackEmulatorBotUserId,
							SLACK_BOT_ID: slackEmulatorBotId,
						}),
					},
				})
				yield* Effect.addFinalizer(() => Effect.promise(() => app.close()))

				const secondaryRoot = yield* emulator.call(
					slackEmulatorAdminToken,
					'chat.postMessage',
					{ channel: emulator.publicChannelId, text: 'another public root' },
					PostedMessageResponse,
				)
				assert.strictEqual(secondaryRoot.message.user, emulator.adminUserId)
				const publicRoot = yield* emulator.call(
					slackEmulatorAliceToken,
					'chat.postMessage',
					{ channel: emulator.publicChannelId, text: `<@${slackEmulatorBotUserId}> public hello` },
					PostedMessageResponse,
				)
				const publicPriorReply = yield* emulator.call(
					slackEmulatorAdminToken,
					'chat.postMessage',
					{ channel: emulator.publicChannelId, thread_ts: publicRoot.ts, text: 'admin context' },
					PostedMessageResponse,
				)
				assert.strictEqual(publicPriorReply.message.thread_ts, publicRoot.ts)
				const mentionResponse = yield* Effect.promise(() =>
					app.handle(
						emulator.signedWebhook(
							eventCallback({
								teamId: emulator.teamId,
								eventId: 'Ev_public_mention',
								type: 'app_mention',
								channelId: publicRoot.channel,
								ts: publicRoot.ts,
								text: publicRoot.message.text,
								userId: publicRoot.message.user ?? emulator.aliceUserId,
							}),
						),
					),
				)
				assert.strictEqual(mentionResponse.status, 200)
				const publicMention = yield* Queue.take(mentions)
				assert.strictEqual(publicMention.author.userId, emulator.aliceUserId)
				assert.strictEqual(publicMention.author.userName, 'alice')
				assert.strictEqual(publicMention.author.fullName, 'Alice Example')
				assert.strictEqual(publicMention.author.isBot, false)
				assert.strictEqual(publicMention.backward.messages.length, 1)
				assert.strictEqual(publicMention.backward.messages[0]?.text, 'admin context')
				assert.notStrictEqual(publicMention.backward.nextCursor, undefined)
				assert.deepStrictEqual(publicMention.forwardTexts, [publicRoot.message.text, 'admin context'])
				assert.deepStrictEqual(publicMention.allTexts, [publicRoot.message.text, 'admin context'])
				assert.deepStrictEqual(
					publicMention.participants.map((participant) => participant.userId),
					[emulator.aliceUserId, emulator.adminUserId],
				)
				assert.strictEqual(publicMention.threadInfo.thread.id.includes(publicRoot.ts), true)
				assert.strictEqual(publicMention.channelInfo.name, 'channels-public')
				assert.ok((publicMention.channelInfo.memberCount ?? 0) >= 3)
				assert.strictEqual(publicMention.channelPage.messages.length, 1)
				assert.strictEqual(
					publicMention.threadPage.threads.some((item) => item.thread.id.includes(publicRoot.ts)),
					true,
				)

				const publicAfterMention = yield* emulator.call(
					slackEmulatorBotToken,
					'conversations.replies',
					{ channel: emulator.publicChannelId, ts: publicRoot.ts },
					HistoryResponse,
				)
				const mentionEcho = publicAfterMention.messages.find((message) =>
					message.text.startsWith('mention echo:'),
				)
				assert.strictEqual(mentionEcho?.user, slackEmulatorBotUserId)
				assert.strictEqual(mentionEcho?.thread_ts, publicRoot.ts)

				const followUp = yield* emulator.call(
					slackEmulatorAliceToken,
					'chat.postMessage',
					{ channel: emulator.publicChannelId, thread_ts: publicRoot.ts, text: 'human follow-up' },
					PostedMessageResponse,
				)
				const followUpResponse = yield* Effect.promise(() =>
					app.handle(
						emulator.signedWebhook(
							eventCallback({
								teamId: emulator.teamId,
								eventId: 'Ev_public_followup',
								type: 'message',
								channelId: followUp.channel,
								ts: followUp.ts,
								threadTs: publicRoot.ts,
								text: followUp.message.text,
								userId: followUp.message.user ?? emulator.aliceUserId,
							}),
						),
					),
				)
				assert.strictEqual(followUpResponse.status, 200)
				assert.deepStrictEqual(yield* Queue.take(followUps), {
					channelId: publicMention.channelId,
					text: 'human follow-up',
					subscribed: true,
				})
				const publicAfterFollowUp = yield* emulator.call(
					slackEmulatorBotToken,
					'conversations.replies',
					{ channel: emulator.publicChannelId, ts: publicRoot.ts },
					HistoryResponse,
				)
				assert.strictEqual(
					publicAfterFollowUp.messages.some(
						(message) =>
							message.user === slackEmulatorBotUserId &&
							message.text === 'follow-up echo: human follow-up',
					),
					true,
				)

				const selfResponse = yield* Effect.promise(() =>
					app.handle(
						emulator.signedWebhook(
							eventCallback({
								teamId: emulator.teamId,
								eventId: 'Ev_bot_self_message',
								type: 'message',
								channelId: publicRoot.channel,
								ts: mentionEcho?.ts ?? publicRoot.ts,
								threadTs: publicRoot.ts,
								text: mentionEcho?.text ?? 'mention echo',
								userId: mentionEcho?.user ?? slackEmulatorBotUserId,
								botId: slackEmulatorBotId,
							}),
						),
					),
				)
				assert.strictEqual(selfResponse.status, 200)
				assert.strictEqual(yield* Ref.get(mentionCount), 1)
				assert.strictEqual(yield* Ref.get(followUpCount), 1)
				assert.strictEqual(yield* Queue.size(mentions), 0)
				assert.strictEqual(yield* Queue.size(followUps), 0)

				const integrationRoot = yield* emulator.call(
					slackEmulatorIntegrationToken,
					'chat.postMessage',
					{ channel: emulator.publicChannelId, text: 'integration-authored root' },
					PostedMessageResponse,
				)
				assert.strictEqual(integrationRoot.message.user, slackEmulatorIntegrationUserId)
				const integrationMention = yield* emulator.call(
					slackEmulatorAliceToken,
					'chat.postMessage',
					{
						channel: emulator.publicChannelId,
						thread_ts: integrationRoot.ts,
						text: `<@${slackEmulatorBotUserId}> join this integration thread`,
					},
					PostedMessageResponse,
				)
				const countBeforeExistingThreadMention = yield* Ref.get(mentionCount)
				const integrationMentionResponse = yield* Effect.promise(() =>
					app.handle(
						emulator.signedWebhook(
							eventCallback({
								teamId: emulator.teamId,
								eventId: 'Ev_integration_thread_mention',
								type: 'app_mention',
								channelId: integrationMention.channel,
								ts: integrationMention.ts,
								threadTs: integrationRoot.ts,
								text: integrationMention.message.text,
								userId: integrationMention.message.user ?? emulator.aliceUserId,
							}),
						),
					),
				)
				assert.strictEqual(integrationMentionResponse.status, 200)
				const existingThreadMention = yield* Queue.take(mentions)
				assert.strictEqual(existingThreadMention.isNew, false)
				assert.strictEqual(existingThreadMention.allTexts.includes('integration-authored root'), true)
				assert.strictEqual(existingThreadMention.nonSelfBotTexts.includes('integration-authored root'), true)
				assert.strictEqual(yield* Ref.get(mentionCount), countBeforeExistingThreadMention + 1)
				const integrationReplies = yield* emulator.call(
					slackEmulatorBotToken,
					'conversations.replies',
					{ channel: emulator.publicChannelId, ts: integrationRoot.ts },
					HistoryResponse,
				)
				assert.strictEqual(
					integrationReplies.messages.some(
						(message) =>
							message.user === slackEmulatorBotUserId &&
							message.thread_ts === integrationRoot.ts &&
							message.text === 'mention echo: join this integration thread',
					),
					true,
				)

				const privateRoot = yield* emulator.call(
					slackEmulatorAliceToken,
					'chat.postMessage',
					{ channel: emulator.privateChannelId, text: `<@${slackEmulatorBotUserId}> private hello` },
					PostedMessageResponse,
				)
				const privateMentionResponse = yield* Effect.promise(() =>
					app.handle(
						emulator.signedWebhook(
							eventCallback({
								teamId: emulator.teamId,
								eventId: 'Ev_private_mention',
								type: 'app_mention',
								channelId: privateRoot.channel,
								ts: privateRoot.ts,
								text: privateRoot.message.text,
								userId: privateRoot.message.user ?? emulator.aliceUserId,
							}),
						),
					),
				)
				assert.strictEqual(privateMentionResponse.status, 200)
				const privateMention = yield* Queue.take(mentions)
				assert.strictEqual(privateMention.channelInfo.name, 'channels-private')
				assert.strictEqual(privateMention.channelInfo.channel.isDm, false)
				const privateFollowUp = yield* emulator.call(
					slackEmulatorAliceToken,
					'chat.postMessage',
					{ channel: emulator.privateChannelId, thread_ts: privateRoot.ts, text: 'private follow-up' },
					PostedMessageResponse,
				)
				yield* Effect.promise(() =>
					app.handle(
						emulator.signedWebhook(
							eventCallback({
								teamId: emulator.teamId,
								eventId: 'Ev_private_followup',
								type: 'message',
								channelId: privateFollowUp.channel,
								ts: privateFollowUp.ts,
								threadTs: privateRoot.ts,
								text: privateFollowUp.message.text,
								userId: privateFollowUp.message.user ?? emulator.aliceUserId,
							}),
						),
					),
				)
				assert.strictEqual((yield* Queue.take(followUps)).text, 'private follow-up')
				const privateHistory = yield* emulator.call(
					slackEmulatorBotToken,
					'conversations.replies',
					{ channel: emulator.privateChannelId, ts: privateRoot.ts },
					HistoryResponse,
				)
				assert.deepStrictEqual(
					privateHistory.messages.map((message) => message.text),
					[
						privateRoot.message.text,
						'mention echo: private hello',
						'private follow-up',
						'follow-up echo: private follow-up',
					],
				)

				for (const [teamId, eventId] of [
					['T_DISABLED', 'Ev_disabled_workspace'],
					['T_UNKNOWN', 'Ev_unknown_workspace'],
				] as const) {
					const response = yield* Effect.promise(() =>
						app.handle(
							emulator.signedWebhook(
								eventCallback({
									teamId,
									eventId,
									type: 'app_mention',
									channelId: publicRoot.channel,
									ts: publicRoot.ts,
									text: publicRoot.message.text,
									userId: publicRoot.message.user ?? emulator.aliceUserId,
								}),
							),
						),
					)
					assert.strictEqual(response.status, 200)
				}
				assert.strictEqual(yield* Ref.get(mentionCount), 3)
				assert.strictEqual(yield* Ref.get(followUpCount), 2)
				const workspaces = yield* Ref.get(loadedWorkspaces)
				assert.strictEqual(workspaces.includes(emulator.teamId), true)
				assert.strictEqual(workspaces.includes('T_DISABLED'), true)
				assert.strictEqual(workspaces.includes('T_UNKNOWN'), true)
				yield* Effect.promise(() => app.close())
				yield* Effect.promise(() => app.close())
				const lateRequest = yield* Effect.promise(() =>
					app.handle(
						emulator.signedWebhook(
							eventCallback({
								teamId: emulator.teamId,
								eventId: 'Ev_after_app_close',
								type: 'app_mention',
								channelId: publicRoot.channel,
								ts: publicRoot.ts,
								text: publicRoot.message.text,
								userId: publicRoot.message.user ?? emulator.aliceUserId,
							}),
						),
					),
				).pipe(Effect.exit)
				assert.strictEqual(Exit.isFailure(lateRequest), true)
				assert.strictEqual(yield* Ref.get(mentionCount), 3)
			}),
		{ timeout: 30_000 },
	)
})

it.effect(
	'releases the emulator listener when its explicit Layer scope closes',
	() =>
		Effect.gen(function* () {
			const scope = yield* Scope.make()
			const context = yield* Layer.buildWithScope(SlackEmulator.layer, scope)
			const emulator = Context.get(context, SlackEmulator)
			yield* Scope.close(scope, Exit.void)
			yield* Scope.close(scope, Exit.void)
			const requestAfterClose = yield* Effect.tryPromise({
				try: (signal) => fetch(`${emulator.emulator.url}/api/auth.test`, { method: 'POST', signal }),
				catch: (cause) => cause,
			}).pipe(Effect.exit)
			assert.strictEqual(Exit.isFailure(requestAfterClose), true)
		}),
	{ timeout: 10_000 },
)
