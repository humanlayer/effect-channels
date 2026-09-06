import { Effect, Match, Schema, SchemaIssue, SchemaTransformation } from 'effect'

import { InvalidSlackThreadId } from './Errors.ts'
import { ChannelId, TenantId, ThreadId, type ChannelRef, type ThreadRef } from './Model.ts'
import {
	SlackChannelAddress,
	SlackThreadRef,
	type SlackChannelAddress as SlackChannelAddressType,
	type SlackThreadRef as SlackThreadRefType,
} from './SlackIdentity.ts'
import { SlackChannelId, SlackMessageTs, SlackTeamId } from './SlackIdentity.ts'

export const encodeSlackChannelId = (teamId: SlackTeamId, channelId: SlackChannelId) =>
	ChannelId.make(`slack:v1:${encodeURIComponent(teamId)}:${encodeURIComponent(channelId)}`)

export const encodeSlackThreadId = (ref: SlackThreadRefType) =>
	ThreadId.make(
		ref.directMessageKind === undefined
			? `slack:v1:${encodeURIComponent(ref.teamId)}:${encodeURIComponent(ref.channelId)}:${encodeURIComponent(ref.threadTs)}`
			: `slack:v1:${encodeURIComponent(ref.teamId)}:${ref.directMessageKind}:${encodeURIComponent(ref.channelId)}${ref.threadTs === SlackDmConversationTs ? '' : `:${encodeURIComponent(ref.threadTs)}`}`,
	)

export const SlackDmConversationTs = SlackMessageTs.make('__conversation__')

const parseSlackThreadId = (threadId: ThreadId) => {
	const parts = threadId.split(':')
	if ((parts.length !== 5 && parts.length !== 6) || parts.at(0) !== 'slack' || parts.at(1) !== 'v1') {
		throw new Error('invalid Slack thread id')
	}
	const encodedTeamId = parts.at(2)
	const kindPart = parts.at(3)
	const directMessageKind: 'im' | 'mpim' | undefined = Match.value(kindPart).pipe(
		Match.when('im', () => 'im' as const),
		Match.when('mpim', () => 'mpim' as const),
		Match.orElse(() => undefined),
	)
	const encodedChannelId = directMessageKind === undefined ? parts.at(3) : parts.at(4)
	const encodedThreadTs = directMessageKind === undefined ? parts.at(4) : parts.at(5)
	if (encodedTeamId === undefined || encodedChannelId === undefined || encodedThreadTs === undefined) {
		if (directMessageKind === undefined || encodedTeamId === undefined || encodedChannelId === undefined) {
			throw new Error('invalid Slack thread id')
		}
	}
	const fields = {
		teamId: SlackTeamId.make(decodeURIComponent(encodedTeamId)),
		channelId: SlackChannelId.make(decodeURIComponent(encodedChannelId)),
		threadTs:
			encodedThreadTs === undefined
				? SlackDmConversationTs
				: SlackMessageTs.make(decodeURIComponent(encodedThreadTs)),
	}
	const ref =
		directMessageKind === undefined
			? SlackThreadRef.make(fields)
			: SlackThreadRef.make({ ...fields, directMessageKind })
	if (encodeSlackThreadId(ref) !== threadId) {
		throw new Error('non-canonical Slack thread id')
	}
	return ref
}

export const SlackThreadIdCodec = ThreadId.pipe(
	Schema.decodeTo(
		Schema.toType(SlackThreadRef),
		SchemaTransformation.transformOrFail({
			decode: (threadId, options) =>
				Effect.try({
					try: () => parseSlackThreadId(threadId),
					catch: () =>
						new SchemaIssue.InvalidValue(
							{ message: 'invalid canonical Slack thread id' },
							threadId,
							options,
						),
				}),
			encode: (ref) => Effect.succeed(encodeSlackThreadId(ref)),
		}),
	),
)

export const decodeSlackThreadId = (threadId: ThreadId) =>
	Schema.decodeEffect(SlackThreadIdCodec)(threadId).pipe(
		Effect.mapError(() => InvalidSlackThreadId.make({ threadId })),
	)

const parseSlackChannelId = (channelId: ChannelId) => {
	const parts = channelId.split(':')
	if (parts.length !== 4 || parts.at(0) !== 'slack' || parts.at(1) !== 'v1') {
		throw new Error('invalid Slack channel id')
	}
	const encodedTeamId = parts.at(2)
	const encodedChannelId = parts.at(3)
	if (encodedTeamId === undefined || encodedChannelId === undefined) {
		throw new Error('invalid Slack channel id')
	}
	const address = SlackChannelAddress.make({
		teamId: SlackTeamId.make(decodeURIComponent(encodedTeamId)),
		channelId: SlackChannelId.make(decodeURIComponent(encodedChannelId)),
	})
	if (encodeSlackChannelId(address.teamId, address.channelId) !== channelId) {
		throw new Error('non-canonical Slack channel id')
	}
	return address
}

export const SlackChannelIdCodec = ChannelId.pipe(
	Schema.decodeTo(
		Schema.toType(SlackChannelAddress),
		SchemaTransformation.transformOrFail({
			decode: (channelId, options) =>
				Effect.try({
					try: () => parseSlackChannelId(channelId),
					catch: () =>
						new SchemaIssue.InvalidValue(
							{ message: 'invalid canonical Slack channel id' },
							channelId,
							options,
						),
				}),
			encode: (address: SlackChannelAddressType) =>
				Effect.succeed(encodeSlackChannelId(address.teamId, address.channelId)),
		}),
	),
)

export const decodeSlackChannelId = (channelId: ChannelId) =>
	Schema.decodeEffect(SlackChannelIdCodec)(channelId).pipe(
		Effect.mapError(() => InvalidSlackThreadId.make({ threadId: channelId })),
	)

export const slackChannelRef = (
	teamId: SlackTeamId,
	channelId: SlackChannelId,
	directMessageKind?: 'im' | 'mpim',
): ChannelRef => ({
	id: encodeSlackChannelId(teamId, channelId),
	provider: 'slack',
	tenant: TenantId.make(teamId),
	isDm: directMessageKind !== undefined || channelId.startsWith('D'),
})

export const slackThreadRef = (ref: SlackThreadRefType, isNew: boolean): ThreadRef => ({
	id: encodeSlackThreadId(ref),
	channel: slackChannelRef(ref.teamId, ref.channelId, ref.directMessageKind),
	isNew,
})

export const slackDmConversationRef = (
	teamId: SlackTeamId,
	channelId: SlackChannelId,
	directMessageKind: 'im' | 'mpim',
) =>
	slackThreadRef(SlackThreadRef.make({ teamId, channelId, threadTs: SlackDmConversationTs, directMessageKind }), true)

export const slackEventThreadRefs = (input: {
	readonly teamId: SlackTeamId
	readonly channelId: SlackChannelId
	readonly rootTs: SlackMessageTs
	readonly isNew: boolean
	readonly directMessageKind: 'im' | 'mpim' | undefined
}) => {
	const fields = { teamId: input.teamId, channelId: input.channelId, threadTs: input.rootTs }
	if (input.directMessageKind === undefined) {
		return { threadRef: slackThreadRef(SlackThreadRef.make(fields), input.isNew) }
	}
	return {
		threadRef: slackThreadRef(
			SlackThreadRef.make({ ...fields, directMessageKind: input.directMessageKind }),
			input.isNew,
		),
		directMessageThread: slackDmConversationRef(input.teamId, input.channelId, input.directMessageKind),
	}
}
