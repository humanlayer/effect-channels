import { ChannelId, TenantId, ThreadId, type ChannelRef, type ThreadRef } from '@humanlayer/channels'
import { Effect, Schema, SchemaIssue, SchemaTransformation } from 'effect'

import { InvalidSlackThreadId } from './Errors.ts'
import {
	SlackChannelAddress,
	SlackChannelId,
	SlackMessageTs,
	SlackTeamId,
	SlackThreadRef,
	type SlackChannelAddress as SlackChannelAddressType,
	type SlackThreadRef as SlackThreadRefType,
} from './Schema.ts'

export const encodeSlackChannelId = (teamId: SlackTeamId, channelId: SlackChannelId) =>
	ChannelId.make(`slack:v1:${encodeURIComponent(teamId)}:${encodeURIComponent(channelId)}`)

export const encodeSlackThreadId = (ref: SlackThreadRefType) =>
	ThreadId.make(
		`slack:v1:${encodeURIComponent(ref.teamId)}:${encodeURIComponent(ref.channelId)}:${encodeURIComponent(ref.threadTs)}`,
	)

const parseSlackThreadId = (threadId: ThreadId) => {
	const parts = threadId.split(':')
	if (parts.length !== 5 || parts.at(0) !== 'slack' || parts.at(1) !== 'v1') {
		throw new Error('invalid Slack thread id')
	}
	const encodedTeamId = parts.at(2)
	const encodedChannelId = parts.at(3)
	const encodedThreadTs = parts.at(4)
	if (encodedTeamId === undefined || encodedChannelId === undefined || encodedThreadTs === undefined) {
		throw new Error('invalid Slack thread id')
	}
	const ref = SlackThreadRef.make({
		teamId: SlackTeamId.make(decodeURIComponent(encodedTeamId)),
		channelId: SlackChannelId.make(decodeURIComponent(encodedChannelId)),
		threadTs: SlackMessageTs.make(decodeURIComponent(encodedThreadTs)),
	})
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

export const slackChannelRef = (teamId: SlackTeamId, channelId: SlackChannelId): ChannelRef => ({
	id: encodeSlackChannelId(teamId, channelId),
	provider: 'slack',
	tenant: TenantId.make(teamId),
	isDm: channelId.startsWith('D'),
})

export const slackThreadRef = (ref: SlackThreadRefType, isNew: boolean): ThreadRef => ({
	id: encodeSlackThreadId(ref),
	channel: slackChannelRef(ref.teamId, ref.channelId),
	isNew,
})
