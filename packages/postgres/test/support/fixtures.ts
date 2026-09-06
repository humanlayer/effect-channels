import { ChannelId, ProviderName, TenantId, ThreadId, UserId, type ThreadRef } from '@humanlayer/channels-slack'
export const testChannelRef = {
	id: ChannelId.make('slack:v1:T_TEST:C_TEST'),
	provider: ProviderName.make('slack'),
	tenant: TenantId.make('T_TEST'),
	isDm: false,
}

export const testAuthor = {
	userId: UserId.make('U_TEST'),
	userName: 'tester',
	fullName: 'Test User',
	isBot: false,
	isMe: false,
}

export const testThreadRefFor = (rootTs: string, isNew: boolean): ThreadRef => ({
	id: ThreadId.make(`slack:v1:T_TEST:C_TEST:${rootTs}`),
	channel: testChannelRef,
	isNew,
})
