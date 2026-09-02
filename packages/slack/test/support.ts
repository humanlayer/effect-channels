import { Effect, Redacted } from 'effect'

import { hmacSha256 } from '../src/SlackSignature.ts'

export const signingSecret = Redacted.make('test-signing-secret')

export const signSlackBody = (body: string, timestamp: string, secret = 'test-signing-secret') => {
	return hmacSha256({
		secret: Redacted.make(secret),
		data: new TextEncoder().encode(`v0:${timestamp}:${body}`),
	}).pipe(Effect.map((signature) => `v0=${bytesToHex(signature)}`))
}

export const bytesToHex = (bytes: Uint8Array) =>
	Array.from(bytes, (byte) => byte.toString(16).padStart(2, '0')).join('')

export const appMentionCallback = {
	type: 'event_callback' as const,
	team_id: 'T_TEST',
	event_id: 'Ev_TEST_1',
	event_time: 1_788_000_000,
	event: {
		type: 'app_mention' as const,
		user: 'U_HUMAN',
		text: '<@U_BOT> hello from Slack',
		ts: '100.1',
		channel: 'C_TEST',
	},
}
