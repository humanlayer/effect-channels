import { Context } from 'effect'

import type { SlackConnection, SlackConnectionLookupInput } from './SlackConnection.js'

/** Private operation-local identity: the profile lookup must use the credentials that identified its cache entry. */
export class SlackCredentialSnapshot extends Context.Service<
	SlackCredentialSnapshot,
	SlackConnectionLookupInput & SlackConnection
>()('slack/internal/CredentialSnapshot') {}
