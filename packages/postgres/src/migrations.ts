import { ConversationCoordinatorPostgresMigrations } from '@humanlayer/channels'
import { Effect } from 'effect'

import { UserProfileCachePostgresMigrations } from './UserProfileCachePostgres.ts'

export const migrations = Effect.all([ConversationCoordinatorPostgresMigrations, UserProfileCachePostgresMigrations], {
	discard: true,
})
