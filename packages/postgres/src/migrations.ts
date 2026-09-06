import { Effect } from 'effect'

import { ConversationCoordinatorPostgresMigrations } from './ConversationCoordinatorPostgres.ts'
import { UserProfileCachePostgresMigrations } from './UserProfileCachePostgres.ts'

export const migrations = Effect.all([ConversationCoordinatorPostgresMigrations, UserProfileCachePostgresMigrations], {
	discard: true,
})
