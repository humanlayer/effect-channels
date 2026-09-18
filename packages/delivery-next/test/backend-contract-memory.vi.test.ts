import { describe } from '@effect/vitest'

import { mailboxBackendContract } from './backend-contract'
import { MailboxBackendMemory } from './MailboxBackendMemory'

describe('mailbox backend contract', () => {
	mailboxBackendContract('memory', () => MailboxBackendMemory)
})
