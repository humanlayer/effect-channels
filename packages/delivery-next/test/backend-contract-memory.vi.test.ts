import { describe } from '@effect/vitest'

import { MailboxBackendMemory } from '../src/MailboxBackendMemory'
import { mailboxBackendContract } from './backend-contract'

describe('mailbox backend contract', () => {
	mailboxBackendContract('memory', () => MailboxBackendMemory)
})
