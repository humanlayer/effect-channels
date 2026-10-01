const root = 'delivery:next:{mailboxes}'

const encode = (value: string) => {
	let encoded = ''
	for (let offset = 0; offset < value.length; offset++) {
		encoded += value.charCodeAt(offset).toString(16).padStart(4, '0')
	}
	return encoded
}

export const readyMailboxesKey = `${root}:ready`
export const mailboxStateKey = (mailboxKey: string) => `${root}:state:${encode(mailboxKey)}`
export const mailboxPendingKey = (mailboxKey: string) => `${root}:pending:${encode(mailboxKey)}`
export const mailboxEventsKey = (mailboxKey: string) => `${root}:events:${encode(mailboxKey)}`
/** A mailbox's finished deliveries, kept for status reads and repeated requests until the last one's retention ends. */
export const mailboxRetainedKey = (mailboxKey: string) => `${root}:retained:${encode(mailboxKey)}`
export const mailboxSubscriptionsKey = `${root}:subscriptions`
