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
export const mailboxSubscriptionsKey = `${root}:subscriptions`
