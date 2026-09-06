type MailboxKeyInput = { readonly key: string }
type PrefixInput = { readonly prefix: string }

const namespace = 'humanlayer:delivery:v1:{mailboxes}'

const encode = (input: string) => {
	let result = ''
	for (let offset = 0; offset < input.length; offset++) {
		result += input.charCodeAt(offset).toString(16).padStart(4, '0')
	}
	return result
}

export const recordKey = (input: MailboxKeyInput) => `${namespace}:record:${encode(input.key)}`
export const readyKey = (input: PrefixInput) => `${namespace}:ready:${encode(input.prefix)}`
export const readyKeys = (input: MailboxKeyInput) => {
	const keys: Array<string> = []
	for (let length = 0; length <= input.key.length; length++) {
		keys.push(readyKey({ prefix: input.key.slice(0, length) }))
	}
	return keys
}
