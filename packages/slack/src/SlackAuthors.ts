import { Context, Effect, Layer } from 'effect'

import { Message as MessageSchema, type Message } from './Message.ts'
import type { Author, TenantId } from './Model.ts'
import type { MessagePage, ThreadPage } from './Operations.ts'
import { SlackUserDirectory } from './SlackUserDirectory.ts'
import { Thread as ThreadSchema, type Thread } from './Thread.ts'

const messageWithAuthor = (message: Message, author: Author) => {
	const fields = {
		ref: message.ref,
		threadRef: message.threadRef,
		text: message.text,
		markdown: message.markdown,
		author,
		metadata: message.metadata,
		attachments: message.attachments,
		raw: message.raw,
	}
	return message.replyTo === undefined
		? MessageSchema.make(fields)
		: MessageSchema.make({ ...fields, replyTo: message.replyTo })
}

const make = Effect.gen(function* () {
	const users = yield* SlackUserDirectory
	const resolveAuthor = Effect.fn('slack.authors.resolve')(function* ({
		tenant,
		author,
	}: {
		readonly tenant: TenantId
		readonly author: Author
	}) {
		return yield* users.getUser({ provider: 'slack', tenant, userId: author.userId }).pipe(
			Effect.map((profile) => profile.author),
			Effect.catchTags({
				UnknownTenant: () => Effect.succeed(author),
				UserLookupFailed: () => Effect.succeed(author),
			}),
		)
	})

	const resolveMessage = (message: Message) =>
		resolveAuthor({ tenant: message.threadRef.channel.tenant, author: message.author }).pipe(
			Effect.map((author) => messageWithAuthor(message, author)),
		)

	const resolveMessages = (messages: ReadonlyArray<Message>) =>
		Effect.gen(function* () {
			const authors = new Map<string, Author>()
			return yield* Effect.forEach(messages, (message) => {
				const input = {
					provider: message.threadRef.channel.provider,
					tenant: message.threadRef.channel.tenant,
					userId: message.author.userId,
				}
				const key = `${input.provider}\u0000${input.tenant}\u0000${input.userId}`
				const existing = authors.get(key)
				if (existing !== undefined) {
					return Effect.succeed(messageWithAuthor(message, existing))
				}
				return Effect.map(resolveAuthor({ tenant: input.tenant, author: message.author }), (author) => {
					authors.set(key, author)
					return messageWithAuthor(message, author)
				})
			})
		})

	const resolveDelivery = ({ thread, message }: { readonly thread: Thread; readonly message: Message }) =>
		Effect.gen(function* () {
			const all = [message, ...thread.recentMessages]
			if (thread.currentMessage !== undefined) {
				all.push(thread.currentMessage)
			}
			const resolved = yield* resolveMessages(all)
			const resolvedMessage = resolved.at(0) ?? message
			const recentMessages = resolved.slice(1, 1 + thread.recentMessages.length)
			const currentMessage =
				thread.currentMessage === undefined ? undefined : resolved.at(1 + thread.recentMessages.length)
			const resolvedThread =
				currentMessage === undefined
					? ThreadSchema.make({ ref: thread.ref, recentMessages })
					: ThreadSchema.make({ ref: thread.ref, currentMessage, recentMessages })
			return { thread: resolvedThread, message: resolvedMessage }
		})

	return {
		resolveAuthor,
		resolveMessage,
		resolveDelivery,
		messagePage: (page: MessagePage) =>
			resolveMessages(page.messages).pipe(Effect.map((messages) => ({ ...page, messages }))),
		threadPage: (page: ThreadPage) =>
			Effect.forEach(page.threads, (summary) =>
				resolveMessage(summary.rootMessage).pipe(Effect.map((rootMessage) => ({ ...summary, rootMessage }))),
			).pipe(Effect.map((threads) => ({ ...page, threads }))),
	}
})

/** Internal message assembly, deliberately absent from the package's public exports. */
export class SlackAuthors extends Context.Service<SlackAuthors>()('slack/Authors', { make }) {
	static readonly layer = Layer.effect(SlackAuthors, SlackAuthors.make).pipe(
		Layer.provideMerge(SlackUserDirectory.layer),
	)
}
