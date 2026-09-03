import { Cache, Context, Data, Effect, Layer, Option } from 'effect'

import type { Message } from './Message.ts'
import { Message as MessageSchema } from './Message.ts'
import type { GetUserInput } from './Operations.ts'
import { ProviderRegistry } from './ProviderRegistry.ts'
import type { Author, ProviderName, TenantId, UserId, UserProfile } from './Schema.ts'
import type { Thread } from './Thread.ts'
import { Thread as ThreadSchema } from './Thread.ts'

class UserDirectoryKey extends Data.Class<{
	readonly provider: ProviderName
	readonly tenant: TenantId
	readonly userId: UserId
}> {}

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

/** Resolves and caches provider user profiles while preserving ID-based authors when lookup fails. */
export class UserDirectory extends Context.Service<
	UserDirectory,
	{
		/** Hydrates one author through its provider directory. */
		readonly hydrateAuthor: (input: GetUserInput, author: Author) => Effect.Effect<Author>
		/** Hydrates a message author without changing message identity or content. */
		readonly hydrateMessage: (message: Message) => Effect.Effect<Message>
		/** Hydrates messages with one lookup per workspace-scoped author. */
		readonly hydrateMessages: (messages: ReadonlyArray<Message>) => Effect.Effect<ReadonlyArray<Message>>
		/** Hydrates a thread's current and recent messages. */
		readonly hydrateThread: (thread: Thread) => Effect.Effect<Thread>
		/** Hydrates one inbound message and its thread with shared lookup reuse. */
		readonly hydrateDelivery: (
			thread: Thread,
			message: Message,
		) => Effect.Effect<{ readonly thread: Thread; readonly message: Message }>
	}
>()('channels/UserDirectory') {
	/** Constructs the standard workspace-scoped provider profile directory. */
	static readonly layer = Layer.effect(
		UserDirectory,
		Effect.gen(function* () {
			const registry = yield* ProviderRegistry
			const profiles = yield* Cache.make({
				capacity: 10_000,
				timeToLive: '8 days',
				lookup: (input: UserDirectoryKey) =>
					registry.byName({ provider: input.provider }).pipe(
						Effect.flatMap((provider) => provider.getUser(input)),
						Effect.map(Option.some),
						Effect.catchTag('UserLookupFailed', (error) =>
							error.retryable
								? Effect.fail(error)
								: Effect.logWarning(
										'channels user profile lookup is not retryable; using provider id',
									).pipe(
										Effect.annotateLogs({
											provider: input.provider,
											tenant: input.tenant,
											user_id: input.userId,
										}),
										Effect.as(Option.none<UserProfile>()),
									),
						),
					),
			})

			const lookup = (input: GetUserInput): Effect.Effect<Option.Option<UserProfile>> =>
				Effect.gen(function* () {
					const key = new UserDirectoryKey(input)
					return yield* Cache.get(profiles, key).pipe(
						Effect.catchCause(() =>
							Effect.logWarning(
								'channels retryable user profile hydration failed; using provider id',
							).pipe(
								Effect.annotateLogs({
									provider: input.provider,
									tenant: input.tenant,
									user_id: input.userId,
								}),
								Effect.andThen(Cache.invalidate(profiles, key)),
								Effect.as(Option.none<UserProfile>()),
							),
						),
					)
				})

			const hydrateAuthor = (input: GetUserInput, author: Author) =>
				Effect.map(lookup(input), Option.match({ onNone: () => author, onSome: (profile) => profile.author }))

			const hydrateMessage = (message: Message) =>
				Effect.map(
					hydrateAuthor(
						{
							provider: message.threadRef.channel.provider,
							tenant: message.threadRef.channel.tenant,
							userId: message.author.userId,
						},
						message.author,
					),
					(author) => messageWithAuthor(message, author),
				)

			const hydrateMessages = (messages: ReadonlyArray<Message>) =>
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
						return Effect.map(hydrateAuthor(input, message.author), (author) => {
							authors.set(key, author)
							return messageWithAuthor(message, author)
						})
					})
				})

			const hydrateThread = (thread: Thread) =>
				Effect.gen(function* () {
					const recentMessages = yield* hydrateMessages(thread.recentMessages)
					const currentMessage =
						thread.currentMessage === undefined ? undefined : yield* hydrateMessage(thread.currentMessage)
					return currentMessage === undefined
						? ThreadSchema.make({ ref: thread.ref, recentMessages })
						: ThreadSchema.make({ ref: thread.ref, currentMessage, recentMessages })
				})

			const hydrateDelivery = (thread: Thread, message: Message) =>
				Effect.gen(function* () {
					const all = [message, ...thread.recentMessages]
					if (thread.currentMessage !== undefined) {
						all.push(thread.currentMessage)
					}
					const hydrated = yield* hydrateMessages(all)
					const hydratedMessage = hydrated.at(0) ?? message
					const recentMessages = hydrated.slice(1, 1 + thread.recentMessages.length)
					const currentMessage =
						thread.currentMessage === undefined ? undefined : hydrated.at(1 + thread.recentMessages.length)
					const hydratedThread =
						currentMessage === undefined
							? ThreadSchema.make({ ref: thread.ref, recentMessages })
							: ThreadSchema.make({ ref: thread.ref, currentMessage, recentMessages })
					return { thread: hydratedThread, message: hydratedMessage }
				})

			return UserDirectory.of({ hydrateAuthor, hydrateMessage, hydrateMessages, hydrateThread, hydrateDelivery })
		}),
	)
}
