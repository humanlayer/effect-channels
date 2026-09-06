import { Cache, Context, Data, Duration, Effect, Layer, Match, Option } from 'effect'

import type { Message } from './Message.ts'
import { Message as MessageSchema } from './Message.ts'
import type { Author, ProviderName, TenantId, UserId, UserProfile } from './Model.ts'
import type { GetUserInput, MessagePage, ThreadPage, ThreadSummary } from './Operations.ts'
import { Slack } from './Slack.ts'
import {
	UserProfileCache,
	UserProfileCacheKey,
	UserProfileFound,
	UserProfileUnavailable,
	type UserProfileCacheEntry,
} from './SlackUserProfileCache.ts'
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

export const hydrateHistoryMessage = (message: Message) =>
	Effect.flatMap(
		Effect.serviceOption(SlackUserDirectory),
		Option.match({
			onNone: () => Effect.succeed(message),
			onSome: (directory) => directory.hydrateMessage(message),
		}),
	)

export const hydrateHistoryPage = (page: MessagePage) =>
	Effect.map(Effect.forEach(page.messages, hydrateHistoryMessage), (messages) => ({ ...page, messages }))

export const hydrateThreadSummary = (summary: ThreadSummary) =>
	Effect.map(hydrateHistoryMessage(summary.rootMessage), (rootMessage) => ({ ...summary, rootMessage }))

export const hydrateThreadPage = (page: ThreadPage) =>
	Effect.map(Effect.forEach(page.threads, hydrateThreadSummary), (threads) => ({ ...page, threads }))

/** Resolves and caches provider user profiles while preserving ID-based authors when lookup fails. */
export class SlackUserDirectory extends Context.Service<
	SlackUserDirectory,
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
>()('slack/SlackUserDirectory') {
	/** Constructs the standard directory with a small local single-flight cache over the configured shared cache. */
	static make(options: { readonly localCapacity?: number; readonly localTimeToLive?: Duration.Input } = {}) {
		return Layer.effect(
			SlackUserDirectory,
			Effect.gen(function* () {
				const slack = yield* Slack
				const shared = yield* UserProfileCache

				const readShared = (key: UserProfileCacheKey) =>
					shared.get(key).pipe(
						Effect.catchTag('UserProfileCacheError', (error) =>
							Effect.logWarning('Slack shared user profile cache read failed', error).pipe(
								Effect.annotateLogs({
									provider: key.provider,
									tenant: key.tenant,
									user_id: key.userId,
								}),
								Effect.as(Option.none<UserProfileCacheEntry>()),
							),
						),
					)

				const writeShared = (key: UserProfileCacheKey, entry: UserProfileCacheEntry) =>
					shared.set(key, entry).pipe(
						Effect.catchTag('UserProfileCacheError', (error) =>
							Effect.logWarning('Slack shared user profile cache write failed', error).pipe(
								Effect.annotateLogs({
									provider: key.provider,
									tenant: key.tenant,
									user_id: key.userId,
								}),
							),
						),
					)

				const profiles = yield* Cache.make({
					capacity: options.localCapacity ?? 1_000,
					timeToLive: options.localTimeToLive ?? '5 minutes',
					lookup: (input: UserDirectoryKey) =>
						Effect.gen(function* () {
							const key = UserProfileCacheKey.make(input)
							const cached = yield* readShared(key)
							if (Option.isSome(cached)) {
								return cached.value
							}
							return yield* slack.getUser(input).pipe(
								Effect.map((profile) => UserProfileFound.make({ profile })),
								Effect.tap((entry) => writeShared(key, entry)),
								Effect.catchTag('UserLookupFailed', (error) => {
									if (error.retryable) {
										return Effect.fail(error)
									}
									const entry = UserProfileUnavailable.make({})
									return Effect.logWarning(
										'Slack user profile lookup is not retryable; using provider id',
										error,
									).pipe(
										Effect.annotateLogs({
											provider: input.provider,
											tenant: input.tenant,
											user_id: input.userId,
										}),
										Effect.andThen(writeShared(key, entry)),
										Effect.as(entry),
									)
								}),
							)
						}),
				})

				const lookup = (input: GetUserInput): Effect.Effect<Option.Option<UserProfile>> =>
					Effect.gen(function* () {
						const key = new UserDirectoryKey(input)
						return yield* Cache.get(profiles, key).pipe(
							Effect.map(
								Match.type<UserProfileCacheEntry>().pipe(
									Match.tagsExhaustive({
										UserProfileFound: ({ profile }) => Option.some(profile),
										UserProfileUnavailable: () => Option.none<UserProfile>(),
									}),
								),
							),
							Effect.catchTags({
								UnknownTenant: (error) =>
									Effect.logWarning(
										'Slack user profile installation is unavailable; using provider id',
										error,
									).pipe(
										Effect.andThen(Cache.invalidate(profiles, key)),
										Effect.as(Option.none<UserProfile>()),
									),
								UserLookupFailed: (error) =>
									Effect.logWarning(
										'Slack retryable user profile hydration failed; using provider id',
										error,
									).pipe(
										Effect.annotateLogs({
											provider: input.provider,
											tenant: input.tenant,
											user_id: input.userId,
										}),
										Effect.andThen(Cache.invalidate(profiles, key)),
										Effect.as(Option.none<UserProfile>()),
									),
							}),
						)
					}).pipe(Effect.withSpan('slack.directory.lookup'))

				const hydrateAuthor = (input: GetUserInput, author: Author) =>
					Effect.map(
						lookup(input),
						Option.match({ onNone: () => author, onSome: (profile) => profile.author }),
					)

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
							thread.currentMessage === undefined
								? undefined
								: yield* hydrateMessage(thread.currentMessage)
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
							thread.currentMessage === undefined
								? undefined
								: hydrated.at(1 + thread.recentMessages.length)
						const hydratedThread =
							currentMessage === undefined
								? ThreadSchema.make({ ref: thread.ref, recentMessages })
								: ThreadSchema.make({ ref: thread.ref, currentMessage, recentMessages })
						return { thread: hydratedThread, message: hydratedMessage }
					})

				return SlackUserDirectory.of({
					hydrateAuthor: Effect.fn('slack.directory.hydrate_author')(hydrateAuthor),
					hydrateMessage: Effect.fn('slack.directory.hydrate_message')(hydrateMessage),
					hydrateMessages: Effect.fn('slack.directory.hydrate_messages')(hydrateMessages),
					hydrateThread: Effect.fn('slack.directory.hydrate_thread')(hydrateThread),
					hydrateDelivery: Effect.fn('slack.directory.hydrate_delivery')(hydrateDelivery),
				})
			}),
		)
	}

	/** Compatibility Layer using the in-process profile cache. */
	static readonly layer = SlackUserDirectory.make().pipe(Layer.provide(UserProfileCache.layerMemory))
}
