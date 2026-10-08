/**
 * Each agent session's workspace: the Computer Durable Object (see `computer/`) with the same name as
 * its AgentSession Durable Object. Repos clone into `/workspace/<name>` when the session starts.
 */
import type { RpcCallError } from 'alchemy'
import * as Cloudflare from 'alchemy/Cloudflare'
import { Clock, Context, Data, Effect, FileSystem, Schema } from 'effect'

import {
	type ClonedRepo,
	type CommandInput,
	type CommandOutput,
	type ComputerResult,
	type RepoSpec,
} from './computer/Contract'
import { type ComputerFiles, workspaceFileSystem } from './WorkspaceFileSystem'

const REPO_NAME = /^(?!\.{1,2}$)[\w.-]+$/

/**
 * One repo to clone: an https URL, a `ref` (the remote's default branch when absent), and the directory
 * `name` under `/workspace` (the URL's last path segment when absent).
 */
export const Repo = Schema.Struct({
	url: Schema.String.check(Schema.isStartingWith('https://')),
	name: Schema.optionalKey(Schema.String),
	ref: Schema.optionalKey(Schema.String),
})
export type Repo = typeof Repo.Type

/** The directory a repo clones into, under `/workspace`. */
export const repoName = (repo: Repo) =>
	repo.name ?? (repo.url.replace(/\/+$/, '').split('/').at(-1) ?? '').replace(/\.git$/, '')

/** A session's repos: every directory name valid and distinct. */
export const Repos = Schema.Array(Repo).check(
	Schema.makeFilter((repos) => {
		const names = repos.map(repoName)
		const invalid = names.find((name) => !REPO_NAME.test(name))
		if (invalid !== undefined) return `repo directory name "${invalid}" is invalid; pass a "name"`
		return new Set(names).size === names.length || 'repo directory names must be distinct'
	}),
)

/** A repo failed to clone, so the session did not start. Crosses the ChatSession RPC boundary encoded. */
export class RepoCloneError extends Schema.TaggedError<RepoCloneError>()('RepoCloneError', {
	message: Schema.String,
}) {}

/** The shell could not run a command at all. A command that runs and fails is a {@link CommandOutput}. */
export class ShellError extends Data.TaggedError('ShellError')<{ readonly message: string }> {}

type ComputerMethods = ComputerFiles & {
	readonly prepare: (repos: ReadonlyArray<RepoSpec>) => Effect.Effect<ReadonlyArray<ClonedRepo>, RpcCallError>
	readonly exec: (input: CommandInput) => Effect.Effect<ComputerResult<CommandOutput>, RpcCallError>
	readonly startContainer: () => Effect.Effect<ComputerResult<null>, RpcCallError>
	readonly expireAt: (deleteAt: number) => Effect.Effect<void, RpcCallError>
	readonly destroy: () => Effect.Effect<void, RpcCallError>
}

/** The Computer Durable Object namespace. Its class lives in the Computer Worker. */
export class Computer extends Cloudflare.DurableObject<Computer, ComputerMethods>()('Computer') {}

type ComputerClient = {
	readonly [K in keyof ComputerMethods]: ComputerMethods[K]
}

export class Workspace extends Context.Service<
	Workspace,
	{
		/** Clone the session's repos fresh, replacing any clone a cut-off attempt left behind. */
		readonly prepare: (repos: ReadonlyArray<Repo>) => Effect.Effect<ReadonlyArray<ClonedRepo>, RepoCloneError>
		/** The session's workspace as a `FileSystem`, for fold's file tools and skill loader. */
		readonly fileSystem: FileSystem.FileSystem
		/** Run one command in the session's shell or container. */
		readonly exec: (input: CommandInput) => Effect.Effect<CommandOutput, ShellError>
		/**
		 * Start the session's container, so its first command doesn't wait for it. A failure is logged, not
		 * raised: the first container command starts it again.
		 */
		readonly startContainer: Effect.Effect<void>
		/**
		 * Have the workspace delete itself at `deleteAt` (epoch milliseconds) unless moved again: the backup
		 * for a session that never deletes it. A failure is logged, not raised.
		 */
		readonly expireAt: (deleteAt: number) => Effect.Effect<void>
		/** Delete the workspace now. A failure is logged, not raised: the workspace's own deadline still holds. */
		readonly destroy: Effect.Effect<void>
	}
>()('alchemy-cloudflare/Workspace') {
	/** Bind this service to the one Computer selected by the AgentSession Durable Object root. */
	static readonly make = (computer: ComputerClient) =>
		Workspace.of({
			prepare: Effect.fn('alchemy_cloudflare.workspace.prepare')((repos) =>
				computer
					.prepare(repos.map((repo) => ({ name: repoName(repo), url: repo.url, ref: repo.ref ?? null })))
					.pipe(
						Effect.tap((cloned) =>
							Effect.logInfo('workspace.prepare').pipe(Effect.annotateLogs({ cloned })),
						),
						Effect.mapError((error) => new RepoCloneError({ message: error.message })),
					),
			),
			fileSystem: workspaceFileSystem(computer),
			expireAt: (deleteAt) =>
				computer
					.expireAt(deleteAt)
					.pipe(
						Effect.catch((error) =>
							Effect.logWarning('workspace.expireAt failed').pipe(
								Effect.annotateLogs({ error: error.message }),
							),
						),
					),
			destroy: computer.destroy().pipe(
				Effect.tap(() => Effect.logInfo('workspace.destroyed')),
				Effect.catch((error) =>
					Effect.logWarning('workspace.destroy failed').pipe(Effect.annotateLogs({ error: error.message })),
				),
			),
			startContainer: Effect.gen(function* () {
				const started = yield* Clock.currentTimeMillis
				const result = yield* computer
					.startContainer()
					.pipe(
						Effect.catch((error) =>
							Effect.succeed({ ok: false as const, code: 'RPC', message: error.message }),
						),
					)
				const details = { millis: (yield* Clock.currentTimeMillis) - started }
				yield* result.ok
					? Effect.logInfo('workspace.container.started').pipe(Effect.annotateLogs(details))
					: Effect.logWarning('workspace.container.start failed').pipe(
							Effect.annotateLogs({ ...details, error: result.message }),
						)
			}).pipe(Effect.withSpan('alchemy_cloudflare.workspace.startContainer')),
			exec: Effect.fn('alchemy_cloudflare.workspace.exec')(function* (input) {
				const result = yield* computer
					.exec(input)
					.pipe(Effect.mapError((error) => new ShellError({ message: error.message })))
				yield* Effect.logInfo('workspace.exec').pipe(
					Effect.annotateLogs({
						backend: input.backend,
						command: input.command,
						cwd: input.cwd,
						outcome: result.ok ? { status: result.value.status, exitCode: result.value.exitCode } : result,
					}),
				)
				return result.ok ? result.value : yield* new ShellError({ message: result.message })
			}),
		})
}
