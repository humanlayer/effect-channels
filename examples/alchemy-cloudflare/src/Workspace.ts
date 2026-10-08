import { type GitHubApiError, GitHubGitCredentials, type GitHubRepositoryRef } from '@humanlayer/channels-github'
/**
 * Each agent session's workspace: the Computer Durable Object (see `computer/`) with the same name as
 * its AgentSession Durable Object. The discussion's repository clones into `/workspace/<name>` when the
 * session starts. Git reaches GitHub with the GitHub App's credentials, which the agent never sees.
 */
import type { RpcCallError } from 'alchemy'
import * as Cloudflare from 'alchemy/Cloudflare'
import { Clock, Context, Data, Effect, FileSystem, Layer, Redacted, Schema } from 'effect'

import {
	COMPUTER_WORKER_NAME,
	type CommandInput,
	type CommandOutput,
	type ComputerResult,
	type FetchedBranch,
	type GitHeaders,
	type MergedRef,
	type PreparedRepo,
	type PrepareRepoInput,
	type PulledRepo,
	type PushedBranch,
} from './computer/Contract'
import { type ComputerFiles, workspaceFileSystem } from './WorkspaceFileSystem'

/** A GitHub repository the session works on, cloned into `/workspace/<repository name>`. */
type RepositoryInput = { readonly repository: GitHubRepositoryRef }

/** Where the repository clones into, under `/workspace`. */
export const repositoryDirectoryName = (repository: GitHubRepositoryRef) => repository.repository

const repositoryUrl = (repository: GitHubRepositoryRef) =>
	`https://github.com/${repository.owner}/${repository.repository}.git`

/** A repo failed to clone, so the session did not start. */
export class RepoCloneError extends Schema.TaggedError<RepoCloneError>()('RepoCloneError', {
	message: Schema.String,
}) {}

/** A cloned repo could not be fast-forwarded: unreachable remote, or local changes in the way. */
export class RepoPullError extends Data.TaggedError('RepoPullError')<{ readonly message: string }> {}

/** Fetching from or pushing to the repository's remote failed. */
export class GitRemoteError extends Data.TaggedError('GitRemoteError')<{ readonly message: string }> {}

/** A merge failed: uncommitted changes, a conflict, or an unknown ref. Nothing changed. */
export class GitMergeError extends Data.TaggedError('GitMergeError')<{ readonly message: string }> {}

/** The shell could not run a command at all. A command that runs and fails is a {@link CommandOutput}. */
export class ShellError extends Data.TaggedError('ShellError')<{ readonly message: string }> {}

type ComputerMethods = ComputerFiles & {
	readonly prepare: (input: PrepareRepoInput) => Effect.Effect<PreparedRepo, RpcCallError>
	readonly pull: (input: {
		readonly name: string
		readonly headers: GitHeaders
	}) => Effect.Effect<ComputerResult<PulledRepo>, RpcCallError>
	readonly fetchBranch: (input: {
		readonly name: string
		readonly branch: string
		readonly headers: GitHeaders
	}) => Effect.Effect<ComputerResult<FetchedBranch>, RpcCallError>
	readonly pushBranch: (input: {
		readonly name: string
		readonly branch: string
		readonly headers: GitHeaders
	}) => Effect.Effect<ComputerResult<PushedBranch>, RpcCallError>
	readonly mergeRef: (input: {
		readonly name: string
		readonly ref: string
	}) => Effect.Effect<ComputerResult<MergedRef>, RpcCallError>
	readonly exec: (input: CommandInput) => Effect.Effect<ComputerResult<CommandOutput>, RpcCallError>
	readonly startContainer: () => Effect.Effect<ComputerResult<null>, RpcCallError>
	readonly expireAt: (deleteAt: number) => Effect.Effect<void, RpcCallError>
	readonly destroy: () => Effect.Effect<void, RpcCallError>
}

/** The Computer Durable Object namespace. Its class lives in the Computer Worker. */
export class Computer extends Cloudflare.DurableObject<Computer, ComputerMethods>()('Computer') {}

export class Workspace extends Context.Service<
	Workspace,
	{
		/**
		 * Clone the repository fresh, replacing any clone a cut-off attempt left behind, and check out `branch`:
		 * the remote's when it has one, otherwise a new one from the default branch, pushed at once. `null`
		 * stays on the default branch.
		 */
		readonly prepare: (
			input: RepositoryInput & { readonly branch: string | null },
		) => Effect.Effect<PreparedRepo, RepoCloneError>
		/** Fast-forward the repository's checked-out branch to its remote branch. */
		readonly pull: (input: RepositoryInput) => Effect.Effect<PulledRepo, RepoPullError>
		/** Fetch the remote's `branch` into `origin/<branch>`. */
		readonly fetchBranch: (
			input: RepositoryInput & { readonly branch: string },
		) => Effect.Effect<FetchedBranch, GitRemoteError>
		/** Push `branch`, which must be checked out, to the remote branch of the same name. */
		readonly pushBranch: (
			input: RepositoryInput & { readonly branch: string },
		) => Effect.Effect<PushedBranch, GitRemoteError>
		/** Merge `ref` into the checked-out branch. */
		readonly mergeRef: (
			input: RepositoryInput & { readonly ref: string },
		) => Effect.Effect<MergedRef, GitMergeError>
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
	/**
	 * The workspace of this AgentSession object: the Computer with the object's name. Alchemy also runs the
	 * object's setup at deploy time, with no real object and so no name, so each call looks the Computer up.
	 * Git reaches GitHub as the GitHub App.
	 */
	static readonly layer = Layer.effect(
		Workspace,
		Effect.gen(function* () {
			const computers = yield* Computer.from(COMPUTER_WORKER_NAME)
			const state = yield* Cloudflare.DurableObjectState
			const credentials = yield* GitHubGitCredentials

			/** The session's Computer, looked up by this object's name when used. */
			const computer = Effect.suspend(() =>
				Schema.decodeUnknownEffect(Schema.NonEmptyString)(state.id.name),
			).pipe(
				Effect.orDie,
				Effect.map((name) => computers.getByName(name)),
			)

			/**
			 * The request headers that let git reach the repository as the GitHub App. They go to the Computer
			 * with each network operation and nowhere else.
			 */
			const gitHeaders = (repository: GitHubRepositoryRef) =>
				credentials.authorization({ repository }).pipe(
					Effect.map((authorization): GitHeaders => ({ Authorization: Redacted.value(authorization) })),
					Effect.tapError((error) => Effect.logError('workspace.git_credentials failed', error)),
				)

			return Workspace.of({
				prepare: Effect.fn('alchemy_cloudflare.workspace.prepare')(function* ({ repository, branch }) {
					const toError = (message: string) => new RepoCloneError({ message })
					const headers = yield* gitHeaders(repository).pipe(
						Effect.mapError((error) => toError(accessDenied(repository, error))),
					)
					const prepared = yield* Effect.flatMap(computer, (client) =>
						client.prepare({
							name: repositoryDirectoryName(repository),
							url: repositoryUrl(repository),
							branch,
							headers,
						}),
					).pipe(
						Effect.mapError((error) => toError(error.message)),
						Effect.tapError((error) => Effect.logError('workspace.prepare failed', error)),
					)
					yield* Effect.logInfo('workspace.prepare').pipe(Effect.annotateLogs({ prepared }))
					return prepared
				}),
				pull: Effect.fn('alchemy_cloudflare.workspace.pull')(function* ({ repository }) {
					const toError = (message: string) => new RepoPullError({ message })
					const headers = yield* gitHeaders(repository).pipe(
						Effect.mapError((error) => toError(accessDenied(repository, error))),
					)
					const name = repositoryDirectoryName(repository)
					const pulled = yield* computerResult(
						Effect.flatMap(computer, (client) => client.pull({ name, headers })),
						toError,
					)
					yield* Effect.logInfo('workspace.pull').pipe(Effect.annotateLogs({ repo: name, pulled }))
					return pulled
				}),
				fetchBranch: Effect.fn('alchemy_cloudflare.workspace.fetch_branch')(function* ({ repository, branch }) {
					const toError = (message: string) => new GitRemoteError({ message })
					const headers = yield* gitHeaders(repository).pipe(
						Effect.mapError((error) => toError(accessDenied(repository, error))),
					)
					const name = repositoryDirectoryName(repository)
					const fetched = yield* computerResult(
						Effect.flatMap(computer, (client) => client.fetchBranch({ name, branch, headers })),
						toError,
					)
					yield* Effect.logInfo('workspace.fetch_branch').pipe(Effect.annotateLogs({ repo: name, fetched }))
					return fetched
				}),
				pushBranch: Effect.fn('alchemy_cloudflare.workspace.push_branch')(function* ({ repository, branch }) {
					const toError = (message: string) => new GitRemoteError({ message })
					const headers = yield* gitHeaders(repository).pipe(
						Effect.mapError((error) => toError(accessDenied(repository, error))),
					)
					const name = repositoryDirectoryName(repository)
					const pushed = yield* computerResult(
						Effect.flatMap(computer, (client) => client.pushBranch({ name, branch, headers })),
						toError,
					)
					yield* Effect.logInfo('workspace.push_branch').pipe(Effect.annotateLogs({ repo: name, pushed }))
					return pushed
				}),
				mergeRef: Effect.fn('alchemy_cloudflare.workspace.merge_ref')(function* ({ repository, ref }) {
					const name = repositoryDirectoryName(repository)
					const merged = yield* computerResult(
						Effect.flatMap(computer, (client) => client.mergeRef({ name, ref })),
						(message) => new GitMergeError({ message }),
					)
					yield* Effect.logInfo('workspace.merge_ref').pipe(Effect.annotateLogs({ repo: name, ref, merged }))
					return merged
				}),
				fileSystem: workspaceFileSystem(computer),
				expireAt: (deleteAt) =>
					Effect.flatMap(computer, (client) => client.expireAt(deleteAt)).pipe(
						Effect.catch((error) =>
							Effect.logWarning('workspace.expireAt failed').pipe(
								Effect.annotateLogs({ error: error.message }),
							),
						),
					),
				destroy: Effect.flatMap(computer, (client) => client.destroy()).pipe(
					Effect.tap(() => Effect.logInfo('workspace.destroyed')),
					Effect.catch((error) =>
						Effect.logWarning('workspace.destroy failed').pipe(
							Effect.annotateLogs({ error: error.message }),
						),
					),
				),
				startContainer: Effect.gen(function* () {
					const started = yield* Clock.currentTimeMillis
					const result = yield* Effect.flatMap(computer, (client) => client.startContainer()).pipe(
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
					const result = yield* Effect.flatMap(computer, (client) => client.exec(input)).pipe(
						Effect.mapError((error) => new ShellError({ message: error.message })),
					)
					yield* Effect.logInfo('workspace.exec').pipe(
						Effect.annotateLogs({
							backend: input.backend,
							command: input.command,
							cwd: input.cwd,
							outcome: result.ok
								? { status: result.value.status, exitCode: result.value.exitCode }
								: result,
						}),
					)
					return result.ok ? result.value : yield* new ShellError({ message: result.message })
				}),
			})
		}),
	)

	/** The workspace's files as this object's `FileSystem`, for fold's file tools and skill loader. */
	static readonly fileSystemLayer = Layer.effect(
		FileSystem.FileSystem,
		Effect.gen(function* () {
			return (yield* Workspace).fileSystem
		}),
	)
}

/** Why git could not reach the repository: GitHub would not give the App a token for it. */
const accessDenied = (repository: GitHubRepositoryRef, error: GitHubApiError) =>
	`GitHub did not grant access to ${repository.owner}/${repository.repository} (${error.reason}).`

/** A Computer method's outcome as an Effect, failing with its message. */
const computerResult = <A, E>(
	call: Effect.Effect<ComputerResult<A>, RpcCallError>,
	onFailure: (message: string) => E,
) =>
	call.pipe(
		Effect.mapError((error) => onFailure(error.message)),
		Effect.flatMap((result) => (result.ok ? Effect.succeed(result.value) : Effect.fail(onFailure(result.message)))),
		Effect.tapError((error) => Effect.logWarning('workspace.git failed', error)),
	)
