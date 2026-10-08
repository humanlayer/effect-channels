/**
 * The Computer Durable Object: one `@cloudflare/computer` Workspace per AgentSession Durable Object, using
 * the same object name. The Workspace is a virtual filesystem in the object's SQLite, with git, and two places to run
 * commands on it:
 *
 * - `shell`: just-bash in a Worker the Worker Loader starts, which reads and writes this same filesystem.
 * - `container`: this object's Linux container (see `Dockerfile`), running `computerd`, which mounts a copy
 *   of the filesystem at `/workspace`. Each command first sends the container the files changed since the
 *   last one, then copies back what the command changed.
 *
 * A plain Durable Object rather than an Effect one, because `withWorkspace` must wrap the
 * `cloudflare:workers` class.
 */
import { type DurableObjectStorageLike, getWorkspace, withWorkspace } from '@cloudflare/computer'
import {
	CloudflareContainerBackend,
	type CloudflareContainerBackendOptions,
	type IWorkspaceContainerAPI,
	type WorkspaceContainerAPI,
	withWorkspaceContainer,
} from '@cloudflare/computer/backends/container'
import { WorkerShellBackend } from '@cloudflare/computer/backends/worker-shell'
import { createGitClient, type GitClient } from '@cloudflare/computer/git'
import jq from '@cloudflare/computer/shell/jq'
import { DurableObject } from 'cloudflare:workers'
import { Data, Effect, Option, Schema } from 'effect'

import {
	type Backend,
	type CommandInput,
	type CommandOutput,
	COMPUTER_BINDING,
	type ComputerResult,
	type FetchedBranch,
	type FileInfo,
	GIT_IDENTITY,
	type GitHeaders,
	type MergedRef,
	type PreparedRepo,
	type PrepareRepoInput,
	type PulledRepo,
	type PushedBranch,
	WORKSPACE_ROOT,
} from './Contract'

/**
 * Entrypoints the backends reach this object through, via `ctx.exports`: the shell calls back into the
 * workspace through WorkspaceServiceProxy, and the container dials its connection in through WorkspaceProxy.
 */
export { WorkspaceProxy, WorkspaceServiceProxy } from '@cloudflare/computer'

type Env = {
	/** Starts the shell's Worker. */
	readonly LOADER: WorkerLoader
}

/** How long `startContainer` waits for the no-op command, past the container's own 30s start budget. */
const CONTAINER_START_TIMEOUT_MILLIS = 10_000

/** How long `destroy` waits after answering before the object restarts. */
const RESTART_DELAY_MILLIS = 1_000

/**
 * The object's storage as `@cloudflare/computer` types it. Its `exec` promises whatever row type the caller
 * names, which a Durable Object's SQL cannot; the rows are the same at runtime.
 */
const workspaceStorage = (storage: DurableObjectStorage): DurableObjectStorageLike => {
	function exec<Row extends object>(query: string, ...bindings: Array<unknown>): { toArray(): Array<Row> }
	function exec(query: string, ...bindings: Array<unknown>): { toArray(): Array<object> } {
		return storage.sql.exec(query, ...bindings)
	}
	return {
		sql: { exec },
		transaction: (closure) => storage.transaction(async () => closure()),
		transactionSync: (closure) => storage.transactionSync(closure),
	}
}

/** What `withWorkspaceContainer` adds to the object. */
type ContainerOwner = {
	getWorkspaceContainer(): WorkspaceContainerAPI | IWorkspaceContainerAPI | Promise<IWorkspaceContainerAPI>
}

/**
 * The object as the container backend's host. `@cloudflare/computer` types the container API against its
 * own copy of the Workers types, whose `Fetcher` has more methods than ours; the object is the same.
 */
function containerHost(self: ContainerOwner): Awaited<ReturnType<CloudflareContainerBackendOptions['container']>>
function containerHost(self: ContainerOwner): ContainerOwner {
	return self
}

/** A thrown workspace error carries its POSIX-style code, like `ENOENT`. */
const ErrorCode = Schema.Struct({ code: Schema.String })
const errorCodeOf = (cause: unknown): string =>
	Option.match(Schema.decodeUnknownOption(ErrorCode)(cause), { onNone: () => 'UNKNOWN', onSome: ({ code }) => code })

class WorkspaceOperationError extends Data.TaggedError('WorkspaceOperationError')<{
	readonly code: string
	readonly message: string
}> {}

/** Run one file operation, returning a thrown workspace error as its code and message. */
const attempt = <A>(operation: () => Promise<A>): Promise<ComputerResult<A>> =>
	Effect.runPromise(
		Effect.tryPromise({
			try: operation,
			catch: (cause) => new WorkspaceOperationError({ code: errorCodeOf(cause), message: String(cause) }),
		}).pipe(
			Effect.match({
				onSuccess: (value): ComputerResult<A> => ({ ok: true, value }),
				onFailure: ({ code, message }): ComputerResult<A> => ({ ok: false, code, message }),
			}),
		),
	)

/** Replace each header value in `text`, so a credential in an error message goes no further. */
const scrub = (text: string, headers: GitHeaders) =>
	Object.values(headers).reduce((scrubbed, value) => scrubbed.replaceAll(value, '[redacted]'), text)

/** {@link attempt} for a git network operation, scrubbing its headers from any failure. */
const attemptGit = async <A>(headers: GitHeaders, operation: () => Promise<A>): Promise<ComputerResult<A>> => {
	const result = await attempt(operation)
	return result.ok ? result : { ...result, message: scrub(result.message, headers) }
}

const repoDir = (name: string) => `${WORKSPACE_ROOT}/${name}`

/** The remote has no such ref: isomorphic-git's `NotFoundError`, wrapped in the git client's error. */
const RefNotFound = Schema.Struct({ cause: Schema.Struct({ code: Schema.Literal('NotFoundError') }) })
const isRefNotFound = Schema.is(RefNotFound)

const currentBranchOf = async (git: GitClient, dir: string) => {
	const branch = await git.currentBranch({ dir })
	if (branch === undefined) throw new Error(`${dir} has no branch checked out`)
	return branch
}

/** Make `git pull` on `branch` pull the remote branch of the same name. */
const trackOrigin = async (git: GitClient, dir: string, branch: string) => {
	await git.configSet({ dir, path: `branch.${branch}.remote`, value: 'origin' })
	await git.configSet({ dir, path: `branch.${branch}.merge`, value: `refs/heads/${branch}` })
}

const pushToOrigin = async (git: GitClient, dir: string, branch: string, headers: Record<string, string>) => {
	const result = await git.push({ dir, remote: 'origin', ref: branch, remoteRef: branch, headers })
	if (!result.ok) throw new Error(`git push of ${branch} was rejected: ${result.error ?? 'no reason given'}`)
}

/** The workspace file methods `prepareRepo` uses. */
type RepoFiles = {
	readonly mkdir: (path: string, options: { readonly recursive: boolean }) => Promise<void>
	readonly rm: (path: string, options: { readonly recursive: boolean; readonly force: boolean }) => Promise<void>
}

/** Clone the repo and check out its work branch; see `Computer.prepare`. */
const prepareRepo = async (git: GitClient, fs: RepoFiles, input: PrepareRepoInput): Promise<PreparedRepo> => {
	const dir = repoDir(input.name)
	const headers = { ...input.headers }
	await fs.mkdir(WORKSPACE_ROOT, { recursive: true })
	await fs.rm(dir, { recursive: true, force: true })
	await git.clone({ url: input.url, dir, headers })
	const defaultBranch = await currentBranchOf(git, dir)
	await git.configSet({ dir, path: 'user.name', value: GIT_IDENTITY.name })
	await git.configSet({ dir, path: 'user.email', value: GIT_IDENTITY.email })

	let createdBranch = false
	if (input.branch !== null && input.branch !== defaultBranch) {
		const remoteHasBranch = await git
			.fetch({ dir, remote: 'origin', ref: input.branch, singleBranch: true, headers })
			.then(
				() => true,
				(cause: unknown) => (isRefNotFound(cause) ? false : Promise.reject(cause)),
			)
		if (remoteHasBranch) {
			await git.branch({ dir, name: input.branch, startPoint: `refs/remotes/origin/${input.branch}` })
			await git.checkout({ dir, ref: input.branch, force: true })
		} else {
			await git.branch({ dir, name: input.branch, checkout: true })
			await pushToOrigin(git, dir, input.branch, headers)
			createdBranch = true
		}
		await trackOrigin(git, dir, input.branch)
	}

	return {
		dir,
		branch: input.branch ?? defaultBranch,
		defaultBranch,
		commit: await git.revParse({ dir, ref: 'HEAD' }),
		createdBranch,
	}
}

/** Abort the object so it restarts empty. abort throws to unwind; the object resets either way. */
const abortQuietly = (abort: () => void) => Effect.runSync(Effect.ignore(Effect.try(abort)))

class ComputerBase extends withWorkspaceContainer(class extends DurableObject<Env> {}) {
	/**
	 * For withWorkspace's options, which see the instance but not `ctx` or `env`: DurableObject keeps them
	 * protected.
	 */
	readonly storage = workspaceStorage(this.ctx.storage)
	readonly shell = new WorkerShellBackend({
		id: 'shell' satisfies Backend,
		loader: this.env.LOADER,
		workspace: { binding: COMPUTER_BINDING, id: this.ctx.id.toString() },
		ctx: this.ctx,
		commands: [jq],
	})
	readonly container = new CloudflareContainerBackend({
		id: 'container' satisfies Backend,
		container: () => containerHost(this),
		workspace: { binding: COMPUTER_BINDING, id: this.ctx.id.toString() },
		/** Commands may install packages. */
		egress: { mode: 'direct' },
	})
}

export class Computer extends withWorkspace(ComputerBase, (self) => ({
	storage: self.storage,
	git: createGitClient(),
	backends: [self.shell, self.container],
})) {
	/** The container's connection, which `computerd` dials in through WorkspaceProxy. */
	override async fetch(request: Request): Promise<Response> {
		return await this.container.handleFetch(request)
	}

	/**
	 * Start the container and connect to it, so the first container command doesn't wait for it. Runs a
	 * no-op command: the workspace connects a backend on its first command.
	 */
	async startContainer(): Promise<ComputerResult<null>> {
		return await this.exec({
			backend: 'container',
			command: 'true',
			cwd: '/',
			timeoutMs: CONTAINER_START_TIMEOUT_MILLIS,
		}).then((result) => (result.ok ? { ok: true, value: null } : result))
	}

	/**
	 * Clone the repo into `/workspace/<name>`, replacing whatever a cut-off earlier attempt left there, and
	 * check out the branch to work on. A branch the remote has is checked out from it; a new one is made
	 * from the default branch and pushed, so the remote has it from the start. Rejects with what failed.
	 */
	async prepare(input: PrepareRepoInput): Promise<PreparedRepo> {
		using workspace = await getWorkspace(this)
		return await prepareRepo(workspace.git, workspace.fs, input).catch((cause: unknown) => {
			throw new Error(scrub(`Preparing ${input.name} from ${input.url} failed: ${String(cause)}`, input.headers))
		})
	}

	/**
	 * Fast-forward the current branch of `/workspace/<name>` to its remote branch, so the agent sees commits
	 * pushed since it was cloned. Fails rather than merge, such as when local commits or edits are in the way.
	 */
	async pull(input: { readonly name: string; readonly headers: GitHeaders }): Promise<ComputerResult<PulledRepo>> {
		using workspace = await getWorkspace(this)
		const git: GitClient = workspace.git
		const dir = repoDir(input.name)
		return await attemptGit(input.headers, async () => {
			const before = await git.revParse({ dir, ref: 'HEAD' })
			await git.pull({ dir, fastForwardOnly: true, headers: { ...input.headers } })
			return { before, after: await git.revParse({ dir, ref: 'HEAD' }) }
		})
	}

	/** Fetch the remote's `branch` into `origin/<branch>`, leaving the working tree alone. */
	async fetchBranch(input: {
		readonly name: string
		readonly branch: string
		readonly headers: GitHeaders
	}): Promise<ComputerResult<FetchedBranch>> {
		using workspace = await getWorkspace(this)
		const git: GitClient = workspace.git
		const dir = repoDir(input.name)
		return await attemptGit(input.headers, async () => {
			await git.fetch({
				dir,
				remote: 'origin',
				ref: input.branch,
				singleBranch: true,
				headers: { ...input.headers },
			})
			return {
				branch: input.branch,
				commit: await git.revParse({ dir, ref: `refs/remotes/origin/${input.branch}` }),
			}
		})
	}

	/** Push `branch` to the remote branch of the same name. Fails unless `branch` is checked out. */
	async pushBranch(input: {
		readonly name: string
		readonly branch: string
		readonly headers: GitHeaders
	}): Promise<ComputerResult<PushedBranch>> {
		using workspace = await getWorkspace(this)
		const git: GitClient = workspace.git
		const dir = repoDir(input.name)
		return await attemptGit(input.headers, async () => {
			const current = await git.currentBranch({ dir })
			if (current !== input.branch) {
				throw new Error(`The checked-out branch is ${current ?? 'none (detached HEAD)'}, not ${input.branch}.`)
			}
			await pushToOrigin(git, dir, input.branch, { ...input.headers })
			return { branch: input.branch, commit: await git.revParse({ dir, ref: 'HEAD' }) }
		})
	}

	/**
	 * Merge `ref`, such as `origin/main`, into the current branch, fast-forwarding when it can, and update the
	 * working tree to the result. Refuses while tracked files have uncommitted changes. A conflict aborts the
	 * merge and leaves everything as it was.
	 */
	async mergeRef(input: { readonly name: string; readonly ref: string }): Promise<ComputerResult<MergedRef>> {
		using workspace = await getWorkspace(this)
		const git: GitClient = workspace.git
		const dir = repoDir(input.name)
		return await attempt(async () => {
			const changed = (await git.status({ dir })).filter((entry) => entry.worktree !== '?')
			if (changed.length > 0) {
				throw new Error(
					`Commit or discard the changes to ${changed.map((entry) => entry.path).join(', ')} before merging.`,
				)
			}
			const branch = await currentBranchOf(git, dir)
			const result = await git.merge({ dir, theirs: input.ref })
			await git.checkout({ dir, ref: branch, force: true })
			return {
				commit: await git.revParse({ dir, ref: 'HEAD' }),
				fastForward: result.fastForward === true,
				alreadyMerged: result.alreadyMerged === true,
			}
		})
	}

	/**
	 * The file methods fold's read, write, edit and apply_patch tools and its skill loader need. Paths are
	 * absolute.
	 */

	async readFile(path: string): Promise<ComputerResult<Uint8Array>> {
		using workspace = await getWorkspace(this)
		return await attempt(async () => new Response(await workspace.fs.readFile(path)).bytes())
	}

	async writeFile(path: string, content: string): Promise<ComputerResult<null>> {
		using workspace = await getWorkspace(this)
		return await attempt(async () => {
			await workspace.fs.writeFile(path, content)
			return null
		})
	}

	async mkdir(path: string, recursive: boolean): Promise<ComputerResult<null>> {
		using workspace = await getWorkspace(this)
		return await attempt(async () => {
			await workspace.fs.mkdir(path, { recursive })
			return null
		})
	}

	async rm(path: string, recursive: boolean, force: boolean): Promise<ComputerResult<null>> {
		using workspace = await getWorkspace(this)
		return await attempt(async () => {
			await workspace.fs.rm(path, { recursive, force })
			return null
		})
	}

	/** Follows symlinks. */
	async stat(path: string): Promise<ComputerResult<FileInfo>> {
		using workspace = await getWorkspace(this)
		return await attempt(async () => {
			const stat = await workspace.fs.stat(path)
			const type = stat.isDirectory ? 'Directory' : stat.isSymbolicLink ? 'SymbolicLink' : 'File'
			return { type, size: stat.size, mtime: stat.mtime, mode: stat.mode, inode: stat.inode }
		})
	}

	/**
	 * Delete this workspace at `deleteAt` (epoch milliseconds) unless moved again before then. Its session
	 * normally deletes it first; this is the backup for one that never does, such as a failed first clone.
	 */
	async expireAt(deleteAt: number): Promise<void> {
		await this.ctx.storage.setAlarm(deleteAt)
	}

	/** Delete the workspace: the container, files, git data, and the backup alarm. The object then restarts empty. */
	async destroy(): Promise<void> {
		if (this.ctx.container?.running === true) await this.ctx.container.destroy('workspace deleted')
		await this.ctx.storage.deleteAlarm()
		await this.ctx.storage.deleteAll()
		/**
		 * The in-memory workspace must go: its tables are gone. Wait until this call has answered: Cloudflare
		 * holds the answer until the delete is saved, and an abort before then fails the call.
		 */
		setTimeout(() => abortQuietly(() => this.ctx.abort('workspace deleted')), RESTART_DELAY_MILLIS)
	}

	/** Only `expireAt` sets the alarm, and each call replaces it, so it fires at the latest deadline. */
	override async alarm(): Promise<void> {
		await this.destroy()
	}

	/** Run one command. A command that fails still succeeds here, with its exit code. */
	async exec(input: CommandInput): Promise<ComputerResult<CommandOutput>> {
		using workspace = await getWorkspace(this)
		return await attempt(async () => {
			using handle = await workspace.runtime.exec(input.command, {
				backend: input.backend,
				cwd: input.cwd,
				encoding: 'utf8',
				timeoutMs: input.timeoutMs,
			})
			const { status, exitCode, stdout, stderr } = await handle.result()
			return { status, exitCode, stdout, stderr }
		})
	}

	/** The directory's entry names. */
	async readdir(path: string): Promise<ComputerResult<ReadonlyArray<string>>> {
		using workspace = await getWorkspace(this)
		return await attempt(async () => (await workspace.fs.readdir(path)).map((entry) => entry.name))
	}
}

export default {
	fetch: () => new Response('Not found', { status: 404 }),
} satisfies ExportedHandler
