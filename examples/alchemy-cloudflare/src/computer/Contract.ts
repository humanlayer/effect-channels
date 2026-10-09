/**
 * What both sides of the Computer boundary share: the Computer Worker's script name, where repos live, and
 * the values its RPC methods take and return. Plain values only, so the Computer bundle stays free of Effect.
 */

/**
 * The Computer Worker's script name. Fixed, so ChatWorker can bind the Computer Durable Object across
 * scripts; it also means one deploy of this stack per Cloudflare account.
 */
export const COMPUTER_WORKER_NAME = 'humanlayer-channels-cloudflare-computer'

/** Every repo clones into `${WORKSPACE_ROOT}/<name>`. */
export const WORKSPACE_ROOT = '/workspace'

/** The Computer's binding name on its own Worker, which the shell uses to call back into it. */
export const COMPUTER_BINDING = 'Computer'

/**
 * Request headers for one git network operation, such as a private repository's `Authorization`. Sent with
 * that request only: never written to the remote's URL, `.git/config`, or a log.
 */
export type GitHeaders = Readonly<Record<string, string>>

/** Who the agent's commits are by. Written to each repo's own git config. */
export const GIT_IDENTITY = { name: 'HumanLayer Agent', email: 'agent@humanlayer.dev' } as const

export type PrepareRepoInput = {
	/** The directory under `/workspace`. */
	readonly name: string
	/** An https URL with no credentials. */
	readonly url: string
	/**
	 * The branch to work on: checked out from the remote when it is there, otherwise created from the
	 * default branch and pushed. `null` stays on the default branch.
	 */
	readonly branch: string | null
	readonly headers: GitHeaders
}

export type PreparedRepo = {
	readonly dir: string
	/** The branch checked out. */
	readonly branch: string
	/** The remote's default branch. */
	readonly defaultBranch: string
	/** The commit checked out. */
	readonly commit: string
	/** Whether `branch` was new, so this created it and pushed it. */
	readonly createdBranch: boolean
}

/** A pulled repo's commit before and after the pull; the same commit when nothing came in. */
export type PulledRepo = {
	readonly before: string
	readonly after: string
}

/** A remote branch fetched into `origin/<branch>`. */
export type FetchedBranch = {
	readonly branch: string
	readonly commit: string
}

/** A branch pushed to the remote branch of the same name. */
export type PushedBranch = {
	readonly branch: string
	readonly commit: string
}

/** A ref merged into the current branch. */
export type MergedRef = {
	readonly commit: string
	readonly fastForward: boolean
	readonly alreadyMerged: boolean
}

/**
 * A file or command method's outcome. Failures come back as values, not thrown: an error thrown across RPC
 * keeps its message but loses its `code` (`ENOENT`, `EISDIR`, ...), which the file tools need.
 */
export type ComputerResult<A> =
	| { readonly ok: true; readonly value: A }
	| { readonly ok: false; readonly code: string; readonly message: string }

export type FileInfo = {
	readonly type: 'File' | 'Directory' | 'SymbolicLink'
	readonly size: number
	/** Milliseconds since the epoch. */
	readonly mtime: number
	readonly mode: number
	readonly inode: number
}

/**
 * Where a command runs: `shell` is just-bash in a Worker, fast but with only text commands and git;
 * `container` is a Linux container with node, bun, python and internet access, slow to start.
 */
export type Backend = 'shell' | 'container'

/** One command, run on `backend` in `cwd` and stopped after `timeoutMs`. */
export type CommandInput = {
	readonly backend: Backend
	readonly command: string
	readonly cwd: string
	readonly timeoutMs: number
}

export type CommandOutput = {
	/** `cancelled` when the command hit its timeout. */
	readonly status: 'completed' | 'failed' | 'cancelled'
	readonly exitCode: number
	readonly stdout: string
	readonly stderr: string
}
