/**
 * The AgentSession's Fold conversation: the service that opens its Fold session over the session's Workspace
 * and Fold log, pulls the repository, and reads the discussion, with the agent's prompt and tools.
 */
import { GitHubApi, GitHubId } from '@humanlayer/channels-github'
import { DEFAULT_CODING_PROMPT, type OutputStore, webTools } from '@humanlayer/fold-agent'
import { skillsFromDisk } from '@humanlayer/fold-agent/skills'
import { fileTools, type Photon } from '@humanlayer/fold-agent/tools/files'
import {
	type FoldEventLog,
	type FoldSession,
	defineAgent,
	eventLogSource,
	openaiModel,
	resumeSession,
	skillTool,
	startSession,
} from '@humanlayer/fold-core'
import * as Cloudflare from 'alchemy/Cloudflare'
import type { RuntimeContext } from 'alchemy/RuntimeContext'
import { Config, Context, Data, Effect, Layer, Match, Option, Predicate, Schema, Stream } from 'effect'
import type { FileSystem, Path, Scope } from 'effect'
import type { HttpClient } from 'effect/http'

import type { ActiveDelivery } from './ActiveDelivery'
import { AgentSessionMessage } from './AgentSessionMessage'
import { bashTool } from './BashTool'
import { checkTools } from './CheckTools'
import { WORKSPACE_ROOT } from './computer/Contract'
import type { DeliveryApi } from './DeliveryApi'
import { updatePlanTool } from './DeliveryPlanTool'
import { DiscussionSeen, readDiscussionContext } from './DiscussionContext'
import * as FoldLog from './DurableObjectSqliteFoldAgentEventLog'
import { createPullRequestTool, githubTools } from './GitHubTools'
import { gitTools } from './GitTools'
import { type RepoCloneError, Workspace, repositoryDirectoryName } from './Workspace'

const MODEL = 'gpt-6.1-sol'
/** The highest comment, review, and line comment IDs the agent has been shown. */
const DISCUSSION_SEEN_KEY = 'discussion_seen'
const HOME = '/root'
/** Where the bash tool saves output too long to show, outside every repository. */
export const TOOL_OUTPUT_DIRECTORY = `${WORKSPACE_ROOT}/.fold/tool-output`

/** What a turn needs from the Fold session. */
export type TurnSession = Pick<FoldSession, 'rootAgentId' | 'entries' | 'send'>

/** What pulling the discussion's repository did before a new turn. */
export type RepositoryUpdate = Data.TaggedEnum<{
	Unchanged: {}
	Updated: { readonly before: string; readonly after: string }
	Failed: { readonly reason: string }
}>
export const RepositoryUpdate = Data.taggedEnum<RepositoryUpdate>()

/** A pull request's branch cannot be checked out: GitHub could not be read, or the branch is in a fork or gone. */
export class PullRequestBranchUnavailable extends Data.TaggedError('PullRequestBranchUnavailable')<{
	readonly reason: string
}> {
	override get message() {
		return `I can't work on this pull request's branch: ${this.reason}`
	}
}

const AgentSessionMetadata = Schema.Struct({
	githubDiscussion: AgentSessionMessage.fields.githubDiscussion,
	repositoryName: Schema.String,
	/** The branch the session checked out. */
	branch: Schema.String,
	/** The repository's default branch. */
	defaultBranch: Schema.String,
	/** The branch the session commits and pushes to; none for a pull request, until it can push to one. */
	workBranch: Schema.NullOr(Schema.String),
})
type AgentSessionMetadata = typeof AgentSessionMetadata.Type

type GitHubDiscussion = AgentSessionMessage['githubDiscussion']

/**
 * The branch the session works on, the same on every mention: `humanlayer/issue-<number>` for an issue, the
 * pull request's own branch for a pull request. A branch in a fork, or deleted, cannot be worked on.
 */
export const workBranchFor = (discussion: GitHubDiscussion) =>
	Match.value(discussion).pipe(
		Match.tagsExhaustive({
			GitHubIssue: (issue) => Effect.succeed(`humanlayer/issue-${issue.ref.number}`),
			GitHubPullRequest: (pullRequest) =>
				pullRequest.fetchInfo().pipe(
					Effect.mapError((error) => new PullRequestBranchUnavailable({ reason: error.message })),
					Effect.flatMap(({ headRef, headRepository }) =>
						Predicate.isNull(headRepository)
							? Effect.fail(new PullRequestBranchUnavailable({ reason: 'its branch has been deleted.' }))
							: headRepository.repositoryId === pullRequest.ref.repositoryId
								? Effect.succeed(headRef)
								: Effect.fail(
										new PullRequestBranchUnavailable({
											reason: `its branch is in the fork ${headRepository.owner}/${headRepository.repository}, which I can't push to.`,
										}),
									),
					),
				),
		}),
	)

/** For a pull request, the tools for its checks. */
const pullRequestCheckTools = ({ githubDiscussion }: AgentSessionMetadata) =>
	Predicate.isTagged(githubDiscussion, 'GitHubPullRequest') ? checkTools(githubDiscussion) : []

/** For an issue with a branch, the tool that opens its pull request. */
const issuePullRequestTools = ({ githubDiscussion, workBranch, defaultBranch }: AgentSessionMetadata) =>
	Predicate.isTagged(githubDiscussion, 'GitHubIssue') && Predicate.isNotNull(workBranch)
		? [createPullRequestTool({ issue: githubDiscussion, branch: workBranch, base: defaultBranch })]
		: []

const discussionName = (discussion: GitHubDiscussion) =>
	`${discussion.ref.owner}/${discussion.ref.repository}#${discussion.ref.number}, ${Match.value(discussion).pipe(
		Match.tagsExhaustive({ GitHubIssue: () => 'an issue', GitHubPullRequest: () => 'a pull request' }),
	)}`

const branchPolicy = ({ githubDiscussion, branch, defaultBranch, workBranch }: AgentSessionMetadata) =>
	Predicate.isNull(workBranch)
		? 'This session has no branch to push to, so you can inspect, run, and explain the code, but not push changes.'
		: [
				`Make your changes on ${workBranch}: commit them with bash (git add, git commit), then push them with git_push. ` +
					`Never commit to ${defaultBranch}. Later requests on this discussion continue on the same branch, pulled from GitHub first.`,
				Match.value(githubDiscussion).pipe(
					Match.tagsExhaustive({
						GitHubIssue: () =>
							`When the work is ready for review, push it, then open a pull request with github_create_pull_request; if one is already open from ${workBranch}, it returns that one. Put the pull request's link in your answer.`,
						GitHubPullRequest: () =>
							`${workBranch} is this pull request's branch, so pushing updates the pull request. Use github_pull_request_checks and github_check_failure to see why a check failed.`,
					}),
				),
				`To bring in the latest ${defaultBranch}, git_fetch it, then git_merge origin/${defaultBranch}.`,
				branch === workBranch ? '' : `Check out ${workBranch} before you commit.`,
			]
				.filter((line) => line.length > 0)
				.join(' ')

/** Fold Agent's coding prompt, then what this agent needs to know about GitHub and its workspace. */
const systemPrompt = (metadata: AgentSessionMetadata) =>
	[
		DEFAULT_CODING_PROMPT,
		'You have no subagents here; do the work yourself.',
		`You are working on GitHub ${discussionName(metadata.githubDiscussion)}. Someone mentioned you there, and each mention is one request. ` +
			'Your final answer is posted there as a comment for you when you finish, so write it in GitHub Markdown and do not also post it with github_post_comment. Use that tool only for other comments, such as a reply in a review thread. If you need something from them, ask in your final answer; they reply by mentioning you again.',
		'Match your effort to the request. If it is a question, answer it as soon as you can, checking only what you need to answer it correctly. Do deeper work, such as changing code, only when asked to. For work of more than a few steps, post a plan with update_plan before you start.',
		'Write clearly and without padding. Start with the answer, or the decision you need from them. Then give the reasoning and details that matter to it, as fully as the question needs: a simple question gets a short answer, a design question can get a longer, structured one. Use plain words, explain any term they may not know, and say each thing once. Leave out restating the request, step-by-step accounts of what you checked, and hedges that do not change the answer. Link to the code on GitHub instead of explaining it at length.',
		`The repository is cloned at ${WORKSPACE_ROOT}/${metadata.repositoryName}, on branch ${metadata.branch}. Its default branch is ${metadata.defaultBranch}.`,
		branchPolicy(metadata),
		'git in bash cannot reach GitHub. Use git_fetch, git_pull, and git_push for that.',
		'When you keep a plan with update_plan, list every step with a stable ID, keep exactly one step InProgress while you work, and update it as steps complete or fail.',
		'Each request starts with the discussion in <github-discussion>: all of it on your first request, then what is new since your last turn. Use the GitHub tools to read anything older or cut off. Use web_search and web_fetch to look up documentation and other information online.',
		`Command output too long to show in full is saved under ${TOOL_OUTPUT_DIRECTORY}; read that file when you need more than the end of it.`,
	].join('\n\n')

const repoSkills = (repositoryName: string) =>
	skillTool(
		skillsFromDisk({
			cwd: WORKSPACE_ROOT,
			home: HOME,
			extraPaths: [
				`${WORKSPACE_ROOT}/${repositoryName}/.claude/skills`,
				`${WORKSPACE_ROOT}/${repositoryName}/.agents/skills`,
			],
		}),
	)

/** Fold's model: GPT-6.1 Sol through the OpenAI API, at medium reasoning. */
const foldModel = Effect.gen(function* () {
	const apiKey = yield* Config.Redacted('OPENAI_API_KEY').pipe(Effect.orDie)
	return openaiModel({ apiKey, model: MODEL, reasoning: 'medium' })
})

/** The services the agent's tools take from the context its Fold session starts in. */
type FoldHost =
	| FileSystem.FileSystem
	| Path.Path
	| Photon
	| Workspace
	| OutputStore
	| GitHubApi
	| HttpClient.HttpClient
	| ActiveDelivery
	| DeliveryApi

/** The AgentSession's Fold conversation. */
export class AgentConversation extends Context.Service<
	AgentConversation,
	{
		/**
		 * Open the Fold session for one turn, starting it with the message's discussion when the log is empty.
		 * It stays open until the scope closes.
		 */
		readonly open: (
			message: AgentSessionMessage,
		) => Effect.Effect<TurnSession, RepoCloneError | PullRequestBranchUnavailable, Scope.Scope | RuntimeContext>
		/** Pull commits pushed since the repository was cloned or last pulled. A failure is reported, not raised. */
		readonly pullRepository: (message: AgentSessionMessage) => Effect.Effect<RepositoryUpdate>
		/**
		 * What the agent has not seen of the discussion, to put before the prompt; `markSeen` records it as seen
		 * once the agent has it. A failure is reported in the text, not raised.
		 */
		readonly readDiscussion: (
			message: AgentSessionMessage,
		) => Effect.Effect<{ readonly text: string; readonly markSeen: Effect.Effect<void> }>
	}
>()('alchemy-cloudflare/AgentConversation', {
	/**
	 * The AgentSession's Fold conversation over its Workspace and Fold log. Each turn opens the session: the
	 * first clones the discussion's repository and starts it, later ones resume it from the log.
	 */
	make: Effect.gen(function* () {
		const workspace = yield* Workspace
		const { storage } = (yield* Cloudflare.DurableObjectState).raw
		const model = yield* foldModel
		const botUserId = yield* Config.schema(GitHubId, 'GITHUB_BOT_USER_ID').pipe(Effect.orDie)
		const github = yield* GitHubApi
		/** Fold's tools read their services from the context the session starts in, so it starts in this object's. */
		const foldHost = yield* Effect.context<FoldHost>()

		const agentFor = (metadata: AgentSessionMetadata) =>
			defineAgent({
				name: 'github-agent',
				systemPrompt: systemPrompt(metadata),
				model,
				tools: [
					...fileTools({ cwd: WORKSPACE_ROOT }),
					bashTool,
					...webTools(),
					updatePlanTool,
					repoSkills(metadata.repositoryName),
					...githubTools(metadata.githubDiscussion),
					...gitTools({ repository: metadata.githubDiscussion.ref, workBranch: metadata.workBranch }),
					...issuePullRequestTools(metadata),
					...pullRequestCheckTools(metadata),
				],
			})

		/**
		 * Clone the discussion's repository onto its work branch, warm up the container in the background, and
		 * start the session.
		 */
		const start = (message: AgentSessionMessage, log: FoldEventLog) =>
			Effect.gen(function* () {
				const discussion = message.githubDiscussion
				const workBranch = yield* workBranchFor(discussion)
				const prepared = yield* workspace.prepare({ repository: discussion.ref, branch: workBranch })
				yield* Effect.forkScoped(workspace.startContainer)
				const metadata = AgentSessionMetadata.make({
					githubDiscussion: discussion,
					repositoryName: repositoryDirectoryName(discussion.ref),
					branch: prepared.branch,
					defaultBranch: prepared.defaultBranch,
					workBranch,
				})
				yield* Effect.logInfo('agent_session.started').pipe(
					Effect.annotateLogs({
						branch: prepared.branch,
						defaultBranch: prepared.defaultBranch,
						createdBranch: prepared.createdBranch,
						commit: prepared.commit,
					}),
				)
				const encodedMetadata = yield* Schema.encodeEffect(AgentSessionMetadata)(metadata).pipe(Effect.orDie)
				return yield* startSession({
					agent: agentFor(metadata),
					log,
					cwd: WORKSPACE_ROOT,
					meta: encodedMetadata,
				})
			})

		const savedSeen = Effect.promise(() => storage.get(DISCUSSION_SEEN_KEY)).pipe(
			Effect.flatMap((value) =>
				Predicate.isUndefined(value)
					? Effect.succeedNone
					: Effect.asSome(Schema.decodeUnknownEffect(DiscussionSeen)(value)),
			),
			Effect.catchTag('SchemaError', (error) =>
				Effect.logWarning(
					'agent_session: the saved discussion progress is unreadable; showing it all',
					error,
				).pipe(Effect.as(Option.none<DiscussionSeen>())),
			),
		)

		return {
			readDiscussion: (message) =>
				Effect.gen(function* () {
					const seen = yield* savedSeen
					const context = yield* readDiscussionContext({
						discussion: message.githubDiscussion,
						seen,
						botUserId,
						requestComments: message.requestComments ?? [],
					})
					yield* Effect.logInfo('agent_session.discussion_read').pipe(
						Effect.annotateLogs({ first: Option.isNone(seen), characters: context.text.length }),
					)
					return {
						text: context.text,
						markSeen: Effect.promise(() => storage.put(DISCUSSION_SEEN_KEY, context.seen)),
					}
				}).pipe(
					Effect.provideService(GitHubApi, github),
					Effect.catchTag('GitHubApiError', (error) =>
						Effect.logWarning('agent_session.read_discussion failed', error).pipe(
							Effect.as({
								text: `<system-information>Reading the GitHub discussion failed, so it is not shown here: ${error.message}. Use the GitHub tools to read it.</system-information>\n\n`,
								markSeen: Effect.void,
							}),
						),
					),
				),
			pullRepository: (message) =>
				workspace.pull({ repository: message.githubDiscussion.ref }).pipe(
					Effect.map(({ before, after }) =>
						before === after ? RepositoryUpdate.Unchanged() : RepositoryUpdate.Updated({ before, after }),
					),
					Effect.catchTag('RepoPullError', (error) =>
						Effect.logWarning('agent_session.pull_repository failed').pipe(
							Effect.annotateLogs({ reason: error.message }),
							Effect.as(RepositoryUpdate.Failed({ reason: error.message })),
						),
					),
					Effect.withSpan('agent_session.pull_repository'),
				),
			open: Effect.fn('agent_session.open_conversation')(function* (message: AgentSessionMessage) {
				const eventLog = yield* FoldLog.open(storage).pipe(Effect.orDie)
				const log = eventLogSource(Effect.succeed(eventLog))
				const entries = yield* Stream.runCollect(eventLog.entries()).pipe(Effect.orDie)
				const started = entries.find((entry) => Predicate.isTagged(entry, 'session_started'))
				if (Predicate.isUndefined(started)) return yield* start(message, log)
				const metadata = yield* Schema.decodeUnknownEffect(AgentSessionMetadata)(started.meta).pipe(
					Effect.orDie,
				)
				yield* Effect.logInfo('agent_session.resumed').pipe(
					Effect.annotateLogs({ branch: metadata.branch, entries: entries.length }),
				)
				return yield* resumeSession({ agent: agentFor(metadata), log })
			}, Effect.provideContext(foldHost)),
		}
	}),
}) {
	static readonly layer = Layer.effect(this, this.make)
}
