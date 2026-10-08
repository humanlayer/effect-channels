import { GitHubApi } from '@humanlayer/channels-github'
import { DEFAULT_CODING_PROMPT, layerOutputStore, OutputStore, webTools } from '@humanlayer/fold-agent'
import { skillsFromDisk } from '@humanlayer/fold-agent/skills'
import { fileTools, Photon } from '@humanlayer/fold-agent/tools/files'
import {
	type FoldEventLog,
	customModel,
	defineAgent,
	eventLogSource,
	resolveOpenAiReasoning,
	resumeSession,
	skillTool,
	startSession,
} from '@humanlayer/fold-core'
import * as Cloudflare from 'alchemy/Cloudflare'
import type { RuntimeContext } from 'alchemy/RuntimeContext'
import { Context, Effect, FileSystem, Layer, Match, Path, Predicate, Schema, Stream } from 'effect'
import { LanguageModel } from 'effect/ai'
import { HttpClient } from 'effect/http'

import { bashTool } from './BashTool'
import { WORKSPACE_ROOT } from './computer/Contract'
import { DeliveryApi } from './DeliveryApi'
import { updatePlanTool } from './DeliveryPlanTool'
import { ActiveDelivery, AgentConversation, AgentSessionMessage, DeliveryTurns, RepositoryUpdate } from './DeliveryTurn'
import * as FoldLog from './DurableObjectSqliteFoldAgentEventLog'
import { githubTools } from './GitHubTools'
import { gitTools } from './GitTools'
import { SessionExpiry } from './SessionExpiry'
import { RunRecovery } from './SessionRecovery'
import { Workspace, repositoryDirectoryName } from './Workspace'

const MODEL = '@cf/zai-org/glm-5.3'
const HOME = '/root'
const WORKSPACE_GRACE_MILLIS = 24 * 60 * 60 * 1_000

/** Where the bash tool saves output too long to show, outside every repository. */
const TOOL_OUTPUT_DIRECTORY = `${WORKSPACE_ROOT}/.fold/tool-output`

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

/** An issue's work goes on `humanlayer/issue-<number>`, the same branch on every mention. */
const workBranchFor = (discussion: GitHubDiscussion) =>
	Match.value(discussion).pipe(
		Match.tagsExhaustive({
			GitHubIssue: (issue) => `humanlayer/issue-${issue.ref.number}`,
			GitHubPullRequest: () => null,
		}),
	)

const discussionName = (discussion: GitHubDiscussion) =>
	`${discussion.ref.owner}/${discussion.ref.repository}#${discussion.ref.number}, ${Match.value(discussion).pipe(
		Match.tagsExhaustive({ GitHubIssue: () => 'an issue', GitHubPullRequest: () => 'a pull request' }),
	)}`

const branchPolicy = ({ branch, defaultBranch, workBranch }: AgentSessionMetadata) =>
	Predicate.isNull(workBranch)
		? 'This session has no branch to push to, so you can inspect, run, and explain the code, but not push changes.'
		: [
				`Make your changes on ${workBranch}: commit them with bash (git add, git commit), then push them with git_push. ` +
					`Never commit to ${defaultBranch}. Later requests on this discussion continue on the same branch.`,
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
			'Your final answer is posted there as a comment, so write it in GitHub Markdown. If you need something from them, ask in your final answer; they reply by mentioning you again.',
		`The repository is cloned at ${WORKSPACE_ROOT}/${metadata.repositoryName}, on branch ${metadata.branch}. Its default branch is ${metadata.defaultBranch}.`,
		branchPolicy(metadata),
		'git in bash cannot reach GitHub. Use git_fetch, git_pull, and git_push for that.',
		'For a request of more than one step, keep a plan with update_plan: list every step with a stable ID, keep exactly one step InProgress while you work, and update it as steps complete or fail.',
		'Use the GitHub tools to read the discussion. Use web_search and web_fetch to look up documentation and other information online.',
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

/**
 * The AgentSession namespace; callers address it directly by the GitHub discussion's mailbox key. `send`
 * saves the handed-off delivery and starts its turn in the background, returning once the turn is accepted;
 * the turn reports its result to the delivery.
 */
export class AgentSession extends Cloudflare.DurableObject<
	AgentSession,
	{
		readonly send: (input: typeof AgentSessionMessage.Encoded) => Effect.Effect<void, never, RuntimeContext>
		readonly alarm: () => Effect.Effect<void, never, RuntimeContext>
	}
>()('AgentSession') {}

/**
 * The AgentSession namespace as a service, as `DeliveryMailboxes` is for mailboxes. The Worker entrypoint
 * provides it from {@link AgentSession}.
 */
export class AgentSessions extends Context.Service<
	AgentSessions,
	{
		readonly getByName: (mailboxKey: string) => {
			readonly send: (input: typeof AgentSessionMessage.Encoded) => Effect.Effect<void, never, RuntimeContext>
		}
	}
>()('alchemy-cloudflare/AgentSessions') {}

/**
 * Fold's model: GLM through the Workers AI binding, at its highest reasoning level. The binding's model is
 * built when the session starts, in the session's scope: it needs the live object's runtime context.
 */
const foldModel = Effect.gen(function* () {
	const ai = yield* Cloudflare.Workers.AI()
	return customModel({
		activeModel: {
			providerId: 'cloudflare-workers-ai',
			providerKind: 'openai-compatible',
			modelId: MODEL,
			role: null,
			requestedReasoningLevel: 'max',
			reasoning: resolveOpenAiReasoning('max'),
		},
		make: Layer.build(ai.model({ model: MODEL })).pipe(Effect.map(Context.get(LanguageModel.LanguageModel))),
	})
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

/**
 * The AgentSession's Fold conversation over its Workspace and Fold log. Each turn opens the session: the
 * first clones the discussion's repository and starts it, later ones resume it from the log.
 */
const AgentConversationLive = Layer.effect(
	AgentConversation,
	Effect.gen(function* () {
		const workspace = yield* Workspace
		const { storage } = (yield* Cloudflare.DurableObjectState).raw
		const model = yield* foldModel
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
				],
			})

		/**
		 * Clone the discussion's repository onto its work branch, warm up the container in the background, and
		 * start the session.
		 */
		const start = (message: AgentSessionMessage, log: FoldEventLog) =>
			Effect.gen(function* () {
				const discussion = message.githubDiscussion
				const workBranch = workBranchFor(discussion)
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

		return AgentConversation.of({
			pullRepository: (message) =>
				workspace.pull({ repository: message.githubDiscussion.ref }).pipe(
					Effect.map(({ before, after }) =>
						before === after ? RepositoryUpdate.Unchanged() : RepositoryUpdate.Updated({ before, after }),
					),
					Effect.catchTag('RepoPullError', (error) =>
						Effect.logWarning('agent_session.pull_repository failed', error).pipe(
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
				yield* Effect.logInfo('agent_session.resumed').pipe(
					Effect.annotateLogs({ branch: metadata.branch, entries: entries.length }),
				)
				return yield* resumeSession({ agent: agentFor(metadata), log })
			}, Effect.provideContext(foldHost)),
		})
	}),
)

/** This object's services. The Worker entrypoint provides their dependencies. */
const AgentSessionLive = DeliveryTurns.layer.pipe(
	Layer.provideMerge(AgentConversationLive),
	Layer.provideMerge(Layer.mergeAll(ActiveDelivery.layer, RunRecovery.layer, SessionExpiry.layer)),
	Layer.provideMerge(layerOutputStore({ directory: TOOL_OUTPUT_DIRECTORY })),
	Layer.provideMerge(Workspace.fileSystemLayer),
	Layer.provideMerge(Workspace.layer),
)

/**
 * Build this object's services, then, once the object is live, create the Fold log's table, recover a turn a
 * deploy or crash cut off, and return the RPC methods.
 */
const AgentSessionImplementation = Effect.gen(function* () {
	const state = yield* Cloudflare.DurableObjectState
	const workspace = yield* Workspace
	const recovery = yield* RunRecovery
	const expiry = yield* SessionExpiry
	const turns = yield* DeliveryTurns

	return Effect.gen(function* () {
		const mailboxKey = yield* Schema.decodeUnknownEffect(Schema.NonEmptyString)(state.id.name)
		yield* FoldLog.createTable(state.raw.storage)
		if (!(yield* expiry.expired)) {
			yield* expiry.ensureScheduled
			yield* turns.recover
		}

		return {
			send: Effect.fn('agent_session.send')(function* (input: typeof AgentSessionMessage.Encoded) {
				const message = yield* Schema.decodeEffect(AgentSessionMessage)(input).pipe(Effect.orDie)
				if (message.githubDiscussion.mailboxKey !== mailboxKey) {
					return yield* Effect.die(
						new Error('AgentSession message does not match this Durable Object mailbox key'),
					)
				}
				const deadline = yield* expiry.touch
				yield* workspace.expireAt(deadline + WORKSPACE_GRACE_MILLIS)
				return yield* turns.accept(message)
			}),
			alarm: () =>
				Effect.flatMap(recovery.alarm, (recoveryOwnsAlarm) =>
					recoveryOwnsAlarm ? Effect.void : expiry.alarm(workspace.destroy),
				),
		}
	}).pipe(
		Effect.tapCause((cause) => Effect.logError('AgentSession could not start', cause)),
		Effect.orDie,
	)
}).pipe(Effect.provide(AgentSessionLive))

/** Register the AgentSession implementation in the Worker runtime. */
export const AgentSessionDOLive = AgentSession.make(AgentSessionImplementation)
