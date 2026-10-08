import * as SqliteClient from '@effect/sql-sqlite-do/SqliteClient'
import { GitHubApi, GitHubIssue, GitHubPullRequest } from '@humanlayer/channels-github'
import { skillsFromDisk } from '@humanlayer/fold-agent/skills'
import { fileTools, Photon } from '@humanlayer/fold-agent/tools/files'
import {
	type AgentFinishedLogEntry,
	type AgentId,
	type FoldSession,
	EventLog,
	type LogEntry,
	type UserMessageLogEntry,
	customModel,
	defineAgent,
	eventLogSource,
	layerLiveIdFactory,
	resolveOpenAiReasoning,
	resumeSession,
	skillTool,
	startSession,
} from '@humanlayer/fold-core'
import * as Cloudflare from 'alchemy/Cloudflare'
import {
	Context,
	Effect,
	FileSystem,
	Layer,
	Option,
	Path,
	Predicate,
	Schema,
	Scope,
	Stream,
	SynchronizedRef,
} from 'effect'
import { LanguageModel } from 'effect/ai'
import { FetchHttpClient } from 'effect/http'

import { bashTool } from './BashTool'
import { COMPUTER_WORKER_NAME, WORKSPACE_ROOT } from './computer/Contract'
import { layer as DurableObjectSqliteFoldAgentEventLogLive } from './DurableObjectSqliteFoldAgentEventLog'
import { githubTools } from './GitHubTools'
import { SessionExpiry } from './SessionExpiry'
import { RunRecovery } from './SessionRecovery'
import { Computer, Repo, RepoCloneError, Workspace, repoName } from './Workspace'

const MODEL = '@cf/zai-org/glm-5.3'
const HOME = '/root'
const RESTART_NUDGE =
	'<system-information>A restart cut you off before you finished. Continue where you left off.</system-information>'
const MAX_RESTART_NUDGES = 3
const WORKSPACE_GRACE_MILLIS = 24 * 60 * 60 * 1_000

export const AgentSessionMessage = Schema.Struct({
	prompt: Schema.String,
	githubDiscussion: Schema.Union([GitHubIssue, GitHubPullRequest]),
})
export type AgentSessionMessage = typeof AgentSessionMessage.Type

const AgentSessionMetadata = Schema.Struct({
	githubDiscussion: Schema.Union([GitHubIssue, GitHubPullRequest]),
	repositoryName: Schema.String,
})
type AgentSessionMetadata = typeof AgentSessionMetadata.Type

const repositoryFor = (discussion: GitHubIssue | GitHubPullRequest): Repo => ({
	url: `https://github.com/${discussion.ref.owner}/${discussion.ref.repository}.git`,
})

const systemPrompt = ({ githubDiscussion, repositoryName }: AgentSessionMetadata) =>
	[
		'You are a concise coding agent working on a GitHub discussion.',
		`The repository is cloned at ${WORKSPACE_ROOT}/${repositoryName}.`,
		`The discussion is ${githubDiscussion.ref.owner}/${githubDiscussion.ref.repository}#${githubDiscussion.ref.number}.`,
		'Use the GitHub tools to inspect the discussion and communicate progress. Use bash and file tools to inspect, edit, and test the repository.',
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

const openRootMessages = (entries: ReadonlyArray<LogEntry>, rootAgentId: AgentId) => {
	const lastFinishedSeq =
		entries.findLast((entry) => Predicate.isTagged(entry, 'agent-finished') && entry.agentId === rootAgentId)
			?.seq ?? -1
	return entries.filter(
		(entry): entry is UserMessageLogEntry =>
			Predicate.isTagged(entry, 'user-message') && entry.agentId === rootAgentId && entry.seq > lastFinishedSeq,
	)
}

const isRestartNudge = ({ message }: UserMessageLogEntry) =>
	Predicate.isString(message.content)
		? message.content === RESTART_NUDGE
		: message.content.some((part) => part.type === 'text' && part.text === RESTART_NUDGE)

/** Queue a message into a running turn and resolve when the run that consumed it finishes. */
const deliver = (session: FoldSession, prompt: string) =>
	session.send(prompt).pipe(Effect.catchTag('SubagentNotFoundError', (error) => Effect.die(error)))

/** The AgentSession namespace; callers address it directly by the GitHub discussion's mailbox key. */
export class AgentSession extends Cloudflare.DurableObject<
	AgentSession,
	{
		readonly send: (input: AgentSessionMessage) => Effect.Effect<AgentFinishedLogEntry, RepoCloneError>
		readonly alarm: () => Effect.Effect<void>
	}
>()('AgentSession', { errors: [RepoCloneError] }) {}

const AgentSessionImplementation = Effect.gen(function* () {
	const state = yield* Cloudflare.DurableObjectState
	const ai = yield* Cloudflare.Workers.AI()
	const computers = yield* Computer.from(COMPUTER_WORKER_NAME)
	const githubApi = yield* GitHubApi
	const SqliteLive = SqliteClient.layer({ storage: state.raw.storage })
	const EventLogLive = DurableObjectSqliteFoldAgentEventLogLive.pipe(
		Layer.provide(SqliteLive),
		Layer.provide(layerLiveIdFactory),
	)
	const InstanceLive = Layer.mergeAll(EventLogLive, RunRecovery.layer, SessionExpiry.layer)

	// Alchemy evaluates this returned Effect once per AgentSession object, with RuntimeContext available.
	return Effect.gen(function* () {
		const mailboxKey = yield* Schema.decodeUnknownEffect(Schema.NonEmptyString)(state.id.name)
		const workspace = Workspace.make(computers.getByName(mailboxKey))
		const eventLog = yield* EventLog
		const recovery = yield* RunRecovery
		const expiry = yield* SessionExpiry
		const log = eventLogSource(Effect.succeed(eventLog))
		const initialEntries = yield* Stream.runCollect(eventLog.entries())
		const isEmpty = initialEntries.length === 0
		const started = initialEntries.find((entry) => Predicate.isTagged(entry, 'session_started'))
		const resumedMetadata = Predicate.isUndefined(started)
			? Option.none<AgentSessionMetadata>()
			: Option.some(yield* Schema.decodeUnknownEffect(AgentSessionMetadata)(started.meta))
		const languageModel = yield* Layer.build(ai.model({ model: MODEL })).pipe(
			Effect.map((context) => Context.get(context, LanguageModel.LanguageModel)),
		)
		const model = customModel({
			activeModel: {
				providerId: 'cloudflare-workers-ai',
				providerKind: 'openai-compatible',
				modelId: MODEL,
				role: null,
				requestedReasoningLevel: 'max',
				reasoning: resolveOpenAiReasoning('max'),
			},
			make: Effect.succeed(languageModel),
		})
		const agentFor = (metadata: AgentSessionMetadata) =>
			defineAgent({
				name: 'github-agent',
				systemPrompt: systemPrompt(metadata),
				model,
				tools: [
					...fileTools({ cwd: WORKSPACE_ROOT }),
					bashTool(workspace.exec),
					repoSkills(metadata.repositoryName),
					...githubTools(metadata.githubDiscussion),
				],
			})
		const instanceScope = yield* Scope.make()

		const start = (message: AgentSessionMessage) =>
			Effect.gen(function* () {
				const repository = repositoryFor(message.githubDiscussion)
				yield* workspace.prepare([repository])
				yield* Effect.forkIn(workspace.startContainer, instanceScope)
				const metadata = AgentSessionMetadata.make({
					githubDiscussion: message.githubDiscussion,
					repositoryName: repoName(repository),
				})
				const encodedMetadata = yield* Schema.encodeEffect(AgentSessionMetadata)(metadata).pipe(Effect.orDie)
				return yield* startSession({
					agent: agentFor(metadata),
					log,
					cwd: WORKSPACE_ROOT,
					meta: encodedMetadata,
				})
			})

		const resume = Option.match(resumedMetadata, {
			onNone: () => Effect.die(new Error('AgentSession log has no valid session-started metadata')),
			onSome: (metadata) => resumeSession({ agent: agentFor(metadata), log }),
		})
		const opened = yield* SynchronizedRef.make(Option.none<FoldSession>())
		const open = (message?: AgentSessionMessage) =>
			SynchronizedRef.modifyEffect(opened, (current) =>
				Option.match(current, {
					onSome: (session) => Effect.succeed([session, current] as const),
					onNone: () =>
						(isEmpty
							? Predicate.isUndefined(message)
								? Effect.die(new Error('Cannot recover an AgentSession before its first message'))
								: start(message)
							: resume
						).pipe(
							Effect.provideService(FileSystem.FileSystem, workspace.fileSystem),
							Effect.provideService(GitHubApi, githubApi),
							Effect.provide(Layer.mergeAll(Path.layer, FetchHttpClient.layer, Photon.layer)),
							Scope.provide(instanceScope),
							Effect.map((session) => [session, Option.some(session)] as const),
						),
				}),
			)

		if (!isEmpty && !(yield* expiry.expired)) {
			yield* expiry.ensureScheduled
			yield* Effect.forkIn(
				Effect.gen(function* () {
					const session = yield* open()
					const cutOff = openRootMessages(yield* session.entries, session.rootAgentId)
					if (cutOff.length === 0 || cutOff.filter(isRestartNudge).length >= MAX_RESTART_NUDGES) return
					yield* recovery.run(deliver(session, RESTART_NUDGE))
				}),
				instanceScope,
			)
		}

		return {
			send: (input: AgentSessionMessage) =>
				recovery.run(
					Effect.gen(function* () {
						const message = yield* Schema.decodeUnknownEffect(AgentSessionMessage)(input).pipe(Effect.orDie)
						if (message.githubDiscussion.mailboxKey !== mailboxKey) {
							return yield* Effect.die(
								new Error('AgentSession message does not match this Durable Object mailbox key'),
							)
						}
						const deadline = yield* expiry.touch
						yield* workspace.expireAt(deadline + WORKSPACE_GRACE_MILLIS)
						return yield* deliver(yield* open(message), message.prompt)
					}),
				),
			alarm: () =>
				Effect.flatMap(recovery.alarm, (recoveryOwnsAlarm) =>
					recoveryOwnsAlarm ? Effect.void : expiry.alarm(workspace.destroy),
				),
		}
	}).pipe(Effect.provide(InstanceLive), Effect.orDie)
})

/** Register the AgentSession implementation in the Worker runtime. */
export const AgentSessionDOLive = AgentSession.make(AgentSessionImplementation)
