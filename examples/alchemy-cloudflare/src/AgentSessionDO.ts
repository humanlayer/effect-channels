import * as SqliteClient from '@effect/sql-sqlite-do/SqliteClient'
import { GitHubApi, type GitHubIssue, type GitHubPullRequest } from '@humanlayer/channels-github'
import { skillsFromDisk } from '@humanlayer/fold-agent/skills'
import { fileTools, Photon } from '@humanlayer/fold-agent/tools/files'
import {
	type FoldSession,
	EventLog,
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

import { bashTool } from './BashTool'
import { COMPUTER_WORKER_NAME, WORKSPACE_ROOT } from './computer/Contract'
import { DeliveryApi } from './DeliveryApi'
import { ActiveDelivery, AgentConversation, AgentSessionMessage, DeliveryTurns } from './DeliveryTurn'
import { layer as DurableObjectSqliteFoldAgentEventLogLive } from './DurableObjectSqliteFoldAgentEventLog'
import { githubTools } from './GitHubTools'
import { SessionExpiry } from './SessionExpiry'
import { RunRecovery } from './SessionRecovery'
import { Computer, type Repo, Workspace, repoName } from './Workspace'

const MODEL = '@cf/zai-org/glm-5.3'
const HOME = '/root'
const WORKSPACE_GRACE_MILLIS = 24 * 60 * 60 * 1_000

const AgentSessionMetadata = Schema.Struct({
	githubDiscussion: AgentSessionMessage.fields.githubDiscussion,
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

/**
 * The AgentSession namespace; callers address it directly by the GitHub discussion's mailbox key. `send`
 * saves the handed-off delivery and starts its turn in the background, returning once the turn is accepted;
 * the turn reports its result to the delivery.
 */
export class AgentSession extends Cloudflare.DurableObject<
	AgentSession,
	{
		readonly send: (input: typeof AgentSessionMessage.Encoded) => Effect.Effect<void>
		readonly alarm: () => Effect.Effect<void>
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
			readonly send: (input: typeof AgentSessionMessage.Encoded) => Effect.Effect<void>
		}
	}
>()('alchemy-cloudflare/AgentSessions') {}

/** Fold's model: GLM through the Workers AI binding, at its highest reasoning level. */
const foldModel = Effect.gen(function* () {
	const languageModel = yield* LanguageModel.LanguageModel
	return customModel({
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
})

/**
 * The AgentSession's Fold conversation over its Workspace and Fold log. The first message clones the
 * discussion's repository and starts the session; later ones, and a fresh object, resume it. The session
 * lives as long as this layer.
 */
const AgentConversationLive = Layer.effect(
	AgentConversation,
	Effect.gen(function* () {
		const scope = yield* Scope.Scope
		const workspace = yield* Workspace
		const eventLog = yield* EventLog
		const githubApi = yield* GitHubApi
		const path = yield* Path.Path
		const photon = yield* Photon
		const model = yield* foldModel
		const log = eventLogSource(Effect.succeed(eventLog))
		const initialEntries = yield* Stream.runCollect(eventLog.entries())
		const isEmpty = initialEntries.length === 0
		const started = initialEntries.find((entry) => Predicate.isTagged(entry, 'session_started'))
		const resumedMetadata = Predicate.isUndefined(started)
			? Option.none<AgentSessionMetadata>()
			: Option.some(yield* Schema.decodeUnknownEffect(AgentSessionMetadata)(started.meta))
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

		const start = (message: AgentSessionMessage) =>
			Effect.gen(function* () {
				const repository = repositoryFor(message.githubDiscussion)
				yield* workspace.prepare([repository])
				yield* Effect.forkIn(workspace.startContainer, scope)
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

		return AgentConversation.of({
			open: (message) =>
				SynchronizedRef.modifyEffect(opened, (current) =>
					Option.match(current, {
						onSome: (session) => Effect.succeed([session, current] as const),
						onNone: () =>
							(isEmpty ? start(message) : resume).pipe(
								Effect.provideService(FileSystem.FileSystem, workspace.fileSystem),
								Effect.provideService(GitHubApi, githubApi),
								Effect.provideService(Path.Path, path),
								Effect.provideService(Photon, photon),
								Scope.provide(scope),
								Effect.map((session) => [session, Option.some(session)] as const),
							),
					}),
				).pipe(Effect.withSpan('agent_session.open_conversation')),
		})
	}),
)

const AgentSessionImplementation = Effect.gen(function* () {
	const state = yield* Cloudflare.DurableObjectState
	const ai = yield* Cloudflare.Workers.AI()
	const computers = yield* Computer.from(COMPUTER_WORKER_NAME)
	const githubApi = yield* GitHubApi
	const deliveryApi = yield* DeliveryApi
	const EventLogLive = DurableObjectSqliteFoldAgentEventLogLive.pipe(
		Layer.provide(SqliteClient.layer({ storage: state.raw.storage })),
		Layer.provide(layerLiveIdFactory),
	)

	/** Alchemy evaluates this returned Effect once per AgentSession object, with RuntimeContext available. */
	return Effect.gen(function* () {
		const mailboxKey = yield* Schema.decodeUnknownEffect(Schema.NonEmptyString)(state.id.name)

		/** This object's services. Built once into a scope that is never closed: they live as long as the object. */
		const InstanceLive = DeliveryTurns.layer.pipe(
			Layer.provideMerge(AgentConversationLive),
			Layer.provideMerge(Layer.mergeAll(ActiveDelivery.layer, RunRecovery.layer, SessionExpiry.layer)),
			Layer.provideMerge(EventLogLive),
			Layer.provideMerge(Layer.succeed(Workspace, Workspace.make(computers.getByName(mailboxKey)))),
			Layer.provideMerge(ai.model({ model: MODEL })),
			Layer.provideMerge(Layer.mergeAll(Path.layer, Photon.layer)),
			Layer.provideMerge(Layer.succeed(GitHubApi, githubApi)),
			Layer.provideMerge(Layer.succeed(DeliveryApi, deliveryApi)),
		)
		const instance = yield* Layer.buildWithScope(InstanceLive, yield* Scope.make())

		return yield* agentSessionMethods(mailboxKey).pipe(Effect.provideContext(instance))
	}).pipe(
		Effect.tapCause((cause) => Effect.logError('AgentSession could not start', cause)),
		Effect.orDie,
	)
})

/** The RPC methods, after recovering a turn a deploy or crash cut off. */
const agentSessionMethods = (mailboxKey: string) =>
	Effect.gen(function* () {
		const workspace = yield* Workspace
		const recovery = yield* RunRecovery
		const expiry = yield* SessionExpiry
		const turns = yield* DeliveryTurns
		const hasLog = (yield* Stream.runCount((yield* EventLog).entries())) > 0

		if (!(yield* expiry.expired)) {
			if (hasLog) yield* expiry.ensureScheduled
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
	})

/** Register the AgentSession implementation in the Worker runtime. */
export const AgentSessionDOLive = AgentSession.make(AgentSessionImplementation)
