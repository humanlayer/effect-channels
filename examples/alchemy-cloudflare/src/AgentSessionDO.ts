import { GitHubIssue, GitHubPullRequest } from '@humanlayer/channels-github'
import { skillsFromDisk } from '@humanlayer/fold-agent/skills'
import { fileTools, Photon } from '@humanlayer/fold-agent/tools/files'
import { AgentFinishedLogEntry, LogEntry, SessionId } from '@humanlayer/fold-core'
import * as Cloudflare from 'alchemy/Cloudflare'
import { Schema, Effect, Layer } from 'effect'

import { RepoCloneError } from './Workspace'

const AgentSessionSendInput = Schema.Struct({
	prompt: Schema.String,
	whenRunning: Schema.Literals(['interrupt', 'queue', 'steer']),
	githubDiscussion: Schema.Union([GitHubIssue, GitHubPullRequest]),
})
type AgentSessionSendInput = typeof AgentSessionSendInput.Type

const AgentSessionResult = Schema.Struct({
	finalMessage: Schema.String,
})
export type AgentSessionResult = typeof AgentSessionResult.Type

export default class AgentSession extends Cloudflare.DurableObject<
	AgentSession,
	{
		send: (input: AgentSessionSendInput) => Effect.Effect<AgentFinishedLogEntry, RepoCloneError>
		logEntries: () => Effect.Effect<ReadonlyArray<LogEntry>>
	}
>()('AgentSession') {}
