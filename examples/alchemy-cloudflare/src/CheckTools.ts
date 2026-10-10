/**
 * The agent's tools for a pull request's checks: what ran on its latest commit, and why one failed. A failed
 * check's full job log is saved in the workspace, and the agent sees its end.
 */
import {
	GitHubCheckRun,
	GitHubCheckRunRef,
	GitHubId,
	type GitHubApi,
	type GitHubCheckAnnotation,
	type GitHubCheckRunInfo,
	type GitHubPullRequest,
} from '@humanlayer/channels-github'
import type { OutputStore } from '@humanlayer/fold-agent'
import { defineTool, ToolResultFailure, ToolResultText, type FoldTool } from '@humanlayer/fold-core'
import { Array as Arr, Effect, Predicate, Schema } from 'effect'

import { visibleOutput } from './ToolOutput'
import { NoParameters } from './ToolParameters'

/** Longest check summary or output text shown in full. */
const MAX_TEXT_LENGTH = 2_000
/** Most annotations listed for one check. */
const MAX_ANNOTATIONS = 50

const clip = (text: string | null) =>
	Predicate.isNull(text) || text.length <= MAX_TEXT_LENGTH ? text : `${text.slice(0, MAX_TEXT_LENGTH)}…`

const failure = (error: { readonly message: string }) =>
	ToolResultFailure.make({ text: `GitHub request failed: ${error.message}` })

/** A check as the agent sees it in a list. */
const checkSummary = (check: GitHubCheckRunInfo) => ({
	id: check.ref.id,
	name: check.name,
	status: check.status,
	conclusion: check.conclusion,
	title: check.outputTitle,
	summary: clip(check.outputSummary),
	annotations: check.annotationCount,
	url: check.detailsUrl ?? check.url,
})

/** The checks that ran on the pull request's latest commit, each with its ID for `github_check_failure`. */
export const listPullRequestChecks = Effect.fn('agent_session.list_checks')(function* (pullRequest: GitHubPullRequest) {
	const { headSha } = yield* pullRequest.fetchInfo()
	const runs = yield* pullRequest.listCheckRunsForRef(headSha)
	const checks = yield* Effect.forEach(runs, (run) => run.fetchInfo(), { concurrency: 8 })
	const current = checks.filter((check) => check.headSha === headSha).map(checkSummary)
	return ToolResultText.make({ text: JSON.stringify({ headSha, checks: current }, null, 2) })
})

const annotationLine = (annotation: GitHubCheckAnnotation) => {
	const lines =
		annotation.startLine === annotation.endLine
			? `${annotation.startLine}`
			: `${annotation.startLine}-${annotation.endLine}`
	const text = [annotation.title, annotation.message].filter(Predicate.isNotNull).join(': ')
	return `- ${annotation.level ?? 'note'} at ${annotation.path}:${lines}: ${text}`
}

/**
 * Why a check failed: its result and output, its annotations, and, for a GitHub Actions job, its failed
 * steps and the end of its log, with the whole log saved in the workspace.
 */
export const describeCheckFailure = Effect.fn('agent_session.describe_check_failure')(function* (
	pullRequest: GitHubPullRequest,
	checkRunId: number,
) {
	const { installationId, repositoryId, owner, repository } = pullRequest.ref
	const checkRun = GitHubCheckRun.make({
		ref: GitHubCheckRunRef.make({ installationId, repositoryId, owner, repository, id: GitHubId.make(checkRunId) }),
	})
	const info = yield* checkRun.fetchInfo()
	const annotations = info.annotationCount > 0 ? yield* checkRun.listAnnotations() : []
	const job = yield* checkRun.resolveActionsJob()

	const sections: Array<string> = [
		`Check "${info.name}" (check run ${info.ref.id}) on ${info.headSha.slice(0, 7)}: ${info.conclusion ?? info.status}. ${info.detailsUrl ?? info.url ?? ''}`.trim(),
	]
	const output = [info.outputTitle, clip(info.outputSummary), clip(info.outputText)].filter(Predicate.isNotNull)
	if (Arr.isArrayNonEmpty(output)) sections.push(`Output:\n${output.join('\n\n')}`)
	if (Arr.isReadonlyArrayNonEmpty(annotations)) {
		const listed = annotations.slice(0, MAX_ANNOTATIONS).map(annotationLine)
		const more = annotations.length > MAX_ANNOTATIONS ? [`- …and ${annotations.length - MAX_ANNOTATIONS} more`] : []
		sections.push(`Annotations:\n${[...listed, ...more].join('\n')}`)
	}
	if (Predicate.isNull(job)) {
		sections.push('This check is not a GitHub Actions job, so there is no job log.')
		return ToolResultText.make({ text: sections.join('\n\n') })
	}
	const jobInfo = yield* job.fetchInfo()
	const failedSteps = jobInfo.steps.filter(
		(step) =>
			Predicate.isNotNull(step.conclusion) && step.conclusion !== 'success' && step.conclusion !== 'skipped',
	)
	sections.push(
		Arr.isReadonlyArrayNonEmpty(failedSteps)
			? `Failed steps in job "${jobInfo.name}":\n${failedSteps.map((step) => `- ${step.number}. ${step.name}: ${step.conclusion}`).join('\n')}`
			: `Job "${jobInfo.name}" has no failed steps.`,
	)
	const log = yield* job.downloadLog()
	sections.push(`Job log:\n${yield* visibleOutput(log, 'github_check_failure')}`)
	return ToolResultText.make({ text: sections.join('\n\n') })
})

/** The check tools for a pull request session. */
export const checkTools = (pullRequest: GitHubPullRequest): ReadonlyArray<FoldTool<GitHubApi | OutputStore>> => [
	defineTool({
		name: 'github_pull_request_checks',
		description:
			"List the checks on the pull request's latest commit: each one's ID, name, status, result, summary, annotation count, and link.",
		parameters: NoParameters,
		success: ToolResultText,
		failure: ToolResultFailure,
		handler: () => listPullRequestChecks(pullRequest).pipe(Effect.mapError(failure)),
	}),
	defineTool({
		name: 'github_check_failure',
		description:
			"Find out why a check failed: its output, its annotations, and, for a GitHub Actions job, the failed steps and the end of the job's log. The whole log is saved in the workspace.",
		parameters: Schema.Struct({
			check_run_id: GitHubId.annotate({ description: 'The check run ID, from github_pull_request_checks' }),
		}),
		success: ToolResultText,
		failure: ToolResultFailure,
		handler: ({ check_run_id }) => describeCheckFailure(pullRequest, check_run_id).pipe(Effect.mapError(failure)),
	}),
]
