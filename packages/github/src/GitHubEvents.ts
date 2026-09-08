import type { EventDefinition } from '@humanlayer/channels-delivery'
import { Schema } from 'effect'

import {
	GitHubDiscussionRef,
	GitHubId,
	GitHubIssueRef,
	GitHubPullRequestRef,
	issueResourceKey,
} from './GitHubResource.js'

export const GitHubUser = Schema.Struct({ id: GitHubId, login: Schema.NonEmptyString, type: Schema.String })
export interface GitHubUser extends Schema.Schema.Type<typeof GitHubUser> {}
export const GitHubIssueData = Schema.Struct({
	id: GitHubId,
	number: GitHubId,
	title: Schema.String,
	body: Schema.NullOr(Schema.String),
	state: Schema.Literals(['open', 'closed']),
	html_url: Schema.String,
	user: GitHubUser,
	pull_request: Schema.optionalKey(Schema.Struct({ url: Schema.String })),
})
export interface GitHubIssueData extends Schema.Schema.Type<typeof GitHubIssueData> {}
export const GitHubCommentData = Schema.Struct({
	id: GitHubId,
	body: Schema.String,
	html_url: Schema.String,
	user: GitHubUser,
})
export interface GitHubCommentData extends Schema.Schema.Type<typeof GitHubCommentData> {}
const common = {
	deliveryId: Schema.NonEmptyString,
	sender: GitHubUser,
}
export const GitHubIssueEvent = Schema.Union([
	Schema.Struct({
		...common,
		resource: GitHubIssueRef,
		issue: GitHubIssueData,
		event: Schema.Literal('issues'),
		action: Schema.Literals(['opened', 'edited', 'closed', 'reopened']),
	}),
	Schema.Struct({
		...common,
		resource: GitHubDiscussionRef,
		issue: GitHubIssueData,
		event: Schema.Literal('issue_comment'),
		action: Schema.Literals(['created', 'edited', 'deleted']),
		comment: GitHubCommentData,
	}),
	Schema.Struct({
		...common,
		resource: GitHubPullRequestRef,
		event: Schema.Literal('pull_request'),
		action: Schema.Literals(['opened', 'edited', 'reopened']),
		pull_request: GitHubIssueData,
	}),
]).check(
	Schema.makeFilter((event) => {
		const data = event.event === 'pull_request' ? event.pull_request : event.issue
		return (
			data.number === event.resource.number &&
			(event.event === 'pull_request' ||
				(data.pull_request === undefined) === (event.resource.kind === 'github.issue'))
		)
	}),
)
export type GitHubIssueEvent = typeof GitHubIssueEvent.Type

export const issueEventDefinition: EventDefinition<typeof GitHubIssueEvent, typeof GitHubDiscussionRef> = {
	name: 'github.issue',
	version: '1',
	provider: 'github',
	event: GitHubIssueEvent,
	resource: GitHubDiscussionRef,
	resourceKey: issueResourceKey,
	identify: (event) => ({
		installation: String(event.resource.repository.installationId),
		eventId: event.deliveryId,
		resource: event.resource,
	}),
}
