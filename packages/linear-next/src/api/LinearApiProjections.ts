import { Schema } from 'effect'

import type { LinearIssueRequest } from '../LinearApi'
import {
	LinearCommentRef,
	LinearIssueInfo,
	LinearIssueLabel,
	LinearParticipant,
	LinearUser,
	LinearWorkflowState,
} from '../LinearModels'
import { LinearComment, LinearIssueAttachment } from '../LinearResources'
import { LinearApiAttachment, LinearApiComment, LinearApiIssue, LinearApiParticipant } from './LinearApiSchemas'

export const projectLinearParticipant = (value: typeof LinearApiParticipant.Type | null) => {
	if (value === null) return null
	return LinearParticipant.make({
		id: value.id,
		name: value.name,
		email: value.email ?? null,
		url: null,
		avatarUrl: null,
	})
}

export const projectLinearIssue = (ref: LinearIssueRequest['issue'], value: typeof LinearApiIssue.Type) =>
	LinearIssueInfo.make({
		ref: { ...ref, teamId: value.team.id },
		identifier: value.identifier,
		title: value.title,
		description: value.description,
		priority: value.priority,
		url: value.url,
		state: LinearWorkflowState.make(value.state),
		labels: value.labels.nodes.map((label) => LinearIssueLabel.make(label)),
		assignee: projectLinearParticipant(value.assignee),
		delegate: projectLinearParticipant(value.delegate),
	})

export const projectLinearComment = (issue: LinearIssueRequest['issue'], value: typeof LinearApiComment.Type) =>
	LinearComment.make({
		ref: LinearCommentRef.make({ ...issue, commentId: value.id }),
		issue,
		parentCommentId: value.parent?.id ?? null,
		content: { markdown: value.body },
		author: projectLinearParticipant(value.user),
	})

export const projectLinearAttachment = (issue: LinearIssueRequest['issue'], value: typeof LinearApiAttachment.Type) => {
	const input: {
		id: typeof value.id
		issueId: typeof issue.issueId
		title: string
		subtitle: string | null
		url: string
		metadata?: Readonly<Record<string, Schema.Json>>
		ref: { issue: typeof issue; attachmentId: typeof value.id }
	} = {
		id: value.id,
		issueId: issue.issueId,
		title: value.title,
		subtitle: value.subtitle,
		url: value.url,
		ref: { issue, attachmentId: value.id },
	}
	if (value.metadata !== undefined) input.metadata = value.metadata
	return LinearIssueAttachment.make(input)
}

export const projectLinearUser = (value: typeof LinearApiParticipant.Type) =>
	LinearUser.make({
		id: value.id,
		name: value.name,
		email: value.email ?? null,
		active: value.active ?? true,
		app: value.app ?? false,
		isAssignable: value.isAssignable ?? false,
		canAccessAnyPublicTeam: value.canAccessAnyPublicTeam ?? false,
		teamIds: (value.teams?.nodes ?? []).map(({ id }) => id),
	})
