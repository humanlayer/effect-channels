import type { LinearIssueRequest } from '../LinearApi'
import { discoverLinearFiles, LinearFile, LinearFileRef } from '../LinearFiles'
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
import type { LinearUploadTarget } from './RequestLinearFileUpload'

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
		files: discoverLinearFiles(
			LinearFileRef.make({ organizationId: issue.organizationId, issueId: issue.issueId }),
			value.body,
		),
	})

/** Linear returns the storage key as `filename`, so the public name is the caller's filename. */
export const projectLinearUploadedFile = (
	issue: LinearIssueRequest['issue'],
	filename: string,
	target: LinearUploadTarget,
) =>
	LinearFile.make({
		ref: LinearFileRef.make({ organizationId: issue.organizationId, issueId: issue.issueId }),
		url: target.assetUrl,
		name: filename,
		contentType: target.contentType,
		size: target.size,
	})

export const projectLinearAttachment = (issue: LinearIssueRequest['issue'], value: typeof LinearApiAttachment.Type) =>
	LinearIssueAttachment.make({ ...value, issueId: issue.issueId, ref: { issue, attachmentId: value.id } })

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
