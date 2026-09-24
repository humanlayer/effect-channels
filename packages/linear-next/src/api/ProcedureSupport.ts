import { Predicate, Schema } from 'effect'

import type { LinearListAppUsersRequest, LinearListAssignableUsersRequest, LinearIssueRequest } from '../LinearApi'
import {
	LinearAttachmentId,
	LinearCommentId,
	LinearIssueLabelId,
	LinearReactionId,
	LinearTeamId,
	LinearUserId,
	LinearWorkflowStateId,
} from '../LinearIdentity'
import {
	LinearCommentRef,
	LinearIssueInfo,
	LinearIssueLabel,
	LinearParticipant,
	LinearUser,
	LinearWorkflowState,
} from '../LinearModels'
import { LinearComment, LinearIssueAttachment } from '../LinearResources'
import { linearGraphql } from './LinearGraphql'

export const Participant = Schema.Struct({
	id: LinearUserId,
	name: Schema.String,
	email: Schema.optionalKey(Schema.NullOr(Schema.String)),
	active: Schema.optionalKey(Schema.Boolean),
	app: Schema.optionalKey(Schema.Boolean),
	isAssignable: Schema.optionalKey(Schema.Boolean),
	canAccessAnyPublicTeam: Schema.optionalKey(Schema.Boolean),
	teams: Schema.optionalKey(Schema.Struct({ nodes: Schema.Array(Schema.Struct({ id: LinearTeamId })) })),
})
export const State = Schema.Struct({ id: LinearWorkflowStateId, name: Schema.String, type: Schema.String })
export const Label = Schema.Struct({ id: LinearIssueLabelId, name: Schema.String, color: Schema.String })
export const Issue = Schema.Struct({
	id: Schema.NonEmptyString,
	identifier: Schema.NonEmptyString,
	title: Schema.String,
	description: Schema.NullOr(Schema.String),
	priority: Schema.Int,
	url: Schema.String,
	team: Schema.Struct({ id: LinearTeamId }),
	state: State,
	labels: Schema.Struct({ nodes: Schema.Array(Label) }),
	assignee: Schema.NullOr(Participant),
	delegate: Schema.NullOr(Participant),
})
export const Comment = Schema.Struct({
	id: LinearCommentId,
	body: Schema.String,
	parent: Schema.optionalKey(Schema.NullOr(Schema.Struct({ id: LinearCommentId }))),
	user: Schema.NullOr(Participant),
})
export const Attachment = Schema.Struct({
	id: LinearAttachmentId,
	title: Schema.String,
	subtitle: Schema.NullOr(Schema.String),
	url: Schema.String,
	metadata: Schema.optionalKey(Schema.Record(Schema.String, Schema.Json)),
})
export const Reaction = Schema.Struct({ id: LinearReactionId, emoji: Schema.String, user: Schema.NullOr(Participant) })
export const PageInfo = Schema.Struct({ endCursor: Schema.NullOr(Schema.String), hasNextPage: Schema.Boolean })

export const participant = (value: typeof Participant.Type | null) =>
	value === null
		? null
		: LinearParticipant.make({
				id: value.id,
				name: value.name,
				email: value.email ?? null,
				url: null,
				avatarUrl: null,
			})
export const issueInfo = (ref: LinearIssueRequest['issue'], value: typeof Issue.Type) =>
	LinearIssueInfo.make({
		ref: { ...ref, teamId: value.team.id },
		identifier: value.identifier,
		title: value.title,
		description: value.description,
		priority: value.priority,
		url: value.url,
		state: LinearWorkflowState.make(value.state),
		labels: value.labels.nodes.map((label) => LinearIssueLabel.make(label)),
		assignee: participant(value.assignee),
		delegate: participant(value.delegate),
	})
export const comment = (issue: LinearIssueRequest['issue'], value: typeof Comment.Type) =>
	LinearComment.make({
		ref: LinearCommentRef.make({ ...issue, commentId: value.id }),
		issue,
		parentCommentId: value.parent?.id ?? null,
		content: { markdown: value.body },
		author: participant(value.user),
	})
export const attachment = (issue: LinearIssueRequest['issue'], value: typeof Attachment.Type) =>
	LinearIssueAttachment.make({
		id: value.id,
		issueId: issue.issueId,
		title: value.title,
		subtitle: value.subtitle,
		url: value.url,
		...(Predicate.isUndefined(value.metadata) ? {} : { metadata: value.metadata }),
		ref: { issue, attachmentId: value.id },
	})
export const user = (value: typeof Participant.Type) =>
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
export const pageOptions = (input: LinearListAppUsersRequest | LinearListAssignableUsersRequest) => ({
	first: input.first ?? 50,
	...(Predicate.isUndefined(input.after) ? {} : { after: input.after }),
	...(Predicate.isUndefined(input.query) ? {} : { query: input.query }),
})
export const runGraphql = <A>(
	operation: Parameters<typeof linearGraphql<A>>[0]['operation'],
	query: string,
	variables: Schema.Json,
	data: Schema.Codec<A, unknown, never, never>,
) => linearGraphql({ operation, query, variables, data })
export const issueFields = `id identifier title description priority url team { id } state { id name type } labels { nodes { id name color } } assignee { id name email } delegate { id name email }`
