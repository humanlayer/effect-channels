import { Schema } from 'effect'

import { LinearWebhookDeliveryId } from './LinearIdentity'
import { LinearAgentGuidance, LinearAgentPrompt, LinearAgentSessionComment, LinearParticipant } from './LinearModels'
import {
	LinearAgentSession,
	LinearComment,
	LinearIssue,
	LinearIssueAttachment,
	LinearReaction,
} from './LinearResources'
import {
	LinearCommentReactionNotification,
	LinearIssueAssignedNotification,
	LinearIssueCommentMentionNotification,
	LinearIssueMentionNotification,
	LinearIssueNewCommentNotification,
	LinearIssueReactionNotification,
	LinearIssueStatusChangedNotification,
	LinearIssueUnassignedNotification,
	LinearWebhookIssue,
} from './LinearWebhookEventSchemas'

const issueActivityFields = {
	eventId: LinearWebhookDeliveryId,
	issue: LinearIssue,
	actor: Schema.NullOr(LinearParticipant),
}

export const LinearIssueOpened = Schema.TaggedStruct('LinearIssueOpened', issueActivityFields)
export type LinearIssueOpened = typeof LinearIssueOpened.Type

export const LinearIssueMention = Schema.TaggedStruct('LinearIssueMention', {
	...issueActivityFields,
	notification: LinearIssueMentionNotification,
})
export type LinearIssueMention = typeof LinearIssueMention.Type

export const LinearCommentMention = Schema.TaggedStruct('LinearCommentMention', {
	...issueActivityFields,
	comment: LinearComment,
	notification: LinearIssueCommentMentionNotification,
})
export type LinearCommentMention = typeof LinearCommentMention.Type

export const LinearAssignmentNotification = Schema.TaggedStruct('LinearAssignmentNotification', {
	...issueActivityFields,
	notification: LinearIssueAssignedNotification,
})
export type LinearAssignmentNotification = typeof LinearAssignmentNotification.Type

export const LinearIssueUnassigned = Schema.TaggedStruct('LinearIssueUnassignedNotification', {
	...issueActivityFields,
	notification: LinearIssueUnassignedNotification,
})
export const LinearIssueNewComment = Schema.TaggedStruct('LinearIssueNewCommentNotification', {
	...issueActivityFields,
	comment: LinearComment,
	notification: LinearIssueNewCommentNotification,
})
export const LinearIssueStatusChanged = Schema.TaggedStruct('LinearIssueStatusChangedNotification', {
	...issueActivityFields,
	notification: LinearIssueStatusChangedNotification,
})
export const LinearIssueReaction = Schema.TaggedStruct('LinearIssueReactionNotification', {
	...issueActivityFields,
	notification: LinearIssueReactionNotification,
})
export const LinearCommentReaction = Schema.TaggedStruct('LinearCommentReactionNotification', {
	...issueActivityFields,
	comment: LinearComment,
	notification: LinearCommentReactionNotification,
})

export const LinearIssueActivity = Schema.Union([
	LinearIssueOpened,
	LinearIssueMention,
	LinearCommentMention,
	LinearAssignmentNotification,
	LinearIssueUnassigned,
	LinearIssueNewComment,
	LinearIssueStatusChanged,
	LinearIssueReaction,
	LinearCommentReaction,
])
export type LinearIssueActivity = typeof LinearIssueActivity.Type

export const LinearAgentSessionCreated = Schema.TaggedStruct('LinearAgentSessionCreated', {
	session: LinearAgentSession,
	issue: LinearIssue,
	promptContext: Schema.NullOr(Schema.String),
	previousComments: Schema.Array(LinearAgentSessionComment),
	guidance: Schema.Array(LinearAgentGuidance),
	deliveryId: LinearWebhookDeliveryId,
})
export type LinearAgentSessionCreated = typeof LinearAgentSessionCreated.Type

export const LinearAgentSessionPrompted = Schema.TaggedStruct('LinearAgentSessionPrompted', {
	session: LinearAgentSession,
	issue: LinearIssue,
	prompt: LinearAgentPrompt,
	deliveryId: LinearWebhookDeliveryId,
})
export type LinearAgentSessionPrompted = typeof LinearAgentSessionPrompted.Type

const changeValue = Schema.optionalKey(Schema.Json)
const issueChange = <Tag extends string>(tag: Tag) => Schema.TaggedStruct(tag, { previous: changeValue })

export const LinearIssueTitleChanged = issueChange('LinearIssueTitleChanged')
export const LinearIssueDescriptionChanged = issueChange('LinearIssueDescriptionChanged')
export const LinearIssueStatusChange = issueChange('LinearIssueStatusChanged')
export const LinearIssuePriorityChanged = issueChange('LinearIssuePriorityChanged')
export const LinearIssueLabelsChanged = issueChange('LinearIssueLabelsChanged')
export const LinearIssueAssigneeChanged = issueChange('LinearIssueAssigneeChanged')
export const LinearIssueDelegateChanged = issueChange('LinearIssueDelegateChanged')
export const LinearIssueProjectChanged = issueChange('LinearIssueProjectChanged')
export const LinearIssueMilestoneChanged = issueChange('LinearIssueMilestoneChanged')
export const LinearIssueCycleChanged = issueChange('LinearIssueCycleChanged')
export const LinearIssueTeamChanged = issueChange('LinearIssueTeamChanged')
export const LinearIssueParentChanged = issueChange('LinearIssueParentChanged')
export const LinearIssueEstimateChanged = issueChange('LinearIssueEstimateChanged')
export const LinearIssueDueDateChanged = issueChange('LinearIssueDueDateChanged')
export const LinearIssueSubscribersChanged = issueChange('LinearIssueSubscribersChanged')
export const LinearIssueArchiveChanged = issueChange('LinearIssueArchiveChanged')
export const LinearIssueLifecycleChanged = issueChange('LinearIssueLifecycleChanged')
export const LinearIssueReleasesChanged = issueChange('LinearIssueReleasesChanged')
export const LinearIssueSlaChanged = issueChange('LinearIssueSlaChanged')

export const LinearIssueChange = Schema.Union([
	LinearIssueTitleChanged,
	LinearIssueDescriptionChanged,
	LinearIssueStatusChange,
	LinearIssuePriorityChanged,
	LinearIssueLabelsChanged,
	LinearIssueAssigneeChanged,
	LinearIssueDelegateChanged,
	LinearIssueProjectChanged,
	LinearIssueMilestoneChanged,
	LinearIssueCycleChanged,
	LinearIssueTeamChanged,
	LinearIssueParentChanged,
	LinearIssueEstimateChanged,
	LinearIssueDueDateChanged,
	LinearIssueSubscribersChanged,
	LinearIssueArchiveChanged,
	LinearIssueLifecycleChanged,
	LinearIssueReleasesChanged,
	LinearIssueSlaChanged,
])
export type LinearIssueChange = typeof LinearIssueChange.Type

const subscribedFields = { eventId: LinearWebhookDeliveryId, actor: Schema.NullOr(LinearParticipant) }

export const LinearIssueUpdated = Schema.TaggedStruct('LinearIssueUpdated', {
	...subscribedFields,
	issue: LinearWebhookIssue,
	changes: Schema.Array(LinearIssueChange),
	otherChanges: Schema.Record(Schema.String, Schema.Json),
})
export const LinearIssueRemoved = Schema.TaggedStruct('LinearIssueRemoved', {
	...subscribedFields,
	issue: LinearWebhookIssue,
})
export const LinearCommentCreated = Schema.TaggedStruct('LinearCommentCreated', {
	...subscribedFields,
	comment: LinearComment,
})
export const LinearCommentUpdated = Schema.TaggedStruct('LinearCommentUpdated', {
	...subscribedFields,
	comment: LinearComment,
	previousBody: Schema.optionalKey(Schema.NullOr(Schema.String)),
	otherChanges: Schema.Record(Schema.String, Schema.Json),
})
export const LinearCommentRemoved = Schema.TaggedStruct('LinearCommentRemoved', {
	...subscribedFields,
	comment: LinearComment,
})
export const LinearReactionAdded = Schema.TaggedStruct('LinearReactionAdded', {
	...subscribedFields,
	reaction: LinearReaction,
})
export const LinearReactionRemoved = Schema.TaggedStruct('LinearReactionRemoved', {
	...subscribedFields,
	reaction: LinearReaction,
})
export const LinearIssueAttachmentCreated = Schema.TaggedStruct('LinearIssueAttachmentCreated', {
	...subscribedFields,
	attachment: LinearIssueAttachment,
})
export const LinearIssueAttachmentUpdated = Schema.TaggedStruct('LinearIssueAttachmentUpdated', {
	...subscribedFields,
	attachment: LinearIssueAttachment,
	otherChanges: Schema.Record(Schema.String, Schema.Json),
})
export const LinearIssueAttachmentRemoved = Schema.TaggedStruct('LinearIssueAttachmentRemoved', {
	...subscribedFields,
	attachment: LinearIssueAttachment,
})

export const LinearSubscribedIssueEvent = Schema.Union([
	LinearIssueUpdated,
	LinearIssueRemoved,
	LinearCommentCreated,
	LinearCommentUpdated,
	LinearCommentRemoved,
	LinearReactionAdded,
	LinearReactionRemoved,
	LinearIssueAttachmentCreated,
	LinearIssueAttachmentUpdated,
	LinearIssueAttachmentRemoved,
])
export type LinearSubscribedIssueEvent = typeof LinearSubscribedIssueEvent.Type

export const LinearIssueCallbackEvent = Schema.Union([LinearIssueActivity, LinearSubscribedIssueEvent])
export type LinearIssueCallbackEvent = typeof LinearIssueCallbackEvent.Type

const activationFields = {
	issue: LinearIssue,
	events: Schema.Array(LinearIssueCallbackEvent),
}

export const LinearIssueCreated = Schema.TaggedStruct('LinearIssueCreated', {
	...activationFields,
	trigger: LinearIssueOpened,
})
export type LinearIssueCreated = typeof LinearIssueCreated.Type

export const LinearIssueMentioned = Schema.TaggedStruct('LinearIssueMentioned', {
	...activationFields,
	trigger: LinearIssueMention,
})
export type LinearIssueMentioned = typeof LinearIssueMentioned.Type

export const LinearCommentMentioned = Schema.TaggedStruct('LinearCommentMentioned', {
	...activationFields,
	trigger: LinearCommentMention,
})
export type LinearCommentMentioned = typeof LinearCommentMentioned.Type

export const LinearMentioned = Schema.Union([LinearIssueMentioned, LinearCommentMentioned])
export type LinearMentioned = typeof LinearMentioned.Type

export const LinearIssueAssigned = Schema.TaggedStruct('LinearIssueAssigned', {
	...activationFields,
	trigger: LinearAssignmentNotification,
})
export type LinearIssueAssigned = typeof LinearIssueAssigned.Type

export class LinearSubscribedEvents extends Schema.TaggedClass<LinearSubscribedEvents>()('LinearSubscribedEvents', {
	issue: LinearIssue,
	events: Schema.NonEmptyArray(LinearSubscribedIssueEvent),
}) {}

/** The single source of truth for callback names and callback argument shapes. */
export interface LinearCallbackEventMap {
	readonly onAgentSessionCreated: LinearAgentSessionCreated
	readonly onAgentSessionPrompted: LinearAgentSessionPrompted
	readonly onIssueCreated: LinearIssueCreated
	readonly onMentioned: LinearMentioned
	readonly onAssigned: LinearIssueAssigned
	readonly onSubscribedEvent: LinearSubscribedEvents
}
