import { Schema } from 'effect'

import {
	LinearIssueArchiveChanged,
	LinearIssueAssigneeChanged,
	LinearIssueCycleChanged,
	LinearIssueDelegateChanged,
	LinearIssueDescriptionChanged,
	LinearIssueDueDateChanged,
	LinearIssueEstimateChanged,
	LinearIssueLabelsChanged,
	LinearIssueLifecycleChanged,
	LinearIssueMilestoneChanged,
	LinearIssueParentChanged,
	LinearIssuePriorityChanged,
	LinearIssueProjectChanged,
	LinearIssueReleasesChanged,
	LinearIssueSlaChanged,
	LinearIssueStatusChange,
	LinearIssueSubscribersChanged,
	LinearIssueTeamChanged,
	LinearIssueTitleChanged,
	type LinearIssueChange,
} from './LinearCallbackEvents'

const families = [
	[['title'], LinearIssueTitleChanged],
	[['description', 'descriptionData'], LinearIssueDescriptionChanged],
	[['state', 'stateId', 'startedAt', 'completedAt', 'canceledAt'], LinearIssueStatusChange],
	[['priority', 'priorityLabel'], LinearIssuePriorityChanged],
	[['labels', 'labelIds'], LinearIssueLabelsChanged],
	[['assignee', 'assigneeId'], LinearIssueAssigneeChanged],
	[['delegate', 'delegateId'], LinearIssueDelegateChanged],
	[['project', 'projectId'], LinearIssueProjectChanged],
	[['projectMilestone', 'projectMilestoneId'], LinearIssueMilestoneChanged],
	[['cycle', 'cycleId'], LinearIssueCycleChanged],
	[['team', 'teamId', 'previousIdentifiers'], LinearIssueTeamChanged],
	[['parentId', 'subIssueSortOrder'], LinearIssueParentChanged],
	[['estimate'], LinearIssueEstimateChanged],
	[['dueDate'], LinearIssueDueDateChanged],
	[['subscriberIds'], LinearIssueSubscribersChanged],
	[['archivedAt', 'trashed'], LinearIssueArchiveChanged],
	[['triagedAt', 'startedTriageAt', 'snoozedUntilAt'], LinearIssueLifecycleChanged],
	[['releases'], LinearIssueReleasesChanged],
	[['slaStartedAt', 'slaBreachesAt', 'slaType'], LinearIssueSlaChanged],
] as const

export const normalizeLinearIssueChanges = (updatedFrom: Readonly<Record<string, unknown>>) => {
	const changes: Array<LinearIssueChange> = []
	const otherChanges: Record<string, Schema.Json> = {}
	for (const [field, previous] of Object.entries(updatedFrom)) {
		const family = families.find(([fields]) => (fields as ReadonlyArray<string>).includes(field))
		if (family === undefined) otherChanges[field] = previous as Schema.Json
		else changes.push(family[1].make({ previous: previous as Schema.Json }) as LinearIssueChange)
	}
	return { changes, otherChanges }
}
