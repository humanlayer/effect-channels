import { Array as Arr, Option, type Schema, Struct } from 'effect'

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
import type { LinearIssueUpdatedFrom } from './LinearWebhookEventSchemas'

type IssueChangeEvent<Key extends keyof LinearIssueUpdatedFrom> = {
	readonly fields: { readonly previous: { readonly fields: { readonly [K in Key]: Schema.Top } } }
	readonly make: (input: { readonly previous: Pick<LinearIssueUpdatedFrom, Key> }) => LinearIssueChange
}

const issueChange =
	<Key extends keyof LinearIssueUpdatedFrom>(event: IssueChangeEvent<Key>) =>
	(updatedFrom: LinearIssueUpdatedFrom) => {
		const previous = Struct.pick(updatedFrom, Struct.keys(event.fields.previous.fields))
		return Arr.isReadonlyArrayEmpty(Struct.keys(previous)) ? Option.none() : Option.some(event.make({ previous }))
	}

const issueChanges = [
	issueChange(LinearIssueTitleChanged),
	issueChange(LinearIssueDescriptionChanged),
	issueChange(LinearIssueStatusChange),
	issueChange(LinearIssuePriorityChanged),
	issueChange(LinearIssueLabelsChanged),
	issueChange(LinearIssueAssigneeChanged),
	issueChange(LinearIssueDelegateChanged),
	issueChange(LinearIssueProjectChanged),
	issueChange(LinearIssueMilestoneChanged),
	issueChange(LinearIssueCycleChanged),
	issueChange(LinearIssueTeamChanged),
	issueChange(LinearIssueParentChanged),
	issueChange(LinearIssueEstimateChanged),
	issueChange(LinearIssueDueDateChanged),
	issueChange(LinearIssueSubscribersChanged),
	issueChange(LinearIssueArchiveChanged),
	issueChange(LinearIssueLifecycleChanged),
	issueChange(LinearIssueReleasesChanged),
	issueChange(LinearIssueSlaChanged),
]

/** One change event per field group that the update touched, each carrying the previous values Linear sent for that group. */
export const normalizeLinearIssueChanges = (updatedFrom: LinearIssueUpdatedFrom): Array<LinearIssueChange> =>
	Arr.getSomes(issueChanges.map((change) => change(updatedFrom)))
