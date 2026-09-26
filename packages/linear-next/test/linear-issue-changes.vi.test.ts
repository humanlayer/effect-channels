import { describe, it } from '@effect/vitest'
import { Effect } from 'effect'

import { normalizeLinearIssueChanges } from '../src/LinearIssueChanges'

describe('Linear issue changes', () => {
	it.effect('emits one typed change per changed field group with its previous values', ({ expect }) =>
		Effect.sync(() => {
			expect(
				normalizeLinearIssueChanges({
					title: 'Old title',
					stateId: 'old-state',
					startedAt: null,
					labelIds: ['old-label'],
				}),
			).toEqual([
				{ _tag: 'LinearIssueTitleChanged', previous: { title: 'Old title' } },
				{ _tag: 'LinearIssueStatusChanged', previous: { stateId: 'old-state', startedAt: null } },
				{ _tag: 'LinearIssueLabelsChanged', previous: { labelIds: ['old-label'] } },
			])
		}),
	)

	it.effect('emits no change when no modeled field changed', ({ expect }) =>
		Effect.sync(() => {
			expect(normalizeLinearIssueChanges({})).toEqual([])
		}),
	)
})
