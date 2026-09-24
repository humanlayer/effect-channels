import { describe, it } from '@effect/vitest'
import { Effect } from 'effect'

import { normalizeLinearIssueChanges } from '../src/LinearIssueChanges'

describe('Linear issue changes', () => {
	it.effect('keeps one typed change per changed field and bounds unknown fields', ({ expect }) =>
		Effect.sync(() => {
			const normalized = normalizeLinearIssueChanges({
				title: 'Old title',
				stateId: 'old-state',
				labelIds: ['old-label'],
				futureProviderField: { old: true },
			})
			expect(normalized.changes.map((change) => change._tag)).toEqual([
				'LinearIssueTitleChanged',
				'LinearIssueStatusChanged',
				'LinearIssueLabelsChanged',
			])
			expect(normalized.otherChanges).toEqual({ futureProviderField: { old: true } })
		}),
	)
})
