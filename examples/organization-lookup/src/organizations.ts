import { GitHubOrganizations } from '@humanlayer/channels-github'
import { SlackOrganizations } from '@humanlayer/channels-slack'
import { Effect, Layer } from 'effect'

export const slackOrganizations = SlackOrganizations.layer(({ workspaceId }) =>
	Effect.sync(() => {
		if (workspaceId === 'T_NORTH') return { organizationId: 'north' }
		if (workspaceId === 'T_SOUTH') return { organizationId: 'south' }
		return null
	}),
)

export const githubOrganizations = GitHubOrganizations.layer(({ installationId }) =>
	Effect.sync(() => {
		if (installationId === 100) return { organizationId: 'north' }
		if (installationId === 200) return { organizationId: 'south' }
		return null
	}),
)

export const organizations = Layer.merge(slackOrganizations, githubOrganizations)
