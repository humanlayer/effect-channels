import { Config, Layer, Redacted } from 'effect'
import * as HttpClient from 'effect/unstable/http/HttpClient'

import { LinearAuth } from '../src/index'
import { LinearApiLiveOptions, makeLinearApiLiveBase } from '../src/LinearApiLive'
import { LinearIssueId, LinearOrganizationId, LinearTeamId, LinearUserId } from '../src/LinearIdentity'
import { LinearIssueRef } from '../src/LinearModels'

export const organizationId = LinearOrganizationId.make('org-1')
export const appUserId = LinearUserId.make('app-1')
export const issue = LinearIssueRef.make({
	organizationId,
	teamId: LinearTeamId.make('team-1'),
	issueId: LinearIssueId.make('issue-1'),
})
export const options = LinearApiLiveOptions.make({
	auth: LinearAuth.developerToken({ token: Config.succeed(Redacted.make('token-never-log')) }),
	organizationId: Config.succeed(organizationId),
	appUserId: Config.succeed(appUserId),
})
export const apiLayer = (http: HttpClient.HttpClient) =>
	makeLinearApiLiveBase(options).pipe(Layer.provide(Layer.succeed(HttpClient.HttpClient, http)))

export const viewer = { data: { viewer: { id: appUserId, organization: { id: organizationId } } } }
export const issueJson = {
	id: issue.issueId,
	identifier: 'CORE-1',
	title: 'Smoke',
	description: null,
	priority: 2,
	url: 'https://linear.app/issue/CORE-1',
	team: { id: issue.teamId },
	state: { id: 'state-1', name: 'Todo', type: 'unstarted' },
	labels: { nodes: [] },
	assignee: null,
	delegate: null,
}
