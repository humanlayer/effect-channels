import { Config, Effect, Layer, Redacted, Schema } from 'effect'
import * as HttpClient from 'effect/unstable/http/HttpClient'
import * as HttpClientRequest from 'effect/unstable/http/HttpClientRequest'

import { linearGraphqlRequest } from '../src/api/LinearGraphql'
import { LinearAuth } from '../src/index'
import type { LinearApi } from '../src/LinearApi'
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

/** Decodes a GraphQL body a fake Linear server received with the production request Schema, rejecting undeclared keys. */
export const decodeGraphqlRequest =
	<V>(variables: Schema.Codec<V, unknown, never, never>) =>
	(request: HttpClientRequest.HttpClientRequest) =>
		HttpClientRequest.toWeb(request).pipe(
			Effect.flatMap((web) => Effect.promise(() => web.text())),
			Effect.flatMap(
				Schema.decodeUnknownEffect(Schema.fromJsonString(linearGraphqlRequest(variables)), {
					onExcessProperty: 'error',
				}),
			),
			Effect.orDie,
		)

/** A complete LinearApi whose every method dies; tests override the methods they exercise. */
export const unusedLinearApi: LinearApi['Service'] = {
	createAgentActivity: () => Effect.die('unused LinearApi method'),
	updateAgentSession: () => Effect.die('unused LinearApi method'),
	getIssue: () => Effect.die('unused LinearApi method'),
	updateIssue: () => Effect.die('unused LinearApi method'),
	listAssignableUsers: () => Effect.die('unused LinearApi method'),
	listAppUsers: () => Effect.die('unused LinearApi method'),
	getUser: () => Effect.die('unused LinearApi method'),
	listIssueComments: () => Effect.die('unused LinearApi method'),
	listIssueAttachments: () => Effect.die('unused LinearApi method'),
	createComment: () => Effect.die('unused LinearApi method'),
	updateComment: () => Effect.die('unused LinearApi method'),
	deleteComment: () => Effect.die('unused LinearApi method'),
	createReaction: () => Effect.die('unused LinearApi method'),
	deleteReaction: () => Effect.die('unused LinearApi method'),
	createAttachment: () => Effect.die('unused LinearApi method'),
	updateAttachment: () => Effect.die('unused LinearApi method'),
	deleteAttachment: () => Effect.die('unused LinearApi method'),
	uploadFile: () => Effect.die('unused LinearApi method'),
	uploadAttachment: () => Effect.die('unused LinearApi method'),
	downloadFile: () => Effect.die('unused LinearApi method'),
	downloadFileBytes: () => Effect.die('unused LinearApi method'),
}
