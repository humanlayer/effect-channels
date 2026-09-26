import { describe, it } from '@effect/vitest'
import { Effect, Ref, Schema } from 'effect'
import { HttpClient, HttpClientResponse } from 'effect/unstable/http'

import { GetIssueVariables } from '../src/api/GetIssue'
import { GetViewerIdentityVariables } from '../src/api/GetViewerIdentity'
import { linearGraphqlRequest } from '../src/api/LinearGraphql'
import { ListAppUsersVariables } from '../src/api/ListAppUsers'
import { ListAssignableUsersVariables } from '../src/api/ListAssignableUsers'
import { LinearApi } from '../src/LinearApi'
import { LinearIssueRef } from '../src/LinearModels'
import { apiLayer, decodeGraphqlRequest, issue, issueJson, viewer } from './api-test-fixtures'

const AppUsersVariables = Schema.Union([ListAppUsersVariables, GetIssueVariables, GetViewerIdentityVariables])
const AppUsersRequest = linearGraphqlRequest(AppUsersVariables)
const decodeAppUsersRequest = decodeGraphqlRequest(AppUsersVariables)
const decodeAssignableUsersRequest = decodeGraphqlRequest(
	Schema.Union([ListAssignableUsersVariables, GetViewerIdentityVariables]),
)

const app = (id: string, direct: boolean, publicAccess = false) => ({
	id,
	name: id,
	email: null,
	active: true,
	app: true,
	isAssignable: true,
	canAccessAnyPublicTeam: publicAccess,
	teams: { nodes: direct ? [{ id: issue.teamId }] : [] },
})

describe('Linear user directories', () => {
	it.effect('uses root users, explicit visibility, and a bounded membership lookup', ({ expect }) =>
		Effect.gen(function* () {
			const appQuery = yield* Ref.make<typeof AppUsersRequest.Type | null>(null)
			const http = HttpClient.make((request) =>
				Effect.gen(function* () {
					const body = yield* decodeAppUsersRequest(request)
					if (body.query.includes('viewer')) return HttpClientResponse.fromWeb(request, Response.json(viewer))
					if (body.query.includes('query LinearIssue('))
						return HttpClientResponse.fromWeb(request, Response.json({ data: { issue: issueJson } }))
					yield* Ref.set(appQuery, body)
					return HttpClientResponse.fromWeb(
						request,
						Response.json({
							data: {
								issue: { team: { id: issue.teamId, visibility: 'restricted' } },
								users: {
									nodes: [app('direct', true), app('public-only', false, true)],
									pageInfo: { endCursor: 'next', hasNextPage: true },
								},
							},
						}),
					)
				}),
			)
			const nullable = LinearIssueRef.make({ ...issue, teamId: null })
			const page = yield* Effect.flatMap(LinearApi, (api) =>
				api.listAppUsers({ issue: nullable, first: 25, after: 'cursor', query: 'bot' }),
			).pipe(Effect.provide(apiLayer(http)))
			expect(page.users.map(({ id }) => id)).toEqual(['direct'])
			const request = yield* Ref.get(appQuery)
			expect(request?.query).toContain(' users(first: $first')
			expect(request?.query).not.toContain('organization { users')
			expect(request?.query).toContain('visibility')
			expect(request?.query).toContain('teams(first: 1, filter: { id: { eq: $teamId } })')
			expect(request?.variables).toEqual({
				issueId: issue.issueId,
				teamId: issue.teamId,
				first: 25,
				after: 'cursor',
				query: 'bot',
			})
		}),
	)

	it.effect('allows all-public app access only for an explicitly public team', ({ expect }) =>
		Effect.gen(function* () {
			const http = HttpClient.make((request) =>
				Effect.gen(function* () {
					const body = yield* decodeAppUsersRequest(request)
					if (body.query.includes('viewer')) return HttpClientResponse.fromWeb(request, Response.json(viewer))
					if (body.query.includes('query LinearIssue('))
						return HttpClientResponse.fromWeb(request, Response.json({ data: { issue: issueJson } }))
					return HttpClientResponse.fromWeb(
						request,
						Response.json({
							data: {
								issue: { team: { id: issue.teamId, visibility: 'public' } },
								users: {
									nodes: [app('public-only', false, true)],
									pageInfo: { endCursor: null, hasNextPage: false },
								},
							},
						}),
					)
				}),
			)
			const page = yield* Effect.flatMap(LinearApi, (api) => api.listAppUsers({ issue })).pipe(
				Effect.provide(apiLayer(http)),
			)
			expect(page.users.map(({ id }) => id)).toEqual(['public-only'])
		}),
	)

	it.effect('resolves nullable team refs inside the assignable-users query', ({ expect }) =>
		Effect.gen(function* () {
			const http = HttpClient.make((request) =>
				Effect.gen(function* () {
					const body = yield* decodeAssignableUsersRequest(request)
					if (body.query.includes('viewer')) return HttpClientResponse.fromWeb(request, Response.json(viewer))
					expect(body.query).toContain('issue(id: $issueId)')
					expect(body.query).not.toContain('$teamId')
					expect(body.variables).toEqual({ issueId: issue.issueId, first: 50 })
					return HttpClientResponse.fromWeb(
						request,
						Response.json({
							data: {
								issue: {
									team: {
										id: issue.teamId,
										members: { nodes: [], pageInfo: { endCursor: null, hasNextPage: false } },
									},
								},
							},
						}),
					)
				}),
			)
			const nullable = LinearIssueRef.make({ ...issue, teamId: null })
			yield* Effect.flatMap(LinearApi, (api) => api.listAssignableUsers({ issue: nullable })).pipe(
				Effect.provide(apiLayer(http)),
			)
		}),
	)
})
