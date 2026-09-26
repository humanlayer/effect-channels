import { Schema } from 'effect'

import { LinearOrganizationId, LinearUserId } from '../LinearIdentity'
import { linearGraphql } from './LinearGraphql'

/** The viewer query takes no variables, so its variables object must be empty. */
export const GetViewerIdentityVariables = Schema.Record(Schema.String, Schema.Never)

const GetViewerIdentityResponse = Schema.Struct({
	viewer: Schema.Struct({
		id: LinearUserId,
		organization: Schema.Struct({ id: LinearOrganizationId }),
	}),
})

const query = `query LinearViewerIdentity {
  viewer {
    id
    organization { id }
  }
}`

export const getViewerIdentity = () =>
	linearGraphql({
		operation: 'viewer_identity',
		query,
		variables: GetViewerIdentityVariables,
		input: {},
		response: GetViewerIdentityResponse,
	})
