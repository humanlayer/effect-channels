import { Schema } from 'effect'

import { LinearOrganizationId, LinearUserId } from '../LinearIdentity'
import { linearGraphql } from './LinearGraphql'

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
		variables: {},
		response: GetViewerIdentityResponse,
	})
