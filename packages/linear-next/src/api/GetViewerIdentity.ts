import { Redacted, Schema } from 'effect'

import { LinearOrganizationId, LinearUserId } from '../LinearIdentity'
import { linearGraphql } from './LinearGraphql'

const ViewerIdentityData = Schema.Struct({
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

export const getViewerIdentity = (credential: Redacted.Redacted<string>) =>
	linearGraphql({
		operation: 'viewer_identity',
		query,
		variables: {},
		credential,
		data: ViewerIdentityData,
	})
