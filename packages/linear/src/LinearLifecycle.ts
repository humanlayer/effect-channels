import { Schema } from 'effect'

import { LinearOrganizationId, LinearTeamId, LinearWebhookDeliveryId } from './LinearIdentity'

export const LinearLifecycleDisposition = Schema.Literals(['observed_team_access_change', 'observed_revocation'])
export type LinearLifecycleDisposition = typeof LinearLifecycleDisposition.Type

export const LinearTeamAccessChanged = Schema.TaggedStruct('LinearTeamAccessChanged', {
	deliveryId: LinearWebhookDeliveryId,
	organizationId: LinearOrganizationId,
	addedTeamIds: Schema.Array(LinearTeamId),
	removedTeamIds: Schema.Array(LinearTeamId),
	disposition: Schema.Literal('observed_team_access_change'),
})

export const LinearInstallationRevoked = Schema.TaggedStruct('LinearInstallationRevoked', {
	deliveryId: LinearWebhookDeliveryId,
	organizationId: LinearOrganizationId,
	disposition: Schema.Literal('observed_revocation'),
})
