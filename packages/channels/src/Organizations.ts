import { Context, Effect, Layer, Option } from 'effect'

import type { OrganizationStoreError } from './Errors.ts'
import type { OrganizationLookup } from './Operations.ts'
import { OrgId } from './Schema.ts'

export class Organizations extends Context.Service<
	Organizations,
	{
		readonly resolve: (input: OrganizationLookup) => Effect.Effect<Option.Option<OrgId>, OrganizationStoreError>
	}
>()('channels/Organizations') {
	static readonly layerDefault = Layer.succeed(
		Organizations,
		Organizations.of({ resolve: () => Effect.succeed(Option.some(OrgId.make('default'))) }),
	)

	static make(resolve: (input: OrganizationLookup) => Effect.Effect<Option.Option<OrgId>, OrganizationStoreError>) {
		return Layer.succeed(Organizations, Organizations.of({ resolve }))
	}
}
