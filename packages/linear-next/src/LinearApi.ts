import { Context } from 'effect'

/** Provider-native Linear operations live behind this service. Phase 1 establishes the injectable boundary. */
export class LinearApi extends Context.Service<LinearApi, {}>()(
	'@humanlayer/channels-linear-next/LinearApi',
) {}
