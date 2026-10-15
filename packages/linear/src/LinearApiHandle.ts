/**
 * A second handle on the `LinearApi` service. It shares the service's key, so it reads the same slot in
 * context. Linear files and resources read the service through it: importing `LinearApi` itself would form an
 * import cycle, since `LinearApi`'s constructor builds those files and resources.
 */
import { Context } from 'effect'

import type { LinearApi } from './LinearApi'
import type { LinearApiOperations } from './LinearApiContract'

export const linearApiKey = '@humanlayer/channels-linear/LinearApi'

export const LinearApiHandle = Context.Service<LinearApi, LinearApiOperations>(linearApiKey)
