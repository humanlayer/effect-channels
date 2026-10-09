import { Schema } from 'effect'

/**
 * Parameters for a tool that takes none: an object with no fields. `Schema.Struct({})` will not do: it accepts
 * any value but null, which is not an object schema, and OpenAI rejects a tool whose parameters are not one.
 */
export const NoParameters = Schema.Record(Schema.String, Schema.Never)
