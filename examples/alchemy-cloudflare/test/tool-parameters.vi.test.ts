import { describe, it } from '@effect/vitest'
import { DeliveryPlan } from '@humanlayer/channels-delivery'
import { Schema } from 'effect'
import { toCodecOpenAI } from 'effect/ai/OpenAiStructuredOutput'

import { NoParameters } from '../src/ToolParameters'

describe('tool parameters', () => {
	it('gives OpenAI an object with no fields for a tool that takes none', ({ expect }) => {
		expect(toCodecOpenAI(NoParameters).jsonSchema).toEqual({
			type: 'object',
			properties: {},
			additionalProperties: false,
		})
		expect(Schema.decodeSync(NoParameters)({})).toEqual({})
	})

	it('gives OpenAI an object for the plan', ({ expect }) => {
		expect(toCodecOpenAI(DeliveryPlan).jsonSchema).toMatchObject({ type: 'object' })
	})
})
