import { NodeRuntime } from '@effect/platform-node'
import { SlackState } from '@humanlayer/channels-slack'
import { Effect, Layer } from 'effect'

import { seed } from '../src/seed.ts'
import { storage } from '../src/storage.ts'

NodeRuntime.runMain(seed.pipe(Effect.provide(SlackState.layer.pipe(Layer.provide(storage)))))
