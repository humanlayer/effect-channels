import { NodeRuntime } from '@effect/platform-node'
import { SlackState } from '@humanlayer/channels-slack'
import { Effect, Layer } from 'effect'

import { seed } from '../src/seed.js'
import { storage } from '../src/storage.js'

NodeRuntime.runMain(seed.pipe(Effect.provide(SlackState.layer.pipe(Layer.provide(storage)))))
