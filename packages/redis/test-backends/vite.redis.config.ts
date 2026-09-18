import { defineConfig } from 'vite-plus'

export default defineConfig({
	envDir: false,
	test: {
		include: ['packages/redis/test-backends/*.redis.test.ts'],
		fileParallelism: false,
		passWithNoTests: false,
	},
})
