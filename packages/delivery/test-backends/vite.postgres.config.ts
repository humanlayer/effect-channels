import { defineConfig } from 'vite-plus'

export default defineConfig({
	envDir: false,
	test: {
		include: ['packages/delivery/test-backends/*.postgres.test.ts'],
		fileParallelism: false,
		passWithNoTests: false,
	},
})
