import { defineConfig } from 'vite-plus'

export default defineConfig({
	envDir: false,
	test: {
		include: ['packages/sql/test-backends/*.postgres.test.ts'],
		fileParallelism: false,
		passWithNoTests: false,
	},
})
