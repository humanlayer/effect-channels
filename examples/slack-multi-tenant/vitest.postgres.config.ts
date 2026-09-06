import { defineConfig } from 'vite-plus'

export default defineConfig({
	envDir: false,
	test: {
		include: ['test-backends/**/*.postgres.test.ts'],
		testTimeout: 30_000,
		hookTimeout: 30_000,
	},
})
