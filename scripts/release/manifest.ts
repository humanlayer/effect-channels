import { join } from 'node:path'

export const root = join(import.meta.dirname, '../..')
export const stage = join(root, '.release')
export const scope = '@humanlayer'
export const repository = { type: 'git', url: 'git+https://github.com/humanlayer/effect-channels.git' }

/** Every directory in `packages/`, in publish order: a package comes after the packages it depends on. */
export const libraries = ['delivery', 'slack', 'github', 'linear', 'sql', 'redis', 'alchemy-cloudflare'] as const

export const versionPattern = /^\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?(?:\+[0-9A-Za-z.-]+)?$/

export async function json<T extends object = Record<string, unknown>>(path: string): Promise<T> {
	return Bun.file(path).json()
}
