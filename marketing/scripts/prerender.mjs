// Injects server-rendered markup into dist/index.html so the landing page
// has real content in the initial HTML response — crawlers and agents that
// don't execute JS (many AI bots, curl, etc.) would otherwise see only the
// empty `<div id="root"></div>` shell. Runs after the client build and the
// throwaway SSR build (see package.json's `build` script); the SSR bundle
// is deleted once its markup has been extracted.
import fs from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

const rootDir = path.dirname(path.dirname(fileURLToPath(import.meta.url)))
const distDir = path.join(rootDir, 'dist')
const ssrDir = path.join(distDir, '.ssr')

const { render } = await import(path.join(ssrDir, 'entry-server.js'))
const appHtml = render()

const indexPath = path.join(distDir, 'index.html')
const template = fs.readFileSync(indexPath, 'utf-8')
if (!template.includes('<div id="root"></div>')) {
  throw new Error('prerender: expected an empty <div id="root"></div> in dist/index.html')
}
const html = template.replace('<div id="root"></div>', `<div id="root">${appHtml}</div>`)
fs.writeFileSync(indexPath, html)

fs.rmSync(ssrDir, { recursive: true, force: true })

console.log('prerender: injected server-rendered markup into dist/index.html')
