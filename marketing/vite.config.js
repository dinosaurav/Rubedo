import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// https://vite.dev/config/
export default defineConfig({
  plugins: [react()],
  build: {
    // The MkDocs site builds separately into dist/docs/ (see site_dir in
    // ../mkdocs.yml) and is served at /docs/ on rubedo.dev. Vite's default
    // `emptyOutDir: true` would wipe the whole outDir — including dist/docs/
    // — on every `vite build`, so it's disabled here. `npm run build` is
    // therefore non-destructive but non-cleaning: stale files from a
    // previous Vite build can linger in dist/ (outside dist/docs/). See
    // README.md in this directory for the recommended clean-build recipe.
    emptyOutDir: false,
  },
})
