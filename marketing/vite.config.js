import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

// https://vite.dev/config/
export default defineConfig(({ command }) => ({
  plugins: [react()],
// Served from the rubedo.run domain root (custom domain on GitHub Pages),
  // so the built artifact uses '/'. `command === 'serve'` is the `npm run dev`
  // path, which also serves at '/' with HMR — so both branches now agree.
  // If you ever drop the custom domain and go back to the un-customized GitHub
  // Pages project page, change this to '/Rubedo/' (and site_url in ../mkdocs.yml).
  base: '/',
  build: {
    // The MkDocs site builds separately into dist/docs/ (see site_dir in
    // ../mkdocs.yml) and is served at /docs/ under that base. Vite's default
    // `emptyOutDir: true` would wipe the whole outDir — including dist/docs/
    // — on every `vite build`, so it's disabled here. `npm run build` is
    // therefore non-destructive but non-cleaning: stale files from a
    // previous Vite build can linger in dist/ (outside dist/docs/). See
    // README.md in this directory for the recommended clean-build recipe.
    emptyOutDir: false,
  },
}))
