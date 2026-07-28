import { StrictMode } from 'react'
import { hydrateRoot } from 'react-dom/client'
import './index.css'
import App from './App.jsx'

// The build prerenders index.html (see scripts/prerender.mjs), so the root
// already has server-rendered markup — hydrate over it instead of
// clobbering it with a fresh client render.
hydrateRoot(
  document.getElementById('root'),
  <StrictMode>
    <App />
  </StrictMode>,
)
