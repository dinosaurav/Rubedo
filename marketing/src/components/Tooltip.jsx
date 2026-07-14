import React, { useState } from 'react'

// Paperclip-punk tooltip: dashed underline cue, hover reveals a
// sharp-bordered annotation box. Packs explanation next to the concept.
export default function Tooltip({ children, text }) {
  const [show, setShow] = useState(false)
  return (
    <span
      className="tip"
      onMouseEnter={() => setShow(true)}
      onMouseLeave={() => setShow(false)}
      onFocus={() => setShow(true)}
      onBlur={() => setShow(false)}
      tabIndex={0}
    >
      <span className="term">{children}</span>
      <span className="term-dot" aria-hidden="true">?</span>
      {show && <span className="tip-card" role="tooltip">{text}</span>}
    </span>
  )
}
