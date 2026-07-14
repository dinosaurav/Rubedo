import React from 'react'
import { Highlight, Prism } from 'prism-react-renderer'

// Paperclip-punk syntax theme: red accents on warm white,
// sharp token colors that match the blueprint aesthetic.
const paperclipTheme = {
  plain: {
    color: '#1a1a1a',
    backgroundColor: '#faf9f7',
  },
  styles: [
    { types: ['comment'], style: { color: '#9a9a9a', fontStyle: 'italic' } },
    { types: ['keyword', 'builtin'], style: { color: '#FF0033', fontWeight: 700 } },
    { types: ['decorator', 'annotation'], style: { color: '#FF0033' } },
    { types: ['string', 'char'], style: { color: '#0a8f4f' } },
    { types: ['number', 'boolean'], style: { color: '#b25a00' } },
    { types: ['function'], style: { color: '#1a1a1a', fontWeight: 700 } },
    { types: ['class-name', 'maybe-class-name'], style: { color: '#1d4ed8' } },
    { types: ['operator', 'punctuation'], style: { color: '#6b6b6b' } },
    { types: ['variable', 'constant'], style: { color: '#1a1a1a' } },
    { types: ['property'], style: { color: '#8B0000' } },
    { types: ['parameter'], style: { color: '#1a1a1a' } },
  ],
}

export default function CodeBlock({ code, language = 'python', className = '' }) {
  // For plain-text blocks (ASCII art, stats), skip highlighting entirely.
  if (language === 'text') {
    return (
      <pre className={`pre-block ${className}`}>
        {code}
      </pre>
    )
  }

  return (
    <Highlight theme={paperclipTheme} code={code.trim()} language={language} prism={Prism}>
      {({ tokens, getLineProps, getTokenProps }) => (
        <pre className={`pre-block ${className}`}>
          {tokens.map((line, i) => {
            const lineProps = getLineProps({ line })
            return (
              <div key={i} {...lineProps} className={lineProps.className}>
                {line.map((token, key) => {
                  const tokenProps = getTokenProps({ token })
                  return <span key={key} {...tokenProps} />
                })}
              </div>
            )
          })}
        </pre>
      )}
    </Highlight>
  )
}
