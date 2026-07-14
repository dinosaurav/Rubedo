import React from 'react'

// A styled, branded SVG of the join -> expand -> reduce diamond, mirroring
// the real p.describe(format="ascii") output. Hardcoded hex (not CSS vars) so
// it renders identically in any context (e.g. an <img>, an OG preview).
const RED = '#FF0033'
const DARK = '#1a1a1a'
const WHITE = '#ffffff'

function Node({ x, y, w, h, name, tag }) {
  return (
    <g>
      <rect
        x={x} y={y} width={w} height={h} rx={0} ry={0}
        fill={WHITE} stroke={DARK} strokeWidth={1.5}
        style={{ filter: 'drop-shadow(3px 3px 0 rgba(255,0,51,0.16))' }}
      />
      <text
        x={x + w / 2} y={y + h / 2 - 1} textAnchor="middle"
        fill={DARK} fontFamily="ui-monospace, 'SF Mono', Menlo, monospace"
        fontSize={13} fontWeight={700}
      >
        {name}
      </text>
      <text
        x={x + w / 2} y={y + h / 2 + 15} textAnchor="middle"
        fill={RED} fontFamily="ui-monospace, 'SF Mono', Menlo, monospace"
        fontSize={10} fontWeight={700}
      >
        [{tag}]
      </text>
    </g>
  )
}

export default function DiamondDag() {
  return (
    <svg
      width="100%"
      viewBox="0 0 520 420"
      xmlns="http://www.w3.org/2000/svg"
      style={{ display: 'block', height: 'auto' }}
      role="img"
      aria-label="Rubedo pipeline diamond: two sources join, fan out into per-article lanes, and fold back into a per-region digest."
    >
      <defs>
        <pattern id="diamond-grid" width="22" height="22" patternUnits="userSpaceOnUse">
          <path d="M22 0 H0 V22" fill="none" stroke={RED} strokeWidth="0.5" opacity="0.06" />
        </pattern>
        <marker id="diamond-arrow" markerWidth="10" markerHeight="10" refX="9" refY="3" orient="auto" markerUnits="strokeWidth">
          <path d="M0,0 L0,6 L9,3 z" fill={RED} />
        </marker>
      </defs>

      <rect x="0" y="0" width="520" height="420" fill="url(#diamond-grid)" />

      {/* connectors (drawn first so nodes sit on top) */}
      <g stroke={RED} strokeWidth={1.75} fill="none">
        {/* feeds -> feed */}
        <line x1="111" y1="70" x2="132" y2="101" markerEnd="url(#diamond-arrow)" />
        {/* publishers -> publisher */}
        <line x1="409" y1="70" x2="388" y2="101" markerEnd="url(#diamond-arrow)" />
        {/* feed -> feed_meta (diagonal, lands on top edge left of center) */}
        <line x1="132" y1="146" x2="235" y2="183" markerEnd="url(#diamond-arrow)" />
        {/* publisher -> feed_meta (diagonal, lands on top edge right of center) */}
        <line x1="388" y1="146" x2="285" y2="183" markerEnd="url(#diamond-arrow)" />
        {/* feed_meta -> articles */}
        <line x1="260" y1="232" x2="260" y2="265" markerEnd="url(#diamond-arrow)" />
        {/* articles -> digest */}
        <line x1="260" y1="314" x2="260" y2="347" markerEnd="url(#diamond-arrow)" />
      </g>

      {/* nodes */}
      <Node x={36} y={24} w={150} h={46} name="feeds" tag="expand" />
      <Node x={334} y={24} w={150} h={46} name="publishers" tag="expand" />
      <Node x={72} y={104} w={120} h={42} name="feed" tag="map" />
      <Node x={328} y={104} w={120} h={42} name="publisher" tag="map" />
      <Node x={180} y={186} w={160} h={46} name="feed_meta" tag="join" />
      <Node x={185} y={268} w={150} h={46} name="articles" tag="expand" />
      <Node x={210} y={350} w={100} h={42} name="digest" tag="reduce" />
    </svg>
  )
}
