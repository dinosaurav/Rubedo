import React from 'react';

export default function BlueprintDiagram() {
  return (
    <div style={{ padding: '2rem', border: '1px solid var(--border-color)', backgroundColor: 'var(--bg-color)', position: 'relative', marginTop: '2rem', marginBottom: '2rem' }}>
      <div className="blueprint-bg" style={{ position: 'absolute', top: 0, left: 0, right: 0, bottom: 0, opacity: 0.5, pointerEvents: 'none' }}></div>
      <svg width="100%" height="300" viewBox="0 0 800 300" xmlns="http://www.w3.org/2000/svg" style={{ position: 'relative', zIndex: 1 }}>
        <defs>
          <marker id="arrow" markerWidth="10" markerHeight="10" refX="9" refY="3" orient="auto" markerUnits="strokeWidth">
            <path d="M0,0 L0,6 L9,3 z" fill="var(--accent-primary)" />
          </marker>
        </defs>
        
        {/* Source Nodes */}
        <rect x="50" y="50" width="120" height="60" fill="none" stroke="var(--text-primary)" strokeWidth="2" />
        <text x="110" y="85" textAnchor="middle" fill="var(--text-primary)" fontFamily="monospace" fontSize="14">FolderSource</text>
        
        <rect x="50" y="150" width="120" height="60" fill="none" stroke="var(--text-primary)" strokeWidth="2" />
        <text x="110" y="185" textAnchor="middle" fill="var(--text-primary)" fontFamily="monospace" fontSize="14">CsvSource</text>
        
        {/* Pipeline / Engine */}
        <rect x="300" y="50" width="200" height="200" fill="none" stroke="var(--accent-primary)" strokeWidth="3" />
        <text x="400" y="80" textAnchor="middle" fill="var(--accent-primary)" fontWeight="bold" letterSpacing="2">RUBEDO ENGINE</text>
        
        <circle cx="400" cy="150" r="40" fill="none" stroke="var(--text-primary)" strokeWidth="2" strokeDasharray="5,5" />
        <text x="400" y="155" textAnchor="middle" fill="var(--text-primary)" fontSize="12">@step</text>
        
        {/* Connections */}
        <line x1="170" y1="80" x2="300" y2="100" stroke="var(--text-primary)" strokeWidth="2" markerEnd="url(#arrow)" />
        <line x1="170" y1="180" x2="300" y2="150" stroke="var(--text-primary)" strokeWidth="2" markerEnd="url(#arrow)" />
        <line x1="500" y1="150" x2="650" y2="150" stroke="var(--accent-primary)" strokeWidth="2" markerEnd="url(#arrow)" />
        
        {/* Ledger/Output */}
        <rect x="650" y="100" width="100" height="100" fill="none" stroke="var(--text-primary)" strokeWidth="2" />
        <line x1="650" y1="120" x2="750" y2="120" stroke="var(--text-primary)" strokeWidth="1" />
        <line x1="650" y1="140" x2="750" y2="140" stroke="var(--text-primary)" strokeWidth="1" />
        <line x1="650" y1="160" x2="750" y2="160" stroke="var(--text-primary)" strokeWidth="1" />
        <line x1="650" y1="180" x2="750" y2="180" stroke="var(--text-primary)" strokeWidth="1" />
        <text x="700" y="90" textAnchor="middle" fill="var(--text-primary)" fontSize="12" fontWeight="bold">Run Ledger</text>
        
        {/* Explanatory Annotations */}
        <text x="575" y="140" textAnchor="middle" fill="var(--accent-primary)" fontSize="10" fontFamily="monospace">addressable</text>
        <text x="575" y="170" textAnchor="middle" fill="var(--accent-primary)" fontSize="10" fontFamily="monospace">cache</text>
      </svg>
    </div>
  );
}
