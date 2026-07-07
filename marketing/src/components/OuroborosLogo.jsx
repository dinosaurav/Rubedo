import React from 'react';

export default function OuroborosLogo({ size = 48, color = 'var(--accent-primary)' }) {
  return (
    <svg width={size} height={size} viewBox="0 0 100 100" fill="none" stroke={color} strokeWidth="6" xmlns="http://www.w3.org/2000/svg" style={{ display: 'block' }}>
      {/* Outer broken circle */}
      <path d="M 50 10 A 40 40 0 1 1 10 50 A 40 40 0 0 1 45 10.3" strokeLinecap="square" />
      {/* Arrow head pointing into the tail */}
      <polygon points="45,2 60,10 45,18" fill={color} stroke="none" />
      {/* Inner geometric shapes for technical feel */}
      <circle cx="50" cy="50" r="20" stroke={color} strokeWidth="2" strokeDasharray="4 4" />
      <line x1="50" y1="30" x2="50" y2="70" stroke={color} strokeWidth="2" />
      <line x1="30" y1="50" x2="70" y2="50" stroke={color} strokeWidth="2" />
    </svg>
  );
}
