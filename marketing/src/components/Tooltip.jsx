import React from 'react';

export default function Tooltip({ text, children }) {
  return (
    <span className="tooltip-container">
      {children}
      <span className="tooltip-content">{text}</span>
    </span>
  );
}
