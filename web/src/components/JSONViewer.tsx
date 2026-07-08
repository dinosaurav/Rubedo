import { useState } from 'react';
import { ChevronRight, ChevronDown } from 'lucide-react';

interface JSONViewerProps {
  data: any;
  level?: number;
  isLast?: boolean;
}

export default function JSONViewer({ data, level = 0, isLast = true }: JSONViewerProps) {
  const [expanded, setExpanded] = useState(level < 2);

  if (data === null) {
    return <span style={{ color: '#ef4444' }}>null{isLast ? '' : ','}</span>;
  }
  
  if (typeof data === 'boolean') {
    return <span style={{ color: '#3b82f6' }}>{data ? 'true' : 'false'}{isLast ? '' : ','}</span>;
  }
  
  if (typeof data === 'number') {
    return <span style={{ color: '#22c55e' }}>{data}{isLast ? '' : ','}</span>;
  }
  
  if (typeof data === 'string') {
    return <span style={{ color: '#f59e0b', whiteSpace: 'pre-wrap', wordBreak: 'break-word' }}>"{data}"{isLast ? '' : ','}</span>;
  }

  if (Array.isArray(data)) {
    if (data.length === 0) return <span>[]{isLast ? '' : ','}</span>;
    
    return (
      <span style={{ display: 'inline-flex', flexDirection: 'column' }}>
        <span 
          onClick={() => setExpanded(!expanded)} 
          style={{ cursor: 'pointer', display: 'inline-flex', alignItems: 'center' }}
        >
          {expanded ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
          <span>[</span>
          {!expanded && <span style={{ color: 'var(--text-muted)' }}> ... ]{isLast ? '' : ','}</span>}
        </span>
        
        {expanded && (
          <>
            <div style={{ paddingLeft: '1.5rem', borderLeft: '1px solid var(--border-color)', marginLeft: '6px' }}>
              {data.map((item, i) => (
                <div key={i}>
                  <JSONViewer data={item} level={level + 1} isLast={i === data.length - 1} />
                </div>
              ))}
            </div>
            <span style={{ marginLeft: '6px' }}>]{isLast ? '' : ','}</span>
          </>
        )}
      </span>
    );
  }

  if (typeof data === 'object') {
    const keys = Object.keys(data);
    if (keys.length === 0) return <span>{"{}"}{isLast ? '' : ','}</span>;
    
    return (
      <span style={{ display: 'inline-flex', flexDirection: 'column' }}>
        <span 
          onClick={() => setExpanded(!expanded)} 
          style={{ cursor: 'pointer', display: 'inline-flex', alignItems: 'center' }}
        >
          {expanded ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
          <span>{"{"}</span>
          {!expanded && <span style={{ color: 'var(--text-muted)' }}> ... {"}"}{isLast ? '' : ','}</span>}
        </span>
        
        {expanded && (
          <>
            <div style={{ paddingLeft: '1.5rem', borderLeft: '1px solid var(--border-color)', marginLeft: '6px' }}>
              {keys.map((key, i) => (
                <div key={key}>
                  <span style={{ color: '#a855f7' }}>"{key}"</span>
                  <span>: </span>
                  <JSONViewer data={data[key]} level={level + 1} isLast={i === keys.length - 1} />
                </div>
              ))}
            </div>
            <span style={{ marginLeft: '6px' }}>{"}"}{isLast ? '' : ','}</span>
          </>
        )}
      </span>
    );
  }

  return <span>{String(data)}{isLast ? '' : ','}</span>;
}
