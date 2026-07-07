// A curated palette for communities
const colors = [
  '#FF6B6B', '#4ECDC4', '#45B7D1', '#FDCB6E', '#6C5CE7',
  '#A8E6CF', '#FD79A8', '#00B894', '#E17055', '#0984E3',
  '#D63031', '#E84393', '#00CEC9', '#B2BEC3', '#FFEAA7'
];

document.addEventListener('DOMContentLoaded', () => {
  const loading = document.getElementById('loading');
  const panel = document.getElementById('side-panel');
  const closeBtn = document.getElementById('close-panel');
  
  // Panel Elements
  const pType = document.getElementById('node-type');
  const pId = document.getElementById('node-id');
  const pCommunity = document.getElementById('node-community');
  const pSummary = document.getElementById('node-summary');
  const pConnections = document.getElementById('node-connections');
  
  // UI Controls
  const searchInput = document.getElementById('node-search');
  const searchResults = document.getElementById('search-results');
  const viewSelect = document.getElementById('view-select');
  const labelSelect = document.getElementById('label-select');
  
  // Chat Elements
  const chatMessages = document.getElementById('chat-messages');
  const chatInput = document.getElementById('chat-input');
  const chatSend = document.getElementById('chat-send');
  
  let graphData = null;
  let filteredData = null;
  let prValues = [];
  let Graph = null;
  let hoverNode = null;
  let highlightedNodes = new Set();
  
  closeBtn.addEventListener('click', () => {
    panel.classList.remove('open');
  });

  // Re-render graph on label toggle
  labelSelect.addEventListener('change', () => {
    if (Graph) Graph.nodeRelSize(4); // Trigger a re-render
  });

  function getPercentile(val) {
    if (prValues.length <= 1) return 0;
    // Find index of first element >= val
    let idx = prValues.findIndex(v => v >= val);
    if (idx === -1) return 1.0;
    return idx / (prValues.length - 1);
  }

  fetch('/api/graph')
    .then(res => res.json())
    .then(data => {
      loading.classList.remove('active');
      const links = data.links || data.edges || [];
      graphData = { nodes: data.nodes, links: links };
      filteredData = { nodes: [...graphData.nodes], links: [...graphData.links] };
      
      // Calculate sorted pageranks for percentile mapping
      prValues = graphData.nodes.map(n => n.pagerank || 0).sort((a, b) => a - b);
      
      initGraph();
      setupSearch();
      setupViews();
      setupChat();
    })
    .catch(err => {
      console.error(err);
      loading.innerHTML = `<h2>Error loading graph. Make sure graph.json exists!</h2>`;
    });

  function initGraph() {
    Graph = ForceGraph()
      (document.getElementById('graph'))
      .graphData(filteredData)
      .nodeId('id')
      .nodeLabel('id')
      .linkDirectionalArrowLength(3.5)
      .linkDirectionalArrowRelPos(1)
      .linkColor(() => 'rgba(255,255,255,0.1)')
      .onNodeHover(node => {
        document.body.style.cursor = node ? 'pointer' : null;
        hoverNode = node;
      })
      .onNodeClick(node => focusNode(node))
      .nodeCanvasObject((node, ctx, globalScale) => {
        const isHovered = node === hoverNode;
        const isHighlighted = highlightedNodes.has(node.id);
        
        // Drastically reduced size: scale multiplier from 50 -> keeps nodes reasonable
        let baseR = Math.sqrt(Math.max(0, node.pagerank || 0.001)) * 50;
        if (baseR < 2) baseR = 2; // minimum radius
        const r = isHovered || isHighlighted ? baseR * 1.5 : baseR;
        
        // View-specific color logic
        let color = '#ffffff';
        if (node.type === 'external') {
          color = 'rgba(120, 120, 120, 0.5)'; // Faded out for broad external libraries
        } else if (viewSelect.value === 'pagerank') {
          // Heatmap from blue to red based on Percentile Rank!
          // This guarantees the entire color spectrum is used evenly regardless of skew.
          const normalized = getPercentile(node.pagerank || 0); 
          
          // We can use an HSL color wheel for a beautiful gradient: 240 (Blue) -> 0 (Red)
          const hue = Math.floor((1 - normalized) * 240);
          color = `hsl(${hue}, 100%, 50%)`;
        } else {
          color = colors[(node.community_id || 0) % colors.length];
        }

        ctx.fillStyle = color;
        ctx.globalAlpha = (isHovered || isHighlighted) ? 1.0 : 0.9;
        
        if (isHovered || isHighlighted) {
          ctx.shadowColor = color;
          ctx.shadowBlur = 10;
          ctx.lineWidth = 1.5 / globalScale;
          ctx.strokeStyle = '#ffffff';
        } else {
          ctx.shadowBlur = 0;
          ctx.lineWidth = 0;
        }

        ctx.beginPath();
        if (node.type === 'file') {
          // Draw square for files
          ctx.rect(node.x - r, node.y - r, r * 2, r * 2);
        } else {
          // Draw circle for functions/classes
          ctx.arc(node.x, node.y, r, 0, 2 * Math.PI, false);
        }
        
        ctx.fill();
        if (isHovered || isHighlighted) ctx.stroke();
        
        ctx.globalAlpha = 1.0; // reset
        
        // Draw Labels based on selection
        const labelMode = labelSelect.value;
        if (labelMode === 'all' || (labelMode === 'files' && node.type === 'file')) {
          const label = node.id.split('/').pop().split('::').pop();
          const fontSize = 12 / globalScale;
          ctx.font = `${fontSize}px Inter`;
          ctx.textAlign = 'center';
          ctx.textBaseline = 'middle';
          ctx.fillStyle = '#ffffff';
          ctx.fillText(label, node.x, node.y + r + (fontSize/2) + 2);
        }
      });
  }

  function focusNode(node) {
    if (!node) return;
    openPanel(node);
    Graph.centerAt(node.x, node.y, 1000);
    Graph.zoom(8, 2000);
  }

  function openPanel(node) {
    pType.textContent = node.type || 'Unknown';
    pId.textContent = node.id;
    pCommunity.textContent = `Community ${node.community_id || 0} • PR: ${(node.pagerank || 0).toFixed(4)}`;
    
    if (node.summary) {
      pSummary.textContent = node.summary;
      pSummary.style.color = 'var(--text-main)';
    } else {
      pSummary.textContent = 'No semantic summary generated for this node.';
      pSummary.style.color = 'var(--text-muted)';
    }
    
    pConnections.innerHTML = '';
    const outLinks = filteredData.links.filter(l => (l.source.id || l.source) === node.id);
    const inLinks = filteredData.links.filter(l => (l.target.id || l.target) === node.id);
    
    if (outLinks.length === 0 && inLinks.length === 0) {
      pConnections.innerHTML = '<p style="color:var(--text-muted);font-size:14px;">No direct connections.</p>';
    }
    
    outLinks.forEach(l => addConnectionItem('OUT', l.type || 'connects', l.target.id || l.target));
    inLinks.forEach(l => addConnectionItem('IN', l.type || 'connects', l.source.id || l.source));
    
    panel.classList.add('open');
  }

  function addConnectionItem(dir, type, target) {
    const div = document.createElement('div');
    div.className = 'connection-item';
    const spanType = document.createElement('span');
    spanType.className = 'conn-type';
    spanType.textContent = type.toUpperCase();
    if (dir === 'IN') {
      spanType.style.borderColor = '#A8E6CF';
      spanType.style.color = '#A8E6CF';
    }
    div.appendChild(spanType);
    div.appendChild(document.createTextNode(target));
    div.onclick = () => {
      const targetNode = filteredData.nodes.find(n => n.id === target);
      if (targetNode) focusNode(targetNode);
    };
    pConnections.appendChild(div);
  }

  // --- Search Logic ---
  function setupSearch() {
    searchInput.addEventListener('input', (e) => {
      const q = e.target.value.toLowerCase();
      searchResults.innerHTML = '';
      if (!q) {
        searchResults.classList.add('dropdown-hidden');
        return;
      }
      
      const matches = filteredData.nodes.filter(n => n.id.toLowerCase().includes(q)).slice(0, 10);
      if (matches.length > 0) {
        searchResults.classList.remove('dropdown-hidden');
        matches.forEach(node => {
          const div = document.createElement('div');
          div.className = 'search-item';
          div.textContent = node.id;
          div.onclick = () => {
            searchInput.value = '';
            searchResults.classList.add('dropdown-hidden');
            focusNode(node);
          };
          searchResults.appendChild(div);
        });
      } else {
        searchResults.classList.add('dropdown-hidden');
      }
    });

    document.addEventListener('click', (e) => {
      if (!e.target.closest('.search-container')) {
        searchResults.classList.add('dropdown-hidden');
      }
    });
  }

  // --- View Toggle Logic ---
  function setupViews() {
    viewSelect.addEventListener('change', (e) => {
      const mode = e.target.value;
      if (mode === 'files') {
        const fileNodes = graphData.nodes.filter(n => n.type === 'file');
        const fileNodeIds = new Set(fileNodes.map(n => n.id));
        const fileLinks = graphData.links.filter(l => 
          fileNodeIds.has(l.source.id || l.source) && fileNodeIds.has(l.target.id || l.target)
        );
        filteredData = { nodes: fileNodes, links: fileLinks };
      } else {
        filteredData = { nodes: [...graphData.nodes], links: [...graphData.links] };
      }
      
      Graph.graphData(filteredData);
    });
  }

  // --- Chat Logic ---
  function setupChat() {
    chatSend.addEventListener('click', sendMessage);
    chatInput.addEventListener('keypress', (e) => {
      if (e.key === 'Enter') sendMessage();
    });
  }

  async function sendMessage() {
    const text = chatInput.value.trim();
    if (!text) return;
    
    appendMessage('user', text);
    chatInput.value = '';
    
    const loadingId = appendMessage('system', 'Thinking...');
    
    try {
      const res = await fetch('/api/chat', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ message: text })
      });
      const data = await res.json();
      
      document.getElementById(loadingId).remove();
      
      if (data.error) {
        appendMessage('system', 'Error: ' + data.error);
        return;
      }
      
      appendMessage('system', data.reply);
      
      if (data.highlight_nodes && data.highlight_nodes.length > 0) {
        highlightedNodes = new Set(data.highlight_nodes);
        if (data.highlight_nodes[0]) {
          const firstNode = filteredData.nodes.find(n => n.id === data.highlight_nodes[0]);
          if (firstNode) focusNode(firstNode);
        }
      } else {
        highlightedNodes.clear();
      }
      
    } catch (err) {
      document.getElementById(loadingId).remove();
      appendMessage('system', 'Network error reaching LLM endpoint.');
    }
  }

  function appendMessage(role, content) {
    const msgId = 'msg-' + Math.random().toString(36).substr(2, 9);
    const div = document.createElement('div');
    div.id = msgId;
    div.className = `message ${role}`;
    div.textContent = content;
    chatMessages.appendChild(div);
    chatMessages.scrollTop = chatMessages.scrollHeight;
    return msgId;
  }
});
