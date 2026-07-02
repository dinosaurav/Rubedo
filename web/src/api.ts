const API_URL = 'http://localhost:8000/api';

export async function fetchRuns() {
  const res = await fetch(`${API_URL}/runs`);
  return res.json();
}

export async function fetchRun(id: string) {
  const res = await fetch(`${API_URL}/runs/${id}`);
  return res.json();
}

export async function fetchRunCoordinates(id: string) {
  const res = await fetch(`${API_URL}/runs/${id}/coordinates`);
  return res.json();
}

export async function fetchRunEvents(id: string) {
  const res = await fetch(`${API_URL}/runs/${id}/events`);
  return res.json();
}

export async function fetchMaterializations(limit = 100, offset = 0) {
  const res = await fetch(`${API_URL}/materializations?limit=${limit}&offset=${offset}`);
  const items = await res.json();
  const totalHeader = res.headers.get('X-Total-Count');
  const total = totalHeader !== null ? Number(totalHeader) : items.length;
  return { items, total };
}

export async function fetchCurrentOutputs() {
  const res = await fetch(`${API_URL}/current-outputs`);
  return res.json();
}

export async function previewSelection(selection: any) {
  const res = await fetch(`${API_URL}/selection/preview`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(selection)
  });
  return res.json();
}

export async function invalidateSelection(selection: any, reason: string) {
  const res = await fetch(`${API_URL}/selection/invalidate?reason=${encodeURIComponent(reason)}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(selection)
  });
  return res.json();
}

export async function diffRuns(leftId: string, rightId: string) {
  const res = await fetch(`${API_URL}/runs/${leftId}/diff/${rightId}`);
  return res.json();
}

export async function fetchObject(outputAddress: string) {
  const res = await fetch(`${API_URL}/objects/${outputAddress}`);
  return res.json();
}

export async function fetchPipelines() {
  const res = await fetch(`${API_URL}/pipelines`);
  return res.json();
}


