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

export async function fetchMaterializations() {
  const res = await fetch(`${API_URL}/materializations`);
  return res.json();
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

export async function fetchProcessors() {
  const res = await fetch(`${API_URL}/processors`);
  return res.json();
}

export async function runProcessor(id: string, payload: any) {
  const res = await fetch(`${API_URL}/processors/${id}/run`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload)
  });
  if (!res.ok) {
    const data = await res.json();
    throw new Error(data.detail || 'Failed to run processor');
  }
  return res.json();
}

export async function fetchExecutions() {
  const res = await fetch(`${API_URL}/executions`);
  return res.json();
}

export async function fetchExecution(id: string) {
  const res = await fetch(`${API_URL}/executions/${id}`);
  return res.json();
}

export async function fetchExecutionStdout(id: string) {
  const res = await fetch(`${API_URL}/executions/${id}/stdout`);
  return res.text();
}

export async function fetchExecutionStderr(id: string) {
  const res = await fetch(`${API_URL}/executions/${id}/stderr`);
  return res.text();
}

