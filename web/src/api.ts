const API_URL = 'http://localhost:8000/api';

export async function fetchJson(url: string, options?: RequestInit) {
  try {
    const res = await fetch(url, options);
    if (!res.ok) {
      let errorMsg = res.statusText;
      try {
        const errJson = await res.json();
        if (errJson.detail) errorMsg = typeof errJson.detail === 'string' ? errJson.detail : JSON.stringify(errJson.detail);
      } catch (e) {
        // Ignore JSON parse errors for error bodies
      }
      throw new Error(`API error (${res.status}): ${errorMsg}`);
    }
    return res;
  } catch (err: any) {
    if (err.name === 'TypeError' && err.message.includes('fetch')) {
      throw new Error(`API unreachable: Is the backend server running at ${API_URL}?`);
    }
    throw err;
  }
}

export async function fetchRuns() {
  const res = await fetchJson(`${API_URL}/runs`);
  return res.json();
}

export async function fetchRun(id: string) {
  const res = await fetchJson(`${API_URL}/runs/${id}`);
  return res.json();
}

export async function fetchRunCoordinates(id: string) {
  const res = await fetchJson(`${API_URL}/runs/${id}/coordinates`);
  return res.json();
}

export async function fetchRunEvents(id: string) {
  const res = await fetchJson(`${API_URL}/runs/${id}/events`);
  return res.json();
}

export async function fetchMaterializations(limit = 100, offset = 0) {
  const res = await fetchJson(`${API_URL}/materializations?limit=${limit}&offset=${offset}`);
  const items = await res.json();
  const totalHeader = res.headers.get('X-Total-Count');
  const total = totalHeader !== null ? Number(totalHeader) : items.length;
  return { items, total };
}

export async function fetchCurrentOutputs() {
  const res = await fetchJson(`${API_URL}/current-outputs`);
  return res.json();
}

export async function previewSelection(selection: any) {
  const res = await fetchJson(`${API_URL}/selection/preview`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(selection)
  });
  return res.json();
}

export async function invalidateSelection(selection: any, reason: string) {
  const res = await fetchJson(`${API_URL}/selection/invalidate?reason=${encodeURIComponent(reason)}`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(selection)
  });
  return res.json();
}

export async function fetchObject(outputAddress: string) {
  const res = await fetchJson(`${API_URL}/objects/${outputAddress}`);
  return res.json();
}

export async function fetchPipelines() {
  const res = await fetchJson(`${API_URL}/pipelines`);
  return res.json();
}

export async function searchRun(runId: string, query: string) {
  const res = await fetchJson(`${API_URL}/runs/${runId}/search?query=${encodeURIComponent(query)}`);
  return res.json();
}

export async function fetchStepOutputs(runId: string, stepName: string, limit = 50, offset = 0) {
  const res = await fetchJson(`${API_URL}/runs/${runId}/steps/${encodeURIComponent(stepName)}/outputs?limit=${limit}&offset=${offset}`);
  return res.json();
}

