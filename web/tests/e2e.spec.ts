import { test, expect } from '@playwright/test';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { spawn, execSync, type ChildProcess } from 'child_process';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const PORT = '8731';
const projectRoot = path.resolve(__dirname, '../..');

let tmpdir: string;
let serverProcess: ChildProcess | null = null;

const pipelineScript = `
import os, sys
from pydantic import BaseModel
from rubedo import pipeline, ProcessResult

p = pipeline(name="e2e-demo")

@p.step
def scan():
    folder = os.path.join(os.path.dirname(__file__), "input")
    for name in sorted(os.listdir(folder)):
        path = os.path.join(folder, name)
        if os.path.isfile(path):
            yield path

@p.step
def read(scan: str):
    text = open(scan).read()
    return {"lines": text.splitlines(), "name": os.path.basename(scan)}

@p.step(shape="reduce")
def total(read: dict):
    return sum(v["lines"].__len__() for v in read.values())

p.run()
`;

test.beforeAll(async () => {
  tmpdir = fs.mkdtempSync(path.join(os.tmpdir(), 'rubedo-e2e-'));
  fs.mkdirSync(path.join(tmpdir, 'input'));
  fs.writeFileSync(path.join(tmpdir, 'input/a.txt'), 'line1\nline2\n');
  fs.writeFileSync(path.join(tmpdir, 'input/b.txt'), 'hello\n');
  fs.writeFileSync(path.join(tmpdir, 'pipeline.py'), pipelineScript);

  const uvRun = ['uv', 'run', 'python'];

  execSync(`${uvRun.join(' ')} ${path.join(tmpdir, 'pipeline.py')}`, {
    cwd: projectRoot,
    env: { ...process.env, RUBEDO_HOME: tmpdir },
    stdio: 'pipe',
  });

  serverProcess = spawn(uvRun[0], [...uvRun.slice(1), '-m', 'uvicorn', 'rubedo.server:app', '--port', PORT], {
    cwd: projectRoot,
    env: { ...process.env, RUBEDO_HOME: tmpdir },
    stdio: 'pipe',
  });

  // Wait for server to be ready
  for (let i = 0; i < 30; i++) {
    try {
      const res = await fetch(`http://localhost:${PORT}/api/runs`);
      if (res.ok) return;
    } catch {
      // not ready yet
    }
    await new Promise((r) => setTimeout(r, 500));
  }
  throw new Error('Server did not start in time');
});

test.afterAll(() => {
  if (serverProcess) {
    serverProcess.kill('SIGTERM');
    serverProcess = null;
  }
  try {
    fs.rmSync(tmpdir, { recursive: true, force: true });
  } catch {
    // ignore
  }
});

test('Runs page lists the run', async ({ page }) => {
  await page.goto('/');

  await expect(page.locator('h1.page-title')).toHaveText('Runs');
  await expect(page.locator('tbody tr')).toHaveCount(1);
  await expect(page.locator('tbody tr td').first()).toContainText(/./);
  await expect(page.locator('tbody tr')).toContainText('e2e-demo');
  await expect(page.locator('tbody tr')).toContainText('completed');
});

test('Run detail shows stats and coordinates', async ({ page }) => {
  await page.goto('/');
  await page.locator('tbody tr a').first().click();
  await expect(page).toHaveURL(/\/runs\/.+/);

  await expect(page.locator('.stat-label', { hasText: 'Created' })).toBeVisible();
  await expect(page.locator('.stat-value', { hasText: 'completed' })).toBeVisible();

  // Should have coordinate rows (scan + read + total lanes)
  const coordRows = page.locator('tbody tr');
  const count = await coordRows.count();
  expect(count).toBeGreaterThan(0);
});

test('Pipelines page shows the pipeline with a DAG', async ({ page }) => {
  await page.goto('/pipelines');

  await expect(page.locator('h1.page-title')).toHaveText('Pipelines');
  await expect(page.locator('.card')).toContainText('e2e-demo');
  await expect(page.locator('.card h2 code')).toContainText('e2e-demo');

  // DagView renders step boxes
  await expect(page.locator('[data-step]')).toHaveCount(3);
  await expect(page.locator('[data-step="scan"]')).toBeVisible();
  await expect(page.locator('[data-step="read"]')).toBeVisible();
  await expect(page.locator('[data-step="total"]')).toBeVisible();
});

test('SPA fallback works on refresh', async ({ page }) => {
  await page.goto('/pipelines');
  await expect(page.locator('h1.page-title')).toHaveText('Pipelines');
  // Reload — client-side route should still work via SPA fallback
  await page.reload();
  await expect(page.locator('h1.page-title')).toHaveText('Pipelines');
});
