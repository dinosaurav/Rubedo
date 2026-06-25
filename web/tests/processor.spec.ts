import { test, expect } from '@playwright/test';
import { spawn } from 'child_process';
import path from 'path';
import fs from 'fs';
import os from 'os';

let backendProcess: any;
let tmpDir: string;
const BACKEND_PORT = 8001;

test.beforeAll(async () => {
  tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'batchbrain-playwright-proc-'));
  
  // Create test input files
  const inputDir = path.join(tmpDir, 'examples', 'input');
  fs.mkdirSync(inputDir, { recursive: true });
  fs.writeFileSync(path.join(inputDir, 'a.txt'), 'line1\nline2\n');
  fs.writeFileSync(path.join(inputDir, 'b.txt'), 'line1\nline2\nline3\n');
  
  // Copy batchbrain_processors.py
  const rootProcessors = path.join(process.cwd(), '..', 'batchbrain_processors.py');
  fs.copyFileSync(rootProcessors, path.join(tmpDir, 'batchbrain_processors.py'));
  
  backendProcess = spawn(
    path.join(process.cwd(), '..', '.venv', 'Scripts', 'python.exe'),
    ['-m', 'uvicorn', 'batchbrain.server:app', '--port', BACKEND_PORT.toString()],
    { 
      cwd: tmpDir,
      env: { ...process.env, PYTHONPATH: path.join(process.cwd(), '..') }
    }
  );

  backendProcess.stderr.on('data', (d: any) => console.log('BACKEND STDERR:', d.toString()));
  backendProcess.stdout.on('data', (d: any) => console.log('BACKEND STDOUT:', d.toString()));

  // Wait for backend to be ready
  for (let i = 0; i < 30; i++) {
    try {
      const res = await fetch(`http://localhost:${BACKEND_PORT}/api/processors`);
      if (res.ok) {
        console.log('Backend ready! Processors:', await res.json());
        break;
      }
    } catch (e) {}
    await new Promise(r => setTimeout(r, 500));
  }
});

test.afterAll(async () => {
  if (backendProcess) {
    backendProcess.kill();
  }
  try {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  } catch (e) {}
});

test.use({
  baseURL: 'http://localhost:5173',
});

test('processor execution logic and caching', async ({ page }) => {
  page.on('console', msg => console.log('PAGE LOG:', msg.text()));
  page.on('pageerror', err => console.log('PAGE ERROR:', err.message));
  page.on('requestfailed', req => console.log('REQUEST FAILED:', req.url(), req.failure()?.errorText));

  // Override API_URL via intercepting requests from frontend to 8000 -> 8001
  await page.route('http://localhost:8000/api/**', async (route) => {
    const url = route.request().url().replace(':8000', `:${BACKEND_PORT}`);
    const response = await fetch(url, {
      method: route.request().method(),
      headers: route.request().headers(),
      body: route.request().postData()
    });
    const body = await response.text();
    await route.fulfill({
      status: response.status,
      contentType: response.headers.get('content-type') || 'application/json',
      body,
    });
  });

  // 1. Run processor with min_lines=0
  await page.goto('/processors');
  
  // Wait for table to load and click Run on count-lines
  await page.waitForSelector('td:has-text("count-lines")');
  await page.click('button:has-text("Run")');
  
  // Fill input min_lines = 0
  await page.fill('input[type="number"]', '0');
  await page.click('button:has-text("Submit Execution")');
  
  // Wait for ExecutionDetail page
  await expect(page.locator('h1')).toContainText('Execution: exec_');
  // Wait until succeeded
  await expect(page.locator('.status-badge')).toContainText('succeeded', { timeout: 15000 });
  
  // Click run link
  await page.click('a[href^="/runs/"]');
  
  // Check RunDetail stats
  await expect(page.locator('.stat-value').nth(1)).toContainText('2'); // created
  await expect(page.locator('.stat-value').nth(2)).toContainText('0'); // reused
  
  // 2. Run with same inputs again
  await page.goto('/processors');
  await page.waitForSelector('td:has-text("count-lines")');
  await page.click('button:has-text("Run")');
  await page.fill('input[type="number"]', '0');
  await page.click('button:has-text("Submit Execution")');
  await expect(page.locator('.status-badge')).toContainText('succeeded', { timeout: 15000 });
  await page.click('a[href^="/runs/"]');
  // Reused should be 2
  await expect(page.locator('.stat-value').nth(1)).toContainText('0'); // created
  await expect(page.locator('.stat-value').nth(2)).toContainText('2'); // reused
  
  // 3. Run with min_lines=10 (Different inputs = recompute)
  await page.goto('/processors');
  await page.waitForSelector('td:has-text("count-lines")');
  await page.click('button:has-text("Run")');
  await page.fill('input[type="number"]', '10');
  await page.click('button:has-text("Submit Execution")');
  await expect(page.locator('.status-badge')).toContainText('succeeded', { timeout: 15000 });
  await page.click('a[href^="/runs/"]');
  // Created should be 2 again since config hash changed!
  await expect(page.locator('.stat-value').nth(1)).toContainText('2'); // created
  await expect(page.locator('.stat-value').nth(2)).toContainText('0'); // reused

  // 4. Invalidate b.txt
  await page.goto('/select');
  await page.fill('input[placeholder*="*.txt"]', '*b.txt*');
  await page.click('button:has-text("Preview Selection")');
  await page.waitForSelector('td:has-text("Valid")');
  
  page.on('dialog', dialog => dialog.accept());
  await page.click('button:has-text("Invalidate")');

  // 5. Run min_lines=10 again
  await page.goto('/processors');
  await page.waitForSelector('td:has-text("count-lines")');
  await page.click('button:has-text("Run")');
  await page.fill('input[type="number"]', '10');
  await page.click('button:has-text("Submit Execution")');
  await expect(page.locator('.status-badge')).toContainText('succeeded', { timeout: 15000 });
  await page.click('a[href^="/runs/"]');
  
  // Assert exactly 1 reused, 1 created
  await expect(page.locator('.stat-value').nth(1)).toContainText('1'); // created
  await expect(page.locator('.stat-value').nth(2)).toContainText('1'); // reused
});




