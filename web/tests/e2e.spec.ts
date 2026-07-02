import { test, expect } from '@playwright/test';
import * as fs from 'fs';
import * as path from 'path';
import * as os from 'os';
import { spawn, execSync, ChildProcess } from 'child_process';

let tmpdir: string;
let backendProcess: ChildProcess;

test.beforeAll(async () => {
  tmpdir = fs.mkdtempSync(path.join(os.tmpdir(), 'batchbrain-e2e-'));
  const projectRoot = path.resolve(process.cwd(), '../');
  
  const scriptContent = `
import os
from batchbrain import process
from examples.simple_process import count_lines

if not os.path.exists('test_input'):
    os.makedirs('test_input')
    with open('test_input/a.txt', 'w') as f: f.write('a')
    with open('test_input/b.txt', 'w') as f: f.write('b')
    with open('test_input/c.txt', 'w') as f: f.write('c')

process('test_input', count_lines, code_version='v1')
`;
  const runBatch1Path = path.join(tmpdir, 'run1.py');
  fs.writeFileSync(runBatch1Path, scriptContent);
  const pythonExe = os.platform() === 'win32' ? path.join(projectRoot, '.venv', 'Scripts', 'python.exe') : path.join(projectRoot, '.venv', 'bin', 'python');

  execSync(`"${pythonExe}" run1.py`, { cwd: tmpdir, env: { ...process.env, PYTHONPATH: projectRoot } });
  
  const script2Content = `
import os
from batchbrain import process
from examples.simple_process import count_lines

os.remove('test_input/b.txt')
process('test_input', count_lines, code_version='v1')
`;
  const runBatch2Path = path.join(tmpdir, 'run2.py');
  fs.writeFileSync(runBatch2Path, script2Content);
  execSync(`"${pythonExe}" run2.py`, { cwd: tmpdir, env: { ...process.env, PYTHONPATH: projectRoot } });

  backendProcess = spawn(pythonExe, ['-m', 'uvicorn', 'batchbrain.server:app', '--port', '8000'], {
    cwd: tmpdir,
    env: { ...process.env, PYTHONPATH: projectRoot },
  });
  
  await new Promise(r => setTimeout(r, 4000));
});

test.afterAll(() => {
  if (backendProcess) backendProcess.kill();
  try { fs.rmSync(tmpdir, { recursive: true, force: true }); } catch (e) {}
});

test('golden loop validates logical deletes and UI rendering', async ({ page }) => {
  await page.goto('/runs');
  
  // We should see two runs in the table (plus header row = 3)
  const rows = page.locator('tbody tr');
  await expect(rows).toHaveCount(2);
  
  // The first row (latest run) should have 1 removed.
  // Columns are: ID, Kind, Status, Started, Created, Reused, Failed, Removed, Actions
  // Removed is index 7 (0-indexed: ID(0), Kind(1), Status(2), Started(3), Created(4), Reused(5), Failed(6), Removed(7))
  const latestRunRow = rows.nth(0);
  await expect(latestRunRow.locator('td').nth(7)).toHaveText('1');
  
  // The reused count for this run should be 2
  await expect(latestRunRow.locator('td').nth(5)).toHaveText('2');
  
  // Click View to go to run detail
  await latestRunRow.locator('a').click();
  await expect(page).toHaveURL(/.*\/runs\/.+/);
  
  // In RunDetail, we should see a Removed stat card with '1'
  const statValues = page.locator('.stat-value');
  await expect(statValues).toHaveCount(5); // Status, Created, Reused, Failed, Removed
  const removedCard = page.locator('.stat-card').filter({ hasText: 'Removed' });
  await expect(removedCard.locator('.stat-value')).toHaveText('1');
  
  // Look at Coordinate table
  // There should be a row for b.txt with status "removed"
  const bRow = page.locator('tbody tr').filter({ hasText: 'b.txt' });
  await expect(bRow).toBeVisible();
  const badge = bRow.locator('.badge');
  await expect(badge).toHaveText('removed');
  await expect(badge).toHaveClass(/badge-warning/);
  
  // Go to Compare Runs
  await page.goto('/diff');
  // It should automatically have two runs
  await page.waitForSelector('select option:not([value=""])', { state: 'attached' });
  await page.locator('button:has-text("Compare")').click({ force: true });
  
  // The diff table should have b.txt as removed
  const diffRow = page.locator('tbody tr').filter({ hasText: 'b.txt' });
  await expect(diffRow).toBeVisible();
  const diffBadge = diffRow.locator('td').nth(1).locator('.badge');
  await expect(diffBadge).toHaveText('removed');
});
