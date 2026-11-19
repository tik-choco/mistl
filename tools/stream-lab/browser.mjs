import { spawn } from 'node:child_process';
import { accessSync, constants, statSync } from 'node:fs';
import { mkdir, mkdtemp, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { delimiter, join, resolve } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';

function isExecutable(path) {
  try {
    accessSync(path, constants.X_OK);
    return statSync(path).isFile();
  } catch {
    return false;
  }
}

export function findBrowserExecutable() {
  if (process.env.STREAM_LAB_BROWSER) return process.env.STREAM_LAB_BROWSER;
  const onPath = names => names.flatMap(name =>
    (process.env.PATH ?? '').split(delimiter).filter(Boolean).map(dir => join(dir, name)));
  let candidates;
  if (process.platform === 'win32') {
    const roots = [process.env.PROGRAMFILES, process.env['PROGRAMFILES(X86)'], process.env.LOCALAPPDATA];
    const installed = relative => roots.filter(Boolean).map(root => join(root, relative));
    candidates = [
      ...installed('Google/Chrome/Application/chrome.exe'), ...onPath(['chrome.exe']),
      ...installed('Microsoft/Edge/Application/msedge.exe'), ...onPath(['msedge.exe']),
    ];
  } else if (process.platform === 'darwin') {
    candidates = [
      '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
      ...onPath(['google-chrome', 'chrome']),
      '/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge',
      ...onPath(['microsoft-edge']),
    ];
  } else {
    candidates = onPath(['google-chrome', 'google-chrome-stable', 'chrome',
      'microsoft-edge', 'microsoft-edge-stable', 'chromium', 'chromium-browser']);
  }
  const found = candidates.find(isExecutable);
  if (!found) throw new Error('Chrome or Edge was not found. Set STREAM_LAB_BROWSER to its executable.');
  return found;
}

async function connectCdp(url) {
  const ws = new WebSocket(url);
  const pending = new Map();
  const listeners = new Set();
  let nextId = 0;

  ws.addEventListener('message', ({ data }) => {
    const message = JSON.parse(data);
    if (message.id === undefined) {
      for (const listener of listeners) listener(message);
      return;
    }
    const request = pending.get(message.id);
    if (!request) return;
    const { resolve, reject, timer, method } = request;
    pending.delete(message.id);
    clearTimeout(timer);
    if (message.error) reject(new Error(`${method}: ${message.error.message}`));
    else resolve(message.result);
  });
  ws.addEventListener('close', () => {
    for (const { reject, timer, method } of pending.values()) {
      clearTimeout(timer);
      reject(new Error(`${method}: CDP connection closed`));
    }
    pending.clear();
    listeners.clear();
  });

  try {
    await new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error('Timed out connecting to Chrome CDP')), 5000);
      ws.addEventListener('open', () => {
        clearTimeout(timer);
        resolve();
      }, { once: true });
      ws.addEventListener('error', event => {
        clearTimeout(timer);
        reject(new Error(event.message ?? 'Could not connect to Chrome CDP'));
      }, { once: true });
    });
  } catch (error) {
    ws.close();
    throw error;
  }

  return {
    send(method, params = {}, sessionId, timeoutMs = 30000) {
      if (ws.readyState !== WebSocket.OPEN) return Promise.reject(new Error('CDP connection closed'));
      return new Promise((resolve, reject) => {
        const id = ++nextId;
        ws.send(JSON.stringify({ id, method, params, sessionId }));
        const timer = setTimeout(() => {
          pending.delete(id);
          reject(new Error(`${method}: timed out after ${timeoutMs} ms`));
        }, timeoutMs);
        pending.set(id, { resolve, reject, timer, method });
      });
    },
    onEvent(listener) {
      listeners.add(listener);
      return () => listeners.delete(listener);
    },
    close() { ws.close(); },
  };
}

export async function launchBrowser({ executable, headless = true, userDataDir, extraArgs = [] } = {}) {
  executable ??= findBrowserExecutable();
  const temporary = userDataDir === undefined;
  const profile = temporary ? await mkdtemp(join(tmpdir(), 'stream-lab-browser-')) : resolve(userDataDir);
  await mkdir(profile, { recursive: true });
  const portFile = join(profile, 'DevToolsActivePort');
  await rm(portFile, { force: true });

  const proc = spawn(executable, [
    '--auto-accept-this-tab-capture',
    '--use-fake-ui-for-media-stream',
    '--autoplay-policy=no-user-gesture-required',
    '--disable-background-timer-throttling',
    '--disable-renderer-backgrounding',
    '--disable-backgrounding-occluded-windows',
    '--force-device-scale-factor=1',
    '--no-first-run',
    '--no-default-browser-check',
    '--window-size=1920,1080',
    ...(headless ? ['--headless=new'] : []),
    ...extraArgs,
    '--remote-debugging-port=0',
    `--user-data-dir=${profile}`,
    'about:blank',
  ], { stdio: ['ignore', 'ignore', 'pipe'], windowsHide: headless });
  let stderr = '';
  proc.stderr.setEncoding('utf8').on('data', chunk => { stderr = (stderr + chunk).slice(-8192); });
  let stopped;
  const exited = new Promise(resolve => {
    proc.once('error', error => { stopped = error.message; resolve(); });
    proc.once('exit', (code, signal) => { stopped = `exit ${code ?? signal}`; resolve(); });
  });
  let cdp;
  let closing;
  function close() {
    return closing ??= (async () => {
      if (!stopped && cdp) {
        await cdp.send('Browser.close', {}, undefined, 2000).catch(() => {});
        await Promise.race([exited, delay(3000, undefined, { ref: false })]);
      }
      cdp?.close();
      if (!stopped) {
        proc.kill();
        await exited;
      }
      if (temporary) await rm(profile, { recursive: true, force: true, maxRetries: 8, retryDelay: 200 });
    })();
  }

  try {
    const deadline = Date.now() + 15000;
    while (!stopped && Date.now() < deadline) {
      // Chrome also prints the endpoint on stderr; prefer it because on
      // Windows reading DevToolsActivePort while Chrome is still writing it
      // fails with EBUSY.
      const listening = stderr.match(/DevTools listening on (ws:\/\/\S+)/);
      if (listening) {
        cdp = await connectCdp(listening[1]);
        return { cdp, close };
      }
      const contents = await readFile(portFile, 'utf8').catch(error => {
        if (error.code !== 'ENOENT' && error.code !== 'EBUSY') throw error;
        return '';
      });
      const [port, endpoint] = contents.trim().split(/\r?\n/);
      if (/^\d+$/.test(port) && endpoint?.startsWith('/devtools/browser/')) {
        cdp = await connectCdp(`ws://127.0.0.1:${port}${endpoint}`);
        return { cdp, close };
      }
      await delay(50);
    }
    throw new Error(stopped ?? 'timed out waiting for DevToolsActivePort');
  } catch (error) {
    await close().catch(() => {});
    throw new Error(`Could not launch ${executable}: ${error.message}${stderr && `\n${stderr.trim()}`}`);
  }
}

export async function openPage(browser, url) {
  const { cdp } = browser;
  const { targetId } = await cdp.send('Target.createTarget', { url: 'about:blank' });
  const { sessionId } = await cdp.send('Target.attachToTarget', { targetId, flatten: true });
  const send = (method, params, timeout) => cdp.send(method, params, sessionId, timeout);
  const consoleMessages = [];
  function log(message) {
    consoleMessages.push(message);
    if (consoleMessages.length > 2000) consoleMessages.shift();
  }

  const unsubscribe = cdp.onEvent(event => {
    if (event.method === 'Target.detachedFromTarget' && event.params.sessionId === sessionId) unsubscribe();
    if (event.sessionId !== sessionId) return;
    const { method, params } = event;
    if (method === 'Runtime.consoleAPICalled') {
      log(`${params.type}: ${params.args.map(arg => arg.value ?? arg.description ?? arg.type).join(' ')}`);
    } else if (method === 'Runtime.exceptionThrown') {
      const details = params.exceptionDetails;
      log(`error: ${details.exception?.description ?? details.text}`);
    } else if (method === 'Log.entryAdded') {
      log(`${params.entry.level}: ${params.entry.text}`);
    }
  });

  try {
    await Promise.all([send('Runtime.enable'), send('Log.enable'), send('Page.enable')]);
    await cdp.send('Target.activateTarget', { targetId });
    const { errorText } = await send('Page.navigate', { url });
    if (errorText) throw new Error(`Could not open ${url}: ${errorText}`);
  } catch (error) {
    unsubscribe();
    await cdp.send('Target.closeTarget', { targetId }).catch(() => {});
    throw error;
  }

  async function evaluate(expr, { userGesture = false, awaitPromise = true } = {}, timeoutMs) {
    const { result, exceptionDetails } = await send('Runtime.evaluate', {
      expression: expr, userGesture, awaitPromise, returnByValue: true,
    }, timeoutMs);
    if (exceptionDetails) throw new Error(exceptionDetails.exception?.description ?? exceptionDetails.text);
    return result.value;
  }

  async function waitFor(expr, timeoutMs) {
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      const value = await evaluate(expr, {}, Math.max(1, deadline - Date.now()));
      if (value) return value;
      await delay(Math.max(0, Math.min(50, deadline - Date.now())));
    }
    throw new Error(`Timed out after ${timeoutMs} ms waiting for ${expr}`);
  }

  return { evaluate, waitFor, consoleMessages };
}
