import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
import { Window } from 'happy-dom';

const html = fs.readFileSync(new URL('../src/web/assets/index.html', import.meta.url), 'utf8');
const start = html.indexOf('    <div class="subtab-page" data-subtab="nostr" id="panel-nostr">');
const end = html.indexOf('    <div class="topology-slot" id="topology-slot-store">', start);
assert.ok(start >= 0 && end > start);
const translationsStart = html.indexOf('  var T = {');
const translationsEnd = html.indexOf('\n  };', translationsStart);
assert.ok(translationsStart >= 0 && translationsEnd > translationsStart);
const translations = vm.runInNewContext(html.slice(translationsStart, translationsEnd + 5) + '\nT;');

const window = new Window({ url: 'http://localhost/' });
window.document.body.innerHTML = html.slice(start, end);
window.eval(fs.readFileSync(new URL('../src/web/assets/features/nostr.js', import.meta.url), 'utf8'));
const root = window.document.getElementById('panel-nostr');
let locale = 'en';
const view = window.MistlNostr.create({
  root,
  t: (key, values = {}) => (translations[locale][key] || key).replace(/\{(\w+)\}/g, (_, name) => values[name] ?? ''),
  copy: () => {},
  setRunning: () => Promise.resolve({ running: true }),
});
const status = {
  running: true,
  upstreams: {
    gateway: true,
    interest: 2,
    relays: [
      { url: '<img src=x onerror=alert(1)>', role: 'bridge', state: 'connected', received: 5, published: 1 },
      { url: 'wss://relay.example', role: 'upstream', state: 'idle', error: '<b>error</b>' },
    ],
  },
};
view.render(status);
const interest = root.querySelector('[data-nostr-interest]');
const relayRows = () => [...root.querySelectorAll('[data-nostr-upstream-rows] li')];
assert.equal(interest.hidden, false);
assert.equal(interest.textContent, 'Signaling interest: 2 filters');
assert.equal(relayRows()[0].querySelector('.nostr-upstream-role').textContent, 'Bridge');
assert.equal(relayRows()[1].querySelector('.nostr-upstream-role').textContent, 'Upstream');
assert.equal(relayRows()[0].querySelector('.mono').textContent, status.upstreams.relays[0].url);
assert.ok(relayRows()[1].textContent.includes('<b>error</b>'));
assert.equal(root.querySelector('img, b'), null, 'Relay data stays literal text');

for (const [language, bridge, upstream, count] of [
  ['ja', 'ブリッジ', '上流', 'シグナリングの購読フィルター: 2 件'],
  ['zh', '桥接', '上游', '信令订阅过滤器：2 个'],
  ['en', 'Bridge', 'Upstream', 'Signaling interest: 2 filters'],
]) {
  locale = language;
  view.refreshLanguage();
  assert.equal(relayRows()[0].querySelector('.nostr-upstream-role').textContent, bridge);
  assert.equal(relayRows()[1].querySelector('.nostr-upstream-role').textContent, upstream);
  assert.equal(interest.textContent, count);
}

view.render({ running: true, upstreams: { interest: 0, relays: [{}] } });
assert.equal(interest.hidden, false, 'Zero interest is shown');
assert.equal(interest.textContent, 'Signaling interest: 0 filters');
assert.equal(relayRows()[0].querySelector('.nostr-upstream-role').hidden, true, 'Missing role stays hidden');
view.render({ running: true, upstreams: { relays: [{ role: '<img>' }] } });
assert.equal(interest.hidden, true, 'Missing interest is not reported as zero');
assert.equal(interest.textContent, '');
assert.equal(relayRows()[0].querySelector('.nostr-upstream-role').hidden, true, 'Unknown roles stay hidden');
view.render({ running: true, upstreams: { interest: -1 } });
assert.equal(interest.hidden, true, 'Invalid interest is not displayed');
view.render({ running: true });
assert.equal(root.querySelector('[data-nostr-upstreams]').hidden, true);
console.log('PASS: Nostr relay roles, signaling interest, localization and safe rendering');
window.happyDOM.abort();
