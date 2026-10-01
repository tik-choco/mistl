import assert from 'node:assert/strict';
import fs from 'node:fs';
import {Window} from 'happy-dom';

const window = new Window({url: 'http://localhost/'});
const {document} = window;
document.body.innerHTML = '<div id="dm-app"></div>';
window.eval(fs.readFileSync(new URL('../src/web/assets/features/messenger.js', import.meta.url), 'utf8'));

const ALICE = 'did:key:z6Mk' + 'a'.repeat(44);
const BOB = 'did:key:z6Mk' + 'b'.repeat(44);
const HOSTILE = '<img src=x onerror=alert(1)>';
const calls = [];
let sent = [];
const msgs = {
  [ALICE]: [
    {id: '1'.repeat(32), mine: false, ts_ms: Date.now() - 5000, text: HOSTILE, status: 'received'},
    {id: '2'.repeat(32), mine: false, ts_ms: Date.now() - 4000, file: {cid: 'bafy', name: '<b>x</b>.txt', size: 2048, mime: 'text/plain'}, status: 'received'},
    {id: '3'.repeat(32), mine: true, ts_ms: Date.now() - 3000, text: 'hello', status: 'delivered'},
  ],
};
const call = async (cmd, args) => {
  calls.push({cmd, args});
  if (cmd === 'dm.ls') {
    return {conversations: [{did: ALICE, unread: 2, updated_ms: 5, last: msgs[ALICE].at(-1)}]};
  }
  if (cmd === 'dm.history') return {messages: msgs[args.did] || []};
  if (cmd === 'dm.read') return {ok: true};
  if (cmd === 'dm.send') { sent.push(args); return {message: {}}; }
  if (cmd === 'dm.download') return {sandbox_path: 'dm/' + args.id + '/x.txt', name: 'x.txt', size: 2048};
  throw new Error('unexpected ' + cmd);
};
const peers = {
  list: () => [{did: ALICE, online: true, name: HOSTILE}, {did: BOB, online: false}],
  byDid: (did) => peers.list().find((p) => p.did === did) || null,
  label: (did) => (did === ALICE ? HOSTILE : 'bob'),
  avatarEl: () => null,
  subscribe: () => () => {},
};
const same = (a, b) => assert.equal(JSON.stringify(a), JSON.stringify(b));
const flush = async () => { for (let i = 0; i < 10; i++) await new Promise((r) => setImmediate(r)); };

const host = document.getElementById('dm-app');
const view = window.MistlMessenger.mount(host, {call, peers, lang: () => 'en'});
await flush();

// People list: conversations and online peers are merged, hostile names stay text.
const rows = [...host.querySelectorAll('.msgr-person')];
assert.equal(rows.length, 2);
assert.equal(host.querySelector('.msgr-badge').textContent, '2');
assert.equal(host.querySelector('img'), null, 'peer text must never become markup');
assert.ok(rows[0].querySelector('.msgr-name').textContent === HOSTILE);

// Open the conversation.
rows[0].click();
await flush();
assert.equal(host.querySelector('.msgr').classList.contains('is-open'), true);
assert.ok(calls.some((c) => c.cmd === 'dm.read' && c.args.did === ALICE));
assert.equal(host.querySelectorAll('.msgr-bubble').length, 3);
assert.equal(host.querySelector('.msgr-text').textContent, HOSTILE);
assert.equal(host.querySelector('img'), null);
assert.equal(host.querySelector('b'), null);
assert.ok(host.querySelector('.msgr-file-name').textContent.includes('<b>x</b>.txt'));
assert.equal(host.querySelector('.msgr-status.is-delivered').textContent, '✓✓');
assert.ok(!JSON.stringify(calls).includes('"key"'), 'the UI never handles file keys');

// Enter sends, Shift+Enter does not.
const input = host.querySelector('.msgr-input');
input.value = 'line one';
input.dispatchEvent(new window.KeyboardEvent('keydown', {key: 'Enter', shiftKey: true, bubbles: true, cancelable: true}));
await flush();
assert.equal(sent.length, 0);
input.dispatchEvent(new window.KeyboardEvent('keydown', {key: 'Enter', bubbles: true, cancelable: true}));
await flush();
same(sent, [{did: ALICE, text: 'line one'}]);
assert.equal(input.value, '');

// Whitespace-only input is not sent.
input.value = '   ';
input.dispatchEvent(new window.KeyboardEvent('keydown', {key: 'Enter', bubbles: true, cancelable: true}));
await flush();
assert.equal(sent.length, 1);

// Attach: upload into the sandbox, then dm.send with the sandbox name.
let uploaded = null;
view.destroy();
const view2 = window.MistlMessenger.mount(host, {
  call, peers, lang: () => 'ja',
  upload: async (file) => { uploaded = file.name; return {imported: 'report.pdf'}; },
});
await flush();
host.querySelector('.msgr-person').click();
await flush();
const fileInput = host.querySelector('input[type=file]');
const file = new window.File(['hello'], 'report.pdf');
Object.defineProperty(fileInput, 'files', {value: [file], configurable: true});
fileInput.dispatchEvent(new window.Event('change', {bubbles: true}));
await flush();
assert.equal(uploaded, 'report.pdf');
same(sent.at(-1), {did: ALICE, sandbox: 'report.pdf'});
assert.equal(host.querySelector('.msgr-send').textContent, '送信', 'ja strings are used');

// Download goes through dm.download with did + message id only.
const before = calls.length;
host.querySelector('.msgr-download').click();
await flush();
const dl = calls.slice(before).find((c) => c.cmd === 'dm.download');
same(dl.args, {did: ALICE, id: '2'.repeat(32)});

// Oversized files are refused client-side without an upload.
uploaded = null;
const big = new window.File(['x'], 'big.bin');
Object.defineProperty(big, 'size', {value: 300 * 1024 * 1024});
Object.defineProperty(fileInput, 'files', {value: [big], configurable: true});
fileInput.dispatchEvent(new window.Event('change', {bubbles: true}));
await flush();
assert.equal(uploaded, null);
assert.ok(host.querySelector('.msgr-notice.is-error'));

view2.destroy();
await window.happyDOM.close();
console.log('messenger ui ok');
