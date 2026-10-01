import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
import { webcrypto } from 'node:crypto';
import { Window } from 'happy-dom';

const source = fs.readFileSync(new URL('../src/web/assets/features/storage.js', import.meta.url), 'utf8');
const context = { window: { btoa, atob }, TextEncoder, URLSearchParams };
vm.runInNewContext(source, context);
const helpers = context.window.MistlStorage;
const passphrases = Array.from({ length: 32 }, () => helpers.generateSharePassphrase(webcrypto));
assert.equal(new Set(passphrases).size, 32);
assert.ok(passphrases.every(p => /^[A-Za-z0-9_-]{24}$/.test(p)));
const share = { folder_id: 'folder-1', folder_name: '日本語 <img>', local_dir: '/shared', room_id: 'room', folder_key_hash: 'a'.repeat(64), access_grant_mode: 'shared', files: [{}, { deletedAt: 'yesterday' }] };
const link = helpers.folderShareLink(share, 'did:key:owner');
assert.deepEqual(JSON.parse(Buffer.from(link.split('=')[1], 'base64url')), {
  v: 1, type: 'folder-share', roomId: 'room', folderId: 'folder-1', folderName: share.folder_name,
  ownerNodeId: 'did:key:owner', accessGrantMode: 'shared', folderKeyHash: share.folder_key_hash, senderProfile: { name: 'mistl' }
});
assert.equal(helpers.shareLinkHasKey(link), false);
const embedded = '#tc-share=' + Buffer.from(JSON.stringify({ key: 'secret' })).toString('base64url');
assert.equal(helpers.shareLinkHasKey(embedded), true);
assert.equal(helpers.shareLinkHasKey('invalid'), false);

const window = new Window({ url: 'http://localhost/' });
window.TextEncoder = TextEncoder;
window.document.body.innerHTML = '<div id="shares"></div>';
window.eval(source);
const { document } = window;
const calls = [], copies = [];
let rows = [share], finish, fail = false, locale = 'en';
const feature = window.MistlStorage.createFolderShare({
  mount: document.getElementById('shares'), getLocale: () => locale,
  copy: async text => { copies.push(text); },
  call: async (cmd, args) => {
    calls.push({ cmd, args: structuredClone(args) });
    if (cmd === 'store.folder-share.ls') return { shares: rows };
    if (cmd === 'key.did') return { did: 'did:key:owner' };
    if (cmd === 'store.browse-dirs') return { roots: [{ label: 'Root', path: '/shared' }], path: '/shared', parent: '/', dirs: [] };
    if (cmd === 'store.folder-share.stop') { rows = []; return {}; }
    if (cmd === 'store.folder-share') {
      if (fail) throw new Error('publish failed');
      return new Promise(resolve => { finish = resolve; });
    }
    throw new Error(cmd);
  }
});
const flush = async () => { for (let i = 0; i < 10; i++) await new Promise(r => setImmediate(r)); };
const modal = () => document.querySelector('.store-share-modal');
const step = () => modal().querySelector('.store-share-body').dataset.step;
const clickText = text => [...modal().querySelectorAll('button')].find(b => b.textContent === text).click();
const submit = () => modal().querySelector('form').dispatchEvent(new window.Event('submit', { cancelable: true }));
await feature.refresh();
assert.equal(document.querySelector('img'), null);
assert.ok(document.querySelector('.store-share-info').textContent.includes('Files: 1'));
document.querySelector('[data-share-action="0"]').click(); await flush();
assert.equal(copies.at(-1), link);
feature.open(); submit(); assert.equal(step(), 'path');
clickText('Browse…'); await flush(); clickText('Use this folder');
submit(); assert.equal(step(), 'password');
clickText('Generate'); const generated = modal().querySelector('input').value;
assert.match(generated, /^[A-Za-z0-9_-]{24}$/);
clickText('Show'); assert.equal(modal().querySelector('input').type, 'text');
submit(); assert.equal(step(), 'confirm');
clickText('Back'); assert.equal(modal().querySelector('input').value, generated);
await feature.refresh(); assert.equal(modal().querySelector('input').value, generated);
submit(); submit(); await flush(); assert.equal(step(), 'busy');
assert.deepEqual(calls.find(c => c.cmd === 'store.folder-share').args, { path: '/shared', passphrase: generated });
assert.equal(modal().querySelector('form'), null);
finish({ share_url: link }); await flush(); assert.equal(step(), 'result');
assert.equal(modal().querySelector('input[type=password]').value, generated);
clickText('Copy'); await flush(); assert.equal(copies.at(-1), link);
modal().dispatchEvent(new window.KeyboardEvent('keydown', { key: 'Escape', bubbles: true }));
assert.equal(modal(), null);
document.querySelector('[data-share-action="1"]').click();
assert.equal(calls.some(c => c.cmd === 'store.folder-share.stop'), false);
clickText('Back'); assert.equal(modal(), null);
document.querySelector('[data-share-action="1"]').click(); clickText('Stop'); await flush();
assert.deepEqual(calls.at(-2), { cmd: 'store.folder-share.stop', args: { folder_id: share.folder_id } });
assert.equal(document.querySelector('.store-share-row'), null);
feature.open(); modal().querySelector('input').value = '/shared'; submit();
modal().querySelector('input').value = 'test'; submit(); fail = true; submit(); await flush();
assert.equal(step(), 'confirm'); assert.ok(modal().textContent.includes('publish failed'));
modal().querySelector('.modal-close').click(); assert.equal(modal(), null);
fail = false;
feature.open(); modal().querySelector('input').value = '/shared'; submit();
modal().querySelector('input').value = 'test'; submit(); submit(); await flush();
assert.equal(step(), 'busy');
modal().querySelector('.modal-close').click(); assert.equal(modal(), null);
feature.open(); assert.equal(modal(), null, 'no duplicate publication while a request is pending');
finish({ share_url: embedded }); await flush();
assert.equal(modal(), null, 'late success never reopens a dismissed dialog');
feature.open(); modal().querySelector('input').value = '/shared'; submit();
modal().querySelector('input').value = 'test'; submit(); submit(); await flush();
finish({ share_url: embedded }); await flush();
assert.ok(modal().textContent.includes('passphrase is included'));
assert.equal(modal().querySelector('input[type=password]'), null);
modal().querySelector('.modal-close').click();
locale = 'ja'; await feature.refresh(); assert.ok(document.querySelector('.store-share-heading').textContent.includes('フォルダーを共有'));
feature.open(); modal().dispatchEvent(new window.MouseEvent('click', { bubbles: true })); assert.equal(modal(), null);
locale = 'zh'; await feature.refresh(); assert.ok(document.querySelector('.store-share-heading').textContent.includes('共享文件夹'));
console.log('PASS storage share: helpers, link compatibility, wizard, browse, busy/result, errors, confirmation, translations');
await window.happyDOM.close();
