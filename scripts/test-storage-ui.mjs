import assert from 'node:assert/strict';
import fs from 'node:fs';
import { Window } from 'happy-dom';

const window = new Window({ url: 'http://localhost/' });
const document = window.document;
const html = fs.readFileSync(new URL('../src/web/assets/index.html', import.meta.url), 'utf8');
const start = html.indexOf('  <section class="panel panel-span2" id="panel-store">');
const end = html.indexOf('  <section class="panel panel-span2" id="panel-scheduler">', start);
assert.ok(start > 0 && end > start);
document.body.innerHTML = html.slice(start, end);
window.eval(fs.readFileSync(new URL('../src/web/assets/features/storage.js', import.meta.url), 'utf8'));

const root = document.getElementById('panel-store');
assert.equal(root.querySelectorAll('.store-main > .subtab-page').length, 3, 'All three storage destinations stay in the workspace');
assert.ok(root.contains(document.getElementById('store-sync-form')), 'Folder sync remains in its original panel');
const copies = [], saves = [];
let uploads = 0;
let locale = 'en';
const browser = window.MistlStorage.create({
  root,
  t: (key, values = {}) => `${locale}:${key}:${Object.values(values).join('|')}`,
  getLocale: () => locale,
  formatBytes: size => `${size} B`,
  copy: value => copies.push(value),
  saveToPath: (entry, path) => saves.push([entry.cid, path]),
  pickFiles: () => uploads++,
  downloadUrl: entry => `/api/store/download?cid=${encodeURIComponent(entry.cid)}`,
});
const fixtures = [
  { cid: 'cid-10', name: 'File10.txt', size: 12, stored_at: '2026-01-01T00:00:00Z' },
  { cid: 'cid-2', name: 'File2.txt', size: 2, stored_at: '2026-02-01T00:00:00Z' },
  { cid: 'cid-image', name: '<img src=x onerror=alert(1)>.png', size: 1000, stored_at: '2026-03-01T00:00:00Z' },
];
const rowNames = () => [...root.querySelectorAll('#store-table .store-file-name')].map(el => el.textContent);
browser.render(fixtures);
assert.equal(root.querySelectorAll('#store-table tbody tr').length, 3);
assert.equal(root.querySelector('a[download="File2.txt"]').getAttribute('href'), '/api/store/download?cid=cid-2', 'Downloads use the injected transport adapter and retain the filename');
assert.deepEqual(rowNames().slice(-2), ['File2.txt', 'File10.txt'], 'Numeric filenames use natural sorting');
assert.equal(root.querySelectorAll('img').length, 0, 'Names never become HTML');
assert.equal(fixtures[0].cid, 'cid-10', 'Presentation sorting never mutates server data');

const search = root.querySelector('#store-search');
search.value = 'CID-2';
search.dispatchEvent(new window.Event('input'));
assert.deepEqual(rowNames(), ['File2.txt'], 'Search includes full CID and ignores case');
search.value = 'missing';
search.dispatchEvent(new window.Event('input'));
assert.equal(root.querySelector('#store-empty').hidden, false);
assert.match(root.querySelector('#store-empty').textContent, /noResults/);
search.value = '';
search.dispatchEvent(new window.Event('input'));
const sort = root.querySelector('#store-sort');
sort.value = 'recent';
sort.dispatchEvent(new window.Event('change'));
assert.equal(rowNames()[0], fixtures[2].name);

const opener = root.querySelector('[data-store-cid="cid-2"]');
opener.click();
const detail = root.querySelector('[data-store-detail]');
assert.equal(detail.hidden, false);
assert.equal(document.activeElement.id, 'store-detail-title', 'Opening details moves keyboard focus to the readable filename');
assert.ok(detail.textContent.includes('cid-2'));
const path = detail.querySelector('input');
path.value = 'C:\\Downloads\\example.txt';
path.focus();
browser.render(fixtures.slice());
assert.equal(detail.querySelector('input'), path, 'Identical polls preserve detail DOM and drafts');
browser.render([...fixtures, { cid: 'new', name: 'New.txt', size: 0 }]);
assert.equal(detail.querySelector('input'), path, 'An unrelated new file does not erase a typed save path');
assert.equal(document.activeElement, path);
detail.querySelector('form').dispatchEvent(new window.Event('submit', { cancelable: true }));
assert.deepEqual(saves, [['cid-2', 'C:\\Downloads\\example.txt']]);
detail.querySelector('.store-detail-actions button').click();
assert.deepEqual(copies, ['cid-2']);
detail.dispatchEvent(new window.KeyboardEvent('keydown', { key: 'Escape', bubbles: true }));
assert.equal(detail.hidden, true);
assert.equal(document.activeElement.dataset.storeCid, 'cid-2', 'Escape restores focus to the file');

root.querySelector('[data-store-view="grid"]').click();
assert.equal(root.querySelector('[data-store-grid]').hidden, false);
assert.equal(root.querySelector('#store-table').hidden, true);
root.querySelector('[data-store-cid="cid-image"]').click();
assert.equal(detail.querySelector('h3').textContent, fixtures[2].name, 'Full filenames are available from the grid too');
assert.equal(root.querySelectorAll('img').length, 0);
locale = 'ja';
browser.render(fixtures);
assert.match(detail.querySelector('.store-eyebrow').textContent, /^ja:/, 'Open details follow language changes');
root.querySelector('[data-store-upload]').click();
assert.equal(uploads, 1);
browser.render([]);
assert.equal(detail.hidden, true, 'Removed selections close cleanly');
assert.match(root.querySelector('#store-empty').textContent, /browser.empty/);

for (const match of fs.readFileSync(new URL('../src/web/assets/features/storage.js', import.meta.url), 'utf8').matchAll(/t\("(store\.[^"]+)"/g)) {
  if (match[1].endsWith('.')) continue;
  const occurrences = html.split(`"${match[1]}":`).length - 1;
  assert.equal(occurrences, 3, `${match[1]} must be translated in EN, JA and ZH`);
}
console.log('PASS: storage workspace structure, filtering, natural sorting, safe filenames, details, polling draft preservation, save/download adapters, grid view, focus and localization');
window.happyDOM.abort();
