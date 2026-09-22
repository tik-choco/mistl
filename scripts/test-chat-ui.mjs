import assert from 'node:assert/strict';
import fs from 'node:fs';
import { Window } from 'happy-dom';

const source = fs.readFileSync(new URL('../src/web/assets/features/chat.js', import.meta.url), 'utf8');
const status = { enabled: true, rooms: [{ room: 'general', joined: true }, { room: '日本語 / 長いルーム名', joined: false }] };
const post = (id, text, extra = {}) => ({ id, type: 'tc-chat:post', kind: 'text', fromId: 'did:key:author', fromName: 'Alice', timestamp: 1700000000000, text, ...extra });
const settle = () => new Promise(resolve => setImmediate(resolve));
const deferred = () => { let resolve, reject; const promise = new Promise((yes, no) => { resolve = yes; reject = no; }); return { promise, resolve, reject }; };

function mount(overrides = {}) {
  const window = new Window({ url: 'http://localhost/' });
  window.eval(source);
  const root = window.document.createElement('div');
  window.document.body.appendChild(root);
  let locale = 'ja', logs = [post('one', 'Hello')], settings = 0;
  const changes = [], calls = [];
  const ports = {
    language: () => locale,
    readRooms: () => Promise.resolve(status),
    readLog: (room, limit) => { calls.push({ room, limit }); return Promise.resolve(logs); },
    formatBytes: bytes => `${bytes} B`,
    openSettings: () => { settings++; },
    changed: snapshot => changes.push(snapshot),
    ...overrides,
  };
  const view = window.MistlChat.create({ root, ports });
  return { window, root, view, ports, calls, changes, setLogs: value => { logs = value; }, setLocale: value => { locale = value; }, settings: () => settings };
}

const app = mount();
await app.view.refresh();
assert.equal(app.view.selectedRoom(), 'general');
assert.equal(app.calls[0].limit, 100);
assert.equal(app.root.querySelectorAll('.mc-room').length, 2);
assert.equal(app.root.querySelector('.mc-room').getAttribute('aria-pressed'), 'true');
assert.equal(app.root.querySelector('.mc-message-text').textContent, 'Hello');
assert.equal(app.root.querySelector('.mc-open').hidden, true, 'No invented deployment link');
assert.equal(app.root.querySelector('textarea'), null, 'Read-only relay must not pretend to send messages');
app.root.querySelector('.mc-setup').click();
assert.equal(app.settings(), 1);

const longText = '<img src=x onerror=alert(1)>\n' + '見切れない長いメッセージ '.repeat(300);
app.setLogs([post('one', longText)]);
await app.view.refresh();
assert.equal(app.root.querySelector('.mc-message-text').textContent, longText, 'Full message remains available');
assert.equal(app.root.querySelector('img'), null, 'Untrusted message content stays literal text');
const viewport = app.root.querySelector('.mc-viewport');
Object.defineProperties(viewport, { scrollHeight: { value: 1000, configurable: true }, clientHeight: { value: 300, configurable: true } });
viewport.scrollTop = 120;
const firstNode = app.root.querySelector('.mc-message');
await app.view.refresh();
assert.equal(app.root.querySelector('.mc-message'), firstNode, 'Unchanged polls preserve DOM and text selection');
app.setLogs([post('one', longText), post('two', 'Another message')]);
await app.view.refresh();
assert.equal(app.root.querySelector('.mc-message'), firstNode, 'New messages append without rebuilding history');
assert.equal(viewport.scrollTop, 120, 'Incoming messages do not interrupt reading');
assert.equal(app.root.querySelector('.mc-latest').hidden, false);
app.root.querySelector('.mc-latest').click();
assert.equal(viewport.scrollTop, 1000);
assert.equal(app.root.querySelector('.mc-latest').hidden, true);

app.root.querySelector('.mc-author').click();
assert.equal(app.root.querySelector('dialog').open, true);
assert.ok(app.root.querySelector('.mc-detail-body').textContent.includes('did:key:author'));
assert.equal(app.root.querySelector('.mc-detail-text').textContent, longText);
assert.equal(app.root.querySelector('.mc-technical').open, false, 'Wire metadata stays behind disclosure');
app.root.querySelector('.mc-close').click();
assert.equal(app.root.querySelector('dialog').open, false);

const search = app.root.querySelector('.mc-header input');
search.value = 'another'; search.dispatchEvent(new app.window.Event('input'));
assert.equal(app.root.querySelectorAll('.mc-message').length, 1);
assert.equal(app.root.querySelector('.mc-message-text').textContent, 'Another message');
search.value = 'does not exist'; search.dispatchEvent(new app.window.Event('input'));
assert.equal(app.root.querySelectorAll('.mc-message').length, 0);
assert.ok(app.root.querySelector('.mc-empty').textContent.includes('一致するメッセージ'));
search.value = ''; search.dispatchEvent(new app.window.Event('input'));
assert.equal(app.root.querySelectorAll('.mc-message').length, 2);

app.setLocale('zh');
await app.view.setRooms(status);
assert.equal(search.placeholder, '搜索对话');
app.setLocale('en');
await app.view.setRooms(status);
assert.equal(search.placeholder, 'Search this conversation');
app.setLocale('unknown');
await app.view.setRooms(status);
assert.equal(search.placeholder, 'Search this conversation', 'Unsupported locale falls back to English');

const roomButtons = app.root.querySelectorAll('.mc-room');
roomButtons[1].click(); await settle();
assert.equal(app.view.selectedRoom(), status.rooms[1].room);
await app.view.setRooms({ ...status, rooms: [status.rooms[1], status.rooms[0]] });
assert.equal(app.view.selectedRoom(), status.rooms[1].room, 'Polling/reordering keeps selected room');
const roomSearch = app.root.querySelector('.mc-sidebar input');
roomSearch.value = 'general'; roomSearch.dispatchEvent(new app.window.Event('input'));
assert.equal(app.root.querySelectorAll('.mc-room').length, 1);
assert.equal(app.view.selectedRoom(), status.rooms[1].room, 'Room filtering does not change active conversation');

app.setLogs([post('deleted', 'Removed original'), { id: 'delete', type: 'tc-chat:post-delete', targetId: 'deleted', fromId: 'did:key:author' }, post('edited', 'Outdated original'), { id: 'edit', type: 'tc-chat:post-edit', targetId: 'edited', fromId: 'did:key:author' }]);
await app.view.refresh();
assert.ok(!app.root.querySelector('.mc-messages').textContent.includes('Removed original'), 'Deleted originals are not displayed');
assert.ok(!app.root.querySelector('.mc-messages').textContent.includes('Outdated original'), 'Edits without replacement bodies do not display stale originals');
app.root.querySelector('.mc-message-more').click();
assert.ok(!app.root.querySelector('.mc-technical').textContent.includes('Removed original'), 'Removed text is absent from technical details too');
app.root.querySelector('.mc-close').click();

app.setLogs([post('owned', 'Keep this message'), { id: 'foreign-delete', type: 'tc-chat:post-delete', targetId: 'owned', fromId: 'did:key:someone-else' }]);
await app.view.refresh();
assert.equal(app.root.querySelector('.mc-message-text').textContent, 'Keep this message', 'A different actor cannot hide another sender\'s message');
app.root.querySelector('.mc-message-more').click();
const openTechnical = app.root.querySelector('.mc-technical');
openTechnical.open = true;
openTechnical.querySelector('summary').focus();
await app.view.refresh();
assert.equal(app.root.querySelector('.mc-technical'), openTechnical, 'Unchanged polling preserves the open detail DOM');
assert.equal(app.window.document.activeElement, openTechnical.querySelector('summary'));
app.setLogs([post('owned', 'Keep this message'), { id: 'own-delete', type: 'tc-chat:post-delete', targetId: 'owned', fromId: 'did:key:author' }]);
await app.view.refresh();
assert.ok(!app.root.querySelector('.mc-detail-text').textContent.includes('Keep this message'), 'Open details update after deletion');
assert.ok(!app.root.querySelector('.mc-technical').textContent.includes('Keep this message'), 'Already-open technical details also remove deleted originals');
assert.equal(app.root.querySelector('.mc-technical').open, true, 'Updating content preserves technical disclosure');
assert.equal(app.window.document.activeElement, app.root.querySelector('.mc-technical summary'), 'Updating details preserves keyboard focus');
app.root.querySelector('.mc-close').click();
await settle();
assert.equal(app.window.document.activeElement, app.root.querySelector('.mc-message-more'), 'Closing finds the opener replaced during polling');

app.root.querySelector('.mc-message-more').click();
await app.view.setRooms({ enabled: false, rooms: status.rooms });
assert.equal(app.root.querySelectorAll('.mc-message').length, 0);
assert.equal(app.root.querySelector('dialog').open, false, 'Disabling relay closes an open message detail');
assert.ok(app.root.querySelector('.mc-empty').textContent.includes('Turn on tc-chat'));
await app.view.setRooms({ enabled: true, rooms: [] });
assert.equal(app.view.selectedRoom(), '');
assert.ok(app.root.querySelector('.mc-empty').textContent.includes('Add a tc-chat room'));

// A slow response for the previous room must not replace the chosen room.
const oldRequest = deferred(), newRequest = deferred();
const race = mount({ readLog: room => room === 'general' ? oldRequest.promise : newRequest.promise });
const started = race.view.refresh(); await settle();
race.root.querySelectorAll('.mc-room')[1].click(); await settle();
newRequest.resolve([post('new', 'Current room')]); await settle();
oldRequest.resolve([post('old', 'Wrong room')]); await started;
assert.equal(race.root.querySelector('.mc-message-text').textContent, 'Current room');
assert.equal(race.view.selectedRoom(), status.rooms[1].room);

const disableRequest = deferred();
const disable = mount({ readLog: () => disableRequest.promise });
const disabledPending = disable.view.refresh(); await settle();
await disable.view.setRooms({ enabled: false, rooms: status.rooms });
disableRequest.resolve([post('old', 'Should not appear')]); await disabledPending;
assert.equal(disable.root.querySelectorAll('.mc-message').length, 0, 'Disabling invalidates pending room requests');

const links = mount({ roomUrl: room => 'https://chat.example/#/' + encodeURIComponent(room) });
await links.view.refresh();
links.root.querySelectorAll('.mc-room')[1].click(); await settle();
assert.equal(new URL(links.root.querySelector('.mc-open').href).hash, '#/' + encodeURIComponent(status.rooms[1].room));
links.ports.roomUrl = () => 'javascript:alert(1)'; await links.view.refresh();
assert.equal(links.root.querySelector('.mc-open').hidden, true, 'Unsafe links are rejected');
links.ports.readRooms = () => Promise.reject(new Error('offline'));
await links.view.refresh();
assert.equal(links.root.querySelector('.mc-error').hidden, false);
assert.equal(links.root.querySelectorAll('.mc-message').length, 1, 'Transient errors retain existing conversation');
links.root.querySelector('.mc-message-more').click();
await links.view.setRooms({ enabled: true, rooms: [] });
assert.equal(links.root.querySelector('.mc-error').hidden, true, 'Successful empty status clears the previous load error');
assert.equal(links.root.querySelector('dialog').open, false, 'Removing a room closes its message detail');

console.log('PASS: tc-chat layout ports, complete literal text, detail drawer, localization, search, selected room preservation, incremental polling, scroll preservation, request races, deletion/edit display, disabled state, safe links, recoverable errors');
