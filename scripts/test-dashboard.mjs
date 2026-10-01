// Assemble the production page, feature assets and API adapters together.
// Deterministic DOM integration; visual layout still needs browser review.
import assert from 'node:assert/strict';
import fs from 'node:fs';
import { Window } from 'happy-dom';

const html = fs.readFileSync(new URL('../src/web/assets/index.html', import.meta.url), 'utf8');
const scripts = [...html.matchAll(/<script\b([^>]*)>([\s\S]*?)<\/script>/g)];
const longName = '設計資料とレビュー用の長いファイル名'.repeat(8) + '<img src=x onerror=alert(1)>.txt';
const longId = 'did:key:z6Mk' + 'sample-long-node-identifier'.repeat(8);
const room = 'design-review';
const calls = [], intervals = [], errors = [];
const build = {channel:'dev', version:'0.1.0', instance:'ui-test', instance_id:'mistl-ui-test', build_id:'ui-test-build', mistlib_version:'0.6.2'};
const chatMessages = [{type:'tc-chat:post', id:'message-1', fromId:longId, fromName:'桜田さん', timestamp:1789999200000, kind:'text', text:'こんにちは <script>alert(1)</script>\n' + '長い文章も最後まで読めます。'.repeat(20)}];
const fixtures = {
  'daemon.status': {pid:123, version:'0.1.0', uptime_secs:10, build, network:{enabled:false, state:'off', saved:true}},
  'stream.status': {running:false},
  'ai.status': {providing:false, serving:false},
  'topology.status': {self:{node_id:longId, did:longId, display_name:'長い名前のノード'.repeat(12)}, rooms:[room], peers:[{node_id:'peer-two',state:'connected'}], room_peers:[{room,peers:[{node_id:'peer-two',state:'connected'}]}], modules:{chat_relay:{rooms:[{room,joined:true}]}, store:{rooms:[]}}, stream:{running:false}},
  'profile.show': {did:longId, display_name:'UI test'},
  'chat.rooms': {enabled:true, rooms:[{room,joined:true},{room:'random',joined:false}]},
  'store.ls': [{cid:'bafy-long-file-id',name:longName,size:12400,stored_at:'2026-09-22T00:00:00Z'}, {cid:'bafy-photo',name:'photo.png',size:4567,stored_at:'2026-09-21T00:00:00Z'}],
  'store.sandbox.ls': {entries:[]},
  'store.folder-sync.ls': {syncs:[]},
  'store.browse-dirs': {default_export_dir:'C:\\Downloads',entries:[]},
  'sched.ls': {jobs:[]}, 'sched.logs': {runs:[]},
  'bot.list': {pipelines:[]}, 'bot.logs': {runs:[]}, 'bot.items': {items:[]},
  'config.show': {identity:{display_name:'UI test'},storage:{room_ids:[]},stream:{},chat_relay:{enabled:true,rooms:[room]},ai:{providers:[],presets:[]},scheduler:{enabled:false},bot:{enabled:false},update:{},ui:{enabled:true,listen:'127.0.0.1:6480'}},
  'update.status': {current:'0.1.0'}, 'tunnel.status': {running:false, peers:[], forwards:[]}, 'tunnel.room.list': {rooms:[]},
};
const window = new Window({url:'http://127.0.0.1:6480/#panel-store',settings:{disableJavaScriptEvaluation:true, disableJavaScriptFileLoading:true, disableCSSFileLoading:true}});
const {document} = window;
const onUnhandled = error => errors.push(error);
process.on('unhandledRejection', onUnhandled);
window.addEventListener('error', event => errors.push(event.error || event.message));
window.localStorage.setItem('mistl-lang', 'ja');
window.mistlRuntime = build;
window.setInterval = (callback, ms) => { intervals.push({callback,ms}); return intervals.length; };
window.clearInterval = () => {};
window.fetch = async (url, options) => {
  assert.equal(url, '/api/call', 'no feature makes an unexpected network request');
  const request = JSON.parse(options.body);
  calls.push(request);
  assert.equal(options.headers['x-mistl-ui'], '1');
  let data;
  if (request.cmd === 'chat.log') data = request.args.room === room ? chatMessages : [];
  else if (request.cmd === 'tunnel.room.set') { fixtures['tunnel.status'].room = request.args.room; data = {room:request.args.room}; }
  else if (request.cmd === 'tunnel.start') { Object.assign(fixtures['tunnel.status'], {running:true,self_id:'self',graph:{policy:{broadcast:true,locked:false,links:[],permissions:{}},nodes:[]}}); data = {running:true}; }
  else if (request.cmd === 'config.set') data = {applies:'daemon restart'};
  else { assert.ok(Object.hasOwn(fixtures, request.cmd), 'fixture for ' + request.cmd); data = fixtures[request.cmd]; }
  return {ok:true, json:async () => ({ok:true, data:structuredClone(data)})};
};
const flush = async () => { for (let i=0; i<15; i++) await new Promise(resolve => setImmediate(resolve)); };
try {
  document.write(html.replace(/<script\b[^>]*>[\s\S]*?<\/script>/g, ''));
  for (const [, attrs, source] of scripts) {
    const src = attrs.match(/src="([^"]+)"/);
    window.eval(src ? fs.readFileSync(new URL('../src/web' + src[1], import.meta.url), 'utf8') : source);
  }
  await flush();
  assert.deepEqual(errors, [], 'page boots without runtime errors');
  assert.equal(document.querySelectorAll('.store-file-open').length, 2);
  const fileButton = [...document.querySelectorAll('.store-file-open')].find(node => node.title === longName);
  assert.ok(fileButton, 'long filename remains available in full');
  fileButton.click();
  assert.ok(document.querySelector('[data-store-detail]').textContent.includes(longName));
  assert.equal(document.querySelector('[data-store-detail] img'), null, 'filename is text, not HTML');
  document.querySelector('[data-store-view="grid"]').click();
  assert.equal(document.querySelectorAll('.store-tile-open').length, 2);
  const search = document.getElementById('store-search');
  search.value = 'photo'; search.dispatchEvent(new window.Event('input'));
  assert.equal(document.querySelectorAll('.store-tile-open').length, 1);
  search.value = ''; search.dispatchEvent(new window.Event('input'));

  const selfNode = [...document.querySelectorAll('.topology-node-clickable')].find(node => node.getAttribute('aria-label')?.includes('長い名前'));
  assert.ok(selfNode, 'interactive topology nodes are present');
  selfNode.dispatchEvent(new window.KeyboardEvent('keydown', {key:'Enter', bubbles:true}));
  const topologyDialog = document.querySelector('.topology-detail');
  assert.equal(topologyDialog.open, true);
  assert.ok(topologyDialog.textContent.includes(longId), 'detail carries the full node ID');
  topologyDialog.querySelector('.topology-detail-close').click();

  document.querySelector('a[href="#panel-chat"]').click();
  document.querySelector('#panel-chat [data-subtab-target="rooms"]').click();
  await flush();
  assert.equal(document.querySelectorAll('.mc-message').length, 1);
  assert.ok(document.querySelector('.mc-message-text').textContent.includes('<script>alert(1)</script>'));
  assert.equal(document.querySelector('.mc-message-text script'), null);
  const initialMessage = document.querySelector('.mc-message');
  chatMessages.push({...chatMessages[0],id:'message-2',text:'新しいメッセージ',timestamp:1789999260000});
  window.__mistlViewSwitchUntil = 0;
  intervals.find(timer => timer.ms === 5000).callback();
  await flush();
  assert.equal(document.querySelectorAll('.mc-message').length, 2, 'scheduled polling refreshes the conversation');
  assert.equal(document.querySelector('.mc-message'), initialMessage, 'existing message DOM survives polling');
  document.querySelector('.mc-message-more').click();
  assert.equal(document.querySelector('.mc-dialog').open, true);
  assert.ok(document.querySelector('.mc-dialog').textContent.includes(longId));
  document.querySelector('.mc-close').click();

  for (const language of ['en','zh','ja']) {
    document.getElementById('lang-' + language + '-btn').click();
    await flush();
    assert.equal(document.documentElement.lang, language);
    assert.equal(document.querySelectorAll('.mc-message').length, 2);
    assert.equal(document.querySelectorAll('.store-tile-open').length, 2);
    assert.ok(!document.querySelector('[data-store-toolbar]').textContent.includes('store.browser.'));
  }
  const toggle = document.getElementById('settings-scheduler-enabled');
  toggle.checked = true; toggle.dispatchEvent(new window.Event('change'));
  await flush();
  const apply = toggle.closest('.settings-row').querySelector('.badge');
  assert.equal(apply.textContent, '変更を適用');
  assert.notEqual(apply.style.display, 'none', 'a pending change keeps a concrete action');
  assert.ok(!document.querySelector('.toast-stack')?.textContent.includes('再起動が必要'));
  document.getElementById('tunnel-room-input').value = 'graph-room';
  document.getElementById('tunnel-room-connect-form').dispatchEvent(new window.Event('submit',{bubbles:true,cancelable:true}));
  await flush();
  assert.deepEqual(calls.filter(c=>['tunnel.room.set','tunnel.start'].includes(c.cmd)).map(c=>c.cmd),['tunnel.room.set','tunnel.start']);
  assert.equal(document.querySelectorAll('#tunnel-graph .tg-node').length,1,'room Connect starts the session and displays the graph');
  const roomForm = document.getElementById('tunnel-room-connect-form');
  assert.equal(roomForm.hidden, true, 'a running session folds the room form behind Switch room');
  document.getElementById('btn-tunnel-room-toggle').click();
  assert.equal(roomForm.hidden, false);
  // One pending table for decisions this user must make; ids stay full and literal.
  const evilPeer = longId + '<img src=x onerror=alert(1)>';
  Object.assign(fixtures['tunnel.status'], {
    pending_auth: [{id:7, peer_id:evilPeer, verified:false, proto:'tcp', forward_key:'tcp:127.0.0.1:22', target_addr:'127.0.0.1:22'}],
    pending_forwards: [{req_id:'r1', peer_id:'peer-two', proto:'udp', remote_addr:'127.0.0.1:9000', target:'udp:127.0.0.1:9000@self'}],
    pending_outgoing: [{req_id:'out-1', sent_at_ms:1, peer_id:'peer-two', proto:'tcp', local_addr:'127.0.0.1:8080', remote_addr:'127.0.0.1:80', target:'tcp:127.0.0.1:80@peer-two'}],
  });
  fixtures['tunnel.auth.approve'] = {ok:true};
  document.querySelector('[data-target="tunnel"]').click();
  await flush();
  const pendingRows = document.querySelectorAll('#tunnel-pending-table tbody tr');
  assert.equal(document.getElementById('tunnel-pending-section').hidden, false);
  assert.equal(pendingRows.length, 2, 'own proposals are not decisions for this user, so they are not in the pending table');
  assert.equal(document.getElementById('tunnel-pending-count').textContent, '2', 'only actionable rows are counted');
  assert.equal(document.querySelector('#tunnel-pending-table img'), null);
  assert.ok(pendingRows[0].querySelector('.tunnel-grant-peer').textContent.startsWith(evilPeer), 'peer id is shown in full');
  assert.equal(pendingRows[0].querySelector('input[type=checkbox]').disabled, true, 'unverified peers cannot be remembered');
  pendingRows[0].querySelector('button.primary').click();
  await flush();
  assert.deepEqual(calls.filter(c=>c.cmd==='tunnel.auth.approve').map(c=>c.args), [{id:7, remember:false}]);
  const waiting = document.querySelector('#tunnel-graph .tg-outgoing .tg-wait');
  assert.ok(waiting && waiting.textContent.includes('peer-two'), 'an unanswered own proposal shows as waiting on the graph');
  assert.equal(document.getElementById('tunnel-history').tagName, 'DETAILS', 'trust & history no longer needs subtabs');
  assert.equal(document.querySelector('#panel-tunnel [data-subtab-target]'), null, 'the tunnel panel has no subtabs left');
  assert.deepEqual(errors, []);
  assert.equal(document.querySelectorAll('.toast.error').length, 0, 'no hidden initialization failure');
  console.log('PASS: full dashboard boot, feature composition, long/literal content, topology keyboard details, chat polling, all locales, pending settings action, tunnel session and pending approvals');
} finally {
  process.removeListener('unhandledRejection', onUnhandled);
  await window.happyDOM.close();
}
