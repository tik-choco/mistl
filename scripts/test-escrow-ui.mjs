// Assemble the production page, feature assets and API adapters together.
// Deterministic DOM integration; visual layout still needs browser review.
import assert from 'node:assert/strict';
import fs from 'node:fs';
import { Window } from 'happy-dom';
import './test-i18n.mjs';

const html = fs.readFileSync(new URL('../src/web/assets/index.html', import.meta.url), 'utf8');
const scripts = [...html.matchAll(/<script\b([^>]*)>([\s\S]*?)<\/script>/g)];
const longName = '\u8a2d\u8a08\u8cc7\u6599\u3068\u30ec\u30d3\u30e5\u30fc\u7528\u306e\u9577\u3044\u30d5\u30a1\u30a4\u30eb\u540d'.repeat(8) + '<img src=x onerror=alert(1)>.txt';
const longId = 'did:key:z6Mk' + 'sample-long-node-identifier'.repeat(8);
const room = 'design-review';
const calls = [], intervals = [], errors = [], archives = [];
const modelFailures=new Set();let rejectSetting=null;
const modelGates=new Map(), modelResults=new Map();
let modelNow=Date.now();
const build = {channel:'dev', version:'0.1.0', instance:'ui-test', instance_id:'mistl-ui-test', build_id:'ui-test-build', mistlib_version:'0.6.2'};
const chatMessages = [{type:'tc-chat:post', id:'message-1', fromId:longId, fromName:'\u685c\u7530\u3055\u3093', timestamp:1789999200000, kind:'text', text:'\u3053\u3093\u306b\u3061\u306f <script>alert(1)</script>\n' + '\u9577\u3044\u6587\u7ae0\u3082\u6700\u5f8c\u307e\u3067\u8aad\u3081\u307e\u3059\u3002'.repeat(20)}];
const fixtures = {
  'daemon.status': {pid:123, version:'0.1.0', uptime_secs:10, build, network:{enabled:false, state:'off', saved:true}},
  'stream.status': {running:false},
  'ai.external.get': {registrations:[]},
  'ai.status': {providing:true, serving:false, rooms:[{provider_id:'home',room:'home-room',enabled:true,joined:true,providing:true,peers:3},{provider_id:'team',room:'team-room',enabled:true,joined:false,providing:false,peers:0}]},
  'topology.status': {self:{node_id:longId, did:longId, display_name:'\u9577\u3044\u540d\u524d\u306e\u30ce\u30fc\u30c9'.repeat(12)}, rooms:[room], peers:[{node_id:'peer-two',state:'connected'}], room_peers:[{room,peers:[{node_id:'peer-two',state:'connected'}]}], modules:{ai:{room:'unrelated-mailbox',joined:false},chat_relay:{rooms:[{room,joined:true}]}, store:{rooms:[]}}, stream:{running:false}},
  'profile.show': {did:longId, display_name:'UI test'},
  'chat.rooms': {enabled:true, rooms:[{room,joined:true},{room:'random',joined:false}]},
  'store.ls': [{cid:'bafy-long-file-id',name:longName,size:12400,stored_at:'2026-09-22T00:00:00Z'}, {cid:'bafy-photo',name:'photo.png',size:4567,stored_at:'2026-09-21T00:00:00Z'}],
  'store.sandbox.ls': {entries:[]},
  'store.folder-sync.ls': {syncs:[]},
  'store.browse-dirs': {default_export_dir:'C:\\Downloads',entries:[]},
  'sched.ls': {jobs:[]}, 'sched.logs': {runs:[]},
  'bot.list': {pipelines:[]}, 'bot.logs': {runs:[]}, 'bot.items': {items:[]},
  'config.show': {identity:{display_name:'UI test'},storage:{room_ids:[]},stream:{},chat_relay:{enabled:true,rooms:[room]},ai:{
    request_timeout_secs:120, api_listen:'127.0.0.1:6478',
    default_ref:{provider_id:'old',model:'gem-old'},
    tts:{provider_id:'old',model:'tts-old',voice:'voice-old',lang_voices:{en:'voice-en'}}, stt:null,
    providers:[
      {id:'local',label:'Ollama <img src=x>',base_url:'http://localhost:11434/v1',api_key:'***',models:['gem-small',...Array.from({length:300},(_,i)=>'model-'+i)]},
      {id:'old',label:'Old server',base_url:'https://old.example/v1',api_key:'***',enabled:false,models:['gem-old']},
      {id:'home',label:'Home',base_url:'mist-network://home-room',api_key:'',models:['gem-small','stale-model'],provide:true,shared:[{provider_id:'local',model:'gem-small'}]},
      {id:'team',label:'Team',base_url:'mist-network://team-room',api_key:'',models:['gem-team'],provide:false,shared:[]}
    ]},scheduler:{enabled:false},bot:{enabled:false,pipelines:[]},update:{},ui:{enabled:true,listen:'127.0.0.1:6480'}},
  'bot.options':{rooms:[{room,joined:true}]},
  'update.status': {current:'0.1.0'}, 'tunnel.status': {running:false, peers:[], forwards:[]}, 'tunnel.room.list': {rooms:[]},
};
const window = new Window({url:'http://127.0.0.1:6480/#panel-store',settings:{disableJavaScriptEvaluation:true, disableJavaScriptFileLoading:true, disableCSSFileLoading:true}});
const {document} = window;
const reducedMotion=Object.assign(new window.EventTarget(),{matches:false});
const matchMedia=window.matchMedia.bind(window);
window.matchMedia=query=>query==='(prefers-reduced-motion: reduce)'?reducedMotion:matchMedia(query);
window.Date.now=()=>modelNow;
const onUnhandled = error => errors.push(error);
process.on('unhandledRejection', onUnhandled);
window.addEventListener('error', event => errors.push(event.error || event.message));
window.localStorage.setItem('mistl-lang', 'ja');
window.mistlRuntime = build;
window.setInterval = (callback, ms) => { intervals.push({callback,ms}); return intervals.length; };
window.clearInterval = () => {};
window.confirm = () => true;
window.URL.createObjectURL = () => 'blob:escrow-test';
window.URL.revokeObjectURL = () => {};
window.HTMLAnchorElement.prototype.click = function () {};
window.fetch = async (url, options) => {
  if (url.startsWith('/api/escrow/export')) {
    archives.push({url, options});
    return {ok:true,headers:{get:()=>''},blob:async()=>new window.Blob(['test'])};
  }
  if (url.startsWith('/api/escrow/import')) {
    archives.push({url, options});
    return {ok:true,json:async()=>({ok:true,data:{imported:1,unchanged:0,skipped:[],rooms:[]}})};
  }
  assert.equal(url, '/api/call', 'no feature makes an unexpected network request');
  const request = JSON.parse(options.body);
  calls.push(request);
  assert.equal(options.headers['x-mistl-ui'], '1');
  let data;
  if (request.cmd === "escrow.rooms.add" || request.cmd === "escrow.rooms.remove") {
    if (failEscrow) return {ok:true,json:async()=>({ok:false,error:"room rejected"})};
    const cfg=fixtures["config.show"].escrow;
    if(request.cmd.endsWith("add")) { if(!cfg.rooms.includes(request.args.room)) cfg.rooms.push(request.args.room); cfg.enabled=true; }
    else cfg.rooms=cfg.rooms.filter(r=>r!==request.args.room);
    const status=fixtures["escrow.status"];status.enabled=cfg.enabled;
    for(const r of status.rooms) r.configured=cfg.rooms.includes(r.room);
    for(const room of cfg.rooms) if(!status.rooms.some(r=>r.room===room)) status.rooms.push({room,configured:true,joined:false,slots:0,bytes:0});
    return {ok:true,json:async()=>({ok:true,data:{applies:"daemon restart",enabled:cfg.enabled,rooms:cfg.rooms}})};
  }
  if (request.cmd === 'chat.log') data = request.args.room === room ? chatMessages : [];
  else if (request.cmd === 'tunnel.room.set') { fixtures['tunnel.status'].room = request.args.room; data = {room:request.args.room}; }
  else if (request.cmd === 'tunnel.start') { Object.assign(fixtures['tunnel.status'], {running:true,self_id:'self',graph:{policy:{broadcast:true,locked:false,links:[],permissions:{}},nodes:[]}}); data = {running:true}; }
  else if (request.cmd === 'ai.upstream_models') {
    const p=fixtures['config.show'].ai.providers.find(p=>p.id===request.args.provider_id);
    assert.ok(p && p.enabled!==false,'disabled providers are never queried');
    if(modelGates.has(p.id))await modelGates.get(p.id).promise;
    data=modelFailures.has(p.id)?{models:p.models,live:false,error:'models endpoint unavailable'}:modelResults.get(p.id)||{models:p.id==='home'?['gem-small']:p.models,live:p.id!=='team'};
  }
  else if (request.cmd === 'ai.external.remove') {
    const regs=fixtures['ai.external.get'].registrations;
    const before=regs.length;
    fixtures['ai.external.get'].registrations=regs.filter(r=>r.owner!==request.args.owner);
    data={owner:request.args.owner,removed:before!==fixtures['ai.external.get'].registrations.length};
  }
  else if (request.cmd === 'config.set') {
    if(request.args.path===rejectSetting)return {ok:true,json:async()=>({ok:false,error:'setting rejected'})};
    const [section,...keys]=request.args.path.split('.');
    let target=fixtures['config.show'][section];
    for(const key of keys.slice(0,-1)) target=target[key]??={};
    target[keys.at(-1)]=structuredClone(request.args.value);
    if (request.args.path === 'ai.providers') {
      fixtures['ai.status'].rooms = request.args.value.filter(p=>p.base_url.startsWith('mist-network://')).map(p=>{
        const previous = fixtures['ai.status'].rooms.find(room=>room.provider_id===p.id);
        return {...previous,provider_id:p.id,room:p.base_url.slice(15),enabled:p.enabled!==false,providing:p.enabled!==false && !!p.provide};
      });
      fixtures['ai.status'].providing = fixtures['ai.status'].rooms.some(room=>room.providing);
    }
    data = {applies:section==='ai'?'applied immediately':'daemon restart'};
  }
  else { assert.ok(Object.hasOwn(fixtures, request.cmd), 'fixture for ' + request.cmd); data = fixtures[request.cmd]; }
  return {ok:true, json:async () => ({ok:true, data:structuredClone(data)})};
};
const flush = async () => { await new Promise(resolve=>setTimeout(resolve,5)); for (let i=0; i<15; i++) await new Promise(resolve => setImmediate(resolve)); };
let failEscrow=false;
fixtures["config.show"].escrow={enabled:false,rooms:["configured"],max_blob_bytes:33554432,max_total_bytes:1073741824,max_slots_per_room:1024,auto_replicas:3,dead_after_secs:86400};
fixtures["escrow.status"]={enabled:false,running:true,applies:"daemon restart",bytes:5000,rooms:[
  {room:"configured",configured:true,joined:false,slots:2,bytes:400,auto:{target:3,slots:2,healthy:1,replicating:1,degraded:0,conflicts:0,held:1,insufficient_capacity:false,providers:[{did:'did:key:self',node:'self',self:true,online:true},{did:'did:key:<img src=x>',node:'peer',self:false,online:false}]}},
  {room:"old/<img src=x>",configured:false,joined:true,slots:3,bytes:500},
  {room:"raft",configured:false,joined:true,slots:0,bytes:0,replication:{role:'leader'}}
],head:{secret:"HEAD-SECRET"},blob:"BLOB-SECRET"};
fixtures["daemon.restart"]={restarting:true};
try {
  document.write(html.replace(/<script\b[^>]*>[\s\S]*?<\/script>/g, ''));
  for (const [, attrs, source] of scripts) {
    const src = attrs.match(/src="([^"]+)"/);
    window.eval(src ? fs.readFileSync(new URL('../src/web' + src[1], import.meta.url), 'utf8') : source);
  }
  await flush();
  assert.deepEqual(errors, []);
  const q=s=>document.querySelector(s), page=q('#panel-escrow');
  assert.equal(q('#settings-escrow-max_blob_bytes').value,'0.03125');
  assert.equal(q('#settings-escrow-max_total_bytes').value,'1');
  assert.equal(q('#settings-escrow-max_slots_per_room').value,'1024');
  assert.equal(q('#settings-escrow-auto_replicas').value,'3');
  assert.equal(q('#settings-escrow-dead_after_secs').value,'86400');
  window.location.hash='#panel-escrow';window.dispatchEvent(new window.Event('hashchange'));await flush();
  assert.ok(q('#panel-store').classList.contains('active'));
  assert.ok(page.classList.contains('active'));
  assert.equal(page.querySelectorAll('tbody tr').length,3);
  assert.match(page.querySelector('[data-escrow-summary]').textContent,/5000 B|4.9 KB/);
  assert.equal(page.querySelector('[data-escrow-restart-notice]').hidden,false);
  assert.equal(page.querySelectorAll('img').length,0);
  assert.ok(page.textContent.includes('old/<img src=x>'));
  assert.ok(!page.textContent.includes('HEAD-SECRET') && !page.textContent.includes('BLOB-SECRET'));
  assert.equal(page.querySelectorAll('tbody button:not([hidden])').length,7);
  const initialRows=[...page.querySelectorAll('tbody tr')], meshRow=initialRows[0], copies=meshRow.cells[5];
  assert.equal(meshRow.cells.length,7);
  assert.ok(copies.textContent.includes('1/2') && copies.querySelector('.escrow-providers').textContent.includes('1/2'));
  assert.equal(copies.querySelector('.escrow-copy-state').dataset.state,'replicating');
  assert.ok(copies.title.includes('did:key:self') && copies.title.includes('did:key:<img src=x>'));
  assert.ok(copies.getAttribute('aria-label').includes('did:key:self'));
  assert.equal(initialRows[1].cells[5].textContent,'');
  assert.equal(initialRows[2].cells[5].textContent,'');
  assert.equal(meshRow.querySelectorAll('.escrow-actions button').length,3);
  assert.equal(initialRows[2].querySelector('.escrow-actions button').disabled,true);
  meshRow.querySelectorAll('.escrow-actions button')[0].click();await flush();
  assert.equal(archives.at(-1).url,'/api/escrow/export?room=configured');
  meshRow.querySelectorAll('.escrow-actions button')[1].click();
  const archiveInput=page.querySelector('[data-escrow-file]');
  const file=new window.File(['checkpoint'],'checkpoints.mistl-escrow');
  Object.defineProperty(archiveInput,'files',{value:[file],configurable:true});
  archiveInput.dispatchEvent(new window.Event('change'));await flush();
  assert.equal(archives.at(-1).url,'/api/escrow/import?room=configured');
  assert.equal(archives.at(-1).options.headers['x-mistl-ui'],'1');
  assert.equal(archives.at(-1).options.headers['x-mistl-instance'],build.instance_id+'/'+build.build_id);
  assert.equal(archives.at(-1).options.body,file);
  const auto=fixtures['escrow.status'].rooms[0].auto;
  for(const [changes,state] of [[{healthy:2,replicating:0,degraded:0,conflicts:0},'healthy'],[{healthy:1,replicating:1},'replicating'],[{degraded:1},'degraded'],[{conflicts:1},'conflict']]) {
    Object.assign(auto,changes);page.querySelector('[data-escrow-refresh]').click();await flush();
    assert.equal(copies.querySelector('.escrow-copy-state').dataset.state,state);
  }
  auto.insufficient_capacity=true;page.querySelector('[data-escrow-refresh]').click();await flush();
  assert.ok(copies.title.includes('容量不足'));
  const input=q('#escrow-room'),form=page.querySelector('form'),error=page.querySelector('[data-escrow-error]');
  for(const value of ['', 'bad room', '日本語', 'x'.repeat(97), 'x\n']) assert.equal(window.MistlEscrow.validRoom(value),false,value);
  assert.equal(window.MistlEscrow.validRoom('A-z0_./:-'),true);
  for(const value of ['bad room','a'.repeat(97),'']) {
    input.value=value;const before=calls.length;form.dispatchEvent(new window.Event('submit',{cancelable:true}));await flush();
    assert.equal(calls.length,before,'invalid input never calls IPC');assert.equal(error.hidden,false);
  }
  input.value='tce-new';input.dispatchEvent(new window.Event('input'));form.dispatchEvent(new window.Event('submit',{cancelable:true}));await flush();
  assert.ok(calls.some(c=>c.cmd==='escrow.rooms.add'&&c.args.room==='tce-new'));
  assert.equal(input.value,'');assert.equal(q('#settings-escrow-enabled').checked,true);
  assert.ok([...q('#settings-drawer-body-escrow').querySelectorAll('.chip-text')].some(c=>c.textContent==='tce-new'));
  input.value='draft-room';input.focus();const row=[...page.querySelectorAll('tbody tr')].find(r=>r.cells[0].textContent==='tce-new'),remove=row.querySelector('button.danger');
  page.querySelector('[data-escrow-refresh]').click();await flush();
  assert.equal(input.value,'draft-room');assert.equal(document.activeElement,input);assert.equal(row.querySelector('button.danger'),remove);
  remove.click();await flush();assert.ok(calls.some(c=>c.cmd==='escrow.rooms.remove'&&c.args.room==='tce-new'));
  assert.equal(row.querySelector('button.danger').hidden,true,'retained room remains read-only');
  failEscrow=true;input.value='failed';form.dispatchEvent(new window.Event('submit',{cancelable:true}));await flush();assert.equal(input.value,'failed');assert.match(error.textContent,/room rejected/);assert.equal(page.querySelector('[data-escrow-add]').disabled,false);failEscrow=false;
  const chips=q('#settings-escrow-rooms');chips.value='invalid room';const before=calls.length;chips.dispatchEvent(new window.KeyboardEvent('keydown',{key:'Enter',bubbles:true}));await flush();assert.equal(calls.length,before);
  chips.value='valid-chip';chips.dispatchEvent(new window.KeyboardEvent('keydown',{key:'Enter',bubbles:true}));await flush();assert.ok(calls.some(c=>c.cmd==='config.set'&&c.args.path==='escrow.rooms'&&c.args.value.includes('valid-chip')));
  const blob=q('#settings-escrow-max_blob_bytes');blob.value='0.0625';blob.dispatchEvent(new window.Event('blur'));await flush();assert.ok(calls.some(c=>c.cmd==='config.set'&&c.args.path==='escrow.max_blob_bytes'&&c.args.value===67108864));
  assert.equal(blob.closest('.settings-field').querySelector('.badge.restart').style.display,'inline-block');
  for(const [key,value] of [['auto_replicas','4'],['dead_after_secs','120']]) {
    const field=q('#settings-escrow-'+key);field.value=value;field.dispatchEvent(new window.Event('blur'));await flush();
    assert.ok(calls.some(c=>c.cmd==='config.set'&&c.args.path==='escrow.'+key&&c.args.value===Number(value)));
  }
  const enabled=q('#settings-escrow-enabled');enabled.checked=false;enabled.dispatchEvent(new window.Event('change'));await flush();assert.ok(calls.some(c=>c.cmd==='config.set'&&c.args.path==='escrow.enabled'&&c.args.value===false));
  for(const language of ['en','zh','ja']) {q('#lang-'+language+'-btn').click();await flush();assert.ok(!page.textContent.includes('escrow.configured'));assert.ok(page.textContent.includes('Escrow'));assert.ok(!copies.textContent.includes('escrow.state.'));assert.ok(!copies.textContent.includes('escrow.providersOnline'));}
  window.__mistlViewSwitchUntil=0;const count=calls.filter(c=>c.cmd==='escrow.status').length;intervals.find(i=>i.ms===5000).callback();await flush();assert.ok(calls.filter(c=>c.cmd==='escrow.status').length>count);
  page.querySelector('[data-escrow-restart]').click();await flush();assert.ok(calls.some(c=>c.cmd==='daemon.restart'));
  console.log('PASS: Escrow status/mesh states and providers/safe text/row export-import/validation/add-remove/config settings/all locales/polling/restart');
} finally {
  process.removeListener('unhandledRejection', onUnhandled);
  window.happyDOM.abort();
}
