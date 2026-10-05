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
const calls = [], intervals = [], errors = [];
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
window.fetch = async (url, options) => {
  assert.equal(url, '/api/call', 'no feature makes an unexpected network request');
  const request = JSON.parse(options.body);
  calls.push(request);
  assert.equal(options.headers['x-mistl-ui'], '1');
  let data;
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

  const selfNode = [...document.querySelectorAll('.topology-node-clickable')].find(node => node.getAttribute('aria-label')?.includes('\u9577\u3044\u540d\u524d'));
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
  chatMessages.push({...chatMessages[0],id:'message-2',text:'\u65b0\u3057\u3044\u30e1\u30c3\u30bb\u30fc\u30b8',timestamp:1789999260000});
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
  assert.equal(apply.textContent, '\u5909\u66f4\u3092\u9069\u7528');
  assert.notEqual(apply.style.display, 'none', 'a pending change keeps a concrete action');
  assert.ok(!document.querySelector('.toast-stack')?.textContent.includes('\u518d\u8d77\u52d5\u304c\u5fc5\u8981'));
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
  // Model references, room cards and bot transforms share one picker.
  const aiCfg=fixtures['config.show'].ai;
  const aiWrites=()=>calls.filter(c=>c.cmd==='config.set' && c.args.path.startsWith('ai.'));
  const card=id=>document.querySelector('.ai-provider-card[data-provider-id="'+id+'"]');
  const sharing=()=>document.getElementById('ai-sharing-editor');
  const sharingRow=(provider,model)=>sharing().querySelector('.ai-sharing-row[data-provider-id="'+provider+'"][data-model="'+model+'"]');
  const assertUnique=nodes=>{
    const keys=[...nodes].map(n=>JSON.stringify([n.dataset.providerId,n.dataset.model]));
    assert.equal(new Set(keys).size,keys.length,'each provider/model is rendered once');
  };
  const input=(node,value)=>{node.value=value;node.dispatchEvent(new window.Event('input',{bubbles:true}));};
  const change=node=>node.dispatchEvent(new window.Event('change',{bubbles:true}));
  const openPicker=async id=>{document.querySelector('#'+id+' .ai-model-trigger').click();await flush();const dialog=document.querySelector('.ai-model-dialog');assertUnique(dialog.querySelectorAll('.ai-model-option'));return dialog;};
  const pick=async (id,provider,model)=>{
    const dialog=await openPicker(id);
    input(dialog.querySelector('input[type=search]'),model);
    const option=dialog.querySelector('button[data-provider-id="'+provider+'"][data-model="'+model+'"]');
    assert.ok(option,'model option '+provider+'/'+model);option.click();await flush();
  };
  const modelCalls=id=>calls.filter(c=>c.cmd==='ai.upstream_models' && (!id || c.args.provider_id===id)).length;
  const holdModels=id=>{let resolve;const promise=new Promise(r=>{resolve=r;});modelGates.set(id,{promise,resolve});};
  const releaseModels=id=>{modelGates.get(id).resolve();modelGates.delete(id);};
  assert.equal(modelCalls(),0,'hidden AI settings do not fetch models at startup');
  holdModels('local');
  document.querySelector('a[href="#panel-ai"]').click();await flush();
  assert.equal(modelCalls('local'),1,'opening AI starts discovery');
  assert.equal(card('local').querySelector('[role=status]').dataset.state,'fetching');
  assert.ok(sharingRow('local','model-299'),'sharing shows cached models immediately');
  document.querySelector('#panel-ai [data-subtab-target=sharing]').click();await flush();
  let pendingDialog=await openPicker('settings-ai-default_ref');
  pendingDialog.querySelector('[data-source=local]').click();
  assert.ok(pendingDialog.querySelector('[data-provider-id=local][data-model=model-299]'),'picker shows cache during revalidation');
  pendingDialog.querySelector('input[type=search]').focus();
  input(pendingDialog.querySelector('input[type=search]'),'fresh');
  sharing().querySelector('input[type=search]').focus();await flush();
  assert.equal(modelCalls('local'),1,'panel, picker and checklist dedupe an in-flight request');
  modelResults.set('local',{models:aiCfg.providers[0].models.concat(['fresh-model']),live:true});
  releaseModels('local');await flush();
  assert.ok(pendingDialog.querySelector('[data-provider-id=local][data-model=fresh-model]'),'open picker updates in place');
  assert.ok(sharingRow('local','fresh-model'),'sharing updates in place after discovery');
  assert.equal(pendingDialog.querySelector('input[type=search]').value,'fresh','query survives revalidation');
  assert.equal(card('local').querySelector('[role=status]').dataset.state,'ok');
  assert.equal(card('home').querySelector('[role=status]').dataset.state,'ok','room cards show fetch success');
  pendingDialog.querySelector('.modal-close').click();
  const justFetched=modelCalls('local');
  pendingDialog=await openPicker('settings-ai-default_ref');pendingDialog.querySelector('.modal-close').click();
  sharing().querySelector('input[type=search]').focus();await flush();
  document.querySelector('a[href="#panel-settings"]').click();await flush();
  document.querySelector('a[href="#panel-ai"]').click();await flush();
  assert.equal(modelCalls('local'),justFetched,'successful discovery is throttled for ten seconds across all open paths');
  assert.equal(document.querySelector('#panel-ai [data-role=refresh]'),null,'AI refresh button is removed');
  assert.ok([...document.querySelectorAll('.ai-provider-card button')].every(b=>!['Refresh','\u66f4\u65b0'].includes(b.textContent)),'cards have no refresh buttons');
  assert.equal(document.querySelectorAll('#panel-ai [role=tab]').length,3);
  assert.equal(document.querySelector('#panel-ai [data-subtab-target=network]'),null);
  assert.equal(document.getElementById('settings-ai-room_id'),null);
  assert.equal(document.querySelectorAll('.ai-provider-card').length,4);
  assert.ok(card('home').querySelector('.ai-room-status').textContent.includes('3'));
  assert.ok(card('team').querySelector('.ai-room-status').textContent.includes('0'));
  // I: status occupies its own header slot, and narrow layouts hide only its text.
  const originalSize={width:window.innerWidth,height:window.innerHeight};
  for(const width of [360,390]) {
    window.happyDOM.setWindowSize({width,height:844});
    for(const id of ['local','old','home','team']) {
      const head=card(id).querySelector('.ai-provider-head'),name=head.querySelector('.ai-provider-name');
      const status=head.querySelector('.ai-provider-status'),address=card(id).querySelector('.ai-provider-address');
      assert.deepEqual([...head.children].map(n=>n.className),['toggle-switch','ai-provider-summary','ai-provider-status']);
      const style=n=>window.getComputedStyle(n);
      assert.equal(style(head).flexWrap,'nowrap');assert.equal(style(name).minWidth,'0');
      assert.equal(style(head.querySelector('.ai-provider-summary')).flexGrow,'1');assert.equal(style(name).textOverflow,'ellipsis');assert.equal(name.title,name.textContent);
      assert.equal(style(status).flexShrink,'0');assert.equal(style(status).width,'28px');
      assert.equal(style(status.querySelector('.ai-provider-status-text')).display,'none');
      assert.equal(style(address).textOverflow,'ellipsis');assert.equal(style(address).whiteSpace,'nowrap');assert.ok(address.title);
      assert.equal(status.querySelector('.ai-provider-status-dot').getAttribute('aria-hidden'),'true');
      assert.ok(status.getAttribute('aria-label').includes(name.textContent),'dot exposes the full name and status to assistive technology');
    }
    assert.equal(card('home').querySelector('.ai-provider-status').dataset.connectionState,'connected');
    assert.equal(card('home').querySelector('.ai-provider-status').dataset.providing,'true');
    assert.equal(card('team').querySelector('.ai-provider-status').dataset.connectionState,'offline');
    const dot=card('home').querySelector('.ai-provider-status'),edit=card('home').querySelector('.ai-provider-summary');
    dot.click();assert.equal(dot.getAttribute('aria-expanded'),'true');
    dot.dispatchEvent(new window.PointerEvent('pointerleave',{pointerType:'touch'}));
    await new Promise(resolve=>setTimeout(resolve,220));
    assert.equal(dot.getAttribute('aria-expanded'),'true','touch pointerleave does not dismiss tapped details');
    const details=document.getElementById(dot.getAttribute('aria-controls'));
    assert.equal(details.getAttribute('role'),'dialog');assert.ok(details.textContent.includes('3'));
    assert.ok(details.textContent.includes('\u63d0\u4f9b\u4e2d'));assert.ok(details.textContent.includes('\u66f4\u65b0\u65e5\u6642'));
    assert.equal(edit.getAttribute('aria-expanded'),'false','dot does not open the editor');
    details.dispatchEvent(new window.PointerEvent('pointerdown',{bubbles:true}));
    document.body.dispatchEvent(new window.MouseEvent('click',{bubbles:true}));
    assert.equal(dot.getAttribute('aria-expanded'),'true','release outside after an inside press keeps details open');
    document.body.dispatchEvent(new window.PointerEvent('pointerdown',{bubbles:true}));
    document.body.dispatchEvent(new window.MouseEvent('click',{bubbles:true}));
    assert.equal(dot.getAttribute('aria-expanded'),'false','an outside-origin press closes details');
    dot.click();dot.click();assert.equal(dot.getAttribute('aria-expanded'),'false','second tap closes details');
    dot.click();document.dispatchEvent(new window.KeyboardEvent('keydown',{key:'Escape',bubbles:true}));
    assert.equal(dot.getAttribute('aria-expanded'),'false');assert.equal(document.activeElement,dot);
    assert.equal(edit.getAttribute('aria-expanded'),'false','all detail close paths leave the editor unchanged');
  }
  window.happyDOM.setWindowSize(originalSize);
  assert.equal(card('local').querySelector('img'),null,'provider label is literal text');
  assert.equal(card('local').querySelector('input[type=checkbox]').checked,true,'absent enabled means true');
  assert.ok(document.querySelector('#settings-ai-default_ref .ai-model-warning').textContent.includes('\u7121\u52b9'));
  assert.ok(document.querySelector('#settings-ai-tts .ai-model-warning').textContent.includes('\u7121\u52b9'));
  assert.equal(aiWrites().length,0,'loading/refreshing models never rewrites references or creates presets');
  assert.equal(document.querySelector('#topo-strip-ai'),null,'redundant single-room status strip is removed');
  assert.ok(!document.getElementById('panel-ai').textContent.includes('unrelated-mailbox'),'legacy topology room is never shown in AI');
  for(const id of ['home','team']) {
    assert.equal(card(id).querySelector('[data-room-provide],.ai-model-option,input[type=search],.ai-shared-results'),null,'room cards have no provide UI');
    assert.equal(card(id).querySelectorAll('input[type=checkbox]').length,1,'only enable remains on room cards');
  }
  assert.equal(document.getElementById('btn-ai-provide-toggle'),null,'room chips are the only provide control');
  assert.ok(!/ai\.provide\.(start|stop)|ai\.(startProviding|stopProviding|alreadyProviding|nowProviding|stoppedProviding|wasNotProviding)/.test(html),'global provide handlers and strings are removed');
  assert.ok(sharing().querySelector('[data-room-provide=home]').textContent.includes('\u63d0\u4f9b\u4e2d'));
  assert.ok(sharing().querySelector('[data-room-provide=home]').textContent.includes('3'));
  assert.ok(sharing().querySelector('[data-room-provide=team]').textContent.includes('\u672a\u63a5\u7d9a'));
  fixtures['ai.status'].rooms[0].providing=false;
  fixtures['ai.status'].providing=false;
  window.__mistlViewSwitchUntil=0;intervals.find(timer=>timer.ms===5000).callback();await flush();
  assert.ok(sharing().querySelector('[data-room-provide=home]').textContent.includes('\u505c\u6b62'),'sharing chip state follows status polls');
  sharing().querySelector('[data-room-provide=home]').click();await flush();
  assert.equal(aiCfg.providers.find(p=>p.id==='home').provide,false);
  assert.equal(fixtures['ai.status'].providing,false);
  sharing().querySelector('[data-room-provide=home]').click();await flush();
  assert.equal(aiCfg.providers.find(p=>p.id==='home').provide,true);
  assert.equal(fixtures['ai.status'].providing,true);
  assert.equal(calls.filter(c=>c.cmd.startsWith('ai.provide.')).length,0);
  assert.ok([...sharing().querySelectorAll('.ai-sharing-row')].every(n=>n.dataset.providerId==='local'),'sharing offers only enabled HTTP models');
  assertUnique(sharing().querySelectorAll('.ai-sharing-row'));
  assert.equal(sharing().querySelector('.ai-sharing-group .ai-sharing-row').dataset.model,'gem-small','shared models are pinned within their group');
  input(sharing().querySelector('input[type=search]'),'ollama model-299');
  let sharedOption=sharingRow('local','model-299').querySelector('[data-room-id=home]');
  assert.ok(sharedOption);sharedOption.click();await flush();
  assert.deepEqual(aiCfg.providers.find(p=>p.id==='home').shared,[{provider_id:'local',model:'gem-small'},{provider_id:'local',model:'model-299'}]);
  assert.deepEqual(aiCfg.providers.find(p=>p.id==='team').shared,[],'editing one room never edits another');
  assert.equal(sharingRow('local','model-299').querySelector('[data-room-id=home]').getAttribute('aria-pressed'),'true');
  sharingRow('local','model-299').querySelector('[data-room-id=team]').click();await flush();
  assert.deepEqual(aiCfg.providers.find(p=>p.id==='team').shared,[{provider_id:'local',model:'model-299'}]);
  assert.equal(sharing().querySelector('input[type=search]').value,'ollama model-299','search survives model refresh/save');
  assertUnique(sharing().querySelectorAll('.ai-sharing-row'));
  input(sharing().querySelector('input[type=search]'),'');
  const orderedShared=[...sharing().querySelectorAll('.ai-sharing-row')];
  assert.deepEqual(orderedShared.slice(0,2).map(n=>n.dataset.model),['gem-small','model-299']);
  assertUnique(orderedShared);
  sharingRow('local','model-299').querySelector('[data-room-id=home]').click();await flush();
  assert.deepEqual(aiCfg.providers.find(p=>p.id==='home').shared,[{provider_id:'local',model:'gem-small'}],'chip removes the exact reference');
  sharingRow('local','model-298').querySelector('[data-room-id=team]').click();
  sharingRow('local','model-297').querySelector('[data-room-id=team]').click();await flush();
  assert.deepEqual(aiCfg.providers.find(p=>p.id==='team').shared.map(r=>r.model),['model-299','model-298','model-297'],'rapid shared edits compose in the write queue');
  const provide=sharing().querySelector('[data-room-provide=team]');provide.click();await flush();
  assert.equal(aiCfg.providers.find(p=>p.id==='team').provide,true);
  assert.equal(sharing().querySelector('[data-room-provide=team]').getAttribute('aria-pressed'),'true');
  sharing().querySelector('[data-room-provide=team]').click();await flush();
  assert.equal(aiCfg.providers.find(p=>p.id==='team').provide,false);
  let dialog=await openPicker('settings-ai-default_ref');
  assert.equal(dialog.querySelector('button[data-provider-id=old]').disabled,true,'disabled assignment is visible but not selectable');
  assert.equal(dialog.querySelector('.ai-model-source[aria-selected=true]').dataset.source,'recent','disabled assignment opens Recent without offering a disabled source');
  assert.equal(dialog.querySelector('[data-source=old]'),null,'disabled providers never appear in sources');
  assert.equal(dialog.querySelector('.ai-model-option').getAttribute('aria-selected'),'true');
  assert.ok(dialog.querySelector('.ai-model-option .ai-model-check'));
  input(dialog.querySelector('input[type=search]'),'ollama gem');
  assert.ok(dialog.querySelector('[data-provider-id=local][data-model=gem-small]'));
  assert.equal(dialog.querySelector('[data-provider-id=home]'),null,'search tokens are ANDed across model and provider');
  input(dialog.querySelector('input[type=search]'),'gem');
  assertUnique(dialog.querySelectorAll('.ai-model-option'));
  assert.ok([...dialog.querySelectorAll('.ai-model-group')].every(n=>!['\u6700\u8fd1','\u5272\u5f53\u4e2d'].includes(n.textContent)),'search has no pinned section');
  assert.equal(dialog.querySelector('[data-provider-id=old]'),null,'search only offers enabled providers');
  assert.ok(dialog.querySelector('[data-provider-id=local][data-model=gem-small]'));
  assert.ok(dialog.querySelector('[data-provider-id=home][data-model=gem-small]'),'same model under different providers remains distinct');
  assert.ok(dialog.querySelector('[data-provider-id=team][data-model=gem-team]').classList.contains('ai-model-cached'),'cached room result is dimmed');
  dialog.querySelector('input').dispatchEvent(new window.KeyboardEvent('keydown',{key:'Escape',bubbles:true}));
  assert.equal(document.querySelector('.ai-model-dialog'),null);
  assert.deepEqual(aiCfg.default_ref,{provider_id:'old',model:'gem-old'},'dismiss does not repoint a disabled reference');
  assert.equal(document.activeElement,document.querySelector('#settings-ai-default_ref .ai-model-trigger'));
  dialog=await openPicker('settings-ai-default_ref');
  const searchModel=dialog.querySelector('input[type=search]');input(searchModel,'ollama gem');
  searchModel.dispatchEvent(new window.KeyboardEvent('keydown',{key:'ArrowDown',bubbles:true}));
  assert.equal(document.activeElement.dataset.model,'gem-small');
  document.activeElement.dispatchEvent(new window.KeyboardEvent('keydown',{key:'ArrowUp',bubbles:true}));
  document.activeElement.dispatchEvent(new window.KeyboardEvent('keydown',{key:'ArrowDown',bubbles:true}));
  document.activeElement.dispatchEvent(new window.KeyboardEvent('keydown',{key:'Enter',bubbles:true}));
  await flush();
  assert.deepEqual(aiCfg.default_ref,{provider_id:'local',model:'gem-small'});
  assert.ok(document.querySelector('#settings-ai-default_ref .ai-model-trigger').textContent.includes('Ollama'));
  window.localStorage.setItem('mistl-recent-models',JSON.stringify([{provider_id:'home',model:'gem-small'},{provider_id:'local',model:'gem-small'},{provider_id:'local',model:'gem-small'},{provider_id:'local',model:'model-299'}]));
  dialog=await openPicker('settings-ai-default_ref');
  assert.equal(dialog.querySelector('.ai-model-source[aria-selected=true]').dataset.source,'local','assigned provider is the initial source');
  assert.ok([...dialog.querySelectorAll('.ai-model-option')].every(n=>n.dataset.providerId==='local'),'initial right pane only contains assigned provider');
  assert.equal(dialog.querySelector('.ai-model-provider'),null,'single-source rows omit provider subtitles');
  assert.equal(dialog.querySelector('.ai-model-group'),null,'single source omits the redundant provider header');
  const pickerDimensions=[window.getComputedStyle(dialog).width,window.getComputedStyle(dialog).height];
  assert.match(html,/\.ai-model-dialog \{[^}]*height: min\(420px, calc\(100dvh - 32px\)\)/,'picker has fixed bounded height (browser harness verifies geometry)');
  dialog.querySelector('[data-source=recent]').click();
  assert.deepEqual([window.getComputedStyle(dialog).width,window.getComputedStyle(dialog).height],pickerDimensions,'source switches preserve fixed picker size');
  let options=[...dialog.querySelectorAll('.ai-model-option')];
  assert.deepEqual(options.slice(0,3).map(n=>[n.dataset.providerId,n.dataset.model]),[['local','gem-small'],['home','gem-small'],['local','model-299']],'assigned first, deduplicated recents follow');
  assert.equal(dialog.querySelector('.ai-model-source[aria-selected=true]').dataset.source,'recent','Recent is a source');
  assert.ok(options.every(row=>row.querySelector('.ai-model-provider')),'Recent retains each provider subtitle');
  assert.ok(options[0].querySelector('.ai-model-check'));assert.equal(options[0].getAttribute('aria-selected'),'true');
  const enabledOptions=options.filter(n=>!n.disabled);
  const keyboardSearch=dialog.querySelector('input[type=search]');keyboardSearch.focus();
  for(const option of enabledOptions) {
    document.activeElement.dispatchEvent(new window.KeyboardEvent('keydown',{key:'ArrowDown',bubbles:true}));
    assert.equal(document.activeElement,option,'keyboard follows the rendered list');
  }
  enabledOptions[1].focus();
  document.activeElement.dispatchEvent(new window.KeyboardEvent('keydown',{key:'ArrowDown',bubbles:true}));
  assert.equal(document.activeElement,enabledOptions[2],'arrow order follows focus after tab or pointer navigation');
  input(keyboardSearch,'gem');
  assertUnique(dialog.querySelectorAll('.ai-model-option'));
  assert.equal(dialog.querySelector('[data-provider-id=local][data-model=gem-small]').getAttribute('aria-selected'),'true');
  assert.ok(dialog.querySelector('[data-provider-id=home][data-model=gem-small] .ai-model-mark'),'searched recents have inline marks');
  assert.equal(dialog.querySelector('[data-source=all]').getAttribute('aria-selected'),'true','typing searches across all sources');
  assert.equal(dialog.querySelector('[data-source=local]').dataset.count,'1');
  assert.equal(dialog.querySelector('[data-source=home]').dataset.count,'1');
  assert.equal(dialog.querySelector('[data-source=team]').dataset.count,'1');
  assert.equal(dialog.querySelector('.ai-model-provider'),null,'grouped search rows omit redundant provider subtitles');
  assert.ok(dialog.querySelector('.ai-model-group'),'search still labels provider groups');
  dialog.querySelector('[data-source=home]').click();
  assert.ok([...dialog.querySelectorAll('.ai-model-option')].every(row=>row.dataset.providerId==='home'),'clicking a source narrows search');
  assert.equal(dialog.querySelector('[data-source=home]').getAttribute('aria-selected'),'true');
  dialog.querySelector('[data-source=all]').click();
  assert.ok(dialog.querySelector('[data-provider-id=local][data-model=gem-small]'),'All restores global search');
  input(keyboardSearch,'ollama gem');
  assert.equal(dialog.querySelector('[data-source=home]').dataset.count,'0');
  assert.equal(dialog.querySelector('[data-source=home]').dataset.empty,'true','zero-match sources are dimmed');
  dialog.querySelector('[data-source=home]').click();
  assert.equal(dialog.querySelectorAll('.ai-model-option').length,0,'zero-match Room source has an empty right pane');
  input(keyboardSearch,'');
  assert.equal(dialog.querySelector('.ai-model-source[aria-selected=true]').dataset.source,'recent','clearing query restores previous source');
  assert.equal(dialog.querySelector('[data-source=all]'),null,'All exists only during search');
  dialog.querySelector('[data-source=team]').click();
  assert.ok([...dialog.querySelectorAll('.ai-model-option')].every(row=>row.dataset.providerId==='team'),'source switch replaces right pane');
  dialog.querySelector('[data-source=local]').click();
  input(keyboardSearch,'gem');dialog.querySelector('[data-source=home]').click();input(keyboardSearch,'');
  assert.equal(dialog.querySelector('.ai-model-source[aria-selected=true]').dataset.source,'local','search narrowing does not overwrite saved source');
  const key=(node,value)=>node.dispatchEvent(new window.KeyboardEvent('keydown',{key:value,bubbles:true}));
  keyboardSearch.focus();key(keyboardSearch,'ArrowDown');
  assert.equal(document.activeElement.dataset.model,'gem-small','Down leaves search for models');
  key(document.activeElement,'ArrowLeft');
  assert.equal(document.activeElement.dataset.source,'local','Left enters selected source');
  key(document.activeElement,'ArrowDown');assert.equal(document.activeElement.dataset.source,'home');
  key(document.activeElement,'ArrowUp');assert.equal(document.activeElement.dataset.source,'local');
  key(document.activeElement,'ArrowDown');key(document.activeElement,'Enter');
  assert.equal(dialog.querySelector('.ai-model-source[aria-selected=true]').dataset.source,'home','Enter switches source');
  key(document.activeElement,'ArrowRight');assert.equal(document.activeElement.dataset.providerId,'home','Right enters model pane');
  key(document.activeElement,'ArrowLeft');key(document.activeElement,'Escape');
  assert.equal(document.querySelector('.ai-model-dialog'),null,'Esc closes from source pane');
  assert.equal(document.activeElement,document.querySelector('#settings-ai-default_ref .ai-model-trigger'));
  dialog=await openPicker('settings-ai-default_ref');
  assert.equal(document.activeElement,dialog.querySelector('input[type=search]'),'focus starts in search');
  window.happyDOM.setWindowSize({width:390,height:844});
  assert.equal(window.getComputedStyle(dialog.querySelector('.ai-model-sources')).display,'flex','narrow sources become a chip row');
  assert.equal(window.getComputedStyle(dialog.querySelector('.ai-model-sources')).overflowX,'auto','narrow chips scroll horizontally');
  assert.equal(window.getComputedStyle(dialog.querySelector('.ai-model-panes')).gridTemplateColumns,'minmax(0, 1fr)','narrow picker is a single column');
  window.happyDOM.setWindowSize({width:1024,height:768});
  dialog.querySelector('.modal-close').click();
  await pick('settings-ai-tts','home','network-auto');
  assert.equal(aiCfg.tts.provider_id,'home');assert.equal(aiCfg.tts.model,'network-auto');
  assert.deepEqual(aiCfg.tts.lang_voices,{en:'voice-en'},'voice configuration survives a model change');
  dialog=await openPicker('settings-ai-tts');
  assert.equal(dialog.querySelector('.ai-model-source[aria-selected=true]').dataset.source,'home','voice starts with assigned source');
  assert.equal(dialog.querySelector('.ai-model-option').dataset.model,'network-auto','room auto is the first voice row');
  assert.ok(dialog.querySelector('[data-source=browser]'),'voice has Browser default source');
  dialog.querySelector('[data-source=local]').click();
  assert.equal(dialog.querySelector('[data-model=network-auto]'),null,'HTTP voice sources have no Room sentinel');
  dialog.querySelector('.modal-close').click();
  const voice=document.getElementById('settings-ai-tts-voice');voice.value='custom-voice';change(voice);await flush();
  assert.equal(aiCfg.tts.voice,'custom-voice');
  dialog=await openPicker('settings-ai-tts');dialog.querySelector('[data-source=browser]').click();await flush();
  assert.equal(aiCfg.tts,null,'browser default sends null');
  await pick('settings-ai-stt','team','network-auto');
  assert.deepEqual(aiCfg.stt,{provider_id:'team',model:'network-auto'});
  // Failed /models still offers the cache; search never offers a manual-entry row.
  modelNow+=10001;
  modelFailures.add('local');
  dialog=await openPicker('settings-ai-default_ref');input(dialog.querySelector('input[type=search]'),'unlisted-model');
  assert.equal(dialog.querySelectorAll('[data-model=unlisted-model]').length,0,'no manual-entry row for an unlisted model');
  assert.ok(![...dialog.querySelectorAll('.ai-model-option')].some(o=>o.textContent.includes('\u624b\u5165\u529b')),'no manual-entry text anywhere');
  assert.equal(card('local').querySelector('[role=status]').dataset.state,'error');
  assert.equal(card('local').querySelector('[role=status]').title,'models endpoint unavailable','full error is available as a tooltip');
  assert.ok(aiCfg.providers[0].models.includes('model-299'),'failure preserves the persisted cache');
  dialog.querySelector('.modal-close').click();
  dialog=await openPicker('settings-ai-default_ref');input(dialog.querySelector('input[type=search]'),'model-299');
  assert.ok(dialog.querySelector('[data-provider-id=local][data-model=model-299]'),'HTTP cache remains usable when fetching fails');dialog.querySelector('.modal-close').click();
  modelFailures.clear();
  modelResults.delete('local');
  for(let i=0;i<10;i++) await pick('settings-ai-default_ref','local','model-'+i);
  const recent=JSON.parse(window.localStorage.getItem('mistl-recent-models'));
  assert.equal(recent.length,8);assert.equal(recent[0].model,'model-9');
  await pick('settings-ai-default_ref','local','model-9');
  assert.equal(JSON.parse(window.localStorage.getItem('mistl-recent-models')).length,8,'recents are deduplicated');
  dialog=await openPicker('settings-ai-default_ref');
  dialog.querySelector('[data-source=recent]').click();
  const recentCount=dialog.querySelectorAll('.ai-model-option').length;
  assert.equal(recentCount,8,'Recent has at most eight entries including assignment');
  dialog.querySelector('.modal-close').click();
  const savedRef=structuredClone(aiCfg.default_ref);
  const enabledLocal=card('local').querySelector('input[type=checkbox]');enabledLocal.checked=false;change(enabledLocal);await flush();
  assert.deepEqual(aiCfg.default_ref,savedRef,'disable preserves default reference');
  assert.ok(document.querySelector('#settings-ai-default_ref .ai-model-warning').textContent.includes('\u7121\u52b9'));
  dialog=await openPicker('settings-ai-default_ref');
  assert.ok([...dialog.querySelectorAll('button[data-provider-id=local]')].every(b=>b.disabled),'disabled provider is excluded from offered results and recents');
  dialog.querySelector('.modal-close').click();
  const beforeReenable=modelCalls('local');
  const enabledAgain=card('local').querySelector('input[type=checkbox]');enabledAgain.checked=true;change(enabledAgain);await flush();
  assert.equal(modelCalls('local'),beforeReenable+1,'re-enable fetches immediately despite the success throttle');
  assert.deepEqual(aiCfg.default_ref,savedRef,'reenable preserves the same reference');
  // Addendum H: one header action and a shared, press-origin guarded popup.
  assert.equal(document.querySelectorAll('.ai-provider-add').length,0,'bottom tiles are gone');
  const connectionForm=()=>document.querySelector('.ai-connection-popup:not(.ai-add-room)');
  const openConnection=()=>{document.querySelector('[data-add-connection]').click();return connectionForm();};
  assert.ok(document.querySelector('.ai-connection-header [data-add-connection]'),'header action is above the cards');
  let connection=openConnection();
  assert.equal(connection.querySelector('[data-connection-kind=http]').getAttribute('aria-pressed'),'true','HTTP is initially selected');
  input(connection.querySelector('[data-connection-name]'),'Kept name');
  connection.querySelector('[data-connection-kind=room]').click();
  assert.equal(connection.querySelector('[data-connection-name]').value,'Kept name','kind toggle preserves Name');
  connection.requestSubmit();await flush();
  assert.equal(connection.querySelector('[data-add-room-id]').getAttribute('aria-invalid'),'true','empty room ID gets inline validation');
  connection.querySelector('[data-connection-kind=http]').click();
  assert.equal(connection.querySelector('[data-connection-name]').value,'Kept name');
  connection.dispatchEvent(new window.Event('submit',{bubbles:true,cancelable:true}));await flush();
  assert.equal(connection.querySelector('[data-connection-url]').getAttribute('aria-invalid'),'true','empty URL gets inline validation');
  input(connection.querySelector('[data-connection-url]'),'ftp://invalid.example');
  connection.dispatchEvent(new window.Event('submit',{bubbles:true,cancelable:true}));await flush();
  assert.equal(connection.querySelector('[data-connection-url]').getAttribute('aria-invalid'),'true','non HTTP URL is rejected');
  const connectionOverlay=connection.parentElement;
  connection.querySelector('input').dispatchEvent(new window.MouseEvent('mousedown',{bubbles:true}));
  connectionOverlay.dispatchEvent(new window.MouseEvent('click',{bubbles:true}));
  assert.equal(connection.hidden,false,'inside press / outside release keeps the popup open');
  connectionOverlay.dispatchEvent(new window.MouseEvent('mousedown',{bubbles:true}));
  connectionOverlay.dispatchEvent(new window.MouseEvent('click',{bubbles:true}));
  assert.equal(connection.hidden,true,'outside-origin click closes the popup');
  for(const [roomKind,address] of [[false,'https://new.example/v1'],[true,'new-room']]) {
    connection=openConnection();connection.querySelector('[data-connection-kind='+ (roomKind?'room':'http') +']').click();
    input(connection.querySelector('[data-connection-name]'),roomKind?'New room':'New HTTP');
    input(connection.querySelector(roomKind?'[data-add-room-id]':'[data-connection-url]'),address);
    if(!roomKind)input(connection.querySelector('[data-connection-key]'),'new-key');
    connection.querySelector(roomKind?'[data-add-room-id]':'[data-connection-url]').dispatchEvent(new window.KeyboardEvent('keydown',{key:'Enter',bubbles:true}));await flush();
    const newP=aiCfg.providers.at(-1);assert.equal(newP.base_url,roomKind?'mist-network://'+address:address);
    assert.equal(newP.api_key,roomKind?'':'new-key');assert.equal(newP.enabled,true);
    assert.equal(modelCalls(newP.id),1,'creation fetches the new provider immediately');
    assert.equal(aiWrites().at(-1).args.path,'ai.providers','creation persists the full provider array');
    assert.equal(connection.hidden,true);assert.equal(document.activeElement.closest('.ai-provider-card'),card(newP.id));
  }
  connection=openConnection();
  assert.equal(connection.querySelector('[data-connection-kind=room]').getAttribute('aria-pressed'),'true','the last kind is remembered');
  input(connection.querySelector('[data-add-room-id]'),'new-room');
  const duplicateCount=aiCfg.providers.length,duplicateWrites=aiWrites().length;
  connection.requestSubmit();await flush();
  assert.equal(connection.hidden,false);assert.equal(connection.querySelector('[data-connection-reuse]').hidden,false);
  assert.equal(aiCfg.providers.length,duplicateCount);assert.equal(aiWrites().length,duplicateWrites,'duplicate validation does not write');
  connection.querySelector('[data-connection-reuse]').click();await flush();
  assert.equal(connection.hidden,true);assert.equal(aiCfg.providers.length,duplicateCount);
  // J: single-column disclosure cards, independent controls, per-field commits.
  assert.equal(window.getComputedStyle(document.querySelector('.ai-provider-grid')).gridTemplateColumns,'minmax(0, 1fr)');
  assert.equal(document.querySelector('[data-provider-edit]'),null);
  const header=card('local').querySelector('.ai-provider-summary');
  for (const activate of [()=>header.click(),()=>header.dispatchEvent(new window.KeyboardEvent('keydown',{key:'Enter',bubbles:true})),()=>header.dispatchEvent(new window.KeyboardEvent('keydown',{key:' ',bubbles:true}))]) {
    activate();assert.equal(header.getAttribute('aria-expanded'),'true');
    assert.ok(card('local').querySelector('.ai-provider-editor'));
    assert.deepEqual([...card('local').querySelectorAll('.ai-provider-editor button:not([hidden])')].map(n=>n.className),['danger'],'no edit, save or cancel controls');
    header.click();assert.equal(header.getAttribute('aria-expanded'),'false');
  }
  const enabledControl=card('local').querySelector('input[type=checkbox]');
  enabledControl.click();await flush();assert.equal(header.getAttribute('aria-expanded'),'false','switch does not expand');
  enabledControl.click();await flush();
  header.click();
  let providerEditor=card('local').querySelector('.ai-provider-editor');
  const field=key=>providerEditor.querySelector('[data-provider-field='+key+']');
  const enter=node=>node.dispatchEvent(new window.KeyboardEvent('keydown',{key:'Enter',bubbles:true}));
  const blur=node=>node.dispatchEvent(new window.FocusEvent('blur'));
  const providerWrites=()=>aiWrites().filter(c=>c.args.path==='ai.providers');
  let writes=providerWrites().length;
  const toastCount=document.querySelectorAll('.toast').length;
  input(field('label'),'Renamed endpoint');blur(field('label'));await flush();
  assert.equal(providerWrites().length,writes+1);assert.equal(aiCfg.providers[0].label,'Renamed endpoint');
  assert.equal(document.querySelectorAll('.toast').length,toastCount,'field saves use inline feedback without a toast');
  assert.ok(field('label').parentNode.querySelector('.ai-provider-field-feedback').textContent);
  input(field('label'),'Rename with Enter');enter(field('label'));blur(field('label'));await flush();
  assert.equal(providerWrites().length,writes+2,'Enter plus blur dedupes');
  input(field('label'),'Discard');field('label').dispatchEvent(new window.KeyboardEvent('keydown',{key:'Escape',bubbles:true}));blur(field('label'));await flush();
  assert.equal(field('label').value,'Rename with Enter');assert.equal(providerWrites().length,writes+2);assert.equal(header.getAttribute('aria-expanded'),'true','Esc reverts the field without collapsing');
  rejectSetting='ai.providers';input(field('label'),'Rejected');blur(field('label'));await flush();
  assert.equal(aiCfg.providers[0].label,'Rename with Enter');assert.equal(field('label').getAttribute('aria-invalid'),'true');
  assert.ok(field('label').parentNode.textContent.includes('setting rejected'));
  rejectSetting=null;field('label').dispatchEvent(new window.KeyboardEvent('keydown',{key:'Escape',bubbles:true}));
  writes=providerWrites().length;
  const beforeSameHost=modelCalls('local');input(field('base_url'),'http://localhost:11434/alternate/v1');blur(field('base_url'));await flush();
  assert.equal(providerWrites().length,writes+1);assert.equal(aiCfg.providers[0].api_key,'***');assert.equal(modelCalls('local'),beforeSameHost+1,'same-host commit fetches immediately');
  input(field('base_url'),'http://localhost:11434/enter/v1');enter(field('base_url'));await flush();assert.equal(providerWrites().length,writes+2,'URL Enter commits');
  input(field('base_url'),'http://localhost:11434/discard');field('base_url').dispatchEvent(new window.KeyboardEvent('keydown',{key:'Escape',bubbles:true}));blur(field('base_url'));await flush();
  assert.equal(field('base_url').value,'http://localhost:11434/enter/v1');assert.equal(providerWrites().length,writes+2);
  // Scheme and port changes are new origins too, matching the daemon guard.
  for (const url of ['https://localhost:11434/v1','http://localhost:11435/v1','https://changed.example/v1']) {
    field('base_url').focus();input(field('base_url'),url);blur(field('base_url'));await flush();
    assert.equal(providerWrites().length,writes+2,'masked key never follows a changed origin');
    assert.equal(document.activeElement,field('api_key'));assert.equal(providerEditor.querySelector('[data-provider-key-note]').hidden,false);
    enter(field('api_key'));await flush();assert.equal(providerWrites().length,writes+2,'mask is not a replacement key');
  }
  input(field('base_url'),'http://localhost:11434/enter/v1');enter(field('base_url'));await flush();
  assert.equal(providerEditor.querySelector('[data-provider-key-note]').hidden,true,'restoring the original URL clears the pending host');
  enter(field('api_key'));await flush();assert.equal(providerWrites().length,writes+2,'reverted host is not sent with the mask');
  input(field('base_url'),'https://changed.example/v1');blur(field('base_url'));await flush();
  input(field('api_key'),'');blur(field('api_key'));await flush();assert.equal(providerWrites().length,writes+2,'pending host requires an entered key');
  const beforeEdit=modelCalls('local');
  modelResults.set('local',{models:['ignored-error-model'],live:false,error:'HTTP 503: '+ 'details '.repeat(30)});
  input(field('api_key'),'new-host-key');enter(field('api_key'));blur(field('api_key'));await flush();
  assert.equal(providerWrites().length,writes+3,'URL and replacement key sent together exactly once');
  const sent=providerWrites().at(-1).args.value.find(p=>p.id==='local');assert.equal(sent.base_url,'https://changed.example/v1');assert.equal(sent.api_key,'new-host-key');
  assert.equal(providerEditor.querySelector('[data-provider-key-note]').hidden,true);
  assert.equal(modelCalls('local'),beforeEdit+1,'URL/key commit fetches immediately');
  assert.equal(card('local').querySelector('[role=status]').dataset.state,'error','IPC error metadata is displayed');
  assert.ok(card('local').querySelector('[role=status]').title.length>80);
  const errorDot=card('local').querySelector('.ai-provider-status');errorDot.click();
  assert.equal(header.getAttribute('aria-expanded'),'true','status dot does not collapse');assert.equal(errorDot.dataset.connectionState,'error');
  assert.ok(document.getElementById(errorDot.getAttribute('aria-controls')).textContent.includes('details '.repeat(30)),'popover contains the complete error');errorDot.click();
  assert.ok(aiCfg.providers[0].models.includes('model-299'),'editing a URL and failing leaves the old cache intact');
  input(field('api_key'),'discard-key');field('api_key').dispatchEvent(new window.KeyboardEvent('keydown',{key:'Escape',bubbles:true}));blur(field('api_key'));await flush();assert.equal(field('api_key').value,'new-host-key');
  input(field('api_key'),'changed-key');modelResults.delete('local');blur(field('api_key'));await flush();
  assert.equal(modelCalls('local'),beforeEdit+2,'API key blur commit fetches immediately');header.click();
  card('home').querySelector('.ai-provider-summary').click();providerEditor=card('home').querySelector('.ai-provider-editor');
  assert.equal(card('home').querySelectorAll('.ai-room-status').length,1,'Room status appears once while expanded');
  assert.ok(card('home').querySelector('.ai-provider-head .ai-room-status'),'Room status lives in header');
  assert.equal(providerEditor.querySelector('[role=status],.ai-room-status'),null,'editor does not repeat Room status');
  const roomDot=card('home').querySelector('.ai-provider-status');roomDot.click();roomDot.click();
  assert.equal(card('home').querySelector('.ai-provider-editor'),providerEditor,'detail toggling preserves an expanded editor');
  assert.equal(card('home').querySelector('.ai-provider-summary').getAttribute('aria-expanded'),'true');
  writes=providerWrites().length;input(field('base_url'),'team-room');enter(field('base_url'));await flush();
  assert.equal(providerWrites().length,writes,'duplicate room is not sent');assert.equal(providerEditor.querySelector('[data-provider-reuse]').hidden,false);
  providerEditor.querySelector('[data-provider-reuse]').click();await flush();assert.equal(card('team').querySelector('.ai-provider-summary').getAttribute('aria-expanded'),'true','duplicate points to existing card');
  card('team').querySelector('.ai-provider-summary').click();
  input(field('base_url'),'discard-room');field('base_url').dispatchEvent(new window.KeyboardEvent('keydown',{key:'Escape',bubbles:true}));assert.equal(field('base_url').value,'home-room');
  const beforeRoomEdit=modelCalls('home');input(field('base_url'),'edited-room');blur(field('base_url'));await flush();
  assert.equal(modelCalls('home'),beforeRoomEdit+1,'Room ID blur commit fetches immediately');
  input(field('base_url'),'entered-room');enter(field('base_url'));await flush();assert.equal(aiCfg.providers.find(p=>p.id==='home').base_url,'mist-network://entered-room');
  input(field('base_url'),'edited-room');blur(field('base_url'));await flush();
  window.confirm=()=>false;writes=providerWrites().length;providerEditor.querySelector('button.danger').click();await flush();assert.equal(providerWrites().length,writes,'delete requires confirmation');
  card('home').querySelector('.ai-provider-summary').click();
  // Incoming room advertisements arrive via the existing status poll.
  fixtures['ai.status'].rooms[0].models=['new-room-model'];
  fixtures['ai.status'].rooms[0].room='edited-room';
  dialog=await openPicker('settings-ai-default_ref');input(dialog.querySelector('input[type=search]'),'new-room');
  window.__mistlViewSwitchUntil=0;intervals.find(timer=>timer.ms===5000).callback();await flush();
  assert.ok(dialog.querySelector('[data-provider-id=home][data-model=new-room-model]'),'room hello updates an open picker through status');
  fixtures['ai.status'].rooms[0].joined=false;window.__mistlViewSwitchUntil=0;
  intervals.find(timer=>timer.ms===5000).callback();await flush();
  assert.ok(dialog.querySelector('[data-source=home]').textContent.includes('\u672a\u63a5\u7d9a'),'source connection state updates even when models do not change');
  assert.equal(dialog.querySelector('input[type=search]').value,'new-room','status updates preserve search');
  fixtures['ai.status'].rooms[0].joined=true;
  dialog.querySelector('.modal-close').click();
  document.querySelector('a[href="#panel-store"]').click();await flush();
  modelNow+=10001;const closedCalls=modelCalls();window.__mistlViewSwitchUntil=0;
  intervals.find(timer=>timer.ms===5000).callback();await flush();
  assert.equal(modelCalls(),closedCalls,'closed model views do not periodically fetch');
  dialog=await openPicker('settings-ai-default_ref');
  const modelOverlay=dialog.parentElement;
  dialog.querySelector('input').dispatchEvent(new window.MouseEvent('mousedown',{bubbles:true}));
  modelOverlay.dispatchEvent(new window.MouseEvent('click',{bubbles:true}));
  assert.ok(document.querySelector('.ai-model-dialog'),'a press starting inside the picker cannot close it on overlay release');
  modelOverlay.dispatchEvent(new window.MouseEvent('mousedown',{bubbles:true}));
  modelOverlay.dispatchEvent(new window.MouseEvent('click',{bubbles:true}));
  assert.equal(document.querySelector('.ai-model-dialog'),null,'a press starting on the overlay can close the picker');
  // Bot forms preserve disabled assignments during editing and serialize refs + voice.
  const pipeline={id:'model-bot',enabled:true,schedule:'@hourly',source:{kind:'global-articles',rooms:[room],langs:[]},transforms:[
    {kind:'summarize',model:{provider_id:'old',model:'gem-old'},reasoning_effort:'high'},
    {kind:'translate',model:{provider_id:'local',model:'gem-small'},reasoning_effort:'low',target_lang:'ja'},
    {kind:'tts',model:{provider_id:'local',model:'speech-model'},voice:'pipeline-voice'}
  ],sinks:[{kind:'chat-post',room}]};
  fixtures['config.show'].bot.pipelines=[pipeline];fixtures['bot.list']={pipelines:[pipeline]};
  document.querySelector('a[href="#panel-bot"]').click();document.querySelector('[data-target=bot]').click();await flush();
  [...document.querySelectorAll('#bot-pipelines-table button')].find(b=>b.textContent==='\u7de8\u96c6').click();await flush();
  assert.ok(document.querySelector('#bot-summarize-model .ai-model-warning').textContent.includes('\u7121\u52b9'));
  assert.equal(document.getElementById('bot-tts-voice').value,'pipeline-voice');
  for(const [id,source] of [['bot-summarize-model','recent'],['bot-translate-model','local'],['bot-tts-model','local']]) {
    dialog=await openPicker(id);
    assert.equal(dialog.querySelector('.ai-model-source[aria-selected=true]').dataset.source,source,'every bot transform uses source panes');
    dialog.querySelector('.modal-close').click();
  }
  const botTtsHost=document.getElementById('bot-tts-model');
  const explicitBotTts=structuredClone(botTtsHost._ref);botTtsHost._setRef(null);
  dialog=await openPicker('bot-tts-model');
  assert.equal(dialog.querySelector('.ai-model-source[aria-selected=true]').dataset.source,aiCfg.default_ref.provider_id,'inherited assignment determines initial source');
  assert.ok(dialog.querySelector('[data-source=default]'),'bots retain default-model action');
  dialog.querySelector('[data-source=home]').click();assert.equal(dialog.querySelector('.ai-model-option').dataset.model,'network-auto','bot voice Room sentinel is first');
  dialog.querySelector('.modal-close').click();botTtsHost._setRef(explicitBotTts);
  await pick('bot-translate-model','home','gem-small');
  document.getElementById('btn-bot-save').click();await flush();
  const savedPipeline=fixtures['config.show'].bot.pipelines[0];
  assert.deepEqual(savedPipeline.transforms[0].model,{provider_id:'old',model:'gem-old'},'bot editing preserves a disabled assignment');
  assert.deepEqual(savedPipeline.transforms[1].model,{provider_id:'home',model:'gem-small'});
  assert.equal(savedPipeline.transforms[2].voice,'pipeline-voice');
  assert.equal(savedPipeline.transforms[0].reasoning_effort,'high');
  assert.equal(savedPipeline.transforms[1].reasoning_effort,'low','task effort survives changing the model');
  assert.ok(!JSON.stringify(savedPipeline).includes('preset_id'));
  assert.ok(aiWrites().every(c=>['ai.providers','ai.default_ref','ai.tts','ai.stt'].includes(c.args.path)));
  assert.ok(!JSON.stringify(aiWrites()).includes('temperature'));
  // Opening Sharing revalidates all enabled providers after the throttle.
  document.querySelector('a[href="#panel-ai"]').click();await flush();
  document.querySelector('#panel-ai [data-subtab-target=tasks]').click();
  modelNow+=10001;
  const beforeSharing=modelCalls();
  document.querySelector('#panel-ai [data-subtab-target=sharing]').click();await flush();
  assert.equal(modelCalls()-beforeSharing,aiCfg.providers.filter(p=>p.enabled!==false).length,'Sharing uses Addendum A revalidation');
  const beforeSharedRejected=structuredClone(aiCfg.providers.find(p=>p.id==='home').shared);
  rejectSetting='ai.providers';sharingRow('local','gem-small').querySelector('[data-room-id=home]').click();await flush();rejectSetting=null;
  assert.deepEqual(aiCfg.providers.find(p=>p.id==='home').shared,beforeSharedRejected);
  assert.equal(sharingRow('local','gem-small').querySelector('[data-room-id=home]').getAttribute('aria-pressed'),'true','a failed save restores the sharing chip');
  document.querySelectorAll('.toast.error button').forEach(b=>b.click());
  // Empty states and disabled exclusions use the same live provider configuration.
  const setEnabled=async (id,value)=>{const cb=card(id).querySelector('input[type=checkbox]');cb.checked=value;change(cb);await flush();};
  const enabledRooms=aiCfg.providers.filter(p=>p.enabled!==false && p.base_url.startsWith('mist-network://')).map(p=>p.id);
  for(const id of enabledRooms)await setEnabled(id,false);
  assert.equal(sharing().querySelectorAll('[data-room-id]').length,0);
  assert.ok(sharing().textContent.includes('\u30eb\u30fc\u30e0\u3092\u8ffd\u52a0'));
  for(const id of enabledRooms)await setEnabled(id,true);
  const enabledHttp=aiCfg.providers.filter(p=>p.enabled!==false && !p.base_url.startsWith('mist-network://')).map(p=>p.id);
  for(const id of enabledHttp)await setEnabled(id,false);
  assert.equal(sharing().querySelectorAll('.ai-sharing-row').length,0);
  assert.ok(sharing().textContent.includes('\u6709\u52b9\u306aHTTP'));
  for(const id of enabledHttp)await setEnabled(id,true);
  // Sharing adds existing connections or new rooms through the same provider array.
  const addForm=()=>document.querySelector('.ai-add-room');
  const openAdd=()=>{sharing().querySelector('[data-add-room]').click();assert.equal(addForm().hidden,false);return addForm();};
  const submitRoom=async (id,label='')=>{
    const form=openAdd();input(form.querySelector('[data-add-room-id]'),id);input(form.querySelector('[data-add-room-label]'),label);
    form.dispatchEvent(new window.Event('submit',{bubbles:true,cancelable:true}));await flush();
    if(!form.querySelector('[data-connection-reuse]').hidden){form.querySelector('[data-connection-reuse]').click();await flush();}
  };
  let addRoomForm=openAdd();
  assert.ok([...addRoomForm.querySelector('select').options].some(o=>o.value==='team'));
  assert.ok(![...addRoomForm.querySelector('select').options].some(o=>o.value==='home'),'already providing rooms are excluded');
  addRoomForm.querySelector('select').value='team';
  const existingShared=structuredClone(aiCfg.providers.find(p=>p.id==='team').shared),existingCount=aiCfg.providers.length;
  addRoomForm.querySelector('[data-add-room-use]').click();await flush();
  assert.equal(aiCfg.providers.length,existingCount);
  assert.equal(aiCfg.providers.find(p=>p.id==='team').provide,true);
  assert.deepEqual(aiCfg.providers.find(p=>p.id==='team').shared,existingShared);
  assert.equal(addForm().hidden,true);
  addRoomForm=openAdd();addRoomForm.querySelector('[data-add-room-random]').click();
  const randomId=addRoomForm.querySelector('[data-add-room-id]').value;
  assert.match(randomId,/^[A-Za-z0-9_-]{16,22}$/);
  addRoomForm.querySelector('[data-add-room-random]').click();
  assert.notEqual(addRoomForm.querySelector('[data-add-room-id]').value,randomId);
  const draftId=addRoomForm.querySelector('[data-add-room-id]').value;
  input(addRoomForm.querySelector('[data-add-room-label]'),'New sharing room <img src=x>');
  // Automatic model updates preserve the open form and its draft.
  sharingRow('local','model-299').querySelector('[data-room-id=home]').click();await flush();
  assert.equal(addForm().hidden,false);assert.equal(addForm().querySelector('[data-add-room-id]').value,draftId);
  addForm().dispatchEvent(new window.Event('submit',{bubbles:true,cancelable:true}));await flush();
  const addedRoom=aiCfg.providers.find(p=>p.base_url==='mist-network://'+draftId);
  assert.ok(addedRoom);assert.equal(aiCfg.providers.length,existingCount+1);
  assert.equal(addedRoom.enabled,true);assert.equal(addedRoom.provide,true);assert.equal(addedRoom.api_key,'');assert.deepEqual(addedRoom.shared,[]);
  assert.equal(addedRoom.label,'New sharing room <img src=x>');
  assert.equal(sharing().querySelector('img'),null,'room label is literal text');
  assert.ok(card(addedRoom.id),'new room immediately appears in Connections');
  assert.ok([...sharing().querySelectorAll('.ai-sharing-row')].every(row=>[...row.querySelectorAll('[data-room-id]')].some(chip=>chip.dataset.roomId===addedRoom.id)),'new room appears on every model row');
  assert.equal(modelCalls(addedRoom.id),1,'new room refreshes automatically');
  assert.equal(aiWrites().at(-1).args.path,'ai.providers');
  await submitRoom(draftId,'Ignored duplicate label');
  assert.equal(aiCfg.providers.length,existingCount+1,'duplicate room id reuses the existing provider');
  assert.equal(aiCfg.providers.find(p=>p.id===addedRoom.id).label,addedRoom.label);
  assert.equal(document.activeElement.dataset.roomProvide,addedRoom.id,'duplicate selects the existing chip');
  await submitRoom('unlabelled-room');
  assert.equal(aiCfg.providers.at(-1).label,'unlabelled-room','optional label defaults to the room id');
  await setEnabled(addedRoom.id,false);
  await submitRoom(draftId);
  assert.equal(aiCfg.providers.find(p=>p.id===addedRoom.id).enabled,true,'duplicate disabled room is enabled and selected');
  assert.equal(aiCfg.providers.filter(p=>p.base_url==='mist-network://'+draftId).length,1);
  const copied=[];
  window.navigator.clipboard.writeText=async value=>{copied.push(value);};
  const providersBeforeCopy=structuredClone(aiCfg.providers),writesBeforeCopy=aiWrites().length;
  const copyNode=sharing().querySelector('[data-copy-room-id="'+addedRoom.id+'"]'),copyText=copyNode.textContent;
  assert.equal(copyText,'','copy is icon-only');
  assert.equal(copyNode.parentElement.dataset.roomGroup,addedRoom.id,'copy lives inside the room chip container');
  assert.equal(copyNode.parentElement.querySelector('button button'),null,'copy and provide have independent accessible buttons');
  assert.ok(copyNode.title.includes(draftId));assert.ok(copyNode.getAttribute('aria-label').includes(draftId));
  const originalTimeout=window.setTimeout.bind(window);let resetCopy;
  window.setTimeout=(callback,ms,...args)=>{if(ms===1200){resetCopy=callback;return 0;}return originalTimeout(callback,ms,...args);};
  sharing().querySelector('[data-copy-room-id="'+addedRoom.id+'"]').click();await flush();
  window.setTimeout=originalTimeout;
  assert.equal(sharing().querySelector('[data-copy-room-id="'+addedRoom.id+'"]').dataset.copied,'true');
  assert.ok(sharing().querySelector('[data-copy-room-id="'+addedRoom.id+'"] .ai-copy-icon'),'copy feedback keeps a fixed icon slot');
  assert.equal(copyNode.textContent,copyText,'copy feedback never changes the button width through its label');
  resetCopy();assert.equal(copyNode.dataset.copied,undefined,'copy check resets after the feedback interval');
  assert.deepEqual(copied,[draftId]);assert.deepEqual(aiCfg.providers,providersBeforeCopy);assert.equal(aiWrites().length,writesBeforeCopy,'copy never toggles providing');
  assert.ok(document.querySelector('.toast.success').parentElement.textContent.includes('\u30b3\u30d4\u30fc'));
  const beforeFailedAdd=aiCfg.providers.length;
  rejectSetting='ai.providers';await submitRoom('rejected-room');rejectSetting=null;
  assert.equal(aiCfg.providers.length,beforeFailedAdd);assert.equal(addForm().hidden,false);
  assert.equal(addForm().querySelector('[data-add-room-id]').value,'rejected-room','failed create retains the draft');
  assert.equal(addForm().querySelector('button.primary').disabled,false);
  addForm().dispatchEvent(new window.KeyboardEvent('keydown',{key:'Escape',bubbles:true}));assert.equal(addForm().hidden,true);
  document.querySelectorAll('.toast.error button').forEach(b=>b.click());
  // Happy DOM has no layout/WAAPI; controlled geometry checks interruption and cleanup.
  const content=document.querySelector('.ai-subtab-content'),animations=[];
  const heights={connection:420,tasks:730,sharing:610};let visualHeight=null;
  content.getBoundingClientRect=()=>({height:visualHeight??heights[content.querySelector('.subtab-page.active').dataset.subtab]});
  content.animate=(frames,options)=>{
    const animation={frames,options,cancelled:false,cancel(){this.cancelled=true;visualHeight=null;this.oncancel?.();}};
    animations.push(animation);return animation;
  };
  const switchTab=name=>document.querySelector('#panel-ai [data-subtab-target='+name+']').click();
  switchTab('connection');
  assert.equal(animations.at(-1).options.duration,200);assert.equal(animations.at(-1).options.easing,'cubic-bezier(.2,.8,.2,1)');
  visualHeight=520;const interrupted=animations.at(-1);switchTab('tasks');
  assert.equal(interrupted.cancelled,true);assert.equal(animations.at(-1).frames[0].height,'520px','rapid switch starts from the displayed height');
  const finalAnimation=animations.at(-1);interrupted.onfinish();assert.equal(finalAnimation.cancelled,false,'stale completion cannot cancel the latest switch');
  finalAnimation.onfinish();assert.equal(finalAnimation.cancelled,true);
  assert.equal(content.style.height,'');assert.equal(content.style.overflow,'');assert.equal(content.getBoundingClientRect().height,730,'completed tab uses natural height');
  switchTab('sharing');switchTab('connection');switchTab('tasks');
  await new Promise(resolve=>setTimeout(resolve,300));
  assert.equal(animations.at(-1).cancelled,true,'fallback cleanup handles a missing finish event');
  switchTab('sharing');const reduceInterrupted=animations.at(-1);
  reducedMotion.matches=true;reducedMotion.dispatchEvent(new window.Event('change'));
  assert.equal(reduceInterrupted.cancelled,true,'changing reduced-motion cancels an active animation');
  const animationCount=animations.length;switchTab('tasks');switchTab('sharing');
  assert.equal(animations.length,animationCount,'reduced-motion switches immediately');assert.equal(content.style.height,'');assert.equal(content.style.overflow,'');
  reducedMotion.matches=false;content.animate=undefined;
  // Drive WAAPI completion explicitly: collapsed content remains until finish.
  const motionRuns=[];
  window.HTMLElement.prototype.animate=function(frames,options){
    const run={node:this,frames,options,playState:"paused",cancelled:false,cancel(){this.cancelled=true;this.playState="idle";}};
    motionRuns.push(run);return run;
  };
  const latest=node=>motionRuns.findLast(run=>run.node===node || node.id && run.node.id===node.id);
  const finish=node=>latest(node).onfinish?.();
  switchTab('connection');
  const editTrigger=card('local').querySelector('.ai-provider-summary');editTrigger.click();
  let motionEditor=card('local').querySelector('.ai-provider-editor');
  assert.equal(editTrigger.getAttribute('aria-expanded'),'true');
  assert.equal(latest(motionEditor).frames[0].height,'0px');
  assert.equal(latest(motionEditor).frames[0].opacity,'0');
  finish(motionEditor);
  assert.equal(motionEditor.style.height,'');assert.equal(motionEditor.style.overflow,'','expanded focus rings are not clipped');
  editTrigger.click();
  assert.ok(motionEditor.isConnected,'closing editor stays mounted');assert.equal(motionEditor.inert,true);
  assert.equal(editTrigger.getAttribute('aria-expanded'),'false');
  assert.equal(latest(motionEditor).frames[1].height,'0px');assert.equal(latest(motionEditor).frames[1].opacity,0);
  finish(motionEditor);assert.equal(motionEditor.isConnected,false,'editor removed only after collapse');
  connection=openConnection();finish(connection);finish(connection.parentElement);
  connection.querySelector('[data-connection-kind=http]').click();
  const switchingFields=connection.querySelector('.ai-connection-fields');
  assert.ok(latest(switchingFields).frames[0].height,'kind changes animate the fields height');finish(switchingFields);
  input(connection.querySelector('[data-connection-name]'),'Animated HTTP');input(connection.querySelector('[data-connection-url]'),'https://animated.example/v1');
  connection.requestSubmit();await flush();
  const animatedProvider=aiCfg.providers.find(p=>p.label==='Animated HTTP');
  assert.equal(latest(card(animatedProvider.id)).frames[0].opacity,0,'new HTTP card enters once');
  assert.equal(modelCalls(animatedProvider.id),1,'animated HTTP card fetches immediately');
  finish(card(animatedProvider.id));finish(connection);finish(connection.parentElement);
  switchTab('sharing');openAdd();finish(addForm());finish(addForm().parentElement);
  addForm().dispatchEvent(new window.KeyboardEvent('keydown',{key:'Escape',bubbles:true}));
  assert.equal(addForm().hidden,false,'closing popup remains mounted until finish');
  const closingRoom=latest(addForm());openAdd();
  assert.equal(closingRoom.cancelled,true,'reopening cancels the close');
  finish(addForm());finish(addForm().parentElement);assert.equal(addForm().hidden,false);assert.equal(addForm().parentElement.inert,false);
  addForm().dispatchEvent(new window.KeyboardEvent('keydown',{key:'Escape',bubbles:true}));finish(addForm());finish(addForm().parentElement);
  assert.equal(addForm().hidden,true);assert.equal(addForm().style.overflow,'');
  await submitRoom('motion-test-room','Motion test');
  const motionProvider=aiCfg.providers.find(p=>p.base_url==='mist-network://motion-test-room');
  const enteredCard=card(motionProvider.id),enteredChip=sharing().querySelector('[data-room-group="'+motionProvider.id+'"]');
  for(const node of [enteredCard,enteredChip]){
    assert.equal(latest(node).frames[0].opacity,0);assert.equal(latest(node).frames[0].transform,'translateY(4px)');
    finish(node);assert.equal(node.style.transform,'');assert.equal(node.style.opacity,'');
  }
  finish(addForm());finish(addForm().parentElement);
  switchTab('connection');enteredCard.querySelector('.ai-provider-summary').click();finish(enteredCard.querySelector('.ai-provider-editor'));
  window.confirm=()=>true;enteredCard.querySelector('button.danger').click();await flush();
  assert.ok(enteredCard.isConnected,'removed card stays mounted for its fade');assert.equal(enteredCard.inert,true);
  assert.equal(document.querySelector('.ai-provider-card').dataset.providerId,'local','fading removal preserves the other cards positions');
  assert.equal(latest(enteredCard).frames[1].opacity,0);finish(enteredCard);assert.equal(enteredCard.isConnected,false);
  assert.ok(enteredChip.isConnected);finish(enteredChip);assert.equal(enteredChip.isConnected,false);
  switchTab('sharing');
  dialog=await openPicker('settings-ai-default_ref');
  const animatedOverlay=dialog.parentElement;
  assert.equal(latest(animatedOverlay).frames[0].opacity,0);assert.equal(latest(dialog).frames[0].transform,'scale(.98)');
  finish(animatedOverlay);finish(dialog);
  const animatedResults=dialog.querySelector('.ai-model-results');
  dialog.querySelector('[data-source=home]').click();
  assert.equal(latest(animatedResults).options.duration,120,'source content uses fast cross-fade');
  assert.deepEqual(structuredClone(latest(animatedResults).frames),[{opacity:0},{opacity:1}]);finish(animatedResults);
  dialog.querySelector('.modal-close').click();
  assert.ok(animatedOverlay.isConnected);assert.equal(animatedOverlay.inert,true);
  assert.equal(latest(animatedOverlay).options.duration,120);finish(animatedOverlay);
  assert.equal(animatedOverlay.isConnected,false);finish(dialog);
  switchTab('connection');editTrigger.click();motionEditor=card('local').querySelector('.ai-provider-editor');
  reducedMotion.matches=true;reducedMotion.dispatchEvent(new window.Event('change'));
  assert.equal(latest(motionEditor).cancelled,true,'reduced motion finishes in-progress disclosure');
  const motionCount=motionRuns.length;
  editTrigger.click();
  assert.equal(motionEditor.isConnected,false);
  connection=openConnection();connection.dispatchEvent(new window.KeyboardEvent('keydown',{key:'Escape',bubbles:true}));assert.equal(connection.hidden,true);
  switchTab('sharing');openAdd();addForm().dispatchEvent(new window.KeyboardEvent('keydown',{key:'Escape',bubbles:true}));assert.equal(addForm().hidden,true);
  dialog=await openPicker('settings-ai-default_ref');dialog.querySelector('[data-source=home]').click();dialog.querySelector('.modal-close').click();assert.equal(dialog.isConnected,false);
  assert.equal(motionRuns.length,motionCount,'reduced-motion interactions create no animations');
  const highlight=document.querySelector('.ai-tab-highlight');
  assert.ok(highlight.style.transform.startsWith('translate('),'highlight ends at the selected tab');
  reducedMotion.matches=false;delete window.HTMLElement.prototype.animate;
  // Both catalogs render concrete labels, and model recents tolerate unavailable storage.
  const tabNames={en:['Connections','Tasks','Sharing'],ja:['\u63a5\u7d9a\u5148','\u30bf\u30b9\u30af','\u63d0\u4f9b'],zh:['\u8fde\u63a5','\u4efb\u52a1','\u5171\u4eab']};
  for(const language of ['en','zh','ja']) {
    document.getElementById('lang-'+language+'-btn').click();await flush();
    assert.ok(!document.getElementById('ai-task-editor').textContent.includes('ai.model.'));
    assert.deepEqual([...document.querySelectorAll('#panel-ai [data-subtab-target]')].map(n=>n.textContent),tabNames[language]);
    assert.ok(!sharing().textContent.includes('ai.sharing.'));
    connection=openConnection();
    assert.equal(connection.querySelector('[data-connection-name]').parentElement.firstChild.textContent,{en:'Name',zh:'\u540d\u79f0',ja:'\u540d\u524d'}[language]);
    assert.equal(connection.querySelector('button.primary').textContent,{en:'Add',zh:'\u6dfb\u52a0',ja:'\u8ffd\u52a0'}[language]);
    assert.ok(!connection.textContent.includes('ai.connection.'),'connection popup strings are localized');
    connection.dispatchEvent(new window.KeyboardEvent('keydown',{key:'Escape',bubbles:true}));
    card('old').querySelector('.ai-provider-summary').click();
    const localizedEditor=card('old').querySelector('.ai-provider-editor');
    assert.equal(localizedEditor.querySelector('button.danger').textContent,{en:'Delete connection',zh:'\u5220\u9664\u8fde\u63a5',ja:'\u63a5\u7d9a\u5148\u3092\u524a\u9664'}[language]);
    const localizedUrl=localizedEditor.querySelector('[data-provider-field=base_url]');
    input(localizedUrl,'https://locale.example/v1');localizedUrl.dispatchEvent(new window.Event('blur'));await flush();
    assert.equal(localizedEditor.querySelector('[data-provider-key-note]').hidden,false);
    assert.ok(!localizedEditor.textContent.includes('ai.provider.'),'inline field strings are localized');
    card('old').querySelector('.ai-provider-summary').click();
    dialog=await openPicker('settings-ai-stt');
    assert.equal(dialog.querySelector('.ai-model-sources').getAttribute('aria-label'),{en:'Sources',zh:'\u6765\u6e90',ja:'\u63a5\u7d9a\u5148'}[language]);
    input(dialog.querySelector('input[type=search]'),'gem');
    assert.equal(dialog.querySelector('[data-source=all] .ai-model-source-name').textContent,{en:'All',zh:'\u5168\u90e8',ja:'\u3059\u3079\u3066'}[language]);
    assert.ok(!dialog.textContent.includes('ai.model.'));dialog.querySelector('.modal-close').click();
  }
  // External contributions are read-only and never sent through config.set.
  fixtures['ai.external.get'].registrations=[{owner:'tc-npc',label:'NPC <img src=x>',providers:[{id:'http',label:'Managed HTTP',base_url:'http://external.example/v1',api_key:'***',enabled:true}],rooms:[{room:'external-room',consume:true,provide:true,shared:[{provider_id:'http',model:'external-model'}]}],warnings:[],status:{rooms:[{room:'external-room',joined:true,providing:true,peers:2,models:['external-model']}]}}];
  const poll=intervals.find(i=>i.ms===5000);
  for (const language of ['en','zh','ja']) {
    document.getElementById('lang-'+language+'-btn').click();poll.callback();await flush();
    const external=document.querySelector('[data-external-owner="tc-npc"]');
    assert.ok(external);assert.ok(external.textContent.includes('tc-npc'));
    assert.ok(external.textContent.includes('external-model'));assert.ok(!external.querySelector('input,select,textarea'));
    assert.equal(external.querySelectorAll('button').length,1);assert.ok(!external.querySelector('img'));
    assert.ok(!external.textContent.includes('ai.external.'));assert.ok(!external.textContent.includes('ai.sharing.'));
    assert.equal(external.querySelector('button').textContent,{en:'Unregister',zh:'\u53d6\u6d88\u6ce8\u518c',ja:'\u767b\u9332\u3092\u89e3\u9664'}[language]);
  }
  const remove=document.querySelector('[data-external-remove="tc-npc"]'),beforeCalls=calls.length;
  window.confirm=()=>false;remove.click();await flush();assert.equal(calls.length,beforeCalls,'cancel leaves registrations unchanged');
  window.confirm=()=>true;remove.click();await flush();
  assert.equal(document.querySelector('[data-external-owner="tc-npc"]'),null);
  assert.ok(calls.slice(beforeCalls).some(c=>c.cmd==='ai.external.remove' && c.args.owner==='tc-npc'));
  assert.ok(!calls.slice(beforeCalls).some(c=>c.cmd==='config.set'),'external removal never rewrites user config');
  const oldGet=window.localStorage.getItem.bind(window.localStorage),oldSet=window.localStorage.setItem.bind(window.localStorage);
  window.localStorage.getItem=key=>{if(key==='mistl-recent-models')throw Error('blocked');return oldGet(key);};
  window.localStorage.setItem=(key,value)=>{if(key==='mistl-recent-models')throw Error('blocked');return oldSet(key,value);};
  await pick('settings-ai-default_ref','local','gem-small');
  window.localStorage.getItem=oldGet;window.localStorage.setItem=oldSet;
  // A rejected setting restores the visible assignment and leaves config intact.
  const beforeRejected=structuredClone(aiCfg.default_ref);
  rejectSetting='ai.default_ref';await pick('settings-ai-default_ref','local','model-299');rejectSetting=null;
  assert.deepEqual(aiCfg.default_ref,beforeRejected);
  assert.ok(document.querySelector('#settings-ai-default_ref .ai-model-trigger').textContent.includes(beforeRejected.model));
  assert.ok(document.querySelector('.toast.error').textContent.includes('setting rejected'));
  document.querySelectorAll('.toast.error button').forEach(b=>b.click());
  // Default task effort distinguishes explicit none from absent; voice speed
  // writes the nested key without replacing the rest of the voice config.
  switchTab('tasks');
  const effort=()=>document.getElementById('settings-ai-default_reasoning_effort');
  const defaultRow=document.querySelector('#settings-ai-default_ref').closest('.ai-task-row');
  assert.equal(effort().closest('.ai-task-row'),defaultRow,'effort shares the model row');
  assert.ok(defaultRow.classList.contains('with-effort'));
  assert.equal(defaultRow.querySelectorAll('.settings-label').length,1,'no separate effort label row');
  assert.equal(document.getElementById('settings-ai-request_timeout_secs').value,'120');
  assert.equal(document.getElementById('settings-ai-api_listen').value,'127.0.0.1:6478');
  assert.ok(effort().textContent.includes('\u672a\u8a2d\u5b9a'));
  effort().click();
  assert.deepEqual([...document.querySelectorAll('.ai-reasoning-option')].map(n=>n.dataset.value),['','none','minimal','low','medium','high','xhigh','max']);
  assert.equal(document.activeElement,document.querySelector('.ai-reasoning-menu'));
  document.querySelector('.ai-reasoning-menu').dispatchEvent(new window.KeyboardEvent('keydown',{key:'End',bubbles:true}));
  assert.equal(document.querySelector('.ai-reasoning-option[data-active=true]').dataset.value,'max');
  document.querySelector('.ai-reasoning-menu').dispatchEvent(new window.KeyboardEvent('keydown',{key:'Enter',bubbles:true}));await flush();
  assert.equal(aiCfg.default_reasoning_effort,'max');assert.equal(document.activeElement,effort());
  assert.equal(effort().querySelectorAll('i.filled').length,6);
  for(const value of ['none','minimal','low','medium','high','xhigh','max','']) {
    effort().click();document.querySelector('.ai-reasoning-option[data-value="'+value+'"]').click();await flush();
    assert.equal(aiCfg.default_reasoning_effort,value||null);
    assert.ok(calls.some(c=>c.cmd==='config.set'&&c.args.path==='ai.default_reasoning_effort'&&c.args.value===(value||null)));
  }
  rejectSetting='ai.default_reasoning_effort';effort().click();document.querySelector('.ai-reasoning-option[data-value=high]').click();await flush();rejectSetting=null;
  assert.equal(aiCfg.default_reasoning_effort,null);assert.ok(effort().textContent.includes('\u672a\u8a2d\u5b9a'));
  effort().click();document.querySelector('.ai-reasoning-menu').dispatchEvent(new window.KeyboardEvent('keydown',{key:'Escape',bubbles:true}));
  assert.equal(document.querySelector('.ai-reasoning-menu'),null);assert.equal(document.activeElement,effort());
  effort().click();document.body.dispatchEvent(new window.PointerEvent('pointerdown',{bubbles:true}));assert.equal(document.querySelector('.ai-reasoning-menu'),null);
  await pick('settings-ai-tts','local','gem-small');
  let speed=document.getElementById('settings-ai-tts-speed');
  aiCfg.tts.voice='preserved-voice';aiCfg.tts.lang_voices={ja:'preserved-ja'};
  assert.equal(speed.tagName,'BUTTON');assert.equal(speed.disabled,false);
  const speedOptions=()=>[...document.querySelectorAll('.ai-choice-menu [role=option]')];
  const chooseSpeed=async value=>{speed.click();document.querySelector('.ai-choice-menu [data-value="'+value+'"]').click();await flush();};
  speed.click();
  assert.deepEqual(speedOptions().map(n=>n.dataset.value),['','0.75','1','1.25','1.5','2']);
  assert.deepEqual(speedOptions().slice(1).map(n=>n.textContent.trim()),['0.75×','1×','1.25×','1.5×','2×']);
  assert.equal(document.activeElement,document.querySelector('.ai-choice-menu'));
  document.querySelector('.ai-choice-menu').dispatchEvent(new window.KeyboardEvent('keydown',{key:'End',bubbles:true}));
  document.querySelector('.ai-choice-menu').dispatchEvent(new window.KeyboardEvent('keydown',{key:'Enter',bubbles:true}));await flush();
  assert.equal(aiCfg.tts.speed,2);assert.equal(document.activeElement,speed);
  for(const value of ['0.75','1','1.25','1.5','2']) {await chooseSpeed(value);assert.equal(aiCfg.tts.speed,Number(value));}
  // A stored custom speed is retained in numeric order, with the same × label.
  for(const value of [.25,1.35,4]) {
    aiCfg.tts.speed=value;
    document.querySelector('[data-target="settings"]').click();await flush();
    speed=document.getElementById('settings-ai-tts-speed');speed.click();
    assert.ok(speedOptions().some(n=>n.dataset.value===String(value)&&n.querySelector('.ai-reasoning-text').textContent===value+'×'));
    const expected=['',...[.75,1,1.25,1.5,2,value].sort((a,b)=>a-b).map(String)];
    assert.deepEqual(speedOptions().map(n=>n.dataset.value),expected);
    document.querySelector('.ai-choice-menu [data-value="'+value+'"]').click();await flush();
    assert.equal(aiCfg.tts.speed,value);
  }
  assert.equal(aiCfg.tts.voice,'preserved-voice');assert.deepEqual(aiCfg.tts.lang_voices,{ja:'preserved-ja'});
  assert.ok(calls.some(c=>c.cmd==='config.set'&&c.args.path==='ai.tts.speed'&&c.args.value===1.35));
  rejectSetting='ai.tts.speed';await chooseSpeed('2');rejectSetting=null;
  assert.equal(aiCfg.tts.speed,4);assert.equal(speed.textContent.trim(),'4×');assert.equal(speed.disabled,false);
  await chooseSpeed('');assert.equal(aiCfg.tts.speed,null);
  assert.ok(calls.some(c=>c.cmd==='config.set'&&c.args.path==='ai.tts.speed'&&c.args.value===null));
  assert.equal(speed.textContent.trim(),'提供側の既定（未指定）');
  speed.click();assert.deepEqual(speedOptions().map(n=>n.dataset.value),['','0.75','1','1.25','1.5','2']);
  document.querySelector('.ai-choice-menu').dispatchEvent(new window.KeyboardEvent('keydown',{key:'Escape',bubbles:true}));
  assert.equal(document.querySelector('.ai-choice-menu'),null);assert.equal(document.activeElement,speed);
  speed.click();document.body.dispatchEvent(new window.PointerEvent('pointerdown',{bubbles:true}));assert.equal(document.querySelector('.ai-choice-menu'),null);
  dialog=await openPicker('settings-ai-tts');dialog.querySelector('[data-source=browser]').click();await flush();
  assert.equal(aiCfg.tts,null);assert.equal(speed.disabled,true);
  await pick('settings-ai-tts','local','gem-small');
  for(const language of ['en','zh','ja']) {
    effort().click();document.getElementById('lang-'+language+'-btn').click();await flush();
    assert.equal(document.querySelector('.ai-reasoning-menu'),null,'language rebuild cleans up the portal');
    effort().click();assert.ok(!document.querySelector('.ai-reasoning-menu').textContent.includes('ai.effort.'));
    document.querySelector('.ai-reasoning-menu').dispatchEvent(new window.KeyboardEvent('keydown',{key:'Escape',bubbles:true}));
    assert.ok(document.querySelector('label[for=settings-ai-tts-speed]').textContent.trim());
    const localizedSpeed=document.getElementById('settings-ai-tts-speed');localizedSpeed.click();
    assert.equal(document.querySelector('.ai-choice-menu [data-value=""] .ai-reasoning-text').textContent,{en:'Provider default (not specified)',zh:'提供方默认（未指定）',ja:'提供側の既定（未指定）'}[language]);
    document.getElementById('lang-'+({en:'zh',zh:'ja',ja:'en'}[language])+'-btn').click();await flush();
    assert.equal(document.querySelector('.ai-choice-menu'),null,'language rebuild also removes the speed portal');
  }
  document.getElementById('lang-ja-btn').click();await flush();
  for(const input of document.querySelectorAll('input[type=checkbox]')) {
    if(/^bot-lang-/.test(input.id)) {assert.equal(input.getAttribute('role'),null);continue;}
    assert.equal(input.getAttribute('role'),'switch');assert.ok(input.closest('.toggle-switch'),'all binary inputs share the switch component');
  }
  for(const theme of ['light','dark']) {
    document.documentElement.dataset.theme=theme;
    for(const control of document.querySelectorAll('.toggle-switch')) {
      const css=window.getComputedStyle(control);assert.equal(css.width,'36px');assert.equal(css.height,'22px');
    }
  }
  document.querySelectorAll('.toast.error button').forEach(b=>b.click());
  assert.deepEqual(errors, []);
  assert.equal(document.querySelectorAll('.toast.error').length, 0, 'no hidden initialization failure');
  console.log('PASS: S3 shared 36x22 switches in both themes, multi-select semantics, same-row effort, 8 effort choices/unset/keyboard/rollback/portal cleanup, TTS speed choices/custom/provider default/keyboard/rollback/voice preservation, configured inputs, en/zh/ja controls');
  console.log('PASS: full dashboard integration; two-pane model sources/search/counts/narrowing/restore, recent/voice/inherited refs, no manual-entry row, pane keyboard, fixed size/narrow chips/cross-fade/reduced motion, single Room header status, I 360/390 status layout/labels/details/touch/press origin/editor preservation, H header popup/kind/Name/validation/press origin/persistence/entrance/fetch, F.1 grouped subtitles/Recent labels, J single-column/header keyboard/independent switch and dot/blur and Enter commits/Esc/inline feedback and errors/masked-origin combined writes/duplicate and confirmed deletion, provider/room cards, sharing add-room/existing/duplicate/random/copy, eased height cleanup, disabled-reference preservation, bot transforms, all locales and existing features');
} finally {
  process.removeListener('unhandledRejection', onUnhandled);
  await window.happyDOM.close();
}
