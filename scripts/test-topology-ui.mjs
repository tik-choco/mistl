import assert from 'node:assert/strict';
import fs from 'node:fs';
import { Window } from 'happy-dom';

const html = fs.readFileSync(new URL('../src/web/assets/index.html', import.meta.url), 'utf8');
const window = new Window({settings:{disableJavaScriptEvaluation:true,disableJavaScriptFileLoading:true,disableCSSFileLoading:true}});
const {document} = window;
let time = 0, sequence = 0, frames = new Map(), intersection, requests = 0;
const timers = new Map(), copies = [];
const motion = Object.assign(new window.EventTarget(), {matches:false});
window.matchMedia = () => motion;
window.performance.now = () => time;
window.requestAnimationFrame = fn => { const id = ++sequence; frames.set(id, fn); return id; };
window.cancelAnimationFrame = id => frames.delete(id);
window.setInterval = (fn, ms) => { const id = ++sequence; timers.set(id, {fn,ms}); return id; };
window.clearInterval = id => timers.delete(id);
window.IntersectionObserver = class { constructor(fn) { intersection = fn; } observe() {} };
window.ResizeObserver = class { observe() {} };
const advance = (count = 1) => {
  for (let i = 0; i < count; i++) {
    time += 16.7;
    const batch = frames; frames = new Map();
    for (const fn of batch.values()) fn(time);
    assert.ok(frames.size <= 1, 'Only one animation frame may be pending');
  }
};
const settle = () => {
  for (let i = 0; frames.size && i < 800; i++) advance();
  assert.equal(frames.size, 0, 'Physics cools and stops requesting animation frames');
};
const snapshot = () => ({
  self:{node_id:'self',did:'did:key:self',display_name:'My node'}, rooms:['design','studio'],
  peers:[{node_id:'alice',name:'Alice',state:'connected'},{node_id:'bob',state:'connected'},
    {node_id:'carol',state:'connecting'},{node_id:'dave',state:'failed'}],
  room_peers:[{room:'design',peers:[{node_id:'alice',state:'connected'},{node_id:'bob',state:'connected'}]},
    {room:'studio',peers:[{node_id:'alice',state:'connected'},{node_id:'carol',state:'connecting'},{node_id:'dave',state:'failed'}]}],
  stream:{running:false}, activity:[]
});
let response = snapshot();
try {
  document.write(html.replace(/<script\b[^>]*>[\s\S]*?<\/script>/g, ''));
  const svg = document.getElementById('topology-svg');
  svg.getBoundingClientRect = () => ({left:0,top:0,width:420,height:380,bottom:380});
  svg.setPointerCapture = () => {};
  Object.assign(window, {
    el:(tag,attrs={})=>{const node=document.createElement(tag);for(const [key,value] of Object.entries(attrs)) {if(key==='text')node.textContent=value;else node.setAttribute(key,value);}return node;},
    t:key=>key, shortNodeId:id=>id || '', copyToClipboard:id=>copies.push(id), state:{topology:response},
    cascadeRoleBadge:document.getElementById('cascade-role-badge'), cascadeSource:document.getElementById('cascade-source'),
    cascadeOffNote:document.getElementById('cascade-off-note'),
    api:async () => { requests++; return structuredClone(response); }
  });
  window.eval(fs.readFileSync(new URL('../src/web/assets/features/topology.js', import.meta.url), 'utf8'));
  const start = html.indexOf('  /* ---------- live topology');
  window.eval(html.slice(start, html.indexOf('  /* ---------- ai panel ---------- */', start)));
  const feature = window.topologyPresentation;
  let graph = window.renderTopology();
  assert.equal(graph.nodes.length, 7, 'Self, four peers and two hollow room hubs');
  assert.equal(graph.edges.length, 7, 'A peer in two rooms links to both hubs');
  settle();
  const getNode = key => [...svg.querySelectorAll('[data-topology-key]')].find(n=>n.getAttribute('data-topology-key')===key);
  const alice = getNode('alice'), bob = getNode('bob');
  const transform = alice.getAttribute('transform');
  window.renderTopology();
  assert.equal(getNode('alice'), alice, 'Polling preserves DOM and keyboard targets');
  assert.equal(alice.getAttribute('transform'), transform, 'Unchanged poll preserves position');
  assert.equal(frames.size, 0, 'Unchanged inactive snapshot does not reheat physics');
  alice.dispatchEvent(new window.MouseEvent('mouseenter'));
  assert.ok(bob.classList.contains('is-dimmed'), 'Hover dims non-neighbours');
  assert.equal(getNode('room:design').classList.contains('is-dimmed'), false);
  assert.equal(svg.querySelectorAll('.topology-edge.is-highlighted').length, 2);
  window.renderTopology(); assert.ok(bob.classList.contains('is-dimmed'), 'Hover survives polling');
  alice.dispatchEvent(new window.MouseEvent('mouseleave'));
  response.peers[2].state = 'connected'; window.renderTopology();
  assert.ok(frames.size > 0, 'A reconnect on an existing node wakes its connection ripple');
  advance(10); assert.ok(Number(getNode('carol').querySelector('.topology-ripple').getAttribute('opacity')) > 0); settle();
  const longLabel = '<img src=x onerror=alert(1)>' + '長い名前'.repeat(25);
  const longModels = graph.nodes.map(n=>n.key==='alice'?{...n,label:longLabel,name:longLabel}:n);
  feature.setSnapshot(longModels); alice.focus();
  const preview = document.querySelector('.topology-preview'), dialog = document.querySelector('.topology-detail');
  assert.equal(preview.hidden, false);
  assert.ok(preview.textContent.includes(longLabel), 'Full preview label stays literal text');
  assert.equal(preview.querySelector('img'), null);
  assert.ok(alice.querySelector('text').textContent.endsWith('…'));
  alice.dispatchEvent(new window.KeyboardEvent('keydown', {key:'Enter',bubbles:true}));
  assert.equal(dialog.open, true); assert.equal(preview.hidden, true); assert.equal(copies.length, 0);
  assert.ok(dialog.textContent.includes(longLabel) && dialog.textContent.includes('studio'));
  dialog.querySelector('.primary').click(); assert.deepEqual(copies, ['alice']);
  const content = dialog.querySelector('.topology-detail-body'), child = content.firstChild;
  feature.setSnapshot(longModels); assert.equal(content.firstChild, child, 'Stable detail preserves text selection');
  dialog.querySelector('.topology-detail-close').click();
  assert.equal(document.activeElement, alice, 'Dialog restores persistent node focus');
  window.MistlPeers = {byDid:()=>null,byNode:id=>id==='bob'?{name:'Bobby'}:null,label:p=>p.name,avatarEl:()=>document.createElement('span')};
  window.renderTopology(); assert.equal(bob.querySelector('text').textContent, 'Bobby');

  const viewport = svg.querySelector('.topology-viewport');
  const before = viewport.getAttribute('transform');
  svg.dispatchEvent(new window.WheelEvent('wheel',{clientX:200,clientY:180,deltaY:800,cancelable:true}));
  assert.notEqual(viewport.getAttribute('transform'), before); assert.ok(svg.classList.contains('is-zoomed-out'));
  const pointer = (target,type,id,x,y) => target.dispatchEvent(new window.PointerEvent(type,{button:0,pointerId:id,clientX:x,clientY:y,bubbles:true}));
  pointer(svg,'pointerdown',1,200,180); pointer(svg,'pointermove',1,230,200); pointer(svg,'pointerup',1,230,200);
  const manualView = viewport.getAttribute('transform');
  response.peers.push({node_id:'eve',state:'connected'}); window.renderTopology(); settle();
  assert.equal(viewport.getAttribute('transform'), manualView, 'Node changes respect manual camera');
  svg.dispatchEvent(new window.MouseEvent('dblclick',{bubbles:true}));
  assert.notEqual(viewport.getAttribute('transform'), manualView);
  const position = bob.getAttribute('transform');
  pointer(bob,'pointerdown',2,180,160); pointer(svg,'pointermove',2,310,240); advance();
  assert.notEqual(bob.getAttribute('transform'), position);
  pointer(svg,'pointerup',2,310,240); bob.dispatchEvent(new window.MouseEvent('click')); assert.equal(dialog.open, false, 'Dragging does not open details'); settle();
  pointer(svg,'pointerdown',3,100,100); pointer(svg,'pointerdown',4,200,100);
  const pinchView = viewport.getAttribute('transform'); pointer(svg,'pointermove',4,300,100);
  assert.notEqual(viewport.getAttribute('transform'), pinchView, 'Pinch zoom works');
  pointer(svg,'pointerup',3,100,100); pointer(svg,'pointerup',4,300,100);

  const paths = [], originalActivity = feature.activity;
  feature.activity = (path,count,bytes) => { paths.push([...path]); originalActivity(path,count,bytes); };
  response.activity = [{room:'design',node_id:'alice',tx:{count:10,bytes:40000,age_ms:20},rx:null}];
  window.updateTopologyActivity(response, window.buildTopologyGraph(response));
  assert.deepEqual(paths.at(-1), ['self','room:design','alice'], 'TX routes through room hub');
  assert.ok(svg.querySelectorAll('.topology-particle').length > 0); advance(20); settle();
  window.updateTopologyActivity(response, window.buildTopologyGraph(response));
  assert.equal(svg.querySelectorAll('.topology-particle').length, 0, 'Unchanged counters produce no traffic');
  response.activity = [{...response.activity[0],rx:{count:4,bytes:8000,age_ms:10}}];
  window.updateTopologyActivity(response, graph);
  assert.deepEqual(paths.at(-1), ['alice','room:design','self'], 'RX reverses route'); settle();
  response.activity = [{...response.activity[0],tx:{count:200,bytes:900000,age_ms:20}}];
  graph = window.renderTopology(); window.updateTopologyActivity(response, graph);
  assert.ok(svg.querySelectorAll('.topology-particle').length > 1, 'Count/byte deltas scale burst'); settle();
  response.activity = [{...response.activity[0],tx:{count:1,bytes:1,age_ms:10}}];
  window.updateTopologyActivity(response, graph); assert.equal(frames.size, 0, 'Rollback silently reseeds');
  motion.matches = true; motion.dispatchEvent(new window.Event('change')); settle();
  response.peers[3].state = 'connected'; window.renderTopology();
  assert.ok(getNode('dave').classList.contains('has-activity'), 'Reduced-motion reconnect uses a static highlight');
  response.activity = [{...response.activity[0],tx:{count:4,bytes:200,age_ms:10}}];
  window.updateTopologyActivity(response, graph);
  assert.equal(svg.querySelectorAll('.topology-particle').length, 0); assert.ok(alice.classList.contains('has-activity'));
  motion.matches = false; motion.dispatchEvent(new window.Event('change')); settle();

  response.stream = {running:true,backend:'relay',publisher:'alice',clients:2,
    cascade:{enabled:true,role:'follower',leader:'alice',source:'leader',relay_peers:['alice']},
    flow:{video_age_ms:20,audio_age_ms:40,video_delivered_age_ms:10,audio_delivered_age_ms:20}};
  window.renderTopology(); advance(20);
  assert.ok(svg.querySelector('.topology-particle.is-media'), 'Media creates a continuous particle stream');
  assert.equal(document.getElementById('cascade-role-badge').hidden, false);
  assert.equal(document.getElementById('topology-flow-note').hidden, false);
  advance(240); settle(); assert.equal(svg.querySelectorAll('.is-flowing').length, 0, 'Flow expires without another snapshot');
  window.topologyPolling = true; window.syncTopologyPolling();
  await Promise.resolve(); await Promise.resolve();
  assert.ok([...timers.values()].some(t=>t.ms===1000));
  const calls = requests, pending = window.pollTopology(); window.pollTopology(); await pending;
  assert.ok(requests <= calls + 1, 'Overlapping polls share one request');
  intersection([{isIntersecting:false}]);
  assert.equal(frames.size, 0); assert.equal(timers.size, 0, 'Offscreen pauses polling and all animation');
  assert.equal(svg.querySelectorAll('.topology-particle').length, 0);
  intersection([{isIntersecting:true}]); assert.ok(timers.size > 0);
  window.topologyPolling = false; window.syncTopologyPolling(); await window.topologyRequest;
  Object.defineProperty(document,'hidden',{configurable:true,value:true}); document.dispatchEvent(new window.Event('visibilitychange'));
  assert.equal(frames.size, 0, 'Hidden tab pauses');
  Object.defineProperty(document,'hidden',{configurable:true,value:false}); document.dispatchEvent(new window.Event('visibilitychange'));
  response = snapshot(); window.state.topology = response; window.renderTopology(); settle();
  alice.focus(); alice.dispatchEvent(new window.KeyboardEvent('keydown',{key:' ',bubbles:true})); assert.equal(dialog.open, true);
  response.peers = response.peers.filter(p=>p.node_id!=='alice');
  response.room_peers.forEach(r=>r.peers=r.peers.filter(p=>p.node_id!=='alice'));
  window.renderTopology(); assert.equal(getNode('alice'), alice, 'Removed nodes animate out');
  assert.ok(dialog.textContent.includes('topology.details.unavailable'));
  settle(); assert.equal(getNode('alice'), undefined);
  dialog.querySelector('.topology-detail-close').click(); assert.equal(document.activeElement, svg);
  graph = window.buildTopologyGraph({...snapshot(),rooms:['design'],room_peers:[]});
  assert.equal(graph.nodes.some(n=>n.kind==='room'), false);
  assert.ok(graph.edges.every(e=>e.source==='self'||e.target==='self'), 'One room uses direct links');
  window.state.topology = {self:{node_id:'self'},peers:[],rooms:[]}; window.renderTopology(); settle();
  assert.equal(svg.querySelectorAll('.topology-node').length, 1); assert.ok(getNode('self').classList.contains('is-alone'));
  assert.equal(document.getElementById('topology-empty-note').hidden, false);
  assert.equal(document.getElementById('topology-fold'), null); assert.equal(document.getElementById('topology-legend'), null);
  assert.equal(svg.querySelector('.topology-node-sub, .topology-edge-label, .topology-node-glyph'), null);
  assert.match(html, /var POLL_INTERVAL_MS = 5000;/);
  console.log('PASS: topology diff/positions, hubs, cooled rAF, hover, a11y/details, peer names, zoom/pan/pinch/drag/fit, activity deltas/directions, reduced motion, flow expiry, visibility/1s polling, removal/empty state');
} finally { await window.happyDOM.close(); }
