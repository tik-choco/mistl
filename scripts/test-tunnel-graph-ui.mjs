import assert from 'node:assert/strict';
import fs from 'node:fs';
import {Window} from 'happy-dom';

const window = new Window({url:'http://localhost/'});
const {document} = window;
document.documentElement.lang='ja';
document.body.innerHTML='<div id="graph" class="tunnel-graph"></div>';
let time=0, sequence=0, frames=new Map();
const intersections=[];
const motion=Object.assign(new window.EventTarget(),{matches:false});
window.matchMedia=()=>motion; window.performance.now=()=>time;
window.requestAnimationFrame=fn=>{const id=++sequence;frames.set(id,fn);return id;};
window.cancelAnimationFrame=id=>frames.delete(id);
window.IntersectionObserver=class {constructor(fn){intersections.push(fn);}observe(){}};
window.ResizeObserver=class {observe(){}};
const advance=(count=1)=>{for(let i=0;i<count;i++){time+=16.7;const batch=frames;frames=new Map();for(const fn of batch.values())fn(time);}};
const settle=()=>{for(let i=0;frames.size&&i<800;i++)advance();assert.equal(frames.size,0,'inactive graphs stop requesting frames');};
window.SVGElement.prototype.setPointerCapture=()=>{};
window.SVGElement.prototype.click=function(){this.dispatchEvent(new window.MouseEvent('click',{bubbles:true}));};
window.eval(fs.readFileSync(new URL('../src/web/assets/features/topology.js',import.meta.url),'utf8'));
window.eval(fs.readFileSync(new URL('../src/web/assets/features/tunnel-graph.js',import.meta.url),'utf8'));
const rights={edit_links:true,add_forwards:true,remove_forwards:true};
const data={running:true,room:'graph-test',self_id:'self',forwards:[
  {direction:'serve',proto:'tcp',addr:'127.0.0.1:4000',target:'legacy-svc',state:'listening',active_conns:1,bytes_in:1024,bytes_out:2048,graph_managed:false}
],graph:{policy:{broadcast:false,locked:false,links:[],permissions:{}},nodes:[
  {id:'peer-a',node:{broadcast:false,locked:false,links:[],permissions:{...rights},forwards:[{direction:'serve',proto:'tcp',addr:'127.0.0.1:3000',target:'service@peer-a',state:'listening',graph_managed:true}]}},
  {id:'peer-b',node:{broadcast:false,locked:false,links:[],permissions:{...rights},forwards:[]}},
  {id:'legacy<img src=x onerror=alert(1)>',node:null}
]}};
const calls=[];let failure=null;
let confirmAnswer=true;const confirmCalls=[];const busyPorts=new Set();
const get=(id)=>id==='self'?{...data.graph.policy,forwards:data.forwards}:data.graph.nodes.find(n=>n.id===id).node;
const feature=window.MistlTunnelGraph.create({document,host:document.getElementById('graph'),refresh:async()=>feature.render(structuredClone(data)),confirm:(text)=>{confirmCalls.push(text);return confirmAnswer;},api:async(cmd,args)=>{
  calls.push({cmd,args:structuredClone(args)});if(failure)throw new Error(failure);
  if(cmd==='tunnel.forward.remove')return {ok:true};
  if(cmd==='tunnel.port.check')return busyPorts.has(args.addr)?{available:false,error:args.addr+' is already in use by another program; choose a different port',suggestion:'127.0.0.1:18081'}:{available:true};
  if(cmd==='tunnel.forward.propose')return {};
  assert.equal(cmd,'tunnel.graph.command');assert.equal(args.room,data.room);
  const action=args.action,node=get(args.node_id);
  if(action.type==='link'){node.links=node.links.filter(id=>id!==action.peer_id);if(action.connected)node.links.push(action.peer_id);if(args.node_id==='self')data.graph.policy.links=node.links;}
  if(action.type==='broadcast')data.graph.policy.broadcast=action.enabled;
  if(action.type==='lock')data.graph.policy.locked=action.locked;
  if(action.type==='permission')data.graph.policy.permissions[action.peer_id]=action.permissions;
  return {applied:true};
}});
const modal=(scope=document)=>[...scope.querySelectorAll('.tg-modal')].at(-1);
const pick=(value,scope)=>{const b=modal(scope).querySelector(`.tg-choice[data-value="${value}"]`);assert.ok(b,'choice '+value);b.click();};
const submitStep=scope=>modal(scope).querySelector('form').dispatchEvent(new window.Event('submit',{bubbles:true,cancelable:true}));
const step=scope=>modal(scope).querySelector('.tg-dialog-body').dataset.step;
const flush=async()=>{for(let i=0;i<10;i++)await new Promise(r=>setImmediate(r));};
const card=id=>[...document.getElementById('graph').querySelectorAll('.tg-node')].find(n=>n.dataset.nodeId===id);
const panel=id=>{
  const overlay=document.querySelector('.tg-modal'),body=overlay.querySelector('.tg-dialog-body');
  if(overlay.hidden||body.dataset.nodeId!==id||body.dataset.step)card(id).click();
  return overlay;
};
feature.render(structuredClone(data));settle();
assert.equal(document.querySelectorAll('.tg-node').length,4);
assert.equal(document.querySelector('img'),null,'untrusted IDs are literal text');
assert.equal(card('peer-a').querySelector('[role=switch]'),null,'remote cards show no switches; only the owner changes broadcast and lock');
assert.equal(card('peer-a').querySelector('.tg-gear'),null,'only the self card has a settings gear');
card('self').querySelector('.tg-link-handle').click();card('peer-a').querySelector('.tg-link-handle').click();await flush();
assert.deepEqual(calls.slice(-2).map(c=>[c.args.node_id,c.args.action.peer_id]),[['self','peer-a'],['peer-a','self']]);
assert.equal(document.querySelectorAll('.tg-direct').length,1);
document.querySelector('.tg-direct').dispatchEvent(new window.KeyboardEvent('keydown',{key:'Enter'}));
assert.equal(modal().hidden,false,'link details open in a modal');
modal().querySelector('.danger').click();await flush();
assert.equal(modal().hidden,true,'the link editor closes once an action is chosen');
assert.equal(document.querySelectorAll('.tg-direct').length,0,'remove link updates both endpoints');
// A third node can wire two remote nodes when both owners delegated link edits.
card('peer-a').querySelector('.tg-link-handle').click();card('peer-b').querySelector('.tg-link-handle').click();await flush();
assert.deepEqual(calls.slice(-2).map(c=>c.args.node_id),['peer-a','peer-b']);
data.graph.nodes[1].node.locked=true;feature.render(structuredClone(data));
const before=calls.length;card('self').querySelector('.tg-link-handle').click();card('peer-b').querySelector('.tg-link-handle').click();await flush();
assert.equal(calls.length,before,'locked remote denies edits before either side changes');
assert.ok(document.querySelector('.tg-message').classList.contains('tg-error'));
// Forward wizard: one question per step in a modal.
panel('self').querySelector('.tg-add').click();
assert.equal(modal().hidden,false,'the add button opens a modal');
assert.equal(step(),'type');
assert.equal(modal().querySelector('input'),null,'the first step asks only what to do');
assert.deepEqual([...modal().querySelectorAll('.tg-choice')].map(b=>b.dataset.value),['propose','connect','serve'],'self node lists propose first');
pick('serve');assert.equal(step(),'serve');
const serveInput=modal().querySelector('input[type=text]');serveInput.value='127.0.0.1:9000';serveInput.dispatchEvent(new window.Event('input'));
feature.render(structuredClone(data));assert.equal(modal().querySelector('input[type=text]').value,'127.0.0.1:9000','polling preserves draft');
modal().querySelector('.tg-back').click();assert.equal(step(),'type','back returns to the previous question');
assert.equal(modal().querySelector('.tg-chosen').dataset.value,'serve','the earlier answer stays selected');
pick('serve');assert.equal(modal().querySelector('input[type=text]').value,'127.0.0.1:9000','going back keeps typed values');
const callsBeforeBadAddr=calls.length;modal().querySelector('input[type=text]').value='not-an-address';submitStep();
assert.equal(step(),'serve','invalid address stays on the step');assert.ok(modal().querySelector('.tg-dialog-status').classList.contains('tg-error'));
assert.equal(calls.length,callsBeforeBadAddr);
modal().querySelector('input[type=text]').value='127.0.0.1:9000';submitStep();assert.equal(step(),'confirm');
assert.ok(modal().querySelector('.tg-flow').textContent.includes('127.0.0.1:9000'),'confirm step summarizes the forward');
submitStep();await flush();
assert.deepEqual(calls.at(-1).args.action,{type:'add_forward',direction:'serve',proto:'tcp',addr:'127.0.0.1:9000',target:''});
assert.equal(modal().hidden,true,'wizard closes after success');
panel('self').querySelector('.tg-add').click();pick('connect');assert.equal(step(),'service');
assert.ok([...modal().querySelectorAll('.tg-choice')].some(b=>b.dataset.value==='service@peer-a'));
busyPorts.add('127.0.0.1:8080');
pick('service@peer-a');assert.equal(step(),'local');await flush();
assert.deepEqual(calls.at(-1),{cmd:'tunnel.port.check',args:{proto:'tcp',addr:'127.0.0.1:8080'}},'the local step checks the port right away');
assert.equal(modal().querySelector('.tg-dialog-foot .primary').disabled,true,'a busy port cannot be confirmed');
assert.ok(modal().querySelector('.tg-port-note').textContent.includes('使用中'));
submitStep();await flush();assert.equal(step(),'local','submitting a busy port stays on the step');
modal().querySelector('.tg-suggest').click();await flush();
assert.equal(modal().querySelector('input[type=text]').value,'127.0.0.1:18081','the suggested free port can be adopted');
assert.equal(modal().querySelector('.tg-dialog-foot .primary').disabled,false);
submitStep();await flush();assert.equal(step(),'confirm');submitStep();await flush();
assert.deepEqual(calls.at(-1).args.action,{type:'add_forward',direction:'connect',proto:'tcp',addr:'127.0.0.1:18081',target:'service@peer-a'});
busyPorts.clear();
panel('self').querySelector('.tg-add').click();
modal().dispatchEvent(new window.KeyboardEvent('keydown',{key:'Escape',bubbles:true,cancelable:true}));
assert.equal(modal().hidden,true,'Escape closes the wizard');
panel('peer-a').querySelector('.tg-add').click();
assert.deepEqual([...modal().querySelectorAll('.tg-choice')].map(b=>b.dataset.value),['serve','connect'],'remote nodes cannot propose');
modal().querySelector('.modal-close').click();assert.equal(modal().hidden,true);
// Node settings: switches in a modal, permissions one level deeper.
panel('self').querySelector('.tg-gear').click();
const broadcastSwitch=modal().querySelector('[role=switch]');broadcastSwitch.checked=true;broadcastSwitch.dispatchEvent(new window.Event('change'));await flush();
assert.deepEqual(calls.at(-1).args.action,{type:'broadcast',enabled:true});
assert.equal(modal().querySelector('[role=switch]').checked,true,'switch reflects the refreshed state');
data.graph.policy.broadcast=false;feature.render(structuredClone(data));
pick('permissions');
const grant=document.querySelector('.tg-grants input');grant.click();await flush();
assert.equal(calls.at(-1).args.action.permissions.edit_links,true);
assert.equal(calls.at(-1).args.action.permissions.add_forwards,false);
modal().querySelector('.modal-close').click();
failure='node owner has not granted permission';panel('peer-a').querySelector('.tg-remove').click();await flush();
assert.ok(document.querySelector('.tg-message').textContent.includes(failure),'backend denial stays visible');failure=null;
// Self node's legacy (non graph-managed) forward: × shows live stats, asks for confirmation, and removes via tunnel.forward.remove.
const statsNode=panel('self').querySelector('.tg-stats');
assert.ok(statsNode,'self forward rows show a live stats line');
assert.ok(statsNode.textContent.includes('listening')&&statsNode.textContent.includes('1')&&statsNode.textContent.includes('1.0 KB'),'stats show state, active connections and byte counters');
data.forwards[0].bytes_out=4096;data.forwards[0].bytes_in=8192;data.forwards[0].active_conns=3;
feature.render(structuredClone(data));
assert.equal(panel('self').querySelector('.tg-stats'),statsNode,'stats text updates in place without redrawing the card');
assert.ok(statsNode.textContent.includes('4.0 KB')&&statsNode.textContent.includes('8.0 KB')&&statsNode.textContent.includes('3'),'updated stats are reflected in place');
const callsBeforeDecline=calls.length;confirmAnswer=false;
panel('self').querySelector('.tg-remove').click();await flush();
assert.equal(calls.length,callsBeforeDecline,'declining the confirmation makes no backend call');
confirmAnswer=true;
panel('self').querySelector('.tg-remove').click();await flush();
assert.equal(calls.at(-1).cmd,'tunnel.forward.remove','legacy forwards are removed through tunnel.forward.remove');
assert.deepEqual(calls.at(-1).args,{target:'legacy-svc'});
assert.ok(confirmCalls.at(-1).includes('legacy-svc'),'confirmation text names the target');
// Propose dialog: listed first on the self node, auto-selects the sole peer, and shows the full peer id.
const proposeHost=document.createElement('div');document.body.append(proposeHost);
const longPeerId='peer-with-a-very-long-full-identifier-0123456789abcdef';
const proposeData={running:true,room:'propose-room',self_id:'self',forwards:[],graph:{policy:{broadcast:false,locked:false,links:[],permissions:{}},nodes:[
  {id:longPeerId,node:{broadcast:false,locked:false,links:[],permissions:{},forwards:[]}}
]}};
const proposeCalls=[];
const proposeFeature=window.MistlTunnelGraph.create({document,host:proposeHost,refresh:async()=>proposeFeature.render(structuredClone(proposeData)),confirm:()=>true,api:async(cmd,args)=>{
  if(cmd==='tunnel.port.check')return {available:true};
  proposeCalls.push({cmd,args:structuredClone(args)});
  if(cmd==='tunnel.forward.propose')return {message:'ok-from-server'};
  throw new Error('unexpected command '+cmd);
}});
proposeFeature.render(structuredClone(proposeData));
const proposeSelfCard=[...proposeHost.querySelectorAll('.tg-node')].find(n=>n.dataset.nodeId==='self');
proposeSelfCard.click();modal().querySelector('.tg-add').click();
const pModal=modal(document);
assert.equal(pModal.querySelector('.tg-choice').dataset.value,'propose','propose is the first type on the self node');
pick('propose',document);
assert.equal(step(document),'remote','the sole peer is auto-selected and the peer step is skipped');
const pRemote=pModal.querySelector('input[type=text]');pRemote.value='127.0.0.1:22';submitStep(document);
assert.equal(step(document),'local');await flush();submitStep(document);await flush();
assert.equal(step(document),'confirm');
assert.ok(pModal.querySelector('.tg-flow').textContent.includes(longPeerId),'confirm shows the full peer id, never truncated');
submitStep(document);await flush();
assert.deepEqual(proposeCalls.at(-1),{cmd:'tunnel.forward.propose',args:{proto:'tcp',local:'127.0.0.1:8080',remote:'127.0.0.1:22',peer_id:longPeerId}});
assert.equal(proposeHost.querySelector('.tg-message').textContent,'ok-from-server','server-provided message is shown on success');
assert.equal(pModal.hidden,true,'dialog closes after a successful proposal');
proposeHost.remove();
// Waiting for the other side: own proposals, parked connections, denials.
document.documentElement.lang='en';
const waitHost=document.createElement('div');document.body.append(waitHost);
const waitPeer='0123456789abcdef';
const waitData={running:true,room:'wait-room',self_id:'self',notices:[{timestamp_ms:1,kind:'info',text:'old, must not be replayed',code:''}],
  forwards:[{direction:'connect',proto:'tcp',addr:'127.0.0.1:3333',target:'tcp:127.0.0.1:22@'+waitPeer,key:'tcp:127.0.0.1:22@'+waitPeer,state:'listening',awaiting_approval:true,awaiting_since_ms:5,approval_peer_id:waitPeer}],
  pending_outgoing:[{req_id:'req-1',sent_at_ms:7,peer_id:waitPeer,proto:'tcp',local_addr:'127.0.0.1:2222',remote_addr:'127.0.0.1:22',target:'tcp:127.0.0.1:22@'+waitPeer}],
  graph:{policy:{broadcast:false,locked:false,links:[],permissions:{}},nodes:[{id:waitPeer,node:{broadcast:false,locked:false,links:[],permissions:{},forwards:[]}}]}};
const waitCalls=[],toasts=[];
window.MistlPeers={label:id=>id===waitPeer?'Alice':id.slice(0,8),byNode:id=>id===waitPeer?{node_id:id,name:'Alice'}:null,avatarEl:()=>document.createElement('span'),subscribe:()=>{}};
const waitFeature=window.MistlTunnelGraph.create({document,host:waitHost,toast:(text,kind)=>toasts.push([text,kind]),refresh:async()=>waitFeature.render(structuredClone(waitData)),confirm:()=>true,api:async(cmd,args)=>{waitCalls.push({cmd,args:structuredClone(args)});return {ok:true};}});
waitFeature.render(structuredClone(waitData));
const waitSelf=[...waitHost.querySelectorAll('.tg-node')].find(n=>n.dataset.nodeId==='self');
waitSelf.click();const waitModal=modal();
const outgoingRow=waitModal.querySelector('.tg-outgoing');
assert.ok(outgoingRow,'an unanswered proposal is a row on the self node card');
assert.ok(outgoingRow.textContent.includes('127.0.0.1:2222')&&outgoingRow.textContent.includes('Alice')&&outgoingRow.textContent.includes('127.0.0.1:22'),'the row shows local address, peer and remote address');
assert.equal(outgoingRow.querySelector('.tg-wait').textContent,'⏳ Waiting for Alice to accept');
assert.ok(outgoingRow.textContent.includes(waitPeer),'the full peer id stays next to the claimed name');
assert.equal(waitModal.querySelectorAll('.tg-wait').length,2,'the parked connect forward shows its own waiting pill');
assert.ok([...waitModal.querySelectorAll('.tg-wait')].some(e=>e.textContent==='⏳ Waiting for Alice to approve the connection'));
assert.equal(waitHost.querySelectorAll('.tg-wire.tg-awaiting').length,1,'the self-peer wire is styled as pending without any explicit link');
assert.equal([...waitHost.querySelectorAll('.tg-node')].find(n=>n.dataset.nodeId===waitPeer).querySelector('.topology-node-label').textContent,'Alice');
document.documentElement.lang='ja';waitFeature.render(structuredClone(waitData));
assert.equal(waitModal.querySelector('.tg-outgoing .tg-wait').textContent,'⏳ Alice の承認待ち','ja pill text');
document.documentElement.lang='en';waitFeature.render(structuredClone(waitData));
waitModal.querySelector('.tg-outgoing .tg-remove').click();await flush();
assert.deepEqual(waitCalls.at(-1),{cmd:'tunnel.forward.propose.cancel',args:{req_id:'req-1'}},'cancel withdraws the proposal by req_id');
// The answer arrives: denied -> a notice and a toast, never silent.
waitData.pending_outgoing=[];
waitData.notices=[...waitData.notices,{timestamp_ms:2,kind:'error',text:'peer denied tcp:127.0.0.1:22@'+waitPeer,code:'proposal_denied',peer_id:waitPeer,target:'tcp:127.0.0.1:22@'+waitPeer}];
waitFeature.render(structuredClone(waitData));
assert.equal(toasts.length,1,'old notices are not replayed, the new one is shown once');
assert.equal(toasts[0][1],'error');
assert.ok(toasts[0][0].includes('Alice')&&toasts[0][0].includes(waitPeer)&&toasts[0][0].includes('denied'),'the toast names the peer and says it was denied');
assert.ok(waitHost.querySelector('.tg-message').classList.contains('tg-error'));
waitFeature.render(structuredClone(waitData));assert.equal(toasts.length,1,'polling again does not repeat the toast');
waitData.notices=[...waitData.notices,{timestamp_ms:3,kind:'info',text:'forward established: x',code:'proposal_accepted',peer_id:waitPeer,target:'tcp:127.0.0.1:22@'+waitPeer,detail:'127.0.0.1:2222'}];
waitFeature.render(structuredClone(waitData));
assert.equal(toasts.length,2);assert.equal(toasts[1][1],'success');assert.ok(toasts[1][0].includes('accepted'));
// A denied connection leaves a marker on the forward.
waitData.forwards[0]={...waitData.forwards[0],awaiting_approval:false,denied_at_ms:9};
waitFeature.render(structuredClone(waitData));
assert.ok([...waitModal.querySelectorAll('.tg-denied')].some(e=>e.textContent==='✕ Alice denied the connection'));
assert.equal(waitHost.querySelectorAll('.tg-wire.tg-awaiting').length,0,'nothing is waiting any more, so the wire is no longer pending');
waitModal.querySelector('.modal-close').click();
delete window.MistlPeers;waitHost.remove();document.documentElement.lang='ja';
settle();
const svg=document.querySelector('.tg-graph'),self=card('self');
const beforeMove=self.getAttribute('transform');
self.focus();self.dispatchEvent(new window.KeyboardEvent('keydown',{key:'ArrowRight',bubbles:true,cancelable:true}));
settle();assert.notEqual(self.getAttribute('transform'),beforeMove,'keyboard moves a node');
const moved=self.getAttribute('transform');
feature.render(structuredClone(data));settle();assert.equal(self.getAttribute('transform'),moved,'dragged nodes stay pinned across polls');
feature.render({...structuredClone(data),running:false});
assert.equal(frames.size,0,'stopped graph does not animate');
feature.render(structuredClone(data));settle();assert.equal(document.getElementById('graph').querySelectorAll('.tg-node').length,4,'restart restores graph');
document.documentElement.lang='en';feature.render(structuredClone(data));
assert.equal(document.querySelector('.tg-toolbar'),null,'no explanatory toolbar');
assert.equal(document.querySelector('.tunnel-graph > .tg-help'),null);
feature.render({...structuredClone(data),room:'another-room'});settle();
assert.notEqual(card('self').getAttribute('transform'),moved,'room changes reset positions');
panel('self').querySelector('.tg-add').click();feature.render({...structuredClone(data),room:'third-room'});
assert.equal(document.querySelector('.tg-modal').hidden,true,'room changes close stale editors');
settle();
// Shared graph interactions operate on the same incremental SVG nodes.
feature.render(structuredClone(data));settle();
const viewport=svg.querySelector('.topology-viewport');
const pointer=(target,type,id,x,y)=>target.dispatchEvent(new window.PointerEvent(type,{pointerId:id,button:0,clientX:x,clientY:y,bubbles:true,cancelable:true}));
const cameraBefore=viewport.getAttribute('transform');
svg.dispatchEvent(new window.WheelEvent('wheel',{deltaY:180,clientX:200,clientY:150,bubbles:true,cancelable:true}));
assert.notEqual(viewport.getAttribute('transform'),cameraBefore,'wheel zooms');
pointer(svg,'pointerdown',1,20,20);pointer(svg,'pointermove',1,80,70);pointer(svg,'pointerup',1,80,70);
const panned=viewport.getAttribute('transform');
pointer(svg,'pointerdown',2,100,100);pointer(svg,'pointerdown',3,200,100);pointer(svg,'pointermove',3,300,100);
assert.notEqual(viewport.getAttribute('transform'),panned,'pinch zooms');
pointer(svg,'pointerup',2,100,100);pointer(svg,'pointerup',3,300,100);
svg.dispatchEvent(new window.KeyboardEvent('keydown',{key:'Home',bubbles:true,cancelable:true}));
assert.equal(viewport.getAttribute('transform'),cameraBefore,'Home fits');
svg.dispatchEvent(new window.WheelEvent('wheel',{deltaY:100,clientX:100,clientY:100,cancelable:true}));
document.querySelector('.tg-fit').click();assert.equal(viewport.getAttribute('transform'),cameraBefore,'fit button fits');
const dragNode=card('peer-a'),position=dragNode.getAttribute('transform');
pointer(dragNode,'pointerdown',4,180,160);pointer(svg,'pointermove',4,310,240);advance();
assert.notEqual(dragNode.getAttribute('transform'),position,'body drag moves the node');
pointer(svg,'pointerup',4,310,240);dragNode.click();assert.equal(document.querySelector('.tg-modal').hidden,true,'dragging does not open a dialog');
settle();const pinned=dragNode.getAttribute('transform');
feature.render(structuredClone(data));settle();assert.equal(dragNode.getAttribute('transform'),pinned,'body drag pins its position');
const stableSelf=card('self');feature.render(structuredClone(data));
assert.equal(card('self'),stableSelf);assert.equal(frames.size,0,'unchanged poll preserves DOM and stays cooled');
// Keyboard linking, Escape, and dragging a handle preserve the existing permission checks.
stableSelf.dispatchEvent(new window.KeyboardEvent('keydown',{key:'l',bubbles:true}));
card('peer-a').dispatchEvent(new window.KeyboardEvent('keydown',{key:'Enter',bubbles:true,cancelable:true}));await flush();
assert.ok(data.graph.policy.links.includes('peer-a'),'L then Enter links nodes');
stableSelf.dispatchEvent(new window.KeyboardEvent('keydown',{key:'l',bubbles:true}));
document.dispatchEvent(new window.KeyboardEvent('keydown',{key:'Escape',bubbles:true}));
assert.equal(stableSelf.classList.contains('tg-connecting'),false,'Escape cancels linking');
const handle=stableSelf.querySelector('.tg-link-handle'),worldLayer=svg.querySelector('.topology-viewport');
pointer(handle,'pointerdown',5,50,50);pointer(document,'pointermove',5,170,180);
assert.ok(worldLayer.querySelector('.tg-rubber'),'link preview lives in world coordinates');
const originalElementFromPoint=document.elementFromPoint.bind(document);
document.elementFromPoint=()=>card('peer-a');
data.graph.policy.links=[];data.graph.nodes[0].node.links=data.graph.nodes[0].node.links.filter(id=>id!=='self');
feature.render(structuredClone(data));
const beforeHandleCalls=calls.length;
pointer(document,'pointerup',5,170,180);await flush();
assert.equal(worldLayer.querySelector('.tg-rubber'),null);
assert.equal(calls.length,beforeHandleCalls+2,'handle drag links both endpoints');
document.elementFromPoint=originalElementFromPoint;settle();advance(25);
// Opening a legacy node exposes identity, never actions.
card('legacy<img src=x onerror=alert(1)>').click();
assert.equal(document.querySelector('.tg-modal').querySelector('.tg-add'),null);
assert.equal(document.querySelector('.tg-modal').querySelector('.tg-link'),null);
document.querySelector('.tg-modal .modal-close').click();
// Counter deltas use peer metrics first, then scoped targets / approval_peer_id.
const f=data.forwards[0];f.peers=[{peer_id:'peer-a',bytes_in:0,bytes_out:0,active_conns:0}];
feature.render(structuredClone(data));settle();
for(let i=0;i<7;i++){
  f.peers[0].bytes_out+=1048576;f.bytes_out+=1048576;
  feature.render(structuredClone(data));
  assert.ok(svg.querySelector('.topology-particle'),'each traffic snapshot replenishes the particle budget');settle();
}
feature.render(structuredClone(data));assert.equal(frames.size,0,'unchanged counters produce no activity');
f.peers[0].bytes_out=0;feature.render(structuredClone(data));assert.equal(frames.size,0,'counter rollback silently reseeds');
const con={direction:'connect',proto:'tcp',addr:'127.0.0.1:5000',target:'svc@peer-a',bytes_in:0,bytes_out:0};
data.forwards.push(con);feature.render(structuredClone(data));settle();
con.bytes_in=1024;feature.render(structuredClone(data));assert.ok(svg.querySelector('.topology-particle'),'scoped connect target identifies remote peer');settle();
con.target='unscoped';con.approval_peer_id='peer-a';feature.render(structuredClone(data));settle();
con.bytes_out=1024;feature.render(structuredClone(data));assert.ok(svg.querySelector('.topology-particle'),'approval notice identifies remote peer');settle();
con.target='unknown';delete con.approval_peer_id;feature.render(structuredClone(data));settle();
con.bytes_out+=1024;feature.render(structuredClone(data));assert.equal(frames.size,0,'unknown peer does not invent a path');
con.target='svc@peer-a';feature.render(structuredClone(data));settle();
const openPanel=panel('self'),liveStats=openPanel.querySelector('.tg-stats'),removeFocus=openPanel.querySelector('.tg-remove');
removeFocus.focus();f.bytes_in+=512;f.active_conns++;
feature.render(structuredClone(data));
assert.equal(openPanel.querySelector('.tg-stats'),liveStats);assert.equal(document.activeElement,removeFocus,'live stats preserve focused action');
openPanel.querySelector('.modal-close').click();
motion.matches=true;motion.dispatchEvent(new window.Event('change'));settle();
con.bytes_out+=1024;feature.render(structuredClone(data));
assert.equal(svg.querySelectorAll('.topology-particle').length,0);
assert.ok(card('peer-a').classList.contains('has-activity'),'reduced motion uses a static highlight');
feature.render(structuredClone(data));assert.equal(card('peer-a').classList.contains('has-activity'),false,'quiet poll clears static activity');
motion.matches=false;motion.dispatchEvent(new window.Event('change'));settle();
con.bytes_out+=1024;feature.render(structuredClone(data));assert.ok(frames.size);
intersections[0]([{isIntersecting:false}]);assert.equal(frames.size,0);assert.equal(svg.querySelectorAll('.topology-particle').length,0,'offscreen drops traffic animations');
intersections[0]([{isIntersecting:true}]);settle();
Object.defineProperty(document,'hidden',{configurable:true,value:true});document.dispatchEvent(new window.Event('visibilitychange'));
con.bytes_out+=1024;feature.render(structuredClone(data));assert.equal(frames.size,0,'hidden tab requests no frames');
Object.defineProperty(document,'hidden',{configurable:true,value:false});document.dispatchEvent(new window.Event('visibilitychange'));settle();
// Nodes animate out while focus falls back to the graph after removal.
const leaving=card('peer-b');leaving.focus();data.graph.nodes=data.graph.nodes.filter(n=>n.id!=='peer-b');
feature.render(structuredClone(data));assert.equal(card('peer-b'),leaving,'exit is incremental');
settle();assert.equal(card('peer-b'),undefined);assert.equal(document.activeElement,svg);
// Port mappings remain visible without explicit links or broadcast permissions.
document.documentElement.lang='en';
const portsHost=document.createElement('div');document.body.append(portsHost);
const portsData={running:true,room:'ports-room',self_id:'self',forwards:[
  {direction:'connect',proto:'tcp',addr:'127.0.0.1:2222',target:'tcp:127.0.0.1:22@peer-a'},
  {direction:'serve',proto:'tcp',addr:'localhost:22',target:'graph:ssh@self',peers:[{peer_id:'peer-c',active_conns:0,bytes_in:0,bytes_out:0}]},
  {direction:'serve',proto:'udp',addr:'[::1]:5353',target:'graph:dns@self'},
  {direction:'serve',proto:'tcp',addr:'0.0.0.0:80',target:'graph:web@self'},
  {direction:'serve',proto:'tcp',addr:'192.168.1.5:443',target:'graph:lan@self'}
],pending_outgoing:[{req_id:'pending-port',peer_id:'peer-b',proto:'tcp',local_addr:'0.0.0.0:8080',remote_addr:'192.168.1.5:80'}],
graph:{policy:{broadcast:false,locked:false,links:[],permissions:{}},nodes:[
  {id:'peer-a',node:{broadcast:false,locked:false,links:[],permissions:{},forwards:[
    {direction:'connect',proto:'udp',addr:'[::1]:9000',target:'localhost:5353@self'},
    {direction:'serve',proto:'tcp',addr:'127.1.2.3:22',target:'graph:ssh@peer-a'},
    {direction:'serve',proto:'udp',addr:'192.168.1.5:80',target:'graph:lan@peer-a'}
  ]}},
  {id:'peer-b',node:null},
  {id:'peer-c',node:{broadcast:false,locked:false,links:[],permissions:{},forwards:[]}}
]}};
const portsFeature=window.MistlTunnelGraph.create({document,host:portsHost,refresh:async()=>{},api:async()=>{throw new Error('unexpected port label action');}});
const renderPorts=()=>portsFeature.render(structuredClone(portsData));
const portNode=id=>[...portsHost.querySelectorAll('.tg-node')].find(n=>n.dataset.nodeId===id);
const edgeLabel=peer=>[...portsHost.querySelectorAll('.tg-port-label')].find(n=>n.textContent.includes(peer));
const portTexts=parent=>[...parent.querySelectorAll('text')].map(n=>n.textContent);
renderPorts();advance();
assert.equal(portsHost.querySelectorAll('.tg-wire').length,2,'mappings alone create edges, including to a legacy peer');
const portLabel=edgeLabel('peer-a');
assert.deepEqual(portTexts(portLabel),['UDP peer-a :9000 → This node :5353','TCP This node :2222 → peer-a :22'],'self and remote connect mappings identify both ends and listening direction');
const pendingLabel=edgeLabel('peer-b');
assert.deepEqual(portTexts(pendingLabel),['⏳ TCP This node :8080 → peer-b 192.168.1.5:80']);
assert.ok(pendingLabel.querySelector('text').classList.contains('tg-port-wait'),'pending mappings use warning styling');
assert.deepEqual(portTexts(portNode('self').querySelector('.tg-services')),['↑ TCP :22','↑ UDP :5353','↑ TCP :80','+1'],'published service lines elide local hosts and cap at three');
assert.deepEqual(portTexts(portNode('peer-a').querySelector('.tg-services')),['↑ TCP :22','↑ UDP 192.168.1.5:80'],'remote services elide 127/8 and retain LAN hosts');
const portWire=[...portsHost.querySelectorAll('.tg-wire')].find(n=>n.getAttribute('aria-label').includes('peer-a'));
const assertMidpoint=()=>{
  const x=(Number(portWire.getAttribute('x1'))+Number(portWire.getAttribute('x2')))/2;
  const y=(Number(portWire.getAttribute('y1'))+Number(portWire.getAttribute('y2')))/2;
  assert.equal(portLabel.getAttribute('transform'),`translate(${x},${y})`,'port label follows the edge midpoint');
};
assertMidpoint();const firstMidpoint=portLabel.getAttribute('transform');advance();assertMidpoint();
assert.notEqual(portLabel.getAttribute('transform'),firstMidpoint,'a physics frame moves the port label');settle();
const stablePortText=portLabel.querySelector('text'),stableService=portNode('self').querySelector('.tg-services text');
const portObserver=new window.MutationObserver(()=>{});
portObserver.observe(portLabel,{subtree:true,childList:true,characterData:true});
portObserver.observe(portNode('self').querySelector('.tg-services'),{subtree:true,childList:true,characterData:true});
renderPorts();
assert.equal(portObserver.takeRecords().length,0,'identical polls do not rewrite label text');
assert.equal(portLabel.querySelector('text'),stablePortText);assert.equal(portNode('self').querySelector('.tg-services text'),stableService);
assert.equal(frames.size,0,'unchanged mappings keep the graph cooled');
portsData.graph.nodes[0].node.locked=true;renderPorts();
assert.equal(portObserver.takeRecords().length,0,'unrelated node changes preserve port text too');portObserver.disconnect();
portsData.forwards[0].target='graph:ssh@peer-a';renderPorts();
assert.deepEqual(portTexts(edgeLabel('peer-a')),['UDP peer-a :9000 → This node :5353','TCP This node :2222 → peer-a :22'],'opaque service keys resolve through published services');
portsData.forwards.push(
  {direction:'connect',proto:'udp',addr:'0.0.0.0:8081',target:'udp:192.168.1.5:80@peer-a'},
  {direction:'connect',proto:'tcp',addr:'::1:8443',target:'[2001:db8::5]:443@peer-a'},
  {direction:'connect',proto:'tcp',addr:'localhost:8022',target:'tcp:22@peer-a'}
);renderPorts();
const cappedLabel=edgeLabel('peer-a');
assert.equal(portTexts(cappedLabel).length,4);assert.equal(portTexts(cappedLabel).at(-1),'+2','edge labels cap at three plus the remaining count');
assert.ok(cappedLabel.querySelector('title').textContent.includes('UDP This node :8081 → peer-a 192.168.1.5:80'),'full label retains non-loopback target hosts');
assert.ok(cappedLabel.querySelector('title').textContent.includes('TCP This node :8443 → peer-a [2001:db8::5]:443'),'IPv6 target hosts stay bracketed');
assert.ok(cappedLabel.querySelector('title').textContent.includes('TCP This node :8022 → peer-a :22'),'port-only protocol targets work');
portNode('self').click();
assert.equal(modal().querySelectorAll('.tg-forward').length,portsData.forwards.length+1,'the node dialog keeps the full forwards and pending proposal list');
modal().querySelector('.modal-close').click();
const metrics=portsData.forwards[1].peers[0];metrics.active_conns=1;renderPorts();settle();
const liveServe=edgeLabel('peer-c');assert.ok(liveServe,'active serve peer metrics create a mapping edge');
assert.deepEqual(portTexts(liveServe),['TCP peer-c → This node :22'],'serve metrics do not invent a remote listen port');
const stableLiveText=liveServe.querySelector('text');metrics.active_conns=2;metrics.bytes_in=1024;renderPorts();settle();
assert.equal(edgeLabel('peer-c').querySelector('text'),stableLiveText,'live counts do not rebuild mapping text');
metrics.active_conns=0;renderPorts();settle();assert.equal(edgeLabel('peer-c'),undefined,'inactive serve peers lose their mapping-only edge');
const portsSvg=portsHost.querySelector('svg');
portsSvg.dispatchEvent(new window.WheelEvent('wheel',{deltaY:1600,clientX:200,clientY:150,bubbles:true,cancelable:true}));
assert.ok(portsSvg.classList.contains('is-zoomed-out'));
portNode('peer-a').dispatchEvent(new window.MouseEvent('mouseenter'));
assert.ok(edgeLabel('peer-a').classList.contains('is-highlighted'),'highlighting exposes the adjacent port labels at low zoom');
assert.equal(edgeLabel('peer-b').classList.contains('is-highlighted'),false);
portNode('self').click();assert.ok(edgeLabel('peer-b').classList.contains('is-highlighted'),'selected nodes also highlight their mapping labels');
modal().querySelector('.modal-close').click();
portsData.pending_outgoing=[];renderPorts();assert.equal(edgeLabel('peer-b'),undefined,'cancelled proposals remove mapping-only labels');
portsFeature.render({...portsData,running:false});assert.equal(portsHost.querySelectorAll('.tg-port-label').length,0,'reset removes edge labels');
portsHost.remove();settle();
// Maximize retains the live graph and consumes one Escape action at a time.
feature.render(structuredClone(data));settle();
const maxHost=document.getElementById('graph'),maxButton=maxHost.querySelector('.tg-maximize');
const parentBefore=maxHost.parentNode,nextBefore=maxHost.nextSibling;
const escapeMax=target=>target.dispatchEvent(new window.KeyboardEvent('keydown',{key:'Escape',bubbles:true,cancelable:true}));
svg.getBoundingClientRect=()=>({left:0,top:0,width:maxHost.classList.contains('graph-maximized')?1200:640,height:maxHost.classList.contains('graph-maximized')?800:380});
const maxScale=()=>Number(viewport.getAttribute('transform').match(/scale\(([^)]+)\)/)[1]);
document.body.style.overflow='auto';maxButton.click();
assert.ok(maxHost.classList.contains('graph-maximized'));assert.equal(maxButton.getAttribute('aria-pressed'),'true');
assert.equal(maxButton.title,'Restore');assert.equal(maxButton.getAttribute('aria-label'),'Restore');
assert.equal(document.activeElement,svg);assert.equal(document.body.style.overflow,'hidden');
card('self').dispatchEvent(new window.KeyboardEvent('keydown',{key:'l',bubbles:true}));
maxHost.querySelector('.tg-wire').dispatchEvent(new window.KeyboardEvent('keydown',{key:'Enter',bubbles:true}));
assert.equal(document.querySelector('.tg-modal').hidden,false);
assert.ok(card('self').classList.contains('tg-connecting'));
let otherEscapes=0;const otherEscape=e=>{if(e.key==='Escape')otherEscapes++;};document.addEventListener('keydown',otherEscape);
escapeMax(document.querySelector('.tg-modal .modal-close'));
assert.equal(document.querySelector('.tg-modal').hidden,true,'first Escape closes the editor');
assert.ok(card('self').classList.contains('tg-connecting'),'closing a modal preserves the pending link');
assert.ok(maxHost.classList.contains('graph-maximized'));
escapeMax(svg);
assert.equal(card('self').classList.contains('tg-connecting'),false,'second Escape cancels linking');
assert.ok(maxHost.classList.contains('graph-maximized'));
escapeMax(svg);
assert.equal(maxHost.classList.contains('graph-maximized'),false,'third Escape restores the graph');
assert.equal(maxButton.getAttribute('aria-pressed'),'false');assert.equal(document.activeElement,maxButton);
assert.equal(document.body.style.overflow,'auto');assert.equal(maxHost.parentNode,parentBefore);assert.equal(maxHost.nextSibling,nextBefore);
assert.equal(otherEscapes,0,'each Escape is consumed without triggering another dashboard handler');document.removeEventListener('keydown',otherEscape);
const singleNode={...structuredClone(data),forwards:[],pending_outgoing:[],graph:{policy:{...data.graph.policy,links:[]},nodes:[]}};
feature.render(singleNode);settle();maxHost.querySelector('.tg-fit').click();assert.equal(maxScale(),2.2);
svg.dispatchEvent(new window.WheelEvent('wheel',{deltaY:1800,clientX:200,clientY:150,cancelable:true}));
intersections[0]([{isIntersecting:false}]);
maxButton.click();assert.equal(maxScale(),4,'maximize clears manual view and raises the tunnel fit cap');
assert.equal(svg.getAttribute('viewBox'),'0 0 1200 800');
intersections[0]([{isIntersecting:false}]);assert.equal(maxHost.querySelector('.tg-viewport').classList.contains('is-paused'),false,'maximized graph ignores stale offscreen observation');
singleNode.forwards=[{direction:'connect',addr:'127.0.0.1:2222',target:'svc@self'}];feature.render(singleNode);
assert.equal(maxButton.getAttribute('aria-pressed'),'true','polling keeps maximization');
document.documentElement.lang='zh';feature.render(singleNode);assert.equal(maxButton.title,'还原');
maxButton.click();assert.equal(maxScale(),2.2,'restore resets the normal tunnel fit cap');assert.equal(maxButton.title,'最大化');
document.documentElement.lang='ja';feature.render(singleNode);assert.equal(maxButton.title,'最大化');
maxButton.click();assert.equal(maxButton.title,'元に戻す');
feature.render({...singleNode,running:false});
assert.equal(maxButton.getAttribute('aria-pressed'),'false','stopping restores the graph');assert.equal(document.body.style.overflow,'auto');
await window.happyDOM.close();
console.log('PASS: incremental force graph, cooled/hidden/reduced-motion animation, pan/zoom/pinch/fit, pinned drag, keyboard/handle linking, traffic deltas, forward wizard, permissions, confirmation, live stats/focus, room isolation, literal content, approval/cancel/notices, port mappings/services/caps/midpoints/stable polling, maximize/restore, fit caps, modal/link/restore Escape order, focus/scroll return');
