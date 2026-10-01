import assert from 'node:assert/strict';
import fs from 'node:fs';
import {Window} from 'happy-dom';

const window = new Window({url:'http://localhost/'});
const {document} = window;
document.documentElement.lang='ja';
document.body.innerHTML='<div id="graph"></div>';
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
const card=id=>[...document.querySelectorAll('.tg-node')].find(n=>n.dataset.nodeId===id);
feature.render(structuredClone(data));
assert.equal(document.querySelectorAll('.tg-node').length,4);
assert.equal(document.querySelector('img'),null,'untrusted IDs are literal text');
assert.equal(card('peer-a').querySelector('[role=switch]'),null,'remote cards show no switches; only the owner changes broadcast and lock');
assert.equal(card('peer-a').querySelector('.tg-gear'),null,'only the self card has a settings gear');
card('self').querySelector('.tg-port').click();card('peer-a').querySelector('.tg-port').click();await flush();
assert.deepEqual(calls.slice(-2).map(c=>[c.args.node_id,c.args.action.peer_id]),[['self','peer-a'],['peer-a','self']]);
assert.equal(document.querySelectorAll('.tg-direct').length,1);
document.querySelector('.tg-direct').dispatchEvent(new window.KeyboardEvent('keydown',{key:'Enter'}));
assert.equal(modal().hidden,false,'link details open in a modal');
modal().querySelector('.danger').click();await flush();
assert.equal(modal().hidden,true,'the link editor closes once an action is chosen');
assert.equal(document.querySelectorAll('.tg-direct').length,0,'remove link updates both endpoints');
// A third node can wire two remote nodes when both owners delegated link edits.
card('peer-a').querySelector('.tg-port').click();card('peer-b').querySelector('.tg-port').click();await flush();
assert.deepEqual(calls.slice(-2).map(c=>c.args.node_id),['peer-a','peer-b']);
data.graph.nodes[1].node.locked=true;feature.render(structuredClone(data));
const before=calls.length;card('self').querySelector('.tg-port').click();card('peer-b').querySelector('.tg-port').click();await flush();
assert.equal(calls.length,before,'locked remote denies edits before either side changes');
assert.ok(document.querySelector('.tg-message').classList.contains('tg-error'));
// Forward wizard: one question per step in a modal.
card('self').querySelector('.tg-add').click();
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
card('self').querySelector('.tg-add').click();pick('connect');assert.equal(step(),'service');
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
card('self').querySelector('.tg-add').click();
modal().dispatchEvent(new window.KeyboardEvent('keydown',{key:'Escape',bubbles:true,cancelable:true}));
assert.equal(modal().hidden,true,'Escape closes the wizard');
card('peer-a').querySelector('.tg-add').click();
assert.deepEqual([...modal().querySelectorAll('.tg-choice')].map(b=>b.dataset.value),['serve','connect'],'remote nodes cannot propose');
modal().querySelector('.modal-close').click();assert.equal(modal().hidden,true);
// Node settings: switches in a modal, permissions one level deeper.
card('self').querySelector('.tg-gear').click();
const broadcastSwitch=modal().querySelector('[role=switch]');broadcastSwitch.checked=true;broadcastSwitch.dispatchEvent(new window.Event('change'));await flush();
assert.deepEqual(calls.at(-1).args.action,{type:'broadcast',enabled:true});
assert.equal(modal().querySelector('[role=switch]').checked,true,'switch reflects the refreshed state');
data.graph.policy.broadcast=false;feature.render(structuredClone(data));
pick('permissions');
const grant=document.querySelector('.tg-grants input');grant.click();await flush();
assert.equal(calls.at(-1).args.action.permissions.edit_links,true);
assert.equal(calls.at(-1).args.action.permissions.add_forwards,false);
modal().querySelector('.modal-close').click();
failure='node owner has not granted permission';card('peer-a').querySelector('.tg-remove').click();await flush();
assert.ok(document.querySelector('.tg-message').textContent.includes(failure),'backend denial stays visible');failure=null;
// Self node's legacy (non graph-managed) forward: × shows live stats, asks for confirmation, and removes via tunnel.forward.remove.
const statsNode=card('self').querySelector('.tg-stats');
assert.ok(statsNode,'self forward rows show a live stats line');
assert.ok(statsNode.textContent.includes('listening')&&statsNode.textContent.includes('1')&&statsNode.textContent.includes('1.0 KB'),'stats show state, active connections and byte counters');
data.forwards[0].bytes_out=4096;data.forwards[0].bytes_in=8192;data.forwards[0].active_conns=3;
feature.render(structuredClone(data));
assert.equal(card('self').querySelector('.tg-stats'),statsNode,'stats text updates in place without redrawing the card');
assert.ok(statsNode.textContent.includes('4.0 KB')&&statsNode.textContent.includes('8.0 KB')&&statsNode.textContent.includes('3'),'updated stats are reflected in place');
const callsBeforeDecline=calls.length;confirmAnswer=false;
card('self').querySelector('.tg-remove').click();await flush();
assert.equal(calls.length,callsBeforeDecline,'declining the confirmation makes no backend call');
confirmAnswer=true;
card('self').querySelector('.tg-remove').click();await flush();
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
proposeSelfCard.querySelector('.tg-add').click();
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
const head=card('self').querySelector('.tg-node-head');head.focus();head.dispatchEvent(new window.KeyboardEvent('keydown',{key:'ArrowRight',cancelable:true}));
assert.equal(card('self').style.left,'50px','keyboard can rearrange nodes');
assert.equal(document.activeElement.className,'tg-node-head');
const saved=window.localStorage.getItem('mistl-tunnel-layout:graph-test');assert.ok(saved.includes('50'));
feature.render({...structuredClone(data),running:false});assert.equal(document.querySelectorAll('.tg-node').length,0);
feature.render(structuredClone(data));assert.equal(document.querySelectorAll('.tg-node').length,4,'restart restores identical graph');
document.documentElement.lang='en';feature.render(structuredClone(data));assert.equal(document.querySelector('.tg-toolbar h3').textContent,'Build your connections');
feature.render({...structuredClone(data),room:'another-room'});assert.equal(card('self').style.left,'30px','layout is room-scoped');
card('self').querySelector('.tg-add').click();feature.render({...structuredClone(data),room:'third-room'});
assert.equal([...document.querySelectorAll('.tg-modal')][0].hidden,true,'room changes close stale editors');
await window.happyDOM.close();
console.log('PASS: graph wiring, forward wizard steps, remote-to-remote edits, permission/lock gates, forwarding forms, draft preservation, room isolation, restart, layout keyboard, localization and literal content');
