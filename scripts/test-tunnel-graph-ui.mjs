import assert from 'node:assert/strict';
import fs from 'node:fs';
import {Window} from 'happy-dom';

const window = new Window({url:'http://localhost/'});
const {document} = window;
document.documentElement.lang='ja';
document.body.innerHTML='<div id="graph"></div>';
window.eval(fs.readFileSync(new URL('../src/web/assets/features/tunnel-graph.js',import.meta.url),'utf8'));
const rights={edit_links:true,add_forwards:true,remove_forwards:true};
const data={running:true,room:'graph-test',self_id:'self',forwards:[],graph:{policy:{broadcast:false,locked:false,links:[],permissions:{}},nodes:[
  {id:'peer-a',node:{broadcast:false,locked:false,links:[],permissions:{...rights},forwards:[{direction:'serve',proto:'tcp',addr:'127.0.0.1:3000',target:'service@peer-a',state:'listening',graph_managed:true}]}},
  {id:'peer-b',node:{broadcast:false,locked:false,links:[],permissions:{...rights},forwards:[]}},
  {id:'legacy<img src=x onerror=alert(1)>',node:null}
]}};
const calls=[];let failure=null;
const get=(id)=>id==='self'?{...data.graph.policy,forwards:data.forwards}:data.graph.nodes.find(n=>n.id===id).node;
const feature=window.MistlTunnelGraph.create({document,host:document.getElementById('graph'),refresh:async()=>feature.render(structuredClone(data)),api:async(cmd,args)=>{
  calls.push({cmd,args:structuredClone(args)});if(failure)throw new Error(failure);
  assert.equal(cmd,'tunnel.graph.command');assert.equal(args.room,data.room);
  const action=args.action,node=get(args.node_id);
  if(action.type==='link'){node.links=node.links.filter(id=>id!==action.peer_id);if(action.connected)node.links.push(action.peer_id);if(args.node_id==='self')data.graph.policy.links=node.links;}
  if(action.type==='broadcast')data.graph.policy.broadcast=action.enabled;
  if(action.type==='lock')data.graph.policy.locked=action.locked;
  if(action.type==='permission')data.graph.policy.permissions[action.peer_id]=action.permissions;
  return {applied:true};
}});
const flush=async()=>{for(let i=0;i<10;i++)await new Promise(r=>setImmediate(r));};
const card=id=>[...document.querySelectorAll('.tg-node')].find(n=>n.dataset.nodeId===id);
feature.render(structuredClone(data));
assert.equal(document.querySelectorAll('.tg-node').length,4);
assert.equal(document.querySelector('img'),null,'untrusted IDs are literal text');
assert.equal(card('peer-a').querySelector('[role=switch]').disabled,true,'only owner changes broadcast and lock');
card('self').querySelector('.tg-port').click();card('peer-a').querySelector('.tg-port').click();await flush();
assert.deepEqual(calls.slice(-2).map(c=>[c.args.node_id,c.args.action.peer_id]),[['self','peer-a'],['peer-a','self']]);
assert.equal(document.querySelectorAll('.tg-direct').length,1);
document.querySelector('.tg-direct').dispatchEvent(new window.KeyboardEvent('keydown',{key:'Enter'}));
document.querySelector('.tg-detail .danger').click();await flush();
assert.equal(document.querySelectorAll('.tg-direct').length,0,'remove link updates both endpoints');
// A third node can wire two remote nodes when both owners delegated link edits.
card('peer-a').querySelector('.tg-port').click();card('peer-b').querySelector('.tg-port').click();await flush();
assert.deepEqual(calls.slice(-2).map(c=>c.args.node_id),['peer-a','peer-b']);
data.graph.nodes[1].node.locked=true;feature.render(structuredClone(data));
const before=calls.length;card('self').querySelector('.tg-port').click();card('peer-b').querySelector('.tg-port').click();await flush();
assert.equal(calls.length,before,'locked remote denies edits before either side changes');
assert.ok(document.querySelector('.tg-message').classList.contains('tg-error'));
card('self').querySelector('.tg-add').click();
const form=document.querySelector('.tg-detail form'),inputs=form.querySelectorAll('input');inputs[0].value='127.0.0.1:9000';
feature.render(structuredClone(data));assert.equal(inputs[0].value,'127.0.0.1:9000','polling preserves draft');
form.dispatchEvent(new window.Event('submit',{bubbles:true,cancelable:true}));await flush();
assert.deepEqual(calls.at(-1).args.action,{type:'add_forward',direction:'serve',proto:'tcp',addr:'127.0.0.1:9000',target:''});
card('self').querySelector('.tg-add').click();
const form2=document.querySelector('.tg-detail form'),direction=form2.querySelectorAll('select')[0];direction.value='connect';direction.dispatchEvent(new window.Event('change'));
const services=form2.querySelectorAll('select')[2];assert.ok([...services.options].some(o=>o.value==='service@peer-a'));
services.value='service@peer-a';form2.dispatchEvent(new window.Event('submit',{cancelable:true}));await flush();
assert.equal(calls.at(-1).args.action.target,'service@peer-a');
card('self').querySelector('.tg-permissions').click();
const grant=document.querySelector('.tg-grants input');grant.click();await flush();
assert.equal(calls.at(-1).args.action.permissions.edit_links,true);
assert.equal(calls.at(-1).args.action.permissions.add_forwards,false);
failure='node owner has not granted permission';card('peer-a').querySelector('.tg-remove').click();await flush();
assert.ok(document.querySelector('.tg-message').textContent.includes(failure),'backend denial stays visible');failure=null;
const head=card('self').querySelector('.tg-node-head');head.focus();head.dispatchEvent(new window.KeyboardEvent('keydown',{key:'ArrowRight',cancelable:true}));
assert.equal(card('self').style.left,'50px','keyboard can rearrange nodes');
assert.equal(document.activeElement.className,'tg-node-head');
const saved=window.localStorage.getItem('mistl-tunnel-layout:graph-test');assert.ok(saved.includes('50'));
feature.render({...structuredClone(data),running:false});assert.equal(document.querySelectorAll('.tg-node').length,0);
feature.render(structuredClone(data));assert.equal(document.querySelectorAll('.tg-node').length,4,'restart restores identical graph');
document.documentElement.lang='en';feature.render(structuredClone(data));assert.equal(document.querySelector('.tg-toolbar h3').textContent,'Build your connections');
feature.render({...structuredClone(data),room:'another-room'});assert.equal(card('self').style.left,'30px','layout is room-scoped');
assert.equal(document.querySelector('.tg-detail').hidden,true,'room changes close stale editors');
await window.happyDOM.close();
console.log('PASS: graph wiring, remote-to-remote edits, permission/lock gates, forwarding forms, draft preservation, room isolation, restart, layout keyboard, localization and literal content');
