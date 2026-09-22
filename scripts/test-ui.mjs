import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
const html = fs.readFileSync(new URL('../src/web/assets/index.html', import.meta.url), 'utf8');
for (const [, script] of html.matchAll(/<script\b[^>]*>([\s\S]*?)<\/script>/g)) new vm.Script(script);
const ids = [...html.matchAll(/\bid="([^"]+)"/g)].map(m => m[1]);
assert.equal(new Set(ids).size, ids.length, 'HTML IDs must be unique');
class Element {
  constructor(tag, attrs = {}) { this.tag = tag; Object.assign(this, attrs); this.children = []; }
  appendChild(child) { this.children.push(child); }
  replaceChildren() { this.children = []; }
}
const messages = new Element('div');
const wrap = {scrollHeight: 1000, clientHeight: 300, scrollTop: 700};
const context = vm.createContext({
  tunnelChatMessages: messages, tunnelChatSnapshot: [], tunnelChatLanguage: null,
  currentLang: 'ja', tunnelChatEmpty: {}, document: {getElementById: () => wrap},
  el: (tag, attrs) => new Element(tag, attrs), t: key => key,
  fmtEpochMs: value => String(value),
});
const renderer = html.slice(html.indexOf('  function renderTunnelChat(items)'), html.indexOf('  function renderTunnelRoomList'));
vm.runInContext(renderer, context);
const first = {mine: true, timestamp_ms: 1, text: '<script>unsafe</script>'};
context.renderTunnelChat([first]);
assert.equal(messages.children.length, 1);
assert.equal(messages.children[0].children[1].text, first.text, 'Message must remain literal text');
assert.equal(wrap.scrollTop, 1000, 'Follow messages when at the bottom');
const initialNode = messages.children[0];
wrap.scrollTop = 120;
context.renderTunnelChat([first]);
assert.equal(messages.children[0], initialNode, 'Unchanged polling preserves DOM and selections');
context.renderTunnelChat([first, {timestamp_ms: 2, text: 'new'}]);
assert.equal(messages.children[0], initialNode, 'Appending preserves older message nodes');
assert.equal(wrap.scrollTop, 120, 'Incoming message must not interrupt reading');
assert.equal(messages.children.length, 2);
context.currentLang = 'en';
context.renderTunnelChat([first]);
assert.equal(messages.children.length, 1, 'Language switch refreshes messages');
context.renderTunnelChat([]);
assert.equal(messages.children.length, 0, 'Room history reset removes old messages');
assert.equal(context.tunnelChatEmpty.hidden, false);
console.log('PASS: script syntax, unique IDs, chat text safety, incremental polling, scroll preservation, language switch, history reset');
// Circular nodes keep labels outside the marker; inspection is delegated.
const inspected = [];
function svgElement(tag, attrs = {}) {
  const node = new Element(tag, attrs);
  node.classList = {add() {}};
  node.listeners = {};
  node.setAttribute = (key, value) => { node[key] = value; };
  node.addEventListener = (event, callback) => { node.listeners[event] = callback; };
  return node;
}
const graphContext = vm.createContext({
  svgEl: svgElement, TOPO_NODE_H: 144,
  topologyTruncateLabel: value => value,
  topologyPresentation: {bindNode: (node, key) => inspected.push(key)}, t: key => key,
});
const nodeStart = html.indexOf('  function topologyNodeBox(');
vm.runInContext(html.slice(nodeStart, html.indexOf('  /* ---- auto-layout helpers', nodeStart)), graphContext);
const parent = new Element('svg');
const node = graphContext.topologyNodeBox(parent, 150, 100, 180, 'My node', 'Connected', 'kind-self', 'peer-123', 'Connected');
assert.equal(node.radius, 30);
const circle = node.el.children.find(child => child.tag === 'circle');
assert.equal(circle.r, 30);
assert.equal(circle.cx, 90);
const label = node.el.children.find(child => child.tag === 'text');
assert.ok(label.y > circle.cy + circle.r, 'Label must sit below the circular node');
assert.deepEqual(inspected, ['peer-123']);
assert.ok(node.el.children.find(child => child.tag === 'rect' && child.class === 'topology-node-hit'), 'Entire label area must be clickable');
assert.ok(!html.includes('id="settings-drawer-ai"'), 'AI settings must not require opening a drawer');
console.log('PASS: circular node geometry, external label, detail feature boundary, label hit area, direct AI settings');
