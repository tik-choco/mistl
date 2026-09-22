import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';

class Element {
  constructor(tag, document) {
    this.tag = tag; this.ownerDocument = document; this.children = []; this.attributes = {};
    this.listeners = {}; this.style = {}; this.isConnected = true; this.hidden = false;
    const classes = new Set();
    this.classList = { add: c => classes.add(c), toggle: (c, on) => on ? classes.add(c) : classes.delete(c), contains: c => classes.has(c) };
  }
  appendChild(child) { this.children.push(child); return child; }
  replaceChildren(...children) { this.children = children; }
  setAttribute(key, value) { this.attributes[key] = value; }
  getAttribute(key) { return this.attributes[key] ?? null; }
  removeAttribute(key) { delete this.attributes[key]; }
  addEventListener(type, listener) { (this.listeners[type] ||= []).push(listener); }
  dispatch(type, event = {}) { (this.listeners[type] || []).forEach(listener => listener(event)); }
  getBoundingClientRect() { return this.rect || {left: 290, top: 280, bottom: 324, width: 200, height: 120}; }
  focus() { this.ownerDocument.activeElement = this; this.dispatch('focus'); }
  showModal() { this.open = true; }
  close() { this.open = false; this.dispatch('close'); }
}
const document = { listeners: {}, createElement(tag) { return new Element(tag, this); }, addEventListener(type, fn) { this.listeners[type] = fn; } };
document.body = new Element('body', document);
const window = { innerWidth: 390, innerHeight: 640, clearTimeout() {}, setTimeout() {}, addEventListener() {} };
const context = vm.createContext({window});
const source = fs.readFileSync(new URL('../src/web/assets/features/topology.js', import.meta.url), 'utf8');
vm.runInContext(source, context);
let fallbackFocus = 0;
const copies = [];
const graphElement = new Element('svg', document);
graphElement.parentElement = {clientWidth: 360};
const controls = new Element('div', document);
const feature = window.MistlTopology.create({document, window, graph: graphElement, controls, t: key => key, copy: id => copies.push(id), focusFallback: () => fallbackFocus++});
feature.setGraphWidth(740);
assert.equal(graphElement.style.width, '740px', 'Natural scale keeps graph labels readable');
controls.children[0].dispatch('click');
assert.equal(graphElement.style.width, '334px', 'Fit control explicitly shows the entire graph');
assert.equal(controls.children[0].getAttribute('aria-pressed'), 'true');
feature.setGraphWidth(860);
assert.equal(graphElement.style.width, '334px', 'Fit preference survives graph updates');
controls.children[1].dispatch('click');
assert.equal(graphElement.style.width, '860px', 'Actual size restores readable graph scale');
const [preview, dialog] = document.body.children;
const close = dialog.children[0].children[1];
const content = dialog.children[1];
const copy = dialog.children[2].children[0];
function allText(node) { return [node.textContent || '', ...node.children.map(allText)].join(' '); }
const record = {key: 'peer-1', id: 'peer-1' + '0123456789'.repeat(12), label: '<img src=x onerror=alert(1)>' + '長い名前'.repeat(25), role: 'peer', state: 'connected', rooms: ['room-' + '長'.repeat(90)]};
feature.setSnapshot([record]);
feature.beginRender();
const node = new Element('g', document);
feature.bindNode(node, record.key);
feature.endRender();
assert.equal(node.getAttribute('aria-haspopup'), 'dialog');
assert.equal(node.getAttribute('tabindex'), '0');
node.focus();
assert.equal(preview.hidden, false, 'Keyboard focus shows full preview');
assert.ok(allText(preview).includes(record.id));
assert.ok(allText(preview).includes(record.label), 'Full labels remain literal text');
assert.ok(parseFloat(preview.style.left) + 200 <= 390 - 8, 'Preview stays inside mobile viewport');
document.listeners.keydown({key: 'Escape'});
assert.equal(preview.hidden, true, 'Escape dismisses preview');
node.dispatch('keydown', {key: 'Enter', preventDefault() {}});
assert.equal(dialog.open, true);
assert.equal(preview.hidden, true);
assert.equal(copies.length, 0, 'Selecting a node must not copy as a side effect');
assert.ok(allText(content).includes(record.label));
assert.ok(allText(content).includes(record.rooms[0]));
assert.equal(node.classList.contains('is-selected'), true);
copy.dispatch('click');
assert.deepEqual(copies, [record.id], 'Dedicated copy action uses full ID');
const stableChildren = content.children;
feature.setSnapshot([{...record}]);
assert.equal(content.children, stableChildren, 'Unchanged polling preserves detail text selection');
feature.setSnapshot([{...record, state: 'reconnecting'}]);
assert.ok(allText(content).includes('reconnecting'), 'Open detail receives current state');
feature.beginRender();
node.isConnected = false;
const replacement = new Element('g', document);
feature.bindNode(replacement, record.key);
feature.endRender();
close.dispatch('click');
assert.equal(document.activeElement, replacement, 'Closing after a poll restores focus to the rebuilt node');
assert.equal(dialog.open, false);
replacement.dispatch('click');
feature.setSnapshot([]);
assert.ok(allText(content).includes('topology.details.unavailable'), 'Removed connections are visibly identified as last known data');
assert.ok(allText(content).includes(record.id), 'Removal does not discard ID being inspected');
feature.beginRender();
replacement.isConnected = false;
feature.endRender();
close.dispatch('click');
assert.equal(fallbackFocus, 1, 'Closing removed node focuses graph fallback');
feature.setSnapshot([{key: 'viewers', label: 'Viewers', viewers: '3'}]);
const viewers = new Element('g', document);
feature.bindNode(viewers, 'viewers');
viewers.dispatch('keydown', {key: ' ', preventDefault() {}});
assert.equal(dialog.open, true, 'Space opens details for aggregate viewers too');
assert.equal(copy.hidden, true, 'Aggregate viewers have no invented node ID');
assert.equal(dialog.getAttribute('aria-labelledby'), dialog.children[0].children[0].id);

const html = fs.readFileSync(new URL('../src/web/assets/index.html', import.meta.url), 'utf8');
const geometry = vm.createContext({TOPO_NODE_W_MIN: 112, TOPO_NODE_W_MAX: 220});
vm.runInContext(html.slice(html.indexOf('  function topologyCharUnits('), html.indexOf('  /* Keep graph geometry here;')), geometry);
for (const label of ['長い日本語の表示名'.repeat(30), 'abcdefghijklmnopqrstuvwxyz'.repeat(10), '🔐🌍😀'.repeat(30)]) {
  const compact = geometry.topologyTruncateLabel(label, 20);
  assert.ok(geometry.topologyTextUnits(compact) <= 20);
  assert.ok(compact.includes('…'));
  assert.ok(compact.isWellFormed(), 'Truncation preserves Unicode characters');
}
assert.match(html, /id="topology-svg"[^>]*role="group"/, 'Interactive SVG exposes child buttons');
assert.ok(!html.includes('padding-bottom: 36vh'), 'Topology must not reserve a mobile overlay');
console.log('PASS: topology full-text preview, keyboard details, mobile preview bounds, copy, live snapshots, focus restoration, removed nodes, viewers, Unicode truncation');
