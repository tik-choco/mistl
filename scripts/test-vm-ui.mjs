import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';
import { Window } from 'happy-dom';

const html = fs.readFileSync(new URL('../src/web/assets/index.html', import.meta.url), 'utf8');
const translationsStart = html.indexOf('  var T = {');
const translationsEnd = html.indexOf('\n  };', translationsStart);
const translations = vm.runInNewContext(html.slice(translationsStart, translationsEnd + 5) + '\nT;');
const shellStart = html.indexOf('  /* ---------- virtual machines ---------- */');
const shellEnd = html.indexOf('  /* ---------- Nostr relay', shellStart);
assert.ok(shellStart >= 0 && shellEnd > shellStart);

const window = new Window({ url: 'http://localhost/' });
const root = window.document.createElement('section');
root.id = 'panel-vm';
window.document.body.append(root);
const self = { node: 'self', self: true, name: 'Local', online: true, ha_capable: true, score: 10, mem_free_mb: 4096, running_vms: 0 };
const slower = { ...self, self: false, node: 'slower', name: 'Smaller host', score: 20 };
const faster = { ...slower, node: 'faster', name: '<img src=x>', score: 50, mem_free_mb: 8192 };
const voter = { ...slower, node: 'abcdef1234567890', name: '', online: false, ha_capable: false, score: null };
const members = [self, slower, faster, voter];
const host = (member) => ({ node: member.node, role: 'host' });
const group = [host(self), host(slower), host(faster), { node: voter.node, role: 'witness' }];
const guest = { server_id: '0123456789abcdef', name: 'Guest', state: 'running' };
const invite = 'test-only-invite';
let locale = 'en', joined = false, holder, denyLeave = true, remote = false, copied;
const calls = [];
const timers = new Map();
let timerId = 0;
window.setTimeout = (callback, delay) => { timers.set(++timerId, { callback, delay }); return timerId; };
window.clearTimeout = (id) => timers.delete(id);
Object.assign(window, {
  t: (key, values = {}) => {
    assert.ok(translations[locale][key], `Missing ${locale} translation: ${key}`);
    return translations[locale][key].replace(/\{(\w+)\}/g, (_, name) => values[name] ?? '');
  },
  state: { topology: { self: { node_id: self.node } } },
  fmtBytes: String,
  fmtUptime: String,
  copyToClipboard: (text) => { copied = text; },
  api: async (cmd, args) => {
    calls.push({ cmd, args: { ...args } });
    switch (cmd) {
      case 'vm.doctor': return { ready: true };
      case 'vm.list': return { vms: [holder ? { ...guest, holder, state: holder === self.node ? 'running' : 'standby' } : guest] };
      case 'vm.pool.status': return { joined, members: joined ? members : [] };
      case 'vm.pool.create': joined = true; return { pool_id: 'pool', room: 'room', invite };
      case 'vm.pool.invite':
        if (remote) throw new Error('`vm.pool.invite` is not available from a remote dashboard session; use the CLI');
        return { invite };
      case 'vm.pool.leave':
        if (denyLeave) throw new Error('This node protects a VM');
        joined = false; return { left: true };
      case 'vm.pool.join': joined = true; return { pool_id: 'pool', room: 'room' };
      case 'config.show': return { vm: {} };
      case 'vm.protect': holder = self.node; return { members: group };
      case 'vm.ha.status': return { members: group, holder, state: holder === self.node ? 'holder' : 'standby' };
      case 'vm.move': holder = args.to; return { holder };
      default: assert.fail('Unexpected command ' + cmd);
    }
  },
});
window.eval(fs.readFileSync(new URL('../src/web/assets/features/vm.js', import.meta.url), 'utf8'));
window.eval(html.slice(shellStart, shellEnd));
const panel = root.querySelector('.vm-pool');
const dialog = root.querySelector('dialog');
const card = () => root.querySelector('.vm-list .vm-card');
const flush = async () => {
  await new Promise(setImmediate);
  const immediate = [...timers.values()].find(({ delay }) => delay === 0);
  if (immediate) {
    immediate.callback();
    await new Promise(setImmediate);
  }
};
const buttons = (scope) => [...scope.querySelectorAll('button')].filter((button) => !button.closest('[hidden]'));
const control = (scope, label) => buttons(scope).find((button) => button.textContent === label);
const click = (scope, label) => {
  const button = control(scope, label);
  assert.ok(button && !button.disabled, `Expected an enabled button: ${label}`);
  button.click();
};
const close = () => dialog.querySelector('.vm-dismiss').click();
const poll = async () => {
  assert.equal(timers.size, 1, 'VM list and pool share a timer');
  [...timers.values()][0].callback();
  await flush();
};
const fact = (label) => [...card().querySelectorAll('dt')].find((term) => term.textContent === label)?.nextElementSibling.textContent;
const lastMutation = (cmd, args) => assert.deepEqual(calls.filter((call) => call.cmd === cmd).at(-1), { cmd, args });

try {
  window.syncVmVisibility();
  assert.equal(calls.length, 0, 'VM panel polls only when visible');
  root.classList.add('active');
  window.syncVmVisibility();
  await flush();
  assert.deepEqual(calls.map(({ cmd }) => cmd), ['vm.pool.status', 'vm.list', 'vm.doctor']);
  assert.deepEqual(buttons(panel).map((button) => button.textContent), ['Create pool', 'Join pool']);
  assert.equal(control(card(), 'Protect'), undefined);
  assert.equal(fact('Running on'), 'this node');
  assert.equal(card().querySelector('.vm-protection').textContent, 'Not protected');
  for (const [language, title, create] of [
    ['ja', 'プール', 'プールを作成'],
    ['zh', '资源池', '创建资源池'],
    ['en', 'Pool', 'Create pool'],
  ]) {
    locale = language;
    window.vmView.refreshLanguage();
    assert.equal(panel.querySelector('h3').textContent, title);
    assert.ok(control(panel, create));
  }

  click(panel, 'Create pool');
  assert.equal(dialog.querySelector('h3').textContent, 'Create a pool on this node?');
  click(dialog, 'Create pool');
  await flush();
  lastMutation('vm.pool.create', {});
  assert.equal(dialog.querySelector('code').textContent, invite);
  assert.match(dialog.textContent, /Keep this code secret/);
  click(dialog, 'Copy');
  assert.equal(copied, invite);
  close();
  assert.ok(!root.textContent.includes(invite), 'Closing clears the invite from the DOM');
  assert.deepEqual(buttons(panel).map((button) => button.textContent), ['Invite', 'Leave']);
  assert.match(panel.textContent, /Local \(this node\).*Can host.*8192 MB/);
  assert.match(panel.textContent, /abcdef12Vote only/);
  assert.equal(panel.querySelector('[data-online="false"]').getAttribute('aria-label'), 'Offline');
  assert.equal(panel.querySelector('img'), null, 'Node names stay literal');

  click(panel, 'Invite');
  await flush();
  lastMutation('vm.pool.invite', {});
  assert.equal(dialog.querySelector('code').textContent, invite);
  close();
  click(panel, 'Leave');
  click(dialog, 'Leave');
  await flush();
  assert.equal(dialog.querySelector('[role="alert"]').textContent, 'This node protects a VM');
  denyLeave = false;
  click(dialog, 'Leave');
  await flush();
  assert.equal(dialog.open, false);
  click(panel, 'Join pool');
  const code = dialog.querySelector('textarea');
  assert.equal(code, window.document.activeElement);
  assert.equal(control(dialog, 'Join pool').disabled, true);
  code.value = ' ' + invite + '\n';
  code.dispatchEvent(new window.Event('input', { bubbles: true }));
  click(dialog, 'Join pool');
  await flush();
  lastMutation('vm.pool.join', { code: invite });

  click(root, 'New VM');
  dialog.querySelector('input').value = 'Second guest';
  click(dialog, 'Next');
  await flush();
  click(dialog, 'Next');
  assert.match(dialog.textContent, /PlacementAutomatic/);
  close();

  slower.online = false;
  faster.score = null;
  await poll();
  assert.equal(control(card(), 'Protect'), undefined, 'An offline host and a null score are insufficient');
  slower.online = true;
  faster.score = 50;
  await poll();
  click(card(), 'Protect');
  click(dialog, 'Protect');
  await flush();
  lastMutation('vm.protect', { id: guest.server_id });
  assert.equal(control(card(), 'Protect'), undefined);
  assert.equal(card().querySelector('.vm-protection').textContent, 'Protected on 3 nodes');

  click(card(), 'Move');
  const targets = [...dialog.querySelectorAll('.vm-move-targets button')];
  assert.deepEqual(targets.map((button) => button.querySelector('strong').textContent), [faster.name, slower.name]);
  assert.match(targets[0].textContent, /8192 MB/);
  targets[0].click();
  assert.match(dialog.textContent, /about a minute.*running programs will end/);
  click(dialog, 'Move');
  await flush();
  lastMutation('vm.move', { id: guest.server_id, to: faster.node });
  assert.equal(fact('Running on'), faster.name);
  assert.equal(control(card(), 'Move'), undefined, 'Standbys cannot initiate a move');
  holder = '0123456789fedcba';
  await poll();
  assert.equal(fact('Running on'), '01234567');

  remote = true;
  click(panel, 'Invite');
  await flush();
  assert.equal(dialog.querySelector('code'), null);
  assert.match(dialog.querySelector('[role="alert"]').textContent, /remote dashboard session$/);
  close();
  assert.equal(control(panel, 'Invite').disabled, true);
  root.classList.remove('active');
  window.syncVmVisibility();
  assert.equal(timers.size, 0, 'Leaving the VM panel stops polling');

  console.log('PASS: VM pool polling, membership and invite workflows, protection, moves, and translations');
} finally {
  window.happyDOM.abort();
}
