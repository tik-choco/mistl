/* Direct messages between mistl nodes: people list + conversation view.
 *
 *   window.MistlMessenger.mount(el, { call, peers, lang })
 *
 *   call(cmd, args) -> Promise<result>   (dm.send / dm.ls / dm.history / dm.read / dm.download)
 *   peers           -> window.MistlPeers or null  (list(), byDid(), label(), avatarEl(), subscribe())
 *   lang()          -> "en" | "ja" | "zh"
 *   upload(file)    -> optional Promise<{imported}>; defaults to POST /api/store/sandbox-upload
 *
 * Peer-supplied text (names, messages, file names) is only ever assigned through
 * textContent / attributes, never innerHTML. */
(function (root) {
  'use strict';

  const POLL_MS = 3000;
  const MAX_TEXT = 4000;
  const MAX_FILE_BYTES = 256 * 1024 * 1024;

  const words = {
    en: {
      title: 'Messages', search: 'Search people', people: 'People', online: 'Online', offline: 'Offline',
      emptyPeers: 'No one to message yet.',
      emptyPeersHelp: 'Both nodes must be in the same room (for example the Tunnel, AI or Storage room). People appear here once they are connected and verified.',
      pick: 'Choose someone to start a conversation.', empty: 'No messages yet. Say hello.',
      placeholder: 'Write a message', send: 'Send', attach: 'Attach a file', back: 'Back',
      uploading: 'Uploading {name}…', sending: 'Sending…', download: 'Download', downloading: 'Downloading…',
      saved: 'Saved', tooBig: 'This file is larger than 256 MiB.', tooLong: 'Messages are limited to {n} characters.',
      sent: 'Sent', delivered: 'Delivered', failed: 'Not delivered', today: 'Today', yesterday: 'Yesterday',
      error: 'Error', you: 'You', unread: 'unread',
      loadFailed: 'Could not load messages',
    },
    ja: {
      title: 'メッセージ', search: '人を検索', people: 'ユーザー', online: 'オンライン', offline: 'オフライン',
      emptyPeers: 'メッセージを送れる相手がまだいません。',
      emptyPeersHelp: '両方のノードが同じルーム（トンネル・AI・ストレージなどのルーム）に参加している必要があります。接続して認証されると、ここに表示されます。',
      pick: '会話を始める相手を選んでください。', empty: 'まだメッセージはありません。',
      placeholder: 'メッセージを入力', send: '送信', attach: 'ファイルを添付', back: '戻る',
      uploading: '{name} をアップロード中…', sending: '送信中…', download: 'ダウンロード', downloading: 'ダウンロード中…',
      saved: '保存しました', tooBig: 'ファイルが 256 MiB を超えています。', tooLong: 'メッセージは {n} 文字までです。',
      sent: '送信済み', delivered: '配信済み', failed: '届きませんでした', today: '今日', yesterday: '昨日',
      error: 'エラー', you: 'あなた', unread: '未読',
      loadFailed: 'メッセージを読み込めませんでした',
    },
    zh: {
      title: '消息', search: '搜索联系人', people: '联系人', online: '在线', offline: '离线',
      emptyPeers: '还没有可以发消息的对象。',
      emptyPeersHelp: '双方节点必须在同一个房间（例如隧道、AI 或存储房间）。连接并通过验证后会显示在这里。',
      pick: '选择一个联系人开始聊天。', empty: '还没有消息，打个招呼吧。',
      placeholder: '输入消息', send: '发送', attach: '添加文件', back: '返回',
      uploading: '正在上传 {name}…', sending: '正在发送…', download: '下载', downloading: '正在下载…',
      saved: '已保存', tooBig: '文件超过 256 MiB。', tooLong: '消息最多 {n} 个字符。',
      sent: '已发送', delivered: '已送达', failed: '未送达', today: '今天', yesterday: '昨天',
      error: '错误', you: '你', unread: '未读',
      loadFailed: '无法加载消息',
    },
  };

  function mount(host, options) {
    options = options || {};
    const doc = host.ownerDocument || document;
    const call = options.call;
    const peers = options.peers || null;
    const langOf = typeof options.lang === 'function' ? options.lang : function () { return 'en'; };

    let convs = [];          // dm.ls conversations
    let selected = '';       // open did
    let messages = [];       // open conversation
    let query = '';
    let notice = null;       // {kind:'info'|'error', text}
    let busy = 0;            // in-flight uploads/sends
    let destroyed = false;
    let timer = 0;
    let listSig = '', msgSig = '', lastLang = '';
    const downloading = new Set();
    const savedMsgs = new Set();
    let unsubscribe = null;

    function lang() { const l = String(langOf() || 'en'); return words[l] ? l : 'en'; }
    function t(key, vars) {
      let s = (words[lang()] && words[lang()][key]) || words.en[key] || key;
      if (vars) Object.keys(vars).forEach(function (k) { s = s.split('{' + k + '}').join(String(vars[k])); });
      return s;
    }
    function el(tag, cls, text) {
      const e = doc.createElement(tag);
      if (cls) e.className = cls;
      if (text != null) e.textContent = text;
      return e;
    }
    function shortDid(did) { return did.length > 18 ? did.slice(0, 12) + '…' + did.slice(-4) : did; }
    function peerOf(did) { try { return peers && peers.byDid ? peers.byDid(did) : null; } catch (e) { return null; } }
    function nameOf(did) {
      let n = '';
      try { n = peers && peers.label ? peers.label(did) : ''; } catch (e) { n = ''; }
      return n ? String(n) : shortDid(did);
    }
    function isOnline(did) { const p = peerOf(did); return !!(p && p.online); }
    function avatar(did, size) {
      try {
        if (peers && peers.avatarEl) {
          const node = peers.avatarEl(peerOf(did) || did, size);
          if (node) return node;
        }
      } catch (e) { /* fall through to the initial */ }
      const a = el('span', 'msgr-avatar', (nameOf(did).charAt(0) || '?').toUpperCase());
      a.style.width = a.style.height = size + 'px';
      let h = 0; for (let i = 0; i < did.length; i++) h = (h * 31 + did.charCodeAt(i)) % 360;
      a.style.setProperty('--msgr-hue', String(h));
      return a;
    }
    function fmtBytes(n) {
      n = Number(n) || 0; const u = ['B', 'KB', 'MB', 'GB']; let i = 0;
      while (n >= 1024 && i < u.length - 1) { n /= 1024; i++; }
      return (i === 0 ? String(n) : n.toFixed(1)) + ' ' + u[i];
    }
    function langTag() { return { en: 'en', ja: 'ja', zh: 'zh-CN' }[lang()]; }
    function dayKey(ms) { const d = new Date(ms); return d.getFullYear() + '-' + d.getMonth() + '-' + d.getDate(); }
    function dayLabel(ms) {
      const now = new Date(), d = new Date(ms);
      if (dayKey(ms) === dayKey(now.getTime())) return t('today');
      if (dayKey(ms) === dayKey(now.getTime() - 86400000)) return t('yesterday');
      return d.toLocaleDateString(langTag(), { year: 'numeric', month: 'short', day: 'numeric' });
    }
    function timeLabel(ms) { return new Date(ms).toLocaleTimeString(langTag(), { hour: '2-digit', minute: '2-digit' }); }
    function errText(e) { return (e && e.message) ? e.message : String(e); }

    /* ---------- skeleton ---------- */
    host.textContent = '';
    host.classList.add('msgr-host');
    const root = el('div', 'msgr');
    const side = el('div', 'msgr-side');
    const sideHead = el('div', 'msgr-side-head');
    const titleEl = el('h3', 'msgr-title');
    const search = el('input', 'msgr-search'); search.type = 'search'; search.autocomplete = 'off';
    sideHead.append(titleEl, search);
    const list = el('div', 'msgr-list');
    side.append(sideHead, list);

    const main = el('div', 'msgr-main');
    const head = el('div', 'msgr-head');
    const backBtn = el('button', 'msgr-back'); backBtn.type = 'button'; backBtn.textContent = '‹';
    const headWho = el('div', 'msgr-who');
    head.append(backBtn, headWho);
    const body = el('div', 'msgr-body');
    const noticeEl = el('div', 'msgr-notice'); noticeEl.hidden = true;
    const composer = el('form', 'msgr-composer');
    const attachBtn = el('button', 'msgr-icon'); attachBtn.type = 'button'; attachBtn.textContent = '＋';
    const fileInput = el('input'); fileInput.type = 'file'; fileInput.hidden = true;
    const input = el('textarea', 'msgr-input'); input.rows = 1; input.maxLength = MAX_TEXT;
    const sendBtn = el('button', 'msgr-send'); sendBtn.type = 'submit';
    composer.append(attachBtn, fileInput, input, sendBtn);
    main.append(head, body, noticeEl, composer);
    root.append(side, main);
    host.append(root);

    function setNotice(kind, text) {
      notice = text ? { kind: kind, text: text } : null;
      noticeEl.hidden = !notice;
      noticeEl.textContent = notice ? notice.text : '';
      noticeEl.className = 'msgr-notice' + (notice ? ' is-' + notice.kind : '');
      noticeEl.setAttribute('role', notice && notice.kind === 'error' ? 'alert' : 'status');
    }

    function applyStatic() {
      titleEl.textContent = t('title');
      search.placeholder = t('search');
      input.placeholder = t('placeholder');
      sendBtn.textContent = t('send');
      attachBtn.title = t('attach'); attachBtn.setAttribute('aria-label', t('attach'));
      backBtn.setAttribute('aria-label', t('back')); backBtn.title = t('back');
    }

    /* ---------- people list ---------- */
    function people() {
      const map = new Map();
      convs.forEach(function (c) { map.set(c.did, { did: c.did, unread: c.unread || 0, updated: c.updated_ms || 0, last: c.last }); });
      let online = [];
      try { online = peers && peers.list ? peers.list() || [] : []; } catch (e) { online = []; }
      online.forEach(function (p) {
        if (!p || !p.did) return;
        if (!map.has(p.did)) map.set(p.did, { did: p.did, unread: 0, updated: 0, last: null });
      });
      if (selected && !map.has(selected)) map.set(selected, { did: selected, unread: 0, updated: 0, last: null });
      const q = query.trim().toLowerCase();
      const out = Array.from(map.values()).map(function (p) {
        p.name = nameOf(p.did); p.online = isOnline(p.did); return p;
      }).filter(function (p) { return !q || p.name.toLowerCase().indexOf(q) >= 0 || p.did.toLowerCase().indexOf(q) >= 0; });
      out.sort(function (a, b) {
        if (!!b.unread !== !!a.unread) return b.unread ? 1 : -1;
        if (b.updated !== a.updated) return b.updated - a.updated;
        if (a.online !== b.online) return a.online ? -1 : 1;
        return a.name.localeCompare(b.name);
      });
      return out;
    }
    function preview(last) {
      if (!last) return '';
      const who = last.mine ? t('you') + ': ' : '';
      if (last.text) return who + last.text.replace(/\s+/g, ' ');
      if (last.file) return who + '📎 ' + last.file.name;
      return '';
    }
    function renderList() {
      const ps = people();
      const sig = JSON.stringify([lang(), selected, query, ps.map(function (p) { return [p.did, p.name, p.online, p.unread, p.updated, p.last && p.last.status, p.last && p.last.id]; })]);
      if (sig === listSig) return;
      listSig = sig;
      list.textContent = '';
      if (!ps.length) {
        const empty = el('div', 'msgr-empty');
        empty.append(el('p', 'msgr-empty-title', t('emptyPeers')), el('p', 'msgr-empty-help', t('emptyPeersHelp')));
        list.append(empty);
        return;
      }
      ps.forEach(function (p) {
        const row = el('button', 'msgr-person' + (p.did === selected ? ' is-active' : '')); row.type = 'button';
        const av = el('span', 'msgr-av'); av.append(avatar(p.did, 36));
        const dot = el('span', 'msgr-dot' + (p.online ? ' is-online' : '')); dot.title = p.online ? t('online') : t('offline');
        av.append(dot);
        const txt = el('span', 'msgr-person-text');
        txt.append(el('span', 'msgr-name', p.name), el('span', 'msgr-preview', preview(p.last)));
        row.append(av, txt);
        if (p.unread) { const b = el('span', 'msgr-badge', p.unread > 99 ? '99+' : String(p.unread)); b.title = t('unread'); row.append(b); }
        row.addEventListener('click', function () { open(p.did); });
        list.append(row);
      });
    }

    /* ---------- conversation ---------- */
    function ticks(m) {
      if (!m.mine) return null;
      const s = el('span', 'msgr-status is-' + m.status);
      s.textContent = m.status === 'delivered' ? '✓✓' : m.status === 'failed' ? '!' : '✓';
      s.title = t(m.status === 'failed' ? 'failed' : m.status === 'delivered' ? 'delivered' : 'sent');
      return s;
    }
    function fileBubble(m) {
      const f = el('div', 'msgr-file');
      const info = el('div', 'msgr-file-info');
      info.append(el('span', 'msgr-file-name', fileName(m.file)), el('span', 'msgr-file-size', fmtBytes(m.file.size)));
      f.append(el('span', 'msgr-file-icon', '📎'), info);
      if (!m.mine) {
        const key = selected + '/' + m.id;
        const btn = el('button', 'msgr-download'); btn.type = 'button';
        btn.textContent = downloading.has(key) ? t('downloading') : savedMsgs.has(key) ? t('saved') : t('download');
        btn.disabled = downloading.has(key);
        btn.addEventListener('click', function () { download(m); });
        f.append(btn);
      }
      return f;
    }
    function fileName(file) { return String(file && file.name || 'file'); }
    function renderMessages(force) {
      const sig = JSON.stringify([lang(), selected, Array.from(downloading), Array.from(savedMsgs), messages.map(function (m) { return [m.id, m.status]; })]);
      if (!force && sig === msgSig) return;
      msgSig = sig;
      const stick = body.scrollHeight - body.scrollTop - body.clientHeight < 80 || !body.firstChild;
      body.textContent = '';
      if (!selected) { body.append(el('div', 'msgr-hint', t('pick'))); return; }
      if (!messages.length) { body.append(el('div', 'msgr-hint', t('empty'))); return; }
      let day = '';
      messages.forEach(function (m) {
        const k = dayKey(m.ts_ms);
        if (k !== day) { day = k; body.append(el('div', 'msgr-day', dayLabel(m.ts_ms))); }
        const row = el('div', 'msgr-row' + (m.mine ? ' is-mine' : ''));
        const bubble = el('div', 'msgr-bubble');
        if (m.file) bubble.append(fileBubble(m));
        if (m.text) bubble.append(el('div', 'msgr-text', m.text));
        const meta = el('div', 'msgr-meta', timeLabel(m.ts_ms));
        const tk = ticks(m); if (tk) meta.append(tk);
        bubble.append(meta);
        row.append(bubble);
        body.append(row);
      });
      if (stick) body.scrollTop = body.scrollHeight;
    }
    function renderHead() {
      headWho.textContent = '';
      if (!selected) { head.hidden = true; return; }
      head.hidden = false;
      const av = el('span', 'msgr-av'); av.append(avatar(selected, 32));
      const nm = el('div', 'msgr-who-text');
      const on = isOnline(selected);
      nm.append(el('div', 'msgr-who-name', nameOf(selected)), el('div', 'msgr-who-state' + (on ? ' is-online' : ''), on ? t('online') : t('offline')));
      headWho.append(av, nm);
    }
    function renderAll() {
      if (lang() !== lastLang) { lastLang = lang(); applyStatic(); listSig = ''; msgSig = ''; }
      root.classList.toggle('is-open', !!selected);
      composer.hidden = !selected;
      renderList(); renderHead(); renderMessages(false);
    }

    /* ---------- data ---------- */
    function refreshList() {
      return call('dm.ls', {}).then(function (r) {
        convs = (r && r.conversations) || [];
        const cur = convs.find(function (c) { return c.did === selected; });
        if (cur && cur.unread) call('dm.read', { did: selected }).catch(function () {});
      }).catch(function () { /* transient: keep the previous list */ });
    }
    function refreshMessages() {
      if (!selected) return Promise.resolve();
      const did = selected;
      return call('dm.history', { did: did, limit: 500 }).then(function (r) {
        if (did !== selected) return;
        messages = (r && r.messages) || [];
        if (notice && notice.kind === 'error' && notice.text === t('loadFailed')) setNotice('', '');
      }).catch(function () { if (did === selected) setNotice('error', t('loadFailed')); });
    }
    function tick() {
      if (destroyed) return;
      if (!host.isConnected || doc.visibilityState === 'hidden') return;
      Promise.all([refreshList(), refreshMessages()]).then(function () { if (!destroyed) renderAll(); });
    }
    function open(did) {
      selected = did; messages = []; msgSig = ''; savedMsgs.clear();
      setNotice('', '');
      renderAll();
      call('dm.read', { did: did }).catch(function () {});
      refreshMessages().then(function () { if (selected === did) { renderMessages(true); refreshList().then(renderAll); } });
      input.focus({ preventScroll: true });
    }

    /* ---------- actions ---------- */
    function send(text) {
      busy++; setNotice('info', t('sending'));
      return call('dm.send', { did: selected, text: text }).then(function () { setNotice('', ''); }, function (e) {
        setNotice('error', errText(e));
      }).then(function () { busy--; return Promise.all([refreshList(), refreshMessages()]); }).then(renderAll);
    }
    composer.addEventListener('submit', function (e) {
      e.preventDefault();
      const text = input.value;
      if (!selected || !text.trim()) return;
      if (text.length > MAX_TEXT) { setNotice('error', t('tooLong', { n: MAX_TEXT })); return; }
      input.value = ''; autosize();
      send(text);
    });
    input.addEventListener('keydown', function (e) {
      if (e.key === 'Enter' && !e.shiftKey && !e.isComposing && e.keyCode !== 229) {
        e.preventDefault();
        composer.requestSubmit ? composer.requestSubmit() : sendBtn.click();
      }
    });
    function autosize() { input.style.height = 'auto'; input.style.height = Math.min(input.scrollHeight, 140) + 'px'; }
    input.addEventListener('input', autosize);
    search.addEventListener('input', function () { query = search.value; renderList(); });
    backBtn.addEventListener('click', function () { selected = ''; messages = []; setNotice('', ''); renderAll(); });

    function defaultUpload(file) {
      return new Promise(function (resolve, reject) {
        const rt = (doc.defaultView || window).mistlRuntime || {};
        const xhr = new XMLHttpRequest();
        xhr.open('POST', '/api/store/sandbox-upload');
        xhr.setRequestHeader('x-mistl-ui', '1');
        xhr.setRequestHeader('x-mistl-instance', [rt.instance_id, rt.build_id].join('/'));
        xhr.setRequestHeader('x-file-name', encodeURIComponent(file.name));
        xhr.upload.addEventListener('progress', function (e) {
          if (e.lengthComputable) setNotice('info', t('uploading', { name: file.name }) + ' ' + Math.round(e.loaded / e.total * 100) + '%');
        });
        xhr.onload = function () {
          let j = null; try { j = JSON.parse(xhr.responseText); } catch (e) { j = null; }
          if (j && j.ok === true) resolve(j.data); else reject(new Error((j && j.error) || ('upload failed (' + xhr.status + ')')));
        };
        xhr.onerror = function () { reject(new Error('daemon unreachable')); };
        xhr.send(file);
      });
    }
    attachBtn.addEventListener('click', function () { if (selected) fileInput.click(); });
    fileInput.addEventListener('change', function () {
      const file = fileInput.files && fileInput.files[0];
      fileInput.value = '';
      if (!file || !selected) return;
      if (file.size > MAX_FILE_BYTES) { setNotice('error', t('tooBig')); return; }
      const did = selected;
      busy++; setNotice('info', t('uploading', { name: file.name }));
      Promise.resolve((options.upload || defaultUpload)(file)).then(function (r) {
        const sandbox = r && (r.imported || r.path);
        if (!sandbox) throw new Error('upload returned no path');
        setNotice('info', t('sending'));
        return call('dm.send', { did: did, sandbox: sandbox });
      }).then(function () { setNotice('', ''); }, function (e) { setNotice('error', errText(e)); })
        .then(function () { busy--; return Promise.all([refreshList(), refreshMessages()]); }).then(renderAll);
    });

    function download(m) {
      const did = selected, key = did + '/' + m.id;
      if (downloading.has(key)) return;
      downloading.add(key); setNotice('', ''); renderMessages(true);
      call('dm.download', { did: did, id: m.id }).then(function (r) {
        if (!r || !r.sandbox_path) throw new Error('no file returned');
        savedMsgs.add(key);
        const a = doc.createElement('a');
        a.href = '/api/store/sandbox-download?path=' + encodeURIComponent(r.sandbox_path);
        a.download = String(r.name || m.file.name || 'file');
        a.rel = 'noopener'; a.hidden = true;
        doc.body.appendChild(a); a.click(); a.remove();
      }).catch(function (e) { setNotice('error', errText(e)); })
        .then(function () { downloading.delete(key); renderMessages(true); });
    }

    /* ---------- lifecycle ---------- */
    applyStatic(); lastLang = lang();
    renderAll();
    tick();
    timer = setInterval(tick, POLL_MS);
    doc.addEventListener('visibilitychange', tick);
    if (peers && peers.subscribe) {
      try { unsubscribe = peers.subscribe(function () { if (!destroyed) renderAll(); }); } catch (e) { unsubscribe = null; }
    }

    return {
      refresh: tick,
      open: open,
      destroy: function () {
        destroyed = true; clearInterval(timer);
        doc.removeEventListener('visibilitychange', tick);
        if (typeof unsubscribe === 'function') { try { unsubscribe(); } catch (e) { /* ignore */ } }
        host.textContent = '';
      },
    };
  }

  root.MistlMessenger = { mount: mount, words: words };
})(typeof window !== 'undefined' ? window : globalThis);
