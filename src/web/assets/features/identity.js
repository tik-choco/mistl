/* Linked browser identities. The shell supplies transport; secrets stay in the wizard. */
(function (global) {
  'use strict';
  const words = {
    en: {
      title: 'Linked apps', help: 'Browser apps paired here are recognised as the same person.',
      pair: 'Pair a browser app', delegations: 'Delegations', empty: 'No apps linked yet.',
      more: 'More', manual: 'Delegate manually', backup: 'Back up identity', import: 'Import identity',
      copy: 'Copy', copied: 'Copied', refresh: 'Refresh', close: 'Close', next: 'Next', back: 'Back', done: 'Done',
      lifetime: 'How long should this app stay linked?', days: '{n} days',
      codeTitle: 'Enter this code in the app', codeHelp: 'Enter this code in the app’s “Pair with mistl” screen.',
      countdown: 'Code expires in {time}', paired: 'App linked', app: 'App', leaf: 'Browser DID',
      expired: 'Expired', cancelled: 'Pairing cancelled', expiredHelp: 'This code has expired.', again: 'Try again',
      issued: 'Issued', expires: 'Expires', leafTitle: 'Which browser DID do you want to link?',
      invalidLeaf: 'Enter a did:key browser identity.', result: 'Delegation ready',
      backupWarning: 'This file and its passphrase are your identity. Keep both safe and store them separately.',
      passTitle: 'Choose a backup passphrase', passHint: 'Use at least 8 characters.',
      passLabel: 'Passphrase', confirmTitle: 'Confirm your passphrase', confirmLabel: 'Repeat passphrase',
      passShort: 'Use at least 8 characters.', passMismatch: 'The passphrases do not match.',
      download: 'Download backup', downloaded: 'Encrypted backup downloaded',
      importTitle: 'Choose an identity file', json: 'Paste identity JSON', file: 'Or choose a JSON file',
      invalidJson: 'Choose or paste a valid identity JSON object.', tooLarge: 'Identity files must be smaller than 1 MiB.',
      unlockTitle: 'Unlock this identity file', validate: 'Check identity',
      confirmImport: 'Replace this identity?', current: 'Current DID', newDid: 'New DID', none: 'None',
      importWarning: 'Your node ID will change. The old key will be backed up. Restart mistl to use the imported identity.',
      replace: 'Import this identity', same: 'This is already your identity. No changes were made.',
      imported: 'Identity imported', backupDir: 'Old identity backup', restart: 'Restart now',
      restartHelp: 'Restart mistl to use this identity.', restarting: 'Restarting mistl… Reload this page once it is ready.',
      error: 'The request failed. Try again.', secretError: 'Could not process the identity. Check the file or passphrase and try again.',
      copyError: 'Could not copy. Select and copy the text manually.'
    },
    ja: {
      title: '連携アプリ', help: 'ここで連携したブラウザーアプリは、同じユーザーとして認識されます。',
      pair: 'ブラウザーアプリと連携', delegations: '委任一覧', empty: '連携したアプリはまだありません。',
      more: 'その他', manual: '手動で委任', backup: 'ID をバックアップ', import: 'ID をインポート',
      copy: 'コピー', copied: 'コピーしました', refresh: '更新', close: '閉じる', next: '次へ', back: '戻る', done: '完了',
      lifetime: 'このアプリの連携期間は？', days: '{n} 日',
      codeTitle: 'アプリにこのコードを入力', codeHelp: 'アプリの「mistl と連携」画面にこのコードを入力してください。',
      countdown: 'コードの有効期限まで {time}', paired: 'アプリを連携しました', app: 'アプリ', leaf: 'ブラウザーの DID',
      expired: '期限切れ', cancelled: '連携をキャンセルしました', expiredHelp: 'このコードは期限切れです。', again: 'やり直す',
      issued: '発行日時', expires: '有効期限', leafTitle: '連携するブラウザーの DID は？',
      invalidLeaf: 'did:key 形式のブラウザー ID を入力してください。', result: '委任を発行しました',
      backupWarning: 'このファイルとパスフレーズは、あなたの ID そのものです。別々の安全な場所に保管してください。',
      passTitle: 'バックアップ用のパスフレーズは？', passHint: '8 文字以上で入力してください。',
      passLabel: 'パスフレーズ', confirmTitle: 'パスフレーズを確認', confirmLabel: 'もう一度入力',
      passShort: '8 文字以上で入力してください。', passMismatch: 'パスフレーズが一致しません。',
      download: 'バックアップをダウンロード', downloaded: '暗号化したバックアップをダウンロードしました',
      importTitle: 'ID ファイルを選択', json: 'ID の JSON を貼り付け', file: 'または JSON ファイルを選択',
      invalidJson: '有効な ID の JSON オブジェクトを選択または貼り付けてください。', tooLarge: 'ID ファイルは 1 MiB 未満にしてください。',
      unlockTitle: 'ID ファイルのパスフレーズは？', validate: 'ID を確認',
      confirmImport: 'この ID に切り替えますか？', current: '現在の DID', newDid: '新しい DID', none: 'なし',
      importWarning: 'ノード ID が変わります。古い鍵はバックアップされます。インポートした ID を使うには mistl の再起動が必要です。',
      replace: 'この ID をインポート', same: 'すでにこの ID を使用しています。変更はありません。',
      imported: 'ID をインポートしました', backupDir: '古い ID のバックアップ先', restart: '今すぐ再起動',
      restartHelp: 'この ID を使うには mistl を再起動してください。', restarting: 'mistl を再起動しています。準備ができたらページを再読み込みしてください。',
      error: '処理に失敗しました。再試行してください。', secretError: 'ID を処理できませんでした。ファイルやパスフレーズを確認して再試行してください。',
      copyError: 'コピーできませんでした。テキストを選択して手動でコピーしてください。'
    }
  };

  function mount(host, options) {
    const doc = host.ownerDocument;
    const call = options.call;
    let did = '', delegations = [], wizard = null, destroyed = false, refreshing = false, pairingBusy = false;
    function lang() { return options.lang ? options.lang() : doc.documentElement.lang; }
    function t(key, vars) {
      let text = (words[lang()] || words.en)[key] || words.en[key] || key;
      Object.keys(vars || {}).forEach(function (key) { text = text.split('{' + key + '}').join(String(vars[key])); });
      return text;
    }
    function el(tag, cls, text) {
      const node = doc.createElement(tag);
      if (cls) node.className = cls;
      if (text != null) node.textContent = text;
      return node;
    }
    function label(node, key, vars) {
      node.dataset.identityText = key; node.identityVars = vars; node.textContent = t(key, vars); return node;
    }
    function button(key, fn, cls) {
      const node = label(el('button', cls), key); node.type = 'button';
      node.addEventListener('click', fn); return node;
    }
    function paragraph(parent, key, cls) { const node = label(el('p', cls), key); parent.append(node); return node; }
    function message(target, text) { target.textContent = text; target.hidden = !text; }
    function shortDid(value) { return value.length > 30 ? value.slice(0, 18) + '…' + value.slice(-8) : value; }
    async function copy(value, node, error) {
      try { await global.navigator.clipboard.writeText(value); label(node, 'copied'); }
      catch (_) { message(error, t('copyError')); }
    }
    function date(value) {
      const parsed = new Date(value);
      return Number.isNaN(parsed.getTime()) ? '—' : parsed.toLocaleString(lang() || 'en');
    }
    const card = el('div', 'identity-card');
    const heading = el('div', 'identity-heading');
    heading.append(label(el('h3'), 'title'));
    const refreshButton = label(button('refresh', refresh), 'refresh'); heading.append(refreshButton);
    card.append(heading, label(el('p', 'identity-muted'), 'help'));
    const didRow = el('div', 'identity-did-row'), didText = el('code', 'identity-did');
    const notice = el('p', 'identity-error'); notice.hidden = true; notice.setAttribute('role', 'status');
    const copyDid = label(button('copy', function () { copy(did, copyDid, notice); }), 'copy'); copyDid.disabled = true;
    didRow.append(didText, copyDid); card.append(didRow);
    const pairButton = label(button('pair', function () { open('pair'); }, 'primary'), 'pair'); card.append(pairButton);
    const listHeading = el('div', 'identity-heading identity-list-heading');
    listHeading.append(label(el('h4'), 'delegations')); card.append(listHeading);
    const list = el('div', 'identity-delegations'); card.append(list);
    const more = el('details', 'identity-more'), actions = el('div', 'identity-actions');
    more.append(label(el('summary'), 'more'));
    ['manual', 'backup', 'import'].forEach(function (kind) {
      actions.append(label(button(kind, function () { open(kind); }), kind));
    });
    more.append(actions); card.append(more, notice); host.append(card);

    function renderList() {
      list.replaceChildren();
      if (!delegations.length) { paragraph(list, 'empty', 'identity-muted'); return; }
      delegations.forEach(function (item) {
        const row = el('div', 'identity-delegation'), leaf = String(item.leaf || '');
        const name = el('code', 'identity-leaf', shortDid(leaf)); name.title = leaf; name.tabIndex = 0; row.append(name);
        if (item.expired || Date.parse(item.exp) <= Date.now()) row.append(label(el('span', 'identity-expired'), 'expired'));
        row.append(el('small', 'identity-muted', t('issued') + ': ' + date(item.iat) + ' · ' + t('expires') + ': ' + date(item.exp)));
        list.append(row);
      });
    }
    async function refresh() {
      if (refreshing || destroyed) return;
      refreshing = true; refreshButton.disabled = true; message(notice, '');
      try {
        const result = await Promise.all([call('key.did', {}), call('key.delegations', {})]);
        if (destroyed) return;
        did = String(result[0].did || ''); didText.textContent = did; copyDid.disabled = !did;
        delegations = Array.isArray(result[1]) ? result[1] : []; renderList();
      } catch (_) { message(notice, t('error')); }
      finally { refreshing = false; refreshButton.disabled = false; }
    }

    function stopTimers(w) { clearTimeout(w.poll); clearInterval(w.tick); }
    async function cancelPair(w) {
      try { await call('key.pair.cancel', {}); }
      catch (_) { if (!destroyed) message(notice, t('error')); }
      finally { pairingBusy = false; pairButton.disabled = false; }
    }
    function close() {
      const w = wizard; if (!w) return;
      wizard = null; stopTimers(w); w.closed = true;
      w.data = null; w.passphrase = ''; w.dialog.querySelectorAll('input, textarea').forEach(function (input) { input.value = ''; });
      if (w.waiting) { w.waiting = false; pairingBusy = true; pairButton.disabled = true; cancelPair(w); }
      w.dialog.close(); w.dialog.remove();
      if (w.opener && w.opener.isConnected) w.opener.focus();
    }
    function open(kind) {
      if (wizard || (kind === 'pair' && pairingBusy)) return;
      const dialog = el('dialog', 'identity-dialog'); dialog.setAttribute('aria-labelledby', 'identity-wizard-title');
      const header = el('div', 'identity-heading'), title = el('h3'); title.id = 'identity-wizard-title';
      const dismiss = button('close', close, 'identity-dismiss'); dismiss.removeAttribute('data-identity-text');
      dismiss.textContent = '×'; dismiss.setAttribute('aria-label', t('close'));
      header.append(title, dismiss);
      const body = el('div', 'identity-body'), error = el('p', 'identity-error'); error.hidden = true; error.setAttribute('role', 'alert');
      dialog.append(header, body, error); doc.body.append(dialog);
      wizard = { kind: kind, dialog: dialog, title: title, body: body, error: error, ttl: '60d', leaf: '',
        passphrase: '', data: null, waiting: false, closed: false, busy: false, opener: doc.activeElement };
      dialog.addEventListener('cancel', function (event) { event.preventDefault(); close(); });
      dialog.addEventListener('click', function (event) {
        if (event.target !== dialog) return;
        const r = dialog.getBoundingClientRect();
        if (event.clientX < r.left || event.clientX > r.right || event.clientY < r.top || event.clientY > r.bottom) close();
      });
      dialog.showModal();
      if (kind === 'pair') lifetime(wizard);
      if (kind === 'manual') leafStep(wizard);
      if (kind === 'backup') backupPass(wizard);
      if (kind === 'import') importSource(wizard);
    }
    function step(w, key) {
      label(w.title, key); w.body.replaceChildren(); message(w.error, '');
      w.body.setAttribute('aria-busy', 'false');
    }
    function focus(w) { const input = w.body.querySelector('input, textarea, select, button'); if (input) input.focus(); }
    function form(w, submitKey, action, back) {
      const node = el('form', 'identity-form'), fields = el('div', 'identity-fields'), footer = el('div', 'identity-actions');
      if (back) footer.append(button('back', back));
      const submit = label(el('button', 'primary'), submitKey); submit.type = 'submit'; footer.append(submit);
      node.append(fields, footer); w.body.append(node);
      node.addEventListener('submit', function (event) { event.preventDefault(); run(w, action); });
      return fields;
    }
    async function run(w, action) {
      if (w.busy || w.closed) return;
      w.busy = true; message(w.error, ''); w.body.setAttribute('aria-busy', 'true');
      const buttons = Array.from(w.body.querySelectorAll('button')); buttons.forEach(function (b) { b.disabled = true; });
      try { await action(); }
      catch (_) { if (!w.closed) message(w.error, t(w.kind === 'backup' || w.kind === 'import' ? 'secretError' : 'error')); }
      finally {
        w.busy = false; w.body.setAttribute('aria-busy', 'false'); buttons.forEach(function (b) { b.disabled = false; });
      }
    }
    function inputField(parent, key, type, value) {
      const wrap = el('label', 'identity-field'), input = el(type === 'textarea' ? 'textarea' : 'input');
      wrap.append(label(el('span'), key)); if (type !== 'textarea') input.type = type;
      input.value = value || ''; input.autocomplete = 'off'; input.spellcheck = false;
      wrap.append(input); parent.append(wrap); return input;
    }
    function lifetime(w) {
      step(w, 'lifetime');
      const fields = form(w, 'next', async function () {
        w.ttl = fields.querySelector('input:checked').value;
        if (w.kind === 'pair') await startPair(w);
        else {
          const result = await call('key.delegate', { leaf: w.leaf, ttl: w.ttl });
          if (w.closed) return;
          step(w, 'result'); const json = JSON.stringify(result, null, 2);
          w.body.append(el('pre', 'identity-json', json));
          const copyResult = button('copy', function () { copy(json, copyResult, w.error); });
          w.body.append(copyResult, button('done', close)); refresh(); focus(w);
        }
      }, w.kind === 'manual' ? function () { leafStep(w); } : null);
      [30, 60, 90, 365].forEach(function (days) {
        const choice = el('label', 'identity-choice'), radio = el('input'); radio.type = 'radio';
        radio.name = 'identity-ttl'; radio.value = days + 'd'; radio.checked = radio.value === w.ttl;
        choice.append(radio, label(el('span'), 'days', { n: days })); fields.append(choice);
      }); focus(w);
    }
    async function startPair(w) {
      pairingBusy = true; pairButton.disabled = true;
      let result;
      try {
        result = await call('key.pair.start', { ttl: w.ttl });
        if (w.closed) { await cancelPair(w); return; }
      } finally { pairingBusy = false; pairButton.disabled = false; }
      w.waiting = true; w.expires = result.expires_at;
      step(w, 'codeTitle');
      const code = String(result.formatted_code || result.code || '');
      w.body.append(el('div', 'identity-pair-code', code));
      const copyCode = button('copy', function () { copy(code, copyCode, w.error); }); w.body.append(copyCode);
      paragraph(w.body, 'codeHelp', 'identity-muted');
      const countdown = el('p', 'identity-countdown'); w.body.append(countdown);
      function tick() {
        const seconds = Math.max(0, Math.ceil((Date.parse(w.expires) - Date.now()) / 1000));
        countdown.textContent = Number.isFinite(seconds) ? t('countdown', { time: Math.floor(seconds / 60) + ':' + String(seconds % 60).padStart(2, '0') }) : '';
      }
      tick(); w.tick = setInterval(tick, 1000);
      async function poll() {
        if (w.closed || !w.waiting) return;
        try {
          const status = await call('key.pair.status', {});
          if (w.closed || !w.waiting) return;
          message(w.error, '');
          if (status.status === 'issued') {
            w.waiting = false; stopTimers(w); step(w, 'paired');
            detail(w, 'app', status.app || '—'); detail(w, 'leaf', status.leaf || '—');
            w.body.append(button('done', close, 'primary')); refresh(); focus(w); return;
          }
          if (status.status === 'expired' || status.status === 'cancelled' || status.status === 'none') {
            w.waiting = false; stopTimers(w); step(w, status.status === 'cancelled' ? 'cancelled' : 'expired');
            if (status.status !== 'cancelled') paragraph(w.body, 'expiredHelp');
            w.body.append(button('again', function () { lifetime(w); }, 'primary')); focus(w); return;
          }
        } catch (_) { if (!w.closed) message(w.error, t('error')); }
        if (!w.closed && w.waiting) w.poll = setTimeout(poll, 1500);
      }
      w.poll = setTimeout(poll, 1500); focus(w);
    }
    function detail(w, key, value) {
      const field = el('div', 'identity-detail'); field.append(label(el('span', 'identity-muted'), key), el('code', null, String(value)));
      w.body.append(field);
    }
    function leafStep(w) {
      step(w, 'leafTitle');
      const fields = form(w, 'next', function () {
        w.leaf = input.value.trim();
        if (!/^did:key:z[1-9A-HJ-NP-Za-km-z]+$/.test(w.leaf)) { message(w.error, t('invalidLeaf')); input.focus(); return; }
        lifetime(w);
      });
      const input = inputField(fields, 'leaf', 'text', w.leaf); input.required = true; focus(w);
    }
    function backupPass(w) {
      w.passphrase = ''; step(w, 'passTitle'); paragraph(w.body, 'backupWarning', 'identity-warning');
      const fields = form(w, 'next', function () {
        if (Array.from(input.value).length < 8) { message(w.error, t('passShort')); input.focus(); return; }
        w.passphrase = input.value; input.value = ''; backupConfirm(w);
      });
      const input = inputField(fields, 'passLabel', 'password'); input.required = true; input.autocomplete = 'new-password';
      paragraph(fields, 'passHint', 'identity-muted'); focus(w);
    }
    function backupConfirm(w) {
      step(w, 'confirmTitle');
      const fields = form(w, 'download', async function () {
        if (input.value !== w.passphrase) { message(w.error, t('passMismatch')); input.focus(); return; }
        const result = await call('key.export', { passphrase: w.passphrase });
        input.value = ''; w.passphrase = '';
        if (w.closed) return;
        const blob = new Blob([JSON.stringify(result.envelope, null, 2) + '\n'], { type: 'application/json' });
        const url = URL.createObjectURL(blob), link = el('a'); link.href = url;
        link.download = 'mistl-identity-' + String(result.did).replace(/^did:key:z/, '').slice(0, 8) + '.json';
        doc.body.append(link); link.click(); link.remove(); setTimeout(function () { URL.revokeObjectURL(url); }, 1000);
        step(w, 'downloaded'); paragraph(w.body, 'backupWarning', 'identity-warning'); w.body.append(button('done', close, 'primary')); focus(w);
      }, function () { backupPass(w); });
      const input = inputField(fields, 'confirmLabel', 'password'); input.required = true; input.autocomplete = 'new-password'; focus(w);
    }
    function importSource(w) {
      w.data = null; w.passphrase = ''; step(w, 'importTitle');
      const fields = form(w, 'next', async function () {
        if (input.value.length >= 1024 * 1024) { message(w.error, t('tooLarge')); return; }
        let data;
        try { data = JSON.parse(input.value); } catch (_) { message(w.error, t('invalidJson')); return; }
        if (!data || typeof data !== 'object' || Array.isArray(data)) { message(w.error, t('invalidJson')); return; }
        w.data = data;
        if (Object.prototype.hasOwnProperty.call(data, 'cipherText')) { input.value = ''; file.value = ''; importPass(w); }
        else await checkImport(w);
      });
      const input = inputField(fields, 'json', 'textarea'); input.rows = 7;
      const file = inputField(fields, 'file', 'file'); file.accept = '.json,application/json';
      file.addEventListener('change', function () {
        const selected = file.files[0];
        if (!selected) return;
        run(w, async function () {
          if (selected.size >= 1024 * 1024) { message(w.error, t('tooLarge')); return; }
          const text = await selected.text(); if (!w.closed) input.value = text;
        });
      }); focus(w);
    }
    function importPass(w) {
      step(w, 'unlockTitle');
      const fields = form(w, 'validate', async function () { w.passphrase = input.value; await checkImport(w); }, function () { importSource(w); });
      const input = inputField(fields, 'passLabel', 'password'); input.required = true; focus(w);
    }
    function importArgs(w) {
      const args = { data: w.data }; if (w.passphrase) args.passphrase = w.passphrase; return args;
    }
    async function checkImport(w) {
      const result = await call('key.import', Object.assign(importArgs(w), { dry_run: true }));
      if (w.closed) return;
      if (!result.changed) {
        w.data = null; w.passphrase = ''; step(w, 'import'); paragraph(w.body, 'same'); w.body.append(button('done', close, 'primary')); focus(w); return;
      }
      step(w, 'confirmImport'); detail(w, 'current', result.previous_did || t('none')); detail(w, 'newDid', result.did);
      paragraph(w.body, 'importWarning', 'identity-warning');
      form(w, 'replace', async function () {
        const imported = await call('key.import', Object.assign(importArgs(w), { force: true }));
        w.data = null; w.passphrase = '';
        if (w.closed) return;
        step(w, 'imported'); detail(w, 'newDid', imported.did);
        if (imported.backup_dir) detail(w, 'backupDir', imported.backup_dir);
        if (imported.restart_required) {
          paragraph(w.body, 'restartHelp', 'identity-warning');
          const restart = button('restart', function () {
            run(w, async function () {
              await call('daemon.restart', {}); if (w.closed) return;
              step(w, 'imported'); paragraph(w.body, 'restarting'); w.body.append(button('done', close)); focus(w);
            });
          }, 'primary'); w.body.append(restart);
        }
        w.body.append(button('done', close)); focus(w);
      }, function () { importSource(w); }); focus(w);
    }
    const observer = new MutationObserver(function () {
      [card, wizard && wizard.dialog].filter(Boolean).forEach(function (root) {
        root.querySelectorAll('[data-identity-text]').forEach(function (node) { node.textContent = t(node.dataset.identityText, node.identityVars); });
      });
      if (wizard) wizard.dialog.querySelector('.identity-dismiss').setAttribute('aria-label', t('close'));
      renderList();
    });
    observer.observe(doc.documentElement, { attributes: true, attributeFilter: ['lang'] });
    renderList();
    // Load only on entry to Profile; opening another panel never creates an identity.
    const panel = host.closest('.panel');
    const panelObserver = new MutationObserver(function () { if (panel.classList.contains('active')) refresh(); });
    if (panel) { panelObserver.observe(panel, { attributes: true, attributeFilter: ['class'] }); if (panel.classList.contains('active')) refresh(); }
    return { refresh: refresh, destroy: function () { destroyed = true; close(); observer.disconnect(); panelObserver.disconnect(); host.replaceChildren(); } };
  }
  global.MistlIdentity = { mount: mount };
})(window);
