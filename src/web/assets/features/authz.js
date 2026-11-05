/* Access management. The shell owns transport; token secrets live only in a modal. */
(function (global) {
  'use strict';
  const words = {
    en: {
      title: 'Access', help: 'Choose who can use this node and how much they can use.', refresh: 'Refresh',
      ai: 'AI', escrow: 'Escrow', open: 'Open', allowlist: 'Allowlist',
      openHelp: 'Anyone can use this service unless blocked.', allowlistHelp: 'Only allowed peers with a verified identity can use this service.',
      denyHelp: 'Blocks always take priority. Allow rules require a verified identity.',
      allow: 'Allow someone', deny: 'Block someone', limits: 'Limits', mode: 'Change access mode',
      rules: 'Rules', allowed: 'Allowed', blocked: 'Blocked', emptyRules: 'No rules yet.',
      counts: '{allow} allowed · {deny} blocked', more: 'More', remove: 'Remove', overrides: 'Individual limits',
      usage: 'Usage', emptyUsage: 'No usage recorded yet.', account: 'Account', requests: 'Requests', chars: 'Output characters', stored: 'Stored', periodLabel: 'Period',
      reset: 'Reset usage', gauge: 'Stored bytes reflect files on disk. Remove stored files to free space.',
      tokens: 'API tokens', require: 'Require token for local API', apiHelp: 'When enabled, clients need a live Bearer token for the OpenAI-compatible API.',
      create: 'Create token', emptyTokens: 'No API tokens yet.', allModels: 'All models', lastUsed: 'Last used', expires: 'Expires', never: 'Never',
      revoked: 'Revoked', expired: 'Expired', active: 'Active', revoke: 'Revoke', delete: 'Delete',
      next: 'Next', back: 'Back', close: 'Close', confirm: 'Confirm', done: 'Done', copy: 'Copy', copied: 'Copied',
      who: 'Who should this rule apply to?', manual: 'Enter DID / node id', subject: 'DID / node id',
      subjectHelp: 'Use a did:key identity or a 16-character lowercase hexadecimal node ID.',
      invalidSubject: 'Enter a valid did:key identity or 16-character lowercase hexadecimal node ID.',
      duration: 'How long should this last?', forever: 'Forever', day1: '1 day', day7: '7 days', day30: '30 days',
      scope: 'Whose limits do you want to change?', defaults: 'Default for every peer', individual: 'One peer',
      period: 'When should counters reset?', day: 'Daily (UTC)', month: 'Monthly (UTC)', total: 'Never (total)',
      maxRequests: 'How many requests are allowed?', maxChars: 'How many output characters are allowed?',
      maxStored: 'How much can each node store?', unlimited: 'Leave blank for unlimited. Zero disables usage.',
      number: 'Maximum', invalidNumber: 'Enter a whole number of zero or more within the supported range.',
      unlimitedShort: 'Unlimited', perNode: 'per node', tokenName: 'What is this token for?', name: 'Name',
      models: 'Which models can it use?', modelsHelp: 'Choose configured models, or allow all models including future additions.',
      noModels: 'No configured models found. You can allow all models.', selectModels: 'Choose at least one model, or allow all models.',
      nameError: 'Enter a name of 1 to 64 characters.', review: 'Review your changes', service: 'Service', action: 'Action',
      reveal: 'Save your API token', once: 'Shown only once. Copy this token now and keep it somewhere safe.',
      copyError: 'Could not copy. Select the token and copy it manually.', error: 'The request failed. Refresh and try again.',
      resetHelp: 'Reset the AI counters for this account? This also resets enforcement of its usage limit.',
      revokeHelp: 'Revoke this token? Clients using it will lose access immediately.', deleteHelp: 'Delete this token permanently? Clients using it will lose access.',
      removeHelp: 'Remove this entry?', apiChange: 'Change the token requirement?', limitSummary: '{requests} requests · {chars} characters · {period}',
      changed: 'Saved', loading: 'Loading…', defaultLimits: 'Default limits', token: 'Token', modelsError: 'Could not load configured models. Go back and try again.',
      shared: 'Shared access', sharedHelp: 'Give someone signed access they can keep and use until it expires or is revoked.', give: 'Give access',
      given: 'Given by me', held: 'Given to me', presented: 'In use here', emptyCaps: 'No shared access yet.', from: 'From',
      capWho: 'Who should receive access?', manualDid: 'Enter DID', did: 'DID', didHelp: 'Use the recipient’s did:key identity.', invalidDid: 'Enter a valid Ed25519 did:key identity.',
      capWhat: 'What can they use?', infer: 'Use my AI', aiAccess: 'Use AI', read: 'Read stored files', readWrite: 'Read and write stored files', capAction: 'What can they do?',
      capResource: 'Which room or slot?', resource: 'Room or room/slot', resourceHelp: 'Enter a room for all its slots, or room/slot with a 32-character lowercase hexadecimal slot ID.', invalidResource: 'Enter a valid room (up to 96 ASCII characters) or room/slot with a 32-character lowercase hexadecimal slot ID.',
      custom: 'Custom', days: 'Number of days', dayCount: '{days} days', invalidDays: 'Enter a whole number from 1 to 366 days.',
      capLimits: 'Should daily AI usage be limited?', noLimit: 'No limit', setLimit: 'Set daily limits',
      capResult: 'Access issued', delivered: 'Sent to the connected peer.', notDelivered: 'Could not send directly. Copy the access JSON and send it to the recipient.',
      capJson: 'Access JSON', capCopyError: 'Could not copy. Select the JSON and copy it manually.',
      capRevokeHelp: 'Revoke this access? Connected providers will be notified; offline providers may learn about it later.',
      localRevoke: 'Revoke locally', localRevokeHelp: 'Revoke this access on this node immediately?', capRemoveHelp: 'Remove this access from this node?',
      import: 'Import', importTitle: 'Paste shared access JSON', invalidJson: 'Enter a valid capability JSON object (up to 8 KiB).',
      issuers: 'Trusted issuers', issuersHelp: 'These identities may grant access to this service on your behalf.', addIssuer: 'Add trusted issuer', emptyIssuers: 'No additional trusted issuers.',
      issuerWho: 'Who may issue access?', issuerHelp: 'Trust this DID to grant access to this service?', issuerRemoveHelp: 'Stop trusting this issuer? Access issued by this identity will be checked again.'
    },
    ja: {
      title: 'アクセス制御', help: 'このノードを利用できる相手と、利用できる量を設定します。', refresh: '更新',
      ai: 'AI', escrow: 'エスクロー', open: '公開', allowlist: '許可リスト',
      openHelp: 'ブロックした相手以外は誰でも利用できます。', allowlistHelp: '許可した、本人確認済みのピアだけが利用できます。',
      denyHelp: 'ブロックが常に優先されます。許可ルールは本人確認済みの相手に適用されます。',
      allow: '相手を許可', deny: '相手をブロック', limits: '利用上限', mode: 'アクセス方式を変更',
      rules: 'ルール', allowed: '許可', blocked: 'ブロック', emptyRules: 'ルールはありません。',
      counts: '許可 {allow} 件 · ブロック {deny} 件', more: 'その他', remove: '削除', overrides: '個別の上限',
      usage: '利用状況', emptyUsage: '利用記録はまだありません。', account: '利用者', requests: 'リクエスト', chars: '出力文字数', stored: '保存量', periodLabel: '集計期間',
      reset: '利用記録をリセット', gauge: '保存量はディスク上のファイルから集計します。空きを増やすには保存済みファイルを削除してください。',
      tokens: 'API トークン', require: 'ローカル API にトークンを必須にする', apiHelp: '有効にすると、OpenAI 互換 API の利用に有効な Bearer トークンが必要になります。',
      create: 'トークンを作成', emptyTokens: 'API トークンはありません。', allModels: 'すべてのモデル', lastUsed: '最終利用', expires: '有効期限', never: 'なし',
      revoked: '失効済み', expired: '期限切れ', active: '有効', revoke: '失効', delete: '削除',
      next: '次へ', back: '戻る', close: '閉じる', confirm: '確定', done: '完了', copy: 'コピー', copied: 'コピーしました',
      who: 'どの相手に適用しますか？', manual: 'DID / ノード ID を入力', subject: 'DID / ノード ID',
      subjectHelp: 'did:key 形式の ID、または小文字の16進数16文字のノード ID を入力してください。',
      invalidSubject: '有効な did:key 形式の ID、または小文字の16進数16文字のノード ID を入力してください。',
      duration: '有効期間は？', forever: '無期限', day1: '1 日', day7: '7 日', day30: '30 日',
      scope: '誰の利用上限を変更しますか？', defaults: 'すべてのピアの既定値', individual: '特定のピア',
      period: '利用量の集計期間は？', day: '毎日（UTC）', month: '毎月（UTC）', total: 'リセットなし（累計）',
      maxRequests: 'リクエスト数の上限は？', maxChars: '出力文字数の上限は？', maxStored: 'ノードごとの保存量の上限は？',
      unlimited: '空欄で無制限、0 で利用不可になります。', number: '上限', invalidNumber: '対応範囲内の0以上の整数を入力してください。',
      unlimitedShort: '無制限', perNode: 'ノードごと', tokenName: '何に使うトークンですか？', name: '名前',
      models: '利用できるモデルは？', modelsHelp: '設定済みのモデルを選ぶか、今後追加するモデルも含めてすべて許可できます。',
      noModels: '設定済みのモデルがありません。すべてのモデルを許可できます。', selectModels: 'モデルを1つ以上選ぶか、すべて許可してください。',
      nameError: '名前は1〜64文字で入力してください。', review: '変更内容を確認', service: 'サービス', action: '操作',
      reveal: 'API トークンを保存', once: 'この画面で一度だけ表示されます。今すぐコピーして安全に保管してください。',
      copyError: 'コピーできませんでした。トークンを選択して手動でコピーしてください。', error: '処理に失敗しました。更新して再試行してください。',
      resetHelp: 'この利用者の AI 利用記録をリセットしますか？利用上限の判定もリセットされます。',
      revokeHelp: '失効すると、このトークンを使うクライアントは直ちに利用できなくなります。', deleteHelp: 'トークンを完全に削除しますか？使用中のクライアントは利用できなくなります。',
      removeHelp: 'この項目を削除しますか？', apiChange: 'トークンの必須設定を変更しますか？', limitSummary: '{requests} リクエスト · {chars} 文字 · {period}',
      changed: '保存しました', loading: '読み込み中…', defaultLimits: '既定の上限', token: 'トークン', modelsError: '設定済みモデルを取得できませんでした。戻って再試行してください。',
      shared: '権限の共有', sharedHelp: '署名付きの利用権限を渡します。有効期限または失効まで利用できます。', give: '権限を渡す',
      given: '渡した権限', held: '受け取った権限', presented: 'このノードで利用中', emptyCaps: '共有された権限はありません。', from: '発行者',
      capWho: '誰に権限を渡しますか？', manualDid: 'DID を入力', did: 'DID', didHelp: '相手の did:key 形式の ID を入力してください。', invalidDid: '有効な Ed25519 の did:key 形式の ID を入力してください。',
      capWhat: '何を利用できますか？', infer: '自分の AI を利用', aiAccess: 'AI を利用', read: '保存ファイルの読み取り', readWrite: '保存ファイルの読み書き', capAction: 'どの操作を許可しますか？',
      capResource: 'どのルーム・スロットを共有しますか？', resource: 'ルーム、または room/slot', resourceHelp: '全スロットならルーム名、1つだけなら room/slot を入力してください。スロット ID は小文字の16進数32文字です。', invalidResource: '有効なルーム名（半角96文字以内）、または room/slot（小文字の16進数32文字のスロット ID）を入力してください。',
      custom: '指定する', days: '日数', dayCount: '{days} 日', invalidDays: '1〜366 日の整数で入力してください。',
      capLimits: 'AI の1日の利用量を制限しますか？', noLimit: '制限なし', setLimit: '1日の上限を設定',
      capResult: '権限を発行しました', delivered: '接続中の相手に送信しました。', notDelivered: '直接送信できませんでした。権限の JSON をコピーして相手に渡してください。',
      capJson: '権限の JSON', capCopyError: 'コピーできませんでした。JSON を選択して手動でコピーしてください。',
      capRevokeHelp: 'この権限を失効させますか？接続中の提供者に通知します。オフラインの提供者への反映は遅れる場合があります。',
      localRevoke: 'このノードで失効', localRevokeHelp: 'この権限をこのノードで直ちに失効させますか？', capRemoveHelp: '受け取った権限をこのノードから削除しますか？',
      import: 'インポート', importTitle: '共有された権限の JSON を貼り付け', invalidJson: '有効な権限の JSON オブジェクトを入力してください（8 KiB 以下）。',
      issuers: '信頼する発行者', issuersHelp: 'これらの ID は、あなたに代わってこのサービスの利用権限を発行できます。', addIssuer: '発行者を追加', emptyIssuers: '追加の発行者はありません。',
      issuerWho: '誰に権限の発行を任せますか？', issuerHelp: 'この DID に、このサービスの利用権限の発行を許可しますか？', issuerRemoveHelp: 'この発行者の信頼を解除しますか？発行済みの権限も再判定されます。'
    }
  };

  function mount(host, options) {
    const doc = host.ownerDocument, call = options.call, panel = host.closest('.panel');
    let policy = null, tokens = [], usage = {}, peers = [], caps = {}, wizard = null, busy = false, destroyed = false, snapshot = '';
    function lang() { return options.lang ? options.lang() : doc.documentElement.lang; }
    function t(key, vars) {
      let value = (words[lang()] || words.en)[key] || words.en[key] || key;
      Object.keys(vars || {}).forEach(function (k) { value = value.split('{' + k + '}').join(String(vars[k])); });
      return value;
    }
    function el(tag, cls, text) {
      const node = doc.createElement(tag); if (cls) node.className = cls;
      if (text != null) node.textContent = text; return node;
    }
    function btn(key, fn, cls, focusKey) {
      const node = el('button', cls, t(key)); node.type = 'button';
      if (focusKey) node.dataset.authzFocus = focusKey;
      node.addEventListener('click', fn); return node;
    }
    function short(value) { value = String(value || ''); return value.length > 38 ? value.slice(0, 20) + '…' + value.slice(-10) : value; }
    function identity(value) { const node = el('code', 'authz-id', short(value)); node.title = String(value || ''); node.tabIndex = 0; return node; }
    function date(value) { return value && Number.isFinite(Date.parse(value)) ? new Date(value).toLocaleString(lang() || 'en') : t('never'); }
    function capExpiry(value) {
      const remaining = Number(value) - Date.now();
      if (!Number.isFinite(remaining)) return t('never');
      if (remaining <= 0) return t('expired');
      const unit = remaining >= 86400000 ? 'day' : remaining >= 3600000 ? 'hour' : 'minute';
      const scale = unit === 'day' ? 86400000 : unit === 'hour' ? 3600000 : 60000;
      return new Intl.RelativeTimeFormat(lang() || 'en', { numeric: 'always' }).format(Math.ceil(remaining / scale), unit);
    }
    function capWhat(cap) {
      const action = cap.svc === 'ai' ? 'aiAccess' : (cap.act || []).includes('write') ? 'readWrite' : 'read';
      return t(cap.svc) + ' · ' + t(action) + (cap.svc === 'escrow' ? ' · ' + cap.res : '');
    }
    function amount(value) { return value == null ? t('unlimitedShort') : Number(value).toLocaleString(lang() || 'en'); }
    function bytes(value) {
      if (value == null) return t('unlimitedShort');
      const unit = value >= 1073741824 ? 1073741824 : 1048576;
      return Number((value / unit).toFixed(2)).toLocaleString(lang() || 'en') + (unit === 1073741824 ? ' GiB' : ' MiB');
    }
    function limitsText(limits, service) {
      limits = limits || {};
      return service === 'escrow' ? bytes(limits.max_stored_bytes) + ' / ' + t('perNode') : t('limitSummary', {
        requests: amount(limits.max_requests), chars: amount(limits.max_output_chars), period: t(limits.period || 'day')
      });
    }
    function muted(parent, text) { parent.append(el('p', 'authz-muted', text)); }
    const heading = el('div', 'authz-heading'), title = el('h2'), refreshButton = btn('refresh', refresh);
    heading.append(title, refreshButton);
    const intro = el('p', 'authz-muted'), notice = el('p', 'authz-error'), content = el('div', 'authz-content');
    notice.setAttribute('role', 'status'); notice.hidden = true; host.classList.add('authz-root'); host.append(heading, intro, notice, content);
    function error(text) { notice.textContent = text; notice.hidden = !text; }
    function translate() {
      title.textContent = t('title'); intro.textContent = t('help'); refreshButton.textContent = t('refresh');
      doc.querySelectorAll('[data-authz-title]').forEach(function (node) { node.textContent = t('title'); });
      render(); if (wizard && !wizard.busy) showStep(wizard);
    }
    async function refresh() {
      if (busy || destroyed) return;
      busy = true; refreshButton.disabled = true;
      try {
        const data = await Promise.all([call('authz.show', {}), call('authz.usage', {}), call('authz.peers', {}), call('authz.token.list', {}), call('authz.cap.list', {})]);
        if (destroyed) return;
        policy = data[0].policy; usage = data[1] || {}; peers = Array.isArray(data[2]) ? data[2] : []; tokens = Array.isArray(data[3]) ? data[3] : [];
        caps = data[4] || {};
        const next = JSON.stringify(data); if (next !== snapshot) { snapshot = next; render(); }
        error('');
      } catch (_) { if (!destroyed) error(t('error')); }
      finally { busy = false; refreshButton.disabled = false; }
    }
    function section(key) {
      const node = el('section', 'authz-section'), h = el('div', 'authz-heading'); h.append(el('h3', null, t(key))); node.append(h); return node;
    }
    function render() {
      const active = doc.activeElement, focusKey = active && active.dataset.authzFocus;
      const expanded = Array.from(content.querySelectorAll('details[open]')).map(function (n) { return n.dataset.authzDetails; });
      content.replaceChildren();
      if (!policy) { muted(content, t('loading')); return; }
      ['ai', 'escrow'].forEach(function (service) {
        const p = policy[service], card = section(service), head = card.firstChild;
        head.append(btn(p.mode, function () { modeWizard(service); }, 'authz-mode', service + '-mode'));
        muted(card, t(p.mode === 'allowlist' ? 'allowlistHelp' : 'openHelp'));
        muted(card, t('counts', { allow: (p.allow || []).length, deny: (p.deny || []).length }));
        const summary = el('p', 'authz-limit-summary', t('defaultLimits') + ': ' + limitsText(p.limits, service)); card.append(summary);
        const actions = el('div', 'authz-actions');
        ['allow', 'deny'].forEach(function (kind) { actions.append(btn(kind, function () { ruleWizard(service, kind); }, kind === 'allow' ? 'primary' : '', service + '-' + kind)); });
        actions.append(btn('limits', function () { limitsWizard(service); }, '', service + '-limits')); card.append(actions);
        const details = el('details', 'authz-more'); details.dataset.authzDetails = service + '-rules'; details.open = expanded.includes(details.dataset.authzDetails);
        details.append(el('summary', null, t('rules') + ' / ' + t('more'))); muted(details, t('denyHelp'));
        let count = 0;
        ['allow', 'deny'].forEach(function (kind) {
          (p[kind] || []).forEach(function (rule, i) {
            count++; const row = el('div', 'authz-row'), body = el('div', 'authz-row-body');
            body.append(el('strong', 'authz-badge', t(kind === 'allow' ? 'allowed' : 'blocked')), identity(rule.subject));
            muted(body, t('expires') + ': ' + date(rule.expires_at));
            if (rule.note) muted(body, rule.note);
            row.append(body, btn('remove', function () { confirmAction('removeHelp', rule.subject, 'authz.' + kind + '.remove', { service: service, subject: rule.subject }); }, '', service + '-' + kind + i)); details.append(row);
          });
        });
        if (!count) muted(details, t('emptyRules'));
        if ((p.overrides || []).length) {
          details.append(el('h4', null, t('overrides')));
          p.overrides.forEach(function (item, i) {
            const row = el('div', 'authz-row'), body = el('div', 'authz-row-body'); body.append(identity(item.subject)); muted(body, limitsText(item.limits, service));
            row.append(body, btn('limits', function () { limitsWizard(service, item); }, '', service + '-override' + i), btn('remove', function () {
              confirmAction('removeHelp', item.subject, 'authz.limits.set', { service: service, subject: item.subject, limits: null });
            }, '', service + '-remove-override' + i)); details.append(row);
          });
        }
        renderIssuers(details, service, p.issuers || []);
        card.append(details); usageTable(card, service); content.append(card);
      });
      renderCaps(expanded);
      renderTokens();
      if (focusKey) Array.from(content.querySelectorAll('[data-authz-focus]')).some(function (n) { if (n.dataset.authzFocus === focusKey) { n.focus(); return true; } return false; });
    }
    function renderIssuers(parent, service, issuers) {
      parent.append(el('h4', null, t('issuers'))); muted(parent, t('issuersHelp'));
      parent.append(btn('addIssuer', function () {
        const who = subjectStep(true); who.title = 'issuerWho';
        open([who, review(function (w, p) {
          muted(p, t('issuerHelp')); detail(p, 'service', t(service)); detail(p, 'did', w.values.subject);
        })], { peer: 'manual', manual: '' }, function (v) { return call('authz.issuer.add', { service: service, did: v.subject }); });
      }, '', service + '-issuer-add'));
      if (!issuers.length) muted(parent, t('emptyIssuers'));
      issuers.forEach(function (did) {
        const row = el('div', 'authz-row'), body = el('div', 'authz-row-body'); body.append(identity(did));
        row.append(body, btn('remove', function () { confirmAction('issuerRemoveHelp', did, 'authz.issuer.remove', { service: service, did: did }); }, '', service + '-issuer-' + did)); parent.append(row);
      });
    }
    function renderCaps(expanded) {
      const card = section('shared'); card.firstChild.append(btn('give', capWizard, 'primary', 'give-access')); muted(card, t('sharedHelp'));
      ['issued', 'held', 'presented'].forEach(function (kind) {
        card.append(el('h4', null, t(kind === 'issued' ? 'given' : kind)));
        const items = caps[kind] || [];
        if (!items.length) muted(card, t('emptyCaps'));
        items.forEach(function (item) {
          const cap = item.cap || item, row = el('div', 'authz-row'), body = el('div', 'authz-row-body');
          body.append(identity(kind === 'issued' ? cap.aud : kind === 'held' ? cap.fromId : item.did || item.node));
          if (kind === 'held') body.prepend(el('span', 'authz-muted', t('from')));
          const revokedIds = new Set((caps.revoked || []).map(function (v) { return v.id; }));
          let link = cap, revoked = !!item.revoked;
          while (link && !revoked) { revoked = revokedIds.has(link.id); link = link.prf; }
          if (revoked) body.append(el('span', 'authz-badge authz-revoked', t('revoked')));
          muted(body, capWhat(cap));
          if (cap.lim && cap.svc === 'ai') muted(body, limitsText(cap.lim, 'ai'));
          const expiry = el('p', 'authz-muted', t('expires') + ': ' + capExpiry(cap.exp)); expiry.title = new Date(cap.exp).toLocaleString(lang() || 'en'); body.append(expiry);
          const actions = el('div', 'authz-actions');
          if (kind === 'held') actions.append(btn('remove', function () { confirmAction('capRemoveHelp', capWhat(cap), 'authz.cap.remove', { id: cap.id }); }, '', 'cap-remove-' + cap.id));
          else if (!revoked) actions.append(btn(kind === 'issued' ? 'revoke' : 'localRevoke', function () {
            const args = { id: cap.id }; if (kind === 'presented') args.local = true;
            confirmAction(kind === 'issued' ? 'capRevokeHelp' : 'localRevokeHelp', capWhat(cap), 'authz.cap.revoke', args);
          }, '', 'cap-' + kind + '-' + cap.id));
          row.append(body, actions); card.append(row);
        });
      });
      const more = el('details', 'authz-more'); more.dataset.authzDetails = 'shared-more'; more.open = expanded.includes('shared-more');
      more.append(el('summary', null, t('more')), btn('import', importCap, '', 'cap-import')); card.append(more); content.append(card);
    }
    function meter(parent, used, limit, key, storage) {
      const node = el('div', 'authz-meter'); node.append(el('span', null, (storage ? bytes(used) : amount(used || 0)) + ' / ' + (storage ? bytes(limit) : amount(limit))));
      if (limit != null) {
        const bar = el('progress'); bar.max = limit > 0 ? limit : 1; bar.value = Math.min(bar.max, used || 0); bar.setAttribute('aria-label', t(key));
        if (limit === 0 && used > 0) bar.value = 1; node.append(bar);
      }
      parent.append(node);
    }
    function usageTable(parent, service) {
      parent.append(el('h4', 'authz-usage-title', t('usage')));
      const rows = usage[service] || [];
      if (!rows.length) muted(parent, t('emptyUsage'));
      else {
        const wrap = el('div', 'authz-table-wrap'), table = el('table', 'authz-table'), header = el('tr'), thead = el('thead'), tbody = el('tbody');
        const keys = service === 'ai' ? ['account', 'requests', 'chars', 'periodLabel', 'reset'] : ['account', 'stored'];
        keys.forEach(function (key) { const th = el('th', null, t(key)); th.scope = 'col'; header.append(th); }); thead.append(header);
        rows.forEach(function (item) {
          const row = el('tr'), account = el('td'), id = item.account || item.node;
          if (item.name) account.append(el('strong', null, item.name)); account.append(identity(item.did || id)); row.append(account);
          if (service === 'ai') {
            const limits = item.limits || {}, req = el('td'), chars = el('td'), reset = el('td');
            meter(req, item.requests, limits.max_requests, 'requests'); meter(chars, item.output_chars, limits.max_output_chars, 'chars');
            reset.append(btn('reset', function () { confirmAction('resetHelp', id, 'authz.usage.reset', { service: 'ai', account: item.account }); }, '', 'reset-' + id));
            row.append(req, chars, el('td', null, t(item.period || 'day')), reset);
          } else { const stored = el('td'); meter(stored, item.stored_bytes, item.limit, 'stored', true); row.append(stored); }
          tbody.append(row);
        }); table.append(thead, tbody); wrap.append(table); parent.append(wrap);
      }
      if (service === 'escrow') muted(parent, t('gauge'));
    }
    function renderTokens() {
      const card = section('tokens'); card.firstChild.append(btn('create', tokenWizard, 'primary', 'create-token'));
      const label = el('label', 'authz-switch'), toggle = el('input'); toggle.type = 'checkbox'; toggle.setAttribute('role', 'switch'); toggle.checked = !!(policy.api || {}).require_token; toggle.dataset.authzFocus = 'require-token';
      label.append(toggle, el('span', null, t('require'))); card.append(label); muted(card, t('apiHelp'));
      toggle.addEventListener('change', function () {
        const value = toggle.checked; toggle.checked = !value;
        confirmAction('apiChange', t('require') + ': ' + (value ? '✓' : '—'), 'authz.api.set', { require_token: value });
      });
      if (!tokens.length) muted(card, t('emptyTokens'));
      tokens.forEach(function (token) {
        const row = el('div', 'authz-row'), body = el('div', 'authz-row-body');
        const state = token.revoked_at ? 'revoked' : token.expires_at && Date.parse(token.expires_at) <= Date.now() ? 'expired' : 'active';
        body.append(el('strong', null, token.name), el('code', 'authz-id', token.hint), el('span', 'authz-badge authz-' + state, t(state)));
        muted(body, (token.models || []).length ? token.models.join(', ') : t('allModels')); muted(body, limitsText(token.limits, 'ai'));
        muted(body, t('lastUsed') + ': ' + date(token.last_used_at) + ' · ' + t('expires') + ': ' + date(token.expires_at));
        const actions = el('div', 'authz-actions');
        if (!token.revoked_at) actions.append(btn('revoke', function () { confirmAction('revokeHelp', token.name, 'authz.token.revoke', { id: token.id }); }, '', 'revoke-' + token.id));
        actions.append(btn('delete', function () { confirmAction('deleteHelp', token.name, 'authz.token.delete', { id: token.id }); }, '', 'delete-' + token.id));
        row.append(body, actions); card.append(row);
      }); content.append(card);
    }

    function close() {
      const w = wizard; if (!w || w.busy) return;
      wizard = null; w.closed = true; w.secret = ''; w.capResult = null; w.values = {}; w.body.replaceChildren(); w.dialog.close(); w.dialog.remove();
      if (w.opener && w.opener.isConnected) w.opener.focus();
      else if (w.opener && w.opener.dataset.authzFocus) {
        const key = w.opener.dataset.authzFocus;
        Array.from(host.querySelectorAll('[data-authz-focus]')).some(function (node) { if (node.dataset.authzFocus === key) { node.focus(); return true; } return false; });
      }
    }
    function open(steps, values, finish) {
      if (wizard) return;
      const dialog = el('dialog', 'authz-dialog'), header = el('div', 'authz-heading'), heading = el('h3'), body = el('div', 'authz-body'), error = el('p', 'authz-error');
      heading.id = 'authz-wizard-title'; dialog.setAttribute('aria-labelledby', heading.id); error.hidden = true; error.setAttribute('role', 'alert');
      const dismiss = btn('close', close, 'authz-dismiss'); dismiss.textContent = '×'; dismiss.setAttribute('aria-label', t('close'));
      header.append(heading, dismiss); dialog.append(header, body, error); doc.body.append(dialog);
      const w = { dialog: dialog, heading: heading, body: body, error: error, dismiss: dismiss, steps: steps, values: values, finish: finish, index: 0, opener: doc.activeElement, busy: false, closed: false, secret: '' };
      wizard = w; dialog.addEventListener('cancel', function (event) { event.preventDefault(); close(); });
      dialog.addEventListener('click', function (event) {
        if (event.target !== dialog) return;
        const r = dialog.getBoundingClientRect(); if (event.clientX < r.left || event.clientX > r.right || event.clientY < r.top || event.clientY > r.bottom) close();
      });
      dialog.showModal(); showStep(w); return w;
    }
    function showError(w, key) { w.error.textContent = t(key); w.error.hidden = false; }
    function showStep(w) {
      w.body.replaceChildren(); w.error.hidden = true; w.dismiss.setAttribute('aria-label', t('close'));
      if (w.secret) { reveal(w); return; }
      if (w.capResult) { revealCap(w); return; }
      w.heading.textContent = t(w.steps[w.index].title);
      muted(w.body, (w.index + 1) + ' / ' + w.steps.length);
      const form = el('form', 'authz-form'), fields = el('div', 'authz-fields'), actions = el('div', 'authz-actions');
      form.append(fields, actions); w.body.append(form); w.steps[w.index].render(w, fields);
      if (w.index > 0) actions.append(btn('back', function () { w.index--; showStep(w); }));
      const submit = el('button', 'primary', t(w.index === w.steps.length - 1 ? 'confirm' : 'next')); submit.type = 'submit'; actions.append(submit);
      form.addEventListener('submit', function (event) {
        event.preventDefault(); if (w.busy) return;
        if (w.steps[w.index].validate && !w.steps[w.index].validate(w)) return;
        if (w.index < w.steps.length - 1) { w.index++; showStep(w); }
        else run(w);
      });
      const focus = fields.querySelector('input:checked, input, textarea, select, button') || submit; focus.focus();
    }
    async function run(w) {
      w.busy = true; w.error.hidden = true; w.dialog.setAttribute('aria-busy', 'true');
      w.dialog.querySelectorAll('button, input, textarea, select').forEach(function (n) { n.disabled = true; });
      try {
        const result = await w.finish(w.values);
        if (destroyed) return;
        if (result && result.token) { w.secret = String(result.token); w.busy = false; showStep(w); }
        else if (result && result.cap) { w.capResult = result; w.busy = false; showStep(w); }
        else { w.busy = false; close(); }
        await refresh();
      } catch (_) { if (!w.closed) showError(w, 'error'); }
      finally {
        w.busy = false; w.dialog.setAttribute('aria-busy', 'false');
        w.dialog.querySelectorAll('button, input, textarea, select').forEach(function (n) { n.disabled = false; });
      }
    }
    function field(parent, key, value, change, type) {
      const label = el('label', 'authz-field'), input = el('input'); input.type = type || 'text'; input.value = value == null ? '' : value; input.autocomplete = 'off'; input.spellcheck = false;
      label.append(el('span', null, t(key)), input); parent.append(label); input.addEventListener('input', function () { change(input.value); }); return input;
    }
    function choice(parent, key, value, current, change, name) {
      const label = el('label', 'authz-choice'), input = el('input'); input.type = 'radio'; input.name = name || 'authz-choice'; input.value = value; input.checked = value === current;
      label.append(input, el('span', null, key)); parent.append(label); input.addEventListener('change', function () { if (input.checked) change(value); }); return label;
    }
    function detail(parent, key, value) {
      const row = el('div', 'authz-detail'); row.append(el('span', 'authz-muted', t(key)), el('div', null, value)); parent.append(row);
    }
    function review(render) { return { title: 'review', render: render }; }
    function confirmAction(title, value, cmd, args) {
      open([{ title: title, render: function (w, parent) { detail(parent, 'action', value); } }], {}, function () { return call(cmd, args); });
    }
    function modeWizard(service) {
      open([{ title: 'mode', render: function (w, parent) {
        ['open', 'allowlist'].forEach(function (mode) { const row = choice(parent, t(mode), mode, w.values.mode, function (v) { w.values.mode = v; }); row.append(el('small', 'authz-muted', t(mode + 'Help'))); });
      } }, review(function (w, p) { detail(p, 'service', t(service)); detail(p, 'mode', t(w.values.mode)); muted(p, t(w.values.mode + 'Help')); })], { mode: policy[service].mode }, function (v) { return call('authz.mode.set', { service: service, mode: v.mode }); });
    }
    function subjectStep(didOnly) {
      return { title: didOnly ? 'capWho' : 'who', render: function (w, parent) {
        const seen = new Set(); peers.forEach(function (peer) {
          if (seen.has(peer.did)) return; seen.add(peer.did);
          const row = choice(parent, short(peer.did), peer.did, w.values.peer, function (value) { w.values.peer = value; }); row.title = peer.did;
          row.append(el('small', 'authz-muted', peer.node));
        });
        choice(parent, t(didOnly ? 'manualDid' : 'manual'), 'manual', w.values.peer, function (v) { w.values.peer = v; });
        const input = field(parent, didOnly ? 'did' : 'subject', w.values.manual, function (v) { w.values.manual = v; w.values.peer = 'manual'; parent.querySelector('input[value="manual"]').checked = true; });
        input.maxLength = 100; muted(parent, t(didOnly ? 'didHelp' : 'subjectHelp'));
      }, validate: function (w) {
        const subject = w.values.peer === 'manual' ? (w.values.manual || '').trim() : w.values.peer;
        const pattern = didOnly ? /^did:key:z6Mk[1-9A-HJ-NP-Za-km-z]{44}$/ : /^(?:[0-9a-f]{16}|did:key:z6Mk[1-9A-HJ-NP-Za-km-z]{44})$/;
        if (!pattern.test(subject || '')) { showError(w, didOnly ? 'invalidDid' : 'invalidSubject'); return false; }
        w.values.subject = subject;
        if (w.values.loadLimits) { w.values.loadLimits(subject); }
        return true;
      } };
    }
    function durationStep() {
      return { title: 'duration', render: function (w, parent) {
        [['forever', 0], ['day1', 86400], ['day7', 604800], ['day30', 2592000]].forEach(function (item) {
          choice(parent, t(item[0]), item[1], w.values.ttl, function (v) { w.values.ttl = v; });
        });
      } };
    }
    function duration(v) { return t(v.ttl === 86400 ? 'day1' : v.ttl === 604800 ? 'day7' : v.ttl === 2592000 ? 'day30' : 'forever'); }
    function ruleWizard(service, kind) {
      open([subjectStep(), durationStep(), review(function (w, p) {
        detail(p, 'service', t(service)); detail(p, 'action', t(kind)); detail(p, 'subject', w.values.subject); detail(p, 'expires', duration(w.values)); muted(p, t('denyHelp'));
      })], { peer: 'manual', manual: '', ttl: 0 }, function (v) {
        const args = { service: service, subject: v.subject }; if (v.ttl) args.ttl_secs = v.ttl;
        return call('authz.' + kind + '.add', args);
      });
    }
    function periodStep() {
      return { title: 'period', render: function (w, parent) {
        ['day', 'month', 'total'].forEach(function (period) { choice(parent, t(period), period, w.values.limits.period || 'day', function (v) { w.values.limits.period = v; }); });
      } };
    }
    function numberStep(title, key, storage) {
      return { title: title, render: function (w, parent) {
        let value = storage ? w.values.storageValue : w.values.limits[key];
        if (storage && value === undefined) { value = w.values.limits[key] == null ? '' : w.values.limits[key] / 1048576; w.values.unit = 1048576; w.values.storageValue = value; }
        const input = field(parent, 'number', value, function (v) { if (storage) w.values.storageValue = v; else w.values.limits[key] = v; }, 'number');
        input.min = '0'; input.step = storage ? 'any' : '1'; input.max = String(Number.MAX_SAFE_INTEGER);
        if (storage) {
          const label = el('label', 'authz-field'), select = el('select'); label.append(el('span', null, 'MiB / GiB'));
          [[1048576, 'MiB'], [1073741824, 'GiB']].forEach(function (unit) { const option = el('option', null, unit[1]); option.value = unit[0]; option.selected = w.values.unit === unit[0]; select.append(option); });
          select.addEventListener('change', function () { w.values.unit = Number(select.value); }); label.append(select); parent.append(label);
        }
        muted(parent, t('unlimited'));
      }, validate: function (w) {
        const raw = storage ? w.values.storageValue : w.values.limits[key];
        const value = raw === '' || raw == null ? null : Number(raw) * (storage ? w.values.unit : 1);
        if (value != null && (!Number.isSafeInteger(value) || value < 0)) { showError(w, 'invalidNumber'); return false; }
        w.values.limits[key] = value; return true;
      } };
    }
    function aiLimitSteps() { return [periodStep(), numberStep('maxRequests', 'max_requests'), numberStep('maxChars', 'max_output_chars')]; }
    function limitsWizard(service, override) {
      const p = policy[service], values = { scope: override ? 'individual' : 'defaults', peer: override ? override.subject : 'manual', manual: override ? override.subject : '', subject: override && override.subject, limits: Object.assign({}, override ? override.limits : p.limits) };
      values.loadLimits = function (subject) {
        if (values.loadedSubject === subject) return;
        values.loadedSubject = subject;
        const existing = (p.overrides || []).find(function (item) { return item.subject === subject; });
        values.limits = Object.assign({}, existing ? existing.limits : p.limits); delete values.storageValue;
      };
      const scope = { title: 'scope', render: function (w, parent) {
        ['defaults', 'individual'].forEach(function (key) { choice(parent, t(key), key, w.values.scope, function (v) { w.values.scope = v; }); });
      }, validate: function (w) {
        w.steps = [scope].concat(w.values.scope === 'individual' ? [subjectStep()] : [], limitSteps, [summary]);
        if (w.values.scope === 'defaults' && w.values.loadedSubject) { w.values.limits = Object.assign({}, p.limits); delete w.values.storageValue; delete w.values.loadedSubject; }
        return true;
      } };
      const limitSteps = service === 'ai' ? aiLimitSteps() : [numberStep('maxStored', 'max_stored_bytes', true)];
      const summary = review(function (w, parent) { detail(parent, 'service', t(service)); detail(parent, 'account', w.values.scope === 'individual' ? w.values.subject : t('defaults')); detail(parent, 'limits', limitsText(w.values.limits, service)); });
      open([scope].concat(override ? [subjectStep()] : [], limitSteps, [summary]), values, function (v) {
        const args = { service: service, limits: v.limits }; if (v.scope === 'individual') args.subject = v.subject;
        return call('authz.limits.set', args);
      });
    }
    function tokenWizard() {
      const name = { title: 'tokenName', render: function (w, parent) { const input = field(parent, 'name', w.values.name, function (v) { w.values.name = v; }); input.required = true; input.maxLength = 128; }, validate: function (w) {
        w.values.name = w.values.name.trim(); if (!w.values.name || Array.from(w.values.name).length > 64) { showError(w, 'nameError'); return false; } return true;
      } };
      const models = { title: 'models', render: function (w, parent) {
        muted(parent, t('modelsHelp'));
        const all = el('label', 'authz-choice'), toggle = el('input'); toggle.type = 'checkbox'; toggle.checked = w.values.allModels; all.append(toggle, el('span', null, t('allModels'))); parent.append(all);
        const list = el('div', 'authz-models'); parent.append(list);
        function showModels() {
          list.replaceChildren(); if (!w.values.modelsLoaded) { muted(list, t('loading')); return; }
          if (w.values.modelsError) { muted(list, t('modelsError')); return; }
          if (!w.values.availableModels.length) muted(list, t('noModels'));
          w.values.availableModels.forEach(function (model) {
            const row = el('label', 'authz-choice'), box = el('input'); box.type = 'checkbox'; box.checked = w.values.models.includes(model); box.disabled = w.values.allModels;
            box.addEventListener('change', function () { w.values.models = box.checked ? w.values.models.concat([model]) : w.values.models.filter(function (v) { return v !== model; }); });
            row.append(box, el('span', null, model)); list.append(row);
          });
        }
        toggle.addEventListener('change', function () { w.values.allModels = toggle.checked; showModels(); }); showModels();
        if (!w.values.modelsLoaded) {
          Promise.resolve().then(function () { return options.models ? options.models() : []; }).then(function (items) {
            w.values.availableModels = Array.from(new Set((items || []).filter(function (v) { return typeof v === 'string'; }))).sort();
            w.values.modelsLoaded = true; w.values.modelsError = false;
            if (!w.closed && list.isConnected) showModels();
          }).catch(function () { w.values.modelsLoaded = true; w.values.modelsError = true; if (!w.closed && list.isConnected) showModels(); });
        }
      }, validate: function (w) { if (!w.values.allModels && !w.values.models.length) { showError(w, 'selectModels'); return false; } return true; } };
      open([name, models].concat(aiLimitSteps(), [durationStep(), review(function (w, parent) {
        detail(parent, 'name', w.values.name); detail(parent, 'models', w.values.allModels ? t('allModels') : w.values.models.join(', ')); detail(parent, 'limits', limitsText(w.values.limits, 'ai')); detail(parent, 'expires', duration(w.values));
      })]), { name: '', models: [], allModels: true, availableModels: [], limits: { period: 'day' }, ttl: 0 }, function (v) {
        const args = { name: v.name, models: v.allModels ? [] : v.models, limits: v.limits }; if (v.ttl) args.ttl_secs = v.ttl;
        return call('authz.token.create', args);
      });
    }
    function capWizard() {
      const what = { title: 'capWhat', render: function (w, parent) {
        [['ai', 'infer'], ['escrow', 'escrow']].forEach(function (item) { choice(parent, t(item[1]), item[0], w.values.service, function (v) { w.values.service = v; }); });
      }, validate: function (w) {
        w.steps = [subjectStep(true), what].concat(w.values.service === 'escrow' ? [action, resource] : [], [expiry], w.values.service === 'ai' ? [limits] : [], [summary]); return true;
      } };
      const action = { title: 'capAction', render: function (w, parent) {
        ['read', 'readWrite'].forEach(function (key) { choice(parent, t(key), key, w.values.action, function (v) { w.values.action = v; }); });
      } };
      const resource = { title: 'capResource', render: function (w, parent) {
        const input = field(parent, 'resource', w.values.resource, function (v) { w.values.resource = v; }); input.maxLength = 513; muted(parent, t('resourceHelp'));
      }, validate: function (w) {
        const value = w.values.resource.trim(), normalized = capResource({ resource: value });
        if (!/^[A-Za-z0-9_./:-]{1,96}\/(?:[0-9a-f]{32}|\*)$/.test(normalized)) { showError(w, 'invalidResource'); return false; }
        w.values.resource = value; return true;
      } };
      const expiry = { title: 'duration', render: function (w, parent) {
        [['day1', 86400], ['day7', 604800], ['day30', 2592000], ['custom', 'custom']].forEach(function (item) {
          choice(parent, t(item[0]), item[1], w.values.ttl, function (v) { w.values.ttl = v; });
        });
        const input = field(parent, 'days', w.values.days, function (v) { w.values.days = v; w.values.ttl = 'custom'; parent.querySelector('input[value="custom"]').checked = true; }, 'number');
        input.min = '1'; input.max = '366'; input.step = '1';
      }, validate: function (w) {
        if (w.values.ttl === 'custom' && (!Number.isInteger(Number(w.values.days)) || Number(w.values.days) < 1 || Number(w.values.days) > 366)) { showError(w, 'invalidDays'); return false; }
        return true;
      } };
      const requestLimit = numberStep('maxRequests', 'max_requests'), charLimit = numberStep('maxChars', 'max_output_chars');
      const limits = { title: 'capLimits', render: function (w, parent) {
        [['noLimit', false], ['setLimit', true]].forEach(function (item) { choice(parent, t(item[0]), item[1], w.values.limited, function (v) { w.values.limited = v; }); });
      }, validate: function (w) {
        w.steps = [subjectStep(true), what, expiry, limits].concat(w.values.limited ? [requestLimit, charLimit] : [], [summary]); return true;
      } };
      const summary = review(function (w, parent) {
        const v = w.values;
        detail(parent, 'did', v.subject); detail(parent, 'service', t(v.service));
        detail(parent, 'action', t(v.service === 'ai' ? 'infer' : v.action));
        if (v.service === 'escrow') detail(parent, 'resource', capResource(v));
        detail(parent, 'expires', v.ttl === 'custom' ? t('dayCount', { days: v.days }) : duration(v));
        if (v.service === 'ai') detail(parent, 'limits', v.limited ? limitsText(v.limits, 'ai') : t('noLimit'));
      });
      open([subjectStep(true), what, expiry, limits, summary], { peer: 'manual', manual: '', service: 'ai', action: 'read', resource: '', ttl: 604800, days: '', limited: false, limits: { period: 'day' } }, function (v) {
        const args = { aud: v.subject, service: v.service, actions: v.service === 'ai' ? ['infer'] : v.action === 'read' ? ['read'] : ['read', 'write'],
          resource: v.service === 'ai' ? '*' : capResource(v), ttl_secs: v.ttl === 'custom' ? Number(v.days) * 86400 : v.ttl, deliver: true };
        if (v.service === 'ai' && v.limited) args.limits = v.limits;
        return call('authz.cap.issue', args);
      });
    }
    function capResource(values) { return /\/(?:[0-9a-f]{32}|\*)$/.test(values.resource) ? values.resource : values.resource + '/*'; }
    function importCap() {
      open([{ title: 'importTitle', render: function (w, parent) {
        const label = el('label', 'authz-field'), input = el('textarea', 'authz-json'); input.rows = 9; input.value = w.values.json; input.spellcheck = false;
        label.append(el('span', null, t('capJson')), input); parent.append(label); input.addEventListener('input', function () { w.values.json = input.value; });
      }, validate: function (w) {
        try {
          const cap = JSON.parse(w.values.json);
          if (!cap || Array.isArray(cap) || cap.t !== 'mistl-cap-v1' || new TextEncoder().encode(w.values.json).length > 8192) throw new Error();
          w.values.cap = cap; return true;
        } catch (_) { showError(w, 'invalidJson'); return false; }
      } }], { json: '' }, function (v) { return call('authz.cap.import', { cap: v.cap }); });
    }
    function revealCap(w) {
      w.heading.textContent = t('capResult'); muted(w.body, t(w.capResult.delivered ? 'delivered' : 'notDelivered'));
      const json = el('textarea', 'authz-secret authz-json'); json.readOnly = true; json.rows = 10; json.value = JSON.stringify(w.capResult.cap, null, 2); json.setAttribute('aria-label', t('capJson')); json.spellcheck = false; w.body.append(json);
      const actions = el('div', 'authz-actions'), copy = btn('copy', async function () {
        try { await global.navigator.clipboard.writeText(json.value); if (!w.closed) copy.textContent = t('copied'); }
        catch (_) { if (!w.closed) { showError(w, 'capCopyError'); json.focus(); json.select(); } }
      }); actions.append(copy, btn('done', close, 'primary')); w.body.append(actions); json.focus();
    }
    function reveal(w) {
      w.heading.textContent = t('reveal'); muted(w.body, t('once'));
      const secret = el('textarea', 'authz-secret'); secret.readOnly = true; secret.rows = 3; secret.value = w.secret; secret.setAttribute('aria-label', t('token')); secret.spellcheck = false; w.body.append(secret);
      const actions = el('div', 'authz-actions'), copy = btn('copy', async function () {
        try { await global.navigator.clipboard.writeText(w.secret); if (!w.closed) copy.textContent = t('copied'); }
        catch (_) { if (!w.closed) { showError(w, 'copyError'); secret.focus(); secret.select(); } }
      }); actions.append(copy, btn('done', close, 'primary')); w.body.append(actions); secret.focus(); secret.select();
    }
    const languageObserver = new MutationObserver(translate); languageObserver.observe(doc.documentElement, { attributes: true, attributeFilter: ['lang'] });
    const panelObserver = new MutationObserver(function () { if (panel && panel.classList.contains('active')) refresh(); });
    if (panel) panelObserver.observe(panel, { attributes: true, attributeFilter: ['class'] });
    const timer = global.setInterval(function () { if (!wizard && (!panel || panel.classList.contains('active'))) refresh(); }, 15000);
    translate(); if (!panel || panel.classList.contains('active')) refresh();
    return { refresh: refresh, destroy: function () { destroyed = true; if (wizard) { wizard.busy = false; close(); } global.clearInterval(timer); languageObserver.disconnect(); panelObserver.disconnect(); host.replaceChildren(); } };
  }
  global.MistlAuthz = { mount: mount };
})(window);
