(function (root) {
  'use strict';
  const words = {
    ja: {
      title:'接続を組み立てる', help:'端子どうしをドラッグ（または順にクリック）してノードを結びます。線をクリックすると詳細を表示します。',
      stopped:'Room ID で接続すると、ここにノードが表示されます。', self:'このノード', peer:'リモートノード', legacy:'グラフ未対応・状態待ち',
      broadcast:'Broadcast', broadcastHelp:'ON: ルーム全体から接続を受付。OFF: 線で結んだ相手だけ。各転送の承認は別途必要です。TCP のデータ複製ではありません。',
      lock:'編集ロック', lockHelp:'他ノードによる線・転送設定の変更を止めます。既存の通信は続きます。所有者は編集できます。',
      locked:'ロック中', settings:'ノードの設定',
      add:'＋ ポート転送', addTitle:'ポート転送を追加', empty:'ポート転送はまだありません', remove:'削除', permissions:'接続・編集権限', connect:'接続端子', choose:'接続先の端子を選択してください（Esc で取消）',
      save:'追加', direct:'直接接続', room:'ルーム公開', pending:'片側のみ設定済み', disconnect:'線を削除',
      permissionHelp:'このノードに対する操作を、相手ごとに許可します。通信自体には従来の転送承認も適用されます。',
      links:'このノードにつながる線を変更', addGrant:'ポート転送を追加（ローカルサービス公開を含む）', removeGrant:'ポート転送を削除',
      peerId:'相手の Node ID', grant:'権限を設定', close:'閉じる', reset:'配置を整える', owner:'所有者のみ変更できます',
      unavailable:'相手の状態を取得できていません。相手も対応版で接続してください。', denied:'編集権限がありません。相手側の「接続・編集権限」で許可してください。',
      applied:'設定を反映しました', failed:'変更できませんでした', partial:'片側の変更後に失敗しました。表示を確認して再試行してください。',
      broadcastDisconnect:'Broadcast が ON のノードがあります。線を削除してもルーム公開による通信は続きます。限定するには各ノードで Broadcast を OFF にしてください。',
      legacyForward:'旧形式の転送（このグラフからは削除できません）', waiting:'相手の確認を待っています…',
      error:'エラー', conns:'接続', removeConfirm:'「{target}」への転送を削除しますか？',
      back:'戻る', next:'次へ', step:'{n} / {total}',
      askType:'何をしますか？',
      typePropose:'相手側のポートを使う', typeProposeHelp:'相手側の 127.0.0.1:22 などを、このノードのポートとして使えるようにします。相手の承認が必要です。',
      typeConnect:'相手が公開したサービスに接続', typeConnectHelp:'相手がすでに公開しているサービスを、このノードのポートで受けます。',
      typeServe:'このノードのサービスを公開', typeServeHelp:'このノードで動いているサービスを、ルームの相手が使えるようにします。',
      askPeer:'どのノードへ転送しますか？', noPeers:'ルームに相手のノードがいません。',
      askRemote:'相手側のどのアドレスへ転送しますか？', remoteHint:'相手のノードから見たアドレスです（例: SSH なら 127.0.0.1:22）。',
      askLocal:'このノードのどのアドレスで受けますか？', localHint:'ここに接続すると、通信が相手側へ転送されます。',
      askService:'どのサービスに接続しますか？', noService:'公開されているサービスがありません。先に相手のノードでサービスを公開してください。',
      askServe:'公開するサービスのアドレスは？', serveHint:'このノードで待ち受けているサービスです（例: Web サーバーなら 127.0.0.1:8080）。',
      askConfirm:'この内容で追加しますか？', published:'ルームに公開', sendProposal:'提案を送る',
      proposeNote:'相手が承認すると転送が始まります。', proposeSent:'相手に提案しました。相手が受け入れると転送が始まります。',
      badAddr:'「ホスト:ポート」の形式で入力してください（例: 127.0.0.1:8080）。', proto:'プロトコル',
      portBusy:'このアドレスは他のプログラムが使用中です。別のポートを指定してください。', portUnusable:'このアドレスでは待ち受けできません', useSuggestion:'{addr} を使う',
      waitAccept:'{peer} の承認待ち', waitApprove:'{peer} の接続承認待ち', deniedBy:'{peer} が接続を拒否しました',
      cancel:'提案を取り消す', cancelled:'提案を取り消しました。', proposalWire:'承認待ち', toPeer:'→ {peer}（{addr}）',
      nDenied:'{peer} が提案を断りました（{target}）。', nTimeout:'{peer} から応答がなく、提案がタイムアウトしました（{target}）。',
      nPeerLeft:'{peer} が応答する前に退出しました（{target}）。', nFailed:'提案が失敗しました（{target}）: {detail}',
      nAccepted:'{peer} が承認しました。{local} への接続が {target} へ転送されます。', nAddFailed:'{peer} は承認しましたが、このノードでポートを開けませんでした: {detail}',
    },
    en: {
      title:'Build your connections', help:'Drag between ports (or click two ports in turn) to link nodes. Click a link for details.',
      stopped:'Connect with a Room ID to show nodes here.', self:'This node', peer:'Remote node', legacy:'Graph unsupported or waiting for state',
      broadcast:'Broadcast', broadcastHelp:'ON accepts connections from the room. OFF limits traffic to linked peers. Forward approval still applies. TCP data is not duplicated.',
      lock:'Edit lock', lockHelp:'Stops remote changes to links and forwards. Existing traffic continues. The owner can still edit.',
      locked:'Locked', settings:'Node settings',
      add:'+ Port forward', addTitle:'Add a port forward', empty:'No port forwards yet', remove:'Remove', permissions:'Connection permissions', connect:'Connection port', choose:'Choose the destination port (Esc to cancel)',
      save:'Add', direct:'Direct link', room:'Room broadcast', pending:'One side configured', disconnect:'Remove link',
      permissionHelp:'Grant each peer permission to modify this node. Existing forward authorization still applies to traffic.',
      links:'Change links involving this node', addGrant:'Add forwards (including publishing local services)', removeGrant:'Remove forwards',
      peerId:'Peer Node ID', grant:'Set permissions', close:'Close', reset:'Arrange nodes', owner:'Only the owner can change this',
      unavailable:'Peer graph is unavailable. Connect the peer using a compatible version.', denied:'Permission required. Ask the peer owner to grant access in Connection permissions.',
      applied:'Changes applied', failed:'Could not apply change', partial:'One side changed before an error. Inspect the graph before retrying.',
      broadcastDisconnect:'Broadcast is enabled on a node. Removing the explicit link leaves room traffic enabled. Turn Broadcast OFF on each node to restrict it.',
      legacyForward:'Legacy forward (cannot be removed from the graph)', waiting:'Waiting for the peer to confirm…',
      error:'Error', conns:'conns', removeConfirm:'Remove the forward to "{target}"?',
      back:'Back', next:'Next', step:'{n} / {total}',
      askType:'What do you want to do?',
      typePropose:'Use a port on a peer', typeProposeHelp:'Make an address on the peer, such as 127.0.0.1:22, reachable through a port on this node. The peer must accept.',
      typeConnect:'Connect to a service a peer published', typeConnectHelp:'Receive a service the peer already published on a port of this node.',
      typeServe:'Publish a service on this node', typeServeHelp:'Let peers in the room use a service running on this node.',
      askPeer:'Which node should receive the traffic?', noPeers:'There are no other nodes in the room.',
      askRemote:'Which address on the peer?', remoteHint:'As seen from the peer (for SSH, 127.0.0.1:22).',
      askLocal:'Which address on this node should receive?', localHint:'Connections to this address are forwarded to the peer.',
      askService:'Which service do you want to connect to?', noService:'No services are published. Publish a service on the peer first.',
      askServe:'What is the address of the service?', serveHint:'A service listening on this node (for a web server, 127.0.0.1:8080).',
      askConfirm:'Add this forward?', published:'Published to the room', sendProposal:'Send proposal',
      proposeNote:'The forward starts once the peer accepts.', proposeSent:'Proposal sent. The forward starts once the peer accepts.',
      badAddr:'Enter host:port (for example 127.0.0.1:8080).', proto:'Protocol',
      portBusy:'Another program is using this address. Choose a different port.', portUnusable:'Cannot listen on this address', useSuggestion:'Use {addr}',
      waitAccept:'Waiting for {peer} to accept', waitApprove:'Waiting for {peer} to approve the connection', deniedBy:'{peer} denied the connection',
      cancel:'Cancel proposal', cancelled:'Proposal cancelled.', proposalWire:'Waiting for approval', toPeer:'→ {peer} ({addr})',
      nDenied:'{peer} denied your proposal for {target}.', nTimeout:'{peer} did not answer your proposal for {target} (timed out).',
      nPeerLeft:'{peer} left before answering your proposal for {target}.', nFailed:'Proposal for {target} failed: {detail}',
      nAccepted:'{peer} accepted. Connections to {local} are now forwarded to {target}.', nAddFailed:'{peer} accepted, but this node could not open the port: {detail}',
    },
    // Only the strings added after the first release; the rest fall back to English.
    zh: {
      waitAccept:'等待 {peer} 接受', waitApprove:'等待 {peer} 批准连接', deniedBy:'{peer} 拒绝了连接',
      cancel:'取消提议', cancelled:'提议已取消。', proposalWire:'等待批准', toPeer:'→ {peer}（{addr}）',
      nDenied:'{peer} 拒绝了你的提议（{target}）。', nTimeout:'{peer} 未回应，提议已超时（{target}）。',
      nPeerLeft:'{peer} 在回应之前已离开（{target}）。', nFailed:'提议失败（{target}）：{detail}',
      nAccepted:'{peer} 已接受。到 {local} 的连接将转发到 {target}。', nAddFailed:'{peer} 已接受，但本节点无法打开端口：{detail}',
    }
  };
  const ADDR = /^\S+:\d{1,5}$/;
  let instances = 0;

  function create(options) {
    const doc = options.document, host = options.host;
    let noticeSeen = null, subscribed = false;
    let snapshot = null, models = [], positions = {}, source = null, drag = null, busy = false, signature = '', room = '', language = '';
    const statsEls = new Map();
    function statKey(f) { return f.target || f.addr; }
    function formatBytes(n) {
      n = Number(n) || 0; const units = ['B','KB','MB','GB','TB']; let i = 0;
      while (n >= 1024 && i < units.length - 1) { n /= 1024; i++; }
      return (i === 0 ? String(n) : n.toFixed(1)) + ' ' + units[i];
    }
    function statsText(f) { return (f.state || '') + ' · ' + (f.active_conns || 0) + ' ' + t('conns') + ' · ↑' + formatBytes(f.bytes_out) + ' ↓' + formatBytes(f.bytes_in); }
    function updateStats(forwards) { (forwards || []).forEach(f => { const e = statsEls.get(statKey(f)); if (e) e.textContent = statsText(f); }); }
    const lang = () => { const l = String(doc.documentElement.lang || '').slice(0, 2); return l === 'ja' || l === 'zh' ? l : 'en'; };
    const t = key => (words[lang()][key] || words.en[key] || key);
    const fmt = (key, vars) => t(key).replace(/\{(\w+)\}/g, (m, name) => (vars && vars[name] != null ? String(vars[name]) : m));
    // Peer names are self-claimed display text; ids stay visible next to them.
    const peersApi = () => root.MistlPeers && typeof root.MistlPeers.label === 'function' ? root.MistlPeers : null;
    const peerName = id => { const p = peersApi(); const peer = p && p.byNode && p.byNode(id); return peer && peer.name ? p.label(id) : ''; };
    const peerLabel = id => peerName(id) || id;
    const peerText = id => { const name = peerName(id); return name && name !== id ? name + ' (' + id + ')' : id; };
    const el = (tag, cls, text) => { const e = doc.createElement(tag); if (cls) e.className = cls; if (text != null) e.textContent = text; return e; };
    const button = (text, cls, action) => { const e = el('button', cls, text); e.type = 'button'; e.addEventListener('click', action); return e; };
    const line = el('div', 'tg-toolbar'), title = el('h3'), hint = el('p', 'tg-help');
    const message = el('div', 'tg-message'); message.setAttribute('role', 'status'); message.setAttribute('aria-live', 'polite');
    const viewport = el('div', 'tg-viewport'), canvas = el('div', 'tg-canvas');
    const svg = doc.createElementNS('http://www.w3.org/2000/svg', 'svg'); svg.classList.add('tg-wires'); svg.setAttribute('aria-label', 'Connections');
    const nodes = el('div', 'tg-nodes');
    const arrange = button('', '', () => { positions = {}; savePositions(); drawNodes(); });
    line.append(title, arrange); canvas.append(svg, nodes); viewport.append(canvas); host.append(line, hint, message, viewport);

    // Every editor (forward wizard, node settings, link details) opens in one
    // modal. It hangs off <body> so a transformed dashboard panel can never
    // become the containing block of the fixed overlay.
    const overlay = el('div', 'modal-overlay tg-modal'); overlay.hidden = true;
    const dialog = el('div', 'modal-panel tg-dialog'); dialog.setAttribute('role', 'dialog'); dialog.setAttribute('aria-modal', 'true');
    const dialogTitle = el('h3'); dialogTitle.id = 'tg-dialog-title-' + (++instances); dialog.setAttribute('aria-labelledby', dialogTitle.id);
    const dialogHead = el('div', 'modal-header'), dialogClose = button('×', 'modal-close', () => closeModal());
    const dialogBody = el('div', 'tg-dialog-body'), dialogStatus = el('p', 'tg-dialog-status'); dialogStatus.setAttribute('role', 'status');
    dialogHead.append(dialogTitle, dialogClose); dialog.append(dialogHead, dialogBody, dialogStatus); overlay.append(dialog);
    (doc.body || host).append(overlay);
    let opener = null, wizard = null;
    overlay.addEventListener('pointerdown', e => { if (e.target === overlay) closeModal(); });
    overlay.addEventListener('keydown', e => {
      if (e.key === 'Escape') { e.preventDefault(); e.stopPropagation(); closeModal(); return; }
      if (e.key !== 'Tab') return;
      const items = focusables(); if (!items.length) return;
      const first = items[0], last = items[items.length - 1];
      if (e.shiftKey && doc.activeElement === first) { e.preventDefault(); last.focus(); }
      else if (!e.shiftKey && doc.activeElement === last) { e.preventDefault(); first.focus(); }
    });
    function focusables() { return [...dialog.querySelectorAll('button,input,select,[tabindex]')].filter(e => !e.disabled && !e.closest('[hidden]')); }
    function modal(titleText) {
      if (overlay.hidden) { opener = doc.activeElement; overlay.hidden = false; }
      dialogTitle.textContent = titleText; dialogClose.setAttribute('aria-label', t('close')); dialogClose.title = t('close');
      dialogBody.replaceChildren(); dialogStatus.textContent = ''; dialogStatus.classList.remove('tg-error');
      return dialogBody;
    }
    function focusFirst(preferred) { (preferred || dialogBody.querySelector('input,.tg-chosen') || focusables().find(e => e !== dialogClose) || dialogClose).focus(); }
    function closeModal() {
      if (overlay.hidden) return;
      overlay.hidden = true; wizard = null; dialogBody.replaceChildren();
      if (opener && opener.isConnected && opener.focus) opener.focus({preventScroll:true});
      opener = null;
    }

    function savePositions() { try { root.localStorage.setItem('mistl-tunnel-layout:' + room, JSON.stringify(positions)); } catch (_) {} }
    function can(model, permission) { return model.self || !!(model.node && !model.node.locked && model.node.permissions[permission]); }
    function notice(text, error) {
      message.textContent = text; message.classList.toggle('tg-error', !!error);
      if (!overlay.hidden) { dialogStatus.textContent = text; dialogStatus.classList.toggle('tg-error', !!error); }
    }
    async function perform(work) {
      if (busy) return;
      busy = true; host.setAttribute('aria-busy', 'true'); overlay.setAttribute('aria-busy', 'true'); notice(t('waiting'));
      try { const message = await work(); notice(message || t('applied')); }
      catch (error) { notice(t('failed') + ': ' + (error.message || error), true); }
      finally { busy = false; host.setAttribute('aria-busy', 'false'); overlay.setAttribute('aria-busy', 'false'); await options.refresh(); }
    }
    function command(model, action) { return options.api('tunnel.graph.command', {node_id:model.id, room:model.room, action}); }
    function modelById(id) { return models.find(m => m.id === id); }
    async function link(a, b, connected) {
      if (!a || !b || a.id === b.id) return;
      const changing = [a,b].filter((m,i) => !!m.node?.links.includes([b,a][i].id) !== connected);
      if (changing.some(m => !m.node || !can(m,'edit_links'))) { notice(t('denied'), true); return; }
      await perform(async () => {
        let applied = 0;
        try {
          for (const m of changing) { await command(m, {type:'link', peer_id:m.id === a.id ? b.id : a.id, connected}); applied++; }
        } catch (e) { throw new Error((applied ? t('partial') + ' ' : '') + e.message); }
      });
      if (!connected && (a.node.broadcast || b.node.broadcast)) notice(t('broadcastDisconnect'));
    }
    function choose(id) {
      if (!source) { source = id; notice(t('choose')); drawWires(); return; }
      const from = source; source = null; drawWires(); if (from !== id) link(modelById(from),modelById(id),true);
    }
    function position(model, index) {
      const saved = positions[model.id];
      return saved && Number.isFinite(saved.x) && Number.isFinite(saved.y) ? saved : {x:30 + (index % 3)*324, y:40 + Math.floor(index/3)*360};
    }
    function drawNodes() {
      const focused=doc.activeElement, focusedCard=focused?.closest('.tg-node');
      const focusId=focusedCard?.dataset.nodeId, focusIndex=focusedCard?[...focusedCard.querySelectorAll('button,input,[tabindex]')].indexOf(focused):-1;
      nodes.replaceChildren(); statsEls.clear();
      models.forEach((model,index) => {
        const card = el('article','tg-node' + (model.self ? ' tg-self' : '')); card.dataset.nodeId = model.id;
        const pos = position(model,index); card.style.left = Math.max(8,pos.x) + 'px'; card.style.top = Math.max(8,pos.y) + 'px';
        const head = el('div','tg-node-head'); head.tabIndex = 0; head.title = model.id; head.setAttribute('aria-label', model.id + ' — ' + t('help'));
        const named = !model.self && peerName(model.id);
        const avatar = named && peersApi().avatarEl ? peersApi().avatarEl(model.id, 18) : null;
        head.append(avatar || el('span','tg-node-dot'), el('strong','',model.self ? t('self') : (named || t('peer'))));
        const id = el('div','tg-node-id', model.id); id.title = model.id;
        function connector(side) {
          const port = button('', 'tg-port' + (side === 'left' ? ' tg-port-in' : ''), () => choose(model.id)); port.title = t('connect'); port.setAttribute('aria-label', t('connect') + ' — ' + model.id + ' (' + side + ')'); port.dataset.port = model.id;
          port.addEventListener('pointerdown', e => { if (e.button !== 0) return; drag = {port:model.id,side,x:e.clientX,y:e.clientY,moved:false}; });
          return port;
        }
        head.addEventListener('pointerdown', e => {
          if (e.button !== 0) return;
          drag = {id:model.id,card,x:e.clientX,y:e.clientY,left:parseFloat(card.style.left),top:parseFloat(card.style.top)};
          if (head.setPointerCapture) head.setPointerCapture(e.pointerId);
        });
        head.addEventListener('keydown', e => {
          const delta = {ArrowLeft:[-20,0],ArrowRight:[20,0],ArrowUp:[0,-20],ArrowDown:[0,20]}[e.key]; if (!delta) return;
          e.preventDefault(); positions[model.id] = {x:Math.max(8,parseFloat(card.style.left)+delta[0]),y:Math.max(8,parseFloat(card.style.top)+delta[1])}; savePositions(); drawNodes();
          [...nodes.querySelectorAll('.tg-node')].find(n=>n.dataset.nodeId===model.id)?.querySelector('.tg-node-head').focus();
        });
        card.append(head,id,connector('right'),connector('left'));
        if (model.node) {
          if (model.self) {
            const gear = button('⚙','tg-gear',()=>showSettings(model)); gear.title = t('settings'); gear.setAttribute('aria-label', t('settings')); card.append(gear);
          }
          const badges = el('div','tg-badges');
          if (model.node.broadcast) badges.append(el('span','tg-badge',t('broadcast')));
          if (model.node.locked) badges.append(el('span','tg-badge',t('locked')));
          if (badges.childNodes.length) card.append(badges);
          const list = el('div','tg-forwards');
          const outgoing = model.node.outgoing || [];
          if (!model.node.forwards.length && !outgoing.length) list.append(el('p','tg-empty',t('empty')));
          // Proposals this node sent that the peer has not answered yet.
          outgoing.forEach(o => list.append(outgoingRow(o)));
          model.node.forwards.forEach(f => {
            const row = el('div','tg-forward'), text = el('div','tg-forward-text');
            text.append(el('span','tg-proto',String(f.proto || '').toUpperCase()),el('span','',f.direction === 'serve' ? ' ↑ ' : ' ↓ '),el('span','',f.addr));
            if (f.direction === 'connect') { const dest = el('small','',f.target); dest.title=f.target; text.append(dest); }
            if (f.awaiting_approval) text.append(pill('tg-wait','⏳ '+fmt('waitApprove',{peer:peerLabel(f.approval_peer_id||'')}),f.approval_peer_id));
            else if (f.denied_at_ms) text.append(pill('tg-denied','✕ '+fmt('deniedBy',{peer:peerLabel(f.approval_peer_id||'')}),f.approval_peer_id));
            if (f.state === 'error') text.append(el('small','tg-error', t('error') + ': ' + f.error));
            if (model.self) { const stats = el('small','tg-stats'); stats.textContent = statsText(f); text.append(stats); statsEls.set(statKey(f), stats); }
            row.append(text);
            const doRemove = () => {
              if (!(options.confirm || root.confirm)(t('removeConfirm').replace('{target}', () => f.target))) return;
              if (f.graph_managed) perform(() => command(model,{type:'remove_forward',target:f.target}));
              else perform(() => options.api('tunnel.forward.remove',{target:f.target}));
            };
            if (model.self) {
              const remove = button('×','tg-remove',doRemove);
              remove.title=t('remove'); remove.setAttribute('aria-label',t('remove')+' '+f.addr); row.append(remove);
            } else if (f.graph_managed) {
              const remove = button('×','tg-remove',doRemove);
              remove.title=t('remove'); remove.setAttribute('aria-label',t('remove')+' '+f.addr); remove.disabled=!can(model,'remove_forwards'); row.append(remove);
            } else row.title=t('legacyForward');
            list.append(row);
          });
          card.append(list);
          // Remote nodes only offer the button when their owner granted it.
          if (can(model,'add_forwards')) card.append(button(t('add'),'tg-add',()=>showForward(model)));
        } else card.append(el('p','tg-empty',t('legacy')));
        nodes.append(card);
      });
      drawWires();
      if(focusId&&focusIndex>=0) [...nodes.children].find(c=>c.dataset.nodeId===focusId)?.querySelectorAll('button,input,[tabindex]')[focusIndex]?.focus({preventScroll:true});
    }
    // A status pill; the full peer id stays reachable next to a claimed name.
    function pill(cls, text, peerId) {
      const wrap = el('span','tg-pill-wrap'), p = el('span','tg-pill '+cls,text); p.setAttribute('role','status'); wrap.append(p);
      if (peerId && peerLabel(peerId) !== peerId) wrap.append(el('small','tg-peer-id',peerId));
      return wrap;
    }
    function outgoingRow(o) {
      const row = el('div','tg-forward tg-outgoing'), text = el('div','tg-forward-text');
      text.append(el('span','tg-proto',String(o.proto || '').toUpperCase()),el('span','',' ↓ '),el('span','',o.local_addr || String(o.listen_port || '')));
      const dest = el('small','',fmt('toPeer',{peer:peerLabel(o.peer_id),addr:o.remote_addr || ''})); dest.title = o.target || ''; text.append(dest);
      text.append(pill('tg-wait','⏳ '+fmt('waitAccept',{peer:peerLabel(o.peer_id)}),o.peer_id));
      row.append(text);
      if (o.req_id) {
        const cancel = button('×','tg-remove',() => perform(async () => { await options.api('tunnel.forward.propose.cancel',{req_id:o.req_id}); return t('cancelled'); }));
        cancel.title = t('cancel'); cancel.setAttribute('aria-label',t('cancel')+' '+(o.local_addr || '')); row.append(cancel);
      }
      return row;
    }
    // Peers this node is waiting on: an unanswered proposal, or a connection the owner parked for approval.
    function waitingPeers() {
      const self = models.find(m => m.self), set = new Set();
      if (!self || !self.node) return set;
      (self.node.outgoing || []).forEach(o => o.peer_id && set.add(o.peer_id));
      self.node.forwards.forEach(f => { if (f.awaiting_approval && f.approval_peer_id) set.add(f.approval_peer_id); });
      return set;
    }
    function drawWires() {
      svg.replaceChildren();
      let width=660,height=380;
      const cards = new Map([...nodes.children].map(c=>[c.dataset.nodeId,c]));
      cards.forEach(c=>{width=Math.max(width,parseFloat(c.style.left)+310);height=Math.max(height,parseFloat(c.style.top)+(c.offsetHeight||250)+40);c.classList.toggle('tg-connecting',c.dataset.nodeId===source);});
      canvas.style.width=width+'px';canvas.style.height=height+'px'; svg.setAttribute('width',width);svg.setAttribute('height',height);
      const waiting=waitingPeers();
      models.forEach((a,i)=>models.slice(i+1).forEach(b=>{
        // The self <-> peer wire is pending while a proposal or approval is outstanding.
        const awaiting=(a.self&&waiting.has(b.id))||(b.self&&waiting.has(a.id));
        const ab=!!a.node?.links.includes(b.id), ba=!!b.node?.links.includes(a.id), explicit=ab||ba;
        const allowed=!!a.node&&!!b.node&&(a.node.broadcast||ab)&&(b.node.broadcast||ba);
        if (!awaiting&&(!a.node||!b.node)) return;
        if (!explicit&&!allowed&&!awaiting) return;
        let ca=cards.get(a.id),cb=cards.get(b.id);
        if(parseFloat(ca.style.left)>parseFloat(cb.style.left)) [ca,cb]=[cb,ca];
        const leftA=parseFloat(ca.style.left),leftB=parseFloat(cb.style.left),x1=leftA+282,y1=parseFloat(ca.style.top)+48,y2=parseFloat(cb.style.top)+48;
        const horizontal=leftB-leftA>=310,x2=leftB+(horizontal?0:282);
        const lane=Math.max(5,Math.min(y1,y2)-76);
        const path=doc.createElementNS(svg.namespaceURI,'path');
        path.setAttribute('d',horizontal
          ? (leftB-leftA<400?`M ${x1} ${y1} C ${x1+22} ${y1}, ${x2-22} ${y2}, ${x2} ${y2}`:`M ${x1} ${y1} L ${x1+18} ${y1} L ${x1+18} ${lane} L ${x2-18} ${lane} L ${x2-18} ${y2} L ${x2} ${y2}`)
          : `M ${x1} ${y1} C ${Math.max(x1,x2)+80} ${y1}, ${Math.max(x1,x2)+80} ${y2}, ${x2} ${y2}`);
        path.setAttribute('class','tg-wire '+(explicit||allowed?(explicit?(allowed?'tg-direct':'tg-pending'):'tg-broadcast'):'')+(awaiting?' tg-awaiting':''));
        path.setAttribute('role','button');path.setAttribute('tabindex','0');path.setAttribute('aria-label',`${a.id} ↔ ${b.id} · ${awaiting?t('proposalWire'):t(explicit?(allowed?'direct':'pending'):'room')}`);
        const open=()=>{ if(a.node&&b.node) showEdge(a,b,explicit,allowed); };path.addEventListener('click',open);path.addEventListener('keydown',e=>{if(e.key==='Enter'||e.key===' '){e.preventDefault();open();}});svg.append(path);
      }));
    }
    function showEdge(a,b,explicit,allowed) {
      const pane=modal(t(explicit?(allowed?'direct':'pending'):'room'));
      pane.append(flow([nodeLabel(a),''],[nodeLabel(b),''],'↔'));
      if (a.node.broadcast||b.node.broadcast) pane.append(el('p','tg-help',t('broadcastDisconnect')));
      const foot=el('div','tg-dialog-foot');
      // The editor closes first so the outcome lands in the panel status line.
      if (explicit) { const remove=button(t('disconnect'),'danger',()=>{closeModal();link(a,b,false);});remove.disabled=![a,b].every(m=>can(m,'edit_links'));foot.append(remove); }
      if (!allowed || !explicit) { const connect=button(t('direct'),'primary',()=>{closeModal();link(a,b,true);});connect.disabled=![a,b].every(m=>can(m,'edit_links'));foot.append(connect); }
      pane.append(foot);focusFirst();
    }
    function nodeLabel(model) { return model.self ? t('self') : peerText(model.id); }
    // A two-box "from → to" picture used by the link editor and the wizard summary.
    function flow(from,to,arrow) {
      const wrap=el('div','tg-flow');
      [from,to].forEach((side,i)=>{
        if (i) wrap.append(el('span','tg-flow-arrow',arrow||'→'));
        const box=el('div','tg-flow-box');box.append(el('span','tg-flow-label',side[0]));if (side[1]) box.append(el('span','tg-flow-value',side[1]));wrap.append(box);
      });
      return wrap;
    }

    // ---- port-forward wizard: one question per step ----
    function wizardSteps(w) {
      const steps=['type'];
      if (w.type==='propose') { if (peersFor(w.model).length!==1) steps.push('peer'); steps.push('remote','local'); }
      if (w.type==='connect') steps.push('service','local');
      if (w.type==='serve') steps.push('serve');
      if (w.type) steps.push('confirm');
      return steps;
    }
    function peersFor(model) { return models.filter(m=>m.id!==model.id); }
    function servicesFor(model) {
      const out=[];
      models.filter(m=>m.id!==model.id&&m.node).forEach(m=>m.node.forwards.filter(f=>f.direction==='serve').forEach(f=>{
        out.push({node:m.id,proto:f.proto,addr:f.addr,target:f.target.includes('@')?f.target:f.target+'@'+m.id});
      }));
      return out;
    }
    function showForward(model) {
      wizard={model,type:'',peer:'',service:null,proto:'tcp',remote:'',local:'127.0.0.1:8080',addr:'127.0.0.1:8080',index:0};
      renderStep();
    }
    function go(delta) { wizard.index=Math.max(0,wizard.index+delta); renderStep(); }
    function choice(titleText,helpText,value,selected,action,mono) {
      const b=button('','tg-choice'+(selected?' tg-chosen':''),action);b.dataset.value=value;b.setAttribute('aria-pressed',selected?'true':'false');
      b.append(el('strong',mono?'tg-mono':'',titleText));if (helpText) b.append(el('small','',helpText));return b;
    }
    function addrInput(value,placeholder) {
      const input=el('input');input.type='text';input.value=value;input.placeholder=placeholder;input.required=true;input.autocomplete='off';input.spellcheck=false;
      return input;
    }
    function protoSwitch(w) {
      const group=el('div','tg-seg');group.setAttribute('role','radiogroup');group.setAttribute('aria-label',t('proto'));
      [['tcp','TCP'],['udp','UDP']].forEach(([value,text])=>{
        const label=el('label'),input=el('input');input.type='radio';input.name='tg-proto-'+dialogTitle.id;input.value=value;input.checked=w.proto===value;
        input.addEventListener('change',()=>{if(input.checked)w.proto=value;});label.append(input,el('span','',text));group.append(label);
      });
      return group;
    }
    function renderStep() {
      const w=wizard,model=w.model,steps=wizardSteps(w),step=steps[w.index];
      const pane=modal(t('addTitle'));pane.dataset.step=step;
      if (!model.self) pane.append(el('p','tg-dialog-node',peerText(model.id)));
      if (w.type) pane.append(el('p','tg-step',t('step').replace('{n}',w.index+1).replace('{total}',steps.length)));
      const form=el('form','tg-wizard');form.noValidate=true;pane.append(form);
      const ask=key=>form.append(el('h4','tg-question',t(key)));
      const foot=el('div','tg-dialog-foot');
      let input=null,submitText=t('next'),onNext=null,nextButton=null;
      if (step==='type') {
        ask('askType');
        const types=model.self?['propose','connect','serve']:['serve','connect'];
        const keys={propose:'typePropose',connect:'typeConnect',serve:'typeServe'};
        types.forEach(type=>form.append(choice(t(keys[type]),t(keys[type]+'Help'),type,w.type===type,()=>{
          if (w.type!==type) Object.assign(w,{type,peer:'',service:null,proto:'tcp'});
          go(1);
        })));
      } else if (step==='peer') {
        ask('askPeer');
        const peers=peersFor(model);
        if (!peers.length) form.append(el('p','tg-empty',t('noPeers')));
        peers.forEach(p=>{const name=peerName(p.id);form.append(choice(name||p.id,name?p.id:'',p.id,w.peer===p.id,()=>{w.peer=p.id;go(1);},true));});
      } else if (step==='service') {
        ask('askService');
        const services=servicesFor(model);
        if (!services.length) form.append(el('p','tg-empty',t('noService')));
        services.forEach(s=>form.append(choice(String(s.proto||'').toUpperCase()+'  '+s.addr,peerText(s.node),s.target,w.service?.target===s.target,()=>{w.service=s;w.proto=s.proto;go(1);},true)));
      } else if (step==='remote'||step==='local'||step==='serve') {
        const key={remote:'remote',local:'local',serve:'addr'}[step];
        ask({remote:'askRemote',local:'askLocal',serve:'askServe'}[step]);
        input=addrInput(w[key],step==='remote'?'127.0.0.1:22':'127.0.0.1:8080');input.setAttribute('aria-label',t({remote:'askRemote',local:'askLocal',serve:'askServe'}[step]));
        input.addEventListener('input',()=>{w[key]=input.value;});
        form.append(input);
        if (step!=='local') form.append(protoSwitch(w));
        form.append(el('p','tg-help',t({remote:'remoteHint',local:'localHint',serve:'serveHint'}[step])));
        // The local listener must be free on this machine: ask the daemon
        // while typing and keep Next disabled for a busy address.
        const portCheck=step==='local'?checkLocalPort(w,input,form,()=>nextButton):null;
        onNext=async()=>{
          const value=input.value.trim();
          if (!ADDR.test(value)) { dialogStatus.textContent=t('badAddr');dialogStatus.classList.add('tg-error');input.setAttribute('aria-invalid','true');input.focus();return; }
          if (portCheck&&!(await portCheck())) { input.focus();return; }
          w[key]=value;go(1);
        };
      } else if (step==='confirm') {
        ask('askConfirm');
        const proto=String(w.proto).toUpperCase(),self=nodeLabel(model);
        if (w.type==='propose') { const peer=w.peer||peersFor(model)[0]?.id||'';w.peer=peer;form.append(flow([self,w.local],[peerText(peer),w.remote])); }
        else if (w.type==='connect') form.append(flow([self,w.local],[peerText(w.service.node),w.service.addr]));
        else form.append(flow([self,w.addr],[t('published'),'']));
        form.append(el('p','tg-summary-proto',t('proto')+': '+proto));
        if (w.type==='propose') form.append(el('p','tg-help',t('proposeNote')));
        submitText=t(w.type==='propose'?'sendProposal':'save');
        onNext=()=>submitForward(w);
      }
      if (w.index>0) foot.append(button(t('back'),'tg-back',()=>go(-1)));
      if (onNext) { nextButton=el('button','primary',submitText);nextButton.type='submit';foot.append(nextButton); }
      form.append(foot);
      form.addEventListener('submit',e=>{e.preventDefault();if(onNext&&!busy)onNext();});
      focusFirst(input||(step==='confirm'?foot.querySelector('.primary'):null));
    }
    // Returns a re-check function resolving to false only when the daemon
    // reports the address unusable; a failed check never blocks the wizard
    // because the daemon refuses a busy port again on submit.
    function checkLocalPort(w,input,form,getNext) {
      const note=el('div','tg-port-note');note.setAttribute('role','status');form.append(note);
      let seq=0,timer=null,lastOk=true;
      const check=async()=>{
        const value=input.value.trim(),mine=++seq;
        if (!ADDR.test(value)) { note.replaceChildren();input.removeAttribute('aria-invalid');if(getNext())getNext().disabled=false;return true; }
        let result;
        try { result=await options.api('tunnel.port.check',{proto:w.proto,addr:value}); } catch (_) { return true; }
        if (mine!==seq||wizard!==w) return lastOk;
        lastOk=!(result&&result.available===false);
        note.replaceChildren();input.toggleAttribute('aria-invalid',!lastOk);
        if (getNext()) getNext().disabled=!lastOk;
        if (!lastOk) {
          const busy=String(result.error||'').includes('already in use');
          note.append(el('p','tg-error',busy?t('portBusy'):t('portUnusable')+': '+(result.error||'')));
          if (result.suggestion) note.append(button(t('useSuggestion').replace('{addr}',()=>result.suggestion),'tg-suggest',()=>{
            input.value=result.suggestion;w.local=result.suggestion;check();input.focus();
          }));
        }
        return lastOk;
      };
      input.addEventListener('input',()=>{clearTimeout(timer);timer=setTimeout(check,250);});
      check();
      return check;
    }
    function submitForward(w) {
      if (w.type==='propose') {
        perform(async()=>{
          const result=await options.api('tunnel.forward.propose',{proto:w.proto,local:w.local,remote:w.remote,peer_id:w.peer});
          closeModal();
          return (result&&result.message)||t('proposeSent');
        });
        return;
      }
      const addr=w.type==='serve'?w.addr:w.local,target=w.type==='connect'?w.service.target:'';
      perform(async()=>{await command(w.model,{type:'add_forward',direction:w.type,proto:w.proto,addr,target});closeModal();});
    }

    // ---- node settings: switches first, permissions one level deeper ----
    function showSettings(model) {
      const pane=modal(t('settings'));
      pane.append(setting(model,'broadcast',t('broadcast'),t('broadcastHelp')),setting(model,'locked',t('lock'),t('lockHelp')));
      pane.append(choice(t('permissions'),t('permissionHelp'),'permissions',false,()=>showPermissions(model)));
      focusFirst(pane.querySelector('input'));
    }
    function setting(model,key,label,help) {
      const wrap=el('label','tg-setting'),text=el('span','tg-setting-text'),input=el('input');
      input.type='checkbox';input.checked=!!model.node[key];input.setAttribute('role','switch');
      const control=el('span','toggle-switch');control.append(input,el('span','slider'));
      text.append(el('strong','',label),el('small','',help));wrap.append(text,control);
      input.addEventListener('change',async()=>{
        const value=input.checked;input.checked=!!model.node[key];
        await perform(()=>command(model,key==='broadcast'?{type:'broadcast',enabled:value}:{type:'lock',locked:value}));
        const now=modelById(model.id);if(now?.node){model=now;input.checked=!!now.node[key];}
      });
      return wrap;
    }
    function showPermissions(model) {
      const pane=modal(t('permissions'));pane.append(el('p','tg-help',t('permissionHelp')));
      const ids=[...new Set([...models.filter(m=>!m.self).map(m=>m.id),...Object.keys(snapshot.graph.policy.permissions)])];
      const rows=el('div','tg-grant-list');pane.append(rows);
      ids.forEach(id=>permissionRow(rows,model,id));
      const form=el('form','tg-grant-add'),input=el('input');input.type='text';input.required=true;input.placeholder=t('peerId');input.setAttribute('aria-label',t('peerId'));
      const add=el('button','',t('grant'));add.type='submit';form.append(input,add);pane.append(form);
      form.addEventListener('submit',e=>{e.preventDefault();const id=input.value.trim();if(id&&id!==model.id&&!ids.includes(id)){ids.push(id);permissionRow(rows,model,id);input.value='';}});
      const foot=el('div','tg-dialog-foot');foot.append(button(t('back'),'tg-back',()=>showSettings(modelById(model.id)||model)));pane.append(foot);
      focusFirst(pane.querySelector('input'));
    }
    function permissionRow(pane,model,id) {
      const row=el('fieldset','tg-grants');row.append(el('legend','tg-full-id',id));
      const grants={...(snapshot.graph.policy.permissions[id]||{})};
      [['edit_links','links'],['add_forwards','addGrant'],['remove_forwards','removeGrant']].forEach(([key,label])=>{
        const wrap=el('label','switch-control'),input=el('input');input.type='checkbox';input.setAttribute('role','switch');input.checked=!!grants[key];
        const control=el('span','toggle-switch');control.append(input,el('span','slider'));wrap.append(el('span','',t(label)),control);row.append(wrap);
        input.addEventListener('change',()=>{const next={edit_links:!!grants.edit_links,add_forwards:!!grants.add_forwards,remove_forwards:!!grants.remove_forwards,[key]:input.checked};input.checked=!!grants[key];perform(async()=>{await command(model,{type:'permission',peer_id:id,permissions:next});Object.assign(grants,next);input.checked=!!next[key];});});
      });pane.append(row);
    }
    doc.addEventListener('pointermove',e=>{
      if(!drag)return;
      if(drag.port){
        drag.moved ||= Math.abs(e.clientX-drag.x)+Math.abs(e.clientY-drag.y)>6;
        if(drag.moved){source=drag.port;drawWires();const rect=canvas.getBoundingClientRect(), card=[...nodes.children].find(c=>c.dataset.nodeId===drag.port);const path=doc.createElementNS(svg.namespaceURI,'path');path.setAttribute('class','tg-wire tg-direct');path.style.pointerEvents='none';path.setAttribute('d',`M ${parseFloat(card.style.left)+(drag.side==='left'?0:282)} ${parseFloat(card.style.top)+48} L ${e.clientX-rect.left} ${e.clientY-rect.top}`);svg.append(path);}
        return;
      }
      positions[drag.id]={x:Math.max(8,drag.left+e.clientX-drag.x),y:Math.max(8,drag.top+e.clientY-drag.y)};
      drag.card.style.left=positions[drag.id].x+'px';drag.card.style.top=positions[drag.id].y+'px';drawWires();
    });
    doc.addEventListener('pointerup',e=>{
      if(!drag)return;const ended=drag;drag=null;
      if(ended.port&&ended.moved){const target=doc.elementFromPoint(e.clientX,e.clientY)?.closest('[data-port]');source=null;if(target)link(modelById(ended.port),modelById(target.dataset.port),true);drawWires();}
      else savePositions();
    });
    doc.addEventListener('pointercancel',()=>{drag=null;source=null;drawWires();});
    doc.addEventListener('keydown',e=>{if(e.key==='Escape'){source=null;drag=null;notice('');drawWires();}});

    // Localized text for a session notice that carries a machine-readable code
    // (the outcome of a proposal); anything else keeps the daemon's text.
    function noticeText(n) {
      const codes={proposal_denied:'nDenied',proposal_timeout:'nTimeout',proposal_peer_left:'nPeerLeft',proposal_failed:'nFailed',proposal_accepted:'nAccepted',proposal_add_failed:'nAddFailed'};
      if (!codes[n.code]) return String(n.text||'');
      return fmt(codes[n.code],{peer:peerText(n.peer_id||''),target:String(n.target||'').replace(/@[^@]*$/,''),local:n.detail||'',detail:n.detail||''});
    }
    // Shows each notice once as it appears (the first snapshot only sets the
    // baseline, so old notices are not replayed on page load). The answer to a
    // proposal -- denied, timed out, accepted -- thus never goes unnoticed.
    function announce(list) {
      list=Array.isArray(list)?list.filter(n=>n&&typeof n==='object'):[];
      const keys=new Set(list.map(n=>(n.timestamp_ms||0)+'|'+(n.text||'')));
      if (noticeSeen===null) { noticeSeen=keys; return; }
      list.forEach(n=>{
        if (noticeSeen.has((n.timestamp_ms||0)+'|'+(n.text||''))) return;
        const text=noticeText(n),bad=n.kind==='error';
        notice(text,bad);
        if (options.toast) options.toast(text,bad?'error':'success');
      });
      noticeSeen=keys;
    }
    function render(data) {
      announce(data&&data.notices);
      snapshot=data;
      const nextRoom=data?.room||'';
      if(room!==nextRoom){room=nextRoom;source=null;closeModal();positions={};try{positions=JSON.parse(root.localStorage.getItem('mistl-tunnel-layout:'+room)||'{}')||{};}catch(_){} signature='';}
      title.textContent=t('title');arrange.textContent=t('reset');hint.textContent=data?.running?t('help'):t('stopped');
      viewport.hidden=!data?.running;arrange.hidden=!data?.running;
      if(!data?.running){models=[];signature='';source=null;closeModal();nodes.replaceChildren();svg.replaceChildren();statsEls.clear();return;}
      if(!data.graph){hint.textContent=t('unavailable');models=[];signature='';closeModal();nodes.replaceChildren();svg.replaceChildren();statsEls.clear();return;}
      const policy=data.graph.policy;
      if(!subscribed&&peersApi()&&typeof peersApi().subscribe==='function'){subscribed=true;peersApi().subscribe(()=>{signature='';if(snapshot&&!drag)render(snapshot);});}
      models=[{id:data.self_id,self:true,node:{...policy,forwards:data.forwards||[],outgoing:data.pending_outgoing||[]}},...(data.graph.nodes||[]).map(m=>({...m,self:false}))].map(m=>({...m,room,node:m.node?{...m.node,forwards:(m.node.forwards||[]).filter(f=>f&&typeof f.addr==='string'&&typeof f.target==='string')}:null}));
      const names=models.map(m=>peerName(m.id)).join('\u0001');
      const next=JSON.stringify(models.map(m=>({...m,node:m.node?{...m.node,forwards:m.node.forwards.map(({active_conns,bytes_in,bytes_out,peers,...f})=>f)}:null})))+names,lang=doc.documentElement.lang;
      updateStats(data.forwards);
      if(drag||(next===signature&&lang===language))return;
      signature=next;language=lang;drawNodes();
    }
    return {render};
  }
  root.MistlTunnelGraph={create};
})(window);
