(function (root) {
  'use strict';
  const words = {
    ja: {
      title:'接続を組み立てる', help:'接続端子をドラッグ、または順にクリックしてノード同士を結びます。見出しをドラッグすると配置を変えられます。',
      stopped:'Room ID で接続すると、ここにノードが表示されます。', self:'このノード', peer:'リモートノード', legacy:'グラフ未対応・状態待ち',
      broadcast:'Broadcast', broadcastHelp:'ON: ルーム全体から接続を受付。OFF: 線で結んだ相手だけ。各転送の承認は別途必要です。TCP のデータ複製ではありません。',
      lock:'編集ロック', lockHelp:'他ノードによる線・転送設定の変更を止めます。既存の通信は続きます。所有者は編集できます。',
      add:'＋ ポート転送', empty:'ポート転送はまだありません', remove:'削除', permissions:'接続・編集権限', connect:'接続端子', choose:'接続先の端子を選択してください（Esc で取消）',
      source:'サービス公開', listener:'ローカル受信 → 相手のサービス', direction:'転送の種類', proto:'プロトコル', addr:'アドレス', service:'接続先サービス',
      select:'サービスを選択', save:'追加', cancel:'取消', direct:'直接接続', room:'ルーム公開', pending:'片側のみ設定済み', disconnect:'線を削除',
      permissionHelp:'このノードに対する操作を、相手ごとに許可します。通信自体には従来の転送承認も適用されます。',
      links:'このノードにつながる線を変更', addGrant:'ポート転送を追加（ローカルサービス公開を含む）', removeGrant:'ポート転送を削除',
      peerId:'相手の Node ID', grant:'権限を設定', close:'閉じる', reset:'配置を整える', owner:'所有者のみ変更できます',
      unavailable:'相手の状態を取得できていません。相手も対応版で接続してください。', denied:'編集権限がありません。相手側の「接続・編集権限」で許可してください。',
      applied:'設定を反映しました', failed:'変更できませんでした', partial:'片側の変更後に失敗しました。表示を確認して再試行してください。',
      broadcastDisconnect:'Broadcast が ON のノードがあります。線を削除してもルーム公開による通信は続きます。限定するには各ノードで Broadcast を OFF にしてください。',
      legacyForward:'既存の転送（下の一覧で管理）', waiting:'相手の確認を待っています…', noService:'公開サービスがありません。先に接続先ノードでサービスを追加してください。',
      forwardsHelp:'公開するサービス、または受信用ポートをノードに追加できます。受信用アドレスは 127.0.0.1:ポートです。',
      selected:'選択中', offline:'未接続', edit:'設定', error:'エラー',
    },
    en: {
      title:'Build your connections', help:'Drag between connection ports, or click two ports in sequence. Drag a node heading to rearrange it.',
      stopped:'Connect with a Room ID to see your nodes here.', self:'This node', peer:'Remote node', legacy:'Waiting / graph unsupported',
      broadcast:'Broadcast', broadcastHelp:'ON accepts connections from the room. OFF limits traffic to linked peers. Forward approval still applies. TCP data is not duplicated.',
      lock:'Edit lock', lockHelp:'Stops remote changes to links and forwards. Existing traffic continues. The owner can still edit.',
      add:'+ Port forward', empty:'No port forwards yet', remove:'Remove', permissions:'Connection permissions', connect:'Connection port', choose:'Choose the destination port (Esc to cancel)',
      source:'Publish a service', listener:'Local listener → peer service', direction:'Forward type', proto:'Protocol', addr:'Address', service:'Destination service',
      select:'Select a service', save:'Add', cancel:'Cancel', direct:'Direct link', room:'Room broadcast', pending:'One side configured', disconnect:'Remove link',
      permissionHelp:'Grant each peer permission to modify this node. Existing forward authorization still applies to traffic.',
      links:'Change links involving this node', addGrant:'Add forwards (including publishing local services)', removeGrant:'Remove forwards',
      peerId:'Peer Node ID', grant:'Set permissions', close:'Close', reset:'Arrange nodes', owner:'Only the owner can change this',
      unavailable:'Peer graph is unavailable. Connect the peer using a compatible version.', denied:'Permission required. Ask the peer owner to grant access in Connection permissions.',
      applied:'Changes applied', failed:'Could not apply change', partial:'One side changed before an error. Inspect the graph before retrying.',
      broadcastDisconnect:'Broadcast is enabled on a node. Removing the explicit link leaves room traffic enabled. Turn Broadcast OFF on each node to restrict it.',
      legacyForward:'Existing forward (managed in the list below)', waiting:'Waiting for the peer to confirm…', noService:'No published services. Add a service on the destination node first.',
      forwardsHelp:'Add published services or receiving ports to this node. Receiving addresses use 127.0.0.1:port.',
      selected:'Selected', offline:'Offline', edit:'Settings', error:'Error',
    }
  };

  function create(options) {
    const doc = options.document, host = options.host;
    let snapshot = null, models = [], positions = {}, source = null, drag = null, busy = false, signature = '', room = '', language = '';
    const t = key => (words[doc.documentElement.lang === 'ja' ? 'ja' : 'en'][key] || key);
    const el = (tag, cls, text) => { const e = doc.createElement(tag); if (cls) e.className = cls; if (text != null) e.textContent = text; return e; };
    const button = (text, cls, action) => { const e = el('button', cls, text); e.type = 'button'; e.addEventListener('click', action); return e; };
    const line = el('div', 'tg-toolbar'), title = el('h3'), hint = el('p', 'tg-help');
    const message = el('div', 'tg-message'); message.setAttribute('role', 'status'); message.setAttribute('aria-live', 'polite');
    const viewport = el('div', 'tg-viewport'), canvas = el('div', 'tg-canvas');
    const svg = doc.createElementNS('http://www.w3.org/2000/svg', 'svg'); svg.classList.add('tg-wires'); svg.setAttribute('aria-label', 'Connections');
    const nodes = el('div', 'tg-nodes'), detail = el('div', 'tg-detail'); detail.hidden = true;
    const arrange = button('', '', () => { positions = {}; savePositions(); drawNodes(); });
    line.append(title, arrange); canvas.append(svg, nodes); viewport.append(canvas); host.append(line, hint, message, viewport, detail);

    function savePositions() { try { root.localStorage.setItem('mistl-tunnel-layout:' + room, JSON.stringify(positions)); } catch (_) {} }
    function can(model, permission) { return model.self || !!(model.node && !model.node.locked && model.node.permissions[permission]); }
    function notice(text, error) { message.textContent = text; message.classList.toggle('tg-error', !!error); }
    async function perform(work) {
      if (busy) return;
      busy = true; host.setAttribute('aria-busy', 'true'); notice(t('waiting'));
      try { await work(); notice(t('applied')); }
      catch (error) { notice(t('failed') + ': ' + (error.message || error), true); }
      finally { busy = false; host.setAttribute('aria-busy', 'false'); await options.refresh(); }
    }
    function command(model, action) { return options.api('tunnel.graph.command', {node_id:model.id, room:model.room, action}); }
    function toggle(model, key, label, help) {
      const wrap = el('label', 'tg-toggle'), input = el('input'); input.type = 'checkbox'; input.checked = !!model.node[key]; input.disabled = !model.self;
      input.setAttribute('role', 'switch'); input.setAttribute('aria-label', label + ' — ' + model.id);
      wrap.title = help + (!model.self ? ' ' + t('owner') : '');
      input.addEventListener('change', () => { const value = input.checked; input.checked = !!model.node[key]; perform(() => command(model, key === 'broadcast' ? {type:'broadcast', enabled:value} : {type:'lock', locked:value})); });
      wrap.append(input, el('span', '', label)); return wrap;
    }
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
      return saved && Number.isFinite(saved.x) && Number.isFinite(saved.y) ? saved : {x:30 + (index % 3)*324, y:40 + Math.floor(index/3)*520};
    }
    function drawNodes() {
      const focused=doc.activeElement, focusedCard=focused?.closest('.tg-node');
      const focusId=focusedCard?.dataset.nodeId, focusIndex=focusedCard?[...focusedCard.querySelectorAll('button,input,[tabindex]')].indexOf(focused):-1;
      nodes.replaceChildren();
      models.forEach((model,index) => {
        const card = el('article','tg-node' + (model.self ? ' tg-self' : '')); card.dataset.nodeId = model.id;
        const pos = position(model,index); card.style.left = Math.max(8,pos.x) + 'px'; card.style.top = Math.max(8,pos.y) + 'px';
        const head = el('div','tg-node-head'); head.tabIndex = 0; head.title = model.id; head.setAttribute('aria-label', model.id + ' — ' + t('help'));
        head.append(el('span','tg-node-dot'), el('strong','',model.self ? t('self') : t('peer')));
        const id = el('div','tg-node-id', model.id); id.title = model.id;
        function connector(side) {
          const port = button('○', 'tg-port' + (side === 'left' ? ' tg-port-in' : ''), () => choose(model.id)); port.title = t('connect'); port.setAttribute('aria-label', t('connect') + ' — ' + model.id + ' (' + side + ')'); port.dataset.port = model.id;
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
          const switches = el('div','tg-switches'); switches.append(toggle(model,'broadcast',t('broadcast'),t('broadcastHelp')),toggle(model,'locked',t('lock'),t('lockHelp'))); card.append(switches);
          const list = el('div','tg-forwards');
          if (!model.node.forwards.length) list.append(el('p','tg-empty',t('empty')));
          model.node.forwards.forEach(f => {
            const row = el('div','tg-forward'), text = el('div','tg-forward-text');
            text.append(el('span','tg-proto',String(f.proto || '').toUpperCase()),el('span','',f.direction === 'serve' ? ' ↑ ' : ' ↓ '),el('span','',f.addr));
            if (f.direction === 'connect') { const dest = el('small','',f.target); dest.title=f.target; text.append(dest); }
            if (f.state === 'error') text.append(el('small','tg-error', t('error') + ': ' + f.error));
            row.append(text);
            if (f.graph_managed) {
              const remove = button('×','tg-remove',()=>perform(()=>command(model,{type:'remove_forward',target:f.target})));
              remove.title=t('remove'); remove.setAttribute('aria-label',t('remove')+' '+f.addr); remove.disabled=!can(model,'remove_forwards'); row.append(remove);
            } else row.title=t('legacyForward');
            list.append(row);
          });
          card.append(list);
          const add = button(t('add'),'tg-add',()=>showForward(model)); add.disabled=!can(model,'add_forwards'); add.title=add.disabled?t('denied'):t('forwardsHelp'); card.append(add);
          if (model.self) card.append(button(t('permissions'),'tg-permissions',()=>showPermissions(model)));
        } else card.append(el('p','tg-empty',t('legacy')));
        nodes.append(card);
      });
      drawWires();
      if(focusId&&focusIndex>=0) [...nodes.children].find(c=>c.dataset.nodeId===focusId)?.querySelectorAll('button,input,[tabindex]')[focusIndex]?.focus({preventScroll:true});
    }
    function drawWires() {
      svg.replaceChildren();
      let width=660,height=380;
      const cards = new Map([...nodes.children].map(c=>[c.dataset.nodeId,c]));
      cards.forEach(c=>{width=Math.max(width,parseFloat(c.style.left)+310);height=Math.max(height,parseFloat(c.style.top)+(c.offsetHeight||350)+40);c.classList.toggle('tg-connecting',c.dataset.nodeId===source);});
      canvas.style.width=width+'px';canvas.style.height=height+'px'; svg.setAttribute('width',width);svg.setAttribute('height',height);
      models.forEach((a,i)=>models.slice(i+1).forEach(b=>{
        if (!a.node || !b.node) return;
        const ab=a.node.links.includes(b.id), ba=b.node.links.includes(a.id), explicit=ab||ba;
        const allowed=(a.node.broadcast||ab)&&(b.node.broadcast||ba);
        if (!explicit&&!allowed) return;
        let ca=cards.get(a.id),cb=cards.get(b.id);
        if(parseFloat(ca.style.left)>parseFloat(cb.style.left)) [ca,cb]=[cb,ca];
        const leftA=parseFloat(ca.style.left),leftB=parseFloat(cb.style.left),x1=leftA+282,y1=parseFloat(ca.style.top)+48,y2=parseFloat(cb.style.top)+48;
        const horizontal=leftB-leftA>=310,x2=leftB+(horizontal?0:282);
        const lane=Math.max(5,Math.min(y1,y2)-76);
        const path=doc.createElementNS(svg.namespaceURI,'path');
        path.setAttribute('d',horizontal
          ? (leftB-leftA<400?`M ${x1} ${y1} C ${x1+22} ${y1}, ${x2-22} ${y2}, ${x2} ${y2}`:`M ${x1} ${y1} L ${x1+18} ${y1} L ${x1+18} ${lane} L ${x2-18} ${lane} L ${x2-18} ${y2} L ${x2} ${y2}`)
          : `M ${x1} ${y1} C ${Math.max(x1,x2)+80} ${y1}, ${Math.max(x1,x2)+80} ${y2}, ${x2} ${y2}`);
        path.setAttribute('class','tg-wire '+(explicit?(allowed?'tg-direct':'tg-pending'):'tg-broadcast'));
        path.setAttribute('role','button');path.setAttribute('tabindex','0');path.setAttribute('aria-label',`${a.id} ↔ ${b.id} · ${t(explicit?(allowed?'direct':'pending'):'room')}`);
        const open=()=>showEdge(a,b,explicit,allowed);path.addEventListener('click',open);path.addEventListener('keydown',e=>{if(e.key==='Enter'||e.key===' '){e.preventDefault();open();}});svg.append(path);
      }));
    }
    function openDetail(titleText) {
      detail.replaceChildren();detail.hidden=false;
      const header=el('div','tg-detail-head');header.append(el('h4','',titleText),button(t('close'),'',()=>{detail.hidden=true;}));detail.append(header);return detail;
    }
    function showEdge(a,b,explicit,allowed) {
      const pane=openDetail(t(explicit?(allowed?'direct':'pending'):'room'));
      pane.append(el('p','tg-full-id',a.id+' ↔ '+b.id));
      if (a.node.broadcast||b.node.broadcast) pane.append(el('p','tg-help',t('broadcastDisconnect')));
      if (explicit) { const remove=button(t('disconnect'),'danger',()=>link(a,b,false));remove.disabled=![a,b].every(m=>can(m,'edit_links'));pane.append(remove); }
      if (!allowed || !explicit) { const connect=button(t('direct'),'primary',()=>link(a,b,true));connect.disabled=![a,b].every(m=>can(m,'edit_links'));pane.append(connect); }
    }
    function field(form,text,input) { const label=el('label','tg-field');label.append(el('span','',text),input);form.append(label);return input; }
    function select(values) { const input=el('select');values.forEach(([value,text])=>{const opt=el('option','',text);opt.value=value;input.append(opt);});return input; }
    function showForward(model) {
      const pane=openDetail(t('add')+' · '+model.id),form=el('form','tg-form');pane.append(el('p','tg-help',t('forwardsHelp')),form);
      const direction=field(form,t('direction'),select([['serve',t('source')],['connect',t('listener')]]));
      const proto=field(form,t('proto'),select([['tcp','TCP'],['udp','UDP']]));
      const addr=field(form,t('addr'),el('input'));addr.value='127.0.0.1:8080';addr.required=true;
      const service=field(form,t('service'),select([['',t('select')]]));
      const update=()=>{
        service.replaceChildren();const placeholder=el('option','',t('select'));placeholder.value='';service.append(placeholder);
        models.filter(m=>m.id!==model.id&&m.node).forEach(m=>m.node.forwards.filter(f=>f.direction==='serve'&&f.proto===proto.value).forEach(f=>{
          const opt=el('option','',m.id+' · '+f.addr);opt.value=f.target.includes('@')?f.target:f.target+'@'+m.id;service.append(opt);
        }));
        service.parentElement.hidden=direction.value!=='connect';service.required=direction.value==='connect';
        service.title=service.options.length===1?t('noService'):'';
      };direction.addEventListener('change',update);proto.addEventListener('change',update);update();
      const save=el('button','primary',t('save'));save.type='submit';form.append(save);
      form.addEventListener('submit',e=>{e.preventDefault();if(!form.reportValidity())return;perform(async()=>{await command(model,{type:'add_forward',direction:direction.value,proto:proto.value,addr:addr.value.trim(),target:direction.value==='connect'?service.value:''});detail.hidden=true;});});addr.focus();
    }
    function showPermissions(model) {
      const pane=openDetail(t('permissions'));pane.append(el('p','tg-help',t('permissionHelp')));
      const ids=[...new Set([...models.filter(m=>!m.self).map(m=>m.id),...Object.keys(snapshot.graph.policy.permissions)])];
      ids.forEach(id=>permissionRow(pane,model,id));
      const form=el('form','tg-form'),input=field(form,t('peerId'),el('input'));input.required=true;input.placeholder='Node ID';const add=el('button','',t('grant'));add.type='submit';form.append(add);pane.append(form);
      form.addEventListener('submit',e=>{e.preventDefault();const id=input.value.trim();if(id&&id!==model.id&&!ids.includes(id)){ids.push(id);permissionRow(pane,model,id);input.value='';}});
    }
    function permissionRow(pane,model,id) {
      const row=el('fieldset','tg-grants');row.append(el('legend','tg-full-id',id));
      const grants={...(snapshot.graph.policy.permissions[id]||{})};
      [['edit_links','links'],['add_forwards','addGrant'],['remove_forwards','removeGrant']].forEach(([key,label])=>{
        const wrap=el('label'),input=el('input');input.type='checkbox';input.checked=!!grants[key];wrap.append(input,el('span','',t(label)));row.append(wrap);
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

    function render(data) {
      snapshot=data;
      const nextRoom=data?.room||'';
      if(room!==nextRoom){room=nextRoom;source=null;detail.hidden=true;positions={};try{positions=JSON.parse(root.localStorage.getItem('mistl-tunnel-layout:'+room)||'{}')||{};}catch(_){} signature='';}
      title.textContent=t('title');arrange.textContent=t('reset');hint.textContent=data?.running?t('help'):t('stopped');
      viewport.hidden=!data?.running;arrange.hidden=!data?.running;
      if(!data?.running){models=[];signature='';source=null;detail.hidden=true;nodes.replaceChildren();svg.replaceChildren();return;}
      if(!data.graph){hint.textContent=t('unavailable');models=[];signature='';detail.hidden=true;nodes.replaceChildren();svg.replaceChildren();return;}
      const policy=data.graph.policy;
      models=[{id:data.self_id,self:true,node:{...policy,forwards:data.forwards||[]}},...(data.graph.nodes||[]).map(m=>({...m,self:false}))].map(m=>({...m,room,node:m.node?{...m.node,forwards:(m.node.forwards||[]).filter(f=>f&&typeof f.addr==='string'&&typeof f.target==='string')}:null}));
      const next=JSON.stringify(models.map(m=>({...m,node:m.node?{...m.node,forwards:m.node.forwards.map(({active_conns,bytes_in,bytes_out,peers,...f})=>f)}:null}))),lang=doc.documentElement.lang;
      if(drag||(next===signature&&lang===language))return;
      signature=next;language=lang;drawNodes();
    }
    return {render};
  }
  root.MistlTunnelGraph={create};
})(window);
