/* tc-chat presentation adapter. The view owns DOM and selection only; all
   transport, configuration and navigation are supplied through explicit ports. */
(function (global) {
  "use strict";

  var copy = {
    en: {
      rooms: "Rooms", findRoom: "Find a room", search: "Search this conversation", history: "Conversation history",
      joined: "Connected", waiting: "Connecting", roomCount: "rooms", noRooms: "Your conversations start here",
      configure: "Set up rooms", noRoomsHint: "Add a tc-chat room in settings to see its conversation here.",
      disabled: "Turn on tc-chat in settings to see your conversations.", empty: "No messages yet",
      emptyHint: "Messages will appear here as they arrive in this room.", noMatches: "No matching messages",
      noMatchesHint: "Try another word or sender name.", noRoomMatches: "No matching rooms", loading: "Loading conversation…",
      error: "Couldn't load the conversation. Try refreshing.", refresh: "Refresh", unknown: "Unknown sender",
      readOnly: "Conversation preview", readOnlyHint: "Use tc-chat to send messages and replies.", open: "Open in tc-chat ↗",
      details: "Message details", close: "Close", author: "Sender", identity: "Sender ID", sent: "Sent", room: "Room",
      kind: "Type", technical: "Technical details", latest: "Latest messages ↓", today: "Today", yesterday: "Yesterday",
      post: "Message", media: "Media", file: "File", project: "Board post", event: "Calendar event", reaction: "Reaction",
      edit: "Message edited", deleted: "Message deleted", reactionAdded: "Reacted", reactionRemoved: "Removed reaction",
      unavailable: "Message content isn't available in this preview.", editHint: "A message was edited. Open tc-chat to see its current content.",
      deleteHint: "A message was deleted.", target: "Related message", attachment: "Attachment", verified: "Verified sender",
      count: "{count} recent messages", editedContent: "This message has been edited. Open tc-chat for the latest version.",
    },
    ja: {
      rooms: "ルーム", findRoom: "ルームを検索", search: "会話を検索", history: "会話履歴",
      joined: "接続中", waiting: "接続待ち", roomCount: "ルーム", noRooms: "ここから会話をつなげましょう",
      configure: "ルームを設定", noRoomsHint: "設定でtc-chatのルームを追加すると、ここに会話が表示されます。",
      disabled: "設定でtc-chatを有効にすると、会話を確認できます。", empty: "まだメッセージはありません",
      emptyHint: "このルームに届いたメッセージが、ここに表示されます。", noMatches: "一致するメッセージはありません",
      noMatchesHint: "別の言葉や送信者の名前で検索してください。", noRoomMatches: "一致するルームはありません", loading: "会話を読み込み中…",
      error: "会話を読み込めませんでした。更新して再度お試しください。", refresh: "更新", unknown: "名前未設定",
      readOnly: "会話のプレビュー", readOnlyHint: "メッセージの送信や返信はtc-chatから行えます。", open: "tc-chatで開く ↗",
      details: "メッセージの詳細", close: "閉じる", author: "送信者", identity: "送信者ID", sent: "送信日時", room: "ルーム",
      kind: "種類", technical: "技術情報", latest: "最新のメッセージ ↓", today: "今日", yesterday: "昨日",
      post: "メッセージ", media: "メディア", file: "ファイル", project: "ボードの投稿", event: "カレンダーの予定", reaction: "リアクション",
      edit: "メッセージを編集", deleted: "メッセージを削除", reactionAdded: "リアクション", reactionRemoved: "リアクションを解除",
      unavailable: "このプレビューではメッセージの本文を表示できません。", editHint: "メッセージが編集されました。最新の内容はtc-chatで確認できます。",
      deleteHint: "メッセージが削除されました。", target: "関連メッセージ", attachment: "添付ファイル", verified: "本人確認済み",
      count: "最近のメッセージ {count} 件", editedContent: "編集済みのメッセージです。最新の内容はtc-chatで確認できます。",
    },
    zh: {
      rooms: "房间", findRoom: "搜索房间", search: "搜索对话", history: "对话记录",
      joined: "已连接", waiting: "连接中", roomCount: "个房间", noRooms: "从这里开始对话",
      configure: "设置房间", noRoomsHint: "在设置中添加 tc-chat 房间，即可在这里查看对话。",
      disabled: "在设置中启用 tc-chat，即可查看对话。", empty: "暂无消息",
      emptyHint: "此房间收到的消息将显示在这里。", noMatches: "没有匹配的消息",
      noMatchesHint: "请尝试其他关键词或发送者名称。", noRoomMatches: "没有匹配的房间", loading: "正在加载对话…",
      error: "无法加载对话。请刷新重试。", refresh: "刷新", unknown: "未设置名称",
      readOnly: "对话预览", readOnlyHint: "请使用 tc-chat 发送消息和回复。", open: "在 tc-chat 中打开 ↗",
      details: "消息详情", close: "关闭", author: "发送者", identity: "发送者 ID", sent: "发送时间", room: "房间",
      kind: "类型", technical: "技术信息", latest: "最新消息 ↓", today: "今天", yesterday: "昨天",
      post: "消息", media: "媒体", file: "文件", project: "看板帖子", event: "日历事件", reaction: "回应",
      edit: "消息已编辑", deleted: "消息已删除", reactionAdded: "添加回应", reactionRemoved: "移除回应",
      unavailable: "此预览无法显示消息内容。", editHint: "一条消息已编辑。请在 tc-chat 中查看最新内容。",
      deleteHint: "一条消息已删除。", target: "相关消息", attachment: "附件", verified: "身份已验证",
      count: "最近 {count} 条消息", editedContent: "此消息已编辑。请在 tc-chat 中查看最新内容。",
    },
  };

  function safeRoomUrl(value) {
    try {
      var url = new URL(value);
      return url.protocol === "https:" || url.protocol === "http:" ? url.href : null;
    } catch (_) { return null; }
  }

  function create(options) {
    var root = options.root;
    var ports = options.ports;
    var doc = root.ownerDocument;
    var rooms = [], selected = "", enabled = false, initialized = false;
    var items = [], rendered = [], renderedLanguage = "", query = "", roomQuery = "";
    var roomsSignature = "", requestVersion = 0, roomRequest = 0, loading = false, loadError = false;
    var pendingLog = null, pendingRooms = null, detailMessageId = null;
    var detailSignature = "", detailTrigger = null, detailTriggerClass = "mc-message-more";

    function language() { var value = ports.language ? ports.language() : "en"; return copy[value] ? value : "en"; }
    function t(key) { return copy[language()][key] || copy.en[key] || key; }
    function el(tag, cls, text) {
      var node = doc.createElement(tag);
      if (cls) node.className = cls;
      if (text !== undefined) node.textContent = String(text);
      return node;
    }
    function button(cls, text, action) {
      var node = el("button", cls, text); node.type = "button"; node.addEventListener("click", action); return node;
    }
    function searchInput(action) {
      var node = el("input"); node.type = "search"; node.autocomplete = "off";
      node.addEventListener("input", function () { action(node.value); }); return node;
    }
    function attr(node, name, value) { node.setAttribute(name, value); }
    function notify() {
      if (ports.changed) ports.changed({ room: selected || null, items: items.slice(), status: { enabled: enabled, rooms: rooms.slice() } });
    }
    function validDate(value) {
      if (typeof value !== "number" || !Number.isFinite(value)) return null;
      var date = new Date(value); return Number.isNaN(date.getTime()) ? null : date;
    }
    function sender(item) { return String(item.fromName || item.fromId || t("unknown")); }
    function kind(item) {
      if (item.type === "tc-chat:reaction") return t("reaction");
      if (item.type === "tc-chat:post-edit") return t("edit");
      if (item.type === "tc-chat:post-delete") return t("deleted");
      return t(({media: "media", file: "file", project: "project", event: "event"})[item.kind] || "post");
    }
    function content(item) {
      if (item._deleted || item.type === "tc-chat:post-delete") return t("deleteHint");
      if (item._edited || item.type === "tc-chat:post-edit") return t("editHint");
      if (item.type === "tc-chat:reaction") return (item.emoji || "") + " " + t(item.op === "remove" ? "reactionRemoved" : "reactionAdded");
      if (item.kind === "file" || item.kind === "media") {
        return String(item.fileName || t("attachment")) + (item.fileSize && ports.formatBytes ? " · " + ports.formatBytes(item.fileSize) : "");
      }
      return typeof item.text === "string" && item.text ? item.text : t("unavailable");
    }
    // Relay edits do not include replacement bodies. Never show stale/deleted
    // originals as if they were the current message in a conversation preview.
    function displayItems() {
      var edits = new Set(), deletes = new Set();
      items.forEach(function (item) {
        // A valid signature identifies the actor; it does not authorize them
        // to edit somebody else's message. Match tc-chat's ownership check.
        if (typeof item.targetId !== "string" || typeof item.fromId !== "string") return;
        var target = JSON.stringify([item.targetId, item.fromId]);
        if (item.type === "tc-chat:post-edit") edits.add(target);
        if (item.type === "tc-chat:post-delete") deletes.add(target);
      });
      return items.map(function (item) {
        if (item.type !== "tc-chat:post") return item;
        var target = JSON.stringify([item.id, item.fromId]);
        return Object.assign({}, item, { _edited: edits.has(target), _deleted: deletes.has(target) });
      });
    }

    root.classList.add("mistl-chat");
    var sidebar = el("aside", "mc-sidebar");
    var brand = el("div", "mc-brand");
    brand.append(el("span", "mc-brand-mark", "#"), el("strong", "", "tc-chat"));
    var roomSearch = searchInput(function (value) { roomQuery = value.toLocaleLowerCase().trim(); renderRooms(true); });
    var roomLabel = el("div", "mc-section-label");
    var roomList = el("nav", "mc-rooms");
    var setup = button("mc-setup", "", function () { if (ports.openSettings) ports.openSettings(); });
    sidebar.append(brand, roomSearch, roomLabel, roomList, setup);
    var main = el("div", "mc-main");
    var header = el("div", "mc-header");
    var heading = el("div", "mc-heading");
    var roomName = el("h3", "mc-room-name");
    var roomStatus = el("span", "mc-room-status");
    heading.append(roomName, roomStatus);
    var messageSearch = searchInput(function (value) { query = value.toLocaleLowerCase().trim(); renderMessages(true); });
    header.append(heading, messageSearch);
    var viewport = el("div", "mc-viewport");
    attr(viewport, "tabindex", "0"); attr(viewport, "role", "region");
    var messageList = el("div", "mc-messages");
    var empty = el("div", "mc-empty");
    attr(empty, "role", "status");
    var emptyIcon = el("span", "mc-empty-icon", "#"); attr(emptyIcon, "aria-hidden", "true");
    var emptyTitle = el("strong"), emptyHint = el("p");
    empty.append(emptyIcon, emptyTitle, emptyHint);
    viewport.append(messageList, empty);
    var latest = button("mc-latest", "", function () { viewport.scrollTop = viewport.scrollHeight; latest.hidden = true; });
    latest.hidden = true;
    viewport.addEventListener("scroll", function () { if (nearBottom()) latest.hidden = true; });
    var footer = el("div", "mc-footer");
    var footerCopy = el("div"), previewLabel = el("strong"), previewHint = el("span");
    footerCopy.append(previewLabel, previewHint);
    var openLink = el("a", "mc-open"); openLink.target = "_blank"; openLink.rel = "noopener noreferrer";
    footer.append(footerCopy, openLink);
    var errorNote = el("div", "mc-error"); attr(errorNote, "role", "status"); errorNote.hidden = true;
    main.append(header, errorNote, viewport, latest, footer);
    var dialog = el("dialog", "mc-dialog");
    attr(dialog, "aria-labelledby", "chat-relay-detail-heading");
    var detailHead = el("div", "mc-detail-head");
    var detailTitle = el("h3"); detailTitle.id = "chat-relay-detail-heading";
    var close = button("mc-close", "×", function () { dialog.close(); });
    detailHead.append(detailTitle, close);
    var detailBody = el("div", "mc-detail-body"); dialog.append(detailHead, detailBody);
    dialog.addEventListener("close", function () {
      // Native focus restoration cannot reach an opener removed by a log
      // refresh. Find its current equivalent before falling back to the room.
      if (detailTrigger && detailTrigger.isConnected) return;
      var row = Array.from(messageList.children).find(function (node) { return node.dataset.messageKey === String(detailMessageId); });
      var target = row && row.querySelector("." + detailTriggerClass);
      if (!target) target = enabled && roomList.querySelector(".is-active") || setup;
      target.focus({ preventScroll: true });
    });
    dialog.addEventListener("click", function (event) { if (event.target === dialog) { var r = dialog.getBoundingClientRect(); if (event.clientX < r.left || event.clientX > r.right || event.clientY < r.top || event.clientY > r.bottom) dialog.close(); } });
    root.replaceChildren(sidebar, main, dialog);

    function nearBottom() { return viewport.scrollHeight - viewport.scrollTop - viewport.clientHeight < 64; }
    function renderLabels() {
      roomSearch.placeholder = t("findRoom"); attr(roomSearch, "aria-label", t("findRoom"));
      messageSearch.placeholder = t("search"); attr(messageSearch, "aria-label", t("search"));
      attr(roomList, "aria-label", t("rooms")); attr(viewport, "aria-label", t("history"));
      setup.textContent = t("configure"); latest.textContent = t("latest");
      previewLabel.textContent = t("readOnly"); previewHint.textContent = t("readOnlyHint");
      detailTitle.textContent = t("details"); attr(close, "aria-label", t("close")); close.title = t("close");
      openLink.textContent = t("open");
    }
    function renderHeader() {
      roomName.textContent = selected ? "# " + selected : "tc-chat";
      roomName.title = selected;
      var room = rooms.find(function (entry) { return entry.room === selected; });
      roomStatus.textContent = room ? t(room.joined ? "joined" : "waiting") + " · " + t("count").replace("{count}", items.length) : t("history");
      roomStatus.classList.toggle("is-connected", !!(enabled && room && room.joined));
      var url = selected && ports.roomUrl ? safeRoomUrl(ports.roomUrl(selected)) : null;
      openLink.hidden = !url;
      if (url) openLink.href = url; else openLink.removeAttribute("href");
      messageSearch.disabled = !selected || !enabled;
      errorNote.hidden = !loadError; errorNote.textContent = loadError ? t("error") : "";
    }
    function renderRooms(force) {
      var signature = JSON.stringify([rooms, selected, enabled, language(), roomQuery]);
      if (!force && signature === roomsSignature) return;
      roomsSignature = signature; roomList.replaceChildren();
      roomLabel.textContent = t("rooms") + " · " + rooms.length;
      var visible = rooms.filter(function (room) { return room.room.toLocaleLowerCase().includes(roomQuery); });
      visible.forEach(function (room) {
        var row = button("mc-room" + (room.room === selected ? " is-active" : ""), undefined, function () { selectRoom(room.room); });
        row.title = room.room; attr(row, "aria-pressed", String(room.room === selected));
        var hash = el("span", "mc-room-hash", "#"); attr(hash, "aria-hidden", "true");
        var label = el("span", "mc-room-label", room.room);
        var dot = el("span", "mc-room-dot" + (enabled && room.joined ? " is-connected" : ""));
        dot.title = t(enabled && room.joined ? "joined" : "waiting"); attr(dot, "aria-label", dot.title);
        row.append(hash, label, dot); roomList.appendChild(row);
      });
      if (!visible.length && roomQuery) roomList.appendChild(el("p", "mc-room-empty", t("noRoomMatches")));
    }
    function renderEmpty(visibleCount) {
      empty.hidden = visibleCount > 0;
      var title, hint;
      if (!initialized || loading && !items.length) { title = "loading"; hint = null; }
      else if (loadError && !items.length) { title = "error"; hint = null; }
      else if (!rooms.length) { title = "noRooms"; hint = "noRoomsHint"; }
      else if (!enabled) { title = "noRooms"; hint = "disabled"; }
      else if (query) { title = "noMatches"; hint = "noMatchesHint"; }
      else { title = "empty"; hint = "emptyHint"; }
      emptyTitle.textContent = t(title); emptyHint.textContent = hint ? t(hint) : "";
    }
    function dateLabel(date) {
      var today = new Date(), yesterday = new Date(); yesterday.setDate(today.getDate() - 1);
      if (date.toDateString() === today.toDateString()) return t("today");
      if (date.toDateString() === yesterday.toDateString()) return t("yesterday");
      return date.toLocaleDateString(language(), { year: "numeric", month: "short", day: "numeric" });
    }
    function showDetails(item, trigger) {
      var signature = JSON.stringify([item, selected, language()]);
      if (dialog.open && signature === detailSignature) return;
      var technicalWasOpen = dialog.open && detailBody.querySelector(".mc-technical").open;
      var focusInBody = dialog.open && detailBody.contains(doc.activeElement);
      if (!dialog.open) {
        detailTrigger = trigger || doc.activeElement;
        detailTriggerClass = detailTrigger && detailTrigger.classList.contains("mc-avatar") ? "mc-avatar" : detailTrigger && detailTrigger.classList.contains("mc-author") ? "mc-author" : "mc-message-more";
      }
      detailMessageId = item.id;
      detailSignature = signature;
      detailBody.replaceChildren();
      detailBody.appendChild(el("p", "mc-detail-text", content(item)));
      var fields = el("dl");
      var date = validDate(item.timestamp);
      [["author", sender(item)], ["identity", item.fromId], ["sent", date ? date.toLocaleString(language()) : "—"], ["room", selected], ["kind", kind(item)], ["target", item.targetId]].forEach(function (pair) {
        if (!pair[1]) return; fields.append(el("dt", "", t(pair[0])), el("dd", "", pair[1]));
      });
      detailBody.appendChild(fields);
      var technical = el("details", "mc-technical");
      technical.open = !!technicalWasOpen;
      technical.appendChild(el("summary", "", t("technical")));
      var wire = Object.assign({}, item); delete wire._edited; delete wire._deleted;
      // A deleted/edited message's former text must not reappear in the drawer.
      if (item._deleted || item._edited) delete wire.text;
      technical.appendChild(el("pre", "", JSON.stringify(wire, null, 2)));
      detailBody.appendChild(technical);
      if (!dialog.open) dialog.showModal();
      else if (focusInBody) technical.querySelector("summary").focus({ preventScroll: true });
    }
    function messageNode(item) {
      var row = el("article", "mc-message" + (item.type !== "tc-chat:post" || item._deleted || item._edited ? " is-event" : ""));
      var name = sender(item), initial = Array.from(name.trim())[0] || "?";
      var avatar = button("mc-avatar", initial.toLocaleUpperCase(), function (event) { showDetails(item, event.currentTarget); });
      avatar.title = name; attr(avatar, "aria-label", t("author") + ": " + name);
      var hash = Array.from(String(item.fromId || name)).reduce(function (value, ch) { return (value * 31 + ch.charCodeAt(0)) >>> 0; }, 0);
      avatar.style.setProperty("--mc-avatar-hue", hash % 360);
      var column = el("div", "mc-message-column");
      var meta = el("div", "mc-message-meta");
      var author = button("mc-author", name, function (event) { showDetails(item, event.currentTarget); }); author.title = String(item.fromId || name);
      var date = validDate(item.timestamp), time = el("time", "mc-time", date ? date.toLocaleTimeString(language(), { hour: "2-digit", minute: "2-digit" }) : "—");
      if (date) { time.dateTime = date.toISOString(); time.title = date.toLocaleString(language()); }
      meta.append(author, time);
      var more = button("mc-message-more", "···", function (event) { showDetails(item, event.currentTarget); }); more.title = t("details"); attr(more, "aria-label", t("details"));
      meta.appendChild(more);
      var bubble = el("div", "mc-bubble");
      if (item.kind && item.kind !== "text" || item.type !== "tc-chat:post") bubble.appendChild(el("span", "mc-kind", kind(item)));
      bubble.appendChild(el("div", "mc-message-text", content(item)));
      column.append(meta, bubble); row.append(avatar, column); return row;
    }
    function renderMessages(reset) {
      var list = enabled ? displayItems().filter(function (item) {
        return !query || (sender(item) + " " + content(item) + " " + kind(item)).toLocaleLowerCase().includes(query);
      }) : [];
      var signatures = list.map(function (item) { return JSON.stringify(item); });
      var sameLanguage = renderedLanguage === language();
      var same = sameLanguage && JSON.stringify(signatures) === JSON.stringify(rendered);
      renderEmpty(list.length); renderHeader();
      if (same && !reset) return;
      var follow = nearBottom(), oldTop = viewport.scrollTop;
      var append = !reset && sameLanguage && rendered.length <= signatures.length && rendered.every(function (key, i) { return key === signatures[i]; });
      var start = append ? rendered.length : 0;
      var anchor = null, anchorOffset = 0;
      if (!append && !reset && !follow) {
        Array.from(messageList.children).some(function (node) {
          if (node.dataset.messageKey && node.offsetTop >= oldTop) { anchor = node.dataset.messageKey; anchorOffset = node.offsetTop - oldTop; return true; }
          return false;
        });
      }
      if (!append) messageList.replaceChildren();
      for (var i = start; i < list.length; i++) {
        var date = validDate(list[i].timestamp), prev = i ? validDate(list[i - 1].timestamp) : null;
        if (date && (!prev || date.toDateString() !== prev.toDateString())) messageList.appendChild(el("div", "mc-date", dateLabel(date)));
        var node = messageNode(list[i]); node.dataset.messageKey = String(list[i].id || signatures[i]); messageList.appendChild(node);
      }
      rendered = signatures; renderedLanguage = language();
      if (reset && query) viewport.scrollTop = 0;
      else if (reset || follow) { viewport.scrollTop = viewport.scrollHeight; latest.hidden = true; }
      else {
        var restored = anchor && Array.from(messageList.children).find(function (node) { return node.dataset.messageKey === anchor; });
        viewport.scrollTop = restored ? restored.offsetTop - anchorOffset : oldTop;
        if (append && list.length > start) latest.hidden = false;
      }
    }
    function renderAll(reset) {
      renderLabels(); renderRooms(); renderMessages(!!reset);
      if (dialog.open) {
        var current = displayItems().find(function (item) { return item.id === detailMessageId; });
        if (current) showDetails(current); else dialog.close();
      }
    }
    function selectRoom(room) {
      if (room === selected) return pendingLog || Promise.resolve();
      selected = room; items = []; rendered = []; requestVersion++; pendingLog = null; query = ""; messageSearch.value = "";
      if (dialog.open) dialog.close();
      latest.hidden = true; loading = enabled && !!room; loadError = false;
      renderAll(true); notify(); return loadLog();
    }
    function loadLog() {
      if (!enabled || !selected) { loading = false; renderAll(); return Promise.resolve(); }
      if (pendingLog) return pendingLog;
      var room = selected, version = ++requestVersion;
      loading = true; renderEmpty(items.length);
      pendingLog = Promise.resolve().then(function () { return ports.readLog(room, 100); }).then(function (result) {
        if (version !== requestVersion || room !== selected || !enabled) return;
        items = Array.isArray(result) ? result.filter(function (item) { return item && typeof item === "object"; }) : [];
        loading = false; loadError = false; renderAll(); notify();
      }, function () {
        if (version !== requestVersion || room !== selected) return;
        loading = false; loadError = true; renderAll();
      }).finally(function () { if (version === requestVersion) pendingLog = null; });
      return pendingLog;
    }
    function setRooms(status) {
      initialized = true;
      loadError = false;
      rooms = status && Array.isArray(status.rooms) ? status.rooms.filter(function (room) { return room && typeof room.room === "string" && room.room; }) : [];
      var wasEnabled = enabled; enabled = !!(status && status.enabled);
      if (!enabled) { requestVersion++; pendingLog = null; items = []; loading = false; }
      var next = rooms.some(function (room) { return room.room === selected; }) ? selected : rooms.length ? rooms[0].room : "";
      if (next !== selected) return selectRoom(next);
      if (!wasEnabled && enabled) loadError = false;
      renderAll(); notify(); return loadLog();
    }
    function refresh() {
      if (pendingRooms) return pendingRooms;
      var version = ++roomRequest;
      pendingRooms = Promise.resolve().then(function () { return ports.readRooms(); }).then(function (status) {
        if (version === roomRequest) return setRooms(status);
      }, function () { initialized = true; loading = false; loadError = true; renderAll(); }).finally(function () { if (version === roomRequest) pendingRooms = null; });
      return pendingRooms;
    }
    renderAll();
    return { refresh: refresh, setRooms: setRooms, selectedRoom: function () { return selected; } };
  }
  global.MistlChat = Object.freeze({ create: create });
})(window);
