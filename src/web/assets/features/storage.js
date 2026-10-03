/* File-browser presentation. Transport, translations and file operations are
   supplied by the host; this module never reaches into dashboard state. */
(function (global) {
  "use strict";

  function visibleItems(items, query, sort, locale) {
    var needle = String(query || "").trim().toLocaleLowerCase(locale);
    return (items || []).filter(function (item) {
      return !needle || [item.name, item.cid].some(function (value) {
        return String(value || "").toLocaleLowerCase(locale).includes(needle);
      });
    }).slice().sort(function (a, b) {
      if (sort === "size") return (Number(b.size) || 0) - (Number(a.size) || 0) || byName(a, b);
      if (sort === "recent") return (Date.parse(b.stored_at) || 0) - (Date.parse(a.stored_at) || 0) || byName(a, b);
      return byName(a, b);
    });
    function byName(a, b) { return String(a.name || a.cid).localeCompare(String(b.name || b.cid), locale, { numeric: true }); }
  }

  function create(options) {
    var root = options.root;
    var doc = root.ownerDocument;
    var t = options.t;
    var query = "", sort = "name", view = "list", selected = null, items = [];
    var lastSnapshot = null, returnFocus = null;
    var tbody = root.querySelector("#store-table tbody");
    var table = root.querySelector("#store-table");
    var grid = root.querySelector("[data-store-grid]");
    var empty = root.querySelector("#store-empty");
    var detail = root.querySelector("[data-store-detail]");
    var layout = root.querySelector("[data-store-layout]");
    var count = root.querySelector("[data-store-count]");
    var search = root.querySelector("#store-search");
    var sortSelect = root.querySelector("#store-sort");
    var listButton = root.querySelector('[data-store-view="list"]');
    var gridButton = root.querySelector('[data-store-view="grid"]');
    var uploadButton = root.querySelector("[data-store-upload]");

    function node(tag, className, text) {
      var result = doc.createElement(tag);
      if (className) result.className = className;
      if (text != null) result.textContent = String(text);
      return result;
    }
    function icon(kind) {
      var paths = {
        file: '<path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><path d="M14 2v6h6M8 13h8M8 17h5"/>',
        image: '<rect x="3" y="3" width="18" height="18" rx="3"/><circle cx="8" cy="8" r="1.5"/><path d="m21 15-6-6L3 21"/>',
        archive: '<rect x="4" y="2" width="16" height="20" rx="2"/><path d="M12 2v12m-2 2h4v3h-4z"/>',
        audio: '<path d="M9 18V5l12-2v13M9 9l12-2"/><circle cx="6" cy="18" r="3"/><circle cx="18" cy="16" r="3"/>',
        video: '<rect x="2" y="5" width="14" height="14" rx="2"/><path d="m16 10 6-4v12l-6-4"/>'
      };
      var wrap = node("span", "store-file-icon store-file-icon-" + kind);
      // Only constant, local SVG paths enter innerHTML; file metadata stays text.
      wrap.innerHTML = '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">' + paths[kind] + '</svg>';
      return wrap;
    }
    function fileKind(entry) {
      var name = String(entry.name || "").toLowerCase();
      if (/\.(png|jpe?g|gif|webp|avif|bmp|svg)$/.test(name)) return "image";
      if (/\.(zip|gz|tar|7z|rar)$/.test(name)) return "archive";
      if (/\.(mp3|wav|ogg|flac|m4a)$/.test(name)) return "audio";
      if (/\.(mp4|webm|mov|mkv)$/.test(name)) return "video";
      return "file";
    }
    function nameOf(entry) { return entry.name || entry.cid; }
    function dateOf(entry) {
      var ms = Date.parse(entry.stored_at);
      return Number.isFinite(ms) ? new Date(ms).toLocaleString(options.getLocale(), { dateStyle: "medium", timeStyle: "short" }) : (entry.stored_at || "—");
    }
    function download(entry, compact) {
      var link = node("a", "download-link" + (compact ? " store-download-compact" : ""), t("store.download"));
      link.href = options.downloadUrl(entry);
      link.download = nameOf(entry);
      link.title = t("store.browser.downloadFile", { name: nameOf(entry) });
      link.setAttribute("aria-label", link.title);
      return link;
    }
    function openButton(entry, tile) {
      var button = node("button", tile ? "store-tile-open" : "store-file-open");
      button.type = "button";
      button.dataset.storeCid = entry.cid;
      button.title = nameOf(entry);
      button.setAttribute("aria-label", t("store.browser.openFile", { name: nameOf(entry) }));
      button.setAttribute("aria-expanded", String(selected === entry.cid));
      button.setAttribute("aria-controls", "store-file-detail");
      button.appendChild(icon(fileKind(entry)));
      var labels = node("span", "store-file-labels");
      labels.appendChild(node("span", "store-file-name", nameOf(entry)));
      labels.appendChild(node("span", "store-file-kind", t("store.browser.type." + fileKind(entry))));
      button.appendChild(labels);
      button.addEventListener("click", function () {
        selected = entry.cid;
        returnFocus = button;
        renderRows();
        renderDetail(entry);
        detail.querySelector("h3").focus();
      });
      return button;
    }
    function renderRows() {
      var focused = doc.activeElement;
      var focusedCid = focused && focused.dataset && focused.dataset.storeCid;
      var rows = visibleItems(items, query, sort, options.getLocale());
      tbody.replaceChildren();
      grid.replaceChildren();
      table.hidden = view !== "list" || !rows.length;
      grid.hidden = view !== "grid" || !rows.length;
      empty.hidden = rows.length > 0;
      empty.textContent = query.trim() ? t("store.browser.noResults") : t("store.browser.empty");
      count.textContent = t("store.browser.count", { shown: rows.length, total: items.length });
      var total = items.reduce(function (sum, entry) { return sum + (Number(entry.size) || 0); }, 0);
      root.querySelector("[data-store-summary]").textContent = t("store.browser.summary", { count: items.length, size: options.formatBytes(total) });
      listButton.setAttribute("aria-pressed", String(view === "list"));
      gridButton.setAttribute("aria-pressed", String(view === "grid"));
      rows.forEach(function (entry) {
        if (view === "grid") {
          var tile = node("article", "store-file-tile" + (selected === entry.cid ? " selected" : ""));
          tile.appendChild(openButton(entry, true));
          var footer = node("div", "store-tile-footer");
          footer.appendChild(node("span", "muted", options.formatBytes(entry.size)));
          footer.appendChild(download(entry, true));
          tile.appendChild(footer);
          grid.appendChild(tile);
        } else {
          var row = node("tr", selected === entry.cid ? "selected" : "");
          var name = node("td");
          name.appendChild(openButton(entry, false));
          row.appendChild(name);
          row.appendChild(node("td", "store-size-cell", options.formatBytes(entry.size)));
          var date = node("td", "store-date-cell", dateOf(entry));
          date.title = entry.stored_at || "";
          row.appendChild(date);
          var action = node("td", "store-action-cell");
          action.appendChild(download(entry, true));
          row.appendChild(action);
          tbody.appendChild(row);
        }
      });
      if (focusedCid) focusFile(focusedCid);
    }
    function focusFile(cid, scroll) {
      Array.prototype.some.call(root.querySelectorAll("[data-store-cid]"), function (button) {
        if (button.dataset.storeCid !== cid) return false;
        button.focus({ preventScroll: !scroll });
        return true;
      });
    }
    function closeDetail() {
      var cid = selected;
      selected = null;
      detail.hidden = true;
      layout.classList.remove("has-detail");
      renderRows();
      if (returnFocus && returnFocus.isConnected) returnFocus.focus();
      else focusFile(cid, true);
    }
    function renderDetail(entry) {
      detail.dataset.signature = JSON.stringify([options.getLocale(), entry]);
      detail.replaceChildren();
      detail.hidden = false;
      layout.classList.add("has-detail");
      var head = node("div", "store-detail-head");
      head.appendChild(node("span", "store-eyebrow", t("store.browser.details")));
      var close = node("button", "store-detail-close", "×");
      close.type = "button";
      close.title = t("store.browser.close");
      close.setAttribute("aria-label", close.title);
      close.addEventListener("click", closeDetail);
      head.appendChild(close);
      detail.appendChild(head);
      detail.appendChild(icon(fileKind(entry)));
      var title = node("h3", "store-detail-title", nameOf(entry));
      title.id = "store-detail-title";
      title.tabIndex = -1;
      detail.appendChild(title);
      var list = node("dl", "store-detail-fields");
      [["store.browser.kind", t("store.browser.type." + fileKind(entry))], ["store.col.size", options.formatBytes(entry.size)], ["store.col.storedAt", dateOf(entry)], ["store.col.cid", entry.cid]].forEach(function (field) {
        list.appendChild(node("dt", "", t(field[0])));
        list.appendChild(node("dd", field[0] === "store.col.cid" ? "mono" : "", field[1]));
      });
      detail.appendChild(list);
      var actions = node("div", "store-detail-actions");
      actions.appendChild(download(entry, false));
      var copy = node("button", "", t("store.browser.copyCid"));
      copy.type = "button";
      copy.addEventListener("click", function () { options.copy(entry.cid); });
      actions.appendChild(copy);
      detail.appendChild(actions);
      var disclosure = node("details", "store-save-details");
      disclosure.appendChild(node("summary", "", t("store.saveToPath")));
      var form = node("form", "store-detail-save");
      var label = node("label", "", t("store.browser.outputPath"));
      var input = node("input");
      input.type = "text";
      input.placeholder = t("store.savePathPlaceholder");
      label.appendChild(input);
      form.appendChild(label);
      var save = node("button", "", t("store.saveToPath"));
      save.type = "submit";
      form.appendChild(save);
      form.addEventListener("submit", function (event) {
        event.preventDefault();
        options.saveToPath(entry, input.value.trim(), save);
      });
      disclosure.appendChild(form);
      detail.appendChild(disclosure);
    }

    search.addEventListener("input", function () { query = search.value; renderRows(); });
    sortSelect.addEventListener("change", function () { sort = sortSelect.value; renderRows(); });
    [listButton, gridButton].forEach(function (button) {
      button.addEventListener("click", function () { view = button.dataset.storeView; renderRows(); });
    });
    uploadButton.addEventListener("click", options.pickFiles);
    detail.addEventListener("keydown", function (event) {
      if (event.key === "Escape") { event.preventDefault(); closeDetail(); }
    });
    var navigation = root.querySelector(".store-sidebar .subtabs");
    navigation.addEventListener("keydown", function (event) {
      if (event.key !== "ArrowUp" && event.key !== "ArrowDown") return;
      var buttons = Array.prototype.slice.call(navigation.querySelectorAll("button"));
      var index = buttons.indexOf(event.target);
      if (index < 0) return;
      event.preventDefault();
      var next = buttons[(index + (event.key === "ArrowDown" ? 1 : -1) + buttons.length) % buttons.length];
      next.click();
      next.focus();
    });

    return {
      render: function (nextItems) {
        items = Array.isArray(nextItems) ? nextItems : [];
        var snapshot = JSON.stringify([options.getLocale(), items]);
        if (snapshot === lastSnapshot) return;
        lastSnapshot = snapshot;
        renderRows();
        var entry = items.find(function (item) { return item.cid === selected; });
        if (entry) {
          // Polling must not replace a path the user is currently typing.
          var signature = JSON.stringify([options.getLocale(), entry]);
          if (detail.dataset.signature !== signature) renderDetail(entry);
        } else if (selected) closeDetail();
      }
    };
  }

  /* Owner-approval list for folder-key requests (`store.folder-access.*`).
     options: { mount, call(cmd, args) -> Promise, t }. Requester and folder
     names are peer-influenced, so everything is rendered with textContent. */
  function createFolderAccess(options) {
    var mount = options.mount;
    var doc = mount.ownerDocument;
    function label(key, fallback) {
      var value = options.t ? options.t(key) : key;
      return value && value !== key ? value : fallback;
    }
    function node(tag, className, text) {
      var result = doc.createElement(tag);
      if (className) result.className = className;
      if (text != null) result.textContent = String(text);
      return result;
    }
    var lastSnapshot = null;
    function act(button, cmd, args) {
      button.disabled = true;
      options.call(cmd, args).then(refresh, function () { button.disabled = false; });
    }
    function render(pending) {
      var snapshot = JSON.stringify(pending);
      if (snapshot === lastSnapshot) return;
      lastSnapshot = snapshot;
      mount.textContent = "";
      mount.hidden = pending.length === 0;
      if (!pending.length) return;
      mount.appendChild(node("h3", "store-access-title", label("store.access.title", "Folder access requests")));
      pending.forEach(function (req) {
        var row = node("div", "store-access-row");
        var info = node("div", "store-access-info");
        info.appendChild(node("strong", null, req.folder_name || req.folder_id));
        info.appendChild(node("span", "store-access-who", req.requester_did));
        info.appendChild(node("span", "store-access-meta", label("store.access.node", "node") + " " + req.requester_node_id + " · " + req.first_seen));
        var actions = node("div", "store-access-actions");
        var approve = node("button", null, label("store.access.approve", "Approve"));
        var remember = node("button", null, label("store.access.approveRemember", "Approve and remember"));
        var deny = node("button", null, label("store.access.deny", "Deny"));
        approve.type = remember.type = deny.type = "button";
        approve.addEventListener("click", function () { act(approve, "store.folder-access.approve", { id: req.id }); });
        remember.addEventListener("click", function () { act(remember, "store.folder-access.approve", { id: req.id, remember: true }); });
        deny.addEventListener("click", function () { act(deny, "store.folder-access.deny", { id: req.id }); });
        [approve, remember, deny].forEach(function (b) { actions.appendChild(b); });
        row.appendChild(info);
        row.appendChild(actions);
        mount.appendChild(row);
      });
    }
    function refresh() {
      return options.call("store.folder-access.ls", {}).then(function (data) {
        render(Array.isArray(data && data.pending) ? data.pending : []);
      }, function () {});
    }
    mount.hidden = true;
    return { refresh: refresh };
  }

  var shareStrings = {
    en: { title: "My shared folders", share: "Share a folder", empty: "You are not sharing any folders.", path: "Which folder do you want to share?", browse: "Browse…", use: "Use this folder", up: "Up", back: "Back", next: "Next", close: "Close", password: "Choose a passphrase", generate: "Generate", show: "Show", hide: "Hide", passwordHint: "Anyone syncing this folder needs the passphrase. It is not included in the link.", confirm: "Share this folder?", busy: "Encrypting and publishing files… This may take a while.", background: "Sharing continues if you close this window. Check My shared folders for the result.", result: "Folder shared", link: "Share link", copy: "Copy", copied: "Copied", send: "Send this link and passphrase to the other mistl; they paste it in Folder sync.", embedded: "The passphrase is included in this link. Send the link to the other mistl; they paste it in Folder sync.", files: "Files", room: "Room", stop: "Stop", stopConfirm: "Stop sharing {name}? Files already received by others remain on their devices.", required: "Enter a value to continue.", error: "Could not complete the action: ", stopped: "Sharing stopped.", refresh: "Refresh" },
    ja: { title: "自分が共有中のフォルダー", share: "フォルダーを共有", empty: "共有中のフォルダーはありません。", path: "どのフォルダーを共有しますか？", browse: "参照…", use: "このフォルダーを使う", up: "上へ", back: "戻る", next: "次へ", close: "閉じる", password: "パスフレーズを決めてください", generate: "生成", show: "表示", hide: "隠す", passwordHint: "同期する相手にはパスフレーズが必要です。リンクには含まれません。", confirm: "このフォルダーを共有しますか？", busy: "ファイルを暗号化して公開しています… 時間がかかる場合があります。", background: "閉じても共有処理は続きます。結果は共有中の一覧で確認できます。", result: "フォルダーを共有しました", link: "共有リンク", copy: "コピー", copied: "コピーしました", send: "このリンクとパスフレーズを相手の mistl に送り、「フォルダー同期」に貼り付けてもらってください。", embedded: "このリンクにはパスフレーズが含まれます。相手の mistl に送り、「フォルダー同期」に貼り付けてもらってください。", files: "ファイル", room: "ルーム", stop: "停止", stopConfirm: "{name} の共有を停止しますか？ 相手が受信済みのファイルは残ります。", required: "値を入力してください。", error: "操作を完了できませんでした: ", stopped: "共有を停止しました。", refresh: "更新" },
    zh: { title: "我共享的文件夹", share: "共享文件夹", empty: "您尚未共享任何文件夹。", path: "要共享哪个文件夹？", browse: "浏览…", use: "使用此文件夹", up: "上一级", back: "返回", next: "下一步", close: "关闭", password: "设置口令", generate: "生成", show: "显示", hide: "隐藏", passwordHint: "同步此文件夹的人需要口令。链接不包含口令。", confirm: "共享此文件夹？", busy: "正在加密并发布文件… 可能需要一些时间。", background: "关闭窗口后共享仍会继续。请在共享列表中查看结果。", result: "文件夹已共享", link: "共享链接", copy: "复制", copied: "已复制", send: "将此链接和口令发送给另一台 mistl，在“文件夹同步”中粘贴。", embedded: "此链接包含口令。将链接发送给另一台 mistl，在“文件夹同步”中粘贴。", files: "文件", room: "房间", stop: "停止", stopConfirm: "停止共享 {name}？其他人已接收的文件将保留。", required: "请输入内容以继续。", error: "无法完成操作: ", stopped: "共享已停止。", refresh: "刷新" }
  };

  function generateSharePassphrase(crypto) {
    // 24 unbiased base64url characters = 144 bits of entropy.
    var alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    return Array.from(crypto.getRandomValues(new Uint8Array(24)), function (b) { return alphabet[b & 63]; }).join("");
  }
  function encodeSharePayload(payload) {
    var bytes = new TextEncoder().encode(JSON.stringify(payload));
    return "#tc-share=" + global.btoa(Array.from(bytes, function (b) { return String.fromCharCode(b); }).join("")).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
  }
  function folderShareLink(share, ownerDid) {
    if (share.share_url) return share.share_url;
    if (!ownerDid) throw new Error("Missing owner identity");
    // Mirrors storage::sharelink::build_folder_share_link; never include the key.
    return encodeSharePayload({ v: 1, type: "folder-share", roomId: share.room_id, folderId: share.folder_id, folderName: share.folder_name, ownerNodeId: ownerDid, accessGrantMode: share.access_grant_mode || "shared", folderKeyHash: share.folder_key_hash, senderProfile: { name: "mistl" } });
  }
  function shareLinkHasKey(link) {
    try {
      var token = new URLSearchParams(String(link).split("#").pop()).get("tc-share");
      if (!token) return false;
      var base64 = token.replace(/-/g, "+").replace(/_/g, "/");
      var payload = JSON.parse(global.atob(base64 + "=".repeat((4 - base64.length % 4) % 4)));
      return typeof payload.key === "string" && !!payload.key.trim();
    } catch (_) { return false; }
  }

  /* options: { mount, call(cmd,args) -> Promise, getLocale?, copy? }.
     The host owns transport; refresh can join the Folder sync polling loop. */
  function createFolderShare(options) {
    var mount = options.mount, doc = mount.ownerDocument;
    var shares = [], snapshot = null, refreshing = null, modal = null, draft = null, publishing = false;
    function t(key, vars) {
      var locale = String(options.getLocale ? options.getLocale() : doc.documentElement.lang || "en").split("-")[0];
      var text = (shareStrings[locale] || shareStrings.en)[key];
      return vars ? text.replace(/\{(\w+)\}/g, function (_, k) { return vars[k]; }) : text;
    }
    function node(tag, cls, text) {
      var n = doc.createElement(tag);
      if (cls) n.className = cls;
      if (text != null) n.textContent = String(text);
      return n;
    }
    function button(text, cls, action) {
      var b = node("button", cls, text); b.type = "button"; b.addEventListener("click", action); return b;
    }
    var heading = node("div", "store-share-heading"), title = node("h3"), list = node("div", "store-share-list"), status = node("p", "store-share-status");
    status.setAttribute("role", "status");
    var add = button("", "primary", openWizard), reload = button("", "", function () { refresh(); });
    heading.append(title, add, reload); mount.append(heading, list, status);
    function failure(error, target) { (target || status).textContent = t("error") + String(error.message || error); }
    async function copy(text, b, target) {
      try {
        if (options.copy) await options.copy(text);
        else await doc.defaultView.navigator.clipboard.writeText(text);
        b.textContent = t("copied");
      } catch (e) { failure(e, target); }
    }
    async function copyLink(share, b) {
      b.disabled = true;
      try {
        var identity = share.share_url ? null : await options.call("key.did", {});
        await copy(folderShareLink(share, identity && identity.did), b);
      } catch (e) { failure(e); }
      finally { if (b.isConnected) b.disabled = false; }
    }
    function render() {
      title.textContent = t("title"); add.textContent = t("share"); reload.textContent = t("refresh"); add.disabled = publishing;
      var next = JSON.stringify([t("title"), shares]);
      if (snapshot === next) return;
      snapshot = next;
      var active = doc.activeElement, focusId = active && active.dataset.shareId, focusAction = active && active.dataset.shareAction;
      list.replaceChildren();
      if (!shares.length) list.append(node("p", "muted", t("empty")));
      shares.forEach(function (share) {
        var row = node("article", "store-share-row"), info = node("div", "store-share-info"), actions = node("div", "store-share-actions");
        info.append(node("strong", "", share.folder_name || share.folder_id), node("span", "store-share-path", share.local_dir));
        var count = (share.files || []).filter(function (file) { return !file.deletedAt; }).length;
        info.append(node("span", "muted", t("files") + ": " + count + " · " + t("room") + ": " + share.room_id));
        var link = button(t("link") + " · " + t("copy"), "", function () { copyLink(share, link); });
        var stop = button(t("stop"), "danger", function () { openStop(share); });
        [link, stop].forEach(function (b, i) { b.dataset.shareId = share.folder_id; b.dataset.shareAction = String(i); });
        actions.append(link, stop); row.append(info, actions); list.append(row);
      });
      Array.from(list.querySelectorAll("button")).some(function (b) {
        if (b.dataset.shareId !== focusId || b.dataset.shareAction !== focusAction) return false;
        b.focus(); return true;
      });
    }
    function refresh() {
      if (refreshing) return refreshing;
      refreshing = Promise.resolve().then(function () { return options.call("store.folder-share.ls", {}); }).then(function (data) {
        shares = Array.isArray(data && data.shares) ? data.shares : []; render();
      }, function (e) { failure(e); }).finally(function () { refreshing = null; });
      return refreshing;
    }
    function close() {
      if (!modal) return;
      var opener = modal.opener; modal.overlay.remove(); modal = null; draft = null;
      if (opener && opener.isConnected) opener.focus();
    }
    function open(titleText) {
      close();
      var overlay = node("div", "modal-overlay store-share-modal"), panel = node("div", "modal-panel"), head = node("div", "modal-header"), h = node("h3", "", titleText);
      var body = node("div", "store-share-body"), note = node("p", "store-share-status");
      panel.setAttribute("role", "dialog"); panel.setAttribute("aria-modal", "true"); panel.setAttribute("aria-label", titleText); panel.tabIndex = -1;
      note.setAttribute("role", "status");
      var x = button("×", "modal-close", close); x.setAttribute("aria-label", t("close"));
      head.append(h, x); panel.append(head, body, note); overlay.append(panel);
      modal = { overlay: overlay, panel: panel, body: body, note: note, opener: doc.activeElement, title: h };
      // Close only when the press also started on the overlay, so a text
      // selection dragged out of the dialog doesn't close it.
      var pressStarted = false;
      overlay.addEventListener("mousedown", function (e) { pressStarted = e.target === overlay; });
      overlay.addEventListener("click", function (e) { var shouldClose = pressStarted && e.target === overlay; pressStarted = false; if (shouldClose) close(); });
      overlay.addEventListener("keydown", function (e) {
        if (e.key === "Escape") { e.preventDefault(); e.stopPropagation(); close(); }
        if (e.key !== "Tab") return;
        var items = Array.from(panel.querySelectorAll("button, input")).filter(function (n) { return !n.disabled; });
        var first = items[0], last = items[items.length - 1];
        if (e.shiftKey && doc.activeElement === first) { e.preventDefault(); last.focus(); }
        else if (!e.shiftKey && doc.activeElement === last) { e.preventDefault(); first.focus(); }
      });
      doc.body.append(overlay); panel.focus();
    }
    function openWizard() {
      if (publishing) return;
      open(t("share")); draft = { path: "", passphrase: "", step: "path" }; step();
    }
    function step() {
      var current = modal, value = draft;
      current.body.replaceChildren(); current.note.textContent = "";
      current.body.dataset.step = value.step;
      current.title.textContent = t(value.step === "path" ? "path" : value.step === "password" ? "password" : "confirm");
      var form = node("form"), footer = node("div", "store-share-actions"), input;
      if (value.step !== "confirm") {
        input = node("input"); input.type = value.step === "password" ? "password" : "text";
        input.autocomplete = "off"; input.spellcheck = false; input.value = value.step === "path" ? value.path : value.passphrase;
        input.setAttribute("aria-label", current.title.textContent);
        input.addEventListener("input", function () { value[value.step === "path" ? "path" : "passphrase"] = input.value; });
        form.append(input);
        if (value.step === "path") {
          var dirs = node("div", "store-share-dirs");
          form.append(button(t("browse"), "", function () { browse({}, dirs, input, current); }), dirs);
        } else {
          form.append(button(t("generate"), "", function () {
            try { input.value = value.passphrase = generateSharePassphrase(doc.defaultView.crypto); }
            catch (e) { failure(e, current.note); }
          }), button(t("show"), "", function (e) { input.type = input.type === "password" ? "text" : "password"; e.currentTarget.textContent = t(input.type === "password" ? "show" : "hide"); }), node("p", "muted", t("passwordHint")));
        }
      } else {
        form.append(node("p", "store-share-path", value.path), node("p", "muted", t("passwordHint")));
      }
      if (value.step !== "path") footer.append(button(t("back"), "", function () { value.step = value.step === "confirm" ? "password" : "path"; step(); }));
      var next = node("button", "primary", t(value.step === "confirm" ? "share" : "next")); next.type = "submit";
      footer.append(next); form.append(footer); current.body.append(form);
      form.addEventListener("submit", function (e) {
        e.preventDefault();
        if (publishing) return;
        if (input) {
          value[value.step === "path" ? "path" : "passphrase"] = input.value.trim();
          if (!input.value.trim()) { current.note.textContent = t("required"); input.focus(); return; }
          value.step = value.step === "path" ? "password" : "confirm"; step();
        } else publish(current, value);
      });
      if (input) input.focus(); else next.focus();
    }
    async function browse(args, target, input, current) {
      var request = {}; current.browseRequest = request;
      target.replaceChildren(node("p", "muted", "…"));
      try {
        var data = await options.call("store.browse-dirs", args);
        if (modal !== current || current.browseRequest !== request || !target.isConnected) return;
        target.replaceChildren();
        (data.roots || []).forEach(function (r) { target.append(button(r.label, "", function () { browse({ path: r.path }, target, input, current); })); });
        if (data.path) {
          target.append(node("p", "store-share-path", data.path), button(t("use"), "primary", function () { input.value = draft.path = data.path; target.replaceChildren(); input.focus(); }));
        }
        if (data.parent) target.append(button(t("up"), "", function () { browse({ path: data.parent }, target, input, current); }));
        (data.dirs || []).forEach(function (d) { target.append(button(d.name, "store-share-dir", function () { browse({ path: d.path }, target, input, current); })); });
      } catch (e) { if (modal === current && current.browseRequest === request) { target.replaceChildren(); failure(e, current.note); } }
    }
    async function publish(current, value) {
      publishing = true; render();
      current.body.dataset.step = "busy"; current.body.replaceChildren(node("p", "", t("busy")), node("p", "muted", t("background")));
      current.panel.setAttribute("aria-busy", "true"); current.panel.focus();
      try {
        var result = await options.call("store.folder-share", { path: value.path, passphrase: value.passphrase });
        if (!result || !result.share_url) throw new Error("Missing share link in response");
        if (modal === current) {
          current.body.dataset.step = "result"; current.title.textContent = t("result");
          current.body.replaceChildren();
          var link = node("input"); link.type = "text"; link.readOnly = true; link.value = result.share_url; link.setAttribute("aria-label", t("link"));
          var copyButton = button(t("copy"), "", function () { copy(result.share_url, copyButton, current.note); });
          current.body.append(link, copyButton, node("p", "muted", t(shareLinkHasKey(result.share_url) ? "embedded" : "send")));
          if (!shareLinkHasKey(result.share_url)) {
            var secret = node("input"); secret.type = "password"; secret.readOnly = true; secret.value = value.passphrase; secret.setAttribute("aria-label", t("password"));
            var secretCopy = button(t("copy"), "", function () { copy(secret.value, secretCopy, current.note); });
            current.body.append(secret, secretCopy, button(t("show"), "", function (e) { secret.type = secret.type === "password" ? "text" : "password"; e.currentTarget.textContent = t(secret.type === "password" ? "show" : "hide"); }));
          }
          current.body.append(button(t("close"), "primary", close)); link.focus();
        }
      } catch (e) {
        if (modal === current) { value.step = "confirm"; step(); failure(e, current.note); }
        else failure(e);
      } finally {
        publishing = false; if (modal === current) current.panel.removeAttribute("aria-busy"); render(); await refresh();
      }
    }
    function openStop(share) {
      open(t("stop")); var current = modal;
      current.body.append(node("p", "", t("stopConfirm", { name: share.folder_name || share.folder_id })));
      var stop = button(t("stop"), "danger", async function () {
        stop.disabled = true;
        try { await options.call("store.folder-share.stop", { folder_id: share.folder_id }); if (modal === current) close(); status.textContent = t("stopped"); await refresh(); }
        catch (e) { stop.disabled = false; failure(e, modal === current ? current.note : status); }
      });
      current.body.append(button(t("back"), "", close), stop);
    }
    render();
    return { refresh: refresh, open: openWizard };
  }

  global.MistlStorage = Object.freeze({ create: create, createFolderAccess: createFolderAccess, createFolderShare: createFolderShare, visibleItems: visibleItems, generateSharePassphrase: generateSharePassphrase, folderShareLink: folderShareLink, shareLinkHasKey: shareLinkHasKey });
})(window);
