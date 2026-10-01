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

  global.MistlStorage = Object.freeze({ create: create, createFolderAccess: createFolderAccess, visibleItems: visibleItems });
})(window);
