/* Nostr relay presentation; the dashboard shell owns transport and polling. */
(function (global) {
  "use strict";
  function create(options) {
    var root = options.root, t = options.t, last = null, busy = false;
    var actionError = "", statusError = "";
    var toggle = root.querySelector("[data-nostr-toggle]");
    var copy = root.querySelector("[data-nostr-copy]");
    var error = root.querySelector("[data-nostr-error]");
    var rooms = root.querySelector("[data-nostr-rooms]"), rows = new Map();
    function updateError() {
      error.textContent = actionError || statusError;
      error.hidden = !error.textContent;
    }
    function text(selector, value) { root.querySelector(selector).textContent = value; }
    function count(value) { return Number.isFinite(value) && value >= 0 ? String(value) : "0"; }
    function render(data) {
      last = data;
      var running = !!(data && data.running);
      text("[data-nostr-state]", t(!data ? "nostr.loading" : running ? "nostr.running" : "nostr.stopped"));
      root.querySelector("[data-nostr-state]").dataset.running = String(running);
      toggle.textContent = t(running ? "nostr.stop" : "nostr.start");
      toggle.disabled = busy || !data;
      var url = running && typeof data.url === "string" ? data.url : "";
      text("[data-nostr-url]", url || "—");
      copy.disabled = !url;
      var tls = running && data.tls;
      text("[data-nostr-tls]", !running ? "—" : !tls ? t("nostr.noTls") :
        t(tls.source === "self-signed" ? "nostr.selfSigned" : "nostr.pem"));
      text("[data-nostr-fingerprint]", tls && typeof tls.sha256 === "string" ? "SHA-256: " + tls.sha256 : "");
      root.querySelector("[data-nostr-tls-hint]").hidden = !tls || tls.source !== "self-signed";
      ["events", "connections", "subscriptions"].forEach(function (key) {
        text("[data-nostr-" + key + "]", running ? count(data[key]) : "0");
      });
      var meshRooms = running && data.mesh && Array.isArray(data.mesh.rooms) ? data.mesh.rooms : [];
      var configured = running && Array.isArray(data.rooms) ? data.rooms : [];
      var list = meshRooms.length ? meshRooms : configured.map(function (room) { return { room: room }; });
      var kept = new Set();
      list.forEach(function (item) {
        var key = String(item.room), row = rows.get(key);
        if (!row) {
          row = document.createElement("li");
          var name = document.createElement("span"), peers = document.createElement("span");
          name.className = "mono"; peers.className = "nostr-peers";
          row.append(name, peers); rows.set(key, row); rooms.appendChild(row);
        }
        kept.add(key); row.firstChild.textContent = key;
        row.lastChild.textContent = Number.isFinite(item.peers) ? t("nostr.peers", { count: count(item.peers) }) : t("nostr.meshWaiting");
      });
      rows.forEach(function (row, key) { if (!kept.has(key)) { row.remove(); rows.delete(key); } });
      rooms.hidden = !list.length;
      root.querySelector("[data-nostr-rooms-hint]").hidden = !!list.length;
      updateError();
    }
    toggle.addEventListener("click", function () {
      if (busy || !last) return;
      var running = !!last.running;
      busy = true; actionError = ""; render(last);
      Promise.resolve().then(function () { return options.setRunning(!running); }).then(render).catch(function (err) {
        actionError = err.message || String(err); updateError();
      }).finally(function () { busy = false; render(last); });
    });
    copy.addEventListener("click", function () {
      if (last && last.running && typeof last.url === "string") options.copy(last.url);
    });
    render(null);
    return {
      render: render,
      setStatusError: function (message) { statusError = message || ""; updateError(); },
      refreshLanguage: function () { render(last); }
    };
  }
  global.MistlNostr = { create: create };
})(window);
