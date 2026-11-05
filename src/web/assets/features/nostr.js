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
    var upstreams = root.querySelector("[data-nostr-upstreams]");
    var upstreamRows = root.querySelector("[data-nostr-upstream-rows]");
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
      var meshRooms = running && data.mesh && Array.isArray(data.mesh.rooms) ? data.mesh.rooms.filter(function (item) {
        return item && typeof item.room === "string";
      }) : [];
      var configured = running && Array.isArray(data.rooms) ? data.rooms.filter(function (room) {
        return typeof room === "string";
      }) : [];
      var list = meshRooms.length ? meshRooms : configured.map(function (room) { return { room: room }; });
      var kept = new Set();
      list.forEach(function (item) {
        var key = String(item.room), row = rows.get(key);
        if (!row) {
          row = document.createElement("li");
          var name = document.createElement("span"), details = document.createElement("span");
          var peers = document.createElement("span"), role = document.createElement("span");
          var leader = document.createElement("span");
          name.className = "mono"; peers.className = "nostr-peers";
          details.className = "nostr-room-details"; role.className = "nostr-role";
          leader.className = "nostr-leader";
          details.append(peers, role, leader); row.append(name, details);
          rows.set(key, row); rooms.appendChild(row);
        }
        kept.add(key); row.firstChild.textContent = key;
        row.querySelector(".nostr-peers").textContent = Number.isFinite(item.peers) ? t("nostr.peers", { count: count(item.peers) }) : t("nostr.meshWaiting");
        var roleBadge = row.querySelector(".nostr-role");
        roleBadge.hidden = typeof item.role !== "string";
        var roleKey = item.role === "leader" || item.role === "follower" ? item.role : "electing";
        roleBadge.dataset.role = roleKey;
        roleBadge.textContent = t("nostr.role." + roleKey);
        var leaderLabel = row.querySelector(".nostr-leader");
        leaderLabel.hidden = typeof item.leader !== "string" || !item.leader;
        leaderLabel.textContent = leaderLabel.hidden ? "" : t("nostr.leader", { id: item.leader.slice(0, 8) });
      });
      rows.forEach(function (row, key) { if (!kept.has(key)) { row.remove(); rows.delete(key); } });
      rooms.hidden = !list.length;
      root.querySelector("[data-nostr-rooms-hint]").hidden = !!list.length;
      var upstreamData = data && data.upstreams && typeof data.upstreams === "object" && !Array.isArray(data.upstreams) ? data.upstreams : null;
      upstreams.hidden = !upstreamData;
      upstreamRows.replaceChildren();
      if (upstreamData) {
        text("[data-nostr-gateway-hint]", t(upstreamData.gateway === true ? "nostr.gateway" :
          upstreamData.gateway === false ? "nostr.viaLeader" : "nostr.gatewayUnknown"));
        var interest = root.querySelector("[data-nostr-interest]");
        interest.hidden = !Number.isSafeInteger(upstreamData.interest) || upstreamData.interest < 0;
        interest.textContent = interest.hidden ? "" : t("nostr.interest", { count: count(upstreamData.interest) });
        var relays = Array.isArray(upstreamData.relays) ? upstreamData.relays : [];
        root.querySelector("[data-nostr-upstream-empty]").hidden = !!relays.length;
        relays.forEach(function (relay) {
          relay = relay && typeof relay === "object" ? relay : {};
          var row = document.createElement("li"), url = document.createElement("span");
          var details = document.createElement("span"), state = document.createElement("span");
          var role = document.createElement("span"), counts = document.createElement("span");
          var relayError = document.createElement("span");
          url.className = "mono"; details.className = "nostr-upstream-details";
          role.className = "nostr-role nostr-upstream-role";
          relayError.className = "nostr-error";
          url.textContent = typeof relay.url === "string" ? relay.url : "—";
          role.hidden = relay.role !== "bridge" && relay.role !== "upstream";
          role.textContent = role.hidden ? "" : t("nostr.upstreamRole." + relay.role);
          var stateKey = ["idle", "connecting", "connected", "backoff"].includes(relay.state) ? relay.state : "unknown";
          state.textContent = t("nostr.upstreamState." + stateKey);
          counts.textContent = t("nostr.upstreamCounts", { received: count(relay.received), published: count(relay.published) });
          relayError.textContent = typeof relay.error === "string" && relay.error ? t("nostr.upstreamError", { error: relay.error }) : "";
          details.append(role, state, counts, relayError); row.append(url, details); upstreamRows.appendChild(row);
        });
      }
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
