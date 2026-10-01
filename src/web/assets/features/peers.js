/* Self-claimed peer metadata is for display only, never authorization. */
(function (root) {
  "use strict";
  var peers = [];
  var call = null;
  var timer = null;
  var pending = null;
  var subscribers = new Set();
  var strings = {
    en: { avatar: "Avatar" },
    ja: { avatar: "アバター" },
    zh: { avatar: "头像" }
  };

  function shortId(id) {
    id = String(id || "");
    return id.indexOf("did:key:") === 0 ? "did:key…" + id.slice(-6) : id.slice(0, 8);
  }
  function byNode(id) { return peers.find(function (peer) { return peer.node_id === id; }); }
  function byDid(id) { return peers.find(function (peer) { return peer.did === id; }); }
  function resolve(id) { return typeof id === "object" && id ? id : byNode(id) || byDid(id); }
  function label(id) {
    var peer = resolve(id);
    return peer && peer.name || shortId(peer ? peer.node_id || peer.did : id);
  }
  function validAvatar(value) {
    return typeof value === "string" && value.length <= 16384 &&
      /^data:image\/(webp|png|jpeg);base64,(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(value) &&
      value.slice(value.indexOf(",") + 1).length > 0;
  }
  function initials(text) {
    var words = String(text || "?").trim().split(/\s+/);
    return (Array.from(words[0] || "?")[0] + (words.length > 1 ? Array.from(words[words.length - 1])[0] : "")).toUpperCase();
  }
  function color(id) {
    var hash = 0;
    Array.from(id).forEach(function (ch) { hash = (hash * 31 + ch.codePointAt(0)) >>> 0; });
    return "hsl(" + (hash % 360) + ", 48%, 38%)";
  }
  function avatarEl(peerOrId, size) {
    var peer = resolve(peerOrId);
    var id = peer ? peer.did || peer.node_id : String(peerOrId || "");
    var name = label(peer || peerOrId);
    size = Number(size);
    if (!Number.isFinite(size) || size <= 0) size = 32;
    size = Math.max(12, Math.min(256, size));
    function fallback() {
      var circle = root.document.createElement("span");
      circle.className = "mp-avatar mp-avatar-initials";
      circle.textContent = initials(name);
      circle.style.setProperty("--mp-size", size + "px");
      circle.style.backgroundColor = color(id);
      circle.setAttribute("role", "img");
      circle.setAttribute("aria-label", name);
      circle.title = name;
      return circle;
    }
    if (!peer || !validAvatar(peer.avatar)) return fallback();
    var img = root.document.createElement("img");
    img.className = "mp-avatar";
    img.style.setProperty("--mp-size", size + "px");
    var lang = (root.document.documentElement.lang || "en").slice(0, 2);
    img.alt = (strings[lang] || strings.en).avatar + ": " + name;
    img.title = name;
    img.width = img.height = size;
    img.addEventListener("error", function () { if (img.parentNode) img.replaceWith(fallback()); }, { once: true });
    img.src = peer.avatar;
    return img;
  }
  function notify() {
    subscribers.forEach(function (fn) { try { fn(peers.slice()); } catch (_) {} });
  }
  function refresh() {
    if (!call) return Promise.resolve();
    if (pending) return pending;
    pending = Promise.resolve().then(function () { return call("peers.ls", {}); }).then(function (result) {
      if (!result || !Array.isArray(result.peers)) return;
      var next = result.peers.filter(function (p) { return p && typeof p.did === "string" && typeof p.node_id === "string"; }).map(function (p) {
        var copy = Object.assign({}, p);
        copy.rooms = Object.freeze(Array.isArray(p.rooms) ? p.rooms.slice() : []);
        if (!validAvatar(copy.avatar)) delete copy.avatar;
        return Object.freeze(copy);
      });
      if (JSON.stringify(next) !== JSON.stringify(peers)) { peers = next; notify(); }
    }).catch(function () {}).finally(function () { pending = null; });
    return pending;
  }
  function visibleRefresh() { if (root.document.visibilityState !== "hidden") return refresh(); return Promise.resolve(); }
  function init(options) {
    call = options.call;
    if (timer !== null) root.clearInterval(timer);
    root.document.removeEventListener("visibilitychange", visibleRefresh);
    root.document.addEventListener("visibilitychange", visibleRefresh);
    timer = root.setInterval(visibleRefresh, 10000);
    return visibleRefresh();
  }
  function subscribe(fn) {
    subscribers.add(fn);
    return function () { subscribers.delete(fn); };
  }
  root.MistlPeers = Object.freeze({
    init: init, refresh: refresh, list: function () { return peers.slice(); },
    byNode: byNode, byDid: byDid, label: label, avatarEl: avatarEl, subscribe: subscribe
  });
})(window);
