/* Presentation boundary: receives display models and callbacks, never daemon/app state. */
(function (root) {
  "use strict";

  function create(options) {
    var doc = options.document;
    var win = options.window;
    var t = options.t;
    var models = new Map();
    var sourceModels = [];
    var triggers = new Map();
    var selected = null;
    var selectedModel = null;
    var opener = null;
    var hideTimer = null;
    var hovered = null;
    var lastDetailSignature = null;
    var layout = null;

    function element(tag, className, text) {
      var node = doc.createElement(tag);
      if (className) node.className = className;
      if (text != null) node.textContent = text;
      return node;
    }

    function peerFor(model) {
      return root.MistlPeers && (root.MistlPeers.byDid(model.did) || root.MistlPeers.byNode(model.id));
    }

    function refreshModels() {
      models = new Map(sourceModels.map(function (node) {
        var peer = peerFor(node);
        var model = Object.assign({}, node);
        if (model.name || peer) {
          model.label = model.name || root.MistlPeers.label(peer);
          model.did = model.did || peer && peer.did;
          model.avatar = peer && peer.avatar;
        }
        return [model.key, model];
      }));
      updateLabels();
      if (maximize) maximize.refresh();
      if (dialog.open) renderDetail();
      if (hovered && !dialog.open) showPreview(hovered, hovered.getAttribute("data-topology-key"));
    }

    function compactLabel(text, maxUnits) {
      var chars = Array.from(String(text));
      var units = chars.reduce(function (n, ch) { return n + (ch.codePointAt(0) > 255 ? 2 : 1); }, 0);
      if (units <= maxUnits) return text;
      var result = "", used = 0;
      chars.some(function (ch) {
        var weight = ch.codePointAt(0) > 255 ? 2 : 1;
        if (used + weight > maxUnits - 1) return true;
        result += ch; used += weight; return false;
      });
      return result + "…";
    }

    function updateLabels() {
      triggers.forEach(function (nodes, key) {
        var model = models.get(key);
        if (!model) return;
        nodes.forEach(function (node) {
          node.setAttribute("aria-label", [model.label, model.state, t("topology.details.open")].filter(Boolean).join(" · "));
          if (!node.querySelector) return;
          var label = node.querySelector(".topology-node-label");
          if (label) {
            label.textContent = compactLabel(model.label, model.kind === "feature" || model.kind === "room" ? 18 : 22);
          }
          var title = node.querySelector("title");
          if (title) title.textContent = [model.label, model.id, model.role, model.state].filter(Boolean).join(" · ");
        });
      });
    }

    var preview = element("div", "topology-preview");
    preview.id = "topology-node-preview";
    preview.setAttribute("role", "tooltip");
    preview.hidden = true;
    doc.body.appendChild(preview);

    var dialog = element("dialog", "topology-detail");
    dialog.setAttribute("aria-labelledby", "topology-detail-title");
    var header = element("div", "topology-detail-header");
    var heading = element("h2");
    heading.id = "topology-detail-title";
    var close = element("button", "btn topology-detail-close");
    close.type = "button";
    close.autofocus = true;
    header.appendChild(heading);
    header.appendChild(close);
    var content = element("div", "topology-detail-body");
    var footer = element("div", "topology-detail-footer");
    var copy = element("button", "btn primary");
    copy.type = "button";
    footer.appendChild(copy);
    dialog.appendChild(header);
    dialog.appendChild(content);
    dialog.appendChild(footer);
    doc.body.appendChild(dialog);

    function hidePreview() {
      win.clearTimeout(hideTimer);
      preview.hidden = true;
      if (hovered) hovered.removeAttribute("aria-describedby");
      hovered = null;
    }

    function scheduleHide() {
      win.clearTimeout(hideTimer);
      hideTimer = win.setTimeout(hidePreview, 160);
    }

    function showPreview(trigger, key) {
      var model = models.get(key);
      if (!model || dialog.open) return;
      hidePreview();
      hovered = trigger;
      preview.replaceChildren();
      var peer = peerFor(model);
      if (peer) preview.appendChild(root.MistlPeers.avatarEl(peer, 32));
      preview.appendChild(element("strong", "topology-preview-name", model.label));
      preview.appendChild(element("span", "topology-preview-meta", [model.role, model.state].filter(Boolean).join(" · ")));
      if (model.id) preview.appendChild(element("span", "topology-preview-id", model.id));
      preview.appendChild(element("span", "topology-preview-hint", t("topology.details.open")));
      preview.hidden = false;
      trigger.setAttribute("aria-describedby", preview.id);
      var rect = trigger.getBoundingClientRect();
      var bounds = preview.getBoundingClientRect();
      var maxLeft = Math.max(8, win.innerWidth - bounds.width - 8);
      preview.style.left = Math.max(8, Math.min(rect.left + rect.width / 2 - bounds.width / 2, maxLeft)) + "px";
      var top = rect.top - bounds.height - 12;
      if (top < 8) top = rect.bottom + 12;
      preview.style.top = Math.max(8, Math.min(top, win.innerHeight - bounds.height - 8)) + "px";
    }

    function renderDetail() {
      if (!selectedModel) return;
      var model = models.get(selected);
      var unavailable = !model;
      if (model) selectedModel = model;
      model = selectedModel;
      // Preserve text selection and scroll position when polling changes nothing.
      var signature = JSON.stringify([model, unavailable, t("topology.details.title")]);
      if (signature === lastDetailSignature) return;
      lastDetailSignature = signature;
      heading.textContent = t("topology.details.title");
      close.textContent = t("topology.details.close");
      copy.textContent = t("topology.details.copy");
      copy.hidden = !model.id;
      content.replaceChildren();
      var peer = peerFor(model);
      if (peer) content.appendChild(root.MistlPeers.avatarEl(peer, 48));
      content.appendChild(element("h3", "topology-detail-name", model.label));
      if (unavailable) {
        var notice = element("p", "topology-detail-notice", t("topology.details.unavailable"));
        notice.setAttribute("role", "status");
        content.appendChild(notice);
      }
      var list = element("dl", "topology-detail-list");
      function row(label, value, mono) {
        if (value == null || value === "") return;
        list.appendChild(element("dt", null, t(label)));
        list.appendChild(element("dd", mono ? "mono" : null, value));
      }
      row("topology.details.id", model.id, true);
      if (model.did) {
        list.appendChild(element("dt", null, "DID"));
        list.appendChild(element("dd", "mono", model.did));
      }
      row("topology.details.role", model.role);
      row("topology.details.state", model.state);
      row("topology.details.connection", model.connection);
      row("topology.details.media", model.media);
      row("topology.details.viewers", model.viewers);
      if (model.features) row("topology.details.features", model.features.length ? model.features.join(" · ") : t("topology.details.noFeatures"));
      row("topology.details.peers", model.peers);
      content.appendChild(list);
      if (model.rooms) {
        content.appendChild(element("h4", "topology-detail-section", t("topology.rooms")));
        if (model.rooms.length) {
          var rooms = element("ul", "topology-detail-rooms");
          model.rooms.forEach(function (room) { rooms.appendChild(element("li", "mono", room)); });
          content.appendChild(rooms);
        } else {
          content.appendChild(element("p", "muted", t("topology.details.noRooms")));
        }
      }
    }

    function markSelected() {
      triggers.forEach(function (nodes, key) {
        nodes.forEach(function (node) { node.classList.toggle("is-selected", dialog.open && selected === key); });
      });
      if (layout) layout.highlight(dialog.open ? selected : hovered && hovered.getAttribute("data-topology-key") || doc.activeElement && doc.activeElement.getAttribute("data-topology-key"));
    }

    function open(key, trigger) {
      if (!models.has(key)) return;
      selected = key;
      selectedModel = models.get(key);
      opener = trigger;
      lastDetailSignature = null;
      hidePreview();
      renderDetail();
      if (!dialog.open) dialog.showModal();
      markSelected();
    }

    close.addEventListener("click", function () { dialog.close(); });
    dialog.addEventListener("close", function () {
      var current = triggers.get(selected);
      var target = opener && opener.isConnected ? opener : current && current[0];
      selected = null;
      selectedModel = null;
      lastDetailSignature = null;
      markSelected();
      if (target) target.focus({ preventScroll: true });
      else if (options.focusFallback) options.focusFallback();
    });
    copy.addEventListener("click", function () {
      if (selectedModel && selectedModel.id) options.copy(selectedModel.id);
    });
    preview.addEventListener("mouseenter", function () { win.clearTimeout(hideTimer); });
    preview.addEventListener("mouseleave", scheduleHide);
    doc.addEventListener("keydown", function (event) {
      if (event.key === "Escape") hidePreview();
    });
    // A fixed preview must not remain detached from a scrolled/reflowed graph.
    win.addEventListener("scroll", hidePreview, true);
    win.addEventListener("resize", hidePreview);

    if (root.MistlPeers) root.MistlPeers.subscribe(refreshModels);

    var presentation = {
      setSnapshot: function (nodes) {
        sourceModels = nodes;
        refreshModels();
      },
      bindNode: function (node, key) {
        var model = models.get(key);
        if (!model) return;
        var nodes = triggers.get(key) || [];
        nodes.push(node);
        triggers.set(key, nodes);
        node.classList.add("topology-node-clickable");
        node.setAttribute("data-topology-key", key);
        node.setAttribute("tabindex", "0");
        node.setAttribute("role", "button");
        node.setAttribute("aria-haspopup", "dialog");
        node.setAttribute("aria-label", [model.label, model.state, t("topology.details.open")].filter(Boolean).join(" · "));
        node.addEventListener("click", function () { if (!layout || !layout.suppressClick()) open(key, node); });
        node.addEventListener("keydown", function (event) {
          if (event.key === "Enter" || event.key === " ") { event.preventDefault(); open(key, node); }
        });
        function enter() { showPreview(node, key); if (layout) layout.highlight(key); }
        function leave() { scheduleHide(); if (layout) layout.highlight(dialog.open ? selected : null); }
        node.addEventListener("mouseenter", enter);
        node.addEventListener("mouseleave", leave);
        node.addEventListener("focus", enter);
        node.addEventListener("blur", leave);
      },
      unbindNode: function (node, key) {
        triggers.delete(key);
        if (hovered === node) hidePreview();
        if (doc.activeElement === node && options.focusFallback) options.focusFallback();
      },
      setGraph: function (graph) {
        presentation.setSnapshot(graph.nodes);
        layout.update(graph);
        updateLabels();
        markSelected();
      }
    };
    var maximize = null;
    if (options.graph) layout = createGraph(options, presentation, hidePreview);
    if (options.maximize) maximize = createMaximize({
      document: doc, card: options.graph.closest("#topology-card"), graph: options.graph,
      toggle: options.maximize, layout: layout, fitMax: options.fitMax,
      label: function (expanded) { return t(expanded ? "topology.restore" : "topology.maximize"); }
    });
    presentation.activity = function (path, count, bytes) { layout.activity(path, count, bytes); };
    presentation.isVisible = function () { return layout.isVisible(); };
    return presentation;
  }

  function createGraph(options, inspection, hidePreview) {
    var doc = options.document, win = options.window || doc.defaultView, svg = options.graph;
    var card = options.card || svg.closest("#topology-card") || svg;
    var nodes = new Map(), edges = new Map(), liveNodes = [], liveEdges = [];
    var particles = [], raf = null, alpha = 0, lastFrame = 0, particleBudget = 48;
    var width = 640, height = 380, scale = 1, panX = 0, panY = 0;
    var userView = false, fitting = true, intersecting = true, visible = true;
    var highlightKey = null, drag = null, pointers = new Map(), pinch = null, suppressUntil = 0;
    var motion = win.matchMedia("(prefers-reduced-motion: reduce)");
    var reduced = motion.matches;
    var SVG_NS = "http://www.w3.org/2000/svg";
    function el(tag, attrs, parent) {
      var node = doc.createElementNS(SVG_NS, tag);
      Object.keys(attrs || {}).forEach(function (key) { node.setAttribute(key, attrs[key]); });
      if (parent) parent.appendChild(node);
      return node;
    }
    var viewport = el("g", { class: "topology-viewport" }, svg);
    var edgeLayer = el("g", options.onEdgeActivate ? {} : { "aria-hidden": "true" }, viewport);
    var particleLayer = el("g", { "aria-hidden": "true", class: "topology-particles" }, viewport);
    var labelLayer = options.makeEdgeLabel ? el("g", { "pointer-events": "none", "aria-hidden": "true" }, viewport) : null;
    var nodeLayer = el("g", {}, viewport);
    function now() { return win.performance.now(); }
    function clamp(n, min, max) { return Math.max(min, Math.min(max, n)); }
    function camera() {
      viewport.setAttribute("transform", "translate(" + (width / 2 + panX) + "," + (height / 2 + panY) + ") scale(" + scale + ")");
      svg.classList.toggle("is-zoomed-out", scale < 0.6);
    }
    function fit() {
      if (!liveNodes.length) return;
      var minX = Infinity, maxX = -Infinity, minY = Infinity, maxY = -Infinity;
      liveNodes.forEach(function (n) {
        minX = Math.min(minX, n.x - 40); maxX = Math.max(maxX, n.x + 40);
        minY = Math.min(minY, n.y - 28); maxY = Math.max(maxY, n.y + 38);
      });
      scale = clamp(Math.min((width - 48) / (maxX - minX), (height - 72) / (maxY - minY)), 0.15, options.fitMax || 1.25);
      panX = -(minX + maxX) / 2 * scale; panY = -(minY + maxY) / 2 * scale;
      camera();
    }
    function resize() {
      var rect = svg.getBoundingClientRect();
      width = rect.width || 640; height = rect.height || 380;
      svg.setAttribute("viewBox", "0 0 " + width + " " + height);
      if (!userView) fit(); else camera();
      syncVisibility();
    }
    function clearParticles() {
      particles.forEach(function (p) { p.el.remove(); }); particles = [];
    }
    function syncVisibility() {
      var next = !doc.hidden && (intersecting || !!card.closest(".graph-maximized")) && !card.closest("[hidden]") && win.getComputedStyle(card).display !== "none";
      if (visible === next) return;
      visible = next;
      card.classList.toggle("is-paused", !visible);
      if (!visible) {
        if (raf !== null) win.cancelAnimationFrame(raf);
        raf = null; lastFrame = 0; clearParticles(); hidePreview();
      } else wake();
      if (options.onVisibilityChange) options.onVisibilityChange();
    }
    function wake(heat) {
      if (heat) alpha = Math.max(alpha, heat);
      if (visible && raf === null) raf = win.requestAnimationFrame(frame);
    }
    function highlight(key) {
      highlightKey = key;
      var neighbours = new Set(key ? [key] : []);
      if (key) liveEdges.forEach(function (e) {
        if (e.source.key === key) neighbours.add(e.target.key);
        if (e.target.key === key) neighbours.add(e.source.key);
      });
      nodes.forEach(function (n) {
        n.el.classList.toggle("is-dimmed", !!key && !neighbours.has(n.key));
        n.el.classList.toggle("is-highlighted", key === n.key);
      });
      edges.forEach(function (e) {
        var adjacent = e.source.key === key || e.target.key === key;
        e.el.classList.toggle("is-dimmed", !!key && !adjacent);
        e.el.classList.toggle("is-highlighted", !!key && adjacent);
        if (e.label) {
          e.label.classList.toggle("is-dimmed", !!key && !adjacent);
          e.label.classList.toggle("is-highlighted", !!key && adjacent);
        }
      });
      particles.forEach(function (p) {
        p.el.classList.toggle("is-dimmed", !!key && !p.path.some(function (n) { return n.key === key; }));
      });
    }
    // Short spokes keep each feature's rooms visibly clustered around it.
    function restLength(a, b) {
      var kinds = [a.kind, b.kind];
      if (kinds.indexOf("feature") >= 0) return kinds.indexOf("room") >= 0 ? 55 : 65;
      return kinds.indexOf("room") >= 0 ? 85 : 130;
    }
    function physics() {
      // Positions survive snapshots. Cooling is independent of traffic animations.
      for (var i = 0; i < liveNodes.length; i++) {
        var a = liveNodes[i];
        for (var j = i + 1; j < liveNodes.length; j++) {
          var b = liveNodes[j], dx = b.x - a.x, dy = b.y - a.y;
          var distance = Math.sqrt(dx * dx + dy * dy) || 0.01;
          if (distance < 0.1) { dx = 0.1; dy = 0.1; distance = 0.14; }
          var force = Math.min(2.5, 1600 / (distance * distance)) * alpha;
          var collision = Math.max(0, a.radius + b.radius + 30 - distance) * 0.08;
          var fx = dx / distance * (force + collision), fy = dy / distance * (force + collision);
          a.vx -= fx; a.vy -= fy; b.vx += fx; b.vy += fy;
        }
      }
      liveEdges.forEach(function (e) {
        var a = e.source, b = e.target, dx = b.x - a.x, dy = b.y - a.y;
        var distance = Math.sqrt(dx * dx + dy * dy) || 1;
        var force = (distance - (options.linkDistance || restLength(a, b))) * 0.025 * alpha;
        var fx = dx / distance * force, fy = dy / distance * force;
        a.vx += fx; a.vy += fy; b.vx -= fx; b.vy -= fy;
      });
      liveNodes.forEach(function (n) {
        var center = n.kind === "self" ? 0.035 : 0.0015;
        n.vx = (n.vx - n.x * center * alpha) * 0.78;
        n.vy = (n.vy - n.y * center * alpha) * 0.78;
        if (n.pinned || drag && drag.node === n) { n.vx = n.vy = 0; return; }
        n.x += n.vx; n.y += n.vy;
      });
      alpha *= 0.974;
    }
    function addParticle(path, radius, delay, media) {
      if (reduced || !visible || particles.length >= 160 || path.length < 2 || !media && particleBudget <= 0) return;
      if (!media) particleBudget--;
      var dimmed = highlightKey && !path.some(function (n) { return n.key === highlightKey; });
      particles.push({ path: path, start: now() + delay, duration: 1050 + (path.length - 2) * 500,
        el: el("circle", { r: radius, class: "topology-particle" + (media ? " is-media" : "") + (dimmed ? " is-dimmed" : ""), opacity: 0 }, particleLayer) });
      wake();
    }
    function frame(time) {
      raf = null;
      if (!visible) return;
      var dt = Math.min(48, lastFrame ? time - lastFrame : 16.7); lastFrame = time;
      var moving = alpha > 0.002;
      if (moving) physics();
      var changing = false;
      nodes.forEach(function (n, key) {
        var target = n.removing ? 0 : 1;
        if (n.life !== target) {
          n.life = reduced ? target : n.life + Math.sign(target - n.life) * dt / 280;
          n.life = clamp(n.life, 0, 1); changing = true;
          n.el.setAttribute("opacity", n.life);
        }
        if (n.removing && n.life === 0) {
          inspection.unbindNode(n.el, key); n.el.remove(); nodes.delete(key); return;
        }
        var x = Math.round((n.x - n.cx * n.life) * 100) / 100;
        var y = Math.round((n.y - n.cy * n.life) * 100) / 100;
        var transform = "translate(" + x + "," + y + ") scale(" + n.life + ")";
        if (transform !== n.transform) { n.el.setAttribute("transform", transform); n.transform = transform; }
        if (n.rippleUntil > time && !reduced) {
          var progress = 1 - (n.rippleUntil - time) / 850;
          n.ring.setAttribute("r", n.radius + 4 + progress * 19);
          n.ring.setAttribute("opacity", (1 - progress) * 0.7); changing = true;
        } else if (n.rippleUntil) {
          n.rippleUntil = 0; n.ring.setAttribute("opacity", 0);
        }
      });
      var flowing = false;
      edges.forEach(function (e, key) {
        if (e.removing && (!nodes.has(e.source.key) || !nodes.has(e.target.key) || Math.min(e.source.life, e.target.life) === 0)) {
          e.el.remove(); if (e.label) e.label.remove(); edges.delete(key); return;
        }
        var a = e.source, b = e.target;
        var geometry = [a.x.toFixed(2), a.y.toFixed(2), b.x.toFixed(2), b.y.toFixed(2)].join(" ");
        if (geometry !== e.geometry) {
          e.el.setAttribute("x1", a.x); e.el.setAttribute("y1", a.y);
          e.el.setAttribute("x2", b.x); e.el.setAttribute("y2", b.y); e.geometry = geometry;
          positionEdgeLabel(e);
        }
        var opacity = Math.min(a.life, b.life);
        if (e.opacity !== opacity) {
          e.el.setAttribute("opacity", opacity); if (e.label) e.label.setAttribute("opacity", opacity); e.opacity = opacity;
        }
        if (!e.removing && e.flowUntil > time && !reduced) {
          flowing = true;
          if (time >= e.nextParticle) { addParticle([a, b], 1.8, 0, true); e.nextParticle = time + 240; }
        } else if (e.flowUntil && e.flowUntil <= time) {
          e.flowUntil = 0; e.el.classList.remove("is-flowing");
        }
      });
      particles = particles.filter(function (p) {
        var progress = (time - p.start) / p.duration;
        if (progress >= 1 || p.path.some(function (n) { return n.removing; })) { p.el.remove(); return false; }
        if (progress < 0) return true;
        var segment = Math.min(p.path.length - 2, Math.floor(progress * (p.path.length - 1)));
        var fraction = progress * (p.path.length - 1) - segment;
        var a = p.path[segment], b = p.path[segment + 1];
        p.el.setAttribute("cx", a.x + (b.x - a.x) * fraction);
        p.el.setAttribute("cy", a.y + (b.y - a.y) * fraction);
        p.el.setAttribute("opacity", Math.min(1, progress * 8, (1 - progress) * 8));
        return true;
      });
      if (fitting && !userView) { fit(); if (!moving) fitting = false; }
      // No timer keeps a settled, inactive graph alive.
      if (moving || changing || flowing || particles.length) wake(); else lastFrame = 0;
    }
    function positionEdgeLabel(e) {
      if (e.label) e.label.setAttribute("transform", "translate(" + ((e.source.x + e.target.x) / 2) + "," + ((e.source.y + e.target.y) / 2) + ")");
    }
    function update(graph) {
      particleBudget = 48;
      var nextNodes = new Set(graph.nodes.map(function (n) { return n.key; }));
      var changed = liveNodes.length !== nextNodes.size;
      var rippling = false;
      var degree = new Map();
      graph.edges.forEach(function (e) {
        degree.set(e.source, (degree.get(e.source) || 0) + 1);
        degree.set(e.target, (degree.get(e.target) || 0) + 1);
      });
      nodes.forEach(function (n, key) { n.el.classList.remove("has-activity"); if (!nextNodes.has(key) && !n.removing) { n.removing = true; changed = true; } });
      graph.nodes.forEach(function (model, index) {
        var n = nodes.get(model.key);
        var radius = model.kind === "self" ? 10 : model.kind === "feature" ? 5.5 : model.kind === "room" ? 4.5 : 5.5 + Math.min(3, Math.sqrt(degree.get(model.key) || 0));
        if (!n) {
          var link = graph.edges.find(function (e) { return e.source === model.key && nodes.has(e.target) || e.target === model.key && nodes.has(e.source); });
          var neighbour = link && nodes.get(link.source === model.key ? link.target : link.source);
          var angle = index * 2.39996, spread = 42 + Math.sqrt(index) * 14;
          var shape = options.makeNode(nodeLayer, model, radius);
          n = { key: model.key, x: (neighbour ? neighbour.x : 0) + Math.cos(angle) * spread,
            y: (neighbour ? neighbour.y : 0) + Math.sin(angle) * spread, vx: 0, vy: 0,
            el: shape.el, cx: shape.cx, cy: shape.cy, life: reduced ? 1 : 0 };
          n.dot = n.el.querySelector(".topology-node-dot");
          n.label = n.el.querySelector(".topology-node-label");
          n.ring = el("circle", { class: "topology-ripple", cx: n.cx, cy: n.cy, opacity: 0 }, n.el);
          nodes.set(model.key, n); changed = true;
        }
        if (n.removing) { n.removing = false; changed = true; }
        var connected = model.rawState === "connected";
        var justConnected = connected && n.rawState !== "connected";
        if (justConnected && !reduced) { n.rippleUntil = now() + 850; rippling = true; }
        n.rawState = model.rawState; n.kind = model.kind; n.radius = radius;
        var className = "topology-node topology-node-clickable kind-" + model.kind + " state-" + (model.rawState || "connected");
        if (className !== n.className) { n.el.setAttribute("class", className); n.className = className; }
        if (justConnected && reduced) n.el.classList.add("has-activity");
        n.dot.setAttribute("r", radius);
        n.label.setAttribute("y", n.cy + radius + 15);
        n.el.classList.toggle("is-alone", graph.nodes.length === 1);
      });
      var nextEdges = new Set(graph.edges.map(function (e) { return e.key; }));
      edges.forEach(function (e, key) {
        if (!nextEdges.has(key)) {
          e.removing = true;
          if (!e.source.removing && !e.target.removing) { e.el.remove(); if (e.label) e.label.remove(); edges.delete(key); }
        }
      });
      graph.edges.forEach(function (model) {
        var e = edges.get(model.key);
        if (!e) {
          e = { el: el("line", { class: "topology-edge" }, edgeLayer), nextParticle: 0 };
          if (options.onEdgeActivate) {
            e.el.setAttribute("role", "button"); e.el.setAttribute("tabindex", "0");
            e.el.addEventListener("click", function () { if (now() >= suppressUntil) options.onEdgeActivate(e.model, e.el); });
            e.el.addEventListener("keydown", function (event) {
              if (event.key === "Enter" || event.key === " ") { event.preventDefault(); options.onEdgeActivate(e.model, e.el); }
            });
          }
          edges.set(model.key, e); changed = true;
        }
        // The hook may reuse the previous label; geometry and lifetime stay here.
        if (options.makeEdgeLabel && e.model !== model) {
          var previousLabel = e.label;
          e.label = options.makeEdgeLabel(labelLayer, model, previousLabel);
          if (previousLabel && previousLabel !== e.label) previousLabel.remove();
          if (e.label) e.label.setAttribute("opacity", e.opacity == null ? 0 : e.opacity);
        }
        e.model = model;
        var edgeClass = "topology-edge" + (model.className ? " " + model.className : "");
        if (e.className !== edgeClass) { e.el.setAttribute("class", edgeClass); e.className = edgeClass; }
        if (options.onEdgeActivate) e.el.setAttribute("aria-label", model.label || model.key);
        e.source = nodes.get(model.source); e.target = nodes.get(model.target); e.removing = false;
        positionEdgeLabel(e);
        e.flowUntil = model.flowMs ? now() + model.flowMs : 0;
        e.el.classList.toggle("is-flowing", e.flowUntil > now());
      });
      liveNodes = graph.nodes.map(function (n) { return nodes.get(n.key); });
      liveEdges = graph.edges.map(function (e) { return edges.get(e.key); });
      if (changed && !userView) fitting = true;
      highlight(highlightKey);
      syncVisibility();
      if (changed) wake(0.8);
      else if (rippling || liveEdges.some(function (e) { return e.flowUntil > now(); })) wake();
    }
    function point(event) {
      var rect = svg.getBoundingClientRect();
      return { x: event.clientX - rect.left, y: event.clientY - rect.top };
    }
    function world(p) { return { x: (p.x - width / 2 - panX) / scale, y: (p.y - height / 2 - panY) / scale }; }
    function zoom(next, p) {
      var anchor = world(p); scale = clamp(next, 0.15, 4);
      panX = p.x - width / 2 - anchor.x * scale; panY = p.y - height / 2 - anchor.y * scale;
      userView = true; fitting = false; camera(); hidePreview();
    }
    svg.addEventListener("wheel", function (event) {
      event.preventDefault(); zoom(scale * Math.exp(-event.deltaY * (event.deltaMode === 1 ? 0.04 : 0.002)), point(event));
    }, { passive: false });
    svg.addEventListener("pointerdown", function (event) {
      if (event.button !== 0) return;
      var p = point(event); pointers.set(event.pointerId, p);
      svg.setPointerCapture(event.pointerId);
      if (pointers.size === 2) {
        var pair = Array.from(pointers.values());
        pinch = { distance: Math.hypot(pair[1].x - pair[0].x, pair[1].y - pair[0].y), scale: scale };
        drag = null; return;
      }
      var target = event.target.closest("[data-topology-key]");
      var n = target && nodes.get(target.getAttribute("data-topology-key"));
      drag = { node: n, start: p, last: p, moved: false };
      if (n) wake(0.5);
    });
    svg.addEventListener("pointermove", function (event) {
      if (!pointers.has(event.pointerId)) return;
      var p = point(event); pointers.set(event.pointerId, p);
      if (pinch && pointers.size === 2) {
        var pair = Array.from(pointers.values());
        var distance = Math.hypot(pair[1].x - pair[0].x, pair[1].y - pair[0].y);
        zoom(pinch.scale * distance / Math.max(1, pinch.distance), { x: (pair[0].x + pair[1].x) / 2, y: (pair[0].y + pair[1].y) / 2 });
        suppressUntil = now() + 400; return;
      }
      if (!drag) return;
      if (Math.hypot(p.x - drag.start.x, p.y - drag.start.y) > 4) drag.moved = true;
      if (!drag.moved) return;
      hidePreview();
      if (drag.node) {
        var position = world(p); drag.node.x = position.x; drag.node.y = position.y;
        if (options.pinDragged) drag.node.pinned = true;
        wake(0.4);
      } else {
        userView = true; fitting = false;
        panX += p.x - drag.last.x; panY += p.y - drag.last.y; camera();
      }
      drag.last = p;
    });
    function release(event) {
      pointers.delete(event.pointerId);
      if (drag && drag.moved) { suppressUntil = now() + 400; if (drag.node) wake(0.5); }
      drag = null; pinch = null;
    }
    svg.addEventListener("pointerup", release);
    svg.addEventListener("pointercancel", release);
    svg.addEventListener("lostpointercapture", release);
    svg.addEventListener("dblclick", function (event) {
      if (event.target.closest("[data-topology-key]")) return;
      userView = false; fit(); hidePreview();
    });
    svg.addEventListener("keydown", function (event) {
      var target = event.target.closest("[data-topology-key]");
      var n = target && nodes.get(target.getAttribute("data-topology-key"));
      var dx = event.key === "ArrowRight" ? 20 : event.key === "ArrowLeft" ? -20 : 0;
      var dy = event.key === "ArrowDown" ? 20 : event.key === "ArrowUp" ? -20 : 0;
      if (dx || dy) {
        event.preventDefault();
        if (n) { n.x += dx; n.y += dy; if (options.pinDragged) n.pinned = true; wake(0.5); }
        else { userView = true; fitting = false; panX += dx; panY += dy; camera(); }
      } else if (event.key === "+" || event.key === "=" || event.key === "-") {
        event.preventDefault(); zoom(scale * (event.key === "-" ? 0.8 : 1.25), { x: width / 2, y: height / 2 });
      } else if (event.key === "0" || event.key === "Home") { event.preventDefault(); userView = false; fit(); }
    });
    doc.addEventListener("visibilitychange", syncVisibility);
    if (win.IntersectionObserver) new win.IntersectionObserver(function (entries) {
      intersecting = entries[0].isIntersecting; syncVisibility();
    }).observe(card);
    if (win.ResizeObserver) new win.ResizeObserver(resize).observe(svg);
    else win.addEventListener("resize", resize);
    motion.addEventListener("change", function () {
      reduced = motion.matches;
      if (reduced) {
        clearParticles();
        nodes.forEach(function (n) { n.rippleUntil = 0; n.ring.setAttribute("opacity", 0); });
      }
      wake();
    });
    resize(); camera();
    return {
      update: update, highlight: highlight,
      reset: function () {
        if (raf !== null) win.cancelAnimationFrame(raf);
        raf = null; alpha = 0; lastFrame = 0; clearParticles();
        nodes.forEach(function (n, key) { inspection.unbindNode(n.el, key); n.el.remove(); });
        edges.forEach(function (e) { e.el.remove(); if (e.label) e.label.remove(); });
        nodes.clear(); edges.clear(); liveNodes = []; liveEdges = [];
        drag = null; pinch = null; pointers.clear(); highlightKey = null;
        userView = false; fitting = true; scale = 1; panX = panY = 0; camera();
      },
      fit: function () { userView = false; fit(); },
      setFitMax: function (value) {
        options.fitMax = value; userView = false; fitting = true;
        hidePreview(); resize();
      },
      worldPoint: function (event) { return world(point(event)); },
      position: function (key) { var n = nodes.get(key); return n && !n.removing ? { x: n.x, y: n.y } : null; },
      isVisible: function () { syncVisibility(); return visible; },
      suppressClick: function () { return now() < suppressUntil; },
      activity: function (keys, count, bytes) {
        var path = keys.map(function (key) { return nodes.get(key); });
        if (!visible || path.length < 2 || path.some(function (n) { return !n || n.removing; })) return;
        if (reduced) { path.forEach(function (n) { n.el.classList.add("has-activity"); }); return; }
        var amount = Math.min(12, Math.max(1, Math.ceil(Math.log2(1 + count) + Math.log2(1 + bytes / 1024) / 3)));
        var radius = Math.min(3.2, 1.3 + Math.log2(1 + bytes / Math.max(1, count) / 1024) * 0.22);
        for (var i = 0; i < amount; i++) addParticle(path, radius, i * 70, false);
      }
    };
  }

  var activeMaximize = null;
  function createMaximize(options) {
    var doc = options.document, card = options.card, toggle = options.toggle;
    var expanded = false, placeholder = null, overflow = "";
    function refresh() {
      var label = options.label(expanded);
      toggle.setAttribute("aria-pressed", String(expanded));
      toggle.setAttribute("aria-label", label); toggle.title = label;
      toggle.textContent = expanded ? "\u2921" : "\u2922";
    }
    function set(value) {
      if (expanded === value) return;
      if (value && activeMaximize) activeMaximize.restore();
      expanded = value;
      if (value) {
        // Escape transformed panels and the dock's stacking/scroll context.
        placeholder = doc.createComment("graph position");
        card.before(placeholder); doc.body.appendChild(card);
        overflow = doc.body.style.overflow; doc.body.style.overflow = "hidden";
        activeMaximize = control;
      }
      card.classList.toggle("graph-maximized", value);
      if (!value) {
        placeholder.replaceWith(card); placeholder = null;
        doc.body.style.overflow = overflow; activeMaximize = null;
      }
      refresh();
      options.layout.setFitMax(value ? 4 : (options.fitMax || 1.25));
      (value ? options.graph : toggle).focus({ preventScroll: true });
    }
    var control = { refresh: refresh, restore: function () { set(false); } };
    toggle.addEventListener("click", function () { set(!expanded); });
    doc.addEventListener("keydown", function (event) {
      if (!expanded || event.key !== "Escape" || event.defaultPrevented) return;
      // Native dialogs and feature editors handle their own Escape first.
      if (doc.querySelector('dialog[open], .modal-overlay:not([hidden])')) return;
      event.preventDefault(); event.stopImmediatePropagation(); set(false);
    }, true);
    refresh();
    return control;
  }

  root.MistlTopology = Object.freeze({ create: create, createGraph: createGraph, createMaximize: createMaximize });
})(window);
