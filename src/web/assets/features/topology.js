/* Presentation boundary: receives display models and callbacks, never daemon/app state. */
(function (root) {
  "use strict";

  function create(options) {
    var doc = options.document;
    var win = options.window;
    var t = options.t;
    var models = new Map();
    var triggers = new Map();
    var selected = null;
    var selectedModel = null;
    var opener = null;
    var restoreKey = null;
    var hideTimer = null;
    var hovered = null;
    var lastDetailSignature = null;
    var graphWidth = 0;
    var fitGraph = false;

    function element(tag, className, text) {
      var node = doc.createElement(tag);
      if (className) node.className = className;
      if (text != null) node.textContent = text;
      return node;
    }

    var fitButton;
    var actualButton;
    function applyGraphScale() {
      if (!options.graph || !graphWidth) return;
      var available = options.graph.parentElement.clientWidth - 26;
      var width = fitGraph && available > 0 ? Math.min(graphWidth, available) : graphWidth;
      options.graph.style.width = width + "px";
      options.graph.style.minWidth = width + "px";
      options.graph.style.maxWidth = "none";
      if (fitButton) {
        fitButton.textContent = t("topology.view.fit");
        actualButton.textContent = t("topology.view.readable");
        fitButton.setAttribute("aria-pressed", String(fitGraph));
        actualButton.setAttribute("aria-pressed", String(!fitGraph));
        options.controls.setAttribute("aria-label", t("topology.view.label"));
      }
    }
    if (options.controls && options.graph) {
      options.controls.setAttribute("role", "group");
      fitButton = element("button", "btn");
      actualButton = element("button", "btn");
      fitButton.type = actualButton.type = "button";
      fitButton.addEventListener("click", function () { fitGraph = true; hidePreview(); applyGraphScale(); });
      actualButton.addEventListener("click", function () { fitGraph = false; hidePreview(); applyGraphScale(); });
      options.controls.appendChild(fitButton);
      options.controls.appendChild(actualButton);
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
      row("topology.details.role", model.role);
      row("topology.details.state", model.state);
      row("topology.details.connection", model.connection);
      row("topology.details.media", model.media);
      row("topology.details.viewers", model.viewers);
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
    win.addEventListener("resize", function () { hidePreview(); applyGraphScale(); });

    return {
      setGraphWidth: function (width) { graphWidth = width; applyGraphScale(); },
      setSnapshot: function (nodes) {
        models = new Map(nodes.map(function (node) { return [node.key, node]; }));
        if (dialog.open) renderDetail();
      },
      beginRender: function () {
        restoreKey = doc.activeElement && doc.activeElement.getAttribute("data-topology-key");
        hidePreview();
        triggers.clear();
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
        node.addEventListener("click", function () { open(key, node); });
        node.addEventListener("keydown", function (event) {
          if (event.key === "Enter" || event.key === " ") { event.preventDefault(); open(key, node); }
        });
        node.addEventListener("mouseenter", function () { showPreview(node, key); });
        node.addEventListener("mouseleave", scheduleHide);
        node.addEventListener("focus", function () { showPreview(node, key); });
        node.addEventListener("blur", scheduleHide);
      },
      endRender: function () {
        markSelected();
        if (!dialog.open && restoreKey) {
          var nodes = triggers.get(restoreKey);
          if (nodes && nodes[0]) nodes[0].focus({ preventScroll: true });
          else if (options.focusFallback) options.focusFallback();
        }
        restoreKey = null;
      }
    };
  }

  root.MistlTopology = Object.freeze({ create: create });
})(window);
