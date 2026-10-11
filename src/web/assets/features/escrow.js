/* Escrow presentation: the dashboard shell owns transport and configuration. */
(function (global) {
  "use strict";
  function validRoom(room) {
    return typeof room === "string" && room.length >= 1 && room.length <= 96 && !/[^A-Za-z0-9_./:-]/.test(room);
  }
  function create(options) {
    var root = options.root, t = options.t, formatBytes = options.formatBytes;
    var form = root.querySelector("[data-escrow-add-form]");
    var input = root.querySelector("[data-escrow-room]");
    var add = root.querySelector("[data-escrow-add]");
    var error = root.querySelector("[data-escrow-error]");
    var summary = root.querySelector("[data-escrow-summary]");
    var body = root.querySelector("tbody"), rows = new Map(), last = null;
    var restart = root.querySelector("[data-escrow-restart]");
    var busy = false, validationError = false;
    function setError(message) {
      error.textContent = message || ""; error.hidden = !message;
      input.setAttribute("aria-invalid", validationError ? "true" : "false");
    }
    input.addEventListener("input", function () { validationError = false; setError(""); });
    form.addEventListener("submit", function (event) {
      event.preventDefault();
      if (busy) return;
      var room = input.value.trim();
      validationError = !validRoom(room);
      if (validationError) { setError(t("escrow.invalidRoom")); input.focus(); return; }
      busy = true; add.disabled = true;
      Promise.resolve(options.addRoom(room)).then(function () {
        // Preserve any new draft typed while the request was in flight.
        if (input.value.trim() === room) input.value = "";
        setError("");
      }, function (err) { setError(err.message || String(err)); }).finally(function () {
        busy = false; add.disabled = false;
      });
    });
    root.querySelector("[data-escrow-refresh]").addEventListener("click", function (event) {
      options.refresh(event.currentTarget);
    });
    restart.addEventListener("click", function () { options.restart(restart); });
    function count(label, value) {
      var dt = document.createElement("dt"), dd = document.createElement("dd");
      dt.textContent = label; dd.textContent = value;
      summary.appendChild(dt); summary.appendChild(dd);
    }
    function render(data) {
      last = data;
      if (!data) return;
      var rooms = Array.isArray(data.rooms) ? data.rooms : [];
      summary.replaceChildren();
      count(t("escrow.enabled"), t(data.enabled ? "update.on" : "update.off"));
      count(t("escrow.running"), t(data.running ? "update.yes" : "update.no"));
      count(t("escrow.configuredRooms"), rooms.filter(function (r) { return r.configured; }).length);
      count(t("escrow.joinedRooms"), rooms.filter(function (r) { return r.joined; }).length);
      count(t("escrow.totalSlots"), rooms.reduce(function (n, r) { return n + (Number(r.slots) || 0); }, 0));
      count(t("escrow.totalBytes"), formatBytes(data.bytes));
      var kept = new Set();
      rooms.forEach(function (room, index) {
        var key = String(room.room), row = rows.get(key);
        if (!row) {
          row = document.createElement("tr");
          for (var i = 0; i < 6; i++) row.appendChild(document.createElement("td"));
          row.cells[0].className = "mono escrow-room-name";
          var remove = document.createElement("button"); remove.type = "button"; remove.className = "danger";
          remove.addEventListener("click", function () {
            if (remove.disabled) return;
            remove.disabled = true;
            Promise.resolve(options.removeRoom(key)).then(function () {
              input.focus(); setError("");
            }, function (err) { setError(err.message || String(err)); }).finally(function () { remove.disabled = false; });
          });
          row.cells[5].appendChild(remove); rows.set(key, row);
        }
        kept.add(key);
        row.cells[0].textContent = key; row.cells[0].title = key;
        row.cells[1].textContent = t(room.configured ? "update.yes" : "update.no");
        row.cells[2].textContent = t(room.joined ? "update.yes" : "update.no");
        row.cells[3].textContent = Number(room.slots) || 0;
        row.cells[4].textContent = formatBytes(room.bytes);
        var remove = row.cells[5].firstChild;
        remove.textContent = t("escrow.removeRoom");
        remove.setAttribute("aria-label", t("settings.list.removeAria", { item: key }));
        remove.hidden = !room.configured;
        if (body.children[index] !== row) body.insertBefore(row, body.children[index] || null);
      });
      rows.forEach(function (row, key) { if (!kept.has(key)) { row.remove(); rows.delete(key); } });
      root.querySelector("[data-escrow-empty]").hidden = rooms.length !== 0;
      root.querySelector("[data-escrow-restart-notice]").hidden = data.applies !== "daemon restart";
    }
    return { render: render, refreshLanguage: function () {
      if (validationError) setError(t("escrow.invalidRoom"));
      render(last);
    } };
  }
  global.MistlEscrow = { create: create, validRoom: validRoom };
})(window);
