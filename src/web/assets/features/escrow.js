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
    var exportButton = root.querySelector("[data-escrow-export]");
    var importButton = root.querySelector("[data-escrow-import]");
    var archiveInput = root.querySelector("[data-escrow-file]");
    var result = root.querySelector("[data-escrow-result]"), importResult = null;
    var archiveBusy = false, importRoom = null;
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
    function setArchiveBusy(value) {
      archiveBusy = value;
      exportButton.disabled = value; importButton.disabled = value;
      render(last);
    }
    function renderImportResult() {
      result.hidden = !importResult;
      if (!importResult) return;
      var skipped = importResult.skipped || [], reasons = new Map();
      skipped.forEach(function (item) { reasons.set(item.reason, (reasons.get(item.reason) || 0) + 1); });
      var text = t("escrow.importResult", {
        imported: importResult.imported, unchanged: importResult.unchanged, skipped: skipped.length
      });
      var details = [];
      reasons.forEach(function (count, reason) { details.push(reason + ": " + count); });
      if (details.length) text += " (" + details.join(", ") + ")";
      if (importResult.room && !(importResult.rooms || []).length) {
        text += ". " + t("escrow.importRoomEmpty", { room: importResult.room });
      }
      if ((importResult.rooms || []).some(function (room) { return room.imported > 0 && !room.configured; })) {
        text += ". " + t("escrow.importConfigure");
      }
      result.textContent = text;
    }
    // room is null for every room in the daemon (export) or in the file (import).
    function exportArchive(room) {
      if (archiveBusy) return;
      validationError = false; setError(""); setArchiveBusy(true);
      Promise.resolve().then(function () { return options.exportArchive(room); }).catch(function (err) {
        setError(err.message || String(err));
      }).finally(function () { setArchiveBusy(false); });
    }
    function chooseImport(room) {
      if (archiveBusy) return;
      importRoom = room; archiveInput.click();
    }
    exportButton.addEventListener("click", function () { exportArchive(null); });
    importButton.addEventListener("click", function () { chooseImport(null); });
    archiveInput.addEventListener("change", function () {
      var file = archiveInput.files[0], room = importRoom;
      archiveInput.value = "";
      var question = room ? t("escrow.importRoomConfirm", { room: room }) : t("escrow.importConfirm");
      if (!file || archiveBusy || !global.confirm(question)) return;
      validationError = false; setError(""); setArchiveBusy(true);
      importResult = null; renderImportResult();
      Promise.resolve().then(function () { return options.importArchive(file, room); }).then(function (data) {
        importResult = Object.assign({ room: room }, data); renderImportResult();
        return options.refresh();
      }).catch(function (err) { setError(err.message || String(err)); }).finally(function () {
        setArchiveBusy(false);
      });
    });
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
          for (var i = 0; i < 7; i++) row.appendChild(document.createElement("td"));
          row.cells[0].className = "mono escrow-room-name";
          var remove = document.createElement("button"); remove.type = "button"; remove.className = "danger";
          remove.addEventListener("click", function () {
            if (remove.disabled) return;
            remove.disabled = true;
            Promise.resolve(options.removeRoom(key)).then(function () {
              input.focus(); setError("");
            }, function (err) { setError(err.message || String(err)); }).finally(function () { remove.disabled = false; });
          });
          var exportRoom = document.createElement("button"), importInto = document.createElement("button");
          exportRoom.type = importInto.type = "button";
          exportRoom.addEventListener("click", function () { exportArchive(key); });
          importInto.addEventListener("click", function () { chooseImport(key); });
          row.archive = [exportRoom, importInto];
          row.cells[5].className = "escrow-copies";
          row.cells[6].className = "escrow-actions";
          row.cells[6].append(exportRoom, importInto, remove); rows.set(key, row);
        }
        kept.add(key);
        row.cells[0].textContent = key; row.cells[0].title = key;
        row.cells[1].textContent = t(room.configured ? "update.yes" : "update.no");
        row.cells[2].textContent = t(room.joined ? "update.yes" : "update.no");
        row.cells[3].textContent = Number(room.slots) || 0;
        row.cells[4].textContent = formatBytes(room.bytes);
        var copies = row.cells[5], auto = room.auto;
        copies.replaceChildren(); copies.removeAttribute("title"); copies.removeAttribute("aria-label"); copies.removeAttribute("tabindex");
        if (auto && typeof auto === "object") {
          var providers = Array.isArray(auto.providers) ? auto.providers : [];
          var online = providers.filter(function (p) { return p.online; }).length;
          var state = Number(auto.conflicts) > 0 ? "conflict" : Number(auto.degraded) > 0 ? "degraded" :
            Number(auto.replicating) > 0 ? "replicating" : "healthy";
          var count = document.createElement("span"), badge = document.createElement("span"), providerCount = document.createElement("span");
          count.textContent = (Number(auto.healthy) || 0) + "/" + (Number(auto.slots) || 0);
          badge.className = "escrow-copy-state"; badge.dataset.state = state; badge.textContent = t("escrow.state." + state);
          providerCount.className = "escrow-providers"; providerCount.textContent = t("escrow.providersOnline", { online: online, total: providers.length });
          copies.append(count, " ", badge, " ", providerCount);
          var details = providers.map(function (p) {
            return String(p.did || p.node || "-") + (p.node && p.did ? " (" + p.node + ")" : "") +
              ": " + t(p.online ? "escrow.providerOnline" : "escrow.providerOffline") + (p.self ? " (" + t("escrow.providerSelf") + ")" : "");
          });
          if (auto.insufficient_capacity) details.push(t("escrow.insufficientCapacity"));
          copies.title = details.join("\n");
          copies.setAttribute("aria-label", copies.textContent + (details.length ? ". " + details.join("; ") : ""));
          copies.tabIndex = 0;
        }
        row.archive[0].textContent = t("escrow.export");
        row.archive[0].setAttribute("aria-label", t("escrow.exportRoomAria", { room: key }));
        row.archive[0].disabled = archiveBusy || !(Number(room.slots) > 0);
        row.archive[1].textContent = t("escrow.import");
        row.archive[1].setAttribute("aria-label", t("escrow.importRoomAria", { room: key }));
        row.archive[1].disabled = archiveBusy;
        var remove = row.cells[6].lastChild;
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
      renderImportResult();
      render(last);
    } };
  }
  global.MistlEscrow = { create: create, validRoom: validRoom };
})(window);
