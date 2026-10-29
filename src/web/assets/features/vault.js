/* Vault presentation; the dashboard shell owns daemon requests and polling. */
(function (global) {
  "use strict";


  function create(options) {
    var root = options.root, t = options.t, formatBytes = options.formatBytes;
    var setup = root.querySelector("[data-vault-setup]");
    var groupPanel = root.querySelector("[data-vault-group]");
    var groupId = root.querySelector("[data-vault-group-id]");
    var memberList = root.querySelector("[data-vault-members]");
    var ownerBody = root.querySelector("[data-vault-owners]");
    var fileBody = root.querySelector("[data-vault-files]");
    var fileInput = root.querySelector("[data-vault-file]");
    var inviteInput = root.querySelector("[data-vault-invite-input]");
    var error = root.querySelector("[data-vault-error]");
    var codeBox = root.querySelector("[data-vault-code-box]");
    var codeText = root.querySelector("[data-vault-code]");
    var codeLabel = root.querySelector("[data-vault-code-label]");
    var owners = new Map(), files = new Map();
    var lastStatus = null, lastFiles = [], codeKind = null;

    function setError(message) {
      error.textContent = message || "";
      error.hidden = !message;
    }
    function showCode(kind, code) {
      if (typeof code !== "string" || !code) throw new Error(t("vault.codeMissing"));
      codeKind = kind;
      codeText.textContent = code;
      codeLabel.textContent = t(kind === "recovery" ? "vault.recoveryCode" : "vault.inviteCode");
      codeBox.hidden = false;
    }
    function perform(button, cmd, args, done) {
      if (button.disabled) return;
      button.disabled = true;
      setError("");
      Promise.resolve().then(function () { return options.call(cmd, args); }).then(function (data) {
        if (done) done(data || {});
        return options.refresh();
      }).catch(function (err) { setError(err.message || String(err)); }).finally(function () {
        button.disabled = false;
      });
    }
    root.querySelector("[data-vault-refresh]").addEventListener("click", function () {
      setError("");
      options.refresh().catch(function (err) { setError(err.message || String(err)); });
    });
    root.querySelector("[data-vault-sync]").addEventListener("click", function (event) {
      perform(event.currentTarget, "vault.sync", {});
    });
    root.querySelector("[data-vault-init]").addEventListener("click", function (event) {
      perform(event.currentTarget, "vault.init", {}, function (data) { showCode("recovery", data.recovery_code); });
    });
    root.querySelector("[data-vault-join]").addEventListener("click", function (event) {
      var code = inviteInput.value.trim();
      if (!code) { setError(t("vault.inviteRequired")); inviteInput.focus(); return; }
      perform(event.currentTarget, "vault.join", { code: code }, function (data) {
        if (inviteInput.value.trim() === code) inviteInput.value = "";
        showCode("recovery", data.recovery_code);
      });
    });
    root.querySelector("[data-vault-invite]").addEventListener("click", function (event) {
      perform(event.currentTarget, "vault.invite", {}, function (data) { showCode("invite", data.code); });
    });
    root.querySelector("[data-vault-key]").addEventListener("click", function (event) {
      perform(event.currentTarget, "vault.key.show", {}, function (data) { showCode("recovery", data.recovery_code); });
    });
    root.querySelector("[data-vault-copy]").addEventListener("click", function () {
      options.copy(codeText.textContent);
    });
    root.querySelector("[data-vault-dismiss]").addEventListener("click", function () {
      codeBox.hidden = true; codeText.textContent = ""; codeKind = null;
    });
    fileInput.addEventListener("change", function () {
      var file = fileInput.files[0];
      fileInput.value = "";
      if (!file) return;
      fileInput.disabled = true; setError("");
      Promise.resolve().then(function () { return options.upload(file); })
        .then(function () { return options.refresh(); })
        .catch(function (err) { setError(err.message || String(err)); })
        .finally(function () { fileInput.disabled = false; });
    });

    function download(name) {
      var link = document.createElement("a");
      link.href = "/api/vault/download?name=" + encodeURIComponent(name);
      link.download = name; document.body.appendChild(link);
      try { link.click(); } finally { link.remove(); }
    }
    function shortId(id) { return id.length > 24 ? id.slice(0, 18) + "…" : id; }
    function orderedRow(body, row, index) {
      if (body.children[index] !== row) body.insertBefore(row, body.children[index] || null);
    }
    function render(status, listing) {
      var previousGroup = lastStatus && lastStatus.group && lastStatus.group.id;
      if (!status || !status.group || status.group.id !== previousGroup) lastFiles = [];
      lastStatus = status;
      if (listing && Array.isArray(listing.files)) lastFiles = listing.files;
      var group = status && status.group;
      setup.hidden = !!group; groupPanel.hidden = !group;
      root.querySelector("[data-vault-sync]").disabled = !group;
      if (!group) { lastFiles = []; return; }
      groupId.textContent = group.id || ""; groupId.title = group.id || "";
      memberList.replaceChildren();
      (Array.isArray(group.members) ? group.members : []).forEach(function (member) {
        var li = document.createElement("li"), dot = document.createElement("span");
        var id = String(member.did || member.node || ""), name = String(member.name || shortId(id));
        dot.className = "vault-dot" + (member.online ? " online" : "");
        dot.setAttribute("aria-hidden", "true"); li.appendChild(dot);
        var label = document.createElement("span"); label.title = id;
        label.textContent = name + (member.self ? " (" + t("vault.self") + ")" : "") + " — " +
          t(member.online ? "vault.online" : "vault.offline") + ", " + t("vault.free", { size: formatBytes(member.free) });
        li.appendChild(label); memberList.appendChild(li);
      });
      var keptOwners = new Set();
      (Array.isArray(status.owners) ? status.owners : []).forEach(function (owner, index) {
        var id = String(owner.owner || ""), row = owners.get(id);
        if (!row) {
          row = document.createElement("tr");
          for (var i = 0; i < 5; i++) row.appendChild(document.createElement("td"));
          row.cells[0].className = "mono";
          var badge = document.createElement("span"); badge.className = "vault-state";
          row.cells[4].appendChild(badge); owners.set(id, row);
        }
        keptOwners.add(id); row.cells[0].textContent = shortId(id); row.cells[0].title = id;
        row.cells[1].textContent = Number(owner.files) || 0;
        row.cells[2].textContent = formatBytes(owner.bytes);
        row.cells[3].textContent = (Number(owner.confirmed) || 0) + "/" + (Number(owner.target) || 0);
        var state = String(owner.state || ""), validStates = ["local_only", "replicating", "healthy", "degraded", "conflict", "insufficient_capacity"];
        var badge = row.cells[4].firstChild;
        badge.dataset.state = validStates.indexOf(state) >= 0 ? state : "";
        badge.textContent = validStates.indexOf(state) >= 0 ? t("vault.state." + state) : state;
        orderedRow(ownerBody, row, index);
      });
      owners.forEach(function (row, id) { if (!keptOwners.has(id)) { row.remove(); owners.delete(id); } });
      var keptFiles = new Set();
      lastFiles.forEach(function (file, index) {
        var name = String(file.name || ""), row = files.get(name);
        if (!row) {
          row = document.createElement("tr");
          for (var i = 0; i < 3; i++) row.appendChild(document.createElement("td"));
          row.cells[2].className = "vault-actions";
          var get = document.createElement("button"), remove = document.createElement("button");
          get.type = remove.type = "button"; remove.className = "danger";
          get.addEventListener("click", function () { download(name); });
          remove.addEventListener("click", function () {
            if (global.confirm(t("vault.deleteConfirm", { name: name }))) perform(remove, "vault.rm", { name: name });
          });
          row.cells[2].append(get, remove); files.set(name, row);
        }
        keptFiles.add(name); row.cells[0].textContent = name; row.cells[0].title = name;
        row.cells[1].textContent = formatBytes(file.size);
        row.cells[2].children[0].textContent = t("vault.download");
        row.cells[2].children[0].setAttribute("aria-label", t("vault.download") + " " + name);
        row.cells[2].children[1].textContent = t("vault.delete");
        row.cells[2].children[1].setAttribute("aria-label", t("vault.delete") + " " + name);
        orderedRow(fileBody, row, index);
      });
      files.forEach(function (row, name) { if (!keptFiles.has(name)) { row.remove(); files.delete(name); } });
      root.querySelector("[data-vault-empty]").hidden = lastFiles.length !== 0;
    }
    return {
      render: render,
      setError: setError,
      refreshLanguage: function () {
        if (codeKind) codeLabel.textContent = t(codeKind === "recovery" ? "vault.recoveryCode" : "vault.inviteCode");
        if (lastStatus) render(lastStatus, { files: lastFiles });
      }
    };
  }
  global.MistlVault = { create: create };
})(window);
