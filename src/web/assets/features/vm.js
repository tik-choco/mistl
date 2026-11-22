/* Virtual machine cards and dialogs. Transport and polling live in the shell. */
(function (global) {
  "use strict";

  const sizes = [
    { key: "cpus", label: "vm.cpus", fallback: 2 },
    { key: "memory_mb", label: "vm.memory", fallback: 2048 },
    { key: "disk_gb", label: "vm.disk", fallback: 20 },
  ];
  const phases = new Set(["creating", "starting", "stopping", "restoring"]);
  const states = new Set([...phases, "running", "stopped", "paused", "standby", "stale", "error"]);

  function create({ root, t, formatBytes, formatUptime, copy, config, logs, checkpoints, mutate }) {
    const document = root.ownerDocument;
    const cards = new Map();
    let model, doctor, doctorError, activeDialog;

    function node(tag, className, text) {
      const element = document.createElement(tag);
      if (className) element.className = className;
      if (text !== undefined) element.textContent = text;
      return element;
    }

    function write(element, text) {
      if (element.textContent !== text) element.textContent = text;
    }

    function message(element, text) {
      write(element, text || "");
      element.hidden = !text;
    }

    function button(text, onClick, className) {
      const element = node("button", className, text);
      element.type = "button";
      element.addEventListener("click", onClick);
      return element;
    }

    function errorLine() {
      const error = node("p", "vm-error");
      error.hidden = true;
      error.setAttribute("role", "alert");
      return error;
    }

    function commandLine(getCommand) {
      const line = node("div", "vm-command");
      const code = node("code");
      const copyButton = button("", () => copy(getCommand()));
      line.append(code, copyButton);
      return { line, code, copyButton };
    }

    function fields(element, items) {
      element.replaceChildren(...items.map(([label, value]) => {
        const entry = node("div");
        entry.append(node("dt", "", t(label)), node("dd", "", value));
        return entry;
      }));
    }

    function resourceText(row) {
      return [row.cpus && t("vm.cpuCount", { count: row.cpus }), row.memory_mb && row.memory_mb + " MB",
        row.disk_gb && row.disk_gb + " GB"].filter(Boolean).join(" · ");
    }

    const head = node("div", "vm-page-head");
    const heading = node("div");
    const title = node("h2", "", "VM");
    title.id = "vm-title";
    const host = node("details", "vm-host");
    const readiness = node("summary");
    const hostFields = node("dl", "vm-facts");
    const hostNotes = node("ul", "vm-host-notes");
    host.append(readiness, hostFields, hostNotes);
    heading.append(title, host);
    const newButton = button("", openWizard, "primary");
    head.append(heading, newButton);
    const actionError = errorLine();
    const listError = errorLine();
    const remoteHint = node("p", "vm-muted");
    const result = node("p", "vm-result");
    result.setAttribute("role", "status");
    const empty = node("p", "empty-note");
    const list = node("div", "vm-list");
    const key = commandLine(() => "mistl vm key");
    write(key.code, "mistl vm key");
    const keyDetails = node("details", "vm-key");
    const keySummary = node("summary");
    keyDetails.append(keySummary, key.line);
    const dialog = node("dialog", "vm-dialog");
    const dialogHead = node("div", "vm-dialog-head");
    const dialogTitle = node("h3");
    dialogTitle.id = "vm-dialog-title";
    dialogTitle.tabIndex = -1;
    dialog.setAttribute("aria-labelledby", dialogTitle.id);
    const dismiss = button("×", close, "vm-dismiss");
    dialogHead.append(dialogTitle, dismiss);
    const dialogBody = node("div");
    dialog.append(dialogHead, dialogBody);
    dialog.addEventListener("cancel", (event) => { event.preventDefault(); close(); });
    root.append(head, actionError, remoteHint, result, listError, empty, list, keyDetails, dialog);

    function setDoctor(data, error) {
      doctor = data;
      doctorError = error;
      const problems = data?.problems || [];
      const warnings = data?.warnings || [];
      const status = error ? t("vm.hostUnavailable") : !data ? t("vm.hostChecking") :
        data.ready ? t("vm.hostReady") : t("vm.hostMissing");
      write(readiness, [status, ...problems].join(" · "));
      readiness.title = readiness.textContent;
      readiness.dataset.status = data?.ready ? "ready" : error || data ? "missing" : "";
      hostFields.hidden = !data;
      fields(hostFields, data ? [
        ["vm.platform", [data.os, data.arch].filter(Boolean).join(" / ")],
        ["vm.qemu", data.qemu_version || data.qemu_system || "—"],
        ["vm.accel", data.accel || "—"],
        ["vm.network", data.network || "—"],
      ] : []);
      hostNotes.replaceChildren(...[...problems, ...warnings, ...(error ? [error] : [])].map((note) => node("li", "", note)));
      hostNotes.hidden = !hostNotes.childElementCount;
    }

    function makeCard() {
      const element = node("article", "vm-card");
      const header = node("div", "vm-card-head");
      const name = node("h3");
      const state = node("span", "vm-state");
      const ha = node("span", "vm-ha");
      header.append(name, state, ha);
      const facts = node("dl", "vm-facts");
      const factValues = {};
      for (const label of ["resources", "sshTarget", "uptime"]) {
        const field = node("div");
        const term = node("dt");
        const value = node("dd");
        field.append(term, value);
        facts.append(field);
        factValues[label] = { term, value };
      }
      const error = node("p", "vm-error");
      const actions = node("div", "vm-actions");
      const card = { element, name, state, ha, factValues, error, actions };
      card.power = button("", () => {
        if (isRunning(card.row)) confirmAction(card.row, "stop");
        else mutate("start", { id: card.row.server_id });
      });
      card.checkpoint = button("", () => mutate("checkpoint", { id: card.row.server_id }));
      card.logs = button("", () => openReader(card.row, "logs"));
      card.checkpoints = button("", () => openReader(card.row, "checkpoints"));
      card.remove = button("", () => confirmAction(card.row, "rm"));
      card.command = commandLine(() => card.command.code.textContent);
      actions.append(card.power, card.checkpoint, card.logs, card.checkpoints, card.remove);
      element.append(header, facts, error, actions, card.command.line);
      return card;
    }

    function isRunning(row) {
      return row.state === "running" || row.state === "paused";
    }

    function canWriteDisk(row, ha) {
      return row.state !== "standby" && row.state !== "stale" &&
        (!ha || ha.state === "holder" && ha.lease_valid && !ha.stale && !ha.moving && !ha.disabling);
    }

    function mutationLocked(row) {
      return model.remote || model.busy.has(row.server_id) || phases.has(row.state);
    }

    function removalBlocked(row, ha) {
      if (isRunning(row)) return t("vm.stopFirst");
      if (Object.hasOwn(row, "holder") && !ha?.retired) return t("vm.disableHaFirst");
      return "";
    }

    function renderCard(card, row, pending) {
      const id = row.server_id;
      const name = row.name || id;
      const ha = model.ha.get(id);
      const phase = row.state;
      const busy = model.busy.get(id);
      const locked = mutationLocked(row);
      const running = isRunning(row);
      const writable = canWriteDisk(row, ha);
      const stateLabel = { start: "vm.starting", stop: "vm.stopping", checkpoint: "vm.saving", restore: "vm.restoring", rm: "vm.removing" }[busy];
      card.row = row;
      write(card.name, name);
      write(card.state, stateLabel ? t(stateLabel) : states.has(phase) ? t("vm." + phase) : phase || "—");
      card.state.dataset.state = busy || (states.has(phase) ? phase : "");
      const role = ha?.role || (phase === "standby" ? "standby" : "");
      message(card.ha, role ? t("vm.haRole", { role: t("vm." + role) }) : "");

      const values = {
        resources: resourceText(row),
        sshTarget: row.ssh_target,
        uptime: typeof row.uptime_secs === "number" ? formatUptime(row.uptime_secs) : "",
      };
      for (const [key, { term, value }] of Object.entries(card.factValues)) {
        write(term, t("vm." + key));
        write(value, values[key] || "—");
        term.parentElement.hidden = !values[key];
      }
      message(card.error, row.last_error || ha?.last_error);
      card.actions.hidden = pending;
      write(card.power, t(running ? "vm.stop" : "vm.start"));
      card.power.disabled = Boolean(locked || !row.ssh_target || (!running && !writable));
      write(card.checkpoint, t("vm.checkpoint"));
      card.checkpoint.disabled = Boolean(locked || !row.ssh_target || !writable);
      write(card.logs, t("vm.logs"));
      card.logs.disabled = !row.ssh_target;
      write(card.checkpoints, t("vm.checkpoints"));
      write(card.remove, t("vm.remove"));
      const removeReason = removalBlocked(row, ha);
      card.remove.disabled = Boolean(locked || removeReason);
      card.remove.title = removeReason;
      card.command.line.hidden = !row.ssh_target || !/^[0-9a-f]{16}$/.test(id);
      if (!card.command.line.hidden) write(card.command.code, "mistl vm ssh " + id);
      write(card.command.copyButton, t("vm.copy"));
    }

    function render(data) {
      model = data;
      newButton.disabled = Boolean(data.remote || data.creating);
      message(actionError, data.actionError);
      message(remoteHint, data.remote && t("vm.localHint"));
      message(result, data.savedCheckpoint && t("vm.savedCheckpoint", { seq: data.savedCheckpoint }));
      message(listError, data.listError);
      const rows = data.creating ? [{ ...data.creating, state: "creating" }, ...data.rows] : data.rows;
      message(empty, !rows.length && (data.loading ? t("vm.loading") : !data.listError ? t("vm.empty") : ""));
      const ids = new Set(rows.map((row) => row.server_id));
      for (const [id, card] of cards) {
        if (ids.has(id)) continue;
        cards.delete(id);
        card.element.remove();
      }
      let previous = null;
      for (const row of rows) {
        const id = row.server_id;
        if (!cards.has(id)) cards.set(id, makeCard());
        const card = cards.get(id);
        const position = previous ? previous.nextElementSibling : list.firstElementChild;
        if (card.element !== position) list.insertBefore(card.element, position);
        renderCard(card, row, id === undefined);
        previous = card.element;
      }
      if (activeDialog?.sync) activeDialog.sync();
    }

    function refreshLanguage() {
      write(newButton, t("vm.new"));
      write(keySummary, t("vm.sshKey"));
      write(key.copyButton, t("vm.copy"));
      dismiss.setAttribute("aria-label", t("common.close"));
      setDoctor(doctor, doctorError);
      if (model) render(model);
    }

    function show(title, body, focus) {
      write(dialogTitle, title);
      dialogBody.replaceChildren(body);
      if (!dialog.open) dialog.showModal();
      (focus || dialogTitle).focus({ preventScroll: true });
    }

    function close() {
      activeDialog = null;
      if (dialog.open) dialog.close();
    }

    function inputField(label, input) {
      const field = node("label", "vm-field");
      field.append(node("span", "", label), input);
      return field;
    }

    function formStep(back, next, onSubmit) {
      const form = node("form", "vm-form");
      const actions = node("div", "vm-actions vm-dialog-actions");
      const submit = node("button", "primary", next);
      submit.type = "submit";
      if (back) actions.append(button(t("vm.back"), back));
      actions.append(submit);
      form.addEventListener("submit", (event) => { event.preventDefault(); onSubmit(); });
      return { form, actions, submit };
    }

    function openWizard() {
      const args = { name: "" };
      const session = {};
      activeDialog = session;
      const configured = config().catch(() => null);
      nameStep();

      function nameStep() {
        const input = node("input");
        input.required = true;
        input.autocomplete = "off";
        input.value = args.name;
        const { form, actions, submit } = formStep(null, t("vm.next"), async () => {
          const name = input.value.trim();
          if (!name) { message(error, t("vm.nameRequired")); input.focus(); return; }
          args.name = name;
          submit.disabled = true;
          const defaults = await configured;
          if (activeDialog === session) sizeStep(defaults);
        });
        const error = errorLine();
        form.append(inputField(t("vm.name"), input), error, actions);
        show(t("vm.askName"), form, input);
      }

      function sizeStep(defaults) {
        session.sync = null;
        const inputs = sizes.map(({ key, fallback }) => {
          const input = node("input");
          input.type = "number";
          input.required = true;
          input.min = 1;
          input.max = defaults?.["max_" + key] ?? 4294967295;
          input.value = args[key] ?? defaults?.["default_" + key] ?? fallback;
          return input;
        });
        const { form, actions } = formStep(nameStep, t("vm.next"), () => {
          sizes.forEach(({ key }, index) => { args[key] = inputs[index].valueAsNumber; });
          confirmStep(defaults);
        });
        const sizeFields = node("div", "vm-size-fields");
        sizeFields.append(...inputs.map((input, index) => inputField(t(sizes[index].label), input)));
        form.append(sizeFields, actions);
        show(t("vm.askSize"), form, inputs[0]);
      }

      function confirmStep(defaults) {
        const { form, actions, submit } = formStep(() => sizeStep(defaults), t("vm.create"), () => {
          close();
          mutate("create", args);
        });
        session.sync = () => { submit.disabled = newButton.disabled; };
        session.sync();
        const summary = node("dl", "vm-facts");
        fields(summary, [["vm.name", args.name], ["vm.resources", resourceText(args)]]);
        form.append(summary, actions);
        show(t("vm.askCreate"), form);
      }
    }

    function restoreBlocked(id) {
      const row = model.rows.find((row) => row.server_id === id);
      if (!row || mutationLocked(row)) return "";
      if (isRunning(row)) return t("vm.stopFirst");
      if (!canWriteDisk(row, model.ha.get(id))) return t("vm.holderRequired");
      return null;
    }

    function confirmAction(row, action, checkpoint, back) {
      const id = row.server_id;
      const args = { id };
      const name = row.name || id;
      const title = t("vm.confirm." + action, { name, seq: checkpoint?.seq });
      const { form, actions, submit } = formStep(back || close, t("vm." + (action === "rm" ? "remove" : action)), () => {
        if (action === "stop") args.force = force.checked;
        if (action === "restore") args.seq = checkpoint.seq;
        close();
        mutate(action, args);
      });
      const force = node("input");
      force.type = "checkbox";
      if (action === "stop") {
        const label = node("label", "vm-force");
        label.append(force, node("span", "", t("vm.force")));
        form.append(label);
      } else {
        form.append(node("p", "vm-muted", t(action === "rm" ? "vm.removeDisk" : "vm.restoreDisk")));
      }
      if (!back) write(actions.firstElementChild, t("vm.cancel"));
      form.append(actions);
      activeDialog = { sync() {
        const current = model.rows.find((row) => row.server_id === id);
        submit.disabled = !current || mutationLocked(current) || (
          action === "restore" ? restoreBlocked(id) !== null :
          action === "stop" ? !isRunning(current) :
          Boolean(removalBlocked(current, model.ha.get(id)))
        );
      } };
      activeDialog.sync();
      show(title, form);
    }

    function openReader(row, kind) {
      const session = {};
      const id = row.server_id;
      const title = t("vm.readerTitle", { section: t("vm." + kind), name: row.name || id });
      const body = node("div", "vm-form");
      const toolbar = node("div", "vm-actions vm-reader-actions");
      const refresh = button(t("common.refresh"), load);
      const error = errorLine();
      const content = node("div");
      toolbar.append(refresh);
      body.append(toolbar, error, content);
      activeDialog = session;
      show(title, body, refresh);
      load();

      async function load() {
        refresh.disabled = true;
        message(error, "");
        content.setAttribute("aria-busy", "true");
        try {
          const data = await (kind === "logs" ? logs(id) : checkpoints(id));
          if (activeDialog !== session) return;
          if (kind === "logs") {
            const text = data.lines.join("\n");
            const tail = node("pre", "vm-log", text || t("vm.noLogs"));
            tail.tabIndex = 0;
            content.replaceChildren(tail);
            tail.scrollTop = tail.scrollHeight;
          } else {
            const entries = node("ul", "vm-checkpoint-list");
            const controls = data.checkpoints.map((entry) => {
              const item = node("li");
              const info = node("div");
              const time = new Date(entry.created_at);
              const details = [Number.isNaN(time.getTime()) ? entry.created_at : time.toLocaleString(document.documentElement.lang),
                formatBytes(entry.disk_bytes)].filter(Boolean).join(" · ");
              info.append(node("strong", "", t("vm.checkpointNumber", { seq: entry.seq })), node("p", "vm-muted", details));
              const restore = button(t("vm.restore"), () => confirmAction(row, "restore", entry, () => openReader(row, kind)));
              item.append(info, restore);
              entries.append(item);
              return restore;
            });
            const hint = node("p", "vm-muted");
            content.replaceChildren(data.checkpoints.length ? entries : node("p", "vm-muted", t("vm.noCheckpoints")), hint);
            session.sync = () => {
              const reason = restoreBlocked(id);
              controls.forEach((button) => { button.disabled = reason !== null; });
              message(hint, controls.length && (model.remote ? t("vm.localHint") : reason));
            };
            session.sync();
          }
        } catch (err) {
          if (activeDialog === session) message(error, err.message);
        } finally {
          content.removeAttribute("aria-busy");
          refresh.disabled = false;
        }
      }
    }

    refreshLanguage();
    return { render, setDoctor, refreshLanguage, close };
  }

  global.MistlVM = { create };
})(window);
