/* Web page: publish a built site folder and list known sites. Transport and
   polling live in the shell. */
(function (global) {
  "use strict";

  // Mirrors src/web_host/site.rs (BUILD_DIR_NAMES, valid_name, limits).
  const BUILD_DIRS = new Set(["dist", "build", "out", "public", "www", "site", "html"]);
  const MAX_FILES = 20000;
  const MAX_FILE_BYTES = 512 * 1024 * 1024;
  const NAME_RE = /^[a-z0-9](?:[a-z0-9-]{0,46}[a-z0-9])?$/;

  function slugify(value) {
    let out = "";
    for (const c of String(value || "")) {
      if (/[A-Za-z0-9]/.test(c)) out += c.toLowerCase();
      else if (out && !out.endsWith("-")) out += "-";
    }
    return out.slice(0, 48).replace(/-+$/, "");
  }

  function validName(name) {
    return NAME_RE.test(name);
  }

  /* Default site name: the folder, or its parent for build-output folders.
     Browsers do not expose the parent of a picked folder, so a bare `dist`
     yields "" and the user types the name. */
  function suggestName(folder, parent) {
    const base = BUILD_DIRS.has(String(folder || "").toLowerCase()) ? parent : folder;
    const name = slugify(base);
    return validName(name) ? name : "";
  }

  /* The dot segment that makes `web.publish` skip a path (dotfiles and
     dot-folders except .well-known), e.g. ".git/" or "a/.env"; "" if kept. */
  function skippedPrefix(path) {
    const segments = path.split("/");
    const i = segments.findIndex((s) => s.startsWith(".") && s !== ".well-known");
    if (i < 0) return "";
    return segments.slice(0, i + 1).join("/") + (i < segments.length - 1 ? "/" : "");
  }

  /* [{path, file}] -> {files, skipped, total}, sorted like the manifest. */
  function partition(entries) {
    const files = [], skipped = new Set();
    let total = 0;
    for (const entry of entries) {
      const prefix = skippedPrefix(entry.path);
      if (prefix) { skipped.add(prefix); continue; }
      files.push(entry);
      total += entry.file.size;
    }
    files.sort((a, b) => (a.path < b.path ? -1 : a.path > b.path ? 1 : 0));
    return { files, skipped: [...skipped].sort(), total };
  }

  /* <input webkitdirectory> files: strip the picked folder from each path. */
  function fromFileList(list) {
    const entries = [];
    let folder = "";
    for (const file of Array.from(list || [])) {
      const rel = file.webkitRelativePath || file.name;
      const cut = rel.indexOf("/");
      if (cut > 0) {
        folder = folder || rel.slice(0, cut);
        entries.push({ path: rel.slice(cut + 1), file });
      } else {
        entries.push({ path: rel, file });
      }
    }
    return { folder, entries };
  }

  function readBatch(reader) {
    return new Promise((resolve, reject) => reader.readEntries(resolve, reject));
  }

  async function walk(entry, prefix, out, skipped) {
    const path = prefix + entry.name;
    if (entry.name.startsWith(".") && entry.name !== ".well-known") {
      skipped.add(path + (entry.isDirectory ? "/" : ""));
      return;
    }
    if (entry.isFile) {
      out.push({ path, file: await new Promise((resolve, reject) => entry.file(resolve, reject)) });
    } else if (entry.isDirectory) {
      const reader = entry.createReader();
      for (;;) {
        const batch = await readBatch(reader);
        if (!batch.length) break;
        for (const child of batch) await walk(child, path + "/", out, skipped);
      }
    }
  }

  /* Dropped items -> {folder, entries, skipped}. A single dropped folder is
     the site root; `webkitGetAsEntry` must run during the drop event. */
  async function fromDropEntries(roots) {
    const out = [], skipped = new Set();
    const single = roots.length === 1 && roots[0].isDirectory ? roots[0] : null;
    if (single) {
      const reader = single.createReader();
      for (;;) {
        const batch = await readBatch(reader);
        if (!batch.length) break;
        for (const child of batch) await walk(child, "", out, skipped);
      }
    } else {
      for (const root of roots) await walk(root, "", out, skipped);
    }
    return { folder: single ? single.name : "", entries: out, skipped };
  }

  function create({ root, t, formatBytes, copy, confirm, publish, mutate, refresh }) {
    const document = root.ownerDocument;
    const cards = new Map();
    let model = { sites: [], loading: true, error: "" };
    let selection = null, job = null, result = null, publishError = "";

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

    function button(onClick, className) {
      const element = node("button", className);
      element.type = "button";
      element.addEventListener("click", onClick);
      return element;
    }

    function field(labelElement, control) {
      const wrap = node("label", "web-field");
      wrap.append(labelElement, control);
      return wrap;
    }

    function urlRow() {
      const row = node("div", "web-url");
      const link = node("a", "mono");
      link.target = "_blank";
      link.rel = "noopener noreferrer";
      const open = node("a", "web-open");
      open.target = "_blank";
      open.rel = "noopener noreferrer";
      const copyButton = button(() => copy(link.href));
      row.append(link, open, copyButton);
      return {
        row, open, copyButton,
        set(url) {
          if (link.getAttribute("href") !== url) { link.href = url; open.href = url; }
          write(link, url);
          link.title = url;
        },
      };
    }

    /* ---------- layout ---------- */

    const title = node("h2");
    title.id = "web-title";
    const intro = node("p", "web-muted");

    const publishCard = node("section", "web-card web-publish");
    const cardTitle = node("h3");
    // The Choose button is the keyboard path; the zone adds drag and drop.
    const drop = node("div", "web-drop");
    const dropText = node("span");
    const picker = node("input");
    picker.type = "file";
    picker.multiple = true;
    picker.setAttribute("webkitdirectory", "");
    picker.hidden = true;
    const chooseButton = button(() => picker.click());
    drop.append(dropText, chooseButton, picker);

    const chosen = node("div", "web-chosen");
    const chosenName = node("strong");
    const chosenSummary = node("span", "web-muted");
    const clearButton = button(clearSelection);
    chosen.append(chosenName, chosenSummary, clearButton);
    const skippedBox = node("details", "web-skipped");
    const skippedSummary = node("summary");
    const skippedList = node("ul");
    skippedBox.append(skippedSummary, skippedList);

    const nameLabel = node("span");
    const nameInput = node("input", "mono");
    nameInput.type = "text";
    nameInput.id = "web-name";
    nameInput.maxLength = 48;
    nameInput.autocomplete = "off";
    nameInput.spellcheck = false;
    nameInput.placeholder = "my-game";
    const nameHelp = node("p", "web-muted");
    const entryLabel = node("span");
    const entryInput = node("input", "mono");
    entryInput.type = "text";
    entryInput.id = "web-entry";
    entryInput.value = "index.html";
    entryInput.spellcheck = false;
    const spaLabel = node("label", "switch-control");
    const spaText = node("span");
    const spaSwitch = node("span", "toggle-switch");
    const spaInput = node("input");
    spaInput.type = "checkbox";
    spaInput.id = "web-spa";
    spaInput.setAttribute("role", "switch");
    const spaSlider = node("span", "slider");
    spaSlider.setAttribute("aria-hidden", "true");
    spaSwitch.append(spaInput, spaSlider);
    spaLabel.append(spaText, " ", spaSwitch);
    const coiLabel = node("span");
    const coiSelect = node("select");
    coiSelect.id = "web-coi";
    for (const value of ["auto", "on", "off"]) {
      const option = node("option");
      option.value = value;
      coiSelect.append(option);
    }
    const fields = node("div", "web-fields");
    fields.append(field(nameLabel, nameInput), field(entryLabel, entryInput), field(coiLabel, coiSelect));
    const formError = node("p", "web-error");
    formError.setAttribute("role", "alert");

    const actions = node("div", "row web-actions");
    const publishButton = button(startPublish, "primary");
    const abortButton = button(abortPublish, "danger");
    actions.append(publishButton, abortButton);
    const progress = node("div", "web-progress");
    const bar = node("progress");
    bar.max = 1;
    const progressText = node("span", "web-muted");
    progress.append(bar, progressText);

    const done = node("div", "web-result");
    done.setAttribute("role", "status");
    const doneText = node("p");
    const doneUrl = urlRow();
    const gdmHint = node("p", "web-muted");
    done.append(doneText, doneUrl.row, gdmHint);

    const form = node("div", "web-form");
    form.append(chosen, skippedBox, fields, nameHelp, spaLabel, actions, progress);
    publishCard.append(cardTitle, drop, form, formError, done);

    const listHead = node("div", "web-list-head");
    const listTitle = node("h3");
    const refreshButton = button(() => refresh());
    listHead.append(listTitle, refreshButton);
    const listError = node("p", "web-error");
    listError.setAttribute("role", "alert");
    const empty = node("p", "empty-note");
    const list = node("div", "web-list");
    root.append(title, intro, publishCard, listHead, listError, empty, list);

    /* ---------- folder selection ---------- */

    function select(folder, entries, extraSkipped) {
      const parts = partition(entries);
      for (const path of extraSkipped || []) parts.skipped.push(path);
      parts.skipped.sort();
      selection = Object.assign({ folder }, parts);
      result = null;
      publishError = "";
      const suggested = suggestName(folder, "");
      if (suggested || !nameInput.value) nameInput.value = suggested;
      if (!selection.files.some((f) => f.path === entryInput.value.trim())) {
        if (selection.files.some((f) => f.path === "index.html")) entryInput.value = "index.html";
      }
      sync();
      (nameInput.value ? publishButton : nameInput).focus();
    }

    function clearSelection() {
      if (job) return;
      selection = null;
      publishError = "";
      picker.value = "";
      sync();
      chooseButton.focus();
    }

    picker.addEventListener("change", () => {
      const { folder, entries } = fromFileList(picker.files);
      select(folder, entries);
    });
    drop.addEventListener("click", (event) => {
      if (event.target === drop || event.target === dropText) picker.click();
    });
    drop.addEventListener("dragover", (event) => {
      if (job) return;
      event.preventDefault();
      drop.classList.add("dragging");
    });
    drop.addEventListener("dragleave", () => drop.classList.remove("dragging"));
    drop.addEventListener("drop", (event) => {
      event.preventDefault();
      drop.classList.remove("dragging");
      if (job) return;
      const items = Array.from(event.dataTransfer?.items || []);
      const roots = items.filter((item) => item.kind === "file")
        .map((item) => item.webkitGetAsEntry && item.webkitGetAsEntry()).filter(Boolean);
      if (!roots.length) {
        select("", Array.from(event.dataTransfer?.files || [], (file) => ({ path: file.name, file })));
        return;
      }
      fromDropEntries(roots).then(({ folder, entries, skipped }) => select(folder, entries, [...skipped]), (error) => {
        selection = null;
        publishError = t("web.readFailed", { error: error?.message || String(error) });
        sync();
      });
    });

    /* ---------- publish ---------- */

    function problem() {
      if (!selection) return "";
      if (!selection.files.length) return t("web.noFiles");
      if (selection.files.length > MAX_FILES) return t("web.tooMany", { max: MAX_FILES });
      const big = selection.files.find((f) => f.file.size > MAX_FILE_BYTES);
      if (big) return t("web.tooLarge", { path: big.path, max: formatBytes(MAX_FILE_BYTES) });
      const name = nameInput.value.trim();
      if (name && !validName(name)) return t("web.nameInvalid");
      const entry = entryInput.value.trim() || "index.html";
      if (!selection.files.some((f) => f.path === entry)) return t("web.entryMissing", { entry });
      return "";
    }

    function options() {
      const coi = coiSelect.value;
      return {
        name: nameInput.value.trim(),
        entry: entryInput.value.trim() || "index.html",
        spa: spaInput.checked,
        coi: coi === "auto" ? null : coi === "on",
      };
    }

    async function startPublish() {
      if (job || !selection || problem()) return;
      if (!validName(nameInput.value.trim())) {
        publishError = t("web.nameInvalid");
        sync();
        nameInput.focus();
        return;
      }
      publishError = "";
      result = null;
      job = { sent: 0, total: selection.total, count: selection.files.length, phase: "upload", handle: null };
      const current = job;
      sync();
      current.handle = publish(options(), selection.files, (sent, total, phase) => {
        if (job !== current) return;
        Object.assign(current, { sent, total, phase: phase || current.phase });
        syncProgress();
      });
      try {
        const data = await current.handle.promise;
        result = data;
        selection = null;
        picker.value = "";
        refresh();
      } catch (error) {
        publishError = current.aborted ? t("web.aborted") : error?.message || String(error);
      } finally {
        if (job === current) job = null;
        sync();
        if (result) doneUrl.open.focus();
      }
    }

    function abortPublish() {
      if (!job || job.phase === "publish") return;
      job.aborted = true;
      job.handle?.abort();
    }

    /* ---------- render ---------- */

    function syncProgress() {
      progress.hidden = !job;
      if (!job) return;
      const fraction = job.total ? Math.min(1, job.sent / job.total) : 1;
      if (job.phase === "publish") bar.removeAttribute("value");
      else bar.value = fraction;
      write(progressText, job.phase === "publish" ? t("web.publishing") :
        t("web.uploading", { done: formatBytes(job.sent), total: formatBytes(job.total) }));
      abortButton.disabled = job.phase === "publish";
    }

    function sync() {
      drop.hidden = Boolean(selection) || Boolean(job);
      form.hidden = !selection;
      if (selection) {
        write(chosenName, selection.folder || "—");
        write(chosenSummary, t("web.summary", { count: selection.files.length, size: formatBytes(selection.total) }));
        skippedBox.hidden = !selection.skipped.length;
        write(skippedSummary, t("web.skipped", { count: selection.skipped.length }));
        skippedList.replaceChildren(...selection.skipped.slice(0, 200).map((path) => node("li", "mono", path)));
      }
      const busy = Boolean(job);
      for (const control of [nameInput, entryInput, spaInput, coiSelect, clearButton]) control.disabled = busy;
      message(formError, publishError || problem());
      publishButton.hidden = busy;
      publishButton.disabled = Boolean(problem());
      abortButton.hidden = !busy;
      syncProgress();
      done.hidden = !result;
      if (result) {
        write(doneText, t("web.published", { name: result.name }));
        doneUrl.set(result.url);
      }
    }

    function statusText(site) {
      const p = site.progress;
      if (p && p.state === "installing") {
        const pct = p.total_bytes ? Math.floor((p.done_bytes / p.total_bytes) * 100) : 0;
        return t("web.installing", { pct });
      }
      if (p && p.state === "failed") return t("web.failed") + (p.error ? ": " + p.error : "");
      const parts = [t(site.installed ? "web.installed" : "web.notInstalled")];
      if (site.update_pending) parts.push(t("web.updatePending"));
      return parts.join(" · ");
    }

    function makeCard(id) {
      const element = node("article", "web-card web-site");
      const head = node("div", "web-site-head");
      const name = node("h4");
      const own = node("span", "web-badge");
      const trusted = node("span", "web-badge");
      const status = node("span", "web-status");
      head.append(name, own, trusted, status);
      const meta = node("p", "web-muted");
      const url = urlRow();
      const siteActions = node("div", "row web-actions");
      const trustButton = button(() => act(id, card.site.trusted ? "web.untrust" : "web.trust"));
      const removeButton = button(() => {
        if (confirm(t("web.confirmRemove", { name: card.site.name }))) act(id, "web.rm");
      }, "danger");
      siteActions.append(trustButton, removeButton);
      const error = node("p", "web-error");
      error.setAttribute("role", "alert");
      element.append(head, meta, url.row, siteActions, error);
      const card = { element, name, own, trusted, status, meta, url, trustButton, removeButton, error, site: null, busy: false };
      return card;
    }

    async function act(id, cmd) {
      const card = cards.get(id);
      if (!card || card.busy) return;
      card.busy = true;
      message(card.error, "");
      updateCard(card);
      try {
        await mutate(cmd, { site: id });
      } catch (error) {
        message(card.error, error?.message || String(error));
      } finally {
        card.busy = false;
        updateCard(card);
        refresh();
      }
    }

    function updateCard(card) {
      const site = card.site;
      write(card.name, site.name);
      card.name.title = site.site;
      message(card.own, site.own ? t("web.own") : "");
      message(card.trusted, site.trusted && !site.own ? t("web.trusted") : "");
      write(card.status, statusText(site));
      card.status.dataset.state = site.progress?.state || (site.installed ? "installed" : "");
      write(card.meta, [formatBytes(site.size), t("web.files", { count: site.files })].join(" · "));
      card.url.set(site.url);
      write(card.url.open, t("web.open"));
      write(card.url.copyButton, t("web.copy"));
      card.trustButton.hidden = site.own;
      write(card.trustButton, t(site.trusted ? "web.untrust" : "web.trust"));
      write(card.removeButton, t("web.remove"));
      card.trustButton.disabled = card.removeButton.disabled = card.busy;
    }

    function render(next) {
      model = next;
      message(listError, model.error);
      const sites = model.sites || [];
      message(empty, model.loading ? t("web.loading") : !sites.length && !model.error ? t("web.empty") : "");
      const seen = new Set();
      const ordered = sites.map((site) => {
        seen.add(site.site);
        let card = cards.get(site.site);
        if (!card) { card = makeCard(site.site); cards.set(site.site, card); }
        card.site = site;
        updateCard(card);
        return card.element;
      });
      for (const [id, card] of cards) {
        if (!seen.has(id)) { card.element.remove(); cards.delete(id); }
      }
      // Re-append only when the order changed, so focus survives polling.
      if (ordered.some((element, i) => list.children[i] !== element)) list.replaceChildren(...ordered);
    }

    function refreshLanguage() {
      write(title, t("web.title"));
      write(intro, t("web.intro"));
      write(cardTitle, t("web.publish.title"));
      write(dropText, t("web.drop"));
      write(chooseButton, t("web.choose"));
      write(clearButton, t("web.clear"));
      write(nameLabel, t("web.name"));
      write(nameHelp, t("web.nameHelp"));
      write(entryLabel, t("web.entry"));
      write(spaText, t("web.spa"));
      write(coiLabel, t("web.coi"));
      for (const option of coiSelect.options) write(option, t("web.coi." + option.value));
      write(publishButton, t("web.publishBtn"));
      write(abortButton, t("web.abort"));
      write(doneUrl.open, t("web.open"));
      write(doneUrl.copyButton, t("web.copy"));
      write(gdmHint, t("web.gdmHint"));
      write(listTitle, t("web.sites"));
      write(refreshButton, t("common.refresh"));
      sync();
      render(model);
    }

    for (const control of [nameInput, entryInput]) {
      control.addEventListener("input", () => { publishError = ""; sync(); });
    }
    refreshLanguage();
    return { render, refreshLanguage };
  }

  global.MistlWebHost = { create, suggestName, slugify, skippedPrefix, partition, fromFileList, fromDropEntries };
})(window);
