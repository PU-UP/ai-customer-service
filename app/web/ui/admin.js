const $ = (id) => document.getElementById(id);

function toast(kind, title, msg) {
  const el = $("toast");
  const t = $("toastTitle");
  const m = $("toastMsg");
  el.classList.remove("ok", "bad");
  el.classList.add("show");
  el.classList.add(kind === "ok" ? "ok" : "bad");
  t.textContent = title;
  m.textContent = msg || "";
  clearTimeout(el._timer);
  el._timer = setTimeout(() => el.classList.remove("show"), 3200);
}

async function apiGet(url) {
  const r = await fetch(url, { credentials: "same-origin" });
  if (r.status === 401) {
    // Not logged in (or cookie missing/expired). Send user to login first.
    window.location.href = "/admin/login";
    throw new Error("401 Unauthorized");
  }
  if (!r.ok) throw new Error(`${r.status} ${await r.text()}`);
  return await r.json();
}

async function apiPutJson(url, body) {
  const r = await fetch(url, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
    credentials: "same-origin",
  });
  if (r.status === 401) {
    window.location.href = "/admin/login";
    throw new Error("401 Unauthorized");
  }
  const txt = await r.text();
  if (!r.ok) throw new Error(`${r.status} ${txt}`);
  return txt ? JSON.parse(txt) : {};
}

function setTab(name) {
  for (const btn of document.querySelectorAll(".tab")) {
    const active = btn.dataset.tab === name;
    btn.setAttribute("aria-selected", active ? "true" : "false");
  }
  for (const pane of document.querySelectorAll("[data-pane]")) {
    pane.style.display = pane.dataset.pane === name ? "" : "none";
  }
}

function normalizeLines(s) {
  return (s || "")
    .split("\n")
    .map((x) => x.trim())
    .filter(Boolean);
}

function prettyJson(obj) {
  return JSON.stringify(obj ?? {}, null, 2);
}

function safeParseJson(text) {
  try {
    return { ok: true, value: JSON.parse(text) };
  } catch (e) {
    return { ok: false, error: String(e && e.message ? e.message : e) };
  }
}

function clampKeywords(list) {
  const out = [];
  for (const k of list || []) {
    const s = String(k || "").trim();
    if (s) out.push(s);
  }
  return out;
}

function renderFaq(list, filterText) {
  const root = $("faqList");
  root.innerHTML = "";
  const f = (filterText || "").trim().toLowerCase();
  const items = (list || []).filter((it) => {
    if (!f) return true;
    const ks = (it.keywords || []).join(" ").toLowerCase();
    const ans = String(it.answer || "").toLowerCase();
    return ks.includes(f) || ans.includes(f);
  });

  items.forEach((it, idx) => {
    const card = document.createElement("div");
    card.className = "faqCard";

    const head = document.createElement("div");
    head.className = "faqHead";

    const left = document.createElement("div");
    left.style.flex = "1";

    const labelK = document.createElement("div");
    labelK.className = "label";
    labelK.textContent = "关键词（点击 tag 删除；新增用 +）";

    const tagRow = document.createElement("div");
    tagRow.className = "tagRow";

    const kws = clampKeywords(it.keywords || []);
    kws.forEach((k) => {
      const tag = document.createElement("button");
      tag.type = "button";
      tag.className = "tag";
      tag.title = "点击删除";
      tag.textContent = k;
      tag.onclick = () => {
        it.keywords = (it.keywords || []).filter((x) => String(x).trim() !== k);
        renderFaq(list, $("faqSearch").value);
      };
      tagRow.appendChild(tag);
    });

    const addBtn = document.createElement("button");
    addBtn.type = "button";
    addBtn.className = "tagBtn";
    addBtn.textContent = "+ 新增关键词";
    addBtn.onclick = () => {
      const v = prompt("新增关键词（非空）");
      const s = String(v || "").trim();
      if (!s) return;
      it.keywords = clampKeywords([...(it.keywords || []), s]);
      renderFaq(list, $("faqSearch").value);
    };
    tagRow.appendChild(addBtn);

    left.appendChild(labelK);
    left.appendChild(tagRow);

    const del = document.createElement("button");
    del.type = "button";
    del.className = "btnGhost danger";
    del.textContent = "删除";
    del.onclick = () => {
      if (!confirm("确定删除这条 FAQ？")) return;
      const i = list.indexOf(it);
      if (i >= 0) list.splice(i, 1);
      renderFaq(list, $("faqSearch").value);
    };

    head.appendChild(left);
    head.appendChild(del);

    const ansField = document.createElement("div");
    ansField.className = "field";
    ansField.style.marginTop = "10px";
    const l = document.createElement("label");
    l.className = "label";
    l.textContent = "答案";
    const ta = document.createElement("textarea");
    ta.className = "textarea";
    ta.style.minHeight = "140px";
    ta.value = it.answer || "";
    ta.oninput = () => (it.answer = ta.value);
    ansField.appendChild(l);
    ansField.appendChild(ta);

    card.appendChild(head);
    card.appendChild(ansField);
    root.appendChild(card);
  });

  if (items.length === 0) {
    const empty = document.createElement("div");
    empty.className = "muted small";
    empty.textContent = f ? "没有匹配的 FAQ。" : "暂无 FAQ。点击“新增一条”开始。";
    root.appendChild(empty);
  }
}

let state = {
  clubProfile: {},
  faq: [],
  prompt: "",
  paths: {},
};

function syncProfileFormFromJson(cp) {
  $("cp_club_name").value = cp.club_name || "";
  $("cp_city").value = cp.city || "";
  $("cp_venues").value = (cp.venues || []).join("\n");
  $("cp_targets").value = (cp.target_students || []).join("\n");
}

function applyProfileFormToJson(cp) {
  const out = { ...(cp || {}) };
  out.club_name = $("cp_club_name").value || "";
  out.city = $("cp_city").value || "";
  out.venues = normalizeLines($("cp_venues").value);
  out.target_students = normalizeLines($("cp_targets").value);
  return out;
}

function wireProfileSync() {
  const onFormChange = () => {
    state.clubProfile = applyProfileFormToJson(state.clubProfile);
    if ($("cpJsonWrap").style.display !== "none") {
      $("cp_json").value = prettyJson(state.clubProfile);
    }
  };
  ["cp_club_name", "cp_city", "cp_venues", "cp_targets"].forEach((id) => {
    $(id).addEventListener("input", onFormChange);
  });

  $("cp_json").addEventListener("input", () => {
    const parsed = safeParseJson($("cp_json").value);
    if (!parsed.ok) return;
    if (typeof parsed.value !== "object" || parsed.value === null || Array.isArray(parsed.value)) return;
    state.clubProfile = parsed.value;
    syncProfileFormFromJson(state.clubProfile);
  });
}

async function loadAll() {
  const data = await apiGet("/admin/api/assets");
  state.clubProfile = data.club_profile || {};
  state.faq = data.faq || [];
  state.prompt = data.system_prompt || "";
  state.paths = data.paths || {};

  $("scenarioName").textContent = data.scenario || "-";
  $("pathClub").textContent = state.paths.club_profile || "-";
  $("pathFaq").textContent = state.paths.faq || "-";
  $("pathPrompt").textContent = state.paths.system_prompt || "-";

  $("cpPath").textContent = state.paths.club_profile || "-";
  $("faqPath").textContent = state.paths.faq || "-";
  $("promptPath").textContent = state.paths.system_prompt || "-";

  syncProfileFormFromJson(state.clubProfile);
  $("cp_json").value = prettyJson(state.clubProfile);

  $("promptText").value = state.prompt;

  renderFaq(state.faq, $("faqSearch").value);
}

function validateFaq(list) {
  if (!Array.isArray(list)) return "FAQ 必须是数组";
  for (let i = 0; i < list.length; i++) {
    const it = list[i];
    if (!it || typeof it !== "object" || Array.isArray(it)) return `faq[${i}] 必须是对象`;
    const kws = clampKeywords(it.keywords || []);
    if (kws.length === 0) return `faq[${i}].keywords 不能为空`;
    if (!String(it.answer || "").trim()) return `faq[${i}].answer 不能为空`;
  }
  return null;
}

function bindTabs() {
  for (const btn of document.querySelectorAll(".tab")) {
    btn.addEventListener("click", () => setTab(btn.dataset.tab));
  }
}

function init() {
  bindTabs();
  wireProfileSync();

  $("btnReload").onclick = async () => {
    try {
      await loadAll();
      toast("ok", "已重新加载", "已从服务端读取最新资产。");
    } catch (e) {
      toast("bad", "加载失败", String(e));
    }
  };

  $("btnToggleProfileJson").onclick = () => {
    const w = $("cpJsonWrap");
    const open = w.style.display === "none";
    w.style.display = open ? "" : "none";
    if (open) $("cp_json").value = prettyJson(state.clubProfile);
  };

  $("btnSaveProfile").onclick = async () => {
    try {
      const parsed = safeParseJson($("cp_json").value);
      if ($("cpJsonWrap").style.display !== "none") {
        if (!parsed.ok) throw new Error(`Club Profile JSON 无法解析：${parsed.error}`);
        if (typeof parsed.value !== "object" || parsed.value === null || Array.isArray(parsed.value)) {
          throw new Error("Club Profile 必须是 JSON 对象");
        }
        state.clubProfile = parsed.value;
        syncProfileFormFromJson(state.clubProfile);
      } else {
        state.clubProfile = applyProfileFormToJson(state.clubProfile);
      }

      const resp = await apiPutJson("/admin/api/club_profile", state.clubProfile);
      toast("ok", "保存成功", `已写入：${resp.path || "(unknown)"}`);
      await loadAll();
    } catch (e) {
      toast("bad", "保存失败", String(e));
    }
  };

  $("btnAddFaq").onclick = () => {
    state.faq.unshift({ keywords: ["示例关键词"], answer: "示例答案（请修改）" });
    renderFaq(state.faq, $("faqSearch").value);
  };

  $("faqSearch").oninput = () => renderFaq(state.faq, $("faqSearch").value);

  $("btnSaveFaq").onclick = async () => {
    try {
      const err = validateFaq(state.faq);
      if (err) throw new Error(err);
      const resp = await apiPutJson("/admin/api/faq", state.faq);
      toast("ok", "保存成功", `已写入：${resp.path || "(unknown)"}`);
      await loadAll();
    } catch (e) {
      toast("bad", "保存失败", String(e));
    }
  };

  $("btnSavePrompt").onclick = async () => {
    try {
      const text = $("promptText").value || "";
      const resp = await apiPutJson("/admin/api/system_prompt", { text });
      toast("ok", "保存成功", `已写入：${resp.path || "(unknown)"}`);
      await loadAll();
    } catch (e) {
      toast("bad", "保存失败", String(e));
    }
  };

  loadAll().catch((e) => toast("bad", "初始化失败", String(e)));
}

document.addEventListener("DOMContentLoaded", init);

