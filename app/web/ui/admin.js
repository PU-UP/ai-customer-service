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
    ta.style.minHeight = "72px";
    ta.value = it.answer || "";
    ta.oninput = () => (it.answer = ta.value);
    ta.dataset.autogrow = "1";
    ta.dataset.minH = "72";
    ta.dataset.maxH = "520";
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

function coalesce(...xs) {
  for (const x of xs) {
    if (x !== undefined && x !== null) return x;
  }
  return undefined;
}

function profileSchemaVersion(cp) {
  const v = cp && typeof cp === "object" ? cp.meta?.schema_version : undefined;
  return typeof v === "number" ? v : null;
}

function toLines(xs) {
  const out = [];
  for (const x of xs || []) {
    const s = String(x || "").trim();
    if (s) out.push(s);
  }
  return out;
}

function ensureV2(cp) {
  const out = { ...(cp || {}) };
  if (profileSchemaVersion(out) === 2) return out;

  // Best-effort migrate legacy keys into v2 so the UI becomes stable.
  const name = coalesce(out.basics?.name, out.club?.name, out.club_name);
  const city = coalesce(out.basics?.city, out.club?.city, out.city);
  const venues = coalesce(out.venues, out.club?.venues, out.venues);
  const targets = coalesce(out.audience?.targets, out.audience?.target_students, out.target_students);
  const youth = coalesce(out.audience?.youth_program, out.youth_program);

  return {
    meta: { schema_version: 2 },
    basics: { name: name || "", city: city || "" },
    venues: Array.isArray(venues) ? venues : [],
    audience: {
      targets: Array.isArray(targets) ? targets : [],
      youth_program:
        youth && typeof youth === "object" && !Array.isArray(youth)
          ? { status: youth.status || "", note: youth.note || "" }
          : { status: "", note: "" },
    },
    courses: Array.isArray(out.courses) ? out.courses : [],
    policies: out.policies && typeof out.policies === "object" ? out.policies : {},
  };
}

function el(tag, className, text) {
  const d = document.createElement(tag);
  if (className) d.className = className;
  if (text !== undefined) d.textContent = text;
  return d;
}

function inputField(label, value, placeholder) {
  const wrap = el("div", "field");
  const l = el("label", "label", label);
  const inp = document.createElement("input");
  inp.className = "input";
  inp.value = value ?? "";
  if (placeholder) inp.placeholder = placeholder;
  wrap.appendChild(l);
  wrap.appendChild(inp);
  return { wrap, input: inp };
}

function textAreaField(label, value, placeholder, minHeightPx) {
  const wrap = el("div", "field");
  const l = el("label", "label", label);
  const ta = document.createElement("textarea");
  ta.className = "textarea";
  ta.value = value ?? "";
  if (placeholder) ta.placeholder = placeholder;
  if (minHeightPx) ta.style.minHeight = `${minHeightPx}px`;
  ta.dataset.autogrow = "1";
  if (minHeightPx) ta.dataset.minH = String(minHeightPx);
  wrap.appendChild(l);
  wrap.appendChild(ta);
  return { wrap, textarea: ta };
}

function autoGrowTextarea(ta, minHeightPx, maxHeightPx) {
  if (!ta) return;
  const minH = typeof minHeightPx === "number" && Number.isFinite(minHeightPx) ? minHeightPx : 0;
  const maxH = typeof maxHeightPx === "number" && Number.isFinite(maxHeightPx) ? maxHeightPx : Infinity;
  ta.style.height = "auto";
  const next = Math.max(minH, Math.min(ta.scrollHeight, maxH));
  ta.style.height = `${next}px`;
  ta.style.overflowY = ta.scrollHeight > next ? "auto" : "hidden";
}

function wireAutoGrowTextarea(ta, minHeightPx, maxHeightPx) {
  if (!ta) return;
  const handler = () => autoGrowTextarea(ta, minHeightPx, maxHeightPx);
  ta.addEventListener("input", handler);
  // Initial sizing after DOM attach.
  setTimeout(handler, 0);
}

function _parseNumAttr(el, key) {
  const raw = el?.dataset ? el.dataset[key] : undefined;
  if (raw === undefined || raw === null || raw === "") return null;
  const n = Number(raw);
  return Number.isFinite(n) ? n : null;
}

function wireAutoGrowTextareaFromDataset(ta) {
  if (!ta || ta.tagName !== "TEXTAREA") return;
  if (ta.dataset._autogrowWired === "1") return;
  if (ta.dataset.autogrow !== "1") return;
  const minH = _parseNumAttr(ta, "minH");
  const maxH = _parseNumAttr(ta, "maxH");
  ta.dataset._autogrowWired = "1";
  wireAutoGrowTextarea(ta, minH ?? 0, maxH ?? Infinity);
}

function initGlobalAutoGrow() {
  document.querySelectorAll('textarea[data-autogrow="1"]').forEach((ta) => wireAutoGrowTextareaFromDataset(ta));
  const obs = new MutationObserver((mutations) => {
    for (const m of mutations) {
      for (const n of m.addedNodes || []) {
        if (!n || n.nodeType !== 1) continue;
        if (n.tagName === "TEXTAREA") wireAutoGrowTextareaFromDataset(n);
        n.querySelectorAll?.('textarea[data-autogrow="1"]').forEach((ta) => wireAutoGrowTextareaFromDataset(ta));
      }
    }
  });
  obs.observe(document.documentElement, { childList: true, subtree: true });
}

function checkboxField(label, checked) {
  const wrap = el("div", "field");
  const l = el("label", "label", label);
  const row = el("div", "cpFormRow");
  const cb = document.createElement("input");
  cb.type = "checkbox";
  cb.checked = !!checked;
  row.appendChild(cb);
  row.appendChild(el("div", "muted small", "勾选表示：是"));
  wrap.appendChild(l);
  wrap.appendChild(row);
  return { wrap, checkbox: cb };
}

function kvRow(k, v, mono = false) {
  const row = el("div", "cpKv");
  row.appendChild(el("div", "cpK", k));
  const vv = el("div", `cpV${mono ? " cpMono" : ""}`, v);
  row.appendChild(vv);
  return row;
}

function formatPriceMap(map) {
  if (!map || typeof map !== "object" || Array.isArray(map)) return "";
  const keys = Object.keys(map).sort((a, b) => Number(a) - Number(b));
  const parts = [];
  for (const k of keys) {
    const n = map[k];
    if (typeof n !== "number") continue;
    parts.push(`${k}人 ${n}`);
  }
  return parts.join(" / ");
}

function syncEditorState(nextCp) {
  state.clubProfile = ensureV2(nextCp);
  if ($("cpJsonWrap").style.display !== "none") {
    $("cp_json").value = prettyJson(state.clubProfile);
  }
  const prev = $("cpPreview");
  if (prev) renderProfilePreview(prev, state.clubProfile);
}

function renderProfilePreview(rootEl, rawCp) {
  try {
    rootEl.innerHTML = "";
    const cp = toV2ForDisplay(rawCp);

    const secBasic = el("div", "cpSection");
    secBasic.appendChild(el("div", "cpSectionTitle", "基本信息"));
    {
      const row = el("div", "cpFormRow");
      const f1 = inputField("俱乐部名称", cp.basics.name || "", "例如：致旋网球俱乐部");
      const f2 = inputField("城市", cp.basics.city || "", "例如：Sydney");
      row.appendChild(f1.wrap);
      row.appendChild(f2.wrap);
      f1.input.oninput = () => syncEditorState({ ...cp, basics: { ...(cp.basics || {}), name: f1.input.value || "" } });
      f2.input.oninput = () => syncEditorState({ ...cp, basics: { ...(cp.basics || {}), city: f2.input.value || "" } });
      secBasic.appendChild(row);
    }
    rootEl.appendChild(secBasic);

  const secVen = el("div", "cpSection");
  secVen.appendChild(el("div", "cpSectionTitle", "场地"));
  {
    const f = textAreaField("场地（每行一个）", toLines(cp.venues).join("\n"), "每行一个场地", 72);
    wireAutoGrowTextarea(f.textarea, 72, 260);
    f.textarea.oninput = () => syncEditorState({ ...cp, venues: normalizeLines(f.textarea.value) });
    secVen.appendChild(f.wrap);
  }
  rootEl.appendChild(secVen);

  const secAud = el("div", "cpSection");
  secAud.appendChild(el("div", "cpSectionTitle", "适合人群 / 项目"));
  {
    const f = textAreaField("适合人群（每行一个）", toLines(cp.audience.targets).join("\n"), "每行一个", 72);
    wireAutoGrowTextarea(f.textarea, 72, 320);
    f.textarea.oninput = () =>
      syncEditorState({ ...cp, audience: { ...(cp.audience || {}), targets: normalizeLines(f.textarea.value) } });
    secAud.appendChild(f.wrap);
  }
  {
    const row = el("div", "cpFormRow");
    const yp = cp.audience.youth_program || {};
    const s1 = inputField("青少年状态", String(yp.status || ""), "例如：筹备中");
    const s2 = textAreaField("青少年备注", String(yp.note || ""), "例如：暂未正式开班", 54);
    wireAutoGrowTextarea(s2.textarea, 54, 180);
    row.appendChild(s1.wrap);
    row.appendChild(s2.wrap);
    const apply = () =>
      syncEditorState({
        ...cp,
        audience: {
          ...(cp.audience || {}),
          youth_program: { status: s1.input.value || "", note: s2.textarea.value || "" },
        },
      });
    s1.input.oninput = apply;
    s2.textarea.oninput = apply;
    secAud.appendChild(row);
  }
  rootEl.appendChild(secAud);

  const secCourses = el("div", "cpSection");
  secCourses.appendChild(el("div", "cpSectionTitle", "课程与价格（摘要）"));
  const grid = el("div", "cpGrid");
  (cp.courses || []).forEach((c) => {
    const card = el("div", "cpCard");
    const title = el("div", "cpCardTitle", c.name || c.id || "课程");
    card.appendChild(title);
    {
      const f = inputField("一句话简介", c.summary || "", "例如：3-6 人，推荐 4 人班");
      f.input.oninput = () => {
        const next = { ...cp, courses: cp.courses.map((x) => (x.id === c.id ? { ...c, summary: f.input.value || "" } : x)) };
        syncEditorState(next);
      };
      card.appendChild(f.wrap);
    }
    {
      const f = inputField("报名方式", c.booking || "", "例如：私信林教练报名排课 / Topspin APP 下单");
      f.input.oninput = () => {
        const next = { ...cp, courses: cp.courses.map((x) => (x.id === c.id ? { ...c, booking: f.input.value || "" } : x)) };
        syncEditorState(next);
      };
      card.appendChild(f.wrap);
    }

    if (c.id === "fixed_group") {
      const cards = Array.isArray(c.cards) ? c.cards : [];
      cards.forEach((it, idx) => {
        const box = el("div", "field");
        box.appendChild(el("div", "label", `卡片 #${idx + 1}`));
        const row = el("div", "cpFormRow");
        const n = inputField("名称", it.name || "", "例如：单月卡");
        const h = inputField("课时", String(it.hours ?? ""), "例如：8");
        row.appendChild(n.wrap);
        row.appendChild(h.wrap);
        box.appendChild(row);

        const pr = el("div", "cpFormRow");
        const priceMap = it.price_aud_per_person || {};
        ["3", "4", "5", "6"].forEach((k) => {
          const f = inputField(`${k}人价`, String(priceMap[k] ?? ""), "数字");
          f.input.inputMode = "numeric";
          f.input.oninput = () => {
            const v = Number(f.input.value);
            const nextMap = { ...(it.price_aud_per_person || {}) };
            if (!Number.isFinite(v)) delete nextMap[k];
            else nextMap[k] = v;
            const nextCards = cards.map((x, i) => (i === idx ? { ...it, price_aud_per_person: nextMap } : x));
            const nextCourse = { ...c, cards: nextCards };
            syncEditorState({ ...cp, courses: cp.courses.map((x) => (x.id === c.id ? nextCourse : x)) });
          };
          pr.appendChild(f.wrap);
        });
        box.appendChild(pr);

        const applyMeta = () => {
          const hoursNum = Number(h.input.value);
          const nextIt = { ...it, name: n.input.value || "", hours: Number.isFinite(hoursNum) ? hoursNum : it.hours };
          const nextCards = cards.map((x, i) => (i === idx ? nextIt : x));
          const nextCourse = { ...c, cards: nextCards };
          syncEditorState({ ...cp, courses: cp.courses.map((x) => (x.id === c.id ? nextCourse : x)) });
        };
        n.input.oninput = applyMeta;
        h.input.oninput = applyMeta;

        const btnRow = el("div", "cpBtnRow");
        const del = el("button", "cpBtn danger", "删除此卡");
        del.type = "button";
        del.onclick = () => {
          const nextCards = cards.filter((_, i) => i !== idx);
          const nextCourse = { ...c, cards: nextCards };
          syncEditorState({ ...cp, courses: cp.courses.map((x) => (x.id === c.id ? nextCourse : x)) });
        };
        btnRow.appendChild(del);
        box.appendChild(btnRow);

        card.appendChild(box);
      });

      const addRow = el("div", "cpBtnRow");
      const add = el("button", "cpBtn", "+ 新增卡片");
      add.type = "button";
      add.onclick = () => {
        const nextCards = [...cards, { name: "新卡", hours: 0, price_aud_per_person: { "4": 0 } }];
        const nextCourse = { ...c, cards: nextCards };
        syncEditorState({ ...cp, courses: cp.courses.map((x) => (x.id === c.id ? nextCourse : x)) });
      };
      addRow.appendChild(add);
      card.appendChild(addRow);
    } else if (c.id === "walk_in") {
      const f = inputField("单价（/人/节）", String(c.price_aud_per_lesson_per_person ?? ""), "例如：45");
      f.input.inputMode = "numeric";
      f.input.oninput = () => {
        const num = Number(f.input.value);
        const nextCourse = { ...c, price_aud_per_lesson_per_person: Number.isFinite(num) ? num : c.price_aud_per_lesson_per_person };
        syncEditorState({ ...cp, courses: cp.courses.map((x) => (x.id === c.id ? nextCourse : x)) });
      };
      card.appendChild(f.wrap);
    } else if (c.id === "private") {
      const pk = c.packages_aud || {};
      const fixed = pk.fixed_weekly_time || {};
      const flex = pk.flexible_time || {};
      const mk = inputField("不固定时间加价（%）", String(flex.markup_percent ?? ""), "例如：10");
      mk.input.inputMode = "numeric";
      mk.input.oninput = () => {
        const num = Number(mk.input.value);
        const nextFlex = { ...(flex || {}), markup_percent: Number.isFinite(num) ? num : flex.markup_percent };
        const nextCourse = { ...c, packages_aud: { ...(pk || {}), flexible_time: nextFlex } };
        syncEditorState({ ...cp, courses: cp.courses.map((x) => (x.id === c.id ? nextCourse : x)) });
      };
      card.appendChild(mk.wrap);

      const makePkgBox = (titleText, basePathKey, obj) => {
        const box = el("div", "field");
        box.appendChild(el("div", "label", titleText));
        const row = el("div", "cpFormRow");
        const f11_10 = inputField("1v1 10h", String(obj?.one_on_one?.["10_hours"] ?? ""), "数字");
        const f11_20 = inputField("1v1 20h", String(obj?.one_on_one?.["20_hours"] ?? ""), "数字");
        const f12_10 = inputField("1v2总价 10h", String(obj?.one_on_two_total?.["10_hours"] ?? ""), "数字");
        const f12_20 = inputField("1v2总价 20h", String(obj?.one_on_two_total?.["20_hours"] ?? ""), "数字");
        [f11_10, f11_20, f12_10, f12_20].forEach((x) => (x.input.inputMode = "numeric"));
        [f11_10, f11_20, f12_10, f12_20].forEach((x) => row.appendChild(x.wrap));
        box.appendChild(row);

        const apply = () => {
          const toNum = (s) => {
            const n = Number(s);
            return Number.isFinite(n) ? n : undefined;
          };
          const nextObj = {
            one_on_one: {
              "10_hours": toNum(f11_10.input.value),
              "20_hours": toNum(f11_20.input.value),
            },
            one_on_two_total: {
              "10_hours": toNum(f12_10.input.value),
              "20_hours": toNum(f12_20.input.value),
            },
          };
          const clean = (o) => {
            const oo = { ...o };
            ["one_on_one", "one_on_two_total"].forEach((k) => {
              const v = oo[k] || {};
              Object.keys(v).forEach((kk) => (v[kk] === undefined ? delete v[kk] : null));
              if (Object.keys(v).length === 0) delete oo[k];
              else oo[k] = v;
            });
            return oo;
          };
          const nextPk =
            basePathKey === "fixed_weekly_time"
              ? { ...(pk || {}), fixed_weekly_time: clean(nextObj) }
              : { ...(pk || {}), flexible_time: { ...(flex || {}), ...clean(nextObj) } };
          const nextCourse = { ...c, packages_aud: nextPk };
          syncEditorState({ ...cp, courses: cp.courses.map((x) => (x.id === c.id ? nextCourse : x)) });
        };
        [f11_10, f11_20, f12_10, f12_20].forEach((x) => (x.input.oninput = apply));
        return box;
      };

      card.appendChild(makePkgBox("固定时间套餐（AUD）", "fixed_weekly_time", fixed));
      card.appendChild(makePkgBox("不固定时间套餐（AUD，填已加价后的最终价）", "flexible_time", flex));
    }

    const notes = toLines(c.notes);
    {
      const f = textAreaField("备注（每行一条，可空）", notes.join("\n"), "每行一条", 54);
      wireAutoGrowTextarea(f.textarea, 54, 240);
      f.textarea.oninput = () => {
        const nextCourse = { ...c, notes: normalizeLines(f.textarea.value) };
        syncEditorState({ ...cp, courses: cp.courses.map((x) => (x.id === c.id ? nextCourse : x)) });
      };
      card.appendChild(f.wrap);
    }
    grid.appendChild(card);
  });
  secCourses.appendChild(grid);
  rootEl.appendChild(secCourses);

  const secPol = el("div", "cpSection");
  secPol.appendChild(el("div", "cpSectionTitle", "政策"));
  {
    const tr = cp.policies?.trial_lesson || {};
    const cb = checkboxField("是否支持试课", tr.available === true);
    cb.checkbox.onchange = () => {
      const next = {
        ...cp,
        policies: {
          ...(cp.policies || {}),
          trial_lesson: { ...(tr || {}), available: cb.checkbox.checked },
        },
      };
      syncEditorState(next);
    };
    secPol.appendChild(cb.wrap);

    const how = textAreaField("试课预约说明", tr.how_to_book || "", "例如：用户主动问试课时，私信林教练咨询安排", 54);
    wireAutoGrowTextarea(how.textarea, 54, 220);
    how.textarea.oninput = () => {
      const next = {
        ...cp,
        policies: {
          ...(cp.policies || {}),
          trial_lesson: { ...(tr || {}), how_to_book: how.textarea.value || "" },
        },
      };
      syncEditorState(next);
    };
    secPol.appendChild(how.wrap);
  }
  {
    const ca = cp.policies?.coach_assignment || {};
    const row = el("div", "cpFormRow");
    const h = inputField("Head Coach 口径", ca.head_coach || "", "例如：林教练是俱乐部 Head Coach");
    const a = inputField("分配规则口径", ca.assignment_rule || "", "例如：会按学员情况匹配合适教练");
    const s = inputField("更换口径", ca.switch_rule || "", "例如：不满意可随时申请更换");
    row.appendChild(h.wrap);
    row.appendChild(a.wrap);
    row.appendChild(s.wrap);
    const apply = () => {
      const next = {
        ...cp,
        policies: {
          ...(cp.policies || {}),
          coach_assignment: { head_coach: h.input.value || "", assignment_rule: a.input.value || "", switch_rule: s.input.value || "" },
        },
      };
      syncEditorState(next);
    };
    h.input.oninput = apply;
    a.input.oninput = apply;
    s.input.oninput = apply;
    secPol.appendChild(row);
  }
    rootEl.appendChild(secPol);
  } catch (e) {
    console.error("renderProfilePreview failed", e);
    rootEl.innerHTML = "";
    const msg = el("div", "muted small", `渲染失败：${String(e && e.message ? e.message : e)}`);
    rootEl.appendChild(msg);
  }
}

function toV2ForDisplay(rawCp) {
  const v2 = ensureV2(rawCp);
  // Ensure at least the three default course cards exist for consistent preview.
  const byId = {};
  for (const c of v2.courses || []) {
    if (c && typeof c === "object" && !Array.isArray(c) && c.id) byId[c.id] = c;
  }
  const defaults = [
    {
      id: "fixed_group",
      name: "固定小班课",
      summary: "3-6 人，推荐 4 人班",
      booking: "私信林教练报名排课",
      notes: [],
      cards: [],
    },
    {
      id: "walk_in",
      name: "Walk-in class",
      summary: "灵活约课，通常 4 人班",
      booking: "Topspin APP 查看时间并下单",
      price_aud_per_lesson_per_person: undefined,
      notes: [],
    },
    {
      id: "private",
      name: "私教课",
      summary: "支持 1v1 / 1v2",
      booking: "私信林教练报名排课",
      packages_aud: {},
    },
  ];
  const merged = defaults.map((d) => ({ ...d, ...(byId[d.id] || {}) }));
  const extras = (v2.courses || []).filter((c) => c && typeof c === "object" && !Array.isArray(c) && c.id && !defaults.find((d) => d.id === c.id));
  return { ...v2, courses: [...merged, ...extras] };
}

function syncProfileFormFromJson(cp) {
  const v2 = ensureV2(cp);
  const prev = $("cpPreview");
  if (prev) renderProfilePreview(prev, v2);
}

function applyProfileFormToJson(cp) {
  // Legacy no-op: the UI is fully driven by the section editor and optional JSON view.
  // Keep this function to minimize diff and avoid breaking call sites.
  return ensureV2(cp);
}

function wireProfileSync() {
  $("cp_json").addEventListener("input", () => {
    const parsed = safeParseJson($("cp_json").value);
    if (!parsed.ok) return;
    if (typeof parsed.value !== "object" || parsed.value === null || Array.isArray(parsed.value)) return;
    state.clubProfile = ensureV2(parsed.value);
    syncProfileFormFromJson(state.clubProfile);
  });
}

async function loadAll() {
  const prev = $("cpPreview");
  if (prev && !prev.childElementCount) {
    prev.innerHTML = "";
    prev.appendChild(el("div", "muted small", "加载中…"));
  }
  const assetStatus = $("assetStatus");
  if (assetStatus) assetStatus.textContent = "加载中…";

  try {
    const data = await apiGet("/admin/api/assets");
    state.clubProfile = data.club_profile || {};
    state.faq = data.faq || [];
    state.prompt = data.system_prompt || "";
    state.paths = data.paths || {};

    $("assetsDir").textContent = data.assets_dir || "-";
    $("pathClub").textContent = state.paths.club_profile || "-";
    $("pathFaq").textContent = state.paths.faq || "-";
    $("pathPrompt").textContent = state.paths.system_prompt || "-";

    $("cpPath").textContent = state.paths.club_profile || "-";
    $("faqPath").textContent = state.paths.faq || "-";
    $("promptPath").textContent = state.paths.system_prompt || "-";

    state.clubProfile = ensureV2(state.clubProfile);
    syncProfileFormFromJson(state.clubProfile);
    $("cp_json").value = prettyJson(state.clubProfile);
    if (prev) renderProfilePreview(prev, state.clubProfile);
    if (assetStatus) assetStatus.textContent = "已加载";

    $("promptText").value = state.prompt;

    renderFaq(state.faq, $("faqSearch").value);
  } catch (e) {
    const msg = String(e && e.message ? e.message : e);
    toast("bad", "加载失败", msg);
    if (assetStatus) assetStatus.textContent = `失败：${msg}`;
    if (prev) {
      prev.innerHTML = "";
      prev.appendChild(el("div", "muted small", `无法加载资产：${msg}`));
      prev.appendChild(el("div", "cpTiny", "如提示 401/403，请先重新登录 / 检查 ADMIN_TOKEN 是否配置。"));
    }
    throw e;
  }
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
  initGlobalAutoGrow();
  const jsStatus = $("jsStatus");
  if (jsStatus) jsStatus.textContent = "已初始化";

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
        state.clubProfile = ensureV2(parsed.value);
        syncProfileFormFromJson(state.clubProfile);
      } else {
        state.clubProfile = ensureV2(state.clubProfile);
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

