/* ============================================================================
   HEALIX Console — shared script (module 16, prototype v0.1)
   Global `HX`: shell, navigation, role switcher, command palette, as-of control,
   bitemporal data loader, keyboard map, theme, inspector, charts, and the
   shared component helpers used by every prototype page.

   Plain ES2020, no build step, no dependencies. Works from file:// — data is
   loaded from an inline <script type="application/json" id="hx-data-NAME"> block
   when present, otherwise fetched from data/NAME.json. Nothing beyond viewer
   preferences (role, theme, density, inspector width, saved views, recent ids)
   is persisted; no result payloads ever touch localStorage.
   ============================================================================ */
(function (global) {
  "use strict";

  /* ------------------------------------------------------------------ utils */
  const doc = document;
  const $ = (sel, root) => (root || doc).querySelector(sel);
  const $$ = (sel, root) => Array.prototype.slice.call((root || doc).querySelectorAll(sel));
  function el(tag, attrs, children) {
    const node = doc.createElement(tag);
    if (attrs) {
      for (const k in attrs) {
        const v = attrs[k];
        if (v == null || v === false) continue;
        if (k === "class") node.className = v;
        else if (k === "html") node.innerHTML = v;
        else if (k === "text") node.textContent = v;
        else if (k === "dataset") { for (const dk in v) node.dataset[dk] = v[dk]; }
        else if (k.slice(0, 2) === "on" && typeof v === "function") node.addEventListener(k.slice(2), v);
        else if (v === true) node.setAttribute(k, "");
        else node.setAttribute(k, v);
      }
    }
    append(node, children);
    return node;
  }
  function append(node, children) {
    if (children == null) return node;
    (Array.isArray(children) ? children : [children]).forEach((c) => {
      if (c == null || c === false) return;
      node.appendChild(typeof c === "string" || typeof c === "number" ? doc.createTextNode(String(c)) : c);
    });
    return node;
  }
  function clear(node) { while (node && node.firstChild) node.removeChild(node.firstChild); return node; }
  const esc = (s) => String(s == null ? "" : s).replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
  const titleCase = (s) => String(s || "").replace(/[._-]/g, " ").replace(/\b\w/g, (m) => m.toUpperCase());

  /* safe localStorage — never throws, never stores payloads */
  const store = {
    get(k, d) { try { const v = localStorage.getItem("hx." + k); return v == null ? d : JSON.parse(v); } catch (e) { return d; } },
    set(k, v) { try { localStorage.setItem("hx." + k, JSON.stringify(v)); } catch (e) { /* private mode / blocked */ } },
    del(k) { try { localStorage.removeItem("hx." + k); } catch (e) {} },
  };

  /* tiny event bus per channel */
  function emitter() {
    const subs = [];
    return { on(fn) { subs.push(fn); return () => { const i = subs.indexOf(fn); if (i >= 0) subs.splice(i, 1); }; }, emit(x) { subs.slice().forEach((fn) => { try { fn(x); } catch (e) { console.warn(e); } }); } };
  }

  /* shared modal focus management (WCAG 2.4.3 / 2.1.2 for overlay dialogs) ---
     Every overlay (palette, confirm, comment, break-glass, keymap, as-of) runs
     through this: it records the trigger, makes the shell behind inert + aria-
     hidden, traps Tab/Shift-Tab inside the dialog, and — by wrapping the node's
     own .remove() — restores focus to the trigger whenever the dialog closes
     (button, Esc, backdrop click, or programmatic close), with no edits needed
     at the many existing overlay.remove() call sites. */
  function focusablesIn(root) {
    return $$('a[href], button:not([disabled]), input:not([disabled]), select:not([disabled]), textarea:not([disabled]), [tabindex]:not([tabindex="-1"])', root)
      .filter((n) => !n.hasAttribute("hidden") && (n.offsetWidth || n.offsetHeight || n === doc.activeElement));
  }
  function modalize(overlay) {
    const prev = doc.activeElement;
    const shell = $(".hx-shell");
    if (shell) { shell.setAttribute("aria-hidden", "true"); try { shell.inert = true; } catch (e) {} shell.setAttribute("inert", ""); }
    const onKey = (e) => {
      if (e.key !== "Tab") return;
      const f = focusablesIn(overlay); if (!f.length) { e.preventDefault(); return; }
      const first = f[0], last = f[f.length - 1], here = doc.activeElement;
      if (e.shiftKey && (here === first || !overlay.contains(here))) { e.preventDefault(); last.focus(); }
      else if (!e.shiftKey && (here === last || !overlay.contains(here))) { e.preventDefault(); first.focus(); }
    };
    overlay.addEventListener("keydown", onKey, true);
    const origRemove = overlay.remove.bind(overlay);
    overlay.remove = function () {
      overlay.removeEventListener("keydown", onKey, true);
      if (shell) { shell.removeAttribute("aria-hidden"); try { shell.inert = false; } catch (e) {} shell.removeAttribute("inert"); }
      origRemove();
      try { if (prev && prev.focus) prev.focus(); } catch (e) {}
    };
    return overlay;
  }

  /* ------------------------------------------------------------------ icons */
  // Minimal inline icon set (stroke, 24-grid). Returns an <svg> element.
  const ICONS = {
    home: "M3 11l9-8 9 8M5 10v10h5v-6h4v6h5V10",
    inbox: "M4 13h4l2 3h4l2-3h4M4 13V5h16v8M4 13v6h16v-6",
    review: "M5 4h11l4 4v12H5zM15 4v4h4M8 13h8M8 17h5",
    ontology: "M12 3v4M6 21v-4M18 21v-4M12 7a2 2 0 100 4 2 2 0 000-4zM6 17a2 2 0 100-4 2 2 0 000 4zM18 17a2 2 0 100-4 2 2 0 000 4zM11 9l-4 2M13 9l4 2",
    sources: "M4 6a8 3 0 0016 0 8 3 0 00-16 0zM4 6v12a8 3 0 0016 0V6M4 12a8 3 0 0016 0",
    quality: "M12 3l8 4v5c0 5-3.5 8-8 9-4.5-1-8-4-8-9V7zM9 12l2 2 4-4",
    agents: "M12 3l2.5 5 5.5.8-4 3.9.9 5.5L12 15.8 7.1 18.2l.9-5.5-4-3.9L9.5 8z",
    governance: "M12 3l8 3v6c0 4-3.3 7-8 9-4.7-2-8-5-8-9V6zM12 8v6M9 11h6",
    lineage: "M6 6a2 2 0 100-.01M18 6a2 2 0 100-.01M12 18a2 2 0 100-.01M7.5 7l3 8M16.5 7l-3 8",
    clock: "M12 7v5l3 2M12 3a9 9 0 100 18 9 9 0 000-18z",
    bell: "M6 16V10a6 6 0 0112 0v6l2 2H4zM10 20a2 2 0 004 0",
    sun: "M12 7a5 5 0 100 10 5 5 0 000-10zM12 1v3M12 20v3M4 12H1M23 12h-3M5 5l2 2M17 17l2 2M19 5l-2 2M7 17l-2 2",
    moon: "M20 14a8 8 0 01-10-10 8 8 0 1010 10z",
    help: "M9 9a3 3 0 115 2c-1 1-2 1-2 3M12 17h.01M12 3a9 9 0 100 18 9 9 0 000-18z",
    user: "M12 12a4 4 0 100-8 4 4 0 000 8zM4 20a8 8 0 0116 0",
    search: "M11 4a7 7 0 100 14 7 7 0 000-14zM20 20l-4-4",
    chevron: "M9 6l6 6-6 6",
    rail: "M4 6h16M4 12h16M4 18h16",
    check: "M5 12l4 4 10-10",
    warn: "M12 4l9 16H3zM12 10v4M12 17h.01",
    danger: "M12 3a9 9 0 100 18 9 9 0 000-18zM12 8v4M12 16h.01",
    info: "M12 3a9 9 0 100 18 9 9 0 000-18zM12 11v5M12 8h.01",
    lock: "M6 10V8a6 6 0 0112 0v2M5 10h14v10H5zM12 14v3",
    copy: "M9 9h11v11H9zM5 15H4V4h11v1",
    close: "M6 6l12 12M18 6L6 18",
    undo: "M9 7L4 12l5 5M4 12h11a5 5 0 010 10",
    watch: "M2 12s3.5-7 10-7 10 7 10 7-3.5 7-10 7S2 12 2 12zM12 9a3 3 0 100 6 3 3 0 000-6z",
    play: "M6 4l14 8-14 8z",
  };
  function icon(name, cls) {
    const path = ICONS[name] || ICONS.info;
    const svg = doc.createElementNS("http://www.w3.org/2000/svg", "svg");
    svg.setAttribute("class", "hx-icon" + (cls ? " " + cls : ""));
    svg.setAttribute("viewBox", "0 0 24 24");
    svg.setAttribute("aria-hidden", "true");
    const p = doc.createElementNS("http://www.w3.org/2000/svg", "path");
    p.setAttribute("d", path);
    svg.appendChild(p);
    return svg;
  }
  function svgEl(tag, attrs) { const n = doc.createElementNS("http://www.w3.org/2000/svg", tag); for (const k in attrs) n.setAttribute(k, attrs[k]); return n; }

  /* ------------------------------------------------------------------ HX */
  const HX = {};
  HX.el = el; HX.icon = icon; HX._ = { $, $$, append, clear, esc };
  HX.modal = { trap: modalize };
  // Explicit build/data-source flag — the single source of truth for the
  // "synthetic data" claim. A prototype/synthetic build is true; a build wired to
  // live data sets this false so the synthetic ribbon never renders over real PHI.
  HX.isSyntheticBuild = true;

  /* ---- navigation areas (§2.2) --------------------------------------- */
  HX.AREAS = [
    { id: "home", label: "Home", route: "/", page: "index.html", key: "h", icon: "home" },
    { id: "inbox", label: "Inbox", route: "/inbox", page: "index.html", key: "i", icon: "inbox" },
    { id: "cr", label: "Review", route: "/cr", page: "change-requests.html", key: "c", icon: "review" },
    { id: "ontology", label: "Ontology", route: "/ontology", page: "ontology-workbench.html", key: "o", icon: "ontology" },
    { id: "sources", label: "Sources & Flow", route: "/sources", page: "data-catalog.html", key: "s", icon: "sources" },
    { id: "quality", label: "Quality & Warehouse", route: "/dq", page: "dq-console.html", key: "q", icon: "quality" },
    { id: "agents", label: "Agents & Operations", route: "/agents", page: "agent-ops.html", key: "a", icon: "agents" },
    { id: "gov", label: "Governance", route: "/gov", page: "settings-governance.html", key: "v", icon: "governance" },
  ];

  /* ---- canonical route → prototype file map (§6, §2.3) --------------- */
  // Each entry: a regex-free prefix match to a page file; the full canonical
  // route is preserved in the target page's hash so the object page can render it.
  const ROUTE_MAP = [
    [/^\/$/, "index.html"],
    [/^\/w\//, "index.html"],
    [/^\/inbox/, "index.html"],
    [/^\/(cr|release)\b/, "change-requests.html"],
    [/^\/(ontology|scheme|concept|relation|mapping|class|property|axiom|shape|cq|graph)\b/, null], // decided below
    [/^\/(ontology|scheme|facet|relation|class|property|axiom|shape|cq)\b/, "ontology-workbench.html"],
    [/^\/concept\b/, "concept.html"],
    [/^\/(sources?|element|mappings?|mapspec|profiles?|valueset|conceptmap)\b/, "data-catalog.html"],
    [/^\/(dq|scorecard|rule|issue|exception|remediation|quarantine)\b/, "dq-console.html"],
    [/^\/(edw|table|column|migration)\b/, "data-catalog.html"],
    // topic/connector/processor are interop objects — keep them off the catalog
    // regex above so these resolve to the page that actually renders them.
    [/^\/(topic|connector|processor)\b/, "interop-monitor.html"],
    [/^\/(ops|incident|playbook)\b/, "interop-monitor.html"],
    [/^\/(agents?|run|evaluation|canary)\b/, "agent-ops.html"],
    [/^\/(gov|steward|audit)\b/, "settings-governance.html"],
    [/^\/(kpi|exec)\b/, "executive.html"],
    [/^\/lineage\b/, "lineage-explorer.html"],
    [/^\/id\//, null],
  ];
  function routeToPage(route) {
    if (route.indexOf("/concept/") === 0 || route === "/concept") return "concept.html";
    for (const [re, page] of ROUTE_MAP) { if (page && re.test(route)) return page; }
    return "index.html";
  }

  HX.nav = function nav(route, params) {
    // Universal resolver (UX-IA-002): a /id/{anything} route is a 302, not a
    // destination. Resolve it to the canonical object route before navigating so
    // every /id/ link lands on the object page (carrying replacedBy for merged
    // ids) instead of dead-ending on Home.
    if (route && route.indexOf("/id/") === 0) {
      const r = HX.nav.resolve(decodeURIComponent(route.slice(4)));
      if (r && r.route && r.route.indexOf("/id/") !== 0) {
        route = r.route;
        if (r.replacedBy) {
          params = Object.assign({}, params, {
            replacedBy: r.replacedBy.survivor || r.replacedBy.id || r.replacedBy,
            replacedByCr: r.replacedBy.cr || undefined,
          });
        }
      }
    }
    const page = routeToPage(route);
    // preserve temporal + env context
    const carry = HX.asof.get();
    const q = Object.assign({}, params);
    if (carry.asOf) q.asOf = carry.asOf;
    if (carry.asKnownAt) q.asKnownAt = carry.asKnownAt;
    if (HX.env.get() !== "prod") q.env = HX.env.get();
    const qs = Object.keys(q).filter((k) => q[k] != null).map((k) => k + "=" + encodeURIComponent(q[k])).join("&");
    const hash = "#" + route + (qs ? "?" + qs : "");
    rememberRecent(route);
    const here = location.pathname.split("/").pop();
    if (here === page) { location.hash = hash.slice(1).indexOf(route) === 0 ? hash : hash; HX._applyHashRoute(); }
    else location.href = page + hash;
    return hash;
  };
  // Seeded so the canonical worked examples resolve synchronously even before
  // the data-driven index (buildResolverIndex) has finished loading.
  const STATIC_RESOLVE = {
    "72^INPATIENT": "/concept/encounter-class/inpatient",
    "cs72 INPATIENT": "/concept/encounter-class/inpatient",
    "cerner:72^INPATIENT": "/concept/encounter-class/inpatient",
    "71^309308": "/concept/encounter-class/observation-unit",
    "cs71 309308": "/concept/encounter-class/observation-unit",
    "cs72 309308": "/concept/encounter-class/observation-unit",
  };
  HX.nav.resolve = function resolve(anything) {
    const s = String(anything || "").trim();
    const data = HX._resolverIndex || {};
    // 0. seeded worked examples
    if (STATIC_RESOLVE[s]) return { route: STATIC_RESOLVE[s] };
    // 1. merged/deprecated?
    if (data.merged && data.merged[s]) return { route: data.merged[s].route, replacedBy: data.merged[s] };
    // 2. exact id families
    if (/^CR-/.test(s)) return { route: "/cr/" + s };
    if (/^DQR-/.test(s)) return { route: "/rule/" + s };
    if (/^ISS-/.test(s)) return { route: "/issue/" + s };
    if (/^PBK-/.test(s)) return { route: "/playbook/" + s };
    if (/^INC-/.test(s)) return { route: "/incident/" + s };
    if (/^CQ-/.test(s)) return { route: "/cq/" + s };
    if (/^S\d+$/.test(s)) return { route: "/ops/slo/" + s };
    if (/^agent\./.test(s)) return { route: "/agent/" + s };
    // 3. Cerner CODE_SET^CDF (e.g. 72^INPATIENT) or "cs72 309308"
    let m = s.match(/^(\d+)\^([A-Za-z0-9_]+)$/) || s.match(/^cs(\d+)\s+(\d+)$/i);
    if (m && data.codes && data.codes[s]) return { route: data.codes[s] };
    if (m) { const hit = data.codesByPair && data.codesByPair[(m[1]) + "^" + (m[2])]; if (hit) return { route: hit }; }
    // 4. topic name
    if (/^hc\./.test(s)) return { route: "/topic/" + s };
    // 5. EDW FQN edw_gN_x.table(.col)
    if (/^edw_g\d/.test(s)) { const parts = s.split("."); if (parts.length >= 3) return { route: "/column/" + parts[0] + "/" + parts[1] + "/" + parts.slice(2).join(".") }; return { route: "/table/" + parts[0] + "/" + (parts[1] || "") }; }
    // 6. full IRI
    if (/^https?:\/\//.test(s)) { const path = s.replace(/^https?:\/\/[^/]+\//, ""); if (path.indexOf("concept/") === 0) return { route: "/concept/" + path.slice("concept/".length) }; return { route: "/id/" + encodeURIComponent(s) }; }
    // 7. known resolver index entry
    if (data.ids && data.ids[s]) return { route: data.ids[s] };
    return { route: "/id/" + encodeURIComponent(s), unknown: true };
  };

  function rememberRecent(route) {
    const r = store.get("recent", []);
    const next = [route].concat(r.filter((x) => x !== route)).slice(0, 20);
    store.set("recent", next);
  }

  /* ---- roles & PDP verb stub (§3) ------------------------------------ */
  HX.ROLES = [
    { id: "role.ontologist", label: "Ontologist / Taxonomist", home: "/w/ontologist" },
    { id: "role.steward_g2", label: "Data Steward (G2 Clinical)", home: "/w/steward" },
    { id: "role.integration_engineer", label: "Integration Engineer", home: "/w/integration" },
    { id: "role.dw_architect", label: "Data Warehouse Architect", home: "/w/architect" },
    { id: "role.analyst", label: "Analyst / Data Scientist", home: "/w/analyst" },
    { id: "role.privacy_officer", label: "Privacy & Security Officer", home: "/w/privacy" },
    { id: "role.platform_sre", label: "Platform SRE", home: "/w/sre" },
    { id: "role.agent_operator", label: "Agent Operator", home: "/w/operator" },
    { id: "role.executive", label: "Executive Sponsor", home: "/w/exec" },
  ];
  const roleBus = emitter();
  // The REAL principal's permissions never change with View-as. The prototype
  // fixes the principal as a multi-role power user so every verb demos; View-as
  // only changes rendering (home, defaults, emphasis).
  const PRINCIPAL = "role.ontologist";
  HX.role = {
    list() { return HX.ROLES.slice(); },
    get() { return store.get("role", PRINCIPAL); },
    set(roleId) {
      if (!HX.ROLES.some((r) => r.id === roleId)) return;
      const prev = HX.role.get();
      store.set("role", roleId);
      // every View-as switch is audited (§3.1)
      if (prev !== roleId) HX.audit.write({ type: "VIEW_AS_SWITCH", principal: PRINCIPAL, from: prev, to: roleId, at: new Date().toISOString() });
      roleBus.emit(roleId);
      renderViewAsNote();
      renderRail();
    },
    on(cb) { return roleBus.on(cb); },
    find(id) { return HX.ROLES.filter((r) => r.id === id)[0]; },
    principal() { return PRINCIPAL; },
    /* PDP verb stub: verbs come from the real principal, NOT the viewed role */
    verbs(objectType, object) {
      const p = PRINCIPAL; // never widened by View-as
      const base = ["watch", "copy_url", "set_as_of", "compare"];
      const map = {
        cr: ["approve", "approve_with_comment", "request_revision", "reject", "defer", "escalate"],
        issue: ["acknowledge", "assign", "request_source_fix", "accept_with_exception"],
        rule: ["tune", "simulate"],
        concept: ["propose_change", "set_broader", "watch"],
        scheme: ["promote", "export"],
        agent: ["trip_kill_switch", "adjust_threshold"],
      };
      let verbs = base.concat(map[objectType] || []);
      // Veto is the Privacy Officer's disposing action on a phi_affecting CR. The
      // demo principal is a multi-role power user (see note above), and View-as is
      // a documented rendering filter — so surface veto when the object is
      // phi_affecting AND the principal OR the currently-viewed role is privacy.
      // Casting/clearing a veto still writes an AuditEvent on the page.
      const viewed = HX.role.get();
      if (objectType === "cr" && object && object.riskClass === "phi_affecting" &&
          (p === "role.privacy_officer" || viewed === "role.privacy_officer")) verbs.push("veto");
      // de-dupe
      return verbs.filter((v, i) => verbs.indexOf(v) === i);
    },
  };

  /* ---- environment (§2.1) -------------------------------------------- */
  const ENVS = ["dev", "test", "uat", "prod"];
  let firstProdWrite = false;
  HX.env = {
    get() { return HX._env || store.get("env", "uat"); },
    set(env) { if (ENVS.indexOf(env) < 0) return; HX._env = env; store.set("env", env); applyEnvChrome(); },
    list() { return ENVS.slice(); },
    async confirmWrite() {
      if (HX.env.get() === "prod" && !firstProdWrite) {
        const ok = await HX.confirm({ title: "First write in PRODUCTION this session", body: "You are about to perform a write action in the prod environment. Changes here affect live governed artefacts.", typed: "PROD", danger: true });
        if (ok) firstProdWrite = true;
        return ok;
      }
      return true;
    },
  };

  /* ---- facility / domain scope (Fortune-100 multi-facility reality) --- */
  // A Fortune-100 health system runs many facilities, several Cerner domains and
  // M&A/legacy systems. The scope narrows queues, scorecards and audit to an
  // org slice. "all" is the enterprise view (default). The typed-filter grammar
  // also carries facility:/domain:/tenant:/source-system: so the same dimension
  // is honoured by palette, lists and saved views.
  const FACILITIES = [
    { id: "all", label: "All facilities (enterprise)" },
    { id: "FAC-MAIN", label: "Metro Main Hospital", domain: "cerner-prod" },
    { id: "FAC-ONC", label: "Metro Oncology Center (acquired)", domain: "cerner-onc" },
    { id: "FAC-STF", label: "St. Francis (M&A legacy)", domain: "meditech-legacy" },
    { id: "FAC-FIN", label: "Shared services (finance/HR/supply)", domain: "oracle-fusion" },
  ];
  const scopeBus = emitter();
  HX.scope = {
    list() { return FACILITIES.slice(); },
    get() { return HX._scope || store.get("scope", "all"); },
    set(id) {
      if (!FACILITIES.some((f) => f.id === id)) return;
      HX._scope = id; store.set("scope", id);
      HX.audit.write({ type: "SCOPE_SWITCH", principal: HX.role.principal(), to: id });
      applyScopeChrome(); scopeBus.emit(id);
    },
    find(id) { return FACILITIES.filter((f) => f.id === id)[0]; },
    /* filter fragment pages can fold into a typed query, e.g. "facility:FAC-ONC" */
    filter() { const s = HX.scope.get(); return s && s !== "all" ? "facility:" + s : ""; },
    on(cb) { return scopeBus.on(cb); },
  };

  /* ---- data loader (§6) ---------------------------------------------- */
  const dataCache = Object.create(null);
  HX.data = {
    load(name) {
      if (dataCache[name]) return dataCache[name];
      const inline = doc.getElementById("hx-data-" + name);
      let p;
      if (inline) {
        p = new Promise((res, rej) => { try { res(JSON.parse(inline.textContent)); } catch (e) { rej(e); } });
      } else {
        p = fetch("data/" + name + ".json").then((r) => { if (!r.ok) throw new Error("HTTP " + r.status); return r.json(); });
      }
      p = p.catch((e) => {
        // file:// fetch can be blocked; try the inline block as a last resort then re-throw with guidance
        const blk = doc.getElementById("hx-data-" + name);
        if (blk) { try { return JSON.parse(blk.textContent); } catch (_) {} }
        console.warn("HX.data.load('" + name + "') failed:", e.message, "- embed <script type=\"application/json\" id=\"hx-data-" + name + "\"> for file:// use.");
        return {};
      });
      dataCache[name] = p;
      return p;
    },
    /* typed filter grammar: "type:cr state:evaluated risk:high group:G2 ..." */
    parseFilter(str) {
      const out = {};
      String(str || "").trim().split(/\s+/).forEach((tok) => {
        if (!tok) return;
        const i = tok.indexOf(":");
        if (i > 0) { const k = tok.slice(0, i); let v = tok.slice(i + 1); out[k] = v; }
        else { out._q = (out._q ? out._q + " " : "") + tok; }
      });
      return out;
    },
    query(name, filterString) {
      const f = HX.data.parseFilter(filterString);
      const scope = HX.scope.get();
      return HX.data.load(name).then((d) => {
        const key = Array.isArray(d) ? null : Object.keys(d).filter((k) => Array.isArray(d[k]))[0];
        const rows = Array.isArray(d) ? d : (key ? d[key] : []);
        return rows.filter((row) => matchFilter(row, f) && matchScope(row, scope));
      });
    },
    /* bitemporal filter over a records[] of versions */
    asOf(records, asOf, asKnownAt) {
      const vt = asOf ? Date.parse(asOf) : Date.now();
      const tt = asKnownAt ? Date.parse(asKnownAt) : Date.now();
      const current = [];
      const notYetAsserted = [];
      (records || []).forEach((r) => {
        const vf = r.validFrom ? Date.parse(r.validFrom) : -Infinity;
        const vto = r.validTo ? Date.parse(r.validTo) : Infinity;
        const ra = r.recordedAt ? Date.parse(r.recordedAt) : -Infinity;
        const sa = r.supersededAt ? Date.parse(r.supersededAt) : Infinity;
        const validNow = vf <= vt && vt < vto;
        const knownNow = ra <= tt && tt < sa;
        if (validNow && knownNow) current.push(r);
        else if (ra > tt) notYetAsserted.push(r);
      });
      return { current, notYetAsserted };
    },
  };
  // Collection-scope tokens: `type:cr` (or type:issue, type:concept …) names the
  // COLLECTION, not a row field. Inside the matching collection every row is of
  // that type, so the token is a no-op rather than a filter that empties the set.
  const COLLECTION_TYPES = ["cr", "issue", "rule", "concept", "scheme", "agent", "table", "topic", "mapping", "source", "incident", "playbook"];
  function matchFilter(row, f) {
    for (const k in f) {
      if (k === "_q") { const hay = JSON.stringify(row).toLowerCase(); if (hay.indexOf(f._q.toLowerCase()) < 0) return false; continue; }
      const want = f[k];
      const wl = String(want).toLowerCase();
      // collection-scope type token → pass (never excludes rows of its collection)
      if (k === "type" && COLLECTION_TYPES.indexOf(wl) >= 0) continue;
      const rv = deepGet(row, k);
      if (rv == null) return false;
      const sval = String(rv).toLowerCase();
      // age: compares by day-age against an ISO date — never parseFloat the date
      // ("2026-…" would read as 2026), so keep it on the day-based comparator.
      if (k === "age") {
        if (wl[0] === ">") { if (!ageGt(sval, wl)) return false; }
        else if (wl[0] === "<") { if (!ageLt(sval, wl)) return false; }
        else if (sval.indexOf(wl) < 0) return false;
        continue;
      }
      if (wl[0] === ">") { if (!(parseFloat(sval) > parseFloat(wl.slice(1)))) return false; }
      else if (wl[0] === "<") { if (!(parseFloat(sval) < parseFloat(wl.slice(1)))) return false; }
      else if (sval.indexOf(wl) < 0) return false;
    }
    return true;
  }
  // facility / domain / tenant scope match: rows that carry no facility field are
  // never hidden (e.g. concepts), so an enterprise scope only narrows the
  // collections that actually have an org dimension (sources, CRs, issues).
  function matchScope(row, scope) {
    if (!scope || scope === "all") return true;
    const fac = deepGet(row, "facility");
    if (fac == null) return true;
    return String(fac).toLowerCase().indexOf(String(scope).toLowerCase()) >= 0;
  }
  function deepGet(row, key) {
    const aliases = { type: ["type", "subtype"], state: ["state"], risk: ["riskClass", "risk", "severity"], group: ["group"], proposer: ["proposer.agent", "proposer"], gate: ["gatesRequired"], stage: ["stage"], binding: ["binding"], severity: ["severity"], plane: ["plane"], age: ["createdAt", "age"], facility: ["facility", "facilityId"], domain: ["domain", "sourceDomain"], tenant: ["tenant", "facility"], "source-system": ["sourceSystem", "source"] };
    const paths = aliases[key] || [key];
    for (const p of paths) {
      let v = row;
      for (const part of p.split(".")) { v = v == null ? v : v[part]; }
      if (v != null) return Array.isArray(v) ? v.join(" ") : v;
    }
    return null;
  }
  function ageGt(iso, expr) { // age:>5d
    const m = String(expr).match(/>(\d+)d/); if (!m) return false;
    const t = Date.parse(iso); if (isNaN(t)) return false;
    return (Date.now() - t) / 86400000 > parseInt(m[1], 10);
  }
  function ageLt(iso, expr) { // age:<5d
    const m = String(expr).match(/<(\d+)d/); if (!m) return false;
    const t = Date.parse(iso); if (isNaN(t)) return false;
    return (Date.now() - t) / 86400000 < parseInt(m[1], 10);
  }

  /* ---- as-of control (§2.5, §5.2 temporal) --------------------------- */
  const MODES = ["Current", "As-of", "As-known", "Bitemporal point", "Full history"];
  const asofBus = emitter();
  let asofState = { asOf: null, asKnownAt: null, linked: true };
  HX.asof = {
    get() { return Object.assign({}, asofState, { mode: HX.asof.mode() }); },
    set(s) {
      if (!s) return;
      if ("asOf" in s) asofState.asOf = s.asOf || null;
      if ("asKnownAt" in s) asofState.asKnownAt = s.asKnownAt || null;
      if ("linked" in s) asofState.linked = !!s.linked;
      if (asofState.linked && s.asOf && !("asKnownAt" in s)) asofState.asKnownAt = s.asOf;
      writeHashParams();
      renderTimeBanner();
      asofBus.emit(HX.asof.get());
      HX.live(asofBannerText(), "polite");
    },
    clear() { asofState = { asOf: null, asKnownAt: null, linked: true }; writeHashParams(); renderTimeBanner(); asofBus.emit(HX.asof.get()); },
    mode() {
      const a = asofState.asOf, k = asofState.asKnownAt;
      if (!a && !k) return "Current";
      if (a && k) return "Bitemporal point";
      if (a && !k) return "As-of";
      if (!a && k) return "As-known";
      return "Full history";
    },
    on(cb) { return asofBus.on(cb); },
    open() { openAsOfSheet(); },
    close() { const o = $(".hx-overlay[data-asof]"); if (o) o.remove(); },
  };
  function asofBannerText() {
    const s = HX.asof.get();
    if (s.mode === "Current") return "Viewing now.";
    const parts = [];
    if (s.asOf) parts.push("valid at " + HX.format.time(s.asOf, "valid"));
    if (s.asKnownAt) parts.push("as known at " + HX.format.time(s.asKnownAt, "known"));
    return "Viewing " + parts.join(" · ") + " · mode: " + s.mode;
  }

  /* ---- keyboard map (§8) --------------------------------------------- */
  const KEYMAP = {
    Global: { "Ctrl/Cmd-K": "Command palette", "/": "Focus search", "?": "This keyboard map", "[": "Toggle rail (on lists: prev item)", "]": "Toggle inspector (on lists: next item)", "t": "As-of control", "Shift-T": "Return to now", "c": "Compare versions", "y": "Copy IRI", ".": "Act on current row / open SPARQL", "Esc": "Close non-destructive sheet" },
    "Go to": { "g h": "Home", "g i": "Inbox", "g c": "Review (CRs)", "g o": "Ontology", "g s": "Sources & Flow", "g q": "Quality & Warehouse", "g a": "Agents & Operations", "g v": "Governance", "g l": "Lineage" },
    Lists: { "j / k": "Move down / up", "Enter / o": "Open", "Shift-Enter": "Peek in inspector", "Space / x": "Select", "Shift-x": "Select group", "Ctrl-Enter": "Copy URL" },
    "Object page": { "e": "Evidence tab", "h": "History tab", "p": "Provenance tab (object pages)", "i": "Impact tab", "1–8": "Canonical tabs" },
    Review: { "a": "Approve", "A": "Approve with comment", "r": "Reject", "v": "Request revision", "x": "Defer / snooze", "V": "Veto (privacy)", "n / p": "Next / prev diff change (within a CR's diff)", "] / [": "Next / prev item" },
    Tree: { "←/→": "Collapse / expand", "Shift-↑/↓": "Range", "b": "Set broader (CR draft)", "Move to…": "Keyboard reparent" },
    "As-of control": { "←/→": "Step by zoom", "Shift": "×10 step", "Home/End": "Jump to ends", "L": "Toggle link", "Shift-drag": "Unlink" },
    Canvas: { "Tab": "Cycle nodes", "Enter": "Open node", "Space": "Expand", "arrows": "Follow edges", "v": "Toggle list twin" },
  };
  const keyBindings = {};
  let keysEnabled = store.get("keysEnabled", true);
  HX.keys = {
    bind(map) { Object.assign(keyBindings, map); },
    enabled(b) { if (b === undefined) return keysEnabled; keysEnabled = !!b; store.set("keysEnabled", keysEnabled); renderShortcutBanner(); },
    map() { return KEYMAP; },
    open() { openKeymap(); },
  };
  let chordPrefix = null, chordTimer = null;
  function inField(t) { return t && (t.tagName === "INPUT" || t.tagName === "TEXTAREA" || t.isContentEditable || t.tagName === "SELECT"); }
  function onKeyDown(e) {
    const meta = e.ctrlKey || e.metaKey;
    if (meta && (e.key === "k" || e.key === "K")) { e.preventDefault(); HX.palette.open(); return; }
    if (inField(e.target)) { if (e.key === "Escape") e.target.blur(); return; }
    if (!keysEnabled) return;
    if (e.key === "/") { e.preventDefault(); const s = $(".hx-topbar__search"); if (s) HX.palette.open("find"); return; }
    if (e.key === "?") { e.preventDefault(); openKeymap(); return; }
    if (e.key === "Escape") { closeTopOverlay(); return; }
    // chords g + letter
    if (chordPrefix === "g") {
      clearTimeout(chordTimer); chordPrefix = null;
      const area = HX.AREAS.filter((a) => a.key === e.key)[0];
      if (area) { e.preventDefault(); HX.nav(area.route); return; }
      if (e.key === "l") { e.preventDefault(); HX.nav("/lineage"); return; }
      return;
    }
    if (e.key === "g") { chordPrefix = "g"; chordTimer = setTimeout(() => { chordPrefix = null; }, 900); return; }
    // [ and ] toggle rail / inspector by default, but YIELD to a page that has
    // registered its own binding (e.g. Review's prev/next item), so the keymap's
    // documented "] / [ = Next / prev item" actually works on list/review pages.
    if (e.key === "[") { if (keyBindings["["]) { const r = keyBindings["["](e); if (r !== false) e.preventDefault(); } else toggleRail(); return; }
    if (e.key === "]") { if (keyBindings["]"]) { const r = keyBindings["]"](e); if (r !== false) e.preventDefault(); } else HX.inspector.toggle(); return; }
    if (e.key === "t") { HX.asof.open(); return; }
    if (e.key === "T" && e.shiftKey) { HX.asof.clear(); HX.toast("Returned to now", { kind: "ok" }); return; }
    // page-registered single keys
    if (keyBindings[e.key]) { const r = keyBindings[e.key](e); if (r !== false) e.preventDefault(); }
  }

  /* ---- inspector (§2.1) ---------------------------------------------- */
  HX.inspector = {
    el() { return $(".hx-inspector__body"); },
    titleEl() { return $(".hx-inspector__head h2"); },
    open(content, title) {
      doc.documentElement.classList.remove("hx-inspector--closed");
      const shell = $(".hx-shell"); if (shell) shell.classList.remove("hx-inspector--closed");
      const body = HX.inspector.el(); if (!body) return;
      if (title) HX.inspector.titleEl().textContent = title;
      clear(body);
      if (typeof content === "string") {
        // treat as a route → render a peek card
        body.appendChild(el("div", { class: "hx-card", html: "<div class='hx-muted hx-caption'>Peek</div><div class='hx-mono'>" + esc(content) + "</div>" }));
      } else if (content) body.appendChild(content);
    },
    close() { const shell = $(".hx-shell"); if (shell) shell.classList.add("hx-inspector--closed"); },
    toggle() { const shell = $(".hx-shell"); if (shell) shell.classList.toggle("hx-inspector--closed"); },
    peek(route) { HX.inspector.open(route, "Peek"); },
  };

  /* ---- theme & density ----------------------------------------------- */
  HX.theme = {
    get() { return store.get("theme", "system"); },
    set(t) {
      store.set("theme", t);
      if (t === "system") doc.documentElement.removeAttribute("data-theme");
      else doc.documentElement.setAttribute("data-theme", t);
      const btn = $(".hx-theme-toggle"); if (btn) refreshThemeBtn(btn);
    },
  };
  HX.density = { get() { return store.get("density", "comfortable"); }, set(d) { store.set("density", d); if (d === "compact") doc.documentElement.setAttribute("data-density", "compact"); else doc.documentElement.removeAttribute("data-density"); } };

  /* ---- time formatting (§5.1) ---------------------------------------- */
  HX.format = {
    time(iso, which) {
      if (!iso) return "—";
      const t = Date.parse(iso);
      if (isNaN(t)) return String(iso);
      const d = new Date(t);
      const pad = (n) => String(n).padStart(2, "0");
      const stamp = d.getUTCFullYear() + "-" + pad(d.getUTCMonth() + 1) + "-" + pad(d.getUTCDate()) + "T" + pad(d.getUTCHours()) + ":" + pad(d.getUTCMinutes()) + "Z";
      const rel = relTime(t);
      const label = which ? " [" + which + "]" : "";
      return stamp + " · " + rel + label;
    },
    rel(iso) { return relTime(Date.parse(iso)); },
    num(n, d) { return Number(n).toLocaleString(undefined, { maximumFractionDigits: d == null ? 1 : d }); },
  };
  function relTime(t) {
    if (isNaN(t)) return "";
    const s = (t - Date.now()) / 1000; const a = Math.abs(s);
    const fmt = (v, u) => Math.round(v) + " " + u + (Math.round(v) === 1 ? "" : "s");
    let out;
    if (a < 60) out = fmt(a, "sec"); else if (a < 3600) out = fmt(a / 60, "min"); else if (a < 86400) out = fmt(a / 3600, "h"); else if (a < 2592000) out = fmt(a / 86400, "day"); else out = fmt(a / 2592000, "mo");
    return s < 0 ? out + " ago" : "in " + out;
  }

  /* ---- toast (§6) ----------------------------------------------------- */
  HX.toast = function toast(message, opts) {
    opts = opts || {};
    let region = $(".hx-toasts"); if (!region) { region = el("div", { class: "hx-toasts", role: "status", "aria-live": "polite" }); doc.body.appendChild(region); }
    const timeout = opts.timeout == null ? 8000 : opts.timeout;
    const t = el("div", { class: "hx-toast" + (opts.kind ? " hx-toast--" + opts.kind : "") });
    t.appendChild(el("span", { class: "hx-toast__msg", text: message }));
    let timer;
    const dismiss = () => { clearTimeout(timer); if (t.parentNode) t.parentNode.removeChild(t); };
    if (opts.undo) {
      const b = el("button", { class: "hx-btn hx-btn--ghost hx-btn--sm hx-toast__undo" }, [icon("undo"), "Undo"]);
      b.addEventListener("click", () => { try { opts.undo(); } catch (e) {} dismiss(); });
      t.appendChild(b);
    }
    const bar = el("i", { class: "hx-toast__timer" }); bar.style.animationDuration = timeout + "ms"; t.appendChild(bar);
    region.appendChild(t);
    if (timeout) timer = setTimeout(dismiss, timeout);
    return { dismiss };
  };

  /* ---- confirm sheet (§6) -------------------------------------------- */
  HX.confirm = function confirmSheet(opts) {
    opts = opts || {};
    return new Promise((resolve) => {
      const overlay = el("div", { class: "hx-overlay", role: "dialog", "aria-modal": "true", "aria-label": opts.title || "Confirm" });
      let typed = null;
      const sheet = el("div", { class: "hx-confirm-sheet" });
      sheet.appendChild(el("div", { class: "hx-sheet__head" }, [icon(opts.danger ? "danger" : "info"), el("h2", { text: opts.title || "Confirm" })]));
      if (opts.body) sheet.appendChild(el("p", { text: opts.body }));
      if (opts.consequence) sheet.appendChild(el("div", { class: "hx-sheet__consequence", text: opts.consequence }));
      let input;
      const finish = (ok) => { overlay.remove(); resolve(ok); };
      const confirmBtn = el("button", { class: "hx-btn " + (opts.danger ? "hx-btn--danger" : "hx-btn--primary") }, [opts.confirmLabel || "Confirm"]);
      if (opts.typed) {
        const f = el("div", { class: "hx-field" });
        f.appendChild(el("label", { text: "Type " + opts.typed + " to confirm" }));
        input = el("input", { type: "text", autocomplete: "off" });
        confirmBtn.setAttribute("aria-disabled", "true"); confirmBtn.disabled = true;
        input.addEventListener("input", () => { const ok = input.value === opts.typed; confirmBtn.disabled = !ok; if (ok) confirmBtn.removeAttribute("aria-disabled"); else confirmBtn.setAttribute("aria-disabled", "true"); });
        f.appendChild(input); sheet.appendChild(f);
      }
      const cancel = el("button", { class: "hx-btn" }, ["Cancel"]);
      cancel.addEventListener("click", () => finish(false));
      confirmBtn.addEventListener("click", () => { if (!confirmBtn.disabled) finish(true); });
      sheet.appendChild(el("div", { class: "hx-sheet__row" }, [cancel, confirmBtn]));
      overlay.appendChild(sheet);
      overlay.addEventListener("click", (e) => { if (e.target === overlay) finish(false); });
      overlay.addEventListener("keydown", (e) => { if (e.key === "Escape") finish(false); });
      doc.body.appendChild(overlay);
      modalize(overlay);
      (input || cancel).focus();
    });
  };

  /* ---- PHI scan stub (§6) -------------------------------------------- */
  const PHI_PATTERNS = [
    { id: "SSN", re: /\b\d{3}-\d{2}-\d{4}\b/ },
    { id: "MRN-like", re: /(?<!SYN-)(?<![\w-])[A-Z]{0,3}\d{6,}\b/ },
    { id: "DOB", re: /\b(0?[1-9]|1[0-2])[/\-](0?[1-9]|[12]\d|3[01])[/\-](19|20)\d\d\b/ },
    { id: "phone", re: /\b\d{3}[.\-\s]\d{3}[.\-\s]\d{4}\b/ },
    { id: "email", re: /\b[\w.+-]+@[\w-]+\.[\w.-]+\b/ },
    // real-looking name: a clinical title followed by a capitalised surname that is
    // not one of the synthetic markers. Case-sensitive so "Patient synthetic" is fine.
    { id: "name", re: /\b(?:Mr|Mrs|Ms|Dr|Patient|Member)\.?\s+(?!Synthetic|Example|Zz|Test)[A-Z][a-z]{2,}\b/ },
  ];
  HX.phiScan = function phiScan(text) {
    const findings = [];
    (PHI_PATTERNS).forEach((p) => { const m = String(text || "").match(p.re); if (m) findings.push({ kind: p.id, match: m[0] }); });
    return { pass: findings.length === 0, findings };
  };

  /* ---- audit log (in-memory, viewable in governance) ----------------- */
  const auditLog = [];
  HX.audit = {
    write(ev) { ev = Object.assign({ at: new Date().toISOString() }, ev); auditLog.push(ev); return ev; },
    list() { return auditLog.slice(); },
  };

  /* ---- kill switch (§4.9) -------------------------------------------- */
  const killBus = emitter();
  HX.killswitch = {
    get() { return store.get("kill", null); },
    trip(scope, reason) {
      const rec = { active: true, scope: scope, reason: reason, heldBy: HX.role.get(), since: new Date().toISOString(), privacyTriggered: /phi|privacy/.test(JSON.stringify(scope || "")) };
      store.set("kill", rec); HX.audit.write({ type: "KILL_SWITCH_TRIP", scope: scope, by: rec.heldBy }); renderKillBanner(); killBus.emit(rec); return rec;
    },
    release() { const prev = HX.killswitch.get(); store.del("kill"); HX.audit.write({ type: "KILL_SWITCH_RELEASE", scope: prev && prev.scope }); renderKillBanner(); killBus.emit(null); },
    on(cb) { return killBus.on(cb); },
  };

  /* ---- break-glass (§4.5) -------------------------------------------- */
  HX.breakglass = {
    active: null,
    open(issueId) {
      return new Promise((resolve) => {
        const overlay = el("div", { class: "hx-overlay", role: "dialog", "aria-modal": "true", "aria-label": "Break-glass access request" });
        const sheet = el("div", { class: "hx-breakglass-sheet" });
        sheet.appendChild(el("div", { class: "hx-sheet__head" }, [icon("lock"), el("h2", { text: "Break-glass: entitled remediation" })]));
        sheet.appendChild(el("p", { text: "Issue " + issueId + ". This opens a 4-hour, red-framed, audited workspace that shows synthetic record-level values. Every record you view writes an AuditEvent reviewed by the Privacy Officer within 2 business days." }));
        const reason = el("select", {}, [el("option", { value: "", text: "Select reason code…" }), el("option", { value: "PATIENT_SAFETY", text: "PATIENT_SAFETY" }), el("option", { value: "DATA_CORRECTION", text: "DATA_CORRECTION" })]);
        const rf = el("div", { class: "hx-field" }, [el("label", { text: "Reason code" }), reason]);
        const mfa = el("input", { type: "text", inputmode: "numeric", placeholder: "6-digit code", autocomplete: "off" });
        const mf = el("div", { class: "hx-field" }, [el("label", { text: "MFA re-prompt" }), mfa]);
        sheet.appendChild(el("div", { class: "hx-breakglass-sheet__warn" }, [icon("warn"), " Minimum necessary applies. Synthetic SYN- identifiers only."]));
        sheet.appendChild(rf); sheet.appendChild(mf);
        const go = el("button", { class: "hx-btn hx-btn--danger", "aria-disabled": "true" }, ["Enter break-glass"]);
        go.disabled = true;
        const check = () => { const ok = reason.value && mfa.value.length >= 4; go.disabled = !ok; if (ok) go.removeAttribute("aria-disabled"); else go.setAttribute("aria-disabled", "true"); };
        reason.addEventListener("change", check); mfa.addEventListener("input", check);
        const cancel = el("button", { class: "hx-btn" }, ["Cancel"]);
        cancel.addEventListener("click", () => { overlay.remove(); resolve(null); });
        go.addEventListener("click", () => {
          if (go.disabled) return;
          const session = { id: "BG-" + Math.random().toString(36).slice(2, 9).toUpperCase(), issue: issueId, reasonCode: reason.value, principal: HX.role.get(), startedAt: new Date().toISOString(), expiresAt: new Date(Date.now() + 4 * 3600000).toISOString(), recordsViewed: 0 };
          HX.breakglass.active = session;
          HX.audit.write({ type: "BREAK_GLASS_OPEN", session: session.id, issue: issueId, reasonCode: reason.value, by: session.principal });
          HX.live("Break-glass workspace active. 4-hour countdown started.", "assertive");
          overlay.remove(); resolve(session);
        });
        sheet.appendChild(el("div", { class: "hx-sheet__row" }, [cancel, go]));
        overlay.appendChild(sheet); doc.body.appendChild(overlay); modalize(overlay); reason.focus();
      });
    },
    viewRecord() { if (HX.breakglass.active) { HX.breakglass.active.recordsViewed++; HX.audit.write({ type: "BREAK_GLASS_VIEW", session: HX.breakglass.active.id, record: HX.breakglass.active.recordsViewed }); } },
    leave() {
      const s = HX.breakglass.active; HX.breakglass.active = null;
      if (s) { HX.audit.write({ type: "BREAK_GLASS_CLOSE", session: s.id, recordsViewed: s.recordsViewed }); HX.toast("Review item sent to Privacy Officer", { kind: "ok" }); }
      return s;
    },
  };

  /* ================================================================== CHIPS */
  const STATUS_GLYPH = { pass: "✓", ok: "✓", warn: "△", fail: "⬣", danger: "⬣", info: "ⓘ", neutral: "–", pending: "◴", vetoed: "⛊" };
  HX.chip = function chip(kind, value, opts) {
    opts = opts || {};
    const c = el(opts.button ? "button" : "span", { class: "hx-chip hx-chip--" + kind + (opts.lg ? " hx-chip--lg" : "") + (opts.class ? " " + opts.class : "") });
    if (opts.title) c.title = opts.title;
    const g = STATUS_GLYPH[kind] || (opts.glyph || null);
    if (g) c.appendChild(el("span", { class: "hx-chip__glyph", "aria-hidden": "true", text: g }));
    c.appendChild(el("span", { class: "hx-chip__text", text: value }));
    return c;
  };
  HX.groupChip = function groupChip(g) {
    const c = el("span", { class: "hx-chip hx-chip--group", title: "Subject-area group " + g });
    c.style.setProperty("--group-color", "var(--" + String(g).toLowerCase() + ")");
    c.appendChild(el("span", { class: "hx-chip__letter", text: g }));
    c.appendChild(el("span", { class: "hx-chip__text", text: groupName(g) }));
    return c;
  };
  function groupName(g) { return ({ G1: "Party & Identity", G2: "Clinical Care", G3: "Revenue & Finance", G4: "Supply & Ops", G5: "Workforce", G6: "Population Health", G7: "Research", G8: "Reference", G9: "Governance" })[g] || g; }
  HX.phiChip = function phiChip(cls) {
    const c = el("span", { class: "hx-chip hx-chip--phi hx-chip--phi-" + String(cls || "none").replace("phi:", "").replace(/[^a-z]/g, ""), title: "PHI class: " + cls });
    c.appendChild(icon("lock"));
    c.appendChild(el("span", { class: "hx-chip__text", text: phiLabel(cls) }));
    return c;
  };
  function phiLabel(cls) { return ({ "non-phi": "No PHI", "phi:none": "No PHI", "phi:direct": "PHI direct", "phi:clinical": "PHI clinical", "phi:quasi": "PHI quasi", "phi:sensitive": "PHI sensitive", "pii:workforce": "PII workforce", confidential: "Confidential", unknown: "PHI unknown" })[cls] || cls; }
  HX.codeChip = function codeChip(system, code, display, opts) {
    opts = opts || {};
    const c = el("span", { class: "hx-code-chip" + (opts.untrusted ? " hx-code-chip--untrusted" : ""), title: (opts.untrusted ? "source-asserted (untrusted) · " : "") + system + ":" + code });
    c.appendChild(el("span", { class: "hx-code-chip__sys", text: system + ":" }));
    c.appendChild(el("span", { text: code }));
    if (display) c.appendChild(el("span", { class: "hx-code-chip__display", text: display }));
    return c;
  };
  HX.iriChip = function iriChip(iri) {
    const short = String(iri).replace(/^https?:\/\/ontology\.healix\.example\.org\//, "healix:");
    const c = el("span", { class: "hx-iri-chip", title: iri });
    c.appendChild(el("span", { class: "hx-iri-chip__short", text: short }));
    c.appendChild(el("span", { class: "hx-iri-chip__full", text: iri }));
    const b = el("button", { title: "Copy IRI (y)", "aria-label": "Copy IRI" }, [icon("copy")]);
    b.addEventListener("click", () => copyText(iri));
    c.appendChild(b);
    return c;
  };
  function copyText(t) { try { navigator.clipboard.writeText(t); HX.toast("Copied", { kind: "ok", timeout: 2000 }); } catch (e) { HX.toast("Copy unavailable", { kind: "warn", timeout: 2000 }); } }

  /* stage stepper (§5.2) — stageState is {0:'released',1:'review',...} */
  HX.stepper = function stepper(stageState, opts) {
    opts = opts || {};
    const wrap = el("span", { class: "hx-stepper", role: "group", "aria-label": "Pipeline stage" });
    const STAGES = ["Inventory", "Vocabulary", "Taxonomy", "Thesaurus", "Ontology", "Knowledge graph"];
    for (let i = 0; i <= 5; i++) {
      const st = (stageState && stageState[i]) || (stageState && stageState[String(i)]) || "draft";
      const mod = st === "released" ? "filled" : st === "review" ? "half" : "outline";
      const dot = el("button", { class: "hx-stepper__dot hx-stepper__dot--" + mod, title: "Stage " + i + " · " + STAGES[i] + " · " + st, "aria-label": "Stage " + i + " " + STAGES[i] + ": " + st });
      dot.style.setProperty("--dot", "var(--stage-" + i + ")");
      if (opts.onStage) dot.addEventListener("click", () => opts.onStage(i));
      wrap.appendChild(dot);
    }
    if (opts.label) wrap.appendChild(el("span", { class: "hx-stepper__label", text: opts.label }));
    return wrap;
  };

  /* SLA ring (§5.2) */
  HX.sla = function sla(pct, opts) {
    opts = opts || {};
    const mod = pct >= 100 ? " hx-sla--breached" : pct >= 75 ? " hx-sla--at-risk" : "";
    const s = el("span", { class: "hx-sla" + mod, title: opts.title || (pct + "% of SLA consumed") });
    const ring = el("span", { class: "hx-sla__ring" }); ring.style.setProperty("--pct", Math.min(100, pct));
    s.appendChild(ring);
    s.appendChild(el("span", { text: opts.text || (pct >= 100 ? "breached" : Math.round(pct) + "%") }));
    return s;
  };

  /* score meter with κ (§5.2) */
  HX.score = function score(value, threshold, kappa) {
    const below = value < (threshold || 0);
    const s = el("span", { class: "hx-score" + (below ? " hx-score--below" : ""), title: "Evaluator score " + value + (threshold ? " · threshold " + threshold : "") });
    const meter = el("span", { class: "hx-score__meter" });
    const fill = el("span", { class: "hx-score__fill" }); fill.style.width = Math.round(value * 100) + "%"; meter.appendChild(fill);
    if (threshold) { const tick = el("span", { class: "hx-score__tick" }); tick.style.left = Math.round(threshold * 100) + "%"; meter.appendChild(tick); }
    s.appendChild(meter);
    s.appendChild(el("span", { text: value.toFixed ? value.toFixed(2) : value }));
    if (kappa != null) s.appendChild(el("span", { class: "hx-score__kappa", text: "κ " + kappa }));
    return s;
  };

  /* trust strip with gap chips (§2.4 C, §5.2) */
  HX.trust = function trust(obj) {
    const strip = el("div", { class: "hx-trust-strip", role: "group", "aria-label": "Trust strip" });
    const t = (obj && obj.trust) || {};
    const item = (k, node) => { const i = el("span", { class: "hx-trust-strip__item" }); i.appendChild(el("span", { class: "hx-trust-strip__k", text: k })); i.appendChild(node); strip.appendChild(i); };
    const gap = (text) => el("span", { class: "hx-chip hx-chip--gap", text: text });
    if (t.dqi != null && t.band !== undefined || t.dqi != null) {
      const band = t.band || (t.dqi >= 99 ? "Green" : t.dqi >= 97 ? "Amber" : t.dqi >= 95 ? "Red" : "Black");
      item("DQI", HX.chip("band-" + band.toLowerCase(), band + " " + t.dqi + " · coverage " + (t.coverage != null ? t.coverage + "%" : "?")));
    } else item("DQI", gap("No scorecard"));
    item("Freshness", t.freshness ? el("span", { class: "hx-trust-strip__v", text: t.freshness }) : gap("Unknown"));
    item("Binding", obj && obj.binding ? HX.chip("binding-" + obj.binding.replace(/[^a-z-]/g, ""), titleCase(obj.binding)) : gap("Unbound"));
    item("Release", (obj && obj.release) ? el("span", { class: "hx-version-chip", text: obj.release }) : gap("No release"));
    item("Lineage", t.lineageCoverage != null ? el("span", { class: "hx-trust-strip__v", text: t.lineageCoverage + "%" }) : gap("Coverage ?"));
    item("Open issues", el("span", { class: "hx-trust-strip__v", text: t.openIssues != null ? t.openIssues : 0 }));
    item("Steward", (obj && obj.steward) ? el("span", { class: "hx-trust-strip__v", text: obj.steward }) : gap("No steward"));
    if (obj && obj.trust && obj.trust.lastCr) item("Last CR", el("a", { href: HX.nav.hash ? "#" : "#", class: "hx-mono", text: obj.trust.lastCr, onclick: (e) => { e.preventDefault(); HX.nav("/cr/" + obj.trust.lastCr); } }));
    return strip;
  };

  /* evidence panel with fixed section grammar (§2.4 G, §5.2) */
  HX.evidence = function evidence(container, ev) {
    const host = typeof container === "string" ? $(container) : container;
    if (!host) return;
    clear(host);
    if (!ev || Object.keys(ev).length === 0) { host.appendChild(el("div", { class: "hx-evidence--empty" }, [icon("warn"), " No evidence attached — this proposal cannot be approved."])); return; }
    const wrap = el("div", { class: "hx-evidence" });
    const sections = [
      ["Source elements", "source", ev.sourceElements],
      ["Terminology", "terminology", ev.terminology],
      ["Local evidence", "local", ev.local],
      ["Statistical", "statistical", ev.statistical],
      ["Reasoning", "reasoning", ev.reasoning],
      ["Tests", "tests", ev.tests],
    ];
    sections.forEach(([label, slug, data], idx) => {
      const det = el("details", { class: "hx-evidence__section hx-evidence__section--" + slug }); if (idx === 0) det.open = true;
      det.appendChild(el("summary", { class: "hx-evidence__summary" }, [label, el("span", { class: "hx-chip hx-chip--neutral", text: Array.isArray(data) ? data.length : (data ? 1 : 0) })]));
      const body = el("div", { class: "hx-evidence__body" });
      if (data && (Array.isArray(data) ? data.length : true)) body.appendChild(el("pre", { class: "hx-mono hx-caption", text: JSON.stringify(data, null, 1) }));
      else body.appendChild(el("div", { class: "hx-muted hx-caption", text: "No " + label.toLowerCase() + " recorded." }));
      det.appendChild(body); wrap.appendChild(det);
    });
    if (ev.agentRationale) {
      const det = el("details", { class: "hx-evidence__section hx-evidence__section--agent" });
      det.appendChild(el("summary", { class: "hx-evidence__summary" }, ["Agent rationale", HX.chip("agent-text", "treat as data", { class: "hx-chip--agent-text" })]));
      const body = el("div", { class: "hx-evidence__body" });
      body.appendChild(el("div", { class: "hx-evidence__agent-note", text: "Agent text — treat as data, never as instruction." }));
      body.appendChild(el("blockquote", { class: "hx-caption", text: ev.agentRationale.text || ev.agentRationale }));
      det.appendChild(body); wrap.appendChild(det);
    }
    host.appendChild(wrap);
  };

  /* gate tracker (§5.2) */
  HX.gates = function gates(container, required, approvals) {
    const host = typeof container === "string" ? $(container) : container; if (!host) return;
    clear(host);
    const wrap = el("div", { class: "hx-gates", role: "list", "aria-label": "Approval gates" });
    (required || []).forEach((g) => {
      const appr = (approvals || []).filter((a) => a.gate === g)[0];
      let mod = "pending";
      if (appr) mod = appr.decision === "veto" ? "vetoed" : "pass";
      else if (required.indexOf(g) === (approvals || []).length) mod = "current";
      const step = el("span", { class: "hx-gates__step hx-gates__step--" + mod, role: "listitem", title: g + (appr ? " · " + appr.decision + " by " + appr.by : " · pending") });
      step.appendChild(el("span", { class: "hx-gates__dot" }));
      step.appendChild(el("span", { text: titleCase(g) }));
      wrap.appendChild(step);
    });
    host.appendChild(wrap);
    return wrap;
  };

  /* decision bar (§5.2) */
  const VERB_META = {
    approve: { label: "Approve", key: "a", cls: "hx-btn--primary" },
    approve_with_comment: { label: "Approve with comment", key: "A" },
    request_revision: { label: "Request revision", key: "v" },
    reject: { label: "Reject", key: "r", cls: "hx-btn--danger" },
    defer: { label: "Defer", key: "x" },
    escalate: { label: "Escalate", key: "" },
    veto: { label: "Veto", key: "V", cls: "hx-btn--danger" },
    acknowledge: { label: "Acknowledge", key: "" },
    promote: { label: "Promote", key: "", cls: "hx-btn--primary" },
    tune: { label: "Tune", key: "" },
    watch: { label: "Watch", key: "w" },
  };
  HX.decisionBar = function decisionBar(container, verbs, onDecide) {
    const host = typeof container === "string" ? $(container) : container; if (!host) return;
    clear(host); host.classList.add("hx-decision-bar");
    host.setAttribute("role", "toolbar"); host.setAttribute("aria-label", "Decision bar");
    (verbs || []).forEach((v) => {
      const meta = VERB_META[v] || { label: titleCase(v), key: "" };
      const btn = el("button", { class: "hx-btn " + (meta.cls || ""), "data-verb": v }, [meta.label]);
      if (meta.key) btn.appendChild(el("kbd", { text: meta.key }));
      btn.addEventListener("click", () => runDecision(v, onDecide));
      host.appendChild(btn);
    });
    host.appendChild(el("span", { class: "hx-decision-bar__spacer" }));
    host.appendChild(el("span", { class: "hx-decision-bar__hint", text: "Evidence: e · History: h · Next item: ]" }));
    return host;
  };
  async function runDecision(verb, onDecide) {
    const needsComment = /comment|revision|reject|veto|escalate/.test(verb);
    let comment = "";
    if (needsComment) {
      const ok = await promptComment(verb);
      if (ok === false) return;
      comment = ok;
      const scan = HX.phiScan(comment);
      if (!scan.pass) { HX.toast("Comment blocked by PHI guard: " + scan.findings.map((f) => f.kind).join(", "), { kind: "danger" }); return; }
    }
    const envOk = await HX.env.confirmWrite(); if (!envOk) return;
    const danger = /reject|veto/.test(verb);
    const ok = await HX.confirm({ title: titleCase(verb), body: "Gate: this records a decision in " + HX.env.get().toUpperCase() + ".", consequence: danger ? "This cannot be silently undone; it becomes a labelled calibration example." : null, danger: danger, confirmLabel: titleCase(verb) });
    if (!ok) return;
    if (typeof onDecide === "function") onDecide(verb, { comment: comment, env: HX.env.get(), at: new Date().toISOString() });
    HX.live("Decision recorded: " + verb, "polite");
  }
  function promptComment(verb) {
    return new Promise((resolve) => {
      const overlay = el("div", { class: "hx-overlay", role: "dialog", "aria-modal": "true", "aria-label": verb + " comment" });
      const sheet = el("div", { class: "hx-confirm-sheet" });
      sheet.appendChild(el("div", { class: "hx-sheet__head" }, [el("h2", { text: titleCase(verb) + " — comment" })]));
      const ta = el("textarea", { rows: 4, placeholder: "Reason (scanned for PHI before it is stored)" });
      const guard = el("div", { class: "hx-phi-guard" });
      ta.addEventListener("input", () => { const s = HX.phiScan(ta.value); clear(guard); if (!s.pass) guard.appendChild(el("div", { class: "hx-phi-guard__finding" }, [icon("danger"), "PHI-like text: " + s.findings.map((f) => f.kind).join(", ")])); });
      sheet.appendChild(el("div", { class: "hx-field" }, [el("label", { text: "Comment" }), ta]));
      sheet.appendChild(guard);
      const cancel = el("button", { class: "hx-btn" }, ["Cancel"]); cancel.addEventListener("click", () => { overlay.remove(); resolve(false); });
      const ok = el("button", { class: "hx-btn hx-btn--primary" }, ["Record"]); ok.addEventListener("click", () => { overlay.remove(); resolve(ta.value); });
      sheet.appendChild(el("div", { class: "hx-sheet__row" }, [cancel, ok]));
      overlay.appendChild(sheet); doc.body.appendChild(overlay); modalize(overlay); ta.focus();
    });
  }

  /* masked value (§5.2) */
  HX.masked = function masked(value, phiClass, opts) {
    opts = opts || {};
    if (opts.smallCell) return el("span", { class: "hx-masked hx-masked--small-cell", title: "small-cell suppression: n < 11" }, ["< 11"]);
    const m = el("span", { class: "hx-masked", title: "masked: " + (phiClass || "phi") + " — reveal only in entitled remediation workspace under break-glass" });
    m.appendChild(el("span", { "aria-hidden": "true", text: "▒▒▒▒" }));
    m.appendChild(el("span", { class: "hx-sr", text: "masked value, " + (phiClass || "protected") }));
    return m;
  };

  /* diff renderer (§5.2) */
  HX.diff = function diffRender(container, spec) {
    const host = typeof container === "string" ? $(container) : container; if (!host) return;
    clear(host); spec = spec || {};
    const fmt = spec.format || "raw";
    const KNOWN = ["skos", "skos-tree", "owl", "shacl", "ddl", "mapping", "valueset", "yaml", "json", "schema", "remediation", "turtle", "raw"];
    const supported = KNOWN.indexOf(fmt) >= 0 && fmt !== "raw" && fmt !== "turtle";
    const wrap = el("div", { class: "hx-diff hx-diff--" + fmt.replace("skos-tree", "skos") + (supported ? "" : " hx-diff--fallback") });
    const header = el("div", { class: "hx-diff__header" });
    header.appendChild(HX.chip("info", fmt + " diff"));
    if (spec.a) header.appendChild(el("span", { class: "hx-diff__a", text: "A " + spec.a }));
    if (spec.b) header.appendChild(el("span", { class: "hx-diff__b", text: "B " + spec.b }));
    const sw = el("div", { class: "hx-diff__switch", role: "group", "aria-label": "Renderer" });
    ["Native", "Text", "Turtle"].forEach((r, i) => { const b = el("button", { "aria-pressed": i === 0 && supported ? "true" : "false", text: r }); sw.appendChild(b); });
    header.appendChild(sw);
    wrap.appendChild(header);
    if (!supported) wrap.appendChild(el("div", { class: "hx-diff__fallback-note" }, [icon("warn"), "Native renderer not yet available — showing Turtle/YAML diff."]));
    if (fmt === "owl") wrap.appendChild(renderOwlDiff(spec));
    else wrap.appendChild(renderTextDiff(spec));
    if (spec.dryRun) wrap.appendChild(renderDryRun(spec.dryRun));
    host.appendChild(wrap);
    return wrap;
  };
  function renderTextDiff(spec) {
    const body = el("div", { class: "hx-diff__body" });
    const before = JSON.stringify(spec.before || {}, null, 1).split("\n");
    const after = JSON.stringify(spec.after || {}, null, 1).split("\n");
    const bset = new Set(before), aset = new Set(after);
    let ln = 0;
    before.forEach((l) => { if (!aset.has(l)) body.appendChild(diffLine(++ln, "del", "- " + l)); });
    after.forEach((l) => { if (!bset.has(l)) body.appendChild(diffLine(++ln, "add", "+ " + l)); else body.appendChild(diffLine(++ln, "", "  " + l)); });
    return body;
  }
  function diffLine(n, mod, text) { return el("div", { class: "hx-diff__line" + (mod ? " hx-diff__line--" + mod : "") }, [el("span", { class: "hx-diff__gutter", text: n }), el("span", { text: text })]); }
  function renderOwlDiff(spec) {
    const cols = el("div", { class: "hx-diff__cols" });
    const col = (title, body, mod) => { const c = el("div", { class: "hx-diff__col" + (mod ? " hx-diff__col--" + mod : "") }); c.appendChild(el("h4", { text: title })); c.appendChild(el("pre", { class: "hx-mono hx-caption", text: typeof body === "string" ? body : JSON.stringify(body, null, 1) })); return c; };
    cols.appendChild(col("Asserted (before)", spec.before || ""));
    cols.appendChild(col("Asserted (after)", spec.after || ""));
    cols.appendChild(col("Inferred delta", spec.inferred || "(reasoner pending)", "inferred"));
    const drawer = el("details", { class: "hx-explanation-drawer" }, [el("summary", { text: "Explanation" }), el("div", { class: "hx-caption", text: spec.explanation || "Select an inferred axiom to see its justification set." })]);
    const frag = doc.createDocumentFragment(); frag.appendChild(cols); frag.appendChild(drawer); return frag;
  }
  function renderDryRun(dry) {
    const wrap = el("div", { class: "hx-dryrun" });
    Object.keys(dry).forEach((k) => { const pass = /pass|ok|true/i.test(String(dry[k])); wrap.appendChild(el("span", { class: "hx-dryrun__check" }, [icon(pass ? "check" : "warn"), titleCase(k) + ": " + dry[k]])); });
    return wrap;
  }

  /* ---- timeline (§6) -------------------------------------------------- */
  HX.timeline = function timeline(container, events, opts) {
    const host = typeof container === "string" ? $(container) : container; if (!host) return;
    opts = opts || {}; clear(host);
    events = (events || []).slice().filter(Boolean);
    const lanes = opts.lanes || "both";
    const times = [];
    events.forEach((e) => { [e.validFrom, e.validTo, e.recordedAt, e.supersededAt, e.decisionTime].forEach((t) => { if (t) times.push(Date.parse(t)); }); });
    const min = Math.min.apply(null, times.length ? times : [Date.now() - 86400000]);
    const max = Math.max.apply(null, times.filter(isFinite).concat([Date.now()]));
    const span = Math.max(1, max - min);
    const pos = (t) => ((Date.parse(t) - min) / span) * 100;
    const GLYPH = { new: "●", "new-version": "▶", correction: "↺", retraction: "✕", merge: "⤵", unmerge: "⤴" };
    const wrap = el("div", { class: "hx-timeline", role: "group", "aria-label": "Temporal history" });
    const makeLane = (kind, label, timeField, endField) => {
      const lane = el("div", { class: "hx-timeline__lane hx-timeline__lane--" + kind });
      lane.appendChild(el("span", { class: "hx-timeline__lane-label", text: label }));
      events.forEach((e) => {
        const start = e[timeField] || (kind === "valid" ? e.validFrom : e.recordedAt);
        if (!start) return;
        const endT = e[endField] || (kind === "valid" ? e.validTo : e.supersededAt);
        const left = pos(start); const right = endT ? pos(endT) : 100;
        const seg = el("button", { class: "hx-timeline__segment" + (e.changeKind === "retraction" ? " hx-timeline__segment--retracted" : ""), title: (e.label || e.changeKind) + " · " + HX.format.time(start, kind), "aria-label": label + " segment: " + (e.label || "") + " " + (e.changeKind || "") });
        seg.style.left = left + "%"; seg.style.width = Math.max(2, right - left) + "%";
        seg.addEventListener("click", () => {
          const set = {}; set[kind === "valid" ? "asOf" : "asKnownAt"] = start; HX.asof.set(set);
          if (opts.onSelect) opts.onSelect(e);
          $$(".hx-timeline__segment", wrap).forEach((s) => s.setAttribute("aria-pressed", "false")); seg.setAttribute("aria-pressed", "true");
        });
        if (e.changeKind && GLYPH[e.changeKind]) { const g = el("span", { class: "hx-timeline__glyph", title: e.changeKind, text: GLYPH[e.changeKind] }); g.style.left = left + "%"; lane.appendChild(g); }
        lane.appendChild(seg);
      });
      return lane;
    };
    if (lanes === "both" || lanes === "valid") wrap.appendChild(makeLane("valid", "Valid time", "validFrom", "validTo"));
    if (lanes === "both" || lanes === "known") {
      const known = makeLane("known", "Known time", "recordedAt", "supersededAt");
      events.forEach((e) => { if (e.decisionTime) { const d = el("span", { class: "hx-timeline__decision", title: "decision time · " + HX.format.time(e.decisionTime, "known") }); d.style.left = pos(e.decisionTime) + "%"; known.appendChild(d); } });
      wrap.appendChild(known);
    }
    const legend = el("div", { class: "hx-timeline__legend" });
    Object.keys(GLYPH).forEach((k) => legend.appendChild(el("span", {}, [GLYPH[k] + " " + titleCase(k)])));
    legend.appendChild(el("span", {}, ["◆ decision time"]));
    wrap.appendChild(legend);
    host.appendChild(wrap);
    return wrap;
  };

  /* ---- charts (§6, dataviz skill: single accent, semantic bands) ------ */
  function chartFrame(w, h, pad) { const svg = svgEl("svg", { viewBox: "0 0 " + w + " " + h, preserveAspectRatio: "none", role: "img" }); return svg; }
  function toggleTable(host, tableFn) {
    const t = tableFn(); t.hidden = true; host.appendChild(t);
    const btn = el("button", { class: "hx-chart__toggle", "aria-expanded": "false" }, ["Show data table"]);
    btn.addEventListener("click", () => { const show = t.hidden; t.hidden = !show; btn.textContent = show ? "Hide data table" : "Show data table"; btn.setAttribute("aria-expanded", show ? "true" : "false"); });
    host.appendChild(btn);
  }
  HX.chart = {
    bar(container, series, opts) {
      const host = typeof container === "string" ? $(container) : container; if (!host) return; clear(host); opts = opts || {};
      const data = series || []; const w = 320, h = 140, pad = 24;
      const max = Math.max.apply(null, data.map((d) => d.value).concat([opts.max || 1]));
      const bw = (w - pad * 2) / Math.max(1, data.length);
      const svg = chartFrame(w, h);
      svg.setAttribute("aria-label", opts.label || "Bar chart");
      data.forEach((d, i) => {
        const bh = ((d.value) / max) * (h - pad * 2);
        const x = pad + i * bw + bw * 0.15, y = h - pad - bh;
        const rect = svgEl("rect", { class: "hx-chart__bar", x: x, y: y, width: bw * 0.7, height: Math.max(0, bh), rx: 2 });
        if (d.band) rect.setAttribute("class", "hx-chart__bar hx-chart__band-" + d.band);
        svg.appendChild(rect);
        svg.appendChild(svgEl("text", { class: "hx-chart__axis", x: x + bw * 0.35, y: h - pad + 12, "text-anchor": "middle", "font-size": "9" })).textContent = d.label;
      });
      if (opts.threshold != null) { const ty = h - pad - (opts.threshold / max) * (h - pad * 2); svg.appendChild(svgEl("line", { class: "hx-chart__threshold", x1: pad, x2: w - pad, y1: ty, y2: ty })); }
      host.appendChild(svg);
      toggleTable(host, () => chartTable(["Label", "Value"], data.map((d) => [d.label, d.value])));
      return svg;
    },
    line(container, series, opts) {
      const host = typeof container === "string" ? $(container) : container; if (!host) return; clear(host); opts = opts || {};
      const pts = (series || []).map((d, i) => typeof d === "number" ? { x: i, value: d } : d);
      const w = 320, h = 140, pad = 24;
      const vals = pts.map((p) => p.value);
      const max = Math.max.apply(null, vals.concat([opts.max != null ? opts.max : 1]));
      const min = opts.min != null ? opts.min : Math.min.apply(null, vals.concat([0]));
      const sx = (i) => pad + (i / Math.max(1, pts.length - 1)) * (w - pad * 2);
      const sy = (v) => h - pad - ((v - min) / Math.max(1e-9, max - min)) * (h - pad * 2);
      const svg = chartFrame(w, h); svg.setAttribute("aria-label", opts.label || "Line chart");
      if (opts.threshold != null) svg.appendChild(svgEl("line", { class: "hx-chart__threshold", x1: pad, x2: w - pad, y1: sy(opts.threshold), y2: sy(opts.threshold) }));
      const d = pts.map((p, i) => (i ? "L" : "M") + sx(i) + " " + sy(p.value)).join(" ");
      svg.appendChild(svgEl("path", { class: "hx-chart__line", d: d }));
      pts.forEach((p, i) => svg.appendChild(svgEl("circle", { class: "hx-chart__dot", cx: sx(i), cy: sy(p.value), r: 2.5 })));
      host.appendChild(svg);
      toggleTable(host, () => chartTable([opts.xLabel || "#", opts.yLabel || "Value"], pts.map((p, i) => [p.label != null ? p.label : i, p.value])));
      return svg;
    },
    sparkline(container, values, opts) {
      const host = typeof container === "string" ? $(container) : container; if (!host) return; clear(host); opts = opts || {};
      const v = (values || []).map(Number); if (!v.length) return;
      const w = 100, h = 24, max = Math.max.apply(null, v), min = Math.min.apply(null, v);
      const sx = (i) => (i / Math.max(1, v.length - 1)) * w;
      const sy = (x) => h - ((x - min) / Math.max(1e-9, max - min)) * h;
      const svg = chartFrame(w, h); svg.setAttribute("preserveAspectRatio", "none"); svg.setAttribute("aria-label", opts.label || ("trend, latest " + v[v.length - 1]));
      svg.appendChild(svgEl("path", { class: "hx-chart__line", d: v.map((x, i) => (i ? "L" : "M") + sx(i) + " " + sy(x)).join(" "), "stroke-width": "1.5" }));
      const wrap = el("span", { class: "hx-sparkline" }); wrap.appendChild(svg); host.appendChild(wrap);
      return svg;
    },
    burn(container, budget, opts) {
      const host = typeof container === "string" ? $(container) : container; if (!host) return; clear(host); opts = opts || {};
      // budget: {remaining:0..1, burnRate:number}
      const w = 320, h = 120, pad = 20; const svg = chartFrame(w, h); svg.setAttribute("aria-label", opts.label || "Error-budget burn");
      const consumed = 1 - (budget.remaining == null ? 0 : budget.remaining);
      const bw = (w - pad * 2) * consumed;
      svg.appendChild(svgEl("rect", { x: pad, y: h / 2 - 10, width: w - pad * 2, height: 20, rx: 4, fill: "var(--border)" }));
      svg.appendChild(svgEl("rect", { x: pad, y: h / 2 - 10, width: bw, height: 20, rx: 4, fill: consumed > 0.8 ? "var(--danger-fg)" : consumed > 0.5 ? "var(--warn-fg)" : "var(--ok-fg)" }));
      svg.appendChild(svgEl("text", { class: "hx-chart__label", x: pad, y: h / 2 + 26, "font-size": "10" })).textContent = "burn rate " + (budget.burnRate != null ? budget.burnRate + "×" : "?") + " · budget left " + Math.round((budget.remaining || 0) * 100) + "%";
      host.appendChild(svg);
      return svg;
    },
  };
  function chartTable(headers, rows) {
    const t = el("table", { class: "hx-chart__table" });
    t.appendChild(el("thead", {}, [el("tr", {}, headers.map((h) => el("th", { text: h })))]));
    t.appendChild(el("tbody", {}, rows.map((r) => el("tr", {}, r.map((c) => el("td", { text: c }))))));
    return t;
  }

  /* ---- lineage ribbon (§6) ------------------------------------------- */
  HX.lineage = function lineageRibbon(container, trace, opts) {
    const host = typeof container === "string" ? $(container) : container; if (!host) return; opts = opts || {}; clear(host);
    const dir = opts.direction || "downstream";
    const wrap = el("div", { class: "hx-lineage-ribbon" });
    const head = el("div", { class: "hx-lineage-ribbon__head" });
    head.appendChild(HX.chip("info", dir === "downstream" ? "Downstream →" : "← Upstream"));
    const toggle = el("button", { class: "hx-btn hx-btn--sm" }, ["Reverse direction"]);
    toggle.addEventListener("click", () => HX.lineage(host, trace, Object.assign({}, opts, { direction: dir === "downstream" ? "upstream" : "downstream" })));
    head.appendChild(toggle);
    const copy = el("button", { class: "hx-btn hx-btn--sm" }, [icon("copy"), "Copy trace as Markdown"]);
    copy.addEventListener("click", () => copyText(HX.lineage.toMarkdown(trace)));
    head.appendChild(copy);
    wrap.appendChild(head);
    const cols = el("div", { class: "hx-lineage-ribbon__cols" });
    const columns = ["Source", "std", "Canonical", "KG", "EDW"];
    const hops = (trace.hops || []).slice(); if (dir === "upstream") hops.reverse();
    const byCol = {}; columns.forEach((c) => byCol[c] = []);
    hops.forEach((h) => { (byCol[h.column] || (byCol[h.column] = [])).push(h); });
    (dir === "upstream" ? columns.slice().reverse() : columns).forEach((c) => {
      const col = el("div", { class: "hx-lineage-ribbon__col" }); col.appendChild(el("h4", { text: c }));
      (byCol[c] || []).forEach((h) => {
        const hop = el("div", { class: "hx-lineage-ribbon__hop" + (h.edgeStyle === "dashed" ? " hx-lineage-ribbon__hop--dashed" : "") });
        hop.appendChild(el("div", { class: "hx-mono hx-caption", text: h.object }));
        if (h.transform) hop.appendChild(el("div", { class: "hx-caption hx-muted", text: h.transform }));
        if (h.version) hop.appendChild(el("span", { class: "hx-version-chip", text: h.version }));
        if (trace.dqBadges && trace.dqBadges[c]) trace.dqBadges[c].forEach((b) => hop.appendChild(HX.chip("warn", b)));
        col.appendChild(hop);
      });
      if (!(byCol[c] || []).length) col.appendChild(el("div", { class: "hx-chip hx-chip--gap", text: "no hop" }));
      cols.appendChild(col);
    });
    wrap.appendChild(cols);
    host.appendChild(wrap);
    return wrap;
  };
  HX.lineage.toMarkdown = function (trace) {
    const lines = ["# Lineage trace — " + (trace.label || trace.id)];
    (trace.hops || []).forEach((h, i) => { lines.push((i + 1) + ". **" + h.column + "** `" + h.object + "`" + (h.version ? " _(" + h.version + ")_" : "") + (h.transform ? " — " + h.transform : "")); });
    return lines.join("\n");
  };

  /* ---- graph canvas + list twin (§6) --------------------------------- */
  HX.graph = function graph(container, nodes, edges, opts) {
    const host = typeof container === "string" ? $(container) : container; if (!host) return; opts = opts || {}; clear(host);
    const cap = opts.cap || 150;
    const shown = (nodes || []).slice(0, cap);
    const selected = { id: null };
    const w = 600, h = 400;
    // simple deterministic radial layout
    const layout = {};
    shown.forEach((n, i) => { const a = (i / shown.length) * Math.PI * 2; layout[n.id] = { x: w / 2 + Math.cos(a) * (w / 2 - 60), y: h / 2 + Math.sin(a) * (h / 2 - 60) }; });
    const canvas = el("div", { class: "hx-graph-canvas" });
    const svg = svgEl("svg", { viewBox: "0 0 " + w + " " + h, role: "img", "aria-label": opts.label || "Graph canvas" });
    (edges || []).forEach((e) => {
      const a = layout[e.from], b = layout[e.to]; if (!a || !b) return;
      const faded = opts.asOf && e.validFrom && Date.parse(e.validFrom) > Date.parse(opts.asOf);
      svg.appendChild(svgEl("line", { class: "hx-graph-canvas__edge" + (e.inferred ? " hx-graph-canvas__edge--inferred" : "") + (faded ? " hx-graph-canvas__edge--faded" : ""), x1: a.x, y1: a.y, x2: b.x, y2: b.y }));
    });
    shown.forEach((n) => {
      const p = layout[n.id]; const g = svgEl("g", { class: "hx-graph-canvas__node", tabindex: "0", role: "button", "aria-label": n.id });
      const c = svgEl("circle", { cx: p.x, cy: p.y, r: 8, fill: n.group ? "var(--" + String(n.group).toLowerCase() + ")" : "var(--accent)", stroke: n.phi && n.phi !== "non-phi" ? "var(--phi-direct)" : "var(--border-strong)", "stroke-width": n.phi && n.phi !== "non-phi" ? 2 : 1 });
      const label = svgEl("text", { x: p.x + 10, y: p.y + 3 }); label.textContent = shortId(n.id);
      g.appendChild(c); g.appendChild(label);
      const sel = () => { selected.id = n.id; syncSelection(); if (opts.onSelect) opts.onSelect(n); };
      g.addEventListener("click", sel); g.addEventListener("keydown", (e) => { if (e.key === "Enter" || e.key === " ") { e.preventDefault(); sel(); } });
      svg.appendChild(g);
    });
    canvas.appendChild(svg);
    if (shown.length < (nodes || []).length) canvas.appendChild(el("div", { class: "hx-chip hx-chip--neutral", style: "position:absolute;top:8px;left:8px", text: "+" + ((nodes.length) - shown.length) + " clustered" }));
    // list twin
    const twin = opts.twinEl ? (typeof opts.twinEl === "string" ? $(opts.twinEl) : opts.twinEl) : el("div", { class: "hx-graph-list" });
    clear(twin);
    shown.forEach((n) => { const row = el("div", { class: "hx-graph-list__item", tabindex: "0", role: "button", dataset: { id: n.id } }, [el("span", { class: "hx-chip hx-chip--type", text: n.type || "node" }), el("span", { class: "hx-mono", text: n.id })]); row.addEventListener("click", () => { selected.id = n.id; syncSelection(); if (opts.onSelect) opts.onSelect(n); }); twin.appendChild(row); });
    function syncSelection() {
      $$(".hx-graph-list__item", twin).forEach((r) => r.setAttribute("aria-selected", r.dataset.id === selected.id ? "true" : "false"));
      $$(".hx-graph-canvas__node", svg).forEach((g) => { const isSel = g.getAttribute("aria-label") === selected.id; g.querySelector("circle").setAttribute("r", isSel ? 11 : 8); });
    }
    if (opts.twinEl) host.appendChild(canvas);
    else { const split = el("div", { class: "hx-graph hx-graph--split" }); split.appendChild(canvas); split.appendChild(twin); host.appendChild(split); }
    return { canvas, twin, select: (id) => { selected.id = id; syncSelection(); } };
  };
  function shortId(id) { return String(id).split("/").pop().split(".").pop().slice(0, 18); }

  /* ---- live region (§8) ---------------------------------------------- */
  HX.live = function live(message, politeness) {
    const sel = politeness === "assertive" ? ".hx-live--assertive" : ".hx-live";
    let region = $(sel);
    if (!region) { region = el("div", { class: politeness === "assertive" ? "hx-live hx-live--assertive" : "hx-live", "aria-live": politeness === "assertive" ? "assertive" : "polite", role: "status" }); doc.body.appendChild(region); }
    region.textContent = ""; setTimeout(() => { region.textContent = message; }, 30);
  };

  /* ================================================================== PALETTE */
  const paletteProviders = [];
  HX.palette = {
    register(fn) { paletteProviders.push(fn); },
    open(mode) { openPalette(mode || "find"); },
    close() { const p = $(".hx-palette"); if (p) p.remove(); },
  };
  function openPalette(mode) {
    HX.palette.close();
    const overlay = el("div", { class: "hx-palette", role: "dialog", "aria-modal": "true", "aria-label": "Command palette" });
    const box = el("div", { class: "hx-palette__box" });
    const modeChip = el("span", { class: "hx-palette__mode hx-palette__mode--" + mode, text: mode });
    const input = el("input", { type: "text", role: "combobox", "aria-expanded": "true", "aria-controls": "hx-palette-list", "aria-autocomplete": "list", placeholder: mode === "resolve" ? "cs:72 INPATIENT · sct:… · dqr:… · iri:…" : mode === "act" ? "> approve · > set as-of … · > trip kill switch" : "Search concepts, CRs, tables, agents, ids…" });
    box.appendChild(el("div", { class: "hx-palette__input" }, [icon("search"), input, modeChip]));
    const list = el("ul", { class: "hx-palette__list", id: "hx-palette-list", role: "listbox" });
    box.appendChild(list);
    const count = el("span", {}); const live2 = el("div", { class: "hx-live", "aria-live": "polite" });
    box.appendChild(el("div", { class: "hx-palette__foot" }, [el("span", {}, ["↑↓ navigate"]), el("span", {}, ["Enter open"]), el("span", {}, ["Shift-Enter peek"]), el("span", {}, ["Ctrl-Enter copy URL"]), count, live2]));
    overlay.appendChild(box); doc.body.appendChild(overlay); modalize(overlay);
    let items = [], sel = 0, curMode = mode;
    function setMode(m) { curMode = m; modeChip.className = "hx-palette__mode hx-palette__mode--" + m; modeChip.textContent = m; }
    // A typed FILTER clause (type:/state:/group:/facility:…) is a Find query, not
    // a single-id RESOLVE — only genuine resolve prefixes (cs:/sct:/iri:…) or a
    // Cerner CODE^CDF switch to resolve, so the typed grammar reaches the Find
    // provider and matches the lists/saved views.
    const RESOLVE_PREFIX = /^(cs|sct|loinc|v3|cerner|fusion|iri|dqr|cr|topic|table|snomed|icd|rxnorm)\s*:/i;
    const FILTER_KEY = /^(type|state|risk|group|proposer|gate|stage|binding|severity|plane|age|facility|domain|tenant|source-system)\s*:/i;
    function render() {
      const q = input.value.trim();
      if (q[0] === ">") setMode("act");
      else if (!FILTER_KEY.test(q) && (RESOLVE_PREFIX.test(q) || /\^/.test(q))) setMode("resolve");
      else setMode("find");
      clear(list); items = [];
      if (curMode === "act") buildActs(q.replace(/^>/, "").trim()).forEach(addGroup);
      else if (curMode === "resolve") { const r = buildResolve(q); if (r) list.appendChild(r); }
      else { Promise.all(paletteProviders.map((fn) => { try { return fn(q); } catch (e) { return null; } })).then((groups) => { clear(list); items = []; (flatten(groups)).forEach(addGroup); finalize(); }); return; }
      finalize();
    }
    function flatten(groups) { const out = []; (groups || []).forEach((g) => { if (!g) return; (Array.isArray(g) ? g : [g]).forEach((x) => x && out.push(x)); }); return out; }
    function addGroup(group) {
      if (!group || !group.items || !group.items.length) return;
      list.appendChild(el("li", { class: "hx-palette__group", text: group.group || "Results" }));
      group.items.forEach((it) => {
        const li = el("li", { class: "hx-palette__item", role: "option", id: "hx-opt-" + items.length });
        li.appendChild(el("span", { class: "hx-palette__label", text: it.label }));
        (it.chips || []).forEach((ch) => li.appendChild(typeof ch === "string" ? HX.chip("neutral", ch) : ch));
        if (it.id) li.appendChild(el("span", { class: "hx-palette__id", text: it.id }));
        li.addEventListener("click", () => choose(it));
        li.addEventListener("mouseenter", () => { sel = items.indexOf(it); highlight(); });
        list.appendChild(li); items.push(it);
      });
    }
    function finalize() { sel = 0; highlight(); count.textContent = items.length + " result" + (items.length === 1 ? "" : "s"); live2.textContent = items.length + " results"; }
    function highlight() { $$(".hx-palette__item", list).forEach((li, i) => li.setAttribute("aria-selected", i === sel ? "true" : "false")); input.setAttribute("aria-activedescendant", items.length ? "hx-opt-" + sel : ""); const active = $$(".hx-palette__item", list)[sel]; if (active) active.scrollIntoView({ block: "nearest" }); }
    function choose(it, peek, copy) { if (!it) return; if (copy && it.route) { copyText(location.origin + "/" + routeToPage(it.route) + "#" + it.route); return; } if (peek && it.route) { HX.inspector.peek(it.route); HX.palette.close(); return; } if (it.action) it.action(); else if (it.route) HX.nav(it.route); HX.palette.close(); }
    input.addEventListener("input", render);
    input.addEventListener("keydown", (e) => {
      if (e.key === "ArrowDown") { e.preventDefault(); sel = Math.min(items.length - 1, sel + 1); highlight(); }
      else if (e.key === "ArrowUp") { e.preventDefault(); sel = Math.max(0, sel - 1); highlight(); }
      else if (e.key === "Enter") { e.preventDefault(); choose(items[sel], e.shiftKey, e.ctrlKey || e.metaKey); }
      else if (e.key === "Escape") { HX.palette.close(); }
    });
    overlay.addEventListener("click", (e) => { if (e.target === overlay) HX.palette.close(); });
    setMode(mode); input.value = mode === "act" ? "> " : ""; render(); input.focus();
  }
  function buildActs(q) {
    const verbs = [
      { label: "Approve", route: null, action: () => HX.toast("Approve — open a CR to act", { kind: "warn" }) },
      { label: "Request revision", action: () => HX.toast("Request revision — open a CR", { kind: "warn" }) },
      { label: "Set as-of 2026-03-10", action: () => HX.asof.set({ asOf: "2026-03-10T07:00:00Z" }) },
      { label: "Compare with v2026.10.2", action: () => HX.toast("Compare — open an object", { kind: "warn" }) },
      { label: "Watch this page", action: () => HX.toast("Watching this page", { kind: "ok" }) },
      { label: "Open in SPARQL", action: () => HX.toast("SPARQL console (stub)", { kind: "warn" }) },
      { label: "Run CQ-07", action: () => HX.nav("/cq/CQ-07") },
      { label: "Trip kill switch…", action: () => HX.confirm({ title: "Trip kill switch", body: "Scope: platform. This halts applied transitions.", typed: "platform", danger: true }).then((ok) => { if (ok) HX.killswitch.trip({ scope: "platform" }, "manual via palette"); }) },
    ].filter((v) => !q || v.label.toLowerCase().indexOf(q.toLowerCase()) >= 0);
    return [{ group: "Act (verb-first · permission-filtered)", items: verbs }];
  }
  function buildResolve(q) {
    const r = HX.nav.resolve(q.replace(/^(cs|sct|loinc|v3|cerner|fusion|iri|dqr|cr|topic|table):\s*/i, (m, p1) => resolvePrefix(p1, q)));
    const card = el("li", { class: "hx-palette__card" });
    card.appendChild(el("div", { class: "hx-row" }, [HX.chip(r.unknown ? "warn" : "info", r.unknown ? "unknown code" : "resolves"), el("span", { class: "hx-mono", text: r.route })]));
    if (r.unknown) card.appendChild(el("div", { class: "hx-caption hx-muted", text: "No HEALIX concept — propose one (CR-CV-ADD)." }));
    if (/cpt|licensed/i.test(q)) card.appendChild(el("span", { class: "hx-chip hx-chip--licensed-hidden", text: "licensed display hidden" }));
    const go = el("button", { class: "hx-btn hx-btn--sm hx-btn--primary" }, ["Open"]); go.addEventListener("click", () => { HX.nav(r.route); HX.palette.close(); });
    card.appendChild(go);
    return card;
  }
  function resolvePrefix(p, full) { return full.replace(/^[a-z0-9]+:\s*/i, ""); }

  // default Find provider from loaded data (concepts, CRs, agents, tables, topics)
  HX.palette.register(function (q) {
    if (!q) return { group: "Recent", items: store.get("recent", []).slice(0, 6).map((r) => ({ label: r, id: r, route: r })) };
    // One grammar engine everywhere (UX-IA-004): parse the query with the SAME
    // HX.data.parseFilter the lists and saved views use. When it carries typed
    // clauses (type:/state:/risk:/group:/age:/facility:…) filter each collection
    // with matchFilter + matchScope so e.g. `type:cr state:evaluated risk:high
    // group:G2` returns the identical CR set in the palette, lists and saved
    // views. A plain query keeps the richer field-aware substring search below.
    const f = HX.data.parseFilter(q);
    const scope = HX.scope.get();
    const typedKeys = Object.keys(f).filter((k) => k !== "_q");
    const needle = (f._q || q).toLowerCase();
    const match = (s) => String(s || "").toLowerCase().indexOf(needle) >= 0;
    return Promise.all([HX.data.load("concepts"), HX.data.load("change-requests"), HX.data.load("agents"), HX.data.load("catalog"), HX.data.load("topics")]).then(([c, cr, ag, cat, top]) => {
      const groups = [];
      if (typedKeys.length) {
        const grammar = (row) => matchFilter(row, f) && matchScope(row, scope);
        const concepts = (c.concepts || []).filter(grammar).slice(0, 6).map((x) => ({ label: pref(x) + "  ·  " + x.id, id: x.id, route: "/concept/" + x.id, chips: [stageChipMini(x.stage), x.group] }));
        if (concepts.length) groups.push({ group: "Concepts", items: concepts });
        const crs = (cr.changeRequests || []).filter(grammar).slice(0, 8).map((x) => ({ label: x.title, id: x.id, route: "/cr/" + x.id, chips: [x.state, x.riskClass] }));
        if (crs.length) groups.push({ group: "Change requests", items: crs });
        const ags = (ag.agents || []).filter(grammar).slice(0, 4).map((x) => ({ label: x.id, id: x.level, route: "/agent/" + x.id, chips: [x.level] }));
        if (ags.length) groups.push({ group: "Agents", items: ags });
        const tbls = (cat.tables || []).filter(grammar).slice(0, 4).map((x) => ({ label: x.fqn, id: x.group, route: "/table/" + x.schema + "/" + x.fqn.split(".").pop(), chips: [x.group] }));
        if (tbls.length) groups.push({ group: "EDW tables", items: tbls });
        const tps = (top.topics || []).filter(grammar).slice(0, 4).map((x) => ({ label: x.name, id: x.layer, route: "/topic/" + x.name, chips: [x.layer] }));
        if (tps.length) groups.push({ group: "Topics", items: tps });
        return groups;
      }
      const concepts = (c.concepts || []).filter((x) => match(x.id) || (x.labels || []).some((l) => match(l.value)) || (x.notations || []).some((n) => match(n.code))).slice(0, 6)
        .map((x) => ({ label: pref(x) + "  ·  " + x.id, id: x.id, route: "/concept/" + x.id, chips: [stageChipMini(x.stage), x.group] }));
      if (concepts.length) groups.push({ group: "Concepts", items: concepts });
      const crs = (cr.changeRequests || []).filter((x) => match(x.id) || match(x.title)).slice(0, 5).map((x) => ({ label: x.title, id: x.id, route: "/cr/" + x.id, chips: [x.state, x.riskClass] }));
      if (crs.length) groups.push({ group: "Change requests", items: crs });
      const ags = (ag.agents || []).filter((x) => match(x.id) || match(x.role)).slice(0, 4).map((x) => ({ label: x.id, id: x.level, route: "/agent/" + x.id, chips: [x.level] }));
      if (ags.length) groups.push({ group: "Agents", items: ags });
      const tbls = (cat.tables || []).filter((x) => match(x.fqn)).slice(0, 4).map((x) => ({ label: x.fqn, id: x.group, route: "/table/" + x.schema + "/" + x.fqn.split(".").pop(), chips: [x.group] }));
      if (tbls.length) groups.push({ group: "EDW tables", items: tbls });
      const tps = (top.topics || []).filter((x) => match(x.name)).slice(0, 4).map((x) => ({ label: x.name, id: x.layer, route: "/topic/" + x.name, chips: [x.layer] }));
      if (tps.length) groups.push({ group: "Topics", items: tps });
      return groups;
    }).catch(() => []);
  });
  function pref(c) { const l = (c.labels || []).filter((x) => x.role === "pref")[0]; return l ? l.value : c.id; }
  function stageChipMini(stage) { let n = 0; if (stage) Object.keys(stage).forEach((k) => { if (stage[k] === "released") n = Math.max(n, +k); }); return "S" + n; }

  // build resolver index from data (merged concepts → replacedBy)
  function buildResolverIndex() {
    HX._resolverIndex = { ids: {}, codes: {}, codesByPair: {}, merged: {} };
    HX.data.load("concepts").then((d) => {
      (d.concepts || []).forEach((c) => {
        HX._resolverIndex.ids[c.id] = "/concept/" + c.id;
        if (c.iri) HX._resolverIndex.ids[c.iri] = "/concept/" + c.id;
        (c.notations || []).forEach((n) => { if (n.system === "cerner" && n.code) { HX._resolverIndex.codesByPair[n.code] = "/concept/" + c.id; const parts = n.code.split("^"); if (parts.length === 2) { HX._resolverIndex.codes[parts[0] + "^" + parts[1]] = "/concept/" + c.id; HX._resolverIndex.codes["cs" + parts[0] + " " + parts[1]] = "/concept/" + c.id; } } });
        if (c.replacedBy) { const rb = typeof c.replacedBy === "object" ? c.replacedBy : { id: c.replacedBy }; HX._resolverIndex.merged[c.id] = { route: "/concept/" + rb.id, survivor: rb.id, date: rb.date || c.mergedOn || "", cr: rb.cr || null }; }
      });
    }).catch(() => {});
  }

  /* ================================================================== SHELL */
  HX.mount = function mount(opts) {
    opts = opts || {};
    // apply persisted prefs early
    HX.theme.set(HX.theme.get()); HX.density.set(HX.density.get());
    if (opts.env) HX.env.set(opts.env); else HX._env = HX.env.get();
    if (opts.role) HX.role.set(opts.role);

    // Build shell around #hx-main (create if absent)
    let main = $("#hx-main");
    if (!main) { main = el("main", { id: "hx-main", class: "hx-main" }); doc.body.appendChild(main); }
    else main.classList.add("hx-main");
    const shell = el("div", { class: "hx-shell" });
    const top = el("div", { class: "hx-shell__top" });
    top.appendChild(buildTopbar(opts));
    top.appendChild(el("div", { class: "hx-banners", id: "hx-banners" }));
    shell.appendChild(top);
    shell.appendChild(buildRail(opts));
    // move main into shell
    shell.appendChild(main);
    shell.appendChild(buildInspector());
    doc.body.insertBefore(shell, doc.body.firstChild);
    // Skip link (WCAG 2.4.1 Bypass Blocks): the shell (topbar + rail) is inserted
    // as body.firstChild, which would push any page-authored skip link behind the
    // whole header. Re-seat the skip link as the first focusable element, and
    // synthesise one (targeting the main region) on pages that ship without one,
    // so Bypass Blocks is actually satisfied and consistent everywhere.
    main.setAttribute("tabindex", "-1");
    let skip = $(".sr-skip");
    if (!skip) { skip = el("a", { class: "sr-skip", href: "#hx-main" }, ["Skip to content"]); }
    doc.body.insertBefore(skip, doc.body.firstChild);
    // if main wasn't first, ensure shell wraps correctly: move main node already appended

    if (HX.inspector && store.get("inspectorClosed", false)) shell.classList.add("hx-inspector--closed");
    const iw = store.get("inspectorWidth", null); if (iw) doc.documentElement.style.setProperty("--inspector-w", iw + "px");

    applyEnvChrome(); applyScopeChrome(); renderKillBanner(); renderTimeBanner(); renderSyntheticBanner(); renderShortcutBanner(); renderViewAsNote();
    readHashParams(); buildResolverIndex();
    updateChromeHeight();
    doc.addEventListener("keydown", onKeyDown);
    window.addEventListener("hashchange", () => { readHashParams(); renderTimeBanner(); });
    window.addEventListener("resize", updateChromeHeight);

    return { main, shell, inspector: HX.inspector.el(), area: opts.area, role: HX.role.get(), env: HX.env.get() };
  };

  function buildTopbar(opts) {
    const bar = el("div", { class: "hx-topbar", role: "banner" });
    const brand = el("a", { class: "hx-topbar__brand", href: "index.html", "aria-label": "HEALIX Console home" });
    const logo = svgEl("svg", { class: "hx-logo", viewBox: "0 0 24 24", "aria-hidden": "true" });
    logo.appendChild(svgEl("path", { d: "M4 4v16M20 4v16M4 12h16", stroke: "var(--accent)", "stroke-width": "2.4", fill: "none", "stroke-linecap": "round" }));
    brand.appendChild(logo); brand.appendChild(el("span", { text: "HEALIX" }));
    bar.appendChild(brand);
    // rail toggle
    const rt = el("button", { class: "hx-btn hx-btn--icon hx-btn--ghost", title: "Toggle rail ([)", "aria-label": "Toggle navigation rail" }, [icon("rail")]);
    rt.addEventListener("click", toggleRail); bar.appendChild(rt);
    // search / palette
    const search = el("button", { class: "hx-topbar__search", "aria-label": "Search and commands (Ctrl or Cmd K)" }, [icon("search"), el("span", { text: "Search · commands" }), el("kbd", { text: "⌘K" })]);
    search.addEventListener("click", () => HX.palette.open("find")); bar.appendChild(search);
    // as-of control button
    const asof = el("button", { class: "hx-btn hx-btn--sm hx-topbar__asof", id: "hx-asof-btn", title: "As-of control (t)" }, [icon("clock"), el("span", { id: "hx-asof-label", text: "Now · Now" })]);
    asof.addEventListener("click", () => HX.asof.open()); bar.appendChild(asof);
    // right cluster
    const right = el("div", { class: "hx-topbar__right" });
    // env switch
    const env = el("label", { class: "hx-env hx-env--" + HX.env.get(), id: "hx-env", title: "Environment" });
    const envSel = el("select", { "aria-label": "Environment" }, HX.env.list().map((e) => el("option", { value: e, text: e.toUpperCase() })));
    envSel.value = HX.env.get(); envSel.addEventListener("change", () => HX.env.set(envSel.value));
    env.appendChild(envSel); right.appendChild(env);
    // facility / domain scope
    const scope = el("label", { class: "hx-viewas hx-scope", id: "hx-scope", title: "Facility / domain scope (narrows queues, scorecards and audit)" });
    const scopeSel = el("select", { "aria-label": "Facility scope" }, HX.scope.list().map((f) => el("option", { value: f.id, text: f.id === "all" ? "All facilities" : f.label })));
    scopeSel.value = HX.scope.get(); scopeSel.addEventListener("change", () => HX.scope.set(scopeSel.value));
    scope.appendChild(el("span", { class: "hx-caption", text: "Scope" })); scope.appendChild(scopeSel);
    right.appendChild(scope);
    // bell
    const bell = el("button", { class: "hx-btn hx-btn--icon hx-btn--ghost hx-bell", title: "Inbox", "aria-label": "Inbox: needs you / watching" }, [icon("bell")]);
    bell.appendChild(el("span", { class: "hx-bell__count" }, [el("b", { text: "3" }), el("i", { text: "/9" })]));
    bell.addEventListener("click", () => HX.nav("/inbox")); right.appendChild(bell);
    // view as
    const viewas = el("label", { class: "hx-viewas", title: "View as role (rendering only; permissions unchanged)" });
    const vsel = el("select", { "aria-label": "View as role" }, HX.ROLES.map((r) => el("option", { value: r.id, text: r.label })));
    vsel.value = HX.role.get(); vsel.addEventListener("change", () => HX.role.set(vsel.value));
    viewas.appendChild(el("span", { class: "hx-caption", text: "View as" })); viewas.appendChild(vsel);
    right.appendChild(viewas);
    // theme
    const theme = el("button", { class: "hx-btn hx-btn--icon hx-btn--ghost hx-theme-toggle", title: "Theme", "aria-label": "Toggle theme" });
    refreshThemeBtn(theme);
    theme.addEventListener("click", () => { const order = ["system", "light", "dark"]; const cur = HX.theme.get(); HX.theme.set(order[(order.indexOf(cur) + 1) % 3]); });
    right.appendChild(theme);
    // help
    const help = el("button", { class: "hx-btn hx-btn--icon hx-btn--ghost", title: "Keyboard map (?)", "aria-label": "Help and keyboard map" }, [icon("help")]);
    help.addEventListener("click", () => openKeymap()); right.appendChild(help);
    // user
    const user = el("button", { class: "hx-btn hx-btn--icon hx-btn--ghost", title: "Account", "aria-label": "User menu" }, [icon("user")]);
    user.addEventListener("click", () => HX.nav("/gov")); right.appendChild(user);
    bar.appendChild(right);
    return bar;
  }
  function refreshThemeBtn(btn) { clear(btn); const t = HX.theme.get(); btn.appendChild(icon(t === "dark" ? "moon" : "sun")); btn.setAttribute("aria-label", "Theme: " + t); }

  function buildRail() {
    const rail = el("nav", { class: "hx-rail", id: "hx-rail", "aria-label": "Primary" });
    renderRail(rail);
    return rail;
  }
  function renderRail(railEl) {
    const rail = railEl || $("#hx-rail"); if (!rail) return;
    clear(rail);
    const here = location.pathname.split("/").pop() || "index.html";
    HX.AREAS.forEach((a) => {
      const current = a.page === here || (a.id === "home" && here === "index.html" && location.hash.indexOf("/inbox") < 0);
      const item = el("a", { class: "hx-rail__item", href: a.page, "aria-current": current ? "page" : null, title: a.label });
      item.appendChild(icon(a.icon));
      item.appendChild(el("span", { class: "hx-rail__label", text: a.label }));
      item.appendChild(el("kbd", { text: "g " + a.key }));
      item.addEventListener("click", (e) => { e.preventDefault(); HX.nav(a.route); });
      rail.appendChild(item);
    });
    rail.appendChild(el("div", { class: "hx-rail__spacer" }));
    const lineage = el("a", { class: "hx-rail__item", href: "lineage-explorer.html", title: "Lineage explorer (from an object)" });
    lineage.appendChild(icon("lineage")); lineage.appendChild(el("span", { class: "hx-rail__label", text: "Lineage" })); lineage.appendChild(el("kbd", { text: "g l" }));
    lineage.addEventListener("click", (e) => { e.preventDefault(); HX.nav("/lineage"); });
    rail.appendChild(lineage);
  }
  function toggleRail() { const shell = $(".hx-shell"); if (shell) shell.classList.toggle("hx-rail--collapsed"); }

  function buildInspector() {
    const insp = el("aside", { class: "hx-inspector", id: "hx-inspector", "aria-label": "Inspector" });
    const grip = el("button", { class: "hx-inspector__grip", "aria-label": "Resize inspector (use arrow keys)", title: "Drag or arrow keys to resize" });
    wireInspectorResize(grip);
    insp.appendChild(grip);
    const head = el("div", { class: "hx-inspector__head" });
    head.appendChild(el("h2", { text: "Inspector" }));
    const close = el("button", { class: "hx-btn hx-btn--icon hx-btn--ghost hx-btn--sm", "aria-label": "Close inspector (])" }, [icon("close")]);
    close.addEventListener("click", () => { HX.inspector.close(); store.set("inspectorClosed", true); });
    head.appendChild(close);
    insp.appendChild(head);
    insp.appendChild(el("div", { class: "hx-inspector__body", id: "hx-inspector-body", html: "<div class='hx-muted hx-caption'>Shift-click a chip, or Shift-Enter a list item, to peek here.</div>" }));
    return insp;
  }
  function wireInspectorResize(grip) {
    let startX, startW;
    const read = () => parseInt(getComputedStyle(doc.documentElement).getPropertyValue("--inspector-w")) || 400;
    const setW = (w) => { w = Math.max(360, Math.min(480, w)); doc.documentElement.style.setProperty("--inspector-w", w + "px"); store.set("inspectorWidth", w); };
    grip.addEventListener("mousedown", (e) => { startX = e.clientX; startW = read(); const mv = (ev) => setW(startW + (startX - ev.clientX)); const up = () => { doc.removeEventListener("mousemove", mv); doc.removeEventListener("mouseup", up); }; doc.addEventListener("mousemove", mv); doc.addEventListener("mouseup", up); });
    grip.addEventListener("keydown", (e) => { if (e.key === "ArrowLeft") { setW(read() + 20); e.preventDefault(); } else if (e.key === "ArrowRight") { setW(read() - 20); e.preventDefault(); } });
  }

  /* banners */
  function applyEnvChrome() {
    const bar = $(".hx-topbar"); if (bar) bar.classList.toggle("hx-topbar--prod", HX.env.get() === "prod");
    const env = $("#hx-env"); if (env) { env.className = "hx-env hx-env--" + HX.env.get(); const s = env.querySelector("select"); if (s) s.value = HX.env.get(); }
  }
  function applyScopeChrome() {
    const sc = $("#hx-scope"); if (sc) { const s = sc.querySelector("select"); if (s) s.value = HX.scope.get(); sc.classList.toggle("is-scoped", HX.scope.get() !== "all"); }
  }
  // Measure the live sticky top chrome (topbar + banner stack) into --chrome-h so
  // every sticky sub-header (object header, break-glass bar, rail, inspector)
  // pins BELOW the synthetic ribbon and amber time/kill banners instead of behind
  // them. Keeps the focus ring of the first control under the header visible
  // (UX-IA-013 / WCAG 2.4.11 Focus Not Obscured). Recomputed on every banner
  // change and on resize.
  function updateChromeHeight() {
    const top = $(".hx-shell__top"); if (!top) return;
    const set = () => { const h = top.offsetHeight; if (h) doc.documentElement.style.setProperty("--chrome-h", h + "px"); };
    if (typeof requestAnimationFrame === "function") requestAnimationFrame(set); else set();
  }
  function renderKillBanner() {
    const host = $("#hx-banners"); if (!host) return;
    const existing = $(".hx-banner--kill", host); if (existing) existing.remove();
    const k = HX.killswitch.get(); if (!k || !k.active) { updateChromeHeight(); return; }
    const scope = typeof k.scope === "object" ? Object.values(k.scope).join(" / ") : k.scope;
    const b = el("div", { class: "hx-banner hx-banner--kill", role: "alert" }, [icon("danger"), el("span", {}, ["Kill switch active: " + scope + " — held by " + (k.heldBy || "operator") + " since " + HX.format.time(k.since, "known")])]);
    const rel = el("button", { class: "hx-btn hx-btn--sm" }, ["Release"]); rel.addEventListener("click", () => HX.killswitch.release());
    b.appendChild(el("span", { class: "hx-banner__actions" }, [rel])); host.insertBefore(b, host.firstChild);
    HX.live("Kill switch active: " + scope, "assertive");
    updateChromeHeight();
  }
  function renderTimeBanner() {
    const host = $("#hx-banners"); if (!host) return;
    const existing = $(".hx-banner--time", host); if (existing) existing.remove();
    const label = $("#hx-asof-label"); const btn = $("#hx-asof-btn");
    const s = HX.asof.get();
    if (label) label.textContent = s.mode === "Current" ? "Now · Now" : (s.asOf ? HX.format.rel(s.asOf) : "Now") + " · " + (s.asKnownAt ? HX.format.rel(s.asKnownAt) : "Now");
    if (btn) btn.classList.toggle("is-active", s.mode !== "Current");
    if (s.mode === "Current") { updateChromeHeight(); return; }
    const b = el("div", { class: "hx-banner hx-banner--time", role: "status" }, [icon("clock"), el("span", { text: asofBannerText() })]);
    const ret = el("button", { class: "hx-btn hx-btn--sm" }, [icon("undo"), "Return to now"]); ret.addEventListener("click", () => HX.asof.clear());
    b.appendChild(el("span", { class: "hx-banner__actions" }, [ret])); host.appendChild(b);
    updateChromeHeight();
  }
  function renderSyntheticBanner() {
    // The synthetic claim derives from the DATA SOURCE, never the environment
    // name: a real/live-data build must NOT show a falsely reassuring "synthetic,
    // no PHI" ribbon over real PHI. This prototype is a synthetic build, so the
    // ribbon shows on every page; point the runtime at live data (set
    // HX.isSyntheticBuild = false) and it disappears.
    const host = $("#hx-banners"); if (!host || !HX.isSyntheticBuild) return;
    if ($(".hx-banner--synthetic", host)) return;
    host.appendChild(el("div", { class: "hx-banner hx-banner--synthetic", role: "note" }, [icon("info"), "Synthetic data — SYN- identifiers only. Prototype, not connected to any live system."]));
    updateChromeHeight();
  }
  function renderShortcutBanner() {
    const host = $("#hx-banners"); if (!host) return;
    const ex = $(".hx-banner--shortcuts-off", host); if (ex) ex.remove();
    if (keysEnabled) { updateChromeHeight(); return; }
    host.appendChild(el("div", { class: "hx-banner hx-banner--shortcuts-off", role: "status" }, [icon("info"), "Keyboard shortcuts are disabled (Preferences)."]));
    updateChromeHeight();
  }
  function renderViewAsNote() {
    const vs = $(".hx-viewas"); if (!vs) return;
    let note = $(".hx-viewas__note", vs);
    const role = HX.role.find(HX.role.get());
    if (HX.role.get() !== PRINCIPAL) {
      if (!note) { note = el("span", { class: "hx-viewas__note" }); vs.appendChild(note); }
      note.textContent = "Viewing as " + (role ? role.label : HX.role.get()) + " — your permissions unchanged";
    } else if (note) note.remove();
  }

  /* as-of sheet */
  function openAsOfSheet() {
    HX.asof.close();
    const overlay = el("div", { class: "hx-overlay", "data-asof": "1", role: "dialog", "aria-modal": "true", "aria-label": "As-of control" });
    const s = HX.asof.get();
    const sheet = el("div", { class: "hx-asof" });
    const modeSpan = el("span", { class: "hx-asof__mode", text: "Mode: " + s.mode });
    sheet.appendChild(el("div", { class: "hx-asof__head" }, [icon("clock"), modeSpan]));
    // scrub window: the slider maps 0..100 across the last 180 days → now (100).
    const WINDOW_MS = 180 * 86400000;
    const nowMs = Date.now();
    const sliderToIso = (val) => new Date(nowMs - ((100 - val) / 100) * WINDOW_MS).toISOString();
    const isoToSlider = (iso) => { const t = Date.parse(iso); if (isNaN(t)) return 100; return Math.max(0, Math.min(100, Math.round(100 - ((nowMs - t) / WINDOW_MS) * 100))); };
    const readField = (field) => (field.value ? fromLocal(field.value) : null);
    const pendingMode = () => {
      const linked = $("#hx-asof-link") && $("#hx-asof-link").checked;
      const a = readField(v.field); const kk = linked ? a : readField(k.field);
      if (!a && !kk) return "Current";
      if (a && kk) return "Bitemporal point";
      if (a && !kk) return "As-of";
      if (!a && kk) return "As-known";
      return "Full history";
    };
    const syncMode = () => { modeSpan.textContent = "Mode: " + pendingMode(); };
    let scrubTimer = null;
    const applyFromFields = (commit) => {
      const linked = $("#hx-asof-link").checked;
      const vo = readField(v.field);
      const ko = linked ? vo : readField(k.field);
      // An unchanged Apply (both lanes still blank/"Now") is a no-op: stay Current,
      // never silently flip to Bitemporal point just for inspecting the control.
      if (!vo && !ko) HX.asof.clear();
      else HX.asof.set({ asOf: vo, asKnownAt: ko, linked: linked });
      syncMode();
    };
    const mkLane = (kind, label, val) => {
      const lane = el("div", { class: "hx-asof__lane hx-asof__lane--" + kind });
      lane.appendChild(el("span", { class: "hx-asof__lane-label", text: label }));
      // Blank = "Now" (the current asofState), NOT a concrete timestamp — so the
      // control opens showing the state it will keep unless the user edits it.
      const field = el("input", { class: "hx-asof__field", type: "datetime-local", value: val ? toLocal(val) : "", placeholder: "Now", "aria-label": label + " point (blank = now)" });
      lane.appendChild(field);
      const sr = el("input", { class: "hx-asof__slider hx-range--" + kind, type: "range", min: "0", max: "100", step: "1", value: String(val ? isoToSlider(val) : 100), "aria-label": label + " slider — scrub the last 180 days", "aria-valuetext": label + " " + (val ? HX.format.time(val, kind) : "now") });
      lane.appendChild(sr);
      field._kind = kind;
      const api = { lane: lane, field: field, slider: sr };
      // Live scrub: move the slider → map to a timestamp, write the paired field
      // and aria-valuetext, mirror to the linked lane, and apply (debounced) so
      // the top banner moves with the control instead of doing nothing.
      sr.addEventListener("input", () => {
        const iso = sliderToIso(+sr.value);
        field.value = toLocal(iso);
        sr.setAttribute("aria-valuetext", label + " " + HX.format.time(iso, kind));
        const linked = $("#hx-asof-link") && $("#hx-asof-link").checked;
        if (linked && kind === "valid") { k.field.value = field.value; k.slider.value = sr.value; k.slider.setAttribute("aria-valuetext", "Known time " + HX.format.time(iso, "known")); }
        syncMode();
        clearTimeout(scrubTimer); scrubTimer = setTimeout(applyFromFields, 180);
      });
      // Typing in the field also refreshes the slider + mode live.
      field.addEventListener("input", () => { const iso = readField(field); if (iso) sr.value = String(isoToSlider(iso)); syncMode(); });
      return api;
    };
    const v = mkLane("valid", "Valid time", s.asOf);
    const k = mkLane("known", "Known time", s.asKnownAt);
    sheet.appendChild(v.lane);
    const linkWrap = el("label", { class: "hx-asof__link" }, [el("input", { type: "checkbox", checked: s.linked, id: "hx-asof-link" }), " Link valid & known (L)"]);
    linkWrap.querySelector("input").addEventListener("change", () => { if ($("#hx-asof-link").checked) { k.field.value = v.field.value; k.slider.value = v.slider.value; } syncMode(); });
    sheet.appendChild(linkWrap);
    sheet.appendChild(k.lane);
    // quick picks
    const picks = el("div", { class: "hx-asof__picks" });
    [["Now", null], ["Release v2026.10.2", "2026-10-02T00:00:00Z"], ["Before correction", "2026-09-29T00:00:00Z"], ["Yesterday 07:00", new Date(Date.now() - 86400000).toISOString()]].forEach(([label, iso]) => {
      const b = el("button", { class: "hx-btn hx-btn--sm" }, [label]);
      b.addEventListener("click", () => { if (!iso) { HX.asof.clear(); } else { HX.asof.set({ asOf: iso, asKnownAt: $("#hx-asof-link").checked ? iso : s.asKnownAt, linked: $("#hx-asof-link").checked }); } overlay.remove(); });
      picks.appendChild(b);
    });
    sheet.appendChild(picks);
    sheet.appendChild(el("details", { class: "hx-asof__explain" }, [el("summary", { text: "What do valid & known time mean?" }), el("p", { text: "Valid time is when a fact is true in the world (e.g. an encounter class in effect from 18 Sep). Known time (transaction time) is when the platform recorded or corrected it. A correction on 30 Sep changes what we knew, not when the fact was true — so 'as known at 29 Sep' still shows the pre-correction label." })]));
    const foot = el("div", { class: "hx-asof__foot" });
    const cancel = el("button", { class: "hx-btn" }, ["Cancel"]); cancel.addEventListener("click", () => overlay.remove());
    const apply = el("button", { class: "hx-btn hx-btn--primary" }, ["Apply"]);
    apply.addEventListener("click", () => { clearTimeout(scrubTimer); applyFromFields(); overlay.remove(); });
    foot.appendChild(cancel); foot.appendChild(apply); sheet.appendChild(foot);
    // keyboard: L toggles link
    sheet.addEventListener("keydown", (e) => { if (e.key === "l" || e.key === "L") { if (!inField(e.target) || e.target.type === "range") { const lk = $("#hx-asof-link"); lk.checked = !lk.checked; if (lk.checked) { k.field.value = v.field.value; k.slider.value = v.slider.value; } syncMode(); } } if (e.key === "Escape") overlay.remove(); });
    overlay.appendChild(sheet); overlay.addEventListener("click", (e) => { if (e.target === overlay) overlay.remove(); });
    doc.body.appendChild(overlay); modalize(overlay); v.field.focus();
  }
  function toLocal(iso) { const d = new Date(iso); const pad = (n) => String(n).padStart(2, "0"); return d.getFullYear() + "-" + pad(d.getMonth() + 1) + "-" + pad(d.getDate()) + "T" + pad(d.getHours()) + ":" + pad(d.getMinutes()); }
  function fromLocal(v) { if (!v) return null; return new Date(v).toISOString(); }

  /* keymap sheet */
  function openKeymap() {
    if ($(".hx-keymap")) return;
    const overlay = el("div", { class: "hx-keymap", role: "dialog", "aria-modal": "true", "aria-label": "Keyboard shortcuts" });
    const box = el("div", { class: "hx-keymap__box" });
    box.appendChild(el("div", { class: "hx-row" }, [el("h2", { text: "Keyboard shortcuts" }), el("span", { class: "hx-rail__spacer", style: "flex:1" }), (function () { const b = el("button", { class: "hx-btn hx-btn--icon hx-btn--ghost", "aria-label": "Close" }, [icon("close")]); b.addEventListener("click", () => overlay.remove()); return b; })()]));
    const tog = el("label", { class: "hx-row hx-caption" }, [el("input", { type: "checkbox", checked: keysEnabled }), " Shortcuts enabled"]);
    tog.querySelector("input").addEventListener("change", (e) => HX.keys.enabled(e.target.checked));
    box.appendChild(tog);
    const grid = el("div", { class: "hx-keymap__grid" });
    Object.keys(KEYMAP).forEach((section) => {
      const col = el("div", {}); col.appendChild(el("h3", { text: section }));
      const dl = el("dl", {}); Object.keys(KEYMAP[section]).forEach((k) => { dl.appendChild(el("dt", {}, [el("kbd", { text: k })])); dl.appendChild(el("dd", { text: KEYMAP[section][k] })); });
      col.appendChild(dl); grid.appendChild(col);
    });
    box.appendChild(grid); overlay.appendChild(box);
    overlay.addEventListener("click", (e) => { if (e.target === overlay) overlay.remove(); });
    overlay.addEventListener("keydown", (e) => { if (e.key === "Escape") overlay.remove(); });
    doc.body.appendChild(overlay); modalize(overlay); box.querySelector("button").focus();
  }

  function closeTopOverlay() {
    const o = $(".hx-palette") || $(".hx-keymap") || $(".hx-overlay[data-asof]");
    if (o) { o.remove(); return; }
    // non-destructive sheets only; confirm/break-glass stay (Esc handled inside)
  }

  /* hash params ←→ as-of/env (§2.5) */
  function readHashParams() {
    const h = location.hash.slice(1);
    const qi = h.indexOf("?"); if (qi < 0) { return; }
    const params = {}; h.slice(qi + 1).split("&").forEach((p) => { const [k, v] = p.split("="); if (k) params[k] = decodeURIComponent(v || ""); });
    if (params.asOf || params.asKnownAt) asofState = { asOf: params.asOf || null, asKnownAt: params.asKnownAt || null, linked: params.asOf === params.asKnownAt };
    if (params.env && ENVS.indexOf(params.env) >= 0) { HX._env = params.env; }
    renderTimeBanner();
  }
  function writeHashParams() {
    let h = location.hash.slice(1);
    const qi = h.indexOf("?"); const route = qi < 0 ? h : h.slice(0, qi);
    const params = {}; if (qi >= 0) h.slice(qi + 1).split("&").forEach((p) => { const [k, v] = p.split("="); if (k && k !== "asOf" && k !== "asKnownAt") params[k] = v; });
    if (asofState.asOf) params.asOf = encodeURIComponent(asofState.asOf);
    if (asofState.asKnownAt) params.asKnownAt = encodeURIComponent(asofState.asKnownAt);
    const qs = Object.keys(params).map((k) => k + "=" + params[k]).join("&");
    const nh = (route || "/") + (qs ? "?" + qs : "");
    try { history.replaceState(null, "", "#" + nh); } catch (e) { location.hash = nh; }
  }
  HX._applyHashRoute = function () { readHashParams(); };

  /* expose a couple of internals pages may want */
  HX.util = { el, icon, svgEl, titleCase, store, clear, groupName, shortId };

  global.HX = HX;
})(window);
