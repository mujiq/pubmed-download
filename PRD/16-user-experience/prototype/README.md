# HEALIX Console — clickable prototype (module 16, v0.1)

This folder is a **static, dependency-free prototype** of the HEALIX Console: the single
web application through which nine human roles govern the autonomous healthcare knowledge
platform specified in this PRD. It is the executable companion to the UX brief
(`../README.md` in module 16 and the full brief embedded in the build task).

> **Synthetic data only.** Every identifier in `data/*.json` is unmistakably synthetic
> (`SYN-nnnnnn`, `ZZ-TEST-*`, "Dr. Synthetic A. Example", "Example Medical Center – North").
> There is no PHI here and no connection to any live system. The synthetic-data ribbon is
> visible on every page.

## How to open it

No build step, no server, no package install.

- **From the filesystem:** double-click any `*.html` page (start with `index.html`).
  It opens over `file://`. The shared data loads from an inline
  `<script type="application/json" id="hx-data-NAME">` block embedded in each page, so the
  pages work without a server.
- **From a local server (optional):**
  ```sh
  cd prototype
  python3 -m http.server 8000      # then open http://localhost:8000/index.html
  ```
  A server is only needed for datasets a page does **not** embed. `HX.data.load(name)` always
  reads the inline `<script type="application/json" id="hx-data-NAME">` block **first** and
  `fetch()`es `data/NAME.json` only when the page has no such block (see `assets/app.js`,
  `HX.data.load`). Because every page embeds an inline block for the data it renders — and some
  of those blocks are reduced subsets of the full file (e.g. `index.html` ships 5 of the 51
  concepts) — editing `data/*.json` on its own does **not** change what a page shows, over
  `file://` **or** over HTTP. To see a data change, edit the inline block too (see *How to
  extend it*). The `data/*.json` files stay authoritative for tooling and for any page that
  does not embed a given dataset.

Tested in current Chromium and Firefox at 1440×900, 1024×768 and 390×844 (no horizontal
page scroll at phone width).

## What is here

```
prototype/
  assets/
    app.css   — the design system (§5 of the brief): tokens, components, light/dark, responsive
    app.js    — the shared `HX` runtime (§6): shell, nav, palette, as-of, data loader, components
  data/
    concepts.json          51 concepts across 8 schemes (encounter-class, lab-result-status,
                           supplier-type, bed-type, discharge-disposition, invoice-type,
                           position-type, procedure-category), bitemporal versions + provenance
    schemes.json           8 concept schemes with stage rails, exit criteria, collisions, trees
    change-requests.json   18 change requests in every state, with evidence, evaluator scores,
                           gates, dependencies; releases; saved views
    dq.json                scorecards G1–G9 with trends, 13 rules, 8 issues, exceptions, quarantine
    agents.json            20 agents (incl. memory_curator, self_healer, improver), runs,
                           evaluations, calibration, kill switch, canaries, rollbacks
    topics.json            40 Kafka topics, connectors, 22 SLOs (S1–S22), freeze windows, replays
    catalog.json           sources + 12-step onboarding, elements, EDW groups→schemas→tables→columns
                           bound to ontology IRIs, mapping specs, value sets, FHIR profiles
    lineage.json           traces (Cerner CODE_VALUE → HL7 → FHIR → KG property → EDW column) and
                           a ~47-node / ~73-edge neighbourhood graph
    incidents.json         incidents, playbooks, break-glass sessions, audit events, PHI
                           classification, access matrix, stewardship, data contracts, consent
    kpis.json              goals G-01…G-06, KPIs, phase gates, risk register, decisions-waiting
  *.html                   the eleven pages (owned by other builders; see below)
```

The eleven pages (built against this runtime and data) are:
`index.html` (role homes / switcher), `ontology-workbench.html`, `concept.html`,
`change-requests.html`, `dq-console.html`, `lineage-explorer.html`, `data-catalog.html`,
`interop-monitor.html`, `agent-ops.html`, `executive.html`, `settings-governance.html`.

## The shared runtime — `window.HX`

Every page calls `HX.mount({ page, title, area, role?, env? })` once; `HX` then builds the
three-pane shell (rail · canvas · inspector), the top bar (palette, as-of control, env
switch, View-as, theme, help), the banners (kill switch / time / synthetic), the command
palette, the keyboard map and the toast/live regions. The page fills `#hx-main`.

Key entry points (full contract in the brief §6):

| Area | API |
|------|-----|
| Navigation | `HX.nav(route, params)`, `HX.nav.resolve(anything)` (universal `/id/{…}` resolver) |
| Roles / PDP | `HX.role.get/set/list/on`, `HX.role.verbs(type, obj)` (never widened by View-as) |
| Environment | `HX.env.get/set`, `HX.env.confirmWrite()` (first prod write confirm) |
| Data | `HX.data.load(name)`, `HX.data.query(name, "type:cr state:evaluated …")`, `HX.data.asOf(records, asOf, asKnownAt)` |
| As-of | `HX.asof.get/set/clear/mode/on/open` (two linked lanes: `--time-valid` / `--time-known`) |
| Temporal | `HX.timeline(el, events, opts)` (valid & known lanes, changeKind glyphs, decision diamonds) |
| Components | `HX.chip`, `HX.groupChip`, `HX.phiChip`, `HX.codeChip`, `HX.iriChip`, `HX.stepper`, `HX.sla`, `HX.score`, `HX.trust`, `HX.evidence`, `HX.gates`, `HX.decisionBar`, `HX.diff`, `HX.masked` |
| Charts | `HX.chart.bar/line/sparkline/burn` (inline SVG, one accent + semantic bands, each ships a data table) |
| Lineage / graph | `HX.lineage(el, trace, opts)`, `HX.lineage.toMarkdown`, `HX.graph(el, nodes, edges, opts)` (canvas + synced list twin) |
| Palette | `HX.palette.open(mode)`, `HX.palette.register(provider)` — modes **find / resolve / act** |
| Privacy | `HX.phiScan(text)`, `HX.masked`, `HX.breakglass.open(issue)`, `HX.audit.write/list`, `HX.killswitch` |
| Chrome | `HX.toast`, `HX.confirm`, `HX.theme`, `HX.density`, `HX.inspector`, `HX.keys`, `HX.live`, `HX.format.time` |

Nothing but viewer preferences is persisted (role, theme, density, inspector width, saved
views, recent ids) — all through a `try/catch` `localStorage` wrapper that degrades to
in-memory. **No result payloads are ever stored client-side.**

## How to extend it

- **Add synthetic data:** edit the matching `data/*.json` file and keep the shapes shown in
  the brief §7. Then update the inline `<script type="application/json" id="hx-data-NAME">`
  block in **every page that renders that dataset** — the inline block always wins over the
  external file (`HX.data.load` fetches `data/NAME.json` only when a page has no inline block
  for that name), so a `data/*.json` edit alone has no effect on a page that embeds it, whether
  opened over `file://` or served over `http.server`. Keep every patient id `SYN-nnnnnn` and
  every person name prefixed `Zz-Test` or `Synthetic`.
- **Add a page:** give it `<main id="hx-main">`, include `assets/app.css` and `assets/app.js`,
  call `HX.mount(...)`, then build with the `HX.*` helpers so it inherits the shell, palette,
  as-of control and keyboard map for free. Route to it from `HX.AREAS` / the route map in
  `app.js` if it is a new canonical area.
- **Add a palette provider:** `HX.palette.register(q => ({ group, items:[{label, id, route, chips}] }))`.
- **Theming:** all colour comes from tokens on `:root` in `app.css`; dark values live under
  `@media (prefers-color-scheme: dark)` guarded by `:root:not([data-theme="light"])` and again
  under `:root[data-theme="dark"]`. The two temporal tokens `--time-valid` / `--time-known`
  are reserved for the as-of control, timeline and diff A/B headers only.

## Validation

```sh
node --check assets/app.js                 # JS syntax
for f in data/*.json; do python3 -m json.tool "$f" >/dev/null && echo "ok $f"; done
```

## Phase 1 scope (brief §0, AC-19)

Fully designed and interactive in Phase 1:

- **Change-request review** — the queue, the single most-polished CR page (diff · evidence ·
  evaluator verdict · privacy verdict · gates · DecisionBar), the batch-approval tray, releases
  and rollbacks.
- **Concept & scheme** — the canonical object template (breadcrumb, header with stage stepper,
  trust strip with gap chips, temporal history strip, eight fixed tabs, provenance footer) and
  the ontology workbench (stage board, tree, facets, relation board, axiom editor, competency
  questions, collisions/merge, drift inbox).
- **Source onboarding** — the 12-step wizard with the owning actor per step.
- **Quality** — scorecards (four DQ bands: Green ≥ 99.0 / Amber 97.0–98.9 / Red 95.0–96.9 /
  Black < 95.0), rules, issues, and the entitled remediation workspace behind break-glass.
- **Inbox** — the three lanes (Needs you · At risk · FYI) and nine role homes.
- **The as-of control** — two linked lanes (valid & known time), five TKG-01 modes, quick picks,
  typed fields, keyboard operation; it travels in the URL hash.

Everything else is reachable through the generic canonical object template.

## Non-goals (brief §0, AC-19)

- **No notebooks and no natural-language→SQL.** Tables expose a **Query templates** tab only,
  with `asOf` / `asKnownAt` and the ontology version pinned from the as-of control.
- **No clinician break-glass on patient-dimension rows in this console.** The *only* PHI surface
  is the entitled remediation workspace (DQ-REM-013, GOV-ACC-012) reached from a DQ issue, under a
  reason code + MFA, time-boxed to 4 hours, red-framed, audited per record viewed. Agents never
  see a reveal control because none exists for them.
- **The graph canvas is never a Home landing page.** It opens only from an object (focus + hops),
  as a lineage ribbon, or as the SRE pipeline map.

## Merge decisions (brief §0)

The Console is **Vision 1 (decision-first Console)** as the base, with mandated grafts:

- **From Vision 0 (Workbench):** the pipeline-as-spine stage stepper; the fixed eight-tab
  canonical order; the dual-lane as-of control with linking, quick picks, amber time banner and
  "not yet asserted" placeholders; the Evidence-panel section grammar; ontologist depth (tree
  browser, relation board, Manchester axiom editor with live ELK, three-column OWL diff, CQ page,
  collisions/merge, drift inbox); the single Lineage ribbon component; batch-review rules;
  break-glass specifics; shell behaviours and the keyboard map.
- **From Vision 2 (Atlas):** the trust strip with gap chips; the five TKG-01 time modes with the
  reserved `--time-valid` / `--time-known` tokens; the universal resolver `/id/{anything}` with a
  `replacedBy` banner for merged/deprecated objects; empty states that name the CR type that fills
  the gap plus an 8-second undo toast; the stage rail with live exit-criteria checklists and
  Promote gating; the executive drill-down with rate-limited, audited nudge; the pipeline map with
  auto-clustering and a synchronised list/tree twin for every canvas; the "licensed display hidden"
  chip.
- **Explicit corrections (binding):** four DQ bands (above); no clinician break-glass in this
  console; no notebooks / no NL→SQL; the graph canvas is never a Home.
