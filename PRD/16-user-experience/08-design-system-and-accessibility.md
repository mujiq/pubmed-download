---
title: Design System & Accessibility
module: 16-user-experience
doc_id: UX-08
version: 0.1.0
status: draft
owners: [Design Systems, Product Design, Accessibility, Data Platform]
depends_on: [VIS-00, ARC-01, UX-00, UX-02, TKG-01, GOV-01, DQ-00]
last_updated: 2026-10-17
---

# Design System & Accessibility

UX-00 fixes the HEALIX Console vision, the eight-area IA and the canonical page template; UX-02 fixes the shell and the
object model. This document is the **normative home of the `UX-DS-*` (design system) and `UX-A11Y-*` (accessibility and
keyboard) requirements**. It specifies the design tokens (light and dark), typography, grid and layout, the component
inventory and its class-naming contract, iconography, motion, data-visualisation rules, the WCAG 2.2 AA checklist, the
keyboard map, internationalisation and time-zone display rules, performance budgets and the maintainability rules for
the UI codebase. `UX-DS-001..004` and `UX-A11Y-001..006` carry the meanings fixed in UX-00 §9 and are restated
normatively here; the surface is extended from the higher-numbered requirements.

The reference implementation is `prototype/assets/app.css` (the entire design system, plain CSS, no build step) and
`prototype/assets/app.js` (the `window.HX` runtime). Every value below is a token already defined on `:root`; no page
MUST hard-code a colour, spacing, radius or duration. Charts follow the repository's data-visualisation skill; nothing
here forks the DQ bands (DQ-00), the five time modes (TKG-01 §9) or the PHI classes (GOV-01) — the UI renders them.

## 1. Token system (light and dark)

All colour, spacing, radius, shadow, motion and layout values are CSS custom properties on `:root`. Dark values are
declared twice: under `@media (prefers-color-scheme: dark)` guarded by `:root:not([data-theme="light"])` (system
default, user has not overridden) and again under `:root[data-theme="dark"]` (explicit override). `body` sets an
explicit `background: var(--bg-canvas)` and `color: var(--fg)`.

| Token family | Members (light → dark shown for a sample) | Rule |
|--------------|-------------------------------------------|------|
| Surfaces / text | `--bg-canvas` #F6F8FA→#0E1116 · `--bg-surface` · `--bg-raised` · `--bg-sunken` · `--bg-overlay` · `--border` · `--border-strong` · `--fg` #14181F→#E6EAF0 · `--fg-muted` · `--fg-faint` · `--fg-inverse` · `--accent` · `--accent-fg` · `--link` · `--focus` | layered backgrounds, never pure black on pure white |
| Status families | `--ok-*` (check) · `--warn-*` (triangle) · `--danger-*` (octagon) · `--info-*` (circle-i) · `--neutral-*` (dash) · `--pending-*` (clock) · `--vetoed-*` (shield-x) — each `-bg`/`-fg`/`-border` | each family MUST pair with its named glyph |
| DQ black band | `--black-band-bg`/`-fg`/`-border` | DQ Black band only; dark adds a 1 px white border |
| Temporal axes | `--time-valid` #2457C5→#7FA7FF · `--time-valid-bg` · `--time-known` #C9661A→#FFA466 · `--time-known-bg` · `--time-banner-bg`/`-fg` | **reserved**; see UX-DS-005 |
| Stage ramp | `--stage-0` … `--stage-5` | stepper dots, stage rail, stage board |
| Group hues | `--g1` … `--g9` (colour-blind-safe categorical) | letter always rendered beside the hue |
| PHI classes | `--phi-none` · `--pii-workforce` · `--confidential` · `--phi-quasi` · `--phi-clinical` · `--phi-direct` · `--phi-sensitive` | outline/stripe + lock glyph; the only red outside `--danger-*` |
| Risk / env | `--risk-low/-medium/-high/-phi` · `--env-dev/-test/-uat/-prod` · `--frame-emergency` | env-prod is a 2 px red line + chip |

- **UX-DS-001** (P0, analysis) — All colour MUST come from the `:root` tokens; dark values MUST appear under both
  `@media (prefers-color-scheme: dark)` guarded by `:root:not([data-theme="light"])` and `:root[data-theme="dark"]`, and
  `body` MUST set an explicit background. No component rule MUST contain a literal hex, `rgb()` or named colour.
- **UX-DS-002** (P0, analysis) — An automated contrast pass over every token pair that can appear together (text on each
  surface, chip `-fg` on `-bg`, focus ring against adjacent surfaces) MUST report ≥ 4.5:1 for body and small text and
  ≥ 3:1 for large text, UI graphics and focus indicators, in **both** themes; the pass is a CI gate (UX-00 §10).
- **UX-DS-005** (P0, analysis) — `--time-valid` and `--time-known` (and their `-bg` variants) MUST be used **only** for
  the two time axes — as-of slider lanes, timeline lanes, diff A/B headers when diffing moments, interval glyphs — and
  **nowhere else** in the CSS (cross-ref UX-ONT-003). A grep for these tokens outside the temporal components MUST
  return nothing; this keeps valid-time and transaction-time visually unambiguous across the whole product.
- **UX-DS-006** (P0, inspection) — Stage MUST be shown with the `--stage-N` ramp **and** the numeric stage; group MUST be
  shown with the `--gN` hue **and** the letter `G1`–`G9`. Neither stage nor group MUST be conveyed by hue alone.
- **UX-DS-007** (P0, inspection) — PHI class MUST render as a chip combining its `--phi-*` colour, an outline or stripe
  pattern **and** a lock glyph (`--phi-sensitive` adds a hatched left edge + asterisk); `--phi-*` and `--danger-*` are
  the only reds in the system, so a red that is not danger always means PHI.

## 2. Typography

One UI typeface (`--font-ui`: Inter, system fallback) and one monospace (`--font-mono`: JetBrains Mono). Monospace is
**required** for ids, IRIs, codes, notations, SPARQL/SQL/DDL/Turtle, timestamps and tabular numerals, so machine
identifiers never read as prose. `body` sets `font-variant-numeric: tabular-nums` so figures align in tables.

| Token | Size / line | Use |
|-------|-------------|-----|
| `--fs-micro` | 11 / 16 | chip and table meta only; full text on hover/focus |
| `--fs-caption` | 12 / 16 | captions, code chips, timestamps |
| `--fs-dense` | 13 / 20 | **default** in lists and inspectors (12/16 under `[data-density="compact"]`) |
| `--fs-body` | 14 / 22 | prose default; measure capped at 68ch |
| `--fs-lead` | 16 / 24 | section leads |
| `--fs-h3`/`-h2`/`-h1` | 20/28 · 24/32 · 32/40 | headings; page titles render at `--fs-h2` |

- **UX-DS-008** (P0, inspection) — Headings MUST use weight 600 and MUST NOT be coloured to carry meaning; prose MUST cap
  at 68ch; every id, IRI, code, notation, query fragment and timestamp MUST render in `--font-mono`; and the
  plain-language label MUST precede the mono id on every object (UX-18 content rule).

## 3. Grid, layout and responsive behaviour

Layout tokens: `--rail-w` 240 / `--rail-w-collapsed` 56, `--inspector-w` 400 (min 360, max 480, user-resizable and
remembered), `--topbar-h` 48, `--header-h` 72 → `--header-h-compact` 48 on scroll, `--decision-h` 56,
`--content-max` 1600. Spacing scale `--sp-1`…`--sp-16` (4–64 px); radii `--r-ctl` 4 / `--r-card` 8 / `--r-sheet` 12 /
`--r-pill` 999; shadows `--shadow-raised` / `--shadow-overlay` (replaced by a 1 px border in dark). The content grid is
12 columns, gutters 16 px below 1024 px and 24 px above.

```mermaid
flowchart LR
  subgraph wide["≥ 1024 px — three-pane"]
    R1[rail]:::p --- M1[main · minmax 640]:::p --- I1[inspector 360–480]:::p
  end
  subgraph mid["768–1023 px"]
    R2[rail]:::p --- M2[main]:::p --- I2[inspector → bottom sheet]:::s
  end
  subgraph narrow["< 768 px (down to 390)"]
    M3[main · tables become card rows]:::p
    T3[rail → bottom tab bar]:::s
    I3[inspector → bottom sheet]:::s
  end
  classDef p fill:#eef,stroke:#88a; classDef s fill:#ffe,stroke:#aa6;
```

- **UX-DS-009** (P0, test) — Below 1024 px the inspector MUST become a bottom sheet; below 768 px the rail MUST become a
  bottom tab bar and data tables MUST reflow to card rows; at 390 px there MUST be **no horizontal page scroll**. The
  layout MUST remain usable at 200 % zoom (cross-ref UX-A11Y-005).
- **UX-DS-004** (P1, test) — The eleven pages MUST share one `assets/app.css` and one `assets/app.js`, load the
  `data/*.json` fixtures, and render with **no console errors** in Chromium and Firefox at 1440×900, 1024×768 and
  390×844.

## 4. Component inventory and class-naming contract

Components are a single library used by every page so numbers and chips match across roles (UX-P4). Class names follow a
block / element / modifier convention: `.hx-{block}`, `.hx-{block}__{element}`, `.hx-{block}--{modifier}`. The inventory
(full forms in `app.css`): shell (`.hx-shell`, `.hx-rail`, `.hx-topbar`, `.hx-env--*`, `.hx-banner--{kill,time,synthetic,
replaced-by,cert-revoked}`, `.hx-inspector`, `.hx-palette`, `.hx-keymap`, `.hx-toast` + `.hx-toast__undo`, `.hx-live`);
chips (`.hx-chip` + `--{status,pass,warn,fail,pending,vetoed,risk-*,band-green/amber/red/black,phi-*,env-*,binding-*,
level,gap,licensed-hidden,agent-text,untrusted}`, `.hx-code-chip`, `.hx-iri-chip`, `.hx-chip--stage`, `.hx-score`,
`.hx-sla`); cards and tiles (`.hx-card--queue`, `.hx-tile--{stat,band,trust,slo}`, `.hx-sparkline`, `.hx-agent-card`,
`.hx-inbox-row`, `.hx-role-switcher`); object page (`.hx-breadcrumb`, `.hx-object-header`, `.hx-trust-strip`,
`.hx-history-strip`, `.hx-tabs`, `.hx-tab--empty`, `.hx-prov-footer`, `.hx-placeholder--not-asserted`, `.hx-depth-toggle`);
temporal (`.hx-asof` with `__lane--valid`/`__lane--known`, `.hx-timeline` with the six `__glyph--*`); evidence/review
(`.hx-evidence__section--*`, `.hx-verdict-strip`, `.hx-gates`, `.hx-decision-bar`, `.hx-confirm-sheet`, `.hx-batch-tray`,
`.hx-receipt`, `.hx-phi-guard`); diff (`.hx-diff--{skos,owl,shacl,ddl,mapping,valueset,yaml,schema,remediation,raw}`,
`.hx-diff--fallback`); trees/grids/canvases (`.hx-tree`, `.hx-datatable`, `.hx-column-grid`, `.hx-masked`,
`.hx-lineage-ribbon`, `.hx-graph-canvas`, `.hx-graph-list`, `.hx-pipeline-map`, `.hx-stage-rail`, `.hx-state-rail`);
forms/dialogs (`.hx-field`, `.hx-editor`, `.hx-wizard`, `.hx-breakglass-sheet`/`-frame`, `.hx-killswitch`,
`.hx-rollback-card`); states (`.hx-skeleton`, `.hx-empty`, `.hx-error`); charts (`.hx-chart--{bar,line,burn}`).

- **UX-DS-003** (P0, inspection) — Every status, PHI, band, risk and stage chip MUST carry **icon + text** (never colour
  alone); a masked value MUST render the `.hx-masked` glyph (`▒▒▒▒` / "< 11") with a tooltip ("masked: phi:direct"),
  never blank space; a missing value MUST render a `.hx-chip--gap` explaining the gap, never an empty cell.
- **UX-DS-010** (P0, inspection) — All interactive targets MUST be ≥ 24×24 px (buttons `min-height` 32, `.hx-btn--sm` 24,
  chips with a hit area ≥ 24). Each component MUST exist once in the library and be reused; a page MUST NOT fork a
  component's markup or restyle it with local CSS. New classes MUST follow the `.hx-{block}__{element}--{modifier}`
  convention.
- **UX-DS-014** (P1, inspection) — The `.hx-empty` and `.hx-error` states MUST name what is missing and the CR type that
  fixes it, and `.hx-error` MUST surface a copyable correlation id; reversible actions MUST confirm with an 8-second
  undo toast (`.hx-toast__undo`, cross-ref UX-18).

## 5. Iconography

One line-icon set, drawn as inline SVG via `HX.icon(name)`, rendered through `.hx-icon`: 16×16 default (18 in the rail),
`fill: none`, `stroke: currentColor`, `stroke-width: 1.75`, round caps and joins, so an icon always inherits its text
colour and theme. Status glyphs are fixed to their family (check/triangle/octagon/circle-i/dash/clock/shield-x per §1);
the lock glyph marks PHI.

- **UX-DS-011** (P0, inspection) — Icons MUST use `currentColor` (never a hard-coded fill) so they theme automatically;
  a purely decorative icon MUST be `aria-hidden="true"`; an icon that is the only label of a control MUST provide an
  accessible name; and a status icon MUST be the glyph its token family mandates, used consistently everywhere.

## 6. Motion

Durations `--dur-micro` 120 ms, `--dur-panel` 200 ms, `--dur-slow` 320 ms; one easing `--ease`
`cubic-bezier(.2,.8,.2,1)`. Motion is confined to entrances, state-chip pops and the toast timer.

- **UX-DS-012** (P0, analysis) — All motion except opacity MUST be disabled under `prefers-reduced-motion: reduce` (the
  global rule zeros transition and animation durations); motion MUST never be the sole signal of a state change (the
  state chip also changes icon + text); no animation MUST loop indefinitely except a loading shimmer/spinner, which MUST
  stop when content arrives (cross-ref UX-A11Y-005, WCAG 2.2.2).

## 7. Data-visualisation rules

Charts are SVG, built by `HX.chart.{bar,line,sparkline,burn}`, using tokens only, and follow the repository's
data-visualisation skill. Rules: one accent per series (`--chart` classes derive from `--accent`, `--ok-fg`,
`--warn-fg`, `--danger-fg`); thresholds and targets are **labelled dashed lines** (`.hx-chart__threshold`), not bare
colour; DQ bands reuse `.hx-chart__band-ok/-warn/-fail`; grids are neutral (`--border`); values are directly labelled
where space allows rather than relying on a legend.

- **UX-DS-013** (P1, test) — Every chart MUST ship a toggleable **data-table alternative** (`.hx-chart__table` via
  `.hx-chart__toggle`) carrying the same numbers; every metric MUST show its denominator, window and plane (UX-P4); a
  band or series MUST never be distinguished by colour alone (direct label or pattern); and a chart MUST render
  legibly in both themes from tokens (cross-ref UX-A11Y-004).

## 8. Internationalisation and time-zone display

- **UX-DS-015** (P1, inspection) — Every user-visible string MUST be externalisable (no concatenated sentence fragments
  that assume English word order); multilingual object labels MUST carry a `lang` attribute per label (SKOS `@lang`), so
  a screen reader switches pronunciation; the layout MUST tolerate ±40 % string growth without clipping; and the UI MUST
  support a logical-property layout able to mirror for RTL without hard-coded left/right.
- **UX-DS-016** (P0, inspection) — Every timestamp MUST state **which time it is** — valid, known, recorded or event —
  via `HX.format.time(iso, which)`, rendering ISO 8601 in `--font-mono` plus a relative hint (e.g.
  "2026-10-17T09:14Z · 3 h ago"). Stored time is UTC (`Z`, per CONVENTIONS §4); the display time zone MUST be the
  viewer's local zone with the zone shown, and the as-of control's two axes MUST never be conflated (valid vs
  transaction time, UX-DS-005). Durations and SLA clocks MUST show the unit.

## 9. Performance budgets

- **UX-DS-017** (P1, analysis) — The prototype MUST have **no runtime dependencies** (no framework, no CDN script beyond
  the Google-Fonts stylesheet) so a page's shell is interactive without a build step. Per page: first render MUST reach
  interactive in ≤ 1 s on a mid-tier laptop from local assets; `app.js` and `app.css` MUST each stay a single file;
  long lists (`.hx-datatable`) MUST virtualise beyond ~200 rows; the graph canvas MUST cap at 150 nodes with clustering
  (UX-GRAPH-003); and `HX.data.load` MUST cache fixtures in memory so navigation re-renders without re-fetching.

## 10. Maintainability of the UI codebase

- **UX-DS-018** (P1, inspection) — The codebase MUST stay legible and safe to change: (a) all visual constants live as
  `:root` tokens — adding a theme means editing tokens, not components; (b) one runtime namespace `window.HX` with the
  documented surface (`HX.mount`, `HX.nav`, `HX.role`, `HX.asof`, `HX.data`, `HX.chip`, `HX.diff`, `HX.evidence`,
  `HX.keys`, `HX.theme`, `HX.density`, …), and page code calls it rather than re-implementing shell behaviour; (c) verbs
  are **never** hard-coded role→action — the UI renders `HX.role.verbs(type, object)` from the PDP stub (UX-P1,
  UX-ROLE-001); (d) `app.js` MUST pass `node --check` and every `data/*.json` MUST parse (UX-00 §10); (e) nothing but
  viewer preferences (role, theme, density, inspector width, saved views, recent ids) is persisted client-side, every
  read/write of `localStorage` wrapped in try/catch so a blocked store never breaks a page; (f) no result payload and no
  PHI is ever stored client-side (AC-17).

## 11. WCAG 2.2 AA checklist

| SC | Level | Where enforced |
|----|-------|----------------|
| 1.4.3 Contrast (min) / 1.4.11 Non-text contrast | AA | token contrast pass UX-DS-002 |
| 1.4.1 Use of colour | A | icon + text on every chip (UX-DS-003, UX-DS-006) |
| 1.4.10 Reflow / 1.4.4 Resize text | AA | 390 px no h-scroll, 200 % zoom (UX-DS-009) |
| 2.1.1 Keyboard / 2.1.2 No trap | A | full keyboard map §12 (UX-A11Y-002) |
| 2.2.2 Pause, stop, hide | A | reduced motion, no infinite loops (UX-DS-012) |
| 2.4.7 Focus visible / **2.4.11 Focus not obscured** | AA | 2 px ring; sticky bars offset so focus is never hidden (UX-A11Y-003) |
| **2.5.7 Dragging movements** | AA | every drag has a typed / "Move to…" / arrow alternative (UX-A11Y-002) |
| **2.5.8 Target size (min)** | AA | ≥ 24×24 px targets (UX-DS-010) |
| 3.2.3 Consistent navigation / **3.2.6 Consistent help** | A/AA | eight areas identical per role; `?` + keymap same place every page (UX-A11Y-003) |
| **3.3.7 Redundant entry / 3.3.8 Accessible authentication** | A/AA | forms never re-ask known data; no cognitive-test auth |
| 4.1.2 Name, role, value / 4.1.3 Status messages | A/AA | ARIA roles on palette/tree/slider; live regions (UX-A11Y-004/005) |

Items in **bold** are the WCAG 2.2 additions this console treats as launch-blocking.

- **UX-A11Y-001** (P0, analysis) — The UI MUST meet WCAG 2.2 AA; axe-core MUST report **zero serious or critical**
  violations on each of the eleven pages in both themes (CI gate, UX-00 §10).
- **UX-A11Y-002** (P0, test) — Every list, tree, timeline, chart and canvas MUST be fully keyboard-operable; every drag
  (as-of thumbs, tree reparent, inspector splitter, canvas lasso) MUST have a non-drag alternative (2.5.7); all
  interactive targets MUST be ≥ 24×24 px (2.5.8).
- **UX-A11Y-003** (P0, inspection) — The focus ring MUST be `2px solid var(--focus)` + `1px` offset, never removed and
  never obscured by sticky bars (2.4.11); help (`?`) and the keyboard map MUST appear in the same place on every page
  (3.2.6); forms MUST NOT re-ask data the platform already holds (3.3.7) and MUST NOT gate on a cognitive-test auth
  step (3.3.8).
- **UX-A11Y-004** (P1, test) — Charts MUST ship a data-table alternative; trees and graphs MUST have list equivalents
  with **identical selection** (`v` toggles the twin); timelines MUST be ARIA sliders with value text ("Valid time
  10 March 2026 07:00"); the command palette MUST be a combobox with a grouped listbox and a live result count.
- **UX-A11Y-005** (P1, inspection) — Live regions MUST announce state transitions, as-of changes and toasts **politely**
  and kill-switch and break-glass entry **assertively** (`HX.live`); reduced motion MUST be honoured (opacity only); the
  UI MUST be usable at 200 % zoom without loss of content or function.
- **UX-A11Y-006** (P1, test) — Single-key shortcuts MUST be suspended while any input, textarea or contenteditable is
  focused and MUST be globally disableable in Preferences (`HX.keys.enabled(false)`, showing the shortcuts-off banner);
  the `?` sheet MUST list every shortcut, and every button hint MUST show its key.
- **UX-A11Y-007** (P1, inspection) — Multilingual labels MUST carry `lang`; status MUST always be icon + text + colour;
  the synthetic-data ribbon and any PHI masking MUST be announced, not silent (cross-ref UX-DS-015/016, UX-P5).

## 12. Keyboard map (normative)

Single keys are suspended inside form fields and globally disableable (UX-A11Y-006). `Esc` closes non-destructive sheets
only.

| Scope | Keys |
|-------|------|
| Global | `Ctrl/Cmd-K` palette · `/` focus search · `?` keyboard map · `[` toggle rail · `]` toggle inspector · `t` as-of control · `Shift-T` return to now · `c` compare · `y` copy IRI · `.` command on row · `Esc` close |
| Go-to chords | `g h` Home · `g i` Inbox · `g c` Review · `g o` Ontology · `g s` Sources & Flow · `g q` Quality & Warehouse · `g a` Agents & Operations · `g v` Governance · `g l` Lineage |
| Lists | `j`/`k` move · `Enter`/`o` open · `Shift-Enter` peek · `Space`/`x` select · `Shift-x` select group · `Ctrl-Enter` copy URL |
| Object tabs | `e` Evidence · `h` History · `p` Provenance · `i` Impact · `1`–`8` tabs in canonical order |
| Review | `a` approve · `A` approve w/ comment · `r` reject · `v` request revision · `x` defer · `V` veto (privacy) · `n`/`p` next/prev diff change · `]`/`[` next/prev item |
| Tree | `←`/`→` collapse/expand · `Shift+↑/↓` range · `b` set broader · "Move to…" command |
| As-of control | `←`/`→` step hour/day/release · `Shift` ×10 · `Home`/`End` · `L` toggle link · `Shift-drag` unlinks |
| Canvas | `Tab` cycle nodes · `Enter` open · `Space` expand · arrows follow edges · `v` toggle list twin |

## 13. Verification map

| Requirement | Method | Where verified |
|-------------|--------|----------------|
| UX-DS-001, 002, 005 | analysis | token sourcing, dual dark guards, contrast CI pass, reserved time tokens (`app.css`) |
| UX-DS-003, 006, 007 | inspection | icon+text chips, stage/group never colour-alone, PHI lock glyph |
| UX-DS-004, 009, 017 | test/analysis | shared assets, responsive collapse, 390 px no h-scroll, no deps, perf budgets |
| UX-DS-008, 010, 011, 012 | inspection/analysis | typography, component library + naming, iconography, reduced motion |
| UX-DS-013 | test | chart data-table alternative, denominators, no colour-alone |
| UX-DS-014, 018 | inspection | empty/error states name the CR, `HX` namespace, verbs from PDP, `node --check`, no PHI persisted |
| UX-DS-015, 016 | inspection | i18n externalisation + `lang`, which-time labelling, UTC store / local display |
| UX-A11Y-001..006 | analysis/test/inspection | axe clean both themes, keyboard operability + non-drag alternatives, focus ring, ARIA twins/sliders/combobox, live regions, shortcut suspension |
| UX-A11Y-007 | inspection | `lang` on labels, icon+text+colour, PHI/synthetic announced |

Full acceptance criteria (AC-02 themes/contrast, AC-16 keyboard/axe, AC-17 synthetic-data/no-PHI, AC-18 content rules)
ship with `prototype/` (see `prototype/README.md`); this document's `UX-DS-*` and `UX-A11Y-*` requirements are the
design-system and accessibility subset those criteria exercise.
