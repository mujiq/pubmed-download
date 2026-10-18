---
title: Lineage Explorer & Temporal Knowledge-Graph Explorer
module: 16-user-experience
doc_id: UX-06
version: 0.1.0
status: draft
owners: [Product Design, Design Systems, Data Platform, Knowledge Graph Engineering, Governance]
depends_on: [VIS-00, ARC-01, UX-00, UX-02, UX-04, TKG-01, GOV-03, GOV-01, DQ-00, ONT-00, AGT-03]
last_updated: 2026-10-17
---

# Lineage Explorer & Temporal Knowledge-Graph Explorer

UX-02 fixes the shell, the canonical object model and the "one object, one page, all of time" contract (UX-P2). This
document is the **normative home of the `UX-GRAPH-*` requirements**: the two explorers through which any role answers
*where did this fact come from* and *what did we believe, and when*. `UX-GRAPH-001..003` carry the meanings fixed in
UX-00 §9 and are restated normatively here; the surface is extended from `UX-GRAPH-004`.

Both explorers render contracts owned elsewhere — the five time modes and change semantics of TKG-01 §§6, 8–9, the
PROV-O activity chain of GOV-03, the PHI classes of GOV-01, the DQ rule bindings of DQ-00 — and **never fork them**.
The reference implementation is [`prototype/lineage-explorer.html`](prototype/lineage-explorer.html) on the shared `HX`
runtime (`HX.lineage`, `HX.graph`, `HX.timeline`, `HX.asof`), with `data/lineage.json` providing the seeded traces,
neighbourhood graphs and coverage figures.

Requirements use RFC 2119 keywords, a priority (`P0` launch-blocking, `P1` first release, `P2` later) and a
verification method, per `CONVENTIONS.md` §2.

## 1. Two explorers, one surface — and never a Home

The lineage explorer is not an area and not a landing page (UX-02 §1, UX-IA-007, UX-IA-023). It opens **from an
object** — the Impact tab's Lineage ribbon, the `/lineage?from={id}` link, or the universal resolver — and carries the
same `?asOf`/`?asKnownAt` lens the user already held (UX-IA-011, UX-IA-020). It offers two modes over the same object,
plus one deliberate hand-off:

| Mode | What it answers | Component | Opened from |
|------|-----------------|-----------|-------------|
| **Lineage ribbon** | "trace this field end to end" — Cerner → `std` → Canonical (FHIR) → KG → EDW, forwards and backwards | `HX.lineage` (`.hx-lineage-ribbon`) | any object's Impact tab |
| **Neighbourhood** | "what is this concept connected to" — a focus node plus hops | `HX.graph` mode `neighbourhood` (`.hx-graph-canvas` + `.hx-graph-list` twin) | a concept / class / element |
| **Pipeline map** (hand-off) | "how are the topics flowing" — layers raw→ops as columns | lives in `interop-monitor.html` (`/ops/map`), SRE surface | a **link**, never an embedded mode |

```mermaid
flowchart LR
  OBJ["Any object page<br/>Impact tab"]:::o
  RES["/lineage?from={id}<br/>(or resolver when ?from absent)"]:::r
  RIB["Lineage ribbon<br/>5 columns · Up/Downstream"]:::m
  NEI["Neighbourhood canvas<br/>+ synchronised list twin"]:::m
  PM["Pipeline map ↗<br/>interop-monitor.html"]:::x
  OBJ -->|Lineage ribbon| RES --> RIB
  RES --> NEI
  RES -. link, not embed .-> PM
  classDef o fill:#eef,stroke:#88a; classDef r fill:#ffe,stroke:#aa6;
  classDef m fill:#fff,stroke:#999; classDef x fill:#eee,stroke:#999;
```

- **UX-GRAPH-001** (P0, inspection) — The lineage explorer MUST refuse to be a landing page: opened **without** `?from`
  it MUST render a **resolver field** (with worked examples that each carry a `from`), never a canvas and never a Home.
  It MUST be listed under Governance and reachable only from an object's Impact tab or the resolver (UX-IA-007).
- **UX-GRAPH-004** (P0, inspection) — The graph canvas MUST open only as a Lineage ribbon, a concept neighbourhood
  (focus + hops) or the SRE pipeline map; the pipeline map MUST be a **link** to `interop-monitor.html`, not an embedded
  mode, so no explorer view is itself a navigation root (UX-IA-023, UX-00 §8).
- **UX-GRAPH-005** (P1, demo) — An entry banner MUST name the object the explorer was opened from ("Opened from
  {object}") and MUST let the user switch ribbon ⇄ neighbourhood for the **same** object without losing the active
  `?asOf`/`?asKnownAt`; when a mode has no data for the object, the explorer MUST fall back to the available mode and say
  so, never render blank.

## 2. The lineage ribbon — Cerner → FHIR → KG → EDW

The ribbon is a single component (`HX.lineage(el, trace, {direction, asOf})`) used identically on every object's Impact
tab and in the explorer. It lays a trace across **five fixed columns** — **Source · std · Canonical · KG · EDW** — one
hop chip per column (a column may hold more than one hop, e.g. a FHIR publish plus its ConceptMap translation). Each hop
chip carries the **version as it stood at the viewed time**, its PROV-O activity, and the valid/known/recorded
timestamps for that hop; terminology hops (ConceptMap `$translate`) render with a **dashed** edge and a ConceptMap chip;
hops with a DQ rule bound to them carry a DQ badge (DQ-00). The seeded trace `trace-cs72-inpatient` runs
`CODE_VALUE 72^INPATIENT → mapspec cerner-encounter-cdc@v3#e010 → hc.canonical.clinical.encounter.fhir.v1 →
ConceptMap cerner-cs72 → clinical:hasEncounterClass → edw_g2_clinical.dim_encounter_class.code`.

- **UX-GRAPH-002** (P0, demo) — The ribbon MUST render a trace in the five columns **Source · std · Canonical · KG ·
  EDW** with a version chip per hop; MUST toggle **Upstream** (warehouse → source) and **Downstream** (source →
  warehouse) with the **same** component and the same selection; MUST honour the as-of lens (§3); and MUST offer **"Copy
  trace as Markdown"** (`HX.lineage.toMarkdown`) that emits the ordered chain with versions and the active
  as-of/as-known stamps.
- **UX-GRAPH-006** (P1, inspection) — Each hop chip MUST link to the hop object's canonical page (element, mapspec,
  topic, ConceptMap, property, column) carrying the current `?asOf`/`?asKnownAt` (UX-IA-020); terminology hops MUST
  render dashed with a ConceptMap chip; and every hop that a DQ rule evaluates MUST show the rule's badge linking to
  `/rule/{DQR-…}` (e.g. `DQR-VALIDITY-ENCOUNTER-004` on the Canonical column).
- **UX-GRAPH-007** (P1, demo) — Each hop MUST expose, in the inspector, the transformation expression (e.g.
  `PV1-2 / ENCNTR_CLASS_CD → Encounter.class`), the hop's valid/known/recorded times (each labelled which time it is,
  UX-18), the PROV-O activity, confidence and golden-test ids where present; a hop with no recorded activity at the
  viewed time MUST render as a gap, not be omitted silently (UX-IA-014).

## 3. The as-of and as-known-at sliders over lineage

The as-of control (`HX.asof`, UX-02 §3) is the lens for both explorers. Its two lanes are coloured only by the reserved
tokens `--time-valid` and `--time-known` (UX-IA-012). Over a trace, the two axes do **different** things, mirroring
TKG-01 §8:

- **`asOf` (valid time)** chooses *which version of each hop was in force in the world* at that instant. The ribbon
  applies the trace's `asOfVariants` whose boundary is still ahead of `asOf`, so hop versions roll back: at
  `asOf < 2026-09-01` the EDW column hop becomes `dim_encounter_class.encntr_class_cd @ migration 2026.07.0-001` and the
  ConceptMap hop becomes `1.3.0`.
- **`asKnownAt` (transaction time)** chooses *what the platform had recorded* by that instant. A hop whose `recordedAt`
  is after `asKnownAt` had **not yet been asserted**; it MUST render a **"not yet asserted" placeholder**
  (`.hx-placeholder--not-asserted`), not vanish — so an auditor sees that the KG property hop for the observation-unit
  trace did not exist before `2026-09-18T03:02Z`.

The mode label MUST be one of the five TKG-01 §9 names exactly — **Current · As-of (valid) · As-known (transaction) ·
Bitemporal point · Full history** — and the amber time banner names it with a **Return to now** action (UX-IA-011,
UX-IA-025).

```mermaid
flowchart TB
  NOW["Current<br/>asOf=now · asKnownAt=now"]:::c
  ASOF["As-of (valid)<br/>hop versions roll to that validFrom"]:::v
  ASK["As-known (transaction)<br/>hops recorded after asKnownAt → 'not yet asserted'"]:::k
  BI["Bitemporal point<br/>both set → re-adjudication lens"]:::b
  NOW --> ASOF
  NOW --> ASK
  ASOF --> BI
  ASK --> BI
  classDef c fill:#fff,stroke:#999; classDef v fill:#e3ebfb,stroke:#2457c5;
  classDef k fill:#fbeadb,stroke:#c9661a; classDef b fill:#efeafb,stroke:#5b3fb5;
```

- **UX-GRAPH-008** (P0, test) — Moving `asOf` earlier than a trace's `asOfVariant` boundary MUST re-render the affected
  hop chips with their earlier versions (object, version, transform, gaps) and MUST reproduce exactly on reload from the
  URL hash; moving `asKnownAt` before a hop's `recordedAt` MUST render that hop as "not yet asserted", never blank
  (UX-IA-025).
- **UX-GRAPH-009** (P0, analysis) — The two axes MUST be coloured only by `--time-valid` / `--time-known`; the active
  mode MUST be shown as icon + text (never colour alone) and MUST be one of the five TKG-01 §9 names; a changed hop
  under a non-Current lens MUST be marked (e.g. `--time-known` emphasis) so the reviewer sees *which* hops the lens
  altered.
- **UX-GRAPH-010** (P1, inspection) — The sliders MUST be fully keyboard-operable as ARIA sliders with value text
  ("Valid time 1 Sep 2026 00:00"), linked by default and unlinkable by the link toggle or Shift-drag, with typed fields
  and quick picks (now / release / before correction / yesterday) as the **non-drag alternative** (WCAG 2.5.7, UX-A11Y).

## 4. Entity timeline

The entity timeline is the compact `HX.timeline` strip (UX-02 §4 region D, UX-IA-015) rendered for the traced object or
focus node: two lanes — valid time in `--time-valid`, known time in `--time-known` — with version segments and a
`changeKind` glyph at every boundary (new ●, new-version ▶, correction ↺, retraction ✕, merge ⤵, unmerge ⤴, each with a
text label in tooltip and legend) and a decisionTime diamond ◆ where the source carried one (TKG-01 §2). Clicking a
segment sets the as-of control, re-lensing the ribbon and canvas to that moment; it expands to the object's full
History tab (zoom day / month / release).

- **UX-GRAPH-011** (P0, demo) — The entity timeline MUST render both lanes with the six `changeKind` glyphs as icon +
  text, a decisionTime ◆ where present, and retracted segments **struck-through but focusable**; clicking or
  <kbd>Enter</kbd> on a segment MUST set `HX.asof` to that segment's boundary and re-lens both explorers.
- **UX-GRAPH-012** (P1, inspection) — A segment's tooltip MUST name its valid interval, its known interval and the CR
  that produced it (linking `/cr/{id}`), so the timeline is also a provenance index; open intervals MUST render as
  open-ended (no sentinel date), matching TKG-TIME-002.

## 5. Retroactive-correction diff view

The explorer's most governance-critical view compares an object (or a hop) **across transaction time** to show a
retroactive correction — a fact whose *valid* time was left untouched while a later *recorded* belief replaced the
earlier one (TKG-01 §6.2). This is the UI for the TKG-01 §1 clinical worked example: a patient's AF onset recorded as
"January 2026" on 2026-03-02 and corrected to 2025-11-14 on 2026-03-20; "did they have AF on 2025-12-01?" answers
**No** as-known-at 2026-03-10 and **Yes** as-known-at 2026-04-01. The diff is invoked with `?diff=A..B` where A and B
are two `asKnownAt` instants (a transaction-time diff) or two `asOf` instants (a valid-time diff); `HX.diff` renders it
with the A/B headers coloured by the axis being compared (UX-02 §8 `.hx-diff` header rule).

```mermaid
flowchart LR
  A["A · as-known-at 2026-03-10<br/>onset = Jan 2026"]:::a
  B["B · as-known-at 2026-04-01<br/>onset = 2025-11-14 (↺ correction)"]:::b
  Q["Query: AF on 2025-12-01?"]:::q
  A -->|No| Q
  B -->|Yes| Q
  classDef a fill:#fbeadb,stroke:#c9661a; classDef b fill:#fbeadb,stroke:#c9661a;
  classDef q fill:#fff,stroke:#999;
```

- **UX-GRAPH-013** (P0, demo) — A retroactive-correction diff MUST present the **pre-correction** value and the
  **post-correction** value side by side with the `↺ correction` glyph, label which value was *believed when* (the two
  `asKnownAt` stamps), and MUST state that **valid time is unchanged** across the correction (TKG-01 §6.2); the A/B
  headers MUST be coloured by the compared axis using the reserved time tokens (UX-GRAPH-009).
- **UX-GRAPH-014** (P0, inspection) — A **retraction** (changeKind ✕, `entered-in-error`) MUST be shown as a closed
  transaction interval with valid time preserved and MUST link its `healix-time:Retraction` activity; the diff MUST NOT
  present a retraction as a deletion of history (TKG-TIME-007). A **merge/unmerge** (⤵/⤴) MUST surface the survivor and
  the `replacedBy` banner rather than rewriting the subject (TKG-01 §6.4, UX-IA-002).
- **UX-GRAPH-015** (P1, demo) — The correction diff MUST be reachable from a timeline segment boundary ("compare across
  this correction", `c`) and MUST round-trip via the `?diff=A..B` URL; it MUST never reveal a masked or PHI value to do
  so (the AF example uses synthetic `SYN-`/`ZZ-TEST-` identifiers only, UX-P5, AC-17).

## 6. The neighbourhood graph canvas and its list twin

The neighbourhood mode (`HX.graph(el, nodes, edges, {cap:150, mode:'neighbourhood', twinEl})`) draws a focus node plus
its hops: node shape by family (concept, class, property, element, column, rule, value set, external), group fill with
the letter always rendered, a PHI border on PHI-classed nodes, and **inferred edges dashed-light with an "inferred by
ELK" badge** (e.g. the derived `observation-unit disjointWith inpatient`). Pending edges (a proposed broader from an
open CR) render distinctly and link the CR. Every canvas ships a **synchronised list/tree twin** (`.hx-graph-list`) with
identical selection state, toggled by `v`; the twin is the accessible equivalent, not a second-class view.

- **UX-GRAPH-003** (P1, demo) — The graph canvas MUST cap at **150 nodes** with auto-clustering (count badges on
  clusters, a minimap and focus mode beyond the cap) and MUST keep selection in sync with a list/tree twin toggled by
  `v`; selecting a node in either MUST select it in both.
- **UX-GRAPH-016** (P1, inspection) — Inferred edges MUST render dashed-light with an "inferred by ELK" badge and
  pending edges MUST link their CR; a PHI-classed node MUST carry its PHI border and class chip (GOV-01); the canvas MUST
  NOT display any patient-level node or value (no patient appears in `data/lineage.json`; nodes are schema-level — codes,
  concepts, columns, rules).
- **UX-GRAPH-017** (P1, demo) — The canvas MUST be keyboard-operable per UX-A11Y: <kbd>Tab</kbd> cycles nodes,
  <kbd>Enter</kbd> opens or peeks, <kbd>Space</kbd> expands, arrows follow edges; a lasso/multi-select MUST offer
  **Compare**, **Open as list** and **Propose relation** (opening a draft CR with an 8-second undo toast, UX-IA-026),
  and every drag MUST have a non-drag alternative (WCAG 2.5.7).

## 7. Saved views

A lineage or neighbourhood view is fully described by its object, mode, direction, depth and the `?asOf`/`?asKnownAt`
lens — so it is addressable and savable. Saved views use the shared typed filter grammar and the same saved-view store
as the CR queue (UX-02 §5, UX-CR-007); a saved view pins its lens so re-opening it reconstructs the exact as-of /
as-known state. "Share this view" copies the canonical URL with the lens attached (UX-IA-020).

- **UX-GRAPH-018** (P1, demo) — The explorer MUST let a user save the current view (object, mode, direction, depth,
  `asOf`, `asKnownAt`) and reopen it to the **identical** lens and selection; the saved view MUST be expressible as a
  canonical URL that reproduces it on reload, and MUST store only the view descriptor and recent ids client-side — never
  a result payload or any PHI (AC-17, UX-IA-026).
- **UX-GRAPH-019** (P2, demo) — A saved lineage view whose underlying trace gains or loses a hop (a new ConceptMap, a
  pending migration) MUST surface the change as a gap or pending chip on next open rather than silently re-drawing, so a
  shared trace stays legible as it evolves.

## 8. Empty, gap and coverage states

Lineage is itself a measured quantity (lineage coverage %, GOV-03, SLO S19). The explorer MUST make a **missing edge**
as visible as a present one and name the CR type that closes it (UX-IA-024).

- **UX-GRAPH-020** (P0, inspection) — An object with no recorded lineage edges MUST render `.hx-empty` naming the gap
  and the CR type that fills it ("No lineage edges recorded for this object — lineage coverage gap, propose a mapping
  CR"), never a blank canvas; an unbound hop MUST carry a gap chip (e.g. `discharge_disposition_cd` → "Unbound (pending
  CR-CV-BIND)") linking the binding CR (UX-IA-014).
- **UX-GRAPH-021** (P1, inspection) — The ribbon MUST show the trace's coverage figure with its denominator (e.g.
  "coverage 83 %") and, for a `phi:direct` lineage, MUST reflect the 100 % coverage obligation of GOV-03 / SLO S19;
  coverage MUST be shown as a trust value, never asserted without its basis (UX-P4).

## 9. Verification map

| Requirement | Method | Where verified |
|-------------|--------|----------------|
| UX-GRAPH-001, 004, 005 | inspection | landing refusal → resolver; canvas only from an object; pipeline-map hand-off; ribbon⇄neighbourhood switch (`lineage-explorer.html`) |
| UX-GRAPH-002, 006, 007 | demo/inspection | five-column ribbon, Up/Downstream same component, Copy as Markdown, hop links, dashed terminology hops, DQ badges |
| UX-GRAPH-008, 009, 010 | test/analysis/inspection | as-of version roll-back + as-known "not yet asserted"; reserved time tokens + five mode names; ARIA sliders with non-drag alternative |
| UX-GRAPH-011, 012 | demo/inspection | entity timeline lanes, six changeKind glyphs, decision ◆, click-sets-as-of, segment→CR tooltip |
| UX-GRAPH-013, 014, 015 | demo/inspection | retroactive-correction diff (AF onset example), retraction/merge semantics, `?diff=A..B` round-trip, no PHI |
| UX-GRAPH-003, 016, 017 | demo/inspection | 150-node cap + clustering + list twin (`v`); inferred/pending edges; PHI borders; keyboard canvas; lasso actions |
| UX-GRAPH-018, 019 | demo | saved views pin the lens and reproduce via URL; evolving-trace gap/pending chips |
| UX-GRAPH-020, 021 | inspection | empty/gap states name the CR type; coverage with denominator and phi:direct obligation |

Full acceptance criteria (AC-09 for the lineage explorer) ship with `prototype/` (see
[`prototype/README.md`](prototype/README.md)); this document's `UX-GRAPH-*` requirements are the lineage- and
time-explorer subset those criteria exercise.
