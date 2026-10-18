---
title: Catalog, Interoperability & Operations
module: 16-user-experience
doc_id: UX-07
version: 0.1.0
status: draft
owners: [Product Design, Design Systems, Data Platform, Integration Engineering, Data Warehouse Architecture, Agent Operations, Platform SRE]
depends_on: [VIS-00, ARC-01, UX-00, UX-02, SRC-00, EDW-00, IOP-00, STR-00, TKG-01, AGT-00, GOV-01, OPS-00, DQ-00]
last_updated: 2026-10-17
---

# Catalog, Interoperability & Operations

UX-00 fixes the principles, UX-02 fixes the shell, object model and canonical page template. This document is the
normative home of the **`UX-CAT-*` and `UX-OPS-*` requirements** for the four operator-facing surfaces of the Console
where people read the state of the platform and the fleet that runs it:

1. the **data catalog** over the nine subject-area groups G1–G9 — sources, data elements, mapping specs, value sets,
   warehouse schemas/tables/columns bound to ontology IRIs, and conformed dimensions (`data-catalog.html`);
2. the **interoperability monitor** — sources, Kafka topics, HL7 v2 / FHIR R4 validation, partner conformance, the
   SLO wall, the pipeline map, connectors, DLQ, replay and incidents (`interop-monitor.html`);
3. **agent operations** — the roster of 20, autonomy ladders, evaluations and calibration, memory, self-healing
   incidents and the kill switch (`agent-ops.html`);
4. **executive KPIs** — the programme goals, the KPI pages and the audited drill-down (`executive.html`).

It renders — never forks — the contracts it depends on: the Source Registry and Mapping Specification (SRC-00), the
EDW model and its bitemporal `hist_*` contract (EDW-00, TKG-01 §9), the Terminology Service and FHIR profiles
(IOP-00), the Topic Catalog and Schema Registry (STR-00), the SLO wall S1–S22 (OPS-00 §3), the agent catalog,
autonomy ladder, evaluator, calibration and kill switch (AGT-00), the PHI and access policy (GOV-01) and the
programme goals G-01..G-06 (VIS-00 §3). The parent requirements **UX-CAT-001..004** and **UX-OPS-001..003** are
normative in UX-00 §9; this document restates each where it bites and owns the **UX-CAT-010+ / UX-OPS-010+** detail
that the `prototype/` build and the AC-11/AC-12/AC-13/AC-14 acceptance criteria exercise.

Requirements use RFC 2119 keywords with a priority (`P0` launch-blocking, `P1` first release, `P2` later) and a
verification method, per `CONVENTIONS.md` §2. Every surface here is **PHI-free by construction** (UX-P5): catalog
profiling shows masked aggregates, DLQ samples are masked, agent memory is masked, and no reveal control exists on any
of these pages — the sole PHI surface is the break-glass remediation workspace of UX-05 §5.

## 1. Data catalog over G1–G9 (`UX-CAT`)

The catalog spans Area 5 (Sources & Flow) and the warehouse half of Area 6 (Quality & Warehouse). Every row is a
canonical object under the UX-02 §5.2 template, so a source, an element, a mapping spec, a value set, a table and a
column each carry the same breadcrumb, ObjectHeader with stage stepper, trust strip with gap chips, temporal strip and
fixed tab order. The reference implementation is `prototype/data-catalog.html`, whose tabs are **Sources · Elements ·
Mapping specs · Value sets · Warehouse**.

```mermaid
flowchart LR
  SRC[Source /source/{id}<br/>Interfaces · Elements · Onboarding 12/12]:::c
  ELE[Element /element/{src}/{path}<br/>phi_class · CODE_SET · profiling masked]:::c
  MAP[Mapping spec /mapspec/{id}@vN<br/>FHIRPath rows · golden results]:::c
  VS[Value set /valueset/{slug}<br/>Expansion · Expansion delta]:::c
  TBL[Table /table/{schema}/{table}<br/>analyst ⇄ architect depth]:::c
  COL[Column /column/…<br/>ontology_property_iri · phi_class · DQ]:::c
  SRC --> ELE --> MAP --> VS
  TBL --> COL
  VS -.binds.-> COL
  classDef c fill:#eef,stroke:#88a;
```

### 1.1 Source registry and the 12-step onboarding wizard

A source page (`/source/{id}`) carries type-specific tabs **Interfaces · Elements · Topics · Profiling · Onboarding**.
The **Onboarding** tab renders the twelve SRC-00 §4.6 steps as a left stepper (`.hx-wizard`), each step showing its
state and its **owning actor** (an agent id or a role) and each being a change request of the right type, resumable
across sessions. The blocked state follows UX-P3: a step that cannot start names the artefact it waits on and links it,
never greying a control without a reason. Steps 4 (PHI classification signed) and 12 (go-live sign-off) are gated on a
human actor (SRC-REG-011).

> **UX-CAT-010** (P0, demo) — The source page MUST render the twelve SRC-00 §4.6 onboarding steps as a resumable
> stepper, each step showing its state, its owning actor and the CR type it drafts, with step 4 (PHI classification)
> and step 12 (go-live) gated on a human (SRC-REG-011; extends UX-CAT-001). A blocked step MUST name and link the
> artefact it waits on, never grey a control silently (UX-P3).

> **UX-CAT-011** (P0, inspection) — Every stewardship picker in the catalog (source, element, table, conformed
> dimension) MUST accept **role ids only** (`role.steward_g2`, …) and MUST reject a typed person name inline with the
> rule id, because stewardship addresses roles, never named persons (GOV-MDM; UX-02 steward-role object). No named
> person MUST be renderable as a steward anywhere in the Console.

### 1.2 Data elements and the PHI-class clock

A data element page (`/element/{source}/{path}`) shows the element identity from the Data Element Catalog: its source
path, its Cerner `CODE_SET` or Oracle Fusion `LOOKUP_TYPE`/flexfield value-set name (ONT-INV-005), its `phi_class`, and
a **masked profiling** panel (null rate, cardinality, top values suppressed) — never a raw example value, because the
catalogue stores none for `store-none` elements (ONT-INV-004). An element whose `phi_class` is `unknown` renders the
transitional chip with its **10-business-day clock** as an `.hx-sla` ring; on expiry it fails closed to `phi:direct`
and raises an issue to `agent.privacy_guardian` (ONT-INV-003), which surfaces in that officer's **At risk** inbox lane.

> **UX-CAT-012** (P0, demo) — The element page MUST show the `CODE_SET`/`LOOKUP_TYPE` (ONT-INV-005), a masked profiling
> panel with no raw example values (ONT-INV-004), and — for `phi_class = unknown` — the 10-business-day clock that fails
> closed to `phi:direct` on expiry (ONT-INV-003); the band and PHI chips MUST be icon + text + colour (UX-DS-003).

### 1.3 Mapping specs and golden tests

A mapping spec (`/mapspec/{id}@v{N}`) is a versioned object with tabs **Elements · Golden tests**. Its editor
(`.hx-mapspec-editor`) is a three-column working surface — source segment/field tree, FHIRPath transform rows, and a
**golden-results** column (`.hx-golden-results`) showing pass/total per row against the versioned golden set. Required-
element coverage and pass rate render against their gates (100 % required coverage, ≥ 99.5 % element pass rate,
SRC-MAP-009), and the panel states that golden payloads are **de-identified or synthetic only** (SRC-MAP-010), so the
editor never shows real PHI. Submitting a spec change drafts a `mapping` CR reviewable in `change-requests.html` with
its golden results in the diff (UX-CR-012); a spec MUST NOT be able to downgrade an element's `phi_class` below the
registry value (SRC-MAP-008), and the editor marks such an edit invalid inline.

> **UX-CAT-013** (P1, demo) — The mapping-spec editor MUST render FHIRPath rows with a per-row golden-results column
> (SRC-MAP-006/009) and MUST state that golden payloads are synthetic/de-identified only (SRC-MAP-010); an edit that
> would downgrade an element's `phi_class` MUST be blocked inline with the rule id (SRC-MAP-008). Applying a change MUST
> draft a `mapping` CR, never mutate the active spec.

### 1.4 Value sets and concept maps

A value set (`/valueset/{slug}`) and ConceptMap (`/conceptmap/{slug}`) carry tabs **Expansion · Expansion delta**. The
Expansion tab renders the paged, intensionally defined members with the stable `expansion.identifier` (IOP-TS-005); the
Expansion delta tab diffs two expansions (members added/removed) with the same `.hx-diff--valueset` renderer the CR
page uses. Members drawn from licensed terminologies (CPT, SNOMED CT in non-member use) render the **"licensed display
hidden"** chip (`.hx-chip--licensed-hidden`) in place of the display string, resolving the code without reproducing
licensed content. Every binding target cross-links to the governing ConceptMap (IOP-TS-009), closing the loop a column's
binding opens in §1.5.

> **UX-CAT-014** (P1, demo) — A value set page MUST render its paged expansion with the stable `expansion.identifier`
> (IOP-TS-005) and an Expansion delta diff; members from licensed code systems MUST render the "licensed display hidden"
> chip rather than the licensed display, and every bound member MUST cross-link its governing ConceptMap (IOP-TS-009).

### 1.5 Warehouse: groups → schemas → tables → columns, conformed dimensions, query templates

The **Warehouse** tab descends G1–G9 → `edw_gN_*` schemas → tables → columns (EDW-00). A table page
(`/table/{schema}/{table}`) opens at **analyst depth** (grain, keys, business definitions, typical joins, the
certification banner reading the DQ band + coverage) and reveals **architect depth** via `.hx-depth-toggle`
(`?depth=architect`): the portable and Teradata DDL, the eleven-column bitemporal contract (EDW-HIST-006, TKG-01 §9),
secure views and migrations. A column page (`/column/…`) shows type, `ontology_property_iri`, `phi_class`, source FHIR
path and DQ status. A table whose certification is revoked (Red/Black band) renders the certification-revoked banner
(DQ-SCORE-009), matching UX-05 §2.

**Conformed dimensions** (Patient, Practitioner, Organization, Location, Date, Payer, Item, Cost Center, Code, Source
System — ARC-01 §4) are marked as such; a change to one shows it is referenced by facts in ≥ 2 groups (EDW-CONF-021) and
requires the DW Architect **and** the owning steward gate (EDW-CONF-020), surfaced as an `edw_ddl` CR.

The **Query templates** tab is the only query surface in the Console — **no notebooks and no NL→SQL** (UX-00 §8). It
exposes a SQL and a SPARQL template with `asOf`/`asKnownAt` and the ontology version **pinned from the as-of control**
(EDW-HIST-010 as-of accessor, IOP-TS-014 TS release pin); changing the as-of control rewrites the bind variables and the
banner names one of the five TKG-01 §9 modes (**Current · As-of (valid) · As-known (transaction) · Bitemporal point ·
Full history**). A **Copy** action yields the pinned query; nothing executes patient-level data in the UI.

An **Unbound** column (no `ontology_property_iri`) renders a `.hx-chip--gap` ("Unbound") and a **"Bind to property"**
action that drafts a `binding` CR against a released ontology property; binding coverage per group is shown against the
G-01 ≥ 95 % target in the trust strip, never as blank space (UX-P4).

> **UX-CAT-015** (P0, demo) — A table page MUST toggle analyst/architect depth (`?depth=`, extends UX-CAT-002); architect
> depth MUST show the portable and Teradata DDL and the eleven-column bitemporal contract (EDW-HIST-006); a conformed
> dimension MUST be marked as referenced by ≥ 2 groups (EDW-CONF-021) and its change MUST require the DW Architect and
> steward gate (EDW-CONF-020).

> **UX-CAT-016** (P0, demo) — The Query templates tab MUST pin `asOf`/`asKnownAt` and the ontology/TS version from the
> as-of control into both the SQL and SPARQL templates (EDW-HIST-010, IOP-TS-014, TKG-01 §9) and MUST expose Copy only;
> the Console MUST NOT provide a notebook or NL→SQL surface anywhere (UX-00 §8; extends UX-CAT-002).

> **UX-CAT-017** (P1, demo) — An Unbound column MUST render a gap chip and a "Bind to property" action that drafts a
> `binding` CR against a released property (extends UX-CAT-003); binding coverage per group MUST be shown against the
> G-01 ≥ 95 % target as a trust-strip value, never blank (UX-P4).

## 2. Interoperability monitor (`UX-OPS`)

`interop-monitor.html` is the SRE and Integration Engineer view of the platform's promises, with tabs **SLOs ·
Pipeline map · Topics · Connectors · DLQ · Replay · Incidents**.

```mermaid
flowchart LR
  SLO[SLO wall S1–S22<br/>sorted by burn rate]:::o --> TOP[Topic /topic/{name}<br/>lag · subjects · consumers]:::o
  TOP --> CON[Connector /connector/{name}<br/>state · DLQ masked · runbook]:::o
  SLO --> INC[Incident /incident/{id}<br/>self-healer · blast-radius budget]:::o
  INC -->|rollback request| CR[Release rollback<br/>change-requests.html]:::r
  classDef o fill:#eef,stroke:#88a; classDef r fill:#fff,stroke:#999;
```

### 2.1 The SLO wall

The **SLOs** tab renders the twenty-two SLOs **S1–S22** (OPS-00 §3) as burn tiles (`.hx-slo-tile`) sorted by **burn
rate**, each showing the SLI vs its target, error-budget remaining, active alert chips and a runbook link
(`/playbook/PBK-…`). The tile conveys health by icon + text + colour (burning / within budget), never colour alone
(UX-DS-003). Each tile names its owning module and the goal it serves (e.g. S4 canonical validity → G-02, S6 consumer
lag → ARC-NFR-004, S13 KG–EDW reconciliation → ADR-07, S19 lineage coverage → GOV-LIN-016, S22 consent propagation →
GOV-PHI-009), and drills to the object the budget burns on (a topic, a connector, a reconciliation job).

> **UX-OPS-010** (P0, demo) — The SLOs tab MUST render S1–S22 (OPS-00 §3) as burn tiles sorted by burn rate, each showing
> SLI vs target, error-budget remaining, alert chips and a runbook link, conveying health by icon + text + colour; each
> tile MUST drill to the object the budget burns on.

### 2.2 The pipeline map

The pipeline map (`/ops/map`) lays the Kafka layers **raw → std → canonical → dq → kg → edw → ops** out as columns, with
each topic a node **sized by throughput** and **coloured by lag band with a text label** (UX-P4). Beyond the node cap
(150) it **auto-clusters** with count badges, and it keeps selection in sync with a **list/tree twin** toggled by `v`
(UX-GRAPH-003 pattern). The map opens from `/ops` or an SLO drill; it is **never a Home landing page** (UX-00 §8).

> **UX-CAT-018** (P1, demo) — The pipeline map MUST render the seven Kafka layers as columns with node size by throughput
> and lag band by colour **and** label, auto-cluster beyond the 150-node cap, and provide a list twin toggled by `v`
> (extends UX-CAT-004); it MUST open only from an object or `/ops`, never as a Home.

### 2.3 Topics, connectors, DLQ and replay

A topic page (`/topic/{name}`) shows its layer, domain, group, `phi_class` (STR-TOP-008), retention, compatibility
(`BACKWARD_TRANSITIVE` for `canonical`/`dq`, STR-SR-004), schema subjects and versions, producers, consumers with
per-group lag, per-partition lag, and CRs correlated by time; a **Schema versions** tab diffs two subject versions with
the schema-compatibility renderer. A connector/processor page (`/connector/{name}`) shows state, lag, watermark, a
**masked** DLQ sample set, the runbook and recent playbook runs — DLQ payloads are masked exactly as everywhere else
(UX-P5). The **Replay** tab is a wizard (`.hx-replay-form`) that pins topic, offset range and schema versions and shows
a dry-run count before any replay (STR-TOP-027).

> **UX-OPS-011** (P1, demo) — A topic page MUST show layer, domain, group, `phi_class` (STR-TOP-008), compatibility
> (STR-SR-004), schema subjects/versions, producers, consumers with per-group and per-partition lag, and time-correlated
> CRs; connector DLQ samples MUST be masked; the Replay wizard MUST pin versions and show a dry-run count before replay.

### 2.4 HL7 v2 / FHIR validation and partner conformance

The monitor renders interoperability conformance as first-class signals, not buried logs: **canonical validity** (S4,
≥ 99.5 % passing the FHIR validator and HEALIX profile, IOP-FHIR-002), **HL7 v2 acknowledgement** health (AA/CA vs
AE/AR, IOP-V2-002/003), **`Provenance` presence** on every canonical version (IOP-FHIR-008), and **partner conformance**
test pass rates per partner endpoint. Each signal links to the offending record class (masked) or the failing
conformance test, and a drop surfaces on the relevant SLO tile.

> **UX-OPS-012** (P1, demo) — The monitor MUST render canonical FHIR validity (S4, IOP-FHIR-002), HL7 v2 ACK health
> (IOP-V2-002/003), `Provenance` presence (IOP-FHIR-008) and per-partner conformance pass rates, each conveyed by icon +
> text + colour and each linking to the masked offending record class or failing test.

### 2.5 Incidents, playbooks and blast-radius

An incident page (`/incident/{id}`) renders a timeline that **merges the alert, the self-healer's actions and human
decisions** (`.hx-incident-timeline`): each self-healer step shows its **ladder level** and the **blast-radius budget
used vs max** in the window (ADR-09, MEM-HEAL-001/002). At L3+ a step awaits a human with **Pause · Take over · Approve
step** controls; S25 (self-healing safety) requires the budget is never exceeded and autonomous `schema`/`data_mutation`
actions are zero, so the UI shows the budget as a hard gauge, not a suggestion.

> **UX-OPS-013** (P0, demo) — An incident page MUST render a timeline merging the alert, self-healer actions (each with
> its ladder level and blast-radius budget used/max) and human decisions, with Pause / Take over / Approve-step controls
> on any step awaiting a human at L3+ (ADR-09; MEM-HEAL-001/002; S25); the blast-radius budget MUST render as a gauge
> that cannot be silently exceeded.

## 3. Agent operations (`UX-OPS`)

`agent-ops.html` keeps autonomy honest. Its fleet board is **20 agent cards** matching the ARC-01 §5 roster ids; each
agent page carries tabs **Manifest · Ladder · Thresholds · Runs · CRs · Evaluations · Memory · Tools**.

```mermaid
flowchart LR
  FLEET[Fleet board · 20 cards<br/>level/ceiling · accept% · rollback% · κ]:::a
  AG[Agent /agent/agent.{role}<br/>Ladder · Thresholds · Runs · Memory]:::a
  EV[Evaluation /evaluation/{id}<br/>rubric anchors · calibration set]:::a
  KS[Kill switch<br/>scope · consequences · co-sign]:::k
  FLEET --> AG --> EV
  FLEET --> KS
  classDef a fill:#eef,stroke:#88a; classDef k fill:#fde,stroke:#c62828,stroke-width:2px;
```

### 3.1 Fleet board and the autonomy ladder

Each fleet card (`.hx-agent-card`) shows the agent's **level vs ceiling** (`.hx-chip--level` with the ghost ceiling,
L1–L4), acceptance %, rollback %, κ per CR type, open CRs vs `max_open_change_requests`, today's budget use, last run,
and whether its demotion triggers are armed. The agent page's **Ladder** tab renders graduation gauges (acceptance
≥ 90 %, rollback ≤ 2 %, agreement ≥ 0.85, days at level, PHI findings = 0) and the `demotion_triggers`, which **demote
automatically** when breached (AGT-SAFE-014). The **Thresholds** tab is a threshold-editor-as-CR: it **allows raising** a
threshold and **disables lowering** with "requires PRD revision" (UX-OPS-002).

> **UX-OPS-014** (P0, inspection) — The fleet board MUST show 20 cards matching the ARC-01 §5 roster ids, each with level
> vs ceiling, acceptance %, rollback %, κ per CR type, open CRs vs `max_open_change_requests`, budget use and armed
> demotion triggers (extends UX-OPS-001); the Ladder tab MUST render the graduation gauges and the `demotion_triggers`
> that demote automatically (AGT-SAFE-014).

> **UX-OPS-015** (P0, demo) — The Thresholds editor MUST allow raising an auto-approve threshold and MUST disable lowering
> it with the reason "requires PRD revision", drafting the change as a CR rather than mutating the manifest (restates
> UX-OPS-002).

### 3.2 Runs, evaluations, calibration and memory

A run page (`/run/{ulid}`) renders the tool-call timeline with **untrusted inputs delimited** (prompt-injection posture),
the prompt-version hash, a trace waterfall, the CRs produced and the budget spent. An evaluation page (`/evaluation/{id}`)
shows the checks, the rubric rows with their **anchors and justifications**, the verdict and the `calibrationSetVersion`.
The **Calibration** dashboard renders weekly **κ per CR type** (S15, Cohen's κ ≥ 0.8; AGT-SAFE-009); a type whose κ has
fallen **below 0.90 suspends its auto-approval** with a badge, and the disagreement drill-down lists the diverging CRs.
The **Memory** tab shows M1/M2 memory **masked** — metadata, decisions and de-identified evidence only, never PHI
(ADR-08) — and agents have no reveal control (UX-P5).

> **UX-OPS-016** (P1, inspection) — The run page MUST delimit untrusted tool inputs and show the prompt-version hash and
> CRs produced; the evaluation page MUST show rubric anchors, justifications and the `calibrationSetVersion`; the
> Calibration dashboard MUST render weekly κ per CR type and mark a type with κ < 0.90 as auto-approval-suspended (S15;
> AGT-SAFE-009). The Memory tab MUST render M1/M2 masked (ADR-08), with no reveal control (UX-P5).

### 3.3 Kill switch, canaries and rollback

The kill-switch panel (`.hx-killswitch`) picks a scope — **platform / environment / agent / CR type** (AGT-SAFE-012) —
states the consequences in plain language, requires a **typed scope confirm** and MFA, and requires **two-person release**
when the trip was privacy-triggered (AGT-SAFE-013). An active switch raises the red banner — "Kill switch active: {scope}
— held by {role} since {time}" — on **every page** via HX state until released (UX-OPS-003). The **Canary** rail renders
candidate → canary_5 → canary_25 → promoted → monitoring with its rollback condition, and a rollback shows the
`.hx-rollback-card` with a **10-minute cancel window** and a what-will-happen preview (ADR-09; S25 rollbacks ≤ 5 min).

> **UX-OPS-017** (P0, demo) — Tripping the kill switch MUST show the consequences, require a typed scope (platform /
> environment / agent / CR type, AGT-SAFE-012) and MFA, raise a banner with scope, holder and since-time on every page
> until released, and require a co-sign on a privacy-triggered release (AGT-SAFE-013; restates UX-OPS-003). A rollback
> MUST render a what-will-happen preview and a 10-minute cancel window.

## 4. Executive KPIs (`UX-OPS`)

`executive.html` renders the Executive home (UX-ROLE-004, owned by the personas doc; rendered — not redefined — here) and
the KPI drill-down. The home is **one screen at 1440×900 without scrolling**: six goal tiles **G-01..G-06** (value vs
target, 90-day trend; **G-06 always shows "unreviewed PHI-affecting changes = 0"**), the group **band map** G1–G9 (four
bands + certification flag), the phase-gate progress, **Decisions waiting on humans** (CR gates > 7 days, steward
queue-age p50 from `decisionsWaiting` — framed as a capacity signal, not a UI fix), the risk register (items ≥ 15), the
monthly DQI attribution and an **agent-written** "What changed this week" narrative badged as such (UX-18 content rule).

A KPI page (`/kpi/{slug}`) shows the definition, the formula, the **lineage to source tables and views** in
`edw_g9_governance`, a history slider via the as-of control, and a by-group breakdown, plus an **"Explain this band"**
prose panel. Every tile drills to the operational page as-of the report month; **Semantic coverage** drills
`/kpi/semantic-coverage` → G5 → the unbound-attribute list → the gate backlog **read-only** (an executive disposes of
nothing). **Nudge** is **rate-limited to 1 per gate per 7 days** and writes an AuditEvent; a snapshot export is a PDF of
the page at a fixed as-of through the masked API only.

> **UX-OPS-018** (P1, demo) — The Executive home MUST fit one screen at 1440×900 without scrolling and MUST render
> G-01..G-06 (with G-06's "unreviewed PHI-affecting changes = 0" companion), the four-band group map, the phase gates and
> Decisions waiting on humans with the steward queue-age p50 (renders UX-ROLE-004; VIS-00 §3).

> **UX-OPS-019** (P1, demo) — A KPI page MUST show the definition, formula, lineage to the `edw_g9_governance` views, an
> as-of history slider and a by-group breakdown with "Explain this band"; **Semantic coverage** MUST drill to G5 and the
> gate backlog **read-only** (executives dispose of nothing, UX-P6).

> **UX-OPS-020** (P1, inspection) — The Nudge action MUST be rate-limited to 1 per gate per 7 days, show the next-allowed
> time when spent, and write an AuditEvent (GOV-LIN); the snapshot export MUST render the page at a fixed as-of through
> the masked API only, carrying no PHI.

## 5. Verification map

| Requirement(s) | Method | Where verified |
|----------------|--------|----------------|
| UX-CAT-010, 011 | demo/inspection | `data-catalog.html` source Onboarding (12 steps, owner + CR type, step 4/12 human-gated); stewardship pickers reject a typed person name (AC-11) |
| UX-CAT-012 | demo | element page `CODE_SET`/`LOOKUP_TYPE`, masked profiling, `phi_class = unknown` 10-business-day clock |
| UX-CAT-013, 014 | demo | mapping-spec editor with golden results + synthetic-only notice + `phi_class` downgrade block; value-set expansion + delta + licensed-hidden chip |
| UX-CAT-015, 016, 017 | demo | table analyst/architect depth, bitemporal contract, conformed-dimension gate; Query templates pinning as-of + ontology version (no notebooks/NL→SQL); Unbound gap chip → binding CR (AC-11) |
| UX-CAT-018 | demo | `interop-monitor.html` pipeline map: layers as columns, size by throughput, lag band colour + label, clustering, list twin `v` (AC-12) |
| UX-OPS-010, 011, 012, 013 | demo | SLO wall S1–S22 by burn rate; topic/connector pages with masked DLQ + replay dry-run; FHIR/HL7 v2 conformance signals; incident timeline with blast-radius budget + Pause/Take over/Approve (AC-12) |
| UX-OPS-014, 015, 016 | inspection/demo | `agent-ops.html` 20 fleet cards + ladder gauges + demotion triggers; threshold editor raise-only; run/evaluation/calibration (κ < 0.90 suspension); masked memory (AC-13) |
| UX-OPS-017 | demo | kill switch (typed scope, consequences, MFA, banner on every page, privacy co-sign); canary rail; rollback 10-min cancel window (AC-13) |
| UX-OPS-018, 019, 020 | demo/inspection | `executive.html` one-screen scorecard with G-06 companion; KPI drill-down to read-only gate backlog; rate-limited audited Nudge; masked snapshot export (AC-14) |

The parent requirements UX-CAT-001..004 and UX-OPS-001..003 are verified in UX-00 §9 / AC-11..14; the UX-CAT-010+ and
UX-OPS-010+ requirements above are the source-, element-, table-, topic-, agent- and KPI-level detail those criteria
exercise, and they render — never redefine — the SRC-00, EDW-00, IOP-00, STR-00, OPS-00, AGT-00, GOV-01 and TKG-01
contracts.
