---
title: Personas, Jobs-to-be-Done and Role Homes
module: 16-user-experience
doc_id: UX-01
version: 0.1.0
status: draft
owners: [Product Design, Clinical Informatics, Data Platform]
depends_on: [VIS-00, ARC-01, UX-00, GOV-00, GOV-02, DQ-00, AGT-00]
last_updated: 2026-10-18
---

# Personas, Jobs-to-be-Done and Role Homes

This document is the **demand side** of module 16. UX-00 specifies the shell, the canonical object model and the
design system; this document specifies **for whom** and **to do what**. It names the nine personas of
`00-vision-and-scope.md` §5, states each one's goals, pains, top jobs-to-be-done (JTBD) and success metrics, specifies
the **role home dashboard** each one lands on, and gives the **permissions overview** that governs what every home may
render and offer (the detail lives in module 11). It is the source for the `UX-ROLE-010+` requirements and the
reference `prototype/index.html` renders.

> **Design stance.** The Console is one application with one object model and one set of URLs. A persona changes
> *defaults, order and emphasis* — never the objects or the verbs. A user may hold several roles and gets a tabbed
> Home; **View as** (UX-00 §6) previews any persona's Home as a pure rendering filter and never widens the verb set,
> which always resolves against the real principal via the PDP (GOV-02 §3). Every View-as switch is audited
> (UX-ROLE-002).

## 1. Persona model

Each persona maps to exactly one IdP-derived UI role, one Home route and the module 11 access-control role catalogue
(GOV-02 §3.1) that the PDP decides against.

```mermaid
flowchart LR
  subgraph IDP[IdP groups → UI role]
    R1[role.ontologist]
    R2[role.steward_gN]
    R3[role.integration_engineer]
    R4[role.dw_architect]
    R5[role.analyst]
    R6[role.privacy_officer]
    R7[role.platform_sre]
    R8[role.agent_operator]
    R9[role.executive]
  end
  subgraph HOME[Home route /w/role]
    H1[/w/ontologist]
    H2[/w/steward]
    H3[/w/integration]
    H4[/w/architect]
    H5[/w/analyst]
    H6[/w/privacy]
    H7[/w/sre]
    H8[/w/operator]
    H9[/w/exec]
  end
  PDP{{PDP · OPA/Rego<br/>GOV-02 §3 · verbs, never hard-coded}}
  R1-->H1; R2-->H2; R3-->H3; R4-->H4; R5-->H5; R6-->H6; R7-->H7; R8-->H8; R9-->H9
  HOME --> PDP
```

| # | Persona (VIS-00 §5) | UI role | Home | Primary areas (UX-00 §4) | GOV-02 role mapping |
|---|---------------------|---------|------|--------------------------|---------------------|
| 1 | Ontologist / Taxonomist | `role.ontologist` | `/w/ontologist` | Ontology, Review | `role.ontologist` |
| 2 | Data Steward (clinical, financial, supply, HR) | `role.steward_gN` | `/w/steward` | Quality & Warehouse, Review | `role.steward_gN` |
| 3 | Integration Engineer | `role.integration_engineer` | `/w/integration` | Sources & Flow, Review | `role.integration_engineer` |
| 4 | Data Warehouse Architect | `role.dw_architect` | `/w/architect` | Quality & Warehouse, Review | `role.steward_gN` (+ EDW DDL scope) |
| 5 | Analyst / Data Scientist | `role.analyst` | `/w/analyst` | Quality & Warehouse, Ontology (read) | `role.quality_analyst` / `role.researcher` / `role.*_analyst` |
| 6 | Privacy & Security Officer | `role.privacy_officer` | `/w/privacy` | Governance, Review | `role.privacy_officer` |
| 7 | Platform SRE | `role.platform_sre` | `/w/sre` | Agents & Operations | `role.platform_sre` |
| 8 | Agent Operator | `role.agent_operator` | `/w/operator` | Agents & Operations | `role.agent_operator` |
| 9 | Executive sponsor | `role.executive` | `/w/exec` | Governance (KPIs) | `role.executive` (read, aggregates) |

## 2. The role home dashboard (shared contract)

Every Home except Executive is a **three-band** page built from **one card library** so numbers match across roles; no
Home contains a graph canvas (UX-00 §6, merge-decision correction). The bands are fixed in order:

1. **Needs me** — work for which this principal is the required actor (approver, assigned steward, on-call owner,
   break-glass reviewer, kill-switch co-signer). Each item carries an `SLAClock`; keyboard-actionable rows.
2. **At risk** — owned objects trending toward a breach: SLA ≥ 75 % consumed, DQ band drop, rule in tuning, agent
   demoted, release window approaching.
3. **What changed** — applied/released/rolled-back items in scope and agent digests (rolled up per agent per day).

The card library (one implementation per type, so a number means the same thing everywhere): `.hx-tile--stat`
(value · target · 90-day sparkline), `.hx-card--queue`, `.hx-tile--band` (DQ band **always** paired with coverage),
`.hx-sla`, `.hx-stepper`, `.hx-inbox-row`, `.hx-sparkline`. Per **UX-P4**, every tile shows its denominator, window and
plane and renders a **gap chip** where a value is missing, never blank space; status is icon + text + colour.

The Executive Home is the one exception: a single no-scroll scorecard (§3.9). `prototype/index.html` renders all nine.

## 3. The nine personas

For each persona: **Goals** (what success looks like for them), **Pains** (what the Console must remove), **Top JTBD**
(the jobs the Home is optimised for), **Success metrics** (how we know the Home works, tied to VIS-00 §8 KPIs), and
**Home** (what the three bands render). Metrics are the console's own usage/outcome measures, not the platform KPIs the
tiles display.

### 3.1 Ontologist / Taxonomist — `/w/ontologist`

The custodian of meaning across the six Ontology Pipeline stages. Agents propose vocabulary, hierarchy, relations,
axioms and shapes; the ontologist disposes and guards consistency.

| | |
|---|---|
| **Goals** | Keep every scheme consistent and releasable; promote stages only when exit criteria pass; keep the ontology OWL-DL consistent (VIS-00 G-01). |
| **Pains** | Agent proposals arriving without evidence or reasoner results; silent stage blocks; label collisions and orphan concepts discovered late; new source codes with no covering concept (drift). |
| **Top JTBD** | Review an `ontologist_approval` CR with diff + reasoner verdict in ~30 s; see which exit criterion blocks a promotion and jump to the offending concepts; triage the drift inbox; run the CQ-01..20 suite before a release. |
| **Success metrics** | ontologist_approval queue-age p50 < 3 days; share of CRs decided with evidence opened = 100 %; unsatisfiable classes held at 0; CQ pass rate ≥ target at release. |
| **Home** | *Needs me:* `ontologist_approval` gates grouped by stage with SLAClocks; evaluator "revise" returns. *At risk:* per-scheme exit-criteria health strip, relations approaching the 30-day Stage-4 promotion threshold, label collisions COL-1..6, reasoner warnings, CQ pass/fail, release-candidate gate metrics vs thresholds; **Drift inbox** (new `CODE_VALUE`s / lookups with no covering concept). *What changed:* released ontology versions, auto-applied vocabulary CRs in my schemes, external-terminology syncs pending re-validation. |

### 3.2 Data Steward (clinical G2, financial G3, supply G4, HR G5; also G1, G8) — `/w/steward`

Accountable for data meaning and quality within a subject-area group. Approves mappings and golden-record rules, owns
DQ thresholds, resolves issues and exceptions. A multi-group steward sees one band tile per owned group.

| | |
|---|---|
| **Goals** | Keep the group's composite DQ index in band with full coverage; clear the issue and exception queues within SLA; approve safe mappings quickly and in batches. |
| **Pains** | Breaches discovered downstream; per-record approvals that should have been one batch; unmapped codes stuck between Registered and Proposed; exceptions silently expiring. |
| **Top JTBD** | Approve a family of `steward_approval` CRs as a batch; open an issue, read the steward packet (hashed keys, masked values, small-cell suppression) and choose a remediation action; grant/renew an exception with a second approver. (Resolving an entity-resolution case over duplicate G1 identities is a **planned** steward job, not yet in the Phase-1 prototype — see §2.1 of `02-information-architecture.md`.) |
| **Success metrics** | steward_approval median decision time < 1 min; batch adoption ≥ 40 % of eligible families; DQ issue fix-within-SLA ≥ 95 %; zero expired-unrenewed exceptions. |
| **Home** | *Needs me:* `steward_approval` CRs grouped by artefact family with the **BatchApprovalTray** offer; DQ issues in `steward_review`/escalated by SLA; exceptions awaiting a second approver (an **ER-cases (G1)** lane is planned but not yet built in the Phase-1 prototype). *At risk:* my group's band tile (DQI + coverage, 7/28-day sparkline), breaches by severity, rules in tuning, source-fix requests over SLA, quarantine count/oldest, exceptions expiring ≤ 30 days, unmapped codes Registered→Proposed. *What changed:* applied CRs in my scope (7 d), pack versions applied. |

### 3.3 Integration Engineer — `/w/integration`

Brings a source online and keeps its feeds healthy: HL7 v2 → FHIR mappings, topics, profiles, connectors.

| | |
|---|---|
| **Goals** | Complete the 12-step source onboarding; keep every interface within latency/lag SLOs; map every segment and field so nothing falls to the DLQ. |
| **Pains** | Onboarding steps stalling with no visible owner; schema-compatibility breaks; unmapped Z-segments; connectors failing quietly. |
| **Top JTBD** | Resume an onboarding checklist at step N/12 and see the owning actor and CR type per step; triage a schema-compatibility failure; approve a topic/profile/mapping CR at my gate; inspect feed health per interface. |
| **Success metrics** | onboarding step cycle-time within target; schema-compat failures resolved before release window; DLQ depth per interface within budget; golden-test pass rate ≥ 99 %. |
| **Home** | *Needs me:* onboarding checklists in progress (step N/12 with owning actor), topic/profile/mapping CRs at my gate, schema-compatibility failures, connector errors. *At risk:* feed health per interface (S1 latency, DLQ depth, connector state, consumer lag), unmapped segments/fields by volume, Z-segment findings from `agent.inventory`. *What changed:* topics versioned, profiles released, mapping specs applied, golden-test results. |

### 3.4 Data Warehouse Architect — `/w/architect`

Evolves the EDW subject areas, conformed dimensions and DDL in step with the ontology; guards the bitemporal history
contract and the KG↔EDW reconciliation.

| | |
|---|---|
| **Goals** | Keep binding coverage ≥ 95 % per group (VIS-00 G-01); ship DDL migrations safely in release windows; keep KG and EDW reconciled; honour the `hist_*` contract. |
| **Pains** | Ontology releases landing with unprocessed EDW impact; migrations without a compensating migration; reconciliation breaks found late; dry-runs buried in the CR. |
| **Top JTBD** | Review an `edw_ddl` CR with its dry-run panel (parse/lint/naming/FK/grain) and portable + Teradata dialects; schedule a migration into a release window; act on an ontology release's "range changed" (CQ-19-class) impact list. |
| **Success metrics** | binding coverage per group ≥ 95 %; migrations with a compensating migration = 100 %; reconciliation (S13) breaks cleared within SLA; freshness (S11) in band. |
| **Home** | *Needs me:* `edw_ddl` CRs at `architect_approval` with dry-run status, migrations awaiting a release window, conformed-dimension change proposals. *At risk:* binding coverage per group vs ≥ 95 %, KG↔EDW reconciliation breaks (S13), freshness (S11), `hist_*` contract checks (EDW-HIST-001..004), migrations without a compensating migration. *What changed:* applied migrations, ontology releases with unprocessed EDW impact. |

### 3.5 Analyst / Data Scientist — `/w/analyst`

Consumes governed semantics. Reads the EDW and KG with shared meaning and trusts the DQ scores; does not approve
anything. The Home is **search-first** (hero search), not a queue.

| | |
|---|---|
| **Goals** | Find a table/value set/concept that is certified and in-band for my group; write as-of-correct queries; know when semantics change under me. |
| **Pains** | Not knowing whether a table is trustworthy right now; silent deprecations breaking saved queries; rebuilding as-of logic by hand; no safe way to request a change. |
| **Top JTBD** | Search and open a certified object; copy a Query template with `asOf`/`asKnownAt` and ontology version pinned; see breaches affecting my saved queries; file a plain-language change request that an agent drafts (AGT-ORCH-023). |
| **Success metrics** | time-to-trusted-object < 30 s; share of queries using pinned templates; breach-on-saved-query acknowledged before use; plain-language requests converted to a well-formed CR. |
| **Home** | Search-first hero, then: **Trusted now** (tables/value sets/concepts certified Green/Amber for my groups, `is_certified`); breaches affecting my saved queries; semantic changes I should know (deprecations with `replacedBy`, new value-set members); my saved query templates (SQL/SPARQL with as-of pinned); cookbook of as-of patterns; **Request a change** (plain-language → agent-drafted CR). No approval band. |

### 3.6 Privacy & Security Officer — `/w/privacy`

Holds PHI authority. Reviews every `phi_affecting` change (the guardian verdict is pinned), reviews break-glass
sessions, co-signs kill-switch releases, and proves minimum-necessary (GOV-00, GOV-01, GOV-02).

| | |
|---|---|
| **Goals** | Zero unreviewed PHI-affecting changes (VIS-00 G-06); classify every element within its clock; keep lineage coverage at 100 % for `phi:direct`; review every break-glass within 2 business days (GOV-ACC-012). |
| **Pains** | PHI-affecting CRs without the guardian verdict surfaced; unclassified elements past their 10-business-day clock (ONT-INV-003); break-glass sessions slipping the review SLA; PHI leaking into memory, CR text or traces. |
| **Top JTBD** | Review a `phi_affecting` CR with the guardian verdict and veto; review a break-glass session (who, reason code, entities as tokens, query hashes) and decide Appropriate/Escalate; acknowledge a veto; co-sign a privacy-triggered kill-switch release. |
| **Success metrics** | unreviewed PHI-affecting changes = 0; unclassified-element clock breaches = 0; break-glass reviews within 2 business days = 100 %; PGV veto turnaround within SLA. |
| **Home** | *Needs me:* `phi_affecting` CRs at `privacy_officer_approval` with the guardian verdict pinned; vetoes to acknowledge; break-glass sessions awaiting 2-business-day review; kill-switch releases needing co-sign; exception requests. *At risk:* elements with `phi_class = unknown` and their 10-business-day clock, PGV veto spikes per agent, PHI scan hits (memory, CR text, traces), consent propagation (S22), lineage coverage (S19). *What changed:* policy bundle versions, classification changes, de-identification determinations expiring. |

### 3.7 Platform SRE — `/w/sre`

Operates Kafka, the graph store, the EDW and the agent runtime against SLOs S1–S22. Metadata only; no data access.

| | |
|---|---|
| **Goals** | Keep every SLO in budget; resolve incidents fast with bounded blast radius; keep connectors and consumers healthy; honour freeze windows. |
| **Pains** | Alerts without runbook context; self-healer actions proceeding past safe blast radius without a human; a graph canvas forced onto a landing page. |
| **Top JTBD** | Read the SLO wall sorted by burn rate; approve/pause a self-healer step at L3+ with its blast-radius budget; follow a burn → topic → incident chain; open the pipeline map (one click, never embedded on Home). |
| **Success metrics** | SLO burn-rate alerts actioned before budget exhausted; incident MTTR within target; self-healer steps awaiting human acted within SLA; no Home graph canvas (UX-OPS). |
| **Home** | SLO wall S1–S22 sorted by burn rate with error budget remaining; active alerts with runbook links; self-healer actions in flight (playbook, step, blast-radius budget used, awaiting human at L3+); connector/consumer health; freeze calendar; replay/reconciliation jobs; cost. The pipeline map is one click away (`/ops/map`), never embedded. |

### 3.8 Agent Operator — `/w/operator`

Keeps the 20-agent fleet honest: autonomy ladders, calibration, canaries, kill switch, rollbacks (AGT-00, ARC-01 §5).

| | |
|---|---|
| **Goals** | Keep acceptance high and rollback low per agent; suspend auto-approval when calibration drops; promote canaries safely; trip and release the kill switch cleanly. |
| **Pains** | Not seeing which agent is drifting; rollbacks past their cancel window; κ below 0.90 without auto-approval suspending; veto/revise loops hidden. |
| **Top JTBD** | Scan the fleet board (level vs ceiling, acceptance %, rollback %, κ per CR type, open vs max CRs, budget, demotion triggers); cancel a rollback within its 10-minute window; acknowledge a demotion; promote a canary; trip the kill switch with typed scope. |
| **Success metrics** | per-agent acceptance ≥ graduation target; rollback ≤ ceiling; κ < 0.90 auto-suspends auto-approval = 100 % of cases; kill-switch trips carry scope + holder + since-time on every page. |
| **Home** | Fleet board of 20 agent cards (level vs ceiling, acceptance %, rollback %, κ per CR type, open CRs vs max, budget today, last run, demotion triggers armed). *Needs me:* rollbacks with a 10-minute cancel window, demotions to acknowledge, calibration κ < 0.90 (auto-approval suspended), canary promotions. Kill-switch status by scope with holder. *At risk:* veto spikes, revise loops, DLQ growth after release, prompt-injection flags. *What changed:* manifest CRs, prompt/policy versions, playbooks promoted. |

### 3.9 Executive sponsor — `/w/exec`

Programme health in under a minute, with drill-down that never leaves the product. One screen, **no scroll at
1440×900** (UX-ROLE-016). Read-only, aggregates only.

| | |
|---|---|
| **Goals** | See the six programme goals against target at a glance; see where humans are the bottleneck; drill any tile to the operational page as-of the report month. |
| **Pains** | Dashboards that need scrolling or interpretation; vanity numbers without denominators; no safe way to prompt a stuck gate. |
| **Top JTBD** | Read G-01..G-06 and the G1–G9 band map; open "Decisions waiting on humans"; drill a KPI (e.g. Semantic coverage → `/kpi` → group → gate backlog, read-only); send a rate-limited, audited **Nudge**. |
| **Success metrics** | one-screen fit at 1440×900 (no scroll); G-06 companion "unreviewed PHI-affecting changes" renders and equals 0; Nudge rate-limited to 1 per gate per 7 days and audited; every tile drills to its operational page. |
| **Home** | Six goal stat tiles (G-01..G-06; **G-06** always shows the companion "unreviewed PHI-affecting changes = 0"); group band map G1–G9 (four bands + certification flag); phase-gate progress; **Decisions waiting on humans** (CR gates > 7 days, steward queue-age p50); risk-register items ≥ 15; monthly DQI attribution; a "What changed this week" narrative badged **agent-written**. Every tile drills to the operational page as-of the report month; **Nudge** is rate-limited and audited (`.hx-nudge`). |

## 4. Permissions overview (links module 11)

A Home may **render** anything in the eight areas (items a persona cannot act on appear read-only, never removed —
UX-00 §4, UX-IA-001), but it may **offer a verb** only where the PDP permits it. The Console never hard-codes
role→action; `HX.role.verbs(objectType, object)` renders the verb set from the PDP stub (production: OPA/Rego bundle,
GOV-02 §3), evaluated against the **real principal**, using role + purpose + PHI class + subject-area group + facility
(GOV-ACC-003). A request missing any attribute is denied with a reason code (GOV-ACC-021).

| Concern | Home behaviour | Module 11 contract |
|---------|----------------|--------------------|
| Verb rendering | Verbs come from the PDP verb set for each object; **View as** never widens them | GOV-02 §3, RACI §3 (decision rights by change class §2.2) |
| PHI in the UI | PHI-free by construction; the only reveal is the entitled break-glass remediation workspace from a DQ issue | GOV-ACC-012, DQ-REM-013 |
| Minimum necessary | Hashed keys, masked literals, small-cell suppression (n < 11) on every Home tile that touches instance data | GOV-01 §minimum-necessary; UX-P5 |
| Stewardship targets | Steward pickers accept **roles only** (`role.steward_gN`), never named persons | GOV-02 §3.1; GOV-MDM stewardship |
| Audit | Every View-as switch and every gated verb writes an `AuditEvent` visible in the audit explorer | GOV-ACC-021, GOV-LIN (audit); UX-ROLE-002 |
| Agents | Agents have **no** reveal control because none exists for them; agents never invoke break-glass | GOV-ACC-012; UX-P5 |

The Console's UI role ids map onto the GOV-02 §3.1 role catalogue (e.g. the Analyst persona resolves to
`role.quality_analyst`, `role.researcher` or a group analyst role depending on IdP group, each with its own access-matrix
row). The Home renders the union the principal is entitled to; it never grants access the matrix withholds.

## 5. Requirements

Shell- and home-level requirements `UX-ROLE-001..004` are normative in UX-00 §9 and are not restated here. The
following `UX-ROLE-010+` requirements are the persona- and dashboard-level obligations this document owns. All use RFC
2119 keywords with a priority (`P0` launch-blocking, `P1` first release, `P2` later) and a verification method.

> **UX-ROLE-010** (P0, inspection) — Each of the nine personas of §1 MUST map to exactly one UI role and one Home route
> (`/w/{role}`); a user holding several roles MUST get a tabbed Home, and **View as** MUST render any persona's Home
> without changing the verb set returned by `HX.role.verbs` (ties to UX-ROLE-001).

> **UX-ROLE-011** (P0, inspection) — The eight non-executive Homes MUST render the three bands **Needs me · At risk ·
> What changed** in that fixed order, built from the shared card library of §2; a Home MUST NOT contain a graph canvas.

> **UX-ROLE-012** (P0, test) — Every Home tile and queue card MUST link to a filtered list or a canonical object via the
> typed filter grammar (UX-00 §4.1), so a count and the list it opens always agree.

> **UX-ROLE-013** (P1, inspection) — Each persona's **Needs me** band MUST surface the headline work of its §3 row
> (e.g. Ontologist → `ontologist_approval` gates by stage; Steward → `steward_approval` families with BatchApprovalTray;
> Privacy → `phi_affecting` CRs with the guardian verdict pinned), each row carrying an `SLAClock` and keyboard actions.

> **UX-ROLE-014** (P0, inspection) — The Data Steward Home MUST scope every band to the groups the principal owns; a
> multi-group steward MUST see one band tile (DQI + coverage) per owned group, and the Home MUST NOT show objects
> outside those groups as actionable.

> **UX-ROLE-015** (P1, demo) — The Analyst Home MUST be search-first with no approval band, and its "Trusted now" region
> MUST list only objects certified Green or Amber (`is_certified`) for the principal's groups; a deprecated object MUST
> appear only with its `replacedBy` successor.

> **UX-ROLE-016** (P0, demo) — The Executive Home MUST fit one screen at 1440×900 without scrolling, MUST show
> G-01..G-06 with G-06's "unreviewed PHI-affecting changes = 0" companion, and its **Nudge** MUST be rate-limited to one
> per gate per 7 days and MUST write an `AuditEvent` (ties to UX-ROLE-004).

> **UX-ROLE-017** (P1, demo) — The Privacy Home MUST pin the guardian verdict on each `phi_affecting` item and MUST
> surface a break-glass review lane whose rows carry a 2-business-day `SLAClock` (GOV-ACC-012).

> **UX-ROLE-018** (P1, inspection) — The Agent Operator Home MUST render a fleet board of the 20 agent cards matching the
> ARC-01 §5 roster ids, and MUST present a rollback within its 10-minute cancel window and a κ < 0.90 auto-approval
> suspension as **Needs me** items.

> **UX-ROLE-019** (P1, inspection) — The SRE Home MUST present the SLO wall S1–S22 sorted by burn rate and MUST keep the
> pipeline map one click away (`/ops/map`), never embedded on the Home.

> **UX-ROLE-020** (P0, inspection) — Every Home tile that shows a measure MUST carry its denominator, window and plane
> and MUST render a gap chip where a value is missing, never blank space; status MUST be icon + text + colour (UX-P4,
> UX-DS-003).

> **UX-ROLE-021** (P0, inspection) — A Home MUST render areas the persona cannot act on as read-only rather than
> removing them, and MUST offer a verb only where the PDP permits it for the real principal; stewardship pickers MUST
> reject a typed person name inline (roles only) (GOV-02 §3; UX-IA-001).

> **UX-ROLE-022** (P2, analysis) — Each persona's console success metrics (§3) SHOULD be instrumented so the Home's
> effectiveness (decision time, queue age, batch adoption, time-to-trusted-object) can be measured against the VIS-00 §8
> KPIs it serves.

## 6. Traceability

| Requirement(s) | Verified by |
|----------------|-------------|
| UX-ROLE-010, 011, 014, 021 | `prototype/index.html` renders all nine Homes; inspection against §1–§3; PDP-stub verb test (UX-ROLE-001) |
| UX-ROLE-012 | typed-filter list/count agreement test (UX-IA-004) |
| UX-ROLE-013, 017, 018, 019 | per-Home inspection against the §3 rows and the ARC-01 §5 roster / S1–S22 catalogue |
| UX-ROLE-015 | Analyst Home demo: certified-only "Trusted now", `replacedBy` surfacing |
| UX-ROLE-016 | Executive one-screen fit demo; Nudge rate-limit + AuditEvent (shared with UX-ROLE-004) |
| UX-ROLE-020 | design-system inspection (UX-DS-003) over every Home tile |
| UX-ROLE-022 | analytics instrumentation review (Phase 2) |
| Permissions (§4) | module 11 sign-off (GOV-02 §3, GOV-ACC-003/012/021) per UX-00 §10.6 |
</content>
</invoke>
