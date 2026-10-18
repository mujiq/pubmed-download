---
title: Change-Request Review — The 30-Second Decision
module: 16-user-experience
doc_id: UX-04
version: 0.1.0
status: draft
owners: [Product Design, Design Systems, Data Platform, Governance]
depends_on: [VIS-00, ARC-01, UX-00, UX-02, AGT-03, GOV-01, DQ-00, ONT-00, TKG-01]
last_updated: 2026-10-17
---

# Change-Request Review — The 30-Second Decision

UX-00 fixes the principles and UX-02 fixes the shell, object model and canonical template. This document is the
**normative home of the `UX-CR-*` requirements**: the review surface through which the nine human roles (VIS-00 §5)
*dispose* of what the twenty agents (ARC-01 §5) *propose*. Its one measurable promise, from UX-P1, is that a
**medium-risk change request is decidable in about thirty seconds** — the diff, the evaluator verdict, the privacy
verdict, the gate state and the impact are on one screen, and nothing needed to decide is more than one click or one
scroll from the diff. `UX-CR-001..005` carry the meanings fixed in UX-00 §9 and are restated normatively here; the
surface is extended from `UX-CR-006`. This document renders the Change Request schema and gates (AGT-03), the PHI
policy (GOV-01), the DQ bands (DQ-00) and the five time modes (TKG-01 §9); it never forks them. The reference
implementation is [`prototype/change-requests.html`](prototype/change-requests.html) on the shared `HX` runtime.

Requirements use RFC 2119 keywords, a priority (`P0` launch-blocking, `P1` first release, `P2` later) and a
verification method, per `CONVENTIONS.md` §2.

## 1. The 30-second review flow

```mermaid
flowchart LR
  Q["Queue / Inbox<br/>Needs you · filtered · SLA-sorted"]:::q
  T["Triage<br/>open one CR / offer batch"]:::q
  V["Verdict strip<br/>score+κ · privacy · checks n/n · gates"]:::r
  D["Diff viewer<br/>native renderer for the payload type"]:::r
  E["Evidence panel<br/>7 sections · agent rationale quarantined"]:::r
  DEC{"DecisionBar<br/>risk-proportional friction"}:::d
  A["Approve"]:::ok
  R["Request revision / Reject"]:::warn
  X["Veto (privacy)"]:::veto
  AUD["AuditEvent + calibration example<br/>who · gate · env · time"]:::a
  Q --> T --> V --> D --> E --> DEC
  DEC --> A & R & X --> AUD
  classDef q fill:#eef,stroke:#88a; classDef r fill:#fff,stroke:#999;
  classDef d fill:#ffe,stroke:#aa6; classDef ok fill:#efe,stroke:#6a6;
  classDef warn fill:#fef3d6,stroke:#c90; classDef veto fill:#fbe7f3,stroke:#a3177a;
  classDef a fill:#eee,stroke:#999;
```

The flow is the same for every CR type (`taxonomy`, `vocabulary`, `mapping`, `edw_ddl`, `topic_schema`, …); only the
diff renderer, the gate set and the required approver role change. Progressive disclosure (UX-P6) collapses the flow
for a low-risk auto-approved CR to a one-line digest receipt in the FYI lane, and expands it for a `phi_affecting` CR
into a two-gate review with the privacy verdict pinned above the diff.

- **UX-CR-006** (P0, demo) — A reviewer MUST be able to reach a decision on the seeded medium-risk taxonomy CR
  `CR-20261017-01J9ZK` without leaving the review page: the diff, Verdict strip, Evidence panel, GateTracker and
  DecisionBar MUST all be reachable by scroll or a single keystroke (`e` Evidence, `h` History, `]` next item), and no
  modal MUST be required to see any of them.

## 2. Review surface layout

The Review area (`/cr`) uses a three-region layout inside the HX shell: a **list pane** (320 px: saved view selector,
typed-filter bar, `j`/`k` rows, `[`/`]` previous/next item), the **main pane** (the CR canonical page of UX-02 §4), and
the right **inspector** pinned to the Evidence panel. The **DecisionBar** is sticky-bottom (56 px) per UX-02 §4
region E.

```mermaid
flowchart LR
  L["List pane 320px<br/>saved views · typed filters · j/k"]:::l
  M["Main pane · CR page<br/>ObjectHeader · Verdict strip · GateTracker · Diff (default tab)"]:::m
  I["Inspector 360–480px<br/>Evidence panel (default)"]:::i
  DB["DecisionBar · sticky bottom · PDP verbs"]:::db
  L --- M --- I
  M --- DB
  classDef l fill:#eef,stroke:#88a; classDef m fill:#fff,stroke:#999;
  classDef i fill:#efe,stroke:#6a6; classDef db fill:#ffe,stroke:#aa6;
```

- **UX-CR-007** (P0, inspection) — The queue (`.hx-datatable`) MUST expose columns risk, type, title, proposer +
  autonomy level, evaluator score + κ, privacy, a gates mini-stepper, SLA and age; MUST honour the typed filter grammar
  of UX-02 §5 identically to the palette and saved views (e.g. `type:cr state:evaluated risk:high group:G2` returns the
  expected rows); and MUST support the five seeded saved views (`gate:steward_approval group:G2 state:evaluated`,
  `risk:high state:evaluated`, `gate:ontologist_approval`, `risk:phi_affecting`, `age:>5d state:evaluated`).
- **UX-CR-008** (P1, inspection) — The list MUST be navigable by `j`/`k` with `Enter` to open and `Shift-Enter` to peek,
  and MUST preserve the active `?asOf`/`?asKnownAt` when moving between CRs so a reviewer auditing a correction window
  keeps the lens (UX-02 §3, UX-IA-020).

## 3. Triage

Triage is the queue plus the Inbox **Needs you** lane (UX-02 §6): rows an SLA clock, a gate mini-stepper and the
required approver **role**. When ≥ 3 Needs-you CRs share a steward **and** an artefact family (e.g.
`scheme:encounter-class`), the lane offers **"Review as batch (n)"** (§8). Triage never shows a count badge for At risk
or FYI, and never conveys urgency by colour alone (UX-P4, UX-A11Y-005).

- **UX-CR-009** (P1, inspection) — Each queue and Needs-you row MUST render an `.hx-sla` clock (ring + text) that turns
  `--at-risk` at ≥ 75 % consumed and `--breached` past due, sourced from the CR's `sla.dueAt` and labelled with its
  `sla.source` (e.g. `AGT-ORCH-017`); the SLA state MUST be icon + text, never a bare colour.
- **UX-CR-010** (P1, demo) — A CR whose `dependsOn` is unmet MUST render in the `blocked` state with a lock chip linking
  to the blocking CR, and a CR with a `conflictsWith` or rebase conflict MUST render a reason chip (e.g. "already
  approved by `role.steward_g2`"); neither MUST be silently absent from the queue.

## 4. The CR page — Verdict strip and GateTracker

Below the ObjectHeader the CR page carries a **Verdict strip** (`.hx-verdict-strip`) and the **GateTracker**
(`.hx-gates`). The Verdict strip is the one-glance trust summary (UX-P4): evaluator aggregate score as an
`.hx-score` meter with a threshold tick and the κ note for that CR type, the privacy decision chip, and a checks n/n
summary. The GateTracker renders `gatesRequired` as an ordered mini-stepper, each step `pass` / `pending` / `fail` with
the approver and timestamp in its tooltip, so the reviewer sees which gate is theirs and which remain.

| Region | Source (AGT-03) | Rendered by |
|--------|-----------------|-------------|
| Evaluator score + κ + threshold | `evaluation.aggregateScore`, `kappaForType`, `threshold` | `HX.score` |
| Checks n/n | `evaluation.checks[]` (`cycle`, `orphan`, `phi_scan`, `prompt_injection_scan`, `reasoner`, `shacl_fixtures`, …) | Verdict strip |
| Privacy decision | `privacyReview.decision` (`cleared` / `veto`) | `HX.phiChip` + chip |
| Gates | `gatesRequired[]` vs `approvals[]` | `HX.gates` |

- **UX-CR-001** (P0, demo) — The CR page MUST show the Verdict strip (evaluator score + κ, privacy decision, checks
  n/n), the GateTracker, the diff, the seven-section Evidence panel with agent rationale quarantined as untrusted, and a
  sticky DecisionBar whose verbs come from the PDP set (`HX.role.verbs`), never hard-coded role→action.
- **UX-CR-011** (P0, inspection) — The GateTracker MUST render every entry of `gatesRequired` in schema order with its
  state from `approvals`; the **next** gate (the CR's `nextGate`) MUST be visually distinguished with icon + text, and
  the gate the current principal can satisfy MUST be the one the DecisionBar acts on. A gate already satisfied MUST NOT
  be re-offered.

## 5. Diff viewer

The Diff tab is the default tab on a CR page. `HX.diff` picks a **native renderer** by `payload.format` and falls back
visibly when none exists — never a blank diff (UX-02 §8, `.hx-diff--fallback`).

| `payload.format` | Renderer | Native affordances |
|------------------|----------|--------------------|
| `skos-tree` | SKOS tree diff | before/after broader/narrower, moved nodes highlighted |
| `owl` | three-column | asserted-before · asserted-after · inferred delta + Explanation drawer |
| `shacl` | constraint table | added/removed/changed shapes |
| `ddl` | side-by-side | portable + Teradata toggle, **dry-run panel** (`parse`, `lint`, naming regex, FK closure, grain test, row-count delta, est. duration) |
| `mapping-table` | mapping rows | per-row golden-test results column |
| `valueset` | expansion delta | members added/removed |
| `yaml` / `json` / `schema` / `remediation` | typed text diff | compatibility notes |
| unknown | `.hx-diff--fallback` | visible "Native renderer not yet available — showing Turtle/YAML diff" |

- **UX-CR-012** (P0, demo) — The Diff tab MUST render the native **SKOS tree** diff for `CR-20261017-01J9ZK` and the
  **DDL side-by-side with the dry-run panel** (showing `EDW-ARCH-002 pass`, FK closure, grain test, `rowCountDelta`,
  `estimatedDurationSec`) for the `edw_ddl` CR `CR-20261015-01J7QQ`; the renderer switch MUST offer Native / Text /
  Turtle, and a CR with an unknown `payload.format` MUST show the visible fallback state (**UX-CR-005**).
- **UX-CR-013** (P1, demo) — `n`/`p` MUST move between changes within a diff; inline comments on a diff hunk MUST post to
  the Discussion tab, and untrusted source text in a diff MUST be rendered in the quoted (`--untrusted`) style
  (UX-02 §4.1, UX-18 content rule).

## 6. Evidence panel

The Evidence panel (`HX.evidence`, inspector default, `e` to focus) renders the **fixed seven-section grammar** of
UX-02 §4.1. The agent's own reasoning is **quarantined**: a dotted border and the label "agent text, treat as data", so
a reviewer never mistakes a proposal's narrative for verified evidence (UX-P5, GOV prompt-injection posture).

| Section | Class | Holds (AGT-03 `evidence.*`) |
|---------|-------|-----------------------------|
| Source elements | `--source` | `sourceElements[]` — frequency, facilities, masked |
| Terminology | `--terminology` | `$subsumes` / `$lookup` results, licensed-display-hidden chip |
| Local evidence | `--local` | nomenclature, CDF meaning |
| Statistical | `--statistical` | co-occurrence support, etc. |
| Reasoning | `--reasoning` | reasoner / ELK output |
| Tests | `--tests` | golden-suite pass/total |
| Agent rationale | `--agent` | `agentRationale.text`, **quarantined** |

- **UX-CR-014** (P0, inspection) — The Evidence panel MUST render the seven sections in fixed order; `evidence.agentRationale`
  MUST appear only in the quarantined `--agent` section with its untrusted styling and label, never promoted into another
  section; and a CR with **no** attached evidence MUST render `.hx-evidence--empty` ("No evidence attached — this
  proposal cannot be approved") rather than an empty panel.

## 7. Evaluator score and autonomy-level transparency

Trust is shown, not asserted (UX-P4). Beyond the aggregate score the CR page MUST make the agent's standing legible so
a reviewer can calibrate how much scrutiny a proposal deserves: the **proposer's autonomy level vs ceiling** (ARC-01 §5
L1–L4), **κ for this CR type** (evaluator↔human agreement), and the proposer's **rollback rate**. The rubric (with
per-row anchors and justifications) and the `calibrationSetVersion` MUST be inspectable.

- **UX-CR-002** (P0, test) — For medium/high-risk CRs, **Approve MUST be disabled until the diff has scrolled into view
  and one evidence section is opened**, with a tooltip stating why; the evaluator score section MUST be **collapsed below
  the diff for high-risk CRs** so the diff is read before the score.
- **UX-CR-015** (P1, inspection) — The CR page MUST show the proposer's autonomy level with its ceiling (`.hx-chip--level`
  with the ghost ceiling), κ for the CR type next to the score, and the proposer's rollback rate; a score below the
  type's auto-approve `threshold` (e.g. `0.93 < 0.95`) MUST be marked on the `.hx-score` meter, and a type whose κ has
  fallen below 0.90 MUST surface the "auto-approval suspended" context (cross-linking `/agent/agent.{role}`, AGT-03).
- **UX-CR-016** (P2, demo) — When `evaluation.blindReviewEligible` is true and blind-review is enabled in Preferences for
  the configured share, the evaluator score MUST be hidden until the reviewer records a provisional verdict, then revealed
  with the agreement logged — so human and evaluator judgements stay independently calibratable.

## 8. Privacy guardian verdict

The PHI guardian (`agent.privacy_guardian`, L4 veto, ARC-01 §5) reviews every change for PHI impact. Its verdict is a
first-class region, and for a `phi_affecting` CR the **Privacy tab is pinned above the Diff** (UX-P6, GOV-01).

- **UX-CR-005** (P1, inspection) — A `phi_affecting` CR MUST pin the Privacy tab above the diff; an unknown payload
  format MUST render the visible native-renderer fallback state.
- **UX-CR-017** (P0, demo) — The Privacy region MUST render `privacyReview.decision`; on a **veto** it MUST show the
  cited rules (e.g. `GOV-PHI-004`, `GOV-PHI-001`), the `phiFieldsTouched` with their PHI class, and the guardian's
  plain-language verdict, as seeded on `CR-20261014-01J6PH` ("Carry Patient.identifier (SSN) into
  `edw_g1_party.dim_patient`"). A vetoed CR MUST render in the `vetoed` state and MUST NOT offer Approve to any role; a
  Privacy Officer MUST be able to **acknowledge** the veto, never silently override it.
- **UX-CR-018** (P0, inspection) — No `phi_affecting` CR MUST reach an applied state without a `privacy_officer_approval`
  gate present in `gatesRequired`; the console MUST NOT expose any control that reveals the masked PHI values a privacy
  CR concerns (UX-P5; reveal lives only in the break-glass remediation workspace, UX-DQ-003).

## 9. Decision actions and risk-proportional friction

The DecisionBar renders only the verbs the PDP returns for the real principal (`HX.decisionBar(container, verbs,
onDecide)`): **Approve** (`a`), **Approve with comment** (`A`), **Request revision** (`v`), **Reject** (`r`), **Defer**
(`x`), **Escalate**, and **Veto** (`V`, privacy role only). Friction scales with risk (UX-P6): low-risk verbs commit
directly with an undo toast; comment-bearing and destructive verbs run the PHI guard over the comment, confirm the env,
then open a confirm sheet stating the gate, environment and consequence.

```mermaid
flowchart TB
  CLICK["verb clicked"]:::s
  COMMENT{"comment required?<br/>(revision/reject/veto/escalate)"}:::d
  SCAN["HX.phiScan(comment)"]:::s
  BLOCK["blocked: PHI-like text"]:::x
  ENV["HX.env.confirmWrite<br/>(first write in prod)"]:::s
  SHEET["confirm sheet<br/>gate · env · consequence"]:::s
  REC["onDecide → AuditEvent<br/>+ labelled calibration example"]:::a
  CLICK --> COMMENT
  COMMENT -->|yes| SCAN --> ENV
  SCAN -->|fail| BLOCK
  COMMENT -->|no| ENV
  ENV --> SHEET --> REC
  classDef s fill:#fff,stroke:#999; classDef d fill:#ffe,stroke:#aa6;
  classDef x fill:#fde8e8,stroke:#b42318; classDef a fill:#eee,stroke:#999;
```

- **UX-CR-003** (P0, test) — The phi_scan guard MUST block a comment containing a PHI-shaped string (e.g.
  "SSN 123-45-6789") and re-enable submission after removal; the guard MUST name the finding kind and MUST run before any
  comment is stored.
- **UX-CR-019** (P0, inspection) — A destructive or gated verb MUST open a confirm sheet naming the gate, the
  environment (upper-cased) and the consequence ("this becomes a labelled calibration example" / "cannot be silently
  undone"); the first write per session in `prod` MUST additionally confirm (UX-IA-006); nothing MUST execute silently.
- **UX-CR-020** (P0, demo) — A recorded decision MUST animate the CR state chip to its new state (`approved` /
  `in_revision` / `rejected` / `vetoed` / `applied` / `rolled_back`), announce via a polite live region ("CR-… approved;
  n remaining in queue") and advance the saved-view cursor to the next item; a reversible pre-commit action MUST instead
  show the 8-second undo toast (UX-IA-026).
- **UX-CR-021** (P1, inspection) — When a kill switch covers the CR's scope (AGT-03, UX-OPS-003), the DecisionBar MUST
  remain visible but note "no applied transitions for this scope"; the reviewer may still record a verdict, which queues
  until the switch is released.

## 10. Batch review

When several low-variance CRs share a steward and artefact family, the **BatchApprovalTray** (`.hx-batch-tray`) lets one
decision cover many without hiding the outliers.

- **UX-CR-004** (P1, demo) — The batch tray MUST merge evidence across ≥ 2 selected CRs, **exclude** a failing-check or
  high-risk CR with a stated reason ("failing check", "risk high"), **pin outliers**, require a **typed count** for
  batches > 10, and produce a **receipt with one `approvals[]` entry per CR**.
- **UX-CR-022** (P1, inspection) — The batch tray MUST never include a `phi_affecting` CR in a bulk approve, and `Space`
  MUST toggle selection with `Shift-x` selecting a group; the merged-evidence view MUST show the union of source,
  terminology and test evidence so a batch is approved on its combined evidence, not on faith.

## 11. Releases, rollbacks and audit trail

The Review area also holds releases and rollbacks (UX-02 §1 area 3). A release page (`/release/{artefact}/{version}`)
shows gate-metric tiles, breaking flags, migration notes, the redirect map and quorum chips, with a **Rollback flow**
whose "what-will-happen" preview precedes a 10-minute cancel window (cross-linked to Agent Operations, UX-OPS).

- **UX-CR-023** (P1, demo) — A release page MUST render its `state` (`released` / `candidate` / `rolled_back`) as icon +
  text, show gate metrics vs thresholds, and gate Rollback behind a preview of its downstream effect; the seeded
  `topic:hc.kg.assertion.v1 schema v5` rolled-back release MUST render the rollback in its History.
- **UX-CR-024** (P0, inspection) — Every decision, batch approval, veto acknowledgement, release and rollback MUST write
  an AuditEvent (`HX.audit.write`) recording **who** (the principal role), **which gate**, **which environment** and
  **when**, visible in the audit explorer (`settings-governance.html`); the CR page's History and Provenance tabs MUST
  reflect the decision without opening a separate audit module (UX-P2, UX-IA-017). No decision MUST be recordable that
  does not produce such an event.

## 12. Verification map

| Requirement | Method | Where verified |
|-------------|--------|----------------|
| UX-CR-001, 011, 014 | demo/inspection | Verdict strip, GateTracker, seven-section Evidence on `change-requests.html` |
| UX-CR-002, 015, 016 | test/inspection | scroll-and-evidence gate on Approve; collapsed high-risk score; level/κ/rollback transparency; blind review |
| UX-CR-003, 019, 020, 021 | test/demo | phi_scan guard; confirm sheet (gate/env/consequence); state animation + live region; kill-switch note |
| UX-CR-004, 022 | demo/inspection | batch tray: merged evidence, exclusions, typed count > 10, per-CR receipt, no phi_affecting in bulk |
| UX-CR-005, 017, 018 | inspection/demo | Privacy tab pinned; veto verdict on `CR-20261014-01J6PH`; no reveal control; privacy_officer gate |
| UX-CR-006, 007, 008 | demo/inspection | one-screen decidability; queue columns + typed filters + saved views; j/k + as-of carry |
| UX-CR-009, 010 | inspection/demo | SLA clocks; blocked/conflict reason chips |
| UX-CR-012, 013 | demo | native SKOS + DDL dry-run diffs; fallback state; n/p navigation; inline comments |
| UX-CR-023, 024 | demo/inspection | release/rollback pages; AuditEvent on every decision in `settings-governance.html` |

Full acceptance criteria (AC-06, AC-07 for CR review and batch) ship with `prototype/` (see
[`prototype/README.md`](prototype/README.md)); this document's `UX-CR-*` requirements are the review-surface subset
those criteria exercise.
