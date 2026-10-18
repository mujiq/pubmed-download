---
title: Ontology Workbench & Canonical Concept Page
module: 16-user-experience
doc_id: UX-03
version: 0.1.0
status: draft
owners: [Product Design, Design Systems, Clinical Informatics, Data Platform]
depends_on: [VIS-00, ARC-01, UX-00, UX-01, UX-02, ONT-00, TKG-01, AGT-03, GOV-01, DQ-00, EDW-00]
last_updated: 2026-10-17
---

# Ontology Workbench & Canonical Concept Page

UX-02 is the structural contract (the eight areas, the object model, the fixed page template). This document is the
**ontologist's working surface**: how the Console renders Area 4 (Ontology, `/ontology`) and the semantic objects it
holds — the six-stage pipeline board, concept schemes and their trees, the canonical concept page, the thesaurus
relation board, the Manchester axiom editor, SHACL shapes, the competency-question suite, collisions/merge and the
drift inbox — and how an ontologist **proposes and reviews changes inline** without leaving the object.

It renders contracts owned by module 02 and never forks them: the six Ontology Pipeline stages and their exit criteria
(ONT-PIPE-001, ONT-TAX-001..013, ONT-THS-001..010, ONT-OWL-001..016, ONT-KG-001..013), SKOS/OWL/SHACL semantics, the
competency questions CQ-01..CQ-20 (ONT-OWL-013), and the typed change-request schema and gates (AGT-03). Bitemporal
rendering follows TKG-01 §9; the four DQ bands follow DQ-00. The reference implementations are
`prototype/ontology-workbench.html` (stage board and workbench tools) and `prototype/concept.html` (the canonical page).

Requirements use RFC 2119 keywords with a priority (`P0` launch-blocking, `P1` first release, `P2` later) and a
verification method, per `CONVENTIONS.md` §2. The five requirements **UX-ONT-001..005** are normative in UX-00 §9
(canonical template, as-of control, URL round-trip, stage rail, tree/axiom/diff); this document owns the detail
requirements **UX-ONT-010+** that the `prototype/` build and the AC-04/AC-10 acceptance criteria exercise.

## 1. The pipeline is the spine — stage board and stage rail

Stage (0 Inventory → 1 Controlled Vocabulary → 2 Taxonomy → 3 Thesaurus → 4 Ontology → 5 Knowledge Graph) is a visible
attribute on every semantic object (UX-P3). `/ontology` opens on a **stage board**: one column per stage, each holding
the schemes and modules whose next unit of work sits at that stage, with per-column counts and the queue of
`ontologist_approval` CRs feeding it. Every scheme page carries a **stage rail** across the top (S0..S5) whose each
stage exposes a live **exit-criteria checklist** popover — criteria met / total, each unmet criterion a button that
navigates to the offending concepts — and a **Promote** control that is enabled only when every criterion for the next
stage passes.

```mermaid
flowchart LR
  S0[S0 Inventory<br/>Source Registry · elements]:::s0
  S1[S1 Vocabulary<br/>prefLabel · definition · notation · binding]:::s1
  S2[S2 Taxonomy<br/>broader/narrower · facets · polyhierarchy]:::s2
  S3[S3 Thesaurus<br/>related · relation category · mappings]:::s3
  S4[S4 Ontology<br/>OWL classes/properties · axioms · SHACL]:::s4
  S5[S5 Knowledge Graph<br/>instances · RML · bitemporal assertions]:::s5
  S0 -->|ONT-PIPE-001 gate| S1 -->|gate| S2 -->|gate| S3 -->|gate| S4 -->|gate| S5
  classDef s0 fill:#eef,stroke:#888; classDef s1 fill:#e7f3f1,stroke:#4e8e8a;
  classDef s2 fill:#e6f1f4,stroke:#3f8f9e; classDef s3 fill:#e7f0f8,stroke:#3b7fb0;
  classDef s4 fill:#eaeefa,stroke:#4766c4; classDef s5 fill:#eeeafb,stroke:#5a4fcf;
```

Exit criteria are rendered, not invented. Stage 2 → released shows ONT-TAX-001..007 (no `broader` cycle, every concept
a top concept or with a parent, inverse materialisation, no redundant parents PH-5, facet discipline PH-2, bounded
fan-in PH-3, SNOMED subsumption consistency for G2); Stage 3 shows relation-category promotion readiness (ONT-THS-010);
Stage 4 shows reasoner consistency and zero unsatisfiable classes (ONT-OWL-002) and the CQ pass rate (ONT-OWL-013).
When ONT-PIPE-001 blocks stage *n+1* work because a stage *n−1* artefact is still in draft/review, the UI **names that
artefact and links it** — it never greys the Promote button without a reason (UX-P3). Setting the as-of control before a
release replays the earlier rail state (an unmet criterion that has since passed shows as unmet at that time).

> **UX-ONT-010** (P0, demo) — The stage rail MUST show criteria met/total per stage drawn from the module-02 exit
> criteria for that stage (ONT-TAX-001..007 at S2, ONT-THS-010 at S3, ONT-OWL-002/013 at S4); each unmet criterion MUST
> be a control that navigates to the offending concept(s); **Promote** MUST be disabled until every criterion for the
> next stage passes (extends UX-ONT-004).

> **UX-ONT-011** (P0, inspection) — When ONT-PIPE-001 blocks a promotion, the blocked state MUST name the stage *n−1*
> artefact in draft/review and link to it; the UI MUST NOT render a disabled Promote without that reason (UX-P3).

## 2. Scheme page and the concept tree

A concept scheme (`/scheme/{slug}`) is a canonical object (UX-02 §4): breadcrumb, ObjectHeader with stage stepper,
trust strip, temporal strip, the eight fixed tabs, then the type-specific tabs **Tree · Facets · Collisions · Exit
criteria · Exports**. The **Tree** is a virtualised ARIA `tree` with **facet lanes** coloured per the scheme's declared
facets (ONT-TAX-008); a polyhierarchy child appears once under its `healix:primaryBroader` and as a **ghost node** with
a link glyph under each additional parent (PH-1/PH-4); cycle and orphan concepts carry an inline badge (icon + text)
linking to the violating edge (ONT-TAX-001/002). The tree is fully keyboard-operable: `j`/`k` move, `←`/`→`
collapse/expand, `Shift+↑/↓` range-select, `Enter` opens the concept, `b` sets a broader.

Reparenting is the canonical **propose-in-place** gesture: a drag-to-reparent — or the keyboard **"Move to…"**
alternative (2.5.7) — does **not** mutate the tree; it drafts a `CR-TAX-MOVE` change request, runs the polyhierarchy
checks (PH-2 facet discipline, PH-5 redundant-parent) live, and surfaces an **8-second undo toast** with a draft chip
on the moved node. The scheme's **Facets** tab lists each `skos:Collection` with membership counts and flags concepts
missing a membership (ONT-TAX-008). **Collisions** surfaces COL-1..6 and the sibling warnings SIB-1/SIB-2 from the
latest evaluator run; **Exit criteria** mirrors the stage rail as a full page; **Exports** offers the §9 artefacts.

> **UX-ONT-012** (P0, demo) — The scheme Tree MUST be a virtualised ARIA tree with facet lanes, a list-twin equivalent
> with identical selection (UX-A11Y-004), polyhierarchy children shown as ghost nodes under non-primary parents, and
> cycle/orphan badges (icon + text) that link to the offending edge; it MUST be keyboard-operable per UX-00 §8.

> **UX-ONT-013** (P1, demo) — A tree drag-to-reparent and the keyboard "Move to…" alternative MUST draft a `CR-TAX-MOVE`
> (never mutate the tree), MUST run the PH-2/PH-5 checks live and show a failing check inline, and MUST surface an
> 8-second undo toast with a draft chip on the node (extends UX-ONT-005).

## 3. The canonical concept page (reference implementation)

`/concept/{scheme}/{id}` is the reference implementation of the UX-02 §4 template; its type-specific tabs are **Usage**
and **Collisions** (only when flagged). The **Overview** renders the full SKOS record:

- **Labels** — a table by language with role chips (`pref` / `alt` / `hidden`) and `@lang` tags; exactly one
  `skos:prefLabel@en` is enforced with the inline rule id on edit (ONT-CV-001).
- **Definition** — `skos:definition@en` with an "inherited from `sct:448951000`" marker where applicable; scope and
  editorial notes below (ONT-CV-002; PH-1 justification notes shown where polyhierarchy applies).
- **Notations / codes** — each contributing source code as a `.hx-code-chip` (`cerner:71^309308`, `v3:OBSENC`) with
  system prefix muted and a `$lookup` popover (ONT-CV-005); untrusted source displays are quoted (UX-18).
- **Mappings** — a table of `skos:exactMatch`/`closeMatch`/… with match type, confidence and binding status; a licensed
  terminology (SNOMED CT, CPT) shows a **"licensed display hidden"** chip, never the display string (ONT-CV-009).
- **Relations** (tab 2) — `broader`/`narrower` with the `healix:primaryBroader` marked, facet memberships, and
  Stage-3 `skos:related` associations carrying their `healix:relationCategory`.
- **Usage** — where this concept is used: EDW columns (`healix:edwBinding`), FHIR paths, DQ rules, SHACL shapes, Kafka
  topics and an assertion count, each a live cross-link that peeks and navigates (UX-IA-020).

The page is bitemporal end to end (UX-ONT-002): setting valid time before a label correction shows the pre-correction
value with the correction glyph ↺ in the History tab; before first assertion shows `.hx-placeholder--not-asserted`
placeholders; the amber banner names one of the five TKG-01 modes and offers "Return to now". A merged or deprecated
concept remains resolvable and shows the `replacedBy` banner (ONT-KG-012; UX-IA-006). The **Changes** tab narrates the
CR flow that produced the concept; an empty type tab is greyed and names the CR type that would fill it (UX-ONT-001).

> **UX-ONT-014** (P0, inspection) — The concept Overview MUST render labels by language with role and `@lang`,
> definition with any `inheritedFrom` marker, notations as `.hx-code-chip`s with `$lookup`, and mappings with match type
> and confidence; a licensed binding MUST show the "licensed display hidden" chip rather than the display (ONT-CV-009).

> **UX-ONT-015** (P0, demo) — The concept **Usage** tab MUST group and count every dependant (EDW columns, FHIR paths,
> DQ rules, SHACL shapes, topics, assertions) and MUST make each a live cross-link to its canonical page, so a count and
> the objects it represents always agree (UX-IA-023).

## 4. Relation board, axiom editor, OWL diff and derived shapes

The **thesaurus relation board** (`/ontology` → Relation board) is a matrix of `healix:relationCategory` × consistency
score × promotion readiness, built from the relation-type precursor report (ONT-THS-010). A category meeting the Stage-4
thresholds — ≥ 10 occurrences, domain/range consistency, unchanged meaning across ≥ 2 releases, Lead-Ontologist
`promote: true` — is marked **ready**; promoting it drafts an `CR-OWL-AXIOM` that re-expresses the category as a typed
object property (ONT-OWL-011). Categories below threshold stay `skos:related`.

The **Manchester axiom editor** (`.hx-axiom-editor`) is a code field whose autocomplete offers **released IRIs only**
(never draft terms), with a debounced **live ELK panel** reporting profile (OWL 2 EL/DL), consistency, new inferences
and unsatisfiable-class count (ONT-OWL-001/002). Submitting drafts a `CR-OWL-AXIOM`. The **OWL diff** (`.hx-diff--owl`)
renders **three columns — asserted before · asserted after · inferred delta** — with an **Explanation drawer** that
shows the reasoner justification for any inferred or unsatisfiable result (ONT-OWL-016). Alongside it, the **derived
preview** (`.hx-derived-preview`) shows the SHACL shapes and DQ rules that would be regenerated from the changed axioms
(ONT-OWL-006, `healix:derivedFromAxiom`), so the ontologist sees the downstream SHACL/DQ consequence before approving.

> **UX-ONT-016** (P1, demo) — The relation board MUST render category × consistency × promotion-readiness from the
> ONT-THS-010 report and MUST mark a category **ready** only when all Stage-4 promotion criteria pass; promoting MUST
> draft a `CR-OWL-AXIOM`, never mutate the ontology (ONT-OWL-011).

> **UX-ONT-017** (P0, demo) — The Manchester axiom editor MUST autocomplete released IRIs only and MUST show a live ELK
> verdict (profile, consistency, new inferences, unsatisfiable count); submitting MUST draft a `CR-OWL-AXIOM`; the OWL
> diff MUST render three columns with an Explanation drawer (extends UX-ONT-005; ONT-OWL-001/002/016).

> **UX-ONT-018** (P1, inspection) — The axiom editor and OWL-diff MUST show the derived SHACL shapes and DQ rules that
> would be regenerated from the changed axioms (`healix:derivedFromAxiom`), so the SHACL/DQ consequence is visible before
> approval (ONT-OWL-006).

## 5. Competency questions (CQ-01..CQ-20)

The competency question page (`/cq/CQ-07`, `.hx-cq-page`) is the ontology's acceptance surface. The suite table
(`.hx-cq-table`) lists CQ-01..CQ-20 with group and pass/fail/changed status; each CQ page shows its stored SPARQL, a
**Run** against the fixture graph, an **Explain** that renders which axioms and shapes the answer depended on, and
**Compare runs** (a diff of two runs, A/B coloured by the temporal axis when the runs are at different as-of moments).
A release-candidate gate shows the suite pass rate against the ONT-OWL-013 threshold (≥ 90 %; CQ-01..CQ-20 mandatory in
the first release). CQ-19 ("which EDW columns are bound to a property whose range scheme changed") and CQ-20 ("which DQ
rules derive from an axiom that no longer exists") feed the architect and steward impact lists directly.

> **UX-ONT-019** (P1, demo) — The CQ page MUST render CQ-01..CQ-20 with per-CQ Run, Explain and Compare-runs; a
> release-candidate view MUST show the suite pass rate against the ONT-OWL-013 ≥ 90 % threshold with pass/fail/changed
> status (icon + text + colour).

## 6. Collisions and merge

**Collisions** (COL-1..6) and the sibling warnings SIB-1/SIB-2 surface on the scheme page and on a concept when flagged.
The **merge view** (`.hx-merge-view`) shows the candidate concepts side by side as full concept cards, offers a
**survivor swap** (choosing which IRI survives), and renders the **redirect map** (every notation, mapping and relation
that moves to the survivor) and the **downstream impact** (EDW columns, DQ rules, topics). Confirming drafts a merge CR;
on apply the merged IRI is retained with `healix:mergedInto` and no historical assertion is deleted or rewritten
(ONT-KG-012) — the merged concept stays resolvable with the `replacedBy` banner (UX-IA-006).

> **UX-ONT-020** (P1, demo) — The merge view MUST present both concepts as full cards, support survivor swap, and render
> the redirect map and downstream impact before confirmation; applying a merge MUST retain the merged IRI with
> `healix:mergedInto` and MUST leave the merged concept resolvable with a `replacedBy` banner (ONT-KG-012).

## 7. Drift inbox and proposing changes inline

The **drift inbox** (`.hx-drift-inbox`) lists new Cerner `CODE_VALUE`s and Oracle lookups observed by `agent.inventory`
that have **no covering concept** (ONT-KG-007). Each row shows the code, `CODE_SET`/`LOOKUP_TYPE`, 7-day frequency
(masked, aggregate only) and facilities, and a one-click **"propose concept"** that drafts a `CR-CV-ADD`. Across the
workbench, every authoring gesture is a **propose-in-place** draft, never a direct mutation: set-broader drafts
`CR-TAX-ADD-BROADER`, reparent drafts `CR-TAX-MOVE`, an axiom drafts `CR-OWL-AXIOM`, a shape change drafts
`CR-SHACL-CONSTRAINT`, a binding drafts `CR-CV-BIND`. A draft CR can be **reviewed inline** from the object — the Diff,
Evidence (seven-section grammar), evaluator verdict and gates render in the right inspector — and the full review is one
click to `change-requests.html` (UX-CR-001). Agent-drafted definitions always carry `healix:definitionSource =
agent-draft` and are never auto-approved regardless of evaluator score (ONT-CV-010); agent rationale is quarantined as
untrusted (UX-P4).

> **UX-ONT-021** (P1, demo) — The drift inbox MUST list uncovered source codes with masked aggregate frequency and a
> one-click "propose concept" that drafts a `CR-CV-ADD` (ONT-KG-007); nothing patient-level MUST be shown.

> **UX-ONT-022** (P0, inspection) — Every authoring gesture in the workbench MUST draft a typed change request
> (`CR-CV-ADD`, `CR-CV-BIND`, `CR-TAX-ADD-BROADER`, `CR-TAX-MOVE`, `CR-OWL-AXIOM`, `CR-SHACL-CONSTRAINT`) and MUST NOT
> mutate the object directly; a draft MUST be reviewable inline in the inspector with its diff, evidence and gates, and
> agent-drafted content MUST be badged and never auto-approved (ONT-CV-010; UX-CR-001).

## 8. Bulk operations, import/export and version diff

**Bulk operations.** Multi-selecting concepts in the tree or a scheme list offers a bulk action (bulk re-parent, bulk
bind, bulk deprecate, bulk add-facet-membership) that drafts **one change request per affected concept** grouped as a
family, routed to the BatchApprovalTray (UX-CR-004). A bulk set larger than 10 requires a typed count to confirm, and
completion produces a receipt with one entry per drafted CR, so a bulk edit is as auditable as a single one.

**Import / export.** The scheme **Exports** tab offers the module-02 artefacts: FHIR R4 `CodeSystem`
(`content = complete`) and the binding set as a `ConceptMap`, round-trippable without loss of prefLabel, definition,
notation or status (ONT-CV-013), plus SKOS Turtle and the OWL module. Exports for a consumer **without** a terminology
licence omit the licensed binding (ONT-CV-009). Import drafts a review batch rather than loading silently; imported
concepts enter at Stage 1 and must walk the pipeline.

**Version diff.** Two versions or two as-of moments of any scheme or concept compare with `?diff=A..B` and the
`.hx-diff--skos` renderer (labels, notations, relations, bindings), the A/B headers coloured by the temporal axis when
the compared points differ in time (UX-IA-008). The **reasoner report** on an OWL module page renders the last ELK
result per module and the nightly/pre-release HermiT result for `enterprise`, with unsatisfiable classes held at 0
(ONT-OWL-002) and each unsatisfiable or inconsistent result linking to its Explanation drawer (ONT-OWL-016).

> **UX-ONT-023** (P1, demo) — A bulk operation MUST draft one typed CR per affected concept as a family routed to the
> BatchApprovalTray, MUST require a typed count above 10, and MUST produce a receipt with one entry per drafted CR
> (UX-CR-004).

> **UX-ONT-024** (P1, demo) — The scheme Exports tab MUST offer FHIR `CodeSystem` + `ConceptMap` (round-trippable per
> ONT-CV-013), SKOS Turtle and the OWL module; an export for a consumer without the licence MUST omit the licensed
> binding (ONT-CV-009); import MUST draft a review batch and enter concepts at Stage 1, never load silently.

> **UX-ONT-025** (P1, demo) — Scheme and concept version diff MUST use `?diff=A..B` with the `.hx-diff--skos` renderer
> and A/B headers coloured by `--time-valid`/`--time-known` when the compared points differ in time; the OWL-module
> reasoner report MUST render the latest ELK/HermiT verdict with unsatisfiable classes = 0 and link each to its
> Explanation drawer (ONT-OWL-002/016).

## 9. Verification map

| Requirement(s) | Method | Where verified |
|----------------|--------|----------------|
| UX-ONT-010, 011 | demo | `ontology-workbench.html` stage rail: criteria met/total, violation click-through, Promote gating, ONT-PIPE-001 blocked state names the artefact (AC-10) |
| UX-ONT-012, 013 | demo | `ontology-workbench.html` tree: facet lanes, ghost poly nodes, cycle/orphan badges, keyboard, drag/"Move to…" → `CR-TAX-MOVE` + undo toast (AC-10) |
| UX-ONT-014, 015 | inspection/demo | `concept.html` Overview SKOS record, code chips + `$lookup`, licensed-hidden chip, Usage cross-links (AC-04) |
| UX-ONT-016, 017, 018 | demo | `ontology-workbench.html` relation board, Manchester editor + live ELK, three-column OWL diff + Explanation drawer, derived SHACL/DQ preview (AC-10) |
| UX-ONT-019 | demo | `ontology-workbench.html` CQ page: Run / Explain / Compare runs; suite pass rate vs ONT-OWL-013 |
| UX-ONT-020 | demo | `ontology-workbench.html` merge view: survivor swap, redirect map, `mergedInto`; `replacedBy` banner on `concept.html` (AC-04) |
| UX-ONT-021, 022 | demo/inspection | drift inbox → `CR-CV-ADD`; every authoring gesture drafts a typed CR reviewable inline (AC-06, AC-10) |
| UX-ONT-023, 024, 025 | demo | bulk family → BatchApprovalTray; Exports (CodeSystem/ConceptMap/SKOS/OWL, licence-aware); `?diff=A..B` SKOS diff; reasoner report |

The five parent requirements UX-ONT-001..005 are verified in UX-00 §9 / AC-04 and AC-10; the UX-ONT-010+ requirements
above are the workbench- and concept-page detail those acceptance criteria exercise, and they render — never redefine —
the module-02 contracts (ONT-PIPE, ONT-CV, ONT-TAX, ONT-THS, ONT-OWL, ONT-KG) and the AGT-03 change-request schema.
