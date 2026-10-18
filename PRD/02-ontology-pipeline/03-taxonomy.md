---
title: Stage 2 — Taxonomy
module: 02-ontology-pipeline
doc_id: ONT-03
version: 0.1.0
status: draft
owners: [Enterprise Taxonomist, Group Stewards (G1–G9)]
depends_on: [ONT-02, ARC-01]
last_updated: 2026-10-17
---

# Stage 2 — Taxonomy

## 1. Purpose

Stage 2 arranges released Stage 1 concepts into **hierarchies** using `skos:broader` / `skos:narrower`, organised by
**facets**, with `skos:hasTopConcept` roots per scheme. A taxonomy makes the enterprise's categories navigable and
gives Stage 4 its subsumption backbone: every `skos:broader` that survives review is a candidate `rdfs:subClassOf`
(for class-like concepts) or a value-set hierarchy (for code-like concepts).

Stage 2 asserts hierarchy only. It does not assert associative relations (Stage 3) or typed properties (Stage 4).

## 2. Hierarchy construction per group

Each group has one or more **primary taxonomies** (scheme hierarchies rooted at top concepts) and borrows from the
external hierarchies named. Depth guideline: **3–7 levels** for navigational taxonomies; deeper only where mirroring an
external hierarchy (SNOMED CT, ICD-10-CM chapters → categories → codes, COA trees).

| Group | Primary taxonomies (scheme slugs) | Facets | External alignment | Typical depth |
|-------|-----------------------------------|--------|--------------------|---------------|
| G1 Party & Identity | `party-role` (Patient, Practitioner, Employee, Supplier, Payer, Guarantor…), `identifier-type` (MRN, FIN, NPI, EIN, DEA, Supplier#, Person Number), `organization-type`, `location-type` | By party kind (person/org), by relationship, by jurisdiction | FHIR `Patient/Practitioner/Organization/Location` families; HL7 `v2-0203` identifier types; SNOMED CT ‹Person› 125676002, ‹Occupation› 14679004 | 3–4 |
| G2 Clinical Care | `encounter-class`, `encounter-type`, `admit-source`, `discharge-disposition`, `condition-category`, `observation-category`, `lab-test-panel`, `medication-class`, `allergy-category`, `procedure-category`, `specimen-type`, `document-type`, `order-catalog` | Clinical domain (cardiology, oncology…), event lifecycle (ordered → performed → resulted), body system, care setting | SNOMED CT hierarchies: ‹Clinical finding› 404684003, ‹Procedure› 71388002, ‹Specimen› 123038009, ‹Pharmaceutical/biologic product› 373873005, ‹Body structure› 123037004; LOINC class/system/component axes; ICD-10-CM chapters; FHIR resource families (Encounter, Condition, Observation, MedicationRequest…) | 4–7 |
| G3 Revenue Cycle & Finance | `account-type`, `charge-category`, `claim-type`, `adjustment-reason` (CARC/RARC), `payer-class`, `coverage-type`, `gl-natural-account`, `gl-cost-center`, `gl-balancing-entity`, `ap-invoice-type`, `ar-transaction-class`, `budget-category`, `fixed-asset-category` | Financial statement line, responsibility centre, revenue vs expense, payer vs patient liability | Oracle Fusion COA trees (natural account hierarchy, cost-centre tree); X12 CARC/RARC groups; FHIR `Account/Claim/Invoice`; NUBC revenue codes | 3–6 |
| G4 Supply Chain & Operations | `item-category` (UNSPSC-aligned), `uom`, `po-document-type`, `supplier-category`, `inventory-transaction-type`, `asset-category`, `bed-type`, `room-type`, `appointment-type`, `transport-mode` | Product family, clinical use (implant, pharmaceutical, consumable), stocking location, service line | UNSPSC segment→family→class→commodity; GS1 GPC; FHIR `SupplyDelivery/Device/Location/Appointment`; Oracle Fusion item catalog categories | 4 (UNSPSC) |
| G5 Workforce | `job-family`, `position-type`, `assignment-status`, `credential-type`, `licence-type`, `competency-category`, `shift-type`, `absence-type`, `learning-category` | Clinical vs non-clinical, employment type, regulatory requirement | NUCC taxonomy (practitioner specialties); SOC/BLS occupation codes; Oracle HCM job/position/grade trees; FHIR `PractitionerRole/Practitioner.qualification` | 3–4 |
| G6 Population Health & Quality | `measure-domain` (eCQM, HEDIS, MIPS), `registry-type` (diabetes, CHF, oncology), `risk-model`, `care-gap-type`, `sdoh-domain` (housing, food, transport…), `program-type` | Measure steward, condition cohort, SDOH domain | NQF/CMS measure taxonomy; Gravity Project SDOH domains (LOINC/SNOMED); FHIR `Measure/MeasureReport` | 3 |
| G7 Research & Analytics | `study-type`, `study-phase`, `consent-category`, `cohort-type`, `feature-group`, `model-type` | Interventional vs observational, data-use category | ClinicalTrials.gov study types; FHIR `ResearchStudy/ResearchSubject/Consent`; OBI (optional) | 3 |
| G8 Reference & Terminology | `code-system-family`, `value-set-purpose`, `calendar-type`, `geography` (country → state → county → ZIP3), `currency`, `service-line`, `org-hierarchy` | Standard body, geography level | ISO 3166, ISO 4217, FIPS, UCUM; FHIR `CodeSystem/ValueSet/ConceptMap` | 3–5 |
| G9 Metadata, Quality & Governance | `source-system-type`, `data-classification` (module 11 PHI taxonomy, GOV-01 §3), `dq-dimension` (completeness, validity, consistency, timeliness, uniqueness, accuracy), `dq-severity`, `issue-type`, `consent-directive-type`, `access-policy-type`, `audit-event-type` | Governance concern | DAMA DQ dimensions; FHIR `AuditEvent/Consent/Provenance`; DCAT | 2–3 |

### 2.1 Facets

A facet is a mutually exclusive, collectively exhaustive principle of division applied to one scheme. Facets are
modelled as `skos:Collection` instances (`healix:Facet ⊑ skos:Collection`) with `skos:member` concepts and a
`healix:facetOf` link to the scheme. Rules:

- Every scheme with > 20 concepts MUST declare ≥ 1 facet.
- A concept's `skos:broader` parent MUST share its facet (hierarchy does not cross facets); cross-facet relations are
  associative and belong to Stage 3.
- Facets are *not* `skos:Concept`s and MUST NOT appear in `skos:broader` chains (they are organisational, not semantic).

Example (`observation-category`): facets *Observation domain* (vital-signs, laboratory, imaging, social-history,
survey, exam, procedure) and *Result nature* (quantitative, ordinal, nominal, narrative). "Body temperature" has
`skos:broader` → "Vital signs" (domain facet) and is a member of the "Quantitative" facet collection, but it is not
`broader`-linked to "Quantitative".

## 3. Polyhierarchy rules

Polyhierarchy (a concept with ≥ 2 `skos:broader`) is permitted but constrained, because every surviving `broader` may
become a subclass axiom in Stage 4 and multiple inheritance multiplies the axioms the reasoner must check.

| Rule | Detail |
|------|--------|
| PH-1 Justification | Each additional `skos:broader` MUST carry a `skos:editorialNote` naming the facet or external hierarchy that justifies it. |
| PH-2 Facet discipline | All parents of a concept MUST belong to the same facet, or the concept MUST be a leaf (no narrowers) whose parents come from different facets — never both. |
| PH-3 Maximum fan-in | ≤ 3 `skos:broader` per concept; exceptions require taxonomist approval and are listed in the release notes. |
| PH-4 External mirror | Where the taxonomy mirrors SNOMED CT (which is heavily polyhierarchical) the HEALIX scheme MUST pick one *primary* parent (`healix:primaryBroader`) and keep the others as `skos:broader` only if they are within the scheme's declared facet scope. |
| PH-5 No redundant parents | A concept MUST NOT have a `skos:broader` that is itself an ancestor of another of its `skos:broader` parents (transitive redundancy). |
| PH-6 Code hierarchies | Code-like schemes derived from source code sets (Cerner `CODE_SET`, Oracle lookups) are monohierarchical; the hierarchy is a steward-authored grouping, not a source property. |

## 4. Depth and breadth guidelines

| Guideline | Target | Rationale |
|-----------|--------|-----------|
| Depth (navigational schemes) | 3–7 levels from top concept | Human browsability; shallow enough for faceted UI |
| Depth (external mirrors) | As the external hierarchy, but truncated at the level the enterprise uses (e.g. ICD-10-CM at category + used subcategories; UNSPSC to commodity) | Avoid importing millions of unused nodes |
| Breadth | 2–25 siblings under a parent; > 40 triggers a split review | Long sibling lists signal a missing intermediate level or a facet |
| Top concepts per scheme | 1–12 | Each `skos:hasTopConcept` is a facet-level entry point |
| Leaves | Every leaf MUST be a concept actually observed in data (Stage 0 evidence) or required by an external binding; speculative leaves are not created |

## 5. Alignment with SNOMED CT and FHIR resource families

### 5.1 SNOMED CT

For G2 schemes the taxonomist agent computes, for each HEALIX concept with an `exactMatch` to SNOMED CT, the SNOMED
transitive ancestors (via TS `$subsumes` / ECL `>> <sctid>`) and checks that the HEALIX `skos:broader` chain is
**consistent** with SNOMED subsumption: if HEALIX says A `broader` B and both map to SNOMED, then SNOMED MUST say
`B >> A` or the pair MUST be annotated `healix:alignmentException` with a reason (common cause: HEALIX groups by care
setting, SNOMED by morphology). HEALIX does not import SNOMED CT's hierarchy wholesale; it borrows subsumption to
validate its own.

### 5.2 FHIR resource families

Class-like concepts in G1–G7 are organised to mirror the FHIR R4 resource grouping that module 06 uses for
`hc.canonical.<domain>.<resource>` topics: *Individuals* (Patient, Practitioner, PractitionerRole, RelatedPerson,
Person), *Entities* (Organization, Location, Device, Substance), *Clinical summary* (Condition, Procedure,
AllergyIntolerance, FamilyMemberHistory), *Diagnostics* (Observation, DiagnosticReport, Specimen, ImagingStudy),
*Medications* (MedicationRequest, MedicationDispense, MedicationAdministration, Immunization), *Care provision*
(CarePlan, ServiceRequest, Goal), *Workflow* (Encounter, Appointment, Schedule, Slot, Task), *Financial* (Account,
Coverage, Claim, ClaimResponse, Invoice, ChargeItem, PaymentReconciliation), and the HEALIX-ERP extension family
(GLJournal, PurchaseOrder, Position, InventoryBalance). A HEALIX top concept per family makes the taxonomy's shape
predictable to integration engineers and lets Stage 4 derive one OWL class per FHIR resource with a known parent.

## 6. Validation checks (`agent.taxonomist` + `agent.evaluator`)

| Check | Rule | Severity |
|-------|------|----------|
| CYC-1 Cycle | `skos:broader+` MUST be acyclic (SPARQL property-path check `?c skos:broader+ ?c`) | Blocker |
| ORP-1 Orphan | Every concept in a released scheme that is not a top concept MUST have ≥ 1 `skos:broader` | Blocker |
| TOP-1 Top concept | Every `skos:hasTopConcept` MUST have no `skos:broader` within the scheme and MUST have `skos:topConceptOf` | Blocker |
| INV-1 Inverse integrity | `skos:broader`/`skos:narrower` MUST be materialised as inverses in the release artefact | Blocker |
| SIB-1 Sibling label overlap | Sibling prefLabels with normalised Jaro–Winkler ≥ 0.93 or sharing all content words | Warning → steward |
| SIB-2 Sibling definition overlap | Sibling definitions with Jaccard ≥ 0.8 | Warning |
| FAC-1 Facet crossing | `skos:broader` across facets (ONT-TAX-005) | Blocker |
| PH-* | Polyhierarchy rules §3 | Blocker (PH-2, PH-5), Warning (PH-3) |
| DEP-1 Depth | Chain depth > 7 in a navigational scheme | Warning |
| BRD-1 Breadth | > 40 siblings | Warning |
| EXT-1 SNOMED consistency | §5.1 inconsistency without `alignmentException` | Blocker for G2 |
| EXT-2 FHIR family | Class-like concept not reachable from a FHIR-family top concept | Warning |
| LEAF-1 Evidence | Leaf with no Stage 0 evidence and no external binding | Warning |
| DEPR-1 Deprecated parent | Active concept whose only `skos:broader` is deprecated | Blocker |

## 7. Agent behaviour

`agent.taxonomist` (L2) proposes hierarchies in batches per scheme:

1. Seed candidate parents from (a) external subsumption (SNOMED `$subsumes`, LOINC class, UNSPSC code structure,
   Oracle Fusion trees exported via `GL_SEG_VAL_HIER_CF`/tree flattening, Cerner `CODE_VALUE_GROUP`), (b) label and
   definition embeddings clustered against existing parents, (c) steward-supplied facet skeletons.
2. Run the §6 checks locally; discard proposals failing blockers.
3. Emit `CR-TAX-ADD-BROADER`, `CR-TAX-MOVE`, `CR-TAX-SPLIT`, `CR-TAX-FACET` change requests with evidence and the
   `primaryBroader` choice.
4. The evaluator re-runs checks on the merged graph (proposal + current release) and scores explanation quality.
   Auto-apply only for `low` risk: adding a leaf under an existing parent in a non-clinical scheme with external
   subsumption agreement; all `MOVE`, `SPLIT` and any G2 change are steward-reviewed.

## 8. Requirements

| ID | Priority | Verification | Requirement |
|----|----------|--------------|-------------|
| **ONT-TAX-001** | P0 | test | A released scheme MUST contain no `skos:broader` cycle (including through `skos:broaderTransitive`). |
| **ONT-TAX-002** | P0 | test | Every concept in a released scheme MUST either be a `skos:topConceptOf` that scheme or have ≥ 1 `skos:broader` to a concept in the same scheme. |
| **ONT-TAX-003** | P0 | test | `skos:broader` and `skos:narrower` MUST be materialised as mutual inverses in every release artefact. |
| **ONT-TAX-004** | P0 | test | A concept MUST NOT have a `skos:broader` that is also a transitive ancestor of another of its `skos:broader` parents (PH-5). |
| **ONT-TAX-005** | P0 | test | No `skos:broader` MAY connect concepts belonging to different facets of the same scheme; facets MUST be `skos:Collection`s and MUST NOT appear in `skos:broader` chains. |
| **ONT-TAX-006** | P0 | test | Every additional `skos:broader` beyond the first MUST carry a `skos:editorialNote` justification and the concept MUST declare exactly one `healix:primaryBroader`. |
| **ONT-TAX-007** | P0 | test | For G2 schemes, every `skos:broader` pair whose members both map `exactMatch` to SNOMED CT MUST be consistent with SNOMED subsumption at the pinned version, or carry `healix:alignmentException` with a reason. |
| **ONT-TAX-008** | P1 | test | Every scheme with more than 20 concepts MUST declare at least one facet, and every concept MUST be a member of exactly one collection per declared facet. |
| **ONT-TAX-009** | P1 | test | Class-like concepts in G1–G7 MUST be reachable via `skos:broader*` from a top concept that corresponds to a FHIR R4 resource family or the HEALIX-ERP extension family. |
| **ONT-TAX-010** | P1 | test | Oracle Fusion chart-of-accounts trees (natural account, cost centre) and Oracle HCM organisation/position trees in scope MUST be loaded as `skos:broader` hierarchies with tree version recorded in `healix:sourceEvidence`. |
| **ONT-TAX-011** | P1 | analysis | Sibling overlap warnings (SIB-1, SIB-2) MUST be resolved (merge, rename or documented distinction) before release; unresolved warnings MUST be listed in release notes. |
| **ONT-TAX-012** | P1 | test | A concept whose only parents are deprecated MUST be re-parented or deprecated in the same change request. |
| **ONT-TAX-013** | P2 | demo | The registry SHOULD render any scheme as a browsable tree with facet filters and show, for each concept, its SNOMED/FHIR alignment status. |

## 9. Exit criteria

A scheme is `released` from Stage 2 when all blocker checks pass, ONT-TAX-001..007 hold, all warnings are resolved or
listed, the alignment report (§5) is accepted by the group steward, and `agent.release_manager` has tagged the
version. Stage 3 operates only on released taxonomies.
