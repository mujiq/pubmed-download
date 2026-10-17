---
title: Conceptual Model — Entities, Relationships and Patterns per Group
module: 05-edw-schema
doc_id: EDW-02
version: 0.1.0
status: draft
owners: [Data Warehouse Architecture, Ontologist]
depends_on: [ARC-01, EDW-00, EDW-01, RDM-00, TKG-01]
last_updated: 2026-10-17
---

# Conceptual Model

The conceptual model names the **top-level entities and relationships** per subject-area group, independent of
dimensional or physical choices. Every entity here is a class in the enterprise ontology (module 02) and an entry in
the **Logical Model Register** (module 04); the EDW never introduces an entity that is absent from both. Diagrams are
Mermaid `erDiagram`s; cardinalities follow crow's-foot notation. Attribute lists are intentionally short — full
attributes live in the register and in `03-logical-model.md`.

## 1. How UDMH / HCDM patterns were applied

Module 04 (`RDM-*`) harmonises IBM UDMH and Teradata HCDM into a licence-safe logical model. This module consumes three
**patterns** from that harmonisation, not the vendors' entity definitions (README licensing note):

| Pattern (as harmonised in module 04) | Where it appears here | EDW realisation |
|--------------------------------------|-----------------------|-----------------|
| **Party / Party Role / Party Relationship** — a person or organisation is one *party*; roles (patient, practitioner, employee, guarantor, supplier) are time-bounded assertions; relationships between parties are typed and dated | §2, G1 | `dim_party`, `bridge_party_role`, `bridge_organization_affiliation`, `dim_related_person`; conformed `dim_patient` / `dim_practitioner` / `dim_organization` are role-specific projections of `dim_party` |
| **Event / Event Participant / Event Location** — every clinical, financial or operational occurrence is an event with a type, time, status, participants (with function) and location | §3, G2–G5 | one `fact_*` per event type; `bridge_*_performer` / participant bridges where participants are many; conformed PRC/LOC keys where single |
| **Agreement / Account / Financial Transaction** — coverage, contracts and accounts are agreements; charges, claims, payments and journal lines are transactions against them | G3, G4 | `fact_account`, `fact_coverage`, `dim_supplier_contract`; `fact_charge`, `fact_claim_*`, `fact_payment`, `fact_gl_journal_line` |
| **Product / Item / Location hierarchy** — supplies, drugs, devices and services are *items*; stock positions and movements reference item × location | G4 | conformed `dim_item`; `fact_inventory_*` |
| **Classification / Code** — every coded attribute refers to a concept in a versioned code system, optionally via a value set | G8 | `ref_code_system`, `ref_concept`, `ref_value_set_member`, conformed `dim_code` |
| **Subject-area grouping** — UDMH "business areas" and HCDM "subject areas" harmonised into G1–G9 | whole module | one or more `edw_gN_*` schemas per group |

Where the FHIR R4 resource model (module 06) and the blueprint pattern disagree, **FHIR naming wins for entities that
have a FHIR resource** (Encounter, Condition, Observation, Claim …) and the blueprint pattern wins for structure
(party-role, event participants). The register records both the FHIR resource and the blueprint pattern per entity.

## 2. Party-role pattern (G1)

```mermaid
erDiagram
  PARTY ||--o{ PARTY_ROLE : "plays"
  PARTY ||--o{ PARTY_IDENTIFIER : "is identified by"
  PARTY ||--o{ PARTY_RELATIONSHIP : "from"
  PARTY ||--o{ PARTY_RELATIONSHIP : "to"
  PARTY ||--o| PERSON : "is a"
  PARTY ||--o| ORGANIZATION : "is a"
  PARTY_ROLE ||--o| PATIENT : "patient role"
  PARTY_ROLE ||--o| PRACTITIONER : "practitioner role"
  PARTY_ROLE ||--o| EMPLOYEE : "worker role"
  PARTY_ROLE ||--o| PAYER : "payer role"
  PARTY_ROLE ||--o| SUPPLIER : "supplier role"
  PARTY_ROLE ||--o| GUARANTOR : "guarantor role"
  PRACTITIONER ||--o{ PRACTITIONER_ROLE : "practises as"
  ORGANIZATION ||--o{ PRACTITIONER_ROLE : "at"
  ORGANIZATION ||--o{ ORGANIZATION : "part of"
  ORGANIZATION ||--o{ LOCATION : "manages"
  LOCATION ||--o{ LOCATION : "part of"
  PATIENT ||--o{ SOURCE_PATIENT_RECORD : "golden record for"
  SOURCE_SYSTEM ||--o{ SOURCE_PATIENT_RECORD : "originates"
  PARTY {
    string party_bk
    string party_type "person | organization"
    string empi_golden_iri "module 08 resolved IRI"
  }
  PARTY_ROLE {
    string role_code "patient | practitioner | employee | payer | supplier | guarantor | related-person"
    date valid_from
    date valid_to
  }
  PARTY_IDENTIFIER {
    string identifier_system "MRN namespace, NPI, EIN, Supplier#"
    string identifier_value
    date valid_from
    date valid_to
  }
  SOURCE_PATIENT_RECORD {
    string source_record_id
    decimal match_score
    string resolution_method
  }
```

Rules that follow from the pattern:

- A **person is created once** as a `PARTY` by EMPI resolution (module 08). Becoming a patient, employee or
  practitioner adds a `PARTY_ROLE`, never a second party.
- **Conformed dimensions are role projections.** `dim_patient` = `PARTY ⋈ PARTY_ROLE[patient]` with patient-specific
  attributes; `dim_practitioner` likewise. `dim_party` keeps the shared demographic truth.
- **Identifiers are first-class and dated**, so an MRN that moves between persons after an unmerge keeps its history.
- `SOURCE_PATIENT_RECORD` ↔ `PATIENT` is the EDW mirror of the module-08 `sameAs`/`resolvedTo` link, including the
  method and score, so analysts can audit or re-run resolution.

## 3. Event pattern (G2–G5)

```mermaid
erDiagram
  EVENT ||--o{ EVENT_PARTICIPANT : "has"
  EVENT ||--o| LOCATION : "occurs at"
  EVENT }o--|| EVENT_TYPE : "classified by"
  EVENT }o--|| EVENT_STATUS : "in"
  EVENT }o--o| ENCOUNTER : "in context of"
  EVENT }o--|| PATIENT : "subject"
  EVENT ||--o{ EVENT_CODE : "coded as"
  EVENT ||--o{ EVENT_RELATION : "related to"
  EVENT_PARTICIPANT }o--|| PRACTITIONER : "is"
  EVENT_PARTICIPANT }o--|| PARTICIPANT_FUNCTION : "acts as"
  EVENT_CODE }o--|| CONCEPT : "refers to"
  EVENT {
    string event_bk
    datetime event_start_ts
    datetime event_end_ts
    datetime recorded_ts
    string prov_activity_iri
  }
  EVENT_PARTICIPANT {
    string function_code "attending | performer | author | recorder | verifier"
  }
  EVENT_RELATION {
    string relation_type "basedOn | partOf | reasonReference | replaces"
  }
```

Every G2 clinical fact, G3 financial transaction, G4 operational event and G5 workforce event is a specialisation of
`EVENT`. The dimensional translation (`03-logical-model.md` §3) is mechanical: `EVENT_TYPE`/`EVENT_STATUS` become
small value-set dimensions, single participants become conformed `*_practitioner_sk` columns (role-named, e.g.
`attending_practitioner_sk`), multi-valued participants become `bridge_*_performer`, `EVENT_CODE` becomes `code_sk`
(or a bridge when multi-coded), and `EVENT_RELATION` becomes degenerate `*_bk` columns pointing to the related fact.

## 4. Group conceptual diagrams

### 4.1 G2 — Clinical Care

```mermaid
erDiagram
  PATIENT ||--o{ ENCOUNTER : "has"
  PATIENT ||--o{ EPISODE_OF_CARE : "has"
  EPISODE_OF_CARE ||--o{ ENCOUNTER : "groups"
  ENCOUNTER ||--o{ ENCOUNTER_LOCATION : "moves through"
  ENCOUNTER }o--|| ENCOUNTER_CLASS : "classified by"
  ENCOUNTER }o--|| ORGANIZATION : "service provider"
  ENCOUNTER }o--o| PRACTITIONER : "attending"
  ENCOUNTER ||--o{ ENCOUNTER_DIAGNOSIS : "ranks"
  CONDITION ||--o{ ENCOUNTER_DIAGNOSIS : "ranked in"
  PATIENT ||--o{ CONDITION : "has"
  ENCOUNTER ||--o{ PROCEDURE : "includes"
  PROCEDURE ||--o{ PROCEDURE_PERFORMER : "performed by"
  PRACTITIONER ||--o{ PROCEDURE_PERFORMER : "as"
  ENCOUNTER ||--o{ SERVICE_REQUEST : "places"
  SERVICE_REQUEST ||--o{ OBSERVATION : "results in"
  SERVICE_REQUEST ||--o{ DIAGNOSTIC_REPORT : "results in"
  SERVICE_REQUEST ||--o{ SPECIMEN : "collects"
  SPECIMEN ||--o{ OBSERVATION : "analysed in"
  DIAGNOSTIC_REPORT ||--o{ OBSERVATION : "contains"
  SERVICE_REQUEST ||--o| IMAGING_STUDY : "produces"
  ENCOUNTER ||--o{ MEDICATION_REQUEST : "orders"
  MEDICATION_REQUEST ||--o{ MEDICATION_DISPENSE : "fulfilled by"
  MEDICATION_REQUEST ||--o{ MEDICATION_ADMINISTRATION : "administered as"
  MEDICATION_REQUEST }o--|| ITEM : "drug product"
  MEDICATION_REQUEST ||--o{ MEDICATION_REASON : "because of"
  CONDITION ||--o{ MEDICATION_REASON : "reason"
  PATIENT ||--o{ ALLERGY_INTOLERANCE : "has"
  PATIENT ||--o{ IMMUNIZATION : "received"
  ENCOUNTER ||--o{ CLINICAL_DOCUMENT : "documented by"
  PATIENT ||--o{ DEVICE_USE : "uses"
  OBSERVATION }o--|| CONCEPT : "LOINC code"
  CONDITION }o--|| CONCEPT : "ICD-10-CM / SNOMED"
  PROCEDURE }o--|| CONCEPT : "CPT / ICD-10-PCS"
  OBSERVATION }o--o| UCUM_UNIT : "unit"
  ENCOUNTER_DIAGNOSIS {
    int diagnosis_rank
    string poa_code
  }
  OBSERVATION {
    decimal value_numeric
    string value_text
    string interpretation_code
  }
```

Key design decisions: the encounter is the **context hub** but not a mandatory parent (ambulatory meds, problem-list
conditions and patient-reported observations exist without one); diagnosis ranking is an attribute of the
encounter–condition association, not of the condition; medication lifecycle is three events linked by the request;
observations are **component-level** (a blood pressure panel is two observation rows plus a panel pointer).

### 4.2 G3 — Revenue Cycle & Finance

```mermaid
erDiagram
  PATIENT ||--o{ ACCOUNT : "guarantor / subject"
  ACCOUNT ||--o{ CHARGE : "accumulates"
  ENCOUNTER ||--o{ CHARGE : "generates"
  CHARGE }o--|| CONCEPT : "CPT/HCPCS, revenue code"
  CHARGE }o--|| COST_CENTER : "attributed to"
  CHARGE }o--o| ITEM : "supply / drug"
  PATIENT ||--o{ COVERAGE : "covered by"
  PAYER ||--o{ PAYER_PLAN : "offers"
  PAYER_PLAN ||--o{ COVERAGE : "under"
  COVERAGE ||--o{ ELIGIBILITY_CHECK : "verified by"
  COVERAGE ||--o{ PRIOR_AUTHORIZATION : "authorises"
  ACCOUNT ||--o{ CLAIM : "billed as"
  COVERAGE ||--o{ CLAIM : "submitted to"
  CLAIM ||--|{ CLAIM_LINE : "has"
  CLAIM ||--o{ CLAIM_DIAGNOSIS : "supports"
  CLAIM ||--o{ CLAIM_PROCEDURE : "reports"
  CLAIM_LINE ||--o{ CLAIM_LINE_MODIFIER : "modified by"
  CLAIM_LINE }o--o{ CHARGE : "bills"
  CLAIM ||--o{ CLAIM_RESPONSE : "adjudicated by"
  CLAIM_LINE ||--o{ REMITTANCE_LINE : "remitted as"
  REMITTANCE_LINE }o--|| CONCEPT : "CARC / RARC"
  ACCOUNT ||--o{ PAYMENT : "receives"
  REMITTANCE_LINE ||--o{ PAYMENT : "posts"
  PAYMENT ||--o{ GL_JOURNAL_LINE : "accounted by"
  CHARGE ||--o{ GL_JOURNAL_LINE : "accounted by"
  GL_JOURNAL_LINE }o--|| GL_ACCOUNT : "debits / credits"
  GL_JOURNAL_LINE }o--|| COST_CENTER : "in"
  GL_JOURNAL_LINE }o--|| LEDGER : "in"
  GL_JOURNAL_LINE }o--|| ACCOUNTING_PERIOD : "in"
  SUPPLIER ||--o{ AP_INVOICE : "bills"
  AP_INVOICE ||--|{ AP_INVOICE_LINE : "has"
  AP_INVOICE_LINE }o--o| PURCHASE_ORDER_LINE : "matches"
  AP_INVOICE_LINE }o--|| GL_ACCOUNT : "charged to"
  AP_INVOICE ||--o{ AP_PAYMENT : "paid by"
  COST_CENTER ||--o{ BUDGET_LINE : "budgeted"
  COST_CENTER ||--o{ FIXED_ASSET : "owns"
  PROJECT ||--o{ PROJECT_COST : "incurs"
```

Key design decisions: the **charge** is the bridge between clinical G2 and financial G3 (`encounter_bk` on
`fact_charge`); claim lines link to charges many-to-many (bundling); remittance is line-level with CARC/RARC concepts
from G8; every patient-accounting transaction is traceable to GL journal lines, which is where Oracle Fusion ERP and
Cerner patient accounting reconcile.

### 4.3 G4 — Supply Chain & Operations

```mermaid
erDiagram
  ITEM ||--o{ ITEM_IDENTIFIER : "GTIN / UDI / NDC / supplier part"
  ITEM }o--|| ITEM_CATEGORY : "UNSPSC"
  SUPPLIER ||--o{ SUPPLIER_CONTRACT : "holds"
  SUPPLIER_CONTRACT ||--o{ CONTRACT_PRICE : "prices"
  ITEM ||--o{ CONTRACT_PRICE : "priced"
  COST_CENTER ||--o{ PURCHASE_REQUISITION_LINE : "requests"
  PURCHASE_REQUISITION_LINE ||--o| PURCHASE_ORDER_LINE : "sourced as"
  SUPPLIER ||--o{ PURCHASE_ORDER_LINE : "supplies"
  ITEM ||--o{ PURCHASE_ORDER_LINE : "ordered"
  PURCHASE_ORDER_LINE ||--o{ RECEIPT_LINE : "received"
  RECEIPT_LINE ||--o{ INVENTORY_TRANSACTION : "posts"
  INVENTORY_LOCATION ||--o{ INVENTORY_TRANSACTION : "at"
  ITEM ||--o{ INVENTORY_TRANSACTION : "moves"
  INVENTORY_LOCATION ||--o{ INVENTORY_BALANCE : "holds"
  ITEM ||--o{ INVENTORY_BALANCE : "balance of"
  INVENTORY_TRANSACTION ||--o| SUPPLY_USAGE : "issues for"
  SUPPLY_USAGE }o--|| PATIENT : "for"
  SUPPLY_USAGE }o--o| PROCEDURE : "during"
  SUPPLY_USAGE ||--o| CHARGE : "charged as"
  LOCATION ||--o{ INVENTORY_LOCATION : "houses"
  LOCATION ||--o{ BED : "contains"
  BED ||--o{ BED_ASSIGNMENT : "assigned"
  PATIENT ||--o{ BED_ASSIGNMENT : "occupies"
  ENCOUNTER ||--o{ BED_ASSIGNMENT : "during"
  ASSET }o--|| ITEM : "model"
  ASSET }o--|| LOCATION : "located"
  ASSET ||--o{ ASSET_MAINTENANCE : "maintained"
  SCHEDULE ||--o{ SLOT : "offers"
  SLOT ||--o| APPOINTMENT : "booked by"
  PATIENT ||--o{ APPOINTMENT : "attends"
  PRACTITIONER ||--o{ APPOINTMENT : "sees"
  LOCATION ||--o{ APPOINTMENT : "at"
  APPOINTMENT ||--o| ENCOUNTER : "becomes"
  PATIENT ||--o{ TRANSPORT_REQUEST : "needs"
```

Key design decisions: supply usage is the **clinical–supply hinge** (patient, procedure, charge); inventory balances
are snapshots reproducible from transactions (EDW-G4-001); scheduling follows FHIR `Schedule`/`Slot`/`Appointment` and
an appointment may become an encounter (`appointment_bk` on `fact_encounter`).

### 4.4 G5 — Workforce

```mermaid
erDiagram
  PARTY ||--o| WORKER : "is"
  WORKER ||--o{ ASSIGNMENT : "holds"
  ASSIGNMENT }o--|| POSITION : "fills"
  POSITION }o--|| JOB : "is a"
  POSITION }o--|| COST_CENTER : "budgeted in"
  POSITION }o--|| ORGANIZATION : "in"
  POSITION }o--o| LOCATION : "based at"
  WORKER ||--o{ WORKER_EVENT : "experiences"
  WORKER ||--o{ CREDENTIAL : "holds"
  CREDENTIAL }o--|| CREDENTIAL_TYPE : "of type"
  WORKER ||--o{ COMPETENCY_ASSESSMENT : "assessed"
  WORKER ||--o{ SHIFT : "scheduled"
  SHIFT }o--|| LOCATION : "on unit"
  WORKER ||--o{ TIMECARD_ENTRY : "records"
  TIMECARD_ENTRY }o--|| PAY_ELEMENT : "paid as"
  TIMECARD_ENTRY }o--|| COST_CENTER : "charged to"
  WORKER ||--o{ ABSENCE : "takes"
  WORKER ||--o{ PAYROLL_SUMMARY : "summarised"
  WORKER ||--o{ LEARNING_COMPLETION : "completes"
  WORKER ||--o| PRACTITIONER : "same person as"
```

Key design decisions: `WORKER` and `PRACTITIONER` are **two roles of one party** (`bridge_worker_practitioner`);
department is the conformed cost center; payroll is aggregate only (EDW-G5-002).

### 4.5 G6 — Population Health & Quality

```mermaid
erDiagram
  MEASURE ||--o{ MEASURE_POPULATION : "defines"
  MEASURE ||--o{ MEASURE_RESULT : "evaluated as"
  MEASURE_POPULATION ||--o{ MEASURE_RESULT : "for"
  PATIENT ||--o{ MEASURE_RESULT : "subject"
  PRACTITIONER ||--o{ MEASURE_RESULT : "attributed"
  PROGRAM ||--o{ PROGRAM_ENROLLMENT : "enrols"
  PATIENT ||--o{ PROGRAM_ENROLLMENT : "in"
  PROGRAM ||--o{ ATTRIBUTION : "attributes"
  PATIENT ||--o{ ATTRIBUTION : "attributed"
  PRACTITIONER ||--o{ ATTRIBUTION : "to"
  PAYER_PLAN ||--o{ PROGRAM : "sponsors"
  PATIENT ||--o{ CARE_GAP : "has"
  CARE_GAP }o--|| CARE_GAP_TYPE : "type"
  CARE_GAP ||--o{ CARE_GAP_EVIDENCE : "closed by"
  CARE_GAP_EVIDENCE }o--|| CLINICAL_EVENT : "references G2"
  PATIENT ||--o{ RISK_SCORE : "scored"
  RISK_SCORE }o--|| RISK_MODEL : "by"
  PATIENT ||--o{ SDOH_OBSERVATION : "screened"
  SDOH_OBSERVATION }o--|| CONCEPT : "LOINC / SNOMED"
  SDOH_OBSERVATION }o--o| GEOGRAPHY : "area indices"
  REGISTRY ||--o{ REGISTRY_MEMBERSHIP : "includes"
  PATIENT ||--o{ REGISTRY_MEMBERSHIP : "member"
```

### 4.6 G7 — Research & Analytics

```mermaid
erDiagram
  RESEARCH_STUDY ||--o{ RESEARCH_SUBJECT : "enrols"
  PATIENT ||--o{ RESEARCH_SUBJECT : "participates as"
  RESEARCH_SUBJECT ||--o{ RESEARCH_CONSENT : "consents"
  RESEARCH_STUDY }o--|| ORGANIZATION : "sponsor"
  RESEARCH_STUDY }o--|| PRACTITIONER : "principal investigator"
  COHORT_DEFINITION ||--o{ COHORT_MEMBERSHIP : "evaluates"
  PATIENT ||--o{ COHORT_MEMBERSHIP : "member"
  COHORT_DEFINITION }o--o{ RESEARCH_STUDY : "used by"
  FEATURE ||--o{ FEATURE_VALUE : "computed"
  PATIENT ||--o{ FEATURE_VALUE : "for"
  MODEL ||--o{ MODEL_PREDICTION : "predicts"
  PATIENT ||--o{ MODEL_PREDICTION : "about"
  MODEL ||--o{ MODEL_EVALUATION : "evaluated"
  MODEL }o--|| COHORT_DEFINITION : "trained on"
  FEATURE }o--|| CONCEPT : "ontology property"
```

### 4.7 G8 — Reference & Terminology

```mermaid
erDiagram
  CODE_SYSTEM ||--o{ CODE_SYSTEM_VERSION : "released as"
  CODE_SYSTEM_VERSION ||--o{ CONCEPT : "defines"
  CONCEPT ||--o{ CONCEPT_DESIGNATION : "labelled"
  CONCEPT ||--o{ CONCEPT_PROPERTY : "has"
  CONCEPT ||--o{ CONCEPT_HIERARCHY : "ancestor"
  CONCEPT ||--o{ CONCEPT_HIERARCHY : "descendant"
  VALUE_SET ||--o{ VALUE_SET_VERSION : "released as"
  VALUE_SET_VERSION ||--o{ VALUE_SET_MEMBER : "expands to"
  CONCEPT ||--o{ VALUE_SET_MEMBER : "member"
  CONCEPT_MAP ||--o{ CONCEPT_MAP_ELEMENT : "maps"
  CONCEPT ||--o{ CONCEPT_MAP_ELEMENT : "source"
  CONCEPT ||--o{ CONCEPT_MAP_ELEMENT : "target"
  CALENDAR_DATE ||--o{ FISCAL_PERIOD : "belongs to"
  GEOGRAPHY ||--o{ GEOGRAPHY : "part of"
  CURRENCY ||--o{ EXCHANGE_RATE : "from / to"
  UCUM_UNIT ||--o{ UCUM_UNIT : "converts to canonical"
  SERVICE_LINE ||--o{ SERVICE_LINE_RULE : "defined by"
  SERVICE_LINE_RULE }o--|| CONCEPT : "DRG / CPT"
```

### 4.8 G9 — Metadata, Quality & Governance

```mermaid
erDiagram
  SOURCE_SYSTEM ||--o{ SOURCE_INTERFACE : "exposes"
  SOURCE_INTERFACE ||--o{ DATA_ELEMENT : "carries"
  DATA_ELEMENT }o--|| CONCEPT : "ontology property"
  DQ_RULE }o--|| DQ_DIMENSION : "measures"
  DQ_RULE }o--o{ DATA_ELEMENT : "targets"
  DQ_RULE ||--o{ DQ_RESULT : "produces"
  DQ_RESULT }o--|| LOAD_BATCH : "evaluated in"
  DQ_RESULT ||--o{ DQ_ISSUE : "raises"
  AGENT ||--o{ DQ_ISSUE : "opens / remediates"
  PIPELINE ||--o{ LINEAGE_ACTIVITY : "runs as"
  AGENT ||--o{ LINEAGE_ACTIVITY : "performs"
  LINEAGE_ACTIVITY ||--o{ LINEAGE_ENTITY_USE : "used / generated"
  LOAD_BATCH }o--|| LINEAGE_ACTIVITY : "is"
  AGENT ||--o{ CHANGE_REQUEST : "proposes"
  CHANGE_REQUEST ||--o| SCHEMA_CHANGE : "applied as"
  POLICY ||--o{ AUDIT_EVENT : "evaluated"
  PATIENT ||--o{ CONSENT_DIRECTIVE : "declares"
  RECONCILIATION_CHECK }o--|| LOAD_BATCH : "after"
```

## 5. Cross-group relationships

| From | To | Relationship | Carried as |
|------|----|--------------|-----------|
| G2 `ENCOUNTER` | G3 `CHARGE`, `CLAIM` | generates / billed as | `encounter_bk` on `fact_charge`, `fact_claim_header` |
| G4 `APPOINTMENT` | G2 `ENCOUNTER` | becomes | `appointment_bk` on `fact_encounter` |
| G4 `SUPPLY_USAGE` | G2 `PROCEDURE`, G3 `CHARGE` | during / charged as | `procedure_bk`, `charge_bk` |
| G5 `WORKER` | G1 `PRACTITIONER` | same party | `bridge_worker_practitioner` |
| G3 `AP_INVOICE_LINE` | G4 `PURCHASE_ORDER_LINE` | matches | `po_line_bk` |
| G6 `CARE_GAP` | G2 any event | evidenced by | `bridge_care_gap_evidence` (polymorphic) |
| G7 `COHORT_MEMBERSHIP` | G1–G6 | derived from | `cohort_definition` IRI + evaluation `as_of_ts` |
| G9 `DQ_RESULT` | any table | measures | `table_name`, `column_name`, `record_bk` |
| G9 `LOAD_BATCH` | every row | loaded by | `load_batch_id` |

## 6. Conceptual-level requirements

> **EDW-LOG-001** (P0, inspection) — Every entity in this document MUST exist as an OWL class in the Ontology Registry
> and as an entry in the Logical Model Register before any DDL is generated for it.

> **EDW-LOG-002** (P0, inspection) — Persons and organisations MUST be modelled with the party-role pattern; a role
> MUST NOT be modelled as a separate party.

> **EDW-LOG-003** (P0, inspection) — Every clinical, financial, operational and workforce fact MUST be a specialisation
> of the event pattern with `event_start_ts`, `recorded_ts`, status, at least one participant or subject, and
> `prov_activity_iri`.

> **EDW-LOG-004** (P1, inspection) — Cross-group relationships MUST be carried by business keys (`*_bk`) or bridges,
> never by copying attributes of the referenced entity into the referencing fact.

> **EDW-LOG-005** (P0, inspection) — No proprietary UDMH or HCDM entity names, attribute lists or diagrams MAY appear in
> this module; only the harmonised patterns of module 04 are referenced (README licensing note).
