---
title: DQ Rule DSL and Rule Catalogue
module: 09-data-quality
doc_id: DQ-02
version: 0.1.0
status: draft
owners: [Data Quality Lead, Group Data Stewards, agent.dq_author]
depends_on: [DQ-01, ARC-01, ONT-00, IOP-00, EDW-00, TKG-00, STR-00]
last_updated: 2026-10-17
---

# DQ Rule DSL and Rule Catalogue

## 1. Scope

This document specifies the YAML rule DSL that every DQ rule is written in, and the first-release catalogue of
concrete rules across subject-area groups G1–G9, across Cerner Millennium, Oracle Fusion (ERP, HCM, SCM) and the
peripheral systems, including cross-system reconciliation rules. The machine-readable packs in `rules/` are the
source of truth; the tables below are the human index. The JSON Schema `rules/rule.schema.json` validates every pack
in CI and is loaded by `agent.dq_author` as a tool contract.

## 2. Rule DSL (YAML)

### 2.1 Pack envelope

```yaml
pack: g2-clinical                 # file name without extension
version: 0.1.0                    # semantic version of the pack
group: G2                         # G1..G9 or XSYS for cross-system packs
owner: steward.clinical           # steward id (dim_data_steward.steward_bk)
description: Clinical care rules derived from HEALIX clinical shapes and US Core bindings
rules: [ ... ]
```

### 2.2 Rule fields

| Field | Type | Req. | Meaning |
|-------|------|------|---------|
| `id` | string `^DQR-[A-Z]+-[A-Z0-9]+(-[A-Z0-9]+)?-\d{3}$` | MUST | Stable identifier `DQR-<DIMENSION>-<DOMAIN>-<NNN>`; `<DIMENSION>` MUST equal `dimension` upper-cased; `<DOMAIN>` is a registered token (§4) or two registered tokens joined by one hyphen (`KG-EDW`) |
| `name` | string ≤ 120 | MUST | Short human title |
| `description` | string | MUST | What is checked and why (one paragraph) |
| `dimension` | enum | MUST | `completeness, validity, accuracy, consistency, uniqueness, timeliness, integrity, temporal, lineage, semantic` |
| `group` | enum `G1..G9, XSYS` | MUST | Subject-area group; `XSYS` for reconciliation rules spanning groups |
| `domain` | string | MUST | Kafka domain (`party, clinical, finance, supply, workforce, pophealth, research, reference, governance`) |
| `source_systems` | list of enum | MUST | `cerner, oracle_erp, oracle_hcm, oracle_scm, lis, ris_pacs, pharmacy, rcm, clearinghouse, hie, portal, devices, payer, credentialing, timekeeping, scheduling, empi, platform` |
| `cde` | list of string | SHOULD | Canonical element paths (FHIRPath-style) protected by the rule; drives CDE coverage |
| `targets` | list (≥ 1) | MUST | One entry per execution plane; see §2.3 |
| `severity` | enum | MUST | `blocker, critical, major, minor` |
| `quarantine` | boolean | MAY | Default `true` for `blocker`, `false` otherwise; `critical` rules may opt in |
| `threshold` | object | MUST | `{metric: pass_rate|fail_count|value, min?: number, max?: number, window: ISO 8601 duration}`; aggregate breach condition |
| `owner` | string | MUST | Steward id resolving to `dim_data_steward` |
| `derivation` | enum | MUST | `ontology, terminology, profiling, policy` |
| `derived_from` | IRI/URI | MUST | Ontology shape/property IRI, ValueSet canonical URL, profiling `baseline_ref` URN, or policy document URI |
| `remediation` | object | MUST | `{action: auto_fix|steward|source_fix|accept, hint: string, policy_ref?: URI, tool?: string}` |
| `version` | semver | MUST | Rule version; immutable once `active` |
| `status` | enum | MUST | `draft, review, active, tuning, deprecated, retired` |
| `tags` | list of string | MAY | Free-form (`us-core`, `hipaa`, `sox`, `drg`) |
| `tests` | list | SHOULD | Fixture references `{fixture: path, expect: pass|fail}` for CI |

### 2.3 Target (per execution plane)

| Plane | `plane` | Locator fields | `language` | Expression semantics |
|-------|---------|----------------|------------|----------------------|
| Stream (Flink, module 07) | `stream` | `resource` (FHIR resource type or HEALIX-ERP profile name), optional `profile` canonical URL, optional `topic` | `fhirpath` | `expression` is a FHIRPath boolean evaluated per record; **true = pass**. Function extensions: `%vs(url)` membership via Terminology Service cache, `%now`, `%header(name)`, `%ref_exists(ref)` against the entity-resolution cache |
| Graph (KG, module 08) | `graph` | `shape` (SHACL NodeShape IRI) **or** `sparql` | `shacl` or `sparql` | SHACL: pass = `sh:conforms true` for the focus node. SPARQL: a `SELECT` returning **violating focus nodes** (`?focus`, optional `?detail`); empty result = pass. Queries run against the *current* valid-time slice unless `as_of` is given |
| Warehouse (EDW, module 05) | `warehouse` | `table` (`schema.table`), optional `grain` | `sql` | `sql` is a SELECT returning **failing rows** (dbt-test convention) with at least the business key column(s) listed in `key_columns`; zero rows = pass. Dialect: ANSI with Teradata reference; macros `{{ ref('…') }}`, `{{ vs_members('url') }}`, `{{ window_start }}` |

A rule MAY have one, two or three targets. A CDE rule SHOULD have all three so results reconcile (`03-dq-pipelines.md`
§7). Reconciliation rules (XSYS) typically have only a `warehouse` target plus a `graph` target.

### 2.4 Example

```yaml
- id: DQR-CONSISTENCY-ENCOUNTER-002
  name: Encounter period end not before start
  description: Encounter.period.end, when present, must be on or after period.start (temporal axiom healix:endAfterStart on clinical:Encounter).
  dimension: consistency
  group: G2
  domain: clinical
  source_systems: [cerner]
  cde: [Encounter.period]
  targets:
    - plane: stream
      resource: Encounter
      language: fhirpath
      expression: "period.end.empty() or period.end >= period.start"
    - plane: graph
      language: shacl
      shape: https://ontology.healix.example.org/shapes/clinical/EncounterShape#periodOrder
    - plane: warehouse
      table: edw_g2_clinical.fact_encounter
      language: sql
      key_columns: [encounter_bk]
      sql: SELECT encounter_bk FROM {{ ref('fact_encounter') }} WHERE discharge_dt_tm < admit_dt_tm
  severity: critical
  threshold: {metric: pass_rate, min: 0.999, window: PT1H}
  owner: steward.clinical
  derivation: ontology
  derived_from: https://ontology.healix.example.org/clinical/Encounter#endAfterStart
  remediation: {action: source_fix, hint: Cerner encounter discharge before admit; raise to registration team, policy_ref: https://policy.healix.example.org/dq/encounter-period}
  version: 1.0.0
  status: active
```

## 3. Rule catalogue

Legend — **Sev**: B blocker, C critical, M major, m minor. **Planes**: S stream, G graph, W warehouse. **Src**: CER Cerner,
OFE Oracle Fusion ERP, OFH Oracle Fusion HCM, OFS Oracle Fusion SCM, LIS, RIS, PHA pharmacy, RCM/CH revenue cycle/
clearinghouse, CRD credentialing, SCH scheduling, EMPI, PLT platform. Rules marked † are in the YAML packs in `rules/`.

### 3.1 G1 Party & Identity (`domain: party`)

| Rule id | Check | Src | Sev | Planes | Derivation |
|---------|-------|-----|-----|--------|-----------|
| DQR-COMPLETENESS-PATIENT-001 † | `Patient.birthDate` present | CER | C | S G W | ontology (`PatientShape#birthDate minCount 1`) |
| DQR-COMPLETENESS-PATIENT-002 † | `Patient.identifier` of type MRN present with `assigner` (facility) | CER | B | S G W | ontology |
| DQR-VALIDITY-PATIENT-003 † | `Patient.gender` in `administrative-gender` | CER | M | S G W | terminology |
| DQR-CONSISTENCY-PATIENT-004 † | `deceasedDateTime ≥ birthDate` | CER | M | S W | ontology (temporal axiom) |
| DQR-INTEGRITY-PATIENT-005 † | `Patient.managingOrganization` resolves to an Organization | CER | M | S G W | ontology (range) |
| DQR-TEMPORAL-PATIENT-001 † | `birthDate ≤ today` and implied age ≤ 130 y | CER | M | S W | policy |
| DQR-UNIQUENESS-PATIENT-001 † | One active MRN per patient per facility; one patient per active MRN | CER, EMPI | B | G W | ontology (`owl:hasKey`) |
| DQR-VALIDITY-PRACTITIONER-001 † | NPI is 10 digits and passes Luhn with 80840 prefix | CER, CRD | C | S G W | policy (CMS NPI standard) |
| DQR-UNIQUENESS-PRACTITIONER-002 † | One Practitioner per NPI | CER, CRD, OFH | C | G W | ontology (`owl:hasKey`) |
| DQR-VALIDITY-ORGANIZATION-001 † | Supplier tax id matches EIN pattern `^\d{2}-\d{7}$` or SSN-masked | OFS | M | S W | policy |
| DQR-COMPLETENESS-LOCATION-001 † | Bed `Location` has `physicalType=bd` and `partOf` room/unit | CER | m | S G | ontology |
| DQR-UNIQUENESS-ORGANIZATION-002 | One supplier per (tax id, country) | OFS | M | W | ontology |

### 3.2 G2 Clinical Care (`domain: clinical`)

| Rule id | Check | Src | Sev | Planes | Derivation |
|---------|-------|-----|-----|--------|-----------|
| DQR-COMPLETENESS-ENCOUNTER-001 † | `Encounter.class` present | CER | C | S G W | ontology |
| DQR-CONSISTENCY-ENCOUNTER-002 † | `Encounter.period.end ≥ start` | CER | C | S G W | ontology |
| DQR-VALIDITY-ENCOUNTER-003 † | `Encounter.class` in `v3-ActEncounterCode` | CER | M | S W | terminology |
| DQR-UNIQUENESS-ENCOUNTER-004 | One canonical Encounter per Cerner `ENCNTR_ID` | CER | C | W | ontology |
| DQR-COMPLETENESS-CONDITION-001 † | Encounter diagnosis has `Condition.code` and `Encounter.diagnosis.rank` | CER | C | S W | ontology |
| DQR-VALIDITY-CONDITION-002 † | `Condition.code` is a valid ICD-10-CM code in the release in force on `recordedDate` | CER | C | S G W | terminology |
| DQR-SEMANTIC-CONDITION-005 † | ICD-10-CM code sex/age restriction consistent with patient (ontology axioms `healix:restrictedToSex`, `healix:minAge/maxAge`) | CER | C | S G W | ontology |
| DQR-VALIDITY-OBSERVATION-003 † | Lab `Observation.code` in LOINC laboratory value set (`healix/vs/loinc-lab`) | CER, LIS | C | S G W | terminology |
| DQR-COMPLETENESS-OBSERVATION-004 † | Quantitative lab has `valueQuantity.value`, `unit`, `system=UCUM` | CER, LIS | C | S G W | ontology |
| DQR-ACCURACY-OBSERVATION-005 † | Lab value within plausibility range per LOINC (profiled 0.1–99.9 pct widened by clinical bounds) | CER, LIS | M | S W | profiling |
| DQR-CONSISTENCY-OBSERVATION-006 | `referenceRange.low < high`; range unit equals value unit | CER, LIS | M | S W | ontology |
| DQR-INTEGRITY-OBSERVATION-007 | `Observation.encounter` resolves | CER, LIS | M | S G W | ontology |
| DQR-TEMPORAL-OBSERVATION-008 | `effective[x]` within encounter period ± 24 h | CER | M | G W | ontology (temporal) |
| DQR-VALIDITY-MEDICATION-001 † | `MedicationRequest.medication` coded in RxNorm (SCD/SBD/GPCK/BPCK TTY) | CER, PHA | C | S G W | terminology |
| DQR-COMPLETENESS-MEDICATION-002 † | `dosageInstruction.doseAndRate.dose` and `route` present | CER | C | S W | ontology |
| DQR-SEMANTIC-MEDICATION-003 | Route compatible with dose form (`healix:compatibleRoute` axiom) | CER, PHA | M | G W | ontology |
| DQR-TIMELINESS-ADT-001 † | ADT (A01/A02/A03/A04/A08) canonicalised within 5 min of `MSH-7` | CER | C | S W | policy |
| DQR-TIMELINESS-RESULT-002 | Lab result canonical within 10 min of LIS verification time | LIS | M | S | policy |
| DQR-TIMELINESS-ENCOUNTER-001 † | In-progress inpatient/observation encounter without discharge > 30 d (ghost admit, SRC-CER §9) | CER | m | S W | policy |
| DQR-TIMELINESS-ENCOUNTER-002 † | Encounter change canonicalised within 5 s p95 (`hc-event-time − hc-source-time`; data contract `dc.cerner.encounter.v2`, ARC-NFR-001) | CER | M | S W | policy |
| DQR-TIMELINESS-SKEW-001 † | Source time ahead of platform event time by > 5 min (facility clock skew / DST / missing offset) | CER | M | S W | policy |
| DQR-VALIDITY-ALLERGY-001 | `AllergyIntolerance.code` in SNOMED CT substance ∪ RxNorm allergen VS | CER | C | S W | terminology |
| DQR-VALIDITY-IMMUNIZATION-001 | `Immunization.vaccineCode` in CVX | CER | M | S W | terminology |
| DQR-INTEGRITY-SPECIMEN-001 | `DiagnosticReport.specimen` resolves to Specimen with `collection.collectedDateTime` | LIS | M | S G | ontology |
| DQR-COMPLETENESS-IMAGING-001 | `ImagingStudy` has accession identifier and `modality` | RIS | M | S W | ontology |
| DQR-TEMPORAL-ADMINISTRATION-001 | `MedicationAdministration.effective` ≥ linked `MedicationRequest.authoredOn` | CER, PHA | M | G W | ontology (temporal) |

### 3.3 G3 Revenue Cycle & Finance (`domain: finance`)

| Rule id | Check | Src | Sev | Planes | Derivation |
|---------|-------|-----|-----|--------|-----------|
| DQR-COMPLETENESS-CLAIM-001 † | `Claim` has identifier, patient, insurer, total | RCM/CH | C | S W | ontology |
| DQR-INTEGRITY-CLAIM-002 † | `Claim.insurer` resolves to payer Organization | RCM/CH | C | S G W | ontology |
| DQR-VALIDITY-CLAIM-003 † | `Claim.item.productOrService` in CPT/HCPCS VS; modifiers valid | RCM/CH | C | S W | terminology |
| DQR-INTEGRITY-CLAIM-004 † | `Claim.patient` resolves to a Patient with an active MRN | RCM/CH | B | S G W | ontology |
| DQR-CONSISTENCY-CLAIM-005 † | `Claim.total = Σ item.net` (± 0.01) | RCM/CH | C | S W | ontology |
| DQR-ACCURACY-CLAIMRESPONSE-001 † | `ClaimResponse.payment.amount ≤ Claim.total`; 835 CLP04 equals payment amount | CH | C | W | policy |
| DQR-VALIDITY-GL-001 † | Every GL journal line account combination exists and is enabled in the chart of accounts | OFE | C | S W | ontology |
| DQR-ACCURACY-GL-002 † | Journal debits = credits per batch (entered and accounted currency) | OFE | B | S W | policy |
| DQR-TEMPORAL-GL-003 † | Accounting date within an open or future-enterable period | OFE | C | S W | policy |
| DQR-COMPLETENESS-CHARGE-001 † | `ChargeItem` has code, quantity, occurrence, account | CER | C | S W | ontology |
| DQR-TIMELINESS-CHARGE-002 † | Charge posted within 72 h of service date | CER | M | W | policy |
| DQR-INTEGRITY-COVERAGE-001 | `Coverage.payor` resolves; `subscriberId` matches payer-specific pattern | CER, payer | M | S W | ontology + policy |
| DQR-UNIQUENESS-INVOICE-001 | AP invoice number unique per supplier | OFE | C | W | ontology |
| DQR-CONSISTENCY-INVOICE-002 | Invoice total = Σ lines + tax − discounts | OFE | M | S W | ontology |
| DQR-VALIDITY-COSTCENTER-001 | Cost centre segment value in active hierarchy | OFE | M | W | terminology (reference data) |

### 3.4 G4 Supply Chain & Operations (`domain: supply`)

| Rule id | Check | Src | Sev | Planes | Derivation |
|---------|-------|-----|-----|--------|-----------|
| DQR-COMPLETENESS-ITEM-001 † | Item has number, description, primary UOM | OFS | C | S W | ontology |
| DQR-UNIQUENESS-ITEM-002 † | Item number unique per inventory organisation | OFS | C | W | ontology |
| DQR-VALIDITY-ITEM-003 † | Clinical item GTIN/UDI-DI has valid GS1 check digit | OFS | M | S W | policy |
| DQR-VALIDITY-PO-001 † | PO line UOM in UCUM ∪ Oracle UOM value set | OFS | C | S W | terminology |
| DQR-CONSISTENCY-PO-002 † | PO line amount = quantity × unit price (± 0.01) | OFS | M | S W | ontology |
| DQR-INTEGRITY-PO-003 † | PO supplier resolves to an active supplier Organization | OFS | C | S G W | ontology |
| DQR-TEMPORAL-PO-004 † | Need-by date ≥ order date | OFS | m | S W | ontology (temporal) |
| DQR-INTEGRITY-RECEIPT-001 † | Receipt line references an open PO line of the same item | OFS | C | S W | ontology |
| DQR-ACCURACY-INVENTORY-001 † | On-hand quantity ≥ 0 per item/sub-inventory/lot | OFS | C | S W | policy |
| DQR-TIMELINESS-INVENTORY-002 † | On-hand snapshot age ≤ 24 h per inventory organisation | OFS | M | W | policy |
| DQR-TEMPORAL-APPOINTMENT-001 | `Appointment.end > start`; no overlapping booked slots per resource | SCH | M | S G W | ontology (temporal) |
| DQR-CONSISTENCY-BED-001 | Bed status `occupied` iff an in-progress Encounter has that location | CER | M | G W | ontology |

### 3.5 G5 Workforce (`domain: workforce`)

| Rule id | Check | Src | Sev | Planes | Derivation |
|---------|-------|-----|-----|--------|-----------|
| DQR-COMPLETENESS-WORKER-001 † | Worker has person number, legal name, hire date | OFH | C | S W | ontology |
| DQR-UNIQUENESS-WORKER-002 † | One worker per person number; national id unique within legislation | OFH | C | W | ontology |
| DQR-TEMPORAL-ASSIGNMENT-001 † | No overlapping primary assignments for a worker | OFH | C | G W | ontology (temporal) |
| DQR-INTEGRITY-ASSIGNMENT-002 † | Assignment position resolves to an active Position | OFH | C | S G W | ontology |
| DQR-CONSISTENCY-ASSIGNMENT-003 † | Assignment effective start ≥ hire date | OFH | M | S W | ontology |
| DQR-COMPLETENESS-POSITION-001 † | Position has job, department, FTE | OFH | M | S W | ontology |
| DQR-TIMELINESS-LICENCE-001 † | Active clinical assignment has an unexpired licence | OFH, CRD | B | G W | policy |
| DQR-VALIDITY-LICENCE-002 † | Licence type in NUCC taxonomy ∪ state-board licence VS | OFH, CRD | M | S W | terminology |
| DQR-TEMPORAL-LICENCE-003 † | Licence issue date ≤ expiry date | OFH, CRD | M | S W | ontology (temporal) |
| DQR-ACCURACY-TIMECARD-001 † | Shift hours between 0 and 24; no negative pay hours | timekeeping | M | S W | policy |

### 3.6 G6 Population Health & Quality (`domain: pophealth`)

| Rule id | Check | Src | Sev | Planes | Derivation |
|---------|-------|-----|-----|--------|-----------|
| DQR-COMPLETENESS-MEASUREREPORT-001 | Each population group has numerator and denominator counts | derived | M | W | ontology |
| DQR-CONSISTENCY-MEASUREREPORT-002 | numerator ≤ denominator; exclusions ≤ denominator | derived | C | W | ontology |
| DQR-INTEGRITY-CAREGAP-001 | Care gap patient resolves and has active attribution | derived, payer | M | G W | ontology |
| DQR-VALIDITY-SDOH-001 | SDOH Observation code in Gravity Project VS | CER, HIE | m | S W | terminology |

### 3.7 G7 Research & Analytics (`domain: research`)

| Rule id | Check | Src | Sev | Planes | Derivation |
|---------|-------|-----|-----|--------|-----------|
| DQR-INTEGRITY-RESEARCHSUBJECT-001 | `ResearchSubject.individual` resolves and a research Consent exists | research | B | G W | ontology + policy |
| DQR-TEMPORAL-CONSENT-001 | Consent `provision.period` covers every data-use date | research | C | G | ontology (temporal) |
| DQR-UNIQUENESS-COHORT-001 | Subject appears once per cohort version | research | M | W | ontology |

### 3.8 G8 Reference & Terminology (`domain: reference`)

| Rule id | Check | Src | Sev | Planes | Derivation |
|---------|-------|-----|-----|--------|-----------|
| DQR-COMPLETENESS-VALUESET-001 | Every bound ValueSet has an expansion ≤ 30 days old | TS | C | G W | policy |
| DQR-INTEGRITY-CONCEPTMAP-001 | Every ConceptMap target code exists in the target CodeSystem version | TS | C | G W | ontology |
| DQR-VALIDITY-UCUM-001 | All unit strings parse under UCUM grammar | TS, OFS, LIS | M | S W | terminology |
| DQR-TIMELINESS-TERMINOLOGY-002 | Code system release loaded within 30 days of publication (ICD-10-CM Oct 1, CPT Jan 1, LOINC Feb/Aug, RxNorm monthly) | TS | M | W | policy |

### 3.9 G9 Metadata, Quality & Governance (`domain: governance`)

| Rule id | Check | Src | Sev | Planes | Derivation |
|---------|-------|-----|-----|--------|-----------|
| DQR-LINEAGE-RECORD-001 | Canonical record carries `hc-source-time`, `hc-event-time`, source id and `prov_activity_iri` | PLT | B | S G | policy |
| DQR-LINEAGE-EDW-002 | Every EDW fact/dim row has `source_system_sk`, `batch_ulid`, `recorded_at` | PLT | C | W | policy |
| DQR-COMPLETENESS-DQRESULT-001 | Every active rule produced ≥ 1 result in its window (rule liveness) | PLT | M | W | policy |
| DQR-CONSISTENCY-PLANE-001 | Stream vs warehouse pass rate for the same rule/window differs by ≤ 0.5 pp | PLT | M | W | policy |
| DQR-TIMELINESS-ISSUE-001 | Blocker issues acknowledged ≤ 15 min, critical ≤ 30 min | PLT | C | W | policy |

#### 3.9.1 Bitemporal contract, provenance and dual-projection rules (pack `temporal-and-provenance`)

These rules realise the module 08 contract table (TKG-01 §9, clauses C1–C12), TKG-STORE-007 and TKG-04 §7.2. Each rule
tagged `templated-bitemporal` names `edw_g1_party.hist_dim_patient` as its reference warehouse target and is instantiated
by the rule compiler over every `hist_*`, `fact_*` and `bridge_*` table carrying the bitemporal block (EDW-ARCH-003,
EDW-06); graph targets apply to every `healix-time:BitemporalAssertion`. Columns follow TKG-01 §9 exactly: `valid_from`,
`valid_to`, `recorded_at`, `superseded_at`, `change_kind`, `event_time`, `source_time`, `source_tz`, `prov_activity_iri`.

| Rule id | Check | TKG-01 clause | Sev | Planes | Derivation |
|---------|-------|---------------|-----|--------|-----------|
| DQR-COMPLETENESS-TEMPORAL-001 † | `validFrom` / `valid_from` present on every assertion and row | C1 | C | G W | ontology (`healix-time:validFrom`) |
| DQR-COMPLETENESS-TEMPORAL-002 † | `recordedAt` / `recorded_at` present and not before `event_time` (sink clock) | C3 | C | G W | ontology (`healix-time:recordedAt`) |
| DQR-COMPLETENESS-TEMPORAL-003 † | `sourceTimeZone` / `source_tz` present | C12 | M | G W | ontology (`healix-time:sourceTimeZone`) |
| DQR-COMPLETENESS-TEMPORAL-004 † | Closed assertion (`supersededAt` set) carries `changeKind` | C6 (TKG-TIME-006) | C | G W | ontology (`healix-time:changeKind`) |
| DQR-VALIDITY-TEMPORAL-001 † | `valid_to > valid_from` (retraction tombstone `valid_to = valid_from` exempt, C10) | C2 | C | G W | ontology (`healix-time:validTo`) |
| DQR-VALIDITY-TEMPORAL-002 † | Open interval is NULL; no sentinel end dates (≥ 4000-01-01, Oracle 4712, 9999) | C2, C4 | C | G W | ontology (`BitemporalAssertionShape`) |
| DQR-VALIDITY-TEMPORAL-003 † | `change_kind` ∈ `ChangeKindScheme` {`new`, `new-version`, `correction`, `retraction`, `merge`, `unmerge`} | C6 | C | G W | ontology (`healix-time:ChangeKindScheme`) |
| DQR-CONSISTENCY-TEMPORAL-001 † | No overlapping valid intervals per (subject, functional predicate) / per business key within the current transaction-time slice | C9 | C | G W | ontology (`healix-shape:NoOverlapFunctional`) |
| DQR-CONSISTENCY-TEMPORAL-002 † | At most one current row per `(bk, valid_from)`; one current assertion per (subject, functional predicate, validFrom) | C4 (EDW-HIST-003) | C | G W | ontology (`healix-time:supersededAt`) |
| DQR-CONSISTENCY-TEMPORAL-003 † | `superseded_at ≥ recorded_at`; `change_kind = retraction` ⇒ `superseded_at` set, valid time kept | C4, C10 | C | G W | ontology (`healix-time:retractedBy`) |
| DQR-COMPLETENESS-PROV-001 † | `prov:wasGeneratedBy` IRI / `prov_activity_iri` present on every assertion and row | C5 | C | G W | ontology (`BitemporalAssertionShape`) |
| DQR-INTEGRITY-PROV-002 † | `prov_activity_iri` resolves to `fact_lineage_activity`; activity typed `prov:Activity` | C5 | M | G W | policy |
| DQR-CONSISTENCY-KG-001 † | `graph/current/<Gn>` has no drift against current assertions in the batch graphs (nightly rebuild, TKG-STORE-007) | — | C | G | policy |
| DQR-CONSISTENCY-KG-EDW-001 † | Nightly KG ↔ EDW reconciliation per (group, entity type, recorded day): current count, recorded count, content hash, ER link count, retraction count (TKG-04 §7.2; results on `hc.ops.kg-edw-reconciliation.v1`) | C11 | C | W G | policy |
| DQR-TIMELINESS-TEMPORAL-001 † | `recorded_at − event_time ≤ 30 min` for 99 % of versions per hour | C3, C7 | M | W | policy |
| DQR-TIMELINESS-TEMPORAL-002 † | Valid time derived from the §5.1 fallback chain (`validTimePrecision = approximate`) observed per predicate | C1 (TKG-01 §5.1) | m | G | ontology (`healix-time:validTimePrecision`) |

### 3.10 Cross-system reconciliation (`group: XSYS`)

Reconciliation rules compare aggregates or sets across source systems after both sides have landed in the EDW/KG.
They run in the warehouse plane at a declared grain and tolerance, with a graph target for set-difference listing.

| Rule id | Check (grain, tolerance) | Systems | Sev | Planes | Derivation |
|---------|--------------------------|---------|-----|--------|-----------|
| DQR-CONSISTENCY-XSYS-001 † | Cerner posted charges = Oracle AR transactions per facility per day (± 0.5 %) | CER ↔ OFE | C | W G | policy |
| DQR-CONSISTENCY-XSYS-002 † | Cerner charged supply usage = Oracle inventory issue transactions per item per day (± 2 % qty) | CER ↔ OFS | M | W | policy |
| DQR-CONSISTENCY-XSYS-003 † | Every active Cerner practitioner with orders in last 90 d is an active HCM worker or contracted provider | CER ↔ OFH | C | W G | policy |
| DQR-CONSISTENCY-XSYS-004 † | Practitioner with active privileges (credentialing) has an active licence in HCM/credentialing | CRD ↔ OFH | B | W G | policy |
| DQR-CONSISTENCY-XSYS-005 † | Cerner active patient count per facility = EMPI enterprise person count linked (± 0.1 %) | CER ↔ EMPI | M | W | policy |
| DQR-CONSISTENCY-XSYS-006 † | 835 remittance totals = Oracle AR receipts per payer per deposit date (± 0.01) | CH ↔ OFE | C | W | policy |
| DQR-CONSISTENCY-XSYS-007 † | Every Cerner bed/room Location maps to an Oracle Fusion facility/asset location node | CER ↔ OFE | m | W G | ontology |
| DQR-CONSISTENCY-XSYS-008 † | Pharmacy dispense without a Cerner MedicationAdministration or return within 24 h | PHA ↔ CER | M | W | policy |
| DQR-CONSISTENCY-XSYS-009 † | LIS verified result count = Cerner lab Observation count per day per lab section (± 0.5 %) | LIS ↔ CER | M | W | policy |
| DQR-CONSISTENCY-XSYS-010 † | KG entity count = EDW row count per group per as-of time (ADR-07 dual projection, exact) | PLT | C | W G | policy |

**Catalogue size:** 117 rules (83 in YAML packs †). Rules not yet in packs are authored by `agent.dq_author` in the
first release sprint using the DSL and must pass the schema.

## 4. Domain token registry

`<DOMAIN>` tokens in rule ids are controlled to keep ids stable and greppable. First-release registry:

`PATIENT, PRACTITIONER, ORGANIZATION, LOCATION, ENCOUNTER, CONDITION, OBSERVATION, MEDICATION, ADMINISTRATION,
ADT, RESULT, ALLERGY, IMMUNIZATION, SPECIMEN, IMAGING, CLAIM, CLAIMRESPONSE, GL, CHARGE, COVERAGE, INVOICE,
COSTCENTER, ITEM, PO, RECEIPT, INVENTORY, APPOINTMENT, BED, WORKER, ASSIGNMENT, POSITION, LICENCE, TIMECARD,
MEASUREREPORT, CAREGAP, SDOH, RESEARCHSUBJECT, CONSENT, COHORT, VALUESET, CONCEPTMAP, UCUM, TERMINOLOGY, RECORD,
EDW, DQRESULT, PLANE, ISSUE, XSYS, TEMPORAL, PROV, KG, KG-EDW, SKEW`. `TEMPORAL` and `PROV` denote the bitemporal block and
provenance reference shared by every entity (TKG-01 §9); `KG` and the compound `KG-EDW` denote the knowledge-graph projection
and its reconciliation with the EDW (ADR-07). New tokens are added by the Data Quality Lead through a change request.

## 5. Requirements

> **DQ-RULE-001** (P0, test) — Every rule pack in `rules/` MUST validate against `rules/rule.schema.json`; CI MUST
> reject a pull request with an invalid pack.

> **DQ-RULE-002** (P0, test) — Rule ids MUST be unique across all packs, match the DSL regex, and the dimension token
> in the id MUST equal the `dimension` field upper-cased; the linter MUST enforce this.

> **DQ-RULE-003** (P0, test) — Every rule MUST declare at least one target; every `blocker` or `critical` rule
> protecting a CDE MUST declare a `stream` target so the failure is detected before the record reaches a sink.

> **DQ-RULE-004** (P0, test) — Stream expressions MUST be valid FHIRPath (R4) that evaluates to a single boolean;
> warehouse SQL MUST parse in the reference dialect and return the declared `key_columns`; graph targets MUST reference
> a SHACL shape IRI that exists in the Ontology Registry or a SPARQL `SELECT` that binds `?focus`.

> **DQ-RULE-005** (P0, inspection) — Every rule MUST carry `derivation` and `derived_from`; `derived_from` for
> `ontology` rules MUST be an IRI under `https://ontology.healix.example.org/`, for `terminology` rules a ValueSet
> canonical URL, for `profiling` rules a `urn:healix:dq:baseline:` URN, for `policy` rules a governance document URI.

> **DQ-RULE-006** (P0, test) — The first release MUST ship at least the 117 rules catalogued in §3 in status `active`
> or `review`, covering every group G1–G9 and every source class (Cerner, Oracle Fusion ERP/HCM/SCM, ≥ 6 peripherals).

> **DQ-RULE-007** (P1, test) — Every rule SHOULD ship with at least one passing and one failing fixture under `tests`;
> rules of severity `blocker` MUST.

> **DQ-RULE-008** (P1, test) — Cross-system reconciliation rules MUST declare grain and tolerance explicitly in the
> SQL and in `threshold`, and MUST produce the set difference (unmatched keys) as issue detail, never only the delta.

> **DQ-RULE-009** (P0, inspection) — Rule packs are versioned in Git; releasing a pack MUST bump `version` and MUST be
> signed by `agent.release_manager` (ARC-NFR-006).

> **DQ-RULE-010** (P1, test) — Terminology-derived rules MUST pin the value-set expansion version at evaluation time
> and record it in the result's `context.valueset_version`.

> **DQ-RULE-011** (P2, demo) — The catalogue tables in this document MUST be regenerable from the packs by a
> documented script so the prose index never diverges from the machine-readable truth.

> **DQ-RULE-012** (P0, inspection) — Rules MUST NOT embed PHI literals (names, MRNs, dates of birth) in expressions,
> fixtures or hints; fixtures MUST use synthetic data (module 11).

> **DQ-RULE-013** (P0, test) — Every TKG-01 §9 contract clause that names a DQ rule family (C1–C6, C9) MUST be covered by
> an `active` rule in the `temporal-and-provenance` pack with a graph target and a warehouse target; the compiler MUST
> instantiate every rule tagged `templated-bitemporal` over every table that carries the bitemporal block (EDW-ARCH-003)
> and every `healix-time:BitemporalAssertion`, reporting results per table under the same rule id.
