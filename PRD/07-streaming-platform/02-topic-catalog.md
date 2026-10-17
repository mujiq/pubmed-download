---
title: Topic Catalog
module: 07-streaming-platform
doc_id: STR-02
version: 0.1.0
status: draft
owners: [Data Platform, Integration Engineering, Subject-area stewards G1–G9]
depends_on: [ARC-01, STR-01, STR-03, SRC-00, IOP-00, AGT-00, GOV-00]
last_updated: 2026-10-17
---

# Topic Catalog

`topics.yaml` is the **machine-readable source of truth** for every Kafka topic in HEALIX; this document is its prose
companion. It explains the catalog schema, the naming rules, the lifecycle by which a topic is proposed, reviewed and
created, who owns what, the defaults each layer inherits, and the full initial topic list. If this document and
`topics.yaml` disagree, `topics.yaml` wins and this document is corrected.

## 1. Catalog schema

The catalog is validated in CI against [`topics.schema.json`](topics.schema.json) (JSON Schema 2020-12). Each entry has
the following fields:

| Field | Type | Meaning |
|-------|------|---------|
| `name` | string | Topic name, matching the regex in §2 |
| `layer` | enum | `raw`, `std`, `canonical`, `dq`, `kg`, `edw`, `ops`; must equal the second segment of `name` |
| `domain` | string | Group domain (`party` … `governance`) for `canonical`/`dq`/`kg`/`edw`/`ops`; **source-named** domain (`cerner`, `fusion`, `lis`, `clearinghouse`, `hie`, `outbound` …) for `raw`/`std` |
| `group` | enum | `G1`–`G9` per `01-architecture-overview.md` §4, or `source` for `raw`/`std` |
| `owner` | string | Accountable team or stewardship group |
| `source_feed` | string (optional) | Human-readable feed, table or BICC PVO carried by a `raw`/`std` topic |
| `key` | string | Partition key definition (STR-01 §5.1) |
| `key_format` | enum | `string` or `avro` |
| `value_format` | enum | `avro`, `avro-envelope`, `fhir-json`, `fhir-ext-json`, `json`, `protobuf` (STR-03) |
| `schema_subject` | string | Always `<name>-value` (TopicNameStrategy) |
| `compatibility` | enum | Schema Registry compatibility mode; must equal the layer default |
| `partitions` | integer | Immutable for the topic version (STR-TOP-013) |
| `retention_ms` **or** `cleanup_policy: compact` | integer / const | Exactly one of the two |
| `phi_class` | enum | `phi`, `phi-limited`, `de-identified`, `pii`, `non-phi` (module 11 scheme) |
| `producers`, `consumers` | list of principals | `connect.*`, `proc.*`, `sink.*`, `agent.*`, `svc.*`, `ci.*`, `sre.*`; these generate the ACLs (STR-TOP-007) |
| `description` | string | Purpose; starts with the layer label |

Top-level `defaults` carry replication factor 3, `min.insync.replicas` 2, compacted-topic settings, retention and
compatibility per layer, and the allowed enumerations. Wildcard principals (`proc.*`, `agent.*`) are permitted only on
`ops` topics.

## 2. Naming rules

```
hc.<layer>.<domain>[.<subdomain>].<entity>.<event-or-format>.v<N>
^hc\.(raw|std|canonical|dq|kg|edw|ops)(\.[a-z0-9][a-z0-9_-]*){1,5}\.v[1-9][0-9]*$
```

- Lowercase; segments separated by dots; words inside a segment joined by `-` (canonical FHIR extension entities such
  as `gl-journal-line`) or `_` when mirroring a source identifier (`journal_line`, `clinical_event`).
- `raw`/`std` use the **source** as domain and MAY add a sub-domain for the source module or feed family
  (`cerner.cdc.encounter`, `fusion.gl.journal_line`). The final segment before `vN` is the **wire format** (`hl7v2`,
  `avro`, `bicc`, `835`, `csv`, `dicom-json`) in `raw` and the **parsed form** (`hl7v2-parsed`, `typed`, `835-parsed`,
  `validated`) in `std`.
- `canonical` uses the **group domain** and the FHIR resource type in lowercase as entity; format is `fhir` for FHIR R4
  resources and `fhir-ext` for HEALIX-ERP extension resources; the compacted companion appends `-snapshot`.
- Platform layers (`dq`, `kg`, `edw`, `ops`) MAY omit domain and format segments (`hc.dq.result.v1`,
  `hc.kg.assertion.v1`, `hc.edw.cdc.g2.v1`, `hc.ops.lineage.openlineage.v1`).
- `vN` is the **contract version**. It changes only on an incompatible schema change, a partition-count change or a key
  change (CONVENTIONS §5). Compatible schema evolution does not bump `vN`.
- Reserved: topics starting with `_` (internal), `connect-` (Connect internals) and `hc.ops.dlq.` (written by the
  platform only).

## 3. Lifecycle: propose → review → create

```mermaid
stateDiagram-v2
  [*] --> Proposed: change request on hc.ops.agent.changerequest.v1<br/>(agent.source_mapper / agent.inventory / engineer)
  Proposed --> Review: catalog linter + schema validation pass
  Review --> Approved: agent.evaluator score >= threshold<br/>and agent.privacy_guardian approval when phi_class in (phi, phi-limited, pii)
  Review --> Rejected
  Approved --> Merged: PR to topics.yaml merged by agent.release_manager
  Merged --> Created: GitOps applies topic, ACLs, quotas, schema subject compatibility
  Created --> Active: first producer registered; consumers onboarded
  Active --> Deprecated: successor vN+1 created; dual-publish window >= 90 days
  Deprecated --> Retired: zero consumers for 30 days; topic deleted, catalog entry kept with status retired
  Retired --> [*]
```

1. **Propose.** Any agent or engineer opens a typed change request (`kind: topic.create|topic.evolve|topic.deprecate`)
   with the full catalog entry, the schema (STR-03) and the Source Registry feed id (module 03) for `raw` topics.
2. **Review.** CI runs `topics.schema.json` validation, the catalog linter (unique names, layer/name agreement,
   `schema_subject` = `<name>-value`, retention floors, source-named `raw`/`std` domains, every `raw` has a `std`
   counterpart) and schema compatibility. `agent.evaluator` scores completeness and consistency with the ontology
   (entity bound to an ontology class IRI). PHI-bearing topics require `agent.privacy_guardian` approval.
3. **Create.** `agent.release_manager` merges; the GitOps controller creates the topic with the catalog's parameters,
   applies ACLs for the listed principals, registers the subject's compatibility mode and emits
   `hc.ops.schema.change.v1`. Brokers run `auto.create.topics.enable=false`, so this is the only way a topic exists.
4. **Deprecate/retire.** A successor version is dual-published for at least 90 days; the old topic is deleted once no
   consumer group has committed an offset for 30 days. Catalog entries are never deleted, only marked retired in Git
   history, preserving lineage.

A nightly **drift check** compares the clusters' topic configurations and ACLs with the catalog and opens an issue for
any difference.

## 4. Ownership by group

| Group | Kafka domain | Owner (steward team) | Owns topics in |
|-------|--------------|----------------------|----------------|
| source | `cerner`, `fusion`, `lis`, `ris`, `pacs`, `pharmacy`, `clearinghouse`, `payer`, `hie`, `portal`, `devices`, `credentialing`, `timekeeping`, `outbound` | Integration Engineering (per source team) | `raw`, `std` |
| G1 | `party` | G1 Party & Identity stewards | `canonical.party.*`, `dq.quarantine.party`, `edw.cdc.g1` |
| G2 | `clinical` | G2 Clinical data stewards | `canonical.clinical.*`, `dq.quarantine.clinical`, `edw.cdc.g2` |
| G3 | `finance` | G3 Revenue Cycle & Finance stewards | `canonical.finance.*`, `dq.quarantine.finance`, `edw.cdc.g3` |
| G4 | `supply` | G4 Supply Chain & Operations stewards | `canonical.supply.*`, `dq.quarantine.supply`, `edw.cdc.g4` |
| G5 | `workforce` | G5 Workforce stewards | `canonical.workforce.*`, `dq.quarantine.workforce`, `edw.cdc.g5` |
| G6 | `pophealth` | G6 Population Health stewards | `canonical.pophealth.*`, `dq.quarantine.pophealth`, `edw.cdc.g6` |
| G7 | `research` | G7 Research & Analytics stewards | `canonical.research.*`, `dq.quarantine.research`, `edw.cdc.g7` |
| G8 | `reference` | G8 Reference & Terminology stewards | `canonical.reference.*`, `dq.quarantine.reference`, `edw.cdc.g8` |
| G9 | `governance` | G9 Metadata, Quality & Governance (Data Platform) | `dq.result/issue/scorecard/rule-activation`, `kg.*`, `edw.cdc.g9`, all `ops.*` |

Owners approve consumer onboarding, set DQ blocking thresholds for their quarantine topics (module 09) and are paged
for consumer-lag SLO breaches on their topics (module 12).

## 5. Defaults by layer

| Layer | Partitions (default) | Retention / policy | Value format | Compatibility | Typical PHI |
|-------|----------------------|--------------------|--------------|---------------|-------------|
| `raw` | 6 (HL7 high-volume 12–24; BICC 3–6) | 30 d | `avro-envelope` (opaque) / `avro` (rows) | `BACKWARD` | `phi` |
| `std` | same as its `raw` | 14 d | `avro` / `fhir-json` | `BACKWARD` | same as `raw` |
| `canonical` events | 6–12 (Encounter 24, Observation 48) | 90 d | `fhir-json` / `fhir-ext-json` | `BACKWARD_TRANSITIVE` | per resource |
| `canonical` snapshots | same as event topic | compact | same | `BACKWARD_TRANSITIVE` | same |
| `dq` | 6 (result 24) | 90 d (scorecard, rule-activation compact) | `avro` | `BACKWARD_TRANSITIVE` | `phi-limited` |
| `kg` | 24 / 6 / 3 | 30 d | `avro` | `FULL` | `phi` |
| `edw` | 12 (G2 24) | 14 d | `avro` | `FULL` | per group |
| `ops` | 3–6 | 7 d (30 d exceptions) | `json` / `avro` / `avro-envelope` | `BACKWARD` | `non-phi` (DLQ `phi`) |

Deviation from a default is permitted only through the catalog entry and must be justified in the change request.

## 6. Initial topic list

Counts: 48 `raw`, 50 `std` (48 parsed counterparts + 2 outbound), 60 `canonical` (51 event/compact-only + 9 snapshots),
12 `dq`, 3 `kg`, 9 `edw`, 11 `ops` — **193 topics**, 14 compacted.

### 6.1 Raw and standardised layers (per source and feed)

Every registered feed has one `raw` topic and one `std` counterpart with the same key and partition count. Keys for HL7
feeds are `MSH-4|PID-3.1`, falling back to `MSH-10` for messages without a PID segment (STR-01 §5.1).

| Source feed | raw topic | std topic | Key | Parts | PHI |
|---|---|---|---|---|---|
| Cerner ADT (A01-A60) | `hc.raw.cerner.adt.hl7v2.v1` | `hc.std.cerner.adt.hl7v2-parsed.v1` | `MSH-4\|PID-3.1` | 24 | phi |
| Cerner ORM/OMG/OMP | `hc.raw.cerner.orm.hl7v2.v1` | `hc.std.cerner.orm.hl7v2-parsed.v1` | `MSH-4\|PID-3.1` | 12 | phi |
| Cerner ORU^R01 | `hc.raw.cerner.oru.hl7v2.v1` | `hc.std.cerner.oru.hl7v2-parsed.v1` | `MSH-4\|PID-3.1` | 24 | phi |
| Cerner SIU^S12-S26 | `hc.raw.cerner.siu.hl7v2.v1` | `hc.std.cerner.siu.hl7v2-parsed.v1` | `MSH-4\|PID-3.1` | 6 | phi |
| Cerner MDM^T02/T08 | `hc.raw.cerner.mdm.hl7v2.v1` | `hc.std.cerner.mdm.hl7v2-parsed.v1` | `MSH-4\|PID-3.1` | 6 | phi |
| Cerner DFT^P03 | `hc.raw.cerner.dft.hl7v2.v1` | `hc.std.cerner.dft.hl7v2-parsed.v1` | `MSH-4\|PID-3.1` | 12 | phi |
| Cerner RDE/RAS | `hc.raw.cerner.rde.hl7v2.v1` | `hc.std.cerner.rde.hl7v2-parsed.v1` | `MSH-4\|PID-3.1` | 12 | phi |
| Cerner VXU^V04 | `hc.raw.cerner.vxu.hl7v2.v1` | `hc.std.cerner.vxu.hl7v2-parsed.v1` | `MSH-4\|PID-3.1` | 3 | phi |
| Cerner MFN^M02/M05/M08 | `hc.raw.cerner.mfn.hl7v2.v1` | `hc.std.cerner.mfn.hl7v2-parsed.v1` | `MFI-1\|MFE-4` | 3 | non-phi |
| V500.PERSON + PERSON_ALIAS | `hc.raw.cerner.cdc.person.avro.v1` | `hc.std.cerner.cdc.person.typed.v1` | `person_id` | 12 | phi |
| V500.ENCOUNTER + ENCNTR_ALIAS | `hc.raw.cerner.cdc.encounter.avro.v1` | `hc.std.cerner.cdc.encounter.typed.v1` | `encntr_id` | 12 | phi |
| V500.CLINICAL_EVENT (+CE_*) | `hc.raw.cerner.cdc.clinical_event.avro.v1` | `hc.std.cerner.cdc.clinical_event.typed.v1` | `event_id` | 24 | phi |
| V500.ORDERS + ORDER_DETAIL | `hc.raw.cerner.cdc.orders.avro.v1` | `hc.std.cerner.cdc.orders.typed.v1` | `order_id` | 12 | phi |
| V500.DIAGNOSIS + NOMENCLATURE | `hc.raw.cerner.cdc.diagnosis.avro.v1` | `hc.std.cerner.cdc.diagnosis.typed.v1` | `diagnosis_id` | 6 | phi |
| V500.PROCEDURE | `hc.raw.cerner.cdc.procedure.avro.v1` | `hc.std.cerner.cdc.procedure.typed.v1` | `procedure_id` | 6 | phi |
| V500.ALLERGY + REACTION | `hc.raw.cerner.cdc.allergy.avro.v1` | `hc.std.cerner.cdc.allergy.typed.v1` | `allergy_id` | 3 | phi |
| V500.PRSNL + PRSNL_ALIAS | `hc.raw.cerner.cdc.prsnl.avro.v1` | `hc.std.cerner.cdc.prsnl.typed.v1` | `person_id` | 3 | pii |
| V500.ORGANIZATION | `hc.raw.cerner.cdc.organization.avro.v1` | `hc.std.cerner.cdc.organization.typed.v1` | `organization_id` | 3 | non-phi |
| V500.CODE_VALUE + CODE_SET | `hc.raw.cerner.cdc.code_value.avro.v1` | `hc.std.cerner.cdc.code_value.typed.v1` | `code_value` | 3 | non-phi |
| Millennium FHIR R4 $export | `hc.raw.cerner.fhir.bulk.ndjson.v1` | `hc.std.cerner.fhir.bulk.validated.v1` | `resourceType\|id` | 12 | phi |
| FscmTopModelAM.FinGlJournalsAM.JournalLinePVO | `hc.raw.fusion.gl.journal_line.bicc.v1` | `hc.std.fusion.gl.journal_line.typed.v1` | `je_header_id\|je_line_num` | 6 | non-phi |
| FinGlCodeCombinationPVO | `hc.raw.fusion.gl.code_combination.bicc.v1` | `hc.std.fusion.gl.code_combination.typed.v1` | `code_combination_id` | 3 | non-phi |
| FinApInvoicesPVO + InvoiceLinesPVO | `hc.raw.fusion.ap.invoice.bicc.v1` | `hc.std.fusion.ap.invoice.typed.v1` | `invoice_id` | 6 | non-phi |
| PrcPoPurchaseOrderLinePVO | `hc.raw.fusion.po.order_line.bicc.v1` | `hc.std.fusion.po.order_line.typed.v1` | `po_line_id` | 6 | non-phi |
| RcvTransactionPVO | `hc.raw.fusion.po.receipt_line.bicc.v1` | `hc.std.fusion.po.receipt_line.typed.v1` | `rcv_transaction_id` | 3 | non-phi |
| EgpItemPVO | `hc.raw.fusion.inv.item.bicc.v1` | `hc.std.fusion.inv.item.typed.v1` | `inventory_item_id\|organization_id` | 3 | non-phi |
| InvOnhandQuantitiesPVO | `hc.raw.fusion.inv.onhand_balance.bicc.v1` | `hc.std.fusion.inv.onhand_balance.typed.v1` | `inventory_item_id\|organization_id\|subinventory_code` | 6 | non-phi |
| PozSupplierPVO + SitePVO | `hc.raw.fusion.scm.supplier.bicc.v1` | `hc.std.fusion.scm.supplier.typed.v1` | `vendor_id` | 3 | non-phi |
| PerAllPeoplePVO + PersonNamesPVO | `hc.raw.fusion.hcm.worker.bicc.v1` | `hc.std.fusion.hcm.worker.typed.v1` | `person_id` | 6 | pii |
| PerAllAssignmentsMPVO | `hc.raw.fusion.hcm.assignment.bicc.v1` | `hc.std.fusion.hcm.assignment.typed.v1` | `assignment_id` | 6 | pii |
| HcmPositionsPVO | `hc.raw.fusion.hcm.position.bicc.v1` | `hc.std.fusion.hcm.position.typed.v1` | `position_id` | 3 | non-phi |
| FaAdditionsPVO + FaBooksPVO | `hc.raw.fusion.fa.asset.bicc.v1` | `hc.std.fusion.fa.asset.typed.v1` | `asset_id` | 3 | non-phi |
| LIS ORU^R01 | `hc.raw.lis.oru.hl7v2.v1` | `hc.std.lis.oru.hl7v2-parsed.v1` | `MSH-4\|PID-3.1` | 12 | phi |
| RIS ORU^R01 | `hc.raw.ris.oru.hl7v2.v1` | `hc.std.ris.oru.hl7v2-parsed.v1` | `MSH-4\|PID-3.1` | 6 | phi |
| DICOMweb QIDO-RS study metadata | `hc.raw.pacs.study.dicom-json.v1` | `hc.std.pacs.study.typed.v1` | `study_instance_uid` | 6 | phi |
| Pharmacy RDS^O13 | `hc.raw.pharmacy.rds.hl7v2.v1` | `hc.std.pharmacy.rds.hl7v2-parsed.v1` | `MSH-4\|PID-3.1` | 6 | phi |
| X12 005010X222/X223 837P/837I | `hc.raw.clearinghouse.x12.837.v1` | `hc.std.clearinghouse.x12.837-parsed.v1` | `isa13\|gs06\|st02` | 6 | phi |
| X12 005010X221 835 | `hc.raw.clearinghouse.x12.835.v1` | `hc.std.clearinghouse.x12.835-parsed.v1` | `isa13\|gs06\|st02` | 6 | phi |
| X12 005010X214 277CA | `hc.raw.clearinghouse.x12.277.v1` | `hc.std.clearinghouse.x12.277-parsed.v1` | `isa13\|gs06\|st02` | 3 | phi |
| X12 005010X279 271 | `hc.raw.payer.x12.271.v1` | `hc.std.payer.x12.271-parsed.v1` | `isa13\|gs06\|st02` | 6 | phi |
| X12 005010X217 278 | `hc.raw.payer.x12.278.v1` | `hc.std.payer.x12.278-parsed.v1` | `isa13\|gs06\|st02` | 3 | phi |
| Payer attribution/roster files | `hc.raw.payer.attribution.csv.v1` | `hc.std.payer.attribution.csv-parsed.v1` | `payer_id\|member_id` | 3 | phi |
| C-CDA 2.1 via IHE XDS.b / Direct | `hc.raw.hie.ccda.v1` | `hc.std.hie.ccda.parsed.v1` | `document_id` | 6 | phi |
| HIE ADT notifications | `hc.raw.hie.adt.hl7v2.v1` | `hc.std.hie.adt.hl7v2-parsed.v1` | `MSH-4\|PID-3.1` | 6 | phi |
| Portal FHIR Subscription (QuestionnaireResponse) | `hc.raw.portal.questionnaireresponse.fhir-json.v1` | `hc.std.portal.questionnaireresponse.validated.v1` | `patient_id` | 3 | phi |
| Device gateway JSON (IEEE 11073 mapped) | `hc.raw.devices.observation.json.v1` | `hc.std.devices.observation.typed.v1` | `device_id` | 12 | phi |
| Credentialing export (CSV) | `hc.raw.credentialing.practitioner.csv.v1` | `hc.std.credentialing.practitioner.csv-parsed.v1` | `npi` | 3 | pii |
| Timekeeping export (CSV) | `hc.raw.timekeeping.timecard.csv.v1` | `hc.std.timekeeping.timecard.csv-parsed.v1` | `person_number` | 3 | pii |

Outbound HL7 v2 (generated from canonical, STR-05 §2.7):

| Topic | Key | Parts | PHI | Purpose |
|---|---|---|---|---|
| `hc.std.outbound.adt.hl7v2.v1` | `patient_bk` | 12 | phi | ADT for legacy consumers, derived from canonical Encounter/Patient |
| `hc.std.outbound.oru.hl7v2.v1` | `patient_bk` | 12 | phi | ORU^R01 derived from canonical Observation/DiagnosticReport |

### 6.2 Canonical layer (FHIR R4 per domain)

`fhir` topics carry FHIR R4 resources conforming to US Core 6.1 plus HEALIX profiles; `fhir-ext` topics carry
HEALIX-ERP extension resources (module 06). A **snapshot** topic is compacted and keyed identically; it exists for the
conformed-dimension entities (Patient, Practitioner, Organization, Location, Encounter, Account, Coverage, Item,
Worker). Reference topics are compact-only (no separate event topic).

| Group | Event topic | FHIR resource / profile | Key | Parts | PHI | Snapshot |
|---|---|---|---|---|---|---|
| G1 | `hc.canonical.party.patient.fhir.v1` | Patient (US Core) | `patient_bk (EMPI id)` | 12 | phi | `...fhir-snapshot.v1` |
| G1 | `hc.canonical.party.practitioner.fhir.v1` | Practitioner (US Core) | `practitioner_bk (NPI or source id)` | 6 | pii | `...fhir-snapshot.v1` |
| G1 | `hc.canonical.party.practitionerrole.fhir.v1` | PractitionerRole (US Core) | `practitioner_bk` | 6 | pii | no |
| G1 | `hc.canonical.party.organization.fhir.v1` | Organization (US Core) | `organization_bk` | 3 | non-phi | `...fhir-snapshot.v1` |
| G1 | `hc.canonical.party.location.fhir.v1` | Location (US Core) | `location_bk` | 3 | non-phi | `...fhir-snapshot.v1` |
| G1 | `hc.canonical.party.relatedperson.fhir.v1` | RelatedPerson | `patient_bk` | 3 | phi | no |
| G2 | `hc.canonical.clinical.encounter.fhir.v1` | Encounter (US Core) | `patient_bk` | 24 | phi | `...fhir-snapshot.v1` |
| G2 | `hc.canonical.clinical.condition.fhir.v1` | Condition (US Core) | `patient_bk` | 12 | phi | no |
| G2 | `hc.canonical.clinical.procedure.fhir.v1` | Procedure (US Core) | `patient_bk` | 12 | phi | no |
| G2 | `hc.canonical.clinical.observation.fhir.v1` | Observation (US Core Lab/Vitals) | `patient_bk` | 48 | phi | no |
| G2 | `hc.canonical.clinical.diagnosticreport.fhir.v1` | DiagnosticReport (US Core) | `patient_bk` | 12 | phi | no |
| G2 | `hc.canonical.clinical.servicerequest.fhir.v1` | ServiceRequest (US Core) | `patient_bk` | 12 | phi | no |
| G2 | `hc.canonical.clinical.medicationrequest.fhir.v1` | MedicationRequest (US Core) | `patient_bk` | 12 | phi | no |
| G2 | `hc.canonical.clinical.medicationdispense.fhir.v1` | MedicationDispense | `patient_bk` | 6 | phi | no |
| G2 | `hc.canonical.clinical.medicationadministration.fhir.v1` | MedicationAdministration | `patient_bk` | 12 | phi | no |
| G2 | `hc.canonical.clinical.allergyintolerance.fhir.v1` | AllergyIntolerance (US Core) | `patient_bk` | 6 | phi | no |
| G2 | `hc.canonical.clinical.immunization.fhir.v1` | Immunization (US Core) | `patient_bk` | 3 | phi | no |
| G2 | `hc.canonical.clinical.documentreference.fhir.v1` | DocumentReference (US Core) | `patient_bk` | 6 | phi | no |
| G2 | `hc.canonical.clinical.specimen.fhir.v1` | Specimen | `patient_bk` | 6 | phi | no |
| G2 | `hc.canonical.clinical.imagingstudy.fhir.v1` | ImagingStudy | `patient_bk` | 6 | phi | no |
| G3 | `hc.canonical.finance.account.fhir.v1` | Account | `account_bk` | 12 | phi | `...fhir-snapshot.v1` |
| G3 | `hc.canonical.finance.chargeitem.fhir.v1` | ChargeItem | `account_bk` | 12 | phi | no |
| G3 | `hc.canonical.finance.claim.fhir.v1` | Claim | `claim_bk` | 12 | phi | no |
| G3 | `hc.canonical.finance.claimresponse.fhir.v1` | ClaimResponse | `claim_bk` | 6 | phi | no |
| G3 | `hc.canonical.finance.paymentreconciliation.fhir.v1` | PaymentReconciliation | `claim_bk` | 6 | phi | no |
| G3 | `hc.canonical.finance.coverage.fhir.v1` | Coverage (US Core) | `patient_bk` | 6 | phi | `...fhir-snapshot.v1` |
| G3 | `hc.canonical.finance.invoice.fhir.v1` | Invoice | `invoice_bk` | 6 | phi-limited | no |
| G3 | `hc.canonical.finance.gl-journal-line.fhir-ext.v1` | HEALIX-ERP GLJournalLine | `ledger_id\|je_header_id` | 6 | non-phi | no |
| G3 | `hc.canonical.finance.fixed-asset.fhir-ext.v1` | HEALIX-ERP FixedAsset | `asset_bk` | 3 | non-phi | no |
| G4 | `hc.canonical.supply.item.fhir-ext.v1` | HEALIX-ERP Item | `item_bk` | 3 | non-phi | `...fhir-ext-snapshot.v1` |
| G4 | `hc.canonical.supply.inventory-balance.fhir-ext.v1` | HEALIX-ERP InventoryBalance | `item_bk\|location_bk` | 6 | non-phi | no |
| G4 | `hc.canonical.supply.purchase-order-line.fhir-ext.v1` | HEALIX-ERP PurchaseOrderLine | `po_header_bk` | 6 | non-phi | no |
| G4 | `hc.canonical.supply.supplyrequest.fhir.v1` | SupplyRequest | `item_bk\|location_bk` | 6 | non-phi | no |
| G4 | `hc.canonical.supply.supplydelivery.fhir.v1` | SupplyDelivery | `item_bk\|location_bk` | 6 | phi-limited | no |
| G4 | `hc.canonical.supply.appointment.fhir.v1` | Appointment | `patient_bk` | 12 | phi | no |
| G4 | `hc.canonical.supply.slot.fhir.v1` | Schedule/Slot | `location_bk` | 3 | non-phi | no |
| G5 | `hc.canonical.workforce.worker.fhir-ext.v1` | HEALIX-ERP Worker | `worker_bk (person number)` | 6 | pii | `...fhir-ext-snapshot.v1` |
| G5 | `hc.canonical.workforce.assignment.fhir-ext.v1` | HEALIX-ERP Assignment | `worker_bk` | 6 | pii | no |
| G5 | `hc.canonical.workforce.position.fhir-ext.v1` | HEALIX-ERP Position | `position_bk` | 3 | non-phi | no |
| G5 | `hc.canonical.workforce.credential.fhir-ext.v1` | HEALIX-ERP Credential | `worker_bk` | 3 | pii | no |
| G5 | `hc.canonical.workforce.timecard.fhir-ext.v1` | HEALIX-ERP Timecard | `worker_bk` | 6 | pii | no |
| G6 | `hc.canonical.pophealth.measurereport.fhir.v1` | MeasureReport | `measure_bk\|period` | 3 | phi-limited | no |
| G6 | `hc.canonical.pophealth.sdoh-observation.fhir.v1` | Observation (SDOH Clinical Care) | `patient_bk` | 6 | phi | no |
| G6 | `hc.canonical.pophealth.caregap.fhir-ext.v1` | HEALIX CareGap | `patient_bk` | 6 | phi | no |
| G6 | `hc.canonical.pophealth.attribution.fhir-ext.v1` | HEALIX Attribution | `patient_bk` | 6 | phi | no |
| G7 | `hc.canonical.research.researchstudy.fhir.v1` | ResearchStudy | `study_bk` | 3 | non-phi | no |
| G7 | `hc.canonical.research.researchsubject.fhir.v1` | ResearchSubject | `study_bk` | 3 | phi | no |
| G7 | `hc.canonical.research.consent.fhir.v1` | Consent | `patient_bk` | 3 | phi | no |
| G8 | `hc.canonical.reference.codesystem.fhir.v1` | CodeSystem | `url\|version` | 3 | non-phi | compact-only |
| G8 | `hc.canonical.reference.valueset.fhir.v1` | ValueSet | `url\|version` | 3 | non-phi | compact-only |
| G8 | `hc.canonical.reference.conceptmap.fhir.v1` | ConceptMap | `url\|version` | 3 | non-phi | compact-only |

### 6.3 Data-quality layer

| Topic | Key | Parts | Policy | PHI | Purpose |
|---|---|---|---|---|---|
| `hc.dq.result.v1` | `subject_key` | 24 | 90 d | phi-limited | One verdict per (rule, record) with pointer to the evaluated topic/partition/offset; no payload copy |
| `hc.dq.issue.v1` | `issue_id` | 3 | 90 d | phi-limited | Aggregated issues (anomaly, drift, breach) and remediation ticket lifecycle |
| `hc.dq.scorecard.v1` | `scope_id` | 3 | compact | non-phi | Composite DQ index and dimension scores per domain/rule-set/period |
| `hc.dq.rule-activation.v1` | `rule_id` | 3 | compact | non-phi | Active rule versions and thresholds broadcast to evaluators |
| `hc.dq.quarantine.<domain>.v1` for `party`, `clinical`, `finance`, `supply`, `workforce`, `pophealth`, `research`, `reference` | key of the quarantined record | 6 | 90 d | as domain | Canonical records failing a blocking rule, wrapped with failing rule ids and remediation state |

### 6.4 Knowledge-graph layer

| Topic | Key | Parts | Policy | PHI | Purpose |
|---|---|---|---|---|---|
| `hc.kg.assertion.v1` | `subject_iri` | 24 | 30 d | phi | Bitemporal RDF-star assertion batches (valid time, transaction time, PROV-O activity) |
| `hc.kg.retraction.v1` | `subject_iri` | 6 | 30 d | phi | Closes valid or transaction time; never a physical delete |
| `hc.kg.load-receipt.v1` | `batch_ulid` | 3 | 30 d | non-phi | Per named-graph batch receipt for KG/EDW reconciliation |

### 6.5 EDW layer

One topic per subject-area group; records are row-shaped upsert/close events for `edw_gN_*` tables with bitemporal
columns populated and surrogate keys resolved (STR-05 §2.6).

| Group | Subject area | Topic | Target schemas | Parts | PHI |
|---|---|---|---|---|---|
| G1 | Party & Identity | `hc.edw.cdc.g1.v1` | `edw_g1_*` | 12 | phi |
| G2 | Clinical Care | `hc.edw.cdc.g2.v1` | `edw_g2_*` | 24 | phi |
| G3 | Revenue Cycle & Finance | `hc.edw.cdc.g3.v1` | `edw_g3_*` | 12 | phi |
| G4 | Supply Chain & Operations | `hc.edw.cdc.g4.v1` | `edw_g4_*` | 12 | phi-limited |
| G5 | Workforce | `hc.edw.cdc.g5.v1` | `edw_g5_*` | 12 | pii |
| G6 | Population Health & Quality | `hc.edw.cdc.g6.v1` | `edw_g6_*` | 12 | phi |
| G7 | Research & Analytics | `hc.edw.cdc.g7.v1` | `edw_g7_*` | 12 | phi |
| G8 | Reference & Terminology | `hc.edw.cdc.g8.v1` | `edw_g8_*` | 12 | non-phi |
| G9 | Metadata, Quality & Governance | `hc.edw.cdc.g9.v1` | `edw_g9_*` | 12 | phi-limited |

### 6.6 Operations layer

| Topic | Key | Format | Parts | PHI | Retention | Purpose |
|---|---|---|---|---|---|---|
| `hc.ops.lineage.openlineage.v1` | `run_id` | json | 6 | non-phi | 7 d | OpenLineage RunEvent JSON (START/COMPLETE/FAIL) for every job run; projected to PROV-O in the KG. |
| `hc.ops.agent.changerequest.v1` | `cr_id` | json | 3 | non-phi | 30 d | Typed change requests (vocabulary, mapping, schema, DQ rule, topic) emitted by agents; schema owned by module 10. |
| `hc.ops.agent.evaluation.v1` | `cr_id` | json | 3 | non-phi | 30 d | Evaluator scores, rubric results and veto decisions per change request. |
| `hc.ops.audit.v1` | `correlation_id` | avro | 6 | phi-limited | 30 d | FHIR AuditEvent-shaped records for PHI access, config change and admin actions; sunk to immutable storage within 60 s. |
| `hc.ops.dlq.connect.v1` | `source_topic\|connector` | avro-envelope | 6 | phi | 30 d | Dead letters from Kafka Connect source/sink tasks with error headers and original record bytes. |
| `hc.ops.dlq.processor.v1` | `source_topic\|processor` | avro-envelope | 6 | phi | 30 d | Dead letters from stream processors (unparseable, unmappable, schema-incompatible) with stack/diagnostic headers. |
| `hc.ops.connector.bicc.manifest.v1` | `extract_job_id` | avro | 3 | non-phi | 30 d | BICC extract manifests (PVO, files, row counts, watermark) used for exactly-once file processing and completeness checks. |
| `hc.ops.replay.request.v1` | `replay_id` | avro | 3 | non-phi | 30 d | Replay/backfill requests and their state transitions (requested, approved, running, completed). |
| `hc.ops.schema.change.v1` | `subject` | avro | 3 | non-phi | 30 d | Schema Registry subject version events emitted by the GitOps pipeline (registered, deprecated, compatibility changed). |
| `hc.ops.outbound.hl7v2.ack.v1` | `message_control_id` | avro | 3 | phi-limited | 7 d | ACK/NAK outcomes for outbound HL7 v2 messages (MSA-1, MSA-3, retries). |
| `hc.ops.processor.checkpoint.v1` | `processor\|partition` | avro | 3 | non-phi | 7 d | Watermark and checkpoint progress per processor and partition for lag and late-data dashboards. |

## 7. Consumer group naming

Consumer groups are `<principal>.<purpose>` (e.g., `proc.canonicaliser.clinical`, `sink.edw.g2`,
`agent.dq_monitor.issues`). Replays use `<principal>.replay.<replay_id>` so that live and replay lag are observable
separately (STR-05 §7). ACLs grant each principal only its own group prefix.

## 8. Validation

CI and agents validate the catalog with:

```bash
python3 -I -c "import json,sys,yaml,jsonschema; \
  cat=yaml.safe_load(open('PRD/07-streaming-platform/topics.yaml')); \
  jsonschema.Draft202012Validator(json.load(open('PRD/07-streaming-platform/topics.schema.json')), \
    format_checker=jsonschema.FormatChecker()).validate(cat); print(len(cat['topics']),'topics valid')"
```

followed by the linter rules in §3 step 2. Result for this version: 193 topics valid, 0 lint findings.

## 9. Requirements

> **STR-TOP-016** (P0, test) — `topics.yaml` MUST be the only mechanism by which topics are created, altered or
> deleted; brokers MUST run with `auto.create.topics.enable=false` and the GitOps controller MUST be the sole principal
> with `Create`/`Alter`/`Delete` on topics.

> **STR-TOP-017** (P0, test) — Every topic name MUST match the regex in §2, use a source-named domain in `raw`/`std`
> and a group domain in `canonical`, and encode its contract version as `vN`.

> **STR-TOP-018** (P0, test) — The catalog MUST validate against `topics.schema.json` and the linter rules of §3 in CI;
> a failing validation MUST block the merge.

> **STR-TOP-019** (P0, inspection) — Every entry MUST declare `owner`, `phi_class`, `key`, `producers` and
> `consumers`; ACLs MUST be generated from these lists and from nothing else.

> **STR-TOP-020** (P0, test) — Topic creation, evolution and deprecation MUST flow through a typed change request on
> `hc.ops.agent.changerequest.v1`, evaluated by `agent.evaluator`, with `agent.privacy_guardian` approval for
> `phi`, `phi-limited` and `pii` topics.

> **STR-TOP-021** (P1, inspection) — A deprecated topic version MUST be dual-published for at least 90 days and MUST NOT
> be deleted while any consumer group has committed an offset within the last 30 days.

> **STR-TOP-022** (P1, inspection) — Deviations from the layer defaults in §5 MUST be justified in the change request
> and recorded in the catalog entry's `description`.

> **STR-TOP-023** (P0, test) — Every feed registered in the Source Registry (module 03) MUST have exactly one `raw`
> topic and one `std` counterpart with the same key definition and partition count.

> **STR-TOP-024** (P0, inspection) — The `canonical` layer MUST have one event topic per FHIR resource type per domain
> and a compacted `-snapshot` topic for every conformed-dimension entity (Patient, Practitioner, Organization,
> Location, Encounter, Account, Coverage, Item, Worker).

> **STR-TOP-025** (P0, test) — `hc.dq.result.v1` MUST reference the evaluated record by topic, partition, offset and
> key and MUST NOT copy its payload; every group domain except `governance` MUST have a quarantine topic.

> **STR-TOP-026** (P0, inspection) — Exactly one `hc.edw.cdc.gN.v1` topic MUST exist per subject-area group, keyed by
> `table_name|business_key`, with `FULL` compatibility.

> **STR-TOP-027** (P0, inspection) — The `ops` layer MUST provide lineage, audit, dead-letter, change-request,
> evaluation, replay and schema-change topics; wildcard producer principals are permitted only here.

> **STR-TOP-028** (P1, analysis) — Each `canonical` catalog entry SHOULD reference the ontology class IRI of its entity
> (module 02) and each `raw` entry SHOULD reference its Source Registry feed id, enabling the semantic-coverage KPI.

> **STR-TOP-029** (P1, test) — A nightly drift check MUST compare topic configuration and ACLs on every cluster with
> the catalog and open an issue on `hc.dq.issue.v1` for any difference.

> **STR-TOP-030** (P1, test) — Consumer group ids MUST follow `<principal>.<purpose>` and replay groups
> `<principal>.replay.<replay_id>`; ACLs MUST restrict each principal to its own group prefix.
