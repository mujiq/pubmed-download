---
title: Document Index
module: 99-appendices
doc_id: APX-04
version: 0.1.0
status: draft
owners: [Programme Management Office, Enterprise Architecture]
depends_on: [VIS-00, ARC-01]
last_updated: 2026-10-17
---

# Document Index

Complete index of PRD files by module with `doc_id` and one-line purpose. Filenames reflect the repository as of the
`last_updated` date; entries marked *(planned)* are specified by the module table in `PRD/README.md` or by a module
README but not yet present. Requirement prefixes follow `CONVENTIONS.md` §2. Machine-readable artefacts (YAML, SQL,
JSON, Turtle, TriG) are listed under their module and carry no `doc_id`.

## Root

| File | doc_id | Purpose |
|------|--------|---------|
| `PRD/README.md` | — | Programme summary (HEALIX), how to read the PRD, document map, modularity principles, licensing note |
| `PRD/CONVENTIONS.md` | — | Front matter, requirement IDs, naming (topics, EDW, IRIs, DQ rules, agents), time/provenance, versioning, definition of done, diagram conventions |
| `PRD/00-vision-and-scope.md` | VIS-00 | Problem statement, vision, goals G-01..G-06 with measures, non-goals, personas, scope matrix, guiding principles, KPIs |
| `PRD/01-architecture-overview.md` | ARC-01 | Layered reference architecture, end-to-end data flow, technology baseline, subject-area groups G1–G9, agent roster, cross-module contracts, ADRs, NFRs ARC-NFR-001..007 |

## 02 — Ontology Pipeline (prefix `ONT`)

| File | doc_id | Purpose |
|------|--------|---------|
| `02-ontology-pipeline/README.md` | ONT-00 | Module scope: six-stage pipeline (Stage 0–5) from metadata inventory to knowledge graph, interfaces (Ontology Registry API), requirement index |
| `02-ontology-pipeline/01-metadata-and-inventory.md` | ONT-01 | Stage 0: Source Registry and Data Element Catalog, harvesting by `agent.inventory`, steward assignment, inventory completeness |
| `02-ontology-pipeline/02-controlled-vocabularies.md` | ONT-02 | Stage 1: SKOS concept schemes, labels, definitions, scope notes, code bindings, editorial rules (`ONT-CV-*`) |
| `02-ontology-pipeline/03-taxonomy.md` | ONT-03 | Stage 2: hierarchies, facets, polyhierarchy policy, integrity checks, dimension-hierarchy alignment |
| `02-ontology-pipeline/04-thesaurus.md` | ONT-04 | Stage 3: associative and equivalence relations, ISO 25964 conformance, mappings to SNOMED/LOINC/local systems |
| `02-ontology-pipeline/05-ontology.md` | ONT-05 | Stage 4: OWL 2 DL classes/properties/axioms, SHACL shape library, reasoner gates, promotion rules from thesaurus |
| `02-ontology-pipeline/06-knowledge-graph.md` | ONT-06 | Stage 5: instance population against ontology and shapes, competency questions, hand-off to module 08 |
| `02-ontology-pipeline/07-governance-and-versioning.md` | ONT-07 | Ontology Registry API contract, release train, version IRIs, deprecation, change classes, steward roles |
| `02-ontology-pipeline/skeleton.skos.ttl` | — | SKOS skeleton (concept schemes per group) |
| `02-ontology-pipeline/skeleton.owl.ttl` | — | OWL 2 ontology skeleton (base IRI, top-level classes and properties) |
| `02-ontology-pipeline/skeleton.shacl.ttl` | — | SHACL shape library skeleton |

## 03 — Source Systems (prefix `SRC`)

| File | doc_id | Purpose |
|------|--------|---------|
| `03-source-systems/README.md` | SRC-00 | Module scope, Source Registry contract (§Registry), extraction and mapping contract overview, requirement index |
| `03-source-systems/01-cerner-millennium.md` | SRC-01 | Cerner Millennium: HL7 v2 interfaces (ADT/ORU/ORM/SIU/MDM), CDC on replica, FHIR APIs, CODE_VALUE handling (`SRC-CER-*`) |
| `03-source-systems/02-oracle-fusion.md` | SRC-02 | Oracle Fusion ERP/HCM/SCM: BICC extracts, REST, lookups and flexfields, object inventory (`SRC-OFC-*`) |
| `03-source-systems/03-peripheral-systems.md` | SRC-03 | LIS, RIS/PACS metadata, pharmacy, RCM/clearinghouse X12, HIE, portal, devices, payers (`SRC-PER-*`) |
| `03-source-systems/04-source-to-canonical-mapping.md` | SRC-04 | Mapping matrix format, `std` schema contract, source → FHIR canonical mapping rules, steward sign-off, onboarding by configuration |

## 04 — Reference Data Models (prefix `RDM`)

| File | doc_id | Purpose |
|------|--------|---------|
| `04-reference-data-models/README.md` | RDM-00 | How IBM UDMH and Teradata HCDM are used as licensed blueprints; licensing boundaries; requirement index |
| `04-reference-data-models/01-ibm-udmh.md` | RDM-01 | Generic description of UDMH subject-area structure as consumed; pattern extraction method |
| `04-reference-data-models/02-teradata-hcdm.md` | RDM-02 | Generic description of HCDM subject-area structure as consumed; pattern extraction method |
| `04-reference-data-models/03-model-harmonization.md` | RDM-03 | G1–G9 entity harmonisation across UDMH, HCDM and FHIR resource families; harmonisation matrix; deviation log (`RDM-HAR-*`) |

## 05 — EDW Schema (prefix `EDW`)

| File | doc_id | Purpose |
|------|--------|---------|
| `05-edw-schema/README.md` | EDW-00 | Module scope: conceptual/logical/physical models grouped by G1–G9, naming, history strategy, requirement index |
| `05-edw-schema/01-subject-area-groups.md` | EDW-01 | Subject-area groups G1–G9 as EDW schemas (`edw_gN_*`), ownership, Kafka domain alignment |
| `05-edw-schema/02-conceptual-model.md` | EDW-02 | Conceptual model per group aligned to ontology classes |
| `05-edw-schema/03-logical-model.md` | EDW-03 | Logical models: facts, dimensions, bridges per group (`EDW-G1-*`..`EDW-G9-*`) |
| `05-edw-schema/04-physical-model-and-ddl.md` | EDW-04 | Physical model, Teradata-dialect DDL with portable variant, migration rules |
| `05-edw-schema/05-conformed-dimensions.md` | EDW-05 | Patient, Practitioner, Organization, Location, Date/Time, Payer, Item, Cost Center, Code, Source System |
| `05-edw-schema/06-history-and-bitemporality.md` | EDW-06 | `hist_*` bitemporal tables, SCD2 views, as-of functions, reconciliation with KG |
| `05-edw-schema/ddl/00_schemas.sql` | — | Reference DDL: schema creation for `edw_g1_*`..`edw_g9_*` (further `ddl/*.sql` per schema planned) |

## 06 — Interoperability (prefix `IOP`)

| File | doc_id | Purpose |
|------|--------|---------|
| `06-interoperability/README.md` | IOP-00 | Module scope: FHIR R4 canonical model, inbound formats, terminology services, validation; requirement index |
| `06-interoperability/01-canonical-model-fhir.md` | IOP-01 | HEALIX profiles on US Core 6.1, HEALIX-ERP extension model, resource-to-topic mapping (`IOP-FHIR-*`) |
| `06-interoperability/02-hl7v2-to-fhir.md` | IOP-02 | HL7 v2 conformance profiles and V2-to-FHIR mappings for ADT/ORU/ORM/SIU/MDM (`IOP-V2-*`) |
| `06-interoperability/03-cda-x12-other.md` | IOP-03 | C-CDA 2.1 inbound, X12 005010 (837/835/270/271/278), DICOM metadata and other formats (`IOP-CDA-*`, `IOP-X12-*`) |
| `06-interoperability/04-terminology-services.md` | IOP-04 | FHIR Terminology Service contract: code systems, value sets, concept maps, operations, versioning |
| `06-interoperability/05-validation-and-profiles.md` | IOP-05 | Validator in stream, profile publishing (HEALIX IG), conformance suites, partner conformance, outbound |
| `06-interoperability/profiles/StructureDefinition-healix-patient.json` | — | HEALIX Patient profile |
| `06-interoperability/profiles/StructureDefinition-healix-encounter.json` | — | HEALIX Encounter profile |
| `06-interoperability/profiles/StructureDefinition-healix-observation-lab.json` | — | HEALIX laboratory Observation profile |
| `06-interoperability/profiles/StructureDefinition-healix-erp-gl-journal-line.json` | — | HEALIX-ERP GL journal line extension profile |
| `06-interoperability/profiles/ValueSet-healix-encounter-class.json` | — | Enterprise encounter-class value set |
| `06-interoperability/profiles/ConceptMap-cerner-encounter-type-to-healix.json` | — | Cerner encounter type → enterprise encounter class concept map |
| `06-interoperability/profiles/identifier-systems.yaml` | — | Identifier system URIs used in `Identifier.system` |

## 07 — Streaming Platform (prefix `STR`)

| File | doc_id | Purpose |
|------|--------|---------|
| `07-streaming-platform/README.md` | STR-00 | Module scope: Kafka topology, layers `raw→std→canonical→dq→kg/edw→ops`, requirement index |
| `07-streaming-platform/01-kafka-topology.md` | STR-01 | Clusters, KRaft, partitioning keys, replication, ACL model, multi-facility ordering |
| `07-streaming-platform/02-topic-catalog.md` | STR-02 | Topic catalog rules, headers, retention, compaction, catalog linting (`STR-TOP-*`) |
| `07-streaming-platform/03-schema-registry.md` | STR-03 | Subjects, Avro/JSON Schema, compatibility policies, evolution process |
| `07-streaming-platform/04-cdc-and-connectors.md` | STR-04 | Debezium Oracle, MLLP source, BICC poller, FHIR Subscription bridge, X12/file sources |
| `07-streaming-platform/05-stream-processing.md` | STR-05 | Flink canonicalisation jobs, event time, exactly-once, derived topics, replay |
| `07-streaming-platform/topics.yaml` | — | Machine-readable topic catalog (single source of truth for topics) |
| `07-streaming-platform/topics.schema.json` | — | JSON Schema validating `topics.yaml` |

## 08 — Temporal Knowledge Graph (prefix `TKG`)

| File | doc_id | Purpose |
|------|--------|---------|
| `08-temporal-knowledge-graph/README.md` | TKG-00 | Module scope: bitemporal RDF-star graph, store, ingestion, entity resolution; requirement index |
| `08-temporal-knowledge-graph/01-temporal-model.md` | TKG-01 | Bitemporal model contract: valid time, transaction time, named graphs, PROV-O activities, supersession (`TKG-TIME-*`) |
| `08-temporal-knowledge-graph/02-graph-store-and-query.md` | TKG-02 | Store requirements, scale and partitioning, SPARQL endpoint, as-of and competency-question query patterns |
| `08-temporal-knowledge-graph/03-entity-resolution.md` | TKG-03 | EMPI-style resolution (Fellegi–Sunter), thresholds, review queue, merge/unmerge as supersession |
| `08-temporal-knowledge-graph/04-graph-ingestion-from-kafka.md` | TKG-04 | FHIR→RDF mapping, R2RML/RML, idempotent loading, SHACL pre-publish gate |
| `08-temporal-knowledge-graph/temporal-vocabulary.ttl` | — | HEALIX temporal/provenance vocabulary (RDF-star annotation properties) |
| `08-temporal-knowledge-graph/examples.trig` | — | Example bitemporal named graphs in TriG |

## 09 — Data Quality (prefix `DQ`)

| File | doc_id | Purpose |
|------|--------|---------|
| `09-data-quality/README.md` | DQ-00 | Module scope: DQ framework, dimensions, rule catalog, pipelines, scorecards, remediation; requirement index |
| `09-data-quality/01-dq-framework.md` | DQ-01 | Dimensions, critical data elements, thresholds, derivation from ontology/SHACL |
| `09-data-quality/02-rule-catalog.md` | DQ-02 | Rule id scheme `DQR-<DIMENSION>-<DOMAIN>-<NNN>`, rule families incl. cross-system reconciliation (`DQ-RULE-*`) |
| `09-data-quality/03-dq-pipelines.md` | DQ-03 | Stream-plane evaluation, quarantine, backtests, MTTD |
| `09-data-quality/04-scorecards-and-slas.md` | DQ-04 | Composite DQ index, scorecards per group/facility, DQ SLAs |
| `09-data-quality/05-remediation-workflows.md` | DQ-05 | Remediation tickets, `agent.remediator` policy, steward exception handling |
| `09-data-quality/rules/rule.schema.json` | — | JSON Schema for DQ rule packs |
| `09-data-quality/rules/g1-party.yaml` | — | DQ rule pack: G1 Party & Identity |
| `09-data-quality/rules/g2-clinical.yaml` | — | DQ rule pack: G2 Clinical Care |
| `09-data-quality/rules/g3-finance.yaml` | — | DQ rule pack: G3 Revenue Cycle & Finance |
| `09-data-quality/rules/g4-supply.yaml` | — | DQ rule pack: G4 Supply Chain & Operations |
| `09-data-quality/rules/g5-workforce.yaml` | — | DQ rule pack: G5 Workforce |
| `09-data-quality/rules/cross-system-reconciliation.yaml` | — | DQ rule pack: cross-system reconciliation (charge↔claim↔remit, PO↔receipt↔invoice) |

## 10 — Agentic Pipeline (prefix `AGT`)

| File | doc_id | Purpose |
|------|--------|---------|
| `10-agentic-pipeline/README.md` *(planned)* | AGT-00 | Module scope: agent architecture, roster, autonomy levels, evaluation; requirement index |
| `10-agentic-pipeline/01-agent-architecture.md` | AGT-01 | Runtime, models per environment, planning/acting loop, safety boundaries |
| `10-agentic-pipeline/02-agent-catalog.md` *(planned)* | AGT-02 | Per-agent specifications for the roster in ARC-01 §5 |
| `10-agentic-pipeline/03-orchestration-and-workflows.md` | AGT-03 | Change Request schema contract, workflows, gates, HITL steps (`AGT-ORCH-*`) |
| `10-agentic-pipeline/04-tools-and-mcp.md` | AGT-04 | MCP tool servers (registry, terminology, Git, DQ engine, reasoner), allow-lists |
| `10-agentic-pipeline/05-guardrails-hitl-evals.md` | AGT-05 | Guardrails, HITL approval gates, evaluator rubrics, adversarial seeds, κ measurement, autonomy promotion/demotion, privacy guardian veto |
| `10-agentic-pipeline/06-memory-and-state.md` | AGT-06 | Agent memory and state: accepted/rejected proposals, rationale capture, retention |
| `10-agentic-pipeline/agents/agent.schema.json` | — | JSON Schema for agent manifests |
| `10-agentic-pipeline/agents/change-request.schema.json` | — | JSON Schema for typed change requests |
| `10-agentic-pipeline/agents/orchestrator.yaml` | — | Manifest: `agent.orchestrator` |
| `10-agentic-pipeline/agents/inventory.yaml` | — | Manifest: `agent.inventory` |
| `10-agentic-pipeline/agents/vocabulary_curator.yaml` | — | Manifest: `agent.vocabulary_curator` |
| `10-agentic-pipeline/agents/taxonomist.yaml` | — | Manifest: `agent.taxonomist` |
| `10-agentic-pipeline/agents/thesaurus_editor.yaml` | — | Manifest: `agent.thesaurus_editor` |
| `10-agentic-pipeline/agents/ontologist.yaml` | — | Manifest: `agent.ontologist` |
| `10-agentic-pipeline/agents/kg_builder.yaml` | — | Manifest: `agent.kg_builder` |
| `10-agentic-pipeline/agents/source_mapper.yaml` | — | Manifest: `agent.source_mapper` |
| `10-agentic-pipeline/agents/terminology_steward.yaml` | — | Manifest: `agent.terminology_steward` |
| `10-agentic-pipeline/agents/schema_architect.yaml` | — | Manifest: `agent.schema_architect` |
| `10-agentic-pipeline/agents/dq_author.yaml` | — | Manifest: `agent.dq_author` |
| `10-agentic-pipeline/agents/dq_monitor.yaml` | — | Manifest: `agent.dq_monitor` |
| `10-agentic-pipeline/agents/remediator.yaml` | — | Manifest: `agent.remediator` |
| `10-agentic-pipeline/agents/interop_engineer.yaml` | — | Manifest: `agent.interop_engineer` |
| `10-agentic-pipeline/agents/privacy_guardian.yaml` | — | Manifest: `agent.privacy_guardian` |
| `10-agentic-pipeline/agents/evaluator.yaml` | — | Manifest: `agent.evaluator` |
| `10-agentic-pipeline/agents/release_manager.yaml` | — | Manifest: `agent.release_manager` |

## 11 — Governance, Security & Compliance (prefix `GOV`)

| File | doc_id | Purpose |
|------|--------|---------|
| `11-governance-security-compliance/README.md` | GOV-00 | Module scope: HIPAA/PHI, access control, lineage/provenance/audit, stewardship; requirement index |
| `11-governance-security-compliance/01-hipaa-phi-and-privacy.md` | GOV-01 | PHI classification and policy contract, minimum necessary, de-identification, 42 CFR Part 2, BAAs (`GOV-PHI-*`) |
| `11-governance-security-compliance/02-access-control.md` | GOV-02 | Topic ACLs, schema grants, graph policies, role model, non-prod masking |
| `11-governance-security-compliance/03-lineage-provenance-audit.md` | GOV-03 | OpenLineage + PROV-O, audit event schema, immutability, completeness checks |
| `11-governance-security-compliance/04-master-data-and-stewardship.md` | GOV-04 | Master data ownership, steward roles per group, RACI, approval authorities, autonomy board |

## 12 — Platform & Operations (prefix `OPS`)

| File | doc_id | Purpose |
|------|--------|---------|
| `12-platform-and-operations/README.md` | OPS-00 | Module scope: deployment topology, environments, CI/CD, observability, DR/retention; requirement index |
| `12-platform-and-operations/01-deployment-topology.md` | OPS-01 | Clusters, networks, identity, secrets, vendor-swappable interfaces |
| `12-platform-and-operations/02-environments-cicd.md` | OPS-02 | `dev/test/uat/prod`, IaC, artefact validation, signed releases |
| `12-platform-and-operations/03-observability-slos.md` | OPS-03 | Metrics, logs, traces, SLO tables (`OPS-SLO-*`), alerting |
| `12-platform-and-operations/04-dr-and-retention.md` | OPS-04 | RPO/RTO, backup, replay, retention per layer, DR drills |

## 13 — Roadmap & Acceptance (prefix `RMP`)

| File | doc_id | Purpose |
|------|--------|---------|
| `13-roadmap-and-acceptance/README.md` | RMP-00 | Module scope, delivery principles (thin vertical slice first), phase overview and Gantt |
| `13-roadmap-and-acceptance/01-phased-roadmap.md` | RMP-01 | Phases 0–4: scope by module, deliverables, entry/exit criteria, dependencies, team shape, durations, agent autonomy graduation (`RMP-PHn-*`) |
| `13-roadmap-and-acceptance/02-acceptance-criteria.md` | RMP-02 | Acceptance per module tied to G-01..G-06 and NFRs, test approach, agent evaluation method, go-live checklist (`RMP-ACC-*`) |
| `13-roadmap-and-acceptance/03-risks-and-open-questions.md` | RMP-03 | Risk register with likelihood/impact/mitigation/owner, open questions with proposed defaults, decision log (`RMP-RISK-*`) |

## 99 — Appendices (prefix `APX`)

| File | doc_id | Purpose |
|------|--------|---------|
| `99-appendices/glossary.md` | APX-01 | Glossary across ontology, healthcare, Oracle Health/Fusion, data architecture, interoperability and agent terms |
| `99-appendices/code-systems-reference.md` | APX-02 | Code systems, identifier systems, HL7 tables, X12/NUBC lists, ISO, local `urn:healix:*` systems with URIs, licensing, cadence and groups |
| `99-appendices/references.md` | APX-03 | Bibliography: Ontology Pipeline, W3C specs, ISO 25964, HL7/FHIR/X12, warehousing, record linkage, temporal databases, streaming, regulation |
| `99-appendices/document-index.md` | APX-04 | This index |

## Summary

| Module | Prose documents (present / planned) | Machine-readable artefacts present |
|--------|-------------------------------------|------------------------------------|
| Root | 4 / 0 | — |
| 02 | 8 / 0 | 3 Turtle skeletons |
| 03 | 5 / 0 | — |
| 04 | 4 / 0 | — |
| 05 | 7 / 0 | `ddl/00_schemas.sql` |
| 06 | 6 / 0 | 7 profile/terminology files |
| 07 | 6 / 0 | `topics.yaml`, `topics.schema.json` |
| 08 | 5 / 0 | `temporal-vocabulary.ttl`, `examples.trig` |
| 09 | 6 / 0 | 7 rule-pack files |
| 10 | 5 / 2 | 2 schemas, 17 agent manifests |
| 11 | 5 / 0 | — |
| 12 | 5 / 0 | — |
| 13 | 4 / 0 | — |
| 99 | 4 / 0 | — |
| **Total** | **74 / 2** | — |
