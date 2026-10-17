---
title: Architecture Overview
module: 01
doc_id: ARC-01
version: 0.1.0
status: draft
owners: [Enterprise Architecture, Data Platform]
depends_on: [VIS-00]
last_updated: 2026-10-17
---

# Architecture Overview

This document is the **single source of truth** for cross-module contracts: layers, data flow, technology baseline,
subject-area groups, the agent roster, and the interfaces each module must honour. Module documents refine but never
contradict it.

## 1. Reference architecture (layers)

```mermaid
flowchart TB
  subgraph L0[Layer 0 · Source Systems]
    CER[/Cerner Millennium/]:::ext
    OFC[/Oracle Fusion ERP · HCM · SCM/]:::ext
    PER[/Peripheral: LIS · RIS/PACS · Pharmacy · RCM · HIE · Portal · Devices · Payers/]:::ext
  end
  subgraph L1[Layer 1 · Ingestion & Streaming  (module 07)]
    CDC[Kafka Connect · Debezium CDC · MLLP/HL7 v2 listeners · FHIR subscriptions · BICC extracts]
    RAW[(hc.raw.*)]
    STD[(hc.std.*)]
    CAN[(hc.canonical.* FHIR R4)]
    SR[(Schema Registry)]
  end
  subgraph L2[Layer 2 · Semantics  (module 02 · 06)]
    TS[Terminology Service · FHIR CodeSystem/ValueSet/ConceptMap]
    ONT[(Ontology Registry · SKOS/OWL/SHACL)]
  end
  subgraph L3[Layer 3 · Quality  (module 09)]
    DQE[DQ Engine · rules derived from ontology + SHACL]
    DQT[(hc.dq.*)]
  end
  subgraph L4[Layer 4 · Knowledge & Warehouse  (module 05 · 08)]
    TKG[(Temporal Knowledge Graph · RDF-star · bitemporal)]
    EDW[(EDW · G1–G9 subject areas)]
  end
  subgraph L5[Layer 5 · Agentic Control Plane  (module 10)]
    ORCH{{Orchestrator}}
    AG{{Specialist agents}}
    EVAL{{Evaluator / Critic}}
    HITL[Human approval gates]
  end
  subgraph L6[Layer 6 · Governance & Operations  (module 11 · 12)]
    GOV[Policy · PHI · Access · Audit · Lineage (PROV-O / OpenLineage)]
    OBS[Observability · SLOs]
  end
  CER & OFC & PER --> CDC --> RAW --> STD --> CAN
  SR -.-> RAW & STD & CAN
  TS --> CAN
  ONT --> TS
  CAN --> DQE --> DQT
  CAN --> TKG
  CAN --> EDW
  DQT --> TKG & EDW
  ORCH --> AG --> ONT & TS & DQE & EDW & TKG
  AG --> EVAL --> HITL --> ONT
  GOV -.-> L1 & L2 & L3 & L4 & L5
  OBS -.-> L1 & L3 & L4 & L5
  classDef ext fill:#eee,stroke:#999;
```

## 2. End-to-end data flow

1. **Capture.** Source changes arrive as (a) HL7 v2 messages over MLLP from Cerner and peripherals, (b) CDC change
   events from Cerner Millennium's Oracle database replica and from Oracle Fusion via BI Cloud Connector (BICC) /
   REST, (c) FHIR R4 resources via Oracle Health Millennium FHIR APIs and partner FHIR endpoints, (d) X12 EDI
   transactions and C-CDA documents from clearinghouses and HIEs, (e) files (CSV/Parquet) for low-frequency sources.
   Every capture lands unchanged in `hc.raw.*` with source metadata headers.
2. **Standardise.** Parsers produce typed, schema-registered records in `hc.std.*` (HL7 v2 parsed to segment model,
   CDC rows typed, X12 parsed). Structural validation happens here.
3. **Canonicalise.** Mapping functions (declared in module 06, generated/maintained by agents in module 10) convert
   `std` records to **FHIR R4 resources** published to `hc.canonical.<domain>.<resource>.fhir.v1`, with codes
   normalised through the Terminology Service to the enterprise value sets. Non-clinical Oracle Fusion entities use
   FHIR where a resource fits (`Organization`, `Practitioner`, `Location`, `SupplyDelivery`, `Invoice`, `ChargeItem`,
   `Account`) and a governed **FHIR-style extension model** (`HEALIX-ERP` profiles) where it does not (GL journals,
   purchase orders, positions).
4. **Measure quality.** The DQ engine evaluates ontology- and SHACL-derived rules on every canonical record and
   publishes results to `hc.dq.*`; failing records are routed to quarantine topics with a remediation ticket.
5. **Project.** Two sinks consume canonical + DQ streams: the **temporal knowledge graph** (RDF-star, bitemporal) and
   the **EDW** (subject-area schemas). Both carry the same business keys, ontology IRIs and provenance so results
   reconcile.
6. **Govern and learn.** Agents observe schema drift, new codes, failed mappings and DQ anomalies, then propose
   changes to vocabulary, ontology, mappings, DQ rules and EDW DDL through evaluated, versioned change requests.

## 3. Technology baseline (defaults; vendor-swappable behind interfaces)

| Concern | Default | Interface that hides it |
|---------|---------|-------------------------|
| Event backbone | Apache Kafka 3.x (KRaft) + Confluent-compatible Schema Registry (Avro for `raw/std`, FHIR JSON with JSON Schema for `canonical`) | Topic catalog (`07/topics.yaml`) |
| Connectors | Kafka Connect, Debezium (Oracle LogMiner for Millennium replica), HAPI HL7 v2 MLLP source, custom BICC poller, FHIR Subscription bridge | Connector manifests |
| Stream processing | Apache Flink (stateful joins, event-time windows); Kafka Streams for stateless maps | Processor contracts |
| Terminology service | HL7 FHIR Terminology Server (e.g., Ontoserver/Snowstorm-backed), `$lookup/$validate-code/$translate/$expand` | FHIR TS API |
| Ontology tooling | SKOS + OWL 2 DL + SHACL; reasoner (HermiT/ELK); versioned in Git; served by a triple store | Ontology Registry API |
| Knowledge graph | RDF 1.2 / RDF-star triple store with SPARQL 1.2 and named graphs (e.g., GraphDB, Stardog, Amazon Neptune, Virtuoso) | SPARQL endpoint + ingestion API |
| EDW | ANSI SQL; reference DDL in Teradata dialect with portable variant; columnar MPP (Teradata Vantage, Snowflake, Databricks SQL, BigQuery) | DDL + dbt-style models |
| Agents | LLM-driven agents using tool calls over MCP; default model family configurable per environment (recommended: latest Claude models, e.g. `claude-fable-5-1` for orchestration/critique and `claude-sonnet-5-5` / `claude-haiku-5-5` for high-volume mapping and classification) | Agent manifests (`10/agents/*.yaml`) |
| Workflow engine | Durable orchestration (Temporal.io or Apache Airflow for batch, Flink for streaming) | Workflow definitions |
| Lineage | OpenLineage events + PROV-O in graph | Lineage API |
| Secrets/Identity | OIDC, mTLS between services, KMS-managed keys | — |

## 4. Subject-area groups (authoritative)

Derived by harmonising the IBM UDMH and Teradata HCDM subject areas (module 04) with FHIR resource families. Each group
owns EDW schemas (`edw_gN_*`), KG named-graph prefixes and Kafka domain names.

| Code | Group | Kafka domain | Core entities (illustrative) | Primary sources |
|------|-------|--------------|------------------------------|-----------------|
| **G1** | Party & Identity | `party` | Person, Patient, Practitioner, PractitionerRole, Organization, Payer, Employee, Vendor/Supplier, Location, Related Person, Identifiers (MRN, NPI, EIN, Supplier#) | Cerner, Oracle HCM, Oracle SCM, Payers |
| **G2** | Clinical Care | `clinical` | Encounter, Episode of Care, Condition, Procedure, Observation (vitals/labs), Diagnostic Report, Medication Request/Dispense/Administration, Allergy/Intolerance, Immunization, Care Plan, Clinical Document, Service Request/Order, Specimen, Imaging Study (metadata), Device Use | Cerner, LIS, RIS/PACS, Pharmacy, Device gateways |
| **G3** | Revenue Cycle & Finance | `finance` | Account, Charge Item, Claim, Claim Response, Remittance, Payment Reconciliation, Coverage, Eligibility, Prior Authorization, Invoice, GL Journal, Cost Center, Budget, Fixed Asset, Project | Cerner (charges), RCM, Clearinghouse, Oracle Fusion ERP |
| **G4** | Supply Chain & Operations | `supply` | Item/Product, Inventory Balance, Purchase Requisition/Order, Receipt, Supplier Contract, Supply Delivery, Asset/Equipment, Bed, Room, Schedule/Slot/Appointment, Transport | Oracle Fusion SCM, Cerner scheduling, Biomed |
| **G5** | Workforce | `workforce` | Worker, Position, Assignment, Credential/Licence, Competency, Shift/Timecard, Payroll aggregates, Learning | Oracle Fusion HCM, Credentialing, Timekeeping |
| **G6** | Population Health & Quality | `pophealth` | Measure, Measure Report, Registry Membership, Risk Score, Care Gap, Social Determinants (SDOH) Observation, Attribution, Program Enrolment | Derived; HIE; Payer files |
| **G7** | Research & Analytics | `research` | Research Study, Research Subject, Cohort, Consent (research), Feature Store entries, Model Registry links | Derived; Research systems |
| **G8** | Reference & Terminology | `reference` | Code System, Value Set, Concept Map, Calendar, Geography, Currency, Unit (UCUM), Organisation Hierarchy, Service Line | Terminology Service, Oracle Fusion reference data |
| **G9** | Metadata, Quality & Governance | `governance` | Source Registry, Data Element, Lineage Activity, DQ Rule, DQ Result, Issue/Remediation, Consent & Directive, Access Policy, Audit Event | Platform-generated |

**Cross-group conformed dimensions** (module 05): Patient, Practitioner, Organization, Location, Date/Time, Payer,
Item, Cost Center, Code (terminology), Source System. These are owned by G1/G8 and reused by all fact tables.

## 5. Agent roster (authoritative; detail in module 10)

| Agent id | Role | Owns artefacts in | Autonomy level (initial) |
|----------|------|-------------------|--------------------------|
| `agent.orchestrator` | Plans and schedules pipeline runs; routes change requests; enforces gates | 10 | L3 (acts, escalates on policy) |
| `agent.inventory` | Discovers sources, schemas, elements; maintains Source Registry & data dictionary (Stage 0) | 02, 03, G9 | L3 |
| `agent.vocabulary_curator` | Harvests terms, proposes preferred/alternate labels, definitions, code bindings (Stage 1) | 02 | L2 (proposes; auto-approve low-risk) |
| `agent.taxonomist` | Builds/validates hierarchies, polyhierarchy checks, facet analysis (Stage 2) | 02 | L2 |
| `agent.thesaurus_editor` | Proposes associative/equivalence relations, scope notes (Stage 3) | 02 | L2 |
| `agent.ontologist` | Promotes to OWL classes/properties, axioms, SHACL shapes; runs reasoner (Stage 4) | 02 | L1 (always human-approved) |
| `agent.kg_builder` | Generates R2RML/RML + FHIR→RDF mappings, loads bitemporal graph (Stage 5) | 08 | L3 |
| `agent.source_mapper` | Maps source fields → `std` → FHIR; maintains HL7 v2 and CDC mapping specs | 03, 06 | L2 |
| `agent.terminology_steward` | Maintains value sets, concept maps, handles new/unmapped codes | 06, G8 | L2 |
| `agent.schema_architect` | Derives EDW logical/physical models from ontology + UDMH/HCDM patterns; writes DDL migrations | 04, 05 | L1 |
| `agent.dq_author` | Derives DQ rules from ontology constraints, profiles data, proposes thresholds | 09 | L2 |
| `agent.dq_monitor` | Runs DQ pipelines, detects anomalies and drift, opens issues | 09 | L3 |
| `agent.remediator` | Proposes/apply fixes for quarantined records within policy (e.g., code re-mapping) | 09 | L2 |
| `agent.interop_engineer` | Generates/validates FHIR profiles, HL7 v2 conformance profiles, partner conformance tests | 06 | L2 |
| `agent.privacy_guardian` | Classifies PHI, enforces minimisation, blocks disallowed flows, reviews every change for PHI impact | 11 | L4 (veto authority) |
| `agent.evaluator` | Independent critic: scores every proposal against rubrics, tests, reasoner, SHACL, regression suites | 10 | — |
| `agent.release_manager` | Versions and publishes ontology/schema/topic releases; generates changelogs | 02, 05, 07 | L2 |

Autonomy levels: **L1** propose only · **L2** propose, auto-apply when evaluator score ≥ threshold and risk class = low ·
**L3** act within policy, report · **L4** act and veto other agents.

## 6. Cross-module contracts

| Contract | Producer | Consumers | Spec |
|----------|----------|-----------|------|
| Source Registry & Data Element Catalog | 03 (via `agent.inventory`) | 02, 05, 09, 11 | `03-source-systems/README.md` §Registry |
| Ontology Registry API (SKOS/OWL/SHACL, versioned) | 02 | 05, 06, 08, 09, 10 | `02-ontology-pipeline/07-governance-and-versioning.md` |
| Terminology Service (FHIR TS) | 06 | 02, 07, 08, 09 | `06-interoperability/04-terminology-services.md` |
| Topic Catalog | 07 | all | `07-streaming-platform/topics.yaml` |
| Canonical FHIR profiles | 06 | 07, 08, 05, 09 | `06-interoperability/profiles/` |
| Bitemporal graph model | 08 | 05, 09, 11 | `08-temporal-knowledge-graph/01-temporal-model.md` |
| EDW logical model & DDL | 05 | 09, 12 | `05-edw-schema/ddl/` |
| DQ rule pack | 09 | 07, 08, 10 | `09-data-quality/rules/` |
| Change Request schema | 10 | 02, 05, 06, 09, 11 | `10-agentic-pipeline/03-orchestration-and-workflows.md` |
| PHI classification & policy | 11 | all | `11-governance-security-compliance/01-hipaa-phi-and-privacy.md` |

## 7. Key architectural decisions (ADRs)

| ADR | Decision | Rationale | Alternatives rejected |
|-----|----------|-----------|-----------------------|
| ADR-01 | FHIR R4 (US Core 6.1) is the canonical interchange model; HL7 v2 is accepted inbound and emitted outbound for legacy consumers | Broadest ecosystem support, Cerner/Oracle Health native FHIR APIs | FHIR R5 only (ecosystem immaturity), proprietary canonical |
| ADR-02 | RDF/OWL/SKOS/SHACL for semantics; RDF-star for temporal annotation | W3C standard stack, native to the Ontology Pipeline stages, interoperable with FHIR RDF | Pure property graph (weaker standards, no reasoning) |
| ADR-03 | Bitemporal (valid + transaction time) on every assertion in KG and every row in EDW history tables | Clinical/financial audit and as-of reporting | Snapshot-only, SCD2-only |
| ADR-04 | Kafka with layered topics `raw → std → canonical → dq → kg/edw` | Replayability, decoupling, per-layer contracts | Point-to-point ETL, ESB |
| ADR-05 | IBM UDMH + Teradata HCDM used as *blueprints* harmonised into our own licensed-safe logical model | Leverage industry models without lock-in or IP leakage | Adopt one model verbatim |
| ADR-06 | Agents operate under typed change requests with independent evaluator and PHI guardian veto | Safe autonomy; auditable | Fully manual curation; unconstrained agents |
| ADR-07 | Dual projection (KG + EDW) from the same canonical stream, reconciled nightly | Serve graph and SQL consumers without ETL between them | EDW-first then graph, or graph-only |

## 8. Non-functional requirements (platform-level)

| ID | Requirement |
|----|-------------|
| **ARC-NFR-001** (P0) | p95 latency source event → `canonical` topic ≤ 5 s for HL7 v2/CDC; ≤ 15 min for BICC batch extracts. |
| **ARC-NFR-002** (P0) | Sustained throughput ≥ 5,000 canonical resources/s with headroom ×3. |
| **ARC-NFR-003** (P0) | All PHI encrypted in transit (TLS 1.2+) and at rest (AES-256); topic-level ACLs; field-level masking for non-production. |
| **ARC-NFR-004** (P0) | Exactly-once semantics from `std` to `canonical` and idempotent sinks into KG and EDW. |
| **ARC-NFR-005** (P1) | Full replay of any topic layer from retention window (≥ 30 days `raw`, infinite compacted `canonical` snapshots). |
| **ARC-NFR-006** (P0) | Every artefact (ontology, DDL, mapping, rule) versioned in Git with signed releases. |
| **ARC-NFR-007** (P1) | RPO ≤ 15 min, RTO ≤ 4 h for the whole platform. |
