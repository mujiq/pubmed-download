---
title: Glossary
module: 99-appendices
doc_id: APX-01
version: 0.1.0
status: draft
owners: [Enterprise Architecture, Clinical Informatics, Data Governance]
depends_on: [VIS-00, ARC-01]
last_updated: 2026-10-17
---

# Glossary

Terms are grouped by domain. Where a term has a HEALIX-specific meaning that narrows the general one, the HEALIX usage
is given. Standards bodies and canonical URIs for code systems are in `code-systems-reference.md`; sources are in
`references.md`.

## 1. Knowledge organisation and ontology

| Term | Definition | HEALIX usage |
|------|------------|--------------|
| Ontology Pipeline | The staged framework (metadata inventory → controlled vocabulary → taxonomy → thesaurus → ontology → knowledge graph) described in Jessica Talisman's *Ontology Pipeline*, in which each stage is a governed product that later stages build on | Module 02 maps the six stages to Stages 0–5 and assigns an agent to each |
| Metadata inventory | A registry of sources, interfaces, data elements, their definitions, formats, stewards and usage, assembled before any vocabulary work | Stage 0; the Source Registry and Data Element Catalog in G9, maintained by `agent.inventory` |
| Controlled vocabulary | An agreed, finite set of terms with preferred labels, alternate labels, definitions and scope notes used to describe a domain consistently | Stage 1; one SKOS `ConceptScheme` per vocabulary |
| Taxonomy | A controlled vocabulary arranged in a hierarchy of broader/narrower concepts | Stage 2; SKOS `broader`/`narrower` with polyhierarchy constrained by facets |
| Thesaurus | A taxonomy enriched with associative (related) and equivalence (use/use-for) relationships and richer notes, per ISO 25964 | Stage 3; SKOS `related`, `exactMatch`/`closeMatch` and mapping relations to external terminologies |
| Ontology | A formal, machine-interpretable specification of classes, properties and axioms in a domain, enabling reasoning | Stage 4; OWL 2 DL with SHACL shapes for closed-world validation |
| Knowledge graph (KG) | A graph of entities and relationships instantiated against an ontology, typically RDF, queryable and reasonable | Stage 5; the temporal knowledge graph in module 08 |
| Competency question | A natural-language question the ontology/knowledge graph must be able to answer; used to scope and test the ontology | Registered per group; converted to SPARQL tests (RMP-ACC-022) |
| Concept | A unit of thought identified by an IRI, independent of the labels used to express it | `skos:Concept` |
| Concept scheme | An aggregation of concepts forming a vocabulary, taxonomy or thesaurus | `skos:ConceptScheme`, IRI `<base>scheme/<slug>` |
| Preferred label | The one label per language chosen to represent a concept | `skos:prefLabel`; unique per language per scheme |
| Alternate label | A synonym, acronym or variant label for a concept | `skos:altLabel` |
| Hidden label | A misspelling or deprecated label kept for retrieval | `skos:hiddenLabel` |
| Scope note | A note clarifying the intended meaning and boundaries of a concept | `skos:scopeNote`; required by scheme policy for ambiguous concepts |
| Definition | A statement of the meaning of a concept | `skos:definition`; required for ≥ 98 % of concepts at Stage 1 |
| Broader / narrower | Hierarchical relations between concepts in SKOS | `skos:broader`/`skos:narrower`; must be acyclic |
| Polyhierarchy | A concept having more than one broader concept | Allowed only when each parent belongs to a distinct facet |
| Facet | A fundamental category used to partition a domain (e.g., *by body system*, *by setting*) so that hierarchies stay mutually exclusive | Facet tags justify polyhierarchy; drive EDW dimension design |
| Monohierarchy | A hierarchy in which every concept has exactly one broader concept | Default for taxonomies feeding dimension hierarchies |
| Associative relation | A non-hierarchical, non-equivalence relation between concepts (ISO 25964 *RT*) | `skos:related`; symmetric, never also hierarchical |
| Equivalence relation | A relation stating that two terms denote the same concept (USE / UF) or that two concepts in different schemes are equivalent | `skos:exactMatch`, `skos:closeMatch` for cross-scheme mapping |
| Mapping relations (SKOS) | `exactMatch`, `closeMatch`, `broadMatch`, `narrowMatch`, `relatedMatch` between concepts in different schemes | Used for enterprise ↔ SNOMED/LOINC/local code mappings |
| SKOS | Simple Knowledge Organization System, the W3C vocabulary for representing thesauri, taxonomies and controlled vocabularies in RDF | Stages 1–3 artefacts |
| SKOS-XL | SKOS extension for labels as first-class resources (with their own metadata) | Used where label provenance per source is needed |
| OWL 2 | Web Ontology Language, the W3C language for authoring ontologies with formal semantics | Stage 4; OWL 2 DL profile |
| OWL DL / OWL EL / OWL RL | OWL 2 profiles trading expressivity for decidability/tractability | DL for authoring; EL for scalable classification (ELK); RL for rule-based materialisation in the KG |
| Reasoner | Software that infers entailments and checks consistency of an ontology | HermiT, ELK in CI (RMP-ACC-020) |
| Consistency / satisfiability | An ontology is consistent if it has a model; a class is satisfiable if it can have instances | Release gate: consistent, 0 unsatisfiable classes |
| Axiom | A logical statement in an ontology (subclass, disjointness, property domain/range, cardinality) | Proposed by `agent.ontologist`, human-approved |
| Object property / data property | OWL properties relating individuals to individuals / to literals | Promoted from stable thesaurus relations |
| Domain / range | The class of subjects / objects an OWL property may relate | Required for every object property at Stage 4 |
| Disjointness | An axiom stating two classes share no instances | Always human-approved (RMP-RISK-006) |
| Open-world assumption (OWA) | Absence of a statement does not imply its falsity (OWL semantics) | Why SHACL is used for validation |
| Closed-world validation | Checking data against constraints assuming what is not stated is false | SHACL |
| SHACL | Shapes Constraint Language, the W3C language for validating RDF graphs against shapes | Stage 4 shapes; pre-publish gate on every named graph; source of DQ rules |
| Shape | A SHACL node or property shape describing constraints on a focus node | One shape per canonical profile element (RMP-ACC-024) |
| ShEx | Shape Expressions, an alternative RDF validation language used by FHIR RDF | Used to validate FHIR RDF before conversion to HEALIX shapes |
| RDF | Resource Description Framework, the W3C graph data model of subject–predicate–object triples | KG data model |
| RDF-star (RDF 1.2) | Extension of RDF allowing statements about statements (quoted triples), used for annotating assertions | Carries valid time, source event id and confidence per assertion |
| Named graph | A set of triples identified by an IRI, enabling partitioning and metadata per graph | `<base>graph/<source>/<date>/<ulid>` carries transaction time and PROV activity |
| IRI | Internationalized Resource Identifier; the global identifier for RDF resources | Base `https://ontology.healix.example.org/` |
| SPARQL | The W3C query language for RDF | SPARQL 1.2 endpoint on the KG |
| Triple store | A database optimised for storing and querying RDF | Module 08 store |
| Turtle / JSON-LD | RDF serialisation formats | Turtle in Git; JSON-LD for APIs |
| Materialisation | Pre-computing inferred triples and storing them | Used for OWL RL inferences in the KG |
| Dublin Core (DCTERMS) | Metadata vocabulary for descriptive metadata (`dcterms:modified`, `dcterms:creator`) | Versioning metadata on schemes and releases |
| ISO 25964 | International standard for thesauri and interoperability with other vocabularies (Part 1: thesauri; Part 2: interoperability) | Conformance target for Stage 3 |
| ANSI/NISO Z39.19 | US guideline for construction of monolingual controlled vocabularies | Reference for Stage 1–2 editorial rules |
| Upper ontology | A domain-independent ontology of general concepts (e.g., BFO) used to align domain ontologies | Optional alignment considered at Stage 4 |
| Version IRI | An IRI identifying a specific release of an ontology | `owl:versionIRI` per release |
| Deprecation | Marking a concept or axiom as no longer recommended while retaining it | `owl:deprecated`, `skos:hiddenLabel`; never delete |

## 2. Healthcare and clinical informatics

| Term | Definition | HEALIX usage |
|------|------------|--------------|
| ADT | HL7 v2 Admit/Discharge/Transfer message family (A01 admit, A02 transfer, A03 discharge, A04 register, A08 update, A11 cancel admit, A40 merge) | Thin-slice feed → Patient, Encounter |
| ORU | HL7 v2 Observation Result (unsolicited) message, e.g., lab, vitals | → Observation, DiagnosticReport |
| ORM / OMG | HL7 v2 Order message (ORM in 2.3–2.5; OMG general order in 2.5+) | → ServiceRequest |
| SIU | HL7 v2 Scheduling Information Unsolicited message | → Appointment, Schedule, Slot (G4) |
| MDM | HL7 v2 Medical Document Management message | → DocumentReference metadata |
| MLLP | Minimal Lower Layer Protocol, the TCP framing used for HL7 v2 | Ingress transport for Cerner and peripherals |
| ACK / NACK | HL7 v2 acknowledgement; AA accept, AE error, AR reject | Error-rate KPI (< 0.1 %) |
| Segment / field / component | HL7 v2 message structure units (e.g., PID-3 patient identifier list) | `hc.std.*` segment model |
| Z-segment | Locally defined HL7 v2 segment (name starts with Z) | Retained in `std`; promoted only when needed (OQ-17) |
| MRN | Medical Record Number; the patient identifier assigned by a facility or health system | Facility-scoped identifier; crosswalked to enterprise person ID |
| FIN | Financial Identification Number; Cerner's encounter-level account identifier | Encounter business key component |
| Encounter | An interaction between a patient and healthcare provider(s) for the purpose of providing services or assessing health status | FHIR `Encounter`; G2 |
| Episode of care | An association between a patient and an organisation/provider over a period for a condition or purpose, spanning encounters | FHIR `EpisodeOfCare`; G2 |
| Patient class | HL7 v2 PV1-2 category (inpatient, outpatient, emergency, observation) | Mapped to enterprise encounter-class value set |
| Admit source / discharge disposition | Where the patient came from / where they went at discharge (UB-04 point of origin, patient discharge status) | Stage 1 vocabularies in the thin slice |
| NPI | National Provider Identifier, the 10-digit US identifier for providers | `http://hl7.org/fhir/sid/us-npi`; Practitioner/Organization |
| NUCC taxonomy | Provider specialty classification codes maintained by the National Uniform Claim Committee | PractitionerRole specialty |
| DRG / MS-DRG | Diagnosis Related Group; inpatient case-mix classification used for payment (Medicare Severity DRG) | G3 claim attribute |
| CPT | Current Procedural Terminology, AMA-owned procedure codes used for billing | Licensed; G3 charges and claims |
| HCPCS | Healthcare Common Procedure Coding System; Level I = CPT, Level II = CMS codes for supplies, drugs, services | G3/G4 |
| ICD-10-CM | International Classification of Diseases, 10th revision, Clinical Modification (US diagnosis codes) | Condition, claim diagnosis |
| ICD-10-PCS | ICD-10 Procedure Coding System (US inpatient procedures) | Procedure, claim |
| LOINC | Logical Observation Identifiers Names and Codes; universal codes for lab and clinical observations | Observation.code; LOINC parts drive lab taxonomies |
| SNOMED CT | Systematized Nomenclature of Medicine — Clinical Terms; comprehensive clinical terminology with its own ontology | Primary clinical concept binding; US edition |
| RxNorm | NLM normalised names and identifiers for clinical drugs | Medication coding |
| NDC | National Drug Code; FDA product identifier for drugs | Dispense/package-level coding |
| CVX / MVX | CDC vaccine administered codes / manufacturer codes | Immunization |
| UCUM | Unified Code for Units of Measure | Observation.valueQuantity.unit |
| UNII | Unique Ingredient Identifier (FDA) | Allergy substance coding |
| EMPI | Enterprise Master Patient Index; a system/process that links patient identities across sources | Entity resolution in module 08; false-merge risk RMP-RISK-008 |
| Entity resolution / record linkage | Determining which records refer to the same real-world entity | Fellegi–Sunter probabilistic scoring with human review |
| HIE | Health Information Exchange; organisation/network enabling health data sharing across organisations | C-CDA inbound source; outbound partner |
| C-CDA | HL7 Consolidated Clinical Document Architecture; XML clinical document standard (CCD, discharge summary) | Inbound from HIE (Phase 3) |
| CDS | Clinical Decision Support | Non-goal for first release (feed later) |
| DICOM | Digital Imaging and Communications in Medicine; imaging standard | Metadata only (ImagingStudy); pixels out of scope |
| LIS / RIS / PACS / VNA | Laboratory, Radiology Information Systems; Picture Archiving and Communication System; Vendor Neutral Archive | Peripheral sources |
| RCM | Revenue Cycle Management; processes from registration through claims to payment | G3 |
| Clearinghouse | Intermediary that validates and routes X12 claims and remittances between providers and payers | X12 source |
| X12 837 | HIPAA claim transaction (837P professional, 837I institutional, 837D dental) | → FHIR Claim |
| X12 835 | Health care claim payment/remittance advice | → ClaimResponse, PaymentReconciliation |
| X12 270 / 271 | Eligibility inquiry / response | → CoverageEligibilityRequest/Response |
| X12 278 | Health care services review (prior authorization) request/response | → Claim (use = preauthorization) / ClaimResponse per Da Vinci PAS |
| X12 834 | Benefit enrolment and maintenance | Payer attribution (G6) |
| CARC / RARC | Claim Adjustment Reason Codes / Remittance Advice Remark Codes used in 835 | Adjudication coding |
| UB-04 / NUBC | Uniform institutional claim form / National Uniform Billing Committee (revenue codes, type of bill, point of origin, discharge status) | Encounter and claim vocabularies |
| Payer / coverage / plan | Organisation paying for care / a patient's insurance coverage / the product | G1 Payer, G3 Coverage |
| Prior authorization | Payer approval required before a service | G3; X12 278, Da Vinci PAS |
| Charge / charge item | A billable service or item recorded against an encounter account | Cerner charges → FHIR ChargeItem |
| Care gap | A recommended service a patient has not received per a quality measure | G6 |
| eCQM / HEDIS | Electronic Clinical Quality Measures (CMS) / Healthcare Effectiveness Data and Information Set (NCQA) | G6 measure sources |
| SDOH | Social Determinants of Health | G6 Observations per Gravity Project |
| Attribution | Assigning a patient to a provider/organisation accountable for their care | G6 |
| Cohort | A set of patients defined by criteria for research or quality | G7 |
| Consent / directive | A patient's recorded permission or restriction on use/disclosure | FHIR Consent; module 11 |
| 42 CFR Part 2 | US regulation protecting substance use disorder treatment records | Segmentation requirement (OQ-15) |
| HIPAA | Health Insurance Portability and Accountability Act; Privacy, Security and Breach Notification Rules (45 CFR 160/164) | Module 11 baseline |
| PHI | Protected Health Information under HIPAA | Classification on every data element |
| Minimum necessary | HIPAA principle limiting PHI use/disclosure to the minimum needed | Applied to agent tool calls |
| Safe Harbor / Expert determination | HIPAA de-identification methods (remove 18 identifier categories / statistical determination) | Research projections (RMP-ACC-113) |
| BAA | Business Associate Agreement; HIPAA contract with vendors handling PHI | Required for LLM providers (OQ-04) |
| USCDI | United States Core Data for Interoperability; ONC data-class standard | Scope reference for US Core profiles |
| TEFCA / QHIN | Trusted Exchange Framework and Common Agreement / Qualified Health Information Network | Phase 4 readiness assessment |

## 3. Oracle Health (Cerner) and Oracle Fusion

| Term | Definition | HEALIX usage |
|------|------------|--------------|
| Cerner Millennium | The EHR platform now marketed by Oracle Health; Oracle-database-backed clinical system | Primary clinical source |
| Oracle Health | Oracle's healthcare business unit following the Cerner acquisition | Vendor for Millennium and FHIR APIs |
| CCL | Cerner Command Language; Cerner's proprietary SQL-like query/report language for Millennium | Used by SMEs to validate extractions; not a platform dependency |
| CODE_VALUE / CODE_SET | Millennium's reference tables: `CODE_SET` numbers a domain (e.g., encounter type); `CODE_VALUE` rows are the coded values with display and definition | Local code system `urn:healix:cerner:code-set:<n>` |
| PERSON / ENCOUNTER / CLINICAL_EVENT / ORDERS | Core Millennium tables for people, encounters, clinical results and orders | CDC candidates (OQ-03) |
| Millennium FHIR APIs | Oracle Health's FHIR R4 APIs for Millennium | Capture path (c) in ARC-01 §2 |
| HealtheIntent | Oracle Health's population-health data platform | Potential G6 source/consumer; not a dependency |
| Oracle Fusion Cloud | Oracle's SaaS suite: ERP (financials, procurement, projects), HCM (workforce), SCM (supply chain) | G3, G4, G5 sources |
| BICC | BI Cloud Connector; Oracle Fusion's bulk data extraction service writing to object storage | Batch capture path; ≤ 15 min bound |
| PVO | Public View Object; the extractable data objects exposed through BICC | Coverage risk RMP-RISK-004 |
| OTBI | Oracle Transactional Business Intelligence; Fusion's embedded reporting layer | Used for reconciliation of extract counts, not as a source |
| FND_LOOKUP | Fusion's lookup type/value tables for reference codes | Local code system `urn:healix:fusion:lookup:<type>` |
| Descriptive / Key flexfields (DFF/KFF) | Fusion's configurable extension fields and key structures (e.g., chart of accounts) | Chart-of-accounts taxonomy; `urn:healix:fusion:flex:<segment>` |
| Chart of accounts (COA) | The structured set of GL account segments (company, cost centre, natural account...) | G3 taxonomy |
| Ledger / GL journal | General-ledger book / journal entries posted to it | HEALIX-ERP profile |
| Supplier / item / PO / receipt | SCM master and transaction objects | G4 |
| Worker / assignment / position / job | HCM core objects for workforce structure | G5 |

## 4. Data architecture, warehousing and streaming

| Term | Definition | HEALIX usage |
|------|------------|--------------|
| Bitemporal | Data model recording both valid time and transaction time for every fact | ADR-03; every KG assertion and `hist_*` row |
| Valid time | The period during which a fact is true in the real world | `valid_from`, `valid_to` |
| Transaction time | The period during which a fact is recorded in the database as current | `recorded_at`, `superseded_at` |
| As-of query | A query returning the state of data as of a given valid time, transaction time or both | As-of test suite (RMP-ACC-081) |
| Supersession | Replacing a fact with a corrected one while retaining the old version with its transaction-time end | Never delete; merges and corrections are supersessions |
| Late-arriving data | Events that arrive after later events were processed | Handled via bitemporal correction and Flink allowed lateness |
| SCD2 | Slowly Changing Dimension type 2; dimension history by row versioning with effective dates | Analyst-friendly view beside `hist_*` (OQ-07) |
| Conformed dimension | A dimension shared with identical keys and attributes across multiple fact tables/subject areas | Patient, Practitioner, Organization, Location, Date/Time, Payer, Item, Cost Center, Code, Source System |
| Fact / dimension / bridge table | Kimball structures: measurable events / descriptive context / many-to-many resolution | `fact_*`, `dim_*`, `bridge_*` |
| Surrogate key / business key / natural key | System-generated key / enterprise stable identifier / source identifier | `*_sk`, `*_bk`, `*_id` |
| Subject area / subject-area group | A business-aligned partition of the warehouse model | G1–G9 |
| EDW | Enterprise Data Warehouse | Module 05 |
| UDMH | IBM Unified Data Model for Healthcare; licensed industry data model used as a blueprint | Module 04; never reproduced |
| HCDM | Teradata Healthcare Data Model; licensed industry data model used as a blueprint | Module 04; never reproduced |
| Harmonisation matrix | Mapping of G1–G9 entities to UDMH/HCDM patterns, FHIR resource families and HEALIX-native justifications | Module 04 artefact |
| CDC | Change Data Capture; capturing database changes from transaction logs | Debezium on Millennium replica; BICC incremental for Fusion |
| Debezium | Open-source CDC platform on Kafka Connect | Oracle LogMiner connector |
| Kafka | Apache Kafka distributed event streaming platform | Event backbone (ADR-04) |
| KRaft | Kafka's built-in consensus mode replacing ZooKeeper | Cluster mode |
| Topic / partition / offset | Kafka log name / ordered shard / position | Topic catalog `topics.yaml` |
| Compacted topic | Topic retaining the latest record per key | `canonical` snapshots (ARC-NFR-005) |
| Kafka Connect | Framework for source/sink connectors | Ingestion and EDW sink |
| Schema Registry | Service storing and enforcing schemas (Avro/JSON Schema/Protobuf) per subject with compatibility rules | `BACKWARD_TRANSITIVE` on canonical; `FULL` on kg/edw |
| Avro / JSON Schema | Serialisation schema languages | Avro for `raw/std`; JSON Schema for FHIR JSON canonical |
| Exactly-once semantics (EOS) | Processing guarantee that each record affects state exactly once despite failures | Kafka transactions + idempotent sinks (ARC-NFR-004) |
| Idempotent sink | A sink where re-applying the same record produces no additional change | KG and EDW loaders |
| Event time / processing time / source time | When the event occurred / when it was processed / when the source recorded it | Headers `hc-event-time`, `hc-source-time` |
| Watermark | A Flink/streaming marker of event-time progress used to trigger windows | Allowed lateness tuned per topic |
| Apache Flink | Stateful stream-processing framework | Canonicalisation, joins, derived topics |
| Kafka Streams | Lightweight Kafka-native stream library | Stateless maps |
| Replay | Re-consuming a topic from an earlier offset to rebuild downstream state | Drill requirement (RMP-ACC-073) |
| Quarantine topic | Topic receiving records that failed validation or DQ for remediation | `hc.dq.quarantine.*` |
| Dead-letter topic | Topic receiving records that could not be processed at all | Alongside quarantine |
| Lineage | Record of how data moved and was transformed from source to target | OpenLineage events + PROV-O |
| OpenLineage | Open standard for collecting lineage metadata from pipelines (jobs, runs, datasets) | Lineage API; `prov_activity_iri` resolves to a run |
| PROV-O | W3C provenance ontology (Entity, Activity, Agent; `wasGeneratedBy`, `wasDerivedFrom`) | Every assertion references a `prov:Activity` |
| Data element | An atomic unit of data with a definition, format and steward | Data Element Catalog (G9) |
| Critical data element (CDE) | A data element whose quality materially affects a business or clinical outcome | DQ coverage denominator (G-03) |
| Data quality dimension | Category of quality: completeness, validity, accuracy, consistency, timeliness, uniqueness | DQ rule id `DQR-<DIMENSION>-...` |
| DQ rule / threshold / scorecard | Executable check / acceptable breach band / aggregated view of results | Module 09 |
| Backtest | Running a new rule against historical data to estimate breach rate before activation | RMP-ACC-093 |
| Reconciliation | Comparing counts/values between two systems or projections | KG↔EDW nightly; charge↔claim↔remit |
| Three-way match | Matching purchase order, receipt and invoice | G4 reconciliation DQ |
| Golden dataset | Curated, labelled test data with expected outputs | Test assets (RMP-02 §3.1) |
| Synthetic data | Artificially generated data with no real individuals | Default non-prod data (OQ-13) |
| Shadow mode | Running a new pipeline in production without consumers acting on its output | Pre-go-live step |
| Infrastructure as code (IaC) | Declarative definition of environments | RMP-ACC-120 |
| SLO / SLI / error budget | Service level objective / indicator / allowed unreliability | Module 12 |
| RPO / RTO | Recovery Point / Time Objective | ≤ 15 min / ≤ 4 h |
| R2RML / RML | W3C RDB-to-RDF mapping language / its generalisation to other sources | `agent.kg_builder` mappings |
| FHIR RDF | The RDF representation of FHIR resources defined by HL7 | Bridge from canonical FHIR to the KG |
| OMOP CDM | OHDSI Observational Medical Outcomes Partnership Common Data Model | Reference for G7 research projections (not canonical) |

## 5. Interoperability standards

| Term | Definition | HEALIX usage |
|------|------------|--------------|
| HL7 | Health Level Seven International, standards body for v2, CDA, FHIR | — |
| HL7 v2.x | Message-based interoperability standard (2.3–2.8 in scope) | Inbound/outbound legacy format |
| FHIR R4 / R4B / R5 | Fast Healthcare Interoperability Resources, releases 4 (normative base), 4B, 5 | R4 canonical (ADR-01); R5 profile track in Phase 4 |
| Resource | FHIR's unit of exchange (Patient, Encounter, Observation...) | `hc.canonical.<domain>.<resource>.fhir.v1` |
| Profile | A FHIR StructureDefinition constraining a resource for a use case | HEALIX profiles on US Core |
| Extension | FHIR mechanism for adding elements not in the base resource | HEALIX-ERP profiles |
| Implementation Guide (IG) | A published set of profiles, value sets and conformance expectations | US Core, CARIN BB, Da Vinci, HEALIX internal IG |
| US Core | HL7 US realm base FHIR profiles aligned to USCDI | Version 6.1 is canonical base |
| CodeSystem / ValueSet / ConceptMap | FHIR terminology resources: a code system, a selection of codes, a mapping between codes | Terminology Service; G8 |
| Terminology service | FHIR server offering `$lookup`, `$validate-code`, `$expand`, `$translate` | Module 06 |
| Binding strength | Required / extensible / preferred / example — how strictly an element must use a value set | HEALIX profiles tighten to required/extensible |
| Bulk Data (`$export`) | FHIR asynchronous bulk export (NDJSON) | Outbound partner interface |
| Subscription | FHIR mechanism for event notification | Capture path and outbound |
| Bundle | FHIR container of resources (transaction, message, collection) | Synthetic FHIR bundles for tests |
| V2-to-FHIR IG | HL7 implementation guide mapping HL7 v2 segments/messages to FHIR | Basis for ADT/ORU/ORM/SIU maps |
| CARIN Blue Button (CARIN BB) | FHIR IG for payer claims data to consumers (ExplanationOfBenefit) | Guidance for X12 → FHIR claim mappings |
| Da Vinci | HL7 project of payer–provider FHIR IGs (PAS, PDex, CRD, DTR, HRex) | Prior authorization and payer data exchange mapping conventions |
| Gravity Project | HL7 accelerator defining SDOH terminology and FHIR IG | G6 SDOH |
| X12 005010 | Version of ASC X12 EDI standards mandated under HIPAA for administrative transactions | Inbound EDI |
| Companion guide | Payer-specific implementation rules on top of X12 standards | Parser configuration |
| Conformance profile (HL7 v2) | Formal constraint specification for an HL7 v2 message | Golden corpus validation |
| Validator | Tool checking a resource/message against profiles | FHIR validator in stream (≥ 99.5 %) |

## 6. Agents and autonomy

| Term | Definition | HEALIX usage |
|------|------------|--------------|
| Agent | An LLM-driven software component that plans, calls tools and produces proposals or actions under policy | Roster in ARC-01 §5 |
| Orchestrator | The agent that plans and schedules runs, routes change requests and enforces gates | `agent.orchestrator` |
| Specialist agent | An agent owning one pipeline stage or artefact family | e.g., `agent.taxonomist` |
| Evaluator / critic | An independent agent scoring proposals against rubrics, tests, reasoner and shapes | `agent.evaluator`; κ ≥ 0.85 vs humans |
| Autonomy level (L1–L4) | L1 propose only; L2 propose and auto-apply low-risk above threshold; L3 act within policy and report; L4 act and veto | Graduation table RMP-01 §9 |
| Change request (CR) | Typed, versioned proposal to change vocabulary, ontology, mapping, DQ rule or DDL, with evidence, evaluation and approval record | Module 10 schema |
| Change class | The category of a change request (label edit, synonym, hierarchy move, axiom, mapping, DQ threshold, DDL...) determining risk tier and approval route | Eligibility for auto-approval per class |
| Risk class | Low / medium / high impact classification of a change | Low-risk eligible for L2 auto-apply |
| Evaluation gate | A point where a proposal must pass evaluator checks before progressing | ADR-06 |
| HITL | Human-in-the-loop; a human approval or review step in an automated workflow | Approval gates; review queues |
| Auto-approval | Applying a change without human approval after evaluator score ≥ threshold and risk class = low | G-06 target ≥ 80 % |
| Rollback | Reverting an applied change to the previous version | Rate ≤ 2 % |
| Rubric | A structured scoring guide used by evaluator and human adjudicators | Per change class |
| Adjudication | Blinded human judgement of a proposal used to measure evaluator agreement | ≥ 300 per cycle |
| Cohen's kappa (κ) | Chance-corrected inter-rater agreement statistic | Threshold 0.85 |
| Adversarial seed | A deliberately bad proposal injected to test the evaluator | 100 % catch for PHI/reasoner cases |
| Hallucination | Model output presenting fabricated facts (e.g., a non-existent SNOMED code) as true | Mitigated by evidence citation, reasoner, terminology validation |
| Tool / tool call | A function an agent can invoke (query registry, validate code, run reasoner) | Exposed via MCP |
| MCP | Model Context Protocol; open protocol for connecting LLM applications to tools and data sources | Agent tool servers for registry, terminology, Git, DQ engine |
| Guardrail | A policy or check constraining agent behaviour (allow-lists, PHI screening, budgets) | Module 10 and `agent.privacy_guardian` |
| Veto | Authority to block another agent's action | L4, `agent.privacy_guardian` |
| Agent memory | Persisted record of accepted/rejected proposals and rationale used to improve future proposals | Module 10 |
| Manifest | Declarative description of an agent: role, tools, model, autonomy level, policies | `10-agentic-pipeline/agents/*.yaml` |
| Durable workflow | Long-running workflow with persisted state, retries and human steps | Temporal.io (OQ-14) |
| Release train | Scheduled cadence of ontology/schema/topic releases | Monthly minor, quarterly major |
| Autonomy board | Governance body deciding agent promotions/demotions | OQ-11 |
