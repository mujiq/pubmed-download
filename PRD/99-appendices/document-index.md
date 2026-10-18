---
title: Document Index
module: 99-appendices
doc_id: APX-04
version: 0.2.0
status: draft
owners: [Programme Management Office, Enterprise Architecture]
depends_on: [VIS-00, ARC-01]
last_updated: 2026-10-18
---

# Document Index

Complete index of PRD files by module with `doc_id` and the file's title. Generated from the repository as of the
`last_updated` date; every listed file is present. Requirement prefixes follow `CONVENTIONS.md` §2. Machine-readable
artefacts (YAML, SQL, JSON, Turtle, TriG, HTML, CSS, JS) are listed under their module and carry no `doc_id`.

## Root

| File | doc_id | Title |
|------|--------|-------|
| `README.md` | — | Enterprise Healthcare Ontology & Data Platform — Product Requirements (PRD) |
| `CONVENTIONS.md` | — | Conventions |
| `00-vision-and-scope.md` | VIS-00 | Vision and Scope |
| `01-architecture-overview.md` | ARC-01 | Architecture Overview |

## 02 — Ontology Pipeline (prefix `ONT`)

| File | doc_id | Title / purpose |
|------|--------|-----------------|
| `02-ontology-pipeline/01-metadata-and-inventory.md` | ONT-01 | Stage 0 — Metadata and Inventory |
| `02-ontology-pipeline/02-controlled-vocabularies.md` | ONT-02 | Stage 1 — Controlled Vocabularies |
| `02-ontology-pipeline/03-taxonomy.md` | ONT-03 | Stage 2 — Taxonomy |
| `02-ontology-pipeline/04-thesaurus.md` | ONT-04 | Stage 3 — Thesaurus |
| `02-ontology-pipeline/05-ontology.md` | ONT-05 | Stage 4 — Ontology (OWL 2 DL and SHACL) |
| `02-ontology-pipeline/06-knowledge-graph.md` | ONT-06 | Stage 5 — Knowledge Graph Instantiation |
| `02-ontology-pipeline/07-governance-and-versioning.md` | ONT-07 | Ontology Governance, Registry API and Versioning |
| `02-ontology-pipeline/README.md` | ONT-00 | Ontology Pipeline — Module Overview |
| `02-ontology-pipeline/skeleton.owl.ttl` | — | machine-readable artefact |
| `02-ontology-pipeline/skeleton.shacl.ttl` | — | machine-readable artefact |
| `02-ontology-pipeline/skeleton.skos.ttl` | — | machine-readable artefact |

## 03 — Source Systems (prefix `SRC`)

| File | doc_id | Title / purpose |
|------|--------|-----------------|
| `03-source-systems/01-cerner-millennium.md` | SRC-01 | Cerner Millennium (Oracle Health) — Clinical System of Record |
| `03-source-systems/02-oracle-fusion.md` | SRC-02 | Oracle Fusion Cloud — ERP, HCM and SCM |
| `03-source-systems/03-peripheral-systems.md` | SRC-03 | Peripheral Systems — LIS, RIS/PACS, Pharmacy, Revenue Cycle, Payers, HIE, Engagement, Devices, Credentialing, Supply Peripherals, EMPI |
| `03-source-systems/04-source-to-canonical-mapping.md` | SRC-04 | Source-to-Canonical Mapping Specification Contract |
| `03-source-systems/README.md` | SRC-00 | Source Systems — Module Overview and Source Registry Contract |

## 04 — Reference Data Models (prefix `RDM`)

| File | doc_id | Title / purpose |
|------|--------|-----------------|
| `04-reference-data-models/01-ibm-udmh.md` | RDM-01 | IBM Unified Data Model for Healthcare (UDMH) as a Blueprint |
| `04-reference-data-models/02-teradata-hcdm.md` | RDM-02 | Teradata Healthcare Data Model (HCDM) as a Blueprint |
| `04-reference-data-models/03-model-harmonization.md` | RDM-03 | Model Harmonisation — UDMH · HCDM · FHIR R4 · Sources → HEALIX Logical Model |
| `04-reference-data-models/README.md` | RDM-00 | Reference Data Models — Module Overview |

## 05 — EDW Schema (prefix `EDW`)

| File | doc_id | Title / purpose |
|------|--------|-----------------|
| `05-edw-schema/01-subject-area-groups.md` | EDW-01 | Subject-Area Groups G1–G9 — Schemas, Facts, Dimensions and Grain |
| `05-edw-schema/02-conceptual-model.md` | EDW-02 | Conceptual Model — Entities, Relationships and Patterns per Group |
| `05-edw-schema/03-logical-model.md` | EDW-03 | Logical Model — Design Rules and Sample Tables |
| `05-edw-schema/04-physical-model-and-ddl.md` | EDW-04 | Physical Model, DDL Generation and Data Dictionary |
| `05-edw-schema/05-conformed-dimensions.md` | EDW-05 | Conformed Dimensions |
| `05-edw-schema/06-history-and-bitemporality.md` | EDW-06 | History and Bitemporality in the EDW |
| `05-edw-schema/README.md` | EDW-00 | EDW Schema — Module Overview |
| `05-edw-schema/ddl/00_schemas.sql` | — | machine-readable artefact |
| `05-edw-schema/ddl/g1_party.sql` | — | machine-readable artefact |
| `05-edw-schema/ddl/g2_clinical.sql` | — | machine-readable artefact |
| `05-edw-schema/ddl/g3_finance.sql` | — | machine-readable artefact |
| `05-edw-schema/ddl/g4_supply.sql` | — | machine-readable artefact |
| `05-edw-schema/ddl/g8_reference.sql` | — | machine-readable artefact |
| `05-edw-schema/ddl/g9_governance.sql` | — | machine-readable artefact |

## 06 — Interoperability (prefix `IOP`)

| File | doc_id | Title / purpose |
|------|--------|-----------------|
| `06-interoperability/01-canonical-model-fhir.md` | IOP-01 | Canonical Model — FHIR R4 |
| `06-interoperability/02-hl7v2-to-fhir.md` | IOP-02 | HL7 v2 Handling and HL7 v2 → FHIR Mapping |
| `06-interoperability/03-cda-x12-other.md` | IOP-03 | C-CDA, IHE XDS/XCA, X12 005010, DICOM, IEEE 11073 and Bulk Data |
| `06-interoperability/04-terminology-services.md` | IOP-04 | Terminology Services |
| `06-interoperability/05-validation-and-profiles.md` | IOP-05 | Validation Pipeline, Conformance and Profiles |
| `06-interoperability/README.md` | IOP-00 | Interoperability — Module Overview |
| `06-interoperability/profiles/ConceptMap-cerner-encounter-type-to-healix.json` | — | machine-readable artefact |
| `06-interoperability/profiles/StructureDefinition-healix-encounter.json` | — | machine-readable artefact |
| `06-interoperability/profiles/StructureDefinition-healix-erp-gl-journal-line.json` | — | machine-readable artefact |
| `06-interoperability/profiles/StructureDefinition-healix-observation-lab.json` | — | machine-readable artefact |
| `06-interoperability/profiles/StructureDefinition-healix-patient.json` | — | machine-readable artefact |
| `06-interoperability/profiles/ValueSet-healix-encounter-class.json` | — | machine-readable artefact |
| `06-interoperability/profiles/identifier-systems.yaml` | — | machine-readable artefact |

## 07 — Streaming Platform (prefix `STR`)

| File | doc_id | Title / purpose |
|------|--------|-----------------|
| `07-streaming-platform/01-kafka-topology.md` | STR-01 | Kafka Topology, Security, Retention and Disaster Recovery |
| `07-streaming-platform/02-topic-catalog.md` | STR-02 | Topic Catalog |
| `07-streaming-platform/03-schema-registry.md` | STR-03 | Schema Registry, Formats, Envelope and Evolution |
| `07-streaming-platform/04-cdc-and-connectors.md` | STR-04 | CDC and Connectors |
| `07-streaming-platform/05-stream-processing.md` | STR-05 | Stream Processing |
| `07-streaming-platform/README.md` | STR-00 | Streaming Platform (Module 07) — Overview |
| `07-streaming-platform/topics.schema.json` | — | machine-readable artefact |
| `07-streaming-platform/topics.yaml` | — | machine-readable artefact |

## 08 — Temporal Knowledge Graph (prefix `TKG`)

| File | doc_id | Title / purpose |
|------|--------|-----------------|
| `08-temporal-knowledge-graph/01-temporal-model.md` | TKG-01 | Bitemporal Model Contract |
| `08-temporal-knowledge-graph/02-graph-store-and-query.md` | TKG-02 | Graph Store and Query Layer |
| `08-temporal-knowledge-graph/03-entity-resolution.md` | TKG-03 | Entity Resolution (EMPI and Enterprise Master Entities) |
| `08-temporal-knowledge-graph/04-graph-ingestion-from-kafka.md` | TKG-04 | Graph Ingestion from Kafka |
| `08-temporal-knowledge-graph/README.md` | TKG-00 | Temporal Knowledge Graph — Module Overview |
| `08-temporal-knowledge-graph/examples.trig` | — | machine-readable artefact |
| `08-temporal-knowledge-graph/temporal-vocabulary.ttl` | — | machine-readable artefact |

## 09 — Data Quality (prefix `DQ`)

| File | doc_id | Title / purpose |
|------|--------|-----------------|
| `09-data-quality/01-dq-framework.md` | DQ-01 | Data Quality Framework |
| `09-data-quality/02-rule-catalog.md` | DQ-02 | DQ Rule DSL and Rule Catalogue |
| `09-data-quality/03-dq-pipelines.md` | DQ-03 | DQ Pipelines and Execution Planes |
| `09-data-quality/04-scorecards-and-slas.md` | DQ-04 | DQ Scorecards, Index and SLAs |
| `09-data-quality/05-remediation-workflows.md` | DQ-05 | DQ Remediation Workflows |
| `09-data-quality/README.md` | DQ-00 | Data Quality — Module Overview |
| `09-data-quality/rules/cross-system-reconciliation.yaml` | — | machine-readable artefact |
| `09-data-quality/rules/g1-party.yaml` | — | machine-readable artefact |
| `09-data-quality/rules/g2-clinical.yaml` | — | machine-readable artefact |
| `09-data-quality/rules/g3-finance.yaml` | — | machine-readable artefact |
| `09-data-quality/rules/g4-supply.yaml` | — | machine-readable artefact |
| `09-data-quality/rules/g5-workforce.yaml` | — | machine-readable artefact |
| `09-data-quality/rules/rule.schema.json` | — | machine-readable artefact |
| `09-data-quality/rules/temporal-and-provenance.yaml` | — | machine-readable artefact |

## 10 — Agentic Pipeline (prefix `AGT`)

| File | doc_id | Title / purpose |
|------|--------|-----------------|
| `10-agentic-pipeline/01-agent-architecture.md` | AGT-01 | Agent Architecture and Runtime |
| `10-agentic-pipeline/02-agent-catalog.md` | AGT-02 | Agent Catalog |
| `10-agentic-pipeline/03-orchestration-and-workflows.md` | AGT-03 | Orchestration, Change Requests and Workflows |
| `10-agentic-pipeline/04-tools-and-mcp.md` | AGT-04 | Tools and MCP Servers |
| `10-agentic-pipeline/05-guardrails-hitl-evals.md` | AGT-05 | Guardrails, Human-in-the-Loop and Evaluation |
| `10-agentic-pipeline/06-memory-and-state.md` | AGT-06 | Memory and State |
| `10-agentic-pipeline/README.md` | AGT-00 | Agentic Pipeline — Module Overview |
| `10-agentic-pipeline/agents/agent.schema.json` | — | machine-readable artefact |
| `10-agentic-pipeline/agents/change-request.schema.json` | — | machine-readable artefact |
| `10-agentic-pipeline/agents/dq_author.yaml` | — | machine-readable artefact |
| `10-agentic-pipeline/agents/dq_monitor.yaml` | — | machine-readable artefact |
| `10-agentic-pipeline/agents/evaluator.yaml` | — | machine-readable artefact |
| `10-agentic-pipeline/agents/improver.yaml` | — | machine-readable artefact |
| `10-agentic-pipeline/agents/interop_engineer.yaml` | — | machine-readable artefact |
| `10-agentic-pipeline/agents/inventory.yaml` | — | machine-readable artefact |
| `10-agentic-pipeline/agents/kg_builder.yaml` | — | machine-readable artefact |
| `10-agentic-pipeline/agents/memory_curator.yaml` | — | machine-readable artefact |
| `10-agentic-pipeline/agents/ontologist.yaml` | — | machine-readable artefact |
| `10-agentic-pipeline/agents/orchestrator.yaml` | — | machine-readable artefact |
| `10-agentic-pipeline/agents/privacy_guardian.yaml` | — | machine-readable artefact |
| `10-agentic-pipeline/agents/release_manager.yaml` | — | machine-readable artefact |
| `10-agentic-pipeline/agents/remediator.yaml` | — | machine-readable artefact |
| `10-agentic-pipeline/agents/schema_architect.yaml` | — | machine-readable artefact |
| `10-agentic-pipeline/agents/self_healer.yaml` | — | machine-readable artefact |
| `10-agentic-pipeline/agents/source_mapper.yaml` | — | machine-readable artefact |
| `10-agentic-pipeline/agents/taxonomist.yaml` | — | machine-readable artefact |
| `10-agentic-pipeline/agents/terminology_steward.yaml` | — | machine-readable artefact |
| `10-agentic-pipeline/agents/thesaurus_editor.yaml` | — | machine-readable artefact |
| `10-agentic-pipeline/agents/vocabulary_curator.yaml` | — | machine-readable artefact |

## 11 — Governance, Security & Compliance (prefix `GOV`)

| File | doc_id | Title / purpose |
|------|--------|-----------------|
| `11-governance-security-compliance/01-hipaa-phi-and-privacy.md` | GOV-01 | HIPAA, PHI Classification and Privacy |
| `11-governance-security-compliance/02-access-control.md` | GOV-02 | Access Control, Identity and Platform Security |
| `11-governance-security-compliance/03-lineage-provenance-audit.md` | GOV-03 | Lineage, Provenance and Audit |
| `11-governance-security-compliance/04-master-data-and-stewardship.md` | GOV-04 | Master Data Management and Stewardship |
| `11-governance-security-compliance/README.md` | GOV-00 | Governance, Security & Compliance — Module Overview |

## 12 — Platform & Operations (prefix `OPS`)

| File | doc_id | Title / purpose |
|------|--------|-----------------|
| `12-platform-and-operations/01-deployment-topology.md` | OPS-01 | Deployment Topology |
| `12-platform-and-operations/02-environments-cicd.md` | OPS-02 | Environments, GitOps and CI/CD |
| `12-platform-and-operations/03-observability-slos.md` | OPS-03 | Observability and Service Level Objectives |
| `12-platform-and-operations/04-dr-and-retention.md` | OPS-04 | Disaster Recovery and Data Retention |
| `12-platform-and-operations/README.md` | OPS-00 | Platform & Operations — Module Overview |

## 13 — Roadmap & Acceptance (prefix `RMP`)

| File | doc_id | Title / purpose |
|------|--------|-----------------|
| `13-roadmap-and-acceptance/01-phased-roadmap.md` | RMP-01 | Phased Roadmap |
| `13-roadmap-and-acceptance/02-acceptance-criteria.md` | RMP-02 | Acceptance Criteria |
| `13-roadmap-and-acceptance/03-risks-and-open-questions.md` | RMP-03 | Risks and Open Questions |
| `13-roadmap-and-acceptance/README.md` | RMP-00 | Roadmap & Acceptance — Module Overview |

## 14 — Memory & Self-Improvement (prefix `MEM`)

| File | doc_id | Title / purpose |
|------|--------|-----------------|
| `14-memory-and-self-improvement/01-memory-architecture.md` | MEM-01 | Memory Architecture |
| `14-memory-and-self-improvement/02-graphiti-temporal-memory.md` | MEM-02 | Graphiti Temporal Memory |
| `14-memory-and-self-improvement/03-caching-strategy.md` | MEM-03 | Caching Strategy |
| `14-memory-and-self-improvement/04-self-healing.md` | MEM-04 | Self-Healing — Fault Taxonomy, Healing Engine and Playbook Learning |
| `14-memory-and-self-improvement/05-self-improvement-loop.md` | MEM-05 | Self-Improvement Loop — Evaluation-Driven Optimisation, Canary Rollout and Policy Registry |
| `14-memory-and-self-improvement/06-recursive-learning-governance.md` | MEM-06 | Recursive Learning Governance — Bounds on Self-Modification, Recursion, Trust and Kill Switches |
| `14-memory-and-self-improvement/README.md` | MEM-00 | Agent Memory, Caching & Self-Improvement — Module Overview |
| `14-memory-and-self-improvement/healing-playbooks/README.md` | MEM-A3 | Healing Playbooks — Format Specification and Library Index |
| `14-memory-and-self-improvement/cache-policy.yaml` | — | machine-readable artefact |
| `14-memory-and-self-improvement/healing-playbooks/agent-budget-exhaustion.yaml` | — | machine-readable artefact |
| `14-memory-and-self-improvement/healing-playbooks/cdc-lag-millennium.yaml` | — | machine-readable artefact |
| `14-memory-and-self-improvement/healing-playbooks/connector-mllp-sequence-gap.yaml` | — | machine-readable artefact |
| `14-memory-and-self-improvement/healing-playbooks/dq-rule-storm.yaml` | — | machine-readable artefact |
| `14-memory-and-self-improvement/healing-playbooks/kg-load-failure.yaml` | — | machine-readable artefact |
| `14-memory-and-self-improvement/healing-playbooks/schema-drift-fusion-bicc.yaml` | — | machine-readable artefact |
| `14-memory-and-self-improvement/healing-playbooks/unmapped-code-surge.yaml` | — | machine-readable artefact |
| `14-memory-and-self-improvement/memory-schema.yaml` | — | machine-readable artefact |

## 15 — Adversarial Review (prefix `ADV`)

| File | doc_id | Title / purpose |
|------|--------|-----------------|
| `15-adversarial-review/01-findings-register.md` | ADV-REG | Findings Register |
| `15-adversarial-review/02-fortune100-outliers.md` | ADV-OUT | Fortune-100 Legacy, Tech-Debt, M&A and Scale Outliers |
| `15-adversarial-review/03-remediation-plan.md` | ADV-REM | Remediation Plan |
| `15-adversarial-review/README.md` | ADV-00 | Adversarial Review & Production Readiness — Module Overview |

## 16 — User Experience (prefix `UX`)

| File | doc_id | Title / purpose |
|------|--------|-----------------|
| `16-user-experience/01-personas-and-jobs.md` | UX-01 | Personas, Jobs-to-be-Done and Role Homes |
| `16-user-experience/02-information-architecture.md` | UX-02 | Information Architecture, Object Model & Page Template |
| `16-user-experience/03-ontology-workbench.md` | UX-03 | Ontology Workbench & Canonical Concept Page |
| `16-user-experience/04-change-request-review.md` | UX-04 | Change-Request Review — The 30-Second Decision |
| `16-user-experience/05-dq-and-remediation-console.md` | UX-05 | DQ & Remediation Console |
| `16-user-experience/06-graph-lineage-and-time.md` | UX-06 | Lineage Explorer & Temporal Knowledge-Graph Explorer |
| `16-user-experience/07-catalog-interop-and-ops.md` | UX-07 | Catalog, Interoperability & Operations |
| `16-user-experience/08-design-system-and-accessibility.md` | UX-08 | Design System & Accessibility |
| `16-user-experience/README.md` | UX-00 | User Experience & Role Workspaces — Module Overview |
| `16-user-experience/prototype/README.md` | — | HEALIX Console — clickable prototype (module 16, v0.1) |
| `16-user-experience/prototype/agent-ops.html` | — | machine-readable artefact |
| `16-user-experience/prototype/assets/app.css` | — | machine-readable artefact |
| `16-user-experience/prototype/assets/app.js` | — | machine-readable artefact |
| `16-user-experience/prototype/change-requests.html` | — | machine-readable artefact |
| `16-user-experience/prototype/concept.html` | — | machine-readable artefact |
| `16-user-experience/prototype/data/agents.json` | — | machine-readable artefact |
| `16-user-experience/prototype/data/catalog.json` | — | machine-readable artefact |
| `16-user-experience/prototype/data/change-requests.json` | — | machine-readable artefact |
| `16-user-experience/prototype/data/concepts.json` | — | machine-readable artefact |
| `16-user-experience/prototype/data/dq.json` | — | machine-readable artefact |
| `16-user-experience/prototype/data/incidents.json` | — | machine-readable artefact |
| `16-user-experience/prototype/data/kpis.json` | — | machine-readable artefact |
| `16-user-experience/prototype/data/lineage.json` | — | machine-readable artefact |
| `16-user-experience/prototype/data/schemes.json` | — | machine-readable artefact |
| `16-user-experience/prototype/data/topics.json` | — | machine-readable artefact |
| `16-user-experience/prototype/data-catalog.html` | — | machine-readable artefact |
| `16-user-experience/prototype/dq-console.html` | — | machine-readable artefact |
| `16-user-experience/prototype/executive.html` | — | machine-readable artefact |
| `16-user-experience/prototype/index.html` | — | machine-readable artefact |
| `16-user-experience/prototype/interop-monitor.html` | — | machine-readable artefact |
| `16-user-experience/prototype/lineage-explorer.html` | — | machine-readable artefact |
| `16-user-experience/prototype/ontology-workbench.html` | — | machine-readable artefact |
| `16-user-experience/prototype/settings-governance.html` | — | machine-readable artefact |

## 99 — Appendices (prefix `APX`)

| File | doc_id | Title / purpose |
|------|--------|-----------------|
| `99-appendices/code-systems-reference.md` | APX-02 | Code Systems and Value-Set Sources Reference |
| `99-appendices/document-index.md` | APX-04 | Document Index |
| `99-appendices/glossary.md` | APX-01 | Glossary |
| `99-appendices/references.md` | APX-03 | References and Bibliography |

## Summary

| Module | Prose docs | Machine-readable artefacts |
|--------|-----------|----------------------------|
| 02 | 8 | 3 |
| 03 | 5 | 0 |
| 04 | 4 | 0 |
| 05 | 7 | 7 |
| 06 | 6 | 7 |
| 07 | 6 | 2 |
| 08 | 5 | 2 |
| 09 | 6 | 8 |
| 10 | 7 | 22 |
| 11 | 5 | 0 |
| 12 | 5 | 0 |
| 13 | 4 | 0 |
| 14 | 8 | 9 |
| 15 | 4 | 0 |
| 16 | 10 | 23 |
| 99 | 4 | 0 |
| **Total** | **94** | **83** |

