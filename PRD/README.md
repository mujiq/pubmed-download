# Enterprise Healthcare Ontology & Data Platform — Product Requirements (PRD)

**Program codename:** HEALIX (Healthcare Enterprise Autonomous Linked Information eXchange)
**Status:** Draft v0.1 · **Owner:** Data Platform & Clinical Informatics · **Last updated:** 2026-10-17

This folder is the complete, modular specification for an **autonomous, agentic pipeline** that:

1. Builds and maintains the **full enterprise healthcare ontology** by walking the *Ontology Pipeline* stages
   (metadata inventory → controlled vocabulary → taxonomy → thesaurus → ontology → knowledge graph), as framed in
   Jessica Talisman's *Ontology Pipeline: A framework for building knowledge infrastructures*.
2. Runs **data-quality (DQ) pipelines** spanning **Cerner Millennium (Oracle Health)**, **Oracle Fusion Cloud**
   (ERP, HCM, SCM) and all **peripheral systems** (LIS, RIS/PACS, pharmacy, revenue cycle, scheduling, HIE, patient
   engagement, devices, payer/claims).
3. Uses the **IBM Unified Data Model for Healthcare (UDMH)** and the **Teradata Healthcare Data Model (HCDM)** as
   reference blueprints to derive an **enterprise data warehouse (EDW) schema** organised into **grouped subject areas**.
4. Persists everything as a **temporal (bitemporal) knowledge graph** so every fact carries valid time, transaction
   time and provenance.
5. Moves data over **Apache Kafka**, standardising every clinical, financial and operational event as **HL7 v2** and
   **HL7 FHIR R4** messages so the platform is interoperable with any downstream or partner system.

## How to read this PRD

Each numbered folder is a **module** that can be built, reviewed and versioned independently. Every module has a
`README.md` (scope, interfaces, requirement IDs) and one or more numbered specification documents. Machine-readable
artefacts (YAML topic catalogs, SQL DDL, SKOS/OWL skeletons, SHACL shapes, agent manifests, DQ rule packs) live beside
the prose that specifies them so they can be linted and loaded by the agents.

Start with `00-vision-and-scope.md`, then `01-architecture-overview.md`, then `CONVENTIONS.md`. After that, read
modules in any order; dependencies are declared in each module's front matter.

## Document map

| # | Module | Purpose | Key artefacts |
|---|--------|---------|---------------|
| 00 | [Vision & Scope](00-vision-and-scope.md) | Problem, goals, non-goals, personas, success metrics | — |
| 01 | [Architecture Overview](01-architecture-overview.md) | Layered reference architecture, data flow, technology baseline, cross-module contracts | Subject-area groups, agent roster, topic naming |
| — | [Conventions](CONVENTIONS.md) | Requirement IDs, naming, front matter, versioning, definitions of done | — |
| 02 | [Ontology Pipeline](02-ontology-pipeline/README.md) | The six-stage pipeline from metadata to knowledge graph, with governance | SKOS/OWL/SHACL skeletons |
| 03 | [Source Systems](03-source-systems/README.md) | Cerner Millennium, Oracle Fusion, peripheral systems; extraction and mapping contracts | Source inventories, mapping matrices |
| 04 | [Reference Data Models](04-reference-data-models/README.md) | How IBM UDMH and Teradata HCDM are used as blueprints and harmonised | Harmonisation matrix |
| 05 | [EDW Schema](05-edw-schema/README.md) | Conceptual/logical/physical models grouped into subject areas; conformed dimensions; history | `ddl/*.sql` |
| 06 | [Interoperability](06-interoperability/README.md) | FHIR R4 canonical model, HL7 v2 → FHIR, CDA/X12, terminology services, validation | `profiles/*.json` |
| 07 | [Streaming Platform](07-streaming-platform/README.md) | Kafka topology, topic catalog, schema registry, CDC connectors, stream processing | `topics.yaml` |
| 08 | [Temporal Knowledge Graph](08-temporal-knowledge-graph/README.md) | Bitemporal graph model, store, entity resolution, ingestion from Kafka | Graph schema, SPARQL/Cypher patterns |
| 09 | [Data Quality](09-data-quality/README.md) | DQ framework, rule catalog, pipelines, scorecards, remediation | `rules/*.yaml` |
| 10 | [Agentic Pipeline](10-agentic-pipeline/README.md) | Agent architecture, catalog, orchestration, tools/MCP, guardrails, memory | `agents/*.yaml` |
| 14 | [Agent Memory, Caching & Self-Improvement](14-memory-and-self-improvement/README.md) | Mem0 + Graphiti memory layers, semantic/prompt caching, recursive self-healing and self-improving loops | `memory-schema.yaml`, `healing-playbooks/` |
| 11 | [Governance, Security & Compliance](11-governance-security-compliance/README.md) | HIPAA/PHI, access control, lineage/provenance/audit, stewardship | Policy matrices |
| 12 | [Platform & Operations](12-platform-and-operations/README.md) | Deployment topology, environments, CI/CD, observability, DR/retention | SLO tables |
| 15 | [Adversarial Review & Production Readiness](15-adversarial-review/README.md) | Red-team findings, Fortune-100 legacy/tech-debt outliers, remediation log | Findings register |
| 13 | [Roadmap & Acceptance](13-roadmap-and-acceptance/README.md) | Phased delivery, acceptance criteria, risks and open questions | — |
| 99 | [Appendices](99-appendices/) | Glossary, code-system reference, bibliography | — |

## Modularity principles

- **One module, one bounded context.** A module exposes contracts (schemas, topics, APIs, requirement IDs) and hides
  internals. Another module may depend on a contract, never on an internal.
- **Specs are executable where possible.** Topic catalogs, DDL, SHACL shapes, DQ rules and agent manifests are the
  source of truth; prose explains intent and acceptance.
- **Everything is temporal and provenance-aware.** No module may store a fact without valid time, transaction time and
  a PROV-O activity reference (see module 08 and 11).
- **Standards first.** SKOS, OWL 2, SHACL, PROV-O, ISO 25964, FHIR R4 (US Core), HL7 v2.x, X12 005010, SNOMED CT,
  LOINC, RxNorm, ICD-10-CM/PCS, CPT/HCPCS, NDC, CVX, UCUM, NUCC, NPI. Proprietary models (IBM UDMH, Teradata HCDM) are
  used under licence as design blueprints and are never reproduced in this repository.
- **Agents propose, governance disposes.** Every autonomous change to vocabulary, ontology, schema, mapping or DQ rule
  goes through a typed change request with evaluation gates; high-impact classes require human approval (module 10, 11).

## Licensing and attribution note

The IBM Unified Data Model for Healthcare and the Teradata Healthcare Data Model are commercial, licensed products.
This PRD describes *how* the platform consumes them (subject-area alignment, entity patterns, naming harmonisation) and
assumes the adopting organisation holds valid licences. No proprietary entity definitions, attribute lists or diagrams
from either product are copied here. SNOMED CT use requires an affiliate licence in non-member countries; CPT requires
an AMA licence.
