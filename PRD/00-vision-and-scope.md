---
title: Vision and Scope
module: 00
doc_id: VIS-00
version: 0.1.0
status: draft
owners: [Chief Data Officer, Clinical Informatics, Enterprise Architecture]
depends_on: []
last_updated: 2026-10-17
---

# Vision and Scope

## 1. Problem statement

A health system running **Cerner Millennium** for clinical care and **Oracle Fusion Cloud** for finance, supply chain
and workforce, surrounded by dozens of peripheral systems, has no single, trusted, machine-readable definition of
*what its data means*. Terminologies drift between systems, the same patient or supplier exists under many
identifiers, data-quality problems are discovered downstream by analysts, and interoperability is a per-interface
engineering effort. Analytics and AI initiatives repeatedly rebuild semantics from scratch.

The knowledge-infrastructure gap is semantic, not just technical. The *Ontology Pipeline* framework argues that a
durable fix is to build meaning **progressively and in standards**: inventory the metadata, agree the vocabulary,
arrange it into taxonomies, enrich it into thesauri, formalise it into an ontology, and only then instantiate a
knowledge graph. Each stage is a product in itself with its own governance, and later stages inherit the rigour of
earlier ones rather than inventing it.

## 2. Vision

> A self-maintaining knowledge infrastructure in which autonomous agents continuously build, test and publish the
> enterprise healthcare ontology; where every event from every system is standardised to HL7 v2 / FHIR on Kafka;
> where data quality is measured against the ontology at the point of arrival; and where the enterprise data warehouse
> and the temporal knowledge graph are two faithful projections of the same governed semantics.

## 3. Goals

| ID | Goal | Measure of success (first release) |
|----|------|------------------------------------|
| G-01 | Publish an enterprise healthcare ontology covering all nine subject-area groups | ≥ 95 % of EDW attributes bound to an ontology property; OWL DL consistent |
| G-02 | Standardise all Cerner, Oracle Fusion and peripheral events to FHIR R4 on Kafka | 100 % of in-scope interfaces emit `canonical.*` topics; FHIR validator pass rate ≥ 99.5 % |
| G-03 | Run DQ pipelines at ingestion with ontology-derived rules | DQ coverage ≥ 90 % of critical data elements; mean time-to-detect < 15 min |
| G-04 | Deliver an EDW with grouped subject areas derived from UDMH/HCDM | All G1–G9 groups populated; conformed dimensions shared by ≥ 2 groups |
| G-05 | Make all facts bitemporal with provenance in a knowledge graph | 100 % of KG assertions carry valid/transaction time and PROV-O activity |
| G-06 | Run the pipeline autonomously with human approval only at defined gates | ≥ 80 % of vocabulary/mapping/DQ-rule changes auto-approved after evaluation; zero unreviewed PHI-affecting changes |

## 4. Non-goals (first release)

- Replacing Cerner or Oracle Fusion operational functionality or acting as a system of record for clinical care.
- Real-time clinical decision support delivered back into the EHR (the graph may *feed* CDS later; not in scope).
- Imaging pixel data ingestion (DICOM metadata only; pixels remain in PACS/VNA).
- Full genomics variant storage (references to external variant stores only).
- De novo authoring of international terminologies; the platform *binds to* SNOMED CT, LOINC, RxNorm etc.

## 5. Personas

| Persona | Needs | Modules |
|---------|-------|---------|
| Ontologist / Taxonomist | Author and review vocabularies, hierarchies, axioms; see agent proposals with evidence | 02, 10 |
| Data Steward (clinical, financial, supply, HR) | Approve mappings, own DQ thresholds, resolve exceptions | 03, 09, 11 |
| Integration Engineer | Add a source, define a topic, map HL7 v2 → FHIR | 03, 06, 07 |
| Data Warehouse Architect | Evolve subject areas, conformed dimensions, DDL | 04, 05 |
| Analyst / Data Scientist | Query EDW and KG with shared semantics; trust DQ scores | 05, 08, 09 |
| Privacy & Security Officer | Prove PHI minimisation, access control, audit trail | 11 |
| Platform SRE | Operate Kafka, graph store, EDW, agents with SLOs | 12 |
| Agent Operator | Tune agent policies, review evaluations, roll back | 10 |

## 6. Scope matrix

| Dimension | In scope | Out of scope |
|-----------|----------|--------------|
| Sources | Cerner Millennium (clinical, ADT, orders, results, documents, scheduling, charges), Oracle Fusion ERP/HCM/SCM, LIS, RIS/PACS metadata, pharmacy, RCM/claims clearinghouse, HIE, patient portal, device gateways, CRM, payer eligibility | Pixel data, raw genomics, consumer wearables (phase 3) |
| Standards | HL7 v2.3–2.8, FHIR R4 (US Core 6.1 + custom profiles), CDA R2 (C-CDA 2.1 inbound), X12 005010 (837/835/270/271/278), SKOS, OWL 2, SHACL, PROV-O | FHIR R5 (planned via profile versioning), openEHR |
| Platforms | Kafka + Schema Registry + Connect, stream processors, RDF graph store with RDF-star, ANSI-SQL EDW (Teradata-compatible DDL), agent runtime | Vendor lock-in to a single cloud |
| Time | Bitemporal everywhere; event-time streaming | — |

## 7. Guiding principles (from the Ontology Pipeline framework, applied)

1. **Metadata before data.** Nothing enters the pipeline without a registered source, element and steward.
2. **Controlled vocabulary before hierarchy.** Terms get preferred labels, definitions, scope notes and codes before
   they are arranged.
3. **Hierarchy before relationships.** Taxonomies establish `broader/narrower` before thesaurus-level associative
   relations.
4. **Relationships before axioms.** Thesaurus relations are promoted to typed OWL object properties with
   domain/range only after they are stable.
5. **Axioms before instances.** The knowledge graph is populated against a validated ontology and SHACL shapes.
6. **Every stage is governed and versioned.** Each stage has an owner, a change process and a release cadence.
7. **Agents do the work; people hold the authority.** Autonomy scales with demonstrated evaluation performance.

## 8. Success metrics (platform-level KPIs)

- Semantic coverage: % of EDW columns, FHIR elements and Kafka fields bound to ontology IRIs.
- Interoperability: FHIR validation pass rate; HL7 v2 ACK error rate; partner conformance test pass rate.
- Data quality: composite DQ index per subject area; critical-rule breach count; time-to-remediate.
- Autonomy: agent change acceptance rate; rollback rate; evaluator agreement with human reviewers.
- Timeliness: p95 event-to-canonical latency; p95 canonical-to-KG and canonical-to-EDW latency.
- Compliance: PHI access exceptions; audit completeness; lineage coverage.
