# Conventions

All modules in this PRD follow these conventions. Agents (module 10) enforce them with linters.

## 1. Document front matter

Every specification document begins with a YAML front-matter block:

```yaml
---
title: <Document title>
module: <NN-module-slug>
doc_id: <MODULE-PREFIX>-<NN>          # e.g. ONT-03
version: 0.1.0
status: draft | review | approved | deprecated
owners: [<role or team>]
depends_on: [<doc_id>, ...]           # cross-module contracts this document relies on
last_updated: YYYY-MM-DD
---
```

## 2. Requirement identifiers

Requirements are atomic, testable statements with a stable ID: `<PREFIX>-<AREA>-<NNN>`.

| Module | Prefix | Example |
|--------|--------|---------|
| 02 Ontology Pipeline | `ONT` | `ONT-CV-004` (controlled vocabulary) |
| 03 Source Systems | `SRC` | `SRC-CER-012` (Cerner), `SRC-OFC-003` (Oracle Fusion), `SRC-PER-020` (peripheral) |
| 04 Reference Data Models | `RDM` | `RDM-HAR-002` |
| 05 EDW Schema | `EDW` | `EDW-G2-015` (subject-area group 2) |
| 06 Interoperability | `IOP` | `IOP-FHIR-007`, `IOP-V2-003` |
| 07 Streaming | `STR` | `STR-TOP-010` |
| 08 Temporal KG | `TKG` | `TKG-TIME-002` |
| 09 Data Quality | `DQ` | `DQ-RULE-031` |
| 10 Agentic Pipeline | `AGT` | `AGT-ORCH-005` |
| 11 Governance/Security | `GOV` | `GOV-PHI-001` |
| 12 Platform/Ops | `OPS` | `OPS-SLO-004` |
| 13 Roadmap | `RMP` | `RMP-PH1-002` |
| 14 Memory & Self-Improvement | `MEM` | `MEM-GRA-003` (Graphiti), `MEM-CACHE-002`, `MEM-HEAL-004`, `MEM-IMP-001` |
| 16 User Experience | `UX` | `UX-ONT-004` (ontology workbench), `UX-CR-002`, `UX-DS-001` (design system) |
| 15 Adversarial Review | `ADV` | `ADV-LEG-012` (legacy), `ADV-SCALE-003` |

Each requirement uses RFC 2119 keywords (**MUST**, **SHOULD**, **MAY**) and carries a priority
(`P0` launch-blocking, `P1` first release, `P2` later) and a verification method (`test`, `inspection`, `demo`, `analysis`).

Format:

> ONT-CV-004 (P0, test) — The vocabulary service MUST reject a new preferred label that collides
> case-insensitively with an existing preferred label in the same concept scheme.

(The ID above is shown unbolded because it is only an illustration of the format; the normative requirement lives in
`02-ontology-pipeline/02-controlled-vocabularies.md`. In module documents the ID is always bold, which is how the linter
indexes it.)

## 3. Naming

| Thing | Convention | Example |
|-------|-----------|---------|
| Kafka topic | `hc.<layer>.<domain>.<entity>.<event-or-format>.v<N>` (lowercase, dots) | `hc.raw.cerner.adt.hl7v2.v1`, `hc.canonical.clinical.encounter.fhir.v1` |
| Kafka layer | `raw` → `std` (parsed/validated) → `canonical` (FHIR) → `dq` → `kg` → `edw` → `ops` | — |
| Schema Registry subject | `<topic>-value` / `<topic>-key` | `hc.canonical.clinical.encounter.fhir.v1-value` |
| EDW schema (database) | `edw_<group-code>_<subject>` | `edw_g2_clinical` |
| EDW table | `dim_*`, `fact_*`, `bridge_*`, `ref_*`, `stg_*`, `hist_*` ; snake_case | `fact_encounter`, `dim_patient` |
| EDW column | snake_case; keys `*_sk` (surrogate), `*_bk` (business key), `*_id` (natural) | `patient_sk`, `encounter_bk` |
| Ontology IRI base | `https://ontology.healix.example.org/` | `https://ontology.healix.example.org/clinical/Encounter` |
| Concept scheme IRI | `<base>scheme/<scheme-slug>` | `<base>scheme/encounter-class` |
| Named graph (temporal) | `<base>graph/<source>/<yyyy-mm-dd>/<batch-ulid>` | — |
| DQ rule id | `DQR-<DIMENSION>-<DOMAIN>-<NNN>` | `DQR-COMPLETENESS-PATIENT-001` |
| Agent id | `agent.<role>` | `agent.taxonomist` |
| Environment | `dev`, `test`, `uat`, `prod` | — |

Subject-area **group codes** `G1`–`G9` are defined in `01-architecture-overview.md` §4 and are the single source of truth.

## 4. Time and provenance

- All timestamps are ISO 8601 with explicit offset or `Z`; stored as UTC.
- Every persisted fact carries **valid time** (`valid_from`, `valid_to`), **transaction time** (`recorded_at`,
  `superseded_at`) and a **provenance reference** (`prov_activity_iri`). See module 08.
- Kafka records carry **event time** in the header `hc-event-time` and **source time** in `hc-source-time`.

## 5. Versioning

- Documents: semantic version in front matter; breaking contract change ⇒ major bump and a new `vN` topic/IRI.
- Ontology: each release is a dated version IRI (`owl:versionIRI`) and a SKOS `dcterms:modified`.
- Schema Registry: `BACKWARD_TRANSITIVE` compatibility for `canonical.*`; `FULL` for `kg.*` and `edw.*`.
- EDW: migrations are forward-only, idempotent, and reversible by compensating migration.

## 6. Definition of done for a module

1. All `P0` requirements have an automated verification artefact referenced by ID.
2. Machine-readable artefacts validate (YAML schema, SQL parse, SHACL, OWL reasoner consistency).
3. Cross-module `depends_on` entries resolve to approved documents.
4. Security & privacy review (module 11) signed off.
5. Agent evaluation suite (module 10) passes at the thresholds in `13-roadmap-and-acceptance/02-acceptance-criteria.md`.

## 7. Diagram conventions

Diagrams are Mermaid in Markdown so they are diffable. Boxes: rectangles = components, cylinders = stores,
hexagons = agents, parallelograms = external systems.
