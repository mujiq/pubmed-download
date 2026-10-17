---
title: Platform & Operations — Module Overview
module: 12-platform-and-operations
doc_id: OPS-00
version: 0.1.0
status: draft
owners: [Data Platform Engineering, Platform SRE]
depends_on: [VIS-00, ARC-01, GOV-00]
last_updated: 2026-10-17
---

# Module 12 — Platform & Operations

## 1. Purpose and scope

Module 12 specifies how HEALIX is **deployed, promoted, observed and recovered**. It turns the technology baseline
of ARC-01 §3 and the platform-level NFRs (ARC-NFR-001..007) into an operable system: a Kubernetes-based topology
with network zones that reach Cerner Millennium on-prem and Oracle Fusion Cloud, four environments driven by
GitOps, a CI pipeline that validates every machine-readable artefact before promotion, OpenTelemetry-based
observability with SLOs per persona, and a disaster-recovery and retention regime that satisfies RPO ≤ 15 min /
RTO ≤ 4 h (ARC-NFR-007) and the record-retention policy inputs from module 11.

| Question | Document | Requirement prefix |
|----------|----------|--------------------|
| *Where does each component run, how is it reached and how is it made highly available?* | [01 Deployment Topology](01-deployment-topology.md) | `OPS-DEP-NNN` |
| *How does a change travel from Git to production, and how do we keep PHI out of non-prod?* | [02 Environments & CI/CD](02-environments-cicd.md) | `OPS-ENV-NNN` |
| *How do we know the platform is healthy and meeting its promises?* | [03 Observability & SLOs](03-observability-slos.md) | `OPS-OBS-NNN`, `OPS-SLO-NNN` |
| *How do we survive loss of a zone, a store or a region, and how long do we keep data?* | [04 DR & Retention](04-dr-and-retention.md) | `OPS-DR-NNN` |

**In scope:** infrastructure topology and sizing, network zones, HA, environment strategy, GitOps, CI stages and
promotion gates, release and versioning mechanics for ontology/schema/topics, observability stack, SLIs/SLOs,
alerting, on-call, dashboards, cost observability, backup/restore, Kafka mirroring, point-in-time recovery, DR
testing, retention execution, legal hold and data destruction. The module 14 memory stack (Memory API with Mem0,
Graphiti memory graph, Redis semantic cache, healing and improvement controllers) is operated here as platform
components C26–C29 (ARC-01 §3 rows "Agent memory", "Caching", "Self-improvement"; ADR-08, ADR-09).

**Out of scope:** the content of policies (module 11), topic catalog semantics (module 07), DQ rule semantics
(module 09), agent behaviour (module 10). Where this module *executes* a policy (retention, masking in non-prod),
the policy itself is owned elsewhere and referenced by requirement ID.

## 2. Platform component inventory

| # | Component | Role in HEALIX | Default technology (ARC-01 §3) | Stateful? | Owning module | Primary SLO (OPS-03) |
|---|-----------|----------------|--------------------------------|-----------|---------------|----------------------|
| C1 | **Kafka cluster** (KRaft) | Event backbone for `raw → std → canonical → dq → kg/edw → ops` layers | Apache Kafka 3.x, 3 controllers + ≥ 6 brokers per prod cluster | Yes | 07 | Availability 99.95 %; produce latency |
| C2 | **Kafka Connect** | Debezium CDC (Millennium replica), HL7 v2 MLLP source, BICC poller, FHIR Subscription bridge, sink connectors | Kafka Connect distributed mode, Debezium 2.x, HAPI HL7 | Offsets in Kafka | 07, 03 | Connector uptime; ingest latency |
| C3 | **Schema Registry** | Avro (`raw/std`), JSON Schema (`canonical`, `ops`) subjects with compatibility rules | Confluent-compatible (Karapace / Apicurio / Confluent SR) | Backed by Kafka | 07 | Availability 99.95 % |
| C4 | **Flink** | Stateful standardisation, canonicalisation, consent gating, DQ evaluation, event-time joins | Apache Flink 1.19+ on Kubernetes operator; RocksDB state, checkpoints to object storage | Yes (state) | 07, 09 | Event-to-canonical p95 ≤ 5 s |
| C5 | **Kafka Streams apps** | Stateless maps, header guards | Kafka Streams | No | 07 | — |
| C6 | **Terminology Server** | FHIR `CodeSystem`/`ValueSet`/`ConceptMap`; `$lookup`, `$validate-code`, `$translate`, `$expand` | Ontoserver or Snowstorm-backed FHIR TS | Yes (DB) | 06 | p95 `$translate` ≤ 50 ms |
| C7 | **Ontology Registry** | Versioned SKOS/OWL/SHACL served over API; reasoner jobs | Git + triple store + HermiT/ELK job runner | Yes | 02 | Availability 99.9 % |
| C8 | **Temporal knowledge graph store** | RDF-star bitemporal graph, SPARQL 1.2, named graphs, access partitions | GraphDB / Stardog / Neptune / Virtuoso cluster | Yes | 08 | KG load latency; query p95 |
| C9 | **SPARQL proxy** | OIDC auth, query rewriting for named-graph access and masking (GOV-02) | Custom service | No | 11 | — |
| C10 | **EDW** | Subject-area schemas G1–G9, conformed dimensions, history tables | Teradata Vantage / Snowflake / Databricks SQL / BigQuery | Yes | 05 | EDW freshness; query availability |
| C11 | **EDW sink and dbt-style models** | Canonical → EDW projection; nightly reconciliation with KG | Kafka Connect JDBC/native sink + dbt | Offsets in Kafka | 05 | Freshness |
| C12 | **DQ engine** | Executes rule packs on canonical streams; publishes `hc.dq.*` | Flink + Great Expectations-style rule runner | State in Flink | 09 | DQ detection time |
| C13 | **Agent runtime** | Runs `agent.*` with LLM tool calls over MCP; sandboxed per task | Container per task; model gateway | No (memory in stores) | 10 | CR cycle time |
| C14 | **Workflow engine** | Durable orchestration of CRs, pipelines, approvals | Temporal.io (durable) + Airflow (batch schedules) | Yes | 10 | Workflow availability |
| C15 | **MCP servers** | Tool surface for agents: catalog, terminology, graph, EDW (masked), DQ, Git, registry | One MCP server per tool domain behind the tool gateway PEP | No | 10, 11 | Tool latency; denial rate |
| C16 | **Token vault / de-identification service** | FF1 tokens, HMAC pseudonyms, Safe Harbor transform | Custom + HSM | Yes (vault) | 11 | Availability 99.9 % |
| C17 | **Policy Decision Point** | OPA with Rego bundle from access matrix | OPA | Bundle cache | 11 | Decision p99 ≤ 10 ms |
| C18 | **Identity** | OIDC/SAML, workload identity, SCIM | Enterprise IdP + SPIFFE/SPIRE | External | 11 | — |
| C19 | **Secrets and KMS** | Secrets injection; keys for all stores | Vault + cloud KMS / HSM | Yes | 11 | — |
| C20 | **Observability stack** | Metrics, logs, traces, dashboards, alerting, SLO engine | OpenTelemetry Collector, Prometheus/Mimir, Loki, Tempo, Grafana, Alertmanager | Yes | 12 | Own availability 99.9 % |
| C21 | **Lineage service** | OpenLineage ingest, PROV-O mapping, lineage API | Marquez-compatible + custom mapper | Yes | 11 | Mapping latency ≤ 5 min |
| C22 | **SIEM** | Security analytics on `hc.ops.audit.v1` | Enterprise SIEM | External | 11 | Delivery ≤ 60 s |
| C23 | **GitOps and CI** | Argo CD / Flux reconcilers; CI runners | Argo CD, GitHub Actions / GitLab CI runners in management zone | Git | 12 | Pipeline duration |
| C24 | **Object storage** | Flink checkpoints, backups, de-identified extracts, Parquet landing | S3 / ADLS / OCI Object Storage, versioned, encrypted | Yes | 12 | Durability 11 nines |
| C25 | **Edge connectivity** | IPsec VPN / private link to Cerner Millennium on-prem; private endpoint to Oracle Cloud | Cloud VPN gateways / Direct Connect / ExpressRoute / FastConnect | — | 12 | Link availability 99.9 % |
| C26 | **Agent memory service (Memory API + Mem0)** | MCP `memory` server with write-gate / PHI firewall (sole writer to all memory stores, MEM-ARCH-002); Mem0 OSS for M1 preferences, decisions and steward feedback; event-sourced from `hc.ops.agent.memory.episode.v1` (ADR-08) | Mem0 OSS self-hosted in-VPC (`enable_graph: false`) on PostgreSQL + pgvector (default) or Qdrant; one instance per environment | Yes (derived; rebuildable from the write log ≤ 4 h) | 14 | Memory write availability 99.9 %; recall p95 ≤ 200 ms (S23) |
| C27 | **Agent memory graph (Graphiti)** | M1 episodes and M2 bitemporal entity graph of the agent layer (`valid_at/invalid_at/created_at/expired_at` aligned to TKG-01 §4); hybrid search; index of M3 playbooks and prompt versions; never stores PHI or patient instance IRIs (MEM-GRA-014) | Graphiti (Zep) on Neo4j 5.x Enterprise causal cluster, one database per environment; FalkorDB or Amazon Neptune swappable behind the Graphiti driver (MEM-GRA-001) | Yes (derived; rebuildable from the write log) | 14 | Hybrid recall p95 ≤ 500 ms; ingest lag p95 ≤ 60 s (S23) |
| C28 | **Semantic / response cache (Redis)** | Cache tiers C1 exact-match, C2 semantic (Redis vector index), C4 memory-recall cache; C3 Terminology Service gateway cache shares the Redis-compatible tier (`ts.*`); fail-open bypass ≤ 50 ms; provider prompt cache (C0) is external to the platform | Redis Cluster with vector search (Redis Stack-compatible), 3 AZ, 1 replica per shard, one cluster per environment, keyspaces `rc.*` / `sc.*` / `mc.*` / `ts.*`, per-agent ACL users | Yes (ephemeral; never backed up — rebuilt from origin) | 14 | Hit ratio; bypass-on-failure ≤ 0.1 %; invalidation lag p95 ≤ 60 s (S24) |
| C29 | **Self-healing and improvement controllers** | Temporal workflows `healing-incident-v1` (playbook engine, blast-radius budget check, kill-switch hierarchy) and `improvement-candidate-v1` (canary controller 5 % → 25 % → promoted, rollback ≤ 5 min); `playbook-runner` MCP server; recursion ledger (ADR-09) | Temporal workflows on C14; playbooks, prompts and policies as signed Git artefacts (M3); events on `hc.ops.heal.incident.v1`, `hc.ops.heal.action.v1`, `hc.ops.improve.canary.v1`, `hc.ops.agent.policy.release.v1` | State in C14 and Git | 14 | Healing MTTR per fault class; canary rollback ≤ 5 min (S25) |

### 2.1 Component interaction (control and data paths)

```mermaid
flowchart LR
  subgraph SRC[Source-connect zone]
    MLLP[HL7 v2 MLLP listener C2]
    DBZ[Debezium CDC C2]
    BICC[BICC poller C2]
    FSUB[FHIR Subscription bridge C2]
  end
  subgraph PLAT[Platform zone]
    K[(Kafka C1)]
    SR[(Schema Registry C3)]
    FL[Flink C4 · Streams C5]
    TS[Terminology Server C6]
    OR[(Ontology Registry C7)]
    KG[(Graph store C8)]
    EDW[(EDW C10)]
    DQ[DQ engine C12]
    AGR{{Agent runtime C13}}
    WF[Workflow engine C14]
    MCP[MCP servers C15]
    PDP[PDP C17]
    TV[(Token vault C16)]
    MEM[(Memory API · Mem0 C26)]
    GRA[(Graphiti · Neo4j C27)]
    RC[(Redis cache C28)]
    HEAL[Healing · canary controllers C29]
  end
  subgraph MGMT[Management zone]
    OBS[Observability C20]
    LIN[Lineage service C21]
    GIT[GitOps · CI C23]
  end
  MLLP & DBZ & BICC & FSUB -->|mTLS| K
  SR -.-> K
  K <--> FL
  FL --> TS
  TS --> OR
  FL --> DQ --> K
  K --> KG
  K --> EDW
  WF --> AGR --> MCP --> PDP
  MCP --> OR & TS & KG & EDW & TV
  MCP --> MEM & RC
  MEM --> GRA
  MEM -. hc.ops.agent.memory.episode.v1 .-> K
  HEAL --> WF
  HEAL -. healing and canary events .-> K
  OBS -. signals .-> HEAL
  GIT -->|reconcile| PLAT
  PLAT -. OTel .-> OBS
  K -. hc.ops.lineage.openlineage.v1 .-> LIN
  classDef ext fill:#eee,stroke:#999;
```

Data paths are solid; control and telemetry paths are dotted. No component outside the source-connect zone opens a
connection toward a source system; sources are reached only by C2 connectors (GOV-02 §8). C26–C28 hold no PHI by
contract (ADR-08, MEM-CACHE-001, GOV-01 §3 Table 3-A ceiling `internal`) and have no path to `raw`/`std` topics or PHI
stores; C29 holds no credential that writes to sources, `canonical`/`kg`/`edw` topics, DDL or graph assertions
(MEM-HEAL-008).

## 3. Interfaces this module exposes

| Interface | Form | Consumers |
|-----------|------|-----------|
| Environment catalog (`dev`, `test`, `uat`, `prod`) and promotion gates | `02-environments-cicd.md` §2, §5 | All modules |
| CI stage contract (what a module artefact must pass) | `02-environments-cicd.md` §4 | 02, 05, 06, 07, 09, 10, 11 |
| SLI definitions and SLO targets | `03-observability-slos.md` §3 | 13 (acceptance), 07, 08, 05, 09, 10 |
| OpenTelemetry semantic conventions for HEALIX (`healix.*` attributes) | `03-observability-slos.md` §2 | All emitters |
| Backup/restore and DR runbooks; retention job contract | `04-dr-and-retention.md` | 11 (policy owner), 07, 08, 05 |
| Sizing and capacity model | `01-deployment-topology.md` §5 | 13 |

## 4. Dependencies on other modules

| Depends on | For |
|------------|-----|
| ARC-01 | Technology baseline, NFRs ARC-NFR-001..007, layer model |
| GOV-01 / GOV-02 | PHI classes for non-prod masking; network zone rules; key classes; audit topic |
| GOV-03 | Retention periods for audit and lineage; audit emission by platform components |
| Module 07 (`STR-*`) | Topic catalog as the source of Kafka provisioning; retention per topic |
| Module 08 (`TKG-*`), module 05 (`EDW-*`) | Store-specific backup/PITR capabilities |
| Module 10 (`AGT-*`) | Agent manifests and evaluation suite as CI inputs; CR cycle metrics |
| Module 14 (`MEM-*`) | Memory API, `memory-schema.yaml`, `cache-policy.yaml` and healing playbooks as deployable artefacts; `healix.memory.*` / `healix.cache.*` / healing and canary metrics registered as SLOs S23–S25 (MEM-ARCH-040); rebuild-from-write-log DR contract (MEM-ARCH-039); fault taxonomy and kill-switch hierarchy consumed by on-call |

## 5. Requirement index

| Range | Topic | Document | P0 count (indicative) |
|-------|-------|----------|-----------------------|
| `OPS-DEP-001`–`OPS-DEP-021` | Kubernetes topology, zones, HA per component, sizing, cloud mapping, Cerner on-prem connectivity, module 14 memory stack placement | 01 | 14 |
| `OPS-ENV-001`–`OPS-ENV-022` | Environments, GitOps, CI stages, promotion gates, non-prod data, release and versioning | 02 | 15 |
| `OPS-OBS-001`–`OPS-OBS-015` | Telemetry, dashboards, alerting, on-call, cost, module 14 signal registration | 03 | 8 |
| `OPS-SLO-001`–`OPS-SLO-013` | SLI/SLO targets and error budgets, including memory, cache and healing SLOs | 03 | 9 |
| `OPS-DR-001`–`OPS-DR-021` | Backup, mirroring, PITR, RPO/RTO tiers, retention, legal hold, destruction, DR tests, memory-store rebuild | 04 | 14 |

## 6. Operating model

| Role | Responsibilities |
|------|------------------|
| Platform SRE (on-call) | Runs C1–C29; owns SLOs, incident response, DR drills, capacity; pairs with the Agent Operator for C26–C29 |
| Data Platform Engineering | Builds connectors, Flink jobs, sinks, CI pipelines; pairs with module teams |
| Agent Operator (module 10) | Tunes agent policies and evaluation thresholds; consumes agent SLOs |
| Privacy & Security Office (module 11) | Approves non-prod data strategy, DR data handling, retention execution evidence |
| Release Manager (`agent.release_manager` + human) | Executes promotion gates; publishes release notes |

## 7. Operational standards

| Standard | Rule |
|----------|------|
| Infrastructure as code | Every cluster, namespace, topic, database, bucket, policy and dashboard is declared in Git (Terraform, Helm, Kustomize, `topics.yaml`, Grafana JSON); console changes are reverted by reconciliation |
| Runbooks | One runbook per component (C1–C29) and per alert, stored beside the Helm chart; a page without a runbook link is a CI failure; module 14 healing playbooks are the machine-executable counterpart of the C1–C25 runbooks and reference them |
| Change windows | `prod` promotions Tuesday–Thursday 09:00–15:00 local unless an emergency change is approved by the on-call lead and the release manager |
| Incident severity | SEV1 PHI exposure or platform down; SEV2 SLO breach affecting clinical consumers; SEV3 single component degraded; SEV4 cosmetic. SEV1/2 trigger a blameless post-incident review within 5 business days |
| Capacity reviews | Monthly; forecast 6 months using the sizing model in OPS-01 §5 |
| Patching | Kubernetes minor versions within 90 days of release; critical CVEs in platform images within 7 days |
| Access to prod | Through the management zone only; no PHI readable by SRE roles (GOV-02 §3) |
| Evidence | Every drill, restore test, access review and scanner run writes a signed record to G9 for auditors |

## 8. Definition of done for this module

1. Every `P0` requirement has a verification artefact (Terraform/Helm policy test, CI check, chaos or DR drill
   record) registered in module 13.
2. All four environments reconcile from Git with zero manual drift for 30 consecutive days.
3. SLO dashboards for every SLI in OPS-03 exist with error-budget burn alerts wired to on-call.
4. A full DR test has restored `prod` to the secondary region within RTO and RPO (OPS-04 §7) at least once.
5. Non-prod PHI scanner (GOV-PHI-011) has run daily for 30 days with zero findings.
6. Memory stores (C26, C27) have been rebuilt from `hc.ops.agent.memory.episode.v1` within RTO in a drill (OPS-04 §9), the
   Redis cold-start bypass test has passed, and SLOs S23–S25 are on dashboards with burn alerts (OPS-03).
