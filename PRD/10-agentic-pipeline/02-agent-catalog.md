---
title: Agent Catalog
module: 10-agentic-pipeline
doc_id: AGT-02
version: 0.1.0
status: draft
owners: [Agent Operations, Clinical Informatics, Data Governance]
depends_on: [ARC-01, AGT-01, AGT-03, AGT-04, AGT-05, AGT-06]
last_updated: 2026-10-17
---

# Agent Catalog

One section per agent in the authoritative roster (`01-architecture-overview.md` §5). The manifest in
`agents/<role>.yaml` is the executable truth for tools, triggers, budgets and thresholds; this catalog explains the
*why* and gives the operator-facing summary. Levels are the **initial** levels; graduation rules are in README §4
and the per-agent `graduation` block. Rubric ids are defined in AGT-05 §4.3; PGV rules in AGT-05 §2.

| # | Agent | Stage | Level (ceiling) | Model ref | CR types proposed |
|---|-------|-------|-----------------|-----------|-------------------|
| 1 | `agent.orchestrator` | all | L3 (L3) | `${MODEL_ORCHESTRATION}` | — (routes) |
| 2 | `agent.inventory` | 0 | L3 (L3) | `${MODEL_MAPPING}` | vocabulary (stewardship) |
| 3 | `agent.vocabulary_curator` | 1 | L2 (L3) | `${MODEL_MAPPING}` | vocabulary |
| 4 | `agent.taxonomist` | 2 | L2 (L3) | `${MODEL_MAPPING}` | taxonomy |
| 5 | `agent.thesaurus_editor` | 3 | L2 (L3) | `${MODEL_MAPPING}` | thesaurus |
| 6 | `agent.ontologist` | 4 | L1 (L2) | `${MODEL_REASONING}` | ontology |
| 7 | `agent.kg_builder` | 5 | L3 (L3) | `${MODEL_MAPPING}` | mapping |
| 8 | `agent.source_mapper` | 0, 5 | L2 (L3) | `${MODEL_MAPPING}` | mapping |
| 9 | `agent.terminology_steward` | 1, 3 | L2 (L3) | `${MODEL_CLASSIFICATION}` | value_set |
| 10 | `agent.schema_architect` | 4, 5 | L1 (L2) | `${MODEL_REASONING}` | edw_ddl |
| 11 | `agent.dq_author` | 4, 5 | L2 (L3) | `${MODEL_MAPPING}` | dq_rule |
| 12 | `agent.dq_monitor` | 5 | L3 (L3) | `${MODEL_CLASSIFICATION}` | — (triggers, issues) |
| 13 | `agent.remediator` | 5 | L2 (L3) | `${MODEL_CLASSIFICATION}` | dq_rule (remediation plans) |
| 14 | `agent.interop_engineer` | 4, 5 | L2 (L3) | `${MODEL_MAPPING}` | profile, topic |
| 15 | `agent.privacy_guardian` | cross | L4 (L4) | `${MODEL_CRITIQUE}` | topic (ACL/mask) |
| 16 | `agent.evaluator` | cross | n/a | `${MODEL_CRITIQUE}` | — (evaluates) |
| 17 | `agent.release_manager` | all | L2 (L3) | `${MODEL_MAPPING}` | ontology (compensating) |
| 18 | `agent.memory_curator` | cross | L3 (L3) | `${MODEL_REASONING}` | dq_rule (playbook promotion) |
| 19 | `agent.self_healer` | cross | L3 (L3) | `${MODEL_REASONING}` | dq_rule (playbook promotion) |
| 20 | `agent.improver` | cross | L1 (L2) | `${MODEL_CRITIQUE}` | ontology (policy) |

## 1. `agent.orchestrator`

- **Mission.** Plan and schedule runs, route CRs to evaluator → privacy gate → approval gate, enforce autonomy
  policy and budgets. Never edits an artefact.
- **Triggers.** Nightly plan (01:00); every `hc.ops.agent.trigger.v1` event; every CR and evaluation transition;
  manual.
- **Inputs / outputs.** Pending triggers, manifests, gate policy, CR graph → run plans, routing decisions, decision
  log, daily control-plane report.
- **Tools.** `cr-engine` (apply), `lineage`, `ticketing` (apply); read on registries, kafka-admin, dq-engine, git.
- **Autonomy.** L3, ceiling L3. Demoted to L2 on any gate bypass.
- **Risk classes.** `low` (its only CRs are governance/manifests).
- **Rubrics.** RUB-OPS-01 routing/plan quality; RUB-EVD-01.
- **Failure modes → fallbacks.** Trigger storm → debounce + dedup by workflow id; routing bug → kill switch halts
  auto-approvals while proposals continue; budget exhausted → stop launching runs, keep gates alive.

## 2. `agent.inventory`

- **Mission.** Stage 0: discover and register every source, schema, element and code set with steward and PHI hint.
- **Triggers.** `new_source_discovered`, `schema_drift`, nightly crawl (02:30), manual.
- **Inputs / outputs.** Schema Registry, Millennium data dictionary (via replica), Fusion BICC PVO metadata, HL7 v2
  conformance profiles, masked profiles → Source Registry and Data Element Catalog entries,
  `new_element_registered` events, stewardship proposals.
- **Tools.** `source-registry` (apply), `schema-registry`, `kafka-admin.sample_records` (masked), `sql-sandbox`
  (masked), `dq-engine.profile_dataset`, `lineage` (apply), `ticketing`.
- **Autonomy.** L3 (metadata-only, reversible writes). Demoted on a confirmed PHI-hint false negative.
- **Risk classes.** `low`, `medium`.
- **Rubrics.** RUB-INV-01, RUB-EVD-01, RUB-PHI-01.
- **Failure modes.** Source unreachable → mark and retry in 6 h; ambiguous element → register `unclassified` and
  ticket the steward; duplicate discovery → newest source timestamp wins, both recorded.

## 3. `agent.vocabulary_curator`

- **Mission.** Stage 1: turn observed terms and codes into SKOS concepts with preferred/alternate labels,
  definitions, scope notes, notations and matches to standard terminologies.
- **Triggers.** `new_element_registered`, `unmapped_code`, weekday harvest (03:00), manual.
- **Inputs / outputs.** Elements flagged `needs_vocabulary`, Cerner `CODE_VALUE` text (untrusted), Fusion lookup
  meanings, masked frequencies, label style guide → `vocabulary` CRs (create, deprecate, merge), harvest report.
- **Tools.** `ontology-registry` (propose incl. `check_label_collision`), `terminology-service` (read),
  `source-registry`, masked samples, `git` (propose).
- **Autonomy.** L2 → L3 after ≥ 500 CRs / 90 days, κ ≥ 0.97, rollback ≤ 0.5 %, zero vetoes. Threshold 0.90.
- **Risk classes.** `low`, `medium`, `phi_affecting` (a label derived from a PHI-classified element).
- **Rubrics.** RUB-VOC-01/02/03, RUB-EVD-01, RUB-PHI-01.
- **Failure modes.** Label collision → revise with disambiguated label; weak definition → `revise` once then human;
  display text with injected instructions → scan fail, judged on merits; PHI in scope note → PGV-001 veto and
  demotion.

## 4. `agent.taxonomist`

- **Mission.** Stage 2: place concepts in `broader/narrower` hierarchies and facets; nightly validate for cycles,
  orphans, depth, polyhierarchy; align with SNOMED CT / LOINC / ICD subsumption and G1–G9.
- **Triggers.** Applied `vocabulary` CR; nightly validation (04:00); `terminology_release`; manual.
- **Inputs / outputs.** Concepts lacking `skos:broader`, facet definitions, external subsumption → `taxonomy` CRs,
  repair proposals, nightly validation report.
- **Tools.** `ontology-registry` (`get_hierarchy`, `detect_cycles`, `list_orphans`, propose),
  `terminology-service.subsumes/closure`, `graph-query` (read), `git`.
- **Autonomy.** L2 → L3 after ≥ 300 CRs, κ ≥ 0.97, 60 green nightly validations. Threshold 0.92.
- **Risk classes.** `low`, `medium`, `high` (re-parenting a concept with bound DQ rules or dimensions).
- **Rubrics.** RUB-TAX-01, RUB-TAX-02, RUB-EVD-01, RUB-PHI-01.
- **Failure modes.** Cycle introduced → `cycle_detection` fail, reject; polyhierarchy within one facet → revise;
  conflicting re-parenting CRs → serialised and rebased.

## 5. `agent.thesaurus_editor`

- **Mission.** Stage 3: add ISO 25964 associative and equivalence relations, scope/history notes, so relations are
  stable before ontological promotion.
- **Triggers.** Applied `taxonomy` CR; weekly co-occurrence mining (Tue 05:00); manual.
- **Inputs / outputs.** Concepts without relations, external maps (SNOMED↔ICD-10-CM, LOINC↔SNOMED, RxNorm↔NDC),
  masked co-occurrence statistics → `thesaurus` CRs, candidate-relation report.
- **Tools.** `ontology-registry` (propose), `terminology-service`, `graph-query` (read), `sql-sandbox` (masked),
  `git`.
- **Autonomy.** L2 → L3 after ≥ 300 CRs, exactMatch precision ≥ 0.98. Threshold 0.90.
- **Risk classes.** `low`, `medium`.
- **Rubrics.** RUB-THS-01, RUB-EVD-01, RUB-PHI-01.
- **Failure modes.** Over-strong match → downgrade exactMatch→closeMatch and resubmit; spurious co-occurrence
  relation (support high, semantics absent) → judge rejects on RUB-THS-01; wrong exactMatch surfaces in DQ incident
  → demotion 30 days.

## 6. `agent.ontologist`

- **Mission.** Stage 4: promote stable relations into OWL 2 DL classes/properties/axioms and SHACL shapes; run the
  reasoner on every proposal and weekly on the release candidate; keep FHIR and EDW bindings aligned.
- **Triggers.** Thesaurus relation stable ≥ 30 days; weekly reasoning (Sun 02:00); `profile_changed`; manual.
- **Inputs / outputs.** Stable relations with usage stats, current OWL/SHACL, FHIR StructureDefinitions, EDW
  bindings, UDMH/HCDM patterns (by id) → `ontology` CRs (axioms, shapes, bindings), reasoner/SHACL report.
- **Tools.** `ontology-registry` (`run_reasoner`, `validate_shacl`, `explain_inconsistency`, propose),
  `fhir-validator` (read), `edw-metadata` (read), `lineage.impact_analysis`, `git`.
- **Autonomy.** **L1 — always human-approved.** Ceiling L2 for SHACL-only low-risk CRs after ≥ 200 approvals /
  180 days and a PRD revision.
- **Risk classes.** `medium`, `high`, `phi_affecting`.
- **Rubrics.** RUB-ONT-01/02/03, RUB-EVD-01, RUB-PHI-01.
- **Failure modes.** Inconsistency → `owl_consistency` fail with justification attached; reasoner timeout → ELK
  profile fallback, check `skipped` (human review forced); over-constraint breaking existing instances → SHACL on
  synthetic bundles fails.

## 7. `agent.kg_builder`

- **Mission.** Stage 5: generate RML/R2RML and FHIR→RDF mappings, load bitemporal named graphs with PROV-O, run
  entity resolution, reconcile KG with EDW nightly.
- **Triggers.** `ontology_release`; applied `profile`/`edw_ddl` CR; nightly reconciliation (00:00); manual.
- **Inputs / outputs.** Ontology release, canonical schemas, EDW DDL, bitemporal rules, labelled EMPI pairs →
  `mapping` CRs, named-graph loads (operational), reconciliation and ER reports.
- **Tools.** `graph-query` (`load_batch` apply, masked reads), `ontology-registry` (read), `schema-registry`,
  `edw-metadata`, `lineage` (apply), `git`.
- **Autonomy.** L3 (loads are idempotent and replayable). Demoted on reconciliation variance > 0.1 % for 3 nights
  or any PROV-less assertion.
- **Risk classes.** `low`, `medium`, `high`.
- **Rubrics.** RUB-KG-01, RUB-ER-01, RUB-MAP-01, RUB-EVD-01, RUB-PHI-01.
- **Failure modes.** Load failure → retry same batch ULID; low mapping score → load to staging graph only;
  ER false merge on hard negatives → RUB-ER-01 fail, mapping CR rejected.

## 8. `agent.source_mapper`

- **Mission.** Map source fields → `std` → FHIR R4 (HL7 v2 segments, Millennium CDC columns, Fusion BICC/REST
  entities, HEALIX-ERP extensions) with golden tests.
- **Triggers.** `new_source_discovered`, `schema_drift`, `mapping_failure`; applied `profile` CR; manual.
- **Inputs / outputs.** Element catalog, conformance profiles (ADT^A01, ORU^R01, ORM^O01, DFT^P03, SIU^S12…), Avro
  schemas, FHIR profiles, masked samples, golden pairs → `mapping` CRs, golden-test additions, coverage report.
- **Tools.** `source-registry`, `schema-registry`, `kafka-admin.sample_records` (masked), `terminology-service`,
  `fhir-validator`, `dq-engine.run_rule`, `git`.
- **Autonomy.** L2 → L3 after ≥ 500 CRs, first-pass golden pass ≥ 0.98, FHIR pass ≥ 99.5 %. Threshold 0.93.
- **Risk classes.** `low`, `medium`, `high`, `phi_affecting` (most identifier/demographic mappings).
- **Rubrics.** RUB-MAP-01, RUB-MAP-02, RUB-EVD-01, RUB-PHI-01.
- **Failure modes.** Golden test failure → revise with failing cases; PHI routed to wrong ACL → PGV-002 veto;
  dead-letter rise after release → automatic rollback (AGT-ORCH-016) and demotion.

## 9. `agent.terminology_steward`

- **Mission.** Maintain ValueSets and ConceptMaps on the FHIR Terminology Service; classify and map unmapped codes;
  sync external releases monthly.
- **Triggers.** `unmapped_code`, `terminology_release`, monthly sync (1st 06:00), manual.
- **Inputs / outputs.** Unmapped code events, bindings, release manifests, Cerner/Fusion code metadata →
  `value_set` CRs, release impact report, unmapped backlog.
- **Tools.** `terminology-service` (propose; `$lookup/$validate-code/$translate/$expand/$subsumes/$closure`),
  `ontology-registry` (read), `sql-sandbox.profile_column` (masked), `git`.
- **Autonomy.** L2 → L3 after ≥ 2,000 CRs, equivalence precision ≥ 0.98, rollback ≤ 0.3 %. Threshold 0.92.
  High-volume classification runs on `${MODEL_CLASSIFICATION}`.
- **Risk classes.** `low`, `medium`, `high` (equivalence change on a map used by DQ rules or billing).
- **Rubrics.** RUB-VS-01, RUB-VS-02, RUB-EVD-01, RUB-PHI-01.
- **Failure modes.** Ambiguous LOINC (specimen/method) → mark `inexact`, steward decides; retired SNOMED concept in
  use → replacement via historical association, medium risk; never silently defaults to a parent code.

## 10. `agent.schema_architect`

- **Mission.** Derive EDW logical/physical models per G1–G9 from the ontology and UDMH/HCDM patterns; conformed
  dimensions; bitemporal history; forward-only reversible DDL (Teradata + portable).
- **Triggers.** `ontology_release`; applied `vocabulary`/`taxonomy` CR needing a dimension member; weekly drift
  check (Sat 03:00); manual.
- **Inputs / outputs.** Ontology release and bindings, pattern catalogue (by id), DDL/migration history,
  downstream consumers → `edw_ddl` CRs (logical change + forward and compensating migration), dry-run report.
- **Tools.** `edw-metadata` (`propose_ddl`, `ddl_dry_run`, `naming_lint`), `ontology-registry` (read),
  `sql-sandbox` (masked), `lineage.impact_analysis`, `git`.
- **Autonomy.** **L1 — always human-approved.** Ceiling L2 for additive changes to `internal`/`public`-classed columns after ≥ 150 approvals /
  180 days, zero failed migrations, PRD revision.
- **Risk classes.** `medium`, `high`, `phi_affecting`.
- **Rubrics.** RUB-DDL-01, RUB-DDL-02, RUB-EVD-01, RUB-PHI-01.
- **Failure modes.** Dry-run failure in one dialect → revise; mask removed → PGV-003 veto; duplicate dimension
  instead of conformed reuse → RUB-DDL-02 low score, architect review.

## 11. `agent.dq_author`

- **Mission.** Derive DQ rules from SHACL/OWL/value-set/FHIR constraints; profile masked data; propose thresholds;
  backtest 90 days before rules enter the pack.
- **Triggers.** Applied `ontology`/`value_set`/`profile` CR; `dq_anomaly` without covering rule; weekly coverage
  review (Wed 04:00); manual.
- **Inputs / outputs.** Shapes, cardinalities, invariants, bindings, critical-element list, backtest windows →
  `dq_rule` CRs (`DQR-<DIMENSION>-<DOMAIN>-<NNN>`), backtest and coverage reports.
- **Tools.** `dq-engine` (`profile_dataset`, `backtest_rule`, `propose_rule`; masked), `ontology-registry`,
  `terminology-service.expand`, `sql-sandbox` (masked), `git`.
- **Autonomy.** L2 → L3 after ≥ 400 CRs, false-positive rate < 5 %. Threshold 0.90.
- **Risk classes.** `low` (`warn`), `medium` (`error`), `high` (quarantining rule on a critical stream).
- **Rubrics.** RUB-DQR-01, RUB-DQR-02, RUB-EVD-01, RUB-PHI-01.
- **Failure modes.** Implausible breach rate → lower severity and resubmit once; rule writes PHI into results →
  PGV-006 veto; released rule falsely quarantines > 1 % → rollback + demotion.

## 12. `agent.dq_monitor`

- **Mission.** Run DQ pipelines and scorecards, detect anomalies and drift, attribute to upstream changes, open
  issues, trigger peers, watch 72 h after every applied CR.
- **Triggers.** Every 15 min; `dq_breach`; applied `mapping`/`value_set`/`dq_rule` CR (watch window); manual.
- **Inputs / outputs.** `hc.dq.*` results, scorecards, lag and schema history, recent CRs, baselines →
  `dq_anomaly` events, tickets with masked evidence, regression verdicts (which can trigger rollback).
- **Tools.** `dq-engine` (apply: run, scorecard, drift, masked quarantine sample), `kafka-admin` (read, masked
  samples), `lineage` (apply), `schema-registry`, `ticketing` (apply).
- **Autonomy.** L3 (opens issues and triggers only). Demoted when anomaly precision < 0.80 for 2 weeks.
- **Risk classes.** `low`.
- **Rubrics.** RUB-DQM-01, RUB-EVD-01, RUB-PHI-01.
- **Failure modes.** dq-engine unreachable > 2 windows → SRE alert; LLM budget exhausted → rule-only scorecard;
  anomaly flood → dedup by (rule, source, window).

## 13. `agent.remediator`

- **Mission.** Resolve quarantined records within policy: code re-mapping via approved ConceptMaps, deterministic
  normalisation (UCUM case, identifier format), replay after fixes; route the rest to stewards.
- **Triggers.** `dq_anomaly` (quarantine growth); applied `mapping`/`value_set` CR (replay); 4-hourly sweep; manual.
- **Inputs / outputs.** Masked quarantine samples, failing rule ids, ConceptMaps, playbooks → remediation plans
  (`dq_rule` CRs), replay jobs after approval, steward tickets.
- **Tools.** `dq-engine` (`open_quarantine_sample`, `propose_remediation`, `replay_quarantine` with approved CR),
  `terminology-service`, `fhir-validator`, `ticketing`.
- **Autonomy.** L2 → L3 after ≥ 1,000 plans, zero meaning changes, replay success ≥ 0.99. Threshold 0.95.
- **Risk classes.** `low`, `medium`, `phi_affecting` (plans touching identifier fields).
- **Rubrics.** RUB-REM-01, RUB-EVD-01, RUB-PHI-01.
- **Failure modes.** Plan would change clinical meaning (e.g. truncating ICD-10) → RUB-REM-01 fail; replay
  non-idempotent → `idempotency_check` fail; unmasked PHI access attempt → gateway reject, demotion.

## 14. `agent.interop_engineer`

- **Mission.** Generate and validate FHIR profiles (US Core 6.1 derived, HEALIX-ERP), HL7 v2 conformance profiles,
  X12/C-CDA constraints, partner conformance tests; keep canonical topics validating.
- **Triggers.** Applied `ontology`/`value_set` CR; `new_source_discovered`; `fhir_validation_regression`; weekly
  conformance run (Thu 05:00); manual.
- **Inputs / outputs.** Ontology classes/shapes, StructureDefinitions, validation stats, partner requirements →
  `profile` CRs, `topic` CRs (canonical topic + JSON Schema), conformance report.
- **Tools.** `fhir-validator` (propose, `generate_examples`), `schema-registry` (propose),
  `kafka-admin.propose_topic`, `terminology-service`, `ontology-registry`, `git`.
- **Autonomy.** L2 → L3 after ≥ 200 CRs, no compatibility breaks. Threshold 0.92.
- **Risk classes.** `low`, `medium`, `high` (cardinality tightening, schema major bump).
- **Rubrics.** RUB-PRF-01, RUB-TOP-01, RUB-MAP-02, RUB-EVD-01, RUB-PHI-01.
- **Failure modes.** `BACKWARD_TRANSITIVE` break → `schema_compatibility` fail; security-label requirement removed
  → PGV-009 veto; pass rate < 99.5 % after release → rollback and demotion.

## 15. `agent.privacy_guardian`

- **Mission.** Classify PHI on every element, enforce minimisation in tool access, review every CR, block
  disallowed flows, veto. L4 by ADR-06.
- **Triggers.** Every CR entering `evaluated`; `new_element_registered`; `phi_access_exception`; nightly leakage
  scan (01:00); manual.
- **Inputs / outputs.** CRs with payload/impact/evidence, element catalog with lineage, PHI policy matrix, ACLs,
  masks → privacy decisions with PGV rule ids, PHI classifications, leakage incidents, ACL/mask `topic` CRs.
- **Tools.** `cr-engine` (admin: `record_privacy_review`, `veto_cr`, `kill_switch`), `source-registry`
  (`set_phi_classification`), `kafka-admin` (`list_acls`, `propose_acl_change`), read on registries/EDW metadata,
  `ticketing`.
- **Autonomy.** L4, fixed; not subject to graduation or demotion. Fails closed.
- **Risk classes.** All (as reviewer); its own CRs are `topic`-typed and follow the normal gate table.
- **Rubrics.** RUB-PHI-01, RUB-PHI-02 (classification accuracy, veto precision on red-team set), RUB-EVD-01.
- **Failure modes.** Budget/tool exhaustion → fail closed, page Privacy & Security Officer; over-vetoing → human
  officer override with recorded justification; RUB-PHI-02 regression → prompt rollback via red-team gate.

## 16. `agent.evaluator`

- **Mission.** Independent critic: automated checks, rubric scoring as calibrated LLM-as-judge, verdicts,
  regression suites, weekly calibration, graduation evidence. Proposes nothing.
- **Triggers.** CR entering `proposed`; nightly regression suites (02:00); weekly calibration (Sun 06:00); manual.
- **Inputs / outputs.** CR, rubrics, golden sets, human decisions → evaluation records
  (`hc.ops.agent.evaluation.v1`), calibration report, graduation summaries.
- **Tools.** `cr-engine.record_evaluation` (apply); read/sandboxed: reasoner, SHACL, label checks,
  terminology ops, FHIR validation, `backtest_rule`, `ddl_dry_run`, `check_compatibility`, `graph-query`, `git`,
  `lineage`.
- **Autonomy.** Not applicable. Its calibration (RUB-EVL-01, κ ≥ 0.90 per CR type) gates everyone else's
  auto-approval.
- **Risk classes.** None.
- **Failure modes.** Check tool down → `skipped` (forces human review); calibration drift → auto-approval
  suspended for the type; budget pressure → prioritise `phi_affecting`/`high`.

## 17. `agent.release_manager`

- **Mission.** Bundle applied CRs into signed, versioned releases (ontology `owl:versionIRI`, migrations, topics,
  profiles, value sets); changelogs; release gate suite; rollbacks by compensating release.
- **Triggers.** Release windows (Tue/Thu 22:00); CR reaching `applied`; `rollback_requested`; manual.
- **Inputs / outputs.** Applied CRs, gate suite, calendar, consumer list → release manifests/tags/changelogs,
  gate report, compensating CRs, `ontology_release`/`terminology_release` events.
- **Tools.** `ontology-registry.publish_release` (admin), `edw-metadata.apply_migration`,
  `schema-registry.register_schema`, `kafka-admin.apply_topic_change`, `terminology-service.publish_*`,
  `fhir-validator.publish_ig`, `git.merge_pr/tag_release`, `cr-engine` (apply), `lineage`, `ticketing`.
- **Autonomy.** L2 → L3 after ≥ 60 releases / 180 days with zero assembly-caused rollbacks. Threshold 0.95.
  Releases are serialised (one at a time).
- **Risk classes.** `low`, `medium`, `high`.
- **Rubrics.** RUB-REL-01, RUB-EVD-01, RUB-PHI-01.
- **Failure modes.** Gate failure → hold release, human approval; partial publish → atomic abort, nothing
  published; vetoed CR in candidate → removed, gate suite re-run.

## 18. Memory and self-improvement agents (module 14)

These three cross-cutting agents are defined in module 14 and run on the same control plane, manifests and gates as
the rest of the roster. They never touch PHI and never mutate ontology, schema or DQ artefacts except by proposing a
change request that the evaluator and a human approve.

### 18.1 `agent.memory_curator`

- **Mission.** Consolidate episodic memory (Mem0 + Graphiti temporal graph) into playbooks and semantic insights;
  expire and prune; enforce no-PHI-in-memory. See `agents/memory_curator.yaml`.
- **Triggers.** Nightly consolidation (03:17); `episode_batch_threshold` on `hc.ops.agent.memory.episode.v1`; manual.
- **Inputs / outputs.** Episodes and memory edges, resolved incidents, CR outcomes → consolidated playbooks and
  insights (`memory-api`), proposed playbook promotions (CR), memory-hygiene report.
- **Tools.** `memory-api` (apply), `lineage`, `ticketing` (apply), `cr-engine` (propose).
- **Autonomy.** L3, ceiling L3. Immediate demotion to L1 on any confirmed PHI leak into memory.
- **Risk classes.** `low`. **Rubrics.** RUB-MEM-01, RUB-PHI-01, RUB-EVD-01.
- **Failure modes → fallbacks.** Over-consolidation → hold output in review; memory-api down → keep expiry only.

### 18.2 `agent.self_healer`

- **Mission.** Detect platform faults and run the matching healing playbook within blast-radius policy, verify
  recovery, escalate when no playbook fits; learn new playbooks from resolved incidents. See
  `agents/self_healer.yaml` and module 14 `healing-playbooks/`.
- **Triggers.** `fault_detected` on `hc.ops.heal.incident.v1`; health scan every 5 min; manual.
- **Inputs / outputs.** Fault signals and health metrics, playbooks, connector/topic/schema/load status → healing
  actions with verification/rollback (`hc.ops.heal.action.v1`), incident records, proposed playbooks, escalations.
- **Tools.** `healing-orchestrator` (apply), `kafka-admin` (apply, masked), `schema-registry`, `dq-engine`,
  `lineage`, `ticketing`, `cr-engine` (propose).
- **Autonomy.** L3, ceiling L3. Bound by the global kill switch, per-action cooldowns and blast-radius classes.
- **Risk classes.** `low`, `medium`. **Rubrics.** RUB-HEAL-01, RUB-EVD-01, RUB-PHI-01.
- **Failure modes → fallbacks.** Action causes a secondary incident → demote to diagnose-only, page SRE; no playbook
  match → escalate with findings, never improvise a mutating action.

### 18.3 `agent.improver`

- **Mission.** Offline, evaluation-driven optimisation of agent prompts, exemplars, tool policies and thresholds;
  propose canary rollouts. Propose-only; may never alter permission matrices, risk-class gates or the privacy
  guardian. See `agents/improver.yaml`.
- **Triggers.** Weekly optimisation pass (Mon 04:33); `evaluator_drift` on `hc.ops.agent.evaluation.v1`; manual.
- **Inputs / outputs.** Traces, evaluator scores, human overrides, eval datasets, current policies → proposed
  policy changes with canary plan (CR), gated canary requests (`policy-registry`), optimisation report.
- **Tools.** `policy-registry` (propose), `memory-api` (read), `dq-engine` (read), `cr-engine` (propose),
  `git` (propose), `lineage`.
- **Autonomy.** L1, ceiling L2. Graduates only after 20 regression-free promoted rollouts.
- **Risk classes.** `medium`, `high` (changes agent behaviour). **Rubrics.** RUB-IMP-01, RUB-REG-01, RUB-PHI-01.
- **Failure modes → fallbacks.** Candidate regresses → discard and log; promoted policy causes an incident →
  auto-rollback and return to L1.

## 19. End-to-end scenario: a new Cerner encounter type

(Section numbered to match references from AGT-03; §18 is intentionally unused.)

1. **Stage 0 — discovery.** The nightly `agent.inventory` crawl sees a new row in Cerner `CODE_VALUE` for code
   set 71 (Encounter Type): code 309308, display "Observation Unit". It updates element
   `src.cerner.code_value.cs71` and emits `new_element_registered`. `agent.privacy_guardian` classifies the element
   as `internal` metadata.
2. **Stage 1 — vocabulary.** `agent.vocabulary_curator` profiles masked encounters (1,842 in 7 days, 3
   facilities), finds no covering concept in scheme `encounter-class`, confirms SNOMED CT 448951000 via `$lookup`,
   and creates the vocabulary CR shown in AGT-03 §2.2. Evaluator: 0.95, all checks pass; privacy: cleared; policy
   L2/low → `auto_approved` → `applied` (PR merged on `cr/CR-20261017-…`).
3. **Stage 2 — taxonomy.** The applied CR triggers `agent.taxonomist`, which proposes `skos:broader` →
   `clinical/encounter-class/hospital-encounter`, checks SNOMED subsumption (448951000 is-a 32485007 Hospital
   admission? no — it is-a 308335008 Patient encounter procedure) and therefore places it as a sibling of
   `inpatient` and `emergency` under `hospital-encounter` with a facet note. Medium risk (dim_encounter_class is
   bound) → steward approves next morning in the HITL console.
4. **Stage 3 — thesaurus.** `agent.thesaurus_editor` adds `skos:related` to `clinical/bed-type/observation-bed`
   (co-occurrence support 0.91 in masked G4 data) and `skos:closeMatch` to v3-ActCode `OBSENC`; low risk,
   auto-approved.
5. **Stage 4 — ontology.** After 30 stable days, `agent.ontologist` proposes no new class (encounter class is a
   value of `clinical:hasEncounterClass`) but updates the SHACL shape `EncounterShape` so `fhir:Encounter.class`
   accepts the new member and adds a shape constraining `Encounter.length ≤ 48 h` at severity `warn` for this
   class. Reasoner consistent; L1 → ontologist approves.
6. **DQ.** `agent.dq_author` derives `DQR-VALIDITY-ENCOUNTER-004` update (value set membership) and new
   `DQR-CONSISTENCY-ENCOUNTER-021` (observation stay > 48 h → warn), backtests 90 days (0.6 % breach, no
   seasonality), auto-approved.
7. **Terminology.** `agent.terminology_steward` adds the concept to ValueSet `healix-encounter-class` and the
   ConceptMap `cerner-cs71-to-healix-encounter-class`; low risk, auto-approved; canonicaliser stops emitting
   `unknown`.
8. **EDW.** `agent.schema_architect` proposes a `dim_encounter_class` member insert plus `fact_encounter` has no
   DDL change (member-only). L1 → Data Warehouse Architect approves; dry-run clean.
9. **Release.** Thursday 22:00 `agent.release_manager` assembles the CR chain into ontology release 2026.11.0 and
   terminology bindings release, runs the gate suite, publishes, emits `ontology_release`.
10. **Stage 5 — KG.** `agent.kg_builder` regenerates the FHIR→RDF mapping for `Encounter.class`, loads the next
    batches with the new IRI, and the nightly reconciliation shows KG and EDW agree on 1,902 observation-unit
    encounters. `agent.dq_monitor` watches 72 h; no regression; the chain is closed and every agent's graduation
    statistics are updated.

## 20. Requirements

> **AGT-CAT-001** (P0, inspection) — The 20 agents in this catalog MUST be exactly the roster of ARC-01 §5 with the
> same ids and initial autonomy levels; adding, removing or renaming an agent MUST be a revision of ARC-01 first.

> **AGT-CAT-002** (P0, test) — Each agent's manifest MUST declare only the CR types listed for it in the summary
> table; `cr-engine` MUST reject a CR whose `type` is not permitted for the proposer.

> **AGT-CAT-003** (P0, test) — `agent.ontologist` and `agent.schema_architect` MUST have `auto_approve_threshold:
> null` while at L1; every CR they propose MUST carry a human approval gate.

> **AGT-CAT-004** (P0, test) — `agent.evaluator` MUST NOT be able to create CRs, and `agent.privacy_guardian` MUST
> be the only agent with `admin` permission on `cr-engine`.

> **AGT-CAT-005** (P0, test) — Agents that operate without CRs (`inventory` registry writes, `kg_builder.load_batch`,
> `dq_monitor` issues, `orchestrator` routing) MUST record every such action in the decision log with evidence
> pointers.

> **AGT-CAT-006** (P1, test) — Stage-ordering MUST be enforced: a `taxonomy` CR MUST reference an applied
> `vocabulary` concept; a `thesaurus` CR an applied hierarchy placement; an `ontology` promotion a thesaurus
> relation stable for ≥ 30 days (configurable per environment).

> **AGT-CAT-007** (P1, demo) — The end-to-end scenario in §19 MUST be executable as an acceptance test in `test`
> with synthetic data, completing all ten steps with the expected gates and no human intervention other than the
> three human approvals (taxonomy medium, ontology L1, EDW L1).

> **AGT-CAT-008** (P1, inspection) — Each agent's failure modes and fallbacks listed here MUST correspond to
> `escalation` entries in its manifest; the linter MUST flag a manifest lacking `on_veto` for any agent whose
> `risk_classes` include `phi_affecting`.

> **AGT-CAT-009** (P1, analysis) — High-volume agents (`terminology_steward`, `dq_monitor`, `remediator`) MUST run
> on the `${MODEL_CLASSIFICATION}` tier by default with escalation to `${MODEL_MAPPING}` only on structured-output
> failure, to keep daily token caps within the manifests.

> **AGT-CAT-010** (P2, inspection) — Each agent MUST have a published playbook (AGT-06 §1.4) for every trigger it
> declares.
