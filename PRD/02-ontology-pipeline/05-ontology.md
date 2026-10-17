---
title: Stage 4 — Ontology (OWL 2 DL and SHACL)
module: 02-ontology-pipeline
doc_id: ONT-05
version: 0.1.0
status: draft
owners: [Lead Ontologist, Enterprise Ontology Team, Clinical Informatics]
depends_on: [ONT-04, ARC-01, IOP-FHIR-00, TKG-TIME-002]
last_updated: 2026-10-17
---

# Stage 4 — Ontology

## 1. Purpose

Stage 4 formalises the released thesaurus into an **OWL 2 DL** ontology: classes with subsumption derived from
taxonomy, object and data properties promoted from thesaurus relation categories, axioms (domain, range, cardinality,
disjointness, property characteristics) and **SHACL shapes** derived from those axioms for closed-world validation of
instances. The ontology is the semantic contract that the EDW (05), the FHIR canonical layer (06), the knowledge graph
(08) and the DQ engine (09) bind to.

Design stance: **OWL for meaning, SHACL for conformance.** Open-world reasoning (HermiT/ELK) checks that the model is
coherent; SHACL checks that data agrees with the model. Neither substitutes for the other.

## 2. Modularisation

```mermaid
flowchart TB
  CORE[healix-core.owl<br/>upper alignment · temporal hooks · provenance hooks · identifier pattern]
  G1[healix-g1-party.owl]
  G2[healix-g2-clinical.owl]
  G3[healix-g3-finance.owl]
  G4[healix-g4-supply.owl]
  G5[healix-g5-workforce.owl]
  G6[healix-g6-pophealth.owl]
  G7[healix-g7-research.owl]
  G8[healix-g8-reference.owl]
  G9[healix-g9-governance.owl]
  ALL[healix-enterprise.owl<br/>imports all]
  G1 & G2 & G3 & G4 & G5 & G6 & G7 & G8 & G9 -->|owl:imports| CORE
  G2 -->|owl:imports| G1 & G8
  G3 -->|owl:imports| G1 & G2 & G8
  G4 -->|owl:imports| G1 & G8
  G5 -->|owl:imports| G1 & G8
  G6 -->|owl:imports| G2 & G1
  G7 -->|owl:imports| G2 & G1
  G9 -->|owl:imports| G8
  ALL -->|owl:imports| G1 & G2 & G3 & G4 & G5 & G6 & G7 & G8 & G9
  EXT[External: FHIR RDF (fhir:) · SNOMED (sct:) · PROV-O · SKOS · time: · BFO (optional)]
  CORE -.->|references, no import of full SNOMED| EXT
```

| Module | IRI | Contents | Imports |
|--------|-----|----------|---------|
| Core | `<base>ontology/core` | `Entity`, `Event`, `Party`, `Agent`, `Role`, `Identifier`, `CodedValue`, `Quantity`, `TemporalAssertion` hook, `Provenance` hook; shared properties (`hasIdentifier`, `hasCode`, `validFrom/To`, `wasGeneratedBy`) | PROV-O, SKOS, OWL-Time (`time:`) by reference; `fhir:` namespace declared |
| G1–G9 | `<base>ontology/g<N>-<slug>` | Group classes/properties; one OWL file per group | Core + declared group dependencies above |
| Enterprise | `<base>ontology/enterprise` | Empty module that imports all nine; the artefact the reasoner certifies as a whole | all |

Rules: a group module MUST NOT import a module not listed above (prevents cyclic imports and keeps ELK modules small);
cross-group references are via IRIs with the dependency declared. Each module is in the **OWL 2 EL** profile where
possible (G2, G8 — large, hierarchical) so ELK can classify; core and finance may use **OWL 2 DL** features (inverse,
functional, cardinality, disjoint union) checked by HermiT.

## 3. Upper-level alignment

| Layer | Choice | Rationale |
|-------|--------|-----------|
| Upper ontology | **Optional** BFO 2.0 alignment via `healix:Entity ⊑ bfo:Entity` axioms in a separate `healix-bfo-bridge.owl`; OGMS (Ontology for General Medical Science) for `Disease`, `Disorder`, `Diagnosis` distinctions; OBO patterns (realisable entity → role) for party roles | Keeps the enterprise ontology usable without philosophical commitment, while allowing OBO-ecosystem interoperability for research (G7) |
| Clinical model | **FHIR RDF** (`http://hl7.org/fhir/`) is the structural reference: each FHIR R4 resource in scope has a corresponding `healix` class with `healix:fhirResource` annotation; HEALIX does not redefine FHIR element semantics but adds enterprise constraints and cross-resource relations | FHIR is the canonical interchange model (ADR-01); FHIR RDF gives a W3C-standard RDF rendering |
| Terminology model | **SNOMED CT concept model**: SNOMED attributes (`363698007 Finding site`, `246075003 Causative agent`, `363589002 Associated procedure`, `405815000 Procedure device`) are reused as object properties where HEALIX needs them, with `sct:` IRIs and `owl:equivalentProperty` to `healix` properties only when the meaning is identical | Avoids inventing clinical relations SNOMED already defines; SNOMED CT content is referenced by IRI, not imported (licence, size) |
| Provenance & time | PROV-O; OWL-Time for intervals; bitemporal semantics defined in module 08 (`TKG-TIME-*`) | CONVENTIONS §4 |
| Vocabulary | SKOS concept schemes from Stages 1–3 remain SKOS; class-like concepts get an OWL class with `healix:derivedFromConcept`; code-like concepts are *values* (`healix:CodedValue` individuals or `skos:Concept` referenced via `healix:hasCode`) — never classes | Punning avoided; keeps the ontology in DL |

## 4. Class and property design patterns per group

### 4.1 Shared patterns (core)

| Pattern | Shape |
|---------|-------|
| **Role pattern** | `Party` (Person or Organization) `healix:bearsRole` `Role` (`PatientRole`, `PractitionerRole`, `EmployeeRole`, `SupplierRole`, `PayerRole`). Roles are time-bounded; a Person may bear many roles. FHIR `Patient`, `Practitioner`, `RelatedPerson` map to roles of one `Person`. |
| **Identifier pattern** | `Entity healix:hasIdentifier Identifier`; `Identifier healix:identifierSystem xsd:anyURI`, `healix:identifierValue xsd:string`, `healix:identifierType` → `skos:Concept` in `identifier-type`; `healix:assigningOrganization` → `Organization`. MRN, FIN, NPI, Supplier Number, Person Number all use it. |
| **Coded value pattern** | `Entity healix:hasCode CodedValue`; `CodedValue healix:code xsd:string`, `healix:codeSystem xsd:anyURI`, `healix:codeSystemVersion`, `healix:display`, `healix:concept` → `skos:Concept`. One pattern for FHIR `Coding`, HL7 `CWE`, Oracle lookup, Cerner `CODE_VALUE`. |
| **Event pattern** | `Event ⊑ Entity`; `healix:occurredAt time:Instant`, `healix:duringInterval time:Interval`, `healix:hasSubject Entity`, `healix:performedBy Agent`, `healix:atLocation Location`, `healix:hasStatus CodedValue`. Encounter, Procedure, Observation, ChargeEvent, InventoryTransaction, GLJournalPosting are all Events. |
| **Quantity pattern** | `Quantity healix:value xsd:decimal`, `healix:unit` → UCUM `skos:Concept`; `Money ⊑ Quantity` with `healix:currency` → ISO 4217 concept. |
| **Participation pattern** | Reified `Participation` (`healix:participant`, `healix:participationType`, `healix:period`) for n-ary participation in Encounter (attending, consulting), PurchaseOrder (buyer, approver). |
| **Bitemporal hook** | No class in this ontology carries valid/transaction time properties on the class itself; temporal qualification is applied to assertions per module 08 (RDF-star). The ontology declares `healix:validFrom/validTo` and `healix:recordedAt/supersededAt` as annotation-capable datatype properties for use by that model. |

### 4.2 Group patterns

| Group | Key classes | Key object properties (domain → range) | Representative axioms |
|-------|-------------|----------------------------------------|-----------------------|
| G1 Party | `Person`, `Organization`, `Location`, `PatientRole`, `PractitionerRole`, `EmployeeRole`, `SupplierRole`, `PayerRole`, `RelatedPersonRole` | `bearsRole` (Party → Role), `roleOf` inverse, `managingOrganization` (Role → Organization), `partOf` (Location → Location, transitive), `qualifiedBy` (PractitionerRole → Credential) | `Person DisjointWith Organization`; `PatientRole ⊑ Role ⊓ ∃roleOf.Person`; `Location partOf` transitive, irreflexive |
| G2 Clinical | `Encounter`, `EpisodeOfCare`, `Condition`, `Procedure`, `Observation`, `DiagnosticReport`, `MedicationRequest`, `MedicationDispense`, `MedicationAdministration`, `AllergyIntolerance`, `Immunization`, `ServiceRequest`, `Specimen`, `ImagingStudy`, `ClinicalDocument` | `hasSubject` (ClinicalEvent → PatientRole), `partOfEncounter` (ClinicalEvent → Encounter), `basedOn` (Observation/Procedure → ServiceRequest), `isMeasurementOf` (Observation → Condition), `treats` (MedicationRequest → Condition), `hasSpecimen` (Observation → Specimen), `hasResult` (DiagnosticReport → Observation), `findingSite` ≡ `sct:363698007` (Condition → BodyStructure concept) | `Observation ⊑ ∃hasSubject.PatientRole` ; `Observation ⊑ ≤1 hasValue`; `MedicationAdministration ⊑ ∃basedOn.MedicationRequest ⊔ ∃healix:isUnorderedAdministration.{true}`; `Encounter ⊑ ∃hasClass.EncounterClass`; `hasSpecimen` functional for single-specimen observations via SHACL, not OWL |
| G3 Finance | `Account`, `ChargeItem`, `Claim`, `ClaimResponse`, `Remittance`, `PaymentReconciliation`, `Coverage`, `PriorAuthorization`, `Invoice`, `GLJournal`, `GLJournalLine`, `CostCenter`, `NaturalAccount`, `Budget`, `FixedAsset` | `postsToAccount` (ChargeItem/GLJournalLine → NaturalAccount), `chargedToCostCenter`, `billedUnder` (ChargeItem → Claim), `adjudicatedBy` (Claim → ClaimResponse), `coveredBy` (Encounter → Coverage), `payerOf` (Coverage → PayerRole), `forEncounter` (Account/Claim → Encounter), `settles` (Remittance → Claim) | `Claim ⊑ ∃forEncounter.Encounter ⊔ ∃forPatient.PatientRole`; `GLJournal ⊑ ∃hasLine.GLJournalLine`; balanced-journal constraint is SHACL (`sh:sparql`), not OWL; `Invoice DisjointWith Claim` |
| G4 Supply | `Item`, `InventoryBalance`, `InventoryTransaction`, `PurchaseRequisition`, `PurchaseOrder`, `PurchaseOrderLine`, `Receipt`, `SupplierContract`, `SupplyDelivery`, `Asset`, `Bed`, `Room`, `Schedule`, `Slot`, `Appointment` | `orders` (PurchaseOrderLine → Item), `suppliedBy` (PurchaseOrder → SupplierRole), `receivedAgainst` (Receipt → PurchaseOrderLine), `stockedAt` (InventoryBalance → Location), `usedIn` (Item → Procedure), `occupies` (Encounter → Bed), `scheduledIn` (Appointment → Slot) | `Bed ⊑ Location ⊓ ∃partOf.Room`; `PurchaseOrder ⊑ ∃suppliedBy.SupplierRole`; `occupies` inverse-functional per time slice (SHACL) |
| G5 Workforce | `Worker`, `Position`, `Assignment`, `Credential`, `Licence`, `Competency`, `Shift`, `Timecard`, `LearningRecord` | `holdsAssignment` (Worker → Assignment), `fillsPosition` (Assignment → Position), `inOrganizationUnit` (Position → Organization), `requiresCredential` (Position → Credential), `holdsCredential` (Worker → Credential), `workedShift` (Worker → Shift) | `Worker ≡ Person ⊓ ∃bearsRole.EmployeeRole`; `Position ⊑ ∃inOrganizationUnit.Organization`; `Licence ⊑ Credential` |
| G6 PopHealth | `Measure`, `MeasureReport`, `RegistryMembership`, `RiskScore`, `CareGap`, `SDOHObservation`, `Attribution`, `ProgramEnrolment` | `evaluates` (MeasureReport → Measure), `forPopulation` (MeasureReport → Cohort), `attributedTo` (PatientRole → PractitionerRole), `identifiesGap` (CareGap → Measure), `enrolledIn` | `SDOHObservation ⊑ Observation`; `RiskScore ⊑ Observation ⊓ ∃derivedFrom.RiskModel` |
| G7 Research | `ResearchStudy`, `ResearchSubject`, `Cohort`, `ResearchConsent`, `Feature`, `ModelReference` | `enrolls` (ResearchStudy → ResearchSubject), `subjectOf` (ResearchSubject → Person), `consentedBy` (ResearchSubject → ResearchConsent), `memberOf` (Person → Cohort) | `ResearchSubject ⊑ ∃consentedBy.ResearchConsent` (SHACL enforces active status) |
| G8 Reference | `CodeSystem`, `ValueSet`, `ConceptMap`, `Calendar`, `GeographicArea`, `Currency`, `Unit`, `ServiceLine`, `OrganizationHierarchy` | `includesConcept` (ValueSet → skos:Concept), `mapsFrom/mapsTo`, `locatedIn` (Location → GeographicArea) | `Unit ≡ skos:Concept ⊓ ∃skos:inScheme.{ucum-scheme}` expressed via value partition, not punning |
| G9 Governance | `SourceSystem`, `DataElement`, `LineageActivity ⊑ prov:Activity`, `DQRule`, `DQResult`, `Issue`, `ConsentDirective`, `AccessPolicy`, `AuditEvent` | `describesElement`, `evaluates` (DQResult → DQRule), `appliesTo` (DQRule → owl:Class or sh:NodeShape), `derivedFromShape` (DQRule → sh:Shape) | `DQResult ⊑ ∃evaluates.DQRule ⊓ ∃prov:wasGeneratedBy.LineageActivity` |

## 5. Axiom policy

| Axiom type | Policy |
|------------|--------|
| `rdfs:subClassOf` | From Stage 2 `healix:primaryBroader` for class-like concepts; additional `skos:broader` become subclass only after ontologist review (multiple inheritance kept minimal) |
| `owl:disjointWith` | Declared among sibling top-level classes in each group (e.g. `Person`/`Organization`; `Claim`/`Invoice`; `Condition`/`Procedure`/`Observation`); never between classes that FHIR allows to overlap |
| Domain/range | Declared on every object and data property; ranges to `skos:Concept` for coded values are expressed through `CodedValue` to stay in DL |
| Cardinality | Only `≤1` (functional) and `∃` (existential) in OWL; exact counts and `min 1` on *data* belong in SHACL to avoid open-world false negatives |
| Property characteristics | `partOf` transitive; `hasIdentifier` inverse-functional per identifier system (SHACL, not OWL — OWL IFP would violate EL); `exactMatch` symmetric/transitive inherited from SKOS |
| Equivalence to external | `owl:equivalentClass`/`equivalentProperty` to `fhir:` or `sct:` only when a steward asserts identity; otherwise `skos:exactMatch` annotation remains |
| Annotations | `rdfs:label`, `rdfs:comment` (definition), `skos:definition`, `healix:derivedFromConcept`, `healix:fhirResource`, `healix:fhirPath`, `healix:edwBinding` (table.column), `healix:group`, `dcterms:source` |

## 6. SHACL shapes derived from the ontology

`agent.ontologist` derives a SHACL shape graph per module (`<base>shapes/g<N>-<slug>`) using deterministic rules;
stewards may add shapes but may not weaken derived ones.

| OWL source | Derived SHACL |
|------------|---------------|
| `C ⊑ ∃p.D` | `sh:property [ sh:path p ; sh:minCount 1 ; sh:class D ]` on `sh:targetClass C` |
| `C ⊑ ≤1 p` | `sh:maxCount 1` |
| `p rdfs:range D` (object) | `sh:class D` (or `sh:nodeKind sh:IRI` when D is `skos:Concept` from a specific scheme, plus `sh:in` / `sh:sparql` membership check against the scheme) |
| `p rdfs:range xsd:T` | `sh:datatype xsd:T` |
| Enumerated `CodedValue` binding (`healix:bindsTo scheme`) | `sh:sparql` that checks `?code skos:inScheme <scheme>` at the record's `codeSystemVersion` |
| Identifier uniqueness (`hasIdentifier`) | `sh:sparql` uniqueness per `identifierSystem` within the named-graph snapshot |
| Business rules not expressible in OWL (journal balances, encounter end ≥ start, dispense quantity ≤ ordered) | Hand-authored `sh:sparql` or SHACL-AF rules, tagged `healix:ruleSource = steward` |
| Severity | `sh:Violation` for P0 constraints, `sh:Warning` for P1, `sh:Info` for profiling hints; module 09 maps severity to `DQR-*` severities |

Every shape carries `healix:derivedFromAxiom` (the axiom IRI/hash) so a change in OWL regenerates the shape and the
DQ rule (09) traceably. See `skeleton.shacl.ttl` for the pattern.

## 7. Reasoning

| Task | Reasoner | Scope | Gate |
|------|----------|-------|------|
| Classification, consistency, unsatisfiable classes | **ELK** | Each EL-profile module (G2, G4, G5, G6, G7, G8) and `enterprise` restricted to EL axioms | Every CR; blocker |
| Full DL consistency, inferred disjointness violations, property chains | **HermiT** | Core, G1, G3, G9, and `enterprise` nightly | Nightly and before release; blocker |
| Profile check | OWL API profile validator | Each module | Every CR; blocker if a module leaves its declared profile |
| Explanation | Reasoner explanation service | Any unsatisfiable class or inconsistency | Attached to CR for the human reviewer |
| Materialisation | ELK classification → `rdfs:subClassOf` closure exported to the KG store as an inferred graph | Enterprise | Release |

`agent.ontologist` (L1) never applies changes; every ontology CR is approved by the Lead Ontologist, and any CR touching
G2 or PHI-bearing properties is co-approved by `agent.privacy_guardian`.

## 8. Competency questions

Competency questions (CQs) are the acceptance tests of the ontology. Each CQ has a SPARQL query stored in
`cq/<id>.rq`, a fixture graph, and an expected result. A release MUST answer ≥ 90 % correctly (ONT-OWL-013).

| ID | Group | Competency question |
|----|-------|---------------------|
| CQ-01 | G2 | Which inpatient encounters (encounter class *Inpatient*) in the last 30 days had a Condition coded as SNOMED descendant of *Sepsis* (91302008) and a lactate Observation (LOINC 2524-7) > 2 mmol/L within 6 h of admission? |
| CQ-02 | G2 | For a given patient MRN, list all MedicationAdministrations not linked to a MedicationRequest (unordered administrations) by performing PractitionerRole. |
| CQ-03 | G2 | Which Observations are measurements of a Condition the patient does not have on their active problem list (`isMeasurementOf` with no matching Condition)? |
| CQ-04 | G2 | Which Cerner event codes (`CODE_SET 4002`) emitted in the last 7 days have no `exactMatch` to LOINC? |
| CQ-05 | G2/G1 | Which Practitioners performed Procedures of category *Surgical* at a Location whose `partOf*` chain does not include a licensed operating-room Location? |
| CQ-06 | G3 | For each Encounter, what is the total ChargeItem amount, the sum posted to GL natural accounts under *Patient Service Revenue*, and the variance between them? |
| CQ-07 | G3 | Which Claims (837I) were adjudicated with CARC *CO-97* and relate to Encounters whose discharge disposition is *Transfer to acute facility*? |
| CQ-08 | G3 | Which Coverages active on the encounter admission date belong to a PayerRole of class *Medicare Advantage*, and which of those Encounters lack a PriorAuthorization? |
| CQ-09 | G3/G5 | Which cost centres incurred labour cost (Timecards) for Shifts in Locations with zero occupied Beds for the whole shift? |
| CQ-10 | G4 | Which Items of category *Orthopaedic implant* were used in Procedures (via `usedIn`) but have no Receipt against any PurchaseOrderLine in the preceding 180 days (consignment or leakage)? |
| CQ-11 | G4 | Which InventoryBalances at pharmacy Locations fall below par for Items whose RxNorm ingredient appears in an active MedicationRequest in the same facility? |
| CQ-12 | G4/G2 | Which Appointments scheduled into Slots at a Room of type *Infusion chair* overlap in time for the same Room? |
| CQ-13 | G5 | Which Workers hold an Assignment to a Position that `requiresCredential` a Licence the Worker does not hold with status *Active*? |
| CQ-14 | G5/G2 | Which PractitionerRoles performed Procedures requiring NUCC specialty *Interventional Cardiology* without a matching Competency record? |
| CQ-15 | G6 | For measure CMS165 (controlling high blood pressure), which attributed patients have a CareGap and no systolic/diastolic Observation in the measurement period? |
| CQ-16 | G6/G2 | Which patients have an SDOH Observation of *Food insecurity* and an Encounter with discharge disposition *Home* but no ProgramEnrolment in a nutrition programme? |
| CQ-17 | G7 | Which ResearchSubjects of a given ResearchStudy have a ResearchConsent that was withdrawn before the latest Observation included in the study extract? |
| CQ-18 | G1 | Which Persons bear both a PatientRole and an EmployeeRole, and which of their clinical records were accessed (AuditEvent) by a Worker in the same organisation unit? |
| CQ-19 | G8/G9 | Which EDW columns (`healix:edwBinding`) are bound to a property whose range scheme changed in the last ontology release? |
| CQ-20 | G9 | Which DQRules derived from SHACL shapes have a `derivedFromAxiom` that no longer exists in the current release? |

## 9. Requirements

| ID | Priority | Verification | Requirement |
|----|----------|--------------|-------------|
| **ONT-OWL-001** | P0 | test | The `enterprise` module and every group module MUST be in OWL 2 DL; G2, G4, G5, G6, G7 and G8 MUST additionally be in OWL 2 EL, verified by the profile checker on every change request. |
| **ONT-OWL-002** | P0 | test | ELK (per module) and HermiT (`enterprise`, nightly and pre-release) MUST report consistency and zero unsatisfiable classes; any failure blocks release. |
| **ONT-OWL-003** | P0 | test | Every class MUST have `rdfs:label@en`, `skos:definition@en`, `healix:group` and, for classes derived from a concept, `healix:derivedFromConcept`. |
| **ONT-OWL-004** | P0 | test | Every object and data property MUST declare `rdfs:domain` and `rdfs:range`; coded ranges MUST go through `healix:CodedValue` (no punning of `skos:Concept` as a class). |
| **ONT-OWL-005** | P0 | test | Group modules MUST import only the modules listed in §2; the import graph MUST be acyclic. |
| **ONT-OWL-006** | P0 | test | SHACL shape graphs MUST be regenerated deterministically from the OWL axioms per §6, and every derived shape MUST carry `healix:derivedFromAxiom`. |
| **ONT-OWL-007** | P0 | test | Every class corresponding to a FHIR R4 resource in scope MUST carry `healix:fhirResource` and MUST NOT redefine the semantics of a FHIR element (checked by annotation-only use of `fhir:` IRIs). |
| **ONT-OWL-008** | P0 | inspection | Ontology change requests MUST be approved by the Lead Ontologist; changes to G2 classes/properties or to properties ranging over PHI-classified elements MUST additionally be approved by `agent.privacy_guardian`. |
| **ONT-OWL-009** | P0 | test | No class in the ontology MAY carry bitemporal properties as class-level data properties; valid/transaction time MUST be applied per module 08 assertion-level model. |
| **ONT-OWL-010** | P1 | test | SNOMED CT attribute reuse MUST use `sct:` IRIs and MAY declare `owl:equivalentProperty` only with steward sign-off; SNOMED CT content MUST NOT be imported as OWL axioms into HEALIX modules. |
| **ONT-OWL-011** | P1 | test | Every thesaurus relation category promoted to an object property MUST retain a `healix:promotedFromCategory` annotation and the Stage 3 associations MUST be re-expressed as instances of that property in the KG. |
| **ONT-OWL-012** | P1 | test | Disjointness MUST be declared among sibling top-level classes within each group as listed in §5, and the reasoner MUST confirm no instance-level violation on the CQ fixture graph. |
| **ONT-OWL-013** | P1 | test | A release MUST answer ≥ 90 % of registered competency questions correctly on the fixture graph; CQ-01..CQ-20 are mandatory in the first release. |
| **ONT-OWL-014** | P1 | test | Every class MUST carry `healix:edwBinding` annotations for the EDW tables that materialise it, maintained jointly with `agent.schema_architect` (module 05). |
| **ONT-OWL-015** | P2 | analysis | An optional BFO/OGMS bridge module SHOULD be provided for G7 research interoperability, and MUST NOT be imported by `enterprise`. |
| **ONT-OWL-016** | P2 | demo | The registry SHOULD serve reasoner explanations for any unsatisfiable class or inconsistency attached to the originating change request. |

## 10. Exit criteria

The ontology is `released` when ONT-OWL-001..009 pass, the CQ suite meets ONT-OWL-013, the SHACL shape graph
validates against SHACL-SHACL, module 05 confirms EDW bindings resolve, module 09 confirms derived rule generation,
and the Lead Ontologist has signed the release. Stage 5 instantiates only released ontology versions.
