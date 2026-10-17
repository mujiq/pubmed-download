---
title: Code Systems and Value-Set Sources Reference
module: 99-appendices
doc_id: APX-02
version: 0.1.0
status: draft
owners: [Terminology Steward, Interoperability, Data Governance]
depends_on: [ARC-01, VIS-00]
last_updated: 2026-10-17
---

# Code Systems and Value-Set Sources Reference

This appendix lists the code systems, identifier systems and value-set sources the platform binds to, with the
**canonical system URI** used in FHIR `Coding.system` / `Identifier.system`, in the Terminology Service and in the
knowledge graph. The Terminology Service (module 06) is the runtime authority; this table is the human-readable index
and is linted against the `CodeSystem` resources it serves.

Conventions:

- **URI** is the FHIR-recognised system URI where one exists (per HL7 Terminology, THO). Where HL7 has not assigned a
  URI, the steward's URI or an OID URN is used and flagged.
- **Version** is always recorded on every `Coding` (`Coding.version`) and on every KG assertion; never store a code
  without its system version (RMP-ACC-063).
- **Groups** are the G1–G9 subject-area groups from `01-architecture-overview.md` §4 that use the system.
- **Licence**: *Open* = freely usable with attribution/registration; *Member/affiliate* = requires national membership or
  affiliate licence; *Commercial* = paid licence required; *Local* = enterprise-owned.

## 1. Clinical terminologies

| Code system | Canonical URI | Steward | Licence | Release cadence | Groups | Notes |
|-------------|---------------|---------|---------|-----------------|--------|-------|
| SNOMED CT (International) | `http://snomed.info/sct` | SNOMED International | Member/affiliate (US is a member; use governed by NLM UMLS licence) | International: monthly; editions per member | G2, G6, G7 | Edition/version expressed as `http://snomed.info/sct/<moduleId>/version/<YYYYMMDD>`; US edition module `731000124108` |
| SNOMED CT US Edition | `http://snomed.info/sct` with version URI `http://snomed.info/sct/731000124108/version/<YYYYMMDD>` | NLM (for US) | Member | March and September | G2, G6 | Default edition for HEALIX (OQ-09) |
| LOINC | `http://loinc.org` | Regenstrief Institute | Open (licence acceptance) | Twice yearly (plus pre-release) | G2, G6, G7 | Includes LOINC Answer lists (LA codes) and LOINC Parts; parts drive lab taxonomies |
| RxNorm | `http://www.nlm.nih.gov/research/umls/rxnorm` | NLM | Open (UMLS licence acceptance) | Monthly full; weekly updates | G2, G4 | SCD/SBD/GPCK level for medication requests; ingredient for allergies |
| NDC | `http://hl7.org/fhir/sid/ndc` | FDA | Open | Daily directory updates | G2, G4 | 10-digit source formats normalised to 11-digit 5-4-2 for storage; keep original |
| ICD-10-CM | `http://hl7.org/fhir/sid/icd-10-cm` | CDC/NCHS (with CMS) | Open | Annual (Oct 1) plus April updates | G2, G3, G6 | Diagnosis codes; include header/billable flag |
| ICD-10-PCS | `http://www.cms.gov/Medicare/Coding/ICD10` | CMS | Open | Annual (Oct 1) plus April updates | G2, G3 | Inpatient procedures |
| ICD-10 (WHO) | `http://hl7.org/fhir/sid/icd-10` | WHO | Open | Periodic | G7 | Only for international research mapping |
| ICD-9-CM | `http://hl7.org/fhir/sid/icd-9-cm` | CDC/CMS | Open | Frozen | G2, G3 | Historical data only |
| CPT | `http://www.ama-assn.org/go/cpt` | American Medical Association | Commercial (AMA licence) | Annual (Jan 1) plus quarterly Category III/PLA | G3, G2 | Descriptions access-controlled; not redistributed outbound without partner licence (RMP-RISK-003) |
| HCPCS Level II | `https://www.cms.gov/Medicare/Coding/HCPCSReleaseCodeSets` | CMS | Open | Quarterly | G3, G4 | Supplies, drugs (J-codes), DME |
| CVX | `http://hl7.org/fhir/sid/cvx` | CDC | Open | As needed (frequent) | G2, G6 | Vaccines administered |
| MVX | `http://terminology.hl7.org/CodeSystem/MVX` | CDC | Open | As needed | G2 | Vaccine manufacturers |
| UCUM | `http://unitsofmeasure.org` | Regenstrief / UCUM Organization | Open | Infrequent | G2, G4 | Units for all quantities; case-sensitive codes |
| UNII | `http://fdasis.nlm.nih.gov` | FDA / NLM | Open | Regular | G2 | Substances (allergies) |
| CDC Race & Ethnicity | `urn:oid:2.16.840.1.113883.6.238` | CDC | Open | Infrequent | G1 | US Core race/ethnicity extensions |
| DICOM Controlled Terminology | `http://dicom.nema.org/resources/ontology/DCM` | NEMA/DICOM | Open | Per DICOM edition | G2 | Imaging study metadata |
| NCI Thesaurus | `http://ncicb.nci.nih.gov/xml/owl/EVS/Thesaurus.owl` | NCI | Open | Monthly | G7 | Research/oncology concepts |
| MedDRA | `http://terminology.hl7.org/CodeSystem/mdr` | ICH / MSSO | Commercial (subscription) | Twice yearly | G7 | Only if pharmacovigilance scope is added |
| ATC | `http://www.whocc.no/atc` | WHO Collaborating Centre | Open (registration) | Annual | G4, G7 | Drug classification for supply and research |
| Gravity SDOH value sets | Published in Gravity SDOH Clinical Care IG, codes from SNOMED CT/LOINC/ICD-10-CM | HL7 Gravity Project | Open | Per IG release | G6 | Value-set source rather than code system |

## 2. Identifier systems

| Identifier | Canonical URI | Steward | Licence | Cadence | Groups | Notes |
|------------|---------------|---------|---------|---------|--------|-------|
| NPI | `http://hl7.org/fhir/sid/us-npi` | CMS (NPPES) | Open | Monthly NPPES dissemination | G1, G3, G5 | Practitioner and Organization |
| US SSN | `http://hl7.org/fhir/sid/us-ssn` | SSA | — | — | G1 | PHI; stored only where legally required, masked elsewhere |
| US EIN / TIN | `urn:oid:2.16.840.1.113883.4.4` | IRS | — | — | G1, G3, G4 | Organization and supplier tax ids |
| CLIA | `urn:oid:2.16.840.1.113883.4.7` | CMS | Open | — | G1, G2 | Laboratory identifiers |
| DEA number | `urn:oid:2.16.840.1.113883.4.814` | DEA | — | — | G5 | Prescriber credential; confidential |
| Medicare Beneficiary Identifier (MBI) | `http://hl7.org/fhir/sid/us-mbi` | CMS | — | — | G3 | Coverage subscriber id |
| FDA UDI | `http://hl7.org/fhir/NamingSystem/fda-udi` | FDA | Open | — | G4, G2 | Device identifiers (GUDID) |
| GTIN | `https://www.gs1.org/gtin` | GS1 | Membership for assignment | — | G4 | Item barcodes |
| MRN (per facility) | `urn:healix:cerner:mrn:<facility-code>` | HIM | Local | — | G1 | Facility-scoped; crosswalk to enterprise person id |
| FIN (encounter account) | `urn:healix:cerner:fin:<facility-code>` | Patient Accounting | Local | — | G2, G3 | Encounter business key |
| Enterprise person id | `urn:healix:person-id` | Platform (EMPI) | Local | — | G1 | ULID minted by entity resolution (OQ-05) |
| Enterprise encounter id | `urn:healix:encounter-id` | Platform | Local | — | G2 | Stable across facility merges |
| Oracle Fusion person number | `urn:healix:fusion:person-number` | HR | Local | — | G1, G5 | Worker identifier |
| Oracle Fusion supplier number | `urn:healix:fusion:supplier-number` | Procurement | Local | — | G1, G4 | Supplier identifier |
| Oracle Fusion item number | `urn:healix:fusion:item-number` | Supply chain | Local | — | G4 | Item master |
| Payer member id | `urn:healix:payer:<payer-code>:member-id` | Payer | Local | — | G1, G3 | Coverage.subscriberId system per payer |

## 3. HL7 terminology (THO) — v2 tables and v3 code systems

All HL7 v2 tables are served as FHIR CodeSystems at `http://terminology.hl7.org/CodeSystem/v2-<table>`; HL7 v3 code
systems at `http://terminology.hl7.org/CodeSystem/v3-<Name>`. HL7 Terminology (THO) is open and released several times
per year.

| Table / code system | Canonical URI | Meaning | HL7 v2 field | FHIR target | Groups |
|---------------------|---------------|---------|--------------|-------------|--------|
| v2-0001 | `http://terminology.hl7.org/CodeSystem/v2-0001` | Administrative sex | PID-8 | `Patient.gender` via ConceptMap to `http://hl7.org/fhir/administrative-gender` | G1 |
| v2-0003 | `http://terminology.hl7.org/CodeSystem/v2-0003` | Event type (A01, A03, R01...) | MSH-9.2 | Routing; `hc-event-type` header | all |
| v2-0004 | `http://terminology.hl7.org/CodeSystem/v2-0004` | Patient class | PV1-2 | `Encounter.class` (map to `v3-ActCode` I/O/EMER/OBSENC) | G2 |
| v2-0007 | `http://terminology.hl7.org/CodeSystem/v2-0007` | Admission type | PV1-4 | `Encounter.type` / `Encounter.priority` | G2 |
| v2-0023 | `http://terminology.hl7.org/CodeSystem/v2-0023` | Admit source | PV1-14 | `Encounter.hospitalization.admitSource` (enterprise value set) | G2 |
| v2-0112 | `http://terminology.hl7.org/CodeSystem/v2-0112` | Discharge disposition | PV1-36 | `Encounter.hospitalization.dischargeDisposition` | G2 |
| v2-0063 | `http://terminology.hl7.org/CodeSystem/v2-0063` | Relationship | NK1-3, IN1-17 | `RelatedPerson.relationship`, `Coverage.relationship` | G1, G3 |
| v2-0070 | `http://terminology.hl7.org/CodeSystem/v2-0070` | Specimen source | OBR-15, SPM-4 | `Specimen.type` (map to SNOMED) | G2 |
| v2-0074 | `http://terminology.hl7.org/CodeSystem/v2-0074` | Diagnostic service section | OBR-24 | `DiagnosticReport.category` | G2 |
| v2-0076 | `http://terminology.hl7.org/CodeSystem/v2-0076` | Message type | MSH-9.1 | Routing | all |
| v2-0078 | `http://terminology.hl7.org/CodeSystem/v2-0078` | Abnormal flags | OBX-8 | `Observation.interpretation` (map to `v3-ObservationInterpretation`) | G2 |
| v2-0085 | `http://terminology.hl7.org/CodeSystem/v2-0085` | Observation result status | OBX-11 | `Observation.status` | G2 |
| v2-0119 | `http://terminology.hl7.org/CodeSystem/v2-0119` | Order control | ORC-1 | `ServiceRequest.status`/intent logic | G2 |
| v2-0123 | `http://terminology.hl7.org/CodeSystem/v2-0123` | Result status | OBR-25 | `DiagnosticReport.status` | G2 |
| v2-0203 | `http://terminology.hl7.org/CodeSystem/v2-0203` | Identifier type (MR, AN, NPI, SS...) | PID-3.5, PV1-19.5 | `Identifier.type` | G1, G2 |
| v2-0276 | `http://terminology.hl7.org/CodeSystem/v2-0276` | Appointment reason | SCH-7 | `Appointment.reasonCode` | G4 |
| v2-0277 | `http://terminology.hl7.org/CodeSystem/v2-0277` | Appointment type | SCH-8 | `Appointment.appointmentType` | G4 |
| v3-ActCode | `http://terminology.hl7.org/CodeSystem/v3-ActCode` | Act codes incl. encounter class (AMB, EMER, IMP, OBSENC) | — | `Encounter.class` | G2 |
| v3-ActPriority | `http://terminology.hl7.org/CodeSystem/v3-ActPriority` | Priority | — | `Encounter.priority` | G2 |
| v3-RoleCode | `http://terminology.hl7.org/CodeSystem/v3-RoleCode` | Role codes (relationship, location types) | — | `RelatedPerson.relationship`, `Location.type` | G1, G4 |
| v3-MaritalStatus | `http://terminology.hl7.org/CodeSystem/v3-MaritalStatus` | Marital status | PID-16 | `Patient.maritalStatus` | G1 |
| v3-NullFlavor | `http://terminology.hl7.org/CodeSystem/v3-NullFlavor` | Reasons for missing values | — | `data-absent-reason` extensions | all |
| v3-ObservationInterpretation | `http://terminology.hl7.org/CodeSystem/v3-ObservationInterpretation` | Interpretation (H, L, A...) | OBX-8 | `Observation.interpretation` | G2 |
| Observation category | `http://terminology.hl7.org/CodeSystem/observation-category` | laboratory, vital-signs, social-history... | — | `Observation.category` | G2, G6 |
| Condition clinical / verification status | `http://terminology.hl7.org/CodeSystem/condition-clinical`, `.../condition-ver-status` | Condition status | — | `Condition.clinicalStatus/verificationStatus` | G2 |
| Condition category | `http://terminology.hl7.org/CodeSystem/condition-category` (+ US Core `condition-category`) | problem-list-item, encounter-diagnosis | — | `Condition.category` | G2 |
| Administrative gender | `http://hl7.org/fhir/administrative-gender` | FHIR gender codes | — | `Patient.gender` | G1 |
| Identifier use / Name use / Address use | `http://hl7.org/fhir/identifier-use`, `name-use`, `address-use` | FHIR structural codes | — | datatypes | G1 |

## 4. Administrative, billing and EDI code lists

| Code list | Canonical URI | Steward | Licence | Cadence | Groups | Notes |
|-----------|---------------|---------|---------|---------|--------|-------|
| Claim Adjustment Reason Codes (CARC) | `https://x12.org/codes/claim-adjustment-reason-codes` | X12 | Open to view; X12 licence for distribution | Three times yearly | G3 | Used in 835 CAS segments; URI per CARIN BB convention |
| Remittance Advice Remark Codes (RARC) | `https://x12.org/codes/remittance-advice-remark-codes` | X12 (CMS maintains) | Open to view | Three times yearly | G3 | 835 MIA/MOA/LQ |
| Claim Status Category / Claim Status Codes | `https://x12.org/codes/claim-status-category-codes`, `https://x12.org/codes/health-care-claim-status-codes` | X12 | Open to view | Three times yearly | G3 | 277 transactions |
| Service Type Codes | `https://x12.org/codes/service-type-codes` | X12 | Open to view | Three times yearly | G3 | 270/271 EB03 |
| Provider Taxonomy (NUCC) | `http://nucc.org/provider-taxonomy` | NUCC | Open | Twice yearly (Jan, Jul) | G1, G5 | `PractitionerRole.specialty` |
| Place of Service | `https://www.cms.gov/Medicare/Coding/place-of-service-codes/Place_of_Service_Code_Set` | CMS | Open | As needed | G3 | 837P CLM05-1 |
| NUBC Revenue Codes | `https://www.nubc.org/CodeSystem/RevenueCodes` | NUBC (AHA) | Commercial (NUBC manual subscription) | Annual | G3 | 837I SV2; URI per CARIN BB |
| NUBC Type of Bill | `https://www.nubc.org/CodeSystem/TypeOfBill` | NUBC | Commercial | Annual | G3 | CLM05 |
| NUBC Point of Origin | `https://www.nubc.org/CodeSystem/PointOfOrigin` | NUBC | Commercial | Annual | G2, G3 | Admit source on institutional claims |
| NUBC Priority (Type) of Admission or Visit | `https://www.nubc.org/CodeSystem/PriorityTypeOfAdmitOrVisit` | NUBC | Commercial | Annual | G2, G3 | CL1-01 |
| NUBC Patient Discharge Status | `https://www.nubc.org/CodeSystem/PatDischargeStatus` | NUBC | Commercial | Annual | G2, G3 | CL1-03 |
| MS-DRG | `https://www.cms.gov/Medicare/Medicare-Fee-for-Service-Payment/AcuteInpatientPPS/MS-DRG-Classifications-and-Software` | CMS | Open (grouper software licensed separately) | Annual (Oct 1) | G3 | Record DRG version with each claim |
| APR-DRG | Steward URI (3M/Solventum) — record as `urn:healix:ext:apr-drg` until a public URI is confirmed | 3M/Solventum | Commercial | Annual | G3 | Only where the organisation licenses the grouper |
| Claim type | `http://terminology.hl7.org/CodeSystem/claim-type` | HL7 | Open | THO | G3 | institutional, professional, pharmacy |
| Adjudication codes | `http://terminology.hl7.org/CodeSystem/adjudication` | HL7 | Open | THO | G3 | ClaimResponse.item.adjudication |
| Payment type | `http://terminology.hl7.org/CodeSystem/ex-paymenttype` | HL7 | Open | THO | G3 | PaymentReconciliation |
| Coverage class / type | `http://terminology.hl7.org/CodeSystem/coverage-class`, `http://terminology.hl7.org/CodeSystem/v3-ActCode` (coverage types) | HL7 | Open | THO | G3 | Coverage.class, Coverage.type |
| Payer identifiers (CMS/NAIC) | `urn:healix:payer-id` mapping to CMS Plan ID or NAIC code | CMS / NAIC | Open / membership | — | G1, G3 | Payer registry conformed dimension |
| UNSPSC | `https://www.unspsc.org/` | GS1 US (on behalf of UN) | Membership for full set | Periodic | G4 | Item category taxonomy seed |
| NAICS | `https://www.census.gov/naics/` | US Census Bureau | Open | Every 5 years | G1, G4 | Supplier industry classification |
| Chart of accounts (enterprise) | `urn:healix:fusion:coa:<segment>` | Finance | Local | Per fiscal change | G3 | Natural account, cost centre, company segments as SKOS schemes |
| Cost centre hierarchy | `urn:healix:fusion:coa:cost-center` | Finance | Local | Per fiscal change | G3, G5 | Conformed dimension Cost Center |
| Service line | `urn:healix:service-line` | Strategy / Analytics | Local | Annual | G2, G3, G8 | G8 reference; enterprise taxonomy |

## 5. Reference and general-purpose systems (G8)

| System | Canonical URI | Steward | Licence | Cadence | Groups | Notes |
|--------|---------------|---------|---------|---------|--------|-------|
| ISO 3166-1 country codes | `urn:iso:std:iso:3166` | ISO | Open to use; standard text is commercial | As needed | G1, G8 | alpha-2 stored |
| ISO 3166-2 subdivisions | `urn:iso:std:iso:3166:-2` | ISO | As above | As needed | G1, G8 | US states |
| ISO 4217 currency codes | `urn:iso:std:iso:4217` | ISO | As above | As needed | G3, G8 | Money.currency |
| ISO 8601 date/time | — (datatype, not a code system) | ISO | — | — | all | All timestamps UTC with offset |
| BCP 47 language tags | `urn:ietf:bcp:47` | IETF | Open | — | G1 | Patient.communication.language |
| MIME types | `urn:ietf:bcp:13` | IETF / IANA | Open | — | G2 | Attachment.contentType |
| IANA time zones | `https://www.iana.org/time-zones` | IANA | Open | Several per year | G8 | Facility time zone |
| USPS state/ZIP | `https://www.usps.com/` | USPS | Open (licensed products for full database) | Monthly | G1, G8 | Geography dimension |
| US Census geography (FIPS/GEOID) | `https://www.census.gov/` | US Census Bureau | Open | Decennial / annual | G6, G8 | SDOH geography |
| Enterprise calendar | `urn:healix:calendar` | Finance / Analytics | Local | Annual | G8 | Fiscal periods, holidays |
| Organisation hierarchy | `urn:healix:org-hierarchy` | Enterprise Architecture | Local | Quarterly | G1, G8 | Conformed dimension Organization |

## 6. Local (enterprise) code systems

Local systems preserve source semantics; every local code is mapped (where possible) to an enterprise value set and an
international code via `ConceptMap`. Local codes are never overwritten (RMP-RISK-015).

| Pattern | Meaning | Example | Steward | Groups |
|---------|---------|---------|---------|--------|
| `urn:healix:cerner:code-set:<n>` | Cerner Millennium `CODE_SET` number `<n>`; codes are `CODE_VALUE` ids with display and definition | `urn:healix:cerner:code-set:71` (encounter type) | Clinical informatics | G1, G2, G3, G4 |
| `urn:healix:cerner:event-code` | Cerner clinical event codes (`CLINICAL_EVENT.EVENT_CD`) | — | Clinical informatics | G2 |
| `urn:healix:cerner:order-catalog` | Cerner orderable catalog codes | — | Clinical informatics | G2 |
| `urn:healix:cerner:charge-code` | Cerner charge/CDM codes | — | Revenue integrity | G3 |
| `urn:healix:fusion:lookup:<type>` | Oracle Fusion `FND_LOOKUP` type `<type>` | `urn:healix:fusion:lookup:PO_STATUS` | Fusion functional lead | G3, G4, G5 |
| `urn:healix:fusion:flex:<segment>` | Oracle Fusion flexfield value set | `urn:healix:fusion:flex:COST_CENTER` | Finance | G3 |
| `urn:healix:fusion:hcm:<object>` | Oracle Fusion HCM reference objects (job, grade, position type) | `urn:healix:fusion:hcm:job-code` | HR | G5 |
| `urn:healix:lis:<vendor>:test-code` | LIS local test codes | — | Laboratory | G2 |
| `urn:healix:ris:<vendor>:procedure-code` | RIS local procedure codes | — | Radiology | G2 |
| `urn:healix:pharmacy:<vendor>:item` | Pharmacy formulary items | — | Pharmacy | G2, G4 |
| `urn:healix:payer:<payer-code>:<list>` | Payer-specific code lists (plan codes, product codes) | — | Managed care | G3 |
| `urn:healix:dq:dimension` | DQ dimensions (completeness, validity...) | — | Data governance | G9 |
| `urn:healix:phi-class` | PHI classification levels | — | Privacy | G9 |
| `urn:healix:change-class` | Agent change-request classes | — | Agent operator | G9 |
| `https://ontology.healix.example.org/scheme/<slug>` | Enterprise SKOS concept schemes (the governed vocabularies themselves) | `.../scheme/encounter-class` | Head of Semantics | all |

## 7. Value-set sources

| Source | Location | Licence | Used for |
|--------|----------|---------|----------|
| HL7 Terminology (THO) | `https://terminology.hl7.org/` | Open | HL7 v2/v3/FHIR code systems and value sets |
| FHIR R4 core value sets | `http://hl7.org/fhir/ValueSet/<id>` | Open | Structural bindings |
| US Core value sets | `http://hl7.org/fhir/us/core/ValueSet/<id>` | Open | US realm bindings |
| NLM Value Set Authority Center (VSAC) | `https://vsac.nlm.nih.gov/` (`urn:oid:...` value set OIDs) | UMLS licence | eCQM value sets for G6 |
| CARIN BB value sets | `http://hl7.org/fhir/us/carin-bb/ValueSet/<id>` | Open | Claims mapping |
| Da Vinci value sets | `http://hl7.org/fhir/us/davinci-<ig>/ValueSet/<id>` | Open | Prior authorization, payer exchange |
| Gravity SDOH value sets | `http://hl7.org/fhir/us/sdoh-clinicalcare/ValueSet/<id>` | Open | SDOH |
| Enterprise value sets | `https://ontology.healix.example.org/valueset/<slug>` | Local | Governed expansions bound in HEALIX profiles |

## 8. Licensing summary

| Licence class | Systems | Obligations for HEALIX |
|---------------|---------|------------------------|
| Member/affiliate | SNOMED CT | Use within member territory under NLM/UMLS terms; affiliate licence for non-member deployments; no redistribution to partners outside licence |
| Commercial | CPT, NUBC code sets, APR-DRG, MedDRA | Procure and track seats/scope; restrict description text to licensed users; outbound only to licensed partners; CPT descriptions excluded from public IG |
| Open with registration/acceptance | LOINC, RxNorm (UMLS), UCUM, VSAC | Accept terms; attribute; respect UMLS annual re-acceptance |
| Open | ICD-10-CM/PCS, HCPCS, CVX, NPI/NPPES, NUCC, THO, FHIR, X12 code lists (view) | Attribute where required; X12 full code-list distribution needs X12 licence |
| Local | `urn:healix:*`, enterprise schemes | Governed by module 02 change process; PHI never embedded in codes or labels |

## 9. Release-tracking requirements (summary)

- `agent.terminology_steward` runs impact analysis within 5 business days of each release of SNOMED CT US, LOINC,
  RxNorm, ICD-10-CM/PCS, CPT/HCPCS, CVX and NUCC (RMP-RISK-005).
- Every `CodeSystem` served by the Terminology Service carries `version`, `date`, `publisher`, `copyright` and a
  licence tag in its metadata.
- Local code systems are versioned by snapshot date (`urn:healix:...|<YYYY-MM-DD>` in `Coding.version`).
