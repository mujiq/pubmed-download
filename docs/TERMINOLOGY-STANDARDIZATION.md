# PureScore / HikmaEngine Wiki — Terminology Standardization Register

**Status:** Canonical terms **approved** (this session). Body-text rewrite **NOT yet applied** —
this register scopes the proposed follow-up pass for sign-off first.
**Date:** 2026-06-26
**Lenses evaluated:** 🩺 Clinical · 📱 Product / Mobile-UI · ⚙️ Technical

The canonical terms below are now live in the new **Glossary** page
(`tech/glossary.html`, first item in the sidebar) and feed the wiki-wide hover tooltips
(`data/glossary.json` → `assets/acronyms.js`). Each ruling is rendered in-wiki under
**Glossary → Standardization rulings**.

> Hit counts are occurrences across the wiki **source** (markdown docs, `wiki_content.py`,
> `*.body.html` fragments, `data/*.json`). They scope the eventual rewrite; they are not all
> incorrect usages (many are legitimate).

---

## Already applied this session (low-risk label renames only)

| Change | Where | Note |
|---|---|---|
| `Progressive data & degradation` → **`Data quality & graceful degradation`** | NAV label, chapter-3 landing "start", `PTITLE`, the `build_progressive_data` page title + `<h1>` + crumbs, and 3 cross-reference chips | Removes the word "progressive" from the data-quality page so it no longer collides with **Progressive profiling**. Also fixed a pre-existing double-escape bug (`&amp;amp;`) in that page's `<title>`. |
| New **Glossary** page added at top of sidebar | `NAV`, `PTITLE`, `PDESC`, builder registration, `_write_hover_data` merge | Canonical unified glossary; superset of `acronyms.json`. |

No other body text was changed.

---

## The 9 standardization rulings

### 1 · First-time setup flow → **Onboarding**
- **Lens:** 📱 Product
- **Variants found:** `onboarding` (201) · `first-run` (41) · sign-up/registration (4)
- **Ruling:** `Onboarding` = the one-time first-run experience (demographics, consent, cold-start).
  `first-run` is an informal alias. Onboarding is the first phase of **Intake**.
- **Proposed rewrite scope:** demote `first-run` to alias in prose (~6 files). Low.

### 2 · Ongoing data collection → **Intake** (product) — qualify the clinical sense
- **Lens:** 📱 Product (vs 🩺 Clinical homonym)
- **Variants / collision:** `intake` (146, 33 files) used as **both** the product capture process
  **and** clinical "dietary intake" / reservoir "inflow / intake stimulus".
- **Ruling:** bare **Intake** = the product data-collection umbrella (Onboarding + Progressive
  profiling + Check-ins). Clinical uses must be qualified: **dietary intake**, **reservoir inflow** —
  never bare "intake".
- **Proposed rewrite scope:** audit the ~33 files; qualify clinical occurrences (est. ~20 hits in
  Doc 02/04 marker & reservoir tables). Medium.

### 3 · Collecting more user attributes over time → **Progressive profiling**
- **Lens:** 📱 Product
- **Variants / collision:** `progressive profiling` (12) vs `progressive data` (14, = the
  data-quality/degradation chapter) — shared adjective caused confusion.
- **Ruling:** keep **Progressive profiling** for attribute collection; the degradation chapter is
  renamed **Data quality & graceful degradation** (✅ applied).
- **Proposed rewrite scope:** done at label level; verify no prose still calls profiling
  "progressive data". Low.

### 4 · System↔user interaction → **Nudge** (outbound) / **Check-in** (inbound)
- **Lens:** 📱 Product
- **Variants found:** `nudge` (609) · `check-in` (72) · micro-action/daily-action (3)
- **Ruling:** **Nudge** = system→user prompt suggesting a behaviour/action. **Check-in** =
  system→user prompt *requesting* data (mood/stress/adherence). An **adherence check-in** confirms a
  prescribed action was done.
- **Proposed rewrite scope:** add the definitions to Doc 11; reclassify any "nudge" that is really a
  data request as a "check-in" (est. <15 hits). Low–Medium.

### 5 · Care construct → **Care pathway** (protocol) / **Care plan** (instance)
- **Lens:** 🩺 Clinical · 📱 Product
- **Variants found:** `care pathway` (82) · `care plan` (23) · `pathway` (156, broader)
- **Ruling:** **Care pathway** = standardized condition-level clinical PROTOCOL/template (IEAT).
  **Care plan** = the PERSONALIZED instance generated for one user from a pathway. A plan is an
  instance of a pathway.
- **Proposed rewrite scope:** ensure each usage means the right level; ~10 files mix them. Medium.

### 6 · Desired aim → **Goal** (user aspiration) / **Target** (technical setpoint)
- **Lens:** 📱 Product / ⚙️ Technical
- **Variants found:** `goal` (381) · `target` (220) · objective (33) · aspiration (3)
- **Ruling:** **Goal** = user-stated aspiration driving `m_k^goal`. **Target** = technical/clinical
  setpoint or threshold (`B^target`, calibration target, validation target population). Never use
  "target" for a user goal.
- **Proposed rewrite scope:** mostly already disjoint; flag the few "target" = user-goal misuses. Low.

### 7 · Questionnaire family → **Instrument** (set) · **screener** (subtype) · **PRO** (channel)
- **Lens:** 🩺 Clinical · ⚙️ Technical
- **Variants found:** `instrument` (164) · `PRO`/`PROs` (228) · `self-report` (178) ·
  `assessment` (109) · `survey` (36) · `questionnaire` (34) · `screener` (24)
- **Ruling:** **Instrument** = any validated question set (absorbs questionnaire/survey).
  **Screener** = defined subtype (short triage instrument, e.g. PHQ-2). **PRO** = the input
  channel/modality (absorbs "self-report"). **Assessment** is reserved for a clinical
  risk-assessment *process* — never a questionnaire.
- **Proposed rewrite scope:** largest sprawl. Replace `survey`/`self-report` with the canonical
  pair across Doc 02/07/08 + appendices; leave `assessment` only where it means a process. High.

### 8 · The 12 health systems → **Pillar** (domain = evidence-ID token only)
- **Lens:** ⚙️ Technical / 📱 Product
- **Variants found:** `pillar` (4307) · `domain` (782, = pillar in evidence registry & clinical
  scores) · category (759, mostly *not* pillar) · axis (134)
- **Ruling:** **Pillar** is canonical in all prose. **Domain** permitted ONLY as the frozen
  `EVD-<DOMAIN>-…` ID token (renaming would break stable keys).
- **Proposed rewrite scope:** replace prose "domain"→"pillar" in Doc 10/15 (est. ~30 hits); leave ID
  tokens. Medium.

### 9 · A measured/scored quantity → **Marker**
- **Lens:** 🩺 Clinical · ⚙️ Technical
- **Variants found:** `marker` (4458) · `biomarker` (106) · `metric` (214) · `signal` (113)
- **Ruling:** **Marker** = any scored input (lab/wearable/PRO). `biomarker` is an alias. **Metric**
  reserved for a *derived/computed* value (e.g. HRV from raw beats). `signal` is informal — avoid in
  spec prose.
- **Proposed rewrite scope:** `biomarker`→`marker` is mechanical (~106 hits, but note the page
  `appendix-biomarkers.html` filename/brand may stay). Low–Medium.

---

## Recommended follow-up sequence (when you approve the rewrite)
1. **Low-risk first:** #1, #3 (done), #6, #9 — mostly aliasing.
2. **Medium:** #2 (qualify clinical "intake"), #4, #5, #8.
3. **High (do last, with review):** #7 questionnaire family — touches the most surface.

Each pass should edit **source only** (`purescore/*.md`, `wiki_content.py`, `*.body.html`,
`data/*.json`) then `python3 build_wiki.py` — never the generated HTML. Re-run the build guards
after each pass.
