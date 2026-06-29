# Product Configuration & Cohort Simulator — Design

**Status:** Approved design (screen choices locked 2026-06-29). Spec for build; not yet implemented.
**Goal:** make the scoring system **configurable at runtime** — pillars, their markers / wearable
metrics / question responses, the weights, and the reservoir wiring — **without a code change or
rebuild**, and let an editor **simulate the impact across a cohort before publishing**.

> **Why.** Markers, wearable metrics and questions change over time. Today the engine is
> JSON-canonical (`calc-graph.json` + `pillar-weights.json` + `constants.json` → compiled to
> `calc-data.js`, guarded so weights sum to 1) but those files are **hand-edited and require a
> rebuild**. This design keeps that exact mental model but moves the source of truth into a
> **versioned config store** with a **publish → compile → cached snapshot** path, so non-engineers
> can change the system safely and see the consequences first.

## At a glance — the chosen designs

| Screen | Chosen design | One line |
|---|---|---|
| **1 · Config editor** | **100-point allocation sliders** | Weights are a budget that always totals 100%; moving one redistributes the rest (locked items held). |
| **2 · Cohort simulator** | **Before/After distribution diff** | Shadow-compute the candidate config over the 3,626-cell cohort; show score distribution before vs after, Δmean, band-flips, top movers. |
| **3 · Reservoir config** | **Connection matrix** | Adjacency grids for inflow gains, reservoir→pillar feeds, and κ couplings — one editable cell per connection. |
| **4 · Storage & caching** | **Versioned snapshot + in-memory cache** | Publish compiles an immutable, versioned snapshot into each instance's RAM; scoring reads RAM only — **0 DB queries per score**. |

---

## Screen 1 · Pillar & input config — 100-point allocation sliders

**▶ [Open the interactive prototype →](product-config-editor.html)** — live allocation sliders over the real pillar/marker data, with Σ=100% redistribution.

Each pillar owns a set of **inputs** (a unifying term for a **marker**, a **wearable metric**, or a
**question response**). Two budgets, each always summing to 100%:

- **Pillar budget** — the 12 pillar base weights `Wₖ` across the whole score.
- **Within-pillar budget** — the input weights inside one pillar.

```
CV · within-pillar budget = 100%                pillar budget = 100%
ApoB           ########..  18%  (lock)          CV   ##### 13
Systolic BP    #######...  15%                  MET  ##### 12
LDL-C          ######....  12%                  SLP  ####  10
HDL-C          ###.......   6%                  FIT  ####  10
+ Add input  (marker · wearable · question)     …          Σ 100 ✓
move one slider  ->  unlocked rest redistribute
```

**Redistribution rule (keeps Σ = 1 by construction).** When an editor sets input *i* to `wᵢ′`:

```
locked sum   L = Σ weights of locked inputs
remaining    R = 1 − wᵢ′ − L                  (reject/cap the edit if R < 0)
old free sum S = Σ weights of the other unlocked inputs (excl. i, excl. locked)
for each other unlocked input j:   wⱼ′ = wⱼ × R / S      (equal split if S = 0)
```

Adding an input gives it a small default (from its tier) and rescales; removing one frees its
weight back into the pool. The same rule drives the pillar budget. The UI shows a live **Σ = 100% ✓**
and blocks publish if any budget is off (the runtime form of today's `_engine_guard`).

**Edits supported:** add/remove an input, move it between pillars, set its weight, set
two-sidedness, safety-critical flag, green/amber/red bands, and source. Questions and wearables
bind the same way as labs (they are all "inputs").

---

## Screen 2 · Cohort impact simulator — Before/After distribution diff

**▶ [Open the interactive prototype →](product-config-simulator.html)** — adjust candidate weights and watch the before/after PureScore distribution over a 2,000-member cohort.

Before publishing, the editor runs the **candidate config** against the cached cohort and sees the
delta. The cohort is the existing **ClickHouse cohort×marker matrix** (`cohort-percentiles`,
**3,626 cells** = age-band × sex × life-stage × ethnicity × marker → p5/p25/p50/p75/p95/n). A
representative member sample is drawn from the per-cell distributions (or cell medians weighted by
`n`).

```
Cohort N = 3,626              filter: [all] [♀] [45–54] [Emirati] …
PureScore distribution
  before   .:i|I|i:.      mean 71.2   median 73
  after    .:il|Ii:.      mean 69.8   median 71     Δ −1.4
band flips:  4.1% green→amber · 0.3% →red · 0.9% amber→green
top movers (by score Δ):  REN +6.1 · HEP −4.0 · MET −1.8
per-pillar mean contribution Δ:  [CV −0.2][MET −1.8][REN +6.1]…
```

**Metrics shown:** before/after histograms, Δmean & Δmedian, KS distance, **% of members whose band
flips** (the number that matters most), per-pillar mean-contribution Δ, and the **top movers**
(inputs whose weight change drives the biggest shifts). Runs are **shadow** — nothing is written
until publish. Optional **guardrail overlay** (e.g. "≤5% flip to red", "no safety-critical pillar
left uncovered") flags a risky change before sign-off.

---

## Screen 3 · Reservoir config — Connection matrix

**▶ [Open the interactive prototype →](product-config-reservoir.html)** — editable κ-coupling / inflow / feeds matrices over the real reservoir data.

The MONIAC layer is a graph: **knobs → reservoirs → pillars**, plus reservoir↔reservoir **κ
couplings**. It is edited as **adjacency matrices**, one editable cell per connection (blank = no
link), which is precise and complete (no connection can be silently missed).

```
A · INFLOW gains  (source input → reservoir)
                  GLY   ATH   INFL  CRF   …
  HbA1c           0.45   ·     ·     ·
  ApoB             ·    0.50   ·     ·
  sleep debt       ·     ·    0.30   ·
  steps            ·     ·     ·    0.40

B · FEEDS  (reservoir → pillar, with ρₖ cap)     C · κ COUPLING (reservoir → reservoir)
  GLY → MET   cap ρₖ 0.20                            ATH ← GLY  +0.15   ATH ← INFL +0.15
  ATH → CV    cap ρₖ 0.20                            GLY ← SLD  +0.10   GLY ← CRF  −0.08
```

Each cell carries **gain, sign (+/−), and polarity** where relevant; decay τ and setpoint are
per-reservoir fields beside matrix A. Validation at publish: every cell references a valid node,
feeds resolve to a real pillar, and κ stays within bounds. This is the editable form of
`calc-graph.json reservoirs` + `reservoir_coupling` + `reservoir-flows.json`.

---

## Screen 4 · Storage & caching — Versioned snapshot + in-memory cache

The hard requirement: config is **editable at runtime**, yet a PureScore calc must add **zero DB
queries**. The answer is a **compile-on-publish, version-pinned snapshot** held in memory.

```
 edit  ─▶  DB (draft revision)  ─▶  [ Publish ]
                                        │  validate invariants (Σ=1, coverage, refs)
                                        ▼
                              compile a single IMMUTABLE snapshot
                                  config@v42  (one fully-resolved blob)
                                        │  bump active_version pointer → 42
                                        │  emit pub/sub "config:published v42"
                                        ▼
        ┌───────── each scorer instance ─────────┐
        │  on boot / on event: load snapshot v42  │
        │  hold it in memory (atomic swap)        │
        │  SCORE CALC reads RAM only · 0 DB/calc  │
        └─────────────────────────────────────────┘
   every PureScore result is stamped  config_version = 42   (reproducible · auditable)
```

- **Compile-on-publish:** publishing resolves the whole config (pillars, weights, input bindings,
  reservoir edges, constants) into one immutable artifact — the runtime analogue of today's
  `calc-data.js`. Stored keyed by `version` with a checksum.
- **In-memory cache:** each instance keeps the active snapshot in RAM; scoring never touches the DB.
- **Invalidation:** a publish bumps an `active_version` pointer and fires a pub/sub event; instances
  hot-swap to the new snapshot atomically. **TTL fallback:** instances also re-check the cheap
  `active_version` token every N seconds in case an event is missed (belt-and-braces, still no
  per-calc DB read).
- **Provenance:** every score records the `config_version` it used → exact reproducibility, clean
  audit trail (ties to the evidence/provenance registry, Doc 15), and historical re-compute.
- **Rollback:** repoint `active_version` to any prior snapshot — instant, no recompile.
- **Cold start / consistency:** load the active snapshot at boot; multi-instance convergence is
  within seconds and any window is auditable because each score carries its version stamp.

---

## Data model (config entities)

| Entity | Key fields | Notes |
|---|---|---|
| **ConfigRevision** | `version`, `status` (draft / in_review / published / archived), `author`, `created_at`, `published_at`, `parent_version`, `notes` | The unit of change; immutable once published. |
| **PillarConfig** | `revision`, `pillar_code`, `base_weight` | Σ base_weight = 1 (enforced at publish). |
| **InputBinding** | `revision`, `pillar_code`, `input_ref`, `input_type` (marker / wearable / question), `weight`, `tier`, `two_sided`, `safety_critical`, `bands{green,yellow,red}`, `source` | Unifies labs, wearables and questions as "inputs". |
| **ReservoirEdge** | `revision`, `source_ref`+`source_type` (marker / knob / reservoir), `target_ref`+`target_type` (reservoir / pillar), `edge_type` (inflow / feed / kappa), `gain`, `sign`, `polarity` | The matrices in Screen 3. |
| **ConstantConfig** | `revision`, `symbol` (γ, δ, ρₖ, φ…), `value` | The engine constants. |
| **CompiledSnapshot** | `version`, `blob`, `checksum`, `compiled_at` | The cached runtime artifact. |

**Publish-time invariants** (the runtime `_engine_guard`): pillar base weights sum to 1; each
pillar's within-pillar input weights sum to 1 (or are normalized); every pillar retains its
safety-critical coverage; all reservoir edges resolve to real nodes; constants within bounds.

## Publish workflow & governance

```
draft ──edit──▶ simulate (Screen 2) ──guardrails pass?──▶ sign-off ──▶ publish ──▶ v++ ──▶ hot-swap
   ▲                                         │ no                          (admin-governance / Doc 16)
   └─────────────────── revise ◀─────────────┘
```

Sign-off reuses the existing **Governance & sign-off** surface (ch.10) and the Doc 16 safety gates;
no config reaches production without passing simulation guardrails and an approver.

## API sketch

| Method · path | Purpose |
|---|---|
| `GET /config/draft` | Current editable draft revision |
| `PATCH /config/draft/pillars/{p}/inputs/{i}` | Set a weight → returns the redistributed set |
| `POST /config/draft/simulate` | Body: cohort filter → before/after diff (Screen 2) |
| `POST /config/draft/publish` | Validate → compile → bump version (requires sign-off) |
| `GET /config/active` | Active `version` + checksum (the cheap token instances poll) |
| `GET /score?...&config_version=` | Compute (defaults to active; pin a version for reproducibility) |

## How this maps to today's engine

| Today (file + build) | This design (runtime + DB) |
|---|---|
| `calc-graph.json` + `pillar-weights.json` + `constants.json` (hand-edited) | The editable **config store** (draft revision) |
| `python3 build_wiki.py` rebuild | **Publish** → compile step |
| `calc-data.js` (`window.PURESCORE_DATA`) | The **compiled snapshot** in memory |
| `_engine_guard` (build fails if Σ≠1) | **Publish-time validation** (publish blocked if invariants fail) |
| edit → rebuild → ship | edit → **simulate** → sign-off → publish → hot-swap |

## Open questions / next steps

- **Cohort sampling fidelity** for the simulator — per-cell percentile draw vs median-only; how many
  synthetic members; whether to weight by real population mix.
- **Conditioned weight vectors** (sex/age-specific weights) — currently a tracked gap; the slider
  model should extend to per-stratum budgets.
- **Per-marker tier defaults** — even with sliders, new inputs need a sensible starting weight; a
  small tier→default table seeds the redistribution.
- **Build fidelity of this page** — ship as this design doc first; an **interactive mock** (sliders +
  live before/after over a sample cohort) is a natural follow-up using the existing `engine.js`.
