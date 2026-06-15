#!/usr/bin/env node
/* ============================================================================
 * PureScore — executable validation harness (Doc 13; decision D17)
 *
 * Runs a SEEDED SYNTHETIC COHORT through the live scoring engine (extracted from
 * purescore-architecture.html so it can never drift from the calculator) and
 * reports calibration, discrimination, and FAIRNESS gates per Doc 13.
 *
 * HONESTY (Doc 13 §2a, Doc 00): this measures SELF-CONSISTENCY against a known
 * synthetic ground-truth model. It can show a detector is broken; it can NEVER
 * establish real-world performance. No gate here is satisfied for production —
 * that needs prospective, linked, real data + governance sign-off (Doc 11).
 *
 * Usage:  node docs/purescore/assets/validation_harness.js [N] [seed]
 * ========================================================================== */
'use strict';
const fs = require('fs');
const path = require('path');

/* ---- load the scoring engine from the single source of truth ---- */
const HTML = fs.readFileSync(path.join(__dirname, '..', 'purescore-architecture.html'), 'utf8');
const start = HTML.indexOf('const GAMMA=3'), end = HTML.indexOf('/* ---- colours ---- */');
if (start < 0 || end < 0) { console.error('Could not locate engine block in HTML'); process.exit(1); }
let engine = HTML.slice(start, end);
engine += '\n; globalThis.__E = { set:(p)=>{PERSONA=p}, setNatal:(s)=>{CTX.natal=s; CTX.hrt="none"; CTX.stage="reproductive"; syncSex();}, setEthn:(e)=>{CTX.ethnicity=e;}, applyPersona, compute, MARKERS };';
(0, eval)(engine);
const E = globalThis.__E;

/* ---- seeded RNG (deterministic) ---- */
const N = parseInt(process.argv[2] || '6000', 10);
let s = (parseInt(process.argv[3] || '12345', 10)) >>> 0;
function rnd() { s = (s + 0x6D2B79F5) >>> 0; let t = Math.imul(s ^ (s >>> 15), 1 | s); t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t; return ((t ^ (t >>> 14)) >>> 0) / 4294967296; }
function randn() { let u = 0, v = 0; while (u === 0) u = rnd(); while (v === 0) v = rnd(); return Math.sqrt(-2 * Math.log(u)) * Math.cos(2 * Math.PI * v); }
const clamp = (x, a, b) => Math.max(a, Math.min(b, x));

/* ---- helpers ---- */
const mget = id => { for (const k in E.MARKERS) for (const m of E.MARKERS[k]) if (m.id === id) return m; };
const setv = (id, v) => { const m = mget(id); if (m) m.val = v; };
const sig = x => 1 / (1 + Math.exp(-x));
const z = (x, mu, sd) => clamp((x - mu) / sd, -4, 4);

const logit = p => Math.log(clamp(p, 1e-3, 1 - 1e-3) / (1 - clamp(p, 1e-3, 1 - 1e-3)));
/* GROUND-TRUTH risk model — DELIBERATELY a different functional form from PureScore
   (independent logistic on key drivers) so calibration is a real, non-circular test. */
function pTrue(d) {
  const L = 0.95 * z(d.a1c, 5.4, 0.6) + 0.80 * z(d.sbp, 120, 15) + 0.85 * z(d.apob, 80, 25)
    - 0.90 * z(d.egfr, 95, 20) + 0.60 * Math.log(Math.max(d.crp, 0.1))
    - 0.55 * z(d.vo2, 40, 9) + 0.55 * z(d.bmi, 24, 4);
  return sig(1.75 * L - 3.2);
}

/* ---- run the cohort ---- */
E.set('healthy');
const rows = [];
const ETHN = [['south_asian', 0.55], ['gulf_arab', 0.20], ['sea_filipino', 0.10], ['western', 0.15]];  // ~UAE mix
function pickEthn() { let r = rnd(), a = 0; for (const [e, p] of ETHN) { a += p; if (r < a) return e; } return 'western'; }
const ASIAN_TIER = new Set(['south_asian', 'gulf_arab', 'sea_filipino', 'other_arab']);
for (let i = 0; i < N; i++) {
  const natal = rnd() < 0.5 ? 'm' : 'f';
  const eth = pickEthn();
  E.setNatal(natal); E.setEthn(eth); E.applyPersona(true);
  const d = {
    a1c: clamp(5.0 + Math.abs(randn()) * 0.85, 4.5, 9.5),
    sbp: clamp(112 + randn() * 16, 90, 185),
    apob: clamp(70 + Math.abs(randn()) * 32, 40, 180),
    egfr: clamp(102 - Math.abs(randn()) * 26, 12, 120),
    crp: clamp(Math.exp(randn() * 0.85), 0.1, 30),
    vo2: clamp((natal === 'm' ? 42 : 36) + randn() * 10, 12, 70),
    bmi: clamp(24 + randn() * 4.2, 16, 42),
    waist: 0
  };
  d.waist = clamp((natal === 'm' ? 90 : 80) + (d.bmi - 24) * 2.2, 60, 140);
  for (const k of ['a1c', 'sbp', 'apob', 'egfr', 'crp', 'vo2', 'bmi', 'waist']) setv(k, d[k]);
  const out = E.compute();
  const risk = 1 - out.pure / 100;                 // PureScore-derived risk
  const p = pTrue(d);
  const y = rnd() < p ? 1 : 0;                       // synthetic outcome
  rows.push({ risk, y, natal, ethTier: ASIAN_TIER.has(eth) ? 'asian' : 'standard', ew: out.companion.ewTier });
}

/* ---- metrics (field selectable: raw `risk` or recalibrated `cal`) ---- */
const pick = (x, f) => x[f];
function brier(r, f) { return r.reduce((a, x) => a + (pick(x, f) - x.y) ** 2, 0) / r.length; }
function prevalence(r) { return r.reduce((a, x) => a + x.y, 0) / r.length; }
function auroc(r, f) {
  const pos = r.filter(x => x.y === 1).map(x => pick(x, f)), neg = r.filter(x => x.y === 0).map(x => pick(x, f));
  if (!pos.length || !neg.length) return NaN;
  let c = 0; for (const p of pos) for (const n of neg) c += p > n ? 1 : p === n ? 0.5 : 0;
  return c / (pos.length * neg.length);
}
function ece(r, f, bins = 10) {   // equal-frequency (adaptive) ECE — robust to sparse score regions
  const a = [...r].sort((x, y) => pick(x, f) - pick(y, f)); const n = a.length; let e = 0;
  for (let b = 0; b < bins; b++) {
    const g = a.slice(Math.floor(b * n / bins), Math.floor((b + 1) * n / bins));
    if (!g.length) continue;
    const conf = g.reduce((s, x) => s + pick(x, f), 0) / g.length, acc = g.reduce((s, x) => s + x.y, 0) / g.length;
    e += (g.length / n) * Math.abs(conf - acc);
  }
  return e;
}
/* Isotonic (PAV) recalibration (Doc 13 §4): monotone risk→probability map fit on TRAIN */
function isotonicFit(tr) {
  const a = tr.map(x => ({ r: x.risk, y: x.y })).sort((p, q) => p.r - q.r);
  const st = [];
  for (let i = 0; i < a.length; i++) {
    let b = { sum: a[i].y, w: 1, xhi: a[i].r, val: a[i].y };
    while (st.length && st[st.length - 1].val >= b.val) {
      const t = st.pop(); b = { sum: t.sum + b.sum, w: t.w + b.w, xhi: b.xhi, val: (t.sum + b.sum) / (t.w + b.w) };
    }
    st.push(b);
  }
  return st;   // blocks with x-upper-bound xhi and pooled probability val, increasing
}
function isotonicApply(model, r) { for (const b of model) if (r <= b.xhi) return b.val; return model[model.length - 1].val; }

/* ---- train/test split + recalibration ---- */
const cut = Math.floor(rows.length / 2);
const train = rows.slice(0, cut), test = rows.slice(cut);
const iso = isotonicFit(train);
for (const x of test) x.cal = isotonicApply(iso, x.risk);

/* ---- report ---- */
const TH = { ece: 0.05, brierSkillMin: 0.05, auroc: 0.70, fairRatio: 0.80 };  // Doc 13 illustrative gates
const pass = b => b ? 'PASS' : 'FAIL';
const prev = prevalence(test), base = prev * (1 - prev);
const aurocV = auroc(test, 'cal'), eceRaw = ece(test, 'risk'), eceCal = ece(test, 'cal');
const brierCal = brier(test, 'cal'), skill = 1 - brierCal / base;

console.log('PureScore — Validation & Calibration Harness (Doc 13 / D17)');
console.log('SYNTHETIC SELF-CONSISTENCY ONLY — not evidence (Doc 13 §2a, Doc 00).');
console.log('N=' + N + ' (train ' + train.length + ' / test ' + test.length + ')  prevalence=' + (prev * 100).toFixed(1) + '%');
console.log('Isotonic (PAV) recalibration fit on train: ' + iso.length + ' monotone blocks\n');

console.log('Headline score O0 (evaluated on held-out test):');
console.log('  AUROC               = ' + aurocV.toFixed(3) + '   [gate ≥ ' + TH.auroc + ']  ' + pass(aurocV >= TH.auroc));
console.log('  ECE raw (1−pure/100)= ' + eceRaw.toFixed(4) + '   ← uncalibrated: PureScore is a wellness index, not a probability');
console.log('  ECE recalibrated    = ' + eceCal.toFixed(4) + '   [gate ≤ ' + TH.ece + ']  ' + pass(eceCal <= TH.ece));
console.log('  Brier (recal)       = ' + brierCal.toFixed(4) + '   (no-skill ' + base.toFixed(4) + ')');
console.log('  Brier skill (recal) = ' + skill.toFixed(3) + '   [gate ≥ ' + TH.brierSkillMin + ']  ' + pass(skill >= TH.brierSkillMin) + '\n');

/* fairness slice: natal sex (D7/D15) — on recalibrated test scores */
console.log('Fairness — parity by natal-sex subgroup, recalibrated (D7/D15, O7):');
const subs = ['m', 'f'].map(sx => { const g = test.filter(x => x.natal === sx); return { sx, n: g.length, auroc: auroc(g, 'cal'), ece: ece(g, 'cal') }; });
subs.forEach(g => console.log('  natal ' + (g.sx === 'm' ? '♂' : '♀') + '  n=' + g.n + '  AUROC=' + g.auroc.toFixed(3) + '  ECE=' + g.ece.toFixed(4)));
const aurocs = subs.map(g => g.auroc), ratio = Math.min(...aurocs) / Math.max(...aurocs);
console.log('  AUROC max–min ratio = ' + ratio.toFixed(3) + '   [gate ≥ ' + TH.fairRatio + ']  ' + pass(ratio >= TH.fairRatio) + '\n');

/* fairness slice: ethnicity cut-point tier (D18/D21) — recalibrated test scores */
console.log('Fairness — parity by ethnicity cut-point tier (D18/D21, O7):');
const esubs = ['asian', 'standard'].map(t => { const g = test.filter(x => x.ethTier === t); return { t, n: g.length, auroc: auroc(g, 'cal'), ece: ece(g, 'cal') }; });
esubs.forEach(g => console.log('  ' + g.t.padEnd(9) + ' n=' + g.n + '  AUROC=' + g.auroc.toFixed(3) + '  ECE=' + g.ece.toFixed(4)));
const eaur = esubs.map(g => g.auroc), eratio = Math.min(...eaur) / Math.max(...eaur);
console.log('  AUROC max–min ratio = ' + eratio.toFixed(3) + '   [gate ≥ ' + TH.fairRatio + ']  ' + pass(eratio >= TH.fairRatio) + '\n');

const ewRate = test.filter(x => x.ew !== 'none').length / test.length;
console.log('Early-warning (O6): cross-sectional alarm rate=' + (ewRate * 100).toFixed(1) + '%');
console.log('  NOTE: lead-time / per-tier PPV @ realistic prevalence require the LONGITUDINAL generator (Doc 13 §2a,§5) — NOT validated here.\n');

const gates = [aurocV >= TH.auroc, eceCal <= TH.ece, skill >= TH.brierSkillMin, ratio >= TH.fairRatio, eratio >= TH.fairRatio];
console.log('GATE SUMMARY (synthetic): ' + gates.filter(Boolean).length + '/' + gates.length + ' pass → ' + (gates.every(Boolean) ? 'method self-consistent' : 'NEEDS WORK'));
console.log('PRODUCTION VERDICT: NO-SHIP — synthetic ≠ evidence; prospective real-data validation + Doc 11 governance required (Babylon lesson).');
