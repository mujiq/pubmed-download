/* PureScore shared scoring engine — SINGLE SOURCE OF COMPUTE.
 * Logic only. All data (markers, reservoirs, pillars, weights, constants, gates, profiles)
 * comes from window.PURESCORE_DATA, which build_wiki.py resolves from data/calc-graph.json
 * + data/pillar-weights.json + data/constants.json into assets/calc-data.js.
 * Models missing/incomplete data (D33): confidence + decay, cohort imputation, coverage.
 * Illustrative — re-verify (README §5.6).
 */
(function (root) {
  "use strict";
  function D() { return root.PURESCORE_DATA; }
  function clamp01(x) { return Math.max(0, Math.min(1, x)); }
  function r2(x) { return Math.round(x * 100) / 100; }
  function nrm(v, good, bad) { if (good === bad) return 0; return clamp01((v - good) / (bad - good)); }

  // Stage 1 — continuous, monotone, two-sided clinical risk r_i^clin ∈ [0,1] (Doc 03 §1).
  // th = [yellow-cutpoint, red-cutpoint, dir]; opt = optimum (band centre, r=0). The curve is
  // C⁰-continuous: optimum-centred green gradient → 0.15 at the yellow cut → 0.50 at the red cut
  // → saturating toward 1 in the red. No discrete band jumps (replaces the old 3-value quantiser).
  function contRisk(v, opt, y, r, s) {
    if (opt == null) opt = (s > 0 ? Math.min(y, r) : Math.max(y, r));  // fallback optimum (no default)
    var dv = s * (v - opt), dy = s * (y - opt), dr = s * (r - opt);    // adverse distances from optimum
    if (dv <= 0) return 0;                                             // at / past optimum on the good side
    if (dv <= dy) return 0.15 * dv / Math.max(dy, 1e-9);              // green core: optimum-centred → 0.15
    if (dv <= dr) return 0.15 + 0.35 * (dv - dy) / Math.max(dr - dy, 1e-9);  // yellow → red edge
    var scale = Math.max(Math.abs(dr - dy), 1e-9);                     // saturation e-fold = yellow→red width
    return 0.50 + 0.50 * (1 - Math.exp(-(dv - dr) / scale));           // red, saturating → 1
  }
  function markerR(th, v, opt) {
    return contRisk(v, opt, th[0], th[1], th[2] === "hi" ? 1 : -1);
  }
  // Zone labels are read OFF the continuous curve (Doc 03 §1 / Conventions §3.5): r<0.15 green,
  // <0.50 yellow, ≥0.50 red. Pillar status uses its own cuts (Doc 03 §4): R<0.30 / <0.60 / ≥0.60.
  function markerZone(rv) { return rv >= 0.50 ? "red" : (rv >= 0.15 ? "yellow" : "green"); }
  function pillarZone(R) { return R >= 0.60 ? "red" : (R >= 0.30 ? "yellow" : "green"); }
  // Stage 2b — band_clamp: the personal-baseline term may move r WITHIN its safety band but can
  // never flip a band (Doc 03 §2b). anchor = the pre-personalisation safety risk max(r_clin, φ·r_cohort).
  function bandClamp(x, anchor) {
    var lo, hi;
    if (anchor < 0.15) { lo = 0; hi = 0.15; }
    else if (anchor < 0.50) { lo = 0.15; hi = 0.50; }
    else { return Math.max(0.50, Math.min(1, x)); }   // red can only stay red
    return Math.max(lo, Math.min(hi - 1e-9, x));
  }

  function vector(mk) {
    var d = D(), out = {};
    for (var m in d.markers) out[m] = d.markers[m].default;
    for (var o in (mk || {})) if (o in d.markers) out[o] = mk[o];
    return out;
  }

  // reservoir loads from a value vector (missing inputs already imputed to cohort median by caller)
  function reservoirs(vv) {
    var d = D(), base = {};
    for (var rid in d.reservoirs) {
      var inp = d.reservoirs[rid].inputs, s = 0;
      for (var i = 0; i < inp.length; i++) s += nrm(vv[inp[i][0]], inp[i][1], inp[i][2]);
      base[rid] = s / inp.length;
    }
    var c = {}; for (var k in base) c[k] = base[k];
    (d.reservoir_coupling || []).forEach(function (cp) {
      var v = base[cp.t];
      (cp.add || []).forEach(function (a) { v += a[1] * base[a[0]]; });
      (cp.subDeficit || []).forEach(function (a) { v -= a[1] * (1 - base[a[0]]); });
      c[cp.t] = clamp01(v);
    });
    return { base: base, coupled: c };
  }

  // Per-marker evaluation under a data state (present | stale | missing). Returns the active
  // pipeline branches + the effective risk r, weight w, and confidence (D33 fallback rules).
  function markerEval(m, V, st, d) {
    var mm = d.markers[m], C = d.const, ep = d.engine_params;
    var state = (st && st[m]) || "present";
    var q = (ep.q_source[mm.source] != null ? ep.q_source[mm.source] : 0.8);
    var rclin = markerR(mm.th, V[m], mm.default);
    var cohort = mm.default;                       // illustrative age×sex cohort median (D33)
    var rcohort = markerR(mm.th, cohort, mm.default);
    var rsafe = Math.max(rclin, C.phi * rcohort);  // Stage 2 — cohort may only RAISE concern (Doc 03 §2)
    var o = { state: state, rclin: rclin, rcohort: rcohort, cohort: cohort,
              imputable: !!mm.imputable, source: mm.source };
    if (state === "stale") {
      o.conf = q * Math.exp(-ep.stale_dt_days / ep.tau_days);
      o.r = rsafe * o.conf + 0.10 * (1 - o.conf);  // blend toward neutral as confidence decays
      o.w = 1; o.included = true; o.confBranch = "stale"; o.imputed = false; o.usedVal = V[m];
    } else if (state === "missing") {
      if (mm.imputable) {                          // impute to cohort median, down-weighted, low confidence
        o.conf = C.q_impute; o.r = rcohort; o.w = C.rp_impute;
        o.included = true; o.confBranch = "impute"; o.imputed = true; o.usedVal = cohort;
      } else {                                     // non-imputable → drop from pillar mean
        o.conf = 0; o.r = null; o.w = 0;
        o.included = false; o.confBranch = "drop"; o.imputed = false; o.usedVal = null;
      }
    } else {                                       // present
      o.conf = q; o.r = rsafe; o.w = 1; o.included = true; o.confBranch = "ok"; o.imputed = false; o.usedVal = V[m];
    }
    o.rsafe = (o.r != null ? o.r : null);          // band_clamp anchor for the Stage-2b term (Doc 03 §2b)
    o.blendBranch = (C.phi * rcohort > rclin) ? "raise" : "anchor";
    return o;
  }

  // Active applicability-gate branch ids from profile attributes.
  function gateState(o) {
    o = o || {}; var a = o.age;
    return {
      sex: o.sex || null,
      preg: o.preg ? "yes" : "no",
      age: (a == null ? null : (a < 40 ? "young" : (a < 65 ? "mid" : "older"))),
      lifestage: o.lifestage || null
    };
  }

  // Full trace. opts = {adh, preg, sex, age, lifestage, st:{marker:'present'|'stale'|'missing'}}
  function score(mk, opts) {
    var d = D(), C = d.const, V = vector(mk), o = opts || {}, st = o.st || {};
    var managed = o.managed || {}, confound = o.confound || {};   // from medications (D3)
    var pz = o.pz || {};                                          // optional personal z per marker (Stage 2b)
    var gamma = C.gamma || 3, kappa = (C.kappa_resp != null ? C.kappa_resp : 0.10);
    var RV = {}; for (var mm in V) RV[mm] = (st[mm] === "missing") ? d.markers[mm].default : V[mm];
    var RES = reservoirs(RV), L = RES.coupled;
    var rk = {}, crit = [], detail = {}, covW = 0, confW = 0;
    for (var pid in d.pillars) {
      var P = d.pillars[pid], wsum = 0, rsum = 0, cr = false, mdet = {}, pconf = 0, pcov = 0, n = P.markers.length;
      P.markers.forEach(function (m) {
        var ev = markerEval(m, V, st, d), isCrit = P.critical.indexOf(m) >= 0;
        if (managed[m]) { ev.conf *= 0.9; ev.managed = true; }       // controlled: shown but tagged (D3)
        if (confound[m]) { ev.conf *= 0.7; ev.confounded = true; }   // drug confounds the reading (D3)
        var critRed = isCrit && ev.state === "present" && ev.rclin >= 0.50;   // only fresh data hard-fires
        // Stage 2b — personal-baseline responsiveness, band-clamped, never on a critical marker.
        var zi = (pz[m] != null ? pz[m] : 0);
        var doPers = ev.included && ev.state === "present" && !isCrit;
        var rpers = doPers ? kappa * Math.tanh(zi / 2) : 0;
        var reff = ev.included ? (doPers ? bandClamp(ev.r + rpers, ev.rsafe) : ev.r) : null;
        if (ev.included) { rsum += ev.w * Math.pow(reff, gamma); wsum += ev.w; }  // Stage 3 — γ-power-mean
        if (critRed) cr = true;
        pconf += ev.conf; pcov += ev.w;
        mdet[m] = { v: ev.usedVal, raw: V[m], r: (reff != null ? reff : null), rsafe: ev.rsafe, z: zi,
          zone: (reff != null ? markerZone(reff) : "na"), critical: isCrit, state: ev.state,
          conf: ev.conf, w: ev.w, included: ev.included, imputed: ev.imputed, rcohort: ev.rcohort,
          managed: !!ev.managed, confounded: !!ev.confounded,
          branch: { conf: ev.confBranch, blend: ev.blendBranch, pers: (doPers ? "on" : "off"), crit: (critRed ? "fire" : "pass") } };
      });
      var hasData = wsum > 0;                                        // Stage 3 — empty-pillar rule (Doc 03 §3)
      var Rm = hasData ? Math.pow(rsum / wsum, 1 / gamma) : 0;       // power-mean only over observed markers
      var bt = 0; P.reservoirs.forEach(function (rid) { bt += L[rid]; }); bt = P.reservoirs.length ? bt / P.reservoirs.length : 0;
      var R = clamp01(Rm + C.rho * bt);
      if (cr) { R = Math.max(R, C.r_crit); crit.push(pid); }
      rk[pid] = R;
      var cov = pcov / n, conf = pconf / n;
      detail[pid] = { Rm: Rm, bt: bt, R: R, crit: cr, noData: !hasData, weight: P.weight, coverage: cov, confidence: conf,
        lowCoverage: cov < C.cov_green_floor || !hasData, markers: mdet,
        reservoirs: P.reservoirs.map(function (rid) { return { id: rid, L: L[rid], base: RES.base[rid] }; }) };
      covW += P.weight * cov; confW += P.weight * conf;
    }
    // Stage 5 — aggregate via δ-power-mean over pillars WITH data (empty pillars excluded, ΣW renormalised).
    var num = 0, wsum = 0; for (var p2 in d.pillars) { if (detail[p2].noData) continue; num += d.pillars[p2].weight * Math.pow(rk[p2], C.delta); wsum += d.pillars[p2].weight; }
    var Rtot = wsum > 0 ? Math.pow(num / wsum, 1 / C.delta) : 0;   // normalise by ΣW so edited weights stay valid
    var sc = Math.round(100 * (1 - Rtot));
    if (crit.length) sc = Math.min(sc, C.pure_crit_cap);
    var coverage = covW, confidence = confW, lowCov = coverage < C.cov_green_floor;
    return {
      mk: V, st: st, res: RES, rk: rk, crit: crit, Rtot: Rtot, score: sc, detail: detail,
      coverage: coverage, confidence: confidence, lowCoverage: lowCov,
      companion: { confidence: confidence, coverage: coverage, lowCoverage: lowCov,
        trajectory: (Rtot < 0.3 ? "stable / improving" : "watch"), ew: (crit.length > 0 || lowCov) },
      nudge: nudge(rk), adherence: adherence(o.adh), actuarial: actuarial(sc),
      gates: gateState(o), preg: o.preg || 0
    };
  }

  function nudge(rk) {
    var arr = Object.keys(rk).map(function (p) { return { pillar: p, R: rk[p] }; })
      .sort(function (a, b) { return b.R - a.R; });
    return arr.slice(0, 5).map(function (x) {
      var impact = r2(x.R), phat = 0.6, ease = 0.7;
      return { pillar: x.pillar, impact: impact, phat: phat, ease: ease, U: r2(impact * phat * ease) };
    });
  }
  function adherence(adh) { var a = adh != null ? adh : 0.8; return { adherence: a, ewma: a, inflow: r2(a * 0.2) }; }
  function actuarial(sc) { var rr = r2(2 - sc / 100); return { rr: rr, premium: r2(1 + 0.5 * (rr - 1)) }; }

  root.PureScore = {
    data: D, score: score, reservoirs: reservoirs, vector: vector,
    markerEval: markerEval, gateState: gateState,
    markerR: markerR, markerZone: markerZone, pillarZone: pillarZone, bandClamp: bandClamp, nrm: nrm, clamp01: clamp01,
    profiles: function () { return D().profiles; }
  };
})(typeof window !== "undefined" ? window : this);
