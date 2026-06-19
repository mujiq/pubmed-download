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

  function markerR(th, v) {
    var y = th[0], r = th[1], dir = th[2];
    if (dir === "hi") { if (v >= r) return 0.66; if (v >= y) return 0.40; return 0.10; }
    if (v <= r) return 0.66; if (v <= y) return 0.40; return 0.10;
  }
  function markerZone(rv) { return rv >= 0.66 ? "red" : (rv >= 0.40 ? "yellow" : "green"); }

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
    var rclin = markerR(mm.th, V[m]);
    var cohort = mm.default;                       // illustrative age×sex cohort median (D33)
    var rcohort = markerR(mm.th, cohort);
    var o = { state: state, rclin: rclin, rcohort: rcohort, cohort: cohort,
              imputable: !!mm.imputable, source: mm.source };
    if (state === "stale") {
      o.conf = q * Math.exp(-ep.stale_dt_days / ep.tau_days);
      o.r = rclin * o.conf + 0.10 * (1 - o.conf);  // blend toward neutral as confidence decays
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
      o.conf = q; o.r = rclin; o.w = 1; o.included = true; o.confBranch = "ok"; o.imputed = false; o.usedVal = V[m];
    }
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
    var RV = {}; for (var mm in V) RV[mm] = (st[mm] === "missing") ? d.markers[mm].default : V[mm];
    var RES = reservoirs(RV), L = RES.coupled;
    var rk = {}, crit = [], detail = {}, covW = 0, confW = 0;
    for (var pid in d.pillars) {
      var P = d.pillars[pid], wsum = 0, rsum = 0, cr = false, mdet = {}, pconf = 0, pcov = 0, n = P.markers.length;
      P.markers.forEach(function (m) {
        var ev = markerEval(m, V, st, d), isCrit = P.critical.indexOf(m) >= 0;
        var critRed = isCrit && ev.state === "present" && ev.rclin >= 0.66;   // only fresh data hard-fires
        if (ev.included) { rsum += ev.w * ev.r; wsum += ev.w; }
        if (critRed) cr = true;
        pconf += ev.conf; pcov += ev.w;
        mdet[m] = { v: ev.usedVal, raw: V[m], r: (ev.r != null ? ev.r : null),
          zone: (ev.r != null ? markerZone(ev.r) : "na"), critical: isCrit, state: ev.state,
          conf: ev.conf, w: ev.w, included: ev.included, imputed: ev.imputed, rcohort: ev.rcohort,
          branch: { conf: ev.confBranch, blend: ev.blendBranch, pers: (isCrit ? "off" : "on"), crit: (critRed ? "fire" : "pass") } };
      });
      var Rm = wsum > 0 ? rsum / wsum : 0;
      var bt = 0; P.reservoirs.forEach(function (rid) { bt += L[rid]; }); bt = P.reservoirs.length ? bt / P.reservoirs.length : 0;
      var R = clamp01(Rm + C.rho * bt);
      if (cr) { R = Math.max(R, C.r_crit); crit.push(pid); }
      rk[pid] = R;
      var cov = pcov / n, conf = pconf / n;
      detail[pid] = { Rm: Rm, bt: bt, R: R, crit: cr, weight: P.weight, coverage: cov, confidence: conf,
        lowCoverage: cov < C.cov_green_floor, markers: mdet,
        reservoirs: P.reservoirs.map(function (rid) { return { id: rid, L: L[rid], base: RES.base[rid] }; }) };
      covW += P.weight * cov; confW += P.weight * conf;
    }
    var num = 0, wsum = 0; for (var p2 in d.pillars) { num += d.pillars[p2].weight * Math.pow(rk[p2], C.delta); wsum += d.pillars[p2].weight; }
    var Rtot = Math.pow(num / (wsum || 1), 1 / C.delta);   // normalise by ΣW so edited weights stay valid
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
    markerR: markerR, markerZone: markerZone, nrm: nrm, clamp01: clamp01,
    profiles: function () { return D().profiles; }
  };
})(typeof window !== "undefined" ? window : this);
