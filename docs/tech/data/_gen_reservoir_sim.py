#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Generate reservoir-sim.html from data/reservoir-flows.json (JSON-canonical).
Standalone full page (BIOMONIAC hydraulic-machine UI), data inlined at build time so it
runs from file:// without fetch/CORS. Engine = the same 5-shape relaxation solver, made
fully data-driven and generalized from 9 to the canonical 15 reservoirs.
Run:  python3 data/_gen_reservoir_sim.py   (from docs/tech/)
"""
import json, os
HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
M = json.load(open(os.path.join(HERE, "reservoir-flows.json"), encoding="utf-8"))
DATA = json.dumps(M, ensure_ascii=False, separators=(",", ":"))

PAGE = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>PureScore — Reservoir Simulator (BIOMONIAC)</title>
<style>
:root{--bg:#080e1a;--panel:#0f1829;--panel2:#0c1422;--glass:#121d30;--line:#1a2744;--txt:#e2eaf5;--muted:#6b8ab0;
--accent:#36cfbf;--accent2:#2ba89a;--accent-bg:rgba(54,207,191,0.12);--asset:#36cfbf;--burden:#ef6b6b;--warn:#f5a623;--good:#4ade80;--bad:#ef4444;--rx:#6366f1;}
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,'Segoe UI',Roboto,Helvetica,sans-serif;background:var(--bg);color:var(--txt);height:100vh;overflow:hidden}
header{display:flex;align-items:center;gap:14px;padding:10px 20px;background:var(--panel2);border-bottom:1px solid var(--line);height:48px}
.logo{font-size:15px;font-weight:800;letter-spacing:.5px}.logo i{color:var(--accent);font-style:normal}
.sub{color:var(--muted);font-size:11px}
.back{color:var(--muted);font-size:11px;text-decoration:none;border:1px solid var(--line);padding:3px 9px;border-radius:5px}
.back:hover{color:var(--txt);border-color:var(--accent)}
.patient-sel{background:var(--glass);border:1px solid var(--line);color:var(--txt);padding:4px 10px;border-radius:6px;font-size:12px;cursor:pointer;margin-left:8px}
.patient-sel:focus{outline:1px solid var(--accent)}
.hz{margin-left:auto;display:flex;gap:3px}
.hz button{background:var(--glass);border:1px solid var(--line);color:var(--muted);padding:4px 11px;border-radius:5px;font-size:11px;cursor:pointer;transition:.12s}
.hz button.on{background:var(--accent);color:var(--bg);border-color:var(--accent);font-weight:700}
.machine{display:grid;grid-template-columns:248px 1fr 270px;height:calc(100vh - 48px - 52px)}
.vcol{background:var(--panel2);border-right:1px solid var(--line);overflow-y:auto;padding:14px 14px 20px}
.sec{font-size:9px;text-transform:uppercase;letter-spacing:1.1px;color:var(--muted);margin:10px 0 8px;font-weight:700}
.valve{margin-bottom:11px}
.vh{display:flex;justify-content:space-between;align-items:baseline;margin-bottom:3px}
.vn{font-size:11.5px;font-weight:600}.vd{font-size:10.5px;font-weight:700;text-align:right}
.vd.pos{color:var(--good)}.vd.neg{color:var(--burden)}.vd.neut{color:var(--muted)}
.vwrap{position:relative;height:6px;margin-top:2px}
.vcenter{position:absolute;left:50%;top:-3px;width:1.5px;height:12px;background:var(--muted);opacity:.35;z-index:1;pointer-events:none}
input[type=range]{-webkit-appearance:none;width:100%;height:6px;border-radius:3px;background:var(--glass);outline:none;cursor:pointer}
input[type=range]::-webkit-slider-thumb{-webkit-appearance:none;width:14px;height:14px;border-radius:50%;background:var(--accent);border:2px solid var(--bg);box-shadow:0 0 5px rgba(54,207,191,.35);cursor:pointer;transition:transform .1s}
input[type=range]::-webkit-slider-thumb:hover{transform:scale(1.18)}
input[type=range]::-moz-range-thumb{width:14px;height:14px;border-radius:50%;background:var(--accent);border:2px solid var(--bg);cursor:pointer}
input[type=range].rx{opacity:.6}
input[type=range].rx::-webkit-slider-thumb{background:var(--rx)}
input[type=range].rx::-moz-range-thumb{background:var(--rx)}
.rxbadge{font-size:8px;padding:1px 5px;border-radius:3px;background:var(--rx);color:#fff;margin-left:5px}
.rcol{padding:16px 20px;overflow-y:auto;display:flex;flex-direction:column;align-items:center}
.patient-card{background:var(--panel);border:1px solid var(--line);border-radius:10px;padding:10px 14px;width:100%;max-width:600px;margin-bottom:12px;display:flex;align-items:center;gap:12px}
.pc-name{font-weight:700;font-size:14px}.pc-meta{color:var(--muted);font-size:11px}
.pc-ps{margin-left:auto;text-align:right}
.pc-ps b{font-size:20px;font-weight:800}.pc-ps span{display:block;font-size:10px;color:var(--muted)}
.tanks{display:grid;grid-template-columns:repeat(5,1fr);gap:10px;max-width:600px;width:100%}
.tank{background:var(--panel);border:1px solid var(--line);border-radius:10px;padding:9px 9px 7px;position:relative;cursor:pointer;transition:border-color .2s,box-shadow .2s}
.tank:hover{border-color:var(--accent);box-shadow:0 0 10px rgba(54,207,191,.12)}
.tank.cascade{border-color:var(--accent);box-shadow:0 0 14px rgba(54,207,191,.22);transition:box-shadow .15s}
.tlbl{font-size:8.5px;text-transform:uppercase;letter-spacing:.5px;color:var(--muted);margin-bottom:3px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.tval{font-size:18px;font-weight:800;margin-bottom:1px}
.tbio{font-size:9px;color:var(--muted);min-height:12px;margin-bottom:5px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.tglass{width:100%;height:36px;background:#060d18;border-radius:0 0 7px 7px;border:1px solid var(--line);position:relative;overflow:hidden}
.tliq{position:absolute;bottom:0;left:0;right:0;border-radius:0 0 6px 6px;transition:height .85s cubic-bezier(.4,0,.2,1),background-color .7s}
.tliq::before{content:'';position:absolute;top:-4px;left:-5%;width:110%;height:8px;border-radius:45%;opacity:.55;background:inherit;filter:brightness(1.4);animation:sway 2.2s ease-in-out infinite alternate}
@keyframes sway{0%{transform:translateX(-2%) scaleY(.6)}100%{transform:translateX(2%) scaleY(1.4)}}
.tdelta{font-size:9px;margin-top:4px;font-weight:700;min-height:13px}
.tdelta.up{color:var(--good)}.tdelta.dn{color:var(--burden)}.tdelta.st{color:var(--muted)}
.pbadge{position:absolute;top:6px;right:7px;font-size:6.5px;text-transform:uppercase;letter-spacing:.3px;padding:1.5px 4px;border-radius:3px;font-weight:800}
.pbadge.asset{background:var(--accent-bg);color:var(--accent)}.pbadge.burden{background:rgba(239,107,107,.12);color:var(--burden)}
.hub{position:absolute;bottom:5px;right:7px;font-size:11px;opacity:.7}
.spiral{position:absolute;bottom:4px;right:7px;font-size:10px;opacity:.7}
.tpipes{display:none;background:var(--bg);border:1px solid var(--line);border-radius:6px;padding:7px 8px;margin-top:6px;font-size:10px;max-height:130px;overflow-y:auto}
.tank.open{grid-column:span 2}.tank.open .tpipes{display:block}
.pipe{display:flex;justify-content:space-between;padding:2px 0;color:var(--muted)}
.pipe b{color:var(--txt)}.pipe .pin{color:var(--burden)}.pipe .pout{color:var(--good)}
.connections{max-width:600px;width:100%;position:relative;margin-top:8px}
.conn-label{font-size:9px;color:var(--muted);text-align:center;letter-spacing:.5px}
.ocol{background:var(--panel2);border-left:1px solid var(--line);overflow-y:auto;padding:14px}
.outcome{background:var(--panel);border:1px solid var(--line);border-radius:10px;padding:12px;margin-bottom:8px;transition:border-color .2s}
.outcome.cascade{border-color:var(--accent)}
.oname{font-size:9px;text-transform:uppercase;letter-spacing:.7px;color:var(--muted);margin-bottom:3px}
.oval{font-size:22px;font-weight:800}
.oband{font-size:10px;font-weight:700;margin-top:1px}
.odelta{font-size:11px;font-weight:700;margin-top:3px;min-height:14px}
.odelta.up{color:var(--good)}.odelta.dn{color:var(--burden)}.odelta.st{color:var(--muted)}
.obar{height:5px;border-radius:3px;background:#0a1120;margin-top:6px;overflow:hidden}
.obar i{display:block;height:100%;border-radius:3px;transition:width .85s ease,background .7s}
.ps{background:var(--panel);border:1px solid var(--line);border-radius:12px;padding:16px;margin-top:10px;text-align:center}
.ps-label{font-size:9px;text-transform:uppercase;letter-spacing:1px;color:var(--muted);margin-bottom:4px}
.ps-val{font-size:36px;font-weight:900;line-height:1}
.ps-bar{height:7px;border-radius:4px;background:#0a1120;margin-top:8px;overflow:hidden}
.ps-bar i{display:block;height:100%;border-radius:4px;transition:width .85s ease,background .7s}
.ps-delta{font-size:11px;margin-top:6px;font-weight:700}
.ps-pillars{display:grid;grid-template-columns:1fr 1fr;gap:4px 10px;margin-top:8px;text-align:left}
.ps-p{font-size:10px;color:var(--muted);display:flex;justify-content:space-between}.ps-p b{color:var(--txt)}
.lb{margin-top:12px}
.lb-title{font-size:9px;text-transform:uppercase;letter-spacing:1px;color:var(--muted);margin-bottom:6px}
.lev{display:flex;justify-content:space-between;align-items:center;padding:4px 0;font-size:11px;border-bottom:1px dashed var(--line)}
.lev-s{font-weight:700;color:var(--accent);min-width:36px;text-align:right}
.lev-rank{color:var(--warn);font-size:9px;font-weight:800;width:18px}
footer{background:var(--panel);border-top:1px solid var(--line);padding:10px 20px;display:flex;align-items:center;gap:12px;height:52px}
.ribbon{flex:1;font-size:12px;color:var(--muted);line-height:1.35;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.ribbon b{color:var(--txt)}.ribbon em{color:var(--accent);font-style:normal;font-weight:600}
.abtn{background:var(--glass);border:1px solid var(--line);color:var(--txt);padding:7px 14px;border-radius:7px;font-size:12px;cursor:pointer;transition:.12s;white-space:nowrap;font-weight:500}
.abtn:hover{border-color:var(--accent)}.abtn.pri{background:var(--accent);color:var(--bg);border-color:var(--accent);font-weight:700}
.abtn.pri:hover{background:var(--accent2)}
::-webkit-scrollbar{width:5px}::-webkit-scrollbar-track{background:transparent}::-webkit-scrollbar-thumb{background:var(--line);border-radius:3px}
</style>
</head>
<body>
<header>
  <div class="logo"><i>BIO</i>MONIAC</div>
  <span class="sub">Reservoir Simulator · runnable Doc 04 MONIAC</span>
  <a class="back" href="04-moniac-reservoir-dynamics.html">Doc 04 ↗</a>
  <a class="back" href="purescore-uber-map.html">Calc Explorer ↗</a>
  <select class="patient-sel" id="patSel" onchange="loadPatient(this.value)"></select>
  <div class="hz" id="hzSel"></div>
</header>
<div class="machine">
  <div class="vcol">
    <div class="sec">Behavioral Knobs — drag to intervene</div><div id="bValves"></div>
    <div class="sec" style="margin-top:14px">Pharmacological (Rx)</div><div id="rxValves"></div>
  </div>
  <div class="rcol">
    <div class="patient-card" id="pCard"></div>
    <div class="sec" style="width:100%;max-width:600px">15 reservoirs — hydraulic state (tap for pipes)</div>
    <div class="tanks" id="tanksGrid"></div>
    <div class="connections"><div class="conn-label" id="connLabel"></div></div>
  </div>
  <div class="ocol">
    <div class="sec">Outcomes (GDP gauges)</div><div id="outcomes"></div>
    <div id="psCard"></div><div class="lb" id="leaderboard"></div>
  </div>
</div>
<footer>
  <div class="ribbon" id="ribbon">Drag a knob to see the cascade…</div>
  <button class="abtn pri" onclick="easiestWin()">Easiest Win</button>
  <button class="abtn" onclick="resetAll()">Reset</button>
</footer>
<script>
const M = __DATA__;
const RES = M.reservoirs, RES_MAP = Object.fromEntries(RES.map(r=>[r.id,r]));
const VALVES = M.valves, RX_VALVES = M.rx_valves, ALL_VALVES = [...VALVES,...RX_VALVES];
const VALVE_MAP = Object.fromEntries(ALL_VALVES.map(v=>[v.id,v]));
const OUT = M.outcomes, OUT_MAP = Object.fromEntries(OUT.map(o=>[o.id,o]));
const FLOWS = M.flows, PATIENTS = M.patients;
const ASSET_R = RES.filter(r=>r.pol==='asset').map(r=>r.id);
const BURDEN_R = RES.filter(r=>r.pol==='burden').map(r=>r.id);

function tfShape(kind,x){switch(kind){
  case 'linear':return x;
  case 'saturating':return Math.tanh(1.6*x);
  case 'log':{const s=x>=0?1:-1;return s*Math.log1p(2.5*Math.abs(x))/Math.log1p(2.5);}
  case 'threshold':{const dz=.15;if(Math.abs(x)<=dz)return 0;const s=x>=0?1:-1;return s*(Math.abs(x)-dz)/(1-dz);}
  case 'j_curve':return Math.tanh(1.6*x)-0.9*Math.max(0,x-0.6)**2/0.16;
  default:return x;}}
function getSetpoint(id){return RES_MAP[id]?RES_MAP[id].sp:50;}
function getSat(id){return RES_MAP[id]?RES_MAP[id].sat:[0,100];}
function getTau(id){return RES_MAP[id]?RES_MAP[id].tau:14;}
function clip(id,v){const[lo,hi]=getSat(id);return Math.min(Math.max(v,lo),hi);}
function simulate(init,valves,days){
  const L={...init};for(const o of OUT)if(!(o.id in L))L[o.id]=50;
  for(let d=0;d<days;d++){const nx={...L};
    for(const id of Object.keys(L)){const inb=FLOWS.filter(f=>f.tgt===id);if(!inb.length)continue;
      const mft=Math.max(...inb.map(f=>f.t),14),tau=Math.max(getTau(id),mft,1),sp=getSetpoint(id);let eq=sp;
      for(const f of inb){let sig;
        if(f.src in valves)sig=(valves[f.src]-50)/50;
        else if(f.src in L)sig=(L[f.src]-getSetpoint(f.src))/50;else sig=0;
        eq+=f.s*f.g*tfShape(f.sh,sig)*f.c;}
      eq=clip(id,eq);const a=1-Math.exp(-1/tau);nx[id]=clip(id,L[id]+a*(eq-L[id]));}
    Object.assign(L,nx);}
  return L;}
function objective(L){let s=0;for(const o of OUT){const v=L[o.id]||50;s+=o.w*(o.pol==='asset'?v:(100-v));}return s;}
function sensitivity(profile,valves,h){
  const base=objective(simulate({...profile},{...valves},h)),rank=[];
  for(const v of VALVES){const pr={...valves},dir=v.up?1:-1;pr[v.id]=Math.min(100,Math.max(0,50+dir*15));
    rank.push({id:v.id,name:v.name,gain:objective(simulate({...profile},pr,h))-base});}
  rank.sort((a,b)=>b.gain-a.gain);return rank;}

const RISK_A={O_ASCVD:[0.76,0.0387],O_T2DM:[1.30,0.0345]};
function riskPct(id,l){if(RISK_A[id]){const[a,b]=RISK_A[id];return Math.min(70,a*Math.exp(b*l));}return l;}
function toBio(id,L){const r=RES_MAP[id];if(!r||!r.bio)return L.toFixed(0)+'/100';
  const parts=r.bio.split('|'),tmpl=parts[0],base=parseFloat(parts[1]),slope=parseFloat(parts[2]);
  let val=r.bioexp==='crp'?0.3*Math.exp(L/28):base+L*slope;
  return tmpl.replace(/\{p(\d)\}/,(m,n)=>val.toFixed(+n));}
function toOutDisplay(id,L){const m={
  O_ASCVD:l=>riskPct('O_ASCVD',l).toFixed(1)+'%',O_T2DM:l=>riskPct('O_T2DM',l).toFixed(1)+'%',
  O_MORT:l=>'HR '+(0.5+l*.02).toFixed(2),O_BIOAGE:l=>((-10+l*.25)>=0?'+':'')+(-10+l*.25).toFixed(1)+' yr',
  O_ENERGY:l=>l.toFixed(0)+'/100'};return(m[id]||(l=>l.toFixed(0)))(L);}
function outBand(id,L){const v=id==='O_ASCVD'?riskPct('O_ASCVD',L):id==='O_T2DM'?riskPct('O_T2DM',L):
  id==='O_MORT'?(0.5+L*.02):id==='O_BIOAGE'?(-10+L*.25):L;
  const B={O_ASCVD:[[5,'Low','good'],[7.5,'Borderline','warn'],[20,'Intermediate','warn'],[99,'High','bad']],
   O_T2DM:[[7,'Low','good'],[17,'Moderate','warn'],[33,'High','bad'],[99,'Very High','bad']],
   O_MORT:[[.8,'Protected','good'],[1.2,'Average','warn'],[1.6,'Elevated','bad'],[99,'High','bad']],
   O_BIOAGE:[[-3,'Younger','good'],[2,'On Track','warn'],[6,'Older','bad'],[99,'Much Older','bad']],
   O_ENERGY:[[35,'Depleted','bad'],[50,'Low','warn'],[75,'Good','good'],[99,'Thriving','good']]};
  for(const[th,lbl,cls]of(B[id]||[]))if(v<=th)return[lbl,cls];return['—','warn'];}
function tankColor(pol,l){if(pol==='asset')return l>=55?'#36cfbf':l>=38?'#f5a623':'#ef6b6b';return l<=38?'#36cfbf':l<=58?'#f5a623':'#ef6b6b';}
function isImproving(id,d){const pol=RES_MAP[id]?.pol||OUT_MAP[id]?.pol||'burden';return pol==='asset'?d>0:d<0;}
function computePureScore(L){
  const riskIds=['O_ASCVD','O_T2DM','O_MORT'];
  const pRisk=100-riskIds.reduce((s,id)=>s+(L[id]||50),0)/riskIds.length;
  const pReserve=(ASSET_R.reduce((s,id)=>s+(L[id]||50),0)/ASSET_R.length +
                  (100-BURDEN_R.reduce((s,id)=>s+(L[id]||50),0)/BURDEN_R.length))/2;
  const pLifestyle=50, pVitality=L['O_ENERGY']||50;
  return 0.4*pRisk+0.25*pReserve+0.15*pLifestyle+0.2*pVitality;}

let horizon=90,patIdx=0,valveState={},baseLevels={},intLevels={},openTankId=null;
function initValves(){valveState={};for(const v of ALL_VALVES)valveState[v.id]=50;}
const HUB=RES.map(r=>r.id).reduce((a,id)=>FLOWS.filter(f=>f.src===id).length>FLOWS.filter(f=>f.src===a).length?id:a);

function buildHorizon(){document.getElementById('hzSel').innerHTML=[30,90,365].map(h=>`<button class="${h===horizon?'on':''}" onclick="setHorizon(${h})">${h===365?'1yr':h+'d'}</button>`).join('');}
function buildPatients(){document.getElementById('patSel').innerHTML=PATIENTS.map((p,i)=>`<option value="${i}">${p.name} — ${p.meta.split('·')[0].trim()}</option>`).join('');}
function buildValves(list,cid){document.getElementById(cid).innerHTML=list.map(v=>`
  <div class="valve"><div class="vh"><span class="vn">${v.name}${v.rx?'<span class="rxbadge">Rx</span>':''}</span>
  <span class="vd neut" id="vd-${v.id}">baseline</span></div>
  <div class="vwrap"><div class="vcenter"></div>
  <input type="range" min="0" max="100" value="50" step="1" class="${v.rx?'rx':''}" id="vs-${v.id}" oninput="onValve('${v.id}',this.value)"></div></div>`).join('');}
function buildTanks(){
  const order=[...BURDEN_R,...ASSET_R];
  document.getElementById('tanksGrid').innerHTML=order.map(id=>{const r=RES_MAP[id];
    const hub=id===HUB?'<span class="hub" title="Most outflows (hub)">&#x2B21;</span>':'';
    const spiral=(id==='sld'||id==='allo')?'<span class="spiral" title="sleep↔stress spiral">&#x21C4;</span>':'';
    return `<div class="tank" id="tank-${id}" onclick="toggleTank('${id}')"><span class="pbadge ${r.pol}">${r.pol}</span>
    <div class="tlbl">${r.name}</div><div class="tval" id="tv-${id}">—</div><div class="tbio" id="tb-${id}"></div>
    <div class="tglass"><div class="tliq" id="tl-${id}"></div></div><div class="tdelta st" id="td-${id}"></div>
    ${hub}${spiral}<div class="tpipes" id="tp-${id}"></div></div>`;}).join('');
  document.getElementById('connLabel').textContent=`${RES_MAP[HUB].name} is the metabolic hub (${FLOWS.filter(f=>f.src===HUB).length} outflows) · Sleep-debt ↔ Allostatic load form a vicious/virtuous spiral`;}
function buildOutcomes(){document.getElementById('outcomes').innerHTML=OUT.map(o=>`
  <div class="outcome" id="out-${o.id}"><div class="oname">${o.name}</div><div class="oval" id="ov-${o.id}">—</div>
  <div class="oband" id="ob-${o.id}"></div><div class="odelta st" id="od-${o.id}"></div><div class="obar"><i id="of-${o.id}"></i></div></div>`).join('');}
function buildPureScore(){document.getElementById('psCard').innerHTML=`<div class="ps"><div class="ps-label">PureScore</div>
  <div class="ps-val" id="psVal">—</div><div class="ps-bar"><i id="psFill"></i></div><div class="ps-delta st" id="psDelta"></div>
  <div class="ps-pillars"><div class="ps-p">Risk (40%) <b id="pp-risk">—</b></div><div class="ps-p">Reserve (25%) <b id="pp-res">—</b></div>
  <div class="ps-p">Lifestyle (15%) <b id="pp-life">50</b></div><div class="ps-p">Vitality (20%) <b id="pp-vit">—</b></div></div></div>`;}

function updateAll(){const p=PATIENTS[patIdx],profile={...p.res};
  const bv={};for(const v of ALL_VALVES)bv[v.id]=50;baseLevels=simulate(profile,bv,horizon);
  intLevels=simulate({...profile},{...valveState},horizon);
  updatePatientCard();updateTanks();updateOutcomes();updatePureScore();updateRibbon();}
function updatePatientCard(){const p=PATIENTS[patIdx],ps=computePureScore(intLevels);
  const col=ps>=55?'var(--good)':ps>=40?'var(--warn)':'var(--burden)';
  document.getElementById('pCard').innerHTML=`<div><div class="pc-name">${p.name}</div><div class="pc-meta">${p.meta}</div></div>
  <div class="pc-ps"><b style="color:${col}">${ps.toFixed(0)}</b><span>PureScore</span></div>`;}
function updateTanks(){for(const r of RES){const lev=intLevels[r.id],d=lev-baseLevels[r.id],col=tankColor(r.pol,lev);
  const tv=document.getElementById('tv-'+r.id);tv.textContent=lev.toFixed(0);tv.style.color=col;
  document.getElementById('tb-'+r.id).textContent=toBio(r.id,lev);
  const liq=document.getElementById('tl-'+r.id);liq.style.height=Math.max(2,Math.min(100,lev))+'%';liq.style.backgroundColor=col;
  const de=document.getElementById('td-'+r.id);
  if(Math.abs(d)<0.5){de.textContent='stable';de.className='tdelta st';}
  else{const imp=isImproving(r.id,d);de.textContent=`${imp?'▼':'▲'} ${Math.abs(d).toFixed(1)}`;de.className='tdelta '+(imp?'up':'dn');}
  if(openTankId===r.id)showPipes(r.id);}}
function updateOutcomes(){for(const o of OUT){const lev=intLevels[o.id]||50,bl=baseLevels[o.id]||50,d=lev-bl;
  const[bl2,bc]=outBand(o.id,lev),col=bc==='good'?'var(--good)':bc==='bad'?'var(--burden)':'var(--warn)';
  const ov=document.getElementById('ov-'+o.id);ov.textContent=toOutDisplay(o.id,lev);ov.style.color=col;
  const be=document.getElementById('ob-'+o.id);be.textContent=bl2;be.style.color=col;
  const de=document.getElementById('od-'+o.id);
  if(Math.abs(d)<0.3){de.textContent='';de.className='odelta st';}
  else{const imp=isImproving(o.id,d);const dd=o.id==='O_ASCVD'||o.id==='O_T2DM'?`${(riskPct(o.id,lev)-riskPct(o.id,bl)).toFixed(1)}%`:
    o.id==='O_MORT'?`${((lev-bl)*0.02).toFixed(2)} HR`:o.id==='O_BIOAGE'?`${((lev-bl)*0.25).toFixed(1)} yr`:`${d.toFixed(1)} pts`;
    de.textContent=`${imp?'▼':'▲'} ${dd}`;de.className='odelta '+(imp?'up':'dn');}
  const fi=document.getElementById('of-'+o.id);fi.style.width=(o.pol==='burden'?Math.max(0,100-lev):lev)+'%';fi.style.background=col;}}
function updatePureScore(){const ps=computePureScore(intLevels),bps=computePureScore(baseLevels),d=ps-bps;
  const col=ps>=55?'var(--good)':ps>=40?'var(--warn)':'var(--burden)';
  document.getElementById('psVal').textContent=ps.toFixed(0);document.getElementById('psVal').style.color=col;
  const fi=document.getElementById('psFill');fi.style.width=ps+'%';fi.style.background=col;
  const de=document.getElementById('psDelta');
  if(Math.abs(d)<0.3){de.textContent='';de.className='ps-delta st';}
  else{de.textContent=`${d>0?'▲':'▼'} ${Math.abs(d).toFixed(1)} ${d>0?'better':'worse'}`;de.className='ps-delta '+(d>0?'up':'dn');}
  const riskIds=['O_ASCVD','O_T2DM','O_MORT'];
  const pRisk=100-riskIds.reduce((s,id)=>s+(intLevels[id]||50),0)/riskIds.length;
  const pRes=(ASSET_R.reduce((s,id)=>s+(intLevels[id]||50),0)/ASSET_R.length+(100-BURDEN_R.reduce((s,id)=>s+(intLevels[id]||50),0)/BURDEN_R.length))/2;
  document.getElementById('pp-risk').textContent=pRisk.toFixed(0);document.getElementById('pp-res').textContent=pRes.toFixed(0);
  document.getElementById('pp-vit').textContent=(intLevels['O_ENERGY']||50).toFixed(0);}
function updateRibbon(){const ch=[];for(const v of ALL_VALVES){const s=valveState[v.id];if(Math.abs(s-50)<1)continue;
  const d=(s-50)/100*(v.rng[1]-v.rng[0]),sign=d>0?'+':'',val=v.p?d.toFixed(v.p):Math.round(d);ch.push(`<em>${sign}${val} ${v.unit}</em> ${v.name.toLowerCase()}`);}
  if(!ch.length){document.getElementById('ribbon').innerHTML='Drag a knob to see the cascade…';return;}
  const ef=[],eD=(intLevels.O_ENERGY||50)-(baseLevels.O_ENERGY||50);if(Math.abs(eD)>1)ef.push(`vitality <em>${eD>0?'+':''}${eD.toFixed(0)}</em>`);
  const aD=riskPct('O_ASCVD',intLevels.O_ASCVD||50)-riskPct('O_ASCVD',baseLevels.O_ASCVD||50);if(Math.abs(aD)>0.3)ef.push(`ASCVD <em>${aD>0?'+':''}${aD.toFixed(1)}%</em>`);
  const tD=riskPct('O_T2DM',intLevels.O_T2DM||50)-riskPct('O_T2DM',baseLevels.O_T2DM||50);if(Math.abs(tD)>0.3)ef.push(`T2DM <em>${tD>0?'+':''}${tD.toFixed(1)}%</em>`);
  document.getElementById('ribbon').innerHTML=`<b>${ch.slice(0,3).join(' & ')}</b>${ch.length>3?` +${ch.length-3} more`:''} → ${ef.join(', ')||'minimal change'}`;}
function updateLeaderboard(){const rank=sensitivity(PATIENTS[patIdx].res,valveState,horizon);
  document.getElementById('leaderboard').innerHTML=`<div class="lb-title">Highest-leverage knobs for this patient</div>`+
   rank.slice(0,6).map((r,i)=>`<div class="lev"><span class="lev-rank">#${i+1}</span><span>${r.name}</span><span class="lev-s">${r.gain>=0?'+':''}${r.gain.toFixed(1)}</span></div>`).join('');}
function showPipes(id){const inb=FLOWS.filter(f=>f.tgt===id),outb=FLOWS.filter(f=>f.src===id),el=document.getElementById('tp-'+id),lines=[];
  if(inb.length){lines.push('<div style="color:var(--muted);margin-bottom:3px;font-weight:700">Inbound pipes:</div>');
   for(const f of inb){const sn=RES_MAP[f.src]?.name||VALVE_MAP[f.src]?.name||OUT_MAP[f.src]?.name||f.src;const dir=f.s>0?'fills +':'drains −';
    const cls=(f.s>0&&RES_MAP[id]?.pol==='burden')||(f.s<0&&RES_MAP[id]?.pol==='asset')?'pin':'pout';
    lines.push(`<div class="pipe"><b>${sn}</b> <span class="${cls}">${dir}${f.g} (${(f.c*100).toFixed(0)}%)</span></div>`);}}
  if(outb.length){lines.push('<div style="color:var(--muted);margin:4px 0 3px;font-weight:700">Outbound pipes:</div>');
   for(const f of outb){const tn=RES_MAP[f.tgt]?.name||OUT_MAP[f.tgt]?.name||f.tgt;lines.push(`<div class="pipe"><b>${tn}</b> <span style="color:var(--muted)">${f.s>0?'→ fills':'→ drains'} ${f.g} (${(f.c*100).toFixed(0)}%)</span></div>`);}}
  el.innerHTML=lines.join('');}
function toggleTank(id){if(openTankId===id){document.getElementById('tank-'+id).classList.remove('open');openTankId=null;}
  else{if(openTankId)document.getElementById('tank-'+openTankId)?.classList.remove('open');openTankId=id;showPipes(id);document.getElementById('tank-'+id).classList.add('open');}}
let cascadeTimer=null;
function onValve(vid,val){valveState[vid]=parseFloat(val);const v=VALVE_MAP[vid],d=(val-50)/100*(v.rng[1]-v.rng[0]),de=document.getElementById('vd-'+vid);
  if(Math.abs(val-50)<1){de.textContent='baseline';de.className='vd neut';}
  else{const sign=d>0?'+':'',t=v.p?d.toFixed(v.p):Math.round(d);de.textContent=`${sign}${t} ${v.unit}`;de.className='vd '+(((val>50)===v.up)?'pos':'neg');}
  FLOWS.filter(f=>f.src===vid).map(f=>f.tgt).forEach(t=>{const el=document.getElementById('tank-'+t)||document.getElementById('out-'+t);if(el)el.classList.add('cascade');});
  clearTimeout(cascadeTimer);cascadeTimer=setTimeout(()=>document.querySelectorAll('.cascade').forEach(el=>el.classList.remove('cascade')),600);
  intLevels=simulate({...PATIENTS[patIdx].res},{...valveState},horizon);
  updateTanks();updateOutcomes();updatePureScore();updatePatientCard();updateRibbon();}
function setHorizon(h){horizon=h;buildHorizon();updateAll();updateLeaderboard();}
function loadPatient(idx){patIdx=parseInt(idx);initValves();for(const v of ALL_VALVES){const el=document.getElementById('vs-'+v.id);if(el)el.value=50;const de=document.getElementById('vd-'+v.id);if(de){de.textContent='baseline';de.className='vd neut';}}
  openTankId=null;document.querySelectorAll('.tank.open').forEach(el=>el.classList.remove('open'));updateAll();updateLeaderboard();}
function resetAll(){initValves();for(const v of ALL_VALVES){const el=document.getElementById('vs-'+v.id);if(el)el.value=50;const de=document.getElementById('vd-'+v.id);if(de){de.textContent='baseline';de.className='vd neut';}}updateAll();}
function easiestWin(){const rank=sensitivity(PATIENTS[patIdx].res,valveState,horizon);if(!rank.length)return;
  const best=rank[0],v=VALVE_MAP[best.id],dir=v.up?1:-1,target=Math.min(100,Math.max(0,50+dir*15)),sl=document.getElementById('vs-'+best.id);if(!sl)return;
  const start=parseFloat(sl.value),t0=performance.now();
  function anim(now){const pr=Math.min((now-t0)/500,1),e=1-Math.pow(1-pr,3);sl.value=start+(target-start)*e;onValve(best.id,sl.value);if(pr<1)requestAnimationFrame(anim);}
  requestAnimationFrame(anim);sl.closest('.valve')?.scrollIntoView({behavior:'smooth',block:'center'});}
function init(){initValves();buildPatients();buildHorizon();buildValves(VALVES,'bValves');buildValves(RX_VALVES,'rxValves');buildTanks();buildOutcomes();buildPureScore();updateAll();updateLeaderboard();}
init();
</script>
</body>
</html>
"""

out = os.path.join(HERE, "..", "reservoir-sim.html")
html = PAGE.replace("__DATA__", DATA)
open(os.path.normpath(out), "w", encoding="utf-8").write(html)
print("OK wrote reservoir-sim.html (%d KB) from reservoir-flows.json" % (len(html)//1024))
